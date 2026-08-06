package centrifuge

import (
	"sync"
	"sync/atomic"

	"github.com/centrifugal/protocol"
)

const (
	// backOffMinSamples is how many compressed frames are observed before the
	// achieved ratio is trusted enough to act on.
	backOffMinSamples = 32
	// backOffProbeEvery is how many passthrough frames go by before one is
	// compressed again to check whether the payload shape has changed.
	backOffProbeEvery = 512
	// shipMargin is how far past break-even a connection must be before a
	// dictionary is sent.
	//
	// Shipping is a bet that the future resembles the past: nothing has actually
	// been saved at the moment a dictionary goes out, so a connection that ends
	// straight afterwards paid for nothing. The margin is how much evidence is
	// demanded before taking that bet, and it bounds the downside - at 1.5 a
	// short connection measured 0.53x, i.e. traffic nearly doubled, while at 6
	// the worst case across a rate/duration grid was ~3% worse than sending
	// nothing, against an upside of 2-5x.
	shipMargin = 6
	// seedRatio is the deliberately pessimistic saving assumed before any real
	// compression has been measured, so the break-even guard errs towards
	// shipping late rather than early.
	seedRatio = 0.25
	// structureRatio is what the structure dictionary is worth against traffic it
	// was never trained on. Measured across unrelated applications it removes
	// 29-42% of a frame; the low end is used so the guard is not optimistic about
	// payloads it has never seen.
	structureRatio = 0.29
)

// structureCodec is the protocol structure dictionary. It comes from the protocol
// package so server and SDKs cannot drift, and is shared by every connection on
// the node: it holds no application data, so unlike a learned dictionary there
// is nothing to keep separate. One instance means one compressor pool and a
// frame cache that hits across every connection using it.
var structureCodec = protocol.StructureFrameCodec()

// connectionCompression is the built-in ConnectionCompression.
//
// The dictionary is not sent at connect time. It costs its own size in bytes,
// and a connection which only ever receives a handful of frames would spend more
// on the dictionary than it saves - measurably worse than sending nothing. So a
// connection first accumulates enough traffic that the dictionary is certain to
// pay for itself, and only then is it shipped.
type connectionCompression struct {
	engine       *DictionaryCompressionEngine
	protoType    protocol.Type
	backOffRatio float64

	mu    sync.RWMutex
	codec *protocol.DeflateFrameCodec
	// channels are the subscribed channels that could supply a dictionary, and
	// chosen is the one that did. A connection may only use a dictionary built
	// from a channel it is subscribed to - that is the safety boundary - and
	// among those it takes the busiest, so the largest share of its traffic is
	// matched.
	channels []string
	chosen   string
	// started records that the connect reply has been written. It is the frame
	// that tells the client the feature was accepted, so nothing before it may be
	// framed - and everything after it must be.
	started bool
	// structureSent records that the structure dictionary stage has been settled,
	// either by sending it or by finding the client already had it.
	structureSent bool
	// heldIDs are dictionary ids the client advertised at connect. An id is a
	// hash of the content, so a match means byte identical, and the dictionary can
	// be activated by naming it rather than sending it.
	heldIDs map[string]struct{}
	// bytesSeen counts uncompressed payload bytes written to this connection,
	// ratio is a running estimate of the fraction each byte compression removes,
	// and compressed counts frames actually put through the compressor.
	bytesSeen  int
	ratio      float64
	compressed int
	sent       bool

	// backedOff and probeIn are read on every frame, so they are atomics: the
	// whole point of the backed-off path is to be cheaper than compressing,
	// which a lock acquisition per frame would undercut.
	backedOff atomic.Bool
	probeIn   atomic.Int64
}

var _ ConnectionCompression = (*connectionCompression)(nil)

// OnSubscribe implements ConnectionCompression. It records channels that could
// supply a dictionary; which one is used is decided later, when the connection
// has carried enough traffic to be worth sending one.
func (c *connectionCompression) OnSubscribe(channel string) {
	if c.engine.dictionaryChannel(channel) == "" {
		return
	}
	c.mu.Lock()
	for _, ch := range c.channels {
		if ch == channel {
			c.mu.Unlock()
			return
		}
	}
	c.channels = append(c.channels, channel)
	c.mu.Unlock()
}

// OnUnsubscribe implements ConnectionCompression. The channel stops being a
// candidate immediately, so a dictionary built from traffic published after the
// connection lost access can never be handed to it.
//
// A dictionary already in use is deliberately left alone. Its bytes have been
// delivered and its content is frozen at build time, so keeping it discloses
// nothing new - while swapping it would cost another dictionary transfer for no
// security gain.
func (c *connectionCompression) OnUnsubscribe(channel string) {
	c.mu.Lock()
	for i, ch := range c.channels {
		if ch == channel {
			c.channels = append(c.channels[:i], c.channels[i+1:]...)
			break
		}
	}
	c.mu.Unlock()
}

// Encode implements ConnectionCompression.
func (c *connectionCompression) Encode(frame []byte) ([]byte, bool, []byte, bool) {
	before, beforeBinary := c.activationFrame()

	c.mu.Lock()
	// Counted on every path, including while a dictionary is already in use: this
	// is what decides whether the connection has earned the channel dictionary,
	// and stopping once the built-in one is active would mean it never does.
	c.bytesSeen += len(frame)
	codec := c.codec
	c.mu.Unlock()

	if codec == nil {
		// Before the connect reply has gone out. Sampling happens in the broadcast
		// path, where the channel is known.
		return before, beforeBinary, frame, false
	}
	if !c.shouldCompress() {
		// Measured as not worth compressing for this connection. The frame still
		// carries a codec marker, so the receiver is unaffected by the back-off.
		return before, beforeBinary, codec.Passthrough(nil, frame), true
	}
	out := c.engine.compress(codec, frame)
	c.observeRatio(len(frame), len(out))
	return before, beforeBinary, out, true
}

// activationFrame returns an encoded frame carrying a dictionary if this
// connection should start using one before the next payload frame is written,
// and whether that frame must go out binary.
//
// Compression arrives in two stages.
//
// The first is the structure dictionary: protocol envelope and generic JSON
// shape, no application data, so it needs no channel and no entitlement check.
// A client that cached it from an earlier connection gets it back for the price
// of naming an id, and it applies from the very next frame. A client that does
// not have it has to be sent it, so it has to earn it like anything else.
//
// The second replaces it with a dictionary built from a channel's real traffic,
// once the connection has carried enough for that transfer to pay for itself.
func (c *connectionCompression) activationFrame() ([]byte, bool) {
	c.mu.Lock()
	if !c.started {
		// The connect reply is the frame that tells the client the feature was
		// accepted, so it - and nothing before it - goes out unframed. No codec is
		// installed here: whatever comes next must apply from the frame after this
		// one, not to this one.
		c.started = true
		c.mu.Unlock()
		return nil, false
	}
	structureSent, sent := c.structureSent, c.sent
	channels := append([]string(nil), c.channels...)
	c.mu.Unlock()

	if !structureSent {
		f, binary, done := c.activateStructure()
		if done {
			return f, binary
		}
		// Strictly ordered: nothing else may be offered until the structure stage
		// is settled. It is not only that a channel dictionary would supersede it -
		// the frame delivering a channel dictionary is compressed against whatever
		// is already installed, so sending it first would cost 5538 B on JSON
		// instead of 1657 + 460. Waiting is cheaper than jumping ahead.
		return nil, false
	}
	if sent || len(channels) == 0 {
		return nil, false
	}
	// Decided once, here: by now the engine knows how much traffic each channel
	// carries, so the busiest subscribed channel with a ready dictionary wins.
	//
	// TODO: this is where MaxChannelDictionariesPerConn would take effect - a
	// connection would fill several slots in egress order rather than pinning one,
	// and Encode would pick per frame by dominant channel. See the field's comment
	// for what else that needs.
	chosen := c.engine.pickChannel(channels, c.protoType)
	if chosen == "" {
		return nil, false
	}
	codec, encoded, dictRatio, frameCost := c.engine.currentWithRatio(chosen, c.protoType)
	if codec == nil || !c.shouldShip(frameCost, dictRatio) {
		return nil, false
	}
	return c.activate(codec, chosen, encoded)
}

// activateStructure settles the structure dictionary stage. done reports whether
// a frame was produced; when it is false the caller may go on to consider a
// channel dictionary in the same pass.
func (c *connectionCompression) activateStructure() (frame []byte, binary bool, done bool) {
	codec := c.engine.structureCodec()
	if codec == nil {
		c.mu.Lock()
		c.structureSent = true
		c.mu.Unlock()
		return nil, false, false
	}

	c.mu.Lock()
	_, held := c.heldIDs[codec.ID()]
	c.mu.Unlock()

	if held {
		// Nothing to transfer: naming it is enough, so there is nothing to earn
		// and no reason to wait.
		f, b := c.activateDictionary(codec, "", "", true)
		return f, b, true
	}
	// It has to be sent, so it is weighed like any other dictionary. Sending it
	// unconditionally measured 0.52x on connections that ended early - traffic
	// doubling - which is exactly what the break-even guard exists to prevent.
	if !c.shouldShip(c.engine.structureFrameCost(c.protoType), structureRatio) {
		return nil, false, false
	}
	f, b := c.activateDictionary(codec, "", c.engine.structureEncoded(), false)
	return f, b, true
}

// activate emits the ConnectionState frame that upgrades this connection to a
// channel dictionary, and installs it.
//
// The dictionary content travels base64 encoded on JSON, where a bytes field
// carries raw JSON and cannot hold binary, and raw on Protobuf where it can.
func (c *connectionCompression) activate(codec *protocol.DeflateFrameCodec, channel, encoded string) ([]byte, bool) {
	return c.activateDictionary(codec, channel, encoded, false)
}

// activateDictionary emits the ConnectionState frame that switches this
// connection to a dictionary, and installs it.
//
// byID omits the content entirely: the client advertised this id at connect, and
// an id is a hash of the content, so it already holds the same bytes. Otherwise
// the content travels base64 encoded on JSON, where a bytes field carries raw
// JSON and cannot hold binary, and raw on Protobuf where it can.
func (c *connectionCompression) activateDictionary(codec *protocol.DeflateFrameCodec, channel, encoded string, byID bool) ([]byte, bool) {
	c.mu.RLock()
	installed := c.codec
	c.mu.RUnlock()

	d := &protocol.Dictionary{Id: codec.ID()}
	// The first dictionary a connection receives is the only one whose delivery
	// frame cannot be compressed - nothing is installed yet to compress it
	// against - so its content is deflated instead. Later ones travel verbatim
	// inside a frame that is already compressed, and deflating twice would only
	// cost CPU.
	deflate := !byID && installed == nil
	if deflate {
		if packed := protocol.DeflateDictionary(codec.Dict()); len(packed) > 0 && len(packed) < len(codec.Dict()) {
			d.Flags |= protocol.DictionaryFlagDeflate
			encoded = base64Std(packed)
		} else {
			deflate = false
		}
	}
	var data []byte
	var err error
	if c.protoType == protocol.TypeJSON {
		if !byID {
			d.DataB64 = encoded
		}
		data, err = protocol.DefaultJsonReplyEncoder.Encode(
			&protocol.Reply{Push: &protocol.Push{State: &protocol.ConnectionState{Dictionary: d}}})
	} else {
		if !byID {
			if deflate {
				d.Data = protocol.DeflateDictionary(codec.Dict())
			} else {
				d.Data = codec.Dict()
			}
		}
		data, err = protocol.DefaultProtobufReplyEncoder.Encode(
			&protocol.Reply{Push: &protocol.Push{State: &protocol.ConnectionState{Dictionary: d}}})
	}
	if err != nil {
		return nil, false
	}

	c.mu.Lock()
	prev := c.codec
	if channel == "" {
		c.structureSent = true
	} else {
		c.sent = true
		c.chosen = channel
	}
	c.codec = codec
	c.mu.Unlock()

	enc := protocol.GetDataEncoder(c.protoType)
	defer protocol.PutDataEncoder(c.protoType, enc)
	_ = enc.Encode(data)
	frame := enc.Finish()

	// The frame announcing a dictionary cannot be encoded with it - the client
	// only has it once this frame is decoded - but it can be encoded with the one
	// already in use, which by now is always the structure dictionary. The client
	// decodes the frame before installing what it carries, so the ordering holds.
	//
	// This is not a detail: a dictionary is a concatenation of real message
	// samples, so it is ordinary text and compresses well. Measured, it takes a
	// 4 KB dictionary from 5538 B to 1326 B on JSON and 4129 B to 698 B on
	// Protobuf, which divides every break-even in the feature by the same factor.
	if prev != nil {
		return prev.Compress(nil, frame), true
	}
	return frame, false
}

// shouldShip reports whether this connection has carried enough traffic for a
// dictionary costing frameCost bytes to deliver to be worth sending.
//
// The test is on bytes rather than message count so it behaves the same for a
// connection receiving many tiny frames and one receiving few large ones.
func (c *connectionCompression) shouldShip(frameCost int, dictRatio float64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.sent {
		return false
	}
	// Prefer what this dictionary was actually measured to achieve on its
	// channel. Assuming instead delays every connection several fold - and most
	// of all the quiet ones, which may never reach a pessimistic threshold at all.
	r := dictRatio
	if r <= 0 {
		r = seedRatio
	}
	// What matters is the gain over what this connection is ALREADY paying, not
	// over sending nothing. A connection running on the structure dictionary only
	// gains the difference by upgrading, and weighing the full ratio against the
	// cost ships dictionaries that can never pay themselves back - measured, that
	// left a connection worse off than the one it upgraded from.
	gain := r - c.ratio
	if gain <= 0 {
		return false
	}
	if frameCost <= 0 {
		return false
	}
	return float64(c.bytesSeen)*gain >= float64(frameCost)*shipMargin
}

// shouldCompress reports whether this frame should be compressed, or passed
// through because compression has been measured not to pay off.
//
// Compressed output is discarded by the codec when it does not shrink the frame,
// so an incompressible payload already costs no bandwidth - but it costs the
// full compression CPU every single frame. Already compressed media and
// encrypted blobs are common enough that paying for them forever is worth
// avoiding.
func (c *connectionCompression) shouldCompress() bool {
	if !c.backedOff.Load() {
		return true
	}
	if c.probeIn.Add(-1) <= 0 {
		c.probeIn.Store(backOffProbeEvery)
		return true // periodic probe: payload shape may have changed
	}
	return false
}

// backedOffNow reports whether the connection is currently skipping compression.
func (c *connectionCompression) backedOffNow() bool { return c.backedOff.Load() }

// observeRatio refines the saving estimate from an actual compression result and
// engages or releases the back-off.
func (c *connectionCompression) observeRatio(raw, compressed int) {
	if raw <= 0 {
		return
	}
	got := 1 - float64(compressed)/float64(raw)
	if got < 0 {
		got = 0
	}
	c.mu.Lock()
	if c.ratio == 0 {
		c.ratio = got
	} else {
		c.ratio = 0.9*c.ratio + 0.1*got
	}
	c.compressed++
	enough, ratio := c.compressed >= backOffMinSamples, c.ratio
	c.mu.Unlock()

	switch {
	case enough && ratio < c.backOffRatio:
		if c.backedOff.CompareAndSwap(false, true) {
			c.probeIn.Store(backOffProbeEvery)
		}
	case ratio >= c.backOffRatio:
		c.backedOff.Store(false)
	}
}
