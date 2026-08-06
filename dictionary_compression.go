package centrifuge

import (
	"encoding/base64"
	"sync"
	"sync/atomic"
	"time"

	"github.com/centrifugal/centrifuge/internal/convert"
	"github.com/centrifugal/protocol"
	"github.com/maypok86/otter/v2"
)

// ConnectionFlagDictionaryCompression is the ConnectRequest.Flag bit a client
// sets to advertise that it supports dictionary compression: it understands
// ConnectionState pushes carrying a dictionary, and can decode frames prefixed
// with a compressed/raw marker.
//
// The bit describes the mechanism, not an algorithm. Today that implies the
// protocol's default codec, since nothing on the wire names one. If a second
// algorithm is ever needed the client would name it in a dedicated connect
// field, and this bit would keep meaning what it means now.
//
// A server may only compress for clients which set it, which is what makes the
// feature safe to enable for a mixed fleet. The server echoes the bit back in
// ConnectResult.Flag when it actually engaged compression.
const ConnectionFlagDictionaryCompression int64 = 1 << 0

// compressionAware is implemented by transports which can apply connection-level
// compression. Client uses it to install the engine's encoder once a connect
// command has advertised support.
type compressionAware interface {
	setConnectionCompression(cc ConnectionCompression)
	connectionCompression() ConnectionCompression
}

// DictionaryCompression is the pluggable engine behind connection-level frame
// compression. Set Config.DictionaryCompression to enable the feature; leaving
// it nil disables it.
//
// Centrifuge owns negotiation and framing: it checks that the client advertised
// support, calls NewConnection once per eligible connection, and writes whatever
// the returned ConnectionCompression produces. Everything else - which
// dictionary to use, when a connection is ready for one, how a frame is encoded
// - belongs to the engine, so an application can replace the built-in policy
// without touching the transport.
//
// Use NewDictionaryCompressionEngine for the built-in implementation, which derives a
// dictionary from the traffic a node actually sends.
type DictionaryCompression interface {
	// NewConnection is called once per connection, after its connect command has
	// been received. clientFlags is ConnectRequest.Flag verbatim, so the engine
	// decides for itself whether this client can decode what it produces - see
	// ConnectionFlagDictionaryCompression.
	//
	// Returning nil leaves the connection uncompressed, which is what every
	// client predating the feature gets.
	NewConnection(params ConnectionParams) ConnectionCompression
}

// ConnectionParams describes the client a ConnectionCompression is being
// created for. It is a struct rather than a parameter list so further
// negotiation can be added without breaking engines implemented outside this
// package.
type ConnectionParams struct {
	// ProtocolType is the connection's protocol. Dictionaries are never shared
	// between protocols: one built from JSON frames is useless on Protobuf.
	ProtocolType ProtocolType
	// ClientFlags is the capability bitmask the client advertised.
	ClientFlags int64
	// HeldDictionaryIDs are dictionary ids the client says it already has, from a
	// previous connection. An id is a hash of the content, so a match means the
	// two sides hold identical bytes and the dictionary can be activated by
	// naming it instead of sending it. An unrecognised id is simply ignored.
	HeldDictionaryIDs []string
}

// ConnectionCompression encodes outgoing frames for a single connection.
type ConnectionCompression interface {
	// OnSubscribe is called when the connection subscribes to a channel.
	//
	// It exists so an engine can choose a dictionary the connection is entitled
	// to. A connection may safely use a dictionary derived from any channel it
	// subscribes to - it is already authorized to read that channel - and that
	// is what keeps dictionary content from crossing a trust boundary.
	OnSubscribe(channel string)

	// OnUnsubscribe is called when the connection leaves a channel, including
	// when a server or admin removes it.
	//
	// An engine MUST stop treating that channel as a source from this point. A
	// dictionary is built over time, so one handed out after access was revoked
	// could carry content published after the revocation - which the connection
	// was never entitled to see.
	//
	// A dictionary already delivered is a different matter: those bytes have been
	// sent, its content is frozen at build time, and it discloses nothing further.
	// There is no need to withdraw it.
	OnUnsubscribe(channel string)

	// Encode is called for every frame about to be written, on the connection's
	// write goroutine, and returns:
	//
	//	before       - a frame to write verbatim ahead of out, or nil. This is how
	//	               a dictionary reaches the client: it has to arrive before the
	//	               first frame that needs it, and returning it here makes that
	//	               ordering structural rather than something to synchronise.
	//	beforeBinary - whether before must be sent as a binary message. It is
	//	               encoded under whichever dictionary is active at the time,
	//	               which for an upgrade is the previous one.
	//	out          - the bytes to write for this frame.
	//	binary       - whether out must be sent as a binary message. Compressed
	//	               payloads need this even on a JSON connection.
	Encode(frame []byte) (before []byte, beforeBinary bool, out []byte, binary bool)
}

// DictionaryCompressionConfig configures the built-in DictionaryCompression.
//
// The engine samples the frames a node actually sends, builds a shared
// dictionary out of them, and hands it to a connection once that connection has
// carried enough traffic for the dictionary to pay for itself. Because the
// dictionary comes from real traffic there is nothing application specific to
// configure.
type DictionaryCompressionConfig struct {
	// UseChannelDictionary reports whether a channel's traffic may be used to
	// build a compression dictionary. A nil func - the default - means no channel
	// may, so the feature is inert until an operator opts something in.
	//
	// Returning true is a disclosure decision, not a performance one. It asserts:
	//
	//	every message on this channel is safe to show every subscriber,
	//	including subscribers who join later.
	//
	// A dictionary is a verbatim sample of real frames. It is handed to each
	// subscriber that earns it, and it is built once and never rebuilt - so a
	// member joining a year from now receives fragments of messages published
	// today, even on a channel that keeps no history.
	//
	//	Safe:   public feeds anyone may subscribe to - prices, odds, scores,
	//	        status boards, anything already world-readable.
	//	Unsafe: private rooms, direct messages, per-user channels, and anything
	//	        whose membership changes over time or whose earlier content is
	//	        not meant for later arrivals.
	//
	// Dictionaries are always per channel and never merged, so opting one channel
	// in can never expose it to another's subscribers. The remaining question is
	// only the one above: is this channel's past safe for its future.
	//
	// Among channels that qualify, prefer busy ones - quiet channels gain little
	// anyway, since the dictionary is withheld until a connection has carried
	// several times its size, and they would occupy budget under MaxChannelDictionaries
	// that a busier channel could use.
	UseChannelDictionary func(channel string) bool

	// StructureDictionary overrides the protocol structure dictionary this node
	// offers. Leave it nil to use the default, which is what almost everyone
	// should do.
	//
	// It must contain no application data. Unlike a channel dictionary it is
	// handed to every connection regardless of what that connection subscribes
	// to, so anything in it is disclosed to everyone. Protocol envelope, field
	// names and value shapes your own schema uses are fine; sampled traffic is
	// not.
	//
	// Changing it changes its id, since an id is a hash of the content, so
	// clients holding the previous one simply miss the cache once and are sent
	// the new bytes. There is no coordinated rollout to do.
	StructureDictionary []byte

	// DisableStructureDictionary turns off the structure dictionary stage, so a
	// connection compresses only once it has earned a channel dictionary.
	//
	// Rarely useful. The structure dictionary beats permessage-deflate on bytes
	// and costs about half the CPU, and connections that never earn a channel
	// dictionary have nothing else.
	DisableStructureDictionary bool

	// MaxChannelDictionaries caps how many dictionaries the node holds at once. Channels
	// that would exceed it simply never get one and stay uncompressed, so cost
	// has a hard ceiling however many channels are opted in.
	//
	// The cap matters because a DEFLATE writer is bound to its dictionary and can
	// only be pooled per dictionary: with many rarely-used dictionaries each pool
	// is emptied by GC between uses and the ~800 KB writer is rebuilt every time.
	// Measured at 1000 dictionaries that is 817 KB allocated per compression,
	// against 456 B for a shared one.
	//
	// Slots go to the busiest channels. Volume decays, so a channel that has gone
	// quiet releases its slot to one that has become busy, and a challenger must
	// beat the weakest holder by a clear margin before displacing it - without
	// that, channels of similar volume trade slots continuously.
	//
	// Zero means 64.
	// It defaults to 0, which means no channel takes part however
	// UseChannelDictionary is written. Both decisions are therefore explicit: the
	// predicate says WHICH channels may contribute, which is a disclosure
	// decision, and this says HOW MANY dictionaries you are willing to pay for,
	// which is a cost decision.
	MaxChannelDictionaries int

	// MaxChannelDictionariesPerConn caps how many channel dictionaries a single
	// connection may hold. Slot zero is the structure dictionary and is never
	// counted: it costs nothing to hold and is the fallback for frames whose
	// channel has no slot.
	//
	// It defaults to 1, which is one dictionary per connection, chosen from the
	// busiest channel that connection subscribes to. Raising it lets each frame
	// be compressed against its own channel's dictionary instead, which is worth
	// a great deal when the writer emits one message per frame and nothing at all
	// once it batches - measured, 2.30x against 4.80x at batch 1, and marginally
	// negative at batch 16. The engine measures its own batching and declines the
	// extra slots where they would not pay, so this is a ceiling rather than a
	// target.
	//
	// TODO: not honoured yet. A connection still holds exactly one channel
	// dictionary whatever this is set to, so raising it currently does nothing.
	// Implementing it needs three pieces:
	//
	//   1. A slot table per connection instead of a single codec, with the frame
	//      marker carrying the slot index so the client knows which dictionary a
	//      frame was compressed against.
	//   2. Per-frame selection by dominant channel, falling back to the best slot
	//      held and then to the structure dictionary.
	//   3. A gate on measured messages-per-frame, so the extra slots are only
	//      bought below about 4 - above that they cost transfer and return
	//      nothing.
	//
	// Expect it to improve the shared frame cache rather than harm it: today
	// subscribers of one channel hold different dictionaries and so need separate
	// compressions of the same publication - simulated at 18 per publication,
	// dropping to 1 once every subscriber of a channel uses that channel's
	// dictionary. See _examples and the studies behind CENTRIFUGE_STUDY=1.
	MaxChannelDictionariesPerConn int

	// DictionarySize is the target size in bytes of the generated dictionary.
	// Larger dictionaries compress slightly better but cost proportionally more
	// CPU per frame, because DEFLATE rehashes the dictionary on every frame.
	// Measurements put the useful range at roughly 1.5-6 KB; beyond that the
	// ratio flattens while CPU keeps climbing. Zero means 4096.
	DictionarySize int

	// MinSamples is how many frames the engine observes before building the
	// first dictionary. Zero means 64.
	MinSamples int

	// FrameCacheSize bounds the shared cache of compressed frames, in bytes.
	// The cache is what keeps this affordable under fan-out: subscribers of a
	// channel which are not batching all produce byte-identical frames for the
	// same publication, so without it one publication to N subscribers would be
	// compressed N times.
	//
	// It only helps while frames are shareable. With ConnectReply.WriteDelay set
	// each connection batches a different mix of messages, no two frames match,
	// and the cache becomes pure overhead - measured at about 3.5%. Set a
	// negative value to disable it in that case.
	//
	// Zero means 8 MB.
	FrameCacheSize int

	// BackOffRatio is the fraction of each byte compression must remove to be
	// worth its CPU. A connection whose measured saving falls below this stops
	// compressing and passes frames through, re-probing periodically. Zero means
	// 0.15. It rarely triggers on JSON connections, where base64 payloads and
	// the always-matchable protocol envelope keep the ratio above it.
	BackOffRatio float64
}

func (c DictionaryCompressionConfig) dictionarySize() int {
	if c.DictionarySize <= 0 {
		return 4096
	}
	return c.DictionarySize
}

func (c DictionaryCompressionConfig) maxChannelDictionaries() int {
	if c.MaxChannelDictionaries < 0 {
		return 0
	}
	return c.MaxChannelDictionaries
}

// maxChannelDictionariesPerConn defaults to 1, which is one dictionary per
// connection - the behaviour that has always shipped. Raising it authorises the
// engine to buy more, and it still only does so where its own measured batching
// says they will pay.
func (c DictionaryCompressionConfig) maxChannelDictionariesPerConn() int {
	if c.MaxChannelDictionariesPerConn <= 0 {
		return 1
	}
	return c.MaxChannelDictionariesPerConn
}

func (c DictionaryCompressionConfig) minSamples() int {
	if c.MinSamples <= 0 {
		return 64
	}
	return c.MinSamples
}

func (c DictionaryCompressionConfig) frameCacheSize() int {
	if c.FrameCacheSize == 0 {
		return 8 * 1024 * 1024
	}
	return c.FrameCacheSize
}

// structureDictionary returns the structure dictionary bytes this node offers.
func (c DictionaryCompressionConfig) structureDictionary() []byte {
	if c.DisableStructureDictionary {
		return nil
	}
	if len(c.StructureDictionary) > 0 {
		return c.StructureDictionary
	}
	return protocol.StructureDictionary
}

func (c DictionaryCompressionConfig) backOffRatio() float64 {
	if c.BackOffRatio <= 0 {
		return 0.15
	}
	return c.BackOffRatio
}

// DictionaryGroupStats describes one group's dictionary.
type DictionaryGroupStats struct {
	// Ready reports whether this group's dictionary has been built.
	Ready bool
	// Sampling reports whether the group is still gathering frames.
	Sampling bool
	// ID identifies the dictionary content.
	ID string
	// Size is the dictionary size in bytes.
	Size int
	// Protocol the dictionary was built for.
	Protocol string
}

// DictionaryCompressionStats reports activity of the built-in engine.
type DictionaryCompressionStats struct {
	// Groups holds per-group state, keyed by "group/protocol". Its length is the
	// number of dictionaries the node is holding, which is what the feature's
	// memory and CPU overhead scales with.
	Groups map[string]DictionaryGroupStats
	// DictionariesReady counts slots with a built dictionary.
	DictionariesReady int
	// Candidates counts channels whose volume is tracked without holding a slot.
	// They cost one counter each and are how a channel that becomes busy can take
	// a slot from one that has gone quiet.
	Candidates int
	// FrameCompressions counts frames actually compressed.
	FrameCompressions int64
	// FrameCacheHits counts frames served from the shared cache instead of being
	// compressed again. Under fan-out this should dominate FrameCompressions; a
	// low value means frames are not shareable, usually because connections are
	// batching or because most channels have a single subscriber.
	FrameCacheHits int64
}

// DictionaryCompressionEngine is the built-in DictionaryCompression, exposing
// statistics on top of the interface.
// dictKey identifies one dictionary. Protocol is part of it because a JSON
// connection gains nothing from a dictionary built out of Protobuf frames.
type dictKey struct {
	channel string
	proto   protocol.Type
}

// channelDict is one channel's dictionary and the samples still being gathered
// for it. Cost is per dictionary, which is what MaxChannelDictionaries bounds.
type channelDict struct {
	// volume estimates the EGRESS this channel is responsible for: published
	// bytes multiplied by subscriber count. That, not publish rate, is what a
	// dictionary would save - a channel with a hundred subscribers and a modest
	// rate moves far more bytes than a busy one with a single subscriber, and
	// deserves the slot.
	//
	// It keeps counting after the dictionary is built, since counting only while
	// sampling would measure how fast a channel reached MinSamples rather than
	// how much traffic it carries. It decays, so it means "busiest now" rather
	// than "busiest ever".
	volume atomic.Int64
	// promoted reports whether this channel holds one of the MaxChannelDictionaries
	// slots. Channels without one are tracked for their volume alone, so a busy
	// newcomer is visible and can take a slot from a channel that has gone quiet.
	promoted bool
	samples  [][]byte
	sampleSz int
	codec    *protocol.DeflateFrameCodec
	// encoded holds the base64 form used by JSON connections, computed once.
	encoded string
	// frameCost is what delivering this dictionary actually costs on the wire:
	// the activation frame, compressed the way it will be sent. Measured once at
	// build time so the break-even test can weigh a real cost rather than infer
	// one from the dictionary length, which overstates it several fold.
	frameCost int
	// ratio is the saving this dictionary actually achieves on this channel's
	// traffic, measured once at build time against a real sample.
	//
	// It decides how soon a connection is worth sending the dictionary to, and
	// measuring beats assuming by a wide margin: a pessimistic 25% assumption put
	// break-even at 24 KB for a 4 KB dictionary, where the measured 88% puts it at
	// under 7 KB. On a connection receiving a message every five seconds that is
	// the difference between twelve minutes and three.
	ratio float64
}

const (
	// admissionMargin is how far a candidate must exceed the weakest slot holder
	// before taking its place. Without it, channels of similar volume trade slots
	// continuously: simulated over 2000 rounds, 288 evictions became 24 once a
	// 2x margin was applied. Higher is calmer but slower to react.
	admissionMargin = 2
	// decayHalfLife is how often volumes are halved. Without decay a channel that
	// dominated an hour ago and has since gone silent keeps its slot forever -
	// simulated, 0 of 4 slots were released to the channels that had become busy.
	decayHalfLife = 5 * time.Minute
	// candidateFactor bounds volume tracking for channels without a slot, as a
	// multiple of MaxChannelDictionaries. They cost a counter each, no samples or codec.
	candidateFactor = 4
	// rebalanceEvery spaces out slot reconsideration, which needs the write lock.
	// It runs on the broadcast path, so it must stay rare.
	rebalanceEvery = 512
)

type DictionaryCompressionEngine struct {
	mu     sync.RWMutex
	config DictionaryCompressionConfig
	dicts  map[dictKey]*channelDict
	// holders counts promoted entries, so the cap can be checked without walking
	// the map on every observation.
	holders   int
	lastDecay time.Time
	observes  atomic.Int64
	frames    *otter.Cache[string, []byte]
	hits      atomic.Int64
	misses    atomic.Int64

	// The structure dictionary and everything derived from it, computed once at
	// construction. It is the same bytes for every connection on this node.
	structure         *protocol.DeflateFrameCodec
	structureB64      string
	structureCostJSON int
	structureCostPB   int
}

// channelGroup resolves a channel to its dictionary group, or "" when the
// channel takes no part.
// dictionaryChannel returns the channel if it takes part, or "" if not.
func (d *DictionaryCompressionEngine) dictionaryChannel(channel string) string {
	if d.config.UseChannelDictionary == nil || !d.config.UseChannelDictionary(channel) {
		return ""
	}
	return channel
}

var _ DictionaryCompression = (*DictionaryCompressionEngine)(nil)

// sampleCap bounds how much traffic is retained for dictionary construction.
const sampleCap = 64 * 1024

// NewDictionaryCompressionEngine returns the built-in engine.
//
// With no further configuration it offers the structure dictionary alone, which
// costs one small transfer per client - cached and reused across that client's
// later connections - and needs no decision about which channels take part,
// because it contains no application data. Measured, that on its own beats
// permessage-deflate on bytes at every batch size and on CPU by roughly half.
//
// Setting UseChannelDictionary and MaxChannelDictionaries adds dictionaries
// learned from real traffic on the channels named, which is worth more but
// requires deciding, per channel, that its content may be shown to every
// subscriber including later ones.
func NewDictionaryCompressionEngine(config DictionaryCompressionConfig) *DictionaryCompressionEngine {
	d := &DictionaryCompressionEngine{config: config, dicts: map[dictKey]*channelDict{}}
	if dict := config.structureDictionary(); len(dict) > 0 {
		id := protocol.DictionaryID(dict)
		d.structure = protocol.NewDeflateFrameCodec(id, dict)
		d.structureB64 = base64Std(dict)
		// This one cannot be compressed on delivery: it is the first dictionary a
		// connection sees, so there is nothing to compress it against.
		d.structureCostJSON = plainActivationFrameCost(protocol.TypeJSON, id, dict, d.structureB64)
		d.structureCostPB = plainActivationFrameCost(protocol.TypeProtobuf, id, dict, d.structureB64)
	}
	if size := config.frameCacheSize(); size > 0 {
		d.frames = otter.Must(&otter.Options[string, []byte]{
			MaximumWeight: uint64(size),
			Weigher: func(key string, value []byte) uint32 {
				return uint32(len(key) + len(value))
			},
			// Entries are only useful while a publication is still being fanned
			// out, so they expire quickly and keep the working set small.
			ExpiryCalculator: otter.ExpiryWriting[string, []byte](2 * time.Second),
		})
	}
	return d
}

// NewConnection implements DictionaryCompression, serving only clients which
// advertised ConnectionFlagDictionaryCompression.
func (d *DictionaryCompressionEngine) NewConnection(params ConnectionParams) ConnectionCompression {
	if params.ClientFlags&ConnectionFlagDictionaryCompression == 0 {
		return nil
	}
	c := &connectionCompression{
		engine:       d,
		protoType:    params.ProtocolType.toProto(),
		backOffRatio: d.config.backOffRatio(),
	}
	if len(params.HeldDictionaryIDs) > 0 {
		c.heldIDs = make(map[string]struct{}, len(params.HeldDictionaryIDs))
		for _, id := range params.HeldDictionaryIDs {
			c.heldIDs[id] = struct{}{}
		}
	}
	return c
}

// plainActivationFrameCost measures the wire size of an activation frame that
// cannot be compressed, because no dictionary is installed yet. Its content is
// deflated instead, which is what the connection will actually send.
func plainActivationFrameCost(proto protocol.Type, id string, dict []byte, encoded string) int {
	d := &protocol.Dictionary{Id: id}
	if packed := protocol.DeflateDictionary(dict); len(packed) > 0 && len(packed) < len(dict) {
		d.Flags |= protocol.DictionaryFlagDeflate
		dict = packed
		encoded = base64Std(packed)
	}
	var data []byte
	var err error
	if proto == protocol.TypeJSON {
		d.DataB64 = encoded
		data, err = protocol.DefaultJsonReplyEncoder.Encode(
			&protocol.Reply{Push: &protocol.Push{State: &protocol.ConnectionState{Dictionary: d}}})
	} else {
		d.Data = dict
		data, err = protocol.DefaultProtobufReplyEncoder.Encode(
			&protocol.Reply{Push: &protocol.Push{State: &protocol.ConnectionState{Dictionary: d}}})
	}
	if err != nil {
		return len(dict)
	}
	enc := protocol.GetDataEncoder(proto)
	defer protocol.PutDataEncoder(proto, enc)
	_ = enc.Encode(data)
	return len(enc.Finish())
}

// Stats returns current engine statistics.
func (d *DictionaryCompressionEngine) Stats() DictionaryCompressionStats {
	s := DictionaryCompressionStats{
		FrameCompressions: d.misses.Load(),
		FrameCacheHits:    d.hits.Load(),
	}
	d.mu.RLock()
	defer d.mu.RUnlock()
	s.Groups = make(map[string]DictionaryGroupStats, len(d.dicts))
	for k, g := range d.dicts {
		if !g.promoted {
			// A channel whose volume is being watched in case it earns a slot.
			// It holds no dictionary and costs nothing but a counter.
			s.Candidates++
			continue
		}
		gs := DictionaryGroupStats{Protocol: string(k.proto), Sampling: g.codec == nil}
		if g.codec != nil {
			gs.Ready = true
			gs.ID = g.codec.ID()
			gs.Size = len(g.codec.Dict())
			s.DictionariesReady++
		}
		s.Groups[k.channel+"/"+string(k.proto)] = gs
	}
	return s
}

// observe feeds an outgoing frame into the sample buffer. It runs on the write
// path, so it stays cheap: it copies only while the buffer is filling and does
// nothing once a dictionary exists.
// observe feeds one encoded publication into a group's sample buffer. It is
// called from the broadcast path, where the channel - and therefore the group -
// is known. Sampling anywhere downstream of that would mix channels together and
// defeat the whole point of grouping.
func (d *DictionaryCompressionEngine) observe(channel string, proto protocol.Type, frame []byte, subscribers int) {
	if channel == "" || len(frame) == 0 {
		return
	}
	k := dictKey{channel: channel, proto: proto}

	d.mu.RLock()
	g, ok := d.dicts[k]
	d.mu.RUnlock()

	if !ok {
		// First sight of this channel: start watching its volume. A free slot is
		// taken immediately; otherwise it waits and may earn one at a rebalance.
		d.mu.Lock()
		if g, ok = d.dicts[k]; !ok {
			if len(d.dicts) >= d.config.maxChannelDictionaries()*candidateFactor {
				d.dropWeakestCandidateLocked()
			}
			g = &channelDict{}
			if d.holders < d.config.maxChannelDictionaries() {
				g.promoted = true
				d.holders++
			}
			d.dicts[k] = g
		}
		d.mu.Unlock()
	}
	// Egress, not publish volume: one publication costs len(frame) bytes per
	// subscriber. A channel with no subscribers still counts as one so a brand
	// new channel is not scored at zero.
	if subscribers < 1 {
		subscribers = 1
	}
	g.volume.Add(int64(len(frame)) * int64(subscribers))

	d.mu.RLock()
	promoted, built := g.promoted, g.codec != nil
	d.mu.RUnlock()
	if built || !promoted {
		// Nothing to sample: either it is done, or this channel is only being
		// watched in case it earns a slot later.
		d.maybeRebalance()
		return
	}

	d.mu.Lock()
	if g.codec == nil && g.sampleSz < sampleCap {
		cp := make([]byte, len(frame))
		copy(cp, frame)
		g.samples = append(g.samples, cp)
		g.sampleSz += len(cp)
	}
	// Build once there are enough frames, or once the sample buffer is full. The
	// second condition matters for channels carrying large frames: they fill the
	// buffer long before reaching MinSamples, and without it the dictionary would
	// never be built for them at all.
	ready := g.codec == nil && (len(g.samples) >= d.config.minSamples() || g.sampleSz >= sampleCap)
	d.mu.Unlock()
	if ready {
		d.build(k)
	}
	d.maybeRebalance()
}

// dropWeakestCandidateLocked frees a tracking entry when the candidate set is
// full. Slot holders are never dropped here - only rebalance moves those.
func (d *DictionaryCompressionEngine) dropWeakestCandidateLocked() {
	var weakest dictKey
	var weakestVol int64 = -1
	found := false
	for k, g := range d.dicts {
		if g.promoted {
			continue
		}
		if v := g.volume.Load(); !found || v < weakestVol {
			weakest, weakestVol, found = k, v, true
		}
	}
	if found {
		delete(d.dicts, weakest)
	}
}

// maybeRebalance reconsiders slot allocation now and then. It runs on the
// broadcast path, so the work is spaced out and done under one lock.
func (d *DictionaryCompressionEngine) maybeRebalance() {
	if d.observes.Add(1)%rebalanceEvery != 0 {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()

	now := time.Now()
	if d.lastDecay.IsZero() {
		d.lastDecay = now
	} else if now.Sub(d.lastDecay) >= decayHalfLife {
		for _, g := range d.dicts {
			g.volume.Store(g.volume.Load() / 2)
		}
		d.lastDecay = now
	}

	// Best candidate takes the weakest slot, but only by a clear margin - a
	// narrow win would just be traded back at the next rebalance.
	var bestKey, weakKey dictKey
	var bestVol, weakVol int64 = -1, -1
	haveBest, haveWeak := false, false
	for k, g := range d.dicts {
		v := g.volume.Load()
		if g.promoted {
			if !haveWeak || v < weakVol {
				weakKey, weakVol, haveWeak = k, v, true
			}
			continue
		}
		if !haveBest || v > bestVol {
			bestKey, bestVol, haveBest = k, v, true
		}
	}
	if !haveBest {
		return
	}
	if d.holders < d.config.maxChannelDictionaries() {
		d.dicts[bestKey].promoted = true
		d.holders++
		return
	}
	if !haveWeak || bestVol <= weakVol*admissionMargin {
		return
	}
	// Evicting only stops NEW connections being given this dictionary. Anyone
	// already using it keeps a reference to the codec, so nothing is disrupted.
	delete(d.dicts, weakKey)
	d.dicts[bestKey].promoted = true
}

// build turns collected samples into a dictionary: recent traffic concatenated
// and truncated to the target size, keeping the tail so the most recent frames
// survive. DEFLATE matches anywhere in the window, so nothing smarter is needed
// for the envelope and field-name redundancy which dominates small frames.
func (d *DictionaryCompressionEngine) build(k dictKey) {
	d.mu.Lock()
	defer d.mu.Unlock()
	g, ok := d.dicts[k]
	if !ok || g.codec != nil || len(g.samples) == 0 {
		return
	}
	target := d.config.dictionarySize()
	var buf []byte
	for i := len(g.samples) - 1; i >= 0 && len(buf) < target; i-- {
		buf = append(buf, g.samples[i]...)
	}
	if len(buf) > target {
		buf = buf[:target]
	}
	id := protocol.DictionaryID(buf)
	g.codec = protocol.NewDeflateFrameCodec(id, buf)
	g.encoded = base64.StdEncoding.EncodeToString(buf)

	// Measure what this dictionary is actually worth on this channel, using a
	// sample that did not shape it where possible, so the figure is not flattered
	// by compressing a frame the dictionary literally contains.
	probe := g.samples[0]
	if len(g.samples) > 1 {
		probe = g.samples[len(g.samples)-1]
	}
	if out := g.codec.Compress(nil, probe); len(probe) > 0 {
		g.ratio = 1 - float64(len(out))/float64(len(probe))
		if g.ratio < 0 {
			g.ratio = 0
		}
	}

	g.frameCost = activationFrameCost(k.proto, id, buf, g.encoded)

	g.samples = nil
	g.sampleSz = 0
}

// activationFrameCost measures the wire size of the frame that delivers this
// dictionary. The frame is compressed against the structure dictionary, which
// every connection already holds by the time a channel dictionary is offered -
// worth about 4x on JSON and 6x on Protobuf over sending it verbatim.
func activationFrameCost(proto protocol.Type, id string, dict []byte, encoded string) int {
	d := &protocol.Dictionary{Id: id}
	var data []byte
	var err error
	if proto == protocol.TypeJSON {
		d.DataB64 = encoded
		data, err = protocol.DefaultJsonReplyEncoder.Encode(
			&protocol.Reply{Push: &protocol.Push{State: &protocol.ConnectionState{Dictionary: d}}})
	} else {
		d.Data = dict
		data, err = protocol.DefaultProtobufReplyEncoder.Encode(
			&protocol.Reply{Push: &protocol.Push{State: &protocol.ConnectionState{Dictionary: d}}})
	}
	if err != nil {
		return len(dict) // conservative: assume no saving
	}
	enc := protocol.GetDataEncoder(proto)
	defer protocol.PutDataEncoder(proto, enc)
	_ = enc.Encode(data)
	return len(protocol.StructureFrameCodec().Compress(nil, enc.Finish()))
}

func (d *DictionaryCompressionEngine) current(channel string, proto protocol.Type) (*protocol.DeflateFrameCodec, string) {
	c, e, _, _ := d.currentWithRatio(channel, proto)
	return c, e
}

// currentWithRatio also returns the saving this dictionary was measured to
// achieve, which is what decides when a connection has earned it.
func (d *DictionaryCompressionEngine) currentWithRatio(channel string, proto protocol.Type) (*protocol.DeflateFrameCodec, string, float64, int) {
	if channel == "" {
		return nil, "", 0, 0
	}
	d.mu.RLock()
	defer d.mu.RUnlock()
	if g, ok := d.dicts[dictKey{channel: channel, proto: proto}]; ok {
		return g.codec, g.encoded, g.ratio, g.frameCost
	}
	return nil, "", 0, 0
}

// compress returns the framed form of frame, reusing a cached result when the
// same frame has just been compressed for another connection.
func (d *DictionaryCompressionEngine) compress(codec *protocol.DeflateFrameCodec, frame []byte) []byte {
	if d.frames == nil {
		d.misses.Add(1)
		return codec.Compress(nil, frame)
	}
	// The dictionary has to be part of the key. Connections subscribed to
	// different channels hold different dictionaries yet receive the same frames,
	// so keying on frame bytes alone would hand one connection output encoded
	// against a dictionary it does not have.
	key := make([]byte, 0, len(codec.ID())+1+len(frame))
	key = append(append(append(key, codec.ID()...), 0), frame...)
	if out, ok := d.frames.GetIfPresent(convert.BytesToString(key)); ok {
		d.hits.Add(1)
		return out
	}
	d.misses.Add(1)
	out := codec.Compress(nil, frame)
	d.frames.Set(string(key), out)
	return out
}

// structureCodec returns the codec for the structure dictionary this node
// offers, or nil when the feature is configured without one.
//
// It is one instance for the whole process: the bytes are identical for every
// connection, so a single codec means a single compressor pool and a frame cache
// that hits across every connection using it. That is why the structure stage
// has the cheapest CPU profile of any configuration - measured, 9.63
// core-seconds per million deliveries against permessage-deflate's 20.78.
func (d *DictionaryCompressionEngine) structureCodec() *protocol.DeflateFrameCodec {
	return d.structure
}

// structureEncoded is the base64 form JSON connections need, computed once.
func (d *DictionaryCompressionEngine) structureEncoded() string {
	return d.structureB64
}

// structureFrameCost is the measured wire cost of delivering the structure
// dictionary to a client that does not already hold it. It cannot be compressed
// against anything, since this is the first dictionary the connection sees.
func (d *DictionaryCompressionEngine) structureFrameCost(proto protocol.Type) int {
	if proto == protocol.TypeProtobuf {
		return d.structureCostPB
	}
	return d.structureCostJSON
}

// cacheStats reports frame cache hits and misses, for tests.
// pickChannel chooses which of a connection's subscribed channels should supply
// its dictionary: the one contributing the most bytes, so the largest possible
// share of that connection's traffic is matched. A connection holds one
// dictionary, and its frames may batch several channels together, so this is
// about covering the dominant one - picking a quiet channel instead measured
// 19-35% worse depending on how lopsided the traffic is.
func (d *DictionaryCompressionEngine) pickChannel(channels []string, proto protocol.Type) string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	best, bestVol := "", int64(-1)
	for _, ch := range channels {
		g, ok := d.dicts[dictKey{channel: ch, proto: proto}]
		if !ok || g.codec == nil {
			continue // no dictionary ready for this channel yet
		}
		if v := g.volume.Load(); v > bestVol {
			best, bestVol = ch, v
		}
	}
	return best
}

func (d *DictionaryCompressionEngine) cacheStats() (hits, misses int64) {
	return d.hits.Load(), d.misses.Load()
}

func base64Std(b []byte) string { return base64.StdEncoding.EncodeToString(b) }
