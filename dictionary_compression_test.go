package centrifuge

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/centrifugal/centrifuge/internal/websocket"

	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/require"
)

func sampleFrame(i int) []byte {
	return []byte(fmt.Sprintf(`{"push":{"channel":"demo","pub":{"data":{"seq":%d,"v":"payload"}}}}`, i))
}

// newTestConn builds an engine plus one connection encoder, primed past the
// break-even guard so tests can exercise the compressing path directly.
const testChannel2 = "demo:channel"

// useAll opts every channel in, which is the simplest thing an operator can write.
func useAll(string) bool { return true }

func newTestConn(t *testing.T, cfg DictionaryCompressionConfig, pt ProtocolType) (*DictionaryCompressionEngine, *connectionCompression) {
	t.Helper()
	if cfg.UseChannelDictionary == nil {
		cfg.UseChannelDictionary = useAll
	}
	if cfg.MaxChannelDictionaries == 0 {
		// The production default is 0, which keeps channel dictionaries off until
		// an operator states a budget. These tests are about what happens once one
		// is stated.
		cfg.MaxChannelDictionaries = 64
	}
	e := NewDictionaryCompressionEngine(cfg)
	cc := e.NewConnection(ConnectionParams{ProtocolType: pt, ClientFlags: ConnectionFlagDictionaryCompression}).(*connectionCompression)
	cc.OnSubscribe(testChannel2)
	// Sampling happens in the broadcast path, where the channel is known.
	for i := 0; i < cfg.minSamples(); i++ {
		e.observe(testChannel2, pt.toProto(), sampleFrame(i), 1)
	}
	return e, cc
}

// upgradeFrame returns the ConnectionState frame a connection would send now,
// or nil. It steps past the connect-reply gate that a real connection passes on
// its first write, so a test can drive the channel-dictionary decision directly.
func upgradeFrame(cc *connectionCompression) []byte {
	settleStructure(cc)
	f, _ := cc.activationFrame()
	return f
}

// settleStructure puts a connection in the state it reaches once the structure
// dictionary stage is behind it, so a test can drive the channel-dictionary
// decision directly.
func settleStructure(cc *connectionCompression) {
	cc.mu.Lock()
	cc.started = true
	cc.structureSent = true
	if cc.codec == nil {
		cc.codec = structureCodec
	}
	cc.mu.Unlock()
}

func TestDictionaryBuildsAfterMinSamples(t *testing.T) {
	e := NewDictionaryCompressionEngine(DictionaryCompressionConfig{
		MinSamples: 16, DictionarySize: 1024, UseChannelDictionary: useAll,
		MaxChannelDictionaries: 64,
	})
	for i := 0; i < 15; i++ {
		e.observe(testChannel2, protocol.TypeJSON, sampleFrame(i), 1)
	}
	codec, _ := e.current(testChannel2, protocol.TypeJSON)
	require.Nil(t, codec, "must not build before MinSamples")

	e.observe(testChannel2, protocol.TypeJSON, sampleFrame(15), 1)
	codec, encoded := e.current(testChannel2, protocol.TypeJSON)
	require.NotNil(t, codec)
	require.NotEmpty(t, encoded)
	require.NotEmpty(t, codec.ID())
	require.LessOrEqual(t, len(codec.Dict()), 1024)
}

// Large frames fill the sample buffer long before MinSamples is reached. Without
// the sample-cap trigger the dictionary would never be built for them at all,
// silently disabling the feature for exactly the traffic that benefits most.
func TestDictionaryBuildsWhenSampleBufferFills(t *testing.T) {
	e := NewDictionaryCompressionEngine(DictionaryCompressionConfig{
		MinSamples: 1000, DictionarySize: 4096, UseChannelDictionary: useAll,
		MaxChannelDictionaries: 64,
	})
	big := bytes.Repeat([]byte("x"), 8*1024)
	for i := 0; i < sampleCap/len(big)+1; i++ {
		e.observe(testChannel2, protocol.TypeJSON, big, 1)
	}
	codec, _ := e.current(testChannel2, protocol.TypeJSON)
	require.NotNil(t, codec, "must build once the sample buffer is full")
}

// A channel dictionary costs its own size in bytes. A connection which only
// receives a handful of frames would spend more on it than it saves, so it is
// withheld until the connection has provably carried enough traffic. The
// built-in dictionary is not subject to this - it costs nothing to deliver.
func TestDictionaryWithheldUntilBreakEven(t *testing.T) {
	e, cc := newTestConn(t, DictionaryCompressionConfig{MinSamples: 8, DictionarySize: 4096}, ProtocolTypeJSON)
	_ = e
	for i := 0; i < 8; i++ {
		before, _, _, _ := cc.Encode(sampleFrame(i))
		require.Nil(t, before, "must not ship a channel dictionary this early")
	}
	require.Nil(t, upgradeFrame(cc), "still below break-even")

	// Push the connection well past break-even.
	cc.mu.Lock()
	cc.bytesSeen = 1 << 20
	cc.mu.Unlock()
	require.NotNil(t, upgradeFrame(cc))
	require.Nil(t, upgradeFrame(cc), "dictionary must ship exactly once")
}

func TestActivationThenCompression(t *testing.T) {
	e, cc := newTestConn(t, DictionaryCompressionConfig{MinSamples: 8, DictionarySize: 1024}, ProtocolTypeJSON)
	cc.mu.Lock()
	cc.bytesSeen = 1 << 20
	cc.mu.Unlock()

	frame := sampleFrame(999)
	// Past the connect reply and the structure dictionary stage, which is where a
	// returning client starts.
	settleStructure(cc)

	before, beforeBinary, out, compressed := cc.Encode(frame)
	require.NotNil(t, before, "dictionary must be delivered before the first frame that needs it")
	require.True(t, compressed)
	require.Less(t, len(out), len(frame))

	// The activation frame must be readable without the channel dictionary, since
	// the client can not decode against it until it has applied it. By now the
	// built-in dictionary is in use, so the frame arrives under that one.
	require.True(t, beforeBinary)
	plain, err := structureCodec.Decompress(nil, before, 1<<20)
	require.NoError(t, err)
	reply, err := protocol.NewJSONReplyDecoder(plain).Decode()
	require.NoError(t, err)
	require.NotNil(t, reply.Push.State.Dictionary)
	require.NotEmpty(t, reply.Push.State.Dictionary.Id)

	codec, _ := e.current(testChannel2, protocol.TypeJSON)
	back, err := codec.Decompress(nil, out, 1<<20)
	require.NoError(t, err)
	require.Equal(t, frame, back)
}

// The dictionary is carried as base64 on JSON connections because a bytes field
// holds raw JSON there, and as raw bytes on Protobuf connections where that
// expansion is avoidable. Getting this backwards silently costs ~25% of the
// dictionary transfer, or fails to encode at all.
func TestActivationFrameEncodingPerProtocol(t *testing.T) {
	build := func(pt ProtocolType) ([]byte, *DictionaryCompressionEngine) {
		e, cc := newTestConn(t, DictionaryCompressionConfig{MinSamples: 64, DictionarySize: 4096}, pt)
		cc.mu.Lock()
		cc.bytesSeen = 1 << 20
		cc.mu.Unlock()
		// Sent under the built-in dictionary, which is what the client holds at
		// that point, so unwrap it the way a client would.
		frame, err := structureCodec.Decompress(nil, upgradeFrame(cc), 1<<20)
		require.NoError(t, err)
		return frame, e
	}

	jsonFrame, jsonEngine := build(ProtocolTypeJSON)
	require.NotNil(t, jsonFrame)
	jr, err := protocol.NewJSONReplyDecoder(jsonFrame).Decode()
	require.NoError(t, err)
	require.NotEmpty(t, jr.Push.State.Dictionary.DataB64, "JSON must carry base64 in data_b64")
	require.Empty(t, jr.Push.State.Dictionary.Data, "JSON must not set the raw bytes field")

	pbFrame, pbEngine := build(ProtocolTypeProtobuf)
	require.NotNil(t, pbFrame)
	pr, err := protocol.NewProtobufReplyDecoder(pbFrame).Decode()
	require.NoError(t, err)
	require.NotEmpty(t, pr.Push.State.Dictionary.Data, "Protobuf must carry raw bytes in data")
	require.Empty(t, pr.Push.State.Dictionary.DataB64, "Protobuf must not pay for base64")

	// Each protocol keeps its own dictionary: a JSON connection gains nothing
	// from one built out of Protobuf frames.
	jc, _ := jsonEngine.current(testChannel2, protocol.TypeJSON)
	require.NotNil(t, jc)
	pc, _ := pbEngine.current(testChannel2, protocol.TypeProtobuf)
	require.Equal(t, pc.Dict(), []byte(pr.Push.State.Dictionary.Data))
	require.Less(t, len(pbFrame), len(jsonFrame), "raw bytes must be cheaper than base64")
	t.Logf("activation frame: JSON %d B, Protobuf %d B (%.0f%% smaller)",
		len(jsonFrame), len(pbFrame), 100*(1-float64(len(pbFrame))/float64(len(jsonFrame))))
}

// The property that makes this affordable at scale: subscribers of a channel
// which are not batching all receive byte-identical frames, so one publication
// must cost one compression regardless of how many connections it reaches.
func TestFrameCacheCollapsesFanout(t *testing.T) {
	e, _ := newTestConn(t, DictionaryCompressionConfig{MinSamples: 8, DictionarySize: 1024}, ProtocolTypeJSON)
	codec, _ := e.current(testChannel2, protocol.TypeJSON)
	require.NotNil(t, codec)

	const pubs, subs = 50, 500
	for p := 0; p < pubs; p++ {
		frame := sampleFrame(10000 + p)
		for s := 0; s < subs; s++ {
			out := e.compress(codec, frame)
			back, err := codec.Decompress(nil, out, 1<<20)
			require.NoError(t, err)
			require.Equal(t, frame, back)
		}
	}
	hits, misses := e.cacheStats()
	require.Equal(t, int64(pubs), misses, "each publication must be compressed exactly once")
	require.Equal(t, int64(pubs*(subs-1)), hits)
	t.Logf("%d publications x %d subscribers: %d compressions, %d hits (%.0fx less work)",
		pubs, subs, misses, hits, float64(hits+misses)/float64(misses))
}

// An incompressible payload already costs no bandwidth, because the codec
// discards output that does not shrink the frame. Without a back-off it would
// still pay full compression CPU on every frame forever.
func TestBacksOffOnIncompressiblePayloads(t *testing.T) {
	e, cc := newTestConn(t, DictionaryCompressionConfig{MinSamples: 8, DictionarySize: 1024}, ProtocolTypeJSON)
	cc.mu.Lock()
	cc.bytesSeen = 1 << 20
	cc.mu.Unlock()
	require.NotNil(t, upgradeFrame(cc))

	seed := uint32(12345)
	frame := func() []byte {
		out := make([]byte, 256)
		for i := range out {
			seed = seed*1664525 + 1013904223
			out[i] = byte(seed >> 24)
		}
		return out
	}
	for i := 0; i < backOffMinSamples+2; i++ {
		cc.Encode(frame())
	}
	require.True(t, cc.backedOffNow(), "must stop compressing incompressible traffic")

	// While backed off frames still round trip: the receiver sees an ordinary
	// raw frame and needs no knowledge that the sender gave up.
	codec, _ := e.current(testChannel2, protocol.TypeJSON)
	f := frame()
	_, _, out, compressed := cc.Encode(f)
	require.True(t, compressed, "frames must still carry a codec marker")
	back, err := codec.Decompress(nil, out, 1<<20)
	require.NoError(t, err)
	require.Equal(t, f, back)
	require.Equal(t, 1, len(out)-len(f), "passthrough must cost exactly one marker byte")

	for i := 0; i < backOffProbeEvery+backOffMinSamples*3; i++ {
		cc.Encode(sampleFrame(20000 + i))
	}
	require.False(t, cc.backedOffNow(), "must resume once traffic is compressible again")
}

// staticCompression is a minimal third-party engine: a fixed dictionary shipped
// on the first frame, no sampling and no break-even guard. It exists to prove
// the interface is genuinely pluggable without reaching into internals.
type staticCompression struct{ dict []byte }

func (s *staticCompression) NewConnection(params ConnectionParams) ConnectionCompression {
	return &staticConn{codec: protocol.NewDeflateFrameCodec("static-v1", s.dict), protoType: params.ProtocolType.toProto(), dict: s.dict}
}

type staticConn struct {
	codec     *protocol.DeflateFrameCodec
	protoType protocol.Type
	dict      []byte
	sent      bool
}

func (s *staticConn) OnSubscribe(string)   {}
func (s *staticConn) OnUnsubscribe(string) {}

func (s *staticConn) Encode(frame []byte) ([]byte, bool, []byte, bool) {
	var before []byte
	if !s.sent {
		s.sent = true
		data, _ := protocol.DefaultJsonReplyEncoder.Encode(&protocol.Reply{Push: &protocol.Push{
			State: &protocol.ConnectionState{Dictionary: &protocol.Dictionary{
				Id: s.codec.ID(), DataB64: base64Std(s.dict),
			}},
		}})
		enc := protocol.GetDataEncoder(s.protoType)
		defer protocol.PutDataEncoder(s.protoType, enc)
		_ = enc.Encode(data)
		before = enc.Finish()
	}
	return before, false, s.codec.Compress(nil, frame), true
}

func TestCustomEngineIsPluggable(t *testing.T) {
	dict := bytes.Repeat([]byte(`{"push":{"channel":"demo","pub":{"data":`), 20)
	var engine DictionaryCompression = &staticCompression{dict: dict}

	cc := engine.NewConnection(ConnectionParams{ProtocolType: ProtocolTypeJSON, ClientFlags: ConnectionFlagDictionaryCompression})
	frame := sampleFrame(7)

	before, _, out, binary := cc.Encode(frame)
	require.NotNil(t, before, "custom engine must be able to ship its dictionary")
	require.True(t, binary)
	require.Less(t, len(out), len(frame))

	// A client decodes it exactly like the built-in engine's output.
	reply, err := protocol.NewJSONReplyDecoder(before).Decode()
	require.NoError(t, err)
	require.Equal(t, "static-v1", reply.Push.State.Dictionary.Id)

	codec := protocol.NewDeflateFrameCodec("static-v1", dict)
	back, err := codec.Decompress(nil, out, 1<<20)
	require.NoError(t, err)
	require.Equal(t, frame, back)

	before2, _, _, _ := cc.Encode(frame)
	require.Nil(t, before2, "dictionary must not be re-sent")
}

// A client which does not advertise support must keep working unchanged against
// a node with compression enabled. This is what makes the feature safe to switch
// on for a mixed fleet, so it is verified over a real connection.
func TestLegacyClientUnaffected(t *testing.T) {
	t.Parallel()
	n, _ := New(Config{
		DictionaryCompression: NewDictionaryCompressionEngine(DictionaryCompressionConfig{MinSamples: 2, DictionarySize: 512}),
	})
	n.OnConnecting(func(ctx context.Context, e ConnectEvent) (ConnectReply, error) {
		return ConnectReply{Credentials: &Credentials{UserID: "legacy"}}, nil
	})
	require.NoError(t, n.Run())
	defer func() { _ = n.Shutdown(context.Background()) }()

	mux := http.NewServeMux()
	mux.Handle("/connection/websocket", NewWebsocketHandler(n, WebsocketConfig{
		CheckOrigin: func(r *http.Request) bool { return true },
	}))
	server := httptest.NewServer(mux)
	defer server.Close()

	conn, resp, _, err := (&websocket.Dialer{}).Dial("ws"+server.URL[4:]+"/connection/websocket", nil)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	defer func() { _ = conn.Close() }()

	// Connect WITHOUT the feature flag, the way an older SDK would.
	data, err := protocol.NewJSONCommandEncoder().Encode(&protocol.Command{Id: 1, Connect: &protocol.ConnectRequest{}})
	require.NoError(t, err)
	require.NoError(t, conn.WriteMessage(websocket.TextMessage, data))

	_, reply, err := conn.ReadMessage()
	require.NoError(t, err)
	require.Equal(t, byte('{'), reply[0], "legacy client must not receive framed output")
	r, err := protocol.NewJSONReplyDecoder(reply).Decode()
	require.NoError(t, err)
	require.NotEmpty(t, r.Connect.Client)

	for i := 0; i < 200; i++ {
		_, err = n.Publish("legacy:test", []byte(`{"seq":1}`))
		require.NoError(t, err)
	}
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(300*time.Millisecond)))
	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			break // read deadline: nothing more arrived, which is fine
		}
		require.NotEqual(t, protocol.FrameCodecCompressed, msg[0], "legacy client received a compressed frame")
	}
}

func BenchmarkBackOffCheck(b *testing.B) {
	cc := &connectionCompression{}
	b.Run("compressing", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = cc.shouldCompress()
		}
	})
	cc.backedOff.Store(true)
	cc.probeIn.Store(backOffProbeEvery)
	b.Run("backed-off", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = cc.shouldCompress()
		}
	})
}

func BenchmarkCompressFanout(b *testing.B) {
	for _, subs := range []int{1, 100, 1000} {
		for _, cached := range []bool{false, true} {
			size := 8 << 20
			if !cached {
				size = -1
			}
			b.Run(fmt.Sprintf("subs=%d/cache=%v", subs, cached), func(b *testing.B) {
				e := NewDictionaryCompressionEngine(DictionaryCompressionConfig{
					MinSamples: 8, DictionarySize: 4096, FrameCacheSize: size,
				})
				cc := e.NewConnection(ConnectionParams{ProtocolType: ProtocolTypeJSON, ClientFlags: ConnectionFlagDictionaryCompression})
				for i := 0; i < 8; i++ {
					cc.Encode(sampleFrame(i))
				}
				codec, _ := e.current(testChannel2, protocol.TypeJSON)
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					frame := sampleFrame(i)
					for s := 0; s < subs; s++ {
						_ = e.compress(codec, frame)
					}
				}
			})
		}
	}
}

// The engine, not Centrifuge, decides whether it can serve a client. A client
// which did not advertise support must be left uncompressed - that is the whole
// safety property for a mixed fleet.
func TestEngineOwnsNegotiation(t *testing.T) {
	e := NewDictionaryCompressionEngine(DictionaryCompressionConfig{})
	require.Nil(t, e.NewConnection(ConnectionParams{ProtocolType: ProtocolTypeJSON}),
		"client advertising nothing must be left uncompressed")
	require.NotNil(t, e.NewConnection(ConnectionParams{ProtocolType: ProtocolTypeJSON, ClientFlags: ConnectionFlagDictionaryCompression}))
	require.NotNil(t, e.NewConnection(ConnectionParams{ProtocolType: ProtocolTypeJSON, ClientFlags: ConnectionFlagDictionaryCompression | 1<<9}),
		"unknown extra flags must not confuse the engine")
}

// A client can not assume the feature it advertised was accepted: the node may
// have no engine, or the engine may decline this connection. ConnectResult.Flag
// is how it finds out, so it must reflect reality in both directions.
func TestConnectReplyReportsAcceptance(t *testing.T) {
	t.Parallel()

	connect := func(n *Node, flag int64) *protocol.ConnectResult {
		mux := http.NewServeMux()
		mux.Handle("/connection/websocket", NewWebsocketHandler(n, WebsocketConfig{
			CheckOrigin: func(r *http.Request) bool { return true },
		}))
		server := httptest.NewServer(mux)
		defer server.Close()

		conn, resp, _, err := (&websocket.Dialer{}).Dial("ws"+server.URL[4:]+"/connection/websocket", nil)
		require.NoError(t, err)
		defer func() { _ = resp.Body.Close() }()
		defer func() { _ = conn.Close() }()

		data, err := protocol.NewJSONCommandEncoder().Encode(
			&protocol.Command{Id: 1, Connect: &protocol.ConnectRequest{Flag: flag}})
		require.NoError(t, err)
		require.NoError(t, conn.WriteMessage(websocket.TextMessage, data))
		_, reply, err := conn.ReadMessage()
		require.NoError(t, err)
		r, err := protocol.NewJSONReplyDecoder(reply).Decode()
		require.NoError(t, err)
		return r.Connect
	}

	newNode := func(engine DictionaryCompression) *Node {
		n, _ := New(Config{DictionaryCompression: engine})
		n.OnConnecting(func(ctx context.Context, e ConnectEvent) (ConnectReply, error) {
			return ConnectReply{Credentials: &Credentials{UserID: "u"}}, nil
		})
		require.NoError(t, n.Run())
		t.Cleanup(func() { _ = n.Shutdown(context.Background()) })
		return n
	}

	withEngine := newNode(NewDictionaryCompressionEngine(DictionaryCompressionConfig{}))
	res := connect(withEngine, ConnectionFlagDictionaryCompression)
	require.NotZero(t, res.Flag&ConnectionFlagDictionaryCompression,
		"server must confirm the feature it enabled")

	res = connect(withEngine, 0)
	require.Zero(t, res.Flag&ConnectionFlagDictionaryCompression,
		"a client which advertised nothing must not be told the feature is on")

	noEngine := newNode(nil)
	res = connect(noEngine, ConnectionFlagDictionaryCompression)
	require.Zero(t, res.Flag&ConnectionFlagDictionaryCompression,
		"a node without an engine must not confirm the feature")
}

// Dictionaries are per channel and never merged, so a channel that was not
// opted in contributes to nothing - and a channel that was contributes only to
// its own dictionary, which only its subscribers receive.
//
// This is the property the whole design rests on. A dictionary is built from
// real frames, so its bytes ARE payload: anything that merges channels hands one
// channel's content to another channel's subscribers.
func TestDictionariesNeverMixChannels(t *testing.T) {
	secret := "alice-balance-84210-EUR-and-her-home-address"
	e := NewDictionaryCompressionEngine(DictionaryCompressionConfig{
		MinSamples:     8,
		DictionarySize: 4096,
		UseChannelDictionary: func(ch string) bool {
			return strings.HasPrefix(ch, "personal:") || ch == "odds:board"
		},
		MaxChannelDictionaries: 64,
	})
	for i := 0; i < 20; i++ {
		e.observe(e.dictionaryChannel("personal:#alice"), protocol.TypeJSON,
			[]byte(fmt.Sprintf(`{"push":{"channel":"personal:#alice","pub":{"data":{"note":"%s","seq":%d}}}}`, secret, i)), 1)
		e.observe(e.dictionaryChannel("personal:#bob"), protocol.TypeJSON,
			[]byte(fmt.Sprintf(`{"push":{"channel":"personal:#bob","pub":{"data":{"note":"bob","seq":%d}}}}`, i)), 1)
		e.observe(e.dictionaryChannel("odds:board"), protocol.TypeJSON,
			[]byte(fmt.Sprintf(`{"push":{"channel":"odds:board","pub":{"data":{"market":"1x2","seq":%d}}}}`, i)), 1)
	}

	for _, ch := range []string{"personal:#bob", "odds:board"} {
		d, _ := e.current(ch, protocol.TypeJSON)
		require.NotNil(t, d, ch+" should have its own dictionary")
		require.NotContains(t, string(d.Dict()), secret,
			"a dictionary must never contain another channel's payload")
	}

	// A channel left out contributes nothing and has nothing.
	e.observe(e.dictionaryChannel("secret:room"), protocol.TypeJSON, []byte(`{"push":{"channel":"secret:room"}}`), 1)
	d, _ := e.current("secret:room", protocol.TypeJSON)
	require.Nil(t, d, "a channel not opted in must never get a dictionary")

	// And Bob is only ever offered a dictionary from a channel he subscribes to.
	bob := e.NewConnection(ConnectionParams{ProtocolType: ProtocolTypeJSON, ClientFlags: ConnectionFlagDictionaryCompression}).(*connectionCompression)
	bob.OnSubscribe("personal:#bob")
	bob.mu.Lock()
	bob.bytesSeen = 1 << 20
	bob.mu.Unlock()
	require.NotNil(t, upgradeFrame(bob))
	require.Equal(t, "personal:#bob", bob.chosen)
}

// A connection holds one dictionary but its frames batch several channels
// together, so the choice should cover the channel that dominates its traffic.
// Picking a quiet one instead measured 19-35% worse.
func TestConnectionPicksBusiestSubscribedChannel(t *testing.T) {
	e := NewDictionaryCompressionEngine(DictionaryCompressionConfig{
		MinSamples: 8, DictionarySize: 1024, UseChannelDictionary: useAll,
		MaxChannelDictionaries: 64,
	})
	// "hot" carries an order of magnitude more traffic than "quiet".
	for i := 0; i < 200; i++ {
		e.observe("hot", protocol.TypeJSON, sampleFrame(i), 1)
	}
	for i := 0; i < 20; i++ {
		e.observe("quiet", protocol.TypeJSON, sampleFrame(i), 1)
	}

	// Subscribed to the quiet one FIRST, so subscription order must not decide.
	cc := e.NewConnection(ConnectionParams{ProtocolType: ProtocolTypeJSON, ClientFlags: ConnectionFlagDictionaryCompression}).(*connectionCompression)
	cc.OnSubscribe("quiet")
	cc.OnSubscribe("hot")
	cc.mu.Lock()
	cc.bytesSeen = 1 << 20
	cc.mu.Unlock()
	require.NotNil(t, upgradeFrame(cc))
	require.Equal(t, "hot", cc.chosen, "the busiest subscribed channel must win, not the first")
}

// A connection subscribed only to channels without dictionaries gets none - it
// must never be handed one built from traffic it cannot read.
func TestConnectionWithoutEligibleChannelGetsNothing(t *testing.T) {
	e := NewDictionaryCompressionEngine(DictionaryCompressionConfig{
		MinSamples: 8, DictionarySize: 1024,
		UseChannelDictionary:   func(ch string) bool { return ch == "odds:board" },
		MaxChannelDictionaries: 64,
	})
	for i := 0; i < 16; i++ {
		e.observe("odds:board", protocol.TypeJSON, sampleFrame(i), 1)
	}
	cc := e.NewConnection(ConnectionParams{ProtocolType: ProtocolTypeJSON, ClientFlags: ConnectionFlagDictionaryCompression}).(*connectionCompression)
	cc.OnSubscribe("user:#42")
	cc.mu.Lock()
	cc.bytesSeen = 1 << 20
	cc.mu.Unlock()
	require.Nil(t, upgradeFrame(cc), "no subscribed channel with a dictionary means no dictionary")
}

// MaxChannelDictionaries is the operator's CPU-for-bandwidth dial. It has to hold
// however many channels appear, because a DEFLATE writer is bound to its
// dictionary and rarely-used pools get rebuilt at ~800 KB a time.
func TestMaxChannelDictionariesCapsCost(t *testing.T) {
	e := NewDictionaryCompressionEngine(DictionaryCompressionConfig{
		MinSamples: 4, DictionarySize: 512, MaxChannelDictionaries: 8,
		UseChannelDictionary: useAll,
	})
	for ch := 0; ch < 200; ch++ {
		for i := 0; i < 4; i++ {
			e.observe(fmt.Sprintf("room:%03d", ch), protocol.TypeJSON, sampleFrame(i), 1)
		}
	}
	require.LessOrEqual(t, len(e.Stats().Groups), 8, "must not exceed MaxChannelDictionaries")
	require.Greater(t, len(e.Stats().Groups), 0, "the cap must not disable the feature")
}

// Batching and per-channel dictionaries interact in a way worth pinning down.
//
// With ConnectReply.WriteDelay set, one frame carries messages from SEVERAL
// channels, but a connection holds exactly ONE dictionary - the busiest channel
// it subscribes to. So a mixed frame is compressed whole against a dictionary
// that only matches part of it.
//
// That is correct and safe: compression is lossless whatever dictionary is used,
// the client decompresses with the same one, and the dictionary's content is
// something this connection was already entitled to read. Only the ratio varies,
// and even a partly-matching dictionary beats none - a batched frame's
// cross-message redundancy carries most of the weight.
func TestBatchedFramesMixChannelsUnderOneDictionary(t *testing.T) {
	t.Parallel()
	const withDict, without = "odds:board", "user:#42"

	n, _ := New(Config{
		DictionaryCompression: NewDictionaryCompressionEngine(DictionaryCompressionConfig{
			MinSamples:             8,
			DictionarySize:         1024,
			UseChannelDictionary:   func(ch string) bool { return ch == withDict },
			MaxChannelDictionaries: 64,
		}),
	})
	n.OnConnecting(func(ctx context.Context, e ConnectEvent) (ConnectReply, error) {
		return ConnectReply{
			Credentials:        &Credentials{UserID: "u"},
			WriteDelay:         30 * time.Millisecond, // force multi-message frames
			MaxMessagesInFrame: 16,
		}, nil
	})
	n.OnConnect(func(c *Client) {
		c.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) { cb(SubscribeReply{}, nil) })
	})
	require.NoError(t, n.Run())
	defer func() { _ = n.Shutdown(context.Background()) }()

	mux := http.NewServeMux()
	mux.Handle("/connection/websocket", NewWebsocketHandler(n, WebsocketConfig{
		CheckOrigin: func(r *http.Request) bool { return true },
		// This raw client does not answer pings, and a pong with no preceding
		// ping is itself grounds for disconnect - so move pings past the test.
		PingPongConfig: PingPongConfig{PingInterval: 10 * time.Minute, PongTimeout: time.Minute},
	}))
	server := httptest.NewServer(mux)
	defer server.Close()

	conn, resp, _, err := (&websocket.Dialer{}).Dial("ws"+server.URL[4:]+"/connection/websocket", nil)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	defer func() { _ = conn.Close() }()

	send := func(cmd *protocol.Command) {
		data, err := protocol.NewJSONCommandEncoder().Encode(cmd)
		require.NoError(t, err)
		require.NoError(t, conn.WriteMessage(websocket.TextMessage, data))
	}
	send(&protocol.Command{Id: 1, Connect: &protocol.ConnectRequest{Flag: ConnectionFlagDictionaryCompression}})
	send(&protocol.Command{Id: 2, Subscribe: &protocol.SubscribeRequest{Channel: withDict}})
	send(&protocol.Command{Id: 3, Subscribe: &protocol.SubscribeRequest{Channel: without}})

	const perChannel = 300
	var codec, pending *protocol.DeflateFrameCodec
	got := map[string]int{}
	batched, compressed, mixedFrames, subsReady := 0, 0, 0, 0
	published := false
	deadline := time.Now().Add(20 * time.Second)

	for got[withDict]+got[without] < perChannel*2 && time.Now().Before(deadline) {
		require.NoError(t, conn.SetReadDeadline(time.Now().Add(5*time.Second)))
		_, msg, err := conn.ReadMessage()
		require.NoError(t, err)

		if codec != nil {
			msg, err = codec.Decompress(nil, msg, 1<<20)
			require.NoError(t, err, "batched frame must decompress cleanly")
			compressed++
		}
		lines := bytes.Split(bytes.TrimRight(msg, "\n"), []byte("\n"))
		if len(lines) > 1 {
			batched++
		}
		seenHere := map[string]bool{}
		for _, line := range lines {
			r, err := protocol.NewJSONReplyDecoder(line).Decode()
			require.NoError(t, err)
			if r.Push != nil && r.Push.State != nil && r.Push.State.Dictionary != nil {
				raw, derr := base64.StdEncoding.DecodeString(r.Push.State.Dictionary.DataB64)
				require.NoError(t, derr)
				// The first dictionary of a connection travels deflated: its
				// delivery frame could not be compressed, since nothing was
				// installed yet to compress it against.
				if r.Push.State.Dictionary.Flags&protocol.DictionaryFlagDeflate != 0 {
					raw, derr = protocol.InflateDictionary(raw, 1<<20)
					require.NoError(t, derr)
				}
				pending = protocol.NewDeflateFrameCodec(r.Push.State.Dictionary.Id, raw)
			}
			if r.Push != nil && r.Push.Pub != nil {
				got[r.Push.Channel]++
				seenHere[r.Push.Channel] = true
			}
			if r.Id == 2 || r.Id == 3 {
				subsReady++
			}
		}
		if len(seenHere) > 1 {
			mixedFrames++
		}
		if pending != nil {
			codec, pending = pending, nil
		}
		// Publish only once both subscriptions are confirmed, or the first
		// publications land before the client is subscribed and never arrive.
		if subsReady >= 2 && !published {
			published = true
			go func() {
				for i := 0; i < perChannel; i++ {
					_, _ = n.Publish(withDict, []byte(fmt.Sprintf(`{"market":"1x2","seq":%d}`, i)))
					_, _ = n.Publish(without, []byte(fmt.Sprintf(`{"type":"balance","seq":%d}`, i)))
				}
			}()
		}
	}

	require.Equal(t, perChannel, got[withDict], "every publication on the dictionary channel must arrive")
	require.Equal(t, perChannel, got[without], "every publication on the other channel must arrive")
	require.Greater(t, batched, 0, "WriteDelay should have produced multi-message frames")
	require.Greater(t, compressed, 0, "compression should have activated")
	require.Greater(t, mixedFrames, 0,
		"a frame carrying both channels is the case under test - one dictionary, mixed content")
	t.Logf("%d frames batched, %d compressed, %d carried both channels at once",
		batched, compressed, mixedFrames)
}

// Slots must go to the channels that are busy NOW, not to whichever appeared
// first. Without decay and admission, four short-lived rooms that opened early
// hold the budget forever and a genuinely busy channel arriving later gets
// nothing - which is the opposite of what MaxChannelDictionaries promises.
func TestBusyChannelTakesSlotFromQuietOne(t *testing.T) {
	e := NewDictionaryCompressionEngine(DictionaryCompressionConfig{
		MinSamples: 4, DictionarySize: 512, MaxChannelDictionaries: 4,
		UseChannelDictionary: useAll,
	})
	// Four rooms open first, carry a trickle, then go silent forever.
	for r := 0; r < 4; r++ {
		for i := 0; i < 8; i++ {
			e.observe(fmt.Sprintf("room:old%d", r), protocol.TypeJSON, sampleFrame(i), 1)
		}
	}
	require.Equal(t, 4, len(e.Stats().Groups), "the early rooms take the free slots")

	// A genuinely busy channel appears afterwards and carries far more traffic.
	for i := 0; i < 20000; i++ {
		e.observe("odds:board", protocol.TypeJSON, sampleFrame(i), 1)
	}

	_, held := e.Stats().Groups["odds:board/json"]
	require.True(t, held, "the busiest channel must end up holding a slot")
	d, _ := e.current("odds:board", protocol.TypeJSON)
	require.NotNil(t, d, "and must actually have a dictionary")
	require.LessOrEqual(t, len(e.Stats().Groups), 4, "still within MaxChannelDictionaries")
}

// Evicting a dictionary must not disturb connections already using it: they hold
// the codec directly, so eviction only means new connections stop being given it.
func TestEvictionDoesNotBreakExistingConnections(t *testing.T) {
	e := NewDictionaryCompressionEngine(DictionaryCompressionConfig{
		MinSamples: 4, DictionarySize: 512, MaxChannelDictionaries: 1,
		UseChannelDictionary: useAll,
	})
	for i := 0; i < 8; i++ {
		e.observe("quiet", protocol.TypeJSON, sampleFrame(i), 1)
	}
	cc := e.NewConnection(ConnectionParams{ProtocolType: ProtocolTypeJSON, ClientFlags: ConnectionFlagDictionaryCompression}).(*connectionCompression)
	cc.OnSubscribe("quiet")
	cc.mu.Lock()
	cc.bytesSeen = 1 << 20
	cc.mu.Unlock()
	require.NotNil(t, upgradeFrame(cc), "connection gets the quiet channel's dictionary")

	// A busier channel takes the only slot.
	for i := 0; i < 20000; i++ {
		e.observe("busy", protocol.TypeJSON, sampleFrame(i), 1)
	}
	_, stillHeld := e.Stats().Groups["quiet/json"]
	require.False(t, stillHeld, "the quiet channel should have lost its slot")

	// The existing connection keeps compressing and round tripping regardless.
	frame := sampleFrame(999)
	_, _, out, compressed := cc.Encode(frame)
	require.True(t, compressed)
	cc.mu.RLock()
	codec := cc.codec
	cc.mu.RUnlock()
	back, err := codec.Decompress(nil, out, 1<<20)
	require.NoError(t, err)
	require.Equal(t, frame, back, "an evicted dictionary must keep working for its users")
}

// Slot allocation must rank channels by bytes leaving the machine, not by
// publish rate. A channel with many subscribers and a modest rate moves far more
// traffic than a busy one nobody is listening to, and a dictionary is worth the
// slot in proportion to what it saves.
func TestSlotsRankByEgressNotPublishRate(t *testing.T) {
	e := NewDictionaryCompressionEngine(DictionaryCompressionConfig{
		MinSamples: 4, DictionarySize: 512, MaxChannelDictionaries: 1,
		UseChannelDictionary: useAll,
	})
	// "lonely" publishes 20x more often, but to a single subscriber.
	for i := 0; i < 4000; i++ {
		e.observe("lonely", protocol.TypeJSON, sampleFrame(i), 1)
	}
	// "popular" publishes far less, but reaches 500 subscribers each time.
	for i := 0; i < 200; i++ {
		e.observe("popular", protocol.TypeJSON, sampleFrame(i), 500)
	}

	_, popularHeld := e.Stats().Groups["popular/json"]
	require.True(t, popularHeld,
		"the channel responsible for more egress must hold the slot, "+
			"even though it publishes 20x less often")
	require.Equal(t, 1, len(e.Stats().Groups))
}

// Leaving a channel must stop it being a dictionary source immediately.
//
// A dictionary is built over time from live traffic, so one handed out after a
// connection lost access would carry content published after the revocation -
// content it was never entitled to see. This is the admin-revokes-access case.
func TestNoDictionaryFromChannelAfterUnsubscribe(t *testing.T) {
	e := NewDictionaryCompressionEngine(DictionaryCompressionConfig{
		MinSamples: 8, DictionarySize: 2048, UseChannelDictionary: useAll,
		MaxChannelDictionaries: 64,
	})
	cc := e.NewConnection(ConnectionParams{ProtocolType: ProtocolTypeJSON, ClientFlags: ConnectionFlagDictionaryCompression}).(*connectionCompression)
	cc.OnSubscribe("secret:room")
	cc.OnUnsubscribe("secret:room") // access revoked

	// Traffic published AFTER the revocation shapes the dictionary.
	afterRevocation := "board-decision-taken-after-alice-was-removed"
	for i := 0; i < 20; i++ {
		e.observe("secret:room", protocol.TypeJSON, []byte(fmt.Sprintf(
			`{"push":{"channel":"secret:room","pub":{"data":{"note":"%s","seq":%d}}}}`, afterRevocation, i)), 1)
	}
	built, _ := e.current("secret:room", protocol.TypeJSON)
	require.NotNil(t, built, "the channel still builds a dictionary for its real subscribers")
	require.Contains(t, string(built.Dict()), afterRevocation,
		"and it does contain post-revocation content, which is exactly the risk")

	cc.mu.Lock()
	cc.bytesSeen = 1 << 20
	cc.mu.Unlock()
	require.Nil(t, upgradeFrame(cc),
		"a connection that left the channel must never be handed its dictionary")

	// Re-subscribing makes it a legitimate source again.
	cc.OnSubscribe("secret:room")
	require.NotNil(t, upgradeFrame(cc))
	require.Equal(t, "secret:room", cc.chosen)
}

// A dictionary already delivered is left in place on unsubscribe. Its bytes have
// been sent and its content is frozen at build time, so keeping it discloses
// nothing further - while swapping it would cost another transfer for nothing.
func TestActiveDictionarySurvivesUnsubscribe(t *testing.T) {
	e, cc := newTestConn(t, DictionaryCompressionConfig{MinSamples: 8, DictionarySize: 1024}, ProtocolTypeJSON)
	cc.mu.Lock()
	cc.bytesSeen = 1 << 20
	cc.mu.Unlock()
	require.NotNil(t, upgradeFrame(cc))

	cc.OnUnsubscribe(testChannel2)

	frame := sampleFrame(4242)
	_, _, out, compressed := cc.Encode(frame)
	require.True(t, compressed, "an active connection keeps compressing")
	codec, _ := e.current(testChannel2, protocol.TypeJSON)
	back, err := codec.Decompress(nil, out, 1<<20)
	require.NoError(t, err)
	require.Equal(t, frame, back)
}

// The feature has exactly one hard ordering requirement: the frame carrying the
// dictionary must reach the client before the first frame compressed with it.
// Get that wrong and the stream is undecodable, not merely inefficient.
//
// It is structural rather than hoped for - the activation frame is written
// immediately before the payload frame on the same goroutine, and all writes for
// a connection are serialized. This hammers the path from many publishers at
// once, which is when an ordering mistake would show up.
func TestActivationOrderingUnderConcurrentPublishers(t *testing.T) {
	t.Parallel()
	const channel = "odds:board"

	n, _ := New(Config{
		DictionaryCompression: NewDictionaryCompressionEngine(DictionaryCompressionConfig{
			MinSamples:             8,
			DictionarySize:         1024,
			UseChannelDictionary:   func(ch string) bool { return ch == channel },
			MaxChannelDictionaries: 64,
		}),
	})
	n.OnConnecting(func(ctx context.Context, e ConnectEvent) (ConnectReply, error) {
		return ConnectReply{Credentials: &Credentials{UserID: "u"}}, nil
	})
	n.OnConnect(func(c *Client) {
		c.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) { cb(SubscribeReply{}, nil) })
	})
	require.NoError(t, n.Run())
	defer func() { _ = n.Shutdown(context.Background()) }()

	mux := http.NewServeMux()
	mux.Handle("/connection/websocket", NewWebsocketHandler(n, WebsocketConfig{
		CheckOrigin:    func(r *http.Request) bool { return true },
		PingPongConfig: PingPongConfig{PingInterval: 10 * time.Minute, PongTimeout: time.Minute},
	}))
	server := httptest.NewServer(mux)
	defer server.Close()

	conn, resp, _, err := (&websocket.Dialer{}).Dial("ws"+server.URL[4:]+"/connection/websocket", nil)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	defer func() { _ = conn.Close() }()

	send := func(cmd *protocol.Command) {
		data, err := protocol.NewJSONCommandEncoder().Encode(cmd)
		require.NoError(t, err)
		require.NoError(t, conn.WriteMessage(websocket.TextMessage, data))
	}
	send(&protocol.Command{Id: 1, Connect: &protocol.ConnectRequest{Flag: ConnectionFlagDictionaryCompression}})
	send(&protocol.Command{Id: 2, Subscribe: &protocol.SubscribeRequest{Channel: channel}})

	const publishers, perPublisher = 8, 150
	const total = publishers * perPublisher
	var codec, pending *protocol.DeflateFrameCodec
	got, subsReady := 0, 0
	published := false
	deadline := time.Now().Add(30 * time.Second)

	for got < total && time.Now().Before(deadline) {
		require.NoError(t, conn.SetReadDeadline(time.Now().Add(10*time.Second)))
		_, msg, err := conn.ReadMessage()
		require.NoError(t, err)
		if codec != nil {
			msg, err = codec.Decompress(nil, msg, 1<<20)
			// A compressed frame arriving before its dictionary would fail here.
			require.NoError(t, err, "frame must decode - ordering violation if not")
		}
		for _, line := range bytes.Split(bytes.TrimRight(msg, "\n"), []byte("\n")) {
			r, err := protocol.NewJSONReplyDecoder(line).Decode()
			require.NoError(t, err, "every frame must parse cleanly")
			if r.Push != nil && r.Push.State != nil && r.Push.State.Dictionary != nil {
				raw, derr := base64.StdEncoding.DecodeString(r.Push.State.Dictionary.DataB64)
				require.NoError(t, derr)
				// The first dictionary of a connection travels deflated: its
				// delivery frame could not be compressed, since nothing was
				// installed yet to compress it against.
				if r.Push.State.Dictionary.Flags&protocol.DictionaryFlagDeflate != 0 {
					raw, derr = protocol.InflateDictionary(raw, 1<<20)
					require.NoError(t, derr)
				}
				pending = protocol.NewDeflateFrameCodec(r.Push.State.Dictionary.Id, raw)
			}
			if r.Push != nil && r.Push.Pub != nil {
				got++
			}
			if r.Id == 2 {
				subsReady++
			}
		}
		if pending != nil {
			codec, pending = pending, nil
		}
		if subsReady > 0 && !published {
			published = true
			for p := 0; p < publishers; p++ {
				go func(p int) {
					for i := 0; i < perPublisher; i++ {
						_, _ = n.Publish(channel, []byte(fmt.Sprintf(`{"p":%d,"seq":%d}`, p, i)))
					}
				}(p)
			}
		}
	}
	require.Equal(t, total, got, "every publication must arrive intact")
	require.NotNil(t, codec, "compression should have activated during the run")
}

// A dictionary is a verbatim sample of past traffic, so a subscriber that earns
// one receives fragments of messages published before they joined - even on a
// channel keeping no history - and the dictionary is never rebuilt, so that
// snapshot reaches every future subscriber too.
//
// This is inherent to learning from real messages, not a defect to be fixed, and
// it is exactly why UseChannelDictionary is a disclosure decision: opt in only
// channels whose past is safe for their future. The test exists so the behaviour
// cannot change silently.
func TestDictionaryDisclosesPastTrafficToLateJoiners(t *testing.T) {
	e := NewDictionaryCompressionEngine(DictionaryCompressionConfig{
		MinSamples: 8, DictionarySize: 2048, UseChannelDictionary: useAll,
		MaxChannelDictionaries: 64,
	})
	past := "merger-with-acme-signed-price-42M"
	for i := 0; i < 30; i++ {
		e.observe("room:board", protocol.TypeJSON, []byte(fmt.Sprintf(
			`{"push":{"channel":"room:board","pub":{"data":{"msg":"%s","seq":%d}}}}`, past, i)), 5)
	}

	// Someone joining only now, with no history on the channel.
	newcomer := e.NewConnection(ConnectionParams{ProtocolType: ProtocolTypeJSON, ClientFlags: ConnectionFlagDictionaryCompression}).(*connectionCompression)
	newcomer.OnSubscribe("room:board")
	newcomer.mu.Lock()
	newcomer.bytesSeen = 1 << 20
	newcomer.mu.Unlock()
	require.NotNil(t, upgradeFrame(newcomer))

	codec, _ := e.current("room:board", protocol.TypeJSON)
	require.Contains(t, string(codec.Dict()), past,
		"a late joiner's dictionary carries content from before they subscribed - "+
			"opt in only channels whose past is safe for their future")

	// And it is a permanent snapshot: later traffic never replaces it.
	for i := 0; i < 5000; i++ {
		e.observe("room:board", protocol.TypeJSON, []byte(fmt.Sprintf(
			`{"push":{"channel":"room:board","pub":{"data":{"msg":"later chatter","seq":%d}}}}`, i)), 5)
	}
	later, _ := e.current("room:board", protocol.TypeJSON)
	require.Equal(t, codec.ID(), later.ID(), "dictionaries are built once and never rebuilt")
}

// ServerTagsFilter is set by the server and cannot be overridden by the client,
// so it withholds publications a subscriber may not see. The channel's dictionary
// is built from ALL of them, so handing it to a filtered subscriber would route
// straight around the filter. Such a subscription must not be a dictionary source.
func TestServerFilteredSubscriptionIsNotADictionarySource(t *testing.T) {
	t.Parallel()
	const channel = "market:all"
	restricted := "US-only-position-limit-breach-account-8812"

	newNode := func(withFilter bool) (*Node, *DictionaryCompressionEngine) {
		e := NewDictionaryCompressionEngine(DictionaryCompressionConfig{
			MinSamples: 4, DictionarySize: 1024, UseChannelDictionary: useAll,
			MaxChannelDictionaries: 64,
		})
		n, _ := New(Config{DictionaryCompression: e})
		n.OnConnecting(func(ctx context.Context, ev ConnectEvent) (ConnectReply, error) {
			return ConnectReply{Credentials: &Credentials{UserID: "u"}}, nil
		})
		n.OnConnect(func(c *Client) {
			c.OnSubscribe(func(ev SubscribeEvent, cb SubscribeCallback) {
				opts := SubscribeOptions{}
				if withFilter {
					opts.ServerTagsFilter = &FilterNode{Op: "", Key: "region", Cmp: "eq", Val: "EU"}
				}
				cb(SubscribeReply{Options: opts}, nil)
			})
		})
		require.NoError(t, n.Run())
		t.Cleanup(func() { _ = n.Shutdown(context.Background()) })
		return n, e
	}

	// Whether a filtered subscriber ends up with a dictionary is observable from
	// the connection's own compression state.
	check := func(withFilter bool) bool {
		n, e := newNode(withFilter)
		mux := http.NewServeMux()
		mux.Handle("/connection/websocket", NewWebsocketHandler(n, WebsocketConfig{
			CheckOrigin:    func(r *http.Request) bool { return true },
			PingPongConfig: PingPongConfig{PingInterval: 10 * time.Minute, PongTimeout: time.Minute},
		}))
		server := httptest.NewServer(mux)
		defer server.Close()

		conn, resp, _, err := (&websocket.Dialer{}).Dial("ws"+server.URL[4:]+"/connection/websocket", nil)
		require.NoError(t, err)
		defer func() { _ = resp.Body.Close() }()
		defer func() { _ = conn.Close() }()

		send := func(cmd *protocol.Command) {
			data, err := protocol.NewJSONCommandEncoder().Encode(cmd)
			require.NoError(t, err)
			require.NoError(t, conn.WriteMessage(websocket.TextMessage, data))
		}
		send(&protocol.Command{Id: 1, Connect: &protocol.ConnectRequest{Flag: ConnectionFlagDictionaryCompression}})
		send(&protocol.Command{Id: 2, Subscribe: &protocol.SubscribeRequest{Channel: channel}})

		gotDict := false
		deadline := time.Now().Add(6 * time.Second)
		published := false
		var codec, pending *protocol.DeflateFrameCodec
		for time.Now().Before(deadline) {
			require.NoError(t, conn.SetReadDeadline(time.Now().Add(1500*time.Millisecond)))
			_, msg, err := conn.ReadMessage()
			if err != nil {
				break
			}
			if codec != nil {
				msg, err = codec.Decompress(nil, msg, 1<<20)
				require.NoError(t, err)
			}
			for _, line := range bytes.Split(bytes.TrimRight(msg, "\n"), []byte("\n")) {
				r, derr := protocol.NewJSONReplyDecoder(line).Decode()
				if derr != nil {
					continue
				}
				if r.Push != nil && r.Push.State != nil && r.Push.State.Dictionary != nil {
					gotDict = true
				}
				if r.Id == 2 && !published {
					published = true
					go func() {
						for i := 0; i < 400; i++ {
							_, _ = n.Publish(channel, []byte(fmt.Sprintf(
								`{"note":"%s","seq":%d}`, restricted, i)), WithTags(map[string]string{"region": "US"}))
						}
					}()
				}
			}
			if pending != nil {
				codec, pending = pending, nil
			}
			if gotDict {
				break
			}
		}
		// The dictionary itself does get built from the unfiltered traffic.
		d, _ := e.current(channel, protocol.TypeJSON)
		if d != nil {
			require.Contains(t, string(d.Dict()), restricted,
				"the dictionary is built from all publications, filtered or not")
		}
		return gotDict
	}

	require.False(t, check(true),
		"a server-filtered subscriber must never receive the channel dictionary")
}

// The dictionary is withheld until a connection has carried enough traffic to
// earn it back, and how much that is depends on what the dictionary is worth.
// Measuring beats assuming: a pessimistic assumption delays every connection
// several fold, and quiet ones may never reach it at all.
func TestBreakEvenUsesMeasuredRatio(t *testing.T) {
	e := NewDictionaryCompressionEngine(DictionaryCompressionConfig{
		MinSamples: 32, DictionarySize: 1024, UseChannelDictionary: useAll,
		MaxChannelDictionaries: 64,
	})
	for i := 0; i < 32; i++ {
		e.observe("odds:board", protocol.TypeJSON, sampleFrame(i), 1)
	}
	codec, _, ratio, frameCost := e.currentWithRatio("odds:board", protocol.TypeJSON)
	require.NotNil(t, codec)
	require.Greater(t, ratio, seedRatio,
		"a real dictionary should beat the pessimistic assumption by a wide margin")
	require.Less(t, frameCost, len(codec.Dict()),
		"the activation frame is compressed, so it must cost less than the dictionary itself")

	// A connection carrying only what the measured ratio justifies gets it; the
	// old assumed threshold would have been several times higher.
	need := float64(frameCost) * shipMargin / ratio
	assumed := float64(frameCost) * shipMargin / seedRatio
	require.Less(t, need, assumed/2, "measuring should more than halve the wait")

	cc := e.NewConnection(ConnectionParams{ProtocolType: ProtocolTypeJSON, ClientFlags: ConnectionFlagDictionaryCompression}).(*connectionCompression)
	cc.OnSubscribe("odds:board")
	cc.mu.Lock()
	cc.bytesSeen = int(need) - 1
	cc.mu.Unlock()
	require.Nil(t, upgradeFrame(cc), "just below break-even, still withheld")

	cc.mu.Lock()
	cc.bytesSeen = int(need) + 1
	cc.mu.Unlock()
	require.NotNil(t, upgradeFrame(cc), "at break-even, sent")
	t.Logf("measured ratio %.0f%%, delivery %d B (dictionary is %d B): break-even %.1f KB instead of %.1f KB",
		ratio*100, frameCost, len(codec.Dict()), need/1024, assumed/1024)
}

// The built-in dictionary is what a connection has before - and possibly
// instead of - a learned one. It is compiled into the server and every SDK, so
// it costs nothing to deliver and applies from the frame after connect. This
// measures what that is worth on traffic shapes it was never trained on.
func TestBuiltinDictionaryHelpsWithoutTraining(t *testing.T) {
	shapes := map[string][]byte{
		"chat":      []byte(`{"push":{"channel":"room:42","pub":{"data":{"id":"m-90183","user":"alice","text":"see you at six","createdAt":"2026-08-05T09:14:22Z"},"offset":9013}}}`),
		"odds":      []byte(`{"push":{"channel":"odds:board","pub":{"data":{"eventId":"evt-100317","market":"1x2","home":"Arsenal","away":"Chelsea","odds":{"h":2.15,"d":3.4,"a":3.05},"ts":1785920062114}}}}`),
		"telemetry": []byte(`{"push":{"channel":"dev:8821","pub":{"data":{"deviceId":"dev-8821","status":"online","temp":21.4,"battery":88,"timestamp":1785920062114}}}}`),
	}
	for name, frame := range shapes {
		out := structureCodec.Compress(nil, frame)
		back, err := structureCodec.Decompress(nil, out, 1<<20)
		require.NoError(t, err)
		require.Equal(t, frame, back)
		ratio := float64(len(frame)) / float64(len(out))
		require.Greater(t, ratio, 1.15,
			"the built-in dictionary must earn its place on untrained traffic: %s", name)
		t.Logf("%-10s %d B -> %d B (%.2fx)", name, len(frame), len(out), ratio)
	}
}

// A dictionary's id is a hash of its content. That is what makes client-side
// caching safe: a client advertising an id and a server holding it necessarily
// have identical bytes, so a dictionary that changes becomes an ordinary cache
// miss rather than silent corruption.
func TestStructureDictionaryIDIsContentHash(t *testing.T) {
	require.Equal(t, protocol.DictionaryID(protocol.StructureDictionary), protocol.StructureDictionaryID)
	require.Equal(t, protocol.StructureDictionaryID, protocol.StructureFrameCodec().ID())

	changed := append(append([]byte(nil), protocol.StructureDictionary...), '!')
	require.NotEqual(t, protocol.StructureDictionaryID, protocol.DictionaryID(changed),
		"a changed dictionary must get a new id, or cached clients would decode against the wrong bytes")
}

// An operator may supply their own structure dictionary. It is delivered and
// cached exactly like the default, with no coordinated client rollout, because
// its identity comes from its content.
func TestCustomStructureDictionary(t *testing.T) {
	custom := bytes.Repeat([]byte(`{"orderId":"","symbol":"","side":"","qty":,"price":`), 20)
	e := NewDictionaryCompressionEngine(DictionaryCompressionConfig{StructureDictionary: custom})
	codec := e.structureCodec()
	require.NotNil(t, codec)
	require.Equal(t, protocol.DictionaryID(custom), codec.ID())
	require.Equal(t, custom, codec.Dict())

	off := NewDictionaryCompressionEngine(DictionaryCompressionConfig{DisableStructureDictionary: true})
	require.Nil(t, off.structureCodec())
}

// A client that kept the structure dictionary from an earlier connection gets
// it back for the price of naming an id, so it compresses from the first frame.
// This is the whole point of caching it: short and quiet connections are exactly
// the ones that can never earn a dictionary they have to be sent.
func TestHeldStructureDictionaryActivatesImmediately(t *testing.T) {
	e := NewDictionaryCompressionEngine(DictionaryCompressionConfig{
		UseChannelDictionary:   func(string) bool { return false },
		MaxChannelDictionaries: 64,
	})
	cc := e.NewConnection(ConnectionParams{
		ProtocolType:      ProtocolTypeJSON,
		ClientFlags:       ConnectionFlagDictionaryCompression,
		HeldDictionaryIDs: []string{protocol.StructureDictionaryID},
	}).(*connectionCompression)

	var raw, sent int
	for i := 0; i < 20; i++ {
		frame := sampleFrame(i)
		before, _, out, _ := cc.Encode(frame)
		raw += len(frame)
		sent += len(out) + len(before)
	}
	require.Less(t, sent, raw, "a returning client must compress from the start")
	t.Logf("client already held the structure dictionary: %d B -> %d B (%.2fx)",
		raw, sent, float64(raw)/float64(sent))

	// Naming it must cost far less than sending it.
	held := e.NewConnection(ConnectionParams{ProtocolType: ProtocolTypeJSON,
		ClientFlags:       ConnectionFlagDictionaryCompression,
		HeldDictionaryIDs: []string{protocol.StructureDictionaryID}}).(*connectionCompression)
	held.mu.Lock()
	held.started = true
	held.mu.Unlock()
	f, _ := held.activationFrame()
	require.NotNil(t, f)
	require.Less(t, len(f), e.structureFrameCost(protocol.TypeJSON)/4,
		"announcing a cached dictionary by id must be far cheaper than sending it")
	t.Logf("announce by id: %d B versus %d B to send it",
		len(f), e.structureFrameCost(protocol.TypeJSON))
}

// A client that has never seen the structure dictionary has to be sent it, so it
// is weighed like any other dictionary. Sending it regardless would make short
// connections worse off - measured, traffic nearly doubling.
func TestColdClientEarnsStructureDictionary(t *testing.T) {
	e := NewDictionaryCompressionEngine(DictionaryCompressionConfig{
		UseChannelDictionary:   func(string) bool { return false },
		MaxChannelDictionaries: 64,
	})
	cc := e.NewConnection(ConnectionParams{
		ProtocolType: ProtocolTypeJSON,
		ClientFlags:  ConnectionFlagDictionaryCompression,
	}).(*connectionCompression)

	for i := 0; i < 20; i++ {
		before, _, _, compressed := cc.Encode(sampleFrame(i))
		require.Nil(t, before, "nothing may be sent this early")
		require.False(t, compressed)
	}

	cc.mu.Lock()
	cc.bytesSeen = 1 << 22
	cc.mu.Unlock()
	before, _, _, _ := cc.Encode(sampleFrame(99))
	require.NotNil(t, before, "once it has carried enough, the dictionary is sent")
	require.Greater(t, len(before), e.structureFrameCost(protocol.TypeJSON)/2,
		"a client that does not hold it must receive the content")
}

// A dictionary is a concatenation of real message samples, so it is ordinary
// text and compresses well. The frame delivering it used to go out verbatim on
// the reasoning that the client could not decode it yet - true only for the
// first dictionary. Once one is installed the next can be compressed against it,
// because the client decodes the frame before installing what it carries.
//
// This is the difference between a dictionary paying for itself in ~30 frames
// and ~120, so it is worth a guard.
func TestActivationFrameIsCompressed(t *testing.T) {
	for _, tc := range []struct {
		name  string
		proto ProtocolType
	}{{"JSON", ProtocolTypeJSON}, {"Protobuf", ProtocolTypeProtobuf}} {
		t.Run(tc.name, func(t *testing.T) {
			e, cc := newTestConn(t, DictionaryCompressionConfig{
				MinSamples: 64, DictionarySize: 4096}, tc.proto)
			cc.mu.Lock()
			cc.bytesSeen = 1 << 24
			cc.mu.Unlock()

			frame := upgradeFrame(cc)
			require.NotNil(t, frame)

			codec, _ := e.current(testChannel2, protocol.Type(tc.proto))
			require.NotNil(t, codec)
			require.Less(t, len(frame), len(codec.Dict()),
				"the delivery frame must cost less than the dictionary it carries")

			// It must still decode with the dictionary the client holds at that
			// point, which is the structure dictionary.
			plain, err := structureCodec.Decompress(nil, frame, 1<<20)
			require.NoError(t, err)
			var id string
			if tc.proto == ProtocolTypeJSON {
				r, derr := protocol.NewJSONReplyDecoder(plain).Decode()
				require.NoError(t, derr)
				id = r.Push.State.Dictionary.Id
			} else {
				r, derr := protocol.NewProtobufReplyDecoder(plain).Decode()
				require.NoError(t, derr)
				id = r.Push.State.Dictionary.Id
			}
			require.Equal(t, codec.ID(), id)
			t.Logf("%s: %d B dictionary delivered in a %d B frame (%.1fx)",
				tc.name, len(codec.Dict()), len(frame),
				float64(len(codec.Dict()))/float64(len(frame)))
		})
	}
}
