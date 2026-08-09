package centrifuge

// What this package owns for dictionary compression is a place to plug an engine
// in, a profile to hand it, and a guarantee about frame ordering. It owns no
// dictionaries, no sampling and no decisions - those live in whatever supplies
// the engine.
//
// So these tests cover exactly that: the interface is usable from outside, the
// profile reaches it, the dictionary reaches the client in the connect reply,
// and everything after that reply is compressed.

import (
	"bytes"
	"context"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/centrifugal/centrifuge/internal/websocket"
	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/require"
)

// testCompression is an engine supplied from outside the package, which is the
// only way engines exist. It hands every supporting connection one fixed
// dictionary, and records what it was told about the connection.
type testCompression struct {
	dict []byte
	// seen records the params of the last connection created, so a test can
	// assert what the server resolved.
	seen ConnectionParams
	// last is the connection most recently handed out, so a test can assert its
	// lifecycle rather than only what went on the wire.
	last *testConnCompression
}

func (e *testCompression) NewConnection(params ConnectionParams) ConnectionCompression {
	e.seen = params
	if params.ClientFlags&ConnectionFlagDictionaryCompression == 0 {
		return nil
	}
	e.last = &testConnCompression{
		codec: protocol.NewDeflateFrameCodec(protocol.DictionaryID(e.dict), e.dict),
		proto: params.ProtocolType.toProto(),
		held:  params.HeldDictionaryID,
	}
	return e.last
}

type testConnCompression struct {
	codec   *protocol.DeflateFrameCodec
	proto   protocol.Type
	held    string
	encoded atomic.Int64
	closed  atomic.Int64
}

func (c *testConnCompression) Dictionary() *protocol.Dictionary {
	d := &protocol.Dictionary{Id: c.codec.ID()}
	if c.held == c.codec.ID() {
		// The client already holds these bytes, so naming it is enough.
		return d
	}
	packed := protocol.DeflateDictionary(c.codec.Dict())
	if c.proto == protocol.TypeJSON {
		d.DataB64 = base64.StdEncoding.EncodeToString(packed)
	} else {
		d.Data = packed
	}
	return d
}

func (c *testConnCompression) Encode(frame []byte) ([]byte, bool) {
	c.encoded.Add(1)
	return c.codec.Compress(nil, frame), true
}

func (c *testConnCompression) Close() { c.closed.Add(1) }

func testDictionary() []byte {
	return bytes.Repeat([]byte(`{"push":{"channel":"demo","pub":{"data":{"seq":,"v":"x"}}}}`), 40)
}

// A raw client, so the test sees exactly what goes on the wire rather than what
// an SDK makes of it.
type wireClient struct {
	t    *testing.T
	conn *websocket.Conn
	// held is the dictionary this client advertised, and therefore the one it
	// can decode a compressed connect reply with.
	held  []byte
	codec *protocol.DeflateFrameCodec
}

func dialWire(t *testing.T, url string, req *protocol.ConnectRequest) *wireClient {
	t.Helper()
	conn, _, _, err := (&websocket.Dialer{}).Dial(url, nil)
	require.NoError(t, err)
	// Closed before the server is, otherwise httptest waits out the open
	// connection on every test.
	t.Cleanup(func() { _ = conn.Close() })
	w := &wireClient{t: t, conn: conn}
	if req.Dict != "" {
		w.held = testDictionary()
	}
	data, err := protocol.NewJSONCommandEncoder().Encode(&protocol.Command{Id: 1, Connect: req})
	require.NoError(t, err)
	require.NoError(t, conn.WriteMessage(websocket.TextMessage, data))
	return w
}

// connectResult reads the connect reply and installs any dictionary it
// carried.
//
// The reply is raw when it is delivering a dictionary, and compressed when the
// client already held one - so this checks the frame marker rather than
// assuming, which is exactly what a real client does. A client that advertised
// an id can always decode, because it has the bytes that id names.
func (w *wireClient) connectResult() *protocol.ConnectResult {
	w.t.Helper()
	require.NoError(w.t, w.conn.SetReadDeadline(time.Now().Add(5*time.Second)))
	_, msg, err := w.conn.ReadMessage()
	require.NoError(w.t, err)
	if len(msg) > 0 && msg[0] == protocol.FrameCodecCompressed {
		require.NotNil(w.t, w.held, "a compressed connect reply means the server took the id this client advertised")
		codec := protocol.NewDeflateFrameCodec(protocol.DictionaryID(w.held), w.held)
		msg, err = codec.Decompress(nil, msg, 1<<20)
		require.NoError(w.t, err)
		w.codec = codec
	}
	r, err := protocol.NewJSONReplyDecoder(msg).Decode()
	require.NoError(w.t, err)
	require.NotNil(w.t, r.Connect)
	if d := r.Connect.Dict; d != nil && d.DataB64 != "" {
		raw, derr := protocol.InflateDictionary(mustBase64(w.t, d.DataB64), 1<<20)
		require.NoError(w.t, derr)
		w.codec = protocol.NewDeflateFrameCodec(d.Id, raw)
	}
	return r.Connect
}

// next reads one frame, decoding it if a dictionary was installed.
func (w *wireClient) next() []*protocol.Reply {
	w.t.Helper()
	require.NoError(w.t, w.conn.SetReadDeadline(time.Now().Add(5*time.Second)))
	_, msg, err := w.conn.ReadMessage()
	require.NoError(w.t, err)
	if w.codec != nil {
		msg, err = w.codec.Decompress(nil, msg, 1<<20)
		require.NoError(w.t, err, "every frame after the connect reply must decode")
	}
	var out []*protocol.Reply
	for _, line := range bytes.Split(bytes.TrimRight(msg, "\n"), []byte("\n")) {
		r, derr := protocol.NewJSONReplyDecoder(line).Decode()
		require.NoError(w.t, derr)
		out = append(out, r)
	}
	return out
}

// subscribe sends a subscribe command and waits for its reply, which is already
// compressed - so this doubles as a check that the client can decode.
func (w *wireClient) subscribe(ch string) {
	w.t.Helper()
	data, err := protocol.NewJSONCommandEncoder().Encode(&protocol.Command{
		Id: 2, Subscribe: &protocol.SubscribeRequest{Channel: ch},
	})
	require.NoError(w.t, err)
	require.NoError(w.t, w.conn.WriteMessage(websocket.TextMessage, data))
	replies := w.next()
	require.NotNil(w.t, replies[0].Subscribe)
}

func mustBase64(t *testing.T, s string) []byte {
	t.Helper()
	b, err := base64.StdEncoding.DecodeString(s)
	require.NoError(t, err)
	return b
}

func newCompressionNode(t *testing.T, e DictionaryCompression, onConnecting func(ConnectEvent) ConnectReply) (*Node, string) {
	t.Helper()
	n, err := New(Config{DictionaryCompression: e})
	require.NoError(t, err)
	n.OnConnecting(func(ctx context.Context, ev ConnectEvent) (ConnectReply, error) {
		reply := ConnectReply{Credentials: &Credentials{UserID: "u"}}
		if onConnecting != nil {
			r := onConnecting(ev)
			r.Credentials = reply.Credentials
			reply = r
		}
		return reply, nil
	})
	n.OnConnect(func(c *Client) {
		c.OnSubscribe(func(ev SubscribeEvent, cb SubscribeCallback) { cb(SubscribeReply{}, nil) })
	})
	require.NoError(t, n.Run())
	t.Cleanup(func() { _ = n.Shutdown(context.Background()) })

	mux := http.NewServeMux()
	mux.Handle("/connection/websocket", NewWebsocketHandler(n, WebsocketConfig{
		// A raw client does not answer pings, and a pong with no preceding ping
		// is itself grounds for disconnect, so move pings past the test.
		PingPongConfig: PingPongConfig{PingInterval: 10 * time.Minute, PongTimeout: time.Minute},
	}))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return n, "ws" + strings.TrimPrefix(srv.URL, "http") + "/connection/websocket"
}

// The connect reply carries the dictionary, and everything after it is
// compressed. That ordering is the whole reason delivery lives here rather than
// in a push: there is no window in which a compressed frame can reach a client
// that does not yet hold the dictionary.
func TestDictionaryArrivesInConnectReply(t *testing.T) {
	t.Parallel()
	e := &testCompression{dict: testDictionary()}
	n, url := newCompressionNode(t, e, nil)

	w := dialWire(t, url, &protocol.ConnectRequest{Flag: ConnectionFlagDictionaryCompression})
	res := w.connectResult()

	require.Equal(t, ConnectionFlagDictionaryCompression, res.Flag,
		"server must confirm what it enabled")
	require.NotNil(t, res.Dict, "dictionary must travel in the connect reply")
	require.NotEmpty(t, res.Dict.Id)
	require.NotEmpty(t, res.Dict.DataB64, "a client holding nothing must be sent the content")
	require.NotNil(t, w.codec)

	w.subscribe("demo")
	_, err := n.Publish("demo", []byte(`{"seq":1,"v":"x"}`))
	require.NoError(t, err)
	replies := w.next()
	require.NotNil(t, replies[0].Push)
	require.NotNil(t, replies[0].Push.Pub)
}

// A client that already holds the dictionary is told its id and nothing else.
func TestHeldDictionaryIsNamedNotSent(t *testing.T) {
	t.Parallel()
	dict := testDictionary()
	e := &testCompression{dict: dict}
	_, url := newCompressionNode(t, e, nil)

	id := protocol.DictionaryID(dict)
	w := dialWire(t, url, &protocol.ConnectRequest{
		Flag: ConnectionFlagDictionaryCompression,
		Dict: id,
	})
	res := w.connectResult()

	require.NotNil(t, res.Dict)
	require.Equal(t, id, res.Dict.Id)
	require.Empty(t, res.Dict.DataB64, "the client already holds the bytes")
	require.Empty(t, res.Dict.Data)
	require.Equal(t, id, e.seen.HeldDictionaryID, "the engine must see what the client holds")
}

// A client that cannot decode is left alone, whatever the server has to offer.
func TestClientWithoutSupportGetsNothing(t *testing.T) {
	t.Parallel()
	e := &testCompression{dict: testDictionary()}
	_, url := newCompressionNode(t, e, nil)

	w := dialWire(t, url, &protocol.ConnectRequest{})
	res := w.connectResult()

	require.Zero(t, res.Flag)
	require.Nil(t, res.Dict)
	require.Nil(t, w.codec)
}

// The profile a connection ends up with is the one the application returned,
// and only that one. A client's declaration reaches the application as a
// request and goes no further on its own, so a server that expresses no opinion
// classifies nobody. The alternative - letting an unanswered claim stand - would
// mean any policy expressed by returning "" was silently ignored.
func TestProfileResolution(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		declared string
		reply    string
		want     string
	}{
		{"declaration alone classifies nobody", "dashboard", "", ""},
		{"application answers the declaration", "dashboard", "dashboard", "dashboard"},
		{"application overrides the declaration", "dashboard", "trusted", "trusted"},
		{"application classifies a silent client", "", "trusted", "trusted"},
		{"nobody says anything", "", "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := &testCompression{dict: testDictionary()}
			_, url := newCompressionNode(t, e, func(ConnectEvent) ConnectReply {
				return ConnectReply{Profile: tc.reply}
			})
			w := dialWire(t, url, &protocol.ConnectRequest{
				Flag:    ConnectionFlagDictionaryCompression,
				Profile: tc.declared,
			})
			w.connectResult()
			require.Equal(t, tc.want, e.seen.Profile)
		})
	}
}

// The declaration still has to reach the application, or it has nothing to
// validate against and no way to honour a client that asked correctly.
func TestConnectEventCarriesDeclaredProfile(t *testing.T) {
	t.Parallel()
	seen := make(chan string, 1)
	e := &testCompression{dict: testDictionary()}
	_, url := newCompressionNode(t, e, func(ev ConnectEvent) ConnectReply {
		seen <- ev.Profile
		// Echoing only a name this server knows is the intended pattern: it is
		// what turns an assertion into a classification.
		if ev.Profile == "dashboard" {
			return ConnectReply{Profile: ev.Profile}
		}
		return ConnectReply{}
	})
	w := dialWire(t, url, &protocol.ConnectRequest{
		Flag:    ConnectionFlagDictionaryCompression,
		Profile: "dashboard",
	})
	w.connectResult()
	require.Equal(t, "dashboard", <-seen)
	require.Equal(t, "dashboard", e.seen.Profile)
}

// Client.Profile reports the same resolved value, so an application can filter
// and measure by it without knowing how it was decided.
func TestClientProfileAccessor(t *testing.T) {
	t.Parallel()
	got := make(chan string, 1)
	n, err := New(Config{})
	require.NoError(t, err)
	n.OnConnecting(func(ctx context.Context, ev ConnectEvent) (ConnectReply, error) {
		return ConnectReply{Credentials: &Credentials{UserID: "u"}, Profile: "trusted"}, nil
	})
	n.OnConnect(func(c *Client) { got <- c.Profile() })
	require.NoError(t, n.Run())
	t.Cleanup(func() { _ = n.Shutdown(context.Background()) })

	mux := http.NewServeMux()
	mux.Handle("/connection/websocket", NewWebsocketHandler(n, WebsocketConfig{
		PingPongConfig: PingPongConfig{PingInterval: 10 * time.Minute, PongTimeout: time.Minute},
	}))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	w := dialWire(t, "ws"+strings.TrimPrefix(srv.URL, "http")+"/connection/websocket",
		&protocol.ConnectRequest{Profile: "ignored"})
	w.connectResult()
	select {
	case p := <-got:
		require.Equal(t, "trusted", p)
	case <-time.After(5 * time.Second):
		t.Fatal("OnConnect never ran")
	}
}

// Nil engine means the feature is absent: no flag, no dictionary, nothing
// changed for any client.
func TestNilEngineChangesNothing(t *testing.T) {
	t.Parallel()
	_, url := newCompressionNode(t, nil, nil)
	w := dialWire(t, url, &protocol.ConnectRequest{Flag: ConnectionFlagDictionaryCompression})
	res := w.connectResult()
	require.Zero(t, res.Flag)
	require.Nil(t, res.Dict)
}

// An engine may decline a particular connection, which must leave it working
// and uncompressed rather than half configured.
func TestEngineMayDecline(t *testing.T) {
	t.Parallel()
	_, url := newCompressionNode(t, declineAll{}, nil)
	w := dialWire(t, url, &protocol.ConnectRequest{Flag: ConnectionFlagDictionaryCompression})
	res := w.connectResult()
	require.Zero(t, res.Flag)
	require.Nil(t, res.Dict)
}

type declineAll struct{}

func (declineAll) NewConnection(ConnectionParams) ConnectionCompression { return nil }

// An implementation that batches its accounting has to be told when a
// connection ends, or everything since its last flush is lost. That loss is not
// evenly spread: short connections are the ones that end before a flush and
// also the ones most likely to have paid for a dictionary transfer, so dropping
// them makes compression look better than it was.
func TestConnectionCompressionClosedOnDisconnect(t *testing.T) {
	t.Parallel()
	e := &testCompression{dict: testDictionary()}
	n, url := newCompressionNode(t, e, nil)

	w := dialWire(t, url, &protocol.ConnectRequest{Flag: ConnectionFlagDictionaryCompression})
	w.connectResult()
	w.subscribe("demo")
	cc := e.last
	require.NotNil(t, cc)
	require.Zero(t, cc.closed.Load(), "still connected")

	require.NoError(t, w.conn.Close())
	require.Eventually(t, func() bool { return cc.closed.Load() == 1 },
		5*time.Second, 10*time.Millisecond, "close must reach the codec exactly once")
	_ = n
}

// A connection that never writes a frame after the connect reply still has a
// codec - it is sitting unpromoted, waiting for a first write that never comes.
// Closing has to find it there too, otherwise the quietest connections, which
// are exactly the ones a dictionary transfer is hardest to justify for, would be
// the ones missing from the accounting.
func TestConnectionCompressionClosedWhenNeverWritten(t *testing.T) {
	t.Parallel()
	e := &testCompression{dict: testDictionary()}
	_, url := newCompressionNode(t, e, nil)

	w := dialWire(t, url, &protocol.ConnectRequest{Flag: ConnectionFlagDictionaryCompression})
	w.connectResult()
	cc := e.last
	require.NotNil(t, cc)
	require.Zero(t, cc.encoded.Load(), "nothing was written after the connect reply")

	require.NoError(t, w.conn.Close())
	require.Eventually(t, func() bool { return cc.closed.Load() == 1 },
		5*time.Second, 10*time.Millisecond, "an unpromoted codec must still be closed")
}

// The user is the only identity that survives a reconnect, so it is what a
// staged rollout has to split on. Without it an implementation can only draw a
// fresh number per connection, which re-dices the cohort on every reconnect and
// makes a client pay to be resent a dictionary it already had.
func TestConnectionParamsCarryUserID(t *testing.T) {
	t.Parallel()
	e := &testCompression{dict: testDictionary()}
	_, url := newCompressionNode(t, e, nil)
	w := dialWire(t, url, &protocol.ConnectRequest{Flag: ConnectionFlagDictionaryCompression})
	w.connectResult()
	require.Equal(t, "u", e.seen.UserID)
}

// A client that already holds the dictionary has nothing to wait for, so its
// connect reply is compressed like every other frame. The saving is small - a
// warm reply is a couple of hundred bytes - but the rule is simpler than
// exempting one frame: compression starts once both sides provably hold the
// dictionary, which for this client is immediately.
func TestWarmConnectReplyIsItselfCompressed(t *testing.T) {
	t.Parallel()
	e := &testCompression{dict: testDictionary()}
	_, url := newCompressionNode(t, e, nil)

	// First connection: cold, so the reply carries the bytes and is raw.
	cold := dialWire(t, url, &protocol.ConnectRequest{Flag: ConnectionFlagDictionaryCompression})
	first := cold.connectResult()
	require.NotEmpty(t, first.Dict.DataB64, "a client holding nothing is sent the bytes")

	// Second connection advertises the id it now holds.
	conn, _, _, err := (&websocket.Dialer{}).Dial(url, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	data, err := protocol.NewJSONCommandEncoder().Encode(&protocol.Command{
		Id: 1, Connect: &protocol.ConnectRequest{
			Flag: ConnectionFlagDictionaryCompression, Dict: first.Dict.Id,
		},
	})
	require.NoError(t, err)
	require.NoError(t, conn.WriteMessage(websocket.TextMessage, data))

	require.NoError(t, conn.SetReadDeadline(time.Now().Add(5*time.Second)))
	_, msg, err := conn.ReadMessage()
	require.NoError(t, err)

	// Self-describing: the marker says what this is, so a client never has to
	// predict whether its reply was compressed.
	require.Equal(t, protocol.FrameCodecCompressed, msg[0],
		"a warm client's connect reply must itself be compressed")

	codec := protocol.NewDeflateFrameCodec(first.Dict.Id, testDictionary())
	decoded, err := codec.Decompress(nil, msg, 1<<20)
	require.NoError(t, err, "and must decode against the dictionary the client already had")
	r, err := protocol.NewJSONReplyDecoder(decoded).Decode()
	require.NoError(t, err)
	require.NotNil(t, r.Connect)
	require.Equal(t, first.Dict.Id, r.Connect.Dict.Id)
	require.Empty(t, r.Connect.Dict.DataB64, "and must not re-send bytes it already holds")
	require.Less(t, len(msg), len(decoded), "compressed on the wire, not merely marked")
}

// OnTransportWrite must see frames before they are compressed.
//
// This is load-bearing and easy to break without noticing. The training side of
// dictionary compression observes connections through this hook and parses what
// it sees; if the hook ever moved after the codec, every frame on an already
// compressed connection would arrive as deflate bytes, extraction would reject
// them all as unparseable, and retraining a profile that is already serving a
// dictionary would silently learn nothing. The symptom would be "the dictionary
// got worse after retraining", with a parse rate of zero as the only clue.
func TestTransportWriteObservesUncompressedFrames(t *testing.T) {
	t.Parallel()
	e := &testCompression{dict: testDictionary()}

	seen := make(chan []byte, 32)
	n, err := New(Config{DictionaryCompression: e})
	require.NoError(t, err)
	n.OnConnecting(func(ctx context.Context, ev ConnectEvent) (ConnectReply, error) {
		return ConnectReply{Credentials: &Credentials{UserID: "u"}}, nil
	})
	n.OnTransportWrite(func(c *Client, ev TransportWriteEvent) bool {
		cp := make([]byte, len(ev.Data))
		copy(cp, ev.Data)
		select {
		case seen <- cp:
		default:
		}
		return true
	})
	n.OnConnect(func(c *Client) {
		c.OnSubscribe(func(ev SubscribeEvent, cb SubscribeCallback) { cb(SubscribeReply{}, nil) })
	})
	require.NoError(t, n.Run())
	t.Cleanup(func() { _ = n.Shutdown(context.Background()) })

	mux := http.NewServeMux()
	mux.Handle("/connection/websocket", NewWebsocketHandler(n, WebsocketConfig{
		PingPongConfig: PingPongConfig{PingInterval: 10 * time.Minute, PongTimeout: time.Minute},
	}))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	url := "ws" + strings.TrimPrefix(srv.URL, "http") + "/connection/websocket"

	w := dialWire(t, url, &protocol.ConnectRequest{Flag: ConnectionFlagDictionaryCompression})
	require.NotNil(t, w.connectResult().Dict, "this connection must actually be compressing")
	w.subscribe("demo")

	_, err = n.Publish("demo", []byte(`{"observable":"payload"}`))
	require.NoError(t, err)
	w.next()

	// Every frame the hook saw must still be readable as protocol data. A
	// compressed one would fail to decode here, which is exactly the
	// regression this guards - and the check is not vacuous, since the same
	// bytes after the codec do carry the compressed marker (asserted below).
	var sawPublication bool
	var lastRaw []byte
	for len(seen) > 0 {
		frame := <-seen
		require.NotEqual(t, protocol.FrameCodecCompressed, frame[0],
			"the hook must run before compression, not after")
		lastRaw = frame
		for _, line := range bytes.Split(bytes.TrimRight(frame, "\n"), []byte("\n")) {
			r, derr := protocol.NewJSONReplyDecoder(line).Decode()
			require.NoError(t, derr, "an observer must be able to parse what it is given")
			if r.Push != nil && r.Push.Pub != nil {
				require.Contains(t, string(r.Push.Pub.Data), "observable")
				sawPublication = true
			}
		}
	}
	require.True(t, sawPublication, "the publication must have reached the hook at all")

	// Prove the assertion above discriminates: the very same frame, once
	// encoded by this connection's codec, is marked compressed and no longer
	// parses. Without this the test would pass even if compression had been
	// silently disabled for the connection.
	require.NotNil(t, e.last)
	encoded, _ := e.last.Encode(lastRaw)
	require.Equal(t, protocol.FrameCodecCompressed, encoded[0])
	_, derr := protocol.NewJSONReplyDecoder(encoded).Decode()
	require.Error(t, derr, "a compressed frame must not parse as protocol data")
}

// An engine that names a dictionary the client never advertised is refused.
//
// The contract says to set only the id when the client presented that same id,
// because naming it is a claim that both sides already hold the bytes. An
// engine that gets this wrong - a third-party one, or a bug in a supplied one -
// would otherwise have the server compress every frame, including the connect
// reply, against bytes the client provably does not have. The connection would
// receive nothing it could read and have no way to say why, so the server
// declines rather than trusting the claim.
func TestNamedDictionaryTheClientNeverAdvertisedIsRefused(t *testing.T) {
	t.Parallel()
	e := &misnamingCompression{dict: testDictionary()}
	_, url := newCompressionNode(t, e, nil)

	w := dialWire(t, url, &protocol.ConnectRequest{Flag: ConnectionFlagDictionaryCompression})
	res := w.connectResult()

	require.Nil(t, res.Dict, "a dictionary the client cannot hold must not be offered")
	require.Zero(t, res.Flag&ConnectionFlagDictionaryCompression,
		"and the connection must not be told compression is on")
	require.Equal(t, int64(1), e.last.closed.Load(),
		"the refused codec is closed, not leaked")

	// The connection still works, uncompressed.
	w.subscribe("demo")
}

// misnamingCompression names a dictionary regardless of what the client
// advertised, which is precisely what the contract forbids.
type misnamingCompression struct {
	dict []byte
	last *testConnCompression
}

func (e *misnamingCompression) NewConnection(params ConnectionParams) ConnectionCompression {
	if params.ClientFlags&ConnectionFlagDictionaryCompression == 0 {
		return nil
	}
	e.last = &testConnCompression{
		codec: protocol.NewDeflateFrameCodec(protocol.DictionaryID(e.dict), e.dict),
		proto: params.ProtocolType.toProto(),
		// Claim the client holds it whatever it actually advertised.
		held: protocol.DictionaryID(e.dict),
	}
	return e.last
}

// A client that advertises a dictionary it holds has already installed the
// codec for it - it has to, because a server that recognises the id compresses
// the connect reply itself, and a codec learned from that reply could never
// read it. So every frame such a client receives has to carry a marker for it
// to strip, including a reply this server chose not to compress.
//
// Without this, changing what a profile serves stalled every returning client
// for as long as its connect took to time out: the server sent an unmarked
// reply, the client read the first byte of protocol as a codec marker, and the
// connection died before the connect result arrived.
func TestReplyIsMarkedWhenTheClientAlreadyHoldsACodec(t *testing.T) {
	t.Parallel()

	t.Run("server serves a different dictionary than the one advertised", func(t *testing.T) {
		e := &testCompression{dict: testDictionary()}
		_, url := newCompressionNode(t, e, nil)

		msg := dialAdvertising(t, url, "an-id-this-server-does-not-have")
		require.Equal(t, protocol.FrameCodecRaw, msg[0],
			"the reply carries new bytes so it cannot be compressed, but it must still be marked")
		replies, err := protocol.NewJSONReplyDecoder(msg[1:]).Decode()
		require.NoError(t, err)
		require.NotEmpty(t, replies.Connect.Dict.DataB64, "and it carries the dictionary to use next")
	})

	t.Run("server serves nothing at all", func(t *testing.T) {
		// No dictionary for anyone - the engine declines this connection.
		e := &testCompression{dict: nil}
		_, url := newCompressionNode(t, e, nil)

		msg := dialAdvertising(t, url, "an-id-this-server-does-not-have")
		require.Equal(t, protocol.FrameCodecRaw, msg[0],
			"declining to compress does not release the server from marking frames")
	})
}

// dialAdvertising connects declaring support and a held dictionary id, and
// returns the raw bytes of the first frame the server writes.
func dialAdvertising(t *testing.T, url, held string) []byte {
	t.Helper()
	conn, _, _, err := (&websocket.Dialer{}).Dial(url, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	data, err := protocol.NewJSONCommandEncoder().Encode(&protocol.Command{
		Id: 1, Connect: &protocol.ConnectRequest{
			Flag: ConnectionFlagDictionaryCompression, Dict: held,
		},
	})
	require.NoError(t, err)
	require.NoError(t, conn.WriteMessage(websocket.TextMessage, data))

	require.NoError(t, conn.SetReadDeadline(time.Now().Add(5*time.Second)))
	_, msg, err := conn.ReadMessage()
	require.NoError(t, err)
	require.NotEmpty(t, msg)
	return msg
}
