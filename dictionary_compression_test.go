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
}

func (e *testCompression) NewConnection(params ConnectionParams) ConnectionCompression {
	e.seen = params
	if params.ClientFlags&ConnectionFlagDictionaryCompression == 0 {
		return nil
	}
	return &testConnCompression{
		codec: protocol.NewDeflateFrameCodec(protocol.DictionaryID(e.dict), e.dict),
		proto: params.ProtocolType.toProto(),
		held:  params.HeldDictionaryID,
	}
}

type testConnCompression struct {
	codec *protocol.DeflateFrameCodec
	proto protocol.Type
	held  string
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
	return c.codec.Compress(nil, frame), true
}

func testDictionary() []byte {
	return bytes.Repeat([]byte(`{"push":{"channel":"demo","pub":{"data":{"seq":,"v":"x"}}}}`), 40)
}

// A raw client, so the test sees exactly what goes on the wire rather than what
// an SDK makes of it.
type wireClient struct {
	t     *testing.T
	conn  *websocket.Conn
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
	data, err := protocol.NewJSONCommandEncoder().Encode(&protocol.Command{Id: 1, Connect: req})
	require.NoError(t, err)
	require.NoError(t, conn.WriteMessage(websocket.TextMessage, data))
	return w
}

// connectResult reads the connect reply, which is always raw, and installs any
// dictionary it carried.
func (w *wireClient) connectResult() *protocol.ConnectResult {
	w.t.Helper()
	require.NoError(w.t, w.conn.SetReadDeadline(time.Now().Add(5*time.Second)))
	_, msg, err := w.conn.ReadMessage()
	require.NoError(w.t, err)
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
