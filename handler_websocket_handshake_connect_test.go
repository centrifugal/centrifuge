package centrifuge

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/centrifugal/centrifuge/internal/websocket"

	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/require"
)

const handshakeConnectTestChannel = "test"

// newHandshakeConnectServer starts a server accepting connect, subscribe and
// publish commands and returns its node and WebSocket URL.
func newHandshakeConnectServer(t *testing.T, config WebsocketConfig) (*Node, string) {
	t.Helper()
	n := defaultNodeNoHandlers()
	t.Cleanup(func() { _ = n.Shutdown(context.Background()) })
	n.OnConnecting(func(_ context.Context, e ConnectEvent) (ConnectReply, error) {
		if e.Token == "invalid" {
			return ConnectReply{}, DisconnectInvalidToken
		}
		return ConnectReply{Credentials: &Credentials{UserID: "test"}}, nil
	})
	n.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			if e.Channel != handshakeConnectTestChannel {
				cb(SubscribeReply{}, ErrorPermissionDenied)
				return
			}
			cb(SubscribeReply{}, nil)
		})
		client.OnPublish(func(_ PublishEvent, cb PublishCallback) {
			cb(PublishReply{}, nil)
		})
	})
	mux := http.NewServeMux()
	mux.Handle("/connection/websocket", NewWebsocketHandler(n, config))
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	return n, "ws" + server.URL[4:] + "/connection/websocket"
}

// encodeHandshakeFrame encodes commands as one frame of the protocol.
func encodeHandshakeFrame(t *testing.T, protoType ProtocolType, cmds ...*protocol.Command) []byte {
	t.Helper()
	var frame []byte
	for _, cmd := range cmds {
		if protoType == ProtocolTypeProtobuf {
			b, err := cmd.MarshalVT()
			require.NoError(t, err)
			frame = binary.AppendUvarint(frame, uint64(len(b)))
			frame = append(frame, b...)
			continue
		}
		b, err := json.Marshal(cmd)
		require.NoError(t, err)
		if len(frame) > 0 {
			frame = append(frame, '\n')
		}
		frame = append(frame, b...)
	}
	return frame
}

func connectCommand(id uint32) *protocol.Command {
	return &protocol.Command{Id: id, Connect: &protocol.ConnectRequest{}}
}

func subscribeCommand(id uint32, channel string) *protocol.Command {
	return &protocol.Command{Id: id, Subscribe: &protocol.SubscribeRequest{Channel: channel}}
}

func publishCommand(id uint32) *protocol.Command {
	return &protocol.Command{Id: id, Publish: &protocol.PublishRequest{Channel: handshakeConnectTestChannel, Data: []byte(`{}`)}}
}

// handshakeConnectCommands returns connect and subscribe commands as one frame.
func handshakeConnectCommands(t *testing.T, protoType ProtocolType) []byte {
	t.Helper()
	return encodeHandshakeFrame(t, protoType, connectCommand(1), subscribeCommand(2, handshakeConnectTestChannel))
}

func handshakeConnectSubprotocol(data []byte) string {
	return handshakeConnectPrefix + base64.RawURLEncoding.EncodeToString(data)
}

// paddedHandshakeConnectSubprotocol encodes data with base64url padding,
// which is not accepted.
func paddedHandshakeConnectSubprotocol(data []byte) string {
	if len(data)%3 == 0 {
		// Make sure the encoding needs padding.
		data = append(data[:len(data):len(data)], ' ')
	}
	return handshakeConnectPrefix + base64.URLEncoding.EncodeToString(data)
}

func dialHandshakeConnect(t *testing.T, url string, compression bool, subprotocols ...string) (*websocket.Conn, string) {
	t.Helper()
	dialer := &websocket.Dialer{Subprotocols: subprotocols, EnableCompression: compression}
	conn, resp, subprotocol, err := dialer.Dial(url, nil)
	require.NoError(t, err)
	_ = resp.Body.Close()
	t.Cleanup(func() { _ = conn.Close() })
	return conn, subprotocol
}

func writeFrame(t *testing.T, conn *websocket.Conn, protoType ProtocolType, frame []byte) {
	t.Helper()
	messageType := websocket.TextMessage
	if protoType == ProtocolTypeProtobuf {
		messageType = websocket.BinaryMessage
	}
	require.NoError(t, conn.WriteMessage(messageType, frame))
}

// readUntil reads replies, passing each to f, until f returns true.
func readUntil(t *testing.T, conn *websocket.Conn, protoType ProtocolType, f func(*protocol.Reply) bool) {
	t.Helper()
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(5*time.Second)))
	for {
		_, data, err := conn.ReadMessage()
		require.NoError(t, err)
		var decode func() (*protocol.Reply, error)
		if protoType == ProtocolTypeProtobuf {
			decode = protocol.NewProtobufReplyDecoder(data).Decode
		} else {
			decode = protocol.NewJSONReplyDecoder(data).Decode
		}
		done := false
		for {
			reply, err := decode()
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
			if f(reply) {
				done = true
			}
		}
		if done {
			return
		}
	}
}

// readReplies reads until n replies with an id arrive and returns them in the
// order received.
func readReplies(t *testing.T, conn *websocket.Conn, protoType ProtocolType, n int) []*protocol.Reply {
	t.Helper()
	var replies []*protocol.Reply
	readUntil(t, conn, protoType, func(reply *protocol.Reply) bool {
		if reply.Id != 0 {
			replies = append(replies, reply)
		}
		return len(replies) >= n
	})
	require.Len(t, replies, n)
	return replies
}

// readHandshakeReplyIDs reads n replies, requires them to have no error and
// returns their ids in the order received.
func readHandshakeReplyIDs(t *testing.T, conn *websocket.Conn, protoType ProtocolType, n int) []uint32 {
	t.Helper()
	ids := make([]uint32, 0, n)
	for _, reply := range readReplies(t, conn, protoType, n) {
		require.Nil(t, reply.Error, "reply %d", reply.Id)
		ids = append(ids, reply.Id)
	}
	return ids
}

// readPublication reads until a publication push arrives.
func readPublication(t *testing.T, conn *websocket.Conn, protoType ProtocolType) *protocol.Push {
	t.Helper()
	var push *protocol.Push
	readUntil(t, conn, protoType, func(reply *protocol.Reply) bool {
		if reply.Push != nil && reply.Push.Pub != nil {
			push = reply.Push
		}
		return push != nil
	})
	return push
}

// requireClosedWith reads until the server closes the connection and checks
// the close code.
func requireClosedWith(t *testing.T, conn *websocket.Conn, code uint32) {
	t.Helper()
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(5*time.Second)))
	for {
		_, _, err := conn.ReadMessage()
		if err == nil {
			continue
		}
		var closeErr *websocket.CloseError
		require.ErrorAs(t, err, &closeErr)
		require.Equal(t, int(code), closeErr.Code)
		return
	}
}

func TestWebsocketHandshakeConnect(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name        string
		protoType   ProtocolType
		subprotocol string
		config      WebsocketConfig
		compression bool
	}{
		{"json", ProtocolTypeJSON, "cf-json-hc", WebsocketConfig{HandshakeConnect: true}, false},
		{"protobuf", ProtocolTypeProtobuf, "cf-proto-hc", WebsocketConfig{HandshakeConnect: true}, false},
		{"json with legacy plain", ProtocolTypeJSON, "cf-json-hc", WebsocketConfig{HandshakeConnect: true}, false},
		{"protobuf with legacy plain", ProtocolTypeProtobuf, "cf-proto-hc", WebsocketConfig{HandshakeConnect: true}, false},
		{"json compression", ProtocolTypeJSON, "cf-json-hc", WebsocketConfig{HandshakeConnect: true, Compression: true}, true},
		{"protobuf compression", ProtocolTypeProtobuf, "cf-proto-hc", WebsocketConfig{HandshakeConnect: true, Compression: true}, true},
		{"json off read loop", ProtocolTypeJSON, "cf-json-hc", WebsocketConfig{HandshakeConnect: true, ProcessCommandsOffReadLoop: true}, false},
		{"protobuf off read loop", ProtocolTypeProtobuf, "cf-proto-hc", WebsocketConfig{HandshakeConnect: true, ProcessCommandsOffReadLoop: true}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			node, url := newHandshakeConnectServer(t, tc.config)
			data := handshakeConnectCommands(t, tc.protoType)
			offer := []string{tc.subprotocol, handshakeConnectSubprotocol(data)}
			if strings.HasSuffix(tc.name, "with legacy plain") {
				// Exactly what centrifuge-js offers.
				plain := "centrifuge-json"
				if tc.protoType == ProtocolTypeProtobuf {
					plain = "centrifuge-protobuf"
				}
				offer = []string{tc.subprotocol, plain, handshakeConnectSubprotocol(data)}
			}
			conn, subprotocol := dialHandshakeConnect(t, url, tc.compression, offer...)
			require.Equal(t, tc.subprotocol, subprotocol)

			// Frames sent right after the handshake are processed after the
			// commands from the handshake, in order.
			writeFrame(t, conn, tc.protoType, encodeHandshakeFrame(t, tc.protoType, publishCommand(3)))
			writeFrame(t, conn, tc.protoType, encodeHandshakeFrame(t, tc.protoType, publishCommand(4), publishCommand(5)))
			require.Equal(t, []uint32{1, 2, 3, 4, 5}, readHandshakeReplyIDs(t, conn, tc.protoType, 5))

			// The subscription from the handshake is real.
			_, err := node.Publish(handshakeConnectTestChannel, []byte(`{"from":"server"}`))
			require.NoError(t, err)
			push := readPublication(t, conn, tc.protoType)
			require.Equal(t, handshakeConnectTestChannel, push.Channel)
			require.JSONEq(t, `{"from":"server"}`, string(push.Pub.Data))
		})
	}
}

func TestWebsocketHandshakeConnectOnlyConnect(t *testing.T) {
	t.Parallel()
	_, url := newHandshakeConnectServer(t, WebsocketConfig{HandshakeConnect: true})
	data := encodeHandshakeFrame(t, ProtocolTypeJSON, connectCommand(1))
	conn, subprotocol := dialHandshakeConnect(t, url, false, "cf-json-hc", "cf-json", handshakeConnectSubprotocol(data))
	require.Equal(t, "cf-json-hc", subprotocol)
	// Subscribe sent after open, as for subscriptions not fitting the handshake.
	writeFrame(t, conn, ProtocolTypeJSON, encodeHandshakeFrame(t, ProtocolTypeJSON, subscribeCommand(2, handshakeConnectTestChannel)))
	require.Equal(t, []uint32{1, 2}, readHandshakeReplyIDs(t, conn, ProtocolTypeJSON, 2))
}

func TestWebsocketHandshakeConnectAtMessageSizeLimit(t *testing.T) {
	t.Parallel()
	data := handshakeConnectCommands(t, ProtocolTypeJSON)
	_, url := newHandshakeConnectServer(t, WebsocketConfig{HandshakeConnect: true, MessageSizeLimit: len(data)})
	conn, subprotocol := dialHandshakeConnect(t, url, false, "cf-json-hc", "cf-json", handshakeConnectSubprotocol(data))
	require.Equal(t, "cf-json-hc", subprotocol)
	require.Equal(t, []uint32{1, 2}, readHandshakeReplyIDs(t, conn, ProtocolTypeJSON, 2))
}

func TestWebsocketHandshakeConnectNotTaken(t *testing.T) {
	t.Parallel()
	jsonData := handshakeConnectCommands(t, ProtocolTypeJSON)
	protobufData := handshakeConnectCommands(t, ProtocolTypeProtobuf)
	enabled := WebsocketConfig{HandshakeConnect: true}
	for _, tc := range []struct {
		name         string
		config       WebsocketConfig
		protoType    ProtocolType
		subprotocols []string
		want         string
	}{
		{"disabled", WebsocketConfig{}, ProtocolTypeJSON, []string{"cf-json-hc", "cf-json", handshakeConnectSubprotocol(jsonData)}, "cf-json"},
		{"disabled protobuf", WebsocketConfig{}, ProtocolTypeProtobuf, []string{"cf-proto-hc", "cf-proto", handshakeConnectSubprotocol(protobufData)}, "cf-proto"},
		{"disabled protobuf legacy fallback", WebsocketConfig{}, ProtocolTypeProtobuf, []string{"cf-proto-hc", "centrifuge-protobuf", handshakeConnectSubprotocol(protobufData)}, "centrifuge-protobuf"},
		// Exactly what centrifuge-js offers.
		{"disabled js client offer", WebsocketConfig{}, ProtocolTypeJSON, []string{"cf-json-hc", "centrifuge-json", handshakeConnectSubprotocol(jsonData)}, "centrifuge-json"},
		{"disabled no fallback offered", WebsocketConfig{}, ProtocolTypeJSON, []string{"cf-json-hc", handshakeConnectSubprotocol(jsonData)}, ""},
		{"invalid base64", enabled, ProtocolTypeJSON, []string{"cf-json-hc", "cf-json", "cf-connect.!!!"}, "cf-json"},
		{"padded base64", enabled, ProtocolTypeJSON, []string{"cf-json-hc", "cf-json", paddedHandshakeConnectSubprotocol(jsonData)}, "cf-json"},
		{"standard base64 alphabet", enabled, ProtocolTypeJSON, []string{"cf-json-hc", "cf-json", "cf-connect.+/+/"}, "cf-json"},
		{"empty data", enabled, ProtocolTypeJSON, []string{"cf-json-hc", "cf-json", "cf-connect."}, "cf-json"},
		{"no data", enabled, ProtocolTypeJSON, []string{"cf-json-hc", "cf-json"}, "cf-json"},
		{"no data protobuf", enabled, ProtocolTypeProtobuf, []string{"cf-proto-hc", "cf-proto"}, "cf-proto"},
		// The frame sent after open fits the limit, the padded one from the handshake does not.
		{"over message size limit", WebsocketConfig{HandshakeConnect: true, MessageSizeLimit: len(jsonData)}, ProtocolTypeJSON, []string{"cf-json-hc", "cf-json", handshakeConnectSubprotocol(append(jsonData[:len(jsonData):len(jsonData)], ' '))}, "cf-json"},
		{"hc not offered", enabled, ProtocolTypeJSON, []string{"cf-json", handshakeConnectSubprotocol(jsonData)}, "cf-json"},
		{"hc offered after plain", enabled, ProtocolTypeJSON, []string{"cf-json", "cf-json-hc", handshakeConnectSubprotocol(jsonData)}, "cf-json"},
		{"hc not offered protobuf", enabled, ProtocolTypeProtobuf, []string{"cf-proto", handshakeConnectSubprotocol(protobufData)}, "cf-proto"},
		{"legacy name", enabled, ProtocolTypeJSON, []string{"centrifuge-json", handshakeConnectSubprotocol(jsonData)}, "centrifuge-json"},
		{"legacy name protobuf", enabled, ProtocolTypeProtobuf, []string{"centrifuge-protobuf", handshakeConnectSubprotocol(protobufData)}, "centrifuge-protobuf"},
		{"only data offered", enabled, ProtocolTypeJSON, []string{handshakeConnectSubprotocol(jsonData)}, ""},
		{"wrong prefix case", enabled, ProtocolTypeJSON, []string{"cf-json-hc", "cf-json", "CF-CONNECT." + base64.RawURLEncoding.EncodeToString(jsonData)}, "cf-json"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, url := newHandshakeConnectServer(t, tc.config)
			conn, subprotocol := dialHandshakeConnect(t, url, false, tc.subprotocols...)
			require.Equal(t, tc.want, subprotocol)
			// Commands from the handshake are not processed, the client sends
			// them after the connection opens. Had the server processed them
			// too, the second connect would fail as already authenticated.
			writeFrame(t, conn, tc.protoType, handshakeConnectCommands(t, tc.protoType))
			writeFrame(t, conn, tc.protoType, encodeHandshakeFrame(t, tc.protoType, publishCommand(3)))
			require.Equal(t, []uint32{1, 2, 3}, readHandshakeReplyIDs(t, conn, tc.protoType, 3))
		})
	}
}

func TestWebsocketHandshakeConnectBadData(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name        string
		subprotocol string
		data        []byte
	}{
		{"malformed json", "cf-json-hc", []byte(`{"id":1,`)},
		{"no commands", "cf-json-hc", []byte(` `)},
		{"protobuf data as json", "cf-json-hc", handshakeConnectCommands(t, ProtocolTypeProtobuf)},
		{"json data as protobuf", "cf-proto-hc", handshakeConnectCommands(t, ProtocolTypeJSON)},
		{"command before connect", "cf-json-hc", encodeHandshakeFrame(t, ProtocolTypeJSON, subscribeCommand(1, handshakeConnectTestChannel))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, url := newHandshakeConnectServer(t, WebsocketConfig{HandshakeConnect: true})
			conn, subprotocol := dialHandshakeConnect(t, url, false, tc.subprotocol, handshakeConnectSubprotocol(tc.data))
			require.Equal(t, tc.subprotocol, subprotocol)
			// Handled like a bad first frame.
			requireClosedWith(t, conn, DisconnectBadRequest.Code)
		})
	}
}

func TestWebsocketHandshakeConnectRejected(t *testing.T) {
	t.Parallel()
	_, url := newHandshakeConnectServer(t, WebsocketConfig{HandshakeConnect: true})
	data := encodeHandshakeFrame(t, ProtocolTypeJSON, &protocol.Command{Id: 1, Connect: &protocol.ConnectRequest{Token: "invalid"}}, subscribeCommand(2, handshakeConnectTestChannel))
	conn, subprotocol := dialHandshakeConnect(t, url, false, "cf-json-hc", handshakeConnectSubprotocol(data))
	require.Equal(t, "cf-json-hc", subprotocol)
	requireClosedWith(t, conn, DisconnectInvalidToken.Code)
}

func TestWebsocketHandshakeConnectSubscribeError(t *testing.T) {
	t.Parallel()
	_, url := newHandshakeConnectServer(t, WebsocketConfig{HandshakeConnect: true})
	data := encodeHandshakeFrame(t, ProtocolTypeJSON, connectCommand(1), subscribeCommand(2, "forbidden"), subscribeCommand(3, handshakeConnectTestChannel))
	conn, subprotocol := dialHandshakeConnect(t, url, false, "cf-json-hc", handshakeConnectSubprotocol(data))
	require.Equal(t, "cf-json-hc", subprotocol)
	writeFrame(t, conn, ProtocolTypeJSON, encodeHandshakeFrame(t, ProtocolTypeJSON, publishCommand(4)))
	replies := readReplies(t, conn, ProtocolTypeJSON, 4)
	require.Equal(t, uint32(1), replies[0].Id)
	require.Nil(t, replies[0].Error)
	require.Equal(t, uint32(2), replies[1].Id)
	require.NotNil(t, replies[1].Error)
	require.Equal(t, ErrorPermissionDenied.Code, replies[1].Error.Code)
	// The connection keeps working.
	require.Equal(t, uint32(3), replies[2].Id)
	require.Nil(t, replies[2].Error)
	require.Equal(t, uint32(4), replies[3].Id)
	require.Nil(t, replies[3].Error)
}

func TestWebsocketShortSubprotocols(t *testing.T) {
	t.Parallel()
	for _, config := range []WebsocketConfig{{}, {HandshakeConnect: true}} {
		_, url := newHandshakeConnectServer(t, config)
		for _, tc := range []struct {
			subprotocol string
			protoType   ProtocolType
		}{
			{"cf-json", ProtocolTypeJSON},
			{"cf-proto", ProtocolTypeProtobuf},
			{"centrifuge-json", ProtocolTypeJSON},
			{"centrifuge-protobuf", ProtocolTypeProtobuf},
		} {
			conn, subprotocol := dialHandshakeConnect(t, url, false, tc.subprotocol)
			require.Equal(t, tc.subprotocol, subprotocol)
			writeFrame(t, conn, tc.protoType, handshakeConnectCommands(t, tc.protoType))
			require.Equal(t, []uint32{1, 2}, readHandshakeReplyIDs(t, conn, tc.protoType, 2), tc.subprotocol)
		}
	}
}

func TestHandshakeConnectData(t *testing.T) {
	t.Parallel()
	newRequest := func(protocols ...string) *http.Request {
		r := httptest.NewRequest(http.MethodGet, "/", nil)
		r.Header.Set("Sec-WebSocket-Protocol", strings.Join(protocols, ", "))
		return r
	}
	// "YWJj" is "abc".
	require.Equal(t, []byte("abc"), handshakeConnectData(newRequest("cf-json-hc", "cf-connect.YWJj"), 3))
	require.Equal(t, []byte("abc"), handshakeConnectData(newRequest("cf-connect.YWJj", "cf-json-hc"), 3))
	require.Nil(t, handshakeConnectData(newRequest("cf-json-hc", "cf-connect.YWJj"), 2))
	// Unpadded only.
	require.Equal(t, []byte("ab"), handshakeConnectData(newRequest("cf-connect.YWI"), 10))
	require.Nil(t, handshakeConnectData(newRequest("cf-connect.YWI="), 10))
	// URL alphabet only: 0xfb 0xff is "-_8" in base64url and "+/8" in standard.
	require.Equal(t, []byte{0xfb, 0xff}, handshakeConnectData(newRequest("cf-connect.-_8"), 10))
	require.Nil(t, handshakeConnectData(newRequest("cf-connect.+/8"), 10))
	// The first entry wins.
	require.Equal(t, []byte("abc"), handshakeConnectData(newRequest("cf-connect.YWJj", "cf-connect.YWI"), 10))
	// Spaces around entries are trimmed.
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.Header.Set("Sec-WebSocket-Protocol", "  cf-json-hc ,   cf-connect.YWJj  ")
	require.Equal(t, []byte("abc"), handshakeConnectData(r, 10))
	require.Nil(t, handshakeConnectData(newRequest("cf-json-hc"), 10))
	require.Nil(t, handshakeConnectData(newRequest("cf-connect."), 10))
	require.Nil(t, handshakeConnectData(newRequest("CF-CONNECT.YWJj"), 10))
	require.Nil(t, handshakeConnectData(newRequest("xcf-connect.YWJj"), 10))
	require.Nil(t, handshakeConnectData(httptest.NewRequest(http.MethodGet, "/", nil), 10))
}
