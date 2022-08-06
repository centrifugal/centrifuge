package centrifuge

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/centrifugal/protocol"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
)

func TestWebsocketHandler(t *testing.T) {
	n, _ := New(Config{})
	require.NoError(t, n.Run())
	defer func() { _ = n.Shutdown(context.Background()) }()
	mux := http.NewServeMux()
	mux.Handle("/connection/websocket", NewWebsocketHandler(n, WebsocketConfig{
		UseWriteBufferPool: true,
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}))
	server := httptest.NewServer(mux)
	defer server.Close()

	url := "ws" + server.URL[4:]
	conn, resp, err := websocket.DefaultDialer.Dial(url+"/connection/websocket", nil)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusSwitchingProtocols, resp.StatusCode)
	require.NotNil(t, conn)
	defer func() { _ = conn.Close() }()
}

func TestWebsocketHandlerProtocolV2(t *testing.T) {
	n, _ := New(Config{})
	require.NoError(t, n.Run())
	defer func() { _ = n.Shutdown(context.Background()) }()
	mux := http.NewServeMux()
	mux.Handle("/connection/websocket", NewWebsocketHandler(n, WebsocketConfig{
		UseWriteBufferPool: true,
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
		Compression: true,
	}))
	server := httptest.NewServer(mux)
	defer server.Close()

	url := "ws" + server.URL[4:]
	conn, resp, err := websocket.DefaultDialer.Dial(url+"/connection/websocket?cf_protocol_version=v2", nil)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusSwitchingProtocols, resp.StatusCode)
	require.NotNil(t, conn)
	defer func() { _ = conn.Close() }()
}

func TestWebsocketHandlerSubprotocol(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	done := make(chan struct{})

	node.OnConnecting(func(ctx context.Context, event ConnectEvent) (ConnectReply, error) {
		require.Equal(t, event.Transport.Protocol(), ProtocolTypeProtobuf)
		close(done)
		return ConnectReply{}, nil
	})

	mux := http.NewServeMux()
	mux.Handle("/connection/websocket", NewWebsocketHandler(node, WebsocketConfig{}))
	server := httptest.NewServer(mux)
	defer server.Close()

	url := "ws" + server.URL[4:]
	dialer := &websocket.Dialer{
		Proxy:            http.ProxyFromEnvironment,
		HandshakeTimeout: 45 * time.Second,
	}
	dialer.Subprotocols = []string{"centrifuge-protobuf"}
	conn, resp, err := dialer.Dial(url+"/connection/websocket", nil)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusSwitchingProtocols, resp.StatusCode)
	require.NotNil(t, conn)
	defer func() { _ = conn.Close() }()
	err = conn.WriteMessage(websocket.BinaryMessage, getConnectCommandProtobuf(t))
	require.NoError(t, err)
	waitWithTimeout(t, done)
}

func TestWebsocketHandlerURLParams(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	done := make(chan struct{})

	node.OnConnecting(func(ctx context.Context, event ConnectEvent) (ConnectReply, error) {
		require.Equal(t, event.Transport.Protocol(), ProtocolTypeProtobuf)
		require.Equal(t, event.Transport.ProtocolVersion(), ProtocolVersion1)
		close(done)
		return ConnectReply{}, nil
	})

	mux := http.NewServeMux()
	mux.Handle("/connection/websocket", NewWebsocketHandler(node, WebsocketConfig{
		ProtocolVersion: ProtocolVersion2,
	}))
	server := httptest.NewServer(mux)
	defer server.Close()

	url := "ws" + server.URL[4:]
	dialer := &websocket.Dialer{
		Proxy:            http.ProxyFromEnvironment,
		HandshakeTimeout: 45 * time.Second,
	}

	// Connect with invalid protocol version.
	_, _, err := dialer.Dial(url+"/connection/websocket?cf_protocol=protobuf&cf_protocol_version=v3", nil)
	require.Error(t, err)

	conn, resp, err := dialer.Dial(url+"/connection/websocket?cf_protocol=protobuf&cf_protocol_version=v1", nil)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusSwitchingProtocols, resp.StatusCode)
	require.NotNil(t, conn)
	defer func() { _ = conn.Close() }()
	err = conn.WriteMessage(websocket.BinaryMessage, getConnectCommandProtobuf(t))
	require.NoError(t, err)
	waitWithTimeout(t, done)
}

func TestWebsocketTransportWrite(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	node.OnConnecting(func(ctx context.Context, event ConnectEvent) (ConnectReply, error) {
		require.Equal(t, event.Transport.Protocol(), ProtocolTypeProtobuf)
		transport := event.Transport.(Transport)
		// Write to transport directly - this is only valid for tests, in normal situation
		// we write over client methods.
		require.NoError(t, transport.Write([]byte("hello")))
		return ConnectReply{}, DisconnectForceNoReconnect
	})

	mux := http.NewServeMux()
	mux.Handle("/connection/websocket", NewWebsocketHandler(node, WebsocketConfig{}))
	server := httptest.NewServer(mux)
	defer server.Close()

	url := "ws" + server.URL[4:]
	dialer := &websocket.Dialer{
		Proxy:            http.ProxyFromEnvironment,
		HandshakeTimeout: 45 * time.Second,
	}
	dialer.Subprotocols = []string{"centrifuge-protobuf"}
	conn, resp, err := dialer.Dial(url+"/connection/websocket", nil)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusSwitchingProtocols, resp.StatusCode)
	require.NotNil(t, conn)
	defer func() { _ = conn.Close() }()
	err = conn.WriteMessage(websocket.BinaryMessage, getConnectCommandProtobuf(t))
	require.NoError(t, err)

	msgType, msg, err := conn.ReadMessage()
	require.NoError(t, err)
	require.Equal(t, websocket.BinaryMessage, msgType)
	l, _ := binary.Uvarint(msg[0:])
	require.Equal(t, uint64(5), l)
}

func TestWebsocketTransportWriteMany(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	node.OnConnecting(func(ctx context.Context, event ConnectEvent) (ConnectReply, error) {
		require.Equal(t, event.Transport.Protocol(), ProtocolTypeProtobuf)
		transport := event.Transport.(Transport)
		// Write to transport directly - this is only valid for tests, in normal situation
		// we write over client methods.
		require.NoError(t, transport.WriteMany([]byte("11"), []byte("2")))
		return ConnectReply{}, DisconnectForceNoReconnect
	})

	mux := http.NewServeMux()
	mux.Handle("/connection/websocket", NewWebsocketHandler(node, WebsocketConfig{}))
	server := httptest.NewServer(mux)
	defer server.Close()

	url := "ws" + server.URL[4:]
	dialer := &websocket.Dialer{
		Proxy:            http.ProxyFromEnvironment,
		HandshakeTimeout: 45 * time.Second,
	}
	dialer.Subprotocols = []string{"centrifuge-protobuf"}
	conn, resp, err := dialer.Dial(url+"/connection/websocket", nil)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusSwitchingProtocols, resp.StatusCode)
	require.NotNil(t, conn)
	defer func() { _ = conn.Close() }()
	err = conn.WriteMessage(websocket.BinaryMessage, getConnectCommandProtobuf(t))
	require.NoError(t, err)

	msgType, msg, err := conn.ReadMessage()
	require.NoError(t, err)
	require.Equal(t, websocket.BinaryMessage, msgType)
	l1, n := binary.Uvarint(msg[0:])
	require.Equal(t, uint64(2), l1)
	l2, _ := binary.Uvarint(msg[n+int(l1):])
	require.Equal(t, uint64(1), l2)
}

func getConnectCommandProtobuf(t *testing.T) []byte {
	connectRequest := &protocol.ConnectRequest{}
	encoder := protocol.NewProtobufCommandEncoder()
	cmd, err := encoder.Encode(&protocol.Command{
		Id:      1,
		Connect: connectRequest,
	})
	require.NoError(t, err)
	return cmd
}

func waitWithTimeout(t *testing.T, ch chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(3 * time.Second):
		require.Fail(t, "timeout")
	}
}

func TestWebsocketHandlerProtobuf(t *testing.T) {
	n, _ := New(Config{})
	require.NoError(t, n.Run())
	defer func() { _ = n.Shutdown(context.Background()) }()
	mux := http.NewServeMux()
	mux.Handle("/connection/websocket", NewWebsocketHandler(n, WebsocketConfig{
		UseWriteBufferPool: true,
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}))
	server := httptest.NewServer(mux)
	defer server.Close()

	url := "ws" + server.URL[4:]
	conn, resp, err := websocket.DefaultDialer.Dial(url+"/connection/websocket?format=protobuf", nil)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusSwitchingProtocols, resp.StatusCode)
	require.NotNil(t, conn)
	defer func() { _ = conn.Close() }()
}

func TestWebsocketHandlerPing(t *testing.T) {
	n, _ := New(Config{})
	require.NoError(t, n.Run())
	defer func() { _ = n.Shutdown(context.Background()) }()
	mux := http.NewServeMux()
	mux.Handle("/connection/websocket", NewWebsocketHandler(n, WebsocketConfig{
		AppLevelPingInterval: time.Second,
	}))
	server := httptest.NewServer(mux)
	defer server.Close()

	n.OnConnecting(func(ctx context.Context, event ConnectEvent) (ConnectReply, error) {
		return ConnectReply{
			Credentials: &Credentials{
				UserID: "test",
			},
		}, nil
	})

	url := "ws" + server.URL[4:]

	conn, resp, err := websocket.DefaultDialer.Dial(url+"/connection/websocket", nil)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusSwitchingProtocols, resp.StatusCode)
	require.NotNil(t, conn)
	defer func() { _ = conn.Close() }()

	closeCh := make(chan struct{})

	err = conn.WriteMessage(websocket.TextMessage, []byte(`{"id": 1, "connect": {}}`))
	require.NoError(t, err)

	go func() {
		for {
			_, msg, err := conn.ReadMessage()
			if err != nil {
				break
			}
			if strings.Contains(string(msg), "{}") {
				close(closeCh)
				break
			}
		}
	}()

	select {
	case <-closeCh:
	case <-time.After(5 * time.Second):
		require.Fail(t, "timeout waiting for ping")
	}
}

func TestWebsocketHandlerCustomDisconnect(t *testing.T) {
	n, _ := New(Config{})
	require.NoError(t, n.Run())
	defer func() { _ = n.Shutdown(context.Background()) }()
	mux := http.NewServeMux()
	mux.Handle("/connection/websocket", NewWebsocketHandler(n, WebsocketConfig{}))
	server := httptest.NewServer(mux)
	defer server.Close()

	url := "ws" + server.URL[4:]

	conn, resp, err := websocket.DefaultDialer.Dial(url+"/connection/websocket", nil)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	var graceCh chan struct{}

	n.OnConnecting(func(ctx context.Context, event ConnectEvent) (ConnectReply, error) {
		graceCh = event.Transport.(*websocketTransport).graceCh
		return ConnectReply{}, DisconnectInvalidToken
	})

	connectRequest := &protocol.ConnectRequest{
		Token: "boom",
	}
	cmd := &protocol.Command{
		Id:      1,
		Connect: connectRequest,
	}
	cmdBytes, _ := json.Marshal(cmd)

	_ = conn.WriteMessage(websocket.TextMessage, cmdBytes)
	_, _, err = conn.ReadMessage()
	require.Error(t, err)
	closeErr, ok := err.(*websocket.CloseError)
	require.True(t, ok)
	require.Equal(t, int(DisconnectInvalidToken.Code), closeErr.Code)
	select {
	case <-graceCh:
	case <-time.After(5 * time.Second):
		require.Fail(t, "timeout waiting for graceful close")
	}
}

func newRealConnJSON(b testing.TB, channel string, url string) *websocket.Conn {
	conn := newRealConnJSONConnect(b, url)

	subscribeRequest := &protocol.SubscribeRequest{
		Channel: channel,
	}
	params, _ := json.Marshal(subscribeRequest)
	cmd := &protocol.Command{
		Id:     2,
		Method: protocol.Command_SUBSCRIBE,
		Params: params,
	}
	cmdBytes, _ := json.Marshal(cmd)
	_ = conn.WriteMessage(websocket.TextMessage, cmdBytes)
	_, _, err := conn.ReadMessage()
	require.NoError(b, err)
	return conn
}

func newRealConnProtobuf(b testing.TB, channel string, url string) *websocket.Conn {
	conn := newRealConnProtobufConnect(b, url)

	subscribeRequest := &protocol.SubscribeRequest{
		Channel: channel,
	}
	params, _ := subscribeRequest.MarshalVT()
	cmd := &protocol.Command{
		Id:     2,
		Method: protocol.Command_SUBSCRIBE,
		Params: params,
	}
	cmdBytes, _ := cmd.MarshalVT()

	var buf bytes.Buffer
	bs := make([]byte, 8)
	n := binary.PutUvarint(bs, uint64(len(cmdBytes)))
	buf.Write(bs[:n])
	buf.Write(cmdBytes)

	_ = conn.WriteMessage(websocket.BinaryMessage, buf.Bytes())
	_, _, err := conn.ReadMessage()
	require.NoError(b, err)
	return conn
}

func testAuthMiddleware(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		newCtx := SetCredentials(ctx, &Credentials{
			UserID: "test_user_id",
		})
		r = r.WithContext(newCtx)
		h.ServeHTTP(w, r)
	})
}

// TestWebsocketHandlerConcurrentConnections allows catching errors related
// to invalid buffer pool usages.
func TestWebsocketHandlerConcurrentConnections(t *testing.T) {
	n := defaultTestNode()
	defer func() { _ = n.Shutdown(context.Background()) }()

	mux := http.NewServeMux()
	mux.Handle("/connection/websocket", testAuthMiddleware(NewWebsocketHandler(n, WebsocketConfig{
		WriteBufferSize: 0,
		ReadBufferSize:  0,
	})))
	server := httptest.NewServer(mux)
	defer server.Close()

	url := "ws" + server.URL[4:]

	numConns := 100

	var conns []*websocket.Conn
	for i := 0; i < numConns; i++ {
		conn := newRealConnJSONV2(t, "test"+strconv.Itoa(i), url)
		conns = append(conns, conn)
	}
	defer func() {
		for _, conn := range conns {
			_ = conn.Close()
		}
	}()

	var wg sync.WaitGroup

	for i := 0; i < numConns; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			payload := []byte(`{"input":"test` + strconv.Itoa(i) + `"}`)

			_, err := n.Publish("test"+strconv.Itoa(i), payload)
			if err != nil {
				require.Fail(t, err.Error())
			}

			var firstNonPingMessage []byte
			for {
				_, data, err := conns[i].ReadMessage()
				if err != nil {
					require.Fail(t, err.Error())
				}
				messages := bytes.Split(data, []byte("\n"))
				for _, msg := range messages {
					if string(msg) == "{}" {
						continue
					}
					firstNonPingMessage = msg
				}
				if string(firstNonPingMessage) == "" {
					continue
				}
				break
			}

			var rep protocol.Reply
			err = json.Unmarshal(firstNonPingMessage, &rep)
			require.NoError(t, err)

			require.NotNil(t, rep.Push)
			require.NotNil(t, rep.Push.Pub)

			if !strings.Contains(string(rep.Push.Pub.Data), string(payload)) {
				require.Fail(t, "where is our payload? %s %s", string(payload), string(rep.Push.Pub.Data))
			}
		}(i)
	}

	wg.Wait()
}

func TestWebsocketHandlerConnectionsBroadcast(t *testing.T) {
	n := defaultTestNode()
	defer func() { _ = n.Shutdown(context.Background()) }()

	mux := http.NewServeMux()
	mux.Handle("/connection/websocket", testAuthMiddleware(NewWebsocketHandler(n, WebsocketConfig{
		WriteBufferSize: 0,
		ReadBufferSize:  0,
	})))
	server := httptest.NewServer(mux)
	defer server.Close()

	url := "ws" + server.URL[4:]

	numConns := 100

	var conns []*websocket.Conn
	for i := 0; i < numConns; i++ {
		conn := newRealConnJSONV2(t, "test", url)
		conns = append(conns, conn)
	}
	defer func() {
		for _, conn := range conns {
			_ = conn.Close()
		}
	}()

	payload := []byte(`{"input":"payload"}`)

	_, err := n.Publish("test", payload)
	if err != nil {
		require.Fail(t, err.Error())
	}

	var wg sync.WaitGroup

	for i := 0; i < numConns; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			var payloadMessage []byte
		LOOP:
			for {
				_, data, err := conns[i].ReadMessage()
				if err != nil {
					require.Fail(t, err.Error())
				}
				messages := bytes.Split(data, []byte("\n"))
				for _, msg := range messages {
					if strings.Contains(string(msg), "payload") {
						payloadMessage = msg
						break LOOP
					}
				}
			}

			var rep protocol.Reply
			err := json.Unmarshal(payloadMessage, &rep)
			require.NoError(t, err)

			require.NotNil(t, rep.Push)
			require.NotNil(t, rep.Push.Pub)

			if !strings.Contains(string(rep.Push.Pub.Data), string(payload)) {
				require.Fail(t, "where is our payload? %s %s", string(payload), string(rep.Push.Pub.Data))
			}
		}(i)
	}

	wg.Wait()
}

func TestCheckSameHostOrigin(t *testing.T) {
	t.Parallel()

	n := defaultTestNode()
	defer func() { _ = n.Shutdown(context.Background()) }()

	testCases := []struct {
		name    string
		origin  string
		url     string
		success bool
	}{
		{
			name:    "empty_origin",
			origin:  "",
			success: true,
			url:     "https://example.com/websocket/connection",
		},
		{
			name:    "invalid_host",
			origin:  "invalid",
			url:     "https://example.com/websocket/connection",
			success: false,
		},
		{
			name:    "unauthorized",
			origin:  "https://example.com",
			url:     "wss://example1.com/websocket/connection",
			success: false,
		},
		{
			name:    "authorized",
			origin:  "https://example.com",
			url:     "wss://example.com/websocket/connection",
			success: true,
		},
		{
			name:    "authorized_case_insensitive",
			origin:  "https://examplE.com",
			url:     "wss://example.com/websocket/connection",
			success: true,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			r := httptest.NewRequest("GET", tc.url, nil)
			r.Header.Set("Origin", tc.origin)

			require.Equal(t, tc.success, sameHostOriginCheck(n)(r))
		})
	}
}

// BenchmarkWsPubSub allows benchmarking full flow with one real
// Websocket connection subscribed to one channel. This is not very representative
// in terms of time for operation as network IO involved but useful to look at
// total allocations and difference between JSON and Protobuf cases using various buffer sizes.
func BenchmarkWsPubSubV1(b *testing.B) {
	n := defaultTestNodeBenchmark(b)
	defer func() { _ = n.Shutdown(context.Background()) }()

	mux := http.NewServeMux()
	mux.Handle("/connection/websocket", testAuthMiddleware(NewWebsocketHandler(n, WebsocketConfig{
		WriteBufferSize: 0,
		ReadBufferSize:  0,
	})))
	server := httptest.NewServer(mux)
	defer server.Close()

	url := "ws" + server.URL[4:]

	payload := []byte(`{"input": "test"}`)

	benchmarks := []struct {
		name    string
		getConn func(b testing.TB, channel string, url string) *websocket.Conn
	}{
		{"JSON", newRealConnJSON},
		{"PB", newRealConnProtobuf},
	}
	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			b.ReportAllocs()
			conn := bm.getConn(b, "test", url)
			defer func() { _ = conn.Close() }()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := n.Publish("test", payload)
				if err != nil {
					panic(err)
				}
				_, _, err = conn.ReadMessage()
				if err != nil {
					panic(err)
				}
			}
		})
	}
}

func newRealConnJSONConnect(b testing.TB, url string) *websocket.Conn {
	conn, resp, err := websocket.DefaultDialer.Dial(url+"/connection/websocket", nil)
	require.NoError(b, err)
	defer func() { _ = resp.Body.Close() }()

	connectRequest := &protocol.ConnectRequest{}
	params, _ := json.Marshal(connectRequest)
	cmd := &protocol.Command{
		Id:     1,
		Method: protocol.Command_CONNECT,
		Params: params,
	}
	cmdBytes, _ := json.Marshal(cmd)

	_ = conn.WriteMessage(websocket.TextMessage, cmdBytes)
	_, _, err = conn.ReadMessage()
	require.NoError(b, err)
	return conn
}

func newRealConnProtobufConnect(b testing.TB, url string) *websocket.Conn {
	conn, resp, err := websocket.DefaultDialer.Dial(url+"/connection/websocket?format=protobuf", nil)
	require.NoError(b, err)
	defer func() { _ = resp.Body.Close() }()

	connectRequest := &protocol.ConnectRequest{}
	params, _ := connectRequest.MarshalVT()
	cmd := &protocol.Command{
		Id:     1,
		Method: protocol.Command_CONNECT,
		Params: params,
	}

	cmdBytes, _ := cmd.MarshalVT()

	var buf bytes.Buffer
	bs := make([]byte, 8)
	n := binary.PutUvarint(bs, uint64(len(cmdBytes)))
	buf.Write(bs[:n])
	buf.Write(cmdBytes)

	_ = conn.WriteMessage(websocket.BinaryMessage, buf.Bytes())
	_, _, err = conn.ReadMessage()
	require.NoError(b, err)
	return conn
}

func BenchmarkWsConnectV1(b *testing.B) {
	n := defaultTestNodeBenchmark(b)
	defer func() { _ = n.Shutdown(context.Background()) }()

	n.OnConnect(func(client *Client) {})

	mux := http.NewServeMux()
	mux.Handle("/connection/websocket", testAuthMiddleware(NewWebsocketHandler(n, WebsocketConfig{
		WriteBufferSize: 0,
		ReadBufferSize:  0,
	})))
	server := httptest.NewServer(mux)
	defer server.Close()

	url := "ws" + server.URL[4:]

	benchmarks := []struct {
		name    string
		getConn func(b testing.TB, url string) *websocket.Conn
	}{
		{"JSON", newRealConnJSONConnect},
		{"PB", newRealConnProtobufConnect},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				conn := bm.getConn(b, url)
				_ = conn.Close()
			}
		})
	}
}

func BenchmarkWsConnectV2(b *testing.B) {
	b.Skip()
	n := defaultTestNodeBenchmark(b)
	defer func() { _ = n.Shutdown(context.Background()) }()

	n.OnConnect(func(client *Client) {})

	mux := http.NewServeMux()
	mux.Handle("/connection/websocket", testAuthMiddleware(NewWebsocketHandler(n, WebsocketConfig{
		ProtocolVersion: ProtocolVersion2,
		WriteBufferSize: 0,
		ReadBufferSize:  0,
	})))
	server := httptest.NewServer(mux)
	defer server.Close()

	url := "ws" + server.URL[4:]

	benchmarks := []struct {
		name    string
		getConn func(b testing.TB, url string) *websocket.Conn
	}{
		{"JSON", newRealConnJSONConnectV2},
		{"PB", newRealConnProtobufConnectV2},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				conn := bm.getConn(b, url)
				_ = conn.Close()
			}
		})
	}
}

func BenchmarkWsCommandReplyV1(b *testing.B) {
	n := defaultTestNodeBenchmark(b)
	defer func() { _ = n.Shutdown(context.Background()) }()

	n.OnConnect(func(client *Client) {
		client.OnRPC(func(event RPCEvent, callback RPCCallback) {
			callback(RPCReply{
				Data: []byte("{}"),
			}, nil)
		})
	})

	mux := http.NewServeMux()
	mux.Handle("/connection/websocket", testAuthMiddleware(NewWebsocketHandler(n, WebsocketConfig{
		WriteBufferSize: 0,
		ReadBufferSize:  0,
	})))
	server := httptest.NewServer(mux)
	defer server.Close()

	url := "ws" + server.URL[4:]

	payload := []byte(`{"input": "test"}`)

	benchmarks := []struct {
		name    string
		getConn func(b testing.TB, url string) *websocket.Conn
	}{
		{"JSON", newRealConnJSONConnect},
		{"PB", newRealConnProtobufConnect},
	}

	rpcRequest := &protocol.RPCRequest{
		Data: payload,
	}

	params, _ := json.Marshal(rpcRequest)
	cmd := &protocol.Command{
		Id:     1,
		Method: protocol.Command_RPC,
		Params: params,
	}
	jsonCommand, _ := json.Marshal(cmd)

	params, _ = rpcRequest.MarshalVT()
	cmd = &protocol.Command{
		Id:     1,
		Method: protocol.Command_RPC,
		Params: params,
	}
	cmdBytes, _ := cmd.MarshalVT()

	var buf bytes.Buffer
	bs := make([]byte, 8)
	nBytes := binary.PutUvarint(bs, uint64(len(cmdBytes)))
	buf.Write(bs[:nBytes])
	buf.Write(cmdBytes)

	protobufCommand := buf.Bytes()

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			b.ReportAllocs()
			conn := bm.getConn(b, url)
			defer func() { _ = conn.Close() }()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var err error
				if bm.name == "JSON" {
					err = conn.WriteMessage(websocket.TextMessage, jsonCommand)
				} else {
					err = conn.WriteMessage(websocket.BinaryMessage, protobufCommand)
				}
				if err != nil {
					b.Fatal(err)
				}
				_, _, err = conn.ReadMessage()
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func newRealConnJSONConnectV2(b testing.TB, url string) *websocket.Conn {
	conn, resp, err := websocket.DefaultDialer.Dial(url+"/connection/websocket", nil)
	require.NoError(b, err)
	defer func() { _ = resp.Body.Close() }()

	cmd := &protocol.Command{
		Id:      1,
		Connect: &protocol.ConnectRequest{},
	}
	cmdBytes, _ := json.Marshal(cmd)

	_ = conn.WriteMessage(websocket.TextMessage, cmdBytes)
	_, _, err = conn.ReadMessage()
	require.NoError(b, err)
	return conn
}

func newRealConnProtobufConnectV2(b testing.TB, url string) *websocket.Conn {
	conn, resp, err := websocket.DefaultDialer.Dial(url+"/connection/websocket?format=protobuf", nil)
	require.NoError(b, err)
	defer func() { _ = resp.Body.Close() }()

	cmd := &protocol.Command{
		Id:      1,
		Connect: &protocol.ConnectRequest{},
	}

	cmdBytes, _ := cmd.MarshalVT()

	var buf bytes.Buffer
	bs := make([]byte, 8)
	n := binary.PutUvarint(bs, uint64(len(cmdBytes)))
	buf.Write(bs[:n])
	buf.Write(cmdBytes)

	_ = conn.WriteMessage(websocket.BinaryMessage, buf.Bytes())
	_, _, err = conn.ReadMessage()
	require.NoError(b, err)
	return conn
}

func newRealConnJSONV2(b testing.TB, channel string, url string) *websocket.Conn {
	conn := newRealConnJSONConnectV2(b, url)

	cmd := &protocol.Command{
		Id: 2,
		Subscribe: &protocol.SubscribeRequest{
			Channel: channel,
		},
	}
	cmdBytes, _ := json.Marshal(cmd)
	_ = conn.WriteMessage(websocket.TextMessage, cmdBytes)
	_, _, err := conn.ReadMessage()
	require.NoError(b, err)
	return conn
}

func newRealConnProtobufV2(b testing.TB, channel string, url string) *websocket.Conn {
	conn := newRealConnProtobufConnectV2(b, url)

	cmd := &protocol.Command{
		Id: 2,
		Subscribe: &protocol.SubscribeRequest{
			Channel: channel,
		},
	}
	cmdBytes, _ := cmd.MarshalVT()

	var buf bytes.Buffer
	bs := make([]byte, 8)
	nBytes := binary.PutUvarint(bs, uint64(len(cmdBytes)))
	buf.Write(bs[:nBytes])
	buf.Write(cmdBytes)

	_ = conn.WriteMessage(websocket.BinaryMessage, buf.Bytes())
	_, _, err := conn.ReadMessage()
	require.NoError(b, err)
	return conn
}

func BenchmarkWsPubSubV2(b *testing.B) {
	n := defaultTestNodeBenchmark(b)
	defer func() { _ = n.Shutdown(context.Background()) }()

	mux := http.NewServeMux()
	mux.Handle("/connection/websocket", testAuthMiddleware(NewWebsocketHandler(n, WebsocketConfig{
		ProtocolVersion: ProtocolVersion2,
		WriteBufferSize: 0,
		ReadBufferSize:  0,
	})))
	server := httptest.NewServer(mux)
	defer server.Close()

	url := "ws" + server.URL[4:]

	payload := []byte(`{"input": "test"}`)

	benchmarks := []struct {
		name    string
		getConn func(b testing.TB, channel string, url string) *websocket.Conn
	}{
		{"JSON", newRealConnJSONV2},
		{"PB", newRealConnProtobufV2},
	}
	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			b.ReportAllocs()
			conn := bm.getConn(b, "test", url)
			defer func() { _ = conn.Close() }()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := n.Publish("test", payload)
				if err != nil {
					panic(err)
				}
				_, _, err = conn.ReadMessage()
				if err != nil {
					panic(err)
				}
			}
		})
	}
}

func BenchmarkWsCommandReplyV2(b *testing.B) {
	n := defaultTestNodeBenchmark(b)
	defer func() { _ = n.Shutdown(context.Background()) }()

	n.OnConnect(func(client *Client) {
		client.OnRPC(func(event RPCEvent, callback RPCCallback) {
			callback(RPCReply{
				Data: []byte("{}"),
			}, nil)
		})
	})

	mux := http.NewServeMux()
	mux.Handle("/connection/websocket", testAuthMiddleware(NewWebsocketHandler(n, WebsocketConfig{
		ProtocolVersion: ProtocolVersion2,
		WriteBufferSize: 0,
		ReadBufferSize:  0,
	})))
	server := httptest.NewServer(mux)
	defer server.Close()

	url := "ws" + server.URL[4:]

	payload := []byte(`{"input": "test"}`)

	benchmarks := []struct {
		name    string
		getConn func(b testing.TB, url string) *websocket.Conn
	}{
		{"JSON", newRealConnJSONConnectV2},
		{"PB", newRealConnProtobufConnectV2},
	}

	rpcRequest := &protocol.RPCRequest{
		Data: payload,
	}

	cmd := &protocol.Command{
		Id:  1,
		Rpc: rpcRequest,
	}
	jsonCommand, _ := json.Marshal(cmd)

	cmdBytes, _ := cmd.MarshalVT()

	var buf bytes.Buffer
	bs := make([]byte, 8)
	nBytes := binary.PutUvarint(bs, uint64(len(cmdBytes)))
	buf.Write(bs[:nBytes])
	buf.Write(cmdBytes)

	protobufCommand := buf.Bytes()

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			b.ReportAllocs()
			conn := bm.getConn(b, url)
			defer func() { _ = conn.Close() }()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var err error
				if bm.name == "JSON" {
					err = conn.WriteMessage(websocket.TextMessage, jsonCommand)
				} else {
					err = conn.WriteMessage(websocket.BinaryMessage, protobufCommand)
				}
				if err != nil {
					b.Fatal(err)
				}
				_, _, err = conn.ReadMessage()
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
