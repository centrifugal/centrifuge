package centrifuge

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/centrifugal/centrifuge/internal/websocket"

	"github.com/centrifugal/protocol"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func TestWebsocketHandler(t *testing.T) {
	t.Parallel()
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

	dialer := &websocket.Dialer{}

	url := "ws" + server.URL[4:]
	conn, resp, _, err := dialer.Dial(url+"/connection/websocket", nil)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusSwitchingProtocols, resp.StatusCode)
	require.NotNil(t, conn)
	defer func() { _ = conn.Close() }()
}

func TestWebsocketHandlerProtocolV2(t *testing.T) {
	t.Parallel()
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

	dialer := &websocket.Dialer{}
	url := "ws" + server.URL[4:]
	conn, resp, _, err := dialer.Dial(url+"/connection/websocket?cf_protocol_version=v2", nil)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusSwitchingProtocols, resp.StatusCode)
	require.NotNil(t, conn)
	defer func() { _ = conn.Close() }()
}

func TestWebsocketHandlerSubprotocol(t *testing.T) {
	t.Parallel()
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

	dialer := &websocket.Dialer{}
	url := "ws" + server.URL[4:]
	dialer.Subprotocols = []string{"centrifuge-protobuf"}
	conn, resp, _, err := dialer.Dial(url+"/connection/websocket", nil)
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
	t.Parallel()
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	done := make(chan struct{})

	node.OnConnecting(func(ctx context.Context, event ConnectEvent) (ConnectReply, error) {
		require.Equal(t, event.Transport.Protocol(), ProtocolTypeProtobuf)
		require.Equal(t, event.Transport.ProtocolVersion(), ProtocolVersion2)
		close(done)
		return ConnectReply{}, nil
	})

	mux := http.NewServeMux()
	mux.Handle("/connection/websocket", NewWebsocketHandler(node, WebsocketConfig{}))
	server := httptest.NewServer(mux)
	defer server.Close()

	url := "ws" + server.URL[4:]
	dialer := &websocket.Dialer{}

	conn, resp, _, err := dialer.Dial(url+"/connection/websocket?cf_protocol=protobuf&cf_protocol_version=v1", nil)
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
	t.Parallel()
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
		Subprotocols: []string{"centrifuge-protobuf"},
	}
	conn, resp, subprotocol, err := dialer.Dial(url+"/connection/websocket", nil)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusSwitchingProtocols, resp.StatusCode)
	require.NotNil(t, conn)
	defer func() { _ = conn.Close() }()
	err = conn.WriteMessage(websocket.BinaryMessage, getConnectCommandProtobuf(t))
	require.NoError(t, err)
	require.Equal(t, "centrifuge-protobuf", subprotocol)

	msgType, msg, err := conn.ReadMessage()
	require.NoError(t, err)
	require.Equal(t, websocket.BinaryMessage, msgType)
	l, _ := binary.Uvarint(msg[0:])
	require.Equal(t, uint64(5), l)
}

func TestWebsocketTransportWriteMany(t *testing.T) {
	t.Parallel()
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
		Subprotocols: []string{"centrifuge-protobuf"},
	}
	conn, resp, _, err := dialer.Dial(url+"/connection/websocket", nil)
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
	t.Parallel()
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

	dialer := &websocket.Dialer{}
	url := "ws" + server.URL[4:]
	conn, resp, _, err := dialer.Dial(url+"/connection/websocket?format=protobuf", nil)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusSwitchingProtocols, resp.StatusCode)
	require.NotNil(t, conn)
	defer func() { _ = conn.Close() }()
}

func TestWebsocketHandlerPing(t *testing.T) {
	t.Parallel()
	n, _ := New(Config{})
	require.NoError(t, n.Run())
	defer func() { _ = n.Shutdown(context.Background()) }()
	mux := http.NewServeMux()
	mux.Handle("/connection/websocket", NewWebsocketHandler(n, WebsocketConfig{
		PingPongConfig: PingPongConfig{
			PingInterval: time.Second,
		},
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

	dialer := &websocket.Dialer{}
	conn, resp, _, err := dialer.Dial(url+"/connection/websocket", nil)
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

func TestWebsocketHandler_FramePingPong(t *testing.T) {
	t.Parallel()
	defaultFramePingInterval = time.Second
	n, _ := New(Config{})
	require.NoError(t, n.Run())
	defer func() { _ = n.Shutdown(context.Background()) }()
	mux := http.NewServeMux()
	mux.Handle("/connection/websocket", NewWebsocketHandler(n, WebsocketConfig{}))
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

	dialer := &websocket.Dialer{}
	conn, resp, _, err := dialer.Dial(url+"/connection/websocket?cf_ws_frame_ping_pong=true", nil)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusSwitchingProtocols, resp.StatusCode)
	require.NotNil(t, conn)
	defer func() { _ = conn.Close() }()

	closeCh := make(chan struct{})

	conn.SetPingHandler(func(_ []byte) error {
		close(closeCh)
		return nil
	})

	err = conn.WriteMessage(websocket.TextMessage, []byte(`{"id": 1, "connect": {}}`))
	require.NoError(t, err)

	go func() {
		for {
			_, msg, err := conn.ReadMessage()
			if err != nil {
				break
			}
			if strings.Contains(string(msg), "{}") {
				require.Fail(t, "unexpected app-level ping")
			}
		}
	}()

	select {
	case <-closeCh:
	case <-time.After(5 * time.Second):
		require.Fail(t, "timeout waiting for frame ping")
	}
}

func TestWebsocketHandlerCustomDisconnect(t *testing.T) {
	t.Parallel()
	n, _ := New(Config{})
	require.NoError(t, n.Run())
	defer func() { _ = n.Shutdown(context.Background()) }()
	mux := http.NewServeMux()
	mux.Handle("/connection/websocket", NewWebsocketHandler(n, WebsocketConfig{}))
	server := httptest.NewServer(mux)
	defer server.Close()

	url := "ws" + server.URL[4:]
	dialer := &websocket.Dialer{}
	conn, resp, _, err := dialer.Dial(url+"/connection/websocket", nil)
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
	var closeErr *websocket.CloseError
	ok := errors.As(err, &closeErr)
	require.True(t, ok)
	require.Equal(t, int(DisconnectInvalidToken.Code), closeErr.Code)
	select {
	case <-graceCh:
	case <-time.After(5 * time.Second):
		require.Fail(t, "timeout waiting for graceful close")
	}
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
	t.Parallel()
	n := defaultTestNode()
	defer func() { _ = n.Shutdown(context.Background()) }()
	// This test stresses the buffer pool with 100 concurrent connections; the
	// default test-init pong timeout (500ms) is too tight when -race + parallel
	// suite contention stretches read scheduling. Disable pings/pongs entirely
	// — they are unrelated to the buffer-pool invariant under test.
	n.OnConnecting(func(_ context.Context, _ ConnectEvent) (ConnectReply, error) {
		return ConnectReply{
			PingPongConfig: &PingPongConfig{PingInterval: -1},
		}, nil
	})

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
		conn := newRealConnJSON(t, "test"+strconv.Itoa(i), url, false)
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
	t.Parallel()
	n := defaultTestNode()
	defer func() { _ = n.Shutdown(context.Background()) }()
	// Same rationale as TestWebsocketHandlerConcurrentConnections: 100
	// concurrent connections under -race stretch read scheduling past
	// the test-init 500 ms pong timeout. The test asserts buffer-pool
	// invariants, not ping/pong behavior, so disable pings for this run.
	n.OnConnecting(func(_ context.Context, _ ConnectEvent) (ConnectReply, error) {
		return ConnectReply{
			PingPongConfig: &PingPongConfig{PingInterval: -1},
		}, nil
	})

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
		conn := newRealConnJSON(t, "test", url, false)
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

func BenchmarkWsConnect(b *testing.B) {
	b.Skip()
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
		getConn func(b testing.TB, url string, compression bool) *websocket.Conn
	}{
		{"JSON", newRealConnJSONConnect},
		{"PB", newRealConnProtobufConnect},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				conn := bm.getConn(b, url, false)
				_ = conn.Close()
			}
		})
	}
}

func newRealConnJSONConnect(b testing.TB, url string, compression bool) *websocket.Conn {
	dialer := &websocket.Dialer{
		EnableCompression: compression,
	}
	conn, resp, _, err := dialer.Dial(url+"/connection/websocket", nil)
	require.NoError(b, err)
	defer func() { _ = resp.Body.Close() }()

	cmd := &protocol.Command{
		Id:      1,
		Connect: &protocol.ConnectRequest{},
	}
	cmdBytes, _ := json.Marshal(cmd)

	require.NoError(b, conn.WriteMessage(websocket.TextMessage, cmdBytes))
	_, _, err = conn.ReadMessage()
	require.NoError(b, err)
	return conn
}

func newRealConnProtobufConnect(b testing.TB, url string, compression bool) *websocket.Conn {
	dialer := &websocket.Dialer{
		EnableCompression: compression,
	}
	conn, resp, _, err := dialer.Dial(url+"/connection/websocket?format=protobuf", nil)
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

	require.NoError(b, conn.WriteMessage(websocket.BinaryMessage, buf.Bytes()))
	_, _, err = conn.ReadMessage()
	require.NoError(b, err)
	return conn
}

func newRealConnJSON(b testing.TB, channel string, url string, compression bool) *websocket.Conn {
	conn := newRealConnJSONConnect(b, url, compression)

	cmd := &protocol.Command{
		Id: 2,
		Subscribe: &protocol.SubscribeRequest{
			Channel: channel,
		},
	}
	cmdBytes, _ := json.Marshal(cmd)
	require.NoError(b, conn.WriteMessage(websocket.TextMessage, cmdBytes))
	_, _, err := conn.ReadMessage()
	require.NoError(b, err)
	return conn
}

func newRealConnProtobuf(b testing.TB, channel string, url string, compression bool) *websocket.Conn {
	conn := newRealConnProtobufConnect(b, url, compression)

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

func BenchmarkWsPubSub(b *testing.B) {
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
		getConn func(b testing.TB, channel string, url string, compression bool) *websocket.Conn
	}{
		{"JSON", newRealConnJSON},
		{"PB", newRealConnProtobuf},
	}
	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			b.ReportAllocs()
			conn := bm.getConn(b, "test", url, false)
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

func TestWsBroadcastCompressionCache(t *testing.T) {
	t.Parallel()
	n := defaultTestNode()
	defer func() { _ = n.Shutdown(context.Background()) }()

	payload := []byte(`{"input": "test"}`)

	tests := []struct {
		getConn     func(b testing.TB, channel string, url string, compression bool) *websocket.Conn
		compression bool
		cacheSizeMB int64
	}{
		{newRealConnJSON, false, 0},
		{newRealConnJSON, true, 0},
		{newRealConnJSON, true, 50},
	}

	numConns := 10

	for _, bm := range tests {
		testName := "compress_" + fmt.Sprintf("%v", bm.compression) + "_" +
			"cache_" + strconv.FormatInt(bm.cacheSizeMB, 10) + "MB"
		t.Run(testName, func(t *testing.T) {
			mux := http.NewServeMux()
			mux.Handle("/connection/websocket", testAuthMiddleware(NewWebsocketHandler(n, WebsocketConfig{
				Compression:                         true,
				CompressionPreparedMessageCacheSize: bm.cacheSizeMB * 1024 * 1024,
			})))
			server := httptest.NewServer(mux)
			defer server.Close()

			url := "ws" + server.URL[4:]

			connections := make([]*websocket.Conn, 0, numConns)
			for i := 0; i < numConns; i++ {
				conn := bm.getConn(t, "test", url, bm.compression)
				connections = append(connections, conn)
			}
			defer func() {
				for _, conn := range connections {
					_ = conn.Close()
				}
			}()
			_, err := n.Publish("test", payload)
			if err != nil {
				require.NoError(t, err)
			}
			for _, conn := range connections {
				_, _, err = conn.ReadMessage()
				require.NoError(t, err)
			}
		})
	}
}

func BenchmarkWsBroadcastCompressionCache(b *testing.B) {
	n := defaultTestNodeBenchmark(b)
	defer func() { _ = n.Shutdown(context.Background()) }()

	// Pre-generate payloads for unique message scenarios.
	numUniqueMessages := 100
	payloads := make([][]byte, numUniqueMessages)
	for i := 0; i < numUniqueMessages; i++ {
		payloads[i] = []byte(fmt.Sprintf(`{"input": "test", "id": %d}`, i))
	}

	benchmarks := []struct {
		name           string
		compression    bool
		cacheSizeMB    int64
		uniqueMessages int // number of unique messages to cycle through
	}{
		{"no_compress", false, 0, 1},
		{"compress_no_cache", true, 0, 1},
		{"compress_cache_same_msg", true, 50, 1},
		{"compress_cache_100_msgs", true, 50, 100},
	}

	numConns := 100

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			b.ReportAllocs()

			mux := http.NewServeMux()
			mux.Handle("/connection/websocket", testAuthMiddleware(NewWebsocketHandler(n, WebsocketConfig{
				Compression:                         true,
				CompressionPreparedMessageCacheSize: bm.cacheSizeMB * 1024 * 1024,
			})))
			server := httptest.NewServer(mux)
			defer server.Close()

			url := "ws" + server.URL[4:]

			conns := make([]*websocket.Conn, 0, numConns)
			for i := 0; i < numConns; i++ {
				conn := newRealConnJSON(b, "test", url, bm.compression)
				conns = append(conns, conn)
			}
			defer func() {
				for _, conn := range conns {
					_ = conn.Close()
				}
			}()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				payload := payloads[i%bm.uniqueMessages]
				_, err := n.Publish("test", payload)
				if err != nil {
					panic(err)
				}
				for _, conn := range conns {
					_, _, err = conn.ReadMessage()
					if err != nil {
						panic(err)
					}
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
		WriteBufferSize: 0,
		ReadBufferSize:  0,
	})))
	server := httptest.NewServer(mux)
	defer server.Close()

	url := "ws" + server.URL[4:]

	payload := []byte(`{"input": "test"}`)

	benchmarks := []struct {
		name    string
		getConn func(b testing.TB, url string, compression bool) *websocket.Conn
	}{
		{"JSON", newRealConnJSONConnect},
		{"PB", newRealConnProtobufConnect},
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
			conn := bm.getConn(b, url, false)
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

func BenchmarkWsCommandReplyV2Multiple(b *testing.B) {
	n := defaultTestNodeBenchmark(b)
	defer func() { _ = n.Shutdown(context.Background()) }()

	n.OnConnect(func(client *Client) {
		client.OnRPC(func(event RPCEvent, callback RPCCallback) {
			callback(RPCReply{
				Data: []byte(`{"test_response": 1}`),
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
		getConn func(b testing.TB, url string, compression bool) *websocket.Conn
	}{
		{"JSON", newRealConnJSONConnect},
		{"PB", newRealConnProtobufConnect},
	}

	rpcRequest := &protocol.RPCRequest{
		Data: payload,
	}

	cmd := &protocol.Command{
		Id:  1,
		Rpc: rpcRequest,
	}
	jsonBytes, _ := json.Marshal(cmd)
	jsonCommand := append([]byte{}, jsonBytes...)
	jsonCommand = append(jsonCommand, []byte("\n")...)
	jsonCommand = append(jsonCommand, jsonBytes...)
	jsonCommand = append(jsonCommand, []byte("\n")...)
	jsonCommand = append(jsonCommand, jsonBytes...)
	jsonCommand = append(jsonCommand, []byte("\n")...)
	jsonCommand = append(jsonCommand, jsonBytes...)

	cmdBytes, _ := cmd.MarshalVT()

	var buf bytes.Buffer
	bs := make([]byte, 8)
	nBytes := binary.PutUvarint(bs, uint64(len(cmdBytes)))
	buf.Write(bs[:nBytes])
	buf.Write(cmdBytes)
	bs = make([]byte, 8)
	nBytes = binary.PutUvarint(bs, uint64(len(cmdBytes)))
	buf.Write(bs[:nBytes])
	buf.Write(cmdBytes)
	bs = make([]byte, 8)
	nBytes = binary.PutUvarint(bs, uint64(len(cmdBytes)))
	buf.Write(bs[:nBytes])
	buf.Write(cmdBytes)
	bs = make([]byte, 8)
	nBytes = binary.PutUvarint(bs, uint64(len(cmdBytes)))
	buf.Write(bs[:nBytes])
	buf.Write(cmdBytes)

	protobufCommand := buf.Bytes()

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			b.ReportAllocs()
			conn := bm.getConn(b, url, false)
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
				var n int
				for {
					_, d, err := conn.ReadMessage()
					if err != nil {
						b.Fatal(err)
					}
					n += strings.Count(string(d), "test_response")
					if n == 4 {
						break
					}
				}
			}
		})
	}
}

func BenchmarkWsCommandReplyV2MultipleParallel(b *testing.B) {
	n := defaultTestNodeBenchmark(b)
	defer func() { _ = n.Shutdown(context.Background()) }()

	n.OnConnect(func(client *Client) {
		client.OnRPC(func(event RPCEvent, callback RPCCallback) {
			go func() {
				callback(RPCReply{
					Data: event.Data,
				}, nil)
			}()
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

	benchmarks := []struct {
		name    string
		getConn func(b testing.TB, url string, compression bool) *websocket.Conn
	}{
		{"JSON", newRealConnJSONConnect},
		{"PB", newRealConnProtobufConnect},
	}

	cmd1 := &protocol.Command{
		Id: 1,
		Rpc: &protocol.RPCRequest{
			Data: []byte(`{"input":"test1"}`),
		},
	}
	cmd2 := &protocol.Command{
		Id: 2,
		Rpc: &protocol.RPCRequest{
			Data: []byte(`{"input":"test2"}`),
		},
	}
	cmd3 := &protocol.Command{
		Id: 3,
		Rpc: &protocol.RPCRequest{
			Data: []byte(`{"input":"test3"}`),
		},
	}

	jsonBytes1, _ := json.Marshal(cmd1)
	jsonCommand := append([]byte{}, jsonBytes1...)
	jsonCommand = append(jsonCommand, []byte("\n")...)
	jsonBytes2, _ := json.Marshal(cmd2)
	jsonCommand = append(jsonCommand, jsonBytes2...)
	jsonCommand = append(jsonCommand, []byte("\n")...)
	jsonBytes3, _ := json.Marshal(cmd3)
	jsonCommand = append(jsonCommand, jsonBytes3...)

	cmdBytes1, _ := cmd1.MarshalVT()
	cmdBytes2, _ := cmd2.MarshalVT()
	cmdBytes3, _ := cmd3.MarshalVT()
	var buf bytes.Buffer
	bs := make([]byte, 8)
	nBytes := binary.PutUvarint(bs, uint64(len(cmdBytes1)))
	buf.Write(bs[:nBytes])
	buf.Write(cmdBytes1)
	bs = make([]byte, 8)
	nBytes = binary.PutUvarint(bs, uint64(len(cmdBytes2)))
	buf.Write(bs[:nBytes])
	buf.Write(cmdBytes2)
	bs = make([]byte, 8)
	nBytes = binary.PutUvarint(bs, uint64(len(cmdBytes3)))
	buf.Write(bs[:nBytes])
	buf.Write(cmdBytes3)
	protobufCommand := buf.Bytes()

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			b.RunParallel(func(pb *testing.PB) {
				conn := bm.getConn(b, url, false)
				defer func() { _ = conn.Close() }()
				b.ResetTimer()
				b.ReportAllocs()
				for pb.Next() {
					var err error
					if bm.name == "JSON" {
						err = conn.WriteMessage(websocket.TextMessage, jsonCommand)
					} else {
						err = conn.WriteMessage(websocket.BinaryMessage, protobufCommand)
					}
					if err != nil {
						b.Fatal(err)
					}
					var n int
					for {
						_, d, err := conn.ReadMessage()
						if err != nil {
							b.Fatal(err)
						}
						var dec protocol.ReplyDecoder
						if bm.name == "JSON" {
							dec = protocol.NewJSONReplyDecoder(d)
						} else {
							dec = protocol.NewProtobufReplyDecoder(d)
						}
						for {
							reply, err := dec.Decode()
							if reply != nil {
								if reply.Rpc == nil {
									continue
								}
								if reply.Id == 1 && !bytes.Equal(reply.Rpc.Data, []byte(`{"input":"test1"}`)) {
									b.Fatal("unexpected payload")
								}
								if reply.Id == 2 && !bytes.Equal(reply.Rpc.Data, []byte(`{"input":"test2"}`)) {
									b.Fatal("unexpected payload")
								}
								if reply.Id == 3 && !bytes.Equal(reply.Rpc.Data, []byte(`{"input":"test3"}`)) {
									b.Fatal("unexpected payload")
								}
								n += 1
							}
							if err == io.EOF {
								break
							}
						}
						if n == 3 {
							break
						}
					}
				}
			})
		})
	}
}

// TestWebsocketTransportAcceptProtocol verifies AcceptProtocol gets propagated from
// the original HTTP request's protocol version.
func TestWebsocketTransportAcceptProtocol(t *testing.T) {
	t.Parallel()
	transport := &websocketTransport{
		opts: websocketTransportOptions{
			protoMajor: 1,
			protoType:  ProtocolTypeJSON,
		},
	}
	require.Equal(t, "h1", transport.AcceptProtocol())
}

// dialCompressed opens a permessage-deflate WebSocket connection to handler and
// returns the client conn. The client compresses outbound frames at the maximum
// level so a redundant payload yields a tiny compressed frame.
func dialCompressed(t *testing.T, config WebsocketConfig) *websocket.Conn {
	t.Helper()
	n, _ := New(Config{})
	require.NoError(t, n.Run())
	t.Cleanup(func() { _ = n.Shutdown(context.Background()) })

	config.CheckOrigin = func(r *http.Request) bool { return true }
	mux := http.NewServeMux()
	mux.Handle("/connection/websocket", NewWebsocketHandler(n, config))
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	dialer := &websocket.Dialer{EnableCompression: true}
	url := "ws" + server.URL[4:]
	conn, resp, _, err := dialer.Dial(url+"/connection/websocket", nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = resp.Body.Close() })
	t.Cleanup(func() { _ = conn.Close() })
	require.NoError(t, conn.SetCompressionLevel(9))
	return conn
}

// TestWebsocketHandlerDecompressionBombRejected proves end-to-end that with
// compression enabled the default decompressed limit (MessageSizeLimit *
// defaultWebsocketDecompressedMessageSizeLimitMultiplier) rejects a tiny
// compressed frame that inflates past the limit, before any authentication.
func TestWebsocketHandlerDecompressionBombRejected(t *testing.T) {
	t.Parallel()
	// MessageSizeLimit 1024 -> decompressed limit = 1024 * 10 = 10240 bytes.
	conn := dialCompressed(t, WebsocketConfig{
		Compression:      true,
		MessageSizeLimit: 1024,
	})

	// 64KB of zeros compresses to well under the 1024-byte compressed limit but
	// inflates far past the 10240-byte decompressed limit.
	bomb := make([]byte, 64*1024)
	require.NoError(t, conn.WriteMessage(websocket.BinaryMessage, bomb))

	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, _, err := conn.ReadMessage()
	require.Error(t, err)
	require.Truef(t, websocket.IsCloseError(err, websocket.CloseMessageTooBig),
		"expected close %d, got %v", websocket.CloseMessageTooBig, err)
	var closeErr *websocket.CloseError
	require.ErrorAs(t, err, &closeErr)
	require.Contains(t, closeErr.Text, "after decompression")
}

// TestWebsocketHandlerOutgoingCloseMetric proves the server-sent 1009 close from
// the decompression-limit path is recorded in the outgoing close metric labeled
// by transport and code.
func TestWebsocketHandlerOutgoingCloseMetric(t *testing.T) {
	t.Parallel()
	n, _ := New(Config{})
	require.NoError(t, n.Run())
	defer func() { _ = n.Shutdown(context.Background()) }()
	mux := http.NewServeMux()
	mux.Handle("/connection/websocket", NewWebsocketHandler(n, WebsocketConfig{
		Compression:      true,
		MessageSizeLimit: 1024, // decompressed limit = 10240
		CheckOrigin:      func(r *http.Request) bool { return true },
	}))
	server := httptest.NewServer(mux)
	defer server.Close()

	dialer := &websocket.Dialer{EnableCompression: true}
	url := "ws" + server.URL[4:]
	conn, resp, _, err := dialer.Dial(url+"/connection/websocket", nil)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	defer func() { _ = conn.Close() }()
	require.NoError(t, conn.SetCompressionLevel(9))

	bomb := make([]byte, 64*1024)
	require.NoError(t, conn.WriteMessage(websocket.BinaryMessage, bomb))
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, _, _ = conn.ReadMessage() // server closes with 1009

	counter := n.metrics.transportOutgoingCloseCount.WithLabelValues(transportWebsocket, "1009")
	require.Eventually(t, func() bool {
		var m dto.Metric
		if err := counter.Write(&m); err != nil {
			return false
		}
		return m.GetCounter().GetValue() == 1
	}, 2*time.Second, 10*time.Millisecond, "expected outgoing close 1009 to be metered")
}

// TestWebsocketHandlerDecompressedMessageSizeLimitOverride proves the explicit
// DecompressedMessageSizeLimit takes precedence over the multiplier-derived
// default: a payload that would pass the multiplier limit (10240) but exceeds
// the override (2048) is rejected.
func TestWebsocketHandlerDecompressedMessageSizeLimitOverride(t *testing.T) {
	t.Parallel()
	conn := dialCompressed(t, WebsocketConfig{
		Compression:                  true,
		MessageSizeLimit:             1024, // multiplier default would be 10240
		DecompressedMessageSizeLimit: 2048, // explicit, must win
	})

	// 4096 zeros: > 2048 (override) but < 10240 (multiplier). A 1009 close
	// therefore proves the override is in effect; otherwise the frame would
	// pass the size gate and be rejected later as an invalid command.
	payload := make([]byte, 4096)
	require.NoError(t, conn.WriteMessage(websocket.BinaryMessage, payload))

	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, _, err := conn.ReadMessage()
	require.Error(t, err)
	require.Truef(t, websocket.IsCloseError(err, websocket.CloseMessageTooBig),
		"expected close %d (override in effect), got %v", websocket.CloseMessageTooBig, err)
}

// TestWebsocketHandlerCompressedHeadroom proves the multiplier leaves headroom
// above MessageSizeLimit for legitimately compressible inbound frames: a payload
// larger than MessageSizeLimit but within the decompressed limit is NOT rejected
// for being too big (it is only later rejected as an invalid command, with a
// different close code).
func TestWebsocketHandlerCompressedHeadroom(t *testing.T) {
	t.Parallel()
	conn := dialCompressed(t, WebsocketConfig{
		Compression:      true,
		MessageSizeLimit: 1024, // decompressed limit = 10240
	})

	// 4096 zeros: > MessageSizeLimit (1024) but < decompressed limit (10240).
	payload := make([]byte, 4096)
	require.NoError(t, conn.WriteMessage(websocket.BinaryMessage, payload))

	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, _, err := conn.ReadMessage()
	require.Error(t, err) // invalid command -> disconnect, but...
	require.Falsef(t, websocket.IsCloseError(err, websocket.CloseMessageTooBig),
		"frame within decompressed limit must not be rejected as too big, got %v", err)
}

// Tests for WebsocketConfig.ProcessCommandsOffReadLoop. The option moves frame
// processing to a goroutine per frame, so the properties worth pinning are that
// the connection still processes commands strictly in order and that the extra
// goroutines really do go away.

// newOffReadLoopServer starts a node and websocket handler. writeWithTimer
// removes the per-connection writer goroutine, which lets a goroutine-count
// assertion be about the read loop alone.
func newOffReadLoopServer(t *testing.T, offReadLoop bool, writeWithTimer bool) (*Node, string) {
	t.Helper()
	n, err := New(Config{LogLevel: LogLevelError})
	require.NoError(t, err)
	n.OnConnecting(func(_ context.Context, _ ConnectEvent) (ConnectReply, error) {
		reply := ConnectReply{Credentials: &Credentials{UserID: "test"}}
		if writeWithTimer {
			reply.WriteDelay = time.Millisecond
			reply.WriteWithTimer = true
		}
		return reply, nil
	})
	n.OnConnect(func(client *Client) {
		client.OnRPC(func(e RPCEvent, cb RPCCallback) {
			// Echo the request back so the reply can be tied to its command.
			cb(RPCReply{Data: e.Data}, nil)
		})
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{}, nil)
		})
	})
	require.NoError(t, n.Run())

	mux := http.NewServeMux()
	mux.Handle("/connection/websocket", NewWebsocketHandler(n, WebsocketConfig{
		PingPongConfig:             PingPongConfig{PingInterval: time.Hour, PongTimeout: time.Minute},
		ProcessCommandsOffReadLoop: offReadLoop,
	}))
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	t.Cleanup(func() { _ = n.Shutdown(context.Background()) })
	return n, "ws" + server.URL[4:]
}

func dialAndConnect(t *testing.T, url string) *websocket.Conn {
	t.Helper()
	dialer := &websocket.Dialer{}
	conn, resp, _, err := dialer.Dial(url+"/connection/websocket", nil)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	cmd, err := json.Marshal(&protocol.Command{Id: 1, Connect: &protocol.ConnectRequest{}})
	require.NoError(t, err)
	require.NoError(t, conn.WriteMessage(websocket.TextMessage, cmd))
	_, _, err = conn.ReadMessage()
	require.NoError(t, err)
	return conn
}

// readReplyIDs reads until it has collected want reply ids, in arrival order.
func readReplyIDs(t *testing.T, conn *websocket.Conn, want int) []uint32 {
	t.Helper()
	ids := make([]uint32, 0, want)
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(20*time.Second)))
	for len(ids) < want {
		_, data, err := conn.ReadMessage()
		require.NoError(t, err)
		for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
			if line == "" {
				continue
			}
			var reply struct {
				ID uint32 `json:"id"`
				// Checked, not just collected: a command that was rejected still
				// produces a reply carrying its id, so ignoring the error field
				// lets a test sail past a subscribe that never took effect.
				Error json.RawMessage `json:"error"`
			}
			require.NoError(t, json.Unmarshal([]byte(line), &reply))
			require.Empty(t, reply.Error, "command %d failed", reply.ID)
			if reply.ID != 0 {
				ids = append(ids, reply.ID)
			}
		}
	}
	return ids
}

// TestProcessCommandsOffReadLoopPreservesOrder pipelines commands without
// waiting for replies. Handing each frame to its own goroutine must not let
// frames overtake one another: the read loop waits for each before reading the
// next, and these replies must come back in the order the commands were sent.
func TestProcessCommandsOffReadLoopPreservesOrder(t *testing.T) {
	t.Parallel()
	for _, offReadLoop := range []bool{false, true} {
		offReadLoop := offReadLoop
		name := "inline"
		if offReadLoop {
			name = "off_read_loop"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, url := newOffReadLoopServer(t, offReadLoop, false)
			conn := dialAndConnect(t, url)
			defer func() { _ = conn.Close() }()

			const numCommands = 200
			for i := 0; i < numCommands; i++ {
				cmd, err := json.Marshal(&protocol.Command{
					Id:  uint32(2 + i),
					Rpc: &protocol.RPCRequest{Method: "echo", Data: []byte(`{}`)},
				})
				require.NoError(t, err)
				require.NoError(t, conn.WriteMessage(websocket.TextMessage, cmd))
			}

			ids := readReplyIDs(t, conn, numCommands)
			require.Len(t, ids, numCommands)
			for i, id := range ids {
				require.Equal(t, uint32(2+i), id, "reply %d out of order", i)
			}
		})
	}
}

// TestProcessCommandsOffReadLoopMultipleCommandsPerFrame puts several commands
// in one frame, which the stream decoder splits. The whole frame is handled by
// a single handoff, so this checks the batching path is unaffected.
func TestProcessCommandsOffReadLoopMultipleCommandsPerFrame(t *testing.T) {
	t.Parallel()
	_, url := newOffReadLoopServer(t, true, false)
	conn := dialAndConnect(t, url)
	defer func() { _ = conn.Close() }()

	const perFrame = 10
	var frame strings.Builder
	for i := 0; i < perFrame; i++ {
		cmd, err := json.Marshal(&protocol.Command{
			Id:  uint32(2 + i),
			Rpc: &protocol.RPCRequest{Method: "echo", Data: []byte(`{}`)},
		})
		require.NoError(t, err)
		frame.Write(cmd)
		frame.WriteString("\n")
	}
	require.NoError(t, conn.WriteMessage(websocket.TextMessage, []byte(frame.String())))

	ids := readReplyIDs(t, conn, perFrame)
	require.Len(t, ids, perFrame)
	for i, id := range ids {
		require.Equal(t, uint32(2+i), id)
	}
}

// TestProcessCommandsOffReadLoopNoGoroutineLeak checks the per-frame goroutines
// are transient and that closing the connections releases the read loops. A
// leak here would turn the option from a stack saving into a goroutine leak.
func TestProcessCommandsOffReadLoopNoGoroutineLeak(t *testing.T) {
	// The timer-driven writer leaves the read loop as the only lasting
	// per-connection goroutine, so the count below is a direct statement about
	// the per-frame handoff goroutines being transient.
	_, url := newOffReadLoopServer(t, true, true)

	settleGoroutines := func() int {
		last, stable := -1, 0
		for i := 0; i < 200; i++ {
			runtime.GC()
			n := runtime.NumGoroutine()
			if n == last {
				if stable++; stable >= 3 {
					return n
				}
			} else {
				stable, last = 0, n
			}
			time.Sleep(20 * time.Millisecond)
		}
		return runtime.NumGoroutine()
	}

	before := settleGoroutines()

	const numConns = 20
	conns := make([]*websocket.Conn, 0, numConns)
	for i := 0; i < numConns; i++ {
		conn := dialAndConnect(t, url)
		for j := 0; j < 5; j++ {
			cmd, err := json.Marshal(&protocol.Command{
				Id:  uint32(2 + j),
				Rpc: &protocol.RPCRequest{Method: "echo", Data: []byte(`{}`)},
			})
			require.NoError(t, err)
			require.NoError(t, conn.WriteMessage(websocket.TextMessage, cmd))
		}
		readReplyIDs(t, conn, 5)
		conns = append(conns, conn)
	}

	// While connected, each connection should hold its read loop and nothing
	// more: the per-frame goroutines have all exited by now.
	live := settleGoroutines()
	require.LessOrEqual(t, live-before, numConns+5,
		"expected about one goroutine per connection (the read loop), got %d extra for %d connections",
		live-before, numConns)

	for _, c := range conns {
		_ = c.Close()
	}
	after := settleGoroutines()
	require.LessOrEqual(t, after-before, 5, "goroutines leaked after disconnect: %d extra", after-before)
}

// TestProcessCommandsOffReadLoopInterleavedWithPushes is the messiest realistic
// shape: a client pipelining commands while the server pushes into a channel it
// is subscribed to. Replies and pushes are produced by different goroutines and
// interleave in the connection's write queue, while command processing itself
// has moved to a goroutine per frame.
//
// Replies must still come back in command order, every command must be
// answered, and no push may be lost. Run with -race this also exercises the
// handoff's synchronisation against the broadcast path.
func TestProcessCommandsOffReadLoopInterleavedWithPushes(t *testing.T) {
	t.Parallel()
	n, url := newOffReadLoopServer(t, true, false)
	conn := dialAndConnect(t, url)
	defer func() { _ = conn.Close() }()

	const channel = "interleave"
	sub, err := json.Marshal(&protocol.Command{
		Id: 2, Subscribe: &protocol.SubscribeRequest{Channel: channel},
	})
	require.NoError(t, err)
	require.NoError(t, conn.WriteMessage(websocket.TextMessage, sub))
	require.Equal(t, []uint32{2}, readReplyIDs(t, conn, 1))

	const numCommands = 150
	const numPushes = 150

	// Push from the server while the client pipelines commands, so the two
	// streams are produced concurrently rather than in phases.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < numPushes; i++ {
			_, pErr := n.Publish(channel, []byte(`{"push":1}`))
			require.NoError(t, pErr)
		}
	}()

	for i := 0; i < numCommands; i++ {
		cmd, mErr := json.Marshal(&protocol.Command{
			Id:  uint32(3 + i),
			Rpc: &protocol.RPCRequest{Method: "echo", Data: []byte(`{}`)},
		})
		require.NoError(t, mErr)
		require.NoError(t, conn.WriteMessage(websocket.TextMessage, cmd))
	}
	wg.Wait()

	// Collect until every reply and every push has arrived.
	var replyIDs []uint32
	pushes := 0
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(30*time.Second)))
	for len(replyIDs) < numCommands || pushes < numPushes {
		_, data, rErr := conn.ReadMessage()
		require.NoError(t, rErr, "got %d/%d replies and %d/%d pushes",
			len(replyIDs), numCommands, pushes, numPushes)
		for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
			if line == "" {
				continue
			}
			var msg struct {
				ID   uint32          `json:"id"`
				Push json.RawMessage `json:"push"`
			}
			require.NoError(t, json.Unmarshal([]byte(line), &msg))
			switch {
			case msg.ID != 0:
				replyIDs = append(replyIDs, msg.ID)
			case len(msg.Push) > 0:
				pushes++
			}
		}
	}

	require.Len(t, replyIDs, numCommands)
	for i, id := range replyIDs {
		require.Equal(t, uint32(3+i), id, "reply %d out of order", i)
	}
	require.Equal(t, numPushes, pushes)
}

// What a connection costs while it is doing nothing is what caps connection
// density. These benchmarks hold b.N *real* WebSocket connections open, put
// each through a realistic command sequence, let them go idle, and then report
// what each one still occupies.
//
// The number this file exists to measure is stack-B/conn. A goroutine's stack
// grows to the deepest call it ever makes and is not handed back while the
// goroutine lives, so a read loop that has processed a single publish keeps the
// stack that the whole dispatch-and-fan-out chain needed, for the entire life
// of the connection. That is what WebsocketConfig.ProcessCommandsOffReadLoop
// targets, and the Inline/OffReadLoop pairs below are its A/B.
//
// Both ends of every connection live in this process, so heap-B/conn covers the
// client half too and is only meaningful relative to another run of this same
// benchmark. stack-B/conn is not affected the same way: the client half has no
// goroutine of its own here - the benchmark goroutine drives it - so
// essentially all of it is server-side read loops.
//
// Set CENTRIFUGE_BENCH_HEAP_PROFILE to a path to also write an inuse_space
// profile with every connection live.

// settle blocks until the goroutine count stops moving, so a measurement
// baseline is not taken while previous work is still unwinding.
func settle(b *testing.B) {
	b.Helper()
	deadline := time.Now().Add(10 * time.Second)
	last := -1
	stable := 0
	for time.Now().Before(deadline) {
		runtime.GC()
		n := runtime.NumGoroutine()
		if n == last {
			if stable++; stable >= 3 {
				return
			}
		} else {
			stable = 0
			last = n
		}
		time.Sleep(20 * time.Millisecond)
	}
	b.Log("goroutine count did not settle; footprint baseline may be noisy")
}

type wsFootprintParams struct {
	ws             WebsocketConfig
	writeDelay     time.Duration
	writeWithTimer bool
	// commandsPerConn is how many publishes each connection issues before going
	// idle. It must be at least one: a connection that never sends a command
	// never drives the dispatch chain, its read loop stack never grows, and the
	// benchmark would report an identical footprint for both variants while
	// measuring nothing at all.
	commandsPerConn int
}

// benchWsRoundTrip sends one command and reads until the matching reply comes
// back, skipping the pushes a self-subscribed publish generates. Returning only
// on the matching id keeps the connection in lockstep, so a command that
// silently failed surfaces as a failure rather than as a faster benchmark.
func benchWsRoundTrip(b *testing.B, conn *websocket.Conn, cmd *protocol.Command) {
	b.Helper()
	raw, err := json.Marshal(cmd)
	if err != nil {
		b.Fatal(err)
	}
	if err = conn.WriteMessage(websocket.TextMessage, raw); err != nil {
		b.Fatal(err)
	}
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		_, data, rErr := conn.ReadMessage()
		if rErr != nil {
			b.Fatal(rErr)
		}
		for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
			if line == "" {
				continue
			}
			var reply struct {
				ID    uint32          `json:"id"`
				Error json.RawMessage `json:"error"`
			}
			if err = json.Unmarshal([]byte(line), &reply); err != nil {
				b.Fatalf("decoding reply %q: %v", line, err)
			}
			if len(reply.Error) > 0 {
				b.Fatalf("command %d failed: %s", cmd.Id, reply.Error)
			}
			if reply.ID == cmd.Id {
				return
			}
		}
	}
	b.Fatalf("no reply for command %d", cmd.Id)
}

// benchWsExerciseConn subscribes the connection and has it publish, so the read
// loop runs the full dispatch chain - handler, broker, fan-out back to this
// same connection - and its stack reaches the depth a real connection reaches.
func benchWsExerciseConn(b *testing.B, conn *websocket.Conn, channel string, publishes int) {
	b.Helper()
	benchWsRoundTrip(b, conn, &protocol.Command{
		Id:        2,
		Subscribe: &protocol.SubscribeRequest{Channel: channel},
	})
	for i := 0; i < publishes; i++ {
		benchWsRoundTrip(b, conn, &protocol.Command{
			Id: uint32(3 + i),
			Publish: &protocol.PublishRequest{
				Channel: channel,
				Data:    []byte(`{"input":"hello world, this is a benchmark payload"}`),
			},
		})
	}
}

func benchWsConnFootprint(b *testing.B, p wsFootprintParams) {
	if p.commandsPerConn < 1 {
		b.Fatal("commandsPerConn must be >= 1, otherwise the read loop stack never grows")
	}

	n, err := New(Config{LogLevel: LogLevelError})
	if err != nil {
		b.Fatal(err)
	}
	n.OnConnecting(func(_ context.Context, _ ConnectEvent) (ConnectReply, error) {
		return ConnectReply{
			Credentials:    &Credentials{UserID: "bench"},
			WriteDelay:     p.writeDelay,
			WriteWithTimer: p.writeWithTimer,
		}, nil
	})
	n.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{}, nil)
		})
		client.OnPublish(func(e PublishEvent, cb PublishCallback) {
			cb(PublishReply{}, nil)
		})
	})
	if err = n.Run(); err != nil {
		b.Fatal(err)
	}
	defer func() { _ = n.Shutdown(context.Background()) }()

	// These clients answer replies and then go quiet, so a server ping firing
	// mid-run would disconnect connections already established and the footprint
	// would be measured over a shrinking population.
	ws := p.ws
	ws.PingPongConfig = PingPongConfig{PingInterval: time.Hour, PongTimeout: time.Minute}

	mux := http.NewServeMux()
	mux.Handle("/connection/websocket", NewWebsocketHandler(n, ws))
	server := httptest.NewServer(mux)
	defer server.Close()
	url := "ws" + server.URL[4:]

	// Warm up before taking the baseline. A node allocates several MB lazily on
	// first use - Prometheus label combinations, summary quantile streams, broker
	// maps - and that is fixed cost, not per-connection cost. Left inside the
	// measured window it lands in heap-B/conn divided by b.N, which at a few
	// hundred connections is larger than the per-connection heap itself and
	// swamps it with noise. Running a few connections through the full command
	// path first forces those allocations to happen before the baseline.
	for i := 0; i < 8; i++ {
		warm := newRealConnJSONConnect(b, url, false)
		benchWsExerciseConn(b, warm, fmt.Sprintf("warmup%d", i), p.commandsPerConn)
		_ = warm.Close()
	}

	// Connections torn down by warmup, or by an earlier benchmark in this
	// process, are still unwinding and would otherwise be charged to the
	// baseline.
	settle(b)

	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	goroutinesBefore := runtime.NumGoroutine()

	b.ResetTimer()
	conns := make([]*websocket.Conn, 0, b.N)
	for i := 0; i < b.N; i++ {
		conn := newRealConnJSONConnect(b, url, false)
		benchWsExerciseConn(b, conn, fmt.Sprintf("bench%d", i), p.commandsPerConn)
		conns = append(conns, conn)
	}
	b.StopTimer()

	// Every connection must still be registered. Without this the benchmark
	// divides by b.N while the server has quietly dropped a share of them,
	// reporting a footprint well below the real one.
	if got := n.hub.NumClients(); got != b.N {
		b.Fatalf("server holds %d connections, want %d: some were dropped during the run", got, b.N)
	}

	// Go shrinks an oversized goroutine stack by at most half per GC cycle, so a
	// single collection reports the peak the read loop reached rather than what
	// an idle connection settles at. Several cycles give the steady state, which
	// is what a mostly-idle fleet actually costs.
	for i := 0; i < 8; i++ {
		runtime.GC()
	}
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	goroutinesAfter := runtime.NumGoroutine()

	// MemStats counters are unsigned and the heap can legitimately end smaller
	// than it started, so subtract as signed - an unsigned wrap here renders as
	// a nonsense 3.6e16 rather than as an obvious error.
	stackDelta := int64(after.StackInuse) - int64(before.StackInuse)
	heapDelta := int64(after.HeapAlloc) - int64(before.HeapAlloc)
	goroutineDelta := goroutinesAfter - goroutinesBefore

	// Each connection must have left exactly its read loop behind. Anything else
	// means the baseline was taken while unrelated goroutines were still
	// unwinding, which silently skews every metric here - run one variant per
	// process if this trips.
	if perConn := float64(goroutineDelta) / float64(b.N); perConn < 0.95 || perConn > 1.05 {
		b.Fatalf("%.3f goroutines/conn, want ~1.0: measurement baseline is polluted", perConn)
	}

	b.ReportMetric(float64(stackDelta)/float64(b.N), "stack-B/conn")
	b.ReportMetric(float64(heapDelta)/float64(b.N), "heap-B/conn")
	b.ReportMetric(float64(goroutineDelta)/float64(b.N), "goroutines/conn")

	if path := os.Getenv("CENTRIFUGE_BENCH_HEAP_PROFILE"); path != "" {
		f, ferr := os.Create(path)
		if ferr != nil {
			b.Fatal(ferr)
		}
		if perr := pprof.Lookup("heap").WriteTo(f, 0); perr != nil {
			b.Fatal(perr)
		}
		_ = f.Close()
	}

	runtime.KeepAlive(conns)
	for _, c := range conns {
		_ = c.Close()
	}
}

// The A/B pair. Both use the timer-driven writer, so the writer goroutine is
// already gone and the only per-connection goroutine left is the read loop -
// exactly what ProcessCommandsOffReadLoop acts on.

func BenchmarkWsFootprint_Inline(b *testing.B) {
	benchWsConnFootprint(b, wsFootprintParams{
		ws: WebsocketConfig{ReadBufferSize: 512, UseWriteBufferPool: true},
		// Only needs to be non-zero to put the writer in timer mode, which is what
		// removes the writer goroutine. Kept small because every command round
		// trip waits for it and each connection makes five of them.
		writeDelay:      time.Millisecond,
		writeWithTimer:  true,
		commandsPerConn: 3,
	})
}

func BenchmarkWsFootprint_OffReadLoop(b *testing.B) {
	benchWsConnFootprint(b, wsFootprintParams{
		ws: WebsocketConfig{
			ReadBufferSize: 512, UseWriteBufferPool: true,
			ProcessCommandsOffReadLoop: true,
		},
		// Only needs to be non-zero to put the writer in timer mode, which is what
		// removes the writer goroutine. Kept small because every command round
		// trip waits for it and each connection makes five of them.
		writeDelay:      time.Millisecond,
		writeWithTimer:  true,
		commandsPerConn: 3,
	})
}

// The same pair with stock buffer settings and the goroutine writer, to confirm
// the stack effect is independent of the other tuning.

func BenchmarkWsFootprint_DefaultBuffers_Inline(b *testing.B) {
	benchWsConnFootprint(b, wsFootprintParams{commandsPerConn: 3})
}

func BenchmarkWsFootprint_DefaultBuffers_OffReadLoop(b *testing.B) {
	benchWsConnFootprint(b, wsFootprintParams{
		ws:              WebsocketConfig{ProcessCommandsOffReadLoop: true},
		commandsPerConn: 3,
	})
}

// --- The cost side ---------------------------------------------------------
//
// ProcessCommandsOffReadLoop buys stack by spending a goroutine spawn and two
// scheduler handoffs per frame. These measure that price on a connection that
// is doing nothing but sending commands - the worst case for the option, and
// the opposite of the fleet it is meant to help.

func benchWsCommandThroughput(b *testing.B, offReadLoop bool) {
	const channel = "bench"
	n, err := New(Config{LogLevel: LogLevelError})
	if err != nil {
		b.Fatal(err)
	}
	n.OnConnecting(func(_ context.Context, _ ConnectEvent) (ConnectReply, error) {
		return ConnectReply{Credentials: &Credentials{UserID: "bench"}}, nil
	})
	n.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) { cb(SubscribeReply{}, nil) })
		client.OnPublish(func(e PublishEvent, cb PublishCallback) { cb(PublishReply{}, nil) })
	})
	if err = n.Run(); err != nil {
		b.Fatal(err)
	}
	defer func() { _ = n.Shutdown(context.Background()) }()

	mux := http.NewServeMux()
	mux.Handle("/connection/websocket", NewWebsocketHandler(n, WebsocketConfig{
		PingPongConfig:             PingPongConfig{PingInterval: time.Hour, PongTimeout: time.Minute},
		ProcessCommandsOffReadLoop: offReadLoop,
	}))
	server := httptest.NewServer(mux)
	defer server.Close()

	conn := newRealConnJSONConnect(b, "ws"+server.URL[4:], false)
	defer func() { _ = conn.Close() }()
	benchWsRoundTrip(b, conn, &protocol.Command{
		Id: 2, Subscribe: &protocol.SubscribeRequest{Channel: channel},
	})

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		benchWsRoundTrip(b, conn, &protocol.Command{
			Id: uint32(3 + i),
			Publish: &protocol.PublishRequest{
				Channel: channel,
				Data:    []byte(`{"input":"hello world, this is a benchmark payload"}`),
			},
		})
	}
	b.StopTimer()

}

func BenchmarkWsCommandThroughput_Inline(b *testing.B) {
	benchWsCommandThroughput(b, false)
}

func BenchmarkWsCommandThroughput_OffReadLoop(b *testing.B) {
	benchWsCommandThroughput(b, true)
}

// --- Where does the read goroutine's stack high-water mark come from? -------
//
// The read goroutine settles at 4KB rather than Go's 2KB minimum, and Go only
// doubles a stack when a call chain does not fit. Whether that doubling happens
// during connection setup, or later while actually parsing a frame, decides
// whether restructuring setup out of the read loop's frame could get it back to
// 2KB. These two probes separate the cases.
//
// StackProbe_UpgradedOnly holds connections that completed the WebSocket
// handshake and nothing else. Setup has run in full and the goroutine is parked
// in the read path, but no frame has ever been parsed.
//
// StackProbe_Exercised is the same connection after real commands.
//
// Equal numbers mean setup or the idle read path sets the mark, and moving work
// out of the loop cannot help. A lower number for UpgradedOnly means frame
// parsing is what grows the stack.

func benchWsStackProbe(b *testing.B, sendCommands bool) {
	n, err := New(Config{
		LogLevel: LogLevelError,
		// A connection that never authenticates is closed as stale by default,
		// which is exactly what the UpgradedOnly probe holds open on purpose.
		ClientStaleCloseDelay: time.Hour,
	})
	if err != nil {
		b.Fatal(err)
	}
	n.OnConnecting(func(_ context.Context, _ ConnectEvent) (ConnectReply, error) {
		return ConnectReply{
			Credentials:    &Credentials{UserID: "bench"},
			WriteDelay:     time.Millisecond,
			WriteWithTimer: true,
		}, nil
	})
	n.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) { cb(SubscribeReply{}, nil) })
		client.OnPublish(func(e PublishEvent, cb PublishCallback) { cb(PublishReply{}, nil) })
	})
	if err = n.Run(); err != nil {
		b.Fatal(err)
	}
	defer func() { _ = n.Shutdown(context.Background()) }()

	mux := http.NewServeMux()
	mux.Handle("/connection/websocket", NewWebsocketHandler(n, WebsocketConfig{
		PingPongConfig:             PingPongConfig{PingInterval: time.Hour, PongTimeout: time.Minute},
		ReadBufferSize:             512,
		UseWriteBufferPool:         true,
		ProcessCommandsOffReadLoop: true,
	}))
	server := httptest.NewServer(mux)
	defer server.Close()
	url := "ws" + server.URL[4:] + "/connection/websocket"

	dial := func() *websocket.Conn {
		d := &websocket.Dialer{}
		conn, resp, _, dErr := d.Dial(url, nil)
		if dErr != nil {
			b.Fatal(dErr)
		}
		_ = resp.Body.Close()
		return conn
	}

	for i := 0; i < 8; i++ {
		warm := newRealConnJSONConnect(b, "ws"+server.URL[4:], false)
		benchWsExerciseConn(b, warm, fmt.Sprintf("warmup%d", i), 3)
		_ = warm.Close()
	}
	settle(b)

	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	goroutinesBefore := runtime.NumGoroutine()

	b.ResetTimer()
	conns := make([]*websocket.Conn, 0, b.N)
	for i := 0; i < b.N; i++ {
		var conn *websocket.Conn
		if sendCommands {
			conn = newRealConnJSONConnect(b, "ws"+server.URL[4:], false)
			benchWsExerciseConn(b, conn, fmt.Sprintf("bench%d", i), 3)
		} else {
			conn = dial()
		}
		conns = append(conns, conn)
	}
	b.StopTimer()

	// The same guards the footprint benchmark carries. Stack per connection is a
	// division by b.N, so a run that quietly lost connections reports a smaller
	// stack and looks like a result. Only connections that have authenticated
	// appear in the hub, so the upgraded-only probe is held to the goroutine
	// count below instead.
	if sendCommands {
		if got := n.hub.NumClients(); got != b.N {
			b.Fatalf("server holds %d connections, want %d: some were dropped during the run", got, b.N)
		}
	}

	for i := 0; i < 8; i++ {
		runtime.GC()
	}
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	goroutineDelta := runtime.NumGoroutine() - goroutinesBefore

	if perConn := float64(goroutineDelta) / float64(b.N); perConn < 0.95 || perConn > 1.05 {
		b.Fatalf("%.3f goroutines/conn, want ~1.0: the measurement is not one read goroutine per connection", perConn)
	}

	b.ReportMetric(float64(int64(after.StackInuse)-int64(before.StackInuse))/float64(b.N), "stack-B/conn")
	b.ReportMetric(float64(goroutineDelta)/float64(b.N), "goroutines/conn")

	runtime.KeepAlive(conns)
	for _, c := range conns {
		_ = c.Close()
	}
}

func BenchmarkWsStackProbe_UpgradedOnly(b *testing.B) { benchWsStackProbe(b, false) }
func BenchmarkWsStackProbe_Exercised(b *testing.B)    { benchWsStackProbe(b, true) }

// --- Footprint while connections are actually busy --------------------------
//
// The footprint benchmarks above measure idle connections, which is the case
// ProcessCommandsOffReadLoop is designed for. This measures the opposite case,
// because the option is expected to lose there and the size of the loss decides
// whether it is safe to recommend for a busy deployment.
//
// Inline, a busy connection is one goroutine grown to its peak. Offloaded, it is
// the read goroutine parked at its own size plus a processing goroutine that
// grows from Go's 2KB minimum every single frame. While a frame is in flight
// both are live, so peak stack can be higher than doing nothing at all.
//
// Stack is sampled during the load rather than after it, since the whole point
// is what is resident while frames are in flight. ReadMemStats stops the world
// briefly, so it is sampled at a modest interval and both variants pay it
// equally.

func benchWsUnderLoad(b *testing.B, offReadLoop bool) {
	const numConns = 100
	const channel = "load"

	n, err := New(Config{LogLevel: LogLevelError})
	if err != nil {
		b.Fatal(err)
	}
	n.OnConnecting(func(_ context.Context, _ ConnectEvent) (ConnectReply, error) {
		return ConnectReply{Credentials: &Credentials{UserID: "bench"}}, nil
	})
	n.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) { cb(SubscribeReply{}, nil) })
		client.OnPublish(func(e PublishEvent, cb PublishCallback) { cb(PublishReply{}, nil) })
	})
	if err = n.Run(); err != nil {
		b.Fatal(err)
	}
	defer func() { _ = n.Shutdown(context.Background()) }()

	mux := http.NewServeMux()
	mux.Handle("/connection/websocket", NewWebsocketHandler(n, WebsocketConfig{
		PingPongConfig:             PingPongConfig{PingInterval: time.Hour, PongTimeout: time.Minute},
		ReadBufferSize:             512,
		UseWriteBufferPool:         true,
		ProcessCommandsOffReadLoop: offReadLoop,
	}))
	server := httptest.NewServer(mux)
	defer server.Close()
	url := "ws" + server.URL[4:]

	conns := make([]*websocket.Conn, 0, numConns)
	for i := 0; i < numConns; i++ {
		conn := newRealConnJSONConnect(b, url, false)
		benchWsRoundTrip(b, conn, &protocol.Command{
			Id: 2, Subscribe: &protocol.SubscribeRequest{Channel: channel},
		})
		conns = append(conns, conn)
	}
	defer func() {
		for _, c := range conns {
			_ = c.Close()
		}
	}()
	settle(b)

	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	// Every connection sends continuously for the whole window, so the load is
	// sustained rather than bursty and the samples describe a steady state.
	var wg sync.WaitGroup
	stop := make(chan struct{})
	payload := []byte(`{"input":"hello world, this is a benchmark payload"}`)
	for ci, conn := range conns {
		wg.Add(1)
		go func(ci int, conn *websocket.Conn) {
			defer wg.Done()
			id := 3
			for {
				select {
				case <-stop:
					return
				default:
				}
				cmd, _ := json.Marshal(&protocol.Command{
					Id:      uint32(id),
					Publish: &protocol.PublishRequest{Channel: channel, Data: payload},
				})
				if err := conn.WriteMessage(websocket.TextMessage, cmd); err != nil {
					return
				}
				// A publish into a channel this connection is subscribed to
				// produces a reply *and* a push. Reading only one of them leaves
				// the other queued, the server's write queue grows past its limit
				// and it disconnects the client - which looks like the benchmark
				// measuring something rather than breaking.
				if !readUntilReplyID(conn, uint32(id)) {
					return
				}
				id++
			}
		}(ci, conn)
	}

	b.ResetTimer()

	var peakStack, sumStack int64
	var peakGoroutines, samples int
	for i := 0; i < b.N; i++ {
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		s := int64(m.StackInuse) - int64(before.StackInuse)
		if s > peakStack {
			peakStack = s
		}
		sumStack += s
		if g := runtime.NumGoroutine(); g > peakGoroutines {
			peakGoroutines = g
		}
		samples++
		time.Sleep(25 * time.Millisecond)
	}

	close(stop)
	wg.Wait()
	b.StopTimer()

	if got := n.hub.NumClients(); got != numConns {
		b.Fatalf("server holds %d connections, want %d: load dropped connections", got, numConns)
	}

	b.ReportMetric(float64(peakStack)/numConns, "peak-stack-B/conn")
	b.ReportMetric(float64(sumStack)/float64(samples)/numConns, "mean-stack-B/conn")
	b.ReportMetric(float64(peakGoroutines)/numConns, "peak-goroutines/conn")
}

func BenchmarkWsUnderLoad_Inline(b *testing.B)      { benchWsUnderLoad(b, false) }
func BenchmarkWsUnderLoad_OffReadLoop(b *testing.B) { benchWsUnderLoad(b, true) }

// readUntilReplyID drains messages until the reply with the given id arrives,
// discarding pushes. Reports false if the connection failed.
func readUntilReplyID(conn *websocket.Conn, want uint32) bool {
	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			return false
		}
		for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
			if line == "" {
				continue
			}
			var reply struct {
				ID uint32 `json:"id"`
			}
			if json.Unmarshal([]byte(line), &reply) == nil && reply.ID == want {
				return true
			}
		}
	}
}
