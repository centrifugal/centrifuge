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

	"github.com/centrifugal/protocol"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
)

func TestWebsocketHandler(t *testing.T) {
	n, _ := New(Config{})
	mux := http.NewServeMux()
	mux.Handle("/connection/websocket", NewWebsocketHandler(n, WebsocketConfig{}))
	server := httptest.NewServer(mux)
	defer server.Close()

	url := "ws" + server.URL[4:]

	conn, resp, err := websocket.DefaultDialer.Dial(url+"/connection/websocket", nil)
	assert.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusSwitchingProtocols, resp.StatusCode)
	assert.NotNil(t, conn)
	defer conn.Close()
}

func TestWebsocketHandlerCustomDisconnect(t *testing.T) {
	n, _ := New(Config{})
	mux := http.NewServeMux()
	mux.Handle("/connection/websocket", NewWebsocketHandler(n, WebsocketConfig{}))
	server := httptest.NewServer(mux)
	defer server.Close()

	url := "ws" + server.URL[4:]

	conn, resp, err := websocket.DefaultDialer.Dial(url+"/connection/websocket", nil)
	assert.NoError(t, err)
	defer resp.Body.Close()

	n.OnConnecting(func(ctx context.Context, event ConnectEvent) (ConnectReply, error) {
		return ConnectReply{}, DisconnectInvalidToken
	})

	connectRequest := &protocol.ConnectRequest{
		Token: "boom",
	}
	params, _ := json.Marshal(connectRequest)
	cmd := &protocol.Command{
		ID:     1,
		Method: protocol.MethodTypeConnect,
		Params: params,
	}
	cmdBytes, _ := json.Marshal(cmd)

	_ = conn.WriteMessage(websocket.TextMessage, cmdBytes)
	_, _, err = conn.ReadMessage()
	assert.Error(t, err)
	closeErr, ok := err.(*websocket.CloseError)
	assert.True(t, ok)
	assert.Equal(t, int(DisconnectInvalidToken.Code), closeErr.Code)
}

func newRealConnJSON(b testing.TB, channel string, url string) *websocket.Conn {
	conn, resp, err := websocket.DefaultDialer.Dial(url+"/connection/websocket", nil)
	assert.NoError(b, err)
	defer resp.Body.Close()

	connectRequest := &protocol.ConnectRequest{}
	params, _ := json.Marshal(connectRequest)
	cmd := &protocol.Command{
		ID:     1,
		Method: protocol.MethodTypeConnect,
		Params: params,
	}
	cmdBytes, _ := json.Marshal(cmd)

	_ = conn.WriteMessage(websocket.TextMessage, cmdBytes)
	_, _, err = conn.ReadMessage()
	assert.NoError(b, err)

	subscribeRequest := &protocol.SubscribeRequest{
		Channel: channel,
	}
	params, _ = json.Marshal(subscribeRequest)
	cmd = &protocol.Command{
		ID:     2,
		Method: protocol.MethodTypeSubscribe,
		Params: params,
	}
	cmdBytes, _ = json.Marshal(cmd)
	_ = conn.WriteMessage(websocket.TextMessage, cmdBytes)
	_, _, err = conn.ReadMessage()
	assert.NoError(b, err)
	return conn
}

func newRealConnProtobuf(b testing.TB, channel string, url string) *websocket.Conn {
	conn, resp, err := websocket.DefaultDialer.Dial(url+"/connection/websocket?format=protobuf", nil)
	assert.NoError(b, err)
	defer resp.Body.Close()

	connectRequest := &protocol.ConnectRequest{}
	params, _ := connectRequest.Marshal()
	cmd := &protocol.Command{
		ID:     1,
		Method: protocol.MethodTypeConnect,
		Params: params,
	}

	cmdBytes, _ := cmd.Marshal()

	var buf bytes.Buffer
	bs := make([]byte, 8)
	n := binary.PutUvarint(bs, uint64(len(cmdBytes)))
	buf.Write(bs[:n])
	buf.Write(cmdBytes)

	_ = conn.WriteMessage(websocket.BinaryMessage, buf.Bytes())
	_, _, err = conn.ReadMessage()
	assert.NoError(b, err)

	subscribeRequest := &protocol.SubscribeRequest{
		Channel: channel,
	}
	params, _ = subscribeRequest.Marshal()
	cmd = &protocol.Command{
		ID:     2,
		Method: protocol.MethodTypeSubscribe,
		Params: params,
	}
	cmdBytes, _ = cmd.Marshal()

	buf.Reset()
	bs = make([]byte, 8)
	n = binary.PutUvarint(bs, uint64(len(cmdBytes)))
	buf.Write(bs[:n])
	buf.Write(cmdBytes)

	_ = conn.WriteMessage(websocket.BinaryMessage, buf.Bytes())
	_, _, err = conn.ReadMessage()
	assert.NoError(b, err)
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

// TestWebsocketHandlerConcurrentConnections allows to catch errors related
// to invalid buffer pool usages.
func TestWebsocketHandlerConcurrentConnections(t *testing.T) {
	n := nodeWithMemoryEngine()

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
		conn := newRealConnJSON(t, "test"+strconv.Itoa(i), url)
		conns = append(conns, conn)
	}
	defer func() {
		for _, conn := range conns {
			conn.Close()
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
				assert.Fail(t, err.Error())
			}

			_, data, err := conns[i].ReadMessage()
			if err != nil {
				assert.Fail(t, err.Error())
			}

			var rep protocol.Reply
			err = json.Unmarshal(data, &rep)
			assert.NoError(t, err)

			var push protocol.Push
			err = json.Unmarshal(rep.Result, &push)
			assert.NoError(t, err)

			var pub protocol.Publication
			err = json.Unmarshal(push.Data, &pub)
			assert.NoError(t, err)

			if !strings.Contains(string(pub.Data), string(payload)) {
				assert.Fail(t, "ooops, where is our payload? %s %s", string(payload), string(pub.Data))
			}
		}(i)
	}

	wg.Wait()
}

// BenchmarkWebsocketHandler allows to benchmark full flow with one real
// Websocket connection subscribed to one channel. This is not very representative
// in terms of time for operation as network IO involved but useful to look at
// total allocs and difference between JSON and Protobuf cases using various buffer sizes.
func BenchmarkWebsocketHandler(b *testing.B) {
	n := nodeWithMemoryEngine()

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
		{"Protobuf", newRealConnProtobuf},
	}
	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			conn := bm.getConn(b, "test", url)
			defer conn.Close()
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
			b.ReportAllocs()
		})
	}
}
