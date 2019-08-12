package centrifuge

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/centrifugal/centrifuge/internal/proto"
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
	assert.Equal(t, http.StatusSwitchingProtocols, resp.StatusCode)
	assert.NotNil(t, conn)
	defer conn.Close()
}

func newRealConnJSON(b testing.TB, channel string, url string) *websocket.Conn {
	conn, _, err := websocket.DefaultDialer.Dial(url+"/connection/websocket", nil)
	assert.NoError(b, err)

	connectRequest := &proto.ConnectRequest{}
	params, _ := json.Marshal(connectRequest)
	cmd := &proto.Command{
		ID:     1,
		Method: proto.MethodTypeConnect,
		Params: params,
	}
	cmdBytes, _ := json.Marshal(cmd)

	conn.WriteMessage(websocket.TextMessage, cmdBytes)
	_, _, err = conn.ReadMessage()
	assert.NoError(b, err)

	subscribeRequest := &proto.SubscribeRequest{
		Channel: channel,
	}
	params, _ = json.Marshal(subscribeRequest)
	cmd = &proto.Command{
		ID:     2,
		Method: proto.MethodTypeSubscribe,
		Params: params,
	}
	cmdBytes, _ = json.Marshal(cmd)
	conn.WriteMessage(websocket.TextMessage, cmdBytes)
	_, _, err = conn.ReadMessage()
	assert.NoError(b, err)
	return conn
}

func newRealConnProtobuf(b testing.TB, channel string, url string) *websocket.Conn {
	conn, _, err := websocket.DefaultDialer.Dial(url+"/connection/websocket?format=protobuf", nil)
	assert.NoError(b, err)

	connectRequest := &proto.ConnectRequest{}
	params, _ := connectRequest.Marshal()
	cmd := &proto.Command{
		ID:     1,
		Method: proto.MethodTypeConnect,
		Params: params,
	}

	cmdBytes, _ := cmd.Marshal()

	var buf bytes.Buffer
	bs := make([]byte, 8)
	n := binary.PutUvarint(bs, uint64(len(cmdBytes)))
	buf.Write(bs[:n])
	buf.Write(cmdBytes)

	conn.WriteMessage(websocket.BinaryMessage, buf.Bytes())
	_, _, err = conn.ReadMessage()
	assert.NoError(b, err)

	subscribeRequest := &proto.SubscribeRequest{
		Channel: channel,
	}
	params, _ = subscribeRequest.Marshal()
	cmd = &proto.Command{
		ID:     2,
		Method: proto.MethodTypeSubscribe,
		Params: params,
	}
	cmdBytes, _ = cmd.Marshal()

	buf.Reset()
	bs = make([]byte, 8)
	n = binary.PutUvarint(bs, uint64(len(cmdBytes)))
	buf.Write(bs[:n])
	buf.Write(cmdBytes)

	conn.WriteMessage(websocket.BinaryMessage, buf.Bytes())
	_, _, err = conn.ReadMessage()
	assert.NoError(b, err)
	return conn
}

// TestWebsocketHandlerConcurrentConnections allows to catch errors related
// to invalid buffer pool usages.
func TestWebsocketHandlerConcurrentConnections(t *testing.T) {
	n := nodeWithMemoryEngine()
	c := n.Config()
	c.ClientInsecure = true
	n.Reload(c)

	mux := http.NewServeMux()
	mux.Handle("/connection/websocket", NewWebsocketHandler(n, WebsocketConfig{
		WriteBufferSize: 0,
		ReadBufferSize:  0,
	}))
	server := httptest.NewServer(mux)
	defer server.Close()

	url := "ws" + server.URL[4:]

	numConns := 100

	conns := []*websocket.Conn{}
	for i := 0; i < numConns; i++ {
		conn := newRealConnJSON(t, "test"+strconv.Itoa(i), url)
		defer conn.Close()
		conns = append(conns, conn)
	}

	var wg sync.WaitGroup

	for i := 0; i < numConns; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			payload := []byte(`{"input":"test` + strconv.Itoa(i) + `"}`)

			err := n.Publish("test"+strconv.Itoa(i), payload)
			if err != nil {
				assert.Fail(t, err.Error())
			}

			_, data, err := conns[i].ReadMessage()
			if err != nil {
				assert.Fail(t, err.Error())
			}

			var rep proto.Reply
			err = json.Unmarshal(data, &rep)
			assert.NoError(t, err)

			var push proto.Push
			err = json.Unmarshal(rep.Result, &push)
			assert.NoError(t, err)

			var pub Publication
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
	c := n.Config()
	c.ClientInsecure = true
	n.Reload(c)

	mux := http.NewServeMux()
	mux.Handle("/connection/websocket", NewWebsocketHandler(n, WebsocketConfig{
		WriteBufferSize: 0,
		ReadBufferSize:  0,
	}))
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
				err := n.Publish("test", payload)
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
