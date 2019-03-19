package centrifuge

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"net/http"
	"net/http/httptest"
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

func newRealConnJSON(b *testing.B, url string) *websocket.Conn {
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
		Channel: "test",
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

func newRealConnProtobuf(b *testing.B, url string) *websocket.Conn {
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
		Channel: "test",
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

// BenchmarkWebsocketHandler allows to benchmark full flow with one real
// Websocket connection subscribed to one channel. This is not very representative
// in terms of time for operation as network IO involved but useful to look at
// total allocs and difference between JSON and Protobuf cases.
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
		getConn func(b *testing.B, url string) *websocket.Conn
	}{
		{"JSON", newRealConnJSON},
		{"Protobuf", newRealConnProtobuf},
	}
	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			conn := bm.getConn(b, url)
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
