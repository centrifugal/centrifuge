package centrifuge

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/centrifugal/protocol"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
)

func sockjsData(data []byte) []byte {
	quoted, _ := json.Marshal(string(data))
	return []byte(fmt.Sprintf("[%s]", string(quoted)))
}

func TestSockjsHandler(t *testing.T) {
	n, _ := New(Config{})
	require.NoError(t, n.Run())
	defer func() { _ = n.Shutdown(context.Background()) }()
	mux := http.NewServeMux()

	n.OnConnecting(func(ctx context.Context, event ConnectEvent) (ConnectReply, error) {
		require.Equal(t, transportSockJS, event.Transport.Name())
		require.Equal(t, ProtocolTypeJSON, event.Transport.Protocol())
		return ConnectReply{
			Credentials: &Credentials{UserID: "user"},
			Data:        []byte(`{"SockJS connect response": 1}`),
		}, nil
	})

	doneCh := make(chan struct{})

	n.OnConnect(func(client *Client) {
		err := client.Send([]byte(`{"SockJS write": 1}`))
		require.NoError(t, err)
		client.Disconnect(DisconnectForceReconnect)
	})

	mux.Handle("/connection/sockjs/", NewSockjsHandler(n, SockjsConfig{
		HandlerPrefix: "/connection/sockjs",
	}))
	server := httptest.NewServer(mux)
	defer server.Close()

	url := "ws" + server.URL[4:]

	conn, resp, err := websocket.DefaultDialer.Dial(url+"/connection/sockjs/220/fi0988475/websocket", nil)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusSwitchingProtocols, resp.StatusCode)
	require.NotNil(t, conn)
	defer func() { _ = conn.Close() }()
	_, p, err := conn.ReadMessage()
	require.NoError(t, err)
	require.Equal(t, "o", string(p)) // open frame of SockJS protocol.

	connectRequest := &protocol.ConnectRequest{
		Token: "boom",
	}
	params, _ := json.Marshal(connectRequest)
	cmd := &protocol.Command{
		Id:     1,
		Method: protocol.Command_CONNECT,
		Params: params,
	}
	cmdBytes, _ := json.Marshal(cmd)
	err = conn.WriteMessage(websocket.TextMessage, sockjsData(cmdBytes))
	require.NoError(t, err)

	go func() {
		pos := 0
		contentExpected := []string{
			"SockJS connect response",
			"SockJS write",
			"force reconnect",
		}

	loop:
		for {
			_, p, err = conn.ReadMessage()
			if err != nil {
				break loop
			}

			for {
				if strings.Contains(string(p), contentExpected[pos]) {
					pos++
					if pos >= len(contentExpected) {
						close(doneCh)
						break loop
					}
				} else {
					break
				}
			}
		}
	}()

	select {
	case <-doneCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for closing done channel")
	}
}
