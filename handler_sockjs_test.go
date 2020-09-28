package centrifuge

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
)

func TestSockjsHandler(t *testing.T) {
	n, _ := New(Config{})
	mux := http.NewServeMux()
	mux.Handle("/connection/sockjs/", NewSockjsHandler(n, SockjsConfig{
		HandlerPrefix: "/connection/sockjs",
	}))
	server := httptest.NewServer(mux)
	defer server.Close()

	url := "ws" + server.URL[4:]

	conn, resp, err := websocket.DefaultDialer.Dial(url+"/connection/sockjs/220/fi0pbfvm/websocket", nil)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusSwitchingProtocols, resp.StatusCode)
	require.NotNil(t, conn)
	defer conn.Close()
	_, p, err := conn.ReadMessage()
	require.NoError(t, err)
	require.Equal(t, "o", string(p)) // open frame of SockJS protocol.
}
