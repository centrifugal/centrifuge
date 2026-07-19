package centrifuge

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/centrifugal/centrifuge/internal/websocket"
	"github.com/stretchr/testify/require"
)

// TestWebsocketTransport_PingWriteDeadlineRace guards against a data race between
// the writer goroutine and the frame-ping timer goroutine on the websocket write
// deadline. writeData arms and clears conn.SetWriteDeadline (the gorilla
// writeDeadline field, not safe for concurrent use); the ping goroutine must not
// touch that same field. Run under -race.
func TestWebsocketTransport_PingWriteDeadlineRace(t *testing.T) {
	var upgrader websocket.Upgrader
	connCh := make(chan *websocket.Conn, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, _, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		connCh <- c
	}))
	defer srv.Close()

	dialer := &websocket.Dialer{}
	clientConn, _, _, err := dialer.Dial("ws"+strings.TrimPrefix(srv.URL, "http")+"/", nil)
	require.NoError(t, err)
	defer func() { _ = clientConn.Close() }()
	// Drain client reads so server-side control/data writes don't block.
	go func() {
		for {
			if _, _, err := clientConn.ReadMessage(); err != nil {
				return
			}
		}
	}()

	serverConn := <-connCh
	tr := newWebsocketTransport(serverConn, websocketTransportOptions{
		protoType:    ProtocolTypeJSON,
		writeTimeout: time.Second,
		protoMajor:   1,
		pingPong:     PingPongConfig{PingInterval: time.Hour, PongTimeout: time.Hour},
	}, make(chan struct{}), true)
	defer func() { _ = tr.Close(DisconnectConnectionClosed) }()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			_ = tr.Write([]byte(`{"n":1}`))
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			tr.ping()
		}
	}()
	wg.Wait()
}
