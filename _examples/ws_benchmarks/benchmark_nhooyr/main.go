package main

import (
	"bytes"
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
	"time"

	_ "net/http/pprof"

	"github.com/centrifugal/centrifuge"
	"github.com/centrifugal/protocol"
	"nhooyr.io/websocket"
)

func handleLog(e centrifuge.LogEntry) {
	log.Printf("%s: %v", e.Message, e.Fields)
}

func waitExitSignal(n *centrifuge.Node) {
	sigCh := make(chan os.Signal, 1)
	done := make(chan bool, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		_ = n.Shutdown(context.Background())
		done <- true
	}()
	<-done
}

type customWebsocketHandler struct {
	node *centrifuge.Node
}

func newWebsocketHandler(node *centrifuge.Node) http.Handler {
	return &customWebsocketHandler{node}
}

const websocketTransportName = "websocket"

type customWebsocketTransport struct {
	mu      sync.RWMutex
	closed  bool
	closeCh chan struct{}

	conn      *websocket.Conn
	protoType centrifuge.ProtocolType
}

func newWebsocketTransport(conn *websocket.Conn, protoType centrifuge.ProtocolType) *customWebsocketTransport {
	return &customWebsocketTransport{
		conn:      conn,
		protoType: protoType,
		closeCh:   make(chan struct{}),
	}
}

// Name ...
func (t *customWebsocketTransport) Name() string {
	return websocketTransportName
}

// Protocol ...
func (t *customWebsocketTransport) Protocol() centrifuge.ProtocolType {
	return t.protoType
}

func (t *customWebsocketTransport) ProtocolVersion() centrifuge.ProtocolVersion {
	return centrifuge.ProtocolVersion2
}

// Unidirectional returns whether transport is unidirectional.
func (t *customWebsocketTransport) Unidirectional() bool {
	return false
}

// Emulation ...
func (t *customWebsocketTransport) Emulation() bool {
	return false
}

// DisabledPushFlags ...
func (t *customWebsocketTransport) DisabledPushFlags() uint64 {
	return centrifuge.PushFlagDisconnect
}

// PingPongConfig ...
func (t *customWebsocketTransport) PingPongConfig() centrifuge.PingPongConfig {
	return centrifuge.PingPongConfig{
		PingInterval: 25 * time.Second,
		PongTimeout:  10 * time.Second,
	}
}

// Write ...
func (t *customWebsocketTransport) Write(message []byte) error {
	select {
	case <-t.closeCh:
		return nil
	default:
		var messageType = websocket.MessageText
		protoType := protocol.TypeJSON

		if t.Protocol() == centrifuge.ProtocolTypeProtobuf {
			messageType = websocket.MessageBinary
			protoType = protocol.TypeProtobuf
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		encoder := protocol.GetDataEncoder(protoType)
		defer protocol.PutDataEncoder(protoType, encoder)
		_ = encoder.Encode(message)
		return t.conn.Write(ctx, messageType, encoder.Finish())
	}
}

// WriteMany ...
func (t *customWebsocketTransport) WriteMany(messages ...[]byte) error {
	select {
	case <-t.closeCh:
		return nil
	default:
		var messageType = websocket.MessageText
		protoType := protocol.TypeJSON

		if t.Protocol() == centrifuge.ProtocolTypeProtobuf {
			messageType = websocket.MessageBinary
			protoType = protocol.TypeProtobuf
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		encoder := protocol.GetDataEncoder(protoType)
		defer protocol.PutDataEncoder(protoType, encoder)
		for i := range messages {
			_ = encoder.Encode(messages[i])
		}
		return t.conn.Write(ctx, messageType, encoder.Finish())
	}
}

// Close ...
func (t *customWebsocketTransport) Close(disconnect centrifuge.Disconnect) error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil
	}
	t.closed = true
	close(t.closeCh)
	t.mu.Unlock()

	if disconnect != centrifuge.DisconnectConnectionClosed {
		return t.conn.Close(websocket.StatusCode(disconnect.Code), disconnect.Reason)
	}
	return t.conn.Close(websocket.StatusNormalClosure, "")
}

func (s *customWebsocketHandler) ServeHTTP(rw http.ResponseWriter, r *http.Request) {

	conn, err := websocket.Accept(rw, r, &websocket.AcceptOptions{})
	if err != nil {
		s.node.Log(centrifuge.NewLogEntry(centrifuge.LogLevelDebug, "websocket upgrade error", map[string]any{"error": err.Error()}))
		return
	}

	var protoType = centrifuge.ProtocolTypeJSON
	if r.URL.Query().Get("format") == "protobuf" {
		protoType = centrifuge.ProtocolTypeProtobuf
	}

	transport := newWebsocketTransport(conn, protoType)

	select {
	case <-s.node.NotifyShutdown():
		_ = transport.Close(centrifuge.DisconnectShutdown)
		return
	default:
	}

	c, closeFn, err := centrifuge.NewClient(r.Context(), s.node, transport)
	if err != nil {
		s.node.Log(centrifuge.NewLogEntry(centrifuge.LogLevelError, "error creating client", map[string]any{"transport": websocketTransportName}))
		return
	}
	defer func() { _ = closeFn() }()
	s.node.Log(centrifuge.NewLogEntry(centrifuge.LogLevelDebug, "client connection established", map[string]any{"client": c.ID(), "transport": websocketTransportName}))
	defer func(started time.Time) {
		s.node.Log(centrifuge.NewLogEntry(centrifuge.LogLevelDebug, "client connection completed", map[string]any{"client": c.ID(), "transport": websocketTransportName, "duration": time.Since(started)}))
	}(time.Now())

	for {
		_, data, err := conn.Read(context.Background())
		if err != nil {
			return
		}
		ok := centrifuge.HandleReadFrame(c, bytes.NewReader(data))
		if !ok {
			return
		}
	}
}

func main() {
	log.Printf("NumCPU: %d", runtime.NumCPU())

	node, _ := centrifuge.New(centrifuge.Config{
		LogLevel:   centrifuge.LogLevelError,
		LogHandler: handleLog,
	})

	node.OnConnecting(func(ctx context.Context, e centrifuge.ConnectEvent) (centrifuge.ConnectReply, error) {
		return centrifuge.ConnectReply{
			Credentials: &centrifuge.Credentials{
				UserID: "bench",
			},
		}, nil
	})

	node.OnConnect(func(client *centrifuge.Client) {
		client.OnSubscribe(func(e centrifuge.SubscribeEvent, cb centrifuge.SubscribeCallback) {
			cb(centrifuge.SubscribeReply{}, nil)
		})

		client.OnPublish(func(e centrifuge.PublishEvent, cb centrifuge.PublishCallback) {
			cb(centrifuge.PublishReply{}, nil)
		})
	})

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}

	http.Handle("/connection/websocket", newWebsocketHandler(node))

	go func() {
		if err := http.ListenAndServe(":8000", nil); err != nil {
			log.Fatal(err)
		}
	}()

	waitExitSignal(node)
	log.Println("bye!")
}
