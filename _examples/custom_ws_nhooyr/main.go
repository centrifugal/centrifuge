package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	_ "net/http/pprof"

	"github.com/centrifugal/centrifuge"
	"github.com/centrifugal/protocol"
	"nhooyr.io/websocket"
)

type clientMessage struct {
	Timestamp int64  `json:"timestamp"`
	Input     string `json:"input"`
}

func handleLog(e centrifuge.LogEntry) {
	log.Printf("%s: %v", e.Message, e.Fields)
}

func authMiddleware(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		newCtx := centrifuge.SetCredentials(ctx, &centrifuge.Credentials{
			UserID:   "42",
			ExpireAt: time.Now().Unix() + 60,
			Info:     []byte(`{"name": "Alexander"}`),
		})
		r = r.WithContext(newCtx)
		h.ServeHTTP(w, r)
	})
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

func main() {
	node, _ := centrifuge.New(centrifuge.Config{
		LogLevel:   centrifuge.LogLevelInfo,
		LogHandler: handleLog,
	})

	node.OnConnecting(func(ctx context.Context, e centrifuge.ConnectEvent) (centrifuge.ConnectReply, error) {
		return centrifuge.ConnectReply{
			Credentials: &centrifuge.Credentials{
				UserID: "",
			},
		}, nil
	})

	node.OnConnect(func(client *centrifuge.Client) {
		transport := client.Transport()
		log.Printf("user %s connected via %s with protocol: %s", client.UserID(), transport.Name(), transport.Protocol())

		// Connect handler should not block, so start separate goroutine to
		// periodically send messages to client.
		go func() {
			for {
				select {
				case <-client.Context().Done():
					return
				case <-time.After(5 * time.Second):
					err := client.Send([]byte(`{"time": "` + strconv.FormatInt(time.Now().Unix(), 10) + `"}`))
					if err != nil {
						if err == io.EOF {
							return
						}
						log.Println(err.Error())
					}
				}
			}
		}()

		client.OnRefresh(func(e centrifuge.RefreshEvent, cb centrifuge.RefreshCallback) {
			log.Printf("user %s connection is going to expire, refreshing", client.UserID())
			cb(centrifuge.RefreshReply{
				ExpireAt: time.Now().Unix() + 60,
			}, nil)
		})

		client.OnSubscribe(func(e centrifuge.SubscribeEvent, cb centrifuge.SubscribeCallback) {
			log.Printf("user %s subscribes on %s", client.UserID(), e.Channel)
			cb(centrifuge.SubscribeReply{
				Options: centrifuge.SubscribeOptions{
					ExpireAt: time.Now().Unix() + 60,
				},
			}, nil)
		})

		client.OnSubRefresh(func(e centrifuge.SubRefreshEvent, cb centrifuge.SubRefreshCallback) {
			log.Printf("user %s subscription on channel %s is going to expire, refreshing", client.UserID(), e.Channel)
			cb(centrifuge.SubRefreshReply{
				ExpireAt: time.Now().Unix() + 60,
			}, nil)
		})

		client.OnUnsubscribe(func(e centrifuge.UnsubscribeEvent) {
			log.Printf("user %s unsubscribed from %s", client.UserID(), e.Channel)
		})

		client.OnPublish(func(e centrifuge.PublishEvent, cb centrifuge.PublishCallback) {
			log.Printf("user %s publishes into channel %s: %s", client.UserID(), e.Channel, string(e.Data))
			var msg clientMessage
			err := json.Unmarshal(e.Data, &msg)
			if err != nil {
				cb(centrifuge.PublishReply{}, centrifuge.ErrorBadRequest)
				return
			}
			cb(centrifuge.PublishReply{}, nil)
		})

		client.OnRPC(func(e centrifuge.RPCEvent, cb centrifuge.RPCCallback) {
			log.Printf("[user %s] sent RPC, data: %s, method: %s", client.UserID(), string(e.Data), e.Method)
			switch e.Method {
			case "getCurrentYear":
				cb(centrifuge.RPCReply{Data: []byte(`{"year": "2020"}`)}, nil)
			default:
				cb(centrifuge.RPCReply{}, centrifuge.ErrorMethodNotFound)
			}
		})

		client.OnDisconnect(func(e centrifuge.DisconnectEvent) {
			log.Printf("user %s disconnected, disconnect: %s", client.UserID(), e.Disconnect)
		})
	})

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}

	websocketHandler := newWebsocketHandler(node)
	http.Handle("/connection/websocket", authMiddleware(websocketHandler))
	http.Handle("/", http.FileServer(http.Dir("./")))

	go func() {
		if err := http.ListenAndServe(":8000", nil); err != nil {
			log.Fatal(err)
		}
	}()

	waitExitSignal(node)
	log.Println("bye!")
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

// Name implementation.
func (t *customWebsocketTransport) Name() string {
	return websocketTransportName
}

// Protocol implementation.
func (t *customWebsocketTransport) Protocol() centrifuge.ProtocolType {
	return t.protoType
}

// ProtocolVersion ...
func (t *customWebsocketTransport) ProtocolVersion() centrifuge.ProtocolVersion {
	return centrifuge.ProtocolVersion2
}

// Unidirectional implementation.
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
