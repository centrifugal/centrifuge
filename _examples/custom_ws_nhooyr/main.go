package main

import (
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
	"nhooyr.io/websocket"
)

func handleLog(e centrifuge.LogEntry) {
	log.Printf("%s: %v", e.Message, e.Fields)
}

func authMiddleware(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		newCtx := centrifuge.SetCredentials(ctx, &centrifuge.Credentials{
			UserID: "42",
		})
		r = r.WithContext(newCtx)
		h.ServeHTTP(w, r)
	})
}

func waitExitSignal(n *centrifuge.Node) {
	sigs := make(chan os.Signal, 1)
	done := make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		n.Shutdown(context.Background())
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

	conn    *websocket.Conn
	enc     centrifuge.Encoding
	request *http.Request
}

func newWebsocketTransport(conn *websocket.Conn, enc centrifuge.Encoding, r *http.Request) *customWebsocketTransport {
	return &customWebsocketTransport{
		conn:    conn,
		enc:     enc,
		request: r,
		closeCh: make(chan struct{}),
	}
}

func (t *customWebsocketTransport) Name() string {
	return websocketTransportName
}

func (t *customWebsocketTransport) Encoding() centrifuge.Encoding {
	return t.enc
}

func (t *customWebsocketTransport) Meta() centrifuge.TransportMeta {
	return centrifuge.TransportMeta{
		Request: t.request,
	}
}

func (t *customWebsocketTransport) Write(data []byte) error {
	select {
	case <-t.closeCh:
		return nil
	default:
		var messageType = websocket.MessageText
		if t.Encoding() == centrifuge.EncodingProtobuf {
			messageType = websocket.MessageBinary
		}
		err := t.conn.Write(context.Background(), messageType, data)
		if err != nil {
			return err
		}
		return nil
	}
}

func (t *customWebsocketTransport) Close(disconnect *centrifuge.Disconnect) error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil
	}
	t.closed = true
	close(t.closeCh)
	t.mu.Unlock()

	if disconnect != nil {
		reason, err := json.Marshal(disconnect)
		if err != nil {
			return err
		}
		return t.conn.Close(websocket.StatusCode(disconnect.Code), string(reason))
	}
	return t.conn.Close(websocket.StatusNormalClosure, "")
}

func (s *customWebsocketHandler) ServeHTTP(rw http.ResponseWriter, r *http.Request) {

	conn, err := websocket.Accept(rw, r, websocket.AcceptOptions{})
	if err != nil {
		s.node.Log(centrifuge.NewLogEntry(centrifuge.LogLevelDebug, "websocket upgrade error", map[string]interface{}{"error": err.Error()}))
		return
	}

	var enc = centrifuge.EncodingJSON
	if r.URL.Query().Get("format") == "protobuf" {
		enc = centrifuge.EncodingProtobuf
	}

	transport := newWebsocketTransport(conn, enc, r)

	select {
	case <-s.node.NotifyShutdown():
		transport.Close(centrifuge.DisconnectShutdown)
		return
	default:
	}

	c, err := centrifuge.NewClient(r.Context(), s.node, transport)
	if err != nil {
		s.node.Log(centrifuge.NewLogEntry(centrifuge.LogLevelError, "error creating client", map[string]interface{}{"transport": websocketTransportName}))
		return
	}
	s.node.Log(centrifuge.NewLogEntry(centrifuge.LogLevelDebug, "client connection established", map[string]interface{}{"client": c.ID(), "transport": websocketTransportName}))
	defer func(started time.Time) {
		s.node.Log(centrifuge.NewLogEntry(centrifuge.LogLevelDebug, "client connection completed", map[string]interface{}{"client": c.ID(), "transport": websocketTransportName, "duration": time.Since(started)}))
	}(time.Now())
	defer c.Close(nil)

	for {
		_, data, err := conn.Read(context.Background())
		if err != nil {
			return
		}
		ok := c.Handle(data)
		if !ok {
			return
		}
	}
}

func main() {
	cfg := centrifuge.DefaultConfig

	// Set secret to handle requests with JWT auth too. This is
	// not required if you don't use token authentication and
	// private subscriptions verified by token.
	cfg.Secret = "secret"
	cfg.Publish = true
	cfg.LogLevel = centrifuge.LogLevelDebug
	cfg.LogHandler = handleLog

	cfg.Namespaces = []centrifuge.ChannelNamespace{
		centrifuge.ChannelNamespace{
			Name: "chat",
			ChannelOptions: centrifuge.ChannelOptions{
				Publish:         true,
				Presence:        true,
				JoinLeave:       true,
				HistoryLifetime: 60,
				HistorySize:     1000,
				HistoryRecover:  true,
			},
		},
	}

	node, _ := centrifuge.New(cfg)

	node.On().ClientConnected(func(ctx context.Context, client *centrifuge.Client) {

		client.On().Subscribe(func(e centrifuge.SubscribeEvent) centrifuge.SubscribeReply {
			log.Printf("user %s subscribes on %s", client.UserID(), e.Channel)
			return centrifuge.SubscribeReply{}
		})

		client.On().Unsubscribe(func(e centrifuge.UnsubscribeEvent) centrifuge.UnsubscribeReply {
			log.Printf("user %s unsubscribed from %s", client.UserID(), e.Channel)
			return centrifuge.UnsubscribeReply{}
		})

		client.On().Publish(func(e centrifuge.PublishEvent) centrifuge.PublishReply {
			log.Printf("user %s publishes into channel %s: %s", client.UserID(), e.Channel, string(e.Data))
			return centrifuge.PublishReply{}
		})

		client.On().Message(func(e centrifuge.MessageEvent) centrifuge.MessageReply {
			log.Printf("Message from user: %s, data: %s", client.UserID(), string(e.Data))
			return centrifuge.MessageReply{}
		})

		client.On().Disconnect(func(e centrifuge.DisconnectEvent) centrifuge.DisconnectReply {
			log.Printf("user %s disconnected, disconnect: %#v", client.UserID(), e.Disconnect)
			return centrifuge.DisconnectReply{}
		})

		transport := client.Transport()
		log.Printf("user %s connected via %s with encoding: %s", client.UserID(), transport.Name(), transport.Encoding())

		// Connect handler should not block, so start separate goroutine to
		// periodically send messages to client.
		go func() {
			for {
				err := client.Send(centrifuge.Raw(`{"time": "` + strconv.FormatInt(time.Now().Unix(), 10) + `"}`))
				if err != nil {
					if err != io.EOF {
						log.Println(err.Error())
					} else {
						return
					}
				}
				time.Sleep(5 * time.Second)
			}
		}()
	})

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}

	http.Handle("/connection/websocket", authMiddleware(newWebsocketHandler(node)))
	http.Handle("/", http.FileServer(http.Dir("./")))

	go func() {
		if err := http.ListenAndServe(":8000", nil); err != nil {
			log.Fatal(err)
		}
	}()

	waitExitSignal(node)
	log.Println("bye!")
}
