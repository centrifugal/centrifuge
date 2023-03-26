package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/centrifugal/centrifuge/internal/cancelctx"
	"github.com/gorilla/websocket"

	_ "net/http/pprof"

	"github.com/centrifugal/centrifuge"
)

var (
	port  = flag.Int("port", 8000, "Port to bind app to")
	redis = flag.Bool("redis", false, "Use Redis")
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

var exampleChannel = "unidirectional"

func main() {
	flag.Parse()

	node, _ := centrifuge.New(centrifuge.Config{
		LogLevel:   centrifuge.LogLevelDebug,
		LogHandler: handleLog,
	})

	if *redis {
		redisShardConfigs := []centrifuge.RedisShardConfig{
			{Address: "localhost:6379"},
		}
		var redisShards []*centrifuge.RedisShard
		for _, redisConf := range redisShardConfigs {
			redisShard, err := centrifuge.NewRedisShard(node, redisConf)
			if err != nil {
				log.Fatal(err)
			}
			redisShards = append(redisShards, redisShard)
		}
		// Using Redis Broker here to scale nodes.
		broker, err := centrifuge.NewRedisBroker(node, centrifuge.RedisBrokerConfig{
			Shards: redisShards,
		})
		if err != nil {
			log.Fatal(err)
		}
		node.SetBroker(broker)

		presenceManager, err := centrifuge.NewRedisPresenceManager(node, centrifuge.RedisPresenceManagerConfig{
			Shards: redisShards,
		})
		if err != nil {
			log.Fatal(err)
		}
		node.SetPresenceManager(presenceManager)
	}

	node.OnConnecting(func(ctx context.Context, e centrifuge.ConnectEvent) (centrifuge.ConnectReply, error) {
		return centrifuge.ConnectReply{
			Subscriptions: map[string]centrifuge.SubscribeOptions{
				exampleChannel: {},
			},
		}, nil
	})

	node.OnConnect(func(client *centrifuge.Client) {
		client.OnUnsubscribe(func(e centrifuge.UnsubscribeEvent) {
			log.Printf("user %s unsubscribed from %s", client.UserID(), e.Channel)
		})
		client.OnDisconnect(func(e centrifuge.DisconnectEvent) {
			log.Printf("user %s disconnected, disconnect: %s", client.UserID(), e.Disconnect)
		})
		transport := client.Transport()
		log.Printf("user %s connected via %s", client.UserID(), transport.Name())
	})

	// Publish to a channel periodically.
	go func() {
		for {
			currentTime := strconv.FormatInt(time.Now().Unix(), 10)
			_, err := node.Publish(exampleChannel, []byte(`{"server_time": "`+currentTime+`"}`))
			if err != nil {
				log.Println(err.Error())
			}
			time.Sleep(5 * time.Second)
		}
	}()

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}

	websocketHandler := NewWebsocketHandler(node, WebsocketConfig{
		ReadBufferSize:     1024,
		UseWriteBufferPool: true,
	})
	http.Handle("/connection/websocket", authMiddleware(websocketHandler))
	http.Handle("/subscribe", handleSubscribe(node))
	http.Handle("/unsubscribe", handleUnsubscribe(node))
	http.Handle("/", http.FileServer(http.Dir("./")))

	go func() {
		if err := http.ListenAndServe(":"+strconv.Itoa(*port), nil); err != nil {
			log.Fatal(err)
		}
	}()

	waitExitSignal(node)
	log.Println("bye!")
}

func handleSubscribe(node *centrifuge.Node) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		clientID := req.URL.Query().Get("client")
		if clientID == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		err := node.Subscribe("42", exampleChannel, centrifuge.WithSubscribeClient(clientID))
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}
}

func handleUnsubscribe(node *centrifuge.Node) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		clientID := req.URL.Query().Get("client")
		if clientID == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		err := node.Unsubscribe("42", exampleChannel, centrifuge.WithUnsubscribeClient(clientID))
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}
}

// websocketTransport is a wrapper struct over websocket connection to fit session
// interface so client will accept it.
type websocketTransport struct {
	mu        sync.RWMutex
	writeMu   sync.Mutex // sync general write with unidirectional ping write.
	conn      *websocket.Conn
	closed    bool
	closeCh   chan struct{}
	graceCh   chan struct{}
	opts      websocketTransportOptions
	pingTimer *time.Timer
}

type websocketTransportOptions struct {
	protoType          centrifuge.ProtocolType
	pingInterval       time.Duration
	writeTimeout       time.Duration
	compressionMinSize int
}

func newWebsocketTransport(conn *websocket.Conn, opts websocketTransportOptions, graceCh chan struct{}) *websocketTransport {
	transport := &websocketTransport{
		conn:    conn,
		closeCh: make(chan struct{}),
		graceCh: graceCh,
		opts:    opts,
	}
	if opts.pingInterval > 0 {
		transport.addPing()
	}
	return transport
}

func (t *websocketTransport) ping() {
	select {
	case <-t.closeCh:
		return
	default:
		deadline := time.Now().Add(t.opts.pingInterval / 2)
		err := t.conn.WriteControl(websocket.PingMessage, nil, deadline)
		if err != nil {
			_ = t.Close(centrifuge.DisconnectWriteError)
			return
		}
		t.addPing()
	}
}

func (t *websocketTransport) addPing() {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return
	}
	t.pingTimer = time.AfterFunc(t.opts.pingInterval, t.ping)
	t.mu.Unlock()
}

// Name returns name of transport.
func (t *websocketTransport) Name() string {
	return "websocket"
}

// Protocol returns transport protocol.
func (t *websocketTransport) Protocol() centrifuge.ProtocolType {
	return t.opts.protoType
}

// ProtocolVersion returns transport ProtocolVersion.
func (t *websocketTransport) ProtocolVersion() centrifuge.ProtocolVersion {
	return centrifuge.ProtocolVersion2
}

// Unidirectional returns whether transport is unidirectional.
func (t *websocketTransport) Unidirectional() bool {
	return true
}

// Emulation ...
func (t *websocketTransport) Emulation() bool {
	return false
}

// DisabledPushFlags ...
func (t *websocketTransport) DisabledPushFlags() uint64 {
	return 0
}

// PingPongConfig ...
func (t *websocketTransport) PingPongConfig() centrifuge.PingPongConfig {
	return centrifuge.PingPongConfig{
		PingInterval: DefaultWebsocketPingInterval,
		PongTimeout:  DefaultWebsocketPingInterval / 3,
	}
}

func (t *websocketTransport) writeData(data []byte) error {
	if t.opts.compressionMinSize > 0 {
		t.conn.EnableWriteCompression(len(data) > t.opts.compressionMinSize)
	}
	var messageType = websocket.TextMessage
	if t.Protocol() == centrifuge.ProtocolTypeProtobuf {
		messageType = websocket.BinaryMessage
	}

	t.writeMu.Lock()
	if t.opts.writeTimeout > 0 {
		_ = t.conn.SetWriteDeadline(time.Now().Add(t.opts.writeTimeout))
	}
	err := t.conn.WriteMessage(messageType, data)
	if err != nil {
		t.writeMu.Unlock()
		return err
	}
	if t.opts.writeTimeout > 0 {
		_ = t.conn.SetWriteDeadline(time.Time{})
	}
	t.writeMu.Unlock()

	return nil
}

func (t *websocketTransport) Write(message []byte) error {
	return t.WriteMany(message)
}

func (t *websocketTransport) WriteMany(messages ...[]byte) error {
	select {
	case <-t.closeCh:
		return nil
	default:
		for i := 0; i < len(messages); i++ {
			err := t.writeData(messages[i])
			if err != nil {
				return err
			}
		}
		return nil
	}
}

const closeFrameWait = 5 * time.Second

// Close closes transport.
func (t *websocketTransport) Close(_ centrifuge.Disconnect) error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil
	}
	t.closed = true
	if t.pingTimer != nil {
		t.pingTimer.Stop()
	}
	close(t.closeCh)
	t.mu.Unlock()
	return t.conn.Close()
}

// Defaults.
const (
	DefaultWebsocketPingInterval     = 25 * time.Second
	DefaultWebsocketWriteTimeout     = 1 * time.Second
	DefaultWebsocketMessageSizeLimit = 65536 // 64KB
)

// WebsocketConfig represents config for WebsocketHandler.
type WebsocketConfig struct {
	// CompressionLevel sets a level for websocket compression.
	// See possible value description at https://golang.org/pkg/compress/flate/#NewWriter
	CompressionLevel int

	// CompressionMinSize allows setting minimal limit in bytes for
	// message to use compression when writing it into client connection.
	// By default, it's 0 - i.e. all messages will be compressed when
	// WebsocketCompression enabled and compression negotiated with client.
	CompressionMinSize int

	// ReadBufferSize is a parameter that is used for raw websocket Upgrader.
	// If set to zero reasonable default value will be used.
	ReadBufferSize int

	// WriteBufferSize is a parameter that is used for raw websocket Upgrader.
	// If set to zero reasonable default value will be used.
	WriteBufferSize int

	// MessageSizeLimit sets the maximum size in bytes of allowed message from client.
	// By default DefaultWebsocketMaxMessageSize will be used.
	MessageSizeLimit int

	// CheckOrigin func to provide custom origin check logic.
	// nil means allow all origins.
	CheckOrigin func(r *http.Request) bool

	// PingInterval sets interval server will send ping messages to clients.
	// By default DefaultPingInterval will be used.
	PingInterval time.Duration

	// WriteTimeout is maximum time of write message operation.
	// Slow client will be disconnected.
	// By default DefaultWebsocketWriteTimeout will be used.
	WriteTimeout time.Duration

	// Compression allows to enable websocket permessage-deflate
	// compression support for raw websocket connections. It does
	// not guarantee that compression will be used - i.e. it only
	// says that server will try to negotiate it with client.
	Compression bool

	// UseWriteBufferPool enables using buffer pool for writes.
	UseWriteBufferPool bool
}

// WebsocketHandler handles WebSocket client connections. WebSocket protocol
// is a bidirectional connection between a client an a server for low-latency
// communication.
type WebsocketHandler struct {
	node    *centrifuge.Node
	upgrade *websocket.Upgrader
	config  WebsocketConfig
}

var writeBufferPool = &sync.Pool{}

// NewWebsocketHandler creates new WebsocketHandler.
func NewWebsocketHandler(n *centrifuge.Node, c WebsocketConfig) *WebsocketHandler {
	upgrade := &websocket.Upgrader{
		ReadBufferSize:    c.ReadBufferSize,
		EnableCompression: c.Compression,
	}
	if c.UseWriteBufferPool {
		upgrade.WriteBufferPool = writeBufferPool
	} else {
		upgrade.WriteBufferSize = c.WriteBufferSize
	}
	if c.CheckOrigin != nil {
		upgrade.CheckOrigin = c.CheckOrigin
	} else {
		upgrade.CheckOrigin = sameHostOriginCheck()
	}
	return &WebsocketHandler{
		node:    n,
		config:  c,
		upgrade: upgrade,
	}
}

type ConnectRequest struct {
	Token   string                       `json:"token,omitempty"`
	Data    json.RawMessage              `json:"data,omitempty"`
	Subs    map[string]*SubscribeRequest `json:"subs,omitempty"`
	Name    string                       `json:"name,omitempty"`
	Version string                       `json:"version,omitempty"`
}

type SubscribeRequest struct {
	Recover bool   `json:"recover,omitempty"`
	Epoch   string `json:"epoch,omitempty"`
	Offset  uint64 `json:"offset,omitempty"`
}

func (s *WebsocketHandler) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	compression := s.config.Compression
	compressionLevel := s.config.CompressionLevel
	compressionMinSize := s.config.CompressionMinSize

	conn, err := s.upgrade.Upgrade(rw, r, nil)
	if err != nil {
		s.node.Log(centrifuge.NewLogEntry(centrifuge.LogLevelDebug, "websocket upgrade error", map[string]any{"error": err.Error()}))
		return
	}

	if compression {
		err := conn.SetCompressionLevel(compressionLevel)
		if err != nil {
			s.node.Log(centrifuge.NewLogEntry(centrifuge.LogLevelError, "websocket error setting compression level", map[string]any{"error": err.Error()}))
		}
	}

	pingInterval := s.config.PingInterval
	if pingInterval == 0 {
		pingInterval = DefaultWebsocketPingInterval
	}
	writeTimeout := s.config.WriteTimeout
	if writeTimeout == 0 {
		writeTimeout = DefaultWebsocketWriteTimeout
	}
	messageSizeLimit := s.config.MessageSizeLimit
	if messageSizeLimit == 0 {
		messageSizeLimit = DefaultWebsocketMessageSizeLimit
	}

	if messageSizeLimit > 0 {
		conn.SetReadLimit(int64(messageSizeLimit))
	}
	if pingInterval > 0 {
		pongWait := pingInterval * 10 / 9
		_ = conn.SetReadDeadline(time.Now().Add(pongWait))
		conn.SetPongHandler(func(string) error {
			_ = conn.SetReadDeadline(time.Now().Add(pongWait))
			return nil
		})
	}

	// Separate goroutine for better GC of caller's data.
	go func() {
		opts := websocketTransportOptions{
			pingInterval:       pingInterval,
			writeTimeout:       writeTimeout,
			compressionMinSize: compressionMinSize,
			protoType:          centrifuge.ProtocolTypeJSON,
		}

		graceCh := make(chan struct{})
		transport := newWebsocketTransport(conn, opts, graceCh)

		select {
		case <-s.node.NotifyShutdown():
			_ = transport.Close(centrifuge.DisconnectShutdown)
			return
		default:
		}

		ctxCh := make(chan struct{})
		defer close(ctxCh)

		c, closeFn, err := centrifuge.NewClient(cancelctx.New(r.Context(), ctxCh), s.node, transport)
		if err != nil {
			s.node.Log(centrifuge.NewLogEntry(centrifuge.LogLevelError, "error creating client", map[string]any{"transport": transport.Name()}))
			return
		}
		defer func() { _ = closeFn() }()

		s.node.Log(centrifuge.NewLogEntry(centrifuge.LogLevelDebug, "client connection established", map[string]any{"client": c.ID(), "transport": transport.Name()}))
		defer func(started time.Time) {
			s.node.Log(centrifuge.NewLogEntry(centrifuge.LogLevelDebug, "client connection completed", map[string]any{"client": c.ID(), "transport": transport.Name(), "duration": time.Since(started)}))
		}(time.Now())

		_, data, err := conn.ReadMessage()
		if err != nil {
			return
		}

		var req ConnectRequest
		err = json.Unmarshal(data, &req)
		if err != nil {
			return
		}

		connectRequest := centrifuge.ConnectRequest{
			Token:   req.Token,
			Data:    req.Data,
			Name:    req.Name,
			Version: req.Version,
		}
		if req.Subs != nil {
			subs := make(map[string]centrifuge.SubscribeRequest)
			for k, v := range connectRequest.Subs {
				subs[k] = centrifuge.SubscribeRequest{
					Recover: v.Recover,
					Offset:  v.Offset,
					Epoch:   v.Epoch,
				}
			}
		}

		c.Connect(connectRequest)

		for {
			_, _, err := conn.ReadMessage()
			if err != nil {
				break
			}
		}

		// https://github.com/gorilla/websocket/issues/448
		conn.SetPingHandler(nil)
		conn.SetPongHandler(nil)
		conn.SetCloseHandler(nil)
		_ = conn.SetReadDeadline(time.Now().Add(closeFrameWait))
		for {
			if _, _, err := conn.NextReader(); err != nil {
				close(graceCh)
				break
			}
		}
	}()
}

func sameHostOriginCheck() func(r *http.Request) bool {
	return func(r *http.Request) bool {
		err := checkSameHost(r)
		if err != nil {
			return false
		}
		return true
	}
}

func checkSameHost(r *http.Request) error {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return nil
	}
	u, err := url.Parse(origin)
	if err != nil {
		return fmt.Errorf("failed to parse Origin header %q: %w", origin, err)
	}
	if strings.EqualFold(r.Host, u.Host) {
		return nil
	}
	return fmt.Errorf("request Origin %q is not authorized for Host %q", origin, r.Host)
}
