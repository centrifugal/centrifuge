package centrifuge

import (
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	transportWebsocket = "websocket"
)

// websocketTransport is a wrapper struct over websocket connection to fit session
// interface so client will accept it.
type websocketTransport struct {
	mu        sync.RWMutex
	conn      *websocket.Conn
	req       *http.Request
	closed    bool
	closeCh   chan struct{}
	opts      *websocketTransportOptions
	pingTimer *time.Timer
}

type websocketTransportOptions struct {
	encType            EncodingType
	protoType          ProtocolType
	pingInterval       time.Duration
	writeTimeout       time.Duration
	compressionMinSize int
}

func newWebsocketTransport(conn *websocket.Conn, req *http.Request, opts *websocketTransportOptions) *websocketTransport {
	transport := &websocketTransport{
		conn:    conn,
		req:     req,
		closeCh: make(chan struct{}),
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
		err := t.conn.WriteControl(websocket.PingMessage, []byte("ping"), deadline)
		if err != nil {
			t.Close(DisconnectServerError)
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

func (t *websocketTransport) Name() string {
	return transportWebsocket
}

func (t *websocketTransport) Protocol() ProtocolType {
	return t.opts.protoType
}

func (t *websocketTransport) Encoding() EncodingType {
	return t.opts.encType
}

func (t *websocketTransport) Meta() TransportMeta {
	return TransportMeta{
		Request: t.req,
	}
}

func (t *websocketTransport) Write(data []byte) error {
	select {
	case <-t.closeCh:
		return nil
	default:
		if t.opts.compressionMinSize > 0 {
			t.conn.EnableWriteCompression(len(data) > t.opts.compressionMinSize)
		}
		if t.opts.writeTimeout > 0 {
			_ = t.conn.SetWriteDeadline(time.Now().Add(t.opts.writeTimeout))
		}

		var messageType = websocket.TextMessage
		if t.Protocol() == ProtocolTypeProtobuf {
			messageType = websocket.BinaryMessage
		}

		err := t.conn.WriteMessage(messageType, data)
		if err != nil {
			return err
		}

		if t.opts.writeTimeout > 0 {
			_ = t.conn.SetWriteDeadline(time.Time{})
		}
		return nil
	}
}

func (t *websocketTransport) Close(disconnect *Disconnect) error {
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

	if disconnect != nil {
		deadline := time.Now().Add(time.Second)
		reason, err := json.Marshal(disconnect)
		if err != nil {
			return err
		}
		msg := websocket.FormatCloseMessage(disconnect.Code, string(reason))
		_ = t.conn.WriteControl(websocket.CloseMessage, msg, deadline)
		return t.conn.Close()
	}
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
	// Compression allows to enable websocket permessage-deflate
	// compression support for raw websocket connections. It does
	// not guarantee that compression will be used - i.e. it only
	// says that server will try to negotiate it with client.
	Compression bool

	// CompressionLevel sets a level for websocket compression.
	// See posiible value description at https://golang.org/pkg/compress/flate/#NewWriter
	CompressionLevel int

	// CompressionMinSize allows to set minimal limit in bytes for
	// message to use compression when writing it into client connection.
	// By default it's 0 - i.e. all messages will be compressed when
	// WebsocketCompression enabled and compression negotiated with client.
	CompressionMinSize int

	// ReadBufferSize is a parameter that is used for raw websocket Upgrader.
	// If set to zero reasonable default value will be used.
	ReadBufferSize int

	// WriteBufferSize is a parameter that is used for raw websocket Upgrader.
	// If set to zero reasonable default value will be used.
	WriteBufferSize int

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

	// MessageSizeLimit sets the maximum size in bytes of allowed message from client.
	// By default DefaultWebsocketMaxMessageSize will be used.
	MessageSizeLimit int
}

// WebsocketHandler handles websocket client connections.
type WebsocketHandler struct {
	node   *Node
	config WebsocketConfig
}

// NewWebsocketHandler creates new WebsocketHandler.
func NewWebsocketHandler(n *Node, c WebsocketConfig) *WebsocketHandler {
	return &WebsocketHandler{
		node:   n,
		config: c,
	}
}

func (s *WebsocketHandler) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	transportConnectCount.WithLabelValues(transportWebsocket).Inc()

	compression := s.config.Compression
	compressionLevel := s.config.CompressionLevel
	compressionMinSize := s.config.CompressionMinSize

	upgrader := websocket.Upgrader{
		ReadBufferSize:    s.config.ReadBufferSize,
		WriteBufferSize:   s.config.WriteBufferSize,
		EnableCompression: s.config.Compression,
	}
	if s.config.CheckOrigin != nil {
		upgrader.CheckOrigin = s.config.CheckOrigin
	} else {
		upgrader.CheckOrigin = func(r *http.Request) bool {
			// Allow all connections.
			return true
		}
	}

	conn, err := upgrader.Upgrade(rw, r, nil)
	if err != nil {
		s.node.logger.log(newLogEntry(LogLevelDebug, "websocket upgrade error", map[string]interface{}{"error": err.Error()}))
		return
	}

	if compression {
		err := conn.SetCompressionLevel(compressionLevel)
		if err != nil {
			s.node.logger.log(newLogEntry(LogLevelError, "websocket error setting compression level", map[string]interface{}{"error": err.Error()}))
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

	var protocol = ProtocolTypeJSON
	if r.URL.Query().Get("format") == "protobuf" || r.URL.Query().Get("protocol") == "protobuf" {
		protocol = ProtocolTypeProtobuf
	}

	var enc = EncodingTypeJSON
	if r.URL.Query().Get("encoding") == "binary" {
		enc = EncodingTypeBinary
	}

	// Separate goroutine for better GC of caller's data.
	go func() {
		opts := &websocketTransportOptions{
			pingInterval:       pingInterval,
			writeTimeout:       writeTimeout,
			compressionMinSize: compressionMinSize,
			encType:            enc,
			protoType:          protocol,
		}

		transport := newWebsocketTransport(conn, r, opts)

		select {
		case <-s.node.NotifyShutdown():
			transport.Close(DisconnectShutdown)
			return
		default:
		}

		ch := make(chan struct{})
		defer close(ch)

		c, err := NewClient(newCustomCancelContext(r.Context(), ch), s.node, transport)
		if err != nil {
			s.node.logger.log(newLogEntry(LogLevelError, "error creating client", map[string]interface{}{"transport": transportWebsocket}))
			return
		}
		s.node.logger.log(newLogEntry(LogLevelDebug, "client connection established", map[string]interface{}{"client": c.ID(), "transport": transportWebsocket}))
		defer func(started time.Time) {
			s.node.logger.log(newLogEntry(LogLevelDebug, "client connection completed", map[string]interface{}{"client": c.ID(), "transport": transportWebsocket, "duration": time.Since(started)}))
		}(time.Now())
		defer c.Close(nil)

		for {
			_, data, err := conn.ReadMessage()
			if err != nil {
				return
			}
			ok := c.Handle(data)
			if !ok {
				return
			}
		}
	}()
}
