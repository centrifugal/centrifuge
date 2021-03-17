package centrifuge

import (
	"net/http"
	"sync"
	"time"

	"github.com/centrifugal/centrifuge/internal/cancelctx"
	"github.com/centrifugal/centrifuge/internal/timers"

	"github.com/centrifugal/protocol"
	"github.com/gorilla/websocket"
)

const (
	transportWebsocket = "websocket"
)

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
	encType            EncodingType
	protoType          ProtocolType
	pingInterval       time.Duration
	writeTimeout       time.Duration
	compressionMinSize int
	unidirectional     bool
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
		err := t.writeData([]byte(""))
		if err != nil {
			_ = t.Close(DisconnectWriteError)
			return
		}
		deadline := time.Now().Add(t.opts.pingInterval / 2)
		err = t.conn.WriteControl(websocket.PingMessage, nil, deadline)
		if err != nil {
			_ = t.Close(DisconnectWriteError)
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
	return transportWebsocket
}

// Protocol returns transport protocol.
func (t *websocketTransport) Protocol() ProtocolType {
	return t.opts.protoType
}

// Encoding returns transport encoding.
func (t *websocketTransport) Encoding() EncodingType {
	return t.opts.encType
}

// Unidirectional returns whether transport is unidirectional.
func (t *websocketTransport) Unidirectional() bool {
	return t.opts.unidirectional
}

// DisabledPushFlags ...
func (t *websocketTransport) DisabledPushFlags() uint64 {
	if !t.Unidirectional() {
		return PushFlagDisconnect
	}
	return 0
}

func (t *websocketTransport) writeData(data []byte) error {
	if t.opts.compressionMinSize > 0 {
		t.conn.EnableWriteCompression(len(data) > t.opts.compressionMinSize)
	}
	var messageType = websocket.TextMessage
	if t.Protocol() == ProtocolTypeProtobuf {
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

// Write data to transport.
func (t *websocketTransport) Write(messages ...[]byte) error {
	select {
	case <-t.closeCh:
		return nil
	default:
		if !t.opts.unidirectional {
			protoType := t.Protocol().toProto()
			encoder := protocol.GetDataEncoder(protoType)
			defer protocol.PutDataEncoder(protoType, encoder)
			for i := range messages {
				_ = encoder.Encode(messages[i])
			}
			return t.writeData(encoder.Finish())
		}
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
		msg := websocket.FormatCloseMessage(int(disconnect.Code), disconnect.CloseText())
		err := t.conn.WriteControl(websocket.CloseMessage, msg, time.Now().Add(time.Second))
		if err != nil {
			return t.conn.Close()
		}
		select {
		case <-t.graceCh:
		default:
			// Wait for closing handshake completion.
			tm := timers.AcquireTimer(closeFrameWait)
			select {
			case <-t.graceCh:
			case <-tm.C:
			}
			timers.ReleaseTimer(tm)
		}
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
	// CompressionLevel sets a level for websocket compression.
	// See possible value description at https://golang.org/pkg/compress/flate/#NewWriter
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

	// Unidirectional allows enabling unidirectional mode.
	Unidirectional bool
}

// WebsocketHandler handles websocket client connections.
type WebsocketHandler struct {
	node    *Node
	upgrade *websocket.Upgrader
	config  WebsocketConfig
}

var writeBufferPool = &sync.Pool{}

// NewWebsocketHandler creates new WebsocketHandler.
func NewWebsocketHandler(n *Node, c WebsocketConfig) *WebsocketHandler {
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
		upgrade.CheckOrigin = func(r *http.Request) bool {
			// Allow all connections.
			return true
		}
	}
	return &WebsocketHandler{
		node:    n,
		config:  c,
		upgrade: upgrade,
	}
}

func (s *WebsocketHandler) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	incTransportConnect(transportWebsocket)

	compression := s.config.Compression
	compressionLevel := s.config.CompressionLevel
	compressionMinSize := s.config.CompressionMinSize

	conn, err := s.upgrade.Upgrade(rw, r, nil)
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

	var protoType = ProtocolTypeJSON
	if r.URL.Query().Get("format") == "protobuf" || r.URL.Query().Get("protocol") == "protobuf" {
		protoType = ProtocolTypeProtobuf
	}

	var encType = EncodingTypeJSON
	if r.URL.Query().Get("encoding") == "binary" {
		encType = EncodingTypeBinary
	}

	// Separate goroutine for better GC of caller's data.
	go func() {
		opts := websocketTransportOptions{
			pingInterval:       pingInterval,
			writeTimeout:       writeTimeout,
			compressionMinSize: compressionMinSize,
			encType:            encType,
			protoType:          protoType,
			unidirectional:     s.config.Unidirectional,
		}

		graceCh := make(chan struct{})
		transport := newWebsocketTransport(conn, opts, graceCh)

		select {
		case <-s.node.NotifyShutdown():
			_ = transport.Close(DisconnectShutdown)
			return
		default:
		}

		ctxCh := make(chan struct{})
		defer close(ctxCh)

		c, closeFn, err := NewClient(cancelctx.New(r.Context(), ctxCh), s.node, transport)
		if err != nil {
			s.node.logger.log(newLogEntry(LogLevelError, "error creating client", map[string]interface{}{"transport": transportWebsocket}))
			return
		}
		defer func() { _ = closeFn() }()

		s.node.logger.log(newLogEntry(LogLevelDebug, "client connection established", map[string]interface{}{"client": c.ID(), "transport": transportWebsocket}))
		defer func(started time.Time) {
			s.node.logger.log(newLogEntry(LogLevelDebug, "client connection completed", map[string]interface{}{"client": c.ID(), "transport": transportWebsocket, "duration": time.Since(started)}))
		}(time.Now())

		if transport.Unidirectional() {
			_, data, err := conn.ReadMessage()
			if err != nil {
				return
			}
			decoder := protocol.GetParamsDecoder(transport.Protocol().toProto())
			req, err := decoder.DecodeConnect(data)
			if err != nil {
				return
			}
			err = c.unidirectionalConnect(req)
			if err != nil {
				return
			}
		}

		for {
			_, data, err := conn.ReadMessage()
			if err != nil {
				break
			}
			if !transport.Unidirectional() {
				closed := !c.Handle(data)
				if closed {
					break
				}
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
