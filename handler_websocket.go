package centrifuge

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/centrifugal/centrifuge/internal/cancelctx"
	"github.com/centrifugal/centrifuge/internal/timers"
	"github.com/centrifugal/centrifuge/internal/websocket"

	"github.com/centrifugal/protocol"
)

// WebsocketConfig represents config for WebsocketHandler.
type WebsocketConfig struct {
	// CheckOrigin func to provide custom origin check logic.
	// nil means that sameHostOriginCheck function will be used which
	// expects Origin host to match request Host.
	CheckOrigin func(r *http.Request) bool

	// ReadBufferSize is a parameter that is used for raw websocket Upgrader.
	// If set to zero reasonable default value will be used.
	ReadBufferSize int

	// WriteBufferSize is a parameter that is used for raw websocket Upgrader.
	// If set to zero reasonable default value will be used.
	WriteBufferSize int

	// UseWriteBufferPool enables using buffer pool for writes.
	UseWriteBufferPool bool

	// MessageSizeLimit sets the maximum size in bytes of allowed message from client.
	// By default, 65536 bytes (64KB) will be used.
	MessageSizeLimit int

	// WriteTimeout is maximum time of write message operation.
	// Slow client will be disconnected.
	// By default, 1 * time.Second will be used.
	WriteTimeout time.Duration

	// Compression allows enabling websocket permessage-deflate
	// compression support for raw websocket connections. It does
	// not guarantee that compression will be used - i.e. it only
	// says that server will try to negotiate it with client.
	// Note: enabling compression may lead to performance degradation.
	Compression bool

	// CompressionLevel sets a level for websocket compression.
	// See possible value description at https://golang.org/pkg/compress/flate/#NewWriter
	CompressionLevel int

	// CompressionMinSize allows setting minimal limit in bytes for
	// message to use compression when writing it into client connection.
	// By default, it's 0 - i.e. all messages will be compressed when
	// WebsocketCompression enabled and compression negotiated with client.
	CompressionMinSize int

	PingPongConfig
}

// WebsocketHandler handles WebSocket client connections. WebSocket protocol
// is a bidirectional connection between a client and a server for low-latency
// communication.
type WebsocketHandler struct {
	node    *Node
	upgrade *websocket.Upgrader
	config  WebsocketConfig
}

var writeBufferPool = &sync.Pool{}

// NewWebsocketHandler creates new WebsocketHandler.
func NewWebsocketHandler(node *Node, config WebsocketConfig) *WebsocketHandler {
	upgrade := &websocket.Upgrader{
		ReadBufferSize:    config.ReadBufferSize,
		EnableCompression: config.Compression,
		Subprotocols:      []string{"centrifuge-json", "centrifuge-protobuf"},
	}
	if config.UseWriteBufferPool {
		upgrade.WriteBufferPool = writeBufferPool
	} else {
		upgrade.WriteBufferSize = config.WriteBufferSize
	}
	if config.CheckOrigin != nil {
		upgrade.CheckOrigin = config.CheckOrigin
	} else {
		upgrade.CheckOrigin = sameHostOriginCheck(node)
	}
	return &WebsocketHandler{
		node:    node,
		config:  config,
		upgrade: upgrade,
	}
}

func (s *WebsocketHandler) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	s.node.metrics.incTransportConnect(transportWebsocket)

	var protoType = ProtocolTypeJSON
	var useFramePingPong bool

	if r.URL.RawQuery != "" {
		query := r.URL.Query()
		if query.Get("format") == "protobuf" || query.Get("cf_protocol") == "protobuf" {
			protoType = ProtocolTypeProtobuf
		}
		if query.Get("cf_ws_frame_ping_pong") == "true" {
			// This is a way for tools like Postman, wscat and others to maintain
			// active connection to the Centrifuge-based server without the need to
			// respond to app-level pings. We rely on native websocket ping/pong
			// frames in this case.
			useFramePingPong = true
		}
	}

	compression := s.config.Compression
	compressionLevel := s.config.CompressionLevel
	compressionMinSize := s.config.CompressionMinSize

	conn, subProtocol, err := s.upgrade.Upgrade(rw, r, nil)
	if err != nil {
		s.node.logger.log(newLogEntry(LogLevelDebug, "websocket upgrade error", map[string]any{"error": err.Error()}))
		return
	}

	if compression {
		err := conn.SetCompressionLevel(compressionLevel)
		if err != nil {
			s.node.logger.log(newLogEntry(LogLevelError, "websocket error setting compression level", map[string]any{"error": err.Error()}))
		}
	}

	writeTimeout := s.config.WriteTimeout
	if writeTimeout == 0 {
		writeTimeout = 1 * time.Second
	}
	messageSizeLimit := s.config.MessageSizeLimit
	if messageSizeLimit == 0 {
		messageSizeLimit = 65536 // 64KB
	}
	if messageSizeLimit > 0 {
		conn.SetReadLimit(int64(messageSizeLimit))
	}

	if subProtocol == "centrifuge-protobuf" {
		protoType = ProtocolTypeProtobuf
	}

	if useFramePingPong {
		pongWait := framePingInterval * 10 / 9
		_ = conn.SetReadDeadline(time.Now().Add(pongWait))
		conn.SetPongHandler(func(string) error {
			_ = conn.SetReadDeadline(time.Now().Add(pongWait))
			return nil
		})
	}

	// Separate goroutine for better GC of caller's data.
	go func() {
		opts := websocketTransportOptions{
			pingPong:           s.config.PingPongConfig,
			writeTimeout:       writeTimeout,
			compressionMinSize: compressionMinSize,
			protoType:          protoType,
		}

		graceCh := make(chan struct{})
		transport := newWebsocketTransport(conn, opts, graceCh, useFramePingPong)

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
			s.node.logger.log(newLogEntry(LogLevelError, "error creating client", map[string]any{"transport": transportWebsocket}))
			return
		}
		defer func() { _ = closeFn() }()

		if s.node.LogEnabled(LogLevelDebug) {
			s.node.logger.log(newLogEntry(LogLevelDebug, "client connection established", map[string]any{"client": c.ID(), "transport": transportWebsocket}))
			defer func(started time.Time) {
				s.node.logger.log(newLogEntry(LogLevelDebug, "client connection completed", map[string]any{"client": c.ID(), "transport": transportWebsocket, "duration": time.Since(started)}))
			}(time.Now())
		}

		for {
			_, r, err := conn.NextReader()
			if err != nil {
				break
			}
			proceed := HandleReadFrame(c, r)
			if !proceed {
				break
			}
		}

		if useFramePingPong {
			conn.SetPingHandler(nil)
			conn.SetPongHandler(nil)
		}

		_ = conn.SetReadDeadline(time.Now().Add(closeFrameWait))
		for {
			if _, _, err := conn.NextReader(); err != nil {
				close(graceCh)
				break
			}
		}
	}()
}

// HandleReadFrame is a helper to read Centrifuge commands from frame-based io.Reader and
// process them. Frame-based means that EOF treated as the end of the frame, not the entire
// connection close.
func HandleReadFrame(c *Client, r io.Reader) bool {
	protoType := c.Transport().Protocol().toProto()
	decoder := protocol.GetStreamCommandDecoder(protoType, r)
	defer protocol.PutStreamCommandDecoder(protoType, decoder)

	hadCommands := false

	for {
		cmd, cmdProtocolSize, err := decoder.Decode()
		if cmd != nil {
			hadCommands = true
			proceed := c.HandleCommand(cmd, cmdProtocolSize)
			if !proceed {
				return false
			}
		}
		if err != nil {
			if err == io.EOF {
				if !hadCommands {
					c.node.logger.log(newLogEntry(LogLevelInfo, "empty request received", map[string]any{"client": c.ID(), "user": c.UserID()}))
					c.Disconnect(DisconnectBadRequest)
					return false
				}
				break
			} else {
				c.node.logger.log(newLogEntry(LogLevelInfo, "error reading command", map[string]any{"client": c.ID(), "user": c.UserID(), "error": err.Error()}))
				c.Disconnect(DisconnectBadRequest)
				return false
			}
		}
	}
	return true
}

const (
	transportWebsocket = "websocket"
)

// websocketTransport is a wrapper struct over websocket connection to fit session
// interface so client will accept it.
type websocketTransport struct {
	mu              sync.RWMutex
	conn            *websocket.Conn
	closed          bool
	closeCh         chan struct{}
	graceCh         chan struct{}
	opts            websocketTransportOptions
	nativePingTimer *time.Timer
}

type websocketTransportOptions struct {
	protoType          ProtocolType
	pingPong           PingPongConfig
	writeTimeout       time.Duration
	compressionMinSize int
}

func newWebsocketTransport(conn *websocket.Conn, opts websocketTransportOptions, graceCh chan struct{}, useNativePingPong bool) *websocketTransport {
	transport := &websocketTransport{
		conn:    conn,
		closeCh: make(chan struct{}),
		graceCh: graceCh,
		opts:    opts,
	}
	if useNativePingPong {
		transport.addPing()
	}
	return transport
}

// Name returns name of transport.
func (t *websocketTransport) Name() string {
	return transportWebsocket
}

// Protocol returns transport protocol.
func (t *websocketTransport) Protocol() ProtocolType {
	return t.opts.protoType
}

// ProtocolVersion returns transport ProtocolVersion.
func (t *websocketTransport) ProtocolVersion() ProtocolVersion {
	return ProtocolVersion2
}

// Unidirectional returns whether transport is unidirectional.
func (t *websocketTransport) Unidirectional() bool {
	return false
}

// Emulation ...
func (t *websocketTransport) Emulation() bool {
	return false
}

// DisabledPushFlags ...
func (t *websocketTransport) DisabledPushFlags() uint64 {
	// Websocket sends disconnects in Close frames.
	return PushFlagDisconnect
}

// PingPongConfig ...
func (t *websocketTransport) PingPongConfig() PingPongConfig {
	t.mu.RLock()
	useNativePingPong := t.nativePingTimer != nil
	t.mu.RUnlock()
	if useNativePingPong {
		return PingPongConfig{
			PingInterval: -1,
			PongTimeout:  -1,
		}
	}
	return t.opts.pingPong
}

func (t *websocketTransport) writeData(data []byte) error {
	if t.opts.compressionMinSize > 0 {
		t.conn.EnableWriteCompression(len(data) > t.opts.compressionMinSize)
	}
	var messageType = websocket.TextMessage
	if t.Protocol() == ProtocolTypeProtobuf {
		messageType = websocket.BinaryMessage
	}
	if t.opts.writeTimeout > 0 {
		_ = t.conn.SetWriteDeadline(time.Now().Add(t.opts.writeTimeout))
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

// Write data to transport.
func (t *websocketTransport) Write(message []byte) error {
	select {
	case <-t.closeCh:
		return nil
	default:
		protoType := t.Protocol().toProto()
		if protoType == protocol.TypeJSON {
			// Fast path for one JSON message.
			return t.writeData(message)
		}
		encoder := protocol.GetDataEncoder(protoType)
		defer protocol.PutDataEncoder(protoType, encoder)
		_ = encoder.Encode(message)
		return t.writeData(encoder.Finish())
	}
}

// WriteMany data to transport.
func (t *websocketTransport) WriteMany(messages ...[]byte) error {
	select {
	case <-t.closeCh:
		return nil
	default:
		protoType := t.Protocol().toProto()
		encoder := protocol.GetDataEncoder(protoType)
		defer protocol.PutDataEncoder(protoType, encoder)
		for i := range messages {
			_ = encoder.Encode(messages[i])
		}
		return t.writeData(encoder.Finish())
	}
}

const closeFrameWait = 5 * time.Second

// Close closes transport.
func (t *websocketTransport) Close(disconnect Disconnect) error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil
	}
	t.closed = true
	close(t.closeCh)
	if t.nativePingTimer != nil {
		t.nativePingTimer.Stop()
	}
	t.mu.Unlock()

	if disconnect.Code != DisconnectConnectionClosed.Code {
		msg := websocket.FormatCloseMessage(int(disconnect.Code), disconnect.Reason)
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

var framePingInterval = 25 * time.Second

func (t *websocketTransport) ping() {
	select {
	case <-t.closeCh:
		return
	default:
		deadline := time.Now().Add(framePingInterval / 2)
		err := t.conn.WriteControl(websocket.PingMessage, nil, deadline)
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
	t.nativePingTimer = time.AfterFunc(framePingInterval, t.ping)
	t.mu.Unlock()
}

func sameHostOriginCheck(n *Node) func(r *http.Request) bool {
	return func(r *http.Request) bool {
		err := checkSameHost(r)
		if err != nil {
			n.logger.log(newLogEntry(LogLevelInfo, "origin check failure", map[string]any{"error": err.Error()}))
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
