package centrifuge

import (
	"fmt"
	"io"
	"math/bits"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/centrifugal/centrifuge/internal/cancelctx"
	"github.com/centrifugal/centrifuge/internal/timers"

	"github.com/centrifugal/protocol"
	"github.com/gorilla/websocket"
)

// WebsocketConfig represents config for WebsocketHandler.
type WebsocketConfig struct {
	// ProtocolVersion the handler will serve. If not set we are expecting
	// client connected using ProtocolVersion2.
	ProtocolVersion ProtocolVersion

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
	// By default, 65536 bytes (64KB) will be used.
	MessageSizeLimit int

	// CheckOrigin func to provide custom origin check logic.
	// nil means that sameHostOriginCheck function will be used which
	// expects Origin host to match request Host.
	CheckOrigin func(r *http.Request) bool

	// WriteTimeout is maximum time of write message operation.
	// Slow client will be disconnected.
	// By default, 1 * time.Second will be used.
	WriteTimeout time.Duration

	// Compression allows enabling websocket permessage-deflate
	// compression support for raw websocket connections. It does
	// not guarantee that compression will be used - i.e. it only
	// says that server will try to negotiate it with client.
	Compression bool

	// UseWriteBufferPool enables using buffer pool for writes.
	UseWriteBufferPool bool

	// PingInterval sets interval server will send ping frames to clients.
	// By default, 25 * time.Second is used. Only used for clients with ProtocolVersion1.
	PingInterval time.Duration
	// PongTimeout sets the time to wait for pong messages from the client.
	// By default, PingInterval / 3 is used. Only used for clients with ProtocolVersion1.
	PongTimeout time.Duration

	PingPongConfig
}

// WebsocketHandler handles WebSocket client connections. WebSocket protocol
// is a bidirectional connection between a client and a server for low-latency
// communication.
type WebsocketHandler struct {
	node    *Node
	upgrade *websocket.Upgrader
	config  WebsocketConfig
	version ProtocolVersion
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
	if config.ProtocolVersion == 0 {
		config.ProtocolVersion = ProtocolVersion2
	}
	return &WebsocketHandler{
		node:    node,
		config:  config,
		upgrade: upgrade,
		version: config.ProtocolVersion,
	}
}

func (s *WebsocketHandler) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	incTransportConnect(transportWebsocket)

	var protoType = ProtocolTypeJSON
	protoVersion := s.config.ProtocolVersion

	if r.URL.RawQuery != "" {
		query := r.URL.Query()
		if query.Get("format") == "protobuf" || query.Get("cf_protocol") == "protobuf" {
			protoType = ProtocolTypeProtobuf
		}
		if queryProtocolVersion := query.Get("cf_protocol_version"); queryProtocolVersion != "" {
			switch queryProtocolVersion {
			case "v1":
				protoVersion = ProtocolVersion1
			case "v2":
				protoVersion = ProtocolVersion2
			default:
				s.node.logger.log(newLogEntry(LogLevelInfo, "unknown protocol version", map[string]interface{}{"transport": transportWebsocket, "version": queryProtocolVersion}))
				http.Error(rw, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
				return
			}
		}
	}

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

	var (
		pingInterval time.Duration
		pongTimeout  time.Duration
	)

	appLevelPing := protoVersion > ProtocolVersion1 && s.config.PingPongConfig.PingInterval >= 0

	if !appLevelPing {
		pingInterval = s.config.PingInterval
		if pingInterval == 0 {
			pingInterval = 25 * time.Second
		}
		pongTimeout = s.config.PongTimeout
		if pongTimeout == 0 {
			pongTimeout = pingInterval / 3
		}
	} else {
		pingInterval, pongTimeout = getPingPongPeriodValues(s.config.PingPongConfig)
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

	if !appLevelPing && pingInterval > 0 {
		readDeadline := pingInterval + pongTimeout
		_ = conn.SetReadDeadline(time.Now().Add(readDeadline))
		conn.SetPongHandler(func(string) error {
			_ = conn.SetReadDeadline(time.Now().Add(readDeadline))
			return nil
		})
	}

	subProtocol := conn.Subprotocol()
	if subProtocol == "centrifuge-protobuf" {
		protoType = ProtocolTypeProtobuf
	}

	// Separate goroutine for better GC of caller's data.
	go func() {
		opts := websocketTransportOptions{
			appLevelPing:       appLevelPing,
			pingInterval:       pingInterval,
			pongTimeout:        pongTimeout,
			writeTimeout:       writeTimeout,
			compressionMinSize: compressionMinSize,
			protoType:          protoType,
			protoVersion:       protoVersion,
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

		if s.node.LogEnabled(LogLevelDebug) {
			s.node.logger.log(newLogEntry(LogLevelDebug, "client connection established", map[string]interface{}{"client": c.ID(), "transport": transportWebsocket}))
			defer func(started time.Time) {
				s.node.logger.log(newLogEntry(LogLevelDebug, "client connection completed", map[string]interface{}{"client": c.ID(), "transport": transportWebsocket, "duration": time.Since(started)}))
			}(time.Now())
		}

		for {
			_, r, err := conn.NextReader()
			if err != nil {
				break
			}
			proceed := s.handleRead(c, protoType.toProto(), r)
			if !proceed {
				break
			}
		}

		// https://github.com/gorilla/websocket/issues/448
		// We only set custom pong handler in proto v1.
		if protoVersion == ProtocolVersion1 {
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

func (s *WebsocketHandler) handleRead(c *Client, protoType protocol.Type, r io.Reader) bool {
	decoder := protocol.GetStreamCommandDecoder(protoType, r)
	defer protocol.PutStreamCommandDecoder(protoType, decoder)

	for {
		cmd, err := decoder.Decode()
		if cmd != nil {
			proceed := c.HandleCommand(cmd)
			if !proceed {
				return false
			}
		}
		if err != nil {
			if err == io.EOF {
				break
			} else {
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
	mu        sync.RWMutex
	conn      *websocket.Conn
	closed    bool
	closeCh   chan struct{}
	graceCh   chan struct{}
	opts      websocketTransportOptions
	pingTimer *time.Timer
}

type websocketTransportOptions struct {
	protoType          ProtocolType
	pingInterval       time.Duration
	pongTimeout        time.Duration
	writeTimeout       time.Duration
	compressionMinSize int
	protoVersion       ProtocolVersion
	appLevelPing       bool
}

func newWebsocketTransport(conn *websocket.Conn, opts websocketTransportOptions, graceCh chan struct{}) *websocketTransport {
	transport := &websocketTransport{
		conn:    conn,
		closeCh: make(chan struct{}),
		graceCh: graceCh,
		opts:    opts,
	}
	if !opts.appLevelPing && opts.pingInterval > 0 {
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

// ProtocolVersion returns transport ProtocolVersion.
func (t *websocketTransport) ProtocolVersion() ProtocolVersion {
	return t.opts.protoVersion
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

// AppLevelPing ...
func (t *websocketTransport) AppLevelPing() AppLevelPing {
	return AppLevelPing{
		PingInterval: t.opts.pingInterval,
		PongTimeout:  t.opts.pongTimeout,
	}
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
	if t.pingTimer != nil {
		t.pingTimer.Stop()
	}
	close(t.closeCh)
	t.mu.Unlock()

	if disconnect.Code != DisconnectConnectionClosed.Code {
		msg := websocket.FormatCloseMessage(int(disconnect.Code), disconnect.CloseText(t.ProtocolVersion()))
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

func sameHostOriginCheck(n *Node) func(r *http.Request) bool {
	return func(r *http.Request) bool {
		err := checkSameHost(r)
		if err != nil {
			n.logger.log(newLogEntry(LogLevelInfo, "origin check failure", map[string]interface{}{"error": err.Error()}))
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

var (
	// Verify ByteBuffer implements io.Writer.
	_ io.Writer = &ByteBuffer{}
)

// ByteBuffer implements a simple byte buffer.
type ByteBuffer struct {
	// B is the underlying byte slice.
	B []byte
}

// Reset resets bb.
func (bb *ByteBuffer) Reset() {
	bb.B = bb.B[:0]
}

// Write appends p to bb.
func (bb *ByteBuffer) Write(p []byte) (int, error) {
	bb.B = append(bb.B, p...)
	return len(p), nil
}

// pools contain pools for byte slices of various capacities.
var pools [19]sync.Pool

// maxBufferLength is the maximum length of an element that can be added to the Pool.
const maxBufferLength = 262144 // 2^18

// Log of base two, round up (for v > 0).
func nextLogBase2(v uint32) uint32 {
	return uint32(bits.Len32(v - 1))
}

// Log of base two, round down (for v > 0)
func prevLogBase2(num uint32) uint32 {
	next := nextLogBase2(num)
	if num == (1 << next) {
		return next
	}
	return next - 1
}

// getByteBuffer returns byte buffer with the given capacity.
func getByteBuffer(length int) *ByteBuffer {
	if length == 0 {
		return &ByteBuffer{
			B: nil,
		}
	}
	if length > maxBufferLength {
		return &ByteBuffer{
			B: make([]byte, 0, length),
		}
	}
	idx := nextLogBase2(uint32(length))
	if v := pools[idx].Get(); v != nil {
		return v.(*ByteBuffer)
	}
	return &ByteBuffer{
		B: make([]byte, 0, 1<<idx),
	}
}

// putByteBuffer returns bb to the pool.
func putByteBuffer(bb *ByteBuffer) {
	capacity := cap(bb.B)
	if capacity == 0 || capacity > maxBufferLength {
		return // drop.
	}
	idx := prevLogBase2(uint32(capacity))
	bb.Reset()
	pools[idx].Put(bb)
}
