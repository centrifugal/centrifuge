package centrifuge

import (
	"net/http"
	"sync"
	"time"

	"github.com/centrifugal/centrifuge/internal/cancelctx"

	"github.com/centrifugal/protocol"
	"github.com/gorilla/websocket"
	"github.com/igm/sockjs-go/v3/sockjs"
)

// SockjsConfig represents config for SockJS handler.
type SockjsConfig struct {
	// ProtocolVersion this handler will use. If not set we are using ProtocolVersion1.
	// ProtocolVersion2 is more optimized for the message introspection but not supported
	// by all clients in the ecosystem yet.
	ProtocolVersion ProtocolVersion

	// HandlerPrefix sets prefix for SockJS handler endpoint path.
	HandlerPrefix string

	// URL is an address to SockJS client javascript library.
	URL string

	// HeartbeatDelay sets how often to send heartbeat frames to clients.
	HeartbeatDelay time.Duration

	// CheckOrigin allows to decide whether to use CORS or not in XHR case.
	// When false returned then CORS headers won't be set.
	CheckOrigin func(*http.Request) bool

	// WebsocketCheckOrigin allows to set custom CheckOrigin func for underlying
	// Gorilla Websocket based websocket.Upgrader.
	WebsocketCheckOrigin func(*http.Request) bool

	// WebsocketReadBufferSize is a parameter that is used for raw websocket websocket.Upgrader.
	// If set to zero reasonable default value will be used.
	WebsocketReadBufferSize int

	// WebsocketWriteBufferSize is a parameter that is used for raw websocket websocket.Upgrader.
	// If set to zero reasonable default value will be used.
	WebsocketWriteBufferSize int

	// WebsocketUseWriteBufferPool enables using buffer pool for writes in Websocket transport.
	WebsocketUseWriteBufferPool bool

	// WebsocketWriteTimeout is maximum time of write message operation.
	// Slow client will be disconnected.
	// By default DefaultWebsocketWriteTimeout will be used.
	WebsocketWriteTimeout time.Duration
}

// SockjsHandler accepts SockJS connections. SockJS has a bunch of fallback
// transports when WebSocket connection is not supported. It comes with additional
// costs though: small protocol framing overhead, lack of binary support, more
// goroutines per connection, and you need to use sticky session mechanism on
// your load balancer in case you are using HTTP-based SockJS fallbacks and have
// more than one Centrifuge Node on a backend (so SockJS to be able to emulate
// bidirectional protocol). So if you can afford it - use WebsocketHandler only.
type SockjsHandler struct {
	node            *Node
	config          SockjsConfig
	handler         http.Handler
	protocolVersion ProtocolVersion
}

// NewSockjsHandler creates new SockjsHandler.
func NewSockjsHandler(node *Node, config SockjsConfig) *SockjsHandler {
	options := sockjs.DefaultOptions
	wsUpgrader := &websocket.Upgrader{
		ReadBufferSize:  config.WebsocketReadBufferSize,
		WriteBufferSize: config.WebsocketWriteBufferSize,
		Error:           func(w http.ResponseWriter, r *http.Request, status int, reason error) {},
	}
	if config.WebsocketCheckOrigin != nil {
		wsUpgrader.CheckOrigin = config.WebsocketCheckOrigin
	} else {
		wsUpgrader.CheckOrigin = sameHostOriginCheck(node)
	}
	if config.WebsocketUseWriteBufferPool {
		wsUpgrader.WriteBufferPool = writeBufferPool
	} else {
		wsUpgrader.WriteBufferSize = config.WebsocketWriteBufferSize
	}
	options.WebsocketUpgrader = wsUpgrader
	// Override sockjs url. It's important to use the same SockJS
	// library version on client and server sides when using iframe
	// based SockJS transports, otherwise SockJS will raise error
	// about version mismatch.
	options.SockJSURL = config.URL
	if config.CheckOrigin != nil {
		options.CheckOrigin = config.CheckOrigin
	} else {
		options.CheckOrigin = sameHostOriginCheck(node)
	}

	options.HeartbeatDelay = config.HeartbeatDelay
	wsWriteTimeout := config.WebsocketWriteTimeout
	if wsWriteTimeout == 0 {
		wsWriteTimeout = DefaultWebsocketWriteTimeout
	}
	options.WebsocketWriteTimeout = wsWriteTimeout

	if config.ProtocolVersion == 0 {
		config.ProtocolVersion = ProtocolVersion1
	}

	s := &SockjsHandler{
		node:            node,
		config:          config,
		protocolVersion: config.ProtocolVersion,
	}

	handler := newSockJSHandler(s, config.HandlerPrefix, options)
	s.handler = handler
	return s
}

func (s *SockjsHandler) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	s.handler.ServeHTTP(rw, r)
}

// newSockJSHandler returns SockJS handler bind to sockjsPrefix url prefix.
// SockJS handler has several handlers inside responsible for various tasks
// according to SockJS protocol.
func newSockJSHandler(s *SockjsHandler, sockjsPrefix string, sockjsOpts sockjs.Options) http.Handler {
	return sockjs.NewHandler(sockjsPrefix, sockjsOpts, s.sockJSHandler)
}

// sockJSHandler called when new client connection comes to SockJS endpoint.
func (s *SockjsHandler) sockJSHandler(sess sockjs.Session) {
	incTransportConnect(transportSockJS)

	// Separate goroutine for better GC of caller's data.
	go func() {
		transport := newSockjsTransport(sess, s.protocolVersion)

		select {
		case <-s.node.NotifyShutdown():
			_ = transport.Close(DisconnectShutdown)
			return
		default:
		}

		ctxCh := make(chan struct{})
		defer close(ctxCh)
		c, closeFn, err := NewClient(cancelctx.New(sess.Request().Context(), ctxCh), s.node, transport)
		if err != nil {
			s.node.logger.log(newLogEntry(LogLevelError, "error creating client", map[string]interface{}{"transport": transportSockJS}))
			return
		}
		defer func() { _ = closeFn() }()
		s.node.logger.log(newLogEntry(LogLevelDebug, "client connection established", map[string]interface{}{"client": c.ID(), "transport": transportSockJS}))
		defer func(started time.Time) {
			s.node.logger.log(newLogEntry(LogLevelDebug, "client connection completed", map[string]interface{}{"client": c.ID(), "transport": transportSockJS, "duration": time.Since(started)}))
		}(time.Now())

		var needWaitLoop bool

		for {
			if msg, err := sess.Recv(); err == nil {
				if ok := c.Handle([]byte(msg)); !ok {
					needWaitLoop = true
					break
				}
				continue
			}
			break
		}

		if needWaitLoop {
			// One extra loop till we get an error from session,
			// this is required to wait until close frame will be sent
			// into connection inside Client implementation and transport
			// closed with proper disconnect reason.
			for {
				if _, err := sess.Recv(); err != nil {
					break
				}
			}
		}
	}()
}

const (
	transportSockJS = "sockjs"
)

type sockjsTransport struct {
	mu              sync.RWMutex
	closed          bool
	closeCh         chan struct{}
	session         sockjs.Session
	protocolVersion ProtocolVersion
}

func newSockjsTransport(s sockjs.Session, v ProtocolVersion) *sockjsTransport {
	t := &sockjsTransport{
		session:         s,
		closeCh:         make(chan struct{}),
		protocolVersion: v,
	}
	return t
}

// Name returns name of transport.
func (t *sockjsTransport) Name() string {
	return transportSockJS
}

// Protocol returns transport protocol.
func (t *sockjsTransport) Protocol() ProtocolType {
	return ProtocolTypeJSON
}

// ProtocolVersion returns transport ProtocolVersion.
func (t *sockjsTransport) ProtocolVersion() ProtocolVersion {
	return t.protocolVersion
}

// Unidirectional returns whether transport is unidirectional.
func (t *sockjsTransport) Unidirectional() bool {
	return false
}

// DisabledPushFlags ...
func (t *sockjsTransport) DisabledPushFlags() uint64 {
	if !t.Unidirectional() {
		return PushFlagDisconnect
	}
	return 0
}

// Write data to transport.
func (t *sockjsTransport) Write(message []byte) error {
	select {
	case <-t.closeCh:
		return nil
	default:
		// No need to use protocol encoders here since
		// SockJS only supports JSON.
		return t.session.Send(string(message))
	}
}

// Write data to transport.
func (t *sockjsTransport) WriteMany(messages ...[]byte) error {
	select {
	case <-t.closeCh:
		return nil
	default:
		encoder := protocol.GetDataEncoder(ProtocolTypeJSON.toProto())
		defer protocol.PutDataEncoder(ProtocolTypeJSON.toProto(), encoder)
		for i := range messages {
			_ = encoder.Encode(messages[i])
		}
		return t.session.Send(string(encoder.Finish()))
	}
}

// Close closes transport.
func (t *sockjsTransport) Close(disconnect *Disconnect) error {
	t.mu.Lock()
	if t.closed {
		// Already closed, noop.
		t.mu.Unlock()
		return nil
	}
	t.closed = true
	close(t.closeCh)
	t.mu.Unlock()

	if disconnect == nil {
		disconnect = DisconnectNormal
	}
	return t.session.Close(disconnect.Code, disconnect.CloseText())
}
