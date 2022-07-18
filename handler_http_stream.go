package centrifuge

import (
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/centrifugal/protocol"
)

// HTTPStreamConfig represents config for HTTPStreamHandler.
// EXPERIMENTAL, this is still a subject to change, do not use in production.
type HTTPStreamConfig struct {
	// MaxRequestBodySize limits request body size.
	MaxRequestBodySize int
	// AppLevelPingInterval tells how often to issue application-level server-to-client pings.
	// AppLevelPingInterval is only used for clients with ProtocolVersion2.
	// AppLevelPingInterval is EXPERIMENTAL and is a subject to change.
	// For zero value 25 secs will be used. To disable sending app-level pings in ProtocolVersion2 use -1.
	AppLevelPingInterval time.Duration
	// AppLevelPongTimeout sets time for application-level pong check after issuing
	// ping. AppLevelPongTimeout must be less than AppLevelPingInterval.
	// AppLevelPongTimeout is only used for clients with ProtocolVersion2.
	// AppLevelPongTimeout is EXPERIMENTAL and is a subject to change.
	// For zero value AppLevelPingInterval / 3 will be used. To disable pong checks use -1.
	AppLevelPongTimeout time.Duration
}

// HTTPStreamHandler handles WebSocket client connections. WebSocket protocol
// is a bidirectional connection between a client and a server for low-latency
// communication.
// EXPERIMENTAL, this is still a subject to change, do not use in production.
type HTTPStreamHandler struct {
	node   *Node
	config HTTPStreamConfig
}

// NewHTTPStreamHandler creates new HTTPStreamHandler.
// EXPERIMENTAL, this is still a subject to change, do not use in production.
func NewHTTPStreamHandler(node *Node, config HTTPStreamConfig) *HTTPStreamHandler {
	return &HTTPStreamHandler{
		node:   node,
		config: config,
	}
}

const defaultMaxHTTPStreamingBodySize = 64 * 1024

func (h *HTTPStreamHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	incTransportConnect(transportHTTPStream)

	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}

	protocolType := ProtocolTypeJSON
	if r.Header.Get("Content-Type") == "application/octet-stream" {
		protocolType = ProtocolTypeProtobuf
	}

	var requestData []byte
	if r.Method == http.MethodPost {
		maxBytesSize := h.config.MaxRequestBodySize
		if maxBytesSize == 0 {
			maxBytesSize = defaultMaxHTTPStreamingBodySize
		}
		r.Body = http.MaxBytesReader(w, r.Body, int64(maxBytesSize))
		var err error
		requestData, err = io.ReadAll(r.Body)
		if err != nil {
			h.node.Log(NewLogEntry(LogLevelError, "error reading body", map[string]interface{}{"error": err.Error()}))
			if len(requestData) >= maxBytesSize {
				w.WriteHeader(http.StatusRequestEntityTooLarge)
				return
			}
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	} else {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	var (
		pingInterval time.Duration
		pongTimeout  time.Duration
	)

	if h.config.AppLevelPingInterval >= 0 {
		pingInterval = h.config.AppLevelPingInterval
		if pingInterval == 0 {
			pingInterval = 25 * time.Second
		}
		pongTimeout = h.config.AppLevelPongTimeout
		if pongTimeout < 0 {
			pongTimeout = 0
		} else if pongTimeout == 0 {
			pongTimeout = pingInterval / 3
		}
	}

	transport := newHTTPStreamTransport(r, httpStreamTransportConfig{
		protocolType: protocolType,
		pingInterval: pingInterval,
		pongTimeout:  pongTimeout,
	})
	c, closeFn, err := NewClient(r.Context(), h.node, transport)
	if err != nil {
		h.node.Log(NewLogEntry(LogLevelError, "error create client", map[string]interface{}{"error": err.Error(), "transport": transportHTTPStream}))
		return
	}
	defer func() { _ = closeFn() }()
	defer close(transport.closedCh) // need to execute this after client closeFn.

	if h.node.LogEnabled(LogLevelDebug) {
		h.node.Log(NewLogEntry(LogLevelDebug, "client connection established", map[string]interface{}{"transport": transport.Name(), "client": c.ID()}))
		defer func(started time.Time) {
			h.node.Log(NewLogEntry(LogLevelDebug, "client connection completed", map[string]interface{}{"duration": time.Since(started), "transport": transport.Name(), "client": c.ID()}))
		}(time.Now())
	}

	if r.ProtoMajor == 1 {
		// An endpoint MUST NOT generate an HTTP/2 message containing connection-specific header fields.
		// Source: RFC7540.
		w.Header().Set("Connection", "keep-alive")
	}
	w.Header().Set("X-Accel-Buffering", "no")
	w.Header().Set("Cache-Control", "private, no-cache, no-store, must-revalidate, max-age=0")
	w.Header().Set("Pragma", "no-cache")
	w.Header().Set("Expire", "0")
	w.WriteHeader(http.StatusOK)

	flusher, ok := w.(http.Flusher)
	if !ok {
		return
	}

	_ = c.Handle(requestData)

	for {
		select {
		case <-r.Context().Done():
			return
		case <-transport.disconnectCh:
			return
		case data, ok := <-transport.messages:
			if !ok {
				return
			}
			if protocolType == ProtocolTypeProtobuf {
				protoType := protocolType.toProto()
				encoder := protocol.GetDataEncoder(protoType)
				_ = encoder.Encode(data)
				_, err := w.Write(encoder.Finish())
				if err != nil {
					return
				}
				protocol.PutDataEncoder(protoType, encoder)
			} else {
				_, err = w.Write(data)
				if err != nil {
					return
				}
				_, err = w.Write([]byte("\n"))
				if err != nil {
					return
				}
			}
			flusher.Flush()
		}
	}
}

const (
	transportHTTPStream = "http_stream"
)

type httpStreamTransport struct {
	mu           sync.Mutex
	req          *http.Request
	messages     chan []byte
	disconnectCh chan struct{}
	closedCh     chan struct{}
	closed       bool
	config       httpStreamTransportConfig
}

type httpStreamTransportConfig struct {
	protocolType ProtocolType
	pingInterval time.Duration
	pongTimeout  time.Duration
}

func newHTTPStreamTransport(req *http.Request, config httpStreamTransportConfig) *httpStreamTransport {
	return &httpStreamTransport{
		messages:     make(chan []byte),
		disconnectCh: make(chan struct{}),
		closedCh:     make(chan struct{}),
		req:          req,
		config:       config,
	}
}

func (t *httpStreamTransport) Name() string {
	return transportHTTPStream
}

func (t *httpStreamTransport) Protocol() ProtocolType {
	return t.config.protocolType
}

// ProtocolVersion returns transport protocol version.
func (t *httpStreamTransport) ProtocolVersion() ProtocolVersion {
	return ProtocolVersion2
}

// Unidirectional returns whether transport is unidirectional.
func (t *httpStreamTransport) Unidirectional() bool {
	return false
}

// Emulation ...
func (t *httpStreamTransport) Emulation() bool {
	return true
}

// DisabledPushFlags ...
func (t *httpStreamTransport) DisabledPushFlags() uint64 {
	return 0
}

// AppLevelPing ...
func (t *httpStreamTransport) AppLevelPing() AppLevelPing {
	return AppLevelPing{
		PingInterval: t.config.pingInterval,
		PongTimeout:  t.config.pongTimeout,
	}
}

func (t *httpStreamTransport) Write(message []byte) error {
	return t.WriteMany(message)
}

func (t *httpStreamTransport) WriteMany(messages ...[]byte) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return nil
	}
	for i := 0; i < len(messages); i++ {
		select {
		case t.messages <- messages[i]:
		case <-t.closedCh:
			return nil
		}
	}
	return nil
}

func (t *httpStreamTransport) Close(_ Disconnect) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return nil
	}
	t.closed = true
	close(t.disconnectCh)
	<-t.closedCh
	return nil
}
