package centrifuge

import (
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/centrifugal/centrifuge/internal/readerpool"

	"github.com/centrifugal/protocol"
)

// HTTPStreamConfig represents config for HTTPStreamHandler.
type HTTPStreamConfig struct {
	PingPongConfig
	// MaxRequestBodySize limits request body size.
	MaxRequestBodySize int
}

// HTTPStreamHandler handles WebSocket client connections. WebSocket protocol
// is a bidirectional connection between a client and a server for low-latency
// communication.
type HTTPStreamHandler struct {
	node   *Node
	config HTTPStreamConfig
}

// NewHTTPStreamHandler creates new HTTPStreamHandler.
func NewHTTPStreamHandler(node *Node, config HTTPStreamConfig) *HTTPStreamHandler {
	return &HTTPStreamHandler{
		node:   node,
		config: config,
	}
}

const defaultMaxHTTPStreamingBodySize = 64 * 1024

const streamingResponseWriteTimeout = time.Second

func (h *HTTPStreamHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.node.metrics.incTransportConnect(transportHTTPStream)

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
			h.node.Log(NewLogEntry(LogLevelInfo, "error reading http stream request body", map[string]any{"error": err.Error()}))
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

	transport := newHTTPStreamTransport(r, httpStreamTransportConfig{
		protocolType: protocolType,
		pingPong:     h.config.PingPongConfig,
	})

	c, closeFn, err := NewClient(r.Context(), h.node, transport)
	if err != nil {
		h.node.Log(NewLogEntry(LogLevelError, "error create client", map[string]any{"error": err.Error(), "transport": transportHTTPStream}))
		return
	}
	defer func() { _ = closeFn() }()
	defer close(transport.closedCh) // need to execute this after client closeFn.

	if h.node.LogEnabled(LogLevelDebug) {
		h.node.Log(NewLogEntry(LogLevelDebug, "client connection established", map[string]any{"transport": transport.Name(), "client": c.ID()}))
		defer func(started time.Time) {
			h.node.Log(NewLogEntry(LogLevelDebug, "client connection completed", map[string]any{"duration": time.Since(started), "transport": transport.Name(), "client": c.ID()}))
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

	_, ok := w.(http.Flusher)
	if !ok {
		return
	}

	rc := http.NewResponseController(w)

	reader := readerpool.GetBytesReader(requestData)
	_ = HandleReadFrame(c, reader)
	readerpool.PutBytesReader(reader)

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
			_ = rc.SetWriteDeadline(time.Now().Add(streamingResponseWriteTimeout))
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
			_ = rc.Flush()
			_ = rc.SetWriteDeadline(time.Time{})
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
	pingPong     PingPongConfig
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

// PingPongConfig ...
func (t *httpStreamTransport) PingPongConfig() PingPongConfig {
	return t.config.pingPong
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
