package centrifuge

import (
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/centrifugal/protocol"
)

// HttpStreamingConfig represents config for HttpStreamingHandler.
type HttpStreamingConfig struct {
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

// HttpStreamingHandler handles WebSocket client connections. WebSocket protocol
// is a bidirectional connection between a client and a server for low-latency
// communication.
type HttpStreamingHandler struct {
	node   *Node
	config HttpStreamingConfig
}

// NewHttpStreamingHandler creates new HttpStreamingHandler.
func NewHttpStreamingHandler(node *Node, config HttpStreamingConfig) *HttpStreamingHandler {
	return &HttpStreamingHandler{
		node:   node,
		config: config,
	}
}

func (h *HttpStreamingHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	incTransportConnect(transportHttpStreaming)

	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}

	var cmd *protocol.Command
	if r.Method == http.MethodPost {
		maxBytesSize := int64(h.config.MaxRequestBodySize)
		if maxBytesSize == 0 {
			maxBytesSize = 64 * 1024
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxBytesSize)
		connectRequestData, err := io.ReadAll(r.Body)
		if err != nil {
			if len(connectRequestData) >= int(maxBytesSize) {
				w.WriteHeader(http.StatusRequestEntityTooLarge)
				return
			}
			h.node.Log(NewLogEntry(LogLevelError, "error reading body", map[string]interface{}{"error": err.Error()}))
			return
		}
		cmd, err = protocol.NewJSONCommandDecoder(connectRequestData).Decode()
		if err != nil && err != io.EOF {
			if h.node.LogEnabled(LogLevelDebug) {
				h.node.Log(NewLogEntry(LogLevelDebug, "malformed connect request", map[string]interface{}{"error": err.Error()}))
			}
			return
		}
	} else {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	transport := newStreamTransport(r)
	c, closeFn, err := NewClient(r.Context(), h.node, transport)
	if err != nil {
		h.node.Log(NewLogEntry(LogLevelError, "error create client", map[string]interface{}{"error": err.Error(), "transport": transportHttpStreaming}))
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

	// TODO: make sure it is ok to call handleCommand from the outside of the client.
	_ = c.handleCommand(cmd)

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
			_, err = w.Write(data)
			if err != nil {
				return
			}
			_, err = w.Write([]byte("\n"))
			if err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

const (
	transportHttpStreaming = "http_streaming"
)

type streamTransport struct {
	mu           sync.Mutex
	req          *http.Request
	messages     chan []byte
	disconnectCh chan *Disconnect
	closedCh     chan struct{}
	closed       bool
}

func newStreamTransport(req *http.Request) *streamTransport {
	return &streamTransport{
		messages:     make(chan []byte),
		disconnectCh: make(chan *Disconnect),
		closedCh:     make(chan struct{}),
		req:          req,
	}
}

func (t *streamTransport) Name() string {
	return transportHttpStreaming
}

func (t *streamTransport) Protocol() ProtocolType {
	return ProtocolTypeJSON
}

// ProtocolVersion returns transport protocol version.
func (t *streamTransport) ProtocolVersion() ProtocolVersion {
	return ProtocolVersion2
}

// Unidirectional returns whether transport is unidirectional.
func (t *streamTransport) Unidirectional() bool {
	return false
}

// Emulation ...
func (t *streamTransport) Emulation() bool {
	return true
}

// DisabledPushFlags ...
func (t *streamTransport) DisabledPushFlags() uint64 {
	return 0
}

// AppLevelPing ...
func (t *streamTransport) AppLevelPing() AppLevelPing {
	return AppLevelPing{
		PingInterval: 25 * time.Second,
		PongTimeout:  10 * time.Second,
	}
}

func (t *streamTransport) Write(message []byte) error {
	return t.WriteMany(message)
}

func (t *streamTransport) WriteMany(messages ...[]byte) error {
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

func (t *streamTransport) Close(_ *Disconnect) error {
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
