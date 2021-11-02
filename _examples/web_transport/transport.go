package main

import (
	"bufio"
	"net/http"
	"sync"
	"time"

	"github.com/lucas-clemente/quic-go"

	"github.com/centrifugal/protocol"

	"github.com/lucas-clemente/quic-go/http3"

	"github.com/centrifugal/centrifuge"
)

type quicHandler struct {
	node *centrifuge.Node
}

func newQUICHandler(node *centrifuge.Node) http.Handler {
	return &quicHandler{node}
}

const transportName = "quic"

type customQUICTransport struct {
	mu        sync.RWMutex
	closed    bool
	closeCh   chan struct{}
	protoType centrifuge.ProtocolType
	stream    *quic.Stream
}

func newQUICTransport(protoType centrifuge.ProtocolType, stream *quic.Stream) *customQUICTransport {
	return &customQUICTransport{
		protoType: protoType,
		closeCh:   make(chan struct{}),
		stream:    stream,
	}
}

// Name implementation.
func (t *customQUICTransport) Name() string {
	return transportName
}

// Protocol implementation.
func (t *customQUICTransport) Protocol() centrifuge.ProtocolType {
	return t.protoType
}

// Unidirectional implementation.
func (t *customQUICTransport) Unidirectional() bool {
	return false
}

// DisabledPushFlags ...
func (t *customQUICTransport) DisabledPushFlags() uint64 {
	return centrifuge.PushFlagDisconnect
}

// Write ...
func (t *customQUICTransport) Write(message []byte) error {
	select {
	case <-t.closeCh:
		return nil
	default:
		protoType := protocol.TypeJSON
		encoder := protocol.GetDataEncoder(protoType)
		defer protocol.PutDataEncoder(protoType, encoder)

		err := encoder.Encode(message)
		if err != nil {
			return err
		}

		_, err = (*t.stream).Write(append(encoder.Finish(), '\n'))

		return err
	}
}

// WriteMany ...
func (t *customQUICTransport) WriteMany(messages ...[]byte) error {
	select {
	case <-t.closeCh:
		return nil
	default:
		protoType := protocol.TypeJSON
		encoder := protocol.GetDataEncoder(protoType)
		defer protocol.PutDataEncoder(protoType, encoder)

		for i := range messages {
			err := encoder.Encode(messages[i])
			if err != nil {
				return err
			}
		}

		_, err := (*t.stream).Write(append(encoder.Finish(), '\n'))

		return err
	}
}

// Close ...
func (t *customQUICTransport) Close(disconnect *centrifuge.Disconnect) error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil
	}
	t.closed = true
	close(t.closeCh)
	t.mu.Unlock()

	if disconnect != nil {
		return (*t.stream).Close()
	}

	return (*t.stream).Close()
}

func (s *quicHandler) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	var protoType = centrifuge.ProtocolTypeJSON
	ctx := req.Context()

	var wt http3.WebTransport
	if wtb, ok := req.Body.(http3.WebTransporter); ok {
		var err error
		wt, err = wtb.WebTransport()
		if err != nil {
			rw.WriteHeader(http.StatusInternalServerError)
			return
		}
	}

	rw.WriteHeader(http.StatusOK)

	stream, err := wt.AcceptStream(ctx)
	if err != nil {
		s.node.Log(centrifuge.NewLogEntry(centrifuge.LogLevelError, "error creating stream", map[string]interface{}{"error": err}))
		rw.WriteHeader(http.StatusInternalServerError)
		return
	}

	//log.Println("initiated by ", stream.StreamID().InitiatedBy())

	transport := newQUICTransport(protoType, &stream)
	c, closeFn, err := centrifuge.NewClient(req.Context(), s.node, transport)
	if err != nil {
		s.node.Log(centrifuge.NewLogEntry(centrifuge.LogLevelError, "error creating client", map[string]interface{}{"transport": transportName}))
		return
	}

	defer func() { _ = closeFn() }()
	s.node.Log(centrifuge.NewLogEntry(centrifuge.LogLevelDebug, "client connection established", map[string]interface{}{"client": c.ID(), "transport": transportName}))
	defer func(started time.Time) {
		s.node.Log(centrifuge.NewLogEntry(centrifuge.LogLevelDebug, "client connection completed", map[string]interface{}{"client": c.ID(), "transport": transportName, "duration": time.Since(started)}))
	}(time.Now())

	s.node.Log(centrifuge.NewLogEntry(centrifuge.LogLevelDebug, "Stream params",
		map[string]interface{}{
			"ID":  stream.StreamID(),
			"Num": stream.StreamID().StreamNum(),
		}))

	r := bufio.NewReader(stream)
	for {
		reply, err := r.ReadBytes('\n')
		if err != nil {
			return
		}

		s.node.Log(centrifuge.NewLogEntry(centrifuge.LogLevelDebug, "Stream message",
			map[string]interface{}{
				"message": string(reply),
			}))

		ok := c.Handle(reply)
		if !ok {
			return
		}
	}

}
