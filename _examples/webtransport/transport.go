package main

import (
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/centrifugal/centrifuge"
	"github.com/centrifugal/protocol"
	"github.com/lucas-clemente/quic-go"
	"github.com/lucas-clemente/quic-go/http3"
)

type WebtransportConfig struct{}

type WebtransportHandler struct {
	node   *centrifuge.Node
	config WebtransportConfig
}

func NewWebtransportHandler(node *centrifuge.Node, c WebtransportConfig) *WebtransportHandler {
	return &WebtransportHandler{node: node, config: c}
}

const transportName = "webtransport"

var format string

type webtransportTransport struct {
	mu           sync.RWMutex
	closed       bool
	closeCh      chan struct{}
	protoType    centrifuge.ProtocolType
	stream       quic.Stream
	webTransport http3.WebTransport
}

func newWebtransportTransport(protoType centrifuge.ProtocolType, webTransport http3.WebTransport, stream quic.Stream) *webtransportTransport {
	return &webtransportTransport{
		protoType:    protoType,
		closeCh:      make(chan struct{}),
		stream:       stream,
		webTransport: webTransport,
	}
}

// Name implementation.
func (t *webtransportTransport) Name() string {
	return transportName
}

// Protocol implementation.
func (t *webtransportTransport) Protocol() centrifuge.ProtocolType {
	return t.protoType
}

// Unidirectional implementation.
func (t *webtransportTransport) Unidirectional() bool {
	return false
}

// DisabledPushFlags ...
func (t *webtransportTransport) DisabledPushFlags() uint64 {
	return centrifuge.PushFlagDisconnect
}

// Write ...
func (t *webtransportTransport) Write(message []byte) error {
	select {
	case <-t.closeCh:
		return nil
	default:
		protoType := protocol.TypeJSON
		if t.protoType == centrifuge.ProtocolTypeProtobuf {
			protoType = protocol.TypeProtobuf
		}
		encoder := protocol.GetDataEncoder(protoType)
		defer protocol.PutDataEncoder(protoType, encoder)
		_ = encoder.Encode(message)
		_, err := t.stream.Write(encoder.Finish())
		return err
	}
}

// WriteMany ...
func (t *webtransportTransport) WriteMany(messages ...[]byte) error {
	select {
	case <-t.closeCh:
		return nil
	default:
		protoType := protocol.TypeJSON
		if t.protoType == centrifuge.ProtocolTypeProtobuf {
			protoType = protocol.TypeProtobuf
		}
		encoder := protocol.GetDataEncoder(protoType)
		defer protocol.PutDataEncoder(protoType, encoder)

		for i := range messages {
			err := encoder.Encode(messages[i])
			if err != nil {
				return err
			}
		}
		_, err := t.stream.Write(encoder.Finish())
		return err
	}
}

// Close ...
func (t *webtransportTransport) Close(disconnect *centrifuge.Disconnect) error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil
	}
	t.closed = true
	close(t.closeCh)
	t.mu.Unlock()

	defer func() {
		_ = t.webTransport.Close()
	}()

	if disconnect != nil {
		return t.stream.Close()
	}

	return t.stream.Close()
}

func (s *WebtransportHandler) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	protoType := centrifuge.ProtocolTypeJSON
	format := req.URL.Query().Get("format")
	if format == "protobuf" {
		protoType = centrifuge.ProtocolTypeProtobuf
	}

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

	ctx := req.Context()
	stream, err := wt.AcceptStream(ctx)
	if err != nil {
		s.node.Log(centrifuge.NewLogEntry(centrifuge.LogLevelError, "error accepting stream", map[string]interface{}{"error": err}))
		rw.WriteHeader(http.StatusInternalServerError)
		return
	}

	transport := newWebtransportTransport(protoType, wt, stream)
	c, closeFn, err := centrifuge.NewClient(req.Context(), s.node, transport)
	if err != nil {
		s.node.Log(centrifuge.NewLogEntry(centrifuge.LogLevelError, "error creating client", map[string]interface{}{"transport": transportName}))
		return
	}
	defer func() { _ = closeFn() }()

	if s.node.LogEnabled(centrifuge.LogLevelDebug) {
		s.node.Log(centrifuge.NewLogEntry(centrifuge.LogLevelDebug, "client connection established", map[string]interface{}{"client": c.ID(), "transport": transportName}))
		defer func(started time.Time) {
			s.node.Log(centrifuge.NewLogEntry(centrifuge.LogLevelDebug, "client connection completed", map[string]interface{}{"client": c.ID(), "transport": transportName, "duration": time.Since(started)}))
		}(time.Now())
	}

	var commandDecoder protocol.StreamCommandDecoder
	if protoType == centrifuge.ProtocolTypeJSON {
		commandDecoder = protocol.NewJSONStreamCommandDecoder(stream)
	} else {
		commandDecoder = protocol.NewProtobufStreamCommandDecoder(stream)
	}
	for {
		cmd, err := commandDecoder.Decode()
		if err != nil && err != io.EOF {
			s.node.Log(centrifuge.NewLogEntry(centrifuge.LogLevelInfo, "error decoding command", map[string]interface{}{"client": c.ID(), "user": c.UserID(), "transport": transportName, "error": err.Error()}))
			c.Disconnect(centrifuge.DisconnectBadRequest)
			// Do we need read further here till EOF to process close handshake properly?
			return
		}
		ok := c.HandleCommand(cmd)
		if !ok {
			return
		}
		if err == io.EOF {
			return
		}
	}
}
