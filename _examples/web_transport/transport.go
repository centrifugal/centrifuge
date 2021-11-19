package main

import (
	"bufio"
	"encoding/binary"
	"flag"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/centrifugal/centrifuge"
	"github.com/centrifugal/protocol"
	"github.com/lucas-clemente/quic-go"
	"github.com/lucas-clemente/quic-go/http3"
)

type webtransportHandler struct {
	node *centrifuge.Node
}

func newWebtransportHandler(node *centrifuge.Node) *webtransportHandler {
	return &webtransportHandler{node}
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
		var err error
		protoType := protocol.TypeJSON
		if t.Protocol() == centrifuge.ProtocolTypeProtobuf {
			protoType = protocol.TypeProtobuf
		}

		encoder := protocol.GetDataEncoder(protoType)
		defer protocol.PutDataEncoder(protoType, encoder)

		err = encoder.Encode(message)
		if err != nil {
			return err
		}

		if protoType == protocol.TypeJSON {
			_, err = t.stream.Write(append(encoder.Finish(), '\n'))
		} else if protoType == protocol.TypeProtobuf {
			_, err = t.stream.Write(encoder.Finish())
		}

		return err
	}
}

// WriteMany ...
func (t *webtransportTransport) WriteMany(messages ...[]byte) error {
	select {
	case <-t.closeCh:
		return nil
	default:
		var err error
		protoType := protocol.TypeJSON
		if t.Protocol() == centrifuge.ProtocolTypeProtobuf {
			protoType = protocol.TypeProtobuf
		}

		encoder := protocol.GetDataEncoder(protoType)
		defer protocol.PutDataEncoder(protoType, encoder)

		for i := range messages {
			err = encoder.Encode(messages[i])
			if err != nil {
				return err
			}
		}

		if protoType == protocol.TypeJSON {
			_, err = t.stream.Write(append(encoder.Finish(), '\n'))
		} else if protoType == protocol.TypeProtobuf {
			_, err = t.stream.Write(encoder.Finish())
		}

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
		err := t.webTransport.Close()
		if err != nil {
			log.Println("error on web transport closing: ", err)
		}
	}()

	if disconnect != nil {
		return t.stream.Close()
	}

	return t.stream.Close()
}

func (s *webtransportHandler) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	if format == "" {
		msgFormat := flag.String("format", "json", "help message for flag n")
		flag.Parse()
		format = *msgFormat
	}

	protoType := centrifuge.ProtocolTypeJSON
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
		s.node.Log(centrifuge.NewLogEntry(centrifuge.LogLevelError, "error creating stream", map[string]interface{}{"error": err}))
		rw.WriteHeader(http.StatusInternalServerError)
		return
	}

	//log.Println("initiated by ", stream.StreamID().InitiatedBy())

	transport := newWebtransportTransport(protoType, wt, stream)
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
		var reply []byte
		if protoType == centrifuge.ProtocolTypeJSON {
			reply, err = r.ReadBytes('\n')
			if err != nil {
				log.Println("error on read bytes stream: ", err)
				return
			}

			s.node.Log(centrifuge.NewLogEntry(centrifuge.LogLevelDebug, "Stream message",
				map[string]interface{}{
					"message": string(reply),
				}))
		} else if protoType == centrifuge.ProtocolTypeProtobuf {
			msgLength, err := binary.ReadUvarint(r)
			if err != nil {
				if err != io.EOF {
					log.Println("error on read message length: ", err)
				}

				return
			}

			msgLengthBytes := make([]byte, binary.MaxVarintLen64)
			bytesNum := binary.PutUvarint(msgLengthBytes, msgLength)

			data := make([]byte, msgLength)
			_, err = r.Read(data)
			if err != nil {
				log.Println("error on read bytes from stream buffer: ", err)

				return
			}

			reply = append(msgLengthBytes[:bytesNum], data...)
		}

		ok := c.Handle(reply)
		if !ok {
			return
		}
	}
}
