package main

import (
	"io/ioutil"
	"net"
	"sync"
	"time"

	"github.com/centrifugal/centrifuge"
	"github.com/centrifugal/protocol"
	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
)

const websocketTransportName = "websocket"

type customWebsocketTransport struct {
	mu      sync.RWMutex
	closed  bool
	closeCh chan struct{}

	conn      net.Conn
	protoType centrifuge.ProtocolType
}

func newWebsocketTransport(conn net.Conn, protoType centrifuge.ProtocolType) *customWebsocketTransport {
	return &customWebsocketTransport{
		conn:      conn,
		protoType: protoType,
		closeCh:   make(chan struct{}),
	}
}

func (t *customWebsocketTransport) Name() string {
	return websocketTransportName
}

func (t *customWebsocketTransport) Protocol() centrifuge.ProtocolType {
	return t.protoType
}

// Unidirectional returns whether transport is unidirectional.
func (t *customWebsocketTransport) Unidirectional() bool {
	return false
}

// Emulation ...
func (t *customWebsocketTransport) Emulation() bool {
	return false
}

// DisabledPushFlags ...
func (t *customWebsocketTransport) DisabledPushFlags() uint64 {
	return centrifuge.PushFlagDisconnect
}

func (t *customWebsocketTransport) ProtocolVersion() centrifuge.ProtocolVersion {
	return centrifuge.ProtocolVersion2
}

// PingPongConfig ...
func (t *customWebsocketTransport) PingPongConfig() centrifuge.PingPongConfig {
	return centrifuge.PingPongConfig{
		PingInterval: 25 * time.Second,
		PongTimeout:  10 * time.Second,
	}
}

func (t *customWebsocketTransport) read() ([]byte, bool, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	h, r, err := wsutil.NextReader(t.conn, ws.StateServerSide)
	if err != nil {
		return nil, false, err
	}
	if h.OpCode.IsControl() {
		return nil, true, wsutil.ControlFrameHandler(t.conn, ws.StateServerSide)(h, r)
	}

	data, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, false, err
	}

	return data, false, nil
}

// Write ...
func (t *customWebsocketTransport) Write(message []byte) error {
	select {
	case <-t.closeCh:
		return nil
	default:
		messageType := ws.OpText
		protoType := protocol.TypeJSON

		if t.Protocol() == centrifuge.ProtocolTypeProtobuf {
			messageType = ws.OpBinary
			protoType = protocol.TypeProtobuf
		}

		encoder := protocol.GetDataEncoder(protoType)
		defer protocol.PutDataEncoder(protoType, encoder)
		_ = encoder.Encode(message)
		return wsutil.WriteServerMessage(t.conn, messageType, encoder.Finish())
	}
}

// WriteMany ...
func (t *customWebsocketTransport) WriteMany(messages ...[]byte) error {
	select {
	case <-t.closeCh:
		return nil
	default:
		messageType := ws.OpText
		protoType := protocol.TypeJSON

		if t.Protocol() == centrifuge.ProtocolTypeProtobuf {
			messageType = ws.OpBinary
			protoType = protocol.TypeProtobuf
		}

		encoder := protocol.GetDataEncoder(protoType)
		defer protocol.PutDataEncoder(protoType, encoder)
		for i := range messages {
			_ = encoder.Encode(messages[i])
		}
		return wsutil.WriteServerMessage(t.conn, messageType, encoder.Finish())
	}
}

// Close ...
func (t *customWebsocketTransport) Close(disconnect centrifuge.Disconnect) error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil
	}
	t.closed = true
	close(t.closeCh)
	t.mu.Unlock()

	if disconnect != centrifuge.DisconnectConnectionClosed {
		data := ws.NewCloseFrameBody(ws.StatusCode(disconnect.Code), disconnect.Reason)
		_ = wsutil.WriteServerMessage(t.conn, ws.OpClose, data)
		return t.conn.Close()
	}
	data := ws.NewCloseFrameBody(ws.StatusNormalClosure, "")
	_ = wsutil.WriteServerMessage(t.conn, ws.OpClose, data)
	return t.conn.Close()
}
