package main

import (
	"io/ioutil"
	"net"
	"sync"

	"github.com/centrifugal/centrifuge"
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

func (t *customWebsocketTransport) Encoding() centrifuge.EncodingType {
	return centrifuge.EncodingTypeJSON
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

func (t *customWebsocketTransport) Write(data []byte) error {
	select {
	case <-t.closeCh:
		return nil
	default:
		messageType := ws.OpText

		if t.Protocol() == centrifuge.ProtocolTypeProtobuf {
			messageType = ws.OpBinary
		}

		if err := wsutil.WriteServerMessage(t.conn, messageType, data); err != nil {
			return err
		}

		return nil
	}
}

func (t *customWebsocketTransport) Close(disconnect *centrifuge.Disconnect) error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil
	}
	t.closed = true
	close(t.closeCh)
	t.mu.Unlock()

	if disconnect != nil {
		data := ws.NewCloseFrameBody(ws.StatusCode(disconnect.Code), disconnect.CloseText())
		_ = wsutil.WriteServerMessage(t.conn, ws.OpClose, data)
		return t.conn.Close()
	}
	data := ws.NewCloseFrameBody(ws.StatusNormalClosure, "")
	_ = wsutil.WriteServerMessage(t.conn, ws.OpClose, data)
	return t.conn.Close()
}
