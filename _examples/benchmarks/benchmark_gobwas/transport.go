package main

import (
	"encoding/json"
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

	conn net.Conn
	enc  centrifuge.Encoding
}

func newWebsocketTransport(conn net.Conn, enc centrifuge.Encoding) *customWebsocketTransport {
	return &customWebsocketTransport{
		conn:    conn,
		enc:     enc,
		closeCh: make(chan struct{}),
	}
}

func (t *customWebsocketTransport) Name() string {
	return websocketTransportName
}

func (t *customWebsocketTransport) Encoding() centrifuge.Encoding {
	return t.enc
}

func (t *customWebsocketTransport) Meta() centrifuge.TransportMeta {
	return centrifuge.TransportMeta{
		Request: nil,
	}
}

func (t *customWebsocketTransport) read() ([]byte, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	h, r, err := wsutil.NextReader(t.conn, ws.StateServerSide)
	if err != nil {
		return nil, err
	}
	if h.OpCode.IsControl() {
		return nil, wsutil.ControlFrameHandler(t.conn, ws.StateServerSide)(h, r)
	}

	data, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}

	return data, nil
}

func (t *customWebsocketTransport) Write(data []byte) error {
	select {
	case <-t.closeCh:
		return nil
	default:
		// var messageType = ws.OpBinary
		// if t.Encoding() == centrifuge.EncodingProtobuf {
		// 	messageType = websocket.MessageBinary
		// }
		err := wsutil.WriteServerMessage(t.conn, ws.OpText, data)
		if err != nil {
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
		reason, err := json.Marshal(disconnect)
		if err != nil {
			return err
		}
		data := ws.NewCloseFrameBody(ws.StatusCode(disconnect.Code), string(reason))
		wsutil.WriteServerMessage(t.conn, ws.OpClose, data)
		return t.conn.Close()
	}
	data := ws.NewCloseFrameBody(ws.StatusNormalClosure, "")
	wsutil.WriteServerMessage(t.conn, ws.OpClose, data)
	return t.conn.Close()
}
