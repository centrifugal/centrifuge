package centrifuge

// Connection lifecycle fuzzing, see client_lifecycle_fuzz_test.go for the
// harness.
//
// Input byte 0 is the hold mask (bits: join/leave, presence, UnsubscribeHandler,
// sync point, OnConnecting, OnConnect, RefreshHandler, other handlers), byte 1
// the flags (fzConnectFlag*), then 3 bytes per operation (fzConnectStep).

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/centrifugal/protocol"
)

const (
	fzConnectFlagUserLimit    = 1 << 0 // UserConnectionLimit 2
	fzConnectFlagChannelLimit = 1 << 1 // ClientChannelLimit 1
	fzConnectFlagSmallQueue   = 1 << 2 // ClientQueueMaxSize 2KB
	fzConnectFlagShutdown     = 1 << 3 // Node.Shutdown after the operations
	fzConnectFlagHoldMapPres  = 1 << 4 // Hold map client presence calls too
)

// fzConnecting is how OnConnecting answers.
type fzConnecting struct {
	kind byte // fzConnect*
	subs byte // Connect-time subscriptions: bit 0 s0 (recovery), bit 1 s1 (positioning, presence, join/leave, map presence).
	exp  byte // 1: expiring with client-side refresh, 2: with server-side refresh.
}

const (
	fzConnectAllow = iota
	fzConnectDeny
	fzConnectDisconnect
	fzConnectError
)

func (w *fzWorld) onConnecting(_ context.Context, e ConnectEvent) (ConnectReply, error) {
	w.mu.Lock()
	fc := w.byID[e.ClientID]
	dec := fc.connecting
	w.trace = append(w.trace, fmt.Sprintf("  [c%d OnConnecting]", fc.idx))
	w.mu.Unlock()
	w.hold(fzHoldConnecting, fmt.Sprintf("connecting c%d", fc.idx))
	switch dec.kind {
	case fzConnectDeny:
		return ConnectReply{}, ErrorPermissionDenied
	case fzConnectDisconnect:
		return ConnectReply{}, DisconnectInvalidToken
	case fzConnectError:
		return ConnectReply{}, errors.New("connecting error")
	}
	reply := ConnectReply{Credentials: &Credentials{UserID: "u"}}
	if dec.exp != 0 {
		reply.Credentials.ExpireAt = time.Now().Unix() + 3600
		reply.ClientSideRefresh = dec.exp == 1
	}
	if dec.subs != 0 {
		reply.Subscriptions = map[string]SubscribeOptions{}
	}
	if dec.subs&1 != 0 {
		reply.Subscriptions[fzS0] = SubscribeOptions{EnableRecovery: true}
	}
	if dec.subs&2 != 0 {
		reply.Subscriptions[fzS1] = SubscribeOptions{
			EnablePositioning: true, EmitPresence: true, EmitJoinLeave: true,
			MapClientPresenceChannel: "cp:" + fzS1,
		}
	}
	return reply, nil
}

var fzConnectTarget = &fzTarget{
	config: func(data []byte) fzConfig {
		m := uint16(data[0])
		conf := fzConfig{
			connecting: true,
			holdMask: m&1*fzHoldJoinLeave | m>>1&1*fzHoldPresence | m>>2&1*fzHoldUnsub |
				m>>3&1*fzHoldSyncPoint | m>>4&1*fzHoldConnecting | m>>5&1*fzHoldOnConnect |
				m>>6&1*fzHoldRefresh | m>>7&1*fzHoldHandler | fzHoldWrite,
		}
		flags := data[1]
		if flags&fzConnectFlagHoldMapPres != 0 {
			conf.holdMask |= fzHoldMapPresence
		}
		if flags&fzConnectFlagUserLimit != 0 {
			conf.userConnLimit = 2
		}
		if flags&fzConnectFlagChannelLimit != 0 {
			conf.channelLimit = 1
		}
		if flags&fzConnectFlagSmallQueue != 0 {
			conf.clientQueueSize = 2048
		}
		return conf
	},
	start: func(*fzWorld, []byte) {},
	step:  fzConnectStep,
	end: func(w *fzWorld, data []byte) {
		if data[1]&fzConnectFlagShutdown != 0 {
			w.tracef("op Node.Shutdown")
			w.spawn(func() { _ = w.node.Shutdown(context.Background()) })
		}
	},
	alphabet: map[byte][3]byte{
		'A': {0, 0, 0},                       // connect, allowed
		'B': {0, 1, 1 << 3},                  // connect with s0 recovering from the known position
		'C': {0, 0, 2<<3 | 2<<5},             // connect with s1 (presence, join/leave), server-side refresh
		'D': {0, 0, 5},                       // connect, denied
		'E': {2, 0, 0},                       // c0 subscribe s0
		'F': {6, 0, 0},                       // c0 transport closed
		'G': {6, 0, 2},                       // Node.Disconnect u
		'H': {10, 0, 0},                      // release the oldest held call
		'I': {13, 0, 0},                      // publish s0
		'J': {6, 1, 0},                       // c1 transport closed
		'K': {9, 0, 0 | fzRefreshExpired<<2}, // c0 expires, RefreshHandler says expired
		'L': {6, 0, 7},                       // c0 insufficient state on s0
		'M': {5, 0, 1},                       // Node.Unsubscribe u s0
	},
}

const fzConnectOps = 18

// fzConnectStep runs an operation. Operations which need the client's writer
// are skipped until OnConnect returned.
func fzConnectStep(w *fzWorld, op, a, b byte) {
	switch op % fzConnectOps {
	case 0, 1, 17:
		w.connectOp(a, b)
		return
	}
	w.mu.Lock()
	if len(w.conns) == 0 {
		w.mu.Unlock()
		w.tracef("op %d: no connections", op%fzConnectOps)
		return
	}
	w.mu.Unlock()
	fc := w.conn(a)
	w.mu.Lock()
	connected := fc.connected
	w.mu.Unlock()
	// The application gets a Client in OnConnect: its methods are called after
	// that, Node methods and the transport at any time.
	switch op % fzConnectOps {
	case 4, 8, 9:
		if !connected {
			w.tracef("op %d c%d: not connected", op%fzConnectOps, fc.idx)
			return
		}
	case 5:
		if !connected && b&1 == 0 {
			w.tracef("op %d c%d: not connected", op%fzConnectOps, fc.idx)
			return
		}
	case 6, 7:
		if !connected && b%8 != 0 && b%8 != 2 && b%8 != 7 {
			w.tracef("op %d c%d: not connected", op%fzConnectOps, fc.idx)
			return
		}
	}
	switch op % fzConnectOps {
	case 2, 3:
		w.commandOp(fc, a, b)
	case 4:
		w.serverSubscribe(fc, fzStreams[(a>>1)&1], b)
	case 5:
		ch := fzStreams[(a>>1)&1]
		if b&1 == 0 {
			w.tracef("op c%d server Client.Unsubscribe %s", fc.idx, ch)
			w.spawn(func() { fc.c.Unsubscribe(ch) })
		} else {
			w.tracef("op Node.Unsubscribe u %s", ch)
			w.spawn(func() { _ = w.node.Unsubscribe("u", ch) })
		}
	case 6, 7:
		w.endOp(fc, a, b)
	case 8: // ping, pong, pong check
		w.tracef("op c%d ping pong=%v check=%v", fc.idx, b&1 != 0, b&2 != 0)
		w.spawn(func() { fc.c.sendPing() })
		if b&1 != 0 {
			fc.queued.Add(1)
			fc.cmdCh <- fzCmd{cmd: &protocol.Command{}}
			w.settleQueue(fc)
		}
		if b&2 != 0 {
			w.spawn(func() { fc.c.checkPong() })
		}
	case 9: // refresh
		switch b % 3 {
		case 0:
			w.mu.Lock()
			fc.refresh = (b >> 2) & 3
			w.mu.Unlock()
			w.expireOp(fc)
		case 1:
			w.tracef("op c%d Client.Refresh extend", fc.idx)
			w.spawn(func() { _ = fc.c.Refresh(WithRefreshExpireAt(time.Now().Unix() + 3600)) })
		case 2:
			w.tracef("op c%d Client.Refresh expired", fc.idx)
			w.spawn(func() { _ = fc.c.Refresh(WithRefreshExpired(true)) })
		}
	case 10, 11:
		w.tracef("op release %s", w.releaseHold(int(a)))
		time.Sleep(fzSettle)
	case 12:
		w.answerPending(a, b)
	case 13:
		w.publish(a, b)
	case 14:
		w.tracef("op c%d presence tick", fc.idx)
		w.spawn(func() { fc.c.updatePresence() })
	case 15:
		w.tracef("op c%d stall writes %v", fc.idx, b&1 != 0)
		fc.tr.set(func(t *fzTransport) { t.stall = b&1 != 0 })
	case 16:
		w.tracef("op settle")
		time.Sleep(5 * fzSettle)
	}
}

func (w *fzWorld) connectOp(a, b byte) {
	w.mu.Lock()
	n := len(w.conns)
	w.mu.Unlock()
	if n >= 6 {
		w.tracef("op connect: enough connections")
		return
	}
	fc := w.newConn()
	kind := byte(fzConnectAllow)
	if k := b & 7; k >= 5 {
		kind = k - 4
	}
	dec := fzConnecting{kind: kind, subs: (b >> 3) & 3, exp: ((b >> 5) & 3) % 3}
	w.mu.Lock()
	fc.connecting = dec
	fc.refresh = (a >> 2) & 3
	w.mu.Unlock()
	var recover []string
	if a&1 != 0 {
		recover = fzStreams
	}
	w.tracef("op c%d connect kind=%d subs=%02b exp=%d recover=%v", fc.idx, dec.kind, dec.subs, dec.exp, recover != nil)
	fc.connect(recover)
	w.settleQueue(fc)
}

// commandOp sends a command racing whatever else happens to the connection.
func (w *fzWorld) commandOp(fc *fzConn, a, b byte) {
	ch := fzStreams[(b>>3)&1]
	cmd := &protocol.Command{}
	m := fzMeta{ch: ch, kind: "other"}
	var dec fzDecision
	switch b % 8 {
	case 0:
		req := &protocol.SubscribeRequest{Channel: ch}
		w.mu.Lock()
		pos, ok := w.lastPos[ch]
		w.mu.Unlock()
		if a&2 != 0 && ok {
			req.Recover, req.Offset, req.Epoch = true, pos.Offset, pos.Epoch
		}
		cmd.Subscribe = req
		m = fzMeta{ch: ch, kind: "sub", req: req}
		dec = fzDecision{set: true, mode: (a >> 2) & 3, opts: b >> 4}
	case 1:
		cmd.Unsubscribe = &protocol.UnsubscribeRequest{Channel: ch}
		m.kind = "unsub"
	case 2:
		cmd.Publish = &protocol.PublishRequest{Channel: ch, Data: []byte(`{}`)}
	case 3:
		cmd.Presence = &protocol.PresenceRequest{Channel: ch}
	case 4:
		cmd.History = &protocol.HistoryRequest{Channel: ch}
	case 5:
		cmd.Rpc = &protocol.RPCRequest{Method: "m", Data: []byte(`{}`)}
	case 6:
		cmd.Send = &protocol.SendRequest{Data: []byte(`{}`)}
	case 7:
		w.mu.Lock()
		fc.refresh = (b >> 4) & 3
		w.mu.Unlock()
		cmd.Refresh = &protocol.RefreshRequest{Token: "t"}
	}
	w.tracef("op c%d command %d %s", fc.idx, b%8, ch)
	fc.send(cmd, dec, m)
	w.settleQueue(fc)
}

// expireOp moves the connection's expiration to the past and expires it.
func (w *fzWorld) expireOp(fc *fzConn) {
	w.tracef("op c%d expire", fc.idx)
	fc.c.mu.Lock()
	if fc.c.exp > 0 {
		fc.c.exp = time.Now().Unix() - 1
	}
	fc.c.mu.Unlock()
	w.spawn(func() { fc.c.expire() })
}

// endOp ends the connection one of the ways a connection ends.
func (w *fzWorld) endOp(fc *fzConn, a, b byte) {
	switch b % 8 {
	case 0:
		w.tracef("op c%d transport closed", fc.idx)
		w.spawn(func() { _ = fc.c.close(DisconnectConnectionClosed) })
	case 1:
		w.tracef("op c%d Client.Disconnect()", fc.idx)
		w.spawn(func() { fc.c.Disconnect() })
	case 2:
		w.tracef("op Node.Disconnect u")
		w.spawn(func() { _ = w.node.Disconnect("u") })
	case 3:
		w.tracef("op c%d write error", fc.idx)
		fc.tr.set(func(t *fzTransport) { t.fail = true })
		w.spawn(func() { _ = fc.c.Send([]byte(`{}`)) })
	case 4:
		w.tracef("op c%d slow", fc.idx)
		fc.tr.set(func(t *fzTransport) { t.stall = true })
		w.spawn(func() {
			data := []byte(`"` + strings.Repeat("x", 1000) + `"`)
			for i := 0; i < 4; i++ {
				_ = fc.c.Send(data)
			}
		})
	case 5:
		w.tracef("op c%d no pong", fc.idx)
		w.spawn(func() {
			fc.c.sendPing()
			fc.c.checkPong()
		})
	case 6:
		w.expireOp(fc)
	case 7:
		ch := fzStreams[(a>>1)&1]
		w.tracef("op insufficient state %s", ch)
		w.spawn(func() {
			res, err := w.node.History(ch, WithLimit(0))
			if err != nil {
				return
			}
			off := res.Offset + 2
			_ = w.node.handlePublication(ch, StreamPosition{Offset: off, Epoch: res.Epoch}, &Publication{Offset: off, Data: []byte(`{}`)}, nil, nil)
		})
	}
}

func FuzzClientConnectLifecycle(f *testing.F) {
	fzFuzz(f, fzConnectTarget)
}
