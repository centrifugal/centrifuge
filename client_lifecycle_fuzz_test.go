package centrifuge

// Model-based fuzzing of the client lifecycle.
//
// A fuzz input encodes a sequence of operations on a few connections of one
// user and a few channels: client commands go through HandleCommand (one queue
// per connection, like a transport reading a socket), server-side calls run
// concurrently. Broker, presence and map broker calls, handlers, recovery sync
// points and transport writes can be held until the sequence releases them. The
// frames written to each transport are decoded into a model of what the client
// sees. Checked:
//
//   - handlers: one UnsubscribeHandler call per subscribe attempt
//     SubscribeHandler allowed, the next SubscribeHandler of a channel only after
//     the previous attempt's UnsubscribeHandler, nothing after DisconnectHandler
//     except UnsubscribeHandler calls for attempts allowed after it,
//     DisconnectHandler once per connection OnConnect was called for;
//   - wire: per channel frame order (nothing of a channel while unsubscribed,
//     contiguous offsets for positioned subscriptions and recovery), nothing
//     before the connect reply, no writes after the transport is closed;
//   - state at quiet points: subscriptions in the hub, presence, map client
//     presence and join/leave alternation agree with the client's subscriptions;
//   - at the end: all of it is empty, no leaked tracking counters, recovery
//     buffers, timers or goroutines.
//
// FuzzClientSubscribeLifecycle and FuzzClientConnectLifecycle (see
// client_connect_fuzz_test.go) share all of this and differ in their operations.
// Their seed corpora are in testdata/fuzz. Run one with (minimizing new inputs
// for the default 60s would take most of the time):
//
//	go test -run '^$' -fuzz '^FuzzClientSubscribeLifecycle$' -fuzztime 10m -fuzzminimizetime 10x -parallel 4 .
//
// Environment variables of the helper tests (skipped when unset):
//
//	FZ_TARGET  subscribe (default) or connect.
//	FZ_INPUT   hex input to replay in TestFuzzLifecycleReplay (as printed on a
//	           failure), FZ_REPEAT times (default 1). FZ_TRACE=1 logs the trace.
//	FZ_ALPHA   letters of the target's alphabet: TestFuzzLifecycleEnumerate runs
//	           every sequence of FZ_DEPTH of them with hold mask FZ_MASK (and flags
//	           FZ_FLAGS), shard FZ_SHARD of FZ_SHARDS, appending failures to FZ_OUT.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime/pprof"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/centrifugal/protocol"
)

const (
	fzS0 = testChannelRecoveryOrderingPrefix + ":s0"
	fzS1 = testChannelRecoveryOrderingPrefix + ":s1"
	fzM0 = testChannelRecoveryOrderingPrefix + ":m0"
	fzM1 = testChannelRecoveryOrderingPrefix + ":m1"
	fzP0 = "poll:p0"
)

var (
	fzStreams = []string{fzS0, fzS1}
	fzMaps    = []string{fzM0, fzM1}
	fzAll     = []string{fzS0, fzS1, fzM0, fzM1, fzP0}
)

// Calls which a world's hold mask holds until released.
const (
	fzHoldJoinLeave   = 1 << 0
	fzHoldPresence    = 1 << 1
	fzHoldMapPresence = 1 << 2
	fzHoldUnsub       = 1 << 3 // UnsubscribeHandler
	fzHoldSyncPoint   = 1 << 4
	fzHoldMapCommit   = 1 << 5
	fzHoldMapRead     = 1 << 6
	fzHoldConnecting  = 1 << 7 // OnConnecting
	fzHoldOnConnect   = 1 << 8 // OnConnect, before it sets the client's handlers
	fzHoldRefresh     = 1 << 9 // RefreshHandler
	fzHoldHandler     = 1 << 10
	// Transport writes of a connection, while its writes are stalled.
	fzHoldWrite = 1 << 11
)

// fzCur is the world of the running input, for the hooks of the sync points.
var fzCur atomic.Pointer[fzWorld]

// fzSettle is how long an operation may run before the next one starts.
const fzSettle = 4 * time.Millisecond

type fzHold struct {
	desc    string
	release chan struct{}
}

type fzPending struct {
	fc   *fzConn
	e    SubscribeEvent
	cb   SubscribeCallback
	opts byte
}

type fzConfig struct {
	holdMask uint16
	// connecting sets OnConnecting answering with fzConn.connecting.
	connecting      bool
	userConnLimit   int
	channelLimit    int
	clientQueueSize int
}

type fzWorld struct {
	tb    testing.TB
	label string
	node  *Node
	// Not wrapped, to read the state without holds.
	mapBroker *MemoryMapBroker
	presence  *MemoryPresenceManager

	mu         sync.Mutex
	holdMask   uint16
	auto       bool // Release everything from now on.
	holds      []*fzHold
	pending    []*fzPending
	conns      []*fzConn
	byID       map[string]*fzConn
	violations []string
	trace      []string
	timeouts   []string
	errLogs    []string
	joinLog    map[string]string
	// Positions the client knows, for recovery.
	lastPos map[string]StreamPosition

	running atomic.Int32 // Operations in progress.
}

// violate must be called with w.mu held.
func (w *fzWorld) violate(format string, args ...any) {
	w.violations = append(w.violations, fmt.Sprintf(format, args...))
}

func (w *fzWorld) violateU(format string, args ...any) {
	w.mu.Lock()
	w.violate(format, args...)
	w.mu.Unlock()
}

func (w *fzWorld) tracef(format string, args ...any) {
	w.mu.Lock()
	w.trace = append(w.trace, fmt.Sprintf(format, args...))
	w.mu.Unlock()
}

func (w *fzWorld) holding(kind uint16) bool {
	return !w.auto && w.holdMask&kind != 0
}

func (w *fzWorld) hold(kind uint16, desc string) {
	w.mu.Lock()
	if !w.holding(kind) {
		w.mu.Unlock()
		return
	}
	h := &fzHold{desc: desc, release: make(chan struct{})}
	w.holds = append(w.holds, h)
	w.trace = append(w.trace, "  [held "+desc+"]")
	w.mu.Unlock()
	<-h.release
}

func (w *fzWorld) releaseHold(i int) string {
	w.mu.Lock()
	if len(w.holds) == 0 {
		w.mu.Unlock()
		return "none"
	}
	i %= len(w.holds)
	h := w.holds[i]
	w.holds = slices.Delete(w.holds, i, i+1)
	w.mu.Unlock()
	close(h.release)
	return h.desc
}

func (w *fzWorld) releaseAll() {
	w.mu.Lock()
	w.auto = true
	hs := w.holds
	w.holds = nil
	w.mu.Unlock()
	for _, h := range hs {
		close(h.release)
	}
}

func (w *fzWorld) recordJoinLeave(ch, uid, ev string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	key := ch + "|" + uid
	last := w.joinLog[key]
	if ev == "join" && last == "join" {
		w.violate("join/leave: join after join for %s", key)
	}
	if ev == "leave" && last != "join" {
		w.violate("join/leave: leave without join for %s (last %q)", key, last)
	}
	w.joinLog[key] = ev
	w.trace = append(w.trace, "  [broker "+ev+" "+key+"]")
}

func (w *fzWorld) onLog(e LogEntry) {
	m := e.Message
	w.mu.Lock()
	defer w.mu.Unlock()
	if (strings.Contains(m, "timeout") && !strings.Contains(m, "catch-up")) || strings.Contains(m, "not finished") {
		w.timeouts = append(w.timeouts, m)
	}
	if e.Level >= LogLevelError && !strings.Contains(m, "invoked more than once") {
		w.errLogs = append(w.errLogs, fmt.Sprintf("%s %v", m, e.Fields))
	}
}

func fzShortID(id string) string { return id[:min(4, len(id))] }

type fzBroker struct {
	*MemoryBroker
	w *fzWorld
}

func (b *fzBroker) PublishJoin(ch string, info *ClientInfo) error {
	b.w.hold(fzHoldJoinLeave, "join "+ch+" "+fzShortID(info.ClientID))
	b.w.recordJoinLeave(ch, info.ClientID, "join")
	return b.MemoryBroker.PublishJoin(ch, info)
}

func (b *fzBroker) PublishLeave(ch string, info *ClientInfo) error {
	b.w.hold(fzHoldJoinLeave, "leave "+ch+" "+fzShortID(info.ClientID))
	b.w.recordJoinLeave(ch, info.ClientID, "leave")
	return b.MemoryBroker.PublishLeave(ch, info)
}

type fzPresence struct {
	*MemoryPresenceManager
	w *fzWorld
}

func (p *fzPresence) AddPresence(ch string, uid string, info *ClientInfo) error {
	p.w.hold(fzHoldPresence, "addPresence "+ch+" "+fzShortID(uid))
	return p.MemoryPresenceManager.AddPresence(ch, uid, info)
}

func (p *fzPresence) RemovePresence(ch string, uid string, user string) error {
	p.w.hold(fzHoldPresence, "removePresence "+ch+" "+fzShortID(uid))
	return p.MemoryPresenceManager.RemovePresence(ch, uid, user)
}

type fzMapBroker struct {
	*MemoryMapBroker
	w *fzWorld
}

func (b *fzMapBroker) Publish(ctx context.Context, ch string, key string, opts MapPublishOptions) (MapUpdateResult, error) {
	if strings.HasPrefix(ch, "cp:") {
		b.w.hold(fzHoldMapPresence, "mapPresenceAdd "+ch+" "+fzShortID(key))
	}
	return b.MemoryMapBroker.Publish(ctx, ch, key, opts)
}

func (b *fzMapBroker) Remove(ctx context.Context, ch string, key string, opts MapRemoveOptions) (MapUpdateResult, error) {
	if strings.HasPrefix(ch, "cp:") {
		b.w.hold(fzHoldMapPresence, "mapPresenceRemove "+ch+" "+fzShortID(key))
	}
	return b.MemoryMapBroker.Remove(ctx, ch, key, opts)
}

func (b *fzMapBroker) ReadState(ctx context.Context, ch string, opts MapReadStateOptions) (MapStateResult, error) {
	if !strings.HasPrefix(ch, "cp:") {
		b.w.hold(fzHoldMapRead, "mapReadState "+ch)
	}
	return b.MemoryMapBroker.ReadState(ctx, ch, opts)
}

func (b *fzMapBroker) ReadStream(ctx context.Context, ch string, opts MapReadStreamOptions) (MapStreamResult, error) {
	if !strings.HasPrefix(ch, "cp:") {
		b.w.hold(fzHoldMapRead, "mapReadStream "+ch)
	}
	return b.MemoryMapBroker.ReadStream(ctx, ch, opts)
}

// fzTransport decodes frames into the connection's wire model as they are
// written. Its writes can be stalled (held) or made to fail.
type fzTransport struct {
	fc     *fzConn
	cancel func()

	mu     sync.Mutex
	closed bool
	stall  bool
	fail   bool
	// marker is written by fzWorld.flushWire, markerSeen is closed when it is.
	marker     []byte
	markerSeen chan struct{}
}

func (t *fzTransport) Name() string                     { return transportWebsocket }
func (t *fzTransport) AcceptProtocol() string           { return "h1" }
func (t *fzTransport) Protocol() ProtocolType           { return ProtocolTypeJSON }
func (t *fzTransport) ProtocolVersion() ProtocolVersion { return ProtocolVersion2 }
func (t *fzTransport) Unidirectional() bool             { return false }
func (t *fzTransport) Emulation() bool                  { return false }
func (t *fzTransport) DisabledPushFlags() uint64        { return PushFlagDisconnect }

// PingPongConfig: timers never fire during an input, pings are operations.
func (t *fzTransport) PingPongConfig() PingPongConfig {
	return PingPongConfig{PingInterval: time.Hour, PongTimeout: time.Minute}
}

func (t *fzTransport) Write(data []byte) error { return t.WriteMany(data) }

func (t *fzTransport) WriteMany(data ...[]byte) error {
	w := t.fc.w
	t.mu.Lock()
	stall := t.stall
	t.mu.Unlock()
	if stall {
		w.hold(fzHoldWrite, fmt.Sprintf("write c%d", t.fc.idx))
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		w.violateU("client %d: write after transport close", t.fc.idx)
		return io.EOF
	}
	if t.fail {
		return errors.New("write error")
	}
	for _, d := range data {
		if t.marker != nil && len(d) > 0 && &d[0] == &t.marker[0] {
			close(t.markerSeen)
			t.marker = nil
			continue
		}
		t.fc.processFrame(d)
	}
	return nil
}

func (t *fzTransport) Close(Disconnect) error {
	t.mu.Lock()
	t.closed = true
	t.mu.Unlock()
	t.cancel()
	return nil
}

func (t *fzTransport) set(f func(t *fzTransport)) {
	t.mu.Lock()
	f(t)
	t.mu.Unlock()
}

const (
	wUnsub = iota
	wSub
	wMapLoading
)

func fzState(s int) string {
	return [...]string{"unsubscribed", "subscribed", "mapLoading"}[s]
}

// fzWire is the state of a channel as the client sees it.
type fzWire struct {
	state      int
	serverSide bool // Subscribed by a subscribe push.
	unsubSent  int  // Client unsubscribe commands without reply.
	// unsubByReply: unsubscribed by a client unsubscribe reply, a server
	// unsubscribe which removed the subscription first may still push.
	unsubByReply bool
	ambiguous    bool
	// Requests with ids up to since were sent before the current subscription:
	// their error replies do not end it.
	since      uint32
	positioned bool
	offset     uint64
	epoch      string
	mapNext    *protocol.SubscribeRequest
}

type fzMeta struct {
	ch   string
	kind string // connect, sub, mapsub, poll, unsub, other
	req  *protocol.SubscribeRequest
	subs map[string]*protocol.SubscribeRequest // Of a connect.
}

// fzRel is an UnsubscribeHandler call for a client-side subscription.
type fzRel struct {
	subscribed bool
	code       uint32
}

// fzCand is a SubscribeHandler call which came while UnsubscribeHandler calls
// from..to of the channel's previous attempts were not completed.
type fzCand struct {
	ch       string
	from, to int
}

type fzDecision struct {
	set  bool
	mode byte
	opts byte
}

type fzCmd struct {
	cmd *protocol.Command
	dec fzDecision
}

// fzConn is a connection with its handlers and wire model.
type fzConn struct {
	w      *fzWorld
	idx    int
	c      *Client
	tr     *fzTransport
	cmdCh  chan fzCmd
	queued atomic.Int32
	nextID uint32

	// Guarded by w.mu.
	cur          fzDecision // Of the command in HandleCommand.
	connecting   fzConnecting
	refresh      byte // How RefreshHandler answers, see fzConn.onRefresh.
	stopped      bool // HandleCommand returned false.
	onConnects   int
	connected    bool // OnConnect returned.
	allowed      map[string]int
	released     map[string]int
	completed    map[string]int
	lateAllowed  map[string]int
	lateReleases map[string]int
	rels         map[string][]fzRel
	cands        []fzCand
	disconnected bool
	disconnects  int
	connectReply bool // Successful connect reply on the wire.
	wire         map[string]*fzWire
	meta         map[uint32]fzMeta
}

func (fc *fzConn) wireCh(ch string) *fzWire {
	wc := fc.wire[ch]
	if wc == nil {
		wc = &fzWire{}
		fc.wire[ch] = wc
	}
	return wc
}

func (fc *fzConn) closed() bool {
	fc.c.mu.RLock()
	defer fc.c.mu.RUnlock()
	return fc.c.status == statusClosed
}

// handlerCall checks a handler call of the connection, with w.mu held.
func (fc *fzConn) handlerCall(name string) {
	if !fc.connected {
		fc.w.violate("client %d: %s before OnConnect returned", fc.idx, name)
	}
}

func (fc *fzConn) buildReply(e SubscribeEvent, opts byte, expired bool) SubscribeReply {
	var r SubscribeReply
	switch e.Type {
	case SubscriptionTypeMap:
		r.Options.Type = SubscriptionTypeMap
	case SubscriptionTypeSharedPoll:
		r.Options.ExpireAt = time.Now().Unix() + 3600
		r.ClientSideRefresh = true
	default:
		r.Options.EnableRecovery = opts&1 != 0
		r.Options.EnablePositioning = opts&16 != 0
	}
	r.Options.EmitPresence = opts&2 != 0
	r.Options.EmitJoinLeave = opts&4 != 0
	if opts&8 != 0 {
		r.Options.MapClientPresenceChannel = "cp:" + e.Channel
	}
	if expired {
		r.Options.ExpireAt = time.Now().Unix() - 10
		r.ClientSideRefresh = false
	}
	return r
}

// How a SubscribeHandler call is answered.
const (
	fzAllow = iota
	fzDeny
	fzExpired // Allowed with an ExpireAt in the past, refused by Centrifuge.
	fzAllowTwice
	fzDenyThenAllow
)

func (fc *fzConn) answer(e SubscribeEvent, cb SubscribeCallback, kind int, opts byte) {
	w := fc.w
	reply := fc.buildReply(e, opts, kind == fzExpired)
	var err error
	if kind == fzDeny || kind == fzDenyThenAllow {
		err = ErrorPermissionDenied
	}
	if err == nil {
		closed := fc.closed()
		w.mu.Lock()
		fc.allowed[e.Channel]++
		if closed || fc.disconnected {
			fc.lateAllowed[e.Channel]++
		}
		w.mu.Unlock()
	}
	cb(reply, err)
	switch kind {
	case fzAllowTwice:
		cb(reply, nil)
	case fzDenyThenAllow:
		cb(fc.buildReply(e, opts, false), nil)
	}
}

func (fc *fzConn) onSubscribe(e SubscribeEvent, cb SubscribeCallback) {
	w := fc.w
	w.mu.Lock()
	fc.handlerCall("SubscribeHandler")
	dec := fc.cur
	fc.cur = fzDecision{}
	ch := e.Channel
	if fc.allowed[ch] > fc.completed[ch] {
		fc.cands = append(fc.cands, fzCand{ch: ch, from: fc.completed[ch], to: fc.allowed[ch]})
	}
	if fc.disconnected {
		w.violate("client %d: SubscribeHandler after DisconnectHandler on %s", fc.idx, ch)
	}
	w.trace = append(w.trace, fmt.Sprintf("  [c%d SubscribeHandler %s mode=%d]", fc.idx, ch, dec.mode&3))
	if !dec.set {
		dec.mode = 2
	}
	if dec.mode&3 == 2 {
		w.pending = append(w.pending, &fzPending{fc: fc, e: e, cb: cb, opts: dec.opts})
		w.mu.Unlock()
		return
	}
	w.mu.Unlock()
	switch dec.mode & 3 {
	case 0:
		fc.answer(e, cb, fzAllow, dec.opts)
	case 1:
		fc.answer(e, cb, fzDeny, dec.opts)
	case 3:
		fc.answer(e, cb, fzExpired, dec.opts)
	}
}

func (fc *fzConn) onUnsubscribe(e UnsubscribeEvent) {
	w := fc.w
	w.mu.Lock()
	fc.handlerCall("UnsubscribeHandler")
	w.trace = append(w.trace, fmt.Sprintf("  [c%d UnsubscribeHandler %s code=%d subscribed=%v serverSide=%v]", fc.idx, e.Channel, e.Code, e.Subscribed, e.ServerSide))
	if e.ServerSide {
		if fc.disconnected {
			w.violate("client %d: UnsubscribeHandler of server-side %s after DisconnectHandler", fc.idx, e.Channel)
		}
		w.mu.Unlock()
		return
	}
	ch := e.Channel
	fc.released[ch]++
	if fc.released[ch] > fc.allowed[ch] {
		w.violate("client %d: UnsubscribeHandler without allowed attempt on %s (code %d subscribed %v)", fc.idx, ch, e.Code, e.Subscribed)
	}
	if fc.disconnected {
		fc.lateReleases[ch]++
	}
	fc.rels[ch] = append(fc.rels[ch], fzRel{subscribed: e.Subscribed, code: e.Code})
	w.mu.Unlock()
	w.hold(fzHoldUnsub, fmt.Sprintf("unsubHandler c%d %s", fc.idx, ch))
	w.mu.Lock()
	fc.completed[ch]++
	w.mu.Unlock()
}

func (fc *fzConn) onDisconnect(e DisconnectEvent) {
	w := fc.w
	w.mu.Lock()
	fc.handlerCall("DisconnectHandler")
	fc.disconnected = true
	fc.disconnects++
	w.trace = append(w.trace, fmt.Sprintf("  [c%d DisconnectHandler code=%d]", fc.idx, e.Code))
	w.mu.Unlock()
}

// onHandler is RPC, publish, presence, history and send handlers: they answer
// asynchronously after an optional hold.
func (fc *fzConn) onHandler(name string, answer func()) {
	w := fc.w
	w.mu.Lock()
	fc.handlerCall(name)
	w.trace = append(w.trace, fmt.Sprintf("  [c%d %s]", fc.idx, name))
	w.mu.Unlock()
	w.async(func() {
		w.hold(fzHoldHandler, fmt.Sprintf("%s c%d", name, fc.idx))
		answer()
	})
}

// async runs f as an operation in progress.
func (w *fzWorld) async(f func()) {
	w.running.Add(1)
	go func() {
		defer w.running.Add(-1)
		f()
	}()
}

// How RefreshHandler answers.
const (
	fzRefreshExtend = iota
	fzRefreshExpired
	fzRefreshError
	fzRefreshDisconnect
)

func (fc *fzConn) onRefresh(e RefreshEvent, cb RefreshCallback) {
	w := fc.w
	w.mu.Lock()
	fc.handlerCall("RefreshHandler")
	mode := fc.refresh
	w.trace = append(w.trace, fmt.Sprintf("  [c%d RefreshHandler clientSide=%v mode=%d]", fc.idx, e.ClientSideRefresh, mode))
	w.mu.Unlock()
	w.async(func() {
		w.hold(fzHoldRefresh, fmt.Sprintf("refresh c%d", fc.idx))
		switch mode % 4 {
		case fzRefreshExtend:
			cb(RefreshReply{ExpireAt: time.Now().Unix() + 3600}, nil)
		case fzRefreshExpired:
			cb(RefreshReply{Expired: true}, nil)
		case fzRefreshError:
			cb(RefreshReply{}, ErrorInternal)
		case fzRefreshDisconnect:
			cb(RefreshReply{}, DisconnectExpired)
		}
	})
}

func (fc *fzConn) onConnect() {
	w := fc.w
	c := fc.c
	w.mu.Lock()
	fc.onConnects++
	if fc.onConnects > 1 {
		w.violate("client %d: OnConnect called %d times", fc.idx, fc.onConnects)
	}
	w.trace = append(w.trace, fmt.Sprintf("  [c%d OnConnect]", fc.idx))
	w.mu.Unlock()
	w.hold(fzHoldOnConnect, fmt.Sprintf("onConnect c%d", fc.idx))
	c.OnSubscribe(fc.onSubscribe)
	c.OnUnsubscribe(fc.onUnsubscribe)
	c.OnDisconnect(fc.onDisconnect)
	c.OnRefresh(fc.onRefresh)
	c.OnRPC(func(e RPCEvent, cb RPCCallback) {
		fc.onHandler("RPCHandler", func() { cb(RPCReply{}, nil) })
	})
	c.OnPublish(func(e PublishEvent, cb PublishCallback) {
		fc.onHandler("PublishHandler", func() {
			cb(PublishReply{Options: PublishOptions{HistorySize: 100, HistoryTTL: time.Minute}}, nil)
		})
	})
	c.OnPresence(func(e PresenceEvent, cb PresenceCallback) {
		fc.onHandler("PresenceHandler", func() { cb(PresenceReply{}, nil) })
	})
	c.OnHistory(func(e HistoryEvent, cb HistoryCallback) {
		fc.onHandler("HistoryHandler", func() { cb(HistoryReply{}, nil) })
	})
	c.OnMessage(func(e MessageEvent) {
		fc.onHandler("MessageHandler", func() {})
	})
	w.mu.Lock()
	fc.connected = true
	w.mu.Unlock()
}

// send queues a command, its id is set here.
func (fc *fzConn) send(cmd *protocol.Command, dec fzDecision, m fzMeta) {
	w := fc.w
	w.mu.Lock()
	fc.nextID++
	cmd.Id = fc.nextID
	if m.kind != "" {
		fc.meta[cmd.Id] = m
	}
	if m.kind == "unsub" {
		fc.wireCh(m.ch).unsubSent++
	}
	w.mu.Unlock()
	fc.queued.Add(1)
	fc.cmdCh <- fzCmd{cmd: cmd, dec: dec}
}

// runQueue handles the commands like a transport: after HandleCommand returned
// false it reads no more and closes the client.
func (fc *fzConn) runQueue() {
	for x := range fc.cmdCh {
		fc.w.mu.Lock()
		stopped := fc.stopped
		fc.cur = x.dec
		fc.w.mu.Unlock()
		if !stopped && !fc.c.HandleCommand(x.cmd, 0) {
			fc.w.mu.Lock()
			fc.stopped = true
			fc.w.mu.Unlock()
			_ = fc.c.close(DisconnectConnectionClosed)
		}
		fc.w.mu.Lock()
		fc.cur = fzDecision{}
		fc.w.mu.Unlock()
		fc.queued.Add(-1)
	}
}

func (fc *fzConn) processFrame(data []byte) {
	dec := protocol.NewJSONReplyDecoder(data)
	for {
		r, err := dec.Decode()
		if err != nil {
			return
		}
		fc.w.mu.Lock()
		fc.onReply(r)
		fc.w.mu.Unlock()
	}
}

// subscribed applies a successful subscribe result to the wire model.
func (fc *fzConn) subscribed(ch string, wc *fzWire, res *protocol.SubscribeResult, req *protocol.SubscribeRequest) {
	w := fc.w
	wc.state = wSub
	wc.unsubByReply = false
	wc.serverSide = false
	wc.ambiguous = false
	if res == nil {
		wc.positioned = false
		return
	}
	wc.positioned = res.Recoverable || res.Positioned
	wc.offset, wc.epoch = res.Offset, res.Epoch
	if res.Recovered && req != nil {
		exp := req.Offset + 1
		for _, p := range res.Publications {
			if p.Offset != exp {
				w.violate("client %d wire: recovered publications on %s not contiguous: got %d want %d", fc.idx, ch, p.Offset, exp)
				break
			}
			exp++
		}
	}
	if n := len(res.Publications); n > 0 && res.Publications[n-1].Offset > wc.offset {
		wc.offset = res.Publications[n-1].Offset
	}
	if wc.positioned {
		w.lastPos[ch] = StreamPosition{Offset: wc.offset, Epoch: wc.epoch}
	}
}

// onReply applies a frame to the wire model. Called with w.mu held.
func (fc *fzConn) onReply(r *protocol.Reply) {
	w := fc.w
	if r.Id == 0 && r.Push == nil {
		return // Ping.
	}
	m, ok := fc.meta[r.Id]
	if !fc.connectReply && (!ok || m.kind != "connect") {
		w.violate("client %d wire: frame before connect reply: %v", fc.idx, r)
	}
	if r.Id != 0 {
		if !ok {
			return
		}
		delete(fc.meta, r.Id)
		wc := fc.wireCh(m.ch)
		desc := "ok"
		if r.Error != nil {
			desc = fmt.Sprintf("error %d", r.Error.Code)
		}
		w.trace = append(w.trace, fmt.Sprintf("  [c%d wire reply %s %s id=%d %s]", fc.idx, m.kind, m.ch, r.Id, desc))
		switch m.kind {
		case "connect":
			if r.Error != nil || r.Connect == nil {
				return
			}
			fc.connectReply = true
			chans := make([]string, 0, len(r.Connect.Subs))
			for ch := range r.Connect.Subs {
				chans = append(chans, ch)
			}
			sort.Strings(chans)
			for _, ch := range chans {
				w.trace = append(w.trace, fmt.Sprintf("  [c%d wire connect sub %s]", fc.idx, ch))
				fc.subscribed(ch, fc.wireCh(ch), r.Connect.Subs[ch], m.subs[ch])
			}
		case "unsub":
			wc.unsubSent--
			// A server-side subscription made while the client unsubscribed is a
			// separate one for the SDK, the reply does not end it.
			if r.Error == nil && !wc.serverSide {
				if wc.state != wUnsub {
					wc.unsubByReply = true
				}
				wc.state = wUnsub
				wc.mapNext = nil
			}
		case "sub", "poll":
			if r.Error != nil {
				if r.Error.Code != ErrorAlreadySubscribed.Code && r.Id > wc.since {
					wc.state = wUnsub
				}
				return
			}
			if wc.state == wSub && !wc.ambiguous {
				w.violate("client %d wire: subscribe reply on %s while already subscribed", fc.idx, m.ch)
			}
			wc.since = r.Id
			if m.kind == "poll" {
				fc.subscribed(m.ch, wc, nil, nil)
			} else {
				fc.subscribed(m.ch, wc, r.Subscribe, m.req)
			}
		case "mapsub":
			fc.onMapReply(r, m, wc)
		}
		return
	}
	p := r.Push
	ch := p.Channel
	wc := fc.wireCh(ch)
	switch {
	case p.Pub != nil:
		if ch == fzP0 {
			return
		}
		w.trace = append(w.trace, fmt.Sprintf("  [c%d wire publication %s offset=%d]", fc.idx, ch, p.Pub.Offset))
		if wc.state != wSub {
			w.violate("client %d wire: publication (offset %d) on %s while %s", fc.idx, p.Pub.Offset, ch, fzState(wc.state))
			return
		}
		if wc.positioned {
			if p.Pub.Offset != wc.offset+1 {
				w.violate("client %d wire: publication on %s offset %d, want %d", fc.idx, ch, p.Pub.Offset, wc.offset+1)
			}
			wc.offset = p.Pub.Offset
			w.lastPos[ch] = StreamPosition{Offset: wc.offset, Epoch: wc.epoch}
		}
	case p.Join != nil, p.Leave != nil:
		w.trace = append(w.trace, fmt.Sprintf("  [c%d wire join/leave push %s]", fc.idx, ch))
		if wc.state != wSub {
			w.violate("client %d wire: join/leave push on %s while %s", fc.idx, ch, fzState(wc.state))
		}
	case p.Unsubscribe != nil:
		w.trace = append(w.trace, fmt.Sprintf("  [c%d wire unsubscribe push %s code=%d]", fc.idx, ch, p.Unsubscribe.Code))
		if wc.state == wUnsub && !wc.unsubByReply {
			w.violate("client %d wire: unsubscribe push on %s while unsubscribed", fc.idx, ch)
		}
		wc.unsubByReply = false
		wc.state = wUnsub
		wc.serverSide = false
		wc.ambiguous = false
		wc.mapNext = nil
	case p.Subscribe != nil:
		w.trace = append(w.trace, fmt.Sprintf("  [c%d wire subscribe push %s]", fc.idx, ch))
		if wc.state == wSub && wc.unsubSent == 0 && !wc.ambiguous {
			w.violate("client %d wire: subscribe push on %s while subscribed", fc.idx, ch)
		}
		fc.subscribed(ch, wc, &protocol.SubscribeResult{
			Recoverable: p.Subscribe.Recoverable, Positioned: p.Subscribe.Positioned,
			Offset: p.Subscribe.Offset, Epoch: p.Subscribe.Epoch,
		}, nil)
		wc.since = fc.nextID
		// Subscribed by a subscribe push while a client unsubscribe was in flight:
		// the unsubscribe reply is for the previous subscription. Whether it
		// removes this subscription or the previous one can't be told from the
		// wire.
		wc.serverSide = wc.unsubSent > 0
		wc.ambiguous = wc.serverSide
	case p.Refresh != nil, p.Message != nil, p.Disconnect != nil:
	}
}

func (fc *fzConn) onMapReply(r *protocol.Reply, m fzMeta, wc *fzWire) {
	w := fc.w
	if r.Error != nil {
		if r.Error.Code != ErrorAlreadySubscribed.Code && r.Id > wc.since {
			if wc.state == wSub {
				w.violate("client %d wire: map subscribe error %d on %s while subscribed", fc.idx, r.Error.Code, m.ch)
			}
			wc.state = wUnsub
			wc.mapNext = nil
		}
		return
	}
	res := r.Subscribe
	if res.Phase == MapPhaseLive {
		if wc.state == wSub && !wc.ambiguous {
			w.violate("client %d wire: map live reply on %s while already subscribed", fc.idx, m.ch)
		}
		wc.state = wSub
		wc.unsubByReply = false
		wc.serverSide = false
		wc.ambiguous = false
		wc.since = r.Id
		wc.mapNext = nil
		wc.offset, wc.epoch = res.Offset, res.Epoch
		wc.positioned = res.Epoch != ""
		for _, p := range res.Publications {
			if p.Offset > wc.offset {
				wc.offset = p.Offset
			}
		}
		if wc.positioned {
			w.lastPos[m.ch] = StreamPosition{Offset: wc.offset, Epoch: wc.epoch}
		}
		return
	}
	if wc.state == wSub {
		w.violate("client %d wire: map page reply on %s while subscribed", fc.idx, m.ch)
	}
	wc.state = wMapLoading
	next := &protocol.SubscribeRequest{
		Channel: m.ch, Type: m.req.Type, Phase: res.Phase, Limit: m.req.Limit,
		Cursor: res.Cursor, Offset: res.Offset, Epoch: res.Epoch,
	}
	if res.Phase == MapPhaseStream {
		next.Recover = m.req.Recover
	}
	wc.mapNext = next
}

func newFzWorld(tb testing.TB, label string, conf fzConfig) *fzWorld {
	w := &fzWorld{
		tb:       tb,
		label:    label,
		holdMask: conf.holdMask,
		byID:     map[string]*fzConn{},
		joinLog:  map[string]string{},
		lastPos:  map[string]StreamPosition{},
	}
	node, err := New(Config{
		LogLevel:            LogLevelInfo,
		LogHandler:          w.onLog,
		UserConnectionLimit: conf.userConnLimit,
		ClientChannelLimit:  conf.channelLimit,
		ClientQueueMaxSize:  conf.clientQueueSize,
		Map: MapConfig{
			GetMapChannelOptions: func(ch string) MapChannelOptions {
				if strings.HasPrefix(ch, "cp:") {
					return MapChannelOptions{Mode: MapModeEphemeral, KeyTTL: time.Minute, MinPageSize: 1}
				}
				return MapChannelOptions{Mode: MapModeRecoverable, KeyTTL: time.Minute, MinPageSize: 1, SubscribeCatchUpTimeout: time.Minute}
			},
		},
		SharedPoll: SharedPollConfig{
			GetSharedPollChannelOptions: func(channel string) (SharedPollChannelOptions, bool) {
				return SharedPollChannelOptions{RefreshInterval: time.Second, RefreshBatchSize: 100, MaxKeysPerConnection: 100}, strings.HasPrefix(channel, "poll:")
			},
		},
	})
	if err != nil {
		tb.Fatal(err)
	}
	mb, err := NewMemoryBroker(node, MemoryBrokerConfig{})
	if err != nil {
		tb.Fatal(err)
	}
	node.SetBroker(&fzBroker{MemoryBroker: mb, w: w})
	pm, err := NewMemoryPresenceManager(node, MemoryPresenceManagerConfig{})
	if err != nil {
		tb.Fatal(err)
	}
	w.presence = pm
	node.SetPresenceManager(&fzPresence{MemoryPresenceManager: pm, w: w})
	mapBroker, err := NewMemoryMapBroker(node, MemoryMapBrokerConfig{})
	if err != nil {
		tb.Fatal(err)
	}
	w.mapBroker = mapBroker
	node.SetMapBroker(&fzMapBroker{MemoryMapBroker: mapBroker, w: w})
	node.OnSharedPoll(func(context.Context, SharedPollEvent) (SharedPollResult, error) {
		return SharedPollResult{}, nil
	})
	if conf.connecting {
		node.OnConnecting(w.onConnecting)
	}
	node.OnConnect(func(client *Client) {
		w.mu.Lock()
		fc := w.byID[client.ID()]
		w.mu.Unlock()
		fc.onConnect()
	})
	if err := node.Run(); err != nil {
		tb.Fatal(err)
	}
	w.node = node
	// Some history and map state to recover from.
	for _, ch := range fzStreams {
		for i := 0; i < 3; i++ {
			_, _ = node.Publish(ch, []byte(`{}`), WithHistory(100, time.Minute))
		}
	}
	for _, ch := range fzMaps {
		for i := 0; i < 3; i++ {
			_, _ = node.MapPublish(context.Background(), ch, "k"+strconv.Itoa(i), MapPublishOptions{Data: []byte(`{}`)})
		}
	}
	return w
}

// newConn makes a connection and starts its command queue.
func (w *fzWorld) newConn() *fzConn {
	w.mu.Lock()
	fc := &fzConn{
		w:            w,
		idx:          len(w.conns),
		cmdCh:        make(chan fzCmd, 256),
		allowed:      map[string]int{},
		released:     map[string]int{},
		completed:    map[string]int{},
		lateAllowed:  map[string]int{},
		lateReleases: map[string]int{},
		rels:         map[string][]fzRel{},
		wire:         map[string]*fzWire{},
		meta:         map[uint32]fzMeta{},
	}
	w.conns = append(w.conns, fc)
	w.mu.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	fc.tr = &fzTransport{fc: fc, cancel: cancel}
	c, _, err := NewClient(SetCredentials(ctx, &Credentials{UserID: "u"}), w.node, fc.tr)
	if err != nil {
		w.tb.Fatal(err)
	}
	fc.c = c
	w.mu.Lock()
	w.byID[c.ID()] = fc
	w.mu.Unlock()
	go fc.runQueue()
	return fc
}

// connect sends a connect command, with recovery of subs from the positions
// the client knows.
func (fc *fzConn) connect(subs []string) {
	req := &protocol.ConnectRequest{}
	m := fzMeta{kind: "connect"}
	if len(subs) > 0 {
		req.Subs = map[string]*protocol.SubscribeRequest{}
		m.subs = map[string]*protocol.SubscribeRequest{}
		fc.w.mu.Lock()
		for _, ch := range subs {
			if pos, ok := fc.w.lastPos[ch]; ok {
				r := &protocol.SubscribeRequest{Recover: true, Offset: pos.Offset, Epoch: pos.Epoch}
				req.Subs[ch] = r
				m.subs[ch] = r
			}
		}
		fc.w.mu.Unlock()
	}
	fc.send(&protocol.Command{Connect: req}, fzDecision{}, m)
}

func (w *fzWorld) conn(a byte) *fzConn {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.conns[int(a)%len(w.conns)]
}

// spawn runs f and waits for it up to the settle time.
func (w *fzWorld) spawn(f func()) {
	done := make(chan struct{})
	w.async(func() {
		defer close(done)
		f()
	})
	select {
	case <-done:
		time.Sleep(fzSettle / 4)
	case <-time.After(fzSettle):
	}
}

// settleQueue waits until the connection's commands are handled or the settle
// time passed.
func (w *fzWorld) settleQueue(fc *fzConn) {
	deadline := time.Now().Add(fzSettle)
	for fc.queued.Load() > 0 && time.Now().Before(deadline) {
		time.Sleep(100 * time.Microsecond)
	}
	time.Sleep(fzSettle / 4)
}

// answerPending answers pending SubscribeHandler call a with kind b.
func (w *fzWorld) answerPending(a, b byte) {
	w.mu.Lock()
	if len(w.pending) == 0 {
		w.mu.Unlock()
		w.tracef("op answer: nothing pending")
		return
	}
	i := int(a) % len(w.pending)
	p := w.pending[i]
	w.pending = slices.Delete(w.pending, i, i+1)
	w.mu.Unlock()
	kind := int(b % 5)
	w.tracef("op answer c%d %s kind=%d", p.fc.idx, p.e.Channel, kind)
	w.spawn(func() { p.fc.answer(p.e, p.cb, kind, p.opts) })
}

// clientStacks returns the stacks of the world's goroutines in the code of a
// client (with all: or of its writer or command queue).
func (w *fzWorld) clientStacks(all bool) []string {
	var buf bytes.Buffer
	_ = pprof.Lookup("goroutine").WriteTo(&buf, 1)
	label := `"fzworld":"` + w.label + `"`
	var out []string
	for _, g := range strings.Split(buf.String(), "\n\n") {
		if !strings.Contains(g, label) || strings.Contains(g, "runtime/pprof.writeGoroutine") {
			continue
		}
		if strings.Contains(g, "centrifuge.(*Client)") ||
			all && (strings.Contains(g, "centrifuge.(*writer)") || strings.Contains(g, "centrifuge.(*fzConn).runQueue")) {
			out = append(out, g)
		}
	}
	return out
}

// quiet reports whether nothing is in progress: no operations, commands, holds,
// pending SubscribeHandler calls or goroutines in client code.
func (w *fzWorld) quiet() bool {
	if w.running.Load() != 0 {
		return false
	}
	w.mu.Lock()
	conns := slices.Clone(w.conns)
	idle := len(w.holds) == 0 && len(w.pending) == 0
	w.mu.Unlock()
	if !idle {
		return false
	}
	for _, fc := range conns {
		if fc.queued.Load() != 0 {
			return false
		}
	}
	return len(w.clientStacks(false)) == 0
}

// flushWire waits until what connections queued for writing is on the wire: a
// marker written after it arrives.
func (w *fzWorld) flushWire() {
	w.mu.Lock()
	var conns []*fzConn
	for _, fc := range w.conns {
		if fc.connectReply {
			conns = append(conns, fc)
		}
	}
	w.mu.Unlock()
	for _, fc := range conns {
		if fc.closed() {
			continue
		}
		marker := []byte(`{}`) // Decodes as a ping.
		seen := make(chan struct{})
		fc.tr.set(func(t *fzTransport) { t.marker, t.markerSeen = marker, seen })
		if fc.c.writeEncodedPushData(marker, "", "", protocol.FrameTypeServerPing, ChannelBatchConfig{}) != nil {
			continue
		}
		deadline := time.Now().Add(time.Second)
	wait:
		for time.Now().Before(deadline) && !fc.closed() {
			select {
			case <-seen:
				break wait
			case <-time.After(time.Millisecond):
			}
		}
	}
}

// drain releases everything and answers what is pending (with the kinds in rest,
// then allowing) until all is quiet.
func (w *fzWorld) drain(rest []byte) bool {
	w.releaseAll()
	deadline := time.Now().Add(15 * time.Second)
	stable := 0
	for time.Now().Before(deadline) {
		w.mu.Lock()
		pend := w.pending
		w.pending = nil
		w.mu.Unlock()
		for _, p := range pend {
			kind := fzAllow
			if len(rest) > 0 {
				kind = int(rest[0] % 3)
				rest = rest[1:]
			}
			w.async(func() { p.fc.answer(p.e, p.cb, kind, p.opts) })
		}
		if w.quiet() {
			stable++
			if stable > 2 {
				return true
			}
			if stable == 1 {
				w.flushWire()
			}
		} else {
			stable = 0
		}
		time.Sleep(time.Millisecond)
	}
	return false
}

func (w *fzWorld) presenceOf(ch string) map[string]bool {
	res := map[string]bool{}
	p, _ := w.presence.Presence(ch)
	for k := range p {
		res[k] = true
	}
	return res
}

func (w *fzWorld) mapPresenceOf(ch string) map[string]bool {
	res := map[string]bool{}
	st, err := w.mapBroker.ReadState(context.Background(), "cp:"+ch, MapReadStateOptions{Limit: -1})
	if err != nil {
		return res
	}
	for _, p := range st.Publications {
		res[p.Key] = true
	}
	return res
}

// checkConsistent compares wire, client, hub and broker state at a quiet point.
func (w *fzWorld) checkConsistent() {
	w.mu.Lock()
	conns := slices.Clone(w.conns)
	w.mu.Unlock()
	nSub := map[string]int{}
	for _, ch := range fzAll {
		pres := w.presenceOf(ch)
		mpres := w.mapPresenceOf(ch)
		for _, fc := range conns {
			c := fc.c
			c.mu.RLock()
			chCtx, ok := c.channels[ch]
			closed := c.status == statusClosed
			c.mu.RUnlock()
			serverSub := ok && channelHasFlag(chCtx.flags, flagSubscribed)
			if serverSub {
				nSub[ch]++
			}
			w.mu.Lock()
			wc := fc.wireCh(ch)
			if !closed && fc.connectReply && !wc.ambiguous && serverSub != (wc.state == wSub) {
				w.violate("client %d: %s wire %s but server subscribed=%v", fc.idx, ch, fzState(wc.state), serverSub)
			}
			wantPres := serverSub && channelHasFlag(chCtx.flags, flagEmitPresence)
			if pres[c.uid] != wantPres && ch != fzP0 {
				w.violate("client %d: %s presence=%v want %v", fc.idx, ch, pres[c.uid], wantPres)
			}
			wantMPres := serverSub && chCtx.mapClientPresenceChannel != ""
			if mpres[c.uid] != wantMPres {
				w.violate("client %d: %s map client presence=%v want %v", fc.idx, ch, mpres[c.uid], wantMPres)
			}
			joined := w.joinLog[ch+"|"+c.uid] == "join"
			wantJoined := serverSub && channelHasFlag(chCtx.flags, flagEmitJoinLeave)
			if joined != wantJoined {
				w.violate("client %d: %s joined=%v want %v", fc.idx, ch, joined, wantJoined)
			}
			w.mu.Unlock()
		}
	}
	for _, ch := range fzAll {
		if ch == fzP0 {
			continue
		}
		if n := w.node.hub.NumSubscribers(ch); n != nSub[ch] {
			w.violateU("hub: %s has %d subscribers, %d clients subscribed", ch, n, nSub[ch])
		}
	}
	live := 0
	for _, fc := range conns {
		fc.c.mu.RLock()
		if fc.c.authenticated && fc.c.status != statusClosed {
			live++
		}
		fc.c.mu.RUnlock()
	}
	if n := len(w.node.hub.UserConnections("u")); n != live {
		w.violateU("hub: %d connections of the user, %d live clients", n, live)
	}
}

func (w *fzWorld) checkHandlers(noTimeouts bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, fc := range w.conns {
		want := 0
		if fc.onConnects > 0 {
			want = 1
		}
		if fc.disconnects != want {
			w.violate("client %d: %d DisconnectHandler calls, %d OnConnect calls", fc.idx, fc.disconnects, fc.onConnects)
		}
		var chans []string
		for ch := range fc.allowed {
			chans = append(chans, ch)
		}
		for ch := range fc.released {
			if _, ok := fc.allowed[ch]; !ok {
				chans = append(chans, ch)
			}
		}
		sort.Strings(chans)
		for _, ch := range chans {
			if fc.allowed[ch] != fc.released[ch] {
				w.violate("client %d: %s allowed %d attempts, UnsubscribeHandler called %d times", fc.idx, ch, fc.allowed[ch], fc.released[ch])
			}
			if noTimeouts && fc.lateReleases[ch] > fc.lateAllowed[ch] {
				w.violate("client %d: %s %d UnsubscribeHandler calls after DisconnectHandler, %d attempts allowed after close", fc.idx, ch, fc.lateReleases[ch], fc.lateAllowed[ch])
			}
		}
		if !noTimeouts {
			continue
		}
		for _, cand := range fc.cands {
			rels := fc.rels[cand.ch]
			for k := cand.from; k < cand.to && k < len(rels); k++ {
				r := rels[k]
				w.violate("client %d: SubscribeHandler on %s before UnsubscribeHandler #%d of previous attempt (subscribed=%v code=%d)", fc.idx, cand.ch, k+1, r.subscribed, r.code)
			}
		}
	}
}

// checkFinal checks that all clients are gone without leaving anything behind.
func (w *fzWorld) checkFinal() {
	w.mu.Lock()
	noTimeouts := len(w.timeouts) == 0
	conns := slices.Clone(w.conns)
	w.mu.Unlock()
	w.checkHandlers(noTimeouts)
	for _, ch := range fzAll {
		if p := w.presenceOf(ch); len(p) > 0 {
			w.violateU("final: presence left in %s: %v", ch, p)
		}
		if p := w.mapPresenceOf(ch); len(p) > 0 {
			w.violateU("final: map client presence left in %s: %v", ch, p)
		}
		if n := w.node.hub.NumSubscribers(ch); n != 0 {
			w.violateU("final: hub %s has %d subscribers", ch, n)
		}
	}
	if n := w.node.hub.NumClients(); n != 0 {
		w.violateU("final: hub has %d clients", n)
	}
	w.mu.Lock()
	for k, v := range w.joinLog {
		if v == "join" {
			w.violate("final: no leave after join for %s", k)
		}
	}
	w.mu.Unlock()
	for _, fc := range conns {
		c := fc.c
		var leaks []string
		c.mu.RLock()
		if c.status != statusClosed {
			leaks = append(leaks, "not closed")
		}
		if len(c.channels) != 0 {
			leaks = append(leaks, fmt.Sprintf("channels=%d", len(c.channels)))
		}
		if len(c.mapSubscribing) != 0 {
			leaks = append(leaks, fmt.Sprintf("mapSubscribing=%d", len(c.mapSubscribing)))
		}
		if len(c.mapPaginationLocks) != 0 {
			leaks = append(leaks, "mapPaginationLocks")
		}
		if c.pendingUnsubscribes != 0 {
			leaks = append(leaks, "pendingUnsubscribes")
		}
		if t := c.tracking; t != nil {
			if t.attemptEnds != 0 || len(t.channelAttemptEnds) != 0 {
				leaks = append(leaks, "tracking.attemptEnds")
			}
			if len(t.channelLeaves) != 0 {
				leaks = append(leaks, "tracking.channelLeaves")
			}
			if len(t.mapSubscribePending) != 0 {
				leaks = append(leaks, "tracking.mapSubscribePending")
			}
		}
		if c.timer != nil && c.timer.Stop() {
			leaks = append(leaks, "timer")
		}
		c.mu.RUnlock()
		if c.pubSubSync.Buffering() || c.pubSubSync.Held() != 0 {
			leaks = append(leaks, "recovery buffer")
		}
		if len(leaks) > 0 {
			w.violateU("final: client %d leaks %v", fc.idx, leaks)
		}
	}
}

// fzTarget is what a fuzz target adds to the shared harness.
type fzTarget struct {
	// config returns the world's configuration for an input.
	config func(data []byte) fzConfig
	// start makes the initial connections.
	start func(w *fzWorld, data []byte)
	step  func(w *fzWorld, op, a, b byte)
	// end runs after the operations, before the drain (optional).
	end func(w *fzWorld, data []byte)
	// alphabet names operations for TestFuzzLifecycleEnumerate.
	alphabet map[byte][3]byte
}

// setFzGlobals shortens the client timeouts and sets the test hooks, returning a
// function which restores them.
func setFzGlobals() func() {
	prevTimeouts := []time.Duration{subscribeInProgressTimeout, pendingUnsubscribesSubscribeTimeout, closeSubscribesTimeout, pendingUnsubscribesDisconnectTimeout}
	subscribeInProgressTimeout = time.Second
	pendingUnsubscribesSubscribeTimeout = time.Second
	closeSubscribesTimeout = 2 * time.Second
	pendingUnsubscribesDisconnectTimeout = 2 * time.Second
	prevInTest := isInTest.Load()
	isInTest.Store(true)
	prevDelay := testSyncPointDelay.Load()
	testSyncPointDelay.Store(1)
	atSync := func(channel string) {
		if w := fzCur.Load(); w != nil {
			w.hold(fzHoldSyncPoint, "syncPoint "+channel)
		}
	}
	prevAtSync := testAtSyncPoint.Swap(&atSync)
	afterCommit := func(channel string) {
		if w := fzCur.Load(); w != nil {
			w.hold(fzHoldMapCommit, "mapCommit "+channel)
		}
	}
	prevAfterCommit := testAfterMapCommit.Swap(&afterCommit)
	return func() {
		subscribeInProgressTimeout = prevTimeouts[0]
		pendingUnsubscribesSubscribeTimeout = prevTimeouts[1]
		closeSubscribesTimeout = prevTimeouts[2]
		pendingUnsubscribesDisconnectTimeout = prevTimeouts[3]
		isInTest.Store(prevInTest)
		testSyncPointDelay.Store(prevDelay)
		testAtSyncPoint.Store(prevAtSync)
		testAfterMapCommit.Store(prevAfterCommit)
	}
}

var fzWorldSeq atomic.Int64

// fzRun runs one input and returns the violations and the trace.
func fzRun(tb testing.TB, tgt *fzTarget, data []byte) (violations []string, trace []string) {
	if len(data) < 2 {
		return nil, nil
	}
	defer setFzGlobals()()
	label := strconv.FormatInt(fzWorldSeq.Add(1), 10)
	pprof.Do(context.Background(), pprof.Labels("fzworld", label), func(context.Context) {
		violations, trace = fzRunWorld(tb, tgt, label, data)
	})
	return violations, trace
}

func fzRunWorld(tb testing.TB, tgt *fzTarget, label string, data []byte) ([]string, []string) {
	w := newFzWorld(tb, label, tgt.config(data))
	fzCur.Store(w)
	defer fzCur.Store(nil)
	w.tracef("input %x", data)
	tgt.start(w, data)
	ops := data[2:]
	const maxOps = 40
	i := 0
	for n := 0; n < maxOps && i+2 < len(ops); n++ {
		tgt.step(w, ops[i], ops[i+1], ops[i+2])
		i += 3
	}
	if tgt.end != nil {
		tgt.end(w, data)
	}
	rest := ops[min(i, len(ops)):]
	w.tracef("-- drain")
	if !w.drain(rest) {
		w.violateU("stuck: not quiet after drain:\n%s", strings.Join(w.clientStacks(false), "\n\n"))
	}
	w.checkConsistent()
	w.tracef("-- close all")
	w.mu.Lock()
	conns := slices.Clone(w.conns)
	w.mu.Unlock()
	for _, fc := range conns {
		_ = fc.c.close(DisconnectForceNoReconnect)
	}
	if !w.drain(nil) {
		w.violateU("stuck: not quiet after close:\n%s", strings.Join(w.clientStacks(false), "\n\n"))
	}
	w.checkFinal()
	for _, fc := range conns {
		close(fc.cmdCh)
	}
	_ = w.node.Shutdown(context.Background())
	var gs []string
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if gs = w.clientStacks(true); len(gs) == 0 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if len(gs) > 0 {
		w.violateU("goroutine leak:\n%s", strings.Join(gs, "\n\n"))
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.timeouts) > 0 {
		w.trace = append(w.trace, "timeouts: "+strings.Join(w.timeouts, "; "))
	}
	if len(w.errLogs) > 0 {
		w.trace = append(w.trace, "error logs: "+strings.Join(w.errLogs, "; "))
	}
	return w.violations, w.trace
}

func fzFuzz(f *testing.F, tgt *fzTarget) {
	f.Fuzz(func(t *testing.T, data []byte) {
		v, trace := fzRun(t, tgt, data)
		if len(v) > 0 {
			t.Fatalf("violations:\n  %s\ninput: %x\ntrace:\n%s", strings.Join(v, "\n  "), data, strings.Join(trace, "\n"))
		}
	})
}

func fzTargetFromEnv(t *testing.T) *fzTarget {
	switch os.Getenv("FZ_TARGET") {
	case "", "subscribe":
		return fzSubscribeTarget
	case "connect":
		return fzConnectTarget
	}
	t.Fatalf("unknown FZ_TARGET %q", os.Getenv("FZ_TARGET"))
	return nil
}

// TestFuzzLifecycleReplay replays FZ_INPUT, see the top of the file.
func TestFuzzLifecycleReplay(t *testing.T) {
	in := os.Getenv("FZ_INPUT")
	if in == "" {
		t.Skip("FZ_INPUT not set")
	}
	tgt := fzTargetFromEnv(t)
	var data []byte
	if _, err := fmt.Sscanf(in, "%x", &data); err != nil {
		t.Fatal(err)
	}
	n, _ := strconv.Atoi(os.Getenv("FZ_REPEAT"))
	n = max(n, 1)
	fails := 0
	var first []string
	for i := 0; i < n; i++ {
		v, trace := fzRun(t, tgt, data)
		if os.Getenv("FZ_TRACE") != "" {
			t.Logf("violations: %v\ntrace:\n%s", v, strings.Join(trace, "\n"))
		}
		if len(v) > 0 {
			fails++
			if first == nil {
				first = append(slices.Clone(v), "--- trace:")
				first = append(first, trace...)
			}
		}
	}
	if fails > 0 {
		t.Fatalf("%d/%d runs failed; first:\n%s", fails, n, strings.Join(first, "\n"))
	}
}

// TestFuzzLifecycleEnumerate runs all sequences of FZ_ALPHA, see the top of the
// file.
func TestFuzzLifecycleEnumerate(t *testing.T) {
	alpha := os.Getenv("FZ_ALPHA")
	if alpha == "" {
		t.Skip("FZ_ALPHA not set")
	}
	tgt := fzTargetFromEnv(t)
	depth, _ := strconv.Atoi(os.Getenv("FZ_DEPTH"))
	shard, _ := strconv.Atoi(os.Getenv("FZ_SHARD"))
	shards, _ := strconv.Atoi(os.Getenv("FZ_SHARDS"))
	shards = max(shards, 1)
	mask, _ := strconv.ParseUint(os.Getenv("FZ_MASK"), 0, 8)
	flags, _ := strconv.ParseUint(os.Getenv("FZ_FLAGS"), 0, 8)
	out, err := os.OpenFile(os.Getenv("FZ_OUT"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = out.Close() }()
	for _, l := range alpha {
		if _, ok := tgt.alphabet[byte(l)]; !ok {
			t.Fatalf("letter %q not in the alphabet", l)
		}
	}
	total := 1
	for i := 0; i < depth; i++ {
		total *= len(alpha)
	}
	ran, failed := 0, 0
	seq := make([]byte, depth)
	for n := shard; n < total; n += shards {
		x := n
		for i := 0; i < depth; i++ {
			seq[i] = alpha[x%len(alpha)]
			x /= len(alpha)
		}
		data := []byte{byte(mask), byte(flags)}
		for _, l := range seq {
			op := tgt.alphabet[l]
			data = append(data, op[0], op[1], op[2])
		}
		v, trace := fzRun(t, tgt, data)
		ran++
		if len(v) > 0 {
			failed++
			_, _ = fmt.Fprintf(out, "=== %s input=%x\n  %s\n%s\n\n", seq, data, strings.Join(v, "\n  "), strings.Join(trace, "\n"))
		}
		if ran%2000 == 0 {
			t.Logf("shard %d: %d/%d ran, %d failed", shard, ran, total/shards, failed)
		}
	}
	t.Logf("shard %d: ran %d, failed %d", shard, ran, failed)
}

// Subscribe lifecycle: input byte 0 is the hold mask, bit 0 of byte 1 adds a
// second connection, then 3 bytes per operation (fzSubscribeStep).

var fzSubscribeTarget = &fzTarget{
	config: func(data []byte) fzConfig {
		return fzConfig{holdMask: uint16(data[0] & 0x7f)}
	},
	start: func(w *fzWorld, data []byte) {
		n := 1 + int(data[1]&1)
		for i := 0; i < n; i++ {
			fc := w.newConn()
			fc.connect(nil)
		}
		// Connected before the operations start.
		for _, fc := range w.conns {
			for fc.queued.Load() > 0 {
				time.Sleep(100 * time.Microsecond)
			}
		}
	},
	step: fzSubscribeStep,
	alphabet: map[byte][3]byte{
		'A': {0, 0, 2 | 15<<2}, // client subscribe s0, SubscribeHandler pending, recovery+presence+join/leave+map presence
		'B': {0, 0, 0 | 15<<2}, // client subscribe s0, allowed synchronously
		'C': {8, 0, 0},         // server Client.Unsubscribe s0
		'D': {5, 0, 0},         // client unsubscribe s0
		'E': {11, 0, 0},        // close(ForceNoReconnect)
		'F': {12, 0, 0},        // answer pending: allow
		'G': {12, 0, 2},        // answer pending: allow with past ExpireAt (refused)
		'H': {14, 0, 0},        // release the oldest held call
		'I': {7, 0, 14},        // server Client.Subscribe s0 with presence, join/leave, map presence
		'J': {16, 0, 0},        // publish to s0
		'K': {17, 0, 0},        // presence tick
		'L': {12, 0, 3},        // answer pending: allow twice
		'M': {12, 0, 1},        // answer pending: deny
	},
}

const fzSubscribeOps = 20

func fzSubscribeStep(w *fzWorld, op, a, b byte) {
	fc := w.conn(a)
	switch op % fzSubscribeOps {
	case 0, 1: // client subscribe to a stream channel
		ch := fzStreams[(a>>1)&1]
		req := &protocol.SubscribeRequest{Channel: ch}
		w.mu.Lock()
		pos, ok := w.lastPos[ch]
		w.mu.Unlock()
		if (a>>2)&1 != 0 && ok {
			req.Recover = true
			req.Epoch = pos.Epoch
			back := uint64((a >> 3) & 3)
			if pos.Offset >= back {
				req.Offset = pos.Offset - back
			}
		}
		w.tracef("op c%d client subscribe %s recover=%v off=%d mode=%d opts=%05b", fc.idx, ch, req.Recover, req.Offset, b&3, b>>2)
		fc.send(&protocol.Command{Subscribe: req}, fzDecision{set: true, mode: b & 3, opts: b >> 2}, fzMeta{ch: ch, kind: "sub", req: req})
		w.settleQueue(fc)
	case 2: // client map subscribe
		ch := fzMaps[(a>>1)&1]
		req := &protocol.SubscribeRequest{Channel: ch, Type: int32(SubscriptionTypeMap), Phase: MapPhaseState, Limit: 1 + int32((a>>5)&3)}
		w.mu.Lock()
		pos, ok := w.lastPos[ch]
		w.mu.Unlock()
		if (a>>2)&1 != 0 && ok {
			req.Recover = true
			req.Epoch = pos.Epoch
			req.Offset = pos.Offset
			req.Phase = MapPhaseLive
			if (a>>3)&1 != 0 {
				req.Phase = MapPhaseStream
				req.Limit = 1
				if req.Offset > 1 {
					req.Offset--
				}
			}
		}
		w.tracef("op c%d map subscribe %s phase=%d recover=%v mode=%d opts=%05b", fc.idx, ch, req.Phase, req.Recover, b&3, b>>2)
		fc.send(&protocol.Command{Subscribe: req}, fzDecision{set: true, mode: b & 3, opts: b >> 2}, fzMeta{ch: ch, kind: "mapsub", req: req})
		w.settleQueue(fc)
	case 3: // map continuation
		ch := fzMaps[(a>>1)&1]
		w.mu.Lock()
		wc := fc.wireCh(ch)
		next := wc.mapNext
		wc.mapNext = nil
		w.mu.Unlock()
		if next == nil {
			w.tracef("op c%d map continue %s: nothing", fc.idx, ch)
			return
		}
		w.tracef("op c%d map continue %s phase=%d cursor=%q off=%d", fc.idx, ch, next.Phase, next.Cursor, next.Offset)
		fc.send(&protocol.Command{Subscribe: next}, fzDecision{}, fzMeta{ch: ch, kind: "mapsub", req: next})
		w.settleQueue(fc)
	case 4: // shared poll subscribe
		req := &protocol.SubscribeRequest{Channel: fzP0, Type: int32(SubscriptionTypeSharedPoll)}
		w.tracef("op c%d poll subscribe mode=%d opts=%05b", fc.idx, b&3, b>>2)
		fc.send(&protocol.Command{Subscribe: req}, fzDecision{set: true, mode: b & 3, opts: b >> 2}, fzMeta{ch: fzP0, kind: "poll", req: req})
		w.settleQueue(fc)
	case 5, 6: // client unsubscribe
		ch := fzAll[int(a>>1)%len(fzAll)]
		w.tracef("op c%d client unsubscribe %s", fc.idx, ch)
		fc.send(&protocol.Command{Unsubscribe: &protocol.UnsubscribeRequest{Channel: ch}}, fzDecision{}, fzMeta{ch: ch, kind: "unsub"})
		w.settleQueue(fc)
	case 7: // server-side Client.Subscribe
		w.serverSubscribe(fc, fzStreams[(a>>1)&1], b)
	case 8, 9: // server-side Client.Unsubscribe
		ch := fzAll[int(a>>1)%len(fzAll)]
		w.tracef("op c%d server Client.Unsubscribe %s", fc.idx, ch)
		w.spawn(func() { fc.c.Unsubscribe(ch) })
	case 10: // Node.Unsubscribe
		ch := fzAll[int(a>>1)%len(fzAll)]
		w.tracef("op Node.Unsubscribe u %s", ch)
		w.spawn(func() { _ = w.node.Unsubscribe("u", ch) })
	case 11: // disconnects
		switch b & 3 {
		case 0:
			w.tracef("op c%d close(ForceNoReconnect)", fc.idx)
			w.spawn(func() { _ = fc.c.close(DisconnectForceNoReconnect) })
		case 1:
			w.tracef("op Node.Disconnect u")
			w.spawn(func() { _ = w.node.Disconnect("u") })
		case 2:
			w.tracef("op c%d Client.Disconnect()", fc.idx)
			w.spawn(func() { fc.c.Disconnect() })
		case 3:
			w.tracef("op c%d close(ConnectionClosed)", fc.idx)
			w.spawn(func() { _ = fc.c.close(DisconnectConnectionClosed) })
		}
	case 12, 13: // answer a pending SubscribeHandler
		w.answerPending(a, b)
	case 14, 15: // release a held call
		w.tracef("op release %s", w.releaseHold(int(a)))
		time.Sleep(fzSettle)
	case 16:
		w.publish(a, b)
	case 17:
		w.tracef("op c%d presence tick", fc.idx)
		w.spawn(func() { fc.c.updatePresence() })
	case 18: // expire map catch-ups (clock skew)
		w.tracef("op c%d expire map catch-ups", fc.idx)
		fc.c.mu.Lock()
		for _, st := range fc.c.mapSubscribing {
			st.startedAt = time.Now().Add(-time.Hour).UnixNano()
		}
		fc.c.mu.Unlock()
	case 19:
		w.tracef("op settle")
		time.Sleep(5 * fzSettle)
	}
}

func (w *fzWorld) serverSubscribe(fc *fzConn, ch string, b byte) {
	opts := []SubscribeOption{WithEmitPresence(b&2 != 0), WithEmitJoinLeave(b&4 != 0), WithPositioning(b&16 != 0)}
	if b&8 != 0 {
		opts = append(opts, func(o *SubscribeOptions) { o.MapClientPresenceChannel = "cp:" + ch })
	}
	w.tracef("op c%d server subscribe %s opts=%05b", fc.idx, ch, b)
	w.spawn(func() { _ = fc.c.Subscribe(ch, opts...) })
}

func (w *fzWorld) publish(a, b byte) {
	if a&1 == 0 {
		ch := fzStreams[(a>>1)&1]
		w.tracef("op publish %s", ch)
		w.spawn(func() { _, _ = w.node.Publish(ch, []byte(`{}`), WithHistory(100, time.Minute)) })
		return
	}
	ch := fzMaps[(a>>1)&1]
	w.tracef("op map publish %s", ch)
	w.spawn(func() {
		_, _ = w.node.MapPublish(context.Background(), ch, "k"+strconv.Itoa(int(b%4)), MapPublishOptions{Data: []byte(`{}`)})
	})
}

func FuzzClientSubscribeLifecycle(f *testing.F) {
	fzFuzz(f, fzSubscribeTarget)
}
