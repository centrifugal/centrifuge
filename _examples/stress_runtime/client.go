package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/centrifugal/centrifuge"
	cent "github.com/centrifugal/centrifuge-go"
)

var userSeq atomic.Uint64
var debugDisc bool

func newUser(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, userSeq.Add(1))
}

// newChannel builds a unique channel name with the behaviour prefix attached.
func newChannel(prefix, hint string) string {
	return prefix + hint + "-" + fmt.Sprint(userSeq.Add(1))
}

type conn struct {
	c        *cent.Client
	clientID atomic.Value // string, set on every Connected event.
	connects atomic.Int64
	// connectings counts Connecting events: the initial connect plus every
	// reconnect. This is the counter to watch for "did the connection drop?" —
	// the SDK only reports Disconnected for terminal (non-reconnecting) codes, so
	// a forced reconnect never shows up in disconnects.
	connectings atomic.Int64
	disconnects atomic.Int64
	lastErr     atomic.Value // string
	lastDiscode atomic.Int64 // code of the last Connecting or Disconnected event
}

func (c *conn) id() string {
	v, _ := c.clientID.Load().(string)
	return v
}

func (c *conn) errString() string {
	v, _ := c.lastErr.Load().(string)
	return v
}

// dialOptions tune how a stress client connects.
type dialOptions struct {
	protobuf bool
	// setup runs after handlers are attached but before Connect, so scenarios can
	// register client-level (server-side subscription) handlers without racing
	// the connection's reader goroutine.
	setup func(*cent.Client)
}

type dialOption func(*dialOptions)

func withProtobuf() dialOption                  { return func(o *dialOptions) { o.protobuf = true } }
func withSetup(f func(*cent.Client)) dialOption { return func(o *dialOptions) { o.setup = f } }

// dial connects a client as user and waits for the first Connected event.
func dial(wsURL, user string, opts ...dialOption) (*conn, error) {
	var do dialOptions
	for _, o := range opts {
		o(&do)
	}
	cfg := cent.Config{
		Token:             user,
		Name:              "stress",
		MinReconnectDelay: 100 * time.Millisecond,
		MaxReconnectDelay: 500 * time.Millisecond,
		// Generous transport timeouts: with many scenarios hammering one
		// in-process node in parallel, CPU contention can otherwise exceed the
		// tight defaults and cause spurious disconnects unrelated to correctness.
		ReadTimeout:        20 * time.Second,
		WriteTimeout:       15 * time.Second,
		HandshakeTimeout:   15 * time.Second,
		MaxServerPingDelay: 30 * time.Second,
	}
	if strings.HasPrefix(user, userRefresh) || strings.HasPrefix(user, userExpire) {
		u := user
		cfg.GetToken = func(cent.ConnectionTokenEvent) (string, error) { return u, nil }
	}
	if strings.HasPrefix(user, userPing) {
		// Tight bound so the client actively fails if the server stops pinging.
		cfg.MaxServerPingDelay = 3 * time.Second
	}
	var cl *cent.Client
	if do.protobuf {
		cl = cent.NewProtobufClient(wsURL, cfg)
	} else {
		cl = cent.NewJsonClient(wsURL, cfg)
	}
	co := &conn{c: cl}
	connected := make(chan struct{})
	var once sync.Once
	cl.OnConnected(func(e cent.ConnectedEvent) {
		co.clientID.Store(e.ClientID)
		co.connects.Add(1) // first is the initial connect; later ones are reconnects.
		once.Do(func() { close(connected) })
	})
	cl.OnConnecting(func(e cent.ConnectingEvent) {
		if co.connectings.Add(1) > 1 {
			// Not the initial connect — record why we are reconnecting.
			co.lastDiscode.Store(int64(e.Code))
		}
	})
	cl.OnDisconnected(func(e cent.DisconnectedEvent) {
		co.lastDiscode.Store(int64(e.Code))
		co.disconnects.Add(1)
	})
	cl.OnError(func(e cent.ErrorEvent) { co.lastErr.Store(e.Error.Error()) })
	if do.setup != nil {
		do.setup(cl)
	}
	if err := cl.Connect(); err != nil {
		return nil, err
	}
	select {
	case <-connected:
		return co, nil
	case <-time.After(10 * time.Second):
		cl.Close()
		return nil, fmt.Errorf("connect timeout (lastErr=%v)", co.lastErr.Load())
	}
}

func closeAll(conns []*conn) {
	for _, c := range conns {
		if c != nil {
			c.c.Close()
		}
	}
}

// subscribe creates a subscription, runs setup (which MUST register any
// OnPublication/OnJoin/OnLeave handlers — they have to be attached before
// Subscribe or the reader goroutine races the registration), subscribes, and
// waits for the Subscribed event.
func subscribe(cl *cent.Client, channel string, cfg cent.SubscriptionConfig, setup func(*cent.Subscription)) (*cent.Subscription, *cent.SubscribedEvent, error) {
	sub, err := cl.NewSubscription(channel, cfg)
	if err != nil {
		return nil, nil, err
	}
	subscribed := make(chan cent.SubscribedEvent, 1)
	errCh := make(chan error, 1)
	sub.OnSubscribed(func(e cent.SubscribedEvent) {
		select {
		case subscribed <- e:
		default:
		}
	})
	sub.OnError(func(e cent.SubscriptionErrorEvent) {
		select {
		case errCh <- e.Error:
		default:
		}
	})
	if setup != nil {
		setup(sub)
	}
	if err := sub.Subscribe(); err != nil {
		return nil, nil, err
	}
	select {
	case e := <-subscribed:
		return sub, &e, nil
	case err := <-errCh:
		return nil, nil, err
	case <-time.After(10 * time.Second):
		return nil, nil, fmt.Errorf("subscribe timeout on %s", channel)
	}
}

// forceReconnect kicks a connection off the server and waits until the client
// has actually noticed. Callers publish into the gap it opens, so it must not
// return while the client is still attached — hence the edge-triggered
// Connecting counter rather than a state poll that a fast reconnect can hide.
func forceReconnect(node *centrifuge.Node, co *conn, user string) error {
	before := co.connectings.Load()
	if err := node.Disconnect(user, centrifuge.WithCustomDisconnect(centrifuge.DisconnectForceReconnect)); err != nil {
		return fmt.Errorf("force disconnect: %w", err)
	}
	if !waitFor(10*time.Second, func() bool { return co.connectings.Load() > before }) {
		return fmt.Errorf("client never noticed the forced reconnect")
	}
	return nil
}

// newTimeoutContext is a tiny helper so scenario-free code (warm-up, checks)
// does not have to import context just for a deadline.
func newTimeoutContext(d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), d)
}

// newRand returns a deterministic generator derived from the suite seed, so a
// randomized scenario can be replayed with -seed.
func newRand(seed int64) *rand.Rand { return rand.New(rand.NewSource(seed)) }

// errorCode extracts a protocol error code from a client error, if any.
func errorCode(err error) (uint32, bool) {
	var e *cent.Error
	if errors.As(err, &e) {
		return e.Code, true
	}
	return 0, false
}

// ---------------------------------------------------------------------------
// Small polling helpers. Every wait in the suite is bounded so a hung scenario
// fails with a precise message instead of eating the suite deadline.
// ---------------------------------------------------------------------------

// waitFor polls cond until it returns true or the timeout expires.
func waitFor(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// waitCount polls until got() reaches want, returning the last observed value.
func waitCount(timeout time.Duration, want int64, got func() int64) (int64, bool) {
	var last int64
	ok := waitFor(timeout, func() bool {
		last = got()
		return last >= want
	})
	return last, ok
}

// recorder collects publication offsets/payloads from a subscription.
type recorder struct {
	mu      sync.Mutex
	offsets []uint64
	data    []string
}

func (r *recorder) add(offset uint64, data []byte) {
	r.mu.Lock()
	r.offsets = append(r.offsets, offset)
	r.data = append(r.data, string(data))
	r.mu.Unlock()
}

func (r *recorder) len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.offsets)
}

func (r *recorder) snapshot() ([]uint64, []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]uint64(nil), r.offsets...), append([]string(nil), r.data...)
}

// notificationRegistry collects node.Notify deliveries by op.
type notificationRegistry struct {
	mu   sync.Mutex
	seen map[string][]string
}

var notifications = &notificationRegistry{seen: map[string][]string{}}

func (n *notificationRegistry) record(op string, data []byte) {
	n.mu.Lock()
	n.seen[op] = append(n.seen[op], string(data))
	n.mu.Unlock()
}

func (n *notificationRegistry) get(op string) []string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return append([]string(nil), n.seen[op]...)
}
