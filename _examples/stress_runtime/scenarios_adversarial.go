package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/centrifugal/centrifuge"
	cent "github.com/centrifugal/centrifuge-go"
	"github.com/gorilla/websocket"
)

// ---------------------------------------------------------------------------
// Adversarial clients. These speak the wire protocol directly (centrifuge-go by
// construction never sends anything invalid) and run against the "strict" node
// whenever they need small limits, so the throughput scenarios keep their
// generous ones.
// ---------------------------------------------------------------------------

// expectation describes how the server must react to a frame.
type expectation int

const (
	expectClose expectation = iota // server closes the connection
	expectErr                      // server replies with a protocol error
	expectOK                       // server replies successfully
)

type badCase struct {
	name string
	// setup runs on a freshly connected raw connection (after a successful
	// connect command unless preConnect is set).
	preConnect bool
	setup      func(*rawWS) error
	frame      []byte
	want       expectation
	wantCode   uint32 // close code or error code; 0 means "don't check"
}

// malformedProtocol throws invalid, hostile and edge-case frames at the server
// and asserts every one produces the documented reaction — never a hang, never a
// panic — and that a healthy connection alongside them is unaffected.
func malformedProtocol(ctx context.Context, e *env) (string, error) {
	// A control connection kept alive for the whole barrage.
	control, err := dial(e.wsURL, newUser("malformed-control"))
	if err != nil {
		return fail("control dial: %v", err)
	}
	defer control.c.Close()
	controlCh := newChannel(chRecov, "malformed")
	var controlPubs atomic.Int64
	if _, _, err := subscribe(control.c, controlCh, cent.SubscriptionConfig{Positioned: true}, func(s *cent.Subscription) {
		s.OnPublication(func(cent.PublicationEvent) { controlPubs.Add(1) })
	}); err != nil {
		return fail("control subscribe: %v", err)
	}
	controlConn := control.connectings.Load()

	longChannel := chPlain + strings.Repeat("z", 300)
	subCh := newChannel(chPlain, "malformed")

	cases := []badCase{
		{
			name: "garbage before connect", preConnect: true,
			frame: []byte("this is not a protocol frame"),
			want:  expectClose, wantCode: 3501,
		},
		{
			name: "json array instead of command", preConnect: true,
			frame: []byte(`[1,2,3]`),
			want:  expectClose, wantCode: 3501,
		},
		{
			name: "command before connect", preConnect: true,
			frame: mustJSON(command{ID: 1, Subscribe: &subscribeCmd{Channel: subCh}}),
			want:  expectClose, wantCode: 3501,
		},
		{
			name:  "second connect on the same connection",
			frame: mustJSON(command{ID: 2, Connect: &connectCmd{Token: newUser("dup")}}),
			want:  expectClose, wantCode: 3501,
		},
		{
			name:  "unnecessary pong",
			frame: []byte(`{}`),
			want:  expectClose, wantCode: 3501,
		},
		{
			name:  "command without id",
			frame: []byte(`{"subscribe":{"channel":"` + subCh + `"}}`),
			want:  expectClose, wantCode: 3501,
		},
		{
			name:  "unknown method",
			frame: []byte(`{"id":7,"definitely_not_a_method":{"a":1}}`),
			want:  expectClose, wantCode: 3501,
		},
		{
			name:  "truncated json mid-session",
			frame: []byte(`{"id":8,"rpc":{"method":`),
			want:  expectClose, wantCode: 3501,
		},
		{
			name:  "subscribe with empty channel",
			frame: mustJSON(command{ID: 9, Subscribe: &subscribeCmd{Channel: ""}}),
			want:  expectClose, wantCode: 3501,
		},
		{
			name:  "channel name over the length limit",
			frame: mustJSON(command{ID: 10, Subscribe: &subscribeCmd{Channel: longChannel}}),
			want:  expectErr, wantCode: 107,
		},
		{
			name: "duplicate subscribe to the same channel",
			setup: func(r *rawWS) error {
				if err := r.sendJSON(command{ID: 11, Subscribe: &subscribeCmd{Channel: subCh}}); err != nil {
					return err
				}
				rep, err := r.readReply(10 * time.Second)
				if err != nil {
					return err
				}
				if rep.Error != nil {
					return fmt.Errorf("first subscribe failed: %d %s", rep.Error.Code, rep.Error.Message)
				}
				return nil
			},
			frame: mustJSON(command{ID: 12, Subscribe: &subscribeCmd{Channel: subCh}}),
			want:  expectErr, wantCode: 105,
		},
		{
			name:  "unsubscribe from a channel that was never subscribed",
			frame: mustJSON(command{ID: 13, Unsubscribe: &unsubscribeCmd{Channel: newChannel(chPlain, "never")}}),
			want:  expectOK,
		},
		{
			name:  "publish to a channel the app denies",
			frame: mustJSON(command{ID: 14, Publish: &publishCmd{Channel: newChannel(chNoPub, "malformed"), Data: []byte(`{"x":1}`)}}),
			want:  expectErr, wantCode: 103,
		},
		{
			name:  "maximum command id",
			frame: mustJSON(command{ID: 4294967295, RPC: &rpcCmd{Method: "echo", Data: []byte(`{"x":1}`)}}),
			want:  expectOK,
		},
	}

	for _, c := range cases {
		if err := runBadCase(e.wsURL, c); err != nil {
			return fail("%s: %v", c.name, err)
		}
	}

	// Several commands batched into a single frame must all be answered.
	if err := checkBatchedFrame(e.wsURL); err != nil {
		return fail("batched frame: %v", err)
	}
	// An id-less command is a protocol error for every method except send — and
	// send must produce a push rather than a reply.
	if err := checkAsyncSend(e.wsURL); err != nil {
		return fail("id-less send: %v", err)
	}

	// The control connection must have sailed through all of it.
	if d := control.connectings.Load() - controlConn; d != 0 {
		return fail("control connection dropped %d time(s) (code=%d) during the barrage", d, control.lastDiscode.Load())
	}
	const controlMsgs = 20
	for i := 0; i < controlMsgs; i++ {
		if _, err := e.node.Publish(controlCh, []byte(`{"ok":1}`), centrifuge.WithHistory(100, time.Minute)); err != nil {
			return fail("control publish %d: %v", i, err)
		}
	}
	if got, ok := waitCount(10*time.Second, controlMsgs, controlPubs.Load); !ok {
		return fail("control connection received %d/%d publications after the barrage", got, controlMsgs)
	}
	return okf("%d malformed/edge-case frames handled as expected; control connection unaffected", len(cases)+2)
}

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

func runBadCase(wsURL string, c badCase) error {
	r, err := dialRaw(wsURL)
	if err != nil {
		return fmt.Errorf("raw dial: %w", err)
	}
	defer r.close()
	if !c.preConnect {
		rep, err := r.connect(newUser("raw"))
		if err != nil {
			return fmt.Errorf("connect: %w", err)
		}
		if rep.Error != nil || rep.Connect == nil {
			return fmt.Errorf("connect rejected: %+v", rep.Error)
		}
	}
	if c.setup != nil {
		if err := c.setup(r); err != nil {
			return fmt.Errorf("setup: %w", err)
		}
	}
	if err := r.sendBytes(c.frame); err != nil {
		// A write can legitimately fail if the server already closed us.
		if c.want != expectClose {
			return fmt.Errorf("send: %w", err)
		}
		return nil
	}

	switch c.want {
	case expectClose:
		code, err := r.expectClosed(10 * time.Second)
		if err != nil {
			return err
		}
		if c.wantCode != 0 && code != int(c.wantCode) {
			return fmt.Errorf("closed with code %d, want %d", code, c.wantCode)
		}
	case expectErr:
		rep, err := r.readReply(10 * time.Second)
		if err != nil {
			return fmt.Errorf("read reply: %w", err)
		}
		if rep.Error == nil {
			return fmt.Errorf("got a successful reply, want error %d", c.wantCode)
		}
		if c.wantCode != 0 && rep.Error.Code != c.wantCode {
			return fmt.Errorf("error code %d (%s), want %d", rep.Error.Code, rep.Error.Message, c.wantCode)
		}
	case expectOK:
		rep, err := r.readReply(10 * time.Second)
		if err != nil {
			return fmt.Errorf("read reply: %w", err)
		}
		if rep.Error != nil {
			return fmt.Errorf("error %d (%s), want success", rep.Error.Code, rep.Error.Message)
		}
	}
	return nil
}

// checkAsyncSend sends an id-less send command — the one command the protocol
// allows without an id. It must be accepted (our handler echoes it back as an
// asynchronous message push) rather than rejected like every other id-less frame.
func checkAsyncSend(wsURL string) error {
	r, err := dialRaw(wsURL)
	if err != nil {
		return err
	}
	defer r.close()
	if rep, err := r.connect(newUser("asend-raw")); err != nil {
		return err
	} else if rep.Connect == nil {
		return fmt.Errorf("connect rejected: %+v", rep.Error)
	}
	if err := r.sendBytes([]byte(`{"send":{"data":{"hello":1}}}`)); err != nil {
		return err
	}
	rep, err := r.readReply(10 * time.Second)
	if err != nil {
		return fmt.Errorf("no echo push: %w", err)
	}
	if rep.Error != nil {
		return fmt.Errorf("error %d (%s), want the message echoed back", rep.Error.Code, rep.Error.Message)
	}
	if rep.Push == nil || rep.Push.Message == nil {
		return fmt.Errorf("got %+v, want a message push", rep)
	}
	if got := string(*rep.Push.Message); !strings.Contains(got, `{"hello":1}`) {
		return fmt.Errorf("echoed %s, want it to carry {\"hello\":1}", got)
	}
	return nil
}

// checkBatchedFrame sends several commands inside one websocket frame — the
// protocol allows it and the server must answer every one of them.
func checkBatchedFrame(wsURL string) error {
	r, err := dialRaw(wsURL)
	if err != nil {
		return err
	}
	defer r.close()
	if rep, err := r.connect(newUser("batch")); err != nil {
		return err
	} else if rep.Connect == nil {
		return fmt.Errorf("connect rejected: %+v", rep.Error)
	}
	const n = 5
	var frame []byte
	for i := 0; i < n; i++ {
		frame = append(frame, mustJSON(command{ID: uint32(100 + i), RPC: &rpcCmd{Method: "batch", Data: []byte(fmt.Sprintf(`{"i":%d}`, i))}})...)
		frame = append(frame, '\n')
	}
	if err := r.sendBytes(frame); err != nil {
		return err
	}
	seen := map[uint32]bool{}
	for len(seen) < n {
		rep, err := r.readReply(10 * time.Second)
		if err != nil {
			return fmt.Errorf("after %d/%d replies: %w", len(seen), n, err)
		}
		if rep.Error != nil {
			return fmt.Errorf("reply %d error: %d %s", rep.ID, rep.Error.Code, rep.Error.Message)
		}
		if rep.RPC == nil {
			continue
		}
		i := rep.ID - 100
		if want := fmt.Sprintf(`{"method":"batch","data":{"i":%d}}`, i); string(rep.RPC.Data) != want {
			return fmt.Errorf("reply %d echoed %s, want %s", rep.ID, rep.RPC.Data, want)
		}
		seen[rep.ID] = true
	}
	return nil
}

// staleConnection opens a connection and never authenticates. The server must
// close it once ClientStaleCloseDelay elapses.
func staleConnection(ctx context.Context, e *env) (string, error) {
	before := e.strictNode.Hub().NumClients()
	r, err := dialRaw(e.strictWSURL)
	if err != nil {
		return fail("raw dial: %v", err)
	}
	defer r.close()
	start := time.Now()
	code, err := r.expectClosed(10 * time.Second)
	if err != nil {
		return fail("connection was not closed: %v", err)
	}
	if code != 3502 {
		return fail("closed with code %d, want 3502 (stale)", code)
	}
	elapsed := time.Since(start)
	if !waitFor(5*time.Second, func() bool { return e.strictNode.Hub().NumClients() <= before }) {
		return fail("hub still holds the stale connection")
	}
	return okf("unauthenticated connection closed as stale after %s (code 3502)", elapsed.Truncate(10*time.Millisecond))
}

// oversizedFrame sends a frame larger than the transport's message size limit.
// The server must close the connection rather than buffer it.
func oversizedFrame(ctx context.Context, e *env) (string, error) {
	r, err := dialRaw(e.strictWSURL)
	if err != nil {
		return fail("raw dial: %v", err)
	}
	defer r.close()
	if rep, err := r.connect(newUser("big")); err != nil {
		return fail("connect: %v", err)
	} else if rep.Connect == nil {
		return fail("connect rejected: %+v", rep.Error)
	}
	// Strict node's MessageSizeLimit is 4 KiB.
	huge := mustJSON(command{ID: 2, RPC: &rpcCmd{Method: "big", Data: []byte(fmt.Sprintf(`{"pad":%q}`, strings.Repeat("y", 64*1024)))}})
	if err := r.sendBytes(huge); err != nil {
		// The server can tear the connection down before the whole frame is even
		// written — that is the enforcement working, just earlier.
		return okf("frame of %d bytes over a 4 KiB limit was cut off mid-write (%v)", len(huge), err)
	}
	if _, err := r.expectClosed(10 * time.Second); err != nil {
		return fail("connection not closed after an oversized frame: %v", err)
	}
	return okf("frame of %d bytes over a 4 KiB limit closed the connection", len(huge))
}

// slowClient stops reading and lets the server pile messages up behind it. The
// server must give up on that connection and release everything it held.
func slowClient(ctx context.Context, e *env) (string, error) {
	ch := newChannel(chPlain, "slow")
	r, err := dialRaw(e.strictWSURL)
	if err != nil {
		return fail("raw dial: %v", err)
	}
	defer r.close()
	if rep, err := r.connect(newUser("slow")); err != nil {
		return fail("connect: %v", err)
	} else if rep.Connect == nil {
		return fail("connect rejected: %+v", rep.Error)
	}
	if err := r.sendJSON(command{ID: 2, Subscribe: &subscribeCmd{Channel: ch}}); err != nil {
		return fail("subscribe send: %v", err)
	}
	rep, err := r.readReply(10 * time.Second)
	if err != nil {
		return fail("subscribe reply: %v", err)
	}
	if rep.Error != nil {
		return fail("subscribe error: %d %s", rep.Error.Code, rep.Error.Message)
	}

	// From here on the client reads nothing. The strict node's queue is 64 KiB,
	// so a few megabytes must trip the slow-client guard.
	payload := []byte(fmt.Sprintf(`{"pad":%q}`, strings.Repeat("s", 4096)))
	for i := 0; i < 3000; i++ {
		if _, err := e.strictNode.Publish(ch, payload); err != nil {
			return fail("publish %d: %v", i, err)
		}
	}
	code, err := r.expectClosed(30 * time.Second)
	if err != nil {
		return fail("unresponsive reader was never disconnected: %v", err)
	}
	if !waitFor(10*time.Second, func() bool { return e.strictNode.Hub().NumSubscribers(ch) == 0 }) {
		return fail("hub still has %d subscribers for %s after dropping the slow client",
			e.strictNode.Hub().NumSubscribers(ch), ch)
	}
	return okf("unresponsive reader dropped with close code %d, hub released the channel", code)
}

// noPong keeps reading but never answers pings. The server must drop the
// connection once PongTimeout expires.
func noPong(ctx context.Context, e *env) (string, error) {
	r, err := dialRaw(e.wsURL)
	if err != nil {
		return fail("raw dial: %v", err)
	}
	defer r.close()
	// "ping:" users get a 2s ping interval and a 1s pong timeout.
	if rep, err := r.connect(userPing + newUser("nopong")); err != nil {
		return fail("connect: %v", err)
	} else if rep.Connect == nil {
		return fail("connect rejected: %+v", rep.Error)
	}
	// Read frames but never send a pong.
	start := time.Now()
	deadline := start.Add(15 * time.Second)
	var pings int
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return fail("connection survived %s and %d pings without a single pong", time.Since(start), pings)
		}
		_ = r.ws.SetReadDeadline(time.Now().Add(remaining))
		_, frame, err := r.ws.ReadMessage()
		if err != nil {
			var ce *websocket.CloseError
			if errors.As(err, &ce) {
				if ce.Code != 3012 {
					return fail("closed with code %d, want 3012 (no pong)", ce.Code)
				}
				return okf("connection dropped after %s and %d unanswered pings (code 3012)",
					time.Since(start).Truncate(10*time.Millisecond), pings)
			}
			return fail("connection survived %s and %d pings, then read failed: %v", time.Since(start), pings, err)
		}
		for _, raw := range splitReplies(frame) {
			var rep reply
			if json.Unmarshal(raw, &rep) == nil && rep.isPing() {
				pings++
			}
		}
	}
}

// channelLimit checks both channel-limit paths on the strict node (8 channels):
// a client-side subscribe past the limit is an error, while a connection whose
// server-side subscriptions exceed it is refused outright.
func channelLimit(ctx context.Context, e *env) (string, error) {
	r, err := dialRaw(e.strictWSURL)
	if err != nil {
		return fail("raw dial: %v", err)
	}
	defer r.close()
	if rep, err := r.connect(newUser("chlimit")); err != nil {
		return fail("connect: %v", err)
	} else if rep.Connect == nil {
		return fail("connect rejected: %+v", rep.Error)
	}
	const limit = 8
	for i := 0; i < limit; i++ {
		if err := r.sendJSON(command{ID: uint32(10 + i), Subscribe: &subscribeCmd{Channel: newChannel(chPlain, "chlimit")}}); err != nil {
			return fail("subscribe %d send: %v", i, err)
		}
		rep, err := r.readReply(10 * time.Second)
		if err != nil {
			return fail("subscribe %d reply: %v", i, err)
		}
		if rep.Error != nil {
			return fail("subscribe %d of %d failed with %d %s", i+1, limit, rep.Error.Code, rep.Error.Message)
		}
	}
	if err := r.sendJSON(command{ID: 99, Subscribe: &subscribeCmd{Channel: newChannel(chPlain, "chlimit")}}); err != nil {
		return fail("over-limit subscribe send: %v", err)
	}
	rep, err := r.readReply(10 * time.Second)
	if err != nil {
		return fail("over-limit subscribe reply: %v", err)
	}
	if rep.Error == nil {
		return fail("subscribe %d succeeded despite a limit of %d", limit+1, limit)
	}
	if rep.Error.Code != 106 {
		return fail("over-limit subscribe returned %d (%s), want 106", rep.Error.Code, rep.Error.Message)
	}

	// Server-side subscriptions beyond the limit must refuse the connection.
	var chans []string
	for i := 0; i < limit+2; i++ {
		chans = append(chans, newChannel(chPlain, "chlimit-ss"))
	}
	r2, err := dialRaw(e.strictWSURL)
	if err != nil {
		return fail("raw dial 2: %v", err)
	}
	defer r2.close()
	if err := r2.sendJSON(command{ID: 1, Connect: &connectCmd{Token: userSSub + strings.Join(chans, ",")}}); err != nil {
		return fail("over-limit connect send: %v", err)
	}
	code, err := r2.expectClosed(10 * time.Second)
	if err != nil {
		return fail("connection with %d server-side subs was accepted: %v", len(chans), err)
	}
	if code != 3505 {
		return fail("closed with code %d, want 3505 (channel limit)", code)
	}
	return okf("client-side subscribe past %d channels → error 106; %d server-side subs → close 3505", limit, len(chans))
}

// expiredConnection uses a refresh handler that always reports the connection as
// expired: the server must keep closing it with DisconnectExpired.
func expiredConnection(ctx context.Context, e *env) (string, error) {
	co, err := dial(e.strictWSURL, userExpire+newUser("exp"))
	if err != nil {
		return fail("dial: %v", err)
	}
	defer co.c.Close()
	// Connection TTL is 1s with a 500ms grace, so several expirations fit here.
	// Code 3005 is a reconnecting code, so the SDK reports each one as a new
	// Connecting event rather than a terminal Disconnected one.
	if got, ok := waitCount(30*time.Second, 3, co.connectings.Load); !ok {
		return fail("connection was re-established %d time(s) in 30s, want at least 3 (expiry never fired)", got)
	}
	if code := co.lastDiscode.Load(); code != 3005 {
		return fail("last disconnect code is %d, want 3005 (expired)", code)
	}
	return okf("expired connection closed with code 3005, %d reconnect cycles observed", co.connectings.Load()-1)
}

// subscribeUnsubscribeRace drives client-side and server-side subscribe and
// unsubscribe at the same channel concurrently, while publishing into it. The
// server must never wedge: after everything quiesces the channel drains and the
// connection is still usable.
func subscribeUnsubscribeRace(ctx context.Context, e *env) (string, error) {
	user := newUser("subrace")
	co, err := dial(e.wsURL, user)
	if err != nil {
		return fail("dial: %v", err)
	}
	defer co.c.Close()
	ch := newChannel(chRecov, "subrace")

	raceCtx, cancel := context.WithTimeout(ctx, 6*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	var clientOps, serverOps, publishes atomic.Int64

	sub, err := co.c.NewSubscription(ch, cent.SubscriptionConfig{Recoverable: true, Positioned: true})
	if err != nil {
		return fail("new subscription: %v", err)
	}
	sub.OnPublication(func(cent.PublicationEvent) {})

	// Client-side subscribe/unsubscribe churn.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for raceCtx.Err() == nil {
			_ = sub.Subscribe()
			_ = sub.Unsubscribe()
			clientOps.Add(2)
			// Throttle a little: the point is the subscribe/unsubscribe race, not
			// how the server copes with a six-figure command rate on one socket.
			time.Sleep(100 * time.Microsecond)
		}
	}()
	// Server-side subscribe/unsubscribe churn on the very same channel.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for raceCtx.Err() == nil {
			_ = e.node.Subscribe(user, ch, centrifuge.WithRecovery(true), centrifuge.WithPositioning(true))
			_ = e.node.Unsubscribe(user, ch)
			serverOps.Add(2)
			time.Sleep(time.Millisecond)
		}
	}()
	// Traffic through the channel while its subscription state flaps.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for raceCtx.Err() == nil {
			if _, err := e.node.Publish(ch, []byte(`{"r":1}`), centrifuge.WithHistory(200, time.Minute)); err == nil {
				publishes.Add(1)
			}
			time.Sleep(time.Millisecond)
		}
	}()
	wg.Wait()

	// Quiesce: make sure nothing is left subscribed either way.
	_ = sub.Unsubscribe()
	_ = co.c.RemoveSubscription(sub)
	_ = e.node.Unsubscribe(user, ch)
	if !waitFor(15*time.Second, func() bool { return e.node.Hub().NumSubscribers(ch) == 0 }) {
		return fail("channel still has %d subscribers after the race quiesced (wedged subscription state)",
			e.node.Hub().NumSubscribers(ch))
	}
	// The connection may legitimately have been reconnecting when the race ended;
	// what matters is that it comes back and works.
	if !waitFor(20*time.Second, func() bool { return co.c.State() == cent.StateConnected }) {
		return fail("connection did not come back after the race (state=%v, code=%d)", co.c.State(), co.lastDiscode.Load())
	}
	// The connection must still work end to end.
	res, err := co.c.RPC(ctx, "echo", []byte(`{"after":"race"}`))
	if err != nil {
		return fail("RPC after race: %v", err)
	}
	if string(res.Data) != `{"method":"echo","data":{"after":"race"}}` {
		return fail("unexpected echo after race: %s", res.Data)
	}
	newCh := newChannel(chRecov, "subrace-after")
	if _, _, err := subscribe(co.c, newCh, cent.SubscriptionConfig{Positioned: true}, nil); err != nil {
		return fail("subscribe after race: %v", err)
	}
	return okf("%d client + %d server sub/unsub ops with %d publishes: no wedge, channel drained",
		clientOps.Load(), serverOps.Load(), publishes.Load())
}

// disconnectDuringSubscribe repeatedly tears connections down in the middle of a
// burst of subscribes — the window where a subscription is half-registered — and
// verifies nothing is left behind in the hub.
func disconnectDuringSubscribe(ctx context.Context, e *env) (string, error) {
	const rounds, perRound = 120, 30
	var channels []string
	for r := 0; r < rounds; r++ {
		if ctx.Err() != nil {
			break
		}
		user := newUser("discsub")
		co, err := dial(e.wsURL, user)
		if err != nil {
			return fail("round %d dial: %v", r, err)
		}
		round := make([]string, 0, perRound)
		for i := 0; i < perRound; i++ {
			ch := newChannel(chRecov, "discsub")
			round = append(round, ch)
			s, err := co.c.NewSubscription(ch, cent.SubscriptionConfig{Recoverable: true, Positioned: true})
			if err != nil {
				co.c.Close()
				return fail("round %d new sub %d: %v", r, i, err)
			}
			if err := s.Subscribe(); err != nil {
				co.c.Close()
				return fail("round %d subscribe %d: %v", r, i, err)
			}
		}
		channels = append(channels, round...)
		// Close while subscribes are still in flight — alternate between a hard
		// client close and a server-side disconnect.
		if r%2 == 0 {
			co.c.Close()
		} else {
			_ = e.node.Disconnect(user, centrifuge.WithCustomDisconnect(centrifuge.DisconnectForceNoReconnect))
			waitFor(5*time.Second, func() bool { return co.c.State() != cent.StateConnected })
			co.c.Close()
		}
	}
	var stuck string
	waitFor(20*time.Second, func() bool {
		stuck = ""
		for _, ch := range channels {
			if e.node.Hub().NumSubscribers(ch) != 0 {
				stuck = ch
				return false
			}
		}
		return true
	})
	if stuck != "" {
		return fail("channel %s still has %d subscribers after the connection went away",
			stuck, e.node.Hub().NumSubscribers(stuck))
	}
	return okf("%d rounds × %d in-flight subscribes torn down, hub clean", rounds, perRound)
}
