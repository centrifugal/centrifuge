package main

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/centrifugal/centrifuge"
	cent "github.com/centrifugal/centrifuge-go"
)

// ---------------------------------------------------------------------------
// Server-side subscriptions.
// ---------------------------------------------------------------------------

// serverSideSubs exercises subscriptions the server installs at connect time:
// the client never sends a subscribe command, yet it must receive the subscribed
// push, every publication, and full recovery across a forced reconnect.
func serverSideSubs(ctx context.Context, e *env) (string, error) {
	ch := newChannel(chRecov, "ssub")
	user := userSSub + ch

	r := &recorder{}
	subscribed := make(chan cent.ServerSubscribedEvent, 8)
	var unsubs atomic.Int64
	co, err := dial(e.wsURL, user, withSetup(func(cl *cent.Client) {
		cl.OnSubscribed(func(ev cent.ServerSubscribedEvent) {
			select {
			case subscribed <- ev:
			default:
			}
		})
		cl.OnUnsubscribed(func(cent.ServerUnsubscribedEvent) { unsubs.Add(1) })
		cl.OnPublication(func(ev cent.ServerPublicationEvent) { r.add(ev.Offset, ev.Data) })
	}))
	if err != nil {
		return fail("dial: %v", err)
	}
	defer co.c.Close()

	var first cent.ServerSubscribedEvent
	select {
	case first = <-subscribed:
	case <-time.After(10 * time.Second):
		return fail("no server-side subscribed event for %s", ch)
	}
	if first.Channel != ch {
		return fail("subscribed to %q, want %q", first.Channel, ch)
	}
	if !first.Recoverable || !first.Positioned {
		return fail("server-side sub not recoverable/positioned (recoverable=%v positioned=%v)", first.Recoverable, first.Positioned)
	}

	const live, gap = 20, 15
	for i := 0; i < live; i++ {
		if _, err := e.node.Publish(ch, []byte(fmt.Sprintf(`{"live":%d}`, i)), centrifuge.WithHistory(500, time.Minute)); err != nil {
			return fail("live publish %d: %v", i, err)
		}
	}
	if !waitFor(10*time.Second, func() bool { return r.len() >= live }) {
		return fail("received %d/%d live publications on server-side sub", r.len(), live)
	}

	// Force a reconnect and publish while the client is away: the server-side
	// subscription must be re-established with recovery.
	if err := forceReconnect(e.node, co, user); err != nil {
		return fail("%v", err)
	}
	for i := 0; i < gap; i++ {
		if _, err := e.node.Publish(ch, []byte(fmt.Sprintf(`{"gap":%d}`, i)), centrifuge.WithHistory(500, time.Minute)); err != nil {
			return fail("gap publish %d: %v", i, err)
		}
	}
	var again cent.ServerSubscribedEvent
	select {
	case again = <-subscribed:
	case <-time.After(15 * time.Second):
		return fail("server-side subscription not re-established after reconnect")
	}
	if !again.Recovered {
		return fail("server-side subscription did not recover (wasRecovering=%v)", again.WasRecovering)
	}
	if !waitFor(10*time.Second, func() bool { return r.len() >= live+gap }) {
		return fail("received %d/%d publications after recovery", r.len(), live+gap)
	}
	offs, _ := r.snapshot()
	if len(offs) != live+gap {
		return fail("received %d publications, want exactly %d (duplicates across recovery?)", len(offs), live+gap)
	}
	for i := 1; i < len(offs); i++ {
		if offs[i] != offs[i-1]+1 {
			return fail("server-side sub offset gap: %d then %d", offs[i-1], offs[i])
		}
	}
	return okf("server-side sub: %d live + %d recovered, contiguous offsets %d..%d", live, gap, offs[0], offs[len(offs)-1])
}

// serverSubAPIChurn drives node.Subscribe/node.Unsubscribe against several
// connections of one user, verifying every cycle reaches every connection, that
// publications land while subscribed, and that the hub is clean at the end.
func serverSubAPIChurn(ctx context.Context, e *env) (string, error) {
	const conns, cycles = 4, 120
	user := newUser("subapi")
	ch := newChannel(chPlain, "subapi")

	subs := make([]*atomic.Int64, conns)
	unsubs := make([]*atomic.Int64, conns)
	pubs := make([]*atomic.Int64, conns)
	clients := make([]*conn, conns)
	defer func() { closeAll(clients) }()

	for i := 0; i < conns; i++ {
		s, u, p := &atomic.Int64{}, &atomic.Int64{}, &atomic.Int64{}
		subs[i], unsubs[i], pubs[i] = s, u, p
		co, err := dial(e.wsURL, user, withSetup(func(cl *cent.Client) {
			cl.OnSubscribed(func(cent.ServerSubscribedEvent) { s.Add(1) })
			cl.OnUnsubscribed(func(cent.ServerUnsubscribedEvent) { u.Add(1) })
			cl.OnPublication(func(cent.ServerPublicationEvent) { p.Add(1) })
		}))
		if err != nil {
			return fail("conn %d dial: %v", i, err)
		}
		clients[i] = co
	}

	allReached := func(counters []*atomic.Int64, want int64) bool {
		for _, c := range counters {
			if c.Load() < want {
				return false
			}
		}
		return true
	}

	for i := 1; i <= cycles; i++ {
		if err := e.node.Subscribe(user, ch); err != nil {
			return fail("cycle %d subscribe: %v", i, err)
		}
		if !waitFor(10*time.Second, func() bool { return allReached(subs, int64(i)) }) {
			return fail("cycle %d: only some connections subscribed (%v)", i, counterValues(subs))
		}
		if _, err := e.node.Publish(ch, []byte(fmt.Sprintf(`{"c":%d}`, i))); err != nil {
			return fail("cycle %d publish: %v", i, err)
		}
		if !waitFor(10*time.Second, func() bool { return allReached(pubs, int64(i)) }) {
			return fail("cycle %d: publication not delivered to every connection (%v)", i, counterValues(pubs))
		}
		if err := e.node.Unsubscribe(user, ch); err != nil {
			return fail("cycle %d unsubscribe: %v", i, err)
		}
		if !waitFor(10*time.Second, func() bool { return allReached(unsubs, int64(i)) }) {
			return fail("cycle %d: only some connections unsubscribed (%v)", i, counterValues(unsubs))
		}
	}
	for i := range subs {
		if got := pubs[i].Load(); got != cycles {
			return fail("conn %d received %d publications, want exactly %d (delivery after unsubscribe?)", i, got, cycles)
		}
	}
	if !waitFor(5*time.Second, func() bool { return e.node.Hub().NumSubscribers(ch) == 0 }) {
		return fail("hub still has %d subscribers for %s", e.node.Hub().NumSubscribers(ch), ch)
	}
	return okf("%d server-API subscribe/publish/unsubscribe cycles across %d connections", cycles, conns)
}

func counterValues(cs []*atomic.Int64) []int64 {
	out := make([]int64, len(cs))
	for i, c := range cs {
		out[i] = c.Load()
	}
	return out
}

// multiConnSameUser checks the user-targeted server APIs fan out to every
// connection of a user — including the client whitelist on disconnect.
func multiConnSameUser(ctx context.Context, e *env) (string, error) {
	const n = 5
	user := newUser("multi")
	ch := newChannel(chPlain, "multi")

	pubs := make([]*atomic.Int64, n)
	clients := make([]*conn, n)
	defer func() { closeAll(clients) }()
	var subscribed, unsubscribed atomic.Int64
	for i := 0; i < n; i++ {
		p := &atomic.Int64{}
		pubs[i] = p
		co, err := dial(e.wsURL, user, withSetup(func(cl *cent.Client) {
			cl.OnSubscribed(func(cent.ServerSubscribedEvent) { subscribed.Add(1) })
			cl.OnUnsubscribed(func(cent.ServerUnsubscribedEvent) { unsubscribed.Add(1) })
			cl.OnPublication(func(cent.ServerPublicationEvent) { p.Add(1) })
		}))
		if err != nil {
			return fail("conn %d dial: %v", i, err)
		}
		clients[i] = co
	}

	if err := e.node.Subscribe(user, ch); err != nil {
		return fail("subscribe: %v", err)
	}
	if got, ok := waitCount(10*time.Second, n, subscribed.Load); !ok {
		return fail("%d/%d connections subscribed via user API", got, n)
	}
	const msgs = 20
	for i := 0; i < msgs; i++ {
		if _, err := e.node.Publish(ch, []byte(`{"m":1}`)); err != nil {
			return fail("publish %d: %v", i, err)
		}
	}
	if !waitFor(10*time.Second, func() bool {
		for _, p := range pubs {
			if p.Load() < msgs {
				return false
			}
		}
		return true
	}) {
		return fail("publications not delivered to all connections: %v", counterValues(pubs))
	}

	// Refresh must not disturb anything.
	connBefore := make([]int64, n)
	for i, c := range clients {
		connBefore[i] = c.connectings.Load()
	}
	if err := e.node.Refresh(user); err != nil {
		return fail("refresh: %v", err)
	}
	if err := e.node.Unsubscribe(user, ch); err != nil {
		return fail("unsubscribe: %v", err)
	}
	if got, ok := waitCount(10*time.Second, n, unsubscribed.Load); !ok {
		return fail("%d/%d connections unsubscribed via user API", got, n)
	}

	// Disconnect everyone except the first connection.
	keep := clients[0].id()
	if keep == "" {
		return fail("no client id captured for whitelisted connection")
	}
	if err := e.node.Disconnect(user,
		centrifuge.WithCustomDisconnect(centrifuge.DisconnectForceNoReconnect),
		centrifuge.WithDisconnectClientWhitelist([]string{keep})); err != nil {
		return fail("disconnect: %v", err)
	}
	if !waitFor(10*time.Second, func() bool {
		for i := 1; i < n; i++ {
			if clients[i].c.State() == cent.StateConnected {
				return false
			}
		}
		return true
	}) {
		return fail("non-whitelisted connections still connected after disconnect")
	}
	time.Sleep(300 * time.Millisecond)
	if st := clients[0].c.State(); st != cent.StateConnected {
		return fail("whitelisted connection was disconnected too (state=%v, code=%d)", st, clients[0].lastDiscode.Load())
	}
	if d := clients[0].connectings.Load() - connBefore[0]; d != 0 {
		return fail("whitelisted connection dropped %d time(s) (code=%d)", d, clients[0].lastDiscode.Load())
	}
	return okf("user API reached all %d connections; whitelist kept 1 alive, dropped %d", n, n-1)
}

// ---------------------------------------------------------------------------
// Protocol / codec coverage.
// ---------------------------------------------------------------------------

// protobufDeltaRecovery runs the trickiest combination — binary protocol, fossil
// delta and stream recovery — over the Protobuf codec, which is a completely
// separate encode/decode path from the JSON one every other scenario uses.
func protobufDeltaRecovery(ctx context.Context, e *env) (string, error) {
	user := newUser("pbdelta")
	co, err := dial(e.wsURL, user, withProtobuf())
	if err != nil {
		return fail("dial: %v", err)
	}
	defer co.c.Close()
	ch := newChannel(chDelta, "pbdelta")

	r := &recorder{}
	subscribed := make(chan struct{}, 8)
	sub, err := co.c.NewSubscription(ch, cent.SubscriptionConfig{
		Delta: cent.DeltaTypeFossil, Recoverable: true, Positioned: true,
	})
	if err != nil {
		return fail("new subscription: %v", err)
	}
	sub.OnPublication(func(ev cent.PublicationEvent) { r.add(ev.Offset, ev.Data) })
	sub.OnSubscribed(func(cent.SubscribedEvent) {
		select {
		case subscribed <- struct{}{}:
		default:
		}
	})
	if err := sub.Subscribe(); err != nil {
		return fail("subscribe: %v", err)
	}
	select {
	case <-subscribed:
	case <-time.After(10 * time.Second):
		return fail("initial subscribe did not complete")
	}

	// Payloads share a large common prefix so fossil actually produces deltas,
	// and carry non-ASCII bytes to make sure binary framing round-trips.
	body := strings.Repeat("δelta-body-ключ-", 40)
	payload := func(i int) string { return fmt.Sprintf(`{"body":%q,"seq":%d}`, body, i) }

	const live, gap = 250, 120
	var want []string
	for i := 0; i < live; i++ {
		want = append(want, payload(i))
		if _, err := e.node.Publish(ch, []byte(payload(i)),
			centrifuge.WithDelta(true), centrifuge.WithHistory(500, time.Minute)); err != nil {
			return fail("live publish %d: %v", i, err)
		}
	}
	if !waitFor(15*time.Second, func() bool { return r.len() >= live }) {
		return fail("received %d/%d live delta publications", r.len(), live)
	}

	if err := forceReconnect(e.node, co, user); err != nil {
		return fail("%v", err)
	}
	for i := live; i < live+gap; i++ {
		want = append(want, payload(i))
		if _, err := e.node.Publish(ch, []byte(payload(i)),
			centrifuge.WithDelta(true), centrifuge.WithHistory(500, time.Minute)); err != nil {
			return fail("gap publish %d: %v", i, err)
		}
	}
	select {
	case <-subscribed:
	case <-time.After(15 * time.Second):
		return fail("did not resubscribe after forced reconnect")
	}
	if !waitFor(15*time.Second, func() bool { return r.len() >= live+gap }) {
		return fail("received %d/%d publications after recovery", r.len(), live+gap)
	}
	_, got := r.snapshot()
	if len(got) != len(want) {
		return fail("received %d publications, want exactly %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			return fail("protobuf delta mismatch at %d:\n  want %s\n  got  %s", i, want[i], got[i])
		}
	}
	return okf("protobuf + fossil delta + recovery: %d live + %d recovered reconstructed exactly", live, gap)
}

// cacheRecovery checks RecoveryModeCache semantics: a (re)subscribe delivers the
// latest publication only — never the whole backlog — and matching positions
// deliver nothing at all.
func cacheRecovery(ctx context.Context, e *env) (string, error) {
	user := newUser("cache")
	ch := newChannel(chCache, "cache")

	// Fill the cache before anyone subscribes.
	const backlog = 12
	for i := 0; i < backlog; i++ {
		if _, err := e.node.Publish(ch, []byte(fmt.Sprintf(`{"pre":%d}`, i)), centrifuge.WithHistory(100, time.Minute)); err != nil {
			return fail("backlog publish %d: %v", i, err)
		}
	}

	co, err := dial(e.wsURL, user)
	if err != nil {
		return fail("dial: %v", err)
	}
	defer co.c.Close()

	r := &recorder{}
	subscribed := make(chan struct{}, 8)
	sub, err := co.c.NewSubscription(ch, cent.SubscriptionConfig{Recoverable: true, Positioned: true})
	if err != nil {
		return fail("new subscription: %v", err)
	}
	sub.OnPublication(func(ev cent.PublicationEvent) { r.add(ev.Offset, ev.Data) })
	sub.OnSubscribed(func(cent.SubscribedEvent) {
		select {
		case subscribed <- struct{}{}:
		default:
		}
	})
	if err := sub.Subscribe(); err != nil {
		return fail("subscribe: %v", err)
	}
	select {
	case <-subscribed:
	case <-time.After(10 * time.Second):
		return fail("subscribe did not complete")
	}
	// Cache mode: exactly the latest publication, not the whole backlog.
	if !waitFor(5*time.Second, func() bool { return r.len() >= 1 }) {
		return fail("cache recovery delivered nothing on subscribe")
	}
	time.Sleep(300 * time.Millisecond) // give any extra (wrong) deliveries a chance.
	_, got := r.snapshot()
	if len(got) != 1 {
		return fail("cache subscribe delivered %d publications, want exactly 1 (%v)", len(got), got)
	}
	if want := fmt.Sprintf(`{"pre":%d}`, backlog-1); got[0] != want {
		return fail("cache subscribe delivered %s, want the latest %s", got[0], want)
	}

	// Live publications flow normally.
	const liveMsgs = 5
	for i := 0; i < liveMsgs; i++ {
		if _, err := e.node.Publish(ch, []byte(fmt.Sprintf(`{"live":%d}`, i)), centrifuge.WithHistory(100, time.Minute)); err != nil {
			return fail("live publish %d: %v", i, err)
		}
	}
	if !waitFor(8*time.Second, func() bool { return r.len() >= 1+liveMsgs }) {
		return fail("received %d live publications, want %d", r.len()-1, liveMsgs)
	}

	// Reconnect with a backlog produced while away: cache recovery must deliver
	// exactly one publication — the newest.
	if err := forceReconnect(e.node, co, user); err != nil {
		return fail("%v", err)
	}
	const away = 7
	for i := 0; i < away; i++ {
		if _, err := e.node.Publish(ch, []byte(fmt.Sprintf(`{"away":%d}`, i)), centrifuge.WithHistory(100, time.Minute)); err != nil {
			return fail("away publish %d: %v", i, err)
		}
	}
	select {
	case <-subscribed:
	case <-time.After(15 * time.Second):
		return fail("did not resubscribe after forced reconnect")
	}
	wantTotal := 1 + liveMsgs + 1
	if !waitFor(10*time.Second, func() bool { return r.len() >= wantTotal }) {
		return fail("received %d publications after cache recovery, want %d", r.len(), wantTotal)
	}
	time.Sleep(300 * time.Millisecond)
	_, got = r.snapshot()
	if len(got) != wantTotal {
		return fail("cache recovery delivered %d publications total, want %d (whole backlog replayed?): %v", len(got), wantTotal, got)
	}
	if want := fmt.Sprintf(`{"away":%d}`, away-1); got[len(got)-1] != want {
		return fail("cache recovery delivered %s, want the latest %s", got[len(got)-1], want)
	}
	return okf("cache mode: latest-only on subscribe and on recovery (%d publications for %d published)", wantTotal, backlog+liveMsgs+away)
}

// sseTransport connects over Server-Sent Events with a server-side subscription
// and verifies the SSE framing delivers every publication in order.
func sseTransport(ctx context.Context, e *env) (string, error) {
	ch := newChannel(chPlain, "sse")
	sc, err := dialSSE(e.httpURL, userSSub+ch)
	if err != nil {
		return fail("sse dial: %v", err)
	}
	defer sc.close()
	if err := sc.waitConnected(10 * time.Second); err != nil {
		return fail("sse connect: %v", err)
	}
	const n = 150
	for i := 0; i < n; i++ {
		if _, err := e.node.Publish(ch, []byte(fmt.Sprintf(`{"n":%d}`, i))); err != nil {
			return fail("publish %d: %v", i, err)
		}
	}
	if !waitFor(15*time.Second, func() bool { return len(sc.publications(ch)) >= n }) {
		return fail("sse received %d/%d publications", len(sc.publications(ch)), n)
	}
	got := sc.publications(ch)
	if len(got) != n {
		return fail("sse received %d publications, want exactly %d", len(got), n)
	}
	for i, g := range got {
		if want := fmt.Sprintf(`{"n":%d}`, i); g != want {
			return fail("sse publication %d is %s, want %s (out of order?)", i, g, want)
		}
	}
	return okf("SSE transport delivered %d publications in order", n)
}

// httpStreamEmulation drives the bidirectional-over-unidirectional path: replies
// and pushes stream down over HTTP, while commands travel up through the
// emulation endpoint.
func httpStreamEmulation(ctx context.Context, e *env) (string, error) {
	ch := newChannel(chRecov, "emu")
	sc, err := dialHTTPStream(e.httpURL, newUser("emu"))
	if err != nil {
		return fail("http_stream dial: %v", err)
	}
	defer sc.close()
	if err := sc.waitConnected(10 * time.Second); err != nil {
		return fail("http_stream connect: %v", err)
	}

	// Subscribe over the emulation uplink.
	if err := sc.emulate(command{ID: 2, Subscribe: &subscribeCmd{Channel: ch}}); err != nil {
		return fail("emulate subscribe: %v", err)
	}
	rep, err := sc.waitReplyWithID(2, 10*time.Second)
	if err != nil {
		return fail("subscribe reply: %v", err)
	}
	if rep.Error != nil {
		return fail("subscribe error: %d %s", rep.Error.Code, rep.Error.Message)
	}

	// Server-side publications must reach the stream.
	const n = 50
	for i := 0; i < n; i++ {
		if _, err := e.node.Publish(ch, []byte(fmt.Sprintf(`{"n":%d}`, i)), centrifuge.WithHistory(200, time.Minute)); err != nil {
			return fail("publish %d: %v", i, err)
		}
	}
	if !waitFor(15*time.Second, func() bool { return len(sc.publications(ch)) >= n }) {
		return fail("stream received %d/%d publications", len(sc.publications(ch)), n)
	}

	// A publish sent up through emulation must come back down the stream.
	if err := sc.emulate(command{ID: 3, Publish: &publishCmd{Channel: ch, Data: []byte(`{"via":"emulation"}`)}}); err != nil {
		return fail("emulate publish: %v", err)
	}
	if rep, err = sc.waitReplyWithID(3, 10*time.Second); err != nil {
		return fail("publish reply: %v", err)
	}
	if rep.Error != nil {
		return fail("publish error: %d %s", rep.Error.Code, rep.Error.Message)
	}
	if !waitFor(10*time.Second, func() bool {
		pubs := sc.publications(ch)
		return len(pubs) > 0 && pubs[len(pubs)-1] == `{"via":"emulation"}`
	}) {
		return fail("publication sent over emulation never came back on the stream")
	}

	// RPC round-trip over the same path.
	if err := sc.emulate(command{ID: 4, RPC: &rpcCmd{Method: "emu", Data: []byte(`{"x":1}`)}}); err != nil {
		return fail("emulate rpc: %v", err)
	}
	rep, err = sc.waitReplyWithID(4, 10*time.Second)
	if err != nil {
		return fail("rpc reply: %v", err)
	}
	if rep.Error != nil {
		return fail("rpc error: %d %s", rep.Error.Code, rep.Error.Message)
	}
	if rep.RPC == nil || string(rep.RPC.Data) != `{"method":"emu","data":{"x":1}}` {
		return fail("unexpected rpc echo: %v", rep.RPC)
	}
	return okf("http_stream downlink + emulation uplink: %d pushes, publish and RPC round-trips", n+1)
}

// ---------------------------------------------------------------------------
// Broker semantics under concurrency.
// ---------------------------------------------------------------------------

// concurrentPublishOffsets publishes to one history-backed channel from many
// goroutines at once. Offsets must stay unique and contiguous, and a positioned
// subscriber must see every publication in order without ever having to
// resubscribe (a resubscribe would mean the server broke stream continuity).
func concurrentPublishOffsets(ctx context.Context, e *env) (string, error) {
	co, err := dial(e.wsURL, newUser("offsets"))
	if err != nil {
		return fail("dial: %v", err)
	}
	defer co.c.Close()
	ch := newChannel(chRecov, "offsets")

	const workers, each = 64, 80
	const total = workers * each

	r := &recorder{}
	var resubscribes atomic.Int64
	sub, err := co.c.NewSubscription(ch, cent.SubscriptionConfig{Recoverable: true, Positioned: true})
	if err != nil {
		return fail("new subscription: %v", err)
	}
	sub.OnPublication(func(ev cent.PublicationEvent) { r.add(ev.Offset, ev.Data) })
	subscribed := make(chan struct{}, 4)
	sub.OnSubscribed(func(cent.SubscribedEvent) {
		select {
		case subscribed <- struct{}{}:
		default:
		}
	})
	sub.OnSubscribing(func(cent.SubscribingEvent) { resubscribes.Add(1) })
	if err := sub.Subscribe(); err != nil {
		return fail("subscribe: %v", err)
	}
	select {
	case <-subscribed:
	case <-time.After(10 * time.Second):
		return fail("subscribe did not complete")
	}
	initialSubscribings := resubscribes.Load()

	var wg sync.WaitGroup
	var pubErrs atomic.Int64
	var lastErr atomic.Value
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < each; i++ {
				_, err := e.node.Publish(ch, []byte(fmt.Sprintf(`{"w":%d,"i":%d}`, w, i)),
					centrifuge.WithHistory(total+100, time.Minute))
				if err != nil {
					pubErrs.Add(1)
					lastErr.Store(err.Error())
				}
			}
		}(w)
	}
	wg.Wait()
	if pubErrs.Load() > 0 {
		return fail("%d concurrent publishes failed (lastErr=%v)", pubErrs.Load(), lastErr.Load())
	}

	// History must hold every publication with contiguous offsets.
	hist, err := e.node.History(ch, centrifuge.WithLimit(total+100))
	if err != nil {
		return fail("history: %v", err)
	}
	if len(hist.Publications) != total {
		return fail("history holds %d publications, want %d", len(hist.Publications), total)
	}
	for i := 1; i < len(hist.Publications); i++ {
		if hist.Publications[i].Offset != hist.Publications[i-1].Offset+1 {
			return fail("history offset gap under concurrency: %d then %d",
				hist.Publications[i-1].Offset, hist.Publications[i].Offset)
		}
	}

	if !waitFor(20*time.Second, func() bool { return r.len() >= total }) {
		return fail("subscriber received %d/%d publications", r.len(), total)
	}
	offs, _ := r.snapshot()
	if len(offs) != total {
		return fail("subscriber received %d publications, want exactly %d", len(offs), total)
	}
	for i := 1; i < len(offs); i++ {
		if offs[i] != offs[i-1]+1 {
			return fail("delivery offset gap under concurrency: %d then %d", offs[i-1], offs[i])
		}
	}
	if got := resubscribes.Load() - initialSubscribings; got != 0 {
		return fail("subscription resubscribed %d time(s) — server broke stream continuity", got)
	}
	return okf("%d concurrent publishes: offsets unique, contiguous and delivered in order", total)
}

// idempotentPublish verifies the broker's idempotency key suppresses duplicates
// — including when the duplicates race each other.
func idempotentPublish(ctx context.Context, e *env) (string, error) {
	co, err := dial(e.wsURL, newUser("idem"))
	if err != nil {
		return fail("dial: %v", err)
	}
	defer co.c.Close()
	ch := newChannel(chRecov, "idem")
	r := &recorder{}
	if _, _, err := subscribe(co.c, ch, cent.SubscriptionConfig{Positioned: true}, func(s *cent.Subscription) {
		s.OnPublication(func(ev cent.PublicationEvent) { r.add(ev.Offset, ev.Data) })
	}); err != nil {
		return fail("subscribe: %v", err)
	}

	// Sequential duplicates.
	const seqDupes = 50
	var firstPos centrifuge.StreamPosition
	for i := 0; i < seqDupes; i++ {
		res, err := e.node.Publish(ch, []byte(`{"idem":"seq"}`),
			centrifuge.WithHistory(100, time.Minute), centrifuge.WithIdempotencyKey("seq-key"))
		if err != nil {
			return fail("sequential publish %d: %v", i, err)
		}
		if i == 0 {
			if res.Suppressed {
				return fail("first publish with a fresh idempotency key was suppressed")
			}
			firstPos = res.StreamPosition
		} else {
			if !res.Suppressed {
				return fail("duplicate publish %d was not suppressed", i)
			}
			if res.StreamPosition != firstPos {
				return fail("duplicate publish %d returned position %+v, want %+v", i, res.StreamPosition, firstPos)
			}
		}
	}

	// Concurrent duplicates on a second key.
	const conDupes = 64
	var wg sync.WaitGroup
	var accepted atomic.Int64
	var pubErrs atomic.Int64
	positions := make([]centrifuge.StreamPosition, conDupes)
	for i := 0; i < conDupes; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			res, err := e.node.Publish(ch, []byte(`{"idem":"con"}`),
				centrifuge.WithHistory(100, time.Minute), centrifuge.WithIdempotencyKey("con-key"))
			if err != nil {
				pubErrs.Add(1)
				return
			}
			positions[i] = res.StreamPosition
			if !res.Suppressed {
				accepted.Add(1)
			}
		}(i)
	}
	wg.Wait()
	if pubErrs.Load() > 0 {
		return fail("%d concurrent idempotent publishes errored", pubErrs.Load())
	}
	if accepted.Load() != 1 {
		return fail("%d of %d concurrent duplicates were accepted, want exactly 1", accepted.Load(), conDupes)
	}
	for i, p := range positions {
		if p != positions[0] {
			return fail("concurrent duplicate %d returned position %+v, want %+v", i, p, positions[0])
		}
	}

	hist, err := e.node.History(ch, centrifuge.WithLimit(100))
	if err != nil {
		return fail("history: %v", err)
	}
	if len(hist.Publications) != 2 {
		return fail("history holds %d publications, want 2 (one per idempotency key)", len(hist.Publications))
	}
	if !waitFor(8*time.Second, func() bool { return r.len() >= 2 }) {
		return fail("subscriber received %d publications, want 2", r.len())
	}
	time.Sleep(300 * time.Millisecond)
	if got := r.len(); got != 2 {
		return fail("subscriber received %d publications, want exactly 2 (suppressed pubs were fanned out)", got)
	}
	return okf("%d sequential + %d concurrent duplicates collapsed to 1 publication each", seqDupes, conDupes)
}

// historyPagination walks a long stream forwards page by page and backwards in
// one shot, checking both directions reconstruct it exactly.
func historyPagination(ctx context.Context, e *env) (string, error) {
	ch := newChannel(chRecov, "histpage")
	const n, page = 2000, 50
	for i := 0; i < n; i++ {
		if _, err := e.node.Publish(ch, []byte(fmt.Sprintf(`{"n":%d}`, i)), centrifuge.WithHistory(n+50, time.Minute)); err != nil {
			return fail("publish %d: %v", i, err)
		}
	}
	top, err := e.node.History(ch)
	if err != nil {
		return fail("history top: %v", err)
	}
	if top.Offset != uint64(n) {
		return fail("stream top offset is %d, want %d", top.Offset, n)
	}

	// Forward pagination.
	var forward []uint64
	since := &centrifuge.StreamPosition{Offset: 0, Epoch: top.Epoch}
	for pages := 0; ; pages++ {
		if pages > n/page+5 {
			return fail("forward pagination did not terminate after %d pages", pages)
		}
		res, err := e.node.History(ch, centrifuge.WithSince(since), centrifuge.WithLimit(page))
		if err != nil {
			return fail("forward page %d: %v", pages, err)
		}
		if len(res.Publications) == 0 {
			break
		}
		for _, p := range res.Publications {
			forward = append(forward, p.Offset)
		}
		last := res.Publications[len(res.Publications)-1]
		since = &centrifuge.StreamPosition{Offset: last.Offset, Epoch: top.Epoch}
	}
	if len(forward) != n {
		return fail("forward pagination collected %d offsets, want %d", len(forward), n)
	}
	for i, off := range forward {
		if off != uint64(i+1) {
			return fail("forward pagination offset %d is %d, want %d", i, off, i+1)
		}
	}

	// Reverse read of the whole stream.
	rev, err := e.node.History(ch, centrifuge.WithLimit(n+50), centrifuge.WithReverse(true))
	if err != nil {
		return fail("reverse history: %v", err)
	}
	if len(rev.Publications) != n {
		return fail("reverse history returned %d publications, want %d", len(rev.Publications), n)
	}
	for i, p := range rev.Publications {
		if want := uint64(n - i); p.Offset != want {
			return fail("reverse history offset %d is %d, want %d", i, p.Offset, want)
		}
	}

	// A since-position at the top must yield nothing.
	tail, err := e.node.History(ch, centrifuge.WithSince(&centrifuge.StreamPosition{Offset: top.Offset, Epoch: top.Epoch}), centrifuge.WithLimit(page))
	if err != nil {
		return fail("tail history: %v", err)
	}
	if len(tail.Publications) != 0 {
		return fail("since-top returned %d publications, want 0", len(tail.Publications))
	}
	return okf("%d publications paginated forward in %d-sized pages and read back in reverse", n, page)
}

// unrecoverablePosition drops a recoverable subscriber past the end of the
// retention window: while it is away more publications happen than the channel
// keeps, so its saved position can no longer be bridged. The client must be told
// recovery failed, resubscribe cleanly, and keep receiving live publications.
// It also checks RemoveHistory actually empties the stream.
func unrecoverablePosition(ctx context.Context, e *env) (string, error) {
	// Small window on purpose: this is what makes a position unrecoverable.
	const window = 5
	pub := func(ch string, data string) error {
		_, err := e.node.Publish(ch, []byte(data), centrifuge.WithHistory(window, time.Minute))
		return err
	}

	// RemoveHistory must leave the stream empty.
	wipeCh := newChannel(chRecov, "wipe")
	for i := 0; i < 3; i++ {
		if err := pub(wipeCh, fmt.Sprintf(`{"w":%d}`, i)); err != nil {
			return fail("wipe publish %d: %v", i, err)
		}
	}
	if err := e.node.RemoveHistory(wipeCh); err != nil {
		return fail("remove history: %v", err)
	}
	if hist, err := e.node.History(wipeCh, centrifuge.WithLimit(100)); err != nil {
		return fail("history after remove: %v", err)
	} else if len(hist.Publications) != 0 {
		return fail("history still holds %d publications after RemoveHistory", len(hist.Publications))
	}

	user := newUser("unrecov")
	co, err := dial(e.wsURL, user)
	if err != nil {
		return fail("dial: %v", err)
	}
	defer co.c.Close()
	ch := newChannel(chRecov, "unrecov")

	var mu sync.Mutex
	var events []cent.SubscribedEvent
	r := &recorder{}
	subscribed := make(chan struct{}, 8)
	sub, err := co.c.NewSubscription(ch, cent.SubscriptionConfig{Recoverable: true, Positioned: true})
	if err != nil {
		return fail("new subscription: %v", err)
	}
	sub.OnPublication(func(ev cent.PublicationEvent) { r.add(ev.Offset, ev.Data) })
	sub.OnSubscribed(func(ev cent.SubscribedEvent) {
		mu.Lock()
		events = append(events, ev)
		mu.Unlock()
		select {
		case subscribed <- struct{}{}:
		default:
		}
	})
	if err := sub.Subscribe(); err != nil {
		return fail("subscribe: %v", err)
	}
	select {
	case <-subscribed:
	case <-time.After(10 * time.Second):
		return fail("initial subscribe did not complete")
	}

	for i := 0; i < 10; i++ {
		if err := pub(ch, fmt.Sprintf(`{"a":%d}`, i)); err != nil {
			return fail("publish %d: %v", i, err)
		}
	}
	if !waitFor(8*time.Second, func() bool { return r.len() >= 10 }) {
		return fail("received %d/10 publications before the gap", r.len())
	}

	if err := forceReconnect(e.node, co, user); err != nil {
		return fail("%v", err)
	}
	// Far more than the retention window, so the saved position falls out of it.
	const away = 40
	for i := 0; i < away; i++ {
		if err := pub(ch, fmt.Sprintf(`{"b":%d}`, i)); err != nil {
			return fail("gap publish %d: %v", i, err)
		}
	}
	select {
	case <-subscribed:
	case <-time.After(20 * time.Second):
		return fail("subscription never came back after an unrecoverable position")
	}
	mu.Lock()
	last := events[len(events)-1]
	nEvents := len(events)
	mu.Unlock()
	if !last.WasRecovering {
		return fail("client did not even attempt recovery after the reconnect")
	}
	if last.Recovered {
		return fail("recovery reported success although %d publications passed through a %d-publication window", away, window)
	}

	// The subscription must be usable again: live publications flow.
	before := r.len()
	for i := 0; i < 5; i++ {
		if err := pub(ch, fmt.Sprintf(`{"c":%d}`, i)); err != nil {
			return fail("live publish %d: %v", i, err)
		}
	}
	if !waitFor(10*time.Second, func() bool { return r.len() >= before+5 }) {
		return fail("only %d live publications delivered after recovery failure, want 5", r.len()-before)
	}
	if st := sub.State(); st != cent.SubStateSubscribed {
		return fail("subscription state is %v after recovery failure, want subscribed", st)
	}
	return okf("position lost past a %d-publication window reported as unrecovered (%d subscribed events); subscription healthy afterwards", window, nEvents)
}

// subRefreshExpiry gives a subscription a 2s TTL and checks the server-side sub
// refresh handler keeps it alive and delivering for several TTL windows.
func subRefreshExpiry(ctx context.Context, e *env) (string, error) {
	co, err := dial(e.wsURL, newUser("subexp"))
	if err != nil {
		return fail("dial: %v", err)
	}
	defer co.c.Close()
	ch := newChannel(chSubExp, "subexp")
	var got atomic.Int64
	var unsubs atomic.Int64
	sub, _, err := subscribe(co.c, ch, cent.SubscriptionConfig{}, func(s *cent.Subscription) {
		s.OnPublication(func(cent.PublicationEvent) { got.Add(1) })
		s.OnUnsubscribed(func(cent.UnsubscribedEvent) { unsubs.Add(1) })
	})
	if err != nil {
		return fail("subscribe: %v", err)
	}
	connBefore := co.connectings.Load()
	// Span several 2s TTL windows, publishing throughout.
	sent := 0
	deadline := time.Now().Add(7 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := e.node.Publish(ch, []byte(`{"t":1}`)); err != nil {
			return fail("publish: %v", err)
		}
		sent++
		time.Sleep(400 * time.Millisecond)
	}
	if u := unsubs.Load(); u != 0 {
		return fail("subscription was unsubscribed %d time(s) despite refresh", u)
	}
	if d := co.connectings.Load() - connBefore; d != 0 {
		return fail("connection dropped %d time(s) (code=%d) — sub refresh path broken", d, co.lastDiscode.Load())
	}
	if st := sub.State(); st != cent.SubStateSubscribed {
		return fail("subscription state is %v after the TTL windows", st)
	}
	if !waitFor(5*time.Second, func() bool { return got.Load() >= int64(sent) }) {
		return fail("delivered %d/%d publications across the TTL windows", got.Load(), sent)
	}
	return okf("subscription survived ~3 TTL windows via sub refresh (%d publications delivered)", sent)
}

// ---------------------------------------------------------------------------
// Node-level APIs and payload edge cases.
// ---------------------------------------------------------------------------

// surveyNotify round-trips the node survey and notification control APIs.
func surveyNotify(ctx context.Context, e *env) (string, error) {
	op := "survey-" + newUser("op")
	const concurrent = 16
	var wg sync.WaitGroup
	var bad atomic.Int64
	var lastErr atomic.Value
	for i := 0; i < concurrent; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			data := []byte(fmt.Sprintf(`{"i":%d}`, i))
			sctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			results, err := e.node.Survey(sctx, op, data, "")
			cancel()
			if err != nil {
				bad.Add(1)
				lastErr.Store(err.Error())
				return
			}
			if len(results) != 1 {
				bad.Add(1)
				lastErr.Store(fmt.Sprintf("survey returned %d node results, want 1", len(results)))
				return
			}
			want := fmt.Sprintf(`{"op":%q,"echo":%s}`, op, data)
			for node, res := range results {
				if res.Code != 0 || string(res.Data) != want {
					bad.Add(1)
					lastErr.Store(fmt.Sprintf("node %s returned code=%d data=%s, want %s", node, res.Code, res.Data, want))
				}
			}
		}(i)
	}
	wg.Wait()
	if bad.Load() > 0 {
		return fail("%d/%d surveys wrong (lastErr=%v)", bad.Load(), concurrent, lastErr.Load())
	}

	notifyOp := "notify-" + newUser("op")
	const notifyCount = 10
	for i := 0; i < notifyCount; i++ {
		if err := e.node.Notify(notifyOp, []byte(fmt.Sprintf(`{"n":%d}`, i)), ""); err != nil {
			return fail("notify %d: %v", i, err)
		}
	}
	if !waitFor(10*time.Second, func() bool { return len(notifications.get(notifyOp)) >= notifyCount }) {
		return fail("received %d/%d notifications", len(notifications.get(notifyOp)), notifyCount)
	}
	got := notifications.get(notifyOp)
	sort.Strings(got)
	if len(got) != notifyCount {
		return fail("received %d notifications, want exactly %d", len(got), notifyCount)
	}
	return okf("%d concurrent surveys and %d notifications round-tripped", concurrent, notifyCount)
}

func notificationsFor(op string) []string { return notifications.get(op) }

// asyncSendEcho hammers the asynchronous message path in both directions: the
// client sends, the server echoes with Client.Send, the client must see them all.
func asyncSendEcho(ctx context.Context, e *env) (string, error) {
	const n = 5000
	var mu sync.Mutex
	seen := map[string]int{}
	co, err := dial(e.wsURL, newUser("asend"), withSetup(func(cl *cent.Client) {
		cl.OnMessage(func(ev cent.MessageEvent) {
			mu.Lock()
			seen[string(ev.Data)]++
			mu.Unlock()
		})
	}))
	if err != nil {
		return fail("dial: %v", err)
	}
	defer co.c.Close()

	var wg sync.WaitGroup
	var sendErrs atomic.Int64
	var lastErr atomic.Value
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			sctx, cancel := context.WithTimeout(ctx, 15*time.Second)
			defer cancel()
			if err := co.c.Send(sctx, []byte(fmt.Sprintf(`{"m":%d}`, i))); err != nil {
				sendErrs.Add(1)
				lastErr.Store(err.Error())
			}
		}(i)
	}
	wg.Wait()
	if sendErrs.Load() > 0 {
		return fail("%d/%d async sends failed (lastErr=%v)", sendErrs.Load(), n, lastErr.Load())
	}
	waitFor(15*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(seen) >= n
	})
	mu.Lock()
	defer mu.Unlock()
	if len(seen) != n {
		return fail("received %d/%d distinct echoes", len(seen), n)
	}
	for i := 0; i < n; i++ {
		key := fmt.Sprintf(`{"m":%d}`, i)
		if c := seen[key]; c != 1 {
			return fail("echo %s delivered %d times, want 1", key, c)
		}
	}
	return okf("%d async messages echoed back exactly once each", n)
}

// largePayloads pushes payloads from a kilobyte up to a megabyte through fanout,
// history and delta, checking every byte survives the round trip.
func largePayloads(ctx context.Context, e *env) (string, error) {
	co, err := dial(e.wsURL, newUser("large"))
	if err != nil {
		return fail("dial: %v", err)
	}
	defer co.c.Close()

	sizes := []int{1 << 10, 32 << 10, 256 << 10, 1 << 20}
	pad := func(n int) string { return strings.Repeat("x", n) }

	// Fanout + history path.
	ch := newChannel(chRecov, "large")
	r := &recorder{}
	if _, _, err := subscribe(co.c, ch, cent.SubscriptionConfig{Positioned: true}, func(s *cent.Subscription) {
		s.OnPublication(func(ev cent.PublicationEvent) { r.add(ev.Offset, ev.Data) })
	}); err != nil {
		return fail("subscribe: %v", err)
	}
	var want []string
	for i, size := range sizes {
		payload := fmt.Sprintf(`{"i":%d,"pad":%q}`, i, pad(size))
		want = append(want, payload)
		if _, err := e.node.Publish(ch, []byte(payload), centrifuge.WithHistory(50, time.Minute)); err != nil {
			return fail("publish %d bytes: %v", size, err)
		}
	}
	if !waitFor(20*time.Second, func() bool { return r.len() >= len(sizes) }) {
		return fail("received %d/%d large publications", r.len(), len(sizes))
	}
	_, got := r.snapshot()
	for i := range want {
		if got[i] != want[i] {
			return fail("payload %d (%d bytes) corrupted: got %d bytes", i, len(want[i]), len(got[i]))
		}
	}
	hist, err := e.node.History(ch, centrifuge.WithLimit(50))
	if err != nil {
		return fail("history: %v", err)
	}
	if len(hist.Publications) != len(sizes) {
		return fail("history holds %d large publications, want %d", len(hist.Publications), len(sizes))
	}
	for i, p := range hist.Publications {
		if string(p.Data) != want[i] {
			return fail("history payload %d differs from what was published", i)
		}
	}

	// Delta path with a large shared body: the delta must reconstruct exactly.
	dch := newChannel(chDelta, "large")
	dr := &recorder{}
	if _, _, err := subscribe(co.c, dch, cent.SubscriptionConfig{
		Delta: cent.DeltaTypeFossil, Recoverable: true, Positioned: true,
	}, func(s *cent.Subscription) {
		s.OnPublication(func(ev cent.PublicationEvent) { dr.add(ev.Offset, ev.Data) })
	}); err != nil {
		return fail("delta subscribe: %v", err)
	}
	body := pad(256 << 10)
	var dwant []string
	for i := 0; i < 5; i++ {
		payload := fmt.Sprintf(`{"body":%q,"seq":%d}`, body, i)
		dwant = append(dwant, payload)
		if _, err := e.node.Publish(dch, []byte(payload),
			centrifuge.WithDelta(true), centrifuge.WithHistory(20, time.Minute)); err != nil {
			return fail("delta publish %d: %v", i, err)
		}
	}
	if !waitFor(20*time.Second, func() bool { return dr.len() >= len(dwant) }) {
		return fail("received %d/%d large delta publications", dr.len(), len(dwant))
	}
	_, dgot := dr.snapshot()
	for i := range dwant {
		if dgot[i] != dwant[i] {
			return fail("large delta payload %d reconstructed incorrectly (%d vs %d bytes)", i, len(dgot[i]), len(dwant[i]))
		}
	}

	// Client-initiated large publish (incoming frame path).
	cpayload := fmt.Sprintf(`{"client":%q}`, pad(512<<10))
	if _, err := co.c.Publish(ctx, ch, []byte(cpayload)); err != nil {
		return fail("client publish of %d bytes: %v", len(cpayload), err)
	}
	if !waitFor(15*time.Second, func() bool { return r.len() >= len(sizes)+1 }) {
		return fail("client-published large payload never came back")
	}
	_, got = r.snapshot()
	if got[len(got)-1] != cpayload {
		return fail("client-published large payload came back corrupted (%d vs %d bytes)", len(got[len(got)-1]), len(cpayload))
	}
	return okf("payloads up to %d KiB survived fanout, history, delta and client publish", (1<<20)/1024)
}

// manyChannelsOneConn puts a thousand channels on a single connection, fans a
// publication into each, and then drains them all.
func manyChannelsOneConn(ctx context.Context, e *env) (string, error) {
	co, err := dial(e.wsURL, newUser("manych"))
	if err != nil {
		return fail("dial: %v", err)
	}
	defer co.c.Close()

	const n = 2000
	channels := make([]string, n)
	counters := make([]*atomic.Int64, n)
	var subscribed atomic.Int64
	for i := 0; i < n; i++ {
		ch := newChannel(chPlain, "manych")
		channels[i] = ch
		cnt := &atomic.Int64{}
		counters[i] = cnt
		sub, err := co.c.NewSubscription(ch)
		if err != nil {
			return fail("new sub %d: %v", i, err)
		}
		sub.OnPublication(func(cent.PublicationEvent) { cnt.Add(1) })
		sub.OnSubscribed(func(cent.SubscribedEvent) { subscribed.Add(1) })
		if err := sub.Subscribe(); err != nil {
			return fail("subscribe %d: %v", i, err)
		}
	}
	if got, ok := waitCount(30*time.Second, n, subscribed.Load); !ok {
		return fail("%d/%d channels subscribed", got, n)
	}
	for i, ch := range channels {
		if _, err := e.node.Publish(ch, []byte(fmt.Sprintf(`{"i":%d}`, i))); err != nil {
			return fail("publish to %s: %v", ch, err)
		}
	}
	if !waitFor(30*time.Second, func() bool {
		for _, c := range counters {
			if c.Load() < 1 {
				return false
			}
		}
		return true
	}) {
		missing := 0
		for _, c := range counters {
			if c.Load() < 1 {
				missing++
			}
		}
		return fail("%d/%d channels never received their publication", missing, n)
	}
	for _, c := range counters {
		if c.Load() != 1 {
			return fail("a channel received %d publications, want exactly 1", c.Load())
		}
	}
	// Drain: unsubscribe everything and check the hub lets go.
	for _, ch := range channels {
		if s, ok := co.c.GetSubscription(ch); ok {
			_ = s.Unsubscribe()
			_ = co.c.RemoveSubscription(s)
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
		return fail("channel %s still has subscribers after unsubscribing all", stuck)
	}
	return okf("%d channels on one connection: all delivered, all drained", n)
}

// errorPaths checks application-level errors surface with the right protocol
// code and leave the connection fully usable.
func errorPaths(ctx context.Context, e *env) (string, error) {
	co, err := dial(e.wsURL, newUser("errs"))
	if err != nil {
		return fail("dial: %v", err)
	}
	defer co.c.Close()

	const codePermissionDenied, codeInternal, codeNotAvailable = 103, 100, 108

	// Subscribe denied.
	denyCh := newChannel(chDeny, "errs")
	_, _, serr := subscribe(co.c, denyCh, cent.SubscriptionConfig{}, nil)
	if serr == nil {
		return fail("subscribe to %s succeeded, want permission denied", denyCh)
	}
	if code, ok := errorCode(serr); !ok || code != codePermissionDenied {
		return fail("subscribe error is %v, want code %d", serr, codePermissionDenied)
	}

	checks := []struct {
		name string
		want uint32
		call func() error
	}{
		{"publish to nopub channel", codePermissionDenied, func() error {
			_, err := co.c.Publish(ctx, newChannel(chNoPub, "errs"), []byte(`{}`))
			return err
		}},
		{"rpc denied", codePermissionDenied, func() error {
			_, err := co.c.RPC(ctx, "deny", []byte(`{}`))
			return err
		}},
		{"rpc handler error", codeInternal, func() error {
			_, err := co.c.RPC(ctx, "boom", []byte(`{}`))
			return err
		}},
		{"presence denied", codeNotAvailable, func() error {
			_, err := co.c.Presence(ctx, denyCh)
			return err
		}},
		{"presence stats denied", codeNotAvailable, func() error {
			_, err := co.c.PresenceStats(ctx, denyCh)
			return err
		}},
		{"history denied", codeNotAvailable, func() error {
			_, err := co.c.History(ctx, denyCh)
			return err
		}},
	}
	for _, c := range checks {
		err := c.call()
		if err == nil {
			return fail("%s succeeded, want error %d", c.name, c.want)
		}
		code, ok := errorCode(err)
		if !ok {
			return fail("%s returned non-protocol error %v", c.name, err)
		}
		if code != c.want {
			return fail("%s returned code %d, want %d", c.name, code, c.want)
		}
	}

	// After all those errors the connection must still be healthy.
	if st := co.c.State(); st != cent.StateConnected {
		return fail("connection state is %v after error barrage", st)
	}
	res, err := co.c.RPC(ctx, "echo", []byte(`{"ok":1}`))
	if err != nil {
		return fail("healthy RPC after errors: %v", err)
	}
	if string(res.Data) != `{"method":"echo","data":{"ok":1}}` {
		return fail("unexpected echo after errors: %s", res.Data)
	}
	okCh := newChannel(chRecov, "errs")
	if _, _, err := subscribe(co.c, okCh, cent.SubscriptionConfig{Positioned: true}, nil); err != nil {
		return fail("subscribe after errors: %v", err)
	}
	return okf("%d denied operations returned exact codes; connection stayed usable", len(checks)+1)
}

// recoveryStorm is the hardest recovery test in the suite: a group of
// recoverable subscribers on one busy channel is kicked off the server over and
// over while publications keep flowing. Every client must end up with a stream
// that has no gap and no duplicate — recovery has to bridge every single
// interruption, and the offsets it delivers must splice cleanly onto the live
// ones it saw before and after.
func recoveryStorm(ctx context.Context, e *env) (string, error) {
	const clients, kicks = 10, 8
	ch := newChannel(chRecov, "storm")
	// Server-side subscription declared at connect time, so every reconnect
	// re-establishes it and the recovery happens inside the connect handshake.
	user := userSSub + ch
	// History has to comfortably outlive a reconnect window, otherwise a legit
	// recovery failure would look like a bug.
	const historySize = 20000

	recs := make([]*recorder, clients)
	conns := make([]*conn, clients)
	defer func() { closeAll(conns) }()
	for i := 0; i < clients; i++ {
		r := &recorder{}
		recs[i] = r
		co, err := dial(e.wsURL, user, withSetup(func(cl *cent.Client) {
			cl.OnPublication(func(ev cent.ServerPublicationEvent) { r.add(ev.Offset, ev.Data) })
		}))
		if err != nil {
			return fail("client %d dial: %v", i, err)
		}
		conns[i] = co
	}
	stormCtx, cancel := context.WithCancel(ctx)
	var published atomic.Int64
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for stormCtx.Err() == nil {
			n := published.Load() + 1
			if _, err := e.node.Publish(ch, []byte(fmt.Sprintf(`{"n":%d}`, n)),
				centrifuge.WithHistory(historySize, time.Minute)); err != nil {
				return
			}
			published.Store(n)
			time.Sleep(time.Millisecond)
		}
	}()

	for k := 0; k < kicks; k++ {
		time.Sleep(500 * time.Millisecond)
		if err := e.node.Disconnect(user, centrifuge.WithCustomDisconnect(centrifuge.DisconnectForceReconnect)); err != nil {
			cancel()
			wg.Wait()
			return fail("kick %d: %v", k, err)
		}
	}
	time.Sleep(time.Second)
	cancel()
	wg.Wait()

	total := published.Load()
	if total < 1000 {
		return fail("only %d publications made it through the storm", total)
	}
	// Let the last recoveries settle.
	waitFor(20*time.Second, func() bool {
		for _, r := range recs {
			if int64(r.len()) < total {
				return false
			}
		}
		return true
	})

	minSeen := int64(total)
	for i, r := range recs {
		offs, _ := r.snapshot()
		if len(offs) == 0 {
			return fail("client %d received nothing", i)
		}
		for j := 1; j < len(offs); j++ {
			if offs[j] == offs[j-1] {
				return fail("client %d got offset %d twice across a reconnect", i, offs[j])
			}
			if offs[j] != offs[j-1]+1 {
				return fail("client %d lost publications across a reconnect: offset %d followed by %d",
					i, offs[j-1], offs[j])
			}
		}
		if offs[0] != 1 {
			return fail("client %d started at offset %d, want 1", i, offs[0])
		}
		if int64(len(offs)) < minSeen {
			minSeen = int64(len(offs))
		}
	}
	// A short tail can still be in flight, but nothing may be missing in between.
	if minSeen < total-int64(20) {
		return fail("slowest client received %d of %d publications", minSeen, total)
	}
	return okf("%d clients survived %d forced reconnects over %d publications with zero gaps or duplicates",
		clients, kicks, total)
}
