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

// connectChurn opens and closes many connections; the suite's final no-leak
// check verifies the hub returns to its baseline afterwards.
func connectChurn(ctx context.Context, e *env) (string, error) {
	// Sustained connect/close churn for the whole load window. A global
	// NumClients leak assertion can't run here because scenarios share the node
	// in parallel; the suite performs one authoritative no-leak check at the end.
	const workers = 32
	var wg sync.WaitGroup
	var cycles, failed atomic.Int64
	var lastErr atomic.Value
	deadline := time.Now().Add(loadDur)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(deadline) && ctx.Err() == nil {
				co, err := dial(e.wsURL, newUser("churn"))
				if err != nil {
					failed.Add(1)
					lastErr.Store(err.Error())
					continue
				}
				co.c.Close()
				cycles.Add(1)
			}
		}()
	}
	wg.Wait()
	if failed.Load() > 0 {
		return fail("%d connections failed to establish (%d succeeded, lastErr=%v)", failed.Load(), cycles.Load(), lastErr.Load())
	}
	return okf("%d connect/close cycles over %s (%d workers)", cycles.Load(), loadDur, workers)
}

// pubsubFanout verifies every subscriber receives every publication exactly once
// and in offset order.
func pubsubFanout(ctx context.Context, e *env) (string, error) {
	const subs, msgs = 40, 1000
	ch := newChannel(chRecov, "fanout")
	recvs := make([]*recorder, subs)
	clients := make([]*conn, subs)
	defer func() { closeAll(clients) }()
	for i := 0; i < subs; i++ {
		co, err := dial(e.wsURL, newUser("fanout"))
		if err != nil {
			return fail("subscriber %d dial: %v", i, err)
		}
		clients[i] = co
		r := &recorder{}
		recvs[i] = r
		_, _, err = subscribe(co.c, ch, cent.SubscriptionConfig{Positioned: true}, func(s *cent.Subscription) {
			s.OnPublication(func(ev cent.PublicationEvent) { r.add(ev.Offset, ev.Data) })
		})
		if err != nil {
			return fail("subscriber %d subscribe: %v", i, err)
		}
	}
	for i := 0; i < msgs; i++ {
		if _, err := e.node.Publish(ch, []byte(fmt.Sprintf(`{"n":%d}`, i)), centrifuge.WithHistory(500, time.Minute)); err != nil {
			return fail("publish %d: %v", i, err)
		}
	}
	waitFor(15*time.Second, func() bool {
		for _, r := range recvs {
			if r.len() < msgs {
				return false
			}
		}
		return true
	})
	for i, r := range recvs {
		offs, _ := r.snapshot()
		if len(offs) != msgs {
			return fail("subscriber %d received %d/%d publications", i, len(offs), msgs)
		}
		for j := 1; j < len(offs); j++ {
			if offs[j] <= offs[j-1] {
				return fail("subscriber %d out-of-order/duplicate offsets: %d then %d", i, offs[j-1], offs[j])
			}
		}
	}
	return okf("%d subscribers × %d msgs delivered in order", subs, msgs)
}

// subscribeChurn subscribes and unsubscribes many channels; verifies event
// symmetry and that the hub retains no subscribers afterward.
func subscribeChurn(ctx context.Context, e *env) (string, error) {
	co, err := dial(e.wsURL, newUser("subchurn"))
	if err != nil {
		return fail("dial: %v", err)
	}
	defer co.c.Close()
	const n = 1000
	var subCount, unsubCount atomic.Int64
	channels := make([]string, n)
	for i := 0; i < n; i++ {
		ch := newChannel(chPres, "subchurn")
		channels[i] = ch
		sub, err := co.c.NewSubscription(ch)
		if err != nil {
			return fail("new sub %d: %v", i, err)
		}
		sub.OnSubscribed(func(cent.SubscribedEvent) { subCount.Add(1) })
		sub.OnUnsubscribed(func(cent.UnsubscribedEvent) { unsubCount.Add(1) })
		if err := sub.Subscribe(); err != nil {
			return fail("subscribe %d: %v", i, err)
		}
	}
	if got, ok := waitCount(25*time.Second, n, subCount.Load); !ok {
		return fail("only %d/%d subscribed", got, n)
	}
	for _, ch := range channels {
		if s, ok := co.c.GetSubscription(ch); ok {
			_ = s.Unsubscribe()
			_ = co.c.RemoveSubscription(s)
		}
	}
	if got, ok := waitCount(25*time.Second, n, unsubCount.Load); !ok {
		return fail("only %d/%d unsubscribed", got, n)
	}
	// Hub must retain no subscribers for these channels. Hub removal is async
	// relative to the client's OnUnsubscribed, so poll with a bounded wait.
	var remaining string
	waitFor(10*time.Second, func() bool {
		remaining = ""
		for _, ch := range channels {
			if got := e.node.Hub().NumSubscribers(ch); got != 0 {
				remaining = fmt.Sprintf("%s (%d subs)", ch, got)
				return false
			}
		}
		return true
	})
	if remaining != "" {
		return fail("channel %s still subscribed after unsubscribe+wait", remaining)
	}
	return okf("%d subscribe/unsubscribe cycles, clean hub", n)
}

// historyAPI validates history retrieval and since-position (the recovery data
// path): full history has the right count/order, and since-offset returns the
// correct suffix.
func historyAPI(ctx context.Context, e *env) (string, error) {
	co, err := dial(e.wsURL, newUser("hist"))
	if err != nil {
		return fail("dial: %v", err)
	}
	defer co.c.Close()
	ch := newChannel(chRecov, "hist")
	const n = 500
	for i := 0; i < n; i++ {
		if _, err := e.node.Publish(ch, []byte(fmt.Sprintf(`{"n":%d}`, i)), centrifuge.WithHistory(500, time.Minute)); err != nil {
			return fail("publish %d: %v", i, err)
		}
	}
	res, err := co.c.History(ctx, ch, cent.WithHistoryLimit(1000))
	if err != nil {
		return fail("history: %v", err)
	}
	if len(res.Publications) != n {
		return fail("history returned %d/%d publications", len(res.Publications), n)
	}
	pubs := append([]cent.Publication(nil), res.Publications...)
	sort.Slice(pubs, func(i, j int) bool { return pubs[i].Offset < pubs[j].Offset })
	for i := 1; i < len(pubs); i++ {
		if pubs[i].Offset != pubs[i-1].Offset+1 {
			return fail("history offset gap: %d then %d", pubs[i-1].Offset, pubs[i].Offset)
		}
	}
	// Since the 10th offset → expect the remaining n-10.
	since := &cent.StreamPosition{Offset: pubs[9].Offset, Epoch: res.Epoch}
	res2, err := co.c.History(ctx, ch, cent.WithHistorySince(since), cent.WithHistoryLimit(1000))
	if err != nil {
		return fail("history-since: %v", err)
	}
	if len(res2.Publications) != n-10 {
		return fail("history-since returned %d, want %d", len(res2.Publications), n-10)
	}
	return okf("history %d pubs contiguous; since-offset suffix correct", n)
}

// positioning verifies live publications arrive with strictly increasing
// offsets and matching stream position.
func positioning(ctx context.Context, e *env) (string, error) {
	co, err := dial(e.wsURL, newUser("pos"))
	if err != nil {
		return fail("dial: %v", err)
	}
	defer co.c.Close()
	ch := newChannel(chRecov, "pos")
	r := &recorder{}
	_, _, err = subscribe(co.c, ch, cent.SubscriptionConfig{Positioned: true, Recoverable: true}, func(s *cent.Subscription) {
		s.OnPublication(func(ev cent.PublicationEvent) { r.add(ev.Offset, ev.Data) })
	})
	if err != nil {
		return fail("subscribe: %v", err)
	}
	const n = 1000
	for i := 0; i < n; i++ {
		if _, err := e.node.Publish(ch, []byte(`{"x":1}`), centrifuge.WithHistory(500, time.Minute)); err != nil {
			return fail("publish %d: %v", i, err)
		}
	}
	waitFor(10*time.Second, func() bool { return r.len() >= n })
	got, _ := r.snapshot()
	if len(got) != n {
		return fail("received %d/%d positioned publications", len(got), n)
	}
	for i := 1; i < len(got); i++ {
		if got[i] != got[i-1]+1 {
			return fail("position gap: offset %d then %d", got[i-1], got[i])
		}
	}
	return okf("%d publications, contiguous offsets %d..%d", n, got[0], got[len(got)-1])
}

// recoveryReconnect forces a reconnect mid-stream and verifies the client
// recovers the publications it missed while disconnected — no gap, no dupe.
func recoveryReconnect(ctx context.Context, e *env) (string, error) {
	user := newUser("recov")
	co, err := dial(e.wsURL, user)
	if err != nil {
		return fail("dial: %v", err)
	}
	defer co.c.Close()
	ch := newChannel(chRecov, "recov")

	var mu sync.Mutex
	seen := map[uint64]int{}
	// Manage the subscription directly: register handlers before Subscribe and
	// use a single OnSubscribed to signal both the initial subscribe and the
	// post-reconnect resubscribe (buffered so both are observable).
	subscribed := make(chan struct{}, 8)
	sub, err := co.c.NewSubscription(ch, cent.SubscriptionConfig{Recoverable: true, Positioned: true})
	if err != nil {
		return fail("new subscription: %v", err)
	}
	sub.OnPublication(func(ev cent.PublicationEvent) {
		mu.Lock()
		seen[ev.Offset]++
		mu.Unlock()
	})
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

	// Publish a batch live.
	for i := 0; i < 10; i++ {
		if _, err := e.node.Publish(ch, []byte(`{"p":1}`), centrifuge.WithHistory(200, time.Minute)); err != nil {
			return fail("live publish %d: %v", i, err)
		}
	}
	if !waitFor(5*time.Second, func() bool { mu.Lock(); defer mu.Unlock(); return len(seen) == 10 }) {
		return fail("only %d/10 live publications delivered before reconnect", len(seen))
	}

	// Force a reconnect; while the client is away, publish more (to be recovered).
	if err := forceReconnect(e.node, co, user); err != nil {
		return fail("%v", err)
	}
	for i := 0; i < 15; i++ {
		if _, err := e.node.Publish(ch, []byte(`{"p":2}`), centrifuge.WithHistory(200, time.Minute)); err != nil {
			return fail("gap publish %d: %v", i, err)
		}
	}
	// Wait for resubscribe (recovery) to complete.
	select {
	case <-subscribed:
	case <-time.After(15 * time.Second):
		return fail("client did not resubscribe after forced reconnect")
	}
	waitFor(8*time.Second, func() bool { mu.Lock(); defer mu.Unlock(); return len(seen) >= 25 })
	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 25 {
		return fail("recovered %d/25 distinct offsets after reconnect", len(seen))
	}
	for off, c := range seen {
		if c != 1 {
			return fail("offset %d delivered %d times (duplicate across recovery)", off, c)
		}
	}
	return okf("recovered all 25 offsets across forced reconnect, no dupes")
}

// deltaCorrectness subscribes with fossil delta and verifies the client
// reconstructs every payload the server publishes with delta enabled.
func deltaCorrectness(ctx context.Context, e *env) (string, error) {
	co, err := dial(e.wsURL, newUser("delta"))
	if err != nil {
		return fail("dial: %v", err)
	}
	defer co.c.Close()
	ch := newChannel(chDelta, "delta")
	r := &recorder{}
	_, _, err = subscribe(co.c, ch, cent.SubscriptionConfig{
		Delta: cent.DeltaTypeFossil, Recoverable: true, Positioned: true,
	}, func(s *cent.Subscription) {
		s.OnPublication(func(ev cent.PublicationEvent) { r.add(ev.Offset, ev.Data) })
	})
	if err != nil {
		return fail("subscribe: %v", err)
	}
	const n = 400
	want := make([]string, n)
	body := strings.Repeat("shared-body-", 10)
	for i := 0; i < n; i++ {
		payload := fmt.Sprintf(`{"body":%q,"seq":%d}`, body, i)
		want[i] = payload
		if _, err := e.node.Publish(ch, []byte(payload),
			centrifuge.WithDelta(true), centrifuge.WithHistory(200, time.Minute)); err != nil {
			return fail("publish %d: %v", i, err)
		}
	}
	waitFor(10*time.Second, func() bool { return r.len() >= n })
	_, got := r.snapshot()
	if len(got) != n {
		return fail("received %d/%d delta publications", len(got), n)
	}
	for i := 0; i < n; i++ {
		if got[i] != want[i] {
			return fail("delta reconstruction mismatch at %d:\n  want %s\n  got  %s", i, want[i], got[i])
		}
	}
	return okf("%d fossil-delta publications reconstructed exactly", n)
}

// presenceJoinLeave verifies presence membership, presence stats, and join/leave
// push events across several subscribers.
func presenceJoinLeave(ctx context.Context, e *env) (string, error) {
	ch := newChannel(chPres, "pres")
	const n = 25
	clients := make([]*conn, n)
	defer func() { closeAll(clients) }()
	// Watcher subscribes first and counts join/leave events.
	watcher, err := dial(e.wsURL, newUser("pres-w"))
	if err != nil {
		return fail("watcher dial: %v", err)
	}
	defer watcher.c.Close()
	var joins, leaves atomic.Int64
	_, _, err = subscribe(watcher.c, ch, cent.SubscriptionConfig{JoinLeave: true}, func(s *cent.Subscription) {
		s.OnJoin(func(cent.JoinEvent) { joins.Add(1) })
		s.OnLeave(func(cent.LeaveEvent) { leaves.Add(1) })
	})
	if err != nil {
		return fail("watcher subscribe: %v", err)
	}

	for i := 0; i < n; i++ {
		co, err := dial(e.wsURL, newUser("pres-m"))
		if err != nil {
			return fail("member %d dial: %v", i, err)
		}
		clients[i] = co
		if _, _, err := subscribe(co.c, ch, cent.SubscriptionConfig{JoinLeave: true}, nil); err != nil {
			return fail("member %d subscribe: %v", i, err)
		}
	}
	// Presence should list watcher + n members.
	wantMembers := n + 1
	var pres cent.PresenceResult
	waitFor(8*time.Second, func() bool {
		pres, err = watcher.c.Presence(ctx, ch)
		return err == nil && len(pres.Clients) == wantMembers
	})
	if len(pres.Clients) != wantMembers {
		return fail("presence has %d clients, want %d", len(pres.Clients), wantMembers)
	}
	stats, err := watcher.c.PresenceStats(ctx, ch)
	if err != nil {
		return fail("presence stats: %v", err)
	}
	if stats.NumClients != wantMembers {
		return fail("presence stats NumClients=%d, want %d", stats.NumClients, wantMembers)
	}
	// Members leave; watcher must observe leaves and presence must shrink.
	closeAll(clients)
	waitFor(10*time.Second, func() bool {
		p, err := watcher.c.Presence(ctx, ch)
		return err == nil && len(p.Clients) == 1
	})
	p, _ := watcher.c.Presence(ctx, ch)
	if len(p.Clients) != 1 {
		return fail("presence after leave has %d clients, want 1", len(p.Clients))
	}
	if joins.Load() < int64(n) {
		return fail("observed %d join events, want >= %d", joins.Load(), n)
	}
	if leaves.Load() < int64(n) {
		return fail("observed %d leave events, want >= %d", leaves.Load(), n)
	}
	return okf("presence/stats correct; %d joins, %d leaves observed", joins.Load(), leaves.Load())
}

// tagsFilter verifies both client-requested and server-enforced tags filters
// deliver only matching publications.
func tagsFilter(ctx context.Context, e *env) (string, error) {
	co, err := dial(e.wsURL, newUser("tags"))
	if err != nil {
		return fail("dial: %v", err)
	}
	defer co.c.Close()

	chClient := newChannel(chTags, "tags")
	var mu sync.Mutex
	var clientTeams []string
	_, _, err = subscribe(co.c, chClient, cent.SubscriptionConfig{TagsFilter: cent.FilterEq("team", "eng")}, func(s *cent.Subscription) {
		s.OnPublication(func(ev cent.PublicationEvent) {
			mu.Lock()
			clientTeams = append(clientTeams, ev.Tags["team"])
			mu.Unlock()
		})
	})
	if err != nil {
		return fail("client-filter subscribe: %v", err)
	}

	// Server-enforced filter channel.
	chServer := newChannel(chSTags, "tags")
	var serverTeams []string
	_, _, err = subscribe(co.c, chServer, cent.SubscriptionConfig{}, func(s *cent.Subscription) {
		s.OnPublication(func(ev cent.PublicationEvent) {
			mu.Lock()
			serverTeams = append(serverTeams, ev.Tags["team"])
			mu.Unlock()
		})
	})
	if err != nil {
		return fail("server-filter subscribe: %v", err)
	}

	teams := []string{"eng", "sales", "eng", "ops", "eng", "sales"}
	for _, ch := range []string{chClient, chServer} {
		for i, tm := range teams {
			if _, err := e.node.Publish(ch, []byte(fmt.Sprintf(`{"i":%d}`, i)), centrifuge.WithTags(map[string]string{"team": tm})); err != nil {
				return fail("publish to %s: %v", ch, err)
			}
		}
	}
	wantEng := 3
	waitFor(8*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(clientTeams) >= wantEng && len(serverTeams) >= wantEng
	})
	time.Sleep(300 * time.Millisecond) // let any wrongly-delivered pubs arrive too.
	mu.Lock()
	defer mu.Unlock()
	for label, teamsGot := range map[string][]string{"client": clientTeams, "server": serverTeams} {
		if len(teamsGot) != wantEng {
			return fail("%s-filter delivered %d publications, want %d (%v)", label, len(teamsGot), wantEng, teamsGot)
		}
		for _, tm := range teamsGot {
			if tm != "eng" {
				return fail("%s-filter leaked a publication with team=%q", label, tm)
			}
		}
	}
	return okf("client & server tags filters each delivered only the %d eng publications", wantEng)
}

// rpcConcurrent fires many concurrent RPCs and verifies each echo is correct.
func rpcConcurrent(ctx context.Context, e *env) (string, error) {
	co, err := dial(e.wsURL, newUser("rpc"))
	if err != nil {
		return fail("dial: %v", err)
	}
	defer co.c.Close()
	const n = 5000
	var wg sync.WaitGroup
	var bad atomic.Int64
	var lastErr atomic.Value
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			data := []byte(fmt.Sprintf(`{"i":%d}`, i))
			cctx, cancel := context.WithTimeout(ctx, 15*time.Second)
			res, err := co.c.RPC(cctx, "echo", data)
			cancel()
			if err != nil {
				bad.Add(1)
				lastErr.Store(err.Error())
				return
			}
			want := fmt.Sprintf(`{"method":"echo","data":%s}`, data)
			if string(res.Data) != want {
				bad.Add(1)
				lastErr.Store("echo mismatch: " + string(res.Data))
			}
		}(i)
	}
	wg.Wait()
	if bad.Load() > 0 {
		return fail("%d/%d RPC round-trips wrong or failed (lastErr=%v)", bad.Load(), n, lastErr.Load())
	}
	return okf("%d concurrent RPC round-trips correct", n)
}

// clientPublishFanout verifies client-initiated publishes reach other
// subscribers.
func clientPublishFanout(ctx context.Context, e *env) (string, error) {
	ch := newChannel(chPres, "cpub")
	pub, err := dial(e.wsURL, newUser("cpub-p"))
	if err != nil {
		return fail("publisher dial: %v", err)
	}
	defer pub.c.Close()
	const subs, msgs = 16, 400
	counters := make([]*atomic.Int64, subs)
	clients := make([]*conn, subs)
	defer func() { closeAll(clients) }()
	for i := 0; i < subs; i++ {
		co, err := dial(e.wsURL, newUser("cpub-s"))
		if err != nil {
			return fail("subscriber %d dial: %v", i, err)
		}
		clients[i] = co
		cnt := &atomic.Int64{}
		counters[i] = cnt
		if _, _, err := subscribe(co.c, ch, cent.SubscriptionConfig{}, func(s *cent.Subscription) {
			s.OnPublication(func(cent.PublicationEvent) { cnt.Add(1) })
		}); err != nil {
			return fail("subscriber %d subscribe: %v", i, err)
		}
	}
	// Publisher subscribes too (needed to publish into channel it participates in).
	psub, _, err := subscribe(pub.c, ch, cent.SubscriptionConfig{}, nil)
	if err != nil {
		return fail("publisher subscribe: %v", err)
	}
	for i := 0; i < msgs; i++ {
		if _, err := psub.Publish(ctx, []byte(fmt.Sprintf(`{"n":%d}`, i))); err != nil {
			return fail("client publish %d: %v", i, err)
		}
	}
	waitFor(10*time.Second, func() bool {
		for _, c := range counters {
			if c.Load() < msgs {
				return false
			}
		}
		return true
	})
	for i, c := range counters {
		if c.Load() != msgs {
			return fail("subscriber %d received %d/%d client-published msgs", i, c.Load(), msgs)
		}
	}
	return okf("%d subscribers each received %d client publishes", subs, msgs)
}

// sameConnConcurrency hammers a single connection with concurrent publishes,
// history, presence, RPC and send calls — stressing per-connection locking and
// the writer/queue.
func sameConnConcurrency(ctx context.Context, e *env) (string, error) {
	co, err := dial(e.wsURL, newUser("sameconn"))
	if err != nil {
		return fail("dial: %v", err)
	}
	defer co.c.Close()
	ch := newChannel(chRecov, "sameconn")
	if _, _, err := subscribe(co.c, ch, cent.SubscriptionConfig{Recoverable: true, Positioned: true}, nil); err != nil {
		return fail("subscribe: %v", err)
	}
	const workers, each = 64, 150
	var wg sync.WaitGroup
	var errs atomic.Int64
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < each; i++ {
				if ctx.Err() != nil {
					return
				}
				cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
				var e2 error
				switch (w + i) % 4 {
				case 0:
					_, e2 = co.c.Publish(cctx, ch, []byte(`{"a":1}`))
				case 1:
					_, e2 = co.c.History(cctx, ch, cent.WithHistoryLimit(1))
				case 2:
					_, e2 = co.c.Presence(cctx, ch)
				case 3:
					_, e2 = co.c.RPC(cctx, "m", []byte(`{}`))
				}
				cancel()
				if e2 != nil {
					errs.Add(1)
					co.lastErr.Store(e2.Error())
				}
			}
		}(w)
	}
	wg.Wait()
	if errs.Load() > 0 {
		return fail("%d/%d concurrent ops failed (lastErr=%v)", errs.Load(), workers*each, co.errString())
	}
	return okf("%d concurrent ops on one connection, all succeeded", workers*each)
}

// refreshConnection verifies a short-TTL connection stays alive via refresh.
func refreshConnection(ctx context.Context, e *env) (string, error) {
	co, err := dial(e.wsURL, userRefresh+newUser("r"))
	if err != nil {
		return fail("dial: %v", err)
	}
	defer co.c.Close()
	ch := newChannel(chRecov, "refresh")
	if _, _, err := subscribe(co.c, ch, cent.SubscriptionConfig{Recoverable: true, Positioned: true}, nil); err != nil {
		return fail("subscribe: %v", err)
	}
	// Live well past the 2s TTL; the connection must refresh and stay usable.
	connBefore := co.connectings.Load()
	time.Sleep(6 * time.Second)
	if co.c.State() != cent.StateConnected {
		return fail("connection not connected after TTL window (state=%v, lastCode=%d)", co.c.State(), co.lastDiscode.Load())
	}
	if d := co.connectings.Load() - connBefore; d != 0 {
		return fail("connection dropped %d time(s) (code=%d) while being refreshed", d, co.lastDiscode.Load())
	}
	if _, err := co.c.Publish(ctx, ch, []byte(`{"alive":1}`)); err != nil {
		return fail("publish after refresh window: %v", err)
	}
	return okf("connection survived TTL window via refresh with zero disconnects")
}

// pingPong verifies the server↔client keepalive: the connection is pinged every
// two seconds and must stay alive and functional across many cycles. A broken pong
// path drops the connection server-side (PongTimeout); a broken ping path drops
// it client-side (MaxServerPingDelay) — either way this scenario catches it.
func pingPong(ctx context.Context, e *env) (string, error) {
	co, err := dial(e.wsURL, userPing+newUser("pp"))
	if err != nil {
		return fail("dial: %v", err)
	}
	defer co.c.Close()
	ch := newChannel(chRecov, "pp")
	var got atomic.Int64
	_, _, err = subscribe(co.c, ch, cent.SubscriptionConfig{}, func(s *cent.Subscription) {
		s.OnPublication(func(cent.PublicationEvent) { got.Add(1) })
	})
	if err != nil {
		return fail("subscribe: %v", err)
	}
	connBefore := co.connectings.Load()
	// Span several server ping cycles; publish along the way to confirm the
	// connection stays fully functional under the ping/pong traffic.
	const window = 7 * time.Second
	sent := 0
	deadline := time.Now().Add(window)
	for time.Now().Before(deadline) {
		if st := co.c.State(); st != cent.StateConnected {
			return fail("connection dropped during ping window (state=%v, code=%d, lastErr=%v)",
				st, co.lastDiscode.Load(), co.errString())
		}
		pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		_, perr := co.c.Publish(pctx, ch, []byte(`{"ping":1}`))
		cancel()
		if perr != nil {
			return fail("publish during ping window: %v", perr)
		}
		sent++
		time.Sleep(700 * time.Millisecond)
	}
	if d := co.connectings.Load() - connBefore; d != 0 {
		return fail("connection dropped %d time(s) (code=%d) during the ping/pong window", d, co.lastDiscode.Load())
	}
	waitFor(3*time.Second, func() bool { return got.Load() >= int64(sent) })
	if int64(sent) != got.Load() {
		return fail("delivered %d/%d publications during ping window", got.Load(), sent)
	}
	return okf("stayed alive & functional across %s of 2s pings (%d pubs round-tripped)", window, sent)
}

// mixedChaos runs a burst of many clients doing random operations and asserts
// the server neither errors nor leaks connections.
func mixedChaos(ctx context.Context, e *env) (string, error) {
	const clients = 80
	var wg sync.WaitGroup
	var opErrs atomic.Int64
	var lastChaosErr atomic.Value
	chaosCtx, cancel := context.WithTimeout(ctx, loadDur)
	defer cancel()
	for c := 0; c < clients; c++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			co, err := dial(e.wsURL, newUser("chaos"))
			if err != nil {
				opErrs.Add(1)
				lastChaosErr.Store("dial: " + err.Error())
				return
			}
			defer co.c.Close()
			ch := fmt.Sprintf("%schaos-%d", chRecov, id%5) // shared channels → real fanout
			_, _, err = subscribe(co.c, ch, cent.SubscriptionConfig{Recoverable: true, Positioned: true, JoinLeave: true}, func(s *cent.Subscription) {
				s.OnPublication(func(cent.PublicationEvent) {})
			})
			if err != nil {
				opErrs.Add(1)
				lastChaosErr.Store("subscribe: " + err.Error())
				return
			}
			rng := newRand(suiteSeed + int64(id))
			for chaosCtx.Err() == nil {
				octx, oc := context.WithTimeout(chaosCtx, 8*time.Second)
				var e2 error
				switch rng.Intn(5) {
				case 0:
					_, e2 = co.c.Publish(octx, ch, []byte(`{"c":1}`))
				case 1:
					_, e2 = co.c.History(octx, ch, cent.WithHistoryLimit(5))
				case 2:
					_, e2 = co.c.Presence(octx, ch)
				case 3:
					_, e2 = co.c.RPC(octx, "chaos", []byte(`{}`))
				case 4:
					_, e2 = co.c.PresenceStats(octx, ch)
				}
				oc()
				if e2 != nil && chaosCtx.Err() == nil {
					opErrs.Add(1)
					lastChaosErr.Store(e2.Error())
				}
				time.Sleep(time.Duration(rng.Intn(15)) * time.Millisecond)
			}
		}(c)
	}
	wg.Wait()
	if opErrs.Load() > 0 {
		return fail("%d operations errored during chaos (lastErr=%v)", opErrs.Load(), lastChaosErr.Load())
	}
	return okf("%d chaos clients ran mixed ops with no errors", clients)
}
