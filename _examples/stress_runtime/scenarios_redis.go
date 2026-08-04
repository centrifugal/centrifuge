package main

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/centrifugal/centrifuge"
	cent "github.com/centrifugal/centrifuge-go"
)

// ---------------------------------------------------------------------------
// Redis engine scenarios.
//
// These run against two nodes that share one Redis instance and key prefix, so
// every one of them exercises code the memory-backed nodes never touch: the Lua
// history/presence scripts, PUB/SUB fanout between processes, and the control
// plane that carries survey, notification, subscribe, unsubscribe and disconnect
// between nodes. Wherever it matters the publisher and the subscriber sit on
// *different* nodes, because that is the path that can actually break.
//
// All fanout assertions use positioned/recoverable channels on purpose: Redis
// PUB/SUB is at-most-once, and the server's own gap detection plus recovery is
// exactly what these scenarios are checking. A gap that survives to the client
// is a real defect, not flakiness.
// ---------------------------------------------------------------------------

// redisHistory is the publish option set used by the Redis scenarios: a stream
// big enough that a reconnect never falls out of it, with a short TTL so the
// suite leaves nothing behind in Redis.
func redisHistory(size int) centrifuge.PublishOption {
	return centrifuge.WithHistory(size, 2*time.Minute)
}

// redisClusterView checks the Redis control plane: both nodes see each other,
// a survey reaches both, and a notification is delivered to both.
func redisClusterView(ctx context.Context, e *env) (string, error) {
	infoA, err := e.redisA.Info()
	if err != nil {
		return fail("info on node A: %v", err)
	}
	infoB, err := e.redisB.Info()
	if err != nil {
		return fail("info on node B: %v", err)
	}
	if len(infoA.Nodes) != 2 || len(infoB.Nodes) != 2 {
		return fail("node A sees %d nodes, node B sees %d, want 2 each", len(infoA.Nodes), len(infoB.Nodes))
	}

	// A survey has to reach both nodes and come back with both answers.
	op := "redis-survey-" + newUser("op")
	const concurrent = 8
	var wg sync.WaitGroup
	var bad atomic.Int64
	var lastErr atomic.Value
	for i := 0; i < concurrent; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			from := e.redisA
			if i%2 == 1 {
				from = e.redisB
			}
			data := []byte(fmt.Sprintf(`{"i":%d}`, i))
			sctx, cancel := context.WithTimeout(ctx, 15*time.Second)
			results, err := from.Survey(sctx, op, data, "")
			cancel()
			if err != nil {
				bad.Add(1)
				lastErr.Store(err.Error())
				return
			}
			if len(results) != 2 {
				bad.Add(1)
				lastErr.Store(fmt.Sprintf("survey returned %d node results, want 2", len(results)))
				return
			}
			want := fmt.Sprintf(`{"op":%q,"echo":%s}`, op, data)
			for node, res := range results {
				if res.Code != 0 || string(res.Data) != want {
					bad.Add(1)
					lastErr.Store(fmt.Sprintf("node %s returned code=%d data=%s", node, res.Code, res.Data))
				}
			}
		}(i)
	}
	wg.Wait()
	if bad.Load() > 0 {
		return fail("%d/%d cross-node surveys wrong (lastErr=%v)", bad.Load(), concurrent, lastErr.Load())
	}

	// A notification must land on both nodes.
	notifyOp := "redis-notify-" + newUser("op")
	if err := e.redisA.Notify(notifyOp, []byte(`{"n":1}`), ""); err != nil {
		return fail("notify: %v", err)
	}
	if !waitFor(15*time.Second, func() bool { return len(notifications.get(notifyOp)) >= 2 }) {
		return fail("notification delivered to %d nodes, want 2", len(notifications.get(notifyOp)))
	}

	// Targeting a single node must reach only that node.
	targeted := "redis-notify-one-" + newUser("op")
	if err := e.redisA.Notify(targeted, []byte(`{"n":2}`), e.redisB.ID()); err != nil {
		return fail("targeted notify: %v", err)
	}
	if !waitFor(15*time.Second, func() bool { return len(notifications.get(targeted)) >= 1 }) {
		return fail("targeted notification never arrived")
	}
	time.Sleep(300 * time.Millisecond)
	if got := len(notifications.get(targeted)); got != 1 {
		return fail("targeted notification delivered %d times, want 1", got)
	}
	return okf("2-node Redis cluster: %d surveys answered by both nodes, broadcast and targeted notifications correct", concurrent)
}

// redisPubSubFanout publishes on one node and checks subscribers on *both*
// nodes receive everything, in order, exactly once.
func redisPubSubFanout(ctx context.Context, e *env) (string, error) {
	const subs, msgs = 20, 500
	ch := newChannel(chRecov, "redis-fanout")
	recs := make([]*recorder, subs)
	clients := make([]*conn, subs)
	defer func() { closeAll(clients) }()
	for i := 0; i < subs; i++ {
		url := e.redisAWS
		if i%2 == 1 {
			url = e.redisBWS
		}
		co, err := dial(url, newUser("redis-fanout"))
		if err != nil {
			return fail("subscriber %d dial: %v", i, err)
		}
		clients[i] = co
		r := &recorder{}
		recs[i] = r
		if _, _, err := subscribe(co.c, ch, cent.SubscriptionConfig{Positioned: true, Recoverable: true}, func(s *cent.Subscription) {
			s.OnPublication(func(ev cent.PublicationEvent) { r.add(ev.Offset, ev.Data) })
		}); err != nil {
			return fail("subscriber %d subscribe: %v", i, err)
		}
	}
	for i := 0; i < msgs; i++ {
		if _, err := e.redisA.Publish(ch, []byte(fmt.Sprintf(`{"n":%d}`, i)), redisHistory(msgs+100)); err != nil {
			return fail("publish %d: %v", i, err)
		}
	}
	waitFor(30*time.Second, func() bool {
		for _, r := range recs {
			if r.len() < msgs {
				return false
			}
		}
		return true
	})
	for i, r := range recs {
		offs, _ := r.snapshot()
		if len(offs) != msgs {
			return fail("subscriber %d (node %s) received %d/%d publications",
				i, nodeLabel(i), len(offs), msgs)
		}
		for j := 1; j < len(offs); j++ {
			if offs[j] != offs[j-1]+1 {
				return fail("subscriber %d saw a gap or duplicate over Redis PUB/SUB: offset %d then %d",
					i, offs[j-1], offs[j])
			}
		}
	}
	return okf("%d subscribers split across both Redis nodes each received %d publications in order", subs, msgs)
}

func nodeLabel(i int) string {
	if i%2 == 1 {
		return "B"
	}
	return "A"
}

// redisCrossNodeRecovery disconnects a client through the *other* node's control
// channel and verifies it recovers everything it missed from the Redis stream.
func redisCrossNodeRecovery(ctx context.Context, e *env) (string, error) {
	user := newUser("redis-recov")
	// Client lives on node B; every server-side action is issued from node A.
	co, err := dial(e.redisBWS, user)
	if err != nil {
		return fail("dial: %v", err)
	}
	defer co.c.Close()
	ch := newChannel(chRecov, "redis-recov")

	var mu sync.Mutex
	seen := map[uint64]int{}
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
	case <-time.After(15 * time.Second):
		return fail("initial subscribe did not complete")
	}

	const live, gap = 20, 30
	for i := 0; i < live; i++ {
		if _, err := e.redisA.Publish(ch, []byte(`{"p":1}`), redisHistory(500)); err != nil {
			return fail("live publish %d: %v", i, err)
		}
	}
	if !waitFor(15*time.Second, func() bool { mu.Lock(); defer mu.Unlock(); return len(seen) == live }) {
		mu.Lock()
		got := len(seen)
		mu.Unlock()
		return fail("only %d/%d live publications crossed nodes", got, live)
	}

	// Force the reconnect from node A — the disconnect travels over the Redis
	// control channel to node B where the connection actually lives.
	if err := forceReconnect(e.redisA, co, user); err != nil {
		return fail("cross-node %v", err)
	}
	for i := 0; i < gap; i++ {
		if _, err := e.redisA.Publish(ch, []byte(`{"p":2}`), redisHistory(500)); err != nil {
			return fail("gap publish %d: %v", i, err)
		}
	}
	select {
	case <-subscribed:
	case <-time.After(20 * time.Second):
		return fail("client did not resubscribe after the cross-node disconnect")
	}
	waitFor(20*time.Second, func() bool { mu.Lock(); defer mu.Unlock(); return len(seen) >= live+gap })
	mu.Lock()
	defer mu.Unlock()
	if len(seen) != live+gap {
		return fail("recovered %d/%d distinct offsets from the Redis stream", len(seen), live+gap)
	}
	for off, c := range seen {
		if c != 1 {
			return fail("offset %d delivered %d times across Redis recovery", off, c)
		}
	}
	return okf("cross-node disconnect: all %d offsets recovered from the Redis stream, no duplicates", live+gap)
}

// redisCrossNodeControl issues every user-targeted server API from node A
// against a connection that lives on node B.
func redisCrossNodeControl(ctx context.Context, e *env) (string, error) {
	user := newUser("redis-ctl")
	ch := newChannel(chRecov, "redis-ctl")
	var subs, unsubs, pubs atomic.Int64
	co, err := dial(e.redisBWS, user, withSetup(func(cl *cent.Client) {
		cl.OnSubscribed(func(cent.ServerSubscribedEvent) { subs.Add(1) })
		cl.OnUnsubscribed(func(cent.ServerUnsubscribedEvent) { unsubs.Add(1) })
		cl.OnPublication(func(cent.ServerPublicationEvent) { pubs.Add(1) })
	}))
	if err != nil {
		return fail("dial: %v", err)
	}
	defer co.c.Close()

	// Subscribe from the other node.
	if err := e.redisA.Subscribe(user, ch,
		centrifuge.WithRecovery(true), centrifuge.WithPositioning(true)); err != nil {
		return fail("cross-node subscribe: %v", err)
	}
	if got, ok := waitCount(15*time.Second, 1, subs.Load); !ok {
		return fail("cross-node subscribe never reached the connection (%d events)", got)
	}
	const msgs = 30
	for i := 0; i < msgs; i++ {
		if _, err := e.redisA.Publish(ch, []byte(fmt.Sprintf(`{"n":%d}`, i)), redisHistory(200)); err != nil {
			return fail("publish %d: %v", i, err)
		}
	}
	if got, ok := waitCount(20*time.Second, msgs, pubs.Load); !ok {
		return fail("%d/%d publications reached the server-side subscription", got, msgs)
	}

	// Refresh from the other node must not disturb the connection.
	connBefore := co.connectings.Load()
	if err := e.redisA.Refresh(user); err != nil {
		return fail("cross-node refresh: %v", err)
	}
	// Unsubscribe from the other node.
	if err := e.redisA.Unsubscribe(user, ch); err != nil {
		return fail("cross-node unsubscribe: %v", err)
	}
	if got, ok := waitCount(15*time.Second, 1, unsubs.Load); !ok {
		return fail("cross-node unsubscribe never reached the connection (%d events)", got)
	}
	if !waitFor(15*time.Second, func() bool { return e.redisB.Hub().NumSubscribers(ch) == 0 }) {
		return fail("node B hub still has %d subscribers for %s", e.redisB.Hub().NumSubscribers(ch), ch)
	}
	if d := co.connectings.Load() - connBefore; d != 0 {
		return fail("connection dropped %d time(s) (code=%d) during cross-node refresh/unsubscribe", d, co.lastDiscode.Load())
	}

	// Finally disconnect it from the other node.
	if err := e.redisA.Disconnect(user, centrifuge.WithCustomDisconnect(centrifuge.DisconnectForceNoReconnect)); err != nil {
		return fail("cross-node disconnect: %v", err)
	}
	if !waitFor(15*time.Second, func() bool { return co.c.State() != cent.StateConnected }) {
		return fail("connection survived the cross-node disconnect")
	}
	if code := co.lastDiscode.Load(); code != 3503 {
		return fail("disconnected with code %d, want 3503", code)
	}
	return okf("subscribe, %d publications, refresh, unsubscribe and disconnect all crossed nodes correctly", msgs)
}

// redisPresenceCrossNode checks presence and join/leave are consistent when the
// members are spread across both nodes.
func redisPresenceCrossNode(ctx context.Context, e *env) (string, error) {
	ch := newChannel(chPres, "redis-pres")
	const members = 20

	// Watcher on node A counts join/leave for members that connect to both nodes.
	watcher, err := dial(e.redisAWS, newUser("redis-pres-w"))
	if err != nil {
		return fail("watcher dial: %v", err)
	}
	defer watcher.c.Close()
	var joins, leaves atomic.Int64
	if _, _, err := subscribe(watcher.c, ch, cent.SubscriptionConfig{JoinLeave: true}, func(s *cent.Subscription) {
		s.OnJoin(func(cent.JoinEvent) { joins.Add(1) })
		s.OnLeave(func(cent.LeaveEvent) { leaves.Add(1) })
	}); err != nil {
		return fail("watcher subscribe: %v", err)
	}

	clients := make([]*conn, members)
	defer func() { closeAll(clients) }()
	for i := 0; i < members; i++ {
		url := e.redisAWS
		if i%2 == 1 {
			url = e.redisBWS
		}
		co, err := dial(url, newUser("redis-pres-m"))
		if err != nil {
			return fail("member %d dial: %v", i, err)
		}
		clients[i] = co
		if _, _, err := subscribe(co.c, ch, cent.SubscriptionConfig{JoinLeave: true}, nil); err != nil {
			return fail("member %d subscribe: %v", i, err)
		}
	}

	want := members + 1
	// Presence must agree from either node.
	if !waitFor(20*time.Second, func() bool {
		pa, errA := e.redisA.Presence(ch)
		pb, errB := e.redisB.Presence(ch)
		return errA == nil && errB == nil && len(pa.Presence) == want && len(pb.Presence) == want
	}) {
		pa, _ := e.redisA.Presence(ch)
		pb, _ := e.redisB.Presence(ch)
		return fail("presence is %d on node A and %d on node B, want %d on both", len(pa.Presence), len(pb.Presence), want)
	}
	statsA, err := e.redisA.PresenceStats(ch)
	if err != nil {
		return fail("presence stats on node A: %v", err)
	}
	statsB, err := e.redisB.PresenceStats(ch)
	if err != nil {
		return fail("presence stats on node B: %v", err)
	}
	if statsA.NumClients != want || statsB.NumClients != want {
		return fail("presence stats are %d on A and %d on B, want %d", statsA.NumClients, statsB.NumClients, want)
	}
	// Every member is a distinct user, so users == clients.
	if statsA.NumUsers != want {
		return fail("presence stats NumUsers=%d, want %d", statsA.NumUsers, want)
	}

	closeAll(clients)
	clients = nil
	if !waitFor(25*time.Second, func() bool {
		pa, errA := e.redisA.Presence(ch)
		return errA == nil && len(pa.Presence) == 1
	}) {
		pa, _ := e.redisA.Presence(ch)
		return fail("presence after leave has %d clients, want 1", len(pa.Presence))
	}
	if got, ok := waitCount(20*time.Second, members, joins.Load); !ok {
		return fail("watcher observed %d join events, want %d", got, members)
	}
	if got, ok := waitCount(20*time.Second, members, leaves.Load); !ok {
		return fail("watcher observed %d leave events, want %d", got, members)
	}
	return okf("presence agreed across both nodes for %d members; %d joins and %d leaves crossed nodes",
		want, joins.Load(), leaves.Load())
}

// redisDeltaRecovery runs fossil delta over the Redis broker — the previous
// publication comes back from the Redis stream, not from process memory — and
// then recovers across a reconnect.
func redisDeltaRecovery(ctx context.Context, e *env) (string, error) {
	user := newUser("redis-delta")
	co, err := dial(e.redisBWS, user)
	if err != nil {
		return fail("dial: %v", err)
	}
	defer co.c.Close()
	ch := newChannel(chDelta, "redis-delta")

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
	case <-time.After(15 * time.Second):
		return fail("initial subscribe did not complete")
	}

	body := strings.Repeat("redis-δelta-body-", 30)
	payload := func(i int) string { return fmt.Sprintf(`{"body":%q,"seq":%d}`, body, i) }
	const live, gap = 100, 50
	var want []string
	for i := 0; i < live; i++ {
		want = append(want, payload(i))
		if _, err := e.redisA.Publish(ch, []byte(payload(i)), centrifuge.WithDelta(true), redisHistory(500)); err != nil {
			return fail("live publish %d: %v", i, err)
		}
	}
	if !waitFor(25*time.Second, func() bool { return r.len() >= live }) {
		return fail("received %d/%d live delta publications over Redis", r.len(), live)
	}
	if err := forceReconnect(e.redisA, co, user); err != nil {
		return fail("%v", err)
	}
	for i := live; i < live+gap; i++ {
		want = append(want, payload(i))
		if _, err := e.redisA.Publish(ch, []byte(payload(i)), centrifuge.WithDelta(true), redisHistory(500)); err != nil {
			return fail("gap publish %d: %v", i, err)
		}
	}
	select {
	case <-subscribed:
	case <-time.After(20 * time.Second):
		return fail("did not resubscribe after the forced reconnect")
	}
	if !waitFor(25*time.Second, func() bool { return r.len() >= len(want) }) {
		return fail("received %d/%d delta publications after recovery", r.len(), len(want))
	}
	_, got := r.snapshot()
	if len(got) != len(want) {
		return fail("received %d delta publications, want exactly %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			return fail("Redis delta mismatch at %d:\n  want %s\n  got  %s", i, want[i], got[i])
		}
	}
	return okf("fossil delta over Redis: %d live + %d recovered reconstructed exactly across nodes", live, gap)
}

// redisConcurrentPublishOffsets hammers one Redis stream from both nodes at once
// and checks the Lua add script keeps offsets unique and contiguous, and that a
// subscriber on a third connection sees every publication exactly once in order.
func redisConcurrentPublishOffsets(ctx context.Context, e *env) (string, error) {
	co, err := dial(e.redisBWS, newUser("redis-offsets"))
	if err != nil {
		return fail("dial: %v", err)
	}
	defer co.c.Close()
	ch := newChannel(chRecov, "redis-offsets")

	const workers, each = 24, 40
	const total = workers * each

	r := &recorder{}
	if _, _, err := subscribe(co.c, ch, cent.SubscriptionConfig{Recoverable: true, Positioned: true}, func(s *cent.Subscription) {
		s.OnPublication(func(ev cent.PublicationEvent) { r.add(ev.Offset, ev.Data) })
	}); err != nil {
		return fail("subscribe: %v", err)
	}

	var wg sync.WaitGroup
	var pubErrs atomic.Int64
	var lastErr atomic.Value
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			// Half the writers go through each node — same Redis stream, two
			// different processes' worth of concurrency.
			node := e.redisA
			if w%2 == 1 {
				node = e.redisB
			}
			for i := 0; i < each; i++ {
				if _, err := node.Publish(ch, []byte(fmt.Sprintf(`{"w":%d,"i":%d}`, w, i)), redisHistory(total+100)); err != nil {
					pubErrs.Add(1)
					lastErr.Store(err.Error())
				}
			}
		}(w)
	}
	wg.Wait()
	if pubErrs.Load() > 0 {
		return fail("%d concurrent Redis publishes failed (lastErr=%v)", pubErrs.Load(), lastErr.Load())
	}

	hist, err := e.redisA.History(ch, centrifuge.WithLimit(total+100))
	if err != nil {
		return fail("history: %v", err)
	}
	if len(hist.Publications) != total {
		return fail("Redis history holds %d publications, want %d", len(hist.Publications), total)
	}
	for i := 1; i < len(hist.Publications); i++ {
		if hist.Publications[i].Offset != hist.Publications[i-1].Offset+1 {
			return fail("Redis history offset gap under concurrency: %d then %d",
				hist.Publications[i-1].Offset, hist.Publications[i].Offset)
		}
	}
	if !waitFor(30*time.Second, func() bool { return r.len() >= total }) {
		return fail("subscriber received %d/%d publications", r.len(), total)
	}
	offs, _ := r.snapshot()
	if len(offs) != total {
		return fail("subscriber received %d publications, want exactly %d", len(offs), total)
	}
	for i := 1; i < len(offs); i++ {
		if offs[i] != offs[i-1]+1 {
			return fail("delivery gap or duplicate under cross-node concurrency: offset %d then %d", offs[i-1], offs[i])
		}
	}
	return okf("%d publishes racing from both nodes into one Redis stream: offsets unique, contiguous, delivered in order", total)
}

// redisIdempotentPublish checks the idempotency key is honoured inside Redis —
// including when the duplicates arrive from two different nodes at once.
func redisIdempotentPublish(ctx context.Context, e *env) (string, error) {
	ch := newChannel(chRecov, "redis-idem")
	key := "redis-key-" + newUser("k")

	const dupes = 40
	var wg sync.WaitGroup
	var accepted, pubErrs atomic.Int64
	positions := make([]centrifuge.StreamPosition, dupes)
	for i := 0; i < dupes; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			node := e.redisA
			if i%2 == 1 {
				node = e.redisB
			}
			res, err := node.Publish(ch, []byte(`{"idem":"redis"}`), redisHistory(100), centrifuge.WithIdempotencyKey(key))
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
		return fail("%d idempotent publishes errored", pubErrs.Load())
	}
	if accepted.Load() != 1 {
		return fail("%d of %d cross-node duplicates were accepted, want exactly 1", accepted.Load(), dupes)
	}
	for i, p := range positions {
		if p != positions[0] {
			return fail("duplicate %d returned position %+v, want %+v", i, p, positions[0])
		}
	}
	hist, err := e.redisB.History(ch, centrifuge.WithLimit(100))
	if err != nil {
		return fail("history: %v", err)
	}
	if len(hist.Publications) != 1 {
		return fail("Redis history holds %d publications, want 1", len(hist.Publications))
	}
	return okf("%d duplicates racing from both nodes under one idempotency key collapsed to a single publication", dupes)
}

// redisCacheRecovery checks RecoveryModeCache against the Redis stream.
func redisCacheRecovery(ctx context.Context, e *env) (string, error) {
	user := newUser("redis-cache")
	ch := newChannel(chCache, "redis-cache")
	const backlog = 15
	for i := 0; i < backlog; i++ {
		if _, err := e.redisA.Publish(ch, []byte(fmt.Sprintf(`{"pre":%d}`, i)), redisHistory(100)); err != nil {
			return fail("backlog publish %d: %v", i, err)
		}
	}
	co, err := dial(e.redisBWS, user)
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
	case <-time.After(15 * time.Second):
		return fail("subscribe did not complete")
	}
	if !waitFor(10*time.Second, func() bool { return r.len() >= 1 }) {
		return fail("cache recovery over Redis delivered nothing on subscribe")
	}
	time.Sleep(400 * time.Millisecond)
	_, got := r.snapshot()
	if len(got) != 1 {
		return fail("cache subscribe delivered %d publications, want exactly 1 (%v)", len(got), got)
	}
	if want := fmt.Sprintf(`{"pre":%d}`, backlog-1); got[0] != want {
		return fail("cache subscribe delivered %s, want the latest %s", got[0], want)
	}

	if err := forceReconnect(e.redisA, co, user); err != nil {
		return fail("%v", err)
	}
	const away = 9
	for i := 0; i < away; i++ {
		if _, err := e.redisA.Publish(ch, []byte(fmt.Sprintf(`{"away":%d}`, i)), redisHistory(100)); err != nil {
			return fail("away publish %d: %v", i, err)
		}
	}
	select {
	case <-subscribed:
	case <-time.After(20 * time.Second):
		return fail("did not resubscribe after the forced reconnect")
	}
	if !waitFor(15*time.Second, func() bool { return r.len() >= 2 }) {
		return fail("cache recovery delivered nothing after the reconnect")
	}
	time.Sleep(400 * time.Millisecond)
	_, got = r.snapshot()
	if len(got) != 2 {
		return fail("cache recovery delivered %d publications total, want 2 (backlog replayed?): %v", len(got), got)
	}
	if want := fmt.Sprintf(`{"away":%d}`, away-1); got[1] != want {
		return fail("cache recovery delivered %s, want the latest %s", got[1], want)
	}
	return okf("Redis cache mode: latest-only on subscribe and on recovery (2 publications for %d published)", backlog+away)
}

// redisHistoryPagination walks a Redis stream forwards page by page and
// backwards in one shot.
func redisHistoryPagination(ctx context.Context, e *env) (string, error) {
	ch := newChannel(chRecov, "redis-histpage")
	const n, page = 600, 50
	for i := 0; i < n; i++ {
		if _, err := e.redisA.Publish(ch, []byte(fmt.Sprintf(`{"n":%d}`, i)), redisHistory(n+50)); err != nil {
			return fail("publish %d: %v", i, err)
		}
	}
	// Read the top from the *other* node.
	top, err := e.redisB.History(ch)
	if err != nil {
		return fail("history top: %v", err)
	}
	if top.Offset != uint64(n) {
		return fail("stream top offset is %d, want %d", top.Offset, n)
	}

	var forward []uint64
	since := &centrifuge.StreamPosition{Offset: 0, Epoch: top.Epoch}
	for pages := 0; ; pages++ {
		if pages > n/page+5 {
			return fail("forward pagination did not terminate after %d pages", pages)
		}
		res, err := e.redisB.History(ch, centrifuge.WithSince(since), centrifuge.WithLimit(page))
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

	rev, err := e.redisB.History(ch, centrifuge.WithLimit(n+50), centrifuge.WithReverse(true))
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
	tail, err := e.redisB.History(ch, centrifuge.WithSince(&centrifuge.StreamPosition{Offset: top.Offset, Epoch: top.Epoch}), centrifuge.WithLimit(page))
	if err != nil {
		return fail("tail history: %v", err)
	}
	if len(tail.Publications) != 0 {
		return fail("since-top returned %d publications, want 0", len(tail.Publications))
	}
	return okf("%d publications written on node A paginated and reverse-read from node B", n)
}

// redisRecoveryStorm is the Redis version of the reconnect storm: subscribers
// spread across both nodes are kicked repeatedly while publications keep
// flowing, and every one of them must end with an unbroken stream.
func redisRecoveryStorm(ctx context.Context, e *env) (string, error) {
	const clients, kicks = 8, 6
	ch := newChannel(chRecov, "redis-storm")
	user := userSSub + ch
	const historySize = 20000

	recs := make([]*recorder, clients)
	conns := make([]*conn, clients)
	defer func() { closeAll(conns) }()
	for i := 0; i < clients; i++ {
		r := &recorder{}
		recs[i] = r
		url := e.redisAWS
		if i%2 == 1 {
			url = e.redisBWS
		}
		co, err := dial(url, user, withSetup(func(cl *cent.Client) {
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
			if _, err := e.redisA.Publish(ch, []byte(fmt.Sprintf(`{"n":%d}`, n)), redisHistory(historySize)); err != nil {
				return
			}
			published.Store(n)
			time.Sleep(2 * time.Millisecond)
		}
	}()

	for k := 0; k < kicks; k++ {
		time.Sleep(600 * time.Millisecond)
		// Kick from whichever node the connection is *not* on half the time.
		node := e.redisA
		if k%2 == 1 {
			node = e.redisB
		}
		if err := node.Disconnect(user, centrifuge.WithCustomDisconnect(centrifuge.DisconnectForceReconnect)); err != nil {
			cancel()
			wg.Wait()
			return fail("kick %d: %v", k, err)
		}
	}
	time.Sleep(1500 * time.Millisecond)
	cancel()
	wg.Wait()

	total := published.Load()
	if total < 200 {
		return fail("only %d publications made it through the storm", total)
	}
	waitFor(30*time.Second, func() bool {
		for _, r := range recs {
			if int64(r.len()) < total {
				return false
			}
		}
		return true
	})

	minSeen := total
	for i, r := range recs {
		offs, _ := r.snapshot()
		if len(offs) == 0 {
			return fail("client %d received nothing", i)
		}
		if offs[0] != 1 {
			return fail("client %d started at offset %d, want 1", i, offs[0])
		}
		for j := 1; j < len(offs); j++ {
			if offs[j] == offs[j-1] {
				return fail("client %d got offset %d twice across a Redis reconnect", i, offs[j])
			}
			if offs[j] != offs[j-1]+1 {
				return fail("client %d lost publications across a Redis reconnect: offset %d followed by %d",
					i, offs[j-1], offs[j])
			}
		}
		if int64(len(offs)) < minSeen {
			minSeen = int64(len(offs))
		}
	}
	if minSeen < total-20 {
		return fail("slowest client received %d of %d publications", minSeen, total)
	}
	return okf("%d clients across both nodes survived %d forced reconnects over %d publications with zero gaps or duplicates",
		clients, kicks, total)
}

// redisChaos keeps both Redis nodes busy with random mixed operations for the
// whole load window.
func redisChaos(ctx context.Context, e *env) (string, error) {
	const clients = 40
	var wg sync.WaitGroup
	var opErrs atomic.Int64
	var ops atomic.Int64
	var lastErr atomic.Value
	chaosCtx, cancel := context.WithTimeout(ctx, loadDur)
	defer cancel()
	for c := 0; c < clients; c++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			url := e.redisAWS
			if id%2 == 1 {
				url = e.redisBWS
			}
			co, err := dial(url, newUser("redis-chaos"))
			if err != nil {
				opErrs.Add(1)
				lastErr.Store("dial: " + err.Error())
				return
			}
			defer co.c.Close()
			// Shared channels, so both nodes fan out to each other over Redis.
			ch := fmt.Sprintf("%sredis-chaos-%d", chRecov, id%5)
			if _, _, err := subscribe(co.c, ch, cent.SubscriptionConfig{Recoverable: true, Positioned: true, JoinLeave: true}, func(s *cent.Subscription) {
				s.OnPublication(func(cent.PublicationEvent) {})
			}); err != nil {
				opErrs.Add(1)
				lastErr.Store("subscribe: " + err.Error())
				return
			}
			rng := newRand(suiteSeed + int64(1000+id))
			for chaosCtx.Err() == nil {
				octx, oc := context.WithTimeout(chaosCtx, 10*time.Second)
				var e2 error
				switch rng.Intn(5) {
				case 0:
					_, e2 = co.c.Publish(octx, ch, []byte(`{"c":1}`))
				case 1:
					_, e2 = co.c.History(octx, ch, cent.WithHistoryLimit(5))
				case 2:
					_, e2 = co.c.Presence(octx, ch)
				case 3:
					_, e2 = co.c.RPC(octx, "redis-chaos", []byte(`{}`))
				case 4:
					_, e2 = co.c.PresenceStats(octx, ch)
				}
				oc()
				ops.Add(1)
				if e2 != nil && chaosCtx.Err() == nil {
					opErrs.Add(1)
					lastErr.Store(e2.Error())
				}
				time.Sleep(time.Duration(rng.Intn(20)) * time.Millisecond)
			}
		}(c)
	}
	wg.Wait()
	if opErrs.Load() > 0 {
		return fail("%d of %d operations errored during Redis chaos (lastErr=%v)", opErrs.Load(), ops.Load(), lastErr.Load())
	}
	return okf("%d chaos clients across both Redis nodes ran %d mixed ops with no errors", clients, ops.Load())
}
