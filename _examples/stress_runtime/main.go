// Command stress_runtime is a self-contained runtime stress/regression tool for
// the centrifuge server library. It boots an in-process Node exercising every
// major feature (recovery, positioning, delta, presence, join/leave, tags
// filters, RPC, client publish, refresh, concurrent per-connection ops) and
// then hammers it with real centrifuge-go clients over WebSocket.
//
// Each feature is a self-checking scenario with hard invariants (exact message
// counts, ordering, recovered ranges, delta reconstruction, presence sets, no
// leaked connections/subscriptions). Scenarios run in parallel and the whole
// suite is bounded to well under a minute. On any violation the tool prints
// exactly which scenario failed and why, and exits non-zero — so a green run is
// strong evidence the runtime is not broken.
//
// It runs hard: the sustained scenarios churn hundreds of thousands of
// connections and run tens of concurrent clients for ~18s, yet the whole suite
// stays well under a minute because scenarios run in parallel.
//
//	go run .                     // full suite (~18s)
//	go run -race .               // race-check the in-process server under load
//	go run . -only ping_pong     // one scenario
//	go run . -v                  // log server-side disconnects
//	go run . -d 60s              // change the overall suite deadline
package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/centrifugal/centrifuge"
	cent "github.com/centrifugal/centrifuge-go"
)

// ---------------------------------------------------------------------------
// Server: one Node wired for the full feature matrix.
// ---------------------------------------------------------------------------

// subOptions returns SubscribeOptions derived from a channel-name prefix, so a
// single OnSubscribe handler can serve every feature the scenarios need.
func subOptions(channel string) centrifuge.SubscribeOptions {
	o := centrifuge.SubscribeOptions{}
	switch {
	case strings.HasPrefix(channel, "recov:"), strings.HasPrefix(channel, "delta:"):
		o.EnableRecovery = true
		o.EnablePositioning = true
	case strings.HasPrefix(channel, "pos:"):
		o.EnablePositioning = true
	case strings.HasPrefix(channel, "stags:"):
		o.ServerTagsFilter = &centrifuge.FilterNode{Key: "team", Cmp: "eq", Val: "eng"}
	case strings.HasPrefix(channel, "tags:"):
		o.AllowTagsFilter = true
	}
	if strings.HasPrefix(channel, "delta:") {
		o.AllowedDeltaTypes = []centrifuge.DeltaType{centrifuge.DeltaTypeFossil}
	}
	// Presence + join/leave everywhere they don't conflict — cheap and lets any
	// channel be inspected. (Kept off delta channels to avoid mixing concerns.)
	if !strings.HasPrefix(channel, "delta:") {
		o.EmitPresence = true
		o.EmitJoinLeave = true
		o.PushJoinLeave = true
	}
	return o
}

func buildNode() (*centrifuge.Node, error) {
	node, err := centrifuge.New(centrifuge.Config{
		LogLevel: centrifuge.LogLevelError,
		// Generous limits so bursty concurrent scenarios don't trip slow-client
		// disconnects or the default per-client channel cap under heavy load (this
		// is a stress harness, not a production sizing).
		ClientQueueMaxSize: 128 * 1024 * 1024,
		ClientChannelLimit: 100000,
	})
	if err != nil {
		return nil, err
	}
	broker, err := centrifuge.NewMemoryBroker(node, centrifuge.MemoryBrokerConfig{})
	if err != nil {
		return nil, err
	}
	node.SetBroker(broker)
	pm, err := centrifuge.NewMemoryPresenceManager(node, centrifuge.MemoryPresenceManagerConfig{})
	if err != nil {
		return nil, err
	}
	node.SetPresenceManager(pm)

	node.OnConnecting(func(ctx context.Context, e centrifuge.ConnectEvent) (centrifuge.ConnectReply, error) {
		user := e.Token // the harness carries the user id in the token field.
		if user == "" {
			user = "anon"
		}
		cred := &centrifuge.Credentials{UserID: user}
		// Users whose id starts with "refresh:" get a short TTL to exercise the
		// connection-refresh path (client must refresh to stay connected).
		if strings.HasPrefix(user, "refresh:") {
			cred.ExpireAt = time.Now().Add(2 * time.Second).Unix()
		}
		reply := centrifuge.ConnectReply{Credentials: cred}
		// "ping:" users get a fast per-connection ping so the ping/pong scenario
		// can observe many cycles in a short window. If pongs weren't processed the
		// server would drop the connection after PongTimeout.
		if strings.HasPrefix(user, "ping:") {
			reply.PingPongConfig = &centrifuge.PingPongConfig{
				PingInterval: 500 * time.Millisecond,
				PongTimeout:  2 * time.Second,
			}
		}
		return reply, nil
	})

	node.OnConnect(func(client *centrifuge.Client) {
		client.OnSubscribe(func(e centrifuge.SubscribeEvent, cb centrifuge.SubscribeCallback) {
			cb(centrifuge.SubscribeReply{Options: subOptions(e.Channel)}, nil)
		})
		client.OnPublish(func(e centrifuge.PublishEvent, cb centrifuge.PublishCallback) {
			// Allow client publishes (library auto-publishes when Result is nil);
			// give history-backed channels a history so client-published messages
			// are recoverable too.
			var opts centrifuge.PublishOptions
			if strings.HasPrefix(e.Channel, "recov:") || strings.HasPrefix(e.Channel, "delta:") {
				opts.HistorySize = 200
				opts.HistoryTTL = time.Minute
			}
			cb(centrifuge.PublishReply{Options: opts}, nil)
		})
		client.OnRPC(func(e centrifuge.RPCEvent, cb centrifuge.RPCCallback) {
			// Echo method + data back as a valid JSON object (the JSON client
			// requires reply data to be valid JSON).
			data := e.Data
			if len(data) == 0 {
				data = []byte("null")
			}
			cb(centrifuge.RPCReply{Data: []byte(fmt.Sprintf(`{"method":%q,"data":%s}`, e.Method, data))}, nil)
		})
		client.OnMessage(func(e centrifuge.MessageEvent) {
			// Async send: echo back as a message so the client can observe it.
			_ = client.Send(e.Data)
		})
		client.OnPresence(func(e centrifuge.PresenceEvent, cb centrifuge.PresenceCallback) {
			cb(centrifuge.PresenceReply{}, nil)
		})
		client.OnPresenceStats(func(e centrifuge.PresenceStatsEvent, cb centrifuge.PresenceStatsCallback) {
			cb(centrifuge.PresenceStatsReply{}, nil)
		})
		client.OnHistory(func(e centrifuge.HistoryEvent, cb centrifuge.HistoryCallback) {
			cb(centrifuge.HistoryReply{}, nil)
		})
		client.OnRefresh(func(e centrifuge.RefreshEvent, cb centrifuge.RefreshCallback) {
			// Keep refreshing connections alive.
			cb(centrifuge.RefreshReply{ExpireAt: time.Now().Add(2 * time.Second).Unix()}, nil)
		})
		client.OnSubRefresh(func(e centrifuge.SubRefreshEvent, cb centrifuge.SubRefreshCallback) {
			cb(centrifuge.SubRefreshReply{ExpireAt: time.Now().Add(2 * time.Second).Unix()}, nil)
		})
		client.OnDisconnect(func(e centrifuge.DisconnectEvent) {
			if debugDisc && e.Code != centrifuge.DisconnectConnectionClosed.Code {
				fmt.Fprintf(os.Stderr, "[srv-disconnect] user=%s code=%d reason=%q\n", client.UserID(), e.Code, e.Reason)
			}
		})
	})
	if err := node.Run(); err != nil {
		return nil, err
	}
	return node, nil
}

// httptestMux mounts the WebSocket handler for the stress clients.
func httptestMux(node *centrifuge.Node) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/connection/websocket", centrifuge.NewWebsocketHandler(node, centrifuge.WebsocketConfig{
		UseWriteBufferPool: true,
	}))
	return mux
}

// ---------------------------------------------------------------------------
// Client helpers.
// ---------------------------------------------------------------------------

var userSeq atomic.Uint64
var debugDisc bool

// loadDur is how long the sustained-throughput scenarios (connect/chaos) run.
// Kept well under the suite deadline; scenarios run in parallel so the suite
// wall-clock stays near this value.
var loadDur = 18 * time.Second

func newUser(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, userSeq.Add(1))
}

type conn struct {
	c           *cent.Client
	connects    atomic.Int64
	disconnects atomic.Int64
	lastErr     atomic.Value // string
}

// dial connects a JSON client as user and waits for the first Connected event.
func dial(wsURL, user string) (*conn, error) {
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
	if strings.HasPrefix(user, "refresh:") {
		u := user
		cfg.GetToken = func(cent.ConnectionTokenEvent) (string, error) { return u, nil }
	}
	if strings.HasPrefix(user, "ping:") {
		// Tight bound so the client actively fails if the server stops pinging.
		cfg.MaxServerPingDelay = 3 * time.Second
	}
	cl := cent.NewJsonClient(wsURL, cfg)
	co := &conn{c: cl}
	connected := make(chan struct{})
	var once sync.Once
	cl.OnConnected(func(cent.ConnectedEvent) {
		co.connects.Add(1) // first is the initial connect; later ones are reconnects.
		once.Do(func() { close(connected) })
	})
	cl.OnDisconnected(func(cent.DisconnectedEvent) { co.disconnects.Add(1) })
	cl.OnError(func(e cent.ErrorEvent) { co.lastErr.Store(e.Error.Error()) })
	if err := cl.Connect(); err != nil {
		return nil, err
	}
	select {
	case <-connected:
		return co, nil
	case <-time.After(5 * time.Second):
		cl.Close()
		return nil, fmt.Errorf("connect timeout (lastErr=%v)", co.lastErr.Load())
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
	case <-time.After(5 * time.Second):
		return nil, nil, fmt.Errorf("subscribe timeout on %s", channel)
	}
}

// ---------------------------------------------------------------------------
// Scenario framework.
// ---------------------------------------------------------------------------

type env struct {
	wsURL string
	node  *centrifuge.Node
}

type result struct {
	name   string
	pass   bool
	detail string
	dur    time.Duration
}

type scenario struct {
	name string
	run  func(ctx context.Context, e *env) (string, error)
}

func fail(format string, a ...any) (string, error) { return "", fmt.Errorf(format, a...) }
func okf(format string, a ...any) (string, error)  { return fmt.Sprintf(format, a...), nil }

// ---------------------------------------------------------------------------
// Scenarios.
// ---------------------------------------------------------------------------

// connectChurn opens and closes many connections; verifies the hub returns to
// its baseline count (detects connection/registration leaks).
func connectChurn(ctx context.Context, e *env) (string, error) {
	// Sustained connect/close churn for the whole load window. A global
	// NumClients leak assertion can't run here because scenarios share the node
	// in parallel; the suite performs one authoritative no-leak check at the end.
	const workers = 32
	var wg sync.WaitGroup
	var cycles, failed atomic.Int64
	deadline := time.Now().Add(loadDur)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(deadline) && ctx.Err() == nil {
				co, err := dial(e.wsURL, newUser("churn"))
				if err != nil {
					failed.Add(1)
					continue
				}
				co.c.Close()
				cycles.Add(1)
			}
		}()
	}
	wg.Wait()
	if failed.Load() > 0 {
		return fail("%d connections failed to establish (%d succeeded)", failed.Load(), cycles.Load())
	}
	return okf("%d connect/close cycles over %s (%d workers)", cycles.Load(), loadDur, workers)
}

// pubsubFanout verifies every subscriber receives every publication exactly once
// and in offset order.
func pubsubFanout(ctx context.Context, e *env) (string, error) {
	const subs, msgs = 25, 300
	ch := "recov:" + newUser("fanout")
	type recv struct {
		mu   sync.Mutex
		offs []uint64
	}
	recvs := make([]*recv, subs)
	clients := make([]*conn, subs)
	for i := 0; i < subs; i++ {
		co, err := dial(e.wsURL, newUser("fanout"))
		if err != nil {
			return fail("subscriber %d dial: %v", i, err)
		}
		clients[i] = co
		r := &recv{}
		recvs[i] = r
		_, _, err = subscribe(co.c, ch, cent.SubscriptionConfig{Positioned: true}, func(s *cent.Subscription) {
			s.OnPublication(func(ev cent.PublicationEvent) {
				r.mu.Lock()
				r.offs = append(r.offs, ev.Offset)
				r.mu.Unlock()
			})
		})
		if err != nil {
			return fail("subscriber %d subscribe: %v", i, err)
		}
	}
	defer func() {
		for _, c := range clients {
			c.c.Close()
		}
	}()
	for i := 0; i < msgs; i++ {
		if _, err := e.node.Publish(ch, []byte(fmt.Sprintf(`{"n":%d}`, i)), centrifuge.WithHistory(200, time.Minute)); err != nil {
			return fail("publish %d: %v", i, err)
		}
	}
	// Wait for delivery.
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		done := true
		for _, r := range recvs {
			r.mu.Lock()
			n := len(r.offs)
			r.mu.Unlock()
			if n < msgs {
				done = false
				break
			}
		}
		if done {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	for i, r := range recvs {
		r.mu.Lock()
		offs := append([]uint64(nil), r.offs...)
		r.mu.Unlock()
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
	const n = 400
	var subCount, unsubCount atomic.Int64
	channels := make([]string, n)
	for i := 0; i < n; i++ {
		ch := fmt.Sprintf("pres:subchurn-%s-%d", newUser("x"), i)
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
	// Wait until all subscribed, then unsubscribe+remove all.
	deadline := time.Now().Add(25 * time.Second)
	for time.Now().Before(deadline) && subCount.Load() < n {
		time.Sleep(20 * time.Millisecond)
	}
	if subCount.Load() != n {
		return fail("only %d/%d subscribed", subCount.Load(), n)
	}
	for _, ch := range channels {
		if s, ok := co.c.GetSubscription(ch); ok {
			_ = s.Unsubscribe()
			_ = co.c.RemoveSubscription(s)
		}
	}
	deadline = time.Now().Add(25 * time.Second)
	for time.Now().Before(deadline) && unsubCount.Load() < n {
		time.Sleep(20 * time.Millisecond)
	}
	if unsubCount.Load() != n {
		return fail("only %d/%d unsubscribed", unsubCount.Load(), n)
	}
	// Hub must retain no subscribers for these channels. Hub removal is async
	// relative to the client's OnUnsubscribed, so poll with a bounded wait.
	deadline = time.Now().Add(10 * time.Second)
	var remaining string
	for time.Now().Before(deadline) {
		remaining = ""
		for _, ch := range channels {
			if got := e.node.Hub().NumSubscribers(ch); got != 0 {
				remaining = fmt.Sprintf("%s (%d subs)", ch, got)
				break
			}
		}
		if remaining == "" {
			break
		}
		time.Sleep(30 * time.Millisecond)
	}
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
	ch := "recov:" + newUser("hist")
	const n = 200
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
	ch := "recov:" + newUser("pos")
	var mu sync.Mutex
	var offs []uint64
	_, _, err = subscribe(co.c, ch, cent.SubscriptionConfig{Positioned: true, Recoverable: true}, func(s *cent.Subscription) {
		s.OnPublication(func(ev cent.PublicationEvent) {
			mu.Lock()
			offs = append(offs, ev.Offset)
			mu.Unlock()
		})
	})
	if err != nil {
		return fail("subscribe: %v", err)
	}
	const n = 300
	for i := 0; i < n; i++ {
		if _, err := e.node.Publish(ch, []byte(`{"x":1}`), centrifuge.WithHistory(500, time.Minute)); err != nil {
			return fail("publish %d: %v", i, err)
		}
	}
	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		l := len(offs)
		mu.Unlock()
		if l >= n {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	mu.Lock()
	got := append([]uint64(nil), offs...)
	mu.Unlock()
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
	ch := "recov:" + newUser("recov")

	var mu sync.Mutex
	seen := map[uint64]int{}
	record := func(off uint64) {
		mu.Lock()
		seen[off]++
		mu.Unlock()
	}
	// Manage the subscription directly: register handlers before Subscribe and
	// use a single OnSubscribed to signal both the initial subscribe and the
	// post-reconnect resubscribe (buffered so both are observable).
	subscribed := make(chan struct{}, 8)
	sub, err := co.c.NewSubscription(ch, cent.SubscriptionConfig{Recoverable: true, Positioned: true})
	if err != nil {
		return fail("new subscription: %v", err)
	}
	sub.OnPublication(func(ev cent.PublicationEvent) { record(ev.Offset) })
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
	case <-time.After(5 * time.Second):
		return fail("initial subscribe did not complete")
	}
	reSubscribed := subscribed // reuse the same channel for the reconnect signal.

	// Publish a batch live.
	for i := 0; i < 10; i++ {
		if _, err := e.node.Publish(ch, []byte(`{"p":1}`), centrifuge.WithHistory(200, time.Minute)); err != nil {
			return fail("live publish %d: %v", i, err)
		}
	}
	time.Sleep(400 * time.Millisecond)

	// Force a reconnect; while the client is away, publish more (to be recovered).
	discBefore := co.disconnects.Load()
	if err := e.node.Disconnect(user, centrifuge.WithCustomDisconnect(centrifuge.DisconnectForceReconnect)); err != nil {
		return fail("force disconnect: %v", err)
	}
	// Wait until the client actually noticed the disconnect.
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) && co.disconnects.Load() == discBefore {
		time.Sleep(20 * time.Millisecond)
	}
	for i := 0; i < 15; i++ {
		if _, err := e.node.Publish(ch, []byte(`{"p":2}`), centrifuge.WithHistory(200, time.Minute)); err != nil {
			return fail("gap publish %d: %v", i, err)
		}
	}
	// Wait for resubscribe (recovery) to complete.
	select {
	case <-reSubscribed:
	case <-time.After(6 * time.Second):
		return fail("client did not resubscribe after forced reconnect")
	}
	// Give recovery deliveries a moment.
	deadline = time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(seen)
		mu.Unlock()
		if n >= 25 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
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
	ch := "delta:" + newUser("delta")
	var mu sync.Mutex
	var got []string
	_, _, err = subscribe(co.c, ch, cent.SubscriptionConfig{
		Delta: cent.DeltaTypeFossil, Recoverable: true, Positioned: true,
	}, func(s *cent.Subscription) {
		s.OnPublication(func(ev cent.PublicationEvent) {
			mu.Lock()
			got = append(got, string(ev.Data))
			mu.Unlock()
		})
	})
	if err != nil {
		return fail("subscribe: %v", err)
	}
	const n = 120
	want := make([]string, n)
	body := strings.Repeat("shared-body-", 10)
	for i := 0; i < n; i++ {
		payload := fmt.Sprintf(`{"body":%q,"seq":%d}`, body, i)
		want[i] = payload
		if _, err := e.node.Publish(ch, []byte(payload),
			centrifuge.WithDelta(true), centrifuge.WithHistory(100, time.Minute)); err != nil {
			return fail("publish %d: %v", i, err)
		}
	}
	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		l := len(got)
		mu.Unlock()
		if l >= n {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
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
	ch := "pres:" + newUser("pres")
	const n = 25
	clients := make([]*conn, n)
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
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		pres, err = watcher.c.Presence(ctx, ch)
		if err == nil && len(pres.Clients) == wantMembers {
			break
		}
		time.Sleep(30 * time.Millisecond)
	}
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
	for _, c := range clients {
		c.c.Close()
	}
	deadline = time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) {
		p, err := watcher.c.Presence(ctx, ch)
		if err == nil && len(p.Clients) == 1 {
			break
		}
		time.Sleep(30 * time.Millisecond)
	}
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
	// Client-requested filter.
	co, err := dial(e.wsURL, newUser("tags"))
	if err != nil {
		return fail("dial: %v", err)
	}
	defer co.c.Close()

	chClient := "tags:" + newUser("tags")
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
	chServer := "stags:" + newUser("tags")
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
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		done := len(clientTeams) >= wantEng && len(serverTeams) >= wantEng
		mu.Unlock()
		if done {
			break
		}
		time.Sleep(30 * time.Millisecond)
	}
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
	const n = 2000
	var wg sync.WaitGroup
	var bad atomic.Int64
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			data := []byte(fmt.Sprintf(`{"i":%d}`, i))
			cctx, cancel := context.WithTimeout(ctx, 8*time.Second)
			res, err := co.c.RPC(cctx, "echo", data)
			cancel()
			if err != nil {
				bad.Add(1)
				return
			}
			want := fmt.Sprintf(`{"method":"echo","data":%s}`, data)
			if string(res.Data) != want {
				bad.Add(1)
			}
		}(i)
	}
	wg.Wait()
	if bad.Load() > 0 {
		return fail("%d/%d RPC round-trips wrong or failed", bad.Load(), n)
	}
	return okf("%d concurrent RPC round-trips correct", n)
}

// clientPublishFanout verifies client-initiated publishes reach other
// subscribers.
func clientPublishFanout(ctx context.Context, e *env) (string, error) {
	ch := "pres:" + newUser("cpub")
	pub, err := dial(e.wsURL, newUser("cpub-p"))
	if err != nil {
		return fail("publisher dial: %v", err)
	}
	defer pub.c.Close()
	const subs, msgs = 12, 150
	counters := make([]*atomic.Int64, subs)
	clients := make([]*conn, subs)
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
	defer func() {
		for _, c := range clients {
			c.c.Close()
		}
	}()
	// Publisher subscribes too (needed to publish into channel it participates in)
	psub, _, err := subscribe(pub.c, ch, cent.SubscriptionConfig{}, nil)
	if err != nil {
		return fail("publisher subscribe: %v", err)
	}
	for i := 0; i < msgs; i++ {
		if _, err := psub.Publish(ctx, []byte(fmt.Sprintf(`{"n":%d}`, i))); err != nil {
			return fail("client publish %d: %v", i, err)
		}
	}
	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) {
		done := true
		for _, c := range counters {
			if c.Load() < msgs {
				done = false
				break
			}
		}
		if done {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
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
	ch := "recov:" + newUser("sameconn")
	if _, _, err := subscribe(co.c, ch, cent.SubscriptionConfig{Recoverable: true, Positioned: true}, nil); err != nil {
		return fail("subscribe: %v", err)
	}
	const workers, each = 40, 60
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
				cctx, cancel := context.WithTimeout(ctx, 4*time.Second)
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
		return fail("%d/%d concurrent ops failed (lastErr=%v)", errs.Load(), workers*each, co.lastErr.Load())
	}
	return okf("%d concurrent ops on one connection, all succeeded", workers*each)
}

// refreshConnection verifies a short-TTL connection stays alive via refresh.
func refreshConnection(ctx context.Context, e *env) (string, error) {
	co, err := dial(e.wsURL, "refresh:"+newUser("r"))
	if err != nil {
		return fail("dial: %v", err)
	}
	defer co.c.Close()
	ch := "recov:" + newUser("refresh")
	if _, _, err := subscribe(co.c, ch, cent.SubscriptionConfig{Recoverable: true, Positioned: true}, nil); err != nil {
		return fail("subscribe: %v", err)
	}
	// Live well past the 2s TTL; the connection must refresh and stay usable.
	discBefore := co.disconnects.Load()
	time.Sleep(6 * time.Second)
	if co.c.State() != cent.StateConnected {
		return fail("connection not connected after TTL window (state=%v, disconnects=%d)", co.c.State(), co.disconnects.Load())
	}
	if _, err := co.c.Publish(ctx, ch, []byte(`{"alive":1}`)); err != nil {
		return fail("publish after refresh window: %v", err)
	}
	return okf("connection survived TTL window via refresh (disconnects delta=%d)", co.disconnects.Load()-discBefore)
}

// pingPong verifies the server↔client keepalive: the connection is pinged every
// ~500ms and must stay alive and functional across many cycles. A broken pong
// path drops the connection server-side (PongTimeout); a broken ping path drops
// it client-side (MaxServerPingDelay) — either way this scenario catches it.
func pingPong(ctx context.Context, e *env) (string, error) {
	co, err := dial(e.wsURL, "ping:"+newUser("pp"))
	if err != nil {
		return fail("dial: %v", err)
	}
	defer co.c.Close()
	ch := "recov:" + newUser("pp")
	var got atomic.Int64
	_, _, err = subscribe(co.c, ch, cent.SubscriptionConfig{}, func(s *cent.Subscription) {
		s.OnPublication(func(cent.PublicationEvent) { got.Add(1) })
	})
	if err != nil {
		return fail("subscribe: %v", err)
	}
	discBefore := co.disconnects.Load()
	// Span ~8 server ping cycles; publish along the way to confirm the connection
	// stays fully functional under the ping/pong traffic.
	const window = 4 * time.Second
	sent := 0
	deadline := time.Now().Add(window)
	for time.Now().Before(deadline) {
		if st := co.c.State(); st != cent.StateConnected {
			return fail("connection dropped during ping window (state=%v, disconnects=%d, lastErr=%v)",
				st, co.disconnects.Load(), co.lastErr.Load())
		}
		pctx, cancel := context.WithTimeout(ctx, 3*time.Second)
		_, perr := co.c.Publish(pctx, ch, []byte(`{"ping":1}`))
		cancel()
		if perr != nil {
			return fail("publish during ping window: %v", perr)
		}
		sent++
		time.Sleep(500 * time.Millisecond)
	}
	if d := co.disconnects.Load() - discBefore; d != 0 {
		return fail("connection saw %d disconnect(s) during the ping/pong window", d)
	}
	if int64(sent) != got.Load() {
		return fail("delivered %d/%d publications during ping window", got.Load(), sent)
	}
	return okf("stayed alive & functional across %s of 500ms pings (%d pubs round-tripped)", window, sent)
}

// mixedChaos runs a burst of many clients doing random operations and asserts
// the server neither errors nor leaks connections.
func mixedChaos(ctx context.Context, e *env) (string, error) {
	const clients = 60
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
				return
			}
			defer co.c.Close()
			ch := fmt.Sprintf("recov:chaos-%d", id%5) // shared channels → real fanout
			_, _, err = subscribe(co.c, ch, cent.SubscriptionConfig{Recoverable: true, Positioned: true, JoinLeave: true}, func(s *cent.Subscription) {
				s.OnPublication(func(cent.PublicationEvent) {})
			})
			if err != nil {
				opErrs.Add(1)
				return
			}
			rng := rand.New(rand.NewSource(int64(id)))
			for chaosCtx.Err() == nil {
				octx, oc := context.WithTimeout(chaosCtx, 3*time.Second)
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

// ---------------------------------------------------------------------------
// Runner.
// ---------------------------------------------------------------------------

func main() {
	dur := flag.Duration("d", 45*time.Second, "overall suite deadline")
	verbose := flag.Bool("v", false, "log server-side disconnects (for debugging)")
	only := flag.String("only", "", "run only the named scenario")
	flag.Parse()
	debugDisc = *verbose

	node, err := buildNode()
	if err != nil {
		fmt.Println("failed to build node:", err)
		os.Exit(2)
	}
	defer func() { _ = node.Shutdown(context.Background()) }()

	mux := httptestMux(node)
	srv := httptest.NewServer(mux)
	defer srv.Close()
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/connection/websocket"

	e := &env{wsURL: wsURL, node: node}

	scenarios := []scenario{
		{"connect_churn", connectChurn},
		{"pubsub_fanout", pubsubFanout},
		{"subscribe_churn", subscribeChurn},
		{"history_api", historyAPI},
		{"positioning", positioning},
		{"recovery_reconnect", recoveryReconnect},
		{"delta_correctness", deltaCorrectness},
		{"presence_joinleave", presenceJoinLeave},
		{"tags_filter", tagsFilter},
		{"rpc_concurrent", rpcConcurrent},
		{"client_publish_fanout", clientPublishFanout},
		{"same_conn_concurrency", sameConnConcurrency},
		{"refresh_connection", refreshConnection},
		{"ping_pong", pingPong},
		{"mixed_chaos", mixedChaos},
	}

	if *only != "" {
		filtered := scenarios[:0]
		for _, sc := range scenarios {
			if sc.name == *only {
				filtered = append(filtered, sc)
			}
		}
		scenarios = filtered
	}

	ctx, cancel := context.WithTimeout(context.Background(), *dur)
	defer cancel()

	fmt.Printf("centrifuge runtime stress suite: %d scenarios, deadline %s\n\n", len(scenarios), *dur)
	start := time.Now()
	results := make([]result, len(scenarios))
	var wg sync.WaitGroup
	for i, sc := range scenarios {
		wg.Add(1)
		go func(i int, sc scenario) {
			defer wg.Done()
			t0 := time.Now()
			detail, err := runGuarded(ctx, e, sc)
			r := result{name: sc.name, dur: time.Since(t0)}
			if err != nil {
				r.pass = false
				r.detail = err.Error()
			} else {
				r.pass = true
				r.detail = detail
			}
			results[i] = r
		}(i, sc)
	}
	wg.Wait()

	// Authoritative leak check: once every scenario's clients are closed, the hub
	// must drain back to zero connections and channels. This is the one place a
	// global assertion is valid, because nothing else is connecting anymore.
	leakDetail := "no leaked connections/channels"
	leakOK := true
	deadline := time.Now().Add(5 * time.Second)
	for {
		nc, nch := node.Hub().NumClients(), node.Hub().NumChannels()
		if nc == 0 && nch == 0 {
			break
		}
		if time.Now().After(deadline) {
			leakOK = false
			leakDetail = fmt.Sprintf("hub did not drain: NumClients=%d, NumChannels=%d", nc, nch)
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	results = append(results, result{name: "no_leaks_final", pass: leakOK, detail: leakDetail})

	// Report.
	fmt.Println(strings.Repeat("-", 78))
	passed := 0
	for _, r := range results {
		status := "PASS"
		if r.pass {
			passed++
		} else {
			status = "FAIL"
		}
		fmt.Printf("%-4s %-22s %6.1fs  %s\n", status, r.name, r.dur.Seconds(), r.detail)
	}
	fmt.Println(strings.Repeat("-", 78))
	fmt.Printf("total %.1fs — %d passed, %d failed\n", time.Since(start).Seconds(), passed, len(results)-passed)

	if passed != len(results) {
		os.Exit(1)
	}
}

// runGuarded runs a scenario, converting panics and deadline into failures so
// one bad scenario never takes down the suite.
func runGuarded(ctx context.Context, e *env, sc scenario) (detail string, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("PANIC: %v", r)
		}
	}()
	done := make(chan struct{})
	go func() {
		detail, err = sc.run(ctx, e)
		close(done)
	}()
	select {
	case <-done:
		return detail, err
	case <-ctx.Done():
		return "", fmt.Errorf("scenario did not finish before suite deadline")
	}
}
