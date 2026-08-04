// Command stress_runtime is a self-contained runtime stress/regression tool for
// the centrifuge server library. It boots four in-process Nodes — a generous
// memory-backed one for throughput, a deliberately strict one for limit
// enforcement, and two Redis-backed ones sharing a Redis instance and key prefix
// so they form a real two-node cluster — and hammers them with real
// centrifuge-go clients over WebSocket, with raw protocol clients over
// WebSocket/SSE/HTTP-streaming, and with the node's own server-side APIs.
//
// A local Redis is required by default; pass -no-redis to run only the
// memory-backed scenarios.
//
// Every feature is a self-checking scenario with hard invariants: exact message
// counts, ordering, recovered ranges, delta reconstruction, cache-recovery
// semantics, idempotency suppression, presence sets, exact protocol error
// codes, exact disconnect codes, and no leaked connections/subscriptions. On any
// violation the tool prints which scenario failed and why, and exits non-zero —
// so a green run is strong evidence the runtime is not broken.
//
// It runs hard: the sustained scenarios churn hundreds of thousands of
// connections and run tens of concurrent clients for the whole load window,
// while the rest hunt for correctness bugs in the trickier corners.
//
//	go run .                       // full suite
//	go run -race .                 // race-check the in-process server under load
//	go run . -list                 // list scenarios
//	go run . -only ping_pong,delta_correctness
//	go run . -v                    // log server-side disconnects
//	go run . -p 4                  // cap how many scenarios run at once
//	go run . -load 30s -d 180s     // longer load window and suite deadline
//	go run . -repeat 5             // run the selection several times (flake hunting)
//	go run . -redis host:port      // point at a different Redis
//	go run . -no-redis             // skip the Redis nodes and every redis_* scenario
package main

import (
	"context"
	"flag"
	"fmt"
	"net/http/httptest"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/centrifugal/centrifuge"
)

// loadDur is how long the sustained-throughput scenarios (connect churn, chaos)
// run. Scenarios run concurrently so the suite wall-clock stays near this value
// plus whatever the slowest correctness scenario needs.
var loadDur = 18 * time.Second

// suiteSeed makes randomized scenarios reproducible; it is printed on every run.
var suiteSeed int64

type env struct {
	// Main node: generous limits, used by everything that measures throughput or
	// correctness.
	wsURL   string
	httpURL string
	node    *centrifuge.Node

	// Strict node: small queue/channel limits and short close delays, used by the
	// scenarios that assert the server enforces its limits.
	strictWSURL string
	strictNode  *centrifuge.Node

	// Two Redis-backed nodes sharing one Redis instance and key prefix — a real
	// two-node cluster. Nil when the suite runs with -no-redis.
	redisA, redisB     *centrifuge.Node
	redisAWS, redisBWS string
	redisPrefix        string
}

// nodes returns every running node by name, for the final leak check.
func (e *env) nodes() map[string]*centrifuge.Node {
	all := map[string]*centrifuge.Node{"main": e.node, "strict": e.strictNode}
	if e.redisA != nil {
		all["redis-a"] = e.redisA
		all["redis-b"] = e.redisB
	}
	return all
}

func (e *env) redisEnabled() bool { return e.redisA != nil }

type result struct {
	name   string
	pass   bool
	detail string
	dur    time.Duration
}

type scenario struct {
	name string
	// timeout bounds a single scenario so one hang fails on its own instead of
	// eating the whole suite deadline. Zero means defaultScenarioTimeout.
	timeout time.Duration
	run     func(ctx context.Context, e *env) (string, error)
}

const defaultScenarioTimeout = 90 * time.Second

func fail(format string, a ...any) (string, error) { return "", fmt.Errorf(format, a...) }
func okf(format string, a ...any) (string, error)  { return fmt.Sprintf(format, a...), nil }

// allScenarios is ordered longest-first so a bounded worker pool starts the
// expensive ones immediately and fills the gaps with the quick ones.
func allScenarios() []scenario {
	return []scenario{
		// Sustained load.
		{name: "connect_churn", timeout: 0, run: connectChurn},
		{name: "mixed_chaos", run: mixedChaos},

		// Core feature matrix.
		{name: "pubsub_fanout", run: pubsubFanout},
		{name: "subscribe_churn", run: subscribeChurn},
		{name: "history_api", run: historyAPI},
		{name: "positioning", run: positioning},
		{name: "recovery_reconnect", run: recoveryReconnect},
		{name: "delta_correctness", run: deltaCorrectness},
		{name: "presence_joinleave", run: presenceJoinLeave},
		{name: "tags_filter", run: tagsFilter},
		{name: "rpc_concurrent", run: rpcConcurrent},
		{name: "client_publish_fanout", run: clientPublishFanout},
		{name: "same_conn_concurrency", run: sameConnConcurrency},
		{name: "refresh_connection", run: refreshConnection},
		{name: "ping_pong", run: pingPong},

		// Server-driven subscriptions and user-targeted APIs.
		{name: "server_side_subs", run: serverSideSubs},
		{name: "server_sub_api_churn", run: serverSubAPIChurn},
		{name: "multi_conn_same_user", run: multiConnSameUser},

		// Alternative codecs and transports.
		{name: "protobuf_delta_recovery", run: protobufDeltaRecovery},
		{name: "sse_transport", run: sseTransport},
		{name: "http_stream_emulation", run: httpStreamEmulation},

		// Broker/stream semantics.
		{name: "cache_recovery", run: cacheRecovery},
		{name: "concurrent_publish_offsets", run: concurrentPublishOffsets},
		{name: "idempotent_publish", run: idempotentPublish},
		{name: "history_pagination", run: historyPagination},
		{name: "unrecoverable_position", run: unrecoverablePosition},
		{name: "sub_refresh_expiry", run: subRefreshExpiry},
		{name: "recovery_storm", run: recoveryStorm},

		// Node APIs and payload edges.
		{name: "survey_notify", run: surveyNotify},
		{name: "async_send_echo", run: asyncSendEcho},
		{name: "large_payloads", run: largePayloads},
		{name: "many_channels_one_conn", run: manyChannelsOneConn},
		{name: "error_paths", run: errorPaths},

		// Adversarial clients and limit enforcement.
		{name: "malformed_protocol", run: malformedProtocol},
		{name: "stale_connection", run: staleConnection},
		{name: "oversized_frame", run: oversizedFrame},
		{name: "slow_client", run: slowClient},
		{name: "no_pong", run: noPong},
		{name: "channel_limit", run: channelLimit},
		{name: "expired_connection", run: expiredConnection},
		{name: "subscribe_unsubscribe_race", run: subscribeUnsubscribeRace},
		{name: "disconnect_during_subscribe", run: disconnectDuringSubscribe},

		// Redis engine: two nodes sharing one Redis instance.
		{name: "redis_chaos", run: redisChaos},
		{name: "redis_cluster_view", run: redisClusterView},
		{name: "redis_pubsub_fanout", run: redisPubSubFanout},
		{name: "redis_cross_node_recovery", run: redisCrossNodeRecovery},
		{name: "redis_cross_node_control", run: redisCrossNodeControl},
		{name: "redis_presence_cross_node", run: redisPresenceCrossNode},
		{name: "redis_delta_recovery", run: redisDeltaRecovery},
		{name: "redis_concurrent_publish_offsets", run: redisConcurrentPublishOffsets},
		{name: "redis_idempotent_publish", run: redisIdempotentPublish},
		{name: "redis_cache_recovery", run: redisCacheRecovery},
		{name: "redis_history_pagination", run: redisHistoryPagination},
		{name: "redis_recovery_storm", run: redisRecoveryStorm},
	}
}

func main() {
	dur := flag.Duration("d", 180*time.Second, "overall suite deadline")
	load := flag.Duration("load", loadDur, "how long the sustained-load scenarios run")
	verbose := flag.Bool("v", false, "log server-side disconnects (for debugging)")
	only := flag.String("only", "", "comma-separated scenario names to run")
	skip := flag.String("skip", "", "comma-separated scenario names to skip")
	list := flag.Bool("list", false, "list scenario names and exit")
	par := flag.Int("p", 0, "max scenarios running at once (0 = NumCPU, min 4)")
	repeat := flag.Int("repeat", 1, "run the selection this many times")
	seed := flag.Int64("seed", 0, "seed for randomized scenarios (0 = time-based)")
	redisAddr := flag.String("redis", "127.0.0.1:6379", "Redis address for the two Redis-backed nodes")
	noRedis := flag.Bool("no-redis", false, "skip the Redis-backed nodes and every redis_* scenario")
	flag.Parse()
	debugDisc = *verbose
	loadDur = *load

	scenarios := allScenarios()
	if *list {
		for _, sc := range scenarios {
			fmt.Println(sc.name)
		}
		return
	}
	if *noRedis {
		scenarios = dropRedisScenarios(scenarios)
	}
	if scenarios = filterScenarios(scenarios, *only, *skip); len(scenarios) == 0 {
		fmt.Println("no scenarios selected")
		os.Exit(2)
	}

	// Randomized scenarios derive their generators from suiteSeed, so a failing
	// run can be replayed with -seed.
	suiteSeed = *seed
	if suiteSeed == 0 {
		suiteSeed = time.Now().UnixNano()
	}

	parallelism := *par
	if parallelism <= 0 {
		parallelism = runtime.NumCPU()
	}
	if parallelism < 4 {
		parallelism = 4
	}

	node, closeNode, err := buildNode(mainNodeConfig())
	if err != nil {
		fmt.Println("failed to build main node:", err)
		os.Exit(2)
	}
	defer closeNode()
	strictNode, closeStrict, err := buildNode(strictNodeConfig())
	if err != nil {
		fmt.Println("failed to build strict node:", err)
		os.Exit(2)
	}
	defer closeStrict()

	srv := httptest.NewServer(nodeMux(node, mainNodeConfig().ws))
	defer srv.Close()
	strictSrv := httptest.NewServer(nodeMux(strictNode, strictNodeConfig().ws))
	defer strictSrv.Close()

	e := &env{
		wsURL:       wsURLOf(srv.URL),
		httpURL:     srv.URL,
		node:        node,
		strictWSURL: wsURLOf(strictSrv.URL),
		strictNode:  strictNode,
	}

	// Redis is required by default: a green run is supposed to say something
	// about the Redis engine too, so a missing Redis is a hard error rather than
	// a silent reduction in coverage.
	if !*noRedis {
		prefix := fmt.Sprintf("stress-%d", suiteSeed)
		redisA, closeA, err := buildNode(redisNodeConfig("redis-a", *redisAddr, prefix))
		if err != nil {
			fmt.Printf("failed to build Redis node at %s: %v\n", *redisAddr, err)
			fmt.Println("start a local Redis, point -redis at one, or run with -no-redis to skip the Redis scenarios")
			os.Exit(2)
		}
		defer closeA()
		redisB, closeB, err := buildNode(redisNodeConfig("redis-b", *redisAddr, prefix))
		if err != nil {
			fmt.Printf("failed to build second Redis node at %s: %v\n", *redisAddr, err)
			os.Exit(2)
		}
		defer closeB()

		redisSrvA := httptest.NewServer(nodeMux(redisA, redisNodeConfig("redis-a", *redisAddr, prefix).ws))
		defer redisSrvA.Close()
		redisSrvB := httptest.NewServer(nodeMux(redisB, redisNodeConfig("redis-b", *redisAddr, prefix).ws))
		defer redisSrvB.Close()

		e.redisA, e.redisB = redisA, redisB
		e.redisAWS, e.redisBWS = wsURLOf(redisSrvA.URL), wsURLOf(redisSrvB.URL)
		e.redisPrefix = prefix

		// Both nodes must see each other before any cross-node scenario runs.
		if err := waitForCluster(e, 20*time.Second); err != nil {
			fmt.Println("Redis nodes did not form a cluster:", err)
			os.Exit(2)
		}
	}

	// Warm up every node so anything the library starts lazily is already
	// running, then take the goroutine baseline the final check compares against.
	if err := warmup(e); err != nil {
		fmt.Println("warm-up failed:", err)
		os.Exit(2)
	}
	goroutineBaseline, goroutineBaselineFrames := libGoroutines()

	engines := "memory"
	if e.redisEnabled() {
		engines = fmt.Sprintf("memory + redis(%s, prefix %s)", *redisAddr, e.redisPrefix)
	}
	fmt.Printf("centrifuge runtime stress suite: %d scenarios, %s, parallelism %d, load window %s, deadline %s, seed %d\n",
		len(scenarios), engines, parallelism, loadDur, *dur, suiteSeed)
	fmt.Printf("goroutine baseline after warm-up: %d library goroutines\n\n", goroutineBaseline)

	ctx, cancel := context.WithTimeout(context.Background(), *dur)
	defer cancel()

	start := time.Now()
	failures := 0
	for round := 1; round <= *repeat; round++ {
		if *repeat > 1 {
			fmt.Printf("=== round %d/%d ===\n", round, *repeat)
		}
		results := runScenarios(ctx, e, scenarios, parallelism)
		results = append(results, finalLeakCheck(e))
		results = append(results, goroutineLeakCheck(goroutineBaseline, goroutineBaselineFrames))
		failures += report(results)
		if ctx.Err() != nil {
			break
		}
	}
	fmt.Printf("total %.1fs — %d failed\n", time.Since(start).Seconds(), failures)
	if failures > 0 {
		os.Exit(1)
	}
}

func wsURLOf(httpURL string) string {
	return "ws" + strings.TrimPrefix(httpURL, "http") + "/connection/websocket"
}

func filterScenarios(scenarios []scenario, only, skip string) []scenario {
	set := func(s string) map[string]bool {
		m := map[string]bool{}
		for _, name := range strings.Split(s, ",") {
			if name = strings.TrimSpace(name); name != "" {
				m[name] = true
			}
		}
		return m
	}
	onlySet, skipSet := set(only), set(skip)
	out := scenarios[:0]
	for _, sc := range scenarios {
		if len(onlySet) > 0 && !onlySet[sc.name] {
			continue
		}
		if skipSet[sc.name] {
			continue
		}
		out = append(out, sc)
	}
	return out
}

// runScenarios runs the selection with a bounded worker pool: enough concurrency
// to keep the node genuinely contended, bounded enough that CPU starvation does
// not turn correctness assertions into timeouts.
func runScenarios(ctx context.Context, e *env, scenarios []scenario, parallelism int) []result {
	results := make([]result, len(scenarios))
	sem := make(chan struct{}, parallelism)
	var wg sync.WaitGroup
	for i, sc := range scenarios {
		wg.Add(1)
		go func(i int, sc scenario) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			t0 := time.Now()
			detail, err := runGuarded(ctx, e, sc)
			r := result{name: sc.name, dur: time.Since(t0)}
			if err != nil {
				r.detail = err.Error()
			} else {
				r.pass = true
				r.detail = detail
			}
			results[i] = r
		}(i, sc)
	}
	wg.Wait()
	return results
}

// runGuarded runs a scenario, converting panics, its own timeout and the suite
// deadline into failures so one bad scenario never takes down the suite.
func runGuarded(ctx context.Context, e *env, sc scenario) (string, error) {
	timeout := sc.timeout
	if timeout == 0 {
		timeout = defaultScenarioTimeout
		// Sustained-load scenarios need at least the load window plus slack.
		if min := loadDur + 30*time.Second; timeout < min {
			timeout = min
		}
	}
	scCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	type outcome struct {
		detail string
		err    error
	}
	// Buffered so a scenario finishing after the deadline abandons its result
	// without blocking; results travel over the channel (not shared variables) so
	// an abandoned scenario never races the already-returned caller.
	done := make(chan outcome, 1)
	go func() {
		// recover must live on the scenario goroutine — a deferred recover in the
		// caller cannot catch a panic from here.
		defer func() {
			if r := recover(); r != nil {
				done <- outcome{err: fmt.Errorf("PANIC: %v", r)}
			}
		}()
		detail, err := sc.run(scCtx, e)
		done <- outcome{detail: detail, err: err}
	}()
	select {
	case out := <-done:
		return out.detail, out.err
	case <-scCtx.Done():
		if ctx.Err() != nil {
			return "", fmt.Errorf("scenario did not finish before the suite deadline")
		}
		return "", fmt.Errorf("scenario exceeded its %s timeout", timeout)
	}
}

// finalLeakCheck is the one place a global assertion is valid: every scenario has
// finished and closed its clients, so both hubs must drain to zero.
func finalLeakCheck(e *env) result {
	nodes := e.nodes()
	names := make([]string, 0, len(nodes))
	for name := range nodes {
		names = append(names, name)
	}
	sort.Strings(names)
	var stuck []string
	ok := waitFor(20*time.Second, func() bool {
		stuck = stuck[:0]
		for _, name := range names {
			n := nodes[name]
			nc, nch := n.Hub().NumClients(), n.Hub().NumChannels()
			if nc != 0 || nch != 0 {
				stuck = append(stuck, fmt.Sprintf("%s: NumClients=%d NumChannels=%d", name, nc, nch))
			}
		}
		return len(stuck) == 0
	})
	if !ok {
		return result{name: "no_leaks_final", detail: "hub did not drain — " + strings.Join(stuck, "; ")}
	}
	return result{name: "no_leaks_final", pass: true,
		detail: fmt.Sprintf("all %d hubs drained to zero connections and channels", len(names))}
}

// waitForCluster blocks until both Redis-backed nodes see each other through the
// Redis control plane.
func waitForCluster(e *env, timeout time.Duration) error {
	var lastA, lastB int
	ok := waitFor(timeout, func() bool {
		infoA, errA := e.redisA.Info()
		infoB, errB := e.redisB.Info()
		if errA != nil || errB != nil {
			return false
		}
		lastA, lastB = len(infoA.Nodes), len(infoB.Nodes)
		return lastA == 2 && lastB == 2
	})
	if !ok {
		return fmt.Errorf("node A sees %d nodes, node B sees %d, want 2 each", lastA, lastB)
	}
	return nil
}

// dropRedisScenarios removes every scenario that needs the Redis nodes.
func dropRedisScenarios(scenarios []scenario) []scenario {
	out := scenarios[:0]
	for _, sc := range scenarios {
		if strings.HasPrefix(sc.name, "redis_") {
			continue
		}
		out = append(out, sc)
	}
	return out
}

func report(results []result) int {
	sorted := append([]result(nil), results...)
	sort.SliceStable(sorted, func(i, j int) bool { return !sorted[i].pass && sorted[j].pass })

	fmt.Println(strings.Repeat("-", 100))
	failed := 0
	for _, r := range sorted {
		status := "PASS"
		if !r.pass {
			status = "FAIL"
			failed++
		}
		fmt.Printf("%-4s %-28s %6.1fs  %s\n", status, r.name, r.dur.Seconds(), r.detail)
	}
	fmt.Println(strings.Repeat("-", 100))
	fmt.Printf("%d passed, %d failed\n\n", len(results)-failed, failed)
	return failed
}
