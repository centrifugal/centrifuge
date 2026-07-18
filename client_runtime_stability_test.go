package centrifuge

import (
	"context"
	"math"
	"runtime"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/require"
)

// pingTrackingTransport records the gap between consecutive server pings so a
// test can assert ping punctuality — the property that decides whether real SDKs
// keep the connection (they disconnect when a ping is overdue).
type pingTrackingTransport struct {
	countingTransport
	lastPing atomic.Int64
	maxGap   atomic.Int64
	pings    atomic.Int64
}

func (t *pingTrackingTransport) record() {
	now := time.Now().UnixNano()
	if prev := t.lastPing.Swap(now); prev != 0 {
		if gap := now - prev; gap > t.maxGap.Load() {
			t.maxGap.Store(gap)
		}
	}
	t.pings.Add(1)
}

func (t *pingTrackingTransport) Write(data []byte) error {
	t.record()
	return t.countingTransport.Write(data)
}

func (t *pingTrackingTransport) WriteMany(data ...[]byte) error {
	t.record()
	return t.countingTransport.WriteMany(data...)
}

// steadyGoroutines approximates the RESIDENT goroutine count. A presence tick
// spawns transient workers (up to clientPresenceUpdateConcurrency each, plus one
// per tick under a TimerScheduler), so a single NumGoroutine sample fluctuates
// with however many ticks happen to be in flight — on a fast machine that alone
// exceeds a tight threshold and makes a leak check flaky. Taking the minimum
// across samples filters the transients out while still growing if goroutines
// actually leak.
func steadyGoroutines() int {
	minN := math.MaxInt
	for i := 0; i < 25; i++ {
		runtime.GC()
		if n := runtime.NumGoroutine(); n < minN {
			minN = n
		}
		time.Sleep(10 * time.Millisecond)
	}
	return minN
}

type stabilityOpts struct {
	useWheel    bool
	numConns    int
	numChannels int
	rtt         time.Duration
	duration    time.Duration
	concurrency int
}

// runRuntimeStability holds a fixed population of connections open with presence
// AND positioning enabled against a slow PresenceManager, then asserts the
// runtime stayed stable: nothing disconnected, pings stayed punctual, presence
// kept being refreshed, position checks kept running, and goroutines did not grow.
func runRuntimeStability(t *testing.T, o stabilityOpts) {
	pm := newTrackingPresenceManager(o.rtt)
	cfg := Config{
		LogLevel:                        LogLevelError,
		LogHandler:                      func(entry LogEntry) {},
		ClientChannelLimit:              1000,
		ClientPresenceUpdateInterval:    100 * time.Millisecond,
		ClientChannelPositionCheckDelay: 100 * time.Millisecond,
		ClientChannelPositionMaxTimeLag: time.Hour,
		clientPresenceUpdateConcurrency: o.concurrency,
		clientPositionCheckConcurrency:  o.concurrency,
	}
	var wheel *testSharedTimerScheduler
	if o.useWheel {
		wheel = newTestSharedTimerScheduler(4)
		cfg.ClientTimerScheduler = wheel
	}
	node, err := New(cfg)
	require.NoError(t, err)
	node.SetPresenceManager(pm)
	cb := &countingBroker{Broker: node.broker}
	node.SetBroker(cb)
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{Options: SubscribeOptions{
				EmitPresence:      true,
				EnablePositioning: true,
			}}, nil)
		})
	})
	require.NoError(t, node.Run())
	defer func() {
		_ = node.Shutdown(context.Background())
		if wheel != nil {
			wheel.Stop()
		}
	}()

	const pingInterval = 200 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	transports := make([]*pingTrackingTransport, 0, o.numConns)
	for i := 0; i < o.numConns; i++ {
		tr := &pingTrackingTransport{}
		tr.pingInterval = pingInterval
		tr.pongTimeout = -1 // no pongs from this transport; see timerbench_test.go
		transports = append(transports, tr)
		client := newTestClientCustomTransport(t, ctx, node, tr, "u"+strconv.Itoa(i))
		rw := testReplyWriterWrapper()
		require.NoError(t, client.connectCmd(&protocol.ConnectRequest{}, &protocol.Command{}, time.Now(), rw.rw))
		client.triggerConnect()
		client.scheduleOnConnectTimers()
		for j := 0; j < o.numChannels; j++ {
			srw := testReplyWriterWrapper()
			require.NoError(t, client.handleSubscribe(&protocol.SubscribeRequest{
				Channel: "ch" + strconv.Itoa(i) + "_" + strconv.Itoa(j),
			}, &protocol.Command{Id: 1}, time.Now(), srw.rw))
		}
	}
	require.Equal(t, o.numConns, node.hub.NumClients())

	time.Sleep(500 * time.Millisecond) // settle
	goroutinesBefore := steadyGoroutines()
	addsBefore := pm.adds.Load()
	historyBefore := cb.historyCalls.Load()
	for _, tr := range transports {
		tr.maxGap.Store(0)
	}

	time.Sleep(o.duration)

	// 1. Nothing dropped.
	require.Equal(t, o.numConns, node.hub.NumClients(),
		"connections were dropped during steady-state run")

	// 2. Pings stayed punctual. This is what real SDKs disconnect on.
	var worstGap time.Duration
	for _, tr := range transports {
		if g := time.Duration(tr.maxGap.Load()); g > worstGap {
			worstGap = g
		}
	}
	require.NotZero(t, worstGap, "no pings observed")
	require.Less(t, worstGap, 2*pingInterval,
		"a ping was delayed beyond 2x the interval — presence/position work is blocking the timer")

	// 3. Presence kept being refreshed and position checks kept running.
	require.Greater(t, pm.adds.Load(), addsBefore, "presence stopped being refreshed")
	require.Greater(t, cb.historyCalls.Load(), historyBefore, "position checks stopped running")

	// 4. Presence is complete: every client x channel is present.
	require.Equal(t, o.numConns*o.numChannels, pm.liveEntries(),
		"presence entries missing for live connections")

	// 5. No goroutine leak. A leaked tick goroutine would accumulate once per
	// tick — hundreds over this window — so resident growth is what matters, not
	// the transient workers in flight at any instant.
	goroutinesAfter := steadyGoroutines()
	require.Less(t, goroutinesAfter, goroutinesBefore+o.numConns/2,
		"resident goroutine count grew from %d to %d during steady state", goroutinesBefore, goroutinesAfter)

	t.Logf("wheel=%v conns=%d ch=%d rtt=%v: worst ping gap %v (interval %v), presence adds %d, position checks %d, goroutines %d->%d",
		o.useWheel, o.numConns, o.numChannels, o.rtt, worstGap, pingInterval,
		pm.adds.Load()-addsBefore, cb.historyCalls.Load()-historyBefore,
		goroutinesBefore, goroutinesAfter)
}

func TestRuntimeStability_Runtime(t *testing.T) {
	runRuntimeStability(t, stabilityOpts{
		useWheel: false, numConns: 200, numChannels: 4,
		rtt: time.Millisecond, duration: 3 * time.Second,
	})
}

func TestRuntimeStability_Scheduler(t *testing.T) {
	runRuntimeStability(t, stabilityOpts{
		useWheel: true, numConns: 200, numChannels: 4,
		rtt: time.Millisecond, duration: 3 * time.Second,
	})
}

// Slow PresenceManager: a tick takes far longer than the presence interval, so
// ticks overlap and the in-flight guard is active throughout. Pings must still
// be punctual and connections must survive — this is the core of issue #557.
func TestRuntimeStability_SlowPresenceManager(t *testing.T) {
	runRuntimeStability(t, stabilityOpts{
		useWheel: false, numConns: 50, numChannels: 8,
		rtt: 10 * time.Millisecond, duration: 3 * time.Second,
	})
}

func TestRuntimeStability_SlowPresenceManager_Scheduler(t *testing.T) {
	runRuntimeStability(t, stabilityOpts{
		useWheel: true, numConns: 50, numChannels: 8,
		rtt: 10 * time.Millisecond, duration: 3 * time.Second,
	})
}

// Concurrent presence updates must keep every invariant: presence complete for
// all live connections, pings punctual, no goroutine growth.
func TestRuntimeStability_Concurrent(t *testing.T) {
	runRuntimeStability(t, stabilityOpts{
		useWheel: false, numConns: 50, numChannels: 8, concurrency: 8,
		rtt: 10 * time.Millisecond, duration: 3 * time.Second,
	})
}

func TestRuntimeStability_Concurrent_Scheduler(t *testing.T) {
	runRuntimeStability(t, stabilityOpts{
		useWheel: true, numConns: 50, numChannels: 8, concurrency: 8,
		rtt: 10 * time.Millisecond, duration: 3 * time.Second,
	})
}

// TestRuntimeStability_SlowRefreshHandler covers the expire timer op. An
// application's RefreshHandler is commonly backed by an HTTP call (Centrifugo's
// refresh proxy runs it inline unless client concurrency is configured), so it
// blocks the timer callback. Under a TimerScheduler that shares a goroutine
// between connections that would delay unrelated connections' pings.
//
// Token TTLs cluster in practice (everyone reconnects after a deploy), so many
// connections refresh in the same window — this test makes them all expire at
// once on purpose.
func TestRuntimeStability_SlowRefreshHandler(t *testing.T) {
	const numConns = 60
	const pingInterval = 200 * time.Millisecond
	const refreshDuration = 150 * time.Millisecond

	wheel := newTestSharedTimerScheduler(4)
	defer wheel.Stop()
	node, err := New(Config{
		LogLevel:             LogLevelError,
		LogHandler:           func(entry LogEntry) {},
		ClientTimerScheduler: wheel,
	})
	require.NoError(t, err)
	var refreshes atomic.Int64
	node.OnConnect(func(client *Client) {
		client.OnRefresh(func(e RefreshEvent, cb RefreshCallback) {
			// Simulates a refresh proxy HTTP call running inline.
			time.Sleep(refreshDuration)
			refreshes.Add(1)
			cb(RefreshReply{ExpireAt: time.Now().Unix() + 1}, nil)
		})
	})
	require.NoError(t, node.Run())
	defer func() { _ = node.Shutdown(context.Background()) }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	transports := make([]*pingTrackingTransport, 0, numConns)
	for i := 0; i < numConns; i++ {
		tr := &pingTrackingTransport{}
		tr.pingInterval = pingInterval
		tr.pongTimeout = -1
		transports = append(transports, tr)
		newCtx := SetCredentials(ctx, &Credentials{
			UserID: "u" + strconv.Itoa(i),
			// All connections expire at the same moment on purpose.
			ExpireAt: time.Now().Unix() + 1,
		})
		client, err := newClient(newCtx, node, tr)
		require.NoError(t, err)
		rw := testReplyWriterWrapper()
		require.NoError(t, client.connectCmd(&protocol.ConnectRequest{}, &protocol.Command{}, time.Now(), rw.rw))
		client.triggerConnect()
		client.scheduleOnConnectTimers()
	}

	time.Sleep(500 * time.Millisecond)
	for _, tr := range transports {
		tr.maxGap.Store(0)
	}
	time.Sleep(3 * time.Second)

	require.Positive(t, refreshes.Load(), "refresh handler never ran")
	require.Equal(t, numConns, node.hub.NumClients(), "connections dropped")

	var worstGap time.Duration
	for _, tr := range transports {
		if g := time.Duration(tr.maxGap.Load()); g > worstGap {
			worstGap = g
		}
	}
	t.Logf("%d conns all refreshing with a %v handler: worst ping gap %v (interval %v), %d refreshes",
		numConns, refreshDuration, worstGap, pingInterval, refreshes.Load())
	require.Less(t, worstGap, 2*pingInterval,
		"a slow RefreshHandler delayed pings — expire is blocking the shared timer goroutine")
}
