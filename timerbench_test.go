package centrifuge

import (
	"context"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/centrifugal/protocol"
)

// countingTransport is a minimal Transport that counts writes with no
// allocation or channel work, so benchmarks measure the timer machinery
// rather than the transport.
type countingTransport struct {
	writes       atomic.Int64
	pingInterval time.Duration
	pongTimeout  time.Duration
}

func (t *countingTransport) Name() string           { return "counting" }
func (t *countingTransport) AcceptProtocol() string { return "" }
func (t *countingTransport) Protocol() ProtocolType { return ProtocolTypeJSON }
func (t *countingTransport) ProtocolVersion() ProtocolVersion {
	return ProtocolVersion2
}
func (t *countingTransport) Unidirectional() bool      { return false }
func (t *countingTransport) Emulation() bool           { return false }
func (t *countingTransport) DisabledPushFlags() uint64 { return 0 }
func (t *countingTransport) PingPongConfig() PingPongConfig {
	return PingPongConfig{PingInterval: t.pingInterval, PongTimeout: t.pongTimeout}
}
func (t *countingTransport) Write(_ []byte) error {
	t.writes.Add(1)
	return nil
}
func (t *countingTransport) WriteMany(m ...[]byte) error {
	t.writes.Add(int64(len(m)))
	return nil
}
func (t *countingTransport) Close(_ Disconnect) error { return nil }

// cpuTime returns process CPU time (user+sys) consumed so far.
func cpuTime() time.Duration {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0
	}
	u := time.Duration(ru.Utime.Sec)*time.Second + time.Duration(ru.Utime.Usec)*time.Microsecond
	s := time.Duration(ru.Stime.Sec)*time.Second + time.Duration(ru.Stime.Usec)*time.Microsecond
	return u + s
}

type benchTimerOpts struct {
	useWheel     bool
	numConns     int
	numChannels  int // presence-enabled channels per connection
	pingInterval time.Duration
	presenceIvl  time.Duration
	window       time.Duration
	// wheelShards overrides the timer wheel shard count (0 = PRO's default of 4).
	// The wheel takes a per-shard mutex on every AddTimer, so shard count bounds
	// how well it scales with connection count.
	wheelShards int
	// idle: with a realistic (long) ping interval most connections do nothing
	// during the window. Per-event normalization is meaningless there — the cost
	// under test is per-connection and per-unit-time (runtime timer heap
	// maintenance, GC scanning live timer objects), so absolute process CPU is
	// the metric and the steady-state event guard does not apply.
	idle bool
}

// benchPeriodicEvents measures CPU consumed by per-connection periodic events
// (ping + presence) over a fixed wall-clock window, for N idle connections.
func benchPeriodicEvents(b *testing.B, o benchTimerOpts) {
	cfg := Config{
		LogLevel:                     LogLevelError,
		LogHandler:                   func(entry LogEntry) {},
		ClientChannelLimit:           1000,
		ClientPresenceUpdateInterval: o.presenceIvl,
	}
	var wheel *ShardedTimerWheel
	if o.useWheel {
		shards := o.wheelShards
		if shards == 0 {
			shards = 4 // what Centrifugo PRO uses for batch_periodic_events: true
		}
		wheel = NewShardedTimerWheel(shards, 256, 100*time.Millisecond, 4)
		cfg.ClientTimerScheduler = wheel
	}
	node, err := New(cfg)
	if err != nil {
		b.Fatal(err)
	}
	node.SetPresenceManager(&noopPresenceManager{})
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{Options: SubscribeOptions{EmitPresence: true}}, nil)
		})
	})
	if err := node.Run(); err != nil {
		b.Fatal(err)
	}
	defer func() {
		_ = node.Shutdown(context.Background())
		if wheel != nil {
			wheel.Stop()
		}
	}()

	transports := make([]*countingTransport, 0, o.numConns)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for i := 0; i < o.numConns; i++ {
		// PongTimeout < 0 disables pong checks (config.go:487). Required: this
		// transport never sends pongs, so otherwise every client would be
		// disconnected with DisconnectNoPong and we would measure churn.
		tr := &countingTransport{pingInterval: o.pingInterval, pongTimeout: -1}
		transports = append(transports, tr)
		client := newTestClientCustomTransport(b, ctx, node, tr, "user"+strconv.Itoa(i))
		rwWrapper := testReplyWriterWrapper()
		if err := client.connectCmd(&protocol.ConnectRequest{}, &protocol.Command{}, time.Now(), rwWrapper.rw); err != nil {
			b.Fatal(err)
		}
		// Mirrors connectClientV2: connectCmd alone does not arm client timers.
		client.triggerConnect()
		client.scheduleOnConnectTimers()
		for j := 0; j < o.numChannels; j++ {
			rw := testReplyWriterWrapper()
			if err := client.handleSubscribe(&protocol.SubscribeRequest{
				Channel: "ch" + strconv.Itoa(j),
			}, &protocol.Command{Id: 1}, time.Now(), rw.rw); err != nil {
				b.Fatal(err)
			}
		}
	}

	// Let setup settle so it is not attributed to the measurement window.
	time.Sleep(500 * time.Millisecond)

	// Baseline the write counters: they are cumulative since client creation, so
	// setup traffic (e.g. subscribe-time pushes) must not count as events.
	var writesBefore int64
	for _, tr := range transports {
		writesBefore += tr.writes.Load()
	}

	var ms0, ms1 runtimeMemStats
	readMemStats(&ms0)
	cpu0 := cpuTime()
	start := time.Now()

	// Profile ONLY the measurement window: -cpuprofile would cover the whole
	// process, and building N connections dwarfs the idle steady state we care
	// about, making the profile useless.
	if path := os.Getenv("BENCH_WINDOW_CPUPROFILE"); path != "" {
		f, err := os.Create(path)
		if err != nil {
			b.Fatal(err)
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			b.Fatal(err)
		}
		defer f.Close()
	}

	b.ResetTimer()
	time.Sleep(o.window)
	b.StopTimer()
	if os.Getenv("BENCH_WINDOW_CPUPROFILE") != "" {
		pprof.StopCPUProfile()
	}

	cpuUsed := cpuTime() - cpu0
	wall := time.Since(start)
	readMemStats(&ms1)

	// Guard: all connections must still be alive. If clients churn (e.g. via
	// DisconnectNoPong) we would be measuring connect/close, not periodic events.
	if got := node.hub.NumClients(); got != o.numConns {
		b.Fatalf("connection churn detected: %d of %d clients alive — benchmark invalid", got, o.numConns)
	}

	var pings int64
	for _, tr := range transports {
		pings += tr.writes.Load()
	}
	pings -= writesBefore
	if pings == 0 {
		b.Fatal("no pings observed — benchmark is not measuring anything")
	}
	if !o.idle {
		// Guard: observed events must be close to what the ping interval implies.
		expected := float64(o.numConns) * (float64(o.window) / float64(o.pingInterval))
		if float64(pings) < 0.6*expected || float64(pings) > 1.5*expected {
			b.Fatalf("event count %d far from expected ~%.0f — benchmark not in steady state", pings, expected)
		}
	}
	allocs := ms1.Mallocs - ms0.Mallocs
	bytes := ms1.TotalAlloc - ms0.TotalAlloc

	// Absolute process CPU: the headline for the idle case.
	b.ReportMetric(float64(cpuUsed)/float64(wall), "cpu-cores")
	b.ReportMetric(float64(ms1.NumGC-ms0.NumGC), "gc-cycles")
	b.ReportMetric(float64(ms1.PauseTotalNs-ms0.PauseTotalNs)/1e6, "gc-pause-ms")
	b.ReportMetric(float64(ms1.HeapAlloc)/(1024*1024), "heap-MB")
	if !o.idle {
		b.ReportMetric(float64(cpuUsed.Nanoseconds())/float64(pings), "cpu-ns/event")
		b.ReportMetric(float64(allocs)/float64(pings), "allocs/event")
		b.ReportMetric(float64(bytes)/float64(pings), "B/event")
	}
	b.ReportMetric(float64(pings), "events")
}

type noopPresenceManager struct{}

func (m *noopPresenceManager) Presence(_ string) (map[string]*ClientInfo, error) {
	return nil, nil
}
func (m *noopPresenceManager) PresenceStats(_ string) (PresenceStats, error) {
	return PresenceStats{}, nil
}
func (m *noopPresenceManager) AddPresence(_ string, _ string, _ *ClientInfo) error { return nil }
func (m *noopPresenceManager) RemovePresence(_ string, _ string, _ string) error   { return nil }

// Idle connections, ping only (no presence channels) — the case
// batch_periodic_events explicitly targets.
func BenchmarkPeriodic_PingOnly_Runtime_10k(b *testing.B) {
	benchPeriodicEvents(b, benchTimerOpts{
		useWheel: false, numConns: 10000, numChannels: 0,
		pingInterval: 1 * time.Second, presenceIvl: 25 * time.Second, window: 3 * time.Second,
	})
}

func BenchmarkPeriodic_PingOnly_Wheel_10k(b *testing.B) {
	benchPeriodicEvents(b, benchTimerOpts{
		useWheel: true, numConns: 10000, numChannels: 0,
		pingInterval: 1 * time.Second, presenceIvl: 25 * time.Second, window: 3 * time.Second,
	})
}

// Connections with presence channels — ping + presence ticks.
func BenchmarkPeriodic_Presence_Runtime_10k(b *testing.B) {
	benchPeriodicEvents(b, benchTimerOpts{
		useWheel: false, numConns: 10000, numChannels: 8,
		pingInterval: 1 * time.Second, presenceIvl: 1 * time.Second, window: 3 * time.Second,
	})
}

func BenchmarkPeriodic_Presence_Wheel_10k(b *testing.B) {
	benchPeriodicEvents(b, benchTimerOpts{
		useWheel: true, numConns: 10000, numChannels: 8,
		pingInterval: 1 * time.Second, presenceIvl: 1 * time.Second, window: 3 * time.Second,
	})
}

type runtimeMemStats = runtime.MemStats

func readMemStats(m *runtime.MemStats) { runtime.ReadMemStats(m) }

// Higher connection counts — where per-connection timer cost should dominate.
func BenchmarkPeriodic_PingOnly_Runtime_100k(b *testing.B) {
	benchPeriodicEvents(b, benchTimerOpts{
		useWheel: false, numConns: 100000, numChannels: 0,
		pingInterval: 1 * time.Second, presenceIvl: 25 * time.Second, window: 5 * time.Second,
	})
}

func BenchmarkPeriodic_PingOnly_Wheel_100k(b *testing.B) {
	benchPeriodicEvents(b, benchTimerOpts{
		useWheel: true, numConns: 100000, numChannels: 0,
		pingInterval: 1 * time.Second, presenceIvl: 25 * time.Second, window: 5 * time.Second,
	})
}

func BenchmarkPeriodic_PingOnly_Runtime_300k(b *testing.B) {
	benchPeriodicEvents(b, benchTimerOpts{
		useWheel: false, numConns: 300000, numChannels: 0,
		pingInterval: 1 * time.Second, presenceIvl: 25 * time.Second, window: 5 * time.Second,
	})
}

func BenchmarkPeriodic_PingOnly_Wheel_300k(b *testing.B) {
	benchPeriodicEvents(b, benchTimerOpts{
		useWheel: true, numConns: 300000, numChannels: 0,
		pingInterval: 1 * time.Second, presenceIvl: 25 * time.Second, window: 5 * time.Second,
	})
}

// ---- Idle connections with a REALISTIC ping interval ----
// This is the scenario batch_periodic_events targets: many mostly-idle
// connections. Headline metric is absolute process CPU, not CPU per event.

func BenchmarkIdle_Runtime_100k(b *testing.B) {
	benchPeriodicEvents(b, benchTimerOpts{
		useWheel: false, numConns: 100000, numChannels: 0, idle: true,
		pingInterval: 25 * time.Second, presenceIvl: 25 * time.Second, window: 30 * time.Second,
	})
}

func BenchmarkIdle_Wheel_100k(b *testing.B) {
	benchPeriodicEvents(b, benchTimerOpts{
		useWheel: true, numConns: 100000, numChannels: 0, idle: true,
		pingInterval: 25 * time.Second, presenceIvl: 25 * time.Second, window: 30 * time.Second,
	})
}

func BenchmarkIdle_Runtime_200k(b *testing.B) {
	benchPeriodicEvents(b, benchTimerOpts{
		useWheel: false, numConns: 200000, numChannels: 0, idle: true,
		pingInterval: 25 * time.Second, presenceIvl: 25 * time.Second, window: 30 * time.Second,
	})
}

func BenchmarkIdle_Wheel_200k(b *testing.B) {
	benchPeriodicEvents(b, benchTimerOpts{
		useWheel: true, numConns: 200000, numChannels: 0, idle: true,
		pingInterval: 25 * time.Second, presenceIvl: 25 * time.Second, window: 30 * time.Second,
	})
}

// 1M idle connections — the production scale this work is aimed at. Setup is
// slow (~minutes), so keep the count low when running these.
func BenchmarkIdle_Runtime_1M(b *testing.B) {
	benchPeriodicEvents(b, benchTimerOpts{
		useWheel: false, numConns: 1000000, numChannels: 0, idle: true,
		pingInterval: 25 * time.Second, presenceIvl: 25 * time.Second, window: 30 * time.Second,
	})
}

func BenchmarkIdle_Wheel_1M(b *testing.B) {
	benchPeriodicEvents(b, benchTimerOpts{
		useWheel: true, numConns: 1000000, numChannels: 0, idle: true,
		pingInterval: 25 * time.Second, presenceIvl: 25 * time.Second, window: 30 * time.Second,
	})
}

// Shard-count sweep at 1M idle connections. The wheel serializes AddTimer on a
// per-shard mutex, so with PRO's 4 shards a 1M-connection node funnels ~40k
// AddTimer/s through 4 locks.
func BenchmarkIdle_Wheel_1M_Shards16(b *testing.B) {
	benchPeriodicEvents(b, benchTimerOpts{
		useWheel: true, wheelShards: 16, numConns: 1000000, numChannels: 0, idle: true,
		pingInterval: 25 * time.Second, presenceIvl: 25 * time.Second, window: 30 * time.Second,
	})
}

func BenchmarkIdle_Wheel_1M_Shards64(b *testing.B) {
	benchPeriodicEvents(b, benchTimerOpts{
		useWheel: true, wheelShards: 64, numConns: 1000000, numChannels: 0, idle: true,
		pingInterval: 25 * time.Second, presenceIvl: 25 * time.Second, window: 30 * time.Second,
	})
}

func BenchmarkIdle_Wheel_1M_Shards256(b *testing.B) {
	benchPeriodicEvents(b, benchTimerOpts{
		useWheel: true, wheelShards: 256, numConns: 1000000, numChannels: 0, idle: true,
		pingInterval: 25 * time.Second, presenceIvl: 25 * time.Second, window: 30 * time.Second,
	})
}

// Shard count at a SMALL connection count: raising the wheel's default shards
// must not cost anything for deployments that are not at 1M. Each shard runs its
// own ticker goroutine, so more shards means more idle wakeups.
func BenchmarkIdle_Wheel_10k_Shards4(b *testing.B) {
	benchPeriodicEvents(b, benchTimerOpts{
		useWheel: true, wheelShards: 4, numConns: 10000, numChannels: 0, idle: true,
		pingInterval: 25 * time.Second, presenceIvl: 25 * time.Second, window: 30 * time.Second,
	})
}

func BenchmarkIdle_Wheel_10k_Shards64(b *testing.B) {
	benchPeriodicEvents(b, benchTimerOpts{
		useWheel: true, wheelShards: 64, numConns: 10000, numChannels: 0, idle: true,
		pingInterval: 25 * time.Second, presenceIvl: 25 * time.Second, window: 30 * time.Second,
	})
}
