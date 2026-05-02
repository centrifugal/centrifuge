package centrifuge

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

// BenchmarkSharedPoll_RefreshCycle benchmarks the full refresh cycle:
// handler call → onRefreshResponse → version compare → broadcast.
// Backend latency is simulated; measures Centrifuge overhead.
func BenchmarkSharedPoll_RefreshCycle(b *testing.B) {
	latencies := []time.Duration{0, 1 * time.Millisecond, 10 * time.Millisecond}
	keyCounts := []int{100, 1000, 10000}

	for _, latency := range latencies {
		for _, numKeys := range keyCounts {
			name := fmt.Sprintf("latency=%v/keys=%d", latency, numKeys)
			b.Run(name, func(b *testing.B) {
				benchRefreshCycle(b, numKeys, latency)
			})
		}
	}
}

func benchRefreshCycle(b *testing.B, numKeys int, latency time.Duration) {
	var callCount atomic.Int64

	data := json.RawMessage(`{"votes":42,"title":"hello world"}`)

	node, err := New(Config{
		SharedPoll: SharedPollConfig{
			GetSharedPollChannelOptions: func(channel string) (SharedPollChannelOptions, bool) {
				return SharedPollChannelOptions{
					RefreshInterval:      time.Hour, // long: we drive cycles manually
					RefreshBatchSize:     1000,
					MaxKeysPerConnection: numKeys + 1,
				}, true
			},
		},
	})
	if err != nil {
		b.Fatal(err)
	}

	node.OnSharedPoll(func(ctx context.Context, event SharedPollEvent) (SharedPollResult, error) {
		if latency > 0 {
			time.Sleep(latency)
		}
		callCount.Add(1)
		items := make([]SharedPollRefreshItem, len(event.Items))
		for i, item := range event.Items {
			items[i] = SharedPollRefreshItem{
				Key:     item.Key,
				Data:    data,
				Version: uint64(callCount.Load()),
			}
		}
		return SharedPollResult{Items: items}, nil
	})

	if err := node.Run(); err != nil {
		b.Fatal(err)
	}
	defer func() { _ = node.Shutdown(context.Background()) }()

	// Populate channel state with items.
	mgr := node.sharedPollManager
	opts, _ := node.config.SharedPoll.GetSharedPollChannelOptions("bench:ch")
	for i := 0; i < numKeys; i++ {
		_, _, _ = mgr.track("bench:ch", opts, fmt.Sprintf("item_%d", i))
	}

	// Ensure keyed hub exists.
	node.keyedManager.getOrCreateChannel("bench:ch", keyedChannelOptions{})

	// Stop the background refresh worker and override RefreshInterval for
	// spread delay calculation (time.Hour would cause multi-minute chunk delays).
	s := mgr.channels["bench:ch"]
	s.mu.Lock()
	if s.workerCancel != nil {
		s.workerCancel()
	}
	s.opts.RefreshInterval = time.Millisecond
	s.mu.Unlock()
	time.Sleep(10 * time.Millisecond) // let worker goroutine exit

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		callCount.Store(int64(i + 1))
		s.runRefreshCycle(context.Background(), node, "bench:ch", mgr.sem)
	}

	b.ReportMetric(float64(callCount.Load()-int64(b.N)), "extra_calls")
}

// BenchmarkSharedPoll_LargeChannel benchmarks a single large channel
// with varying concurrency limits, similar to relay's LargeChannel benchmark.
func BenchmarkSharedPoll_LargeChannel(b *testing.B) {
	type scenario struct {
		latency     time.Duration
		concurrency int
	}
	scenarios := []scenario{
		{10 * time.Millisecond, 1},
		{10 * time.Millisecond, 10},
		{10 * time.Millisecond, 50},
		{10 * time.Millisecond, 100},
		{50 * time.Millisecond, 1},
		{50 * time.Millisecond, 10},
		{50 * time.Millisecond, 50},
		{50 * time.Millisecond, 100},
	}

	totalKeys := 100_000
	batchSize := 1000

	for _, sc := range scenarios {
		expected := time.Duration(float64(sc.latency) * float64(totalKeys/batchSize) / float64(sc.concurrency))
		name := fmt.Sprintf("latency=%v/conc=%d/keys=100k/expected_cycle=%v", sc.latency, sc.concurrency, expected)
		b.Run(name, func(b *testing.B) {
			benchLargeChannel(b, totalKeys, batchSize, sc.latency, sc.concurrency)
		})
	}
}

func benchLargeChannel(b *testing.B, totalKeys, batchSize int, latency time.Duration, concurrency int) {
	var batchCount atomic.Int64

	data := json.RawMessage(`{"votes":42}`)

	node, err := New(Config{
		SharedPoll: SharedPollConfig{
			ConcurrencyLimit: concurrency,
			GetSharedPollChannelOptions: func(channel string) (SharedPollChannelOptions, bool) {
				return SharedPollChannelOptions{
					RefreshInterval:      time.Hour, // long: we drive cycles manually
					RefreshBatchSize:     batchSize,
					MaxKeysPerConnection: totalKeys + 1,
				}, true
			},
		},
	})
	if err != nil {
		b.Fatal(err)
	}

	node.OnSharedPoll(func(ctx context.Context, event SharedPollEvent) (SharedPollResult, error) {
		if latency > 0 {
			time.Sleep(latency)
		}
		batchCount.Add(1)
		items := make([]SharedPollRefreshItem, len(event.Items))
		for i, item := range event.Items {
			items[i] = SharedPollRefreshItem{
				Key:     item.Key,
				Data:    data,
				Version: 1,
			}
		}
		return SharedPollResult{Items: items}, nil
	})

	if err := node.Run(); err != nil {
		b.Fatal(err)
	}
	defer func() { _ = node.Shutdown(context.Background()) }()

	mgr := node.sharedPollManager
	opts, _ := node.config.SharedPoll.GetSharedPollChannelOptions("bench:ch")
	for i := 0; i < totalKeys; i++ {
		_, _, _ = mgr.track("bench:ch", opts, fmt.Sprintf("item_%d", i))
	}

	node.keyedManager.getOrCreateChannel("bench:ch", keyedChannelOptions{})

	// Stop the background refresh worker and override RefreshInterval for
	// spread delay calculation (time.Hour would cause multi-minute chunk delays).
	s := mgr.channels["bench:ch"]
	s.mu.Lock()
	if s.workerCancel != nil {
		s.workerCancel()
	}
	s.opts.RefreshInterval = time.Millisecond
	s.mu.Unlock()
	time.Sleep(10 * time.Millisecond) // let worker goroutine exit

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		batchCount.Store(0)
		s.runRefreshCycle(context.Background(), node, "bench:ch", mgr.sem)
	}

	b.ReportMetric(float64(batchCount.Load()), "batches/poll")
}
