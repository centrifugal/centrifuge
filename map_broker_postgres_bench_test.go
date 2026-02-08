//go:build integration

package centrifuge

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func setupPostgresMapBrokerBench(b *testing.B) (*PostgresMapBroker, func()) {
	b.Helper()

	connString := getPostgresConnString(b)

	node, _ := New(Config{})
	broker, err := NewPostgresMapBroker(node, PostgresMapBrokerConfig{
		ConnString: connString,
	})
	if err != nil {
		b.Fatal(err)
	}
	_ = broker.RegisterEventHandler(nil)

	// Clean up tables
	ctx := context.Background()
	_, _ = broker.pool.Exec(ctx, "DELETE FROM cf_map_stream WHERE channel LIKE 'bench_%'")
	_, _ = broker.pool.Exec(ctx, "DELETE FROM cf_map_state WHERE channel LIKE 'bench_%'")
	_, _ = broker.pool.Exec(ctx, "DELETE FROM cf_map_meta WHERE channel LIKE 'bench_%'")
	_, _ = broker.pool.Exec(ctx, "DELETE FROM cf_map_idempotency WHERE channel LIKE 'bench_%'")

	return broker, func() {
		_ = broker.Close(context.Background())
		_ = node.Shutdown(context.Background())
	}
}

// BenchmarkPostgresMapBroker_PublishStreamOnly benchmarks publishing to stream.
func BenchmarkPostgresMapBroker_PublishStreamOnly(b *testing.B) {
	broker, cleanup := setupPostgresMapBrokerBench(b)
	defer cleanup()

	ctx := context.Background()
	channel := "bench_stream"

	b.ReportAllocs()
	b.ResetTimer()

	var counter int64
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := atomic.AddInt64(&counter, 1)
			data := []byte(fmt.Sprintf("message_%d", i))
			_, err := broker.Publish(ctx, channel, "", MapPublishOptions{
				Data:       data,
				StreamSize: -1, // Disable size and rely only on TTL for better efficiency.
				StreamTTL:  300 * time.Second,
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkPostgresMapBroker_PublishMapStateSimple benchmarks simple keyed state.
func BenchmarkPostgresMapBroker_PublishMapStateSimple(b *testing.B) {
	broker, cleanup := setupPostgresMapBrokerBench(b)
	defer cleanup()

	ctx := context.Background()
	channel := "bench_map_simple"

	b.ReportAllocs()
	b.ResetTimer()

	var counter int64
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := atomic.AddInt64(&counter, 1)
			key := fmt.Sprintf("key%d", i)
			data := []byte(fmt.Sprintf("data%d", i))
			_, err := broker.Publish(ctx, channel, key, MapPublishOptions{
				Data:       data,
				StreamSize: -1, // Disable size and rely only on TTL for better efficiency.
				StreamTTL:  300 * time.Second,
				KeyTTL:     300 * time.Second,
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkPostgresMapBroker_PublishMapStateOrdered benchmarks ordered keyed state.
func BenchmarkPostgresMapBroker_PublishMapStateOrdered(b *testing.B) {
	broker, cleanup := setupPostgresMapBrokerBench(b)
	defer cleanup()

	ctx := context.Background()
	channel := "bench_map_ordered"

	b.ReportAllocs()
	b.ResetTimer()

	var counter int64
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := atomic.AddInt64(&counter, 1)
			key := fmt.Sprintf("key%d", i)
			data := []byte(fmt.Sprintf("data%d", i))
			_, err := broker.Publish(ctx, channel, key, MapPublishOptions{
				Data:       data,
				Ordered:    true,
				Score:      i,
				StreamSize: -1, // Disable size and rely only on TTL for better efficiency.
				StreamTTL:  300 * time.Second,
				KeyTTL:     300 * time.Second,
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkPostgresMapBroker_PublishCombined benchmarks publishing with stream + state.
func BenchmarkPostgresMapBroker_PublishCombined(b *testing.B) {
	broker, cleanup := setupPostgresMapBrokerBench(b)
	defer cleanup()

	ctx := context.Background()
	channel := "bench_combined"

	b.ReportAllocs()
	b.ResetTimer()

	var counter int64
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := atomic.AddInt64(&counter, 1)
			key := fmt.Sprintf("key%d", i)
			data := []byte(fmt.Sprintf("data%d", i))
			_, err := broker.Publish(ctx, channel, key, MapPublishOptions{
				Data:       data,
				StreamSize: -1, // Disable size and rely only on TTL for better efficiency.
				StreamTTL:  300 * time.Second,
				KeyTTL:     300 * time.Second,
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkPostgresMapBroker_ReadStream benchmarks reading from stream.
func BenchmarkPostgresMapBroker_ReadStream(b *testing.B) {
	broker, cleanup := setupPostgresMapBrokerBench(b)
	defer cleanup()

	ctx := context.Background()
	channel := "bench_read_stream"

	// Prepopulate stream with 1000 messages
	var sp StreamPosition
	for i := 0; i < 1000; i++ {
		data := []byte(fmt.Sprintf("message_%d", i))
		res, err := broker.Publish(ctx, channel, "", MapPublishOptions{
			Data:       data,
			StreamSize: -1, // Disable size and rely only on TTL for better efficiency.
			StreamTTL:  300 * time.Second,
		})
		if err != nil {
			b.Fatal(err)
		}
		sp = res.Position
	}

	b.ReportAllocs()
	b.ResetTimer()

	sp.Offset = sp.Offset - 1000

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _, err := broker.ReadStream(ctx, channel, MapReadStreamOptions{
				Filter: StreamFilter{
					Limit: 1000,
					Since: &sp,
				},
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkPostgresMapBroker_ReadStateFull benchmarks reading full unordered state.
func BenchmarkPostgresMapBroker_ReadStateFull(b *testing.B) {
	broker, cleanup := setupPostgresMapBrokerBench(b)
	defer cleanup()

	ctx := context.Background()
	channel := "bench_read_state"

	// Prepopulate state with 1000 entries
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key%d", i)
		data := []byte(fmt.Sprintf("data%d", i))
		_, err := broker.Publish(ctx, channel, key, MapPublishOptions{
			Data:       data,
			StreamSize: -1, // Disable size and rely only on TTL for better efficiency.
			StreamTTL:  300 * time.Second,
			KeyTTL:     300 * time.Second,
		})
		if err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _, _, err := broker.ReadState(ctx, channel, MapReadStateOptions{
				Limit:    0, // Use default of 100
				StateTTL: 300 * time.Second,
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkPostgresMapBroker_ReadStatePaginated benchmarks paginated state reads.
func BenchmarkPostgresMapBroker_ReadStatePaginated(b *testing.B) {
	broker, cleanup := setupPostgresMapBrokerBench(b)
	defer cleanup()

	ctx := context.Background()
	channel := "bench_read_state_paginated"

	// Prepopulate state with 1000 entries
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key%d", i)
		data := []byte(fmt.Sprintf("data%d", i))
		_, err := broker.Publish(ctx, channel, key, MapPublishOptions{
			Data:       data,
			StreamSize: -1, // Disable size and rely only on TTL for better efficiency.
			StreamTTL:  300 * time.Second,
			KeyTTL:     300 * time.Second,
		})
		if err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _, _, err := broker.ReadState(ctx, channel, MapReadStateOptions{
				Cursor:   "0",
				Limit:    100, // Read 100 at a time
				StateTTL: 300 * time.Second,
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkPostgresMapBroker_ReadStateOrdered benchmarks reading ordered state.
func BenchmarkPostgresMapBroker_ReadStateOrdered(b *testing.B) {
	broker, cleanup := setupPostgresMapBrokerBench(b)
	defer cleanup()

	ctx := context.Background()
	channel := "bench_read_state_ordered"

	// Prepopulate ordered state with 1000 entries
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key%d", i)
		data := []byte(fmt.Sprintf("data%d", i))
		_, err := broker.Publish(ctx, channel, key, MapPublishOptions{
			Data:       data,
			Ordered:    true,
			Score:      int64(i),
			StreamSize: -1, // Disable size and rely only on TTL for better efficiency.
			StreamTTL:  300 * time.Second,
			KeyTTL:     300 * time.Second,
		})
		if err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _, _, err := broker.ReadState(ctx, channel, MapReadStateOptions{
				Ordered:  true,
				Limit:    100,
				StateTTL: 300 * time.Second,
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkPostgresMapBroker_Stats benchmarks reading state statistics.
func BenchmarkPostgresMapBroker_Stats(b *testing.B) {
	broker, cleanup := setupPostgresMapBrokerBench(b)
	defer cleanup()

	ctx := context.Background()
	channel := "bench_stats"

	// Prepopulate state with 1000 entries
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key%d", i)
		data := []byte(fmt.Sprintf("data%d", i))
		_, err := broker.Publish(ctx, channel, key, MapPublishOptions{
			Data:       data,
			StreamSize: -1, // Disable size and rely only on TTL for better efficiency.
			StreamTTL:  300 * time.Second,
			KeyTTL:     300 * time.Second,
		})
		if err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := broker.Stats(ctx, channel)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkPostgresMapBroker_Remove benchmarks removing keys.
func BenchmarkPostgresMapBroker_Remove(b *testing.B) {
	broker, cleanup := setupPostgresMapBrokerBench(b)
	defer cleanup()

	ctx := context.Background()
	channel := "bench_remove"

	// Prepopulate with keys
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("key%d", i)
		data := []byte(fmt.Sprintf("data%d", i))
		_, err := broker.Publish(ctx, channel, key, MapPublishOptions{
			Data:       data,
			StreamSize: -1, // Disable size and rely only on TTL for better efficiency.
			StreamTTL:  300 * time.Second,
			KeyTTL:     300 * time.Second,
		})
		if err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()

	var counter int64
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := atomic.AddInt64(&counter, 1)
			key := fmt.Sprintf("key%d", i)
			_, err := broker.Remove(ctx, channel, key, MapRemoveOptions{
				StreamSize: -1, // Disable size and rely only on TTL for better efficiency.
				StreamTTL:  300 * time.Second,
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkPostgresMapBroker_IdempotentPublish benchmarks idempotent publishing.
func BenchmarkPostgresMapBroker_IdempotentPublish(b *testing.B) {
	broker, cleanup := setupPostgresMapBrokerBench(b)
	defer cleanup()

	ctx := context.Background()
	channel := "bench_idempotent"

	b.ReportAllocs()
	b.ResetTimer()

	var counter int64
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := atomic.AddInt64(&counter, 1)
			data := []byte(fmt.Sprintf("message_%d", i))
			idempotencyKey := fmt.Sprintf("key_%d", i)
			_, err := broker.Publish(ctx, channel, "", MapPublishOptions{
				Data:                data,
				IdempotencyKey:      idempotencyKey,
				IdempotentResultTTL: 60 * time.Second,
				//StreamSize:          10000, // This is rather expensive, rely on TTL fits PG better.
				StreamTTL: 300 * time.Second,
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkPostgresMapBroker_CAS benchmarks CAS (Compare-And-Swap) operations.
func BenchmarkPostgresMapBroker_CAS(b *testing.B) {
	broker, cleanup := setupPostgresMapBrokerBench(b)
	defer cleanup()

	ctx := context.Background()
	channel := "bench_cas"

	// Create initial key
	res, err := broker.Publish(ctx, channel, "shared_counter", MapPublishOptions{
		Data:       []byte("0"),
		StreamSize: -1, // Disable size and rely only on TTL for better efficiency.
		StreamTTL:  300 * time.Second,
	})
	if err != nil {
		b.Fatal(err)
	}
	lastPos := res.Position

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			// Read current state
			entries, pos, _, err := broker.ReadState(ctx, channel, MapReadStateOptions{
				Key: "shared_counter",
			})
			if err != nil || len(entries) == 0 {
				continue
			}

			// Attempt CAS
			expectedPos := StreamPosition{Offset: entries[0].Offset, Epoch: pos.Epoch}
			_, _ = broker.Publish(ctx, channel, "shared_counter", MapPublishOptions{
				Data:             []byte("updated"),
				ExpectedPosition: &expectedPos,
				//StreamSize:       10000, // This is rather expensive, rely on TTL fits PG better.
				StreamTTL: 300 * time.Second,
			})
		}
	})
	_ = lastPos
}

// ============================================================================
// Outbox Mode Benchmarks
// ============================================================================

func setupPostgresMapBrokerOutboxBench(b *testing.B) (*PostgresMapBroker, func()) {
	b.Helper()

	connString := getPostgresConnString(b)

	node, _ := New(Config{})
	// Use fewer shards than pool size to leave connections available for Publish calls.
	// Each outbox worker holds a connection for its advisory lock.
	broker, err := NewPostgresMapBroker(node, PostgresMapBrokerConfig{
		ConnString: connString,
		PoolSize:   32,
		Outbox: OutboxConfig{
			NumShards:    8, // Must be less than PoolSize to leave room for Publish
			PollInterval: 10 * time.Millisecond,
			BatchSize:    1000,
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	_ = broker.RegisterEventHandler(nil)

	// Clean up tables
	ctx := context.Background()
	_, _ = broker.pool.Exec(ctx, "DELETE FROM cf_map_stream WHERE channel LIKE 'bench_%'")
	_, _ = broker.pool.Exec(ctx, "DELETE FROM cf_map_state WHERE channel LIKE 'bench_%'")
	_, _ = broker.pool.Exec(ctx, "DELETE FROM cf_map_meta WHERE channel LIKE 'bench_%'")
	_, _ = broker.pool.Exec(ctx, "DELETE FROM cf_map_idempotency WHERE channel LIKE 'bench_%'")
	_, _ = broker.pool.Exec(ctx, "DELETE FROM cf_map_outbox WHERE channel LIKE 'bench_%'")

	return broker, func() {
		_ = broker.Close(context.Background())
		_ = node.Shutdown(context.Background())
	}
}

// setupPostgresMapBrokerOutboxBenchWithHandler creates broker and registers handler.
func setupPostgresMapBrokerOutboxBenchWithHandler(b *testing.B, handler BrokerEventHandler) (*PostgresMapBroker, func()) {
	b.Helper()

	connString := getPostgresConnString(b)

	node, _ := New(Config{})
	// Use fewer shards than pool size to leave connections available for Publish calls.
	// Each outbox worker holds a connection for its advisory lock.
	broker, err := NewPostgresMapBroker(node, PostgresMapBrokerConfig{
		ConnString: connString,
		PoolSize:   32,
		Outbox: OutboxConfig{
			NumShards:    8, // Must be less than PoolSize to leave room for Publish
			PollInterval: 5 * time.Millisecond,
			BatchSize:    1000,
		},
	})
	if err != nil {
		b.Fatal(err)
	}

	// Clean up tables before registering handler (which starts workers)
	ctx := context.Background()
	_, _ = broker.pool.Exec(ctx, "DELETE FROM cf_map_stream WHERE channel LIKE 'bench_%'")
	_, _ = broker.pool.Exec(ctx, "DELETE FROM cf_map_state WHERE channel LIKE 'bench_%'")
	_, _ = broker.pool.Exec(ctx, "DELETE FROM cf_map_meta WHERE channel LIKE 'bench_%'")
	_, _ = broker.pool.Exec(ctx, "DELETE FROM cf_map_idempotency WHERE channel LIKE 'bench_%'")
	_, _ = broker.pool.Exec(ctx, "DELETE FROM cf_map_outbox WHERE channel LIKE 'bench_%'")

	// Register handler which starts workers
	_ = broker.RegisterEventHandler(handler)

	return broker, func() {
		_ = broker.Close(context.Background())
		_ = node.Shutdown(context.Background())
	}
}

// BenchmarkPostgresMapBroker_OutboxPublish benchmarks publishing with outbox mode.
func BenchmarkPostgresMapBroker_OutboxPublish(b *testing.B) {
	broker, cleanup := setupPostgresMapBrokerOutboxBench(b)
	defer cleanup()

	ctx := context.Background()
	channel := "bench_outbox_publish"

	b.ReportAllocs()
	b.ResetTimer()

	var counter int64
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := atomic.AddInt64(&counter, 1)
			key := fmt.Sprintf("key%d", i)
			data := []byte(fmt.Sprintf("data%d", i))
			_, err := broker.Publish(ctx, channel, key, MapPublishOptions{
				Data:       data,
				StreamSize: -1,
				StreamTTL:  300 * time.Second,
				KeyTTL:     300 * time.Second,
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkPostgresMapBroker_OutboxThroughput measures outbox delivery throughput.
func BenchmarkPostgresMapBroker_OutboxThroughput(b *testing.B) {
	ctx := context.Background()
	channel := fmt.Sprintf("bench_outbox_throughput_%d", time.Now().UnixNano())

	// Track deliveries
	var delivered int64
	doneCh := make(chan struct{})

	broker, cleanup := setupPostgresMapBrokerOutboxBenchWithHandler(b, &testBrokerEventHandler{
		HandlePublicationFunc: func(ch string, pub *Publication, sp StreamPosition, delta bool, prevPub *Publication) error {
			if ch != channel {
				return nil
			}
			if atomic.AddInt64(&delivered, 1) >= int64(b.N) {
				select {
				case <-doneCh:
				default:
					close(doneCh)
				}
			}
			return nil
		},
	})
	defer cleanup()

	// Wait for at least one outbox worker to claim a shard
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if len(broker.OutboxClaimedShards()) > 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if len(broker.OutboxClaimedShards()) == 0 {
		b.Fatal("no outbox worker claimed any shards")
	}

	b.ReportAllocs()
	b.ResetTimer()

	// Publish all messages first
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%d", i)
		data := []byte(fmt.Sprintf("data%d", i))
		_, err := broker.Publish(ctx, channel, key, MapPublishOptions{
			Data:       data,
			StreamSize: -1,
			StreamTTL:  300 * time.Second,
		})
		if err != nil {
			b.Fatal(err)
		}
	}

	// Wait for all deliveries
	select {
	case <-doneCh:
	case <-time.After(60 * time.Second):
		b.Fatalf("timeout: only delivered %d/%d", atomic.LoadInt64(&delivered), b.N)
	}

	b.StopTimer()
}

// BenchmarkPostgresMapBroker_OutboxLatency measures publish-to-delivery latency.
func BenchmarkPostgresMapBroker_OutboxLatency(b *testing.B) {
	ctx := context.Background()
	channel := fmt.Sprintf("bench_outbox_latency_%d", time.Now().UnixNano())

	latencies := make(chan time.Duration, b.N)
	publishTimes := make(map[string]time.Time)
	var mu sync.Mutex

	broker, cleanup := setupPostgresMapBrokerOutboxBenchWithHandler(b, &testBrokerEventHandler{
		HandlePublicationFunc: func(ch string, pub *Publication, sp StreamPosition, delta bool, prevPub *Publication) error {
			if ch != channel {
				return nil
			}
			mu.Lock()
			if publishTime, ok := publishTimes[pub.Key]; ok {
				latency := time.Since(publishTime)
				select {
				case latencies <- latency:
				default:
				}
				delete(publishTimes, pub.Key)
			}
			mu.Unlock()
			return nil
		},
	})
	defer cleanup()

	// Wait for at least one outbox worker to claim a shard
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if len(broker.OutboxClaimedShards()) > 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if len(broker.OutboxClaimedShards()) == 0 {
		b.Fatal("no outbox worker claimed any shards")
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%d", i)
		data := []byte(fmt.Sprintf("data%d", i))

		mu.Lock()
		publishTimes[key] = time.Now()
		mu.Unlock()

		_, err := broker.Publish(ctx, channel, key, MapPublishOptions{
			Data:       data,
			StreamSize: -1,
			StreamTTL:  300 * time.Second,
		})
		if err != nil {
			b.Fatal(err)
		}
	}

	// Collect latencies
	timeout := time.After(60 * time.Second)
	var totalLatency time.Duration
	var count int

loop:
	for {
		select {
		case l := <-latencies:
			totalLatency += l
			count++
			if count >= b.N {
				break loop
			}
		case <-timeout:
			break loop
		}
	}

	b.StopTimer()

	if count > 0 {
		avgLatency := totalLatency / time.Duration(count)
		b.ReportMetric(float64(avgLatency.Microseconds()), "us/op")
	}
}
