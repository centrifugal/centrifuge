//go:build integration
// +build integration

package centrifuge

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func setupPostgresMapEngineBench(b *testing.B) (*PostgresMapEngine, func()) {
	b.Helper()

	connString := getPostgresConnString(b)

	node, _ := New(Config{})
	engine, err := NewPostgresMapEngine(node, PostgresMapEngineConfig{
		ConnString: connString,
	})
	if err != nil {
		b.Fatal(err)
	}
	_ = engine.RegisterBrokerEventHandler(nil)

	// Clean up tables
	ctx := context.Background()
	_, _ = engine.pool.Exec(ctx, "DELETE FROM cf_map_stream WHERE channel LIKE 'bench_%'")
	_, _ = engine.pool.Exec(ctx, "DELETE FROM cf_map_state WHERE channel LIKE 'bench_%'")
	_, _ = engine.pool.Exec(ctx, "DELETE FROM cf_map_meta WHERE channel LIKE 'bench_%'")
	_, _ = engine.pool.Exec(ctx, "DELETE FROM cf_map_idempotency WHERE channel LIKE 'bench_%'")

	return engine, func() {
		_ = engine.Close(context.Background())
		_ = node.Shutdown(context.Background())
	}
}

// BenchmarkPostgresMapEngine_PublishStreamOnly benchmarks publishing to stream.
func BenchmarkPostgresMapEngine_PublishStreamOnly(b *testing.B) {
	engine, cleanup := setupPostgresMapEngineBench(b)
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
			_, err := engine.Publish(ctx, channel, "", MapPublishOptions{
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

// BenchmarkPostgresMapEngine_PublishMapStateSimple benchmarks simple keyed state.
func BenchmarkPostgresMapEngine_PublishMapStateSimple(b *testing.B) {
	engine, cleanup := setupPostgresMapEngineBench(b)
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
			_, err := engine.Publish(ctx, channel, key, MapPublishOptions{
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

// BenchmarkPostgresMapEngine_PublishMapStateOrdered benchmarks ordered keyed state.
func BenchmarkPostgresMapEngine_PublishMapStateOrdered(b *testing.B) {
	engine, cleanup := setupPostgresMapEngineBench(b)
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
			_, err := engine.Publish(ctx, channel, key, MapPublishOptions{
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

// BenchmarkPostgresMapEngine_PublishCombined benchmarks publishing with stream + snapshot.
func BenchmarkPostgresMapEngine_PublishCombined(b *testing.B) {
	engine, cleanup := setupPostgresMapEngineBench(b)
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
			_, err := engine.Publish(ctx, channel, key, MapPublishOptions{
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

// BenchmarkPostgresMapEngine_ReadStream benchmarks reading from stream.
func BenchmarkPostgresMapEngine_ReadStream(b *testing.B) {
	engine, cleanup := setupPostgresMapEngineBench(b)
	defer cleanup()

	ctx := context.Background()
	channel := "bench_read_stream"

	// Prepopulate stream with 1000 messages
	var sp StreamPosition
	for i := 0; i < 1000; i++ {
		data := []byte(fmt.Sprintf("message_%d", i))
		res, err := engine.Publish(ctx, channel, "", MapPublishOptions{
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
			_, _, err := engine.ReadStream(ctx, channel, MapReadStreamOptions{
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

// BenchmarkPostgresMapEngine_ReadStateFull benchmarks reading full unordered snapshot.
func BenchmarkPostgresMapEngine_ReadStateFull(b *testing.B) {
	engine, cleanup := setupPostgresMapEngineBench(b)
	defer cleanup()

	ctx := context.Background()
	channel := "bench_read_snapshot"

	// Prepopulate snapshot with 1000 entries
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key%d", i)
		data := []byte(fmt.Sprintf("data%d", i))
		_, err := engine.Publish(ctx, channel, key, MapPublishOptions{
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
			_, _, _, err := engine.ReadState(ctx, channel, MapReadStateOptions{
				Limit:       0, // Use default of 100
				StateTTL: 300 * time.Second,
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkPostgresMapEngine_ReadStatePaginated benchmarks paginated snapshot reads.
func BenchmarkPostgresMapEngine_ReadStatePaginated(b *testing.B) {
	engine, cleanup := setupPostgresMapEngineBench(b)
	defer cleanup()

	ctx := context.Background()
	channel := "bench_read_snapshot_paginated"

	// Prepopulate snapshot with 1000 entries
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key%d", i)
		data := []byte(fmt.Sprintf("data%d", i))
		_, err := engine.Publish(ctx, channel, key, MapPublishOptions{
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
			_, _, _, err := engine.ReadState(ctx, channel, MapReadStateOptions{
				Cursor:      "0",
				Limit:       100, // Read 100 at a time
				StateTTL: 300 * time.Second,
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkPostgresMapEngine_ReadStateOrdered benchmarks reading ordered snapshot.
func BenchmarkPostgresMapEngine_ReadStateOrdered(b *testing.B) {
	engine, cleanup := setupPostgresMapEngineBench(b)
	defer cleanup()

	ctx := context.Background()
	channel := "bench_read_snapshot_ordered"

	// Prepopulate ordered snapshot with 1000 entries
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key%d", i)
		data := []byte(fmt.Sprintf("data%d", i))
		_, err := engine.Publish(ctx, channel, key, MapPublishOptions{
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
			_, _, _, err := engine.ReadState(ctx, channel, MapReadStateOptions{
				Ordered:     true,
				Limit:       100,
				StateTTL: 300 * time.Second,
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkPostgresMapEngine_Stats benchmarks reading snapshot statistics.
func BenchmarkPostgresMapEngine_Stats(b *testing.B) {
	engine, cleanup := setupPostgresMapEngineBench(b)
	defer cleanup()

	ctx := context.Background()
	channel := "bench_stats"

	// Prepopulate snapshot with 1000 entries
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key%d", i)
		data := []byte(fmt.Sprintf("data%d", i))
		_, err := engine.Publish(ctx, channel, key, MapPublishOptions{
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
			_, err := engine.Stats(ctx, channel)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkPostgresMapEngine_Remove benchmarks removing keys.
func BenchmarkPostgresMapEngine_Remove(b *testing.B) {
	engine, cleanup := setupPostgresMapEngineBench(b)
	defer cleanup()

	ctx := context.Background()
	channel := "bench_remove"

	// Prepopulate with keys
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("key%d", i)
		data := []byte(fmt.Sprintf("data%d", i))
		_, err := engine.Publish(ctx, channel, key, MapPublishOptions{
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
			_, err := engine.Remove(ctx, channel, key, MapRemoveOptions{
				StreamSize: -1, // Disable size and rely only on TTL for better efficiency.
				StreamTTL:  300 * time.Second,
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkPostgresMapEngine_IdempotentPublish benchmarks idempotent publishing.
func BenchmarkPostgresMapEngine_IdempotentPublish(b *testing.B) {
	engine, cleanup := setupPostgresMapEngineBench(b)
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
			_, err := engine.Publish(ctx, channel, "", MapPublishOptions{
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

// BenchmarkPostgresMapEngine_CAS benchmarks CAS (Compare-And-Swap) operations.
func BenchmarkPostgresMapEngine_CAS(b *testing.B) {
	engine, cleanup := setupPostgresMapEngineBench(b)
	defer cleanup()

	ctx := context.Background()
	channel := "bench_cas"

	// Create initial key
	res, err := engine.Publish(ctx, channel, "shared_counter", MapPublishOptions{
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
			entries, pos, _, err := engine.ReadState(ctx, channel, MapReadStateOptions{
				Key: "shared_counter",
			})
			if err != nil || len(entries) == 0 {
				continue
			}

			// Attempt CAS
			expectedPos := StreamPosition{Offset: entries[0].Offset, Epoch: pos.Epoch}
			_, _ = engine.Publish(ctx, channel, "shared_counter", MapPublishOptions{
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

func setupPostgresMapEngineOutboxBench(b *testing.B) (*PostgresMapEngine, func()) {
	b.Helper()

	connString := getPostgresConnString(b)

	node, _ := New(Config{})
	// Use fewer shards than pool size to leave connections available for Publish calls.
	// Each outbox worker holds a connection for its advisory lock.
	engine, err := NewPostgresMapEngine(node, PostgresMapEngineConfig{
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
	_ = engine.RegisterBrokerEventHandler(nil)

	// Clean up tables
	ctx := context.Background()
	_, _ = engine.pool.Exec(ctx, "DELETE FROM cf_map_stream WHERE channel LIKE 'bench_%'")
	_, _ = engine.pool.Exec(ctx, "DELETE FROM cf_map_state WHERE channel LIKE 'bench_%'")
	_, _ = engine.pool.Exec(ctx, "DELETE FROM cf_map_meta WHERE channel LIKE 'bench_%'")
	_, _ = engine.pool.Exec(ctx, "DELETE FROM cf_map_idempotency WHERE channel LIKE 'bench_%'")
	_, _ = engine.pool.Exec(ctx, "DELETE FROM cf_map_outbox WHERE channel LIKE 'bench_%'")

	return engine, func() {
		_ = engine.Close(context.Background())
		_ = node.Shutdown(context.Background())
	}
}

// setupPostgresMapEngineOutboxBenchWithHandler creates engine and registers handler.
func setupPostgresMapEngineOutboxBenchWithHandler(b *testing.B, handler BrokerEventHandler) (*PostgresMapEngine, func()) {
	b.Helper()

	connString := getPostgresConnString(b)

	node, _ := New(Config{})
	// Use fewer shards than pool size to leave connections available for Publish calls.
	// Each outbox worker holds a connection for its advisory lock.
	engine, err := NewPostgresMapEngine(node, PostgresMapEngineConfig{
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
	_, _ = engine.pool.Exec(ctx, "DELETE FROM cf_map_stream WHERE channel LIKE 'bench_%'")
	_, _ = engine.pool.Exec(ctx, "DELETE FROM cf_map_state WHERE channel LIKE 'bench_%'")
	_, _ = engine.pool.Exec(ctx, "DELETE FROM cf_map_meta WHERE channel LIKE 'bench_%'")
	_, _ = engine.pool.Exec(ctx, "DELETE FROM cf_map_idempotency WHERE channel LIKE 'bench_%'")
	_, _ = engine.pool.Exec(ctx, "DELETE FROM cf_map_outbox WHERE channel LIKE 'bench_%'")

	// Register handler which starts workers
	_ = engine.RegisterBrokerEventHandler(handler)

	return engine, func() {
		_ = engine.Close(context.Background())
		_ = node.Shutdown(context.Background())
	}
}

// BenchmarkPostgresMapEngine_OutboxPublish benchmarks publishing with outbox mode.
func BenchmarkPostgresMapEngine_OutboxPublish(b *testing.B) {
	engine, cleanup := setupPostgresMapEngineOutboxBench(b)
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
			_, err := engine.Publish(ctx, channel, key, MapPublishOptions{
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

// BenchmarkPostgresMapEngine_OutboxThroughput measures outbox delivery throughput.
func BenchmarkPostgresMapEngine_OutboxThroughput(b *testing.B) {
	ctx := context.Background()
	channel := fmt.Sprintf("bench_outbox_throughput_%d", time.Now().UnixNano())

	// Track deliveries
	var delivered int64
	doneCh := make(chan struct{})

	engine, cleanup := setupPostgresMapEngineOutboxBenchWithHandler(b, &testBrokerEventHandler{
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
		if len(engine.OutboxClaimedShards()) > 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if len(engine.OutboxClaimedShards()) == 0 {
		b.Fatal("no outbox worker claimed any shards")
	}

	b.ReportAllocs()
	b.ResetTimer()

	// Publish all messages first
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%d", i)
		data := []byte(fmt.Sprintf("data%d", i))
		_, err := engine.Publish(ctx, channel, key, MapPublishOptions{
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

// BenchmarkPostgresMapEngine_OutboxLatency measures publish-to-delivery latency.
func BenchmarkPostgresMapEngine_OutboxLatency(b *testing.B) {
	ctx := context.Background()
	channel := fmt.Sprintf("bench_outbox_latency_%d", time.Now().UnixNano())

	latencies := make(chan time.Duration, b.N)
	publishTimes := make(map[string]time.Time)
	var mu sync.Mutex

	engine, cleanup := setupPostgresMapEngineOutboxBenchWithHandler(b, &testBrokerEventHandler{
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
		if len(engine.OutboxClaimedShards()) > 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if len(engine.OutboxClaimedShards()) == 0 {
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

		_, err := engine.Publish(ctx, channel, key, MapPublishOptions{
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
