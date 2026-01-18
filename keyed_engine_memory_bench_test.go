package centrifuge

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

func setupMemoryKeyedEngineBench(b *testing.B) (*MemoryKeyedEngine, func()) {
	b.Helper()
	node, _ := New(Config{})
	engine, _ := NewMemoryKeyedEngine(node, MemoryKeyedEngineConfig{})
	_ = engine.RegisterBrokerEventHandler(nil)
	return engine, func() {
		_ = node.Shutdown(context.Background())
	}
}

// BenchmarkMemoryKeyedEngine_PublishStreamOnly benchmarks publishing to stream without snapshots.
func BenchmarkMemoryKeyedEngine_PublishStreamOnly(b *testing.B) {
	engine, cleanup := setupMemoryKeyedEngineBench(b)
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
			_, _, err := engine.Publish(ctx, channel, "", data, KeyedPublishOptions{
				StreamSize: 10000,
				StreamTTL:  300 * time.Second,
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkMemoryKeyedEngine_PublishKeyedStateSimple benchmarks simple keyed state.
func BenchmarkMemoryKeyedEngine_PublishKeyedStateSimple(b *testing.B) {
	engine, cleanup := setupMemoryKeyedEngineBench(b)
	defer cleanup()

	ctx := context.Background()
	channel := "bench_keyed_simple"

	b.ReportAllocs()
	b.ResetTimer()

	var counter int64
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := atomic.AddInt64(&counter, 1)
			key := fmt.Sprintf("key%d", i)
			data := []byte(fmt.Sprintf("data%d", i))
			_, _, err := engine.Publish(ctx, channel, key, data, KeyedPublishOptions{
				StreamSize: 10000,
				StreamTTL:  300 * time.Second,
				KeyTTL:     300 * time.Second,
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkMemoryKeyedEngine_PublishKeyedStateOrdered benchmarks ordered keyed state.
func BenchmarkMemoryKeyedEngine_PublishKeyedStateOrdered(b *testing.B) {
	engine, cleanup := setupMemoryKeyedEngineBench(b)
	defer cleanup()

	ctx := context.Background()
	channel := "bench_keyed_ordered"

	b.ReportAllocs()
	b.ResetTimer()

	var counter int64
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := atomic.AddInt64(&counter, 1)
			key := fmt.Sprintf("key%d", i)
			data := []byte(fmt.Sprintf("data%d", i))
			_, _, err := engine.Publish(ctx, channel, key, data, KeyedPublishOptions{
				Ordered:    true,
				Score:      i,
				StreamSize: 10000,
				StreamTTL:  300 * time.Second,
				KeyTTL:     300 * time.Second,
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkMemoryKeyedEngine_PublishCombined benchmarks publishing with stream + snapshot.
func BenchmarkMemoryKeyedEngine_PublishCombined(b *testing.B) {
	engine, cleanup := setupMemoryKeyedEngineBench(b)
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
			_, _, err := engine.Publish(ctx, channel, key, data, KeyedPublishOptions{
				StreamSize: 10000,
				StreamTTL:  300 * time.Second,
				KeyTTL:     300 * time.Second,
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkMemoryKeyedEngine_ReadStream benchmarks reading from stream.
func BenchmarkMemoryKeyedEngine_ReadStream(b *testing.B) {
	engine, cleanup := setupMemoryKeyedEngineBench(b)
	defer cleanup()

	ctx := context.Background()
	channel := "bench_read_stream"

	// Prepopulate stream with 1000 messages
	var sp StreamPosition
	for i := 0; i < 1000; i++ {
		data := []byte(fmt.Sprintf("message_%d", i))
		var err error
		sp, _, err = engine.Publish(ctx, channel, "", data, KeyedPublishOptions{
			StreamSize: 10000,
			StreamTTL:  300 * time.Second,
		})
		if err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()

	sp.Offset = sp.Offset - 1000

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _, err := engine.ReadStream(ctx, channel, KeyedReadStreamOptions{
				Filter: HistoryFilter{
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

// BenchmarkMemoryKeyedEngine_ReadSnapshotFull benchmarks reading full unordered snapshot.
func BenchmarkMemoryKeyedEngine_ReadSnapshotFull(b *testing.B) {
	engine, cleanup := setupMemoryKeyedEngineBench(b)
	defer cleanup()

	ctx := context.Background()
	channel := "bench_read_snapshot"

	// Prepopulate snapshot with 1000 entries
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key%d", i)
		data := []byte(fmt.Sprintf("data%d", i))
		_, _, err := engine.Publish(ctx, channel, key, data, KeyedPublishOptions{
			StreamSize: 10000,
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
			_, _, _, err := engine.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{
				Limit:       0, // Read all
				SnapshotTTL: 300 * time.Second,
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkMemoryKeyedEngine_ReadSnapshotPaginated benchmarks paginated snapshot reads.
func BenchmarkMemoryKeyedEngine_ReadSnapshotPaginated(b *testing.B) {
	engine, cleanup := setupMemoryKeyedEngineBench(b)
	defer cleanup()

	ctx := context.Background()
	channel := "bench_read_snapshot_paginated"

	// Prepopulate snapshot with 1000 entries
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key%d", i)
		data := []byte(fmt.Sprintf("data%d", i))
		_, _, err := engine.Publish(ctx, channel, key, data, KeyedPublishOptions{
			StreamSize: 10000,
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
			_, _, _, err := engine.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{
				Cursor:      "0",
				Limit:       100, // Read 100 at a time
				SnapshotTTL: 300 * time.Second,
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkMemoryKeyedEngine_ReadSnapshotOrdered benchmarks reading ordered snapshot.
func BenchmarkMemoryKeyedEngine_ReadSnapshotOrdered(b *testing.B) {
	engine, cleanup := setupMemoryKeyedEngineBench(b)
	defer cleanup()

	ctx := context.Background()
	channel := "bench_read_snapshot_ordered"

	// Prepopulate ordered snapshot with 1000 entries
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key%d", i)
		data := []byte(fmt.Sprintf("data%d", i))
		_, _, err := engine.Publish(ctx, channel, key, data, KeyedPublishOptions{
			Ordered:    true,
			Score:      int64(i),
			StreamSize: 10000,
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
			_, _, _, err := engine.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{
				Ordered:     true,
				Limit:       100,
				SnapshotTTL: 300 * time.Second,
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkMemoryKeyedEngine_Stats benchmarks reading snapshot statistics.
func BenchmarkMemoryKeyedEngine_Stats(b *testing.B) {
	engine, cleanup := setupMemoryKeyedEngineBench(b)
	defer cleanup()

	ctx := context.Background()
	channel := "bench_stats"

	// Prepopulate snapshot with 1000 entries
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key%d", i)
		data := []byte(fmt.Sprintf("data%d", i))
		_, _, err := engine.Publish(ctx, channel, key, data, KeyedPublishOptions{
			StreamSize: 10000,
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

// BenchmarkMemoryKeyedEngine_Remove benchmarks removing keys.
func BenchmarkMemoryKeyedEngine_Remove(b *testing.B) {
	engine, cleanup := setupMemoryKeyedEngineBench(b)
	defer cleanup()

	ctx := context.Background()
	channel := "bench_remove"

	// Prepopulate with keys
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("key%d", i)
		data := []byte(fmt.Sprintf("data%d", i))
		_, _, err := engine.Publish(ctx, channel, key, data, KeyedPublishOptions{
			StreamSize: 10000,
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
			_, err := engine.Remove(ctx, channel, key, KeyedRemoveOptions{
				Publish:    false,
				StreamSize: 10000,
				StreamTTL:  300 * time.Second,
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkMemoryKeyedEngine_IdempotentPublish benchmarks idempotent publishing.
func BenchmarkMemoryKeyedEngine_IdempotentPublish(b *testing.B) {
	engine, cleanup := setupMemoryKeyedEngineBench(b)
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
			_, _, err := engine.Publish(ctx, channel, "", data, KeyedPublishOptions{
				IdempotencyKey:      idempotencyKey,
				IdempotentResultTTL: 60 * time.Second,
				StreamSize:          10000,
				StreamTTL:           300 * time.Second,
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkMemoryKeyedEngine_VersionedPublish benchmarks version-based publishing.
func BenchmarkMemoryKeyedEngine_VersionedPublish(b *testing.B) {
	engine, cleanup := setupMemoryKeyedEngineBench(b)
	defer cleanup()

	ctx := context.Background()
	channel := "bench_versioned"

	b.ReportAllocs()
	b.ResetTimer()

	var counter int64
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := atomic.AddInt64(&counter, 1)
			key := fmt.Sprintf("key%d", i%100) // Reuse 100 keys
			data := []byte(fmt.Sprintf("data_%d", i))
			_, _, err := engine.Publish(ctx, channel, key, data, KeyedPublishOptions{
				Version:    uint64(i),
				StreamSize: 10000,
				StreamTTL:  300 * time.Second,
				KeyTTL:     300 * time.Second,
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkMemoryKeyedEngine_PublishWithDelta benchmarks publishing with delta compression.
func BenchmarkMemoryKeyedEngine_PublishWithDelta(b *testing.B) {
	engine, cleanup := setupMemoryKeyedEngineBench(b)
	defer cleanup()

	ctx := context.Background()
	channel := "bench_delta"

	// Prepopulate with some keys
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key%d", i)
		data := []byte(fmt.Sprintf("initial_data%d", i))
		_, _, err := engine.Publish(ctx, channel, key, data, KeyedPublishOptions{
			StreamSize: 10000,
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
			key := fmt.Sprintf("key%d", i%100) // Reuse 100 keys
			data := []byte(fmt.Sprintf("updated_data_%d", i))
			_, _, err := engine.Publish(ctx, channel, key, data, KeyedPublishOptions{
				UseDelta:   true,
				StreamSize: 10000,
				StreamTTL:  300 * time.Second,
				KeyTTL:     300 * time.Second,
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
