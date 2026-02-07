package centrifuge

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

func setupMemoryMapEngineBench(b *testing.B) (*MemoryMapEngine, func()) {
	b.Helper()
	node, _ := New(Config{})
	engine, _ := NewMemoryMapEngine(node, MemoryMapEngineConfig{})
	_ = engine.RegisterBrokerEventHandler(nil)
	return engine, func() {
		_ = node.Shutdown(context.Background())
	}
}

// BenchmarkMemoryMapEngine_PublishStreamOnly benchmarks publishing to stream without snapshots.
func BenchmarkMemoryMapEngine_PublishStreamOnly(b *testing.B) {
	engine, cleanup := setupMemoryMapEngineBench(b)
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
			Data: data,
				StreamSize: 10000,
				StreamTTL:  300 * time.Second,
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkMemoryMapEngine_PublishMapStateSimple benchmarks simple keyed state.
func BenchmarkMemoryMapEngine_PublishMapStateSimple(b *testing.B) {
	engine, cleanup := setupMemoryMapEngineBench(b)
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
			Data: data,
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

// BenchmarkMemoryMapEngine_PublishMapStateOrdered benchmarks ordered keyed state.
func BenchmarkMemoryMapEngine_PublishMapStateOrdered(b *testing.B) {
	engine, cleanup := setupMemoryMapEngineBench(b)
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
			Data: data,
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

// BenchmarkMemoryMapEngine_PublishCombined benchmarks publishing with stream + snapshot.
func BenchmarkMemoryMapEngine_PublishCombined(b *testing.B) {
	engine, cleanup := setupMemoryMapEngineBench(b)
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
			Data: data,
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

// BenchmarkMemoryMapEngine_ReadStream benchmarks reading from stream.
func BenchmarkMemoryMapEngine_ReadStream(b *testing.B) {
	engine, cleanup := setupMemoryMapEngineBench(b)
	defer cleanup()

	ctx := context.Background()
	channel := "bench_read_stream"

	// Prepopulate stream with 1000 messages
	var sp StreamPosition
	for i := 0; i < 1000; i++ {
		data := []byte(fmt.Sprintf("message_%d", i))
		res, err := engine.Publish(ctx, channel, "", MapPublishOptions{
			Data: data,
			StreamSize: 10000,
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

// BenchmarkMemoryMapEngine_ReadStateFull benchmarks reading full unordered snapshot.
func BenchmarkMemoryMapEngine_ReadStateFull(b *testing.B) {
	engine, cleanup := setupMemoryMapEngineBench(b)
	defer cleanup()

	ctx := context.Background()
	channel := "bench_read_snapshot"

	// Prepopulate snapshot with 1000 entries
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key%d", i)
		data := []byte(fmt.Sprintf("data%d", i))
		_, err := engine.Publish(ctx, channel, key, MapPublishOptions{
			Data: data,
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
			_, _, _, err := engine.ReadState(ctx, channel, MapReadStateOptions{
				Limit:       0, // Read all
				StateTTL: 300 * time.Second,
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkMemoryMapEngine_ReadStatePaginated benchmarks paginated snapshot reads.
func BenchmarkMemoryMapEngine_ReadStatePaginated(b *testing.B) {
	engine, cleanup := setupMemoryMapEngineBench(b)
	defer cleanup()

	ctx := context.Background()
	channel := "bench_read_snapshot_paginated"

	// Prepopulate snapshot with 1000 entries
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key%d", i)
		data := []byte(fmt.Sprintf("data%d", i))
		_, err := engine.Publish(ctx, channel, key, MapPublishOptions{
			Data: data,
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

// BenchmarkMemoryMapEngine_ReadStateOrdered benchmarks reading ordered snapshot.
func BenchmarkMemoryMapEngine_ReadStateOrdered(b *testing.B) {
	engine, cleanup := setupMemoryMapEngineBench(b)
	defer cleanup()

	ctx := context.Background()
	channel := "bench_read_snapshot_ordered"

	// Prepopulate ordered snapshot with 1000 entries
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key%d", i)
		data := []byte(fmt.Sprintf("data%d", i))
		_, err := engine.Publish(ctx, channel, key, MapPublishOptions{
			Data: data,
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

// BenchmarkMemoryMapEngine_Stats benchmarks reading snapshot statistics.
func BenchmarkMemoryMapEngine_Stats(b *testing.B) {
	engine, cleanup := setupMemoryMapEngineBench(b)
	defer cleanup()

	ctx := context.Background()
	channel := "bench_stats"

	// Prepopulate snapshot with 1000 entries
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key%d", i)
		data := []byte(fmt.Sprintf("data%d", i))
		_, err := engine.Publish(ctx, channel, key, MapPublishOptions{
			Data: data,
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

// BenchmarkMemoryMapEngine_Remove benchmarks removing keys.
func BenchmarkMemoryMapEngine_Remove(b *testing.B) {
	engine, cleanup := setupMemoryMapEngineBench(b)
	defer cleanup()

	ctx := context.Background()
	channel := "bench_remove"

	// Prepopulate with keys
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("key%d", i)
		data := []byte(fmt.Sprintf("data%d", i))
		_, err := engine.Publish(ctx, channel, key, MapPublishOptions{
			Data: data,
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
			_, err := engine.Remove(ctx, channel, key, MapRemoveOptions{
				StreamSize: 10000,
				StreamTTL:  300 * time.Second,
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkMemoryMapEngine_IdempotentPublish benchmarks idempotent publishing.
func BenchmarkMemoryMapEngine_IdempotentPublish(b *testing.B) {
	engine, cleanup := setupMemoryMapEngineBench(b)
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
			Data: data,
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

// BenchmarkMemoryMapEngine_VersionedPublish benchmarks version-based publishing.
func BenchmarkMemoryMapEngine_VersionedPublish(b *testing.B) {
	engine, cleanup := setupMemoryMapEngineBench(b)
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
			_, err := engine.Publish(ctx, channel, key, MapPublishOptions{
			Data: data,
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

// BenchmarkMemoryMapEngine_PublishWithDelta benchmarks publishing with delta compression.
func BenchmarkMemoryMapEngine_PublishWithDelta(b *testing.B) {
	engine, cleanup := setupMemoryMapEngineBench(b)
	defer cleanup()

	ctx := context.Background()
	channel := "bench_delta"

	// Prepopulate with some keys
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key%d", i)
		data := []byte(fmt.Sprintf("initial_data%d", i))
		_, err := engine.Publish(ctx, channel, key, MapPublishOptions{
			Data: data,
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
			_, err := engine.Publish(ctx, channel, key, MapPublishOptions{
			Data: data,
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
