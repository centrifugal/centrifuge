//go:build integration
// +build integration

package centrifuge

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

func setupPostgresKeyedEngineBench(b *testing.B) (*PostgresKeyedEngine, func()) {
	b.Helper()

	connString := getPostgresConnString(b)

	node, _ := New(Config{})
	engine, err := NewPostgresKeyedEngine(node, PostgresKeyedEngineConfig{
		ConnString:   connString,
		PollInterval: 100 * time.Millisecond,
	})
	if err != nil {
		b.Fatal(err)
	}
	_ = engine.RegisterBrokerEventHandler(nil)

	// Clean up tables
	ctx := context.Background()
	_, _ = engine.pool.Exec(ctx, "DELETE FROM cf_keyed_stream WHERE channel LIKE 'bench_%'")
	_, _ = engine.pool.Exec(ctx, "DELETE FROM cf_keyed_snapshot WHERE channel LIKE 'bench_%'")
	_, _ = engine.pool.Exec(ctx, "DELETE FROM cf_keyed_stream_meta WHERE channel LIKE 'bench_%'")
	_, _ = engine.pool.Exec(ctx, "DELETE FROM cf_keyed_idempotency WHERE channel LIKE 'bench_%'")

	return engine, func() {
		_ = engine.Close(context.Background())
		_ = node.Shutdown(context.Background())
	}
}

// BenchmarkPostgresKeyedEngine_PublishStreamOnly benchmarks publishing to stream.
func BenchmarkPostgresKeyedEngine_PublishStreamOnly(b *testing.B) {
	engine, cleanup := setupPostgresKeyedEngineBench(b)
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
			_, err := engine.Publish(ctx, channel, "", KeyedPublishOptions{
				Data: data,
				//StreamSize: 10000, // This is rather expensive, rely on TTL fits PG better.
				StreamTTL: 300 * time.Second,
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkPostgresKeyedEngine_PublishKeyedStateSimple benchmarks simple keyed state.
func BenchmarkPostgresKeyedEngine_PublishKeyedStateSimple(b *testing.B) {
	engine, cleanup := setupPostgresKeyedEngineBench(b)
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
			_, err := engine.Publish(ctx, channel, key, KeyedPublishOptions{
				Data: data,
				//StreamSize: 10000, // This is rather expensive, rely on TTL fits PG better.
				StreamTTL: 300 * time.Second,
				KeyTTL:    300 * time.Second,
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkPostgresKeyedEngine_PublishKeyedStateOrdered benchmarks ordered keyed state.
func BenchmarkPostgresKeyedEngine_PublishKeyedStateOrdered(b *testing.B) {
	engine, cleanup := setupPostgresKeyedEngineBench(b)
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
			_, err := engine.Publish(ctx, channel, key, KeyedPublishOptions{
				Data:    data,
				Ordered: true,
				Score:   i,
				//StreamSize: 10000, // This is rather expensive, rely on TTL fits PG better.
				StreamTTL: 300 * time.Second,
				KeyTTL:    300 * time.Second,
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkPostgresKeyedEngine_PublishCombined benchmarks publishing with stream + snapshot.
func BenchmarkPostgresKeyedEngine_PublishCombined(b *testing.B) {
	engine, cleanup := setupPostgresKeyedEngineBench(b)
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
			_, err := engine.Publish(ctx, channel, key, KeyedPublishOptions{
				Data: data,
				//StreamSize: 10000, // This is rather expensive, rely on TTL fits PG better.
				StreamTTL: 300 * time.Second,
				KeyTTL:    300 * time.Second,
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkPostgresKeyedEngine_ReadStream benchmarks reading from stream.
func BenchmarkPostgresKeyedEngine_ReadStream(b *testing.B) {
	engine, cleanup := setupPostgresKeyedEngineBench(b)
	defer cleanup()

	ctx := context.Background()
	channel := "bench_read_stream"

	// Prepopulate stream with 1000 messages
	var sp StreamPosition
	for i := 0; i < 1000; i++ {
		data := []byte(fmt.Sprintf("message_%d", i))
		res, err := engine.Publish(ctx, channel, "", KeyedPublishOptions{
			Data: data,
			//StreamSize: 10000, // This is rather expensive, rely on TTL fits PG better.
			StreamTTL: 300 * time.Second,
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
			_, _, err := engine.ReadStream(ctx, channel, KeyedReadStreamOptions{
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

// BenchmarkPostgresKeyedEngine_ReadSnapshotFull benchmarks reading full unordered snapshot.
func BenchmarkPostgresKeyedEngine_ReadSnapshotFull(b *testing.B) {
	engine, cleanup := setupPostgresKeyedEngineBench(b)
	defer cleanup()

	ctx := context.Background()
	channel := "bench_read_snapshot"

	// Prepopulate snapshot with 1000 entries
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key%d", i)
		data := []byte(fmt.Sprintf("data%d", i))
		_, err := engine.Publish(ctx, channel, key, KeyedPublishOptions{
			Data: data,
			//StreamSize: 10000, // This is rather expensive, rely on TTL fits PG better.
			StreamTTL: 300 * time.Second,
			KeyTTL:    300 * time.Second,
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
				Limit:       0, // Use default of 100
				SnapshotTTL: 300 * time.Second,
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkPostgresKeyedEngine_ReadSnapshotPaginated benchmarks paginated snapshot reads.
func BenchmarkPostgresKeyedEngine_ReadSnapshotPaginated(b *testing.B) {
	engine, cleanup := setupPostgresKeyedEngineBench(b)
	defer cleanup()

	ctx := context.Background()
	channel := "bench_read_snapshot_paginated"

	// Prepopulate snapshot with 1000 entries
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key%d", i)
		data := []byte(fmt.Sprintf("data%d", i))
		_, err := engine.Publish(ctx, channel, key, KeyedPublishOptions{
			Data: data,
			//StreamSize: 10000, // This is rather expensive, rely on TTL fits PG better.
			StreamTTL: 300 * time.Second,
			KeyTTL:    300 * time.Second,
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

// BenchmarkPostgresKeyedEngine_ReadSnapshotOrdered benchmarks reading ordered snapshot.
func BenchmarkPostgresKeyedEngine_ReadSnapshotOrdered(b *testing.B) {
	engine, cleanup := setupPostgresKeyedEngineBench(b)
	defer cleanup()

	ctx := context.Background()
	channel := "bench_read_snapshot_ordered"

	// Prepopulate ordered snapshot with 1000 entries
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key%d", i)
		data := []byte(fmt.Sprintf("data%d", i))
		_, err := engine.Publish(ctx, channel, key, KeyedPublishOptions{
			Data:    data,
			Ordered: true,
			Score:   int64(i),
			//StreamSize: 10000, // This is rather expensive, rely on TTL fits PG better.
			StreamTTL: 300 * time.Second,
			KeyTTL:    300 * time.Second,
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

// BenchmarkPostgresKeyedEngine_Stats benchmarks reading snapshot statistics.
func BenchmarkPostgresKeyedEngine_Stats(b *testing.B) {
	engine, cleanup := setupPostgresKeyedEngineBench(b)
	defer cleanup()

	ctx := context.Background()
	channel := "bench_stats"

	// Prepopulate snapshot with 1000 entries
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key%d", i)
		data := []byte(fmt.Sprintf("data%d", i))
		_, err := engine.Publish(ctx, channel, key, KeyedPublishOptions{
			Data: data,
			//StreamSize: 10000, // This is rather expensive, rely on TTL fits PG better.
			StreamTTL: 300 * time.Second,
			KeyTTL:    300 * time.Second,
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

// BenchmarkPostgresKeyedEngine_Unpublish benchmarks removing keys.
func BenchmarkPostgresKeyedEngine_Unpublish(b *testing.B) {
	engine, cleanup := setupPostgresKeyedEngineBench(b)
	defer cleanup()

	ctx := context.Background()
	channel := "bench_remove"

	// Prepopulate with keys
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("key%d", i)
		data := []byte(fmt.Sprintf("data%d", i))
		_, err := engine.Publish(ctx, channel, key, KeyedPublishOptions{
			Data: data,
			//StreamSize: 10000, // This is rather expensive, rely on TTL fits PG better.
			StreamTTL: 300 * time.Second,
			KeyTTL:    300 * time.Second,
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
			_, err := engine.Unpublish(ctx, channel, key, KeyedUnpublishOptions{
				//StreamSize: 10000, // This is rather expensive, rely on TTL fits PG better.
				StreamTTL: 300 * time.Second,
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkPostgresKeyedEngine_IdempotentPublish benchmarks idempotent publishing.
func BenchmarkPostgresKeyedEngine_IdempotentPublish(b *testing.B) {
	engine, cleanup := setupPostgresKeyedEngineBench(b)
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
			_, err := engine.Publish(ctx, channel, "", KeyedPublishOptions{
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

// BenchmarkPostgresKeyedEngine_CAS benchmarks CAS (Compare-And-Swap) operations.
func BenchmarkPostgresKeyedEngine_CAS(b *testing.B) {
	engine, cleanup := setupPostgresKeyedEngineBench(b)
	defer cleanup()

	ctx := context.Background()
	channel := "bench_cas"

	// Create initial key
	res, err := engine.Publish(ctx, channel, "shared_counter", KeyedPublishOptions{
		Data: []byte("0"),
		//StreamSize: 10000, // This is rather expensive, rely on TTL fits PG better.
		StreamTTL: 300 * time.Second,
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
			entries, pos, _, err := engine.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{
				Key: "shared_counter",
			})
			if err != nil || len(entries) == 0 {
				continue
			}

			// Attempt CAS
			expectedPos := StreamPosition{Offset: entries[0].Offset, Epoch: pos.Epoch}
			_, _ = engine.Publish(ctx, channel, "shared_counter", KeyedPublishOptions{
				Data:             []byte("updated"),
				ExpectedPosition: &expectedPos,
				//StreamSize:       10000, // This is rather expensive, rely on TTL fits PG better.
				StreamTTL: 300 * time.Second,
			})
		}
	})
	_ = lastPos
}
