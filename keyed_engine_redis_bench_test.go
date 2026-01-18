//go:build integration

package centrifuge

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

func setupSnapshotEngineBench(b *testing.B) (*RedisKeyedEngine, func()) {
	b.Helper()
	node, _ := New(Config{})
	engine := newTestSnapshotRedisEngine(b, node)
	return engine, func() {
		_ = node.Shutdown(context.Background())
	}
}

// BenchmarkSnapshotEngine_PublishStreamOnly benchmarks publishing to stream without snapshots.
func BenchmarkSnapshotEngine_PublishStreamOnly(b *testing.B) {
	engine, cleanup := setupSnapshotEngineBench(b)
	defer cleanup()

	ctx := context.Background()
	channel := randomChannel("bench_stream")

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

// BenchmarkSnapshotEngine_PublishKeyedStateSimple benchmarks simple keyed state (HASH only).
func BenchmarkSnapshotEngine_PublishKeyedStateSimple(b *testing.B) {
	engine, cleanup := setupSnapshotEngineBench(b)
	defer cleanup()

	ctx := context.Background()
	channel := randomChannel("bench_keyed_simple")

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

// BenchmarkSnapshotEngine_PublishKeyedStateOrdered benchmarks ordered keyed state (HASH+ZSET).
func BenchmarkSnapshotEngine_PublishKeyedStateOrdered(b *testing.B) {
	engine, cleanup := setupSnapshotEngineBench(b)
	defer cleanup()

	ctx := context.Background()
	channel := randomChannel("bench_keyed_ordered")

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

// BenchmarkSnapshotEngine_AddMember benchmarks presence/membership operations.
func BenchmarkSnapshotEngine_AddMember(b *testing.B) {
	engine, cleanup := setupSnapshotEngineBench(b)
	defer cleanup()

	ctx := context.Background()
	channel := randomChannel("bench_presence")

	b.ReportAllocs()
	b.ResetTimer()

	var counter int64
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := atomic.AddInt64(&counter, 1)
			clientID := fmt.Sprintf("client%d", i)
			userID := fmt.Sprintf("user%d", i%100) // 100 different users

			err := engine.AddMember(ctx, channel, ClientInfo{
				ClientID: clientID,
				UserID:   userID,
			}, EnginePresenceOptions{
				Publish: false, // Don't publish for benchmark
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkSnapshotEngine_PublishCombined benchmarks publishing with stream + snapshot.
func BenchmarkSnapshotEngine_PublishCombined(b *testing.B) {
	engine, cleanup := setupSnapshotEngineBench(b)
	defer cleanup()

	ctx := context.Background()
	channel := randomChannel("bench_combined")

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

// BenchmarkSnapshotEngine_ReadStream benchmarks reading from stream.
func BenchmarkSnapshotEngine_ReadStream(b *testing.B) {
	engine, cleanup := setupSnapshotEngineBench(b)
	defer cleanup()

	ctx := context.Background()
	channel := randomChannel("bench_read_stream")

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
			_, _, err := engine.ReadStreamZero(ctx, channel, KeyedReadStreamOptions{
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

// BenchmarkSnapshotEngine_ReadSnapshotFull benchmarks reading full unordered snapshot.
func BenchmarkSnapshotEngine_ReadSnapshotFull(b *testing.B) {
	engine, cleanup := setupSnapshotEngineBench(b)
	defer cleanup()

	ctx := context.Background()
	channel := randomChannel("bench_read_snapshot")

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

// BenchmarkSnapshotEngine_ReadSnapshotPaginated benchmarks paginated snapshot reads.
func BenchmarkSnapshotEngine_ReadSnapshotPaginated(b *testing.B) {
	engine, cleanup := setupSnapshotEngineBench(b)
	defer cleanup()

	ctx := context.Background()
	channel := randomChannel("bench_read_snapshot_paginated")

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

// BenchmarkSnapshotEngine_ReadSnapshotOrdered benchmarks reading ordered snapshot.
func BenchmarkSnapshotEngine_ReadSnapshotOrdered(b *testing.B) {
	engine, cleanup := setupSnapshotEngineBench(b)
	defer cleanup()

	ctx := context.Background()
	channel := randomChannel("bench_read_snapshot_ordered")

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

// BenchmarkSnapshotEngine_Members benchmarks reading presence members.
func BenchmarkSnapshotEngine_Members(b *testing.B) {
	engine, cleanup := setupSnapshotEngineBench(b)
	defer cleanup()

	ctx := context.Background()
	channel := randomChannel("bench_members")

	// Prepopulate with 1000 members
	for i := 0; i < 1000; i++ {
		clientID := fmt.Sprintf("client%d", i)
		userID := fmt.Sprintf("user%d", i%100)
		err := engine.AddMember(ctx, channel, ClientInfo{
			ClientID: clientID,
			UserID:   userID,
		}, EnginePresenceOptions{
			Publish: false,
		})
		if err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := engine.Members(ctx, channel)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkSnapshotEngine_MemberStats benchmarks reading presence stats.
func BenchmarkSnapshotEngine_MemberStats(b *testing.B) {
	engine, cleanup := setupSnapshotEngineBench(b)
	defer cleanup()

	ctx := context.Background()
	channel := randomChannel("bench_member_stats")

	// Prepopulate with 1000 members (100 unique users)
	for i := 0; i < 1000; i++ {
		clientID := fmt.Sprintf("client%d", i)
		userID := fmt.Sprintf("user%d", i%100)
		err := engine.AddMember(ctx, channel, ClientInfo{
			ClientID: clientID,
			UserID:   userID,
		}, EnginePresenceOptions{
			Publish: false,
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

// BenchmarkSnapshotEngine_ReadPresenceSnapshot benchmarks reading presence snapshot with revisions.
func BenchmarkSnapshotEngine_ReadPresenceSnapshot(b *testing.B) {
	engine, cleanup := setupSnapshotEngineBench(b)
	defer cleanup()

	ctx := context.Background()
	channel := randomChannel("bench_presence_snapshot")

	// Prepopulate with 1000 members
	for i := 0; i < 1000; i++ {
		clientID := fmt.Sprintf("client%d", i)
		userID := fmt.Sprintf("user%d", i%100)
		err := engine.AddMember(ctx, channel, ClientInfo{
			ClientID: clientID,
			UserID:   userID,
		}, EnginePresenceOptions{
			Publish: false,
		})
		if err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _, err := engine.ReadPresenceSnapshot(ctx, channel, KeyedReadSnapshotOptions{
				Limit:       0, // Read all
				SnapshotTTL: 300 * time.Second,
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkSnapshotEngine_IdempotentPublish benchmarks idempotent publishing.
func BenchmarkSnapshotEngine_IdempotentPublish(b *testing.B) {
	engine, cleanup := setupSnapshotEngineBench(b)
	defer cleanup()

	ctx := context.Background()
	channel := randomChannel("bench_idempotent")

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

// BenchmarkSnapshotEngine_VersionedPublish benchmarks version-based publishing.
func BenchmarkSnapshotEngine_VersionedPublish(b *testing.B) {
	engine, cleanup := setupSnapshotEngineBench(b)
	defer cleanup()

	ctx := context.Background()
	channel := randomChannel("bench_versioned")

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
