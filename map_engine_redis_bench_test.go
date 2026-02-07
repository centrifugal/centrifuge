//go:build integration

package centrifuge

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

func setupSnapshotEngineBench(b *testing.B) (*RedisMapEngine, func()) {
	b.Helper()
	node, _ := New(Config{})
	engine := newTestSnapshotRedisEngine(b, node)
	return engine, func() {
		_ = node.Shutdown(context.Background())
	}
}

// BenchmarkRedisMapEngine_PublishStreamOnly benchmarks publishing to stream without snapshots.
func BenchmarkRedisMapEngine_PublishStreamOnly(b *testing.B) {
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

// BenchmarkRedisMapEngine_PublishMapStateSimple benchmarks simple keyed state (HASH only).
func BenchmarkRedisMapEngine_PublishMapStateSimple(b *testing.B) {
	engine, cleanup := setupSnapshotEngineBench(b)
	defer cleanup()

	ctx := context.Background()
	channel := randomChannel("bench_map_simple")

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

// BenchmarkRedisMapEngine_PublishMapStateOrdered benchmarks ordered keyed state (HASH+ZSET).
func BenchmarkRedisMapEngine_PublishMapStateOrdered(b *testing.B) {
	engine, cleanup := setupSnapshotEngineBench(b)
	defer cleanup()

	ctx := context.Background()
	channel := randomChannel("bench_map_ordered")

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

//// BenchmarkRedisMapEngine_AddMember benchmarks presence/membership operations.
//func BenchmarkRedisMapEngine_AddMember(b *testing.B) {
//	engine, cleanup := setupSnapshotEngineBench(b)
//	defer cleanup()
//
//	ctx := context.Background()
//	channel := randomChannel("bench_presence")
//
//	b.ReportAllocs()
//	b.ResetTimer()
//
//	var counter int64
//	b.RunParallel(func(pb *testing.PB) {
//		for pb.Next() {
//			i := atomic.AddInt64(&counter, 1)
//			clientID := fmt.Sprintf("client%d", i)
//			userID := fmt.Sprintf("user%d", i%100) // 100 different users
//
//			err := engine.AddMember(ctx, channel, ClientInfo{
//				ClientID: clientID,
//				UserID:   userID,
//			}, EnginePresenceOptions{
//				Publish: false, // Don't publish for benchmark
//			})
//			if err != nil {
//				b.Fatal(err)
//			}
//		}
//	})
//}

// BenchmarkRedisMapEngine_PublishCombined benchmarks publishing with stream + snapshot.
func BenchmarkRedisMapEngine_PublishCombined(b *testing.B) {
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

// BenchmarkRedisMapEngine_ReadStream benchmarks reading from stream.
func BenchmarkRedisMapEngine_ReadStream(b *testing.B) {
	engine, cleanup := setupSnapshotEngineBench(b)
	defer cleanup()

	ctx := context.Background()
	channel := randomChannel("bench_read_stream")

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
			_, _, err := engine.ReadStreamZero(ctx, channel, MapReadStreamOptions{
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

// BenchmarkRedisMapEngine_ReadStateFull benchmarks reading full unordered snapshot.
func BenchmarkRedisMapEngine_ReadStateFull(b *testing.B) {
	engine, cleanup := setupSnapshotEngineBench(b)
	defer cleanup()

	ctx := context.Background()
	channel := randomChannel("bench_read_snapshot")

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

// BenchmarkRedisMapEngine_ReadStatePaginated benchmarks paginated snapshot reads.
func BenchmarkRedisMapEngine_ReadStatePaginated(b *testing.B) {
	engine, cleanup := setupSnapshotEngineBench(b)
	defer cleanup()

	ctx := context.Background()
	channel := randomChannel("bench_read_snapshot_paginated")

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

// BenchmarkRedisMapEngine_ReadStateOrdered benchmarks reading ordered snapshot.
func BenchmarkRedisMapEngine_ReadStateOrdered(b *testing.B) {
	engine, cleanup := setupSnapshotEngineBench(b)
	defer cleanup()

	ctx := context.Background()
	channel := randomChannel("bench_read_snapshot_ordered")

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

//
//// BenchmarkRedisMapEngine_Members benchmarks reading presence members.
//func BenchmarkRedisMapEngine_Members(b *testing.B) {
//	engine, cleanup := setupSnapshotEngineBench(b)
//	defer cleanup()
//
//	ctx := context.Background()
//	channel := randomChannel("bench_members")
//
//	// Prepopulate with 1000 members
//	for i := 0; i < 1000; i++ {
//		clientID := fmt.Sprintf("client%d", i)
//		userID := fmt.Sprintf("user%d", i%100)
//		err := engine.AddMember(ctx, channel, ClientInfo{
//			ClientID: clientID,
//			UserID:   userID,
//		}, EnginePresenceOptions{
//			Publish: false,
//		})
//		if err != nil {
//			b.Fatal(err)
//		}
//	}
//
//	b.ReportAllocs()
//	b.ResetTimer()
//
//	b.RunParallel(func(pb *testing.PB) {
//		for pb.Next() {
//			_, err := engine.Members(ctx, channel)
//			if err != nil {
//				b.Fatal(err)
//			}
//		}
//	})
//}

//// BenchmarkRedisMapEngine_MemberStats benchmarks reading presence stats.
//func BenchmarkRedisMapEngine_MemberStats(b *testing.B) {
//	engine, cleanup := setupSnapshotEngineBench(b)
//	defer cleanup()
//
//	ctx := context.Background()
//	channel := randomChannel("bench_member_stats")
//
//	// Prepopulate with 1000 members (100 unique users)
//	for i := 0; i < 1000; i++ {
//		clientID := fmt.Sprintf("client%d", i)
//		userID := fmt.Sprintf("user%d", i%100)
//		err := engine.AddMember(ctx, channel, ClientInfo{
//			ClientID: clientID,
//			UserID:   userID,
//		}, EnginePresenceOptions{
//			Publish: false,
//		})
//		if err != nil {
//			b.Fatal(err)
//		}
//	}
//
//	b.ReportAllocs()
//	b.ResetTimer()
//
//	b.RunParallel(func(pb *testing.PB) {
//		for pb.Next() {
//			_, err := engine.Stats(ctx, channel)
//			if err != nil {
//				b.Fatal(err)
//			}
//		}
//	})
//}

//// BenchmarkRedisMapEngine_ReadPresenceSnapshot benchmarks reading presence snapshot with revisions.
//func BenchmarkRedisMapEngine_ReadPresenceSnapshot(b *testing.B) {
//	engine, cleanup := setupSnapshotEngineBench(b)
//	defer cleanup()
//
//	ctx := context.Background()
//	channel := randomChannel("bench_presence_snapshot")
//
//	// Prepopulate with 1000 members
//	for i := 0; i < 1000; i++ {
//		clientID := fmt.Sprintf("client%d", i)
//		userID := fmt.Sprintf("user%d", i%100)
//		err := engine.AddMember(ctx, channel, ClientInfo{
//			ClientID: clientID,
//			UserID:   userID,
//		}, EnginePresenceOptions{
//			Publish: false,
//		})
//		if err != nil {
//			b.Fatal(err)
//		}
//	}
//
//	b.ReportAllocs()
//	b.ResetTimer()
//
//	b.RunParallel(func(pb *testing.PB) {
//		for pb.Next() {
//			_, _, err := engine.ReadPresenceSnapshot(ctx, channel, MapReadStateOptions{
//				Limit:       0, // Read all
//				StateTTL: 300 * time.Second,
//			})
//			if err != nil {
//				b.Fatal(err)
//			}
//		}
//	})
//}

// BenchmarkRedisMapEngine_IdempotentPublish benchmarks idempotent publishing.
func BenchmarkRedisMapEngine_IdempotentPublish(b *testing.B) {
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

// BenchmarkRedisMapEngine_VersionedPublish benchmarks version-based publishing.
func BenchmarkRedisMapEngine_VersionedPublish(b *testing.B) {
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
