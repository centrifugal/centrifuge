//go:build integration

package centrifuge

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

func setupMapBrokerBench(b *testing.B) (*RedisMapBroker, func()) {
	b.Helper()
	node, _ := New(Config{})
	broker := newTestRedisMapBroker(b, node)
	return broker, func() {
		_ = node.Shutdown(context.Background())
	}
}

// BenchmarkRedisMapBroker_PublishStreamOnly benchmarks publishing to stream without state.
func BenchmarkRedisMapBroker_PublishStreamOnly(b *testing.B) {
	broker, cleanup := setupMapBrokerBench(b)
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
			_, err := broker.Publish(ctx, channel, "", MapPublishOptions{
				Data: data,
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkRedisMapBroker_PublishMapStateSimple benchmarks simple keyed state (HASH only).
func BenchmarkRedisMapBroker_PublishMapStateSimple(b *testing.B) {
	broker, cleanup := setupMapBrokerBench(b)
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
			_, err := broker.Publish(ctx, channel, key, MapPublishOptions{
				Data: data,
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkRedisMapBroker_PublishMapStateOrdered benchmarks ordered keyed state (HASH+ZSET).
func BenchmarkRedisMapBroker_PublishMapStateOrdered(b *testing.B) {
	b.Helper()
	node, _ := New(Config{
		Map: MapConfig{
			GetMapChannelOptions: func(channel string) MapChannelOptions {
				return MapChannelOptions{
					Mode:       MapModeRecoverable,
					ordered:    true,
					StreamSize: 10000,
					StreamTTL:  300 * time.Second,
					MetaTTL:    time.Hour,
					KeyTTL:     300 * time.Second,
				}
			},
		},
	})
	broker := newTestRedisMapBroker(b, node)
	defer func() { _ = node.Shutdown(context.Background()) }()

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
			_, err := broker.Publish(ctx, channel, key, MapPublishOptions{
				Data:  data,
				score: i,
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkRedisMapBroker_PublishCombined benchmarks publishing with stream + state.
func BenchmarkRedisMapBroker_PublishCombined(b *testing.B) {
	broker, cleanup := setupMapBrokerBench(b)
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
			_, err := broker.Publish(ctx, channel, key, MapPublishOptions{
				Data: data,
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkRedisMapBroker_ReadStream benchmarks reading from stream.
func BenchmarkRedisMapBroker_ReadStream(b *testing.B) {
	broker, cleanup := setupMapBrokerBench(b)
	defer cleanup()

	ctx := context.Background()
	channel := randomChannel("bench_read_stream")

	// Prepopulate stream with 1000 messages
	var sp StreamPosition
	for i := 0; i < 1000; i++ {
		data := []byte(fmt.Sprintf("message_%d", i))
		res, err := broker.Publish(ctx, channel, "", MapPublishOptions{
			Data: data,
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
			_, err := broker.ReadStream(ctx, channel, MapReadStreamOptions{
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

// BenchmarkRedisMapBroker_ReadStateFull benchmarks reading full unordered state.
func BenchmarkRedisMapBroker_ReadStateFull(b *testing.B) {
	broker, cleanup := setupMapBrokerBench(b)
	defer cleanup()

	ctx := context.Background()
	channel := randomChannel("bench_read_state")

	// Prepopulate state with 1000 entries
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key%d", i)
		data := []byte(fmt.Sprintf("data%d", i))
		_, err := broker.Publish(ctx, channel, key, MapPublishOptions{
			Data: data,
		})
		if err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := broker.ReadState(ctx, channel, MapReadStateOptions{
				Limit: -1, // Read all
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkRedisMapBroker_ReadStatePaginated benchmarks paginated state reads.
func BenchmarkRedisMapBroker_ReadStatePaginated(b *testing.B) {
	broker, cleanup := setupMapBrokerBench(b)
	defer cleanup()

	ctx := context.Background()
	channel := randomChannel("bench_read_state_paginated")

	// Prepopulate state with 1000 entries
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key%d", i)
		data := []byte(fmt.Sprintf("data%d", i))
		_, err := broker.Publish(ctx, channel, key, MapPublishOptions{
			Data: data,
		})
		if err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := broker.ReadState(ctx, channel, MapReadStateOptions{
				Limit: 100, // Read 100 at a time
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkRedisMapBroker_ReadStateOrdered benchmarks reading ordered state.
func BenchmarkRedisMapBroker_ReadStateOrdered(b *testing.B) {
	b.Helper()
	node, _ := New(Config{
		Map: MapConfig{
			GetMapChannelOptions: func(channel string) MapChannelOptions {
				return MapChannelOptions{
					Mode:       MapModeRecoverable,
					ordered:    true,
					StreamSize: 10000,
					StreamTTL:  300 * time.Second,
					MetaTTL:    time.Hour,
					KeyTTL:     300 * time.Second,
				}
			},
		},
	})
	broker := newTestRedisMapBroker(b, node)
	defer func() { _ = node.Shutdown(context.Background()) }()

	ctx := context.Background()
	channel := randomChannel("bench_read_state_ordered")

	// Prepopulate ordered state with 1000 entries
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key%d", i)
		data := []byte(fmt.Sprintf("data%d", i))
		_, err := broker.Publish(ctx, channel, key, MapPublishOptions{
			Data:  data,
			score: int64(i),
		})
		if err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := broker.ReadState(ctx, channel, MapReadStateOptions{
				Limit: 100,
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkRedisMapBroker_IdempotentPublish benchmarks idempotent publishing.
func BenchmarkRedisMapBroker_IdempotentPublish(b *testing.B) {
	broker, cleanup := setupMapBrokerBench(b)
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
			_, err := broker.Publish(ctx, channel, "", MapPublishOptions{
				Data:                data,
				IdempotencyKey:      idempotencyKey,
				IdempotentResultTTL: 60 * time.Second,
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkRedisMapBroker_VersionedPublish benchmarks version-based publishing.
func BenchmarkRedisMapBroker_VersionedPublish(b *testing.B) {
	broker, cleanup := setupMapBrokerBench(b)
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
			_, err := broker.Publish(ctx, channel, key, MapPublishOptions{
				Data:    data,
				Version: uint64(i),
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkRedisMapBroker_Cleanup benchmarks TTL-based key cleanup throughput.
// Measures how fast the cleanup Lua script processes expired keys.
// Throughput in keys/second = keys/op * 1e9 / ns_per_op.
func BenchmarkRedisMapBroker_Cleanup(b *testing.B) {
	for _, ordered := range []bool{false, true} {
		orderLabel := "unordered"
		if ordered {
			orderLabel = "ordered"
		}
		for _, numKeys := range []int{100, 1000} {
			b.Run(fmt.Sprintf("%s/keys_%d", orderLabel, numKeys), func(b *testing.B) {
				node, _ := New(Config{
					Map: MapConfig{
						GetMapChannelOptions: func(channel string) MapChannelOptions {
							return MapChannelOptions{
								Mode:       MapModeRecoverable,
								KeyTTL:     30 * time.Second,
								StreamSize: numKeys * 3,
								ordered:    ordered,
							}
						},
					},
				})
				broker := newTestRedisMapBroker(b, node)

				ctx := context.Background()
				futureNow := time.Now().UnixMilli() + 31_000

				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					b.StopTimer()
					ch := fmt.Sprintf("bench_cleanup_%s_%d_%d", orderLabel, numKeys, i)
					for k := 0; k < numKeys; k++ {
						_, err := broker.Publish(ctx, ch, fmt.Sprintf("key%d", k), MapPublishOptions{
							Data:  []byte("data"),
							score: int64(k),
						})
						if err != nil {
							b.Fatal(err)
						}
					}
					shardWrapper := broker.shards[0]
					cleanupKey := broker.cleanupRegistrationKeyForChannel(shardWrapper.shard, ch)
					b.StartTimer()

					_ = broker.cleanupChannel(ctx, shardWrapper.shard, ch, cleanupKey, futureNow)
				}
				b.ReportMetric(float64(numKeys), "keys/op")
			})
		}
	}
}
