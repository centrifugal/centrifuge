package centrifuge

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

func setupMemoryMapBrokerBench(b *testing.B) (*MemoryMapBroker, func()) {
	b.Helper()
	node, _ := New(Config{
		Map: MapConfig{
			GetMapChannelOptions: func(channel string) MapChannelOptions {
				return MapChannelOptions{
					Mode:   MapModeRecoverable,
					KeyTTL: 60 * time.Second,
				}
			},
		},
	})
	broker, _ := NewMemoryMapBroker(node, MemoryMapBrokerConfig{})
	_ = broker.RegisterEventHandler(nil)
	return broker, func() {
		_ = node.Shutdown(context.Background())
	}
}

// BenchmarkMemoryMapBroker_PublishStreamOnly benchmarks publishing to stream without state.
func BenchmarkMemoryMapBroker_PublishStreamOnly(b *testing.B) {
	broker, cleanup := setupMemoryMapBrokerBench(b)
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
				Data: data,
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkMemoryMapBroker_PublishMapStateSimple benchmarks simple keyed state.
func BenchmarkMemoryMapBroker_PublishMapStateSimple(b *testing.B) {
	broker, cleanup := setupMemoryMapBrokerBench(b)
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
				Data: data,
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkMemoryMapBroker_PublishMapStateOrdered benchmarks ordered keyed state.
func BenchmarkMemoryMapBroker_PublishMapStateOrdered(b *testing.B) {
	node, _ := New(Config{})
	node.config.Map.GetMapChannelOptions = func(channel string) MapChannelOptions {
		return MapChannelOptions{
			Mode:       MapModeRecoverable,
			ordered:    true,
			StreamSize: 10000,
			StreamTTL:  300 * time.Second,
			KeyTTL:     300 * time.Second,
		}
	}
	broker, _ := NewMemoryMapBroker(node, MemoryMapBrokerConfig{})
	_ = broker.RegisterEventHandler(nil)
	b.Cleanup(func() {
		_ = node.Shutdown(context.Background())
	})

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
				Data:  data,
				score: i,
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkMemoryMapBroker_PublishCombined benchmarks publishing with stream + state.
func BenchmarkMemoryMapBroker_PublishCombined(b *testing.B) {
	broker, cleanup := setupMemoryMapBrokerBench(b)
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
				Data: data,
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkMemoryMapBroker_ReadStream benchmarks reading from stream.
func BenchmarkMemoryMapBroker_ReadStream(b *testing.B) {
	broker, cleanup := setupMemoryMapBrokerBench(b)
	defer cleanup()

	ctx := context.Background()
	channel := "bench_read_stream"

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

// BenchmarkMemoryMapBroker_ReadStateFull benchmarks reading full unordered state.
func BenchmarkMemoryMapBroker_ReadStateFull(b *testing.B) {
	broker, cleanup := setupMemoryMapBrokerBench(b)
	defer cleanup()

	ctx := context.Background()
	channel := "bench_read_state"

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

// BenchmarkMemoryMapBroker_ReadStatePaginated benchmarks paginated state reads.
func BenchmarkMemoryMapBroker_ReadStatePaginated(b *testing.B) {
	broker, cleanup := setupMemoryMapBrokerBench(b)
	defer cleanup()

	ctx := context.Background()
	channel := "bench_read_state_paginated"

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

// BenchmarkMemoryMapBroker_ReadStateOrdered benchmarks reading ordered state.
func BenchmarkMemoryMapBroker_ReadStateOrdered(b *testing.B) {
	node, _ := New(Config{})
	node.config.Map.GetMapChannelOptions = func(channel string) MapChannelOptions {
		return MapChannelOptions{
			Mode:       MapModeRecoverable,
			ordered:    true,
			StreamSize: 10000,
			StreamTTL:  300 * time.Second,
			KeyTTL:     300 * time.Second,
		}
	}
	broker, _ := NewMemoryMapBroker(node, MemoryMapBrokerConfig{})
	_ = broker.RegisterEventHandler(nil)
	b.Cleanup(func() {
		_ = node.Shutdown(context.Background())
	})

	ctx := context.Background()
	channel := "bench_read_state_ordered"

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

// BenchmarkMemoryMapBroker_Stats benchmarks reading state statistics.
func BenchmarkMemoryMapBroker_Stats(b *testing.B) {
	broker, cleanup := setupMemoryMapBrokerBench(b)
	defer cleanup()

	ctx := context.Background()
	channel := "bench_stats"

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
			_, err := broker.Stats(ctx, channel)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkMemoryMapBroker_Remove benchmarks removing keys.
func BenchmarkMemoryMapBroker_Remove(b *testing.B) {
	broker, cleanup := setupMemoryMapBrokerBench(b)
	defer cleanup()

	ctx := context.Background()
	channel := "bench_remove"

	// Prepopulate with keys
	for i := 0; i < 10000; i++ {
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

	var counter int64
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := atomic.AddInt64(&counter, 1)
			key := fmt.Sprintf("key%d", i)
			_, err := broker.Remove(ctx, channel, key, MapRemoveOptions{})
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkMemoryMapBroker_IdempotentPublish benchmarks idempotent publishing.
func BenchmarkMemoryMapBroker_IdempotentPublish(b *testing.B) {
	broker, cleanup := setupMemoryMapBrokerBench(b)
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
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkMemoryMapBroker_VersionedPublish benchmarks version-based publishing.
func BenchmarkMemoryMapBroker_VersionedPublish(b *testing.B) {
	broker, cleanup := setupMemoryMapBrokerBench(b)
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

// BenchmarkMemoryMapBroker_PublishWithDelta benchmarks publishing with delta compression.
func BenchmarkMemoryMapBroker_PublishWithDelta(b *testing.B) {
	broker, cleanup := setupMemoryMapBrokerBench(b)
	defer cleanup()

	ctx := context.Background()
	channel := "bench_delta"

	// Prepopulate with some keys
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key%d", i)
		data := []byte(fmt.Sprintf("initial_data%d", i))
		_, err := broker.Publish(ctx, channel, key, MapPublishOptions{
			Data: data,
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
			_, err := broker.Publish(ctx, channel, key, MapPublishOptions{
				Data:     data,
				UseDelta: true,
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkMemoryMapBroker_Cleanup benchmarks TTL-based key cleanup throughput.
// Throughput in keys/second = keys/op * 1e9 / ns_per_op.
func BenchmarkMemoryMapBroker_Cleanup(b *testing.B) {
	for _, ordered := range []bool{false, true} {
		orderLabel := "unordered"
		if ordered {
			orderLabel = "ordered"
		}
		for _, numKeys := range []int{100, 1000, 10000} {
			b.Run(fmt.Sprintf("%s/keys_%d", orderLabel, numKeys), func(b *testing.B) {
				node, _ := New(Config{
					Map: MapConfig{
						GetMapChannelOptions: func(channel string) MapChannelOptions {
							return MapChannelOptions{
								Mode:    MapModeRecoverable,
								KeyTTL:  time.Millisecond,
								ordered: ordered,
							}
						},
					},
				})
				broker, _ := NewMemoryMapBroker(node, MemoryMapBrokerConfig{})
				broker.mapHub.setEventHandler(&testBrokerEventHandler{})
				b.Cleanup(func() { _ = node.Shutdown(context.Background()) })

				ctx := context.Background()

				b.ReportAllocs()
				b.ReportMetric(float64(numKeys), "keys/op")
				for i := 0; i < b.N; i++ {
					b.StopTimer()
					ch := fmt.Sprintf("bench_cleanup_%d", i)
					for k := 0; k < numKeys; k++ {
						_, _ = broker.Publish(ctx, ch, fmt.Sprintf("key%d", k), MapPublishOptions{
							Data:  []byte("data"),
							score: int64(k),
						})
					}
					time.Sleep(2 * time.Millisecond)
					b.StartTimer()

					var check int64
					broker.mapHub.expireKeysIteration(&check)
				}
			})
		}
	}
}
