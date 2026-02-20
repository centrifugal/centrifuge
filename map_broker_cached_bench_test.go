package centrifuge

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// BenchmarkCachedMapBroker_ReadState_Cached benchmarks reading from warm cache.
func BenchmarkCachedMapBroker_ReadState_Cached(b *testing.B) {
	node, _ := New(Config{
		GetMapChannelOptions: func(channel string) MapChannelOptions {
			return MapChannelOptions{
				SyncMode:      MapSyncConverging,
				RetentionMode: MapRetentionExpiring,
				KeyTTL:        60 * time.Second,
			}
		},
	})
	backend, err := NewMemoryMapBroker(node, MemoryMapBrokerConfig{})
	require.NoError(b, err)
	_ = backend.RegisterEventHandler(nil)

	cached, err := NewCachedMapBroker(node, backend, CachedMapBrokerConfig{
		Cache: MapCacheConfig{
			MaxChannels: 10000,
			StreamSize:  10000,
		},
		SyncInterval: time.Hour, // Disable sync for benchmark
	})
	require.NoError(b, err)
	defer func() { _ = cached.Close(context.Background()) }()

	ctx := context.Background()
	channel := "bench_cached_read"

	// Prepopulate with 1000 entries
	for i := 0; i < 1000; i++ {
		_, err := cached.Publish(ctx, channel, fmt.Sprintf("key%d", i), MapPublishOptions{
			Data: []byte("benchmark_data_payload_for_testing"),
		})
		require.NoError(b, err)
	}

	// Ensure loaded
	_, err = cached.ReadState(ctx, channel, MapReadStateOptions{Limit: -1})
	require.NoError(b, err)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _ = cached.ReadState(ctx, channel, MapReadStateOptions{
			Limit: -1, // Get all
		})
	}
}

// BenchmarkCachedMapBroker_ReadState_ColdLoad benchmarks first read (load from backend).
func BenchmarkCachedMapBroker_ReadState_ColdLoad(b *testing.B) {
	node, _ := New(Config{
		GetMapChannelOptions: func(channel string) MapChannelOptions {
			return MapChannelOptions{
				SyncMode:      MapSyncConverging,
				RetentionMode: MapRetentionExpiring,
				KeyTTL:        60 * time.Second,
			}
		},
	})
	backend, err := NewMemoryMapBroker(node, MemoryMapBrokerConfig{})
	require.NoError(b, err)
	_ = backend.RegisterEventHandler(nil)

	ctx := context.Background()

	// Prepopulate backend
	for i := 0; i < 1000; i++ {
		_, err := backend.Publish(ctx, "bench_cold_load", fmt.Sprintf("key%d", i), MapPublishOptions{
			Data: []byte("benchmark_data_payload_for_testing"),
		})
		require.NoError(b, err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		cached, err := NewCachedMapBroker(node, backend, CachedMapBrokerConfig{
			Cache: MapCacheConfig{
				MaxChannels: 10000,
				StreamSize:  10000,
			},
			SyncInterval: time.Hour,
		})
		require.NoError(b, err)
		b.StartTimer()

		// Cold read triggers load
		_, _ = cached.ReadState(ctx, "bench_cold_load", MapReadStateOptions{Limit: -1})

		b.StopTimer()
		_ = cached.Close(ctx)
		b.StartTimer()
	}
}

// BenchmarkCachedMapBroker_ReadState_Parallel benchmarks parallel cached reads.
func BenchmarkCachedMapBroker_ReadState_Parallel(b *testing.B) {
	node, _ := New(Config{
		GetMapChannelOptions: func(channel string) MapChannelOptions {
			return MapChannelOptions{
				SyncMode:      MapSyncConverging,
				RetentionMode: MapRetentionExpiring,
				KeyTTL:        60 * time.Second,
			}
		},
	})
	backend, err := NewMemoryMapBroker(node, MemoryMapBrokerConfig{})
	require.NoError(b, err)
	_ = backend.RegisterEventHandler(nil)

	cached, err := NewCachedMapBroker(node, backend, CachedMapBrokerConfig{
		Cache: MapCacheConfig{
			MaxChannels: 10000,
			StreamSize:  10000,
		},
		SyncInterval: time.Hour,
	})
	require.NoError(b, err)
	defer func() { _ = cached.Close(context.Background()) }()

	ctx := context.Background()
	channel := "bench_parallel_read"

	// Prepopulate
	for i := 0; i < 1000; i++ {
		_, err := cached.Publish(ctx, channel, fmt.Sprintf("key%d", i), MapPublishOptions{
			Data: []byte("benchmark_data_payload_for_testing"),
		})
		require.NoError(b, err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = cached.ReadState(ctx, channel, MapReadStateOptions{Limit: -1})
		}
	})
}

// BenchmarkCachedMapBroker_ReadState_Paginated benchmarks paginated reads.
func BenchmarkCachedMapBroker_ReadState_Paginated(b *testing.B) {
	node, _ := New(Config{
		GetMapChannelOptions: func(channel string) MapChannelOptions {
			return MapChannelOptions{
				SyncMode:      MapSyncConverging,
				RetentionMode: MapRetentionExpiring,
				KeyTTL:        60 * time.Second,
			}
		},
	})
	backend, err := NewMemoryMapBroker(node, MemoryMapBrokerConfig{})
	require.NoError(b, err)
	_ = backend.RegisterEventHandler(nil)

	cached, err := NewCachedMapBroker(node, backend, CachedMapBrokerConfig{
		Cache: MapCacheConfig{
			MaxChannels: 10000,
			StreamSize:  10000,
		},
		SyncInterval: time.Hour,
	})
	require.NoError(b, err)
	defer func() { _ = cached.Close(context.Background()) }()

	ctx := context.Background()
	channel := "bench_paginated"

	// Prepopulate
	for i := 0; i < 1000; i++ {
		_, err := cached.Publish(ctx, channel, fmt.Sprintf("key%04d", i), MapPublishOptions{
			Data: []byte("benchmark_data"),
		})
		require.NoError(b, err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		cursor := ""
		for {
			stateRes, _ := cached.ReadState(ctx, channel, MapReadStateOptions{
				Limit:  100,
				Cursor: cursor,
			})
			nextCursor := stateRes.Cursor
			if nextCursor == "" {
				break
			}
			cursor = nextCursor
		}
	}
}

// BenchmarkCachedMapBroker_Publish benchmarks publish (backend + cache).
func BenchmarkCachedMapBroker_Publish(b *testing.B) {
	node, _ := New(Config{
		GetMapChannelOptions: func(channel string) MapChannelOptions {
			return MapChannelOptions{
				SyncMode:      MapSyncConverging,
				RetentionMode: MapRetentionExpiring,
				KeyTTL:        60 * time.Second,
			}
		},
	})
	backend, err := NewMemoryMapBroker(node, MemoryMapBrokerConfig{})
	require.NoError(b, err)
	_ = backend.RegisterEventHandler(nil)

	cached, err := NewCachedMapBroker(node, backend, CachedMapBrokerConfig{
		Cache: MapCacheConfig{
			MaxChannels: 10000,
			StreamSize:  10000,
		},
		SyncInterval: time.Hour,
	})
	require.NoError(b, err)
	defer func() { _ = cached.Close(context.Background()) }()

	ctx := context.Background()
	channel := "bench_publish"

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _ = cached.Publish(ctx, channel, fmt.Sprintf("key%d", i), MapPublishOptions{
			Data: []byte("benchmark_data_payload"),
		})
	}
}

// BenchmarkCachedMapBroker_Publish_Parallel benchmarks parallel publishes.
func BenchmarkCachedMapBroker_Publish_Parallel(b *testing.B) {
	node, _ := New(Config{
		GetMapChannelOptions: func(channel string) MapChannelOptions {
			return MapChannelOptions{
				SyncMode:      MapSyncConverging,
				RetentionMode: MapRetentionExpiring,
				KeyTTL:        60 * time.Second,
			}
		},
	})
	backend, err := NewMemoryMapBroker(node, MemoryMapBrokerConfig{})
	require.NoError(b, err)
	_ = backend.RegisterEventHandler(nil)

	cached, err := NewCachedMapBroker(node, backend, CachedMapBrokerConfig{
		Cache: MapCacheConfig{
			MaxChannels: 10000,
			StreamSize:  10000,
		},
		SyncInterval: time.Hour,
	})
	require.NoError(b, err)
	defer func() { _ = cached.Close(context.Background()) }()

	ctx := context.Background()
	channel := "bench_parallel_publish"

	b.ReportAllocs()
	b.ResetTimer()

	var counter int64
	b.RunParallel(func(pb *testing.PB) {
		localCounter := 0
		for pb.Next() {
			localCounter++
			_, _ = cached.Publish(ctx, channel, fmt.Sprintf("key%d", localCounter), MapPublishOptions{
				Data: []byte("benchmark_data_payload"),
			})
		}
	})
	_ = counter
}

// BenchmarkMapCache_EnsureLoaded benchmarks load latency.
func BenchmarkMapCache_EnsureLoaded(b *testing.B) {
	node, _ := New(Config{
		GetMapChannelOptions: func(channel string) MapChannelOptions {
			return MapChannelOptions{
				SyncMode:      MapSyncConverging,
				RetentionMode: MapRetentionExpiring,
				KeyTTL:        60 * time.Second,
			}
		},
	})
	backend, err := NewMemoryMapBroker(node, MemoryMapBrokerConfig{})
	require.NoError(b, err)
	_ = backend.RegisterEventHandler(nil)

	ctx := context.Background()

	// Prepopulate backend with multiple channels
	for ch := 0; ch < 100; ch++ {
		for i := 0; i < 100; i++ {
			_, _ = backend.Publish(ctx, fmt.Sprintf("ch%d", ch), fmt.Sprintf("key%d", i), MapPublishOptions{
				Data: []byte("data"),
			})
		}
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		cached, _ := NewCachedMapBroker(node, backend, CachedMapBrokerConfig{
			Cache: MapCacheConfig{
				MaxChannels: 10000,
				StreamSize:  1000,
			},
			SyncInterval: time.Hour,
		})
		b.StartTimer()

		// Load one channel
		_, _ = cached.ReadState(ctx, fmt.Sprintf("ch%d", i%100), MapReadStateOptions{Limit: -1})

		b.StopTimer()
		_ = cached.Close(ctx)
		b.StartTimer()
	}
}

// BenchmarkMapCache_ApplyPublication benchmarks applying updates to cache.
func BenchmarkMapCache_ApplyPublication(b *testing.B) {
	conf := MapCacheConfig{
		MaxChannels: 10000,
		StreamSize:  10000,
	}
	cache := newMapCache(conf)
	defer func() { _ = cache.Close() }()

	ctx := context.Background()
	channel := "bench_apply"

	// Initialize channel
	opts := MapChannelOptions{
		SyncMode:      MapSyncConverging,
		RetentionMode: MapRetentionExpiring,
		StreamSize:    100,
		StreamTTL:     60 * time.Second,
		MetaTTL:       600 * time.Second,
		KeyTTL:        60 * time.Second,
	}
	_ = cache.EnsureLoaded(ctx, channel, opts, func(ctx context.Context, ch string, opts MapChannelOptions) ([]*Publication, []*Publication, StreamPosition, error) {
		return []*Publication{}, []*Publication{}, StreamPosition{Offset: 0, Epoch: "test"}, nil
	})

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		pub := &Publication{
			Offset: uint64(i + 1),
			Key:    fmt.Sprintf("key%d", i),
			Data:   []byte("data"),
			Time:   time.Now().UnixMilli(),
		}
		_, _ = cache.ApplyPublication(channel, pub, StreamPosition{Offset: uint64(i + 1), Epoch: "test"}, false)
	}
}

// BenchmarkReadState_Direct_vs_Cached compares direct vs cached reads.
func BenchmarkReadState_Direct_vs_Cached(b *testing.B) {
	node, _ := New(Config{
		GetMapChannelOptions: func(channel string) MapChannelOptions {
			return MapChannelOptions{
				SyncMode:      MapSyncConverging,
				RetentionMode: MapRetentionExpiring,
				KeyTTL:        60 * time.Second,
			}
		},
	})
	backend, err := NewMemoryMapBroker(node, MemoryMapBrokerConfig{})
	require.NoError(b, err)
	_ = backend.RegisterEventHandler(nil)

	cached, err := NewCachedMapBroker(node, backend, CachedMapBrokerConfig{
		Cache: MapCacheConfig{
			MaxChannels: 10000,
			StreamSize:  10000,
		},
		SyncInterval: time.Hour,
	})
	require.NoError(b, err)
	defer func() { _ = cached.Close(context.Background()) }()

	ctx := context.Background()
	channel := "bench_compare"

	// Prepopulate
	for i := 0; i < 1000; i++ {
		_, err := backend.Publish(ctx, channel, fmt.Sprintf("key%d", i), MapPublishOptions{
			Data: []byte("benchmark_data_payload_for_testing"),
		})
		require.NoError(b, err)
	}

	// Warm up cached broker
	_, _ = cached.ReadState(ctx, channel, MapReadStateOptions{Limit: -1})

	b.Run("Direct", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, _ = backend.ReadState(ctx, channel, MapReadStateOptions{Limit: -1})
		}
	})

	b.Run("AllowCached", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, _ = cached.ReadState(ctx, channel, MapReadStateOptions{Limit: -1})
		}
	})
}

// BenchmarkPublish_Direct_vs_Cached compares direct vs cached writes.
func BenchmarkPublish_Direct_vs_Cached(b *testing.B) {
	node, _ := New(Config{
		GetMapChannelOptions: func(channel string) MapChannelOptions {
			return MapChannelOptions{
				SyncMode:      MapSyncConverging,
				RetentionMode: MapRetentionExpiring,
				KeyTTL:        60 * time.Second,
			}
		},
	})
	backend, err := NewMemoryMapBroker(node, MemoryMapBrokerConfig{})
	require.NoError(b, err)
	_ = backend.RegisterEventHandler(nil)

	cached, err := NewCachedMapBroker(node, backend, CachedMapBrokerConfig{
		Cache: MapCacheConfig{
			MaxChannels: 10000,
			StreamSize:  10000,
		},
		SyncInterval: time.Hour,
	})
	require.NoError(b, err)
	defer func() { _ = cached.Close(context.Background()) }()

	ctx := context.Background()

	b.Run("Direct", func(b *testing.B) {
		channel := "bench_direct_write"
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, _ = backend.Publish(ctx, channel, fmt.Sprintf("key%d", i), MapPublishOptions{
				Data: []byte("benchmark_data"),
			})
		}
	})

	b.Run("AllowCached", func(b *testing.B) {
		channel := "bench_cached_write"
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, _ = cached.Publish(ctx, channel, fmt.Sprintf("key%d", i), MapPublishOptions{
				Data: []byte("benchmark_data"),
			})
		}
	})
}

// BenchmarkCachedMapBroker_SyncChannel benchmarks single channel sync.
func BenchmarkCachedMapBroker_SyncChannel(b *testing.B) {
	node, _ := New(Config{
		GetMapChannelOptions: func(channel string) MapChannelOptions {
			return MapChannelOptions{
				SyncMode:      MapSyncConverging,
				RetentionMode: MapRetentionExpiring,
				KeyTTL:        60 * time.Second,
			}
		},
	})
	backend, err := NewMemoryMapBroker(node, MemoryMapBrokerConfig{})
	require.NoError(b, err)
	_ = backend.RegisterEventHandler(nil)

	cached, err := NewCachedMapBroker(node, backend, CachedMapBrokerConfig{
		Cache: MapCacheConfig{
			MaxChannels: 10000,
			StreamSize:  10000,
		},
		SyncInterval:  time.Hour, // Disable auto sync
		SyncBatchSize: 100,
	})
	require.NoError(b, err)
	defer func() { _ = cached.Close(context.Background()) }()

	ctx := context.Background()
	channel := "bench_sync"

	// Prepopulate and load
	for i := 0; i < 100; i++ {
		_, _ = cached.Publish(ctx, channel, fmt.Sprintf("key%d", i), MapPublishOptions{
			Data: []byte("data"),
		})
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Add new data to backend
		_, _ = backend.Publish(ctx, channel, fmt.Sprintf("sync_key%d", i), MapPublishOptions{
			Data: []byte("sync_data"),
		})

		// Manually trigger sync
		cached.syncChannel(channel)
	}
}

// BenchmarkCachedMapBroker_ManyChannels benchmarks with many active channels.
func BenchmarkCachedMapBroker_ManyChannels(b *testing.B) {
	node, _ := New(Config{
		GetMapChannelOptions: func(channel string) MapChannelOptions {
			return MapChannelOptions{
				SyncMode:      MapSyncConverging,
				RetentionMode: MapRetentionExpiring,
				KeyTTL:        60 * time.Second,
			}
		},
	})
	backend, err := NewMemoryMapBroker(node, MemoryMapBrokerConfig{})
	require.NoError(b, err)
	_ = backend.RegisterEventHandler(nil)

	cached, err := NewCachedMapBroker(node, backend, CachedMapBrokerConfig{
		Cache: MapCacheConfig{
			MaxChannels: 10000,
			StreamSize:  1000,
		},
		SyncInterval: time.Hour,
	})
	require.NoError(b, err)
	defer func() { _ = cached.Close(context.Background()) }()

	ctx := context.Background()

	// Prepopulate 1000 channels
	for ch := 0; ch < 1000; ch++ {
		for i := 0; i < 10; i++ {
			_, _ = cached.Publish(ctx, fmt.Sprintf("channel_%d", ch), fmt.Sprintf("key%d", i), MapPublishOptions{
				Data: []byte("data"),
			})
		}
	}

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		localCounter := 0
		for pb.Next() {
			ch := fmt.Sprintf("channel_%d", localCounter%1000)
			_, _ = cached.ReadState(ctx, ch, MapReadStateOptions{Limit: -1})
			localCounter++
		}
	})
}

// BenchmarkMapReadState_Postgres_Direct_vs_Cached compares direct Postgres reads vs cached reads.
// This shows the real benefit of caching - avoiding database round-trips.
//
// Run with: CENTRIFUGE_POSTGRES_URL="postgres://user:pass@localhost:5432/db" go test -bench=BenchmarkReadState_Postgres -benchmem
func BenchmarkMapReadState_Postgres_Direct_vs_Cached(b *testing.B) {
	pgURL := os.Getenv("CENTRIFUGE_POSTGRES_URL")
	if pgURL == "" {
		b.Skip("CENTRIFUGE_POSTGRES_URL not set, skipping Postgres benchmark")
	}

	node, _ := New(Config{
		GetMapChannelOptions: func(channel string) MapChannelOptions {
			return MapChannelOptions{
				SyncMode:      MapSyncConverging,
				RetentionMode: MapRetentionExpiring,
				KeyTTL:        60 * time.Second,
			}
		},
	})
	backend, err := NewPostgresMapBroker(node, PostgresMapBrokerConfig{
		ConnString: pgURL,
		BinaryData: true,
	})
	require.NoError(b, err)
	err = backend.RegisterEventHandler(nil)
	require.NoError(b, err)
	defer func() { _ = backend.Close(context.Background()) }()

	cached, err := NewCachedMapBroker(node, backend, CachedMapBrokerConfig{
		Cache: MapCacheConfig{
			MaxChannels: 10000,
			StreamSize:  10000,
		},
		SyncInterval: time.Hour, // Disable sync for benchmark
	})
	require.NoError(b, err)
	defer func() { _ = cached.Close(context.Background()) }()

	ctx := context.Background()
	channel := "bench_pg_compare"

	// Clean up before benchmark
	_ = backend.Clear(ctx, channel, MapClearOptions{})

	// Prepopulate with 1000 entries
	for i := 0; i < 1000; i++ {
		_, err := backend.Publish(ctx, channel, fmt.Sprintf("key%04d", i), MapPublishOptions{
			Data: []byte("benchmark_data_payload_for_testing_postgres_performance"),
		})
		require.NoError(b, err)
	}

	// Warm up cached broker (load into cache)
	_, err = cached.ReadState(ctx, channel, MapReadStateOptions{Limit: -1})
	require.NoError(b, err)

	b.Run("Postgres_Direct", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = backend.ReadState(ctx, channel, MapReadStateOptions{Limit: -1})
		}
	})

	b.Run("Postgres_Cached", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = cached.ReadState(ctx, channel, MapReadStateOptions{Limit: -1, AllowCached: true})
		}
	})

	// Cleanup
	_ = backend.Clear(ctx, channel, MapClearOptions{})
}

// BenchmarkMapReadState_Postgres_Parallel compares parallel read performance.
// Run with: CENTRIFUGE_POSTGRES_URL="postgres://user:pass@localhost:5432/db" go test -bench=BenchmarkReadState_Postgres_Parallel -benchmem
func BenchmarkMapReadState_Postgres_Parallel(b *testing.B) {
	pgURL := os.Getenv("CENTRIFUGE_POSTGRES_URL")
	if pgURL == "" {
		b.Skip("CENTRIFUGE_POSTGRES_URL not set, skipping Postgres benchmark")
	}

	node, _ := New(Config{
		GetMapChannelOptions: func(channel string) MapChannelOptions {
			return MapChannelOptions{
				SyncMode:      MapSyncConverging,
				RetentionMode: MapRetentionExpiring,
				KeyTTL:        60 * time.Second,
			}
		},
	})
	backend, err := NewPostgresMapBroker(node, PostgresMapBrokerConfig{
		ConnString: pgURL,
		PoolSize:   32,
		BinaryData: true,
	})
	require.NoError(b, err)
	err = backend.RegisterEventHandler(nil)
	require.NoError(b, err)
	defer func() { _ = backend.Close(context.Background()) }()

	cached, err := NewCachedMapBroker(node, backend, CachedMapBrokerConfig{
		Cache: MapCacheConfig{
			MaxChannels: 10000,
			StreamSize:  10000,
		},
		SyncInterval: time.Hour,
	})
	require.NoError(b, err)
	defer func() { _ = cached.Close(context.Background()) }()

	ctx := context.Background()
	channel := "bench_pg_parallel"

	// Clean up before benchmark
	_ = backend.Clear(ctx, channel, MapClearOptions{})

	// Prepopulate
	for i := 0; i < 1000; i++ {
		_, err := backend.Publish(ctx, channel, fmt.Sprintf("key%04d", i), MapPublishOptions{
			Data: []byte("benchmark_data_payload_for_testing"),
		})
		require.NoError(b, err)
	}

	// Warm up cache
	_, _ = cached.ReadState(ctx, channel, MapReadStateOptions{Limit: -1})

	b.Run("Postgres_Direct_Parallel", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				_, _ = backend.ReadState(ctx, channel, MapReadStateOptions{Limit: -1})
			}
		})
	})

	b.Run("Postgres_Cached_Parallel", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				_, _ = cached.ReadState(ctx, channel, MapReadStateOptions{Limit: -1})
			}
		})
	})

	// Cleanup
	_ = backend.Clear(ctx, channel, MapClearOptions{})
}

// BenchmarkMapPublish_Postgres_Direct_vs_Cached compares write performance.
// Run with: CENTRIFUGE_POSTGRES_URL="postgres://user:pass@localhost:5432/db" go test -bench=BenchmarkPublish_Postgres -benchmem
func BenchmarkMapPublish_Postgres_Direct_vs_Cached(b *testing.B) {
	pgURL := os.Getenv("CENTRIFUGE_POSTGRES_URL")
	if pgURL == "" {
		b.Skip("CENTRIFUGE_POSTGRES_URL not set, skipping Postgres benchmark")
	}

	node, _ := New(Config{
		GetMapChannelOptions: func(channel string) MapChannelOptions {
			return MapChannelOptions{
				SyncMode:      MapSyncConverging,
				RetentionMode: MapRetentionExpiring,
				KeyTTL:        60 * time.Second,
			}
		},
	})
	backend, err := NewPostgresMapBroker(node, PostgresMapBrokerConfig{
		ConnString: pgURL,
	})
	require.NoError(b, err)
	err = backend.RegisterEventHandler(nil)
	require.NoError(b, err)
	defer func() { _ = backend.Close(context.Background()) }()

	cached, err := NewCachedMapBroker(node, backend, CachedMapBrokerConfig{
		Cache: MapCacheConfig{
			MaxChannels: 10000,
			StreamSize:  10000,
		},
		SyncInterval: time.Hour,
	})
	require.NoError(b, err)
	defer func() { _ = cached.Close(context.Background()) }()

	ctx := context.Background()

	b.Run("Postgres_Direct", func(b *testing.B) {
		channel := "bench_pg_write_direct"
		_ = backend.Clear(ctx, channel, MapClearOptions{})

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = backend.Publish(ctx, channel, fmt.Sprintf("key%d", i), MapPublishOptions{
				Data: []byte("benchmark_data"),
			})
		}

		b.StopTimer()
		_ = backend.Clear(ctx, channel, MapClearOptions{})
	})

	b.Run("Postgres_Cached", func(b *testing.B) {
		channel := "bench_pg_write_cached"
		_ = backend.Clear(ctx, channel, MapClearOptions{})

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = cached.Publish(ctx, channel, fmt.Sprintf("key%d", i), MapPublishOptions{
				Data: []byte("benchmark_data"),
			})
		}

		b.StopTimer()
		_ = backend.Clear(ctx, channel, MapClearOptions{})
	})
}
