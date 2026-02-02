package centrifuge

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// BenchmarkCachedEngine_ReadSnapshot_Cached benchmarks reading from warm cache.
func BenchmarkCachedEngine_ReadSnapshot_Cached(b *testing.B) {
	node, _ := New(Config{})
	backend, err := NewMemoryKeyedEngine(node, MemoryKeyedEngineConfig{})
	require.NoError(b, err)
	_ = backend.RegisterBrokerEventHandler(nil)

	cached, err := NewCachedKeyedEngine(node, backend, CachedKeyedEngineConfig{
		Cache: KeyedCacheConfig{
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
		_, err := cached.Publish(ctx, channel, fmt.Sprintf("key%d", i), KeyedPublishOptions{
			Data:       []byte("benchmark_data_payload_for_testing"),
			StreamSize: 10000,
			StreamTTL:  300 * time.Second,
		})
		require.NoError(b, err)
	}

	// Ensure loaded
	_, _, _, err = cached.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{Limit: 0})
	require.NoError(b, err)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _, _, _ = cached.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{
			Limit: 0, // Get all
		})
	}
}

// BenchmarkCachedEngine_ReadSnapshot_ColdLoad benchmarks first read (load from backend).
func BenchmarkCachedEngine_ReadSnapshot_ColdLoad(b *testing.B) {
	node, _ := New(Config{})
	backend, err := NewMemoryKeyedEngine(node, MemoryKeyedEngineConfig{})
	require.NoError(b, err)
	_ = backend.RegisterBrokerEventHandler(nil)

	ctx := context.Background()

	// Prepopulate backend
	for i := 0; i < 1000; i++ {
		_, err := backend.Publish(ctx, "bench_cold_load", fmt.Sprintf("key%d", i), KeyedPublishOptions{
			Data:       []byte("benchmark_data_payload_for_testing"),
			StreamSize: 10000,
			StreamTTL:  300 * time.Second,
		})
		require.NoError(b, err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		cached, err := NewCachedKeyedEngine(node, backend, CachedKeyedEngineConfig{
			Cache: KeyedCacheConfig{
				MaxChannels: 10000,
				StreamSize:  10000,
			},
			SyncInterval: time.Hour,
		})
		require.NoError(b, err)
		b.StartTimer()

		// Cold read triggers load
		_, _, _, _ = cached.ReadSnapshot(ctx, "bench_cold_load", KeyedReadSnapshotOptions{Limit: 0})

		b.StopTimer()
		_ = cached.Close(ctx)
		b.StartTimer()
	}
}

// BenchmarkCachedEngine_ReadSnapshot_Parallel benchmarks parallel cached reads.
func BenchmarkCachedEngine_ReadSnapshot_Parallel(b *testing.B) {
	node, _ := New(Config{})
	backend, err := NewMemoryKeyedEngine(node, MemoryKeyedEngineConfig{})
	require.NoError(b, err)
	_ = backend.RegisterBrokerEventHandler(nil)

	cached, err := NewCachedKeyedEngine(node, backend, CachedKeyedEngineConfig{
		Cache: KeyedCacheConfig{
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
		_, err := cached.Publish(ctx, channel, fmt.Sprintf("key%d", i), KeyedPublishOptions{
			Data:       []byte("benchmark_data_payload_for_testing"),
			StreamSize: 10000,
			StreamTTL:  300 * time.Second,
		})
		require.NoError(b, err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _, _, _ = cached.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{Limit: 0})
		}
	})
}

// BenchmarkCachedEngine_ReadSnapshot_Paginated benchmarks paginated reads.
func BenchmarkCachedEngine_ReadSnapshot_Paginated(b *testing.B) {
	node, _ := New(Config{})
	backend, err := NewMemoryKeyedEngine(node, MemoryKeyedEngineConfig{})
	require.NoError(b, err)
	_ = backend.RegisterBrokerEventHandler(nil)

	cached, err := NewCachedKeyedEngine(node, backend, CachedKeyedEngineConfig{
		Cache: KeyedCacheConfig{
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
		_, err := cached.Publish(ctx, channel, fmt.Sprintf("key%04d", i), KeyedPublishOptions{
			Data:       []byte("benchmark_data"),
			StreamSize: 10000,
			StreamTTL:  300 * time.Second,
		})
		require.NoError(b, err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		cursor := ""
		for {
			_, _, nextCursor, _ := cached.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{
				Limit:  100,
				Cursor: cursor,
			})
			if nextCursor == "" {
				break
			}
			cursor = nextCursor
		}
	}
}

// BenchmarkCachedEngine_Publish benchmarks publish (backend + cache).
func BenchmarkCachedEngine_Publish(b *testing.B) {
	node, _ := New(Config{})
	backend, err := NewMemoryKeyedEngine(node, MemoryKeyedEngineConfig{})
	require.NoError(b, err)
	_ = backend.RegisterBrokerEventHandler(nil)

	cached, err := NewCachedKeyedEngine(node, backend, CachedKeyedEngineConfig{
		Cache: KeyedCacheConfig{
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
		_, _ = cached.Publish(ctx, channel, fmt.Sprintf("key%d", i), KeyedPublishOptions{
			Data:       []byte("benchmark_data_payload"),
			StreamSize: 10000,
			StreamTTL:  300 * time.Second,
		})
	}
}

// BenchmarkCachedEngine_Publish_Parallel benchmarks parallel publishes.
func BenchmarkCachedEngine_Publish_Parallel(b *testing.B) {
	node, _ := New(Config{})
	backend, err := NewMemoryKeyedEngine(node, MemoryKeyedEngineConfig{})
	require.NoError(b, err)
	_ = backend.RegisterBrokerEventHandler(nil)

	cached, err := NewCachedKeyedEngine(node, backend, CachedKeyedEngineConfig{
		Cache: KeyedCacheConfig{
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
			_, _ = cached.Publish(ctx, channel, fmt.Sprintf("key%d", localCounter), KeyedPublishOptions{
				Data:       []byte("benchmark_data_payload"),
				StreamSize: 10000,
				StreamTTL:  300 * time.Second,
			})
		}
	})
	_ = counter
}

// BenchmarkKeyedCache_EnsureLoaded benchmarks load latency.
func BenchmarkKeyedCache_EnsureLoaded(b *testing.B) {
	node, _ := New(Config{})
	backend, err := NewMemoryKeyedEngine(node, MemoryKeyedEngineConfig{})
	require.NoError(b, err)
	_ = backend.RegisterBrokerEventHandler(nil)

	ctx := context.Background()

	// Prepopulate backend with multiple channels
	for ch := 0; ch < 100; ch++ {
		for i := 0; i < 100; i++ {
			_, _ = backend.Publish(ctx, fmt.Sprintf("ch%d", ch), fmt.Sprintf("key%d", i), KeyedPublishOptions{
				Data:       []byte("data"),
				StreamSize: 1000,
				StreamTTL:  300 * time.Second,
			})
		}
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		cached, _ := NewCachedKeyedEngine(node, backend, CachedKeyedEngineConfig{
			Cache: KeyedCacheConfig{
				MaxChannels: 10000,
				StreamSize:  1000,
			},
			SyncInterval: time.Hour,
		})
		b.StartTimer()

		// Load one channel
		_, _, _, _ = cached.ReadSnapshot(ctx, fmt.Sprintf("ch%d", i%100), KeyedReadSnapshotOptions{Limit: 0})

		b.StopTimer()
		_ = cached.Close(ctx)
		b.StartTimer()
	}
}

// BenchmarkKeyedCache_ApplyPublication benchmarks applying updates to cache.
func BenchmarkKeyedCache_ApplyPublication(b *testing.B) {
	conf := KeyedCacheConfig{
		MaxChannels: 10000,
		StreamSize:  10000,
	}
	cache := newKeyedCache(conf)
	defer func() { _ = cache.Close() }()

	ctx := context.Background()
	channel := "bench_apply"

	// Initialize channel
	opts := DefaultKeyedChannelOptions()
	_ = cache.EnsureLoaded(ctx, channel, opts, func(ctx context.Context, ch string, opts KeyedChannelOptions) ([]*Publication, []*Publication, StreamPosition, error) {
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
		_ = cache.ApplyPublication(channel, pub, StreamPosition{Offset: uint64(i + 1), Epoch: "test"}, false)
	}
}

// BenchmarkReadSnapshot_Direct_vs_Cached compares direct vs cached reads.
func BenchmarkReadSnapshot_Direct_vs_Cached(b *testing.B) {
	node, _ := New(Config{})
	backend, err := NewMemoryKeyedEngine(node, MemoryKeyedEngineConfig{})
	require.NoError(b, err)
	_ = backend.RegisterBrokerEventHandler(nil)

	cached, err := NewCachedKeyedEngine(node, backend, CachedKeyedEngineConfig{
		Cache: KeyedCacheConfig{
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
		_, err := backend.Publish(ctx, channel, fmt.Sprintf("key%d", i), KeyedPublishOptions{
			Data:       []byte("benchmark_data_payload_for_testing"),
			StreamSize: 10000,
			StreamTTL:  300 * time.Second,
		})
		require.NoError(b, err)
	}

	// Warm up cached engine
	_, _, _, _ = cached.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{Limit: 0})

	b.Run("Direct", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, _, _, _ = backend.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{Limit: 0})
		}
	})

	b.Run("Cached", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, _, _, _ = cached.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{Limit: 0})
		}
	})
}

// BenchmarkPublish_Direct_vs_Cached compares direct vs cached writes.
func BenchmarkPublish_Direct_vs_Cached(b *testing.B) {
	node, _ := New(Config{})
	backend, err := NewMemoryKeyedEngine(node, MemoryKeyedEngineConfig{})
	require.NoError(b, err)
	_ = backend.RegisterBrokerEventHandler(nil)

	cached, err := NewCachedKeyedEngine(node, backend, CachedKeyedEngineConfig{
		Cache: KeyedCacheConfig{
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
			_, _ = backend.Publish(ctx, channel, fmt.Sprintf("key%d", i), KeyedPublishOptions{
				Data:       []byte("benchmark_data"),
				StreamSize: 10000,
				StreamTTL:  300 * time.Second,
			})
		}
	})

	b.Run("Cached", func(b *testing.B) {
		channel := "bench_cached_write"
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, _ = cached.Publish(ctx, channel, fmt.Sprintf("key%d", i), KeyedPublishOptions{
				Data:       []byte("benchmark_data"),
				StreamSize: 10000,
				StreamTTL:  300 * time.Second,
			})
		}
	})
}

// BenchmarkCachedEngine_SyncChannel benchmarks single channel sync.
func BenchmarkCachedEngine_SyncChannel(b *testing.B) {
	node, _ := New(Config{})
	backend, err := NewMemoryKeyedEngine(node, MemoryKeyedEngineConfig{})
	require.NoError(b, err)
	_ = backend.RegisterBrokerEventHandler(nil)

	cached, err := NewCachedKeyedEngine(node, backend, CachedKeyedEngineConfig{
		Cache: KeyedCacheConfig{
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
		_, _ = cached.Publish(ctx, channel, fmt.Sprintf("key%d", i), KeyedPublishOptions{
			Data:       []byte("data"),
			StreamSize: 10000,
			StreamTTL:  300 * time.Second,
		})
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Add new data to backend
		_, _ = backend.Publish(ctx, channel, fmt.Sprintf("sync_key%d", i), KeyedPublishOptions{
			Data:       []byte("sync_data"),
			StreamSize: 10000,
			StreamTTL:  300 * time.Second,
		})

		// Manually trigger sync
		cached.syncChannel(channel)
	}
}

// BenchmarkCachedEngine_ManyChannels benchmarks with many active channels.
func BenchmarkCachedEngine_ManyChannels(b *testing.B) {
	node, _ := New(Config{})
	backend, err := NewMemoryKeyedEngine(node, MemoryKeyedEngineConfig{})
	require.NoError(b, err)
	_ = backend.RegisterBrokerEventHandler(nil)

	cached, err := NewCachedKeyedEngine(node, backend, CachedKeyedEngineConfig{
		Cache: KeyedCacheConfig{
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
			_, _ = cached.Publish(ctx, fmt.Sprintf("channel_%d", ch), fmt.Sprintf("key%d", i), KeyedPublishOptions{
				Data:       []byte("data"),
				StreamSize: 1000,
				StreamTTL:  300 * time.Second,
			})
		}
	}

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		localCounter := 0
		for pb.Next() {
			ch := fmt.Sprintf("channel_%d", localCounter%1000)
			_, _, _, _ = cached.ReadSnapshot(ctx, ch, KeyedReadSnapshotOptions{Limit: 0})
			localCounter++
		}
	})
}

// BenchmarkKeyedReadSnapshot_Postgres_Direct_vs_Cached compares direct Postgres reads vs cached reads.
// This shows the real benefit of caching - avoiding database round-trips.
//
// Run with: CENTRIFUGE_POSTGRES_URL="postgres://user:pass@localhost:5432/db" go test -bench=BenchmarkReadSnapshot_Postgres -benchmem
func BenchmarkKeyedReadSnapshot_Postgres_Direct_vs_Cached(b *testing.B) {
	pgURL := os.Getenv("CENTRIFUGE_POSTGRES_URL")
	if pgURL == "" {
		b.Skip("CENTRIFUGE_POSTGRES_URL not set, skipping Postgres benchmark")
	}

	node, _ := New(Config{})
	backend, err := NewPostgresKeyedEngine(node, PostgresKeyedEngineConfig{
		ConnString: pgURL,
	})
	require.NoError(b, err)
	err = backend.RegisterEventHandler(nil)
	require.NoError(b, err)
	defer func() { _ = backend.Close(context.Background()) }()

	cached, err := NewCachedKeyedEngine(node, backend, CachedKeyedEngineConfig{
		Cache: KeyedCacheConfig{
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
	_ = backend.Remove(ctx, channel, KeyedRemoveOptions{})

	// Prepopulate with 1000 entries
	for i := 0; i < 1000; i++ {
		_, err := backend.Publish(ctx, channel, fmt.Sprintf("key%04d", i), KeyedPublishOptions{
			Data:       []byte("benchmark_data_payload_for_testing_postgres_performance"),
			StreamSize: 10000,
			StreamTTL:  300 * time.Second,
		})
		require.NoError(b, err)
	}

	// Warm up cached engine (load into cache)
	_, _, _, err = cached.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{Limit: 0})
	require.NoError(b, err)

	b.Run("Postgres_Direct", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _, _, _ = backend.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{Limit: 0})
		}
	})

	b.Run("Postgres_Cached", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _, _, _ = cached.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{Limit: 0})
		}
	})

	// Cleanup
	_ = backend.Remove(ctx, channel, KeyedRemoveOptions{})
}

// BenchmarkKeyedReadSnapshot_Postgres_Parallel compares parallel read performance.
// Run with: CENTRIFUGE_POSTGRES_URL="postgres://user:pass@localhost:5432/db" go test -bench=BenchmarkReadSnapshot_Postgres_Parallel -benchmem
func BenchmarkKeyedReadSnapshot_Postgres_Parallel(b *testing.B) {
	pgURL := os.Getenv("CENTRIFUGE_POSTGRES_URL")
	if pgURL == "" {
		b.Skip("CENTRIFUGE_POSTGRES_URL not set, skipping Postgres benchmark")
	}

	node, _ := New(Config{})
	backend, err := NewPostgresKeyedEngine(node, PostgresKeyedEngineConfig{
		ConnString: pgURL,
		PoolSize:   32,
	})
	require.NoError(b, err)
	err = backend.RegisterEventHandler(nil)
	require.NoError(b, err)
	defer func() { _ = backend.Close(context.Background()) }()

	cached, err := NewCachedKeyedEngine(node, backend, CachedKeyedEngineConfig{
		Cache: KeyedCacheConfig{
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
	_ = backend.Remove(ctx, channel, KeyedRemoveOptions{})

	// Prepopulate
	for i := 0; i < 1000; i++ {
		_, err := backend.Publish(ctx, channel, fmt.Sprintf("key%04d", i), KeyedPublishOptions{
			Data:       []byte("benchmark_data_payload_for_testing"),
			StreamSize: 10000,
			StreamTTL:  300 * time.Second,
		})
		require.NoError(b, err)
	}

	// Warm up cache
	_, _, _, _ = cached.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{Limit: 0})

	b.Run("Postgres_Direct_Parallel", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				_, _, _, _ = backend.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{Limit: 0})
			}
		})
	})

	b.Run("Postgres_Cached_Parallel", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				_, _, _, _ = cached.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{Limit: 0})
			}
		})
	})

	// Cleanup
	_ = backend.Remove(ctx, channel, KeyedRemoveOptions{})
}

// BenchmarkKeyedPublish_Postgres_Direct_vs_Cached compares write performance.
// Run with: CENTRIFUGE_POSTGRES_URL="postgres://user:pass@localhost:5432/db" go test -bench=BenchmarkPublish_Postgres -benchmem
func BenchmarkKeyedPublish_Postgres_Direct_vs_Cached(b *testing.B) {
	pgURL := os.Getenv("CENTRIFUGE_POSTGRES_URL")
	if pgURL == "" {
		b.Skip("CENTRIFUGE_POSTGRES_URL not set, skipping Postgres benchmark")
	}

	node, _ := New(Config{})
	backend, err := NewPostgresKeyedEngine(node, PostgresKeyedEngineConfig{
		ConnString: pgURL,
	})
	require.NoError(b, err)
	err = backend.RegisterEventHandler(nil)
	require.NoError(b, err)
	defer func() { _ = backend.Close(context.Background()) }()

	cached, err := NewCachedKeyedEngine(node, backend, CachedKeyedEngineConfig{
		Cache: KeyedCacheConfig{
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
		_ = backend.Remove(ctx, channel, KeyedRemoveOptions{})

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = backend.Publish(ctx, channel, fmt.Sprintf("key%d", i), KeyedPublishOptions{
				Data:       []byte("benchmark_data"),
				StreamSize: -1,
				StreamTTL:  300 * time.Second,
			})
		}

		b.StopTimer()
		_ = backend.Remove(ctx, channel, KeyedRemoveOptions{})
	})

	b.Run("Postgres_Cached", func(b *testing.B) {
		channel := "bench_pg_write_cached"
		_ = backend.Remove(ctx, channel, KeyedRemoveOptions{})

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = cached.Publish(ctx, channel, fmt.Sprintf("key%d", i), KeyedPublishOptions{
				Data:       []byte("benchmark_data"),
				StreamSize: -1,
				StreamTTL:  300 * time.Second,
			})
		}

		b.StopTimer()
		_ = backend.Remove(ctx, channel, KeyedRemoveOptions{})
	})
}
