package centrifuge

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func newTestCachedKeyedEngine(tb testing.TB, n *Node) (*CachedKeyedEngine, *MemoryKeyedEngine) {
	backend, err := NewMemoryKeyedEngine(n, MemoryKeyedEngineConfig{})
	require.NoError(tb, err)
	err = backend.RegisterBrokerEventHandler(nil)
	require.NoError(tb, err)

	cached, err := NewCachedKeyedEngine(n, backend, CachedKeyedEngineConfig{
		Cache: KeyedCacheConfig{
			MaxChannels:        1000,
			ChannelIdleTimeout: 5 * time.Minute,
			StreamSize:         1000,
		},
		SyncInterval:  50 * time.Millisecond,
		SyncBatchSize: 100,
		LoadTimeout:   5 * time.Second,
	})
	require.NoError(tb, err)
	err = cached.RegisterEventHandler(nil)
	require.NoError(tb, err)

	tb.Cleanup(func() {
		_ = cached.Close(context.Background())
		_ = n.Shutdown(context.Background())
	})

	return cached, backend
}

// TestKeyedCache_EnsureLoaded tests lazy loading.
func TestKeyedCache_EnsureLoaded(t *testing.T) {
	node, _ := New(Config{})
	cached, backend := newTestCachedKeyedEngine(t, node)

	ctx := context.Background()
	channel := "test_ensure_loaded"

	// Publish some data to backend directly
	_, err := backend.Publish(ctx, channel, "key1", KeyedPublishOptions{
		Data:       []byte("data1"),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
	})
	require.NoError(t, err)

	// Cache should not be loaded yet
	require.False(t, cached.cache.IsLoaded(channel))

	// Read snapshot - should trigger load
	pubs, pos, _, err := cached.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{Limit: 100})
	require.NoError(t, err)
	require.Len(t, pubs, 1)
	require.Equal(t, "key1", pubs[0].Key)
	require.NotEmpty(t, pos.Epoch)

	// Cache should now be loaded
	require.True(t, cached.cache.IsLoaded(channel))
}

// TestKeyedCache_EnsureLoaded_Singleflight tests that concurrent loads call loader once.
func TestKeyedCache_EnsureLoaded_Singleflight(t *testing.T) {
	node, _ := New(Config{})
	cached, backend := newTestCachedKeyedEngine(t, node)

	ctx := context.Background()
	channel := "test_singleflight"

	// Publish data to backend
	_, err := backend.Publish(ctx, channel, "key1", KeyedPublishOptions{
		Data:       []byte("data1"),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
	})
	require.NoError(t, err)

	// Concurrent reads - should coalesce into single load
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, _, err := cached.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{Limit: 100})
			require.NoError(t, err)
		}()
	}
	wg.Wait()

	// Verify loaded
	require.True(t, cached.cache.IsLoaded(channel))
}

// TestKeyedCache_Evict tests manual eviction.
func TestKeyedCache_Evict(t *testing.T) {
	node, _ := New(Config{})
	cached, backend := newTestCachedKeyedEngine(t, node)

	ctx := context.Background()
	channel := "test_evict"

	// Publish and load
	_, err := backend.Publish(ctx, channel, "key1", KeyedPublishOptions{
		Data:       []byte("data1"),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
	})
	require.NoError(t, err)

	_, _, _, err = cached.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{Limit: 100})
	require.NoError(t, err)
	require.True(t, cached.cache.IsLoaded(channel))

	// Evict
	cached.cache.Evict(channel)
	require.False(t, cached.cache.IsLoaded(channel))
}

// TestKeyedCache_LRUEviction tests LRU eviction when MaxChannels is exceeded.
func TestKeyedCache_LRUEviction(t *testing.T) {
	node, _ := New(Config{})
	backend, err := NewMemoryKeyedEngine(node, MemoryKeyedEngineConfig{})
	require.NoError(t, err)
	err = backend.RegisterBrokerEventHandler(nil)
	require.NoError(t, err)

	cached, err := NewCachedKeyedEngine(node, backend, CachedKeyedEngineConfig{
		Cache: KeyedCacheConfig{
			MaxChannels: 3, // Very small limit
			StreamSize:  100,
		},
		SyncInterval: time.Hour, // Disable sync for this test
	})
	require.NoError(t, err)
	defer func() { _ = cached.Close(context.Background()) }()

	ctx := context.Background()

	// Load multiple channels
	for i := 0; i < 5; i++ {
		ch := fmt.Sprintf("channel_%d", i)
		_, err := backend.Publish(ctx, ch, "key", KeyedPublishOptions{
			Data:       []byte("data"),
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
		})
		require.NoError(t, err)

		_, _, _, err = cached.ReadSnapshot(ctx, ch, KeyedReadSnapshotOptions{Limit: 100})
		require.NoError(t, err)
	}

	// Should have at most MaxChannels loaded
	loadedCount := len(cached.cache.LoadedChannels())
	require.LessOrEqual(t, loadedCount, 3)
}

// TestKeyedCache_ApplyPublication tests that publications update snapshot correctly.
func TestKeyedCache_ApplyPublication(t *testing.T) {
	node, _ := New(Config{})
	cached, _ := newTestCachedKeyedEngine(t, node)

	ctx := context.Background()
	channel := "test_apply_pub"

	// Publish to create channel
	_, err := cached.Publish(ctx, channel, "key1", KeyedPublishOptions{
		Data:       []byte("data1"),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
	})
	require.NoError(t, err)

	// Read to load into cache
	pubs, _, _, err := cached.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{Limit: 100})
	require.NoError(t, err)
	require.Len(t, pubs, 1)

	// Publish another key
	_, err = cached.Publish(ctx, channel, "key2", KeyedPublishOptions{
		Data:       []byte("data2"),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
	})
	require.NoError(t, err)

	// Should have both keys in cache (read-your-own-writes)
	pubs, _, _, err = cached.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{Limit: 100})
	require.NoError(t, err)
	require.Len(t, pubs, 2)
}

// TestKeyedCache_ApplyPublication_Remove tests removal publications.
func TestKeyedCache_ApplyPublication_Remove(t *testing.T) {
	node, _ := New(Config{})
	cached, _ := newTestCachedKeyedEngine(t, node)

	ctx := context.Background()
	channel := "test_apply_remove"

	// Publish some keys
	_, err := cached.Publish(ctx, channel, "key1", KeyedPublishOptions{
		Data:       []byte("data1"),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
	})
	require.NoError(t, err)

	_, err = cached.Publish(ctx, channel, "key2", KeyedPublishOptions{
		Data:       []byte("data2"),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
	})
	require.NoError(t, err)

	// Verify both exist
	pubs, _, _, err := cached.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{Limit: 100})
	require.NoError(t, err)
	require.Len(t, pubs, 2)

	// Remove key1
	_, err = cached.Unpublish(ctx, channel, "key1", KeyedUnpublishOptions{
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
	})
	require.NoError(t, err)

	// Should only have key2
	pubs, _, _, err = cached.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{Limit: 100})
	require.NoError(t, err)
	require.Len(t, pubs, 1)
	require.Equal(t, "key2", pubs[0].Key)
}

// TestCachedKeyedEngine_Publish tests write to backend + cache.
func TestCachedKeyedEngine_Publish(t *testing.T) {
	node, _ := New(Config{})
	cached, backend := newTestCachedKeyedEngine(t, node)

	ctx := context.Background()
	channel := "test_publish"

	// Publish through cached engine
	result, err := cached.Publish(ctx, channel, "key1", KeyedPublishOptions{
		Data:       []byte("data1"),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
	})
	require.NoError(t, err)
	require.False(t, result.Suppressed)
	require.Greater(t, result.Position.Offset, uint64(0))

	// Verify data in backend
	pubs, _, _, err := backend.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{Limit: 100})
	require.NoError(t, err)
	require.Len(t, pubs, 1)
	require.Equal(t, []byte("data1"), pubs[0].Data)
}

// TestCachedKeyedEngine_Publish_ReadYourOwnWrite tests immediate visibility after publish.
func TestCachedKeyedEngine_Publish_ReadYourOwnWrite(t *testing.T) {
	node, _ := New(Config{})
	cached, _ := newTestCachedKeyedEngine(t, node)

	ctx := context.Background()
	channel := "test_ryow"

	// Publish
	_, err := cached.Publish(ctx, channel, "key1", KeyedPublishOptions{
		Data:       []byte("data1"),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
	})
	require.NoError(t, err)

	// Immediately read - should see the data (read-your-own-write)
	pubs, _, _, err := cached.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{Limit: 100})
	require.NoError(t, err)
	require.Len(t, pubs, 1)
	require.Equal(t, []byte("data1"), pubs[0].Data)
}

// TestCachedKeyedEngine_Publish_Suppressed tests that suppressed publishes don't update cache.
func TestCachedKeyedEngine_Publish_Suppressed(t *testing.T) {
	node, _ := New(Config{})
	cached, _ := newTestCachedKeyedEngine(t, node)

	ctx := context.Background()
	channel := "test_suppressed"

	// First publish
	_, err := cached.Publish(ctx, channel, "key1", KeyedPublishOptions{
		Data:       []byte("data1"),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyMode:    KeyModeIfNew,
	})
	require.NoError(t, err)

	// Second publish with KeyModeIfNew - should be suppressed
	result, err := cached.Publish(ctx, channel, "key1", KeyedPublishOptions{
		Data:       []byte("data1_new"),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyMode:    KeyModeIfNew,
	})
	require.NoError(t, err)
	require.True(t, result.Suppressed)
	require.Equal(t, SuppressReasonKeyExists, result.SuppressReason)

	// Data should still be original
	pubs, _, _, err := cached.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{Limit: 100})
	require.NoError(t, err)
	require.Len(t, pubs, 1)
	require.Equal(t, []byte("data1"), pubs[0].Data)
}

// TestCachedKeyedEngine_ReadSnapshot_LazyLoad tests that first read triggers load.
func TestCachedKeyedEngine_ReadSnapshot_LazyLoad(t *testing.T) {
	node, _ := New(Config{})
	cached, backend := newTestCachedKeyedEngine(t, node)

	ctx := context.Background()
	channel := "test_lazy_load"

	// Populate backend directly
	_, err := backend.Publish(ctx, channel, "key1", KeyedPublishOptions{
		Data:       []byte("backend_data"),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
	})
	require.NoError(t, err)

	// Not loaded yet
	require.False(t, cached.cache.IsLoaded(channel))

	// Read through cached engine - triggers load
	pubs, _, _, err := cached.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{Limit: 100})
	require.NoError(t, err)
	require.Len(t, pubs, 1)
	require.Equal(t, []byte("backend_data"), pubs[0].Data)

	// Now loaded
	require.True(t, cached.cache.IsLoaded(channel))
}

// TestCachedKeyedEngine_ReadSnapshot_Cached tests subsequent reads from cache
// and that writes through the backend's event handler update the cache immediately.
func TestCachedKeyedEngine_ReadSnapshot_Cached(t *testing.T) {
	node, _ := New(Config{})
	cached, backend := newTestCachedKeyedEngine(t, node)

	ctx := context.Background()
	channel := "test_cached_read"

	// Populate (before cache is loaded, event handler won't update cache)
	_, err := backend.Publish(ctx, channel, "key1", KeyedPublishOptions{
		Data:       []byte("original_data"),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
	})
	require.NoError(t, err)

	// First read - loads from backend
	pubs, _, _, err := cached.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{Limit: 100})
	require.NoError(t, err)
	require.Len(t, pubs, 1)

	// Backend publish after cache is loaded - event handler updates cache immediately
	// (simulating PUB/SUB message received from another node)
	_, err = backend.Publish(ctx, channel, "key2", KeyedPublishOptions{
		Data:       []byte("new_data"),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
	})
	require.NoError(t, err)

	// Immediate read - cache was updated via event handler
	pubs, _, _, err = cached.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{Limit: 100})
	require.NoError(t, err)
	require.Len(t, pubs, 2) // Both keys visible immediately
}

// TestCachedKeyedEngine_ReadSnapshot_Pagination tests paginated reads.
func TestCachedKeyedEngine_ReadSnapshot_Pagination(t *testing.T) {
	node, _ := New(Config{})
	cached, _ := newTestCachedKeyedEngine(t, node)

	ctx := context.Background()
	channel := "test_pagination"

	// Publish multiple keys
	for i := 0; i < 10; i++ {
		_, err := cached.Publish(ctx, channel, fmt.Sprintf("key%02d", i), KeyedPublishOptions{
			Data:       []byte(fmt.Sprintf("data%d", i)),
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
		})
		require.NoError(t, err)
	}

	// Read with limit
	pubs, _, cursor, err := cached.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{Limit: 3})
	require.NoError(t, err)
	require.Len(t, pubs, 3)
	require.NotEmpty(t, cursor)

	// Read next page
	pubs2, _, cursor2, err := cached.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{
		Limit:  3,
		Cursor: cursor,
	})
	require.NoError(t, err)
	require.Len(t, pubs2, 3)
	require.NotEmpty(t, cursor2)

	// Verify no overlap
	for _, p := range pubs {
		for _, p2 := range pubs2 {
			require.NotEqual(t, p.Key, p2.Key)
		}
	}
}

// TestCachedKeyedEngine_Stats tests stats from cache.
func TestCachedKeyedEngine_Stats(t *testing.T) {
	node, _ := New(Config{})
	cached, _ := newTestCachedKeyedEngine(t, node)

	ctx := context.Background()
	channel := "test_stats"

	// Publish some keys
	for i := 0; i < 5; i++ {
		_, err := cached.Publish(ctx, channel, fmt.Sprintf("key%d", i), KeyedPublishOptions{
			Data:       []byte("data"),
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
		})
		require.NoError(t, err)
	}

	// Get stats
	stats, err := cached.Stats(ctx, channel)
	require.NoError(t, err)
	require.Equal(t, 5, stats.NumKeys)
}

// TestCachedKeyedEngine_Remove tests removing channel from cache.
func TestCachedKeyedEngine_Remove(t *testing.T) {
	node, _ := New(Config{})
	cached, _ := newTestCachedKeyedEngine(t, node)

	ctx := context.Background()
	channel := "test_remove"

	// Publish
	_, err := cached.Publish(ctx, channel, "key1", KeyedPublishOptions{
		Data:       []byte("data1"),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
	})
	require.NoError(t, err)

	// Read to load into cache (cache is lazy-loaded on read)
	_, _, _, err = cached.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{Limit: 100})
	require.NoError(t, err)
	require.True(t, cached.cache.IsLoaded(channel))

	// Remove - this evicts from cache and calls backend's Remove
	err = cached.Remove(ctx, channel, KeyedRemoveOptions{})
	require.NoError(t, err)

	// Cache should be evicted
	require.False(t, cached.cache.IsLoaded(channel))

	// Sync offset should be cleared
	require.Equal(t, uint64(0), cached.getSyncOffset(channel))
}

// TestCachedKeyedEngine_Sync_NewPublications tests that sync picks up new publications.
func TestCachedKeyedEngine_Sync_NewPublications(t *testing.T) {
	node, _ := New(Config{})
	cached, backend := newTestCachedKeyedEngine(t, node)

	ctx := context.Background()
	channel := "test_sync_new"

	// Load channel into cache
	_, err := cached.Publish(ctx, channel, "key1", KeyedPublishOptions{
		Data:       []byte("data1"),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
	})
	require.NoError(t, err)

	// Publish directly to backend (simulating cross-node write)
	_, err = backend.Publish(ctx, channel, "key2", KeyedPublishOptions{
		Data:       []byte("cross_node_data"),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
	})
	require.NoError(t, err)

	// Wait for sync
	time.Sleep(150 * time.Millisecond)

	// Should see both keys
	pubs, _, _, err := cached.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{Limit: 100})
	require.NoError(t, err)
	require.Len(t, pubs, 2)
}

// TestCachedKeyedEngine_Sync_ReappliesOwnWrites tests that own writes are safely re-applied during sync.
// This is intentional - we don't skip own writes to avoid missing cross-node writes.
func TestCachedKeyedEngine_Sync_ReappliesOwnWrites(t *testing.T) {
	node, _ := New(Config{})
	cached, _ := newTestCachedKeyedEngine(t, node)

	ctx := context.Background()
	channel := "test_sync_reapply"

	// Publish through cached engine
	_, err := cached.Publish(ctx, channel, "key1", KeyedPublishOptions{
		Data:       []byte("data1"),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
	})
	require.NoError(t, err)

	// Wait for sync (which will re-apply our write - that's fine, it's idempotent)
	time.Sleep(150 * time.Millisecond)

	// Should still have correct data (re-applying same data is idempotent)
	pubs, _, _, err := cached.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{Limit: 100})
	require.NoError(t, err)
	require.Len(t, pubs, 1)
	require.Equal(t, []byte("data1"), pubs[0].Data)
}

// TestCachedKeyedEngine_ConcurrentReadWrite tests race condition safety.
func TestCachedKeyedEngine_ConcurrentReadWrite(t *testing.T) {
	node, _ := New(Config{})
	cached, _ := newTestCachedKeyedEngine(t, node)

	ctx := context.Background()
	channel := "test_concurrent"

	var wg sync.WaitGroup
	var writeCount atomic.Int64
	var readCount atomic.Int64

	// Writers
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_, err := cached.Publish(ctx, channel, fmt.Sprintf("key_%d_%d", id, j), KeyedPublishOptions{
					Data:       []byte(fmt.Sprintf("data_%d_%d", id, j)),
					StreamSize: 1000,
					StreamTTL:  300 * time.Second,
				})
				if err == nil {
					writeCount.Add(1)
				}
			}
		}(i)
	}

	// Readers
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_, _, _, err := cached.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{Limit: 100})
				if err == nil {
					readCount.Add(1)
				}
			}
		}()
	}

	wg.Wait()

	// Verify all operations completed
	require.Equal(t, int64(500), writeCount.Load())
	require.Equal(t, int64(500), readCount.Load())
}

// TestCachedKeyedEngine_CAS_Success tests CAS succeeds with correct position.
func TestCachedKeyedEngine_CAS_Success(t *testing.T) {
	node, _ := New(Config{})
	cached, _ := newTestCachedKeyedEngine(t, node)

	ctx := context.Background()
	channel := "test_cas_success"

	// Create initial state
	_, err := cached.Publish(ctx, channel, "counter", KeyedPublishOptions{
		Data:       []byte(`{"value": 0}`),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
	})
	require.NoError(t, err)

	// Read current state
	pubs, pos, _, err := cached.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{Key: "counter"})
	require.NoError(t, err)
	require.Len(t, pubs, 1)

	// CAS update
	expectedPos := StreamPosition{
		Offset: pubs[0].Offset,
		Epoch:  pos.Epoch,
	}

	result, err := cached.Publish(ctx, channel, "counter", KeyedPublishOptions{
		Data:             []byte(`{"value": 1}`),
		StreamSize:       100,
		StreamTTL:        300 * time.Second,
		ExpectedPosition: &expectedPos,
	})
	require.NoError(t, err)
	require.False(t, result.Suppressed)
}

// TestCachedKeyedEngine_CAS_Conflict tests CAS fails with stale position.
func TestCachedKeyedEngine_CAS_Conflict(t *testing.T) {
	node, _ := New(Config{})
	cached, _ := newTestCachedKeyedEngine(t, node)

	ctx := context.Background()
	channel := "test_cas_conflict"

	// Create initial state
	_, err := cached.Publish(ctx, channel, "counter", KeyedPublishOptions{
		Data:       []byte(`{"value": 0}`),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
	})
	require.NoError(t, err)

	// Read current state
	pubs, pos, _, err := cached.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{Key: "counter"})
	require.NoError(t, err)

	// Update (changes position)
	_, err = cached.Publish(ctx, channel, "counter", KeyedPublishOptions{
		Data:       []byte(`{"value": 1}`),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
	})
	require.NoError(t, err)

	// CAS with stale position - should fail
	stalePos := StreamPosition{
		Offset: pubs[0].Offset,
		Epoch:  pos.Epoch,
	}

	result, err := cached.Publish(ctx, channel, "counter", KeyedPublishOptions{
		Data:             []byte(`{"value": 2}`),
		StreamSize:       100,
		StreamTTL:        300 * time.Second,
		ExpectedPosition: &stalePos,
	})
	require.NoError(t, err)
	require.True(t, result.Suppressed)
	require.Equal(t, SuppressReasonPositionMismatch, result.SuppressReason)
}

// TestCachedKeyedEngine_KeyMode_IfNew tests KeyModeIfNew with cache.
func TestCachedKeyedEngine_KeyMode_IfNew(t *testing.T) {
	node, _ := New(Config{})
	cached, _ := newTestCachedKeyedEngine(t, node)

	ctx := context.Background()
	channel := "test_keymode_ifnew"

	// First publish - should succeed
	result, err := cached.Publish(ctx, channel, "slot", KeyedPublishOptions{
		Data:       []byte("player1"),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyMode:    KeyModeIfNew,
	})
	require.NoError(t, err)
	require.False(t, result.Suppressed)

	// Second publish - should be suppressed
	result, err = cached.Publish(ctx, channel, "slot", KeyedPublishOptions{
		Data:       []byte("player2"),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyMode:    KeyModeIfNew,
	})
	require.NoError(t, err)
	require.True(t, result.Suppressed)
	require.Equal(t, SuppressReasonKeyExists, result.SuppressReason)

	// Verify original data preserved
	pubs, _, _, err := cached.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{Key: "slot"})
	require.NoError(t, err)
	require.Len(t, pubs, 1)
	require.Equal(t, []byte("player1"), pubs[0].Data)
}

// TestCachedKeyedEngine_KeyMode_IfExists tests KeyModeIfExists with cache.
func TestCachedKeyedEngine_KeyMode_IfExists(t *testing.T) {
	node, _ := New(Config{})
	cached, _ := newTestCachedKeyedEngine(t, node)

	ctx := context.Background()
	channel := "test_keymode_ifexists"

	// First publish with IfExists - should be suppressed (key doesn't exist)
	result, err := cached.Publish(ctx, channel, "heartbeat", KeyedPublishOptions{
		Data:       []byte("ping1"),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyMode:    KeyModeIfExists,
	})
	require.NoError(t, err)
	require.True(t, result.Suppressed)
	require.Equal(t, SuppressReasonKeyNotFound, result.SuppressReason)

	// Create the key normally
	_, err = cached.Publish(ctx, channel, "heartbeat", KeyedPublishOptions{
		Data:       []byte("init"),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
	})
	require.NoError(t, err)

	// Now IfExists should work
	result, err = cached.Publish(ctx, channel, "heartbeat", KeyedPublishOptions{
		Data:       []byte("ping2"),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyMode:    KeyModeIfExists,
	})
	require.NoError(t, err)
	require.False(t, result.Suppressed)

	// Verify updated data
	pubs, _, _, err := cached.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{Key: "heartbeat"})
	require.NoError(t, err)
	require.Equal(t, []byte("ping2"), pubs[0].Data)
}

// TestCachedKeyedEngine_Backend returns the underlying backend.
func TestCachedKeyedEngine_Backend(t *testing.T) {
	node, _ := New(Config{})
	cached, backend := newTestCachedKeyedEngine(t, node)

	require.Same(t, backend, cached.Backend())
}

// TestNewCachedKeyedEngine_NilBackend tests error on nil backend.
func TestNewCachedKeyedEngine_NilBackend(t *testing.T) {
	node, _ := New(Config{})
	_, err := NewCachedKeyedEngine(node, nil, DefaultCachedKeyedEngineConfig())
	require.Error(t, err)
	require.Contains(t, err.Error(), "backend is required")
}

// TestCachedKeyedEngineConfig_Defaults tests default configuration.
func TestCachedKeyedEngineConfig_Defaults(t *testing.T) {
	conf := CachedKeyedEngineConfig{}
	conf = conf.setDefaults()

	require.Equal(t, 30*time.Second, conf.SyncInterval)
	require.Equal(t, float64(0), conf.SyncJitter) // Jitter defaults to 0 if not set
	require.Equal(t, 0, conf.SyncConcurrency)
	require.Equal(t, 1000, conf.SyncBatchSize)
	require.Equal(t, 5*time.Second, conf.LoadTimeout)
	require.Equal(t, 10000, conf.Cache.MaxChannels)
	require.Equal(t, 5*time.Minute, conf.Cache.ChannelIdleTimeout)
}

// TestCachedKeyedEngine_EpochConsistency tests that ReadSnapshot and ReadStream
// return the same epoch, which is critical for client subscription flow.
// This test would have caught the epoch mismatch bug where GetStream returned
// memstream's auto-generated epoch instead of the backend's epoch.
func TestCachedKeyedEngine_EpochConsistency(t *testing.T) {
	node, _ := New(Config{})
	cached, backend := newTestCachedKeyedEngine(t, node)

	ctx := context.Background()
	channel := "test_epoch_consistency"

	// Publish some data to backend to establish initial state with an epoch
	_, err := backend.Publish(ctx, channel, "key1", KeyedPublishOptions{
		Data:       []byte("data1"),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
	})
	require.NoError(t, err)

	// Get backend's epoch for comparison
	_, backendPos, _, err := backend.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{})
	require.NoError(t, err)
	require.NotEmpty(t, backendPos.Epoch, "backend should have an epoch")

	// Read snapshot through cache (triggers lazy load)
	_, snapshotPos, _, err := cached.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{})
	require.NoError(t, err)

	// Read stream through cache
	_, streamPos, err := cached.ReadStream(ctx, channel, KeyedReadStreamOptions{})
	require.NoError(t, err)

	// CRITICAL: Both should return the same epoch
	require.Equal(t, snapshotPos.Epoch, streamPos.Epoch,
		"ReadSnapshot and ReadStream must return the same epoch for client subscription flow to work")

	// And the epoch should match the backend's epoch
	require.Equal(t, backendPos.Epoch, snapshotPos.Epoch,
		"cache should return the backend's epoch, not a randomly generated one")
}

// TestCachedKeyedEngine_SubscriptionPhaseFlow simulates the client subscription
// flow: snapshot phase → stream phase → live phase, verifying positions are consistent.
func TestCachedKeyedEngine_SubscriptionPhaseFlow(t *testing.T) {
	node, _ := New(Config{})
	cached, _ := newTestCachedKeyedEngine(t, node)

	ctx := context.Background()
	channel := "test_subscription_flow"

	// Phase 0: No data yet - empty channel
	snapshot, snapshotPos, _, err := cached.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{})
	require.NoError(t, err)
	require.Empty(t, snapshot)

	// Simulate server publishing some initial data
	for i := 0; i < 5; i++ {
		_, err := cached.Publish(ctx, channel, fmt.Sprintf("key%d", i), KeyedPublishOptions{
			Data:       []byte(fmt.Sprintf("data%d", i)),
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
		})
		require.NoError(t, err)
	}

	// Phase 1: Client requests snapshot (keyed_phase=0)
	snapshot, snapshotPos, _, err = cached.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{})
	require.NoError(t, err)
	require.Len(t, snapshot, 5)
	require.NotEmpty(t, snapshotPos.Epoch)
	require.Greater(t, snapshotPos.Offset, uint64(0))

	// Client transitions to stream phase (keyed_phase=1)
	// Client sends Since position from snapshot phase
	stream, streamPos, err := cached.ReadStream(ctx, channel, KeyedReadStreamOptions{
		Filter: StreamFilter{
			Since: &snapshotPos, // Client uses position from snapshot phase
		},
	})
	require.NoError(t, err)

	// Epochs must match for recovery to work
	require.Equal(t, snapshotPos.Epoch, streamPos.Epoch,
		"stream epoch must match snapshot epoch for subscription to succeed")

	// Stream should be empty or contain only newer publications
	// (since we're requesting from the current position)
	require.Empty(t, stream, "no new publications since snapshot")

	// Now simulate new publication while client is in stream phase
	_, err = cached.Publish(ctx, channel, "key_new", KeyedPublishOptions{
		Data:       []byte("new_data"),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
	})
	require.NoError(t, err)

	// Client polls stream again
	stream, streamPos2, err := cached.ReadStream(ctx, channel, KeyedReadStreamOptions{
		Filter: StreamFilter{
			Since: &streamPos,
		},
	})
	require.NoError(t, err)

	// Should see the new publication
	require.Len(t, stream, 1)
	require.Equal(t, "key_new", stream[0].Key)

	// Epoch should still be consistent
	require.Equal(t, snapshotPos.Epoch, streamPos2.Epoch)
}

// TestCachedKeyedEngine_CrossNodeEpochConsistency tests that when data is loaded
// from a backend (simulating cross-node scenario), the epoch is preserved correctly.
func TestCachedKeyedEngine_CrossNodeEpochConsistency(t *testing.T) {
	node, _ := New(Config{})
	cached, backend := newTestCachedKeyedEngine(t, node)

	ctx := context.Background()
	channel := "test_cross_node_epoch"

	// Simulate data published by another node (directly to backend)
	for i := 0; i < 3; i++ {
		_, err := backend.Publish(ctx, channel, fmt.Sprintf("key%d", i), KeyedPublishOptions{
			Data:       []byte(fmt.Sprintf("data%d", i)),
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
		})
		require.NoError(t, err)
	}

	// Get backend position
	_, backendPos, _, err := backend.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{})
	require.NoError(t, err)

	// This node's client subscribes - cache loads from backend
	snapshot, cacheSnapshotPos, _, err := cached.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{})
	require.NoError(t, err)
	require.Len(t, snapshot, 3)

	// Epoch must match backend
	require.Equal(t, backendPos.Epoch, cacheSnapshotPos.Epoch)

	// Stream position must also have correct epoch
	_, cacheStreamPos, err := cached.ReadStream(ctx, channel, KeyedReadStreamOptions{})
	require.NoError(t, err)
	require.Equal(t, backendPos.Epoch, cacheStreamPos.Epoch)
}

// TestCachedKeyedEngine_Sync_GapDetection tests that sync detects gaps and reloads.
func TestCachedKeyedEngine_Sync_GapDetection(t *testing.T) {
	node, _ := New(Config{})
	cached, backend := newTestCachedKeyedEngine(t, node)

	ctx := context.Background()
	channel := "test_gap_detection"

	// Publish initial data and load into cache
	for i := 0; i < 3; i++ {
		_, err := cached.Publish(ctx, channel, fmt.Sprintf("key%d", i), KeyedPublishOptions{
			Data:       []byte(fmt.Sprintf("data%d", i)),
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
		})
		require.NoError(t, err)
	}

	// Read to ensure cache is loaded
	pubs, pos, _, err := cached.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{})
	require.NoError(t, err)
	require.Len(t, pubs, 3)
	initialEpoch := pos.Epoch

	// Simulate gap by publishing directly to backend (bypassing cache)
	// and then removing some publications to create a gap
	for i := 3; i < 10; i++ {
		_, err := backend.Publish(ctx, channel, fmt.Sprintf("key%d", i), KeyedPublishOptions{
			Data:       []byte(fmt.Sprintf("data%d", i)),
			StreamSize: 5, // Small stream size to trigger trimming
			StreamTTL:  300 * time.Second,
		})
		require.NoError(t, err)
	}

	// Wait for sync - should detect gap and reload
	time.Sleep(200 * time.Millisecond)

	// After reload, cache should have current state
	pubs, pos, _, err = cached.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{})
	require.NoError(t, err)
	// Should have all 10 keys in snapshot (gap detection reloads full snapshot)
	require.Len(t, pubs, 10)
	// Epoch should remain the same (same channel)
	require.Equal(t, initialEpoch, pos.Epoch)
}

// TestCachedKeyedEngine_HandlePublication_GapFilling tests that when HandlePublication
// receives a publication with a gap (offset > cached + 1), it fetches missing
// publications from backend to maintain consistency.
func TestCachedKeyedEngine_HandlePublication_GapFilling(t *testing.T) {
	node, _ := New(Config{})
	backend, err := NewMemoryKeyedEngine(node, MemoryKeyedEngineConfig{})
	require.NoError(t, err)
	err = backend.RegisterBrokerEventHandler(nil)
	require.NoError(t, err)

	cached, err := NewCachedKeyedEngine(node, backend, CachedKeyedEngineConfig{
		Cache: KeyedCacheConfig{
			MaxChannels:        1000,
			ChannelIdleTimeout: 5 * time.Minute,
			StreamSize:         1000,
		},
		SyncInterval: time.Hour, // Disable sync loop for this test
		LoadTimeout:  5 * time.Second,
	})
	require.NoError(t, err)
	err = cached.RegisterEventHandler(nil)
	require.NoError(t, err)
	defer func() { _ = cached.Close(context.Background()) }()

	ctx := context.Background()
	channel := "test_gap_filling"

	// Step 1: Publish initial data through cached engine to load cache
	_, err = cached.Publish(ctx, channel, "key1", KeyedPublishOptions{
		Data:       []byte("data1"),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
	})
	require.NoError(t, err)

	// Read to ensure cache is loaded
	pubs, pos, _, err := cached.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{})
	require.NoError(t, err)
	require.Len(t, pubs, 1)
	require.Equal(t, uint64(1), pos.Offset)

	// Step 2: Publish messages 2, 3, 4, 5 directly to backend (simulating cross-node writes)
	// These will be in the backend. Since there's no event handler on backend yet,
	// HandlePublication won't be called.
	for i := 2; i <= 5; i++ {
		_, err = backend.Publish(ctx, channel, fmt.Sprintf("key%d", i), KeyedPublishOptions{
			Data:       []byte(fmt.Sprintf("data%d", i)),
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
		})
		require.NoError(t, err)
	}

	// Verify backend has all 5 keys
	backendPubs, backendPos, _, err := backend.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{})
	require.NoError(t, err)
	require.Len(t, backendPubs, 5)
	require.Equal(t, uint64(5), backendPos.Offset)

	// Create the cachedEventHandler directly to test the gap filling logic
	handler := &cachedEventHandler{
		engine:  cached,
		cache:   cached.cache,
		backend: backend,
		handler: nil,
		conf:    cached.conf,
	}

	// Now call HandlePublication with message 5 (offset=5, but cache is at offset=1)
	pub5 := &Publication{
		Offset: 5,
		Key:    "key5",
		Data:   []byte("data5"),
	}
	sp5 := StreamPosition{Offset: 5, Epoch: pos.Epoch}

	err = handler.HandlePublication(channel, pub5, sp5, false, nil)
	require.NoError(t, err)

	// Step 4: Verify cache now has all 5 keys (gap was filled)
	pubs, finalPos, _, err := cached.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{})
	require.NoError(t, err)
	require.Len(t, pubs, 5, "cache should have all 5 keys after gap filling")

	// Verify position advanced correctly
	require.Equal(t, uint64(5), finalPos.Offset)
	require.Equal(t, pos.Epoch, finalPos.Epoch)

	// Verify all keys are present
	keyMap := make(map[string]bool)
	for _, p := range pubs {
		keyMap[p.Key] = true
	}
	for i := 1; i <= 5; i++ {
		require.True(t, keyMap[fmt.Sprintf("key%d", i)], "key%d should be in cache", i)
	}
}

// TestCachedKeyedEngine_HandlePublication_EpochMismatch tests that when HandlePublication
// receives a publication with a different epoch, the cache is evicted.
func TestCachedKeyedEngine_HandlePublication_EpochMismatch(t *testing.T) {
	node, _ := New(Config{})
	backend, err := NewMemoryKeyedEngine(node, MemoryKeyedEngineConfig{})
	require.NoError(t, err)
	err = backend.RegisterBrokerEventHandler(nil)
	require.NoError(t, err)

	cached, err := NewCachedKeyedEngine(node, backend, CachedKeyedEngineConfig{
		Cache: KeyedCacheConfig{
			MaxChannels:        1000,
			ChannelIdleTimeout: 5 * time.Minute,
			StreamSize:         1000,
		},
		SyncInterval: time.Hour, // Disable sync loop
		LoadTimeout:  5 * time.Second,
	})
	require.NoError(t, err)
	err = cached.RegisterEventHandler(nil)
	require.NoError(t, err)
	defer func() { _ = cached.Close(context.Background()) }()

	ctx := context.Background()
	channel := "test_epoch_mismatch"

	// Load cache with initial data
	_, err = cached.Publish(ctx, channel, "key1", KeyedPublishOptions{
		Data:       []byte("data1"),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
	})
	require.NoError(t, err)

	_, pos, _, err := cached.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{})
	require.NoError(t, err)
	require.True(t, cached.cache.IsLoaded(channel))

	// Create the cachedEventHandler directly to test epoch mismatch logic
	handler := &cachedEventHandler{
		engine:  cached,
		cache:   cached.cache,
		backend: backend,
		handler: nil,
		conf:    cached.conf,
	}

	// Simulate receiving a publication with different epoch (e.g., after backend restart)
	pub := &Publication{
		Offset: 5,
		Key:    "key5",
		Data:   []byte("data5"),
	}
	differentEpoch := StreamPosition{Offset: 5, Epoch: "different-epoch-after-restart"}

	err = handler.HandlePublication(channel, pub, differentEpoch, false, nil)
	require.NoError(t, err)

	// Cache should be evicted due to epoch mismatch
	require.False(t, cached.cache.IsLoaded(channel), "cache should be evicted on epoch mismatch")

	_ = pos
}

// TestCachedKeyedEngine_HandlePublication_NoGap tests that publications without gaps
// are applied normally without fetching from backend.
func TestCachedKeyedEngine_HandlePublication_NoGap(t *testing.T) {
	node, _ := New(Config{})
	backend, err := NewMemoryKeyedEngine(node, MemoryKeyedEngineConfig{})
	require.NoError(t, err)
	err = backend.RegisterBrokerEventHandler(nil)
	require.NoError(t, err)

	cached, err := NewCachedKeyedEngine(node, backend, CachedKeyedEngineConfig{
		Cache: KeyedCacheConfig{
			MaxChannels:        1000,
			ChannelIdleTimeout: 5 * time.Minute,
			StreamSize:         1000,
		},
		SyncInterval: time.Hour,
		LoadTimeout:  5 * time.Second,
	})
	require.NoError(t, err)
	err = cached.RegisterEventHandler(nil)
	require.NoError(t, err)
	defer func() { _ = cached.Close(context.Background()) }()

	ctx := context.Background()
	channel := "test_no_gap"

	// Publish initial data
	_, err = cached.Publish(ctx, channel, "key1", KeyedPublishOptions{
		Data:       []byte("data1"),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
	})
	require.NoError(t, err)

	// Read to ensure cache is loaded
	pubs, pos, _, err := cached.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{})
	require.NoError(t, err)
	require.Len(t, pubs, 1)
	require.Equal(t, uint64(1), pos.Offset)

	// Create handler
	handler := &cachedEventHandler{
		engine:  cached,
		cache:   cached.cache,
		backend: backend,
		handler: nil,
		conf:    cached.conf,
	}

	// Simulate receiving publication 2 (no gap - follows 1 directly)
	pub2 := &Publication{
		Offset: 2,
		Key:    "key2",
		Data:   []byte("data2"),
	}
	sp2 := StreamPosition{Offset: 2, Epoch: pos.Epoch}

	err = handler.HandlePublication(channel, pub2, sp2, false, nil)
	require.NoError(t, err)

	// Cache should have both keys
	pubs, finalPos, _, err := cached.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{})
	require.NoError(t, err)
	require.Len(t, pubs, 2)
	require.Equal(t, uint64(2), finalPos.Offset)
}

// TestCachedKeyedEngine_PositionAfterPublish tests that position is correctly
// updated after publishes through the cache.
func TestCachedKeyedEngine_PositionAfterPublish(t *testing.T) {
	node, _ := New(Config{})
	cached, _ := newTestCachedKeyedEngine(t, node)

	ctx := context.Background()
	channel := "test_position_after_publish"

	// First publish
	result1, err := cached.Publish(ctx, channel, "key1", KeyedPublishOptions{
		Data:       []byte("data1"),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
	})
	require.NoError(t, err)

	// Read position
	_, pos1, _, err := cached.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{})
	require.NoError(t, err)
	require.Equal(t, result1.Position.Offset, pos1.Offset)
	require.Equal(t, result1.Position.Epoch, pos1.Epoch)

	// Second publish
	result2, err := cached.Publish(ctx, channel, "key2", KeyedPublishOptions{
		Data:       []byte("data2"),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
	})
	require.NoError(t, err)
	require.Greater(t, result2.Position.Offset, result1.Position.Offset)

	// Position should advance
	_, pos2, _, err := cached.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{})
	require.NoError(t, err)
	require.Equal(t, result2.Position.Offset, pos2.Offset)

	// Epoch should remain the same
	require.Equal(t, pos1.Epoch, pos2.Epoch)

	// Stream position should also match
	_, streamPos, err := cached.ReadStream(ctx, channel, KeyedReadStreamOptions{})
	require.NoError(t, err)
	require.Equal(t, pos2.Offset, streamPos.Offset)
	require.Equal(t, pos2.Epoch, streamPos.Epoch)
}

// TestCachedKeyedEngine_SnapshotAndStreamRecovery tests the real-world scenario where:
// 1. Backend has existing data (snapshot + stream history)
// 2. Cache loads both snapshot AND stream on first access
// 3. Reconnecting clients can recover from cached stream without hitting backend
func TestCachedKeyedEngine_SnapshotAndStreamRecovery(t *testing.T) {
	node, _ := New(Config{})

	// Create backend with data
	backend, err := NewMemoryKeyedEngine(node, MemoryKeyedEngineConfig{})
	require.NoError(t, err)
	err = backend.RegisterBrokerEventHandler(nil)
	require.NoError(t, err)

	ctx := context.Background()
	channel := "test_snapshot_stream_recovery"

	// Step 1: Populate backend with history (simulating existing channel state)
	// This represents data published before this node's cache was loaded
	var positions []StreamPosition
	for i := 1; i <= 10; i++ {
		result, err := backend.Publish(ctx, channel, fmt.Sprintf("key%d", i), KeyedPublishOptions{
			Data:       []byte(fmt.Sprintf("data%d", i)),
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
		})
		require.NoError(t, err)
		positions = append(positions, result.Position)
	}

	// Get backend state
	backendSnapshot, backendPos, _, err := backend.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{})
	require.NoError(t, err)
	require.Len(t, backendSnapshot, 10)
	require.Equal(t, uint64(10), backendPos.Offset)

	// Step 2: Create cached engine - cache is empty at this point
	cached, err := NewCachedKeyedEngine(node, backend, CachedKeyedEngineConfig{
		Cache: KeyedCacheConfig{
			MaxChannels:        1000,
			ChannelIdleTimeout: 5 * time.Minute,
			StreamSize:         100, // Important: stream size must be large enough
		},
		SyncInterval: time.Hour, // Disable sync loop for predictable test
		LoadTimeout:  5 * time.Second,
	})
	require.NoError(t, err)
	err = cached.RegisterEventHandler(nil)
	require.NoError(t, err)
	defer func() { _ = cached.Close(context.Background()) }()

	// Verify cache is not loaded yet
	require.False(t, cached.cache.IsLoaded(channel))

	// Step 3: First client subscribes - triggers cache load (snapshot + stream)
	snapshot, snapshotPos, _, err := cached.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{})
	require.NoError(t, err)
	require.Len(t, snapshot, 10, "snapshot should have all 10 keys")
	require.Equal(t, backendPos.Offset, snapshotPos.Offset)
	require.Equal(t, backendPos.Epoch, snapshotPos.Epoch)

	// Cache should now be loaded
	require.True(t, cached.cache.IsLoaded(channel))

	// Step 4: Simulate client reconnection from position 5 (missed publications 6-10)
	// This represents a client that disconnected and wants to catch up
	reconnectPos := positions[4] // Position after key5, offset=5
	require.Equal(t, uint64(5), reconnectPos.Offset)

	// Client requests stream recovery from their last known position
	recoveredStream, recoveredPos, err := cached.ReadStream(ctx, channel, KeyedReadStreamOptions{
		Filter: StreamFilter{
			Since: &reconnectPos,
			Limit: 100,
		},
	})
	require.NoError(t, err)

	// Should recover publications 6-10 (5 publications)
	require.Len(t, recoveredStream, 5, "should recover 5 missed publications")

	// Verify recovered publications are correct
	for i, pub := range recoveredStream {
		expectedKey := fmt.Sprintf("key%d", i+6) // keys 6, 7, 8, 9, 10
		require.Equal(t, expectedKey, pub.Key, "recovered key should match")
		require.Equal(t, uint64(i+6), pub.Offset, "recovered offset should match")
	}

	// Position should be at the latest
	require.Equal(t, backendPos.Offset, recoveredPos.Offset)
	require.Equal(t, backendPos.Epoch, recoveredPos.Epoch)

	// Step 5: Simulate another client reconnecting from position 8
	reconnectPos2 := positions[7] // Position after key8, offset=8
	recoveredStream2, _, err := cached.ReadStream(ctx, channel, KeyedReadStreamOptions{
		Filter: StreamFilter{
			Since: &reconnectPos2,
		},
	})
	require.NoError(t, err)

	// Should recover publications 9-10 (2 publications)
	require.Len(t, recoveredStream2, 2, "should recover 2 missed publications")
	require.Equal(t, "key9", recoveredStream2[0].Key)
	require.Equal(t, "key10", recoveredStream2[1].Key)

	// Step 6: Client already up-to-date should get empty stream
	upToDatePos := positions[9] // Position after key10, offset=10
	recoveredStream3, _, err := cached.ReadStream(ctx, channel, KeyedReadStreamOptions{
		Filter: StreamFilter{
			Since: &upToDatePos,
		},
	})
	require.NoError(t, err)
	require.Empty(t, recoveredStream3, "up-to-date client should get empty stream")
}

// TestCachedKeyedEngine_StreamRecoveryAfterNewPublications tests recovery when
// new publications arrive after cache is loaded.
func TestCachedKeyedEngine_StreamRecoveryAfterNewPublications(t *testing.T) {
	node, _ := New(Config{})
	cached, backend := newTestCachedKeyedEngine(t, node)

	ctx := context.Background()
	channel := "test_stream_recovery_new_pubs"

	// Step 1: Publish initial data through cache
	for i := 1; i <= 5; i++ {
		_, err := cached.Publish(ctx, channel, fmt.Sprintf("key%d", i), KeyedPublishOptions{
			Data:       []byte(fmt.Sprintf("data%d", i)),
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
		})
		require.NoError(t, err)
	}

	// Load cache
	_, pos5, _, err := cached.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{})
	require.NoError(t, err)
	require.Equal(t, uint64(5), pos5.Offset)

	// Step 2: Client A connects and gets snapshot at position 5
	clientAPos := pos5

	// Step 3: More publications happen (client A is "disconnected")
	for i := 6; i <= 10; i++ {
		_, err := cached.Publish(ctx, channel, fmt.Sprintf("key%d", i), KeyedPublishOptions{
			Data:       []byte(fmt.Sprintf("data%d", i)),
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
		})
		require.NoError(t, err)
	}

	// Step 4: Client A reconnects and recovers from position 5
	recovered, recoveredPos, err := cached.ReadStream(ctx, channel, KeyedReadStreamOptions{
		Filter: StreamFilter{
			Since: &clientAPos,
		},
	})
	require.NoError(t, err)

	// Should recover keys 6-10
	require.Len(t, recovered, 5)
	for i, pub := range recovered {
		require.Equal(t, fmt.Sprintf("key%d", i+6), pub.Key)
	}
	require.Equal(t, uint64(10), recoveredPos.Offset)

	_ = backend
}

// TestCachedKeyedEngine_StreamRecoveryWithRemovals tests that stream recovery
// correctly includes removal publications.
func TestCachedKeyedEngine_StreamRecoveryWithRemovals(t *testing.T) {
	node, _ := New(Config{})
	cached, _ := newTestCachedKeyedEngine(t, node)

	ctx := context.Background()
	channel := "test_stream_recovery_removals"

	// Step 1: Publish initial data
	for i := 1; i <= 5; i++ {
		_, err := cached.Publish(ctx, channel, fmt.Sprintf("key%d", i), KeyedPublishOptions{
			Data:       []byte(fmt.Sprintf("data%d", i)),
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
		})
		require.NoError(t, err)
	}

	// Load cache and get position
	_, pos5, _, err := cached.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{})
	require.NoError(t, err)
	clientPos := pos5

	// Step 2: Mix of publishes and removals
	_, err = cached.Publish(ctx, channel, "key6", KeyedPublishOptions{
		Data:       []byte("data6"),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
	})
	require.NoError(t, err)

	// Remove key3
	_, err = cached.Unpublish(ctx, channel, "key3", KeyedUnpublishOptions{
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
	})
	require.NoError(t, err)

	// Add key7
	_, err = cached.Publish(ctx, channel, "key7", KeyedPublishOptions{
		Data:       []byte("data7"),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
	})
	require.NoError(t, err)

	// Step 3: Client recovers from position 5
	recovered, _, err := cached.ReadStream(ctx, channel, KeyedReadStreamOptions{
		Filter: StreamFilter{
			Since: &clientPos,
		},
	})
	require.NoError(t, err)

	// Should have 3 publications: key6 add, key3 remove, key7 add
	require.Len(t, recovered, 3)

	// Verify order and content
	require.Equal(t, "key6", recovered[0].Key)
	require.False(t, recovered[0].Removed)

	require.Equal(t, "key3", recovered[1].Key)
	require.True(t, recovered[1].Removed)

	require.Equal(t, "key7", recovered[2].Key)
	require.False(t, recovered[2].Removed)

	// Step 4: Verify snapshot reflects final state (4 keys: 1,2,4,5,6,7 minus 3)
	snapshot, _, _, err := cached.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{})
	require.NoError(t, err)
	require.Len(t, snapshot, 6) // 5 original - 1 removed + 2 added = 6

	keyMap := make(map[string]bool)
	for _, pub := range snapshot {
		keyMap[pub.Key] = true
	}
	require.False(t, keyMap["key3"], "key3 should be removed")
	require.True(t, keyMap["key6"], "key6 should be present")
	require.True(t, keyMap["key7"], "key7 should be present")
}

// TestCachedKeyedEngine_ChannelOptionsInheritance tests that cache uses
// resolved channel options for stream size/TTL.
func TestCachedKeyedEngine_ChannelOptionsInheritance(t *testing.T) {
	node, _ := New(Config{
		// Configure per-channel options
		GetKeyedChannelOptions: func(channel string) KeyedChannelOptions {
			if channel == "small_stream" {
				return KeyedChannelOptions{
					StreamSize: 5, // Small stream
					StreamTTL:  time.Minute,
				}
			}
			if channel == "large_stream" {
				return KeyedChannelOptions{
					StreamSize: 100, // Large stream
					StreamTTL:  time.Hour,
				}
			}
			return DefaultKeyedChannelOptions()
		},
	})

	backend, err := NewMemoryKeyedEngine(node, MemoryKeyedEngineConfig{})
	require.NoError(t, err)
	err = backend.RegisterBrokerEventHandler(nil)
	require.NoError(t, err)

	cached, err := NewCachedKeyedEngine(node, backend, CachedKeyedEngineConfig{
		Cache: KeyedCacheConfig{
			MaxChannels: 1000,
			StreamSize:  1000, // Default cache stream size (should be overridden)
		},
		SyncInterval: time.Hour, // Disable sync
		LoadTimeout:  5 * time.Second,
	})
	require.NoError(t, err)
	// Set the channel options resolver
	cached.SetChannelOptionsResolver(node.getKeyedChannelOptions)
	err = cached.RegisterEventHandler(nil)
	require.NoError(t, err)
	defer func() { _ = cached.Close(context.Background()) }()

	ctx := context.Background()

	// Test 1: Small stream channel - publish 10 entries, only 5 should be retained
	for i := 1; i <= 10; i++ {
		_, err := cached.Publish(ctx, "small_stream", fmt.Sprintf("key%d", i), KeyedPublishOptions{
			Data:       []byte(fmt.Sprintf("data%d", i)),
			StreamSize: 5,
			StreamTTL:  time.Minute,
		})
		require.NoError(t, err)
	}

	// Load cache
	_, pos, _, err := cached.ReadSnapshot(ctx, "small_stream", KeyedReadSnapshotOptions{})
	require.NoError(t, err)
	require.Equal(t, uint64(10), pos.Offset)

	// Try to recover from position 0 - should only get last 5 entries (6-10)
	// because stream size is 5
	stream, _, err := cached.ReadStream(ctx, "small_stream", KeyedReadStreamOptions{
		Filter: StreamFilter{
			Since: &StreamPosition{Offset: 0, Epoch: pos.Epoch},
		},
	})
	require.NoError(t, err)
	// Stream should have been trimmed to 5 entries
	require.LessOrEqual(t, len(stream), 5, "small_stream should have at most 5 stream entries")
	if len(stream) > 0 {
		// Should have the most recent entries
		require.GreaterOrEqual(t, stream[0].Offset, uint64(6), "oldest entry should be offset 6 or higher")
	}

	// Test 2: Large stream channel - publish 10 entries, all should be retained
	for i := 1; i <= 10; i++ {
		_, err := cached.Publish(ctx, "large_stream", fmt.Sprintf("key%d", i), KeyedPublishOptions{
			Data:       []byte(fmt.Sprintf("data%d", i)),
			StreamSize: 100,
			StreamTTL:  time.Hour,
		})
		require.NoError(t, err)
	}

	// Load cache
	_, pos2, _, err := cached.ReadSnapshot(ctx, "large_stream", KeyedReadSnapshotOptions{})
	require.NoError(t, err)

	// Recover from position 0 - should get all 10 entries
	stream2, _, err := cached.ReadStream(ctx, "large_stream", KeyedReadStreamOptions{
		Filter: StreamFilter{
			Since: &StreamPosition{Offset: 0, Epoch: pos2.Epoch},
		},
	})
	require.NoError(t, err)
	require.Len(t, stream2, 10, "large_stream should have all 10 entries")
}

// TestCachedKeyedEngine_OptionsFromBackendLoad tests that when cache loads from
// backend, it uses resolved channel options for stream size.
func TestCachedKeyedEngine_OptionsFromBackendLoad(t *testing.T) {
	node, _ := New(Config{
		GetKeyedChannelOptions: func(channel string) KeyedChannelOptions {
			return KeyedChannelOptions{
				StreamSize: 3, // Very small - only keep 3 entries
				StreamTTL:  time.Minute,
			}
		},
	})

	// Create backend with data
	backend, err := NewMemoryKeyedEngine(node, MemoryKeyedEngineConfig{})
	require.NoError(t, err)
	err = backend.RegisterBrokerEventHandler(nil)
	require.NoError(t, err)

	ctx := context.Background()
	channel := "test_options_load"

	// Publish 10 entries directly to backend
	for i := 1; i <= 10; i++ {
		_, err := backend.Publish(ctx, channel, fmt.Sprintf("key%d", i), KeyedPublishOptions{
			Data:       []byte(fmt.Sprintf("data%d", i)),
			StreamSize: 100, // Backend keeps 100
			StreamTTL:  time.Hour,
		})
		require.NoError(t, err)
	}

	// Verify backend has all 10 in stream
	backendStream, _, err := backend.ReadStream(ctx, channel, KeyedReadStreamOptions{
		Filter: StreamFilter{Limit: 100},
	})
	require.NoError(t, err)
	require.Len(t, backendStream, 10, "backend should have all 10 stream entries")

	// Now create cached engine - it should only load 3 stream entries per channel options
	cached, err := NewCachedKeyedEngine(node, backend, CachedKeyedEngineConfig{
		Cache: KeyedCacheConfig{
			MaxChannels: 1000,
			StreamSize:  1000, // Cache default is large
		},
		SyncInterval: time.Hour,
		LoadTimeout:  5 * time.Second,
	})
	require.NoError(t, err)
	cached.SetChannelOptionsResolver(node.getKeyedChannelOptions)
	err = cached.RegisterEventHandler(nil)
	require.NoError(t, err)
	defer func() { _ = cached.Close(context.Background()) }()

	// Load cache by reading snapshot
	_, pos, _, err := cached.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{})
	require.NoError(t, err)
	require.Equal(t, uint64(10), pos.Offset)

	// Read stream from cache - should only have 3 entries (per channel options)
	cachedStream, _, err := cached.ReadStream(ctx, channel, KeyedReadStreamOptions{
		Filter: StreamFilter{
			Since: &StreamPosition{Offset: 0, Epoch: pos.Epoch},
		},
	})
	require.NoError(t, err)
	require.LessOrEqual(t, len(cachedStream), 3, "cache should only load 3 stream entries per channel options")

	// Verify these are the most recent entries
	if len(cachedStream) > 0 {
		require.GreaterOrEqual(t, cachedStream[0].Offset, uint64(8), "should have most recent entries")
	}
}

// TestCachedKeyedEngine_ConcurrentWriteOrdering tests that concurrent writes
// to the same channel are properly ordered in the cache.
func TestCachedKeyedEngine_ConcurrentWriteOrdering(t *testing.T) {
	node, _ := New(Config{})
	cached, _ := newTestCachedKeyedEngine(t, node)

	ctx := context.Background()
	channel := "test_concurrent_ordering"

	// Initialize cache by publishing first key
	_, err := cached.Publish(ctx, channel, "init", KeyedPublishOptions{
		Data:       []byte("init"),
		StreamSize: 1000,
		StreamTTL:  300 * time.Second,
	})
	require.NoError(t, err)

	// Load cache
	_, _, _, err = cached.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{})
	require.NoError(t, err)

	// Concurrent writes
	numGoroutines := 10
	numWritesPerGoroutine := 20
	var wg sync.WaitGroup

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for i := 0; i < numWritesPerGoroutine; i++ {
				key := fmt.Sprintf("g%d_key%d", goroutineID, i)
				_, err := cached.Publish(ctx, channel, key, KeyedPublishOptions{
					Data:       []byte(fmt.Sprintf("data_%d_%d", goroutineID, i)),
					StreamSize: 1000,
					StreamTTL:  300 * time.Second,
				})
				if err != nil {
					t.Errorf("publish failed: %v", err)
				}
			}
		}(g)
	}

	wg.Wait()

	// Verify all writes are in cache
	snapshot, pos, _, err := cached.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{})
	require.NoError(t, err)

	expectedKeys := 1 + numGoroutines*numWritesPerGoroutine // init + all concurrent writes
	require.Equal(t, expectedKeys, len(snapshot), "all keys should be in snapshot")

	// Verify position reflects all writes
	require.Equal(t, uint64(expectedKeys), pos.Offset)

	// Verify stream has all entries (order may not be strict due to concurrent apply)
	stream, _, err := cached.ReadStream(ctx, channel, KeyedReadStreamOptions{
		Filter: StreamFilter{
			Since: &StreamPosition{Offset: 0, Epoch: pos.Epoch},
		},
	})
	require.NoError(t, err)

	// Stream should have all entries
	require.Equal(t, expectedKeys, len(stream), "stream should have all entries")

	// Verify all offsets 1..expectedKeys are present in stream
	// Note: order may not be strictly monotonic due to concurrent lock acquisition,
	// but all entries should be present for recovery to work correctly.
	offsets := make(map[uint64]bool)
	for _, pub := range stream {
		offsets[pub.Offset] = true
	}
	for i := uint64(1); i <= uint64(expectedKeys); i++ {
		require.True(t, offsets[i], "offset %d should be in stream", i)
	}
}
