//go:build integration

package centrifuge

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// setupTwoNodeCachedRedis creates two CachedKeyedEngine instances backed by the same Redis.
// This simulates two distributed nodes sharing the same backend.
// Note: Since there are no real client subscriptions, PUB/SUB won't work in these tests.
// Instead, we rely on the sync mechanism to propagate changes between nodes.
func setupTwoNodeCachedRedis(t *testing.T) (node1 *Node, cached1 *CachedKeyedEngine, node2 *Node, cached2 *CachedKeyedEngine, cleanup func()) {
	// Node 1
	node1, _ = New(Config{})
	redisConf := testSingleRedisConf(6379)
	shard1, err := NewRedisShard(node1, redisConf)
	require.NoError(t, err)
	backend1, err := NewRedisKeyedEngine(node1, RedisKeyedEngineConfig{
		Shards: []*RedisShard{shard1},
	})
	require.NoError(t, err)

	cached1, err = NewCachedKeyedEngine(node1, backend1, CachedKeyedEngineConfig{
		Cache: KeyedCacheConfig{
			MaxChannels:        1000,
			ChannelIdleTimeout: 5 * time.Minute,
			StreamSize:         1000,
		},
		SyncInterval: 50 * time.Millisecond, // Short interval for testing sync mechanism
	})
	require.NoError(t, err)
	err = cached1.RegisterEventHandler(nil)
	require.NoError(t, err)

	// Node 2 - separate node, same Redis
	node2, _ = New(Config{})
	shard2, err := NewRedisShard(node2, redisConf)
	require.NoError(t, err)
	backend2, err := NewRedisKeyedEngine(node2, RedisKeyedEngineConfig{
		Shards: []*RedisShard{shard2},
	})
	require.NoError(t, err)

	cached2, err = NewCachedKeyedEngine(node2, backend2, CachedKeyedEngineConfig{
		Cache: KeyedCacheConfig{
			MaxChannels:        1000,
			ChannelIdleTimeout: 5 * time.Minute,
			StreamSize:         1000,
		},
		SyncInterval: 50 * time.Millisecond,
	})
	require.NoError(t, err)
	err = cached2.RegisterEventHandler(nil)
	require.NoError(t, err)

	cleanup = func() {
		_ = cached1.Close(context.Background())
		_ = cached2.Close(context.Background())
		_ = node1.Shutdown(context.Background())
		_ = node2.Shutdown(context.Background())
	}

	return node1, cached1, node2, cached2, cleanup
}

// TestCachedKeyedEngine_TwoNodes_BasicPublish tests that publications from one node
// are received by another node via the sync mechanism.
func TestCachedKeyedEngine_TwoNodes_BasicPublish(t *testing.T) {
	_, cached1, _, cached2, cleanup := setupTwoNodeCachedRedis(t)
	defer cleanup()

	ctx := context.Background()
	channel := randomChannel("two_nodes_basic")

	// Load channel on both nodes first
	_, _, _, err := cached1.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{Limit: 100})
	require.NoError(t, err)
	_, _, _, err = cached2.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{Limit: 100})
	require.NoError(t, err)

	// Node 1 publishes
	_, err = cached1.Publish(ctx, channel, "key1", KeyedPublishOptions{
		Data:       []byte("from_node1"),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
	})
	require.NoError(t, err)

	// Wait for sync mechanism to propagate (sync checks every 100ms, interval is 50ms)
	time.Sleep(300 * time.Millisecond)

	// Both nodes should see the publication
	pubs1, pos1, _, err := cached1.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{Limit: 100})
	require.NoError(t, err)
	require.Len(t, pubs1, 1)
	require.Equal(t, "key1", pubs1[0].Key)
	require.Equal(t, []byte("from_node1"), pubs1[0].Data)

	pubs2, pos2, _, err := cached2.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{Limit: 100})
	require.NoError(t, err)
	require.Len(t, pubs2, 1, "node2 should see publication from node1 via sync")
	require.Equal(t, "key1", pubs2[0].Key)
	require.Equal(t, []byte("from_node1"), pubs2[0].Data)

	// Positions should match
	require.Equal(t, pos1.Epoch, pos2.Epoch)
	require.Equal(t, pos1.Offset, pos2.Offset)
}

// TestCachedKeyedEngine_TwoNodes_ConcurrentPublish tests concurrent publications
// from both nodes and verifies eventual consistency via sync mechanism.
func TestCachedKeyedEngine_TwoNodes_ConcurrentPublish(t *testing.T) {
	_, cached1, _, cached2, cleanup := setupTwoNodeCachedRedis(t)
	defer cleanup()

	ctx := context.Background()
	channel := randomChannel("two_nodes_concurrent")

	// Load channel on both nodes
	_, _, _, err := cached1.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{Limit: 100})
	require.NoError(t, err)
	_, _, _, err = cached2.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{Limit: 100})
	require.NoError(t, err)

	// Concurrent publications from both nodes
	var wg sync.WaitGroup
	numPubsPerNode := 10

	// Node 1 publishes keys 0-9
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < numPubsPerNode; i++ {
			_, err := cached1.Publish(ctx, channel, fmt.Sprintf("node1_key%d", i), KeyedPublishOptions{
				Data:       []byte(fmt.Sprintf("node1_data%d", i)),
				StreamSize: 100,
				StreamTTL:  300 * time.Second,
			})
			if err != nil {
				t.Errorf("node1 publish error: %v", err)
			}
		}
	}()

	// Node 2 publishes keys 0-9 (different prefix)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < numPubsPerNode; i++ {
			_, err := cached2.Publish(ctx, channel, fmt.Sprintf("node2_key%d", i), KeyedPublishOptions{
				Data:       []byte(fmt.Sprintf("node2_data%d", i)),
				StreamSize: 100,
				StreamTTL:  300 * time.Second,
			})
			if err != nil {
				t.Errorf("node2 publish error: %v", err)
			}
		}
	}()

	wg.Wait()

	// Wait for sync mechanism to propagate
	time.Sleep(500 * time.Millisecond)

	// Both nodes should see all 20 keys
	pubs1, pos1, _, err := cached1.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{Limit: 100})
	require.NoError(t, err)

	pubs2, pos2, _, err := cached2.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{Limit: 100})
	require.NoError(t, err)

	// Should have all keys from both nodes
	require.Len(t, pubs1, numPubsPerNode*2, "node1 should see all %d keys", numPubsPerNode*2)
	require.Len(t, pubs2, numPubsPerNode*2, "node2 should see all %d keys", numPubsPerNode*2)

	// Positions should match
	require.Equal(t, pos1.Epoch, pos2.Epoch, "epochs should match")
	require.Equal(t, pos1.Offset, pos2.Offset, "offsets should match")

	// Verify all keys are present in both caches
	keys1 := make(map[string]bool)
	for _, pub := range pubs1 {
		keys1[pub.Key] = true
	}
	keys2 := make(map[string]bool)
	for _, pub := range pubs2 {
		keys2[pub.Key] = true
	}

	for i := 0; i < numPubsPerNode; i++ {
		require.True(t, keys1[fmt.Sprintf("node1_key%d", i)], "node1 cache missing node1_key%d", i)
		require.True(t, keys1[fmt.Sprintf("node2_key%d", i)], "node1 cache missing node2_key%d", i)
		require.True(t, keys2[fmt.Sprintf("node1_key%d", i)], "node2 cache missing node1_key%d", i)
		require.True(t, keys2[fmt.Sprintf("node2_key%d", i)], "node2 cache missing node2_key%d", i)
	}
}

// TestCachedKeyedEngine_TwoNodes_UpdateSameKey tests both nodes updating the same key
// and verifies both caches converge to the same final value via sync mechanism.
func TestCachedKeyedEngine_TwoNodes_UpdateSameKey(t *testing.T) {
	_, cached1, _, cached2, cleanup := setupTwoNodeCachedRedis(t)
	defer cleanup()

	ctx := context.Background()
	channel := randomChannel("two_nodes_same_key")

	// Load channel on both nodes
	_, _, _, err := cached1.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{Limit: 100})
	require.NoError(t, err)
	_, _, _, err = cached2.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{Limit: 100})
	require.NoError(t, err)

	// Both nodes update the same key multiple times
	for i := 0; i < 5; i++ {
		_, err = cached1.Publish(ctx, channel, "shared_key", KeyedPublishOptions{
			Data:       []byte(fmt.Sprintf("node1_update%d", i)),
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
		})
		require.NoError(t, err)

		_, err = cached2.Publish(ctx, channel, "shared_key", KeyedPublishOptions{
			Data:       []byte(fmt.Sprintf("node2_update%d", i)),
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
		})
		require.NoError(t, err)
	}

	// Wait for sync mechanism to propagate
	time.Sleep(500 * time.Millisecond)

	// Both nodes should have exactly 1 key with the same final value
	pubs1, pos1, _, err := cached1.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{Limit: 100})
	require.NoError(t, err)
	require.Len(t, pubs1, 1)

	pubs2, pos2, _, err := cached2.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{Limit: 100})
	require.NoError(t, err)
	require.Len(t, pubs2, 1)

	// Both should have the same final value (last write wins)
	require.Equal(t, pubs1[0].Data, pubs2[0].Data, "both nodes should have same final value")
	require.Equal(t, pos1.Epoch, pos2.Epoch)
	require.Equal(t, pos1.Offset, pos2.Offset)

	// Stream should have all 10 updates
	stream1, _, err := cached1.ReadStream(ctx, channel, KeyedReadStreamOptions{
		Filter: StreamFilter{Limit: -1},
	})
	require.NoError(t, err)
	require.Len(t, stream1, 10, "stream should have all 10 updates")

	stream2, _, err := cached2.ReadStream(ctx, channel, KeyedReadStreamOptions{
		Filter: StreamFilter{Limit: -1},
	})
	require.NoError(t, err)
	require.Len(t, stream2, 10, "stream should have all 10 updates")
}

// TestCachedKeyedEngine_TwoNodes_GapFilling tests that when a node loads a channel
// after another node has published, it gets all data from the backend.
func TestCachedKeyedEngine_TwoNodes_GapFilling(t *testing.T) {
	_, cached1, _, cached2, cleanup := setupTwoNodeCachedRedis(t)
	defer cleanup()

	ctx := context.Background()
	channel := randomChannel("two_nodes_gap")

	// Only node 1 loads the channel initially
	_, _, _, err := cached1.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{Limit: 100})
	require.NoError(t, err)

	// Node 1 publishes several keys
	for i := 0; i < 5; i++ {
		_, err = cached1.Publish(ctx, channel, fmt.Sprintf("key%d", i), KeyedPublishOptions{
			Data:       []byte(fmt.Sprintf("data%d", i)),
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
		})
		require.NoError(t, err)
	}

	// Node 2 now loads the channel - should get all data from backend
	pubs2, _, _, err := cached2.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{Limit: 100})
	require.NoError(t, err)
	require.Len(t, pubs2, 5, "node2 should load all 5 keys from backend")

	// Node 1 publishes more
	for i := 5; i < 10; i++ {
		_, err = cached1.Publish(ctx, channel, fmt.Sprintf("key%d", i), KeyedPublishOptions{
			Data:       []byte(fmt.Sprintf("data%d", i)),
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
		})
		require.NoError(t, err)
	}

	// Wait for sync mechanism
	time.Sleep(500 * time.Millisecond)

	// Both should have all 10 keys
	pubs1, pos1, _, err := cached1.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{Limit: 100})
	require.NoError(t, err)
	require.Len(t, pubs1, 10)

	pubs2, pos2, _, err := cached2.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{Limit: 100})
	require.NoError(t, err)
	require.Len(t, pubs2, 10, "node2 should have all 10 keys via sync")

	require.Equal(t, pos1.Offset, pos2.Offset)
}

// TestCachedKeyedEngine_TwoNodes_Unpublish tests that unpublish (removal) propagates
// correctly to both nodes via sync mechanism.
func TestCachedKeyedEngine_TwoNodes_Unpublish(t *testing.T) {
	_, cached1, _, cached2, cleanup := setupTwoNodeCachedRedis(t)
	defer cleanup()

	ctx := context.Background()
	channel := randomChannel("two_nodes_unpublish")

	// Load channel on both nodes
	_, _, _, err := cached1.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{Limit: 100})
	require.NoError(t, err)
	_, _, _, err = cached2.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{Limit: 100})
	require.NoError(t, err)

	// Node 1 publishes some keys
	for i := 0; i < 5; i++ {
		_, err = cached1.Publish(ctx, channel, fmt.Sprintf("key%d", i), KeyedPublishOptions{
			Data:       []byte(fmt.Sprintf("data%d", i)),
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
		})
		require.NoError(t, err)
	}

	// Wait for sync
	time.Sleep(500 * time.Millisecond)

	// Verify both have 5 keys
	pubs1, _, _, _ := cached1.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{Limit: 100})
	require.Len(t, pubs1, 5)
	pubs2, _, _, _ := cached2.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{Limit: 100})
	require.Len(t, pubs2, 5)

	// Node 2 removes some keys
	for i := 0; i < 3; i++ {
		_, err = cached2.Unpublish(ctx, channel, fmt.Sprintf("key%d", i), KeyedUnpublishOptions{
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
		})
		require.NoError(t, err)
	}

	// Wait for sync
	time.Sleep(500 * time.Millisecond)

	// Both should have 2 keys remaining
	pubs1, pos1, _, err := cached1.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{Limit: 100})
	require.NoError(t, err)
	require.Len(t, pubs1, 2, "node1 should see removals from node2")

	pubs2, pos2, _, err := cached2.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{Limit: 100})
	require.NoError(t, err)
	require.Len(t, pubs2, 2)

	require.Equal(t, pos1.Offset, pos2.Offset)

	// Remaining keys should be key3 and key4
	keys := make(map[string]bool)
	for _, pub := range pubs1 {
		keys[pub.Key] = true
	}
	require.True(t, keys["key3"])
	require.True(t, keys["key4"])
}

// TestCachedKeyedEngine_TwoNodes_HighConcurrency stress tests with high concurrency
// from both nodes simultaneously, verifying eventual consistency via sync mechanism.
func TestCachedKeyedEngine_TwoNodes_HighConcurrency(t *testing.T) {
	_, cached1, _, cached2, cleanup := setupTwoNodeCachedRedis(t)
	defer cleanup()

	ctx := context.Background()
	channel := randomChannel("two_nodes_stress")

	// Load channel on both nodes
	_, _, _, err := cached1.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{Limit: 1000})
	require.NoError(t, err)
	_, _, _, err = cached2.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{Limit: 1000})
	require.NoError(t, err)

	var wg sync.WaitGroup
	numGoroutines := 10
	pubsPerGoroutine := 20

	// Node 1 goroutines
	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < pubsPerGoroutine; i++ {
				_, err := cached1.Publish(ctx, channel, fmt.Sprintf("n1_g%d_k%d", gid, i), KeyedPublishOptions{
					Data:       []byte(fmt.Sprintf("n1_g%d_d%d", gid, i)),
					StreamSize: 1000,
					StreamTTL:  300 * time.Second,
				})
				if err != nil {
					t.Errorf("n1 g%d publish error: %v", gid, err)
				}
			}
		}(g)
	}

	// Node 2 goroutines
	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < pubsPerGoroutine; i++ {
				_, err := cached2.Publish(ctx, channel, fmt.Sprintf("n2_g%d_k%d", gid, i), KeyedPublishOptions{
					Data:       []byte(fmt.Sprintf("n2_g%d_d%d", gid, i)),
					StreamSize: 1000,
					StreamTTL:  300 * time.Second,
				})
				if err != nil {
					t.Errorf("n2 g%d publish error: %v", gid, err)
				}
			}
		}(g)
	}

	wg.Wait()

	// Wait for sync mechanism to propagate all changes
	time.Sleep(1 * time.Second)

	expectedKeys := numGoroutines * pubsPerGoroutine * 2 // 10 * 20 * 2 = 400

	pubs1, pos1, _, err := cached1.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{Limit: 1000})
	require.NoError(t, err)

	pubs2, pos2, _, err := cached2.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{Limit: 1000})
	require.NoError(t, err)

	require.Len(t, pubs1, expectedKeys, "node1 should have all %d keys", expectedKeys)
	require.Len(t, pubs2, expectedKeys, "node2 should have all %d keys", expectedKeys)

	require.Equal(t, pos1.Epoch, pos2.Epoch, "epochs should match")
	require.Equal(t, pos1.Offset, pos2.Offset, "offsets should match")
}
