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

// setupTwoNodeCachedRedis creates two CachedMapBroker instances backed by the same Redis.
// This simulates two distributed nodes sharing the same backend.
// Note: Since there are no real client subscriptions, PUB/SUB won't work in these tests.
// Instead, we rely on the sync mechanism to propagate changes between nodes.
func setupTwoNodeCachedRedis(t *testing.T) (node1 *Node, cached1 *CachedMapBroker, node2 *Node, cached2 *CachedMapBroker, cleanup func()) {
	// Node 1
	node1, _ = New(Config{
		GetMapChannelOptions: func(channel string) MapChannelOptions {
			return MapChannelOptions{
				SyncMode:      MapSyncConverging,
				RetentionMode: MapRetentionExpiring,
				KeyTTL:        60 * time.Second,
			}
		},
	})
	redisConf := testSingleRedisConf(6379)
	shard1, err := NewRedisShard(node1, redisConf)
	require.NoError(t, err)
	backend1, err := NewRedisMapBroker(node1, RedisMapBrokerConfig{
		Shards: []*RedisShard{shard1},
	})
	require.NoError(t, err)

	cached1, err = NewCachedMapBroker(node1, backend1, CachedMapBrokerConfig{
		Cache: MapCacheConfig{
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
	node2, _ = New(Config{
		GetMapChannelOptions: func(channel string) MapChannelOptions {
			return MapChannelOptions{
				SyncMode:      MapSyncConverging,
				RetentionMode: MapRetentionExpiring,
				KeyTTL:        60 * time.Second,
			}
		},
	})
	shard2, err := NewRedisShard(node2, redisConf)
	require.NoError(t, err)
	backend2, err := NewRedisMapBroker(node2, RedisMapBrokerConfig{
		Shards: []*RedisShard{shard2},
	})
	require.NoError(t, err)

	cached2, err = NewCachedMapBroker(node2, backend2, CachedMapBrokerConfig{
		Cache: MapCacheConfig{
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

// TestCachedMapBroker_TwoNodes_BasicPublish tests that publications from one node
// are received by another node via the sync mechanism.
func TestCachedMapBroker_TwoNodes_BasicPublish(t *testing.T) {
	_, cached1, _, cached2, cleanup := setupTwoNodeCachedRedis(t)
	defer cleanup()

	ctx := context.Background()
	channel := randomChannel("two_nodes_basic")

	// Load channel on both nodes first
	_, err := cached1.ReadState(ctx, channel, MapReadStateOptions{Cached: true, Limit: 100})
	require.NoError(t, err)
	_, err = cached2.ReadState(ctx, channel, MapReadStateOptions{Cached: true, Limit: 100})
	require.NoError(t, err)

	// Node 1 publishes
	_, err = cached1.Publish(ctx, channel, "key1", MapPublishOptions{
		Data: []byte("from_node1"),
	})
	require.NoError(t, err)

	// Wait for sync mechanism to propagate (sync checks every 100ms, interval is 50ms)
	time.Sleep(300 * time.Millisecond)

	// Both nodes should see the publication
	stateRes, err := cached1.ReadState(ctx, channel, MapReadStateOptions{Cached: true, Limit: 100})
	pubs1, pos1, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
	require.NoError(t, err)
	require.Len(t, pubs1, 1)
	require.Equal(t, "key1", pubs1[0].Key)
	require.Equal(t, []byte("from_node1"), pubs1[0].Data)

	stateRes, err = cached2.ReadState(ctx, channel, MapReadStateOptions{Cached: true, Limit: 100})
	pubs2, pos2, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
	require.NoError(t, err)
	require.Len(t, pubs2, 1, "node2 should see publication from node1 via sync")
	require.Equal(t, "key1", pubs2[0].Key)
	require.Equal(t, []byte("from_node1"), pubs2[0].Data)

	// Positions should match
	require.Equal(t, pos1.Epoch, pos2.Epoch)
	require.Equal(t, pos1.Offset, pos2.Offset)
}

// TestCachedMapBroker_TwoNodes_ConcurrentPublish tests concurrent publications
// from both nodes and verifies eventual consistency via sync mechanism.
func TestCachedMapBroker_TwoNodes_ConcurrentPublish(t *testing.T) {
	_, cached1, _, cached2, cleanup := setupTwoNodeCachedRedis(t)
	defer cleanup()

	ctx := context.Background()
	channel := randomChannel("two_nodes_concurrent")

	// Load channel on both nodes
	_, err := cached1.ReadState(ctx, channel, MapReadStateOptions{Cached: true, Limit: 100})
	require.NoError(t, err)
	_, err = cached2.ReadState(ctx, channel, MapReadStateOptions{Cached: true, Limit: 100})
	require.NoError(t, err)

	// Concurrent publications from both nodes
	var wg sync.WaitGroup
	numPubsPerNode := 10

	// Node 1 publishes keys 0-9
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < numPubsPerNode; i++ {
			_, err := cached1.Publish(ctx, channel, fmt.Sprintf("node1_key%d", i), MapPublishOptions{
				Data: []byte(fmt.Sprintf("node1_data%d", i)),
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
			_, err := cached2.Publish(ctx, channel, fmt.Sprintf("node2_key%d", i), MapPublishOptions{
				Data: []byte(fmt.Sprintf("node2_data%d", i)),
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
	stateRes, err := cached1.ReadState(ctx, channel, MapReadStateOptions{Cached: true, Limit: 100})
	pubs1, pos1, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
	require.NoError(t, err)

	stateRes, err = cached2.ReadState(ctx, channel, MapReadStateOptions{Cached: true, Limit: 100})
	pubs2, pos2, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
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

// TestCachedMapBroker_TwoNodes_UpdateSameKey tests both nodes updating the same key
// and verifies both caches converge to the same final value via sync mechanism.
func TestCachedMapBroker_TwoNodes_UpdateSameKey(t *testing.T) {
	_, cached1, _, cached2, cleanup := setupTwoNodeCachedRedis(t)
	defer cleanup()

	ctx := context.Background()
	channel := randomChannel("two_nodes_same_key")

	// Load channel on both nodes
	_, err := cached1.ReadState(ctx, channel, MapReadStateOptions{Cached: true, Limit: 100})
	require.NoError(t, err)
	_, err = cached2.ReadState(ctx, channel, MapReadStateOptions{Cached: true, Limit: 100})
	require.NoError(t, err)

	// Both nodes update the same key multiple times
	for i := 0; i < 5; i++ {
		_, err = cached1.Publish(ctx, channel, "shared_key", MapPublishOptions{
			Data: []byte(fmt.Sprintf("node1_update%d", i)),
		})
		require.NoError(t, err)

		_, err = cached2.Publish(ctx, channel, "shared_key", MapPublishOptions{
			Data: []byte(fmt.Sprintf("node2_update%d", i)),
		})
		require.NoError(t, err)
	}

	// Wait for sync mechanism to propagate
	time.Sleep(500 * time.Millisecond)

	// Both nodes should have exactly 1 key with the same final value
	stateRes, err := cached1.ReadState(ctx, channel, MapReadStateOptions{Cached: true, Limit: 100})
	pubs1, pos1, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
	require.NoError(t, err)
	require.Len(t, pubs1, 1)

	stateRes, err = cached2.ReadState(ctx, channel, MapReadStateOptions{Cached: true, Limit: 100})
	pubs2, pos2, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
	require.NoError(t, err)
	require.Len(t, pubs2, 1)

	// Both should have the same final value (last write wins)
	require.Equal(t, pubs1[0].Data, pubs2[0].Data, "both nodes should have same final value")
	require.Equal(t, pos1.Epoch, pos2.Epoch)
	require.Equal(t, pos1.Offset, pos2.Offset)

	// Stream should have all 10 updates
	streamResult1, err := cached1.ReadStream(ctx, channel, MapReadStreamOptions{
		Filter: StreamFilter{Limit: -1},
	})
	require.NoError(t, err)
	require.Len(t, streamResult1.Publications, 10, "stream should have all 10 updates")

	streamResult2, err := cached2.ReadStream(ctx, channel, MapReadStreamOptions{
		Filter: StreamFilter{Limit: -1},
	})
	require.NoError(t, err)
	require.Len(t, streamResult2.Publications, 10, "stream should have all 10 updates")
}

// TestCachedMapBroker_TwoNodes_GapFilling tests that when a node loads a channel
// after another node has published, it gets all data from the backend.
func TestCachedMapBroker_TwoNodes_GapFilling(t *testing.T) {
	_, cached1, _, cached2, cleanup := setupTwoNodeCachedRedis(t)
	defer cleanup()

	ctx := context.Background()
	channel := randomChannel("two_nodes_gap")

	// Only node 1 loads the channel initially
	_, err := cached1.ReadState(ctx, channel, MapReadStateOptions{Cached: true, Limit: 100})
	require.NoError(t, err)

	// Node 1 publishes several keys
	for i := 0; i < 5; i++ {
		_, err = cached1.Publish(ctx, channel, fmt.Sprintf("key%d", i), MapPublishOptions{
			Data: []byte(fmt.Sprintf("data%d", i)),
		})
		require.NoError(t, err)
	}

	// Node 2 now loads the channel - should get all data from backend
	stateRes, err := cached2.ReadState(ctx, channel, MapReadStateOptions{Cached: true, Limit: 100})
	pubs2, _, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
	require.NoError(t, err)
	require.Len(t, pubs2, 5, "node2 should load all 5 keys from backend")

	// Node 1 publishes more
	for i := 5; i < 10; i++ {
		_, err = cached1.Publish(ctx, channel, fmt.Sprintf("key%d", i), MapPublishOptions{
			Data: []byte(fmt.Sprintf("data%d", i)),
		})
		require.NoError(t, err)
	}

	// Wait for sync mechanism
	time.Sleep(500 * time.Millisecond)

	// Both should have all 10 keys
	stateRes, err = cached1.ReadState(ctx, channel, MapReadStateOptions{Cached: true, Limit: 100})
	pubs1, pos1, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
	require.NoError(t, err)
	require.Len(t, pubs1, 10)

	stateRes, err = cached2.ReadState(ctx, channel, MapReadStateOptions{Cached: true, Limit: 100})
	pubs2, pos2, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
	require.NoError(t, err)
	require.Len(t, pubs2, 10, "node2 should have all 10 keys via sync")

	require.Equal(t, pos1.Offset, pos2.Offset)
}

// TestCachedMapBroker_TwoNodes_Remove tests that unpublish (removal) propagates
// correctly to both nodes via sync mechanism.
func TestCachedMapBroker_TwoNodes_Remove(t *testing.T) {
	_, cached1, _, cached2, cleanup := setupTwoNodeCachedRedis(t)
	defer cleanup()

	ctx := context.Background()
	channel := randomChannel("two_nodes_unpublish")

	// Load channel on both nodes
	_, err := cached1.ReadState(ctx, channel, MapReadStateOptions{Cached: true, Limit: 100})
	require.NoError(t, err)
	_, err = cached2.ReadState(ctx, channel, MapReadStateOptions{Cached: true, Limit: 100})
	require.NoError(t, err)

	// Node 1 publishes some keys
	for i := 0; i < 5; i++ {
		_, err = cached1.Publish(ctx, channel, fmt.Sprintf("key%d", i), MapPublishOptions{
			Data: []byte(fmt.Sprintf("data%d", i)),
		})
		require.NoError(t, err)
	}

	// Wait for sync
	time.Sleep(500 * time.Millisecond)

	// Verify both have 5 keys
	stateRes, _ := cached1.ReadState(ctx, channel, MapReadStateOptions{Cached: true, Limit: 100})
	pubs1 := stateRes.Publications
	require.Len(t, pubs1, 5)
	stateRes, _ = cached2.ReadState(ctx, channel, MapReadStateOptions{Cached: true, Limit: 100})
	pubs2 := stateRes.Publications
	require.Len(t, pubs2, 5)

	// Node 2 removes some keys
	for i := 0; i < 3; i++ {
		_, err = cached2.Remove(ctx, channel, fmt.Sprintf("key%d", i), MapRemoveOptions{})
		require.NoError(t, err)
	}

	// Wait for sync
	time.Sleep(500 * time.Millisecond)

	// Both should have 2 keys remaining
	stateRes, err = cached1.ReadState(ctx, channel, MapReadStateOptions{Cached: true, Limit: 100})
	pubs1, pos1, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
	require.NoError(t, err)
	require.Len(t, pubs1, 2, "node1 should see removals from node2")

	stateRes, err = cached2.ReadState(ctx, channel, MapReadStateOptions{Cached: true, Limit: 100})
	pubs2, pos2, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
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

// TestCachedMapBroker_TwoNodes_HighConcurrency stress tests with high concurrency
// from both nodes simultaneously, verifying eventual consistency via sync mechanism.
func TestCachedMapBroker_TwoNodes_HighConcurrency(t *testing.T) {
	_, cached1, _, cached2, cleanup := setupTwoNodeCachedRedis(t)
	defer cleanup()

	ctx := context.Background()
	channel := randomChannel("two_nodes_stress")

	// Load channel on both nodes
	_, err := cached1.ReadState(ctx, channel, MapReadStateOptions{Cached: true, Limit: 1000})
	require.NoError(t, err)
	_, err = cached2.ReadState(ctx, channel, MapReadStateOptions{Cached: true, Limit: 1000})
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
				_, err := cached1.Publish(ctx, channel, fmt.Sprintf("n1_g%d_k%d", gid, i), MapPublishOptions{
					Data: []byte(fmt.Sprintf("n1_g%d_d%d", gid, i)),
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
				_, err := cached2.Publish(ctx, channel, fmt.Sprintf("n2_g%d_k%d", gid, i), MapPublishOptions{
					Data: []byte(fmt.Sprintf("n2_g%d_d%d", gid, i)),
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

	stateRes, err := cached1.ReadState(ctx, channel, MapReadStateOptions{Cached: true, Limit: 1000})
	pubs1, pos1, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
	require.NoError(t, err)

	stateRes, err = cached2.ReadState(ctx, channel, MapReadStateOptions{Cached: true, Limit: 1000})
	pubs2, pos2, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
	require.NoError(t, err)

	require.Len(t, pubs1, expectedKeys, "node1 should have all %d keys", expectedKeys)
	require.Len(t, pubs2, expectedKeys, "node2 should have all %d keys", expectedKeys)

	require.Equal(t, pos1.Epoch, pos2.Epoch, "epochs should match")
	require.Equal(t, pos1.Offset, pos2.Offset, "offsets should match")
}

// setupTwoNodeCachedRedisWithPubSub creates two CachedMapBroker instances with long sync interval
// to ensure tests rely on PUB/SUB propagation, not periodic sync.
func setupTwoNodeCachedRedisWithPubSub(t *testing.T) (node1 *Node, cached1 *CachedMapBroker, node2 *Node, cached2 *CachedMapBroker, cleanup func()) {
	// Node 1
	node1, _ = New(Config{
		GetMapChannelOptions: func(channel string) MapChannelOptions {
			return MapChannelOptions{
				SyncMode:      MapSyncConverging,
				RetentionMode: MapRetentionExpiring,
				KeyTTL:        60 * time.Second,
			}
		},
	})
	redisConf := testSingleRedisConf(6379)
	shard1, err := NewRedisShard(node1, redisConf)
	require.NoError(t, err)
	backend1, err := NewRedisMapBroker(node1, RedisMapBrokerConfig{
		Shards: []*RedisShard{shard1},
	})
	require.NoError(t, err)

	cached1, err = NewCachedMapBroker(node1, backend1, CachedMapBrokerConfig{
		Cache: MapCacheConfig{
			MaxChannels:        1000,
			ChannelIdleTimeout: 5 * time.Minute,
			StreamSize:         1000,
		},
		SyncInterval: 1 * time.Hour, // Very long - we want to test PUB/SUB, not sync
	})
	require.NoError(t, err)
	err = cached1.RegisterEventHandler(nil)
	require.NoError(t, err)

	// Node 2 - separate node, same Redis
	node2, _ = New(Config{
		GetMapChannelOptions: func(channel string) MapChannelOptions {
			return MapChannelOptions{
				SyncMode:      MapSyncConverging,
				RetentionMode: MapRetentionExpiring,
				KeyTTL:        60 * time.Second,
			}
		},
	})
	shard2, err := NewRedisShard(node2, redisConf)
	require.NoError(t, err)
	backend2, err := NewRedisMapBroker(node2, RedisMapBrokerConfig{
		Shards: []*RedisShard{shard2},
	})
	require.NoError(t, err)

	cached2, err = NewCachedMapBroker(node2, backend2, CachedMapBrokerConfig{
		Cache: MapCacheConfig{
			MaxChannels:        1000,
			ChannelIdleTimeout: 5 * time.Minute,
			StreamSize:         1000,
		},
		SyncInterval: 1 * time.Hour, // Very long - we want to test PUB/SUB, not sync
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

// TestCachedMapBroker_TwoNodes_PubSub tests real PUB/SUB propagation between nodes.
// Uses Subscribe to set up PUB/SUB channels and verifies real-time updates.
func TestCachedMapBroker_TwoNodes_PubSub(t *testing.T) {
	_, cached1, _, cached2, cleanup := setupTwoNodeCachedRedisWithPubSub(t)
	defer cleanup()

	ctx := context.Background()
	channel := randomChannel("two_nodes_pubsub")

	// Subscribe on both nodes - this sets up PUB/SUB and preloads cache
	err := cached1.Subscribe(channel)
	require.NoError(t, err)
	err = cached2.Subscribe(channel)
	require.NoError(t, err)

	// Small delay to ensure PUB/SUB subscriptions are established
	time.Sleep(100 * time.Millisecond)

	// Node 1 publishes
	_, err = cached1.Publish(ctx, channel, "key1", MapPublishOptions{
		Data: []byte("from_node1"),
	})
	require.NoError(t, err)

	// Short wait for PUB/SUB propagation (should be near-instant)
	time.Sleep(100 * time.Millisecond)

	// Both nodes should see the publication via PUB/SUB
	stateRes, err := cached1.ReadState(ctx, channel, MapReadStateOptions{Cached: true, Limit: 100})
	pubs1, pos1, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
	require.NoError(t, err)
	require.Len(t, pubs1, 1)

	stateRes, err = cached2.ReadState(ctx, channel, MapReadStateOptions{Cached: true, Limit: 100})
	pubs2, pos2, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
	require.NoError(t, err)
	require.Len(t, pubs2, 1, "node2 should see publication from node1 via PUB/SUB")

	require.Equal(t, pos1.Epoch, pos2.Epoch)
	require.Equal(t, pos1.Offset, pos2.Offset)

	// Cleanup subscriptions
	_ = cached1.Unsubscribe(channel)
	_ = cached2.Unsubscribe(channel)
}

// TestCachedMapBroker_TwoNodes_UpdateSameKeyPubSub tests both nodes updating the same key
// and verifies both caches converge to the same final value via real PUB/SUB.
func TestCachedMapBroker_TwoNodes_UpdateSameKeyPubSub(t *testing.T) {
	_, cached1, _, cached2, cleanup := setupTwoNodeCachedRedisWithPubSub(t)
	defer cleanup()

	ctx := context.Background()
	channel := randomChannel("two_nodes_same_key_pubsub")

	// Subscribe on both nodes - sets up PUB/SUB and preloads cache
	err := cached1.Subscribe(channel)
	require.NoError(t, err)
	err = cached2.Subscribe(channel)
	require.NoError(t, err)

	// Wait for PUB/SUB subscriptions to be established
	time.Sleep(100 * time.Millisecond)

	// Both nodes update the same key multiple times
	for i := 0; i < 5; i++ {
		_, err = cached1.Publish(ctx, channel, "shared_key", MapPublishOptions{
			Data: []byte(fmt.Sprintf("node1_update%d", i)),
		})
		require.NoError(t, err)

		_, err = cached2.Publish(ctx, channel, "shared_key", MapPublishOptions{
			Data: []byte(fmt.Sprintf("node2_update%d", i)),
		})
		require.NoError(t, err)
	}

	// Wait for PUB/SUB propagation
	time.Sleep(200 * time.Millisecond)

	// Both nodes should have exactly 1 key with the same final value
	stateRes, err := cached1.ReadState(ctx, channel, MapReadStateOptions{Cached: true, Limit: 100})
	pubs1, pos1, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
	require.NoError(t, err)
	require.Len(t, pubs1, 1)

	stateRes, err = cached2.ReadState(ctx, channel, MapReadStateOptions{Cached: true, Limit: 100})
	pubs2, pos2, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
	require.NoError(t, err)
	require.Len(t, pubs2, 1)

	// Both should have the same final value (last write wins)
	require.Equal(t, pubs1[0].Data, pubs2[0].Data, "both nodes should have same final value via PUB/SUB")
	require.Equal(t, pos1.Epoch, pos2.Epoch)
	require.Equal(t, pos1.Offset, pos2.Offset)

	// Stream should have all 10 updates
	streamResult1, err := cached1.ReadStream(ctx, channel, MapReadStreamOptions{
		Filter: StreamFilter{Limit: -1},
	})
	require.NoError(t, err)
	require.Len(t, streamResult1.Publications, 10, "stream should have all 10 updates")

	streamResult2, err := cached2.ReadStream(ctx, channel, MapReadStreamOptions{
		Filter: StreamFilter{Limit: -1},
	})
	require.NoError(t, err)
	require.Len(t, streamResult2.Publications, 10, "stream should have all 10 updates")

	// Cleanup subscriptions
	_ = cached1.Unsubscribe(channel)
	_ = cached2.Unsubscribe(channel)
}

// TestCachedMapBroker_TwoNodes_GapFillingPubSub tests that when node2 subscribes after
// node1 has published, it gets all data and then receives real-time updates via PUB/SUB.
func TestCachedMapBroker_TwoNodes_GapFillingPubSub(t *testing.T) {
	_, cached1, _, cached2, cleanup := setupTwoNodeCachedRedisWithPubSub(t)
	defer cleanup()

	ctx := context.Background()
	channel := randomChannel("two_nodes_gap_pubsub")

	// Only node 1 subscribes initially
	err := cached1.Subscribe(channel)
	require.NoError(t, err)

	// Node 1 publishes several keys
	for i := 0; i < 5; i++ {
		_, err = cached1.Publish(ctx, channel, fmt.Sprintf("key%d", i), MapPublishOptions{
			Data: []byte(fmt.Sprintf("data%d", i)),
		})
		require.NoError(t, err)
	}

	// Node 2 now subscribes - should get all data from backend during Subscribe
	err = cached2.Subscribe(channel)
	require.NoError(t, err)

	// Verify node2 loaded all 5 keys
	stateRes, err := cached2.ReadState(ctx, channel, MapReadStateOptions{Cached: true, Limit: 100})
	pubs2, _, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
	require.NoError(t, err)
	require.Len(t, pubs2, 5, "node2 should load all 5 keys during Subscribe")

	// Node 1 publishes more - these should propagate via PUB/SUB
	for i := 5; i < 10; i++ {
		_, err = cached1.Publish(ctx, channel, fmt.Sprintf("key%d", i), MapPublishOptions{
			Data: []byte(fmt.Sprintf("data%d", i)),
		})
		require.NoError(t, err)
	}

	// Wait for PUB/SUB propagation
	time.Sleep(200 * time.Millisecond)

	// Both should have all 10 keys
	stateRes, err = cached1.ReadState(ctx, channel, MapReadStateOptions{Cached: true, Limit: 100})
	pubs1, pos1, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
	require.NoError(t, err)
	require.Len(t, pubs1, 10)

	stateRes, err = cached2.ReadState(ctx, channel, MapReadStateOptions{Cached: true, Limit: 100})
	pubs2, pos2, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
	require.NoError(t, err)
	require.Len(t, pubs2, 10, "node2 should have all 10 keys via PUB/SUB")

	require.Equal(t, pos1.Offset, pos2.Offset)

	// Cleanup subscriptions
	_ = cached1.Unsubscribe(channel)
	_ = cached2.Unsubscribe(channel)
}

// TestCachedMapBroker_TwoNodes_RemovePubSub tests that unpublish (removal) propagates
// correctly to both nodes via real PUB/SUB (not periodic sync).
func TestCachedMapBroker_TwoNodes_RemovePubSub(t *testing.T) {
	_, cached1, _, cached2, cleanup := setupTwoNodeCachedRedisWithPubSub(t)
	defer cleanup()

	ctx := context.Background()
	channel := randomChannel("two_nodes_unpublish_pubsub")

	// Subscribe on both nodes - sets up PUB/SUB and preloads cache
	err := cached1.Subscribe(channel)
	require.NoError(t, err)
	err = cached2.Subscribe(channel)
	require.NoError(t, err)

	// Wait for PUB/SUB subscriptions to be established
	time.Sleep(100 * time.Millisecond)

	// Node 1 publishes some keys
	for i := 0; i < 5; i++ {
		_, err = cached1.Publish(ctx, channel, fmt.Sprintf("key%d", i), MapPublishOptions{
			Data: []byte(fmt.Sprintf("data%d", i)),
		})
		require.NoError(t, err)
	}

	// Wait for PUB/SUB propagation
	time.Sleep(200 * time.Millisecond)

	// Verify both have 5 keys
	stateRes, _ := cached1.ReadState(ctx, channel, MapReadStateOptions{Cached: true, Limit: 100})
	pubs1 := stateRes.Publications
	require.Len(t, pubs1, 5)
	stateRes, _ = cached2.ReadState(ctx, channel, MapReadStateOptions{Cached: true, Limit: 100})
	pubs2 := stateRes.Publications
	require.Len(t, pubs2, 5)

	// Node 2 removes some keys
	for i := 0; i < 3; i++ {
		_, err = cached2.Remove(ctx, channel, fmt.Sprintf("key%d", i), MapRemoveOptions{})
		require.NoError(t, err)
	}

	// Wait for PUB/SUB propagation
	time.Sleep(200 * time.Millisecond)

	// Both should have 2 keys remaining
	stateRes, err = cached1.ReadState(ctx, channel, MapReadStateOptions{Cached: true, Limit: 100})
	pubs1, pos1, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
	require.NoError(t, err)
	require.Len(t, pubs1, 2, "node1 should see removals from node2 via PUB/SUB")

	stateRes, err = cached2.ReadState(ctx, channel, MapReadStateOptions{Cached: true, Limit: 100})
	pubs2, pos2, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
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

	// Cleanup subscriptions
	_ = cached1.Unsubscribe(channel)
	_ = cached2.Unsubscribe(channel)
}

// TestCachedMapBroker_TwoNodes_ConcurrentPublishPubSub tests concurrent publications
// from both nodes using real PUB/SUB propagation (not periodic sync).
func TestCachedMapBroker_TwoNodes_ConcurrentPublishPubSub(t *testing.T) {
	_, cached1, _, cached2, cleanup := setupTwoNodeCachedRedisWithPubSub(t)
	defer cleanup()

	ctx := context.Background()
	channel := randomChannel("two_nodes_concurrent_pubsub")

	// Subscribe on both nodes - sets up PUB/SUB and preloads cache
	err := cached1.Subscribe(channel)
	require.NoError(t, err)
	err = cached2.Subscribe(channel)
	require.NoError(t, err)

	// Wait for PUB/SUB subscriptions to be established
	time.Sleep(100 * time.Millisecond)

	// Concurrent publications from both nodes
	var wg sync.WaitGroup
	numPubsPerNode := 10

	// Node 1 publishes keys 0-9
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < numPubsPerNode; i++ {
			_, err := cached1.Publish(ctx, channel, fmt.Sprintf("node1_key%d", i), MapPublishOptions{
				Data: []byte(fmt.Sprintf("node1_data%d", i)),
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
			_, err := cached2.Publish(ctx, channel, fmt.Sprintf("node2_key%d", i), MapPublishOptions{
				Data: []byte(fmt.Sprintf("node2_data%d", i)),
			})
			if err != nil {
				t.Errorf("node2 publish error: %v", err)
			}
		}
	}()

	wg.Wait()

	// Wait for PUB/SUB propagation (should be fast)
	time.Sleep(200 * time.Millisecond)

	// Both nodes should see all 20 keys
	stateRes, err := cached1.ReadState(ctx, channel, MapReadStateOptions{Cached: true, Limit: 100})
	pubs1, pos1, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
	require.NoError(t, err)

	stateRes, err = cached2.ReadState(ctx, channel, MapReadStateOptions{Cached: true, Limit: 100})
	pubs2, pos2, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
	require.NoError(t, err)

	// Should have all keys from both nodes
	require.Len(t, pubs1, numPubsPerNode*2, "node1 should see all %d keys via PUB/SUB", numPubsPerNode*2)
	require.Len(t, pubs2, numPubsPerNode*2, "node2 should see all %d keys via PUB/SUB", numPubsPerNode*2)

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

	// Cleanup subscriptions
	_ = cached1.Unsubscribe(channel)
	_ = cached2.Unsubscribe(channel)
}

// TestCachedMapBroker_TwoNodes_MissedPubSubMessages tests that when a node misses
// PUB/SUB messages, it detects the gap and fills it from the backend.
// Simulates missed messages by writing directly to backend (SkipPubSub) then
// sending a later message via PUB/SUB to trigger gap detection.
func TestCachedMapBroker_TwoNodes_MissedPubSubMessages(t *testing.T) {
	// Node 1 setup - regular node with PUB/SUB
	node1, _ := New(Config{
		GetMapChannelOptions: func(channel string) MapChannelOptions {
			return MapChannelOptions{
				SyncMode:      MapSyncConverging,
				RetentionMode: MapRetentionExpiring,
				KeyTTL:        60 * time.Second,
			}
		},
	})
	redisConf := testSingleRedisConf(6379)
	shard1, err := NewRedisShard(node1, redisConf)
	require.NoError(t, err)
	backend1, err := NewRedisMapBroker(node1, RedisMapBrokerConfig{
		Shards: []*RedisShard{shard1},
	})
	require.NoError(t, err)

	cached1, err := NewCachedMapBroker(node1, backend1, CachedMapBrokerConfig{
		Cache: MapCacheConfig{
			MaxChannels:        1000,
			ChannelIdleTimeout: 5 * time.Minute,
			StreamSize:         1000,
		},
		SyncInterval: 1 * time.Hour, // Disable sync - test gap filling via HandlePublication
	})
	require.NoError(t, err)
	err = cached1.RegisterEventHandler(nil)
	require.NoError(t, err)

	// Node 2 setup
	node2, _ := New(Config{
		GetMapChannelOptions: func(channel string) MapChannelOptions {
			return MapChannelOptions{
				SyncMode:      MapSyncConverging,
				RetentionMode: MapRetentionExpiring,
				KeyTTL:        60 * time.Second,
			}
		},
	})
	shard2, err := NewRedisShard(node2, redisConf)
	require.NoError(t, err)
	backend2, err := NewRedisMapBroker(node2, RedisMapBrokerConfig{
		Shards: []*RedisShard{shard2},
	})
	require.NoError(t, err)

	cached2, err := NewCachedMapBroker(node2, backend2, CachedMapBrokerConfig{
		Cache: MapCacheConfig{
			MaxChannels:        1000,
			ChannelIdleTimeout: 5 * time.Minute,
			StreamSize:         1000,
		},
		SyncInterval: 1 * time.Hour,
	})
	require.NoError(t, err)
	err = cached2.RegisterEventHandler(nil)
	require.NoError(t, err)

	// "Silent" backend - writes to same Redis but with SkipPubSub
	// This simulates messages that node2 would miss via PUB/SUB
	node3, _ := New(Config{
		GetMapChannelOptions: func(channel string) MapChannelOptions {
			return MapChannelOptions{
				SyncMode:      MapSyncConverging,
				RetentionMode: MapRetentionExpiring,
				KeyTTL:        60 * time.Second,
			}
		},
	})
	shard3, err := NewRedisShard(node3, redisConf)
	require.NoError(t, err)
	silentBackend, err := NewRedisMapBroker(node3, RedisMapBrokerConfig{
		Shards:     []*RedisShard{shard3},
		SkipPubSub: true, // No PUB/SUB - writes go to Redis but no notifications
	})
	require.NoError(t, err)

	defer func() {
		_ = cached1.Close(context.Background())
		_ = cached2.Close(context.Background())
		_ = node1.Shutdown(context.Background())
		_ = node2.Shutdown(context.Background())
		_ = node3.Shutdown(context.Background())
	}()

	ctx := context.Background()
	channel := randomChannel("two_nodes_missed_pubsub")

	// Both nodes subscribe
	err = cached1.Subscribe(channel)
	require.NoError(t, err)
	err = cached2.Subscribe(channel)
	require.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	// Node 1 publishes message 1 - both nodes receive via PUB/SUB
	_, err = cached1.Publish(ctx, channel, "key1", MapPublishOptions{
		Data: []byte("data1"),
	})
	require.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	// Verify both have key1
	stateRes, _ := cached1.ReadState(ctx, channel, MapReadStateOptions{Cached: true, Limit: 100})
	pubs1 := stateRes.Publications
	require.Len(t, pubs1, 1)
	stateRes, _ = cached2.ReadState(ctx, channel, MapReadStateOptions{Cached: true, Limit: 100})
	pubs2, pos2 := stateRes.Publications, stateRes.Position
	require.Len(t, pubs2, 1)
	require.Equal(t, uint64(1), pos2.Offset)

	// Write messages 2, 3, 4 directly to backend WITHOUT PUB/SUB
	// This simulates messages that node2 misses
	for i := 2; i <= 4; i++ {
		_, err = silentBackend.Publish(ctx, channel, fmt.Sprintf("key%d", i), MapPublishOptions{
			Data: []byte(fmt.Sprintf("data%d", i)),
		})
		require.NoError(t, err)
	}

	// Node2's cache still has only key1 (missed 2,3,4 - no PUB/SUB)
	stateRes, _ = cached2.ReadState(ctx, channel, MapReadStateOptions{Cached: true, Limit: 100})
	pubs2, pos2 = stateRes.Publications, stateRes.Position
	require.Len(t, pubs2, 1, "node2 should still have only 1 key (missed PUB/SUB)")
	require.Equal(t, uint64(1), pos2.Offset)

	// Node 1 publishes message 5 via PUB/SUB
	// Node2 receives this, detects gap (cache at 1, received 5), fills from backend
	_, err = cached1.Publish(ctx, channel, "key5", MapPublishOptions{
		Data: []byte("data5"),
	})
	require.NoError(t, err)

	// Wait for PUB/SUB + gap filling
	time.Sleep(200 * time.Millisecond)

	// Node2 should now have all 5 keys (gap filled from backend)
	stateRes, err = cached2.ReadState(ctx, channel, MapReadStateOptions{Cached: true, Limit: 100})
	pubs2, pos2, _ = stateRes.Publications, stateRes.Position, stateRes.Cursor
	require.NoError(t, err)
	require.Len(t, pubs2, 5, "node2 should have all 5 keys after gap filling")
	require.Equal(t, uint64(5), pos2.Offset)

	// Verify all keys present
	keys := make(map[string]bool)
	for _, pub := range pubs2 {
		keys[pub.Key] = true
	}
	for i := 1; i <= 5; i++ {
		require.True(t, keys[fmt.Sprintf("key%d", i)], "node2 missing key%d after gap fill", i)
	}

	// Cleanup
	_ = cached1.Unsubscribe(channel)
	_ = cached2.Unsubscribe(channel)
}

// TestCachedMapBroker_TwoNodes_StreamOrderConsistency verifies that the stream
// order is identical on both nodes after publications from both sides.
func TestCachedMapBroker_TwoNodes_StreamOrderConsistency(t *testing.T) {
	_, cached1, _, cached2, cleanup := setupTwoNodeCachedRedisWithPubSub(t)
	defer cleanup()

	ctx := context.Background()
	channel := randomChannel("two_nodes_stream_order")

	// Subscribe on both nodes
	err := cached1.Subscribe(channel)
	require.NoError(t, err)
	err = cached2.Subscribe(channel)
	require.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	// Interleaved publications from both nodes
	for i := 0; i < 10; i++ {
		_, err = cached1.Publish(ctx, channel, fmt.Sprintf("n1_key%d", i), MapPublishOptions{
			Data: []byte(fmt.Sprintf("n1_data%d", i)),
		})
		require.NoError(t, err)

		_, err = cached2.Publish(ctx, channel, fmt.Sprintf("n2_key%d", i), MapPublishOptions{
			Data: []byte(fmt.Sprintf("n2_data%d", i)),
		})
		require.NoError(t, err)
	}

	// Wait for PUB/SUB propagation
	time.Sleep(300 * time.Millisecond)

	// Read streams from both nodes
	streamResult1, err := cached1.ReadStream(ctx, channel, MapReadStreamOptions{
		Filter: StreamFilter{Limit: -1},
	})
	require.NoError(t, err)

	streamResult2, err := cached2.ReadStream(ctx, channel, MapReadStreamOptions{
		Filter: StreamFilter{Limit: -1},
	})
	require.NoError(t, err)

	stream1 := streamResult1.Publications
	stream2 := streamResult2.Publications
	pos1 := streamResult1.Position
	pos2 := streamResult2.Position

	// Both should have 20 entries
	require.Len(t, stream1, 20, "node1 stream should have 20 entries")
	require.Len(t, stream2, 20, "node2 stream should have 20 entries")

	// Positions should match
	require.Equal(t, pos1.Epoch, pos2.Epoch, "epochs should match")
	require.Equal(t, pos1.Offset, pos2.Offset, "offsets should match")

	// Stream order should be identical (same offsets in same order)
	for i := 0; i < len(stream1); i++ {
		require.Equal(t, stream1[i].Offset, stream2[i].Offset,
			"stream offset mismatch at index %d: node1=%d, node2=%d", i, stream1[i].Offset, stream2[i].Offset)
		require.Equal(t, stream1[i].Key, stream2[i].Key,
			"stream key mismatch at index %d: node1=%s, node2=%s", i, stream1[i].Key, stream2[i].Key)
		require.Equal(t, stream1[i].Data, stream2[i].Data,
			"stream data mismatch at index %d", i)
	}

	// Verify offsets are sequential
	for i := 1; i < len(stream1); i++ {
		require.Equal(t, stream1[i-1].Offset+1, stream1[i].Offset,
			"stream offsets should be sequential: got %d -> %d", stream1[i-1].Offset, stream1[i].Offset)
	}

	// Cleanup
	_ = cached1.Unsubscribe(channel)
	_ = cached2.Unsubscribe(channel)
}

// TestCachedMapBroker_TwoNodes_HighConcurrencyPubSub stress tests with high concurrency
// using real PUB/SUB propagation (not periodic sync).
func TestCachedMapBroker_TwoNodes_HighConcurrencyPubSub(t *testing.T) {
	_, cached1, _, cached2, cleanup := setupTwoNodeCachedRedisWithPubSub(t)
	defer cleanup()

	ctx := context.Background()
	channel := randomChannel("two_nodes_pubsub_stress")

	// Subscribe on both nodes - sets up PUB/SUB and preloads cache
	err := cached1.Subscribe(channel)
	require.NoError(t, err)
	err = cached2.Subscribe(channel)
	require.NoError(t, err)

	// Wait for PUB/SUB subscriptions to be fully established
	time.Sleep(200 * time.Millisecond)

	var wg sync.WaitGroup
	numGoroutines := 10
	pubsPerGoroutine := 20

	// Node 1 goroutines
	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < pubsPerGoroutine; i++ {
				_, err := cached1.Publish(ctx, channel, fmt.Sprintf("n1_g%d_k%d", gid, i), MapPublishOptions{
					Data: []byte(fmt.Sprintf("n1_g%d_d%d", gid, i)),
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
				_, err := cached2.Publish(ctx, channel, fmt.Sprintf("n2_g%d_k%d", gid, i), MapPublishOptions{
					Data: []byte(fmt.Sprintf("n2_g%d_d%d", gid, i)),
				})
				if err != nil {
					t.Errorf("n2 g%d publish error: %v", gid, err)
				}
			}
		}(g)
	}

	wg.Wait()

	// Wait for PUB/SUB propagation - should be fast since it's real-time
	time.Sleep(500 * time.Millisecond)

	expectedKeys := numGoroutines * pubsPerGoroutine * 2 // 10 * 20 * 2 = 400

	stateRes, err := cached1.ReadState(ctx, channel, MapReadStateOptions{Cached: true, Limit: 1000})
	pubs1, pos1, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
	require.NoError(t, err)

	stateRes, err = cached2.ReadState(ctx, channel, MapReadStateOptions{Cached: true, Limit: 1000})
	pubs2, pos2, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
	require.NoError(t, err)

	require.Len(t, pubs1, expectedKeys, "node1 should have all %d keys via PUB/SUB", expectedKeys)
	require.Len(t, pubs2, expectedKeys, "node2 should have all %d keys via PUB/SUB", expectedKeys)

	require.Equal(t, pos1.Epoch, pos2.Epoch, "epochs should match")
	require.Equal(t, pos1.Offset, pos2.Offset, "offsets should match")

	// Cleanup subscriptions
	_ = cached1.Unsubscribe(channel)
	_ = cached2.Unsubscribe(channel)
}
