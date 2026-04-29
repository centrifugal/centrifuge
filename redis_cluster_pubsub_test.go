//go:build integration

package centrifuge

import (
	"context"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/rueidis"
	"github.com/stretchr/testify/require"
)

func newNodeGroupedMapBroker(t *testing.T, n *Node, numPartitions int) *RedisMapBroker {
	return newNodeGroupedMapBrokerPrefix(t, n, numPartitions, getUniquePrefix())
}

func newNodeGroupedMapBrokerPrefix(t *testing.T, n *Node, numPartitions int, prefix string) *RedisMapBroker {
	t.Helper()
	redisConf := RedisShardConfig{
		ClusterAddresses: []string{"localhost:7001", "localhost:7002", "localhost:7003"},
		IOTimeout:        10 * time.Second,
		ConnectTimeout:   10 * time.Second,
	}
	shard, err := NewRedisShard(n, redisConf)
	require.NoError(t, err)

	e, err := NewRedisMapBroker(n, RedisMapBrokerConfig{
		Shards:                     []*RedisShard{shard},
		Prefix:                     prefix,
		NumShardedPubSubPartitions: numPartitions,
		GroupShardedPubSubByNode:   true,
	})
	require.NoError(t, err)

	// Verify sharded PUB/SUB is supported.
	result := shard.client.Do(context.Background(), shard.client.B().Spublish().Channel(prefix+"._").Message("").Build())
	if result.Error() != nil && strings.Contains(result.Error().Error(), "unknown command") {
		t.Skip("sharded PUB/SUB not supported by this Redis version, skipping test")
	}

	return e
}

// waitForSubClientsPopulated polls until every slot in *slotsPtr is non-nil.
// This is the deterministic post-rebuild barrier: rather than sleeping a
// fixed duration and hoping goroutines have finished their dedicate +
// resubscribe cycle, callers wait until the goroutines have actually written
// their connections back into the array. Tests that exercise rebuild paths
// should use this instead of time.Sleep before invoking Subscribe / Publish.
//
// slotsPtr is a pointer to the wrapper's subClients field so the helper sees
// post-rebuild reallocations (the slice header may be replaced by rebuild).
func waitForSubClientsPopulated(t *testing.T, mu *sync.Mutex, slotsPtr *[][]rueidis.DedicatedClient, timeout time.Duration) {
	t.Helper()
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		slots := *slotsPtr
		if len(slots) == 0 {
			return false
		}
		for i := range slots {
			for j := range slots[i] {
				if slots[i][j] == nil {
					return false
				}
			}
		}
		return true
	}, timeout, 100*time.Millisecond, "all subClients slots should be populated")
}

// TestRedisMapBroker_NodeGrouped_ConnectionCount verifies that the number of
// first-dimension subClients entries equals the number of Redis Cluster nodes,
// not the number of partitions.
func TestRedisMapBroker_NodeGrouped_ConnectionCount(t *testing.T) {
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
	numPartitions := 32
	e := newNodeGroupedMapBroker(t, node, numPartitions)
	defer func() { _ = e.Close(context.Background()) }()
	defer func() { _ = node.Shutdown(context.Background()) }()

	wrapper := e.shards[0]

	// With GroupShardedPubSubByNode, subClients should be indexed by node, not partition.
	numNodes := len(wrapper.nodeClients)
	require.Greater(t, numNodes, 0, "should have at least one node")
	require.Equal(t, numNodes, len(wrapper.subClients), "subClients first dimension should equal node count")
	require.Less(t, numNodes, numPartitions, "node count should be less than partition count")

	t.Logf("partitions=%d nodes=%d (%.0f%% reduction in connections)",
		numPartitions, numNodes, float64(numPartitions-numNodes)/float64(numPartitions)*100)

	// Verify partitionToNodeIdx covers all partitions.
	require.Equal(t, numPartitions, len(wrapper.partitionToNodeIdx))

	// Verify every partition maps to a valid node.
	for i, nodeIdx := range wrapper.partitionToNodeIdx {
		require.GreaterOrEqual(t, nodeIdx, 0, "partition %d has invalid nodeIdx", i)
		require.Less(t, nodeIdx, numNodes, "partition %d has out-of-range nodeIdx", i)
	}

	// Verify all nodes have at least one partition.
	for nodeIdx, parts := range wrapper.nodePartitions {
		require.Greater(t, len(parts), 0, "node %d has no partitions assigned", nodeIdx)
	}
}

// TestRedisMapBroker_NodeGrouped_Basic tests basic Publish + ReadState with node-grouped PubSub.
func TestRedisMapBroker_NodeGrouped_Basic(t *testing.T) {
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
	e := newNodeGroupedMapBroker(t, node, 16)
	defer func() { _ = e.Close(context.Background()) }()
	defer func() { _ = node.Shutdown(context.Background()) }()

	ctx := context.Background()
	channel := randomChannel("node_grouped_basic")

	// Publish keyed state.
	result, err := e.Publish(ctx, channel, "key1", MapPublishOptions{
		Data: []byte(`"value1"`),
	})
	require.NoError(t, err)
	require.True(t, result.Position.Offset > 0)

	// Read state back.
	stateRes, err := e.ReadState(ctx, channel, MapReadStateOptions{Limit: 100})
	require.NoError(t, err)
	pubs, _, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
	require.NoError(t, err)
	require.Len(t, pubs, 1)
	require.Equal(t, "key1", pubs[0].Key)
}

// TestRedisMapBroker_NodeGrouped_PubSubTwoNodes tests PubSub delivery between two
// MapBroker instances with node-grouped connections, using many channels to verify
// proper distribution across Redis cluster nodes.
func TestRedisMapBroker_NodeGrouped_PubSubTwoNodes(t *testing.T) {
	// Both brokers share the same prefix so they communicate via the same channels.
	prefix := getUniquePrefix()

	node1, _ := New(Config{
		LogLevel:   LogLevelDebug,
		LogHandler: func(entry LogEntry) {},
		Map: MapConfig{
			GetMapChannelOptions: func(channel string) MapChannelOptions {
				return MapChannelOptions{
					Mode:   MapModeRecoverable,
					KeyTTL: 60 * time.Second,
				}
			},
		},
	})
	e1 := newNodeGroupedMapBrokerPrefix(t, node1, 16, prefix)
	defer func() { _ = e1.Close(context.Background()) }()
	defer func() { _ = node1.Shutdown(context.Background()) }()

	// Use enough channels to ensure they distribute across all Redis cluster nodes.
	msgNum := 50
	var numPublications int64
	pubCh := make(chan struct{})

	handler := &testBrokerEventHandler{
		HandleControlFunc: func(bytes []byte) error {
			return nil
		},
		HandlePublicationFunc: func(ch string, pub *Publication, sp StreamPosition, delta bool, prevPub *Publication) error {
			c := atomic.AddInt64(&numPublications, 1)
			if c == int64(msgNum) {
				close(pubCh)
			}
			return nil
		},
		HandleJoinFunc: func(ch string, info *ClientInfo) error {
			return nil
		},
		HandleLeaveFunc: func(ch string, info *ClientInfo) error {
			return nil
		},
	}
	err := e1.RegisterEventHandler(handler)
	require.NoError(t, err)

	// Subscribe to channels — they'll hash to different partitions (and nodes).
	for i := 0; i < msgNum; i++ {
		require.NoError(t, e1.Subscribe("test_ngp_map_"+strconv.Itoa(i)))
	}

	// Verify channels distribute across multiple nodes.
	nodeHits := map[int]int{}
	for i := 0; i < msgNum; i++ {
		ch := "test_ngp_map_" + strconv.Itoa(i)
		partIdx := consistentIndex(ch, 16)
		nodeIdx := e1.shards[0].partitionToNodeIdx[partIdx]
		nodeHits[nodeIdx]++
	}
	numNodesHit := len(nodeHits)
	require.Greater(t, numNodesHit, 1, "channels should distribute across multiple nodes, got distribution: %v", nodeHits)
	t.Logf("channel distribution across %d nodes: %v", numNodesHit, nodeHits)

	// Setup a second broker to publish (same prefix).
	node2, _ := New(Config{
		Map: MapConfig{
			GetMapChannelOptions: func(channel string) MapChannelOptions {
				return MapChannelOptions{
					Mode:   MapModeRecoverable,
					KeyTTL: 60 * time.Second,
				}
			},
		},
	})
	e2 := newNodeGroupedMapBrokerPrefix(t, node2, 16, prefix)
	defer func() { _ = e2.Close(context.Background()) }()
	defer func() { _ = node2.Shutdown(context.Background()) }()
	_ = e2.RegisterEventHandler(&testBrokerEventHandler{
		HandleControlFunc: func(bytes []byte) error { return nil },
		HandlePublicationFunc: func(ch string, pub *Publication, sp StreamPosition, delta bool, prevPub *Publication) error {
			return nil
		},
		HandleJoinFunc:  func(ch string, info *ClientInfo) error { return nil },
		HandleLeaveFunc: func(ch string, info *ClientInfo) error { return nil },
	})

	// Publish from node2.
	for i := 0; i < msgNum; i++ {
		_, err := e2.Publish(context.Background(), "test_ngp_map_"+strconv.Itoa(i), "", MapPublishOptions{
			Data: []byte(`"hello"`),
		})
		require.NoError(t, err)
	}

	select {
	case <-pubCh:
		// All messages received.
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for publications: got %d/%d", atomic.LoadInt64(&numPublications), msgNum)
	}
}

// TestRedisMapBroker_NodeGrouped_TopologyStable verifies that refreshTopology
// returns false when the cluster topology hasn't changed.
func TestRedisMapBroker_NodeGrouped_TopologyStable(t *testing.T) {
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
	e := newNodeGroupedMapBroker(t, node, 32)
	defer func() { _ = e.Close(context.Background()) }()
	defer func() { _ = node.Shutdown(context.Background()) }()

	wrapper := e.shards[0]

	// Save original mapping for comparison.
	origMapping := make([]int, len(wrapper.partitionToNodeIdx))
	copy(origMapping, wrapper.partitionToNodeIdx)
	origNumNodes := len(wrapper.nodeClients)

	// refreshTopology should return false — nothing changed.
	changed := e.refreshTopology(wrapper)
	require.False(t, changed, "refreshTopology should return false when topology is stable")

	// Mapping should be identical.
	require.Equal(t, origMapping, wrapper.partitionToNodeIdx, "partition mapping should not change")
	require.Equal(t, origNumNodes, len(wrapper.nodeClients), "node count should not change")
}

// TestRedisMapBroker_NodeGrouped_TopologyDoneChannel verifies that
// closeTopologyDone properly signals goroutines and can be called multiple times.
func TestRedisMapBroker_NodeGrouped_TopologyDoneChannel(t *testing.T) {
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
	e := newNodeGroupedMapBroker(t, node, 16)
	defer func() { _ = e.Close(context.Background()) }()
	defer func() { _ = node.Shutdown(context.Background()) }()

	wrapper := e.shards[0]

	// topologyDone should initially be open (not closed).
	select {
	case <-wrapper.topologyDone:
		t.Fatal("topologyDone should not be closed initially")
	default:
		// Good — channel is open.
	}

	// Close it — simulates topology change detection.
	e.closeTopologyDone(wrapper)

	// Should now be closed.
	select {
	case <-wrapper.topologyDone:
		// Good — channel is closed.
	default:
		t.Fatal("topologyDone should be closed after closeTopologyDone")
	}

	// Calling closeTopologyDone again should not panic (idempotent).
	e.closeTopologyDone(wrapper)

	// After rebuild, topologyDone should be a new open channel.
	e.rebuildNodeGroupedPubSub(wrapper)

	select {
	case <-wrapper.topologyDone:
		t.Fatal("topologyDone should be open after rebuild")
	default:
		// Good — new channel is open.
	}
}

// TestRedisMapBroker_NodeGrouped_QueryClusterSlots verifies that CLUSTER SLOTS
// returns valid slot ranges from the live cluster.
func TestRedisMapBroker_NodeGrouped_QueryClusterSlots(t *testing.T) {
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
	redisConf := RedisShardConfig{
		ClusterAddresses: []string{"localhost:7001", "localhost:7002", "localhost:7003"},
		IOTimeout:        10 * time.Second,
		ConnectTimeout:   10 * time.Second,
	}
	shard, err := NewRedisShard(node, redisConf)
	require.NoError(t, err)
	defer func() { _ = node.Shutdown(context.Background()) }()

	ranges, err := queryClusterSlots(context.Background(), shard.client)
	require.NoError(t, err)
	require.Greater(t, len(ranges), 0, "should have at least one slot range")

	// Verify ranges cover all 16384 slots.
	covered := make([]bool, 16384)
	for _, r := range ranges {
		require.LessOrEqual(t, r.start, r.end, "start should <= end")
		require.Less(t, r.end, uint16(16384), "end should be < 16384")
		require.NotEmpty(t, r.addr, "addr should not be empty")
		for s := r.start; s <= r.end; s++ {
			covered[s] = true
		}
	}
	for i, c := range covered {
		require.True(t, c, "slot %d is not covered by any range", i)
	}

	t.Logf("cluster has %d slot ranges", len(ranges))
	for _, r := range ranges {
		t.Logf("  slots %d-%d → %s", r.start, r.end, r.addr)
	}
}

// TestRedisMapBroker_NodeGrouped_RebuildPreservesPartitions verifies that
// rebuildNodeGroupedPubSub produces the same mapping on a stable cluster.
func TestRedisMapBroker_NodeGrouped_RebuildPreservesPartitions(t *testing.T) {
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
	e := newNodeGroupedMapBroker(t, node, 64)
	defer func() { _ = e.Close(context.Background()) }()
	defer func() { _ = node.Shutdown(context.Background()) }()

	wrapper := e.shards[0]

	// Save original state.
	origMapping := make([]int, len(wrapper.partitionToNodeIdx))
	copy(origMapping, wrapper.partitionToNodeIdx)
	origNodePartitions := make([][]int, len(wrapper.nodePartitions))
	for i, parts := range wrapper.nodePartitions {
		origNodePartitions[i] = make([]int, len(parts))
		copy(origNodePartitions[i], parts)
	}
	origNumNodes := len(wrapper.nodeClients)

	// Force rebuild — should produce identical mapping on stable cluster.
	e.rebuildNodeGroupedPubSub(wrapper)

	require.Equal(t, origNumNodes, len(wrapper.nodeClients), "node count should be same after rebuild")
	require.Equal(t, origMapping, wrapper.partitionToNodeIdx, "partition→node mapping should be same after rebuild")
	for i, parts := range wrapper.nodePartitions {
		require.Equal(t, origNodePartitions[i], parts, "nodePartitions[%d] should be same after rebuild", i)
	}
}

// TestRedisMapBroker_NodeGrouped_RapidRebuildPreservesConnections is a stress
// test for the rebuild path. It triggers many rebuilds in rapid succession on
// a stable cluster (numNodes unchanged — the same array is reused) and then
// verifies subClients slots end up populated and pub/sub still works.
//
// The race that motivated the compare-and-swap on the cleanup defer requires
// an old cleanup goroutine's defer to fire AFTER a new loop has written its
// conn into the same slot. In practice that ordering is rare — the new
// loop's init takes milliseconds (Dedicate + subscribe + resubscribe) while
// an old defer is microseconds — so this test exercises the rebuild
// machinery rather than deterministically reproducing the clobber. Useful as
// general regression coverage even though a flake-free reproducer for the
// specific race would require instrumented hooks.
func TestRedisMapBroker_NodeGrouped_RapidRebuildPreservesConnections(t *testing.T) {
	prefix := getUniquePrefix()

	node1, _ := New(Config{
		Map: MapConfig{
			GetMapChannelOptions: func(channel string) MapChannelOptions {
				return MapChannelOptions{
					Mode:   MapModeRecoverable,
					KeyTTL: 60 * time.Second,
				}
			},
		},
	})
	e1 := newNodeGroupedMapBrokerPrefix(t, node1, 16, prefix)
	defer func() { _ = e1.Close(context.Background()) }()
	defer func() { _ = node1.Shutdown(context.Background()) }()

	var numPubs atomic.Int64
	pubCh := make(chan struct{}, 1)
	require.NoError(t, e1.RegisterEventHandler(&testBrokerEventHandler{
		HandleControlFunc: func(bytes []byte) error { return nil },
		HandlePublicationFunc: func(ch string, pub *Publication, sp StreamPosition, delta bool, prevPub *Publication) error {
			numPubs.Add(1)
			select {
			case pubCh <- struct{}{}:
			default:
			}
			return nil
		},
		HandleJoinFunc:  func(ch string, info *ClientInfo) error { return nil },
		HandleLeaveFunc: func(ch string, info *ClientInfo) error { return nil },
	}))

	testChannel := "rapid_rebuild_test_ch"
	require.NoError(t, e1.Subscribe(testChannel))

	// Publisher broker (same prefix).
	node2, _ := New(Config{
		Map: MapConfig{
			GetMapChannelOptions: func(channel string) MapChannelOptions {
				return MapChannelOptions{
					Mode:   MapModeRecoverable,
					KeyTTL: 60 * time.Second,
				}
			},
		},
	})
	e2 := newNodeGroupedMapBrokerPrefix(t, node2, 16, prefix)
	defer func() { _ = e2.Close(context.Background()) }()
	defer func() { _ = node2.Shutdown(context.Background()) }()
	_ = e2.RegisterEventHandler(&testBrokerEventHandler{
		HandleControlFunc: func(bytes []byte) error { return nil },
		HandlePublicationFunc: func(ch string, pub *Publication, sp StreamPosition, delta bool, prevPub *Publication) error {
			return nil
		},
		HandleJoinFunc:  func(ch string, info *ClientInfo) error { return nil },
		HandleLeaveFunc: func(ch string, info *ClientInfo) error { return nil },
	})

	wrapper := e1.shards[0]

	// Sanity check: publication works in the steady state.
	_, err := e2.Publish(context.Background(), testChannel, "", MapPublishOptions{
		Data: []byte(`"baseline"`),
	})
	require.NoError(t, err)
	select {
	case <-pubCh:
	case <-time.After(5 * time.Second):
		t.Fatal("baseline publication did not arrive")
	}

	// Trigger many rebuilds in rapid succession on a stable cluster.
	// numNodes is unchanged across these — exactly the path where the
	// race lived (old defer clobbering new init in the same array slot).
	// Tight spacing — well under the 500ms drain heuristic — so old
	// loops' cleanup goroutines are likely still pending when the next
	// rebuild fires.
	const numRebuilds = 50
	for i := 0; i < numRebuilds; i++ {
		e1.closeTopologyDone(wrapper)
		time.Sleep(20 * time.Millisecond)
		e1.rebuildNodeGroupedPubSub(wrapper)
	}

	// Give the system time to settle: goroutines must reach the point where
	// they re-subscribe and write their conn into the (post-final-rebuild)
	// subClients slot. With CAS on the cleanup defer, those writes survive.
	require.Eventually(t, func() bool {
		wrapper.subClientsMu.Lock()
		defer wrapper.subClientsMu.Unlock()
		for i := range wrapper.subClients {
			for j := range wrapper.subClients[i] {
				if wrapper.subClients[i][j] == nil {
					return false
				}
			}
		}
		return true
	}, 15*time.Second, 200*time.Millisecond, "all subClients slots should be populated after rebuilds settle")

	// Final functional check: a publication still arrives end-to-end.
	numPubs.Store(0)
	pubCh = make(chan struct{}, 1)
	require.NoError(t, e1.Subscribe(testChannel))
	_, err = e2.Publish(context.Background(), testChannel, "", MapPublishOptions{
		Data: []byte(`"post_rapid_rebuild"`),
	})
	require.NoError(t, err)
	select {
	case <-pubCh:
	case <-time.After(10 * time.Second):
		t.Fatal("post-rebuild publication did not arrive — subClients slot likely got nil-clobbered")
	}
}

// TestRedisMapBroker_NodeGrouped_Cleanup tests that the cleanup worker functions
// correctly with node-grouped PubSub connections.
func TestRedisMapBroker_NodeGrouped_Cleanup(t *testing.T) {
	node, _ := New(Config{
		Map: MapConfig{
			GetMapChannelOptions: func(channel string) MapChannelOptions {
				return MapChannelOptions{
					Mode:   MapModeRecoverable,
					KeyTTL: 1 * time.Second,
				}
			},
		},
	})
	redisConf := RedisShardConfig{
		ClusterAddresses: []string{"localhost:7001", "localhost:7002", "localhost:7003"},
		IOTimeout:        10 * time.Second,
		ConnectTimeout:   10 * time.Second,
	}
	shard, err := NewRedisShard(node, redisConf)
	require.NoError(t, err)

	prefix := getUniquePrefix()
	e, err := NewRedisMapBroker(node, RedisMapBrokerConfig{
		Shards:                     []*RedisShard{shard},
		Prefix:                     prefix,
		NumShardedPubSubPartitions: 16,
		GroupShardedPubSubByNode:   true,
		CleanupInterval:            500 * time.Millisecond,
		CleanupBatchSize:           100,
	})
	require.NoError(t, err)
	defer func() { _ = e.Close(context.Background()) }()
	defer func() { _ = node.Shutdown(context.Background()) }()

	// Verify sharded PUB/SUB is supported.
	result := shard.client.Do(context.Background(), shard.client.B().Spublish().Channel(prefix+"._").Message("").Build())
	if result.Error() != nil && strings.Contains(result.Error().Error(), "unknown command") {
		t.Skip("sharded PUB/SUB not supported by this Redis version, skipping test")
	}

	err = e.RegisterEventHandler(&testBrokerEventHandler{
		HandleControlFunc: func(bytes []byte) error { return nil },
		HandlePublicationFunc: func(ch string, pub *Publication, sp StreamPosition, delta bool, prevPub *Publication) error {
			return nil
		},
		HandleJoinFunc:  func(ch string, info *ClientInfo) error { return nil },
		HandleLeaveFunc: func(ch string, info *ClientInfo) error { return nil },
	})
	require.NoError(t, err)

	ctx := context.Background()
	channel := randomChannel("node_grouped_cleanup")

	// Publish with very short TTL.
	_, err = e.Publish(ctx, channel, "expire_key", MapPublishOptions{
		Data: []byte(`"temp"`),
	})
	require.NoError(t, err)

	// Verify key exists.
	stateRes, err := e.ReadState(ctx, channel, MapReadStateOptions{Limit: 100})
	require.NoError(t, err)
	pubs, _, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
	require.NoError(t, err)
	require.Len(t, pubs, 1)

	// Wait for TTL expiry + cleanup cycle.
	time.Sleep(3 * time.Second)

	// Key should be cleaned up.
	stateRes, err = e.ReadState(ctx, channel, MapReadStateOptions{Limit: 100})
	require.NoError(t, err)
	pubs, _, _ = stateRes.Publications, stateRes.Position, stateRes.Cursor
	require.NoError(t, err)
	require.Len(t, pubs, 0, "expired key should be cleaned up")
}

// newNodeGroupedBroker creates a RedisBroker with GroupShardedPubSubByNode=true on a 3-node cluster.
func newNodeGroupedBroker(t *testing.T, n *Node, numPartitions int) *RedisBroker {
	return newNodeGroupedBrokerPrefix(t, n, numPartitions, getUniquePrefix())
}

func newNodeGroupedBrokerPrefix(t *testing.T, n *Node, numPartitions int, prefix string) *RedisBroker {
	t.Helper()
	redisConf := RedisShardConfig{
		ClusterAddresses: []string{"localhost:7001", "localhost:7002", "localhost:7003"},
		IOTimeout:        10 * time.Second,
		ConnectTimeout:   10 * time.Second,
	}
	shard, err := NewRedisShard(n, redisConf)
	require.NoError(t, err)

	b, err := NewRedisBroker(n, RedisBrokerConfig{
		Shards:                     []*RedisShard{shard},
		Prefix:                     prefix,
		NumShardedPubSubPartitions: numPartitions,
		GroupShardedPubSubByNode:   true,
		numResubscribeShards:       4,
		numPubSubProcessors:        2,
	})
	require.NoError(t, err)
	n.SetBroker(b)

	// Verify sharded PUB/SUB is supported.
	result := shard.client.Do(context.Background(), shard.client.B().Spublish().Channel(prefix+"._").Message("").Build())
	if result.Error() != nil && strings.Contains(result.Error().Error(), "unknown command") {
		t.Skip("sharded PUB/SUB not supported by this Redis version, skipping test")
	}

	return b
}

// TestRedisBroker_NodeGrouped_ConnectionCount verifies that the number of
// first-dimension subClients entries equals the number of Redis Cluster nodes.
func TestRedisBroker_NodeGrouped_ConnectionCount(t *testing.T) {
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
	numPartitions := 32
	b := newNodeGroupedBroker(t, node, numPartitions)
	defer func() { _ = node.Shutdown(context.Background()) }()
	defer stopRedisBroker(b)

	wrapper := b.shards[0]

	numNodes := len(wrapper.nodeClients)
	require.Greater(t, numNodes, 0, "should have at least one node")
	require.Equal(t, numNodes, len(wrapper.subClients), "subClients first dimension should equal node count")
	require.Less(t, numNodes, numPartitions, "node count should be less than partition count")

	t.Logf("partitions=%d nodes=%d (%.0f%% reduction in connections)",
		numPartitions, numNodes, float64(numPartitions-numNodes)/float64(numPartitions)*100)

	// Verify partitionToNodeIdx covers all partitions.
	require.Equal(t, numPartitions, len(wrapper.partitionToNodeIdx))

	// Verify every partition maps to a valid node.
	for i, nodeIdx := range wrapper.partitionToNodeIdx {
		require.GreaterOrEqual(t, nodeIdx, 0, "partition %d has invalid nodeIdx", i)
		require.Less(t, nodeIdx, numNodes, "partition %d has out-of-range nodeIdx", i)
	}

	// Verify all nodes have at least one partition.
	for nodeIdx, parts := range wrapper.nodePartitions {
		require.Greater(t, len(parts), 0, "node %d has no partitions assigned", nodeIdx)
	}
}

// TestRedisBroker_NodeGrouped_PubSubTwoNodes tests PubSub delivery between two broker
// instances with node-grouped connections, using many channels to verify proper distribution
// across Redis cluster nodes.
func TestRedisBroker_NodeGrouped_PubSubTwoNodes(t *testing.T) {
	// Both brokers share the same prefix so they communicate via the same channels.
	prefix := getUniquePrefix()

	node1, _ := New(Config{
		LogLevel:   LogLevelDebug,
		LogHandler: func(entry LogEntry) {},
		Map: MapConfig{
			GetMapChannelOptions: func(channel string) MapChannelOptions {
				return MapChannelOptions{
					Mode:   MapModeRecoverable,
					KeyTTL: 60 * time.Second,
				}
			},
		},
	})
	b1 := newNodeGroupedBrokerPrefix(t, node1, 16, prefix)
	defer func() { _ = node1.Shutdown(context.Background()) }()
	defer stopRedisBroker(b1)

	// Use enough channels to ensure they distribute across all Redis cluster nodes.
	msgNum := 50
	var numPublications int64
	var numJoins int64
	var numLeaves int64
	pubCh := make(chan struct{})
	joinCh := make(chan struct{})
	leaveCh := make(chan struct{})

	brokerEventHandler := &testBrokerEventHandler{
		HandleControlFunc: func(bytes []byte) error {
			return nil
		},
		HandlePublicationFunc: func(ch string, pub *Publication, sp StreamPosition, delta bool, prevPub *Publication) error {
			c := atomic.AddInt64(&numPublications, 1)
			if c == int64(msgNum) {
				close(pubCh)
			}
			return nil
		},
		HandleJoinFunc: func(ch string, info *ClientInfo) error {
			c := atomic.AddInt64(&numJoins, 1)
			if c == int64(msgNum) {
				close(joinCh)
			}
			return nil
		},
		HandleLeaveFunc: func(ch string, info *ClientInfo) error {
			c := atomic.AddInt64(&numLeaves, 1)
			if c == int64(msgNum) {
				close(leaveCh)
			}
			return nil
		},
	}
	_ = b1.RegisterControlEventHandler(brokerEventHandler)
	_ = b1.RegisterBrokerEventHandler(brokerEventHandler)

	// Subscribe to channels — they'll hash to different partitions (and nodes).
	for i := 0; i < msgNum; i++ {
		require.NoError(t, b1.Subscribe("test_ngp_broker_"+strconv.Itoa(i)))
	}

	// Verify channels distribute across multiple nodes.
	nodeHits := map[int]int{}
	for i := 0; i < msgNum; i++ {
		ch := "test_ngp_broker_" + strconv.Itoa(i)
		partIdx := consistentIndex(ch, 16)
		nodeIdx := b1.shards[0].partitionToNodeIdx[partIdx]
		nodeHits[nodeIdx]++
	}
	numNodesHit := len(nodeHits)
	require.Greater(t, numNodesHit, 1, "channels should distribute across multiple nodes, got distribution: %v", nodeHits)
	t.Logf("channel distribution across %d nodes: %v", numNodesHit, nodeHits)

	// Setup a second broker to publish (same prefix).
	node2, _ := New(Config{
		Map: MapConfig{
			GetMapChannelOptions: func(channel string) MapChannelOptions {
				return MapChannelOptions{
					Mode:   MapModeRecoverable,
					KeyTTL: 60 * time.Second,
				}
			},
		},
	})
	b2 := newNodeGroupedBrokerPrefix(t, node2, 16, prefix)
	_ = node2.Run()
	defer func() { _ = node2.Shutdown(context.Background()) }()
	defer stopRedisBroker(b2)

	// Publish from node2.
	for i := 0; i < msgNum; i++ {
		ch := "test_ngp_broker_" + strconv.Itoa(i)
		_, err := node2.Publish(ch, []byte("123"))
		require.NoError(t, err)
		err = b2.PublishJoin(ch, &ClientInfo{})
		require.NoError(t, err)
		err = b2.PublishLeave(ch, &ClientInfo{})
		require.NoError(t, err)
	}

	select {
	case <-pubCh:
	case <-time.After(10 * time.Second):
		require.Fail(t, fmt.Sprintf("timeout waiting for publications: got %d/%d", atomic.LoadInt64(&numPublications), msgNum))
	}
	select {
	case <-joinCh:
	case <-time.After(10 * time.Second):
		require.Fail(t, fmt.Sprintf("timeout waiting for joins: got %d/%d", atomic.LoadInt64(&numJoins), msgNum))
	}
	select {
	case <-leaveCh:
	case <-time.After(10 * time.Second):
		require.Fail(t, fmt.Sprintf("timeout waiting for leaves: got %d/%d", atomic.LoadInt64(&numLeaves), msgNum))
	}
}

// TestRedisBroker_NodeGrouped_TopologyStable verifies that refreshBrokerTopology
// returns false when the cluster topology hasn't changed.
func TestRedisBroker_NodeGrouped_TopologyStable(t *testing.T) {
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
	b := newNodeGroupedBroker(t, node, 32)
	defer func() { _ = node.Shutdown(context.Background()) }()
	defer stopRedisBroker(b)

	wrapper := b.shards[0]

	// Save original mapping for comparison.
	origMapping := make([]int, len(wrapper.partitionToNodeIdx))
	copy(origMapping, wrapper.partitionToNodeIdx)
	origNumNodes := len(wrapper.nodeClients)

	// refreshBrokerTopology should return false — nothing changed.
	changed := b.refreshBrokerTopology(wrapper)
	require.False(t, changed, "refreshBrokerTopology should return false when topology is stable")

	// Mapping should be identical.
	require.Equal(t, origMapping, wrapper.partitionToNodeIdx, "partition mapping should not change")
	require.Equal(t, origNumNodes, len(wrapper.nodeClients), "node count should not change")
}

// TestRedisBroker_NodeGrouped_RebuildPreservesPartitions verifies that
// rebuildBrokerNodeGroupedPubSub produces the same mapping on a stable cluster.
func TestRedisBroker_NodeGrouped_RebuildPreservesPartitions(t *testing.T) {
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
	b := newNodeGroupedBroker(t, node, 64)
	defer func() { _ = node.Shutdown(context.Background()) }()
	defer stopRedisBroker(b)

	wrapper := b.shards[0]

	// Save original state.
	origMapping := make([]int, len(wrapper.partitionToNodeIdx))
	copy(origMapping, wrapper.partitionToNodeIdx)
	origNodePartitions := make([][]int, len(wrapper.nodePartitions))
	for i, parts := range wrapper.nodePartitions {
		origNodePartitions[i] = make([]int, len(parts))
		copy(origNodePartitions[i], parts)
	}
	origNumNodes := len(wrapper.nodeClients)

	// Force rebuild — should produce identical mapping on stable cluster.
	b.rebuildBrokerNodeGroupedPubSub(wrapper)

	require.Equal(t, origNumNodes, len(wrapper.nodeClients), "node count should be same after rebuild")
	require.Equal(t, origMapping, wrapper.partitionToNodeIdx, "partition→node mapping should be same after rebuild")
	for i, parts := range wrapper.nodePartitions {
		require.Equal(t, origNodePartitions[i], parts, "nodePartitions[%d] should be same after rebuild", i)
	}
}

// --- Slot migration helpers ---

var redisCLI = func() string {
	if p, err := exec.LookPath("redis-cli"); err == nil {
		return p
	}
	return ""
}()

func requireRedisCLI(t *testing.T) {
	t.Helper()
	if redisCLI == "" {
		t.Skip("redis-cli not found in PATH")
	}
}

// clusterNodeInfo holds parsed info from CLUSTER NODES output.
type clusterNodeInfo struct {
	id    string
	addr  string // host:port
	port  int
	slots [][2]int // pairs of [start, end]
}

// runRedisCLI runs a redis-cli command against a specific port and returns trimmed stdout.
func runRedisCLI(t *testing.T, port int, args ...string) string {
	t.Helper()
	cmdArgs := append([]string{"-p", strconv.Itoa(port)}, args...)
	cmd := exec.Command(redisCLI, cmdArgs...)
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "redis-cli -p %d %v failed: %s", port, args, string(out))
	return strings.TrimSpace(string(out))
}

// parseClusterNodes parses CLUSTER NODES output into structured data (masters only).
func parseClusterNodes(t *testing.T, port int) []clusterNodeInfo {
	t.Helper()
	output := runRedisCLI(t, port, "CLUSTER", "NODES")
	var nodes []clusterNodeInfo
	for _, line := range strings.Split(output, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 8 {
			continue
		}
		// Only include masters.
		if !strings.Contains(fields[2], "master") {
			continue
		}
		// Parse address: "host:port@cport"
		addrParts := strings.Split(fields[1], "@")
		hostPort := addrParts[0]
		parts := strings.Split(hostPort, ":")
		p, _ := strconv.Atoi(parts[1])

		node := clusterNodeInfo{
			id:   fields[0],
			addr: hostPort,
			port: p,
		}

		// Parse slot ranges (fields[8:]).
		for _, sf := range fields[8:] {
			if strings.Contains(sf, "[") {
				continue // skip migrating/importing markers
			}
			rangeParts := strings.Split(sf, "-")
			if len(rangeParts) == 2 {
				s, _ := strconv.Atoi(rangeParts[0])
				e, _ := strconv.Atoi(rangeParts[1])
				node.slots = append(node.slots, [2]int{s, e})
			} else if len(rangeParts) == 1 {
				s, _ := strconv.Atoi(rangeParts[0])
				node.slots = append(node.slots, [2]int{s, s})
			}
		}
		nodes = append(nodes, node)
	}
	return nodes
}

// findNodeForSlot returns the clusterNodeInfo that owns the given slot.
func findNodeForSlot(nodes []clusterNodeInfo, slot int) *clusterNodeInfo {
	for i := range nodes {
		for _, r := range nodes[i].slots {
			if slot >= r[0] && slot <= r[1] {
				return &nodes[i]
			}
		}
	}
	return nil
}

// migrateSlot migrates a single slot from source to target using CLUSTER SETSLOT commands.
func migrateSlot(t *testing.T, slot int, source, target *clusterNodeInfo, allNodes []clusterNodeInfo) {
	t.Helper()
	slotStr := strconv.Itoa(slot)

	t.Logf("migrating slot %d from %s (%s) to %s (%s)",
		slot, source.addr, source.id[:8], target.addr, target.id[:8])

	// Step 1: Set importing on target.
	runRedisCLI(t, target.port, "CLUSTER", "SETSLOT", slotStr, "IMPORTING", source.id)

	// Step 2: Set migrating on source.
	runRedisCLI(t, source.port, "CLUSTER", "SETSLOT", slotStr, "MIGRATING", target.id)

	// Step 3: Migrate all keys in the slot.
	for {
		keysOutput := runRedisCLI(t, source.port, "CLUSTER", "GETKEYSINSLOT", slotStr, "100")
		if keysOutput == "" || keysOutput == "(empty array)" || keysOutput == "(empty list or set)" {
			break
		}
		keys := strings.Split(keysOutput, "\n")
		var cleanKeys []string
		for _, k := range keys {
			k = strings.TrimSpace(k)
			// Strip redis-cli numbering like "1) "
			if idx := strings.Index(k, ") "); idx >= 0 {
				k = k[idx+2:]
			}
			k = strings.Trim(k, "\"")
			if k != "" {
				cleanKeys = append(cleanKeys, k)
			}
		}
		if len(cleanKeys) == 0 {
			break
		}
		args := []string{"-p", strconv.Itoa(source.port),
			"MIGRATE", "127.0.0.1", strconv.Itoa(target.port), "", "0", "5000", "KEYS"}
		args = append(args, cleanKeys...)
		cmd := exec.Command(redisCLI, args...)
		out, err := cmd.CombinedOutput()
		require.NoError(t, err, "MIGRATE failed: %s", string(out))
	}

	// Step 4: Notify all nodes that the slot now belongs to target.
	for _, node := range allNodes {
		runRedisCLI(t, node.port, "CLUSTER", "SETSLOT", slotStr, "NODE", target.id)
	}

	t.Logf("slot %d migration complete", slot)
}

// TestRedisMapBroker_NodeGrouped_SlotMigration tests that PubSub recovers after
// a Redis Cluster slot migration. Uses redis-cli to perform the actual migration.
func TestRedisMapBroker_NodeGrouped_SlotMigration(t *testing.T) {
	requireRedisCLI(t)
	prefix := getUniquePrefix()
	numPartitions := 128

	node1, _ := New(Config{
		LogLevel: LogLevelInfo,
		LogHandler: func(entry LogEntry) {
			t.Logf("[node1] %s %v", entry.Message, entry.Fields)
		},
		Map: MapConfig{
			GetMapChannelOptions: func(channel string) MapChannelOptions {
				return MapChannelOptions{
					Mode:   MapModeRecoverable,
					KeyTTL: 60 * time.Second,
				}
			},
		},
	})
	e1 := newNodeGroupedMapBrokerPrefix(t, node1, numPartitions, prefix)
	defer func() { _ = e1.Close(context.Background()) }()
	defer func() { _ = node1.Shutdown(context.Background()) }()

	// Pick a partition and find which cluster node owns its slot.
	targetPartition := 10
	slot := int(redisSlot("{" + strconv.Itoa(targetPartition) + "}"))
	t.Logf("target partition=%d slot=%d", targetPartition, slot)

	clusterNodes := parseClusterNodes(t, 7001)
	require.GreaterOrEqual(t, len(clusterNodes), 3, "need at least 3 cluster nodes")

	sourceNode := findNodeForSlot(clusterNodes, slot)
	require.NotNil(t, sourceNode, "no node found owning slot %d", slot)
	t.Logf("slot %d is on node %s (%s)", slot, sourceNode.addr, sourceNode.id[:8])

	// Pick a different node as migration target.
	var targetNode *clusterNodeInfo
	for i := range clusterNodes {
		if clusterNodes[i].id != sourceNode.id {
			targetNode = &clusterNodes[i]
			break
		}
	}
	require.NotNil(t, targetNode, "no target node found")

	// Find a channel that hashes to the target partition.
	var testChannel string
	for i := 0; i < 10000; i++ {
		ch := fmt.Sprintf("slotmig_%d", i)
		if consistentIndex(ch, numPartitions) == targetPartition {
			testChannel = ch
			break
		}
	}
	require.NotEmpty(t, testChannel, "couldn't find a channel hashing to partition %d", targetPartition)
	t.Logf("test channel=%q hashes to partition %d", testChannel, targetPartition)

	// --- Phase 1: Verify PubSub works before migration ---
	var numPubs atomic.Int64
	pubCh := make(chan struct{}, 1)
	handler := &testBrokerEventHandler{
		HandleControlFunc: func(bytes []byte) error { return nil },
		HandlePublicationFunc: func(ch string, pub *Publication, sp StreamPosition, delta bool, prevPub *Publication) error {
			if numPubs.Add(1) == 1 {
				select {
				case pubCh <- struct{}{}:
				default:
				}
			}
			return nil
		},
		HandleJoinFunc:  func(ch string, info *ClientInfo) error { return nil },
		HandleLeaveFunc: func(ch string, info *ClientInfo) error { return nil },
	}
	require.NoError(t, e1.RegisterEventHandler(handler))
	require.NoError(t, e1.Subscribe(testChannel))

	// Second broker to publish.
	node2, _ := New(Config{
		Map: MapConfig{
			GetMapChannelOptions: func(channel string) MapChannelOptions {
				return MapChannelOptions{
					Mode:   MapModeRecoverable,
					KeyTTL: 60 * time.Second,
				}
			},
		},
	})
	e2 := newNodeGroupedMapBrokerPrefix(t, node2, numPartitions, prefix)
	defer func() { _ = e2.Close(context.Background()) }()
	defer func() { _ = node2.Shutdown(context.Background()) }()
	_ = e2.RegisterEventHandler(&testBrokerEventHandler{
		HandleControlFunc: func(bytes []byte) error { return nil },
		HandlePublicationFunc: func(ch string, pub *Publication, sp StreamPosition, delta bool, prevPub *Publication) error {
			return nil
		},
		HandleJoinFunc:  func(ch string, info *ClientInfo) error { return nil },
		HandleLeaveFunc: func(ch string, info *ClientInfo) error { return nil },
	})

	// Publish and verify delivery before migration.
	_, err := e2.Publish(context.Background(), testChannel, "", MapPublishOptions{
		Data: []byte(`"before_migration"`),
	})
	require.NoError(t, err)

	select {
	case <-pubCh:
		t.Log("phase 1: publication received before migration")
	case <-time.After(10 * time.Second):
		t.Fatal("phase 1: timeout waiting for publication before migration")
	}

	// Record the broker's node index for this partition BEFORE migration.
	wrapper := e1.shards[0]
	wrapper.subClientsMu.Lock()
	origNodeIdx := wrapper.partitionToNodeIdx[targetPartition]
	wrapper.subClientsMu.Unlock()
	t.Logf("partition %d is at broker nodeIdx=%d before migration", targetPartition, origNodeIdx)

	// --- Phase 2: Migrate the slot ---
	migrateSlot(t, slot, sourceNode, targetNode, clusterNodes)
	// Always migrate back for cleanup, even if test fails.
	defer func() {
		currentNodes := parseClusterNodes(t, 7001)
		currentSource := findNodeForSlot(currentNodes, slot)
		var currentTarget *clusterNodeInfo
		for i := range currentNodes {
			if currentNodes[i].id == sourceNode.id {
				currentTarget = &currentNodes[i]
				break
			}
		}
		if currentSource != nil && currentTarget != nil && currentSource.id != currentTarget.id {
			t.Logf("cleanup: migrating slot %d back from %s to %s",
				slot, currentSource.addr, currentTarget.addr)
			migrateSlot(t, slot, currentSource, currentTarget, currentNodes)
		}
	}()

	// --- Phase 3: Wait for topology rebuild and verify ---
	t.Log("phase 2: slot migrated, waiting for topology rebuild...")

	// Wait for topology rebuild and goroutine restart to settle: poll until
	// every subClients slot is repopulated post-rebuild. This is the
	// deterministic version of "sleep N seconds and hope" — the rebuild
	// cycle (sunsubscribe → topologyRebuildCh → close + sleep + rebuild +
	// runForever retry + dedicate + resubscribe) has a variable wall time.
	waitForSubClientsPopulated(t, &wrapper.subClientsMu, &wrapper.subClients, 15*time.Second)

	// Verify topology was rebuilt correctly.
	wrapper.subClientsMu.Lock()
	newNodeIdx := wrapper.partitionToNodeIdx[targetPartition]
	topologyClosed := func() bool {
		select {
		case <-wrapper.topologyDone:
			return true
		default:
			return false
		}
	}()
	allConnsUp := true
	for ni := range wrapper.subClients {
		for _, conn := range wrapper.subClients[ni] {
			if conn == nil {
				allConnsUp = false
			}
		}
	}
	wrapper.subClientsMu.Unlock()

	// Partition should have moved to a different node index.
	require.NotEqual(t, origNodeIdx, newNodeIdx,
		"partition %d should have moved from nodeIdx=%d to a different node", targetPartition, origNodeIdx)
	require.False(t, topologyClosed, "topologyDone should be open after rebuild")
	require.True(t, allConnsUp, "all subClients should be connected after rebuild")
	t.Logf("phase 2: topology rebuilt, partition %d moved from nodeIdx=%d to nodeIdx=%d", targetPartition, origNodeIdx, newNodeIdx)

	// Re-subscribe the test channel on the new connection.
	// In production, Hub().Channels() provides channels for automatic resubscribe.
	// Here we test the infrastructure recovery by explicitly subscribing on the new topology.
	require.NoError(t, e1.Subscribe(testChannel))

	// Reset publication counter.
	numPubs.Store(0)
	pubCh = make(chan struct{}, 1)

	// Publish and verify delivery after migration.
	_, err = e2.Publish(context.Background(), testChannel, "", MapPublishOptions{
		Data: []byte(`"after_migration"`),
	})
	require.NoError(t, err)

	select {
	case <-pubCh:
		t.Log("phase 3: publication received after slot migration — recovery successful")
	case <-time.After(10 * time.Second):
		t.Fatal("phase 3: timeout waiting for publication after slot migration")
	}
}

// --- Add/Remove node helpers ---

var redisServer = func() string {
	if p, err := exec.LookPath("redis-server"); err == nil {
		return p
	}
	return ""
}()

// startRedisNode starts a redis-server on the given port with cluster enabled.
// Returns a cleanup function that shuts down the server.
func startRedisNode(t *testing.T, port int) func() {
	t.Helper()
	portStr := strconv.Itoa(port)
	confFile := fmt.Sprintf("/tmp/nodes-%s.conf", portStr)

	cmd := exec.Command(redisServer,
		"--port", portStr,
		"--cluster-enabled", "yes",
		"--cluster-config-file", confFile,
		"--save", "",
		"--appendonly", "no",
		"--daemonize", "yes",
		"--loglevel", "warning",
	)
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "failed to start redis-server on port %d: %s", port, string(out))

	// Wait for the server to be ready.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		pingCmd := exec.Command(redisCLI, "-p", portStr, "PING")
		if pingOut, err := pingCmd.CombinedOutput(); err == nil && strings.TrimSpace(string(pingOut)) == "PONG" {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	return func() {
		shutCmd := exec.Command(redisCLI, "-p", portStr, "SHUTDOWN", "NOSAVE")
		_ = shutCmd.Run()
		// Clean up config file.
		_ = exec.Command("rm", "-f", confFile).Run()
	}
}

// getNodeID returns the CLUSTER MYID of the node at the given port.
func getNodeID(t *testing.T, port int) string {
	t.Helper()
	output := runRedisCLI(t, port, "CLUSTER", "MYID")
	return strings.TrimSpace(output)
}

// waitForClusterConverge polls until `redis-cli --cluster check` passes without
// configuration disagreement errors. This ensures all nodes agree on config epochs
// after a topology change (add-node, reshard, etc.).
func waitForClusterConverge(t *testing.T, port int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		cmd := exec.Command(redisCLI, "--cluster", "check",
			fmt.Sprintf("127.0.0.1:%d", port),
		)
		out, _ := cmd.CombinedOutput()
		output := string(out)
		if !strings.Contains(output, "[ERR]") {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("cluster did not converge within %v", timeout)
}

// addNodeToCluster adds a new node to an existing cluster and waits for all
// existing nodes to recognize it and agree on the configuration.
func addNodeToCluster(t *testing.T, newPort, existingPort int, allPorts []int) {
	t.Helper()
	cmd := exec.Command(redisCLI, "--cluster", "add-node",
		fmt.Sprintf("127.0.0.1:%d", newPort),
		fmt.Sprintf("127.0.0.1:%d", existingPort),
	)
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "add-node failed: %s", string(out))

	// Wait for the new node to appear in CLUSTER NODES on ALL cluster nodes.
	newID := getNodeID(t, newPort)
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		allSee := true
		for _, port := range allPorts {
			output := runRedisCLI(t, port, "CLUSTER", "NODES")
			if !strings.Contains(output, newID) {
				allSee = false
				break
			}
		}
		if allSee {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}

	// Wait for all nodes to agree on configuration (config epochs converged).
	waitForClusterConverge(t, existingPort, 15*time.Second)
}

// reshardToNode reshards numSlots slots from sourceID to targetID via redis-cli.
func reshardToNode(t *testing.T, existingPort int, fromID, toID string, numSlots int) {
	t.Helper()
	cmd := exec.Command(redisCLI, "--cluster", "reshard",
		fmt.Sprintf("127.0.0.1:%d", existingPort),
		"--cluster-from", fromID,
		"--cluster-to", toID,
		"--cluster-slots", strconv.Itoa(numSlots),
		"--cluster-yes",
	)
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "reshard failed: %s", string(out))
}

// removeNodeFromCluster removes a node from the cluster.
func removeNodeFromCluster(t *testing.T, existingPort int, nodeID string) {
	t.Helper()
	cmd := exec.Command(redisCLI, "--cluster", "del-node",
		fmt.Sprintf("127.0.0.1:%d", existingPort),
		nodeID,
	)
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "del-node failed: %s", string(out))
}

// forceRueidisTopologyRefresh triggers MOVED errors by issuing SET commands against
// keys that hash to various slots, forcing rueidis to refresh its internal node map.
func forceRueidisTopologyRefresh(t *testing.T, client rueidis.Client) {
	t.Helper()
	ctx := context.Background()
	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("__topology_probe__%d", i)
		_ = client.Do(ctx, client.B().Set().Key(key).Value("x").Ex(10).Build()).Error()
	}
}

// cleanupNode7004 ensures port 7004 is not in the cluster and not running.
// Safe to call even if 7004 was never started.
func cleanupNode7004(t *testing.T) {
	t.Helper()
	// Try to shut down redis on 7004.
	_ = exec.Command(redisCLI, "-p", "7004", "SHUTDOWN", "NOSAVE").Run()
	_ = exec.Command("rm", "-f", "/tmp/nodes-7004.conf").Run()

	// Get node IDs for canonical owners (7001, 7002, 7003).
	output := runRedisCLI(t, 7001, "CLUSTER", "NODES")
	canonicalNodeID := func(port string) string {
		for _, l := range strings.Split(output, "\n") {
			if strings.Contains(l, port) && strings.Contains(l, "master") {
				return strings.Fields(l)[0]
			}
		}
		return ""
	}
	nodeIDs := map[int]string{
		7001: canonicalNodeID("7001"),
		7002: canonicalNodeID("7002"),
		7003: canonicalNodeID("7003"),
	}

	// Reassign any slot owned by 7004 (or noaddr) back to its CANONICAL
	// owner based on the slot number, not blindly to 7001. The canonical
	// distribution from create-cluster is:
	//   0-5460     → 7001
	//   5461-10922 → 7002
	//   10923-16383→ 7003
	// Putting slots back to the wrong owner is the prior bug that made
	// rerunning this test corrupt the cluster across iterations.
	canonicalOwnerForSlot := func(slot int) string {
		switch {
		case slot <= 5460:
			return nodeIDs[7001]
		case slot <= 10922:
			return nodeIDs[7002]
		default:
			return nodeIDs[7003]
		}
	}

	for _, line := range strings.Split(output, "\n") {
		if !strings.Contains(line, "7004") && !strings.Contains(line, "noaddr") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		nodeID := fields[0]
		// Check if this node has slots — reclaim them first.
		if strings.Contains(fields[2], "master") {
			for _, sf := range fields[8:] {
				if strings.Contains(sf, "[") {
					continue
				}
				rangeParts := strings.Split(sf, "-")
				var start, end int
				if len(rangeParts) == 2 {
					start, _ = strconv.Atoi(rangeParts[0])
					end, _ = strconv.Atoi(rangeParts[1])
				} else if len(rangeParts) == 1 {
					start, _ = strconv.Atoi(rangeParts[0])
					end = start
				}
				for s := start; s <= end; s++ {
					targetID := canonicalOwnerForSlot(s)
					if targetID == "" {
						continue
					}
					for _, p := range []int{7001, 7002, 7003} {
						_ = exec.Command(redisCLI, "-p", strconv.Itoa(p),
							"CLUSTER", "SETSLOT", strconv.Itoa(s), "NODE", targetID).Run()
					}
				}
			}
		}
		// Forget the node from all known nodes.
		for _, p := range []int{7001, 7002, 7003} {
			_ = exec.Command(redisCLI, "-p", strconv.Itoa(p), "CLUSTER", "FORGET", nodeID).Run()
		}
	}

	// Restore canonical distribution for any slots that drifted to a
	// non-canonical owner during a partially-completed earlier test run.
	// Without this, slots reshared from 7002 to 7001 (or any other
	// non-canonical placement) by a failed run stay there permanently.
	output = runRedisCLI(t, 7001, "CLUSTER", "NODES")
	for _, line := range strings.Split(output, "\n") {
		fields := strings.Fields(line)
		if len(fields) < 9 || !strings.Contains(line, "master") {
			continue
		}
		ownerID := fields[0]
		port := 0
		for p, id := range nodeIDs {
			if id == ownerID {
				port = p
				break
			}
		}
		if port == 0 {
			continue
		}
		for _, sf := range fields[8:] {
			if strings.Contains(sf, "[") {
				continue
			}
			rangeParts := strings.Split(sf, "-")
			var start, end int
			if len(rangeParts) == 2 {
				start, _ = strconv.Atoi(rangeParts[0])
				end, _ = strconv.Atoi(rangeParts[1])
			} else if len(rangeParts) == 1 {
				start, _ = strconv.Atoi(rangeParts[0])
				end = start
			}
			for s := start; s <= end; s++ {
				canonical := canonicalOwnerForSlot(s)
				if canonical == "" || canonical == ownerID {
					continue
				}
				for _, p := range []int{7001, 7002, 7003} {
					_ = exec.Command(redisCLI, "-p", strconv.Itoa(p),
						"CLUSTER", "SETSLOT", strconv.Itoa(s), "NODE", canonical).Run()
				}
			}
		}
	}
}

// TestRedisMapBroker_NodeGrouped_AddRemoveNode tests that a running broker's PubSub
// handles node additions and removals without panicking. It adds a 4th Redis Cluster
// node, verifies the running broker discovers it and PubSub still works, then removes
// the node and verifies no panic + PubSub recovery.
func TestRedisMapBroker_NodeGrouped_AddRemoveNode(t *testing.T) {
	requireRedisCLI(t)
	if redisServer == "" {
		t.Skip("redis-server not found in PATH")
	}
	// Ensure clean state from any previous failed runs.
	cleanupNode7004(t)

	prefix := getUniquePrefix()
	numPartitions := 128

	// Broker goroutines (topology refresh, per-node PubSub loops) can outlive
	// Close() under -count=N reruns: rebuilds and runForever retries keep
	// firing briefly after closeCh fires. If those tail-end log entries hit
	// t.Logf after the test function has returned, Go's testing package
	// panics ("Log in goroutine after test has completed"). Gate the log
	// handler on an atomic flag flipped at function exit; tail logs after
	// that point are silently dropped.
	var testEnded atomic.Bool
	defer testEnded.Store(true) // registered first → runs last

	node1, _ := New(Config{
		LogLevel: LogLevelInfo,
		LogHandler: func(entry LogEntry) {
			if testEnded.Load() {
				return
			}
			t.Logf("[e1] %s %v", entry.Message, entry.Fields)
		},
		Map: MapConfig{
			GetMapChannelOptions: func(channel string) MapChannelOptions {
				return MapChannelOptions{
					Mode:   MapModeRecoverable,
					KeyTTL: 60 * time.Second,
				}
			},
		},
	})
	e1 := newNodeGroupedMapBrokerPrefix(t, node1, numPartitions, prefix)
	defer func() { _ = e1.Close(context.Background()) }()
	defer func() { _ = node1.Shutdown(context.Background()) }()

	var numPubs atomic.Int64
	pubCh := make(chan struct{}, 1)
	signalPub := func() {
		select {
		case pubCh <- struct{}{}:
		default:
		}
	}
	handler := &testBrokerEventHandler{
		HandleControlFunc: func(bytes []byte) error { return nil },
		HandlePublicationFunc: func(ch string, pub *Publication, sp StreamPosition, delta bool, prevPub *Publication) error {
			numPubs.Add(1)
			signalPub()
			return nil
		},
		HandleJoinFunc:  func(ch string, info *ClientInfo) error { return nil },
		HandleLeaveFunc: func(ch string, info *ClientInfo) error { return nil },
	}
	require.NoError(t, e1.RegisterEventHandler(handler))

	testChannel := "add_remove_node_test_ch"
	require.NoError(t, e1.Subscribe(testChannel))

	// Publisher broker (same prefix).
	node2, _ := New(Config{
		Map: MapConfig{
			GetMapChannelOptions: func(channel string) MapChannelOptions {
				return MapChannelOptions{
					Mode:   MapModeRecoverable,
					KeyTTL: 60 * time.Second,
				}
			},
		},
	})
	e2 := newNodeGroupedMapBrokerPrefix(t, node2, numPartitions, prefix)
	defer func() { _ = e2.Close(context.Background()) }()
	defer func() { _ = node2.Shutdown(context.Background()) }()
	_ = e2.RegisterEventHandler(&testBrokerEventHandler{
		HandleControlFunc: func(bytes []byte) error { return nil },
		HandlePublicationFunc: func(ch string, pub *Publication, sp StreamPosition, delta bool, prevPub *Publication) error {
			return nil
		},
		HandleJoinFunc:  func(ch string, info *ClientInfo) error { return nil },
		HandleLeaveFunc: func(ch string, info *ClientInfo) error { return nil },
	})

	wrapper := e1.shards[0]

	// --- Phase 1: Verify PubSub works with 3 nodes ---
	t.Log("phase 1: verifying PubSub with 3 nodes")
	wrapper.subClientsMu.Lock()
	require.Equal(t, 3, len(wrapper.nodeClients), "should start with 3 nodes")
	wrapper.subClientsMu.Unlock()

	_, err := e2.Publish(context.Background(), testChannel, "", MapPublishOptions{
		Data: []byte(`"before_add"`),
	})
	require.NoError(t, err)

	select {
	case <-pubCh:
		t.Log("phase 1: publication received with 3 nodes")
	case <-time.After(10 * time.Second):
		t.Fatal("phase 1: timeout waiting for publication with 3 nodes")
	}

	// --- Phase 2: Add a 4th node to running cluster ---
	t.Log("phase 2: adding node on port 7004")
	cleanupNode := startRedisNode(t, 7004)
	defer func() {
		cleanupNode()
		// Also clean up cluster membership.
		cleanupNode7004(t)
	}()

	addNodeToCluster(t, 7004, 7001, []int{7001, 7002, 7003})
	newNodeID := getNodeID(t, 7004)
	t.Logf("phase 2: new node ID = %s", newNodeID)

	clusterNodes := parseClusterNodes(t, 7001)
	var sourceNode *clusterNodeInfo
	for i := range clusterNodes {
		if clusterNodes[i].id != newNodeID && len(clusterNodes[i].slots) > 0 {
			sourceNode = &clusterNodes[i]
			break
		}
	}
	require.NotNil(t, sourceNode, "need a source node with slots")
	t.Logf("phase 2: resharding 1000 slots from %s to %s", sourceNode.id[:8], newNodeID[:8])
	reshardToNode(t, 7001, sourceNode.id, newNodeID, 1000)
	waitForClusterConverge(t, 7001, 30*time.Second)

	// Force rueidis to discover the new node by issuing commands that will get
	// MOVED redirects for the migrated slots.
	forceRueidisTopologyRefresh(t, e1.shards[0].shard.client)

	// Poll until our refreshTopology sees 4 nodes and triggers rebuild.
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if e1.refreshTopology(wrapper) {
			t.Log("phase 2: topology change detected, triggering rebuild")
			e1.closeTopologyDone(wrapper)
			time.Sleep(500 * time.Millisecond)
			e1.rebuildNodeGroupedPubSub(wrapper)
		}
		wrapper.subClientsMu.Lock()
		nodeCount := len(wrapper.nodeClients)
		wrapper.subClientsMu.Unlock()
		if nodeCount == 4 {
			break
		}
		// Keep poking rueidis to discover new node.
		forceRueidisTopologyRefresh(t, e1.shards[0].shard.client)
		time.Sleep(1 * time.Second)
	}

	wrapper.subClientsMu.Lock()
	nodeCount := len(wrapper.nodeClients)
	wrapper.subClientsMu.Unlock()
	require.Equal(t, 4, nodeCount, "broker should see 4 nodes after add")
	t.Logf("phase 2: node count = %d, maxNodeGoroutines = %d", nodeCount, wrapper.maxNodeGoroutines)
	require.Equal(t, 4, wrapper.maxNodeGoroutines, "maxNodeGoroutines should be 4")

	// Wait for all subClients slots to be populated by post-rebuild goroutines
	// before exercising Subscribe. A fixed sleep here was flaky because in
	// rapid-rebuild scenarios goroutines may not have completed their
	// dedicate+resubscribe cycle within a hard-coded window.
	waitForSubClientsPopulated(t, &wrapper.subClientsMu, &wrapper.subClients, 15*time.Second)

	// Re-subscribe the test channel on the new topology.
	require.NoError(t, e1.Subscribe(testChannel))

	numPubs.Store(0)
	pubCh = make(chan struct{}, 1)

	_, err = e2.Publish(context.Background(), testChannel, "", MapPublishOptions{
		Data: []byte(`"after_add"`),
	})
	require.NoError(t, err)

	select {
	case <-pubCh:
		t.Log("phase 2: publication received with 4 nodes")
	case <-time.After(10 * time.Second):
		t.Fatal("phase 2: timeout waiting for publication with 4 nodes")
	}

	// --- Phase 3: Remove the 4th node ---
	t.Log("phase 3: removing node on port 7004")

	reshardToNode(t, 7001, newNodeID, sourceNode.id, 1000)
	waitForClusterConverge(t, 7001, 30*time.Second)
	removeNodeFromCluster(t, 7001, newNodeID)
	waitForClusterConverge(t, 7001, 30*time.Second)

	// Shut down 7004 so rueidis detects the connection is dead and drops it
	// from its internal Nodes() map. Without this, rueidis keeps the stale
	// connection alive and buildNodeMapping returns 4 nodes indefinitely.
	cleanupNode()
	time.Sleep(2 * time.Second)

	// Force rueidis to notice the topology change.
	forceRueidisTopologyRefresh(t, e1.shards[0].shard.client)

	// Poll until we see 3 nodes.
	deadline = time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		forceRueidisTopologyRefresh(t, e1.shards[0].shard.client)
		if e1.refreshTopology(wrapper) {
			t.Log("phase 3: topology change detected, triggering rebuild")
			e1.closeTopologyDone(wrapper)
			time.Sleep(500 * time.Millisecond)
			e1.rebuildNodeGroupedPubSub(wrapper)
		}
		wrapper.subClientsMu.Lock()
		nodeCount = len(wrapper.nodeClients)
		wrapper.subClientsMu.Unlock()
		if nodeCount == 3 {
			break
		}
		time.Sleep(1 * time.Second)
	}

	wrapper.subClientsMu.Lock()
	nodeCount = len(wrapper.nodeClients)
	wrapper.subClientsMu.Unlock()
	require.Equal(t, 3, nodeCount, "broker should see 3 nodes after remove")
	t.Logf("phase 3: node count = %d", nodeCount)

	// Wait for the post-shrink subClients (size 3) to be populated by the
	// post-rebuild goroutines. Orphaned goroutines for nodeIdx=3 self-skip
	// via the bounds check; we only require slots [0..2] to be live.
	waitForSubClientsPopulated(t, &wrapper.subClientsMu, &wrapper.subClients, 15*time.Second)

	// Re-subscribe.
	require.NoError(t, e1.Subscribe(testChannel))

	numPubs.Store(0)
	pubCh = make(chan struct{}, 1)

	_, err = e2.Publish(context.Background(), testChannel, "", MapPublishOptions{
		Data: []byte(`"after_remove"`),
	})
	require.NoError(t, err)

	select {
	case <-pubCh:
		t.Log("phase 3: publication received with 3 nodes — no panic, recovery successful")
	case <-time.After(10 * time.Second):
		t.Fatal("phase 3: timeout waiting for publication after remove")
	}
}

// TestRedisBroker_NodeGrouped_SlotMigration is the RedisBroker counterpart to
// TestRedisMapBroker_NodeGrouped_SlotMigration. The CAS fix for the rebuild
// cleanup defer lives in shared code (redis_pubsub_shared.go) and is exercised
// by both broker types; without this counterpart, the regular broker side has
// no integration coverage of the rebuild path under real slot migration.
func TestRedisBroker_NodeGrouped_SlotMigration(t *testing.T) {
	requireRedisCLI(t)
	prefix := getUniquePrefix()
	numPartitions := 128

	node1, _ := New(Config{
		LogLevel:   LogLevelInfo,
		LogHandler: func(entry LogEntry) {},
		Map: MapConfig{
			GetMapChannelOptions: func(channel string) MapChannelOptions {
				return MapChannelOptions{
					Mode:   MapModeRecoverable,
					KeyTTL: 60 * time.Second,
				}
			},
		},
	})
	b1 := newNodeGroupedBrokerPrefix(t, node1, numPartitions, prefix)
	defer func() { _ = node1.Shutdown(context.Background()) }()
	defer stopRedisBroker(b1)

	// Pick a partition and find which cluster node owns its slot.
	targetPartition := 10
	slot := int(redisSlot("{" + strconv.Itoa(targetPartition) + "}"))
	t.Logf("target partition=%d slot=%d", targetPartition, slot)

	clusterNodes := parseClusterNodes(t, 7001)
	require.GreaterOrEqual(t, len(clusterNodes), 3, "need at least 3 cluster nodes")

	sourceNode := findNodeForSlot(clusterNodes, slot)
	require.NotNil(t, sourceNode, "no node found owning slot %d", slot)

	var targetNode *clusterNodeInfo
	for i := range clusterNodes {
		if clusterNodes[i].id != sourceNode.id {
			targetNode = &clusterNodes[i]
			break
		}
	}
	require.NotNil(t, targetNode, "no target node found")

	// Find a channel that hashes to the target partition.
	var testChannel string
	for i := 0; i < 10000; i++ {
		ch := fmt.Sprintf("broker_slotmig_%d", i)
		if consistentIndex(ch, numPartitions) == targetPartition {
			testChannel = ch
			break
		}
	}
	require.NotEmpty(t, testChannel, "couldn't find a channel hashing to partition %d", targetPartition)

	// --- Phase 1: Verify PubSub works before migration ---
	var numPubs atomic.Int64
	pubCh := make(chan struct{}, 1)
	handler := &testBrokerEventHandler{
		HandleControlFunc: func(bytes []byte) error { return nil },
		HandlePublicationFunc: func(ch string, pub *Publication, sp StreamPosition, delta bool, prevPub *Publication) error {
			if numPubs.Add(1) == 1 {
				select {
				case pubCh <- struct{}{}:
				default:
				}
			}
			return nil
		},
		HandleJoinFunc:  func(ch string, info *ClientInfo) error { return nil },
		HandleLeaveFunc: func(ch string, info *ClientInfo) error { return nil },
	}
	_ = b1.RegisterControlEventHandler(handler)
	_ = b1.RegisterBrokerEventHandler(handler)
	require.NoError(t, b1.Subscribe(testChannel))

	// Second broker / node to publish (same prefix).
	node2, _ := New(Config{
		Map: MapConfig{
			GetMapChannelOptions: func(channel string) MapChannelOptions {
				return MapChannelOptions{
					Mode:   MapModeRecoverable,
					KeyTTL: 60 * time.Second,
				}
			},
		},
	})
	b2 := newNodeGroupedBrokerPrefix(t, node2, numPartitions, prefix)
	require.NoError(t, node2.Run())
	defer func() { _ = node2.Shutdown(context.Background()) }()
	defer stopRedisBroker(b2)

	_, err := node2.Publish(testChannel, []byte(`"before_migration"`))
	require.NoError(t, err)

	select {
	case <-pubCh:
		t.Log("phase 1: publication received before migration")
	case <-time.After(10 * time.Second):
		t.Fatal("phase 1: timeout waiting for publication before migration")
	}

	wrapper := b1.shards[0]
	wrapper.subClientsMu.Lock()
	origNodeIdx := wrapper.partitionToNodeIdx[targetPartition]
	wrapper.subClientsMu.Unlock()

	// --- Phase 2: Migrate the slot ---
	migrateSlot(t, slot, sourceNode, targetNode, clusterNodes)
	defer func() {
		currentNodes := parseClusterNodes(t, 7001)
		currentSource := findNodeForSlot(currentNodes, slot)
		var currentTarget *clusterNodeInfo
		for i := range currentNodes {
			if currentNodes[i].id == sourceNode.id {
				currentTarget = &currentNodes[i]
				break
			}
		}
		if currentSource != nil && currentTarget != nil && currentSource.id != currentTarget.id {
			migrateSlot(t, slot, currentSource, currentTarget, currentNodes)
		}
	}()

	// --- Phase 3: Wait for topology rebuild and verify ---
	t.Log("phase 2: slot migrated, waiting for topology rebuild...")
	waitForSubClientsPopulated(t, &wrapper.subClientsMu, &wrapper.subClients, 15*time.Second)

	wrapper.subClientsMu.Lock()
	newNodeIdx := wrapper.partitionToNodeIdx[targetPartition]
	wrapper.subClientsMu.Unlock()
	require.NotEqual(t, origNodeIdx, newNodeIdx,
		"partition %d should have moved to a different node", targetPartition)

	// Re-subscribe and verify delivery on the new topology.
	require.NoError(t, b1.Subscribe(testChannel))
	numPubs.Store(0)
	pubCh = make(chan struct{}, 1)

	_, err = node2.Publish(testChannel, []byte(`"after_migration"`))
	require.NoError(t, err)

	select {
	case <-pubCh:
		t.Log("phase 3: publication received after slot migration — recovery successful")
	case <-time.After(10 * time.Second):
		t.Fatal("phase 3: timeout waiting for publication after slot migration")
	}
}

// TestRedisBroker_NodeGrouped_AddRemoveNode is the RedisBroker counterpart to
// TestRedisMapBroker_NodeGrouped_AddRemoveNode. Adds a 4th cluster node,
// verifies the running broker discovers it and PubSub still works, then
// removes the node and verifies recovery.
func TestRedisBroker_NodeGrouped_AddRemoveNode(t *testing.T) {
	requireRedisCLI(t)
	if redisServer == "" {
		t.Skip("redis-server not found in PATH")
	}
	cleanupNode7004(t)

	prefix := getUniquePrefix()
	numPartitions := 128

	// See AddRemoveNode (map broker) for the rationale on testEnded gating.
	var testEnded atomic.Bool
	defer testEnded.Store(true)

	node1, _ := New(Config{
		LogLevel: LogLevelInfo,
		LogHandler: func(entry LogEntry) {
			if testEnded.Load() {
				return
			}
			t.Logf("[b1] %s %v", entry.Message, entry.Fields)
		},
		Map: MapConfig{
			GetMapChannelOptions: func(channel string) MapChannelOptions {
				return MapChannelOptions{
					Mode:   MapModeRecoverable,
					KeyTTL: 60 * time.Second,
				}
			},
		},
	})
	b1 := newNodeGroupedBrokerPrefix(t, node1, numPartitions, prefix)
	defer func() { _ = node1.Shutdown(context.Background()) }()
	defer stopRedisBroker(b1)

	var numPubs atomic.Int64
	pubCh := make(chan struct{}, 1)
	signalPub := func() {
		select {
		case pubCh <- struct{}{}:
		default:
		}
	}
	handler := &testBrokerEventHandler{
		HandleControlFunc: func(bytes []byte) error { return nil },
		HandlePublicationFunc: func(ch string, pub *Publication, sp StreamPosition, delta bool, prevPub *Publication) error {
			numPubs.Add(1)
			signalPub()
			return nil
		},
		HandleJoinFunc:  func(ch string, info *ClientInfo) error { return nil },
		HandleLeaveFunc: func(ch string, info *ClientInfo) error { return nil },
	}
	_ = b1.RegisterControlEventHandler(handler)
	_ = b1.RegisterBrokerEventHandler(handler)

	testChannel := "broker_add_remove_node_test_ch"
	require.NoError(t, b1.Subscribe(testChannel))

	node2, _ := New(Config{
		Map: MapConfig{
			GetMapChannelOptions: func(channel string) MapChannelOptions {
				return MapChannelOptions{
					Mode:   MapModeRecoverable,
					KeyTTL: 60 * time.Second,
				}
			},
		},
	})
	b2 := newNodeGroupedBrokerPrefix(t, node2, numPartitions, prefix)
	require.NoError(t, node2.Run())
	defer func() { _ = node2.Shutdown(context.Background()) }()
	defer stopRedisBroker(b2)

	wrapper := b1.shards[0]

	// --- Phase 1: Verify PubSub works with 3 nodes ---
	t.Log("phase 1: verifying PubSub with 3 nodes")
	wrapper.subClientsMu.Lock()
	require.Equal(t, 3, len(wrapper.nodeClients), "should start with 3 nodes")
	wrapper.subClientsMu.Unlock()

	_, err := node2.Publish(testChannel, []byte(`"before_add"`))
	require.NoError(t, err)
	select {
	case <-pubCh:
		t.Log("phase 1: publication received with 3 nodes")
	case <-time.After(10 * time.Second):
		t.Fatal("phase 1: timeout waiting for publication with 3 nodes")
	}

	// --- Phase 2: Add a 4th node ---
	t.Log("phase 2: adding node on port 7004")
	cleanupNode := startRedisNode(t, 7004)
	defer func() {
		cleanupNode()
		cleanupNode7004(t)
	}()

	addNodeToCluster(t, 7004, 7001, []int{7001, 7002, 7003})
	newNodeID := getNodeID(t, 7004)

	clusterNodes := parseClusterNodes(t, 7001)
	var sourceNode *clusterNodeInfo
	for i := range clusterNodes {
		if clusterNodes[i].id != newNodeID && len(clusterNodes[i].slots) > 0 {
			sourceNode = &clusterNodes[i]
			break
		}
	}
	require.NotNil(t, sourceNode, "need a source node with slots")
	reshardToNode(t, 7001, sourceNode.id, newNodeID, 1000)
	waitForClusterConverge(t, 7001, 30*time.Second)

	forceRueidisTopologyRefresh(t, b1.shards[0].shard.client)

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if b1.refreshBrokerTopology(wrapper) {
			t.Log("phase 2: topology change detected, triggering rebuild")
			b1.closeBrokerTopologyDone(wrapper)
			time.Sleep(500 * time.Millisecond)
			b1.rebuildBrokerNodeGroupedPubSub(wrapper)
		}
		wrapper.subClientsMu.Lock()
		nodeCount := len(wrapper.nodeClients)
		wrapper.subClientsMu.Unlock()
		if nodeCount == 4 {
			break
		}
		forceRueidisTopologyRefresh(t, b1.shards[0].shard.client)
		time.Sleep(1 * time.Second)
	}

	wrapper.subClientsMu.Lock()
	nodeCount := len(wrapper.nodeClients)
	wrapper.subClientsMu.Unlock()
	require.Equal(t, 4, nodeCount, "broker should see 4 nodes after add")
	require.Equal(t, 4, wrapper.maxNodeGoroutines, "maxNodeGoroutines should be 4")

	waitForSubClientsPopulated(t, &wrapper.subClientsMu, &wrapper.subClients, 15*time.Second)

	require.NoError(t, b1.Subscribe(testChannel))
	numPubs.Store(0)
	pubCh = make(chan struct{}, 1)

	_, err = node2.Publish(testChannel, []byte(`"after_add"`))
	require.NoError(t, err)
	select {
	case <-pubCh:
		t.Log("phase 2: publication received with 4 nodes")
	case <-time.After(10 * time.Second):
		t.Fatal("phase 2: timeout waiting for publication with 4 nodes")
	}

	// --- Phase 3: Remove the 4th node ---
	t.Log("phase 3: removing node on port 7004")

	reshardToNode(t, 7001, newNodeID, sourceNode.id, 1000)
	waitForClusterConverge(t, 7001, 30*time.Second)
	removeNodeFromCluster(t, 7001, newNodeID)
	waitForClusterConverge(t, 7001, 30*time.Second)

	cleanupNode()
	time.Sleep(2 * time.Second)

	forceRueidisTopologyRefresh(t, b1.shards[0].shard.client)

	deadline = time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		forceRueidisTopologyRefresh(t, b1.shards[0].shard.client)
		if b1.refreshBrokerTopology(wrapper) {
			t.Log("phase 3: topology change detected, triggering rebuild")
			b1.closeBrokerTopologyDone(wrapper)
			time.Sleep(500 * time.Millisecond)
			b1.rebuildBrokerNodeGroupedPubSub(wrapper)
		}
		wrapper.subClientsMu.Lock()
		nodeCount = len(wrapper.nodeClients)
		wrapper.subClientsMu.Unlock()
		if nodeCount == 3 {
			break
		}
		time.Sleep(1 * time.Second)
	}

	wrapper.subClientsMu.Lock()
	nodeCount = len(wrapper.nodeClients)
	wrapper.subClientsMu.Unlock()
	require.Equal(t, 3, nodeCount, "broker should see 3 nodes after remove")

	waitForSubClientsPopulated(t, &wrapper.subClientsMu, &wrapper.subClients, 15*time.Second)

	require.NoError(t, b1.Subscribe(testChannel))
	numPubs.Store(0)
	pubCh = make(chan struct{}, 1)

	_, err = node2.Publish(testChannel, []byte(`"after_remove"`))
	require.NoError(t, err)
	select {
	case <-pubCh:
		t.Log("phase 3: publication received with 3 nodes — recovery successful")
	case <-time.After(10 * time.Second):
		t.Fatal("phase 3: timeout waiting for publication after remove")
	}
}
