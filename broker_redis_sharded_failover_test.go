//go:build integration

package centrifuge

import (
	"context"
	"net"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/rueidis"
	"github.com/stretchr/testify/require"
)

// clusterReplicasAddrs returns the addresses of a Redis Cluster whose primaries
// have replicas (CENTRIFUGE_REDIS_CLUSTER_REPLICAS_ADDRS), skipping the test
// without one.
func clusterReplicasAddrs(t *testing.T) []string {
	t.Helper()
	v := os.Getenv("CENTRIFUGE_REDIS_CLUSTER_REPLICAS_ADDRS")
	if v == "" {
		t.Skip("CENTRIFUGE_REDIS_CLUSTER_REPLICAS_ADDRS not set: needs a Redis Cluster with replicas")
	}
	return strings.Split(v, ",")
}

// slotOwners returns the primary and the first replica serving a slot, from
// CLUSTER SLOTS ("" for what it doesn't list: right after a failover the
// slot may be listed without a replica). It asserts nothing, so it can run in
// an Eventually condition.
func slotOwners(c rueidis.Client, slot int64) (primary, replica string) {
	ranges, err := c.Do(context.Background(), c.B().ClusterSlots().Build()).ToArray()
	if err != nil {
		return "", ""
	}
	addr := func(m rueidis.RedisMessage) string {
		node, err := m.ToArray()
		if err != nil || len(node) < 2 {
			return ""
		}
		host, _ := node[0].ToString()
		port, _ := node[1].AsInt64()
		return net.JoinHostPort(host, strconv.FormatInt(port, 10))
	}
	for _, r := range ranges {
		parts, err := r.ToArray()
		if err != nil || len(parts) < 3 {
			continue
		}
		start, _ := parts[0].AsInt64()
		end, _ := parts[1].AsInt64()
		if slot < start || slot > end {
			continue
		}
		primary = addr(parts[2])
		if len(parts) > 3 {
			replica = addr(parts[3])
		}
		return primary, replica
	}
	return "", ""
}

// After a failover the client's slot map may still name the demoted primary,
// now a replica, which accepts SSUBSCRIBE without MOVED. A sharded PUB/SUB
// connection restarted by the SUNSUBSCRIBE of the failover must land on the
// promoted primary, not on the replica: a managed service replacing nodes in
// maintenance removes the replica next, and the subscription would receive
// nothing until the liveness probe (disabled here) gives up.
func TestRedisBroker_ShardedPubSubResubscribesOnPrimaryAfterFailover(t *testing.T) {
	addrs := clusterReplicasAddrs(t)
	node, err := New(Config{})
	require.NoError(t, err)
	s, err := NewRedisShard(node, RedisShardConfig{ClusterAddresses: addrs, IOTimeout: 5 * time.Second})
	require.NoError(t, err)
	t.Cleanup(s.Close)
	b, err := NewRedisBroker(node, RedisBrokerConfig{
		Prefix:                     getUniquePrefix(),
		Shards:                     []*RedisShard{s},
		NumShardedPubSubPartitions: 1,
		pubSubProbeInterval:        -1, // no liveness probe: only the resubscribe itself can recover
	})
	require.NoError(t, err)
	var received atomic.Int64
	require.NoError(t, b.RegisterBrokerEventHandler(&testBrokerEventHandler{
		HandlePublicationFunc: func(string, *Publication, StreamPosition, bool, *Publication) error {
			received.Add(1)
			return nil
		},
	}))
	t.Cleanup(func() { _ = b.Close(context.Background()) })

	ch := "sharded_failover_" + randString(6)
	require.NoError(t, b.Subscribe(ch))
	delivered := func() bool {
		before := received.Load()
		_, err := b.Publish(ch, []byte(`{}`), PublishOptions{})
		return err == nil && received.Load() > before
	}
	require.Eventually(t, delivered, 10*time.Second, 50*time.Millisecond, "no delivery before the failover")

	ctx := context.Background()
	shardCh := string(b.pubSubShardChannelID(0, 0, true))
	slot, err := s.client.Do(ctx, s.client.B().ClusterKeyslot().Key(shardCh).Build()).AsInt64()
	require.NoError(t, err)
	// Right after a failover (a previous test's failback) the slot may be
	// listed without its replica for a moment.
	var oldPrimary, promoted string
	require.Eventually(t, func() bool {
		oldPrimary, promoted = slotOwners(s.client, slot)
		return oldPrimary != "" && promoted != ""
	}, 30*time.Second, 100*time.Millisecond, "slot has no replica")
	nodes := s.client.Nodes()
	require.Contains(t, nodes, promoted)
	require.Contains(t, nodes, oldPrimary)

	// The replica takes over; the old primary becomes its replica and drops
	// its sharded subscribers with SUNSUBSCRIBE.
	t.Cleanup(func() {
		// Give the slot back, so the cluster is as it was for other tests. A
		// replica refuses CLUSTER FAILOVER until it has synced with its new
		// primary: ask again until the slot is back.
		require.Eventually(t, func() bool {
			if p, _ := slotOwners(nodes[oldPrimary], slot); p == oldPrimary {
				return true
			}
			_ = nodes[oldPrimary].Do(ctx, nodes[oldPrimary].B().ClusterFailover().Build()).Error()
			return false
		}, 60*time.Second, time.Second)
	})
	// A replica refuses CLUSTER FAILOVER until it is in sync (after a previous
	// test's failback, for example): ask again until it takes over.
	require.Eventually(t, func() bool {
		if p, _ := slotOwners(nodes[promoted], slot); p == promoted {
			return true
		}
		_ = nodes[promoted].Do(ctx, nodes[promoted].B().ClusterFailover().Build()).Error()
		return false
	}, 60*time.Second, time.Second, "no failover")

	// The subscription is on the promoted primary, not left on the replica.
	subscribers := func(addr string) int64 {
		res, err := nodes[addr].Do(ctx, nodes[addr].B().PubsubShardnumsub().Channel(shardCh).Build()).ToArray()
		if err != nil || len(res) < 2 {
			return -1
		}
		n, _ := res[1].AsInt64()
		return n
	}
	require.Eventually(t, func() bool { return subscribers(promoted) == 1 }, 15*time.Second, 100*time.Millisecond,
		"the sharded subscription isn't on the promoted primary")
	require.Zero(t, subscribers(oldPrimary), "nothing left subscribed on the demoted primary")
	require.Eventually(t, delivered, 10*time.Second, 50*time.Millisecond, "no delivery after the failover")
}
