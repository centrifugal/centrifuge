//go:build integration

package centrifuge

import (
	"context"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// newTwinTestBroker is newLineageTestBroker with the twin of the write
// counter switchable.
func newTwinTestBroker(t *testing.T, tt lineageRedisTest, twinOff bool) *RedisBroker {
	t.Helper()
	node := testNode(t)
	var conf RedisShardConfig
	if tt.UseCluster {
		var addrs []string
		for i := 0; i < 3; i++ {
			addrs = append(addrs, net.JoinHostPort("127.0.0.1", strconv.Itoa(tt.Port+i)))
		}
		conf = RedisShardConfig{ClusterAddresses: addrs, IOTimeout: 10 * time.Second}
	} else {
		conf = testSingleRedisConf(tt.Port)
	}
	s, err := NewRedisShard(node, conf)
	require.NoError(t, err)
	s.lineage.twinDisabledForTest = twinOff
	b, err := NewRedisBroker(node, RedisBrokerConfig{Prefix: getUniquePrefix(), Shards: []*RedisShard{s}})
	require.NoError(t, err)
	node.SetBroker(b)
	require.NoError(t, node.Run())
	t.Cleanup(func() {
		_ = node.Shutdown(context.Background())
		stopRedisBroker(b)
	})
	return b
}

// twinKey is the lineage key of a channel's slot, holding the write counter
// and its twin.
func twinKey(b *RedisBroker, ch string) string {
	s := b.shards[0].shard
	return s.lineage.tokenKey(string(b.historyMetaKey(s, ch)))
}

func twinFields(t *testing.T, b *RedisBroker, ch string) (string, string) {
	t.Helper()
	s := b.shards[0].shard
	vals, err := s.client.Do(context.Background(), s.client.B().Hmget().Key(twinKey(b, ch)).Field("c", "l").Build()).ToArray()
	require.NoError(t, err)
	c, _ := vals[0].ToString()
	l, _ := vals[1].ToString()
	return c, l
}

// gateTests are the setups with the twin: one address (not Redis Cluster).
func gateTests() []lineageRedisTest {
	var out []lineageRedisTest
	for _, tt := range lineageRedisTests {
		if tt.Gate && !tt.UseCluster {
			out = append(out, tt)
		}
	}
	return out
}

// Redis Cluster (and Sentinel) don't use the twin: writes skip its cost.
func TestRedisLineage_TwinOffInCluster(t *testing.T) {
	for _, tt := range lineageRedisTests {
		if !tt.Gate || !tt.UseCluster {
			continue
		}
		t.Run(tt.Name, func(t *testing.T) {
			b := newTwinTestBroker(t, tt, false)
			lineagePublish(t, b, "twin_cluster_"+randString(6), 1)
			require.False(t, b.shards[0].shard.lineage.twinOn())
		})
	}
}

// Every write bumps the counter and its local twin together.
func TestRedisLineage_TwinCountsWrites(t *testing.T) {
	for _, tt := range gateTests() {
		t.Run(tt.Name, func(t *testing.T) {
			b := newTwinTestBroker(t, tt, false)
			ch := "twin_count_" + randString(6)
			lineagePublish(t, b, ch, 1)
			require.True(t, b.shards[0].shard.lineage.twinOn())
			c0, _ := twinFields(t, b, ch)
			before, _ := strconv.Atoi(c0)
			lineagePublish(t, b, ch, 3)
			c, l := twinFields(t, b, ch)
			require.Equal(t, c, l)
			after, _ := strconv.Atoi(c)
			require.GreaterOrEqual(t, after-before, 3)
		})
	}
}

// Data that came from another server (the local twin behind or missing)
// starts a new epoch, although the server holds our scripts and marker.
func TestRedisLineage_TwinCopiedDataStartsNewEpoch(t *testing.T) {
	for _, tt := range gateTests() {
		for _, how := range []string{"behind", "missing"} {
			t.Run(tt.Name+"_"+how, func(t *testing.T) {
				b := newTwinTestBroker(t, tt, false)
				s := b.shards[0].shard
				ch := "twin_copied_" + randString(6)
				before := lineagePublish(t, b, ch, 2)
				ctx := context.Background()
				if how == "behind" {
					require.NoError(t, s.client.Do(ctx, s.client.B().Hset().Key(twinKey(b, ch)).FieldValue().FieldValue("l", "0").Build()).Error())
				} else {
					require.NoError(t, s.client.Do(ctx, s.client.B().Hdel().Key(twinKey(b, ch)).Field("l").Build()).Error())
				}
				rot0, _, _ := lineageStatsOf(b)
				after := lineagePublish(t, b, ch, 1)
				require.NotEqual(t, before.Epoch, after.Epoch, "a new epoch")
				require.Equal(t, uint64(1), after.Offset)
				rot1, _, _ := lineageStatsOf(b)
				require.Equal(t, rot0+1, rot1)
				c, l := twinFields(t, b, ch)
				require.Equal(t, c, l, "the rotation rebased the twin")
			})
		}
	}
}

// An evicted lineage key (token and counters gone together) is not caught by
// the twin: the server's scripts and marker show its lineage is intact.
func TestRedisLineage_TwinEvictedKeepsEpoch(t *testing.T) {
	for _, tt := range gateTests() {
		t.Run(tt.Name, func(t *testing.T) {
			b := newTwinTestBroker(t, tt, false)
			s := b.shards[0].shard
			ch := "twin_evicted_" + randString(6)
			before := lineagePublish(t, b, ch, 2)
			require.NoError(t, s.client.Do(context.Background(), s.client.B().Del().Key(twinKey(b, ch)).Build()).Error())
			after := lineagePublish(t, b, ch, 1)
			require.Equal(t, before.Epoch, after.Epoch)
			require.Equal(t, uint64(3), after.Offset)
		})
	}
}

// Many concurrent calls finding copied data rotate the server once.
func TestRedisLineage_TwinConcurrentRotatesOnce(t *testing.T) {
	for _, tt := range gateTests() {
		t.Run(tt.Name, func(t *testing.T) {
			b := newTwinTestBroker(t, tt, false)
			s := b.shards[0].shard
			ch := "twin_concurrent_" + randString(6)
			lineagePublish(t, b, ch, 1)
			require.NoError(t, s.client.Do(context.Background(), s.client.B().Hset().Key(twinKey(b, ch)).FieldValue().FieldValue("l", "0").Build()).Error())
			rot0, _, _ := lineageStatsOf(b)
			var wg sync.WaitGroup
			epochs := make([]string, 20)
			for i := range epochs {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					res, err := b.Publish(ch, []byte(`{}`), PublishOptions{HistorySize: 100, HistoryTTL: time.Minute})
					require.NoError(t, err)
					epochs[i] = res.StreamPosition.Epoch
				}(i)
			}
			wg.Wait()
			rot1, _, _ := lineageStatsOf(b)
			require.Equal(t, rot0+1, rot1, "one rotation")
			for _, e := range epochs {
				require.Equal(t, epochs[0], e, "one new epoch")
			}
		})
	}
}

// Control: with the twin off the same copied data goes unnoticed.
func TestRedisLineage_TwinControl(t *testing.T) {
	tt := lineageRedisTests[0]
	b := newTwinTestBroker(t, tt, true)
	ch := "twin_control_" + randString(6)
	before := lineagePublish(t, b, ch, 2)
	require.False(t, b.shards[0].shard.lineage.twinOn())
	s := b.shards[0].shard
	require.NoError(t, s.client.Do(context.Background(), s.client.B().Hset().Key(twinKey(b, ch)).FieldValue().FieldValue("c", "7").FieldValue("l", "1").Build()).Error())
	after := lineagePublish(t, b, ch, 1)
	require.Equal(t, before.Epoch, after.Epoch)
}
