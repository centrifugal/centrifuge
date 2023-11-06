//go:build integration

package centrifuge

import (
	"context"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func newTestRedisPresenceManager(tb testing.TB, n *Node, useCluster bool) *RedisPresenceManager {
	if useCluster {
		return NewTestRedisPresenceManagerClusterWithPrefix(tb, n, getUniquePrefix())
	}
	return NewTestRedisPresenceManagerWithPrefix(tb, n, getUniquePrefix())
}

func stopRedisPresenceManager(pm *RedisPresenceManager) {
	for _, s := range pm.shards {
		s.Close()
	}
}

func NewTestRedisPresenceManagerWithPrefix(tb testing.TB, n *Node, prefix string) *RedisPresenceManager {
	redisConf := testRedisConf()
	s, err := NewRedisShard(n, redisConf)
	require.NoError(tb, err)
	pm, err := NewRedisPresenceManager(n, RedisPresenceManagerConfig{
		Prefix: prefix,
		Shards: []*RedisShard{s},
	})
	if err != nil {
		tb.Fatal(err)
	}
	n.SetPresenceManager(pm)
	err = n.Run()
	if err != nil {
		tb.Fatal(err)
	}
	return pm
}

func NewTestRedisPresenceManagerClusterWithPrefix(tb testing.TB, n *Node, prefix string) *RedisPresenceManager {
	redisConf := RedisShardConfig{
		ClusterAddresses: []string{"localhost:7000", "localhost:7001", "localhost:7002"},
		IOTimeout:        10 * time.Second,
	}
	s, err := NewRedisShard(n, redisConf)
	require.NoError(tb, err)
	pm, err := NewRedisPresenceManager(n, RedisPresenceManagerConfig{
		Prefix: prefix,
		Shards: []*RedisShard{s},
	})
	if err != nil {
		tb.Fatal(err)
	}
	n.SetPresenceManager(pm)
	err = n.Run()
	if err != nil {
		tb.Fatal(err)
	}
	return pm
}

var redisPresenceTests = []struct {
	Name       string
	UseCluster bool
}{
	{"with_cluster", true},
	{"without_cluster", false},
}

func TestRedisPresenceManager(t *testing.T) {
	for _, tt := range redisPresenceTests {
		t.Run(tt.Name, func(t *testing.T) {
			node := testNode(t)
			pm := newTestRedisPresenceManager(t, node, tt.UseCluster)
			defer func() { _ = node.Shutdown(context.Background()) }()
			defer stopRedisPresenceManager(pm)

			// test adding presence
			require.NoError(t, pm.AddPresence("channel", "uid", &ClientInfo{}))

			p, err := pm.Presence("channel")
			require.NoError(t, err)
			require.Equal(t, 1, len(p))

			s, err := pm.PresenceStats("channel")
			require.NoError(t, err)
			require.Equal(t, 1, s.NumUsers)
			require.Equal(t, 1, s.NumClients)

			err = pm.RemovePresence("channel", "uid")
			require.NoError(t, err)

			p, err = pm.Presence("channel")
			require.NoError(t, err)
			require.Equal(t, 0, len(p))
		})
	}
}

func BenchmarkRedisAddPresence_1Ch(b *testing.B) {
	for _, tt := range benchRedisTests {
		b.Run(tt.Name, func(b *testing.B) {
			node := benchNode(b)
			pm := newTestRedisPresenceManager(b, node, tt.UseCluster)
			defer func() { _ = node.Shutdown(context.Background()) }()
			defer stopRedisPresenceManager(pm)
			b.SetParallelism(getBenchParallelism())
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					err := pm.AddPresence("channel", "uid", &ClientInfo{})
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}

func BenchmarkRedisAddPresence_ManyCh(b *testing.B) {
	for _, tt := range benchRedisTests {
		b.Run(tt.Name, func(b *testing.B) {
			node := benchNode(b)
			pm := newTestRedisPresenceManager(b, node, tt.UseCluster)
			defer func() { _ = node.Shutdown(context.Background()) }()
			defer stopRedisPresenceManager(pm)
			b.SetParallelism(getBenchParallelism())
			j := int32(0)
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					jj := atomic.AddInt32(&j, 1)
					channel := "channel" + strconv.Itoa(int(jj)%benchmarkNumDifferentChannels)
					err := pm.AddPresence(channel, "uid", &ClientInfo{})
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}

func BenchmarkRedisPresence_1Ch(b *testing.B) {
	for _, tt := range benchRedisTests {
		b.Run(tt.Name, func(b *testing.B) {
			node := benchNode(b)
			pm := newTestRedisPresenceManager(b, node, tt.UseCluster)
			defer func() { _ = node.Shutdown(context.Background()) }()
			defer stopRedisPresenceManager(pm)
			b.SetParallelism(getBenchParallelism())
			_ = pm.AddPresence("channel", "uid", &ClientInfo{})
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					_, err := pm.Presence("channel")
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}

func BenchmarkRedisPresence_ManyCh(b *testing.B) {
	for _, tt := range benchRedisTests {
		b.Run(tt.Name, func(b *testing.B) {
			node := benchNode(b)
			pm := newTestRedisPresenceManager(b, node, tt.UseCluster)
			defer func() { _ = node.Shutdown(context.Background()) }()
			defer stopRedisPresenceManager(pm)
			b.SetParallelism(getBenchParallelism())
			_ = pm.AddPresence("channel", "uid", &ClientInfo{})
			j := int32(0)
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					jj := atomic.AddInt32(&j, 1)
					channel := "channel" + strconv.Itoa(int(jj)%benchmarkNumDifferentChannels)
					_, err := pm.Presence(channel)
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}
