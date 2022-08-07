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

func NewTestRedisPresenceManagerWithPrefix(tb testing.TB, n *Node, prefix string) *RedisPresenceManager {
	redisConf := testRedisConf()
	s, err := NewRedisShard(n, redisConf)
	require.NoError(tb, err)
	e, err := NewRedisPresenceManager(n, RedisPresenceManagerConfig{
		Prefix: prefix,
		Shards: []*RedisShard{s},
	})
	if err != nil {
		tb.Fatal(err)
	}
	n.SetPresenceManager(e)
	err = n.Run()
	if err != nil {
		tb.Fatal(err)
	}
	return e
}

func NewTestRedisPresenceManagerClusterWithPrefix(tb testing.TB, n *Node, prefix string) *RedisPresenceManager {
	redisConf := RedisShardConfig{
		ClusterAddresses: []string{"localhost:7000", "localhost:7001", "localhost:7002"},
		Password:         testRedisPassword,

		ReadTimeout: 100 * time.Second,
	}
	s, err := NewRedisShard(n, redisConf)
	require.NoError(tb, err)
	e, err := NewRedisPresenceManager(n, RedisPresenceManagerConfig{
		Prefix: prefix,
		Shards: []*RedisShard{s},
	})
	if err != nil {
		tb.Fatal(err)
	}
	n.SetPresenceManager(e)
	err = n.Run()
	if err != nil {
		tb.Fatal(err)
	}
	return e
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
			e := newTestRedisPresenceManager(t, node, tt.UseCluster)
			defer func() { _ = node.Shutdown(context.Background()) }()

			// test adding presence
			require.NoError(t, e.AddPresence("channel", "uid", &ClientInfo{}))

			p, err := e.Presence("channel")
			require.NoError(t, err)
			require.Equal(t, 1, len(p))

			s, err := e.PresenceStats("channel")
			require.NoError(t, err)
			require.Equal(t, 1, s.NumUsers)
			require.Equal(t, 1, s.NumClients)

			err = e.RemovePresence("channel", "uid")
			require.NoError(t, err)

			p, err = e.Presence("channel")
			require.NoError(t, err)
			require.Equal(t, 0, len(p))
		})
	}
}

func BenchmarkRedisAddPresence_1Ch(b *testing.B) {
	for _, tt := range benchRedisTests {
		b.Run(tt.Name, func(b *testing.B) {
			node := testNode(b)
			e := newTestRedisPresenceManager(b, node, tt.UseCluster)
			defer func() { _ = node.Shutdown(context.Background()) }()
			b.SetParallelism(128)
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					err := e.AddPresence("channel", "uid", &ClientInfo{})
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
			node := testNode(b)
			e := newTestRedisPresenceManager(b, node, tt.UseCluster)
			defer func() { _ = node.Shutdown(context.Background()) }()
			b.SetParallelism(128)
			_ = e.AddPresence("channel", "uid", &ClientInfo{})
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					_, err := e.Presence("channel")
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
			node := testNode(b)
			e := newTestRedisPresenceManager(b, node, tt.UseCluster)
			defer func() { _ = node.Shutdown(context.Background()) }()
			b.SetParallelism(128)
			_ = e.AddPresence("channel", "uid", &ClientInfo{})
			j := int32(0)
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					jj := atomic.AddInt32(&j, 1)
					channel := "channel" + strconv.Itoa(int(jj)%benchmarkNumDifferentChannels)
					_, err := e.Presence(channel)
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}
