//go:build integration

package centrifuge

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func newTestRedisPresenceManager(tb testing.TB, n *Node, useCluster bool, userMapping bool) *RedisPresenceManager {
	if useCluster {
		return NewTestRedisPresenceManagerClusterWithPrefix(tb, n, getUniquePrefix(), userMapping)
	}
	return NewTestRedisPresenceManagerWithPrefix(tb, n, getUniquePrefix(), userMapping)
}

func stopRedisPresenceManager(pm *RedisPresenceManager) {
	for _, s := range pm.shards {
		s.Close()
	}
}

func NewTestRedisPresenceManagerWithPrefix(tb testing.TB, n *Node, prefix string, userMapping bool) *RedisPresenceManager {
	redisConf := testRedisConf()
	s, err := NewRedisShard(n, redisConf)
	require.NoError(tb, err)
	pm, err := NewRedisPresenceManager(n, RedisPresenceManagerConfig{
		Prefix: prefix,
		Shards: []*RedisShard{s},
		EnableUserMapping: func(_ string) bool {
			return userMapping
		},
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

func NewTestRedisPresenceManagerClusterWithPrefix(tb testing.TB, n *Node, prefix string, userMapping bool) *RedisPresenceManager {
	redisConf := RedisShardConfig{
		ClusterAddresses: []string{"localhost:7000", "localhost:7001", "localhost:7002"},
		IOTimeout:        10 * time.Second,
	}
	s, err := NewRedisShard(n, redisConf)
	require.NoError(tb, err)
	pm, err := NewRedisPresenceManager(n, RedisPresenceManagerConfig{
		Prefix: prefix,
		Shards: []*RedisShard{s},
		EnableUserMapping: func(_ string) bool {
			return userMapping
		},
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
			pm := newTestRedisPresenceManager(t, node, tt.UseCluster, false)
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

			err = pm.RemovePresence("channel", "uid", "")
			require.NoError(t, err)

			p, err = pm.Presence("channel")
			require.NoError(t, err)
			require.Equal(t, 0, len(p))
		})
	}
}

func TestRedisPresenceManagerWithUserMapping(t *testing.T) {
	for _, tt := range redisPresenceTests {
		t.Run(tt.Name, func(t *testing.T) {
			node := testNode(t)
			pm := newTestRedisPresenceManager(t, node, tt.UseCluster, true)
			defer func() { _ = node.Shutdown(context.Background()) }()
			defer stopRedisPresenceManager(pm)

			// adding presence for the first time.
			require.NoError(t, pm.AddPresence("channel", "uid", &ClientInfo{
				ClientID: "uid",
				UserID:   "1",
			}))

			// same conn, same user.
			require.NoError(t, pm.AddPresence("channel", "uid", &ClientInfo{
				ClientID: "uid",
				UserID:   "1",
			}))

			stats, err := pm.PresenceStats("channel")
			require.NoError(t, err)
			require.Equal(t, 1, stats.NumClients)
			require.Equal(t, 1, stats.NumUsers)

			// same user, different conn
			require.NoError(t, pm.AddPresence("channel", "uid-2", &ClientInfo{
				ClientID: "uid-2",
				UserID:   "1",
			}))

			stats, err = pm.PresenceStats("channel")
			require.NoError(t, err)
			require.Equal(t, 2, stats.NumClients)
			require.Equal(t, 1, stats.NumUsers)

			// different user, different conn
			require.NoError(t, pm.AddPresence("channel", "uid-3", &ClientInfo{
				ClientID: "uid-3",
				UserID:   "2",
			}))

			stats, err = pm.PresenceStats("channel")
			require.NoError(t, err)
			require.Equal(t, 3, stats.NumClients)
			require.Equal(t, 2, stats.NumUsers)

			err = pm.RemovePresence("channel", "uid", "1")
			require.NoError(t, err)

			stats, err = pm.PresenceStats("channel")
			require.NoError(t, err)
			require.Equal(t, 2, stats.NumClients)
			require.Equal(t, 2, stats.NumUsers)

			err = pm.RemovePresence("channel", "uid-2", "1")
			require.NoError(t, err)

			stats, err = pm.PresenceStats("channel")
			require.NoError(t, err)
			require.Equal(t, 1, stats.NumClients)
			require.Equal(t, 1, stats.NumUsers)

			err = pm.RemovePresence("channel", "uid-3", "2")
			require.NoError(t, err)

			stats, err = pm.PresenceStats("channel")
			require.NoError(t, err)
			require.Equal(t, 0, stats.NumClients)
			require.Equal(t, 0, stats.NumUsers)
		})
	}
}

func TestRedisPresenceManagerWithUserMappingExpire(t *testing.T) {
	t.Parallel()
	for _, tt := range redisPresenceTests {
		t.Run(tt.Name, func(t *testing.T) {
			t.Parallel()
			node := testNode(t)
			pm := newTestRedisPresenceManager(t, node, tt.UseCluster, true)
			pm.config.PresenceTTL = 2 * time.Second
			defer func() { _ = node.Shutdown(context.Background()) }()
			defer stopRedisPresenceManager(pm)

			// adding presence for the first time.
			require.NoError(t, pm.AddPresence("channel", "uid", &ClientInfo{
				ClientID: "uid",
				UserID:   "1",
			}))
			// same user, different conn
			require.NoError(t, pm.AddPresence("channel", "uid-2", &ClientInfo{
				ClientID: "uid-2",
				UserID:   "1",
			}))
			// different user, different conn
			require.NoError(t, pm.AddPresence("channel", "uid-3", &ClientInfo{
				ClientID: "uid-3",
				UserID:   "2",
			}))

			stats, err := pm.PresenceStats("channel")
			require.NoError(t, err)
			require.Equal(t, 3, stats.NumClients)
			require.Equal(t, 2, stats.NumUsers)

			timer := time.NewTimer(2 * time.Second)
		LOOP:
			for {
				select {
				case <-timer.C:
					break LOOP
				case <-time.After(500 * time.Millisecond):
					// keep one entry.
					require.NoError(t, pm.AddPresence("channel", "uid-2", &ClientInfo{
						ClientID: "uid-2",
						UserID:   "1",
					}))
				}
			}

			stats, err = pm.PresenceStats("channel")
			require.NoError(t, err)
			require.Equal(t, 1, stats.NumClients)
			require.Equal(t, 1, stats.NumUsers)

			time.Sleep(2 * time.Second)

			stats, err = pm.PresenceStats("channel")
			require.NoError(t, err)
			require.Equal(t, 0, stats.NumClients)
			require.Equal(t, 0, stats.NumUsers)
		})
	}
}

func BenchmarkRedisAddPresence_1Ch(b *testing.B) {
	for _, tt := range benchRedisTests {
		b.Run(tt.Name, func(b *testing.B) {
			node := benchNode(b)
			pm := newTestRedisPresenceManager(b, node, tt.UseCluster, false)
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
			pm := newTestRedisPresenceManager(b, node, tt.UseCluster, false)
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
			pm := newTestRedisPresenceManager(b, node, tt.UseCluster, false)
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
			pm := newTestRedisPresenceManager(b, node, tt.UseCluster, false)
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

func BenchmarkRedisPresenceStatsWithMapping(b *testing.B) {
	node := benchNode(b)
	pm := newTestRedisPresenceManager(b, node, false, true)
	defer func() { _ = node.Shutdown(context.Background()) }()
	defer stopRedisPresenceManager(pm)
	b.SetParallelism(getBenchParallelism())

	sem := make(chan struct{}, 100)
	numClients := 100_000

	var wg sync.WaitGroup
	wg.Add(numClients)
	for i := 0; i < numClients; i++ {
		sem <- struct{}{}
		i := i
		go func() {
			defer wg.Done()
			defer func() {
				<-sem
			}()
			clientID := "uid" + strconv.Itoa(i)
			userID := "user" + strconv.Itoa(i)
			_ = pm.AddPresence("channel", "uid"+strconv.Itoa(i), &ClientInfo{
				ClientID: clientID,
				UserID:   userID,
			})
		}()
	}
	wg.Wait()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			s, err := pm.PresenceStats("channel")
			if err != nil {
				b.Fatal(err)
			}
			require.Equal(b, s.NumClients, numClients)
			require.Equal(b, s.NumUsers, numClients)
		}
	})
}
