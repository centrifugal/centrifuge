//go:build integration

package centrifuge

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/centrifugal/protocol"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func getUniquePrefix() string {
	return "centrifuge-test-" + randString(3) + "-" + strconv.FormatInt(time.Now().UnixNano(), 10)
}

func newTestRedisBroker(tb testing.TB, n *Node, useStreams bool, useCluster bool, port int) *RedisBroker {
	if useCluster {
		return NewTestRedisBrokerCluster(tb, n, getUniquePrefix(), useStreams, port)
	}
	return NewTestRedisBroker(tb, n, getUniquePrefix(), useStreams, port)
}

func testNode(tb testing.TB) *Node {
	node, err := New(Config{
		LogLevel:   LogLevelDebug,
		LogHandler: func(entry LogEntry) {},
	})
	require.NoError(tb, err)
	return node
}

func benchNode(tb testing.TB) *Node {
	node, err := New(Config{
		LogLevel:   LogLevelError,
		LogHandler: func(entry LogEntry) {},
	})
	require.NoError(tb, err)
	return node
}

func testSingleRedisConf(port int) RedisShardConfig {
	if port == 0 {
		port = 6379
	}
	return RedisShardConfig{
		Address:        "127.0.0.1:" + strconv.Itoa(port),
		IOTimeout:      10 * time.Second,
		ConnectTimeout: 10 * time.Second,
	}
}

func NewTestRedisBroker(tb testing.TB, n *Node, prefix string, useStreams bool, port int) *RedisBroker {
	tb.Helper()
	redisConf := testSingleRedisConf(port)
	s, err := NewRedisShard(n, redisConf)
	require.NoError(tb, err)
	require.Equal(tb, s.Mode(), RedisShardModeStandalone)
	e, err := NewRedisBroker(n, RedisBrokerConfig{
		Prefix:   prefix,
		UseLists: !useStreams,
		Shards:   []*RedisShard{s},
	})
	require.NoError(tb, err)
	n.SetBroker(e)
	err = n.Run()
	require.NoError(tb, err)
	return e
}

func NewTestRedisBrokerCluster(tb testing.TB, n *Node, prefix string, useStreams bool, port int) *RedisBroker {
	tb.Helper()

	numClusterNodes := 3
	numClusterNodesStr := os.Getenv("CENTRIFUGE_REDIS_CLUSTER_BENCHMARKS_NUM_NODES")
	if numClusterNodesStr != "" {
		num, err := strconv.Atoi(numClusterNodesStr)
		require.NoError(tb, err)
		numClusterNodes = num
	}

	var clusterAddresses []string

	for i := 0; i < numClusterNodes; i++ {
		clusterAddresses = append(clusterAddresses, net.JoinHostPort("127.0.0.1", strconv.Itoa(port+i)))
	}

	redisConf := RedisShardConfig{
		ClusterAddresses: clusterAddresses,
		IOTimeout:        10 * time.Second,
	}
	s, err := NewRedisShard(n, redisConf)
	require.NoError(tb, err)
	require.Equal(tb, s.Mode(), RedisShardModeCluster)
	brokerConfig := RedisBrokerConfig{
		Prefix:   prefix,
		UseLists: !useStreams,
		Shards:   []*RedisShard{s},
	}

	numClusterShardsStr := os.Getenv("CENTRIFUGE_REDIS_CLUSTER_BENCHMARKS_NUM_SHARDS")
	if numClusterShardsStr != "" {
		num, err := strconv.Atoi(numClusterShardsStr)
		require.NoError(tb, err)
		brokerConfig.NumShardedPubSubPartitions = num
	}

	e, err := NewRedisBroker(n, brokerConfig)
	if err != nil {
		tb.Fatal(err)
	}
	n.SetBroker(e)
	err = n.Run()
	require.NoError(tb, err)
	return e
}

func NewTestRedisBrokerSentinel(tb testing.TB) *RedisBroker {
	tb.Helper()
	n, _ := New(Config{})
	redisConf := RedisShardConfig{
		SentinelAddresses:  []string{"127.0.0.1:26379"},
		SentinelMasterName: "mymaster",
		IOTimeout:          10 * time.Second,
		ConnectTimeout:     10 * time.Second,
	}
	s, err := NewRedisShard(n, redisConf)
	require.NoError(tb, err)
	require.Equal(tb, s.Mode(), RedisShardModeSentinel)
	e, err := NewRedisBroker(n, RedisBrokerConfig{
		Shards: []*RedisShard{s},
	})
	if err != nil {
		tb.Fatal(err)
	}
	n.SetBroker(e)
	err = n.Run()
	require.NoError(tb, err)
	return e
}

func stopRedisBroker(b *RedisBroker) {
	_ = b.Close(context.Background())
	for _, s := range b.shards {
		s.shard.Close()
	}
}

func TestRedisBroker_NoShards(t *testing.T) {
	n, _ := New(Config{})
	_, err := NewRedisBroker(n, RedisBrokerConfig{})
	require.Error(t, err)
}

func TestRedisBrokerSentinel(t *testing.T) {
	b := NewTestRedisBrokerSentinel(t)
	defer stopRedisBroker(b)
	_, _, err := b.History("test", HistoryOptions{
		Filter: HistoryFilter{
			Limit: -1,
		},
	})
	require.NoError(t, err)
}

type historyRedisTest struct {
	Name       string
	UseStreams bool
	UseCluster bool
	Port       int
}

var historyRedisTests = []historyRedisTest{
	{"rd_single_list", false, false, 6379},
	{"vk_single_list", false, false, 16379},
	{"rd_single_strm", true, false, 6379},
	{"vk_single_strm", true, false, 16379},
	{"df_single_list", false, false, 36379},
	{"df_single_strm", true, false, 36379},
	{"rd_cluster_list", false, true, 7000},
	{"rd_cluster_strm", true, true, 7000},
	{"vk_cluster_strm", true, true, 17000},
}

type noHistoryRedisTest struct {
	Name       string
	UseCluster bool
	Port       int
}

var noHistoryRedisTests = []noHistoryRedisTest{
	{"rd_single", false, 6379},
	{"df_single", false, 36379},
	{"vk_single", false, 16379},
	{"rd_cluster", false, 0},
}

var historyBenchRedisTests = func() (tests []historyRedisTest) {
	for _, t := range historyRedisTests {
		if os.Getenv("CENTRIFUGE_REDIS_CLUSTER_BENCHMARKS") == "" && t.UseCluster {
			continue
		}
		tests = append(tests, t)
	}
	return
}()

var noHistoryBenchRedisTests = func() (tests []noHistoryRedisTest) {
	for _, t := range noHistoryRedisTests {
		if os.Getenv("CENTRIFUGE_REDIS_CLUSTER_BENCHMARKS") == "" && t.UseCluster {
			continue
		}
		tests = append(tests, t)
	}
	return
}()

func excludeNoHistoryClusterTests(tests []noHistoryRedisTest) []noHistoryRedisTest {
	var res []noHistoryRedisTest
	for _, t := range tests {
		if t.UseCluster {
			continue
		}
		res = append(res, t)
	}
	return res
}

func TestRedisBroker(t *testing.T) {
	for _, tt := range historyRedisTests {
		t.Run(tt.Name, func(t *testing.T) {
			node := testNode(t)

			b := newTestRedisBroker(t, node, tt.UseStreams, tt.UseCluster, tt.Port)
			defer func() { _ = node.Shutdown(context.Background()) }()
			defer stopRedisBroker(b)

			_, _, err := b.Publish("channel", testPublicationData(), PublishOptions{})
			require.NoError(t, err)
			_, _, err = b.Publish("channel", testPublicationData(), PublishOptions{})
			require.NoError(t, err)
			require.NoError(t, b.Subscribe("channel"))
			require.NoError(t, b.Unsubscribe("channel"))

			rawData := []byte("{}")

			// test adding history
			_, _, err = b.Publish("channel", rawData, PublishOptions{HistorySize: 4, HistoryTTL: time.Second})
			require.NoError(t, err)
			pubs, _, err := b.History("channel", HistoryOptions{
				Filter: HistoryFilter{
					Limit: -1,
				},
			})
			require.NoError(t, err)
			require.Equal(t, 1, len(pubs))
			require.Equal(t, pubs[0].Data, []byte("{}"))

			// test history limit
			_, _, err = b.Publish("channel", rawData, PublishOptions{HistorySize: 4, HistoryTTL: time.Second})
			require.NoError(t, err)
			_, _, err = b.Publish("channel", rawData, PublishOptions{HistorySize: 4, HistoryTTL: time.Second})
			require.NoError(t, err)
			_, _, err = b.Publish("channel", rawData, PublishOptions{HistorySize: 4, HistoryTTL: time.Second})
			require.NoError(t, err)
			pubs, _, err = b.History("channel", HistoryOptions{
				Filter: HistoryFilter{
					Limit: 2,
				},
			})
			require.NoError(t, err)
			require.Equal(t, 2, len(pubs))

			// test history limit greater than history size
			_, _, err = b.Publish("channel", rawData, PublishOptions{HistorySize: 1, HistoryTTL: time.Second})
			require.NoError(t, err)
			_, _, err = b.Publish("channel", rawData, PublishOptions{HistorySize: 1, HistoryTTL: time.Second})
			require.NoError(t, err)
			_, _, err = b.Publish("channel", rawData, PublishOptions{HistorySize: 1, HistoryTTL: time.Second})
			require.NoError(t, err)

			// ask all history.
			pubs, _, err = b.History("channel", HistoryOptions{
				Filter: HistoryFilter{
					Limit: -1,
				},
			})
			require.NoError(t, err)
			require.Equal(t, 1, len(pubs))

			// ask more history than history_size.
			pubs, _, err = b.History("channel", HistoryOptions{
				Filter: HistoryFilter{
					Limit: 2,
				},
			})
			require.NoError(t, err)
			require.Equal(t, 1, len(pubs))

			// test publishing control message.
			err = b.PublishControl([]byte(""), "", "")
			require.NoError(t, nil, err)

			// test publishing control message.
			err = b.PublishControl([]byte(""), "test", "")
			require.NoError(t, nil, err)

			require.NoError(t, b.PublishJoin("channel", &ClientInfo{}))
			require.NoError(t, b.PublishLeave("channel", &ClientInfo{}))

			// TODO: test resubscribe upon broker temporary unavailability.
			b.logResubscribed(3, time.Second, map[string]any{"test": "case"})
		})
	}
}

func TestRedisBrokerPublishNoPubSub(t *testing.T) {
	node := testNode(t)
	defer func() { _ = node.Shutdown(context.Background()) }()

	redisConf := testSingleRedisConf(6379)
	s, err := NewRedisShard(node, redisConf)
	require.NoError(t, err)
	b, err := NewRedisBroker(node, RedisBrokerConfig{
		Prefix:     uuid.NewString(),
		Shards:     []*RedisShard{s},
		SkipPubSub: true,
	})
	require.NoError(t, err)
	node.SetBroker(b)
	err = node.Run()
	require.NoError(t, err)
	sp, _, err := b.Publish("channel", []byte(`{}`), PublishOptions{
		HistorySize: 4,
		HistoryTTL:  time.Second,
	})
	require.NoError(t, err)
	require.True(t, sp.Offset > 0)
}

func TestRedisBrokerPublishIdempotent(t *testing.T) {
	for _, tt := range historyRedisTests {
		t.Run(tt.Name, func(t *testing.T) {
			node := testNode(t)

			b := newTestRedisBroker(t, node, tt.UseStreams, tt.UseCluster, tt.Port)
			defer func() { _ = node.Shutdown(context.Background()) }()
			defer stopRedisBroker(b)

			_, _, err := b.Publish("channel", testPublicationData(), PublishOptions{
				IdempotencyKey: "publish_no_history",
			})
			require.NoError(t, err)

			_, _, err = b.Publish("channel", testPublicationData(), PublishOptions{
				IdempotencyKey: "publish_no_history",
			})
			require.NoError(t, err)

			rawData := []byte("{}")

			// test adding history
			sp1, _, err := b.Publish("channel", rawData, PublishOptions{
				HistorySize:    4,
				HistoryTTL:     time.Second,
				IdempotencyKey: "publish_with_history",
			})
			require.NoError(t, err)
			pubs, _, err := b.History("channel", HistoryOptions{
				Filter: HistoryFilter{
					Limit: -1,
				},
			})
			require.NoError(t, err)
			require.Equal(t, 1, len(pubs))

			// test publish with  history and same idempotency key.
			sp2, _, err := b.Publish("channel", rawData, PublishOptions{
				HistorySize:    4,
				HistoryTTL:     time.Second,
				IdempotencyKey: "publish_with_history",
			})
			require.NoError(t, err)
			pubs, _, err = b.History("channel", HistoryOptions{
				Filter: HistoryFilter{
					Limit: -1,
				},
			})
			require.NoError(t, err)
			require.Equal(t, 1, len(pubs))

			require.Equal(t, sp1, sp2)
		})
	}
}

func TestRedisBrokerPublishSkipOldVersion(t *testing.T) {
	for _, tt := range historyRedisTests {
		t.Run(tt.Name, func(t *testing.T) {
			if !tt.UseStreams {
				// Does not work with lists.
				t.Skip("Skip test for Redis lists - not implemented")
			}

			node := testNode(t)

			b := newTestRedisBroker(t, node, tt.UseStreams, tt.UseCluster, tt.Port)
			defer func() { _ = node.Shutdown(context.Background()) }()
			defer stopRedisBroker(b)

			channel := uuid.New().String()

			_, _, err := b.Publish(channel, testPublicationData(), PublishOptions{
				HistorySize: 2,
				HistoryTTL:  5 * time.Second,
				Version:     1,
			})
			require.NoError(t, err)
			_, _, err = b.Publish(channel, testPublicationData(), PublishOptions{
				HistorySize: 2,
				HistoryTTL:  5 * time.Second,
				Version:     1,
			})
			require.NoError(t, err)
			pubs, _, err := b.History(channel, HistoryOptions{
				Filter: HistoryFilter{
					Limit: -1,
				},
			})
			require.NoError(t, err)
			require.Equal(t, 1, len(pubs))

			channel2 := uuid.NewString()
			// Test publish with history and with version and version epoch.
			_, _, err = b.Publish(channel2, testPublicationData(), PublishOptions{
				HistorySize:  2,
				HistoryTTL:   5 * time.Second,
				Version:      1,
				VersionEpoch: "xyz",
			})
			require.NoError(t, err)
			// Publish with same version and epoch.
			_, _, err = b.Publish(channel2, testPublicationData(), PublishOptions{
				HistorySize:  2,
				HistoryTTL:   5 * time.Second,
				Version:      1,
				VersionEpoch: "xyz",
			})
			require.NoError(t, err)
			pubs, _, err = b.History(channel2, HistoryOptions{
				Filter: HistoryFilter{
					Limit: -1,
				},
			})
			require.NoError(t, err)
			require.Equal(t, 1, len(pubs))
			// Publish with same version and different epoch.
			_, _, err = b.Publish(channel2, testPublicationData(), PublishOptions{
				HistorySize:  2,
				HistoryTTL:   time.Second,
				Version:      1,
				VersionEpoch: "aaa",
			})
			require.NoError(t, err)
			pubs, _, err = b.History(channel2, HistoryOptions{
				Filter: HistoryFilter{
					Limit: -1,
				},
			})
			require.NoError(t, err)
			require.Equal(t, 2, len(pubs))
		})
	}
}

func TestRedisCurrentPosition(t *testing.T) {
	for _, tt := range historyRedisTests {
		t.Run(tt.Name, func(t *testing.T) {
			node := testNode(t)
			b := newTestRedisBroker(t, node, tt.UseStreams, tt.UseCluster, tt.Port)
			defer func() { _ = node.Shutdown(context.Background()) }()
			defer stopRedisBroker(b)

			channel := "test-current-position"

			_, streamTop, err := b.History(channel, HistoryOptions{
				Filter: HistoryFilter{
					Limit: 0,
				},
			})
			require.NoError(t, err)
			require.Equal(t, uint64(0), streamTop.Offset)

			rawData := []byte("{}")
			_, _, err = b.Publish(channel, rawData, PublishOptions{HistorySize: 10, HistoryTTL: 2 * time.Second})
			require.NoError(t, err)

			_, streamTop, err = b.History(channel, HistoryOptions{
				Filter: HistoryFilter{
					Limit: 0,
				},
			})
			require.NoError(t, err)
			require.Equal(t, uint64(1), streamTop.Offset)
		})
	}
}

func TestRedisBrokerRecover(t *testing.T) {
	for _, tt := range historyRedisTests {
		t.Run(tt.Name, func(t *testing.T) {
			node := testNode(t)
			b := newTestRedisBroker(t, node, tt.UseStreams, tt.UseCluster, tt.Port)
			defer func() { _ = node.Shutdown(context.Background()) }()
			defer stopRedisBroker(b)

			rawData := []byte("{}")

			for i := 0; i < 5; i++ {
				_, _, err := b.Publish("channel", rawData, PublishOptions{HistorySize: 10, HistoryTTL: 2 * time.Second})
				require.NoError(t, err)
			}

			_, streamTop, err := b.History("channel", HistoryOptions{
				Filter: HistoryFilter{
					Limit: 0,
					Since: nil,
				},
			})
			require.NoError(t, err)

			pubs, _, err := b.History("channel", HistoryOptions{
				Filter: HistoryFilter{
					Limit: -1,
					Since: &StreamPosition{Offset: 2, Epoch: streamTop.Epoch},
				},
			})
			require.NoError(t, err)
			require.Equal(t, 3, len(pubs))
			require.Equal(t, uint64(3), pubs[0].Offset)
			require.Equal(t, uint64(4), pubs[1].Offset)
			require.Equal(t, uint64(5), pubs[2].Offset)

			for i := 0; i < 10; i++ {
				_, _, err := b.Publish("channel", rawData, PublishOptions{HistorySize: 10, HistoryTTL: 2 * time.Second})
				require.NoError(t, err)
			}

			pubs, _, err = b.History("channel", HistoryOptions{
				Filter: HistoryFilter{
					Limit: -1,
					Since: &StreamPosition{Offset: 0, Epoch: streamTop.Epoch},
				},
			})
			require.NoError(t, err)
			require.Equal(t, 10, len(pubs))

			pubs, _, err = b.History("channel", HistoryOptions{
				Filter: HistoryFilter{
					Limit: -1,
					Since: &StreamPosition{Offset: 100, Epoch: streamTop.Epoch},
				},
			})
			require.NoError(t, err)
			require.Equal(t, 0, len(pubs))

			require.NoError(t, b.RemoveHistory("channel"))
			pubs, _, err = b.History("channel", HistoryOptions{
				Filter: HistoryFilter{
					Limit: -1,
					Since: &StreamPosition{Offset: 2, Epoch: streamTop.Epoch},
				},
			})
			require.NoError(t, err)
			require.Equal(t, 0, len(pubs))
		})
	}
}

func pubSubChannels(t *testing.T, e *RedisBroker) ([]string, error) {
	t.Helper()
	client := e.shards[0].shard.client
	return client.Do(context.Background(), client.B().PubsubChannels().Pattern(e.messagePrefix+"*").Build()).AsStrSlice()
}

func TestRedisBrokerSubscribeUnsubscribe(t *testing.T) {
	for _, tt := range noHistoryRedisTests {
		t.Run(tt.Name, func(t *testing.T) {
			// Custom prefix to not collide with other tests.
			node := testNode(t)
			b := NewTestRedisBroker(t, node, getUniquePrefix(), false, 0)
			defer func() { _ = node.Shutdown(context.Background()) }()
			defer stopRedisBroker(b)

			if b.shards[0].shard.isCluster {
				t.Skip("Channels command is not supported when Redis Cluster is used")
			}

			require.NoError(t, b.Subscribe("1-test"))
			require.NoError(t, b.Subscribe("1-test"))
			channels, err := pubSubChannels(t, b)
			require.NoError(t, err)
			if len(channels) != 1 {
				// Redis PUBSUB CHANNELS command looks like eventual consistent, so sometimes
				// it returns wrong results, sleeping for a while helps in such situations.
				// See https://gist.github.com/FZambia/80a5241e06b4662f7fe89cfaf24072c3
				time.Sleep(2000 * time.Millisecond)
				channels, err := pubSubChannels(t, b)
				require.NoError(t, err)
				require.Equal(t, 1, len(channels), fmt.Sprintf("%#v", channels))
			}

			require.NoError(t, b.Unsubscribe("1-test"))
			channels, err = pubSubChannels(t, b)
			require.NoError(t, err)
			if len(channels) != 0 {
				time.Sleep(2000 * time.Millisecond)
				channels, _ := pubSubChannels(t, b)
				require.Equal(t, 0, len(channels), fmt.Sprintf("%#v", channels))
			}

			var wg sync.WaitGroup

			// The same channel in parallel.
			for i := 0; i < 100; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					require.NoError(t, b.Subscribe("2-test"))
					require.NoError(t, b.Unsubscribe("2-test"))
				}()
			}
			wg.Wait()
			channels, err = pubSubChannels(t, b)
			require.NoError(t, err)

			if len(channels) != 0 {
				time.Sleep(2000 * time.Millisecond)
				channels, _ := pubSubChannels(t, b)
				require.Equal(t, 0, len(channels), fmt.Sprintf("%#v", channels))
			}

			// Different channels in parallel.
			for i := 0; i < 100; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					require.NoError(t, b.Subscribe("3-test-"+strconv.Itoa(i)))
					require.NoError(t, b.Unsubscribe("3-test-"+strconv.Itoa(i)))
				}(i)
			}
			wg.Wait()
			channels, err = pubSubChannels(t, b)
			require.Equal(t, nil, err)
			if len(channels) != 0 {
				time.Sleep(2000 * time.Millisecond)
				channels, err := pubSubChannels(t, b)
				require.NoError(t, err)
				require.Equal(t, 0, len(channels), fmt.Sprintf("%#v", channels))
			}

			// The same channel sequential.
			for i := 0; i < 1000; i++ {
				require.NoError(t, b.Subscribe("4-test"))
				require.NoError(t, b.Unsubscribe("4-test"))
			}
			channels, err = pubSubChannels(t, b)
			require.NoError(t, err)
			if len(channels) != 0 {
				time.Sleep(2000 * time.Millisecond)
				channels, _ := pubSubChannels(t, b)
				require.Equal(t, 0, len(channels), fmt.Sprintf("%#v", channels))
			}

			// Different channels sequential.
			for j := 0; j < 10; j++ {
				for i := 0; i < 100; i++ {
					require.NoError(t, b.Subscribe("5-test-"+strconv.Itoa(i)))
					require.NoError(t, b.Unsubscribe("5-test-"+strconv.Itoa(i)))
				}
				channels, err = pubSubChannels(t, b)
				require.NoError(t, err)
				if len(channels) != 0 {
					time.Sleep(2000 * time.Millisecond)
					channels, err := pubSubChannels(t, b)
					require.NoError(t, err)
					require.Equal(t, 0, len(channels), fmt.Sprintf("%#v", channels))
				}
			}

			// Different channels subscribe only in parallel.
			for i := 0; i < 100; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					require.NoError(t, b.Subscribe("6-test-"+strconv.Itoa(i)))
				}(i)
			}
			wg.Wait()
			channels, err = pubSubChannels(t, b)
			require.NoError(t, err)
			if len(channels) != 100 {
				time.Sleep(2000 * time.Millisecond)
				channels, err := pubSubChannels(t, b)
				require.NoError(t, err)
				require.Equal(t, 100, len(channels), fmt.Sprintf("%#v", channels))
			}

			// Different channels unsubscribe only in parallel.
			for i := 0; i < 100; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					require.NoError(t, b.Unsubscribe("6-test-"+strconv.Itoa(i)))
				}(i)
			}
			wg.Wait()
			channels, err = pubSubChannels(t, b)
			require.NoError(t, err)
			if len(channels) != 0 {
				time.Sleep(2000 * time.Millisecond)
				channels, _ := pubSubChannels(t, b)
				require.Equal(t, 0, len(channels), fmt.Sprintf("%#v", channels))
			}

			for i := 0; i < 100; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					require.NoError(t, b.Unsubscribe("7-test-"+strconv.Itoa(i)))
					require.NoError(t, b.Unsubscribe("8-test-"+strconv.Itoa(i)))
					require.NoError(t, b.Subscribe("8-test-"+strconv.Itoa(i)))
					require.NoError(t, b.Unsubscribe("9-test-"+strconv.Itoa(i)))
					require.NoError(t, b.Subscribe("7-test-"+strconv.Itoa(i)))
					require.NoError(t, b.Unsubscribe("8-test-"+strconv.Itoa(i)))
					require.NoError(t, b.Subscribe("9-test-"+strconv.Itoa(i)))
					require.NoError(t, b.Unsubscribe("9-test-"+strconv.Itoa(i)))
					require.NoError(t, b.Unsubscribe("7-test-"+strconv.Itoa(i)))
				}(i)
			}
			wg.Wait()
			channels, err = pubSubChannels(t, b)
			require.NoError(t, err)
			if len(channels) != 0 {
				time.Sleep(2000 * time.Millisecond)
				channels, err := pubSubChannels(t, b)
				require.NoError(t, err)
				require.Equal(t, 0, len(channels), fmt.Sprintf("%#v", channels))
			}
		})
	}
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randString(n int) string {
	random := rand.New(rand.NewSource(time.Now().UnixNano()))
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[random.Intn(len(letterRunes))]
	}
	return string(b)
}

// TestRedisConsistentIndex exists to test consistent hashing algorithm we use.
// As we use random in this test we carefully do requires here.
// At least it can protect us from stupid mistakes to certain degree.
// We just expect +-equal distribution and keeping most of chans on
// the same shard after resharding.
func TestRedisConsistentIndex(t *testing.T) {
	numChans := 10000
	numShards := 10
	chans := make([]string, numChans)
	for i := 0; i < numChans; i++ {
		chans[i] = randString(rand.Intn(10) + 1)
	}
	chanToShard := make(map[string]int)
	chanToReshard := make(map[string]int)
	shardToChan := make(map[int][]string)

	for _, ch := range chans {
		shard := consistentIndex(ch, numShards)
		reshard := consistentIndex(ch, numShards+1)
		chanToShard[ch] = shard
		chanToReshard[ch] = reshard

		if _, ok := shardToChan[shard]; !ok {
			shardToChan[shard] = []string{}
		}
		shardChans := shardToChan[shard]
		shardChans = append(shardChans, ch)
		shardToChan[shard] = shardChans
	}

	for shard, shardChans := range shardToChan {
		shardFraction := float64(len(shardChans)) * 100 / float64(len(chans))
		fmt.Printf("Shard %d: %f%%\n", shard, shardFraction)
	}

	sameShards := 0

	// And test resharding.
	for ch, shard := range chanToShard {
		reshard := chanToReshard[ch]
		if shard == reshard {
			sameShards++
		}
	}
	sameFraction := float64(sameShards) * 100 / float64(len(chans))
	fmt.Printf("Same shards after resharding: %f%%\n", sameFraction)
	require.True(t, sameFraction > 0.7)
}

func TestRedisBrokerHandlePubSubMessage(t *testing.T) {
	for _, tt := range excludeNoHistoryClusterTests(noHistoryRedisTests) {
		t.Run(tt.Name, func(t *testing.T) {
			node := testNode(t)
			b := NewTestRedisBroker(t, node, getUniquePrefix(), tt.UseCluster, tt.Port)
			defer func() { _ = node.Shutdown(context.Background()) }()
			defer stopRedisBroker(b)
			err := b.handleRedisClientMessage(&testBrokerEventHandler{HandlePublicationFunc: func(ch string, pub *Publication, sp StreamPosition, delta bool, prevPub *Publication) error {
				require.Equal(t, "test", ch)
				require.Equal(t, uint64(16901), sp.Offset)
				require.Equal(t, "xyz", sp.Epoch)
				return nil
			}}, b.messageChannelID(b.shards[0].shard, "test"), []byte("__p1:16901:xyz__dsdsd"))
			require.Error(t, err)

			err = b.handleRedisClientMessage(&testBrokerEventHandler{HandlePublicationFunc: func(ch string, pub *Publication, sp StreamPosition, delta bool, prevPub *Publication) error {
				return nil
			}}, b.messageChannelID(b.shards[0].shard, "test"), []byte("__p1:16901"))
			require.Error(t, err)

			pub := &protocol.Publication{
				Data: []byte("{}"),
			}
			data, err := pub.MarshalVT()
			require.NoError(t, err)
			var publicationHandlerCalled bool
			err = b.handleRedisClientMessage(&testBrokerEventHandler{HandlePublicationFunc: func(ch string, pub *Publication, sp StreamPosition, delta bool, prevPub *Publication) error {
				publicationHandlerCalled = true
				require.Equal(t, "test", ch)
				require.Equal(t, uint64(16901), sp.Offset)
				require.Equal(t, "xyz", sp.Epoch)
				return nil
			}}, b.messageChannelID(b.shards[0].shard, "test"), []byte("__p1:16901:xyz__"+string(data)))
			require.NoError(t, err)
			require.True(t, publicationHandlerCalled)

			info := &protocol.ClientInfo{
				User: "12",
			}
			data, err = info.MarshalVT()
			require.NoError(t, err)
			var joinHandlerCalled bool
			err = b.handleRedisClientMessage(&testBrokerEventHandler{HandleJoinFunc: func(ch string, info *ClientInfo) error {
				joinHandlerCalled = true
				require.Equal(t, "test", ch)
				require.Equal(t, "12", info.UserID)
				return nil
			}}, b.messageChannelID(b.shards[0].shard, "test"), append(joinTypePrefix, data...))
			require.NoError(t, err)
			require.True(t, joinHandlerCalled)

			var leaveHandlerCalled bool
			err = b.handleRedisClientMessage(&testBrokerEventHandler{HandleLeaveFunc: func(ch string, info *ClientInfo) error {
				leaveHandlerCalled = true
				require.Equal(t, "test", ch)
				require.Equal(t, "12", info.UserID)
				return nil
			}}, b.messageChannelID(b.shards[0].shard, "test"), append(leaveTypePrefix, data...))
			require.NoError(t, err)
			require.True(t, leaveHandlerCalled)
		})
	}
}

func BenchmarkRedisExtractPushData(b *testing.B) {
	data := []byte(`__p1:16901:xyz__\x12\nchat:index\x1aU\"\x0e{\"input\":\"__\"}*C\n\x0242\x12$37cb00a9-bcfa-4284-a1ae-607c7da3a8f4\x1a\x15{\"name\": \"Alexander\"}\"\x00`)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, sp, _, _, ok := extractPushData(data)
		if !ok {
			b.Fatal("wrong data")
		}
		if sp.Offset != 16901 {
			b.Fatal("wrong offset")
		}
		if sp.Epoch != "xyz" {
			b.Fatal("wrong epoch")
		}
	}
}

func TestRedisExtractPushData(t *testing.T) {
	data := []byte(`__p1:16901:xyz.123__\x12\nchat:index\x1aU\"\x0e{\"input\":\"__\"}*C\n\x0242\x12$37cb00a9-bcfa-4284-a1ae-607c7da3a8f4\x1a\x15{\"name\": \"Alexander\"}\"\x00`)
	pushData, pushType, sp, _, _, ok := extractPushData(data)
	require.True(t, ok)
	require.Equal(t, pubPushType, pushType)
	require.Equal(t, uint64(16901), sp.Offset)
	require.Equal(t, "xyz.123", sp.Epoch)
	require.Equal(t, []byte(`\x12\nchat:index\x1aU\"\x0e{\"input\":\"__\"}*C\n\x0242\x12$37cb00a9-bcfa-4284-a1ae-607c7da3a8f4\x1a\x15{\"name\": \"Alexander\"}\"\x00`), pushData)

	data = []byte(`\x12\nchat:index\x1aU\"\x0e{\"input\":\"__\"}*C\n\x0242\x12$37cb00a9-bcfa-4284-a1ae-607c7da3a8f4\x1a\x15{\"name\": \"Alexander\"}\"\x00`)
	pushData, pushType, sp, _, _, ok = extractPushData(data)
	require.True(t, ok)
	require.Equal(t, pubPushType, pushType)
	require.Equal(t, uint64(0), sp.Offset)
	require.Equal(t, []byte(`\x12\nchat:index\x1aU\"\x0e{\"input\":\"__\"}*C\n\x0242\x12$37cb00a9-bcfa-4284-a1ae-607c7da3a8f4\x1a\x15{\"name\": \"Alexander\"}\"\x00`), pushData)

	data = []byte(`__j__\x12\nchat:index\x1aU\"\x0e{\"input\":\"__\"}*C\n\x0242\x12$37cb00a9-bcfa-4284-a1ae-607c7da3a8f4\x1a\x15{\"name\": \"Alexander\"}\"\x00`)
	pushData, pushType, sp, _, _, ok = extractPushData(data)
	require.True(t, ok)
	require.Equal(t, joinPushType, pushType)
	require.Equal(t, uint64(0), sp.Offset)
	require.Equal(t, []byte(`\x12\nchat:index\x1aU\"\x0e{\"input\":\"__\"}*C\n\x0242\x12$37cb00a9-bcfa-4284-a1ae-607c7da3a8f4\x1a\x15{\"name\": \"Alexander\"}\"\x00`), pushData)

	data = []byte(`__l__\x12\nchat:index\x1aU\"\x0e{\"input\":\"__\"}*C\n\x0242\x12$37cb00a9-bcfa-4284-a1ae-607c7da3a8f4\x1a\x15{\"name\": \"Alexander\"}\"\x00`)
	pushData, pushType, sp, _, _, ok = extractPushData(data)
	require.True(t, ok)
	require.Equal(t, leavePushType, pushType)
	require.Equal(t, uint64(0), sp.Offset)
	require.Equal(t, []byte(`\x12\nchat:index\x1aU\"\x0e{\"input\":\"__\"}*C\n\x0242\x12$37cb00a9-bcfa-4284-a1ae-607c7da3a8f4\x1a\x15{\"name\": \"Alexander\"}\"\x00`), pushData)

	data = []byte(`____\x12\nchat:index\x1aU\"\x0e{\"input\":\"__\"}*C\n\x0242\x12$37cb00a9-bcfa-4284-a1ae-607c7da3a8f4\x1a\x15{\"name\": \"Alexander\"}\"\x00`)
	_, _, _, _, _, ok = extractPushData(data)
	require.False(t, ok)

	data = []byte(`__a__\x12\nchat:index\x1aU\"\x0e{\"input\":\"__\"}*C\n\x0242\x12$37cb00a9-bcfa-4284-a1ae-607c7da3a8f4\x1a\x15{\"name\": \"Alexander\"}\"\x00`)
	_, _, _, _, _, ok = extractPushData(data)
	require.False(t, ok)
}

func TestNode_OnSurvey_TwoNodes(t *testing.T) {
	for _, tt := range excludeNoHistoryClusterTests(noHistoryRedisTests) {
		t.Run(tt.Name, func(t *testing.T) {
			redisConf := testSingleRedisConf(tt.Port)

			node1, _ := New(Config{})

			s, err := NewRedisShard(node1, redisConf)
			require.NoError(t, err)

			prefix := getUniquePrefix()

			b1, _ := NewRedisBroker(node1, RedisBrokerConfig{
				Prefix: prefix,
				Shards: []*RedisShard{s},
			})
			node1.SetBroker(b1)
			_ = node1.Run()
			defer func() { _ = node1.Shutdown(context.Background()) }()
			defer stopRedisBroker(b1)

			node1.OnSurvey(func(event SurveyEvent, callback SurveyCallback) {
				require.Nil(t, event.Data)
				require.Equal(t, "test_op", event.Op)
				callback(SurveyReply{
					Data: []byte("1"),
					Code: 1,
				})
			})

			node2, _ := New(Config{})

			s2, err := NewRedisShard(node2, redisConf)
			require.NoError(t, err)
			b2, _ := NewRedisBroker(node2, RedisBrokerConfig{
				Prefix: prefix,
				Shards: []*RedisShard{s2},
			})
			node2.SetBroker(b2)
			_ = node2.Run()
			defer func() { _ = node2.Shutdown(context.Background()) }()
			defer stopRedisBroker(b2)

			node2.OnSurvey(func(event SurveyEvent, callback SurveyCallback) {
				require.Nil(t, event.Data)
				require.Equal(t, "test_op", event.Op)
				callback(SurveyReply{
					Data: []byte("2"),
					Code: 2,
				})
			})

			waitAllNodes(t, node1, 2)

			results, err := node1.Survey(context.Background(), "test_op", nil, "")
			require.NoError(t, err)
			require.Len(t, results, 2)
			res, ok := results[node1.ID()]
			require.True(t, ok)
			require.Equal(t, uint32(1), res.Code)
			require.Equal(t, []byte("1"), res.Data)
			res, ok = results[node2.ID()]
			require.True(t, ok)
			require.Equal(t, uint32(2), res.Code)
			require.Equal(t, []byte("2"), res.Data)
		})
	}
}

func TestNode_OnNotification_TwoNodes(t *testing.T) {
	for _, tt := range excludeNoHistoryClusterTests(noHistoryRedisTests) {
		t.Run(tt.Name, func(t *testing.T) {
			redisConf := testSingleRedisConf(tt.Port)

			node1, _ := New(Config{})

			s, err := NewRedisShard(node1, redisConf)
			require.NoError(t, err)

			prefix := getUniquePrefix()

			b1, _ := NewRedisBroker(node1, RedisBrokerConfig{
				Prefix: prefix,
				Shards: []*RedisShard{s},
			})
			node1.SetBroker(b1)
			_ = node1.Run()
			defer func() { _ = node1.Shutdown(context.Background()) }()
			defer stopRedisBroker(b1)

			ch1 := make(chan struct{})

			node1.OnNotification(func(event NotificationEvent) {
				require.Equal(t, []byte(`notification`), event.Data)
				require.Equal(t, "test_op", event.Op)
				require.NotEmpty(t, event.FromNodeID)
				require.Equal(t, node1.ID(), event.FromNodeID)
				close(ch1)
			})

			node2, _ := New(Config{})

			s2, err := NewRedisShard(node2, redisConf)
			require.NoError(t, err)
			b2, _ := NewRedisBroker(node2, RedisBrokerConfig{
				Prefix: prefix,
				Shards: []*RedisShard{s2},
			})
			node2.SetBroker(b2)
			_ = node2.Run()
			defer func() { _ = node2.Shutdown(context.Background()) }()
			defer stopRedisBroker(b2)

			ch2 := make(chan struct{})

			node2.OnNotification(func(event NotificationEvent) {
				require.Equal(t, []byte(`notification`), event.Data)
				require.Equal(t, "test_op", event.Op)
				require.NotEqual(t, node2.ID(), event.FromNodeID)
				close(ch2)
			})

			waitAllNodes(t, node1, 2)

			err = node1.Notify("test_op", []byte(`notification`), "")
			require.NoError(t, err)
			tm := time.After(5 * time.Second)
			select {
			case <-ch1:
				select {
				case <-ch2:
				case <-tm:
					t.Fatal("timeout on ch2")
				}
			case <-tm:
				t.Fatal("timeout on ch1")
			}
		})
	}
}

func TestRedisPubSubTwoNodes(t *testing.T) {
	for _, tt := range excludeNoHistoryClusterTests(noHistoryRedisTests) {
		t.Run(tt.Name, func(t *testing.T) {
			redisConf := testSingleRedisConf(tt.Port)

			prefix := getUniquePrefix()

			node1, _ := New(Config{})
			s, err := NewRedisShard(node1, redisConf)
			require.NoError(t, err)
			b1, _ := NewRedisBroker(node1, RedisBrokerConfig{
				Prefix:               prefix,
				Shards:               []*RedisShard{s},
				numResubscribeShards: 4,
				numPubSubProcessors:  2,
			})
			node1.SetBroker(b1)
			defer func() { _ = node1.Shutdown(context.Background()) }()
			defer stopRedisBroker(b1)

			msgNum := 10
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

			for i := 0; i < msgNum; i++ {
				require.NoError(t, b1.Subscribe("test"+strconv.Itoa(i)))
			}

			node2, _ := New(Config{})
			s2, err := NewRedisShard(node2, redisConf)
			require.NoError(t, err)

			b2, _ := NewRedisBroker(node2, RedisBrokerConfig{
				Prefix: prefix,
				Shards: []*RedisShard{s2},
			})
			node2.SetBroker(b2)
			_ = node2.Run()
			defer func() { _ = node2.Shutdown(context.Background()) }()
			defer stopRedisBroker(b2)

			for i := 0; i < msgNum; i++ {
				_, err = node2.Publish("test"+strconv.Itoa(i), []byte("123"))
				require.NoError(t, err)
				err = b2.PublishJoin("test"+strconv.Itoa(i), &ClientInfo{})
				require.NoError(t, err)
				err = b2.PublishLeave("test"+strconv.Itoa(i), &ClientInfo{})
				require.NoError(t, err)
			}

			select {
			case <-pubCh:
			case <-time.After(time.Second):
				require.Fail(t, "timeout waiting for PUB/SUB message")
			}
			select {
			case <-joinCh:
			case <-time.After(time.Second):
				require.Fail(t, "timeout waiting for PUB/SUB join message")
			}
			select {
			case <-leaveCh:
			case <-time.After(time.Second):
				require.Fail(t, "timeout waiting for PUB/SUB leave message")
			}
		})
	}
}

type testDeltaPublishHandle struct {
	ch      string
	pub     *Publication
	sp      StreamPosition
	prevPub *Publication
}

func TestRedisPubSubTwoNodesWithDelta(t *testing.T) {
	// This is special because it actually uses history, but we re-use existing infrastructure.
	for _, tt := range excludeNoHistoryClusterTests(noHistoryRedisTests) {
		t.Run(tt.Name, func(t *testing.T) {
			redisConf := testSingleRedisConf(tt.Port)

			prefix := getUniquePrefix()

			ch := "test" + uuid.NewString()

			node1, _ := New(Config{})
			s, err := NewRedisShard(node1, redisConf)
			require.NoError(t, err)
			b1, _ := NewRedisBroker(node1, RedisBrokerConfig{
				Prefix:               prefix,
				Shards:               []*RedisShard{s},
				numResubscribeShards: 4,
				numPubSubProcessors:  2,
			})
			node1.SetBroker(b1)
			defer func() { _ = node1.Shutdown(context.Background()) }()
			defer stopRedisBroker(b1)

			msgNum := 2
			var numPublications int64
			pubCh := make(chan struct{})
			var resultsMu sync.Mutex
			results := make([]testDeltaPublishHandle, 0, msgNum)

			brokerEventHandler := &testBrokerEventHandler{
				HandleControlFunc: func(bytes []byte) error {
					return nil
				},
				HandlePublicationFunc: func(ch string, pub *Publication, sp StreamPosition, delta bool, prevPub *Publication) error {
					resultsMu.Lock()
					defer resultsMu.Unlock()
					results = append(results, testDeltaPublishHandle{
						ch:      ch,
						pub:     pub,
						sp:      sp,
						prevPub: prevPub,
					})
					c := atomic.AddInt64(&numPublications, 1)
					if c == int64(msgNum) {
						close(pubCh)
					}
					return nil
				},
			}
			_ = b1.RegisterControlEventHandler(brokerEventHandler)
			_ = b1.RegisterBrokerEventHandler(brokerEventHandler)

			require.NoError(t, b1.Subscribe(ch))

			node2, _ := New(Config{})
			s2, err := NewRedisShard(node2, redisConf)
			require.NoError(t, err)

			b2, _ := NewRedisBroker(node2, RedisBrokerConfig{
				Prefix: prefix,
				Shards: []*RedisShard{s2},
			})
			node2.SetBroker(b2)
			_ = node2.Run()
			defer func() { _ = node2.Shutdown(context.Background()) }()
			defer stopRedisBroker(b2)

			for i := 0; i < msgNum; i++ {
				sp, err := node2.Publish(ch, []byte("123"),
					WithHistory(1, time.Minute), WithDelta(true))
				require.NoError(t, err)
				require.Equal(t, sp.Offset, uint64(i+1))
			}

			select {
			case <-pubCh:
			case <-time.After(time.Second):
				require.Fail(t, "timeout waiting for PUB/SUB message")
			}

			resultsMu.Lock()
			defer resultsMu.Unlock()
			require.Len(t, results, msgNum)
			require.Nil(t, results[0].prevPub)
			require.NotNil(t, results[1].prevPub)
		})
	}
}

func TestRedisClusterShardedPubSub(t *testing.T) {
	redisConf := RedisShardConfig{
		ClusterAddresses: []string{"localhost:7000", "localhost:7001", "localhost:7002"},
		IOTimeout:        10 * time.Second,
		ConnectTimeout:   10 * time.Second,
	}

	prefix := getUniquePrefix()

	node1, _ := New(Config{})
	s, err := NewRedisShard(node1, redisConf)
	require.NoError(t, err)
	b1, _ := NewRedisBroker(node1, RedisBrokerConfig{
		Prefix:                     prefix,
		Shards:                     []*RedisShard{s},
		numSubscribeShards:         2,
		NumShardedPubSubPartitions: 9,
	})
	node1.SetBroker(b1)
	defer func() { _ = node1.Shutdown(context.Background()) }()
	defer stopRedisBroker(b1)

	result := s.client.Do(context.Background(), s.client.B().Spublish().Channel(prefix+"._").Message("").Build())
	if result.Error() != nil && strings.Contains(result.Error().Error(), "unknown command") {
		t.Skip("sharded PUB/SUB not supported by this Redis version, skipping test")
	} else {
		require.NoError(t, result.Error())
	}

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

	for i := 0; i < msgNum; i++ {
		require.NoError(t, b1.Subscribe("test"+strconv.Itoa(i)))
	}

	node2, _ := New(Config{})
	s2, err := NewRedisShard(node2, redisConf)
	require.NoError(t, err)

	b2, _ := NewRedisBroker(node2, RedisBrokerConfig{
		Prefix:                     prefix,
		Shards:                     []*RedisShard{s2},
		numSubscribeShards:         2,
		NumShardedPubSubPartitions: 9,
	})
	node2.SetBroker(b2)
	_ = node2.Run()
	defer func() { _ = node2.Shutdown(context.Background()) }()
	defer stopRedisBroker(b2)

	for i := 0; i < msgNum; i++ {
		_, err = node2.Publish("test"+strconv.Itoa(i), []byte("123"))
		require.NoError(t, err)
		err = b2.PublishJoin("test"+strconv.Itoa(i), &ClientInfo{})
		require.NoError(t, err)
		err = b2.PublishLeave("test"+strconv.Itoa(i), &ClientInfo{})
		require.NoError(t, err)
	}

	select {
	case <-pubCh:
	case <-time.After(time.Second):
		require.Fail(t, "timeout waiting for PUB/SUB message")
	}
	select {
	case <-joinCh:
	case <-time.After(time.Second):
		require.Fail(t, "timeout waiting for PUB/SUB join message")
	}
	select {
	case <-leaveCh:
	case <-time.After(time.Second):
		require.Fail(t, "timeout waiting for PUB/SUB leave message")
	}
}

func waitAllNodes(tb testing.TB, node *Node, numNodes int) {
	// Here we are waiting for 2 nodes join.
	// Note: maybe introduce events in future?
	i := 0
	maxLoops := 20
	time.Sleep(50 * time.Millisecond)
	for {
		info, err := node.Info()
		require.NoError(tb, err)
		if len(info.Nodes) == numNodes {
			break
		}
		i++
		if i > maxLoops {
			require.Fail(tb, "timeout waiting for all nodes in cluster")
		}
		time.Sleep(250 * time.Millisecond)
	}
}

type benchSurveyTest struct {
	Name          string
	NumOtherNodes int
	DataSize      int
	UseCluster    bool
}

var benchSurveyTests = func() (tests []benchSurveyTest) {
	for _, useCluster := range []bool{false, true} {
		if os.Getenv("CENTRIFUGE_REDIS_CLUSTER_BENCHMARKS") == "" && useCluster {
			continue
		}
		for _, dataSize := range []int{512, 4096} {
			for _, numOtherNodes := range []int{0, 1, 2, 3, 4, 9, 99} {
				name := ""
				if numOtherNodes+1 > 1 {
					name += fmt.Sprintf("%d nodes %dB", numOtherNodes+1, dataSize)
				} else {
					name += fmt.Sprintf("%d node %dB", numOtherNodes+1, dataSize)
				}
				if useCluster {
					name += " cluster"
				}
				tests = append(tests, benchSurveyTest{
					Name:          name,
					NumOtherNodes: numOtherNodes,
					DataSize:      dataSize,
					UseCluster:    useCluster,
				})
			}
		}
	}
	return
}()

func BenchmarkRedisSurvey(b *testing.B) {
	for _, tt := range benchSurveyTests {
		b.Run(tt.Name, func(b *testing.B) {
			prefix := getUniquePrefix()
			redisConf := testSingleRedisConf(6379)
			data := make([]byte, tt.DataSize)

			var nodes []*Node
			var shards []*RedisShard

			for i := 0; i < tt.NumOtherNodes; i++ {
				node, _ := New(Config{})
				shard, err := NewRedisShard(node, redisConf)
				if err != nil {
					b.Fatal(err)
				}
				broker, _ := NewRedisBroker(node, RedisBrokerConfig{
					Prefix: prefix,
					Shards: []*RedisShard{shard},
				})
				node.SetBroker(broker)
				_ = node.Run()
				nodes = append(nodes, node)
				shards = append(shards, shard)

				node.OnSurvey(func(event SurveyEvent, callback SurveyCallback) {
					callback(SurveyReply{
						Data: data,
						Code: 1,
					})
				})
			}

			node, _ := New(Config{})
			shard, err := NewRedisShard(node, redisConf)
			if err != nil {
				b.Fatal(err)
			}
			broker, _ := NewRedisBroker(node, RedisBrokerConfig{
				Prefix: prefix,
				Shards: []*RedisShard{shard},
			})
			node.SetBroker(broker)
			_ = node.Run()
			nodes = append(nodes, node)
			shards = append(shards, shard)

			node.OnSurvey(func(event SurveyEvent, callback SurveyCallback) {
				callback(SurveyReply{
					Data: data,
					Code: 2,
				})
			})

			waitAllNodes(b, node, tt.NumOtherNodes+1)

			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					_, err := node.Survey(context.Background(), "test_op", nil, "")
					if err != nil {
						b.Fatal(err)
					}
				}
			})
			b.StopTimer()
			for _, n := range nodes {
				_ = n.Shutdown(context.Background())
			}
			for _, s := range shards {
				s.Close()
			}
		})
	}
}

func BenchmarkRedisConsistentIndex(b *testing.B) {
	for i := 0; i < b.N; i++ {
		consistentIndex(strconv.Itoa(i), 4)
	}
}

func BenchmarkRedisIndex(b *testing.B) {
	for i := 0; i < b.N; i++ {
		index(strconv.Itoa(i), 4)
	}
}

func BenchmarkRedisPublish_1Ch(b *testing.B) {
	for _, tt := range noHistoryBenchRedisTests {
		b.Run(tt.Name, func(b *testing.B) {
			node := benchNode(b)
			broker := newTestRedisBroker(b, node, false, tt.UseCluster, tt.Port)
			defer func() { _ = node.Shutdown(context.Background()) }()
			defer stopRedisBroker(broker)
			rawData := []byte(`{"bench": true}`)
			b.SetParallelism(getBenchParallelism())
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					_, _, err := broker.Publish("channel", rawData, PublishOptions{})
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}

const benchmarkNumDifferentChannels = 1024

func BenchmarkRedisPublish_ManyCh(b *testing.B) {
	for _, tt := range noHistoryRedisTests {
		b.Run(tt.Name, func(b *testing.B) {
			node := benchNode(b)
			broker := newTestRedisBroker(b, node, false, tt.UseCluster, tt.Port)
			defer func() { _ = node.Shutdown(context.Background()) }()
			defer stopRedisBroker(broker)
			rawData := []byte(`{"bench": true}`)
			b.SetParallelism(getBenchParallelism())
			j := int32(0)
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					jj := atomic.AddInt32(&j, 1)
					channel := "channel" + strconv.Itoa(int(jj)%benchmarkNumDifferentChannels)
					_, _, err := broker.Publish(channel, rawData, PublishOptions{})
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}

func BenchmarkRedisPublish_History_1Ch(b *testing.B) {
	for _, tt := range historyBenchRedisTests {
		b.Run(tt.Name, func(b *testing.B) {
			node := benchNode(b)
			broker := newTestRedisBroker(b, node, tt.UseStreams, tt.UseCluster, tt.Port)
			defer func() { _ = node.Shutdown(context.Background()) }()
			defer stopRedisBroker(broker)
			rawData := []byte(`{"bench": true}`)
			chOpts := PublishOptions{HistorySize: 100, HistoryTTL: 100 * time.Second}
			b.SetParallelism(getBenchParallelism())
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					var err error
					pos, _, err := broker.Publish("channel", rawData, chOpts)
					if err != nil {
						b.Fatal(err)
					}
					if pos.Offset == 0 {
						b.Fail()
					}
				}
			})
		})
	}
}

func BenchmarkRedisPub_History_ManyCh(b *testing.B) {
	for _, tt := range historyBenchRedisTests {
		b.Run(tt.Name, func(b *testing.B) {
			node := benchNode(b)
			broker := newTestRedisBroker(b, node, tt.UseStreams, tt.UseCluster, tt.Port)
			defer func() { _ = node.Shutdown(context.Background()) }()
			defer stopRedisBroker(broker)
			rawData := []byte(`{"bench": true}`)
			chOpts := PublishOptions{HistorySize: 100, HistoryTTL: 100 * time.Second}
			b.SetParallelism(getBenchParallelism())
			j := int32(0)
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					jj := atomic.AddInt32(&j, 1)
					channel := "channel" + strconv.Itoa(int(jj)%benchmarkNumDifferentChannels)
					var err error
					pos, _, err := broker.Publish(channel, rawData, chOpts)
					if err != nil {
						b.Fatal(err)
					}
					if pos.Offset == 0 {
						b.Fail()
					}
				}
			})
		})
	}
}

func BenchmarkRedisSubscribe(b *testing.B) {
	for _, tt := range noHistoryRedisTests {
		b.Run(tt.Name, func(b *testing.B) {
			node := benchNode(b)
			broker := newTestRedisBroker(b, node, false, tt.UseCluster, tt.Port)
			defer func() { _ = node.Shutdown(context.Background()) }()
			defer stopRedisBroker(broker)
			i := int32(0)
			b.SetParallelism(getBenchParallelism())
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					ii := atomic.AddInt32(&i, 1)
					err := broker.Subscribe("subscribe" + strconv.Itoa(int(ii)%benchmarkNumDifferentChannels))
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}

func BenchmarkRedisHistory_1Ch(b *testing.B) {
	for _, tt := range historyBenchRedisTests {
		b.Run(tt.Name, func(b *testing.B) {
			node := benchNode(b)
			broker := newTestRedisBroker(b, node, tt.UseStreams, tt.UseCluster, tt.Port)
			defer func() { _ = node.Shutdown(context.Background()) }()
			defer stopRedisBroker(broker)
			rawData := []byte("{}")
			for i := 0; i < 4; i++ {
				_, _, err := broker.Publish("channel", rawData, PublishOptions{HistorySize: 4, HistoryTTL: 300 * time.Second})
				require.NoError(b, err)
			}
			b.SetParallelism(getBenchParallelism())
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					_, err := broker.node.History("channel", WithLimit(-1), WithSince(nil))
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}

func BenchmarkRedisRecover_1Ch(b *testing.B) {
	for _, tt := range historyBenchRedisTests {
		b.Run(tt.Name, func(b *testing.B) {
			node := benchNode(b)
			broker := newTestRedisBroker(b, node, tt.UseStreams, tt.UseCluster, tt.Port)
			defer func() { _ = node.Shutdown(context.Background()) }()
			defer stopRedisBroker(broker)
			rawData := []byte(randString(800))

			numMessages := 10
			numMissing := 10
			for i := 1; i <= numMessages; i++ {
				_, _, err := broker.Publish("channel", rawData, PublishOptions{HistorySize: numMessages, HistoryTTL: 300 * time.Second})
				require.NoError(b, err)
			}
			b.SetParallelism(getBenchParallelism())
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					pubs, _, err := broker.History("channel", HistoryOptions{
						Filter: HistoryFilter{
							Limit: -1,
							Since: &StreamPosition{Offset: uint64(numMessages - numMissing), Epoch: ""},
						},
					})
					if err != nil {
						b.Fatal(err)
					}
					if len(pubs) != numMissing {
						b.Fail()
					}
				}
			})
		})
	}
}

func nodeWithRedisBroker(tb testing.TB, useStreams bool, useCluster bool, port int) *Node {
	n, err := New(Config{})
	if err != nil {
		tb.Fatal(err)
	}
	newTestRedisBroker(tb, n, useStreams, useCluster, port)
	n.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{}, nil)
		})
		client.OnPublish(func(e PublishEvent, cb PublishCallback) {
			cb(PublishReply{}, nil)
		})
	})
	return n
}

func testRedisClientSubscribeRecover(t *testing.T, tt historyRedisTest, rt recoverTest) {
	node := nodeWithRedisBroker(t, tt.UseStreams, tt.UseCluster, tt.Port)
	node.config.RecoveryMaxPublicationLimit = rt.Limit
	defer func() { _ = node.Shutdown(context.Background()) }()
	defer stopRedisBroker(node.broker.(*RedisBroker))

	channel := "test_recovery_redis_" + tt.Name

	for i := 1; i <= rt.NumPublications; i++ {
		_, err := node.Publish(channel, []byte(`{"n": `+strconv.Itoa(i)+`}`), WithHistory(rt.HistorySize, time.Duration(rt.HistoryTTLSeconds)*time.Second))
		require.NoError(t, err)
	}

	time.Sleep(time.Duration(rt.Sleep) * time.Second)

	_, streamTop, err := node.broker.History(channel, HistoryOptions{
		Filter: HistoryFilter{
			Limit: 0,
			Since: nil,
		},
	})
	require.NoError(t, err)

	historyResult, err := node.recoverHistory(channel, StreamPosition{rt.SinceOffset, streamTop.Epoch}, 0)
	require.NoError(t, err)
	recoveredPubs, recovered := isStreamRecovered(historyResult, rt.SinceOffset, streamTop.Epoch, nil)
	require.Equal(t, rt.NumRecovered, len(recoveredPubs))
	require.Equal(t, rt.Recovered, recovered)
}

var brokerRecoverTests = []recoverTest{
	{"empty_stream", 10, 60, 0, 0, 0, 0, 0, true, RecoveryModeStream},
	{"from_position", 10, 60, 10, 8, 2, 0, 0, true, RecoveryModeStream},
	{"from_position_limited", 10, 60, 10, 5, 0, 0, 2, false, RecoveryModeStream},
	{"from_position_with_server_limit", 10, 60, 10, 5, 0, 0, 1, false, RecoveryModeStream},
	{"from_position_that_already_gone", 10, 60, 20, 8, 0, 0, 0, false, RecoveryModeStream},
	{"from_position_that_not_exist_yet", 10, 60, 20, 108, 0, 0, 0, false, RecoveryModeStream},
	{"same_position_no_pubs_expected", 10, 60, 7, 7, 0, 0, 0, true, RecoveryModeStream},
	{"empty_position_recover_expected", 10, 60, 4, 0, 4, 0, 0, true, RecoveryModeStream},
	{"from_position_in_expired_stream", 10, 1, 10, 8, 0, 3, 0, false, RecoveryModeStream},
	{"from_same_position_in_expired_stream", 10, 1, 1, 1, 0, 3, 0, true, RecoveryModeStream},
}

func TestRedisClientSubscribeRecover(t *testing.T) {
	for _, tt := range historyRedisTests {
		for _, rt := range brokerRecoverTests {
			t.Run(tt.Name+"_"+rt.Name, func(t *testing.T) {
				testRedisClientSubscribeRecover(t, tt, rt)
			})
		}
	}
}

func TestRedisHistoryIteration(t *testing.T) {
	for _, tt := range historyRedisTests {
		t.Run(tt.Name, func(t *testing.T) {
			node := testNode(t)
			broker := newTestRedisBroker(t, node, tt.UseStreams, tt.UseCluster, tt.Port)
			defer func() { _ = node.Shutdown(context.Background()) }()
			defer stopRedisBroker(broker)
			it := historyIterationTest{100, 5}
			startPosition := it.prepareHistoryIteration(t, broker.node)
			it.testHistoryIteration(t, broker.node, startPosition)
		})
	}
}

func TestRedisHistoryReversedNoMetaYet(t *testing.T) {
	for _, tt := range historyRedisTests {
		t.Run(tt.Name, func(t *testing.T) {
			node := testNode(t)
			broker := newTestRedisBroker(t, node, tt.UseStreams, tt.UseCluster, tt.Port)
			defer func() { _ = node.Shutdown(context.Background()) }()
			defer stopRedisBroker(broker)
			pubs, sp, err := broker.History(
				randString(10),
				HistoryOptions{
					Filter:  HistoryFilter{Limit: 10, Reverse: true},
					MetaTTL: 24 * time.Hour,
				},
			)
			require.NoError(t, err)
			require.Equal(t, uint64(0), sp.Offset)
			require.Len(t, pubs, 0)

			pubs, sp, err = broker.History(
				randString(10),
				HistoryOptions{
					Filter:  HistoryFilter{Limit: -1, Reverse: true},
					MetaTTL: 24 * time.Hour,
				},
			)
			require.NoError(t, err)
			require.Equal(t, uint64(0), sp.Offset)
			require.Len(t, pubs, 0)
		})
	}
}

func TestRedisHistoryIterationReverse(t *testing.T) {
	for _, tt := range historyRedisTests {
		t.Run(tt.Name, func(t *testing.T) {
			if !tt.UseStreams || tt.UseCluster {
				t.Skip()
			}
			node := testNode(t)
			broker := newTestRedisBroker(t, node, tt.UseStreams, tt.UseCluster, tt.Port)
			defer func() { _ = node.Shutdown(context.Background()) }()
			defer stopRedisBroker(broker)
			it := historyIterationTest{100, 5}
			startPosition := it.prepareHistoryIteration(t, broker.node)
			it.testHistoryIterationReverse(t, broker.node, startPosition)
		})
	}
}

func BenchmarkRedisHistoryIteration(b *testing.B) {
	for _, tt := range historyBenchRedisTests {
		b.Run(tt.Name, func(b *testing.B) {
			node := benchNode(b)
			broker := newTestRedisBroker(b, node, tt.UseStreams, tt.UseCluster, tt.Port)
			defer func() { _ = node.Shutdown(context.Background()) }()
			defer stopRedisBroker(broker)
			it := historyIterationTest{10000, 100}
			startPosition := it.prepareHistoryIteration(b, broker.node)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				it.testHistoryIteration(b, broker.node, startPosition)
			}
		})
	}
}

type throughputTest struct {
	NumSubscribeShards   int
	NumResubscribeShards int
	NumPubSubProcessors  int
	Port                 int
}

var throughputTests = []throughputTest{
	{1, 0, 0, 6379},
	{2, 0, 0, 6379},
	{4, 0, 0, 6379},
	{1, 0, 0, 36379},
	{2, 0, 0, 36379},
	{4, 0, 0, 36379},
}

func BenchmarkPubSubThroughput(b *testing.B) {
	for _, tt := range throughputTests {
		b.Run(fmt.Sprintf("%dsh_%dsub_%dproc_%d", tt.NumSubscribeShards, tt.NumResubscribeShards, tt.NumPubSubProcessors, tt.Port), func(b *testing.B) {
			redisConf := testSingleRedisConf(tt.Port)

			node1, _ := New(Config{})
			defer func() { _ = node1.Shutdown(context.Background()) }()

			s, err := NewRedisShard(node1, redisConf)
			require.NoError(b, err)

			prefix := getUniquePrefix()

			b1, err := NewRedisBroker(node1, RedisBrokerConfig{
				Prefix:               prefix,
				Shards:               []*RedisShard{s},
				numSubscribeShards:   tt.NumSubscribeShards,
				numResubscribeShards: tt.NumResubscribeShards,
				numPubSubProcessors:  tt.NumPubSubProcessors,
			})
			require.NoError(b, err)
			defer stopRedisBroker(b1)

			node1.SetBroker(b1)

			numChannels := benchmarkNumDifferentChannels
			pubCh := make(chan struct{}, numChannels)
			brokerEventHandler := &testBrokerEventHandler{
				HandleControlFunc: func(bytes []byte) error {
					return nil
				},
				HandlePublicationFunc: func(ch string, pub *Publication, sp StreamPosition, delta bool, prevPub *Publication) error {
					pubCh <- struct{}{}
					return nil
				},
			}
			_ = b1.RegisterControlEventHandler(brokerEventHandler)
			_ = b1.RegisterBrokerEventHandler(brokerEventHandler)

			for i := 0; i < numChannels; i++ {
				require.NoError(b, b1.Subscribe("test"+strconv.Itoa(i)))
			}

			node2, _ := New(Config{})
			s2, err := NewRedisShard(node2, redisConf)
			require.NoError(b, err)

			b2, _ := NewRedisBroker(node2, RedisBrokerConfig{
				Prefix: prefix,
				Shards: []*RedisShard{s2},
			})
			node2.SetBroker(b2)
			_ = node2.Run()
			defer func() { _ = node2.Shutdown(context.Background()) }()
			defer stopRedisBroker(b2)

			b.ReportAllocs()
			b.SetParallelism(getBenchParallelism())
			b.ResetTimer()
			var i int64
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					currentI := atomic.AddInt64(&i, 1) % int64(numChannels)
					_, err = node2.Publish("test"+strconv.FormatInt(currentI, 10), []byte("123"))
					if err != nil {
						b.Fatal(err)
					}
					<-pubCh
				}
			})
		})
	}
}

var tab = [256]uint16{
	0x0000, 0x1021, 0x2042, 0x3063, 0x4084, 0x50a5, 0x60c6, 0x70e7,
	0x8108, 0x9129, 0xa14a, 0xb16b, 0xc18c, 0xd1ad, 0xe1ce, 0xf1ef,
	0x1231, 0x0210, 0x3273, 0x2252, 0x52b5, 0x4294, 0x72f7, 0x62d6,
	0x9339, 0x8318, 0xb37b, 0xa35a, 0xd3bd, 0xc39c, 0xf3ff, 0xe3de,
	0x2462, 0x3443, 0x0420, 0x1401, 0x64e6, 0x74c7, 0x44a4, 0x5485,
	0xa56a, 0xb54b, 0x8528, 0x9509, 0xe5ee, 0xf5cf, 0xc5ac, 0xd58d,
	0x3653, 0x2672, 0x1611, 0x0630, 0x76d7, 0x66f6, 0x5695, 0x46b4,
	0xb75b, 0xa77a, 0x9719, 0x8738, 0xf7df, 0xe7fe, 0xd79d, 0xc7bc,
	0x48c4, 0x58e5, 0x6886, 0x78a7, 0x0840, 0x1861, 0x2802, 0x3823,
	0xc9cc, 0xd9ed, 0xe98e, 0xf9af, 0x8948, 0x9969, 0xa90a, 0xb92b,
	0x5af5, 0x4ad4, 0x7ab7, 0x6a96, 0x1a71, 0x0a50, 0x3a33, 0x2a12,
	0xdbfd, 0xcbdc, 0xfbbf, 0xeb9e, 0x9b79, 0x8b58, 0xbb3b, 0xab1a,
	0x6ca6, 0x7c87, 0x4ce4, 0x5cc5, 0x2c22, 0x3c03, 0x0c60, 0x1c41,
	0xedae, 0xfd8f, 0xcdec, 0xddcd, 0xad2a, 0xbd0b, 0x8d68, 0x9d49,
	0x7e97, 0x6eb6, 0x5ed5, 0x4ef4, 0x3e13, 0x2e32, 0x1e51, 0x0e70,
	0xff9f, 0xefbe, 0xdfdd, 0xcffc, 0xbf1b, 0xaf3a, 0x9f59, 0x8f78,
	0x9188, 0x81a9, 0xb1ca, 0xa1eb, 0xd10c, 0xc12d, 0xf14e, 0xe16f,
	0x1080, 0x00a1, 0x30c2, 0x20e3, 0x5004, 0x4025, 0x7046, 0x6067,
	0x83b9, 0x9398, 0xa3fb, 0xb3da, 0xc33d, 0xd31c, 0xe37f, 0xf35e,
	0x02b1, 0x1290, 0x22f3, 0x32d2, 0x4235, 0x5214, 0x6277, 0x7256,
	0xb5ea, 0xa5cb, 0x95a8, 0x8589, 0xf56e, 0xe54f, 0xd52c, 0xc50d,
	0x34e2, 0x24c3, 0x14a0, 0x0481, 0x7466, 0x6447, 0x5424, 0x4405,
	0xa7db, 0xb7fa, 0x8799, 0x97b8, 0xe75f, 0xf77e, 0xc71d, 0xd73c,
	0x26d3, 0x36f2, 0x0691, 0x16b0, 0x6657, 0x7676, 0x4615, 0x5634,
	0xd94c, 0xc96d, 0xf90e, 0xe92f, 0x99c8, 0x89e9, 0xb98a, 0xa9ab,
	0x5844, 0x4865, 0x7806, 0x6827, 0x18c0, 0x08e1, 0x3882, 0x28a3,
	0xcb7d, 0xdb5c, 0xeb3f, 0xfb1e, 0x8bf9, 0x9bd8, 0xabbb, 0xbb9a,
	0x4a75, 0x5a54, 0x6a37, 0x7a16, 0x0af1, 0x1ad0, 0x2ab3, 0x3a92,
	0xfd2e, 0xed0f, 0xdd6c, 0xcd4d, 0xbdaa, 0xad8b, 0x9de8, 0x8dc9,
	0x7c26, 0x6c07, 0x5c64, 0x4c45, 0x3ca2, 0x2c83, 0x1ce0, 0x0cc1,
	0xef1f, 0xff3e, 0xcf5d, 0xdf7c, 0xaf9b, 0xbfba, 0x8fd9, 0x9ff8,
	0x6e17, 0x7e36, 0x4e55, 0x5e74, 0x2e93, 0x3eb2, 0x0ed1, 0x1ef0,
}

// numSlots is the number of slots keys are sharded into in a redis cluster
const numSlots = 16384

// crc16 returns checksum for a given set of bytes based on the crc algorithm
// defined for hashing redis keys in a cluster setup
func crc16(buf []byte) uint16 {
	crc := uint16(0)
	for _, b := range buf {
		index := byte(crc>>8) ^ b
		crc = (crc << 8) ^ tab[index]
	}
	return crc
}

// Slot returns the cluster slot the given key will fall into, taking into
// account curly braces within the key as per the spec.
func redisSlot(key string) uint16 {
	if start := strings.Index(key, "{"); start >= 0 {
		if end := strings.Index(key[start+1:], "}"); end > 0 {
			key = key[start+1 : start+1+end]
		}
	}
	return crc16([]byte(key)) % numSlots
}

type shardedSlotsTestCase struct {
	numRedisNodes   int
	numShards       int
	clusterPartFunc clusterFuncWrapper
}

func detectRedisNodeIndex(slot uint16, numNodes int) int {
	slotsPerRedis := 16384 / numNodes
	s := 0
	for i := 0; i < numNodes; i++ {
		s += slotsPerRedis
		if int(slot) < s {
			return i
		}
	}
	return numNodes - 1
}

type clusterPartFunc func(string) string

type clusterFuncWrapper struct {
	name string
	fn   clusterPartFunc
}

// TestPreShardedSlots allows experimenting with Redis slots and its distribution.
func TestPreShardedSlots(t *testing.T) {
	t.Skip()
	redisNodesChoices := []int{3, 5, 8, 16}
	numShardsChoices := []int{32}

	clusterPartFuncChoices := []clusterFuncWrapper{
		{
			name: "simple_number",
			fn: func(idx string) string {
				return "{" + idx + "}"
			},
		},
		{
			name: "repeat_number",
			fn: func(idx string) string {
				return "{" + idx + "," + idx + "," + idx + "," + idx + "}"
			},
		},
	}

	var testCases []shardedSlotsTestCase

	for _, fw := range clusterPartFuncChoices {
		for _, rn := range redisNodesChoices {
			for _, sc := range numShardsChoices {
				testCases = append(testCases, shardedSlotsTestCase{
					numRedisNodes:   rn,
					numShards:       sc,
					clusterPartFunc: fw,
				})
			}
		}
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%s_%d_nodes_%d_shards", tc.clusterPartFunc.name, tc.numRedisNodes, tc.numShards), func(t *testing.T) {
			nodeIndexCounts := make([]int, tc.numRedisNodes)
			for i := 0; i < tc.numShards; i++ {
				ch := tc.clusterPartFunc.fn(strconv.Itoa(i))
				slot := redisSlot(ch)
				nodeIndex := detectRedisNodeIndex(slot, tc.numRedisNodes)
				nodeIndexCounts[nodeIndex]++
			}
			t.Logf("%s: %v", t.Name(), nodeIndexCounts)
		})
	}
}

func TestParseDeltaPush(t *testing.T) {
	tests := []struct {
		name           string
		input          string
		expectError    bool
		expectedResult deltaPublicationPush
	}{
		{
			name:        "valid data with colon in payload",
			input:       "d1:1234567890:epoch1:4:test:18:payload:with:colon",
			expectError: false,
			expectedResult: deltaPublicationPush{
				Offset:            1234567890,
				Epoch:             "epoch1",
				PrevPayloadLength: 4,
				PrevPayload:       "test",
				PayloadLength:     18,
				Payload:           "payload:with:colon",
			},
		},
		{
			name:        "valid data with empty payload",
			input:       "d1:1234567890:epoch2:0::0:",
			expectError: false,
			expectedResult: deltaPublicationPush{
				Offset:            1234567890,
				Epoch:             "epoch2",
				PrevPayloadLength: 0,
				PrevPayload:       "",
				PayloadLength:     0,
				Payload:           "",
			},
		},
		{
			name:        "invalid format - missing parts",
			input:       "d1:123456:epoch3",
			expectError: true,
		},
		{
			name:        "invalid offset",
			input:       "d1:notanumber:epoch4:4:test:5:hello",
			expectError: true,
		},
		{
			name:        "invalid prev payload length",
			input:       "d1:12:epoch4:invalid:test:5:hello",
			expectError: true,
		},
		{
			name:        "invalid prev payload length",
			input:       "d1:12:epoch4:4:test:invalid:hello",
			expectError: true,
		},
		{
			name:        "invalid format no payload",
			input:       "d1:12:epoch4:4:test:5:",
			expectError: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := parseDeltaPush(tc.input)
			if tc.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expectedResult, result)
			}
		})
	}
}

// 1000 streams with 100 messages in each stream, each message 500 bytes => 63MB in Redis.
// 2000 streams with 100 messages in each stream, each message 500 bytes => 126MB in Redis.
// 1000 streams with 200 messages in each stream, each message 500 bytes => 121MB in Redis.
// 1000 streams with 400 messages in each stream, each message 500 bytes => 242MB in Redis.
func TestRedisMemoryUsage(t *testing.T) {
	t.Skip()
	numStreams := 1000
	numMessagesInStream := 400
	messageSizeBytes := 500
	for _, tt := range historyRedisTests {
		t.Run(tt.Name, func(t *testing.T) {
			node := testNode(t)
			broker := newTestRedisBroker(t, node, tt.UseStreams, tt.UseCluster, tt.Port)
			defer func() { _ = node.Shutdown(context.Background()) }()
			defer stopRedisBroker(broker)
			rawData := []byte(randString(messageSizeBytes))
			for i := 0; i < numStreams; i++ {
				for j := 0; j < numMessagesInStream; j++ {
					_, _, err := broker.Publish("channel"+strconv.Itoa(i), rawData, PublishOptions{
						HistorySize: numMessagesInStream,
						HistoryTTL:  100 * time.Second,
					})
					require.NoError(t, err)
				}
			}
		})
	}
}

// See https://github.com/centrifugal/centrifugo/issues/925.
// If there is a deadlock – test will hang.
func TestRedisClientSubscribeRecoveryServerSubs(t *testing.T) {
	isInTest = true
	doneCh := make(chan struct{})
	defer close(doneCh)
	node := nodeWithRedisBroker(t, true, false, 6379)
	defer func() { _ = node.Shutdown(context.Background()) }()
	defer stopRedisBroker(node.broker.(*RedisBroker))

	channel1 := testChannelRedisClientSubscribeRecoveryDeadlock1
	channel2 := testChannelRedisClientSubscribeRecoveryDeadlock2

	for _, ch := range []string{channel1, channel2} {
		go func(channel string) {
			i := 0
			for {
				_, err := node.Publish(channel, []byte(`{"n": `+strconv.Itoa(i)+`}`), WithHistory(1000, time.Second))
				if err != nil {
					if !strings.Contains(err.Error(), "rueidis client is closing") {
						require.NoError(t, err)
					}
					return
				}
				time.Sleep(10 * time.Millisecond)
				i++
			}
		}(ch)
	}

	node.OnConnecting(func(ctx context.Context, event ConnectEvent) (ConnectReply, error) {
		return ConnectReply{
			Subscriptions: map[string]SubscribeOptions{
				channel1: {EnableRecovery: true},
				channel2: {EnableRecovery: true},
			},
		}, nil
	})

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(event SubscribeEvent, callback SubscribeCallback) {
			callback(SubscribeReply{
				Options: SubscribeOptions{EnableRecovery: true},
			}, nil)
		})
	})

	time.Sleep(10 * time.Millisecond)

	var wg sync.WaitGroup

	for i := 0; i < 1; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			client := newTestClient(t, node, "42")
			rwWrapper := testReplyWriterWrapper()
			err := client.connectCmd(&protocol.ConnectRequest{
				Subs: map[string]*protocol.SubscribeRequest{},
			}, &protocol.Command{}, time.Now(), rwWrapper.rw)
			require.NoError(t, err)
			require.Nil(t, rwWrapper.replies[0].Error)
			require.True(t, client.authenticated)
			_ = extractConnectReply(rwWrapper.replies)
			client.triggerConnect()
			client.scheduleOnConnectTimers()
		}()
	}

	waitGroupWithTimeout(t, &wg, 5*time.Second)
}

func waitGroupWithTimeout(t *testing.T, wg *sync.WaitGroup, timeout time.Duration) {
	c := make(chan struct{})
	go func() {
		wg.Wait()
		close(c)
	}()
	select {
	case <-c:
	case <-time.After(timeout):
		require.Fail(t, "timeout")
	}
}

// Similar to TestRedisClientSubscribeRecoveryServerSubs test, but uses client-side subscriptions.
func TestRedisClientSubscribeRecoveryClientSubs(t *testing.T) {
	doneCh := make(chan struct{})
	defer close(doneCh)
	node := nodeWithRedisBroker(t, true, false, 6379)
	defer func() { _ = node.Shutdown(context.Background()) }()
	defer stopRedisBroker(node.broker.(*RedisBroker))

	channel1 := "TestRedisClientSubscribeRecovery1"
	channel2 := "TestRedisClientSubscribeRecovery2"

	for _, channel := range []string{channel1, channel2} {
		go func(channel string) {
			i := 0
			for {
				_, err := node.Publish(channel, []byte(`{"n": `+strconv.Itoa(i)+`}`), WithHistory(1000, time.Second))
				if err != nil {
					if !strings.Contains(err.Error(), "rueidis client is closing") {
						require.NoError(t, err)
					}
					return
				}
				time.Sleep(10 * time.Millisecond)
				i++
			}
		}(channel)
	}

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(event SubscribeEvent, callback SubscribeCallback) {
			callback(SubscribeReply{
				Options: SubscribeOptions{EnableRecovery: true},
			}, nil)
		})
	})

	time.Sleep(10 * time.Millisecond)

	var wg sync.WaitGroup

	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			client := newTestClient(t, node, "42")
			connectClientV2(t, client)
			rwWrapper := testReplyWriterWrapper()
			err := client.handleSubscribe(&protocol.SubscribeRequest{
				Channel: channel1,
				Recover: true,
				Epoch:   "",
			}, &protocol.Command{}, time.Now(), rwWrapper.rw)
			require.NoError(t, err)
			require.Equal(t, 1, len(rwWrapper.replies))
			require.Nil(t, rwWrapper.replies[0].Error)
			res := extractSubscribeResult(rwWrapper.replies)
			require.Empty(t, res.Offset)
			require.NotZero(t, res.Epoch)
			require.True(t, res.Recovered)

			err = client.handleUnsubscribe(&protocol.UnsubscribeRequest{
				Channel: channel1,
			}, &protocol.Command{}, time.Now(), rwWrapper.rw)
			require.NoError(t, err)
			require.Equal(t, 2, len(rwWrapper.replies))
			require.Nil(t, rwWrapper.replies[1].Error)

			err = client.handleSubscribe(&protocol.SubscribeRequest{
				Channel: channel1,
				Recover: true,
				Epoch:   "",
			}, &protocol.Command{}, time.Now(), rwWrapper.rw)
			require.NoError(t, err)
			require.Equal(t, 3, len(rwWrapper.replies))
			require.Nil(t, rwWrapper.replies[2].Error)
			res = extractSubscribeResult(rwWrapper.replies)
			require.Empty(t, res.Offset)
			require.NotZero(t, res.Epoch)
			require.True(t, res.Recovered)
		}()
	}

	wg.Wait()
}
