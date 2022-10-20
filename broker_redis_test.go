//go:build integration

package centrifuge

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/require"
)

const (
	testRedisAddress  = "127.0.0.1:6379"
	testRedisPassword = ""
	testRedisDB       = 9
)

func getUniquePrefix() string {
	return "centrifuge-test-" + randString(3) + "-" + strconv.FormatInt(time.Now().UnixNano(), 10)
}

func newTestRedisBroker(tb testing.TB, n *Node, useStreams bool, useCluster bool) *RedisBroker {
	if useCluster {
		return NewTestRedisBrokerClusterWithPrefix(tb, n, getUniquePrefix(), useStreams)
	}
	return NewTestRedisBrokerWithPrefix(tb, n, getUniquePrefix(), useStreams)
}

func testNode(tb testing.TB) *Node {
	node, err := New(Config{
		LogLevel:   LogLevelDebug,
		LogHandler: func(entry LogEntry) {},
	})
	require.NoError(tb, err)
	return node
}

func testRedisConf() RedisShardConfig {
	return RedisShardConfig{
		Address:        testRedisAddress,
		DB:             testRedisDB,
		Password:       testRedisPassword,
		IOTimeout:      10 * time.Second,
		ConnectTimeout: 10 * time.Second,
	}
}

func NewTestRedisBrokerWithPrefix(tb testing.TB, n *Node, prefix string, useStreams bool) *RedisBroker {
	redisConf := testRedisConf()
	s, err := NewRedisShard(n, redisConf)
	require.NoError(tb, err)
	e, err := NewRedisBroker(n, RedisBrokerConfig{
		Prefix:         prefix,
		UseLists:       !useStreams,
		HistoryMetaTTL: 3600 * time.Second,
		Shards:         []*RedisShard{s},
	})
	require.NoError(tb, err)
	n.SetBroker(e)
	err = n.Run()
	if err != nil {
		tb.Fatal(err)
	}
	return e
}

func NewTestRedisBrokerClusterWithPrefix(tb testing.TB, n *Node, prefix string, useStreams bool) *RedisBroker {
	redisConf := RedisShardConfig{
		ClusterAddresses: []string{"localhost:7000", "localhost:7001", "localhost:7002"},
		Password:         testRedisPassword,
		IOTimeout:        10 * time.Second,
	}
	s, err := NewRedisShard(n, redisConf)
	require.NoError(tb, err)
	e, err := NewRedisBroker(n, RedisBrokerConfig{
		Prefix:         prefix,
		UseLists:       !useStreams,
		HistoryMetaTTL: 300 * time.Second,
		Shards:         []*RedisShard{s},
	})
	if err != nil {
		tb.Fatal(err)
	}
	n.SetBroker(e)
	err = n.Run()
	if err != nil {
		tb.Fatal(err)
	}
	return e
}

func NewTestRedisBrokerSentinel(tb testing.TB) *RedisBroker {
	n, _ := New(Config{})
	redisConf := RedisShardConfig{
		SentinelAddresses:  []string{"127.0.0.1:26379"},
		SentinelMasterName: "mymaster",
		IOTimeout:          10 * time.Second,
	}
	s, err := NewRedisShard(n, redisConf)
	require.NoError(tb, err)
	e, err := NewRedisBroker(n, RedisBrokerConfig{
		Shards: []*RedisShard{s},
	})
	if err != nil {
		tb.Fatal(err)
	}
	n.SetBroker(e)
	err = n.Run()
	if err != nil {
		tb.Fatal(err)
	}
	return e
}

func TestRedisBroker_NoShards(t *testing.T) {
	n, _ := New(Config{})
	_, err := NewRedisBroker(n, RedisBrokerConfig{})
	require.Error(t, err)
}

func TestRedisBrokerSentinel(t *testing.T) {
	e := NewTestRedisBrokerSentinel(t)
	_, _, err := e.History("test", HistoryFilter{
		Limit: -1,
	})
	require.NoError(t, err)
}

type redisTest struct {
	Name       string
	UseStreams bool
	UseCluster bool
}

var redisTests = []redisTest{
	{"lists", false, false},
	{"streams", true, false},
	{"lists_cluster", false, true},
	{"streams_cluster", true, true},
}

var benchRedisTests = func() (tests []redisTest) {
	for _, useCluster := range []bool{false, true} {
		if os.Getenv("CENTRIFUGE_REDIS_CLUSTER_BENCHMARKS") == "" && useCluster {
			continue
		}
		for _, useStream := range []bool{false, true} {
			var name string
			if useStream {
				name = "streams"
			} else {
				name = "lists"
			}
			if useCluster {
				name += "_cluster"
			}
			tests = append(tests, redisTest{
				Name:       name,
				UseCluster: useCluster,
				UseStreams: useStream,
			})
		}
	}
	return
}()

func TestRedisBroker(t *testing.T) {
	for _, tt := range redisTests {
		t.Run(tt.Name, func(t *testing.T) {
			node := testNode(t)

			e := newTestRedisBroker(t, node, tt.UseStreams, tt.UseCluster)
			defer func() { _ = node.Shutdown(context.Background()) }()

			_, err := e.Publish("channel", testPublicationData(), PublishOptions{})
			require.NoError(t, err)
			_, err = e.Publish("channel", testPublicationData(), PublishOptions{})
			require.NoError(t, err)
			require.NoError(t, e.Subscribe("channel"))
			require.NoError(t, e.Unsubscribe("channel"))

			rawData := []byte("{}")

			// test adding history
			_, err = e.Publish("channel", rawData, PublishOptions{HistorySize: 4, HistoryTTL: time.Second})
			require.NoError(t, err)
			pubs, _, err := e.History("channel", HistoryFilter{
				Limit: -1,
			})
			require.NoError(t, err)
			require.Equal(t, 1, len(pubs))
			require.Equal(t, pubs[0].Data, []byte("{}"))

			// test history limit
			_, err = e.Publish("channel", rawData, PublishOptions{HistorySize: 4, HistoryTTL: time.Second})
			require.NoError(t, err)
			_, err = e.Publish("channel", rawData, PublishOptions{HistorySize: 4, HistoryTTL: time.Second})
			require.NoError(t, err)
			_, err = e.Publish("channel", rawData, PublishOptions{HistorySize: 4, HistoryTTL: time.Second})
			require.NoError(t, err)
			pubs, _, err = e.History("channel", HistoryFilter{
				Limit: 2,
			})
			require.NoError(t, err)
			require.Equal(t, 2, len(pubs))

			// test history limit greater than history size
			_, err = e.Publish("channel", rawData, PublishOptions{HistorySize: 1, HistoryTTL: time.Second})
			require.NoError(t, err)
			_, err = e.Publish("channel", rawData, PublishOptions{HistorySize: 1, HistoryTTL: time.Second})
			require.NoError(t, err)
			_, err = e.Publish("channel", rawData, PublishOptions{HistorySize: 1, HistoryTTL: time.Second})
			require.NoError(t, err)

			// ask all history.
			pubs, _, err = e.History("channel", HistoryFilter{
				Limit: -1,
			})
			require.NoError(t, err)
			require.Equal(t, 1, len(pubs))

			// ask more history than history_size.
			pubs, _, err = e.History("channel", HistoryFilter{
				Limit: 2,
			})
			require.NoError(t, err)
			require.Equal(t, 1, len(pubs))

			// test publishing control message.
			err = e.PublishControl([]byte(""), "", "")
			require.NoError(t, nil, err)

			// test publishing control message.
			err = e.PublishControl([]byte(""), "test", "")
			require.NoError(t, nil, err)

			require.NoError(t, e.PublishJoin("channel", &ClientInfo{}))
			require.NoError(t, e.PublishLeave("channel", &ClientInfo{}))
		})
	}
}

func TestRedisCurrentPosition(t *testing.T) {
	for _, tt := range redisTests {
		t.Run(tt.Name, func(t *testing.T) {
			node := testNode(t)
			e := newTestRedisBroker(t, node, tt.UseStreams, tt.UseCluster)
			defer func() { _ = node.Shutdown(context.Background()) }()

			channel := "test-current-position"

			_, streamTop, err := e.History(channel, HistoryFilter{
				Limit: 0,
			})
			require.NoError(t, err)
			require.Equal(t, uint64(0), streamTop.Offset)

			rawData := []byte("{}")
			_, err = e.Publish(channel, rawData, PublishOptions{HistorySize: 10, HistoryTTL: 2 * time.Second})
			require.NoError(t, err)

			_, streamTop, err = e.History(channel, HistoryFilter{
				Limit: 0,
			})
			require.NoError(t, err)
			require.Equal(t, uint64(1), streamTop.Offset)
		})
	}
}

func TestRedisBrokerRecover(t *testing.T) {
	for _, tt := range redisTests {
		t.Run(tt.Name, func(t *testing.T) {
			node := testNode(t)
			e := newTestRedisBroker(t, node, tt.UseStreams, tt.UseCluster)
			defer func() { _ = node.Shutdown(context.Background()) }()

			rawData := []byte("{}")

			for i := 0; i < 5; i++ {
				_, err := e.Publish("channel", rawData, PublishOptions{HistorySize: 10, HistoryTTL: 2 * time.Second})
				require.NoError(t, err)
			}

			_, streamTop, err := e.History("channel", HistoryFilter{
				Limit: 0,
				Since: nil,
			})
			require.NoError(t, err)

			pubs, _, err := e.History("channel", HistoryFilter{
				Limit: -1,
				Since: &StreamPosition{Offset: 2, Epoch: streamTop.Epoch},
			})
			require.NoError(t, err)
			require.Equal(t, 3, len(pubs))
			require.Equal(t, uint64(3), pubs[0].Offset)
			require.Equal(t, uint64(4), pubs[1].Offset)
			require.Equal(t, uint64(5), pubs[2].Offset)

			for i := 0; i < 10; i++ {
				_, err := e.Publish("channel", rawData, PublishOptions{HistorySize: 10, HistoryTTL: 2 * time.Second})
				require.NoError(t, err)
			}

			pubs, _, err = e.History("channel", HistoryFilter{
				Limit: -1,
				Since: &StreamPosition{Offset: 0, Epoch: streamTop.Epoch},
			})
			require.NoError(t, err)
			require.Equal(t, 10, len(pubs))

			pubs, _, err = e.History("channel", HistoryFilter{
				Limit: -1,
				Since: &StreamPosition{Offset: 100, Epoch: streamTop.Epoch},
			})
			require.NoError(t, err)
			require.Equal(t, 0, len(pubs))

			require.NoError(t, e.RemoveHistory("channel"))
			pubs, _, err = e.History("channel", HistoryFilter{
				Limit: -1,
				Since: &StreamPosition{Offset: 2, Epoch: streamTop.Epoch},
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
	// Custom prefix to not collide with other tests.
	node := testNode(t)
	e := NewTestRedisBrokerWithPrefix(t, node, getUniquePrefix(), false)
	defer func() { _ = node.Shutdown(context.Background()) }()

	if e.shards[0].shard.useCluster {
		t.Skip("Channels command is not supported when Redis Cluster is used")
	}

	require.NoError(t, e.Subscribe("1-test"))
	require.NoError(t, e.Subscribe("1-test"))
	channels, err := pubSubChannels(t, e)
	require.NoError(t, err)
	if len(channels) != 1 {
		// Redis PUBSUB CHANNELS command looks like eventual consistent, so sometimes
		// it returns wrong results, sleeping for a while helps in such situations.
		// See https://gist.github.com/FZambia/80a5241e06b4662f7fe89cfaf24072c3
		time.Sleep(2000 * time.Millisecond)
		channels, err := pubSubChannels(t, e)
		require.NoError(t, err)
		require.Equal(t, 1, len(channels), fmt.Sprintf("%#v", channels))
	}

	require.NoError(t, e.Unsubscribe("1-test"))
	channels, err = pubSubChannels(t, e)
	require.NoError(t, err)
	if len(channels) != 0 {
		time.Sleep(2000 * time.Millisecond)
		channels, _ := pubSubChannels(t, e)
		require.Equal(t, 0, len(channels), fmt.Sprintf("%#v", channels))
	}

	var wg sync.WaitGroup

	// The same channel in parallel.
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			require.NoError(t, e.Subscribe("2-test"))
			require.NoError(t, e.Unsubscribe("2-test"))
		}()
	}
	wg.Wait()
	channels, err = pubSubChannels(t, e)
	require.NoError(t, err)

	if len(channels) != 0 {
		time.Sleep(2000 * time.Millisecond)
		channels, _ := pubSubChannels(t, e)
		require.Equal(t, 0, len(channels), fmt.Sprintf("%#v", channels))
	}

	// Different channels in parallel.
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			require.NoError(t, e.Subscribe("3-test-"+strconv.Itoa(i)))
			require.NoError(t, e.Unsubscribe("3-test-"+strconv.Itoa(i)))
		}(i)
	}
	wg.Wait()
	channels, err = pubSubChannels(t, e)
	require.Equal(t, nil, err)
	if len(channels) != 0 {
		time.Sleep(2000 * time.Millisecond)
		channels, err := pubSubChannels(t, e)
		require.NoError(t, err)
		require.Equal(t, 0, len(channels), fmt.Sprintf("%#v", channels))
	}

	// The same channel sequential.
	for i := 0; i < 10000; i++ {
		require.NoError(t, e.Subscribe("4-test"))
		require.NoError(t, e.Unsubscribe("4-test"))
	}
	channels, err = pubSubChannels(t, e)
	require.NoError(t, err)
	if len(channels) != 0 {
		time.Sleep(2000 * time.Millisecond)
		channels, _ := pubSubChannels(t, e)
		require.Equal(t, 0, len(channels), fmt.Sprintf("%#v", channels))
	}

	// Different channels sequential.
	for j := 0; j < 10; j++ {
		for i := 0; i < 10000; i++ {
			require.NoError(t, e.Subscribe("5-test-"+strconv.Itoa(i)))
			require.NoError(t, e.Unsubscribe("5-test-"+strconv.Itoa(i)))
		}
		channels, err = pubSubChannels(t, e)
		require.NoError(t, err)
		if len(channels) != 0 {
			time.Sleep(2000 * time.Millisecond)
			channels, err := pubSubChannels(t, e)
			require.NoError(t, err)
			require.Equal(t, 0, len(channels), fmt.Sprintf("%#v", channels))
		}
	}

	// Different channels subscribe only in parallel.
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			require.NoError(t, e.Subscribe("6-test-"+strconv.Itoa(i)))
		}(i)
	}
	wg.Wait()
	channels, err = pubSubChannels(t, e)
	require.NoError(t, err)
	if len(channels) != 100 {
		time.Sleep(2000 * time.Millisecond)
		channels, err := pubSubChannels(t, e)
		require.NoError(t, err)
		require.Equal(t, 100, len(channels), fmt.Sprintf("%#v", channels))
	}

	// Different channels unsubscribe only in parallel.
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			require.NoError(t, e.Unsubscribe("6-test-"+strconv.Itoa(i)))
		}(i)
	}
	wg.Wait()
	channels, err = pubSubChannels(t, e)
	require.NoError(t, err)
	if len(channels) != 0 {
		time.Sleep(2000 * time.Millisecond)
		channels, _ := pubSubChannels(t, e)
		require.Equal(t, 0, len(channels), fmt.Sprintf("%#v", channels))
	}

	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			require.NoError(t, e.Unsubscribe("7-test-"+strconv.Itoa(i)))
			require.NoError(t, e.Unsubscribe("8-test-"+strconv.Itoa(i)))
			require.NoError(t, e.Subscribe("8-test-"+strconv.Itoa(i)))
			require.NoError(t, e.Unsubscribe("9-test-"+strconv.Itoa(i)))
			require.NoError(t, e.Subscribe("7-test-"+strconv.Itoa(i)))
			require.NoError(t, e.Unsubscribe("8-test-"+strconv.Itoa(i)))
			require.NoError(t, e.Subscribe("9-test-"+strconv.Itoa(i)))
			require.NoError(t, e.Unsubscribe("9-test-"+strconv.Itoa(i)))
			require.NoError(t, e.Unsubscribe("7-test-"+strconv.Itoa(i)))
		}(i)
	}
	wg.Wait()
	channels, err = pubSubChannels(t, e)
	require.NoError(t, err)
	if len(channels) != 0 {
		time.Sleep(2000 * time.Millisecond)
		channels, err := pubSubChannels(t, e)
		require.NoError(t, err)
		require.Equal(t, 0, len(channels), fmt.Sprintf("%#v", channels))
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

	rand.Seed(time.Now().UnixNano())
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
	node := testNode(t)
	e := NewTestRedisBrokerWithPrefix(t, node, getUniquePrefix(), false)
	defer func() { _ = node.Shutdown(context.Background()) }()
	err := e.handleRedisClientMessage(&testBrokerEventHandler{HandlePublicationFunc: func(ch string, pub *Publication, sp StreamPosition) error {
		require.Equal(t, "test", ch)
		require.Equal(t, uint64(16901), sp.Offset)
		require.Equal(t, "xyz", sp.Epoch)
		return nil
	}}, e.messageChannelID(e.shards[0].shard, "test"), []byte("__p1:16901:xyz__dsdsd"))
	require.Error(t, err)

	err = e.handleRedisClientMessage(&testBrokerEventHandler{HandlePublicationFunc: func(ch string, pub *Publication, sp StreamPosition) error {
		return nil
	}}, e.messageChannelID(e.shards[0].shard, "test"), []byte("__p1:16901"))
	require.Error(t, err)

	pub := &protocol.Publication{
		Data: []byte("{}"),
	}
	data, err := pub.MarshalVT()
	require.NoError(t, err)
	var publicationHandlerCalled bool
	err = e.handleRedisClientMessage(&testBrokerEventHandler{HandlePublicationFunc: func(ch string, pub *Publication, sp StreamPosition) error {
		publicationHandlerCalled = true
		require.Equal(t, "test", ch)
		require.Equal(t, uint64(16901), sp.Offset)
		require.Equal(t, "xyz", sp.Epoch)
		return nil
	}}, e.messageChannelID(e.shards[0].shard, "test"), []byte("__p1:16901:xyz__"+string(data)))
	require.NoError(t, err)
	require.True(t, publicationHandlerCalled)

	info := &protocol.ClientInfo{
		User: "12",
	}
	data, err = info.MarshalVT()
	require.NoError(t, err)
	var joinHandlerCalled bool
	err = e.handleRedisClientMessage(&testBrokerEventHandler{HandleJoinFunc: func(ch string, info *ClientInfo) error {
		joinHandlerCalled = true
		require.Equal(t, "test", ch)
		require.Equal(t, "12", info.UserID)
		return nil
	}}, e.messageChannelID(e.shards[0].shard, "test"), append(joinTypePrefix, data...))
	require.NoError(t, err)
	require.True(t, joinHandlerCalled)

	var leaveHandlerCalled bool
	err = e.handleRedisClientMessage(&testBrokerEventHandler{HandleLeaveFunc: func(ch string, info *ClientInfo) error {
		leaveHandlerCalled = true
		require.Equal(t, "test", ch)
		require.Equal(t, "12", info.UserID)
		return nil
	}}, e.messageChannelID(e.shards[0].shard, "test"), append(leaveTypePrefix, data...))
	require.NoError(t, err)
	require.True(t, leaveHandlerCalled)
}

func BenchmarkRedisExtractPushData(b *testing.B) {
	data := []byte(`__p1:16901:xyz__\x12\nchat:index\x1aU\"\x0e{\"input\":\"__\"}*C\n\x0242\x12$37cb00a9-bcfa-4284-a1ae-607c7da3a8f4\x1a\x15{\"name\": \"Alexander\"}\"\x00`)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, sp, ok := extractPushData(data)
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
	pushData, pushType, sp, ok := extractPushData(data)
	require.True(t, ok)
	require.Equal(t, pubPushType, pushType)
	require.Equal(t, uint64(16901), sp.Offset)
	require.Equal(t, "xyz.123", sp.Epoch)
	require.Equal(t, []byte(`\x12\nchat:index\x1aU\"\x0e{\"input\":\"__\"}*C\n\x0242\x12$37cb00a9-bcfa-4284-a1ae-607c7da3a8f4\x1a\x15{\"name\": \"Alexander\"}\"\x00`), pushData)

	data = []byte(`__16901__\x12\nchat:index\x1aU\"\x0e{\"input\":\"__\"}*C\n\x0242\x12$37cb00a9-bcfa-4284-a1ae-607c7da3a8f4\x1a\x15{\"name\": \"Alexander\"}\"\x00`)
	pushData, pushType, sp, ok = extractPushData(data)
	require.True(t, ok)
	require.Equal(t, pubPushType, pushType)
	require.Equal(t, uint64(16901), sp.Offset)
	require.Equal(t, "", sp.Epoch)
	require.Equal(t, []byte(`\x12\nchat:index\x1aU\"\x0e{\"input\":\"__\"}*C\n\x0242\x12$37cb00a9-bcfa-4284-a1ae-607c7da3a8f4\x1a\x15{\"name\": \"Alexander\"}\"\x00`), pushData)

	data = []byte(`\x12\nchat:index\x1aU\"\x0e{\"input\":\"__\"}*C\n\x0242\x12$37cb00a9-bcfa-4284-a1ae-607c7da3a8f4\x1a\x15{\"name\": \"Alexander\"}\"\x00`)
	pushData, pushType, sp, ok = extractPushData(data)
	require.True(t, ok)
	require.Equal(t, pubPushType, pushType)
	require.Equal(t, uint64(0), sp.Offset)
	require.Equal(t, []byte(`\x12\nchat:index\x1aU\"\x0e{\"input\":\"__\"}*C\n\x0242\x12$37cb00a9-bcfa-4284-a1ae-607c7da3a8f4\x1a\x15{\"name\": \"Alexander\"}\"\x00`), pushData)

	data = []byte(`__4294967337__\x12\nchat:index\x1aU\"\x0e{\"input\":\"__\"}*C\n\x0242\x12$37cb00a9-bcfa-4284-a1ae-607c7da3a8f4\x1a\x15{\"name\": \"Alexander\"}\"\x00`)
	pushData, pushType, sp, ok = extractPushData(data)
	require.True(t, ok)
	require.Equal(t, pubPushType, pushType)
	require.Equal(t, uint64(4294967337), sp.Offset)
	require.Equal(t, []byte(`\x12\nchat:index\x1aU\"\x0e{\"input\":\"__\"}*C\n\x0242\x12$37cb00a9-bcfa-4284-a1ae-607c7da3a8f4\x1a\x15{\"name\": \"Alexander\"}\"\x00`), pushData)

	data = []byte(`__j__\x12\nchat:index\x1aU\"\x0e{\"input\":\"__\"}*C\n\x0242\x12$37cb00a9-bcfa-4284-a1ae-607c7da3a8f4\x1a\x15{\"name\": \"Alexander\"}\"\x00`)
	pushData, pushType, sp, ok = extractPushData(data)
	require.True(t, ok)
	require.Equal(t, joinPushType, pushType)
	require.Equal(t, uint64(0), sp.Offset)
	require.Equal(t, []byte(`\x12\nchat:index\x1aU\"\x0e{\"input\":\"__\"}*C\n\x0242\x12$37cb00a9-bcfa-4284-a1ae-607c7da3a8f4\x1a\x15{\"name\": \"Alexander\"}\"\x00`), pushData)

	data = []byte(`__l__\x12\nchat:index\x1aU\"\x0e{\"input\":\"__\"}*C\n\x0242\x12$37cb00a9-bcfa-4284-a1ae-607c7da3a8f4\x1a\x15{\"name\": \"Alexander\"}\"\x00`)
	pushData, pushType, sp, ok = extractPushData(data)
	require.True(t, ok)
	require.Equal(t, leavePushType, pushType)
	require.Equal(t, uint64(0), sp.Offset)
	require.Equal(t, []byte(`\x12\nchat:index\x1aU\"\x0e{\"input\":\"__\"}*C\n\x0242\x12$37cb00a9-bcfa-4284-a1ae-607c7da3a8f4\x1a\x15{\"name\": \"Alexander\"}\"\x00`), pushData)

	data = []byte(`____\x12\nchat:index\x1aU\"\x0e{\"input\":\"__\"}*C\n\x0242\x12$37cb00a9-bcfa-4284-a1ae-607c7da3a8f4\x1a\x15{\"name\": \"Alexander\"}\"\x00`)
	_, _, _, ok = extractPushData(data)
	require.False(t, ok)

	data = []byte(`__a__\x12\nchat:index\x1aU\"\x0e{\"input\":\"__\"}*C\n\x0242\x12$37cb00a9-bcfa-4284-a1ae-607c7da3a8f4\x1a\x15{\"name\": \"Alexander\"}\"\x00`)
	_, _, _, ok = extractPushData(data)
	require.False(t, ok)
}

func TestNode_OnSurvey_TwoNodes(t *testing.T) {
	redisConf := testRedisConf()

	node1, _ := New(Config{})

	s, err := NewRedisShard(node1, redisConf)
	require.NoError(t, err)

	prefix := getUniquePrefix()

	e1, _ := NewRedisBroker(node1, RedisBrokerConfig{
		Prefix: prefix,
		Shards: []*RedisShard{s},
	})
	node1.SetBroker(e1)
	_ = node1.Run()
	defer func() { _ = node1.Shutdown(context.Background()) }()

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
	e2, _ := NewRedisBroker(node2, RedisBrokerConfig{
		Prefix: prefix,
		Shards: []*RedisShard{s2},
	})
	node2.SetBroker(e2)
	_ = node2.Run()
	defer func() { _ = node2.Shutdown(context.Background()) }()

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
}

func TestNode_OnNotification_TwoNodes(t *testing.T) {
	redisConf := testRedisConf()

	node1, _ := New(Config{})

	s, err := NewRedisShard(node1, redisConf)
	require.NoError(t, err)

	prefix := getUniquePrefix()

	e1, _ := NewRedisBroker(node1, RedisBrokerConfig{
		Prefix: prefix,
		Shards: []*RedisShard{s},
	})
	node1.SetBroker(e1)
	_ = node1.Run()
	defer func() { _ = node1.Shutdown(context.Background()) }()

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
	e2, _ := NewRedisBroker(node2, RedisBrokerConfig{
		Prefix: prefix,
		Shards: []*RedisShard{s2},
	})
	node2.SetBroker(e2)
	_ = node2.Run()
	defer func() { _ = node2.Shutdown(context.Background()) }()

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
}

func TestRedisPubSubTwoNodes(t *testing.T) {
	redisConf := testRedisConf()

	node1, _ := New(Config{})

	s, err := NewRedisShard(node1, redisConf)
	require.NoError(t, err)

	prefix := getUniquePrefix()

	e1, _ := NewRedisBroker(node1, RedisBrokerConfig{
		Prefix:           prefix,
		Shards:           []*RedisShard{s},
		NumPubSubShards:  4,
		NumPubSubWorkers: 2,
	})

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
		HandlePublicationFunc: func(ch string, pub *Publication, sp StreamPosition) error {
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
	_ = e1.Run(brokerEventHandler)

	for i := 0; i < msgNum; i++ {
		require.NoError(t, e1.Subscribe("test"+strconv.Itoa(i)))
	}

	node2, _ := New(Config{})
	s2, err := NewRedisShard(node2, redisConf)
	require.NoError(t, err)

	e2, _ := NewRedisBroker(node2, RedisBrokerConfig{
		Prefix: prefix,
		Shards: []*RedisShard{s2},
	})
	node2.SetBroker(e2)
	_ = node2.Run()
	defer func() { _ = node2.Shutdown(context.Background()) }()

	for i := 0; i < msgNum; i++ {
		_, err = node2.Publish("test"+strconv.Itoa(i), []byte("123"))
		require.NoError(t, err)
		err = e2.PublishJoin("test"+strconv.Itoa(i), &ClientInfo{})
		require.NoError(t, err)
		err = e2.PublishLeave("test"+strconv.Itoa(i), &ClientInfo{})
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
			redisConf := testRedisConf()
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
	for _, tt := range benchRedisTests {
		b.Run(tt.Name, func(b *testing.B) {
			node := testNode(b)
			e := newTestRedisBroker(b, node, tt.UseStreams, tt.UseCluster)
			defer func() { _ = node.Shutdown(context.Background()) }()
			rawData := []byte(`{"bench": true}`)
			b.SetParallelism(128)
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					_, err := e.Publish("channel", rawData, PublishOptions{})
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}

const benchmarkNumDifferentChannels = 1000

func BenchmarkRedisPublish_ManyCh(b *testing.B) {
	for _, tt := range benchRedisTests {
		b.Run(tt.Name, func(b *testing.B) {
			node := testNode(b)
			e := newTestRedisBroker(b, node, tt.UseStreams, tt.UseCluster)
			defer func() { _ = node.Shutdown(context.Background()) }()
			rawData := []byte(`{"bench": true}`)
			b.SetParallelism(128)
			j := int32(0)
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					jj := atomic.AddInt32(&j, 1)
					channel := "channel" + strconv.Itoa(int(jj)%benchmarkNumDifferentChannels)
					_, err := e.Publish(channel, rawData, PublishOptions{})
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}

func BenchmarkRedisPublish_History_1Ch(b *testing.B) {
	for _, tt := range benchRedisTests {
		b.Run(tt.Name, func(b *testing.B) {
			node := testNode(b)
			e := newTestRedisBroker(b, node, tt.UseStreams, tt.UseCluster)
			defer func() { _ = node.Shutdown(context.Background()) }()
			rawData := []byte(`{"bench": true}`)
			chOpts := PublishOptions{HistorySize: 100, HistoryTTL: 100 * time.Second}
			b.SetParallelism(128)
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					var err error
					pos, err := e.Publish("channel", rawData, chOpts)
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
	for _, tt := range benchRedisTests {
		b.Run(tt.Name, func(b *testing.B) {
			node := testNode(b)
			e := newTestRedisBroker(b, node, tt.UseStreams, tt.UseCluster)
			defer func() { _ = node.Shutdown(context.Background()) }()
			rawData := []byte(`{"bench": true}`)
			chOpts := PublishOptions{HistorySize: 100, HistoryTTL: 100 * time.Second}
			b.SetParallelism(128)
			j := int32(0)
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					jj := atomic.AddInt32(&j, 1)
					channel := "channel" + strconv.Itoa(int(jj)%benchmarkNumDifferentChannels)
					var err error
					pos, err := e.Publish(channel, rawData, chOpts)
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
	type test struct {
		Name       string
		UseCluster bool
	}
	tests := []test{
		{"non_cluster", false},
	}
	if os.Getenv("CENTRIFUGE_REDIS_CLUSTER_BENCHMARKS") != "" {
		tests = append(tests, test{"with_cluster", true})
	}

	for _, tt := range tests {
		b.Run(tt.Name, func(b *testing.B) {
			node := testNode(b)
			e := newTestRedisBroker(b, node, false, tt.UseCluster)
			defer func() { _ = node.Shutdown(context.Background()) }()
			i := int32(0)
			b.SetParallelism(128)
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					ii := atomic.AddInt32(&i, 1)
					err := e.Subscribe("subscribe" + strconv.Itoa(int(ii)))
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}

func BenchmarkRedisHistory_1Ch(b *testing.B) {
	for _, tt := range benchRedisTests {
		b.Run(tt.Name, func(b *testing.B) {
			node := testNode(b)
			e := newTestRedisBroker(b, node, tt.UseStreams, tt.UseCluster)
			defer func() { _ = node.Shutdown(context.Background()) }()
			rawData := []byte("{}")
			for i := 0; i < 4; i++ {
				_, err := e.Publish("channel", rawData, PublishOptions{HistorySize: 4, HistoryTTL: 300 * time.Second})
				require.NoError(b, err)
			}
			b.SetParallelism(128)
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					_, err := e.node.History("channel", WithLimit(-1), WithSince(nil))
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}

func BenchmarkRedisRecover_1Ch(b *testing.B) {
	for _, tt := range benchRedisTests {
		b.Run(tt.Name, func(b *testing.B) {
			node := testNode(b)
			e := newTestRedisBroker(b, node, tt.UseStreams, tt.UseCluster)
			defer func() { _ = node.Shutdown(context.Background()) }()
			rawData := []byte("{}")
			numMessages := 1000
			numMissing := 5
			for i := 1; i <= numMessages; i++ {
				_, err := e.Publish("channel", rawData, PublishOptions{HistorySize: numMessages, HistoryTTL: 300 * time.Second})
				require.NoError(b, err)
			}
			b.SetParallelism(128)
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					pubs, _, err := e.History("channel", HistoryFilter{
						Limit: -1,
						Since: &StreamPosition{Offset: uint64(numMessages - numMissing), Epoch: ""},
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

func nodeWithRedisBroker(tb testing.TB, useStreams bool, useCluster bool) *Node {
	n, err := New(Config{})
	if err != nil {
		tb.Fatal(err)
	}
	newTestRedisBroker(tb, n, useStreams, useCluster)
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

func testRedisClientSubscribeRecover(t *testing.T, tt recoverTest, useStreams bool, useCluster bool) {
	node := nodeWithRedisBroker(t, useStreams, useCluster)
	node.config.RecoveryMaxPublicationLimit = tt.Limit
	defer func() { _ = node.Shutdown(context.Background()) }()

	channel := "test_recovery_redis_" + tt.Name

	for i := 1; i <= tt.NumPublications; i++ {
		_, err := node.Publish(channel, []byte(`{"n": `+strconv.Itoa(i)+`}`), WithHistory(tt.HistorySize, time.Duration(tt.HistoryTTLSeconds)*time.Second))
		require.NoError(t, err)
	}

	time.Sleep(time.Duration(tt.Sleep) * time.Second)

	_, streamTop, err := node.broker.History(channel, HistoryFilter{
		Limit: 0,
		Since: nil,
	})
	require.NoError(t, err)

	historyResult, err := node.recoverHistory(channel, StreamPosition{tt.SinceOffset, streamTop.Epoch})
	require.NoError(t, err)
	recoveredPubs, recovered := isRecovered(historyResult, tt.SinceOffset, streamTop.Epoch)
	require.Equal(t, tt.NumRecovered, len(recoveredPubs))
	require.Equal(t, tt.Recovered, recovered)
}

func TestRedisClientSubscribeRecoverStreams(t *testing.T) {
	for _, tt := range recoverTests {
		t.Run(tt.Name, func(t *testing.T) {
			testRedisClientSubscribeRecover(t, tt, true, false)
		})
	}
}

func TestRedisClientSubscribeRecoverLists(t *testing.T) {
	for _, tt := range recoverTests {
		t.Run(tt.Name, func(t *testing.T) {
			testRedisClientSubscribeRecover(t, tt, false, false)
		})
	}
}

func TestRedisClientSubscribeRecoverStreamsCluster(t *testing.T) {
	for _, tt := range recoverTests {
		t.Run(tt.Name, func(t *testing.T) {
			testRedisClientSubscribeRecover(t, tt, true, true)
		})
	}
}

func TestRedisClientSubscribeRecoverListsCluster(t *testing.T) {
	for _, tt := range recoverTests {
		t.Run(tt.Name, func(t *testing.T) {
			testRedisClientSubscribeRecover(t, tt, false, true)
		})
	}
}

func TestRedisHistoryIteration(t *testing.T) {
	for _, tt := range redisTests {
		t.Run(tt.Name, func(t *testing.T) {
			node := testNode(t)
			e := newTestRedisBroker(t, node, tt.UseStreams, tt.UseCluster)
			defer func() { _ = node.Shutdown(context.Background()) }()
			it := historyIterationTest{100, 5}
			startPosition := it.prepareHistoryIteration(t, e.node)
			it.testHistoryIteration(t, e.node, startPosition)
		})
	}
}

func TestRedisHistoryIterationReverse(t *testing.T) {
	for _, tt := range redisTests {
		t.Run(tt.Name, func(t *testing.T) {
			if !tt.UseStreams || tt.UseCluster {
				t.Skip()
			}
			node := testNode(t)
			e := newTestRedisBroker(t, node, tt.UseStreams, tt.UseCluster)
			defer func() { _ = node.Shutdown(context.Background()) }()
			it := historyIterationTest{100, 5}
			startPosition := it.prepareHistoryIteration(t, e.node)
			it.testHistoryIterationReverse(t, e.node, startPosition)
		})
	}
}

func BenchmarkRedisHistoryIteration(b *testing.B) {
	for _, tt := range benchRedisTests {
		b.Run(tt.Name, func(b *testing.B) {
			node := testNode(b)
			e := newTestRedisBroker(b, node, tt.UseStreams, tt.UseCluster)
			defer func() { _ = node.Shutdown(context.Background()) }()
			it := historyIterationTest{10000, 100}
			startPosition := it.prepareHistoryIteration(b, e.node)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				it.testHistoryIteration(b, e.node, startPosition)
			}
		})
	}
}

type throughputTest struct {
	NumPubSubWorkers int
}

var throughputTests = []throughputTest{
	{1}, {2}, {4}, {8},
}

func BenchmarkPubSubThroughput(b *testing.B) {
	for _, tt := range throughputTests {
		b.Run(fmt.Sprintf("%dwrk", tt.NumPubSubWorkers), func(b *testing.B) {
			redisConf := testRedisConf()

			node1, _ := New(Config{})

			s, err := NewRedisShard(node1, redisConf)
			require.NoError(b, err)

			prefix := getUniquePrefix()

			e1, _ := NewRedisBroker(node1, RedisBrokerConfig{
				Prefix:           prefix,
				Shards:           []*RedisShard{s},
				NumPubSubWorkers: tt.NumPubSubWorkers,
			})

			numChannels := 1024
			pubCh := make(chan struct{}, 1024)
			brokerEventHandler := &testBrokerEventHandler{
				HandleControlFunc: func(bytes []byte) error {
					return nil
				},
				HandlePublicationFunc: func(ch string, pub *Publication, sp StreamPosition) error {
					pubCh <- struct{}{}
					return nil
				},
			}
			_ = e1.Run(brokerEventHandler)

			for i := 0; i < numChannels; i++ {
				require.NoError(b, e1.Subscribe("test"+strconv.Itoa(i)))
			}

			node2, _ := New(Config{})
			s2, err := NewRedisShard(node2, redisConf)
			require.NoError(b, err)

			e2, _ := NewRedisBroker(node2, RedisBrokerConfig{
				Prefix: prefix,
				Shards: []*RedisShard{s2},
			})
			node2.SetBroker(e2)
			_ = node2.Run()
			defer func() { _ = node2.Shutdown(context.Background()) }()

			b.ReportAllocs()
			b.SetParallelism(128)
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
