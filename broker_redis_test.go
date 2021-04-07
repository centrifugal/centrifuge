// +build integration

package centrifuge

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/centrifugal/protocol"
	"github.com/gomodule/redigo/redis"
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
	conf := DefaultConfig
	conf.LogLevel = LogLevelDebug
	conf.LogHandler = func(entry LogEntry) {}
	node, err := New(conf)
	require.NoError(tb, err)
	return node
}

func testRedisConf() RedisShardConfig {
	return RedisShardConfig{
		Address:        testRedisAddress,
		DB:             testRedisDB,
		Password:       testRedisPassword,
		ReadTimeout:    100 * time.Second,
		ConnectTimeout: 10 * time.Second,
		WriteTimeout:   10 * time.Second,
	}
}

func NewTestRedisBrokerWithPrefix(tb testing.TB, n *Node, prefix string, useStreams bool) *RedisBroker {
	redisConf := testRedisConf()
	s, err := NewRedisShard(n, redisConf)
	require.NoError(tb, err)
	e, err := NewRedisBroker(n, RedisBrokerConfig{
		Prefix:         prefix,
		UseStreams:     useStreams,
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

func NewTestRedisBrokerClusterWithPrefix(tb testing.TB, n *Node, prefix string, useStreams bool) *RedisBroker {
	redisConf := RedisShardConfig{
		ClusterAddresses: []string{"localhost:7000", "localhost:7001", "localhost:7002"},
		Password:         testRedisPassword,
		ReadTimeout:      100 * time.Second,
	}
	s, err := NewRedisShard(n, redisConf)
	require.NoError(tb, err)
	e, err := NewRedisBroker(n, RedisBrokerConfig{
		Prefix:         prefix,
		UseStreams:     useStreams,
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
		ReadTimeout:        100 * time.Second,
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

var redisTests = []struct {
	Name       string
	UseStreams bool
	UseCluster bool
}{
	{"lists", false, false},
	{"streams", true, false},
	{"lists_cluster", false, true},
	{"streams_cluster", true, true},
}

var benchRedisTests = []struct {
	Name       string
	UseStreams bool
}{
	{"lists", false},
	{"streams", true},
}

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
	conn := e.shards[0].pool.Get()
	defer func() { require.NoError(t, conn.Close()) }()
	return redis.Strings(conn.Do("PUBSUB", "channels", e.messagePrefix+"*"))
}

func TestRedisBrokerSubscribeUnsubscribe(t *testing.T) {
	// Custom prefix to not collide with other tests.
	node := testNode(t)
	e := NewTestRedisBrokerWithPrefix(t, node, getUniquePrefix(), false)
	defer func() { _ = node.Shutdown(context.Background()) }()

	if e.shards[0].useCluster {
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
		time.Sleep(500 * time.Millisecond)
		channels, err := pubSubChannels(t, e)
		require.NoError(t, err)
		require.Equal(t, 1, len(channels), fmt.Sprintf("%#v", channels))
	}

	require.NoError(t, e.Unsubscribe("1-test"))
	channels, err = pubSubChannels(t, e)
	require.NoError(t, err)
	if len(channels) != 0 {
		time.Sleep(500 * time.Millisecond)
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
		time.Sleep(500 * time.Millisecond)
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
		time.Sleep(500 * time.Millisecond)
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
		time.Sleep(500 * time.Millisecond)
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
			time.Sleep(500 * time.Millisecond)
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
		time.Sleep(500 * time.Millisecond)
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
		time.Sleep(500 * time.Millisecond)
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
		time.Sleep(500 * time.Millisecond)
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
	}}, e.messageChannelID("test"), []byte("__p1:16901:xyz__dsdsd"))
	require.Error(t, err)

	err = e.handleRedisClientMessage(&testBrokerEventHandler{HandlePublicationFunc: func(ch string, pub *Publication, sp StreamPosition) error {
		return nil
	}}, e.messageChannelID("test"), []byte("__p1:16901"))
	require.Error(t, err)

	pub := &protocol.Publication{
		Data: []byte("{}"),
	}
	data, err := pub.Marshal()
	require.NoError(t, err)
	var publicationHandlerCalled bool
	err = e.handleRedisClientMessage(&testBrokerEventHandler{HandlePublicationFunc: func(ch string, pub *Publication, sp StreamPosition) error {
		publicationHandlerCalled = true
		require.Equal(t, "test", ch)
		require.Equal(t, uint64(16901), sp.Offset)
		require.Equal(t, "xyz", sp.Epoch)
		return nil
	}}, e.messageChannelID("test"), []byte("__p1:16901:xyz__"+string(data)))
	require.NoError(t, err)
	require.True(t, publicationHandlerCalled)

	info := &protocol.ClientInfo{
		User: "12",
	}
	data, err = info.Marshal()
	require.NoError(t, err)
	var joinHandlerCalled bool
	err = e.handleRedisClientMessage(&testBrokerEventHandler{HandleJoinFunc: func(ch string, info *ClientInfo) error {
		joinHandlerCalled = true
		require.Equal(t, "test", ch)
		require.Equal(t, "12", info.UserID)
		return nil
	}}, e.messageChannelID("test"), append(joinTypePrefix, data...))
	require.NoError(t, err)
	require.True(t, joinHandlerCalled)

	var leaveHandlerCalled bool
	err = e.handleRedisClientMessage(&testBrokerEventHandler{HandleLeaveFunc: func(ch string, info *ClientInfo) error {
		leaveHandlerCalled = true
		require.Equal(t, "test", ch)
		require.Equal(t, "12", info.UserID)
		return nil
	}}, e.messageChannelID("test"), append(leaveTypePrefix, data...))
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

	node1, _ := New(DefaultConfig)

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

	node2, _ := New(DefaultConfig)

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

	results, err := node1.Survey(context.Background(), "test_op", nil)
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

	node1, _ := New(DefaultConfig)

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

	node2, _ := New(DefaultConfig)

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

	node1, _ := New(DefaultConfig)

	s, err := NewRedisShard(node1, redisConf)
	require.NoError(t, err)

	prefix := getUniquePrefix()

	e1, _ := NewRedisBroker(node1, RedisBrokerConfig{
		Prefix: prefix,
		Shards: []*RedisShard{s},
	})
	messageCh := make(chan struct{})
	joinCh := make(chan struct{})
	leaveCh := make(chan struct{})
	brokerEventHandler := &testBrokerEventHandler{
		HandleControlFunc: func(bytes []byte) error {
			return nil
		},
		HandlePublicationFunc: func(ch string, pub *Publication, sp StreamPosition) error {
			close(messageCh)
			return nil
		},
		HandleJoinFunc: func(ch string, info *ClientInfo) error {
			close(joinCh)
			return nil
		},
		HandleLeaveFunc: func(ch string, info *ClientInfo) error {
			close(leaveCh)
			return nil
		},
	}
	_ = e1.Run(brokerEventHandler)
	require.NoError(t, e1.Subscribe("test"))

	node2, _ := New(DefaultConfig)
	s2, err := NewRedisShard(node2, redisConf)
	require.NoError(t, err)

	e2, _ := NewRedisBroker(node2, RedisBrokerConfig{
		Prefix: prefix,
		Shards: []*RedisShard{s2},
	})
	node2.SetBroker(e2)
	_ = node2.Run()
	defer func() { _ = node2.Shutdown(context.Background()) }()

	_, err = node2.Publish("test", []byte("123"))
	require.NoError(t, err)
	err = e2.PublishJoin("test", &ClientInfo{})
	require.NoError(t, err)
	err = e2.PublishLeave("test", &ClientInfo{})
	require.NoError(t, err)

	select {
	case <-messageCh:
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

var benchSurveyTests = []struct {
	Name          string
	NumOtherNodes int
	DataSize      int
}{
	{"1 node 512B", 0, 512},
	{"2 nodes 512B", 1, 512},
	{"3 nodes 512B", 2, 512},
	{"4 nodes 512B", 3, 512},
	{"5 nodes 512B", 4, 512},
	{"10 nodes 512B", 9, 512},
	{"100 nodes 512B", 99, 512},
	{"1 node 4KB", 0, 4096},
	{"2 nodes 4KB", 1, 4096},
	{"3 nodes 4KB", 2, 4096},
	{"4 nodes 4KB", 3, 4096},
	{"5 nodes 4KB", 4, 4096},
	{"10 nodes 4KB", 9, 4096},
	{"100 nodes 4KB", 99, 4096},
}

func BenchmarkRedisSurvey(b *testing.B) {
	for _, tt := range benchSurveyTests {
		b.Run(tt.Name, func(b *testing.B) {
			prefix := getUniquePrefix()
			redisConf := testRedisConf()
			data := make([]byte, tt.DataSize)

			for i := 0; i < tt.NumOtherNodes; i++ {
				node, _ := New(DefaultConfig)
				s, err := NewRedisShard(node, redisConf)
				if err != nil {
					b.Fatal(err)
				}
				broker, _ := NewRedisBroker(node, RedisBrokerConfig{
					Prefix: prefix,
					Shards: []*RedisShard{s},
				})
				node.SetBroker(broker)
				_ = node.Run()

				node.OnSurvey(func(event SurveyEvent, callback SurveyCallback) {
					callback(SurveyReply{
						Data: data,
						Code: 1,
					})
				})
			}

			node, _ := New(DefaultConfig)
			s, err := NewRedisShard(node, redisConf)
			if err != nil {
				b.Fatal(err)
			}
			broker, _ := NewRedisBroker(node, RedisBrokerConfig{
				Prefix: prefix,
				Shards: []*RedisShard{s},
			})
			node.SetBroker(broker)
			_ = node.Run()

			node.OnSurvey(func(event SurveyEvent, callback SurveyCallback) {
				callback(SurveyReply{
					Data: data,
					Code: 2,
				})
			})

			waitAllNodes(b, node, tt.NumOtherNodes+1)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := node.Survey(context.Background(), "test_op", nil)
				if err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
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
	node := testNode(b)
	e := newTestRedisBroker(b, node, false, false)
	defer func() { _ = node.Shutdown(context.Background()) }()
	rawData := []byte(`{"bench": true}`)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := e.Publish("channel", rawData, PublishOptions{})
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRedisPublish_1Ch_Parallel(b *testing.B) {
	node := testNode(b)
	e := newTestRedisBroker(b, node, false, false)
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
}

const benchmarkNumDifferentChannels = 1000

func BenchmarkRedisPublish_ManyCh(b *testing.B) {
	node := testNode(b)
	e := newTestRedisBroker(b, node, false, false)
	defer func() { _ = node.Shutdown(context.Background()) }()
	rawData := []byte(`{"bench": true}`)
	j := 0
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		channel := "channel" + strconv.Itoa(j%benchmarkNumDifferentChannels)
		j++
		_, err := e.Publish(channel, rawData, PublishOptions{})
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRedisPublish_ManyCh_Parallel(b *testing.B) {
	node := testNode(b)
	e := newTestRedisBroker(b, node, false, false)
	defer func() { _ = node.Shutdown(context.Background()) }()
	rawData := []byte(`{"bench": true}`)
	b.SetParallelism(128)
	j := 0
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			channel := "channel" + strconv.Itoa(j%benchmarkNumDifferentChannels)
			j++
			_, err := e.Publish(channel, rawData, PublishOptions{})
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkRedisPublish_History_1Ch(b *testing.B) {
	for _, tt := range benchRedisTests {
		b.Run(tt.Name, func(b *testing.B) {
			node := testNode(b)
			e := newTestRedisBroker(b, node, tt.UseStreams, false)
			defer func() { _ = node.Shutdown(context.Background()) }()
			rawData := []byte(`{"bench": true}`)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				chOpts := PublishOptions{HistorySize: 100, HistoryTTL: 100 * time.Second}
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
	}
}

func BenchmarkRedisPublish_History_1Ch_Parallel(b *testing.B) {
	for _, tt := range benchRedisTests {
		b.Run(tt.Name, func(b *testing.B) {
			node := testNode(b)
			e := newTestRedisBroker(b, node, tt.UseStreams, false)
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

func BenchmarkRedisPublish_History_ManyCh(b *testing.B) {
	for _, tt := range benchRedisTests {
		b.Run(tt.Name, func(b *testing.B) {
			node := testNode(b)
			e := newTestRedisBroker(b, node, tt.UseStreams, false)
			defer func() { _ = node.Shutdown(context.Background()) }()
			rawData := []byte(`{"bench": true}`)
			j := 0
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				j++
				channel := "channel" + strconv.Itoa(j%benchmarkNumDifferentChannels)
				chOpts := PublishOptions{HistorySize: 100, HistoryTTL: 100 * time.Second}
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
	}
}

func BenchmarkRedisPub_History_ManyCh_Parallel(b *testing.B) {
	for _, tt := range benchRedisTests {
		b.Run(tt.Name, func(b *testing.B) {
			node := testNode(b)
			e := newTestRedisBroker(b, node, tt.UseStreams, false)
			defer func() { _ = node.Shutdown(context.Background()) }()
			rawData := []byte(`{"bench": true}`)
			chOpts := PublishOptions{HistorySize: 100, HistoryTTL: 100 * time.Second}
			b.SetParallelism(128)
			j := 0
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					j++
					channel := "channel" + strconv.Itoa(j%benchmarkNumDifferentChannels)
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
	node := testNode(b)
	e := newTestRedisBroker(b, node, false, false)
	defer func() { _ = node.Shutdown(context.Background()) }()
	j := 0
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		j++
		err := e.Subscribe("subscribe" + strconv.Itoa(j))
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRedisSubscribe_Parallel(b *testing.B) {
	node := testNode(b)
	e := newTestRedisBroker(b, node, false, false)
	defer func() { _ = node.Shutdown(context.Background()) }()
	i := 0
	b.SetParallelism(128)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i++
			err := e.Subscribe("subscribe" + strconv.Itoa(i))
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkRedisHistory_1Ch(b *testing.B) {
	for _, tt := range benchRedisTests {
		b.Run(tt.Name, func(b *testing.B) {
			node := testNode(b)
			e := newTestRedisBroker(b, node, tt.UseStreams, false)
			defer func() { _ = node.Shutdown(context.Background()) }()
			rawData := []byte("{}")
			for i := 0; i < 4; i++ {
				_, _ = e.Publish("channel", rawData, PublishOptions{HistorySize: 4, HistoryTTL: 300 * time.Second})
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _, err := e.History("channel", HistoryFilter{
					Limit: -1,
				})
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkRedisHistory_1Ch_Parallel(b *testing.B) {
	for _, tt := range benchRedisTests {
		b.Run(tt.Name, func(b *testing.B) {
			node := testNode(b)
			e := newTestRedisBroker(b, node, tt.UseStreams, false)
			defer func() { _ = node.Shutdown(context.Background()) }()
			rawData := []byte("{}")
			for i := 0; i < 4; i++ {
				_, err := e.Publish("channel", rawData, PublishOptions{HistorySize: 4, HistoryTTL: 300 * time.Second})
				require.NoError(b, err)
			}
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					_, _, err := e.History("channel", HistoryFilter{
						Limit: -1,
					})
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}

func BenchmarkRedisRecover_1Ch_Parallel(b *testing.B) {
	for _, tt := range benchRedisTests {
		b.Run(tt.Name, func(b *testing.B) {
			node := testNode(b)
			e := newTestRedisBroker(b, node, tt.UseStreams, false)
			defer func() { _ = node.Shutdown(context.Background()) }()
			rawData := []byte("{}")
			numMessages := 1000
			numMissing := 5
			for i := 1; i <= numMessages; i++ {
				_, err := e.Publish("channel", rawData, PublishOptions{HistorySize: numMessages, HistoryTTL: 300 * time.Second})
				require.NoError(b, err)
			}
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
	c := DefaultConfig
	n, err := New(c)
	if err != nil {
		tb.Fatal(err)
	}
	e := newTestRedisBroker(tb, n, useStreams, useCluster)
	n.SetBroker(e)
	err = n.Run()
	if err != nil {
		tb.Fatal(err)
	}
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
		_, err := node.Publish(channel, []byte(`{"n": `+strconv.Itoa(i)+`}`), WithHistory(tt.HistorySize, time.Duration(tt.HistoryLifetime)*time.Second))
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
			it := historyIterationTest{10000, 100}
			startPosition := it.prepareHistoryIteration(t, e.node)
			it.testHistoryIteration(t, e.node, startPosition)
		})
	}
}

func BenchmarkRedisHistoryIteration(b *testing.B) {
	for _, tt := range benchRedisTests {
		b.Run(tt.Name, func(b *testing.B) {
			node := testNode(b)
			e := newTestRedisBroker(b, node, tt.UseStreams, false)
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
