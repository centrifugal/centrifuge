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
	"github.com/stretchr/testify/require"
)

const (
	testRedisHost     = "127.0.0.1"
	testRedisPort     = 6379
	testRedisPassword = ""
	testRedisDB       = 9
)

func getUniquePrefix() string {
	return "centrifuge-test-" + randString(3) + "-" + strconv.FormatInt(time.Now().UnixNano(), 10)
}

func newTestRedisEngine(tb testing.TB, useStreams bool) *RedisEngine {
	return NewTestRedisEngineWithPrefix(tb, getUniquePrefix(), useStreams)
}

func NewTestRedisEngineWithPrefix(tb testing.TB, prefix string, useStreams bool) *RedisEngine {
	n, _ := New(Config{})
	redisConf := RedisShardConfig{
		// TODO: ideally we need separate tests for Redis Cluster.
		// ClusterAddrs: []string{"localhost:30001", "localhost:30002", "localhost:30003"},
		Host:        testRedisHost,
		Port:        testRedisPort,
		DB:          testRedisDB,
		Password:    testRedisPassword,
		Prefix:      prefix,
		ReadTimeout: 100 * time.Second,
	}
	e, err := NewRedisEngine(n, RedisEngineConfig{
		UseStreams:          useStreams,
		PublishOnHistoryAdd: true,
		HistoryMetaTTL:      300 * time.Second,
		Shards:              []RedisShardConfig{redisConf},
	})
	if err != nil {
		tb.Fatal(err)
	}
	n.SetEngine(e)
	err = n.Run()
	if err != nil {
		panic(err)
	}
	return e
}

var redisTests = []struct {
	Name       string
	UseStreams bool
}{
	{"lists", false},
	{"streams", true},
}

func TestRedisEngine(t *testing.T) {
	for _, tt := range redisTests {
		t.Run(tt.Name, func(t *testing.T) {
			e := newTestRedisEngine(t, tt.UseStreams)

			_, err := e.Channels()
			if e.shards[0].useCluster() {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			pub := newTestPublication()

			require.NoError(t, e.Publish("channel", pub, nil))
			require.NoError(t, e.Publish("channel", pub, nil))
			require.NoError(t, e.Subscribe("channel"))
			require.NoError(t, e.Unsubscribe("channel"))

			// test adding presence
			require.NoError(t, e.AddPresence("channel", "uid", &protocol.ClientInfo{}, 25*time.Second))

			p, err := e.Presence("channel")
			require.NoError(t, err)
			require.Equal(t, 1, len(p))

			err = e.RemovePresence("channel", "uid")
			require.NoError(t, err)

			rawData := protocol.Raw("{}")
			pub = &protocol.Publication{UID: "test UID", Data: rawData}

			// test adding history
			_, _, err = e.AddHistory("channel", pub, &ChannelOptions{HistorySize: 4, HistoryLifetime: 1})
			require.NoError(t, err)
			pubs, _, err := e.History("channel", HistoryFilter{
				Limit: -1,
			})
			require.NoError(t, err)
			require.Equal(t, 1, len(pubs))
			require.Equal(t, pubs[0].UID, "test UID")

			// test history limit
			_, _, err = e.AddHistory("channel", pub, &ChannelOptions{HistorySize: 4, HistoryLifetime: 1})
			require.NoError(t, err)
			_, _, err = e.AddHistory("channel", pub, &ChannelOptions{HistorySize: 4, HistoryLifetime: 1})
			require.NoError(t, err)
			_, _, err = e.AddHistory("channel", pub, &ChannelOptions{HistorySize: 4, HistoryLifetime: 1})
			require.NoError(t, err)
			pubs, _, err = e.History("channel", HistoryFilter{
				Limit: 2,
			})
			require.NoError(t, err)
			require.Equal(t, 2, len(pubs))

			// test history limit greater than history size
			_, _, err = e.AddHistory("channel", pub, &ChannelOptions{HistorySize: 1, HistoryLifetime: 1})
			require.NoError(t, err)
			_, _, err = e.AddHistory("channel", pub, &ChannelOptions{HistorySize: 1, HistoryLifetime: 1})
			require.NoError(t, err)
			_, _, err = e.AddHistory("channel", pub, &ChannelOptions{HistorySize: 1, HistoryLifetime: 1})
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
			err = e.PublishControl([]byte(""))
			require.NoError(t, nil, err)

			// test publishing join message.
			joinMessage := protocol.Join{}
			require.NoError(t, e.PublishJoin("channel", &joinMessage, nil))

			// test publishing leave message.
			leaveMessage := protocol.Leave{}
			require.NoError(t, e.PublishLeave("channel", &leaveMessage, nil))
		})
	}
}

func TestRedisCurrentPosition(t *testing.T) {
	for _, tt := range redisTests {
		t.Run(tt.Name, func(t *testing.T) {
			e := newTestRedisEngine(t, tt.UseStreams)

			channel := "test-current-position"

			_, streamTop, err := e.History(channel, HistoryFilter{
				Limit: 0,
			})
			require.NoError(t, err)
			require.Equal(t, uint64(0), streamTop.Offset)

			pub := &protocol.Publication{Data: protocol.Raw("{}")}
			_, _, err = e.AddHistory(channel, pub, &ChannelOptions{HistorySize: 10, HistoryLifetime: 2})
			require.NoError(t, err)

			_, streamTop, err = e.History(channel, HistoryFilter{
				Limit: 0,
			})
			require.NoError(t, err)
			require.Equal(t, uint64(1), streamTop.Offset)
		})
	}
}

func TestRedisEngineRecover(t *testing.T) {
	for _, tt := range redisTests {
		t.Run(tt.Name, func(t *testing.T) {
			e := newTestRedisEngine(t, tt.UseStreams)

			rawData := protocol.Raw("{}")

			for i := 0; i < 5; i++ {
				pub := &protocol.Publication{Data: rawData}
				_, _, err := e.AddHistory("channel", pub, &ChannelOptions{HistorySize: 10, HistoryLifetime: 2})
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
				pub := &protocol.Publication{Data: rawData}
				_, _, err := e.AddHistory("channel", pub, &ChannelOptions{HistorySize: 10, HistoryLifetime: 2})
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

func TestRedisEngineSubscribeUnsubscribe(t *testing.T) {
	// Custom prefix to not collide with other tests.
	e := NewTestRedisEngineWithPrefix(t, getUniquePrefix(), false)

	if e.shards[0].useCluster() {
		t.Skip("Channels command is not supported when Redis Cluster is used")
	}

	require.NoError(t, e.Subscribe("1-test"))
	require.NoError(t, e.Subscribe("1-test"))
	channels, err := e.Channels()
	require.NoError(t, err)
	if len(channels) != 1 {
		// Redis PUBSUB CHANNELS command looks like eventual consistent, so sometimes
		// it returns wrong results, sleeping for a while helps in such situations.
		// See https://gist.github.com/FZambia/80a5241e06b4662f7fe89cfaf24072c3
		time.Sleep(500 * time.Millisecond)
		channels, err := e.Channels()
		require.NoError(t, err)
		require.Equal(t, 1, len(channels), fmt.Sprintf("%#v", channels))
	}

	require.NoError(t, e.Unsubscribe("1-test"))
	channels, err = e.Channels()
	require.NoError(t, err)
	if len(channels) != 0 {
		time.Sleep(500 * time.Millisecond)
		channels, _ := e.Channels()
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
	channels, err = e.Channels()
	require.NoError(t, err)

	if len(channels) != 0 {
		time.Sleep(500 * time.Millisecond)
		channels, _ := e.Channels()
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
	channels, err = e.Channels()
	require.Equal(t, nil, err)
	if len(channels) != 0 {
		time.Sleep(500 * time.Millisecond)
		channels, err := e.Channels()
		require.NoError(t, err)
		require.Equal(t, 0, len(channels), fmt.Sprintf("%#v", channels))
	}

	// The same channel sequential.
	for i := 0; i < 10000; i++ {
		require.NoError(t, e.Subscribe("4-test"))
		require.NoError(t, e.Unsubscribe("4-test"))
	}
	channels, err = e.Channels()
	require.NoError(t, err)
	if len(channels) != 0 {
		time.Sleep(500 * time.Millisecond)
		channels, _ := e.Channels()
		require.Equal(t, 0, len(channels), fmt.Sprintf("%#v", channels))
	}

	// Different channels sequential.
	for j := 0; j < 10; j++ {
		for i := 0; i < 10000; i++ {
			require.NoError(t, e.Subscribe("5-test-"+strconv.Itoa(i)))
			require.NoError(t, e.Unsubscribe("5-test-"+strconv.Itoa(i)))
		}
		channels, err = e.Channels()
		require.NoError(t, err)
		if len(channels) != 0 {
			time.Sleep(500 * time.Millisecond)
			channels, err := e.Channels()
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
	channels, err = e.Channels()
	require.NoError(t, err)
	if len(channels) != 100 {
		time.Sleep(500 * time.Millisecond)
		channels, err := e.Channels()
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
	channels, err = e.Channels()
	require.NoError(t, err)
	if len(channels) != 0 {
		time.Sleep(500 * time.Millisecond)
		channels, _ := e.Channels()
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
	channels, err = e.Channels()
	require.NoError(t, err)
	if len(channels) != 0 {
		time.Sleep(500 * time.Millisecond)
		channels, err := e.Channels()
		require.NoError(t, err)
		require.Equal(t, 0, len(channels), fmt.Sprintf("%#v", channels))
	}
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randString(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
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

func TestRedisExtractPushData(t *testing.T) {
	data := []byte(`__16901__\x12\nchat:index\x1aU\"\x0e{\"input\":\"__\"}*C\n\x0242\x12$37cb00a9-bcfa-4284-a1ae-607c7da3a8f4\x1a\x15{\"name\": \"Alexander\"}\"\x00`)
	pushData, offset := extractPushData(data)
	require.Equal(t, uint64(16901), offset)
	require.Equal(t, []byte(`\x12\nchat:index\x1aU\"\x0e{\"input\":\"__\"}*C\n\x0242\x12$37cb00a9-bcfa-4284-a1ae-607c7da3a8f4\x1a\x15{\"name\": \"Alexander\"}\"\x00`), pushData)

	data = []byte(`\x12\nchat:index\x1aU\"\x0e{\"input\":\"__\"}*C\n\x0242\x12$37cb00a9-bcfa-4284-a1ae-607c7da3a8f4\x1a\x15{\"name\": \"Alexander\"}\"\x00`)
	pushData, offset = extractPushData(data)
	require.Equal(t, uint64(0), offset)
	require.Equal(t, []byte(`\x12\nchat:index\x1aU\"\x0e{\"input\":\"__\"}*C\n\x0242\x12$37cb00a9-bcfa-4284-a1ae-607c7da3a8f4\x1a\x15{\"name\": \"Alexander\"}\"\x00`), pushData)

	data = []byte(`__4294967337__\x12\nchat:index\x1aU\"\x0e{\"input\":\"__\"}*C\n\x0242\x12$37cb00a9-bcfa-4284-a1ae-607c7da3a8f4\x1a\x15{\"name\": \"Alexander\"}\"\x00`)
	pushData, offset = extractPushData(data)
	require.Equal(t, uint64(4294967337), offset)
	require.Equal(t, []byte(`\x12\nchat:index\x1aU\"\x0e{\"input\":\"__\"}*C\n\x0242\x12$37cb00a9-bcfa-4284-a1ae-607c7da3a8f4\x1a\x15{\"name\": \"Alexander\"}\"\x00`), pushData)
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

func BenchmarkRedisPublish_OneChannel(b *testing.B) {
	e := newTestRedisEngine(b, false)
	rawData := protocol.Raw(`{"bench": true}`)
	pub := &protocol.Publication{UID: "test UID", Data: rawData}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := e.Publish("channel", pub, &ChannelOptions{HistorySize: 0, HistoryLifetime: 0})
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkRedisPublish_OneChannel_Parallel(b *testing.B) {
	e := newTestRedisEngine(b, false)
	rawData := protocol.Raw(`{"bench": true}`)
	pub := &protocol.Publication{UID: "test UID", Data: rawData}
	b.SetParallelism(128)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			err := e.Publish("channel", pub, &ChannelOptions{HistorySize: 0, HistoryLifetime: 0})
			if err != nil {
				panic(err)
			}
		}
	})
}

const benchmarkNumDifferentChannels = 1000

func BenchmarkRedisPublish_ManyChannels(b *testing.B) {
	e := newTestRedisEngine(b, false)
	rawData := protocol.Raw(`{"bench": true}`)
	pub := &protocol.Publication{UID: "test UID", Data: rawData}
	j := 0
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		channel := "channel" + strconv.Itoa(j%benchmarkNumDifferentChannels)
		j++
		err := e.Publish(channel, pub, &ChannelOptions{HistorySize: 0, HistoryLifetime: 0})
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkRedisPublish_ManyChannels_Parallel(b *testing.B) {
	e := newTestRedisEngine(b, false)
	rawData := protocol.Raw(`{"bench": true}`)
	pub := &protocol.Publication{UID: "test UID", Data: rawData}
	b.SetParallelism(128)
	j := 0
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			channel := "channel" + strconv.Itoa(j%benchmarkNumDifferentChannels)
			j++
			err := e.Publish(channel, pub, &ChannelOptions{HistorySize: 0, HistoryLifetime: 0})
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkRedisPublish_History_OneChannel(b *testing.B) {
	for _, tt := range redisTests {
		b.Run(tt.Name, func(b *testing.B) {
			e := newTestRedisEngine(b, tt.UseStreams)
			rawData := protocol.Raw(`{"bench": true}`)
			pub := &protocol.Publication{UID: "test-uid", Data: rawData}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				chOpts := &ChannelOptions{HistorySize: 100, HistoryLifetime: 100}
				var err error
				_, published, err := e.AddHistory("channel", pub, chOpts)
				if err != nil {
					b.Fatal(err)
				}
				if !published {
					b.Fail()
				}
			}
		})
	}
}

func BenchmarkRedisPublish_History_OneChannel_Parallel(b *testing.B) {
	for _, tt := range redisTests {
		b.Run(tt.Name, func(b *testing.B) {
			e := newTestRedisEngine(b, tt.UseStreams)
			rawData := protocol.Raw(`{"bench": true}`)
			chOpts := &ChannelOptions{HistorySize: 100, HistoryLifetime: 100}
			b.SetParallelism(128)
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					pub := &protocol.Publication{UID: "test-uid", Data: rawData}
					var err error
					_, published, err := e.AddHistory("channel", pub, chOpts)
					if err != nil {
						b.Fatal(err)
					}
					if !published {
						b.Fail()
					}
				}
			})
		})
	}
}

func BenchmarkRedisPublish_History_ManyChannels(b *testing.B) {
	for _, tt := range redisTests {
		b.Run(tt.Name, func(b *testing.B) {
			e := newTestRedisEngine(b, tt.UseStreams)
			rawData := protocol.Raw(`{"bench": true}`)
			pub := &protocol.Publication{UID: "test-uid", Data: rawData}
			j := 0
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				j++
				channel := "channel" + strconv.Itoa(j%benchmarkNumDifferentChannels)
				chOpts := &ChannelOptions{HistorySize: 100, HistoryLifetime: 100}
				var err error
				_, published, err := e.AddHistory(channel, pub, chOpts)
				if err != nil {
					b.Fatal(err)
				}
				if !published {
					b.Fail()
				}
			}
		})
	}
}

func BenchmarkRedisPublish_History_ManyChannels_Parallel(b *testing.B) {
	for _, tt := range redisTests {
		b.Run(tt.Name, func(b *testing.B) {
			e := newTestRedisEngine(b, tt.UseStreams)
			rawData := protocol.Raw(`{"bench": true}`)
			chOpts := &ChannelOptions{HistorySize: 100, HistoryLifetime: 100}
			b.SetParallelism(128)
			j := 0
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					j++
					channel := "channel" + strconv.Itoa(j%benchmarkNumDifferentChannels)
					pub := &protocol.Publication{UID: "test-uid", Data: rawData}
					var err error
					_, published, err := e.AddHistory(channel, pub, chOpts)
					if err != nil {
						b.Fatal(err)
					}
					if !published {
						b.Fail()
					}
				}
			})
		})
	}
}

func BenchmarkRedisSubscribe(b *testing.B) {
	e := newTestRedisEngine(b, false)
	j := 0
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		j++
		err := e.Subscribe("subscribe" + strconv.Itoa(j))
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkRedisSubscribe_Parallel(b *testing.B) {
	e := newTestRedisEngine(b, false)
	i := 0
	b.SetParallelism(128)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i++
			err := e.Subscribe("subscribe" + strconv.Itoa(i))
			if err != nil {
				panic(err)
			}
		}
	})
}

func BenchmarkRedisAddPresence_OneChannel(b *testing.B) {
	e := newTestRedisEngine(b, false)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := e.AddPresence("channel", "uid", &protocol.ClientInfo{}, 300*time.Second)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRedisAddPresence_OneChannel_Parallel(b *testing.B) {
	e := newTestRedisEngine(b, false)
	b.SetParallelism(128)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			err := e.AddPresence("channel", "uid", &protocol.ClientInfo{}, 300*time.Second)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkRedisPresence_OneChannel(b *testing.B) {
	e := newTestRedisEngine(b, false)
	_ = e.AddPresence("channel", "uid", &protocol.ClientInfo{}, 300*time.Second)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := e.Presence("channel")
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRedisPresence_OneChannel_Parallel(b *testing.B) {
	e := newTestRedisEngine(b, false)
	b.SetParallelism(128)
	_ = e.AddPresence("channel", "uid", &protocol.ClientInfo{}, 300*time.Second)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := e.Presence("channel")
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkRedisPresence_ManyChannels(b *testing.B) {
	e := newTestRedisEngine(b, false)
	_ = e.AddPresence("channel", "uid", &protocol.ClientInfo{}, 300*time.Second)
	j := 0
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		j++
		channel := "channel" + strconv.Itoa(j%benchmarkNumDifferentChannels)
		_, err := e.Presence(channel)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRedisPresence_ManyChannels_Parallel(b *testing.B) {
	e := newTestRedisEngine(b, false)
	b.SetParallelism(128)
	_ = e.AddPresence("channel", "uid", &protocol.ClientInfo{}, 300*time.Second)
	j := 0
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			j++
			channel := "channel" + strconv.Itoa(j%benchmarkNumDifferentChannels)
			_, err := e.Presence(channel)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkRedisHistory_OneChannel(b *testing.B) {
	for _, tt := range redisTests {
		b.Run(tt.Name, func(b *testing.B) {
			e := newTestRedisEngine(b, tt.UseStreams)
			rawData := protocol.Raw("{}")
			pub := &protocol.Publication{Data: rawData}
			for i := 0; i < 4; i++ {
				_, _, _ = e.AddHistory("channel", pub, &ChannelOptions{HistorySize: 4, HistoryLifetime: 300})
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

func BenchmarkRedisHistory_OneChannel_Parallel(b *testing.B) {
	for _, tt := range redisTests {
		b.Run(tt.Name, func(b *testing.B) {
			e := newTestRedisEngine(b, tt.UseStreams)
			rawData := protocol.Raw("{}")
			pub := &protocol.Publication{UID: "test-uid", Data: rawData}
			for i := 0; i < 4; i++ {
				_, _, err := e.AddHistory("channel", pub, &ChannelOptions{HistorySize: 4, HistoryLifetime: 300})
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

func BenchmarkRedisRecover_OneChannel_Parallel(b *testing.B) {
	for _, tt := range redisTests {
		b.Run(tt.Name, func(b *testing.B) {
			e := newTestRedisEngine(b, tt.UseStreams)
			rawData := protocol.Raw("{}")
			numMessages := 1000
			numMissing := 5
			for i := 1; i <= numMessages; i++ {
				pub := &protocol.Publication{Data: rawData}
				_, _, err := e.AddHistory("channel", pub, &ChannelOptions{HistorySize: numMessages, HistoryLifetime: 300, HistoryRecover: true})
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

func nodeWithRedisEngine(tb testing.TB, useStreams bool) *Node {
	c := DefaultConfig
	n, err := New(c)
	if err != nil {
		panic(err)
	}
	e := newTestRedisEngine(tb, useStreams)
	n.SetEngine(e)
	err = n.Run()
	if err != nil {
		panic(err)
	}
	n.On().ClientConnected(func(ctx context.Context, client *Client) {
		client.On().Subscribe(func(_ SubscribeEvent) SubscribeReply {
			return SubscribeReply{}
		})
		client.On().Publish(func(_ PublishEvent) PublishReply {
			return PublishReply{}
		})
	})
	return n
}

func testRedisClientSubscribeRecover(t *testing.T, tt recoverTest, useStreams bool) {
	node := nodeWithRedisEngine(t, useStreams)

	config := node.Config()
	config.ChannelOptionsFunc = func(channel string) (ChannelOptions, error) {
		return ChannelOptions{
			HistorySize:     tt.HistorySize,
			HistoryLifetime: tt.HistoryLifetime,
			HistoryRecover:  true,
		}, nil
	}
	_ = node.Reload(config)

	transport := newTestTransport()
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
	client, _ := newClient(newCtx, node, transport)

	channel := "test_recovery_redis_" + tt.Name

	for i := 1; i <= tt.NumPublications; i++ {
		_, err := node.Publish(channel, []byte(`{"n": `+strconv.Itoa(i)+`}`))
		require.NoError(t, err)
	}

	time.Sleep(time.Duration(tt.Sleep) * time.Second)

	connectClient(t, client)

	var replies []*protocol.Reply
	rw := testReplyWriter(&replies)

	_, streamTop, err := node.historyManager.History(channel, HistoryFilter{
		Limit: 0,
		Since: nil,
	})
	require.NoError(t, err)

	subCtx := client.subscribeCmd(&protocol.SubscribeRequest{
		Channel: channel,
		Recover: true,
		Offset:  tt.SinceOffset,
		Epoch:   streamTop.Epoch,
	}, rw, false)
	require.Nil(t, subCtx.disconnect)
	require.NotEmpty(t, replies)
	require.Nil(t, replies[0].Error)
	res := extractSubscribeResult(replies, client.Transport().Protocol())
	require.Equal(t, tt.NumRecovered, len(res.Publications))
	require.Equal(t, tt.Recovered, res.Recovered)

	node.Shutdown(context.Background())
}

func TestRedisClientSubscribeRecoverStreams(t *testing.T) {
	for _, tt := range recoverTests {
		t.Run(tt.Name, func(t *testing.T) {
			testRedisClientSubscribeRecover(t, tt, true)
		})
	}
}

func TestRedisClientSubscribeRecoverLists(t *testing.T) {
	for _, tt := range recoverTests {
		t.Run(tt.Name, func(t *testing.T) {
			testRedisClientSubscribeRecover(t, tt, false)
		})
	}
}

func TestRedisEngineHistoryIteration(t *testing.T) {
	for _, tt := range redisTests {
		t.Run(tt.Name, func(t *testing.T) {
			e := newTestRedisEngine(t, tt.UseStreams)
			it := historyIterationTest{10000, 100}
			startPosition := it.prepareHistoryIteration(t, e.node)
			it.testHistoryIteration(t, e.node, startPosition)
		})
	}
}

func BenchmarkRedisEngineHistoryIteration(b *testing.B) {
	for _, tt := range redisTests {
		b.Run(tt.Name, func(b *testing.B) {
			e := newTestRedisEngine(b, tt.UseStreams)
			it := historyIterationTest{10000, 100}
			startPosition := it.prepareHistoryIteration(b, e.node)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				it.testHistoryIteration(b, e.node, startPosition)
			}
		})
	}
}
