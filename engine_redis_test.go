// +build integration

package centrifuge

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/centrifugal/protocol"
	"github.com/gomodule/redigo/redis"
	"github.com/stretchr/testify/assert"
)

type testRedisConn struct {
	redis.Conn
}

const (
	testRedisHost     = "127.0.0.1"
	testRedisPort     = 6379
	testRedisPassword = ""
	testRedisDB       = 9
	testRedisURL      = "redis://:@127.0.0.1:6379/9"
)

func (t testRedisConn) close() error {
	_, err := t.Conn.Do("SELECT", testRedisDB)
	if err != nil {
		return nil
	}
	_, err = t.Conn.Do("FLUSHDB")
	if err != nil {
		return err
	}
	return t.Conn.Close()
}

// Get connection to Redis, select database and if that database not empty
// then panic to prevent existing data corruption.
func dial(t *testing.T) testRedisConn {
	addr := net.JoinHostPort(testRedisHost, strconv.Itoa(testRedisPort))
	c, err := redis.DialTimeout("tcp", addr, 0, 1*time.Second, 1*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	_, err = c.Do("SELECT", testRedisDB)
	if err != nil {
		c.Close()
		t.Fatal(err)
	}

	n, err := redis.Int(c.Do("DBSIZE"))
	if err != nil {
		c.Close()
		t.Fatal(err)
	}

	if n != 0 {
		c.Close()
		t.Fatal(errors.New("database is not empty, test can not continue"))
	}

	return testRedisConn{c}
}

func newTestRedisEngine() *RedisEngine {
	return NewTestRedisEngineWithPrefix("centrifuge-test")
}

func NewTestRedisEngineWithPrefix(prefix string) *RedisEngine {
	n, _ := New(Config{}, nil)
	redisConf := RedisShardConfig{
		Host:        testRedisHost,
		Port:        testRedisPort,
		Password:    testRedisPassword,
		DB:          testRedisDB,
		Prefix:      prefix,
		ReadTimeout: 100 * time.Second,
	}
	e, _ := NewRedisEngine(n, RedisEngineConfig{SequenceTTL: 300 * time.Second, Shards: []RedisShardConfig{redisConf}})
	n.SetEngine(e)
	err := n.Run()
	if err != nil {
		panic(err)
	}
	return e
}

func TestRedisEngine(t *testing.T) {
	c := dial(t)
	defer c.close()

	e := newTestRedisEngine()

	assert.Equal(t, e.name(), "Redis")

	_, err := e.Channels()
	assert.NoError(t, err)

	pub := newTestPublication()

	assert.NoError(t, e.Publish("channel", pub, nil))
	assert.NoError(t, e.Publish("channel", pub, nil))
	assert.NoError(t, e.Subscribe("channel"))
	assert.NoError(t, e.Unsubscribe("channel"))

	// test adding presence
	assert.NoError(t, e.AddPresence("channel", "uid", &ClientInfo{}, 25*time.Second))

	p, err := e.Presence("channel")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(p))

	err = e.RemovePresence("channel", "uid")
	assert.NoError(t, err)

	rawData := Raw([]byte("{}"))
	pub = &Publication{UID: "test UID", Data: rawData}

	// test adding history
	_, err = e.AddHistory("channel", pub, &ChannelOptions{HistorySize: 4, HistoryLifetime: 1})
	assert.NoError(t, err)
	h, _, err := e.History("channel", HistoryFilter{
		Limit: -1,
	})
	assert.NoError(t, err)
	assert.Equal(t, 1, len(h))
	assert.Equal(t, h[0].UID, "test UID")

	// test history limit
	_, err = e.AddHistory("channel", pub, &ChannelOptions{HistorySize: 4, HistoryLifetime: 1})
	assert.NoError(t, err)
	_, err = e.AddHistory("channel", pub, &ChannelOptions{HistorySize: 4, HistoryLifetime: 1})
	assert.NoError(t, err)
	_, err = e.AddHistory("channel", pub, &ChannelOptions{HistorySize: 4, HistoryLifetime: 1})
	assert.NoError(t, err)
	h, _, err = e.History("channel", HistoryFilter{
		Limit: 2,
	})
	assert.NoError(t, err)
	assert.Equal(t, 2, len(h))

	// test history limit greater than history size
	_, err = e.AddHistory("channel", pub, &ChannelOptions{HistorySize: 1, HistoryLifetime: 1})
	assert.NoError(t, err)
	_, err = e.AddHistory("channel", pub, &ChannelOptions{HistorySize: 1, HistoryLifetime: 1})
	assert.NoError(t, err)
	_, err = e.AddHistory("channel", pub, &ChannelOptions{HistorySize: 1, HistoryLifetime: 1})
	assert.NoError(t, err)

	// ask all history.
	h, _, err = e.History("channel", HistoryFilter{
		Limit: -1,
	})
	assert.NoError(t, err)
	assert.Equal(t, 1, len(h))

	// ask more history than history_size.
	h, _, err = e.History("channel", HistoryFilter{
		Limit: 2,
	})
	assert.NoError(t, err)
	assert.Equal(t, 1, len(h))

	// test publishing control message.
	err = e.PublishControl([]byte(""))
	assert.NoError(t, nil, err)

	// test publishing join message.
	joinMessage := Join{}
	assert.NoError(t, e.PublishJoin("channel", &joinMessage, nil))

	// test publishing leave message.
	leaveMessage := Leave{}
	assert.NoError(t, e.PublishLeave("channel", &leaveMessage, nil))
}

func TestRedisCurrentPosition(t *testing.T) {
	c := dial(t)
	defer c.close()
	e := newTestRedisEngine()

	channel := "test-current-position"

	_, recoveryPosition, err := e.History(channel, HistoryFilter{
		Limit: 0,
	})
	assert.NoError(t, err)
	assert.Equal(t, uint32(0), recoveryPosition.Seq)
	assert.Equal(t, uint32(0), recoveryPosition.Gen)

	pub := &Publication{Data: Raw([]byte("{}"))}
	_, err = e.AddHistory(channel, pub, &ChannelOptions{HistorySize: 10, HistoryLifetime: 2})
	assert.NoError(t, err)

	_, recoveryPosition, err = e.History(channel, HistoryFilter{
		Limit: 0,
	})
	assert.NoError(t, err)
	assert.Equal(t, uint32(1), recoveryPosition.Seq)
	assert.Equal(t, uint32(0), recoveryPosition.Gen)
}

func TestRedisEngineRecover(t *testing.T) {

	c := dial(t)
	defer c.close()

	e := newTestRedisEngine()

	rawData := Raw([]byte("{}"))
	pub := &Publication{Data: rawData}

	pub.UID = "1"
	_, err := e.AddHistory("channel", pub, &ChannelOptions{HistorySize: 10, HistoryLifetime: 2})
	assert.NoError(t, err)
	pub.UID = "2"
	_, err = e.AddHistory("channel", pub, &ChannelOptions{HistorySize: 10, HistoryLifetime: 2})
	assert.NoError(t, err)
	pub.UID = "3"
	_, err = e.AddHistory("channel", pub, &ChannelOptions{HistorySize: 10, HistoryLifetime: 2})
	assert.NoError(t, err)
	pub.UID = "4"
	_, err = e.AddHistory("channel", pub, &ChannelOptions{HistorySize: 10, HistoryLifetime: 2})
	assert.NoError(t, err)
	pub.UID = "5"
	_, err = e.AddHistory("channel", pub, &ChannelOptions{HistorySize: 10, HistoryLifetime: 2})
	assert.NoError(t, err)

	_, r, err := e.History("channel", HistoryFilter{
		Limit: 0,
		Since: nil,
	})
	assert.NoError(t, err)

	pubs, _, err := e.History("channel", HistoryFilter{
		Limit: -1,
		Since: &RecoveryPosition{Seq: 2, Gen: 0, Epoch: r.Epoch},
	})
	assert.NoError(t, err)
	assert.Equal(t, 3, len(pubs))
	assert.Equal(t, uint32(3), pubs[0].Seq)
	assert.Equal(t, uint32(4), pubs[1].Seq)
	assert.Equal(t, uint32(5), pubs[2].Seq)

	pubs, _, err = e.History("channel", HistoryFilter{
		Limit: -1,
		Since: &RecoveryPosition{Seq: 6, Gen: 0, Epoch: r.Epoch},
	})
	assert.NoError(t, err)
	assert.Equal(t, 5, len(pubs))

	assert.NoError(t, e.RemoveHistory("channel"))
	pubs, _, err = e.History("channel", HistoryFilter{
		Limit: -1,
		Since: &RecoveryPosition{Seq: 2, Gen: 0, Epoch: r.Epoch},
	})
	assert.NoError(t, err)
	assert.Equal(t, 0, len(pubs))
}

func TestRedisEngineSubscribeUnsubscribe(t *testing.T) {
	c := dial(t)
	defer c.close()

	// Custom prefix to not collide with other tests.
	e := NewTestRedisEngineWithPrefix("TestRedisEngineSubscribeUnsubscribe")

	e.Subscribe("1-test")
	e.Subscribe("1-test")
	channels, err := e.Channels()
	assert.Equal(t, nil, err)
	if len(channels) != 1 {
		// Redis PUBSUB CHANNELS command looks like eventual consistent, so sometimes
		// it returns wrong results, sleeping for a while helps in such situations.
		// See https://gist.github.com/FZambia/80a5241e06b4662f7fe89cfaf24072c3
		time.Sleep(500 * time.Millisecond)
		channels, _ := e.Channels()
		assert.Equal(t, 1, len(channels), fmt.Sprintf("%#v", channels))
	}

	e.Unsubscribe("1-test")
	channels, err = e.Channels()
	assert.Equal(t, nil, err)
	if len(channels) != 0 {
		time.Sleep(500 * time.Millisecond)
		channels, _ := e.Channels()
		assert.Equal(t, 0, len(channels), fmt.Sprintf("%#v", channels))
	}

	var wg sync.WaitGroup

	// The same channel in parallel.
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			e.Subscribe("2-test")
			e.Unsubscribe("2-test")
		}()
	}
	wg.Wait()
	channels, err = e.Channels()
	assert.Equal(t, nil, err)

	if len(channels) != 0 {
		time.Sleep(500 * time.Millisecond)
		channels, _ := e.Channels()
		assert.Equal(t, 0, len(channels), fmt.Sprintf("%#v", channels))
	}

	// Different channels in parallel.
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			e.Subscribe("3-test-" + strconv.Itoa(i))
			e.Unsubscribe("3-test-" + strconv.Itoa(i))
		}(i)
	}
	wg.Wait()
	channels, err = e.Channels()
	assert.Equal(t, nil, err)
	if len(channels) != 0 {
		time.Sleep(500 * time.Millisecond)
		channels, _ := e.Channels()
		assert.Equal(t, 0, len(channels), fmt.Sprintf("%#v", channels))
	}

	// The same channel sequential.
	for i := 0; i < 10000; i++ {
		e.Subscribe("4-test")
		e.Unsubscribe("4-test")
	}
	channels, err = e.Channels()
	assert.Equal(t, nil, err)
	if len(channels) != 0 {
		time.Sleep(500 * time.Millisecond)
		channels, _ := e.Channels()
		assert.Equal(t, 0, len(channels), fmt.Sprintf("%#v", channels))
	}

	// Different channels sequential.
	for j := 0; j < 10; j++ {
		for i := 0; i < 10000; i++ {
			e.Subscribe("5-test-" + strconv.Itoa(i))
			e.Unsubscribe("5-test-" + strconv.Itoa(i))
		}
		channels, err = e.Channels()
		assert.Equal(t, nil, err)
		if len(channels) != 0 {
			time.Sleep(500 * time.Millisecond)
			channels, _ := e.Channels()
			assert.Equal(t, 0, len(channels), fmt.Sprintf("%#v", channels))
		}
	}

	// Different channels subscribe only in parallel.
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			e.Subscribe("6-test-" + strconv.Itoa(i))
		}(i)
	}
	wg.Wait()
	channels, err = e.Channels()
	assert.Equal(t, nil, err)
	if len(channels) != 100 {
		time.Sleep(500 * time.Millisecond)
		channels, _ := e.Channels()
		assert.Equal(t, 100, len(channels), fmt.Sprintf("%#v", channels))
	}

	// Different channels unsubscribe only in parallel.
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			e.Unsubscribe("6-test-" + strconv.Itoa(i))
		}(i)
	}
	wg.Wait()
	channels, err = e.Channels()
	assert.Equal(t, nil, err)
	if len(channels) != 0 {
		time.Sleep(500 * time.Millisecond)
		channels, _ := e.Channels()
		assert.Equal(t, 0, len(channels), fmt.Sprintf("%#v", channels))
	}

	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			e.Unsubscribe("7-test-" + strconv.Itoa(i))
			e.Unsubscribe("8-test-" + strconv.Itoa(i))
			e.Subscribe("8-test-" + strconv.Itoa(i))
			e.Unsubscribe("9-test-" + strconv.Itoa(i))
			e.Subscribe("7-test-" + strconv.Itoa(i))
			e.Unsubscribe("8-test-" + strconv.Itoa(i))
			e.Subscribe("9-test-" + strconv.Itoa(i))
			e.Unsubscribe("9-test-" + strconv.Itoa(i))
			e.Unsubscribe("7-test-" + strconv.Itoa(i))
		}(i)
	}
	wg.Wait()
	channels, err = e.Channels()
	assert.Equal(t, nil, err)
	if len(channels) != 0 {
		time.Sleep(500 * time.Millisecond)
		channels, _ := e.Channels()
		assert.Equal(t, 0, len(channels), fmt.Sprintf("%#v", channels))
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

// TestConsistentIndex exists to test consistent hashing algorithm we use.
// As we use random in this test we carefully do asserts here.
// At least it can protect us from stupid mistakes to certain degree.
// We just expect +-equal distribution and keeping most of chans on
// the same shard after resharding.
func TestConsistentIndex(t *testing.T) {

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
	assert.True(t, sameFraction > 0.7)
}

func TestExtractPushData(t *testing.T) {
	data := []byte(`__16901__\x12\nchat:index\x1aU\"\x0e{\"input\":\"__\"}*C\n\x0242\x12$37cb00a9-bcfa-4284-a1ae-607c7da3a8f4\x1a\x15{\"name\": \"Alexander\"}\"\x00`)
	pushData, seq, gen := extractPushData(data)
	assert.Equal(t, uint32(16901), seq)
	assert.Equal(t, uint32(0), gen)
	assert.Equal(t, []byte(`\x12\nchat:index\x1aU\"\x0e{\"input\":\"__\"}*C\n\x0242\x12$37cb00a9-bcfa-4284-a1ae-607c7da3a8f4\x1a\x15{\"name\": \"Alexander\"}\"\x00`), pushData)

	data = []byte(`\x12\nchat:index\x1aU\"\x0e{\"input\":\"__\"}*C\n\x0242\x12$37cb00a9-bcfa-4284-a1ae-607c7da3a8f4\x1a\x15{\"name\": \"Alexander\"}\"\x00`)
	pushData, seq, gen = extractPushData(data)
	assert.Equal(t, uint32(0), seq)
	assert.Equal(t, uint32(0), gen)
	assert.Equal(t, []byte(`\x12\nchat:index\x1aU\"\x0e{\"input\":\"__\"}*C\n\x0242\x12$37cb00a9-bcfa-4284-a1ae-607c7da3a8f4\x1a\x15{\"name\": \"Alexander\"}\"\x00`), pushData)

	data = []byte(`__4294967337__\x12\nchat:index\x1aU\"\x0e{\"input\":\"__\"}*C\n\x0242\x12$37cb00a9-bcfa-4284-a1ae-607c7da3a8f4\x1a\x15{\"name\": \"Alexander\"}\"\x00`)
	pushData, seq, gen = extractPushData(data)
	assert.Equal(t, uint32(41), seq)
	assert.Equal(t, uint32(1), gen)
	assert.Equal(t, []byte(`\x12\nchat:index\x1aU\"\x0e{\"input\":\"__\"}*C\n\x0242\x12$37cb00a9-bcfa-4284-a1ae-607c7da3a8f4\x1a\x15{\"name\": \"Alexander\"}\"\x00`), pushData)
}

func BenchmarkRedisEngineConsistentIndex(b *testing.B) {
	for i := 0; i < b.N; i++ {
		consistentIndex(strconv.Itoa(i), 4)
	}
}

func BenchmarkRedisEngineIndex(b *testing.B) {
	for i := 0; i < b.N; i++ {
		index(strconv.Itoa(i), 4)
	}
}

func BenchmarkRedisEnginePublish(b *testing.B) {
	e := newTestRedisEngine()
	rawData := Raw([]byte(`{"bench": true}`))
	pub := &Publication{UID: "test UID", Data: rawData}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := e.Publish("channel", pub, &ChannelOptions{HistorySize: 0, HistoryLifetime: 0})
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkRedisEnginePublishParallel(b *testing.B) {
	e := newTestRedisEngine()
	rawData := Raw([]byte(`{"bench": true}`))
	pub := &Publication{UID: "test UID", Data: rawData}
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

func BenchmarkRedisEngineSubscribe(b *testing.B) {
	e := newTestRedisEngine()
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

func BenchmarkRedisEngineSubscribeParallel(b *testing.B) {
	e := newTestRedisEngine()
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

func BenchmarkRedisEnginePublishWithHistory(b *testing.B) {
	e := newTestRedisEngine()
	rawData := Raw([]byte(`{"bench": true}`))
	pub := &Publication{UID: "test-uid", Data: rawData}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		chOpts := &ChannelOptions{HistorySize: 100, HistoryLifetime: 100}
		var err error
		pub, err = e.AddHistory("channel", pub, chOpts)
		if err != nil {
			panic(err)
		}
		err = e.Publish("channel", pub, chOpts)
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkRedisEnginePublishWithHistoryParallel(b *testing.B) {
	e := newTestRedisEngine()
	rawData := Raw([]byte(`{"bench": true}`))
	chOpts := &ChannelOptions{HistorySize: 100, HistoryLifetime: 100}
	b.SetParallelism(128)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			pub := &Publication{UID: "test-uid", Data: rawData}
			var err error
			pub, err = e.AddHistory("channel", pub, chOpts)
			if err != nil {
				b.Fatal(err)
			}
			err = e.Publish("channel", pub, chOpts)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkRedisEngineAddPresence(b *testing.B) {
	e := newTestRedisEngine()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := e.AddPresence("channel", "uid", &ClientInfo{}, 300*time.Second)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRedisEngineAddPresenceParallel(b *testing.B) {
	e := newTestRedisEngine()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			err := e.AddPresence("channel", "uid", &ClientInfo{}, 300*time.Second)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkRedisEnginePresence(b *testing.B) {
	e := newTestRedisEngine()
	e.AddPresence("channel", "uid", &ClientInfo{}, 300*time.Second)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := e.Presence("channel")
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRedisEnginePresenceParallel(b *testing.B) {
	e := newTestRedisEngine()
	e.AddPresence("channel", "uid", &ClientInfo{}, 300*time.Second)
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

func BenchmarkRedisEngineHistory(b *testing.B) {
	e := newTestRedisEngine()
	rawData := Raw([]byte("{}"))
	pub := &Publication{UID: "test UID", Data: rawData}
	for i := 0; i < 4; i++ {
		e.AddHistory("channel", pub, &ChannelOptions{HistorySize: 4, HistoryLifetime: 300})
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
}

func BenchmarkRedisEngineHistoryParallel(b *testing.B) {
	e := newTestRedisEngine()
	rawData := Raw([]byte("{}"))
	pub := &Publication{UID: "test-uid", Data: rawData}
	for i := 0; i < 4; i++ {
		e.AddHistory("channel", pub, &ChannelOptions{HistorySize: 4, HistoryLifetime: 300})
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
}

func BenchmarkRedisEngineHistoryRecoverParallel(b *testing.B) {
	e := newTestRedisEngine()
	rawData := Raw([]byte("{}"))
	numMessages := 100
	for i := 0; i < numMessages; i++ {
		pub := &Publication{Data: rawData}
		e.AddHistory("channel", pub, &ChannelOptions{HistorySize: numMessages, HistoryLifetime: 300})
	}
	_, r, err := e.History("channel", HistoryFilter{
		Limit: 0,
	})
	assert.NoError(b, err)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _, err := e.History("channel", HistoryFilter{
				Limit: -1,
				Since: &RecoveryPosition{Seq: uint32(numMessages - 5), Gen: 0, Epoch: r.Epoch},
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func nodeWithRedisEngine() *Node {
	c := DefaultConfig
	n, err := New(c, nil)
	if err != nil {
		panic(err)
	}
	e := newTestRedisEngine()
	n.SetEngine(e)
	err = n.Run()
	if err != nil {
		panic(err)
	}
	return n
}

func TestClientSubscribeRecoverRedis(t *testing.T) {
	c := dial(t)
	defer c.close()

	for _, tt := range recoverTests {
		t.Run(tt.Name, func(t *testing.T) {
			node := nodeWithRedisEngine()

			config := node.Config()
			config.HistorySize = tt.HistorySize
			config.HistoryLifetime = tt.HistoryLifetime
			config.HistoryRecover = true
			node.Reload(config)

			transport := newTestTransport()
			ctx := context.Background()
			newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
			client, _ := NewClient(newCtx, node, transport)

			channel := "test_recovery_redis_" + tt.Name

			for i := 1; i <= tt.NumPublications; i++ {
				node.Publish(channel, []byte(`{"n": `+strconv.Itoa(i)+`}`))
			}

			time.Sleep(time.Duration(tt.Sleep) * time.Second)

			connectClient(t, client)

			replies := []*protocol.Reply{}
			rw := testReplyWriter(&replies)

			_, recoveryPosition, _ := node.historyManager.History(channel, HistoryFilter{
				Limit: 0,
				Since: nil,
			})
			disconnect := client.subscribeCmd(&protocol.SubscribeRequest{
				Channel: channel,
				Recover: true,
				Seq:     tt.SinceSeq,
				Gen:     recoveryPosition.Gen,
				Epoch:   recoveryPosition.Epoch,
			}, rw)
			assert.Nil(t, disconnect)
			assert.NotEmpty(t, replies)
			assert.Nil(t, replies[0].Error)
			res := extractSubscribeResult(replies)
			assert.Equal(t, tt.NumRecovered, len(res.Publications))
			assert.Equal(t, tt.Recovered, res.Recovered)

			node.Shutdown(context.Background())
		})
	}
}
