//go:build integration

package centrifuge

import (
	"context"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

// benchPresenceTickRedis measures how long ONE connection's presence tick takes
// against a real Redis, as a function of how many presence-enabled channels the
// connection has. This is the shape issue #557 is about: the tick issues one
// PresenceManager round trip per channel, so its duration grows linearly with
// channel count multiplied by Redis RTT.
//
// Run with a shaped loopback to model a real network RTT, e.g.:
//
//	tc qdisc add dev lo root netem delay 500us
//
// Reported metrics are per tick (not per channel), because tick duration is
// what delays the connection and what close() waits for.
func benchPresenceTickRedis(b *testing.B, numChannels int, concurrency int) {
	node := defaultNodeNoHandlers()
	node.config.clientPresenceUpdateConcurrency = concurrency
	node.config.ClientChannelLimit = 10000
	defer func() { _ = node.Shutdown(context.Background()) }()

	pm := newTestRedisPresenceManager(b, node, false, false, false, 6379)
	defer stopRedisPresenceManager(pm)

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{Options: SubscribeOptions{EmitPresence: true}}, nil)
		})
	})

	client := newTestClientV2(b, node, "user")
	connectClientV2(b, client)
	for i := 0; i < numChannels; i++ {
		subscribeClientV2(b, client, "ch"+strconv.Itoa(i))
	}

	b.ResetTimer()
	b.ReportAllocs()
	start := time.Now()
	for i := 0; i < b.N; i++ {
		client.updatePresence()
	}
	elapsed := time.Since(start)
	b.ReportMetric(float64(elapsed.Nanoseconds())/float64(b.N)/1e6, "ms/tick")
	b.ReportMetric(float64(elapsed.Nanoseconds())/float64(b.N)/float64(numChannels)/1e3, "us/channel")
}

// Serial baseline (concurrency 1) vs bounded concurrency, at several channel
// counts. Concurrency lets rueidis auto-pipeline the EVALSHAs of one connection
// into fewer round trips.
func BenchmarkRedisPresenceTick_16ch_C1(b *testing.B)  { benchPresenceTickRedis(b, 16, 1) }
func BenchmarkRedisPresenceTick_16ch_C4(b *testing.B)  { benchPresenceTickRedis(b, 16, 4) }
func BenchmarkRedisPresenceTick_16ch_C8(b *testing.B)  { benchPresenceTickRedis(b, 16, 8) }
func BenchmarkRedisPresenceTick_16ch_C16(b *testing.B) { benchPresenceTickRedis(b, 16, 16) }

func BenchmarkRedisPresenceTick_128ch_C1(b *testing.B)  { benchPresenceTickRedis(b, 128, 1) }
func BenchmarkRedisPresenceTick_128ch_C4(b *testing.B)  { benchPresenceTickRedis(b, 128, 4) }
func BenchmarkRedisPresenceTick_128ch_C8(b *testing.B)  { benchPresenceTickRedis(b, 128, 8) }
func BenchmarkRedisPresenceTick_128ch_C16(b *testing.B) { benchPresenceTickRedis(b, 128, 16) }
func BenchmarkRedisPresenceTick_128ch_C32(b *testing.B) { benchPresenceTickRedis(b, 128, 32) }

func BenchmarkRedisPresenceTick_1024ch_C1(b *testing.B)  { benchPresenceTickRedis(b, 1024, 1) }
func BenchmarkRedisPresenceTick_1024ch_C8(b *testing.B)  { benchPresenceTickRedis(b, 1024, 8) }
func BenchmarkRedisPresenceTick_1024ch_C32(b *testing.B) { benchPresenceTickRedis(b, 1024, 32) }
func BenchmarkRedisPresenceTick_1024ch_C64(b *testing.B) { benchPresenceTickRedis(b, 1024, 64) }

// A single presence channel must not regress: concurrency must cost nothing
// when there is nothing to parallelize.
func BenchmarkRedisPresenceTick_1ch_C1(b *testing.B) { benchPresenceTickRedis(b, 1, 1) }
func BenchmarkRedisPresenceTick_1ch_C8(b *testing.B) { benchPresenceTickRedis(b, 1, 8) }

// benchPositionTickRedis measures the periodic stream position check against a
// real Redis. It has the same one-round-trip-per-channel shape as presence
// (node.checkPosition -> streamTop -> Broker.History), so it gets the same
// bounded-concurrency treatment.
func benchPositionTickRedis(b *testing.B, numChannels int, concurrency int) {
	node, err := New(Config{
		LogLevel:                        LogLevelError,
		LogHandler:                      func(entry LogEntry) {},
		ClientChannelLimit:              10000,
		ClientChannelPositionCheckDelay: 10 * time.Millisecond,
		ClientChannelPositionMaxTimeLag: time.Hour,
	})
	if err != nil {
		b.Fatal(err)
	}
	node.config.clientPositionCheckConcurrency = concurrency
	defer func() { _ = node.Shutdown(context.Background()) }()

	// NewTestRedisBroker runs the node itself.
	broker := NewTestRedisBroker(b, node, getUniquePrefix(), true, 6379)
	defer stopRedisBroker(broker)

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{Options: SubscribeOptions{EnablePositioning: true}}, nil)
		})
	})

	client := newTestClientV2(b, node, "user")
	connectClientV2(b, client)
	for i := 0; i < numChannels; i++ {
		subscribeClientV2(b, client, "pos"+strconv.Itoa(i))
	}

	// Advance the clock on every read so every tick really performs the check.
	var clock atomic.Int64
	base := time.Now().Unix()
	node.mu.Lock()
	node.nowTimeGetter = func() time.Time { return time.Unix(base+clock.Add(100), 0) }
	node.mu.Unlock()

	b.ResetTimer()
	start := time.Now()
	for i := 0; i < b.N; i++ {
		client.updatePresence()
	}
	b.ReportMetric(float64(time.Since(start).Nanoseconds())/float64(b.N)/1e6, "ms/tick")
}

func BenchmarkRedisPositionTick_128ch_C1(b *testing.B)  { benchPositionTickRedis(b, 128, 1) }
func BenchmarkRedisPositionTick_128ch_C8(b *testing.B)  { benchPositionTickRedis(b, 128, 8) }
func BenchmarkRedisPositionTick_128ch_C32(b *testing.B) { benchPositionTickRedis(b, 128, 32) }
