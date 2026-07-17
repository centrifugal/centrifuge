package centrifuge

import (
	"context"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

// benchPresenceTick measures the per-connection cost of one presence tick with
// a no-op PresenceManager, isolating the tick's own CPU/allocations from any
// network or encoding cost in the PresenceManager implementation.
func benchPresenceTick(b *testing.B, numChannels int, withPresence bool) {
	node, err := New(Config{
		LogLevel:           LogLevelError,
		LogHandler:         func(entry LogEntry) {},
		ClientChannelLimit: 1000,
	})
	if err != nil {
		b.Fatal(err)
	}
	node.SetPresenceManager(&noopPresenceManager{})
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{Options: SubscribeOptions{EmitPresence: withPresence}}, nil)
		})
	})
	if err := node.Run(); err != nil {
		b.Fatal(err)
	}
	defer func() { _ = node.Shutdown(context.Background()) }()

	client := newTestClientV2(b, node, "user")
	connectClientV2(b, client)
	for i := 0; i < numChannels; i++ {
		subscribeClientV2(b, client, "ch"+strconv.Itoa(i))
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		client.updatePresence()
	}
}

// With presence enabled on every channel.
func BenchmarkPresenceTick_1(b *testing.B)   { benchPresenceTick(b, 1, true) }
func BenchmarkPresenceTick_16(b *testing.B)  { benchPresenceTick(b, 16, true) }
func BenchmarkPresenceTick_128(b *testing.B) { benchPresenceTick(b, 128, true) }

// No presence on any channel — the tick has no presence work to do at all, but
// still runs every ClientPresenceUpdateInterval for every connection.
func BenchmarkPresenceTick_NoPresence_16(b *testing.B)  { benchPresenceTick(b, 16, false) }
func BenchmarkPresenceTick_NoPresence_128(b *testing.B) { benchPresenceTick(b, 128, false) }

// benchPositionTick measures a tick that performs the periodic stream position
// check. positionCheckTime is reset each iteration (outside the timer) so every
// tick actually performs the check — otherwise the 40s delay gate skips it and
// the benchmark measures nothing.
func benchPositionTick(b *testing.B, numChannels int, withPresence bool) {
	node, err := New(Config{
		LogLevel:                        LogLevelError,
		LogHandler:                      func(entry LogEntry) {},
		ClientChannelLimit:              1000,
		ClientChannelPositionCheckDelay: 10 * time.Millisecond,
		ClientChannelPositionMaxTimeLag: time.Hour,
	})
	if err != nil {
		b.Fatal(err)
	}
	node.SetPresenceManager(&noopPresenceManager{})
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{Options: SubscribeOptions{
				EnablePositioning: true,
				EmitPresence:      withPresence,
			}}, nil)
		})
	})
	if err := node.Run(); err != nil {
		b.Fatal(err)
	}
	defer func() { _ = node.Shutdown(context.Background()) }()

	client := newTestClientV2(b, node, "user")
	connectClientV2(b, client)
	for i := 0; i < numChannels; i++ {
		subscribeClientV2(b, client, "ch"+strconv.Itoa(i))
	}

	// Advance the clock on every read so positionCheckTime is always stale and
	// every tick really performs the check. Using b.StopTimer/StartTimer to reset
	// it instead would call ReadMemStats (stop-the-world) each iteration and
	// inflate ns/op by an order of magnitude.
	var clock atomic.Int64
	base := time.Now().Unix()
	node.mu.Lock()
	node.nowTimeGetter = func() time.Time {
		return time.Unix(base+clock.Add(100), 0)
	}
	node.mu.Unlock()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		client.updatePresence()
	}
}

func BenchmarkPositionTick_1(b *testing.B)   { benchPositionTick(b, 1, false) }
func BenchmarkPositionTick_16(b *testing.B)  { benchPositionTick(b, 16, false) }
func BenchmarkPositionTick_128(b *testing.B) { benchPositionTick(b, 128, false) }

// Presence + positioning together — the heaviest realistic tick.
func BenchmarkPositionTick_WithPresence_128(b *testing.B) { benchPositionTick(b, 128, true) }
