package centrifuge

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The periodic tick snapshots only channels that need work (see
// channelNeedsPresenceTick). These tests pin the non-presence reasons a channel
// must still be included, so the snapshot filter can not silently drop them.

// TestPresenceTick_ExpiresSubWithoutPresence covers a subscription that has an
// expiration but no presence at all. Existing coverage (TestClientSubExpired)
// enables presence on the same channel, so it would pass even if the tick only
// considered presence-enabled channels.
func TestPresenceTick_ExpiresSubWithoutPresence(t *testing.T) {
	t.Parallel()
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	node.config.ClientExpiredSubCloseDelay = 0
	node.config.ClientPresenceUpdateInterval = 10 * time.Millisecond

	doneCh := make(chan struct{})
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(event SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					ExpireAt: time.Now().Unix() + 1,
					// Deliberately no EmitPresence: expiration alone must keep
					// this channel in the tick.
				},
			}, nil)
		})
		client.OnUnsubscribe(func(event UnsubscribeEvent) {
			if event.Code == UnsubscribeCodeExpired {
				close(doneCh)
			}
		})
	})

	client := newTestClient(t, node, "42")
	connectClientV2(t, client)
	subscribeClientV2(t, client, "test")

	select {
	case <-doneCh:
	case <-time.After(5 * time.Second):
		t.Fatal("subscription without presence never expired — tick skipped the channel")
	}
}

// TestPresenceTick_SkipsChannelWithNoWork is the complement: a plain channel
// with no presence, no expiration and no positioning must be skipped entirely,
// which is what makes the tick allocation-free for such connections.
func TestPresenceTick_SkipsChannelWithNoWork(t *testing.T) {
	t.Parallel()
	require.Zero(t, channelTickDuties(
		ChannelContext{flags: flagSubscribed}, true, true),
		"plain subscribed channel should need no tick work")

	require.NotZero(t, channelTickDuties(
		ChannelContext{flags: flagSubscribed | flagEmitPresence}, true, true),
		"channel with presence must be ticked")
	require.NotZero(t, channelTickDuties(
		ChannelContext{flags: flagSubscribed | flagMapClientPresence}, true, true),
		"channel with map client presence must be ticked")
	require.NotZero(t, channelTickDuties(
		ChannelContext{flags: flagSubscribed | flagMapUserPresence}, true, true),
		"channel with map user presence must be ticked")
	require.NotZero(t, channelTickDuties(
		ChannelContext{flags: flagSubscribed, expireAt: 1}, true, true),
		"channel with expiration must be ticked")
	require.NotZero(t, channelTickDuties(
		ChannelContext{flags: flagSubscribed | flagPositioning}, true, true),
		"channel with positioning must be ticked when position checks are enabled")
	require.NotZero(t, channelTickDuties(
		ChannelContext{flags: flagSubscribed | flagKeyed}, true, true),
		"keyed channel must be ticked when shared poll is configured")

	// Config-gated reasons must not force a tick when the feature is disabled.
	require.Zero(t, channelTickDuties(
		ChannelContext{flags: flagSubscribed | flagPositioning}, false, true),
		"positioning should not need a tick when position checks are disabled")
	require.Zero(t, channelTickDuties(
		ChannelContext{flags: flagSubscribed | flagKeyed}, true, false),
		"keyed channel should not need a tick without shared poll options")
}

// TestPresenceTick_KeepsRefreshingPresence guards the re-arm move: the tick now
// re-arms at the start of updatePresence rather than at the end, so presence
// must still be refreshed repeatedly rather than once.
func TestPresenceTick_KeepsRefreshingPresence(t *testing.T) {
	t.Parallel()
	pm := &slowPresenceManager{rtt: 0}
	node, err := New(Config{
		LogLevel:                     LogLevelError,
		LogHandler:                   func(entry LogEntry) {},
		ClientPresenceUpdateInterval: 20 * time.Millisecond,
	})
	require.NoError(t, err)
	node.SetPresenceManager(pm)
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{Options: SubscribeOptions{EmitPresence: true}}, nil)
		})
	})
	require.NoError(t, node.Run())
	defer func() { _ = node.Shutdown(context.Background()) }()

	client := newTestConnectedClientV2(t, node, "user")
	subscribeClientV2(t, client, "test")

	// Presence is added once on subscribe; ticks must keep adding after that.
	require.Eventually(t, func() bool {
		return pm.calls.Load() >= 4
	}, 3*time.Second, 10*time.Millisecond,
		"presence stopped being refreshed — tick did not re-arm (got %d calls)", pm.calls.Load())
}

// TestPresenceTickDefaultIsAllocationFree pins the promise that the default
// (sequential) tick costs nothing beyond the work it dispatches: the concurrency
// knobs must not add allocations when they are off, which is the default.
//
// A channel with no tick duties is used, so the only thing measured is the tick
// machinery itself — snapshot, duty dispatch and compensation.
func TestPresenceTickDefaultIsAllocationFree(t *testing.T) {
	node, err := New(Config{
		LogLevel:           LogLevelError,
		LogHandler:         func(entry LogEntry) {},
		ClientChannelLimit: 1000,
		// clientPresenceUpdateConcurrency / clientPositionCheckConcurrency left at
		// their zero value on purpose: this is the default configuration.
	})
	require.NoError(t, err)
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{}, nil) // no presence, no expiry, no positioning
		})
	})
	require.NoError(t, node.Run())
	defer func() { _ = node.Shutdown(context.Background()) }()

	client := newTestConnectedClientV2(t, node, "user")
	for i := 0; i < 32; i++ {
		subscribeClientV2(t, client, "ch"+strconv.Itoa(i))
	}
	for i := 0; i < 50; i++ { // warm the snapshot pool
		client.updatePresence()
	}

	allocs := testing.AllocsPerRun(200, func() {
		client.updatePresence()
	})
	require.Zero(t, allocs, "default presence tick must not allocate, got %v allocs/op", allocs)
}
