package centrifuge

import (
	"context"
	"testing"
	"time"

	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/require"
)

// streamRemoveIn is in how many seconds the memory broker removes the stream
// of ch, false if no removal is scheduled.
func streamRemoveIn(h *historyHub, ch string) (int64, bool) {
	s := h.shard(ch)
	s.RLock()
	defer s.RUnlock()
	at, ok := s.removes[ch]
	return at - time.Now().Unix(), ok
}

func TestMemoryHistoryHubMetaTTLInitialUnset(t *testing.T) {
	t.Parallel()
	h := newHistoryHub(300*time.Second, make(chan struct{}))

	_, _, err := h.get("empty", HistoryOptions{})
	require.NoError(t, err)
	in, ok := streamRemoveIn(h, "empty")
	require.True(t, ok)
	require.InDelta(t, 300, in, 2, "without the option an empty stream gets the meta TTL")
}

func TestMemoryHistoryHubMetaTTLInitial(t *testing.T) {
	t.Parallel()
	h := newHistoryHub(300*time.Second, make(chan struct{}))
	h.setInitialMetaTTL(60 * time.Second)

	_, _, err := h.get("ch", HistoryOptions{})
	require.NoError(t, err)
	in, ok := streamRemoveIn(h, "ch")
	require.True(t, ok)
	require.InDelta(t, 60, in, 2, "empty stream gets the initial TTL")

	_, _, _, err = h.add("ch", newTestPublication(), PublishOptions{HistorySize: 10, HistoryTTL: time.Minute})
	require.NoError(t, err)
	in, _ = streamRemoveIn(h, "ch")
	require.InDelta(t, 300, in, 2, "publication sets the meta TTL")

	_, _, err = h.get("ch", HistoryOptions{})
	require.NoError(t, err)
	in, _ = streamRemoveIn(h, "ch")
	require.InDelta(t, 300, in, 2, "a read of a stream with publications keeps the meta TTL")

	// A per-channel meta TTL shorter than the initial one wins.
	_, _, err = h.get("short", HistoryOptions{MetaTTL: 10 * time.Second})
	require.NoError(t, err)
	in, _ = streamRemoveIn(h, "short")
	require.InDelta(t, 10, in, 2)
}

func TestMemoryHistoryHubMetaTTLInitialRemoves(t *testing.T) {
	t.Parallel()
	h := newHistoryHub(300*time.Second, make(chan struct{}))
	h.setInitialMetaTTL(time.Second)
	h.runCleanups()
	t.Cleanup(h.close)

	_, _, err := h.get("empty", HistoryOptions{})
	require.NoError(t, err)
	_, _, _, err = h.add("published", newTestPublication(), PublishOptions{HistorySize: 10, HistoryTTL: time.Minute})
	require.NoError(t, err)
	require.Equal(t, 2, h.testNumStreams())

	require.Eventually(t, func() bool {
		return h.testNumStreams() == 1
	}, 10*time.Second, 50*time.Millisecond, "empty stream removed after the initial TTL")
	_, sp, err := h.get("published", HistoryOptions{})
	require.NoError(t, err)
	require.Equal(t, uint64(1), sp.Offset, "stream with publications kept")
}

func newInitialMetaTTLMapBroker(t *testing.T, initial time.Duration, opts MapChannelOptions) *MemoryMapBroker {
	t.Helper()
	node, err := New(Config{
		LogLevel:                        LogLevelError,
		LogHandler:                      func(LogEntry) {},
		ClientChannelPositionCheckDelay: 300 * time.Millisecond,
		HistoryMetaTTLInitial:           initial,
		Map: MapConfig{
			GetMapChannelOptions: func(string) MapChannelOptions { return opts },
		},
	})
	require.NoError(t, err)
	return newTestMemoryMapBroker(t, node)
}

// mapRemoveIn is in how many milliseconds the memory map broker removes ch,
// false if no removal is scheduled.
func mapRemoveIn(e *MemoryMapBroker, ch string) (int64, bool) {
	e.mapHub.RLock()
	defer e.mapHub.RUnlock()
	at, ok := e.mapHub.removes[ch]
	return at - time.Now().UnixMilli(), ok
}

func mapChannelExists(e *MemoryMapBroker, ch string) bool {
	e.mapHub.RLock()
	defer e.mapHub.RUnlock()
	_, ok := e.mapHub.channels[ch]
	return ok
}

func TestMemoryMapBrokerMetaTTLInitialUnset(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	e := newInitialMetaTTLMapBroker(t, 0, MapChannelOptions{Mode: MapModePersistent, StreamSize: 10, StreamTTL: time.Minute})

	_, err := e.ReadState(ctx, "persistent", MapReadStateOptions{Limit: 10})
	require.NoError(t, err)
	_, ok := mapRemoveIn(e, "persistent")
	require.False(t, ok, "without the option a permanent map is never removed")
}

func TestMemoryMapBrokerMetaTTLInitialPermanent(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	e := newInitialMetaTTLMapBroker(t, time.Minute, MapChannelOptions{Mode: MapModePersistent, StreamSize: 10, StreamTTL: time.Minute})

	_, err := e.ReadState(ctx, "ch", MapReadStateOptions{Limit: 10})
	require.NoError(t, err)
	in, ok := mapRemoveIn(e, "ch")
	require.True(t, ok)
	require.InDelta(t, time.Minute.Milliseconds(), in, 2000, "empty map gets the initial TTL")

	_, err = e.Publish(ctx, "ch", "k", MapPublishOptions{Data: []byte("v")})
	require.NoError(t, err)
	_, ok = mapRemoveIn(e, "ch")
	require.False(t, ok, "publication makes the map permanent again")

	_, err = e.ReadStream(ctx, "ch", MapReadStreamOptions{Filter: StreamFilter{Limit: 10}})
	require.NoError(t, err)
	_, ok = mapRemoveIn(e, "ch")
	require.False(t, ok, "a read of a map with publications keeps it permanent")
}

func TestMemoryMapBrokerMetaTTLInitialMetaTTL(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	e := newInitialMetaTTLMapBroker(t, time.Minute, MapChannelOptions{
		Mode: MapModeRecoverable, StreamSize: 10, StreamTTL: time.Minute, KeyTTL: time.Hour, MetaTTL: time.Hour,
	})

	_, err := e.ReadStream(ctx, "ch", MapReadStreamOptions{Filter: StreamFilter{Limit: 10}})
	require.NoError(t, err)
	in, _ := mapRemoveIn(e, "ch")
	require.InDelta(t, time.Minute.Milliseconds(), in, 2000, "empty map gets the initial TTL")

	_, err = e.Publish(ctx, "ch", "k", MapPublishOptions{Data: []byte("v")})
	require.NoError(t, err)
	in, _ = mapRemoveIn(e, "ch")
	require.InDelta(t, time.Hour.Milliseconds(), in, 2000, "publication sets the meta TTL")

	_, err = e.ReadState(ctx, "ch", MapReadStateOptions{Limit: 10})
	require.NoError(t, err)
	in, _ = mapRemoveIn(e, "ch")
	require.InDelta(t, time.Hour.Milliseconds(), in, 2000)
}

func TestMemoryMapBrokerMetaTTLInitialRemoves(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	e := newInitialMetaTTLMapBroker(t, time.Second, MapChannelOptions{Mode: MapModePersistent, StreamSize: 10, StreamTTL: time.Minute})

	_, err := e.ReadState(ctx, "empty", MapReadStateOptions{Limit: 10})
	require.NoError(t, err)
	_, err = e.ReadState(ctx, "published", MapReadStateOptions{Limit: 10})
	require.NoError(t, err)
	_, err = e.Publish(ctx, "published", "k", MapPublishOptions{Data: []byte("v")})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return !mapChannelExists(e, "empty")
	}, 10*time.Second, 50*time.Millisecond, "empty map removed after the initial TTL")
	require.True(t, mapChannelExists(e, "published"), "map with publications kept")
}

// The initial TTL of a subscription reaches the broker on subscribe, and is
// kept in the channel context for position checks.
func TestClientSubscribeHistoryMetaTTLInitial(t *testing.T) {
	t.Parallel()
	node := defaultNodeNoHandlers()
	t.Cleanup(func() { _ = node.Shutdown(context.Background()) })
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{Options: SubscribeOptions{EnablePositioning: true, HistoryMetaTTLInitial: 2 * time.Minute}}, nil)
		})
	})
	client := newTestConnectedClientV2(t, node, "user")
	subscribeClientV2(t, client, "ch")

	hub := node.getBroker("ch").(*MemoryBroker).historyHub
	in, ok := streamRemoveIn(hub, "ch")
	require.True(t, ok)
	require.InDelta(t, 120, in, 2, "subscribe reads with the subscription's initial TTL")

	client.mu.RLock()
	chCtx := client.channels["ch"]
	client.mu.RUnlock()
	require.Equal(t, uint32(120), chCtx.metaTTLInitialSeconds)

	// A position check reads with it too.
	_, err := node.checkPosition("ch", chCtx.streamPosition, 0, time.Duration(chCtx.metaTTLInitialSeconds)*time.Second, false)
	require.NoError(t, err)
	in, _ = streamRemoveIn(hub, "ch")
	require.InDelta(t, 120, in, 2)
}

func TestNodeHistoryMetaTTLInitial(t *testing.T) {
	t.Parallel()
	node := defaultNodeNoHandlers()
	t.Cleanup(func() { _ = node.Shutdown(context.Background()) })

	_, err := node.History("ch", WithHistoryMetaTTLInitial(time.Minute))
	require.NoError(t, err)
	in, ok := streamRemoveIn(node.getBroker("ch").(*MemoryBroker).historyHub, "ch")
	require.True(t, ok)
	require.InDelta(t, 60, in, 2)
}

// MapChannelOptions.MetaTTLInitial applies without the global option.
func TestMemoryMapBrokerMetaTTLInitialPerChannel(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	e := newInitialMetaTTLMapBroker(t, 0, MapChannelOptions{Mode: MapModePersistent, StreamSize: 10, StreamTTL: time.Minute, MetaTTLInitial: time.Minute})

	_, err := e.ReadState(ctx, "ch", MapReadStateOptions{Limit: 10})
	require.NoError(t, err)
	in, ok := mapRemoveIn(e, "ch")
	require.True(t, ok)
	require.InDelta(t, time.Minute.Milliseconds(), in, 2000)

	_, err = e.Publish(ctx, "ch", "k", MapPublishOptions{Data: []byte("v")})
	require.NoError(t, err)
	_, ok = mapRemoveIn(e, "ch")
	require.False(t, ok, "publication makes the map permanent again")
}

func TestMapChannelOptionsMetaTTLInitialValidation(t *testing.T) {
	t.Parallel()
	resolve := func(opts MapChannelOptions) error {
		_, err := ResolveAndValidateMapChannelOptions(func(string) MapChannelOptions { return opts }, "ch")
		return err
	}
	require.Error(t, resolve(MapChannelOptions{Mode: MapModeEphemeral, KeyTTL: time.Minute, MetaTTLInitial: time.Minute}))
	require.Error(t, resolve(MapChannelOptions{Mode: MapModePersistent, MetaTTLInitial: -time.Second}))
	require.NoError(t, resolve(MapChannelOptions{Mode: MapModePersistent, MetaTTLInitial: time.Minute}))
}

// A subscription's initial TTL below 3 position checks fails the subscription:
// its meta could expire under live subscribers.
func TestClientSubscribeHistoryMetaTTLInitialTooShort(t *testing.T) {
	t.Parallel()
	node := defaultNodeNoHandlers() // ClientChannelPositionCheckDelay 40s.
	t.Cleanup(func() { _ = node.Shutdown(context.Background()) })
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{Options: SubscribeOptions{EnablePositioning: true, HistoryMetaTTLInitial: time.Minute}}, nil)
		})
	})
	client := newTestConnectedClientV2(t, node, "user")

	rwWrapper := testReplyWriterWrapper()
	err := client.handleSubscribe(&protocol.SubscribeRequest{Channel: "ch"}, &protocol.Command{Id: 1}, time.Now(), rwWrapper.rw)
	require.NoError(t, err)
	require.Equal(t, ErrorInternal.toProto(), rwWrapper.replies[0].Error)
	require.NotContains(t, client.Channels(), "ch")
	require.Zero(t, node.Hub().NumSubscribers("ch"))
	_, ok := streamRemoveIn(node.getBroker("ch").(*MemoryBroker).historyHub, "ch")
	require.False(t, ok, "no meta read with the invalid TTL")
}

// A sub-second initial TTL isn't truncated to 0, which would mean the global one.
func TestClientSubscribeHistoryMetaTTLInitialRoundsUp(t *testing.T) {
	t.Parallel()
	node, err := New(Config{
		LogLevel:                        LogLevelError,
		LogHandler:                      func(LogEntry) {},
		ClientChannelPositionCheckDelay: 100 * time.Millisecond,
	})
	require.NoError(t, err)
	require.NoError(t, node.Run())
	t.Cleanup(func() { _ = node.Shutdown(context.Background()) })
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{Options: SubscribeOptions{EnablePositioning: true, HistoryMetaTTLInitial: 300 * time.Millisecond}}, nil)
		})
	})
	client := newTestConnectedClientV2(t, node, "user")
	subscribeClientV2(t, client, "ch")

	client.mu.RLock()
	chCtx := client.channels["ch"]
	client.mu.RUnlock()
	require.Equal(t, uint32(1), chCtx.metaTTLInitialSeconds)
}

// A map's initial TTL below 3 position checks fails subscribing to the map,
// while publishing keeps working: only subscriptions rely on it.
func TestMapSubscribeMetaTTLInitialTooShort(t *testing.T) {
	t.Parallel()
	node, broker := newTestNodeWithMapBroker(t) // ClientChannelPositionCheckDelay 40s.
	node.config.Map.GetMapChannelOptions = func(string) MapChannelOptions {
		return MapChannelOptions{Mode: MapModePersistent, MinPageSize: 1, MetaTTLInitial: time.Minute}
	}
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{Options: SubscribeOptions{Type: SubscriptionTypeMap}}, nil)
		})
	})
	client := newTestConnectedClientV2(t, node, "user")

	protoErr := subscribeMapClientExpectError(t, client, &protocol.SubscribeRequest{
		Channel: "ch",
		Type:    int32(SubscriptionTypeMap),
		Phase:   MapPhaseState,
		Limit:   100,
	})
	require.Equal(t, ErrorInternal.Code, protoErr.Code)
	require.NotContains(t, client.Channels(), "ch")

	_, err := broker.Publish(context.Background(), "ch", "k", MapPublishOptions{Data: []byte(`{}`)})
	require.NoError(t, err)
}

// HistoryReply.MetaTTL and MetaTTLInitial apply to the history read of a
// client's history request, which creates the meta of an empty channel.
func TestClientHistoryReplyMetaTTL(t *testing.T) {
	t.Parallel()
	node := defaultNodeNoHandlers() // ClientChannelPositionCheckDelay 40s.
	t.Cleanup(func() { _ = node.Shutdown(context.Background()) })
	client := newTestClient(t, node, "42")
	var reply HistoryReply
	client.OnHistory(func(e HistoryEvent, cb HistoryCallback) {
		cb(reply, nil)
	})
	connectClientV2(t, client)
	hub := node.getBroker("ch").(*MemoryBroker).historyHub

	history := func(ch string) *protocol.Error {
		rwWrapper := testReplyWriterWrapper()
		err := client.handleHistory(&protocol.HistoryRequest{Channel: ch}, &protocol.Command{}, time.Now(), rwWrapper.rw)
		require.NoError(t, err)
		require.Len(t, rwWrapper.replies, 1)
		return rwWrapper.replies[0].Error
	}

	reply = HistoryReply{MetaTTL: 10 * time.Minute}
	require.Nil(t, history("meta_ttl"))
	in, ok := streamRemoveIn(hub, "meta_ttl")
	require.True(t, ok)
	require.InDelta(t, 600, in, 2)

	reply = HistoryReply{MetaTTLInitial: 2 * time.Minute}
	require.Nil(t, history("initial"))
	in, ok = streamRemoveIn(hub, "initial")
	require.True(t, ok)
	require.InDelta(t, 120, in, 2)

	reply = HistoryReply{MetaTTLInitial: time.Minute}
	require.Equal(t, ErrorInternal.toProto(), history("too_short"))
	_, ok = streamRemoveIn(hub, "too_short")
	require.False(t, ok, "no meta read with the invalid TTL")
}
