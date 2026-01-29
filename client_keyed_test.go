package centrifuge

import (
	"context"
	"testing"
	"time"

	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/require"
)

// newTestNodeWithKeyedEngine creates a test node with a memory keyed engine.
func newTestNodeWithKeyedEngine(t *testing.T) (*Node, *MemoryKeyedEngine) {
	node, err := New(Config{
		LogLevel:   LogLevelTrace,
		LogHandler: func(entry LogEntry) {},
	})
	require.NoError(t, err)

	engine, err := NewMemoryKeyedEngine(node, MemoryKeyedEngineConfig{})
	require.NoError(t, err)
	err = engine.RegisterBrokerEventHandler(nil)
	require.NoError(t, err)

	node.SetKeyedEngine(engine)

	err = node.Run()
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = node.Shutdown(context.Background())
	})

	return node, engine
}

// subscribeKeyedClient performs a keyed subscribe request and returns the result.
func subscribeKeyedClient(t testing.TB, client *Client, req *protocol.SubscribeRequest) *protocol.SubscribeResult {
	rwWrapper := testReplyWriterWrapper()
	err := client.handleSubscribe(req, &protocol.Command{Id: 1}, time.Now(), rwWrapper.rw)
	require.NoError(t, err)
	require.Len(t, rwWrapper.replies, 1)
	require.Nil(t, rwWrapper.replies[0].Error, "subscribe error: %v", rwWrapper.replies[0].Error)
	return rwWrapper.replies[0].Subscribe
}

// subscribeKeyedClientExpectError performs a keyed subscribe request expecting an error.
func subscribeKeyedClientExpectError(t testing.TB, client *Client, req *protocol.SubscribeRequest) *protocol.Error {
	rwWrapper := testReplyWriterWrapper()
	err := client.handleSubscribe(req, &protocol.Command{Id: 1}, time.Now(), rwWrapper.rw)
	if err != nil {
		// Direct error returned
		var clientErr *Error
		if ok := err.(*Error); ok != nil {
			clientErr = ok
			return &protocol.Error{Code: clientErr.Code, Message: clientErr.Message}
		}
		t.Fatalf("unexpected error type: %T", err)
	}
	require.Len(t, rwWrapper.replies, 1)
	require.NotNil(t, rwWrapper.replies[0].Error)
	return rwWrapper.replies[0].Error
}

func TestKeyedSubscribe_SnapshotPhase(t *testing.T) {
	node, engine := newTestNodeWithKeyedEngine(t)

	channel := "test_keyed"
	ctx := context.Background()

	// Pre-populate some keyed data. Must use valid JSON for data since test uses JSON transport.
	_, err := engine.Publish(ctx, channel, "key1", KeyedPublishOptions{
		Data:       []byte(`{"value":"data1"}`),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)

	_, err = engine.Publish(ctx, channel, "key2", KeyedPublishOptions{
		Data:       []byte(`{"value":"data2"}`),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					EnableKeyed: true,
				},
			}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")

	// Verify keyed engine is set.
	require.NotNil(t, node.KeyedEngine(), "KeyedEngine should be set")

	// Send snapshot phase request.
	rwWrapper := testReplyWriterWrapper()
	err = client.handleSubscribe(&protocol.SubscribeRequest{
		Channel:    channel,
		Keyed:      true,
		KeyedPhase: KeyedPhaseSnapshot,
		KeyedLimit: 100,
	}, &protocol.Command{Id: 1}, time.Now(), rwWrapper.rw)

	require.NoError(t, err)
	require.Len(t, rwWrapper.replies, 1)

	result := rwWrapper.replies[0].Subscribe
	require.Nil(t, rwWrapper.replies[0].Error)
	require.True(t, result.Keyed)
	require.Equal(t, KeyedPhaseSnapshot, result.KeyedPhase)
	require.Len(t, result.Publications, 2)
	require.NotEmpty(t, result.Epoch)
	require.Greater(t, result.Offset, uint64(0))
}

func TestKeyedSubscribe_SnapshotPagination(t *testing.T) {
	node, engine := newTestNodeWithKeyedEngine(t)

	channel := "test_keyed_pagination"
	ctx := context.Background()

	// Pre-populate keyed data.
	for i := 0; i < 10; i++ {
		_, err := engine.Publish(ctx, channel, string(rune('a'+i)), KeyedPublishOptions{
			Data:       []byte(`{"v":"data"}`),
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
			KeyTTL:     300 * time.Second,
		})
		require.NoError(t, err)
	}

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					EnableKeyed: true,
				},
			}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")

	// First page.
	result := subscribeKeyedClient(t, client, &protocol.SubscribeRequest{
		Channel:    channel,
		Keyed:      true,
		KeyedPhase: KeyedPhaseSnapshot,
		KeyedLimit: 5,
	})

	require.True(t, result.Keyed)
	require.Equal(t, KeyedPhaseSnapshot, result.KeyedPhase)
	require.Len(t, result.Publications, 5)
	require.NotEmpty(t, result.KeyedCursor) // More pages available.

	epoch := result.Epoch

	// Second page using cursor.
	result2 := subscribeKeyedClient(t, client, &protocol.SubscribeRequest{
		Channel:     channel,
		Keyed:       true,
		KeyedPhase:  KeyedPhaseSnapshot,
		KeyedLimit:  5,
		KeyedCursor: result.KeyedCursor,
	})

	require.True(t, result2.Keyed)
	require.Equal(t, KeyedPhaseSnapshot, result2.KeyedPhase)
	require.Len(t, result2.Publications, 5)
	require.Empty(t, result2.KeyedCursor) // Last page.
	require.Equal(t, epoch, result2.Epoch)
}

func TestKeyedSubscribe_StreamPhase(t *testing.T) {
	node, engine := newTestNodeWithKeyedEngine(t)

	channel := "test_stream"
	ctx := context.Background()

	// Pre-populate stream data.
	var lastOffset uint64
	var epoch string
	for i := 0; i < 5; i++ {
		res, err := engine.Publish(ctx, channel, string(rune('a'+i)), KeyedPublishOptions{
			Data:       []byte(`{"v":"data"}`),
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
			KeyTTL:     300 * time.Second,
		})
		require.NoError(t, err)
		lastOffset = res.Position.Offset
		epoch = res.Position.Epoch
	}

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					EnableKeyed: true,
				},
			}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")

	// First, do snapshot to authorize.
	subscribeKeyedClient(t, client, &protocol.SubscribeRequest{
		Channel:    channel,
		Keyed:      true,
		KeyedPhase: KeyedPhaseSnapshot,
		KeyedLimit: 100,
	})

	// Now do stream phase from offset 2.
	result := subscribeKeyedClient(t, client, &protocol.SubscribeRequest{
		Channel:     channel,
		Keyed:       true,
		KeyedPhase:  KeyedPhaseStream,
		KeyedOffset: 2,
		KeyedEpoch:  epoch,
		KeyedLimit:  100,
	})

	require.True(t, result.Keyed)
	require.Equal(t, KeyedPhaseStream, result.KeyedPhase)
	require.Len(t, result.Publications, 3) // Publications 3, 4, 5.
	require.Equal(t, lastOffset, result.Offset)
}

func TestKeyedSubscribe_LivePhase(t *testing.T) {
	node, engine := newTestNodeWithKeyedEngine(t)

	channel := "test_live"
	ctx := context.Background()

	// Pre-populate some data.
	res, err := engine.Publish(ctx, channel, "key1", KeyedPublishOptions{
		Data:       []byte(`{"v":"data1"}`),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					EnableKeyed: true,
				},
			}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")

	// First do snapshot to authorize.
	subscribeKeyedClient(t, client, &protocol.SubscribeRequest{
		Channel:    channel,
		Keyed:      true,
		KeyedPhase: KeyedPhaseSnapshot,
		KeyedLimit: 100,
	})

	// Now join live.
	result := subscribeKeyedClient(t, client, &protocol.SubscribeRequest{
		Channel:     channel,
		Keyed:       true,
		KeyedPhase:  KeyedPhaseLive,
		KeyedOffset: res.Position.Offset,
		KeyedEpoch:  res.Position.Epoch,
	})

	require.True(t, result.Keyed)
	require.Equal(t, KeyedPhaseLive, result.KeyedPhase)
	require.Equal(t, res.Position.Epoch, result.Epoch)

	// Client should now be subscribed.
	require.Contains(t, client.channels, channel)
	chCtx := client.channels[channel]
	require.True(t, channelHasFlag(chCtx.flags, flagKeyed))
	require.True(t, channelHasFlag(chCtx.flags, flagSubscribed))
}

func TestKeyedSubscribe_DirectLive(t *testing.T) {
	node, engine := newTestNodeWithKeyedEngine(t)

	channel := "test_direct_live"
	ctx := context.Background()

	// Pre-populate some data.
	_, err := engine.Publish(ctx, channel, "key1", KeyedPublishOptions{
		Data:       []byte(`{"v":"data1"}`),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					EnableKeyed: true,
				},
			}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")

	// Go directly to live without pagination.
	result := subscribeKeyedClient(t, client, &protocol.SubscribeRequest{
		Channel:    channel,
		Keyed:      true,
		KeyedPhase: KeyedPhaseLive,
	})

	require.True(t, result.Keyed)
	require.Equal(t, KeyedPhaseLive, result.KeyedPhase)
	require.NotEmpty(t, result.Epoch)
	require.Contains(t, client.channels, channel)
}

func TestKeyedSubscribe_FullTwoPhase(t *testing.T) {
	node, engine := newTestNodeWithKeyedEngine(t)

	channel := "test_two_phase"
	ctx := context.Background()

	// Pre-populate data.
	for i := 0; i < 5; i++ {
		_, err := engine.Publish(ctx, channel, string(rune('a'+i)), KeyedPublishOptions{
			Data:       []byte(`{"v":"data"}`),
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
			KeyTTL:     300 * time.Second,
		})
		require.NoError(t, err)
	}

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					EnableKeyed: true,
				},
			}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")

	// Phase 1: Snapshot.
	snapshotResult := subscribeKeyedClient(t, client, &protocol.SubscribeRequest{
		Channel:    channel,
		Keyed:      true,
		KeyedPhase: KeyedPhaseSnapshot,
		KeyedLimit: 100,
	})

	require.True(t, snapshotResult.Keyed)
	require.Equal(t, KeyedPhaseSnapshot, snapshotResult.KeyedPhase)
	require.Len(t, snapshotResult.Publications, 5)

	// Phase 2: Live.
	liveResult := subscribeKeyedClient(t, client, &protocol.SubscribeRequest{
		Channel:     channel,
		Keyed:       true,
		KeyedPhase:  KeyedPhaseLive,
		KeyedOffset: snapshotResult.Offset,
		KeyedEpoch:  snapshotResult.Epoch,
	})

	require.True(t, liveResult.Keyed)
	require.Equal(t, KeyedPhaseLive, liveResult.KeyedPhase)
	require.Contains(t, client.channels, channel)
}

func TestKeyedSubscribe_NotEnabled(t *testing.T) {
	node, _ := newTestNodeWithKeyedEngine(t)

	channel := "test_not_enabled"

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			// Don't enable keyed mode.
			cb(SubscribeReply{
				Options: SubscribeOptions{
					EnableKeyed: false,
				},
			}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")

	// Try keyed subscribe - should fail with BadRequest since keyed is not enabled.
	rwWrapper := testReplyWriterWrapper()
	err := client.handleSubscribe(&protocol.SubscribeRequest{
		Channel:    channel,
		Keyed:      true,
		KeyedPhase: KeyedPhaseSnapshot,
	}, &protocol.Command{Id: 1}, time.Now(), rwWrapper.rw)
	require.NoError(t, err)
	require.Len(t, rwWrapper.replies, 1)
	require.NotNil(t, rwWrapper.replies[0].Error)
	require.Equal(t, ErrorBadRequest.Code, rwWrapper.replies[0].Error.Code)
}

func TestKeyedSubscribe_AlreadySubscribed(t *testing.T) {
	node, _ := newTestNodeWithKeyedEngine(t)

	channel := "test_already_subscribed"

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					EnableKeyed: true,
				},
			}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")

	// First subscribe.
	subscribeKeyedClient(t, client, &protocol.SubscribeRequest{
		Channel:    channel,
		Keyed:      true,
		KeyedPhase: KeyedPhaseLive,
	})

	// Second subscribe should fail.
	rwWrapper := testReplyWriterWrapper()
	err := client.handleSubscribe(&protocol.SubscribeRequest{
		Channel:    channel,
		Keyed:      true,
		KeyedPhase: KeyedPhaseSnapshot,
	}, &protocol.Command{Id: 1}, time.Now(), rwWrapper.rw)
	require.Equal(t, ErrorAlreadySubscribed, err)
}

func TestKeyedSubscribe_WithPresence(t *testing.T) {
	node, engine := newTestNodeWithKeyedEngine(t)

	channel := "test_with_presence"
	ctx := context.Background()

	// Pre-populate data.
	_, err := engine.Publish(ctx, channel, "key1", KeyedPublishOptions{
		Data:       []byte(`{"v":"data1"}`),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					EnableKeyed:             true,
					EmitKeyedClientPresence: true,
				},
			}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")

	// Subscribe with keyed client presence.
	subscribeKeyedClient(t, client, &protocol.SubscribeRequest{
		Channel:    channel,
		Keyed:      true,
		KeyedPhase: KeyedPhaseLive,
	})

	// Verify presence was added to :clients channel.
	clientsChannel := channel + ":clients"
	entries, _, _, err := engine.ReadSnapshot(ctx, clientsChannel, KeyedReadSnapshotOptions{
		Limit: 100,
	})
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, client.uid, entries[0].Key)

	// Verify flagEmitKeyedClientPresence is set.
	require.Contains(t, client.channels, channel)
	chCtx := client.channels[channel]
	require.True(t, channelHasFlag(chCtx.flags, flagEmitKeyedClientPresence))
	require.True(t, channelHasFlag(chCtx.flags, flagKeyed))
}

func TestKeyedSubscribe_PresenceCleanupOnUnsubscribe(t *testing.T) {
	node, engine := newTestNodeWithKeyedEngine(t)

	channel := "test_presence_cleanup"
	ctx := context.Background()

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					EnableKeyed:             true,
					EmitKeyedClientPresence: true,
				},
			}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")

	// Subscribe with keyed client presence.
	subscribeKeyedClient(t, client, &protocol.SubscribeRequest{
		Channel:    channel,
		Keyed:      true,
		KeyedPhase: KeyedPhaseLive,
	})

	// Verify presence exists in :clients channel.
	clientsChannel := channel + ":clients"
	entries, _, _, err := engine.ReadSnapshot(ctx, clientsChannel, KeyedReadSnapshotOptions{
		Limit: 100,
	})
	require.NoError(t, err)
	require.Len(t, entries, 1)

	// Unsubscribe.
	client.Unsubscribe(channel)

	// Verify presence was removed.
	entries, _, _, err = engine.ReadSnapshot(ctx, clientsChannel, KeyedReadSnapshotOptions{
		Limit: 100,
	})
	require.NoError(t, err)
	require.Len(t, entries, 0)
}

func TestKeyedSubscribe_PresenceCleanupOnDisconnect(t *testing.T) {
	node, engine := newTestNodeWithKeyedEngine(t)

	channel := "test_presence_disconnect"
	ctx := context.Background()

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					EnableKeyed:             true,
					EmitKeyedClientPresence: true,
				},
			}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")

	// Subscribe with keyed client presence.
	subscribeKeyedClient(t, client, &protocol.SubscribeRequest{
		Channel:    channel,
		Keyed:      true,
		KeyedPhase: KeyedPhaseLive,
	})

	// Verify presence exists in :clients channel.
	clientsChannel := channel + ":clients"
	entries, _, _, err := engine.ReadSnapshot(ctx, clientsChannel, KeyedReadSnapshotOptions{
		Limit: 100,
	})
	require.NoError(t, err)
	require.Len(t, entries, 1)

	// Disconnect client.
	err = client.close(DisconnectForceNoReconnect)
	require.NoError(t, err)

	// Verify presence was removed.
	entries, _, _, err = engine.ReadSnapshot(ctx, clientsChannel, KeyedReadSnapshotOptions{
		Limit: 100,
	})
	require.NoError(t, err)
	require.Len(t, entries, 0)
}

func TestPresenceSubscribe_Snapshot(t *testing.T) {
	node, engine := newTestNodeWithKeyedEngine(t)

	ctx := context.Background()

	// Pre-populate presence data.
	presenceChannel := "test_presence_sub:clients"
	_, err := engine.Publish(ctx, presenceChannel, "client1", KeyedPublishOptions{
		ClientInfo: &ClientInfo{ClientID: "client1", UserID: "user1"},
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)

	_, err = engine.Publish(ctx, presenceChannel, "client2", KeyedPublishOptions{
		ClientInfo: &ClientInfo{ClientID: "client2", UserID: "user2"},
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)

	node.OnConnect(func(client *Client) {
		// Presence subscriptions go through OnPresenceSubscribe (separate permission scope).
		client.OnPresenceSubscribe(func(e PresenceSubscribeEvent, cb PresenceSubscribeCallback) {
			// Event receives the base channel (without :clients suffix).
			require.Equal(t, "test_presence_sub", e.Channel)
			cb(PresenceSubscribeReply{}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")

	// Subscribe to presence snapshot.
	result := subscribeKeyedClient(t, client, &protocol.SubscribeRequest{
		Channel:       presenceChannel,
		KeyedPresence: true,
		KeyedPhase:    KeyedPhaseSnapshot,
		KeyedLimit:    100,
	})

	require.True(t, result.Keyed)
	require.Equal(t, KeyedPhaseSnapshot, result.KeyedPhase)
	require.Len(t, result.Publications, 2)
}

func TestPresenceSubscribe_Live(t *testing.T) {
	node, engine := newTestNodeWithKeyedEngine(t)

	ctx := context.Background()

	// Pre-populate presence data.
	presenceChannel := "test_presence_live:clients"
	_, err := engine.Publish(ctx, presenceChannel, "client1", KeyedPublishOptions{
		ClientInfo: &ClientInfo{ClientID: "client1", UserID: "user1"},
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)

	node.OnConnect(func(client *Client) {
		client.OnPresenceSubscribe(func(e PresenceSubscribeEvent, cb PresenceSubscribeCallback) {
			cb(PresenceSubscribeReply{}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")

	// First do snapshot.
	subscribeKeyedClient(t, client, &protocol.SubscribeRequest{
		Channel:       presenceChannel,
		KeyedPresence: true,
		KeyedPhase:    KeyedPhaseSnapshot,
		KeyedLimit:    100,
	})

	// Then go live.
	result := subscribeKeyedClient(t, client, &protocol.SubscribeRequest{
		Channel:       presenceChannel,
		KeyedPresence: true,
		KeyedPhase:    KeyedPhaseLive,
	})

	require.True(t, result.Keyed)
	require.Equal(t, KeyedPhaseLive, result.KeyedPhase)

	// Verify presence subscription is tracked in unified channels map with flagKeyedPresence.
	client.mu.RLock()
	ctx2, ok := client.channels[presenceChannel]
	client.mu.RUnlock()
	require.True(t, ok)
	require.True(t, channelHasFlag(ctx2.flags, flagKeyedPresence))
}

func TestPresenceSubscribe_NotAllowed(t *testing.T) {
	node, _ := newTestNodeWithKeyedEngine(t)

	presenceChannel := "test_presence_not_allowed:clients"

	node.OnConnect(func(client *Client) {
		client.OnPresenceSubscribe(func(e PresenceSubscribeEvent, cb PresenceSubscribeCallback) {
			// Deny presence subscriptions.
			cb(PresenceSubscribeReply{}, ErrorPermissionDenied)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")

	// Try to subscribe to presence - should be denied.
	rwWrapper := testReplyWriterWrapper()
	err := client.handleSubscribe(&protocol.SubscribeRequest{
		Channel:       presenceChannel,
		KeyedPresence: true,
		KeyedPhase:    KeyedPhaseSnapshot,
	}, &protocol.Command{Id: 1}, time.Now(), rwWrapper.rw)
	require.NoError(t, err)
	require.Len(t, rwWrapper.replies, 1)
	require.NotNil(t, rwWrapper.replies[0].Error)
	require.Equal(t, ErrorPermissionDenied.Code, rwWrapper.replies[0].Error.Code)
}

func TestPresenceSubscribe_NoHandler(t *testing.T) {
	node, _ := newTestNodeWithKeyedEngine(t)

	presenceChannel := "test_presence_no_handler:clients"

	// No OnPresenceSubscribe handler set.
	node.OnConnect(func(client *Client) {})

	client := newTestConnectedClientV2(t, node, "user1")

	// Try to subscribe to presence - should fail because no OnPresenceSubscribe handler.
	rwWrapper := testReplyWriterWrapper()
	err := client.handleSubscribe(&protocol.SubscribeRequest{
		Channel:       presenceChannel,
		KeyedPresence: true,
		KeyedPhase:    KeyedPhaseSnapshot,
	}, &protocol.Command{Id: 1}, time.Now(), rwWrapper.rw)
	require.NoError(t, err)
	require.Len(t, rwWrapper.replies, 1)
	require.NotNil(t, rwWrapper.replies[0].Error)
	require.Equal(t, ErrorNotAvailable.Code, rwWrapper.replies[0].Error.Code)
}

func TestPresenceSubscribe_AlreadySubscribed(t *testing.T) {
	node, _ := newTestNodeWithKeyedEngine(t)

	presenceChannel := "test_presence_already_sub:clients"

	node.OnConnect(func(client *Client) {
		client.OnPresenceSubscribe(func(e PresenceSubscribeEvent, cb PresenceSubscribeCallback) {
			cb(PresenceSubscribeReply{}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")

	// First subscribe to presence.
	subscribeKeyedClient(t, client, &protocol.SubscribeRequest{
		Channel:       presenceChannel,
		KeyedPresence: true,
		KeyedPhase:    KeyedPhaseSnapshot,
	})
	subscribeKeyedClient(t, client, &protocol.SubscribeRequest{
		Channel:       presenceChannel,
		KeyedPresence: true,
		KeyedPhase:    KeyedPhaseLive,
	})

	// Second subscribe should fail with AlreadySubscribed.
	rwWrapper := testReplyWriterWrapper()
	err := client.handleSubscribe(&protocol.SubscribeRequest{
		Channel:       presenceChannel,
		KeyedPresence: true,
		KeyedPhase:    KeyedPhaseSnapshot,
	}, &protocol.Command{Id: 1}, time.Now(), rwWrapper.rw)
	require.NoError(t, err)
	require.Len(t, rwWrapper.replies, 1)
	require.NotNil(t, rwWrapper.replies[0].Error)
	require.Equal(t, ErrorAlreadySubscribed.Code, rwWrapper.replies[0].Error.Code)
}

func TestKeyedPresenceTTL(t *testing.T) {
	node, _ := newTestNodeWithKeyedEngine(t)

	// Default ClientPresenceUpdateInterval is 27 seconds.
	node.config.ClientPresenceUpdateInterval = 10 * time.Second

	client := newTestClientV2(t, node, "user1")

	// TTL should be 3x the update interval.
	ttl := client.keyedPresenceTTL()
	require.Equal(t, 30*time.Second, ttl)

	// With very small interval, minimum should apply.
	node.config.ClientPresenceUpdateInterval = 5 * time.Second
	ttl = client.keyedPresenceTTL()
	require.Equal(t, 30*time.Second, ttl) // Minimum 30 seconds.
}

func TestKeyedSubscribe_WithKeyedClientAndUserPresence(t *testing.T) {
	node, engine := newTestNodeWithKeyedEngine(t)

	channel := "test_keyed_presence"
	ctx := context.Background()

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					EnableKeyed:             true,
					EmitKeyedClientPresence: true,
					EmitKeyedUserPresence:   true,
				},
			}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")

	// Subscribe with both client and user presence.
	subscribeKeyedClient(t, client, &protocol.SubscribeRequest{
		Channel:    channel,
		Keyed:      true,
		KeyedPhase: KeyedPhaseLive,
	})

	// Verify :clients presence was added (key=clientId, full info).
	clientsChannel := channel + ":clients"
	entries, _, _, err := engine.ReadSnapshot(ctx, clientsChannel, KeyedReadSnapshotOptions{
		Limit: 100,
	})
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, client.uid, entries[0].Key)
	require.NotNil(t, entries[0].Info)
	require.Equal(t, client.uid, entries[0].Info.ClientID)
	require.Equal(t, "user1", entries[0].Info.UserID)

	// Verify :users presence was added (key=userId, no info).
	usersChannel := channel + ":users"
	entries, _, _, err = engine.ReadSnapshot(ctx, usersChannel, KeyedReadSnapshotOptions{
		Limit: 100,
	})
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, "user1", entries[0].Key)
	// No ClientInfo for :users channel.
	require.Nil(t, entries[0].Info)

	// Verify flags are set correctly.
	require.Contains(t, client.channels, channel)
	chCtx := client.channels[channel]
	require.True(t, channelHasFlag(chCtx.flags, flagEmitKeyedClientPresence))
	require.True(t, channelHasFlag(chCtx.flags, flagEmitKeyedUserPresence))
	require.True(t, channelHasFlag(chCtx.flags, flagKeyed))
}

func TestKeyedSubscribe_KeyedPresenceCleanupOnDisconnect(t *testing.T) {
	node, engine := newTestNodeWithKeyedEngine(t)

	channel := "test_keyed_presence_cleanup"
	ctx := context.Background()

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					EnableKeyed:             true,
					EmitKeyedClientPresence: true,
					EmitKeyedUserPresence:   true,
				},
			}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")

	// Subscribe with both client and user presence.
	subscribeKeyedClient(t, client, &protocol.SubscribeRequest{
		Channel:    channel,
		Keyed:      true,
		KeyedPhase: KeyedPhaseLive,
	})

	// Verify presence exists in both channels.
	clientsChannel := channel + ":clients"
	usersChannel := channel + ":users"

	entries, _, _, err := engine.ReadSnapshot(ctx, clientsChannel, KeyedReadSnapshotOptions{Limit: 100})
	require.NoError(t, err)
	require.Len(t, entries, 1)

	entries, _, _, err = engine.ReadSnapshot(ctx, usersChannel, KeyedReadSnapshotOptions{Limit: 100})
	require.NoError(t, err)
	require.Len(t, entries, 1)

	// Disconnect client.
	err = client.close(DisconnectForceNoReconnect)
	require.NoError(t, err)

	// Verify :clients presence was removed.
	entries, _, _, err = engine.ReadSnapshot(ctx, clientsChannel, KeyedReadSnapshotOptions{Limit: 100})
	require.NoError(t, err)
	require.Len(t, entries, 0)

	// Verify :users presence is NOT removed (TTL-based expiration).
	entries, _, _, err = engine.ReadSnapshot(ctx, usersChannel, KeyedReadSnapshotOptions{Limit: 100})
	require.NoError(t, err)
	require.Len(t, entries, 1) // Still there, will expire via TTL.
}

func TestKeyedSubscribe_MultipleClientsPerUser(t *testing.T) {
	node, engine := newTestNodeWithKeyedEngine(t)

	channel := "test_multi_clients"
	ctx := context.Background()

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					EnableKeyed:             true,
					EmitKeyedClientPresence: true,
					EmitKeyedUserPresence:   true,
				},
			}, nil)
		})
	})

	// Connect two clients with the same user.
	client1 := newTestConnectedClientV2(t, node, "user1")
	client2 := newTestConnectedClientV2(t, node, "user1")

	// Subscribe both clients.
	subscribeKeyedClient(t, client1, &protocol.SubscribeRequest{
		Channel:    channel,
		Keyed:      true,
		KeyedPhase: KeyedPhaseLive,
	})
	subscribeKeyedClient(t, client2, &protocol.SubscribeRequest{
		Channel:    channel,
		Keyed:      true,
		KeyedPhase: KeyedPhaseLive,
	})

	// Verify :clients has two entries (one per connection).
	clientsChannel := channel + ":clients"
	entries, _, _, err := engine.ReadSnapshot(ctx, clientsChannel, KeyedReadSnapshotOptions{Limit: 100})
	require.NoError(t, err)
	require.Len(t, entries, 2)

	// Verify :users has one entry (deduplicated by userId).
	usersChannel := channel + ":users"
	entries, _, _, err = engine.ReadSnapshot(ctx, usersChannel, KeyedReadSnapshotOptions{Limit: 100})
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, "user1", entries[0].Key)

	// Disconnect one client.
	err = client1.close(DisconnectForceNoReconnect)
	require.NoError(t, err)

	// :clients should have one entry.
	entries, _, _, err = engine.ReadSnapshot(ctx, clientsChannel, KeyedReadSnapshotOptions{Limit: 100})
	require.NoError(t, err)
	require.Len(t, entries, 1)

	// :users still has the user (TTL refresh from client2).
	entries, _, _, err = engine.ReadSnapshot(ctx, usersChannel, KeyedReadSnapshotOptions{Limit: 100})
	require.NoError(t, err)
	require.Len(t, entries, 1)
}
