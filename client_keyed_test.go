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

func TestKeyedSubscribe_CleanupOnUnsubscribe(t *testing.T) {
	node, engine := newTestNodeWithKeyedEngine(t)

	channel := "test_cleanup_on_unsub"
	ctx := context.Background()

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					EnableKeyed:          true,
					CleanupOnUnsubscribe: true,
				},
			}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")
	clientID := client.ID()

	// Subscribe with CleanupOnUnsubscribe.
	subscribeKeyedClient(t, client, &protocol.SubscribeRequest{
		Channel:    channel,
		Keyed:      true,
		KeyedPhase: KeyedPhaseLive,
	})

	// Publish a key with key=clientID (simulating cursor/ephemeral state).
	_, err := engine.Publish(ctx, channel, clientID, KeyedPublishOptions{
		Publish:    true,
		Data:       []byte(`{"x":100,"y":200}`),
		StreamSize: 1000,
		ClientInfo: &ClientInfo{ClientID: clientID, UserID: "user1"},
	})
	require.NoError(t, err)

	// Verify key exists.
	entries, _, _, err := engine.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{
		Limit: 100,
	})
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, clientID, entries[0].Key)

	// Unsubscribe - should trigger cleanup of key=clientID.
	client.Unsubscribe(channel)

	// Verify key was removed.
	entries, _, _, err = engine.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{
		Limit: 100,
	})
	require.NoError(t, err)
	require.Len(t, entries, 0)
}

func TestKeyedSubscribe_CleanupOnDisconnect(t *testing.T) {
	node, engine := newTestNodeWithKeyedEngine(t)

	channel := "test_cleanup_on_disconnect"
	ctx := context.Background()

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					EnableKeyed:          true,
					CleanupOnUnsubscribe: true,
				},
			}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")
	clientID := client.ID()

	// Subscribe with CleanupOnUnsubscribe.
	subscribeKeyedClient(t, client, &protocol.SubscribeRequest{
		Channel:    channel,
		Keyed:      true,
		KeyedPhase: KeyedPhaseLive,
	})

	// Publish a key with key=clientID.
	_, err := engine.Publish(ctx, channel, clientID, KeyedPublishOptions{
		Publish:    true,
		Data:       []byte(`{"x":100,"y":200}`),
		StreamSize: 1000,
		ClientInfo: &ClientInfo{ClientID: clientID, UserID: "user1"},
	})
	require.NoError(t, err)

	// Verify key exists.
	entries, _, _, err := engine.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{
		Limit: 100,
	})
	require.NoError(t, err)
	require.Len(t, entries, 1)

	// Disconnect client - should trigger cleanup of key=clientID.
	err = client.close(DisconnectForceNoReconnect)
	require.NoError(t, err)

	// Verify key was removed.
	entries, _, _, err = engine.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{
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

// TestKeyedEngine_ReadSnapshotByKey tests the Key filter for ReadSnapshot.
func TestKeyedEngine_ReadSnapshotByKey(t *testing.T) {
	_, engine := newTestNodeWithKeyedEngine(t)
	ctx := context.Background()
	ch := "test_read_by_key"

	// Publish 3 keys
	_, err := engine.Publish(ctx, ch, "key1", KeyedPublishOptions{
		Data:       []byte(`{"value":"data1"}`),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)

	_, err = engine.Publish(ctx, ch, "key2", KeyedPublishOptions{
		Data:       []byte(`{"value":"data2"}`),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)

	_, err = engine.Publish(ctx, ch, "key3", KeyedPublishOptions{
		Data:       []byte(`{"value":"data3"}`),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)

	// Read single key
	pubs, pos, cursor, err := engine.ReadSnapshot(ctx, ch, KeyedReadSnapshotOptions{
		Key: "key2",
	})
	require.NoError(t, err)
	require.Len(t, pubs, 1)
	require.Equal(t, "key2", pubs[0].Key)
	require.Equal(t, []byte(`{"value":"data2"}`), pubs[0].Data)
	require.Empty(t, cursor) // No pagination for single key
	require.NotEmpty(t, pos.Epoch)

	// Read non-existent key
	pubs, _, _, err = engine.ReadSnapshot(ctx, ch, KeyedReadSnapshotOptions{
		Key: "nonexistent",
	})
	require.NoError(t, err)
	require.Len(t, pubs, 0)
}

// TestKeyedEngine_CASSuccess tests successful CAS update.
func TestKeyedEngine_CASSuccess(t *testing.T) {
	_, engine := newTestNodeWithKeyedEngine(t)
	ctx := context.Background()
	ch := "test_cas_success"

	// Publish initial value
	res1, err := engine.Publish(ctx, ch, "counter", KeyedPublishOptions{
		Data:       []byte(`{"value":10}`),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)
	require.False(t, res1.Suppressed)

	// Read current state - position includes offset AND epoch
	pubs, pos, _, err := engine.ReadSnapshot(ctx, ch, KeyedReadSnapshotOptions{Key: "counter"})
	require.NoError(t, err)
	require.Len(t, pubs, 1)
	expectedPos := StreamPosition{Offset: pubs[0].Offset, Epoch: pos.Epoch}

	// CAS update with correct position
	res2, err := engine.Publish(ctx, ch, "counter", KeyedPublishOptions{
		Data:             []byte(`{"value":15}`),
		ExpectedPosition: &expectedPos,
		StreamSize:       100,
		StreamTTL:        300 * time.Second,
		KeyTTL:           300 * time.Second,
	})
	require.NoError(t, err)
	require.False(t, res2.Suppressed)
	require.Greater(t, res2.Position.Offset, res1.Position.Offset)

	// Verify the value was updated
	pubs, _, _, err = engine.ReadSnapshot(ctx, ch, KeyedReadSnapshotOptions{Key: "counter"})
	require.NoError(t, err)
	require.Len(t, pubs, 1)
	require.Equal(t, []byte(`{"value":15}`), pubs[0].Data)
}

// TestKeyedEngine_CASConflict tests CAS conflict when position has changed.
func TestKeyedEngine_CASConflict(t *testing.T) {
	_, engine := newTestNodeWithKeyedEngine(t)
	ctx := context.Background()
	ch := "test_cas_conflict"

	// Publish initial value
	_, err := engine.Publish(ctx, ch, "counter", KeyedPublishOptions{
		Data:       []byte(`{"value":10}`),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)

	// Read current state
	pubs, pos, _, err := engine.ReadSnapshot(ctx, ch, KeyedReadSnapshotOptions{Key: "counter"})
	require.NoError(t, err)
	require.Len(t, pubs, 1)
	originalPos := StreamPosition{Offset: pubs[0].Offset, Epoch: pos.Epoch}

	// Another client updates the key (simulated)
	_, err = engine.Publish(ctx, ch, "counter", KeyedPublishOptions{
		Data:       []byte(`{"value":12}`),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)

	// CAS with stale position - should fail
	res, err := engine.Publish(ctx, ch, "counter", KeyedPublishOptions{
		Data:             []byte(`{"value":15}`),
		ExpectedPosition: &originalPos, // stale offset!
		StreamSize:       100,
		StreamTTL:        300 * time.Second,
		KeyTTL:           300 * time.Second,
	})
	require.NoError(t, err)
	require.True(t, res.Suppressed)
	require.Equal(t, SuppressReasonPositionMismatch, res.SuppressReason)

	// CurrentPublication should contain the current state for immediate retry
	require.NotNil(t, res.CurrentPublication)
	require.Equal(t, []byte(`{"value":12}`), res.CurrentPublication.Data)

	// Immediate retry using returned position - should succeed
	retryPos := StreamPosition{Offset: res.CurrentPublication.Offset, Epoch: res.Position.Epoch}
	res2, err := engine.Publish(ctx, ch, "counter", KeyedPublishOptions{
		Data:             []byte(`{"value":15}`),
		ExpectedPosition: &retryPos,
		StreamSize:       100,
		StreamTTL:        300 * time.Second,
		KeyTTL:           300 * time.Second,
	})
	require.NoError(t, err)
	require.False(t, res2.Suppressed)

	// Verify the value was updated
	pubs, _, _, err = engine.ReadSnapshot(ctx, ch, KeyedReadSnapshotOptions{Key: "counter"})
	require.NoError(t, err)
	require.Len(t, pubs, 1)
	require.Equal(t, []byte(`{"value":15}`), pubs[0].Data)
}

// TestKeyedEngine_CASNonExistent tests CAS on a key that doesn't exist.
func TestKeyedEngine_CASNonExistent(t *testing.T) {
	_, engine := newTestNodeWithKeyedEngine(t)
	ctx := context.Background()
	ch := "test_cas_nonexistent"

	// First create the channel by publishing something
	_, err := engine.Publish(ctx, ch, "other_key", KeyedPublishOptions{
		Data:       []byte(`{"value":1}`),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)

	// Get the epoch
	_, pos, _, err := engine.ReadSnapshot(ctx, ch, KeyedReadSnapshotOptions{Limit: 1})
	require.NoError(t, err)

	// Try CAS on non-existent key with expected position
	expectedPos := StreamPosition{Offset: 42, Epoch: pos.Epoch}
	res, err := engine.Publish(ctx, ch, "newkey", KeyedPublishOptions{
		Data:             []byte(`{"value":1}`),
		ExpectedPosition: &expectedPos, // expects key to exist
		StreamSize:       100,
		StreamTTL:        300 * time.Second,
		KeyTTL:           300 * time.Second,
	})
	require.NoError(t, err)
	require.True(t, res.Suppressed)
	require.Equal(t, SuppressReasonPositionMismatch, res.SuppressReason)
}

// TestKeyedEngine_CASWrongEpoch tests CAS with correct offset but wrong epoch.
func TestKeyedEngine_CASWrongEpoch(t *testing.T) {
	_, engine := newTestNodeWithKeyedEngine(t)
	ctx := context.Background()
	ch := "test_cas_wrong_epoch"

	// Publish initial value
	_, err := engine.Publish(ctx, ch, "counter", KeyedPublishOptions{
		Data:       []byte(`{"value":10}`),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)

	// Read current state
	pubs, _, _, err := engine.ReadSnapshot(ctx, ch, KeyedReadSnapshotOptions{Key: "counter"})
	require.NoError(t, err)
	require.Len(t, pubs, 1)

	// Use correct offset but wrong epoch
	wrongPos := StreamPosition{Offset: pubs[0].Offset, Epoch: "wrong-epoch"}

	res, err := engine.Publish(ctx, ch, "counter", KeyedPublishOptions{
		Data:             []byte(`{"value":15}`),
		ExpectedPosition: &wrongPos,
		StreamSize:       100,
		StreamTTL:        300 * time.Second,
		KeyTTL:           300 * time.Second,
	})
	require.NoError(t, err)
	require.True(t, res.Suppressed)
	require.Equal(t, SuppressReasonPositionMismatch, res.SuppressReason)

	// CurrentPublication should contain the current state for immediate retry
	require.NotNil(t, res.CurrentPublication)
	require.Equal(t, []byte(`{"value":10}`), res.CurrentPublication.Data)

	// Verify value unchanged
	pubs, _, _, err = engine.ReadSnapshot(ctx, ch, KeyedReadSnapshotOptions{Key: "counter"})
	require.NoError(t, err)
	require.Len(t, pubs, 1)
	require.Equal(t, []byte(`{"value":10}`), pubs[0].Data)
}

// TestKeyedEngine_StreamDataDifferentPayloads tests publishing with different
// data for snapshot (full state) and stream (incremental update).
func TestKeyedEngine_StreamDataDifferentPayloads(t *testing.T) {
	_, engine := newTestNodeWithKeyedEngine(t)
	ctx := context.Background()
	ch := "test_stream_data"

	// Publish with different payloads: snapshot gets full state, stream gets delta
	_, err := engine.Publish(ctx, ch, "counter", KeyedPublishOptions{
		Data:       []byte(`{"count":100}`),  // Full state → snapshot
		StreamData: []byte(`{"delta":100}`),  // Incremental → stream
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)

	// Read snapshot - should have full state
	pubs, pos, _, err := engine.ReadSnapshot(ctx, ch, KeyedReadSnapshotOptions{Key: "counter"})
	require.NoError(t, err)
	require.Len(t, pubs, 1)
	require.Equal(t, []byte(`{"count":100}`), pubs[0].Data)

	// Read stream - should have incremental data
	streamPubs, _, err := engine.ReadStream(ctx, ch, KeyedReadStreamOptions{
		Filter: StreamFilter{Limit: 10},
	})
	require.NoError(t, err)
	require.Len(t, streamPubs, 1)
	require.Equal(t, []byte(`{"delta":100}`), streamPubs[0].Data)

	// Update with CAS: read current position, update with different payloads
	expectedPos := StreamPosition{Offset: pubs[0].Offset, Epoch: pos.Epoch}
	_, err = engine.Publish(ctx, ch, "counter", KeyedPublishOptions{
		Data:             []byte(`{"count":105}`), // New full state → snapshot
		StreamData:       []byte(`{"delta":5}`),   // Incremental → stream
		ExpectedPosition: &expectedPos,
		StreamSize:       100,
		StreamTTL:        300 * time.Second,
		KeyTTL:           300 * time.Second,
	})
	require.NoError(t, err)

	// Verify snapshot has new full state
	pubs, _, _, err = engine.ReadSnapshot(ctx, ch, KeyedReadSnapshotOptions{Key: "counter"})
	require.NoError(t, err)
	require.Len(t, pubs, 1)
	require.Equal(t, []byte(`{"count":105}`), pubs[0].Data)

	// Verify stream has both incremental updates
	streamPubs, _, err = engine.ReadStream(ctx, ch, KeyedReadStreamOptions{
		Filter: StreamFilter{Limit: 10},
	})
	require.NoError(t, err)
	require.Len(t, streamPubs, 2)
	require.Equal(t, []byte(`{"delta":100}`), streamPubs[0].Data)
	require.Equal(t, []byte(`{"delta":5}`), streamPubs[1].Data)
}

// TestKeyedEngine_StreamDataWithoutStreamData tests that when StreamData is not set,
// Data is used for both snapshot and stream.
func TestKeyedEngine_StreamDataWithoutStreamData(t *testing.T) {
	_, engine := newTestNodeWithKeyedEngine(t)
	ctx := context.Background()
	ch := "test_no_stream_data"

	// Publish without StreamData - Data should be used for both
	_, err := engine.Publish(ctx, ch, "item", KeyedPublishOptions{
		Data:       []byte(`{"name":"test","value":42}`),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)

	// Read snapshot
	pubs, _, _, err := engine.ReadSnapshot(ctx, ch, KeyedReadSnapshotOptions{Key: "item"})
	require.NoError(t, err)
	require.Len(t, pubs, 1)
	require.Equal(t, []byte(`{"name":"test","value":42}`), pubs[0].Data)

	// Read stream - should have same data
	streamPubs, _, err := engine.ReadStream(ctx, ch, KeyedReadStreamOptions{
		Filter: StreamFilter{Limit: 10},
	})
	require.NoError(t, err)
	require.Len(t, streamPubs, 1)
	require.Equal(t, []byte(`{"name":"test","value":42}`), streamPubs[0].Data)
}
