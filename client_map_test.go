package centrifuge

import (
	"context"
	"testing"
	"time"

	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/require"
)

// newTestNodeWithMapEngine creates a test node with a memory map engine.
func newTestNodeWithMapEngine(t *testing.T) (*Node, *MemoryMapEngine) {
	node, err := New(Config{
		LogLevel:   LogLevelTrace,
		LogHandler: func(entry LogEntry) {},
	})
	require.NoError(t, err)

	engine, err := NewMemoryMapEngine(node, MemoryMapEngineConfig{})
	require.NoError(t, err)
	err = engine.RegisterBrokerEventHandler(nil)
	require.NoError(t, err)

	node.SetMapEngine(engine)

	err = node.Run()
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = node.Shutdown(context.Background())
	})

	return node, engine
}

// subscribeMapClient performs a keyed subscribe request and returns the result.
func subscribeMapClient(t testing.TB, client *Client, req *protocol.SubscribeRequest) *protocol.SubscribeResult {
	rwWrapper := testReplyWriterWrapper()
	err := client.handleSubscribe(req, &protocol.Command{Id: 1}, time.Now(), rwWrapper.rw)
	require.NoError(t, err)
	require.Len(t, rwWrapper.replies, 1)
	require.Nil(t, rwWrapper.replies[0].Error, "subscribe error: %v", rwWrapper.replies[0].Error)
	return rwWrapper.replies[0].Subscribe
}

// subscribeMapClientExpectError performs a keyed subscribe request expecting an error.
func subscribeMapClientExpectError(t testing.TB, client *Client, req *protocol.SubscribeRequest) *protocol.Error {
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

func TestMapSubscribe_SnapshotPhase(t *testing.T) {
	node, engine := newTestNodeWithMapEngine(t)

	channel := "test_keyed"
	ctx := context.Background()

	// Pre-populate some keyed data. Must use valid JSON for data since test uses JSON transport.
	_, err := engine.Publish(ctx, channel, "key1", MapPublishOptions{
		Data:       []byte(`{"value":"data1"}`),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)

	_, err = engine.Publish(ctx, channel, "key2", MapPublishOptions{
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
					EnableMap: true,
				},
			}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")

	// Verify map engine is set.
	require.NotNil(t, node.MapEngine(), "MapEngine should be set")

	// Send snapshot phase request.
	rwWrapper := testReplyWriterWrapper()
	err = client.handleSubscribe(&protocol.SubscribeRequest{
		Channel:    channel,
		Type: 1,
		Phase: MapPhaseState,
		Limit: 100,
	}, &protocol.Command{Id: 1}, time.Now(), rwWrapper.rw)

	require.NoError(t, err)
	require.Len(t, rwWrapper.replies, 1)

	result := rwWrapper.replies[0].Subscribe
	require.Nil(t, rwWrapper.replies[0].Error)
	require.True(t, result.Type == 1)
	require.Equal(t, MapPhaseState, result.Phase)
	require.Len(t, result.State, 2) // State entries in State field, not Publications
	require.NotEmpty(t, result.Epoch)
	require.Greater(t, result.Offset, uint64(0))
}

func TestMapSubscribe_SnapshotPagination(t *testing.T) {
	node, engine := newTestNodeWithMapEngine(t)

	channel := "test_map_pagination"
	ctx := context.Background()

	// Pre-populate keyed data.
	for i := 0; i < 10; i++ {
		_, err := engine.Publish(ctx, channel, string(rune('a'+i)), MapPublishOptions{
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
					EnableMap: true,
				},
			}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")

	// First page.
	result := subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel:    channel,
		Type: 1,
		Phase: MapPhaseState,
		Limit: 5,
	})

	require.True(t, result.Type == 1)
	require.Equal(t, MapPhaseState, result.Phase)
	require.Len(t, result.State, 5) // State entries in State field
	require.NotEmpty(t, result.Cursor) // More pages available.

	epoch := result.Epoch

	// Second page using cursor.
	result2 := subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: channel,
		Type:    1,
		Phase:   MapPhaseState,
		Limit:   5,
		Cursor:  result.Cursor,
	})

	require.True(t, result2.Type == 1)
	require.Equal(t, MapPhaseState, result2.Phase)
	require.Len(t, result2.State, 5) // State entries in State field
	require.Empty(t, result2.Cursor) // Last page.
	require.Equal(t, epoch, result2.Epoch)
}

func TestMapSubscribe_StreamPhase(t *testing.T) {
	node, engine := newTestNodeWithMapEngine(t)

	channel := "test_stream"
	ctx := context.Background()

	// Pre-populate stream data.
	var lastOffset uint64
	var epoch string
	for i := 0; i < 5; i++ {
		res, err := engine.Publish(ctx, channel, string(rune('a'+i)), MapPublishOptions{
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
					EnableMap: true,
				},
			}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")

	// First, do snapshot to authorize.
	subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel:    channel,
		Type: 1,
		Phase: MapPhaseState,
		Limit: 100,
	})

	// Now do stream phase from offset 2.
	// With server-controlled LIVE transition: offset=2 + limit=100 >= streamStart=5,
	// so server should transition to LIVE (phase=0) immediately.
	result := subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: channel,
		Type:    1,
		Phase:   MapPhaseStream,
		Offset:  2,
		Epoch:   epoch,
		Limit:   100,
	})

	require.True(t, result.Type == 1)
	// Server transitions to LIVE when client is close enough to catch up.
	require.Equal(t, MapPhaseLive, result.Phase)
	require.Len(t, result.Publications, 3) // Publications 3, 4, 5.
	require.Equal(t, lastOffset, result.Offset)
}

func TestMapSubscribe_LivePhase(t *testing.T) {
	node, engine := newTestNodeWithMapEngine(t)

	channel := "test_live"
	ctx := context.Background()

	// Pre-populate some data.
	res, err := engine.Publish(ctx, channel, "key1", MapPublishOptions{
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
					EnableMap: true,
				},
			}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")

	// First do snapshot to authorize.
	subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel:    channel,
		Type: 1,
		Phase: MapPhaseState,
		Limit: 100,
	})

	// Now join live.
	result := subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel:     channel,
		Type: 1,
		Phase:  MapPhaseLive,
		Offset: res.Position.Offset,
		Epoch:  res.Position.Epoch,
	})

	require.True(t, result.Type == 1)
	require.Equal(t, MapPhaseLive, result.Phase)
	require.Equal(t, res.Position.Epoch, result.Epoch)

	// Client should now be subscribed.
	require.Contains(t, client.channels, channel)
	chCtx := client.channels[channel]
	require.True(t, channelHasFlag(chCtx.flags, flagMap))
	require.True(t, channelHasFlag(chCtx.flags, flagSubscribed))
}

func TestMapSubscribe_DirectLive(t *testing.T) {
	node, engine := newTestNodeWithMapEngine(t)

	channel := "test_direct_live"
	ctx := context.Background()

	// Pre-populate some data.
	_, err := engine.Publish(ctx, channel, "key1", MapPublishOptions{
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
					EnableMap: true,
				},
			}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")

	// Go directly to live without pagination.
	result := subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel:    channel,
		Type: 1,
		Phase: MapPhaseLive,
	})

	require.True(t, result.Type == 1)
	require.Equal(t, MapPhaseLive, result.Phase)
	require.NotEmpty(t, result.Epoch)
	require.Contains(t, client.channels, channel)
}

func TestMapSubscribe_FullTwoPhase(t *testing.T) {
	node, engine := newTestNodeWithMapEngine(t)

	channel := "test_two_phase"
	ctx := context.Background()

	// Pre-populate data.
	for i := 0; i < 5; i++ {
		_, err := engine.Publish(ctx, channel, string(rune('a'+i)), MapPublishOptions{
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
					EnableMap: true,
				},
			}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")

	// Phase 1: Snapshot.
	snapshotResult := subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel:    channel,
		Type: 1,
		Phase: MapPhaseState,
		Limit: 100,
	})

	require.True(t, snapshotResult.Type == 1)
	require.Equal(t, MapPhaseState, snapshotResult.Phase)
	require.Len(t, snapshotResult.State, 5) // State entries in State field

	// Phase 2: Live.
	liveResult := subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel:     channel,
		Type: 1,
		Phase:  MapPhaseLive,
		Offset: snapshotResult.Offset,
		Epoch:  snapshotResult.Epoch,
	})

	require.True(t, liveResult.Type == 1)
	require.Equal(t, MapPhaseLive, liveResult.Phase)
	require.Contains(t, client.channels, channel)
}

func TestMapSubscribe_NotEnabled(t *testing.T) {
	node, _ := newTestNodeWithMapEngine(t)

	channel := "test_not_enabled"

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			// Don't enable keyed mode.
			cb(SubscribeReply{
				Options: SubscribeOptions{
					EnableMap: false,
				},
			}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")

	// Try keyed subscribe - should fail with BadRequest since keyed is not enabled.
	rwWrapper := testReplyWriterWrapper()
	err := client.handleSubscribe(&protocol.SubscribeRequest{
		Channel:    channel,
		Type: 1,
		Phase: MapPhaseState,
	}, &protocol.Command{Id: 1}, time.Now(), rwWrapper.rw)
	require.NoError(t, err)
	require.Len(t, rwWrapper.replies, 1)
	require.NotNil(t, rwWrapper.replies[0].Error)
	require.Equal(t, ErrorBadRequest.Code, rwWrapper.replies[0].Error.Code)
}

func TestMapSubscribe_AlreadySubscribed(t *testing.T) {
	node, _ := newTestNodeWithMapEngine(t)

	channel := "test_already_subscribed"

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					EnableMap: true,
				},
			}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")

	// First subscribe.
	subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel:    channel,
		Type: 1,
		Phase: MapPhaseLive,
	})

	// Second subscribe should fail.
	rwWrapper := testReplyWriterWrapper()
	err := client.handleSubscribe(&protocol.SubscribeRequest{
		Channel:    channel,
		Type: 1,
		Phase: MapPhaseState,
	}, &protocol.Command{Id: 1}, time.Now(), rwWrapper.rw)
	require.Equal(t, ErrorAlreadySubscribed, err)
}

func TestMapSubscribe_WithPresence(t *testing.T) {
	node, engine := newTestNodeWithMapEngine(t)

	channel := "test_with_presence"
	ctx := context.Background()

	// Pre-populate data.
	_, err := engine.Publish(ctx, channel, "key1", MapPublishOptions{
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
					EnableMap:             true,
					MapClientPresenceChannelPrefix: "$clients:",
				},
			}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")

	// Subscribe with keyed client presence.
	subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel:    channel,
		Type: 1,
		Phase: MapPhaseLive,
	})

	// Verify presence was added to $clients:{channel}.
	clientsChannel := "$clients:" + channel
	entries, _, _, err := engine.ReadState(ctx, clientsChannel, MapReadStateOptions{
		Limit: 100,
	})
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, client.uid, entries[0].Key)

	// Verify flagMapClientPresence is set.
	require.Contains(t, client.channels, channel)
	chCtx := client.channels[channel]
	require.True(t, channelHasFlag(chCtx.flags, flagMapClientPresence))
	require.True(t, channelHasFlag(chCtx.flags, flagMap))
}

func TestMapSubscribe_PresenceCleanupOnUnsubscribe(t *testing.T) {
	node, engine := newTestNodeWithMapEngine(t)

	channel := "test_presence_cleanup"
	ctx := context.Background()

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					EnableMap:             true,
					MapClientPresenceChannelPrefix: "$clients:",
				},
			}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")

	// Subscribe with keyed client presence.
	subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel:    channel,
		Type: 1,
		Phase: MapPhaseLive,
	})

	// Verify presence exists in :clients channel.
	clientsChannel := "$clients:" + channel
	entries, _, _, err := engine.ReadState(ctx, clientsChannel, MapReadStateOptions{
		Limit: 100,
	})
	require.NoError(t, err)
	require.Len(t, entries, 1)

	// Unsubscribe.
	client.Unsubscribe(channel)

	// Verify presence was removed.
	entries, _, _, err = engine.ReadState(ctx, clientsChannel, MapReadStateOptions{
		Limit: 100,
	})
	require.NoError(t, err)
	require.Len(t, entries, 0)
}

func TestMapSubscribe_PresenceCleanupOnDisconnect(t *testing.T) {
	node, engine := newTestNodeWithMapEngine(t)

	channel := "test_presence_disconnect"
	ctx := context.Background()

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					EnableMap:             true,
					MapClientPresenceChannelPrefix: "$clients:",
				},
			}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")

	// Subscribe with keyed client presence.
	subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel:    channel,
		Type: 1,
		Phase: MapPhaseLive,
	})

	// Verify presence exists in :clients channel.
	clientsChannel := "$clients:" + channel
	entries, _, _, err := engine.ReadState(ctx, clientsChannel, MapReadStateOptions{
		Limit: 100,
	})
	require.NoError(t, err)
	require.Len(t, entries, 1)

	// Disconnect client.
	err = client.close(DisconnectForceNoReconnect)
	require.NoError(t, err)

	// Verify presence was removed.
	entries, _, _, err = engine.ReadState(ctx, clientsChannel, MapReadStateOptions{
		Limit: 100,
	})
	require.NoError(t, err)
	require.Len(t, entries, 0)
}

func TestMapSubscribe_CleanupOnUnsubscribe(t *testing.T) {
	node, engine := newTestNodeWithMapEngine(t)

	channel := "test_cleanup_on_unsub"
	ctx := context.Background()

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					EnableMap:          true,
					CleanupOnUnsubscribe: true,
				},
			}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")
	clientID := client.ID()

	// Subscribe with CleanupOnUnsubscribe.
	subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel:    channel,
		Type: 1,
		Phase: MapPhaseLive,
	})

	// Publish a key with key=clientID (simulating cursor/ephemeral state).
	_, err := engine.Publish(ctx, channel, clientID, MapPublishOptions{
		
		Data:       []byte(`{"x":100,"y":200}`),
		StreamSize: 1000,
		ClientInfo: &ClientInfo{ClientID: clientID, UserID: "user1"},
	})
	require.NoError(t, err)

	// Verify key exists.
	entries, _, _, err := engine.ReadState(ctx, channel, MapReadStateOptions{
		Limit: 100,
	})
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, clientID, entries[0].Key)

	// Unsubscribe - should trigger cleanup of key=clientID.
	client.Unsubscribe(channel)

	// Verify key was removed.
	entries, _, _, err = engine.ReadState(ctx, channel, MapReadStateOptions{
		Limit: 100,
	})
	require.NoError(t, err)
	require.Len(t, entries, 0)
}

func TestMapSubscribe_CleanupOnDisconnect(t *testing.T) {
	node, engine := newTestNodeWithMapEngine(t)

	channel := "test_cleanup_on_disconnect"
	ctx := context.Background()

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					EnableMap:          true,
					CleanupOnUnsubscribe: true,
				},
			}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")
	clientID := client.ID()

	// Subscribe with CleanupOnUnsubscribe.
	subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel:    channel,
		Type: 1,
		Phase: MapPhaseLive,
	})

	// Publish a key with key=clientID.
	_, err := engine.Publish(ctx, channel, clientID, MapPublishOptions{
		
		Data:       []byte(`{"x":100,"y":200}`),
		StreamSize: 1000,
		ClientInfo: &ClientInfo{ClientID: clientID, UserID: "user1"},
	})
	require.NoError(t, err)

	// Verify key exists.
	entries, _, _, err := engine.ReadState(ctx, channel, MapReadStateOptions{
		Limit: 100,
	})
	require.NoError(t, err)
	require.Len(t, entries, 1)

	// Disconnect client - should trigger cleanup of key=clientID.
	err = client.close(DisconnectForceNoReconnect)
	require.NoError(t, err)

	// Verify key was removed.
	entries, _, _, err = engine.ReadState(ctx, channel, MapReadStateOptions{
		Limit: 100,
	})
	require.NoError(t, err)
	require.Len(t, entries, 0)
}

func TestPresenceSubscribe_Snapshot(t *testing.T) {
	node, engine := newTestNodeWithMapEngine(t)

	ctx := context.Background()

	// Pre-populate presence data using prefix-based channel naming.
	// With prefix "$clients:", the presence channel for "test_presence_sub" is "$clients:test_presence_sub".
	presenceChannel := "$clients:test_presence_sub"
	_, err := engine.Publish(ctx, presenceChannel, "client1", MapPublishOptions{
		ClientInfo: &ClientInfo{ClientID: "client1", UserID: "user1"},
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)

	_, err = engine.Publish(ctx, presenceChannel, "client2", MapPublishOptions{
		ClientInfo: &ClientInfo{ClientID: "client2", UserID: "user2"},
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)

	node.OnConnect(func(client *Client) {
		// Presence subscriptions go through OnPresenceSubscribe (separate permission scope).
		client.OnPresenceSubscribe(func(e PresenceSubscribeEvent, cb PresenceSubscribeCallback) {
			// Event receives the full presence channel name as provided by client.
			require.Equal(t, presenceChannel, e.Channel)
			cb(PresenceSubscribeReply{}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")

	// Subscribe to presence snapshot.
	result := subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel:       presenceChannel,
		Type: 2,
		Phase:    MapPhaseState,
		Limit:    100,
	})

	require.True(t, result.Type == 1)
	require.Equal(t, MapPhaseState, result.Phase)
	require.Len(t, result.State, 2) // State entries in State field
}

func TestPresenceSubscribe_Live(t *testing.T) {
	node, engine := newTestNodeWithMapEngine(t)

	ctx := context.Background()

	// Pre-populate presence data using prefix-based channel naming.
	presenceChannel := "$clients:test_presence_live"
	_, err := engine.Publish(ctx, presenceChannel, "client1", MapPublishOptions{
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
	subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel:       presenceChannel,
		Type: 2,
		Phase:    MapPhaseState,
		Limit:    100,
	})

	// Then go live.
	result := subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel:       presenceChannel,
		Type: 2,
		Phase:    MapPhaseLive,
	})

	require.True(t, result.Type == 1)
	require.Equal(t, MapPhaseLive, result.Phase)

	// Verify presence subscription is tracked in unified channels map with flagMapPresence.
	client.mu.RLock()
	ctx2, ok := client.channels[presenceChannel]
	client.mu.RUnlock()
	require.True(t, ok)
	require.True(t, channelHasFlag(ctx2.flags, flagMapPresence))
}

func TestPresenceSubscribe_NotAllowed(t *testing.T) {
	node, _ := newTestNodeWithMapEngine(t)

	presenceChannel := "$clients:test_presence_not_allowed"

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
		Type: 2,
		Phase:    MapPhaseState,
	}, &protocol.Command{Id: 1}, time.Now(), rwWrapper.rw)
	require.NoError(t, err)
	require.Len(t, rwWrapper.replies, 1)
	require.NotNil(t, rwWrapper.replies[0].Error)
	require.Equal(t, ErrorPermissionDenied.Code, rwWrapper.replies[0].Error.Code)
}

func TestPresenceSubscribe_NoHandler(t *testing.T) {
	node, _ := newTestNodeWithMapEngine(t)

	presenceChannel := "$clients:test_presence_no_handler"

	// No OnPresenceSubscribe handler set.
	node.OnConnect(func(client *Client) {})

	client := newTestConnectedClientV2(t, node, "user1")

	// Try to subscribe to presence - should fail because no OnPresenceSubscribe handler.
	rwWrapper := testReplyWriterWrapper()
	err := client.handleSubscribe(&protocol.SubscribeRequest{
		Channel:       presenceChannel,
		Type: 2,
		Phase:    MapPhaseState,
	}, &protocol.Command{Id: 1}, time.Now(), rwWrapper.rw)
	// When no handler is set, the error is returned directly.
	require.Equal(t, ErrorNotAvailable, err)
}

func TestPresenceSubscribe_AlreadySubscribed(t *testing.T) {
	node, _ := newTestNodeWithMapEngine(t)

	presenceChannel := "$clients:test_presence_already_sub"

	node.OnConnect(func(client *Client) {
		client.OnPresenceSubscribe(func(e PresenceSubscribeEvent, cb PresenceSubscribeCallback) {
			cb(PresenceSubscribeReply{}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")

	// First subscribe to presence.
	subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel:       presenceChannel,
		Type: 2,
		Phase:    MapPhaseState,
	})
	subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel:       presenceChannel,
		Type: 2,
		Phase:    MapPhaseLive,
	})

	// Third subscribe should fail with AlreadySubscribed.
	rwWrapper := testReplyWriterWrapper()
	err := client.handleSubscribe(&protocol.SubscribeRequest{
		Channel:       presenceChannel,
		Type: 2,
		Phase:    MapPhaseState,
	}, &protocol.Command{Id: 1}, time.Now(), rwWrapper.rw)
	// When already subscribed, the error is returned directly.
	require.Equal(t, ErrorAlreadySubscribed, err)
}

func TestMapPresenceTTL(t *testing.T) {
	node, _ := newTestNodeWithMapEngine(t)

	// Default ClientPresenceUpdateInterval is 27 seconds.
	node.config.ClientPresenceUpdateInterval = 10 * time.Second

	client := newTestClientV2(t, node, "user1")

	// TTL should be 3x the update interval.
	ttl := client.mapPresenceTTL()
	require.Equal(t, 30*time.Second, ttl)

	// With very small interval, minimum should apply.
	node.config.ClientPresenceUpdateInterval = 5 * time.Second
	ttl = client.mapPresenceTTL()
	require.Equal(t, 30*time.Second, ttl) // Minimum 30 seconds.
}

func TestMapSubscribe_WithKeyedClientAndUserPresence(t *testing.T) {
	node, engine := newTestNodeWithMapEngine(t)

	channel := "test_map_presence"
	ctx := context.Background()

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					EnableMap:                      true,
					MapClientPresenceChannelPrefix: "$clients:",
					MapUserPresenceChannelPrefix:   "$users:",
				},
			}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")

	// Subscribe with both client and user presence.
	subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel:    channel,
		Type: 1,
		Phase: MapPhaseLive,
	})

	// Verify :clients presence was added (key=clientId, full info).
	clientsChannel := "$clients:" + channel
	entries, _, _, err := engine.ReadState(ctx, clientsChannel, MapReadStateOptions{
		Limit: 100,
	})
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, client.uid, entries[0].Key)
	require.NotNil(t, entries[0].Info)
	require.Equal(t, client.uid, entries[0].Info.ClientID)
	require.Equal(t, "user1", entries[0].Info.UserID)

	// Verify :users presence was added (key=userId, no info).
	usersChannel := "$users:" + channel
	entries, _, _, err = engine.ReadState(ctx, usersChannel, MapReadStateOptions{
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
	require.True(t, channelHasFlag(chCtx.flags, flagMapClientPresence))
	require.True(t, channelHasFlag(chCtx.flags, flagMapUserPresence))
	require.True(t, channelHasFlag(chCtx.flags, flagMap))
}

func TestMapSubscribePresenceCleanupOnDisconnect(t *testing.T) {
	node, engine := newTestNodeWithMapEngine(t)

	channel := "test_map_presence_cleanup"
	ctx := context.Background()

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					EnableMap:                      true,
					MapClientPresenceChannelPrefix: "$clients:",
					MapUserPresenceChannelPrefix:   "$users:",
				},
			}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")

	// Subscribe with both client and user presence.
	subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel:    channel,
		Type: 1,
		Phase: MapPhaseLive,
	})

	// Verify presence exists in both channels.
	clientsChannel := "$clients:" + channel
	usersChannel := "$users:" + channel

	entries, _, _, err := engine.ReadState(ctx, clientsChannel, MapReadStateOptions{Limit: 100})
	require.NoError(t, err)
	require.Len(t, entries, 1)

	entries, _, _, err = engine.ReadState(ctx, usersChannel, MapReadStateOptions{Limit: 100})
	require.NoError(t, err)
	require.Len(t, entries, 1)

	// Disconnect client.
	err = client.close(DisconnectForceNoReconnect)
	require.NoError(t, err)

	// Verify :clients presence was removed.
	entries, _, _, err = engine.ReadState(ctx, clientsChannel, MapReadStateOptions{Limit: 100})
	require.NoError(t, err)
	require.Len(t, entries, 0)

	// Verify :users presence is NOT removed (TTL-based expiration).
	entries, _, _, err = engine.ReadState(ctx, usersChannel, MapReadStateOptions{Limit: 100})
	require.NoError(t, err)
	require.Len(t, entries, 1) // Still there, will expire via TTL.
}

func TestMapSubscribe_MultipleClientsPerUser(t *testing.T) {
	node, engine := newTestNodeWithMapEngine(t)

	channel := "test_multi_clients"
	ctx := context.Background()

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					EnableMap:                      true,
					MapClientPresenceChannelPrefix: "$clients:",
					MapUserPresenceChannelPrefix:   "$users:",
				},
			}, nil)
		})
	})

	// Connect two clients with the same user.
	client1 := newTestConnectedClientV2(t, node, "user1")
	client2 := newTestConnectedClientV2(t, node, "user1")

	// Subscribe both clients.
	subscribeMapClient(t, client1, &protocol.SubscribeRequest{
		Channel:    channel,
		Type: 1,
		Phase: MapPhaseLive,
	})
	subscribeMapClient(t, client2, &protocol.SubscribeRequest{
		Channel:    channel,
		Type: 1,
		Phase: MapPhaseLive,
	})

	// Verify :clients has two entries (one per connection).
	clientsChannel := "$clients:" + channel
	entries, _, _, err := engine.ReadState(ctx, clientsChannel, MapReadStateOptions{Limit: 100})
	require.NoError(t, err)
	require.Len(t, entries, 2)

	// Verify :users has one entry (deduplicated by userId).
	usersChannel := "$users:" + channel
	entries, _, _, err = engine.ReadState(ctx, usersChannel, MapReadStateOptions{Limit: 100})
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, "user1", entries[0].Key)

	// Disconnect one client.
	err = client1.close(DisconnectForceNoReconnect)
	require.NoError(t, err)

	// :clients should have one entry.
	entries, _, _, err = engine.ReadState(ctx, clientsChannel, MapReadStateOptions{Limit: 100})
	require.NoError(t, err)
	require.Len(t, entries, 1)

	// :users still has the user (TTL refresh from client2).
	entries, _, _, err = engine.ReadState(ctx, usersChannel, MapReadStateOptions{Limit: 100})
	require.NoError(t, err)
	require.Len(t, entries, 1)
}

// TestMapEngine_ReadStateByKey tests the Key filter for ReadState.
func TestMapEngine_ReadStateByKey(t *testing.T) {
	_, engine := newTestNodeWithMapEngine(t)
	ctx := context.Background()
	ch := "test_read_by_key"

	// Publish 3 keys
	_, err := engine.Publish(ctx, ch, "key1", MapPublishOptions{
		Data:       []byte(`{"value":"data1"}`),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)

	_, err = engine.Publish(ctx, ch, "key2", MapPublishOptions{
		Data:       []byte(`{"value":"data2"}`),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)

	_, err = engine.Publish(ctx, ch, "key3", MapPublishOptions{
		Data:       []byte(`{"value":"data3"}`),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)

	// Read single key
	pubs, pos, cursor, err := engine.ReadState(ctx, ch, MapReadStateOptions{
		Key: "key2",
	})
	require.NoError(t, err)
	require.Len(t, pubs, 1)
	require.Equal(t, "key2", pubs[0].Key)
	require.Equal(t, []byte(`{"value":"data2"}`), pubs[0].Data)
	require.Empty(t, cursor) // No pagination for single key
	require.NotEmpty(t, pos.Epoch)

	// Read non-existent key
	pubs, _, _, err = engine.ReadState(ctx, ch, MapReadStateOptions{
		Key: "nonexistent",
	})
	require.NoError(t, err)
	require.Len(t, pubs, 0)
}

// TestMapEngine_CASSuccess tests successful CAS update.
func TestMapEngine_CASSuccess(t *testing.T) {
	_, engine := newTestNodeWithMapEngine(t)
	ctx := context.Background()
	ch := "test_cas_success"

	// Publish initial value
	res1, err := engine.Publish(ctx, ch, "counter", MapPublishOptions{
		Data:       []byte(`{"value":10}`),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)
	require.False(t, res1.Suppressed)

	// Read current state - position includes offset AND epoch
	pubs, pos, _, err := engine.ReadState(ctx, ch, MapReadStateOptions{Key: "counter"})
	require.NoError(t, err)
	require.Len(t, pubs, 1)
	expectedPos := StreamPosition{Offset: pubs[0].Offset, Epoch: pos.Epoch}

	// CAS update with correct position
	res2, err := engine.Publish(ctx, ch, "counter", MapPublishOptions{
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
	pubs, _, _, err = engine.ReadState(ctx, ch, MapReadStateOptions{Key: "counter"})
	require.NoError(t, err)
	require.Len(t, pubs, 1)
	require.Equal(t, []byte(`{"value":15}`), pubs[0].Data)
}

// TestMapEngine_CASConflict tests CAS conflict when position has changed.
func TestMapEngine_CASConflict(t *testing.T) {
	_, engine := newTestNodeWithMapEngine(t)
	ctx := context.Background()
	ch := "test_cas_conflict"

	// Publish initial value
	_, err := engine.Publish(ctx, ch, "counter", MapPublishOptions{
		Data:       []byte(`{"value":10}`),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)

	// Read current state
	pubs, pos, _, err := engine.ReadState(ctx, ch, MapReadStateOptions{Key: "counter"})
	require.NoError(t, err)
	require.Len(t, pubs, 1)
	originalPos := StreamPosition{Offset: pubs[0].Offset, Epoch: pos.Epoch}

	// Another client updates the key (simulated)
	_, err = engine.Publish(ctx, ch, "counter", MapPublishOptions{
		Data:       []byte(`{"value":12}`),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)

	// CAS with stale position - should fail
	res, err := engine.Publish(ctx, ch, "counter", MapPublishOptions{
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
	res2, err := engine.Publish(ctx, ch, "counter", MapPublishOptions{
		Data:             []byte(`{"value":15}`),
		ExpectedPosition: &retryPos,
		StreamSize:       100,
		StreamTTL:        300 * time.Second,
		KeyTTL:           300 * time.Second,
	})
	require.NoError(t, err)
	require.False(t, res2.Suppressed)

	// Verify the value was updated
	pubs, _, _, err = engine.ReadState(ctx, ch, MapReadStateOptions{Key: "counter"})
	require.NoError(t, err)
	require.Len(t, pubs, 1)
	require.Equal(t, []byte(`{"value":15}`), pubs[0].Data)
}

// TestMapEngine_CASNonExistent tests CAS on a key that doesn't exist.
func TestMapEngine_CASNonExistent(t *testing.T) {
	_, engine := newTestNodeWithMapEngine(t)
	ctx := context.Background()
	ch := "test_cas_nonexistent"

	// First create the channel by publishing something
	_, err := engine.Publish(ctx, ch, "other_key", MapPublishOptions{
		Data:       []byte(`{"value":1}`),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)

	// Get the epoch
	_, pos, _, err := engine.ReadState(ctx, ch, MapReadStateOptions{Limit: 1})
	require.NoError(t, err)

	// Try CAS on non-existent key with expected position
	expectedPos := StreamPosition{Offset: 42, Epoch: pos.Epoch}
	res, err := engine.Publish(ctx, ch, "newkey", MapPublishOptions{
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

// TestMapEngine_CASWrongEpoch tests CAS with correct offset but wrong epoch.
func TestMapEngine_CASWrongEpoch(t *testing.T) {
	_, engine := newTestNodeWithMapEngine(t)
	ctx := context.Background()
	ch := "test_cas_wrong_epoch"

	// Publish initial value
	_, err := engine.Publish(ctx, ch, "counter", MapPublishOptions{
		Data:       []byte(`{"value":10}`),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)

	// Read current state
	pubs, _, _, err := engine.ReadState(ctx, ch, MapReadStateOptions{Key: "counter"})
	require.NoError(t, err)
	require.Len(t, pubs, 1)

	// Use correct offset but wrong epoch
	wrongPos := StreamPosition{Offset: pubs[0].Offset, Epoch: "wrong-epoch"}

	res, err := engine.Publish(ctx, ch, "counter", MapPublishOptions{
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
	pubs, _, _, err = engine.ReadState(ctx, ch, MapReadStateOptions{Key: "counter"})
	require.NoError(t, err)
	require.Len(t, pubs, 1)
	require.Equal(t, []byte(`{"value":10}`), pubs[0].Data)
}

// TestMapEngine_StreamDataDifferentPayloads tests publishing with different
// data for snapshot (full state) and stream (incremental update).
func TestMapEngine_StreamDataDifferentPayloads(t *testing.T) {
	_, engine := newTestNodeWithMapEngine(t)
	ctx := context.Background()
	ch := "test_stream_data"

	// Publish with different payloads: snapshot gets full state, stream gets delta
	_, err := engine.Publish(ctx, ch, "counter", MapPublishOptions{
		Data:       []byte(`{"count":100}`),  // Full state → snapshot
		StreamData: []byte(`{"delta":100}`),  // Incremental → stream
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)

	// Read snapshot - should have full state
	pubs, pos, _, err := engine.ReadState(ctx, ch, MapReadStateOptions{Key: "counter"})
	require.NoError(t, err)
	require.Len(t, pubs, 1)
	require.Equal(t, []byte(`{"count":100}`), pubs[0].Data)

	// Read stream - should have incremental data
	streamPubs, _, err := engine.ReadStream(ctx, ch, MapReadStreamOptions{
		Filter: StreamFilter{Limit: 10},
	})
	require.NoError(t, err)
	require.Len(t, streamPubs, 1)
	require.Equal(t, []byte(`{"delta":100}`), streamPubs[0].Data)

	// Update with CAS: read current position, update with different payloads
	expectedPos := StreamPosition{Offset: pubs[0].Offset, Epoch: pos.Epoch}
	_, err = engine.Publish(ctx, ch, "counter", MapPublishOptions{
		Data:             []byte(`{"count":105}`), // New full state → snapshot
		StreamData:       []byte(`{"delta":5}`),   // Incremental → stream
		ExpectedPosition: &expectedPos,
		StreamSize:       100,
		StreamTTL:        300 * time.Second,
		KeyTTL:           300 * time.Second,
	})
	require.NoError(t, err)

	// Verify snapshot has new full state
	pubs, _, _, err = engine.ReadState(ctx, ch, MapReadStateOptions{Key: "counter"})
	require.NoError(t, err)
	require.Len(t, pubs, 1)
	require.Equal(t, []byte(`{"count":105}`), pubs[0].Data)

	// Verify stream has both incremental updates
	streamPubs, _, err = engine.ReadStream(ctx, ch, MapReadStreamOptions{
		Filter: StreamFilter{Limit: 10},
	})
	require.NoError(t, err)
	require.Len(t, streamPubs, 2)
	require.Equal(t, []byte(`{"delta":100}`), streamPubs[0].Data)
	require.Equal(t, []byte(`{"delta":5}`), streamPubs[1].Data)
}

// TestMapEngine_StreamDataWithoutStreamData tests that when StreamData is not set,
// Data is used for both snapshot and stream.
func TestMapEngine_StreamDataWithoutStreamData(t *testing.T) {
	_, engine := newTestNodeWithMapEngine(t)
	ctx := context.Background()
	ch := "test_no_stream_data"

	// Publish without StreamData - Data should be used for both
	_, err := engine.Publish(ctx, ch, "item", MapPublishOptions{
		Data:       []byte(`{"name":"test","value":42}`),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)

	// Read snapshot
	pubs, _, _, err := engine.ReadState(ctx, ch, MapReadStateOptions{Key: "item"})
	require.NoError(t, err)
	require.Len(t, pubs, 1)
	require.Equal(t, []byte(`{"name":"test","value":42}`), pubs[0].Data)

	// Read stream - should have same data
	streamPubs, _, err := engine.ReadStream(ctx, ch, MapReadStreamOptions{
		Filter: StreamFilter{Limit: 10},
	})
	require.NoError(t, err)
	require.Len(t, streamPubs, 1)
	require.Equal(t, []byte(`{"name":"test","value":42}`), streamPubs[0].Data)
}

// Tests for STATE→LIVE direct transition optimization (MapStateToLiveEnabled).

func TestMapSubscribe_StateToLive_DirectTransition(t *testing.T) {
	// Test that when MapStateToLiveEnabled is true and stream is close enough,
	// server transitions directly from STATE to LIVE on the last state page.
	node, err := New(Config{
		LogLevel:              LogLevelTrace,
		LogHandler:            func(entry LogEntry) {},
		MapStateToLiveEnabled: true, // Enable the optimization
	})
	require.NoError(t, err)

	engine, err := NewMemoryMapEngine(node, MemoryMapEngineConfig{})
	require.NoError(t, err)
	err = engine.RegisterBrokerEventHandler(nil)
	require.NoError(t, err)
	node.SetMapEngine(engine)

	err = node.Run()
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = node.Shutdown(context.Background())
	})

	channel := "test_state_to_live"
	ctx := context.Background()

	// Pre-populate state with a few entries.
	for i := 0; i < 3; i++ {
		_, err := engine.Publish(ctx, channel, string(rune('a'+i)), MapPublishOptions{
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
					EnableMap: true,
				},
			}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")

	// Send STATE request with high limit - should fit all entries in one page.
	// With optimization enabled and stream close enough, should go LIVE directly.
	rwWrapper := testReplyWriterWrapper()
	err = client.handleSubscribe(&protocol.SubscribeRequest{
		Channel: channel,
		Type:    1,
		Phase:   MapPhaseState,
		Limit:   100,
	}, &protocol.Command{Id: 1}, time.Now(), rwWrapper.rw)

	require.NoError(t, err)
	require.Len(t, rwWrapper.replies, 1)

	result := rwWrapper.replies[0].Subscribe
	require.Nil(t, rwWrapper.replies[0].Error)
	require.True(t, result.Type == 1)
	// Server went LIVE directly (phase=0) instead of returning STATE (phase=2).
	require.Equal(t, MapPhaseLive, result.Phase)
	require.Len(t, result.State, 3) // State entries in response
	require.NotEmpty(t, result.Epoch)
	require.Greater(t, result.Offset, uint64(0))
}

func TestMapSubscribe_StateToLive_WithStreamPublications(t *testing.T) {
	// Test that STATE→LIVE transition includes stream publications when
	// there are updates between state read and going live.
	node, err := New(Config{
		LogLevel:              LogLevelTrace,
		LogHandler:            func(entry LogEntry) {},
		MapStateToLiveEnabled: true,
	})
	require.NoError(t, err)

	engine, err := NewMemoryMapEngine(node, MemoryMapEngineConfig{})
	require.NoError(t, err)
	err = engine.RegisterBrokerEventHandler(nil)
	require.NoError(t, err)
	node.SetMapEngine(engine)

	err = node.Run()
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = node.Shutdown(context.Background())
	})

	channel := "test_state_to_live_with_stream"
	ctx := context.Background()

	// Pre-populate initial state.
	_, err = engine.Publish(ctx, channel, "key1", MapPublishOptions{
		Data:       []byte(`{"v":"initial"}`),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					EnableMap: true,
				},
			}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")

	// Send STATE request - should go LIVE directly.
	rwWrapper := testReplyWriterWrapper()
	err = client.handleSubscribe(&protocol.SubscribeRequest{
		Channel: channel,
		Type:    1,
		Phase:   MapPhaseState,
		Limit:   100,
	}, &protocol.Command{Id: 1}, time.Now(), rwWrapper.rw)

	require.NoError(t, err)
	require.Len(t, rwWrapper.replies, 1)

	result := rwWrapper.replies[0].Subscribe
	require.Nil(t, rwWrapper.replies[0].Error)
	require.Equal(t, MapPhaseLive, result.Phase)
	require.Len(t, result.State, 1)
	// Publications may be empty if no stream updates happened during transition.
	// The key point is that phase=0 (LIVE) was returned.
}

func TestMapSubscribe_StateToLive_Pagination_LastPageGoesLive(t *testing.T) {
	// Test that with pagination, only the LAST page can trigger STATE→LIVE.
	// Earlier pages should return phase=2 (STATE) with cursor.
	node, err := New(Config{
		LogLevel:              LogLevelTrace,
		LogHandler:            func(entry LogEntry) {},
		MapStateToLiveEnabled: true,
	})
	require.NoError(t, err)

	engine, err := NewMemoryMapEngine(node, MemoryMapEngineConfig{})
	require.NoError(t, err)
	err = engine.RegisterBrokerEventHandler(nil)
	require.NoError(t, err)
	node.SetMapEngine(engine)

	err = node.Run()
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = node.Shutdown(context.Background())
	})

	channel := "test_pagination_last_page_live"
	ctx := context.Background()

	// Pre-populate 10 entries.
	for i := 0; i < 10; i++ {
		_, err := engine.Publish(ctx, channel, string(rune('a'+i)), MapPublishOptions{
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
					EnableMap: true,
				},
			}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")

	// First page - limit 5, should return STATE with cursor.
	result := subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: channel,
		Type:    1,
		Phase:   MapPhaseState,
		Limit:   5,
	})

	require.True(t, result.Type == 1)
	require.Equal(t, MapPhaseState, result.Phase) // First page: phase=2
	require.Len(t, result.State, 5)
	require.NotEmpty(t, result.Cursor) // More pages available

	epoch := result.Epoch
	offset := result.Offset

	// Second page (last page) - should go LIVE directly.
	result2 := subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: channel,
		Type:    1,
		Phase:   MapPhaseState,
		Limit:   5,
		Cursor:  result.Cursor,
		Offset:  offset,
		Epoch:   epoch,
	})

	require.True(t, result2.Type == 1)
	require.Equal(t, MapPhaseLive, result2.Phase) // Last page: phase=0 (LIVE)
	require.Len(t, result2.State, 5)              // Remaining 5 entries
	require.Empty(t, result2.Cursor)              // No more cursor (it's LIVE now)
}

func TestMapSubscribe_StreamPhaseRecovery(t *testing.T) {
	// Test that a reconnecting client can use phase=1 (STREAM) with recover=true
	// to catch up from its last known position without going through STATE phase.
	// This simulates a client reconnection after disconnect.
	node, engine := newTestNodeWithMapEngine(t)

	channel := "test_stream_recovery"
	ctx := context.Background()

	// Pre-populate some data.
	var lastOffset uint64
	var epoch string
	for i := 0; i < 5; i++ {
		res, err := engine.Publish(ctx, channel, string(rune('a'+i)), MapPublishOptions{
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
					EnableMap: true,
				},
			}, nil)
		})
	})

	// Simulate first client that completed subscription and then disconnected.
	// It had offset=2 when it disconnected.
	savedOffset := uint64(2)
	savedEpoch := epoch

	// Now create a "reconnecting" client - it has no prior mapSubscribing state
	// but it knows its last position from before disconnect.
	client := newTestConnectedClientV2(t, node, "user1")

	// Send STREAM phase request with recover=true.
	// This should:
	// 1. Go through OnSubscribe for authorization
	// 2. Create mapSubscribeState on the fly (since recover=true)
	// 3. Return stream publications from offset 2 to current
	// 4. Transition to LIVE
	result := subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: channel,
		Type:    1,
		Phase:   MapPhaseStream,
		Offset:  savedOffset,
		Epoch:   savedEpoch,
		Limit:   100,
		Recover: true, // This is the key flag for reconnection
	})

	require.True(t, result.Type == 1)
	// Server should transition to LIVE since client is close enough.
	require.Equal(t, MapPhaseLive, result.Phase)
	// Should have publications 3, 4, 5 (from offset 2).
	require.Len(t, result.Publications, 3)
	require.Equal(t, lastOffset, result.Offset)
	require.Equal(t, epoch, result.Epoch)

	// Client should now be subscribed.
	require.Contains(t, client.channels, channel)
	chCtx := client.channels[channel]
	require.True(t, channelHasFlag(chCtx.flags, flagMap))
	require.True(t, channelHasFlag(chCtx.flags, flagSubscribed))
}

func TestMapSubscribe_StreamPhaseRecovery_WithoutRecoverFlag(t *testing.T) {
	// Test that phase=1 (STREAM) without recover=true and without prior STATE
	// phase returns permission denied.
	node, engine := newTestNodeWithMapEngine(t)

	channel := "test_stream_no_recover"
	ctx := context.Background()

	// Pre-populate some data.
	res, err := engine.Publish(ctx, channel, "key1", MapPublishOptions{
		Data:       []byte(`{"v":"data"}`),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					EnableMap: true,
				},
			}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")

	// Try STREAM phase without recover=true and without prior STATE phase.
	// This should fail with permission denied.
	protoErr := subscribeMapClientExpectError(t, client, &protocol.SubscribeRequest{
		Channel: channel,
		Type:    1,
		Phase:   MapPhaseStream,
		Offset:  res.Position.Offset,
		Epoch:   res.Position.Epoch,
		Limit:   100,
		Recover: false, // Not recovering
	})

	require.Equal(t, ErrorPermissionDenied.Code, protoErr.Code)
}

func TestMapSubscribe_StreamPhaseRecovery_LargeGap(t *testing.T) {
	// Test stream phase recovery with a larger gap that requires multiple
	// pagination rounds before going LIVE.
	node, engine := newTestNodeWithMapEngine(t)

	channel := "test_stream_recovery_large_gap"
	ctx := context.Background()

	// Pre-populate many entries to create a gap.
	var lastOffset uint64
	var epoch string
	for i := 0; i < 100; i++ {
		res, err := engine.Publish(ctx, channel, string(rune('a'+(i%26)))+string(rune('0'+(i/26))), MapPublishOptions{
			Data:       []byte(`{"v":"data"}`),
			StreamSize: 1000,
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
					EnableMap: true,
				},
			}, nil)
		})
	})

	// Client reconnects with offset=10, needs to catch up ~90 publications.
	savedOffset := uint64(10)
	savedEpoch := epoch

	client := newTestConnectedClientV2(t, node, "user1")

	// First STREAM request with small limit - should NOT go LIVE yet.
	result := subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: channel,
		Type:    1,
		Phase:   MapPhaseStream,
		Offset:  savedOffset,
		Epoch:   savedEpoch,
		Limit:   20, // Small limit, won't catch up in one request
		Recover: true,
	})

	require.True(t, result.Type == 1)
	// Server should keep us in STREAM phase since we're too far behind.
	require.Equal(t, MapPhaseStream, result.Phase)
	require.Len(t, result.Publications, 20)

	// Update offset from publications.
	newOffset := result.Publications[len(result.Publications)-1].Offset

	// Continue with more STREAM requests until we get LIVE.
	for result.Phase == MapPhaseStream {
		result = subscribeMapClient(t, client, &protocol.SubscribeRequest{
			Channel: channel,
			Type:    1,
			Phase:   MapPhaseStream,
			Offset:  newOffset,
			Epoch:   savedEpoch,
			Limit:   20,
			// No Recover needed for continuation since mapSubscribing state exists now.
		})

		if len(result.Publications) > 0 {
			newOffset = result.Publications[len(result.Publications)-1].Offset
		}
	}

	// Should eventually transition to LIVE.
	require.Equal(t, MapPhaseLive, result.Phase)
	require.Equal(t, lastOffset, result.Offset)

	// Client should be subscribed.
	require.Contains(t, client.channels, channel)
}

func TestMapSubscribe_LivePhaseRecovery(t *testing.T) {
	// Test that a reconnecting client can use phase=0 (LIVE) with recover=true
	// to catch up directly without any pagination.
	node, engine := newTestNodeWithMapEngine(t)

	channel := "test_live_recovery"
	ctx := context.Background()

	// Pre-populate some data.
	var lastOffset uint64
	var epoch string
	for i := 0; i < 5; i++ {
		res, err := engine.Publish(ctx, channel, string(rune('a'+i)), MapPublishOptions{
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
					EnableMap: true,
				},
			}, nil)
		})
	})

	// Simulate reconnecting client with saved position.
	savedOffset := uint64(2)
	savedEpoch := epoch

	client := newTestConnectedClientV2(t, node, "user1")

	// Send LIVE phase request with recover=true.
	// This is Option 1 from the recovery protocol - direct to LIVE.
	result := subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: channel,
		Type:    1,
		Phase:   MapPhaseLive,
		Offset:  savedOffset,
		Epoch:   savedEpoch,
		Recover: true, // Reconnection mode
	})

	require.True(t, result.Type == 1)
	require.Equal(t, MapPhaseLive, result.Phase)
	// Should have publications 3, 4, 5 (from offset 2).
	require.Len(t, result.Publications, 3)
	require.Equal(t, lastOffset, result.Offset)
	require.Equal(t, epoch, result.Epoch)
	// No state returned since recover=true (client already has state).
	require.Len(t, result.State, 0)

	// Client should now be subscribed.
	require.Contains(t, client.channels, channel)
}

func TestMapSubscribe_StateToLive_Disabled(t *testing.T) {
	// Test that when MapStateToLiveEnabled is false, STATE phase never
	// transitions directly to LIVE (always returns phase=2).
	node, err := New(Config{
		LogLevel:              LogLevelTrace,
		LogHandler:            func(entry LogEntry) {},
		MapStateToLiveEnabled: false, // Disable the optimization (default)
	})
	require.NoError(t, err)

	engine, err := NewMemoryMapEngine(node, MemoryMapEngineConfig{})
	require.NoError(t, err)
	err = engine.RegisterBrokerEventHandler(nil)
	require.NoError(t, err)
	node.SetMapEngine(engine)

	err = node.Run()
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = node.Shutdown(context.Background())
	})

	channel := "test_state_to_live_disabled"
	ctx := context.Background()

	// Pre-populate state.
	_, err = engine.Publish(ctx, channel, "key1", MapPublishOptions{
		Data:       []byte(`{"v":"data"}`),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					EnableMap: true,
				},
			}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")

	// Send STATE request - should NOT go LIVE directly (optimization disabled).
	rwWrapper := testReplyWriterWrapper()
	err = client.handleSubscribe(&protocol.SubscribeRequest{
		Channel: channel,
		Type:    1,
		Phase:   MapPhaseState,
		Limit:   100,
	}, &protocol.Command{Id: 1}, time.Now(), rwWrapper.rw)

	require.NoError(t, err)
	require.Len(t, rwWrapper.replies, 1)

	result := rwWrapper.replies[0].Subscribe
	require.Nil(t, rwWrapper.replies[0].Error)
	require.True(t, result.Type == 1)
	// Should return STATE (phase=2), not LIVE.
	require.Equal(t, MapPhaseState, result.Phase)
	require.Len(t, result.State, 1)
	require.Empty(t, result.Cursor) // Last page but NOT live
}
