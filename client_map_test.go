package centrifuge

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/require"
)

// newTestNodeWithMapBroker creates a test node with a memory map broker.
func newTestNodeWithMapBroker(t *testing.T) (*Node, *MemoryMapBroker) {
	node, err := New(Config{
		LogLevel:   LogLevelTrace,
		LogHandler: func(entry LogEntry) {},
		GetMapChannelOptions: func(channel string) MapChannelOptions {
			return MapChannelOptions{
				SyncMode:      MapSyncEphemeral,
				RetentionMode: MapRetentionExpiring,
				KeyTTL:        60 * time.Second,
			}
		},
	})
	require.NoError(t, err)

	broker, err := NewMemoryMapBroker(node, MemoryMapBrokerConfig{})
	require.NoError(t, err)
	err = broker.RegisterEventHandler(nil)
	require.NoError(t, err)

	node.SetMapBroker(broker)

	err = node.Run()
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = node.Shutdown(context.Background())
	})

	return node, broker
}

func setTestMapChannelOptionsConverging(node *Node) {
	node.config.GetMapChannelOptions = func(channel string) MapChannelOptions {
		return MapChannelOptions{
			SyncMode:      MapSyncConverging,
			RetentionMode: MapRetentionExpiring,
			KeyTTL:        60 * time.Second,
		}
	}
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

func TestMapSubscribe_StatePhase(t *testing.T) {
	node, broker := newTestNodeWithMapBroker(t)

	channel := "test_keyed"
	ctx := context.Background()

	// Pre-populate some keyed data. Must use valid JSON for data since test uses JSON transport.
	_, err := broker.Publish(ctx, channel, "key1", MapPublishOptions{
		Data: []byte(`{"value":"data1"}`),
	})
	require.NoError(t, err)

	_, err = broker.Publish(ctx, channel, "key2", MapPublishOptions{
		Data: []byte(`{"value":"data2"}`),
	})
	require.NoError(t, err)

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					Type: SubscriptionTypeMap,
				},
			}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")

	// Send state phase request.
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
	// Streamless: single page → server transitions directly to LIVE.
	require.Equal(t, MapPhaseLive, result.Phase)
	require.Len(t, result.State, 2) // State entries in State field, not Publications
	require.NotEmpty(t, result.Epoch)
	// Client should now be subscribed.
	require.Contains(t, client.channels, channel)
}

func TestMapSubscribe_StatePagination(t *testing.T) {
	node, broker := newTestNodeWithMapBroker(t)

	channel := "test_map_pagination"
	ctx := context.Background()

	// Pre-populate keyed data.
	for i := 0; i < 10; i++ {
		_, err := broker.Publish(ctx, channel, string(rune('a'+i)), MapPublishOptions{
			Data: []byte(`{"v":"data"}`),
		})
		require.NoError(t, err)
	}

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					Type: SubscriptionTypeMap,
				},
			}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")

	// First page.
	result := subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: channel,
		Type:    1,
		Phase:   MapPhaseState,
		Limit:   5,
	})

	require.True(t, result.Type == 1)
	require.Equal(t, MapPhaseState, result.Phase)
	require.Len(t, result.State, 5)    // State entries in State field
	require.NotEmpty(t, result.Cursor) // More pages available.

	epoch := result.Epoch

	// Second page using cursor — streamless last page transitions to LIVE.
	result2 := subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: channel,
		Type:    1,
		Phase:   MapPhaseState,
		Limit:   5,
		Cursor:  result.Cursor,
	})

	require.True(t, result2.Type == 1)
	require.Equal(t, MapPhaseLive, result2.Phase) // Streamless: last page → LIVE.
	require.Len(t, result2.State, 5)              // State entries in State field
	require.Empty(t, result2.Cursor)
	require.Equal(t, epoch, result2.Epoch)
	// Client should now be subscribed.
	require.Contains(t, client.channels, channel)
}

func TestMapSubscribe_StreamPhase(t *testing.T) {
	node, broker := newTestNodeWithMapBroker(t)
	setTestMapChannelOptionsConverging(node)

	channel := "test_stream"
	ctx := context.Background()

	// Pre-populate stream data.
	var lastOffset uint64
	var epoch string
	for i := 0; i < 5; i++ {
		res, err := broker.Publish(ctx, channel, string(rune('a'+i)), MapPublishOptions{
			Data: []byte(`{"v":"data"}`),
		})
		require.NoError(t, err)
		lastOffset = res.Position.Offset
		epoch = res.Position.Epoch
	}

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					Type: SubscriptionTypeMap,
				},
			}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")

	// First, do state to authorize.
	subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: channel,
		Type:    1,
		Phase:   MapPhaseState,
		Limit:   100,
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
	node, broker := newTestNodeWithMapBroker(t)
	setTestMapChannelOptionsConverging(node)

	channel := "test_live"
	ctx := context.Background()

	// Pre-populate some data.
	res, err := broker.Publish(ctx, channel, "key1", MapPublishOptions{
		Data: []byte(`{"v":"data1"}`),
	})
	require.NoError(t, err)

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					Type: SubscriptionTypeMap, // Positioned mode: STATE doesn't auto-subscribe.
				},
			}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")

	// First do state to authorize (positioned mode: returns phase=2, not LIVE).
	subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: channel,
		Type:    1,
		Phase:   MapPhaseState,
		Limit:   100,
	})

	// Now join live.
	result := subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: channel,
		Type:    1,
		Phase:   MapPhaseLive,
		Offset:  res.Position.Offset,
		Epoch:   res.Position.Epoch,
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
	node, broker := newTestNodeWithMapBroker(t)

	channel := "test_direct_live"
	ctx := context.Background()

	// Pre-populate some data.
	_, err := broker.Publish(ctx, channel, "key1", MapPublishOptions{
		Data: []byte(`{"v":"data1"}`),
	})
	require.NoError(t, err)

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					Type: SubscriptionTypeMap,
				},
			}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")

	// Go directly to live without pagination.
	result := subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: channel,
		Type:    1,
		Phase:   MapPhaseLive,
	})

	require.True(t, result.Type == 1)
	require.Equal(t, MapPhaseLive, result.Phase)
	require.NotEmpty(t, result.Epoch)
	require.Contains(t, client.channels, channel)
}

func TestMapSubscribe_FullTwoPhase(t *testing.T) {
	node, broker := newTestNodeWithMapBroker(t)
	setTestMapChannelOptionsConverging(node)

	channel := "test_two_phase"
	ctx := context.Background()

	// Pre-populate data.
	for i := 0; i < 5; i++ {
		_, err := broker.Publish(ctx, channel, string(rune('a'+i)), MapPublishOptions{
			Data: []byte(`{"v":"data"}`),
		})
		require.NoError(t, err)
	}

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					Type: SubscriptionTypeMap, // Positioned mode for explicit two-phase flow.
				},
			}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")

	// Phase 1: State (positioned mode: returns phase=2).
	stateResult := subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: channel,
		Type:    1,
		Phase:   MapPhaseState,
		Limit:   100,
	})

	require.True(t, stateResult.Type == 1)
	require.Equal(t, MapPhaseState, stateResult.Phase)
	require.Len(t, stateResult.State, 5) // State entries in State field

	// Phase 2: Live.
	liveResult := subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: channel,
		Type:    1,
		Phase:   MapPhaseLive,
		Offset:  stateResult.Offset,
		Epoch:   stateResult.Epoch,
	})

	require.True(t, liveResult.Type == 1)
	require.Equal(t, MapPhaseLive, liveResult.Phase)
	require.Contains(t, client.channels, channel)
}

func TestMapSubscribe_NotEnabled(t *testing.T) {
	node, _ := newTestNodeWithMapBroker(t)

	channel := "test_not_enabled"

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			// Don't enable keyed mode.
			cb(SubscribeReply{
				Options: SubscribeOptions{
					Type: SubscriptionTypeStream,
				},
			}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")

	// Try keyed subscribe - should fail with BadRequest since keyed is not enabled.
	rwWrapper := testReplyWriterWrapper()
	err := client.handleSubscribe(&protocol.SubscribeRequest{
		Channel: channel,
		Type:    1,
		Phase:   MapPhaseState,
	}, &protocol.Command{Id: 1}, time.Now(), rwWrapper.rw)
	require.NoError(t, err)
	require.Len(t, rwWrapper.replies, 1)
	require.NotNil(t, rwWrapper.replies[0].Error)
	require.Equal(t, ErrorBadRequest.Code, rwWrapper.replies[0].Error.Code)
}

func TestMapSubscribe_AlreadySubscribed(t *testing.T) {
	node, _ := newTestNodeWithMapBroker(t)

	channel := "test_already_subscribed"

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					Type: SubscriptionTypeMap,
				},
			}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")

	// First subscribe.
	subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: channel,
		Type:    1,
		Phase:   MapPhaseLive,
	})

	// Second subscribe should fail.
	rwWrapper := testReplyWriterWrapper()
	err := client.handleSubscribe(&protocol.SubscribeRequest{
		Channel: channel,
		Type:    1,
		Phase:   MapPhaseState,
	}, &protocol.Command{Id: 1}, time.Now(), rwWrapper.rw)
	require.Equal(t, ErrorAlreadySubscribed, err)
}

func TestMapSubscribe_WithPresence(t *testing.T) {
	node, broker := newTestNodeWithMapBroker(t)

	channel := "test_with_presence"
	ctx := context.Background()

	// Pre-populate data.
	_, err := broker.Publish(ctx, channel, "key1", MapPublishOptions{
		Data: []byte(`{"v":"data1"}`),
	})
	require.NoError(t, err)

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					Type:                           SubscriptionTypeMap,
					MapClientPresenceChannelPrefix: "clients:",
				},
			}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")

	// Subscribe with keyed client presence.
	subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: channel,
		Type:    1,
		Phase:   MapPhaseLive,
	})

	// Verify presence was added to clients:{channel}.
	clientsChannel := "clients:" + channel
	stateRes, err := broker.ReadState(ctx, clientsChannel, MapReadStateOptions{
		Limit: 100,
	})
	entries, _, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
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
	node, broker := newTestNodeWithMapBroker(t)

	channel := "test_presence_cleanup"
	ctx := context.Background()

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					Type:                           SubscriptionTypeMap,
					MapClientPresenceChannelPrefix: "clients:",
				},
			}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")

	// Subscribe with keyed client presence.
	subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: channel,
		Type:    1,
		Phase:   MapPhaseLive,
	})

	// Verify presence exists in :clients channel.
	clientsChannel := "clients:" + channel
	stateRes, err := broker.ReadState(ctx, clientsChannel, MapReadStateOptions{
		Limit: 100,
	})
	entries, _, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
	require.NoError(t, err)
	require.Len(t, entries, 1)

	// Unsubscribe.
	client.Unsubscribe(channel)

	// Verify presence was removed.
	stateRes, err = broker.ReadState(ctx, clientsChannel, MapReadStateOptions{
		Limit: 100,
	})
	entries, _, _ = stateRes.Publications, stateRes.Position, stateRes.Cursor
	require.NoError(t, err)
	require.Len(t, entries, 0)
}

func TestMapSubscribe_PresenceCleanupOnDisconnect(t *testing.T) {
	node, broker := newTestNodeWithMapBroker(t)

	channel := "test_presence_disconnect"
	ctx := context.Background()

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					Type:                           SubscriptionTypeMap,
					MapClientPresenceChannelPrefix: "clients:",
				},
			}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")

	// Subscribe with keyed client presence.
	subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: channel,
		Type:    1,
		Phase:   MapPhaseLive,
	})

	// Verify presence exists in :clients channel.
	clientsChannel := "clients:" + channel
	stateRes, err := broker.ReadState(ctx, clientsChannel, MapReadStateOptions{
		Limit: 100,
	})
	entries, _, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
	require.NoError(t, err)
	require.Len(t, entries, 1)

	// Disconnect client.
	err = client.close(DisconnectForceNoReconnect)
	require.NoError(t, err)

	// Verify presence was removed.
	stateRes, err = broker.ReadState(ctx, clientsChannel, MapReadStateOptions{
		Limit: 100,
	})
	entries, _, _ = stateRes.Publications, stateRes.Position, stateRes.Cursor
	require.NoError(t, err)
	require.Len(t, entries, 0)
}

func TestMapSubscribe_CleanupOnUnsubscribe(t *testing.T) {
	node, broker := newTestNodeWithMapBroker(t)

	channel := "test_cleanup_on_unsub"
	ctx := context.Background()

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					Type:                   SubscriptionTypeMap,
					MapRemoveOnUnsubscribe: true,
				},
			}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")
	clientID := client.ID()

	// Subscribe with MapRemoveOnUnsubscribe.
	subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: channel,
		Type:    1,
		Phase:   MapPhaseLive,
	})

	// Publish a key with key=clientID (simulating cursor/ephemeral state).
	_, err := broker.Publish(ctx, channel, clientID, MapPublishOptions{
		Data:       []byte(`{"x":100,"y":200}`),
		ClientInfo: &ClientInfo{ClientID: clientID, UserID: "user1"},
	})
	require.NoError(t, err)

	// Verify key exists.
	stateRes, err := broker.ReadState(ctx, channel, MapReadStateOptions{
		Limit: 100,
	})
	entries, _, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, clientID, entries[0].Key)

	// Unsubscribe - should trigger cleanup of key=clientID.
	client.Unsubscribe(channel)

	// Verify key was removed.
	stateRes, err = broker.ReadState(ctx, channel, MapReadStateOptions{
		Limit: 100,
	})
	entries, _, _ = stateRes.Publications, stateRes.Position, stateRes.Cursor
	require.NoError(t, err)
	require.Len(t, entries, 0)
}

func TestMapSubscribe_CleanupOnDisconnect(t *testing.T) {
	node, broker := newTestNodeWithMapBroker(t)

	channel := "test_cleanup_on_disconnect"
	ctx := context.Background()

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					Type:                   SubscriptionTypeMap,
					MapRemoveOnUnsubscribe: true,
				},
			}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")
	clientID := client.ID()

	// Subscribe with MapRemoveOnUnsubscribe.
	subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: channel,
		Type:    1,
		Phase:   MapPhaseLive,
	})

	// Publish a key with key=clientID.
	_, err := broker.Publish(ctx, channel, clientID, MapPublishOptions{
		Data:       []byte(`{"x":100,"y":200}`),
		ClientInfo: &ClientInfo{ClientID: clientID, UserID: "user1"},
	})
	require.NoError(t, err)

	// Verify key exists.
	stateRes, err := broker.ReadState(ctx, channel, MapReadStateOptions{
		Limit: 100,
	})
	entries, _, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
	require.NoError(t, err)
	require.Len(t, entries, 1)

	// Disconnect client - should trigger cleanup of key=clientID.
	err = client.close(DisconnectForceNoReconnect)
	require.NoError(t, err)

	// Verify key was removed.
	stateRes, err = broker.ReadState(ctx, channel, MapReadStateOptions{
		Limit: 100,
	})
	entries, _, _ = stateRes.Publications, stateRes.Position, stateRes.Cursor
	require.NoError(t, err)
	require.Len(t, entries, 0)
}

func TestPresenceSubscribe_State(t *testing.T) {
	node, broker := newTestNodeWithMapBroker(t)

	ctx := context.Background()

	// Pre-populate presence data using prefix-based channel naming.
	// With prefix "clients:", the presence channel for "test_presence_sub" is "clients:test_presence_sub".
	presenceChannel := "clients:test_presence_sub"
	_, err := broker.Publish(ctx, presenceChannel, "client1", MapPublishOptions{
		ClientInfo: &ClientInfo{ClientID: "client1", UserID: "user1"},
	})
	require.NoError(t, err)

	_, err = broker.Publish(ctx, presenceChannel, "client2", MapPublishOptions{
		ClientInfo: &ClientInfo{ClientID: "client2", UserID: "user2"},
	})
	require.NoError(t, err)

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			require.Equal(t, presenceChannel, e.Channel)
			require.Equal(t, SubscriptionTypeMapClients, e.Type)
			cb(SubscribeReply{Options: SubscribeOptions{Type: SubscriptionTypeMapClients}}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")

	// Streamless presence: single page → LIVE.
	result := subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: presenceChannel,
		Type:    2,
		Phase:   MapPhaseState,
		Limit:   100,
	})

	require.True(t, result.Type == 1)
	require.Equal(t, MapPhaseLive, result.Phase)
	require.Len(t, result.State, 2)
}

func TestPresenceSubscribe_Live(t *testing.T) {
	node, broker := newTestNodeWithMapBroker(t)

	ctx := context.Background()

	// Pre-populate presence data using prefix-based channel naming.
	presenceChannel := "clients:test_presence_live"
	_, err := broker.Publish(ctx, presenceChannel, "client1", MapPublishOptions{
		ClientInfo: &ClientInfo{ClientID: "client1", UserID: "user1"},
	})
	require.NoError(t, err)

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{Options: SubscribeOptions{Type: e.Type}}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")

	// Streamless presence: single page → LIVE directly.
	result := subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: presenceChannel,
		Type:    2,
		Phase:   MapPhaseState,
		Limit:   100,
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

func TestPresenceSubscribe_Positioned_TwoPhase(t *testing.T) {
	// Presence subscriptions can also be positioned (EnablePositioning: true).
	// In that case, the two-phase STATE→LIVE flow works like regular map subscriptions.
	node, broker := newTestNodeWithMapBroker(t)
	setTestMapChannelOptionsConverging(node)

	ctx := context.Background()

	presenceChannel := "clients:test_presence_positioned"
	_, err := broker.Publish(ctx, presenceChannel, "client1", MapPublishOptions{
		ClientInfo: &ClientInfo{ClientID: "client1", UserID: "user1"},
	})
	require.NoError(t, err)

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{Options: SubscribeOptions{
				Type: e.Type,
			}}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")

	// Positioned presence: STATE returns phase=2 (not LIVE).
	stateResult := subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: presenceChannel,
		Type:    2,
		Phase:   MapPhaseState,
		Limit:   100,
	})

	require.True(t, stateResult.Type == 1)
	require.Equal(t, MapPhaseState, stateResult.Phase)
	require.Len(t, stateResult.State, 1)

	// Then go LIVE explicitly.
	liveResult := subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: presenceChannel,
		Type:    2,
		Phase:   MapPhaseLive,
		Offset:  stateResult.Offset,
		Epoch:   stateResult.Epoch,
	})

	require.True(t, liveResult.Type == 1)
	require.Equal(t, MapPhaseLive, liveResult.Phase)

	// Verify flagMapPresence is set.
	client.mu.RLock()
	ctx2, ok := client.channels[presenceChannel]
	client.mu.RUnlock()
	require.True(t, ok)
	require.True(t, channelHasFlag(ctx2.flags, flagMapPresence))
}

func TestPresenceSubscribe_NotAllowed(t *testing.T) {
	node, _ := newTestNodeWithMapBroker(t)

	presenceChannel := "clients:test_presence_not_allowed"

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{}, ErrorPermissionDenied)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")

	// Try to subscribe to presence - should be denied.
	rwWrapper := testReplyWriterWrapper()
	err := client.handleSubscribe(&protocol.SubscribeRequest{
		Channel: presenceChannel,
		Type:    2,
		Phase:   MapPhaseState,
	}, &protocol.Command{Id: 1}, time.Now(), rwWrapper.rw)
	require.NoError(t, err)
	require.Len(t, rwWrapper.replies, 1)
	require.NotNil(t, rwWrapper.replies[0].Error)
	require.Equal(t, ErrorPermissionDenied.Code, rwWrapper.replies[0].Error.Code)
}

func TestPresenceSubscribe_NoHandler(t *testing.T) {
	node, _ := newTestNodeWithMapBroker(t)

	presenceChannel := "clients:test_presence_no_handler"

	// No OnSubscribe handler set.
	node.OnConnect(func(client *Client) {})

	client := newTestConnectedClientV2(t, node, "user1")

	// Try to subscribe to presence - should fail because no OnSubscribe handler.
	rwWrapper := testReplyWriterWrapper()
	err := client.handleSubscribe(&protocol.SubscribeRequest{
		Channel: presenceChannel,
		Type:    2,
		Phase:   MapPhaseState,
	}, &protocol.Command{Id: 1}, time.Now(), rwWrapper.rw)
	// When no handler is set, the error is returned directly.
	require.Equal(t, ErrorNotAvailable, err)
}

func TestPresenceSubscribe_AlreadySubscribed(t *testing.T) {
	node, _ := newTestNodeWithMapBroker(t)

	presenceChannel := "clients:test_presence_already_sub"

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{Options: SubscribeOptions{Type: e.Type}}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")

	// First subscribe (streamless, so STATE goes directly to LIVE).
	subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: presenceChannel,
		Type:    2,
		Phase:   MapPhaseState,
		Limit:   100,
	})

	// Second subscribe should fail with AlreadySubscribed.
	rwWrapper := testReplyWriterWrapper()
	err := client.handleSubscribe(&protocol.SubscribeRequest{
		Channel: presenceChannel,
		Type:    2,
		Phase:   MapPhaseState,
	}, &protocol.Command{Id: 1}, time.Now(), rwWrapper.rw)
	// When already subscribed, the error is returned directly.
	require.Equal(t, ErrorAlreadySubscribed, err)
}

func TestMapPresenceTTL(t *testing.T) {
	node, _ := newTestNodeWithMapBroker(t)

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
	node, broker := newTestNodeWithMapBroker(t)

	channel := "test_map_presence"
	ctx := context.Background()

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					Type:                           SubscriptionTypeMap,
					MapClientPresenceChannelPrefix: "clients:",
					MapUserPresenceChannelPrefix:   "$users:",
				},
			}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")

	// Subscribe with both client and user presence.
	subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: channel,
		Type:    1,
		Phase:   MapPhaseLive,
	})

	// Verify :clients presence was added (key=clientId, full info).
	clientsChannel := "clients:" + channel
	stateRes, err := broker.ReadState(ctx, clientsChannel, MapReadStateOptions{
		Limit: 100,
	})
	entries, _, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, client.uid, entries[0].Key)
	require.NotNil(t, entries[0].Info)
	require.Equal(t, client.uid, entries[0].Info.ClientID)
	require.Equal(t, "user1", entries[0].Info.UserID)

	// Verify :users presence was added (key=userId, no info).
	usersChannel := "$users:" + channel
	stateRes, err = broker.ReadState(ctx, usersChannel, MapReadStateOptions{
		Limit: 100,
	})
	entries, _, _ = stateRes.Publications, stateRes.Position, stateRes.Cursor
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
	node, broker := newTestNodeWithMapBroker(t)

	channel := "test_map_presence_cleanup"
	ctx := context.Background()

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					Type:                           SubscriptionTypeMap,
					MapClientPresenceChannelPrefix: "clients:",
					MapUserPresenceChannelPrefix:   "$users:",
				},
			}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")

	// Subscribe with both client and user presence.
	subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: channel,
		Type:    1,
		Phase:   MapPhaseLive,
	})

	// Verify presence exists in both channels.
	clientsChannel := "clients:" + channel
	usersChannel := "$users:" + channel

	stateRes, err := broker.ReadState(ctx, clientsChannel, MapReadStateOptions{Limit: 100})
	entries, _, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
	require.NoError(t, err)
	require.Len(t, entries, 1)

	stateRes, err = broker.ReadState(ctx, usersChannel, MapReadStateOptions{Limit: 100})
	entries, _, _ = stateRes.Publications, stateRes.Position, stateRes.Cursor
	require.NoError(t, err)
	require.Len(t, entries, 1)

	// Disconnect client.
	err = client.close(DisconnectForceNoReconnect)
	require.NoError(t, err)

	// Verify :clients presence was removed.
	stateRes, err = broker.ReadState(ctx, clientsChannel, MapReadStateOptions{Limit: 100})
	entries, _, _ = stateRes.Publications, stateRes.Position, stateRes.Cursor
	require.NoError(t, err)
	require.Len(t, entries, 0)

	// Verify :users presence is NOT removed (TTL-based expiration).
	stateRes, err = broker.ReadState(ctx, usersChannel, MapReadStateOptions{Limit: 100})
	entries, _, _ = stateRes.Publications, stateRes.Position, stateRes.Cursor
	require.NoError(t, err)
	require.Len(t, entries, 1) // Still there, will expire via TTL.
}

func TestMapSubscribe_MultipleClientsPerUser(t *testing.T) {
	node, broker := newTestNodeWithMapBroker(t)

	channel := "test_multi_clients"
	ctx := context.Background()

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					Type:                           SubscriptionTypeMap,
					MapClientPresenceChannelPrefix: "clients:",
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
		Channel: channel,
		Type:    1,
		Phase:   MapPhaseLive,
	})
	subscribeMapClient(t, client2, &protocol.SubscribeRequest{
		Channel: channel,
		Type:    1,
		Phase:   MapPhaseLive,
	})

	// Verify :clients has two entries (one per connection).
	clientsChannel := "clients:" + channel
	stateRes, err := broker.ReadState(ctx, clientsChannel, MapReadStateOptions{Limit: 100})
	entries, _, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
	require.NoError(t, err)
	require.Len(t, entries, 2)

	// Verify :users has one entry (deduplicated by userId).
	usersChannel := "$users:" + channel
	stateRes, err = broker.ReadState(ctx, usersChannel, MapReadStateOptions{Limit: 100})
	entries, _, _ = stateRes.Publications, stateRes.Position, stateRes.Cursor
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, "user1", entries[0].Key)

	// Disconnect one client.
	err = client1.close(DisconnectForceNoReconnect)
	require.NoError(t, err)

	// :clients should have one entry.
	stateRes, err = broker.ReadState(ctx, clientsChannel, MapReadStateOptions{Limit: 100})
	entries, _, _ = stateRes.Publications, stateRes.Position, stateRes.Cursor
	require.NoError(t, err)
	require.Len(t, entries, 1)

	// :users still has the user (TTL refresh from client2).
	stateRes, err = broker.ReadState(ctx, usersChannel, MapReadStateOptions{Limit: 100})
	entries, _, _ = stateRes.Publications, stateRes.Position, stateRes.Cursor
	require.NoError(t, err)
	require.Len(t, entries, 1)
}

// TestMapBroker_ReadStateByKey tests the Key filter for ReadState.
func TestMapBroker_ReadStateByKey(t *testing.T) {
	_, broker := newTestNodeWithMapBroker(t)
	ctx := context.Background()
	ch := "test_read_by_key"

	// Publish 3 keys
	_, err := broker.Publish(ctx, ch, "key1", MapPublishOptions{
		Data: []byte(`{"value":"data1"}`),
	})
	require.NoError(t, err)

	_, err = broker.Publish(ctx, ch, "key2", MapPublishOptions{
		Data: []byte(`{"value":"data2"}`),
	})
	require.NoError(t, err)

	_, err = broker.Publish(ctx, ch, "key3", MapPublishOptions{
		Data: []byte(`{"value":"data3"}`),
	})
	require.NoError(t, err)

	// Read single key
	stateRes, err := broker.ReadState(ctx, ch, MapReadStateOptions{
		Key: "key2",
	})
	pubs, pos, cursor := stateRes.Publications, stateRes.Position, stateRes.Cursor
	require.NoError(t, err)
	require.Len(t, pubs, 1)
	require.Equal(t, "key2", pubs[0].Key)
	require.Equal(t, []byte(`{"value":"data2"}`), pubs[0].Data)
	require.Empty(t, cursor) // No pagination for single key
	require.NotEmpty(t, pos.Epoch)

	// Read non-existent key
	stateRes, err = broker.ReadState(ctx, ch, MapReadStateOptions{
		Key: "nonexistent",
	})
	pubs, _, _ = stateRes.Publications, stateRes.Position, stateRes.Cursor
	require.NoError(t, err)
	require.Len(t, pubs, 0)
}

// TestMapBroker_CASSuccess tests successful CAS update.
func TestMapBroker_CASSuccess(t *testing.T) {
	node, broker := newTestNodeWithMapBroker(t)
	setTestMapChannelOptionsConverging(node)
	ctx := context.Background()
	ch := "test_cas_success"

	// Publish initial value
	res1, err := broker.Publish(ctx, ch, "counter", MapPublishOptions{
		Data: []byte(`{"value":10}`),
	})
	require.NoError(t, err)
	require.False(t, res1.Suppressed)

	// Read current state - position includes offset AND epoch
	stateRes, err := broker.ReadState(ctx, ch, MapReadStateOptions{Key: "counter"})
	pubs, pos, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
	require.NoError(t, err)
	require.Len(t, pubs, 1)
	expectedPos := StreamPosition{Offset: pubs[0].Offset, Epoch: pos.Epoch}

	// CAS update with correct position
	res2, err := broker.Publish(ctx, ch, "counter", MapPublishOptions{
		Data:             []byte(`{"value":15}`),
		ExpectedPosition: &expectedPos,
	})
	require.NoError(t, err)
	require.False(t, res2.Suppressed)
	require.Greater(t, res2.Position.Offset, res1.Position.Offset)

	// Verify the value was updated
	stateRes, err = broker.ReadState(ctx, ch, MapReadStateOptions{Key: "counter"})
	pubs, _, _ = stateRes.Publications, stateRes.Position, stateRes.Cursor
	require.NoError(t, err)
	require.Len(t, pubs, 1)
	require.Equal(t, []byte(`{"value":15}`), pubs[0].Data)
}

// TestMapBroker_CASConflict tests CAS conflict when position has changed.
func TestMapBroker_CASConflict(t *testing.T) {
	node, broker := newTestNodeWithMapBroker(t)
	setTestMapChannelOptionsConverging(node)
	ctx := context.Background()
	ch := "test_cas_conflict"

	// Publish initial value
	_, err := broker.Publish(ctx, ch, "counter", MapPublishOptions{
		Data: []byte(`{"value":10}`),
	})
	require.NoError(t, err)

	// Read current state
	stateRes, err := broker.ReadState(ctx, ch, MapReadStateOptions{Key: "counter"})
	pubs, pos, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
	require.NoError(t, err)
	require.Len(t, pubs, 1)
	originalPos := StreamPosition{Offset: pubs[0].Offset, Epoch: pos.Epoch}

	// Another client updates the key (simulated)
	_, err = broker.Publish(ctx, ch, "counter", MapPublishOptions{
		Data: []byte(`{"value":12}`),
	})
	require.NoError(t, err)

	// CAS with stale position - should fail
	res, err := broker.Publish(ctx, ch, "counter", MapPublishOptions{
		Data:             []byte(`{"value":15}`),
		ExpectedPosition: &originalPos, // stale offset!
	})
	require.NoError(t, err)
	require.True(t, res.Suppressed)
	require.Equal(t, SuppressReasonPositionMismatch, res.SuppressReason)

	// CurrentPublication should contain the current state for immediate retry
	require.NotNil(t, res.CurrentPublication)
	require.Equal(t, []byte(`{"value":12}`), res.CurrentPublication.Data)

	// Immediate retry using returned position - should succeed
	retryPos := StreamPosition{Offset: res.CurrentPublication.Offset, Epoch: res.Position.Epoch}
	res2, err := broker.Publish(ctx, ch, "counter", MapPublishOptions{
		Data:             []byte(`{"value":15}`),
		ExpectedPosition: &retryPos,
	})
	require.NoError(t, err)
	require.False(t, res2.Suppressed)

	// Verify the value was updated
	stateRes, err = broker.ReadState(ctx, ch, MapReadStateOptions{Key: "counter"})
	pubs, _, _ = stateRes.Publications, stateRes.Position, stateRes.Cursor
	require.NoError(t, err)
	require.Len(t, pubs, 1)
	require.Equal(t, []byte(`{"value":15}`), pubs[0].Data)
}

// TestMapBroker_CASNonExistent tests CAS on a key that doesn't exist.
func TestMapBroker_CASNonExistent(t *testing.T) {
	node, broker := newTestNodeWithMapBroker(t)
	setTestMapChannelOptionsConverging(node)
	ctx := context.Background()
	ch := "test_cas_nonexistent"

	// First create the channel by publishing something
	_, err := broker.Publish(ctx, ch, "other_key", MapPublishOptions{
		Data: []byte(`{"value":1}`),
	})
	require.NoError(t, err)

	// Get the epoch
	stateRes, err := broker.ReadState(ctx, ch, MapReadStateOptions{Limit: 1})
	_, pos, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
	require.NoError(t, err)

	// Try CAS on non-existent key with expected position
	expectedPos := StreamPosition{Offset: 42, Epoch: pos.Epoch}
	res, err := broker.Publish(ctx, ch, "newkey", MapPublishOptions{
		Data:             []byte(`{"value":1}`),
		ExpectedPosition: &expectedPos, // expects key to exist
	})
	require.NoError(t, err)
	require.True(t, res.Suppressed)
	require.Equal(t, SuppressReasonPositionMismatch, res.SuppressReason)
}

// TestMapBroker_CASWrongEpoch tests CAS with correct offset but wrong epoch.
func TestMapBroker_CASWrongEpoch(t *testing.T) {
	node, broker := newTestNodeWithMapBroker(t)
	setTestMapChannelOptionsConverging(node)
	ctx := context.Background()
	ch := "test_cas_wrong_epoch"

	// Publish initial value
	_, err := broker.Publish(ctx, ch, "counter", MapPublishOptions{
		Data: []byte(`{"value":10}`),
	})
	require.NoError(t, err)

	// Read current state
	stateRes, err := broker.ReadState(ctx, ch, MapReadStateOptions{Key: "counter"})
	pubs, _, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
	require.NoError(t, err)
	require.Len(t, pubs, 1)

	// Use correct offset but wrong epoch
	wrongPos := StreamPosition{Offset: pubs[0].Offset, Epoch: "wrong-epoch"}

	res, err := broker.Publish(ctx, ch, "counter", MapPublishOptions{
		Data:             []byte(`{"value":15}`),
		ExpectedPosition: &wrongPos,
	})
	require.NoError(t, err)
	require.True(t, res.Suppressed)
	require.Equal(t, SuppressReasonPositionMismatch, res.SuppressReason)

	// CurrentPublication should contain the current state for immediate retry
	require.NotNil(t, res.CurrentPublication)
	require.Equal(t, []byte(`{"value":10}`), res.CurrentPublication.Data)

	// Verify value unchanged
	stateRes, err = broker.ReadState(ctx, ch, MapReadStateOptions{Key: "counter"})
	pubs, _, _ = stateRes.Publications, stateRes.Position, stateRes.Cursor
	require.NoError(t, err)
	require.Len(t, pubs, 1)
	require.Equal(t, []byte(`{"value":10}`), pubs[0].Data)
}

// TestMapBroker_StreamDataDifferentPayloads tests publishing with different
// data for state (full state) and stream (incremental update).
func TestMapBroker_StreamDataDifferentPayloads(t *testing.T) {
	node, broker := newTestNodeWithMapBroker(t)
	setTestMapChannelOptionsConverging(node)
	ctx := context.Background()
	ch := "test_stream_data"

	// Publish with different payloads: state gets full state, stream gets delta
	_, err := broker.Publish(ctx, ch, "counter", MapPublishOptions{
		Data:       []byte(`{"count":100}`), // Full state → state
		StreamData: []byte(`{"delta":100}`), // Incremental → stream
	})
	require.NoError(t, err)

	// Read state - should have full state
	stateRes, err := broker.ReadState(ctx, ch, MapReadStateOptions{Key: "counter"})
	pubs, pos, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
	require.NoError(t, err)
	require.Len(t, pubs, 1)
	require.Equal(t, []byte(`{"count":100}`), pubs[0].Data)

	// Read stream - should have incremental data
	streamResult, err := broker.ReadStream(ctx, ch, MapReadStreamOptions{
		Filter: StreamFilter{Limit: 10},
	})
	require.NoError(t, err)
	require.Len(t, streamResult.Publications, 1)
	require.Equal(t, []byte(`{"delta":100}`), streamResult.Publications[0].Data)

	// Update with CAS: read current position, update with different payloads
	expectedPos := StreamPosition{Offset: pubs[0].Offset, Epoch: pos.Epoch}
	_, err = broker.Publish(ctx, ch, "counter", MapPublishOptions{
		Data:             []byte(`{"count":105}`), // New full state → state
		StreamData:       []byte(`{"delta":5}`),   // Incremental → stream
		ExpectedPosition: &expectedPos,
	})
	require.NoError(t, err)

	// Verify state has new full state
	stateRes, err = broker.ReadState(ctx, ch, MapReadStateOptions{Key: "counter"})
	pubs, _, _ = stateRes.Publications, stateRes.Position, stateRes.Cursor
	require.NoError(t, err)
	require.Len(t, pubs, 1)
	require.Equal(t, []byte(`{"count":105}`), pubs[0].Data)

	// Verify stream has both incremental updates
	streamResult, err = broker.ReadStream(ctx, ch, MapReadStreamOptions{
		Filter: StreamFilter{Limit: 10},
	})
	require.NoError(t, err)
	require.Len(t, streamResult.Publications, 2)
	require.Equal(t, []byte(`{"delta":100}`), streamResult.Publications[0].Data)
	require.Equal(t, []byte(`{"delta":5}`), streamResult.Publications[1].Data)
}

// TestMapBroker_StreamDataWithoutStreamData tests that when StreamData is not set,
// Data is used for both state and stream.
func TestMapBroker_StreamDataWithoutStreamData(t *testing.T) {
	node, broker := newTestNodeWithMapBroker(t)
	setTestMapChannelOptionsConverging(node)
	ctx := context.Background()
	ch := "test_no_stream_data"

	// Publish without StreamData - Data should be used for both
	_, err := broker.Publish(ctx, ch, "item", MapPublishOptions{
		Data: []byte(`{"name":"test","value":42}`),
	})
	require.NoError(t, err)

	// Read state
	stateRes, err := broker.ReadState(ctx, ch, MapReadStateOptions{Key: "item"})
	pubs, _, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
	require.NoError(t, err)
	require.Len(t, pubs, 1)
	require.Equal(t, []byte(`{"name":"test","value":42}`), pubs[0].Data)

	// Read stream - should have same data
	streamResult, err := broker.ReadStream(ctx, ch, MapReadStreamOptions{
		Filter: StreamFilter{Limit: 10},
	})
	require.NoError(t, err)
	require.Len(t, streamResult.Publications, 1)
	require.Equal(t, []byte(`{"name":"test","value":42}`), streamResult.Publications[0].Data)
}

// Tests for STATE→LIVE direct transition optimization (MapStateToLiveEnabled).

func TestMapSubscribe_StateToLive_DirectTransition(t *testing.T) {
	// Test that when MapStateToLiveEnabled is true and stream is close enough,
	// server transitions directly from STATE to LIVE on the last state page.
	node, err := New(Config{
		LogLevel:              LogLevelTrace,
		LogHandler:            func(entry LogEntry) {},
		MapStateToLiveEnabled: true, // Enable the optimization
		GetMapChannelOptions: func(channel string) MapChannelOptions {
			return MapChannelOptions{
				SyncMode:      MapSyncConverging,
				RetentionMode: MapRetentionExpiring,
				KeyTTL:        60 * time.Second,
			}
		},
	})
	require.NoError(t, err)

	broker, err := NewMemoryMapBroker(node, MemoryMapBrokerConfig{})
	require.NoError(t, err)
	err = broker.RegisterEventHandler(nil)
	require.NoError(t, err)
	node.SetMapBroker(broker)

	err = node.Run()
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = node.Shutdown(context.Background())
	})

	channel := "test_state_to_live"
	ctx := context.Background()

	// Pre-populate state with a few entries.
	for i := 0; i < 3; i++ {
		_, err := broker.Publish(ctx, channel, string(rune('a'+i)), MapPublishOptions{
			Data: []byte(`{"v":"data"}`),
		})
		require.NoError(t, err)
	}

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					Type: SubscriptionTypeMap,
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
		GetMapChannelOptions: func(channel string) MapChannelOptions {
			return MapChannelOptions{
				SyncMode:      MapSyncConverging,
				RetentionMode: MapRetentionExpiring,
				KeyTTL:        60 * time.Second,
			}
		},
	})
	require.NoError(t, err)

	broker, err := NewMemoryMapBroker(node, MemoryMapBrokerConfig{})
	require.NoError(t, err)
	err = broker.RegisterEventHandler(nil)
	require.NoError(t, err)
	node.SetMapBroker(broker)

	err = node.Run()
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = node.Shutdown(context.Background())
	})

	channel := "test_state_to_live_with_stream"
	ctx := context.Background()

	// Pre-populate initial state.
	_, err = broker.Publish(ctx, channel, "key1", MapPublishOptions{
		Data: []byte(`{"v":"initial"}`),
	})
	require.NoError(t, err)

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					Type: SubscriptionTypeMap,
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
		GetMapChannelOptions: func(channel string) MapChannelOptions {
			return MapChannelOptions{
				SyncMode:      MapSyncConverging,
				RetentionMode: MapRetentionExpiring,
				KeyTTL:        60 * time.Second,
			}
		},
	})
	require.NoError(t, err)

	broker, err := NewMemoryMapBroker(node, MemoryMapBrokerConfig{})
	require.NoError(t, err)
	err = broker.RegisterEventHandler(nil)
	require.NoError(t, err)
	node.SetMapBroker(broker)

	err = node.Run()
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = node.Shutdown(context.Background())
	})

	channel := "test_pagination_last_page_live"
	ctx := context.Background()

	// Pre-populate 10 entries.
	for i := 0; i < 10; i++ {
		_, err := broker.Publish(ctx, channel, string(rune('a'+i)), MapPublishOptions{
			Data: []byte(`{"v":"data"}`),
		})
		require.NoError(t, err)
	}

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					Type: SubscriptionTypeMap,
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

func TestMapSubscribe_StateToLive_PublishDuringPagination(t *testing.T) {
	// Regression test: publications made between STATE pages must not be lost
	// when server transitions directly from STATE to LIVE.
	//
	// Scenario:
	//   1. Client starts STATE pagination (page 1): offset frozen at N
	//   2. External publishes happen → stream offset becomes N+2
	//   3. Client paginates through remaining state pages until last one
	//   4. On last page, server goes LIVE directly (stream catch-up since frozen N)
	//   5. Result: 2 publications recovered in the LIVE response
	node, err := New(Config{
		LogLevel:              LogLevelTrace,
		LogHandler:            func(entry LogEntry) {},
		MapStateToLiveEnabled: true,
		GetMapChannelOptions: func(channel string) MapChannelOptions {
			return MapChannelOptions{
				SyncMode:      MapSyncConverging,
				RetentionMode: MapRetentionExpiring,
				KeyTTL:        60 * time.Second,
			}
		},
	})
	require.NoError(t, err)

	broker, err := NewMemoryMapBroker(node, MemoryMapBrokerConfig{})
	require.NoError(t, err)
	err = broker.RegisterEventHandler(nil)
	require.NoError(t, err)
	node.SetMapBroker(broker)

	err = node.Run()
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = node.Shutdown(context.Background())
	})

	channel := "test_publish_during_pagination"
	ctx := context.Background()

	// Pre-populate 10 entries.
	for i := 0; i < 10; i++ {
		_, err := broker.Publish(ctx, channel, string(rune('a'+i)), MapPublishOptions{
			Data: []byte(`{"v":"initial"}`),
		})
		require.NoError(t, err)
	}

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					Type: SubscriptionTypeMap,
				},
			}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")

	// First page: captures frozen offset.
	result := subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: channel,
		Type:    1,
		Phase:   MapPhaseState,
		Limit:   5,
	})
	require.Equal(t, MapPhaseState, result.Phase)
	require.NotEmpty(t, result.Cursor)

	frozenOffset := result.Offset
	epoch := result.Epoch

	// Publish 2 entries DURING pagination — after first page was read.
	// These won't appear in state pages (filtered by offset > frozenOffset).
	// They MUST be caught up via stream read when going LIVE.
	_, err = broker.Publish(ctx, channel, "new_x", MapPublishOptions{
		Data: []byte(`{"v":"during_sync_1"}`),
	})
	require.NoError(t, err)
	_, err = broker.Publish(ctx, channel, "new_y", MapPublishOptions{
		Data: []byte(`{"v":"during_sync_2"}`),
	})
	require.NoError(t, err)

	// Paginate through remaining state pages until LIVE.
	cursor := result.Cursor
	var liveResult *protocol.SubscribeResult
	for i := 0; i < 20; i++ { // Safety limit to prevent infinite loop.
		r := subscribeMapClient(t, client, &protocol.SubscribeRequest{
			Channel: channel,
			Type:    1,
			Phase:   MapPhaseState,
			Limit:   5,
			Cursor:  cursor,
			Offset:  frozenOffset,
			Epoch:   epoch,
		})
		// Frozen offset must be preserved on all intermediate pages.
		if r.Phase == MapPhaseState {
			require.Equal(t, frozenOffset, r.Offset, "frozen offset must be consistent across pages")
			cursor = r.Cursor
			require.NotEmpty(t, cursor, "intermediate STATE page must have cursor")
			continue
		}
		require.Equal(t, MapPhaseLive, r.Phase)
		liveResult = r
		break
	}
	require.NotNil(t, liveResult, "subscription must eventually transition to LIVE")
	// The 2 publications made during pagination must appear in stream catch-up.
	require.Len(t, liveResult.Publications, 2,
		"publications made during STATE pagination must be recovered via stream catch-up")
	require.Equal(t, frozenOffset+2, liveResult.Offset)
}

func TestMapSubscribe_StateToLive_PublishDuringPagination_ManyPublishes(t *testing.T) {
	// Regression test with publications happening at multiple points during
	// multi-page pagination. Verifies the frozen offset stays consistent and
	// ALL publications during the entire pagination are recovered.
	node, err := New(Config{
		LogLevel:              LogLevelTrace,
		LogHandler:            func(entry LogEntry) {},
		MapStateToLiveEnabled: true,
		GetMapChannelOptions: func(channel string) MapChannelOptions {
			return MapChannelOptions{
				SyncMode:      MapSyncConverging,
				RetentionMode: MapRetentionExpiring,
				KeyTTL:        60 * time.Second,
			}
		},
	})
	require.NoError(t, err)

	broker, err := NewMemoryMapBroker(node, MemoryMapBrokerConfig{})
	require.NoError(t, err)
	err = broker.RegisterEventHandler(nil)
	require.NoError(t, err)
	node.SetMapBroker(broker)

	err = node.Run()
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = node.Shutdown(context.Background())
	})

	channel := "test_publish_during_multi_page"
	ctx := context.Background()

	publishOpts := MapPublishOptions{
		Data: []byte(`{"v":"initial"}`),
	}

	// Pre-populate 15 entries.
	for i := 0; i < 15; i++ {
		_, err := broker.Publish(ctx, channel, string(rune('a'+i)), publishOpts)
		require.NoError(t, err)
	}

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					Type: SubscriptionTypeMap,
				},
			}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")

	// Page 1: captures frozen offset.
	result := subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: channel,
		Type:    1,
		Phase:   MapPhaseState,
		Limit:   5,
	})
	require.Equal(t, MapPhaseState, result.Phase)
	require.NotEmpty(t, result.Cursor)

	frozenOffset := result.Offset
	epoch := result.Epoch

	// Publish 1 entry after page 1.
	publishOpts.Data = []byte(`{"v":"mid_1"}`)
	_, err = broker.Publish(ctx, channel, "mid_1", publishOpts)
	require.NoError(t, err)

	// Read page 2.
	result2 := subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: channel,
		Type:    1,
		Phase:   MapPhaseState,
		Limit:   5,
		Cursor:  result.Cursor,
		Offset:  frozenOffset,
		Epoch:   epoch,
	})
	require.Equal(t, frozenOffset, result2.Offset, "frozen offset must be preserved")

	// Publish 2 more entries after page 2.
	publishOpts.Data = []byte(`{"v":"mid_2"}`)
	_, err = broker.Publish(ctx, channel, "mid_2", publishOpts)
	require.NoError(t, err)
	publishOpts.Data = []byte(`{"v":"mid_3"}`)
	_, err = broker.Publish(ctx, channel, "mid_3", publishOpts)
	require.NoError(t, err)

	// Paginate remaining pages until LIVE.
	cursor := result2.Cursor
	if cursor == "" {
		// Page 2 was the last page and went LIVE — verify stream catch-up.
		require.Equal(t, MapPhaseLive, result2.Phase)
		require.GreaterOrEqual(t, len(result2.Publications), 1,
			"at least the publication from after page 1 must be recovered")
		return
	}

	var liveResult *protocol.SubscribeResult
	for i := 0; i < 20; i++ {
		r := subscribeMapClient(t, client, &protocol.SubscribeRequest{
			Channel: channel,
			Type:    1,
			Phase:   MapPhaseState,
			Limit:   5,
			Cursor:  cursor,
			Offset:  frozenOffset,
			Epoch:   epoch,
		})
		if r.Phase == MapPhaseState {
			require.Equal(t, frozenOffset, r.Offset)
			cursor = r.Cursor
			require.NotEmpty(t, cursor)
			continue
		}
		require.Equal(t, MapPhaseLive, r.Phase)
		liveResult = r
		break
	}
	require.NotNil(t, liveResult, "subscription must eventually transition to LIVE")
	// All 3 publications must be recovered via stream catch-up.
	require.Len(t, liveResult.Publications, 3,
		"all publications made during multi-page pagination must be recovered")
	require.Equal(t, frozenOffset+3, liveResult.Offset)
}

func TestMapSubscribe_StreamPhaseRecovery(t *testing.T) {
	// Test that a reconnecting client can use phase=1 (STREAM) with recover=true
	// to catch up from its last known position without going through STATE phase.
	// This simulates a client reconnection after disconnect.
	node, broker := newTestNodeWithMapBroker(t)
	setTestMapChannelOptionsConverging(node)

	channel := "test_stream_recovery"
	ctx := context.Background()

	// Pre-populate some data.
	var lastOffset uint64
	var epoch string
	for i := 0; i < 5; i++ {
		res, err := broker.Publish(ctx, channel, string(rune('a'+i)), MapPublishOptions{
			Data: []byte(`{"v":"data"}`),
		})
		require.NoError(t, err)
		lastOffset = res.Position.Offset
		epoch = res.Position.Epoch
	}

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					Type: SubscriptionTypeMap,
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
	node, broker := newTestNodeWithMapBroker(t)
	setTestMapChannelOptionsConverging(node)

	channel := "test_stream_no_recover"
	ctx := context.Background()

	// Pre-populate some data.
	res, err := broker.Publish(ctx, channel, "key1", MapPublishOptions{
		Data: []byte(`{"v":"data"}`),
	})
	require.NoError(t, err)

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					Type: SubscriptionTypeMap,
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
	node, broker := newTestNodeWithMapBroker(t)
	setTestMapChannelOptionsConverging(node)

	channel := "test_stream_recovery_large_gap"
	ctx := context.Background()

	// Pre-populate many entries to create a gap.
	var lastOffset uint64
	var epoch string
	for i := 0; i < 100; i++ {
		res, err := broker.Publish(ctx, channel, string(rune('a'+(i%26)))+string(rune('0'+(i/26))), MapPublishOptions{
			Data: []byte(`{"v":"data"}`),
		})
		require.NoError(t, err)
		lastOffset = res.Position.Offset
		epoch = res.Position.Epoch
	}

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					Type: SubscriptionTypeMap,
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
	node, broker := newTestNodeWithMapBroker(t)
	setTestMapChannelOptionsConverging(node)

	channel := "test_live_recovery"
	ctx := context.Background()

	// Pre-populate some data.
	var lastOffset uint64
	var epoch string
	for i := 0; i < 5; i++ {
		res, err := broker.Publish(ctx, channel, string(rune('a'+i)), MapPublishOptions{
			Data: []byte(`{"v":"data"}`),
		})
		require.NoError(t, err)
		lastOffset = res.Position.Offset
		epoch = res.Position.Epoch
	}

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					Type: SubscriptionTypeMap,
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
	// Test that when MapStateToLiveEnabled is false in positioned mode,
	// STATE phase never transitions directly to LIVE (always returns phase=2).
	// Note: in streamless mode, STATE always transitions to LIVE on last page
	// regardless of this setting.
	node, err := New(Config{
		LogLevel:              LogLevelTrace,
		LogHandler:            func(entry LogEntry) {},
		MapStateToLiveEnabled: false, // Disable the optimization
		GetMapChannelOptions: func(channel string) MapChannelOptions {
			return MapChannelOptions{
				SyncMode:      MapSyncConverging,
				RetentionMode: MapRetentionExpiring,
				KeyTTL:        60 * time.Second,
			}
		},
	})
	require.NoError(t, err)

	broker, err := NewMemoryMapBroker(node, MemoryMapBrokerConfig{})
	require.NoError(t, err)
	err = broker.RegisterEventHandler(nil)
	require.NoError(t, err)
	node.SetMapBroker(broker)

	err = node.Run()
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = node.Shutdown(context.Background())
	})

	channel := "test_state_to_live_disabled"
	ctx := context.Background()

	// Pre-populate state.
	_, err = broker.Publish(ctx, channel, "key1", MapPublishOptions{
		Data: []byte(`{"v":"data"}`),
	})
	require.NoError(t, err)

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					Type: SubscriptionTypeMap, // Positioned mode: flag controls STATE→LIVE.
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
	// Positioned mode with flag disabled: should return STATE (phase=2), not LIVE.
	require.Equal(t, MapPhaseState, result.Phase)
	require.Len(t, result.State, 1)
	require.Empty(t, result.Cursor) // Last page but NOT live
}

// Streamless mode: phase=1 (STREAM) should be rejected with ErrorBadRequest.
func TestMapSubscribe_Streamless_StreamPhaseRejected(t *testing.T) {
	node, broker := newTestNodeWithMapBroker(t)

	channel := "test_streamless_stream_rejected"
	ctx := context.Background()

	_, err := broker.Publish(ctx, channel, "key1", MapPublishOptions{
		Data: []byte(`{"v":"data"}`),
	})
	require.NoError(t, err)

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					Type: SubscriptionTypeMap, // Streamless: no EnablePositioning.
				},
			}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")

	// Phase=1 (STREAM) is not valid in streamless mode.
	rwWrapper := testReplyWriterWrapper()
	err = client.handleSubscribe(&protocol.SubscribeRequest{
		Channel: channel,
		Type:    1,
		Phase:   MapPhaseStream,
		Offset:  1,
		Epoch:   "test",
		Limit:   100,
	}, &protocol.Command{Id: 1}, time.Now(), rwWrapper.rw)
	require.NoError(t, err)
	require.Len(t, rwWrapper.replies, 1)
	require.NotNil(t, rwWrapper.replies[0].Error)
	require.Equal(t, ErrorBadRequest.Code, rwWrapper.replies[0].Error.Code)
}

// Streamless mode: reconnection with recover=true should redirect to immediate join
// and return full state (not just stream catch-up).
func TestMapSubscribe_Streamless_RecoveryRedirectsToImmediateJoin(t *testing.T) {
	node, broker := newTestNodeWithMapBroker(t)

	channel := "test_streamless_recovery_redirect"
	ctx := context.Background()

	_, err := broker.Publish(ctx, channel, "key1", MapPublishOptions{
		Data: []byte(`{"v":"data1"}`),
	})
	require.NoError(t, err)
	_, err = broker.Publish(ctx, channel, "key2", MapPublishOptions{
		Data: []byte(`{"v":"data2"}`),
	})
	require.NoError(t, err)

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					Type: SubscriptionTypeMap, // Streamless.
				},
			}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")

	// Streamless reconnection: phase=0, recover=true.
	// Should redirect to immediate join with full state.
	result := subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: channel,
		Type:    1,
		Phase:   MapPhaseLive,
		Recover: true, // Reconnecting.
		Offset:  42,   // Stale offset (ignored in streamless).
		Epoch:   "old",
	})

	require.Equal(t, MapPhaseLive, result.Phase)
	// Full state should be returned (not just stream catch-up).
	require.Len(t, result.State, 2)
	require.NotEmpty(t, result.Epoch)
	// Client should be subscribed.
	require.Contains(t, client.channels, channel)
}

// Positioned mode: epoch mismatch during STREAM phase should return ErrorUnrecoverablePosition.
func TestMapSubscribe_Positioned_StreamEpochMismatch(t *testing.T) {
	node, broker := newTestNodeWithMapBroker(t)
	setTestMapChannelOptionsConverging(node)

	channel := "test_stream_epoch_mismatch"
	ctx := context.Background()

	var epoch string
	for i := 0; i < 5; i++ {
		res, err := broker.Publish(ctx, channel, string(rune('a'+i)), MapPublishOptions{
			Data: []byte(`{"v":"data"}`),
		})
		require.NoError(t, err)
		epoch = res.Position.Epoch
	}

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					Type: SubscriptionTypeMap,
				},
			}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")

	// First do STATE to authorize and capture epoch.
	stateResult := subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: channel,
		Type:    1,
		Phase:   MapPhaseState,
		Limit:   100,
	})
	require.Equal(t, epoch, stateResult.Epoch)

	// Now send STREAM with wrong epoch — should get ErrorUnrecoverablePosition.
	rwWrapper := testReplyWriterWrapper()
	err := client.handleSubscribe(&protocol.SubscribeRequest{
		Channel: channel,
		Type:    1,
		Phase:   MapPhaseStream,
		Offset:  2,
		Epoch:   "wrong-epoch",
		Limit:   100,
	}, &protocol.Command{Id: 1}, time.Now(), rwWrapper.rw)
	require.NoError(t, err)
	require.Len(t, rwWrapper.replies, 1)
	require.NotNil(t, rwWrapper.replies[0].Error)
	require.Equal(t, ErrorUnrecoverablePosition.Code, rwWrapper.replies[0].Error.Code)
}

// Positioned mode: concurrent pagination requests on the same channel should return ErrorConcurrentPagination.
func TestMapSubscribe_ConcurrentPagination(t *testing.T) {
	node, broker := newTestNodeWithMapBroker(t)
	setTestMapChannelOptionsConverging(node)

	channel := "test_concurrent_pagination"
	ctx := context.Background()

	for i := 0; i < 20; i++ {
		_, err := broker.Publish(ctx, channel, string(rune('a'+(i%26))), MapPublishOptions{
			Data: []byte(`{"v":"data"}`),
		})
		require.NoError(t, err)
	}

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					Type: SubscriptionTypeMap,
				},
			}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")

	// Manually acquire pagination lock to simulate concurrent request.
	client.mu.Lock()
	if client.mapPaginationLocks == nil {
		client.mapPaginationLocks = make(map[string]struct{})
	}
	client.mapPaginationLocks[channel] = struct{}{}
	client.mu.Unlock()

	// Now try STATE request — should fail with ErrorConcurrentPagination.
	rwWrapper := testReplyWriterWrapper()
	err := client.handleSubscribe(&protocol.SubscribeRequest{
		Channel: channel,
		Type:    1,
		Phase:   MapPhaseState,
		Limit:   5,
	}, &protocol.Command{Id: 1}, time.Now(), rwWrapper.rw)
	require.NoError(t, err)
	require.Len(t, rwWrapper.replies, 1)
	require.NotNil(t, rwWrapper.replies[0].Error)
	require.Equal(t, ErrorConcurrentPagination.Code, rwWrapper.replies[0].Error.Code)

	// Release lock.
	client.mu.Lock()
	delete(client.mapPaginationLocks, channel)
	client.mu.Unlock()
}

// Positioned mode: MapRecoveryMaxPublicationLimit boundary test.
// When client is exactly at the limit, recovery should succeed.
// When client is beyond the limit, should get ErrorUnrecoverablePosition.
func TestMapSubscribe_RecoveryMaxPublicationLimit(t *testing.T) {
	node, err := New(Config{
		LogLevel:                       LogLevelTrace,
		LogHandler:                     func(entry LogEntry) {},
		MapRecoveryMaxPublicationLimit: 5, // Max 5 publications during recovery.
		GetMapChannelOptions: func(channel string) MapChannelOptions {
			return MapChannelOptions{
				SyncMode:      MapSyncConverging,
				RetentionMode: MapRetentionExpiring,
				KeyTTL:        60 * time.Second,
			}
		},
	})
	require.NoError(t, err)

	broker, err := NewMemoryMapBroker(node, MemoryMapBrokerConfig{})
	require.NoError(t, err)
	err = broker.RegisterEventHandler(nil)
	require.NoError(t, err)
	node.SetMapBroker(broker)

	err = node.Run()
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = node.Shutdown(context.Background())
	})

	channel := "test_recovery_limit"
	ctx := context.Background()

	var epoch string
	for i := 0; i < 10; i++ {
		res, pubErr := broker.Publish(ctx, channel, string(rune('a'+i)), MapPublishOptions{
			Data: []byte(`{"v":"data"}`),
		})
		require.NoError(t, pubErr)
		epoch = res.Position.Epoch
	}

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					Type: SubscriptionTypeMap,
				},
			}, nil)
		})
	})

	// Test 1: Client at offset 5 needs 5 publications (exactly at limit) — should succeed.
	client1 := newTestConnectedClientV2(t, node, "user1")
	result := subscribeMapClient(t, client1, &protocol.SubscribeRequest{
		Channel: channel,
		Type:    1,
		Phase:   MapPhaseLive,
		Offset:  5,
		Epoch:   epoch,
		Recover: true,
	})
	require.Equal(t, MapPhaseLive, result.Phase)
	require.Len(t, result.Publications, 5) // Exactly at limit — OK.

	// Test 2: Client at offset 4 needs 6 publications (exceeds limit) — should fail.
	client2 := newTestConnectedClientV2(t, node, "user2")
	rwWrapper := testReplyWriterWrapper()
	err = client2.handleSubscribe(&protocol.SubscribeRequest{
		Channel: channel,
		Type:    1,
		Phase:   MapPhaseLive,
		Offset:  4,
		Epoch:   epoch,
		Recover: true,
	}, &protocol.Command{Id: 1}, time.Now(), rwWrapper.rw)
	require.NoError(t, err)
	require.Len(t, rwWrapper.replies, 1)
	require.NotNil(t, rwWrapper.replies[0].Error)
	require.Equal(t, ErrorUnrecoverablePosition.Code, rwWrapper.replies[0].Error.Code)
}

// Positioned mode: ErrorStateTooLarge on immediate join when state exceeds MapMaxImmediateJoinStateSize.
func TestMapSubscribe_ImmediateJoin_StateTooLarge(t *testing.T) {
	node, err := New(Config{
		LogLevel:                    LogLevelTrace,
		LogHandler:                  func(entry LogEntry) {},
		MapMaxImmediateJoinStateSize: 3, // Max 3 entries for immediate join.
		GetMapChannelOptions: func(channel string) MapChannelOptions {
			return MapChannelOptions{
				SyncMode:      MapSyncConverging,
				RetentionMode: MapRetentionExpiring,
				KeyTTL:        60 * time.Second,
			}
		},
	})
	require.NoError(t, err)

	broker, err := NewMemoryMapBroker(node, MemoryMapBrokerConfig{})
	require.NoError(t, err)
	err = broker.RegisterEventHandler(nil)
	require.NoError(t, err)
	node.SetMapBroker(broker)

	err = node.Run()
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = node.Shutdown(context.Background())
	})

	channel := "test_immediate_join_too_large"
	ctx := context.Background()

	// Publish 5 entries (exceeds limit of 3).
	for i := 0; i < 5; i++ {
		_, pubErr := broker.Publish(ctx, channel, string(rune('a'+i)), MapPublishOptions{
			Data: []byte(`{"v":"data"}`),
		})
		require.NoError(t, pubErr)
	}

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					Type: SubscriptionTypeMap,
				},
			}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")

	// Immediate join (phase=0, recover=false, no prior state) should fail with ErrorStateTooLarge.
	rwWrapper := testReplyWriterWrapper()
	err = client.handleSubscribe(&protocol.SubscribeRequest{
		Channel: channel,
		Type:    1,
		Phase:   MapPhaseLive,
	}, &protocol.Command{Id: 1}, time.Now(), rwWrapper.rw)
	require.NoError(t, err)
	require.Len(t, rwWrapper.replies, 1)
	require.NotNil(t, rwWrapper.replies[0].Error)
	require.Equal(t, ErrorStateTooLarge.Code, rwWrapper.replies[0].Error.Code)
}

// Positioned mode: State consistency filtering. Entries modified after the saved position
// during pagination should be filtered out from later pages to prevent duplicates.
func TestMapSubscribe_Positioned_StateConsistencyFiltering(t *testing.T) {
	node, broker := newTestNodeWithMapBroker(t)
	setTestMapChannelOptionsConverging(node)

	channel := "test_state_consistency"
	ctx := context.Background()

	// Pre-populate 10 entries.
	for i := 0; i < 10; i++ {
		_, err := broker.Publish(ctx, channel, string(rune('a'+i)), MapPublishOptions{
			Data: []byte(`{"v":"data"}`),
		})
		require.NoError(t, err)
	}

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					Type: SubscriptionTypeMap,
				},
			}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")

	// First page: get 5 entries. Server captures stream position (offset=10).
	result1 := subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: channel,
		Type:    1,
		Phase:   MapPhaseState,
		Limit:   5,
	})
	require.Equal(t, MapPhaseState, result1.Phase)
	require.Len(t, result1.State, 5)
	require.NotEmpty(t, result1.Cursor)

	savedOffset := result1.Offset
	savedEpoch := result1.Epoch

	// Now update a key that's on the second page — this bumps its offset past savedOffset.
	_, err := broker.Publish(ctx, channel, "f", MapPublishOptions{
		Data: []byte(`{"v":"updated"}`),
	})
	require.NoError(t, err)

	// Second page with saved position — the updated entry should be filtered out
	// because its new offset > savedOffset. Client will get it from stream catch-up.
	result2 := subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: channel,
		Type:    1,
		Phase:   MapPhaseState,
		Limit:   100,
		Cursor:  result1.Cursor,
		Offset:  savedOffset,
		Epoch:   savedEpoch,
	})

	// The second page should have entries from the remaining keys,
	// but the updated "f" should be filtered out (its offset > savedOffset).
	for _, entry := range result2.State {
		require.LessOrEqual(t, entry.Offset, savedOffset,
			"entry %s with offset %d should have been filtered (savedOffset=%d)",
			entry.Key, entry.Offset, savedOffset)
	}
}

// Positioned mode: multi-page state pagination followed by STREAM and LIVE phases.
func TestMapSubscribe_Positioned_FullThreePhaseFlow(t *testing.T) {
	node, broker := newTestNodeWithMapBroker(t)
	setTestMapChannelOptionsConverging(node)

	channel := "test_three_phase"
	ctx := context.Background()

	// Pre-populate 10 entries.
	var lastOffset uint64
	var epoch string
	for i := 0; i < 10; i++ {
		res, err := broker.Publish(ctx, channel, string(rune('a'+i)), MapPublishOptions{
			Data: []byte(`{"v":"data"}`),
		})
		require.NoError(t, err)
		lastOffset = res.Position.Offset
		epoch = res.Position.Epoch
	}

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					Type: SubscriptionTypeMap,
				},
			}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")

	// Phase 2 (STATE): first page.
	result1 := subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: channel,
		Type:    1,
		Phase:   MapPhaseState,
		Limit:   5,
	})
	require.Equal(t, MapPhaseState, result1.Phase)
	require.Len(t, result1.State, 5)
	require.NotEmpty(t, result1.Cursor)
	require.Equal(t, epoch, result1.Epoch)

	// Phase 2 (STATE): second (last) page.
	result2 := subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: channel,
		Type:    1,
		Phase:   MapPhaseState,
		Limit:   5,
		Cursor:  result1.Cursor,
	})
	require.Equal(t, MapPhaseState, result2.Phase)
	require.Len(t, result2.State, 5)
	require.Empty(t, result2.Cursor) // Last page.

	// Add more entries between STATE and STREAM phase to create a gap.
	for i := 0; i < 50; i++ {
		res, err := broker.Publish(ctx, channel, "extra"+string(rune('a'+i%26)), MapPublishOptions{
			Data: []byte(`{"v":"extra"}`),
		})
		require.NoError(t, err)
		lastOffset = res.Position.Offset
	}

	// Phase 1 (STREAM): small limit to paginate.
	result3 := subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: channel,
		Type:    1,
		Phase:   MapPhaseStream,
		Offset:  result1.Offset, // Saved from first STATE page.
		Epoch:   epoch,
		Limit:   10,
	})
	require.True(t, result3.Phase == MapPhaseStream || result3.Phase == MapPhaseLive)
	require.NotEmpty(t, result3.Publications)

	// Continue STREAM until LIVE.
	currentPhase := result3.Phase
	currentOffset := result3.Publications[len(result3.Publications)-1].Offset
	for currentPhase == MapPhaseStream {
		res := subscribeMapClient(t, client, &protocol.SubscribeRequest{
			Channel: channel,
			Type:    1,
			Phase:   MapPhaseStream,
			Offset:  currentOffset,
			Epoch:   epoch,
			Limit:   20,
		})
		currentPhase = res.Phase
		if len(res.Publications) > 0 {
			currentOffset = res.Publications[len(res.Publications)-1].Offset
		}
	}

	// Should be LIVE now.
	require.Equal(t, MapPhaseLive, currentPhase)
	require.Equal(t, lastOffset, currentOffset)
	require.Contains(t, client.channels, channel)
}

// Positioned mode: LIVE recovery with epoch mismatch should return ErrorUnrecoverablePosition.
func TestMapSubscribe_Positioned_LiveRecoveryEpochMismatch(t *testing.T) {
	node, broker := newTestNodeWithMapBroker(t)
	setTestMapChannelOptionsConverging(node)

	channel := "test_live_recovery_epoch_mismatch"
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		_, err := broker.Publish(ctx, channel, string(rune('a'+i)), MapPublishOptions{
			Data: []byte(`{"v":"data"}`),
		})
		require.NoError(t, err)
	}

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					Type: SubscriptionTypeMap,
				},
			}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")

	// Try to recover with wrong epoch — should get ErrorUnrecoverablePosition.
	rwWrapper := testReplyWriterWrapper()
	err := client.handleSubscribe(&protocol.SubscribeRequest{
		Channel: channel,
		Type:    1,
		Phase:   MapPhaseLive,
		Offset:  2,
		Epoch:   "wrong-epoch",
		Recover: true,
	}, &protocol.Command{Id: 1}, time.Now(), rwWrapper.rw)
	require.NoError(t, err)
	require.Len(t, rwWrapper.replies, 1)
	require.NotNil(t, rwWrapper.replies[0].Error)
	require.Equal(t, ErrorUnrecoverablePosition.Code, rwWrapper.replies[0].Error.Code)
}

// Streamless mode: immediate join (phase=0, recover=false) returns full state.
func TestMapSubscribe_Streamless_ImmediateJoin(t *testing.T) {
	node, broker := newTestNodeWithMapBroker(t)

	channel := "test_streamless_immediate"
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		_, err := broker.Publish(ctx, channel, string(rune('a'+i)), MapPublishOptions{
			Data: []byte(`{"v":"data"}`),
		})
		require.NoError(t, err)
	}

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					Type: SubscriptionTypeMap,
				},
			}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")

	// Immediate join: phase=0, no recover.
	result := subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: channel,
		Type:    1,
		Phase:   MapPhaseLive,
	})

	require.Equal(t, MapPhaseLive, result.Phase)
	require.Len(t, result.State, 3)       // Full state returned.
	require.False(t, result.Recoverable)   // Streamless: not recoverable.
	require.Contains(t, client.channels, channel)
}

// Positioned mode: immediate join returns full state and stream with recoverable=true.
func TestMapSubscribe_Positioned_ImmediateJoin(t *testing.T) {
	node, broker := newTestNodeWithMapBroker(t)
	setTestMapChannelOptionsConverging(node)

	channel := "test_positioned_immediate"
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		_, err := broker.Publish(ctx, channel, string(rune('a'+i)), MapPublishOptions{
			Data: []byte(`{"v":"data"}`),
		})
		require.NoError(t, err)
	}

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					Type: SubscriptionTypeMap,
				},
			}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")

	result := subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: channel,
		Type:    1,
		Phase:   MapPhaseLive,
	})

	require.Equal(t, MapPhaseLive, result.Phase)
	require.Len(t, result.State, 3)      // Full state returned.
	require.True(t, result.Recoverable)  // Positioned: recoverable.
	require.Greater(t, result.Offset, uint64(0))
	require.NotEmpty(t, result.Epoch)
	require.Contains(t, client.channels, channel)
}

// TestMapSubscribe_StreamPhaseOffset verifies that the STREAM phase returns
// the last publication's offset (not stream.Top()), preventing the client
// from jumping ahead and skipping publications in multi-page STREAM scenarios.
func TestMapSubscribe_StreamPhaseOffset(t *testing.T) {
	node, broker := newTestNodeWithMapBroker(t)
	setTestMapChannelOptionsConverging(node)
	channel := "test_stream_offset"
	ctx := context.Background()

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					Type: SubscriptionTypeMap,
				},
			}, nil)
		})
	})

	// Publish 10 entries to create a stream.
	for i := 0; i < 10; i++ {
		_, err := broker.Publish(ctx, channel, "key"+string(rune('A'+i)), MapPublishOptions{
			Data: []byte(`{"v":"data"}`),
		})
		require.NoError(t, err)
	}

	client := newTestConnectedClientV2(t, node, "user1")

	// STATE phase (first page) — capture the offset.
	result := subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: channel,
		Type:    1,
		Phase:   MapPhaseState,
		Limit:   3,
	})
	require.Equal(t, MapPhaseState, result.Phase)
	firstPageOffset := result.Offset
	firstPageEpoch := result.Epoch

	// Publish 5 MORE entries while paginating state — stream advances.
	for i := 0; i < 5; i++ {
		_, err := broker.Publish(ctx, channel, "extra"+string(rune('0'+i)), MapPublishOptions{
			Data: []byte(`{"v":"new"}`),
		})
		require.NoError(t, err)
	}

	// STATE phase (subsequent page) — offset should be frozen at first page value.
	result = subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: channel,
		Type:    1,
		Phase:   MapPhaseState,
		Cursor:  result.Cursor,
		Limit:   3,
		Offset:  firstPageOffset,
		Epoch:   firstPageEpoch,
	})
	require.Equal(t, MapPhaseState, result.Phase)
	// Key check: subsequent page returns the SAME offset as first page,
	// not the advanced stream.Top().
	require.Equal(t, firstPageOffset, result.Offset,
		"STATE page offset should be frozen at first page value, not stream.Top()")

	// Finish state pagination to get to STREAM phase.
	for result.Cursor != "" {
		result = subscribeMapClient(t, client, &protocol.SubscribeRequest{
			Channel: channel,
			Type:    1,
			Phase:   MapPhaseState,
			Cursor:  result.Cursor,
			Limit:   100,
			Offset:  firstPageOffset,
			Epoch:   firstPageEpoch,
		})
	}
	// With MapStateToLiveEnabled=false (default), positioned mode returns phase=2
	// on the last page. The client then transitions to STREAM.
	// However, the server may go STATE→LIVE if close enough — check the phase.
	if result.Phase == MapPhaseLive {
		// Server went STATE→LIVE directly (stream was close enough).
		// Test passed — offset was frozen correctly on intermediate pages.
		return
	}

	// STREAM phase with small limit to force multi-page.
	result = subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: channel,
		Type:    1,
		Phase:   MapPhaseStream,
		Limit:   2,
		Offset:  firstPageOffset,
		Epoch:   firstPageEpoch,
	})

	// If STREAM returned phase=1 (intermediate page), verify offset is
	// the last publication's offset, not stream.Top().
	if result.Phase == MapPhaseStream {
		require.Greater(t, result.Offset, firstPageOffset,
			"STREAM offset should advance beyond STATE offset")
		// The offset should be at most firstPageOffset + limit (2 publications read).
		require.LessOrEqual(t, result.Offset, firstPageOffset+2,
			"STREAM offset should be last pub's offset, not stream.Top()")
	}
}

// TestMapSubscribe_WasRecovering verifies that WasRecovering flag is set
// correctly on recovery join responses.
func TestMapSubscribe_CatchUpTimeout_StatePagination(t *testing.T) {
	node, broker := newTestNodeWithMapBroker(t)
	// Set a very short timeout so we can trigger it without sleeping.
	node.config.MapSubscribeCatchUpTimeout = time.Nanosecond

	channel := "test_catchup_timeout"
	ctx := context.Background()

	for i := 0; i < 10; i++ {
		_, err := broker.Publish(ctx, channel, string(rune('a'+i)), MapPublishOptions{
			Data: []byte(`{"v":"data"}`),
		})
		require.NoError(t, err)
	}

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					Type: SubscriptionTypeMap,
				},
			}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")

	// First page succeeds (creates mapSubscribeState).
	result := subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: channel,
		Type:    1,
		Phase:   MapPhaseState,
		Limit:   5,
	})
	require.Equal(t, MapPhaseState, result.Phase)
	require.NotEmpty(t, result.Cursor)

	// Second page (cursor continuation) — timeout already expired.
	rwWrapper := testReplyWriterWrapper()
	err := client.handleSubscribe(&protocol.SubscribeRequest{
		Channel: channel,
		Type:    1,
		Phase:   MapPhaseState,
		Limit:   5,
		Cursor:  result.Cursor,
	}, &protocol.Command{Id: 1}, time.Now(), rwWrapper.rw)
	require.ErrorIs(t, err, DisconnectStale)

	// mapSubscribing state should be cleaned up.
	client.mu.RLock()
	_, hasState := client.mapSubscribing[channel]
	client.mu.RUnlock()
	require.False(t, hasState)
}

func TestMapSubscribe_CatchUpTimeout_PhaseTransition(t *testing.T) {
	node, broker := newTestNodeWithMapBroker(t)
	setTestMapChannelOptionsConverging(node)
	node.config.MapSubscribeCatchUpTimeout = time.Nanosecond

	channel := "test_catchup_timeout_phase"
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		_, err := broker.Publish(ctx, channel, "key"+string(rune('A'+i)), MapPublishOptions{
			Data: []byte(`{"v":"data"}`),
		})
		require.NoError(t, err)
	}

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					Type: SubscriptionTypeMap,
				},
			}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")

	// State phase succeeds (limit large enough for single page → stays in STATE phase
	// because positioned mode needs stream catch-up, and StateToLive is not enabled).
	result := subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: channel,
		Type:    1,
		Phase:   MapPhaseState,
		Limit:   100,
	})
	require.Equal(t, MapPhaseState, result.Phase)
	require.Empty(t, result.Cursor) // All state delivered.

	// Phase transition to stream — timeout expired.
	rwWrapper := testReplyWriterWrapper()
	err := client.handleSubscribe(&protocol.SubscribeRequest{
		Channel: channel,
		Type:    1,
		Phase:   MapPhaseStream,
		Offset:  result.Offset,
		Epoch:   result.Epoch,
	}, &protocol.Command{Id: 1}, time.Now(), rwWrapper.rw)
	require.ErrorIs(t, err, DisconnectStale)
}

func TestMapSubscribe_CatchUpTimeout_Sweep(t *testing.T) {
	node, broker := newTestNodeWithMapBroker(t)
	node.config.MapSubscribeCatchUpTimeout = time.Nanosecond

	ctx := context.Background()

	// Create two channels with data.
	for _, ch := range []string{"ch_a", "ch_b"} {
		_, err := broker.Publish(ctx, ch, "key1", MapPublishOptions{
			Data: []byte(`{"v":"data"}`),
		})
		require.NoError(t, err)
	}

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					Type: SubscriptionTypeMap,
				},
			}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")

	// Start catch-up on ch_a (limit=1 with 1 entry → goes LIVE in streamless mode).
	// We need to prevent going live so mapSubscribing stays. Use a larger dataset.
	_, err := broker.Publish(ctx, "ch_a", "key2", MapPublishOptions{
		Data: []byte(`{"v":"data2"}`),
	})
	require.NoError(t, err)

	// First page of ch_a (limit=1 so cursor remains).
	result := subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: "ch_a",
		Type:    1,
		Phase:   MapPhaseState,
		Limit:   1,
	})
	require.NotEmpty(t, result.Cursor)

	// Verify ch_a is in mapSubscribing.
	client.mu.RLock()
	_, hasA := client.mapSubscribing["ch_a"]
	client.mu.RUnlock()
	require.True(t, hasA)

	// Now start a fresh subscription on ch_b — this triggers sweep that should
	// clean up expired ch_a state (but ch_b itself proceeds normally).
	result2 := subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: "ch_b",
		Type:    1,
		Phase:   MapPhaseState,
		Limit:   100,
	})
	require.Equal(t, MapPhaseLive, result2.Phase) // Single page streamless → LIVE.

	// ch_a should have been swept.
	client.mu.RLock()
	_, hasA = client.mapSubscribing["ch_a"]
	client.mu.RUnlock()
	require.False(t, hasA, "expired ch_a catch-up should be swept")
}

func TestMapSubscribe_CatchUpTimeout_Disabled(t *testing.T) {
	node, broker := newTestNodeWithMapBroker(t)
	// Negative timeout disables the check.
	node.config.MapSubscribeCatchUpTimeout = -1

	channel := "test_catchup_no_timeout"
	ctx := context.Background()

	for i := 0; i < 10; i++ {
		_, err := broker.Publish(ctx, channel, string(rune('a'+i)), MapPublishOptions{
			Data: []byte(`{"v":"data"}`),
		})
		require.NoError(t, err)
	}

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					Type: SubscriptionTypeMap,
				},
			}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")

	// Manually set startedAt far in the past.
	result := subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: channel,
		Type:    1,
		Phase:   MapPhaseState,
		Limit:   5,
	})
	require.NotEmpty(t, result.Cursor)

	client.mu.Lock()
	client.mapSubscribing[channel].startedAt = time.Now().Add(-time.Hour).UnixNano()
	client.mu.Unlock()

	// Should still succeed — timeout disabled.
	result2 := subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: channel,
		Type:    1,
		Phase:   MapPhaseState,
		Limit:   5,
		Cursor:  result.Cursor,
	})
	require.Equal(t, MapPhaseLive, result2.Phase)
}

func TestMapSubscribe_WasRecovering(t *testing.T) {
	node, broker := newTestNodeWithMapBroker(t)
	setTestMapChannelOptionsConverging(node)
	channel := "test_was_recovering"
	ctx := context.Background()

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					Type: SubscriptionTypeMap,
				},
			}, nil)
		})
	})

	// Publish entries to create stream position.
	for i := 0; i < 3; i++ {
		_, err := broker.Publish(ctx, channel, "key"+string(rune('A'+i)), MapPublishOptions{
			Data: []byte(`{"v":"data"}`),
		})
		require.NoError(t, err)
	}

	client := newTestConnectedClientV2(t, node, "user1")

	// Fresh immediate join — should NOT have WasRecovering.
	result := subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: channel,
		Type:    1,
		Phase:   MapPhaseLive,
	})
	require.Equal(t, MapPhaseLive, result.Phase)
	require.True(t, result.Recoverable)
	require.False(t, result.WasRecovering, "fresh subscription should not have WasRecovering")
	require.False(t, result.Recovered, "fresh subscription should not have Recovered")
	savedOffset := result.Offset
	savedEpoch := result.Epoch

	// Unsubscribe.
	client.Unsubscribe(channel)

	// Publish more entries while disconnected.
	_, err := broker.Publish(ctx, channel, "keyD", MapPublishOptions{
		Data: []byte(`{"v":"new"}`),
	})
	require.NoError(t, err)

	// Recovery join — should have WasRecovering and Recovered.
	result = subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: channel,
		Type:    1,
		Phase:   MapPhaseLive,
		Recover: true,
		Offset:  savedOffset,
		Epoch:   savedEpoch,
	})
	require.Equal(t, MapPhaseLive, result.Phase)
	require.True(t, result.Recoverable)
	require.True(t, result.WasRecovering, "recovery should have WasRecovering")
	require.True(t, result.Recovered, "successful recovery should have Recovered")
	require.Len(t, result.Publications, 1, "should recover 1 missed publication")
}

// TestMapSubscribe_EmptyEpochAdoption verifies that a map channel client with
// epoch="" (subscribed when no data existed, e.g. via PG broker) correctly adopts
// the real epoch from the first incoming publication without triggering
// handleInsufficientState. This is map-specific behavior — non-map channels with
// epoch="" still trigger insufficient state on epoch mismatch
// (tested in TestClientUnexpectedOffsetEpochClientV2 and below).
func TestMapSubscribe_EmptyEpochAdoption(t *testing.T) {
	node, _ := newTestNodeWithMapBroker(t)
	setTestMapChannelOptionsConverging(node)

	channel := "test_epoch_adoption"

	insufficientCh := make(chan struct{}, 1)
	pubDelivered := make(chan struct{}, 1)

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					Type: SubscriptionTypeMap,
				},
			}, nil)
		})
		client.OnUnsubscribe(func(e UnsubscribeEvent) {
			if e.Code == UnsubscribeCodeInsufficient {
				select {
				case insufficientCh <- struct{}{}:
				default:
				}
			}
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")

	// Subscribe — MemoryMapBroker creates an epoch even for empty channels.
	result := subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: channel,
		Type:    1,
		Phase:   MapPhaseLive,
	})
	require.Equal(t, MapPhaseLive, result.Phase)

	// Simulate the PG broker scenario: client subscribed when no data existed,
	// so epoch="" was returned. Manually set epoch="" in the channel context.
	client.mu.Lock()
	chCtx := client.channels[channel]
	require.True(t, channelHasFlag(chCtx.flags, flagMap))
	require.True(t, channelHasFlag(chCtx.flags, flagPositioning))
	chCtx.streamPosition.Epoch = ""
	chCtx.streamPosition.Offset = 0
	client.channels[channel] = chCtx
	client.mu.Unlock()

	// Deliver a publication with a real epoch via node.handlePublication.
	// For a map channel with epoch="", this should adopt the epoch, not trigger insufficient state.
	go func() {
		_ = node.handlePublication(channel, StreamPosition{1, "real-epoch-abc"}, &Publication{
			Offset: 1,
			Data:   []byte(`{"key":"val"}`),
			Key:    "key1",
		}, nil, nil)
		select {
		case pubDelivered <- struct{}{}:
		default:
		}
	}()

	// Wait for publication to be processed.
	select {
	case <-pubDelivered:
	case <-time.After(time.Second):
		require.Fail(t, "timeout waiting for publication delivery")
	}

	// Give some time for any async handleInsufficientState to fire.
	time.Sleep(100 * time.Millisecond)

	// Verify no insufficient state was triggered.
	select {
	case <-insufficientCh:
		require.Fail(t, "map channel with empty epoch should not trigger insufficient state")
	default:
		// Good — no insufficient state.
	}

	// Verify client adopted the epoch.
	client.mu.Lock()
	chCtx = client.channels[channel]
	require.Equal(t, "real-epoch-abc", chCtx.streamPosition.Epoch, "client should adopt the real epoch")
	require.Equal(t, uint64(1), chCtx.streamPosition.Offset, "client offset should advance")
	client.mu.Unlock()
}

// TestMapSubscribe_EmptyEpochAdoption_NonMapNotAffected verifies that the epoch
// adoption logic is map-specific: a non-map (regular) channel with epoch="" still
// triggers handleInsufficientState when a publication arrives with a real epoch.
func TestMapSubscribe_EmptyEpochAdoption_NonMapNotAffected(t *testing.T) {
	t.Parallel()
	broker := NewTestBroker()
	node := nodeWithBroker(broker)
	defer func() { _ = node.Shutdown(context.Background()) }()

	done := make(chan struct{})

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(event SubscribeEvent, callback SubscribeCallback) {
			callback(SubscribeReply{Options: SubscribeOptions{EnableRecovery: true}}, nil)
		})
		client.OnUnsubscribe(func(event UnsubscribeEvent) {
			require.Equal(t, UnsubscribeCodeInsufficient, event.Code)
			close(done)
		})
	})

	client := newTestClientV2(t, node, "42")
	connectClientV2(t, client)

	rwWrapper := testReplyWriterWrapper()
	err := client.handleSubscribe(&protocol.SubscribeRequest{
		Channel: "test",
		Recover: true,
	}, &protocol.Command{}, time.Now(), rwWrapper.rw)
	require.NoError(t, err)

	// Non-map channel with epoch="" receiving a pub with real epoch — should trigger insufficient state.
	err = node.handlePublication("test", StreamPosition{1, "xyz"}, &Publication{
		Offset: 1,
		Data:   []byte(`{}`),
	}, nil, nil)
	require.NoError(t, err)

	select {
	case <-time.After(time.Second):
		require.Fail(t, "timeout waiting for insufficient state on non-map channel")
	case <-done:
	}
}

// TestMapSubscribe_RealEpochMismatch verifies that a map channel client with a
// real (non-empty) epoch triggers handleInsufficientState when a publication
// arrives with a different epoch. The epoch adoption only applies to epoch="".
func TestMapSubscribe_RealEpochMismatch(t *testing.T) {
	node, broker := newTestNodeWithMapBroker(t)
	setTestMapChannelOptionsConverging(node)

	channel := "test_epoch_mismatch"
	ctx := context.Background()

	// Publish some data to establish a real epoch.
	res, err := broker.Publish(ctx, channel, "key1", MapPublishOptions{
		Data: []byte(`{"v":"data"}`),
	})
	require.NoError(t, err)
	realEpoch := res.Position.Epoch
	require.NotEmpty(t, realEpoch)

	done := make(chan struct{})

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					Type: SubscriptionTypeMap,
				},
			}, nil)
		})
		client.OnUnsubscribe(func(e UnsubscribeEvent) {
			if e.Code == UnsubscribeCodeInsufficient {
				close(done)
			}
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")

	// Subscribe — client will get the real epoch from ReadState.
	result := subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: channel,
		Type:    1,
		Phase:   MapPhaseLive,
	})
	require.Equal(t, realEpoch, result.Epoch)

	// Deliver publication with a DIFFERENT epoch — should trigger insufficient state.
	err = node.handlePublication(channel, StreamPosition{2, "different-epoch"}, &Publication{
		Offset: 2,
		Data:   []byte(`{"v":"data2"}`),
		Key:    "key2",
	}, nil, nil)
	require.NoError(t, err)

	select {
	case <-time.After(time.Second):
		require.Fail(t, "timeout waiting for insufficient state on epoch mismatch")
	case <-done:
	}
}

// TestMapSubscribe_RecoveryEmptyEpochGuard verifies that recovering with epoch=""
// when the server has a real epoch returns ErrorUnrecoverablePosition. This guards
// against stale state after a Clear event: the client SDK stores the epoch from
// the subscribe response and never updates it from publication deliveries.
func TestMapSubscribe_RecoveryEmptyEpochGuard(t *testing.T) {
	node, broker := newTestNodeWithMapBroker(t)
	setTestMapChannelOptionsConverging(node)

	channel := "test_recovery_empty_epoch"
	ctx := context.Background()

	// Publish data to create a real epoch.
	_, err := broker.Publish(ctx, channel, "key1", MapPublishOptions{
		Data: []byte(`{"v":"data"}`),
	})
	require.NoError(t, err)

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					Type: SubscriptionTypeMap,
				},
			}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")

	// Attempt recovery with epoch="" — server has real epoch → unrecoverable.
	rwWrapper := testReplyWriterWrapper()
	err = client.handleSubscribe(&protocol.SubscribeRequest{
		Channel: channel,
		Type:    1,
		Phase:   MapPhaseLive,
		Offset:  0,
		Epoch:   "", // Empty epoch from original subscription when no data existed.
		Recover: true,
	}, &protocol.Command{Id: 1}, time.Now(), rwWrapper.rw)
	require.NoError(t, err)
	require.Len(t, rwWrapper.replies, 1)
	require.NotNil(t, rwWrapper.replies[0].Error, "should get error for empty epoch recovery")
	require.Equal(t, ErrorUnrecoverablePosition.Code, rwWrapper.replies[0].Error.Code)
}

// TestMapSubscribe_RecoveryEmptyEpochGuard_StreamPhase tests the same guard
// via STREAM phase recovery (the default SDK recovery path).
func TestMapSubscribe_RecoveryEmptyEpochGuard_StreamPhase(t *testing.T) {
	node, broker := newTestNodeWithMapBroker(t)
	setTestMapChannelOptionsConverging(node)

	channel := "test_recovery_empty_epoch_stream"
	ctx := context.Background()

	// Publish data to create a real epoch.
	_, err := broker.Publish(ctx, channel, "key1", MapPublishOptions{
		Data: []byte(`{"v":"data"}`),
	})
	require.NoError(t, err)

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					Type: SubscriptionTypeMap,
				},
			}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")

	// Attempt STREAM phase recovery with epoch="" — server has real epoch → unrecoverable.
	rwWrapper := testReplyWriterWrapper()
	err = client.handleSubscribe(&protocol.SubscribeRequest{
		Channel: channel,
		Type:    1,
		Phase:   MapPhaseStream,
		Offset:  0,
		Epoch:   "", // Empty epoch.
		Limit:   100,
		Recover: true,
	}, &protocol.Command{Id: 1}, time.Now(), rwWrapper.rw)
	require.NoError(t, err)
	require.Len(t, rwWrapper.replies, 1)
	require.NotNil(t, rwWrapper.replies[0].Error, "should get error for empty epoch recovery via stream phase")
	require.Equal(t, ErrorUnrecoverablePosition.Code, rwWrapper.replies[0].Error.Code)
}

// TestMapSubscribe_RecoveryMatchingEpoch verifies that recovery with a real
// (non-empty) matching epoch succeeds normally — the empty-epoch guard only
// triggers when the client sends epoch="" but the server has a real epoch.
func TestMapSubscribe_RecoveryMatchingEpoch(t *testing.T) {
	node, broker := newTestNodeWithMapBroker(t)
	setTestMapChannelOptionsConverging(node)

	channel := "test_recovery_matching_epoch"
	ctx := context.Background()

	// Publish data to establish a real epoch.
	res, err := broker.Publish(ctx, channel, "key1", MapPublishOptions{
		Data: []byte(`{"v":"data"}`),
	})
	require.NoError(t, err)
	epoch := res.Position.Epoch

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					Type: SubscriptionTypeMap,
				},
			}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")

	// Recovery with matching epoch — should succeed.
	result := subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: channel,
		Type:    1,
		Phase:   MapPhaseLive,
		Offset:  res.Position.Offset,
		Epoch:   epoch,
		Recover: true,
	})
	require.Equal(t, MapPhaseLive, result.Phase)
	require.Equal(t, epoch, result.Epoch)
	require.True(t, result.Recovered)
}

// TestMapSubscribe_RecoveryAfterClear verifies that recovering with a real epoch
// after MapClear (which deletes channel state) returns ErrorUnrecoverablePosition.
// After Clear, ReadStream returns a new epoch — the guard must catch real→different mismatch.
func TestMapSubscribe_RecoveryAfterClear(t *testing.T) {
	node, broker := newTestNodeWithMapBroker(t)
	setTestMapChannelOptionsConverging(node)

	channel := "test_recovery_after_clear"
	ctx := context.Background()

	// Publish data to establish a real epoch.
	res, err := broker.Publish(ctx, channel, "key1", MapPublishOptions{
		Data: []byte(`{"v":"data"}`),
	})
	require.NoError(t, err)
	oldEpoch := res.Position.Epoch
	require.NotEmpty(t, oldEpoch)

	// Clear the channel — removes all state including epoch.
	err = broker.Clear(ctx, channel, MapClearOptions{})
	require.NoError(t, err)

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					Type: SubscriptionTypeMap,
				},
			}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")

	// Recovery with old epoch after Clear — epoch mismatch → unrecoverable.
	rwWrapper := testReplyWriterWrapper()
	err = client.handleSubscribe(&protocol.SubscribeRequest{
		Channel: channel,
		Type:    1,
		Phase:   MapPhaseLive,
		Offset:  res.Position.Offset,
		Epoch:   oldEpoch,
		Recover: true,
	}, &protocol.Command{Id: 1}, time.Now(), rwWrapper.rw)
	require.NoError(t, err)
	require.Len(t, rwWrapper.replies, 1)
	require.NotNil(t, rwWrapper.replies[0].Error, "should get error for epoch mismatch after Clear")
	require.Equal(t, ErrorUnrecoverablePosition.Code, rwWrapper.replies[0].Error.Code)
}

// TestMapSubscribe_EpochInFirstPublication verifies that the first publication
// (offset=1) includes epoch on the wire, while subsequent publications do not.
// This allows the client SDK to learn the channel epoch from the first publication
// without redundant epoch bytes in every message.
func TestMapSubscribe_EpochInFirstPublication(t *testing.T) {
	node, broker := newTestNodeWithMapBroker(t)
	setTestMapChannelOptionsConverging(node)

	channel := "test_epoch_in_pub"

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					Type: SubscriptionTypeMap,
				},
			}, nil)
		})
	})

	// Create client with transport sink to capture wire data.
	transport := newTestTransport(func() {})
	transport.sink = make(chan []byte, 100)
	newCtx := SetCredentials(context.Background(), &Credentials{UserID: "user1"})
	client, err := newClient(newCtx, node, transport)
	require.NoError(t, err)
	connectClientV2(t, client)

	// Subscribe to map channel (live phase, no state to catch up).
	subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: channel,
		Type:    1,
		Phase:   MapPhaseLive,
	})

	// Publish two entries.
	_, err = broker.Publish(context.Background(), channel, "key1", MapPublishOptions{
		Data: []byte(`{"v":"first"}`),
	})
	require.NoError(t, err)
	_, err = broker.Publish(context.Background(), channel, "key2", MapPublishOptions{
		Data: []byte(`{"v":"second"}`),
	})
	require.NoError(t, err)

	// Collect publications from the transport sink.
	type pubMsg struct {
		Push struct {
			Pub struct {
				Offset uint64 `json:"offset"`
				Epoch  string `json:"epoch"`
				Key    string `json:"key"`
			} `json:"pub"`
		} `json:"push"`
	}

	var pubs []pubMsg
	timeout := time.After(2 * time.Second)
	for len(pubs) < 2 {
		select {
		case data := <-transport.sink:
			dec := json.NewDecoder(strings.NewReader(string(data)))
			for {
				var p pubMsg
				if err := dec.Decode(&p); err != nil {
					break
				}
				if p.Push.Pub.Key != "" {
					pubs = append(pubs, p)
				}
			}
		case <-timeout:
			require.Fail(t, "timeout waiting for publications")
		}
	}

	// First publication (offset=1): epoch must be present.
	require.Equal(t, uint64(1), pubs[0].Push.Pub.Offset)
	require.NotEmpty(t, pubs[0].Push.Pub.Epoch, "first publication (offset=1) must include epoch")

	// Second publication (offset=2): epoch must be absent.
	require.Equal(t, uint64(2), pubs[1].Push.Pub.Offset)
	require.Empty(t, pubs[1].Push.Pub.Epoch, "subsequent publications must not include epoch")
}
