package centrifuge

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"
	"sync"
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
		Map: MapConfig{
			GetMapChannelOptions: func(channel string) MapChannelOptions {
				return MapChannelOptions{
					Mode:        MapModeEphemeral,
					KeyTTL:      60 * time.Second,
					MinPageSize: 1, // Allow small page sizes in tests.
				}
			},
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
	node.config.Map.GetMapChannelOptions = func(channel string) MapChannelOptions {
		return MapChannelOptions{
			Mode:        MapModeRecoverable,
			KeyTTL:      60 * time.Second,
			MinPageSize: 1, // Allow small page sizes in tests.
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

// drainSink reads all available messages from a transport sink channel.
func drainSink(sink chan []byte) [][]byte {
	var messages [][]byte
	for {
		select {
		case msg := <-sink:
			messages = append(messages, msg)
		default:
			return messages
		}
	}
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
	t.Parallel()
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
		Type:    int32(SubscriptionTypeMap),
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
	t.Parallel()
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
		Type:    int32(SubscriptionTypeMap),
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
		Type:    int32(SubscriptionTypeMap),
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
	t.Parallel()
	// Test that STREAM phase is used when the stream advances too far during
	// STATE pagination for state-to-live to kick in.
	node, broker := newTestNodeWithMapBroker(t)
	setTestMapChannelOptionsConverging(node)

	channel := "test_stream"
	ctx := context.Background()

	// Pre-populate 4 entries.
	for i := 0; i < 4; i++ {
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

	// STATE page 1 (limit=2 → 2 entries, cursor). Offset frozen at 4.
	result1 := subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: channel,
		Type:    int32(SubscriptionTypeMap),
		Phase:   MapPhaseState,
		Limit:   2,
	})
	require.Equal(t, MapPhaseState, result1.Phase)
	require.NotEmpty(t, result1.Cursor)
	frozenOffset := result1.Offset
	epoch := result1.Epoch

	// Update EXISTING keys between STATE pages to advance stream offset without
	// adding new state entries. Creates a gap too large for state-to-live.
	var lastOffset uint64
	for i := 0; i < 100; i++ {
		res, err := broker.Publish(ctx, channel, string(rune('a'+i%4)), MapPublishOptions{
			Data: []byte(fmt.Sprintf(`{"v":"update_%d"}`, i)),
		})
		require.NoError(t, err)
		lastOffset = res.Position.Offset
	}

	// STATE page 2 (last page): state-to-live check fails (gap too large) → stays STATE.
	result2 := subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: channel,
		Type:    int32(SubscriptionTypeMap),
		Phase:   MapPhaseState,
		Limit:   2,
		Cursor:  result1.Cursor,
		Offset:  frozenOffset,
		Epoch:   epoch,
	})
	require.Equal(t, MapPhaseState, result2.Phase)
	require.Empty(t, result2.Cursor) // Last page.

	// Now do STREAM phase from frozen offset → should catch up and go LIVE.
	result3 := subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: channel,
		Type:    int32(SubscriptionTypeMap),
		Phase:   MapPhaseStream,
		Offset:  frozenOffset,
		Epoch:   epoch,
		Limit:   200,
	})
	require.True(t, result3.Type == 1)
	require.Equal(t, MapPhaseLive, result3.Phase)
	require.Len(t, result3.Publications, 100)
	require.Equal(t, lastOffset, result3.Offset)
}

func TestMapSubscribe_LivePhase(t *testing.T) {
	t.Parallel()
	// With always-on state-to-live, positioned mode STATE goes directly to LIVE
	// on the last page when stream is close. Verify client is properly subscribed.
	node, broker := newTestNodeWithMapBroker(t)
	setTestMapChannelOptionsConverging(node)

	channel := "test_live"
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

	// STATE with all entries fitting on one page → state-to-live goes LIVE directly.
	result := subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: channel,
		Type:    int32(SubscriptionTypeMap),
		Phase:   MapPhaseState,
		Limit:   100,
	})

	require.True(t, result.Type == 1)
	require.Equal(t, MapPhaseLive, result.Phase)
	require.NotEmpty(t, result.Epoch)
	require.Len(t, result.State, 1)

	// Client should now be subscribed.
	require.Contains(t, client.channels, channel)
	chCtx := client.channels[channel]
	require.True(t, channelHasFlag(chCtx.flags, flagMap))
	require.True(t, channelHasFlag(chCtx.flags, flagSubscribed))
}

func TestMapSubscribe_DirectLive(t *testing.T) {
	t.Parallel()
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

	// STATE phase: state fits in one page → streamless goes LIVE directly.
	result := subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: channel,
		Type:    int32(SubscriptionTypeMap),
		Phase:   MapPhaseState,
		Limit:   100,
	})

	require.True(t, result.Type == 1)
	require.Equal(t, MapPhaseLive, result.Phase)
	require.NotEmpty(t, result.Epoch)
	require.Contains(t, client.channels, channel)
}

func TestMapSubscribe_FullTwoPhase(t *testing.T) {
	t.Parallel()
	// With always-on state-to-live, positioned mode STATE goes directly to LIVE
	// on the last page when all entries fit. This effectively becomes a single step.
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
					Type: SubscriptionTypeMap,
				},
			}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")

	// STATE with all entries fitting on one page → state-to-live goes LIVE directly.
	result := subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: channel,
		Type:    int32(SubscriptionTypeMap),
		Phase:   MapPhaseState,
		Limit:   100,
	})

	require.True(t, result.Type == 1)
	require.Equal(t, MapPhaseLive, result.Phase) // Direct STATE→LIVE.
	require.Len(t, result.State, 5)
	require.Contains(t, client.channels, channel)
}

func TestMapSubscribe_NotEnabled(t *testing.T) {
	t.Parallel()
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
		Type:    int32(SubscriptionTypeMap),
		Phase:   MapPhaseState,
	}, &protocol.Command{Id: 1}, time.Now(), rwWrapper.rw)
	require.NoError(t, err)
	require.Len(t, rwWrapper.replies, 1)
	require.NotNil(t, rwWrapper.replies[0].Error)
	require.Equal(t, ErrorBadRequest.Code, rwWrapper.replies[0].Error.Code)
}

func TestMapSubscribe_AlreadySubscribed(t *testing.T) {
	t.Parallel()
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
		Type:    int32(SubscriptionTypeMap),
		Phase:   MapPhaseState,
		Limit:   100,
	})

	// Second subscribe should fail.
	rwWrapper := testReplyWriterWrapper()
	err := client.handleSubscribe(&protocol.SubscribeRequest{
		Channel: channel,
		Type:    int32(SubscriptionTypeMap),
		Phase:   MapPhaseState,
	}, &protocol.Command{Id: 1}, time.Now(), rwWrapper.rw)
	require.Equal(t, ErrorAlreadySubscribed, err)
}

func TestMapSubscribe_WithPresence(t *testing.T) {
	t.Parallel()
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
					Type:                     SubscriptionTypeMap,
					MapClientPresenceChannel: "clients:" + channel,
				},
			}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")

	// Subscribe with keyed client presence.
	subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: channel,
		Type:    int32(SubscriptionTypeMap),
		Phase:   MapPhaseState,
		Limit:   100,
	})

	// Verify presence was added to clients:{channel}.
	clientsChannel := "clients:" + channel
	stateRes, err := broker.ReadState(ctx, clientsChannel, MapReadStateOptions{
		Limit: 100,
	})
	require.NoError(t, err)
	entries, _, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
	require.Len(t, entries, 1)
	require.Equal(t, client.uid, entries[0].Key)

	// Verify flagMapClientPresence is set.
	require.Contains(t, client.channels, channel)
	chCtx := client.channels[channel]
	require.True(t, channelHasFlag(chCtx.flags, flagMapClientPresence))
	require.True(t, channelHasFlag(chCtx.flags, flagMap))
}

func TestMapSubscribe_PresenceCleanupOnUnsubscribe(t *testing.T) {
	t.Parallel()
	node, broker := newTestNodeWithMapBroker(t)

	channel := "test_presence_cleanup"
	ctx := context.Background()

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					Type:                     SubscriptionTypeMap,
					MapClientPresenceChannel: "clients:" + channel,
				},
			}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")

	// Subscribe with keyed client presence.
	subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: channel,
		Type:    int32(SubscriptionTypeMap),
		Phase:   MapPhaseState,
		Limit:   100,
	})

	// Verify presence exists in :clients channel.
	clientsChannel := "clients:" + channel
	stateRes, err := broker.ReadState(ctx, clientsChannel, MapReadStateOptions{
		Limit: 100,
	})
	require.NoError(t, err)
	require.Len(t, stateRes.Publications, 1)

	// Unsubscribe.
	client.Unsubscribe(channel)

	// Verify presence was removed.
	stateRes, err = broker.ReadState(ctx, clientsChannel, MapReadStateOptions{
		Limit: 100,
	})
	require.NoError(t, err)
	entries, _, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
	require.NoError(t, err)
	require.Len(t, entries, 0)
}

func TestMapSubscribe_PresenceCleanupOnDisconnect(t *testing.T) {
	t.Parallel()
	node, broker := newTestNodeWithMapBroker(t)

	channel := "test_presence_disconnect"
	ctx := context.Background()

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					Type:                     SubscriptionTypeMap,
					MapClientPresenceChannel: "clients:" + channel,
				},
			}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")

	// Subscribe with keyed client presence.
	subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: channel,
		Type:    int32(SubscriptionTypeMap),
		Phase:   MapPhaseState,
		Limit:   100,
	})

	// Verify presence exists in :clients channel.
	clientsChannel := "clients:" + channel
	stateRes, err := broker.ReadState(ctx, clientsChannel, MapReadStateOptions{
		Limit: 100,
	})
	require.NoError(t, err)
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
	require.NoError(t, err)
	entries, _, _ = stateRes.Publications, stateRes.Position, stateRes.Cursor
	require.NoError(t, err)
	require.Len(t, entries, 0)
}

func TestMapSubscribe_CleanupOnUnsubscribe(t *testing.T) {
	t.Parallel()
	node, broker := newTestNodeWithMapBroker(t)

	channel := "test_cleanup_on_unsub"
	ctx := context.Background()

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					Type:                         SubscriptionTypeMap,
					MapRemoveClientOnUnsubscribe: true,
				},
			}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")
	clientID := client.ID()

	// Subscribe with MapRemoveClientOnUnsubscribe.
	subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: channel,
		Type:    int32(SubscriptionTypeMap),
		Phase:   MapPhaseState,
		Limit:   100,
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
	require.NoError(t, err)
	entries, _, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
	require.Len(t, entries, 1)
	require.Equal(t, clientID, entries[0].Key)

	// Unsubscribe - should trigger cleanup of key=clientID.
	client.Unsubscribe(channel)

	// Verify key was removed.
	stateRes, err = broker.ReadState(ctx, channel, MapReadStateOptions{
		Limit: 100,
	})
	require.NoError(t, err)
	entries, _, _ = stateRes.Publications, stateRes.Position, stateRes.Cursor
	require.Len(t, entries, 0)
}

func TestMapSubscribe_CleanupOnDisconnect(t *testing.T) {
	t.Parallel()
	node, broker := newTestNodeWithMapBroker(t)

	channel := "test_cleanup_on_disconnect"
	ctx := context.Background()

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					Type:                         SubscriptionTypeMap,
					MapRemoveClientOnUnsubscribe: true,
				},
			}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")
	clientID := client.ID()

	// Subscribe with MapRemoveClientOnUnsubscribe.
	subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: channel,
		Type:    int32(SubscriptionTypeMap),
		Phase:   MapPhaseState,
		Limit:   100,
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
	require.NoError(t, err)
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
	require.NoError(t, err)
	entries, _, _ = stateRes.Publications, stateRes.Position, stateRes.Cursor
	require.NoError(t, err)
	require.Len(t, entries, 0)
}

func TestPresenceSubscribe_State(t *testing.T) {
	t.Parallel()
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
		Type:    int32(SubscriptionTypeMapClients),
		Phase:   MapPhaseState,
		Limit:   100,
	})

	require.True(t, result.Type == 1)
	require.Equal(t, MapPhaseLive, result.Phase)
	require.Len(t, result.State, 2)
}

func TestPresenceSubscribe_Live(t *testing.T) {
	t.Parallel()
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
		Type:    int32(SubscriptionTypeMapClients),
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
	t.Parallel()
	// Presence subscriptions can also be positioned (EnablePositioning: true).
	// With always-on state-to-live, positioned STATE goes directly to LIVE
	// on the last page when stream is close.
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

	// Positioned presence: STATE with all entries → state-to-live goes LIVE directly.
	result := subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: presenceChannel,
		Type:    int32(SubscriptionTypeMapClients),
		Phase:   MapPhaseState,
		Limit:   100,
	})

	require.True(t, result.Type == 1)
	require.Equal(t, MapPhaseLive, result.Phase) // Direct STATE→LIVE.
	require.Len(t, result.State, 1)

	// Verify flagMapPresence is set.
	client.mu.RLock()
	ctx2, ok := client.channels[presenceChannel]
	client.mu.RUnlock()
	require.True(t, ok)
	require.True(t, channelHasFlag(ctx2.flags, flagMapPresence))
}

func TestPresenceSubscribe_NotAllowed(t *testing.T) {
	t.Parallel()
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
		Type:    int32(SubscriptionTypeMapClients),
		Phase:   MapPhaseState,
	}, &protocol.Command{Id: 1}, time.Now(), rwWrapper.rw)
	require.NoError(t, err)
	require.Len(t, rwWrapper.replies, 1)
	require.NotNil(t, rwWrapper.replies[0].Error)
	require.Equal(t, ErrorPermissionDenied.Code, rwWrapper.replies[0].Error.Code)
}

func TestPresenceSubscribe_NoHandler(t *testing.T) {
	t.Parallel()
	node, _ := newTestNodeWithMapBroker(t)

	presenceChannel := "clients:test_presence_no_handler"

	// No OnSubscribe handler set.
	node.OnConnect(func(client *Client) {})

	client := newTestConnectedClientV2(t, node, "user1")

	// Try to subscribe to presence - should fail because no OnSubscribe handler.
	rwWrapper := testReplyWriterWrapper()
	err := client.handleSubscribe(&protocol.SubscribeRequest{
		Channel: presenceChannel,
		Type:    int32(SubscriptionTypeMapClients),
		Phase:   MapPhaseState,
	}, &protocol.Command{Id: 1}, time.Now(), rwWrapper.rw)
	// When no handler is set, the error is returned directly.
	require.Equal(t, ErrorNotAvailable, err)
}

func TestPresenceSubscribe_AlreadySubscribed(t *testing.T) {
	t.Parallel()
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
		Type:    int32(SubscriptionTypeMapClients),
		Phase:   MapPhaseState,
		Limit:   100,
	})

	// Second subscribe should fail with AlreadySubscribed.
	rwWrapper := testReplyWriterWrapper()
	err := client.handleSubscribe(&protocol.SubscribeRequest{
		Channel: presenceChannel,
		Type:    int32(SubscriptionTypeMapClients),
		Phase:   MapPhaseState,
	}, &protocol.Command{Id: 1}, time.Now(), rwWrapper.rw)
	// When already subscribed, the error is returned directly.
	require.Equal(t, ErrorAlreadySubscribed, err)
}

func TestMapPresenceTTL(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
	node, broker := newTestNodeWithMapBroker(t)

	channel := "test_map_presence"
	ctx := context.Background()

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					Type:                     SubscriptionTypeMap,
					MapClientPresenceChannel: "clients:" + channel,
					MapUserPresenceChannel:   "$users:" + channel,
				},
			}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")

	// Subscribe with both client and user presence.
	subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: channel,
		Type:    int32(SubscriptionTypeMap),
		Phase:   MapPhaseState,
		Limit:   100,
	})

	// Verify :clients presence was added (key=clientId, full info).
	clientsChannel := "clients:" + channel
	stateRes, err := broker.ReadState(ctx, clientsChannel, MapReadStateOptions{
		Limit: 100,
	})
	require.NoError(t, err)
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
	require.NoError(t, err)
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
	t.Parallel()
	node, broker := newTestNodeWithMapBroker(t)

	channel := "test_map_presence_cleanup"
	ctx := context.Background()

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					Type:                     SubscriptionTypeMap,
					MapClientPresenceChannel: "clients:" + channel,
					MapUserPresenceChannel:   "$users:" + channel,
				},
			}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")

	// Subscribe with both client and user presence.
	subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: channel,
		Type:    int32(SubscriptionTypeMap),
		Phase:   MapPhaseState,
		Limit:   100,
	})

	// Verify presence exists in both channels.
	clientsChannel := "clients:" + channel
	usersChannel := "$users:" + channel

	stateRes, err := broker.ReadState(ctx, clientsChannel, MapReadStateOptions{Limit: 100})
	require.NoError(t, err)
	entries, _, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
	require.NoError(t, err)
	require.Len(t, entries, 1)

	stateRes, err = broker.ReadState(ctx, usersChannel, MapReadStateOptions{Limit: 100})
	require.NoError(t, err)
	entries, _, _ = stateRes.Publications, stateRes.Position, stateRes.Cursor
	require.NoError(t, err)
	require.Len(t, entries, 1)

	// Disconnect client.
	err = client.close(DisconnectForceNoReconnect)
	require.NoError(t, err)

	// Verify :clients presence was removed.
	stateRes, err = broker.ReadState(ctx, clientsChannel, MapReadStateOptions{Limit: 100})
	require.NoError(t, err)
	entries, _, _ = stateRes.Publications, stateRes.Position, stateRes.Cursor
	require.NoError(t, err)
	require.Len(t, entries, 0)

	// Verify :users presence is NOT removed (TTL-based expiration).
	stateRes, err = broker.ReadState(ctx, usersChannel, MapReadStateOptions{Limit: 100})
	require.NoError(t, err)
	entries, _, _ = stateRes.Publications, stateRes.Position, stateRes.Cursor
	require.NoError(t, err)
	require.Len(t, entries, 1) // Still there, will expire via TTL.
}

func TestMapSubscribe_MultipleClientsPerUser(t *testing.T) {
	t.Parallel()
	node, broker := newTestNodeWithMapBroker(t)

	channel := "test_multi_clients"
	ctx := context.Background()

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					Type:                     SubscriptionTypeMap,
					MapClientPresenceChannel: "clients:" + channel,
					MapUserPresenceChannel:   "$users:" + channel,
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
		Type:    int32(SubscriptionTypeMap),
		Phase:   MapPhaseState,
		Limit:   100,
	})
	subscribeMapClient(t, client2, &protocol.SubscribeRequest{
		Channel: channel,
		Type:    int32(SubscriptionTypeMap),
		Phase:   MapPhaseState,
		Limit:   100,
	})

	// Verify :clients has two entries (one per connection).
	clientsChannel := "clients:" + channel
	stateRes, err := broker.ReadState(ctx, clientsChannel, MapReadStateOptions{Limit: 100})
	require.NoError(t, err)
	entries, _, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
	require.NoError(t, err)
	require.Len(t, entries, 2)

	// Verify :users has one entry (deduplicated by userId).
	usersChannel := "$users:" + channel
	stateRes, err = broker.ReadState(ctx, usersChannel, MapReadStateOptions{Limit: 100})
	require.NoError(t, err)
	entries, _, _ = stateRes.Publications, stateRes.Position, stateRes.Cursor
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, "user1", entries[0].Key)

	// Disconnect one client.
	err = client1.close(DisconnectForceNoReconnect)
	require.NoError(t, err)

	// :clients should have one entry.
	stateRes, err = broker.ReadState(ctx, clientsChannel, MapReadStateOptions{Limit: 100})
	require.NoError(t, err)
	entries, _, _ = stateRes.Publications, stateRes.Position, stateRes.Cursor
	require.NoError(t, err)
	require.Len(t, entries, 1)

	// :users still has the user (TTL refresh from client2).
	stateRes, err = broker.ReadState(ctx, usersChannel, MapReadStateOptions{Limit: 100})
	require.NoError(t, err)
	entries, _, _ = stateRes.Publications, stateRes.Position, stateRes.Cursor
	require.NoError(t, err)
	require.Len(t, entries, 1)
}

// TestMapBroker_ReadStateByKey tests the Key filter for ReadState.
func TestMapBroker_ReadStateByKey(t *testing.T) {
	t.Parallel()
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
	require.NoError(t, err)
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
	require.NoError(t, err)
	pubs, _, _ = stateRes.Publications, stateRes.Position, stateRes.Cursor
	require.NoError(t, err)
	require.Len(t, pubs, 0)
}

// TestMapBroker_CASSuccess tests successful CAS update.
func TestMapBroker_CASSuccess(t *testing.T) {
	t.Parallel()
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
	require.NoError(t, err)
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
	require.NoError(t, err)
	pubs, _, _ = stateRes.Publications, stateRes.Position, stateRes.Cursor
	require.NoError(t, err)
	require.Len(t, pubs, 1)
	require.Equal(t, []byte(`{"value":15}`), pubs[0].Data)
}

// TestMapBroker_CASConflict tests CAS conflict when position has changed.
func TestMapBroker_CASConflict(t *testing.T) {
	t.Parallel()
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
	require.NoError(t, err)
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

	// CurrentEntry should contain the current state for immediate retry
	require.NotNil(t, res.CurrentEntry)
	require.Equal(t, []byte(`{"value":12}`), res.CurrentEntry.Data)

	// Immediate retry using returned position - should succeed
	retryPos := StreamPosition{Offset: res.CurrentEntry.Offset, Epoch: res.Position.Epoch}
	res2, err := broker.Publish(ctx, ch, "counter", MapPublishOptions{
		Data:             []byte(`{"value":15}`),
		ExpectedPosition: &retryPos,
	})
	require.NoError(t, err)
	require.False(t, res2.Suppressed)

	// Verify the value was updated
	stateRes, err = broker.ReadState(ctx, ch, MapReadStateOptions{Key: "counter"})
	require.NoError(t, err)
	pubs, _, _ = stateRes.Publications, stateRes.Position, stateRes.Cursor
	require.NoError(t, err)
	require.Len(t, pubs, 1)
	require.Equal(t, []byte(`{"value":15}`), pubs[0].Data)
}

// TestMapBroker_CASNonExistent tests CAS on a key that doesn't exist.
func TestMapBroker_CASNonExistent(t *testing.T) {
	t.Parallel()
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
	require.NoError(t, err)
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
	t.Parallel()
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
	require.NoError(t, err)
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

	// CurrentEntry should contain the current state for immediate retry
	require.NotNil(t, res.CurrentEntry)
	require.Equal(t, []byte(`{"value":10}`), res.CurrentEntry.Data)

	// Verify value unchanged
	stateRes, err = broker.ReadState(ctx, ch, MapReadStateOptions{Key: "counter"})
	require.NoError(t, err)
	pubs, _, _ = stateRes.Publications, stateRes.Position, stateRes.Cursor
	require.NoError(t, err)
	require.Len(t, pubs, 1)
	require.Equal(t, []byte(`{"value":10}`), pubs[0].Data)
}

// Tests for STATE→LIVE direct transition optimization.

func TestMapSubscribe_StateToLive_DirectTransition(t *testing.T) {
	t.Parallel()
	// Test that when stream is close enough, server transitions directly
	// from STATE to LIVE on the last state page.
	node, err := New(Config{
		LogLevel:   LogLevelTrace,
		LogHandler: func(entry LogEntry) {},
		Map: MapConfig{
			GetMapChannelOptions: func(channel string) MapChannelOptions {
				return MapChannelOptions{
					Mode:        MapModeRecoverable,
					KeyTTL:      60 * time.Second,
					MinPageSize: 1,
				}
			},
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
		Type:    int32(SubscriptionTypeMap),
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
	t.Parallel()
	// Test that STATE→LIVE transition includes stream publications when
	// there are updates between state read and going live.
	node, err := New(Config{
		LogLevel:   LogLevelTrace,
		LogHandler: func(entry LogEntry) {},
		Map: MapConfig{
			GetMapChannelOptions: func(channel string) MapChannelOptions {
				return MapChannelOptions{
					Mode:   MapModeRecoverable,
					KeyTTL: 60 * time.Second,
				}
			},
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
		Type:    int32(SubscriptionTypeMap),
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
	t.Parallel()
	// Test that with pagination, only the LAST page can trigger STATE→LIVE.
	// Earlier pages should return phase=2 (STATE) with cursor.
	node, err := New(Config{
		LogLevel:   LogLevelTrace,
		LogHandler: func(entry LogEntry) {},
		Map: MapConfig{
			GetMapChannelOptions: func(channel string) MapChannelOptions {
				return MapChannelOptions{
					Mode:        MapModeRecoverable,
					KeyTTL:      60 * time.Second,
					MinPageSize: 1,
				}
			},
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
		Type:    int32(SubscriptionTypeMap),
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
		Type:    int32(SubscriptionTypeMap),
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
	t.Parallel()
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
		LogLevel:   LogLevelTrace,
		LogHandler: func(entry LogEntry) {},
		Map: MapConfig{
			GetMapChannelOptions: func(channel string) MapChannelOptions {
				return MapChannelOptions{
					Mode:        MapModeRecoverable,
					KeyTTL:      60 * time.Second,
					MinPageSize: 1,
				}
			},
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
		Type:    int32(SubscriptionTypeMap),
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
			Type:    int32(SubscriptionTypeMap),
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
	t.Parallel()
	// Regression test with publications happening at multiple points during
	// multi-page pagination. Verifies the frozen offset stays consistent and
	// ALL publications during the entire pagination are recovered.
	node, err := New(Config{
		LogLevel:   LogLevelTrace,
		LogHandler: func(entry LogEntry) {},
		Map: MapConfig{
			GetMapChannelOptions: func(channel string) MapChannelOptions {
				return MapChannelOptions{
					Mode:        MapModeRecoverable,
					KeyTTL:      60 * time.Second,
					MinPageSize: 1,
				}
			},
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
		Type:    int32(SubscriptionTypeMap),
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
		Type:    int32(SubscriptionTypeMap),
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
			Type:    int32(SubscriptionTypeMap),
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
	t.Parallel()
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
		Type:    int32(SubscriptionTypeMap),
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
	t.Parallel()
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
		Type:    int32(SubscriptionTypeMap),
		Phase:   MapPhaseStream,
		Offset:  res.Position.Offset,
		Epoch:   res.Position.Epoch,
		Limit:   100,
		Recover: false, // Not recovering
	})

	require.Equal(t, ErrorPermissionDenied.Code, protoErr.Code)
}

func TestMapSubscribe_StreamPhaseRecovery_LargeGap(t *testing.T) {
	t.Parallel()
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
		Type:    int32(SubscriptionTypeMap),
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
			Type:    int32(SubscriptionTypeMap),
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
	t.Parallel()
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
		Type:    int32(SubscriptionTypeMap),
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

// Streamless mode: phase=1 (STREAM) should be rejected with ErrorBadRequest.
func TestMapSubscribe_Streamless_StreamPhaseRejected(t *testing.T) {
	t.Parallel()
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
		Type:    int32(SubscriptionTypeMap),
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

// Streamless mode: reconnection uses STATE phase to re-sync full state.
func TestMapSubscribe_Streamless_RecoveryUsesStatePhase(t *testing.T) {
	t.Parallel()
	node, broker := newTestNodeWithMapBroker(t)

	channel := "test_streamless_recovery_state"
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

	// Streamless reconnection: use STATE phase to get full state.
	// In streamless mode, last state page transitions directly to LIVE.
	result := subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: channel,
		Type:    int32(SubscriptionTypeMap),
		Phase:   MapPhaseState,
		Limit:   100,
	})

	require.Equal(t, MapPhaseLive, result.Phase)
	// Full state should be returned.
	require.Len(t, result.State, 2)
	require.NotEmpty(t, result.Epoch)
	// Client should be subscribed.
	require.Contains(t, client.channels, channel)
}

// Positioned mode: epoch mismatch during STREAM phase should return ErrorUnrecoverablePosition.
func TestMapSubscribe_Positioned_StreamEpochMismatch(t *testing.T) {
	t.Parallel()
	// Test that STREAM phase with wrong epoch returns ErrorUnrecoverablePosition.
	// We create a scenario where STATE doesn't go LIVE (large stream gap) so
	// the client needs to use STREAM phase, and then send wrong epoch.
	node, broker := newTestNodeWithMapBroker(t)
	setTestMapChannelOptionsConverging(node)

	channel := "test_stream_epoch_mismatch"
	ctx := context.Background()

	// Pre-populate 4 entries.
	for i := 0; i < 4; i++ {
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

	// STATE page 1 (limit=2): gets 2 entries, cursor.
	result1 := subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: channel,
		Type:    int32(SubscriptionTypeMap),
		Phase:   MapPhaseState,
		Limit:   2,
	})
	require.Equal(t, MapPhaseState, result1.Phase)
	require.NotEmpty(t, result1.Cursor)
	frozenOffset := result1.Offset
	epoch := result1.Epoch

	// Update EXISTING keys to push stream offset far ahead without adding new state entries.
	for i := 0; i < 100; i++ {
		_, err := broker.Publish(ctx, channel, string(rune('a'+i%4)), MapPublishOptions{
			Data: []byte(fmt.Sprintf(`{"v":"update_%d"}`, i)),
		})
		require.NoError(t, err)
	}

	// STATE page 2 (last): state-to-live fails (gap too large) → stays STATE.
	result2 := subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: channel,
		Type:    int32(SubscriptionTypeMap),
		Phase:   MapPhaseState,
		Limit:   2,
		Cursor:  result1.Cursor,
		Offset:  frozenOffset,
		Epoch:   epoch,
	})
	require.Equal(t, MapPhaseState, result2.Phase)
	require.Empty(t, result2.Cursor)

	// Now send STREAM with wrong epoch — should get ErrorUnrecoverablePosition.
	rwWrapper := testReplyWriterWrapper()
	err := client.handleSubscribe(&protocol.SubscribeRequest{
		Channel: channel,
		Type:    int32(SubscriptionTypeMap),
		Phase:   MapPhaseStream,
		Offset:  frozenOffset,
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
	t.Parallel()
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
		Type:    int32(SubscriptionTypeMap),
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

// Positioned mode: MapLiveTransitionMaxPublicationLimit boundary test.
// When client is exactly at the limit, recovery should succeed.
// When client is beyond the limit, should get ErrorUnrecoverablePosition.
func TestMapSubscribe_RecoveryMaxPublicationLimit(t *testing.T) {
	t.Parallel()
	node, err := New(Config{
		LogLevel:   LogLevelTrace,
		LogHandler: func(entry LogEntry) {},
		Map: MapConfig{
			GetMapChannelOptions: func(channel string) MapChannelOptions {
				return MapChannelOptions{
					Mode:                              MapModeRecoverable,
					KeyTTL:                            60 * time.Second,
					MinPageSize:                       1,
					LiveTransitionMaxPublicationLimit: 5, // Max 5 publications during recovery.
				}
			},
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
		Type:    int32(SubscriptionTypeMap),
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
		Type:    int32(SubscriptionTypeMap),
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

// Positioned mode: State consistency filtering. Entries modified after the saved position
// during pagination should be filtered out from later pages to prevent duplicates.
func TestMapSubscribe_Positioned_StateConsistencyFiltering(t *testing.T) {
	t.Parallel()
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
		Type:    int32(SubscriptionTypeMap),
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
		Type:    int32(SubscriptionTypeMap),
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
	t.Parallel()
	// Test the full three-phase flow: STATE → STREAM → LIVE.
	// This requires the stream to advance during STATE pagination so that
	// state-to-live can't kick in on the last page.
	node, broker := newTestNodeWithMapBroker(t)
	setTestMapChannelOptionsConverging(node)

	channel := "test_three_phase"
	ctx := context.Background()

	// Pre-populate 10 entries.
	var epoch string
	for i := 0; i < 10; i++ {
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

	// Phase 2 (STATE): first page.
	result1 := subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: channel,
		Type:    int32(SubscriptionTypeMap),
		Phase:   MapPhaseState,
		Limit:   5,
	})
	require.Equal(t, MapPhaseState, result1.Phase)
	require.Len(t, result1.State, 5)
	require.NotEmpty(t, result1.Cursor)
	require.Equal(t, epoch, result1.Epoch)
	frozenOffset := result1.Offset

	// Update EXISTING keys between STATE pages to push stream offset far ahead
	// without adding new state entries. frozenOffset + limit < streamOffset → no state-to-live.
	var lastOffset uint64
	for i := 0; i < 100; i++ {
		res, err := broker.Publish(ctx, channel, string(rune('a'+i%10)), MapPublishOptions{
			Data: []byte(fmt.Sprintf(`{"v":"update_%d"}`, i)),
		})
		require.NoError(t, err)
		lastOffset = res.Position.Offset
	}

	// Phase 2 (STATE): second (last) page.
	// State-to-live fails (frozenOffset=10 + limit=5 = 15 < ~110) → stays STATE.
	result2 := subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: channel,
		Type:    int32(SubscriptionTypeMap),
		Phase:   MapPhaseState,
		Limit:   5,
		Cursor:  result1.Cursor,
		Offset:  frozenOffset,
		Epoch:   epoch,
	})
	require.Equal(t, MapPhaseState, result2.Phase)
	require.Empty(t, result2.Cursor) // Last page.

	// Phase 1 (STREAM): catch up from frozen offset.
	result3 := subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: channel,
		Type:    int32(SubscriptionTypeMap),
		Phase:   MapPhaseStream,
		Offset:  frozenOffset,
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
			Type:    int32(SubscriptionTypeMap),
			Phase:   MapPhaseStream,
			Offset:  currentOffset,
			Epoch:   epoch,
			Limit:   50,
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
	t.Parallel()
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
		Type:    int32(SubscriptionTypeMap),
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

// Streamless mode: STATE phase on last page transitions directly to LIVE with full state.
func TestMapSubscribe_Streamless_StateToLive(t *testing.T) {
	t.Parallel()
	node, broker := newTestNodeWithMapBroker(t)

	channel := "test_streamless_state_to_live"
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

	// STATE phase: state fits in one page → transitions to LIVE.
	result := subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: channel,
		Type:    int32(SubscriptionTypeMap),
		Phase:   MapPhaseState,
		Limit:   100,
	})

	require.Equal(t, MapPhaseLive, result.Phase)
	require.Len(t, result.State, 3)      // Full state returned.
	require.False(t, result.Recoverable) // Streamless: not recoverable.
	require.Contains(t, client.channels, channel)
}

// Positioned mode: STATE phase with small state transitions to LIVE with full state.
func TestMapSubscribe_Positioned_StateToLive_SmallState(t *testing.T) {
	t.Parallel()
	node, broker := newTestNodeWithMapBroker(t)
	setTestMapChannelOptionsConverging(node)

	channel := "test_positioned_state_to_live"
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

	// STATE phase: state fits in one page → stream close → transitions to LIVE.
	result := subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: channel,
		Type:    int32(SubscriptionTypeMap),
		Phase:   MapPhaseState,
		Limit:   100,
	})

	require.Equal(t, MapPhaseLive, result.Phase)
	require.Len(t, result.State, 3)     // Full state returned.
	require.True(t, result.Recoverable) // Positioned: recoverable.
	require.Greater(t, result.Offset, uint64(0))
	require.NotEmpty(t, result.Epoch)
	require.Contains(t, client.channels, channel)
}

// TestMapSubscribe_StreamPhaseOffset verifies that the STREAM phase returns
// the last publication's offset (not stream.Top()), preventing the client
// from jumping ahead and skipping publications in multi-page STREAM scenarios.
func TestMapSubscribe_StreamPhaseOffset(t *testing.T) {
	t.Parallel()
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
		Type:    int32(SubscriptionTypeMap),
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
		Type:    int32(SubscriptionTypeMap),
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
			Type:    int32(SubscriptionTypeMap),
			Phase:   MapPhaseState,
			Cursor:  result.Cursor,
			Limit:   100,
			Offset:  firstPageOffset,
			Epoch:   firstPageEpoch,
		})
	}
	// On last state page, server checks if stream is close enough to go LIVE.
	// If so, it transitions directly. Otherwise returns STATE and client does STREAM.
	if result.Phase == MapPhaseLive {
		// Server went STATE→LIVE directly (stream was close enough).
		// Test passed — offset was frozen correctly on intermediate pages.
		return
	}

	// STREAM phase with small limit to force multi-page.
	result = subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: channel,
		Type:    int32(SubscriptionTypeMap),
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
	t.Parallel()
	node, broker := newTestNodeWithMapBroker(t)
	// Set a very short timeout so we can trigger it without sleeping.
	node.config.Map.GetMapChannelOptions = func(channel string) MapChannelOptions {
		return MapChannelOptions{
			Mode:                    MapModeEphemeral,
			KeyTTL:                  60 * time.Second,
			MinPageSize:             1,
			SubscribeCatchUpTimeout: time.Nanosecond,
		}
	}

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
		Type:    int32(SubscriptionTypeMap),
		Phase:   MapPhaseState,
		Limit:   5,
	})
	require.Equal(t, MapPhaseState, result.Phase)
	require.NotEmpty(t, result.Cursor)

	// Second page (cursor continuation) — timeout already expired.
	rwWrapper := testReplyWriterWrapper()
	err := client.handleSubscribe(&protocol.SubscribeRequest{
		Channel: channel,
		Type:    int32(SubscriptionTypeMap),
		Phase:   MapPhaseState,
		Limit:   5,
		Cursor:  result.Cursor,
	}, &protocol.Command{Id: 1}, time.Now(), rwWrapper.rw)
	require.ErrorIs(t, err, DisconnectSlow)

	// mapSubscribing state should be cleaned up.
	client.mu.RLock()
	_, hasState := client.mapSubscribing[channel]
	client.mu.RUnlock()
	require.False(t, hasState)
}

func TestMapSubscribe_CatchUpTimeout_PhaseTransition(t *testing.T) {
	t.Parallel()
	// Test that catch-up timeout fires when client takes too long between phases.
	// We use multi-page STATE with stream advancing between pages so that
	// state-to-live doesn't kick in, then manipulate startedAt before STREAM.
	node, broker := newTestNodeWithMapBroker(t)
	node.config.Map.GetMapChannelOptions = func(channel string) MapChannelOptions {
		return MapChannelOptions{
			Mode:                    MapModeRecoverable,
			KeyTTL:                  60 * time.Second,
			MinPageSize:             1,
			SubscribeCatchUpTimeout: 10 * time.Second, // Large enough for STATE pages.
		}
	}

	channel := "test_catchup_timeout_phase"
	ctx := context.Background()

	// Pre-populate 4 entries.
	for i := 0; i < 4; i++ {
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

	// STATE page 1 (limit=2): 2 entries, cursor. Offset frozen at 4.
	result := subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: channel,
		Type:    int32(SubscriptionTypeMap),
		Phase:   MapPhaseState,
		Limit:   2,
	})
	require.Equal(t, MapPhaseState, result.Phase)
	require.NotEmpty(t, result.Cursor)

	// Update existing keys to push stream far ahead (frozenOffset=4 + limit=2 < 204).
	for i := 0; i < 200; i++ {
		_, err := broker.Publish(ctx, channel, "key"+string(rune('A'+i%4)), MapPublishOptions{
			Data: []byte(fmt.Sprintf(`{"v":"update_%d"}`, i)),
		})
		require.NoError(t, err)
	}

	// STATE page 2 (last): state-to-live fails (gap too large) → stays STATE.
	result2 := subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: channel,
		Type:    int32(SubscriptionTypeMap),
		Phase:   MapPhaseState,
		Limit:   2,
		Cursor:  result.Cursor,
		Offset:  result.Offset,
		Epoch:   result.Epoch,
	})
	require.Equal(t, MapPhaseState, result2.Phase)
	require.Empty(t, result2.Cursor)

	// Simulate timeout by pushing startedAt far into the past.
	client.mu.Lock()
	client.mapSubscribing[channel].startedAt = time.Now().Add(-time.Hour).UnixNano()
	client.mu.Unlock()

	// Phase transition to STREAM — timeout expired.
	rwWrapper := testReplyWriterWrapper()
	err := client.handleSubscribe(&protocol.SubscribeRequest{
		Channel: channel,
		Type:    int32(SubscriptionTypeMap),
		Phase:   MapPhaseStream,
		Offset:  result.Offset,
		Epoch:   result.Epoch,
	}, &protocol.Command{Id: 1}, time.Now(), rwWrapper.rw)
	require.ErrorIs(t, err, DisconnectSlow)
}

func TestMapSubscribe_CatchUpTimeout_Sweep(t *testing.T) {
	t.Parallel()
	node, broker := newTestNodeWithMapBroker(t)
	node.config.Map.GetMapChannelOptions = func(channel string) MapChannelOptions {
		return MapChannelOptions{
			Mode:                    MapModeEphemeral,
			KeyTTL:                  60 * time.Second,
			MinPageSize:             1,
			SubscribeCatchUpTimeout: time.Nanosecond,
		}
	}

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
		Type:    int32(SubscriptionTypeMap),
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
		Type:    int32(SubscriptionTypeMap),
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
	t.Parallel()
	node, broker := newTestNodeWithMapBroker(t)
	// Negative timeout disables the check.
	node.config.Map.GetMapChannelOptions = func(channel string) MapChannelOptions {
		return MapChannelOptions{
			Mode:                    MapModeEphemeral,
			KeyTTL:                  60 * time.Second,
			MinPageSize:             1,
			SubscribeCatchUpTimeout: -1,
		}
	}

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
		Type:    int32(SubscriptionTypeMap),
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
		Type:    int32(SubscriptionTypeMap),
		Phase:   MapPhaseState,
		Limit:   5,
		Cursor:  result.Cursor,
	})
	require.Equal(t, MapPhaseLive, result2.Phase)
}

func TestMapSubscribe_WasRecovering(t *testing.T) {
	t.Parallel()
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

	// Fresh subscription via STATE — state fits in one page, stream close → goes LIVE.
	// Should NOT have WasRecovering.
	result := subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: channel,
		Type:    int32(SubscriptionTypeMap),
		Phase:   MapPhaseState,
		Limit:   100,
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
		Type:    int32(SubscriptionTypeMap),
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
	t.Parallel()
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
		Type:    int32(SubscriptionTypeMap),
		Phase:   MapPhaseState,
		Limit:   100,
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

// TestEmptyEpochAdoption_AllChannelTypes verifies that the epoch adoption
// logic works for both map and stream channels: any channel with epoch=""
// receiving a publication with a real epoch adopts it instead of triggering
// insufficient state. This supports the case of subscribing via a lagging
// read replica that doesn't have the meta row yet (returns epoch=""), then
// receiving the first publication with the real epoch.
func TestEmptyEpochAdoption_AllChannelTypes(t *testing.T) {
	t.Parallel()
	broker := NewTestBroker()
	node := nodeWithBroker(broker)
	defer func() { _ = node.Shutdown(context.Background()) }()

	insufficientState := make(chan struct{}, 1)

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(event SubscribeEvent, callback SubscribeCallback) {
			callback(SubscribeReply{Options: SubscribeOptions{EnableRecovery: true}}, nil)
		})
		client.OnUnsubscribe(func(event UnsubscribeEvent) {
			if event.Code == UnsubscribeCodeInsufficient {
				select {
				case insufficientState <- struct{}{}:
				default:
				}
			}
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

	// Non-map channel with epoch="" receiving a pub with real epoch —
	// should adopt the epoch (not trigger insufficient state).
	err = node.handlePublication("test", StreamPosition{1, "xyz"}, &Publication{
		Offset: 1,
		Data:   []byte(`{}`),
	}, nil, nil)
	require.NoError(t, err)

	// Give time for any async insufficient state handling to fire.
	time.Sleep(200 * time.Millisecond)

	select {
	case <-insufficientState:
		require.Fail(t, "got insufficient state — epoch adoption should have prevented this")
	default:
		// Success — no insufficient state, epoch was adopted.
	}
}

// TestMapSubscribe_RealEpochMismatch verifies that a map channel client with a
// real (non-empty) epoch triggers handleInsufficientState when a publication
// arrives with a different epoch. The epoch adoption only applies to epoch="".
func TestMapSubscribe_RealEpochMismatch(t *testing.T) {
	t.Parallel()
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
		Type:    int32(SubscriptionTypeMap),
		Phase:   MapPhaseState,
		Limit:   100,
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
	t.Parallel()
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
		Type:    int32(SubscriptionTypeMap),
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
	t.Parallel()
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
		Type:    int32(SubscriptionTypeMap),
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
	t.Parallel()
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
		Type:    int32(SubscriptionTypeMap),
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
	t.Parallel()
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
		Type:    int32(SubscriptionTypeMap),
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

// TestMapSubscribe_StateToLive_EpochFlipDuringPagination verifies that a
// non-recovery STATE→LIVE transition rejects the request with
// ErrorUnrecoverablePosition when the broker's stream epoch differs from the
// state-phase epoch the client paginated against. Without the guard the client
// would merge prior-epoch state with new-epoch live publications and silently
// orphan any keys not republished in the new epoch.
func TestMapSubscribe_StateToLive_EpochFlipDuringPagination(t *testing.T) {
	t.Parallel()
	node, err := New(Config{
		LogLevel:   LogLevelTrace,
		LogHandler: func(entry LogEntry) {},
		Map: MapConfig{
			GetMapChannelOptions: func(channel string) MapChannelOptions {
				return MapChannelOptions{
					Mode:        MapModeRecoverable,
					KeyTTL:      60 * time.Second,
					MinPageSize: 1,
				}
			},
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

	channel := "test_state_to_live_epoch_flip"
	ctx := context.Background()

	// Seed multiple keys so the first state page is not the last.
	for i := 0; i < 4; i++ {
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

	// First page captures the state-phase epoch and a cursor for the next page.
	firstPage := subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: channel,
		Type:    int32(SubscriptionTypeMap),
		Phase:   MapPhaseState,
		Limit:   1,
	})
	require.Equal(t, MapPhaseState, firstPage.Phase)
	require.NotEmpty(t, firstPage.Cursor)
	require.NotEmpty(t, firstPage.Epoch)

	frozenOffset := firstPage.Offset
	stateEpoch := firstPage.Epoch

	// Clear the channel between pagination requests so the LIVE transition's
	// MapStreamRead lands on a freshly-created stream with a different epoch.
	// We deliberately do NOT republish afterward — that path skips the broker's
	// own `since.Epoch != stream.Epoch()` check (which only fires on a populated
	// stream) and exercises the client-side guard we added: a non-empty
	// state-phase epoch must match the live stream's epoch even when
	// isRecovery=false.
	require.NoError(t, broker.Clear(ctx, channel, MapClearOptions{}))

	rwWrapper := testReplyWriterWrapper()
	err = client.handleSubscribe(&protocol.SubscribeRequest{
		Channel: channel,
		Type:    int32(SubscriptionTypeMap),
		Phase:   MapPhaseState,
		Limit:   100, // ample so the remaining state fits in one page → state→live transition.
		Cursor:  firstPage.Cursor,
		Offset:  frozenOffset,
		Epoch:   stateEpoch,
	}, &protocol.Command{Id: 2}, time.Now(), rwWrapper.rw)
	require.NoError(t, err)
	require.Len(t, rwWrapper.replies, 1)
	require.NotNil(t, rwWrapper.replies[0].Error, "state→live with mismatched epoch must error")
	require.Equal(t, ErrorUnrecoverablePosition.Code, rwWrapper.replies[0].Error.Code)
}

// TestMapSubscribe_EpochInFirstPublication verifies that the first publication
// (offset=1) includes epoch on the wire, while subsequent publications do not.
// This allows the client SDK to learn the channel epoch from the first publication
// without redundant epoch bytes in every message.
func TestMapSubscribe_EpochInFirstPublication(t *testing.T) {
	t.Parallel()
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
		Type:    int32(SubscriptionTypeMap),
		Phase:   MapPhaseState,
		Limit:   100,
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

// TestStreamSubscribe_WithMapClientPresence verifies that a stream (regular) subscribe
// with MapClientPresenceChannel sets the right flags, stores presence in MapBroker,
// and cleans up on unsubscribe. Covers Steps 4, 6, 7 of the plan.
func TestStreamSubscribe_WithMapClientPresence(t *testing.T) {
	t.Parallel()
	node, _ := newTestNodeWithMapBroker(t)

	presenceChannel := "clients:test_channel"

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					MapClientPresenceChannel: presenceChannel,
				},
			}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")
	subscribeClientV2(t, client, "test_channel")

	// Verify flags (set synchronously in subscribeCmd).
	client.mu.RLock()
	chCtx := client.channels["test_channel"]
	client.mu.RUnlock()
	require.True(t, channelHasFlag(chCtx.flags, flagMapClientPresence))
	require.False(t, channelHasFlag(chCtx.flags, flagMap), "flagMap should not be set for stream subscribe")
	require.Equal(t, presenceChannel, chCtx.mapClientPresenceChannel)

	// Verify presence added to MapBroker (async goroutine).
	require.Eventually(t, func() bool {
		state, err := node.MapStateRead(context.Background(), presenceChannel, MapReadStateOptions{Limit: 100})
		return err == nil && len(state.Publications) == 1 && state.Publications[0].Key == client.uid
	}, time.Second, 50*time.Millisecond)

	// Unsubscribe — verify presence removed.
	client.Unsubscribe("test_channel")
	require.Eventually(t, func() bool {
		state, err := node.MapStateRead(context.Background(), presenceChannel, MapReadStateOptions{Limit: 100})
		return err == nil && len(state.Publications) == 0
	}, time.Second, 50*time.Millisecond)
}

// TestStreamSubscribe_WithMapPresenceDisconnect verifies that disconnect cleans up
// map client presence for stream subscriptions.
func TestStreamSubscribe_WithMapPresenceDisconnect(t *testing.T) {
	t.Parallel()
	node, _ := newTestNodeWithMapBroker(t)

	presenceChannel := "clients:test_channel"

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					MapClientPresenceChannel: presenceChannel,
				},
			}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")
	subscribeClientV2(t, client, "test_channel")

	// Verify presence exists before disconnect (async goroutine).
	require.Eventually(t, func() bool {
		state, err := node.MapStateRead(context.Background(), presenceChannel, MapReadStateOptions{Limit: 100})
		return err == nil && len(state.Publications) == 1
	}, time.Second, 50*time.Millisecond)

	// Disconnect — verify presence cleaned up.
	_ = client.close(DisconnectForceNoReconnect)
	require.Eventually(t, func() bool {
		state, err := node.MapStateRead(context.Background(), presenceChannel, MapReadStateOptions{Limit: 100})
		return err == nil && len(state.Publications) == 0
	}, time.Second, 50*time.Millisecond)
}

// TestStreamSubscribe_WithMapUserPresence verifies that user presence is NOT removed
// on unsubscribe (TTL-based expiry by design).
func TestStreamSubscribe_WithMapUserPresence(t *testing.T) {
	t.Parallel()
	node, _ := newTestNodeWithMapBroker(t)

	presenceChannel := "users:test_channel"

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					MapUserPresenceChannel: presenceChannel,
				},
			}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")
	subscribeClientV2(t, client, "test_channel")

	// Verify user presence added (async goroutine).
	require.Eventually(t, func() bool {
		state, err := node.MapStateRead(context.Background(), presenceChannel, MapReadStateOptions{Limit: 100})
		return err == nil && len(state.Publications) == 1 && state.Publications[0].Key == "user1"
	}, time.Second, 50*time.Millisecond)

	// Unsubscribe — user presence should NOT be removed (TTL-based expiry).
	client.Unsubscribe("test_channel")
	// Give unsubscribe time to process, then verify presence still exists.
	require.Never(t, func() bool {
		state, err := node.MapStateRead(context.Background(), presenceChannel, MapReadStateOptions{Limit: 100})
		return err != nil || len(state.Publications) == 0
	}, 100*time.Millisecond, 10*time.Millisecond, "user presence should not be removed on unsubscribe")
}

// TestStreamSubscribe_WithRegularAndMapPresence verifies that regular presence and
// map client presence can coexist on a stream subscribe.
func TestStreamSubscribe_WithRegularAndMapPresence(t *testing.T) {
	t.Parallel()
	node, _ := newTestNodeWithMapBroker(t)

	presenceChannel := "clients:test_channel"

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					EmitPresence:             true,
					MapClientPresenceChannel: presenceChannel,
				},
			}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")
	subscribeClientV2(t, client, "test_channel")

	// Verify both flags set (synchronous).
	client.mu.RLock()
	chCtx := client.channels["test_channel"]
	client.mu.RUnlock()
	require.True(t, channelHasFlag(chCtx.flags, flagEmitPresence))
	require.True(t, channelHasFlag(chCtx.flags, flagMapClientPresence))

	// Verify map presence added (async goroutine).
	require.Eventually(t, func() bool {
		state, err := node.MapStateRead(context.Background(), presenceChannel, MapReadStateOptions{Limit: 100})
		return err == nil && len(state.Publications) == 1
	}, time.Second, 50*time.Millisecond)

	// Unsubscribe — both should be cleaned up.
	client.Unsubscribe("test_channel")
	require.Eventually(t, func() bool {
		state, err := node.MapStateRead(context.Background(), presenceChannel, MapReadStateOptions{Limit: 100})
		return err == nil && len(state.Publications) == 0
	}, time.Second, 50*time.Millisecond)
}

// TestStreamSubscribe_MapPresencePeriodicUpdate verifies that updateChannelPresence
// works for stream channels with map presence (Step 6: flagMap gate removed).
func TestStreamSubscribe_MapPresencePeriodicUpdate(t *testing.T) {
	t.Parallel()
	node, _ := newTestNodeWithMapBroker(t)

	presenceChannel := "clients:test_channel"

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					MapClientPresenceChannel: presenceChannel,
				},
			}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")
	subscribeClientV2(t, client, "test_channel")

	// Wait for async presence setup before testing periodic update.
	require.Eventually(t, func() bool {
		state, err := node.MapStateRead(context.Background(), presenceChannel, MapReadStateOptions{Limit: 100})
		return err == nil && len(state.Publications) == 1
	}, time.Second, 50*time.Millisecond)

	// Call updateChannelPresence directly — should not error.
	client.mu.RLock()
	chCtx := client.channels["test_channel"]
	client.mu.RUnlock()
	err := client.updateChannelPresence("test_channel", chCtx)
	require.NoError(t, err)
}

// TestSharedPollSubscribe_WithMapPresence verifies that shared poll subscribe
// with MapClientPresenceChannel sets up presence correctly.
func TestSharedPollSubscribe_WithMapPresence(t *testing.T) {
	t.Parallel()
	presenceChannel := "clients:test_channel"

	node, err := New(Config{
		LogLevel:   LogLevelTrace,
		LogHandler: func(entry LogEntry) {},
		SharedPoll: SharedPollConfig{
			GetSharedPollChannelOptions: func(channel string) (SharedPollChannelOptions, bool) {
				return SharedPollChannelOptions{
					RefreshInterval:      100 * time.Millisecond,
					MaxKeysPerConnection: 100,
				}, true
			},
		},
		Map: MapConfig{
			GetMapChannelOptions: func(channel string) MapChannelOptions {
				return MapChannelOptions{
					Mode:   MapModeEphemeral,
					KeyTTL: 60 * time.Second,
				}
			},
		},
	})
	require.NoError(t, err)

	broker, err := NewMemoryMapBroker(node, MemoryMapBrokerConfig{})
	require.NoError(t, err)
	err = broker.RegisterEventHandler(nil)
	require.NoError(t, err)
	node.SetMapBroker(broker)

	node.OnSharedPoll(func(ctx context.Context, event SharedPollEvent) (SharedPollResult, error) {
		return SharedPollResult{}, nil
	})

	err = node.Run()
	require.NoError(t, err)
	t.Cleanup(func() { _ = node.Shutdown(context.Background()) })

	node.OnConnecting(func(ctx context.Context, e ConnectEvent) (ConnectReply, error) {
		return ConnectReply{}, nil
	})
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					ExpireAt:                 time.Now().Unix() + 3600,
					MapClientPresenceChannel: presenceChannel,
				},
				ClientSideRefresh: true,
			}, nil)
		})
		client.OnTrack(func(e TrackEvent, cb TrackCallback) {
			cb(TrackReply{}, nil)
		})
		client.OnSubRefresh(func(e SubRefreshEvent, cb SubRefreshCallback) {
			cb(SubRefreshReply{}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")
	subscribeSharedPollClient(t, client, "test_channel")

	// Verify flags (synchronous).
	client.mu.RLock()
	chCtx := client.channels["test_channel"]
	client.mu.RUnlock()
	require.True(t, channelHasFlag(chCtx.flags, flagMapClientPresence))
	require.True(t, channelHasFlag(chCtx.flags, flagKeyed))
	require.Equal(t, presenceChannel, chCtx.mapClientPresenceChannel)

	// Verify presence added to MapBroker (async goroutine).
	require.Eventually(t, func() bool {
		state, err := node.MapStateRead(context.Background(), presenceChannel, MapReadStateOptions{Limit: 100})
		return err == nil && len(state.Publications) == 1
	}, time.Second, 50*time.Millisecond)

	// Unsubscribe — verify cleanup.
	client.Unsubscribe("test_channel")
	require.Eventually(t, func() bool {
		state, err := node.MapStateRead(context.Background(), presenceChannel, MapReadStateOptions{Limit: 100})
		return err == nil && len(state.Publications) == 0
	}, time.Second, 50*time.Millisecond)
}

// TestSharedPollSubscribe_WithRegularPresence verifies that shared poll subscribe
// with EmitPresence and EmitJoinLeave sets the right flags.
func TestSharedPollSubscribe_WithRegularPresence(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t)

	node.OnConnecting(func(ctx context.Context, e ConnectEvent) (ConnectReply, error) {
		return ConnectReply{}, nil
	})
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					ExpireAt:      time.Now().Unix() + 3600,
					EmitPresence:  true,
					EmitJoinLeave: true,
				},
				ClientSideRefresh: true,
			}, nil)
		})
		client.OnTrack(func(e TrackEvent, cb TrackCallback) {
			cb(TrackReply{}, nil)
		})
		client.OnSubRefresh(func(e SubRefreshEvent, cb SubRefreshCallback) {
			cb(SubRefreshReply{}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")
	subscribeSharedPollClient(t, client, "test_channel")

	// Verify flags.
	client.mu.RLock()
	chCtx := client.channels["test_channel"]
	client.mu.RUnlock()
	require.True(t, channelHasFlag(chCtx.flags, flagEmitPresence))
	require.True(t, channelHasFlag(chCtx.flags, flagEmitJoinLeave))
	require.True(t, channelHasFlag(chCtx.flags, flagKeyed))
}

// TestMapSubscribe_WithOnlyEmitPresence_Regression verifies that a map subscribe
// with only EmitPresence (no map presence channels) still works correctly after
// the unsubscribe refactoring (Step 7 regression test).
func TestMapSubscribe_WithOnlyEmitPresence_Regression(t *testing.T) {
	t.Parallel()
	node, _ := newTestNodeWithMapBroker(t)

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					Type:         SubscriptionTypeMap,
					EmitPresence: true,
				},
			}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")

	// Subscribe to map channel.
	subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: "test_map",
		Type:    int32(SubscriptionTypeMap),
	})

	// Verify flags.
	client.mu.RLock()
	chCtx := client.channels["test_map"]
	client.mu.RUnlock()
	require.True(t, channelHasFlag(chCtx.flags, flagEmitPresence))
	require.True(t, channelHasFlag(chCtx.flags, flagMap))
	require.False(t, channelHasFlag(chCtx.flags, flagMapClientPresence))

	// Unsubscribe — should work without errors.
	client.Unsubscribe("test_map")
}

func TestMapSubscribe_ServerTagsFilter_StatePhase(t *testing.T) {
	t.Parallel()
	node, broker := newTestNodeWithMapBroker(t)
	setTestMapChannelOptionsConverging(node)

	channel := "test_server_tags_state"
	ctx := context.Background()

	// Publish entries with different tags.
	_, err := broker.Publish(ctx, channel, "admin_item", MapPublishOptions{
		Data: []byte(`{"v":"admin"}`),
		Tags: map[string]string{"role": "admin"},
	})
	require.NoError(t, err)
	_, err = broker.Publish(ctx, channel, "viewer_item", MapPublishOptions{
		Data: []byte(`{"v":"viewer"}`),
		Tags: map[string]string{"role": "viewer"},
	})
	require.NoError(t, err)
	_, err = broker.Publish(ctx, channel, "no_tags_item", MapPublishOptions{
		Data: []byte(`{"v":"notags"}`),
	})
	require.NoError(t, err)

	// Subscribe with ServerTagsFilter = role:admin.
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					Type: SubscriptionTypeMap,
					ServerTagsFilter: &FilterNode{
						Key: "role", Cmp: "eq", Val: "admin",
					},
				},
			}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")
	result := subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: channel,
		Type:    int32(SubscriptionTypeMap),
		Phase:   MapPhaseState,
		Limit:   100,
	})

	// Only the admin_item should be in state — viewer and no_tags filtered out.
	require.Equal(t, MapPhaseLive, result.Phase)
	require.Len(t, result.State, 1)
	require.Equal(t, "admin_item", result.State[0].Key)
}

func TestMapSubscribe_ServerTagsFilter_LiveDelivery(t *testing.T) {
	t.Parallel()
	node, broker := newTestNodeWithMapBroker(t)
	setTestMapChannelOptionsConverging(node)

	channel := "test_server_tags_live"
	ctx := context.Background()

	node.OnConnecting(func(ctx context.Context, e ConnectEvent) (ConnectReply, error) {
		return ConnectReply{Credentials: &Credentials{UserID: "user1"}}, nil
	})
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					Type: SubscriptionTypeMap,
					ServerTagsFilter: &FilterNode{
						Key: "team", Cmp: "eq", Val: "eng",
					},
				},
			}, nil)
		})
	})

	transport := newTestTransport(func() {})
	transport.sink = make(chan []byte, 100)
	transport.setProtocolVersion(ProtocolVersion2)
	newCtx := SetCredentials(context.Background(), &Credentials{UserID: "user1"})
	client, _ := newClient(newCtx, node, transport)
	connectClientV2(t, client)

	result := subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: channel,
		Type:    int32(SubscriptionTypeMap),
		Phase:   MapPhaseState,
		Limit:   100,
	})
	require.Equal(t, MapPhaseLive, result.Phase)

	// Drain subscribe reply from sink.
	drainSink(transport.sink)

	// Publish matching entry.
	_, err := broker.Publish(ctx, channel, "eng_item", MapPublishOptions{
		Data: []byte(`{"v":"eng"}`),
		Tags: map[string]string{"team": "eng"},
	})
	require.NoError(t, err)

	// Publish non-matching entry.
	_, err = broker.Publish(ctx, channel, "sales_item", MapPublishOptions{
		Data: []byte(`{"v":"sales"}`),
		Tags: map[string]string{"team": "sales"},
	})
	require.NoError(t, err)

	// Wait for delivery.
	time.Sleep(50 * time.Millisecond)

	// Check what was delivered — only eng_item should arrive.
	messages := drainSink(transport.sink)
	engFound := false
	salesFound := false
	for _, msg := range messages {
		s := string(msg)
		if strings.Contains(s, "eng_item") {
			engFound = true
		}
		if strings.Contains(s, "sales_item") {
			salesFound = true
		}
	}
	require.True(t, engFound, "eng_item should be delivered")
	require.False(t, salesFound, "sales_item should be filtered out")
}

func TestMapSubscribe_ServerAndClientTagsFilter_AND(t *testing.T) {
	t.Parallel()
	node, broker := newTestNodeWithMapBroker(t)
	setTestMapChannelOptionsConverging(node)

	channel := "test_both_filters"
	ctx := context.Background()

	// Publish entries with multiple tags.
	_, err := broker.Publish(ctx, channel, "eng_admin", MapPublishOptions{
		Data: []byte(`{"v":"eng_admin"}`),
		Tags: map[string]string{"team": "eng", "role": "admin"},
	})
	require.NoError(t, err)
	_, err = broker.Publish(ctx, channel, "eng_viewer", MapPublishOptions{
		Data: []byte(`{"v":"eng_viewer"}`),
		Tags: map[string]string{"team": "eng", "role": "viewer"},
	})
	require.NoError(t, err)
	_, err = broker.Publish(ctx, channel, "sales_admin", MapPublishOptions{
		Data: []byte(`{"v":"sales_admin"}`),
		Tags: map[string]string{"team": "sales", "role": "admin"},
	})
	require.NoError(t, err)

	// Server filter: team=eng (security boundary).
	// Client filter: role=admin (bandwidth optimization).
	// AND semantics: only eng_admin should pass.
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					Type:            SubscriptionTypeMap,
					AllowTagsFilter: true,
					ServerTagsFilter: &FilterNode{
						Key: "team", Cmp: "eq", Val: "eng",
					},
				},
			}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")
	result := subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: channel,
		Type:    int32(SubscriptionTypeMap),
		Phase:   MapPhaseState,
		Limit:   100,
		Tf: &protocol.FilterNode{
			Key: "role", Cmp: "eq", Val: "admin",
		},
	})

	// Only eng_admin passes both filters.
	require.Equal(t, MapPhaseLive, result.Phase)
	require.Len(t, result.State, 1)
	require.Equal(t, "eng_admin", result.State[0].Key)
}

func TestMapSubscribe_ServerTagsFilter_RemovalFiltering(t *testing.T) {
	t.Parallel()
	node, broker := newTestNodeWithMapBroker(t)
	setTestMapChannelOptionsConverging(node)

	channel := "test_server_tags_removal"
	ctx := context.Background()

	// Publish entries with different tags.
	_, err := broker.Publish(ctx, channel, "eng_item", MapPublishOptions{
		Data: []byte(`{"v":"eng"}`),
		Tags: map[string]string{"team": "eng"},
	})
	require.NoError(t, err)
	_, err = broker.Publish(ctx, channel, "sales_item", MapPublishOptions{
		Data: []byte(`{"v":"sales"}`),
		Tags: map[string]string{"team": "sales"},
	})
	require.NoError(t, err)

	node.OnConnecting(func(ctx context.Context, e ConnectEvent) (ConnectReply, error) {
		return ConnectReply{Credentials: &Credentials{UserID: "user1"}}, nil
	})
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					Type: SubscriptionTypeMap,
					ServerTagsFilter: &FilterNode{
						Key: "team", Cmp: "eq", Val: "eng",
					},
				},
			}, nil)
		})
	})

	transport := newTestTransport(func() {})
	transport.sink = make(chan []byte, 100)
	transport.setProtocolVersion(ProtocolVersion2)
	newCtx := SetCredentials(context.Background(), &Credentials{UserID: "user1"})
	client, _ := newClient(newCtx, node, transport)
	connectClientV2(t, client)

	result := subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: channel,
		Type:    int32(SubscriptionTypeMap),
		Phase:   MapPhaseState,
		Limit:   100,
	})
	require.Equal(t, MapPhaseLive, result.Phase)
	require.Len(t, result.State, 1) // Only eng_item.

	drainSink(transport.sink)

	// Remove eng_item (tags from state auto-populated on memory broker).
	_, err = broker.Remove(ctx, channel, "eng_item", MapRemoveOptions{})
	require.NoError(t, err)

	// Remove sales_item.
	_, err = broker.Remove(ctx, channel, "sales_item", MapRemoveOptions{})
	require.NoError(t, err)

	time.Sleep(50 * time.Millisecond)

	// Subscriber should receive removal for eng_item but NOT for sales_item.
	messages := drainSink(transport.sink)
	engRemoval := false
	salesRemoval := false
	for _, msg := range messages {
		s := string(msg)
		if strings.Contains(s, "eng_item") {
			engRemoval = true
		}
		if strings.Contains(s, "sales_item") {
			salesRemoval = true
		}
	}
	require.True(t, engRemoval, "eng_item removal should be delivered")
	require.False(t, salesRemoval, "sales_item removal should be filtered out")
}

func TestSubRefresh_ServerTagsFilter_MapUnsubscribed(t *testing.T) {
	t.Parallel()
	node, _ := newTestNodeWithMapBroker(t)
	setTestMapChannelOptionsConverging(node)

	channel := "test_refresh_filter"

	initialFilter := &FilterNode{Key: "role", Cmp: "eq", Val: "admin"}
	updatedFilter := &FilterNode{Key: "role", Cmp: "eq", Val: "viewer"}

	node.OnConnecting(func(ctx context.Context, e ConnectEvent) (ConnectReply, error) {
		return ConnectReply{
			ClientSideRefresh: true,
			Credentials:       &Credentials{UserID: "user1"},
		}, nil
	})

	unsubscribeCh := make(chan UnsubscribeEvent, 1)
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					Type:             SubscriptionTypeMap,
					ExpireAt:         time.Now().Unix() + 60,
					ServerTagsFilter: initialFilter,
				},
				ClientSideRefresh: true,
			}, nil)
		})
		client.OnSubRefresh(func(e SubRefreshEvent, cb SubRefreshCallback) {
			cb(SubRefreshReply{
				ExpireAt:         time.Now().Unix() + 60,
				ServerTagsFilter: updatedFilter,
			}, nil)
		})
		client.OnUnsubscribe(func(e UnsubscribeEvent) {
			unsubscribeCh <- e
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")
	subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: channel,
		Type:    int32(SubscriptionTypeMap),
		Phase:   MapPhaseState,
		Limit:   100,
	})

	// Refresh with a different filter — should trigger unsubscribe with state invalidated.
	rwWrapper := testReplyWriterWrapper()
	err := client.handleSubRefresh(&protocol.SubRefreshRequest{
		Channel: channel,
		Token:   "new_token",
	}, &protocol.Command{}, time.Now(), rwWrapper.rw)
	require.NoError(t, err)

	select {
	case e := <-unsubscribeCh:
		require.Equal(t, UnsubscribeCodeStateInvalidated, e.Code)
		require.Equal(t, channel, e.Channel)
	case <-time.After(time.Second):
		require.Fail(t, "timeout waiting for unsubscribe event")
	}
}

func TestSubRefresh_ServerTagsFilter_SameFilterNoUnsubscribe(t *testing.T) {
	t.Parallel()
	node, _ := newTestNodeWithMapBroker(t)
	setTestMapChannelOptionsConverging(node)

	channel := "test_refresh_same_filter"

	theFilter := &FilterNode{Key: "role", Cmp: "eq", Val: "admin"}

	node.OnConnecting(func(ctx context.Context, e ConnectEvent) (ConnectReply, error) {
		return ConnectReply{
			ClientSideRefresh: true,
			Credentials:       &Credentials{UserID: "user1"},
		}, nil
	})

	unsubscribeCh := make(chan UnsubscribeEvent, 1)
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					Type:             SubscriptionTypeMap,
					ExpireAt:         time.Now().Unix() + 60,
					ServerTagsFilter: theFilter,
				},
				ClientSideRefresh: true,
			}, nil)
		})
		client.OnSubRefresh(func(e SubRefreshEvent, cb SubRefreshCallback) {
			cb(SubRefreshReply{
				ExpireAt:         time.Now().Unix() + 60,
				ServerTagsFilter: theFilter, // same filter
			}, nil)
		})
		client.OnUnsubscribe(func(e UnsubscribeEvent) {
			unsubscribeCh <- e
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")
	subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: channel,
		Type:    int32(SubscriptionTypeMap),
		Phase:   MapPhaseState,
		Limit:   100,
	})

	// Refresh with the same filter — should NOT trigger unsubscribe.
	rwWrapper := testReplyWriterWrapper()
	err := client.handleSubRefresh(&protocol.SubRefreshRequest{
		Channel: channel,
		Token:   "new_token",
	}, &protocol.Command{}, time.Now(), rwWrapper.rw)
	require.NoError(t, err)

	// Verify we got a normal refresh reply.
	require.Len(t, rwWrapper.replies, 1)
	require.Nil(t, rwWrapper.replies[0].Error)

	// No unsubscribe should happen.
	select {
	case e := <-unsubscribeCh:
		require.Fail(t, "unexpected unsubscribe event", "code: %d", e.Code)
	case <-time.After(100 * time.Millisecond):
		// Expected — no unsubscribe.
	}
}

func TestSubRefresh_ServerTagsFilter_NilNoChange(t *testing.T) {
	t.Parallel()
	node, _ := newTestNodeWithMapBroker(t)
	setTestMapChannelOptionsConverging(node)

	channel := "test_refresh_nil_filter"

	initialFilter := &FilterNode{Key: "role", Cmp: "eq", Val: "admin"}

	node.OnConnecting(func(ctx context.Context, e ConnectEvent) (ConnectReply, error) {
		return ConnectReply{
			ClientSideRefresh: true,
			Credentials:       &Credentials{UserID: "user1"},
		}, nil
	})

	unsubscribeCh := make(chan UnsubscribeEvent, 1)
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					Type:             SubscriptionTypeMap,
					ExpireAt:         time.Now().Unix() + 60,
					ServerTagsFilter: initialFilter,
				},
				ClientSideRefresh: true,
			}, nil)
		})
		client.OnSubRefresh(func(e SubRefreshEvent, cb SubRefreshCallback) {
			cb(SubRefreshReply{
				ExpireAt: time.Now().Unix() + 60,
				// ServerTagsFilter: nil — no change.
			}, nil)
		})
		client.OnUnsubscribe(func(e UnsubscribeEvent) {
			unsubscribeCh <- e
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")
	subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: channel,
		Type:    int32(SubscriptionTypeMap),
		Phase:   MapPhaseState,
		Limit:   100,
	})

	// Refresh with nil filter — should NOT trigger unsubscribe.
	rwWrapper := testReplyWriterWrapper()
	err := client.handleSubRefresh(&protocol.SubRefreshRequest{
		Channel: channel,
		Token:   "new_token",
	}, &protocol.Command{}, time.Now(), rwWrapper.rw)
	require.NoError(t, err)

	require.Len(t, rwWrapper.replies, 1)
	require.Nil(t, rwWrapper.replies[0].Error)

	select {
	case e := <-unsubscribeCh:
		require.Fail(t, "unexpected unsubscribe event", "code: %d", e.Code)
	case <-time.After(100 * time.Millisecond):
		// Expected — no unsubscribe.
	}
}

// TestClientHandleMapPublish covers the map publish callback paths through handleMapPublish:
// not-available without handler, missing channel, callback error, success that delegates
// publish to the broker, and success where the handler supplies a Result directly.
func TestClientHandleMapPublish(t *testing.T) {
	t.Parallel()

	t.Run("ErrorNotAvailableWhenNoHandler", func(t *testing.T) {
		node, _ := newTestNodeWithMapBroker(t)
		client := newTestConnectedClientV2(t, node, "u")
		rwWrapper := testReplyWriterWrapper()
		err := client.handleMapPublish(&protocol.PublishRequest{
			Channel: "ch", Type: 1, Key: "k", Data: []byte(`{}`),
		}, &protocol.Command{}, time.Now(), rwWrapper.rw)
		require.Equal(t, ErrorNotAvailable, err)
	})

	t.Run("EmptyChannelDisconnects", func(t *testing.T) {
		node, _ := newTestNodeWithMapBroker(t)
		node.OnConnect(func(c *Client) {
			c.OnMapPublish(func(_ MapPublishEvent, cb MapPublishCallback) {
				cb(MapPublishReply{}, nil)
			})
		})
		client := newTestConnectedClientV2(t, node, "u")
		rwWrapper := testReplyWriterWrapper()
		err := client.handleMapPublish(&protocol.PublishRequest{
			Channel: "", Type: 1, Key: "k",
		}, &protocol.Command{}, time.Now(), rwWrapper.rw)
		require.Equal(t, DisconnectBadRequest, err)
	})

	t.Run("HandlerErrorIsWrittenToReply", func(t *testing.T) {
		node, _ := newTestNodeWithMapBroker(t)
		node.OnConnect(func(c *Client) {
			c.OnMapPublish(func(_ MapPublishEvent, cb MapPublishCallback) {
				cb(MapPublishReply{}, ErrorBadRequest)
			})
		})
		client := newTestConnectedClientV2(t, node, "u")
		rwWrapper := testReplyWriterWrapper()
		err := client.handleMapPublish(&protocol.PublishRequest{
			Channel: "ch", Type: 1, Key: "k", Data: []byte(`{}`),
		}, &protocol.Command{}, time.Now(), rwWrapper.rw)
		require.NoError(t, err)
		require.NotEmpty(t, rwWrapper.replies)
		require.NotNil(t, rwWrapper.replies[0].Error)
		require.Equal(t, ErrorBadRequest.Code, rwWrapper.replies[0].Error.Code)
	})

	t.Run("HandlerSuccessPublishesViaBroker", func(t *testing.T) {
		node, broker := newTestNodeWithMapBroker(t)
		invoked := make(chan struct{}, 1)
		node.OnConnect(func(c *Client) {
			c.OnMapPublish(func(e MapPublishEvent, cb MapPublishCallback) {
				invoked <- struct{}{}
				// Handler must return the key explicitly — pass the client key through.
				cb(MapPublishReply{Key: e.Key}, nil)
			})
		})
		client := newTestConnectedClientV2(t, node, "u")

		channel := "map-success"

		// Sanity: direct broker.Publish with explicit data populates state.
		_, err := broker.Publish(context.Background(), channel, "sanity", MapPublishOptions{Data: []byte(`{"v":0}`)})
		require.NoError(t, err)
		state, err := broker.ReadState(context.Background(), channel, MapReadStateOptions{Limit: 100})
		require.NoError(t, err)
		require.Len(t, state.Publications, 1, "direct publish failed before handler run")

		rwWrapper := testReplyWriterWrapper()
		err = client.handleMapPublish(&protocol.PublishRequest{
			Channel: channel, Type: 1, Key: "k1", Data: []byte(`{"v":1}`),
		}, &protocol.Command{}, time.Now(), rwWrapper.rw)
		require.NoError(t, err)

		// Confirm the handler was invoked at least once.
		select {
		case <-invoked:
		case <-time.After(time.Second):
			t.Fatal("OnMapPublish handler not invoked")
		}

		require.Len(t, rwWrapper.replies, 1)
		require.Nil(t, rwWrapper.replies[0].Error)

		// Verify broker has the new entry — confirms node.MapPublish was invoked from the callback.
		state, err = broker.ReadState(context.Background(), channel, MapReadStateOptions{Limit: 100})
		require.NoError(t, err)

		var keys []string
		for _, p := range state.Publications {
			keys = append(keys, p.Key)
		}
		require.Contains(t, keys, "k1", "k1 not found in state, got: %v", keys)
	})

	t.Run("HandlerSuppliesResultSkipsBrokerPublish", func(t *testing.T) {
		node, broker := newTestNodeWithMapBroker(t)
		node.OnConnect(func(c *Client) {
			c.OnMapPublish(func(e MapPublishEvent, cb MapPublishCallback) {
				cb(MapPublishReply{
					Key:    e.Key,
					Result: &MapUpdateResult{Position: StreamPosition{Offset: 42, Epoch: "fake"}},
				}, nil)
			})
		})
		client := newTestConnectedClientV2(t, node, "u")

		channel := "map-result-shortcut"
		rwWrapper := testReplyWriterWrapper()
		err := client.handleMapPublish(&protocol.PublishRequest{
			Channel: channel, Type: 1, Key: "k", Data: []byte(`{}`),
		}, &protocol.Command{}, time.Now(), rwWrapper.rw)
		require.NoError(t, err)
		require.Nil(t, rwWrapper.replies[0].Error)

		// Broker must NOT see the publication since the handler claimed it already published.
		state, err := broker.ReadState(context.Background(), channel, MapReadStateOptions{Limit: 100})
		require.NoError(t, err)
		require.Len(t, state.Publications, 0)
	})

	t.Run("EmptyKeyAfterReplyOverrideIsBadRequest", func(t *testing.T) {
		// Both event.Key and reply.Key are empty -> reply must include ErrorBadRequest.
		node, _ := newTestNodeWithMapBroker(t)
		node.OnConnect(func(c *Client) {
			c.OnMapPublish(func(_ MapPublishEvent, cb MapPublishCallback) {
				cb(MapPublishReply{}, nil)
			})
		})
		client := newTestConnectedClientV2(t, node, "u")
		rwWrapper := testReplyWriterWrapper()
		err := client.handleMapPublish(&protocol.PublishRequest{
			Channel: "ch", Type: 1, Key: "", Data: []byte(`{}`),
		}, &protocol.Command{}, time.Now(), rwWrapper.rw)
		require.NoError(t, err)
		require.NotNil(t, rwWrapper.replies[0].Error)
		require.Equal(t, ErrorBadRequest.Code, rwWrapper.replies[0].Error.Code)
	})
}

// TestClientHandleMapRemove covers the symmetric remove paths.
func TestClientHandleMapRemove(t *testing.T) {
	t.Parallel()

	t.Run("ErrorNotAvailableWhenNoHandler", func(t *testing.T) {
		node, _ := newTestNodeWithMapBroker(t)
		client := newTestConnectedClientV2(t, node, "u")
		rwWrapper := testReplyWriterWrapper()
		err := client.handleMapRemove(&protocol.PublishRequest{
			Channel: "ch", Type: 1, Removed: true, Key: "k",
		}, &protocol.Command{}, time.Now(), rwWrapper.rw)
		require.Equal(t, ErrorNotAvailable, err)
	})

	t.Run("EmptyChannelDisconnects", func(t *testing.T) {
		node, _ := newTestNodeWithMapBroker(t)
		node.OnConnect(func(c *Client) {
			c.OnMapRemove(func(_ MapRemoveEvent, cb MapRemoveCallback) {
				cb(MapRemoveReply{}, nil)
			})
		})
		client := newTestConnectedClientV2(t, node, "u")
		rwWrapper := testReplyWriterWrapper()
		err := client.handleMapRemove(&protocol.PublishRequest{
			Channel: "", Type: 1, Removed: true, Key: "k",
		}, &protocol.Command{}, time.Now(), rwWrapper.rw)
		require.Equal(t, DisconnectBadRequest, err)
	})

	t.Run("HandlerErrorIsWrittenToReply", func(t *testing.T) {
		node, _ := newTestNodeWithMapBroker(t)
		node.OnConnect(func(c *Client) {
			c.OnMapRemove(func(_ MapRemoveEvent, cb MapRemoveCallback) {
				cb(MapRemoveReply{}, ErrorBadRequest)
			})
		})
		client := newTestConnectedClientV2(t, node, "u")
		rwWrapper := testReplyWriterWrapper()
		err := client.handleMapRemove(&protocol.PublishRequest{
			Channel: "ch", Type: 1, Removed: true, Key: "k",
		}, &protocol.Command{}, time.Now(), rwWrapper.rw)
		require.NoError(t, err)
		require.NotEmpty(t, rwWrapper.replies)
		require.NotNil(t, rwWrapper.replies[0].Error)
		require.Equal(t, ErrorBadRequest.Code, rwWrapper.replies[0].Error.Code)
	})

	t.Run("HandlerSuccessRemovesViaBroker", func(t *testing.T) {
		node, broker := newTestNodeWithMapBroker(t)
		ctx := context.Background()
		channel := "map-remove-success"
		// Pre-populate so remove has something to act on.
		_, err := broker.Publish(ctx, channel, "kdel", MapPublishOptions{Data: []byte(`{}`)})
		require.NoError(t, err)

		node.OnConnect(func(c *Client) {
			c.OnMapRemove(func(e MapRemoveEvent, cb MapRemoveCallback) {
				// Handler must return the key explicitly — pass client key through.
				cb(MapRemoveReply{Key: e.Key}, nil)
			})
		})
		client := newTestConnectedClientV2(t, node, "u")

		rwWrapper := testReplyWriterWrapper()
		err = client.handleMapRemove(&protocol.PublishRequest{
			Channel: channel, Type: 1, Removed: true, Key: "kdel",
		}, &protocol.Command{}, time.Now(), rwWrapper.rw)
		require.NoError(t, err)
		require.Nil(t, rwWrapper.replies[0].Error)

		state, err := broker.ReadState(ctx, channel, MapReadStateOptions{Limit: 100})
		require.NoError(t, err)
		// Either entries is empty, or the kdel key marked as removed -- in either case
		// no live "kdel" entry remains.
		for _, e := range state.Publications {
			require.NotEqual(t, "kdel", e.Key)
		}
	})
}

// TestValidateAndCreateTagsFilter exercises the two error branches of
// validateAndCreateTagsFilter: tags filter not allowed for the channel, and
// invalid filter structure.
func TestValidateAndCreateTagsFilter(t *testing.T) {
	t.Parallel()
	node := defaultTestNode()
	defer func() { _ = node.Shutdown(context.Background()) }()
	client := newTestClient(t, node, "u-tags")

	// nil filter → no filter, no error.
	tf, err := client.validateAndCreateTagsFilter(&protocol.SubscribeRequest{}, true, "ch")
	require.NoError(t, err)
	require.Nil(t, tf)

	// Filter present but allowTagsFilter=false → ErrorBadRequest.
	req := &protocol.SubscribeRequest{
		Tf: &protocol.FilterNode{Op: "", Cmp: "eq", Key: "k", Val: "v"},
	}
	tf, err = client.validateAndCreateTagsFilter(req, false, "ch")
	require.Equal(t, ErrorBadRequest, err)
	require.Nil(t, tf)

	// Allowed but invalid (leaf with empty Cmp) → ErrorBadRequest.
	bad := &protocol.SubscribeRequest{
		Tf: &protocol.FilterNode{Op: "", Cmp: ""},
	}
	tf, err = client.validateAndCreateTagsFilter(bad, true, "ch")
	require.Equal(t, ErrorBadRequest, err)
	require.Nil(t, tf)

	// Allowed and valid → returns a non-nil filter.
	good := &protocol.SubscribeRequest{
		Tf: &protocol.FilterNode{Op: "", Cmp: "eq", Key: "k", Val: "v"},
	}
	tf, err = client.validateAndCreateTagsFilter(good, true, "ch")
	require.NoError(t, err)
	require.NotNil(t, tf)
}

// TestMapUserPresence_AnonymousSkipped verifies that user-presence operations
// are no-ops when the connecting client has no user ID. Otherwise MapPublish
// would reject the empty user with ErrorBadRequest — which is what surfaces
// in lobby/games-style demos that allow anonymous connections.
func TestMapUserPresence_AnonymousSkipped(t *testing.T) {
	t.Parallel()
	node, _ := newTestNodeWithMapBroker(t)
	// Anonymous client (no user ID).
	ctx, cancelFn := context.WithCancel(context.Background())
	t.Cleanup(cancelFn)
	transport := newTestTransport(cancelFn)
	transport.setProtocolVersion(ProtocolVersion2)
	client, err := newClient(SetCredentials(ctx, &Credentials{UserID: ""}), node, transport)
	require.NoError(t, err)
	require.Equal(t, "", client.user, "test setup expects empty user")

	// addMapUserPresence: must skip silently for anonymous clients.
	require.NoError(t, client.addMapUserPresence("users:games:lobby"))

	// updateMapPresence with only user-presence channel set: must also be a no-op.
	chCtx := ChannelContext{mapUserPresenceChannel: "users:games:lobby"}
	require.NoError(t, client.updateMapPresence(&ClientInfo{ClientID: client.uid}, chCtx))
}

// TestMapUserPresence_AuthenticatedPublishes is the positive counterpart:
// a client with a user ID does actually populate the user-presence channel.
func TestMapUserPresence_AuthenticatedPublishes(t *testing.T) {
	t.Parallel()
	node, broker := newTestNodeWithMapBroker(t)
	client := newTestConnectedClientV2(t, node, "user-42")
	require.Equal(t, "user-42", client.user)

	presenceChannel := "users:games:lobby"
	require.NoError(t, client.addMapUserPresence(presenceChannel))

	state, err := broker.ReadState(context.Background(), presenceChannel, MapReadStateOptions{Limit: 100})
	require.NoError(t, err)
	require.Len(t, state.Publications, 1)
	require.Equal(t, "user-42", state.Publications[0].Key)
}

// TestGetMapPageSize covers each clamp branch in getMapPageSize: default-when-zero,
// clamp-to-min, clamp-to-max, and the unmodified pass-through.
func TestGetMapPageSize(t *testing.T) {
	t.Parallel()
	node := defaultTestNode()
	defer func() { _ = node.Shutdown(context.Background()) }()
	client := newTestClient(t, node, "u-page")

	chOpts := MapChannelOptions{
		MinPageSize:     5,
		MaxPageSize:     100,
		DefaultPageSize: 25,
	}

	// Limit == 0 → default.
	require.Equal(t, 25, client.getMapPageSize(&protocol.SubscribeRequest{Limit: 0}, chOpts))
	// Limit < min → min.
	require.Equal(t, 5, client.getMapPageSize(&protocol.SubscribeRequest{Limit: 2}, chOpts))
	// Limit > max → max.
	require.Equal(t, 100, client.getMapPageSize(&protocol.SubscribeRequest{Limit: 1000}, chOpts))
	// Within bounds → unchanged.
	require.Equal(t, 50, client.getMapPageSize(&protocol.SubscribeRequest{Limit: 50}, chOpts))

	// All-zero options use library defaults — verify they're applied.
	def := client.getMapPageSize(&protocol.SubscribeRequest{Limit: 0}, MapChannelOptions{})
	require.Greater(t, def, 0)
}

// TestBuildMapChannelFlags covers the conditional flag-setting branches in
// buildMapChannelFlags by toggling each option individually.
func TestBuildMapChannelFlags(t *testing.T) {
	t.Parallel()
	node := defaultTestNode()
	defer func() { _ = node.Shutdown(context.Background()) }()
	client := newTestClient(t, node, "u-flags")

	// Baseline: every flag off.
	flags := client.buildMapChannelFlags(false, "", false, SubscribeOptions{}, SubscribeReply{})
	require.True(t, flags&flagSubscribed != 0)
	require.True(t, flags&flagMap != 0)
	require.False(t, flags&flagPositioning != 0)
	require.False(t, flags&flagDeltaAllowed != 0)
	require.False(t, flags&flagEmitJoinLeave != 0)
	require.False(t, flags&flagPushJoinLeave != 0)

	// EnablePositioning → flagPositioning.
	flags = client.buildMapChannelFlags(false, "", false, SubscribeOptions{EnablePositioning: true}, SubscribeReply{})
	require.True(t, flags&flagPositioning != 0)

	// EnableRecovery (also positioning).
	flags = client.buildMapChannelFlags(false, "", false, SubscribeOptions{EnableRecovery: true}, SubscribeReply{})
	require.True(t, flags&flagPositioning != 0)

	// deltaEnabled + Fossil → flagDeltaAllowed.
	flags = client.buildMapChannelFlags(true, string(DeltaTypeFossil), false, SubscribeOptions{}, SubscribeReply{})
	require.True(t, flags&flagDeltaAllowed != 0)

	// EmitJoinLeave / PushJoinLeave / EmitPresence.
	opts := SubscribeOptions{EmitPresence: true, EmitJoinLeave: true, PushJoinLeave: true}
	flags = client.buildMapChannelFlags(false, "", false, opts, SubscribeReply{})
	require.True(t, flags&flagEmitPresence != 0)
	require.True(t, flags&flagEmitJoinLeave != 0)
	require.True(t, flags&flagPushJoinLeave != 0)

	// Presence subscription marker.
	flags = client.buildMapChannelFlags(false, "", true, SubscribeOptions{}, SubscribeReply{})
	require.True(t, flags&flagMapPresence != 0)

	// MapClientPresenceChannel / MapUserPresenceChannel / MapRemoveClientOnUnsubscribe.
	opts = SubscribeOptions{
		MapClientPresenceChannel:     "client-pres",
		MapUserPresenceChannel:       "user-pres",
		MapRemoveClientOnUnsubscribe: true,
	}
	flags = client.buildMapChannelFlags(false, "", false, opts, SubscribeReply{})
	require.True(t, flags&flagMapClientPresence != 0)
	require.True(t, flags&flagMapUserPresence != 0)
	require.True(t, flags&flagCleanupOnUnsubscribe != 0)

	// ClientSideRefresh from reply.
	flags = client.buildMapChannelFlags(false, "", false, SubscribeOptions{}, SubscribeReply{ClientSideRefresh: true})
	require.True(t, flags&flagClientSideRefresh != 0)
}

// TestCleanupMapSubscribingAll covers the loop body of cleanupMapSubscribingAll.
// We seed mapSubscribing with two channels (one with a subscribingCh, one
// without) and verify both are removed and the channel is closed.
func TestCleanupMapSubscribingAll(t *testing.T) {
	t.Parallel()
	node := defaultTestNode()
	defer func() { _ = node.Shutdown(context.Background()) }()
	client := newTestClient(t, node, "u-cleanup")

	ch := make(chan struct{})
	client.mu.Lock()
	if client.mapSubscribing == nil {
		client.mapSubscribing = make(map[string]*mapSubscribeState)
	}
	client.mapSubscribing["a"] = &mapSubscribeState{subscribingCh: ch}
	client.mapSubscribing["b"] = &mapSubscribeState{} // no subscribingCh
	client.mu.Unlock()

	client.cleanupMapSubscribingAll()

	client.mu.Lock()
	require.Empty(t, client.mapSubscribing)
	client.mu.Unlock()

	// The seeded subscribingCh must have been closed.
	select {
	case <-ch:
	default:
		t.Fatal("subscribingCh was not closed by cleanupMapSubscribingAll")
	}
}

// TestMapSubscribe_InvalidPhase covers the default case in handleMapSubscribe
// (invalid phase value).
func TestMapSubscribe_InvalidPhase(t *testing.T) {
	t.Parallel()
	node, _ := newTestNodeWithMapBroker(t)

	channel := "test_invalid_phase"

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{Type: SubscriptionTypeMap},
			}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")

	rwWrapper := testReplyWriterWrapper()
	err := client.handleSubscribe(&protocol.SubscribeRequest{
		Channel: channel,
		Type:    int32(SubscriptionTypeMap),
		Phase:   42, // Not 0/1/2.
	}, &protocol.Command{Id: 1}, time.Now(), rwWrapper.rw)
	require.NoError(t, err)
	require.Len(t, rwWrapper.replies, 1)
	require.NotNil(t, rwWrapper.replies[0].Error)
	require.Equal(t, ErrorBadRequest.Code, rwWrapper.replies[0].Error.Code)
}

// TestMapSubscribe_StreamPhase_ConcurrentPagination covers the
// ErrorConcurrentPagination branch of handleMapStreamPhase.
func TestMapSubscribe_StreamPhase_ConcurrentPagination(t *testing.T) {
	t.Parallel()
	node, _ := newTestNodeWithMapBroker(t)
	setTestMapChannelOptionsConverging(node)

	channel := "test_stream_concurrent"

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{Type: SubscriptionTypeMap},
			}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")

	// Pre-populate mapSubscribing so handleMapStreamPhase finds existing state
	// (avoids the recovery branch and goes straight to the pagination lock check).
	client.mu.Lock()
	if client.mapSubscribing == nil {
		client.mapSubscribing = make(map[string]*mapSubscribeState)
	}
	client.mapSubscribing[channel] = &mapSubscribeState{
		options:       SubscribeOptions{Type: SubscriptionTypeMap, EnablePositioning: true, EnableRecovery: true},
		subscribingCh: make(chan struct{}),
		startedAt:     time.Now().UnixNano(),
	}
	if client.mapPaginationLocks == nil {
		client.mapPaginationLocks = make(map[string]struct{})
	}
	client.mapPaginationLocks[channel] = struct{}{} // Simulate ongoing pagination.
	client.mu.Unlock()

	rwWrapper := testReplyWriterWrapper()
	err := client.handleSubscribe(&protocol.SubscribeRequest{
		Channel: channel,
		Type:    int32(SubscriptionTypeMap),
		Phase:   MapPhaseStream,
		Limit:   5,
	}, &protocol.Command{Id: 1}, time.Now(), rwWrapper.rw)
	require.NoError(t, err)
	require.Len(t, rwWrapper.replies, 1)
	require.NotNil(t, rwWrapper.replies[0].Error)
	require.Equal(t, ErrorConcurrentPagination.Code, rwWrapper.replies[0].Error.Code)

	// Release lock for cleanup.
	client.mu.Lock()
	delete(client.mapPaginationLocks, channel)
	client.mu.Unlock()
}

// TestMapSubscribe_LivePhase_NoMapBroker covers the `getMapBroker == nil`
// branch in handleMapLivePhase.
func TestMapSubscribe_LivePhase_NoMapBroker(t *testing.T) {
	t.Parallel()
	node, _ := newTestNodeWithMapBroker(t)

	channel := "no_broker_channel"

	// Override GetMapBroker to return (nil, true) for our channel — forces
	// getMapBroker to return nil even though a default mapBroker exists.
	node.config.Map.GetMapBroker = func(ch string) (MapBroker, bool) {
		if ch == channel {
			return nil, true
		}
		return nil, false
	}

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{Type: SubscriptionTypeMap},
			}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")

	rwWrapper := testReplyWriterWrapper()
	err := client.handleSubscribe(&protocol.SubscribeRequest{
		Channel: channel,
		Type:    int32(SubscriptionTypeMap),
		Phase:   MapPhaseLive,
	}, &protocol.Command{Id: 1}, time.Now(), rwWrapper.rw)
	require.NoError(t, err)
	require.Len(t, rwWrapper.replies, 1)
	require.NotNil(t, rwWrapper.replies[0].Error)
	require.Equal(t, ErrorNotAvailable.Code, rwWrapper.replies[0].Error.Code)
}

// TestMapSubscribe_LivePhase_StateEpochMismatch covers the epoch-mismatch
// branch in handleMapLivePhase when in-progress mapSubscribing state has a
// captured epoch and the client sends a different one.
func TestMapSubscribe_LivePhase_StateEpochMismatch(t *testing.T) {
	t.Parallel()
	node, broker := newTestNodeWithMapBroker(t)
	setTestMapChannelOptionsConverging(node)

	channel := "test_live_state_epoch_mismatch"
	ctx := context.Background()

	// Populate enough entries that page 1 returns a cursor and the state
	// remains in mapSubscribing (not yet promoted to live).
	for i := 0; i < 10; i++ {
		_, err := broker.Publish(ctx, channel, string(rune('a'+i)), MapPublishOptions{
			Data: []byte(`{"v":"data"}`),
		})
		require.NoError(t, err)
	}

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{Type: SubscriptionTypeMap},
			}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")

	// Advance state via STATE phase first page (small limit → cursor remains,
	// state is captured with epoch).
	page1 := subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: channel,
		Type:    int32(SubscriptionTypeMap),
		Phase:   MapPhaseState,
		Limit:   2,
	})
	require.Equal(t, MapPhaseState, page1.Phase)
	require.NotEmpty(t, page1.Cursor)

	// Sanity: state present with captured epoch.
	client.mu.RLock()
	st, ok := client.mapSubscribing[channel]
	client.mu.RUnlock()
	require.True(t, ok)
	require.NotEmpty(t, st.epoch)

	// Send LIVE phase with non-empty but wrong epoch — must hit the
	// state.epoch != req.Epoch branch.
	rwWrapper := testReplyWriterWrapper()
	err := client.handleSubscribe(&protocol.SubscribeRequest{
		Channel: channel,
		Type:    int32(SubscriptionTypeMap),
		Phase:   MapPhaseLive,
		Epoch:   "definitely-not-the-right-epoch",
		Offset:  page1.Offset,
	}, &protocol.Command{Id: 1}, time.Now(), rwWrapper.rw)
	require.NoError(t, err)
	require.Len(t, rwWrapper.replies, 1)
	require.NotNil(t, rwWrapper.replies[0].Error)
	require.Equal(t, ErrorUnrecoverablePosition.Code, rwWrapper.replies[0].Error.Code)

	// Subscribing state must be cleaned up.
	client.mu.RLock()
	_, present := client.mapSubscribing[channel]
	client.mu.RUnlock()
	require.False(t, present)
}

// TestMapSubscribe_Streamless_StateToLive_ClientTagsFilter covers the
// streamless-with-tags-filter branch in handleMapTransitionToLive
// (allowStreamless==true && sub.tagsFilter != nil).
func TestMapSubscribe_Streamless_StateToLive_ClientTagsFilter(t *testing.T) {
	t.Parallel()
	node, broker := newTestNodeWithMapBroker(t)
	// Ephemeral mode → streamless. Also enable AllowTagsFilter via reply options.

	channel := "test_streamless_client_tags"
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		_, err := broker.Publish(ctx, channel, string(rune('a'+i)), MapPublishOptions{
			Data: []byte(`{"v":"data"}`),
			Tags: map[string]string{"role": "admin"},
		})
		require.NoError(t, err)
	}

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					Type:            SubscriptionTypeMap,
					AllowTagsFilter: true,
				},
			}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")

	result := subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: channel,
		Type:    int32(SubscriptionTypeMap),
		Phase:   MapPhaseState,
		Limit:   100,
		Tf:      &protocol.FilterNode{Op: "", Cmp: "eq", Key: "role", Val: "admin"},
	})
	require.Equal(t, MapPhaseLive, result.Phase)
	require.False(t, result.Recoverable) // Streamless.
	require.Len(t, result.State, 3)
	require.Contains(t, client.channels, channel)
}

// TestMapSubscribe_StateToLive_TagsFilter_OnRecoveredPubs covers the
// server/client tags filter loops applied to publications recovered during
// the live transition. Uses STATE→LIVE flow so the state struct captures
// both server and client filters and they apply to gap publications.
func TestMapSubscribe_StateToLive_TagsFilter_OnRecoveredPubs(t *testing.T) {
	t.Parallel()
	node, broker := newTestNodeWithMapBroker(t)
	setTestMapChannelOptionsConverging(node)

	channel := "test_state_to_live_tags_filter"
	ctx := context.Background()

	// Multiple seed entries with passing tags so STATE pagination has multiple pages.
	for i := 0; i < 3; i++ {
		_, err := broker.Publish(ctx, channel, "seed"+string(rune('a'+i)), MapPublishOptions{
			Data: []byte(`{"v":"seed"}`),
			Tags: map[string]string{"team": "eng", "role": "admin"},
		})
		require.NoError(t, err)
	}

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					Type:             SubscriptionTypeMap,
					AllowTagsFilter:  true,
					ServerTagsFilter: &FilterNode{Key: "team", Cmp: "eq", Val: "eng"},
				},
			}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")

	// First STATE page (small limit so cursor remains and state is captured).
	page1 := subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: channel,
		Type:    int32(SubscriptionTypeMap),
		Phase:   MapPhaseState,
		Limit:   1,
		Tf:      &protocol.FilterNode{Op: "", Cmp: "eq", Key: "role", Val: "admin"},
	})
	require.Equal(t, MapPhaseState, page1.Phase)
	frozenOffset := page1.Offset
	frozenEpoch := page1.Epoch

	// Publish gap entries while paginated so stream advances past frozenOffset.
	// Mix of tags so we can verify both filters fire on the recovered set.
	tagPairs := []struct{ team, role string }{
		{"eng", "admin"},    // passes both
		{"sales", "admin"},  // server filter drops
		{"eng", "viewer"},   // client filter drops
		{"sales", "viewer"}, // both drop
	}
	for i, tp := range tagPairs {
		_, err := broker.Publish(ctx, channel, "gap"+string(rune('a'+i)), MapPublishOptions{
			Data: []byte(`{"v":"x"}`),
			Tags: map[string]string{"team": tp.team, "role": tp.role},
		})
		require.NoError(t, err)
	}

	// Last STATE page → state-to-live transition reads stream gap and applies
	// both server and client tags filters to recoveredPubs.
	page2 := subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: channel,
		Type:    int32(SubscriptionTypeMap),
		Phase:   MapPhaseState,
		Limit:   100,
		Cursor:  page1.Cursor,
		Offset:  frozenOffset,
		Epoch:   frozenEpoch,
		Tf:      &protocol.FilterNode{Op: "", Cmp: "eq", Key: "role", Val: "admin"},
	})
	require.Equal(t, MapPhaseLive, page2.Phase)
	for _, pub := range page2.Publications {
		require.Equal(t, "eng", pub.Tags["team"], "server filter must drop non-eng")
		require.Equal(t, "admin", pub.Tags["role"], "client filter must drop non-admin")
	}
}

// TestMapSubscribe_Recovery_FossilDelta covers the makeRecoveredMapPubsDeltaFossil
// invocation inside handleMapTransitionToLive (deltaEnabled + Fossil).
func TestMapSubscribe_Recovery_FossilDelta(t *testing.T) {
	t.Parallel()
	node, broker := newTestNodeWithMapBroker(t)
	setTestMapChannelOptionsConverging(node)

	channel := "test_recovery_fossil"
	ctx := context.Background()

	// Initial pub anchors the stream offset/epoch we'll resume from.
	res, err := broker.Publish(ctx, channel, "seed", MapPublishOptions{
		Data: []byte(`{"v":"seed"}`),
	})
	require.NoError(t, err)
	startOffset := res.Position.Offset
	startEpoch := res.Position.Epoch

	// Publish a few updates to the same key so Fossil per-key delta has prior data.
	for i := 0; i < 3; i++ {
		_, err := broker.Publish(ctx, channel, "k", MapPublishOptions{
			Data: []byte(`{"v":"This is a fossil delta payload that needs enough length to compress, version=` + string(rune('A'+i)) + `"}`),
		})
		require.NoError(t, err)
	}

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					Type:              SubscriptionTypeMap,
					AllowedDeltaTypes: []DeltaType{DeltaTypeFossil},
				},
			}, nil)
		})
	})

	// Use Protobuf transport so makeRecoveredMapPubsDeltaFossil's binary delta
	// fits the response without needing to JSON-escape.
	transport := newTestTransport(func() {})
	transport.setProtocolVersion(ProtocolVersion2)
	transport.setProtocolType(ProtocolTypeProtobuf)
	newCtx := SetCredentials(context.Background(), &Credentials{UserID: "user1"})
	client, _ := newClient(newCtx, node, transport)
	connectClientV2(t, client)

	rwWrapper := testReplyWriterWrapper()
	err = client.handleSubscribe(&protocol.SubscribeRequest{
		Channel: channel,
		Type:    int32(SubscriptionTypeMap),
		Phase:   MapPhaseLive,
		Offset:  startOffset,
		Epoch:   startEpoch,
		Recover: true,
		Delta:   string(DeltaTypeFossil),
	}, &protocol.Command{Id: 1}, time.Now(), rwWrapper.rw)
	require.NoError(t, err)
	require.Len(t, rwWrapper.replies, 1)
	require.Nil(t, rwWrapper.replies[0].Error)
	result := rwWrapper.replies[0].Subscribe
	require.Equal(t, MapPhaseLive, result.Phase)
	require.True(t, result.Delta)
	require.Greater(t, len(result.Publications), 0)
	// First publication for the key is full; subsequent same-key pubs are deltas.
	hasDelta := false
	for _, pub := range result.Publications {
		if pub.Delta {
			hasDelta = true
			break
		}
	}
	require.True(t, hasDelta, "expected at least one fossil-delta publication")
}

// TestMapSubscribe_StateToLive_PublishDebounce covers the PublishDebounce
// branch in handleMapTransitionToLive.
func TestMapSubscribe_StateToLive_PublishDebounce(t *testing.T) {
	t.Parallel()
	node, broker := newTestNodeWithMapBroker(t)

	channel := "test_publish_debounce"
	ctx := context.Background()

	_, err := broker.Publish(ctx, channel, "k", MapPublishOptions{
		Data: []byte(`{"v":"x"}`),
	})
	require.NoError(t, err)

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					Type:                          SubscriptionTypeMap,
					ClientPublishDebounceInterval: 250 * time.Millisecond,
				},
			}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")
	result := subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: channel,
		Type:    int32(SubscriptionTypeMap),
		Phase:   MapPhaseState,
		Limit:   100,
	})
	require.Equal(t, MapPhaseLive, result.Phase)
	require.Equal(t, uint32(250), result.PublishDebounce)
}

// TestUpdateMapPresence_BothChannels exercises the client + user presence
// branches of updateMapPresence (used by the periodic presence refresh).
func TestUpdateMapPresence_BothChannels(t *testing.T) {
	t.Parallel()
	node, broker := newTestNodeWithMapBroker(t)

	clientPresenceCh := "clients:room"
	userPresenceCh := "users:room"

	client := newTestConnectedClientV2(t, node, "user42")
	info := &ClientInfo{ClientID: client.uid, UserID: client.user}
	chCtx := ChannelContext{
		mapClientPresenceChannel: clientPresenceCh,
		mapUserPresenceChannel:   userPresenceCh,
	}
	require.NoError(t, client.updateMapPresence(info, chCtx))

	// Client presence channel keyed by client ID.
	state, err := broker.ReadState(context.Background(), clientPresenceCh, MapReadStateOptions{Limit: 100})
	require.NoError(t, err)
	require.Len(t, state.Publications, 1)
	require.Equal(t, client.uid, state.Publications[0].Key)

	// User presence channel keyed by user ID.
	state, err = broker.ReadState(context.Background(), userPresenceCh, MapReadStateOptions{Limit: 100})
	require.NoError(t, err)
	require.Len(t, state.Publications, 1)
	require.Equal(t, "user42", state.Publications[0].Key)
}

// TestMapSubscribe_StreamPhase_TagsFilter_IntermediatePage covers the
// per-page tags-filter branch inside handleMapStreamPhase (intermediate
// page path, not transition-to-live).
func TestMapSubscribe_StreamPhase_TagsFilter_IntermediatePage(t *testing.T) {
	t.Parallel()
	node, broker := newTestNodeWithMapBroker(t)
	setTestMapChannelOptionsConverging(node)

	channel := "test_stream_tags_intermediate"
	ctx := context.Background()

	// Publish 50 entries with mixed tags so a small STREAM page stays
	// intermediate (not yet at streamStart).
	var startEpoch string
	var startOffset uint64
	res, err := broker.Publish(ctx, channel, "seed", MapPublishOptions{
		Data: []byte(`{"v":"seed"}`),
		Tags: map[string]string{"role": "viewer"},
	})
	require.NoError(t, err)
	startEpoch = res.Position.Epoch
	startOffset = res.Position.Offset

	for i := 0; i < 50; i++ {
		role := "admin"
		if i%2 == 0 {
			role = "viewer"
		}
		_, err := broker.Publish(ctx, channel, "key"+string(rune('A'+(i%26)))+string(rune('0'+(i/26))), MapPublishOptions{
			Data: []byte(`{"v":"x"}`),
			Tags: map[string]string{"role": role},
		})
		require.NoError(t, err)
	}

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					Type:            SubscriptionTypeMap,
					AllowTagsFilter: true,
				},
			}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")

	// Use a small page size so the first STREAM request stays intermediate.
	result := subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: channel,
		Type:    int32(SubscriptionTypeMap),
		Phase:   MapPhaseStream,
		Offset:  startOffset,
		Epoch:   startEpoch,
		Limit:   5,
		Recover: true,
		Tf:      &protocol.FilterNode{Op: "", Cmp: "eq", Key: "role", Val: "admin"},
	})
	require.Equal(t, MapPhaseStream, result.Phase)
	for _, pub := range result.Publications {
		require.Equal(t, "admin", pub.Tags["role"])
	}
}

// TestMapSubscribe_DirectLiveRecovery_ServerTagsFilter is a regression test for
// a server-side filter bypass on the direct phase=LIVE + recover=true path.
// On clean reconnect (no in-progress STATE/STREAM state on the server),
// handleMapLivePhase took the !hasState branch and never propagated
// reply.Options.ServerTagsFilter into the subInfo used for delivery, so the
// reconnecting subscriber received publications that should have been filtered
// out by the server-side RBAC filter.
func TestMapSubscribe_DirectLiveRecovery_ServerTagsFilter(t *testing.T) {
	t.Parallel()
	node, broker := newTestNodeWithMapBroker(t)
	setTestMapChannelOptionsConverging(node)

	channel := "test_live_recovery_server_filter"
	ctx := context.Background()

	// Anchor offset/epoch.
	res, err := broker.Publish(ctx, channel, "seed", MapPublishOptions{
		Data: []byte(`{"v":"seed"}`),
		Tags: map[string]string{"team": "eng"},
	})
	require.NoError(t, err)
	startOffset := res.Position.Offset
	startEpoch := res.Position.Epoch

	// Mix of matching (team=eng) and non-matching (team=sales) entries in the gap.
	for i, team := range []string{"eng", "sales", "eng", "sales"} {
		_, err := broker.Publish(ctx, channel, "k"+string(rune('a'+i)), MapPublishOptions{
			Data: []byte(`{"v":"x"}`),
			Tags: map[string]string{"team": team},
		})
		require.NoError(t, err)
	}

	// Server-side filter: team=eng. Client cannot override.
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					Type:             SubscriptionTypeMap,
					ServerTagsFilter: &FilterNode{Key: "team", Cmp: "eq", Val: "eng"},
				},
			}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")

	// Direct LIVE recovery — no prior STATE/STREAM. This triggers the
	// !hasState branch in handleMapLivePhase.
	result := subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: channel,
		Type:    int32(SubscriptionTypeMap),
		Phase:   MapPhaseLive,
		Offset:  startOffset,
		Epoch:   startEpoch,
		Recover: true,
	})
	require.Equal(t, MapPhaseLive, result.Phase)
	require.True(t, result.Recovered)

	for _, pub := range result.Publications {
		require.Equal(t, "eng", pub.Tags["team"],
			"server filter team=eng must drop sales entries even on direct LIVE recovery")
	}
}

// TestMapSubscribe_DisconnectRace_NoGhostSubscription stresses the race
// between handleMapTransitionToLive's late c.channels[channel] write and
// Client.close()'s c.channels snapshot. The fix re-checks c.status under
// c.mu before writing channelContext — without it, a close() that runs
// after addSubscription but before the channelContext write would snapshot
// c.channels without our entry (no cleanup queued), and the late write
// would leave a hub subscription with no cleanup path.
//
// Pattern: many short-lived clients subscribe to the same map channel and
// Disconnect concurrently. After all activity drains the hub must show
// zero subscribers — any ghost would persist forever.
func TestMapSubscribe_DisconnectRace_NoGhostSubscription(t *testing.T) {
	t.Parallel()
	node, _ := newTestNodeWithMapBroker(t)
	setTestMapChannelOptionsConverging(node)
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{Type: SubscriptionTypeMap},
			}, nil)
		})
	})

	channel := "test_map_disc_race"
	const iterations = 300

	rng := rand.New(rand.NewSource(1))
	var jitterMu sync.Mutex
	nextJitter := func() time.Duration {
		jitterMu.Lock()
		defer jitterMu.Unlock()
		return time.Duration(rng.Intn(2000)) * time.Microsecond
	}

	var wg sync.WaitGroup
	for i := 0; i < iterations; i++ {
		wg.Add(1)
		jitter := nextJitter()
		go func(i int, jitter time.Duration) {
			defer wg.Done()
			client := newTestClientV2(t, node, fmt.Sprintf("u%d", i))
			connectClientV2(t, client)

			// Spawn the disconnect with random jitter to land at varying
			// points relative to addSubscription / the channelContext write.
			go func() {
				time.Sleep(jitter)
				client.Disconnect(DisconnectForceNoReconnect)
			}()

			// Best-effort subscribe — may succeed, error, or be interrupted
			// by the close. Any panic would surface via the test harness.
			rwWrapper := testReplyWriterWrapper()
			_ = client.handleSubscribe(&protocol.SubscribeRequest{
				Channel: channel,
				Type:    int32(SubscriptionTypeMap),
				Phase:   MapPhaseState,
				Limit:   100,
			}, &protocol.Command{Id: 1}, time.Now(), rwWrapper.rw)
		}(i, jitter)
	}
	wg.Wait()

	// Hub must converge to zero subscribers — a ghost would never be cleaned.
	require.Eventually(t, func() bool {
		return node.hub.NumSubscribers(channel) == 0
	}, 15*time.Second, 50*time.Millisecond,
		"expected zero hub subscribers, got %d", node.hub.NumSubscribers(channel))
}
