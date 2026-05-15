package centrifuge

import (
	"context"
	"testing"
	"time"

	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/require"
)

// TestEncodeKeyedPush_AllProtocols covers the four encoding branches in
// encodeKeyedPush: JSON+bidi (already covered), JSON+uni, Protobuf+bidi,
// Protobuf+uni.
func TestEncodeKeyedPush_AllProtocols(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		proto ProtocolType
		uni   bool
	}{
		{"JSON-bidi", ProtocolTypeJSON, false},
		{"JSON-uni", ProtocolTypeJSON, true},
		{"Protobuf-bidi", ProtocolTypeProtobuf, false},
		{"Protobuf-uni", ProtocolTypeProtobuf, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			node := defaultNodeNoHandlers()
			defer func() { _ = node.Shutdown(context.Background()) }()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			transport := newTestTransport(cancel)
			transport.setProtocolVersion(ProtocolVersion2)
			transport.setProtocolType(tc.proto)
			transport.setUnidirectional(tc.uni)
			c, err := newClient(SetCredentials(ctx, &Credentials{UserID: "u"}), node, transport)
			require.NoError(t, err)

			data, err := c.encodeKeyedPush("ch", &protocol.Publication{
				Key: "k", Data: []byte(`{"v":1}`), Version: 7,
			})
			require.NoError(t, err)
			require.NotEmpty(t, data)
		})
	}
}

// TestKeyedTrack_NoTrackHandler covers the trackHandler == nil branch.
func TestKeyedTrack_NoTrackHandler(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t)
	// Subscribe handler only, no OnTrack.
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options:           SubscribeOptions{ExpireAt: time.Now().Unix() + 3600},
				ClientSideRefresh: true,
			}, nil)
		})
	})
	client := newTestClientV2(t, node, "user1")
	connectClientV2(t, client)
	subscribeSharedPollClient(t, client, "test:channel")

	rwWrapper := testReplyWriterWrapper()
	err := client.handleSubRefresh(&protocol.SubRefreshRequest{
		Channel: "test:channel",
		Type:    typeTrack,
		Track:   []*protocol.TrackBatch{{Items: []*protocol.KeyedItem{{Key: "k", Version: 1}}}},
	}, &protocol.Command{Id: 1}, time.Now(), rwWrapper.rw)
	require.Equal(t, ErrorNotAvailable, err)
}

// TestKeyedTrack_TtlField covers the inner branch in handleTrack that sets
// res.Ttl when reply.ExpireAt > now.
func TestKeyedTrack_TtlField(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t)
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options:           SubscribeOptions{ExpireAt: time.Now().Unix() + 3600},
				ClientSideRefresh: true,
			}, nil)
		})
		client.OnTrack(func(e TrackEvent, cb TrackCallback) {
			cb(TrackReply{Batches: []TrackBatchReply{{ExpireAt: time.Now().Unix() + 60}}}, nil)
		})
	})
	client := newTestClientV2(t, node, "user1")
	connectClientV2(t, client)
	subscribeSharedPollClient(t, client, "test:channel")

	res := trackSharedPollClientWithReply(t, client, "test:channel", []*protocol.KeyedItem{{Key: "k", Version: 1}})
	require.True(t, res.Expires)
	require.Greater(t, res.Ttl, uint32(0))
}

// TestKeyedUntrack_InvokesUntrackHandler covers the optional untrackHandler
// invocation in handleUntrack and the minTrackExpireAt cleanup branch.
func TestKeyedUntrack_InvokesUntrackHandler(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t)
	called := make(chan UntrackEvent, 1)
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options:           SubscribeOptions{ExpireAt: time.Now().Unix() + 3600},
				ClientSideRefresh: true,
			}, nil)
		})
		client.OnTrack(func(e TrackEvent, cb TrackCallback) {
			// Use ExpireAt so minTrackExpireAt gets populated and exercised on cleanup.
			cb(TrackReply{Batches: []TrackBatchReply{{ExpireAt: time.Now().Unix() + 60}}}, nil)
		})
		client.OnUntrack(func(e UntrackEvent) {
			called <- e
		})
	})
	client := newTestClientV2(t, node, "user1")
	connectClientV2(t, client)
	subscribeSharedPollClient(t, client, "test:channel")
	trackSharedPollClient(t, client, "test:channel", []*protocol.KeyedItem{
		{Key: "k", Version: 1},
	})

	untrackSharedPollClient(t, client, "test:channel", []string{"k"})
	select {
	case ev := <-called:
		require.Equal(t, "test:channel", ev.Channel)
		require.Equal(t, []string{"k"}, ev.Keys)
	case <-time.After(time.Second):
		t.Fatal("untrack handler not invoked")
	}

	// minTrackExpireAt entry for the channel must have been removed.
	client.mu.RLock()
	_, present := client.keyed.minTrackExpireAt["test:channel"]
	client.mu.RUnlock()
	require.False(t, present)
}

// TestCleanupKeyed_NoKeyedState covers the early-return branch in cleanupKeyed
// when c.keyed is nil (client never tracked anything).
func TestCleanupKeyed_NoKeyedState(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t)
	setupSharedPollHandlers(node)
	client := newTestClientV2(t, node, "user1")
	connectClientV2(t, client)

	// Sanity: c.keyed is nil before any track call.
	client.mu.RLock()
	require.Nil(t, client.keyed)
	client.mu.RUnlock()

	// Should be a no-op with no keyed state.
	client.cleanupKeyed("anything")
}

// TestCheckTrackExpiration_EarlyReturns covers the three early-return branches
// in checkTrackExpiration: no keyed state, no minExpire entry, and re-check
// after acquiring write lock.
func TestCheckTrackExpiration_EarlyReturns(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t)
	setupSharedPollHandlers(node)
	client := newTestClientV2(t, node, "user1")
	connectClientV2(t, client)

	// 1) No keyed state at all — first early return.
	client.checkTrackExpiration("any-channel", time.Second)

	// 2) Subscribe/track to populate keyed state without ExpireAt → minExpire = 0.
	subscribeSharedPollClient(t, client, "test:channel")
	trackSharedPollClient(t, client, "test:channel", []*protocol.KeyedItem{
		{Key: "k", Version: 1},
	})

	// minTrackExpireAt[channel] is unset (TrackReply{} had no ExpireAt) → fast path returns.
	client.checkTrackExpiration("test:channel", time.Second)
}
