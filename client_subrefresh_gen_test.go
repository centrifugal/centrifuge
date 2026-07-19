package centrifuge

import (
	"context"
	"testing"
	"time"

	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/require"
)

// TestSubRefresh_StaleCallbackDoesNotClobberResubscribe guards the sub-refresh
// callback's write-back to c.channels. The SubRefreshHandler is asynchronous
// (commonly an HTTP proxy round trip). If, while it is in flight, the client
// unsubscribes and resubscribes the same channel, the fresh subscription gets a
// new subGen. A stale sub-refresh — validated against the OLD subscription —
// must not overwrite the fresh subscription's expireAt/info. handleSubRefresh
// gen-matches its write-back (mirrors checkPosition); without it the stale
// expireAt lands on the resubscription.
func TestSubRefresh_StaleCallbackDoesNotClobberResubscribe(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	nowUnix := time.Now().Unix()
	const freshTTL = 3600
	const staleTTL = 7200
	freshExpireAt := nowUnix + freshTTL
	staleExpireAt := nowUnix + staleTTL

	var savedCb SubRefreshCallback
	cbReady := make(chan struct{})

	node.OnConnect(func(c *Client) {
		c.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				ClientSideRefresh: true,
				Options:           SubscribeOptions{ExpireAt: freshExpireAt},
			}, nil)
		})
		c.OnSubRefresh(func(e SubRefreshEvent, cb SubRefreshCallback) {
			// Capture the callback; fire it later, after a resubscribe.
			savedCb = cb
			close(cbReady)
		})
	})

	const ch = "sr"
	client := newTestClientV2(t, node, "u")
	connectClientV2(t, client)

	subscribe := func() {
		rw := testReplyWriterWrapper()
		require.NoError(t, client.handleSubscribe(&protocol.SubscribeRequest{Channel: ch}, &protocol.Command{Id: 1}, time.Now(), rw.rw))
	}
	channelExpireAt := func() (int64, bool) {
		client.mu.RLock()
		defer client.mu.RUnlock()
		cc, ok := client.channels[ch]
		return cc.expireAt, ok && channelHasFlag(cc.flags, flagSubscribed)
	}

	// First subscription (subGen A) with a client-side-refresh token.
	subscribe()
	exp, ok := channelExpireAt()
	require.True(t, ok)
	require.Equal(t, freshExpireAt, exp)

	// Sub-refresh dispatched; handler captures the callback and returns.
	rw := testReplyWriterWrapper()
	require.NoError(t, client.handleSubRefresh(&protocol.SubRefreshRequest{Channel: ch, Token: "tok"}, &protocol.Command{Id: 2}, time.Now(), rw.rw))
	<-cbReady

	// Unsubscribe + resubscribe the same channel in the refresh window (subGen B).
	rwU := testReplyWriterWrapper()
	require.NoError(t, client.handleUnsubscribe(&protocol.UnsubscribeRequest{Channel: ch}, &protocol.Command{Id: 3}, time.Now(), rwU.rw))
	subscribe()
	exp, ok = channelExpireAt()
	require.True(t, ok)
	require.Equal(t, freshExpireAt, exp)

	// The stale sub-refresh (for subGen A) fires now. It must NOT touch subGen B.
	savedCb(SubRefreshReply{ExpireAt: staleExpireAt}, nil)

	exp, ok = channelExpireAt()
	require.True(t, ok)
	require.Equal(t, freshExpireAt, exp,
		"stale sub-refresh clobbered the resubscription's expireAt (got %d, want %d)", exp, freshExpireAt)

	_ = client.close(DisconnectForceNoReconnect)
}

// TestCheckSubscriptionExpiration_StaleCallbackDoesNotClobberResubscribe is the
// server-side (periodic tick) analog of the test above: the async
// SubRefreshHandler for a server-side subscription must not write its expireAt
// back onto a subscription that was resubscribed (fresh subGen) in the window.
func TestCheckSubscriptionExpiration_StaleCallbackDoesNotClobberResubscribe(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	nowUnix := time.Now().Unix()
	freshExpireAt := nowUnix + 3600
	staleExpireAt := nowUnix + 7200

	var savedCb SubRefreshCallback
	cbReady := make(chan struct{})

	node.OnConnect(func(c *Client) {
		// Server-side refresh (ClientSideRefresh stays false).
		c.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{Options: SubscribeOptions{ExpireAt: freshExpireAt}}, nil)
		})
		c.OnSubRefresh(func(e SubRefreshEvent, cb SubRefreshCallback) {
			savedCb = cb
			close(cbReady)
		})
	})

	const ch = "sr2"
	client := newTestClientV2(t, node, "u")
	connectClientV2(t, client)

	subscribe := func() {
		rw := testReplyWriterWrapper()
		require.NoError(t, client.handleSubscribe(&protocol.SubscribeRequest{Channel: ch}, &protocol.Command{Id: 1}, time.Now(), rw.rw))
	}
	channelExpireAt := func() (int64, bool) {
		client.mu.RLock()
		defer client.mu.RUnlock()
		cc, ok := client.channels[ch]
		return cc.expireAt, ok && channelHasFlag(cc.flags, flagSubscribed)
	}

	subscribe()

	// Snapshot the live subscription (subGen A) but with an already-expired
	// expireAt, as the periodic tick would present when the token is due.
	client.mu.RLock()
	snap := client.channels[ch]
	client.mu.RUnlock()
	snap.expireAt = nowUnix - 100

	go client.checkSubscriptionExpiration(ch, snap, 0, func(bool) {})
	<-cbReady

	// Unsubscribe + resubscribe in the refresh window (subGen B).
	rwU := testReplyWriterWrapper()
	require.NoError(t, client.handleUnsubscribe(&protocol.UnsubscribeRequest{Channel: ch}, &protocol.Command{Id: 3}, time.Now(), rwU.rw))
	subscribe()
	exp, ok := channelExpireAt()
	require.True(t, ok)
	require.Equal(t, freshExpireAt, exp)

	// Stale refresh (for subGen A) fires — must not touch subGen B.
	savedCb(SubRefreshReply{ExpireAt: staleExpireAt}, nil)

	exp, ok = channelExpireAt()
	require.True(t, ok)
	require.Equal(t, freshExpireAt, exp,
		"stale server-side sub-refresh clobbered the resubscription's expireAt (got %d, want %d)", exp, freshExpireAt)

	_ = client.close(DisconnectForceNoReconnect)
}
