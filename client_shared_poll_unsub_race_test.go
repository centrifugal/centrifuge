package centrifuge

import (
	"testing"
	"time"

	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/require"
)

// TestSharedPollSubscribe_LostUnsubscribeLeak proves that a shared-poll subscribe
// whose async handler is still in flight can have a concurrent unsubscribe get
// lost, leaving the client subscribed to a channel it unsubscribed from.
//
// Unlike normal subscriptions, the shared-poll reservation (client_shared_poll.go)
// installs an empty ChannelContext with no subscribingCh, so unsubscribe's
// "wait for in-flight subscribe" path (which requires subscribingCh != nil) never
// runs. The unsubscribe removes the empty reservation and returns; the subscribe
// then finalizes and re-adds the channel (flags, keyed state, map presence) that
// nothing will clean up until disconnect.
func TestSharedPollSubscribe_LostUnsubscribeLeak(t *testing.T) {
	node := newTestNodeWithSharedPoll(t)

	var savedCb SubscribeCallback
	cbReady := make(chan struct{})
	node.OnConnect(func(c *Client) {
		c.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			savedCb = cb
			close(cbReady)
		})
		c.OnSubRefresh(func(e SubRefreshEvent, cb SubRefreshCallback) { cb(SubRefreshReply{}, nil) })
	})

	client := newTestClientV2(t, node, "u")
	connectClientV2(t, client)

	const ch = "sp:chan"

	// Dispatch shared-poll subscribe; the handler captures the callback and returns.
	rw := testReplyWriterWrapper()
	require.NoError(t, client.handleSubscribe(
		&protocol.SubscribeRequest{Channel: ch, Type: int32(SubscriptionTypeSharedPoll)},
		&protocol.Command{Id: 1}, time.Now(), rw.rw))
	<-cbReady

	// Unsubscribe while the subscribe is still in flight. With the fix this blocks
	// on the reservation's subscribingCh until the subscribe finalizes, so run it
	// concurrently and release the subscribe from this goroutine.
	unsubDone := make(chan struct{})
	go func() {
		defer close(unsubDone)
		rwU := testReplyWriterWrapper()
		_ = client.handleUnsubscribe(
			&protocol.UnsubscribeRequest{Channel: ch}, &protocol.Command{Id: 2}, time.Now(), rwU.rw)
	}()

	// Give the unsubscribe a moment to reach its wait on subscribingCh, then let
	// the subscribe finalize. The woken unsubscribe must tear the subscription down.
	time.Sleep(50 * time.Millisecond)
	savedCb(SubscribeReply{
		Options:           SubscribeOptions{ExpireAt: time.Now().Unix() + 3600},
		ClientSideRefresh: true,
	}, nil)

	select {
	case <-unsubDone:
	case <-time.After(6 * time.Second):
		t.Fatal("unsubscribe hung waiting on the shared-poll subscribe reservation")
	}

	// The client unsubscribed — it must not remain subscribed.
	client.mu.RLock()
	cc, ok := client.channels[ch]
	subscribed := ok && channelHasFlag(cc.flags, flagSubscribed)
	client.mu.RUnlock()
	require.False(t, subscribed,
		"shared-poll subscription leaked: still subscribed after unsubscribe raced the in-flight subscribe")

	_ = client.close(DisconnectForceNoReconnect)
}
