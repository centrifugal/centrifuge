package centrifuge

import (
	"testing"
	"time"

	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/require"
)

// TestSharedPollTrack_AfterUnsubscribeLeak proves that a track whose OnTrack
// handler is still in flight can commit keyed state after the channel has been
// unsubscribed. The commit re-creates c.keyed.trackedKeys[channel] (and a hub
// track reservation) with no re-check that the channel is still a live keyed
// subscription; close() only cleans keyed channels still present in c.channels,
// so the re-added state leaks past unsubscribe/disconnect.
func TestSharedPollTrack_AfterUnsubscribeLeak(t *testing.T) {
	node := newTestNodeWithSharedPoll(t)

	var savedTrackCb TrackCallback
	trackReady := make(chan struct{})
	node.OnConnect(func(c *Client) {
		c.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options:           SubscribeOptions{ExpireAt: time.Now().Unix() + 3600},
				ClientSideRefresh: true,
			}, nil)
		})
		c.OnTrack(func(e TrackEvent, cb TrackCallback) {
			savedTrackCb = cb
			close(trackReady)
		})
	})

	client := newTestClientV2(t, node, "u")
	connectClientV2(t, client)

	const ch = "sp:trackleak"
	subscribeSharedPollClient(t, client, ch)

	// Track a key; OnTrack captures the callback and returns.
	go func() {
		rw := testReplyWriterWrapper()
		_ = client.handleSubRefresh(&protocol.SubRefreshRequest{
			Channel: ch,
			Type:    typeTrack,
			Track:   []*protocol.TrackBatch{{Items: []*protocol.KeyedItem{{Key: "k1", Version: 1}}}},
		}, &protocol.Command{Id: 2}, time.Now(), rw.rw)
	}()
	<-trackReady

	// Unsubscribe while the track is in flight (cleanupKeyed removes trackedKeys[ch]).
	rwU := testReplyWriterWrapper()
	require.NoError(t, client.handleUnsubscribe(
		&protocol.UnsubscribeRequest{Channel: ch}, &protocol.Command{Id: 3}, time.Now(), rwU.rw))

	// The track commit fires now, after the channel is gone.
	savedTrackCb(TrackReply{}, nil)
	time.Sleep(50 * time.Millisecond)

	client.mu.RLock()
	var leaked bool
	if client.keyed != nil {
		_, leaked = client.keyed.trackedKeys[ch]
	}
	_, stillSubscribed := client.channels[ch]
	client.mu.RUnlock()

	require.False(t, stillSubscribed, "sanity: channel should be unsubscribed")
	require.False(t, leaked,
		"keyed track state leaked: trackedKeys[%s] present after unsubscribe raced the in-flight track", ch)

	_ = client.close(DisconnectForceNoReconnect)
}
