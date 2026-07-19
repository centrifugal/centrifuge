package centrifuge

import (
	"context"
	"testing"

	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/require"
)

// TestMapSubscribe_RecoveryStreamStateHasSubGen guards against a leaked map
// subscription. The recovery-mode stream phase creates its mapSubscribeState on
// the fly; that state must carry a real (non-zero) subGen, exactly like the
// normal state phase. If it were 0, handleMapTransitionToLive would mint a fresh
// generation at go-live time that no longer matches the targetSubGen an
// unsubscribe captured from the state while it was still loading — the
// unsubscribe would fail its identity check and leave the subscription in place.
func TestMapSubscribe_RecoveryStreamStateHasSubGen(t *testing.T) {
	t.Parallel()
	node, broker := newTestNodeWithMapBroker(t)
	setTestMapChannelOptionsConverging(node)

	channel := "test_recovery_subgen"
	ctx := context.Background()

	// Publish enough entries that the stream is well ahead, so a recovery stream
	// subscribe from offset 0 with a small page does NOT immediately go live and
	// instead stays in the loading (mapSubscribing) phase.
	var epoch string
	for i := 0; i < 20; i++ {
		res, err := broker.Publish(ctx, channel, string(rune('a'+i)), MapPublishOptions{
			Data: []byte(`{"v":"d"}`),
		})
		require.NoError(t, err)
		epoch = res.Position.Epoch
	}

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{Options: SubscribeOptions{Type: SubscriptionTypeMap}}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")

	// First request is a recovery stream phase (no prior state), small limit so it
	// stays loading rather than transitioning straight to LIVE.
	result := subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: channel,
		Type:    int32(SubscriptionTypeMap),
		Phase:   MapPhaseStream,
		Offset:  0,
		Epoch:   epoch,
		Limit:   2,
		Recover: true,
	})
	require.Equal(t, MapPhaseStream, result.Phase, "sub should still be loading, not live")

	client.mu.RLock()
	st, ok := client.mapSubscribing[channel]
	var subGen uint64
	if ok {
		subGen = st.subGen
	}
	client.mu.RUnlock()

	require.True(t, ok, "channel should still be in mapSubscribing (loading)")
	require.NotZero(t, subGen, "recovery-mode map subscribe state must carry a non-zero subGen")
}
