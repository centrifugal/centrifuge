package centrifuge

import (
	"sync"
	"testing"
	"time"

	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/require"
)

// TestMapSubConsistency_SubscribeVsUnsubscribe checks the same invariant the
// normal-subscription spec checks, but for map (keyed) subscriptions: after a
// concurrent subscribe/unsubscribe storm on the same channel settles, c.channels
// and the hub routing table must agree. Map subs currently carry subGen==0
// (never assigned), so the unsubscribe identity-match degrades to 0==0 and gives
// no protection against a lagging unsubscribe tearing down a fresh resubscribe.
func TestMapSubConsistency_SubscribeVsUnsubscribe(t *testing.T) {
	node, _ := newTestNodeWithMapBroker(t)
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{Options: SubscribeOptions{Type: SubscriptionTypeMap}}, nil)
		})
	})
	client := newTestConnectedClientV2(t, node, "u")
	const ch = "m"

	mapSubscribe := func() {
		rw := testReplyWriterWrapper()
		_ = client.handleSubscribe(&protocol.SubscribeRequest{
			Channel: ch, Type: int32(SubscriptionTypeMap), Phase: MapPhaseState, Limit: 100,
		}, &protocol.Command{Id: 1}, time.Now(), rw.rw)
	}
	unsubscribe := func() {
		rw := testReplyWriterWrapper()
		_ = client.handleUnsubscribe(&protocol.UnsubscribeRequest{Channel: ch}, &protocol.Command{Id: 1}, time.Now(), rw.rw)
	}

	const rounds = 2500
	for r := 0; r < rounds; r++ {
		var wg sync.WaitGroup
		for i := 0; i < 3; i++ {
			wg.Add(2)
			go func() { defer wg.Done(); mapSubscribe() }()
			go func() { defer wg.Done(); unsubscribe() }()
		}
		wg.Wait()

		present, _ := clientHasChannel(client, ch)
		inHub := hubHasSub(node, ch, client.ID())
		require.Equalf(t, present, inHub,
			"round %d: c.channels has %s=%v but hub has it=%v — inconsistent map subscription state",
			r, ch, present, inHub)

		unsubscribe() // reset
	}
}

// TestMapSubConsistency_NoLeakAfterDisconnect: after a client closes, it must be
// gone from the hub routing table for every map channel, even when map
// subscribe/unsubscribe raced the disconnect. A residual hub entry is a
// permanent leak.
func TestMapSubConsistency_NoLeakAfterDisconnect(t *testing.T) {
	node, _ := newTestNodeWithMapBroker(t)
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{Options: SubscribeOptions{Type: SubscriptionTypeMap}}, nil)
		})
	})
	channels := []string{"a", "b", "c"}

	const rounds = 500
	for r := 0; r < rounds; r++ {
		client := newTestConnectedClientV2(t, node, "u")
		var wg sync.WaitGroup
		for _, ch := range channels {
			wg.Add(2)
			go func(ch string) {
				defer wg.Done()
				rw := testReplyWriterWrapper()
				_ = client.handleSubscribe(&protocol.SubscribeRequest{
					Channel: ch, Type: int32(SubscriptionTypeMap), Phase: MapPhaseState, Limit: 100,
				}, &protocol.Command{Id: 1}, time.Now(), rw.rw)
			}(ch)
			go func(ch string) {
				defer wg.Done()
				rw := testReplyWriterWrapper()
				_ = client.handleUnsubscribe(&protocol.UnsubscribeRequest{Channel: ch}, &protocol.Command{Id: 1}, time.Now(), rw.rw)
			}(ch)
		}
		wg.Add(1)
		go func() { defer wg.Done(); _ = client.close(DisconnectForceNoReconnect) }()
		wg.Wait()
		_ = client.close(DisconnectForceNoReconnect)

		for _, ch := range channels {
			require.Falsef(t, hubHasSub(node, ch, client.ID()),
				"round %d: closed client still routed for map channel %s — leaked subscription", r, ch)
		}
	}
}
