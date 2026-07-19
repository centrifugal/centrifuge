package centrifuge

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/require"
)

// TestMapSubscribe_ConcurrentRecoveryCreateConsistent exercises two concurrent
// recovery stream-phase subscribes for the same channel on one client — the race
// where both pass the lock-free hasState check and reach the on-the-fly state
// creation. The reservation re-check under c.mu makes the outcome well-defined
// (one installs, the other is rejected) rather than one goroutine proceeding on a
// state shadowed by the other's. Under -race this guards the create path against
// races, and the post-churn invariant guards it against leaks.
func TestMapSubscribe_ConcurrentRecoveryCreateConsistent(t *testing.T) {
	node, broker := newTestNodeWithMapBroker(t)
	setTestMapChannelOptionsConverging(node)
	node.OnConnect(func(c *Client) {
		c.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{Options: SubscribeOptions{Type: SubscriptionTypeMap}}, nil)
		})
	})
	ctx := context.Background()

	for iter := 0; iter < 120; iter++ {
		ch := fmt.Sprintf("crc_%d", iter)
		var epoch string
		for j := 0; j < 3; j++ {
			res, err := broker.Publish(ctx, ch, fmt.Sprintf("k%d", j), MapPublishOptions{Data: []byte(`{"v":"d"}`)})
			require.NoError(t, err)
			epoch = res.Position.Epoch
		}

		client := newTestConnectedClientV2(t, node, fmt.Sprintf("u%d", iter))

		var wg sync.WaitGroup
		for i := 0; i < 2; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				rw := testReplyWriterWrapper()
				_ = client.handleSubscribe(&protocol.SubscribeRequest{
					Channel: ch,
					Type:    int32(SubscriptionTypeMap),
					Phase:   MapPhaseStream,
					Recover: true,
					Offset:  1,
					Epoch:   epoch,
					Limit:   100,
				}, &protocol.Command{Id: 1}, time.Now(), rw.rw)
			}()
		}
		wg.Wait()

		urw := testReplyWriterWrapper()
		_ = client.handleUnsubscribe(&protocol.UnsubscribeRequest{Channel: ch}, &protocol.Command{Id: 2}, time.Now(), urw.rw)
		_ = client.close(DisconnectForceNoReconnect)
	}

	require.Eventually(t, func() bool {
		return node.hub.NumSubscriptions() == 0
	}, 10*time.Second, 20*time.Millisecond,
		"map subscriptions leaked after concurrent recovery-create churn: NumSubscriptions=%d",
		node.hub.NumSubscriptions())
}
