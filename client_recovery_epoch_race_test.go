package centrifuge

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/require"
)

// TestSubscribeRecovery_FirstPubEpochRace guards against a data race between the
// first publication's live-broadcast path and a concurrent recovering subscribe.
//
// For offset==1 the node stamps the channel epoch onto the publication so live
// subscribers learn it. With the MemoryBroker the very same *Publication pointer
// is already stored in history (historyHub.add runs before HandlePublication),
// so a subscribe-time recovery read of offset 1 (pubToProto in isStreamRecovered)
// races that stamp. The fix stamps the epoch onto the per-broadcast proto copy
// instead of mutating the shared history object.
//
// Each iteration uses a fresh channel and fires its first publish concurrently
// with a recovering subscribe, so the epoch stamp and the recovery read overlap.
// Run under -race.
func TestSubscribeRecovery_FirstPubEpochRace(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()
	node.OnConnect(func(c *Client) {
		c.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{Options: SubscribeOptions{EnableRecovery: true, EnablePositioning: true}}, nil)
		})
	})

	const iterations = 300
	for i := 0; i < iterations; i++ {
		ch := "epochrace_" + strconv.Itoa(i)
		client := newTestClientV2(t, node, "u")
		connectClientV2(t, client)

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			// First publish to a fresh channel => offset 1 => epoch stamp path.
			_, _ = node.Publish(ch, []byte(`{"m":1}`), WithHistory(10, time.Minute))
		}()
		go func() {
			defer wg.Done()
			rw := testReplyWriterWrapper()
			_ = client.handleSubscribe(&protocol.SubscribeRequest{Channel: ch, Recover: true}, &protocol.Command{Id: 1}, time.Now(), rw.rw)
		}()
		wg.Wait()
		_ = client.close(DisconnectForceNoReconnect)
	}

	require.Eventually(t, func() bool { return node.hub.NumSubscriptions() == 0 }, 4*time.Second, 10*time.Millisecond,
		"leaked hub subscription: NumSubscriptions=%d", node.hub.NumSubscriptions())
}
