package centrifuge

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/centrifugal/protocol"
)

// TestSubConsistency_NoDeadlockRecoveryCloseBroadcast guards the lock order
// between a recovering subscribe's close-path and the broadcast path.
//
// A positioned/recovering subscribe holds pubBufferMu (LockBufferAndReadBuffered)
// until StopBuffering. commitSubscription's closed-path (client closed mid
// subscribe) removes the hub entry, which takes subShard.mu — and the broadcast
// path holds subShard.mu while taking pubBufferMu (SyncPublication). Removing the
// hub entry with the buffer still locked inverts that order and deadlocks.
//
// Here recovery is enabled, a subscribe races close, and broadcasters publish to
// the same channel continuously. A deadlocking implementation hangs; the timeout
// turns that into a failure.
func TestSubConsistency_NoDeadlockRecoveryCloseBroadcast(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()
	node.OnConnect(func(c *Client) {
		c.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{Options: SubscribeOptions{EnableRecovery: true, EnablePositioning: true}}, nil)
		})
	})
	const ch = "b"

	stop := make(chan struct{})
	var pubWG sync.WaitGroup
	for i := 0; i < 4; i++ {
		pubWG.Add(1)
		go func() {
			defer pubWG.Done()
			for {
				select {
				case <-stop:
					return
				default:
					_, _ = node.Publish(ch, []byte(`{"m":1}`), WithHistory(10, time.Minute))
				}
			}
		}()
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		for r := 0; r < 3000; r++ {
			client := newTestClientV2(t, node, "u")
			connectClientV2(t, client)
			var wg sync.WaitGroup
			wg.Add(2)
			go func() {
				defer wg.Done()
				rw := testReplyWriterWrapper()
				_ = client.handleSubscribe(&protocol.SubscribeRequest{Channel: ch, Recover: true}, &protocol.Command{Id: 1}, time.Now(), rw.rw)
			}()
			go func() {
				defer wg.Done()
				_ = client.close(DisconnectForceNoReconnect)
			}()
			wg.Wait()
			_ = client.close(DisconnectForceNoReconnect)
		}
	}()

	select {
	case <-done:
	case <-time.After(20 * time.Second):
		close(stop)
		pubWG.Wait()
		t.Fatal("timeout — likely a lock-order deadlock between a recovering subscribe's close-path and broadcast")
	}
	close(stop)
	pubWG.Wait()
}
