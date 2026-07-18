package centrifuge

import (
	"context"
	"sync"
	"testing"
)

// TestClient_TriggerConnectStatusRace guards a data race on c.status.
// triggerConnect finalizes the connect by writing c.status while holding only
// connectMu, but c.status is otherwise read under c.mu (e.g. Client.Unsubscribe,
// reached from Node.Unsubscribe's fan-out). Since the connecting client is
// already registered in the hub before triggerConnect runs, a concurrent
// server-side hub operation reads c.status under c.mu while triggerConnect writes
// it under connectMu — different locks, a real race. Must be run under -race.
func TestClient_TriggerConnectStatusRace(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()
	// A non-nil connect handler makes triggerConnect take the branch that writes
	// c.status after invoking the handler.
	node.OnConnect(func(c *Client) {})

	const rounds = 4000
	for r := 0; r < rounds; r++ {
		client := newTestClient(t, node, "u") // status == statusConnecting
		var wg sync.WaitGroup
		wg.Add(2)
		// triggerConnect writes c.status; the other goroutine reads it under c.mu,
		// the way every real reader does (Unsubscribe, Subscribe, close, ...). Read
		// the field directly so the test needs no started writer.
		go func() { defer wg.Done(); client.triggerConnect() }()
		go func() {
			defer wg.Done()
			client.mu.RLock()
			_ = client.status
			client.mu.RUnlock()
		}()
		wg.Wait()
		_ = client.close(DisconnectForceNoReconnect)
	}
}
