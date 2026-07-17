package centrifuge

import (
	"context"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestServerSideSubscribe_CloseRace proves the subscription-state race is
// reachable WITHOUT emulation: the server-side Client.Subscribe API can be
// called from an application goroutine concurrently with the client closing.
// Server-side subscribes use no subscribingCh reservation and unsubscribe never
// waits for them, so hub.addSub (inside Subscribe) and the close()/unsubscribe
// teardown are unsynchronized.
//
// After the client is closed it must be gone from the hub. A residual entry is a
// permanent leak: hub.remove() clears the connection registry but not the
// subscription routing table, so a channel left in the hub but not torn down by
// close() references the dead client forever.
func TestServerSideSubscribe_CloseRace(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	const ch = "srv"
	const rounds = 20000
	for r := 0; r < rounds; r++ {
		client := newTestClientV2(t, node, "u"+strconv.Itoa(r))
		connectClientV2(t, client)

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			_ = client.Subscribe(ch) // server-side subscribe from an app goroutine
		}()
		go func() {
			defer wg.Done()
			_ = client.close(DisconnectForceNoReconnect) // client disconnecting
		}()
		wg.Wait()
		_ = client.close(DisconnectForceNoReconnect)

		require.Falsef(t, hubHasSub(node, ch, client.ID()),
			"round %d: closed client still in hub for %s — leaked subscription routing entry", r, ch)
	}
}
