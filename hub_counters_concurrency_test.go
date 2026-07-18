package centrifuge

import (
	"context"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestHubCounters_ConcurrentAddRemove hammers the hub's add/remove paths from
// many goroutines and checks that the O(1) counters (NumClients, NumSubscriptions)
// stay exactly consistent with a full walk of the underlying maps. A missed or
// double increment on any add/remove path would show up as a mismatch or a
// non-zero residual after everything is removed.
func TestHubCounters_ConcurrentAddRemove(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()
	h := node.hub

	walkSubs := func() int {
		total := 0
		for i := 0; i < numHubShards; i++ {
			s := h.subShards[i]
			s.mu.RLock()
			for _, subs := range s.subs {
				total += len(subs)
			}
			s.mu.RUnlock()
		}
		return total
	}
	walkClients := func() int {
		total := 0
		for i := 0; i < numHubShards; i++ {
			cs := h.connShards[i]
			cs.mu.RLock()
			for _, conns := range cs.users {
				total += len(conns)
			}
			cs.mu.RUnlock()
		}
		return total
	}

	const workers = 24
	const perWorker = 400
	const channels = 12

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			clients := make([]*Client, perWorker)
			for i := 0; i < perWorker; i++ {
				c := newTestClient(t, node, "u"+strconv.Itoa(w)+"_"+strconv.Itoa(i))
				clients[i] = c
				h.add(c)
				for ch := 0; ch < channels; ch++ {
					// Some channels shared across workers, some not — exercises both
					// the first-subscriber and additional-subscriber branches.
					chName := "c" + strconv.Itoa((w+ch)%channels)
					_, _, _ = h.addSub(chName, subInfo{client: c})
				}
			}
			// Tear it all back down.
			for i := 0; i < perWorker; i++ {
				c := clients[i]
				for ch := 0; ch < channels; ch++ {
					chName := "c" + strconv.Itoa((w+ch)%channels)
					_, _, _ = h.removeSub(chName, c, anySubGen)
				}
				h.remove(c)
			}
		}(w)
	}
	wg.Wait()

	// After a balanced add/remove, both counters and the maps must be back to zero.
	require.Equal(t, 0, h.NumClients(), "NumClients residual after balanced churn")
	require.Equal(t, 0, h.NumSubscriptions(), "NumSubscriptions residual after balanced churn")
	require.Equal(t, walkClients(), h.NumClients(), "NumClients disagrees with map walk")
	require.Equal(t, walkSubs(), h.NumSubscriptions(), "NumSubscriptions disagrees with map walk")
}
