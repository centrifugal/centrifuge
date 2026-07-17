package centrifuge

import (
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestConnShardNumClientsMatchesUserSets pins the equivalence NumClients relies
// on: len(clients) must equal the sum of the per-user connection sets. If add or
// remove ever stop maintaining both together, the O(1) count would silently
// drift from the real one.
func TestConnShardNumClientsMatchesUserSets(t *testing.T) {
	t.Parallel()
	sumUserSets := func(h *connShard) int {
		h.mu.RLock()
		defer h.mu.RUnlock()
		total := 0
		for _, conns := range h.users {
			total += len(conns)
		}
		return total
	}

	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()
	shard := newConnShard()

	var clients []*Client
	// Several users, several connections each — the case the old sum walked.
	for u := 0; u < 5; u++ {
		for i := 0; i < 4; i++ {
			c := newTestClient(t, node, "user"+strconv.Itoa(u))
			clients = append(clients, c)
			shard.add(c)
			require.Equal(t, sumUserSets(shard), shard.NumClients())
		}
	}
	require.Equal(t, 20, shard.NumClients())

	for _, c := range clients {
		shard.remove(c)
		require.Equal(t, sumUserSets(shard), shard.NumClients())
	}
	require.Zero(t, shard.NumClients())
	require.Zero(t, sumUserSets(shard))

	// Removing an unknown client must not corrupt the count.
	unknown := newTestClient(t, node, "ghost")
	shard.remove(unknown)
	require.Zero(t, shard.NumClients())
	require.Equal(t, sumUserSets(shard), shard.NumClients())
}

// TestSubShardNumSubscriptionsMatchesWalk pins the maintained counter against
// the sum it replaced. The counter must survive resubscribe (same client+channel
// added twice must not double count) and removal of unknown subscriptions.
func TestSubShardNumSubscriptionsMatchesWalk(t *testing.T) {
	t.Parallel()
	walk := func(s *subShard) int {
		s.mu.RLock()
		defer s.mu.RUnlock()
		total := 0
		for _, subs := range s.subs {
			total += len(subs)
		}
		return total
	}

	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()
	h := node.hub

	var clients []*Client
	for i := 0; i < 10; i++ {
		c := newTestClient(t, node, "u"+strconv.Itoa(i))
		clients = append(clients, c)
	}

	check := func(msg string) {
		t.Helper()
		for i := 0; i < numHubShards; i++ {
			require.Equal(t, walk(h.subShards[i]), h.subShards[i].NumSubscriptions(), msg)
		}
	}

	// Many clients across several channels.
	for _, c := range clients {
		for ch := 0; ch < 5; ch++ {
			_, _, _ = h.addSub("chan"+strconv.Itoa(ch), subInfo{client: c})
			check("after add")
		}
	}
	require.Equal(t, 50, h.NumSubscriptions())

	// Resubscribing the same client to the same channel must not double count.
	_, _, _ = h.addSub("chan0", subInfo{client: clients[0]})
	check("after resubscribe")
	require.Equal(t, 50, h.NumSubscriptions())

	// Removing a subscription that is not there must not decrement.
	_, _, _ = h.removeSub("nosuchchan", clients[0])
	check("after removing unknown channel")
	require.Equal(t, 50, h.NumSubscriptions())

	for _, c := range clients {
		for ch := 0; ch < 5; ch++ {
			_, _, _ = h.removeSub("chan"+strconv.Itoa(ch), c)
			check("after remove")
		}
	}
	require.Zero(t, h.NumSubscriptions())
}
