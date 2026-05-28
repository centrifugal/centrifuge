package centrifuge

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// AllUsers (the dispatch modifier introduced for ops where the operator wants
// to act across the whole hub instead of one user bucket) has three behaviors
// the tests below pin:
//
//   1. With userID == "" and AllUsers(true), the op walks every connection
//      on every hub shard. Filters (labelFilter, clientID, sessionID) still
//      apply as narrowers within that walk.
//   2. With userID == "" and AllUsers(false) (or option omitted) the op
//      remains scoped to the anonymous-user bucket — backward-compatible
//      with pre-AllUsers semantics.
//   3. With userID != "", AllUsers is a no-op — the per-user path is always
//      taken so an operator can't accidentally fleet-disconnect when they
//      meant to target a specific user.

func waitDisconnects(t *testing.T, expected int32, counter *int32, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(counter) == expected {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	require.Equal(t, expected, atomic.LoadInt32(counter), "disconnect callbacks didn't fire on time")
}

func TestNode_Disconnect_AllUsers_FleetWide(t *testing.T) {
	t.Parallel()
	n := defaultNodeNoHandlers()
	defer func() { _ = n.Shutdown(context.Background()) }()

	var got int32
	var mu sync.Mutex
	disconnectedUsers := map[string]bool{}
	n.OnConnect(func(c *Client) {
		c.OnDisconnect(func(DisconnectEvent) {
			mu.Lock()
			disconnectedUsers[c.UserID()] = true
			mu.Unlock()
			atomic.AddInt32(&got, 1)
		})
	})

	// One anonymous and two authenticated.
	newTestConnectedClientV2(t, n, "")
	newTestConnectedClientV2(t, n, "alice")
	newTestConnectedClientV2(t, n, "bob")

	err := n.Disconnect("",
		WithCustomDisconnect(DisconnectForceNoReconnect),
		WithDisconnectAllUsers(true),
	)
	require.NoError(t, err)

	waitDisconnects(t, 3, &got, 2*time.Second)
	mu.Lock()
	require.True(t, disconnectedUsers[""], "anonymous client should be disconnected")
	require.True(t, disconnectedUsers["alice"], "alice should be disconnected")
	require.True(t, disconnectedUsers["bob"], "bob should be disconnected")
	mu.Unlock()
}

func TestNode_Disconnect_AllUsersFalse_KeepsAnonymousOnlySemantics(t *testing.T) {
	t.Parallel()
	n := defaultNodeNoHandlers()
	defer func() { _ = n.Shutdown(context.Background()) }()

	var got int32
	var mu sync.Mutex
	disconnectedUsers := map[string]bool{}
	n.OnConnect(func(c *Client) {
		c.OnDisconnect(func(DisconnectEvent) {
			mu.Lock()
			disconnectedUsers[c.UserID()] = true
			mu.Unlock()
			atomic.AddInt32(&got, 1)
		})
	})

	newTestConnectedClientV2(t, n, "")
	newTestConnectedClientV2(t, n, "alice")

	// Empty userID without AllUsers must keep targeting only anonymous.
	err := n.Disconnect("", WithCustomDisconnect(DisconnectForceNoReconnect))
	require.NoError(t, err)

	waitDisconnects(t, 1, &got, 2*time.Second)
	mu.Lock()
	require.True(t, disconnectedUsers[""], "anonymous should be disconnected")
	require.False(t, disconnectedUsers["alice"], "alice should NOT be disconnected")
	mu.Unlock()
	require.True(t, len(n.hub.UserConnections("alice")) > 0, "alice's connection should still be present")
}

func TestNode_Disconnect_AllUsers_NoOpWhenUserIDSet(t *testing.T) {
	t.Parallel()
	n := defaultNodeNoHandlers()
	defer func() { _ = n.Shutdown(context.Background()) }()

	var got int32
	var mu sync.Mutex
	disconnectedUsers := map[string]bool{}
	n.OnConnect(func(c *Client) {
		c.OnDisconnect(func(DisconnectEvent) {
			mu.Lock()
			disconnectedUsers[c.UserID()] = true
			mu.Unlock()
			atomic.AddInt32(&got, 1)
		})
	})

	newTestConnectedClientV2(t, n, "alice")
	newTestConnectedClientV2(t, n, "bob")
	newTestConnectedClientV2(t, n, "")

	// userID is set, AllUsers must be ignored — only alice goes.
	err := n.Disconnect("alice",
		WithCustomDisconnect(DisconnectForceNoReconnect),
		WithDisconnectAllUsers(true),
	)
	require.NoError(t, err)

	waitDisconnects(t, 1, &got, 2*time.Second)
	mu.Lock()
	require.True(t, disconnectedUsers["alice"])
	require.False(t, disconnectedUsers["bob"])
	require.False(t, disconnectedUsers[""])
	mu.Unlock()
}

func TestNode_Refresh_AllUsers_FleetWide(t *testing.T) {
	// Smoke test that Refresh routes through the fleet path without error
	// when AllUsers is set with empty userID. Refresh has no per-event
	// callback we can hook to count invocations, so we just assert the call
	// returns without error and existing connections remain (refresh is
	// non-destructive).
	t.Parallel()
	n := defaultNodeNoHandlers()
	defer func() { _ = n.Shutdown(context.Background()) }()

	newTestConnectedClientV2(t, n, "alice")
	newTestConnectedClientV2(t, n, "bob")

	err := n.Refresh("", WithRefreshAllUsers(true))
	require.NoError(t, err)
	require.True(t, len(n.hub.UserConnections("alice")) > 0)
	require.True(t, len(n.hub.UserConnections("bob")) > 0)
}
