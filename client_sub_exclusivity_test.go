package centrifuge

import (
	"context"
	"testing"

	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/require"
)

// TestValidateSubscribeRequest_RejectsNormalWhileMapLoading proves the
// reservation-exclusivity gap that commitSubscription relies on: a map subscribe
// loading in c.mapSubscribing must block a concurrent normal subscribe for the
// same channel. Otherwise both reserve (mapSubscribing and c.channels), and when
// the map sub goes live commitSubscription overwrites c.channels — discarding the
// normal reservation and orphaning its subscribingCh (a waiting unsubscribe then
// hangs on the 5s timeout).
func TestValidateSubscribeRequest_RejectsNormalWhileMapLoading(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()
	client := newTestClientV2(t, node, "u")
	connectClientV2(t, client)
	const ch = "x"

	// Simulate a map subscribe in progress for ch (reserved, not yet live).
	client.mu.Lock()
	if client.mapSubscribing == nil {
		client.mapSubscribing = map[string]*mapSubscribeState{}
	}
	client.mapSubscribing[ch] = &mapSubscribeState{subscribingCh: make(chan struct{}), subGen: 1}
	client.mu.Unlock()

	// A concurrent normal subscribe for the same channel must be rejected.
	replyErr, disconnect := client.validateSubscribeRequest(&protocol.SubscribeRequest{Channel: ch})
	require.Nil(t, disconnect)
	require.Equal(t, ErrorAlreadySubscribed, replyErr,
		"normal subscribe must be rejected while a map subscribe is loading the same channel")

	// And it must not have reserved c.channels behind the map subscribe's back.
	client.mu.RLock()
	_, reserved := client.channels[ch]
	client.mu.RUnlock()
	require.False(t, reserved,
		"normal subscribe must not reserve c.channels while a map subscribe is loading")
}
