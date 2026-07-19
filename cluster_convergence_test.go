//go:build integration

package centrifuge

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestCluster_CrossNodeServerSideSubscribeConverges validates that a
// server-side Node.Subscribe / Node.Unsubscribe issued on one node converges to
// a client connected on another node via the control PUB/SUB, and that the two
// nodes discover each other in the node registry. Run with -tags integration.
func TestCluster_CrossNodeServerSideSubscribeConverges(t *testing.T) {
	prefix := getUniquePrefix()

	node1, err := New(Config{LogLevel: LogLevelError, LogHandler: func(LogEntry) {}})
	require.NoError(t, err)
	_ = NewTestRedisBroker(t, node1, prefix, true, 6379)
	defer func() { _ = node1.Shutdown(context.Background()) }()

	node2, err := New(Config{LogLevel: LogLevelError, LogHandler: func(LogEntry) {}})
	require.NoError(t, err)
	node2.OnConnect(func(c *Client) {})
	_ = NewTestRedisBroker(t, node2, prefix, true, 6379)
	defer func() { _ = node2.Shutdown(context.Background()) }()

	// Node registry convergence: each node learns about the other.
	require.Eventually(t, func() bool {
		return node1.nodes.size() == 2 && node2.nodes.size() == 2
	}, 15*time.Second, 50*time.Millisecond, "nodes did not discover each other")

	const user = "u"
	const channel = "cross_node_ch"

	// A client connected to node2.
	client := newTestClientV2(t, node2, user)
	connectClientV2(t, client)

	isSubscribed := func() bool {
		client.mu.RLock()
		defer client.mu.RUnlock()
		cc, ok := client.channels[channel]
		return ok && channelHasFlag(cc.flags, flagSubscribed)
	}

	// Server-side subscribe issued on node1 must reach the client on node2.
	require.NoError(t, node1.Subscribe(user, channel))
	require.Eventually(t, isSubscribed, 5*time.Second, 20*time.Millisecond,
		"cross-node server-side subscribe did not converge to the client on node2")

	// Cross-node publication: publish on node1, the client on node2 must be a
	// broker subscriber (node2 subscribed to the channel on the client's behalf).
	require.Eventually(t, func() bool {
		return node2.hub.NumSubscribers(channel) == 1
	}, 5*time.Second, 20*time.Millisecond, "node2 not registered as broker subscriber")

	// Server-side unsubscribe issued on node1 must reach the client on node2.
	require.NoError(t, node1.Unsubscribe(user, channel))
	require.Eventually(t, func() bool { return !isSubscribed() }, 5*time.Second, 20*time.Millisecond,
		"cross-node server-side unsubscribe did not converge")

	_ = client.close(DisconnectForceNoReconnect)
}
