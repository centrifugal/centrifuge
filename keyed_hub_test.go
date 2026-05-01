package centrifuge

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestKeyedHub_AddRemoveSubscriber(t *testing.T) {
	hub := newKeyedHub()
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	client := newTestClientV2(t, node, "user1")

	hub.addSubscriber("key1", client)
	require.True(t, hub.hasSubscriber("key1", client))
	require.Equal(t, 1, hub.subscriberCount("key1"))

	keyEmpty := hub.removeSubscriber("key1", client)
	require.True(t, keyEmpty)
	require.False(t, hub.hasSubscriber("key1", client))
	require.Equal(t, 0, hub.subscriberCount("key1"))
}

func TestKeyedHub_MultipleKeys(t *testing.T) {
	hub := newKeyedHub()
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	client := newTestClientV2(t, node, "user1")

	hub.addSubscriber("key1", client)
	hub.addSubscriber("key2", client)
	hub.addSubscriber("key3", client)

	require.True(t, hub.hasSubscriber("key1", client))
	require.True(t, hub.hasSubscriber("key2", client))
	require.True(t, hub.hasSubscriber("key3", client))

	keys := hub.allKeys()
	require.Len(t, keys, 3)
	require.Equal(t, 3, hub.numKeys())
}

func TestKeyedHub_MultipleSubscribers(t *testing.T) {
	hub := newKeyedHub()
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	client1 := newTestClientV2(t, node, "user1")
	client2 := newTestClientV2(t, node, "user2")
	client3 := newTestClientV2(t, node, "user3")

	hub.addSubscriber("key1", client1)
	hub.addSubscriber("key1", client2)
	hub.addSubscriber("key1", client3)

	require.Equal(t, 3, hub.subscriberCount("key1"))

	// Remove one.
	keyEmpty := hub.removeSubscriber("key1", client2)
	require.False(t, keyEmpty)
	require.Equal(t, 2, hub.subscriberCount("key1"))
	require.False(t, hub.hasSubscriber("key1", client2))
	require.True(t, hub.hasSubscriber("key1", client1))
	require.True(t, hub.hasSubscriber("key1", client3))
}

func TestKeyedHub_SubscriberCount(t *testing.T) {
	hub := newKeyedHub()
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	require.Equal(t, 0, hub.subscriberCount("nonexistent"))

	client := newTestClientV2(t, node, "user1")
	hub.addSubscriber("key1", client)
	require.Equal(t, 1, hub.subscriberCount("key1"))
}

func TestKeyedHub_AllKeys(t *testing.T) {
	hub := newKeyedHub()
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	require.Empty(t, hub.allKeys())

	client := newTestClientV2(t, node, "user1")
	hub.addSubscriber("alpha", client)
	hub.addSubscriber("beta", client)

	keys := hub.allKeys()
	require.Len(t, keys, 2)
	require.Contains(t, keys, "alpha")
	require.Contains(t, keys, "beta")
}

func TestKeyedHub_RemoveAllSubscribers(t *testing.T) {
	hub := newKeyedHub()
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	client1 := newTestClientV2(t, node, "user1")
	client2 := newTestClientV2(t, node, "user2")

	hub.addSubscriber("key1", client1)
	hub.addSubscriber("key1", client2)
	require.Equal(t, 2, hub.subscriberCount("key1"))

	hub.removeAllSubscribers("key1")
	require.Equal(t, 0, hub.subscriberCount("key1"))
	require.Equal(t, 0, hub.numKeys())
}

func TestKeyedHub_Subscribers(t *testing.T) {
	hub := newKeyedHub()
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	require.Nil(t, hub.subscribers("nonexistent"))

	client1 := newTestClientV2(t, node, "user1")
	client2 := newTestClientV2(t, node, "user2")

	hub.addSubscriber("key1", client1)
	hub.addSubscriber("key1", client2)

	subs := hub.subscribers("key1")
	require.Len(t, subs, 2)
}

func TestKeyedHub_NumKeys(t *testing.T) {
	hub := newKeyedHub()
	require.Equal(t, 0, hub.numKeys())

	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()
	client := newTestClientV2(t, node, "user1")

	hub.addSubscriber("key1", client)
	hub.addSubscriber("key2", client)
	require.Equal(t, 2, hub.numKeys())

	hub.removeSubscriber("key1", client)
	require.Equal(t, 1, hub.numKeys())
}

func TestKeyedHub_BroadcastRemovalToUsers(t *testing.T) {
	hub := newKeyedHub()
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	node.OnConnecting(func(ctx context.Context, e ConnectEvent) (ConnectReply, error) {
		return ConnectReply{}, nil
	})

	client1 := newTestClientV2(t, node, "user1")
	connectClientV2(t, client1)
	client2 := newTestClientV2(t, node, "user2")
	connectClientV2(t, client2)
	client3 := newTestClientV2(t, node, "user3")
	connectClientV2(t, client3)

	hub.addSubscriber("key1", client1)
	hub.addSubscriber("key1", client2)
	hub.addSubscriber("key1", client3)

	// Remove subscribers for user1 and user2.
	hub.removeSubscribersForUsers("key1", []string{"user1", "user2"}, nil)

	require.False(t, hub.hasSubscriber("key1", client1))
	require.False(t, hub.hasSubscriber("key1", client2))
	require.True(t, hub.hasSubscriber("key1", client3))
}

func TestKeyedHub_RemoveSubscribersForUsers_Exclude(t *testing.T) {
	hub := newKeyedHub()
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	node.OnConnecting(func(ctx context.Context, e ConnectEvent) (ConnectReply, error) {
		return ConnectReply{}, nil
	})

	client1 := newTestClientV2(t, node, "user1")
	connectClientV2(t, client1)
	client2 := newTestClientV2(t, node, "user2")
	connectClientV2(t, client2)
	client3 := newTestClientV2(t, node, "user3")
	connectClientV2(t, client3)

	hub.addSubscriber("key1", client1)
	hub.addSubscriber("key1", client2)
	hub.addSubscriber("key1", client3)

	// Exclude user1 — remove all others.
	hub.removeSubscribersForUsers("key1", nil, []string{"user1"})

	require.True(t, hub.hasSubscriber("key1", client1))
	require.False(t, hub.hasSubscriber("key1", client2))
	require.False(t, hub.hasSubscriber("key1", client3))
}

// TestKeyedHub_RemoveSubscribersForUsers_MissingKey covers the early-return
// branch when the requested key has no subscribers in the hub.
func TestKeyedHub_RemoveSubscribersForUsers_MissingKey(t *testing.T) {
	hub := newKeyedHub()
	// No-op against a hub that has no entry for "missing-key".
	require.NotPanics(t, func() {
		hub.removeSubscribersForUsers("missing-key", []string{"user1"}, nil)
	})
}

// TestKeyedHub_RemoveSubscribersForUsers_DeletesEmptyKey covers the branch
// where every subscriber is removed and the key entry itself is deleted.
func TestKeyedHub_RemoveSubscribersForUsers_DeletesEmptyKey(t *testing.T) {
	hub := newKeyedHub()
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()
	node.OnConnecting(func(ctx context.Context, e ConnectEvent) (ConnectReply, error) {
		return ConnectReply{}, nil
	})
	client := newTestClientV2(t, node, "user1")
	connectClientV2(t, client)
	hub.addSubscriber("k", client)
	require.True(t, hub.hasSubscriber("k", client))

	hub.removeSubscribersForUsers("k", []string{"user1"}, nil)

	// After all subscribers removed, key entry should be gone.
	require.False(t, hub.hasSubscriber("k", client))
	require.Zero(t, hub.subscriberCount("k"))
}

// TestKeyedHub_BroadcastRemovalToUsers_NoTargets covers the early-return when
// the key has no subscribers at all.
func TestKeyedHub_BroadcastRemovalToUsers_NoTargets(t *testing.T) {
	hub := newKeyedHub()
	require.NotPanics(t, func() {
		hub.broadcastRemovalToUsers("ch", "k", []string{"user1"}, nil)
	})
}

// TestKeyedHub_AllKeys_DedupsAcrossKeys covers the dedup branch in subscribers/
// allKeys when the same client is registered under multiple keys — each unique
// client-id should appear once in the returned set.
func TestKeyedHub_AllKeys_DedupsAcrossKeys(t *testing.T) {
	hub := newKeyedHub()
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()
	node.OnConnecting(func(ctx context.Context, e ConnectEvent) (ConnectReply, error) {
		return ConnectReply{}, nil
	})
	client := newTestClientV2(t, node, "user-multi")
	connectClientV2(t, client)
	// Same client under two keys.
	hub.addSubscriber("k1", client)
	hub.addSubscriber("k2", client)

	// collectAllClients should return the client once even though it appears
	// under two keys (drives the dedup `if _, ok := seen[uid]; ok { continue }`
	// path).
	subs := hub.collectAllClients()
	require.Len(t, subs, 1)
}
