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
