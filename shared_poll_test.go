package centrifuge

import (
	"context"
	"testing"
	"time"

	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/require"
)

func TestSharedPollManager_TrackCreatesChannel(t *testing.T) {
	node := newTestNodeWithSharedPoll(t)
	setupSharedPollHandlers(node)

	require.NotNil(t, node.sharedPollManager)
	require.Empty(t, node.sharedPollManager.channels)

	client := newTestClientV2(t, node, "user1")
	connectClientV2(t, client)
	subscribeSharedPollClient(t, client, "test:channel")
	trackSharedPollClient(t, client, "test:channel", []*protocol.KeyedItem{
		{Key: "key1", Version: 0},
	})

	node.sharedPollManager.mu.RLock()
	_, ok := node.sharedPollManager.channels["test:channel"]
	node.sharedPollManager.mu.RUnlock()
	require.True(t, ok)
}

func TestSharedPollManager_UntrackRemovesFromIndex(t *testing.T) {
	node := newTestNodeWithSharedPoll(t)
	setupSharedPollHandlers(node)

	client := newTestClientV2(t, node, "user1")
	connectClientV2(t, client)
	subscribeSharedPollClient(t, client, "test:channel")
	trackSharedPollClient(t, client, "test:channel", []*protocol.KeyedItem{
		{Key: "key1", Version: 0},
		{Key: "key2", Version: 0},
	})

	// Untrack key1 — it's the last subscriber.
	untrackSharedPollClient(t, client, "test:channel", []string{"key1"})

	node.sharedPollManager.mu.RLock()
	s := node.sharedPollManager.channels["test:channel"]
	node.sharedPollManager.mu.RUnlock()
	require.NotNil(t, s)
	s.mu.Lock()
	_, hasKey1 := s.itemIndex["key1"]
	_, hasKey2 := s.itemIndex["key2"]
	s.mu.Unlock()
	require.False(t, hasKey1)
	require.True(t, hasKey2)
}

func TestSharedPollManager_HasChannel(t *testing.T) {
	node := newTestNodeWithSharedPoll(t)
	setupSharedPollHandlers(node)

	require.False(t, node.sharedPollManager.hasChannel("test:channel"))

	client := newTestClientV2(t, node, "user1")
	connectClientV2(t, client)
	subscribeSharedPollClient(t, client, "test:channel")
	trackSharedPollClient(t, client, "test:channel", []*protocol.KeyedItem{
		{Key: "key1", Version: 0},
	})

	require.True(t, node.sharedPollManager.hasChannel("test:channel"))
}

func TestSharedPollManager_WorkerStartsOnTrack(t *testing.T) {
	node := newTestNodeWithSharedPoll(t)
	setupSharedPollHandlers(node)

	client := newTestClientV2(t, node, "user1")
	connectClientV2(t, client)
	subscribeSharedPollClient(t, client, "test:channel")
	trackSharedPollClient(t, client, "test:channel", []*protocol.KeyedItem{
		{Key: "key1", Version: 0},
	})

	node.sharedPollManager.mu.RLock()
	s := node.sharedPollManager.channels["test:channel"]
	node.sharedPollManager.mu.RUnlock()
	s.mu.Lock()
	require.True(t, s.workerRunning)
	s.mu.Unlock()
}

func TestSharedPollManager_Close(t *testing.T) {
	node, err := New(Config{
		LogLevel:   LogLevelTrace,
		LogHandler: func(entry LogEntry) {},
		GetSharedPollChannelOptions: func(channel string) (SharedPollChannelOptions, bool) {
			return SharedPollChannelOptions{
				RefreshInterval:        100 * time.Millisecond,
				MaxKeysPerConnection:   100,
				MaxConsecutiveAbsences: 2,
			}, true
		},
	})
	require.NoError(t, err)

	node.OnSharedPoll(func(ctx context.Context, event SharedPollEvent) (SharedPollResult, error) {
		return SharedPollResult{}, nil
	})
	err = node.Run()
	require.NoError(t, err)

	setupSharedPollHandlers(node)

	client := newTestClientV2(t, node, "user1")
	connectClientV2(t, client)
	subscribeSharedPollClient(t, client, "test:channel")
	trackSharedPollClient(t, client, "test:channel", []*protocol.KeyedItem{
		{Key: "key1", Version: 0},
	})

	// Give worker time to start.
	time.Sleep(50 * time.Millisecond)

	// Shutdown should complete without hanging.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = node.Shutdown(ctx)
	require.NoError(t, err)
}

func TestSharedPollRefresh_EmptyResponse(t *testing.T) {
	node := newTestNodeWithSharedPoll(t)

	var callCount int32

	node.OnSharedPoll(func(ctx context.Context, event SharedPollEvent) (SharedPollResult, error) {
		callCount++
		return SharedPollResult{Items: nil}, nil
	})

	setupSharedPollHandlers(node)
	client := newTestClientV2(t, node, "user1")
	connectClientV2(t, client)
	subscribeSharedPollClient(t, client, "test:channel")
	trackSharedPollClient(t, client, "test:channel", []*protocol.KeyedItem{
		{Key: "key1", Version: 0},
	})

	// Wait for absence to take effect (needs 2 consecutive absences).
	require.Eventually(t, func() bool {
		node.sharedPollManager.mu.RLock()
		s, ok := node.sharedPollManager.channels["test:channel"]
		node.sharedPollManager.mu.RUnlock()
		if !ok {
			return true
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		entry, exists := s.itemIndex["key1"]
		if !exists {
			return true
		}
		return entry.consecutiveAbsences >= 1
	}, 2*time.Second, 10*time.Millisecond)
}

func TestSharedPollNotify_TriggersRefresh(t *testing.T) {
	callCh := make(chan SharedPollEvent, 10)

	node := newTestNodeWithSharedPoll(t, SharedPollChannelOptions{
		RefreshInterval:           10 * time.Second, // Long interval so timer doesn't fire.
		RefreshBatchSize:          100,
		MaxKeysPerConnection:      100,
		MaxConsecutiveAbsences:    2,
		NotificationBatchMaxSize:  10,
		NotificationBatchMaxDelay: 50 * time.Millisecond,
	})

	node.OnSharedPoll(func(ctx context.Context, event SharedPollEvent) (SharedPollResult, error) {
		callCh <- event
		items := make([]SharedPollRefreshItem, len(event.Items))
		for i, item := range event.Items {
			items[i] = SharedPollRefreshItem{
				Key:     item.Key,
				Data:    []byte(`{"v":1}`),
				Version: 1,
			}
		}
		return SharedPollResult{Items: items}, nil
	})

	setupSharedPollHandlers(node)
	client := newTestClientV2(t, node, "user1")
	connectClientV2(t, client)
	subscribeSharedPollClient(t, client, "test:channel")
	trackSharedPollClient(t, client, "test:channel", []*protocol.KeyedItem{
		{Key: "key1", Version: 0},
		{Key: "key2", Version: 0},
	})

	// Send notification for key1 only.
	node.SharedPollNotify([]SharedPollNotificationItem{
		{Channel: "test:channel", Key: "key1"},
	})

	// Should trigger a backend call within batch delay.
	select {
	case event := <-callCh:
		// Should contain key1.
		found := false
		for _, item := range event.Items {
			if item.Key == "key1" {
				found = true
			}
		}
		require.True(t, found, "notified key should be in poll event")
	case <-time.After(2 * time.Second):
		t.Fatal("expected notified refresh call")
	}
}

func TestSharedPollNotify_BatchBySize(t *testing.T) {
	callCh := make(chan SharedPollEvent, 10)

	node := newTestNodeWithSharedPoll(t, SharedPollChannelOptions{
		RefreshInterval:           10 * time.Second,
		RefreshBatchSize:          100,
		MaxKeysPerConnection:      100,
		MaxConsecutiveAbsences:    2,
		NotificationBatchMaxSize:  3,
		NotificationBatchMaxDelay: 5 * time.Second, // Long delay — batch should fire by size.
	})

	node.OnSharedPoll(func(ctx context.Context, event SharedPollEvent) (SharedPollResult, error) {
		callCh <- event
		items := make([]SharedPollRefreshItem, len(event.Items))
		for i, item := range event.Items {
			items[i] = SharedPollRefreshItem{
				Key:     item.Key,
				Data:    []byte(`{"v":1}`),
				Version: 1,
			}
		}
		return SharedPollResult{Items: items}, nil
	})

	setupSharedPollHandlers(node)
	client := newTestClientV2(t, node, "user1")
	connectClientV2(t, client)
	subscribeSharedPollClient(t, client, "test:channel")
	trackSharedPollClient(t, client, "test:channel", []*protocol.KeyedItem{
		{Key: "key1", Version: 0},
		{Key: "key2", Version: 0},
		{Key: "key3", Version: 0},
	})

	// Send 3 notifications — should trigger immediately by size.
	node.SharedPollNotify([]SharedPollNotificationItem{
		{Channel: "test:channel", Key: "key1"},
		{Channel: "test:channel", Key: "key2"},
		{Channel: "test:channel", Key: "key3"},
	})

	select {
	case event := <-callCh:
		require.Len(t, event.Items, 3)
	case <-time.After(2 * time.Second):
		t.Fatal("expected batch to fire by size")
	}
}

func TestSharedPollNotify_DeduplicatesKeys(t *testing.T) {
	callCh := make(chan SharedPollEvent, 10)

	node := newTestNodeWithSharedPoll(t, SharedPollChannelOptions{
		RefreshInterval:           10 * time.Second,
		RefreshBatchSize:          100,
		MaxKeysPerConnection:      100,
		MaxConsecutiveAbsences:    2,
		NotificationBatchMaxSize:  50,
		NotificationBatchMaxDelay: 50 * time.Millisecond,
	})

	node.OnSharedPoll(func(ctx context.Context, event SharedPollEvent) (SharedPollResult, error) {
		callCh <- event
		items := make([]SharedPollRefreshItem, len(event.Items))
		for i, item := range event.Items {
			items[i] = SharedPollRefreshItem{
				Key:     item.Key,
				Data:    []byte(`{"v":1}`),
				Version: 1,
			}
		}
		return SharedPollResult{Items: items}, nil
	})

	setupSharedPollHandlers(node)
	client := newTestClientV2(t, node, "user1")
	connectClientV2(t, client)
	subscribeSharedPollClient(t, client, "test:channel")
	trackSharedPollClient(t, client, "test:channel", []*protocol.KeyedItem{
		{Key: "key1", Version: 0},
	})

	// Send duplicate notifications for same key.
	node.SharedPollNotify([]SharedPollNotificationItem{
		{Channel: "test:channel", Key: "key1"},
		{Channel: "test:channel", Key: "key1"},
		{Channel: "test:channel", Key: "key1"},
	})

	select {
	case event := <-callCh:
		// Should be deduplicated to 1 key.
		require.Len(t, event.Items, 1)
		require.Equal(t, "key1", event.Items[0].Key)
	case <-time.After(2 * time.Second):
		t.Fatal("expected notified refresh call")
	}
}

func TestSharedPollNotify_UnknownChannelDropped(t *testing.T) {
	node := newTestNodeWithSharedPoll(t)
	setupSharedPollHandlers(node)

	// Should not panic or error.
	node.SharedPollNotify([]SharedPollNotificationItem{
		{Channel: "nonexistent:channel", Key: "key1"},
	})
}

func TestSharedPollNotify_FullFlow_DataDelivered(t *testing.T) {
	// Full flow: client subscribes + tracks → notification arrives →
	// backend polled for notified keys → data delivered to client
	// (per-connection version updated). Timer interval is long so
	// only the notification path triggers the backend call.
	node := newTestNodeWithSharedPoll(t, SharedPollChannelOptions{
		RefreshInterval:           30 * time.Second, // Won't fire during test.
		RefreshBatchSize:          100,
		MaxKeysPerConnection:      100,
		MaxConsecutiveAbsences:    2,
		NotificationBatchMaxSize:  50,
		NotificationBatchMaxDelay: 50 * time.Millisecond,
	})

	node.OnSharedPoll(func(ctx context.Context, event SharedPollEvent) (SharedPollResult, error) {
		items := make([]SharedPollRefreshItem, 0, len(event.Items))
		for _, item := range event.Items {
			switch item.Key {
			case "key1":
				items = append(items, SharedPollRefreshItem{
					Key: "key1", Data: []byte(`{"score":42}`), Version: 10,
				})
			case "key2":
				items = append(items, SharedPollRefreshItem{
					Key: "key2", Data: []byte(`{"score":99}`), Version: 20,
				})
			}
		}
		return SharedPollResult{Items: items}, nil
	})

	setupSharedPollHandlers(node)
	client := newTestClientV2(t, node, "user1")
	connectClientV2(t, client)
	subscribeSharedPollClient(t, client, "test:channel")
	trackSharedPollClient(t, client, "test:channel", []*protocol.KeyedItem{
		{Key: "key1", Version: 0},
		{Key: "key2", Version: 0},
		{Key: "key3", Version: 0}, // key3 won't be notified.
	})

	// Notify only key1 and key2.
	node.SharedPollNotify([]SharedPollNotificationItem{
		{Channel: "test:channel", Key: "key1"},
		{Channel: "test:channel", Key: "key2"},
	})

	// Verify data was delivered to client: per-connection versions updated.
	require.Eventually(t, func() bool {
		client.mu.RLock()
		defer client.mu.RUnlock()
		chanKeys := client.keyed.trackedKeys["test:channel"]
		k1 := chanKeys["key1"]
		k2 := chanKeys["key2"]
		k3 := chanKeys["key3"]
		return k1 != nil && k1.version == 10 &&
			k2 != nil && k2.version == 20 &&
			(k3 == nil || k3.version == 0) // key3 not notified — still at 0.
	}, 2*time.Second, 10*time.Millisecond)
}

func TestSharedPollNotify_FullFlow_TwoClients(t *testing.T) {
	// Two clients track the same key. Notification triggers backend poll.
	// Both clients should receive the update.
	node := newTestNodeWithSharedPoll(t, SharedPollChannelOptions{
		RefreshInterval:           30 * time.Second,
		RefreshBatchSize:          100,
		MaxKeysPerConnection:      100,
		MaxConsecutiveAbsences:    2,
		NotificationBatchMaxSize:  50,
		NotificationBatchMaxDelay: 50 * time.Millisecond,
	})

	node.OnSharedPoll(func(ctx context.Context, event SharedPollEvent) (SharedPollResult, error) {
		items := make([]SharedPollRefreshItem, 0, len(event.Items))
		for _, item := range event.Items {
			if item.Key == "key1" {
				items = append(items, SharedPollRefreshItem{
					Key: "key1", Data: []byte(`{"v":7}`), Version: 7,
				})
			}
		}
		return SharedPollResult{Items: items}, nil
	})

	setupSharedPollHandlers(node)

	client1 := newTestClientV2(t, node, "user1")
	connectClientV2(t, client1)
	subscribeSharedPollClient(t, client1, "test:channel")
	trackSharedPollClient(t, client1, "test:channel", []*protocol.KeyedItem{
		{Key: "key1", Version: 2},
	})

	client2 := newTestClientV2(t, node, "user2")
	connectClientV2(t, client2)
	subscribeSharedPollClient(t, client2, "test:channel")
	trackSharedPollClient(t, client2, "test:channel", []*protocol.KeyedItem{
		{Key: "key1", Version: 5},
	})

	node.SharedPollNotify([]SharedPollNotificationItem{
		{Channel: "test:channel", Key: "key1"},
	})

	// Both clients should receive version 7.
	require.Eventually(t, func() bool {
		client1.mu.RLock()
		k1 := client1.keyed.trackedKeys["test:channel"]["key1"]
		client1.mu.RUnlock()
		client2.mu.RLock()
		k2 := client2.keyed.trackedKeys["test:channel"]["key1"]
		client2.mu.RUnlock()
		return k1 != nil && k1.version == 7 && k2 != nil && k2.version == 7
	}, 2*time.Second, 10*time.Millisecond)
}

func TestSharedPollNotify_FullFlow_Removal(t *testing.T) {
	// Backend returns Removed=true for a notified key.
	// Client should see the item removed.
	node := newTestNodeWithSharedPoll(t, SharedPollChannelOptions{
		RefreshInterval:           30 * time.Second,
		RefreshBatchSize:          100,
		MaxKeysPerConnection:      100,
		MaxConsecutiveAbsences:    2,
		NotificationBatchMaxSize:  50,
		NotificationBatchMaxDelay: 50 * time.Millisecond,
	})

	node.OnSharedPoll(func(ctx context.Context, event SharedPollEvent) (SharedPollResult, error) {
		items := make([]SharedPollRefreshItem, 0, len(event.Items))
		for _, item := range event.Items {
			if item.Key == "key1" {
				items = append(items, SharedPollRefreshItem{
					Key: "key1", Removed: true,
				})
			}
		}
		return SharedPollResult{Items: items}, nil
	})

	setupSharedPollHandlers(node)
	client := newTestClientV2(t, node, "user1")
	connectClientV2(t, client)
	subscribeSharedPollClient(t, client, "test:channel")
	trackSharedPollClient(t, client, "test:channel", []*protocol.KeyedItem{
		{Key: "key1", Version: 3},
	})

	node.SharedPollNotify([]SharedPollNotificationItem{
		{Channel: "test:channel", Key: "key1"},
	})

	// key1 should be removed from itemIndex and hub.
	require.Eventually(t, func() bool {
		node.sharedPollManager.mu.RLock()
		s, ok := node.sharedPollManager.channels["test:channel"]
		node.sharedPollManager.mu.RUnlock()
		if !ok {
			return true
		}
		s.mu.Lock()
		_, exists := s.itemIndex["key1"]
		s.mu.Unlock()
		return !exists
	}, 2*time.Second, 10*time.Millisecond)

	// Client should no longer track key1.
	hub := node.keyedManager.getHub("test:channel")
	require.NotNil(t, hub)
	require.Equal(t, 0, hub.subscriberCount("key1"))
}

func TestSharedPollNotify_UntrackedKeyFiltered(t *testing.T) {
	callCh := make(chan SharedPollEvent, 10)

	node := newTestNodeWithSharedPoll(t, SharedPollChannelOptions{
		RefreshInterval:           10 * time.Second,
		RefreshBatchSize:          100,
		MaxKeysPerConnection:      100,
		MaxConsecutiveAbsences:    2,
		NotificationBatchMaxSize:  50,
		NotificationBatchMaxDelay: 50 * time.Millisecond,
	})

	node.OnSharedPoll(func(ctx context.Context, event SharedPollEvent) (SharedPollResult, error) {
		callCh <- event
		return SharedPollResult{}, nil
	})

	setupSharedPollHandlers(node)
	client := newTestClientV2(t, node, "user1")
	connectClientV2(t, client)
	subscribeSharedPollClient(t, client, "test:channel")
	trackSharedPollClient(t, client, "test:channel", []*protocol.KeyedItem{
		{Key: "key1", Version: 0},
	})

	// Notify for a key that is NOT tracked.
	node.SharedPollNotify([]SharedPollNotificationItem{
		{Channel: "test:channel", Key: "unknown_key"},
	})

	// The notification should be sent to notifCh, but runNotifiedRefreshCycle
	// filters it out since "unknown_key" is not in itemIndex.
	// Wait a bit — no backend call should happen.
	select {
	case <-callCh:
		t.Fatal("should not call backend for untracked key")
	case <-time.After(200 * time.Millisecond):
		// Expected — no call.
	}
}

func TestKeyedManager_GetOrCreateChannel(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	m := node.keyedManager
	opts := KeyedChannelOptions{MaxTrackedPerConnection: 42}

	s1 := m.getOrCreateChannel("ch1", opts)
	require.NotNil(t, s1)

	s2 := m.getOrCreateChannel("ch1", opts)
	require.True(t, s1 == s2, "same channel should return same instance")

	s3 := m.getOrCreateChannel("ch2", opts)
	require.True(t, s1 != s3, "different channels should return different instances")
}

func TestKeyedManager_MaxTrackedPerConnection(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	m := node.keyedManager

	// Not created yet — default 5000.
	require.Equal(t, 5000, m.maxTrackedPerConnection("nonexistent"))

	// Create with custom limit.
	m.getOrCreateChannel("ch1", KeyedChannelOptions{MaxTrackedPerConnection: 100})
	require.Equal(t, 100, m.maxTrackedPerConnection("ch1"))

	// Zero limit — default 5000.
	m.getOrCreateChannel("ch2", KeyedChannelOptions{MaxTrackedPerConnection: 0})
	require.Equal(t, 5000, m.maxTrackedPerConnection("ch2"))
}

func TestKeyedManager_RemoveChannel(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	m := node.keyedManager
	m.getOrCreateChannel("ch1", KeyedChannelOptions{})

	require.NotNil(t, m.getHub("ch1"))

	m.removeChannel("ch1")
	require.Nil(t, m.getHub("ch1"))
}
