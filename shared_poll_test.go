package centrifuge

import (
	"context"
	"sync/atomic"
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
		RefreshMode:               SharedPollRefreshModeFull,
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
		RefreshMode:               SharedPollRefreshModeFull,
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
		var v1 uint64
		if k1 != nil {
			v1 = k1.version
		}
		client1.mu.RUnlock()
		client2.mu.RLock()
		k2 := client2.keyed.trackedKeys["test:channel"]["key1"]
		var v2 uint64
		if k2 != nil {
			v2 = k2.version
		}
		client2.mu.RUnlock()
		return v1 == 7 && v2 == 7
	}, 2*time.Second, 10*time.Millisecond)
}

func TestSharedPollNotify_FullFlow_Removal(t *testing.T) {
	// Backend returns Removed=true for a notified key.
	// Client should see the item removed.
	node := newTestNodeWithSharedPoll(t, SharedPollChannelOptions{
		RefreshMode:               SharedPollRefreshModeFull,
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

	// Drain auto-notify event for cold key "key1" (triggered by track).
	select {
	case <-callCh:
	case <-time.After(500 * time.Millisecond):
	}

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

func TestSharedPollPublish_LocalOnly(t *testing.T) {
	// Publish without PublishEnabled — data delivered locally to subscriber.
	node := newTestNodeWithSharedPoll(t, SharedPollChannelOptions{
		RefreshMode:            SharedPollRefreshModeFull,
		RefreshInterval:        30 * time.Second, // Won't fire during test.
		RefreshBatchSize:       100,
		MaxKeysPerConnection:   100,
		MaxConsecutiveAbsences: 2,
		KeepLatestData:         true,
	})

	node.OnSharedPoll(func(ctx context.Context, event SharedPollEvent) (SharedPollResult, error) {
		return SharedPollResult{}, nil
	})

	setupSharedPollHandlers(node)
	client := newTestClientV2(t, node, "user1")
	connectClientV2(t, client)
	subscribeSharedPollClient(t, client, "test:channel")
	trackSharedPollClient(t, client, "test:channel", []*protocol.KeyedItem{
		{Key: "key1", Version: 0},
	})

	err := node.SharedPollPublish(context.Background(), "test:channel", "key1", 5, []byte(`{"v":5}`))
	require.NoError(t, err)

	// Client should receive the publication.
	require.Eventually(t, func() bool {
		client.mu.RLock()
		defer client.mu.RUnlock()
		k := client.keyed.trackedKeys["test:channel"]["key1"]
		return k != nil && k.version == 5
	}, 2*time.Second, 10*time.Millisecond)
}

func TestSharedPollPublish_FreshFromPublish_SkipsTimerPoll(t *testing.T) {
	// Publish data, verify next timer cycle skips the key.
	callCh := make(chan SharedPollEvent, 10)

	node := newTestNodeWithSharedPoll(t, SharedPollChannelOptions{
		RefreshMode:            SharedPollRefreshModeFull,
		RefreshInterval:        200 * time.Millisecond,
		RefreshBatchSize:       100,
		MaxKeysPerConnection:   100,
		MaxConsecutiveAbsences: 10, // High to avoid removals.
		KeepLatestData:         true,
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
		{Key: "key2", Version: 0},
	})

	// Wait for at least one timer poll that includes both keys.
	// Auto-notify may fire first for cold keys (1 item each) — skip those.
	var timerEvent SharedPollEvent
	require.Eventually(t, func() bool {
		select {
		case event := <-callCh:
			if len(event.Items) == 2 {
				timerEvent = event
				return true
			}
			return false // auto-notify event, keep waiting
		default:
			return false
		}
	}, 2*time.Second, 10*time.Millisecond)
	require.Len(t, timerEvent.Items, 2)

	// Publish to key1 — should mark it fresh.
	err := node.SharedPollPublish(context.Background(), "test:channel", "key1", 10, []byte(`{"v":10}`))
	require.NoError(t, err)

	// Next timer poll should only include key2 (key1 is fresh).
	select {
	case event := <-callCh:
		require.Len(t, event.Items, 1)
		require.Equal(t, "key2", event.Items[0].Key)
	case <-time.After(2 * time.Second):
		t.Fatal("expected timer poll after publish")
	}

	// Following poll should include both keys again (flag cleared).
	select {
	case event := <-callCh:
		require.Len(t, event.Items, 2)
	case <-time.After(2 * time.Second):
		t.Fatal("expected full timer poll after flag cleared")
	}
}

func TestSharedPollPublish_FreshFromPublish_NotSkippedByNotify(t *testing.T) {
	// Publish data, then notify same key — notification should still trigger poll.
	callCh := make(chan SharedPollEvent, 10)

	node := newTestNodeWithSharedPoll(t, SharedPollChannelOptions{
		RefreshMode:               SharedPollRefreshModeFull,
		RefreshInterval:           30 * time.Second, // Won't fire during test.
		RefreshBatchSize:          100,
		MaxKeysPerConnection:      100,
		MaxConsecutiveAbsences:    2,
		KeepLatestData:            true,
		NotificationBatchMaxSize:  50,
		NotificationBatchMaxDelay: 50 * time.Millisecond,
	})

	node.OnSharedPoll(func(ctx context.Context, event SharedPollEvent) (SharedPollResult, error) {
		callCh <- event
		items := make([]SharedPollRefreshItem, len(event.Items))
		for i, item := range event.Items {
			items[i] = SharedPollRefreshItem{
				Key:     item.Key,
				Data:    []byte(`{"v":20}`),
				Version: 20,
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

	// Publish v10 to key1.
	err := node.SharedPollPublish(context.Background(), "test:channel", "key1", 10, []byte(`{"v":10}`))
	require.NoError(t, err)

	// Wait for publish to be applied.
	require.Eventually(t, func() bool {
		client.mu.RLock()
		defer client.mu.RUnlock()
		k := client.keyed.trackedKeys["test:channel"]["key1"]
		return k != nil && k.version == 10
	}, 2*time.Second, 10*time.Millisecond)

	// Now notify key1 — should trigger a backend poll despite freshFromPublish.
	node.SharedPollNotify([]SharedPollNotificationItem{
		{Channel: "test:channel", Key: "key1"},
	})

	select {
	case event := <-callCh:
		found := false
		for _, item := range event.Items {
			if item.Key == "key1" {
				found = true
			}
		}
		require.True(t, found, "notified key should be polled even after publish")
	case <-time.After(2 * time.Second):
		t.Fatal("expected notification-triggered poll")
	}
}

func TestSharedPollPublish_UnknownChannel(t *testing.T) {
	node := newTestNodeWithSharedPoll(t)
	setupSharedPollHandlers(node)

	// Publish to untracked channel — should be a no-op.
	err := node.SharedPollPublish(context.Background(), "nonexistent:channel", "key1", 1, []byte(`{}`))
	require.NoError(t, err)
}

func TestSharedPollPublish_UntrackedKey(t *testing.T) {
	node := newTestNodeWithSharedPoll(t, SharedPollChannelOptions{
		RefreshMode:            SharedPollRefreshModeFull,
		RefreshInterval:        30 * time.Second,
		MaxKeysPerConnection:   100,
		MaxConsecutiveAbsences: 2,
	})

	node.OnSharedPoll(func(ctx context.Context, event SharedPollEvent) (SharedPollResult, error) {
		return SharedPollResult{}, nil
	})

	setupSharedPollHandlers(node)
	client := newTestClientV2(t, node, "user1")
	connectClientV2(t, client)
	subscribeSharedPollClient(t, client, "test:channel")
	trackSharedPollClient(t, client, "test:channel", []*protocol.KeyedItem{
		{Key: "key1", Version: 0},
	})

	// Publish for a key not tracked — should be a no-op.
	err := node.SharedPollPublish(context.Background(), "test:channel", "unknown_key", 1, []byte(`{}`))
	require.NoError(t, err)
}

func TestSharedPollPublish_DeltaWithKeepLatestData(t *testing.T) {
	// With KeepLatestData=true, two publishes → second should use delta.
	node := newTestNodeWithSharedPoll(t, SharedPollChannelOptions{
		RefreshMode:            SharedPollRefreshModeFull,
		RefreshInterval:        30 * time.Second,
		RefreshBatchSize:       100,
		MaxKeysPerConnection:   100,
		MaxConsecutiveAbsences: 2,
		KeepLatestData:         true,
	})

	node.OnSharedPoll(func(ctx context.Context, event SharedPollEvent) (SharedPollResult, error) {
		return SharedPollResult{}, nil
	})

	setupSharedPollHandlers(node)
	client := newTestClientV2(t, node, "user1")
	connectClientV2(t, client)
	subscribeSharedPollClient(t, client, "test:channel")
	trackSharedPollClient(t, client, "test:channel", []*protocol.KeyedItem{
		{Key: "key1", Version: 0},
	})

	// First publish.
	err := node.SharedPollPublish(context.Background(), "test:channel", "key1", 1, []byte(`{"score":10}`))
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		client.mu.RLock()
		defer client.mu.RUnlock()
		k := client.keyed.trackedKeys["test:channel"]["key1"]
		return k != nil && k.version == 1
	}, 2*time.Second, 10*time.Millisecond)

	// Second publish with different data — delta should be available.
	err = node.SharedPollPublish(context.Background(), "test:channel", "key1", 2, []byte(`{"score":20}`))
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		client.mu.RLock()
		defer client.mu.RUnlock()
		k := client.keyed.trackedKeys["test:channel"]["key1"]
		return k != nil && k.version == 2
	}, 2*time.Second, 10*time.Millisecond)

	// Check that entry data is updated in itemIndex.
	node.sharedPollManager.mu.RLock()
	s := node.sharedPollManager.channels["test:channel"]
	node.sharedPollManager.mu.RUnlock()
	s.mu.Lock()
	entry := s.itemIndex["key1"]
	require.NotNil(t, entry)
	require.Equal(t, uint64(2), entry.version)
	require.Equal(t, []byte(`{"score":20}`), entry.data)
	s.mu.Unlock()
}

func TestSharedPollPublish_MultipleClients(t *testing.T) {
	// Two clients tracking same key, publish delivers to both.
	node := newTestNodeWithSharedPoll(t, SharedPollChannelOptions{
		RefreshMode:            SharedPollRefreshModeFull,
		RefreshInterval:        30 * time.Second,
		RefreshBatchSize:       100,
		MaxKeysPerConnection:   100,
		MaxConsecutiveAbsences: 2,
	})

	node.OnSharedPoll(func(ctx context.Context, event SharedPollEvent) (SharedPollResult, error) {
		return SharedPollResult{}, nil
	})

	setupSharedPollHandlers(node)

	client1 := newTestClientV2(t, node, "user1")
	connectClientV2(t, client1)
	subscribeSharedPollClient(t, client1, "test:channel")
	trackSharedPollClient(t, client1, "test:channel", []*protocol.KeyedItem{
		{Key: "key1", Version: 0},
	})

	client2 := newTestClientV2(t, node, "user2")
	connectClientV2(t, client2)
	subscribeSharedPollClient(t, client2, "test:channel")
	trackSharedPollClient(t, client2, "test:channel", []*protocol.KeyedItem{
		{Key: "key1", Version: 0},
	})

	err := node.SharedPollPublish(context.Background(), "test:channel", "key1", 5, []byte(`{"v":5}`))
	require.NoError(t, err)

	// Both clients should receive version 5.
	require.Eventually(t, func() bool {
		client1.mu.RLock()
		k1 := client1.keyed.trackedKeys["test:channel"]["key1"]
		var v1 uint64
		if k1 != nil {
			v1 = k1.version
		}
		client1.mu.RUnlock()
		client2.mu.RLock()
		k2 := client2.keyed.trackedKeys["test:channel"]["key1"]
		var v2 uint64
		if k2 != nil {
			v2 = k2.version
		}
		client2.mu.RUnlock()
		return v1 == 5 && v2 == 5
	}, 2*time.Second, 10*time.Millisecond)
}

func TestSharedPollPublish_BrokerSubscribeOnTrack(t *testing.T) {
	// With PublishEnabled=true, track key → verify broker subscription.
	node := newTestNodeWithSharedPoll(t, SharedPollChannelOptions{
		RefreshMode:            SharedPollRefreshModeFull,
		RefreshInterval:        30 * time.Second,
		MaxKeysPerConnection:   100,
		MaxConsecutiveAbsences: 2,
		PublishEnabled:         true,
	})

	node.OnSharedPoll(func(ctx context.Context, event SharedPollEvent) (SharedPollResult, error) {
		return SharedPollResult{}, nil
	})

	setupSharedPollHandlers(node)
	client := newTestClientV2(t, node, "user1")
	connectClientV2(t, client)
	subscribeSharedPollClient(t, client, "test:channel")
	trackSharedPollClient(t, client, "test:channel", []*protocol.KeyedItem{
		{Key: "key1", Version: 0},
	})

	// Verify broker subscription exists.
	node.sharedPollManager.brokerSubMu.Lock()
	_, hasSub := node.sharedPollManager.brokerSubChans["test:channel"]
	node.sharedPollManager.brokerSubMu.Unlock()
	require.True(t, hasSub, "broker should be subscribed for PublishEnabled channel")
}

func TestSharedPollPublish_BrokerUnsubscribeOnShutdown(t *testing.T) {
	// With PublishEnabled=true, track → untrack all → verify broker unsubscription after shutdown.
	node := newTestNodeWithSharedPoll(t, SharedPollChannelOptions{
		RefreshMode:            SharedPollRefreshModeFull,
		RefreshInterval:        30 * time.Second,
		MaxKeysPerConnection:   100,
		MaxConsecutiveAbsences: 2,
		PublishEnabled:         true,
	})

	node.OnSharedPoll(func(ctx context.Context, event SharedPollEvent) (SharedPollResult, error) {
		return SharedPollResult{}, nil
	})

	setupSharedPollHandlers(node)
	client := newTestClientV2(t, node, "user1")
	connectClientV2(t, client)
	subscribeSharedPollClient(t, client, "test:channel")
	trackSharedPollClient(t, client, "test:channel", []*protocol.KeyedItem{
		{Key: "key1", Version: 0},
	})

	// Verify subscribed.
	node.sharedPollManager.brokerSubMu.Lock()
	_, hasSub := node.sharedPollManager.brokerSubChans["test:channel"]
	node.sharedPollManager.brokerSubMu.Unlock()
	require.True(t, hasSub)

	// Untrack key1.
	untrackSharedPollClient(t, client, "test:channel", []string{"key1"})

	// Wait for channel to be removed (immediate shutdown since no delay).
	require.Eventually(t, func() bool {
		node.sharedPollManager.brokerSubMu.Lock()
		defer node.sharedPollManager.brokerSubMu.Unlock()
		_, exists := node.sharedPollManager.brokerSubChans["test:channel"]
		return !exists
	}, 2*time.Second, 10*time.Millisecond)
}

func TestSharedPollPublish_CrossNode_ViaHandlePublication(t *testing.T) {
	// Call HandlePublication with keyed pub matching shared poll channel → routes to handlePublishedData.
	node := newTestNodeWithSharedPoll(t, SharedPollChannelOptions{
		RefreshMode:            SharedPollRefreshModeFull,
		RefreshInterval:        30 * time.Second,
		MaxKeysPerConnection:   100,
		MaxConsecutiveAbsences: 2,
		PublishEnabled:         true,
	})

	node.OnSharedPoll(func(ctx context.Context, event SharedPollEvent) (SharedPollResult, error) {
		return SharedPollResult{}, nil
	})

	setupSharedPollHandlers(node)
	client := newTestClientV2(t, node, "user1")
	connectClientV2(t, client)
	subscribeSharedPollClient(t, client, "test:channel")
	trackSharedPollClient(t, client, "test:channel", []*protocol.KeyedItem{
		{Key: "key1", Version: 0},
	})

	// Simulate cross-node publication via HandlePublication.
	err := node.HandlePublication("test:channel", &Publication{
		Key:     "key1",
		Data:    []byte(`{"cross":true}`),
		Version: 7,
	}, StreamPosition{}, false, nil)
	require.NoError(t, err)

	// Client should receive the cross-node publication.
	require.Eventually(t, func() bool {
		client.mu.RLock()
		defer client.mu.RUnlock()
		k := client.keyed.trackedKeys["test:channel"]["key1"]
		return k != nil && k.version == 7
	}, 2*time.Second, 10*time.Millisecond)
}

func TestSharedPollPublish_VersionIncrementsConsecutive(t *testing.T) {
	// Multiple publishes → version strictly increases, each delivered.
	node := newTestNodeWithSharedPoll(t, SharedPollChannelOptions{
		RefreshMode:            SharedPollRefreshModeFull,
		RefreshInterval:        30 * time.Second,
		MaxKeysPerConnection:   100,
		MaxConsecutiveAbsences: 2,
	})

	node.OnSharedPoll(func(ctx context.Context, event SharedPollEvent) (SharedPollResult, error) {
		return SharedPollResult{}, nil
	})

	setupSharedPollHandlers(node)
	client := newTestClientV2(t, node, "user1")
	connectClientV2(t, client)
	subscribeSharedPollClient(t, client, "test:channel")
	trackSharedPollClient(t, client, "test:channel", []*protocol.KeyedItem{
		{Key: "key1", Version: 0},
	})

	for v := uint64(1); v <= 5; v++ {
		err := node.SharedPollPublish(context.Background(), "test:channel", "key1", v, []byte(`{}`))
		require.NoError(t, err)

		ver := v
		require.Eventually(t, func() bool {
			client.mu.RLock()
			defer client.mu.RUnlock()
			k := client.keyed.trackedKeys["test:channel"]["key1"]
			return k != nil && k.version == ver
		}, 2*time.Second, 10*time.Millisecond)
	}

	// Stale version should be ignored.
	err := node.SharedPollPublish(context.Background(), "test:channel", "key1", 3, []byte(`{"stale":true}`))
	require.NoError(t, err)

	// Version should still be 5.
	time.Sleep(50 * time.Millisecond)
	client.mu.RLock()
	k := client.keyed.trackedKeys["test:channel"]["key1"]
	client.mu.RUnlock()
	require.Equal(t, uint64(5), k.version)
}

func TestSharedPollPublish_NilManager(t *testing.T) {
	// SharedPollPublish on a node without SharedPollManager returns error.
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	err := node.SharedPollPublish(context.Background(), "test:channel", "key1", 1, []byte(`{}`))
	require.Error(t, err)
}

func TestSharedPoll_StaleResponseDoesNotOverwriteData(t *testing.T) {
	// Verify: when direct publish advances entry.version past what the
	// backend/notification returns, the stale response does NOT overwrite entry.data.
	version := &atomic.Int64{}
	version.Store(2)
	data := &atomic.Value{}
	data.Store([]byte(`{"v":2}`))

	node := newTestNodeWithSharedPoll(t, SharedPollChannelOptions{
		RefreshMode:               SharedPollRefreshModeFull,
		RefreshInterval:           30 * time.Second, // Won't fire during test.
		RefreshBatchSize:          100,
		MaxKeysPerConnection:      100,
		MaxConsecutiveAbsences:    10,
		KeepLatestData:            true,
		NotificationBatchMaxSize:  50,
		NotificationBatchMaxDelay: 50 * time.Millisecond,
	})

	node.OnSharedPoll(func(ctx context.Context, event SharedPollEvent) (SharedPollResult, error) {
		items := make([]SharedPollRefreshItem, 0, len(event.Items))
		for _, item := range event.Items {
			items = append(items, SharedPollRefreshItem{
				Key:     item.Key,
				Data:    data.Load().([]byte),
				Version: uint64(version.Load()),
			})
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

	// Trigger notification → handler returns v2.
	node.SharedPollNotify([]SharedPollNotificationItem{
		{Channel: "test:channel", Key: "key1"},
	})

	// Wait for entry to reach v2.
	require.Eventually(t, func() bool {
		node.sharedPollManager.mu.RLock()
		s := node.sharedPollManager.channels["test:channel"]
		node.sharedPollManager.mu.RUnlock()
		if s == nil {
			return false
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		entry := s.itemIndex["key1"]
		return entry != nil && entry.version == 2
	}, 2*time.Second, 10*time.Millisecond)

	// Direct publish advances to v5.
	v5Data := []byte(`{"v":5,"published":true}`)
	err := node.SharedPollPublish(context.Background(), "test:channel", "key1", 5, v5Data)
	require.NoError(t, err)

	// Verify entry is at v5 with v5 data.
	node.sharedPollManager.mu.RLock()
	s := node.sharedPollManager.channels["test:channel"]
	node.sharedPollManager.mu.RUnlock()
	require.NotNil(t, s)
	s.mu.Lock()
	entry := s.itemIndex["key1"]
	require.Equal(t, uint64(5), entry.version)
	require.Equal(t, v5Data, entry.data)
	s.mu.Unlock()

	// Handler now returns v3 (stale relative to v5).
	version.Store(3)
	data.Store([]byte(`{"v":3}`))

	// Trigger notification → handler returns v3.
	node.SharedPollNotify([]SharedPollNotificationItem{
		{Channel: "test:channel", Key: "key1"},
	})

	// Wait for notification to be processed.
	time.Sleep(200 * time.Millisecond)

	// Assert: entry should still be at v5 with v5 data.
	s.mu.Lock()
	entry = s.itemIndex["key1"]
	require.Equal(t, uint64(5), entry.version, "stale notification should not overwrite version")
	require.Equal(t, v5Data, entry.data, "stale notification should not overwrite data")
	s.mu.Unlock()
}

func TestSharedPoll_EqualVersionPublishNoOverwrite(t *testing.T) {
	// Verify: equal-version publish does NOT overwrite entry.data.
	node := newTestNodeWithSharedPoll(t, SharedPollChannelOptions{
		RefreshMode:               SharedPollRefreshModeFull,
		RefreshInterval:           30 * time.Second,
		RefreshBatchSize:          100,
		MaxKeysPerConnection:      100,
		MaxConsecutiveAbsences:    10,
		KeepLatestData:            true,
		NotificationBatchMaxSize:  50,
		NotificationBatchMaxDelay: 50 * time.Millisecond,
	})

	dataA := []byte(`{"version":"five","source":"handler"}`)

	node.OnSharedPoll(func(ctx context.Context, event SharedPollEvent) (SharedPollResult, error) {
		items := make([]SharedPollRefreshItem, 0, len(event.Items))
		for _, item := range event.Items {
			items = append(items, SharedPollRefreshItem{
				Key:     item.Key,
				Data:    dataA,
				Version: 5,
			})
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

	// Trigger notification → entry reaches v5 with dataA.
	node.SharedPollNotify([]SharedPollNotificationItem{
		{Channel: "test:channel", Key: "key1"},
	})

	require.Eventually(t, func() bool {
		node.sharedPollManager.mu.RLock()
		s := node.sharedPollManager.channels["test:channel"]
		node.sharedPollManager.mu.RUnlock()
		if s == nil {
			return false
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		entry := s.itemIndex["key1"]
		return entry != nil && entry.version == 5
	}, 2*time.Second, 10*time.Millisecond)

	// Publish same version with different data.
	dataB := []byte(`{"version":"five","source":"publish"}`)
	err := node.SharedPollPublish(context.Background(), "test:channel", "key1", 5, dataB)
	require.NoError(t, err)

	// Give time for publish to be processed.
	time.Sleep(50 * time.Millisecond)

	// Assert: entry.data should still be dataA (equal version → skip, no overwrite).
	node.sharedPollManager.mu.RLock()
	s := node.sharedPollManager.channels["test:channel"]
	node.sharedPollManager.mu.RUnlock()
	s.mu.Lock()
	entry := s.itemIndex["key1"]
	require.Equal(t, uint64(5), entry.version)
	require.Equal(t, dataA, entry.data, "equal-version publish should not overwrite data")
	s.mu.Unlock()
}

func TestSharedPoll_VersionlessBasic(t *testing.T) {
	// Default config (no RefreshMode → versionless).
	// Handler returns {key, data} without version (version=0).
	node := newTestNodeWithSharedPoll(t, SharedPollChannelOptions{
		RefreshInterval:        100 * time.Millisecond,
		RefreshBatchSize:       100,
		MaxKeysPerConnection:   100,
		MaxConsecutiveAbsences: 10,
	})

	callCount := &atomic.Int32{}
	data := &atomic.Value{}
	data.Store([]byte(`{"score":42}`))

	node.OnSharedPoll(func(ctx context.Context, event SharedPollEvent) (SharedPollResult, error) {
		callCount.Add(1)
		items := make([]SharedPollRefreshItem, len(event.Items))
		for i, item := range event.Items {
			items[i] = SharedPollRefreshItem{
				Key:  item.Key,
				Data: data.Load().([]byte),
				// Version: 0 — versionless backend.
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

	// Wait for first poll to deliver data.
	require.Eventually(t, func() bool {
		node.sharedPollManager.mu.RLock()
		s := node.sharedPollManager.channels["test:channel"]
		node.sharedPollManager.mu.RUnlock()
		if s == nil {
			return false
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		entry := s.itemIndex["key1"]
		return entry != nil && entry.version > 0
	}, 2*time.Second, 10*time.Millisecond)

	// Verify synthetic version and client per-connection version.
	node.sharedPollManager.mu.RLock()
	s := node.sharedPollManager.channels["test:channel"]
	node.sharedPollManager.mu.RUnlock()
	s.mu.Lock()
	entry := s.itemIndex["key1"]
	require.Equal(t, uint64(1), entry.version, "synthetic version should be 1")
	s.mu.Unlock()

	// Client should have received data via broadcast. Per-connection version
	// is the synthetic version (used for dedup), but wire pub.Version=0.
	require.Eventually(t, func() bool {
		client.mu.RLock()
		defer client.mu.RUnlock()
		k := client.keyed.trackedKeys["test:channel"]["key1"]
		return k != nil && k.version == 1
	}, 2*time.Second, 10*time.Millisecond)

	// Wait for another poll with same data — no new broadcast (hash unchanged).
	prevCalls := callCount.Load()
	require.Eventually(t, func() bool {
		return callCount.Load() > prevCalls
	}, 2*time.Second, 10*time.Millisecond)

	// Version should still be 1 (no change detected).
	s.mu.Lock()
	entry = s.itemIndex["key1"]
	require.Equal(t, uint64(1), entry.version, "version should not increment for unchanged data")
	s.mu.Unlock()

	// Change data → should detect change and increment synthetic version.
	data.Store([]byte(`{"score":99}`))
	require.Eventually(t, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		entry := s.itemIndex["key1"]
		return entry != nil && entry.version == 2
	}, 2*time.Second, 10*time.Millisecond)

	// Client should receive the update.
	require.Eventually(t, func() bool {
		client.mu.RLock()
		defer client.mu.RUnlock()
		k := client.keyed.trackedKeys["test:channel"]["key1"]
		return k != nil && k.version == 2
	}, 2*time.Second, 10*time.Millisecond)
}

func TestSharedPoll_VersionlessUnchangedWithKeepLatestData(t *testing.T) {
	// KeepLatestData=true → uses bytes.Equal instead of xxhash.
	node := newTestNodeWithSharedPoll(t, SharedPollChannelOptions{
		RefreshInterval:        100 * time.Millisecond,
		RefreshBatchSize:       100,
		MaxKeysPerConnection:   100,
		MaxConsecutiveAbsences: 10,
		KeepLatestData:         true,
	})

	node.OnSharedPoll(func(ctx context.Context, event SharedPollEvent) (SharedPollResult, error) {
		items := make([]SharedPollRefreshItem, len(event.Items))
		for i, item := range event.Items {
			items[i] = SharedPollRefreshItem{
				Key:  item.Key,
				Data: []byte(`{"stable":"data"}`),
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

	// Wait for first poll.
	require.Eventually(t, func() bool {
		node.sharedPollManager.mu.RLock()
		s := node.sharedPollManager.channels["test:channel"]
		node.sharedPollManager.mu.RUnlock()
		if s == nil {
			return false
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		entry := s.itemIndex["key1"]
		return entry != nil && entry.version == 1
	}, 2*time.Second, 10*time.Millisecond)

	// After several more polls with same data, version should stay at 1.
	time.Sleep(300 * time.Millisecond)
	node.sharedPollManager.mu.RLock()
	s := node.sharedPollManager.channels["test:channel"]
	node.sharedPollManager.mu.RUnlock()
	s.mu.Lock()
	entry := s.itemIndex["key1"]
	require.Equal(t, uint64(1), entry.version, "version should not increment for unchanged data")
	require.Equal(t, []byte(`{"stable":"data"}`), entry.data)
	s.mu.Unlock()
}

func TestSharedPoll_VersionlessPublishRejected(t *testing.T) {
	node := newTestNodeWithSharedPoll(t, SharedPollChannelOptions{
		RefreshInterval:        30 * time.Second,
		MaxKeysPerConnection:   100,
		MaxConsecutiveAbsences: 2,
	})

	node.OnSharedPoll(func(ctx context.Context, event SharedPollEvent) (SharedPollResult, error) {
		return SharedPollResult{}, nil
	})

	setupSharedPollHandlers(node)
	client := newTestClientV2(t, node, "user1")
	connectClientV2(t, client)
	subscribeSharedPollClient(t, client, "test:channel")
	trackSharedPollClient(t, client, "test:channel", []*protocol.KeyedItem{
		{Key: "key1", Version: 0},
	})

	err := node.SharedPollPublish(context.Background(), "test:channel", "key1", 5, []byte(`{"v":5}`))
	require.Error(t, err)
	require.Contains(t, err.Error(), "versionless")
}

func TestSharedPoll_VersionlessNoCachedData(t *testing.T) {
	// getCachedData returns nil in versionless mode even with KeepLatestData.
	node := newTestNodeWithSharedPoll(t, SharedPollChannelOptions{
		RefreshInterval:        100 * time.Millisecond,
		RefreshBatchSize:       100,
		MaxKeysPerConnection:   100,
		MaxConsecutiveAbsences: 10,
		KeepLatestData:         true,
	})

	node.OnSharedPoll(func(ctx context.Context, event SharedPollEvent) (SharedPollResult, error) {
		return SharedPollResult{
			Items: []SharedPollRefreshItem{
				{Key: "key1", Data: []byte(`{"v":1}`)},
			},
		}, nil
	})

	setupSharedPollHandlers(node)
	client := newTestClientV2(t, node, "user1")
	connectClientV2(t, client)
	subscribeSharedPollClient(t, client, "test:channel")
	trackSharedPollClient(t, client, "test:channel", []*protocol.KeyedItem{
		{Key: "key1", Version: 0},
	})

	// Wait for entry to be populated.
	require.Eventually(t, func() bool {
		node.sharedPollManager.mu.RLock()
		s := node.sharedPollManager.channels["test:channel"]
		node.sharedPollManager.mu.RUnlock()
		if s == nil {
			return false
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		entry := s.itemIndex["key1"]
		return entry != nil && entry.version > 0
	}, 2*time.Second, 10*time.Millisecond)

	// getCachedData should return nil in versionless mode.
	cached := node.sharedPollManager.getCachedData("test:channel", []KeyedItem{
		{Key: "key1", Version: 0},
	})
	require.Nil(t, cached, "getCachedData should return nil in versionless mode")
}

func TestSharedPoll_VersionlessNotification(t *testing.T) {
	node := newTestNodeWithSharedPoll(t, SharedPollChannelOptions{
		RefreshInterval:           30 * time.Second,
		RefreshBatchSize:          100,
		MaxKeysPerConnection:      100,
		MaxConsecutiveAbsences:    10,
		NotificationBatchMaxSize:  50,
		NotificationBatchMaxDelay: 50 * time.Millisecond,
	})

	callCount := &atomic.Int32{}
	data := &atomic.Value{}
	data.Store([]byte(`{"notif":"v1"}`))

	node.OnSharedPoll(func(ctx context.Context, event SharedPollEvent) (SharedPollResult, error) {
		callCount.Add(1)
		items := make([]SharedPollRefreshItem, len(event.Items))
		for i, item := range event.Items {
			items[i] = SharedPollRefreshItem{
				Key:  item.Key,
				Data: data.Load().([]byte),
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

	// Wait for auto-notify from track to deliver first data.
	require.Eventually(t, func() bool {
		node.sharedPollManager.mu.RLock()
		s := node.sharedPollManager.channels["test:channel"]
		node.sharedPollManager.mu.RUnlock()
		if s == nil {
			return false
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		entry := s.itemIndex["key1"]
		return entry != nil && entry.version == 1
	}, 2*time.Second, 10*time.Millisecond)

	// Change data, then notify.
	data.Store([]byte(`{"notif":"v2"}`))
	node.SharedPollNotify([]SharedPollNotificationItem{
		{Channel: "test:channel", Key: "key1"},
	})

	// Should detect change via hash and increment synthetic version.
	require.Eventually(t, func() bool {
		node.sharedPollManager.mu.RLock()
		s := node.sharedPollManager.channels["test:channel"]
		node.sharedPollManager.mu.RUnlock()
		if s == nil {
			return false
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		entry := s.itemIndex["key1"]
		return entry != nil && entry.version == 2
	}, 2*time.Second, 10*time.Millisecond)

	// Notify again with same data — no version change.
	prevCalls := callCount.Load()
	node.SharedPollNotify([]SharedPollNotificationItem{
		{Channel: "test:channel", Key: "key1"},
	})
	require.Eventually(t, func() bool {
		return callCount.Load() > prevCalls
	}, 2*time.Second, 10*time.Millisecond)

	node.sharedPollManager.mu.RLock()
	s := node.sharedPollManager.channels["test:channel"]
	node.sharedPollManager.mu.RUnlock()
	s.mu.Lock()
	entry := s.itemIndex["key1"]
	require.Equal(t, uint64(2), entry.version, "no change → version stays at 2")
	s.mu.Unlock()
}

func TestSharedPoll_VersionlessMultipleKeys(t *testing.T) {
	node := newTestNodeWithSharedPoll(t, SharedPollChannelOptions{
		RefreshInterval:        100 * time.Millisecond,
		RefreshBatchSize:       100,
		MaxKeysPerConnection:   100,
		MaxConsecutiveAbsences: 10,
	})

	callCount := &atomic.Int32{}
	k1Data := &atomic.Value{}
	k1Data.Store([]byte(`{"k1":"a"}`))
	k2Data := &atomic.Value{}
	k2Data.Store([]byte(`{"k2":"a"}`))

	node.OnSharedPoll(func(ctx context.Context, event SharedPollEvent) (SharedPollResult, error) {
		callCount.Add(1)
		items := make([]SharedPollRefreshItem, 0, len(event.Items))
		for _, item := range event.Items {
			switch item.Key {
			case "k1":
				items = append(items, SharedPollRefreshItem{Key: "k1", Data: k1Data.Load().([]byte)})
			case "k2":
				items = append(items, SharedPollRefreshItem{Key: "k2", Data: k2Data.Load().([]byte)})
			}
		}
		return SharedPollResult{Items: items}, nil
	})

	setupSharedPollHandlers(node)
	client := newTestClientV2(t, node, "user1")
	connectClientV2(t, client)
	subscribeSharedPollClient(t, client, "test:channel")
	trackSharedPollClient(t, client, "test:channel", []*protocol.KeyedItem{
		{Key: "k1", Version: 0},
		{Key: "k2", Version: 0},
	})

	// Wait for both keys to get first data.
	require.Eventually(t, func() bool {
		node.sharedPollManager.mu.RLock()
		s := node.sharedPollManager.channels["test:channel"]
		node.sharedPollManager.mu.RUnlock()
		if s == nil {
			return false
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		e1 := s.itemIndex["k1"]
		e2 := s.itemIndex["k2"]
		return e1 != nil && e1.version > 0 && e2 != nil && e2.version > 0
	}, 2*time.Second, 10*time.Millisecond)

	// Record initial versions (versionCounter is per-channel, so order
	// depends on map iteration — capture actual values).
	node.sharedPollManager.mu.RLock()
	s := node.sharedPollManager.channels["test:channel"]
	node.sharedPollManager.mu.RUnlock()
	s.mu.Lock()
	k1InitVersion := s.itemIndex["k1"].version
	k2InitVersion := s.itemIndex["k2"].version
	s.mu.Unlock()

	// Change only k1 data.
	k1Data.Store([]byte(`{"k1":"b"}`))
	require.Eventually(t, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		e1 := s.itemIndex["k1"]
		return e1 != nil && e1.version > k1InitVersion
	}, 2*time.Second, 10*time.Millisecond)

	// k2 should still be at its initial version (unchanged).
	s.mu.Lock()
	e2 := s.itemIndex["k2"]
	require.Equal(t, k2InitVersion, e2.version, "k2 unchanged → version stays the same")
	s.mu.Unlock()
}

func TestSharedPoll_VersionlessAbsenceTracking(t *testing.T) {
	node := newTestNodeWithSharedPoll(t, SharedPollChannelOptions{
		RefreshInterval:        100 * time.Millisecond,
		RefreshBatchSize:       100,
		MaxKeysPerConnection:   100,
		MaxConsecutiveAbsences: 2,
	})

	returnKey := &atomic.Bool{}
	returnKey.Store(true)

	node.OnSharedPoll(func(ctx context.Context, event SharedPollEvent) (SharedPollResult, error) {
		if !returnKey.Load() {
			return SharedPollResult{}, nil
		}
		items := make([]SharedPollRefreshItem, len(event.Items))
		for i, item := range event.Items {
			items[i] = SharedPollRefreshItem{
				Key:  item.Key,
				Data: []byte(`{"present":true}`),
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

	// Wait for initial data.
	require.Eventually(t, func() bool {
		node.sharedPollManager.mu.RLock()
		s := node.sharedPollManager.channels["test:channel"]
		node.sharedPollManager.mu.RUnlock()
		if s == nil {
			return false
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		entry := s.itemIndex["key1"]
		return entry != nil && entry.version > 0
	}, 2*time.Second, 10*time.Millisecond)

	// Stop returning the key.
	returnKey.Store(false)

	// key1 should be removed after MaxConsecutiveAbsences=2 polls.
	require.Eventually(t, func() bool {
		node.sharedPollManager.mu.RLock()
		s, ok := node.sharedPollManager.channels["test:channel"]
		node.sharedPollManager.mu.RUnlock()
		if !ok {
			return true
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		_, exists := s.itemIndex["key1"]
		return !exists
	}, 2*time.Second, 10*time.Millisecond)
}

func TestSharedPoll_VersionlessDelta(t *testing.T) {
	// Versionless + KeepLatestData enables delta compression.
	node := newTestNodeWithSharedPoll(t, SharedPollChannelOptions{
		RefreshInterval:        100 * time.Millisecond,
		RefreshBatchSize:       100,
		MaxKeysPerConnection:   100,
		MaxConsecutiveAbsences: 10,
		KeepLatestData:         true,
	})

	data := &atomic.Value{}
	data.Store([]byte(`{"value":"hello world, this is version one of the data payload"}`))

	node.OnSharedPoll(func(ctx context.Context, event SharedPollEvent) (SharedPollResult, error) {
		items := make([]SharedPollRefreshItem, len(event.Items))
		for i, item := range event.Items {
			items[i] = SharedPollRefreshItem{
				Key:  item.Key,
				Data: data.Load().([]byte),
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

	// Wait for first data.
	require.Eventually(t, func() bool {
		node.sharedPollManager.mu.RLock()
		s := node.sharedPollManager.channels["test:channel"]
		node.sharedPollManager.mu.RUnlock()
		if s == nil {
			return false
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		entry := s.itemIndex["key1"]
		return entry != nil && entry.version == 1 && entry.data != nil
	}, 2*time.Second, 10*time.Millisecond)

	// Change data — delta should be computed from prevData.
	data.Store([]byte(`{"value":"hello world, this is version two of the data payload"}`))
	require.Eventually(t, func() bool {
		node.sharedPollManager.mu.RLock()
		s := node.sharedPollManager.channels["test:channel"]
		node.sharedPollManager.mu.RUnlock()
		if s == nil {
			return false
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		entry := s.itemIndex["key1"]
		return entry != nil && entry.version == 2
	}, 2*time.Second, 10*time.Millisecond)

	// Verify entry data updated.
	node.sharedPollManager.mu.RLock()
	s := node.sharedPollManager.channels["test:channel"]
	node.sharedPollManager.mu.RUnlock()
	s.mu.Lock()
	entry := s.itemIndex["key1"]
	require.Equal(t, []byte(`{"value":"hello world, this is version two of the data payload"}`), entry.data)
	s.mu.Unlock()
}
