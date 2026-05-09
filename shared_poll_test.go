package centrifuge

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/require"
)

// controllableBroker is a test broker with atomic counters and synchronization
// hooks for testing race conditions in SharedPollManager broker subscribe/unsubscribe.
type controllableBroker struct {
	subscribeCount   atomic.Int32
	unsubscribeCount atomic.Int32

	subscribeMu   sync.Mutex
	subscribeErr  error                 // returned by Subscribe when non-nil
	subscribeCh   chan struct{}         // if non-nil, Subscribe blocks until this is closed
	subscribeFunc func(ch string) error // if non-nil, called instead of default behavior

	unsubscribeMu   sync.Mutex
	unsubscribeErr  error
	unsubscribeFunc func(ch string) error
}

func (b *controllableBroker) RegisterBrokerEventHandler(_ BrokerEventHandler) error { return nil }
func (b *controllableBroker) Publish(_ string, _ []byte, _ PublishOptions) (PublishResult, error) {
	return PublishResult{}, nil
}
func (b *controllableBroker) PublishJoin(_ string, _ *ClientInfo) error  { return nil }
func (b *controllableBroker) PublishLeave(_ string, _ *ClientInfo) error { return nil }
func (b *controllableBroker) PublishControl(_ []byte, _, _ string) error { return nil }
func (b *controllableBroker) History(_ string, _ HistoryOptions) ([]*Publication, StreamPosition, error) {
	return nil, StreamPosition{}, nil
}
func (b *controllableBroker) RemoveHistory(_ string) error { return nil }

func (b *controllableBroker) Subscribe(channels ...string) error {
	for _, ch := range channels {
		b.subscribeCount.Add(1)
		b.subscribeMu.Lock()
		fn := b.subscribeFunc
		err := b.subscribeErr
		waitCh := b.subscribeCh
		b.subscribeMu.Unlock()
		if waitCh != nil {
			<-waitCh
		}
		if fn != nil {
			if err := fn(ch); err != nil {
				return err
			}
			continue
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (b *controllableBroker) Unsubscribe(channels ...string) error {
	for _, ch := range channels {
		b.unsubscribeCount.Add(1)
		b.unsubscribeMu.Lock()
		fn := b.unsubscribeFunc
		err := b.unsubscribeErr
		b.unsubscribeMu.Unlock()
		if fn != nil {
			if err := fn(ch); err != nil {
				return err
			}
			continue
		}
		if err != nil {
			return err
		}
	}
	return nil
}

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
		SharedPoll: SharedPollConfig{
			GetSharedPollChannelOptions: func(channel string) (SharedPollChannelOptions, bool) {
				return SharedPollChannelOptions{
					RefreshInterval:      100 * time.Millisecond,
					MaxKeysPerConnection: 100,
				}, true
			},
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

func TestSharedPollNotify_TriggersRefresh(t *testing.T) {
	callCh := make(chan SharedPollEvent, 10)

	node := newTestNodeWithSharedPoll(t, SharedPollChannelOptions{
		RefreshInterval:           10 * time.Second, // Long interval so timer doesn't fire.
		RefreshBatchSize:          100,
		MaxKeysPerConnection:      100,
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
		Mode:                      SharedPollModeVersioned,
		RefreshInterval:           30 * time.Second, // Won't fire during test.
		RefreshBatchSize:          100,
		MaxKeysPerConnection:      100,
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
		Mode:                      SharedPollModeVersioned,
		RefreshInterval:           30 * time.Second,
		RefreshBatchSize:          100,
		MaxKeysPerConnection:      100,
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
		Mode:                      SharedPollModeVersioned,
		RefreshInterval:           30 * time.Second,
		RefreshBatchSize:          100,
		MaxKeysPerConnection:      100,
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
	opts := keyedChannelOptions{MaxTrackedPerConnection: 42}

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
	m.getOrCreateChannel("ch1", keyedChannelOptions{MaxTrackedPerConnection: 100})
	require.Equal(t, 100, m.maxTrackedPerConnection("ch1"))

	// Zero limit — default 5000.
	m.getOrCreateChannel("ch2", keyedChannelOptions{MaxTrackedPerConnection: 0})
	require.Equal(t, 5000, m.maxTrackedPerConnection("ch2"))
}

func TestKeyedManager_RemoveChannel(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	m := node.keyedManager
	m.getOrCreateChannel("ch1", keyedChannelOptions{})

	require.NotNil(t, m.getHub("ch1"))

	m.removeChannel("ch1")
	require.Nil(t, m.getHub("ch1"))
}

func TestSharedPollPublish_LocalOnly(t *testing.T) {
	// Publish without PublishEnabled — data delivered locally to subscriber.
	node := newTestNodeWithSharedPoll(t, SharedPollChannelOptions{
		Mode:                 SharedPollModeVersioned,
		RefreshInterval:      30 * time.Second, // Won't fire during test.
		RefreshBatchSize:     100,
		MaxKeysPerConnection: 100,
		KeepLatestData:       true,
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

	err := node.SharedPollPublish(context.Background(), "test:channel", "key1", 5, "", []byte(`{"v":5}`))
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
		Mode:                 SharedPollModeVersioned,
		RefreshInterval:      200 * time.Millisecond,
		RefreshBatchSize:     100,
		MaxKeysPerConnection: 100,
		KeepLatestData:       true,
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
	err := node.SharedPollPublish(context.Background(), "test:channel", "key1", 10, "", []byte(`{"v":10}`))
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
		Mode:                      SharedPollModeVersioned,
		RefreshInterval:           30 * time.Second, // Won't fire during test.
		RefreshBatchSize:          100,
		MaxKeysPerConnection:      100,
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
	err := node.SharedPollPublish(context.Background(), "test:channel", "key1", 10, "", []byte(`{"v":10}`))
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
	err := node.SharedPollPublish(context.Background(), "nonexistent:channel", "key1", 1, "", []byte(`{}`))
	require.NoError(t, err)
}

func TestSharedPollPublish_UntrackedKey(t *testing.T) {
	node := newTestNodeWithSharedPoll(t, SharedPollChannelOptions{
		Mode:                 SharedPollModeVersioned,
		RefreshInterval:      30 * time.Second,
		MaxKeysPerConnection: 100,
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
	err := node.SharedPollPublish(context.Background(), "test:channel", "unknown_key", 1, "", []byte(`{}`))
	require.NoError(t, err)
}

func TestSharedPollPublish_DeltaWithKeepLatestData(t *testing.T) {
	// With KeepLatestData=true, two publishes → second should use delta.
	node := newTestNodeWithSharedPoll(t, SharedPollChannelOptions{
		Mode:                 SharedPollModeVersioned,
		RefreshInterval:      30 * time.Second,
		RefreshBatchSize:     100,
		MaxKeysPerConnection: 100,
		KeepLatestData:       true,
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
	err := node.SharedPollPublish(context.Background(), "test:channel", "key1", 1, "", []byte(`{"score":10}`))
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		client.mu.RLock()
		defer client.mu.RUnlock()
		k := client.keyed.trackedKeys["test:channel"]["key1"]
		return k != nil && k.version == 1
	}, 2*time.Second, 10*time.Millisecond)

	// Second publish with different data — delta should be available.
	err = node.SharedPollPublish(context.Background(), "test:channel", "key1", 2, "", []byte(`{"score":20}`))
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
		Mode:                 SharedPollModeVersioned,
		RefreshInterval:      30 * time.Second,
		RefreshBatchSize:     100,
		MaxKeysPerConnection: 100,
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

	err := node.SharedPollPublish(context.Background(), "test:channel", "key1", 5, "", []byte(`{"v":5}`))
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
		Mode:                 SharedPollModeVersioned,
		RefreshInterval:      30 * time.Second,
		MaxKeysPerConnection: 100,
		PublishEnabled:       true,
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

	// Verify broker subscription exists for the key-scoped channel.
	node.sharedPollManager.brokerSubMu.Lock()
	_, hasSub := node.sharedPollManager.brokerSubChans[sharedPollKeyChannel("test:channel", "key1")]
	node.sharedPollManager.brokerSubMu.Unlock()
	require.True(t, hasSub, "broker should be subscribed for PublishEnabled key channel")
}

func TestSharedPollPublish_BrokerUnsubscribeOnShutdown(t *testing.T) {
	// With PublishEnabled=true, track → untrack all → verify broker unsubscription after shutdown.
	node := newTestNodeWithSharedPoll(t, SharedPollChannelOptions{
		Mode:                 SharedPollModeVersioned,
		RefreshInterval:      30 * time.Second,
		MaxKeysPerConnection: 100,
		PublishEnabled:       true,
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

	// Verify subscribed for key-scoped channel.
	keyCh := sharedPollKeyChannel("test:channel", "key1")
	node.sharedPollManager.brokerSubMu.Lock()
	_, hasSub := node.sharedPollManager.brokerSubChans[keyCh]
	node.sharedPollManager.brokerSubMu.Unlock()
	require.True(t, hasSub)

	// Untrack key1.
	untrackSharedPollClient(t, client, "test:channel", []string{"key1"})

	// Wait for key-channel subscription to be removed (immediate shutdown since no delay).
	require.Eventually(t, func() bool {
		node.sharedPollManager.brokerSubMu.Lock()
		defer node.sharedPollManager.brokerSubMu.Unlock()
		_, exists := node.sharedPollManager.brokerSubChans[keyCh]
		return !exists
	}, 2*time.Second, 10*time.Millisecond)
}

func TestSharedPollPublish_CrossNode_ViaHandlePublication(t *testing.T) {
	// Call HandlePublication with keyed pub matching shared poll channel → routes to handlePublishedData.
	node := newTestNodeWithSharedPoll(t, SharedPollChannelOptions{
		Mode:                 SharedPollModeVersioned,
		RefreshInterval:      30 * time.Second,
		MaxKeysPerConnection: 100,
		PublishEnabled:       true,
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
		Mode:                 SharedPollModeVersioned,
		RefreshInterval:      30 * time.Second,
		MaxKeysPerConnection: 100,
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
		err := node.SharedPollPublish(context.Background(), "test:channel", "key1", v, "", []byte(`{}`))
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
	err := node.SharedPollPublish(context.Background(), "test:channel", "key1", 3, "", []byte(`{"stale":true}`))
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

	err := node.SharedPollPublish(context.Background(), "test:channel", "key1", 1, "", []byte(`{}`))
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
		Mode:                      SharedPollModeVersioned,
		RefreshInterval:           30 * time.Second, // Won't fire during test.
		RefreshBatchSize:          100,
		MaxKeysPerConnection:      100,
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
	err := node.SharedPollPublish(context.Background(), "test:channel", "key1", 5, "", v5Data)
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
		Mode:                      SharedPollModeVersioned,
		RefreshInterval:           30 * time.Second,
		RefreshBatchSize:          100,
		MaxKeysPerConnection:      100,
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
	err := node.SharedPollPublish(context.Background(), "test:channel", "key1", 5, "", dataB)
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
	// Default config (no Mode → versionless).
	// Handler returns {key, data} without version (version=0).
	node := newTestNodeWithSharedPoll(t, SharedPollChannelOptions{
		RefreshInterval:      100 * time.Millisecond,
		RefreshBatchSize:     100,
		MaxKeysPerConnection: 100,
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
	// is the synthetic version (used for dedup), also sent on wire.
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
		RefreshInterval:      100 * time.Millisecond,
		RefreshBatchSize:     100,
		MaxKeysPerConnection: 100,
		KeepLatestData:       true,
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
		RefreshInterval:      30 * time.Second,
		MaxKeysPerConnection: 100,
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

	err := node.SharedPollPublish(context.Background(), "test:channel", "key1", 5, "", []byte(`{"v":5}`))
	require.Error(t, err)
	require.Contains(t, err.Error(), "versionless")
}

func TestSharedPoll_VersionlessNoCachedData(t *testing.T) {
	// getCachedData returns nil in versionless mode even with KeepLatestData.
	node := newTestNodeWithSharedPoll(t, SharedPollChannelOptions{
		RefreshInterval:      100 * time.Millisecond,
		RefreshBatchSize:     100,
		MaxKeysPerConnection: 100,
		KeepLatestData:       true,
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
	cached := node.sharedPollManager.getCachedData("test:channel", []TrackItem{
		{Key: "key1", Version: 0},
	})
	require.Nil(t, cached, "getCachedData should return nil in versionless mode")
}

func TestSharedPoll_VersionlessNotification(t *testing.T) {
	node := newTestNodeWithSharedPoll(t, SharedPollChannelOptions{
		RefreshInterval:           30 * time.Second,
		RefreshBatchSize:          100,
		MaxKeysPerConnection:      100,
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
		RefreshInterval:      100 * time.Millisecond,
		RefreshBatchSize:     100,
		MaxKeysPerConnection: 100,
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

func TestSharedPoll_VersionlessDelta(t *testing.T) {
	// Versionless + KeepLatestData enables delta compression.
	node := newTestNodeWithSharedPoll(t, SharedPollChannelOptions{
		RefreshInterval:      100 * time.Millisecond,
		RefreshBatchSize:     100,
		MaxKeysPerConnection: 100,
		KeepLatestData:       true,
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

// --- Broker subscribe/unsubscribe race condition tests ---

func newTestNodeWithControllableBroker(t *testing.T, broker *controllableBroker, opts SharedPollChannelOptions) *Node {
	node, err := New(Config{
		LogLevel:   LogLevelTrace,
		LogHandler: func(entry LogEntry) {},
		SharedPoll: SharedPollConfig{
			GetSharedPollChannelOptions: func(channel string) (SharedPollChannelOptions, bool) {
				return opts, true
			},
		},
	})
	require.NoError(t, err)
	node.SetBroker(broker)

	node.OnSharedPoll(func(ctx context.Context, event SharedPollEvent) (SharedPollResult, error) {
		return SharedPollResult{}, nil
	})

	err = node.Run()
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = node.Shutdown(context.Background())
	})
	return node
}

func TestSharedPollManager_ConcurrentTrackSingleBrokerSubscribe(t *testing.T) {
	// Multiple concurrent track() calls for the same channel should result in
	// exactly one broker.Subscribe call.
	broker := &controllableBroker{}
	opts := SharedPollChannelOptions{
		RefreshInterval:      30 * time.Second,
		MaxKeysPerConnection: 100,
		PublishEnabled:       true,
	}
	node := newTestNodeWithControllableBroker(t, broker, opts)
	m := node.sharedPollManager

	const numGoroutines = 20
	var wg sync.WaitGroup
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(i int) {
			defer wg.Done()
			_, _, _ = m.track("test:channel", opts, "key"+string(rune('A'+i)))
		}(i)
	}
	wg.Wait()

	// Each unique key gets its own broker.Subscribe call (per-key PUB/SUB).
	require.Equal(t, int32(numGoroutines), broker.subscribeCount.Load(),
		"expected one broker.Subscribe per unique key")

	// All keys should be tracked.
	m.mu.RLock()
	s := m.channels["test:channel"]
	m.mu.RUnlock()
	require.NotNil(t, s)
	s.mu.Lock()
	require.Equal(t, numGoroutines, len(s.itemIndex))
	s.mu.Unlock()
}

func TestSharedPollManager_BrokerSubscribeFailureCleanup(t *testing.T) {
	// When broker.Subscribe fails, track() should clean up the channel state
	// and return an error.
	broker := &controllableBroker{subscribeErr: errors.New("broker unavailable")}
	opts := SharedPollChannelOptions{
		RefreshInterval:      30 * time.Second,
		MaxKeysPerConnection: 100,
		PublishEnabled:       true,
	}
	node := newTestNodeWithControllableBroker(t, broker, opts)
	m := node.sharedPollManager

	isNew, _, err := m.track("test:channel", opts, "key1")
	require.Error(t, err)
	require.False(t, isNew)

	// Channel state should be fully cleaned up.
	m.mu.RLock()
	_, exists := m.channels["test:channel"]
	m.mu.RUnlock()
	require.False(t, exists, "channel should be removed after broker subscribe failure")

	// Broker sub entry should not exist.
	m.brokerSubMu.Lock()
	_, hasSub := m.brokerSubChans[sharedPollKeyChannel("test:channel", "key1")]
	m.brokerSubMu.Unlock()
	require.False(t, hasSub)
}

func TestSharedPollManager_BrokerSubscribeFailureNoOrphanConcurrent(t *testing.T) {
	// Scenario: goroutine A creates channel, starts broker.Subscribe (which blocks).
	// Goroutine B arrives, sees existing channel, adds its own key.
	// A's broker.Subscribe fails. A should NOT tear down the channel because B's
	// key is still there.
	blockCh := make(chan struct{})
	var callCount atomic.Int32
	broker := &controllableBroker{
		subscribeFunc: func(ch string) error {
			n := callCount.Add(1)
			if n == 1 {
				<-blockCh // Block first call
				return errors.New("broker unavailable")
			}
			return nil
		},
	}
	opts := SharedPollChannelOptions{
		RefreshInterval:      30 * time.Second,
		MaxKeysPerConnection: 100,
		PublishEnabled:       true,
	}
	node := newTestNodeWithControllableBroker(t, broker, opts)
	m := node.sharedPollManager

	// Goroutine A: track key1 (will block on broker.Subscribe).
	var aErr error
	var aDone sync.WaitGroup
	aDone.Add(1)
	go func() {
		defer aDone.Done()
		_, _, aErr = m.track("test:channel", opts, "key1")
	}()

	// Wait for A to reach broker.Subscribe.
	require.Eventually(t, func() bool {
		return broker.subscribeCount.Load() >= 1
	}, time.Second, time.Millisecond)

	// Goroutine B: track key2 on same channel (won't call broker.Subscribe because
	// isNewChannel is false for B).
	isNew2, _, err2 := m.track("test:channel", opts, "key2")
	require.NoError(t, err2)
	require.True(t, isNew2)

	// Unblock A (will fail).
	close(blockCh)
	aDone.Wait()
	require.Error(t, aErr)

	// Channel should still exist because key2 is there.
	m.mu.RLock()
	s, exists := m.channels["test:channel"]
	m.mu.RUnlock()
	require.True(t, exists, "channel should survive because key2 is still tracked")

	s.mu.Lock()
	_, hasKey1 := s.itemIndex["key1"]
	_, hasKey2 := s.itemIndex["key2"]
	s.mu.Unlock()
	require.False(t, hasKey1, "key1 should be cleaned up after subscribe failure")
	require.True(t, hasKey2, "key2 should survive after key1's subscribe failure")
}

func TestSharedPollManager_BrokerUnsubscribeViaDissolver(t *testing.T) {
	// After all keys removed, broker.Unsubscribe should be called via subDissolver.
	broker := &controllableBroker{}
	opts := SharedPollChannelOptions{
		RefreshInterval:      30 * time.Second,
		MaxKeysPerConnection: 100,
		PublishEnabled:       true,
	}
	node := newTestNodeWithControllableBroker(t, broker, opts)
	m := node.sharedPollManager

	isNew, _, err := m.track("test:channel", opts, "key1")
	require.NoError(t, err)
	require.True(t, isNew)

	// Verify broker is subscribed for key-scoped channel.
	keyCh := sharedPollKeyChannel("test:channel", "key1")
	m.brokerSubMu.Lock()
	_, hasSub := m.brokerSubChans[keyCh]
	m.brokerSubMu.Unlock()
	require.True(t, hasSub)

	// Remove the key directly (simulating untrack with no hub).
	m.untrack("test:channel", "key1")

	// Wait for dissolver to process the unsubscribe.
	require.Eventually(t, func() bool {
		m.brokerSubMu.Lock()
		defer m.brokerSubMu.Unlock()
		_, exists := m.brokerSubChans[keyCh]
		return !exists
	}, 2*time.Second, 10*time.Millisecond)

	require.GreaterOrEqual(t, broker.unsubscribeCount.Load(), int32(1))
}

func TestSharedPollManager_BrokerUnsubscribeRetryOnFailure(t *testing.T) {
	// When broker.Unsubscribe fails, dissolver should retry until success.
	var failCount atomic.Int32
	broker := &controllableBroker{
		unsubscribeFunc: func(ch string) error {
			n := failCount.Add(1)
			if n <= 2 {
				return errors.New("temporary failure")
			}
			return nil
		},
	}
	opts := SharedPollChannelOptions{
		RefreshInterval:      30 * time.Second,
		MaxKeysPerConnection: 100,
		PublishEnabled:       true,
	}
	node := newTestNodeWithControllableBroker(t, broker, opts)
	m := node.sharedPollManager

	isNew, _, err := m.track("test:channel", opts, "key1")
	require.NoError(t, err)
	require.True(t, isNew)

	// Remove key to trigger unsubscribe.
	m.untrack("test:channel", "key1")

	// Wait for dissolver to eventually succeed (after 2 failures).
	keyCh := sharedPollKeyChannel("test:channel", "key1")
	require.Eventually(t, func() bool {
		m.brokerSubMu.Lock()
		defer m.brokerSubMu.Unlock()
		_, exists := m.brokerSubChans[keyCh]
		return !exists
	}, 10*time.Second, 50*time.Millisecond)

	// Should have attempted at least 3 times (2 failures + 1 success).
	require.GreaterOrEqual(t, broker.unsubscribeCount.Load(), int32(3))
}

func TestSharedPollManager_BrokerUnsubscribeSkipsIfResubscribed(t *testing.T) {
	// Scenario: track→untrack triggers dissolver unsubscribe. We immediately
	// re-track the channel. The dissolver's stillActive check (or serialization
	// via brokerSubMu) should ensure the final state is: channel active with
	// a valid broker subscription.
	var unsubscribeCalls atomic.Int32
	broker := &controllableBroker{
		unsubscribeFunc: func(ch string) error {
			unsubscribeCalls.Add(1)
			return nil
		},
	}
	opts := SharedPollChannelOptions{
		RefreshInterval:      30 * time.Second,
		MaxKeysPerConnection: 100,
		PublishEnabled:       true,
	}
	node := newTestNodeWithControllableBroker(t, broker, opts)
	m := node.sharedPollManager

	// Track key1 → broker.Subscribe called.
	_, _, err := m.track("test:channel", opts, "key1")
	require.NoError(t, err)

	// Untrack key1 → triggers dissolver unsubscribe job.
	m.untrack("test:channel", "key1")

	// Immediately re-track with key2 → new channel state, new broker.Subscribe.
	_, _, err = m.track("test:channel", opts, "key2")
	require.NoError(t, err)

	// Wait for dissolver to finish any pending work.
	require.Eventually(t, func() bool {
		// The dissolver job either:
		// 1. Sees stillActive=true (channel re-created before job runs) → skip
		// 2. Unsubscribes, then subscribeToBroker re-subscribes
		// Either way, the final state should be consistent.
		m.brokerSubMu.Lock()
		_, hasSub := m.brokerSubChans[sharedPollKeyChannel("test:channel", "key2")]
		m.brokerSubMu.Unlock()
		return hasSub
	}, 2*time.Second, 10*time.Millisecond)

	// Channel should exist with key2.
	m.mu.RLock()
	s, exists := m.channels["test:channel"]
	m.mu.RUnlock()
	require.True(t, exists)

	s.mu.Lock()
	_, hasKey2 := s.itemIndex["key2"]
	s.mu.Unlock()
	require.True(t, hasKey2)
}

func TestSharedPollManager_ConcurrentTrackNoPanic(t *testing.T) {
	// Stress test: concurrent track on different channels should not panic.
	broker := &controllableBroker{}
	opts := SharedPollChannelOptions{
		RefreshInterval:      30 * time.Second,
		MaxKeysPerConnection: 100,
		PublishEnabled:       true,
	}
	node := newTestNodeWithControllableBroker(t, broker, opts)
	m := node.sharedPollManager

	const numGoroutines = 20
	var wg sync.WaitGroup
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(i int) {
			defer wg.Done()
			ch := "test:channel"
			key := "key" + string(rune('A'+i))
			_, _, _ = m.track(ch, opts, key)
		}(i)
	}
	wg.Wait()

	// Verify all keys tracked.
	m.mu.RLock()
	s, exists := m.channels["test:channel"]
	m.mu.RUnlock()
	require.True(t, exists)
	s.mu.Lock()
	require.Equal(t, numGoroutines, len(s.itemIndex))
	s.mu.Unlock()

	// One broker.Subscribe per unique key.
	require.Equal(t, int32(numGoroutines), broker.subscribeCount.Load())
}

func TestSharedPollManager_ScheduleShutdownImmediate(t *testing.T) {
	// When ChannelShutdownDelay is 0, scheduleShutdown calls doShutdown immediately.
	broker := &controllableBroker{}
	opts := SharedPollChannelOptions{
		RefreshInterval:      30 * time.Second,
		MaxKeysPerConnection: 100,
		ChannelShutdownDelay: -1, // immediate
	}
	node := newTestNodeWithControllableBroker(t, broker, opts)
	m := node.sharedPollManager

	// Track a key so channel gets created.
	_, _, _ = m.track("test:shutdown_imm", opts, "k1")
	m.mu.RLock()
	_, exists := m.channels["test:shutdown_imm"]
	m.mu.RUnlock()
	require.True(t, exists)

	// Untrack the key — should trigger immediate shutdown.
	m.untrack("test:shutdown_imm", "k1")

	// Channel should be removed.
	require.Eventually(t, func() bool {
		m.mu.RLock()
		defer m.mu.RUnlock()
		_, exists := m.channels["test:shutdown_imm"]
		return !exists
	}, 2*time.Second, 10*time.Millisecond)
}

func TestSharedPollManager_ScheduleShutdownDelayed(t *testing.T) {
	// When ChannelShutdownDelay > 0, scheduleShutdown defers shutdown.
	// doShutdownLocked is called from the timer callback.
	broker := &controllableBroker{}
	opts := SharedPollChannelOptions{
		RefreshInterval:      30 * time.Second,
		MaxKeysPerConnection: 100,
		ChannelShutdownDelay: 100 * time.Millisecond,
	}
	node := newTestNodeWithControllableBroker(t, broker, opts)
	m := node.sharedPollManager

	// Track and untrack.
	_, _, _ = m.track("test:shutdown_delay", opts, "k1")
	m.untrack("test:shutdown_delay", "k1")

	// Channel should still exist immediately after untrack (delay pending).
	m.mu.RLock()
	_, exists := m.channels["test:shutdown_delay"]
	m.mu.RUnlock()
	require.True(t, exists, "channel should still exist during shutdown delay")

	// After delay, doShutdownLocked fires and channel is removed.
	require.Eventually(t, func() bool {
		m.mu.RLock()
		defer m.mu.RUnlock()
		_, exists := m.channels["test:shutdown_delay"]
		return !exists
	}, 2*time.Second, 10*time.Millisecond)
}

func TestSharedPollManager_ShutdownCancelledByRetrack(t *testing.T) {
	// If a key is re-tracked during shutdown delay, shutdown is cancelled.
	broker := &controllableBroker{}
	opts := SharedPollChannelOptions{
		RefreshInterval:      30 * time.Second,
		MaxKeysPerConnection: 100,
		ChannelShutdownDelay: 500 * time.Millisecond,
	}
	node := newTestNodeWithControllableBroker(t, broker, opts)
	m := node.sharedPollManager

	_, _, _ = m.track("test:retrack", opts, "k1")
	m.untrack("test:retrack", "k1")

	// Re-track before delay fires.
	_, _, _ = m.track("test:retrack", opts, "k2")

	// Channel should still exist after the shutdown delay would have fired.
	// Use Eventually with inverted check: keep asserting existence over a window
	// that exceeds the original delay.
	require.Never(t, func() bool {
		m.mu.RLock()
		defer m.mu.RUnlock()
		_, exists := m.channels["test:retrack"]
		return !exists
	}, 700*time.Millisecond, 50*time.Millisecond, "channel should survive because re-tracked")
}

// --- sharedPollKeyChannel / parseSharedPollKeyChannel tests ---

func TestSharedPollKeyChannel_RoundTrip(t *testing.T) {
	tests := []struct {
		channel string
		key     string
	}{
		{"ch", "k"},
		{"test:channel", "key1"},
		{"a", ""},                  // empty key
		{"", "key"},                // empty channel
		{"chan:with:colons", "k1"}, // colons in channel
		{"ch", "key:with:colons"},  // colons in key
		{"123", "456"},             // numeric strings
		{"a", "b"},                 // single-char
		{string(make([]byte, 200)), "long_channel"}, // long channel name
	}
	for _, tt := range tests {
		ch, key := parseSharedPollKeyChannel(sharedPollKeyChannel(tt.channel, tt.key))
		require.Equal(t, tt.channel, ch, "channel mismatch for %q/%q", tt.channel, tt.key)
		require.Equal(t, tt.key, key, "key mismatch for %q/%q", tt.channel, tt.key)
	}
}

func TestParseSharedPollKeyChannel_Invalid(t *testing.T) {
	tests := []string{
		"normal_channel",      // no colon prefix
		"abc:xyz",             // non-numeric prefix
		"-1:test",             // negative length
		"100:ab",              // length exceeds string
		":test",               // empty prefix (IndexByte returns 0)
		"",                    // empty string
		"0:",                  // zero-length channel, no key
		"99999999999999:long", // overflow
	}
	for _, input := range tests {
		ch, key := parseSharedPollKeyChannel(input)
		if input == "0:" {
			// "0:" → channel="" key="" — this is actually valid (empty channel).
			continue
		}
		require.Equal(t, "", ch, "expected empty channel for %q", input)
		require.Equal(t, "", key, "expected empty key for %q", input)
	}
}

// --- trackKeys tests ---

func TestSharedPollManager_TrackKeys_Basic(t *testing.T) {
	broker := &controllableBroker{}
	opts := SharedPollChannelOptions{
		RefreshInterval:      30 * time.Second,
		MaxKeysPerConnection: 100,
		PublishEnabled:       true,
	}
	node := newTestNodeWithControllableBroker(t, broker, opts)
	m := node.sharedPollManager

	results, err := m.trackKeys("test:channel", opts, []string{"key1", "key2", "key3"})
	require.NoError(t, err)
	require.Len(t, results, 3)

	// All keys should be new.
	for i, r := range results {
		require.True(t, r.isNew, "key %d should be new", i)
		require.Equal(t, uint64(0), r.entryVersion, "key %d version should be 0", i)
	}

	// Channel should exist with 3 keys.
	m.mu.RLock()
	s := m.channels["test:channel"]
	m.mu.RUnlock()
	require.NotNil(t, s)
	s.mu.Lock()
	require.Equal(t, 3, len(s.itemIndex))
	require.True(t, s.workerRunning)
	s.mu.Unlock()

	// Single broker.Subscribe call with 3 key-channels.
	// controllableBroker counts per channel in the variadic call.
	require.Equal(t, int32(3), broker.subscribeCount.Load())

	// All key-channels should be in brokerSubChans.
	m.brokerSubMu.Lock()
	for _, key := range []string{"key1", "key2", "key3"} {
		_, ok := m.brokerSubChans[sharedPollKeyChannel("test:channel", key)]
		require.True(t, ok, "broker sub missing for %s", key)
	}
	m.brokerSubMu.Unlock()
}

func TestSharedPollManager_TrackKeys_MixedNewAndExisting(t *testing.T) {
	broker := &controllableBroker{}
	opts := SharedPollChannelOptions{
		RefreshInterval:      30 * time.Second,
		MaxKeysPerConnection: 100,
		PublishEnabled:       true,
	}
	node := newTestNodeWithControllableBroker(t, broker, opts)
	m := node.sharedPollManager

	// Pre-track key1.
	_, _, err := m.track("test:channel", opts, "key1")
	require.NoError(t, err)
	require.Equal(t, int32(1), broker.subscribeCount.Load())

	// Batch track: key1 (existing) + key2, key3 (new).
	results, err := m.trackKeys("test:channel", opts, []string{"key1", "key2", "key3"})
	require.NoError(t, err)
	require.Len(t, results, 3)

	require.False(t, results[0].isNew, "key1 should not be new")
	require.True(t, results[1].isNew, "key2 should be new")
	require.True(t, results[2].isNew, "key3 should be new")

	// Only 2 new keys should trigger subscribe (key2, key3).
	// Total: 1 (from track) + 2 (from trackKeys) = 3.
	require.Equal(t, int32(3), broker.subscribeCount.Load())
}

func TestSharedPollManager_TrackKeys_NoPublishEnabled(t *testing.T) {
	broker := &controllableBroker{}
	opts := SharedPollChannelOptions{
		RefreshInterval:      30 * time.Second,
		MaxKeysPerConnection: 100,
		PublishEnabled:       false,
	}
	node := newTestNodeWithControllableBroker(t, broker, opts)
	m := node.sharedPollManager

	results, err := m.trackKeys("test:channel", opts, []string{"key1", "key2"})
	require.NoError(t, err)
	require.Len(t, results, 2)
	require.True(t, results[0].isNew)
	require.True(t, results[1].isNew)

	// No broker.Subscribe calls when PublishEnabled=false.
	require.Equal(t, int32(0), broker.subscribeCount.Load())
}

func TestSharedPollManager_TrackKeys_SubscribeFailureCleanup(t *testing.T) {
	broker := &controllableBroker{subscribeErr: errors.New("broker down")}
	opts := SharedPollChannelOptions{
		RefreshInterval:      30 * time.Second,
		MaxKeysPerConnection: 100,
		PublishEnabled:       true,
	}
	node := newTestNodeWithControllableBroker(t, broker, opts)
	m := node.sharedPollManager

	results, err := m.trackKeys("test:channel", opts, []string{"key1", "key2"})
	require.Error(t, err)
	require.Nil(t, results)

	// Channel should be cleaned up (was empty — all keys were new and removed).
	m.mu.RLock()
	_, exists := m.channels["test:channel"]
	m.mu.RUnlock()
	require.False(t, exists, "channel should be removed after total subscribe failure")
}

func TestSharedPollManager_TrackKeys_SubscribeFailurePartialCleanup(t *testing.T) {
	// Pre-track key0 (succeeds), then batch-track key1+key2 (fails).
	// key0 should survive, key1+key2 should be cleaned up.
	var callCount atomic.Int32
	broker := &controllableBroker{
		subscribeFunc: func(ch string) error {
			n := callCount.Add(1)
			if n > 1 {
				// Fail on all calls after the first (the batch call).
				return errors.New("broker down")
			}
			return nil
		},
	}
	opts := SharedPollChannelOptions{
		RefreshInterval:      30 * time.Second,
		MaxKeysPerConnection: 100,
		PublishEnabled:       true,
	}
	node := newTestNodeWithControllableBroker(t, broker, opts)
	m := node.sharedPollManager

	// Pre-track key0 — succeeds.
	_, _, err := m.track("test:channel", opts, "key0")
	require.NoError(t, err)

	// Batch track key1+key2 — broker fails.
	results, err := m.trackKeys("test:channel", opts, []string{"key1", "key2"})
	require.Error(t, err)
	require.Nil(t, results)

	// Channel should still exist (key0 is still tracked).
	m.mu.RLock()
	s, exists := m.channels["test:channel"]
	m.mu.RUnlock()
	require.True(t, exists, "channel should survive because key0 is still tracked")

	s.mu.Lock()
	_, hasKey0 := s.itemIndex["key0"]
	_, hasKey1 := s.itemIndex["key1"]
	_, hasKey2 := s.itemIndex["key2"]
	s.mu.Unlock()
	require.True(t, hasKey0, "key0 should survive")
	require.False(t, hasKey1, "key1 should be cleaned up")
	require.False(t, hasKey2, "key2 should be cleaned up")
}

func TestSharedPollManager_TrackKeys_DuplicateKeys(t *testing.T) {
	broker := &controllableBroker{}
	opts := SharedPollChannelOptions{
		RefreshInterval:      30 * time.Second,
		MaxKeysPerConnection: 100,
		PublishEnabled:       true,
	}
	node := newTestNodeWithControllableBroker(t, broker, opts)
	m := node.sharedPollManager

	// Track with duplicates.
	results, err := m.trackKeys("test:channel", opts, []string{"key1", "key1", "key2"})
	require.NoError(t, err)
	require.Len(t, results, 3)

	// First occurrence is new, second is not.
	require.True(t, results[0].isNew)
	require.False(t, results[1].isNew)
	require.True(t, results[2].isNew)

	// Only 2 unique new keys should trigger subscribe.
	require.Equal(t, int32(2), broker.subscribeCount.Load())

	m.mu.RLock()
	s := m.channels["test:channel"]
	m.mu.RUnlock()
	s.mu.Lock()
	require.Equal(t, 2, len(s.itemIndex)) // 2 unique keys
	s.mu.Unlock()
}

func TestSharedPollManager_TrackKeys_Empty(t *testing.T) {
	broker := &controllableBroker{}
	opts := SharedPollChannelOptions{
		RefreshInterval:      30 * time.Second,
		MaxKeysPerConnection: 100,
		PublishEnabled:       true,
	}
	node := newTestNodeWithControllableBroker(t, broker, opts)
	m := node.sharedPollManager

	results, err := m.trackKeys("test:channel", opts, nil)
	require.NoError(t, err)
	require.Nil(t, results)

	results, err = m.trackKeys("test:channel", opts, []string{})
	require.NoError(t, err)
	require.Nil(t, results)

	// No channel state should be created.
	m.mu.RLock()
	_, exists := m.channels["test:channel"]
	m.mu.RUnlock()
	require.False(t, exists)

	require.Equal(t, int32(0), broker.subscribeCount.Load())
}

func TestSharedPollManager_TrackKeys_ConcurrentBatch(t *testing.T) {
	// Two concurrent trackKeys calls on the same channel should not lose keys.
	broker := &controllableBroker{}
	opts := SharedPollChannelOptions{
		RefreshInterval:      30 * time.Second,
		MaxKeysPerConnection: 100,
		PublishEnabled:       true,
	}
	node := newTestNodeWithControllableBroker(t, broker, opts)
	m := node.sharedPollManager

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, _ = m.trackKeys("test:channel", opts, []string{"a1", "a2", "a3"})
	}()
	go func() {
		defer wg.Done()
		_, _ = m.trackKeys("test:channel", opts, []string{"b1", "b2", "b3"})
	}()
	wg.Wait()

	m.mu.RLock()
	s := m.channels["test:channel"]
	m.mu.RUnlock()
	require.NotNil(t, s)
	s.mu.Lock()
	require.Equal(t, 6, len(s.itemIndex))
	s.mu.Unlock()

	// 6 unique keys = 6 broker subscribe calls.
	require.Equal(t, int32(6), broker.subscribeCount.Load())
}

// TestSharedPoll_Untrack_UnknownChannel covers the early-return branch in
// SharedPollManager.untrack when the channel isn't tracked at all.
func TestSharedPoll_Untrack_UnknownChannel(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t)
	// Pure no-op call — nothing should panic, nothing happens.
	node.sharedPollManager.untrack("never-seen", "any-key")
}

// TestSharedPoll_GetWarmKeyData_UnknownChannel covers the early-return branch
// when the channel isn't tracked.
func TestSharedPoll_GetWarmKeyData_UnknownChannel(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t)
	got := node.sharedPollManager.getWarmKeyData("never-seen", []string{"k"})
	require.Nil(t, got)
}

// TestSharedPoll_GetCachedData_UnknownChannel covers the early-return branch
// when the channel isn't tracked.
func TestSharedPoll_GetCachedData_UnknownChannel(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t)
	got := node.sharedPollManager.getCachedData("never-seen", []TrackItem{{Key: "k", Version: 1}})
	require.Nil(t, got)
}

// TestSharedPoll_MarkNeedsBroadcast_UnknownChannel covers the early-return branch
// when the channel isn't tracked.
func TestSharedPoll_MarkNeedsBroadcast_UnknownChannel(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t)
	// Should be a silent no-op.
	node.sharedPollManager.markNeedsBroadcast("never-seen", []string{"k"})
}

// TestSharedPoll_HandlePublishedData_UnknownChannel covers the early-return
// branch in handlePublishedData when the channel isn't tracked.
func TestSharedPoll_HandlePublishedData_UnknownChannel(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t)
	node.sharedPollManager.handlePublishedData("never-seen", "k", 1, "epoch", []byte(`{}`))
}

// TestSharedPoll_HandlePublishedData_VersionlessGuard covers the defensive
// versionless guard in handlePublishedData (publish should never reach this
// path in versionless mode, but the guard exists).
func TestSharedPoll_HandlePublishedData_VersionlessGuard(t *testing.T) {
	t.Parallel()
	// Versionless channel options.
	node := newTestNodeWithSharedPoll(t, SharedPollChannelOptions{
		RefreshIntervalFn: func(time.Duration) time.Duration {
			return 0 // 0 means versionless mode (no version tracking).
		},
		RefreshInterval:      100 * time.Millisecond,
		MaxKeysPerConnection: 100,
	})
	setupSharedPollHandlers(node)
	client := newTestClientV2(t, node, "u-vless")
	connectClientV2(t, client)
	subscribeSharedPollClient(t, client, "test:channel")
	trackSharedPollClient(t, client, "test:channel", []*protocol.KeyedItem{{Key: "k", Version: 0}})

	// Direct call to handlePublishedData on a versionless channel should hit
	// the defensive guard and return without effect.
	node.sharedPollManager.handlePublishedData("test:channel", "k", 5, "epoch", []byte(`{}`))
}

// TestSharedPoll_SubscribeBrokerKeys_Empty covers the early-return when
// subscribeToBrokerKeys is called with no keys.
func TestSharedPoll_SubscribeBrokerKeys_Empty(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t)
	require.NoError(t, node.sharedPollManager.subscribeToBrokerKeys("ch", nil))
	require.NoError(t, node.sharedPollManager.subscribeToBrokerKeys("ch", []string{}))
}

// TestSharedPoll_UnsubscribeBrokerKeys_Empty covers the early-return in
// unsubscribeFromBrokerKeys when keys is empty.
func TestSharedPoll_UnsubscribeBrokerKeys_Empty(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t)
	node.sharedPollManager.unsubscribeFromBrokerKeys("ch", nil)
	node.sharedPollManager.unsubscribeFromBrokerKeys("ch", []string{})
}

// TestSharedPoll_Publish_VersionlessRejected covers the publish error path
// when called on a versionless channel.
func TestSharedPoll_Publish_VersionlessRejected(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t, SharedPollChannelOptions{
		RefreshIntervalFn: func(time.Duration) time.Duration {
			return 0
		},
		RefreshInterval:      100 * time.Millisecond,
		MaxKeysPerConnection: 100,
	})
	setupSharedPollHandlers(node)
	client := newTestClientV2(t, node, "u-vless")
	connectClientV2(t, client)
	subscribeSharedPollClient(t, client, "test:channel")
	trackSharedPollClient(t, client, "test:channel", []*protocol.KeyedItem{{Key: "k", Version: 0}})

	err := node.sharedPollManager.publish(context.Background(), "test:channel", "k", 1, "ep", []byte(`{}`))
	require.Error(t, err)
	require.Contains(t, err.Error(), "versionless")
}

// TestSharedPoll_Publish_LocalOnly covers the "channel not active, no broker"
// path of publish — falls through to handlePublishedData (which then early-returns
// since the channel isn't tracked).
func TestSharedPoll_Publish_LocalOnly(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t)
	require.NoError(t, node.sharedPollManager.publish(
		context.Background(), "never-seen", "k", 1, "ep", []byte(`{}`),
	))
}

// TestSharedPoll_ScheduleShutdown_Immediate covers the negative-delay
// (immediate shutdown) path.
func TestSharedPoll_ScheduleShutdown_Immediate(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t)
	setupSharedPollHandlers(node)
	client := newTestClientV2(t, node, "u-imm")
	connectClientV2(t, client)
	subscribeSharedPollClient(t, client, "test:channel")
	trackSharedPollClient(t, client, "test:channel", []*protocol.KeyedItem{{Key: "k1", Version: 0}})

	node.sharedPollManager.mu.RLock()
	s := node.sharedPollManager.channels["test:channel"]
	node.sharedPollManager.mu.RUnlock()
	require.NotNil(t, s)

	// Immediate shutdown via negative delay.
	s.scheduleShutdown(node.sharedPollManager, "test:channel", -1)

	// Channel state should now be removed from the manager.
	node.sharedPollManager.mu.RLock()
	_, exists := node.sharedPollManager.channels["test:channel"]
	node.sharedPollManager.mu.RUnlock()
	require.False(t, exists)
}

// TestSharedPoll_ScheduleShutdown_StopsExistingTimer covers the branch in
// scheduleShutdown that stops a previously-scheduled shutdown timer when
// invoked again.
func TestSharedPoll_ScheduleShutdown_StopsExistingTimer(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t)
	setupSharedPollHandlers(node)
	client := newTestClientV2(t, node, "u-twoshutdown")
	connectClientV2(t, client)
	subscribeSharedPollClient(t, client, "test:channel")
	trackSharedPollClient(t, client, "test:channel", []*protocol.KeyedItem{{Key: "k1", Version: 0}})

	node.sharedPollManager.mu.RLock()
	s := node.sharedPollManager.channels["test:channel"]
	node.sharedPollManager.mu.RUnlock()
	require.NotNil(t, s)

	// First schedule with a long delay so the timer doesn't fire.
	s.scheduleShutdown(node.sharedPollManager, "test:channel", time.Hour)
	// Second schedule must stop the first timer (covers the
	// `if s.shutdownTimer != nil { Stop }` branch).
	s.scheduleShutdown(node.sharedPollManager, "test:channel", time.Hour)

	// Cleanup so the channel doesn't linger.
	s.scheduleShutdown(node.sharedPollManager, "test:channel", -1)
}

// TestSharedPoll_Track_AfterShutdown covers the early-return branch in
// SharedPollManager.track when the manager's shutdownCh is already closed.
// We call close() once and rely on the in-test t.Cleanup to skip a second close.
func TestSharedPoll_Track_AfterShutdown(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t)

	opts, _ := node.config.SharedPoll.GetSharedPollChannelOptions("test:channel")

	// Close the manager once — this closes shutdownCh.
	node.sharedPollManager.close()

	// Subsequent track / trackKeys calls must hit the shutdown-guard early return.
	isNew, ver, err := node.sharedPollManager.track("test:channel", opts, "k")
	require.NoError(t, err)
	require.False(t, isNew)
	require.Equal(t, uint64(0), ver)

	results, err := node.sharedPollManager.trackKeys("test:channel", opts, []string{"k1", "k2"})
	require.NoError(t, err)
	require.Len(t, results, 2)
	for _, r := range results {
		require.False(t, r.isNew)
	}

	// Replace shutdownCh with a fresh open channel so the t.Cleanup → Shutdown
	// → close() doesn't double-close.
	node.sharedPollManager.shutdownCh = make(chan struct{})
}

// TestSharedPoll_Stats covers the stats() helper.
func TestSharedPoll_Stats(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t)
	setupSharedPollHandlers(node)
	client := newTestClientV2(t, node, "u-stats")
	connectClientV2(t, client)
	subscribeSharedPollClient(t, client, "test:channel")
	trackSharedPollClient(t, client, "test:channel", []*protocol.KeyedItem{
		{Key: "k1", Version: 0},
		{Key: "k2", Version: 0},
	})

	channels, keys := node.sharedPollManager.stats()
	require.GreaterOrEqual(t, channels, 1)
	require.GreaterOrEqual(t, keys, 2)
}
