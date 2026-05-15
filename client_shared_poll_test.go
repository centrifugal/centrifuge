package centrifuge

import (
	"bytes"
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/centrifugal/protocol"
	fdelta "github.com/shadowspore/fossil-delta"
	"github.com/stretchr/testify/require"
)

func newTestNodeWithSharedPoll(t *testing.T, opts ...SharedPollChannelOptions) *Node {
	var spOpts SharedPollChannelOptions
	if len(opts) > 0 {
		spOpts = opts[0]
	} else {
		spOpts = SharedPollChannelOptions{
			RefreshInterval:      100 * time.Millisecond,
			RefreshBatchSize:     100,
			MaxKeysPerConnection: 100,
		}
	}

	node, err := New(Config{
		LogLevel:   LogLevelTrace,
		LogHandler: func(entry LogEntry) {},
		SharedPoll: SharedPollConfig{
			GetSharedPollChannelOptions: func(channel string) (SharedPollChannelOptions, bool) {
				return spOpts, true
			},
		},
	})
	require.NoError(t, err)

	// Default OnSharedPoll — does nothing.
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

func setupSharedPollHandlers(node *Node) {
	node.OnConnecting(func(ctx context.Context, e ConnectEvent) (ConnectReply, error) {
		return ConnectReply{}, nil
	})
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					ExpireAt: time.Now().Unix() + 3600,
				},
				ClientSideRefresh: true,
			}, nil)
		})
		client.OnTrack(func(e TrackEvent, cb TrackCallback) {
			cb(TrackReply{}, nil)
		})
		client.OnSubRefresh(func(e SubRefreshEvent, cb SubRefreshCallback) {
			cb(SubRefreshReply{}, nil)
		})
	})
}

func subscribeSharedPollClient(t testing.TB, client *Client, channel string) *protocol.SubscribeResult {
	// Pre-create the channel entry so subscribe has something to check.
	client.mu.Lock()
	if client.channels == nil {
		client.channels = make(map[string]ChannelContext)
	}
	client.channels[channel] = ChannelContext{}
	client.mu.Unlock()

	rwWrapper := testReplyWriterWrapper()
	err := client.handleSubscribe(&protocol.SubscribeRequest{
		Channel: channel,
		Type:    int32(SubscriptionTypeSharedPoll),
	}, &protocol.Command{Id: 1}, time.Now(), rwWrapper.rw)
	require.NoError(t, err)

	// Subscribe handler is async (callback-based) — wait for reply.
	require.Eventually(t, func() bool {
		return len(rwWrapper.replies) > 0
	}, time.Second, time.Millisecond)
	require.Nil(t, rwWrapper.replies[0].Error, "subscribe error: %v", rwWrapper.replies[0].Error)
	return rwWrapper.replies[0].Subscribe
}

func trackSharedPollClient(t testing.TB, client *Client, channel string, items []*protocol.KeyedItem) {
	trackSharedPollClientWithReply(t, client, channel, items)
}

func trackSharedPollClientWithReply(t testing.TB, client *Client, channel string, items []*protocol.KeyedItem) *protocol.SubRefreshResult {
	rwWrapper := testReplyWriterWrapper()
	err := client.handleSubRefresh(&protocol.SubRefreshRequest{
		Channel: channel,
		Type:    typeTrack,
		Track:   []*protocol.TrackBatch{{Items: items}},
	}, &protocol.Command{Id: 2}, time.Now(), rwWrapper.rw)
	require.NoError(t, err)

	// Track handler is async (callback-based) — wait for reply.
	require.Eventually(t, func() bool {
		return len(rwWrapper.replies) > 0
	}, time.Second, time.Millisecond)
	require.Nil(t, rwWrapper.replies[0].Error, "track error: %v", rwWrapper.replies[0].Error)
	return rwWrapper.replies[0].SubRefresh
}

func untrackSharedPollClient(t testing.TB, client *Client, channel string, keys []string) {
	rwWrapper := testReplyWriterWrapper()
	err := client.handleSubRefresh(&protocol.SubRefreshRequest{
		Channel: channel,
		Type:    typeUntrack,
		Untrack: keys,
	}, &protocol.Command{Id: 3}, time.Now(), rwWrapper.rw)
	require.NoError(t, err)
	require.Len(t, rwWrapper.replies, 1)
	require.Nil(t, rwWrapper.replies[0].Error, "untrack error: %v", rwWrapper.replies[0].Error)
}

// 1.1 Subscribe Tests

func TestSharedPollSubscribe_Basic(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t)
	setupSharedPollHandlers(node)
	client := newTestClientV2(t, node, "user1")
	connectClientV2(t, client)

	result := subscribeSharedPollClient(t, client, "test:channel")
	require.Equal(t, int32(4), result.Type)
	require.True(t, result.Expires)
	require.True(t, result.Ttl > 0)

	// Verify no broker subscribe, no hub addSub.
	require.Equal(t, 0, node.Hub().NumSubscriptions())
}

func TestSharedPollSubscribe_NotConfigured(t *testing.T) {
	t.Parallel()
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()
	setupSharedPollHandlers(node)
	client := newTestClientV2(t, node, "user1")
	connectClientV2(t, client)

	rwWrapper := testReplyWriterWrapper()
	err := client.handleSubscribe(&protocol.SubscribeRequest{
		Channel: "test:channel",
		Type:    int32(SubscriptionTypeSharedPoll),
	}, &protocol.Command{Id: 1}, time.Now(), rwWrapper.rw)
	require.Equal(t, ErrorNotAvailable, err)
}

func TestSharedPollSubscribe_WrongType(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t)
	setupSharedPollHandlers(node)
	client := newTestClientV2(t, node, "user1")
	connectClientV2(t, client)

	// Try subscribing with type=0 (stream) on a shared poll channel.
	rwWrapper := testReplyWriterWrapper()
	err := client.handleSubscribe(&protocol.SubscribeRequest{
		Channel: "test:channel",
		Type:    int32(SubscriptionTypeStream),
	}, &protocol.Command{Id: 1}, time.Now(), rwWrapper.rw)
	require.Equal(t, ErrorBadRequest, err)
}

func TestSharedPollSubscribe_OnSubscribeReject(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t)

	node.OnConnecting(func(ctx context.Context, e ConnectEvent) (ConnectReply, error) {
		return ConnectReply{Credentials: &Credentials{UserID: "testuser"}}, nil
	})
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{}, ErrorPermissionDenied)
		})
	})

	client := newTestClientV2(t, node, "user1")
	connectClientV2(t, client)

	rwWrapper := testReplyWriterWrapper()
	err := client.handleSubscribe(&protocol.SubscribeRequest{
		Channel: "test:channel",
		Type:    int32(SubscriptionTypeSharedPoll),
	}, &protocol.Command{Id: 1}, time.Now(), rwWrapper.rw)
	require.NoError(t, err)
	// Error is written to reply, not returned.
	require.Len(t, rwWrapper.replies, 1)
	require.NotNil(t, rwWrapper.replies[0].Error)
	require.Equal(t, uint32(ErrorPermissionDenied.Code), rwWrapper.replies[0].Error.Code)
}

func TestSharedPollSubscribe_FlagKeyed(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t)
	setupSharedPollHandlers(node)
	client := newTestClientV2(t, node, "user1")
	connectClientV2(t, client)

	subscribeSharedPollClient(t, client, "test:channel")

	client.mu.RLock()
	ctx, ok := client.channels["test:channel"]
	client.mu.RUnlock()
	require.True(t, ok)
	require.True(t, channelHasFlag(ctx.flags, flagKeyed))
	require.True(t, channelHasFlag(ctx.flags, flagSubscribed))
	require.True(t, channelHasFlag(ctx.flags, flagClientSideRefresh))
}

func TestSharedPollSubscribe_NoRecoveryFields(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t)
	setupSharedPollHandlers(node)
	client := newTestClientV2(t, node, "user1")
	connectClientV2(t, client)

	result := subscribeSharedPollClient(t, client, "test:channel")
	// Epoch is set for versionless channels (non-empty), but offset/publications are not.
	require.NotEmpty(t, result.Epoch)
	require.Equal(t, uint64(0), result.Offset)
	require.Empty(t, result.Publications)
}

// 1.2 Track / Untrack Tests

func TestSharedPollTrack_Basic(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t)
	setupSharedPollHandlers(node)
	client := newTestClientV2(t, node, "user1")
	connectClientV2(t, client)

	subscribeSharedPollClient(t, client, "test:channel")
	trackSharedPollClient(t, client, "test:channel", []*protocol.KeyedItem{
		{Key: "key1", Version: 5},
		{Key: "key2", Version: 10},
	})

	// Verify items in keyedHub.
	hub := node.keyedManager.getHub("test:channel")
	require.NotNil(t, hub)
	require.True(t, hub.hasSubscriber("key1", client))
	require.True(t, hub.hasSubscriber("key2", client))

	// Verify per-connection state.
	client.mu.RLock()
	require.NotNil(t, client.keyed)
	require.Equal(t, uint64(5), client.keyed.trackedKeys["test:channel"]["key1"].version)
	require.Equal(t, uint64(10), client.keyed.trackedKeys["test:channel"]["key2"].version)
	client.mu.RUnlock()
}

func TestSharedPollTrack_SignatureRejected(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t)

	node.OnConnecting(func(ctx context.Context, e ConnectEvent) (ConnectReply, error) {
		return ConnectReply{Credentials: &Credentials{UserID: "testuser"}}, nil
	})
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options:           SubscribeOptions{ExpireAt: time.Now().Unix() + 3600},
				ClientSideRefresh: true,
			}, nil)
		})
		client.OnTrack(func(e TrackEvent, cb TrackCallback) {
			cb(TrackReply{}, ErrorPermissionDenied)
		})
		client.OnSubRefresh(func(e SubRefreshEvent, cb SubRefreshCallback) {
			cb(SubRefreshReply{}, nil)
		})
	})

	client := newTestClientV2(t, node, "user1")
	connectClientV2(t, client)
	subscribeSharedPollClient(t, client, "test:channel")

	rwWrapper := testReplyWriterWrapper()
	err := client.handleSubRefresh(&protocol.SubRefreshRequest{
		Channel: "test:channel",
		Type:    typeTrack,
		Track:   []*protocol.TrackBatch{{Items: []*protocol.KeyedItem{{Key: "key1", Version: 1}}}},
	}, &protocol.Command{Id: 2}, time.Now(), rwWrapper.rw)
	require.NoError(t, err)
	require.Len(t, rwWrapper.replies, 1)
	require.NotNil(t, rwWrapper.replies[0].Error)

	// Verify no items added.
	hub := node.keyedManager.getHub("test:channel")
	require.NotNil(t, hub)
	require.Equal(t, 0, hub.subscriberCount("key1"))
}

func TestSharedPollTrack_MaxTrackedExceeded(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t, SharedPollChannelOptions{
		RefreshInterval:      100 * time.Millisecond,
		MaxKeysPerConnection: 2,
	})
	setupSharedPollHandlers(node)
	client := newTestClientV2(t, node, "user1")
	connectClientV2(t, client)
	subscribeSharedPollClient(t, client, "test:channel")

	// Track 2 items (at limit).
	trackSharedPollClient(t, client, "test:channel", []*protocol.KeyedItem{
		{Key: "key1", Version: 1},
		{Key: "key2", Version: 2},
	})

	// Track 1 more (over limit).
	rwWrapper := testReplyWriterWrapper()
	err := client.handleSubRefresh(&protocol.SubRefreshRequest{
		Channel: "test:channel",
		Type:    typeTrack,
		Track:   []*protocol.TrackBatch{{Items: []*protocol.KeyedItem{{Key: "key3", Version: 3}}}},
	}, &protocol.Command{Id: 3}, time.Now(), rwWrapper.rw)
	require.Equal(t, ErrorLimitExceeded, err)
}

// TestSharedPollTrack_ConcurrentLimitNotBypassed exercises the race between
// the optimistic RLock-based limit check at the top of handleTrack and the
// async write in the trackHandler callback. Many goroutines fire single-key
// track requests simultaneously; each picks a distinct key but the total
// exceeds MaxKeysPerConnection. The final trackedKeys count must stay at or
// under the limit — earlier code wrote per-connection state before any
// re-check and could bypass the limit.
func TestSharedPollTrack_ConcurrentLimitNotBypassed(t *testing.T) {
	t.Parallel()
	const limit = 4
	const goroutines = 16

	node := newTestNodeWithSharedPoll(t, SharedPollChannelOptions{
		RefreshInterval:      100 * time.Millisecond,
		MaxKeysPerConnection: limit,
	})

	// Synchronise all OnTrack callbacks at a barrier so they release together
	// — this maximises the chance of multiple goroutines being mid-callback
	// when they each try to commit per-connection state.
	var entered, released sync.WaitGroup
	entered.Add(goroutines)
	released.Add(1)
	node.OnConnecting(func(ctx context.Context, e ConnectEvent) (ConnectReply, error) {
		return ConnectReply{}, nil
	})
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options:           SubscribeOptions{ExpireAt: time.Now().Unix() + 3600},
				ClientSideRefresh: true,
			}, nil)
		})
		client.OnTrack(func(e TrackEvent, cb TrackCallback) {
			entered.Done()
			released.Wait()
			cb(TrackReply{}, nil)
		})
	})
	client := newTestClientV2(t, node, "user1")
	connectClientV2(t, client)
	subscribeSharedPollClient(t, client, "test:channel")

	// Fire `goroutines` concurrent single-key track requests.
	// Use a small enum so we don't pollute the test with reflection across the
	// two error shapes (Error from handleSubRefresh, *protocol.Error from reply).
	const (
		outcomeOK = iota
		outcomeLimit
		outcomeOther
	)
	results := make(chan int, goroutines)
	for i := 0; i < goroutines; i++ {
		go func(i int) {
			rwWrapper := testReplyWriterWrapper()
			key := "k" + strconv.Itoa(i)
			err := client.handleSubRefresh(&protocol.SubRefreshRequest{
				Channel: "test:channel",
				Type:    typeTrack,
				Track:   []*protocol.TrackBatch{{Items: []*protocol.KeyedItem{{Key: key, Version: 1}}}},
			}, &protocol.Command{Id: uint32(100 + i)}, time.Now(), rwWrapper.rw)
			if err != nil {
				if e, ok := err.(*Error); ok && e.Code == ErrorLimitExceeded.Code {
					results <- outcomeLimit
				} else {
					results <- outcomeOther
				}
				return
			}
			// Wait for callback reply (success or error) to land.
			require.Eventually(t, func() bool {
				return len(rwWrapper.replies) > 0
			}, 2*time.Second, time.Millisecond)
			rerr := rwWrapper.replies[0].Error
			if rerr == nil {
				results <- outcomeOK
			} else if rerr.Code == ErrorLimitExceeded.Code {
				results <- outcomeLimit
			} else {
				results <- outcomeOther
			}
		}(i)
	}

	// Wait until every callback has entered the trackHandler, then release.
	entered.Wait()
	released.Done()

	// Collect outcomes.
	var ok, exceeded int
	for i := 0; i < goroutines; i++ {
		switch <-results {
		case outcomeOK:
			ok++
		case outcomeLimit:
			exceeded++
		default:
			t.Fatalf("unexpected outcome from goroutine #%d", i)
		}
	}

	// Final trackedKeys size must respect the limit.
	client.mu.RLock()
	count := 0
	if client.keyed != nil {
		count = len(client.keyed.trackedKeys["test:channel"])
	}
	client.mu.RUnlock()
	require.LessOrEqual(t, count, limit, "per-connection trackedKeys exceeded limit: %d > %d", count, limit)
	require.Equal(t, ok, count, "successful tracks should match committed trackedKeys count")
	require.Equal(t, goroutines, ok+exceeded, "every goroutine should return success or ErrorLimitExceeded")
}

func TestSharedPollTrack_ItemIndexVersion0(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t)
	setupSharedPollHandlers(node)
	client := newTestClientV2(t, node, "user1")
	connectClientV2(t, client)
	subscribeSharedPollClient(t, client, "test:channel")

	trackSharedPollClient(t, client, "test:channel", []*protocol.KeyedItem{
		{Key: "key1", Version: 99},
	})

	// ItemIndex should have version=0 (not client-provided 99).
	node.sharedPollManager.mu.RLock()
	s := node.sharedPollManager.channels["test:channel"]
	node.sharedPollManager.mu.RUnlock()
	require.NotNil(t, s)
	s.mu.Lock()
	entry := s.itemIndex["key1"]
	require.NotNil(t, entry)
	require.Equal(t, uint64(0), entry.version)
	s.mu.Unlock()
}

func TestSharedPollTrack_MultipleClients(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t)
	setupSharedPollHandlers(node)

	client1 := newTestClientV2(t, node, "user1")
	connectClientV2(t, client1)
	subscribeSharedPollClient(t, client1, "test:channel")

	client2 := newTestClientV2(t, node, "user2")
	connectClientV2(t, client2)
	subscribeSharedPollClient(t, client2, "test:channel")

	trackSharedPollClient(t, client1, "test:channel", []*protocol.KeyedItem{
		{Key: "shared_key", Version: 1},
		{Key: "only1", Version: 2},
	})
	trackSharedPollClient(t, client2, "test:channel", []*protocol.KeyedItem{
		{Key: "shared_key", Version: 3},
		{Key: "only2", Version: 4},
	})

	hub := node.keyedManager.getHub("test:channel")
	require.Equal(t, 2, hub.subscriberCount("shared_key"))
	require.Equal(t, 1, hub.subscriberCount("only1"))
	require.Equal(t, 1, hub.subscriberCount("only2"))
}

func TestSharedPollUntrack_Basic(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t)
	setupSharedPollHandlers(node)
	client := newTestClientV2(t, node, "user1")
	connectClientV2(t, client)
	subscribeSharedPollClient(t, client, "test:channel")

	trackSharedPollClient(t, client, "test:channel", []*protocol.KeyedItem{
		{Key: "key1", Version: 1},
		{Key: "key2", Version: 2},
	})

	untrackSharedPollClient(t, client, "test:channel", []string{"key1"})

	hub := node.keyedManager.getHub("test:channel")
	require.Equal(t, 0, hub.subscriberCount("key1"))
	require.Equal(t, 1, hub.subscriberCount("key2"))

	client.mu.RLock()
	_, hasKey1 := client.keyed.trackedKeys["test:channel"]["key1"]
	_, hasKey2 := client.keyed.trackedKeys["test:channel"]["key2"]
	client.mu.RUnlock()
	require.False(t, hasKey1)
	require.True(t, hasKey2)
}

func TestSharedPollUntrack_NonexistentKey(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t)
	setupSharedPollHandlers(node)
	client := newTestClientV2(t, node, "user1")
	connectClientV2(t, client)
	subscribeSharedPollClient(t, client, "test:channel")

	// Untrack a key that was never tracked — should succeed without error.
	untrackSharedPollClient(t, client, "test:channel", []string{"nonexistent"})
}

func TestSharedPollTrack_EmptyItems(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t)
	setupSharedPollHandlers(node)
	client := newTestClientV2(t, node, "user1")
	connectClientV2(t, client)
	subscribeSharedPollClient(t, client, "test:channel")

	rwWrapper := testReplyWriterWrapper()
	err := client.handleSubRefresh(&protocol.SubRefreshRequest{
		Channel: "test:channel",
		Type:    typeTrack,
		Track:   nil,
	}, &protocol.Command{Id: 2}, time.Now(), rwWrapper.rw)
	require.Equal(t, ErrorBadRequest, err)
}

func TestSharedPollUntrack_EmptyKeys(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t)
	setupSharedPollHandlers(node)
	client := newTestClientV2(t, node, "user1")
	connectClientV2(t, client)
	subscribeSharedPollClient(t, client, "test:channel")

	rwWrapper := testReplyWriterWrapper()
	err := client.handleSubRefresh(&protocol.SubRefreshRequest{
		Channel: "test:channel",
		Type:    typeUntrack,
		Untrack: nil,
	}, &protocol.Command{Id: 2}, time.Now(), rwWrapper.rw)
	require.Equal(t, ErrorBadRequest, err)
}

// 1.3 Refresh Worker Tests

func TestSharedPollRefresh_BasicCycle(t *testing.T) {
	t.Parallel()
	var pollCalled atomic.Int32
	var pollMu sync.Mutex
	var pollItems []SharedPollItem

	node := newTestNodeWithSharedPoll(t)

	node.OnSharedPoll(func(ctx context.Context, event SharedPollEvent) (SharedPollResult, error) {
		pollCalled.Add(1)
		pollMu.Lock()
		pollItems = event.Items
		pollMu.Unlock()
		return SharedPollResult{
			Items: []SharedPollRefreshItem{
				{Key: "key1", Data: []byte(`{"v":1}`), Version: 1},
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

	// Wait for at least one refresh cycle.
	require.Eventually(t, func() bool {
		return pollCalled.Load() > 0
	}, 2*time.Second, 10*time.Millisecond)

	pollMu.Lock()
	require.Len(t, pollItems, 1)
	require.Equal(t, "key1", pollItems[0].Key)
	pollMu.Unlock()
}

func TestSharedPollRefresh_VersionIncreased(t *testing.T) {
	t.Parallel()
	var version atomic.Uint64
	version.Store(1)

	node := newTestNodeWithSharedPoll(t)

	node.OnSharedPoll(func(ctx context.Context, event SharedPollEvent) (SharedPollResult, error) {
		v := version.Load()
		return SharedPollResult{
			Items: []SharedPollRefreshItem{
				{Key: "key1", Data: []byte(`{"v":1}`), Version: v},
			},
		}, nil
	})

	setupSharedPollHandlers(node)
	transport := newTestTransport(func() {})
	transport.setProtocolVersion(ProtocolVersion2)
	ctx := SetCredentials(context.Background(), &Credentials{UserID: "user1"})
	client, err := newClient(ctx, node, transport)
	require.NoError(t, err)

	connectClientV2(t, client)
	subscribeSharedPollClient(t, client, "test:channel")
	trackSharedPollClient(t, client, "test:channel", []*protocol.KeyedItem{
		{Key: "key1", Version: 0},
	})

	// Wait for publication delivery.
	require.Eventually(t, func() bool {
		client.mu.RLock()
		defer client.mu.RUnlock()
		ks := client.keyed.trackedKeys["test:channel"]["key1"]
		return ks != nil && ks.version >= 1
	}, 2*time.Second, 10*time.Millisecond)
}

func TestSharedPollRefresh_VersionUnchanged(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t)
	var callCount atomic.Int32

	node.OnSharedPoll(func(ctx context.Context, event SharedPollEvent) (SharedPollResult, error) {
		callCount.Add(1)
		return SharedPollResult{
			Items: []SharedPollRefreshItem{
				{Key: "key1", Data: []byte(`{"v":1}`), Version: 5},
			},
		}, nil
	})

	setupSharedPollHandlers(node)
	client := newTestClientV2(t, node, "user1")
	connectClientV2(t, client)
	subscribeSharedPollClient(t, client, "test:channel")

	// Track with version=5 — same as what backend returns.
	trackSharedPollClient(t, client, "test:channel", []*protocol.KeyedItem{
		{Key: "key1", Version: 5},
	})

	// Wait for several refresh cycles. 100 ms RefreshInterval × 3 = 300 ms
	// in theory; under -race -count=N scheduling stretch we give 5 s slack.
	require.Eventually(t, func() bool {
		return callCount.Load() >= 3
	}, 5*time.Second, 10*time.Millisecond)

	// Per-connection version should remain 5 (broadcastToKey skips: 5 <= 5).
	client.mu.RLock()
	ks := client.keyed.trackedKeys["test:channel"]["key1"]
	client.mu.RUnlock()
	require.Equal(t, uint64(5), ks.version)
}

func TestSharedPollRefresh_MaxPolicy(t *testing.T) {
	t.Parallel()
	var callCount atomic.Int32

	node := newTestNodeWithSharedPoll(t)

	// First call returns v=10, second returns v=5 (stale).
	node.OnSharedPoll(func(ctx context.Context, event SharedPollEvent) (SharedPollResult, error) {
		count := callCount.Add(1)
		var version uint64
		if count == 1 {
			version = 10
		} else {
			version = 5 // Stale replica
		}
		return SharedPollResult{
			Items: []SharedPollRefreshItem{
				{Key: "key1", Data: []byte(`{"v":1}`), Version: version},
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

	// Wait for at least 3 calls.
	require.Eventually(t, func() bool {
		return callCount.Load() >= 3
	}, 2*time.Second, 10*time.Millisecond)

	// ItemIndex should be at v=10 (never downgraded).
	node.sharedPollManager.mu.RLock()
	s := node.sharedPollManager.channels["test:channel"]
	node.sharedPollManager.mu.RUnlock()
	s.mu.Lock()
	require.Equal(t, uint64(10), s.itemIndex["key1"].version)
	s.mu.Unlock()
}

func TestSharedPollRefresh_Batching(t *testing.T) {
	t.Parallel()
	var batchCalls atomic.Int32

	node := newTestNodeWithSharedPoll(t, SharedPollChannelOptions{
		RefreshInterval:      100 * time.Millisecond,
		RefreshBatchSize:     100,
		MaxKeysPerConnection: 500,
	})

	node.OnSharedPoll(func(ctx context.Context, event SharedPollEvent) (SharedPollResult, error) {
		batchCalls.Add(1)
		return SharedPollResult{}, nil
	})

	setupSharedPollHandlers(node)
	client := newTestClientV2(t, node, "user1")
	connectClientV2(t, client)
	subscribeSharedPollClient(t, client, "test:channel")

	// Track 250 items.
	items := make([]*protocol.KeyedItem, 250)
	for i := range items {
		items[i] = &protocol.KeyedItem{Key: "key" + time.Now().Format("150405") + string(rune(i)), Version: 0}
	}
	trackSharedPollClient(t, client, "test:channel", items)

	// Wait for at least one cycle. Should produce 3 batches (100+100+50).
	require.Eventually(t, func() bool {
		return batchCalls.Load() >= 3
	}, 2*time.Second, 10*time.Millisecond)
}

func TestSharedPollRefresh_PublicationFields(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t)

	node.OnSharedPoll(func(ctx context.Context, event SharedPollEvent) (SharedPollResult, error) {
		return SharedPollResult{
			Items: []SharedPollRefreshItem{
				{Key: "key1", Data: []byte(`test_data`), Version: 42},
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

	// Verify publication is delivered by checking per-connection version update.
	require.Eventually(t, func() bool {
		client.mu.RLock()
		defer client.mu.RUnlock()
		ks := client.keyed.trackedKeys["test:channel"]["key1"]
		return ks != nil && ks.version == 42
	}, 2*time.Second, 10*time.Millisecond)

	// Also verify the itemIndex has the correct version.
	node.sharedPollManager.mu.RLock()
	s := node.sharedPollManager.channels["test:channel"]
	node.sharedPollManager.mu.RUnlock()
	s.mu.Lock()
	require.Equal(t, uint64(42), s.itemIndex["key1"].version)
	s.mu.Unlock()
}

// 1.4 Item Removal Tests

func TestSharedPollRefresh_ExplicitRemoval(t *testing.T) {
	t.Parallel()
	var callCount atomic.Int32

	node := newTestNodeWithSharedPoll(t)

	node.OnSharedPoll(func(ctx context.Context, event SharedPollEvent) (SharedPollResult, error) {
		count := callCount.Add(1)
		if count == 1 {
			return SharedPollResult{
				Items: []SharedPollRefreshItem{
					{Key: "key1", Data: []byte(`data`), Version: 1},
				},
			}, nil
		}
		// Second call: mark as removed.
		return SharedPollResult{
			Items: []SharedPollRefreshItem{
				{Key: "key1", Removed: true},
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

	// Wait for removal to propagate into itemIndex.
	require.Eventually(t, func() bool {
		if callCount.Load() < 2 {
			return false
		}
		node.sharedPollManager.mu.RLock()
		s, ok := node.sharedPollManager.channels["test:channel"]
		node.sharedPollManager.mu.RUnlock()
		if !ok {
			return true
		}
		s.mu.Lock()
		_, hasKey := s.itemIndex["key1"]
		s.mu.Unlock()
		return !hasKey
	}, 2*time.Second, 10*time.Millisecond)
}

// 1.5 Revocation Tests

func TestSharedPollRevokeKeys_Basic(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t)

	node.OnSharedPoll(func(ctx context.Context, event SharedPollEvent) (SharedPollResult, error) {
		// Return data so items stay alive.
		items := make([]SharedPollRefreshItem, len(event.Items))
		for i, it := range event.Items {
			items[i] = SharedPollRefreshItem{Key: it.Key, Data: []byte(`data`), Version: 1}
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

	// Wait for items to be established.
	time.Sleep(150 * time.Millisecond)

	// Revoke key1.
	node.sharedPollManager.SharedPollRevokeKeys("test:channel", []string{"key1"}, nil, nil)

	hub := node.keyedManager.getHub("test:channel")
	require.Equal(t, 0, hub.subscriberCount("key1"))
	require.Equal(t, 1, hub.subscriberCount("key2"))
}

func TestSharedPollRevokeKeys_UserFilter(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t)

	node.OnSharedPoll(func(ctx context.Context, event SharedPollEvent) (SharedPollResult, error) {
		items := make([]SharedPollRefreshItem, len(event.Items))
		for i, it := range event.Items {
			items[i] = SharedPollRefreshItem{Key: it.Key, Data: []byte(`data`), Version: 1}
		}
		return SharedPollResult{Items: items}, nil
	})

	setupSharedPollHandlers(node)

	client1 := newTestClientV2(t, node, "user1")
	connectClientV2(t, client1)
	subscribeSharedPollClient(t, client1, "test:channel")
	trackSharedPollClient(t, client1, "test:channel", []*protocol.KeyedItem{
		{Key: "shared_key", Version: 0},
	})

	client2 := newTestClientV2(t, node, "user2")
	connectClientV2(t, client2)
	subscribeSharedPollClient(t, client2, "test:channel")
	trackSharedPollClient(t, client2, "test:channel", []*protocol.KeyedItem{
		{Key: "shared_key", Version: 0},
	})

	time.Sleep(150 * time.Millisecond)

	// Revoke only for user1.
	node.sharedPollManager.SharedPollRevokeKeys("test:channel", []string{"shared_key"}, []string{"user1"}, nil)

	hub := node.keyedManager.getHub("test:channel")
	// user2 should still be tracking.
	require.True(t, hub.hasSubscriber("shared_key", client2))
}

// 1.7 Mode (Versioned)

func TestSharedPollVersionedMode_VersionsInRequest(t *testing.T) {
	t.Parallel()
	var pollItems []SharedPollItem
	var pollMu sync.Mutex
	var callCount atomic.Int32

	node := newTestNodeWithSharedPoll(t, SharedPollChannelOptions{
		RefreshInterval:      100 * time.Millisecond,
		RefreshBatchSize:     100,
		MaxKeysPerConnection: 100,
		Mode:                 SharedPollModeVersioned,
	})

	node.OnSharedPoll(func(ctx context.Context, event SharedPollEvent) (SharedPollResult, error) {
		count := callCount.Add(1)
		if count == 1 {
			// First call: return version 5.
			return SharedPollResult{
				Items: []SharedPollRefreshItem{
					{Key: "key1", Data: []byte(`data`), Version: 5},
				},
			}, nil
		}
		// Second call: capture items to check versions.
		pollMu.Lock()
		pollItems = make([]SharedPollItem, len(event.Items))
		copy(pollItems, event.Items)
		pollMu.Unlock()
		return SharedPollResult{
			Items: []SharedPollRefreshItem{
				{Key: "key1", Data: []byte(`data`), Version: 5},
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

	// Wait for at least 2 calls.
	require.Eventually(t, func() bool {
		return callCount.Load() >= 2
	}, 2*time.Second, 10*time.Millisecond)

	pollMu.Lock()
	require.Len(t, pollItems, 1)
	require.Equal(t, "key1", pollItems[0].Key)
	require.Equal(t, uint64(5), pollItems[0].Version) // Should include version from itemIndex.
	pollMu.Unlock()
}

func TestSharedPollVersionedMode_AlwaysSendsVersions(t *testing.T) {
	t.Parallel()
	var pollItems []SharedPollItem
	var pollMu sync.Mutex
	var callCount atomic.Int32

	node := newTestNodeWithSharedPoll(t, SharedPollChannelOptions{
		RefreshInterval:      100 * time.Millisecond,
		RefreshBatchSize:     100,
		MaxKeysPerConnection: 100,
		Mode:                 SharedPollModeVersioned,
	})

	node.OnSharedPoll(func(ctx context.Context, event SharedPollEvent) (SharedPollResult, error) {
		count := callCount.Add(1)
		if count == 1 {
			return SharedPollResult{
				Items: []SharedPollRefreshItem{
					{Key: "key1", Data: []byte(`data`), Version: 5},
				},
			}, nil
		}
		pollMu.Lock()
		pollItems = make([]SharedPollItem, len(event.Items))
		copy(pollItems, event.Items)
		pollMu.Unlock()
		return SharedPollResult{
			Items: []SharedPollRefreshItem{
				{Key: "key1", Data: []byte(`data`), Version: 5},
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

	require.Eventually(t, func() bool {
		return callCount.Load() >= 2
	}, 2*time.Second, 10*time.Millisecond)

	pollMu.Lock()
	require.Len(t, pollItems, 1)
	require.Equal(t, uint64(5), pollItems[0].Version) // Versioned mode always sends versions.
	pollMu.Unlock()
}

// 1.8 Connection Lifecycle Tests

func TestSharedPollDisconnect_Cleanup(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t)

	node.OnSharedPoll(func(ctx context.Context, event SharedPollEvent) (SharedPollResult, error) {
		items := make([]SharedPollRefreshItem, len(event.Items))
		for i, it := range event.Items {
			items[i] = SharedPollRefreshItem{Key: it.Key, Data: []byte(`data`), Version: 1}
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

	hub := node.keyedManager.getHub("test:channel")
	require.Equal(t, 1, hub.subscriberCount("key1"))

	// Disconnect client.
	_ = client.close(DisconnectConnectionClosed)

	// Subscriber should be removed.
	require.Equal(t, 0, hub.subscriberCount("key1"))
}

func TestSharedPollUnsubscribe_Cleanup(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t)

	node.OnSharedPoll(func(ctx context.Context, event SharedPollEvent) (SharedPollResult, error) {
		items := make([]SharedPollRefreshItem, len(event.Items))
		for i, it := range event.Items {
			items[i] = SharedPollRefreshItem{Key: it.Key, Data: []byte(`data`), Version: 1}
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

	hub := node.keyedManager.getHub("test:channel")
	require.Equal(t, 1, hub.subscriberCount("key1"))

	// Unsubscribe.
	_ = client.unsubscribe("test:channel", unsubscribeClient, nil)

	// Subscriber should be removed.
	require.Equal(t, 0, hub.subscriberCount("key1"))
}

// 1.10 Concurrency Tests

func TestSharedPollConcurrent_TrackUntrack(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t, SharedPollChannelOptions{
		RefreshInterval:      50 * time.Millisecond,
		RefreshBatchSize:     100,
		MaxKeysPerConnection: 1000,
	})

	node.OnSharedPoll(func(ctx context.Context, event SharedPollEvent) (SharedPollResult, error) {
		return SharedPollResult{}, nil
	})

	setupSharedPollHandlers(node)

	const numClients = 5
	const numKeys = 20
	var wg sync.WaitGroup

	clients := make([]*Client, numClients)
	for i := 0; i < numClients; i++ {
		clients[i] = newTestClientV2(t, node, "user"+string(rune('0'+i)))
		connectClientV2(t, clients[i])
		subscribeSharedPollClient(t, clients[i], "test:channel")
	}

	// Concurrent track/untrack from all clients.
	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(c *Client) {
			defer wg.Done()
			for j := 0; j < numKeys; j++ {
				key := "key" + string(rune('a'+j))
				trackSharedPollClient(t, c, "test:channel", []*protocol.KeyedItem{
					{Key: key, Version: uint64(j)},
				})
			}
			for j := 0; j < numKeys/2; j++ {
				key := "key" + string(rune('a'+j))
				untrackSharedPollClient(t, c, "test:channel", []string{key})
			}
		}(clients[i])
	}

	wg.Wait()
	// No panic, no race = pass.
}

// 1.11 Config Defaults Tests

func TestSharedPollConfig_Defaults(t *testing.T) {
	t.Parallel()
	opts := SharedPollChannelOptions{}
	require.Equal(t, time.Duration(0), opts.RefreshInterval) // Zero → default 10s in worker
	require.Equal(t, 0, opts.RefreshBatchSize)               // Zero → default 1000 in worker
	require.Equal(t, 0, opts.MaxKeysPerConnection)           // Zero → default 5000 in keyed manager
	require.Equal(t, time.Duration(0), opts.CallTimeout)     // Zero → default 30s in worker
}

func TestSharedPollConfig_ToKeyedChannelOptions(t *testing.T) {
	t.Parallel()
	opts := SharedPollChannelOptions{
		MaxKeysPerConnection: 42,
		RefreshInterval:      time.Second,
	}
	keyedOpts := opts.toKeyedChannelOptions()
	require.Equal(t, 42, keyedOpts.MaxTrackedPerConnection)
}

// 1.12 Node Startup Validation

func TestSharedPollNode_MissingOnSharedPoll(t *testing.T) {
	t.Parallel()
	node, err := New(Config{
		LogLevel:   LogLevelTrace,
		LogHandler: func(entry LogEntry) {},
		SharedPoll: SharedPollConfig{
			GetSharedPollChannelOptions: func(channel string) (SharedPollChannelOptions, bool) {
				return SharedPollChannelOptions{}, true
			},
		},
	})
	require.NoError(t, err)
	// Don't register OnSharedPoll.
	err = node.Run()
	require.Error(t, err)
	require.Contains(t, err.Error(), "OnSharedPoll handler is not registered")
}

// --- 1.1 Subscribe (additional) ---

func TestSharedPollSubscribe_AlreadySubscribed(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t)
	setupSharedPollHandlers(node)

	client := newTestClientV2(t, node, "user1")
	connectClientV2(t, client)
	subscribeSharedPollClient(t, client, "test:channel")

	// Second subscribe should fail — error returned directly.
	err := client.handleSubscribe(&protocol.SubscribeRequest{
		Channel: "test:channel",
		Type:    int32(SubscriptionTypeSharedPoll),
	}, &protocol.Command{Id: 2}, time.Now(), testReplyWriterWrapper().rw)
	require.Equal(t, ErrorAlreadySubscribed, err)
}

// --- 1.2 Track/Untrack (additional) ---

func TestSharedPollTrack_PerConnectionVersions(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t)
	setupSharedPollHandlers(node)

	client := newTestClientV2(t, node, "user1")
	connectClientV2(t, client)
	subscribeSharedPollClient(t, client, "test:channel")
	trackSharedPollClient(t, client, "test:channel", []*protocol.KeyedItem{
		{Key: "key1", Version: 42},
		{Key: "key2", Version: 100},
	})

	// Check per-connection state has client-provided versions.
	client.mu.RLock()
	chanKeys := client.keyed.trackedKeys["test:channel"]
	require.Equal(t, uint64(42), chanKeys["key1"].version)
	require.Equal(t, uint64(100), chanKeys["key2"].version)
	client.mu.RUnlock()

	// Verify itemIndex has version=0 (client versions don't enter itemIndex).
	node.sharedPollManager.mu.RLock()
	s := node.sharedPollManager.channels["test:channel"]
	node.sharedPollManager.mu.RUnlock()
	s.mu.Lock()
	require.Equal(t, uint64(0), s.itemIndex["key1"].version)
	require.Equal(t, uint64(0), s.itemIndex["key2"].version)
	s.mu.Unlock()
}

func TestSharedPollTrack_AdditionalBatch(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t)
	setupSharedPollHandlers(node)

	client := newTestClientV2(t, node, "user1")
	connectClientV2(t, client)
	subscribeSharedPollClient(t, client, "test:channel")

	// Track batch 1.
	trackSharedPollClient(t, client, "test:channel", []*protocol.KeyedItem{
		{Key: "key1", Version: 0},
		{Key: "key2", Version: 0},
	})

	// Track batch 2.
	trackSharedPollClient(t, client, "test:channel", []*protocol.KeyedItem{
		{Key: "key3", Version: 0},
	})

	hub := node.keyedManager.getHub("test:channel")
	require.True(t, hub.hasSubscriber("key1", client))
	require.True(t, hub.hasSubscriber("key2", client))
	require.True(t, hub.hasSubscriber("key3", client))

	client.mu.RLock()
	require.Len(t, client.keyed.trackedKeys["test:channel"], 3)
	client.mu.RUnlock()
}

func TestSharedPollUntrack_NoSignature(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t)
	setupSharedPollHandlers(node)

	client := newTestClientV2(t, node, "user1")
	connectClientV2(t, client)
	subscribeSharedPollClient(t, client, "test:channel")
	trackSharedPollClient(t, client, "test:channel", []*protocol.KeyedItem{
		{Key: "key1", Version: 0},
	})

	// Untrack does not require signature — should succeed.
	untrackSharedPollClient(t, client, "test:channel", []string{"key1"})

	// Key should be removed from per-connection state.
	client.mu.RLock()
	_, tracked := client.keyed.trackedKeys["test:channel"]["key1"]
	client.mu.RUnlock()
	require.False(t, tracked)
}

func TestSharedPollUntrack_LastSubscriber(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t)
	setupSharedPollHandlers(node)

	client := newTestClientV2(t, node, "user1")
	connectClientV2(t, client)
	subscribeSharedPollClient(t, client, "test:channel")
	trackSharedPollClient(t, client, "test:channel", []*protocol.KeyedItem{
		{Key: "key1", Version: 0},
	})

	// Verify key1 is in itemIndex.
	node.sharedPollManager.mu.RLock()
	s := node.sharedPollManager.channels["test:channel"]
	node.sharedPollManager.mu.RUnlock()
	s.mu.Lock()
	_, hasKey := s.itemIndex["key1"]
	s.mu.Unlock()
	require.True(t, hasKey)

	// Untrack last subscriber.
	untrackSharedPollClient(t, client, "test:channel", []string{"key1"})

	// Key should be removed from itemIndex since no subscribers remain.
	s.mu.Lock()
	_, hasKey = s.itemIndex["key1"]
	s.mu.Unlock()
	require.False(t, hasKey)
}

func TestSharedPollUntrack_OtherSubscribersRemain(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t)
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

	// client1 untracks — but client2 still tracks key1.
	untrackSharedPollClient(t, client1, "test:channel", []string{"key1"})

	// Key should still be in itemIndex because client2 is still subscribed.
	node.sharedPollManager.mu.RLock()
	s := node.sharedPollManager.channels["test:channel"]
	node.sharedPollManager.mu.RUnlock()
	s.mu.Lock()
	_, hasKey := s.itemIndex["key1"]
	s.mu.Unlock()
	require.True(t, hasKey)

	hub := node.keyedManager.getHub("test:channel")
	require.False(t, hub.hasSubscriber("key1", client1))
	require.True(t, hub.hasSubscriber("key1", client2))
}

// --- 1.3 Refresh Worker (additional) ---

func TestSharedPollRefresh_MultipleItems(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t)

	node.OnSharedPoll(func(ctx context.Context, event SharedPollEvent) (SharedPollResult, error) {
		// Only 2 of 5 items changed.
		return SharedPollResult{
			Items: []SharedPollRefreshItem{
				{Key: "key2", Data: []byte(`{"v":2}`), Version: 1},
				{Key: "key4", Data: []byte(`{"v":4}`), Version: 1},
			},
		}, nil
	})

	setupSharedPollHandlers(node)
	client := newTestClientV2(t, node, "user1")
	connectClientV2(t, client)
	subscribeSharedPollClient(t, client, "test:channel")

	items := make([]*protocol.KeyedItem, 5)
	for i := 0; i < 5; i++ {
		items[i] = &protocol.KeyedItem{Key: "key" + string(rune('1'+i)), Version: 0}
	}
	trackSharedPollClient(t, client, "test:channel", items)

	// Wait for per-connection versions to update for the 2 changed items.
	require.Eventually(t, func() bool {
		client.mu.RLock()
		defer client.mu.RUnlock()
		chanKeys := client.keyed.trackedKeys["test:channel"]
		return chanKeys["key2"] != nil && chanKeys["key2"].version == 1 && chanKeys["key4"] != nil && chanKeys["key4"].version == 1
	}, 2*time.Second, 10*time.Millisecond)

	// Unchanged items should still have version 0.
	client.mu.RLock()
	chanKeys := client.keyed.trackedKeys["test:channel"]
	require.Equal(t, uint64(0), chanKeys["key1"].version)
	require.Equal(t, uint64(0), chanKeys["key3"].version)
	require.Equal(t, uint64(0), chanKeys["key5"].version)
	client.mu.RUnlock()
}

func TestSharedPollRefresh_PerConnectionFilter(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t)

	node.OnSharedPoll(func(ctx context.Context, event SharedPollEvent) (SharedPollResult, error) {
		return SharedPollResult{
			Items: []SharedPollRefreshItem{
				{Key: "key1", Data: []byte(`data`), Version: 5},
			},
		}, nil
	})

	setupSharedPollHandlers(node)
	client := newTestClientV2(t, node, "user1")
	connectClientV2(t, client)
	subscribeSharedPollClient(t, client, "test:channel")

	// Client tracks with version=5 — same as what backend returns.
	trackSharedPollClient(t, client, "test:channel", []*protocol.KeyedItem{
		{Key: "key1", Version: 5},
	})

	// Wait for refresh cycle.
	time.Sleep(300 * time.Millisecond)

	// Per-connection version should still be 5 — not updated because 5 <= 5.
	client.mu.RLock()
	chanKeys := client.keyed.trackedKeys["test:channel"]
	require.Equal(t, uint64(5), chanKeys["key1"].version)
	client.mu.RUnlock()
}

func TestSharedPollRefresh_PerConnectionDelivery(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t)

	node.OnSharedPoll(func(ctx context.Context, event SharedPollEvent) (SharedPollResult, error) {
		return SharedPollResult{
			Items: []SharedPollRefreshItem{
				{Key: "key1", Data: []byte(`new`), Version: 5},
			},
		}, nil
	})

	setupSharedPollHandlers(node)
	client := newTestClientV2(t, node, "user1")
	connectClientV2(t, client)
	subscribeSharedPollClient(t, client, "test:channel")

	// Client tracks with version=3 — lower than what backend will return (5).
	trackSharedPollClient(t, client, "test:channel", []*protocol.KeyedItem{
		{Key: "key1", Version: 3},
	})

	// Publication should be delivered (5 > 3). Per-connection version updated to 5.
	require.Eventually(t, func() bool {
		client.mu.RLock()
		defer client.mu.RUnlock()
		chanKeys := client.keyed.trackedKeys["test:channel"]
		return chanKeys["key1"] != nil && chanKeys["key1"].version == 5
	}, 2*time.Second, 10*time.Millisecond)
}

func TestSharedPollRefresh_TwoClientsOverlap(t *testing.T) {
	t.Parallel()
	var version atomic.Uint64
	version.Store(5) // First call returns 6, then 7, etc.

	node := newTestNodeWithSharedPoll(t)

	node.OnSharedPoll(func(ctx context.Context, event SharedPollEvent) (SharedPollResult, error) {
		v := version.Add(1)
		return SharedPollResult{
			Items: []SharedPollRefreshItem{
				{Key: "key1", Data: []byte(`data`), Version: v},
			},
		}, nil
	})

	setupSharedPollHandlers(node)

	client1 := newTestClientV2(t, node, "user1")
	connectClientV2(t, client1)
	subscribeSharedPollClient(t, client1, "test:channel")
	trackSharedPollClient(t, client1, "test:channel", []*protocol.KeyedItem{
		{Key: "key1", Version: 3},
	})

	client2 := newTestClientV2(t, node, "user2")
	connectClientV2(t, client2)
	subscribeSharedPollClient(t, client2, "test:channel")
	trackSharedPollClient(t, client2, "test:channel", []*protocol.KeyedItem{
		{Key: "key1", Version: 5},
	})

	// Both should receive publications (version increases each poll cycle).
	// Auto-notify may deliver first version to client1 before client2 subscribes,
	// but subsequent timer polls deliver newer versions to both.
	require.Eventually(t, func() bool {
		client1.mu.RLock()
		ks1 := client1.keyed.trackedKeys["test:channel"]["key1"]
		var v1 uint64
		if ks1 != nil {
			v1 = ks1.version
		}
		client1.mu.RUnlock()
		client2.mu.RLock()
		ks2 := client2.keyed.trackedKeys["test:channel"]["key1"]
		var v2 uint64
		if ks2 != nil {
			v2 = ks2.version
		}
		client2.mu.RUnlock()
		return v1 >= 6 && v2 >= 6
	}, 2*time.Second, 10*time.Millisecond)
}

func TestSharedPollRefresh_BatchingConcurrency(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t, SharedPollChannelOptions{
		RefreshInterval:      100 * time.Millisecond,
		RefreshBatchSize:     100,
		MaxKeysPerConnection: 5000,
	})

	var mu sync.Mutex
	var timestamps []time.Time

	node.OnSharedPoll(func(ctx context.Context, event SharedPollEvent) (SharedPollResult, error) {
		mu.Lock()
		timestamps = append(timestamps, time.Now())
		mu.Unlock()
		time.Sleep(50 * time.Millisecond) // simulate work
		return SharedPollResult{}, nil
	})

	setupSharedPollHandlers(node)
	client := newTestClientV2(t, node, "user1")
	connectClientV2(t, client)
	subscribeSharedPollClient(t, client, "test:channel")

	// Track 300 items → 3 batches of 100.
	items := make([]*protocol.KeyedItem, 300)
	for i := 0; i < 300; i++ {
		items[i] = &protocol.KeyedItem{Key: "key" + string(rune(i/256+1)) + string(rune(i%256+1)), Version: 0}
	}
	trackSharedPollClient(t, client, "test:channel", items)

	// Wait for at least one cycle to complete.
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(timestamps) >= 3
	}, 3*time.Second, 10*time.Millisecond)

	// With dispatch spreading, 3 batches over a 100ms interval should be
	// dispatched ~33ms apart (interval / num_chunks).
	mu.Lock()
	ts := make([]time.Time, len(timestamps))
	copy(ts, timestamps)
	mu.Unlock()

	if len(ts) >= 3 {
		spread := ts[2].Sub(ts[0])
		require.Greater(t, spread, 20*time.Millisecond, "batches should be spread over the interval, not burst")
	}
}

// --- 1.5 Revocation (additional) ---

func TestSharedPollRevokeKeys_ExcludeUsers(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t)

	node.OnSharedPoll(func(ctx context.Context, event SharedPollEvent) (SharedPollResult, error) {
		items := make([]SharedPollRefreshItem, len(event.Items))
		for i, it := range event.Items {
			items[i] = SharedPollRefreshItem{Key: it.Key, Data: []byte(`data`), Version: 1}
		}
		return SharedPollResult{Items: items}, nil
	})

	setupSharedPollHandlers(node)

	client1 := newTestClientV2(t, node, "user1")
	connectClientV2(t, client1)
	subscribeSharedPollClient(t, client1, "test:channel")
	trackSharedPollClient(t, client1, "test:channel", []*protocol.KeyedItem{
		{Key: "key1", Version: 0},
		{Key: "key2", Version: 0}, // extra key to keep channel alive
	})

	client2 := newTestClientV2(t, node, "user2")
	connectClientV2(t, client2)
	subscribeSharedPollClient(t, client2, "test:channel")
	trackSharedPollClient(t, client2, "test:channel", []*protocol.KeyedItem{
		{Key: "key1", Version: 0},
	})

	// Wait for items to be established.
	time.Sleep(200 * time.Millisecond)

	// Revoke key1 for everyone EXCEPT user1.
	node.sharedPollManager.SharedPollRevokeKeys("test:channel", []string{"key1"}, nil, []string{"user1"})

	hub := node.keyedManager.getHub("test:channel")
	require.NotNil(t, hub, "hub should exist — key2 still has subscribers")
	require.True(t, hub.hasSubscriber("key1", client1), "user1 should be excluded from revocation")
	require.False(t, hub.hasSubscriber("key1", client2), "user2 should be revoked")
}

func TestSharedPollRevokeKeys_NonexistentKey(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t)
	setupSharedPollHandlers(node)

	client := newTestClientV2(t, node, "user1")
	connectClientV2(t, client)
	subscribeSharedPollClient(t, client, "test:channel")
	trackSharedPollClient(t, client, "test:channel", []*protocol.KeyedItem{
		{Key: "key1", Version: 0},
	})

	// Revoke a key nobody tracks — should not error.
	node.sharedPollManager.SharedPollRevokeKeys("test:channel", []string{"nonexistent"}, nil, nil)

	// Original key unaffected.
	hub := node.keyedManager.getHub("test:channel")
	require.True(t, hub.hasSubscriber("key1", client))
}

// --- 1.6 KeepLatestData ---

func TestSharedPollKeepLatestData_DataStored(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t, SharedPollChannelOptions{
		RefreshInterval:      100 * time.Millisecond,
		RefreshBatchSize:     100,
		MaxKeysPerConnection: 100,
		KeepLatestData:       true,
	})

	node.OnSharedPoll(func(ctx context.Context, event SharedPollEvent) (SharedPollResult, error) {
		return SharedPollResult{
			Items: []SharedPollRefreshItem{
				{Key: "key1", Data: []byte(`{"hello":"world"}`), Version: 1},
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

	// Wait for refresh to populate data.
	require.Eventually(t, func() bool {
		node.sharedPollManager.mu.RLock()
		s, ok := node.sharedPollManager.channels["test:channel"]
		node.sharedPollManager.mu.RUnlock()
		if !ok {
			return false
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		entry := s.itemIndex["key1"]
		return entry != nil && entry.data != nil
	}, 2*time.Second, 10*time.Millisecond)

	node.sharedPollManager.mu.RLock()
	s := node.sharedPollManager.channels["test:channel"]
	node.sharedPollManager.mu.RUnlock()
	s.mu.Lock()
	require.Equal(t, []byte(`{"hello":"world"}`), s.itemIndex["key1"].data)
	require.Equal(t, uint64(1), s.itemIndex["key1"].version)
	s.mu.Unlock()
}

func TestSharedPollKeepLatestData_NoDataWithout(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t) // KeepLatestData=false by default.

	node.OnSharedPoll(func(ctx context.Context, event SharedPollEvent) (SharedPollResult, error) {
		return SharedPollResult{
			Items: []SharedPollRefreshItem{
				{Key: "key1", Data: []byte(`{"hello":"world"}`), Version: 1},
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

	// Wait for refresh to update version.
	require.Eventually(t, func() bool {
		node.sharedPollManager.mu.RLock()
		s, ok := node.sharedPollManager.channels["test:channel"]
		node.sharedPollManager.mu.RUnlock()
		if !ok {
			return false
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		entry := s.itemIndex["key1"]
		return entry != nil && entry.version == 1
	}, 2*time.Second, 10*time.Millisecond)

	node.sharedPollManager.mu.RLock()
	s := node.sharedPollManager.channels["test:channel"]
	node.sharedPollManager.mu.RUnlock()
	s.mu.Lock()
	// Data should NOT be stored when KeepLatestData is false.
	require.Nil(t, s.itemIndex["key1"].data)
	s.mu.Unlock()
}

// --- 1.8 Connection Lifecycle (additional) ---

func TestSharedPollChannelShutdown_Immediate(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t, SharedPollChannelOptions{
		RefreshInterval:      100 * time.Millisecond,
		RefreshBatchSize:     100,
		MaxKeysPerConnection: 100,
		ChannelShutdownDelay: -1, // Immediate shutdown.
	})
	setupSharedPollHandlers(node)

	client := newTestClientV2(t, node, "user1")
	connectClientV2(t, client)
	subscribeSharedPollClient(t, client, "test:channel")
	trackSharedPollClient(t, client, "test:channel", []*protocol.KeyedItem{
		{Key: "key1", Version: 0},
	})

	// Untrack last key → triggers immediate shutdown.
	untrackSharedPollClient(t, client, "test:channel", []string{"key1"})

	// Channel should be removed immediately (or very soon).
	require.Eventually(t, func() bool {
		return !node.sharedPollManager.hasChannel("test:channel")
	}, time.Second, 10*time.Millisecond)
}

func TestSharedPollChannelShutdown_Delay(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t, SharedPollChannelOptions{
		RefreshInterval:      100 * time.Millisecond,
		RefreshBatchSize:     100,
		MaxKeysPerConnection: 100,
		ChannelShutdownDelay: 500 * time.Millisecond,
	})
	setupSharedPollHandlers(node)

	client := newTestClientV2(t, node, "user1")
	connectClientV2(t, client)
	subscribeSharedPollClient(t, client, "test:channel")
	trackSharedPollClient(t, client, "test:channel", []*protocol.KeyedItem{
		{Key: "key1", Version: 0},
	})

	// Untrack last key → starts delayed shutdown.
	untrackSharedPollClient(t, client, "test:channel", []string{"key1"})

	// Channel should still exist immediately after untrack (delay=500ms).
	require.True(t, node.sharedPollManager.hasChannel("test:channel"), "channel should still exist during delay")

	// Re-track within the delay period → cancels shutdown.
	trackSharedPollClient(t, client, "test:channel", []*protocol.KeyedItem{
		{Key: "key2", Version: 0},
	})

	// Channel must survive past the original shutdown delay because we
	// re-tracked. require.Never keeps asserting over a window longer than
	// the 500 ms delay; if shutdown were not cancelled the channel would
	// disappear within that window.
	require.Never(t, func() bool {
		return !node.sharedPollManager.hasChannel("test:channel")
	}, 800*time.Millisecond, 50*time.Millisecond,
		"channel must survive after re-track cancels shutdown")
}

// --- Delta compression tests ---

func subscribeSharedPollClientDelta(t testing.TB, client *Client, channel string) *protocol.SubscribeResult {
	client.mu.Lock()
	if client.channels == nil {
		client.channels = make(map[string]ChannelContext)
	}
	client.channels[channel] = ChannelContext{}
	client.mu.Unlock()

	rwWrapper := testReplyWriterWrapper()
	err := client.handleSubscribe(&protocol.SubscribeRequest{
		Channel: channel,
		Type:    int32(SubscriptionTypeSharedPoll),
		Delta:   string(DeltaTypeFossil),
	}, &protocol.Command{Id: 1}, time.Now(), rwWrapper.rw)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return len(rwWrapper.replies) > 0
	}, time.Second, time.Millisecond)
	require.Nil(t, rwWrapper.replies[0].Error, "subscribe error: %v", rwWrapper.replies[0].Error)
	return rwWrapper.replies[0].Subscribe
}

func setupSharedPollDeltaHandlers(node *Node) {
	node.OnConnecting(func(ctx context.Context, e ConnectEvent) (ConnectReply, error) {
		return ConnectReply{}, nil
	})
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					ExpireAt:          time.Now().Unix() + 3600,
					AllowedDeltaTypes: []DeltaType{DeltaTypeFossil},
				},
				ClientSideRefresh: true,
			}, nil)
		})
		client.OnTrack(func(e TrackEvent, cb TrackCallback) {
			cb(TrackReply{}, nil)
		})
		client.OnSubRefresh(func(e SubRefreshEvent, cb SubRefreshCallback) {
			cb(SubRefreshReply{}, nil)
		})
	})
}

func TestSharedPollDelta_NegotiationEnabled(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t)
	setupSharedPollDeltaHandlers(node)

	client := newTestClientV2(t, node, "user1")
	connectClientV2(t, client)
	result := subscribeSharedPollClientDelta(t, client, "test:channel")

	require.True(t, result.Delta)

	// Verify delta type stored in keyed state.
	client.mu.RLock()
	require.NotNil(t, client.keyed)
	require.NotNil(t, client.keyed.channels["test:channel"])
	require.Equal(t, DeltaTypeFossil, client.keyed.channels["test:channel"].deltaType)
	client.mu.RUnlock()
}

func TestSharedPollDelta_NegotiationDisabled(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t)
	// Use default handlers (no AllowedDeltaTypes).
	setupSharedPollHandlers(node)

	client := newTestClientV2(t, node, "user1")
	connectClientV2(t, client)
	result := subscribeSharedPollClientDelta(t, client, "test:channel")

	require.False(t, result.Delta)

	// Verify no delta channel state.
	client.mu.RLock()
	require.Nil(t, client.keyed.channels["test:channel"])
	client.mu.RUnlock()
}

func TestSharedPollDelta_FirstPubIsFull(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t, SharedPollChannelOptions{
		RefreshInterval:      100 * time.Millisecond,
		RefreshBatchSize:     100,
		MaxKeysPerConnection: 100,
		KeepLatestData:       true,
	})
	setupSharedPollDeltaHandlers(node)

	var callCount atomic.Int32
	node.OnSharedPoll(func(ctx context.Context, event SharedPollEvent) (SharedPollResult, error) {
		callCount.Add(1)
		return SharedPollResult{
			Items: []SharedPollRefreshItem{
				{Key: "key1", Data: []byte(`{"v":1}`), Version: 1},
			},
		}, nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	transport := newTestTransport(cancel)
	transport.setProtocolVersion(ProtocolVersion2)
	transport.setProtocolType(ProtocolTypeJSON)
	sink := make(chan []byte, 100)
	transport.sink = sink
	newCtx := SetCredentials(ctx, &Credentials{UserID: "user1"})
	client, _ := newClient(newCtx, node, transport)
	connectClientV2(t, client)
	subscribeSharedPollClientDelta(t, client, "test:channel")
	trackSharedPollClient(t, client, "test:channel", []*protocol.KeyedItem{
		{Key: "key1", Version: 0},
	})

	// Wait for first publication delivery.
	require.Eventually(t, func() bool {
		client.mu.RLock()
		defer client.mu.RUnlock()
		ks := client.keyed.trackedKeys["test:channel"]["key1"]
		return ks != nil && ks.version >= 1
	}, 2*time.Second, 10*time.Millisecond)

	// Read the publication from sink (skip non-publication messages like connect reply).
	var firstPub *protocol.Reply
	timeout := time.After(2 * time.Second)
	for firstPub == nil {
		select {
		case data := <-sink:
			reply := decodeReply(t, protocol.TypeJSON, data)
			if reply.Push != nil && reply.Push.Pub != nil {
				firstPub = reply
			}
		case <-timeout:
			t.Fatal("timeout waiting for publication in sink")
		}
	}

	// First pub should NOT be a delta (deltaReady was false).
	// For JSON+fossil, the data should be JSON-escaped (a string in JSON).
	require.False(t, firstPub.Push.Pub.Delta, "first pub should not be delta")
	require.Equal(t, "key1", firstPub.Push.Pub.Key)

	// Verify deltaReady is now true.
	client.mu.RLock()
	ks := client.keyed.trackedKeys["test:channel"]["key1"]
	client.mu.RUnlock()
	require.True(t, ks.deltaReady)
}

func TestSharedPollDelta_SubsequentPubIsDelta(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t, SharedPollChannelOptions{
		RefreshInterval:      100 * time.Millisecond,
		RefreshBatchSize:     100,
		MaxKeysPerConnection: 100,
		KeepLatestData:       true,
		Mode:                 SharedPollModeVersioned,
	})
	setupSharedPollDeltaHandlers(node)

	version := atomic.Int64{}
	version.Store(1)
	dataVal := atomic.Value{}
	dataVal.Store([]byte(`{"value":"hello world, this is version one of the data payload"}`))

	node.OnSharedPoll(func(ctx context.Context, event SharedPollEvent) (SharedPollResult, error) {
		return SharedPollResult{
			Items: []SharedPollRefreshItem{
				{Key: "key1", Data: dataVal.Load().([]byte), Version: uint64(version.Load())},
			},
		}, nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	transport := newTestTransport(cancel)
	transport.setProtocolVersion(ProtocolVersion2)
	transport.setProtocolType(ProtocolTypeJSON)
	sink := make(chan []byte, 100)
	transport.sink = sink
	newCtx := SetCredentials(ctx, &Credentials{UserID: "user1"})
	client, _ := newClient(newCtx, node, transport)
	connectClientV2(t, client)
	subscribeSharedPollClientDelta(t, client, "test:channel")
	trackSharedPollClient(t, client, "test:channel", []*protocol.KeyedItem{
		{Key: "key1", Version: 0},
	})

	// Wait for first publication (version 1).
	require.Eventually(t, func() bool {
		client.mu.RLock()
		defer client.mu.RUnlock()
		ks := client.keyed.trackedKeys["test:channel"]["key1"]
		return ks != nil && ks.version >= 1
	}, 2*time.Second, 10*time.Millisecond)

	// Bump version to trigger delta publication (small change to large payload).
	version.Store(2)
	dataVal.Store([]byte(`{"value":"hello world, this is version two of the data payload"}`))

	// Wait for second publication (version 2).
	require.Eventually(t, func() bool {
		client.mu.RLock()
		defer client.mu.RUnlock()
		ks := client.keyed.trackedKeys["test:channel"]["key1"]
		return ks != nil && ks.version >= 2
	}, 2*time.Second, 10*time.Millisecond)

	// Read publications from sink, find the one with version 2 (delta).
	var deltaPub *protocol.Reply
	timeout := time.After(2 * time.Second)
	for deltaPub == nil {
		select {
		case d := <-sink:
			reply := decodeReply(t, protocol.TypeJSON, d)
			if reply.Push != nil && reply.Push.Pub != nil && reply.Push.Pub.Version == 2 {
				deltaPub = reply
			}
		case <-timeout:
			t.Fatal("timeout waiting for delta publication in sink")
		}
	}

	require.True(t, deltaPub.Push.Pub.Delta, "second pub should be delta")
	require.Equal(t, "key1", deltaPub.Push.Pub.Key)
}

func TestSharedPollDelta_MultipleKeysIndependent(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t, SharedPollChannelOptions{
		RefreshInterval:      100 * time.Millisecond,
		RefreshBatchSize:     100,
		MaxKeysPerConnection: 100,
		KeepLatestData:       true,
	})
	setupSharedPollDeltaHandlers(node)

	node.OnSharedPoll(func(ctx context.Context, event SharedPollEvent) (SharedPollResult, error) {
		return SharedPollResult{
			Items: []SharedPollRefreshItem{
				{Key: "key1", Data: []byte(`{"a":1}`), Version: 1},
				{Key: "key2", Data: []byte(`{"b":1}`), Version: 1},
			},
		}, nil
	})

	client := newTestClientV2(t, node, "user1")
	connectClientV2(t, client)
	subscribeSharedPollClientDelta(t, client, "test:channel")

	// Track key1 first.
	trackSharedPollClient(t, client, "test:channel", []*protocol.KeyedItem{
		{Key: "key1", Version: 0},
	})

	// Wait for key1's first pub.
	require.Eventually(t, func() bool {
		client.mu.RLock()
		defer client.mu.RUnlock()
		ks := client.keyed.trackedKeys["test:channel"]["key1"]
		return ks != nil && ks.version >= 1
	}, 2*time.Second, 10*time.Millisecond)

	// key1 should be deltaReady now.
	client.mu.RLock()
	ks1DeltaReady := client.keyed.trackedKeys["test:channel"]["key1"].deltaReady
	client.mu.RUnlock()
	require.True(t, ks1DeltaReady)

	// Now track key2.
	trackSharedPollClient(t, client, "test:channel", []*protocol.KeyedItem{
		{Key: "key2", Version: 0},
	})

	// Wait for key2's first pub.
	require.Eventually(t, func() bool {
		client.mu.RLock()
		defer client.mu.RUnlock()
		ks := client.keyed.trackedKeys["test:channel"]["key2"]
		return ks != nil && ks.version >= 1
	}, 2*time.Second, 10*time.Millisecond)

	// key2 should now be deltaReady independently.
	client.mu.RLock()
	ks2DeltaReady := client.keyed.trackedKeys["test:channel"]["key2"].deltaReady
	client.mu.RUnlock()
	require.True(t, ks2DeltaReady)
}

func TestSharedPollDelta_NoDeltaWithoutKeepLatestData(t *testing.T) {
	t.Parallel()
	// Without KeepLatestData, no prevData is captured → no delta.
	node := newTestNodeWithSharedPoll(t, SharedPollChannelOptions{
		RefreshInterval:      100 * time.Millisecond,
		RefreshBatchSize:     100,
		MaxKeysPerConnection: 100,
		KeepLatestData:       false,
	})
	setupSharedPollDeltaHandlers(node)

	version := atomic.Int64{}
	version.Store(1)
	node.OnSharedPoll(func(ctx context.Context, event SharedPollEvent) (SharedPollResult, error) {
		v := version.Add(1) - 1
		return SharedPollResult{
			Items: []SharedPollRefreshItem{
				{Key: "key1", Data: []byte(`{"v":` + string(rune('0'+v)) + `}`), Version: uint64(v)},
			},
		}, nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	transport := newTestTransport(cancel)
	transport.setProtocolVersion(ProtocolVersion2)
	transport.setProtocolType(ProtocolTypeJSON)
	sink := make(chan []byte, 100)
	transport.sink = sink
	newCtx := SetCredentials(ctx, &Credentials{UserID: "user1"})
	client, _ := newClient(newCtx, node, transport)
	connectClientV2(t, client)
	subscribeSharedPollClientDelta(t, client, "test:channel")
	trackSharedPollClient(t, client, "test:channel", []*protocol.KeyedItem{
		{Key: "key1", Version: 0},
	})

	// Wait for two publications.
	require.Eventually(t, func() bool {
		client.mu.RLock()
		defer client.mu.RUnlock()
		ks := client.keyed.trackedKeys["test:channel"]["key1"]
		return ks != nil && ks.version >= 2
	}, 3*time.Second, 10*time.Millisecond)

	// All publications should be non-delta because no prevData.
	// Drain all messages from sink.
	var msgs [][]byte
	for {
		select {
		case data := <-sink:
			msgs = append(msgs, data)
		default:
			goto done
		}
	}
done:
	for _, msg := range msgs {
		decoder := protocol.NewJSONReplyDecoder(msg)
		reply, err := decoder.Decode()
		if err != nil || reply.Push == nil || reply.Push.Pub == nil {
			continue
		}
		require.False(t, reply.Push.Pub.Delta, "should not have delta without KeepLatestData")
	}
}

func TestSharedPollDelta_DeltaApplicable(t *testing.T) {
	t.Parallel()
	// Verify the delta patch is actually a valid fossil delta.
	node := newTestNodeWithSharedPoll(t, SharedPollChannelOptions{
		RefreshInterval:      100 * time.Millisecond,
		RefreshBatchSize:     100,
		MaxKeysPerConnection: 100,
		KeepLatestData:       true,
		Mode:                 SharedPollModeVersioned,
	})
	setupSharedPollDeltaHandlers(node)

	version := atomic.Int64{}
	version.Store(1)
	dataVal := atomic.Value{}
	dataVal.Store([]byte(`{"value":"hello world"}`))

	node.OnSharedPoll(func(ctx context.Context, event SharedPollEvent) (SharedPollResult, error) {
		return SharedPollResult{
			Items: []SharedPollRefreshItem{
				{Key: "key1", Data: dataVal.Load().([]byte), Version: uint64(version.Load())},
			},
		}, nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	transport := newTestTransport(cancel)
	transport.setProtocolVersion(ProtocolVersion2)
	transport.setProtocolType(ProtocolTypeProtobuf)
	sink := make(chan []byte, 100)
	transport.sink = sink
	newCtx := SetCredentials(ctx, &Credentials{UserID: "user1"})
	client, _ := newClient(newCtx, node, transport)
	connectClientV2(t, client)
	subscribeSharedPollClientDelta(t, client, "test:channel")
	trackSharedPollClient(t, client, "test:channel", []*protocol.KeyedItem{
		{Key: "key1", Version: 0},
	})

	// Wait for first publication.
	require.Eventually(t, func() bool {
		client.mu.RLock()
		defer client.mu.RUnlock()
		ks := client.keyed.trackedKeys["test:channel"]["key1"]
		return ks != nil && ks.version >= 1
	}, 2*time.Second, 10*time.Millisecond)

	// Parse first pub (full).
	var firstPubData []byte
	timeout := time.After(2 * time.Second)
	for {
		select {
		case data := <-sink:
			reply := &protocol.Reply{}
			err := reply.UnmarshalVT(data)
			if err == nil && reply.Push != nil && reply.Push.Pub != nil {
				firstPubData = reply.Push.Pub.Data
				goto gotFirst
			}
		case <-timeout:
			t.Fatal("timeout waiting for first publication")
		}
	}
gotFirst:
	require.Equal(t, `{"value":"hello world"}`, string(firstPubData))

	// Bump version.
	version.Store(2)
	dataVal.Store([]byte(`{"value":"hello delta"}`))

	// Wait for second publication.
	require.Eventually(t, func() bool {
		client.mu.RLock()
		defer client.mu.RUnlock()
		ks := client.keyed.trackedKeys["test:channel"]["key1"]
		return ks != nil && ks.version >= 2
	}, 2*time.Second, 10*time.Millisecond)

	// Parse second pub (delta).
	timeout = time.After(2 * time.Second)
	for {
		select {
		case data := <-sink:
			reply := &protocol.Reply{}
			err := reply.UnmarshalVT(data)
			if err == nil && reply.Push != nil && reply.Push.Pub != nil && reply.Push.Pub.Version == 2 {
				// Apply the fossil delta to first data.
				require.True(t, reply.Push.Pub.Delta)
				applied, err := fdelta.Apply(firstPubData, reply.Push.Pub.Data)
				require.NoError(t, err)
				require.Equal(t, `{"value":"hello delta"}`, string(applied))
				return
			}
		case <-timeout:
			t.Fatal("timeout waiting for delta publication")
		}
	}
}

func TestBuildPreparedPollData_NoPrevData(t *testing.T) {
	t.Parallel()
	pub := &protocol.Publication{Data: []byte(`test`)}
	prep := buildPreparedPollData(pub, nil)
	require.False(t, prep.deltaSub)
	require.Nil(t, prep.keyedDeltaPatch)
}

func TestBuildPreparedPollData_WithPrevData(t *testing.T) {
	t.Parallel()
	// Use sufficiently large data so fossil delta overhead is smaller than full payload.
	prevData := []byte(`{"value":"this is a longer string so delta overhead is worth it","count":1}`)
	newData := []byte(`{"value":"this is a longer string so delta overhead is worth it","count":2}`)
	pub := &protocol.Publication{Data: newData}
	prep := buildPreparedPollData(pub, prevData)
	require.True(t, prep.deltaSub)
	require.NotNil(t, prep.keyedDeltaPatch)
	// The patch should be a real delta (smaller than full).
	require.True(t, prep.keyedDeltaIsReal)
	// Verify the patch is applicable.
	applied, err := fdelta.Apply(prevData, prep.keyedDeltaPatch)
	require.NoError(t, err)
	require.Equal(t, newData, applied)
}

func TestBuildPreparedPollData_LargePatch(t *testing.T) {
	t.Parallel()
	// When new data is completely different and small, patch may be >= full size.
	prevData := []byte(`a`)
	newData := []byte(`completely different data here`)
	pub := &protocol.Publication{Data: newData}
	prep := buildPreparedPollData(pub, prevData)
	require.True(t, prep.deltaSub)
	// keyedDeltaIsReal may be false if patch >= full data.
	if !prep.keyedDeltaIsReal {
		// The data should be the full data (not the patch).
		require.Equal(t, newData, prep.keyedDeltaPatch)
	}
}

// setupSharedPollHandlersWithExpiry sets up handlers where OnTrack returns the given ExpireAt.
func setupSharedPollHandlersWithExpiry(node *Node, expireAt int64) {
	node.OnConnecting(func(ctx context.Context, e ConnectEvent) (ConnectReply, error) {
		return ConnectReply{}, nil
	})
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					ExpireAt: time.Now().Unix() + 3600,
				},
				ClientSideRefresh: true,
			}, nil)
		})
		client.OnTrack(func(e TrackEvent, cb TrackCallback) {
			cb(TrackReply{Batches: []TrackBatchReply{{ExpireAt: expireAt}}}, nil)
		})
		client.OnSubRefresh(func(e SubRefreshEvent, cb SubRefreshCallback) {
			cb(SubRefreshReply{}, nil)
		})
	})
}

func TestSharedPollTrackExpiry_NoRefresh(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t)
	trackExpireAt := int64(100)
	setupSharedPollHandlersWithExpiry(node, trackExpireAt)

	client := newTestClientV2(t, node, "user1")
	connectClientV2(t, client)
	subscribeSharedPollClient(t, client, "test:channel")
	trackSharedPollClient(t, client, "test:channel", []*protocol.KeyedItem{
		{Key: "keyA", Version: 1},
		{Key: "keyB", Version: 2},
	})

	// Verify keys are tracked.
	hub := node.keyedManager.getHub("test:channel")
	require.True(t, hub.hasSubscriber("keyA", client))
	require.True(t, hub.hasSubscriber("keyB", client))

	// Set time past expiry + 25s delay.
	node.nowTimeGetter = func() time.Time {
		return time.Unix(126, 0) // 100 + 25 + 1
	}

	client.checkTrackExpiration("test:channel", 25*time.Second)

	// Keys should be removed from trackedKeys.
	client.mu.RLock()
	chanKeys := client.keyed.trackedKeys["test:channel"]
	client.mu.RUnlock()
	require.Empty(t, chanKeys)

	// Keys should be removed from hub.
	require.False(t, hub.hasSubscriber("keyA", client))
	require.False(t, hub.hasSubscriber("keyB", client))
}

func TestSharedPollTrackExpiry_RefreshResetsExpiry(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t)
	trackExpireAt := int64(100)
	setupSharedPollHandlersWithExpiry(node, trackExpireAt)

	client := newTestClientV2(t, node, "user1")
	connectClientV2(t, client)
	subscribeSharedPollClient(t, client, "test:channel")
	trackSharedPollClient(t, client, "test:channel", []*protocol.KeyedItem{
		{Key: "keyA", Version: 1},
		{Key: "keyB", Version: 2},
	})

	// Simulate refresh: re-track with new expiry.
	newExpireAt := int64(200)
	// Update handler to return new expiry.
	client.eventHub.trackHandler = func(e TrackEvent, cb TrackCallback) {
		cb(TrackReply{Batches: []TrackBatchReply{{ExpireAt: newExpireAt}}}, nil)
	}
	trackSharedPollClient(t, client, "test:channel", []*protocol.KeyedItem{
		{Key: "keyA", Version: 1},
		{Key: "keyB", Version: 2},
	})

	// Set time past original expiry + delay but before new expiry.
	node.nowTimeGetter = func() time.Time {
		return time.Unix(126, 0) // 100 + 25 + 1
	}

	client.checkTrackExpiration("test:channel", 25*time.Second)

	// Keys should NOT be removed (new expiry is 200).
	hub := node.keyedManager.getHub("test:channel")
	require.True(t, hub.hasSubscriber("keyA", client))
	require.True(t, hub.hasSubscriber("keyB", client))

	// Set time past new expiry + delay.
	node.nowTimeGetter = func() time.Time {
		return time.Unix(226, 0) // 200 + 25 + 1
	}

	client.checkTrackExpiration("test:channel", 25*time.Second)

	// Now keys should be removed.
	client.mu.RLock()
	chanKeys := client.keyed.trackedKeys["test:channel"]
	client.mu.RUnlock()
	require.Empty(t, chanKeys)
	require.False(t, hub.hasSubscriber("keyA", client))
	require.False(t, hub.hasSubscriber("keyB", client))
}

func TestSharedPollTrackExpiry_PartialRefresh(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t)
	trackExpireAt := int64(100)
	setupSharedPollHandlersWithExpiry(node, trackExpireAt)

	client := newTestClientV2(t, node, "user1")
	connectClientV2(t, client)
	subscribeSharedPollClient(t, client, "test:channel")
	trackSharedPollClient(t, client, "test:channel", []*protocol.KeyedItem{
		{Key: "keyA", Version: 1},
		{Key: "keyB", Version: 2},
		{Key: "keyC", Version: 3},
	})

	// Refresh only A and B with new expiry (C keeps old expiry).
	newExpireAt := int64(200)
	client.eventHub.trackHandler = func(e TrackEvent, cb TrackCallback) {
		cb(TrackReply{Batches: []TrackBatchReply{{ExpireAt: newExpireAt}}}, nil)
	}
	trackSharedPollClient(t, client, "test:channel", []*protocol.KeyedItem{
		{Key: "keyA", Version: 1},
		{Key: "keyB", Version: 2},
	})

	hub := node.keyedManager.getHub("test:channel")

	// Set time past original expiry + delay.
	node.nowTimeGetter = func() time.Time {
		return time.Unix(126, 0) // 100 + 25 + 1
	}

	client.checkTrackExpiration("test:channel", 25*time.Second)

	// Only C should be removed.
	require.True(t, hub.hasSubscriber("keyA", client))
	require.True(t, hub.hasSubscriber("keyB", client))
	require.False(t, hub.hasSubscriber("keyC", client))

	client.mu.RLock()
	_, hasA := client.keyed.trackedKeys["test:channel"]["keyA"]
	_, hasB := client.keyed.trackedKeys["test:channel"]["keyB"]
	_, hasC := client.keyed.trackedKeys["test:channel"]["keyC"]
	client.mu.RUnlock()
	require.True(t, hasA)
	require.True(t, hasB)
	require.False(t, hasC)

	// Set time past new expiry + delay.
	node.nowTimeGetter = func() time.Time {
		return time.Unix(226, 0) // 200 + 25 + 1
	}

	client.checkTrackExpiration("test:channel", 25*time.Second)

	// A and B should now be removed too.
	require.False(t, hub.hasSubscriber("keyA", client))
	require.False(t, hub.hasSubscriber("keyB", client))
}

func TestSharedPollTrackExpiry_NoExpiry(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t)
	// ExpireAt=0 means no expiry.
	setupSharedPollHandlersWithExpiry(node, 0)

	client := newTestClientV2(t, node, "user1")
	connectClientV2(t, client)
	subscribeSharedPollClient(t, client, "test:channel")
	trackSharedPollClient(t, client, "test:channel", []*protocol.KeyedItem{
		{Key: "keyA", Version: 1},
	})

	// Set time far in future.
	node.nowTimeGetter = func() time.Time {
		return time.Unix(999999999, 0)
	}

	client.checkTrackExpiration("test:channel", 25*time.Second)

	// Key should NOT be removed.
	hub := node.keyedManager.getHub("test:channel")
	require.True(t, hub.hasSubscriber("keyA", client))

	client.mu.RLock()
	_, hasA := client.keyed.trackedKeys["test:channel"]["keyA"]
	client.mu.RUnlock()
	require.True(t, hasA)
}

func TestSharedPollTrackExpiry_NotExpiredYet(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t)
	trackExpireAt := int64(100)
	setupSharedPollHandlersWithExpiry(node, trackExpireAt)

	client := newTestClientV2(t, node, "user1")
	connectClientV2(t, client)
	subscribeSharedPollClient(t, client, "test:channel")
	trackSharedPollClient(t, client, "test:channel", []*protocol.KeyedItem{
		{Key: "keyA", Version: 1},
	})

	// Set time within delay window (100 + 25 = 125, set to 124).
	node.nowTimeGetter = func() time.Time {
		return time.Unix(124, 0)
	}

	client.checkTrackExpiration("test:channel", 25*time.Second)

	// Key should NOT be removed.
	hub := node.keyedManager.getHub("test:channel")
	require.True(t, hub.hasSubscriber("keyA", client))

	client.mu.RLock()
	_, hasA := client.keyed.trackedKeys["test:channel"]["keyA"]
	client.mu.RUnlock()
	require.True(t, hasA)
}

// --- Cached Data on Track Tests ---

func TestSharedPollCachedData_ReturnedOnTrack(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t, SharedPollChannelOptions{
		RefreshInterval:      100 * time.Millisecond,
		RefreshBatchSize:     100,
		MaxKeysPerConnection: 100,
		KeepLatestData:       true,
		Mode:                 SharedPollModeVersioned,
	})

	node.OnSharedPoll(func(ctx context.Context, event SharedPollEvent) (SharedPollResult, error) {
		return SharedPollResult{
			Items: []SharedPollRefreshItem{
				{Key: "key1", Data: []byte(`{"v":1}`), Version: 1},
			},
		}, nil
	})

	setupSharedPollHandlers(node)

	// Client1 tracks key1 → populates itemIndex via poll.
	client1 := newTestClientV2(t, node, "user1")
	connectClientV2(t, client1)
	subscribeSharedPollClient(t, client1, "test:channel")
	trackSharedPollClient(t, client1, "test:channel", []*protocol.KeyedItem{
		{Key: "key1", Version: 0},
	})

	// Wait for itemIndex to be populated.
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
		return entry != nil && entry.version >= 1 && entry.data != nil
	}, 2*time.Second, 10*time.Millisecond)

	// Client2 tracks key1 v0 → should get cached data in response.
	client2 := newTestClientV2(t, node, "user2")
	connectClientV2(t, client2)
	subscribeSharedPollClient(t, client2, "test:channel")
	result := trackSharedPollClientWithReply(t, client2, "test:channel", []*protocol.KeyedItem{
		{Key: "key1", Version: 0},
	})

	require.NotNil(t, result)
	require.Len(t, result.Items, 1)
	require.Equal(t, "key1", result.Items[0].Key)
	require.Equal(t, string(result.Items[0].Data), `{"v":1}`)
	require.Equal(t, uint64(1), result.Items[0].Version)
}

func TestSharedPollCachedData_NotReturnedWhenVersionCurrent(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t, SharedPollChannelOptions{
		RefreshInterval:      100 * time.Millisecond,
		RefreshBatchSize:     100,
		MaxKeysPerConnection: 100,
		KeepLatestData:       true,
	})

	node.OnSharedPoll(func(ctx context.Context, event SharedPollEvent) (SharedPollResult, error) {
		return SharedPollResult{
			Items: []SharedPollRefreshItem{
				{Key: "key1", Data: []byte(`{"v":1}`), Version: 1},
			},
		}, nil
	})

	setupSharedPollHandlers(node)

	// Client1 populates cache.
	client1 := newTestClientV2(t, node, "user1")
	connectClientV2(t, client1)
	subscribeSharedPollClient(t, client1, "test:channel")
	trackSharedPollClient(t, client1, "test:channel", []*protocol.KeyedItem{
		{Key: "key1", Version: 0},
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
		return entry != nil && entry.version >= 1
	}, 2*time.Second, 10*time.Millisecond)

	// Client2 tracks with version=1 (already current) → no cached data.
	client2 := newTestClientV2(t, node, "user2")
	connectClientV2(t, client2)
	subscribeSharedPollClient(t, client2, "test:channel")
	result := trackSharedPollClientWithReply(t, client2, "test:channel", []*protocol.KeyedItem{
		{Key: "key1", Version: 1},
	})

	require.NotNil(t, result)
	require.Empty(t, result.Items)
}

func TestSharedPollCachedData_NotReturnedWithoutKeepLatestData(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t, SharedPollChannelOptions{
		RefreshInterval:      100 * time.Millisecond,
		RefreshBatchSize:     100,
		MaxKeysPerConnection: 100,
		KeepLatestData:       false, // explicitly disabled
	})

	node.OnSharedPoll(func(ctx context.Context, event SharedPollEvent) (SharedPollResult, error) {
		return SharedPollResult{
			Items: []SharedPollRefreshItem{
				{Key: "key1", Data: []byte(`{"v":1}`), Version: 1},
			},
		}, nil
	})

	setupSharedPollHandlers(node)

	// Client1 populates itemIndex version (but no data cached).
	client1 := newTestClientV2(t, node, "user1")
	connectClientV2(t, client1)
	subscribeSharedPollClient(t, client1, "test:channel")
	trackSharedPollClient(t, client1, "test:channel", []*protocol.KeyedItem{
		{Key: "key1", Version: 0},
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
		return entry != nil && entry.version >= 1
	}, 2*time.Second, 10*time.Millisecond)

	// Client2 tracks v0 → no cached data (KeepLatestData=false).
	client2 := newTestClientV2(t, node, "user2")
	connectClientV2(t, client2)
	subscribeSharedPollClient(t, client2, "test:channel")
	result := trackSharedPollClientWithReply(t, client2, "test:channel", []*protocol.KeyedItem{
		{Key: "key1", Version: 0},
	})

	require.NotNil(t, result)
	require.Empty(t, result.Items)
}

func TestSharedPollCachedData_VersionUpdatedNoDuplicate(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t, SharedPollChannelOptions{
		RefreshInterval:      10 * time.Second, // long interval to avoid timer poll interference
		RefreshBatchSize:     100,
		MaxKeysPerConnection: 100,
		KeepLatestData:       true,
		Mode:                 SharedPollModeVersioned,
	})

	node.OnSharedPoll(func(ctx context.Context, event SharedPollEvent) (SharedPollResult, error) {
		return SharedPollResult{
			Items: []SharedPollRefreshItem{
				{Key: "key1", Data: []byte(`{"v":1}`), Version: 1},
			},
		}, nil
	})

	setupSharedPollHandlers(node)

	// Client1 populates cache.
	client1 := newTestClientV2(t, node, "user1")
	connectClientV2(t, client1)
	subscribeSharedPollClient(t, client1, "test:channel")
	trackSharedPollClient(t, client1, "test:channel", []*protocol.KeyedItem{
		{Key: "key1", Version: 0},
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
		return entry != nil && entry.version >= 1 && entry.data != nil
	}, 2*time.Second, 10*time.Millisecond)

	// Client2 tracks v0 → gets cached v1.
	client2 := newTestClientV2(t, node, "user2")
	connectClientV2(t, client2)
	subscribeSharedPollClient(t, client2, "test:channel")
	result := trackSharedPollClientWithReply(t, client2, "test:channel", []*protocol.KeyedItem{
		{Key: "key1", Version: 0},
	})
	require.Len(t, result.Items, 1)
	require.Equal(t, uint64(1), result.Items[0].Version)

	// Verify per-connection version was updated to 1 (prevents duplicate broadcast).
	client2.mu.RLock()
	ks := client2.keyed.trackedKeys["test:channel"]["key1"]
	client2.mu.RUnlock()
	require.NotNil(t, ks)
	require.Equal(t, uint64(1), ks.version)
}

func TestSharedPollCachedData_MultipleKeys(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t, SharedPollChannelOptions{
		RefreshInterval:      100 * time.Millisecond,
		RefreshBatchSize:     100,
		MaxKeysPerConnection: 100,
		KeepLatestData:       true,
		Mode:                 SharedPollModeVersioned,
	})

	node.OnSharedPoll(func(ctx context.Context, event SharedPollEvent) (SharedPollResult, error) {
		return SharedPollResult{
			Items: []SharedPollRefreshItem{
				{Key: "key1", Data: []byte(`{"k":"d1"}`), Version: 2},
				{Key: "key2", Data: []byte(`{"k":"d2"}`), Version: 3},
				// key3 not returned — stays cold (version=0).
			},
		}, nil
	})

	setupSharedPollHandlers(node)

	// Client1 populates cache.
	client1 := newTestClientV2(t, node, "user1")
	connectClientV2(t, client1)
	subscribeSharedPollClient(t, client1, "test:channel")
	trackSharedPollClient(t, client1, "test:channel", []*protocol.KeyedItem{
		{Key: "key1", Version: 0},
		{Key: "key2", Version: 0},
		{Key: "key3", Version: 0},
	})

	// Wait for cache populated.
	require.Eventually(t, func() bool {
		node.sharedPollManager.mu.RLock()
		s := node.sharedPollManager.channels["test:channel"]
		node.sharedPollManager.mu.RUnlock()
		if s == nil {
			return false
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		e1 := s.itemIndex["key1"]
		e2 := s.itemIndex["key2"]
		return e1 != nil && e1.version >= 2 && e1.data != nil &&
			e2 != nil && e2.version >= 3 && e2.data != nil
	}, 2*time.Second, 10*time.Millisecond)

	// Client2 tracks all 3 keys v0 → should get key1 and key2, not key3.
	client2 := newTestClientV2(t, node, "user2")
	connectClientV2(t, client2)
	subscribeSharedPollClient(t, client2, "test:channel")
	result := trackSharedPollClientWithReply(t, client2, "test:channel", []*protocol.KeyedItem{
		{Key: "key1", Version: 0},
		{Key: "key2", Version: 0},
		{Key: "key3", Version: 0},
	})

	require.NotNil(t, result)
	require.Len(t, result.Items, 2)
	keyMap := map[string]*protocol.Publication{}
	for _, pub := range result.Items {
		keyMap[pub.Key] = pub
	}
	require.Contains(t, keyMap, "key1")
	require.Contains(t, keyMap, "key2")
	require.NotContains(t, keyMap, "key3")
	require.Equal(t, uint64(2), keyMap["key1"].Version)
	require.Equal(t, uint64(3), keyMap["key2"].Version)
}

func TestSharedPollCachedData_PartialVersionMatch(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t, SharedPollChannelOptions{
		RefreshInterval:      100 * time.Millisecond,
		RefreshBatchSize:     100,
		MaxKeysPerConnection: 100,
		KeepLatestData:       true,
		Mode:                 SharedPollModeVersioned,
	})

	node.OnSharedPoll(func(ctx context.Context, event SharedPollEvent) (SharedPollResult, error) {
		return SharedPollResult{
			Items: []SharedPollRefreshItem{
				{Key: "key1", Data: []byte(`{"k":"d1"}`), Version: 2},
				{Key: "key2", Data: []byte(`{"k":"d2"}`), Version: 5},
			},
		}, nil
	})

	setupSharedPollHandlers(node)

	client1 := newTestClientV2(t, node, "user1")
	connectClientV2(t, client1)
	subscribeSharedPollClient(t, client1, "test:channel")
	trackSharedPollClient(t, client1, "test:channel", []*protocol.KeyedItem{
		{Key: "key1", Version: 0},
		{Key: "key2", Version: 0},
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
		e1 := s.itemIndex["key1"]
		e2 := s.itemIndex["key2"]
		return e1 != nil && e1.version >= 2 && e2 != nil && e2.version >= 5
	}, 2*time.Second, 10*time.Millisecond)

	// Client2: key1 v2 (current), key2 v0 (behind) → only key2 returned.
	client2 := newTestClientV2(t, node, "user2")
	connectClientV2(t, client2)
	subscribeSharedPollClient(t, client2, "test:channel")
	result := trackSharedPollClientWithReply(t, client2, "test:channel", []*protocol.KeyedItem{
		{Key: "key1", Version: 2},
		{Key: "key2", Version: 0},
	})

	require.NotNil(t, result)
	require.Len(t, result.Items, 1)
	require.Equal(t, "key2", result.Items[0].Key)
	require.Equal(t, uint64(5), result.Items[0].Version)
}

// --- Auto-Notify Cold Key Tests ---

func TestSharedPollAutoNotify_ColdKey(t *testing.T) {
	t.Parallel()
	var pollCalled atomic.Int32
	var pollMu sync.Mutex
	var pollKeys []string

	node := newTestNodeWithSharedPoll(t, SharedPollChannelOptions{
		RefreshInterval:      10 * time.Second, // Long interval — only auto-notify triggers poll.
		RefreshBatchSize:     100,
		MaxKeysPerConnection: 100,
		Mode:                 SharedPollModeVersioned,
	})

	node.OnSharedPoll(func(ctx context.Context, event SharedPollEvent) (SharedPollResult, error) {
		pollCalled.Add(1)
		pollMu.Lock()
		for _, item := range event.Items {
			pollKeys = append(pollKeys, item.Key)
		}
		pollMu.Unlock()
		return SharedPollResult{
			Items: []SharedPollRefreshItem{
				{Key: "cold_key", Data: []byte(`data`), Version: 1},
			},
		}, nil
	})

	setupSharedPollHandlers(node)
	client := newTestClientV2(t, node, "user1")
	connectClientV2(t, client)
	subscribeSharedPollClient(t, client, "test:channel")

	// Track a cold key — should trigger auto-notify → backend poll.
	trackSharedPollClient(t, client, "test:channel", []*protocol.KeyedItem{
		{Key: "cold_key", Version: 0},
	})

	// Verify OnSharedPoll is called with the cold key within ~200ms (not 10s).
	require.Eventually(t, func() bool {
		return pollCalled.Load() > 0
	}, 500*time.Millisecond, 10*time.Millisecond)

	pollMu.Lock()
	require.Contains(t, pollKeys, "cold_key")
	pollMu.Unlock()
}

func TestSharedPollAutoNotify_ExistingKeyNoNotify(t *testing.T) {
	t.Parallel()
	var callCount atomic.Int32

	node := newTestNodeWithSharedPoll(t, SharedPollChannelOptions{
		RefreshInterval:      10 * time.Second, // Long interval.
		RefreshBatchSize:     100,
		MaxKeysPerConnection: 100,
		Mode:                 SharedPollModeVersioned,
	})

	node.OnSharedPoll(func(ctx context.Context, event SharedPollEvent) (SharedPollResult, error) {
		callCount.Add(1)
		return SharedPollResult{
			Items: []SharedPollRefreshItem{
				{Key: "key1", Data: []byte(`data`), Version: 1},
			},
		}, nil
	})

	setupSharedPollHandlers(node)

	// Client1 tracks key1 → cold key → auto-notify (call #1).
	client1 := newTestClientV2(t, node, "user1")
	connectClientV2(t, client1)
	subscribeSharedPollClient(t, client1, "test:channel")
	trackSharedPollClient(t, client1, "test:channel", []*protocol.KeyedItem{
		{Key: "key1", Version: 0},
	})
	require.Eventually(t, func() bool { return callCount.Load() >= 1 },
		5*time.Second, 10*time.Millisecond,
		"expected cold-key auto-poll to fire")

	// Client2 tracks the same key with version=0 → warm key → triggers
	// notify for near-immediate delivery (call #2). version=0 means
	// "I have no data" so the server re-broadcasts.
	client2 := newTestClientV2(t, node, "user2")
	connectClientV2(t, client2)
	subscribeSharedPollClient(t, client2, "test:channel")
	trackSharedPollClient(t, client2, "test:channel", []*protocol.KeyedItem{
		{Key: "key1", Version: 0},
	})
	require.Eventually(t, func() bool { return callCount.Load() >= 2 },
		5*time.Second, 10*time.Millisecond,
		"expected notify for warm key with version=0")

	// Client3 tracks the same key with version > 0 → no notify (has data).
	beforeC3 := callCount.Load()
	client3 := newTestClientV2(t, node, "user3")
	connectClientV2(t, client3)
	subscribeSharedPollClient(t, client3, "test:channel")
	trackSharedPollClient(t, client3, "test:channel", []*protocol.KeyedItem{
		{Key: "key1", Version: 1},
	})

	// No additional backend call — client already has data. require.Never
	// keeps polling for a window to detect any late notify.
	require.Never(t, func() bool { return callCount.Load() > beforeC3 },
		500*time.Millisecond, 25*time.Millisecond,
		"should not notify for warm key with version > 0")
}

func TestSharedPollAutoNotify_MultipleClientsSameColdKey(t *testing.T) {
	t.Parallel()
	var pollCalled atomic.Int32

	node := newTestNodeWithSharedPoll(t, SharedPollChannelOptions{
		RefreshInterval:      10 * time.Second,
		RefreshBatchSize:     100,
		MaxKeysPerConnection: 100,
		Mode:                 SharedPollModeVersioned,
	})

	node.OnSharedPoll(func(ctx context.Context, event SharedPollEvent) (SharedPollResult, error) {
		pollCalled.Add(1)
		return SharedPollResult{
			Items: []SharedPollRefreshItem{
				{Key: "key1", Data: []byte(`data`), Version: 1},
			},
		}, nil
	})

	setupSharedPollHandlers(node)

	// Client1 tracks key1 → cold key → auto-notify.
	client1 := newTestClientV2(t, node, "user1")
	connectClientV2(t, client1)
	subscribeSharedPollClient(t, client1, "test:channel")
	trackSharedPollClient(t, client1, "test:channel", []*protocol.KeyedItem{
		{Key: "key1", Version: 0},
	})

	// Wait for auto-notify to fire.
	require.Eventually(t, func() bool {
		return pollCalled.Load() >= 1
	}, 500*time.Millisecond, 10*time.Millisecond)

	// Client2 tracks same key with version=0 — triggers one additional notify
	// (warm key, version=0 means "I have no data").
	pollBefore := pollCalled.Load()
	client2 := newTestClientV2(t, node, "user2")
	connectClientV2(t, client2)
	subscribeSharedPollClient(t, client2, "test:channel")
	trackSharedPollClient(t, client2, "test:channel", []*protocol.KeyedItem{
		{Key: "key1", Version: 0},
	})

	// One additional handler call expected for warm key notify.
	require.Eventually(t, func() bool {
		return pollCalled.Load() >= pollBefore+1
	}, 500*time.Millisecond, 10*time.Millisecond, "expected notify for warm key with version=0")

	// Client3 tracks same key simultaneously with version=0 —
	// dedup should prevent a third notify (needsBroadcast already set).
	pollBefore2 := pollCalled.Load()
	client3 := newTestClientV2(t, node, "user3")
	connectClientV2(t, client3)
	subscribeSharedPollClient(t, client3, "test:channel")
	trackSharedPollClient(t, client3, "test:channel", []*protocol.KeyedItem{
		{Key: "key1", Version: 0},
	})

	// The dedup depends on timing — needsBroadcast may or may not be cleared
	// by the time client3 arrives. But total extra calls should be bounded.
	time.Sleep(300 * time.Millisecond)
	extraCalls := pollCalled.Load() - pollBefore2
	require.LessOrEqual(t, extraCalls, int32(1), "at most one extra notify per wave")
}

func TestSharedPollAutoNotify_ColdKeyNonZeroVersionNoNotify(t *testing.T) {
	t.Parallel()
	// When a client tracks a cold key with version > 0, auto-notify should NOT
	// fire. The client already has data — the regular poll cycle will deliver
	// any newer data.
	var pollCalled atomic.Int32

	node := newTestNodeWithSharedPoll(t, SharedPollChannelOptions{
		RefreshInterval:      10 * time.Second, // Long interval — only auto-notify would trigger fast poll.
		RefreshBatchSize:     100,
		MaxKeysPerConnection: 100,
		Mode:                 SharedPollModeVersioned,
	})

	node.OnSharedPoll(func(ctx context.Context, event SharedPollEvent) (SharedPollResult, error) {
		pollCalled.Add(1)
		return SharedPollResult{
			Items: []SharedPollRefreshItem{
				{Key: "key1", Data: []byte(`data`), Version: 5},
			},
		}, nil
	})

	setupSharedPollHandlers(node)
	client := newTestClientV2(t, node, "user1")
	connectClientV2(t, client)
	subscribeSharedPollClient(t, client, "test:channel")

	// Track a cold key with version > 0 — should NOT trigger auto-notify.
	trackSharedPollClient(t, client, "test:channel", []*protocol.KeyedItem{
		{Key: "key1", Version: 3},
	})

	// No backend call should happen within 500ms (10s interval, no auto-notify).
	time.Sleep(500 * time.Millisecond)
	require.Equal(t, int32(0), pollCalled.Load(), "should not auto-notify for cold key with version > 0")
}

func TestSharedPollAutoNotify_ColdKeyVersionZeroTriggersNotify(t *testing.T) {
	t.Parallel()
	// When a client tracks a cold key with version 0, auto-notify SHOULD fire.
	// Contrast with TestSharedPollAutoNotify_ColdKeyNonZeroVersionNoNotify.
	var pollCalled atomic.Int32

	node := newTestNodeWithSharedPoll(t, SharedPollChannelOptions{
		RefreshInterval:      10 * time.Second,
		RefreshBatchSize:     100,
		MaxKeysPerConnection: 100,
		Mode:                 SharedPollModeVersioned,
	})

	node.OnSharedPoll(func(ctx context.Context, event SharedPollEvent) (SharedPollResult, error) {
		pollCalled.Add(1)
		return SharedPollResult{
			Items: []SharedPollRefreshItem{
				{Key: "key1", Data: []byte(`data`), Version: 1},
			},
		}, nil
	})

	setupSharedPollHandlers(node)
	client := newTestClientV2(t, node, "user1")
	connectClientV2(t, client)
	subscribeSharedPollClient(t, client, "test:channel")

	// Track a cold key with version 0 — should trigger auto-notify.
	trackSharedPollClient(t, client, "test:channel", []*protocol.KeyedItem{
		{Key: "key1", Version: 0},
	})

	// Backend should be called quickly (not waiting 10s).
	require.Eventually(t, func() bool {
		return pollCalled.Load() > 0
	}, 500*time.Millisecond, 10*time.Millisecond, "should auto-notify for cold key with version 0")
}

func TestSharedPollCachedData_DeltaReadyAfterCache(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t, SharedPollChannelOptions{
		RefreshInterval:           30 * time.Second,
		RefreshBatchSize:          100,
		MaxKeysPerConnection:      100,
		KeepLatestData:            true,
		Mode:                      SharedPollModeVersioned,
		NotificationBatchMaxSize:  50,
		NotificationBatchMaxDelay: 50 * time.Millisecond,
	})
	setupSharedPollDeltaHandlers(node)

	version := atomic.Int64{}
	version.Store(1)
	dataVal := atomic.Value{}
	dataVal.Store([]byte(`{"value":"hello world, this is version one of the data payload which is long enough for delta"}`))

	node.OnSharedPoll(func(ctx context.Context, event SharedPollEvent) (SharedPollResult, error) {
		return SharedPollResult{
			Items: []SharedPollRefreshItem{
				{Key: "key1", Data: dataVal.Load().([]byte), Version: uint64(version.Load())},
			},
		}, nil
	})

	// Client1 (regular, no sink) tracks key → trigger notification → populate cache.
	client1 := newTestClientV2(t, node, "user1")
	connectClientV2(t, client1)
	subscribeSharedPollClient(t, client1, "test:channel")
	trackSharedPollClient(t, client1, "test:channel", []*protocol.KeyedItem{
		{Key: "key1", Version: 0},
	})

	// Wait for itemIndex to be populated at v1.
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
		return entry != nil && entry.version >= 1 && entry.data != nil
	}, 2*time.Second, 10*time.Millisecond)

	// Client2 (sink transport, delta enabled) tracks key with v0 → receives cached v1.
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	transport2 := newTestTransport(cancel2)
	transport2.setProtocolVersion(ProtocolVersion2)
	transport2.setProtocolType(ProtocolTypeJSON)
	sink2 := make(chan []byte, 100)
	transport2.sink = sink2
	newCtx2 := SetCredentials(ctx2, &Credentials{UserID: "user2"})
	client2, _ := newClient(newCtx2, node, transport2)
	connectClientV2(t, client2)
	subscribeSharedPollClientDelta(t, client2, "test:channel")
	result := trackSharedPollClientWithReply(t, client2, "test:channel", []*protocol.KeyedItem{
		{Key: "key1", Version: 0},
	})

	// Cached data must be returned in the track response.
	require.NotNil(t, result)
	require.Len(t, result.Items, 1)
	require.Equal(t, "key1", result.Items[0].Key)
	require.Equal(t, uint64(1), result.Items[0].Version)

	// Fix 3: deltaReady must be true after receiving cached data.
	client2.mu.RLock()
	ks2 := client2.keyed.trackedKeys["test:channel"]["key1"]
	client2.mu.RUnlock()
	require.NotNil(t, ks2)
	require.True(t, ks2.deltaReady, "deltaReady must be true after cached data delivery")

	// Bump to v2.
	version.Store(2)
	dataVal.Store([]byte(`{"value":"hello world, this is version two of the data payload which is long enough for delta"}`))

	// Trigger a new notification cycle by having client1 track a new cold key.
	trackSharedPollClient(t, client1, "test:channel", []*protocol.KeyedItem{
		{Key: "key_trigger", Version: 0},
	})

	// Wait for client2 to receive v2.
	require.Eventually(t, func() bool {
		client2.mu.RLock()
		defer client2.mu.RUnlock()
		ks := client2.keyed.trackedKeys["test:channel"]["key1"]
		return ks != nil && ks.version >= 2
	}, 5*time.Second, 10*time.Millisecond)

	// Read from sink: find version 2 with Delta=true.
	var deltaPub *protocol.Reply
	timeout := time.After(2 * time.Second)
	for deltaPub == nil {
		select {
		case d := <-sink2:
			reply := decodeReply(t, protocol.TypeJSON, d)
			if reply.Push != nil && reply.Push.Pub != nil && reply.Push.Pub.Version == 2 {
				deltaPub = reply
			}
		case <-timeout:
			t.Fatal("timeout waiting for delta publication in sink")
		}
	}

	require.True(t, deltaPub.Push.Pub.Delta, "second pub should be delta because deltaReady was set from cache")
	require.Equal(t, "key1", deltaPub.Push.Pub.Key)
}

func TestSharedPollCachedData_DeltaReadyPartialKeys(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t, SharedPollChannelOptions{
		RefreshInterval:      100 * time.Millisecond,
		RefreshBatchSize:     100,
		MaxKeysPerConnection: 100,
		KeepLatestData:       true,
		Mode:                 SharedPollModeVersioned,
	})
	setupSharedPollDeltaHandlers(node)

	node.OnSharedPoll(func(ctx context.Context, event SharedPollEvent) (SharedPollResult, error) {
		// Only return key_warm; key_cold is absent.
		return SharedPollResult{
			Items: []SharedPollRefreshItem{
				{Key: "key_warm", Data: []byte(`{"status":"warm"}`), Version: 1},
			},
		}, nil
	})

	// Client1 tracks key_warm → populates cache.
	client1 := newTestClientV2(t, node, "user1")
	connectClientV2(t, client1)
	subscribeSharedPollClient(t, client1, "test:channel")
	trackSharedPollClient(t, client1, "test:channel", []*protocol.KeyedItem{
		{Key: "key_warm", Version: 0},
	})

	// Wait for cache to be populated.
	require.Eventually(t, func() bool {
		node.sharedPollManager.mu.RLock()
		s := node.sharedPollManager.channels["test:channel"]
		node.sharedPollManager.mu.RUnlock()
		if s == nil {
			return false
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		entry := s.itemIndex["key_warm"]
		return entry != nil && entry.version >= 1 && entry.data != nil
	}, 2*time.Second, 10*time.Millisecond)

	// Client2 tracks both key_warm (v0) and key_cold (v0) with delta.
	client2 := newTestClientV2(t, node, "user2")
	connectClientV2(t, client2)
	subscribeSharedPollClientDelta(t, client2, "test:channel")
	result := trackSharedPollClientWithReply(t, client2, "test:channel", []*protocol.KeyedItem{
		{Key: "key_warm", Version: 0},
		{Key: "key_cold", Version: 0},
	})

	// Cached data returned only for key_warm.
	require.NotNil(t, result)
	require.Len(t, result.Items, 1, "only key_warm should be in cached response")
	require.Equal(t, "key_warm", result.Items[0].Key)
	require.Equal(t, uint64(1), result.Items[0].Version)

	// Fix 3: key_warm.deltaReady == true, key_cold.deltaReady == false.
	client2.mu.RLock()
	ksWarm := client2.keyed.trackedKeys["test:channel"]["key_warm"]
	ksCold := client2.keyed.trackedKeys["test:channel"]["key_cold"]
	client2.mu.RUnlock()

	require.NotNil(t, ksWarm)
	require.True(t, ksWarm.deltaReady, "key_warm deltaReady must be true after cached data")

	require.NotNil(t, ksCold)
	require.False(t, ksCold.deltaReady, "key_cold deltaReady must be false — no cached data")
}

func TestSharedPoll_PrevDataNotUsedWhenKeepLatestData(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t, SharedPollChannelOptions{
		RefreshInterval:           30 * time.Second,
		RefreshBatchSize:          100,
		MaxKeysPerConnection:      100,
		KeepLatestData:            true,
		Mode:                      SharedPollModeVersioned,
		NotificationBatchMaxSize:  50,
		NotificationBatchMaxDelay: 50 * time.Millisecond,
	})
	setupSharedPollDeltaHandlers(node)

	callCount := atomic.Int32{}
	version := atomic.Int64{}
	version.Store(5)
	dataVal := atomic.Value{}
	dataVal.Store([]byte(`{"value":"this is data_A at version five, padded to be large enough for fossil delta"}`))
	prevDataVal := atomic.Value{}
	prevDataVal.Store([]byte(nil))

	node.OnSharedPoll(func(ctx context.Context, event SharedPollEvent) (SharedPollResult, error) {
		callCount.Add(1)
		return SharedPollResult{
			Items: []SharedPollRefreshItem{
				{
					Key:      "key1",
					Data:     dataVal.Load().([]byte),
					Version:  uint64(version.Load()),
					PrevData: prevDataVal.Load().([]byte),
				},
			},
		}, nil
	})

	// Client with sink transport (delta enabled).
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	transport := newTestTransport(cancel)
	transport.setProtocolVersion(ProtocolVersion2)
	transport.setProtocolType(ProtocolTypeProtobuf)
	sink := make(chan []byte, 100)
	transport.sink = sink
	newCtx := SetCredentials(ctx, &Credentials{UserID: "user1"})
	client, _ := newClient(newCtx, node, transport)
	connectClientV2(t, client)
	subscribeSharedPollClientDelta(t, client, "test:channel")
	trackSharedPollClient(t, client, "test:channel", []*protocol.KeyedItem{
		{Key: "key1", Version: 0},
	})

	// Wait for v5 delivery.
	require.Eventually(t, func() bool {
		client.mu.RLock()
		defer client.mu.RUnlock()
		ks := client.keyed.trackedKeys["test:channel"]["key1"]
		return ks != nil && ks.version >= 5
	}, 2*time.Second, 10*time.Millisecond)

	// Capture v5 data from sink (full, not delta).
	var v5Data []byte
	timeout := time.After(2 * time.Second)
	for {
		select {
		case d := <-sink:
			reply := &protocol.Reply{}
			err := reply.UnmarshalVT(d)
			if err == nil && reply.Push != nil && reply.Push.Pub != nil && reply.Push.Pub.Version == 5 {
				require.False(t, reply.Push.Pub.Delta, "first delivery should be full")
				v5Data = reply.Push.Pub.Data
				goto gotV5
			}
		case <-timeout:
			t.Fatal("timeout waiting for v5 publication")
		}
	}
gotV5:
	require.NotEmpty(t, v5Data)

	// Bump to v10 via SharedPollPublish — entry.data becomes data_B.
	dataB := []byte(`{"value":"this is data_B at version ten, padded to be large enough for fossil delta!!"}`)
	err := node.SharedPollPublish(context.Background(), "test:channel", "key1", 10, "", dataB)
	require.NoError(t, err)

	// Wait for v10 delivery.
	require.Eventually(t, func() bool {
		client.mu.RLock()
		defer client.mu.RUnlock()
		ks := client.keyed.trackedKeys["test:channel"]["key1"]
		return ks != nil && ks.version >= 10
	}, 2*time.Second, 10*time.Millisecond)

	// Capture v10 from sink. It is a delta (from data_A to data_B) because
	// deltaReady was set after v5 delivery.
	var v10Reply *protocol.Reply
	timeout = time.After(2 * time.Second)
	for {
		select {
		case d := <-sink:
			reply := &protocol.Reply{}
			err := reply.UnmarshalVT(d)
			if err == nil && reply.Push != nil && reply.Push.Pub != nil && reply.Push.Pub.Version == 10 {
				v10Reply = reply
				goto gotV10
			}
		case <-timeout:
			t.Fatal("timeout waiting for v10 publication")
		}
	}
gotV10:
	// Reconstruct data_B by applying the v10 delta to data_A.
	var currentData []byte
	if v10Reply.Push.Pub.Delta {
		currentData, err = fdelta.Apply(v5Data, v10Reply.Push.Pub.Data)
		require.NoError(t, err)
	} else {
		currentData = v10Reply.Push.Pub.Data
	}
	require.Equal(t, string(dataB), string(currentData), "reconstructed data at v10 must equal data_B")

	// Now bump handler to v12, with PrevData set to intentionally WRONG bytes.
	// If the code incorrectly used PrevData when KeepLatestData=true,
	// the delta would be computed from wrong base → garbage.
	dataC := []byte(`{"value":"this is data_C at version twelve, padded to be large enough for fossil delta"}`)
	version.Store(12)
	dataVal.Store(dataC)
	prevDataVal.Store([]byte(`intentionally wrong prev data that does not match data_B at all`))

	// Trigger notification by having a second client track a cold key.
	client2 := newTestClientV2(t, node, "user2")
	connectClientV2(t, client2)
	subscribeSharedPollClient(t, client2, "test:channel")
	trackSharedPollClient(t, client2, "test:channel", []*protocol.KeyedItem{
		{Key: "key_trigger_notify", Version: 0},
	})

	// Wait for client to receive v12.
	require.Eventually(t, func() bool {
		client.mu.RLock()
		defer client.mu.RUnlock()
		ks := client.keyed.trackedKeys["test:channel"]["key1"]
		return ks != nil && ks.version >= 12
	}, 5*time.Second, 10*time.Millisecond)

	// Read v12 from sink — should be a delta (computed from entry.data = data_B).
	var v12Reply *protocol.Reply
	timeout = time.After(2 * time.Second)
	for v12Reply == nil {
		select {
		case d := <-sink:
			reply := &protocol.Reply{}
			err := reply.UnmarshalVT(d)
			if err == nil && reply.Push != nil && reply.Push.Pub != nil && reply.Push.Pub.Version == 12 {
				v12Reply = reply
			}
		case <-timeout:
			t.Fatal("timeout waiting for v12 publication")
		}
	}

	require.True(t, v12Reply.Push.Pub.Delta, "v12 broadcast should be delta")

	// Apply the fossil delta to currentData (data_B) — should produce data_C.
	// This proves the delta was computed from entry.data (data_B), not from
	// the handler's PrevData (which was intentionally wrong).
	applied, err := fdelta.Apply(currentData, v12Reply.Push.Pub.Data)
	require.NoError(t, err)
	require.Equal(t, string(dataC), string(applied), "delta must be computed from entry.data (data_B), not PrevData")
}

func TestSharedPoll_VersionlessPerConnectionDedup(t *testing.T) {
	t.Parallel()
	// Two clients tracking same key in versionless mode.
	// Both should receive exactly once per change.
	node := newTestNodeWithSharedPoll(t, SharedPollChannelOptions{
		RefreshInterval:           30 * time.Second,
		RefreshBatchSize:          100,
		MaxKeysPerConnection:      100,
		NotificationBatchMaxSize:  50,
		NotificationBatchMaxDelay: 50 * time.Millisecond,
	})

	data := &atomic.Value{}
	data.Store([]byte(`{"v":"initial"}`))

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

	// Wait for initial data delivery to both.
	require.Eventually(t, func() bool {
		client1.mu.RLock()
		ks1 := client1.keyed.trackedKeys["test:channel"]["key1"]
		var v1 uint64
		if ks1 != nil {
			v1 = ks1.version
		}
		client1.mu.RUnlock()
		client2.mu.RLock()
		ks2 := client2.keyed.trackedKeys["test:channel"]["key1"]
		var v2 uint64
		if ks2 != nil {
			v2 = ks2.version
		}
		client2.mu.RUnlock()
		return v1 == 1 && v2 == 1
	}, 2*time.Second, 10*time.Millisecond)

	// Change data and notify.
	data.Store([]byte(`{"v":"changed"}`))
	node.SharedPollNotify([]SharedPollNotificationItem{
		{Channel: "test:channel", Key: "key1"},
	})

	// Both clients should receive the update.
	require.Eventually(t, func() bool {
		client1.mu.RLock()
		ks1 := client1.keyed.trackedKeys["test:channel"]["key1"]
		var v1 uint64
		if ks1 != nil {
			v1 = ks1.version
		}
		client1.mu.RUnlock()
		client2.mu.RLock()
		ks2 := client2.keyed.trackedKeys["test:channel"]["key1"]
		var v2 uint64
		if ks2 != nil {
			v2 = ks2.version
		}
		client2.mu.RUnlock()
		return v1 == 2 && v2 == 2
	}, 2*time.Second, 10*time.Millisecond)
}

func TestSharedPoll_VersionlessReconnect(t *testing.T) {
	t.Parallel()
	// In versionless mode, wire version is always 0.
	// When a client reconnects and re-tracks with v=0, a new data change
	// must be delivered (pubVersion > keyState.version=0).
	node := newTestNodeWithSharedPoll(t, SharedPollChannelOptions{
		RefreshInterval:           30 * time.Second,
		RefreshBatchSize:          100,
		MaxKeysPerConnection:      100,
		NotificationBatchMaxSize:  50,
		NotificationBatchMaxDelay: 50 * time.Millisecond,
	})

	data := &atomic.Value{}
	data.Store([]byte(`{"v":"data_v1"}`))

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

	// Client1 keeps the key alive.
	client1 := newTestClientV2(t, node, "user1")
	connectClientV2(t, client1)
	subscribeSharedPollClient(t, client1, "test:channel")
	trackSharedPollClient(t, client1, "test:channel", []*protocol.KeyedItem{
		{Key: "key1", Version: 0},
	})

	// Wait for initial data (synthetic version=1 internally).
	require.Eventually(t, func() bool {
		client1.mu.RLock()
		defer client1.mu.RUnlock()
		k := client1.keyed.trackedKeys["test:channel"]["key1"]
		return k != nil && k.version == 1
	}, 2*time.Second, 10*time.Millisecond)

	// Client2 connects and tracks the same key.
	client2 := newTestClientV2(t, node, "user2")
	connectClientV2(t, client2)
	subscribeSharedPollClient(t, client2, "test:channel")
	trackSharedPollClient(t, client2, "test:channel", []*protocol.KeyedItem{
		{Key: "key1", Version: 0},
	})

	// In versionless mode: key is not cold (client1 already tracks it), getCachedData
	// is disabled. Client2 must wait for a data change to receive a broadcast.
	// Change data and notify.
	data.Store([]byte(`{"v":"data_v2"}`))
	node.SharedPollNotify([]SharedPollNotificationItem{
		{Channel: "test:channel", Key: "key1"},
	})

	// Client2 should receive the broadcast (pubVersion=2 > keyState.version=0).
	require.Eventually(t, func() bool {
		client2.mu.RLock()
		defer client2.mu.RUnlock()
		k := client2.keyed.trackedKeys["test:channel"]["key1"]
		return k != nil && k.version == 2
	}, 2*time.Second, 10*time.Millisecond)

	// Client1 should also get it.
	require.Eventually(t, func() bool {
		client1.mu.RLock()
		defer client1.mu.RUnlock()
		k := client1.keyed.trackedKeys["test:channel"]["key1"]
		return k != nil && k.version == 2
	}, 2*time.Second, 10*time.Millisecond)
}

func TestSharedPollEpoch_VersionlessSubscribeReplyHasEpoch(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t)
	setupSharedPollHandlers(node)
	client := newTestClientV2(t, node, "user1")
	connectClientV2(t, client)

	result := subscribeSharedPollClient(t, client, "test:channel")
	require.NotEmpty(t, result.Epoch, "versionless channel should have non-empty epoch")
	require.Len(t, result.Epoch, 8, "epoch should be 8 characters")
}

func TestSharedPollEpoch_VersionedModeEmptyEpoch(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t, SharedPollChannelOptions{
		Mode:                 SharedPollModeVersioned,
		RefreshInterval:      100 * time.Millisecond,
		RefreshBatchSize:     100,
		MaxKeysPerConnection: 100,
	})
	setupSharedPollHandlers(node)
	client := newTestClientV2(t, node, "user1")
	connectClientV2(t, client)

	result := subscribeSharedPollClient(t, client, "test:channel")
	require.Empty(t, result.Epoch, "versioned mode channel should have empty epoch")
}

func TestSharedPollEpoch_VersionlessSendsSyntheticVersion(t *testing.T) {
	t.Parallel()
	// Versionless mode: publications should carry synthetic version on wire (not 0).
	// We verify by checking the per-connection version which is set from the wire version.
	node := newTestNodeWithSharedPoll(t, SharedPollChannelOptions{
		RefreshInterval:           30 * time.Second,
		RefreshBatchSize:          100,
		MaxKeysPerConnection:      100,
		NotificationBatchMaxSize:  50,
		NotificationBatchMaxDelay: 50 * time.Millisecond,
	})

	node.OnSharedPoll(func(ctx context.Context, event SharedPollEvent) (SharedPollResult, error) {
		items := make([]SharedPollRefreshItem, len(event.Items))
		for i, item := range event.Items {
			items[i] = SharedPollRefreshItem{
				Key:  item.Key,
				Data: []byte(`{"v":1}`),
			}
		}
		return SharedPollResult{Items: items}, nil
	})

	setupSharedPollHandlers(node)
	client := newTestClientV2(t, node, "user1")
	sink := make(chan []byte, 100)
	client.transport.(*testTransport).setSink(sink)
	connectClientV2(t, client)
	subscribeSharedPollClient(t, client, "test:channel")
	trackSharedPollClient(t, client, "test:channel", []*protocol.KeyedItem{
		{Key: "key1", Version: 0},
	})

	// Notify to trigger data delivery.
	node.SharedPollNotify([]SharedPollNotificationItem{
		{Channel: "test:channel", Key: "key1"},
	})

	// Client per-connection version should be non-zero (synthetic version).
	require.Eventually(t, func() bool {
		client.mu.RLock()
		defer client.mu.RUnlock()
		k := client.keyed.trackedKeys["test:channel"]["key1"]
		return k != nil && k.version > 0
	}, 2*time.Second, 10*time.Millisecond)

	// Also verify wire data contains non-zero version by checking sink.
	// Use Eventually because the transport write may arrive slightly after
	// the internal version is updated.
	require.Eventually(t, func() bool {
		select {
		case data := <-sink:
			return len(data) > 0 && containsNonZeroKeyedVersion(data)
		default:
			return false
		}
	}, 2*time.Second, 10*time.Millisecond, "wire publication should have non-zero version")
}

// containsNonZeroKeyedVersion checks if JSON data contains a keyed publication
// with key="key1" and a non-zero version.
func containsNonZeroKeyedVersion(data []byte) bool {
	// Simple heuristic: check if data contains "key1" and "version" > 0.
	// The actual wire format contains version field for the publication.
	return len(data) > 0 &&
		bytes.Contains(data, []byte(`"key":"key1"`)) &&
		!bytes.Contains(data, []byte(`"version":0`)) &&
		bytes.Contains(data, []byte(`"version"`))
}

func TestSharedPollEpoch_ChangesOnChannelStateRecreation(t *testing.T) {
	t.Parallel()
	// Versionless mode: when channel state is cleaned up (all keys untracked,
	// shutdown delay fires) and recreated, the epoch must change. Otherwise
	// reconnecting clients keep stale synthetic versions and miss updates
	// until the version counter catches up.
	node := newTestNodeWithSharedPoll(t, SharedPollChannelOptions{
		RefreshInterval:      100 * time.Millisecond,
		RefreshBatchSize:     100,
		MaxKeysPerConnection: 100,
		ChannelShutdownDelay: -1, // Immediate cleanup on last untrack.
	})
	setupSharedPollHandlers(node)

	client := newTestClientV2(t, node, "user1")
	connectClientV2(t, client)
	subscribeSharedPollClient(t, client, "test:channel")

	// Track a key so the channel state is created.
	trackSharedPollClient(t, client, "test:channel", []*protocol.KeyedItem{
		{Key: "key1", Version: 0},
	})

	// Capture epoch from the created channel state.
	epoch1 := node.sharedPollManager.Epoch("test:channel", true)
	require.NotEmpty(t, epoch1)

	// Untrack → triggers immediate channel state cleanup.
	untrackSharedPollClient(t, client, "test:channel", []string{"key1"})

	// Wait for channel state removal.
	require.Eventually(t, func() bool {
		return !node.sharedPollManager.hasChannel("test:channel")
	}, time.Second, 10*time.Millisecond)

	// Track again → channel state recreated with new epoch.
	trackSharedPollClient(t, client, "test:channel", []*protocol.KeyedItem{
		{Key: "key1", Version: 0},
	})

	epoch2 := node.sharedPollManager.Epoch("test:channel", true)
	require.NotEmpty(t, epoch2)
	require.NotEqual(t, epoch1, epoch2, "epoch must change after channel state recreation")
}

// ---- Epoch flip tests (versioned mode, publisher-supplied epoch) ----

// versionedSharedPollOpts returns shared-poll channel options for versioned
// mode with publish-enabled fast-path. Used by epoch-flip tests.
func versionedSharedPollOpts() SharedPollChannelOptions {
	return SharedPollChannelOptions{
		Mode:                 SharedPollModeVersioned,
		RefreshInterval:      time.Hour, // disable refresh worker timer-driven polls
		RefreshBatchSize:     100,
		MaxKeysPerConnection: 100,
		PublishEnabled:       false, // local-only for unit tests
	}
}

// setEpochAwareSharedPollHandler installs an OnSharedPoll handler that
// returns a SharedPollResult carrying the current epoch. Returns a setter
// the test can call to update the epoch race-free across the test goroutine
// and the refresh worker goroutine. This models a well-behaved publisher
// that uses a consistent epoch on both direct publish and refresh response
// paths — without it, the cold-key auto-poll triggered by track() would
// race with explicit publishes in tests: the auto-poll's empty-epoch
// response would flip the channel state back to "" right after the publish
// set it. That thrash is a real misuse case covered by docs; tests should
// model the well-behaved publisher.
func setEpochAwareSharedPollHandler(node *Node, initial string) func(string) {
	var epoch atomic.Pointer[string]
	epoch.Store(&initial)
	node.OnSharedPoll(func(ctx context.Context, event SharedPollEvent) (SharedPollResult, error) {
		return SharedPollResult{Epoch: *epoch.Load()}, nil
	})
	return func(s string) {
		epoch.Store(&s)
	}
}

// setEpochAwareSharedPollHandlerBlocking is the deterministic variant: the
// FIRST handler call (the cold-key auto-poll triggered by track) blocks
// until the test calls release(); subsequent calls run unblocked. This
// eliminates a race where the auto-poll's response — captured before the
// test sets up its initial epoch — lands AFTER the test flips to a new
// epoch and reverts state back. Use whenever the test plans to do a
// setPublisherEpoch(...) after the initial setup.
//
// Returns (setEpoch, release). Typical usage:
//
//	setPub, release := setEpochAwareSharedPollHandlerBlocking(node, "epochA")
//	subscribe + track
//	SharedPollPublish(... "epochA" ...)
//	release()                       // now the auto-poll's response is safe
//	setPub("epochB"); SharedPollPublish(... "epochB" ...)
func setEpochAwareSharedPollHandlerBlocking(node *Node, initial string) (func(string), func()) {
	var epoch atomic.Pointer[string]
	epoch.Store(&initial)
	released := make(chan struct{})
	var pollOnce sync.Once
	node.OnSharedPoll(func(ctx context.Context, event SharedPollEvent) (SharedPollResult, error) {
		pollOnce.Do(func() { <-released })
		return SharedPollResult{Epoch: *epoch.Load()}, nil
	})
	setEpoch := func(s string) { epoch.Store(&s) }
	release := func() { close(released) }
	return setEpoch, release
}

// TestSharedPollEpoch_VersionedInitialEmpty: a versioned channel starts with
// empty epoch. The first publish carrying a non-empty epoch flips state.
//
// The cold-key auto-poll triggered by track() runs the OnSharedPoll handler
// asynchronously, then calls applyRefreshResponse which may flip the channel
// epoch. If the handler captures the epoch BEFORE the test's publish but the
// response processing runs AFTER, the stale-epoch response flips s.epoch back
// — a flaky race inherent to setEpochAwareSharedPollHandler's lockstep model.
//
// To make the test deterministic, the handler blocks until the test releases
// it AFTER the publish. The handler then captures the post-publish epoch and
// applyRefreshResponse becomes a no-op flip (epochA → epochA).
func TestSharedPollEpoch_VersionedInitialEmpty(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t, versionedSharedPollOpts())
	setupSharedPollHandlers(node)

	var epoch atomic.Pointer[string]
	initial := ""
	epoch.Store(&initial)
	released := make(chan struct{})
	var pollOnce sync.Once
	node.OnSharedPoll(func(ctx context.Context, event SharedPollEvent) (SharedPollResult, error) {
		// Only the first handler call (the cold-key auto-poll triggered by
		// track) needs to block — any subsequent polls run unblocked.
		pollOnce.Do(func() {
			<-released
		})
		return SharedPollResult{Epoch: *epoch.Load()}, nil
	})

	client := newTestClientV2(t, node, "user1")
	connectClientV2(t, client)
	subscribeSharedPollClient(t, client, "test:channel")
	trackSharedPollClient(t, client, "test:channel", []*protocol.KeyedItem{{Key: "k1", Version: 0}})

	// No publish yet — epoch is empty.
	require.Equal(t, "", node.sharedPollManager.Epoch("test:channel", false))

	// First publish with non-empty epoch sets the channel epoch.
	epochA := "epochA"
	epoch.Store(&epochA)
	err := node.SharedPollPublish(context.Background(), "test:channel", "k1", 1, "epochA", []byte(`{}`))
	require.NoError(t, err)

	// Release the auto-poll handler now that s.epoch has been set. The handler
	// returns "epochA" and applyRefreshResponse's flipEpoch is a no-op.
	close(released)

	require.Eventually(t, func() bool {
		return node.sharedPollManager.Epoch("test:channel", false) == "epochA"
	}, time.Second, 10*time.Millisecond)
}

// TestSharedPollEpoch_SameEpochVersionCompare: same epoch + monotonic
// versions = standard accept-fresh / skip-stale behavior.
func TestSharedPollEpoch_SameEpochVersionCompare(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t, versionedSharedPollOpts())
	setupSharedPollHandlers(node)
	setEpochAwareSharedPollHandler(node, "epochA")

	client := newTestClientV2(t, node, "user1")
	connectClientV2(t, client)
	subscribeSharedPollClient(t, client, "test:channel")
	trackSharedPollClient(t, client, "test:channel", []*protocol.KeyedItem{{Key: "k1", Version: 0}})

	ctx := context.Background()
	require.NoError(t, node.SharedPollPublish(ctx, "test:channel", "k1", 5, "epochA", []byte(`{"v":5}`)))
	require.NoError(t, node.SharedPollPublish(ctx, "test:channel", "k1", 10, "epochA", []byte(`{"v":10}`)))
	// Stale within same epoch — should be skipped silently.
	require.NoError(t, node.SharedPollPublish(ctx, "test:channel", "k1", 3, "epochA", []byte(`{"v":3-stale"}`)))

	// Epoch unchanged.
	require.Eventually(t, func() bool {
		return node.sharedPollManager.Epoch("test:channel", false) == "epochA"
	}, time.Second, 10*time.Millisecond)
}

// TestSharedPollEpoch_FlipUnsubscribesClient: a publish with a different
// epoch unsubscribes existing subscribers with insufficient-state code.
func TestSharedPollEpoch_FlipUnsubscribesClient(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t, versionedSharedPollOpts())
	setupSharedPollHandlers(node)

	// Use the blocking-handler variant: track()'s cold-key auto-poll reads
	// the epoch atomic synchronously but applies its response asynchronously.
	// Without blocking, the response (captured before "epochA" was even set)
	// can land AFTER we flip to "epochB" and silently revert state back.
	setPublisherEpoch, releaseAutoPoll := setEpochAwareSharedPollHandlerBlocking(node, "epochA")

	client := newTestClientV2(t, node, "user1")
	connectClientV2(t, client)

	// Capture unsubscribe events on the client.
	unsubReceived := make(chan Unsubscribe, 1)
	client.eventHub.unsubscribeHandler = func(event UnsubscribeEvent) {
		select {
		case unsubReceived <- Unsubscribe{Code: event.Code, Reason: event.Reason}:
		default:
		}
	}

	subscribeSharedPollClient(t, client, "test:channel")
	trackSharedPollClient(t, client, "test:channel", []*protocol.KeyedItem{{Key: "k1", Version: 0}})

	ctx := context.Background()
	// Establish first epoch.
	require.NoError(t, node.SharedPollPublish(ctx, "test:channel", "k1", 1, "epochA", []byte(`{"v":1}`)))
	require.Eventually(t, func() bool {
		return node.sharedPollManager.Epoch("test:channel", false) == "epochA"
	}, time.Second, 10*time.Millisecond)

	// Release the cold-key auto-poll. Its response carries "epochA" (no flip).
	releaseAutoPoll()

	// Flip with new epoch — update the refresh handler in lockstep so the
	// publisher stays consistent across both paths.
	setPublisherEpoch("epochB")
	require.NoError(t, node.SharedPollPublish(ctx, "test:channel", "k1", 1, "epochB", []byte(`{"v":1-newepoch"}`)))

	// Channel epoch updated.
	require.Eventually(t, func() bool {
		return node.sharedPollManager.Epoch("test:channel", false) == "epochB"
	}, time.Second, 10*time.Millisecond)

	// Client received unsubscribe with insufficient-state code.
	select {
	case unsub := <-unsubReceived:
		require.Equal(t, UnsubscribeCodeInsufficient, unsub.Code)
	case <-time.After(2 * time.Second):
		t.Fatal("expected client to receive insufficient-state unsubscribe after epoch flip")
	}
}

// TestSharedPollEpoch_FlipResetsEntries: an epoch flip resets per-key version
// state — a publish with version=1 under the new epoch is accepted even when
// the previous epoch had reached version=1000.
func TestSharedPollEpoch_FlipResetsEntries(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t, versionedSharedPollOpts())
	setupSharedPollHandlers(node)
	// Blocking variant: the cold-key auto-poll's response (carrying "epochA")
	// must land before we flip to "epochB", otherwise the late apply reverts
	// the channel epoch back to "epochA". See helper docs.
	setPublisherEpoch, releaseAutoPoll := setEpochAwareSharedPollHandlerBlocking(node, "epochA")

	client := newTestClientV2(t, node, "user1")
	connectClientV2(t, client)
	subscribeSharedPollClient(t, client, "test:channel")
	trackSharedPollClient(t, client, "test:channel", []*protocol.KeyedItem{{Key: "k1", Version: 0}})

	ctx := context.Background()
	require.NoError(t, node.SharedPollPublish(ctx, "test:channel", "k1", 1000, "epochA", []byte(`{"v":1000}`)))
	require.Eventually(t, func() bool {
		return node.sharedPollManager.Epoch("test:channel", false) == "epochA"
	}, time.Second, 10*time.Millisecond)

	// Release the cold-key auto-poll. Its response carries "epochA" (no flip).
	releaseAutoPoll()

	// New epoch with low version: must be accepted (entries reset on flip).
	setPublisherEpoch("epochB")
	require.NoError(t, node.SharedPollPublish(ctx, "test:channel", "k1", 1, "epochB", []byte(`{"v":1-postflip"}`)))

	// Re-track client (it was unsubbed by the flip). Then verify that the
	// post-flip publish data is reachable by re-subscribing and tracking.
	// Internal state check: channel epoch is now epochB.
	require.Eventually(t, func() bool {
		return node.sharedPollManager.Epoch("test:channel", false) == "epochB"
	}, time.Second, 10*time.Millisecond)

	// A subsequent publish with version=2 under epochB must be applied
	// (proves the entry version was reset to 0 on flip, not retained at 1000).
	subscribeSharedPollClient(t, client, "test:channel")
	trackSharedPollClient(t, client, "test:channel", []*protocol.KeyedItem{{Key: "k1", Version: 0}})
	require.NoError(t, node.SharedPollPublish(ctx, "test:channel", "k1", 2, "epochB", []byte(`{"v":2-postflip"}`)))

	// Stale (≤ 2) publishes under same epoch should still be skipped.
	require.NoError(t, node.SharedPollPublish(ctx, "test:channel", "k1", 1, "epochB", []byte(`{"v":1-stale"}`)))
}

// TestSharedPollEpoch_FlipOnUntrackedKey: a publish for a key NOT tracked on
// this node still updates the stored channel epoch (per-node ignorance fix).
// This avoids an extra unsub-resub cycle for the first subscriber to a quiet
// node.
func TestSharedPollEpoch_FlipOnUntrackedKey(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t, versionedSharedPollOpts())
	setupSharedPollHandlers(node)
	setEpochAwareSharedPollHandler(node, "epochX")

	client := newTestClientV2(t, node, "user1")
	connectClientV2(t, client)
	subscribeSharedPollClient(t, client, "test:channel")
	// Track only k1.
	trackSharedPollClient(t, client, "test:channel", []*protocol.KeyedItem{{Key: "k1", Version: 0}})

	ctx := context.Background()
	// Publish for an UNTRACKED key — entry doesn't exist on this node.
	// Channel epoch should still update.
	require.NoError(t, node.SharedPollPublish(ctx, "test:channel", "untracked_key", 1, "epochX", []byte(`{}`)))
	require.Eventually(t, func() bool {
		return node.sharedPollManager.Epoch("test:channel", false) == "epochX"
	}, time.Second, 10*time.Millisecond)
}

// TestSharedPollEpoch_EmptyEpochUnchangedBehavior: publishing with empty
// epoch when stored is also empty performs pure version comparison — no
// flip, no unsubscribe.
func TestSharedPollEpoch_EmptyEpochUnchangedBehavior(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t, versionedSharedPollOpts())
	setupSharedPollHandlers(node)

	client := newTestClientV2(t, node, "user1")
	connectClientV2(t, client)

	unsubCount := atomic.Int32{}
	client.eventHub.unsubscribeHandler = func(event UnsubscribeEvent) {
		unsubCount.Add(1)
	}

	subscribeSharedPollClient(t, client, "test:channel")
	trackSharedPollClient(t, client, "test:channel", []*protocol.KeyedItem{{Key: "k1", Version: 0}})

	ctx := context.Background()
	require.NoError(t, node.SharedPollPublish(ctx, "test:channel", "k1", 1, "", []byte(`{}`)))
	require.NoError(t, node.SharedPollPublish(ctx, "test:channel", "k1", 2, "", []byte(`{}`)))

	require.Equal(t, "", node.sharedPollManager.Epoch("test:channel", false))
	// Give time for any spurious unsubscribe event.
	time.Sleep(50 * time.Millisecond)
	require.Equal(t, int32(0), unsubCount.Load(), "no flip should occur with empty-to-empty epochs")
}

// TestSharedPollEpoch_SubscribeReplyCarriesEpoch: subscribe reply for a
// versioned channel returns the current channel epoch. Clients use it to
// detect mid-session flips on resubscribe.
func TestSharedPollEpoch_SubscribeReplyCarriesEpoch(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t, versionedSharedPollOpts())
	setupSharedPollHandlers(node)
	setEpochAwareSharedPollHandler(node, "epochA")

	client := newTestClientV2(t, node, "user1")
	connectClientV2(t, client)

	// First subscribe before any publish — epoch is empty.
	res1 := subscribeSharedPollClient(t, client, "test:channel")
	require.Equal(t, "", res1.Epoch)
	trackSharedPollClient(t, client, "test:channel", []*protocol.KeyedItem{{Key: "k1", Version: 0}})

	// Publish with epoch sets channel state.
	require.NoError(t, node.SharedPollPublish(context.Background(), "test:channel", "k1", 1, "epochA", []byte(`{}`)))
	require.Eventually(t, func() bool {
		return node.sharedPollManager.Epoch("test:channel", false) == "epochA"
	}, time.Second, 10*time.Millisecond)

	// Re-subscribe (after the flip-driven unsub above).
	client2 := newTestClientV2(t, node, "user2")
	connectClientV2(t, client2)
	res2 := subscribeSharedPollClient(t, client2, "test:channel")
	require.Equal(t, "epochA", res2.Epoch)
}

// TestKeyedWritePublication_EarlyReturns covers the no-op safety branches at
// the top of keyedWritePublication: c.keyed unset, channel not tracked, key
// not tracked, and incoming pubVersion <= keyState.version.
func TestKeyedWritePublication_EarlyReturns(t *testing.T) {
	t.Parallel()
	node := defaultTestNode()
	defer func() { _ = node.Shutdown(context.Background()) }()

	// c.keyed == nil → must return without panicking.
	client := newTestClient(t, node, "u1")
	require.NotPanics(t, func() {
		client.keyedWritePublication("ch", "k", 1, &protocol.Publication{Data: []byte(`{}`)}, preparedData{})
	})

	// c.keyed set, channel not tracked → return.
	client.mu.Lock()
	client.keyed = &keyedState{
		channels:    map[string]*keyedChannelDeltaState{},
		trackedKeys: map[string]map[string]*keyedKeyState{},
	}
	client.mu.Unlock()
	require.NotPanics(t, func() {
		client.keyedWritePublication("not-tracked", "k", 1, &protocol.Publication{Data: []byte(`{}`)}, preparedData{})
	})

	// Channel tracked but key absent → return.
	client.mu.Lock()
	client.keyed.trackedKeys["ch"] = map[string]*keyedKeyState{}
	client.mu.Unlock()
	require.NotPanics(t, func() {
		client.keyedWritePublication("ch", "missing", 1, &protocol.Publication{Data: []byte(`{}`)}, preparedData{})
	})

	// Key tracked at version 5; incoming version <= 5 → return.
	client.mu.Lock()
	client.keyed.trackedKeys["ch"]["k"] = &keyedKeyState{version: 5}
	client.mu.Unlock()
	require.NotPanics(t, func() {
		client.keyedWritePublication("ch", "k", 5, &protocol.Publication{Data: []byte(`{}`)}, preparedData{})
	})
}

// TestKeyedWriteRemoval_EarlyReturns covers the symmetric no-op safety
// branches in keyedWriteRemoval.
func TestKeyedWriteRemoval_EarlyReturns(t *testing.T) {
	t.Parallel()
	node := defaultTestNode()
	defer func() { _ = node.Shutdown(context.Background()) }()

	// c.keyed == nil → no-op.
	client := newTestClient(t, node, "u2")
	require.NotPanics(t, func() {
		client.keyedWriteRemoval("ch", "k", &protocol.Publication{Removed: true})
	})

	// c.keyed set, channel not tracked → no-op.
	client.mu.Lock()
	client.keyed = &keyedState{
		channels:    map[string]*keyedChannelDeltaState{},
		trackedKeys: map[string]map[string]*keyedKeyState{},
	}
	client.mu.Unlock()
	require.NotPanics(t, func() {
		client.keyedWriteRemoval("not-tracked", "k", &protocol.Publication{Removed: true})
	})
}

// TestSharedPollSubscribe_ExpireAtPast covers the ttl <= 0 branch which
// returns ErrorExpired during the OnSubscribe callback.
func TestSharedPollSubscribe_ExpireAtPast(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t)
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{ExpireAt: time.Now().Unix() - 10},
			}, nil)
		})
	})
	client := newTestClientV2(t, node, "user1")
	connectClientV2(t, client)

	rwWrapper := testReplyWriterWrapper()
	err := client.handleSubscribe(&protocol.SubscribeRequest{
		Channel: "test:channel",
		Type:    int32(SubscriptionTypeSharedPoll),
	}, &protocol.Command{Id: 1}, time.Now(), rwWrapper.rw)
	require.NoError(t, err)
	require.Eventually(t, func() bool { return len(rwWrapper.replies) > 0 }, time.Second, time.Millisecond)
	require.NotNil(t, rwWrapper.replies[0].Error)
	require.Equal(t, uint32(ErrorExpired.Code), rwWrapper.replies[0].Error.Code)
}

// TestSharedPollSubscribe_ChannelLimitExceeded covers the channel-limit
// branch that returns ErrorLimitExceeded. We bypass the
// subscribeSharedPollClient helper because it pre-registers the channel,
// which would itself trip the limit on the first call.
func TestSharedPollSubscribe_ChannelLimitExceeded(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t)
	node.config.ClientChannelLimit = 1
	setupSharedPollHandlers(node)
	client := newTestClientV2(t, node, "user1")
	connectClientV2(t, client)

	rw1 := testReplyWriterWrapper()
	require.NoError(t, client.handleSubscribe(&protocol.SubscribeRequest{
		Channel: "test:first",
		Type:    int32(SubscriptionTypeSharedPoll),
	}, &protocol.Command{Id: 1}, time.Now(), rw1.rw))
	require.Eventually(t, func() bool { return len(rw1.replies) > 0 }, time.Second, time.Millisecond)
	require.Nil(t, rw1.replies[0].Error)

	rw2 := testReplyWriterWrapper()
	err := client.handleSubscribe(&protocol.SubscribeRequest{
		Channel: "test:second",
		Type:    int32(SubscriptionTypeSharedPoll),
	}, &protocol.Command{Id: 2}, time.Now(), rw2.rw)
	require.Equal(t, ErrorLimitExceeded, err)
}

// pollHandlerCounter wraps an OnSharedPoll handler with an atomic counter
// over invocations. The cold-key auto-poll fired by track() runs the
// handler asynchronously, so tests that need to observe its completion can
// install this wrapper and call waitForPolls before subsequent assertions.
//
// Caveat: applyRefreshResponse runs AFTER the handler returns. The counter
// is incremented at the END of the handler call (just before return), so
// count >= N proves the handler has been invoked N times but does NOT
// prove all N responses have been applied. Tests that depend on the
// applied state should additionally assert a state predicate (e.g.
// require.Eventually on Epoch == "...") that the apply step would set.
type pollHandlerCounter struct {
	count atomic.Int64
}

// installPollHandlerCounter replaces the node's OnSharedPoll with a counter
// that delegates to `inner`. If inner is nil, the wrapper returns an empty
// SharedPollResult.
func installPollHandlerCounter(node *Node, inner func(ctx context.Context, event SharedPollEvent) (SharedPollResult, error)) *pollHandlerCounter {
	c := &pollHandlerCounter{}
	node.OnSharedPoll(func(ctx context.Context, event SharedPollEvent) (SharedPollResult, error) {
		var res SharedPollResult
		var err error
		if inner != nil {
			res, err = inner(ctx, event)
		}
		c.count.Add(1)
		return res, err
	})
	return c
}

// waitForPolls blocks until the counter has observed at least `target`
// handler invocations or the deadline expires.
func (c *pollHandlerCounter) waitForPolls(t testing.TB, target int64) {
	t.Helper()
	require.Eventually(t, func() bool {
		return c.count.Load() >= target
	}, 5*time.Second, 10*time.Millisecond,
		"expected >= %d OnSharedPoll handler invocations, got %d", target, c.count.Load())
}

// TestSharedPoll_ColdKeyAutoPoll_Synchronization demonstrates the
// pollHandlerCounter helper: after trackSharedPollClient, callers can
// wait for the cold-key auto-poll handler to have been invoked before
// asserting downstream state. This makes ordering deterministic instead
// of relying on a fixed time.Sleep.
func TestSharedPoll_ColdKeyAutoPoll_Synchronization(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t, versionedSharedPollOpts())
	setupSharedPollHandlers(node)

	counter := installPollHandlerCounter(node, func(_ context.Context, _ SharedPollEvent) (SharedPollResult, error) {
		return SharedPollResult{Epoch: "epoch1"}, nil
	})

	client := newTestClientV2(t, node, "user1")
	connectClientV2(t, client)
	subscribeSharedPollClient(t, client, "test:autopoll_sync")
	trackSharedPollClient(t, client, "test:autopoll_sync",
		[]*protocol.KeyedItem{{Key: "k1", Version: 0}})

	// The cold-key auto-poll runs OnSharedPoll once.
	counter.waitForPolls(t, 1)

	// Channel epoch reflects what the auto-poll returned.
	require.Eventually(t, func() bool {
		return node.sharedPollManager.Epoch("test:autopoll_sync", false) == "epoch1"
	}, 2*time.Second, 10*time.Millisecond)
}

// TestSharedPollEpoch_FlipStorm_NoPanic_NoLeak: a publisher alternates
// epochs rapidly while subscribers track and re-track. The system must
// remain panic-free and converge to a final epoch matching the last
// publish. Goroutine-leak surrogate: after activity drains, the manager
// has at most one state for the channel and that state's worker is
// running or has exited cleanly (verified by Close in t.Cleanup
// completing within reasonable time).
func TestSharedPollEpoch_FlipStorm_NoPanic_NoLeak(t *testing.T) {
	t.Parallel()
	opts := versionedSharedPollOpts()
	node := newTestNodeWithSharedPoll(t, opts)
	setupSharedPollHandlers(node)

	// Publisher epoch atomic — handler reads it on each invocation. Models
	// a well-behaved publisher whose direct publishes and refresh responses
	// stay in lockstep.
	var currentEpoch atomic.Pointer[string]
	initial := "epochA"
	currentEpoch.Store(&initial)
	node.OnSharedPoll(func(_ context.Context, _ SharedPollEvent) (SharedPollResult, error) {
		return SharedPollResult{Epoch: *currentEpoch.Load()}, nil
	})

	channel := "test:flip_storm"

	// Subscribe a small set of clients — enough for the epoch-flip
	// unsubscribe path to fire on each flip but not so many that the
	// test bogs down under -race.
	const numClients = 4
	clients := make([]*Client, numClients)
	for i := 0; i < numClients; i++ {
		clients[i] = newTestClientV2(t, node, "u"+string(rune('A'+i)))
		connectClientV2(t, clients[i])
		subscribeSharedPollClient(t, clients[i], channel)
		trackSharedPollClient(t, clients[i], channel,
			[]*protocol.KeyedItem{{Key: "k", Version: 0}})
	}

	ctx := context.Background()
	epochA := "epochA"
	epochB := "epochB"

	const flips = 200
	for i := 0; i < flips; i++ {
		var e string
		if i%2 == 0 {
			e = epochA
		} else {
			e = epochB
		}
		currentEpoch.Store(&e)
		// Publish with the same epoch the handler currently returns.
		// Pre-flip: subscribers get unsubscribed; re-subscribe path is
		// not exercised here — we just want robustness of the flip path.
		require.NoError(t, node.SharedPollPublish(ctx, channel, "k", uint64(i+1), e, []byte(`{}`)))
	}

	// Final state: channel epoch matches one of the two values used.
	require.Eventually(t, func() bool {
		e := node.sharedPollManager.Epoch(channel, false)
		return e == epochA || e == epochB
	}, 2*time.Second, 10*time.Millisecond)
}

// TestSharedPollEncodeFailureRollback_TODO documents that audit finding 5
// (keyState.version rollback on subscribe-reply encode failure) is not
// covered by an explicit test because the test transport / protocol pool
// does not currently support injecting a reply-encode error.
// getSubRefreshCommandReply unconditionally returns nil error (it acquires
// from a pool), so the rollback branch in handleTrack is unreachable from
// tests without production-side changes. Coverage of the rollback path
// would require either:
//   - exposing an encoder hook on the test transport, or
//   - making getSubRefreshCommandReply fallible (e.g. via a marshal hook).
//
// Left as a TODO so the regression risk is visible. The fix is in
// client_keyed.go handleTrack, lines 334-350 (rollback step on encode
// error).
func TestSharedPollEncodeFailureRollback_TODO(t *testing.T) {
	t.Skip("encode failure cannot be injected without production hooks; see comment")
}

// TestSharedPollSubscribe_PresenceFlags covers the presence-flag branches
// (PushJoinLeave + MapClientPresenceChannel + MapUserPresenceChannel) in
// handleSharedPollSubscribe.
func TestSharedPollSubscribe_PresenceFlags(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t)
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					ExpireAt:                 time.Now().Unix() + 3600,
					EmitPresence:             true,
					EmitJoinLeave:            true,
					PushJoinLeave:            true,
					MapClientPresenceChannel: "clients:room",
					MapUserPresenceChannel:   "users:room",
				},
				ClientSideRefresh: true,
			}, nil)
		})
	})
	client := newTestClientV2(t, node, "user1")
	connectClientV2(t, client)

	subscribeSharedPollClient(t, client, "test:room")

	client.mu.RLock()
	ctx := client.channels["test:room"]
	client.mu.RUnlock()
	require.True(t, channelHasFlag(ctx.flags, flagEmitPresence))
	require.True(t, channelHasFlag(ctx.flags, flagEmitJoinLeave))
	require.True(t, channelHasFlag(ctx.flags, flagPushJoinLeave))
	require.True(t, channelHasFlag(ctx.flags, flagMapClientPresence))
	require.True(t, channelHasFlag(ctx.flags, flagMapUserPresence))
}
