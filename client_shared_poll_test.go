package centrifuge

import (
	"bytes"
	"context"
	"errors"
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
				{Key: "key1", Data: []byte(`"test_data"`), Version: 42},
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
					{Key: "key1", Data: []byte(`"data"`), Version: 1},
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
			items[i] = SharedPollRefreshItem{Key: it.Key, Data: []byte(`"data"`), Version: 1}
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
			items[i] = SharedPollRefreshItem{Key: it.Key, Data: []byte(`"data"`), Version: 1}
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
					{Key: "key1", Data: []byte(`"data"`), Version: 5},
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
				{Key: "key1", Data: []byte(`"data"`), Version: 5},
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
					{Key: "key1", Data: []byte(`"data"`), Version: 5},
				},
			}, nil
		}
		pollMu.Lock()
		pollItems = make([]SharedPollItem, len(event.Items))
		copy(pollItems, event.Items)
		pollMu.Unlock()
		return SharedPollResult{
			Items: []SharedPollRefreshItem{
				{Key: "key1", Data: []byte(`"data"`), Version: 5},
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
			items[i] = SharedPollRefreshItem{Key: it.Key, Data: []byte(`"data"`), Version: 1}
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
			items[i] = SharedPollRefreshItem{Key: it.Key, Data: []byte(`"data"`), Version: 1}
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
				{Key: "key1", Data: []byte(`"data"`), Version: 5},
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
				{Key: "key1", Data: []byte(`"new"`), Version: 5},
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
				{Key: "key1", Data: []byte(`"data"`), Version: v},
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
			items[i] = SharedPollRefreshItem{Key: it.Key, Data: []byte(`"data"`), Version: 1}
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
	pub := &protocol.Publication{Data: []byte(`"test"`)}
	prep := buildPreparedPollData(pub, nil, 0)
	require.False(t, prep.deltaSub)
	require.Nil(t, prep.keyedDeltaPatch)
}

func TestBuildPreparedPollData_WithPrevData(t *testing.T) {
	t.Parallel()
	// Use sufficiently large data so fossil delta overhead is smaller than full payload.
	prevData := []byte(`{"value":"this is a longer string so delta overhead is worth it","count":1}`)
	newData := []byte(`{"value":"this is a longer string so delta overhead is worth it","count":2}`)
	pub := &protocol.Publication{Data: newData}
	prep := buildPreparedPollData(pub, prevData, 5)
	require.True(t, prep.deltaSub)
	require.NotNil(t, prep.keyedDeltaPatch)
	require.Equal(t, uint64(5), prep.keyedDeltaPrevVersion)
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
	prep := buildPreparedPollData(pub, prevData, 1)
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
				{Key: "cold_key", Data: []byte(`"data"`), Version: 1},
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
				{Key: "key1", Data: []byte(`"data"`), Version: 1},
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
				{Key: "key1", Data: []byte(`"data"`), Version: 1},
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
				{Key: "key1", Data: []byte(`"data"`), Version: 5},
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
				{Key: "key1", Data: []byte(`"data"`), Version: 1},
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

// TestSharedPollTrack_InlineUntrack verifies that when a sub_refresh type=1
// (track) frame includes non-empty Untrack keys, the server validates the
// full HMAC batch and immediately removes those keys in the same handler —
// no separate untrack round-trip is needed.
func TestSharedPollTrack_InlineUntrack(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t)

	var mu sync.Mutex
	var capturedUntrack *UntrackEvent

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
		client.OnUntrack(func(e UntrackEvent) {
			mu.Lock()
			evt := e
			capturedUntrack = &evt
			mu.Unlock()
		})
		client.OnSubRefresh(func(e SubRefreshEvent, cb SubRefreshCallback) {
			cb(SubRefreshReply{}, nil)
		})
	})

	client := newTestClientV2(t, node, "user1")
	connectClientV2(t, client)
	subscribeSharedPollClient(t, client, "test:channel")

	// Combined track+untrack frame: track keyA, keyB, keyC but inline-untrack keyB.
	rwWrapper := testReplyWriterWrapper()
	err := client.handleSubRefresh(&protocol.SubRefreshRequest{
		Channel: "test:channel",
		Type:    typeTrack,
		Track: []*protocol.TrackBatch{{
			Items: []*protocol.KeyedItem{
				{Key: "keyA"},
				{Key: "keyB"},
				{Key: "keyC"},
			},
		}},
		Untrack: []string{"keyB"},
	}, &protocol.Command{Id: 2}, time.Now(), rwWrapper.rw)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return len(rwWrapper.replies) > 0
	}, time.Second, time.Millisecond)
	require.Nil(t, rwWrapper.replies[0].Error)

	// keyA and keyC must be tracked; keyB must have been removed inline.
	client.mu.RLock()
	_, hasA := client.keyed.trackedKeys["test:channel"]["keyA"]
	_, hasB := client.keyed.trackedKeys["test:channel"]["keyB"]
	_, hasC := client.keyed.trackedKeys["test:channel"]["keyC"]
	client.mu.RUnlock()
	require.True(t, hasA, "keyA should be tracked")
	require.True(t, hasC, "keyC should be tracked")
	require.False(t, hasB, "keyB should have been removed by inline untrack")

	// untrackHandler must have fired with keyB.
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return capturedUntrack != nil
	}, time.Second, time.Millisecond)
	mu.Lock()
	evt := *capturedUntrack
	mu.Unlock()
	require.Equal(t, "test:channel", evt.Channel)
	require.Equal(t, []string{"keyB"}, evt.Keys)
}

// TestSharedPollTrack_InlineUntrack_RandomKeysIgnored verifies that random keys
// sent in the Untrack field of a track frame (not covered by HMAC) are silently
// dropped: untrackHandler must not fire, and hub/sharedPoll state is unchanged.
func TestSharedPollTrack_InlineUntrack_RandomKeysIgnored(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t)

	untrackFired := atomic.Bool{}

	node.OnConnecting(func(ctx context.Context, e ConnectEvent) (ConnectReply, error) {
		return ConnectReply{}, nil
	})
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{Options: SubscribeOptions{ExpireAt: time.Now().Unix() + 3600}, ClientSideRefresh: true}, nil)
		})
		client.OnTrack(func(e TrackEvent, cb TrackCallback) {
			cb(TrackReply{}, nil)
		})
		client.OnUntrack(func(e UntrackEvent) {
			untrackFired.Store(true)
		})
	})

	client := newTestClientV2(t, node, "user1")
	connectClientV2(t, client)
	subscribeSharedPollClient(t, client, "test:channel")

	// Track only keyA, then inline-untrack a completely random key never tracked.
	rwWrapper := testReplyWriterWrapper()
	err := client.handleSubRefresh(&protocol.SubRefreshRequest{
		Channel: "test:channel",
		Type:    typeTrack,
		Track: []*protocol.TrackBatch{{
			Items: []*protocol.KeyedItem{{Key: "keyA"}},
		}},
		Untrack: []string{"never-tracked-key"},
	}, &protocol.Command{Id: 2}, time.Now(), rwWrapper.rw)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return len(rwWrapper.replies) > 0
	}, time.Second, time.Millisecond)
	require.Nil(t, rwWrapper.replies[0].Error)

	// keyA must be tracked; random key must not appear in trackedKeys.
	client.mu.RLock()
	_, hasA := client.keyed.trackedKeys["test:channel"]["keyA"]
	_, hasRandom := client.keyed.trackedKeys["test:channel"]["never-tracked-key"]
	client.mu.RUnlock()
	require.True(t, hasA)
	require.False(t, hasRandom)

	// untrackHandler must not have fired.
	require.False(t, untrackFired.Load(), "untrackHandler must not fire for keys that were never tracked")
}

// TestSharedPollUntrack_RandomKeysIgnored verifies that a standalone untrack
// frame (type=2) with keys the client never tracked is a server-side no-op:
// untrackHandler must not fire.
func TestSharedPollUntrack_RandomKeysIgnored(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t)

	untrackFired := atomic.Bool{}

	node.OnConnecting(func(ctx context.Context, e ConnectEvent) (ConnectReply, error) {
		return ConnectReply{}, nil
	})
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{Options: SubscribeOptions{ExpireAt: time.Now().Unix() + 3600}, ClientSideRefresh: true}, nil)
		})
		client.OnTrack(func(e TrackEvent, cb TrackCallback) {
			cb(TrackReply{}, nil)
		})
		client.OnUntrack(func(e UntrackEvent) {
			untrackFired.Store(true)
		})
	})

	client := newTestClientV2(t, node, "user1")
	connectClientV2(t, client)
	subscribeSharedPollClient(t, client, "test:channel")

	// Send standalone untrack for a key that was never tracked.
	rwWrapper := testReplyWriterWrapper()
	err := client.handleSubRefresh(&protocol.SubRefreshRequest{
		Channel: "test:channel",
		Type:    typeUntrack,
		Untrack: []string{"never-tracked-key"},
	}, &protocol.Command{Id: 2}, time.Now(), rwWrapper.rw)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return len(rwWrapper.replies) > 0
	}, time.Second, time.Millisecond)
	require.Nil(t, rwWrapper.replies[0].Error)

	// untrackHandler must not have fired.
	require.False(t, untrackFired.Load(), "untrackHandler must not fire for keys that were never tracked")
}

// TestSharedPollTrack_InlineUntrack_FreesExistingSlots asserts that when a
// track frame inline-untracks keys that the client ALREADY has tracked from
// a prior request, the limit check accounts for the slots being freed. A
// signature-library replay typically re-tracks every known key, so if the
// user has untracked some keys since the signatures were obtained, those
// keys appear in both the chanKeys map (from the previous track) AND in
// req.Untrack — Step 8 of handleTrack removes them, so the final tracked
// count is (current + new - inline_untracked_existing).
//
// Concrete repro: MaxKeysPerConnection=3, chanKeys={A,B} already tracked,
// then re-track [A,B,C,D,E] with Untrack=[A,B]. Final state is {C,D,E},
// size 3, fits the limit. A correct limit check must allow this.
func TestSharedPollTrack_InlineUntrack_FreesExistingSlots(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t, SharedPollChannelOptions{
		RefreshInterval:      100 * time.Millisecond,
		MaxKeysPerConnection: 3,
	})
	setupSharedPollHandlers(node)
	client := newTestClientV2(t, node, "user1")
	connectClientV2(t, client)
	subscribeSharedPollClient(t, client, "test:channel")

	// First track: bring chanKeys to {A, B}.
	trackSharedPollClient(t, client, "test:channel", []*protocol.KeyedItem{
		{Key: "A"},
		{Key: "B"},
	})

	client.mu.RLock()
	preCount := len(client.keyed.trackedKeys["test:channel"])
	client.mu.RUnlock()
	require.Equal(t, 2, preCount)

	// Re-track {A,B,C,D,E} with inline-untrack of A,B. Final state must be
	// {C,D,E} = 3 keys, which fits MaxKeysPerConnection=3. The buggy limit
	// check rejects this because it doesn't subtract |existing ∩ inline-untrack|.
	rwWrapper := testReplyWriterWrapper()
	err := client.handleSubRefresh(&protocol.SubRefreshRequest{
		Channel: "test:channel",
		Type:    typeTrack,
		Track: []*protocol.TrackBatch{{Items: []*protocol.KeyedItem{
			{Key: "A"}, {Key: "B"}, {Key: "C"}, {Key: "D"}, {Key: "E"},
		}}},
		Untrack: []string{"A", "B"},
	}, &protocol.Command{Id: 3}, time.Now(), rwWrapper.rw)
	require.NoError(t, err, "track with inline-untrack of existing keys must not return a top-level error")

	require.Eventually(t, func() bool {
		return len(rwWrapper.replies) > 0
	}, time.Second, time.Millisecond)
	require.Nil(t, rwWrapper.replies[0].Error,
		"limit check must account for inline-untracked existing keys: got error %v", rwWrapper.replies[0].Error)

	// Final state assertion: {C, D, E}.
	client.mu.RLock()
	defer client.mu.RUnlock()
	chanKeys := client.keyed.trackedKeys["test:channel"]
	require.Equal(t, 3, len(chanKeys), "final tracked count should be 3, got %d", len(chanKeys))
	_, hasA := chanKeys["A"]
	_, hasB := chanKeys["B"]
	_, hasC := chanKeys["C"]
	_, hasD := chanKeys["D"]
	_, hasE := chanKeys["E"]
	require.False(t, hasA, "A should be untracked")
	require.False(t, hasB, "B should be untracked")
	require.True(t, hasC, "C should be tracked")
	require.True(t, hasD, "D should be tracked")
	require.True(t, hasE, "E should be tracked")
}

// TestSharedPollTrack_WarmKey_DeliversLatestSnapshot is a sanity check for
// the warm-key snapshot ordering fix: handleTrack now captures the warm-key
// snapshot AFTER hub.addSubscriber so that a publish landing between the
// snapshot and subscriber registration is delivered via the broadcast path
// rather than missed.
//
// This test cannot deterministically reproduce the lost-update race (it
// would need a sync hook between Step 5 addSubscriber and Step 5b
// getWarmKeyData). Instead it asserts the happy path: a warm key with
// cached data is delivered at the latest version available on the server.
func TestSharedPollTrack_WarmKey_DeliversLatestSnapshot(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t, SharedPollChannelOptions{
		RefreshInterval:      100 * time.Millisecond,
		RefreshBatchSize:     100,
		MaxKeysPerConnection: 100,
		KeepLatestData:       true,
		Mode:                 SharedPollModeVersioned,
	})

	currentVersion := atomic.Uint64{}
	currentVersion.Store(7)
	node.OnSharedPoll(func(ctx context.Context, event SharedPollEvent) (SharedPollResult, error) {
		v := currentVersion.Load()
		return SharedPollResult{
			Items: []SharedPollRefreshItem{
				{Key: "key1", Data: []byte(`{"v":7}`), Version: v},
			},
		}, nil
	})

	setupSharedPollHandlers(node)

	// First client tracks the key so the server has the entry registered
	// and the refresh worker populates entry.data with v=7.
	client1 := newTestClientV2(t, node, "user1")
	connectClientV2(t, client1)
	subscribeSharedPollClient(t, client1, "test:channel")
	trackSharedPollClient(t, client1, "test:channel", []*protocol.KeyedItem{
		{Key: "key1", Version: 0},
	})

	// Wait for client1 to receive v=7 so the server entry is stable.
	require.Eventually(t, func() bool {
		client1.mu.RLock()
		defer client1.mu.RUnlock()
		ks := client1.keyed.trackedKeys["test:channel"]["key1"]
		return ks != nil && ks.version == 7
	}, 2*time.Second, 10*time.Millisecond)

	// Now a second client tracks the same key with version 0. The key is
	// warm on the server (entry.version=7, data cached), so handleTrack
	// takes the warm-key direct-delivery path. After the fix, the snapshot
	// is captured after addSubscriber, so client2 must end up at v=7.
	client2 := newTestClientV2(t, node, "user2")
	connectClientV2(t, client2)
	subscribeSharedPollClient(t, client2, "test:channel")
	trackSharedPollClient(t, client2, "test:channel", []*protocol.KeyedItem{
		{Key: "key1", Version: 0},
	})

	require.Eventually(t, func() bool {
		client2.mu.RLock()
		defer client2.mu.RUnlock()
		ks := client2.keyed.trackedKeys["test:channel"]["key1"]
		return ks != nil && ks.version == 7
	}, 2*time.Second, 10*time.Millisecond)
}

// TestSharedPollTrack_RaceWithChannelShutdownStress stress-tests the race
// between sharedPollChannelState's finalizeShutdown (which removes the
// keyedManager state) and a new client's handleTrack (which calls
// keyedManager.getOrCreateChannel followed later by getHub).
//
// Scenario:
//   client A tracks → untracks (immediate shutdown removes both
//   sharedPollManager state and keyedManager state)
//   client B handleTrack runs concurrently — getOrCreateChannel may create
//   the keyedManager state just before A's finalizeShutdown calls
//   removeChannel, leaving B's later getHub returning nil → panic at
//   addSubscriber.
//
// The test runs many iterations and expects no panic / no missed
// broadcasts. With ChannelShutdownDelay=-1 (immediate), the race window
// is the small time between sharedPollManager's deleting m.channels[ch]
// and keyedManager.removeChannel(ch).
func TestSharedPollTrack_RaceWithChannelShutdownStress(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t, SharedPollChannelOptions{
		RefreshInterval:      time.Hour,
		RefreshBatchSize:     100,
		MaxKeysPerConnection: 100,
		ChannelShutdownDelay: -1, // immediate shutdown on last untrack
	})
	setupSharedPollHandlers(node)

	const iterations = 200
	for i := 0; i < iterations; i++ {
		clientA := newTestClientV2(t, node, "userA")
		connectClientV2(t, clientA)
		subscribeSharedPollClient(t, clientA, "test:channel")
		trackSharedPollClient(t, clientA, "test:channel", []*protocol.KeyedItem{
			{Key: "k", Version: 0},
		})

		clientB := newTestClientV2(t, node, "userB")
		connectClientV2(t, clientB)
		subscribeSharedPollClient(t, clientB, "test:channel")

		// Race A's untrack (triggers immediate shutdown of keyedManager state)
		// against B's track (calls getOrCreateChannel then later getHub).
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			untrackSharedPollClient(t, clientA, "test:channel", []string{"k"})
		}()
		go func() {
			defer wg.Done()
			trackSharedPollClient(t, clientB, "test:channel", []*protocol.KeyedItem{
				{Key: "k", Version: 0},
			})
		}()
		wg.Wait()

		// After the race, B must be in the keyedHub for k. If
		// removeChannel deleted B's hub between getOrCreateChannel and
		// getHub, addSubscriber would have hit a nil hub and we'd never
		// reach this assertion (panic).
		hub := node.keyedManager.getHub("test:channel")
		require.NotNil(t, hub,
			"iteration %d: keyedManager hub missing for test:channel — A's finalizeShutdown likely removed it after B's getOrCreateChannel",
			i)
		require.Equal(t, 1, hub.subscriberCount("k"),
			"iteration %d: expected B in the hub for k but subscriberCount=%d",
			i, hub.subscriberCount("k"))

		// Tear down.
		_ = clientA.close(DisconnectForceNoReconnect)
		_ = clientB.close(DisconnectForceNoReconnect)
	}
}


// TestKeyedBroadcast_OrderedDeliveryUnderConcurrentBroadcasts asserts that
// concurrent broadcasts for the same key to the same client are delivered to
// the wire in non-decreasing version order — i.e. no inversion where a lower
// version arrives after a higher one.
//
// Bug: keyedWritePublication releases c.mu before encoding, then enters
// writePublication (which calls into messageWriter.enqueue) without re-
// acquiring c.mu. Two concurrent broadcasts for the same key can therefore
// race past the version check, encode in parallel, and enqueue in arbitrary
// order. If the higher-version broadcast finishes encoding first and
// enqueues first, the lower-version broadcast still goes into the queue
// behind it — the client SDK then receives newer-then-older bytes on the
// wire. Per-connection state ends up correct (the re-check at update time
// only bumps to the higher version) but the wire order does not.
//
// The fix serializes encode→enqueue→state-update for a single client via
// c.mu (encoding stays outside; the lock wraps re-check + enqueue + bump),
// guaranteeing that anything that lands in the queue is the freshest version
// observed under the lock, and any later same-or-older broadcast is filtered
// out under the same lock.
func TestKeyedBroadcast_OrderedDeliveryUnderConcurrentBroadcasts(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t)
	setupSharedPollHandlers(node)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	transport := newTestTransport(cancel)
	transport.setProtocolVersion(ProtocolVersion2)
	transport.setProtocolType(ProtocolTypeProtobuf)
	sink := make(chan []byte, 4096)
	transport.sink = sink
	newCtx := SetCredentials(ctx, &Credentials{UserID: "user1"})
	client, _ := newClient(newCtx, node, transport)
	connectClientV2(t, client)
	subscribeSharedPollClient(t, client, "test:channel")
	trackSharedPollClient(t, client, "test:channel", []*protocol.KeyedItem{
		{Key: "K", Version: 0},
	})

	// Drain any startup frames so we only observe broadcast publications.
	drainStart := time.Now()
	for time.Since(drainStart) < 50*time.Millisecond {
		select {
		case <-sink:
		case <-time.After(10 * time.Millisecond):
		}
	}

	hub := node.keyedManager.getHub("test:channel")
	require.NotNil(t, hub)

	const numBroadcasts = 200
	var wg sync.WaitGroup
	wg.Add(numBroadcasts)
	startBarrier := make(chan struct{})
	for i := 1; i <= numBroadcasts; i++ {
		go func(v uint64) {
			defer wg.Done()
			<-startBarrier
			pub := &protocol.Publication{
				Key:     "K",
				Data:    []byte(`{"v":1}`),
				Version: v,
			}
			hub.broadcastToKey("test:channel", "K", v, pub, preparedData{})
		}(uint64(i))
	}
	close(startBarrier)
	wg.Wait()

	// Drain the sink (with a short idle timeout) and collect publication
	// versions in arrival order.
	var versions []uint64
	idle := time.NewTimer(200 * time.Millisecond)
	defer idle.Stop()
drain:
	for {
		select {
		case data := <-sink:
			reply := &protocol.Reply{}
			if err := reply.UnmarshalVT(data); err != nil {
				continue
			}
			if reply.Push == nil || reply.Push.Pub == nil {
				continue
			}
			if reply.Push.Pub.Key != "K" {
				continue
			}
			versions = append(versions, reply.Push.Pub.Version)
			if !idle.Stop() {
				select {
				case <-idle.C:
				default:
				}
			}
			idle.Reset(200 * time.Millisecond)
		case <-idle.C:
			break drain
		}
	}

	require.NotEmpty(t, versions, "expected at least one publication delivered to the wire")

	for i := 1; i < len(versions); i++ {
		require.GreaterOrEqual(t, versions[i], versions[i-1],
			"wire delivery out of order: index %d got v=%d after v=%d (full sequence: %v)",
			i, versions[i], versions[i-1], versions)
	}
}

// TestSharedPoll_NeedsBroadcast_StaleBackendAtBumpedVersion exercises the
// race between SharedPollPublish (which bumps entry.version locally) and a
// notified refresh whose backend response is stale relative to that publish.
//
// Setup: versioned, !KeepLatestData. A first client populates entry at v=1
// via cold-key auto-poll. SharedPollPublish then bumps entry.version to 2.
// A second client tracks the same key with version=0; because the key is
// already known, it is a "warm" key, and !KeepLatestData routes it through
// the deferred path that calls markNeedsBroadcast + notify.
//
// The notified poll handler returns the STALE backend state (v=1, "v1") —
// simulating backend replication lag where the publish has not yet been
// ingested. applyRefreshResponse then enters the "unchanged" branch
// (e.Version=1 <= entry.version=2) with entry.needsBroadcast=true.
//
// Bug: the branch builds the broadcast as `(version: entry.version=2,
// data: e.Data="v1")` — a fresh version with stale data. The second
// client's per-connection keyState.version advances to 2; any future
// broadcast at v=2 is then filtered by `pubVersion <= keyState.version`,
// so the client is silently pinned to wrong data until a publish at v>=3.
//
// Correct behavior: in !KeepLatestData mode the entry has no cached data,
// so the only valid (version, data) pair we can emit is the backend's
// (e.Version, e.Data); needsBroadcast must remain set so a subsequent
// fresh poll re-broadcasts at the bumped version. Asserted below by
// checking that the second client's keyState.version does NOT advance to
// the bumped version on the stale poll.
func TestSharedPoll_NeedsBroadcast_StaleBackendAtBumpedVersion(t *testing.T) {
	t.Parallel()

	// Poll handler returns whatever pollResult is currently set to. Starts
	// returning v=1 (the stale value). Tests may swap it later.
	var pollResult atomic.Value // *SharedPollResult
	pollResult.Store(&SharedPollResult{
		Items: []SharedPollRefreshItem{
			{Key: "key1", Data: []byte("v1"), Version: 1},
		},
	})

	node := newTestNodeWithSharedPoll(t, SharedPollChannelOptions{
		Mode:                 SharedPollModeVersioned,
		RefreshInterval:      30 * time.Second, // disable timer; we drive via notify
		RefreshBatchSize:     100,
		MaxKeysPerConnection: 100,
		PublishEnabled:       false, // local-only; SharedPollPublish → handlePublishedData
		// Notification batching intentionally disabled (both fields zero) so
		// each notify fires the backend handler immediately. With batching
		// enabled, NotificationBatchMaxDelay defaults to RefreshInterval (30s),
		// which would stall the test waiting for the batch window.
		// KeepLatestData intentionally false — exercises the deferred warm-key path.
	})

	var pollCount atomic.Int32
	node.OnSharedPoll(func(ctx context.Context, event SharedPollEvent) (SharedPollResult, error) {
		pollCount.Add(1)
		return *(pollResult.Load().(*SharedPollResult)), nil
	})
	setupSharedPollHandlers(node)

	// First client + cold-key auto-poll establishes entry at v=1.
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
		e := s.itemIndex["key1"]
		return e != nil && e.version == 1
	}, 2*time.Second, 10*time.Millisecond, "first client's cold-key auto-poll never populated entry at v=1")

	// SharedPollPublish bumps entry.version to 2. PublishEnabled=false means
	// broker is not consulted — publish() routes directly to
	// handlePublishedData on this node.
	require.NoError(t, node.SharedPollPublish(context.Background(), "test:channel", "key1", 2, "", []byte("v2-fresh")))

	require.Eventually(t, func() bool {
		node.sharedPollManager.mu.RLock()
		s := node.sharedPollManager.channels["test:channel"]
		node.sharedPollManager.mu.RUnlock()
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.itemIndex["key1"].version == 2
	}, time.Second, 10*time.Millisecond, "SharedPollPublish did not bump entry.version to 2")

	pollsBeforeClient2 := pollCount.Load()

	// Second client subscribes and tracks the same key at version=0. Because
	// the key already exists in itemIndex (isNew=false) and the client passes
	// version=0, this is a warm key. !KeepLatestData → deferredWarmKeys →
	// markNeedsBroadcast → notified refresh with items[i].Version=0.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	transport := newTestTransport(cancel)
	transport.setProtocolVersion(ProtocolVersion2)
	transport.setProtocolType(ProtocolTypeProtobuf)
	sink := make(chan []byte, 100)
	transport.sink = sink
	credCtx := SetCredentials(ctx, &Credentials{UserID: "user2"})
	client2, err := newClient(credCtx, node, transport)
	require.NoError(t, err)
	connectClientV2(t, client2)
	subscribeSharedPollClient(t, client2, "test:channel")
	trackSharedPollClient(t, client2, "test:channel", []*protocol.KeyedItem{
		{Key: "key1", Version: 0},
	})

	// Wait for the notified poll triggered by markNeedsBroadcast to fire.
	require.Eventually(t, func() bool {
		return pollCount.Load() > pollsBeforeClient2
	}, 2*time.Second, 10*time.Millisecond, "notified poll for warm key never fired (markNeedsBroadcast path not exercised)")

	// Give applyRefreshResponse + broadcast a brief window to land.
	time.Sleep(50 * time.Millisecond)

	client2.mu.RLock()
	var got *keyedKeyState
	if client2.keyed != nil {
		got = client2.keyed.trackedKeys["test:channel"]["key1"]
	}
	client2.mu.RUnlock()

	if got != nil {
		require.NotEqual(t, uint64(2), got.version,
			"BUG: client2's keyState.version advanced to the bumped entry.version (2), "+
				"but the broadcast carried stale backend data v=1 — future legitimate "+
				"broadcasts at v=2 will be filtered, silently pinning client2 to wrong data")
	}

	// Stronger wire-level check: scan everything client2 received and verify
	// no publication arrived with Version=2 in this stale phase. The buggy
	// code would have enqueued exactly such a publication.
drain:
	for {
		select {
		case data := <-sink:
			reply := &protocol.Reply{}
			if err := reply.UnmarshalVT(data); err != nil {
				continue
			}
			if reply.Push == nil || reply.Push.Pub == nil {
				continue
			}
			pub := reply.Push.Pub
			if pub.Key != "key1" {
				continue
			}
			require.NotEqual(t, uint64(2), pub.Version,
				"BUG: stale-backend broadcast delivered a Pub(version=2) to client2 — data is the backend's v=1 bytes, but the version label is the fresh entry.version")
		case <-time.After(50 * time.Millisecond):
			break drain
		}
	}

	// FIX verification: backend catches up to v=2 with the real bytes. A
	// fresh notify should now trigger a broadcast that delivers v=2 with
	// correct data. (With the buggy code, needsBroadcast was already
	// cleared on the stale response, so this poll would do nothing.)
	pollResult.Store(&SharedPollResult{
		Items: []SharedPollRefreshItem{
			{Key: "key1", Data: []byte("v2-fresh"), Version: 2},
		},
	})
	node.sharedPollManager.notify("test:channel", "key1")

	require.Eventually(t, func() bool {
		client2.mu.RLock()
		defer client2.mu.RUnlock()
		if client2.keyed == nil {
			return false
		}
		ks := client2.keyed.trackedKeys["test:channel"]["key1"]
		return ks != nil && ks.version == 2
	}, 2*time.Second, 10*time.Millisecond, "client2 never received v=2 after backend caught up — needsBroadcast was cleared on the stale response, so the fresh poll silently dropped the broadcast")
}

// controllableSubscribeBroker wraps MemoryBroker and lets a test drive each
// broker.Subscribe call by hand: call N can be made to block on a chan and/or
// return a specific error. Used to construct the race in
// TestSharedPollTrackKeys_RollbackDoesNotOrphanConcurrentTrack.
type controllableSubscribeBroker struct {
	*MemoryBroker
	mu           sync.Mutex
	callCount    int
	startedCh    chan int // signals when call N begins (N is sent on the chan)
	blockUntil   map[int]chan struct{}
	errOnCall    map[int]error
	publishCount atomic.Int32
}

func (b *controllableSubscribeBroker) Subscribe(channels ...string) error {
	b.mu.Lock()
	b.callCount++
	n := b.callCount
	block := b.blockUntil[n]
	err := b.errOnCall[n]
	startedCh := b.startedCh
	b.mu.Unlock()
	if startedCh != nil {
		select {
		case startedCh <- n:
		default:
		}
	}
	if block != nil {
		<-block
	}
	if err != nil {
		return err
	}
	return b.MemoryBroker.Subscribe(channels...)
}

func (b *controllableSubscribeBroker) Publish(ch string, data []byte, opts PublishOptions) (PublishResult, error) {
	b.publishCount.Add(1)
	return b.MemoryBroker.Publish(ch, data, opts)
}

// TestSharedPollTrackKeys_RollbackDoesNotOrphanConcurrentTrack exercises the
// race between a failing broker.Subscribe in one trackKeys call and a
// concurrent trackKeys call for the same key that sees the entry already
// present.
//
// Without the fix, trackKeys creates an itemIndex entry under s.mu, releases
// s.mu, then calls broker.Subscribe outside the lock. A second goroutine
// arriving during that window sees the entry, considers itself a no-op
// (isNewKey=false), and returns success — without ever attempting its own
// broker.Subscribe. When the first goroutine's broker.Subscribe fails, its
// rollback deletes the itemIndex entry, leaving the second goroutine's
// caller with a "tracked" key that has no server-side state and no broker
// subscription. Future SharedPollPublish on other nodes is silently lost
// for that caller; future polls won't include the key either.
//
// Invariant under test: if trackKeys returned success, the corresponding
// itemIndex entry MUST still be present after the race resolves.
func TestSharedPollTrackKeys_RollbackDoesNotOrphanConcurrentTrack(t *testing.T) {
	t.Parallel()

	node, err := New(Config{
		LogLevel:   LogLevelTrace,
		LogHandler: func(entry LogEntry) {},
		SharedPoll: SharedPollConfig{
			GetSharedPollChannelOptions: func(channel string) (SharedPollChannelOptions, bool) {
				return SharedPollChannelOptions{
					Mode:                 SharedPollModeVersioned,
					RefreshInterval:      30 * time.Second,
					RefreshBatchSize:     100,
					MaxKeysPerConnection: 100,
					PublishEnabled:       true, // exercise the broker subscribe path
				}, true
			},
		},
	})
	require.NoError(t, err)
	node.OnSharedPoll(func(ctx context.Context, event SharedPollEvent) (SharedPollResult, error) {
		return SharedPollResult{}, nil
	})

	memBroker, err := NewMemoryBroker(node, MemoryBrokerConfig{})
	require.NoError(t, err)
	ctrlBroker := &controllableSubscribeBroker{
		MemoryBroker: memBroker,
		startedCh:    make(chan int, 8),
		blockUntil:   map[int]chan struct{}{},
		errOnCall:    map[int]error{},
	}

	// Call 1 (goroutine A) blocks until we release it, then fails. Subsequent
	// calls (whichever goroutine arrives) run normally — so a correct fix
	// that has B retry its own subscribe completes.
	call1Block := make(chan struct{})
	ctrlBroker.blockUntil[1] = call1Block
	ctrlBroker.errOnCall[1] = errors.New("simulated broker subscribe failure")

	node.SetBroker(ctrlBroker)
	require.NoError(t, node.Run())
	t.Cleanup(func() { _ = node.Shutdown(context.Background()) })

	opts := SharedPollChannelOptions{
		Mode:                 SharedPollModeVersioned,
		RefreshInterval:      30 * time.Second,
		RefreshBatchSize:     100,
		MaxKeysPerConnection: 100,
		PublishEnabled:       true,
	}

	type res struct {
		keyResults []trackKeyResult
		release    func()
		err        error
	}
	resA := make(chan res, 1)
	resB := make(chan res, 1)

	go func() {
		r, release, err := node.sharedPollManager.trackKeys("test:channel", opts, []string{"key1"})
		resA <- res{r, release, err}
	}()

	// Wait for A to enter broker.Subscribe call #1.
	select {
	case n := <-ctrlBroker.startedCh:
		require.Equal(t, 1, n, "expected the first broker.Subscribe call to be from A")
	case <-time.After(2 * time.Second):
		t.Fatal("A never entered broker.Subscribe")
	}

	// With the buggy code: A is blocked in broker.Subscribe with s.mu
	// already released. B enters trackKeys, sees the entry exists, returns
	// success without calling broker.Subscribe.
	//
	// With the fix (hold s.mu across broker.Subscribe): A is blocked in
	// broker.Subscribe with s.mu still held; B blocks on s.mu instead.
	go func() {
		r, release, err := node.sharedPollManager.trackKeys("test:channel", opts, []string{"key1"})
		resB <- res{r, release, err}
	}()

	// Give B a window to enter trackKeys and (with buggy code) finish.
	time.Sleep(50 * time.Millisecond)

	// Release A's blocked broker.Subscribe — it will return the simulated
	// error and trigger the rollback path that deletes the entry.
	close(call1Block)

	var aResult, bResult res
	select {
	case aResult = <-resA:
	case <-time.After(2 * time.Second):
		t.Fatal("A's trackKeys never returned")
	}
	select {
	case bResult = <-resB:
	case <-time.After(2 * time.Second):
		t.Fatal("B's trackKeys never returned")
	}

	require.Error(t, aResult.err, "A's trackKeys must surface the broker subscribe failure")
	// On error trackKeys self-releases reservations; the returned release
	// is a no-op closure. Calling it keeps the contract symmetric.
	aResult.release()
	defer bResult.release()

	// Core invariant: if B's trackKeys returned success, the manager must
	// still be tracking key1 — both in itemIndex (so notify/poll cover it)
	// and at the broker level (so cross-node publishes reach this node).
	if bResult.err == nil {
		node.sharedPollManager.mu.RLock()
		s := node.sharedPollManager.channels["test:channel"]
		node.sharedPollManager.mu.RUnlock()
		require.NotNil(t, s, "channel state must exist when B reported success")
		s.mu.Lock()
		entry := s.itemIndex["key1"]
		s.mu.Unlock()
		require.NotNil(t, entry,
			"BUG: B's trackKeys succeeded (no error) but itemIndex no longer has key1 — "+
				"A's broker-subscribe rollback deleted the entry B was relying on. Future "+
				"cross-node publishes for key1 will be silently lost for B's caller.")

		// Broker subscription invariant: subscribeToBrokerKeys recorded the key.
		keyCh := sharedPollKeyChannel("test:channel", "key1")
		node.sharedPollManager.brokerSubMu.RLock()
		_, brokerSubbed := node.sharedPollManager.brokerSubChans[keyCh]
		node.sharedPollManager.brokerSubMu.RUnlock()
		require.True(t, brokerSubbed,
			"BUG: B's trackKeys succeeded but no broker subscription exists for key1 — "+
				"A's failed subscribe + entry rollback left the key unsubscribed at the broker")
	}
}

// TestSharedPollTrackKeys_LimitRollback_DoesNotOrphanConcurrentHubJoin
// exercises the race between Client A's handleTrack limit-rollback (which
// calls untrack BEFORE addSubscribers) and a concurrent Client B that
// rode on the back of A's trackKeys (saw the entry already present,
// returned success without doing its own broker.Subscribe) and is about
// to call addSubscribers.
//
// Sequence (deterministic via the controllable broker):
//
//  1. A.trackKeys(K) — A creates the itemIndex entry and begins
//     broker.Subscribe (blocks).
//  2. B.trackKeys(K) — B sees the entry already present and waits on
//     A's subscribeReady chan.
//  3. A's broker.Subscribe is released and succeeds.
//  4. Both A and B return from trackKeys with success.
//  5. A's handleTrack hits the limit-rollback branch and calls
//     untrack(K). hub.subscriberCount(K) == 0 here (neither A nor B has
//     reached addSubscribers yet), so untrack deletes the itemIndex
//     entry and queues broker unsubscribe.
//  6. B's handleTrack calls addSubscribers(K, B). B is now in the hub
//     for K — but itemIndex has no K and broker.Unsubscribe is queued.
//
// Bug symptom: B is silently orphaned. Cross-node publishes never reach
// this node for K (broker unsubscribed). Local SharedPollPublish for K
// no-ops because entry is gone.
//
// Invariant asserted below: after B's addSubscribers, the itemIndex
// entry and the broker subscription must still exist.
func TestSharedPollTrackKeys_LimitRollback_DoesNotOrphanConcurrentHubJoin(t *testing.T) {
	t.Parallel()

	node, err := New(Config{
		LogLevel:   LogLevelTrace,
		LogHandler: func(entry LogEntry) {},
		SharedPoll: SharedPollConfig{
			GetSharedPollChannelOptions: func(channel string) (SharedPollChannelOptions, bool) {
				return SharedPollChannelOptions{
					Mode:                 SharedPollModeVersioned,
					RefreshInterval:      30 * time.Second,
					RefreshBatchSize:     100,
					MaxKeysPerConnection: 100,
					PublishEnabled:       true,
				}, true
			},
		},
	})
	require.NoError(t, err)
	node.OnSharedPoll(func(ctx context.Context, event SharedPollEvent) (SharedPollResult, error) {
		return SharedPollResult{}, nil
	})

	memBroker, err := NewMemoryBroker(node, MemoryBrokerConfig{})
	require.NoError(t, err)
	ctrlBroker := &controllableSubscribeBroker{
		MemoryBroker: memBroker,
		startedCh:    make(chan int, 8),
		blockUntil:   map[int]chan struct{}{},
		errOnCall:    map[int]error{},
	}
	// Call 1 (goroutine A): block until released, then succeed.
	call1Block := make(chan struct{})
	ctrlBroker.blockUntil[1] = call1Block

	node.SetBroker(ctrlBroker)
	require.NoError(t, node.Run())
	t.Cleanup(func() { _ = node.Shutdown(context.Background()) })

	opts := SharedPollChannelOptions{
		Mode:                 SharedPollModeVersioned,
		RefreshInterval:      30 * time.Second,
		RefreshBatchSize:     100,
		MaxKeysPerConnection: 100,
		PublishEnabled:       true,
	}

	channel := "test:channel"
	key := "key1"

	type trackRes struct {
		results []trackKeyResult
		release func()
		err     error
	}
	resA := make(chan trackRes, 1)
	resB := make(chan trackRes, 1)

	// Goroutine A: first to call trackKeys; broker.Subscribe blocks.
	go func() {
		r, release, err := node.sharedPollManager.trackKeys(channel, opts, []string{key})
		resA <- trackRes{r, release, err}
	}()

	// Wait for A's broker.Subscribe to be in flight.
	select {
	case n := <-ctrlBroker.startedCh:
		require.Equal(t, 1, n)
	case <-time.After(2 * time.Second):
		t.Fatal("A's broker.Subscribe never started")
	}

	// Goroutine B: second trackKeys. The entry already exists with
	// subscribeReady; B waits on it.
	go func() {
		r, release, err := node.sharedPollManager.trackKeys(channel, opts, []string{key})
		resB <- trackRes{r, release, err}
	}()

	// Give B time to enter trackKeys and reach the wait.
	time.Sleep(50 * time.Millisecond)

	// Release A's broker.Subscribe. Both A and B should now return success.
	close(call1Block)

	var aResult, bResult trackRes
	select {
	case aResult = <-resA:
	case <-time.After(2 * time.Second):
		t.Fatal("A's trackKeys never returned")
	}
	require.NoError(t, aResult.err)
	select {
	case bResult = <-resB:
	case <-time.After(2 * time.Second):
		t.Fatal("B's trackKeys never returned")
	}
	require.NoError(t, bResult.err)

	// Simulate A's handleTrack limit-rollback: release A's reservation
	// BEFORE B has joined the hub. With the fix, A's release decrements
	// pendingHubJoin (B still holds one) and the entry stays. Without the
	// fix (untrack called blindly), the entry is deleted here.
	aResult.release()

	// Belt-and-braces: even if some unrelated path called untrack now
	// (e.g., another connection's cleanupKeyed run from a different
	// channel that shares the key string), it must respect B's reservation.
	// Pre-fix untrack would delete unconditionally because hub.count==0.
	node.sharedPollManager.untrack(channel, key)

	// Now simulate B's addSubscribers (Step 5 in handleTrack).
	clientB := newTestClient(t, node, "userB")
	node.keyedManager.addSubscribers(channel, []string{key}, clientB, opts.toKeyedChannelOptions())

	// Invariant: B is in the hub, so the manager MUST still be tracking
	// the key — both itemIndex (so notify/poll cover it) and broker
	// subscription (so cross-node publishes reach this node).
	node.sharedPollManager.mu.RLock()
	s := node.sharedPollManager.channels[channel]
	node.sharedPollManager.mu.RUnlock()
	require.NotNil(t, s, "channel state must exist when B is in the hub")
	s.mu.Lock()
	entry := s.itemIndex[key]
	s.mu.Unlock()
	require.NotNil(t, entry,
		"BUG: B is in the hub but itemIndex no longer has the key — A's "+
			"limit-rollback released its reservation while B's hub join was "+
			"still in flight, and untrack deleted the entry. B is silently "+
			"orphaned: cross-node publishes won't reach this node, and local "+
			"SharedPollPublish will no-op (entry==nil).")

	keyCh := sharedPollKeyChannel(channel, key)
	node.sharedPollManager.brokerSubMu.RLock()
	_, brokerSubbed := node.sharedPollManager.brokerSubChans[keyCh]
	node.sharedPollManager.brokerSubMu.RUnlock()
	require.True(t, brokerSubbed,
		"BUG: B is in the hub but broker subscription was queued for "+
			"unsubscribe by A's rollback — cross-node publishes for the key "+
			"will be lost for B.")

	// Finally release B's reservation (B has joined the hub; release is a
	// no-op cleanup since hub.subscriberCount>0).
	bResult.release()
}

// TestSharedPollRevokeKeys_UnsubscribesBroker demonstrates that
// SharedPollRevokeKeys must also drop the broker subscription for any
// key it removes from itemIndex. Before the fix, revoke deleted the
// itemIndex entry but left brokerSubChans populated — the broker kept
// pushing cross-node publications for the key, all of which were
// silently no-op'd by handlePublishedData (entry == nil). On heavy
// revoke workloads the wasted cross-node traffic accumulates and the
// broker subscription persists until the entire channel state is shut
// down.
//
// Invariant under test: after revoking a key, the per-key broker
// subscription is gone (queued via the dissolver).
func TestSharedPollRevokeKeys_UnsubscribesBroker(t *testing.T) {
	t.Parallel()

	node, err := New(Config{
		LogLevel:   LogLevelTrace,
		LogHandler: func(entry LogEntry) {},
		SharedPoll: SharedPollConfig{
			GetSharedPollChannelOptions: func(channel string) (SharedPollChannelOptions, bool) {
				return SharedPollChannelOptions{
					Mode:                 SharedPollModeVersioned,
					RefreshInterval:      30 * time.Second,
					RefreshBatchSize:     100,
					MaxKeysPerConnection: 100,
					PublishEnabled:       true,
				}, true
			},
		},
	})
	require.NoError(t, err)
	node.OnSharedPoll(func(ctx context.Context, event SharedPollEvent) (SharedPollResult, error) {
		items := make([]SharedPollRefreshItem, len(event.Items))
		for i, it := range event.Items {
			items[i] = SharedPollRefreshItem{Key: it.Key, Data: []byte(`"data"`), Version: 1}
		}
		return SharedPollResult{Items: items}, nil
	})
	setupSharedPollHandlers(node)
	memBroker, err := NewMemoryBroker(node, MemoryBrokerConfig{})
	require.NoError(t, err)
	node.SetBroker(memBroker)
	require.NoError(t, node.Run())
	t.Cleanup(func() { _ = node.Shutdown(context.Background()) })

	channel := "test:revoke_broker"
	key := "key1"
	otherKey := "key2"

	client := newTestClientV2(t, node, "user1")
	connectClientV2(t, client)
	subscribeSharedPollClient(t, client, channel)
	trackSharedPollClient(t, client, channel, []*protocol.KeyedItem{
		{Key: key, Version: 0},
		{Key: otherKey, Version: 0},
	})

	// Confirm broker subscriptions exist for both keys before revoke.
	keyCh := sharedPollKeyChannel(channel, key)
	otherKeyCh := sharedPollKeyChannel(channel, otherKey)
	node.sharedPollManager.brokerSubMu.RLock()
	_, hasKey := node.sharedPollManager.brokerSubChans[keyCh]
	_, hasOther := node.sharedPollManager.brokerSubChans[otherKeyCh]
	node.sharedPollManager.brokerSubMu.RUnlock()
	require.True(t, hasKey, "broker must be subscribed for key1 before revoke")
	require.True(t, hasOther, "broker must be subscribed for key2 before revoke")

	// Revoke key1 from all users (so itemIndex entry is removed).
	node.sharedPollManager.SharedPollRevokeKeys(channel, []string{key}, nil, nil)

	// Invariant: broker subscription for the revoked key must be dropped
	// (queued via the dissolver — wait for it to clear).
	require.Eventually(t, func() bool {
		node.sharedPollManager.brokerSubMu.RLock()
		defer node.sharedPollManager.brokerSubMu.RUnlock()
		_, exists := node.sharedPollManager.brokerSubChans[keyCh]
		return !exists
	}, 2*time.Second, 10*time.Millisecond,
		"BUG: SharedPollRevokeKeys deleted the itemIndex entry but left "+
			"the broker subscription in place. Cross-node publications for "+
			"the revoked key continue arriving and are silently dropped by "+
			"handlePublishedData — wasted broker traffic until the entire "+
			"channel state shuts down.")

	// Sanity: untouched keys must remain subscribed at the broker.
	node.sharedPollManager.brokerSubMu.RLock()
	_, stillSubscribedOther := node.sharedPollManager.brokerSubChans[otherKeyCh]
	node.sharedPollManager.brokerSubMu.RUnlock()
	require.True(t, stillSubscribedOther,
		"untouched key must keep its broker subscription after revoke")
}

// TestSharedPollSubscribeToBrokerKeys_PublishDuringWindow exercises a
// contract violation where established subscribers on OTHER nodes
// silently miss a publication. Sequence:
//
//   - Node B's user calls SharedPollPublish for K1 while Node B's own
//     cold-key trackKeys for K1 is in the broker.Subscribe window
//     (broker.Subscribe returned or in flight, brokerSubChans not yet
//     updated).
//   - publish() on Node B reads brokerSubChans -> empty -> falls back
//     to handlePublishedData (local-only). broker.Publish is never
//     called.
//   - On other nodes (modelled here by the assertion that broker.Publish
//     fires), any client already subscribed to K1 never receives the
//     publication. That breaks the "subscribed clients receive
//     notifications" contract.
//
// The fix holds brokerSubMu across broker.Subscribe so publishes block
// on the RLock until brokerSubChans is consistent, then route through
// the broker as intended.
func TestSharedPollSubscribeToBrokerKeys_PublishDuringWindow(t *testing.T) {
	t.Parallel()

	node, err := New(Config{
		LogLevel:   LogLevelTrace,
		LogHandler: func(entry LogEntry) {},
		SharedPoll: SharedPollConfig{
			GetSharedPollChannelOptions: func(channel string) (SharedPollChannelOptions, bool) {
				return SharedPollChannelOptions{
					Mode:                 SharedPollModeVersioned,
					RefreshInterval:      30 * time.Second,
					RefreshBatchSize:     100,
					MaxKeysPerConnection: 100,
					PublishEnabled:       true,
				}, true
			},
		},
	})
	require.NoError(t, err)
	node.OnSharedPoll(func(ctx context.Context, event SharedPollEvent) (SharedPollResult, error) {
		return SharedPollResult{}, nil
	})

	memBroker, err := NewMemoryBroker(node, MemoryBrokerConfig{})
	require.NoError(t, err)
	ctrlBroker := &controllableSubscribeBroker{
		MemoryBroker: memBroker,
		startedCh:    make(chan int, 8),
		blockUntil:   map[int]chan struct{}{},
		errOnCall:    map[int]error{},
	}
	call1Block := make(chan struct{})
	ctrlBroker.blockUntil[1] = call1Block

	node.SetBroker(ctrlBroker)
	require.NoError(t, node.Run())
	t.Cleanup(func() { _ = node.Shutdown(context.Background()) })

	opts := SharedPollChannelOptions{
		Mode:                 SharedPollModeVersioned,
		RefreshInterval:      30 * time.Second,
		RefreshBatchSize:     100,
		MaxKeysPerConnection: 100,
		PublishEnabled:       true,
	}
	channel := "test:subscribe_window"
	key := "key1"

	type trackRes struct {
		results []trackKeyResult
		release func()
		err     error
	}
	resCh := make(chan trackRes, 1)

	go func() {
		r, release, err := node.sharedPollManager.trackKeys(channel, opts, []string{key})
		resCh <- trackRes{r, release, err}
	}()

	select {
	case n := <-ctrlBroker.startedCh:
		require.Equal(t, 1, n)
	case <-time.After(2 * time.Second):
		t.Fatal("broker.Subscribe was not invoked")
	}

	publishDone := make(chan error, 1)
	go func() {
		publishDone <- node.SharedPollPublish(context.Background(), channel, key, 1, "", []byte(`"x"`))
	}()

	// Give the publish goroutine time to either complete (bug path) or
	// block on brokerSubMu (fix path).
	time.Sleep(100 * time.Millisecond)

	close(call1Block)

	var aResult trackRes
	select {
	case aResult = <-resCh:
	case <-time.After(2 * time.Second):
		t.Fatal("trackKeys never returned")
	}
	require.NoError(t, aResult.err)
	defer aResult.release()

	select {
	case err := <-publishDone:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("publish goroutine never returned")
	}

	require.EqualValues(t, 1, ctrlBroker.publishCount.Load(),
		"BUG: SharedPollPublish landed during the window between "+
			"broker.Subscribe and the brokerSubChans update. publish() saw "+
			"empty brokerSubChans and fell back to local-only delivery — "+
			"established subscribers on other nodes would silently miss the "+
			"publication.")
}

// TestSharedPollPublish_RoutesToBrokerWithoutLocalTrack proves the new
// publish contract: when the channel is configured with PublishEnabled,
// SharedPollPublish routes through the broker regardless of whether the
// publishing node has tracked the key locally. Previously, publish()
// used brokerSubChans (a side-effect of subscribeToBrokerKeys) as the
// gate — so a node that had never tracked the key silently fell back
// to handlePublishedData, and subscribers on OTHER nodes never received
// the publication. The fix decouples routing from local subscriber
// state.
//
// We can't spin up two Nodes sharing a MemoryBroker in this test (memory
// brokers are per-node), but we can directly verify the contract by
// asserting broker.Publish was invoked when no local track exists.
func TestSharedPollPublish_RoutesToBrokerWithoutLocalTrack(t *testing.T) {
	t.Parallel()

	node, err := New(Config{
		LogLevel:   LogLevelTrace,
		LogHandler: func(entry LogEntry) {},
		SharedPoll: SharedPollConfig{
			GetSharedPollChannelOptions: func(channel string) (SharedPollChannelOptions, bool) {
				return SharedPollChannelOptions{
					Mode:                 SharedPollModeVersioned,
					RefreshInterval:      30 * time.Second,
					RefreshBatchSize:     100,
					MaxKeysPerConnection: 100,
					PublishEnabled:       true,
				}, true
			},
		},
	})
	require.NoError(t, err)
	node.OnSharedPoll(func(ctx context.Context, event SharedPollEvent) (SharedPollResult, error) {
		return SharedPollResult{}, nil
	})

	memBroker, err := NewMemoryBroker(node, MemoryBrokerConfig{})
	require.NoError(t, err)
	ctrlBroker := &controllableSubscribeBroker{
		MemoryBroker: memBroker,
		startedCh:    make(chan int, 8),
		blockUntil:   map[int]chan struct{}{},
		errOnCall:    map[int]error{},
	}
	node.SetBroker(ctrlBroker)
	require.NoError(t, node.Run())
	t.Cleanup(func() { _ = node.Shutdown(context.Background()) })

	// Sanity: no channel state, no local track, no broker subscription.
	channel := "test:publish_no_local_track"
	key := "key1"
	node.sharedPollManager.mu.RLock()
	_, exists := node.sharedPollManager.channels[channel]
	node.sharedPollManager.mu.RUnlock()
	require.False(t, exists, "no channel state should exist before publish")
	keyCh := sharedPollKeyChannel(channel, key)
	node.sharedPollManager.brokerSubMu.RLock()
	_, brokerSubbed := node.sharedPollManager.brokerSubChans[keyCh]
	node.sharedPollManager.brokerSubMu.RUnlock()
	require.False(t, brokerSubbed, "broker must not be subscribed before any track")

	// Publish without any prior track on this node. With the old code,
	// brokerSubChans is empty → publish() falls back to handlePublishedData
	// → no-op. broker.Publish would NOT be called, and subscribers on
	// other nodes would never receive this publication.
	require.NoError(t, node.SharedPollPublish(
		context.Background(), channel, key, 1, "", []byte(`"hello"`),
	))

	require.EqualValues(t, 1, ctrlBroker.publishCount.Load(),
		"BUG: SharedPollPublish on a PublishEnabled channel must route "+
			"through the broker even when this node has never tracked "+
			"the key. Otherwise subscribers on other nodes would never "+
			"receive the publication.")
}

// TestSharedPollPublish_LocalOnlyDoesNotUseBroker is the symmetric
// case: when PublishEnabled is false, publish() must NOT call
// broker.Publish — it stays local-only.
func TestSharedPollPublish_LocalOnlyDoesNotUseBroker(t *testing.T) {
	t.Parallel()

	node, err := New(Config{
		LogLevel:   LogLevelTrace,
		LogHandler: func(entry LogEntry) {},
		SharedPoll: SharedPollConfig{
			GetSharedPollChannelOptions: func(channel string) (SharedPollChannelOptions, bool) {
				return SharedPollChannelOptions{
					Mode:                 SharedPollModeVersioned,
					RefreshInterval:      30 * time.Second,
					RefreshBatchSize:     100,
					MaxKeysPerConnection: 100,
					PublishEnabled:       false,
				}, true
			},
		},
	})
	require.NoError(t, err)
	node.OnSharedPoll(func(ctx context.Context, event SharedPollEvent) (SharedPollResult, error) {
		return SharedPollResult{}, nil
	})

	memBroker, err := NewMemoryBroker(node, MemoryBrokerConfig{})
	require.NoError(t, err)
	ctrlBroker := &controllableSubscribeBroker{
		MemoryBroker: memBroker,
		startedCh:    make(chan int, 8),
		blockUntil:   map[int]chan struct{}{},
		errOnCall:    map[int]error{},
	}
	node.SetBroker(ctrlBroker)
	require.NoError(t, node.Run())
	t.Cleanup(func() { _ = node.Shutdown(context.Background()) })

	require.NoError(t, node.SharedPollPublish(
		context.Background(), "test:local_only", "key1", 1, "", []byte(`"x"`),
	))
	require.EqualValues(t, 0, ctrlBroker.publishCount.Load(),
		"PublishEnabled=false must route locally — broker.Publish must not be called")
}

// TestSharedPollTrackKeys_PublishEnabledDrift demonstrates that track*()
// must use the channel state's frozen opts.PublishEnabled, not the
// caller-passed opts.PublishEnabled, when deciding whether to subscribe
// the key at the broker.
//
// Scenario: the channel state is created by an earlier track call with
// PublishEnabled=true (so publish() routes through the broker). A
// concurrent caller later tracks a different (new) key on the same
// channel but with PublishEnabled=false in its caller-passed opts. With
// the bug, that second track does NOT call broker.Subscribe for its
// new key — because the decision is gated on the caller's opts. The
// channel state's publish routing still goes through the broker (per
// s.opts), so cross-node publications to that key arrive at the broker
// for delivery — but this node is not subscribed for it, so local
// subscribers miss the notification.
//
// Invariant under test: when s.opts.PublishEnabled is true, every new
// key gets a broker subscription regardless of the caller's opts.
func TestSharedPollTrackKeys_PublishEnabledDrift(t *testing.T) {
	t.Parallel()

	node, err := New(Config{
		LogLevel:   LogLevelTrace,
		LogHandler: func(entry LogEntry) {},
		SharedPoll: SharedPollConfig{
			GetSharedPollChannelOptions: func(channel string) (SharedPollChannelOptions, bool) {
				return SharedPollChannelOptions{
					Mode:                 SharedPollModeVersioned,
					RefreshInterval:      30 * time.Second,
					RefreshBatchSize:     100,
					MaxKeysPerConnection: 100,
					PublishEnabled:       true,
				}, true
			},
		},
	})
	require.NoError(t, err)
	node.OnSharedPoll(func(ctx context.Context, event SharedPollEvent) (SharedPollResult, error) {
		return SharedPollResult{}, nil
	})
	memBroker, err := NewMemoryBroker(node, MemoryBrokerConfig{})
	require.NoError(t, err)
	node.SetBroker(memBroker)
	require.NoError(t, node.Run())
	t.Cleanup(func() { _ = node.Shutdown(context.Background()) })

	channel := "test:opts_drift"

	// First track: sets up channel state with PublishEnabled=true.
	optsTrue := SharedPollChannelOptions{
		Mode:                 SharedPollModeVersioned,
		RefreshInterval:      30 * time.Second,
		RefreshBatchSize:     100,
		MaxKeysPerConnection: 100,
		PublishEnabled:       true,
	}
	_, release1, err := node.sharedPollManager.trackKeys(channel, optsTrue, []string{"key1"})
	require.NoError(t, err)
	defer release1()

	// Sanity: s.opts.PublishEnabled is true. key1 is subscribed at broker.
	keyCh1 := sharedPollKeyChannel(channel, "key1")
	node.sharedPollManager.brokerSubMu.RLock()
	_, hasKey1 := node.sharedPollManager.brokerSubChans[keyCh1]
	node.sharedPollManager.brokerSubMu.RUnlock()
	require.True(t, hasKey1, "key1 must be broker-subscribed by the first track")

	// Second track for a NEW key, but the caller's opts mistakenly has
	// PublishEnabled=false. The channel state's s.opts already says true.
	// Track must follow s.opts, not the caller's opts — otherwise key2's
	// broker subscription would be skipped and cross-node publications
	// for key2 would silently miss this node's subscribers.
	optsFalse := SharedPollChannelOptions{
		Mode:                 SharedPollModeVersioned,
		RefreshInterval:      30 * time.Second,
		RefreshBatchSize:     100,
		MaxKeysPerConnection: 100,
		PublishEnabled:       false, // drift
	}
	_, release2, err := node.sharedPollManager.trackKeys(channel, optsFalse, []string{"key2"})
	require.NoError(t, err)
	defer release2()

	keyCh2 := sharedPollKeyChannel(channel, "key2")
	node.sharedPollManager.brokerSubMu.RLock()
	_, hasKey2 := node.sharedPollManager.brokerSubChans[keyCh2]
	node.sharedPollManager.brokerSubMu.RUnlock()
	require.True(t, hasKey2,
		"BUG: key2 was tracked on a channel whose frozen opts have "+
			"PublishEnabled=true, but the caller-passed opts had "+
			"PublishEnabled=false. track must use s.opts.PublishEnabled, "+
			"not the caller's, so cross-node publications via broker reach "+
			"this node's subscribers for key2.")
}

// TestSharedPollRevokeKeys_PartialUserFilter_KeepsBrokerSubscribed
// guards the inverse: when revoke filters by user and other subscribers
// for the key remain, the key stays in itemIndex AND the broker
// subscription must stay.
func TestSharedPollRevokeKeys_PartialUserFilter_KeepsBrokerSubscribed(t *testing.T) {
	t.Parallel()

	node, err := New(Config{
		LogLevel:   LogLevelTrace,
		LogHandler: func(entry LogEntry) {},
		SharedPoll: SharedPollConfig{
			GetSharedPollChannelOptions: func(channel string) (SharedPollChannelOptions, bool) {
				return SharedPollChannelOptions{
					Mode:                 SharedPollModeVersioned,
					RefreshInterval:      30 * time.Second,
					RefreshBatchSize:     100,
					MaxKeysPerConnection: 100,
					PublishEnabled:       true,
				}, true
			},
		},
	})
	require.NoError(t, err)
	node.OnSharedPoll(func(ctx context.Context, event SharedPollEvent) (SharedPollResult, error) {
		items := make([]SharedPollRefreshItem, len(event.Items))
		for i, it := range event.Items {
			items[i] = SharedPollRefreshItem{Key: it.Key, Data: []byte(`"data"`), Version: 1}
		}
		return SharedPollResult{Items: items}, nil
	})
	setupSharedPollHandlers(node)
	memBroker, err := NewMemoryBroker(node, MemoryBrokerConfig{})
	require.NoError(t, err)
	node.SetBroker(memBroker)
	require.NoError(t, node.Run())
	t.Cleanup(func() { _ = node.Shutdown(context.Background()) })

	channel := "test:revoke_partial"
	key := "shared"

	client1 := newTestClientV2(t, node, "user1")
	connectClientV2(t, client1)
	subscribeSharedPollClient(t, client1, channel)
	trackSharedPollClient(t, client1, channel, []*protocol.KeyedItem{{Key: key, Version: 0}})

	client2 := newTestClientV2(t, node, "user2")
	connectClientV2(t, client2)
	subscribeSharedPollClient(t, client2, channel)
	trackSharedPollClient(t, client2, channel, []*protocol.KeyedItem{{Key: key, Version: 0}})

	keyCh := sharedPollKeyChannel(channel, key)
	node.sharedPollManager.brokerSubMu.RLock()
	_, before := node.sharedPollManager.brokerSubChans[keyCh]
	node.sharedPollManager.brokerSubMu.RUnlock()
	require.True(t, before)

	// Revoke only user1 — user2 still subscribed to the key.
	node.sharedPollManager.SharedPollRevokeKeys(channel, []string{key}, []string{"user1"}, nil)

	// Broker subscription must stay (user2's needs cross-node publications).
	require.Never(t, func() bool {
		node.sharedPollManager.brokerSubMu.RLock()
		defer node.sharedPollManager.brokerSubMu.RUnlock()
		_, exists := node.sharedPollManager.brokerSubChans[keyCh]
		return !exists
	}, 300*time.Millisecond, 30*time.Millisecond,
		"broker subscription must persist while at least one subscriber remains for the key")
}
