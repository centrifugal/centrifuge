package centrifuge

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/require"
)

// holdJoinBroker holds PublishJoin until released and records the order of
// joins and leaves which reached the broker.
type holdJoinBroker struct {
	*MemoryBroker
	joinEntered chan struct{}
	releaseJoin chan struct{}

	mu     sync.Mutex
	events []string
}

func (b *holdJoinBroker) PublishJoin(ch string, info *ClientInfo) error {
	b.joinEntered <- struct{}{}
	<-b.releaseJoin
	b.mu.Lock()
	b.events = append(b.events, "join")
	b.mu.Unlock()
	return b.MemoryBroker.PublishJoin(ch, info)
}

func (b *holdJoinBroker) PublishLeave(ch string, info *ClientInfo) error {
	b.mu.Lock()
	b.events = append(b.events, "leave")
	b.mu.Unlock()
	return b.MemoryBroker.PublishLeave(ch, info)
}

func (b *holdJoinBroker) recorded() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]string(nil), b.events...)
}

func newHoldJoinNode(t *testing.T) (*Node, *holdJoinBroker) {
	t.Helper()
	node, err := New(Config{
		LogLevel:   LogLevelError,
		LogHandler: func(entry LogEntry) {},
		Map: MapConfig{
			GetMapChannelOptions: func(channel string) MapChannelOptions {
				return MapChannelOptions{Mode: MapModeEphemeral, KeyTTL: time.Minute, MinPageSize: 1}
			},
		},
		SharedPoll: SharedPollConfig{
			GetSharedPollChannelOptions: func(channel string) (SharedPollChannelOptions, bool) {
				return SharedPollChannelOptions{RefreshInterval: time.Second, RefreshBatchSize: 100, MaxKeysPerConnection: 100}, strings.HasPrefix(channel, "sp:")
			},
		},
	})
	require.NoError(t, err)
	memBroker, err := NewMemoryBroker(node, MemoryBrokerConfig{})
	require.NoError(t, err)
	broker := &holdJoinBroker{
		MemoryBroker: memBroker,
		joinEntered:  make(chan struct{}, 1),
		releaseJoin:  make(chan struct{}),
	}
	node.SetBroker(broker)
	mapBroker, err := NewMemoryMapBroker(node, MemoryMapBrokerConfig{})
	require.NoError(t, err)
	require.NoError(t, mapBroker.RegisterEventHandler(nil))
	node.SetMapBroker(mapBroker)
	node.OnSharedPoll(func(ctx context.Context, event SharedPollEvent) (SharedPollResult, error) {
		return SharedPollResult{}, nil
	})
	require.NoError(t, node.Run())
	t.Cleanup(func() { _ = node.Shutdown(context.Background()) })
	return node, broker
}

// An unsubscribe which comes after the subscription went live but before its
// join was published publishes the leave after the join, and removes the
// presence and map client presence the subscribe added, for every way of
// subscribing.
func TestJoinLeaveOrder_UnsubscribeBeforeJoinPublished(t *testing.T) {
	t.Parallel()

	const channel = "join_leave_order"
	options := SubscribeOptions{EmitJoinLeave: true, EmitPresence: true}

	testCases := []struct {
		name      string
		subscribe func(t *testing.T, client *Client)
	}{
		{"connect", func(t *testing.T, client *Client) {
			connectClientV2(t, client)
		}},
		{"client", func(t *testing.T, client *Client) {
			rwWrapper := testReplyWriterWrapper()
			require.NoError(t, client.handleSubscribe(&protocol.SubscribeRequest{Channel: channel}, &protocol.Command{Id: 1}, time.Now(), rwWrapper.rw))
		}},
		{"server_side", func(t *testing.T, client *Client) {
			require.NoError(t, client.Subscribe(channel, WithEmitJoinLeave(true), WithEmitPresence(true), func(o *SubscribeOptions) {
				o.MapClientPresenceChannel = "clients:" + channel
			}))
		}},
		{"map", func(t *testing.T, client *Client) {
			rwWrapper := testReplyWriterWrapper()
			require.NoError(t, client.handleSubscribe(&protocol.SubscribeRequest{
				Channel: channel,
				Type:    int32(SubscriptionTypeMap),
				Phase:   MapPhaseState,
				Limit:   100,
			}, &protocol.Command{Id: 1}, time.Now(), rwWrapper.rw))
		}},
		{"shared_poll", func(t *testing.T, client *Client) {
			rwWrapper := testReplyWriterWrapper()
			require.NoError(t, client.handleSubscribe(&protocol.SubscribeRequest{
				Channel: "sp:" + channel,
				Type:    int32(SubscriptionTypeSharedPoll),
			}, &protocol.Command{Id: 1}, time.Now(), rwWrapper.rw))
		}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			node, broker := newHoldJoinNode(t)
			node.OnConnecting(func(ctx context.Context, e ConnectEvent) (ConnectReply, error) {
				if tc.name != "connect" {
					return ConnectReply{}, nil
				}
				opts := options
				opts.MapClientPresenceChannel = "clients:" + channel
				return ConnectReply{Subscriptions: map[string]SubscribeOptions{channel: opts}}, nil
			})
			node.OnConnect(func(client *Client) {
				client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
					opts := options
					opts.Type = e.Type
					opts.MapClientPresenceChannel = "clients:" + e.Channel
					cb(SubscribeReply{Options: opts}, nil)
				})
			})
			ch := channel
			if tc.name == "shared_poll" {
				ch = "sp:" + channel
			}
			client := newTestClientV2(t, node, "user1")
			if tc.name != "connect" {
				connectClientV2(t, client)
			}

			subscribed := make(chan struct{})
			go func() {
				defer close(subscribed)
				tc.subscribe(t, client)
			}()

			select {
			case <-broker.joinEntered:
			case <-time.After(5 * time.Second):
				require.Fail(t, "join not published")
			}
			// The subscription is live, its join is not published yet. A map
			// unsubscribe waits for the subscribe in progress, the others don't.
			require.True(t, client.IsSubscribed(ch))
			unsubscribed := make(chan struct{})
			go func() {
				defer close(unsubscribed)
				client.Unsubscribe(ch)
			}()
			select {
			case <-unsubscribed:
			case <-time.After(100 * time.Millisecond):
			}
			require.Empty(t, broker.recorded(), "leave published before join")

			close(broker.releaseJoin)
			<-subscribed
			<-unsubscribed
			require.Equal(t, []string{"join", "leave"}, broker.recorded())
			presence, err := node.Presence(ch)
			require.NoError(t, err)
			require.Empty(t, presence.Presence)
			clients, err := node.MapStateRead(context.Background(), "clients:"+ch, MapReadStateOptions{Limit: -1})
			require.NoError(t, err)
			require.Empty(t, clients.Publications, "map client presence left behind")
		})
	}
}

// Without a concurrent unsubscribe the leave comes when the subscription ends.
func TestJoinLeaveOrder_UnsubscribeAfterJoin(t *testing.T) {
	t.Parallel()
	node, broker := newHoldJoinNode(t)
	close(broker.releaseJoin)
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{Options: SubscribeOptions{EmitJoinLeave: true, EmitPresence: true}}, nil)
		})
	})
	client := newTestConnectedClientV2(t, node, "user1")
	subscribeClientV2(t, client, "ch")
	<-broker.joinEntered
	require.Equal(t, []string{"join"}, broker.recorded())

	client.Unsubscribe("ch")
	require.Equal(t, []string{"join", "leave"}, broker.recorded())
	presence, err := node.Presence("ch")
	require.NoError(t, err)
	require.Empty(t, presence.Presence)
}

// A subscribe to the channel which comes while the presence removal and leave
// of the previous subscription wait for its join waits for them: otherwise they
// would remove the new subscription's presence and publish a leave after its
// join.
func TestJoinLeaveOrder_ResubscribeWaitsForDeferredLeave(t *testing.T) {
	t.Parallel()

	const channel = "join_leave_resubscribe"
	clientSubscribe := func(t *testing.T, client *Client) {
		rwWrapper := testReplyWriterWrapper()
		require.NoError(t, client.handleSubscribe(&protocol.SubscribeRequest{Channel: channel}, &protocol.Command{Id: 1}, time.Now(), rwWrapper.rw))
		require.Len(t, rwWrapper.replies, 1)
		require.Nil(t, rwWrapper.replies[0].Error)
	}
	serverSubscribe := func(t *testing.T, client *Client) {
		require.NoError(t, client.Subscribe(channel, WithEmitJoinLeave(true), WithEmitPresence(true)))
	}

	for name, subscribe := range map[string]func(t *testing.T, client *Client){
		"client":      clientSubscribe,
		"server_side": serverSubscribe,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			node, broker := newHoldJoinNode(t)
			node.OnConnect(func(client *Client) {
				client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
					cb(SubscribeReply{Options: SubscribeOptions{EmitJoinLeave: true, EmitPresence: true}}, nil)
				})
			})
			client := newTestConnectedClientV2(t, node, "user1")

			subscribed := make(chan struct{})
			go func() {
				defer close(subscribed)
				subscribe(t, client)
			}()
			<-broker.joinEntered
			client.Unsubscribe(channel)

			resubscribed := make(chan struct{})
			go func() {
				defer close(resubscribed)
				subscribe(t, client)
			}()
			select {
			case <-resubscribed:
				require.Fail(t, "resubscribe did not wait for the deferred leave")
			case <-time.After(100 * time.Millisecond):
			}

			close(broker.releaseJoin)
			<-subscribed
			<-resubscribed
			require.Equal(t, []string{"join", "leave", "join"}, broker.recorded())
			require.True(t, client.IsSubscribed(channel))
			presence, err := node.Presence(channel)
			require.NoError(t, err)
			require.Len(t, presence.Presence, 1)
		})
	}
}

// DisconnectHandler comes after the presence removal and leave which an
// unsubscribe left to the subscribe still publishing the join.
func TestJoinLeaveOrder_DisconnectAfterDeferredLeave(t *testing.T) {
	t.Parallel()
	node, broker := newHoldJoinNode(t)
	disconnected := make(chan []string, 1)
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{Options: SubscribeOptions{EmitJoinLeave: true, EmitPresence: true}}, nil)
		})
		client.OnDisconnect(func(e DisconnectEvent) {
			disconnected <- broker.recorded()
		})
	})
	client := newTestConnectedClientV2(t, node, "user1")

	subscribed := make(chan struct{})
	go func() {
		defer close(subscribed)
		require.NoError(t, client.Subscribe("ch", WithEmitJoinLeave(true), WithEmitPresence(true)))
	}()
	<-broker.joinEntered
	go func() { _ = client.close(DisconnectForceNoReconnect) }()
	select {
	case <-disconnected:
		require.Fail(t, "DisconnectHandler called before the deferred leave")
	case <-time.After(100 * time.Millisecond):
	}
	close(broker.releaseJoin)
	<-subscribed
	select {
	case events := <-disconnected:
		require.Equal(t, []string{"join", "leave"}, events)
	case <-time.After(5 * time.Second):
		require.Fail(t, "DisconnectHandler not called")
	}
}

// holdLeaveBroker holds the first PublishLeave until released and records the
// order of joins and leaves.
type holdLeaveBroker struct {
	*MemoryBroker
	once         sync.Once
	leaveEntered chan struct{}
	releaseLeave chan struct{}

	mu     sync.Mutex
	events []string
}

func (b *holdLeaveBroker) PublishJoin(ch string, info *ClientInfo) error {
	b.mu.Lock()
	b.events = append(b.events, "join")
	b.mu.Unlock()
	return b.MemoryBroker.PublishJoin(ch, info)
}

func (b *holdLeaveBroker) PublishLeave(ch string, info *ClientInfo) error {
	first := false
	b.once.Do(func() { first = true })
	if first {
		close(b.leaveEntered)
		<-b.releaseLeave
	}
	b.mu.Lock()
	b.events = append(b.events, "leave")
	b.mu.Unlock()
	return b.MemoryBroker.PublishLeave(ch, info)
}

// A subscribe to the channel which comes while the unsubscribe still removes
// presence and publishes the leave waits for them.
func TestJoinLeaveOrder_ResubscribeWaitsForLeave(t *testing.T) {
	t.Parallel()
	node, err := New(Config{LogLevel: LogLevelError, LogHandler: func(entry LogEntry) {}})
	require.NoError(t, err)
	memBroker, err := NewMemoryBroker(node, MemoryBrokerConfig{})
	require.NoError(t, err)
	broker := &holdLeaveBroker{MemoryBroker: memBroker, leaveEntered: make(chan struct{}), releaseLeave: make(chan struct{})}
	node.SetBroker(broker)
	require.NoError(t, node.Run())
	t.Cleanup(func() { _ = node.Shutdown(context.Background()) })
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(broker.releaseLeave) }) }
	t.Cleanup(release)

	client := newTestConnectedClientV2(t, node, "user1")
	opts := []SubscribeOption{WithEmitJoinLeave(true), WithEmitPresence(true)}
	require.NoError(t, client.Subscribe("ch", opts...))

	unsubscribed := make(chan struct{})
	go func() {
		defer close(unsubscribed)
		client.Unsubscribe("ch")
	}()
	<-broker.leaveEntered
	resubscribed := make(chan error, 1)
	go func() { resubscribed <- client.Subscribe("ch", opts...) }()
	select {
	case <-resubscribed:
		require.Fail(t, "resubscribe did not wait for the leave")
	case <-time.After(100 * time.Millisecond):
	}
	release()
	<-unsubscribed
	require.NoError(t, <-resubscribed)

	broker.mu.Lock()
	events := append([]string(nil), broker.events...)
	broker.mu.Unlock()
	require.Equal(t, []string{"join", "leave", "join"}, events)
	presence, err := node.Presence("ch")
	require.NoError(t, err)
	require.Len(t, presence.Presence, 1)
}

// holdAddPresenceManager holds the next AddPresence, once armed, until released.
type holdAddPresenceManager struct {
	PresenceManager
	armed   atomic.Bool
	entered chan struct{}
	release chan struct{}
}

func (p *holdAddPresenceManager) AddPresence(ch, clientID string, info *ClientInfo) error {
	if p.armed.CompareAndSwap(true, false) {
		close(p.entered)
		<-p.release
	}
	return p.PresenceManager.AddPresence(ch, clientID, info)
}

// A presence tick add which lands after the subscription it was for was
// replaced by one without presence is undone.
func TestJoinLeaveOrder_PresenceTickAfterResubscribeWithoutPresence(t *testing.T) {
	t.Parallel()
	node, err := New(Config{LogLevel: LogLevelError, LogHandler: func(entry LogEntry) {}})
	require.NoError(t, err)
	memPresence, err := NewMemoryPresenceManager(node, MemoryPresenceManagerConfig{})
	require.NoError(t, err)
	presenceManager := &holdAddPresenceManager{PresenceManager: memPresence, entered: make(chan struct{}), release: make(chan struct{})}
	node.SetPresenceManager(presenceManager)
	var withPresence atomic.Bool
	withPresence.Store(true)
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{Options: SubscribeOptions{EmitPresence: withPresence.Load()}}, nil)
		})
	})
	require.NoError(t, node.Run())
	t.Cleanup(func() { _ = node.Shutdown(context.Background()) })

	client := newTestConnectedClientV2(t, node, "user1")
	subscribeClientV2(t, client, "ch")
	client.mu.RLock()
	snapshot := []channelTickItem{{channel: "ch", ctx: client.channels["ch"], duties: dutyPresence}}
	client.mu.RUnlock()

	presenceManager.armed.Store(true)
	tickDone := make(chan struct{})
	go func() {
		defer close(tickDone)
		client.updateChannelPresenceItem(&snapshot[0])
		client.compensateRacedPresence(snapshot)
	}()
	<-presenceManager.entered
	client.Unsubscribe("ch")
	withPresence.Store(false)
	subscribeClientV2(t, client, "ch")
	close(presenceManager.release)
	<-tickDone

	presence, err := node.Presence("ch")
	require.NoError(t, err)
	require.Empty(t, presence.Presence)
}

// slowPresenceMapBroker delays MapPublish to map client presence channels.
type slowPresenceMapBroker struct {
	*MemoryMapBroker
	delay time.Duration
}

func (b *slowPresenceMapBroker) Publish(ctx context.Context, ch string, key string, opts MapPublishOptions) (MapUpdateResult, error) {
	if strings.HasPrefix(ch, "clients:") {
		time.Sleep(b.delay)
	}
	return b.MemoryMapBroker.Publish(ctx, ch, key, opts)
}

// close() which stops waiting for a map go-live still adding presence (past
// subscribeInProgressTimeout) removes the subscription the go-live committed:
// nothing would remove it later.
func TestJoinLeaveOrder_CloseDuringSlowMapGoLive(t *testing.T) {
	t.Parallel()
	node, err := New(Config{
		LogLevel:   LogLevelError,
		LogHandler: func(entry LogEntry) {},
		Map: MapConfig{GetMapChannelOptions: func(string) MapChannelOptions {
			return MapChannelOptions{Mode: MapModeEphemeral, KeyTTL: time.Minute}
		}},
	})
	require.NoError(t, err)
	mapBroker, err := NewMemoryMapBroker(node, MemoryMapBrokerConfig{})
	require.NoError(t, err)
	require.NoError(t, mapBroker.RegisterEventHandler(nil))
	node.SetMapBroker(&slowPresenceMapBroker{MemoryMapBroker: mapBroker, delay: subscribeInProgressTimeout + 500*time.Millisecond})
	unsubscribed := make(chan UnsubscribeEvent, 1)
	disconnected := make(chan struct{})
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{Options: SubscribeOptions{Type: SubscriptionTypeMap, EmitJoinLeave: true, MapClientPresenceChannel: "clients:" + e.Channel}}, nil)
		})
		client.OnUnsubscribe(func(e UnsubscribeEvent) {
			unsubscribed <- e
		})
		client.OnDisconnect(func(e DisconnectEvent) {
			close(disconnected)
		})
	})
	require.NoError(t, node.Run())
	t.Cleanup(func() { _ = node.Shutdown(context.Background()) })
	_, err = mapBroker.Publish(context.Background(), "ch", "key1", MapPublishOptions{Data: []byte(`{}`)})
	require.NoError(t, err)

	client := newTestConnectedClientV2(t, node, "user1")
	subscribed := make(chan struct{})
	go func() {
		defer close(subscribed)
		rwWrapper := testReplyWriterWrapper()
		_ = client.handleSubscribe(&protocol.SubscribeRequest{
			Channel: "ch",
			Type:    int32(SubscriptionTypeMap),
			Phase:   MapPhaseState,
			Limit:   100,
		}, &protocol.Command{Id: 1}, time.Now(), rwWrapper.rw)
	}()
	require.Eventually(t, func() bool { return client.IsSubscribed("ch") }, 2*time.Second, 5*time.Millisecond)
	require.NoError(t, client.close(DisconnectForceNoReconnect))
	<-subscribed
	select {
	case <-disconnected:
	case <-time.After(5 * time.Second):
		require.Fail(t, "DisconnectHandler not called")
	}

	select {
	case e := <-unsubscribed:
		require.True(t, e.Subscribed)
	default:
		require.Fail(t, "UnsubscribeHandler not called")
	}
	require.Zero(t, node.hub.NumSubscribers("ch"))
	require.False(t, client.IsSubscribed("ch"))
	clients, err := node.MapStateRead(context.Background(), "clients:ch", MapReadStateOptions{Limit: -1})
	require.NoError(t, err)
	require.Empty(t, clients.Publications)
}

// lateApplyPresenceMapBroker returns from a MapPublish to a map client presence
// channel when ctx is canceled, but applies it after a delay anyway, as a remote
// broker which already got the command does.
type lateApplyPresenceMapBroker struct {
	*MemoryMapBroker
	delay   time.Duration
	entered chan struct{}
	applied chan struct{}
}

func (b *lateApplyPresenceMapBroker) Publish(ctx context.Context, ch string, key string, opts MapPublishOptions) (MapUpdateResult, error) {
	if !strings.HasPrefix(ch, "clients:") {
		return b.MemoryMapBroker.Publish(ctx, ch, key, opts)
	}
	done := make(chan struct{})
	var res MapUpdateResult
	var err error
	go func() {
		defer close(done)
		time.Sleep(b.delay)
		res, err = b.MemoryMapBroker.Publish(context.Background(), ch, key, opts)
		close(b.applied)
	}()
	close(b.entered)
	select {
	case <-done:
		return res, err
	case <-ctx.Done():
		return MapUpdateResult{}, ctx.Err()
	}
}

// A map client presence add in progress when the client closes completes
// before the presence removal: a canceled add applied by the broker later
// would leave the entry behind.
func TestJoinLeaveOrder_CloseDuringMapClientPresenceAdd(t *testing.T) {
	t.Parallel()
	node, err := New(Config{
		LogLevel:   LogLevelError,
		LogHandler: func(entry LogEntry) {},
		Map: MapConfig{GetMapChannelOptions: func(string) MapChannelOptions {
			return MapChannelOptions{Mode: MapModeEphemeral, KeyTTL: time.Minute}
		}},
	})
	require.NoError(t, err)
	mapBroker, err := NewMemoryMapBroker(node, MemoryMapBrokerConfig{})
	require.NoError(t, err)
	require.NoError(t, mapBroker.RegisterEventHandler(nil))
	broker := &lateApplyPresenceMapBroker{MemoryMapBroker: mapBroker, delay: 200 * time.Millisecond, entered: make(chan struct{}), applied: make(chan struct{})}
	node.SetMapBroker(broker)
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{Options: SubscribeOptions{MapClientPresenceChannel: "clients:" + e.Channel}}, nil)
		})
	})
	require.NoError(t, node.Run())
	t.Cleanup(func() { _ = node.Shutdown(context.Background()) })

	ctx, cancel := context.WithCancel(context.Background())
	transport := newTestTransport(cancel)
	client := newTestConnectedClientWithTransport(t, ctx, node, transport, "user1")
	subscribed := make(chan struct{})
	go func() {
		defer close(subscribed)
		rwWrapper := testReplyWriterWrapper()
		_ = client.handleSubscribe(&protocol.SubscribeRequest{Channel: "ch"}, &protocol.Command{Id: 1}, time.Now(), rwWrapper.rw)
	}()
	<-broker.entered
	cancel() // The connection is gone, as when a transport closes.
	require.NoError(t, client.close(DisconnectForceNoReconnect))
	<-subscribed
	<-broker.applied

	clients, err := node.MapStateRead(context.Background(), "clients:ch", MapReadStateOptions{Limit: -1})
	require.NoError(t, err)
	require.Empty(t, clients.Publications, "map client presence left behind")
}

type holdHistoryBroker struct {
	*MemoryBroker
	armed   atomic.Bool
	entered chan struct{}
	release chan struct{}
}

func (b *holdHistoryBroker) History(ch string, opts HistoryOptions) ([]*Publication, StreamPosition, error) {
	if b.armed.CompareAndSwap(true, false) {
		close(b.entered)
		<-b.release
	}
	return b.MemoryBroker.History(ch, opts)
}

// A presence tick add which lands while a resubscribe holds its reservation
// (before the commit) is kept if the resubscribe goes live with the same
// presence, and undone if it goes live without presence.
func TestJoinLeaveOrder_PresenceTickDuringResubscribe(t *testing.T) {
	t.Parallel()
	for _, withPresence := range []bool{true, false} {
		t.Run(strconv.FormatBool(withPresence), func(t *testing.T) {
			t.Parallel()
			node, err := New(Config{LogLevel: LogLevelError, LogHandler: func(entry LogEntry) {}})
			require.NoError(t, err)
			memPresence, err := NewMemoryPresenceManager(node, MemoryPresenceManagerConfig{})
			require.NoError(t, err)
			presenceManager := &holdAddPresenceManager{PresenceManager: memPresence, entered: make(chan struct{}), release: make(chan struct{})}
			node.SetPresenceManager(presenceManager)
			memBroker, err := NewMemoryBroker(node, MemoryBrokerConfig{})
			require.NoError(t, err)
			broker := &holdHistoryBroker{MemoryBroker: memBroker, entered: make(chan struct{}), release: make(chan struct{})}
			node.SetBroker(broker)
			var subscribes atomic.Int32
			node.OnConnect(func(client *Client) {
				client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
					emitPresence := subscribes.Add(1) == 1 || withPresence
					cb(SubscribeReply{Options: SubscribeOptions{EmitPresence: emitPresence, EnableRecovery: true}}, nil)
				})
			})
			require.NoError(t, node.Run())
			t.Cleanup(func() { _ = node.Shutdown(context.Background()) })

			client := newTestConnectedClientV2(t, node, "user1")
			subscribeClientV2(t, client, "ch")
			client.mu.RLock()
			snapshot := []channelTickItem{{channel: "ch", ctx: client.channels["ch"], duties: dutyPresence}}
			client.mu.RUnlock()

			presenceManager.armed.Store(true)
			tickDone := make(chan struct{})
			go func() {
				defer close(tickDone)
				client.updateChannelPresenceItem(&snapshot[0])
				<-broker.entered // The resubscribe holds its reservation.
				client.compensateRacedPresence(snapshot)
			}()
			<-presenceManager.entered
			client.Unsubscribe("ch")

			broker.armed.Store(true)
			subscribed := make(chan struct{})
			go func() {
				defer close(subscribed)
				rwWrapper := testReplyWriterWrapper()
				_ = client.handleSubscribe(&protocol.SubscribeRequest{Channel: "ch"}, &protocol.Command{Id: 2}, time.Now(), rwWrapper.rw)
			}()
			<-broker.entered
			close(presenceManager.release)
			<-tickDone
			close(broker.release)
			<-subscribed
			require.True(t, client.IsSubscribed("ch"))

			hasPresence := func() bool {
				presence, err := node.Presence("ch")
				require.NoError(t, err)
				_, ok := presence.Presence[client.ID()]
				return ok
			}
			if withPresence {
				require.Never(t, func() bool { return !hasPresence() }, 200*time.Millisecond, 10*time.Millisecond)
			} else {
				require.Eventually(t, func() bool { return !hasPresence() }, 2*time.Second, 10*time.Millisecond)
			}
		})
	}
}
