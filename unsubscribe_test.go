package centrifuge

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/require"
)

func TestUnsubscribe_String(t *testing.T) {
	t.Parallel()
	require.Equal(t, `code: 0, reason: client unsubscribed`, unsubscribeClient.String())
}

// A server unsubscribe sends the unsubscribe push before UnsubscribeHandler,
// and a subscribe to the channel which comes meanwhile waits for it: the client
// must not get the push of the previous subscription after the new one's reply.
func TestServerUnsubscribePushBeforeResubscribeReply(t *testing.T) {
	t.Parallel()
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	inHandler := make(chan struct{}, 1)
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{}, nil)
		})
		client.OnUnsubscribe(func(e UnsubscribeEvent) {
			if e.Code == UnsubscribeCodeServer {
				inHandler <- struct{}{}
				time.Sleep(100 * time.Millisecond)
			}
		})
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	transport := newTestTransport(cancel)
	transport.sink = make(chan []byte, 100)
	transport.setProtocolType(ProtocolTypeJSON)
	transport.setProtocolVersion(ProtocolVersion2)
	client, err := newClient(SetCredentials(ctx, &Credentials{UserID: "u"}), node, transport)
	require.NoError(t, err)
	require.True(t, client.HandleCommand(&protocol.Command{Id: 100, Connect: &protocol.ConnectRequest{}}, 0))
	require.True(t, client.HandleCommand(&protocol.Command{Id: 1, Subscribe: &protocol.SubscribeRequest{Channel: "x"}}, 0))

	go client.Unsubscribe("x")
	<-inHandler
	require.True(t, client.HandleCommand(&protocol.Command{Id: 2, Subscribe: &protocol.SubscribeRequest{Channel: "x"}}, 0))

	var frames []string
	timeout := time.After(2 * time.Second)
	for len(frames) < 2 {
		select {
		case data := <-transport.sink:
			decoder := protocol.NewJSONReplyDecoder(data)
			for {
				reply, err := decoder.Decode()
				if err != nil {
					break
				}
				switch {
				case reply.Id == 2 && reply.Subscribe != nil:
					frames = append(frames, "resubscribe reply")
				case reply.Push != nil && reply.Push.Unsubscribe != nil:
					frames = append(frames, "unsubscribe push")
				}
			}
		case <-timeout:
			require.Fail(t, "timeout", "frames: %v", frames)
		}
	}
	require.Equal(t, []string{"unsubscribe push", "resubscribe reply"}, frames)
	require.True(t, client.IsSubscribed("x"))
}

// The UnsubscribeHandler call for a server unsubscribe of a live subscription
// comes before the SubscribeHandler call of the client's next subscribe to the
// channel, which may come as soon as the client gets the unsubscribe push.
func TestServerUnsubscribeHandlerBeforeNextSubscribeHandler(t *testing.T) {
	t.Parallel()
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	var mu sync.Mutex
	var events []string
	record := func(event string) {
		mu.Lock()
		events = append(events, event)
		mu.Unlock()
	}
	inHandler := make(chan struct{}, 1)
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			record("subscribe")
			cb(SubscribeReply{}, nil)
		})
		client.OnUnsubscribe(func(e UnsubscribeEvent) {
			inHandler <- struct{}{}
			time.Sleep(100 * time.Millisecond)
			record("unsubscribe")
		})
	})
	client := newTestConnectedClientV2(t, node, "u")
	subscribeClientV2(t, client, "ch")

	go client.Unsubscribe("ch")
	<-inHandler
	subscribeClientV2(t, client, "ch")

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []string{"subscribe", "unsubscribe", "subscribe"}, events)
}

// Unsubscribes the server decides on asynchronously (expired subscription,
// changed server tags filter, invalid position) only end the subscription they
// were decided for, not one the client made since by unsubscribing and
// subscribing again.

func TestStaleUnsubscribe_SubscriptionExpired(t *testing.T) {
	t.Parallel()
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()
	node.nowTimeGetter = func() time.Time { return time.Now().Add(10 * time.Second) }

	var subscribes atomic.Int32
	refreshCallbacks := make(chan SubRefreshCallback, 1)
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			opts := SubscribeOptions{}
			if subscribes.Add(1) == 1 {
				opts.ExpireAt = time.Now().Unix() + 1 // Expired by nowTimeGetter.
			}
			cb(SubscribeReply{Options: opts}, nil)
		})
		client.OnSubRefresh(func(e SubRefreshEvent, cb SubRefreshCallback) {
			refreshCallbacks <- cb
		})
	})
	client := newTestConnectedClientV2(t, node, "u")
	subscribeClientV2(t, client, "ch")
	client.mu.RLock()
	chCtx := client.channels["ch"]
	client.mu.RUnlock()

	// As the presence tick does for an expired subscription.
	client.checkSubscriptionExpiration("ch", chCtx, 0, func(result bool) {
		if !result {
			go client.handleAsyncUnsubscribe("ch", chCtx.subGen, unsubscribeExpired)
		}
	})
	cb := <-refreshCallbacks
	rwWrapper := testReplyWriterWrapper()
	require.NoError(t, client.handleUnsubscribe(&protocol.UnsubscribeRequest{Channel: "ch"}, &protocol.Command{Id: 2}, time.Now(), rwWrapper.rw))
	subscribeClientV2(t, client, "ch")

	cb(SubRefreshReply{Expired: true}, nil)
	require.Never(t, func() bool { return !client.IsSubscribed("ch") }, 200*time.Millisecond, 10*time.Millisecond)
}

func TestStaleUnsubscribe_SubRefreshChangesServerTagsFilter(t *testing.T) {
	t.Parallel()
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{Options: SubscribeOptions{
				EnableRecovery: true, AllowedDeltaTypes: []DeltaType{DeltaTypeFossil},
				ExpireAt: time.Now().Unix() + 3600,
			}, ClientSideRefresh: true}, nil)
		})
		client.OnSubRefresh(func(e SubRefreshEvent, cb SubRefreshCallback) {
			// A new filter on a delta subscription: it must start over.
			cb(SubRefreshReply{ExpireAt: time.Now().Unix() + 3600, ServerTagsFilter: &FilterNode{Key: "k", Cmp: "eq", Val: "v"}}, nil)
		})
	})
	client := newTestConnectedClientV2(t, node, "u")
	rwWrapper := testReplyWriterWrapper()
	require.NoError(t, client.handleSubscribe(&protocol.SubscribeRequest{Channel: "ch", Delta: string(DeltaTypeFossil)}, &protocol.Command{Id: 1}, time.Now(), rwWrapper.rw))
	require.True(t, rwWrapper.replies[0].Subscribe.Delta)

	resubscribed := false
	refreshWriter := &replyWriter{write: func(rep *protocol.Reply) {
		if rep.SubRefresh != nil && !resubscribed {
			resubscribed = true
			// The client unsubscribes and subscribes again as the refresh reply
			// is written, before the unsubscribe which follows it.
			unsubscribeWriter := testReplyWriterWrapper()
			require.NoError(t, client.handleUnsubscribe(&protocol.UnsubscribeRequest{Channel: "ch"}, &protocol.Command{Id: 3}, time.Now(), unsubscribeWriter.rw))
			subscribeClientV2(t, client, "ch")
		}
	}}
	require.NoError(t, client.handleSubRefresh(&protocol.SubRefreshRequest{Channel: "ch", Token: "t"}, &protocol.Command{Id: 2}, time.Now(), refreshWriter))
	require.True(t, resubscribed)
	require.Never(t, func() bool { return !client.IsSubscribed("ch") }, 200*time.Millisecond, 10*time.Millisecond)
}

// epochChangingHistoryBroker holds the next History call, once armed, and
// reports a changed epoch from it.
type epochChangingHistoryBroker struct {
	*MemoryBroker
	armed   atomic.Bool
	entered chan struct{}
	release chan struct{}
}

func (b *epochChangingHistoryBroker) History(ch string, opts HistoryOptions) ([]*Publication, StreamPosition, error) {
	pubs, sp, err := b.MemoryBroker.History(ch, opts)
	if b.armed.CompareAndSwap(true, false) {
		close(b.entered)
		<-b.release
		sp.Epoch = "changed"
	}
	return pubs, sp, err
}

func TestStaleUnsubscribe_PositionCheck(t *testing.T) {
	t.Parallel()
	node, err := New(Config{
		LogLevel:                        LogLevelError,
		LogHandler:                      func(LogEntry) {},
		ClientPresenceUpdateInterval:    50 * time.Millisecond,
		ClientChannelPositionCheckDelay: time.Nanosecond,
	})
	require.NoError(t, err)
	memBroker, err := NewMemoryBroker(node, MemoryBrokerConfig{})
	require.NoError(t, err)
	broker := &epochChangingHistoryBroker{MemoryBroker: memBroker, entered: make(chan struct{}), release: make(chan struct{})}
	node.SetBroker(broker)
	require.NoError(t, node.Run())
	defer func() { _ = node.Shutdown(context.Background()) }()
	var subscribes atomic.Int32
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			// The first subscription is positioned, the second one is not.
			cb(SubscribeReply{Options: SubscribeOptions{EnablePositioning: subscribes.Add(1) == 1}}, nil)
		})
	})
	_, err = node.Publish("ch", []byte(`{}`), WithHistory(10, time.Minute))
	require.NoError(t, err)

	client := newTestConnectedClientV2(t, node, "u")
	subscribeClientV2(t, client, "ch")
	time.Sleep(1100 * time.Millisecond) // The position check has second precision.
	broker.armed.Store(true)
	<-broker.entered // The tick checks the position.

	rwWrapper := testReplyWriterWrapper()
	require.NoError(t, client.handleUnsubscribe(&protocol.UnsubscribeRequest{Channel: "ch"}, &protocol.Command{Id: 3}, time.Now(), rwWrapper.rw))
	subscribeClientV2(t, client, "ch")
	close(broker.release)
	require.Never(t, func() bool { return !client.IsSubscribed("ch") }, 300*time.Millisecond, 10*time.Millisecond)
}

// holdRemovePresenceManager holds the first RemovePresence until released.
type holdRemovePresenceManager struct {
	PresenceManager
	once    atomic.Bool
	entered chan struct{}
	release chan struct{}
}

func (p *holdRemovePresenceManager) RemovePresence(ch, clientID, userID string) error {
	if p.once.CompareAndSwap(false, true) {
		close(p.entered)
		<-p.release
	}
	return p.PresenceManager.RemovePresence(ch, clientID, userID)
}

// With per-channel batching, a publication broadcast while an unsubscribe is
// in progress does not reach the client after the unsubscribe.
func TestStaleUnsubscribe_BatchedPublicationAfterUnsubscribe(t *testing.T) {
	t.Parallel()
	node, err := New(Config{
		LogLevel:   LogLevelError,
		LogHandler: func(LogEntry) {},
		GetChannelBatchConfig: func(string) ChannelBatchConfig {
			return ChannelBatchConfig{MaxSize: 100, MaxDelay: 30 * time.Millisecond}
		},
	})
	require.NoError(t, err)
	require.NoError(t, node.Run())
	defer func() { _ = node.Shutdown(context.Background()) }()
	presenceManager := &holdRemovePresenceManager{PresenceManager: node.presenceManager, entered: make(chan struct{}), release: make(chan struct{})}
	node.SetPresenceManager(presenceManager)
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{Options: SubscribeOptions{EmitPresence: true}}, nil)
		})
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	transport := newTestTransport(cancel)
	transport.sink = make(chan []byte, 100)
	transport.setProtocolType(ProtocolTypeJSON)
	transport.setProtocolVersion(ProtocolVersion2)
	client, err := newClient(SetCredentials(ctx, &Credentials{UserID: "u"}), node, transport)
	require.NoError(t, err)
	require.True(t, client.HandleCommand(&protocol.Command{Id: 1, Connect: &protocol.ConnectRequest{}}, 0))
	require.True(t, client.HandleCommand(&protocol.Command{Id: 2, Subscribe: &protocol.SubscribeRequest{Channel: "ch"}}, 0))

	unsubscribed := make(chan struct{})
	go func() {
		defer close(unsubscribed)
		client.HandleCommand(&protocol.Command{Id: 3, Unsubscribe: &protocol.UnsubscribeRequest{Channel: "ch"}}, 0)
	}()
	<-presenceManager.entered
	_, err = node.Publish("ch", []byte(`{}`))
	require.NoError(t, err)
	close(presenceManager.release)
	<-unsubscribed

	var afterUnsubscribe []string
	unsubscribeReplied := false
	timeout := time.After(200 * time.Millisecond)
	for done := false; !done; {
		select {
		case data := <-transport.sink:
			decoder := protocol.NewJSONReplyDecoder(data)
			for {
				reply, err := decoder.Decode()
				if err != nil {
					break
				}
				switch {
				case reply.Id == 3:
					unsubscribeReplied = true
				case unsubscribeReplied && reply.Push != nil && reply.Push.Pub != nil:
					afterUnsubscribe = append(afterUnsubscribe, string(reply.Push.Pub.Data))
				}
			}
		case <-timeout:
			done = true
		}
	}
	require.True(t, unsubscribeReplied)
	require.Empty(t, afterUnsubscribe, "publication after the unsubscribe reply")
}

// A client unsubscribe which finds the subscription removed by a server
// unsubscribe in progress replies after that unsubscribe's push: otherwise the
// client could subscribe again and get the push of the previous subscription.
func TestStaleUnsubscribe_ClientUnsubscribeReplyAfterServerPush(t *testing.T) {
	t.Parallel()
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()
	presenceManager := &holdRemovePresenceManager{PresenceManager: node.presenceManager, entered: make(chan struct{}), release: make(chan struct{})}
	node.SetPresenceManager(presenceManager)
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{Options: SubscribeOptions{EmitPresence: true}}, nil)
		})
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	transport := newTestTransport(cancel)
	transport.sink = make(chan []byte, 100)
	transport.setProtocolType(ProtocolTypeJSON)
	transport.setProtocolVersion(ProtocolVersion2)
	client, err := newClient(SetCredentials(ctx, &Credentials{UserID: "u"}), node, transport)
	require.NoError(t, err)
	require.True(t, client.HandleCommand(&protocol.Command{Id: 1, Connect: &protocol.ConnectRequest{}}, 0))
	require.True(t, client.HandleCommand(&protocol.Command{Id: 2, Subscribe: &protocol.SubscribeRequest{Channel: "ch"}}, 0))

	go client.Unsubscribe("ch")
	<-presenceManager.entered // The server unsubscribe removed the subscription.
	go client.HandleCommand(&protocol.Command{Id: 3, Unsubscribe: &protocol.UnsubscribeRequest{Channel: "ch"}}, 0)
	time.Sleep(50 * time.Millisecond)
	close(presenceManager.release)

	var frames []string
	timeout := time.After(2 * time.Second)
	for len(frames) < 2 {
		select {
		case data := <-transport.sink:
			decoder := protocol.NewJSONReplyDecoder(data)
			for {
				reply, err := decoder.Decode()
				if err != nil {
					break
				}
				switch {
				case reply.Id == 3:
					frames = append(frames, "unsubscribe reply")
				case reply.Push != nil && reply.Push.Unsubscribe != nil:
					frames = append(frames, "unsubscribe push")
				}
			}
		case <-timeout:
			require.Fail(t, "timeout", "frames: %v", frames)
		}
	}
	require.Equal(t, []string{"unsubscribe push", "unsubscribe reply"}, frames)
}

// A client map publish of its own key answered after the client unsubscribed
// does not leave the key behind the MapRemoveClientOnUnsubscribe cleanup.
func TestStaleUnsubscribe_MapPublishAfterCleanup(t *testing.T) {
	t.Parallel()
	node, broker := newTestNodeWithMapBroker(t)
	answers := make(chan func(), 1)
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{Options: SubscribeOptions{Type: SubscriptionTypeMap, MapRemoveClientOnUnsubscribe: true}}, nil)
		})
		client.OnMapPublish(func(e MapPublishEvent, cb MapPublishCallback) {
			answers <- func() { cb(MapPublishReply{Key: e.Key}, nil) }
		})
	})
	client := newTestConnectedClientV2(t, node, "u1")
	subscribeMapClient(t, client, &protocol.SubscribeRequest{Channel: "cursors", Type: int32(SubscriptionTypeMap), Phase: MapPhaseState, Limit: 100})
	rwWrapper := testReplyWriterWrapper()
	require.NoError(t, client.handleMapPublish(&protocol.PublishRequest{Channel: "cursors", Key: client.ID(), Data: []byte(`{}`)}, &protocol.Command{Id: 3}, time.Now(), rwWrapper.rw))
	answer := <-answers
	client.Unsubscribe("cursors")
	answer()
	res, err := broker.ReadState(context.Background(), "cursors", MapReadStateOptions{Limit: 100})
	require.NoError(t, err)
	require.Empty(t, res.Publications, "key of the unsubscribed client left behind")
}

// Node.Unsubscribe of a subscription made on connect while ConnectHandler runs
// takes effect after ConnectHandler returns: UnsubscribeHandler set in it gets
// the call.
func TestUnsubscribeDuringConnectHandler(t *testing.T) {
	t.Parallel()
	node := defaultTestNode()
	defer func() { _ = node.Shutdown(context.Background()) }()
	node.OnConnecting(func(context.Context, ConnectEvent) (ConnectReply, error) {
		return ConnectReply{
			Credentials:   &Credentials{UserID: "u"},
			Subscriptions: map[string]SubscribeOptions{"ch": {}},
		}, nil
	})
	inConnect := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseConnect := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseConnect() // Before the node shutdown: it waits for ConnectHandler.
	var connectReturned atomic.Bool
	unsubscribes := make(chan bool, 2)
	node.OnConnect(func(client *Client) {
		close(inConnect)
		<-release
		client.OnUnsubscribe(func(UnsubscribeEvent) { unsubscribes <- connectReturned.Load() })
		connectReturned.Store(true)
	})
	client, err := newClient(context.Background(), node, newTestTransport(func() {}))
	require.NoError(t, err)
	connected := make(chan struct{})
	go func() {
		defer close(connected)
		connectClientV2(t, client)
	}()
	<-inConnect
	require.NoError(t, node.Unsubscribe("u", "ch"))
	require.True(t, client.IsSubscribed("ch"), "applied after ConnectHandler")
	releaseConnect()
	<-connected
	require.False(t, client.IsSubscribed("ch"))
	select {
	case afterConnect := <-unsubscribes:
		require.True(t, afterConnect)
	case <-time.After(time.Second):
		require.Fail(t, "UnsubscribeHandler not called")
	}
	require.Empty(t, unsubscribes)
}

// Client.Unsubscribe called inside ConnectHandler takes effect after it returns.
func TestUnsubscribeInsideConnectHandler(t *testing.T) {
	t.Parallel()
	node := defaultTestNode()
	defer func() { _ = node.Shutdown(context.Background()) }()
	node.OnConnecting(func(context.Context, ConnectEvent) (ConnectReply, error) {
		return ConnectReply{
			Credentials:   &Credentials{UserID: "u"},
			Subscriptions: map[string]SubscribeOptions{"ch": {}},
		}, nil
	})
	unsubscribed := make(chan struct{}, 1)
	node.OnConnect(func(client *Client) {
		client.OnUnsubscribe(func(UnsubscribeEvent) { unsubscribed <- struct{}{} })
		client.Unsubscribe("ch")
	})
	client, err := newClient(context.Background(), node, newTestTransport(func() {}))
	require.NoError(t, err)
	connectClientV2(t, client)
	require.False(t, client.IsSubscribed("ch"))
	select {
	case <-unsubscribed:
	case <-time.After(time.Second):
		require.Fail(t, "UnsubscribeHandler not called")
	}
}
