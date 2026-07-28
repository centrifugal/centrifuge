package centrifuge

// Code consolidated from per-fix regression test files created on this
// branch. Grouped here to keep the number of test files manageable; each
// section is prefixed with its original file name.

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/centrifugal/centrifuge/internal/queue"
	"github.com/centrifugal/centrifuge/internal/recovery"
	"github.com/centrifugal/centrifuge/internal/websocket"
	"github.com/centrifugal/protocol"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// ===========================================================================
// broker_memory_race_test.go
// ===========================================================================

// TestMemoryBroker_PublishHistoryReadRace guards a data race on
// Publication.Offset. historyHub.add already stamps pub.Offset under the history
// lock and returns that offset, but Publish then set pub.Offset a second time
// outside the lock — a redundant, unsynchronized write that raced a concurrent
// History read of the same shared *Publication (e.g. subscribe-time recovery
// reading Publications[i].Offset). Must be run under -race.
func TestMemoryBroker_PublishHistoryReadRace(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()
	const ch = "hist-race"

	stop := make(chan struct{})
	var wg sync.WaitGroup

	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					_, _ = node.Publish(ch, []byte(`{"x":1}`), WithHistory(10, time.Minute))
				}
			}
		}()
	}
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				res, err := node.History(ch, WithLimit(10))
				if err != nil {
					continue
				}
				for _, p := range res.Publications {
					_ = p.Offset
				}
			}
		}()
	}

	time.Sleep(500 * time.Millisecond)
	close(stop)
	wg.Wait()
}

// ===========================================================================
// client_concurrent_entrypoints_test.go
// ===========================================================================

// TestClient_ConcurrentEntryPoints hammers a single Client instance through many
// entry points at once — command handlers (subscribe/unsubscribe/publish/
// presence/history/rpc/refresh/send), public methods (Info/Channels/Context),
// timer ops (updatePresence/sendPing/checkPong/expire), broadcasts, and
// Disconnect — from many goroutines.
//
// This is not an artificial scenario: the emulation transport dispatches each
// client command on its own goroutine (see emulation.go — `go HandleReadFrame`),
// so command handlers genuinely run concurrently for one client, with no global
// command serialization. Every handler must be safe under that concurrency.
//
// Run under -race; a handler that touches shared client state without c.mu shows
// up as a data race, and any lock-order inversion shows up as a deadlock
// (the test would hang and time out).
func TestClient_ConcurrentEntryPoints(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{Options: SubscribeOptions{
				EmitPresence:      true,
				EmitJoinLeave:     true,
				EnableRecovery:    true,
				EnablePositioning: true,
			}}, nil)
		})
		client.OnPublish(func(e PublishEvent, cb PublishCallback) {
			cb(PublishReply{}, nil)
		})
		client.OnRefresh(func(e RefreshEvent, cb RefreshCallback) {
			cb(RefreshReply{ExpireAt: time.Now().Unix() + 100}, nil)
		})
		client.OnSubRefresh(func(e SubRefreshEvent, cb SubRefreshCallback) {
			cb(SubRefreshReply{ExpireAt: time.Now().Unix() + 100}, nil)
		})
		client.OnRPC(func(e RPCEvent, cb RPCCallback) {
			cb(RPCReply{}, nil)
		})
		client.OnPresence(func(e PresenceEvent, cb PresenceCallback) {
			cb(PresenceReply{}, nil)
		})
		client.OnMessage(func(e MessageEvent) {})
		client.OnAlive(func() {})
	})

	const rounds = 40
	const workers = 12
	channels := []string{"a", "b", "c", "d"}

	for round := 0; round < rounds; round++ {
		client := newTestClientV2(t, node, "u"+strconv.Itoa(round))
		connectClientV2(t, client)
		// Pre-subscribe so unsubscribe/presence/history have something to act on.
		for _, ch := range channels {
			subscribeClientV2(t, client, ch)
		}

		var wg sync.WaitGroup
		start := make(chan struct{})
		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func(seed int) {
				defer wg.Done()
				rnd := rand.New(rand.NewSource(int64(seed)))
				<-start
				for i := 0; i < 60; i++ {
					ch := channels[rnd.Intn(len(channels))]
					switch rnd.Intn(16) {
					case 0:
						rw := testReplyWriterWrapper()
						_ = client.handleSubscribe(&protocol.SubscribeRequest{Channel: ch}, &protocol.Command{Id: 1}, time.Now(), rw.rw)
					case 1:
						rw := testReplyWriterWrapper()
						_ = client.handleUnsubscribe(&protocol.UnsubscribeRequest{Channel: ch}, &protocol.Command{Id: 1}, time.Now(), rw.rw)
					case 2:
						rw := testReplyWriterWrapper()
						_ = client.handlePublish(&protocol.PublishRequest{Channel: ch, Data: []byte(`{}`)}, &protocol.Command{Id: 1}, time.Now(), rw.rw)
					case 3:
						rw := testReplyWriterWrapper()
						_ = client.handlePresence(&protocol.PresenceRequest{Channel: ch}, &protocol.Command{Id: 1}, time.Now(), rw.rw)
					case 4:
						rw := testReplyWriterWrapper()
						_ = client.handlePresenceStats(&protocol.PresenceStatsRequest{Channel: ch}, &protocol.Command{Id: 1}, time.Now(), rw.rw)
					case 5:
						rw := testReplyWriterWrapper()
						_ = client.handleHistory(&protocol.HistoryRequest{Channel: ch}, &protocol.Command{Id: 1}, time.Now(), rw.rw)
					case 6:
						rw := testReplyWriterWrapper()
						_ = client.handleRPC(&protocol.RPCRequest{Data: []byte(`{}`)}, &protocol.Command{Id: 1}, time.Now(), rw.rw)
					case 7:
						rw := testReplyWriterWrapper()
						_ = client.handleRefresh(&protocol.RefreshRequest{}, &protocol.Command{Id: 1}, time.Now(), rw.rw)
					case 8:
						rw := testReplyWriterWrapper()
						_ = client.handleSubRefresh(&protocol.SubRefreshRequest{Channel: ch}, &protocol.Command{Id: 1}, time.Now(), rw.rw)
					case 9:
						_ = client.handleSend(&protocol.SendRequest{Data: []byte(`{}`)}, &protocol.Command{}, time.Now())
					case 10:
						_ = client.Info()
					case 11:
						_ = client.Channels()
					case 12:
						client.updatePresence()
					case 13:
						client.sendPing()
					case 14:
						_, _ = node.Publish(ch, []byte(`{"x":1}`))
					case 15:
						// Disconnect is the teardown racer — only a couple of workers
						// actually fire it so most iterations run against a live client.
						if seed%4 == 0 && i > 30 {
							client.Disconnect(DisconnectForceNoReconnect)
						}
					}
				}
			}(round*workers + w)
		}
		close(start)
		wg.Wait()

		// Ensure the client is closed and the hub does not leak it.
		_ = client.close(DisconnectForceNoReconnect)
	}

	require.Eventually(t, func() bool {
		return node.hub.NumClients() == 0
	}, 5*time.Second, 20*time.Millisecond, "clients leaked in hub after concurrent churn")
}

// ===========================================================================
// client_connect_nodesubscribe_race_test.go
// ===========================================================================

// TestConnect_ServerSideSubs_NodeSubscribeSameChannel guards the exclusivity gap
// where a connect-time server-side subscription and a Node.Subscribe on the same
// channel race. The connect-time path adds to the hub before it writes c.channels
// and never reserves c.channels, so a concurrent Client.Subscribe (from the
// Node.Subscribe fan-out reaching the now-hub-registered connecting client) lands
// between the hub-add and the c.channels write: the two subscribes get different
// generations, c.channels and the hub diverge, and close() removes the wrong
// generation — leaking a hub entry.
//
// The window is tiny, so this hammers connect vs Node.Subscribe on shared
// channels and checks no hub subscription leaks once everything is torn down.
func TestConnect_ServerSideSubs_NodeSubscribeSameChannel(t *testing.T) {
	channels := []string{"o0", "o1", "o2"}
	node, err := New(Config{LogLevel: LogLevelError, LogHandler: func(LogEntry) {}})
	require.NoError(t, err)
	node.OnConnecting(func(ctx context.Context, e ConnectEvent) (ConnectReply, error) {
		subs := make(map[string]SubscribeOptions, len(channels))
		for _, ch := range channels {
			subs[ch] = SubscribeOptions{}
		}
		return ConnectReply{Subscriptions: subs}, nil
	})
	node.OnConnect(func(c *Client) {
		c.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) { cb(SubscribeReply{}, nil) })
	})
	require.NoError(t, node.Run())
	defer func() { _ = node.Shutdown(context.Background()) }()

	const user = "u"
	stop := make(chan struct{})
	var wg sync.WaitGroup

	// Node.Subscribe/Unsubscribe the same channels the connecting clients get at
	// connect time.
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					for _, ch := range channels {
						_ = node.Subscribe(user, ch)
						_ = node.Unsubscribe(user, ch)
					}
				}
			}
		}()
	}

	// Connect/close churn for the same user.
	for w := 0; w < 12; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				ctx, cancel := context.WithCancel(context.Background())
				tr := &countingTransport{pingInterval: time.Second, pongTimeout: -1}
				client, err := newClient(SetCredentials(ctx, &Credentials{UserID: user}), node, tr)
				if err != nil {
					cancel()
					return
				}
				rw := testReplyWriterWrapper()
				_ = client.connectCmd(&protocol.ConnectRequest{}, &protocol.Command{}, time.Now(), rw.rw)
				client.triggerConnect()
				client.scheduleOnConnectTimers()
				_ = client.close(DisconnectForceNoReconnect)
				cancel()
			}
		}(w)
	}

	time.Sleep(2 * time.Second)
	close(stop)
	wg.Wait()

	require.Eventually(t, func() bool {
		return node.hub.NumClients() == 0 && node.hub.NumSubscriptions() == 0
	}, 4*time.Second, 10*time.Millisecond,
		"leaked hub subscriptions after connect vs Node.Subscribe race: NumClients=%d NumSubscriptions=%d",
		node.hub.NumClients(), node.hub.NumSubscriptions())
}

// ===========================================================================
// client_connect_serverside_race_test.go
// ===========================================================================

// TestConnect_ServerSideSubs_CloseRace covers a client that closes while
// connectCmd is applying connect-time server-side subscriptions. Two problems it
// guards:
//   - the server-side subs finalize writes c.channels unconditionally, so subs
//     installed after close() snapshotted c.channels leak in the hub (NumSubscriptions
//     stays > 0 after every client is gone);
//   - connectCmd writes connect fields (c.user) under c.mu while close reads them
//     unsynchronized (a torn string read).
//
// Run under -race.
func TestConnect_ServerSideSubs_CloseRace(t *testing.T) {
	channels := []string{"ss0", "ss1", "ss2", "ss3"}
	node, err := New(Config{LogLevel: LogLevelError, LogHandler: func(LogEntry) {}})
	require.NoError(t, err)
	node.OnConnecting(func(ctx context.Context, e ConnectEvent) (ConnectReply, error) {
		subs := make(map[string]SubscribeOptions, len(channels))
		for _, ch := range channels {
			subs[ch] = SubscribeOptions{}
		}
		return ConnectReply{Subscriptions: subs}, nil
	})
	require.NoError(t, node.Run())
	defer func() { _ = node.Shutdown(context.Background()) }()

	const workers = 16
	const rounds = 200
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for r := 0; r < rounds; r++ {
				ctx, cancel := context.WithCancel(context.Background())
				tr := &countingTransport{pingInterval: 50 * time.Millisecond, pongTimeout: -1}
				client, err := newClient(SetCredentials(ctx, &Credentials{UserID: "u" + strconv.Itoa(w)}), node, tr)
				if err != nil {
					cancel()
					return
				}
				// Close concurrently with connectCmd installing the server-side subs.
				go func() { _ = client.close(DisconnectForceNoReconnect) }()
				rw := testReplyWriterWrapper()
				_ = client.connectCmd(&protocol.ConnectRequest{}, &protocol.Command{}, time.Now(), rw.rw)
				client.triggerConnect()
				client.scheduleOnConnectTimers()
				_ = client.close(DisconnectForceNoReconnect)
				cancel()
			}
		}(w)
	}
	wg.Wait()

	require.Eventually(t, func() bool {
		return node.hub.NumClients() == 0 && node.hub.NumSubscriptions() == 0
	}, 5*time.Second, 10*time.Millisecond,
		"hub leak after connect/close race: NumClients=%d NumSubscriptions=%d",
		node.hub.NumClients(), node.hub.NumSubscriptions())
}

// ===========================================================================
// client_connect_status_race_test.go
// ===========================================================================

// TestClient_TriggerConnectStatusRace guards a data race on c.status.
// triggerConnect finalizes the connect by writing c.status while holding only
// connectMu, but c.status is otherwise read under c.mu (e.g. Client.Unsubscribe,
// reached from Node.Unsubscribe's fan-out). Since the connecting client is
// already registered in the hub before triggerConnect runs, a concurrent
// server-side hub operation reads c.status under c.mu while triggerConnect writes
// it under connectMu — different locks, a real race. Must be run under -race.
func TestClient_TriggerConnectStatusRace(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()
	// A non-nil connect handler makes triggerConnect take the branch that writes
	// c.status after invoking the handler.
	node.OnConnect(func(c *Client) {})

	const rounds = 4000
	for r := 0; r < rounds; r++ {
		client := newTestClient(t, node, "u") // status == statusConnecting
		var wg sync.WaitGroup
		wg.Add(2)
		// triggerConnect writes c.status; the other goroutine reads it under c.mu,
		// the way every real reader does (Unsubscribe, Subscribe, close, ...). Read
		// the field directly so the test needs no started writer.
		go func() { defer wg.Done(); client.triggerConnect() }()
		go func() {
			defer wg.Done()
			client.mu.RLock()
			_ = client.status
			client.mu.RUnlock()
		}()
		wg.Wait()
		_ = client.close(DisconnectForceNoReconnect)
	}
}

// ===========================================================================
// client_keyed_track_race_test.go
// ===========================================================================

// TestSharedPollTrack_AfterUnsubscribeLeak proves that a track whose OnTrack
// handler is still in flight can commit keyed state after the channel has been
// unsubscribed. The commit re-creates c.keyed.trackedKeys[channel] (and a hub
// track reservation) with no re-check that the channel is still a live keyed
// subscription; close() only cleans keyed channels still present in c.channels,
// so the re-added state leaks past unsubscribe/disconnect.
func TestSharedPollTrack_AfterUnsubscribeLeak(t *testing.T) {
	node := newTestNodeWithSharedPoll(t)

	var savedTrackCb TrackCallback
	trackReady := make(chan struct{})
	node.OnConnect(func(c *Client) {
		c.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options:           SubscribeOptions{ExpireAt: time.Now().Unix() + 3600},
				ClientSideRefresh: true,
			}, nil)
		})
		c.OnTrack(func(e TrackEvent, cb TrackCallback) {
			savedTrackCb = cb
			close(trackReady)
		})
	})

	client := newTestClientV2(t, node, "u")
	connectClientV2(t, client)

	const ch = "sp:trackleak"
	subscribeSharedPollClient(t, client, ch)

	// Track a key; OnTrack captures the callback and returns.
	go func() {
		rw := testReplyWriterWrapper()
		_ = client.handleSubRefresh(&protocol.SubRefreshRequest{
			Channel: ch,
			Type:    typeTrack,
			Track:   []*protocol.TrackBatch{{Items: []*protocol.KeyedItem{{Key: "k1", Version: 1}}}},
		}, &protocol.Command{Id: 2}, time.Now(), rw.rw)
	}()
	<-trackReady

	// Unsubscribe while the track is in flight (cleanupKeyed removes trackedKeys[ch]).
	rwU := testReplyWriterWrapper()
	require.NoError(t, client.handleUnsubscribe(
		&protocol.UnsubscribeRequest{Channel: ch}, &protocol.Command{Id: 3}, time.Now(), rwU.rw))

	// The track commit fires now, after the channel is gone.
	savedTrackCb(TrackReply{}, nil)
	time.Sleep(50 * time.Millisecond)

	client.mu.RLock()
	var leaked bool
	if client.keyed != nil {
		_, leaked = client.keyed.trackedKeys[ch]
	}
	_, stillSubscribed := client.channels[ch]
	client.mu.RUnlock()

	require.False(t, stillSubscribed, "sanity: channel should be unsubscribed")
	require.False(t, leaked,
		"keyed track state leaked: trackedKeys[%s] present after unsubscribe raced the in-flight track", ch)

	_ = client.close(DisconnectForceNoReconnect)
}

// ===========================================================================
// client_map_concurrent_recovery_test.go
// ===========================================================================

// TestMapSubscribe_ConcurrentRecoveryCreateConsistent exercises two concurrent
// recovery stream-phase subscribes for the same channel on one client — the race
// where both pass the lock-free hasState check and reach the on-the-fly state
// creation. The reservation re-check under c.mu makes the outcome well-defined
// (one installs, the other is rejected) rather than one goroutine proceeding on a
// state shadowed by the other's. Under -race this guards the create path against
// races, and the post-churn invariant guards it against leaks.
func TestMapSubscribe_ConcurrentRecoveryCreateConsistent(t *testing.T) {
	node, broker := newTestNodeWithMapBroker(t)
	setTestMapChannelOptionsConverging(node)
	node.OnConnect(func(c *Client) {
		c.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{Options: SubscribeOptions{Type: SubscriptionTypeMap}}, nil)
		})
	})
	ctx := context.Background()

	for iter := 0; iter < 120; iter++ {
		ch := fmt.Sprintf("crc_%d", iter)
		var epoch string
		for j := 0; j < 3; j++ {
			res, err := broker.Publish(ctx, ch, fmt.Sprintf("k%d", j), MapPublishOptions{Data: []byte(`{"v":"d"}`)})
			require.NoError(t, err)
			epoch = res.Position.Epoch
		}

		client := newTestConnectedClientV2(t, node, fmt.Sprintf("u%d", iter))

		var wg sync.WaitGroup
		for i := 0; i < 2; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				rw := testReplyWriterWrapper()
				_ = client.handleSubscribe(&protocol.SubscribeRequest{
					Channel: ch,
					Type:    int32(SubscriptionTypeMap),
					Phase:   MapPhaseStream,
					Recover: true,
					Offset:  1,
					Epoch:   epoch,
					Limit:   100,
				}, &protocol.Command{Id: 1}, time.Now(), rw.rw)
			}()
		}
		wg.Wait()

		urw := testReplyWriterWrapper()
		_ = client.handleUnsubscribe(&protocol.UnsubscribeRequest{Channel: ch}, &protocol.Command{Id: 2}, time.Now(), urw.rw)
		_ = client.close(DisconnectForceNoReconnect)
	}

	require.Eventually(t, func() bool {
		return node.hub.NumSubscriptions() == 0
	}, 10*time.Second, 20*time.Millisecond,
		"map subscriptions leaked after concurrent recovery-create churn: NumSubscriptions=%d",
		node.hub.NumSubscriptions())
}

// ===========================================================================
// client_map_consistency_test.go
// ===========================================================================

// TestMapSubConsistency_SubscribeVsUnsubscribe checks the same invariant the
// normal-subscription spec checks, but for map (keyed) subscriptions: after a
// concurrent subscribe/unsubscribe storm on the same channel settles, c.channels
// and the hub routing table must agree. Map subs currently carry subGen==0
// (never assigned), so the unsubscribe identity-match degrades to 0==0 and gives
// no protection against a lagging unsubscribe tearing down a fresh resubscribe.
func TestMapSubConsistency_SubscribeVsUnsubscribe(t *testing.T) {
	node, _ := newTestNodeWithMapBroker(t)
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{Options: SubscribeOptions{Type: SubscriptionTypeMap}}, nil)
		})
	})
	client := newTestConnectedClientV2(t, node, "u")
	const ch = "m"

	mapSubscribe := func() {
		rw := testReplyWriterWrapper()
		_ = client.handleSubscribe(&protocol.SubscribeRequest{
			Channel: ch, Type: int32(SubscriptionTypeMap), Phase: MapPhaseState, Limit: 100,
		}, &protocol.Command{Id: 1}, time.Now(), rw.rw)
	}
	unsubscribe := func() {
		rw := testReplyWriterWrapper()
		_ = client.handleUnsubscribe(&protocol.UnsubscribeRequest{Channel: ch}, &protocol.Command{Id: 1}, time.Now(), rw.rw)
	}

	const rounds = 2500
	for r := 0; r < rounds; r++ {
		var wg sync.WaitGroup
		for i := 0; i < 3; i++ {
			wg.Add(2)
			go func() { defer wg.Done(); mapSubscribe() }()
			go func() { defer wg.Done(); unsubscribe() }()
		}
		wg.Wait()

		present, _ := clientHasChannel(client, ch)
		inHub := hubHasSub(node, ch, client.ID())
		require.Equalf(t, present, inHub,
			"round %d: c.channels has %s=%v but hub has it=%v — inconsistent map subscription state",
			r, ch, present, inHub)

		unsubscribe() // reset
	}
}

// TestMapSubConsistency_NoLeakAfterDisconnect: after a client closes, it must be
// gone from the hub routing table for every map channel, even when map
// subscribe/unsubscribe raced the disconnect. A residual hub entry is a
// permanent leak.
func TestMapSubConsistency_NoLeakAfterDisconnect(t *testing.T) {
	node, _ := newTestNodeWithMapBroker(t)
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{Options: SubscribeOptions{Type: SubscriptionTypeMap}}, nil)
		})
	})
	channels := []string{"a", "b", "c"}

	const rounds = 500
	for r := 0; r < rounds; r++ {
		client := newTestConnectedClientV2(t, node, "u")
		var wg sync.WaitGroup
		for _, ch := range channels {
			wg.Add(2)
			go func(ch string) {
				defer wg.Done()
				rw := testReplyWriterWrapper()
				_ = client.handleSubscribe(&protocol.SubscribeRequest{
					Channel: ch, Type: int32(SubscriptionTypeMap), Phase: MapPhaseState, Limit: 100,
				}, &protocol.Command{Id: 1}, time.Now(), rw.rw)
			}(ch)
			go func(ch string) {
				defer wg.Done()
				rw := testReplyWriterWrapper()
				_ = client.handleUnsubscribe(&protocol.UnsubscribeRequest{Channel: ch}, &protocol.Command{Id: 1}, time.Now(), rw.rw)
			}(ch)
		}
		wg.Add(1)
		go func() { defer wg.Done(); _ = client.close(DisconnectForceNoReconnect) }()
		wg.Wait()
		_ = client.close(DisconnectForceNoReconnect)

		for _, ch := range channels {
			require.Falsef(t, hubHasSub(node, ch, client.ID()),
				"round %d: closed client still routed for map channel %s — leaked subscription", r, ch)
		}
	}
}

// ===========================================================================
// client_map_recovery_subgen_test.go
// ===========================================================================

// TestMapSubscribe_RecoveryStreamStateHasSubGen guards against a leaked map
// subscription. The recovery-mode stream phase creates its mapSubscribeState on
// the fly; that state must carry a real (non-zero) subGen, exactly like the
// normal state phase. If it were 0, handleMapTransitionToLive would mint a fresh
// generation at go-live time that no longer matches the targetSubGen an
// unsubscribe captured from the state while it was still loading — the
// unsubscribe would fail its identity check and leave the subscription in place.
func TestMapSubscribe_RecoveryStreamStateHasSubGen(t *testing.T) {
	t.Parallel()
	node, broker := newTestNodeWithMapBroker(t)
	setTestMapChannelOptionsConverging(node)

	channel := "test_recovery_subgen"
	ctx := context.Background()

	// Publish enough entries that the stream is well ahead, so a recovery stream
	// subscribe from offset 0 with a small page does NOT immediately go live and
	// instead stays in the loading (mapSubscribing) phase.
	var epoch string
	for i := 0; i < 20; i++ {
		res, err := broker.Publish(ctx, channel, string(rune('a'+i)), MapPublishOptions{
			Data: []byte(`{"v":"d"}`),
		})
		require.NoError(t, err)
		epoch = res.Position.Epoch
	}

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{Options: SubscribeOptions{Type: SubscriptionTypeMap}}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "user1")

	// First request is a recovery stream phase (no prior state), small limit so it
	// stays loading rather than transitioning straight to LIVE.
	result := subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: channel,
		Type:    int32(SubscriptionTypeMap),
		Phase:   MapPhaseStream,
		Offset:  0,
		Epoch:   epoch,
		Limit:   2,
		Recover: true,
	})
	require.Equal(t, MapPhaseStream, result.Phase, "sub should still be loading, not live")

	client.mu.RLock()
	st, ok := client.mapSubscribing[channel]
	var subGen uint64
	if ok {
		subGen = st.subGen
	}
	client.mu.RUnlock()

	require.True(t, ok, "channel should still be in mapSubscribing (loading)")
	require.NotZero(t, subGen, "recovery-mode map subscribe state must carry a non-zero subGen")
}

// ===========================================================================
// client_perchannel_writer_leak_test.go
// ===========================================================================

// TestChannelWriter_SizeFlushDoesNotLeakGoroutines guards the per-channel batch
// writer's timer lifecycle. When a batch reaches MaxSize before its MaxDelay
// timer fires, Add stops the timer — but the waitTimer goroutine is blocked on
// <-tm.C, which a stopped timer never delivers, so it must not be left hanging.
func TestChannelWriter_SizeFlushDoesNotLeakGoroutines(t *testing.T) {
	w := newChannelWriter(func([]queue.Item) error { return nil })
	config := ChannelBatchConfig{MaxSize: 4, MaxDelay: time.Second}

	time.Sleep(50 * time.Millisecond)
	base := runtime.NumGoroutine()

	const rounds = 200
	for r := 0; r < rounds; r++ {
		// The 4th item reaches MaxSize and flushes, cancelling the timer that the
		// 1st item started.
		for i := 0; i < 4; i++ {
			w.Add(queue.Item{FrameType: protocol.FrameTypePushPublication, Data: []byte("x")}, config)
		}
	}

	time.Sleep(150 * time.Millisecond) // let any exiting goroutines exit
	leaked := runtime.NumGoroutine() - base
	require.Less(t, leaked, rounds/4,
		"leaked ~%d goroutines over %d size-triggered flushes — waitTimer stays blocked on <-tm.C after the timer is stopped",
		leaked, rounds)
}

// ===========================================================================
// client_position_tick_test.go
// ===========================================================================

// countingBroker wraps a Broker and counts History calls, which is how the
// periodic stream position check reaches the broker (node.checkPosition ->
// node.streamTop -> Broker.History).
type countingBroker struct {
	Broker
	historyCalls atomic.Int64
}

func (b *countingBroker) History(ch string, opts HistoryOptions) ([]*Publication, StreamPosition, error) {
	b.historyCalls.Add(1)
	return b.Broker.History(ch, opts)
}

func newPositionTestNode(t *testing.T) (*Node, *countingBroker) {
	node, err := New(Config{
		LogLevel:                        LogLevelError,
		LogHandler:                      func(entry LogEntry) {},
		ClientPresenceUpdateInterval:    10 * time.Millisecond,
		ClientChannelPositionCheckDelay: 10 * time.Millisecond,
		ClientChannelPositionMaxTimeLag: 5 * time.Second,
	})
	require.NoError(t, err)
	cb := &countingBroker{Broker: node.broker}
	node.SetBroker(cb)
	require.NoError(t, node.Run())
	return node, cb
}

// TestPositionTick_ChecksPositionWithoutPresence covers a positioning channel
// that has no presence at all. The tick snapshots only channels that need work,
// so positioning must keep a channel in the tick on its own. Without this the
// stream position would silently stop being verified.
func TestPositionTick_ChecksPositionWithoutPresence(t *testing.T) {
	t.Parallel()
	node, cb := newPositionTestNode(t)
	defer func() { _ = node.Shutdown(context.Background()) }()

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{Options: SubscribeOptions{
				EnablePositioning: true,
				// Deliberately no EmitPresence.
			}}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "42")
	subscribeClientV2(t, client, "test")
	before := cb.historyCalls.Load()

	require.Eventually(t, func() bool {
		return cb.historyCalls.Load() > before
	}, 3*time.Second, 10*time.Millisecond,
		"stream position never checked for a positioning channel without presence — tick skipped it")
}

// TestPositionTick_InsufficientStateViaTick drives the full path: the tick
// detects a client whose stream position no longer matches the broker's stream
// top and unsubscribes it with insufficient state.
func TestPositionTick_InsufficientStateViaTick(t *testing.T) {
	t.Parallel()
	node, _ := newPositionTestNode(t)
	defer func() { _ = node.Shutdown(context.Background()) }()

	unsubbed := make(chan uint32, 1)
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{Options: SubscribeOptions{
				EnablePositioning: true,
			}}, nil)
		})
		client.OnUnsubscribe(func(event UnsubscribeEvent) {
			select {
			case unsubbed <- event.Code:
			default:
			}
		})
	})

	client := newTestConnectedClientV2(t, node, "42")
	subscribeClientV2(t, client, "test")

	// Simulate a gap: move the client's tracked offset ahead of the broker's
	// stream top so the next position check must fail.
	client.mu.Lock()
	chCtx := client.channels["test"]
	chCtx.streamPosition.Offset = 9999
	chCtx.positionCheckTime = 0 // force a check on the next tick
	client.channels["test"] = chCtx
	client.mu.Unlock()

	select {
	case code := <-unsubbed:
		require.Equal(t, unsubscribeInsufficientState.Code, code,
			"expected insufficient state unsubscribe from periodic position check")
	case <-time.After(5 * time.Second):
		t.Fatal("tick never detected insufficient stream position")
	}
}

// TestPositionTick_PositionCheckTimeAdvances verifies the tick keeps refreshing
// positionCheckTime on a valid position, which is what prevents a valid client
// from being torn down by ClientChannelPositionMaxTimeLag.
func TestPositionTick_PositionCheckTimeAdvances(t *testing.T) {
	t.Parallel()
	node, _ := newPositionTestNode(t)
	defer func() { _ = node.Shutdown(context.Background()) }()

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{Options: SubscribeOptions{EnablePositioning: true}}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "42")
	subscribeClientV2(t, client, "test")

	client.mu.Lock()
	chCtx := client.channels["test"]
	chCtx.positionCheckTime = 0
	client.channels["test"] = chCtx
	client.mu.Unlock()

	require.Eventually(t, func() bool {
		client.mu.RLock()
		defer client.mu.RUnlock()
		return client.channels["test"].positionCheckTime > 0
	}, 3*time.Second, 10*time.Millisecond,
		"positionCheckTime never advanced — position check did not run via tick")
}

// ===========================================================================
// client_presence_issue_test.go
// ===========================================================================

// slowPresenceManager simulates a Redis PresenceManager with a fixed RTT. The
// RTT is atomic so a test can add latency only after the setup phase (each
// subscribe with EmitPresence pays one AddPresence too).
type slowPresenceManager struct {
	rttNanos atomic.Int64
	calls    atomic.Int64
}

func newSlowPresenceManager(rtt time.Duration) *slowPresenceManager {
	m := &slowPresenceManager{}
	m.setRTT(rtt)
	return m
}

func (m *slowPresenceManager) setRTT(rtt time.Duration) {
	m.rttNanos.Store(int64(rtt))
}

func (m *slowPresenceManager) Presence(_ string) (map[string]*ClientInfo, error) {
	return nil, nil
}
func (m *slowPresenceManager) PresenceStats(_ string) (PresenceStats, error) {
	return PresenceStats{}, nil
}
func (m *slowPresenceManager) AddPresence(_ string, _ string, _ *ClientInfo) error {
	m.calls.Add(1)
	if rtt := m.rttNanos.Load(); rtt > 0 {
		time.Sleep(time.Duration(rtt))
	}
	return nil
}
func (m *slowPresenceManager) RemovePresence(_ string, _ string, _ string) error { return nil }

func newSlowPresenceNode(t *testing.T, pm PresenceManager, presenceIvl time.Duration) *Node {
	node, err := New(Config{
		LogLevel:                     LogLevelError,
		LogHandler:                   func(entry LogEntry) {},
		ClientChannelLimit:           1000,
		ClientPresenceUpdateInterval: presenceIvl,
	})
	require.NoError(t, err)
	node.SetPresenceManager(pm)
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{Options: SubscribeOptions{EmitPresence: true}}, nil)
		})
	})
	require.NoError(t, node.Run())
	return node
}

// TestPresenceLoop_DelaysOwnPing proves the serial per-channel presence loop
// delays the connection's own ping, because ping and presence share one timer
// and updatePresence only re-arms it after every Redis round trip has drained.
func TestPresenceLoop_DelaysOwnPing(t *testing.T) {
	const numChannels = 200
	const rtt = 2 * time.Millisecond
	const pingInterval = 300 * time.Millisecond

	pm := newSlowPresenceManager(rtt)
	node := newSlowPresenceNode(t, pm, 400*time.Millisecond)
	defer func() { _ = node.Shutdown(context.Background()) }()

	transport := newTestTransport(func() {})
	transport.setProtocolType(ProtocolTypeJSON)
	transport.setProtocolVersion(ProtocolVersion2)
	transport.setPing(pingInterval, 0)
	sink := make(chan []byte, 4096)
	transport.setSink(sink)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := newTestConnectedClientWithTransport(t, ctx, node, transport, "user")
	for i := 0; i < numChannels; i++ {
		subscribeClientV2(t, client, "ch"+strconv.Itoa(i))
	}

	var pingTimes []time.Time
	done := time.After(3 * time.Second)
loop:
	for {
		select {
		case data := <-sink:
			if string(data) == "{}\n" || string(data) == "{}" {
				pingTimes = append(pingTimes, time.Now())
			}
		case <-done:
			break loop
		}
	}

	require.Greater(t, len(pingTimes), 2, "expected several pings")
	var maxGap time.Duration
	for i := 1; i < len(pingTimes); i++ {
		if gap := pingTimes[i].Sub(pingTimes[i-1]); gap > maxGap {
			maxGap = gap
		}
	}
	t.Logf("ping interval %v, %d channels @ %v RTT, max observed ping gap: %v (%.2fx)",
		pingInterval, numChannels, rtt, maxGap, float64(maxGap)/float64(pingInterval))

	require.Less(t, maxGap, time.Duration(float64(pingInterval)*1.5),
		"ping was delayed by the serial presence loop")
}

// TestPresenceLoop_BlocksClose proves close() no longer waits for the whole
// presence loop: without the closing signal both take presenceMu, delaying
// connection cleanup by N*RTT.
func TestPresenceLoop_BlocksClose(t *testing.T) {
	const numChannels = 200
	const rtt = 10 * time.Millisecond
	// Serial drain of the presence loop — what close() used to wait for.
	const serialDrain = numChannels * rtt // 2s

	// No latency while subscribing — only the presence tick below should pay RTT.
	pm := newSlowPresenceManager(0)
	node := newSlowPresenceNode(t, pm, 25*time.Second)
	defer func() { _ = node.Shutdown(context.Background()) }()

	client := newTestConnectedClientV2(t, node, "user")
	for i := 0; i < numChannels; i++ {
		subscribeClientV2(t, client, "ch"+strconv.Itoa(i))
	}
	pm.setRTT(rtt)
	callsBefore := pm.calls.Load()

	closeBlocked := make(chan time.Duration, 1)
	go func() {
		time.Sleep(20 * time.Millisecond) // let the presence loop get going
		s := time.Now()
		_ = client.close(DisconnectForceNoReconnect)
		closeBlocked <- time.Since(s)
	}()
	loopStart := time.Now()
	client.updatePresence()
	loopDuration := time.Since(loopStart)
	blocked := <-closeBlocked
	calls := pm.calls.Load() - callsBefore

	t.Logf("presence loop took %v; close() blocked for %v; %d/%d presence calls issued",
		loopDuration, blocked, calls, numChannels)

	// Primary, timing-independent check: close() cut the loop short, so most
	// channels never got their presence call.
	require.Less(t, calls, int64(numChannels/2),
		"presence loop kept issuing updates after close() started")
	// Coarse timing guard. The threshold is deliberately loose: close() also does
	// its own O(channels) unsubscribe work, which on a loaded CI runner under
	// -race takes >100ms on its own and has nothing to do with presenceMu. Only
	// a regression back to draining the whole loop (serialDrain) must trip this.
	require.Less(t, blocked, serialDrain/4,
		"close() was blocked waiting for the presence loop to drain")
}

// ===========================================================================
// client_presence_stress_test.go
// ===========================================================================

// trackingPresenceManager records live presence entries so leaks (an entry
// re-added by a presence tick after close removed it) can be detected, and can
// simulate PresenceManager latency.
type trackingPresenceManager struct {
	mu      sync.Mutex
	entries map[string]map[string]struct{} // channel -> clientID
	rtt     time.Duration
	adds    atomic.Int64
	removes atomic.Int64
}

func newTrackingPresenceManager(rtt time.Duration) *trackingPresenceManager {
	return &trackingPresenceManager{
		entries: map[string]map[string]struct{}{},
		rtt:     rtt,
	}
}

func (m *trackingPresenceManager) Presence(_ string) (map[string]*ClientInfo, error) {
	return nil, nil
}
func (m *trackingPresenceManager) PresenceStats(_ string) (PresenceStats, error) {
	return PresenceStats{}, nil
}
func (m *trackingPresenceManager) AddPresence(ch string, clientID string, _ *ClientInfo) error {
	if m.rtt > 0 {
		time.Sleep(m.rtt)
	}
	m.adds.Add(1)
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.entries[ch] == nil {
		m.entries[ch] = map[string]struct{}{}
	}
	m.entries[ch][clientID] = struct{}{}
	return nil
}
func (m *trackingPresenceManager) RemovePresence(ch string, clientID string, _ string) error {
	m.removes.Add(1)
	m.mu.Lock()
	defer m.mu.Unlock()
	if s, ok := m.entries[ch]; ok {
		delete(s, clientID)
		if len(s) == 0 {
			delete(m.entries, ch)
		}
	}
	return nil
}
func (m *trackingPresenceManager) liveEntries() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, s := range m.entries {
		n += len(s)
	}
	return n
}

type stressOpts struct {
	useWheel    bool
	rtt         time.Duration
	numWorkers  int
	numChannels int
	duration    time.Duration
	concurrency int
	positioning bool
	// explicitUnsub races an explicit unsubscribe() against an in-flight presence
	// tick. Unlike close(), unsubscribe() does not take presenceMu, so a tick
	// that already passed its c.channels[ch] check can re-add presence after
	// unsubscribe removed it. That resurrection race is PRE-EXISTING on master
	// (verified by A/B) and is not what these tests are guarding, so it is off by
	// default. See TestPresenceStress_KnownUnsubscribeRace.
	explicitUnsub bool
}

// runPresenceStress hammers connect/subscribe/unsubscribe/disconnect against a
// slow PresenceManager while presence ticks run, exercising the presenceInFlight
// guard and the closing flag under contention.
func runPresenceStress(t *testing.T, o stressOpts) {
	pm := newTrackingPresenceManager(o.rtt)
	cfg := Config{
		LogLevel:           LogLevelError,
		LogHandler:         func(entry LogEntry) {},
		ClientChannelLimit: 1000,
		// Deliberately far shorter than the time a tick takes with o.rtt, so
		// ticks overlap and the in-flight guard is exercised hard.
		ClientPresenceUpdateInterval:    5 * time.Millisecond,
		clientPresenceUpdateConcurrency: o.concurrency,
		clientPositionCheckConcurrency:  o.concurrency,
	}
	if o.positioning {
		// Force the position-check path to actually run on every tick, so the
		// concurrent dutyPosition workers are exercised under churn.
		cfg.ClientChannelPositionCheckDelay = time.Millisecond
	}
	var wheel *testSharedTimerScheduler
	if o.useWheel {
		wheel = newTestSharedTimerScheduler(4)
		cfg.ClientTimerScheduler = wheel
	}
	node, err := New(cfg)
	require.NoError(t, err)
	node.SetPresenceManager(pm)
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{Options: SubscribeOptions{EmitPresence: true, EnablePositioning: o.positioning}}, nil)
		})
	})
	require.NoError(t, node.Run())
	defer func() {
		_ = node.Shutdown(context.Background())
		if wheel != nil {
			wheel.Stop()
		}
	}()

	var wg sync.WaitGroup
	stop := make(chan struct{})
	var cycles atomic.Int64

	for w := 0; w < o.numWorkers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			rnd := rand.New(rand.NewSource(int64(w)))
			for {
				select {
				case <-stop:
					return
				default:
				}
				ctx, cancel := context.WithCancel(context.Background())
				tr := &countingTransport{pingInterval: 20 * time.Millisecond, pongTimeout: -1}
				newCtx := SetCredentials(ctx, &Credentials{UserID: "u" + strconv.Itoa(w)})
				client, err := newClient(newCtx, node, tr)
				if err != nil {
					cancel()
					return
				}
				rw := testReplyWriterWrapper()
				if err := client.connectCmd(&protocol.ConnectRequest{}, &protocol.Command{}, time.Now(), rw.rw); err != nil {
					cancel()
					return
				}
				client.triggerConnect()
				client.scheduleOnConnectTimers()
				for i := 0; i < o.numChannels; i++ {
					srw := testReplyWriterWrapper()
					_ = client.handleSubscribe(&protocol.SubscribeRequest{
						Channel: "ch" + strconv.Itoa(w) + "_" + strconv.Itoa(i),
					}, &protocol.Command{Id: 1}, time.Now(), srw.rw)
				}
				// Live long enough for several presence ticks to start.
				time.Sleep(time.Duration(rnd.Intn(20)+5) * time.Millisecond)
				if o.explicitUnsub && rnd.Intn(2) == 0 && o.numChannels > 0 {
					_ = client.unsubscribe("ch"+strconv.Itoa(w)+"_0", unsubscribeClient, nil)
				}
				_ = client.close(DisconnectForceNoReconnect)
				cancel()
				cycles.Add(1)
			}
		}(w)
	}

	time.Sleep(o.duration)
	close(stop)
	wg.Wait()

	// All clients closed: presence entries must not be left behind. A leak here
	// means a presence tick re-added an entry after close removed it.
	require.Eventually(t, func() bool {
		return pm.liveEntries() == 0
	}, 5*time.Second, 20*time.Millisecond,
		"presence entries leaked after all clients closed: %d live (adds=%d removes=%d)",
		pm.liveEntries(), pm.adds.Load(), pm.removes.Load())

	require.Zero(t, node.hub.NumClients(), "clients left in hub after close")
	require.Greater(t, cycles.Load(), int64(0), "stress did no work")
	t.Logf("wheel=%v rtt=%v: %d connect/subscribe/close cycles, presence adds=%d removes=%d, no leaks",
		o.useWheel, o.rtt, cycles.Load(), pm.adds.Load(), pm.removes.Load())
}

func TestPresenceStress_Runtime(t *testing.T) {
	runPresenceStress(t, stressOpts{
		useWheel: false, rtt: 2 * time.Millisecond,
		numWorkers: 16, numChannels: 8, duration: 3 * time.Second,
	})
}

func TestPresenceStress_Scheduler(t *testing.T) {
	runPresenceStress(t, stressOpts{
		useWheel: true, rtt: 2 * time.Millisecond,
		numWorkers: 16, numChannels: 8, duration: 3 * time.Second,
	})
}

// Zero-latency variant maximizes tick throughput and lock churn.
func TestPresenceStress_Runtime_FastPM(t *testing.T) {
	runPresenceStress(t, stressOpts{
		useWheel: false, rtt: 0,
		numWorkers: 32, numChannels: 4, duration: 3 * time.Second,
	})
}

// TestPresenceStress_UnsubscribeRace covers the presence resurrection race: an
// explicit unsubscribe() does not take presenceMu, so a presence tick already
// past its c.channels[ch] existence check can call AddPresence after unsubscribe
// removed the entry, leaving it to linger until PresenceTTL (60s by default).
//
// This race pre-dates the presence tick changes — A/B against master leaked
// 78/55 entries here — and is fixed by the post-add re-check in
// updateChannelPresence (see removeRacedPresence).
func TestPresenceStress_UnsubscribeRace(t *testing.T) {
	runPresenceStress(t, stressOpts{
		useWheel: false, rtt: 2 * time.Millisecond, explicitUnsub: true,
		numWorkers: 16, numChannels: 8, duration: 3 * time.Second,
	})
}

func TestPresenceStress_UnsubscribeRace_Scheduler(t *testing.T) {
	runPresenceStress(t, stressOpts{
		useWheel: true, rtt: 2 * time.Millisecond, explicitUnsub: true,
		numWorkers: 16, numChannels: 8, duration: 3 * time.Second,
	})
}

// Concurrent presence updates: several updates per connection are in flight at
// once, so the unsubscribe race, the closing bail and the compensation all run
// against parallel workers touching the same snapshot.
func TestPresenceStress_Concurrent(t *testing.T) {
	runPresenceStress(t, stressOpts{
		useWheel: false, rtt: 2 * time.Millisecond, concurrency: 8,
		numWorkers: 16, numChannels: 8, duration: 3 * time.Second,
	})
}

func TestPresenceStress_Concurrent_UnsubscribeRace(t *testing.T) {
	runPresenceStress(t, stressOpts{
		useWheel: false, rtt: 2 * time.Millisecond, concurrency: 8, explicitUnsub: true,
		numWorkers: 16, numChannels: 8, duration: 3 * time.Second,
	})
}

func TestPresenceStress_Concurrent_Scheduler(t *testing.T) {
	runPresenceStress(t, stressOpts{
		useWheel: true, rtt: 2 * time.Millisecond, concurrency: 8, explicitUnsub: true,
		numWorkers: 16, numChannels: 8, duration: 3 * time.Second,
	})
}

// TestPresenceStress_KitchenSink turns on everything at once: the shared-goroutine scheduler
// (offloaded ticks), presence + position concurrency, and an explicit
// unsubscribe race, under churn. It is the broadest -race exercise of the tick
// machinery: presence workers, position workers, the closing bail, and
// compensation all run concurrently against connect/subscribe/unsubscribe/close.
func TestPresenceStress_KitchenSink(t *testing.T) {
	runPresenceStress(t, stressOpts{
		useWheel: true, rtt: time.Millisecond, concurrency: 8,
		positioning: true, explicitUnsub: true,
		numWorkers: 16, numChannels: 8, duration: 3 * time.Second,
	})
}

// ===========================================================================
// client_presence_tick_test.go
// ===========================================================================

// The periodic tick snapshots only channels that need work (see
// channelNeedsPresenceTick). These tests pin the non-presence reasons a channel
// must still be included, so the snapshot filter can not silently drop them.

// TestPresenceTick_ExpiresSubWithoutPresence covers a subscription that has an
// expiration but no presence at all. Existing coverage (TestClientSubExpired)
// enables presence on the same channel, so it would pass even if the tick only
// considered presence-enabled channels.
func TestPresenceTick_ExpiresSubWithoutPresence(t *testing.T) {
	t.Parallel()
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	node.config.ClientExpiredSubCloseDelay = 0
	node.config.ClientPresenceUpdateInterval = 10 * time.Millisecond

	doneCh := make(chan struct{})
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(event SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					ExpireAt: time.Now().Unix() + 1,
					// Deliberately no EmitPresence: expiration alone must keep
					// this channel in the tick.
				},
			}, nil)
		})
		client.OnUnsubscribe(func(event UnsubscribeEvent) {
			if event.Code == UnsubscribeCodeExpired {
				close(doneCh)
			}
		})
	})

	client := newTestClient(t, node, "42")
	connectClientV2(t, client)
	subscribeClientV2(t, client, "test")

	select {
	case <-doneCh:
	case <-time.After(5 * time.Second):
		t.Fatal("subscription without presence never expired — tick skipped the channel")
	}
}

// TestPresenceTick_SkipsChannelWithNoWork is the complement: a plain channel
// with no presence, no expiration and no positioning must be skipped entirely,
// which is what makes the tick allocation-free for such connections.
func TestPresenceTick_SkipsChannelWithNoWork(t *testing.T) {
	t.Parallel()
	require.Zero(t, channelTickDuties(
		ChannelContext{flags: flagSubscribed}, true, true),
		"plain subscribed channel should need no tick work")

	require.NotZero(t, channelTickDuties(
		ChannelContext{flags: flagSubscribed | flagEmitPresence}, true, true),
		"channel with presence must be ticked")
	require.NotZero(t, channelTickDuties(
		ChannelContext{flags: flagSubscribed | flagMapClientPresence}, true, true),
		"channel with map client presence must be ticked")
	require.NotZero(t, channelTickDuties(
		ChannelContext{flags: flagSubscribed | flagMapUserPresence}, true, true),
		"channel with map user presence must be ticked")
	require.NotZero(t, channelTickDuties(
		ChannelContext{flags: flagSubscribed, expireAt: 1}, true, true),
		"channel with expiration must be ticked")
	require.NotZero(t, channelTickDuties(
		ChannelContext{flags: flagSubscribed | flagPositioning}, true, true),
		"channel with positioning must be ticked when position checks are enabled")
	require.NotZero(t, channelTickDuties(
		ChannelContext{flags: flagSubscribed | flagKeyed}, true, true),
		"keyed channel must be ticked when shared poll is configured")

	// Config-gated reasons must not force a tick when the feature is disabled.
	require.Zero(t, channelTickDuties(
		ChannelContext{flags: flagSubscribed | flagPositioning}, false, true),
		"positioning should not need a tick when position checks are disabled")
	require.Zero(t, channelTickDuties(
		ChannelContext{flags: flagSubscribed | flagKeyed}, true, false),
		"keyed channel should not need a tick without shared poll options")
}

// TestPresenceTick_KeepsRefreshingPresence guards the re-arm move: the tick now
// re-arms at the start of updatePresence rather than at the end, so presence
// must still be refreshed repeatedly rather than once.
func TestPresenceTick_KeepsRefreshingPresence(t *testing.T) {
	t.Parallel()
	pm := newSlowPresenceManager(0)
	node, err := New(Config{
		LogLevel:                     LogLevelError,
		LogHandler:                   func(entry LogEntry) {},
		ClientPresenceUpdateInterval: 20 * time.Millisecond,
	})
	require.NoError(t, err)
	node.SetPresenceManager(pm)
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{Options: SubscribeOptions{EmitPresence: true}}, nil)
		})
	})
	require.NoError(t, node.Run())
	defer func() { _ = node.Shutdown(context.Background()) }()

	client := newTestConnectedClientV2(t, node, "user")
	subscribeClientV2(t, client, "test")

	// Presence is added once on subscribe; ticks must keep adding after that.
	require.Eventually(t, func() bool {
		return pm.calls.Load() >= 4
	}, 3*time.Second, 10*time.Millisecond,
		"presence stopped being refreshed — tick did not re-arm (got %d calls)", pm.calls.Load())
}

// TestPresenceTickDefaultIsAllocationFree pins the promise that the default
// (sequential) tick costs nothing beyond the work it dispatches: the concurrency
// knobs must not add allocations when they are off, which is the default.
//
// A channel with no tick duties is used, so the only thing measured is the tick
// machinery itself — snapshot, duty dispatch and compensation.
func TestPresenceTickDefaultIsAllocationFree(t *testing.T) {
	node, err := New(Config{
		LogLevel:           LogLevelError,
		LogHandler:         func(entry LogEntry) {},
		ClientChannelLimit: 1000,
		// clientPresenceUpdateConcurrency / clientPositionCheckConcurrency left at
		// their zero value on purpose: this is the default configuration.
	})
	require.NoError(t, err)
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{}, nil) // no presence, no expiry, no positioning
		})
	})
	require.NoError(t, node.Run())
	defer func() { _ = node.Shutdown(context.Background()) }()

	client := newTestConnectedClientV2(t, node, "user")
	for i := 0; i < 32; i++ {
		subscribeClientV2(t, client, "ch"+strconv.Itoa(i))
	}
	for i := 0; i < 50; i++ { // warm the snapshot pool
		client.updatePresence()
	}

	allocs := testing.AllocsPerRun(200, func() {
		client.updatePresence()
	})
	require.Zero(t, allocs, "default presence tick must not allocate, got %v allocs/op", allocs)
}

// ===========================================================================
// client_recovery_deadlock_test.go
// ===========================================================================

// TestSubConsistency_NoDeadlockRecoveryCloseBroadcast guards the lock order
// between a recovering subscribe's close-path and the broadcast path.
//
// A positioned/recovering subscribe holds pubBufferMu (LockBufferAndReadBuffered)
// until StopBuffering. commitSubscription's closed-path (client closed mid
// subscribe) removes the hub entry, which takes subShard.mu — and the broadcast
// path holds subShard.mu while taking pubBufferMu (SyncPublication). Removing the
// hub entry with the buffer still locked inverts that order and deadlocks.
//
// Here recovery is enabled, a subscribe races close, and broadcasters publish to
// the same channel continuously. A deadlocking implementation hangs; the timeout
// turns that into a failure.
func TestSubConsistency_NoDeadlockRecoveryCloseBroadcast(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()
	node.OnConnect(func(c *Client) {
		c.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{Options: SubscribeOptions{EnableRecovery: true, EnablePositioning: true}}, nil)
		})
	})
	const ch = "b"

	stop := make(chan struct{})
	var pubWG sync.WaitGroup
	for i := 0; i < 4; i++ {
		pubWG.Add(1)
		go func() {
			defer pubWG.Done()
			for {
				select {
				case <-stop:
					return
				default:
					_, _ = node.Publish(ch, []byte(`{"m":1}`), WithHistory(10, time.Minute))
				}
			}
		}()
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		for r := 0; r < 3000; r++ {
			client := newTestClientV2(t, node, "u")
			connectClientV2(t, client)
			var wg sync.WaitGroup
			wg.Add(2)
			go func() {
				defer wg.Done()
				rw := testReplyWriterWrapper()
				_ = client.handleSubscribe(&protocol.SubscribeRequest{Channel: ch, Recover: true}, &protocol.Command{Id: 1}, time.Now(), rw.rw)
			}()
			go func() {
				defer wg.Done()
				_ = client.close(DisconnectForceNoReconnect)
			}()
			wg.Wait()
			_ = client.close(DisconnectForceNoReconnect)
		}
	}()

	select {
	case <-done:
	case <-time.After(20 * time.Second):
		close(stop)
		pubWG.Wait()
		t.Fatal("timeout — likely a lock-order deadlock between a recovering subscribe's close-path and broadcast")
	}
	close(stop)
	pubWG.Wait()
}

// ===========================================================================
// client_recovery_epoch_race_test.go
// ===========================================================================

// TestSubscribeRecovery_FirstPubEpochRace guards against a data race between the
// first publication's live-broadcast path and a concurrent recovering subscribe.
//
// For offset==1 the node stamps the channel epoch onto the publication so live
// subscribers learn it. With the MemoryBroker the very same *Publication pointer
// is already stored in history (historyHub.add runs before HandlePublication),
// so a subscribe-time recovery read of offset 1 (pubToProto in isStreamRecovered)
// races that stamp. The fix stamps the epoch onto the per-broadcast proto copy
// instead of mutating the shared history object.
//
// Each iteration uses a fresh channel and fires its first publish concurrently
// with a recovering subscribe, so the epoch stamp and the recovery read overlap.
// Run under -race.
func TestSubscribeRecovery_FirstPubEpochRace(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()
	node.OnConnect(func(c *Client) {
		c.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{Options: SubscribeOptions{EnableRecovery: true, EnablePositioning: true}}, nil)
		})
	})

	const iterations = 300
	for i := 0; i < iterations; i++ {
		ch := "epochrace_" + strconv.Itoa(i)
		client := newTestClientV2(t, node, "u")
		connectClientV2(t, client)

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			// First publish to a fresh channel => offset 1 => epoch stamp path.
			_, _ = node.Publish(ch, []byte(`{"m":1}`), WithHistory(10, time.Minute))
		}()
		go func() {
			defer wg.Done()
			rw := testReplyWriterWrapper()
			_ = client.handleSubscribe(&protocol.SubscribeRequest{Channel: ch, Recover: true}, &protocol.Command{Id: 1}, time.Now(), rw.rw)
		}()
		wg.Wait()
		_ = client.close(DisconnectForceNoReconnect)
	}

	require.Eventually(t, func() bool { return node.hub.NumSubscriptions() == 0 }, 4*time.Second, 10*time.Millisecond,
		"leaked hub subscription: NumSubscriptions=%d", node.hub.NumSubscriptions())
}

// ===========================================================================
// client_recovery_server_filter_test.go
// ===========================================================================

// TestBroadcastFiltered_MultipleKeys_NoNilBufferedPub guards a nil-pointer crash
// on the buffered recovery path. When a broadcast produces more than one
// prepared key among filtered subscribers (e.g. a JSON and a Protobuf client on
// a channel with a ServerTagsFilter), the filtered-pub marker used to keep the
// recovery buffer's offset continuity must be attached to EVERY filtered key's
// prepared data. Previously it was attached only to the first filtered key, so a
// subscriber on any later key buffered a nil publication, which then paniced in
// recovery.MergePublications during the subscribe reply.
//
// Both subscribers are put into the buffering window before the broadcast, so
// whichever key is iterated second is the one that would buffer nil — making the
// assertion independent of the (randomized) subscriber map order.
func TestBroadcastFiltered_MultipleKeys_NoNilBufferedPub(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()
	node.OnConnect(func(c *Client) {
		c.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{Options: SubscribeOptions{
				ServerTagsFilter: &FilterNode{Key: "team", Cmp: "eq", Val: "eng"},
			}}, nil)
		})
	})

	const ch = "broadcast_filtered_multikey"

	// Two subscribers with distinct prepared keys: JSON and Protobuf.
	jsonTransport := newTestTransport(func() {})
	jsonTransport.setProtocolType(ProtocolTypeJSON)
	jsonClient := newTestConnectedClientWithTransport(t, context.Background(), node, jsonTransport, "json-user")
	subscribeClientV2(t, jsonClient, ch)

	pbTransport := newTestTransport(func() {})
	pbTransport.setProtocolType(ProtocolTypeProtobuf)
	pbClient := newTestConnectedClientWithTransport(t, context.Background(), node, pbTransport, "pb-user")
	subscribeClientV2(t, pbClient, ch)

	// Enter the buffering window for both so the broadcast is buffered, not sent.
	jsonClient.pubSubSync.StartBuffering(ch)
	pbClient.pubSubSync.StartBuffering(ch)

	// Broadcast a publication excluded by the server tags filter (team=sales).
	err := node.hub.broadcastPublication(
		ch,
		StreamPosition{Offset: 1, Epoch: "e"},
		&Publication{Data: []byte(`{"n":1}`), Offset: 1, Tags: map[string]string{"team": "sales"}},
		nil, nil, ChannelBatchConfig{},
	)
	require.NoError(t, err)

	// Neither buffer may contain a nil publication, and merging must not panic.
	for _, c := range []*Client{jsonClient, pbClient} {
		buffered := c.pubSubSync.LockBufferAndReadBuffered(ch)
		for _, p := range buffered {
			require.NotNil(t, p, "filtered broadcast buffered a nil publication (missing filteredPub marker)")
		}
		_, _, ok := recovery.MergePublications(nil, buffered)
		require.True(t, ok)
		c.pubSubSync.StopBuffering(ch)
	}
}

// TestClientSubscribeRecovery_ServerTagsFilterApplied guards against a filter
// bypass on recovery. A regular (non-map) subscription with a ServerTagsFilter
// must, on recovery, receive only publications matching that server filter —
// exactly as the live broadcast path filters them. isStreamRecovered previously
// applied only the client tags filter, so server-filtered publications were
// recovered and delivered on reconnect.
func TestClientSubscribeRecovery_ServerTagsFilterApplied(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()
	node.OnConnect(func(c *Client) {
		c.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{Options: SubscribeOptions{
				EnableRecovery:    true,
				EnablePositioning: true,
				ServerTagsFilter:  &FilterNode{Key: "team", Cmp: "eq", Val: "eng"},
			}}, nil)
		})
	})

	const ch = "recovery_server_filter"
	// Publish three tagged pubs with history; the "sales" one must be filtered out.
	_, err := node.Publish(ch, []byte(`{"n":1}`), WithHistory(10, time.Minute), WithTags(map[string]string{"team": "eng"}))
	require.NoError(t, err)
	_, err = node.Publish(ch, []byte(`{"n":2}`), WithHistory(10, time.Minute), WithTags(map[string]string{"team": "sales"}))
	require.NoError(t, err)
	_, err = node.Publish(ch, []byte(`{"n":3}`), WithHistory(10, time.Minute), WithTags(map[string]string{"team": "eng"}))
	require.NoError(t, err)

	client := newTestClientV2(t, node, "u")
	connectClientV2(t, client)

	rw := testReplyWriterWrapper()
	require.NoError(t, client.handleSubscribe(
		&protocol.SubscribeRequest{Channel: ch, Recover: true},
		&protocol.Command{Id: 1}, time.Now(), rw.rw))
	require.NotEmpty(t, rw.replies)
	res := rw.replies[0].Subscribe
	require.NotNil(t, res)
	require.True(t, res.Recovered, "should have recovered")

	// Only the team=eng publications (offsets 1 and 3) should be delivered; the
	// team=sales one (offset 2) is filtered by the server tags filter.
	require.Len(t, res.Publications, 2, "server tags filter not applied on recovery")
	for _, p := range res.Publications {
		require.Equal(t, "eng", p.Tags["team"],
			"recovered a publication that the server tags filter should have excluded")
	}
}

// TestClientSubscribeCacheRecovery_ServerTagsFilterApplied guards the same
// filter bypass on the RecoveryModeCache path. Cache recovery delivers the
// newest publication, but recoverCache applied only the client tags filter, so
// a server-filtered latest publication was delivered on recovery. With the
// filter applied, cache recovery must fall back to the newest publication that
// passes the server filter.
func TestClientSubscribeCacheRecovery_ServerTagsFilterApplied(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()
	node.OnConnect(func(c *Client) {
		c.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{Options: SubscribeOptions{
				EnableRecovery:    true,
				EnablePositioning: true,
				RecoveryMode:      RecoveryModeCache,
				ServerTagsFilter:  &FilterNode{Key: "team", Cmp: "eq", Val: "eng"},
			}}, nil)
		})
	})

	const ch = "cache_recovery_server_filter"
	// The newest publication (offset 2) is team=sales and must be excluded; cache
	// recovery must fall back to the newest team=eng publication (offset 1).
	_, err := node.Publish(ch, []byte(`{"n":1}`), WithHistory(10, time.Minute), WithTags(map[string]string{"team": "eng"}))
	require.NoError(t, err)
	_, err = node.Publish(ch, []byte(`{"n":2}`), WithHistory(10, time.Minute), WithTags(map[string]string{"team": "sales"}))
	require.NoError(t, err)

	client := newTestClientV2(t, node, "u")
	connectClientV2(t, client)

	rw := testReplyWriterWrapper()
	require.NoError(t, client.handleSubscribe(
		&protocol.SubscribeRequest{Channel: ch, Recover: true},
		&protocol.Command{Id: 1}, time.Now(), rw.rw))
	require.NotEmpty(t, rw.replies)
	res := rw.replies[0].Subscribe
	require.NotNil(t, res)

	for _, p := range res.Publications {
		require.Equal(t, "eng", p.Tags["team"],
			"cache-recovered a publication that the server tags filter should have excluded")
	}
}

// ===========================================================================
// client_runtime_stability_test.go
// ===========================================================================

// pingTrackingTransport records the gap between consecutive server pings so a
// test can assert ping punctuality — the property that decides whether real SDKs
// keep the connection (they disconnect when a ping is overdue).
type pingTrackingTransport struct {
	countingTransport
	lastPing atomic.Int64
	maxGap   atomic.Int64
	pings    atomic.Int64
}

func (t *pingTrackingTransport) record() {
	now := time.Now().UnixNano()
	if prev := t.lastPing.Swap(now); prev != 0 {
		if gap := now - prev; gap > t.maxGap.Load() {
			t.maxGap.Store(gap)
		}
	}
	t.pings.Add(1)
}

func (t *pingTrackingTransport) Write(data []byte) error {
	t.record()
	return t.countingTransport.Write(data)
}

func (t *pingTrackingTransport) WriteMany(data ...[]byte) error {
	t.record()
	return t.countingTransport.WriteMany(data...)
}

// steadyGoroutines approximates the RESIDENT goroutine count. A presence tick
// spawns transient workers (up to clientPresenceUpdateConcurrency each, plus one
// per tick under a TimerScheduler), so a single NumGoroutine sample fluctuates
// with however many ticks happen to be in flight — on a fast machine that alone
// exceeds a tight threshold and makes a leak check flaky. Taking the minimum
// across samples filters the transients out while still growing if goroutines
// actually leak.
//
// The sampling must be DENSE to work: a sample only sees the resident floor if
// it lands in a moment where no tick is in flight anywhere, and with a hundred
// connections ticking those windows are short. Sampling every 10ms (with a
// runtime.GC per sample, which NumGoroutine does not need and which itself takes
// longer than such a window) missed them often enough that two calls made under
// an identical, unchanging connection population disagreed by up to 27
// goroutines — enough to trip the leak check below on its own. At 1ms and no GC
// the same measurement repeats to within a few goroutines.
func steadyGoroutines() int {
	minN := math.MaxInt
	for i := 0; i < 250; i++ {
		if n := runtime.NumGoroutine(); n < minN {
			minN = n
		}
		time.Sleep(time.Millisecond)
	}
	return minN
}

type stabilityOpts struct {
	useWheel    bool
	numConns    int
	numChannels int
	rtt         time.Duration
	duration    time.Duration
	concurrency int
}

// runRuntimeStability holds a fixed population of connections open with presence
// AND positioning enabled against a slow PresenceManager, then asserts the
// runtime stayed stable: nothing disconnected, pings stayed punctual, presence
// kept being refreshed, position checks kept running, and goroutines did not grow.
func runRuntimeStability(t *testing.T, o stabilityOpts) {
	pm := newTrackingPresenceManager(o.rtt)
	cfg := Config{
		LogLevel:                        LogLevelError,
		LogHandler:                      func(entry LogEntry) {},
		ClientChannelLimit:              1000,
		ClientPresenceUpdateInterval:    100 * time.Millisecond,
		ClientChannelPositionCheckDelay: 100 * time.Millisecond,
		ClientChannelPositionMaxTimeLag: time.Hour,
		clientPresenceUpdateConcurrency: o.concurrency,
		clientPositionCheckConcurrency:  o.concurrency,
	}
	var wheel *testSharedTimerScheduler
	if o.useWheel {
		wheel = newTestSharedTimerScheduler(4)
		cfg.ClientTimerScheduler = wheel
	}
	node, err := New(cfg)
	require.NoError(t, err)
	node.SetPresenceManager(pm)
	cb := &countingBroker{Broker: node.broker}
	node.SetBroker(cb)
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{Options: SubscribeOptions{
				EmitPresence:      true,
				EnablePositioning: true,
			}}, nil)
		})
	})
	require.NoError(t, node.Run())
	defer func() {
		_ = node.Shutdown(context.Background())
		if wheel != nil {
			wheel.Stop()
		}
	}()

	const pingInterval = 200 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	transports := make([]*pingTrackingTransport, 0, o.numConns)
	for i := 0; i < o.numConns; i++ {
		tr := &pingTrackingTransport{}
		tr.pingInterval = pingInterval
		tr.pongTimeout = -1 // no pongs from this transport; see timerbench_test.go
		transports = append(transports, tr)
		client := newTestClientCustomTransport(t, ctx, node, tr, "u"+strconv.Itoa(i))
		rw := testReplyWriterWrapper()
		require.NoError(t, client.connectCmd(&protocol.ConnectRequest{}, &protocol.Command{}, time.Now(), rw.rw))
		client.triggerConnect()
		client.scheduleOnConnectTimers()
		for j := 0; j < o.numChannels; j++ {
			srw := testReplyWriterWrapper()
			require.NoError(t, client.handleSubscribe(&protocol.SubscribeRequest{
				Channel: "ch" + strconv.Itoa(i) + "_" + strconv.Itoa(j),
			}, &protocol.Command{Id: 1}, time.Now(), srw.rw))
		}
	}
	require.Equal(t, o.numConns, node.hub.NumClients())

	time.Sleep(500 * time.Millisecond) // settle
	goroutinesBefore := steadyGoroutines()
	addsBefore := pm.adds.Load()
	historyBefore := cb.historyCalls.Load()
	for _, tr := range transports {
		tr.maxGap.Store(0)
	}

	time.Sleep(o.duration)

	// 1. Nothing dropped.
	require.Equal(t, o.numConns, node.hub.NumClients(),
		"connections were dropped during steady-state run")

	// 2. Pings stayed punctual. This is what real SDKs disconnect on.
	var worstGap time.Duration
	for _, tr := range transports {
		if g := time.Duration(tr.maxGap.Load()); g > worstGap {
			worstGap = g
		}
	}
	require.NotZero(t, worstGap, "no pings observed")
	require.Less(t, worstGap, 2*pingInterval,
		"a ping was delayed beyond 2x the interval — presence/position work is blocking the timer")

	// 3. Presence kept being refreshed and position checks kept running.
	require.Greater(t, pm.adds.Load(), addsBefore, "presence stopped being refreshed")
	require.Greater(t, cb.historyCalls.Load(), historyBefore, "position checks stopped running")

	// 4. Presence is complete: every client x channel is present.
	require.Equal(t, o.numConns*o.numChannels, pm.liveEntries(),
		"presence entries missing for live connections")

	// 5. No goroutine leak. A leaked tick goroutine would accumulate once per
	// tick — numConns * duration/ClientPresenceUpdateInterval of them, i.e. over
	// a thousand here — so resident growth is what matters, not the transient
	// workers in flight at any instant. The allowance is numConns rather than a
	// tight bound because steadyGoroutines still carries a few goroutines of
	// slack; it is two orders of magnitude below what any per-tick leak produces.
	goroutinesAfter := steadyGoroutines()
	require.Less(t, goroutinesAfter, goroutinesBefore+o.numConns,
		"resident goroutine count grew from %d to %d during steady state", goroutinesBefore, goroutinesAfter)

	t.Logf("wheel=%v conns=%d ch=%d rtt=%v: worst ping gap %v (interval %v), presence adds %d, position checks %d, goroutines %d->%d",
		o.useWheel, o.numConns, o.numChannels, o.rtt, worstGap, pingInterval,
		pm.adds.Load()-addsBefore, cb.historyCalls.Load()-historyBefore,
		goroutinesBefore, goroutinesAfter)
}

func TestRuntimeStability_Runtime(t *testing.T) {
	runRuntimeStability(t, stabilityOpts{
		useWheel: false, numConns: 200, numChannels: 4,
		rtt: time.Millisecond, duration: 3 * time.Second,
	})
}

func TestRuntimeStability_Scheduler(t *testing.T) {
	runRuntimeStability(t, stabilityOpts{
		useWheel: true, numConns: 200, numChannels: 4,
		rtt: time.Millisecond, duration: 3 * time.Second,
	})
}

// Slow PresenceManager: a tick takes far longer than the presence interval, so
// ticks overlap and the in-flight guard is active throughout. Pings must still
// be punctual and connections must survive — this is the core of issue #557.
func TestRuntimeStability_SlowPresenceManager(t *testing.T) {
	runRuntimeStability(t, stabilityOpts{
		useWheel: false, numConns: 50, numChannels: 8,
		rtt: 10 * time.Millisecond, duration: 3 * time.Second,
	})
}

func TestRuntimeStability_SlowPresenceManager_Scheduler(t *testing.T) {
	runRuntimeStability(t, stabilityOpts{
		useWheel: true, numConns: 50, numChannels: 8,
		rtt: 10 * time.Millisecond, duration: 3 * time.Second,
	})
}

// Concurrent presence updates must keep every invariant: presence complete for
// all live connections, pings punctual, no goroutine growth.
func TestRuntimeStability_Concurrent(t *testing.T) {
	runRuntimeStability(t, stabilityOpts{
		useWheel: false, numConns: 50, numChannels: 8, concurrency: 8,
		rtt: 10 * time.Millisecond, duration: 3 * time.Second,
	})
}

func TestRuntimeStability_Concurrent_Scheduler(t *testing.T) {
	runRuntimeStability(t, stabilityOpts{
		useWheel: true, numConns: 50, numChannels: 8, concurrency: 8,
		rtt: 10 * time.Millisecond, duration: 3 * time.Second,
	})
}

// TestRuntimeStability_SlowRefreshHandler covers the expire timer op. An
// application's RefreshHandler is commonly backed by an HTTP call (Centrifugo's
// refresh proxy runs it inline unless client concurrency is configured), so it
// blocks the timer callback. Under a TimerScheduler that shares a goroutine
// between connections that would delay unrelated connections' pings.
//
// Token TTLs cluster in practice (everyone reconnects after a deploy), so many
// connections refresh in the same window — this test makes them all expire at
// once on purpose.
func TestRuntimeStability_SlowRefreshHandler(t *testing.T) {
	const numConns = 60
	const pingInterval = 200 * time.Millisecond
	const refreshDuration = 150 * time.Millisecond

	wheel := newTestSharedTimerScheduler(4)
	defer wheel.Stop()
	node, err := New(Config{
		LogLevel:             LogLevelError,
		LogHandler:           func(entry LogEntry) {},
		ClientTimerScheduler: wheel,
	})
	require.NoError(t, err)
	var refreshes atomic.Int64
	node.OnConnect(func(client *Client) {
		client.OnRefresh(func(e RefreshEvent, cb RefreshCallback) {
			// Simulates a refresh proxy HTTP call running inline.
			time.Sleep(refreshDuration)
			refreshes.Add(1)
			cb(RefreshReply{ExpireAt: time.Now().Unix() + 1}, nil)
		})
	})
	require.NoError(t, node.Run())
	defer func() { _ = node.Shutdown(context.Background()) }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	transports := make([]*pingTrackingTransport, 0, numConns)
	for i := 0; i < numConns; i++ {
		tr := &pingTrackingTransport{}
		tr.pingInterval = pingInterval
		tr.pongTimeout = -1
		transports = append(transports, tr)
		newCtx := SetCredentials(ctx, &Credentials{
			UserID: "u" + strconv.Itoa(i),
			// All connections expire at the same moment on purpose.
			ExpireAt: time.Now().Unix() + 1,
		})
		client, err := newClient(newCtx, node, tr)
		require.NoError(t, err)
		rw := testReplyWriterWrapper()
		require.NoError(t, client.connectCmd(&protocol.ConnectRequest{}, &protocol.Command{}, time.Now(), rw.rw))
		client.triggerConnect()
		client.scheduleOnConnectTimers()
	}

	time.Sleep(500 * time.Millisecond)
	for _, tr := range transports {
		tr.maxGap.Store(0)
	}
	time.Sleep(3 * time.Second)

	require.Positive(t, refreshes.Load(), "refresh handler never ran")
	require.Equal(t, numConns, node.hub.NumClients(), "connections dropped")

	var worstGap time.Duration
	for _, tr := range transports {
		if g := time.Duration(tr.maxGap.Load()); g > worstGap {
			worstGap = g
		}
	}
	t.Logf("%d conns all refreshing with a %v handler: worst ping gap %v (interval %v), %d refreshes",
		numConns, refreshDuration, worstGap, pingInterval, refreshes.Load())
	require.Less(t, worstGap, 2*pingInterval,
		"a slow RefreshHandler delayed pings — expire is blocking the shared timer goroutine")
}

// ===========================================================================
// client_shared_poll_unsub_race_test.go
// ===========================================================================

// TestSharedPollSubscribe_LostUnsubscribeLeak proves that a shared-poll subscribe
// whose async handler is still in flight can have a concurrent unsubscribe get
// lost, leaving the client subscribed to a channel it unsubscribed from.
//
// Unlike normal subscriptions, the shared-poll reservation (client_shared_poll.go)
// installs an empty ChannelContext with no subscribingCh, so unsubscribe's
// "wait for in-flight subscribe" path (which requires subscribingCh != nil) never
// runs. The unsubscribe removes the empty reservation and returns; the subscribe
// then finalizes and re-adds the channel (flags, keyed state, map presence) that
// nothing will clean up until disconnect.
func TestSharedPollSubscribe_LostUnsubscribeLeak(t *testing.T) {
	node := newTestNodeWithSharedPoll(t)

	var savedCb SubscribeCallback
	cbReady := make(chan struct{})
	node.OnConnect(func(c *Client) {
		c.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			savedCb = cb
			close(cbReady)
		})
		c.OnSubRefresh(func(e SubRefreshEvent, cb SubRefreshCallback) { cb(SubRefreshReply{}, nil) })
	})

	client := newTestClientV2(t, node, "u")
	connectClientV2(t, client)

	const ch = "sp:chan"

	// Dispatch shared-poll subscribe; the handler captures the callback and returns.
	rw := testReplyWriterWrapper()
	require.NoError(t, client.handleSubscribe(
		&protocol.SubscribeRequest{Channel: ch, Type: int32(SubscriptionTypeSharedPoll)},
		&protocol.Command{Id: 1}, time.Now(), rw.rw))
	<-cbReady

	// Unsubscribe while the subscribe is still in flight. With the fix this blocks
	// on the reservation's subscribingCh until the subscribe finalizes, so run it
	// concurrently and release the subscribe from this goroutine.
	unsubDone := make(chan struct{})
	go func() {
		defer close(unsubDone)
		rwU := testReplyWriterWrapper()
		_ = client.handleUnsubscribe(
			&protocol.UnsubscribeRequest{Channel: ch}, &protocol.Command{Id: 2}, time.Now(), rwU.rw)
	}()

	// Give the unsubscribe a moment to reach its wait on subscribingCh, then let
	// the subscribe finalize. The woken unsubscribe must tear the subscription down.
	time.Sleep(50 * time.Millisecond)
	savedCb(SubscribeReply{
		Options:           SubscribeOptions{ExpireAt: time.Now().Unix() + 3600},
		ClientSideRefresh: true,
	}, nil)

	select {
	case <-unsubDone:
	case <-time.After(6 * time.Second):
		t.Fatal("unsubscribe hung waiting on the shared-poll subscribe reservation")
	}

	// The client unsubscribed — it must not remain subscribed.
	client.mu.RLock()
	cc, ok := client.channels[ch]
	subscribed := ok && channelHasFlag(cc.flags, flagSubscribed)
	client.mu.RUnlock()
	require.False(t, subscribed,
		"shared-poll subscription leaked: still subscribed after unsubscribe raced the in-flight subscribe")

	_ = client.close(DisconnectForceNoReconnect)
}

// ===========================================================================
// client_state_consistency_metric_test.go
// ===========================================================================

// TestClientState_MetricsSettleToZeroAfterChurn is an end-to-end invariant check:
// after concurrent full-lifecycle churn (connect with server-side subs, client
// subscribe/unsubscribe, client close, and Node.Disconnect), the hub client and
// subscription counts AND the connections_inflight / subscriptions_inflight
// gauges must all return to exactly zero. Any drift in the inflight gauges, or
// any divergence between c.channels and the hub, shows up as a non-zero residual.
func TestClientState_MetricsSettleToZeroAfterChurn(t *testing.T) {
	registry := prometheus.NewRegistry()
	node, err := New(Config{
		LogLevel:   LogLevelError,
		LogHandler: func(LogEntry) {},
		Metrics:    MetricsConfig{RegistererGatherer: registry},
	})
	require.NoError(t, err)
	serverChannels := []string{"ss0", "ss1"}
	node.OnConnecting(func(ctx context.Context, e ConnectEvent) (ConnectReply, error) {
		subs := make(map[string]SubscribeOptions, len(serverChannels))
		for _, ch := range serverChannels {
			subs[ch] = SubscribeOptions{}
		}
		return ConnectReply{Subscriptions: subs}, nil
	})
	node.OnConnect(func(c *Client) {
		c.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) { cb(SubscribeReply{}, nil) })
	})
	require.NoError(t, node.Run())
	defer func() { _ = node.Shutdown(context.Background()) }()

	gaugeSum := func(needle string) float64 {
		mfs, gErr := registry.Gather()
		require.NoError(t, gErr)
		var sum float64
		for _, mf := range mfs {
			if strings.Contains(mf.GetName(), needle) {
				for _, m := range mf.GetMetric() {
					sum += m.GetGauge().GetValue()
				}
			}
		}
		return sum
	}

	const user = "u"
	clientChannels := []string{"cc0", "cc1", "cc2"}

	var wg sync.WaitGroup
	// A driver that keeps disconnecting the whole user via the server API,
	// racing the connect/close churn.
	wg.Add(1)
	go func() {
		defer wg.Done()
		deadline := time.After(8 * time.Second)
		for {
			select {
			case <-deadline:
				return
			default:
				_ = node.Disconnect(user)
				time.Sleep(time.Millisecond)
			}
		}
	}()

	for w := 0; w < 16; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			deadline := time.After(8 * time.Second)
			for {
				select {
				case <-deadline:
					return
				default:
				}
				ctx, cancel := context.WithCancel(context.Background())
				tr := &countingTransport{pingInterval: time.Second, pongTimeout: -1}
				client, cErr := newClient(SetCredentials(ctx, &Credentials{UserID: user}), node, tr)
				if cErr != nil {
					cancel()
					return
				}
				rw := testReplyWriterWrapper()
				if err := client.connectCmd(&protocol.ConnectRequest{}, &protocol.Command{}, time.Now(), rw.rw); err != nil {
					_ = client.close(DisconnectForceNoReconnect)
					cancel()
					continue
				}
				client.triggerConnect()
				client.scheduleOnConnectTimers()
				ch := clientChannels[w%len(clientChannels)]
				srw := testReplyWriterWrapper()
				_ = client.handleSubscribe(&protocol.SubscribeRequest{Channel: ch}, &protocol.Command{Id: 1}, time.Now(), srw.rw)
				if w%2 == 0 {
					urw := testReplyWriterWrapper()
					_ = client.handleUnsubscribe(&protocol.UnsubscribeRequest{Channel: ch}, &protocol.Command{Id: 2}, time.Now(), urw.rw)
				}
				_ = client.close(DisconnectForceNoReconnect)
				cancel()
			}
		}(w)
	}

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("timeout during churn")
	}

	require.Eventually(t, func() bool {
		return node.hub.NumClients() == 0 && node.hub.NumSubscriptions() == 0 &&
			gaugeSum("connections_inflight") == 0 && gaugeSum("subscriptions_inflight") == 0
	}, 10*time.Second, 20*time.Millisecond,
		"state/metrics did not settle: NumClients=%d NumSubscriptions=%d connInflight=%.0f subInflight=%.0f",
		node.hub.NumClients(), node.hub.NumSubscriptions(),
		gaugeSum("connections_inflight"), gaugeSum("subscriptions_inflight"))
}

// ===========================================================================
// client_sub_exclusivity_test.go
// ===========================================================================

// TestValidateSubscribeRequest_RejectsNormalWhileMapLoading proves the
// reservation-exclusivity gap that commitSubscription relies on: a map subscribe
// loading in c.mapSubscribing must block a concurrent normal subscribe for the
// same channel. Otherwise both reserve (mapSubscribing and c.channels), and when
// the map sub goes live commitSubscription overwrites c.channels — discarding the
// normal reservation and orphaning its subscribingCh (a waiting unsubscribe then
// hangs on the 5s timeout).
func TestValidateSubscribeRequest_RejectsNormalWhileMapLoading(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()
	client := newTestClientV2(t, node, "u")
	connectClientV2(t, client)
	const ch = "x"

	// Simulate a map subscribe in progress for ch (reserved, not yet live).
	client.mu.Lock()
	if client.mapSubscribing == nil {
		client.mapSubscribing = map[string]*mapSubscribeState{}
	}
	client.mapSubscribing[ch] = &mapSubscribeState{subscribingCh: make(chan struct{}), subGen: 1}
	client.mu.Unlock()

	// A concurrent normal subscribe for the same channel must be rejected.
	gotGen, replyErr, disconnect := client.validateSubscribeRequest(&protocol.SubscribeRequest{Channel: ch})
	require.Nil(t, disconnect)
	require.Equal(t, ErrorAlreadySubscribed, replyErr,
		"normal subscribe must be rejected while a map subscribe is loading the same channel")
	require.Zero(t, gotGen, "a rejected subscribe must not mint a reservation generation")

	// And it must not have reserved c.channels behind the map subscribe's back.
	client.mu.RLock()
	_, reserved := client.channels[ch]
	client.mu.RUnlock()
	require.False(t, reserved,
		"normal subscribe must not reserve c.channels while a map subscribe is loading")
}

// ===========================================================================
// client_subrefresh_gen_test.go
// ===========================================================================

// TestSubRefresh_StaleCallbackDoesNotClobberResubscribe guards the sub-refresh
// callback's write-back to c.channels. The SubRefreshHandler is asynchronous
// (commonly an HTTP proxy round trip). If, while it is in flight, the client
// unsubscribes and resubscribes the same channel, the fresh subscription gets a
// new subGen. A stale sub-refresh — validated against the OLD subscription —
// must not overwrite the fresh subscription's expireAt/info. handleSubRefresh
// gen-matches its write-back (mirrors checkPosition); without it the stale
// expireAt lands on the resubscription.
func TestSubRefresh_StaleCallbackDoesNotClobberResubscribe(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	nowUnix := time.Now().Unix()
	const freshTTL = 3600
	const staleTTL = 7200
	freshExpireAt := nowUnix + freshTTL
	staleExpireAt := nowUnix + staleTTL

	var savedCb SubRefreshCallback
	cbReady := make(chan struct{})

	node.OnConnect(func(c *Client) {
		c.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				ClientSideRefresh: true,
				Options:           SubscribeOptions{ExpireAt: freshExpireAt},
			}, nil)
		})
		c.OnSubRefresh(func(e SubRefreshEvent, cb SubRefreshCallback) {
			// Capture the callback; fire it later, after a resubscribe.
			savedCb = cb
			close(cbReady)
		})
	})

	const ch = "sr"
	client := newTestClientV2(t, node, "u")
	connectClientV2(t, client)

	subscribe := func() {
		rw := testReplyWriterWrapper()
		require.NoError(t, client.handleSubscribe(&protocol.SubscribeRequest{Channel: ch}, &protocol.Command{Id: 1}, time.Now(), rw.rw))
	}
	channelExpireAt := func() (int64, bool) {
		client.mu.RLock()
		defer client.mu.RUnlock()
		cc, ok := client.channels[ch]
		return cc.expireAt, ok && channelHasFlag(cc.flags, flagSubscribed)
	}

	// First subscription (subGen A) with a client-side-refresh token.
	subscribe()
	exp, ok := channelExpireAt()
	require.True(t, ok)
	require.Equal(t, freshExpireAt, exp)

	// Sub-refresh dispatched; handler captures the callback and returns.
	rw := testReplyWriterWrapper()
	require.NoError(t, client.handleSubRefresh(&protocol.SubRefreshRequest{Channel: ch, Token: "tok"}, &protocol.Command{Id: 2}, time.Now(), rw.rw))
	<-cbReady

	// Unsubscribe + resubscribe the same channel in the refresh window (subGen B).
	rwU := testReplyWriterWrapper()
	require.NoError(t, client.handleUnsubscribe(&protocol.UnsubscribeRequest{Channel: ch}, &protocol.Command{Id: 3}, time.Now(), rwU.rw))
	subscribe()
	exp, ok = channelExpireAt()
	require.True(t, ok)
	require.Equal(t, freshExpireAt, exp)

	// The stale sub-refresh (for subGen A) fires now. It must NOT touch subGen B.
	savedCb(SubRefreshReply{ExpireAt: staleExpireAt}, nil)

	exp, ok = channelExpireAt()
	require.True(t, ok)
	require.Equal(t, freshExpireAt, exp,
		"stale sub-refresh clobbered the resubscription's expireAt (got %d, want %d)", exp, freshExpireAt)

	_ = client.close(DisconnectForceNoReconnect)
}

// TestCheckSubscriptionExpiration_StaleCallbackDoesNotClobberResubscribe is the
// server-side (periodic tick) analog of the test above: the async
// SubRefreshHandler for a server-side subscription must not write its expireAt
// back onto a subscription that was resubscribed (fresh subGen) in the window.
func TestCheckSubscriptionExpiration_StaleCallbackDoesNotClobberResubscribe(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	nowUnix := time.Now().Unix()
	freshExpireAt := nowUnix + 3600
	staleExpireAt := nowUnix + 7200

	var savedCb SubRefreshCallback
	cbReady := make(chan struct{})

	node.OnConnect(func(c *Client) {
		// Server-side refresh (ClientSideRefresh stays false).
		c.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{Options: SubscribeOptions{ExpireAt: freshExpireAt}}, nil)
		})
		c.OnSubRefresh(func(e SubRefreshEvent, cb SubRefreshCallback) {
			savedCb = cb
			close(cbReady)
		})
	})

	const ch = "sr2"
	client := newTestClientV2(t, node, "u")
	connectClientV2(t, client)

	subscribe := func() {
		rw := testReplyWriterWrapper()
		require.NoError(t, client.handleSubscribe(&protocol.SubscribeRequest{Channel: ch}, &protocol.Command{Id: 1}, time.Now(), rw.rw))
	}
	channelExpireAt := func() (int64, bool) {
		client.mu.RLock()
		defer client.mu.RUnlock()
		cc, ok := client.channels[ch]
		return cc.expireAt, ok && channelHasFlag(cc.flags, flagSubscribed)
	}

	subscribe()

	// Snapshot the live subscription (subGen A) but with an already-expired
	// expireAt, as the periodic tick would present when the token is due.
	client.mu.RLock()
	snap := client.channels[ch]
	client.mu.RUnlock()
	snap.expireAt = nowUnix - 100

	go client.checkSubscriptionExpiration(ch, snap, 0, func(bool) {})
	<-cbReady

	// Unsubscribe + resubscribe in the refresh window (subGen B).
	rwU := testReplyWriterWrapper()
	require.NoError(t, client.handleUnsubscribe(&protocol.UnsubscribeRequest{Channel: ch}, &protocol.Command{Id: 3}, time.Now(), rwU.rw))
	subscribe()
	exp, ok := channelExpireAt()
	require.True(t, ok)
	require.Equal(t, freshExpireAt, exp)

	// Stale refresh (for subGen A) fires — must not touch subGen B.
	savedCb(SubRefreshReply{ExpireAt: staleExpireAt}, nil)

	exp, ok = channelExpireAt()
	require.True(t, ok)
	require.Equal(t, freshExpireAt, exp,
		"stale server-side sub-refresh clobbered the resubscription's expireAt (got %d, want %d)", exp, freshExpireAt)

	_ = client.close(DisconnectForceNoReconnect)
}

// ===========================================================================
// client_subscribe_race_test.go
// ===========================================================================

// hubHasSub reports whether a specific client is registered in the hub for a
// channel (the authoritative broadcast-routing state).
func hubHasSub(node *Node, ch, clientID string) bool {
	s := node.hub.subShards[index(ch, numHubShards)]
	s.mu.RLock()
	defer s.mu.RUnlock()
	subs, ok := s.subs[ch]
	if !ok {
		return false
	}
	_, ok = subs[clientID]
	return ok
}

func clientHasChannel(c *Client, ch string) (present, subscribed bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	ctx, ok := c.channels[ch]
	return ok, ok && channelHasFlag(ctx.flags, flagSubscribed)
}

// TestSubscribeUnsubscribe_StateConsistency hammers a single client with
// concurrent subscribe and unsubscribe commands on the same channel — the shape
// the emulation transport permits, since it dispatches each command on its own
// goroutine. After each round quiesces, the per-client subscription map and the
// hub's routing table must AGREE: a channel is either in both or in neither.
//
// A disagreement is an improper subscription state: a channel in c.channels but
// not the hub gets no publications; a channel in the hub but not c.channels is a
// leaked routing entry (and skews NumSubscribers).
//
// Run under -race; a subscribingCh double-close would also surface here as a
// panic.
func TestSubscribeUnsubscribe_StateConsistency(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{}, nil) // synchronous — subscribe completes inline
		})
	})

	client := newTestClientV2(t, node, "u")
	connectClientV2(t, client)
	const ch = "x"

	const rounds = 20000
	for r := 0; r < rounds; r++ {
		var wg sync.WaitGroup
		// A few subscribers and unsubscribers racing on the same channel.
		for i := 0; i < 3; i++ {
			wg.Add(2)
			go func() {
				defer wg.Done()
				rw := testReplyWriterWrapper()
				_ = client.handleSubscribe(&protocol.SubscribeRequest{Channel: ch}, &protocol.Command{Id: 1}, time.Now(), rw.rw)
			}()
			go func() {
				defer wg.Done()
				rw := testReplyWriterWrapper()
				_ = client.handleUnsubscribe(&protocol.UnsubscribeRequest{Channel: ch}, &protocol.Command{Id: 1}, time.Now(), rw.rw)
			}()
		}
		wg.Wait()

		// Quiesced: c.channels and the hub must agree.
		present, subscribed := clientHasChannel(client, ch)
		inHub := hubHasSub(node, ch, client.ID())
		require.Equalf(t, present, inHub,
			"round %d: c.channels has %s=%v but hub has it=%v — inconsistent subscription state",
			r, ch, present, inHub)
		if present {
			require.Truef(t, subscribed,
				"round %d: channel %s left in c.channels but not flagSubscribed (stuck subscribing)", r, ch)
		}
	}
}

// TestSubscribeUnsubscribe_CloseRace races connect/subscribe/unsubscribe/close
// so that close() iterates c.channels while subscribes and unsubscribes mutate
// it. After the client is closed it must be fully gone from the hub — no channel
// may still route to it, or publications would be delivered to a dead client and
// NumSubscribers would be wrong.
func TestSubscribeUnsubscribe_CloseRace(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{}, nil)
		})
	})

	channels := []string{"a", "b", "c"}
	const rounds = 2000
	for r := 0; r < rounds; r++ {
		client := newTestClientV2(t, node, "u")
		connectClientV2(t, client)

		var wg sync.WaitGroup
		for _, ch := range channels {
			wg.Add(2)
			go func(ch string) {
				defer wg.Done()
				rw := testReplyWriterWrapper()
				_ = client.handleSubscribe(&protocol.SubscribeRequest{Channel: ch}, &protocol.Command{Id: 1}, time.Now(), rw.rw)
			}(ch)
			go func(ch string) {
				defer wg.Done()
				rw := testReplyWriterWrapper()
				_ = client.handleUnsubscribe(&protocol.UnsubscribeRequest{Channel: ch}, &protocol.Command{Id: 1}, time.Now(), rw.rw)
			}(ch)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = client.close(DisconnectForceNoReconnect)
		}()
		wg.Wait()
		_ = client.close(DisconnectForceNoReconnect) // ensure closed

		// The closed client must not linger in the hub for any channel.
		for _, ch := range channels {
			require.Falsef(t, hubHasSub(node, ch, client.ID()),
				"round %d: closed client still routed for channel %s — leaked hub subscription", r, ch)
		}
	}
}

// TestSubscribeUnsubscribe_AsyncCallback covers the realistic proxy-auth shape:
// the subscribe callback runs on its own goroutine (async), so there is a real
// window between validateSubscribeRequest reserving c.channels and subscribeCmd
// registering in the hub. A concurrent unsubscribe must bridge that window via
// subscribingCh without leaving c.channels and the hub inconsistent.
func TestSubscribeUnsubscribe_AsyncCallback(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	var pending sync.WaitGroup // tracks in-flight async subscribe callbacks
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			pending.Add(1)
			go func() {
				defer pending.Done()
				time.Sleep(50 * time.Microsecond) // widen the reserve->hub-add window
				cb(SubscribeReply{}, nil)
			}()
		})
	})

	client := newTestClientV2(t, node, "u")
	connectClientV2(t, client)
	const ch = "x"

	const rounds = 2000
	for r := 0; r < rounds; r++ {
		var wg sync.WaitGroup
		for i := 0; i < 2; i++ {
			wg.Add(2)
			go func() {
				defer wg.Done()
				rw := testReplyWriterWrapper()
				_ = client.handleSubscribe(&protocol.SubscribeRequest{Channel: ch}, &protocol.Command{Id: 1}, time.Now(), rw.rw)
			}()
			go func() {
				defer wg.Done()
				rw := testReplyWriterWrapper()
				_ = client.handleUnsubscribe(&protocol.UnsubscribeRequest{Channel: ch}, &protocol.Command{Id: 1}, time.Now(), rw.rw)
			}()
		}
		wg.Wait()
		pending.Wait() // let async subscribe callbacks finish before checking

		present, subscribed := clientHasChannel(client, ch)
		inHub := hubHasSub(node, ch, client.ID())
		require.Equalf(t, present, inHub,
			"round %d: c.channels has %s=%v but hub has it=%v — inconsistent after async subscribe race",
			r, ch, present, inHub)
		if present {
			require.Truef(t, subscribed, "round %d: %s stuck subscribing", r, ch)
		}
		// Reset to a clean slate for the next round.
		rw := testReplyWriterWrapper()
		_ = client.handleUnsubscribe(&protocol.UnsubscribeRequest{Channel: ch}, &protocol.Command{Id: 1}, time.Now(), rw.rw)
		pending.Wait()
	}
}

// ===========================================================================
// client_subscription_consistency_test.go
// ===========================================================================

// This file is the correctness spec for subscription-state consistency under
// concurrent subscribe / unsubscribe / close, and for the property that any fix
// must not violate (no lock-order deadlock against the broadcast path).
//
// Core invariant (checked only at quiescence): for every client and channel,
//
//	(channel in c.channels)  ==  (client in hub.subShards[channel])
//
// and, observably, a publication reaches the client iff it is subscribed.
//
// The helpers hubHasSub / clientHasChannel live in client_subscribe_race_test.go.

// quiesce waits until nothing is in flight for the channel by polling the two
// views until they are stable and equal (or the deadline passes). It exists so
// the invariant is checked at rest, not during a transient subscribe window
// (reserve-before-hub-add or hub-add-before-c.channels).
func subViewsStable(client *Client, node *Node, ch string) bool {
	p1, _ := clientHasChannel(client, ch)
	h1 := hubHasSub(node, ch, client.ID())
	return p1 == h1
}

// TestSubConsistency_ServerSubscribeVsUnsubscribe is the primary reproducer:
// server-side Client.Subscribe racing Client.Unsubscribe on the same channel.
// Both run on non-reader goroutines, so this is reachable on any transport
// (WebSocket included) — the app driving dynamic server-side subscriptions.
func TestSubConsistency_ServerSubscribeVsUnsubscribe(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()
	client := newTestClientV2(t, node, "u")
	connectClientV2(t, client)
	const ch = "x"

	const rounds = 15000
	for r := 0; r < rounds; r++ {
		var wg sync.WaitGroup
		for i := 0; i < 3; i++ {
			wg.Add(2)
			go func() { defer wg.Done(); _ = client.Subscribe(ch) }()
			go func() { defer wg.Done(); client.Unsubscribe(ch) }()
		}
		wg.Wait()
		require.Truef(t, subViewsStable(client, node, ch),
			"round %d: c.channels and hub disagree after concurrent server-side subscribe/unsubscribe", r)
		client.Unsubscribe(ch) // reset
	}
}

// TestSubConsistency_NoLeakAfterDisconnect: after the client is closed it must be
// gone from the hub's routing table for every channel, even if server-side
// subscribes/unsubscribes were racing the disconnect. A residual entry is a
// permanent leak (hub.remove clears the connection registry, not subShards).
func TestSubConsistency_NoLeakAfterDisconnect(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()
	channels := []string{"a", "b", "c"}

	const rounds = 8000
	for r := 0; r < rounds; r++ {
		client := newTestClientV2(t, node, "u"+strconv.Itoa(r))
		connectClientV2(t, client)
		var wg sync.WaitGroup
		for _, ch := range channels {
			wg.Add(2)
			go func(ch string) { defer wg.Done(); _ = client.Subscribe(ch) }(ch)
			go func(ch string) { defer wg.Done(); client.Unsubscribe(ch) }(ch)
		}
		wg.Add(1)
		go func() { defer wg.Done(); _ = client.close(DisconnectForceNoReconnect) }()
		wg.Wait()
		_ = client.close(DisconnectForceNoReconnect)

		for _, ch := range channels {
			require.Falsef(t, hubHasSub(node, ch, client.ID()),
				"round %d: closed client still routed for %s — leaked subscription entry", r, ch)
		}
	}
}

// TestSubConsistency_NoDeadlockUnderBroadcast is the guard the fix must survive.
// The broadcast path holds subShard.mu and then takes c.mu (writePublication).
// Any fix that instead takes c.mu and then subShard.mu inverts the order and
// deadlocks. Here a background publisher broadcasts continuously while the
// foreground churns server-side subscribe/unsubscribe. A deadlocking fix makes
// this hang; the test timeout turns that into a failure.
//
// It also checks consistency holds under real broadcast load, not just in
// isolation.
func TestSubConsistency_NoDeadlockUnderBroadcast(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()
	client := newTestClientV2(t, node, "u")
	connectClientV2(t, client)
	const ch = "b"

	stop := make(chan struct{})
	var pubWG sync.WaitGroup
	// Several concurrent broadcasters (subShard.mu -> c.mu).
	for i := 0; i < 4; i++ {
		pubWG.Add(1)
		go func() {
			defer pubWG.Done()
			for {
				select {
				case <-stop:
					return
				default:
					_, _ = node.Publish(ch, []byte(`{"m":1}`))
				}
			}
		}()
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		for r := 0; r < 4000; r++ {
			var wg sync.WaitGroup
			for i := 0; i < 3; i++ {
				wg.Add(2)
				go func() { defer wg.Done(); _ = client.Subscribe(ch) }()
				go func() { defer wg.Done(); client.Unsubscribe(ch) }()
			}
			wg.Wait()
			require.Truef(t, subViewsStable(client, node, ch), "round %d: inconsistent under broadcast", r)
			client.Unsubscribe(ch)
		}
	}()

	select {
	case <-done:
	case <-time.After(60 * time.Second):
		close(stop)
		pubWG.Wait()
		t.Fatal("timeout — likely a lock-order deadlock between subscribe/unsubscribe and broadcast")
	}
	close(stop)
	pubWG.Wait()
}

// TestSubConsistency_DeliveryMatchesState checks the observable consequence, not
// just internal maps: after a subscribe/unsubscribe race settles, a publication
// must reach the client iff the client considers itself subscribed. A c.channels/
// hub disagreement shows up here as a missed message (thinks subscribed, gets
// nothing) or a spurious one (thinks unsubscribed, still gets it).
func TestSubConsistency_DeliveryMatchesState(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	const ch = "d"
	const rounds = 600
	for r := 0; r < rounds; r++ {
		sink := make(chan []byte, 256)
		transport := newTestTransport(func() {})
		transport.setProtocolType(ProtocolTypeJSON)
		transport.setProtocolVersion(ProtocolVersion2)
		transport.setSink(sink)
		ctx, cancel := context.WithCancel(context.Background())
		client := newTestClientCustomTransport(t, ctx, node, transport, "u")
		connectClientV2(t, client)

		var wg sync.WaitGroup
		for i := 0; i < 3; i++ {
			wg.Add(2)
			go func() { defer wg.Done(); _ = client.Subscribe(ch) }()
			go func() { defer wg.Done(); client.Unsubscribe(ch) }()
		}
		wg.Wait()

		present, _ := clientHasChannel(client, ch)
		drainTransport(sink, 15*time.Millisecond)

		marker := "marker-" + strconv.Itoa(r)
		_, _ = node.Publish(ch, []byte(`{"marker":"`+marker+`"}`))
		delivered := waitForPayload(t, sink, marker, 120*time.Millisecond)

		require.Equalf(t, present, delivered,
			"round %d: client subscribed=%v but publication delivered=%v — routing does not match client state",
			r, present, delivered)

		_ = client.close(DisconnectForceNoReconnect)
		cancel()
	}
}

// A counter used to keep the linter happy about atomic import if trimmed.

// ===========================================================================
// handler_websocket_ping_race_test.go
// ===========================================================================

// TestWebsocketTransport_PingWriteDeadlineRace guards against a data race between
// the writer goroutine and the frame-ping timer goroutine on the websocket write
// deadline. writeData arms and clears conn.SetWriteDeadline (the gorilla
// writeDeadline field, not safe for concurrent use); the ping goroutine must not
// touch that same field. Run under -race.
func TestWebsocketTransport_PingWriteDeadlineRace(t *testing.T) {
	var upgrader websocket.Upgrader
	connCh := make(chan *websocket.Conn, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, _, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		connCh <- c
	}))
	defer srv.Close()

	dialer := &websocket.Dialer{}
	clientConn, resp, _, err := dialer.Dial("ws"+strings.TrimPrefix(srv.URL, "http")+"/", nil)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	defer func() { _ = clientConn.Close() }()
	// Drain client reads so server-side control/data writes don't block.
	go func() {
		for {
			if _, _, err := clientConn.ReadMessage(); err != nil {
				return
			}
		}
	}()

	serverConn := <-connCh
	tr := newWebsocketTransport(serverConn, websocketTransportOptions{
		protoType:    ProtocolTypeJSON,
		writeTimeout: time.Second,
		protoMajor:   1,
		pingPong:     PingPongConfig{PingInterval: time.Hour, PongTimeout: time.Hour},
	}, make(chan struct{}), true)
	defer func() { _ = tr.Close(DisconnectConnectionClosed) }()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			_ = tr.Write([]byte(`{"n":1}`))
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			tr.ping()
		}
	}()
	wg.Wait()
}

// ===========================================================================
// hub_counters_concurrency_test.go
// ===========================================================================

// TestHubCounters_ConcurrentAddRemove hammers the hub's add/remove paths from
// many goroutines and checks that the O(1) counters (NumClients, NumSubscriptions)
// stay exactly consistent with a full walk of the underlying maps. A missed or
// double increment on any add/remove path would show up as a mismatch or a
// non-zero residual after everything is removed.
func TestHubCounters_ConcurrentAddRemove(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()
	h := node.hub

	walkSubs := func() int {
		total := 0
		for i := 0; i < numHubShards; i++ {
			s := h.subShards[i]
			s.mu.RLock()
			for _, subs := range s.subs {
				total += len(subs)
			}
			s.mu.RUnlock()
		}
		return total
	}
	walkClients := func() int {
		total := 0
		for i := 0; i < numHubShards; i++ {
			cs := h.connShards[i]
			cs.mu.RLock()
			for _, conns := range cs.users {
				total += len(conns)
			}
			cs.mu.RUnlock()
		}
		return total
	}

	const workers = 24
	const perWorker = 400
	const channels = 12

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			clients := make([]*Client, perWorker)
			for i := 0; i < perWorker; i++ {
				c := newTestClient(t, node, "u"+strconv.Itoa(w)+"_"+strconv.Itoa(i))
				clients[i] = c
				h.add(c)
				for ch := 0; ch < channels; ch++ {
					// Some channels shared across workers, some not — exercises both
					// the first-subscriber and additional-subscriber branches.
					chName := "c" + strconv.Itoa((w+ch)%channels)
					_, _, _ = h.addSub(chName, subInfo{client: c})
				}
			}
			// Tear it all back down.
			for i := 0; i < perWorker; i++ {
				c := clients[i]
				for ch := 0; ch < channels; ch++ {
					chName := "c" + strconv.Itoa((w+ch)%channels)
					_, _, _ = h.removeSub(chName, c, anySubGen)
				}
				h.remove(c)
			}
		}(w)
	}
	wg.Wait()

	// After a balanced add/remove, both counters and the maps must be back to zero.
	require.Equal(t, 0, h.NumClients(), "NumClients residual after balanced churn")
	require.Equal(t, 0, h.NumSubscriptions(), "NumSubscriptions residual after balanced churn")
	require.Equal(t, walkClients(), h.NumClients(), "NumClients disagrees with map walk")
	require.Equal(t, walkSubs(), h.NumSubscriptions(), "NumSubscriptions disagrees with map walk")
}

// ===========================================================================
// hub_inflight_metric_test.go
// ===========================================================================

// TestSubscriptionsInflight_NoDriftOnResubscribeOverwrite guards the
// subscriptions_inflight gauge against drift. It must track numSubs exactly:
// incremented only when a genuinely new client+channel subscription is added,
// decremented only when one is actually removed. A resubscribe whose addSub
// overwrites an existing entry (a fresh generation before the prior
// unsubscribe's removeSub ran) adds no new subscription, and the stale
// generation's removeSub is a no-op — so the gauge must not move for either.
func TestSubscriptionsInflight_NoDriftOnResubscribeOverwrite(t *testing.T) {
	registry := prometheus.NewRegistry()
	node, err := New(Config{
		LogLevel:   LogLevelError,
		LogHandler: func(LogEntry) {},
		Metrics:    MetricsConfig{RegistererGatherer: registry},
	})
	require.NoError(t, err)
	require.NoError(t, node.Run())
	defer func() { _ = node.Shutdown(context.Background()) }()

	client := newTestClientV2(t, node, "u")
	const ch = "inflight-ch"

	inflight := func() float64 {
		mfs, gErr := registry.Gather()
		require.NoError(t, gErr)
		var sum float64
		for _, mf := range mfs {
			if strings.Contains(mf.GetName(), "subscriptions_inflight") {
				for _, m := range mf.GetMetric() {
					sum += m.GetGauge().GetValue()
				}
			}
		}
		return sum
	}

	// Initial subscription (generation 5).
	_, err = node.addSubscription(ch, subInfo{client: client, subGen: 5})
	require.NoError(t, err)
	require.Equal(t, float64(1), inflight())

	// Resubscribe overwrite: a fresh generation for the SAME client+channel, as
	// happens when a resubscribe's addSub runs before the prior unsubscribe's
	// removeSub. No new subscription — inflight must stay 1.
	_, err = node.addSubscription(ch, subInfo{client: client, subGen: 7})
	require.NoError(t, err)
	require.Equal(t, float64(1), inflight(), "inflight drifted on resubscribe overwrite")

	// Stale unsubscribe (generation 5): gen mismatch, nothing removed — stays 1.
	require.NoError(t, node.removeSubscription(ch, client, 5))
	require.Equal(t, float64(1), inflight(), "stale unsubscribe wrongly changed inflight")

	// Real unsubscribe (generation 7): back to 0.
	require.NoError(t, node.removeSubscription(ch, client, 7))
	require.Equal(t, float64(0), inflight())
}

// TestConnectionsInflight_NoDriftOnDuplicateAddOrDoubleRemove guards the
// connections_inflight gauge against drift. It must track the hub clients map
// exactly: incremented only when hub.add registers a genuinely new connection,
// decremented only when hub.remove actually removes one. A duplicate add for an
// already-registered uid adds no new connection, and a double remove removes
// nothing — so the gauge must not move for either.
func TestConnectionsInflight_NoDriftOnDuplicateAddOrDoubleRemove(t *testing.T) {
	registry := prometheus.NewRegistry()
	node, err := New(Config{
		LogLevel:   LogLevelError,
		LogHandler: func(LogEntry) {},
		Metrics:    MetricsConfig{RegistererGatherer: registry},
	})
	require.NoError(t, err)
	require.NoError(t, node.Run())
	defer func() { _ = node.Shutdown(context.Background()) }()

	client := newTestClientV2(t, node, "u")

	inflight := func() float64 {
		mfs, gErr := registry.Gather()
		require.NoError(t, gErr)
		var sum float64
		for _, mf := range mfs {
			if strings.Contains(mf.GetName(), "connections_inflight") {
				for _, m := range mf.GetMetric() {
					sum += m.GetGauge().GetValue()
				}
			}
		}
		return sum
	}

	node.addClient(client)
	require.Equal(t, float64(1), inflight())

	// Duplicate add of the same uid — no new connection; must stay 1.
	node.addClient(client)
	require.Equal(t, float64(1), inflight(), "connections_inflight drifted on duplicate add")

	node.removeClient(client)
	require.Equal(t, float64(0), inflight())

	// Double remove — nothing to remove; must stay 0 (not go negative).
	node.removeClient(client)
	require.Equal(t, float64(0), inflight(), "connections_inflight drifted on double remove")
}

// ===========================================================================
// hub_numclients_test.go
// ===========================================================================

// TestConnShardNumClientsMatchesUserSets pins the equivalence NumClients relies
// on: len(clients) must equal the sum of the per-user connection sets. If add or
// remove ever stop maintaining both together, the O(1) count would silently
// drift from the real one.
func TestConnShardNumClientsMatchesUserSets(t *testing.T) {
	t.Parallel()
	sumUserSets := func(h *connShard) int {
		h.mu.RLock()
		defer h.mu.RUnlock()
		total := 0
		for _, conns := range h.users {
			total += len(conns)
		}
		return total
	}

	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()
	shard := newConnShard()

	var clients []*Client
	// Several users, several connections each — the case the old sum walked.
	for u := 0; u < 5; u++ {
		for i := 0; i < 4; i++ {
			c := newTestClient(t, node, "user"+strconv.Itoa(u))
			clients = append(clients, c)
			shard.add(c)
			require.Equal(t, sumUserSets(shard), shard.NumClients())
		}
	}
	require.Equal(t, 20, shard.NumClients())

	for _, c := range clients {
		shard.remove(c)
		require.Equal(t, sumUserSets(shard), shard.NumClients())
	}
	require.Zero(t, shard.NumClients())
	require.Zero(t, sumUserSets(shard))

	// Removing an unknown client must not corrupt the count.
	unknown := newTestClient(t, node, "ghost")
	shard.remove(unknown)
	require.Zero(t, shard.NumClients())
	require.Equal(t, sumUserSets(shard), shard.NumClients())
}

// TestSubShardNumSubscriptionsMatchesWalk pins the maintained counter against
// the sum it replaced. The counter must survive resubscribe (same client+channel
// added twice must not double count) and removal of unknown subscriptions.
func TestSubShardNumSubscriptionsMatchesWalk(t *testing.T) {
	t.Parallel()
	walk := func(s *subShard) int {
		s.mu.RLock()
		defer s.mu.RUnlock()
		total := 0
		for _, subs := range s.subs {
			total += len(subs)
		}
		return total
	}

	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()
	h := node.hub

	var clients []*Client
	for i := 0; i < 10; i++ {
		c := newTestClient(t, node, "u"+strconv.Itoa(i))
		clients = append(clients, c)
	}

	check := func(msg string) {
		t.Helper()
		for i := 0; i < numHubShards; i++ {
			require.Equal(t, walk(h.subShards[i]), h.subShards[i].NumSubscriptions(), msg)
		}
	}

	// Many clients across several channels.
	for _, c := range clients {
		for ch := 0; ch < 5; ch++ {
			_, _, _ = h.addSub("chan"+strconv.Itoa(ch), subInfo{client: c})
			check("after add")
		}
	}
	require.Equal(t, 50, h.NumSubscriptions())

	// Resubscribing the same client to the same channel must not double count.
	_, _, _ = h.addSub("chan0", subInfo{client: clients[0]})
	check("after resubscribe")
	require.Equal(t, 50, h.NumSubscriptions())

	// Removing a subscription that is not there must not decrement.
	_, _, _ = h.removeSub("nosuchchan", clients[0], anySubGen)
	check("after removing unknown channel")
	require.Equal(t, 50, h.NumSubscriptions())

	for _, c := range clients {
		for ch := 0; ch < 5; ch++ {
			_, _, _ = h.removeSub("chan"+strconv.Itoa(ch), c, anySubGen)
			check("after remove")
		}
	}
	require.Zero(t, h.NumSubscriptions())
}

// ===========================================================================
// node_history_singleflight_test.go
// ===========================================================================

// TestHistoryUnrecoverablePositionKeepsResult pins a subtle contract of
// Node.history: it returns a POPULATED HistoryResult together with
// ErrorUnrecoverablePosition, and recovery depends on the StreamPosition in it.
// The single flight branch must not swallow the result when it returns an error.
//
// Both modes are asserted because single flight routes through a different code
// path (historySingleFlight) that unwraps the result from singleflight.Group.
func TestHistoryUnrecoverablePositionKeepsResult(t *testing.T) {
	t.Parallel()
	for _, singleFlight := range []bool{false, true} {
		name := "singleflight=false"
		if singleFlight {
			name = "singleflight=true"
		}
		t.Run(name, func(t *testing.T) {
			node := defaultNodeNoHandlers()
			node.config.UseSingleFlight = singleFlight
			defer func() { _ = node.Shutdown(context.Background()) }()

			_, err := node.Publish("test", []byte("{}"), WithHistory(10, time.Minute))
			require.NoError(t, err)

			// Epoch mismatch makes the position unrecoverable.
			res, err := node.History("test", WithLimit(1), WithSince(&StreamPosition{
				Offset: 0,
				Epoch:  "definitely-not-the-current-epoch",
			}))
			require.ErrorIs(t, err, ErrorUnrecoverablePosition)
			require.NotEmpty(t, res.StreamPosition.Epoch,
				"StreamPosition must survive alongside ErrorUnrecoverablePosition")
			require.Equal(t, uint64(1), res.StreamPosition.Offset)
		})
	}
}

// TestStreamTopMatchesHistory verifies streamTop, which no longer goes through
// Node.History's variadic options, still returns the same stream position as
// History does — in both single flight modes.
func TestStreamTopMatchesHistory(t *testing.T) {
	t.Parallel()
	for _, singleFlight := range []bool{false, true} {
		name := "singleflight=false"
		if singleFlight {
			name = "singleflight=true"
		}
		t.Run(name, func(t *testing.T) {
			node := defaultNodeNoHandlers()
			node.config.UseSingleFlight = singleFlight
			defer func() { _ = node.Shutdown(context.Background()) }()

			for i := 0; i < 3; i++ {
				_, err := node.Publish("test", []byte("{}"), WithHistory(10, time.Minute))
				require.NoError(t, err)
			}

			viaHistory, err := node.History("test", WithHistoryMetaTTL(time.Minute))
			require.NoError(t, err)

			top, err := node.streamTop("test", time.Minute)
			require.NoError(t, err)

			require.Equal(t, viaHistory.StreamPosition, top,
				"streamTop must agree with History on the stream position")
			require.Equal(t, uint64(3), top.Offset)
		})
	}
}

// TestStreamTopIsAllocationFree guards the periodic position check path: it runs
// for every positioned channel of every connection on every check, so it must
// not allocate. Node.History's variadic HistoryOption closures force
// HistoryOptions to escape, which is why streamTop builds them by value.
func TestStreamTopIsAllocationFree(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()
	_, err := node.Publish("test", []byte("{}"), WithHistory(10, time.Minute))
	require.NoError(t, err)

	// Warm up any lazily initialized state (metrics label combinations etc).
	for i := 0; i < 100; i++ {
		_, err := node.streamTop("test", time.Minute)
		require.NoError(t, err)
	}

	allocs := testing.AllocsPerRun(200, func() {
		_, _ = node.streamTop("test", time.Minute)
	})
	require.Zero(t, allocs, "streamTop must not allocate, got %v allocs/op", allocs)
}

// ===========================================================================
// node_medium_race_test.go
// ===========================================================================

// TestNode_ChannelMediumMapConcurrency guards a data race on n.mediums.
// n.mediums is a single map, but it is guarded by sharded mediumLocks (one lock
// per channel-hash). Operations on different channels take different lock shards
// yet mutate the same map, so a subscribe that creates a medium and a publish
// that reads one for a different channel access the map concurrently — a data
// race that can escalate to a fatal "concurrent map writes". Reachable via the
// exported channel-medium options. Must be run under -race.
func TestNode_ChannelMediumMapConcurrency(t *testing.T) {
	node, err := New(Config{
		LogLevel: LogLevelError, LogHandler: func(LogEntry) {},
		GetChannelMediumOptions: func(channel string) ChannelMediumOptions {
			return ChannelMediumOptions{KeepLatestPublication: true} // exported, public API
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	node.OnConnect(func(c *Client) {
		c.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) { cb(SubscribeReply{}, nil) })
	})
	if err := node.Run(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = node.Shutdown(context.Background()) }()

	client := newTestClientV2(t, node, "u")
	connectClientV2(t, client)

	const nch = 128 // spans many medium-lock shards
	names := make([]string, nch)
	for i := range names {
		names[i] = "cm" + strconv.Itoa(i)
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup

	// Publisher: reads n.mediums[ch] for every channel.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				for _, ch := range names {
					_, _ = node.Publish(ch, []byte(`{"x":1}`))
				}
			}
		}
	}()

	// Subscriber churn: creates (first sub) and deletes (last sub) mediums,
	// writing n.mediums[ch] for every channel.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			for _, ch := range names {
				rw := testReplyWriterWrapper()
				_ = client.handleSubscribe(&protocol.SubscribeRequest{Channel: ch}, &protocol.Command{Id: 1}, time.Now(), rw.rw)
			}
			for _, ch := range names {
				client.Unsubscribe(ch)
			}
		}
	}()

	time.Sleep(500 * time.Millisecond)
	close(stop)
	wg.Wait()
}

// ===========================================================================
// presence_tick_bench_test.go
// ===========================================================================

// benchPresenceTick measures the per-connection cost of one presence tick with
// a no-op PresenceManager, isolating the tick's own CPU/allocations from any
// network or encoding cost in the PresenceManager implementation.
func benchPresenceTick(b *testing.B, numChannels int, withPresence bool) {
	node, err := New(Config{
		LogLevel:           LogLevelError,
		LogHandler:         func(entry LogEntry) {},
		ClientChannelLimit: 1000,
	})
	if err != nil {
		b.Fatal(err)
	}
	node.SetPresenceManager(&noopPresenceManager{})
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{Options: SubscribeOptions{EmitPresence: withPresence}}, nil)
		})
	})
	if err := node.Run(); err != nil {
		b.Fatal(err)
	}
	defer func() { _ = node.Shutdown(context.Background()) }()

	client := newTestClientV2(b, node, "user")
	connectClientV2(b, client)
	for i := 0; i < numChannels; i++ {
		subscribeClientV2(b, client, "ch"+strconv.Itoa(i))
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		client.updatePresence()
	}
}

// With presence enabled on every channel.
func BenchmarkPresenceTick_1(b *testing.B)   { benchPresenceTick(b, 1, true) }
func BenchmarkPresenceTick_16(b *testing.B)  { benchPresenceTick(b, 16, true) }
func BenchmarkPresenceTick_128(b *testing.B) { benchPresenceTick(b, 128, true) }

// No presence on any channel — the tick has no presence work to do at all, but
// still runs every ClientPresenceUpdateInterval for every connection.
func BenchmarkPresenceTick_NoPresence_16(b *testing.B)  { benchPresenceTick(b, 16, false) }
func BenchmarkPresenceTick_NoPresence_128(b *testing.B) { benchPresenceTick(b, 128, false) }

// benchPositionTick measures a tick that performs the periodic stream position
// check. positionCheckTime is reset each iteration (outside the timer) so every
// tick actually performs the check — otherwise the 40s delay gate skips it and
// the benchmark measures nothing.
func benchPositionTick(b *testing.B, numChannels int, withPresence bool) {
	node, err := New(Config{
		LogLevel:                        LogLevelError,
		LogHandler:                      func(entry LogEntry) {},
		ClientChannelLimit:              1000,
		ClientChannelPositionCheckDelay: 10 * time.Millisecond,
		ClientChannelPositionMaxTimeLag: time.Hour,
	})
	if err != nil {
		b.Fatal(err)
	}
	node.SetPresenceManager(&noopPresenceManager{})
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{Options: SubscribeOptions{
				EnablePositioning: true,
				EmitPresence:      withPresence,
			}}, nil)
		})
	})
	if err := node.Run(); err != nil {
		b.Fatal(err)
	}
	defer func() { _ = node.Shutdown(context.Background()) }()

	client := newTestClientV2(b, node, "user")
	connectClientV2(b, client)
	for i := 0; i < numChannels; i++ {
		subscribeClientV2(b, client, "ch"+strconv.Itoa(i))
	}

	// Advance the clock on every read so positionCheckTime is always stale and
	// every tick really performs the check. Using b.StopTimer/StartTimer to reset
	// it instead would call ReadMemStats (stop-the-world) each iteration and
	// inflate ns/op by an order of magnitude.
	var clock atomic.Int64
	base := time.Now().Unix()
	node.mu.Lock()
	node.nowTimeGetter = func() time.Time {
		return time.Unix(base+clock.Add(100), 0)
	}
	node.mu.Unlock()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		client.updatePresence()
	}
}

func BenchmarkPositionTick_1(b *testing.B)   { benchPositionTick(b, 1, false) }
func BenchmarkPositionTick_16(b *testing.B)  { benchPositionTick(b, 16, false) }
func BenchmarkPositionTick_128(b *testing.B) { benchPositionTick(b, 128, false) }

// Presence + positioning together — the heaviest realistic tick.
func BenchmarkPositionTick_WithPresence_128(b *testing.B) { benchPositionTick(b, 128, true) }

// ===========================================================================
// timer_scheduler_mock_test.go
// ===========================================================================

// testSharedTimerScheduler is a minimal TimerScheduler for tests. It runs
// scheduled callbacks on a small fixed pool of goroutines shared across all
// connections — the property that matters for exercising onTimerOp's offload
// path: a callback that blocks (e.g. a slow RefreshHandler) holds up other
// callbacks queued on the same worker, so without offloading it would delay
// unrelated connections' pings.
//
// It is intentionally simple (one time.AfterFunc per timer, round-robin worker
// dispatch) rather than a real batching timer wheel — that lives in Centrifugo
// PRO and is not part of the OSS tree.
type testSharedTimerScheduler struct {
	workers  []chan func()
	next     atomic.Uint64
	stopCh   chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
}

func newTestSharedTimerScheduler(numWorkers int) *testSharedTimerScheduler {
	if numWorkers < 1 {
		numWorkers = 1
	}
	s := &testSharedTimerScheduler{
		workers: make([]chan func(), numWorkers),
		stopCh:  make(chan struct{}),
	}
	for i := range s.workers {
		s.workers[i] = make(chan func(), 4096)
		s.wg.Add(1)
		go func(ch chan func()) {
			defer s.wg.Done()
			for {
				select {
				case <-s.stopCh:
					return
				case fn := <-ch:
					fn()
				}
			}
		}(s.workers[i])
	}
	return s
}

func (s *testSharedTimerScheduler) ScheduleTimer(d time.Duration, cb func()) TimerCanceler {
	w := s.workers[int((s.next.Add(1)-1)%uint64(len(s.workers)))]
	c := &testTimerCanceler{}
	c.timer = time.AfterFunc(d, func() {
		if c.canceled.Load() {
			return
		}
		select {
		case w <- cb:
		case <-s.stopCh:
		}
	})
	return c
}

func (s *testSharedTimerScheduler) Stop() {
	s.stopOnce.Do(func() { close(s.stopCh) })
	s.wg.Wait()
}

type testTimerCanceler struct {
	timer    *time.Timer
	canceled atomic.Bool
}

func (c *testTimerCanceler) Cancel() {
	c.canceled.Store(true)
	if c.timer != nil {
		c.timer.Stop()
	}
}

// countingTransport is a minimal Transport that counts writes and reports a
// configurable ping/pong config. Used by the presence/runtime stress tests.
type countingTransport struct {
	writes       atomic.Int64
	pingInterval time.Duration
	pongTimeout  time.Duration
}

func (t *countingTransport) Name() string           { return "counting" }
func (t *countingTransport) AcceptProtocol() string { return "" }
func (t *countingTransport) Protocol() ProtocolType { return ProtocolTypeJSON }
func (t *countingTransport) ProtocolVersion() ProtocolVersion {
	return ProtocolVersion2
}
func (t *countingTransport) Unidirectional() bool      { return false }
func (t *countingTransport) Emulation() bool           { return false }
func (t *countingTransport) DisabledPushFlags() uint64 { return 0 }
func (t *countingTransport) PingPongConfig() PingPongConfig {
	return PingPongConfig{PingInterval: t.pingInterval, PongTimeout: t.pongTimeout}
}
func (t *countingTransport) Write(_ []byte) error {
	t.writes.Add(1)
	return nil
}
func (t *countingTransport) WriteMany(m ...[]byte) error {
	t.writes.Add(int64(len(m)))
	return nil
}
func (t *countingTransport) Close(_ Disconnect) error { return nil }

// noopPresenceManager is a PresenceManager whose operations do nothing, used by
// presence-tick benchmarks that measure tick overhead independent of a backend.
type noopPresenceManager struct{}

func (m *noopPresenceManager) Presence(_ string) (map[string]*ClientInfo, error) {
	return nil, nil
}
func (m *noopPresenceManager) PresenceStats(_ string) (PresenceStats, error) {
	return PresenceStats{}, nil
}
func (m *noopPresenceManager) AddPresence(_ string, _ string, _ *ClientInfo) error { return nil }
func (m *noopPresenceManager) RemovePresence(_ string, _ string, _ string) error   { return nil }

// ===========================================================================
// writer_timermode_integrity_test.go
// ===========================================================================

// recordingTransport records the Data of every item written, in order, so a test
// can assert no message is lost, duplicated, or reordered.
type recordingTransport struct {
	mu   sync.Mutex
	seen []uint64
}

func (r *recordingTransport) write(item queue.Item) error {
	r.mu.Lock()
	r.seen = append(r.seen, binary.BigEndian.Uint64(item.Data))
	r.mu.Unlock()
	return nil
}

func (r *recordingTransport) writeMany(items ...queue.Item) error {
	r.mu.Lock()
	for _, it := range items {
		r.seen = append(r.seen, binary.BigEndian.Uint64(it.Data))
	}
	r.mu.Unlock()
	return nil
}

func seqItem(n uint64) queue.Item {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, n)
	return queue.Item{Data: b}
}

// TestWriterTimerMode_NoLossSingleProducer drives the write_with_timer path with
// a single producer (the real per-connection invariant: the message writer is
// MPSC, and for one connection the producer is the fan-out for that connection).
// Every enqueued message must be written exactly once and in order.
func TestWriterTimerMode_NoLossSingleProducer(t *testing.T) {
	t.Parallel()
	rec := &recordingTransport{}
	w := newWriter(writerConfig{
		MaxQueueSize: 1 << 20,
		WriteFn:      rec.write,
		WriteManyFn:  rec.writeMany,
	}, 0)
	// write_delay + write_with_timer.
	w.run(2*time.Millisecond, 4, 0, true)

	const n = 5000
	for i := uint64(0); i < n; i++ {
		require.Nil(t, w.enqueue(seqItem(i)))
	}
	require.NoError(t, w.close(true)) // flush remaining

	require.Eventually(t, func() bool {
		rec.mu.Lock()
		defer rec.mu.Unlock()
		return len(rec.seen) == n
	}, 5*time.Second, 5*time.Millisecond, "wrote %d of %d", len(rec.seen), n)

	rec.mu.Lock()
	defer rec.mu.Unlock()
	for i := uint64(0); i < n; i++ {
		require.Equal(t, i, rec.seen[i], "message %d out of order or lost/duplicated", i)
	}
}

// TestWriterTimerMode_TimerDrainsWithoutClose targets the lost-wakeup path: it
// verifies the flush TIMER drains the queue on its own, without relying on
// close() to mop up remaining messages. A missed scheduleFlush (message added
// but no timer armed) would leave the queue stuck and time out here.
//
// Runs many rounds so a rare enqueue-vs-flush race has chances to surface.
func TestWriterTimerMode_TimerDrainsWithoutClose(t *testing.T) {
	t.Parallel()
	rec := &recordingTransport{}
	w := newWriter(writerConfig{
		MaxQueueSize: 1 << 20,
		WriteFn:      rec.write,
		WriteManyFn:  rec.writeMany,
	}, 0)
	w.run(time.Millisecond, 4, 0, true)
	defer func() { _ = w.close(false) }()

	var next uint64
	for round := 0; round < 200; round++ {
		batch := 1 + round%9 // vary batch size around the maxMessagesInFrame of 4
		var producers sync.WaitGroup
		for i := 0; i < batch; i++ {
			producers.Add(1)
			go func(v uint64) {
				defer producers.Done()
				_ = w.enqueue(seqItem(v))
			}(next)
			next++
		}
		producers.Wait()
		want := int(next)
		// The timer alone must drain everything enqueued so far.
		require.Eventually(t, func() bool {
			rec.mu.Lock()
			defer rec.mu.Unlock()
			return len(rec.seen) == want
		}, 3*time.Second, time.Millisecond,
			// Do not read len(rec.seen) here: require.Eventually evaluates the
			// message args eagerly on the caller goroutine while the flush timer is
			// still appending under rec.mu — that would be a data race (and the
			// pre-poll count is stale anyway).
			"round %d: timer left the queue stuck (want %d written) — lost flush wakeup",
			round, want)
	}
}

// TestWriterTimerMode_NoLossManyProducers stresses the timerScheduled handoff
// between enqueue and the flush timer under concurrent producers. Ordering is
// not asserted (concurrent producers have no total order), but every message
// must be written exactly once — no loss, no duplication.
func TestWriterTimerMode_NoLossManyProducers(t *testing.T) {
	t.Parallel()
	rec := &recordingTransport{}
	w := newWriter(writerConfig{
		MaxQueueSize: 1 << 20,
		WriteFn:      rec.write,
		WriteManyFn:  rec.writeMany,
	}, 0)
	w.run(time.Millisecond, 8, 0, true)

	const producers = 16
	const perProducer = 2000
	const total = producers * perProducer
	var wg sync.WaitGroup
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(base uint64) {
			defer wg.Done()
			for i := uint64(0); i < perProducer; i++ {
				_ = w.enqueue(seqItem(base + i))
			}
		}(uint64(p) * perProducer)
	}
	wg.Wait()
	require.NoError(t, w.close(true))

	require.Eventually(t, func() bool {
		rec.mu.Lock()
		defer rec.mu.Unlock()
		return len(rec.seen) == total
	}, 10*time.Second, 5*time.Millisecond, "wrote %d of %d", len(rec.seen), total)

	rec.mu.Lock()
	defer rec.mu.Unlock()
	got := make(map[uint64]int, total)
	for _, v := range rec.seen {
		got[v]++
	}
	for i := uint64(0); i < total; i++ {
		require.Equal(t, 1, got[i], "message %d written %d times (expected exactly once)", i, got[i])
	}
}

// ===========================================================================
// review_fixes_test.go — regressions for the post-review hardening pass
// ===========================================================================

// TestCommitSubscription_StolenReservationNotClobbered pins the generation
// check in commitSubscription. A subscribe that stalls past the unsubscribe
// wait-gate timeout loses its reservation: the timeout path nils the
// reservation's subscribingCh, a second unsubscribe gen-matches and deletes the
// entry, and a fresh subscribe re-reserves the channel with a newer generation.
// Without the check, the stalled subscribe's late commit would close the fresh
// reservation's subscribingCh (waking its waiters early) and overwrite
// c.channels with a context that has no hub routing.
func TestCommitSubscription_StolenReservationNotClobbered(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()
	client := newTestClientV2(t, node, "u")
	connectClientV2(t, client)
	const ch = "stolen"

	// A fresh subscribe's reservation, installed after ours was consumed.
	freshCh := make(chan struct{})
	freshGen := client.subGenCounter.Add(1)
	client.mu.Lock()
	client.channels[ch] = ChannelContext{subscribingCh: freshCh, subGen: freshGen}
	client.mu.Unlock()

	// Our stalled attempt commits with an older generation.
	staleGen := freshGen - 1
	subscribingCh, committed := client.commitSubscription(ch, ChannelContext{
		flags:  flagSubscribed,
		subGen: staleGen,
	}, reservationChannels)

	require.False(t, committed, "stale commit must not take over a reservation it no longer owns")
	require.Nil(t, subscribingCh, "stale commit must not return the fresh reservation's subscribingCh")

	select {
	case <-freshCh:
		t.Fatal("stale commit closed the fresh reservation's subscribingCh — its waiters wake early")
	default:
	}

	client.mu.RLock()
	got, ok := client.channels[ch]
	client.mu.RUnlock()
	require.True(t, ok, "fresh reservation must survive the stale commit")
	require.Equal(t, freshGen, got.subGen, "fresh reservation was overwritten by the stale commit")
	require.False(t, channelHasFlag(got.flags, flagSubscribed),
		"stale commit installed its channel context over the fresh reservation")

	// Drop the synthetic reservation: leaving it would make the node shutdown's
	// close() block on its never-closed subscribingCh for the full 5s wait gate.
	client.mu.Lock()
	delete(client.channels, ch)
	client.mu.Unlock()
}

// TestCommitSubscription_StolenMapReservationNotClobbered is the map-kind twin
// of the above: a stale map transition must not delete a fresh map
// subscription's mapSubscribing state (which would orphan its catch-up).
func TestCommitSubscription_StolenMapReservationNotClobbered(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()
	client := newTestClientV2(t, node, "u")
	connectClientV2(t, client)
	const ch = "stolen_map"

	freshState := &mapSubscribeState{
		subscribingCh: make(chan struct{}),
		subGen:        client.subGenCounter.Add(1),
	}
	client.mu.Lock()
	client.mapSubscribing = map[string]*mapSubscribeState{ch: freshState}
	client.mu.Unlock()

	subscribingCh, committed := client.commitSubscription(ch, ChannelContext{
		flags:  flagSubscribed,
		subGen: freshState.subGen - 1,
	}, reservationMap)

	require.False(t, committed)
	require.Nil(t, subscribingCh)
	select {
	case <-freshState.subscribingCh:
		t.Fatal("stale map commit closed the fresh reservation's subscribingCh")
	default:
	}
	client.mu.RLock()
	got, ok := client.mapSubscribing[ch]
	client.mu.RUnlock()
	require.True(t, ok, "stale map commit deleted the fresh mapSubscribing state")
	require.Same(t, freshState, got)
}

// TestCommitSubscription_MatchingReservationCommits is the positive control for
// the two tests above: the generation check must not break the happy path.
func TestCommitSubscription_MatchingReservationCommits(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()
	client := newTestClientV2(t, node, "u")
	connectClientV2(t, client)
	const ch = "ok"

	resvCh := make(chan struct{})
	gen := client.subGenCounter.Add(1)
	client.mu.Lock()
	client.channels[ch] = ChannelContext{subscribingCh: resvCh, subGen: gen}
	client.mu.Unlock()

	subscribingCh, committed := client.commitSubscription(ch, ChannelContext{
		flags:  flagSubscribed,
		subGen: gen,
	}, reservationChannels)

	require.True(t, committed, "matching generation must commit")
	require.Equal(t, resvCh, subscribingCh, "commit must hand back the reservation's subscribingCh to close")

	client.mu.RLock()
	got := client.channels[ch]
	client.mu.RUnlock()
	require.True(t, channelHasFlag(got.flags, flagSubscribed), "committed context must be installed")
	require.Equal(t, gen, got.subGen)
	require.Nil(t, got.subscribingCh, "commit must clear subscribingCh so unsubscribe cannot double-close it")
}

// TestMapStatePhase_DuplicateReservationRejected pins the guard added to the
// map STATE phase. validateSubscribeRequest rejects a duplicate earlier, but
// pagination spans multiple commands so two initial subscribes can both pass
// validation before either installs. Overwriting the first reservation orphans
// its subscribingCh — an unsubscribe waiting on it hits the 5s timeout and
// force-disconnects the client. The phase is called directly here because the
// earlier validation is what makes the window narrow in normal operation.
func TestMapStatePhase_DuplicateReservationRejected(t *testing.T) {
	node, _ := newTestNodeWithMapBroker(t)
	setTestMapChannelOptionsConverging(node)
	node.OnConnect(func(c *Client) {
		c.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{Options: SubscribeOptions{Type: SubscriptionTypeMap}}, nil)
		})
	})
	client := newTestConnectedClientV2(t, node, "u")
	const ch = "dup_state"

	existing := &mapSubscribeState{subscribingCh: make(chan struct{}), subGen: client.subGenCounter.Add(1)}
	client.mu.Lock()
	client.mapSubscribing = map[string]*mapSubscribeState{ch: existing}
	client.mu.Unlock()

	rw := testReplyWriterWrapper()
	err := client.handleMapStatePhase(
		&protocol.SubscribeRequest{Channel: ch, Type: int32(SubscriptionTypeMap), Phase: MapPhaseState},
		SubscribeReply{Options: SubscribeOptions{Type: SubscriptionTypeMap}},
		&protocol.Command{Id: 1}, time.Now(), rw.rw,
	)
	require.Equal(t, ErrorAlreadySubscribed, err, "duplicate map state reservation must be rejected")

	client.mu.RLock()
	got := client.mapSubscribing[ch]
	client.mu.RUnlock()
	require.Same(t, existing, got, "duplicate subscribe overwrote the in-flight reservation")
	select {
	case <-existing.subscribingCh:
		t.Fatal("existing reservation's subscribingCh was closed by the duplicate")
	default:
	}
}

// blockingPresenceManager lets a test suspend AddPresence mid-call, so a close
// can be landed precisely inside the subscribe's presence window.
type blockingPresenceManager struct {
	trackingPresenceManager
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (m *blockingPresenceManager) AddPresence(ch string, clientID string, info *ClientInfo) error {
	m.once.Do(func() {
		close(m.entered)
		<-m.release
	})
	return m.trackingPresenceManager.AddPresence(ch, clientID, info)
}

// TestSubscribeRollback_RemovesPresenceOnCloseDuringSubscribe guards the
// presence leak on the closed-mid-subscribe rollback. subscribeCmd adds
// node-level presence before commitSubscription; if the client closes in that
// window, close() has already snapshotted c.channels without this channel (that
// is why the rollback runs), so nothing else can remove the entry and it
// lingers until PresenceTTL — 60s by default.
func TestSubscribeRollback_RemovesPresenceOnCloseDuringSubscribe(t *testing.T) {
	pm := &blockingPresenceManager{
		trackingPresenceManager: trackingPresenceManager{entries: map[string]map[string]struct{}{}},
		entered:                 make(chan struct{}),
		release:                 make(chan struct{}),
	}
	node, err := New(Config{LogLevel: LogLevelError, LogHandler: func(LogEntry) {}})
	require.NoError(t, err)
	node.SetPresenceManager(pm)
	node.OnConnect(func(c *Client) {
		c.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{Options: SubscribeOptions{EmitPresence: true}}, nil)
		})
	})
	require.NoError(t, node.Run())
	defer func() { _ = node.Shutdown(context.Background()) }()

	client := newTestClientV2(t, node, "u")
	connectClientV2(t, client)
	const ch = "presence_rollback"

	var subDone sync.WaitGroup
	subDone.Add(1)
	go func() {
		defer subDone.Done()
		rw := testReplyWriterWrapper()
		_ = client.handleSubscribe(&protocol.SubscribeRequest{Channel: ch}, &protocol.Command{Id: 1}, time.Now(), rw.rw)
	}()

	<-pm.entered // subscribe is inside AddPresence, past its own closed check

	var closeDone sync.WaitGroup
	closeDone.Add(1)
	go func() {
		defer closeDone.Done()
		_ = client.close(DisconnectForceNoReconnect)
	}()

	// Wait until close has actually flipped the status, so the commit below is
	// guaranteed to take the rollback path rather than committing normally.
	require.Eventually(t, func() bool {
		client.mu.RLock()
		defer client.mu.RUnlock()
		return client.status == statusClosed
	}, 5*time.Second, time.Millisecond, "client did not reach closed status")

	close(pm.release)
	subDone.Wait()
	closeDone.Wait()

	require.Equal(t, 0, pm.liveEntries(),
		"presence entry survived the closed-mid-subscribe rollback — leaks until PresenceTTL")
	require.False(t, hubHasSub(node, ch, client.ID()), "hub entry survived the rollback")
}

// TestConnectServerSideSubs_ErrorRollsBackPresence covers connectCmd aborting
// after some connect-time server-side subscriptions already succeeded. The
// failed channel aborts the whole connect, so the succeeded channels' presence
// entries (and reservations) must be rolled back — close() cannot do it, since
// the connection never completed and its channels never entered a snapshot.
func TestConnectServerSideSubs_ErrorRollsBackPresence(t *testing.T) {
	pm := newTrackingPresenceManager(0)
	node, err := New(Config{LogLevel: LogLevelError, LogHandler: func(LogEntry) {}})
	require.NoError(t, err)
	node.SetPresenceManager(pm)
	// Connect-time server-side subscriptions bypass OnSubscribe (subscribeCmd is
	// called directly with these options), so "bad" is failed via an already
	// expired subscription — it aborts the connect after the others subscribed
	// and added presence.
	node.OnConnecting(func(ctx context.Context, e ConnectEvent) (ConnectReply, error) {
		return ConnectReply{Subscriptions: map[string]SubscribeOptions{
			"ok1": {EmitPresence: true},
			"ok2": {EmitPresence: true},
			"ok3": {EmitPresence: true},
			"bad": {EmitPresence: true, ExpireAt: time.Now().Unix() - 3600},
		}}, nil
	})
	require.NoError(t, node.Run())
	defer func() { _ = node.Shutdown(context.Background()) }()

	client := newTestClientV2(t, node, "u")
	rw := testReplyWriterWrapper()
	connErr := client.connectCmd(&protocol.ConnectRequest{}, &protocol.Command{}, time.Now(), rw.rw)
	require.NotNil(t, connErr, "connect must fail when a server-side subscription fails")

	require.Equal(t, 0, pm.liveEntries(),
		"presence entries from succeeded server-side subs leaked after connect aborted")
	for _, ch := range []string{"ok1", "ok2", "bad", "ok3"} {
		require.Falsef(t, hubHasSub(node, ch, client.ID()), "hub entry leaked for %s", ch)
		present, _ := clientHasChannel(client, ch)
		require.Falsef(t, present, "reservation leaked in c.channels for %s", ch)
	}
}

// TestUnsubscribeEvent_ServerSideRecomputedAfterWait pins the ServerSide field
// of UnsubscribeEvent. An unsubscribe that races an in-flight subscribe reads
// the reservation first (flags==0, so ServerSide reads false), waits on
// subscribingCh, then tears down whatever finalized. The event must describe
// the subscription actually torn down, not the pre-wait reservation.
func TestUnsubscribeEvent_ServerSideRecomputedAfterWait(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	gotServerSide := make(chan bool, 1)
	node.OnConnect(func(c *Client) {
		c.OnUnsubscribe(func(e UnsubscribeEvent) { gotServerSide <- e.ServerSide })
	})
	client := newTestClientV2(t, node, "u")
	connectClientV2(t, client)
	const ch = "srv_side"

	// In-flight subscribe reservation: no flags yet.
	subscribingCh := make(chan struct{})
	gen := client.subGenCounter.Add(1)
	client.mu.Lock()
	client.channels[ch] = ChannelContext{subscribingCh: subscribingCh, subGen: gen}
	client.mu.Unlock()

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = client.unsubscribe(ch, unsubscribeServer, nil)
	}()

	// Let the unsubscribe reach its wait gate, then finalize the subscribe as a
	// server-side subscription carrying the same generation.
	require.Eventually(t, func() bool {
		client.mu.RLock()
		defer client.mu.RUnlock()
		return client.channels[ch].subscribingCh != nil
	}, time.Second, time.Millisecond)
	client.mu.Lock()
	client.channels[ch] = ChannelContext{flags: flagSubscribed | flagServerSide, subGen: gen}
	client.mu.Unlock()
	close(subscribingCh)

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("unsubscribe did not finish")
	}

	select {
	case ss := <-gotServerSide:
		require.True(t, ss,
			"UnsubscribeEvent.ServerSide came from the pre-wait reservation snapshot, not the torn-down subscription")
	case <-time.After(time.Second):
		t.Fatal("no unsubscribe event emitted")
	}
}

// mapReservation reports whether an in-flight map subscribe reservation exists.
func mapReservation(c *Client, ch string) (*mapSubscribeState, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	st, ok := c.mapSubscribing[ch]
	return st, ok
}

// unsubscribeDuration times an unsubscribe. A reservation whose subscribingCh
// was never closed makes unsubscribe block on its 5s wait gate and then
// force-disconnect the client, so a prompt return is the observable signal that
// the reservation was cleaned up properly.
func unsubscribeDuration(c *Client, ch string) time.Duration {
	start := time.Now()
	_ = c.unsubscribe(ch, unsubscribeClient, nil)
	return time.Since(start)
}

// TestMapDirectLiveRecovery_SuccessLeavesNoReservation covers the reservation
// now installed by the direct-to-LIVE recovery join (a path that previously ran
// with no mapSubscribing entry at all, so neither the commit's generation check
// nor the unsubscribe wait-gate covered it). On success the reservation must be
// consumed by the commit, leaving no state behind and no unclosed subscribingCh.
func TestMapDirectLiveRecovery_SuccessLeavesNoReservation(t *testing.T) {
	node, broker := newTestNodeWithMapBroker(t)
	setTestMapChannelOptionsConverging(node)
	const ch = "direct_live_ok"

	var epoch string
	for i := 0; i < 5; i++ {
		res, err := broker.Publish(context.Background(), ch, string(rune('a'+i)), MapPublishOptions{
			Data: []byte(`{"v":"d"}`),
		})
		require.NoError(t, err)
		epoch = res.Position.Epoch
	}

	node.OnConnect(func(c *Client) {
		c.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{Options: SubscribeOptions{Type: SubscriptionTypeMap}}, nil)
		})
	})
	client := newTestConnectedClientV2(t, node, "u")

	res := subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: ch, Type: int32(SubscriptionTypeMap), Phase: MapPhaseLive,
		Offset: 2, Epoch: epoch, Recover: true,
	})
	require.Equal(t, MapPhaseLive, res.Phase)

	_, hasResv := mapReservation(client, ch)
	require.False(t, hasResv, "direct-to-LIVE reservation leaked after a successful subscribe")
	present, subscribed := clientHasChannel(client, ch)
	require.True(t, present && subscribed, "channel must be live after direct-to-LIVE recovery")
	require.True(t, hubHasSub(node, ch, client.ID()), "hub entry missing after successful subscribe")

	// The reservation's subscribingCh must have been closed by the commit —
	// otherwise this blocks for the full 5s wait-gate timeout.
	require.Less(t, unsubscribeDuration(client, ch), 2*time.Second,
		"unsubscribe blocked on an unclosed subscribingCh from the direct-to-LIVE reservation")
	require.False(t, hubHasSub(node, ch, client.ID()))
}

// TestMapDirectLiveRecovery_ErrorRollsBackReservationAndHub is the failure twin:
// an epoch mismatch aborts the transition, and the rollback must remove the hub
// entry and the reservation (closing its subscribingCh) so the channel is
// immediately re-subscribable and a later unsubscribe cannot hang.
func TestMapDirectLiveRecovery_ErrorRollsBackReservationAndHub(t *testing.T) {
	node, broker := newTestNodeWithMapBroker(t)
	setTestMapChannelOptionsConverging(node)
	const ch = "direct_live_err"

	for i := 0; i < 5; i++ {
		_, err := broker.Publish(context.Background(), ch, string(rune('a'+i)), MapPublishOptions{
			Data: []byte(`{"v":"d"}`),
		})
		require.NoError(t, err)
	}

	node.OnConnect(func(c *Client) {
		c.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{Options: SubscribeOptions{Type: SubscriptionTypeMap}}, nil)
		})
	})
	client := newTestConnectedClientV2(t, node, "u")

	protoErr := subscribeMapClientExpectError(t, client, &protocol.SubscribeRequest{
		Channel: ch, Type: int32(SubscriptionTypeMap), Phase: MapPhaseLive,
		Offset: 2, Epoch: "definitely-not-the-epoch", Recover: true,
	})
	require.Equal(t, ErrorUnrecoverablePosition.Code, protoErr.Code)

	_, hasResv := mapReservation(client, ch)
	require.False(t, hasResv, "reservation leaked after the direct-to-LIVE rollback")
	present, _ := clientHasChannel(client, ch)
	require.False(t, present, "channel context leaked after the direct-to-LIVE rollback")
	require.False(t, hubHasSub(node, ch, client.ID()), "hub entry leaked after the direct-to-LIVE rollback")

	// The channel must be immediately re-subscribable: a leaked reservation
	// would make validateSubscribeRequest reject this with AlreadySubscribed.
	require.Less(t, unsubscribeDuration(client, ch), 2*time.Second,
		"unsubscribe blocked on a subscribingCh the rollback failed to close")
}

// partialFailPresenceManager records the presence entry and then reports an
// error, modelling a manager whose write landed server-side but whose call
// failed (or a partial multi-step update). It can also suspend mid-add so the
// add is made to land after a concurrent unsubscribe.
type partialFailPresenceManager struct {
	trackingPresenceManager
	failing atomic.Bool
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (m *partialFailPresenceManager) AddPresence(ch string, clientID string, info *ClientInfo) error {
	if m.failing.Load() {
		m.once.Do(func() {
			close(m.entered)
			<-m.release
		})
	}
	_ = m.trackingPresenceManager.AddPresence(ch, clientID, info)
	if m.failing.Load() {
		return fmt.Errorf("partial presence failure")
	}
	return nil
}

// TestPresenceTick_CompensatesAfterPartialFailure pins the `attempted` return of
// updateChannelPresence. A presence add issued by the tick can land after a
// concurrent unsubscribe already removed the entry, resurrecting it; that is
// what compensateRacedPresence exists to undo. The add may also report an error
// while still having landed server-side (partial multi-step update). Marking the
// channel compensation-eligible only on a nil error therefore leaked exactly the
// entry the compensation was added to remove — it must be marked whenever an add
// was issued.
func TestPresenceTick_CompensatesAfterPartialFailure(t *testing.T) {
	pm := &partialFailPresenceManager{
		trackingPresenceManager: trackingPresenceManager{entries: map[string]map[string]struct{}{}},
		entered:                 make(chan struct{}),
		release:                 make(chan struct{}),
	}
	node, err := New(Config{LogLevel: LogLevelError, LogHandler: func(LogEntry) {}})
	require.NoError(t, err)
	node.SetPresenceManager(pm)
	node.OnConnect(func(c *Client) {
		c.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{Options: SubscribeOptions{EmitPresence: true}}, nil)
		})
	})
	require.NoError(t, node.Run())
	defer func() { _ = node.Shutdown(context.Background()) }()

	client := newTestClientV2(t, node, "u")
	connectClientV2(t, client)
	const ch = "partial"
	subscribeClientV2(t, client, ch)
	require.Equal(t, 1, pm.liveEntries(), "precondition: subscribe added presence")

	// From here every add suspends once, then records the entry and reports an error.
	pm.failing.Store(true)

	client.mu.RLock()
	chCtx := client.channels[ch]
	client.mu.RUnlock()
	snapshot := []channelTickItem{{channel: ch, ctx: chCtx, duties: dutyPresence}}

	var tickDone sync.WaitGroup
	tickDone.Add(1)
	go func() {
		defer tickDone.Done()
		client.updateChannelPresenceItem(&snapshot[0])
	}()

	<-pm.entered // the tick passed its channel-existence check and is inside the add

	// The unsubscribe removes the channel and its presence entry while the tick's
	// add is still in flight.
	_ = client.unsubscribe(ch, unsubscribeClient, nil)
	require.Equal(t, 0, pm.liveEntries(), "precondition: unsubscribe removed the entry")

	close(pm.release)
	tickDone.Wait()

	require.True(t, snapshot[0].presenceAdded,
		"a presence add that errored must still be marked — the entry may exist server-side")
	require.Equal(t, 1, pm.liveEntries(), "precondition: the raced tick add resurrected the entry")

	client.compensateRacedPresence(snapshot)
	require.Equal(t, 0, pm.liveEntries(),
		"presence resurrected by a partially-failed tick add was not compensated — leaks until PresenceTTL")
}

// TestMapLivePhase_TakesPaginationLock pins the serialization of the direct
// LIVE-phase command. validateSubscribeRequest deliberately lets any non-STATE
// subscribe through while a mapSubscribing reservation exists (pagination spans
// commands), and the STATE/STREAM phases rely on the pagination lock to keep
// only one transition per channel in flight. The LIVE phase took no lock, so a
// concurrent STREAM command could adopt the live transition's reservation and
// inherit its subGen — two transitions sharing one generation, where the loser's
// rollback removes the winner's hub entry and leaves the client live in
// c.channels with no routing.
func TestMapLivePhase_TakesPaginationLock(t *testing.T) {
	node, broker := newTestNodeWithMapBroker(t)
	setTestMapChannelOptionsConverging(node)
	const ch = "live_pagination"

	res, err := broker.Publish(context.Background(), ch, "k", MapPublishOptions{Data: []byte(`{"v":"d"}`)})
	require.NoError(t, err)

	node.OnConnect(func(c *Client) {
		c.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{Options: SubscribeOptions{Type: SubscriptionTypeMap}}, nil)
		})
	})
	client := newTestConnectedClientV2(t, node, "u")

	// Another pagination for this channel is in progress.
	require.True(t, client.acquireMapPaginationLock(ch))

	protoErr := subscribeMapClientExpectError(t, client, &protocol.SubscribeRequest{
		Channel: ch, Type: int32(SubscriptionTypeMap), Phase: MapPhaseLive,
		Offset: 0, Epoch: res.Position.Epoch, Recover: true,
	})
	require.Equal(t, ErrorConcurrentPagination.Code, protoErr.Code,
		"LIVE phase must serialize with the other phases on the pagination lock")

	// It must not have installed a reservation while bailing out.
	_, hasResv := mapReservation(client, ch)
	require.False(t, hasResv, "rejected LIVE phase left a reservation behind")

	// Released lock: the same subscribe now succeeds, proving the lock is the
	// only thing that rejected it.
	client.releaseMapPaginationLock(ch)
	sub := subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: ch, Type: int32(SubscriptionTypeMap), Phase: MapPhaseLive,
		Offset: 0, Epoch: res.Position.Epoch, Recover: true,
	})
	require.Equal(t, MapPhaseLive, sub.Phase)
}

// historyErrorBroker fails History (and therefore streamTop) on demand, so a
// subscribe can be made to fail after it has already added presence.
type historyErrorBroker struct {
	*MemoryBroker
	fail atomic.Bool
}

func (b *historyErrorBroker) History(ch string, opts HistoryOptions) ([]*Publication, StreamPosition, error) {
	if b.fail.Load() {
		return nil, StreamPosition{}, fmt.Errorf("history unavailable")
	}
	return b.MemoryBroker.History(ch, opts)
}

// TestSubscribeCmd_RemovesPresenceWhenSubscribeFailsAfterAdd covers the presence
// leak on subscribeCmd's own failure paths. Presence is added partway through,
// before the subscription is committed; a later failure (here a broker error
// resolving the stream position) returns a zero-valued channelContext, so the
// commit-time rollback has no flags to act on and onSubscribeErrorGen only cleans
// the reservation and the hub entry. Nothing else can remove the entry — the
// channel never became a committed subscription — so it lingers until
// PresenceTTL.
func TestSubscribeCmd_RemovesPresenceWhenSubscribeFailsAfterAdd(t *testing.T) {
	node, err := New(Config{LogLevel: LogLevelError, LogHandler: func(LogEntry) {}})
	require.NoError(t, err)
	mb, err := NewMemoryBroker(node, MemoryBrokerConfig{})
	require.NoError(t, err)
	brk := &historyErrorBroker{MemoryBroker: mb}
	node.SetBroker(brk)
	pm := newTrackingPresenceManager(0)
	node.SetPresenceManager(pm)
	node.OnConnect(func(c *Client) {
		c.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{Options: SubscribeOptions{
				EmitPresence:      true,
				EnablePositioning: true, // forces the streamTop lookup below
			}}, nil)
		})
	})
	require.NoError(t, node.Run())
	defer func() { _ = node.Shutdown(context.Background()) }()

	client := newTestClientV2(t, node, "u")
	connectClientV2(t, client)
	const ch = "presence_after_add"

	brk.fail.Store(true)
	rw := testReplyWriterWrapper()
	_ = client.handleSubscribe(&protocol.SubscribeRequest{Channel: ch}, &protocol.Command{Id: 1}, time.Now(), rw.rw)

	present, _ := clientHasChannel(client, ch)
	require.False(t, present, "precondition: the subscribe must have failed")
	require.Equal(t, 0, pm.liveEntries(),
		"presence added before the failure was not removed — leaks until PresenceTTL")
}

// TestOnSubscribeError_GenMatchedDoesNotClobberFreshReservation covers the
// cleanup path taken when an asynchronous OnSubscribe callback fails. That
// callback can outlive its own reservation: if it stalls past the unsubscribe
// wait-gate timeout, the reservation is dropped and a fresh subscribe may
// re-reserve the channel. Undoing by channel name alone deleted that fresh
// reservation and closed a subscribingCh its waiters still need.
func TestOnSubscribeError_GenMatchedDoesNotClobberFreshReservation(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()
	client := newTestClientV2(t, node, "u")
	connectClientV2(t, client)
	const ch = "gen_cleanup"

	staleGen := client.subGenCounter.Add(1)
	freshCh := make(chan struct{})
	freshGen := client.subGenCounter.Add(1)
	client.mu.Lock()
	client.channels[ch] = ChannelContext{subscribingCh: freshCh, subGen: freshGen}
	client.mu.Unlock()

	// A stalled earlier attempt cleaning up with its own (older) generation.
	client.onSubscribeErrorGen(ch, staleGen)

	client.mu.RLock()
	got, ok := client.channels[ch]
	client.mu.RUnlock()
	require.True(t, ok, "stale cleanup deleted the fresh reservation")
	require.Equal(t, freshGen, got.subGen)
	select {
	case <-freshCh:
		t.Fatal("stale cleanup closed the fresh reservation's subscribingCh")
	default:
	}

	// The owning generation still cleans up normally.
	client.onSubscribeErrorGen(ch, freshGen)
	client.mu.RLock()
	_, stillThere := client.channels[ch]
	client.mu.RUnlock()
	require.False(t, stillThere, "matching generation must remove its own reservation")
	select {
	case <-freshCh:
	default:
		t.Fatal("matching cleanup must close the reservation's subscribingCh")
	}
}

// TestMapCommit_InstallsOverOccupiedChannelSlot pins a subtlety that is easy to
// "harden" into a bug. The hub keeps exactly one entry per (channel, client) and
// addSub overwrites it, so by the time a map subscription commits, its own
// addSubscription has already replaced whatever hub entry was there. The commit
// must therefore install into c.channels unconditionally: refusing (because the
// slot looks occupied by another generation) leaves c.channels carrying the older
// subscription while the hub carries this generation, and the ensuing rollback
// removes that hub entry — yielding a channel that looks subscribed but receives
// nothing. Racing subscribes are prevented at the source (validateSubscribeRequest
// and Client.Subscribe reject a channel already in c.channels or c.mapSubscribing),
// not here.
func TestMapCommit_InstallsOverOccupiedChannelSlot(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()
	client := newTestClientV2(t, node, "u")
	connectClientV2(t, client)
	const ch = "occupied"

	// An older live subscription occupies the slot.
	oldGen := client.subGenCounter.Add(1)
	client.mu.Lock()
	client.channels[ch] = ChannelContext{flags: flagSubscribed, subGen: oldGen}
	client.mu.Unlock()

	// A map transition that owns its reservation commits with a newer generation.
	newGen := client.subGenCounter.Add(1)
	state := &mapSubscribeState{subscribingCh: make(chan struct{}), subGen: newGen}
	client.mu.Lock()
	client.mapSubscribing = map[string]*mapSubscribeState{ch: state}
	client.mu.Unlock()

	subscribingCh, committed := client.commitSubscription(ch, ChannelContext{
		flags:  flagSubscribed,
		subGen: newGen,
	}, reservationMap)

	require.True(t, committed,
		"map commit must install even when the channel slot is occupied — the hub entry was already overwritten by this subscribe")
	require.Equal(t, state.subscribingCh, subscribingCh)

	client.mu.RLock()
	got := client.channels[ch]
	_, resvGone := client.mapSubscribing[ch]
	client.mu.RUnlock()
	require.Equal(t, newGen, got.subGen,
		"c.channels must carry the committed generation so it matches the hub entry")
	require.False(t, resvGone, "commit must consume the map reservation")

	client.mu.Lock()
	delete(client.channels, ch)
	client.mu.Unlock()
}

// ===========================================================================
// client_close_teardown_test.go
// ===========================================================================

// gatedPresenceManager tracks which (channel, clientID) pairs are present and
// lets a test block exactly one AddPresence or RemovePresence call to hold the
// connection's close path open at a chosen point.
type gatedPresenceManager struct {
	mu      sync.Mutex
	present map[string]map[string]bool

	// Armed by the test via addArmed/removeArmed. The first call after arming
	// reports itself on <op>Entered and then blocks until <op>Release is closed.
	addArmed      atomic.Bool
	addEntered    chan string
	addRelease    chan struct{}
	removeArmed   atomic.Bool
	removeEntered chan string
	removeRelease chan struct{}
}

func newGatedPresenceManager() *gatedPresenceManager {
	return &gatedPresenceManager{
		present:       map[string]map[string]bool{},
		addEntered:    make(chan string, 1),
		addRelease:    make(chan struct{}),
		removeEntered: make(chan string, 1),
		removeRelease: make(chan struct{}),
	}
}

func (m *gatedPresenceManager) Presence(ch string) (map[string]*ClientInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	res := map[string]*ClientInfo{}
	for clientID := range m.present[ch] {
		res[clientID] = &ClientInfo{ClientID: clientID}
	}
	return res, nil
}

func (m *gatedPresenceManager) PresenceStats(_ string) (PresenceStats, error) {
	return PresenceStats{}, nil
}

func (m *gatedPresenceManager) AddPresence(ch string, clientID string, _ *ClientInfo) error {
	if m.addArmed.CompareAndSwap(true, false) {
		m.addEntered <- ch
		<-m.addRelease
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.present[ch] == nil {
		m.present[ch] = map[string]bool{}
	}
	m.present[ch][clientID] = true
	return nil
}

func (m *gatedPresenceManager) RemovePresence(ch string, clientID string, _ string) error {
	if m.removeArmed.CompareAndSwap(true, false) {
		m.removeEntered <- ch
		<-m.removeRelease
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.present[ch], clientID)
	if len(m.present[ch]) == 0 {
		delete(m.present, ch)
	}
	return nil
}

func (m *gatedPresenceManager) numPresent() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, clients := range m.present {
		n += len(clients)
	}
	return n
}

// countGoroutinesRunning returns how many goroutine stacks currently contain
// needle.
func countGoroutinesRunning(needle string) int {
	buf := make([]byte, 1<<20)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			return strings.Count(string(buf[:n]), needle)
		}
		buf = make([]byte, 2*len(buf))
	}
}

const clientCloseStackFrame = "centrifuge.(*Client).close("

// TestClose_BroadcastDuringCleanupSpawnsNoExtraCloses guards the broadcast path
// against a goroutine storm while a connection is closing.
//
// close() tears the transport (and the writer) down before running the
// per-channel cleanup, so that a slow PresenceManager can not hold the socket
// open. But the connection stays registered in the hub's subscription shards
// until the cleanup loop reaches each channel, so publications keep being routed
// to it in the meantime. Every one of them fails to enqueue on the closed writer,
// and writeEncodedPushData used to answer that by spawning `go c.close(...)` -
// one goroutine per message, each of which only blocks until the in-flight close
// finishes. The storm is worst exactly when Redis is slow, which is when the
// cleanup is slow.
func TestClose_BroadcastDuringCleanupSpawnsNoExtraCloses(t *testing.T) {
	const chA, chB = "close_cleanup_a", "close_cleanup_b"
	const numPublications = 300

	pm := newGatedPresenceManager()
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()
	node.SetPresenceManager(pm)
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{Options: SubscribeOptions{EmitPresence: true}}, nil)
		})
	})

	client := newTestConnectedClientV2(t, node, "42")
	subscribeClientV2(t, client, chA)
	subscribeClientV2(t, client, chB)
	require.Equal(t, 2, pm.numPresent())

	// Block the first RemovePresence of the cleanup loop. The loop walks channels
	// in map order, so we learn which one it took and publish into the other -
	// that one is still in c.channels and still in the hub, i.e. still a live
	// broadcast target with a dead writer.
	pm.removeArmed.Store(true)

	closeDone := make(chan struct{})
	go func() {
		_ = client.close(DisconnectConnectionClosed)
		close(closeDone)
	}()

	var gatedChannel string
	select {
	case gatedChannel = <-pm.removeEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("cleanup loop never reached RemovePresence")
	}
	openChannel := chA
	if gatedChannel == chA {
		openChannel = chB
	}

	// Precondition: the closing connection is still a broadcast target on the
	// channel the cleanup has not reached yet. Without this the test would pass
	// trivially.
	require.Equal(t, 1, node.hub.NumSubscribers(openChannel),
		"connection must still be subscribed while cleanup is in flight")

	before := countGoroutinesRunning(clientCloseStackFrame)
	for i := 0; i < numPublications; i++ {
		_, err := node.Publish(openChannel, []byte(`{"n":`+strconv.Itoa(i)+`}`))
		require.NoError(t, err)
	}
	// Give any goroutine the broadcast path spawned time to be scheduled and show
	// up in the stack dump.
	time.Sleep(100 * time.Millisecond)
	during := countGoroutinesRunning(clientCloseStackFrame)

	// `before` is 1: the close() we started above, parked in RemovePresence.
	require.LessOrEqual(t, during, before+1,
		"broadcasts to a closing connection must not spawn a close goroutine per message "+
			"(before=%d during=%d after %d publications)", before, during, numPublications)

	close(pm.removeRelease)
	select {
	case <-closeDone:
	case <-time.After(5 * time.Second):
		t.Fatal("close did not finish")
	}

	require.Zero(t, pm.numPresent(), "cleanup must still remove presence for every channel")
	require.Zero(t, node.hub.NumSubscribers(chA))
	require.Zero(t, node.hub.NumSubscribers(chB))
	require.Empty(t, node.hub.UserConnections("42"))
}

// TestClose_TransportTeardownNotGatedByInFlightPresenceTick guards the socket
// teardown against an in-flight presence tick.
//
// The tick holds presenceMu across its PresenceManager round trips. close() also
// needs presenceMu - the cleanup removes presence entries and must not overlap a
// tick that is still adding them - but it only needs it for that cleanup. Taking
// it up front put a whole Redis round trip (or its timeout) in front of
// transport.Close, so a hung PresenceManager kept the socket, the hub entry and
// the inflight-connections gauge alive for the duration - the same lingering
// connection this ordering exists to prevent.
func TestClose_TransportTeardownNotGatedByInFlightPresenceTick(t *testing.T) {
	const channel = "close_tick_channel"

	pm := newGatedPresenceManager()
	node, err := New(Config{
		LogLevel:                     LogLevelError,
		LogHandler:                   func(entry LogEntry) {},
		ClientPresenceUpdateInterval: 50 * time.Millisecond,
	})
	require.NoError(t, err)
	node.SetPresenceManager(pm)
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{Options: SubscribeOptions{EmitPresence: true}}, nil)
		})
	})
	require.NoError(t, node.Run())
	defer func() { _ = node.Shutdown(context.Background()) }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	transport := newTestTransport(cancel)
	client := newTestSubscribedClientWithTransport(t, ctx, node, transport, "42", channel)

	// Arm only now: the subscribe above already paid one AddPresence.
	pm.addArmed.Store(true)
	select {
	case <-pm.addEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("presence tick never reached AddPresence")
	}
	// The tick is parked inside AddPresence holding presenceMu.

	closeDone := make(chan struct{})
	go func() {
		_ = client.close(DisconnectConnectionClosed)
		close(closeDone)
	}()

	select {
	case <-transport.closeCh:
	case <-time.After(2 * time.Second):
		close(pm.addRelease)
		t.Fatal("transport was not closed while a presence tick was in flight")
	}
	require.Empty(t, node.hub.UserConnections("42"),
		"connection must leave the hub without waiting for the presence tick")

	// close() is now parked on presenceMu waiting for the tick, as it must be:
	// releasing the tick lets it finish adding presence, and only then does the
	// cleanup remove it.
	close(pm.addRelease)
	select {
	case <-closeDone:
	case <-time.After(5 * time.Second):
		t.Fatal("close did not finish")
	}

	require.Zero(t, pm.numPresent(),
		"presence added by the in-flight tick must still be removed by the cleanup")
	require.Zero(t, node.hub.NumSubscribers(channel))
}
