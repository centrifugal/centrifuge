package centrifuge

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/require"
)

// unsubscribeRecorder collects UnsubscribeHandler calls, and counts those which
// came after DisconnectHandler.
type unsubscribeRecorder struct {
	mu                 sync.Mutex
	events             []UnsubscribeEvent
	disconnected       bool
	afterDisconnect    int
	disconnectHandlers int
}

func (r *unsubscribeRecorder) record(e UnsubscribeEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, e)
	if r.disconnected {
		r.afterDisconnect++
	}
}

func (r *unsubscribeRecorder) recordDisconnect(DisconnectEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.disconnected = true
	r.disconnectHandlers++
}

// requireDisconnectLast makes sure DisconnectHandler was called once, after
// every UnsubscribeHandler call so far.
func (r *unsubscribeRecorder) requireDisconnectLast(t *testing.T) {
	t.Helper()
	r.requireDisconnectedOnce(t)
	r.mu.Lock()
	defer r.mu.Unlock()
	require.Zero(t, r.afterDisconnect, "UnsubscribeHandler called after DisconnectHandler")
}

// requireDisconnectedOnce makes sure DisconnectHandler was called once.
// UnsubscribeHandler calls may still follow it (a SubscribeCallback invoked
// after the disconnect).
func (r *unsubscribeRecorder) requireDisconnectedOnce(t *testing.T) {
	t.Helper()
	require.Eventually(t, func() bool {
		r.mu.Lock()
		defer r.mu.Unlock()
		return r.disconnectHandlers == 1
	}, time.Second, time.Millisecond)
}

func (r *unsubscribeRecorder) get() []UnsubscribeEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]UnsubscribeEvent(nil), r.events...)
}

// requireSingle waits for one UnsubscribeHandler call, makes sure no second one
// follows and returns it.
func (r *unsubscribeRecorder) requireSingle(t *testing.T) UnsubscribeEvent {
	t.Helper()
	require.Eventually(t, func() bool { return len(r.get()) > 0 }, 10*time.Second, time.Millisecond)
	time.Sleep(20 * time.Millisecond)
	events := r.get()
	require.Len(t, events, 1, "UnsubscribeHandler must be called exactly once: %v", events)
	return events[0]
}

func requireAttemptEnded(t *testing.T, e UnsubscribeEvent, channel string, code uint32) {
	t.Helper()
	require.Equal(t, channel, e.Channel)
	require.False(t, e.Subscribed)
	require.False(t, e.ServerSide)
	require.Equal(t, code, e.Code)
}

// newAttemptTestClient returns a connected client whose SubscribeHandler is
// onSubscribe and whose UnsubscribeHandler calls are recorded.
func newAttemptTestClient(t *testing.T, node *Node, onSubscribe SubscribeHandler) (*Client, *unsubscribeRecorder) {
	rec := &unsubscribeRecorder{}
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(onSubscribe)
		client.OnUnsubscribe(rec.record)
		client.OnDisconnect(rec.recordDisconnect)
	})
	return newTestConnectedClientV2(t, node, "user1"), rec
}

func allowSubscribe(reply SubscribeReply) SubscribeHandler {
	return func(_ SubscribeEvent, cb SubscribeCallback) { cb(reply, nil) }
}

func TestSubscribeAttempt_RegularRefusedAfterAllow(t *testing.T) {
	t.Parallel()
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()
	client, rec := newAttemptTestClient(t, node, allowSubscribe(SubscribeReply{
		Options: SubscribeOptions{ExpireAt: time.Now().Unix() - 10},
	}))

	rw := testReplyWriterWrapper()
	err := client.handleSubscribe(&protocol.SubscribeRequest{Channel: "ch"}, &protocol.Command{Id: 1}, time.Now(), rw.rw)
	require.NoError(t, err)
	require.Len(t, rw.replies, 1)
	require.Equal(t, ErrorExpired.Code, rw.replies[0].Error.Code)

	requireAttemptEnded(t, rec.requireSingle(t), "ch", UnsubscribeCodeServer)
	require.Nil(t, rec.get()[0].Disconnect)
	require.False(t, client.IsSubscribed("ch"))
}

// UnsubscribeHandler for an attempt refused after SubscribeHandler allowed it is
// called asynchronously, possibly after the error reply. A new subscribe to the
// channel calls SubscribeHandler only after it, so an application keeping state
// by channel name sees the events in order.
func TestSubscribeAttempt_ResubscribeWaitsForAttemptEnd(t *testing.T) {
	t.Parallel()
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	var mu sync.Mutex
	var events []string
	record := func(e string) {
		mu.Lock()
		events = append(events, e)
		mu.Unlock()
	}
	release := make(chan struct{})
	var subscribes atomic.Int32
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			record("subscribe")
			reply := SubscribeReply{}
			if subscribes.Add(1) == 1 {
				reply.Options.ExpireAt = time.Now().Unix() - 10 // Refused by Centrifuge.
			}
			cb(reply, nil)
		})
		client.OnUnsubscribe(func(e UnsubscribeEvent) {
			<-release
			record("unsubscribe")
		})
	})
	client := newTestConnectedClientV2(t, node, "user1")

	rw := testReplyWriterWrapper()
	require.NoError(t, client.handleSubscribe(&protocol.SubscribeRequest{Channel: "ch"}, &protocol.Command{Id: 1}, time.Now(), rw.rw))
	require.Len(t, rw.replies, 1)
	require.Equal(t, ErrorExpired.Code, rw.replies[0].Error.Code)

	resubscribed := make(chan struct{})
	go func() {
		defer close(resubscribed)
		_ = client.handleSubscribe(&protocol.SubscribeRequest{Channel: "ch"}, &protocol.Command{Id: 2}, time.Now(), testReplyWriterWrapper().rw)
	}()
	time.Sleep(50 * time.Millisecond)
	mu.Lock()
	require.Equal(t, []string{"subscribe"}, events, "SubscribeHandler of the new attempt must wait for the old attempt's UnsubscribeHandler")
	mu.Unlock()
	close(release)
	<-resubscribed
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []string{"subscribe", "unsubscribe", "subscribe"}, events)
}

// A SubscribeHandler which holds a lock while invoking its SubscribeCallback,
// with an UnsubscribeHandler taking the same lock, does not deadlock when
// Centrifuge refuses the attempt inside the callback (regular subscribe with an
// expired subscription, map recovery from a stale epoch).
func TestSubscribeAttempt_RefusalInsideCallbackHoldingLock(t *testing.T) {
	t.Parallel()
	node, broker := newTestNodeWithMapBroker(t)
	setTestMapChannelOptionsConverging(node)
	_, err := broker.Publish(context.Background(), "map", "a", MapPublishOptions{Data: []byte(`{}`)})
	require.NoError(t, err)

	var appMu sync.Mutex
	unsubscribed := make(chan string, 2)
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			appMu.Lock()
			defer appMu.Unlock()
			reply := SubscribeReply{Options: SubscribeOptions{Type: e.Type}}
			if e.Type == SubscriptionTypeStream {
				reply.Options.ExpireAt = time.Now().Unix() - 10
			}
			cb(reply, nil)
		})
		client.OnUnsubscribe(func(e UnsubscribeEvent) {
			appMu.Lock()
			defer appMu.Unlock()
			unsubscribed <- e.Channel
		})
	})
	client := newTestConnectedClientV2(t, node, "user1")

	done := make(chan struct{})
	go func() {
		defer close(done)
		for _, req := range []*protocol.SubscribeRequest{
			{Channel: "ch"},
			{Channel: "map", Type: int32(SubscriptionTypeMap), Phase: MapPhaseLive, Recover: true, Offset: 1, Epoch: "stale"},
		} {
			rw := testReplyWriterWrapper()
			_ = client.handleSubscribe(req, &protocol.Command{Id: 1}, time.Now(), rw.rw)
			require.Len(t, rw.replies, 1)
			require.NotNil(t, rw.replies[0].Error)
		}
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("deadlock: UnsubscribeHandler called inside SubscribeCallback")
	}
	got := []string{<-unsubscribed, <-unsubscribed}
	require.ElementsMatch(t, []string{"ch", "map"}, got)
}

func TestSubscribeAttempt_AsyncCallbackAfterClose(t *testing.T) {
	t.Parallel()
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	cbCh := make(chan SubscribeCallback, 1)
	client, rec := newAttemptTestClient(t, node, func(_ SubscribeEvent, cb SubscribeCallback) { cbCh <- cb })

	rw := testReplyWriterWrapper()
	err := client.handleSubscribe(&protocol.SubscribeRequest{Channel: "ch"}, &protocol.Command{Id: 1}, time.Now(), rw.rw)
	require.NoError(t, err)
	cb := <-cbCh

	// close() waits for the subscribe in progress (the unsubscribe wait gate), so
	// run it concurrently and allow the subscribe once the client is closed.
	closeDone := closeConcurrently(t, client, DisconnectForceNoReconnect)
	cb(SubscribeReply{}, nil)
	<-closeDone

	e := rec.requireSingle(t)
	requireAttemptEnded(t, e, "ch", UnsubscribeCodeDisconnect)
	require.NotNil(t, e.Disconnect)
	require.Equal(t, DisconnectForceNoReconnect.Code, e.Disconnect.Code)
	rec.requireDisconnectLast(t)
	require.Empty(t, rw.replies)
}

// closeConcurrently starts close() and waits until the client is closed. The
// returned channel is closed once close() returns: a regular or shared poll
// subscribe in progress holds it (the unsubscribe wait gate) until the
// SubscribeCallback is invoked.
func closeConcurrently(t *testing.T, client *Client, disconnect Disconnect) chan struct{} {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = client.close(disconnect)
	}()
	require.Eventually(t, func() bool {
		client.mu.RLock()
		defer client.mu.RUnlock()
		return client.status == statusClosed
	}, time.Second, time.Millisecond)
	return done
}

// An unsubscribe which times out waiting for an in-flight subscribe closes the
// client, and close() drops the reservation before the subscribe was allowed.
// The late allow still ends the attempt, once.
func TestSubscribeAttempt_CallbackAfterWaitGateTimeout(t *testing.T) {
	t.Parallel()
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()
	cbCh := make(chan SubscribeCallback, 1)
	client, rec := newAttemptTestClient(t, node, func(_ SubscribeEvent, cb SubscribeCallback) { cbCh <- cb })

	rw := testReplyWriterWrapper()
	err := client.handleSubscribe(&protocol.SubscribeRequest{Channel: "ch"}, &protocol.Command{Id: 1}, time.Now(), rw.rw)
	require.NoError(t, err)
	cb := <-cbCh

	// The unsubscribe gives up after 5s and closes the client, which waits for
	// the callback.
	unsubRW := testReplyWriterWrapper()
	require.NoError(t, client.handleUnsubscribe(&protocol.UnsubscribeRequest{Channel: "ch"}, &protocol.Command{Id: 2}, time.Now(), unsubRW.rw))
	require.Eventually(t, func() bool {
		client.mu.RLock()
		defer client.mu.RUnlock()
		return client.status == statusClosed
	}, 5*time.Second, time.Millisecond)
	// close() doesn't wait for the SubscribeCallback: the attempt ends after
	// DisconnectHandler.
	rec.requireDisconnectedOnce(t)
	require.Empty(t, rec.get())

	cb(SubscribeReply{}, nil)
	e := rec.requireSingle(t)
	requireAttemptEnded(t, e, "ch", UnsubscribeCodeDisconnect)
	require.NotNil(t, e.Disconnect)
	require.Equal(t, DisconnectServerError.Code, e.Disconnect.Code)
}

func TestSubscribeAttempt_HandlerErrorNoUnsubscribe(t *testing.T) {
	t.Parallel()
	node, _ := newTestNodeWithMapBroker(t)
	client, rec := newAttemptTestClient(t, node, func(_ SubscribeEvent, cb SubscribeCallback) {
		cb(SubscribeReply{}, ErrorPermissionDenied)
	})

	rw := testReplyWriterWrapper()
	err := client.handleSubscribe(&protocol.SubscribeRequest{Channel: "ch"}, &protocol.Command{Id: 1}, time.Now(), rw.rw)
	require.NoError(t, err)
	require.Equal(t, ErrorPermissionDenied.Code, rw.replies[0].Error.Code)

	require.Equal(t, ErrorPermissionDenied.Code, subscribeMapClientExpectError(t, client, &protocol.SubscribeRequest{
		Channel: "map", Type: int32(SubscriptionTypeMap), Phase: MapPhaseState,
	}).Code)

	time.Sleep(20 * time.Millisecond)
	require.Empty(t, rec.get())
}

func TestSubscribeAttempt_SubscribedThenUnsubscribed(t *testing.T) {
	t.Parallel()
	node, _ := newTestNodeWithMapBroker(t)
	client, rec := newAttemptTestClient(t, node, func(e SubscribeEvent, cb SubscribeCallback) {
		cb(SubscribeReply{Options: SubscribeOptions{Type: e.Type}}, nil)
	})

	subscribeClientV2(t, client, "ch")
	res := subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: "map", Type: int32(SubscriptionTypeMap), Phase: MapPhaseState,
	})
	require.Equal(t, MapPhaseLive, res.Phase)
	require.Empty(t, rec.get())

	for _, ch := range []string{"ch", "map"} {
		rw := testReplyWriterWrapper()
		require.NoError(t, client.handleUnsubscribe(&protocol.UnsubscribeRequest{Channel: ch}, &protocol.Command{Id: 2}, time.Now(), rw.rw))
	}
	time.Sleep(20 * time.Millisecond)
	events := rec.get()
	require.Len(t, events, 2)
	for i, ch := range []string{"ch", "map"} {
		require.Equal(t, ch, events[i].Channel)
		require.True(t, events[i].Subscribed)
		require.Equal(t, UnsubscribeCodeClient, events[i].Code)
	}
}

func TestSubscribeAttempt_SharedPollRefusedAfterAllow(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t)
	client, rec := newAttemptTestClient(t, node, allowSubscribe(SubscribeReply{
		Options: SubscribeOptions{ExpireAt: time.Now().Unix() - 10},
	}))

	rw := testReplyWriterWrapper()
	err := client.handleSubscribe(&protocol.SubscribeRequest{
		Channel: "ch", Type: int32(SubscriptionTypeSharedPoll),
	}, &protocol.Command{Id: 1}, time.Now(), rw.rw)
	require.NoError(t, err)
	require.Len(t, rw.replies, 1)
	require.Equal(t, ErrorExpired.Code, rw.replies[0].Error.Code)

	requireAttemptEnded(t, rec.requireSingle(t), "ch", UnsubscribeCodeServer)
}

func TestSubscribeAttempt_MapRefusedInCallback(t *testing.T) {
	t.Parallel()
	node, _ := newTestNodeWithMapBroker(t)
	// The handler allows, but with a subscription type which does not match the request.
	client, rec := newAttemptTestClient(t, node, allowSubscribe(SubscribeReply{
		Options: SubscribeOptions{Type: SubscriptionTypeMapClients},
	}))

	protoErr := subscribeMapClientExpectError(t, client, &protocol.SubscribeRequest{
		Channel: "map", Type: int32(SubscriptionTypeMap), Phase: MapPhaseState,
	})
	require.Equal(t, ErrorBadRequest.Code, protoErr.Code)

	requireAttemptEnded(t, rec.requireSingle(t), "map", UnsubscribeCodeServer)
}

// startPaginatedMapSubscribe makes the first state page request of a map
// subscription to a channel with more entries than one page holds, so that the
// subscription stays loading.
func startPaginatedMapSubscribe(t *testing.T, client *Client, broker *MemoryMapBroker, channel string) *protocol.SubscribeResult {
	for i := 0; i < 5; i++ {
		_, err := broker.Publish(context.Background(), channel, string(rune('a'+i)), MapPublishOptions{Data: []byte(`{}`)})
		require.NoError(t, err)
	}
	res := subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: channel, Type: int32(SubscriptionTypeMap), Phase: MapPhaseState, Limit: 2,
	})
	require.Equal(t, MapPhaseState, res.Phase)
	require.NotEmpty(t, res.Cursor)
	return res
}

func TestSubscribeAttempt_MapClientUnsubscribeMidLoad(t *testing.T) {
	t.Parallel()
	node, broker := newTestNodeWithMapBroker(t)
	client, rec := newAttemptTestClient(t, node, allowSubscribe(SubscribeReply{
		Options: SubscribeOptions{Type: SubscriptionTypeMap},
	}))
	startPaginatedMapSubscribe(t, client, broker, "map")

	// Between pages nothing is in progress: the unsubscribe removes the loading
	// subscription right away and the client stays connected.
	rw := testReplyWriterWrapper()
	started := time.Now()
	require.NoError(t, client.handleUnsubscribe(&protocol.UnsubscribeRequest{Channel: "map"}, &protocol.Command{Id: 2}, time.Now(), rw.rw))
	require.Less(t, time.Since(started), time.Second)
	require.Len(t, rw.replies, 1)
	require.Nil(t, rw.replies[0].Error)

	e := rec.requireSingle(t)
	requireAttemptEnded(t, e, "map", UnsubscribeCodeClient)
	require.Nil(t, e.Disconnect)
	client.mu.RLock()
	defer client.mu.RUnlock()
	require.NotEqual(t, statusClosed, client.status)
	require.Empty(t, client.mapSubscribing)
}

func TestSubscribeAttempt_MapCatchUpTimeout(t *testing.T) {
	t.Parallel()
	node, broker := newTestNodeWithMapBroker(t)
	node.config.Map.GetMapChannelOptions = func(channel string) MapChannelOptions {
		return MapChannelOptions{
			Mode:                    MapModeEphemeral,
			KeyTTL:                  60 * time.Second,
			MinPageSize:             1,
			SubscribeCatchUpTimeout: time.Nanosecond,
		}
	}
	client, rec := newAttemptTestClient(t, node, allowSubscribe(SubscribeReply{
		Options: SubscribeOptions{Type: SubscriptionTypeMap},
	}))
	res := startPaginatedMapSubscribe(t, client, broker, "map")

	rw := testReplyWriterWrapper()
	err := client.handleSubscribe(&protocol.SubscribeRequest{
		Channel: "map", Type: int32(SubscriptionTypeMap), Phase: MapPhaseState, Limit: 2, Cursor: res.Cursor,
	}, &protocol.Command{Id: 2}, time.Now(), rw.rw)
	require.ErrorIs(t, err, DisconnectSlow)

	requireAttemptEnded(t, rec.requireSingle(t), "map", UnsubscribeCodeServer)
}

func TestSubscribeAttempt_MapSweptOnOtherSubscribe(t *testing.T) {
	t.Parallel()
	node, broker := newTestNodeWithMapBroker(t)
	node.config.Map.GetMapChannelOptions = func(channel string) MapChannelOptions {
		return MapChannelOptions{
			Mode:                    MapModeEphemeral,
			KeyTTL:                  60 * time.Second,
			MinPageSize:             1,
			SubscribeCatchUpTimeout: time.Nanosecond,
		}
	}
	client, rec := newAttemptTestClient(t, node, allowSubscribe(SubscribeReply{
		Options: SubscribeOptions{Type: SubscriptionTypeMap},
	}))
	startPaginatedMapSubscribe(t, client, broker, "map")

	// A subscribe to another channel sweeps the expired catch-up.
	res := subscribeMapClient(t, client, &protocol.SubscribeRequest{
		Channel: "other", Type: int32(SubscriptionTypeMap), Phase: MapPhaseState,
	})
	require.Equal(t, MapPhaseLive, res.Phase)

	requireAttemptEnded(t, rec.requireSingle(t), "map", UnsubscribeCodeServer)
}

func TestSubscribeAttempt_MapFailedPhase(t *testing.T) {
	t.Parallel()
	node, broker := newTestNodeWithMapBroker(t)
	setTestMapChannelOptionsConverging(node)
	client, rec := newAttemptTestClient(t, node, allowSubscribe(SubscribeReply{
		Options: SubscribeOptions{Type: SubscriptionTypeMap},
	}))
	res := startPaginatedMapSubscribe(t, client, broker, "map")

	// Stream phase with an epoch which does not match the state phase one.
	protoErr := subscribeMapClientExpectError(t, client, &protocol.SubscribeRequest{
		Channel: "map", Type: int32(SubscriptionTypeMap), Phase: MapPhaseStream, Offset: res.Offset, Epoch: "wrong",
	})
	require.Equal(t, ErrorUnrecoverablePosition.Code, protoErr.Code)

	requireAttemptEnded(t, rec.requireSingle(t), "map", UnsubscribeCodeServer)
}

func TestSubscribeAttempt_MapCloseDuringLoading(t *testing.T) {
	t.Parallel()
	node, broker := newTestNodeWithMapBroker(t)
	client, rec := newAttemptTestClient(t, node, allowSubscribe(SubscribeReply{
		Options: SubscribeOptions{Type: SubscriptionTypeMap},
	}))
	startPaginatedMapSubscribe(t, client, broker, "map")

	require.NoError(t, client.close(DisconnectForceNoReconnect))

	e := rec.requireSingle(t)
	requireAttemptEnded(t, e, "map", UnsubscribeCodeDisconnect)
	require.NotNil(t, e.Disconnect)
	require.Equal(t, DisconnectForceNoReconnect.Code, e.Disconnect.Code)
	rec.requireDisconnectLast(t)
}

// requireNoMapSubscribing makes sure the client holds no loading map reservation.
func requireNoMapSubscribing(t *testing.T, client *Client) {
	t.Helper()
	client.mu.RLock()
	defer client.mu.RUnlock()
	require.Empty(t, client.mapSubscribing)
}

// mapCallbackAfterClose makes a map subscribe request with an asynchronous
// SubscribeHandler, closes the client and then allows the subscribe. A client
// can do this on purpose: send the request and disconnect before the handler
// (a proxy, say) answers.
func mapCallbackAfterClose(t *testing.T, node *Node, req *protocol.SubscribeRequest) (*Client, *unsubscribeRecorder) {
	t.Helper()
	cbCh := make(chan SubscribeCallback, 1)
	client, rec := newAttemptTestClient(t, node, func(_ SubscribeEvent, cb SubscribeCallback) { cbCh <- cb })
	rw := testReplyWriterWrapper()
	require.NoError(t, client.handleSubscribe(req, &protocol.Command{Id: 1}, time.Now(), rw.rw))
	cb := <-cbCh
	closeDone := closeConcurrently(t, client, DisconnectForceNoReconnect)
	// close() waits for the map subscribe in SubscribeHandler.
	select {
	case <-closeDone:
		t.Fatal("close() must wait for the map subscribe in SubscribeHandler")
	case <-time.After(100 * time.Millisecond):
	}
	cb(SubscribeReply{Options: SubscribeOptions{Type: SubscriptionType(req.Type)}}, nil)
	<-closeDone
	rec.requireDisconnectLast(t)
	require.Empty(t, rw.replies)
	return client, rec
}

func requireEndedByClose(t *testing.T, rec *unsubscribeRecorder, channel string) {
	t.Helper()
	e := rec.requireSingle(t)
	requireAttemptEnded(t, e, channel, UnsubscribeCodeDisconnect)
	require.NotNil(t, e.Disconnect)
	require.Equal(t, DisconnectForceNoReconnect.Code, e.Disconnect.Code)
}

// A paginated state load allowed after close must not reserve: nothing would
// remove the reservation or end the attempt.
func TestSubscribeAttempt_MapStateCallbackAfterClose(t *testing.T) {
	t.Parallel()
	node, broker := newTestNodeWithMapBroker(t)
	for i := 0; i < 5; i++ {
		_, err := broker.Publish(context.Background(), "map", string(rune('a'+i)), MapPublishOptions{Data: []byte(`{}`)})
		require.NoError(t, err)
	}
	client, rec := mapCallbackAfterClose(t, node, &protocol.SubscribeRequest{
		Channel: "map", Type: int32(SubscriptionTypeMap), Phase: MapPhaseState, Limit: 2,
	})
	requireEndedByClose(t, rec, "map")
	requireNoMapSubscribing(t, client)
}

// Same for a paginated stream recovery.
func TestSubscribeAttempt_MapStreamRecoveryCallbackAfterClose(t *testing.T) {
	t.Parallel()
	node, broker := newTestNodeWithMapBroker(t)
	setTestMapChannelOptionsConverging(node)
	var pos StreamPosition
	for i := 0; i < 5; i++ {
		res, err := broker.Publish(context.Background(), "map", string(rune('a'+i)), MapPublishOptions{Data: []byte(`{}`)})
		require.NoError(t, err)
		pos = res.Position
	}
	client, rec := mapCallbackAfterClose(t, node, &protocol.SubscribeRequest{
		Channel: "map", Type: int32(SubscriptionTypeMap), Phase: MapPhaseStream, Recover: true,
		Offset: 1, Epoch: pos.Epoch, Limit: 1,
	})
	requireEndedByClose(t, rec, "map")
	requireNoMapSubscribing(t, client)
}

// Same for a direct-to-live recovery join.
func TestSubscribeAttempt_MapLiveRecoveryCallbackAfterClose(t *testing.T) {
	t.Parallel()
	node, broker := newTestNodeWithMapBroker(t)
	setTestMapChannelOptionsConverging(node)
	res, err := broker.Publish(context.Background(), "map", "a", MapPublishOptions{Data: []byte(`{}`)})
	require.NoError(t, err)
	client, rec := mapCallbackAfterClose(t, node, &protocol.SubscribeRequest{
		Channel: "map", Type: int32(SubscriptionTypeMap), Phase: MapPhaseLive, Recover: true,
		Offset: res.Position.Offset, Epoch: res.Position.Epoch,
	})
	requireEndedByClose(t, rec, "map")
	requireNoMapSubscribing(t, client)
	client.mu.RLock()
	defer client.mu.RUnlock()
	require.Empty(t, client.channels)
}

// A callback which denies after close ends nothing, and close() completes.
func TestSubscribeAttempt_DeniedCallbackAfterClose(t *testing.T) {
	t.Parallel()
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()
	cbCh := make(chan SubscribeCallback, 1)
	client, rec := newAttemptTestClient(t, node, func(_ SubscribeEvent, cb SubscribeCallback) { cbCh <- cb })

	rw := testReplyWriterWrapper()
	require.NoError(t, client.handleSubscribe(&protocol.SubscribeRequest{Channel: "ch"}, &protocol.Command{Id: 1}, time.Now(), rw.rw))
	cb := <-cbCh
	closeDone := closeConcurrently(t, client, DisconnectForceNoReconnect)
	cb(SubscribeReply{}, ErrorPermissionDenied)
	<-closeDone

	require.Empty(t, rec.get())
	rec.requireDisconnectedOnce(t)
	client.mu.RLock()
	defer client.mu.RUnlock()
	require.Empty(t, client.channels)
	require.Zero(t, client.pendingUnsubscribes)
}

// Shared poll: a callback allowing after close ends the attempt before
// DisconnectHandler.
func TestSubscribeAttempt_SharedPollCallbackAfterClose(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t)
	cbCh := make(chan SubscribeCallback, 1)
	client, rec := newAttemptTestClient(t, node, func(_ SubscribeEvent, cb SubscribeCallback) { cbCh <- cb })

	rw := testReplyWriterWrapper()
	require.NoError(t, client.handleSubscribe(&protocol.SubscribeRequest{
		Channel: "ch", Type: int32(SubscriptionTypeSharedPoll),
	}, &protocol.Command{Id: 1}, time.Now(), rw.rw))
	cb := <-cbCh
	closeDone := closeConcurrently(t, client, DisconnectForceNoReconnect)
	cb(SubscribeReply{}, nil)
	<-closeDone

	requireAttemptEnded(t, rec.requireSingle(t), "ch", UnsubscribeCodeDisconnect)
	rec.requireDisconnectLast(t)
	require.Empty(t, rw.replies)
	client.mu.RLock()
	defer client.mu.RUnlock()
	require.Empty(t, client.channels)
}

// A SubscribeCallback invoked twice (an application bug) counts once: close()
// does not hang on it and the attempt is not ended twice.
func TestSubscribeAttempt_CallbackInvokedTwice(t *testing.T) {
	t.Parallel()
	node, _ := newTestNodeWithMapBroker(t)
	client, rec := newAttemptTestClient(t, node, func(e SubscribeEvent, cb SubscribeCallback) {
		reply := SubscribeReply{Options: SubscribeOptions{Type: e.Type, ExpireAt: time.Now().Unix() - 10}}
		cb(reply, nil)
		cb(reply, nil)
	})

	for _, req := range []*protocol.SubscribeRequest{
		{Channel: "ch"},
		{Channel: "map", Type: int32(SubscriptionTypeMap), Phase: MapPhaseState},
	} {
		rw := testReplyWriterWrapper()
		require.NoError(t, client.handleSubscribe(req, &protocol.Command{Id: 1}, time.Now(), rw.rw))
		require.Len(t, rw.replies, 1)
	}
	require.Eventually(t, func() bool { return len(rec.get()) == 2 }, time.Second, time.Millisecond)

	done := make(chan struct{})
	go func() { _ = client.close(DisconnectForceNoReconnect); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("close() hangs")
	}
	require.Len(t, rec.get(), 2)
	rec.requireDisconnectedOnce(t)
}

// A subscribe handled on a closed client does not call SubscribeHandler: close()
// may be past waiting for it.
func TestSubscribeAttempt_NoHandlerCallOnClosedClient(t *testing.T) {
	t.Parallel()
	mapNode, _ := newTestNodeWithMapBroker(t)
	for _, tc := range []struct {
		node *Node
		req  *protocol.SubscribeRequest
	}{
		{mapNode, &protocol.SubscribeRequest{Channel: "ch"}},
		{mapNode, &protocol.SubscribeRequest{Channel: "map", Type: int32(SubscriptionTypeMap), Phase: MapPhaseState}},
		{newTestNodeWithSharedPoll(t), &protocol.SubscribeRequest{Channel: "poll", Type: int32(SubscriptionTypeSharedPoll)}},
	} {
		client, rec := newAttemptTestClient(t, tc.node, func(SubscribeEvent, SubscribeCallback) {
			t.Error("SubscribeHandler called on a closed client")
		})
		require.NoError(t, client.close(DisconnectForceNoReconnect))
		rw := testReplyWriterWrapper()
		err := client.handleSubscribe(tc.req, &protocol.Command{Id: 1}, time.Now(), rw.rw)
		require.ErrorIs(t, err, DisconnectConnectionClosed, tc.req.Channel)
		require.Empty(t, rec.get())
		client.mu.RLock()
		require.Empty(t, client.channels)
		require.Zero(t, client.pendingUnsubscribes)
		client.mu.RUnlock()
	}
}

// close() gives up waiting for a subscribe stalled inside Centrifuge (here a
// history read) after 5s. When the subscribe finishes, its commit finds the
// client closed, rolls back and ends the allowed attempt, after
// DisconnectHandler.
func TestSubscribeAttempt_CloseWithStalledSubscribe(t *testing.T) {
	t.Parallel()
	node, err := New(Config{LogLevel: LogLevelTrace, LogHandler: func(LogEntry) {}})
	require.NoError(t, err)
	memBroker, err := NewMemoryBroker(node, MemoryBrokerConfig{})
	require.NoError(t, err)
	historyStarted, releaseHistory := make(chan struct{}), make(chan struct{})
	node.SetBroker(&slowHistoryBroker{startPublishingCh: historyStarted, stopPublishingCh: releaseHistory, MemoryBroker: memBroker})
	require.NoError(t, node.Run())
	defer func() { _ = node.Shutdown(context.Background()) }()
	client, rec := newAttemptTestClient(t, node, allowSubscribe(SubscribeReply{
		Options: SubscribeOptions{EnableRecovery: true},
	}))

	subscribeDone := make(chan struct{})
	rw := testReplyWriterWrapper()
	go func() {
		defer close(subscribeDone)
		_ = client.handleSubscribe(&protocol.SubscribeRequest{Channel: "ch", Recover: true}, &protocol.Command{Id: 1}, time.Now(), rw.rw)
	}()
	<-historyStarted

	// One subscribe in progress is waited for up to 5 seconds.
	started := time.Now()
	require.NoError(t, client.close(DisconnectForceNoReconnect))
	require.GreaterOrEqual(t, time.Since(started), subscribeInProgressTimeout.get())
	require.Less(t, time.Since(started), subscribeInProgressTimeout.get()+2*time.Second)
	rec.requireDisconnectedOnce(t)
	require.Empty(t, rec.get())

	close(releaseHistory)
	<-subscribeDone
	e := rec.requireSingle(t)
	requireAttemptEnded(t, e, "ch", UnsubscribeCodeDisconnect)
	require.Zero(t, node.hub.NumSubscribers("ch"))
	client.mu.RLock()
	defer client.mu.RUnlock()
	require.Empty(t, client.channels)
}

// newStalledHistoryClient returns a client whose subscribes with recovery stall
// in the history read until releaseHistory is closed.
func newStalledHistoryClient(t *testing.T) (client *Client, rec *unsubscribeRecorder, historyStarted, releaseHistory chan struct{}) {
	node, err := New(Config{LogLevel: LogLevelTrace, LogHandler: func(LogEntry) {}})
	require.NoError(t, err)
	memBroker, err := NewMemoryBroker(node, MemoryBrokerConfig{})
	require.NoError(t, err)
	historyStarted, releaseHistory = make(chan struct{}), make(chan struct{})
	node.SetBroker(&slowHistoryBroker{startPublishingCh: historyStarted, stopPublishingCh: releaseHistory, MemoryBroker: memBroker})
	require.NoError(t, node.Run())
	t.Cleanup(func() { _ = node.Shutdown(context.Background()) })
	client, rec = newAttemptTestClient(t, node, allowSubscribe(SubscribeReply{
		Options: SubscribeOptions{EnableRecovery: true},
	}))
	return client, rec, historyStarted, releaseHistory
}

// close() during the history read of an allowed subscribe: close waits for it,
// the commit finds the client closed and ends the attempt.
func TestSubscribeAttempt_CloseDuringHistoryRead(t *testing.T) {
	t.Parallel()
	client, rec, historyStarted, releaseHistory := newStalledHistoryClient(t)

	rw := testReplyWriterWrapper()
	go func() {
		_ = client.handleSubscribe(&protocol.SubscribeRequest{Channel: "ch", Recover: true}, &protocol.Command{Id: 1}, time.Now(), rw.rw)
	}()
	<-historyStarted
	closeDone := make(chan struct{})
	go func() { _ = client.close(DisconnectForceNoReconnect); close(closeDone) }()
	require.Eventually(t, func() bool {
		client.mu.RLock()
		defer client.mu.RUnlock()
		return client.status == statusClosed
	}, time.Second, time.Millisecond)
	time.Sleep(20 * time.Millisecond)
	close(releaseHistory)
	<-closeDone

	e := rec.requireSingle(t)
	requireAttemptEnded(t, e, "ch", UnsubscribeCodeDisconnect)
	require.Equal(t, DisconnectForceNoReconnect.Code, e.Disconnect.Code)
	rec.requireDisconnectedOnce(t)
	require.Zero(t, client.node.hub.NumSubscribers("ch"))
}

// A client unsubscribe gives up waiting for a subscribe stalled in its history
// read and closes the client; close() removes the stalled reservation and ends
// the attempt.
func TestSubscribeAttempt_UnsubscribeTimeoutDuringHistoryRead(t *testing.T) {
	t.Parallel()
	client, rec, historyStarted, releaseHistory := newStalledHistoryClient(t)
	defer close(releaseHistory)

	rw := testReplyWriterWrapper()
	go func() {
		_ = client.handleSubscribe(&protocol.SubscribeRequest{Channel: "ch", Recover: true}, &protocol.Command{Id: 1}, time.Now(), rw.rw)
	}()
	<-historyStarted
	unsubRW := testReplyWriterWrapper()
	require.NoError(t, client.handleUnsubscribe(&protocol.UnsubscribeRequest{Channel: "ch"}, &protocol.Command{Id: 2}, time.Now(), unsubRW.rw))

	e := rec.requireSingle(t)
	requireAttemptEnded(t, e, "ch", UnsubscribeCodeDisconnect)
	require.Equal(t, DisconnectServerError.Code, e.Disconnect.Code)
	require.Eventually(t, func() bool {
		rec.mu.Lock()
		defer rec.mu.Unlock()
		return rec.disconnectHandlers == 1
	}, time.Second, time.Millisecond)
	rec.requireDisconnectedOnce(t)
}

// Shared poll: the channel options disappear after the subscription was
// installed. The subscribe fails and the attempt ends.
func TestSubscribeAttempt_SharedPollOptionsGoneAfterInstall(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t)
	var gone atomic.Bool
	getOptions := node.config.SharedPoll.GetSharedPollChannelOptions
	node.config.SharedPoll.GetSharedPollChannelOptions = func(channel string) (SharedPollChannelOptions, bool) {
		opts, ok := getOptions(channel)
		return opts, ok && !gone.Load()
	}
	client, rec := newAttemptTestClient(t, node, func(_ SubscribeEvent, cb SubscribeCallback) {
		gone.Store(true)
		cb(SubscribeReply{}, nil)
	})

	rw := testReplyWriterWrapper()
	require.NoError(t, client.handleSubscribe(&protocol.SubscribeRequest{
		Channel: "ch", Type: int32(SubscriptionTypeSharedPoll),
	}, &protocol.Command{Id: 1}, time.Now(), rw.rw))
	require.Len(t, rw.replies, 1)
	require.Equal(t, ErrorNotAvailable.Code, rw.replies[0].Error.Code)
	requireAttemptEnded(t, rec.requireSingle(t), "ch", UnsubscribeCodeServer)
	client.mu.RLock()
	defer client.mu.RUnlock()
	require.Empty(t, client.channels)
}

// close() starts after an allowed map callback passed its closed check but
// before the attempt reserved (here: while GetMapChannelOptions runs). The
// attempt must not reserve on the closed client, which nothing would clean up.
func TestSubscribeAttempt_MapCloseBeforeReserve(t *testing.T) {
	t.Parallel()
	node, broker := newTestNodeWithMapBroker(t)
	var block atomic.Bool
	entered, release := make(chan struct{}), make(chan struct{})
	getOptions := node.config.Map.GetMapChannelOptions
	node.config.Map.GetMapChannelOptions = func(channel string) MapChannelOptions {
		if block.CompareAndSwap(true, false) {
			close(entered)
			<-release
		}
		return getOptions(channel)
	}
	for i := 0; i < 5; i++ {
		_, err := broker.Publish(context.Background(), "map", string(rune('a'+i)), MapPublishOptions{Data: []byte(`{}`)})
		require.NoError(t, err)
	}
	client, rec := newAttemptTestClient(t, node, func(_ SubscribeEvent, cb SubscribeCallback) {
		go func() {
			block.Store(true)
			cb(SubscribeReply{Options: SubscribeOptions{Type: SubscriptionTypeMap}}, nil)
		}()
	})

	rw := testReplyWriterWrapper()
	require.NoError(t, client.handleSubscribe(&protocol.SubscribeRequest{
		Channel: "map", Type: int32(SubscriptionTypeMap), Phase: MapPhaseState, Limit: 2,
	}, &protocol.Command{Id: 1}, time.Now(), rw.rw))
	<-entered
	closeDone := closeConcurrently(t, client, DisconnectForceNoReconnect)
	close(release)
	<-closeDone

	e := rec.requireSingle(t)
	requireAttemptEnded(t, e, "map", UnsubscribeCodeDisconnect)
	rec.requireDisconnectedOnce(t)
	requireNoMapSubscribing(t, client)
}

// While SubscribeHandler decides a map subscribe, the channel holds no
// reservation, but another subscribe to it (of any type) is rejected without
// calling SubscribeHandler: two attempts on one channel would end with
// UnsubscribeHandler calls the application can't tell apart.
func TestSubscribeAttempt_MapPendingRejectsOtherSubscribes(t *testing.T) {
	t.Parallel()
	for _, first := range []*protocol.SubscribeRequest{
		{Channel: "map", Type: int32(SubscriptionTypeMap), Phase: MapPhaseState},
		{Channel: "map", Type: int32(SubscriptionTypeMap), Phase: 100}, // Fails after the handler.
	} {
		node, _ := newTestNodeWithMapBroker(t)
		var handlerCalls atomic.Int32
		cbCh := make(chan SubscribeCallback, 2)
		client, rec := newAttemptTestClient(t, node, func(_ SubscribeEvent, cb SubscribeCallback) {
			handlerCalls.Add(1)
			cbCh <- cb
		})

		rw := testReplyWriterWrapper()
		require.NoError(t, client.handleSubscribe(first, &protocol.Command{Id: 1}, time.Now(), rw.rw))
		cb := <-cbCh

		for _, second := range []*protocol.SubscribeRequest{
			{Channel: "map", Type: int32(SubscriptionTypeMap), Phase: MapPhaseState},
			{Channel: "map", Type: int32(SubscriptionTypeMap), Phase: MapPhaseLive, Recover: true},
			{Channel: "map", Type: int32(SubscriptionTypeMap), Phase: MapPhaseStream, Recover: true},
			{Channel: "map"},
		} {
			err := client.handleSubscribe(second, &protocol.Command{Id: 2}, time.Now(), testReplyWriterWrapper().rw)
			require.ErrorIs(t, err, ErrorAlreadySubscribed)
		}
		require.ErrorIs(t, client.Subscribe("map"), ErrorAlreadySubscribed)
		require.Equal(t, int32(1), handlerCalls.Load())

		cb(SubscribeReply{Options: SubscribeOptions{Type: SubscriptionTypeMap}}, nil)
		require.Len(t, rw.replies, 1)
		require.NoError(t, client.close(DisconnectForceNoReconnect))
		e := rec.requireSingle(t)
		require.Equal(t, first.Phase == MapPhaseState, e.Subscribed)
		rec.requireDisconnectedOnce(t)
		client.mu.RLock()
		require.Empty(t, client.tracking.mapSubscribePending)
		client.mu.RUnlock()
	}
}

// A shared poll channel is checked against pending map subscribes too.
func TestSubscribeAttempt_MapPendingRejectsSharedPoll(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t)
	client, _ := newAttemptTestClient(t, node, allowSubscribe(SubscribeReply{}))
	client.mu.Lock()
	client.trackingLocked().mapSubscribePending = channelCounts{"ch"}
	client.mu.Unlock()
	err := client.handleSubscribe(&protocol.SubscribeRequest{
		Channel: "ch", Type: int32(SubscriptionTypeSharedPoll),
	}, &protocol.Command{Id: 1}, time.Now(), testReplyWriterWrapper().rw)
	require.ErrorIs(t, err, ErrorAlreadySubscribed)
	client.mu.Lock()
	client.tracking.mapSubscribePending = nil // Not a real attempt: close() must not wait for it.
	client.mu.Unlock()
}

// An UnsubscribeHandler which unsubscribes the client again (from the same
// channel, as Node.Unsubscribe of all user connections would, or from one it is
// not subscribed to) does not wait for its own unsubscribe to finish.
func TestSubscribeAttempt_NestedUnsubscribeDoesNotWait(t *testing.T) {
	t.Parallel()
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()
	var client *Client
	nestedTook := make(chan time.Duration, 1)
	node.OnConnect(func(c *Client) {
		c.OnSubscribe(allowSubscribe(SubscribeReply{}))
		c.OnUnsubscribe(func(e UnsubscribeEvent) {
			if e.Channel == "ch" {
				started := time.Now()
				c.Unsubscribe("ch")
				c.Unsubscribe("dependent")
				nestedTook <- time.Since(started)
			}
		})
	})
	client = newTestConnectedClientV2(t, node, "user1")
	subscribeClientV2(t, client, "ch")
	client.Unsubscribe("ch")
	require.Less(t, <-nestedTook, time.Second)
}

// Map subscribes whose SubscribeHandler still decides count against
// ClientChannelLimit: pipelined first requests can't exceed it.
func TestSubscribeAttempt_PendingMapSubscribesCountAgainstChannelLimit(t *testing.T) {
	t.Parallel()
	node, _ := newTestNodeWithMapBroker(t)
	node.config.ClientChannelLimit = 2
	cbCh := make(chan SubscribeCallback, 3)
	client, _ := newAttemptTestClient(t, node, func(_ SubscribeEvent, cb SubscribeCallback) { cbCh <- cb })

	for i, ch := range []string{"map:0", "map:1"} {
		require.NoError(t, client.handleSubscribe(&protocol.SubscribeRequest{
			Channel: ch, Type: int32(SubscriptionTypeMap), Phase: MapPhaseState,
		}, &protocol.Command{Id: uint32(i + 1)}, time.Now(), testReplyWriterWrapper().rw))
	}
	for _, req := range []*protocol.SubscribeRequest{
		{Channel: "map:2", Type: int32(SubscriptionTypeMap), Phase: MapPhaseState},
		{Channel: "ch"},
	} {
		err := client.handleSubscribe(req, &protocol.Command{Id: 3}, time.Now(), testReplyWriterWrapper().rw)
		require.ErrorIs(t, err, ErrorLimitExceeded, req.Channel)
	}
	require.Len(t, cbCh, 2)
	for i := 0; i < 2; i++ {
		(<-cbCh)(SubscribeReply{Options: SubscribeOptions{Type: SubscriptionTypeMap}}, nil)
	}
	client.mu.RLock()
	defer client.mu.RUnlock()
	require.Empty(t, client.tracking.mapSubscribePending)
	require.Len(t, client.channels, 2)
}

// A client unsubscribe which removes its channel itself does not wait for
// concurrent unsubscribes of other channels.
func TestSubscribeAttempt_ClientUnsubscribeDoesNotWaitForOtherChannels(t *testing.T) {
	t.Parallel()
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()
	entered, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(allowSubscribe(SubscribeReply{}))
		client.OnUnsubscribe(func(e UnsubscribeEvent) {
			if e.Channel == "b" {
				close(entered)
				<-release
			}
		})
	})
	client := newTestConnectedClientV2(t, node, "user1")
	subscribeClientV2(t, client, "a")
	subscribeClientV2(t, client, "b")

	go client.Unsubscribe("b")
	<-entered
	started := time.Now()
	require.NoError(t, client.handleUnsubscribe(&protocol.UnsubscribeRequest{Channel: "a"}, &protocol.Command{Id: 3}, time.Now(), testReplyWriterWrapper().rw))
	require.Less(t, time.Since(started), time.Second)
}

// The pending map marker is cleared before the error reply: a client subscribing
// again as soon as it got the reply is not rejected as already subscribed.
func TestSubscribeAttempt_MapResubscribeRightAfterErrorReply(t *testing.T) {
	t.Parallel()
	node, _ := newTestNodeWithMapBroker(t)
	var calls atomic.Int32
	client, _ := newAttemptTestClient(t, node, func(e SubscribeEvent, cb SubscribeCallback) {
		reply := SubscribeReply{Options: SubscribeOptions{Type: SubscriptionTypeMap}}
		if calls.Add(1) == 1 {
			reply.Options.Type = SubscriptionTypeMapClients // Refused by Centrifuge.
		}
		go cb(reply, nil)
	})
	req := &protocol.SubscribeRequest{Channel: "map", Type: int32(SubscriptionTypeMap), Phase: MapPhaseState}
	resubscribe := make(chan error, 1)
	second := testReplyWriterWrapper()
	rw := &replyWriter{write: func(rep *protocol.Reply) {
		require.NotNil(t, rep.Error)
		resubscribe <- client.handleSubscribe(req, &protocol.Command{Id: 2}, time.Now(), second.rw)
	}}
	require.NoError(t, client.handleSubscribe(req, &protocol.Command{Id: 1}, time.Now(), rw))
	require.NoError(t, <-resubscribe)
	require.Eventually(t, func() bool { return calls.Load() == 2 }, time.Second, time.Millisecond)
}

// A client unsubscribing from a map channel while its SubscribeHandler decides,
// then subscribing again (as an SDK does on unsubscribe() + subscribe()): the
// unsubscribe waits for the attempt and removes what it became, and the new
// subscribe works.
func TestSubscribeAttempt_MapUnsubscribeResubscribeWhileHandlerPending(t *testing.T) {
	t.Parallel()
	node, _ := newTestNodeWithMapBroker(t)
	cbCh := make(chan SubscribeCallback, 2)
	client, rec := newAttemptTestClient(t, node, func(_ SubscribeEvent, cb SubscribeCallback) { cbCh <- cb })
	req := &protocol.SubscribeRequest{Channel: "map", Type: int32(SubscriptionTypeMap), Phase: MapPhaseState}
	reply := SubscribeReply{Options: SubscribeOptions{Type: SubscriptionTypeMap}}

	require.NoError(t, client.handleSubscribe(req, &protocol.Command{Id: 1}, time.Now(), testReplyWriterWrapper().rw))
	cb := <-cbCh

	unsubscribed := make(chan struct{})
	go func() {
		defer close(unsubscribed)
		require.NoError(t, client.handleUnsubscribe(&protocol.UnsubscribeRequest{Channel: "map"}, &protocol.Command{Id: 2}, time.Now(), testReplyWriterWrapper().rw))
	}()
	select {
	case <-unsubscribed:
		t.Fatal("unsubscribe must wait for the map subscribe in SubscribeHandler")
	case <-time.After(50 * time.Millisecond):
	}
	cb(reply, nil)
	<-unsubscribed
	e := rec.requireSingle(t)
	require.Equal(t, UnsubscribeCodeClient, e.Code)

	rw := testReplyWriterWrapper()
	require.NoError(t, client.handleSubscribe(req, &protocol.Command{Id: 3}, time.Now(), rw.rw))
	(<-cbCh)(reply, nil)
	require.Len(t, rw.replies, 1)
	require.Nil(t, rw.replies[0].Error)
	require.True(t, client.IsSubscribed("map"))
}

// A client making attempts which Centrifuge refuses after SubscribeHandler
// allowed them, on many channels, can't pile up UnsubscribeHandler calls: its
// subscribes wait once maxPendingAttemptEnds are in progress.
func TestSubscribeAttempt_AttemptEndsBackpressure(t *testing.T) {
	t.Parallel()
	node, _ := newTestNodeWithMapBroker(t)
	var running, maxRunning atomic.Int32
	var ended atomic.Int32
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{Options: SubscribeOptions{Type: e.Type}}, nil)
		})
		client.OnUnsubscribe(func(e UnsubscribeEvent) {
			n := running.Add(1)
			for {
				m := maxRunning.Load()
				if n <= m || maxRunning.CompareAndSwap(m, n) {
					break
				}
			}
			time.Sleep(2 * time.Millisecond)
			running.Add(-1)
			ended.Add(1)
		})
	})
	client := newTestConnectedClientV2(t, node, "user1")
	for i := 0; i < 50; i++ {
		// An invalid phase is refused after SubscribeHandler.
		_ = client.handleSubscribe(&protocol.SubscribeRequest{
			Channel: "map:" + strconv.Itoa(i), Type: int32(SubscriptionTypeMap), Phase: 100,
		}, &protocol.Command{Id: uint32(i + 1)}, time.Now(), testReplyWriterWrapper().rw)
	}
	require.Eventually(t, func() bool { return ended.Load() == 50 }, 5*time.Second, time.Millisecond)
	require.LessOrEqual(t, maxRunning.Load(), int32(maxPendingAttemptEnds))
}

// A concurrent server unsubscribe which removed a channel before close() looked
// at it calls UnsubscribeHandler before DisconnectHandler.
func TestSubscribeAttempt_DisconnectAfterConcurrentUnsubscribe(t *testing.T) {
	t.Parallel()
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()
	entered, release := make(chan struct{}), make(chan struct{})
	rec := &unsubscribeRecorder{}
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(allowSubscribe(SubscribeReply{}))
		client.OnUnsubscribe(func(e UnsubscribeEvent) {
			close(entered)
			<-release
			rec.record(e)
		})
		client.OnDisconnect(rec.recordDisconnect)
	})
	client := newTestConnectedClientV2(t, node, "user1")
	subscribeClientV2(t, client, "ch")

	go client.Unsubscribe("ch")
	<-entered
	closeDone := make(chan struct{})
	go func() { _ = client.close(DisconnectForceNoReconnect); close(closeDone) }()
	select {
	case <-closeDone:
		t.Fatal("close() returned while UnsubscribeHandler was still running")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	<-closeDone

	e := rec.requireSingle(t)
	require.True(t, e.Subscribed)
	rec.requireDisconnectLast(t)
}

// close() waits for all subscribes in progress (regular, shared poll and map)
// within one budget, not for each of them in turn.
func TestSubscribeAttempt_CloseWaitsForSubscribesWithinOneBudget(t *testing.T) {
	t.Parallel()
	node, _ := newTestNodeWithMapBroker(t)
	cbCh := make(chan SubscribeCallback, 4)
	client, rec := newAttemptTestClient(t, node, func(_ SubscribeEvent, cb SubscribeCallback) { cbCh <- cb })
	// Three regular ones: waiting 5 seconds for each in turn would take longer
	// than the budget.
	for _, req := range []*protocol.SubscribeRequest{
		{Channel: "a"},
		{Channel: "b"},
		{Channel: "c"},
		{Channel: "map", Type: int32(SubscriptionTypeMap), Phase: MapPhaseState},
	} {
		require.NoError(t, client.handleSubscribe(req, &protocol.Command{Id: 1}, time.Now(), testReplyWriterWrapper().rw))
	}
	require.Len(t, cbCh, 4)

	started := time.Now()
	require.NoError(t, client.close(DisconnectForceNoReconnect))
	elapsed := time.Since(started)
	require.GreaterOrEqual(t, elapsed, closeSubscribesTimeout.get())
	require.Less(t, elapsed, closeSubscribesTimeout.get()+2*time.Second)
	rec.requireDisconnectedOnce(t)

	// Callbacks invoked after that still end their attempts, after
	// DisconnectHandler.
	for i := 0; i < 4; i++ {
		(<-cbCh)(SubscribeReply{Options: SubscribeOptions{Type: SubscriptionTypeMap}}, nil)
	}
	require.Eventually(t, func() bool { return len(rec.get()) == 4 }, 5*time.Second, time.Millisecond)
}

// Shared poll: a new subscribe to a channel calls SubscribeHandler only after the
// UnsubscribeHandler call for the previous attempt on it.
func TestSubscribeAttempt_SharedPollResubscribeWaitsForAttemptEnd(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t)
	var mu sync.Mutex
	var events []string
	release := make(chan struct{})
	var subscribes atomic.Int32
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			mu.Lock()
			events = append(events, "subscribe")
			mu.Unlock()
			reply := SubscribeReply{}
			if subscribes.Add(1) == 1 {
				reply.Options.ExpireAt = time.Now().Unix() - 10 // Refused by Centrifuge.
			}
			cb(reply, nil)
		})
		client.OnUnsubscribe(func(e UnsubscribeEvent) {
			<-release
			mu.Lock()
			events = append(events, "unsubscribe")
			mu.Unlock()
		})
	})
	client := newTestConnectedClientV2(t, node, "user1")
	req := &protocol.SubscribeRequest{Channel: "ch", Type: int32(SubscriptionTypeSharedPoll)}
	require.NoError(t, client.handleSubscribe(req, &protocol.Command{Id: 1}, time.Now(), testReplyWriterWrapper().rw))

	resubscribed := make(chan struct{})
	go func() {
		defer close(resubscribed)
		_ = client.handleSubscribe(req, &protocol.Command{Id: 2}, time.Now(), testReplyWriterWrapper().rw)
	}()
	time.Sleep(50 * time.Millisecond)
	mu.Lock()
	require.Equal(t, []string{"subscribe"}, events)
	mu.Unlock()
	close(release)
	<-resubscribed
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []string{"subscribe", "unsubscribe", "subscribe"}, events)
}

// A client unsubscribe which gives up waiting for a map subscribe in
// SubscribeHandler disconnects the client, as for a regular subscribe in
// progress: the attempt could become a subscription after the unsubscribe.
func TestSubscribeAttempt_MapUnsubscribeTimeoutDisconnects(t *testing.T) {
	t.Parallel()
	node, _ := newTestNodeWithMapBroker(t)
	cbCh := make(chan SubscribeCallback, 1)
	client, rec := newAttemptTestClient(t, node, func(_ SubscribeEvent, cb SubscribeCallback) { cbCh <- cb })
	req := &protocol.SubscribeRequest{Channel: "map", Type: int32(SubscriptionTypeMap), Phase: MapPhaseState}
	require.NoError(t, client.handleSubscribe(req, &protocol.Command{Id: 1}, time.Now(), testReplyWriterWrapper().rw))
	cb := <-cbCh

	require.NoError(t, client.handleUnsubscribe(&protocol.UnsubscribeRequest{Channel: "map"}, &protocol.Command{Id: 2}, time.Now(), testReplyWriterWrapper().rw))
	require.Eventually(t, func() bool { return clientClosed(client) }, 15*time.Second, time.Millisecond)
	cb(SubscribeReply{Options: SubscribeOptions{Type: SubscriptionTypeMap}}, nil)
	e := rec.requireSingle(t)
	requireAttemptEnded(t, e, "map", UnsubscribeCodeDisconnect)
	require.Equal(t, DisconnectServerError.Code, e.Disconnect.Code)
	requireNoMapSubscribing(t, client)
}

func clientClosed(client *Client) bool {
	client.mu.RLock()
	defer client.mu.RUnlock()
	return client.status == statusClosed
}

// An UnsubscribeHandler which unsubscribes the client from a channel it is
// subscribing to meanwhile: neither waits for the other beyond the subscribe in
// progress, and the client stays connected.
func TestSubscribeAttempt_UnsubscribeHandlerUnsubscribesChannelBeingSubscribed(t *testing.T) {
	t.Parallel()
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()
	entered := make(chan struct{})
	handlerUnsubscribeTook := make(chan time.Duration, 1)
	var client *Client
	node.OnConnect(func(c *Client) {
		c.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			// Asynchronous, like a subscribe proxy.
			go func() {
				time.Sleep(200 * time.Millisecond)
				cb(SubscribeReply{}, nil)
			}()
		})
		c.OnUnsubscribe(func(e UnsubscribeEvent) {
			if e.Channel == "x" {
				close(entered)
				time.Sleep(50 * time.Millisecond) // The client subscribes to "y" meanwhile.
				started := time.Now()
				c.Unsubscribe("y")
				handlerUnsubscribeTook <- time.Since(started)
			}
		})
	})
	client = newTestConnectedClientV2(t, node, "user1")
	rw := testReplyWriterWrapper()
	require.NoError(t, client.handleSubscribe(&protocol.SubscribeRequest{Channel: "x"}, &protocol.Command{Id: 1}, time.Now(), rw.rw))
	require.Eventually(t, func() bool { return client.IsSubscribed("x") }, time.Second, time.Millisecond)

	go client.Unsubscribe("x")
	<-entered
	started := time.Now()
	require.NoError(t, client.handleSubscribe(&protocol.SubscribeRequest{Channel: "y"}, &protocol.Command{Id: 2}, time.Now(), testReplyWriterWrapper().rw))
	require.Less(t, time.Since(started), time.Second)
	// The unsubscribe waits for the subscribe in progress, then removes it.
	require.Less(t, <-handlerUnsubscribeTook, time.Second)
	require.False(t, clientClosed(client))
}

// A subscribe doesn't wait for UnsubscribeHandler calls of other channels: a slow
// one for a server-side unsubscribe doesn't hold up the client's commands.
func TestSubscribeAttempt_SubscribeDoesNotWaitForOtherChannels(t *testing.T) {
	t.Parallel()
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()
	entered, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(allowSubscribe(SubscribeReply{}))
		client.OnUnsubscribe(func(e UnsubscribeEvent) {
			if e.Channel == "slow" {
				close(entered)
				<-release
			}
		})
	})
	client := newTestConnectedClientV2(t, node, "user1")
	subscribeClientV2(t, client, "slow")
	go client.Unsubscribe("slow")
	<-entered

	started := time.Now()
	subscribeClientV2(t, client, "other")
	require.Less(t, time.Since(started), time.Second)
}

// A subscribe waiting for the UnsubscribeHandler of a previous attempt on its
// channel does not call SubscribeHandler if the client closes meanwhile.
func TestSubscribeAttempt_NoHandlerCallAfterCloseDuringWait(t *testing.T) {
	t.Parallel()
	mapNode, _ := newTestNodeWithMapBroker(t)
	for _, tc := range []struct {
		node *Node
		req  *protocol.SubscribeRequest
	}{
		{mapNode, &protocol.SubscribeRequest{Channel: "ch"}},
		{mapNode, &protocol.SubscribeRequest{Channel: "map", Type: int32(SubscriptionTypeMap), Phase: MapPhaseState}},
		{newTestNodeWithSharedPoll(t), &protocol.SubscribeRequest{Channel: "poll", Type: int32(SubscriptionTypeSharedPoll)}},
	} {
		entered, release := make(chan struct{}), make(chan struct{})
		var handlerCalls atomic.Int32
		tc.node.OnConnect(func(c *Client) {
			c.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
				// The first attempt is refused after the allow: its
				// UnsubscribeHandler call is slow.
				reply := SubscribeReply{Options: SubscribeOptions{Type: e.Type}}
				if handlerCalls.Add(1) == 1 {
					if e.Type == SubscriptionTypeMap {
						reply.Options.Type = SubscriptionTypeMapClients
					} else {
						reply.Options.ExpireAt = time.Now().Unix() - 10
					}
				}
				cb(reply, nil)
			})
			c.OnUnsubscribe(func(e UnsubscribeEvent) {
				if !e.Subscribed && handlerCalls.Load() == 1 {
					close(entered)
					<-release
				}
			})
		})
		client := newTestConnectedClientV2(t, tc.node, "user1")
		_ = client.handleSubscribe(tc.req, &protocol.Command{Id: 1}, time.Now(), testReplyWriterWrapper().rw)
		<-entered

		subscribed := make(chan error, 1)
		go func() {
			subscribed <- client.handleSubscribe(tc.req, &protocol.Command{Id: 2}, time.Now(), testReplyWriterWrapper().rw)
		}()
		time.Sleep(50 * time.Millisecond)
		closeDone := make(chan struct{})
		go func() { _ = client.close(DisconnectForceNoReconnect); close(closeDone) }()
		require.Eventually(t, func() bool { return clientClosed(client) }, time.Second, time.Millisecond)
		close(release)
		require.ErrorIs(t, <-subscribed, DisconnectConnectionClosed, tc.req.Channel)
		<-closeDone
		require.Equal(t, int32(1), handlerCalls.Load(), tc.req.Channel)
		client.mu.RLock()
		require.Empty(t, client.channels)
		require.Empty(t, client.tracking.mapSubscribePending)
		require.Zero(t, client.pendingUnsubscribes)
		client.mu.RUnlock()
	}
}

// A server unsubscribe in the middle of a map load waits for the whole load: the
// client's next page request may already be on its way, and must find the load
// (a stream or live request without one would start a new attempt).
func TestSubscribeAttempt_MapServerUnsubscribeMidLoadWaitsForLoad(t *testing.T) {
	t.Parallel()
	node, broker := newTestNodeWithMapBroker(t)
	var subscribes atomic.Int32
	client, rec := newAttemptTestClient(t, node, func(e SubscribeEvent, cb SubscribeCallback) {
		subscribes.Add(1)
		cb(SubscribeReply{Options: SubscribeOptions{Type: SubscriptionTypeMap}}, nil)
	})
	res := startPaginatedMapSubscribe(t, client, broker, "map")

	unsubscribed := make(chan struct{})
	go func() {
		defer close(unsubscribed)
		client.Unsubscribe("map")
	}()
	select {
	case <-unsubscribed:
		t.Fatal("server unsubscribe must wait for the map load")
	case <-time.After(50 * time.Millisecond):
	}
	// The client goes on with the load until it is live.
	for i := 0; i < 20 && res.Phase != MapPhaseLive; i++ {
		res = subscribeMapClient(t, client, &protocol.SubscribeRequest{
			Channel: "map", Type: int32(SubscriptionTypeMap), Phase: res.Phase, Limit: 2,
			Cursor: res.Cursor, Offset: res.Offset, Epoch: res.Epoch,
		})
	}
	require.Equal(t, MapPhaseLive, res.Phase)
	<-unsubscribed

	e := rec.requireSingle(t)
	require.True(t, e.Subscribed)
	require.Equal(t, UnsubscribeCodeServer, e.Code)
	require.Equal(t, int32(1), subscribes.Load())
	require.False(t, client.IsSubscribed("map"))
	require.False(t, clientClosed(client))
}

// A server unsubscribe waits for a map subscribe in SubscribeHandler, then
// removes what it became.
func TestSubscribeAttempt_MapServerUnsubscribeWhileHandlerPending(t *testing.T) {
	t.Parallel()
	node, _ := newTestNodeWithMapBroker(t)
	cbCh := make(chan SubscribeCallback, 1)
	client, rec := newAttemptTestClient(t, node, func(_ SubscribeEvent, cb SubscribeCallback) { cbCh <- cb })
	require.NoError(t, client.handleSubscribe(&protocol.SubscribeRequest{
		Channel: "map", Type: int32(SubscriptionTypeMap), Phase: MapPhaseState,
	}, &protocol.Command{Id: 1}, time.Now(), testReplyWriterWrapper().rw))
	cb := <-cbCh

	unsubscribed := make(chan struct{})
	go func() {
		defer close(unsubscribed)
		client.Unsubscribe("map")
	}()
	select {
	case <-unsubscribed:
		t.Fatal("server unsubscribe must wait for the map subscribe in SubscribeHandler")
	case <-time.After(50 * time.Millisecond):
	}
	cb(SubscribeReply{Options: SubscribeOptions{Type: SubscriptionTypeMap}}, nil)
	<-unsubscribed
	e := rec.requireSingle(t)
	require.True(t, e.Subscribed)
	require.False(t, client.IsSubscribed("map"))
}

// A subscribe with an unknown subscription type is rejected.
func TestSubscribeAttempt_UnknownTypeRejected(t *testing.T) {
	t.Parallel()
	node, _ := newTestNodeWithMapBroker(t)
	client, rec := newAttemptTestClient(t, node, func(SubscribeEvent, SubscribeCallback) {
		t.Error("SubscribeHandler called for an unknown subscription type")
	})
	err := client.handleSubscribe(&protocol.SubscribeRequest{Channel: "ch", Type: 7}, &protocol.Command{Id: 1}, time.Now(), testReplyWriterWrapper().rw)
	require.ErrorIs(t, err, ErrorBadRequest)
	require.Empty(t, rec.get())
}

// A SubscribeCallback invoked on a closed client still completes its command for
// CommandProcessedHandler.
func TestSubscribeAttempt_CallbackOnClosedClientCompletesCommand(t *testing.T) {
	t.Parallel()
	node, _ := newTestNodeWithMapBroker(t)
	var processed sync.Map
	node.OnCommandProcessed(func(_ *Client, e CommandProcessedEvent) {
		if e.Command.Subscribe != nil {
			processed.Store(e.Command.Subscribe.Channel, true)
		}
	})
	cbCh := make(chan SubscribeCallback, 2)
	client, _ := newAttemptTestClient(t, node, func(_ SubscribeEvent, cb SubscribeCallback) { cbCh <- cb })
	for i, req := range []*protocol.SubscribeRequest{
		{Channel: "ch"},
		{Channel: "map", Type: int32(SubscriptionTypeMap), Phase: MapPhaseState},
	} {
		cmd := &protocol.Command{Id: uint32(i + 1), Subscribe: req}
		require.NoError(t, client.handleSubscribe(req, cmd, time.Now(), testReplyWriterWrapper().rw))
	}
	closeDone := closeConcurrently(t, client, DisconnectForceNoReconnect)
	for i := 0; i < 2; i++ {
		(<-cbCh)(SubscribeReply{Options: SubscribeOptions{Type: SubscriptionTypeMap}}, nil)
	}
	<-closeDone
	for _, ch := range []string{"ch", "map"} {
		_, ok := processed.Load(ch)
		require.True(t, ok, ch)
	}
}

// A subscribe doesn't wait for a slow UnsubscribeHandler of an attempt ended on
// another channel: refused attempts don't serialize the client's other
// subscribes.
func TestSubscribeAttempt_SubscribeDoesNotWaitForOtherChannelAttemptEnds(t *testing.T) {
	t.Parallel()
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()
	entered, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			reply := SubscribeReply{}
			if e.Channel == "refused" {
				reply.Options.ExpireAt = time.Now().Unix() - 10
			}
			cb(reply, nil)
		})
		client.OnUnsubscribe(func(e UnsubscribeEvent) {
			if e.Channel == "refused" {
				close(entered)
				<-release
			}
		})
	})
	client := newTestConnectedClientV2(t, node, "user1")
	_ = client.handleSubscribe(&protocol.SubscribeRequest{Channel: "refused"}, &protocol.Command{Id: 1}, time.Now(), testReplyWriterWrapper().rw)
	<-entered

	started := time.Now()
	subscribeClientV2(t, client, "other")
	require.Less(t, time.Since(started), time.Second)
}

// Shared poll: a SubscribeCallback invoked on a closed client still completes
// its command for CommandProcessedHandler.
func TestSubscribeAttempt_SharedPollCallbackOnClosedClientCompletesCommand(t *testing.T) {
	t.Parallel()
	node := newTestNodeWithSharedPoll(t)
	var processed atomic.Bool
	node.OnCommandProcessed(func(_ *Client, e CommandProcessedEvent) {
		if e.Command.Subscribe != nil {
			processed.Store(true)
		}
	})
	cbCh := make(chan SubscribeCallback, 1)
	client, _ := newAttemptTestClient(t, node, func(_ SubscribeEvent, cb SubscribeCallback) { cbCh <- cb })
	req := &protocol.SubscribeRequest{Channel: "ch", Type: int32(SubscriptionTypeSharedPoll)}
	require.NoError(t, client.handleSubscribe(req, &protocol.Command{Id: 1, Subscribe: req}, time.Now(), testReplyWriterWrapper().rw))
	closeDone := closeConcurrently(t, client, DisconnectForceNoReconnect)
	(<-cbCh)(SubscribeReply{}, nil)
	<-closeDone
	require.True(t, processed.Load())
}

// Stuck UnsubscribeHandler calls for ended attempts hold up one subscribe for
// the wait bound, not each of the client's later subscribes in turn.
func TestSubscribeAttempt_StuckAttemptEndsDelayOneSubscribe(t *testing.T) {
	t.Parallel()
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()
	release := make(chan struct{})
	defer close(release)
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			reply := SubscribeReply{}
			if strings.HasPrefix(e.Channel, "refused") {
				reply.Options.ExpireAt = time.Now().Unix() - 10
			}
			cb(reply, nil)
		})
		client.OnUnsubscribe(func(e UnsubscribeEvent) {
			if strings.HasPrefix(e.Channel, "refused") {
				<-release // Stuck.
			}
		})
	})
	client := newTestConnectedClientV2(t, node, "user1")
	for i := 0; i < maxPendingAttemptEnds; i++ {
		_ = client.handleSubscribe(&protocol.SubscribeRequest{Channel: "refused" + strconv.Itoa(i)}, &protocol.Command{Id: 1}, time.Now(), testReplyWriterWrapper().rw)
	}
	started := time.Now()
	for i := 0; i < 3; i++ {
		subscribeClientV2(t, client, "ok"+strconv.Itoa(i))
	}
	elapsed := time.Since(started)
	require.GreaterOrEqual(t, elapsed, pendingUnsubscribesSubscribeTimeout.get())
	require.Less(t, elapsed, pendingUnsubscribesSubscribeTimeout.get()+2*time.Second)
}

func TestChannelCounts(t *testing.T) {
	t.Parallel()
	var m channelCounts
	m.add("a")
	m.add("b")
	m.add("a")
	require.True(t, m.has("a"))
	m.remove("a")
	require.True(t, m.has("a"))
	m.remove("a")
	require.False(t, m.has("a"))
	m.remove("a") // Not there: no-op.
	require.Equal(t, channelCounts{"b"}, m)
	m.remove("b")
	require.Empty(t, m)
	require.NotNil(t, m, "small backing array kept")

	for i := 0; i < 2*maxRetainedChannelCounts; i++ {
		m.add(strconv.Itoa(i))
	}
	for i := 0; i < 2*maxRetainedChannelCounts; i++ {
		m.remove(strconv.Itoa(i))
	}
	require.Nil(t, m, "large backing array dropped")
}

// attemptTracker is the application side of the SubscribeHandler /
// UnsubscribeHandler contract for one client: it counts what the handler
// allowed (before invoking the callback, as an application allocating per
// subscription state would) and what UnsubscribeHandler released.
type attemptTracker struct {
	mu              sync.Mutex
	allowed         map[string]int
	released        map[string]int
	overReleased    []string
	outOfOrder      []string
	afterDisconnect []string
	disconnects     int
	async           sync.WaitGroup
	kinds           map[string]int // Event kinds, to show what a run covered.
}

func newAttemptTracker() *attemptTracker {
	return &attemptTracker{allowed: map[string]int{}, released: map[string]int{}, kinds: map[string]int{}}
}

func (tr *attemptTracker) allow(ch string) {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	tr.allowed[ch]++
}

func (tr *attemptTracker) onUnsubscribe(e UnsubscribeEvent) {
	if e.ServerSide {
		return // Not from SubscribeHandler.
	}
	if rand.IntN(4) == 0 {
		// A slow handler (a database write, say).
		time.Sleep(time.Duration(rand.IntN(1500)) * time.Microsecond)
	}
	tr.mu.Lock()
	defer tr.mu.Unlock()
	tr.released[e.Channel]++
	if tr.disconnects > 0 {
		tr.afterDisconnect = append(tr.afterDisconnect, e.Channel)
	}
	tr.kinds[fmt.Sprintf("%s subscribed=%v code=%d", strings.SplitN(e.Channel, ":", 2)[0], e.Subscribed, e.Code)]++
	desc := fmt.Sprintf("%s (code %d, subscribed %v)", e.Channel, e.Code, e.Subscribed)
	if tr.released[e.Channel] > tr.allowed[e.Channel] {
		tr.overReleased = append(tr.overReleased, desc)
	}
}

// serverUnsubscribe unsubscribes the client server-side: its UnsubscribeHandler
// call too comes before the next SubscribeHandler call of the channel.
func (tr *attemptTracker) serverUnsubscribe(client *Client, ch string) {
	client.Unsubscribe(ch)
}

func (tr *attemptTracker) onDisconnect(DisconnectEvent) {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	tr.disconnects++
}

// onSubscribe answers synchronously or from another goroutine, allows or denies,
// and sometimes allows with options Centrifuge then refuses.
func (tr *attemptTracker) onSubscribe(e SubscribeEvent, cb SubscribeCallback) {
	tr.mu.Lock()
	if tr.allowed[e.Channel] != tr.released[e.Channel] {
		// A previous attempt on the channel did not get its UnsubscribeHandler
		// call yet.
		tr.outOfOrder = append(tr.outOfOrder, e.Channel)
	}
	tr.mu.Unlock()
	reply := SubscribeReply{Options: SubscribeOptions{Type: e.Type}}
	if e.Type == SubscriptionTypeSharedPoll {
		reply.Options.Type = 0
		reply.Options.ExpireAt = time.Now().Unix() + 3600
		reply.ClientSideRefresh = true
	} else if e.Type == SubscriptionTypeStream {
		reply.Options.EnableRecovery = rand.IntN(2) == 0
		reply.Options.EmitPresence = rand.IntN(2) == 0
		reply.Options.EmitJoinLeave = rand.IntN(2) == 0
	}
	var err error
	switch r := rand.IntN(20); {
	case r < 3:
		err = ErrorPermissionDenied
	case r < 4:
		reply.Options.ExpireAt = time.Now().Unix() - 10 // Refused by Centrifuge.
	case r < 5 && e.Type == SubscriptionTypeMap:
		reply.Options.Type = SubscriptionTypeMapClients // Refused by Centrifuge.
	}
	answer := func() {
		if err == nil {
			tr.allow(e.Channel)
		}
		cb(reply, err)
	}
	if rand.IntN(2) == 0 {
		answer()
		return
	}
	tr.async.Add(1)
	go func() {
		defer tr.async.Done()
		time.Sleep(time.Duration(rand.IntN(2000)) * time.Microsecond)
		answer()
	}()
}

// stressReplyWriter delivers the replies of one request, from whichever
// goroutine writes them.
func stressReplyWriter() (*replyWriter, chan *protocol.Reply) {
	ch := make(chan *protocol.Reply, 4)
	return &replyWriter{write: func(rep *protocol.Reply) {
		d, _ := rep.MarshalCF()
		var r protocol.Reply
		_ = r.UnmarshalCF(d)
		ch <- &r
	}}, ch
}

// jitterHistoryBroker delays history reads randomly, so subscribes commit at
// random moments relative to unsubscribes and disconnects.
type jitterHistoryBroker struct {
	*MemoryBroker
}

func (b *jitterHistoryBroker) History(ch string, opts HistoryOptions) ([]*Publication, StreamPosition, error) {
	time.Sleep(time.Duration(rand.IntN(1000)) * time.Microsecond)
	return b.MemoryBroker.History(ch, opts)
}

func newStressNode(t *testing.T) (*Node, *MemoryMapBroker) {
	node, err := New(Config{
		LogLevel:   LogLevelTrace,
		LogHandler: func(LogEntry) {},
		Map: MapConfig{
			GetMapChannelOptions: func(string) MapChannelOptions {
				// A short catch-up timeout: loads paused between pages expire, and
				// are swept by later map subscribes.
				return MapChannelOptions{Mode: MapModeRecoverable, KeyTTL: time.Minute, MinPageSize: 1, SubscribeCatchUpTimeout: 15 * time.Millisecond}
			},
		},
		SharedPoll: SharedPollConfig{
			GetSharedPollChannelOptions: func(channel string) (SharedPollChannelOptions, bool) {
				return SharedPollChannelOptions{
					RefreshInterval: 100 * time.Millisecond, RefreshBatchSize: 100, MaxKeysPerConnection: 100,
				}, strings.HasPrefix(channel, "poll:")
			},
		},
	})
	require.NoError(t, err)
	broker, err := NewMemoryBroker(node, MemoryBrokerConfig{})
	require.NoError(t, err)
	node.SetBroker(&jitterHistoryBroker{MemoryBroker: broker})
	mapBroker, err := NewMemoryMapBroker(node, MemoryMapBrokerConfig{})
	require.NoError(t, err)
	require.NoError(t, mapBroker.RegisterEventHandler(nil))
	node.SetMapBroker(mapBroker)
	node.OnSharedPoll(func(context.Context, SharedPollEvent) (SharedPollResult, error) {
		return SharedPollResult{}, nil
	})
	require.NoError(t, node.Run())
	t.Cleanup(func() { _ = node.Shutdown(context.Background()) })
	return node, mapBroker
}

var (
	stressRegularChannels = []string{"ch:0", "ch:1", "ch:2"}
	stressMapChannels     = []string{"map:0", "map:1"}
	stressPollChannels    = []string{"poll:0", "poll:1"}
)

func stressAllChannels() []string {
	var all []string
	all = append(all, stressRegularChannels...)
	all = append(all, stressMapChannels...)
	return append(all, stressPollChannels...)
}

// stressClientCommands sends random subscribe and unsubscribe commands, one at a
// time as a connection does, until the client closes or ops run out.
func stressClientCommands(client *Client, mapPositions *sync.Map, ops int) {
	await := func(replies chan *protocol.Reply) *protocol.Reply {
		if rand.IntN(3) == 0 {
			return nil // Pipeline: send the next command without waiting for the reply.
		}
		select {
		case r := <-replies:
			return r
		case <-time.After(50 * time.Millisecond):
			return nil // Disconnected, or the reply was dropped on a closed client.
		}
	}
	for i := 0; i < ops; i++ {
		client.mu.RLock()
		closed := client.status == statusClosed
		client.mu.RUnlock()
		if closed && rand.IntN(4) != 0 {
			return // Sometimes keep sending to a closed client.
		}
		cmd := &protocol.Command{Id: uint32(i + 1)}
		switch op := rand.IntN(10); {
		case op < 3:
			rw, replies := stressReplyWriter()
			ch := stressRegularChannels[rand.IntN(len(stressRegularChannels))]
			_ = client.handleSubscribe(&protocol.SubscribeRequest{Channel: ch, Recover: rand.IntN(2) == 0}, cmd, time.Now(), rw)
			await(replies)
		case op < 6:
			ch := stressMapChannels[rand.IntN(len(stressMapChannels))]
			req := &protocol.SubscribeRequest{Channel: ch, Type: int32(SubscriptionTypeMap), Phase: MapPhaseState, Limit: 2}
			if rand.IntN(4) == 0 {
				// Direct-to-live or stream recovery from a recent position.
				if v, ok := mapPositions.Load(ch); ok && v.(StreamPosition).Offset > 2 {
					pos := v.(StreamPosition)
					req.Recover = true
					req.Epoch = pos.Epoch
					req.Offset = pos.Offset - uint64(rand.IntN(3))
					req.Phase = MapPhaseLive
					if rand.IntN(2) == 0 {
						req.Phase = MapPhaseStream
						req.Offset = 1
						req.Limit = 1
					}
				}
			}
			// Paginate, sometimes abandoning the load halfway.
			for page := 0; page < 20; page++ {
				rw, replies := stressReplyWriter()
				_ = client.handleSubscribe(req, cmd, time.Now(), rw)
				r := await(replies)
				if r == nil || r.Error != nil || r.Subscribe == nil || r.Subscribe.Phase == MapPhaseLive || rand.IntN(6) == 0 {
					break
				}
				next := &protocol.SubscribeRequest{
					Channel: ch, Type: req.Type, Phase: r.Subscribe.Phase, Limit: req.Limit,
					Cursor: r.Subscribe.Cursor, Offset: r.Subscribe.Offset, Epoch: r.Subscribe.Epoch,
				}
				if r.Subscribe.Phase == MapPhaseStream {
					next.Recover = req.Recover
				}
				if rand.IntN(8) == 0 {
					time.Sleep(20 * time.Millisecond) // Longer than the catch-up timeout.
				}
				req = next
			}
		case op < 7:
			rw, replies := stressReplyWriter()
			ch := stressPollChannels[rand.IntN(len(stressPollChannels))]
			_ = client.handleSubscribe(&protocol.SubscribeRequest{Channel: ch, Type: int32(SubscriptionTypeSharedPoll)}, cmd, time.Now(), rw)
			await(replies)
		default:
			ch := stressAllChannels()[rand.IntN(len(stressAllChannels()))]
			_ = client.handleUnsubscribe(&protocol.UnsubscribeRequest{Channel: ch}, cmd, time.Now(), nil)
		}
	}
}

// TestSubscribeAttempt_Stress runs clients which subscribe (regular, map and
// shared poll), paginate, unsubscribe and get unsubscribed and disconnected by
// the server concurrently, with SubscribeHandler answering synchronously or
// asynchronously. Every allowed attempt must get exactly one UnsubscribeHandler
// call, never before it was allowed, a channel's SubscribeHandler call must come
// after the UnsubscribeHandler calls for its previous attempts, and nothing of
// the client may remain. SUBSCRIBE_ATTEMPT_STRESS_ROUNDS raises the
// number of rounds.
func TestSubscribeAttempt_Stress(t *testing.T) {
	t.Parallel()
	rounds := 3
	if v, err := strconv.Atoi(os.Getenv("SUBSCRIBE_ATTEMPT_STRESS_ROUNDS")); err == nil {
		rounds = v
	}
	node, mapBroker := newStressNode(t)
	ctx := context.Background()
	var mapPositions sync.Map
	for _, ch := range stressMapChannels {
		for i := 0; i < 6; i++ {
			res, err := mapBroker.Publish(ctx, ch, "k"+strconv.Itoa(i), MapPublishOptions{Data: []byte(`{}`)})
			require.NoError(t, err)
			mapPositions.Store(ch, res.Position)
		}
	}

	stopPublishing := make(chan struct{})
	publisherDone := make(chan struct{})
	go func() {
		defer close(publisherDone)
		for i := 0; ; i++ {
			select {
			case <-stopPublishing:
				return
			case <-time.After(200 * time.Microsecond):
			}
			ch := stressRegularChannels[i%len(stressRegularChannels)]
			_, _ = node.Publish(ch, []byte(`{}`), WithHistory(10, time.Minute))
			mch := stressMapChannels[i%len(stressMapChannels)]
			if res, err := mapBroker.Publish(ctx, mch, "k"+strconv.Itoa(i%8), MapPublishOptions{Data: []byte(`{}`)}); err == nil {
				mapPositions.Store(mch, res.Position)
			}
		}
	}()
	defer func() {
		close(stopPublishing)
		<-publisherDone
	}()

	trackers := map[*Client]*attemptTracker{}
	var trackersMu sync.Mutex
	node.OnConnect(func(client *Client) {
		tr := newAttemptTracker()
		trackersMu.Lock()
		trackers[client] = tr
		trackersMu.Unlock()
		client.OnSubscribe(tr.onSubscribe)
		client.OnUnsubscribe(tr.onUnsubscribe)
		client.OnDisconnect(tr.onDisconnect)
	})

	const clientsPerRound = 40
	for round := 0; round < rounds; round++ {
		var wg sync.WaitGroup
		for i := 0; i < clientsPerRound; i++ {
			client := newTestConnectedClientV2(t, node, "user"+strconv.Itoa(i))
			trackersMu.Lock()
			tr := trackers[client]
			trackersMu.Unlock()
			commandsDone := make(chan struct{})
			wg.Add(2)
			go func() {
				defer wg.Done()
				defer close(commandsDone)
				stressClientCommands(client, &mapPositions, 30)
			}()
			go func() {
				defer wg.Done()
				// The server unsubscribes, subscribes and disconnects at random moments.
				deadline := time.After(time.Duration(rand.IntN(40)) * time.Millisecond)
				for {
					select {
					case <-deadline:
					case <-commandsDone:
					case <-time.After(time.Duration(rand.IntN(3000)) * time.Microsecond):
						switch rand.IntN(6) {
						case 0, 1:
							ch := stressAllChannels()[rand.IntN(len(stressAllChannels()))]
							go tr.serverUnsubscribe(client, ch)
						case 2:
							ch := stressRegularChannels[rand.IntN(len(stressRegularChannels))]
							go func() { _ = client.Subscribe(ch) }()
						}
						continue
					}
					break
				}
				disconnect := []Disconnect{DisconnectForceNoReconnect, DisconnectConnectionClosed, DisconnectServerError}[rand.IntN(3)]
				_ = client.close(disconnect)
			}()
		}
		wg.Wait()
	}

	// close() waited for every SubscribeCallback, so nothing is pending. Give the
	// asynchronous server unsubscribes (now no-ops) a moment to finish.
	time.Sleep(100 * time.Millisecond)
	trackersMu.Lock()
	defer trackersMu.Unlock()
	require.Len(t, trackers, rounds*clientsPerRound)
	var allowedTotal int
	kinds := map[string]int{}
	for client, tr := range trackers {
		tr.async.Wait()
		// UnsubscribeHandler calls for attempts ended after close() may still be
		// on their way.
		require.Eventually(t, func() bool {
			client.mu.RLock()
			defer client.mu.RUnlock()
			return client.pendingUnsubscribes == 0
		}, 10*time.Second, time.Millisecond, "client %s", client.uid)
		tr.mu.Lock()
		require.Equal(t, 1, tr.disconnects, "client %s", client.uid)
		require.Empty(t, tr.overReleased, "client %s: UnsubscribeHandler without an allowed attempt", client.uid)
		require.Empty(t, tr.outOfOrder, "client %s: SubscribeHandler before the UnsubscribeHandler of a previous attempt on the channel", client.uid)
		require.Empty(t, tr.afterDisconnect, "client %s: UnsubscribeHandler after DisconnectHandler", client.uid)
		for ch, n := range tr.allowed {
			require.Equal(t, n, tr.released[ch], "client %s channel %s: allowed %d attempts, released %d", client.uid, ch, n, tr.released[ch])
			allowedTotal += n
		}
		for k, n := range tr.kinds {
			kinds[k] += n
		}
		tr.mu.Unlock()
		client.mu.RLock()
		require.Empty(t, client.channels, "client %s", client.uid)
		require.Empty(t, client.mapSubscribing, "client %s", client.uid)
		require.Zero(t, client.pendingUnsubscribes, "client %s", client.uid)
		client.mu.RUnlock()
	}
	for _, ch := range stressAllChannels() {
		require.Zero(t, node.hub.NumSubscribers(ch), "channel %s", ch)
	}
	t.Logf("%d clients, %d allowed attempts, all released once, in order per channel", len(trackers), allowedTotal)
	for k, n := range kinds {
		t.Logf("  %s: %d", k, n)
	}
}
