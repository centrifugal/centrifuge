package centrifuge

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/require"
)

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
