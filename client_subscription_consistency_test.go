package centrifuge

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

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
