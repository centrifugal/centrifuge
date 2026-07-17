package centrifuge

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

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
