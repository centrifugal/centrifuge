//go:build integration

package centrifuge

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// TestRedisBrokerPubSubQueueMaxSize checks the PUB/SUB queue on a real
// connection: a handler which is stuck never stops the connection from
// reading, what is over PubSubQueueMaxSize is dropped, and the budget comes
// back - also from messages still waiting when Redis closes the connection.
//
// Not parallel: it closes every PUB/SUB connection of the Redis server.
func TestRedisBrokerPubSubQueueMaxSize(t *testing.T) {
	node := testNode(t)
	defer func() { _ = node.Shutdown(context.Background()) }()
	s, err := NewRedisShard(node, testSingleRedisConf(6379))
	require.NoError(t, err)
	const limit = 64 * 1024
	b, err := NewRedisBroker(node, RedisBrokerConfig{
		Prefix:              uuid.NewString(),
		Shards:              []*RedisShard{s},
		PubSubQueueMaxSize:  limit,
		numPubSubProcessors: 1,
	})
	require.NoError(t, err)
	defer stopRedisBroker(b)

	var blocked atomic.Bool
	blocked.Store(true)
	gate := make(chan struct{})
	var received atomic.Int64
	handler := &testBrokerEventHandler{HandlePublicationFunc: func(ch string, pub *Publication, _ StreamPosition, _ bool, _ *Publication) error {
		if blocked.Load() {
			<-gate
		}
		received.Add(1)
		return nil
	}}
	require.NoError(t, b.RegisterBrokerEventHandler(handler))
	require.NoError(t, b.Subscribe("queue"))

	data := []byte(strings.Repeat("x", 1024))
	used := func() int64 { return b.pubSubBudget.used.Load() }

	// The handler is stuck: the connection keeps reading, the budget fills
	// up and the rest is dropped.
	for range 200 {
		_, err := b.Publish("queue", data, PublishOptions{})
		require.NoError(t, err)
	}
	require.Eventually(t, func() bool { return used() > limit-2*1024 }, 10*time.Second, 10*time.Millisecond)
	require.LessOrEqual(t, used(), int64(limit))

	// The handler goes on: everything queued is delivered and the budget is
	// empty again.
	blocked.Store(false)
	close(gate)
	require.Eventually(t, func() bool { return used() == 0 }, 10*time.Second, 10*time.Millisecond)
	accepted := received.Load()
	require.Greater(t, accepted, int64(40))
	require.Less(t, accepted, int64(200), "what didn't fit was dropped")

	// Messages waiting when Redis closes the connection give their bytes back
	// too, and the broker delivers again once it subscribed again.
	gate = make(chan struct{})
	blocked.Store(true)
	for range 20 {
		_, err := b.Publish("queue", data, PublishOptions{})
		require.NoError(t, err)
	}
	require.Eventually(t, func() bool { return used() > 10*1024 }, 10*time.Second, 10*time.Millisecond)
	client := s.client
	require.NoError(t, client.Do(context.Background(), client.B().ClientKill().TypePubsub().Build()).Error())
	require.Eventually(t, func() bool { return used() <= 2*1024 }, 10*time.Second, 10*time.Millisecond,
		"only the message being handled is still counted")
	blocked.Store(false)
	close(gate)
	require.Eventually(t, func() bool { return used() == 0 }, 10*time.Second, 10*time.Millisecond)

	before := received.Load()
	require.Eventually(t, func() bool {
		_, err := b.Publish("queue", data, PublishOptions{})
		require.NoError(t, err)
		return received.Load() > before
	}, 20*time.Second, 100*time.Millisecond, "delivery after the connection came back")
	require.Eventually(t, func() bool { return used() == 0 }, 10*time.Second, 10*time.Millisecond)
}
