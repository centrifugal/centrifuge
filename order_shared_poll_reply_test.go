package centrifuge

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/require"
)

// A client unsubscribe processed while an asynchronously answered shared poll
// subscribe is being installed waits for it: the client gets the subscribe reply
// first, then the unsubscribe reply, and UnsubscribeHandler comes after the
// subscribe reply.
func TestOrderSharedPollUnsubscribeReplyBeforeSubscribeReply(t *testing.T) {
	var armed atomic.Bool
	inOptions := make(chan struct{})
	release := make(chan struct{})
	node, err := New(Config{
		LogLevel:   LogLevelError,
		LogHandler: func(LogEntry) {},
		SharedPoll: SharedPollConfig{
			GetSharedPollChannelOptions: func(channel string) (SharedPollChannelOptions, bool) {
				if armed.CompareAndSwap(true, false) {
					// Called by the subscribe callback after the subscription was
					// installed and before the reply.
					close(inOptions)
					<-release
				}
				return SharedPollChannelOptions{RefreshInterval: time.Second, RefreshBatchSize: 100, MaxKeysPerConnection: 100}, true
			},
		},
	})
	require.NoError(t, err)
	node.OnSharedPoll(func(context.Context, SharedPollEvent) (SharedPollResult, error) {
		return SharedPollResult{}, nil
	})
	unsubscribeEvents := make(chan UnsubscribeEvent, 1)
	answer := make(chan SubscribeCallback, 1)
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) { answer <- cb })
		client.OnUnsubscribe(func(e UnsubscribeEvent) { unsubscribeEvents <- e })
	})
	require.NoError(t, node.Run())
	defer func() { _ = node.Shutdown(context.Background()) }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	transport := newTestTransport(cancel)
	transport.sink = make(chan []byte, 100)
	client, _, err := NewClient(SetCredentials(ctx, &Credentials{UserID: "u"}), node, transport)
	require.NoError(t, err)
	require.True(t, client.HandleCommand(&protocol.Command{Id: 100, Connect: &protocol.ConnectRequest{}}, 0))
	<-transport.sink

	require.True(t, client.HandleCommand(&protocol.Command{Id: 1, Subscribe: &protocol.SubscribeRequest{
		Channel: "poll", Type: int32(SubscriptionTypeSharedPoll),
	}}, 0))
	cb := <-answer
	armed.Store(true)
	go cb(SubscribeReply{Options: SubscribeOptions{ExpireAt: time.Now().Unix() + 3600}, ClientSideRefresh: true}, nil)
	<-inOptions
	// The client unsubscribes while the subscribe is between its install and its reply.
	unsubscribed := make(chan struct{})
	go func() {
		defer close(unsubscribed)
		client.HandleCommand(&protocol.Command{Id: 2, Unsubscribe: &protocol.UnsubscribeRequest{Channel: "poll"}}, 0)
	}()
	select {
	case <-unsubscribed: // Did not wait for the subscribe in progress (the bug).
	case <-time.After(50 * time.Millisecond): // Waits for it.
	}
	close(release)
	<-unsubscribed

	var order []uint32
	timeout := time.After(2 * time.Second)
	for len(order) < 2 {
		select {
		case data := <-transport.sink:
			decoder := protocol.NewJSONReplyDecoder(data)
			for {
				reply, err := decoder.Decode()
				if err != nil {
					break
				}
				if reply.Id != 0 {
					order = append(order, reply.Id)
				}
			}
		case <-timeout:
			t.Fatalf("replies: %v", order)
		}
	}
	e := <-unsubscribeEvents
	t.Logf("UnsubscribeHandler: code %d", e.Code)
	require.Equal(t, []uint32{1, 2}, order, "subscribe reply must come before the unsubscribe reply")
}
