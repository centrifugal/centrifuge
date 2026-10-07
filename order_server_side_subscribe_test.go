package centrifuge

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/require"
)

// A server-side Client.Subscribe releases an unsubscribe waiting for it only
// after the publications queued since its sync point are written: otherwise the
// unsubscribe push could come before them, and a publication follow the push.
func TestOrderPublicationAfterUnsubscribePush(t *testing.T) {
	isInTest.Store(true)
	testSyncPointDelay.Store(1)
	defer testSyncPointDelay.Store(0)

	atSync := make(chan struct{})
	release := make(chan struct{})
	hook := func(string) {
		atSync <- struct{}{}
		<-release
	}
	testAtSyncPoint.Store(&hook)
	defer testAtSyncPoint.Store(nil)

	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()
	node.OnConnect(func(client *Client) {})

	for i := 0; i < 2000; i++ {
		channel := testChannelRecoveryOrderingPrefix + ":pub_after_push:" + strconv.Itoa(i)
		_, err := node.Publish(channel, []byte(`{}`), WithHistory(10, time.Minute))
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		transport := newTestTransport(cancel)
		transport.sink = make(chan []byte, 100)
		client, _, err := NewClient(SetCredentials(ctx, &Credentials{UserID: "u"}), node, transport)
		require.NoError(t, err)
		require.True(t, client.HandleCommand(&protocol.Command{Id: 1, Connect: &protocol.ConnectRequest{}}, 0))
		<-transport.sink // connect reply

		subscribed := make(chan struct{})
		go func() {
			defer close(subscribed)
			_ = client.Subscribe(channel, WithPositioning(true))
		}()
		<-atSync
		// Queued in the subscription's recovery buffer.
		_, err = node.Publish(channel, []byte(`{}`), WithHistory(10, time.Minute))
		require.NoError(t, err)
		unsubscribed := make(chan struct{})
		go func() {
			defer close(unsubscribed)
			client.Unsubscribe(channel) // Waits for the subscribe in progress.
		}()
		time.Sleep(time.Millisecond)
		close(release)
		<-subscribed
		<-unsubscribed
		release = make(chan struct{})

		var frames []string
		timeout := time.After(time.Second)
		for len(frames) < 3 {
			select {
			case data := <-transport.sink:
				decoder := protocol.NewJSONReplyDecoder(data)
				for {
					reply, err := decoder.Decode()
					if err != nil {
						break
					}
					switch {
					case reply.Push != nil && reply.Push.Subscribe != nil:
						frames = append(frames, "subscribe")
					case reply.Push != nil && reply.Push.Pub != nil:
						frames = append(frames, "publication")
					case reply.Push != nil && reply.Push.Unsubscribe != nil:
						frames = append(frames, "unsubscribe")
					}
				}
			case <-timeout:
				t.Fatalf("iteration %d: frames %v", i, frames)
			}
		}
		require.Equal(t, []string{"subscribe", "publication", "unsubscribe"}, frames, "iteration %d", i)
		_ = client.close(DisconnectForceNoReconnect)
	}
}

// The same for a subscription made on connect (ConnectReply.Subscriptions).
func TestOrderConnectPublicationAfterUnsubscribePush(t *testing.T) {
	isInTest.Store(true)
	testSyncPointDelay.Store(1)
	defer testSyncPointDelay.Store(0)

	atSync := make(chan struct{})
	release := make(chan struct{})
	hook := func(string) {
		atSync <- struct{}{}
		<-release
	}
	testAtSyncPoint.Store(&hook)
	defer testAtSyncPoint.Store(nil)

	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()
	var channel string
	node.OnConnecting(func(ctx context.Context, e ConnectEvent) (ConnectReply, error) {
		return ConnectReply{Subscriptions: map[string]SubscribeOptions{channel: {EnablePositioning: true}}}, nil
	})
	node.OnConnect(func(client *Client) {})

	for i := 0; i < 2000; i++ {
		channel = testChannelRecoveryOrderingPrefix + ":connect_pub_after_push:" + strconv.Itoa(i)
		_, err := node.Publish(channel, []byte(`{}`), WithHistory(10, time.Minute))
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		transport := newTestTransport(cancel)
		transport.sink = make(chan []byte, 100)
		client, _, err := NewClient(SetCredentials(ctx, &Credentials{UserID: "u"}), node, transport)
		require.NoError(t, err)

		connected := make(chan struct{})
		go func() {
			defer close(connected)
			client.HandleCommand(&protocol.Command{Id: 1, Connect: &protocol.ConnectRequest{}}, 0)
		}()
		<-atSync
		// Queued in the subscription's recovery buffer.
		_, err = node.Publish(channel, []byte(`{}`), WithHistory(10, time.Minute))
		require.NoError(t, err)
		unsubscribed := make(chan struct{})
		go func() {
			defer close(unsubscribed)
			client.Unsubscribe(channel) // Waits for the subscribe in progress.
		}()
		time.Sleep(time.Millisecond)
		close(release)
		<-connected
		<-unsubscribed
		release = make(chan struct{})

		var frames []string
		timeout := time.After(time.Second)
		for len(frames) < 3 {
			select {
			case data := <-transport.sink:
				decoder := protocol.NewJSONReplyDecoder(data)
				for {
					reply, err := decoder.Decode()
					if err != nil {
						break
					}
					switch {
					case reply.Id == 1 && reply.Connect != nil:
						frames = append(frames, "connect")
					case reply.Push != nil && reply.Push.Pub != nil:
						frames = append(frames, "publication")
					case reply.Push != nil && reply.Push.Unsubscribe != nil:
						frames = append(frames, "unsubscribe")
					}
				}
			case <-timeout:
				t.Fatalf("iteration %d: frames %v", i, frames)
			}
		}
		require.Equal(t, []string{"connect", "publication", "unsubscribe"}, frames, "iteration %d", i)
		_ = client.close(DisconnectForceNoReconnect)
	}
}
