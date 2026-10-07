package centrifuge

import (
	"context"
	"testing"
	"time"

	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/require"
)

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
