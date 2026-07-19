package centrifuge

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/require"
)

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
