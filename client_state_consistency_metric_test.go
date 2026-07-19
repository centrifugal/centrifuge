package centrifuge

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/centrifugal/protocol"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

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
