package centrifuge

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/require"
)

// TestConnect_ServerSideSubs_CloseRace covers a client that closes while
// connectCmd is applying connect-time server-side subscriptions. Two problems it
// guards:
//   - the server-side subs finalize writes c.channels unconditionally, so subs
//     installed after close() snapshotted c.channels leak in the hub (NumSubscriptions
//     stays > 0 after every client is gone);
//   - connectCmd writes connect fields (c.user) under c.mu while close reads them
//     unsynchronized (a torn string read).
//
// Run under -race.
func TestConnect_ServerSideSubs_CloseRace(t *testing.T) {
	channels := []string{"ss0", "ss1", "ss2", "ss3"}
	node, err := New(Config{LogLevel: LogLevelError, LogHandler: func(LogEntry) {}})
	require.NoError(t, err)
	node.OnConnecting(func(ctx context.Context, e ConnectEvent) (ConnectReply, error) {
		subs := make(map[string]SubscribeOptions, len(channels))
		for _, ch := range channels {
			subs[ch] = SubscribeOptions{}
		}
		return ConnectReply{Subscriptions: subs}, nil
	})
	require.NoError(t, node.Run())
	defer func() { _ = node.Shutdown(context.Background()) }()

	const workers = 16
	const rounds = 200
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for r := 0; r < rounds; r++ {
				ctx, cancel := context.WithCancel(context.Background())
				tr := &countingTransport{pingInterval: 50 * time.Millisecond, pongTimeout: -1}
				client, err := newClient(SetCredentials(ctx, &Credentials{UserID: "u" + strconv.Itoa(w)}), node, tr)
				if err != nil {
					cancel()
					return
				}
				// Close concurrently with connectCmd installing the server-side subs.
				go func() { _ = client.close(DisconnectForceNoReconnect) }()
				rw := testReplyWriterWrapper()
				_ = client.connectCmd(&protocol.ConnectRequest{}, &protocol.Command{}, time.Now(), rw.rw)
				client.triggerConnect()
				client.scheduleOnConnectTimers()
				_ = client.close(DisconnectForceNoReconnect)
				cancel()
			}
		}(w)
	}
	wg.Wait()

	require.Eventually(t, func() bool {
		return node.hub.NumClients() == 0 && node.hub.NumSubscriptions() == 0
	}, 5*time.Second, 10*time.Millisecond,
		"hub leak after connect/close race: NumClients=%d NumSubscriptions=%d",
		node.hub.NumClients(), node.hub.NumSubscriptions())
}
