package centrifuge

import (
	"context"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/require"
)

// TestClient_ConcurrentEntryPoints hammers a single Client instance through many
// entry points at once — command handlers (subscribe/unsubscribe/publish/
// presence/history/rpc/refresh/send), public methods (Info/Channels/Context),
// timer ops (updatePresence/sendPing/checkPong/expire), broadcasts, and
// Disconnect — from many goroutines.
//
// This is not an artificial scenario: the emulation transport dispatches each
// client command on its own goroutine (see emulation.go — `go HandleReadFrame`),
// so command handlers genuinely run concurrently for one client, with no global
// command serialization. Every handler must be safe under that concurrency.
//
// Run under -race; a handler that touches shared client state without c.mu shows
// up as a data race, and any lock-order inversion shows up as a deadlock
// (the test would hang and time out).
func TestClient_ConcurrentEntryPoints(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{Options: SubscribeOptions{
				EmitPresence:      true,
				EmitJoinLeave:     true,
				EnableRecovery:    true,
				EnablePositioning: true,
			}}, nil)
		})
		client.OnPublish(func(e PublishEvent, cb PublishCallback) {
			cb(PublishReply{}, nil)
		})
		client.OnRefresh(func(e RefreshEvent, cb RefreshCallback) {
			cb(RefreshReply{ExpireAt: time.Now().Unix() + 100}, nil)
		})
		client.OnSubRefresh(func(e SubRefreshEvent, cb SubRefreshCallback) {
			cb(SubRefreshReply{ExpireAt: time.Now().Unix() + 100}, nil)
		})
		client.OnRPC(func(e RPCEvent, cb RPCCallback) {
			cb(RPCReply{}, nil)
		})
		client.OnPresence(func(e PresenceEvent, cb PresenceCallback) {
			cb(PresenceReply{}, nil)
		})
		client.OnMessage(func(e MessageEvent) {})
		client.OnAlive(func() {})
	})

	const rounds = 40
	const workers = 12
	channels := []string{"a", "b", "c", "d"}

	for round := 0; round < rounds; round++ {
		client := newTestClientV2(t, node, "u"+strconv.Itoa(round))
		connectClientV2(t, client)
		// Pre-subscribe so unsubscribe/presence/history have something to act on.
		for _, ch := range channels {
			subscribeClientV2(t, client, ch)
		}

		var wg sync.WaitGroup
		start := make(chan struct{})
		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func(seed int) {
				defer wg.Done()
				rnd := rand.New(rand.NewSource(int64(seed)))
				<-start
				for i := 0; i < 60; i++ {
					ch := channels[rnd.Intn(len(channels))]
					switch rnd.Intn(16) {
					case 0:
						rw := testReplyWriterWrapper()
						_ = client.handleSubscribe(&protocol.SubscribeRequest{Channel: ch}, &protocol.Command{Id: 1}, time.Now(), rw.rw)
					case 1:
						rw := testReplyWriterWrapper()
						_ = client.handleUnsubscribe(&protocol.UnsubscribeRequest{Channel: ch}, &protocol.Command{Id: 1}, time.Now(), rw.rw)
					case 2:
						rw := testReplyWriterWrapper()
						_ = client.handlePublish(&protocol.PublishRequest{Channel: ch, Data: []byte(`{}`)}, &protocol.Command{Id: 1}, time.Now(), rw.rw)
					case 3:
						rw := testReplyWriterWrapper()
						_ = client.handlePresence(&protocol.PresenceRequest{Channel: ch}, &protocol.Command{Id: 1}, time.Now(), rw.rw)
					case 4:
						rw := testReplyWriterWrapper()
						_ = client.handlePresenceStats(&protocol.PresenceStatsRequest{Channel: ch}, &protocol.Command{Id: 1}, time.Now(), rw.rw)
					case 5:
						rw := testReplyWriterWrapper()
						_ = client.handleHistory(&protocol.HistoryRequest{Channel: ch}, &protocol.Command{Id: 1}, time.Now(), rw.rw)
					case 6:
						rw := testReplyWriterWrapper()
						_ = client.handleRPC(&protocol.RPCRequest{Data: []byte(`{}`)}, &protocol.Command{Id: 1}, time.Now(), rw.rw)
					case 7:
						rw := testReplyWriterWrapper()
						_ = client.handleRefresh(&protocol.RefreshRequest{}, &protocol.Command{Id: 1}, time.Now(), rw.rw)
					case 8:
						rw := testReplyWriterWrapper()
						_ = client.handleSubRefresh(&protocol.SubRefreshRequest{Channel: ch}, &protocol.Command{Id: 1}, time.Now(), rw.rw)
					case 9:
						_ = client.handleSend(&protocol.SendRequest{Data: []byte(`{}`)}, &protocol.Command{}, time.Now())
					case 10:
						_ = client.Info()
					case 11:
						_ = client.Channels()
					case 12:
						client.updatePresence()
					case 13:
						client.sendPing()
					case 14:
						_, _ = node.Publish(ch, []byte(`{"x":1}`))
					case 15:
						// Disconnect is the teardown racer — only a couple of workers
						// actually fire it so most iterations run against a live client.
						if seed%4 == 0 && i > 30 {
							client.Disconnect(DisconnectForceNoReconnect)
						}
					}
				}
			}(round*workers + w)
		}
		close(start)
		wg.Wait()

		// Ensure the client is closed and the hub does not leak it.
		_ = client.close(DisconnectForceNoReconnect)
	}

	require.Eventually(t, func() bool {
		return node.hub.NumClients() == 0
	}, 5*time.Second, 20*time.Millisecond, "clients leaked in hub after concurrent churn")
}
