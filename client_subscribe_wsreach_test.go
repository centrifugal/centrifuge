package centrifuge

import (
	"context"
	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/require"
	"strconv"
	"sync"
	"testing"
	"time"
)

// Server-side Client.Subscribe (app goroutine) racing a client-command unsubscribe
// on the SAME channel. This is reachable on ANY transport incl WebSocket: the app
// calls Subscribe from its own goroutine while the connection reader processes the
// client's unsubscribe. Neither transport nor emulation gating applies.
func TestWSReach_ServerSubscribeVsClientUnsubscribe(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) { cb(SubscribeReply{}, nil) })
	})
	client := newTestClientV2(t, node, "u")
	connectClientV2(t, client)
	const ch = "z"
	for r := 0; r < 20000; r++ {
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); _ = client.Subscribe(ch) }() // server-side (app goroutine)
		go func() {
			defer wg.Done()
			rw := testReplyWriterWrapper()
			_ = client.handleUnsubscribe(&protocol.UnsubscribeRequest{Channel: ch}, &protocol.Command{Id: 1}, time.Now(), rw.rw)
		}() // client command
		wg.Wait()
		p, _ := clientHasChannel(client, ch)
		h := hubHasSub(node, ch, client.ID())
		require.Equalf(t, p, h, "round %d: c.channels=%v hub=%v (WebSocket-reachable inconsistency)", r, p, h)
		// clean slate
		rw := testReplyWriterWrapper()
		_ = client.handleUnsubscribe(&protocol.UnsubscribeRequest{Channel: ch}, &protocol.Command{Id: 1}, time.Now(), rw.rw)
		_ = strconv.Itoa(r)
	}
}

// Client-command subscribe racing an async unsubscribe (what the presence tick's
// handleAsyncUnsubscribe and the server-side Client.Unsubscribe do — both run on
// non-reader goroutines). This is WebSocket-reachable: the reader processes the
// subscribe while a tick/app goroutine unsubscribes the same channel.
func TestWSReach_ClientSubscribeVsAsyncUnsubscribe(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) { cb(SubscribeReply{}, nil) })
	})
	client := newTestClientV2(t, node, "u")
	connectClientV2(t, client)
	const ch = "w"
	for r := 0; r < 30000; r++ {
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			rw := testReplyWriterWrapper()
			_ = client.handleSubscribe(&protocol.SubscribeRequest{Channel: ch}, &protocol.Command{Id: 1}, time.Now(), rw.rw)
		}()
		go func() { defer wg.Done(); _ = client.unsubscribe(ch, unsubscribeServer, nil) }() // async unsubscribe (tick/server-side)
		wg.Wait()
		p, _ := clientHasChannel(client, ch)
		h := hubHasSub(node, ch, client.ID())
		require.Equalf(t, p, h, "round %d: c.channels=%v hub=%v", r, p, h)
		rw := testReplyWriterWrapper()
		_ = client.handleUnsubscribe(&protocol.UnsubscribeRequest{Channel: ch}, &protocol.Command{Id: 1}, time.Now(), rw.rw)
	}
}

// Maximum WebSocket-reachable concurrency on one channel: several server-side
// Client.Subscribe calls (app goroutines) and several async unsubscribes
// (tick/server-side) at once. All of these run on non-reader goroutines, so this
// is fully reachable on WebSocket without any emulation. If the remaining
// inconsistency were WebSocket-reachable it would surface here.
func TestWSReach_MaxConcurrency(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) { cb(SubscribeReply{}, nil) })
	})
	client := newTestClientV2(t, node, "u")
	connectClientV2(t, client)
	const ch = "m"
	for r := 0; r < 15000; r++ {
		var wg sync.WaitGroup
		for i := 0; i < 3; i++ {
			wg.Add(2)
			go func() { defer wg.Done(); _ = client.Subscribe(ch) }()
			go func() { defer wg.Done(); _ = client.unsubscribe(ch, unsubscribeServer, nil) }()
		}
		wg.Wait()
		p, _ := clientHasChannel(client, ch)
		h := hubHasSub(node, ch, client.ID())
		require.Equalf(t, p, h, "round %d: c.channels=%v hub=%v", r, p, h)
		_ = client.unsubscribe(ch, unsubscribeServer, nil)
	}
}
