package centrifuge

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/centrifugal/protocol"
)

// TestNode_ChannelMediumMapConcurrency guards a data race on n.mediums.
// n.mediums is a single map, but it is guarded by sharded mediumLocks (one lock
// per channel-hash). Operations on different channels take different lock shards
// yet mutate the same map, so a subscribe that creates a medium and a publish
// that reads one for a different channel access the map concurrently — a data
// race that can escalate to a fatal "concurrent map writes". Reachable via the
// exported channel-medium options. Must be run under -race.
func TestNode_ChannelMediumMapConcurrency(t *testing.T) {
	node, err := New(Config{
		LogLevel: LogLevelError, LogHandler: func(LogEntry) {},
		GetChannelMediumOptions: func(channel string) ChannelMediumOptions {
			return ChannelMediumOptions{KeepLatestPublication: true} // exported, public API
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	node.OnConnect(func(c *Client) {
		c.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) { cb(SubscribeReply{}, nil) })
	})
	if err := node.Run(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = node.Shutdown(context.Background()) }()

	client := newTestClientV2(t, node, "u")
	connectClientV2(t, client)

	const nch = 128 // spans many medium-lock shards
	names := make([]string, nch)
	for i := range names {
		names[i] = "cm" + strconv.Itoa(i)
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup

	// Publisher: reads n.mediums[ch] for every channel.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				for _, ch := range names {
					_, _ = node.Publish(ch, []byte(`{"x":1}`))
				}
			}
		}
	}()

	// Subscriber churn: creates (first sub) and deletes (last sub) mediums,
	// writing n.mediums[ch] for every channel.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			for _, ch := range names {
				rw := testReplyWriterWrapper()
				_ = client.handleSubscribe(&protocol.SubscribeRequest{Channel: ch}, &protocol.Command{Id: 1}, time.Now(), rw.rw)
			}
			for _, ch := range names {
				client.Unsubscribe(ch)
			}
		}
	}()

	time.Sleep(500 * time.Millisecond)
	close(stop)
	wg.Wait()
}
