package centrifuge

import (
	"context"
	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/require"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/centrifugal/centrifuge/internal/queue"
)

func BenchmarkPerChannelWriter(b *testing.B) {
	const numChannels = 10
	var wg sync.WaitGroup

	flushFn := func(items []queue.Item) error {
		for range items {
			wg.Done()
		}
		return nil
	}

	w := newPerChannelWriter(flushFn)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wg.Add(1) // Each added message increments the WaitGroup counter.
		channelName := "channel-" + strconv.Itoa(i%numChannels)
		item := queue.Item{Channel: channelName}
		w.Add(item, 10*time.Millisecond, 128)
	}
	w.Close(true)
	wg.Wait() // Wait for all messages to be flushed.
}

func TestClientSubscribeReceivePublication_ChannelBatching_Delay(t *testing.T) {
	t.Parallel()
	node := defaultTestNode()
	node.config.GetChannelBatchConfig = func(channel string) (ChannelBatchConfig, bool) {
		return ChannelBatchConfig{
			MaxBatchSize: 0,
			WriteDelay:   10 * time.Millisecond,
		}, true
	}
	defer func() { _ = node.Shutdown(context.Background()) }()
	transport := newTestTransport(func() {})
	transport.sink = make(chan []byte, 100)
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
	client, _ := newClient(newCtx, node, transport)

	connectClientV2(t, client)

	rwWrapper := testReplyWriterWrapper()

	client.channels["test"] = ChannelContext{}
	subCtx := client.subscribeCmd(&protocol.SubscribeRequest{
		Channel: "test",
	}, SubscribeReply{}, &protocol.Command{}, false, time.Now(), rwWrapper.rw)
	require.Nil(t, subCtx.disconnect)
	require.Nil(t, rwWrapper.replies[0].Error)

	done := make(chan struct{})
	go func() {
		for data := range transport.sink {
			if strings.Contains(string(data), "test message") {
				close(done)
			}
		}
	}()

	_, err := node.Publish("test", []byte(`{"text": "test message"}`))
	require.NoError(t, err)

	select {
	case <-time.After(time.Second):
		require.Fail(t, "timeout receiving publication")
	case <-done:
	}
}

func TestClientSubscribeReceivePublication_ChannelBatching_BatchSize(t *testing.T) {
	t.Parallel()
	node := defaultTestNode()
	node.config.GetChannelBatchConfig = func(channel string) (ChannelBatchConfig, bool) {
		return ChannelBatchConfig{
			MaxBatchSize: 1,
			WriteDelay:   0,
		}, true
	}
	defer func() { _ = node.Shutdown(context.Background()) }()
	transport := newTestTransport(func() {})
	transport.sink = make(chan []byte, 100)
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
	client, _ := newClient(newCtx, node, transport)

	connectClientV2(t, client)

	rwWrapper := testReplyWriterWrapper()

	client.channels["test"] = ChannelContext{}
	subCtx := client.subscribeCmd(&protocol.SubscribeRequest{
		Channel: "test",
	}, SubscribeReply{}, &protocol.Command{}, false, time.Now(), rwWrapper.rw)
	require.Nil(t, subCtx.disconnect)
	require.Nil(t, rwWrapper.replies[0].Error)

	done := make(chan struct{})
	go func() {
		for data := range transport.sink {
			if strings.Contains(string(data), "test message") {
				close(done)
			}
		}
	}()

	_, err := node.Publish("test", []byte(`{"text": "test message"}`))
	require.NoError(t, err)

	select {
	case <-time.After(time.Second):
		require.Fail(t, "timeout receiving publication")
	case <-done:
	}
}
