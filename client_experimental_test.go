package centrifuge

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/centrifugal/centrifuge/internal/queue"

	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/require"
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
		w.Add(item, channelName, ChannelBatchConfig{MaxDelay: 10 * time.Millisecond, MaxSize: 128})
	}
	w.Close(true)
	wg.Wait() // Wait for all messages to be flushed.
}

func TestClientSubscribeReceivePublication_ChannelBatching_Delay(t *testing.T) {
	t.Parallel()
	node := defaultTestNode()
	node.config.GetChannelBatchConfig = func(channel string) ChannelBatchConfig {
		return ChannelBatchConfig{
			MaxSize:  0,
			MaxDelay: 10 * time.Millisecond,
		}
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
	node.config.GetChannelBatchConfig = func(channel string) ChannelBatchConfig {
		return ChannelBatchConfig{
			MaxSize:  1,
			MaxDelay: 0,
		}
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

func TestClientSubscribeReceivePublication_ChannelBatching_FlushLatestOnly(t *testing.T) {
	t.Parallel()
	node := defaultTestNode()
	node.config.GetChannelBatchConfig = func(channel string) ChannelBatchConfig {
		return ChannelBatchConfig{
			MaxSize:                0,
			MaxDelay:               100 * time.Millisecond,
			FlushLatestPublication: true,
		}
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
			if strings.Contains(string(data), "test message 1") {
				panic("should not receive first message")
			}
			if strings.Contains(string(data), "test message 2") {
				close(done)
			}
		}
	}()

	_, err := node.Publish("test", []byte(`{"text": "test message 1"}`))
	require.NoError(t, err)
	_, err = node.Publish("test", []byte(`{"text": "test message 2"}`))
	require.NoError(t, err)

	select {
	case <-time.After(time.Second):
		require.Fail(t, "timeout receiving publication")
	case <-done:
	}
}

// timerCanceler implements the TimerCanceler interface.
type timerCanceler struct {
	timer   *time.Timer
	mu      sync.Mutex
	stopped bool
}

func (c *timerCanceler) Cancel() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.stopped {
		c.timer.Stop()
		c.stopped = true
	}
}

// testTimerScheduler is a test implementation of the TimerScheduler interface.
type testTimerScheduler struct{}

func (s *testTimerScheduler) ScheduleTimer(duration time.Duration, callback func()) TimerCanceler {
	canceler := &timerCanceler{}
	canceler.timer = time.AfterFunc(duration, func() {
		canceler.mu.Lock()
		defer canceler.mu.Unlock()
		if !canceler.stopped {
			go callback()
		}
	})
	return canceler
}

func TestClientLevelPingCustomTimerScheduler(t *testing.T) {
	t.Parallel()
	node := defaultTestNode()
	node.timerScheduler = &testTimerScheduler{}
	defer func() { _ = node.Shutdown(context.Background()) }()
	done := make(chan struct{})
	node.OnConnecting(func(context.Context, ConnectEvent) (ConnectReply, error) {
		return ConnectReply{
			PingPongConfig: &PingPongConfig{
				PingInterval: 5 * time.Second,
				PongTimeout:  3 * time.Second,
			},
		}, nil
	})
	node.OnConnect(func(client *Client) {
		client.OnDisconnect(func(event DisconnectEvent) {
			require.Equal(t, DisconnectNoPong.Code, event.Disconnect.Code)
			close(done)
		})
	})
	ctx, cancelFn := context.WithCancel(context.Background())
	transport := newTestTransport(cancelFn)
	transport.setProtocolVersion(ProtocolVersion2)
	transport.setPing(0, 0)
	client := newTestClientCustomTransport(t, ctx, node, transport, "42")
	connectClientV2(t, client)
	select {
	case <-done:
	case <-time.After(9 * time.Second):
		t.Fatal("no disconnect in timeout")
	}
}
