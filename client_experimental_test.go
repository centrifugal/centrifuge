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

func BenchmarkPerChannelWriter_FlushLatest(b *testing.B) {
	const numChannels = 10
	const numKeys = 50

	flushFn := func(items []queue.Item) error {
		return nil
	}

	w := newPerChannelWriter(flushFn)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		channelName := "channel-" + strconv.Itoa(i%numChannels)
		key := "key-" + strconv.Itoa(i%numKeys)
		item := queue.Item{Channel: channelName, Key: key, FrameType: protocol.FrameTypePushPublication}
		w.Add(item, channelName, ChannelBatchConfig{MaxDelay: 10 * time.Millisecond, MaxSize: 128, FlushLatestPublication: true})
	}
	w.Close(true)
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

func TestChannelWriter_FlushLatestPerKey(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	var flushed []queue.Item

	w := newChannelWriter(func(items []queue.Item) error {
		mu.Lock()
		flushed = append(flushed, items...)
		mu.Unlock()
		return nil
	})

	config := ChannelBatchConfig{
		FlushLatestPublication: true,
		MaxDelay:               100 * time.Millisecond,
	}

	// Publish 3 updates to key "a", 2 updates to key "b", 1 to key "c".
	w.Add(queue.Item{Data: []byte("a1"), Key: "a", FrameType: protocol.FrameTypePushPublication}, config)
	w.Add(queue.Item{Data: []byte("b1"), Key: "b", FrameType: protocol.FrameTypePushPublication}, config)
	w.Add(queue.Item{Data: []byte("a2"), Key: "a", FrameType: protocol.FrameTypePushPublication}, config)
	w.Add(queue.Item{Data: []byte("c1"), Key: "c", FrameType: protocol.FrameTypePushPublication}, config)
	w.Add(queue.Item{Data: []byte("b2"), Key: "b", FrameType: protocol.FrameTypePushPublication}, config)
	w.Add(queue.Item{Data: []byte("a3"), Key: "a", FrameType: protocol.FrameTypePushPublication}, config)

	// Close triggers flush.
	w.close(true)

	mu.Lock()
	defer mu.Unlock()

	// Should have 3 items: latest for "c" (unchanged), "b" (updated), "a" (updated last).
	// Order: "c" stayed at position from first insert, "b" moved after "c", "a" moved to end.
	require.Len(t, flushed, 3)
	require.Equal(t, "c", flushed[0].Key)
	require.Equal(t, []byte("c1"), flushed[0].Data)
	require.Equal(t, "b", flushed[1].Key)
	require.Equal(t, []byte("b2"), flushed[1].Data)
	require.Equal(t, "a", flushed[2].Key)
	require.Equal(t, []byte("a3"), flushed[2].Data)
}

func TestChannelWriter_FlushLatestPerKey_EmptyKeyFallback(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	var flushed []queue.Item

	w := newChannelWriter(func(items []queue.Item) error {
		mu.Lock()
		flushed = append(flushed, items...)
		mu.Unlock()
		return nil
	})

	config := ChannelBatchConfig{
		FlushLatestPublication: true,
		MaxDelay:               100 * time.Millisecond,
	}

	// Regular publications without keys — should coalesce to single latest (backward compat).
	w.Add(queue.Item{Data: []byte("msg1"), FrameType: protocol.FrameTypePushPublication}, config)
	w.Add(queue.Item{Data: []byte("msg2"), FrameType: protocol.FrameTypePushPublication}, config)
	w.Add(queue.Item{Data: []byte("msg3"), FrameType: protocol.FrameTypePushPublication}, config)

	w.close(true)

	mu.Lock()
	defer mu.Unlock()

	// Should have exactly 1 item — the latest.
	require.Len(t, flushed, 1)
	require.Equal(t, []byte("msg3"), flushed[0].Data)
}

func TestChannelWriter_FlushLatestPerKey_MixedWithNonPub(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	var flushed []queue.Item

	w := newChannelWriter(func(items []queue.Item) error {
		mu.Lock()
		flushed = append(flushed, items...)
		mu.Unlock()
		return nil
	})

	config := ChannelBatchConfig{
		FlushLatestPublication: true,
		MaxDelay:               100 * time.Millisecond,
	}

	// Mix of join (non-pub, goes to buffer) and keyed publications.
	w.Add(queue.Item{Data: []byte("join1"), FrameType: protocol.FrameTypePushJoin}, config)
	w.Add(queue.Item{Data: []byte("a1"), Key: "a", FrameType: protocol.FrameTypePushPublication}, config)
	w.Add(queue.Item{Data: []byte("a2"), Key: "a", FrameType: protocol.FrameTypePushPublication}, config)
	w.Add(queue.Item{Data: []byte("b1"), Key: "b", FrameType: protocol.FrameTypePushPublication}, config)

	w.close(true)

	mu.Lock()
	defer mu.Unlock()

	// join1 in buffer + 2 per-key pubs. Buffer first, then pubs in last-updated order.
	require.Len(t, flushed, 3)
	require.Equal(t, []byte("join1"), flushed[0].Data)
	require.Equal(t, "a", flushed[1].Key)
	require.Equal(t, []byte("a2"), flushed[1].Data)
	require.Equal(t, "b", flushed[2].Key)
	require.Equal(t, []byte("b1"), flushed[2].Data)
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

// TestClientWritePublication exercises the experimental WritePublication API for both
// JSON and Protobuf, bidi and unidirectional. It also verifies the no-subscription error.
func TestClientWritePublication(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name           string
		protocolType   ProtocolType
		unidirectional bool
	}{
		{"JSON-bidi", ProtocolTypeJSON, false},
		{"JSON-uni", ProtocolTypeJSON, true},
		{"Protobuf-bidi", ProtocolTypeProtobuf, false},
		{"Protobuf-uni", ProtocolTypeProtobuf, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			node := defaultTestNode()
			defer func() { _ = node.Shutdown(context.Background()) }()

			ctx, cancelFn := context.WithCancel(context.Background())
			transport := newTestTransport(cancelFn)
			transport.setProtocolType(tc.protocolType)
			transport.setProtocolVersion(ProtocolVersion2)
			transport.setUnidirectional(tc.unidirectional)
			sink := make(chan []byte, 16)
			transport.setSink(sink)

			node.OnConnect(func(c *Client) {
				c.OnSubscribe(func(_ SubscribeEvent, cb SubscribeCallback) {
					cb(SubscribeReply{}, nil)
				})
			})

			client := newTestConnectedClientWithTransport(t, ctx, node, transport, "user-write-pub")
			subscribeClientV2(t, client, "writepub")

			// drain any subscribe push.
			drainTransport(sink, 200*time.Millisecond)

			err := client.WritePublication("writepub",
				&Publication{Data: []byte(`{"value":"hello"}`)},
				StreamPosition{},
			)
			require.NoError(t, err)

			received := waitForPayload(t, sink, "hello", 2*time.Second)
			require.True(t, received, "publication payload not delivered")

			// No subscription -> well-formed error returned without disconnecting.
			err = client.WritePublication("not-subscribed",
				&Publication{Data: []byte(`{"value":"x"}`)},
				StreamPosition{},
			)
			require.ErrorIs(t, err, errNoSubscription)
		})
	}
}

// TestClientStateSnapshot exercises the StateSnapshot call with and without a handler
// installed.
func TestClientStateSnapshot(t *testing.T) {
	t.Parallel()
	node := defaultTestNode()
	defer func() { _ = node.Shutdown(context.Background()) }()
	client := newTestClient(t, node, "u")

	// No handler -> nil, nil.
	snap, err := client.StateSnapshot()
	require.NoError(t, err)
	require.Nil(t, snap)

	// Handler returns a value.
	client.OnStateSnapshot(func() (any, error) {
		return map[string]int{"queue_len": 5}, nil
	})
	snap, err = client.StateSnapshot()
	require.NoError(t, err)
	require.Equal(t, map[string]int{"queue_len": 5}, snap)
}

// TestPerChannelWriterDelWriter verifies that delWriter both flushes pending items
// (when requested) and removes the writer from the per-channel map.
func TestPerChannelWriterDelWriter(t *testing.T) {
	t.Parallel()

	flushed := make(chan int, 4)
	pcw := newPerChannelWriter(func(items []queue.Item) error {
		flushed <- len(items)
		return nil
	})

	// Add an item under one channel and one with no flush requested.
	pcw.Add(queue.Item{Data: []byte("a")}, "ch1", ChannelBatchConfig{MaxSize: 100, MaxDelay: time.Hour})
	// Sanity: writer exists.
	require.NotNil(t, pcw.getWriter("ch1"))

	// delWriter without flushing must drop pending items.
	pcw.delWriter("ch1", false)
	select {
	case <-flushed:
		t.Fatal("did not expect flush when flushRemaining=false")
	case <-time.After(50 * time.Millisecond):
	}

	// Add to a different channel and remove with flush.
	pcw.Add(queue.Item{Data: []byte("b")}, "ch2", ChannelBatchConfig{MaxSize: 100, MaxDelay: time.Hour})
	pcw.delWriter("ch2", true)
	select {
	case n := <-flushed:
		require.Equal(t, 1, n)
	case <-time.After(time.Second):
		t.Fatal("expected flush of remaining items")
	}

	// Removing a non-existent channel must be a no-op.
	pcw.delWriter("never-existed", false)
	pcw.Close(false)
}
