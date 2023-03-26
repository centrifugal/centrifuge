package centrifuge

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/require"
)

func testMemoryBroker() *MemoryBroker {
	n, _ := New(Config{
		LogLevel:   LogLevelDebug,
		LogHandler: func(entry LogEntry) {},
	})
	e, _ := NewMemoryBroker(n, MemoryBrokerConfig{})
	n.SetBroker(e)
	err := n.Run()
	if err != nil {
		panic(err)
	}
	return e
}

func newTestPublication() *Publication {
	return &Publication{Data: []byte("{}")}
}

func testPublicationData() []byte {
	return []byte("{}")
}

func TestMemoryBrokerPublishHistory(t *testing.T) {
	e := testMemoryBroker()
	defer func() { _ = e.node.Shutdown(context.Background()) }()

	require.NotEqual(t, nil, e.historyHub)

	_, err := e.Publish("channel", testPublicationData(), PublishOptions{})
	require.NoError(t, err)

	err = e.PublishJoin("channel", &ClientInfo{})
	require.NoError(t, err)

	err = e.PublishLeave("channel", &ClientInfo{})
	require.NoError(t, err)

	pub := newTestPublication()

	// test adding history.
	_, err = e.Publish("channel", testPublicationData(), PublishOptions{HistorySize: 4, HistoryTTL: time.Second})
	require.NoError(t, err)
	pubs, _, err := e.History("channel", HistoryOptions{
		Filter: HistoryFilter{
			Limit: -1,
			Since: nil,
		},
	})
	require.NoError(t, err)
	require.Equal(t, 1, len(pubs))
	require.Equal(t, pubs[0].Data, pub.Data)

	// test history limit.
	_, err = e.Publish("channel", testPublicationData(), PublishOptions{HistorySize: 4, HistoryTTL: time.Second})
	require.NoError(t, err)
	_, err = e.Publish("channel", testPublicationData(), PublishOptions{HistorySize: 4, HistoryTTL: time.Second})
	require.NoError(t, err)
	_, err = e.Publish("channel", testPublicationData(), PublishOptions{HistorySize: 4, HistoryTTL: time.Second})
	require.NoError(t, err)
	pubs, _, err = e.History("channel", HistoryOptions{
		Filter: HistoryFilter{
			Limit: 2,
			Since: nil,
		},
	})
	require.NoError(t, err)
	require.Equal(t, 2, len(pubs))

	// test history limit greater than history size
	_, err = e.Publish("channel", testPublicationData(), PublishOptions{HistorySize: 1, HistoryTTL: time.Second})
	require.NoError(t, err)
	_, err = e.Publish("channel", testPublicationData(), PublishOptions{HistorySize: 1, HistoryTTL: time.Second})
	require.NoError(t, err)
	_, err = e.Publish("channel", testPublicationData(), PublishOptions{HistorySize: 1, HistoryTTL: time.Second})
	require.NoError(t, err)
	pubs, _, err = e.History("channel", HistoryOptions{
		Filter: HistoryFilter{
			Limit: 2,
			Since: nil,
		},
	})
	require.NoError(t, err)
	require.Equal(t, 1, len(pubs))
}

func TestMemoryEngineSubscribeUnsubscribe(t *testing.T) {
	e := testMemoryBroker()
	defer func() { _ = e.node.Shutdown(context.Background()) }()
	require.NoError(t, e.Subscribe("channel"))
	require.NoError(t, e.Unsubscribe("channel"))
}

func TestMemoryHistoryHub(t *testing.T) {
	t.Parallel()
	h := newHistoryHub(0, make(chan struct{}))
	h.runCleanups()
	h.RLock()
	require.Equal(t, 0, len(h.streams))
	h.RUnlock()
	ch1 := "channel1"
	ch2 := "channel2"
	pub := newTestPublication()
	_, _ = h.add(ch1, pub, PublishOptions{HistorySize: 1, HistoryTTL: time.Second})
	_, _ = h.add(ch1, pub, PublishOptions{HistorySize: 1, HistoryTTL: time.Second})
	_, _ = h.add(ch2, pub, PublishOptions{HistorySize: 2, HistoryTTL: time.Second})
	_, _ = h.add(ch2, pub, PublishOptions{HistorySize: 2, HistoryTTL: time.Second})

	hist, _, err := h.get(ch1, HistoryOptions{
		Filter: HistoryFilter{
			Limit: -1,
		},
	})
	require.Equal(t, nil, err)
	require.Equal(t, 1, len(hist))
	hist, _, err = h.get(ch2, HistoryOptions{
		Filter: HistoryFilter{
			Limit: -1,
		},
	})
	require.Equal(t, nil, err)
	require.Equal(t, 2, len(hist))
	time.Sleep(2 * time.Second)

	// test that stream data cleaned up by periodic task
	h.RLock()
	require.Equal(t, 2, len(h.streams))
	items, top, err := h.streams[ch1].Get(0, false, -1, false)
	require.NoError(t, err)
	require.Nil(t, items)
	require.NotZero(t, top)
	items, top, err = h.streams[ch2].Get(0, false, -1, false)
	require.NoError(t, err)
	require.Nil(t, items)
	require.NotZero(t, top)
	h.RUnlock()
	hist, _, err = h.get(ch1, HistoryOptions{
		Filter: HistoryFilter{
			Limit: -1,
		},
	})
	require.Equal(t, nil, err)
	require.Equal(t, 0, len(hist))

	// test history messages limit
	_, _ = h.add(ch1, pub, PublishOptions{HistorySize: 10, HistoryTTL: time.Second})
	_, _ = h.add(ch1, pub, PublishOptions{HistorySize: 10, HistoryTTL: time.Second})
	_, _ = h.add(ch1, pub, PublishOptions{HistorySize: 10, HistoryTTL: time.Second})
	_, _ = h.add(ch1, pub, PublishOptions{HistorySize: 10, HistoryTTL: time.Second})
	hist, _, err = h.get(ch1, HistoryOptions{
		Filter: HistoryFilter{
			Limit: -1,
		},
	})
	require.Equal(t, nil, err)
	require.Equal(t, 4, len(hist))
	hist, _, err = h.get(ch1, HistoryOptions{
		Filter: HistoryFilter{
			Limit: 1,
		},
	})
	require.Equal(t, nil, err)
	require.Equal(t, 1, len(hist))

	// test history limit greater than history size
	_, _ = h.add(ch1, pub, PublishOptions{HistorySize: 1, HistoryTTL: time.Second})
	_, _ = h.add(ch1, pub, PublishOptions{HistorySize: 1, HistoryTTL: time.Second})
	hist, _, err = h.get(ch1, HistoryOptions{
		Filter: HistoryFilter{
			Limit: 2,
		},
	})
	require.Equal(t, nil, err)
	require.Equal(t, 1, len(hist))
}

func TestMemoryHistoryHubMetaTTL(t *testing.T) {
	h := newHistoryHub(1*time.Second, make(chan struct{}))
	h.runCleanups()

	ch1 := "channel1"
	ch2 := "channel2"
	pub := newTestPublication()
	h.RLock()
	require.Equal(t, int64(0), h.nextRemoveCheck)
	h.RUnlock()
	_, _ = h.add(ch1, pub, PublishOptions{HistorySize: 1, HistoryTTL: time.Second})
	_, _ = h.add(ch1, pub, PublishOptions{HistorySize: 1, HistoryTTL: time.Second})
	_, _ = h.add(ch2, pub, PublishOptions{HistorySize: 2, HistoryTTL: time.Second})
	_, _ = h.add(ch2, pub, PublishOptions{HistorySize: 2, HistoryTTL: time.Second})
	h.RLock()
	require.True(t, h.nextRemoveCheck > 0)
	require.Equal(t, 2, len(h.streams))
	h.RUnlock()
	pubs, _, err := h.get(ch1, HistoryOptions{
		Filter: HistoryFilter{Limit: -1},
	})
	require.NoError(t, err)
	require.Len(t, pubs, 1)
	pubs, _, err = h.get(ch2, HistoryOptions{
		Filter: HistoryFilter{Limit: -1},
	})
	require.NoError(t, err)
	require.Len(t, pubs, 2)

	time.Sleep(2 * time.Second)

	// test that stream cleaned up by periodic task
	h.RLock()
	require.Equal(t, 0, len(h.streams))
	require.Equal(t, int64(0), h.nextRemoveCheck)
	h.RUnlock()
}

func TestMemoryBrokerRecover(t *testing.T) {
	e := testMemoryBroker()
	defer func() { _ = e.node.Shutdown(context.Background()) }()

	for i := 0; i < 5; i++ {
		_, err := e.Publish("channel", testPublicationData(), PublishOptions{HistorySize: 10, HistoryTTL: 2 * time.Second})
		require.NoError(t, err)
	}

	_, streamTop, err := e.History("channel", HistoryOptions{
		Filter: HistoryFilter{
			Limit: 0,
			Since: nil,
		},
	})
	require.NoError(t, err)

	pubs, _, err := e.History("channel", HistoryOptions{
		Filter: HistoryFilter{
			Limit: -1,
			Since: &StreamPosition{Offset: 2, Epoch: streamTop.Epoch},
		},
	})
	require.NoError(t, err)
	require.Equal(t, 3, len(pubs))
	require.Equal(t, uint64(3), pubs[0].Offset)
	require.Equal(t, uint64(4), pubs[1].Offset)
	require.Equal(t, uint64(5), pubs[2].Offset)

	for i := 0; i < 10; i++ {
		_, err := e.Publish("channel", testPublicationData(), PublishOptions{HistorySize: 10, HistoryTTL: 2 * time.Second})
		require.NoError(t, err)
	}

	pubs, _, err = e.History("channel", HistoryOptions{
		Filter: HistoryFilter{
			Limit: -1,
			Since: &StreamPosition{Offset: 0, Epoch: streamTop.Epoch},
		},
	})
	require.NoError(t, err)
	require.Equal(t, 10, len(pubs))

	pubs, _, err = e.History("channel", HistoryOptions{
		Filter: HistoryFilter{
			Limit: -1,
			Since: &StreamPosition{Offset: 100, Epoch: streamTop.Epoch},
		},
	})
	require.NoError(t, err)
	require.Equal(t, 0, len(pubs))

	require.NoError(t, e.RemoveHistory("channel"))
	pubs, _, err = e.History("channel", HistoryOptions{
		Filter: HistoryFilter{
			Limit: -1,
			Since: &StreamPosition{Offset: 2, Epoch: streamTop.Epoch},
		},
	})
	require.NoError(t, err)
	require.Equal(t, 0, len(pubs))
}

func BenchmarkMemoryPublish_1Ch(b *testing.B) {
	e := testMemoryBroker()
	defer func() { _ = e.node.Shutdown(context.Background()) }()

	rawData := protocol.Raw(`{"bench": true}`)
	b.SetParallelism(128)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := e.Publish("channel", rawData, PublishOptions{})
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkMemoryPublish_History_1Ch(b *testing.B) {
	e := testMemoryBroker()
	defer func() { _ = e.node.Shutdown(context.Background()) }()

	rawData := protocol.Raw(`{"bench": true}`)
	chOpts := PublishOptions{HistorySize: 100, HistoryTTL: 60 * time.Second}
	b.SetParallelism(128)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			var err error
			streamTop, err := e.Publish("channel", rawData, chOpts)
			if err != nil {
				b.Fatal(err)
			}
			if streamTop.Offset == 0 {
				b.Fatal("zero offset")
			}
		}
	})
}

func BenchmarkMemoryHistory_1Ch(b *testing.B) {
	e := testMemoryBroker()
	defer func() { _ = e.node.Shutdown(context.Background()) }()

	rawData := protocol.Raw("{}")
	for i := 0; i < 4; i++ {
		_, _ = e.Publish("channel", rawData, PublishOptions{HistorySize: 4, HistoryTTL: 300 * time.Second})
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _, err := e.History("channel", HistoryOptions{
				Filter: HistoryFilter{
					Limit: -1,
					Since: nil,
				},
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkMemoryRecover_1Ch(b *testing.B) {
	e := testMemoryBroker()
	defer func() { _ = e.node.Shutdown(context.Background()) }()

	rawData := protocol.Raw("{}")
	numMessages := 1000
	numMissing := 5
	for i := 1; i <= numMessages; i++ {
		_, _ = e.Publish("channel", rawData, PublishOptions{HistorySize: numMessages, HistoryTTL: 300 * time.Second})
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			pubs, _, err := e.History("channel", HistoryOptions{
				Filter: HistoryFilter{
					Limit: -1,
					Since: &StreamPosition{Offset: uint64(numMessages - numMissing), Epoch: ""},
				},
			})
			if err != nil {
				b.Fatal(err)
			}
			if len(pubs) != numMissing {
				b.Fail()
			}
		}
	})
}

type recoverTest struct {
	Name              string
	HistorySize       int
	HistoryTTLSeconds int
	NumPublications   int
	SinceOffset       uint64
	NumRecovered      int
	Sleep             int
	Limit             int
	Recovered         bool
}

var clientRecoverTests = []recoverTest{
	{"empty_stream", 10, 60, 0, 0, 0, 0, 0, true},
	{"from_position", 10, 60, 10, 8, 2, 0, 0, true},
	{"from_position_limited", 10, 60, 10, 5, 0, 0, 2, false},
	{"from_position_with_server_limit", 10, 60, 10, 5, 0, 0, 1, false},
	{"from_position_that_already_gone", 10, 60, 20, 8, 0, 0, 0, false},
	{"from_position_that_not_exist_yet", 10, 60, 20, 108, 0, 0, 0, false},
	{"same_position_no_pubs_expected", 10, 60, 7, 7, 0, 0, 0, true},
	{"empty_position_recover_expected", 10, 60, 4, 0, 4, 0, 0, true},
	{"from_position_in_expired_stream", 10, 1, 10, 8, 0, 3, 0, false},
	{"from_same_position_in_expired_stream", 10, 1, 1, 1, 0, 3, 0, true},
}

type recoverTestChannel struct {
	ChannelPrefix string
}

var recoverTestChannels = []recoverTestChannel{
	{"test_recovery_memory_offset_"},
}

func TestClientSubscribeRecover(t *testing.T) {
	t.Parallel()
	for _, tt := range clientRecoverTests {
		t.Run(tt.Name, func(t *testing.T) {
			node := defaultNodeNoHandlers()
			node.config.RecoveryMaxPublicationLimit = tt.Limit
			node.OnConnect(func(client *Client) {
				client.OnSubscribe(func(event SubscribeEvent, cb SubscribeCallback) {
					opts := SubscribeOptions{EnableRecovery: true}
					cb(SubscribeReply{Options: opts}, nil)
				})
			})

			for _, recoverTestChannel := range recoverTestChannels {
				channel := recoverTestChannel.ChannelPrefix + tt.Name

				client := newTestClient(t, node, "42")

				for i := 1; i <= tt.NumPublications; i++ {
					_, _ = node.Publish(channel, []byte(`{"n": `+strconv.Itoa(i)+`}`), WithHistory(tt.HistorySize, time.Duration(tt.HistoryTTLSeconds)*time.Second))
				}

				time.Sleep(time.Duration(tt.Sleep) * time.Second)

				connectClientV2(t, client)

				_, streamTop, err := node.broker.History(channel, HistoryOptions{
					Filter: HistoryFilter{
						Limit: 0,
						Since: nil,
					},
				})
				require.NoError(t, err)

				subscribeCmd := &protocol.SubscribeRequest{
					Channel: channel,
					Recover: true,
					Epoch:   streamTop.Epoch,
					Offset:  tt.SinceOffset,
				}

				rwWrapper := testReplyWriterWrapper()

				disconnect := client.handleSubscribe(subscribeCmd, &protocol.Command{}, time.Now(), rwWrapper.rw)
				require.Nil(t, disconnect)
				require.Nil(t, rwWrapper.replies[0].Error)
				res := extractSubscribeResult(rwWrapper.replies)
				require.Equal(t, tt.NumRecovered, len(res.Publications))
				require.Equal(t, tt.Recovered, res.Recovered)
				if len(res.Publications) > 1 {
					require.True(t, res.Publications[0].Offset < res.Publications[1].Offset)
				}
			}
			_ = node.Shutdown(context.Background())
		})
	}
}

const historyIterationChannel = "test"

type historyIterationTest struct {
	NumMessages int
	IterateBy   int
}

func (it *historyIterationTest) prepareHistoryIteration(t testing.TB, node *Node) StreamPosition {
	numMessages := it.NumMessages

	channel := historyIterationChannel

	historyResult, err := node.History(channel)
	require.NoError(t, err)
	startPosition := historyResult.StreamPosition

	for i := 1; i <= numMessages; i++ {
		_, err := node.Publish(channel, []byte(`{}`), WithHistory(numMessages, time.Hour))
		require.NoError(t, err)
	}

	historyResult, err = node.History(channel, WithLimit(NoLimit))
	require.NoError(t, err)
	require.Equal(t, numMessages, len(historyResult.Publications))
	return startPosition
}

func (it *historyIterationTest) testHistoryIteration(t testing.TB, node *Node, startPosition StreamPosition) {
	var (
		n         int
		offset    = startPosition.Offset
		epoch     = startPosition.Epoch
		iterateBy = it.IterateBy
	)
	for {
		res, err := node.History(
			historyIterationChannel,
			WithSince(&StreamPosition{Offset: offset, Epoch: epoch}),
			WithLimit(iterateBy),
		)
		if err != nil {
			t.Fatal(err)
		}
		offset += uint64(iterateBy)
		if len(res.Publications) == 0 {
			break
		}
		n += len(res.Publications)
	}
	if n != it.NumMessages {
		t.Fatal("num messages mismatch")
	}
}

func (it *historyIterationTest) testHistoryIterationReverse(t testing.TB, node *Node, startPosition StreamPosition) {
	var (
		n         int
		epoch     = startPosition.Epoch
		iterateBy = it.IterateBy
	)
	var since *StreamPosition
outer:
	for {
		res, err := node.History(
			historyIterationChannel,
			WithSince(since),
			WithLimit(iterateBy),
			WithReverse(true),
		)
		if err != nil {
			t.Fatal(err)
		}
		var checkOffset uint64
	loop:
		for _, pub := range res.Publications {
			n += 1
			if pub.Offset == startPosition.Offset+1 {
				break outer
			}
			if checkOffset == 0 {
				checkOffset = pub.Offset
				continue loop
			}
			if pub.Offset > checkOffset {
				t.Fatal("incorrect order")
			}
			checkOffset = pub.Offset
		}
		if len(res.Publications) == 0 || len(res.Publications) < iterateBy {
			break
		}
		earliestPub := res.Publications[len(res.Publications)-1]
		since = &StreamPosition{Offset: earliestPub.Offset, Epoch: epoch}
	}
	if n != it.NumMessages {
		t.Fatalf("num messages mismatch, expected %d, got %d", it.NumMessages, n)
	}
}

func TestMemoryBrokerHistoryIteration(t *testing.T) {
	e := testMemoryBroker()
	defer func() { _ = e.node.Shutdown(context.Background()) }()

	it := historyIterationTest{10000, 100}
	startPosition := it.prepareHistoryIteration(t, e.node)
	it.testHistoryIteration(t, e.node, startPosition)
}

func TestMemoryBrokerHistoryIterationReverse(t *testing.T) {
	e := testMemoryBroker()
	defer func() { _ = e.node.Shutdown(context.Background()) }()

	it := historyIterationTest{10000, 100}
	startPosition := it.prepareHistoryIteration(t, e.node)
	it.testHistoryIterationReverse(t, e.node, startPosition)
}

func BenchmarkMemoryBrokerHistoryIteration(b *testing.B) {
	e := testMemoryBroker()
	defer func() { _ = e.node.Shutdown(context.Background()) }()

	it := historyIterationTest{10000, 100}
	startPosition := it.prepareHistoryIteration(b, e.node)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		it.testHistoryIteration(b, e.node, startPosition)
	}
}
