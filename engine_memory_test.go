package centrifuge

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/require"
)

func testMemoryEngine() *MemoryEngine {
	n, _ := New(Config{})
	e, _ := NewMemoryEngine(n, MemoryEngineConfig{})
	n.SetEngine(e)
	err := n.Run()
	if err != nil {
		panic(err)
	}
	return e
}

func newTestPublication() *Publication {
	return &Publication{Data: []byte("{}")}
}

func TestMemoryEnginePublishHistory(t *testing.T) {
	e := testMemoryEngine()

	require.NotEqual(t, nil, e.historyHub)
	require.NotEqual(t, nil, e.presenceHub)

	err := e.Publish("channel", newTestPublication(), nil)
	require.NoError(t, err)

	require.NoError(t, e.AddPresence("channel", "uid", &ClientInfo{}, time.Second))
	p, err := e.Presence("channel")
	require.NoError(t, err)
	require.Equal(t, 1, len(p))
	require.NoError(t, e.RemovePresence("channel", "uid"))

	pub := newTestPublication()
	pub.UID = "test UID"

	// test adding history.
	_, err = e.AddHistory("channel", pub, &ChannelOptions{HistorySize: 4, HistoryLifetime: 1})
	require.NoError(t, err)
	h, _, err := e.History("channel", HistoryFilter{
		Limit: -1,
		Since: nil,
	})
	require.NoError(t, err)
	require.Equal(t, 1, len(h))
	require.Equal(t, h[0].UID, "test UID")

	// test history limit.
	_, err = e.AddHistory("channel", pub, &ChannelOptions{HistorySize: 4, HistoryLifetime: 1})
	require.NoError(t, err)
	_, err = e.AddHistory("channel", pub, &ChannelOptions{HistorySize: 4, HistoryLifetime: 1})
	require.NoError(t, err)
	_, err = e.AddHistory("channel", pub, &ChannelOptions{HistorySize: 4, HistoryLifetime: 1})
	require.NoError(t, err)
	h, _, err = e.History("channel", HistoryFilter{
		Limit: 2,
		Since: nil,
	})
	require.NoError(t, err)
	require.Equal(t, 2, len(h))

	// test history limit greater than history size
	_, err = e.AddHistory("channel", pub, &ChannelOptions{HistorySize: 1, HistoryLifetime: 1})
	require.NoError(t, err)
	_, err = e.AddHistory("channel", pub, &ChannelOptions{HistorySize: 1, HistoryLifetime: 1})
	require.NoError(t, err)
	_, err = e.AddHistory("channel", pub, &ChannelOptions{HistorySize: 1, HistoryLifetime: 1})
	require.NoError(t, err)
	h, _, err = e.History("channel", HistoryFilter{
		Limit: 2,
		Since: nil,
	})
	require.NoError(t, err)
	require.Equal(t, 1, len(h))
}

func TestMemoryEngineSubscribeUnsubscribe(t *testing.T) {
	e := testMemoryEngine()
	require.NoError(t, e.Subscribe("channel"))
	require.NoError(t, e.Unsubscribe("channel"))
}

func TestMemoryPresenceHub(t *testing.T) {
	h := newPresenceHub()
	require.Equal(t, 0, len(h.presence))

	testCh1 := "channel1"
	testCh2 := "channel2"
	uid := "uid"

	info := &ClientInfo{
		User:   "user",
		Client: "client",
	}

	_ = h.add(testCh1, uid, info)
	require.Equal(t, 1, len(h.presence))
	_ = h.add(testCh2, uid, info)
	require.Equal(t, 2, len(h.presence))
	_ = h.remove(testCh1, uid)
	// remove non existing must not fail
	err := h.remove(testCh1, uid)
	require.Equal(t, nil, err)
	require.Equal(t, 1, len(h.presence))
	p, err := h.get(testCh1)
	require.Equal(t, nil, err)
	require.Equal(t, 0, len(p))
	p, err = h.get(testCh2)
	require.Equal(t, nil, err)
	require.Equal(t, 1, len(p))
}

func TestMemoryHistoryHub(t *testing.T) {
	h := newHistoryHub(0)
	h.runCleanups()
	h.RLock()
	require.Equal(t, 0, len(h.streams))
	h.RUnlock()
	ch1 := "channel1"
	ch2 := "channel2"
	pub := newTestPublication()
	_, _ = h.add(ch1, pub, &ChannelOptions{HistorySize: 1, HistoryLifetime: 1})
	_, _ = h.add(ch1, pub, &ChannelOptions{HistorySize: 1, HistoryLifetime: 1})
	_, _ = h.add(ch2, pub, &ChannelOptions{HistorySize: 2, HistoryLifetime: 1})

	_, _ = h.add(ch2, pub, &ChannelOptions{HistorySize: 2, HistoryLifetime: 1})

	hist, _, err := h.get(ch1, HistoryFilter{
		Limit: -1,
	})
	require.Equal(t, nil, err)
	require.Equal(t, 1, len(hist))
	hist, _, err = h.get(ch2, HistoryFilter{
		Limit: -1,
	})
	require.Equal(t, nil, err)
	require.Equal(t, 2, len(hist))
	time.Sleep(2 * time.Second)

	// test that stream data cleaned up by periodic task
	h.RLock()
	require.Equal(t, 2, len(h.streams))
	items, top, err := h.streams[ch1].Get(0, -1)
	require.NoError(t, err)
	require.Nil(t, items)
	require.NotZero(t, top)
	items, top, err = h.streams[ch2].Get(0, -1)
	require.NoError(t, err)
	require.Nil(t, items)
	require.NotZero(t, top)
	h.RUnlock()
	hist, _, err = h.get(ch1, HistoryFilter{
		Limit: -1,
	})
	require.Equal(t, nil, err)
	require.Equal(t, 0, len(hist))

	// test history messages limit
	_, _ = h.add(ch1, pub, &ChannelOptions{HistorySize: 10, HistoryLifetime: 1})
	_, _ = h.add(ch1, pub, &ChannelOptions{HistorySize: 10, HistoryLifetime: 1})
	_, _ = h.add(ch1, pub, &ChannelOptions{HistorySize: 10, HistoryLifetime: 1})
	_, _ = h.add(ch1, pub, &ChannelOptions{HistorySize: 10, HistoryLifetime: 1})
	hist, _, err = h.get(ch1, HistoryFilter{
		Limit: -1,
	})
	require.Equal(t, nil, err)
	require.Equal(t, 4, len(hist))
	hist, _, err = h.get(ch1, HistoryFilter{
		Limit: 1,
	})
	require.Equal(t, nil, err)
	require.Equal(t, 1, len(hist))

	// test history limit greater than history size
	_, _ = h.add(ch1, pub, &ChannelOptions{HistorySize: 1, HistoryLifetime: 1})
	_, _ = h.add(ch1, pub, &ChannelOptions{HistorySize: 1, HistoryLifetime: 1})
	hist, _, err = h.get(ch1, HistoryFilter{
		Limit: 2,
	})
	require.Equal(t, nil, err)
	require.Equal(t, 1, len(hist))
}

func TestMemoryHistoryHubSequenceTTL(t *testing.T) {
	h := newHistoryHub(1 * time.Second)
	h.runCleanups()

	ch1 := "channel1"
	ch2 := "channel2"
	pub := newTestPublication()
	_, _ = h.add(ch1, pub, &ChannelOptions{HistorySize: 1, HistoryLifetime: 1})
	_, _ = h.add(ch1, pub, &ChannelOptions{HistorySize: 1, HistoryLifetime: 1})
	_, _ = h.add(ch2, pub, &ChannelOptions{HistorySize: 2, HistoryLifetime: 1})
	_, _ = h.add(ch2, pub, &ChannelOptions{HistorySize: 2, HistoryLifetime: 1})
	h.RLock()
	require.Equal(t, 2, len(h.streams))
	h.RUnlock()

	time.Sleep(2 * time.Second)

	// test that stream cleaned up by periodic task
	h.RLock()
	require.Equal(t, 0, len(h.streams))
	h.RUnlock()
}

func BenchmarkMemoryEnginePublish_SingleChannel(b *testing.B) {
	e := testMemoryEngine()
	rawData := Raw([]byte(`{"bench": true}`))
	pub := &Publication{UID: "test UID", Data: rawData}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := e.Publish("channel", pub, &ChannelOptions{HistorySize: 0, HistoryLifetime: 0})
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkMemoryEnginePublish_SingleChannel_Parallel(b *testing.B) {
	e := testMemoryEngine()
	rawData := Raw([]byte(`{"bench": true}`))
	pub := &Publication{UID: "test UID", Data: rawData}
	b.SetParallelism(128)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			err := e.Publish("channel", pub, &ChannelOptions{HistorySize: 0, HistoryLifetime: 0})
			if err != nil {
				panic(err)
			}
		}
	})
}

func BenchmarkMemoryEnginePublish_WithHistory_SingleChannel(b *testing.B) {
	e := testMemoryEngine()
	rawData := Raw([]byte(`{"bench": true}`))
	pub := &Publication{UID: "test-uid", Data: rawData}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		chOpts := &ChannelOptions{HistorySize: 100, HistoryLifetime: 100}
		var err error
		pub, err = e.AddHistory("channel", pub, chOpts)
		if err != nil {
			panic(err)
		}
		err = e.Publish("channel", pub, chOpts)
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkMemoryEnginePublish_WithHistory_SingleChannel_Parallel(b *testing.B) {
	e := testMemoryEngine()
	rawData := Raw([]byte(`{"bench": true}`))
	chOpts := &ChannelOptions{HistorySize: 100, HistoryLifetime: 100}
	b.SetParallelism(128)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			pub := &Publication{UID: "test-uid", Data: rawData}
			var err error
			pub, err = e.AddHistory("channel", pub, chOpts)
			if err != nil {
				b.Fatal(err)
			}
			err = e.Publish("channel", pub, chOpts)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkMemoryEngineAddPresence_SingleChannel(b *testing.B) {
	e := testMemoryEngine()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := e.AddPresence("channel", "uid", &ClientInfo{}, 300*time.Second)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMemoryEngineAddPresence_SingleChannel_Parallel(b *testing.B) {
	e := testMemoryEngine()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			err := e.AddPresence("channel", "uid", &ClientInfo{}, 300*time.Second)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkMemoryEnginePresence_SingleChannel(b *testing.B) {
	e := testMemoryEngine()
	_ = e.AddPresence("channel", "uid", &ClientInfo{}, 300*time.Second)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := e.Presence("channel")
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMemoryEnginePresence_SingleChannel_Parallel(b *testing.B) {
	e := testMemoryEngine()
	_ = e.AddPresence("channel", "uid", &ClientInfo{}, 300*time.Second)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := e.Presence("channel")
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkMemoryEngineHistory_SingleChannel(b *testing.B) {
	e := testMemoryEngine()
	rawData := Raw([]byte("{}"))
	pub := &Publication{UID: "test UID", Data: rawData}
	for i := 0; i < 4; i++ {
		_, _ = e.AddHistory("channel", pub, &ChannelOptions{HistorySize: 4, HistoryLifetime: 300})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, err := e.History("channel", HistoryFilter{
			Limit: -1,
			Since: nil,
		})
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMemoryEngineHistory_SingleChannel_Parallel(b *testing.B) {
	e := testMemoryEngine()
	rawData := Raw([]byte("{}"))
	pub := &Publication{UID: "test-uid", Data: rawData}
	for i := 0; i < 4; i++ {
		_, _ = e.AddHistory("channel", pub, &ChannelOptions{HistorySize: 4, HistoryLifetime: 300})
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _, err := e.History("channel", HistoryFilter{
				Limit: -1,
				Since: nil,
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkMemoryEngineRecover_SingleChannel_Parallel(b *testing.B) {
	e := testMemoryEngine()
	rawData := Raw([]byte("{}"))
	numMessages := 100
	for i := 1; i <= numMessages; i++ {
		pub := &Publication{Data: rawData}
		_, _ = e.AddHistory("channel", pub, &ChannelOptions{HistorySize: numMessages, HistoryLifetime: 300, HistoryRecover: true})
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _, err := e.History("channel", HistoryFilter{
				Limit: -1,
				Since: &StreamPosition{Seq: uint32(numMessages - 5), Gen: 0, Epoch: ""},
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

var recoverTests = []struct {
	Name            string
	HistorySize     int
	HistoryLifetime int
	NumPublications int
	SinceSeq        uint32
	NumRecovered    int
	Sleep           int
	Recovered       bool
}{
	{"empty_stream", 10, 60, 0, 0, 0, 0, true},
	{"from_position", 10, 60, 10, 8, 2, 0, true},
	{"from_position_that_is_too_far", 10, 60, 20, 8, 10, 0, false},
	{"same_position_no_history_expected", 10, 60, 7, 7, 0, 0, true},
	{"empty_position_recover_expected", 10, 60, 4, 0, 4, 0, true},
	{"from_position_in_expired_stream", 10, 1, 10, 8, 0, 3, false},
	{"from_same_position_in_expired_stream", 10, 1, 1, 1, 0, 3, true},
}

func TestMemoryClientSubscribeRecover(t *testing.T) {
	for _, tt := range recoverTests {
		t.Run(tt.Name, func(t *testing.T) {
			node := nodeWithMemoryEngine()

			config := node.Config()
			config.HistorySize = tt.HistorySize
			config.HistoryLifetime = tt.HistoryLifetime
			config.HistoryRecover = true
			_ = node.Reload(config)

			transport := newTestTransport()
			ctx := context.Background()
			newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
			client, _ := NewClient(newCtx, node, transport)

			channel := "test_recovery_memory_" + tt.Name

			for i := 1; i <= tt.NumPublications; i++ {
				_ = node.Publish(channel, []byte(`{"n": `+strconv.Itoa(i)+`}`))
			}

			time.Sleep(time.Duration(tt.Sleep) * time.Second)

			connectClient(t, client)

			var replies []*protocol.Reply
			rw := testReplyWriter(&replies)

			_, recoveryPosition, _ := node.historyManager.History(channel, HistoryFilter{
				Limit: 0,
				Since: nil,
			})

			subCtx := client.subscribeCmd(&protocol.SubscribeRequest{
				Channel: channel,
				Recover: true,
				Seq:     tt.SinceSeq,
				Gen:     recoveryPosition.Gen,
				Epoch:   recoveryPosition.Epoch,
			}, rw, false)
			require.Nil(t, subCtx.disconnect)
			require.Nil(t, replies[0].Error)
			res := extractSubscribeResult(replies)
			require.Equal(t, tt.NumRecovered, len(res.Publications))
			require.Equal(t, tt.Recovered, res.Recovered)

			_ = node.Shutdown(context.Background())
		})
	}
}
