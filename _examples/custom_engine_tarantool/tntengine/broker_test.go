package tntengine

import (
	"context"
	"log"
	"strconv"
	"testing"
	"time"

	"github.com/centrifugal/centrifuge"
	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/require"
)

func newTestTarantoolEngine(tb testing.TB) (*Broker, *PresenceManager) {
	n, _ := centrifuge.New(centrifuge.DefaultConfig)
	var shards []*Shard
	for _, port := range []string{"3301"} {
		shard, err := NewShard(ShardConfig{Addresses: []string{"127.0.0.1:" + port}})
		if err != nil {
			log.Fatal(err)
		}
		shards = append(shards, shard)
	}

	broker, err := NewBroker(n, BrokerConfig{
		UsePolling: false,
		Shards:     shards,
	})
	if err != nil {
		tb.Fatal(err)
	}

	presenceManager, err := NewPresenceManager(n, PresenceManagerConfig{
		Shards: shards,
	})
	if err != nil {
		tb.Fatal(err)
	}

	n.SetBroker(broker)
	n.SetPresenceManager(presenceManager)
	err = n.Run()
	if err != nil {
		tb.Fatal(err)
	}
	return broker, presenceManager
}

type recoverTest struct {
	Name            string
	HistorySize     int
	HistoryLifetime int
	NumPublications int
	SinceOffset     uint64
	NumRecovered    int
	Sleep           int
	Limit           int
	Recovered       bool
}

var recoverTests = []recoverTest{
	{"empty_stream", 10, 60, 0, 0, 0, 0, 0, true},
	{"from_position", 10, 60, 10, 8, 2, 0, 0, true},
	{"from_position_limited", 10, 60, 10, 5, 2, 0, 2, false},
	{"from_position_with_server_limit", 10, 60, 10, 5, 1, 0, 1, false},
	{"from_position_that_already_gone", 10, 60, 20, 8, 10, 0, 0, false},
	{"from_position_that_not_exist_yet", 10, 60, 20, 108, 0, 0, 0, false},
	{"same_position_no_pubs_expected", 10, 60, 7, 7, 0, 0, 0, true},
	{"empty_position_recover_expected", 10, 60, 4, 0, 4, 0, 0, true},
	{"from_position_in_expired_stream", 10, 1, 10, 8, 0, 3, 0, false},
	{"from_same_position_in_expired_stream", 10, 1, 1, 1, 0, 3, 0, true},
}

func TestTarantoolClientSubscribeRecover(t *testing.T) {
	for _, tt := range recoverTests {
		t.Run(tt.Name, func(t *testing.T) {
			testTarantoolClientSubscribeRecover(t, tt)
		})
	}
}

func nodeWithTarantoolBroker(tb testing.TB) *centrifuge.Node {
	c := centrifuge.DefaultConfig
	return nodeWithTarantoolBrokerWithConfig(tb, c)
}

func nodeWithTarantoolBrokerWithConfig(tb testing.TB, c centrifuge.Config) *centrifuge.Node {
	n, err := centrifuge.New(c)
	if err != nil {
		tb.Fatal(err)
	}
	e, _ := newTestTarantoolEngine(tb)
	n.SetBroker(e)
	err = n.Run()
	if err != nil {
		tb.Fatal(err)
	}
	return n
}

func pubToProto(pub *centrifuge.Publication) *protocol.Publication {
	if pub == nil {
		return nil
	}
	return &protocol.Publication{
		Offset: pub.Offset,
		Data:   pub.Data,
		Info:   infoToProto(pub.Info),
	}
}

func isRecovered(historyResult centrifuge.HistoryResult, cmdOffset uint64, cmdEpoch string) ([]*protocol.Publication, bool) {
	latestOffset := historyResult.Offset
	latestEpoch := historyResult.Epoch

	recoveredPubs := make([]*protocol.Publication, 0, len(historyResult.Publications))
	for _, pub := range historyResult.Publications {
		protoPub := pubToProto(pub)
		recoveredPubs = append(recoveredPubs, protoPub)
	}

	nextOffset := cmdOffset + 1
	var recovered bool
	if len(recoveredPubs) == 0 {
		recovered = latestOffset == cmdOffset && latestEpoch == cmdEpoch
	} else {
		recovered = recoveredPubs[0].Offset == nextOffset &&
			recoveredPubs[len(recoveredPubs)-1].Offset == latestOffset &&
			latestEpoch == cmdEpoch
	}

	return recoveredPubs, recovered
}

// recoverHistory recovers publications since StreamPosition last seen by client.
func recoverHistory(node *centrifuge.Node, ch string, since centrifuge.StreamPosition, maxPublicationLimit int) (centrifuge.HistoryResult, error) {
	limit := centrifuge.NoLimit
	if maxPublicationLimit > 0 {
		limit = maxPublicationLimit
	}
	return node.History(ch, centrifuge.WithLimit(limit), centrifuge.Since(&since))
}

func testTarantoolClientSubscribeRecover(t *testing.T, tt recoverTest) {
	node := nodeWithTarantoolBroker(t)
	defer func() { _ = node.Shutdown(context.Background()) }()

	channel := "test_recovery_tarantool_" + tt.Name

	for i := 1; i <= tt.NumPublications; i++ {
		_, err := node.Publish(channel, []byte(`{"n": `+strconv.Itoa(i)+`}`), centrifuge.WithHistory(tt.HistorySize, time.Duration(tt.HistoryLifetime)*time.Second))
		require.NoError(t, err)
	}

	time.Sleep(time.Duration(tt.Sleep) * time.Second)

	res, err := node.History(channel)
	require.NoError(t, err)
	streamTop := res.StreamPosition

	historyResult, err := recoverHistory(node, channel, centrifuge.StreamPosition{Offset: tt.SinceOffset, Epoch: streamTop.Epoch}, tt.Limit)
	require.NoError(t, err)
	recoveredPubs, recovered := isRecovered(historyResult, tt.SinceOffset, streamTop.Epoch)
	require.Equal(t, tt.NumRecovered, len(recoveredPubs))
	require.Equal(t, tt.Recovered, recovered)
}

func BenchmarkTarantoolPublish_OneChannel(b *testing.B) {
	broker, _ := newTestTarantoolEngine(b)
	rawData := []byte(`{"bench": true}`)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := broker.Publish("channel", rawData, centrifuge.PublishOptions{})
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTarantoolPublish_OneChannel_Parallel(b *testing.B) {
	broker, _ := newTestTarantoolEngine(b)
	rawData := []byte(`{"bench": true}`)
	b.SetParallelism(128)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := broker.Publish("channel", rawData, centrifuge.PublishOptions{})
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkTarantoolSubscribe(b *testing.B) {
	broker, _ := newTestTarantoolEngine(b)
	j := 0
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		j++
		err := broker.Subscribe("subscribe" + strconv.Itoa(j))
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTarantoolSubscribe_Parallel(b *testing.B) {
	broker, _ := newTestTarantoolEngine(b)
	i := 0
	b.SetParallelism(128)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i++
			err := broker.Subscribe("subscribe" + strconv.Itoa(i))
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkTarantoolRecover_OneChannel_Parallel(b *testing.B) {
	broker, _ := newTestTarantoolEngine(b)
	rawData := []byte("{}")
	numMessages := 1000
	numMissing := 5
	for i := 1; i <= numMessages; i++ {
		_, err := broker.Publish("channel", rawData, centrifuge.PublishOptions{HistorySize: numMessages, HistoryTTL: 300 * time.Second})
		require.NoError(b, err)
	}
	_, sp, err := broker.History("channel", centrifuge.HistoryFilter{})
	require.NoError(b, err)
	b.ResetTimer()
	b.SetParallelism(128)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			pubs, _, err := broker.History("channel", centrifuge.HistoryFilter{
				Limit: -1,
				Since: &centrifuge.StreamPosition{Offset: sp.Offset - uint64(numMissing), Epoch: ""},
			})
			if err != nil {
				b.Fatal(err)
			}
			if len(pubs) != numMissing {
				b.Fatalf("len pubs: %d, expected: %d", len(pubs), numMissing)
			}
		}
	})
}

func BenchmarkTarantoolPresence_OneChannel(b *testing.B) {
	_, pm := newTestTarantoolEngine(b)
	_ = pm.AddPresence("channel", "uid", &centrifuge.ClientInfo{})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p, err := pm.Presence("channel")
		if err != nil {
			b.Fatal(err)
		}
		if len(p) != 1 {
			b.Fatal("wrong presence len")
		}
	}
}

func BenchmarkTarantoolPresence_OneChannel_Parallel(b *testing.B) {
	_, pm := newTestTarantoolEngine(b)
	b.SetParallelism(128)
	_ = pm.AddPresence("channel", "uid", &centrifuge.ClientInfo{})
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			p, err := pm.Presence("channel")
			if err != nil {
				b.Fatal(err)
			}
			if len(p) != 1 {
				b.Fatal("wrong presence len")
			}
		}
	})
}

func BenchmarkTarantoolAddPresence_OneChannel(b *testing.B) {
	_, pm := newTestTarantoolEngine(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := pm.AddPresence("channel", "uid", &centrifuge.ClientInfo{})
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTarantoolAddPresence_OneChannel_Parallel(b *testing.B) {
	_, pm := newTestTarantoolEngine(b)
	b.SetParallelism(128)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			err := pm.AddPresence("channel", "uid", &centrifuge.ClientInfo{})
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkTarantoolPresenceStats_OneChannel_Parallel(b *testing.B) {
	_, pm := newTestTarantoolEngine(b)
	_ = pm.AddPresence("channel", "uid", &centrifuge.ClientInfo{})
	b.SetParallelism(128)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			p, err := pm.PresenceStats("channel")
			if err != nil {
				b.Fatal(err)
			}
			if p.NumClients != 1 {
				b.Fatal("wrong presence stats")
			}
		}
	})
}
