package centrifuge

import (
	"context"
	"strconv"
	"testing"
	"time"
)

// drain blocks until every client's writer queue is empty.
//
// Fan-out cost is not paid only by the broadcasting goroutine: each subscriber's
// writer goroutine dequeues and encodes on its own. b.ReportAllocs accounts for
// allocations process-wide, so waiting for the queues to empty before the timer
// stops keeps the writers' work attributed to the run that caused it. Bounded so
// a wedged writer fails the benchmark instead of hanging it.
func drain(clients []*Client) {
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		pending := false
		for _, c := range clients {
			if c.messageWriter.messages.Len() > 0 {
				pending = true
				break
			}
		}
		if !pending {
			return
		}
		time.Sleep(50 * time.Microsecond)
	}
}

// benchSubscribedClients connects numSubs clients subscribed to ch. Transports
// have a nil sink so writer goroutines drain without blocking on a reader.
func benchSubscribedClients(b *testing.B, n *Node, numSubs int, ch string, proto func(i int) ProtocolType) []*Client {
	b.Helper()
	clients := make([]*Client, 0, numSubs)
	for i := 0; i < numSubs; i++ {
		transport := newTestTransport(func() {})
		transport.setProtocolType(proto(i))
		transport.sink = nil
		ctx, cancel := context.WithCancel(context.Background())
		b.Cleanup(cancel)
		c := newTestConnectedClientWithTransport(b, ctx, n, transport, "user"+strconv.Itoa(i))
		subscribeClientV2(b, c, ch)
		clients = append(clients, c)
	}
	return clients
}

func allJSON(int) ProtocolType { return ProtocolTypeJSON }

func benchBroadcast(b *testing.B, numSubs int, withOffset bool) {
	const ch = "bench"
	n := defaultTestNodeBenchmark(b)
	defer func() { _ = n.Shutdown(context.Background()) }()
	clients := benchSubscribedClients(b, n, numSubs, ch, allJSON)

	data := []byte(`{"input":"hello world, this is a benchmark publication payload"}`)
	sp := StreamPosition{Epoch: "test"}
	var offset uint64

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if withOffset {
			offset++
		}
		pub := &Publication{Data: data, Offset: offset, Time: time.Now().UnixMilli()}
		if err := n.hub.broadcastPublication(ch, sp, pub, nil, nil, ChannelBatchConfig{}); err != nil {
			b.Fatal(err)
		}
	}
	drain(clients)
	b.StopTimer()
}

func BenchmarkHubBroadcast_1Sub(b *testing.B)     { benchBroadcast(b, 1, false) }
func BenchmarkHubBroadcast_100Subs(b *testing.B)  { benchBroadcast(b, 100, false) }
func BenchmarkHubBroadcast_1000Subs(b *testing.B) { benchBroadcast(b, 1000, false) }

func BenchmarkHubBroadcastOffset_1Sub(b *testing.B)     { benchBroadcast(b, 1, true) }
func BenchmarkHubBroadcastOffset_100Subs(b *testing.B)  { benchBroadcast(b, 100, true) }
func BenchmarkHubBroadcastOffset_1000Subs(b *testing.B) { benchBroadcast(b, 1000, true) }

// BenchmarkHubBroadcastManyChannels broadcasts across many channels
// concurrently — the shape that exposes per-broadcast (rather than
// per-subscriber) costs such as metric label lookups and prepared-data map
// allocation.
func BenchmarkHubBroadcastManyChannels(b *testing.B) {
	const numChannels = 64
	const subsPerChannel = 10
	n := defaultTestNodeBenchmark(b)
	defer func() { _ = n.Shutdown(context.Background()) }()

	channels := make([]string, numChannels)
	clients := make([]*Client, 0, numChannels*subsPerChannel)
	for i := 0; i < numChannels; i++ {
		channels[i] = "bench" + strconv.Itoa(i)
		for j := 0; j < subsPerChannel; j++ {
			transport := newTestTransport(func() {})
			transport.sink = nil
			ctx, cancel := context.WithCancel(context.Background())
			b.Cleanup(cancel)
			c := newTestConnectedClientWithTransport(b, ctx, n, transport, "u"+strconv.Itoa(i)+"_"+strconv.Itoa(j))
			subscribeClientV2(b, c, channels[i])
			clients = append(clients, c)
		}
	}

	data := []byte(`{"input":"hello world, this is a benchmark publication payload"}`)
	sp := StreamPosition{Epoch: "test"}

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			ch := channels[i%numChannels]
			i++
			pub := &Publication{Data: data, Time: time.Now().UnixMilli()}
			if err := n.hub.broadcastPublication(ch, sp, pub, nil, nil, ChannelBatchConfig{}); err != nil {
				b.Fatal(err)
			}
		}
	})
	drain(clients)
	b.StopTimer()
}

// BenchmarkHubBroadcastMixedProtocol has both JSON and Protobuf subscribers on
// one channel, so the prepared-payload cache must hold more than one entry.
func BenchmarkHubBroadcastMixedProtocol(b *testing.B) {
	const ch = "bench"
	n := defaultTestNodeBenchmark(b)
	defer func() { _ = n.Shutdown(context.Background()) }()
	clients := benchSubscribedClients(b, n, 1000, ch, func(i int) ProtocolType {
		if i%2 == 0 {
			return ProtocolTypeProtobuf
		}
		return ProtocolTypeJSON
	})

	data := []byte(`{"input":"hello world, this is a benchmark publication payload"}`)
	sp := StreamPosition{Epoch: "test"}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		pub := &Publication{Data: data, Time: time.Now().UnixMilli()}
		if err := n.hub.broadcastPublication(ch, sp, pub, nil, nil, ChannelBatchConfig{}); err != nil {
			b.Fatal(err)
		}
	}
	drain(clients)
	b.StopTimer()
}
