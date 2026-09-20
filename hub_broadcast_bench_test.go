package centrifuge

import (
	"context"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/centrifugal/protocol"
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

// delayedHistoryBroker delays History of some channels, as a broker round trip would.
type delayedHistoryBroker struct {
	*MemoryBroker
	delays map[string]time.Duration
}

func (b *delayedHistoryBroker) History(ch string, opts HistoryOptions) ([]*Publication, StreamPosition, error) {
	if d := b.delays[ch]; d > 0 {
		time.Sleep(d)
	}
	return b.MemoryBroker.History(ch, opts)
}

// BenchmarkPublishDuringRecoveringConnects publishes into a channel with 100
// subscribers while other clients keep connecting with that channel and a second one
// in ConnectReply.Subscriptions, both with recovery. The history read of the second
// channel takes a millisecond, so a connect holds the first channel's subscription
// at its sync point for about that long. Reports publish latency percentiles next
// to ns/op.
func BenchmarkPublishDuringRecoveringConnects(b *testing.B) {
	const ch = "bench"
	const slowCh = "bench_slow"
	const numSubs = 100
	const numConnecting = 4

	n, err := New(Config{LogLevel: LogLevelNone})
	if err != nil {
		b.Fatal(err)
	}
	memBroker, err := NewMemoryBroker(n, MemoryBrokerConfig{})
	if err != nil {
		b.Fatal(err)
	}
	n.SetBroker(&delayedHistoryBroker{MemoryBroker: memBroker, delays: map[string]time.Duration{slowCh: time.Millisecond}})
	n.OnConnecting(func(ctx context.Context, e ConnectEvent) (ConnectReply, error) {
		if e.Token != "recovering" {
			return ConnectReply{}, nil
		}
		return ConnectReply{Subscriptions: map[string]SubscribeOptions{
			ch:     {EnableRecovery: true},
			slowCh: {EnableRecovery: true},
		}}, nil
	})
	n.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{}, nil)
		})
	})
	if err := n.Run(); err != nil {
		b.Fatal(err)
	}
	defer func() { _ = n.Shutdown(context.Background()) }()
	clients := benchSubscribedClients(b, n, numSubs, ch, allJSON)

	data := []byte(`{"input":"hello world, this is a benchmark publication payload"}`)
	publish := func() error {
		_, err := n.Publish(ch, data, WithHistory(1000, time.Minute))
		return err
	}
	if err := publish(); err != nil {
		b.Fatal(err)
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < numConnecting; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				transport := newTestTransport(func() {})
				transport.sink = nil
				ctx := SetCredentials(context.Background(), &Credentials{UserID: "connecting"})
				c, err := newClient(ctx, n, transport)
				if err != nil {
					panic(err)
				}
				_ = c.connectCmd(&protocol.ConnectRequest{Token: "recovering"}, &protocol.Command{Id: 1}, time.Now(), nil)
				_ = c.close(DisconnectForceNoReconnect)
			}
		}()
	}
	// Let connects get going.
	time.Sleep(50 * time.Millisecond)

	latencies := make([]time.Duration, b.N)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		started := time.Now()
		if err := publish(); err != nil {
			b.Fatal(err)
		}
		latencies[i] = time.Since(started)
	}
	drain(clients)
	b.StopTimer()
	close(stop)
	wg.Wait()

	slices.Sort(latencies)
	percentile := func(p float64) float64 {
		return float64(latencies[int(float64(len(latencies)-1)*p)].Nanoseconds())
	}
	b.ReportMetric(percentile(0.5), "p50-ns")
	b.ReportMetric(percentile(0.99), "p99-ns")
	b.ReportMetric(percentile(0.999), "p999-ns")
}

// BenchmarkClientSubscribeRecoveringDuringPublish subscribes a client with recovery
// and unsubscribes it again, while publications come into the channel, which has 100
// other subscribers, at a fixed rate (10k/s). It measures the subscribe side of the
// recovery sync, and reports the publications which came per subscribe.
func BenchmarkClientSubscribeRecoveringDuringPublish(b *testing.B) {
	const ch = "bench"
	const interval = 100 * time.Microsecond
	n, err := New(Config{LogLevel: LogLevelNone})
	if err != nil {
		b.Fatal(err)
	}
	n.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{Options: SubscribeOptions{EnableRecovery: true}}, nil)
		})
		client.OnUnsubscribe(func(e UnsubscribeEvent) {})
	})
	if err := n.Run(); err != nil {
		b.Fatal(err)
	}
	defer func() { _ = n.Shutdown(context.Background()) }()
	clients := benchSubscribedClients(b, n, 100, ch, allJSON)

	data := []byte(`{"input":"hello world, this is a benchmark publication payload"}`)
	res, err := n.Publish(ch, data, WithHistory(1000, time.Minute))
	if err != nil {
		b.Fatal(err)
	}
	stop := make(chan struct{})
	published := make(chan struct{})
	var numPublished atomic.Int64
	go func() {
		defer close(published)
		started := time.Now()
		for i := 1; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			// Publications are due on a fixed schedule; a late one is caught up.
			if d := time.Until(started.Add(time.Duration(i) * interval)); d > 0 {
				time.Sleep(d)
			}
			_, _ = n.Publish(ch, data, WithHistory(1000, time.Minute))
			numPublished.Add(1)
		}
	}()

	transport := newTestTransport(func() {})
	transport.sink = nil
	client := newTestConnectedClientWithTransport(b, context.Background(), n, transport, "subscriber")
	rw := &replyWriter{write: func(*protocol.Reply) {}}

	b.ResetTimer()
	b.ReportAllocs()
	publishedBefore := numPublished.Load()
	for i := 0; i < b.N; i++ {
		if err := client.handleSubscribe(&protocol.SubscribeRequest{
			Channel: ch, Recover: true, Offset: res.Offset, Epoch: res.Epoch,
		}, &protocol.Command{Id: uint32(2 * i)}, time.Now(), rw); err != nil {
			b.Fatal(err)
		}
		if err := client.handleUnsubscribe(&protocol.UnsubscribeRequest{Channel: ch},
			&protocol.Command{Id: uint32(2*i + 1)}, time.Now(), rw); err != nil {
			b.Fatal(err)
		}
		// The next subscribe recovers from the latest offset: no history to read
		// beyond what came meanwhile.
		sp, err := n.streamTop(ch, 0)
		if err != nil {
			b.Fatal(err)
		}
		res.StreamPosition = sp
	}
	b.StopTimer()
	b.ReportMetric(float64(numPublished.Load()-publishedBefore)/float64(b.N), "pubs/op")
	close(stop)
	<-published
	drain(clients)
}
