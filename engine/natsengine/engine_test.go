package natsengine

import (
	"strconv"
	"testing"

	"github.com/centrifugal/centrifuge"
)

func newTestNatsEngine() *NatsEngine {
	return NewTestNatsEngineWithPrefix("centrifuge-test")
}

func NewTestNatsEngineWithPrefix(prefix string) *NatsEngine {
	n, _ := centrifuge.New(centrifuge.Config{})
	e, _ := New(n, Config{Prefix: prefix})
	n.SetEngine(e)
	err := n.Run()
	if err != nil {
		panic(err)
	}
	return e
}

func BenchmarkNatsEnginePublish(b *testing.B) {
	e := newTestNatsEngine()
	rawData := centrifuge.Raw([]byte(`{"bench": true}`))
	pub := &centrifuge.Publication{UID: "test UID", Data: rawData}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		<-e.Publish("channel", pub, &centrifuge.ChannelOptions{HistorySize: 0, HistoryLifetime: 0})
	}
}

func BenchmarkNatsEnginePublishParallel(b *testing.B) {
	e := newTestNatsEngine()
	rawData := centrifuge.Raw([]byte(`{"bench": true}`))
	pub := &centrifuge.Publication{UID: "test UID", Data: rawData}
	b.SetParallelism(128)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			<-e.Publish("channel", pub, &centrifuge.ChannelOptions{HistorySize: 0, HistoryLifetime: 0})
		}
	})
}

func BenchmarkNatsEngineSubscribe(b *testing.B) {
	e := newTestNatsEngine()
	j := 0
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		j++
		err := e.Subscribe("subscribe" + strconv.Itoa(j))
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkNatsEngineSubscribeParallel(b *testing.B) {
	e := newTestNatsEngine()
	i := 0
	b.SetParallelism(128)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i++
			err := e.Subscribe("subscribe" + strconv.Itoa(i))
			if err != nil {
				panic(err)
			}
		}
	})
}
