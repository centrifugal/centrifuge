package centrifuge

import (
	"testing"

	"github.com/centrifugal/protocol"

	"github.com/prometheus/client_golang/prometheus"
)

func benchMetrics(b *testing.B, clientLabels []string) *metrics {
	b.Helper()
	m, err := newMetricsRegistry(MetricsConfig{
		MetricsNamespace:   "test",
		RegistererGatherer: prometheus.NewRegistry(),
		ClientLabels:       clientLabels,
	})
	if err != nil {
		b.Fatal(err)
	}
	return m
}

// BenchmarkObserveTransportFrameSize measures the per-frame cost. Frames are the
// hot path - every WebSocket message pays this - so it has to be comparable to
// the per-command metric work already done alongside it.
func BenchmarkObserveTransportFrameSize(b *testing.B) {
	b.Run("no client labels", func(b *testing.B) {
		m := benchMetrics(b, nil)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			m.observeTransportFrameSize(transportWebsocket, 4096, nil)
		}
	})

	b.Run("with client labels", func(b *testing.B) {
		m := benchMetrics(b, []string{"app", "platform"})
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			m.observeTransportFrameSize(transportWebsocket, 4096, nil)
		}
	})
}

// BenchmarkIncTransportMessagesReceived is the existing per-command metric, for
// comparison: whatever a frame costs should be small next to what every command
// in it already costs.
func BenchmarkIncTransportMessagesReceived(b *testing.B) {
	b.Run("no client labels", func(b *testing.B) {
		m := benchMetrics(b, nil)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			m.incTransportMessagesReceived(transportWebsocket, protocol.FrameTypePublish, "", 4096, nil)
		}
	})
	b.Run("with client labels", func(b *testing.B) {
		m := benchMetrics(b, []string{"app", "platform"})
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			m.incTransportMessagesReceived(transportWebsocket, protocol.FrameTypePublish, "", 4096, nil)
		}
	})
}
