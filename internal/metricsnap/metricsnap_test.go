package metricsnap

import (
	"errors"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func TestSnapshot(t *testing.T) {
	reg := prometheus.NewRegistry()
	counter := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "centrifugo", Subsystem: "client", Name: "num_reply_errors",
	}, []string{"method", "code"})
	gauge := prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "centrifugo", Subsystem: "node", Name: "num_clients",
	})
	summary := prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Namespace: "centrifugo", Subsystem: "client", Name: "command_duration_seconds",
		Objectives: map[float64]float64{0.5: 0.05, 0.99: 0.001, 0.999: 0.0001},
	}, []string{"method"})
	histogram := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "centrifugo", Subsystem: "client", Name: "command_duration_seconds_histogram",
	}, []string{"method"})
	other := prometheus.NewCounter(prometheus.CounterOpts{Name: "go_other_total"})
	reg.MustRegister(counter, gauge, summary, histogram, other)

	counter.WithLabelValues("subscribe", "103").Add(3)
	counter.WithLabelValues("publish", "").Add(1)
	gauge.Set(42)
	summary.WithLabelValues("connect").Observe(0.2)
	histogram.WithLabelValues("connect").Observe(0.2)
	histogram.WithLabelValues("connect").Observe(0.4)
	other.Inc()

	s := New(reg, Options{Prefixes: []string{"centrifugo"}, IncludeHistograms: true})
	items, err := s.Snapshot()
	require.NoError(t, err)

	// The keys node info always had: labels sorted by name, empty values
	// dropped, quantiles as "quantile.50".
	require.Equal(t, 3.0, items["centrifugo.client.num_reply_errors.code.103.method.subscribe"])
	require.Equal(t, 1.0, items["centrifugo.client.num_reply_errors.method.publish"])
	require.Equal(t, 42.0, items["centrifugo.node.num_clients"])
	require.Equal(t, 1.0, items["centrifugo.client.command_duration_seconds.count.method.connect"])
	require.InDelta(t, 0.2, items["centrifugo.client.command_duration_seconds.sum.method.connect"], 1e-9)
	require.InDelta(t, 0.2, items["centrifugo.client.command_duration_seconds.quantile.50.method.connect"], 1e-9)
	require.Contains(t, items, "centrifugo.client.command_duration_seconds.quantile.999.method.connect")
	// Histograms: count and sum like summaries.
	require.Equal(t, 2.0, items["centrifugo.client.command_duration_seconds_histogram.count.method.connect"])
	require.InDelta(t, 0.6, items["centrifugo.client.command_duration_seconds_histogram.sum.method.connect"], 1e-9)
	// Only allowed prefixes.
	for k := range items {
		require.NotContains(t, k, "go.other")
	}

	// Counters and counts are changes since the previous snapshot, gauges
	// are current values.
	counter.WithLabelValues("subscribe", "103").Add(2)
	histogram.WithLabelValues("connect").Observe(1)
	gauge.Set(40)
	items, err = s.Snapshot()
	require.NoError(t, err)
	require.Equal(t, 2.0, items["centrifugo.client.num_reply_errors.code.103.method.subscribe"])
	require.Equal(t, 0.0, items["centrifugo.client.num_reply_errors.method.publish"])
	require.Equal(t, 1.0, items["centrifugo.client.command_duration_seconds_histogram.count.method.connect"])
	require.InDelta(t, 1.0, items["centrifugo.client.command_duration_seconds_histogram.sum.method.connect"], 1e-9)
	require.Equal(t, 0.0, items["centrifugo.client.command_duration_seconds.count.method.connect"])
	require.Equal(t, 40.0, items["centrifugo.node.num_clients"])
}

func TestSnapshotNativeHistogram(t *testing.T) {
	reg := prometheus.NewRegistry()
	histogram := prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "centrifugo", Subsystem: "proxy", Name: "duration_seconds_histogram",
		NativeHistogramBucketFactor: 1.1,
	})
	reg.MustRegister(histogram)
	histogram.Observe(0.1)
	histogram.Observe(0.3)

	items, err := New(reg, Options{IncludeHistograms: true}).Snapshot()
	require.NoError(t, err)
	require.Equal(t, 2.0, items["centrifugo.proxy.duration_seconds_histogram.count"])
	require.InDelta(t, 0.4, items["centrifugo.proxy.duration_seconds_histogram.sum"], 1e-9)
}

func TestSnapshotCounterReset(t *testing.T) {
	reg := prometheus.NewRegistry()
	counter := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "centrifugo", Subsystem: "node", Name: "messages_sent_count",
	}, []string{"type"})
	reg.MustRegister(counter)
	counter.WithLabelValues("publication").Add(10)

	s := New(reg, Options{})
	_, err := s.Snapshot()
	require.NoError(t, err)

	// A reset vector counts from zero, it is never negative.
	counter.Reset()
	counter.WithLabelValues("publication").Add(4)
	items, err := s.Snapshot()
	require.NoError(t, err)
	require.Equal(t, 4.0, items["centrifugo.node.messages_sent_count.type.publication"])
}

func TestQuantileString(t *testing.T) {
	require.Equal(t, "50", quantileString(0.5))
	require.Equal(t, "99", quantileString(0.99))
	require.Equal(t, "999", quantileString(0.999))
}

func TestSnapshotWithoutHistograms(t *testing.T) {
	reg := prometheus.NewRegistry()
	histogram := prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "centrifugo", Subsystem: "node", Name: "broadcast_duration_seconds",
	})
	reg.MustRegister(histogram)
	histogram.Observe(0.1)

	// Off by default - snapshots stay as small as they were.
	items, err := New(reg, Options{}).Snapshot()
	require.NoError(t, err)
	require.Empty(t, items)
}

func TestSnapshotGatherError(t *testing.T) {
	failing := prometheus.GathererFunc(func() ([]*dto.MetricFamily, error) {
		return nil, errors.New("boom")
	})
	_, err := New(failing, Options{}).Snapshot()
	require.EqualError(t, err, "boom")
}

func TestSnapshotSkipped(t *testing.T) {
	reg := prometheus.NewRegistry()
	// No subsystem in the name - no key node info could use.
	noSubsystem := prometheus.NewCounter(prometheus.CounterOpts{Name: "uptime"})
	// No observations - quantiles are NaN.
	empty := prometheus.NewSummary(prometheus.SummaryOpts{
		Namespace: "centrifugo", Subsystem: "node", Name: "broadcast_duration_seconds",
		Objectives: map[float64]float64{0.5: 0.05},
	})
	reg.MustRegister(noSubsystem, empty)
	noSubsystem.Inc()

	items, err := New(reg, Options{}).Snapshot()
	require.NoError(t, err)
	require.Equal(t, map[string]float64{
		"centrifugo.node.broadcast_duration_seconds.count": 0,
		"centrifugo.node.broadcast_duration_seconds.sum":   0,
	}, items)
}

// registryWithSeries registers metrics shaped like a node's: counters,
// gauges, summaries and histograms over namespace and method labels, about
// 20 snapshot items per namespace.
func registryWithSeries(namespaces int) *prometheus.Registry {
	reg := prometheus.NewRegistry()
	counter := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "centrifugo", Subsystem: "node", Name: "action_count",
	}, []string{"action", "channel_namespace"})
	gauge := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "centrifugo", Subsystem: "client", Name: "subscriptions_inflight",
	}, []string{"channel_namespace"})
	summary := prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Namespace: "centrifugo", Subsystem: "node", Name: "broadcast_duration_seconds",
		Objectives: map[float64]float64{0.5: 0.05, 0.99: 0.001, 0.999: 0.0001},
	}, []string{"channel_namespace"})
	histogram := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "centrifugo", Subsystem: "node", Name: "pub_sub_lag_seconds",
	}, []string{"channel_namespace"})
	reg.MustRegister(counter, gauge, summary, histogram)
	actions := []string{"publish", "history", "presence", "subscribe", "unsubscribe", "join", "leave", "remove_history", "presence_stats", "info"}
	for i := 0; i < namespaces; i++ {
		ns := "namespace_" + string(rune('a'+i%26)) + strings.Repeat("x", i/26)
		for _, a := range actions {
			counter.WithLabelValues(a, ns).Add(float64(i + 1))
		}
		gauge.WithLabelValues(ns).Set(float64(i))
		summary.WithLabelValues(ns).Observe(0.01)
		histogram.WithLabelValues(ns).Observe(0.01)
	}
	return reg
}

func BenchmarkSnapshot(b *testing.B) {
	for _, tc := range []struct {
		name       string
		namespaces int
	}{{"30 namespaces", 30}, {"300 namespaces", 300}} {
		b.Run(tc.name, func(b *testing.B) {
			s := New(registryWithSeries(tc.namespaces), Options{IncludeHistograms: true})
			items, err := s.Snapshot()
			require.NoError(b, err)
			b.ReportMetric(float64(len(items)), "items")
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := s.Snapshot(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
