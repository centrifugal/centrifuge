package centrifuge

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

// newMetricsTestNode runs a node with its own Prometheus registry, so
// tests don't share metrics.
func newMetricsTestNode(t *testing.T, c Config) *Node {
	t.Helper()
	c.LogLevel = LogLevelError
	c.LogHandler = func(entry LogEntry) {}
	c.Metrics.RegistererGatherer = prometheus.NewRegistry()
	n, err := New(c)
	require.NoError(t, err)
	n.OnConnecting(func(ctx context.Context, e ConnectEvent) (ConnectReply, error) {
		return ConnectReply{}, nil
	})
	n.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{}, nil)
		})
	})
	require.NoError(t, n.Run())
	t.Cleanup(func() { _ = n.Shutdown(context.Background()) })
	return n
}

// metricsState returns the snapshot not yet sent and the time it covers.
func metricsState(n *Node) (map[string]float64, float64) {
	n.metricsMu.Lock()
	defer n.metricsMu.Unlock()
	return n.metricsSnapshot, n.metricsInterval
}

// waitSnapshot waits for a snapshot after the initial one, which Run sends.
func waitSnapshot(t *testing.T, n *Node, within time.Duration) (map[string]float64, float64) {
	t.Helper()
	var items map[string]float64
	var interval float64
	require.Eventually(t, func() bool {
		items, interval = metricsState(n)
		return items != nil
	}, within, 5*time.Millisecond)
	return items, interval
}

func TestNodeInfoMetricsFixedInterval(t *testing.T) {
	n := newMetricsTestNode(t, Config{NodeInfoMetricsAggregateInterval: 50 * time.Millisecond})
	// Run sent the initial snapshot.
	items, _ := metricsState(n)
	require.Nil(t, items)

	_, interval := waitSnapshot(t, n, time.Second)
	// The real time the snapshot covers.
	require.GreaterOrEqual(t, interval, 0.05)
	require.Less(t, interval, 0.5)
}

func TestNodeInfoMetricsHistograms(t *testing.T) {
	hasHistogram := func(native bool) bool {
		n := newMetricsTestNode(t, Config{
			NodeInfoMetricsAggregateInterval: 30 * time.Millisecond,
			Metrics:                          MetricsConfig{EnableNativeHistograms: native},
		})
		client := newTestConnectedClientV2(t, n, "42")
		subscribeClientV2(t, client, "test")
		items, _ := waitSnapshot(t, n, time.Second)
		for k := range items {
			if strings.Contains(k, "_histogram.count") {
				return true
			}
		}
		return false
	}
	// Summaries carry the counts - histograms only when summaries are off.
	require.False(t, hasHistogram(false))
	require.True(t, hasHistogram(true))
}

func TestNodeInfoMetricsSentOnce(t *testing.T) {
	n := newMetricsTestNode(t, Config{NodeInfoMetricsAggregateInterval: 30 * time.Millisecond})
	_, interval := waitSnapshot(t, n, time.Second)

	require.NoError(t, n.pubNode(""))
	info, err := n.Info()
	require.NoError(t, err)
	require.Len(t, info.Nodes, 1)
	require.NotNil(t, info.Nodes[0].Metrics)
	require.InDelta(t, interval, info.Nodes[0].Metrics.Interval, 1e-9)
	// Sent - the next node info carries no metrics until the next snapshot.
	items, _ := metricsState(n)
	require.Nil(t, items)
}

// flakyRegistry fails to gather while fail is set.
type flakyRegistry struct {
	*prometheus.Registry
	fail atomic.Bool
}

func (r *flakyRegistry) Gather() ([]*dto.MetricFamily, error) {
	if r.fail.Load() {
		return nil, errors.New("gather failed")
	}
	return r.Registry.Gather()
}

func TestNodeInfoMetricsGatherError(t *testing.T) {
	reg := &flakyRegistry{Registry: prometheus.NewRegistry()}
	reg.fail.Store(true)
	n, err := New(Config{
		LogLevel:   LogLevelError,
		LogHandler: func(entry LogEntry) {},
		Metrics:    MetricsConfig{RegistererGatherer: reg},
	})
	require.NoError(t, err)
	require.EqualError(t, n.Run(), "gather failed", "the initial snapshot fails Run")
}

func TestNodeInfoMetricsFailedSnapshot(t *testing.T) {
	reg := &flakyRegistry{Registry: prometheus.NewRegistry()}
	n, err := New(Config{
		LogLevel:                         LogLevelError,
		LogHandler:                       func(entry LogEntry) {},
		NodeInfoMetricsAggregateInterval: 50 * time.Millisecond,
		Metrics:                          MetricsConfig{RegistererGatherer: reg},
	})
	require.NoError(t, err)
	require.NoError(t, n.Run())
	t.Cleanup(func() { _ = n.Shutdown(context.Background()) })

	// Snapshots fail for a while - none is ready to send.
	reg.fail.Store(true)
	time.Sleep(150 * time.Millisecond)
	items, _ := metricsState(n)
	require.Nil(t, items)

	// The next one covers the whole time since the last successful one.
	reg.fail.Store(false)
	_, interval := waitSnapshot(t, n, time.Second)
	require.GreaterOrEqual(t, interval, 0.15)
}

func TestNodeInfoMetricsDefaultGatherer(t *testing.T) {
	n, err := New(Config{LogLevel: LogLevelError, LogHandler: func(entry LogEntry) {}})
	require.NoError(t, err)
	n.metrics.config.RegistererGatherer = nil
	require.NoError(t, n.initMetrics())
	items, _ := metricsState(n)
	require.NotNil(t, items, "snapshot of the default registry")
}

func TestNodeInfoMetricsZeroInterval(t *testing.T) {
	n, err := New(Config{LogLevel: LogLevelError, LogHandler: func(entry LogEntry) {}})
	require.NoError(t, err)
	// New sets the default - zero only when set on a node directly.
	n.config.NodeInfoMetricsAggregateInterval = 0
	require.NoError(t, n.initMetrics())
	items, _ := metricsState(n)
	require.Nil(t, items, "metrics off")
}
