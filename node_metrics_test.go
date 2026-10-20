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

// metricsState returns the snapshot not yet sent.
func metricsState(n *Node) map[string]float64 {
	n.metricsMu.Lock()
	defer n.metricsMu.Unlock()
	return n.metricsSnapshot
}

// waitSnapshot waits for a snapshot after the initial one, which Run sends.
func waitSnapshot(t *testing.T, n *Node, within time.Duration) map[string]float64 {
	t.Helper()
	var items map[string]float64
	require.Eventually(t, func() bool {
		items = metricsState(n)
		return items != nil
	}, within, 5*time.Millisecond)
	return items
}

func TestNodeInfoMetricsHistograms(t *testing.T) {
	hasHistogram := func(native bool) bool {
		n := newMetricsTestNode(t, Config{
			NodeInfoMetricsAggregateInterval: 30 * time.Millisecond,
			Metrics:                          MetricsConfig{EnableNativeHistograms: native},
		})
		client := newTestConnectedClientV2(t, n, "42")
		subscribeClientV2(t, client, "test")
		items := waitSnapshot(t, n, time.Second)
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
	// No snapshot from the ticker during the test - the one sent is set here.
	n := newMetricsTestNode(t, Config{NodeInfoMetricsAggregateInterval: time.Hour})
	// Run sent the initial snapshot.
	require.Nil(t, metricsState(n))

	n.metricsMu.Lock()
	n.metricsSnapshot = map[string]float64{"test": 1}
	n.metricsMu.Unlock()
	// Published here or by the periodic node info - the result is the same.
	require.NoError(t, n.pubNode(""))
	info, err := n.Info()
	require.NoError(t, err)
	require.Len(t, info.Nodes, 1)
	require.NotNil(t, info.Nodes[0].Metrics)
	require.Equal(t, map[string]float64{"test": 1}, info.Nodes[0].Metrics.Items)
	// The configured interval, exactly.
	require.Equal(t, time.Hour.Seconds(), info.Nodes[0].Metrics.Interval)
	// Sent - the next node info carries no metrics until the next snapshot.
	require.Nil(t, metricsState(n))
}

// flakyRegistry fails to gather while fail is set, counting failures.
type flakyRegistry struct {
	*prometheus.Registry
	fail   atomic.Bool
	failed atomic.Int64
}

func (r *flakyRegistry) Gather() ([]*dto.MetricFamily, error) {
	if r.fail.Load() {
		r.failed.Add(1)
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
	// Once a tick failed, a snapshot taken before is already stored - drop it.
	require.Eventually(t, func() bool { return reg.failed.Load() >= 1 }, time.Second, 5*time.Millisecond)
	n.metricsMu.Lock()
	n.metricsSnapshot = nil
	n.metricsMu.Unlock()
	require.Eventually(t, func() bool { return reg.failed.Load() >= 3 }, time.Second, 5*time.Millisecond)
	require.Nil(t, metricsState(n))

	// Snapshots are ready again once gathering recovers.
	reg.fail.Store(false)
	waitSnapshot(t, n, time.Second)
}

func TestNodeInfoMetricsDefaultGatherer(t *testing.T) {
	n, err := New(Config{LogLevel: LogLevelError, LogHandler: func(entry LogEntry) {}})
	require.NoError(t, err)
	n.metrics.config.RegistererGatherer = nil
	require.NoError(t, n.initMetrics())
	require.NotNil(t, metricsState(n), "snapshot of the default registry")
}

func TestNodeInfoMetricsZeroInterval(t *testing.T) {
	n, err := New(Config{LogLevel: LogLevelError, LogHandler: func(entry LogEntry) {}})
	require.NoError(t, err)
	// New sets the default - zero only when set on a node directly.
	n.config.NodeInfoMetricsAggregateInterval = 0
	require.NoError(t, n.initMetrics())
	require.Nil(t, metricsState(n), "metrics off")
}
