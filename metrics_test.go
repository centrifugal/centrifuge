package centrifuge

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/centrifugal/protocol"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func BenchmarkMetricsTransportMessagesSent(b *testing.B) {
	m, err := newMetricsRegistry(MetricsConfig{
		MetricsNamespace: "test",
	})
	require.NoError(b, err)

	// Pre-allocate channel strings to avoid strconv.Itoa allocations in hot path
	channels := make([]string, 10)
	for i := 0; i < 10; i++ {
		channels[i] = "channel" + strconv.Itoa(i)
	}

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			m.incTransportMessagesSent("test", protocol.FrameTypePushPublication, channels[i%10], 200, nil)
			i++
		}
	})
}

func BenchmarkMetricsTransportMessagesReceived(b *testing.B) {
	m, err := newMetricsRegistry(MetricsConfig{
		MetricsNamespace: "test",
	})
	require.NoError(b, err)

	// Pre-allocate channel strings to avoid strconv.Itoa allocations in hot path
	channels := make([]string, 10)
	for i := 0; i < 10; i++ {
		channels[i] = "channel" + strconv.Itoa(i)
	}

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			m.incTransportMessagesReceived("test", protocol.FrameTypePushPublication, channels[i%10], 200, nil)
			i++
		}
	})
}

func BenchmarkMetricsCommandDuration(b *testing.B) {
	m, err := newMetricsRegistry(MetricsConfig{
		MetricsNamespace: "test",
		GetChannelNamespaceLabel: func(channel string) string {
			return channel
		},
	})
	require.NoError(b, err)

	// Pre-allocate channel strings to avoid strconv.Itoa allocations in hot path
	channels := make([]string, 1024)
	for i := 0; i < 1024; i++ {
		channels[i] = "channel" + strconv.Itoa(i)
	}

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			started := time.Now()
			m.observeCommandDuration(protocol.FrameTypePresence, time.Since(started), channels[i%1024], nil)
			i++
		}
	})
}

func BenchmarkMetricsIncReplyError(b *testing.B) {
	m, err := newMetricsRegistry(MetricsConfig{
		MetricsNamespace: "test",
		GetChannelNamespaceLabel: func(channel string) string {
			return channel
		},
	})
	require.NoError(b, err)

	// Pre-allocate channel strings to avoid strconv.Itoa allocations in hot path
	channels := make([]string, 1024)
	for i := 0; i < 1024; i++ {
		channels[i] = "channel" + strconv.Itoa(i)
	}

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			m.incReplyError(protocol.FrameTypePresence, 100, channels[i%1024], nil)
			i++
		}
	})
}

func BenchmarkMetricsCommandDuration_NativeHistogram(b *testing.B) {
	m, err := newMetricsRegistry(MetricsConfig{
		MetricsNamespace:       "test_native",
		EnableNativeHistograms: true,
		GetChannelNamespaceLabel: func(channel string) string {
			return channel
		},
	})
	require.NoError(b, err)

	// Pre-allocate channel strings to avoid strconv.Itoa allocations in hot path
	channels := make([]string, 1024)
	for i := 0; i < 1024; i++ {
		channels[i] = "channel" + strconv.Itoa(i)
	}

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			started := time.Now()
			m.observeCommandDuration(protocol.FrameTypePresence, time.Since(started), channels[i%1024], nil)
			i++
		}
	})
}

func BenchmarkMetricsIncActionCount(b *testing.B) {
	m, err := newMetricsRegistry(MetricsConfig{
		MetricsNamespace: "test",
		GetChannelNamespaceLabel: func(channel string) string {
			return channel
		},
	})
	require.NoError(b, err)

	// Pre-allocate channel strings to avoid strconv.Itoa allocations in hot path
	channels := make([]string, 1024)
	for i := 0; i < 1024; i++ {
		channels[i] = "channel" + strconv.Itoa(i)
	}

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			m.incActionCount("history", channels[i%1024])
			i++
		}
	})
}

func BenchmarkMetricsIncRecover(b *testing.B) {
	m, err := newMetricsRegistry(MetricsConfig{
		MetricsNamespace: "test",
		GetChannelNamespaceLabel: func(channel string) string {
			return channel
		},
	})
	require.NoError(b, err)

	// Pre-allocate channel strings to avoid strconv.Itoa allocations in hot path
	channels := make([]string, 1024)
	for i := 0; i < 1024; i++ {
		channels[i] = "channel" + strconv.Itoa(i)
	}

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			m.incRecover(true, channels[i%1024], false, nil)
			i++
		}
	})
}

func BenchmarkMetricsIncDisconnect(b *testing.B) {
	m, err := newMetricsRegistry(MetricsConfig{
		MetricsNamespace: "test",
		GetChannelNamespaceLabel: func(channel string) string {
			return channel
		},
	})
	require.NoError(b, err)

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			m.incServerDisconnect(3000, nil)
			i++
		}
	})
}

func BenchmarkMetricsIncUnsubscribe(b *testing.B) {
	m, err := newMetricsRegistry(MetricsConfig{
		MetricsNamespace: "test",
		GetChannelNamespaceLabel: func(channel string) string {
			return channel
		},
	})
	require.NoError(b, err)

	// Pre-allocate channel strings to avoid strconv.Itoa allocations in hot path
	channels := make([]string, 1024)
	for i := 0; i < 1024; i++ {
		channels[i] = "channel" + strconv.Itoa(i)
	}

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			m.incServerUnsubscribe(2500, channels[i%1024], nil)
			i++
		}
	})
}

// Benchmarks with client labels enabled

func BenchmarkMetricsIncReplyError_ClientLabels(b *testing.B) {
	registry := prometheus.NewRegistry()
	m, err := newMetricsRegistry(MetricsConfig{
		MetricsNamespace:   "test",
		RegistererGatherer: registry,
		GetChannelNamespaceLabel: func(channel string) string {
			return channel
		},
		ClientLabels: []string{"region", "tier"},
	})
	require.NoError(b, err)

	// Pre-create 100 clients with different label combinations
	// Cardinality: 5 regions × 4 tiers = 20 unique combinations
	regions := []string{"us-east-1", "us-west-2", "eu-west-1", "ap-south-1", "ap-northeast-1"}
	tiers := []string{"free", "standard", "premium", "enterprise"}
	clients := make([]*Client, 100)
	for i := 0; i < 100; i++ {
		labels := map[string]string{
			"region": regions[i%len(regions)],
			"tier":   tiers[i%len(tiers)],
		}
		clients[i] = &Client{
			labels: labels,
		}
		// Simulate connect flow: precompute and cache the combination
		combo := m.getOrCreateClientLabelCombinationFromLabels(labels)
		clients[i].labelCombinationCached.Store(combo)
	}

	// Pre-allocate channel strings to avoid strconv.Itoa allocations in hot path
	channels := make([]string, 1024)
	for i := 0; i < 1024; i++ {
		channels[i] = "channel" + strconv.Itoa(i)
	}

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			client := clients[i%len(clients)]
			m.incReplyError(protocol.FrameTypePresence, 100, channels[i%1024], client)
			i++
		}
	})
}

func BenchmarkMetricsIncDisconnect_ClientLabels(b *testing.B) {
	registry := prometheus.NewRegistry()
	m, err := newMetricsRegistry(MetricsConfig{
		MetricsNamespace:   "test",
		RegistererGatherer: registry,
		GetChannelNamespaceLabel: func(channel string) string {
			return channel
		},
		ClientLabels: []string{"region", "tier"},
	})
	require.NoError(b, err)

	// Pre-create 100 clients with different label combinations
	regions := []string{"us-east-1", "us-west-2", "eu-west-1", "ap-south-1", "ap-northeast-1"}
	tiers := []string{"free", "standard", "premium", "enterprise"}
	clients := make([]*Client, 100)
	for i := 0; i < 100; i++ {
		labels := map[string]string{
			"region": regions[i%len(regions)],
			"tier":   tiers[i%len(tiers)],
		}
		clients[i] = &Client{
			labels: labels,
		}
		// Simulate connect flow: precompute and cache the combination
		combo := m.getOrCreateClientLabelCombinationFromLabels(labels)
		clients[i].labelCombinationCached.Store(combo)
	}

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			client := clients[i%len(clients)]
			m.incServerDisconnect(3000, client)
			i++
		}
	})
}

func BenchmarkMetricsIncUnsubscribe_ClientLabels(b *testing.B) {
	registry := prometheus.NewRegistry()
	m, err := newMetricsRegistry(MetricsConfig{
		MetricsNamespace:   "test",
		RegistererGatherer: registry,
		GetChannelNamespaceLabel: func(channel string) string {
			return channel
		},
		ClientLabels: []string{"region", "tier"},
	})
	require.NoError(b, err)

	// Pre-create 100 clients with different label combinations
	regions := []string{"us-east-1", "us-west-2", "eu-west-1", "ap-south-1", "ap-northeast-1"}
	tiers := []string{"free", "standard", "premium", "enterprise"}
	clients := make([]*Client, 100)
	for i := 0; i < 100; i++ {
		labels := map[string]string{
			"region": regions[i%len(regions)],
			"tier":   tiers[i%len(tiers)],
		}
		clients[i] = &Client{
			labels: labels,
		}
		// Simulate connect flow: precompute and cache the combination
		combo := m.getOrCreateClientLabelCombinationFromLabels(labels)
		clients[i].labelCombinationCached.Store(combo)
	}

	// Pre-allocate channel strings to avoid strconv.Itoa allocations in hot path
	channels := make([]string, 1024)
	for i := 0; i < 1024; i++ {
		channels[i] = "channel" + strconv.Itoa(i)
	}

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			client := clients[i%len(clients)]
			m.incServerUnsubscribe(2500, channels[i%1024], client)
			i++
		}
	})
}

func BenchmarkMetricsTransportMessagesSent_ClientLabels(b *testing.B) {
	registry := prometheus.NewRegistry()
	m, err := newMetricsRegistry(MetricsConfig{
		MetricsNamespace:   "test",
		RegistererGatherer: registry,
		ClientLabels:       []string{"region", "tier"},
	})
	require.NoError(b, err)

	// Pre-create 100 clients with different label combinations
	regions := []string{"us-east-1", "us-west-2", "eu-west-1", "ap-south-1", "ap-northeast-1"}
	tiers := []string{"free", "standard", "premium", "enterprise"}
	clients := make([]*Client, 100)
	for i := 0; i < 100; i++ {
		labels := map[string]string{
			"region": regions[i%len(regions)],
			"tier":   tiers[i%len(tiers)],
		}
		clients[i] = &Client{
			labels: labels,
		}
		// Simulate connect flow: precompute and cache the combination
		combo := m.getOrCreateClientLabelCombinationFromLabels(labels)
		clients[i].labelCombinationCached.Store(combo)
	}

	// Pre-allocate channel strings to avoid strconv.Itoa allocations in hot path
	channels := make([]string, 10)
	for i := 0; i < 10; i++ {
		channels[i] = "channel" + strconv.Itoa(i)
	}

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			client := clients[i%len(clients)]
			m.incTransportMessagesSent("test", protocol.FrameTypePushPublication, channels[i%10], 200, client)
			i++
		}
	})
}

func BenchmarkMetricsGetTransportMessagesSentCounters_ClientLabels(b *testing.B) {
	registry := prometheus.NewRegistry()
	m, err := newMetricsRegistry(MetricsConfig{
		MetricsNamespace:   "test",
		RegistererGatherer: registry,
		ClientLabels:       []string{"region", "tier"},
	})
	require.NoError(b, err)

	// Pre-create a client with labels
	labels := map[string]string{
		"region": "us-east-1",
		"tier":   "premium",
	}
	client := &Client{
		labels: labels,
	}
	// Simulate connect flow: precompute and cache the combination
	combo := m.getOrCreateClientLabelCombinationFromLabels(labels)
	client.labelCombinationCached.Store(combo)

	// Pre-extract values to simulate hot path
	clientLabelValues := combo.labelValues
	clientLabelCacheKey := combo.cacheKey

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			// This simulates the actual hot path in client.go writeMany
			counters := m.getTransportMessagesSentCounters("websocket", "push_publication", "", clientLabelValues, clientLabelCacheKey)
			counters.counterSent.Add(1)
			counters.counterSentSize.Add(200)
			i++
		}
	})
}

func BenchmarkMetricsCommandDuration_ClientLabels(b *testing.B) {
	registry := prometheus.NewRegistry()
	m, err := newMetricsRegistry(MetricsConfig{
		MetricsNamespace:   "test",
		RegistererGatherer: registry,
		GetChannelNamespaceLabel: func(channel string) string {
			return channel
		},
		ClientLabels: []string{"region", "tier"},
	})
	require.NoError(b, err)

	// Pre-create 100 clients with different label combinations
	regions := []string{"us-east-1", "us-west-2", "eu-west-1", "ap-south-1", "ap-northeast-1"}
	tiers := []string{"free", "standard", "premium", "enterprise"}
	clients := make([]*Client, 100)
	for i := 0; i < 100; i++ {
		labels := map[string]string{
			"region": regions[i%len(regions)],
			"tier":   tiers[i%len(tiers)],
		}
		clients[i] = &Client{
			labels: labels,
		}
		// Simulate connect flow: precompute and cache the combination
		combo := m.getOrCreateClientLabelCombinationFromLabels(labels)
		clients[i].labelCombinationCached.Store(combo)
	}

	// Pre-allocate channel strings to avoid strconv.Itoa allocations in hot path
	channels := make([]string, 1024)
	for i := 0; i < 1024; i++ {
		channels[i] = "channel" + strconv.Itoa(i)
	}

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			client := clients[i%len(clients)]
			m.observeCommandDuration(protocol.FrameTypeSubscribe, time.Millisecond, channels[i%1024], client)
			i++
		}
	})
}

func TestMetrics(t *testing.T) {
	t.Parallel()
	_, err := newMetricsRegistry(MetricsConfig{
		GetChannelNamespaceLabel: func(channel string) string {
			return channel
		},
		ChannelNamespaceCacheTTL:             -1,
		EnableRecoveredPublicationsHistogram: true,
	})
	require.Error(t, err)

	testCases := []struct {
		name                      string
		metricsNamespace          string
		getChannelNamespaceLabel  func(channel string) string
		channelNamespaceCacheSize int
		registererGatherer        RegistererGatherer
	}{
		{
			name: "no channel namespace",
		},
		{
			name: "with channel namespace",
			getChannelNamespaceLabel: func(channel string) string {
				return channel
			},
		},
		{
			name: "with channel namespace and no cache",
			getChannelNamespaceLabel: func(channel string) string {
				return channel
			},
			channelNamespaceCacheSize: -1,
		},
		{
			name: "with custom registry",
			getChannelNamespaceLabel: func(channel string) string {
				return channel
			},
			registererGatherer: prometheus.NewRegistry(),
		},
		{
			name: "with metrics namespace",
			getChannelNamespaceLabel: func(channel string) string {
				return channel
			},
			metricsNamespace: "test",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			m, err := newMetricsRegistry(MetricsConfig{
				MetricsNamespace:                     tc.metricsNamespace,
				GetChannelNamespaceLabel:             tc.getChannelNamespaceLabel,
				ChannelNamespaceCacheSize:            tc.channelNamespaceCacheSize,
				RegistererGatherer:                   tc.registererGatherer,
				EnableRecoveredPublicationsHistogram: true,
			})
			require.NoError(t, err)

			for i := 0; i < 10; i++ {
				for _, frameType := range []protocol.FrameType{
					protocol.FrameTypeServerPing, protocol.FrameTypeClientPong, protocol.FrameTypePushConnect,
					protocol.FrameTypePushSubscribe, protocol.FrameTypePushPublication, protocol.FrameTypePushJoin,
					protocol.FrameTypePushLeave, protocol.FrameTypePushUnsubscribe, protocol.FrameTypePushMessage,
					protocol.FrameTypePushRefresh, protocol.FrameTypePushDisconnect, protocol.FrameTypeConnect,
					protocol.FrameTypeSubscribe, protocol.FrameTypePublish, protocol.FrameTypeUnsubscribe,
					protocol.FrameTypeRPC, protocol.FrameTypePresence, protocol.FrameTypePresenceStats,
					protocol.FrameTypeHistory, protocol.FrameTypeRefresh, protocol.FrameTypeSubRefresh,
					protocol.FrameTypeSend,
				} {
					m.incTransportMessagesSent("test", frameType, "channel"+strconv.Itoa(i%2), 200, nil)
					m.incTransportMessagesReceived("test", frameType, "channel"+strconv.Itoa(i%2), 200, nil)
					m.observeCommandDuration(frameType, time.Second, "channel"+strconv.Itoa(i%2), nil)
					m.incReplyError(frameType, 100, "channel"+strconv.Itoa(i%2), nil)
				}

				m.incActionCount("unknown", "channel")
				for _, action := range []string{"survey", "notify", "add_client", "remove_client", "add_subscription", "broker_subscribe", "remove_subscription", "broker_unsubscribe", "add_presence", "remove_presence", "presence", "presence_stats", "history", "history_recover", "history_recover_cache", "history_stream_top", "history_remove"} {
					m.incActionCount(action, "channel"+strconv.Itoa(i%2))
				}
				m.incActionCount("unknown", "")

				for _, msgType := range []string{"publication", "join", "leave", "control", "unknown"} {
					m.incMessagesSent(msgType, "channel"+strconv.Itoa(i%2))
					m.incMessagesReceived(msgType, "channel"+strconv.Itoa(i%2))
				}

				m.observeSurveyDuration("test", time.Second)
				m.incRecover(true, "channel"+strconv.Itoa(i%2), false, nil)
				m.incRecover(false, "channel"+strconv.Itoa(i%2), false, nil)
				m.observeRecoveredPublications(10, "channel"+strconv.Itoa(i%2), nil)
				m.observePubSubDeliveryLag(100, "channel"+strconv.Itoa(i%2))
				m.observePubSubDeliveryLag(-10, "channel"+strconv.Itoa(i%2))
				m.observePingPongDuration(time.Second, transportWebsocket, nil)
				m.incServerDisconnect(3000, nil)
				m.incServerDisconnect(30000, nil)
				m.incServerUnsubscribe(2500, "channel"+strconv.Itoa(i%2), nil)
				m.observeBroadcastDuration(time.Now(), "channel"+strconv.Itoa(i%2))
				m.setBuildInfo("1.0.0")
				m.setNumChannels(100)
				m.setNumClients(200)
				m.setNumUsers(300)
				m.setNumNodes(4)
				m.setNumSubscriptions(500)

				// Shared poll metrics.
				cc := m.getSharedPollChannelCached("channel" + strconv.Itoa(i%2))
				cc.cycleDuration.Observe(1.5)
				cc.cycleWorkDuration.Observe(1.0)
				cc.notifyCount.Inc()

				for _, trigger := range []string{"timer", "notification"} {
					ch := "channel" + strconv.Itoa(i%2)
					hc := m.getSharedPollHandlerCached(trigger, ch)
					hc.duration.Observe(0.05)
					hc.semWait.Observe(0.001)
					hc.errorCount.Inc()
					hc.itemsPolled.Add(100)

					rc := m.getSharedPollResultCached(trigger, ch)
					rc.changed.Add(5)
					rc.unchanged.Add(90)
					rc.removed.Add(2)
				}

				pc := m.getSharedPollPublishCached("channel" + strconv.Itoa(i%2))
				pc.applied.Inc()
				pc.skipped.Inc()

				m.setSharedPollNumChannels(10)
				m.setSharedPollNumKeys(500)
			}
		})
	}
}

func TestClientLabels(t *testing.T) {
	t.Run("metrics without client labels configured", func(t *testing.T) {
		// Use custom registry to avoid conflicts
		registry := prometheus.NewRegistry()
		m, err := newMetricsRegistry(MetricsConfig{
			MetricsNamespace:   "test1",
			RegistererGatherer: registry,
		})
		require.NoError(t, err)

		// Without client labels, metrics should work as before
		m.incTransportMessagesSent("ws", protocol.FrameTypePushPublication, "channel", 100, nil)
		m.observeCommandDuration(protocol.FrameTypeConnect, time.Millisecond, "", nil)
	})

	t.Run("metrics with client labels configured and whitelisted", func(t *testing.T) {
		// Use custom registry to avoid conflicts
		registry := prometheus.NewRegistry()
		m, err := newMetricsRegistry(MetricsConfig{
			MetricsNamespace:   "test2",
			RegistererGatherer: registry,
			ClientLabels:       []string{"region", "tier"},
		})
		require.NoError(t, err)

		// Test extractClientLabelValues separately with a mock client structure
		// We can't create a full Client without a Node, so just test the metric functions
		// that accept nil client
		m.incTransportMessagesSent("ws", protocol.FrameTypePushPublication, "channel", 100, nil)
		m.observeCommandDuration(protocol.FrameTypeConnect, time.Millisecond, "", nil)
	})

	t.Run("metrics with missing client label values", func(t *testing.T) {
		registry := prometheus.NewRegistry()
		m, err := newMetricsRegistry(MetricsConfig{
			MetricsNamespace:   "test3",
			RegistererGatherer: registry,
			ClientLabels:       []string{"region", "tier", "auth_type"},
		})
		require.NoError(t, err)

		// Should work with nil client - empty strings used for missing labels
		m.incTransportMessagesSent("ws", protocol.FrameTypePushPublication, "channel", 100, nil)
	})

	t.Run("metrics with client labels but not whitelisted", func(t *testing.T) {
		registry := prometheus.NewRegistry()
		m, err := newMetricsRegistry(MetricsConfig{
			MetricsNamespace:   "test4",
			RegistererGatherer: registry,
			ClientLabels:       []string{"region", "tier"},
		})
		require.NoError(t, err)

		// Metrics should work but without client labels (not whitelisted)
		m.incTransportMessagesSent("ws", protocol.FrameTypePushPublication, "channel", 100, nil)
		m.observeCommandDuration(protocol.FrameTypeConnect, time.Millisecond, "", nil)
	})

	t.Run("extractClientLabelValues with nil client", func(t *testing.T) {
		registry := prometheus.NewRegistry()
		m, err := newMetricsRegistry(MetricsConfig{
			MetricsNamespace:   "test5",
			RegistererGatherer: registry,
			ClientLabels:       []string{"region"},
		})
		require.NoError(t, err)

		// Should handle nil client gracefully
		values := m.extractClientLabelValues(nil)
		require.Nil(t, values)
	})

	t.Run("extractClientLabelValues with client without labels", func(t *testing.T) {
		registry := prometheus.NewRegistry()
		m, err := newMetricsRegistry(MetricsConfig{
			MetricsNamespace:   "test6",
			RegistererGatherer: registry,
			ClientLabels:       []string{"region"},
		})
		require.NoError(t, err)

		// Create a minimal client-like structure for testing
		// Since we can't create a real Client without Node, test the function logic
		// by creating a client with no labels map
		c := &Client{}
		values := m.extractClientLabelValues(c)
		require.NotNil(t, values)
		require.Len(t, values, 1)
		require.Equal(t, "", values[0]) // Should be empty string
	})

	t.Run("buildMetricLabels without client labels", func(t *testing.T) {
		registry := prometheus.NewRegistry()
		m, err := newMetricsRegistry(MetricsConfig{
			MetricsNamespace:   "test7",
			RegistererGatherer: registry,
		})
		require.NoError(t, err)

		labels := m.buildMetricLabels([]string{"transport", "frame_type"})
		require.Equal(t, []string{"transport", "frame_type"}, labels)
	})

	t.Run("buildMetricLabels with client labels and whitelisted", func(t *testing.T) {
		registry := prometheus.NewRegistry()
		m, err := newMetricsRegistry(MetricsConfig{
			MetricsNamespace:   "test8",
			RegistererGatherer: registry,
			ClientLabels:       []string{"region", "tier"},
		})
		require.NoError(t, err)

		labels := m.buildMetricLabels([]string{"transport", "frame_type"})
		require.Equal(t, []string{"transport", "frame_type", "app_region", "app_tier"}, labels)
	})
}

func Test_getHTTPTransportProto(t *testing.T) {
	t.Parallel()
	type args struct {
		protoMajor int8
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "HTTP/1.x",
			args: args{protoMajor: 1},
			want: "h1",
		},
		{
			name: "HTTP/2",
			args: args{protoMajor: 2},
			want: "h2",
		},
		{
			name: "HTTP/3",
			args: args{protoMajor: 3},
			want: "h3",
		},
		{
			name: "unknown HTTP version",
			args: args{protoMajor: 0},
			want: "unknown",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getAcceptProtocolLabel(tt.args.protoMajor); got != tt.want {
				t.Errorf("getAcceptProtocolLabel() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMetrics_MapBrokerAndRedisBrokerCounters(t *testing.T) {
	t.Parallel()
	// Use a dedicated registry: the "test_map" namespace + "broker" subsystem
	// would otherwise collide with another test's "test" namespace + "map_broker"
	// subsystem on the shared default registry (both form *_map_broker_* fqNames).
	m, err := newMetricsRegistry(MetricsConfig{
		MetricsNamespace:   "test_map",
		RegistererGatherer: prometheus.NewRegistry(),
		GetChannelNamespaceLabel: func(channel string) string {
			return channel
		},
	})
	require.NoError(t, err)
	// These were previously uncovered.
	m.incRedisBrokerPubSubErrors("test_broker", "subscribe")
	m.incMapBrokerCleanupErrors("test_broker")
	m.addMapBrokerCleanupRemoved("test_broker", 10)
	m.setMapBrokerCleanupLag("test_broker", 2.5)
}

func BenchmarkSharedPollHandlerCached(b *testing.B) {
	m, err := newMetricsRegistry(MetricsConfig{
		MetricsNamespace: "test",
		GetChannelNamespaceLabel: func(channel string) string {
			return channel
		},
	})
	require.NoError(b, err)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			hc := m.getSharedPollHandlerCached("timer", "channel"+strconv.Itoa(i%10))
			hc.duration.Observe(0.05)
			hc.itemsPolled.Add(100)
			i++
		}
	})
}

func BenchmarkSharedPollResultCached(b *testing.B) {
	m, err := newMetricsRegistry(MetricsConfig{
		MetricsNamespace: "test",
		GetChannelNamespaceLabel: func(channel string) string {
			return channel
		},
	})
	require.NoError(b, err)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			rc := m.getSharedPollResultCached("timer", "channel"+strconv.Itoa(i%10))
			rc.changed.Add(5)
			rc.unchanged.Add(90)
			i++
		}
	})
}

func TestMetrics_EnableNativeHistograms(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m, err := newMetricsRegistry(MetricsConfig{
		MetricsNamespace:       "test_nh",
		RegistererGatherer:     reg,
		EnableNativeHistograms: true,
		GetChannelNamespaceLabel: func(channel string) string {
			return channel
		},
	})
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		m.observeCommandDuration(protocol.FrameTypePublish, time.Duration(i+1)*time.Millisecond, "ns1", nil)
		m.observeSurveyDuration("test_op", time.Duration(i+1)*10*time.Millisecond)
	}

	families, err := reg.Gather()
	require.NoError(t, err)

	// With EnableNativeHistograms on, the legacy Summary metrics must not
	// be exposed; their _histogram companions carry the observations in
	// native (sparse, exponential) form.
	mustNotExist := map[string]bool{
		"test_nh_client_command_duration_seconds": true,
		"test_nh_node_survey_duration_seconds":    true,
	}
	wantHistograms := map[string]bool{
		"test_nh_client_command_duration_seconds_histogram": false,
		"test_nh_node_survey_duration_seconds_histogram":    false,
	}
	for _, mf := range families {
		name := mf.GetName()
		require.False(t, mustNotExist[name],
			"legacy Summary metric %s should not be exposed when flag is on", name)
		if _, ok := wantHistograms[name]; !ok {
			continue
		}
		require.Equal(t, dto.MetricType_HISTOGRAM, mf.GetType(),
			"metric %s should be HISTOGRAM, got %s", name, mf.GetType())
		var native *dto.Histogram
		for _, series := range mf.Metric {
			h := series.Histogram
			if h == nil || h.GetSampleCount() == 0 {
				continue
			}
			native = h
			break
		}
		require.NotNil(t, native, "metric %s has no observed series", name)
		require.NotNil(t, native.Schema, "metric %s missing native Schema", name)
		require.NotEmpty(t, native.PositiveSpan, "metric %s missing PositiveSpan — not in native form", name)
		require.Empty(t, native.Bucket, "metric %s should not expose classic buckets in native-only mode", name)
		wantHistograms[name] = true
	}
	for name, found := range wantHistograms {
		require.True(t, found, "histogram metric %s not present in registry output", name)
	}
}

// TestClientLabelsMetricCacheIsPerLabelCombination pins that every metric
// declaring client labels resolves its Prometheus child per label combination.
//
// Two failures are covered. The caches which resolve a child once and keep it
// exist so the hot path does not re-hash label values on every call, but the
// cached child is bound to the label values it was created with: a key that
// omits them makes the first client to reach a given (code, namespace, frame
// type) own the child forever, so one app_region gets all the traffic and the
// others report zero. And a metric declaring client-label dimensions while
// passing only the base values does not mis-record at all - WithLabelValues
// panics on the cardinality mismatch, taking down whichever goroutine emitted
// it.
func TestClientLabelsMetricCacheIsPerLabelCombination(t *testing.T) {
	t.Parallel()

	m, err := newMetricsRegistry(MetricsConfig{
		MetricsNamespace:                     "test_client_label_cache",
		RegistererGatherer:                   prometheus.NewRegistry(),
		ClientLabels:                         []string{"region"},
		EnableRecoveredPublicationsHistogram: true,
	})
	require.NoError(t, err)

	newClient := func(region string) *Client {
		c := &Client{labels: map[string]string{"region": region}}
		c.labelCombinationCached.Store(m.getOrCreateClientLabelCombinationFromLabels(c.labels))
		return c
	}
	eu, us := newClient("eu"), newClient("us")

	counterValue := func(vec *prometheus.CounterVec, labelValues ...string) float64 {
		t.Helper()
		counter, err := vec.GetMetricWithLabelValues(labelValues...)
		require.NoError(t, err)
		var out dto.Metric
		require.NoError(t, counter.Write(&out))
		return out.GetCounter().GetValue()
	}
	histogramCount := func(vec *prometheus.HistogramVec, labelValues ...string) uint64 {
		t.Helper()
		obs, err := vec.GetMetricWithLabelValues(labelValues...)
		require.NoError(t, err)
		var out dto.Metric
		require.NoError(t, obs.(prometheus.Metric).Write(&out))
		return out.GetHistogram().GetSampleCount()
	}

	// Same non-client labels for both clients - only the client label differs,
	// which is exactly the case a cache key without it collapses.
	for _, c := range []*Client{eu, us} {
		m.incServerDisconnect(3000, c)
		m.incServerUnsubscribe(2000, "ch", c)
		m.incReplyError(protocol.FrameTypeSubscribe, 100, "ch", c)
		m.observeCommandDuration(protocol.FrameTypeSubscribe, time.Millisecond, "ch", c)
		m.incRecover(true, "ch", true, c)
		m.observeRecoveredPublications(3, "ch", c)
		m.observePingPongDuration(time.Millisecond, transportWebsocket, c)
	}

	for _, region := range []string{"eu", "us"} {
		require.Equal(t, float64(1), counterValue(m.serverDisconnectCount, "3000", region),
			"disconnect count for app_region=%s", region)
		require.Equal(t, float64(1), counterValue(m.serverUnsubscribeCount, "2000", "", region),
			"unsubscribe count for app_region=%s", region)
		require.Equal(t, float64(1), counterValue(m.replyErrorCount, "subscribe", "100", "", region),
			"reply error count for app_region=%s", region)
		require.Equal(t, uint64(1), histogramCount(m.commandDurationHistogram, "subscribe", "", region),
			"command duration count for app_region=%s", region)
		require.Equal(t, float64(1), counterValue(m.recoverCount, "yes", "", "yes", region),
			"recover count for app_region=%s", region)
		require.Equal(t, uint64(1), histogramCount(m.recoveredPublications, "", region),
			"recovered publications count for app_region=%s", region)
		require.Equal(t, uint64(1), histogramCount(m.pingPongDurationHistogram, transportWebsocket, region),
			"ping pong count for app_region=%s", region)
	}
}

// TestClientLabelsHotPathsDoNotAllocate pins that the per-message and per-frame
// metric paths reuse the label combination cached on the client instead of
// rebuilding its cache key, which allocated a string on every received command
// and every read frame once ClientLabels was configured.
func TestClientLabelsHotPathsDoNotAllocate(t *testing.T) {
	// Not t.Parallel: testing.AllocsPerRun cannot run in a parallel test.
	m, err := newMetricsRegistry(MetricsConfig{
		MetricsNamespace:   "test_client_label_alloc",
		RegistererGatherer: prometheus.NewRegistry(),
		ClientLabels:       []string{"region"},
	})
	require.NoError(t, err)

	c := &Client{labels: map[string]string{"region": "eu"}}
	c.labelCombinationCached.Store(m.getOrCreateClientLabelCombinationFromLabels(c.labels))

	// Warm the caches so the measured runs only take the hit path.
	m.incTransportMessagesSent(transportWebsocket, protocol.FrameTypePublish, "ch", 100, c)
	m.incTransportMessagesReceived(transportWebsocket, protocol.FrameTypePublish, "ch", 100, c)
	m.observeTransportFrameSize(transportWebsocket, 512, c)

	for _, tc := range []struct {
		name string
		fn   func()
	}{
		{"sent", func() { m.incTransportMessagesSent(transportWebsocket, protocol.FrameTypePublish, "ch", 100, c) }},
		{"received", func() {
			m.incTransportMessagesReceived(transportWebsocket, protocol.FrameTypePublish, "ch", 100, c)
		}},
		{"frame_size", func() { m.observeTransportFrameSize(transportWebsocket, 512, c) }},
	} {
		require.Zero(t, testing.AllocsPerRun(100, tc.fn), "allocations on %s path", tc.name)
	}
}

// TestSubscriptionsAcceptedCounted pins that the subscriptions_accepted counter
// is actually incremented - it was declared and registered but never written,
// so it always reported zero.
func TestSubscriptionsAcceptedCounted(t *testing.T) {
	t.Parallel()

	n, err := New(Config{Metrics: MetricsConfig{
		MetricsNamespace:   "test_subs_accepted",
		RegistererGatherer: prometheus.NewRegistry(),
	}})
	require.NoError(t, err)
	require.NoError(t, n.Run())
	defer func() { _ = n.Shutdown(context.Background()) }()
	n.OnConnect(func(client *Client) {
		client.OnSubscribe(func(_ SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{}, nil)
		})
	})

	client := newTestSubscribedClientV2(t, n, "42", "test_channel")
	require.NotNil(t, client)

	counter, err := n.metrics.subscriptionsAccepted.GetMetricWithLabelValues(client.metricName, "")
	require.NoError(t, err)
	var out dto.Metric
	require.NoError(t, counter.Write(&out))
	require.Equal(t, float64(1), out.GetCounter().GetValue())
}
