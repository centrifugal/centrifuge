package centrifuge

import (
	"strconv"
	"testing"
	"time"

	"github.com/centrifugal/protocol"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func BenchmarkMetricsTransportMessagesSent(b *testing.B) {
	m, err := newMetricsRegistry(MetricsConfig{
		MetricsNamespace: "test",
	})
	require.NoError(b, err)

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			m.incTransportMessagesSent("test", protocol.FrameTypePushPublication, "channel"+strconv.Itoa(i%10), 200, nil)
			i++
		}
	})
}

func BenchmarkMetricsTransportMessagesReceived(b *testing.B) {
	m, err := newMetricsRegistry(MetricsConfig{
		MetricsNamespace: "test",
	})
	require.NoError(b, err)

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			m.incTransportMessagesReceived("test", protocol.FrameTypePushPublication, "channel"+strconv.Itoa(i%10), 200, nil)
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

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			started := time.Now()
			m.observeCommandDuration(protocol.FrameTypePresence, time.Since(started), "channel"+strconv.Itoa(i%1024), nil)
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

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			m.incReplyError(protocol.FrameTypePresence, 100, "channel"+strconv.Itoa(i%1024), nil)
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

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			m.incActionCount("history", "channel"+strconv.Itoa(i%1024))
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

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			m.incRecover(true, "channel"+strconv.Itoa(i%1024), false)
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

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			m.incServerUnsubscribe(2500, "channel"+strconv.Itoa(i%1024), nil)
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
		ClientLabelsMetricWhitelist: []string{
			"client_num_reply_errors",
		},
	})
	require.NoError(b, err)

	// Pre-create 100 clients with different label combinations
	// Cardinality: 5 regions × 4 tiers = 20 unique combinations
	regions := []string{"us-east-1", "us-west-2", "eu-west-1", "ap-south-1", "ap-northeast-1"}
	tiers := []string{"free", "standard", "premium", "enterprise"}
	clients := make([]*Client, 100)
	for i := 0; i < 100; i++ {
		clients[i] = &Client{
			labels: map[string]string{
				"region": regions[i%len(regions)],
				"tier":   tiers[i%len(tiers)],
			},
		}
		// Precompute metric labels to simulate real-world usage
		m.precomputeClientMetricLabels(clients[i])
	}

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			client := clients[i%len(clients)]
			m.incReplyError(protocol.FrameTypePresence, 100, "channel"+strconv.Itoa(i%1024), client)
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
		ClientLabelsMetricWhitelist: []string{
			"client_num_server_disconnects",
		},
	})
	require.NoError(b, err)

	// Pre-create 100 clients with different label combinations
	regions := []string{"us-east-1", "us-west-2", "eu-west-1", "ap-south-1", "ap-northeast-1"}
	tiers := []string{"free", "standard", "premium", "enterprise"}
	clients := make([]*Client, 100)
	for i := 0; i < 100; i++ {
		clients[i] = &Client{
			labels: map[string]string{
				"region": regions[i%len(regions)],
				"tier":   tiers[i%len(tiers)],
			},
		}
		// Precompute metric labels to simulate real-world usage
		m.precomputeClientMetricLabels(clients[i])
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
		ClientLabelsMetricWhitelist: []string{
			"client_num_server_unsubscribes",
		},
	})
	require.NoError(b, err)

	// Pre-create 100 clients with different label combinations
	regions := []string{"us-east-1", "us-west-2", "eu-west-1", "ap-south-1", "ap-northeast-1"}
	tiers := []string{"free", "standard", "premium", "enterprise"}
	clients := make([]*Client, 100)
	for i := 0; i < 100; i++ {
		clients[i] = &Client{
			labels: map[string]string{
				"region": regions[i%len(regions)],
				"tier":   tiers[i%len(tiers)],
			},
		}
		// Precompute metric labels to simulate real-world usage
		m.precomputeClientMetricLabels(clients[i])
	}

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			client := clients[i%len(clients)]
			m.incServerUnsubscribe(2500, "channel"+strconv.Itoa(i%1024), client)
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
		ClientLabelsMetricWhitelist: []string{
			"transport_messages_sent",
		},
	})
	require.NoError(b, err)

	// Pre-create 100 clients with different label combinations
	regions := []string{"us-east-1", "us-west-2", "eu-west-1", "ap-south-1", "ap-northeast-1"}
	tiers := []string{"free", "standard", "premium", "enterprise"}
	clients := make([]*Client, 100)
	for i := 0; i < 100; i++ {
		clients[i] = &Client{
			labels: map[string]string{
				"region": regions[i%len(regions)],
				"tier":   tiers[i%len(tiers)],
			},
		}
		// Precompute metric labels to simulate real-world usage
		m.precomputeClientMetricLabels(clients[i])
	}

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			client := clients[i%len(clients)]
			m.incTransportMessagesSent("test", protocol.FrameTypePushPublication, "channel"+strconv.Itoa(i%10), 200, client)
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
		ClientLabelsMetricWhitelist: []string{
			"transport_messages_sent",
		},
	})
	require.NoError(b, err)

	// Pre-create a client with labels
	client := &Client{
		labels: map[string]string{
			"region": "us-east-1",
			"tier":   "premium",
		},
	}
	m.precomputeClientMetricLabels(client)

	// Pre-extract values to simulate hot path
	clientLabelValues := client.metricLabelValues
	clientLabelCacheKey := client.metricLabelCacheKey

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
		ClientLabelsMetricWhitelist: []string{
			"client_command_duration_seconds",
		},
	})
	require.NoError(b, err)

	// Pre-create 100 clients with different label combinations
	regions := []string{"us-east-1", "us-west-2", "eu-west-1", "ap-south-1", "ap-northeast-1"}
	tiers := []string{"free", "standard", "premium", "enterprise"}
	clients := make([]*Client, 100)
	for i := 0; i < 100; i++ {
		clients[i] = &Client{
			labels: map[string]string{
				"region": regions[i%len(regions)],
				"tier":   tiers[i%len(tiers)],
			},
		}
		// Precompute metric labels to simulate real-world usage
		m.precomputeClientMetricLabels(clients[i])
	}

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			client := clients[i%len(clients)]
			m.observeCommandDuration(protocol.FrameTypeSubscribe, time.Millisecond, "channel"+strconv.Itoa(i%1024), client)
			i++
		}
	})
}

func TestMetrics(t *testing.T) {
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
				m.incRecover(true, "channel"+strconv.Itoa(i%2), false)
				m.incRecover(false, "channel"+strconv.Itoa(i%2), false)
				m.observeRecoveredPublications(10, "channel"+strconv.Itoa(i%2))
				m.observePubSubDeliveryLag(100)
				m.observePubSubDeliveryLag(-10)
				m.observePingPongDuration(time.Second, transportWebsocket)
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
			ClientLabelsMetricWhitelist: []string{
				// Only need to whitelist sent (size is auto-whitelisted as they're paired)
				"transport_messages_sent",
				"client_command_duration_seconds",
			},
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
			ClientLabelsMetricWhitelist: []string{
				// Only whitelist sent - size is auto-paired
				"transport_messages_sent",
			},
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
			// Empty whitelist - no metrics should get client labels
			ClientLabelsMetricWhitelist: []string{},
		})
		require.NoError(t, err)

		// Metrics should work but without client labels (not whitelisted)
		m.incTransportMessagesSent("ws", protocol.FrameTypePushPublication, "channel", 100, nil)
		m.observeCommandDuration(protocol.FrameTypeConnect, time.Millisecond, "", nil)
	})

	t.Run("extractClientLabelValues with nil client", func(t *testing.T) {
		registry := prometheus.NewRegistry()
		m, err := newMetricsRegistry(MetricsConfig{
			MetricsNamespace:            "test5",
			RegistererGatherer:          registry,
			ClientLabels:                []string{"region"},
			ClientLabelsMetricWhitelist: []string{"transport_messages_sent"},
		})
		require.NoError(t, err)

		// Should handle nil client gracefully
		values := m.extractClientLabelValues(nil)
		require.Nil(t, values)
	})

	t.Run("extractClientLabelValues with client without labels", func(t *testing.T) {
		registry := prometheus.NewRegistry()
		m, err := newMetricsRegistry(MetricsConfig{
			MetricsNamespace:            "test6",
			RegistererGatherer:          registry,
			ClientLabels:                []string{"region"},
			ClientLabelsMetricWhitelist: []string{"transport_messages_sent"},
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

		labels := m.buildMetricLabels("transport_messages_sent", []string{"transport", "frame_type"})
		require.Equal(t, []string{"transport", "frame_type"}, labels)
	})

	t.Run("buildMetricLabels with client labels and whitelisted", func(t *testing.T) {
		registry := prometheus.NewRegistry()
		m, err := newMetricsRegistry(MetricsConfig{
			MetricsNamespace:            "test8",
			RegistererGatherer:          registry,
			ClientLabels:                []string{"region", "tier"},
			ClientLabelsMetricWhitelist: []string{"transport_messages_sent"},
		})
		require.NoError(t, err)

		labels := m.buildMetricLabels("transport_messages_sent", []string{"transport", "frame_type"})
		require.Equal(t, []string{"transport", "frame_type", "region", "tier"}, labels)
	})

	t.Run("buildMetricLabels with client labels but not whitelisted", func(t *testing.T) {
		registry := prometheus.NewRegistry()
		m, err := newMetricsRegistry(MetricsConfig{
			MetricsNamespace:            "test9",
			RegistererGatherer:          registry,
			ClientLabels:                []string{"region", "tier"},
			ClientLabelsMetricWhitelist: []string{"other_metric"},
		})
		require.NoError(t, err)

		labels := m.buildMetricLabels("transport_messages_sent", []string{"transport", "frame_type"})
		require.Equal(t, []string{"transport", "frame_type"}, labels) // No client labels added
	})

	t.Run("paired metrics auto-whitelisting", func(t *testing.T) {
		registry := prometheus.NewRegistry()
		m, err := newMetricsRegistry(MetricsConfig{
			MetricsNamespace:   "test10",
			RegistererGatherer: registry,
			ClientLabels:       []string{"region"},
			ClientLabelsMetricWhitelist: []string{
				// Only whitelist sent, size should be auto-paired
				"transport_messages_sent",
			},
		})
		require.NoError(t, err)

		// Both sent and sent_size should have client labels
		require.True(t, m.clientLabelsWhitelist["transport_messages_sent"])
		require.True(t, m.clientLabelsWhitelist["transport_messages_sent_size"])

		// Should work without errors
		m.incTransportMessagesSent("ws", protocol.FrameTypePushPublication, "channel", 100, nil)
	})

	t.Run("paired metrics auto-whitelisting via size metric", func(t *testing.T) {
		registry := prometheus.NewRegistry()
		m, err := newMetricsRegistry(MetricsConfig{
			MetricsNamespace:   "test11",
			RegistererGatherer: registry,
			ClientLabels:       []string{"tier"},
			ClientLabelsMetricWhitelist: []string{
				// Only whitelist size, sent should be auto-paired
				"transport_messages_received_size",
			},
		})
		require.NoError(t, err)

		// Both received and received_size should have client labels
		require.True(t, m.clientLabelsWhitelist["transport_messages_received"])
		require.True(t, m.clientLabelsWhitelist["transport_messages_received_size"])

		// Should work without errors
		m.incTransportMessagesReceived("ws", protocol.FrameTypePushPublication, "channel", 100, nil)
	})
}

func Test_getHTTPTransportProto(t *testing.T) {
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
