package centrifuge

import (
	"strconv"
	"testing"
	"time"

	"github.com/centrifugal/protocol"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func BenchmarkTransportMessagesSent(b *testing.B) {
	m, err := newMetricsRegistry(MetricsConfig{
		MetricsNamespace: "test",
	})
	require.NoError(b, err)

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			m.incTransportMessagesSent("test", protocol.FrameTypePushPublication, "channel"+strconv.Itoa(i%10), 200)
			i++
		}
	})
}

func BenchmarkTransportMessagesReceived(b *testing.B) {
	m, err := newMetricsRegistry(MetricsConfig{
		MetricsNamespace: "test",
	})
	require.NoError(b, err)

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			m.incTransportMessagesReceived("test", protocol.FrameTypePushPublication, "channel"+strconv.Itoa(i%10), 200)
			i++
		}
	})
}

func BenchmarkCommandDuration(b *testing.B) {
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
			m.observeCommandDuration(protocol.FrameTypePresence, time.Since(started), "channel"+strconv.Itoa(i%1024))
			i++
		}
	})
}

func BenchmarkIncReplyError(b *testing.B) {
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
			m.incReplyError(protocol.FrameTypePresence, 100, "channel"+strconv.Itoa(i%1024))
			i++
		}
	})
}

func BenchmarkIncActionCount(b *testing.B) {
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

func BenchmarkIncRecover(b *testing.B) {
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
			m.incRecover(true, "channel"+strconv.Itoa(i%1024))
			i++
		}
	})
}

func BenchmarkIncDisconnect(b *testing.B) {
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
			m.incServerDisconnect(3000)
			i++
		}
	})
}

func BenchmarkIncUnsubscribe(b *testing.B) {
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
			m.incServerUnsubscribe(2500, "channel"+strconv.Itoa(i%1024))
			i++
		}
	})
}

func TestMetrics(t *testing.T) {
	_, err := newMetricsRegistry(MetricsConfig{
		GetChannelNamespaceLabel: func(channel string) string {
			return channel
		},
		ChannelNamespaceCacheTTL: -1,
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
				MetricsNamespace:          tc.metricsNamespace,
				GetChannelNamespaceLabel:  tc.getChannelNamespaceLabel,
				ChannelNamespaceCacheSize: tc.channelNamespaceCacheSize,
				RegistererGatherer:        tc.registererGatherer,
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
					m.incTransportMessagesSent("test", frameType, "channel"+strconv.Itoa(i%2), 200)
					m.incTransportMessagesReceived("test", frameType, "channel"+strconv.Itoa(i%2), 200)
					m.observeCommandDuration(frameType, time.Second, "channel"+strconv.Itoa(i%2))
					m.incReplyError(frameType, 100, "channel"+strconv.Itoa(i%2))
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
				m.incRecover(true, "channel"+strconv.Itoa(i%2))
				m.incRecover(false, "channel"+strconv.Itoa(i%2))
				m.observePubSubDeliveryLag(100)
				m.observePubSubDeliveryLag(-10)
				m.observePingPongDuration(time.Second, transportWebsocket)
				m.incServerDisconnect(3000)
				m.incServerDisconnect(30000)
				m.incServerUnsubscribe(2500, "channel"+strconv.Itoa(i%2))
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
