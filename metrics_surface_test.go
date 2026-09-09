package centrifuge

import (
	"testing"
	"time"

	"github.com/centrifugal/protocol"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// TestMetricsSurfaceUnderFullConfig calls every metric recording path with all
// MetricsConfig options turned on.
//
// A metric declaring label dimensions its recording path does not fill does not
// mis-record - prometheus.WithLabelValues panics on the cardinality mismatch,
// killing whichever goroutine emitted it. Three metrics shipped that way once
// ClientLabels was configured, and nothing exercised them under that config, so
// this walks the whole surface with every option enabled.
func TestMetricsSurfaceUnderFullConfig(t *testing.T) {
	t.Parallel()

	for _, clientLabels := range [][]string{nil, {"region", "tier"}} {
		m, err := newMetricsRegistry(MetricsConfig{
			MetricsNamespace:                     "test_surface",
			RegistererGatherer:                   prometheus.NewRegistry(),
			ClientLabels:                         clientLabels,
			GetChannelNamespaceLabel:             func(ch string) string { return "ns" },
			EnableRecoveredPublicationsHistogram: true,
			ExposeTransportAcceptProtocol:        true,
			RegisteredClientNames:                []string{"js"},
		})
		require.NoError(t, err)

		// Both a client carrying labels and no client at all: the latter is what
		// a metric recorded before connect completes sees.
		labelled := &Client{labels: map[string]string{"region": "eu", "tier": "paid"}}
		labelled.labelCombinationCached.Store(m.getOrCreateClientLabelCombinationFromLabels(labelled.labels))

		for _, c := range []*Client{labelled, nil} {
			for _, frameType := range []protocol.FrameType{protocol.FrameTypeConnect, protocol.FrameTypePublish} {
				m.observeCommandDuration(frameType, time.Millisecond, "ch", c)
				m.incReplyError(frameType, 100, "ch", c)
				m.incTransportMessagesSent(transportWebsocket, frameType, "ch", 10, c)
				m.incTransportMessagesReceived(transportWebsocket, frameType, "ch", 10, c)
			}
			m.observeTransportFrameSize(transportWebsocket, 512, c)
			m.observePingPongDuration(time.Millisecond, transportWebsocket, c)
			m.incServerDisconnect(3000, c)
			m.incServerUnsubscribe(2000, "ch", c)
			m.incRecover(true, "ch", true, c)
			m.incRecover(false, "ch", false, c)
			m.observeRecoveredPublications(3, "ch", c)
		}

		// Paths that carry no client.
		m.incTransportOutgoingClose(transportWebsocket, 3000)
		m.observePubSubDeliveryLag(100, "ch")
		m.observeBroadcastDuration(time.Now(), "ch")
		m.incMessagesSent("publication", "ch")
		m.incMessagesReceived("publication", "ch")
		m.incActionCount("survey", "ch")
		m.observeSurveyDuration("op", time.Millisecond)
		m.incTagsFilterDropped("ch", 2)
		m.incRedisBrokerPubSubErrors("broker", "err")
		m.incBrokerPublishSuppressed(SuppressReasonIdempotency, "ch")
		m.incMapBrokerPublishSuppressed(SuppressReasonVersion, "ch")
		m.incMapBrokerRemoveSuppressed(SuppressReasonKeyExists, "ch")
		m.setMapBrokerCleanupLag("broker", 1)
		m.addMapBrokerCleanupRemoved("broker", 1)
		m.incMapBrokerCleanupErrors("broker")
		m.setBuildInfo("1.0.0")
		m.setNumClients(1)
		m.setNumUsers(1)
		m.setNumSubscriptions(1)
		m.setNumChannels(1)
		m.setNumNodes(1)
		m.setSharedPollNumChannels(1)
		m.setSharedPollNumKeys(1)

		handler := m.getSharedPollHandlerCached("trigger", "ch")
		handler.errorCount.Inc()
		handler.itemsPolled.Inc()
		result := m.getSharedPollResultCached("trigger", "ch")
		result.changed.Inc()
		channel := m.getSharedPollChannelCached("ch")
		channel.notifyCount.Inc()
		publish := m.getSharedPollPublishCached("ch")
		publish.applied.Inc()

		// Everything recorded must also be gatherable: a vec whose children were
		// built with the wrong number of values would fail collection here.
		_, err = m.config.RegistererGatherer.Gather()
		require.NoError(t, err)
	}
}
