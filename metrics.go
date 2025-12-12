package centrifuge

import (
	"errors"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/centrifugal/protocol"
	"github.com/maypok86/otter"
	"github.com/prometheus/client_golang/prometheus"
)

// default namespace for prometheus metrics. Can be changed over Config.
var defaultMetricsNamespace = "centrifuge"

var registryMu sync.RWMutex

type metrics struct {
	messagesSentCount             *prometheus.CounterVec
	messagesReceivedCount         *prometheus.CounterVec
	actionCount                   *prometheus.CounterVec
	buildInfoGauge                *prometheus.GaugeVec
	numClientsGauge               prometheus.Gauge
	numUsersGauge                 prometheus.Gauge
	numSubsGauge                  prometheus.Gauge
	numChannelsGauge              prometheus.Gauge
	numNodesGauge                 prometheus.Gauge
	replyErrorCount               *prometheus.CounterVec
	connectionsAccepted           *prometheus.CounterVec
	connectionsInflight           *prometheus.GaugeVec
	subscriptionsAccepted         *prometheus.CounterVec
	subscriptionsInflight         *prometheus.GaugeVec
	serverUnsubscribeCount        *prometheus.CounterVec
	serverDisconnectCount         *prometheus.CounterVec
	commandDurationSummary        *prometheus.SummaryVec
	surveyDurationSummary         *prometheus.SummaryVec
	recoverCount                  *prometheus.CounterVec
	recoveredPublications         *prometheus.HistogramVec
	transportMessagesSent         *prometheus.CounterVec
	transportMessagesSentSize     *prometheus.CounterVec
	transportMessagesReceived     *prometheus.CounterVec
	transportMessagesReceivedSize *prometheus.CounterVec
	tagsFilterDroppedCount        *prometheus.CounterVec

	messagesReceivedCountPublication prometheus.Counter
	messagesReceivedCountJoin        prometheus.Counter
	messagesReceivedCountLeave       prometheus.Counter
	messagesReceivedCountControl     prometheus.Counter

	messagesSentCountPublication prometheus.Counter
	messagesSentCountJoin        prometheus.Counter
	messagesSentCountLeave       prometheus.Counter
	messagesSentCountControl     prometheus.Counter

	commandDurationConnect       prometheus.Observer
	commandDurationSubscribe     prometheus.Observer
	commandDurationUnsubscribe   prometheus.Observer
	commandDurationPublish       prometheus.Observer
	commandDurationPresence      prometheus.Observer
	commandDurationPresenceStats prometheus.Observer
	commandDurationHistory       prometheus.Observer
	commandDurationSend          prometheus.Observer
	commandDurationRPC           prometheus.Observer
	commandDurationRefresh       prometheus.Observer
	commandDurationSubRefresh    prometheus.Observer
	commandDurationUnknown       prometheus.Observer

	broadcastDurationHistogram *prometheus.HistogramVec
	pubSubLagHistogram         prometheus.Histogram
	pingPongDurationHistogram  *prometheus.HistogramVec

	redisBrokerPubSubErrors           *prometheus.CounterVec
	redisBrokerPubSubDroppedMessages  *prometheus.CounterVec
	redisBrokerPubSubBufferedMessages *prometheus.GaugeVec

	config MetricsConfig

	transportMessagesSentCache     sync.Map
	transportMessagesReceivedCache sync.Map
	commandDurationCache           sync.Map
	replyErrorCache                sync.Map
	actionCache                    sync.Map
	recoverCache                   sync.Map
	unsubscribeCache               sync.Map
	disconnectCache                sync.Map
	messagesSentCache              sync.Map
	messagesReceivedCache          sync.Map
	tagsFilterDroppedCache         sync.Map
	nsCache                        *otter.Cache[string, string]
	codeStrings                    map[uint32]string
	clientLabelsWhitelist          map[string]bool

	// Cache for client label value slices to reduce allocations
	clientLabelValuesCache sync.Map // map[*Client][]string
}

func getMetricsNamespace(config MetricsConfig) string {
	if config.MetricsNamespace == "" {
		return defaultMetricsNamespace
	}
	return config.MetricsNamespace
}

// buildMetricLabels creates a label slice, optionally appending client labels if enabled for this metric.
func (m *metrics) buildMetricLabels(subsystemName string, baseLabels []string) []string {
	if len(m.config.ClientLabels) == 0 || !m.clientLabelsWhitelist[subsystemName] {
		return baseLabels
	}
	labels := make([]string, 0, len(baseLabels)+len(m.config.ClientLabels))
	labels = append(labels, baseLabels...)
	labels = append(labels, m.config.ClientLabels...)
	return labels
}

// precomputeClientMetricLabels precomputes and stores client label values and cache key on the client.
// This should be called once during client connect, with client.mu already held by caller.
// This enables zero-allocation metric recording in hot paths.
func (m *metrics) precomputeClientMetricLabels(c *Client) {
	if len(m.config.ClientLabels) == 0 {
		return
	}

	values := make([]string, len(m.config.ClientLabels))
	if c.labels != nil {
		for i, label := range m.config.ClientLabels {
			values[i] = c.labels[label]
		}
	}
	// Store both the values slice and pre-built cache key
	c.metricLabelValues = values
	c.metricLabelCacheKey = buildClientLabelsCacheKey(values)
}

// extractClientLabelValues extracts client label values from a client, returning empty strings for missing labels.
// For hot paths, prefer using the pre-computed c.metricLabelValues field instead (requires client.mu.RLock).
func (m *metrics) extractClientLabelValues(c *Client) []string {
	if len(m.config.ClientLabels) == 0 || c == nil {
		return nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Use pre-computed values if available
	if c.metricLabelValues != nil {
		return c.metricLabelValues
	}

	// Fallback for edge cases
	values := make([]string, len(m.config.ClientLabels))
	if c.labels == nil {
		return values
	}

	for i, label := range m.config.ClientLabels {
		values[i] = c.labels[label]
	}
	return values
}

// buildClientLabelsCacheKey builds a cache key from client label values without allocations.
// Uses strings.Builder with pre-sized buffer to minimize allocations.
func buildClientLabelsCacheKey(values []string) string {
	if len(values) == 0 {
		return ""
	}
	// Pre-calculate size to avoid Builder growth allocations
	size := 0
	for _, v := range values {
		size += len(v) + 1 // +1 for separator
	}
	var b strings.Builder
	b.Grow(size)
	for i, val := range values {
		if i > 0 {
			b.WriteByte(0) // Use null byte as separator
		}
		b.WriteString(val)
	}
	return b.String()
}

func newMetricsRegistry(config MetricsConfig) (*metrics, error) {
	registryMu.Lock()
	defer registryMu.Unlock()

	metricsNamespace := getMetricsNamespace(config)

	var registerer prometheus.Registerer
	if config.RegistererGatherer != nil {
		registerer = config.RegistererGatherer
	} else {
		registerer = prometheus.DefaultRegisterer
	}

	var nsCache *otter.Cache[string, string]
	if config.GetChannelNamespaceLabel != nil {
		cacheSize := config.ChannelNamespaceCacheSize
		if cacheSize == 0 {
			cacheSize = 4096
		}
		cacheTTL := config.ChannelNamespaceCacheTTL
		if cacheTTL == 0 {
			cacheTTL = 15 * time.Second
		}
		if cacheTTL < 0 {
			return nil, errors.New("channel namespace cache TTL must be positive")
		}
		if cacheSize != -1 {
			c, _ := otter.MustBuilder[string, string](cacheSize).
				WithTTL(cacheTTL).
				Build()
			nsCache = &c
		}
	}

	codeStrings := make(map[uint32]string)
	for i := uint32(0); i <= 5000; i++ {
		codeStrings[i] = strconv.FormatUint(uint64(i), 10)
	}

	clientLabelsWhitelist := make(map[string]bool)
	for _, metricName := range config.ClientLabelsMetricWhitelist {
		clientLabelsWhitelist[metricName] = true
	}

	// Auto-whitelist paired metrics that are always used together
	// If transport_messages_sent is whitelisted, also whitelist transport_messages_sent_size
	if clientLabelsWhitelist["transport_messages_sent"] {
		clientLabelsWhitelist["transport_messages_sent_size"] = true
	}
	if clientLabelsWhitelist["transport_messages_sent_size"] {
		clientLabelsWhitelist["transport_messages_sent"] = true
	}
	// If transport_messages_received is whitelisted, also whitelist transport_messages_received_size
	if clientLabelsWhitelist["transport_messages_received"] {
		clientLabelsWhitelist["transport_messages_received_size"] = true
	}
	if clientLabelsWhitelist["transport_messages_received_size"] {
		clientLabelsWhitelist["transport_messages_received"] = true
	}

	m := &metrics{
		config:                config,
		nsCache:               nsCache,
		codeStrings:           codeStrings,
		clientLabelsWhitelist: clientLabelsWhitelist,
	}

	m.messagesSentCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: "node",
		Name:      "messages_sent_count",
		Help:      "Number of messages sent by node to broker.",
	}, m.buildMetricLabels("node_messages_sent_count", []string{"type", "channel_namespace"}))

	m.messagesReceivedCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: "node",
		Name:      "messages_received_count",
		Help:      "Number of messages received from broker.",
	}, m.buildMetricLabels("node_messages_received_count", []string{"type", "channel_namespace"}))

	m.actionCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: "node",
		Name:      "action_count",
		Help:      "Number of various actions called.",
	}, m.buildMetricLabels("node_action_count", []string{"action", "channel_namespace"}))

	m.numClientsGauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Subsystem: "node",
		Name:      "num_clients",
		Help:      "Number of clients connected.",
	})

	m.numUsersGauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Subsystem: "node",
		Name:      "num_users",
		Help:      "Number of unique users connected.",
	})

	m.numSubsGauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Subsystem: "node",
		Name:      "num_subscriptions",
		Help:      "Number of subscriptions.",
	})

	m.numNodesGauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Subsystem: "node",
		Name:      "num_nodes",
		Help:      "Number of nodes in the cluster.",
	})

	m.buildInfoGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Subsystem: "node",
		Name:      "build",
		Help:      "Node build info.",
	}, m.buildMetricLabels("node_build", []string{"version"}))

	m.numChannelsGauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Subsystem: "node",
		Name:      "num_channels",
		Help:      "Number of channels with one or more subscribers.",
	})

	m.surveyDurationSummary = prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Namespace:  metricsNamespace,
		Subsystem:  "node",
		Name:       "survey_duration_seconds",
		Objectives: map[float64]float64{0.5: 0.05, 0.99: 0.001, 0.999: 0.0001},
		Help:       "Survey duration summary.",
	}, m.buildMetricLabels("node_survey_duration_seconds", []string{"op"}))

	m.commandDurationSummary = prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Namespace:  metricsNamespace,
		Subsystem:  "client",
		Name:       "command_duration_seconds",
		Objectives: map[float64]float64{0.5: 0.05, 0.99: 0.001, 0.999: 0.0001},
		Help:       "Client command duration summary.",
	}, m.buildMetricLabels("client_command_duration_seconds", []string{"method", "channel_namespace"}))

	m.replyErrorCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: "client",
		Name:      "num_reply_errors",
		Help:      "Number of errors in replies sent to clients.",
	}, m.buildMetricLabels("client_num_reply_errors", []string{"method", "code", "channel_namespace"}))

	m.serverUnsubscribeCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: "client",
		Name:      "num_server_unsubscribes",
		Help:      "Number of server initiated unsubscribes.",
	}, m.buildMetricLabels("client_num_server_unsubscribes", []string{"code", "channel_namespace"}))

	m.serverDisconnectCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: "client",
		Name:      "num_server_disconnects",
		Help:      "Number of server initiated disconnects.",
	}, m.buildMetricLabels("client_num_server_disconnects", []string{"code"}))

	m.recoverCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: "client",
		Name:      "recover",
		Help:      "Count of recover operations with success/fail resolution.",
	}, m.buildMetricLabels("client_recover", []string{"recovered", "channel_namespace", "has_recovered_publications"}))

	if config.EnableRecoveredPublicationsHistogram {
		m.recoveredPublications = prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: metricsNamespace,
				Subsystem: "client",
				Name:      "recovered_publications",
				Help:      "Number of publications recovered during subscription recovery.",
				Buckets:   []float64{0, 1, 2, 3, 5, 10, 20, 50, 100, 250, 500, 1000, 2000, 5000, 10000},
			},
			m.buildMetricLabels("client_recovered_publications", []string{"channel_namespace"}),
		)
	}

	m.pingPongDurationHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: metricsNamespace,
		Subsystem: "client",
		Name:      "ping_pong_duration_seconds",
		Help:      "Ping/Pong duration in seconds",
		Buckets: []float64{
			0.000100, 0.000250, 0.000500, // Microsecond resolution.
			0.001, 0.005, 0.010, 0.025, 0.050, 0.100, 0.250, 0.500, // Millisecond resolution.
			1.0, 2.5, 5.0, 10.0, // Second resolution.
		}}, m.buildMetricLabels("client_ping_pong_duration_seconds", []string{"transport"}))

	m.connectionsAccepted = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: "client",
		Name:      "connections_accepted",
		Help:      "Count of accepted transports.",
	}, m.buildMetricLabels("client_connections_accepted", []string{"transport", "accept_protocol", "client_name", "client_version"}))

	m.connectionsInflight = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Subsystem: "client",
		Name:      "connections_inflight",
		Help:      "Number of inflight client connections.",
	}, m.buildMetricLabels("client_connections_inflight", []string{"transport", "accept_protocol", "client_name", "client_version"}))

	m.subscriptionsAccepted = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: "client",
		Name:      "subscriptions_accepted",
		Help:      "Count of accepted client subscriptions.",
	}, m.buildMetricLabels("client_subscriptions_accepted", []string{"client_name", "channel_namespace"}))

	m.subscriptionsInflight = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Subsystem: "client",
		Name:      "subscriptions_inflight",
		Help:      "Number of inflight client subscriptions.",
	}, m.buildMetricLabels("client_subscriptions_inflight", []string{"client_name", "channel_namespace"}))

	m.transportMessagesSent = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: "transport",
		Name:      "messages_sent",
		Help:      "Number of messages sent to client connections over specific transport.",
	}, m.buildMetricLabels("transport_messages_sent", []string{"transport", "frame_type", "channel_namespace"}))

	m.transportMessagesSentSize = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: "transport",
		Name:      "messages_sent_size",
		Help:      "MaxSize in bytes of messages sent to client connections over specific transport (uncompressed and does not include framing overhead).",
	}, m.buildMetricLabels("transport_messages_sent_size", []string{"transport", "frame_type", "channel_namespace"}))

	m.transportMessagesReceived = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: "transport",
		Name:      "messages_received",
		Help:      "Number of messages received from client connections over specific transport.",
	}, m.buildMetricLabels("transport_messages_received", []string{"transport", "frame_type", "channel_namespace"}))

	m.transportMessagesReceivedSize = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: "transport",
		Name:      "messages_received_size",
		Help:      "MaxSize in bytes of messages received from client connections over specific transport (uncompressed and does not include framing overhead).",
	}, m.buildMetricLabels("transport_messages_received_size", []string{"transport", "frame_type", "channel_namespace"}))

	m.tagsFilterDroppedCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: "node",
		Name:      "tags_filter_dropped_publications",
		Help:      "Number of publications dropped due to tags filtering.",
	}, m.buildMetricLabels("node_tags_filter_dropped_publications", []string{"channel_namespace"}))

	m.pubSubLagHistogram = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: metricsNamespace,
		Subsystem: "node",
		Name:      "pub_sub_lag_seconds",
		Help:      "Pub sub lag in seconds",
		Buckets:   []float64{0.001, 0.005, 0.010, 0.025, 0.050, 0.100, 0.200, 0.500, 1.000, 2.000, 5.000, 10.000},
	})

	m.broadcastDurationHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: metricsNamespace,
		Subsystem: "node",
		Name:      "broadcast_duration_seconds",
		Help:      "Broadcast duration in seconds",
		Buckets: []float64{
			0.000001, 0.000005, 0.000010, 0.000050, 0.000100, 0.000250, 0.000500, // Microsecond resolution.
			0.001, 0.005, 0.010, 0.025, 0.050, 0.100, 0.250, 0.500, // Millisecond resolution.
			1.0, 2.5, 5.0, 10.0, // Second resolution.
		}}, m.buildMetricLabels("node_broadcast_duration_seconds", []string{"type", "channel_namespace"}))

	m.redisBrokerPubSubErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: "broker",
		Name:      "redis_pub_sub_errors",
		Help:      "Number of times there was an error in Redis PUB/SUB connection.",
	}, m.buildMetricLabels("broker_redis_pub_sub_errors", []string{"broker_name", "error"}))

	m.redisBrokerPubSubDroppedMessages = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: "broker",
		Name:      "redis_pub_sub_dropped_messages",
		Help:      "Number of dropped messages on application level in Redis PUB/SUB.",
	}, m.buildMetricLabels("broker_redis_pub_sub_dropped_messages", []string{"broker_name", "channel_type"}))

	m.redisBrokerPubSubBufferedMessages = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Subsystem: "broker",
		Name:      "redis_pub_sub_buffered_messages",
		Help:      "Number of messages buffered in Redis PUB/SUB.",
	}, m.buildMetricLabels("broker_redis_pub_sub_buffered_messages", []string{"broker_name", "channel_type", "pub_sub_processor"}))

	m.redisBrokerPubSubDroppedMessages.WithLabelValues("", "control").Add(0)
	m.redisBrokerPubSubDroppedMessages.WithLabelValues("", "client").Add(0)

	// Helper to build message labels with empty client labels if configured
	buildMessageLabels := func(msgType string) []string {
		labels := []string{msgType, ""}
		// Check both sent and received metrics as they share the same labels
		if m.clientLabelsWhitelist["node_messages_sent_count"] || m.clientLabelsWhitelist["node_messages_received_count"] {
			for range m.config.ClientLabels {
				labels = append(labels, "")
			}
		}
		return labels
	}

	m.messagesReceivedCountPublication = m.messagesReceivedCount.WithLabelValues(buildMessageLabels("publication")...)
	m.messagesReceivedCountJoin = m.messagesReceivedCount.WithLabelValues(buildMessageLabels("join")...)
	m.messagesReceivedCountLeave = m.messagesReceivedCount.WithLabelValues(buildMessageLabels("leave")...)
	m.messagesReceivedCountControl = m.messagesReceivedCount.WithLabelValues(buildMessageLabels("control")...)

	m.messagesSentCountPublication = m.messagesSentCount.WithLabelValues(buildMessageLabels("publication")...)
	m.messagesSentCountJoin = m.messagesSentCount.WithLabelValues(buildMessageLabels("join")...)
	m.messagesSentCountLeave = m.messagesSentCount.WithLabelValues(buildMessageLabels("leave")...)
	m.messagesSentCountControl = m.messagesSentCount.WithLabelValues(buildMessageLabels("control")...)

	labelForMethod := func(frameType protocol.FrameType) string {
		return frameType.String()
	}

	// Helper to build initial label values with empty client labels if configured
	buildCommandLabels := func(method string) []string {
		labels := []string{method, ""}
		if m.clientLabelsWhitelist["client_command_duration_seconds"] {
			// Append empty strings for each client label
			for range m.config.ClientLabels {
				labels = append(labels, "")
			}
		}
		return labels
	}

	m.commandDurationConnect = m.commandDurationSummary.WithLabelValues(buildCommandLabels(labelForMethod(protocol.FrameTypeConnect))...)
	m.commandDurationSubscribe = m.commandDurationSummary.WithLabelValues(buildCommandLabels(labelForMethod(protocol.FrameTypeSubscribe))...)
	m.commandDurationUnsubscribe = m.commandDurationSummary.WithLabelValues(buildCommandLabels(labelForMethod(protocol.FrameTypeUnsubscribe))...)
	m.commandDurationPublish = m.commandDurationSummary.WithLabelValues(buildCommandLabels(labelForMethod(protocol.FrameTypePublish))...)
	m.commandDurationPresence = m.commandDurationSummary.WithLabelValues(buildCommandLabels(labelForMethod(protocol.FrameTypePresence))...)
	m.commandDurationPresenceStats = m.commandDurationSummary.WithLabelValues(buildCommandLabels(labelForMethod(protocol.FrameTypePresenceStats))...)
	m.commandDurationHistory = m.commandDurationSummary.WithLabelValues(buildCommandLabels(labelForMethod(protocol.FrameTypeHistory))...)
	m.commandDurationSend = m.commandDurationSummary.WithLabelValues(buildCommandLabels(labelForMethod(protocol.FrameTypeSend))...)
	m.commandDurationRPC = m.commandDurationSummary.WithLabelValues(buildCommandLabels(labelForMethod(protocol.FrameTypeRPC))...)
	m.commandDurationRefresh = m.commandDurationSummary.WithLabelValues(buildCommandLabels(labelForMethod(protocol.FrameTypeRefresh))...)
	m.commandDurationSubRefresh = m.commandDurationSummary.WithLabelValues(buildCommandLabels(labelForMethod(protocol.FrameTypeSubRefresh))...)
	m.commandDurationUnknown = m.commandDurationSummary.WithLabelValues(buildCommandLabels("unknown")...)

	var alreadyRegistered prometheus.AlreadyRegisteredError

	for _, collector := range []prometheus.Collector{
		m.messagesSentCount,
		m.messagesReceivedCount,
		m.actionCount,
		m.numClientsGauge,
		m.numUsersGauge,
		m.numSubsGauge,
		m.numChannelsGauge,
		m.numNodesGauge,
		m.commandDurationSummary,
		m.replyErrorCount,
		m.connectionsAccepted,
		m.connectionsInflight,
		m.subscriptionsAccepted,
		m.subscriptionsInflight,
		m.serverUnsubscribeCount,
		m.serverDisconnectCount,
		m.recoverCount,
		m.pingPongDurationHistogram,
		m.transportMessagesSent,
		m.transportMessagesSentSize,
		m.transportMessagesReceived,
		m.transportMessagesReceivedSize,
		m.tagsFilterDroppedCount,
		m.buildInfoGauge,
		m.surveyDurationSummary,
		m.pubSubLagHistogram,
		m.broadcastDurationHistogram,
		m.redisBrokerPubSubErrors,
		m.redisBrokerPubSubDroppedMessages,
		m.redisBrokerPubSubBufferedMessages,
	} {
		if err := registerer.Register(collector); err != nil && !errors.As(err, &alreadyRegistered) {
			return nil, err
		}
	}

	if config.EnableRecoveredPublicationsHistogram {
		if err := registerer.Register(m.recoveredPublications); err != nil && !errors.As(err, &alreadyRegistered) {
			return nil, err
		}
	}

	return m, nil
}

func (m *metrics) incRedisBrokerPubSubErrors(name string, error string) {
	m.redisBrokerPubSubErrors.WithLabelValues(name, error).Inc()
}

func (m *metrics) getChannelNamespaceLabel(ch string) string {
	if ch == "" {
		return ""
	}
	nsLabel := ""
	if m.config.GetChannelNamespaceLabel == nil {
		return nsLabel
	}
	if m.nsCache == nil {
		return m.config.GetChannelNamespaceLabel(ch)
	}
	var cached bool
	if nsLabel, cached = m.nsCache.Get(ch); cached {
		return nsLabel
	}
	nsLabel = m.config.GetChannelNamespaceLabel(ch)
	m.nsCache.Set(ch, nsLabel)
	return nsLabel
}

type commandDurationLabels struct {
	ChannelNamespace string
	FrameType        protocol.FrameType
}

func (m *metrics) observeCommandDuration(frameType protocol.FrameType, d time.Duration, ch string, c *Client) {
	channelNamespace := m.getChannelNamespaceLabel(ch)

	// Check if we need to add client labels
	clientLabelValues := m.extractClientLabelValues(c)

	if (ch != "" && m.config.GetChannelNamespaceLabel != nil) || clientLabelValues != nil {
		labels := commandDurationLabels{
			ChannelNamespace: channelNamespace,
			FrameType:        frameType,
		}
		summary, ok := m.commandDurationCache.Load(labels)
		if !ok {
			labelValues := []string{frameType.String(), channelNamespace}
			if clientLabelValues != nil {
				labelValues = append(labelValues, clientLabelValues...)
			}
			summary = m.commandDurationSummary.WithLabelValues(labelValues...)
			m.commandDurationCache.Store(labels, summary)
		}
		summary.(prometheus.Observer).Observe(d.Seconds())
		return
	}

	var observer prometheus.Observer

	switch frameType {
	case protocol.FrameTypeConnect:
		observer = m.commandDurationConnect
	case protocol.FrameTypeSubscribe:
		observer = m.commandDurationSubscribe
	case protocol.FrameTypeUnsubscribe:
		observer = m.commandDurationUnsubscribe
	case protocol.FrameTypePublish:
		observer = m.commandDurationPublish
	case protocol.FrameTypePresence:
		observer = m.commandDurationPresence
	case protocol.FrameTypePresenceStats:
		observer = m.commandDurationPresenceStats
	case protocol.FrameTypeHistory:
		observer = m.commandDurationHistory
	case protocol.FrameTypeSend:
		observer = m.commandDurationSend
	case protocol.FrameTypeRPC:
		observer = m.commandDurationRPC
	case protocol.FrameTypeRefresh:
		observer = m.commandDurationRefresh
	case protocol.FrameTypeSubRefresh:
		observer = m.commandDurationSubRefresh
	default:
		observer = m.commandDurationUnknown
	}
	observer.Observe(d.Seconds())
}

func (m *metrics) observePubSubDeliveryLag(lagTimeMilli int64) {
	if lagTimeMilli < 0 {
		lagTimeMilli = -lagTimeMilli
	}
	m.pubSubLagHistogram.Observe(float64(lagTimeMilli) / 1000)
}

func (m *metrics) observeBroadcastDuration(started time.Time, ch string) {
	if m.config.GetChannelNamespaceLabel != nil {
		m.broadcastDurationHistogram.WithLabelValues("publication", m.getChannelNamespaceLabel(ch)).Observe(time.Since(started).Seconds())
		return
	}
	m.broadcastDurationHistogram.WithLabelValues("publication", m.getChannelNamespaceLabel(ch)).Observe(time.Since(started).Seconds())
}

func (m *metrics) observePingPongDuration(duration time.Duration, transport string) {
	m.pingPongDurationHistogram.WithLabelValues(transport).Observe(duration.Seconds())
}

func (m *metrics) setBuildInfo(version string) {
	m.buildInfoGauge.WithLabelValues(version).Set(1)
}

func (m *metrics) setNumClients(n float64) {
	m.numClientsGauge.Set(n)
}

func (m *metrics) setNumUsers(n float64) {
	m.numUsersGauge.Set(n)
}

func (m *metrics) setNumSubscriptions(n float64) {
	m.numSubsGauge.Set(n)
}

func (m *metrics) setNumChannels(n float64) {
	m.numChannelsGauge.Set(n)
}

func (m *metrics) setNumNodes(n float64) {
	m.numNodesGauge.Set(n)
}

type replyErrorLabels struct {
	FrameType        protocol.FrameType
	ChannelNamespace string
	Code             string
}

func (m *metrics) incReplyError(frameType protocol.FrameType, code uint32, ch string, c *Client) {
	channelNamespace := m.getChannelNamespaceLabel(ch)
	labels := replyErrorLabels{
		ChannelNamespace: channelNamespace,
		FrameType:        frameType,
		Code:             m.getCodeLabel(code),
	}
	counter, ok := m.replyErrorCache.Load(labels)
	if !ok {
		labelValues := []string{frameType.String(), labels.Code, channelNamespace}
		if clientLabelValues := m.extractClientLabelValues(c); clientLabelValues != nil {
			labelValues = append(labelValues, clientLabelValues...)
		}
		counter = m.replyErrorCount.WithLabelValues(labelValues...)
		m.replyErrorCache.Store(labels, counter)
	}
	counter.(prometheus.Counter).Inc()
}

type recoverLabels struct {
	ChannelNamespace string
	Success          string
	HasPublications  string
}

func (m *metrics) incRecover(success bool, ch string, hasPublications bool) {
	var successStr string
	if success {
		successStr = "yes"
	} else {
		successStr = "no"
	}
	var hasPubsStr string
	if hasPublications {
		hasPubsStr = "yes"
	} else {
		hasPubsStr = "no"
	}
	channelNamespace := m.getChannelNamespaceLabel(ch)
	labels := recoverLabels{
		ChannelNamespace: channelNamespace,
		Success:          successStr,
		HasPublications:  hasPubsStr,
	}
	counter, ok := m.recoverCache.Load(labels)
	if !ok {
		counter = m.recoverCount.WithLabelValues(successStr, channelNamespace, hasPubsStr)
		m.recoverCache.Store(labels, counter)
	}
	counter.(prometheus.Counter).Inc()
}

func (m *metrics) observeRecoveredPublications(count int, ch string) {
	if m.recoveredPublications != nil {
		channelNamespace := m.getChannelNamespaceLabel(ch)
		m.recoveredPublications.WithLabelValues(channelNamespace).Observe(float64(count))
	}
}

type transportMessageLabels struct {
	Transport        string
	ChannelNamespace string
	FrameType        string
	ClientLabels     string // Concatenated client label values for cache key
}

type transportMessagesSent struct {
	counterSent     prometheus.Counter
	counterSentSize prometheus.Counter
}

type transportMessagesReceived struct {
	counterReceived     prometheus.Counter
	counterReceivedSize prometheus.Counter
}

func (m *metrics) getTransportMessagesSentCounters(transport string, frameType string, namespace string, clientLabelValues []string, clientLabelCacheKey string) transportMessagesSent {
	// If either sent or sent_size metric is whitelisted, apply client labels to BOTH since they're always used together
	if (m.clientLabelsWhitelist["transport_messages_sent"] || m.clientLabelsWhitelist["transport_messages_sent_size"]) && len(m.config.ClientLabels) > 0 {
		if clientLabelValues == nil {
			// Create empty strings for missing client labels
			clientLabelValues = make([]string, len(m.config.ClientLabels))
			clientLabelCacheKey = buildClientLabelsCacheKey(clientLabelValues)
		}
	}

	// Use pre-computed cache key - no allocation here
	labels := transportMessageLabels{
		Transport:        transport,
		ChannelNamespace: namespace,
		FrameType:        frameType,
		ClientLabels:     clientLabelCacheKey,
	}
	counters, ok := m.transportMessagesSentCache.Load(labels)
	if !ok {
		// Pre-allocate labelValues with exact capacity to avoid append reallocation
		labelValues := make([]string, 0, 3+len(clientLabelValues))
		labelValues = append(labelValues, transport, frameType, namespace)
		labelValues = append(labelValues, clientLabelValues...)

		counterSent := m.transportMessagesSent.WithLabelValues(labelValues...)
		counterSentSize := m.transportMessagesSentSize.WithLabelValues(labelValues...)
		counters = transportMessagesSent{
			counterSent:     counterSent,
			counterSentSize: counterSentSize,
		}
		m.transportMessagesSentCache.Store(labels, counters)
	}
	return counters.(transportMessagesSent)
}

func (m *metrics) incTransportMessagesSent(transport string, frameType protocol.FrameType, channel string, size int, c *Client) {
	channelNamespace := m.getChannelNamespaceLabel(channel)

	var clientLabelValues []string
	var clientLabelCacheKey string
	if c != nil && len(m.config.ClientLabels) > 0 {
		c.mu.RLock()
		clientLabelValues = c.metricLabelValues
		clientLabelCacheKey = c.metricLabelCacheKey
		c.mu.RUnlock()
	}

	counters := m.getTransportMessagesSentCounters(transport, frameType.String(), channelNamespace, clientLabelValues, clientLabelCacheKey)
	counters.counterSent.Inc()
	counters.counterSentSize.Add(float64(size))
}

func (m *metrics) incTransportMessagesReceived(transport string, frameType protocol.FrameType, channel string, size int, c *Client) {
	channelNamespace := m.getChannelNamespaceLabel(channel)
	clientLabelValues := m.extractClientLabelValues(c)

	// If either received or received_size metric is whitelisted, apply client labels to BOTH since they're always used together
	if (m.clientLabelsWhitelist["transport_messages_received"] || m.clientLabelsWhitelist["transport_messages_received_size"]) && len(m.config.ClientLabels) > 0 {
		if clientLabelValues == nil {
			clientLabelValues = make([]string, len(m.config.ClientLabels))
		}
	}

	// Build cache key including client labels
	clientLabelsKey := buildClientLabelsCacheKey(clientLabelValues)

	labels := transportMessageLabels{
		Transport:        transport,
		ChannelNamespace: channelNamespace,
		FrameType:        frameType.String(),
		ClientLabels:     clientLabelsKey,
	}
	counters, ok := m.transportMessagesReceivedCache.Load(labels)
	if !ok {
		labelValues := []string{transport, labels.FrameType, channelNamespace}
		if len(clientLabelValues) > 0 {
			labelValues = append(labelValues, clientLabelValues...)
		}
		counterReceived := m.transportMessagesReceived.WithLabelValues(labelValues...)
		counterReceivedSize := m.transportMessagesReceivedSize.WithLabelValues(labelValues...)
		counters = transportMessagesReceived{
			counterReceived:     counterReceived,
			counterReceivedSize: counterReceivedSize,
		}
		m.transportMessagesReceivedCache.Store(labels, counters)
	}
	counters.(transportMessagesReceived).counterReceived.Inc()
	counters.(transportMessagesReceived).counterReceivedSize.Add(float64(size))
}

func (m *metrics) getCodeLabel(code uint32) string {
	codeStr, ok := m.codeStrings[code]
	if !ok {
		return strconv.FormatUint(uint64(code), 10)
	}
	return codeStr
}

type disconnectLabels struct {
	Code string
}

func (m *metrics) incServerDisconnect(code uint32, c *Client) {
	labels := disconnectLabels{
		Code: m.getCodeLabel(code),
	}
	counter, ok := m.disconnectCache.Load(labels)
	if !ok {
		labelValues := []string{labels.Code}
		if clientLabelValues := m.extractClientLabelValues(c); clientLabelValues != nil {
			labelValues = append(labelValues, clientLabelValues...)
		}
		counter = m.serverDisconnectCount.WithLabelValues(labelValues...)
		m.disconnectCache.Store(labels, counter)
	}
	counter.(prometheus.Counter).Inc()
}

type unsubscribeLabels struct {
	Code             string
	ChannelNamespace string
}

func (m *metrics) incServerUnsubscribe(code uint32, ch string, c *Client) {
	labels := unsubscribeLabels{
		Code:             m.getCodeLabel(code),
		ChannelNamespace: m.getChannelNamespaceLabel(ch),
	}
	counter, ok := m.unsubscribeCache.Load(labels)
	if !ok {
		labelValues := []string{labels.Code, labels.ChannelNamespace}
		if clientLabelValues := m.extractClientLabelValues(c); clientLabelValues != nil {
			labelValues = append(labelValues, clientLabelValues...)
		}
		counter = m.serverUnsubscribeCount.WithLabelValues(labelValues...)
		m.unsubscribeCache.Store(labels, counter)
	}
	counter.(prometheus.Counter).Inc()
}

type messageSentLabels struct {
	MsgType          string
	ChannelNamespace string
}

func (m *metrics) incMessagesSent(msgType string, ch string) {
	if m.config.GetChannelNamespaceLabel != nil {
		labels := messageSentLabels{
			MsgType:          msgType,
			ChannelNamespace: m.getChannelNamespaceLabel(ch),
		}
		counter, ok := m.messagesSentCache.Load(labels)
		if !ok {
			counter = m.messagesSentCount.WithLabelValues(msgType, labels.ChannelNamespace)
			m.messagesSentCache.Store(labels, counter)
		}
		counter.(prometheus.Counter).Inc()
		return
	}
	switch msgType {
	case "publication":
		m.messagesSentCountPublication.Inc()
	case "join":
		m.messagesSentCountJoin.Inc()
	case "leave":
		m.messagesSentCountLeave.Inc()
	case "control":
		m.messagesSentCountControl.Inc()
	default:
		m.messagesSentCount.WithLabelValues(msgType, "").Inc()
	}
}

type messageReceivedLabels struct {
	MsgType          string
	ChannelNamespace string
}

func (m *metrics) incMessagesReceived(msgType string, ch string) {
	if m.config.GetChannelNamespaceLabel != nil {
		labels := messageReceivedLabels{
			MsgType:          msgType,
			ChannelNamespace: m.getChannelNamespaceLabel(ch),
		}
		counter, ok := m.messagesReceivedCache.Load(labels)
		if !ok {
			counter = m.messagesReceivedCount.WithLabelValues(msgType, labels.ChannelNamespace)
			m.messagesReceivedCache.Store(labels, counter)
		}
		counter.(prometheus.Counter).Inc()
		return
	}
	switch msgType {
	case "publication":
		m.messagesReceivedCountPublication.Inc()
	case "join":
		m.messagesReceivedCountJoin.Inc()
	case "leave":
		m.messagesReceivedCountLeave.Inc()
	case "control":
		m.messagesReceivedCountControl.Inc()
	default:
		m.messagesReceivedCount.WithLabelValues(msgType, "").Inc()
	}
}

type actionLabels struct {
	Action           string
	ChannelNamespace string
}

func (m *metrics) incActionCount(action string, ch string) {
	channelNamespace := m.getChannelNamespaceLabel(ch)
	labels := actionLabels{
		ChannelNamespace: channelNamespace,
		Action:           action,
	}
	counter, ok := m.actionCache.Load(labels)
	if !ok {
		counter = m.actionCount.WithLabelValues(action, channelNamespace)
		m.actionCache.Store(labels, counter)
	}
	counter.(prometheus.Counter).Inc()
}

func (m *metrics) observeSurveyDuration(op string, d time.Duration) {
	m.surveyDurationSummary.WithLabelValues(op).Observe(d.Seconds())
}

type tagsFilterDroppedLabels struct {
	ChannelNamespace string
}

func (m *metrics) incTagsFilterDropped(ch string, count int) {
	channelNamespace := m.getChannelNamespaceLabel(ch)
	labels := tagsFilterDroppedLabels{
		ChannelNamespace: channelNamespace,
	}
	counter, ok := m.tagsFilterDroppedCache.Load(labels)
	if !ok {
		counter = m.tagsFilterDroppedCount.WithLabelValues(channelNamespace)
		m.tagsFilterDroppedCache.Store(labels, counter)
	}
	counter.(prometheus.Counter).Add(float64(count))
}

// getAcceptProtocolLabel returns the transport accept protocol label based on HTTP version.
func getAcceptProtocolLabel(protoMajor int8) string {
	switch protoMajor {
	case 3:
		return "h3"
	case 2:
		return "h2"
	case 1:
		return "h1"
	default:
		return "unknown"
	}
}
