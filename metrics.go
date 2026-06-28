package centrifuge

import (
	"errors"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/centrifugal/protocol"
	"github.com/maypok86/otter/v2"
	"github.com/prometheus/client_golang/prometheus"
)

// default namespace for prometheus metrics. Can be changed over Config.
var defaultMetricsNamespace = "centrifuge"

var registryMu sync.RWMutex

// clientMetricDef defines a Prometheus metric with its subsystem and name.
type clientMetricDef struct {
	Subsystem string
	Name      string
}

// Client metric definitions.
var (
	metricClientConnectionsAccepted = clientMetricDef{
		Subsystem: "client",
		Name:      "connections_accepted",
	}
	metricClientConnectionsInflight = clientMetricDef{
		Subsystem: "client",
		Name:      "connections_inflight",
	}
	metricClientSubscriptionsAccepted = clientMetricDef{
		Subsystem: "client",
		Name:      "subscriptions_accepted",
	}
	metricClientSubscriptionsInflight = clientMetricDef{
		Subsystem: "client",
		Name:      "subscriptions_inflight",
	}
	metricClientCommandDuration = clientMetricDef{
		Subsystem: "client",
		Name:      "command_duration_seconds",
	}
	metricClientNumReplyErrors = clientMetricDef{
		Subsystem: "client",
		Name:      "num_reply_errors",
	}
	metricClientNumServerUnsubscribes = clientMetricDef{
		Subsystem: "client",
		Name:      "num_server_unsubscribes",
	}
	metricClientNumServerDisconnects = clientMetricDef{
		Subsystem: "client",
		Name:      "num_server_disconnects",
	}
	metricTransportMessagesSent = clientMetricDef{
		Subsystem: "transport",
		Name:      "messages_sent",
	}
	metricTransportMessagesSentSize = clientMetricDef{
		Subsystem: "transport",
		Name:      "messages_sent_size",
	}
	metricTransportMessagesReceived = clientMetricDef{
		Subsystem: "transport",
		Name:      "messages_received",
	}
	metricTransportMessagesReceivedSize = clientMetricDef{
		Subsystem: "transport",
		Name:      "messages_received_size",
	}
)

type metrics struct {
	messagesSentCount      *prometheus.CounterVec
	messagesReceivedCount  *prometheus.CounterVec
	actionCount            *prometheus.CounterVec
	buildInfoGauge         *prometheus.GaugeVec
	numClientsGauge        prometheus.Gauge
	numUsersGauge          prometheus.Gauge
	numSubsGauge           prometheus.Gauge
	numChannelsGauge       prometheus.Gauge
	numNodesGauge          prometheus.Gauge
	replyErrorCount        *prometheus.CounterVec
	connectionsAccepted    *prometheus.CounterVec
	connectionsInflight    *prometheus.GaugeVec
	subscriptionsAccepted  *prometheus.CounterVec
	subscriptionsInflight  *prometheus.GaugeVec
	serverUnsubscribeCount *prometheus.CounterVec
	serverDisconnectCount  *prometheus.CounterVec
	// commandDurationSummary holds the legacy Summary by default; when
	// EnableNativeHistograms is true it is a no-op (the Summary is not
	// exposed). The companion commandDurationHistogram below always carries
	// the real observations, with native schema when the flag is on.
	commandDurationSummary        prometheus.ObserverVec
	commandDurationHistogram      *prometheus.HistogramVec
	surveyDurationSummary         prometheus.ObserverVec
	surveyDurationHistogram       *prometheus.HistogramVec
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

	broadcastDurationHistogram  *prometheus.HistogramVec
	pubSubLagHistogram          *prometheus.HistogramVec
	pingPongDurationHistogram   *prometheus.HistogramVec
	mapPublishSuppressedCount   *prometheus.CounterVec
	mapBrokerCleanupLag         *prometheus.GaugeVec
	mapBrokerCleanupKeysRemoved *prometheus.CounterVec
	mapBrokerCleanupErrors      *prometheus.CounterVec

	redisBrokerPubSubErrors           *prometheus.CounterVec
	redisBrokerPubSubDroppedMessages  *prometheus.CounterVec
	redisBrokerPubSubBufferedMessages *prometheus.GaugeVec

	// brokerPubSub and mapBrokerPubSub bundle the Redis PUB/SUB metric vectors per
	// broker kind so the shared pub/sub loop reports under each broker's own
	// subsystem: broker_* for RedisBroker, map_broker_* for RedisMapBroker.
	brokerPubSub    redisPubSubMetrics
	mapBrokerPubSub redisPubSubMetrics

	// Shared poll metrics.
	sharedPollCycleDurationHistogram     *prometheus.HistogramVec
	sharedPollCycleWorkDurationHistogram *prometheus.HistogramVec
	sharedPollHandlerDurationHistogram   *prometheus.HistogramVec
	sharedPollSemWaitDurationHistogram   *prometheus.HistogramVec
	sharedPollHandlerErrorCount          *prometheus.CounterVec
	sharedPollItemsCount                 *prometheus.CounterVec
	sharedPollNotifyCount                *prometheus.CounterVec
	sharedPollDroppedNotifyCount         *prometheus.CounterVec
	sharedPollPublishCount               *prometheus.CounterVec
	sharedPollNumChannelsGauge           prometheus.Gauge
	sharedPollNumKeysGauge               prometheus.Gauge

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
	mapPublishSuppressedCache      sync.Map
	pubSubLagCache                 sync.Map
	sharedPollHandlerCache         sync.Map
	sharedPollResultCache          sync.Map
	sharedPollChannelCache         sync.Map
	sharedPollPublishCache         sync.Map
	nsCache                        *otter.Cache[string, string]
	codeStrings                    map[uint32]string

	// Cache for client label combinations: maps cache key -> {labelValues, cacheKey}
	// This allows sharing pre-computed label data across all clients with the same label values
	clientLabelCombinationsCache sync.Map // map[string]*clientLabelCombination
}

// clientLabelCombination holds pre-computed label values and cache key for a unique combination
type clientLabelCombination struct {
	labelValues []string
	cacheKey    string
}

func getMetricsNamespace(config MetricsConfig) string {
	if config.MetricsNamespace == "" {
		return defaultMetricsNamespace
	}
	return config.MetricsNamespace
}

// clientLabelPrefix is prepended to every client-label name exported as a
// Prometheus dimension. It guarantees that user-chosen names in
// MetricsConfig.ClientLabels can never collide with built-in metric labels
// like "transport", "code", "method", etc.
const clientLabelPrefix = "app_"

// buildMetricLabels creates a label slice, optionally appending client labels if enabled.
// Exported client-label names are prefixed with clientLabelPrefix.
func (m *metrics) buildMetricLabels(baseLabels []string) []string {
	if len(m.config.ClientLabels) == 0 {
		return baseLabels
	}
	labels := make([]string, 0, len(baseLabels)+len(m.config.ClientLabels))
	labels = append(labels, baseLabels...)
	for _, name := range m.config.ClientLabels {
		labels = append(labels, clientLabelPrefix+name)
	}
	return labels
}

// appendClientLabels appends client label values to base labels if client labels are enabled.
// Returns a new slice with client labels appended, or the original base labels if disabled.
func (m *metrics) appendClientLabels(baseLabels []string, c *Client) []string {
	if len(m.config.ClientLabels) == 0 {
		return baseLabels
	}
	clientLabelValues := m.extractClientLabelValues(c)
	if clientLabelValues != nil {
		result := make([]string, len(baseLabels)+len(clientLabelValues))
		copy(result, baseLabels)
		copy(result[len(baseLabels):], clientLabelValues)
		return result
	}
	// Append empty strings for missing client labels to match metric definition
	result := make([]string, len(baseLabels)+len(m.config.ClientLabels))
	copy(result, baseLabels)
	return result
}

// getOrCreateClientLabelCombinationFromLabels returns a cached combination for the given labels map.
// This is used during client connect to precompute and cache the combination.
func (m *metrics) getOrCreateClientLabelCombinationFromLabels(labels map[string]string) *clientLabelCombination {
	if len(m.config.ClientLabels) == 0 {
		return nil
	}

	// Build cache key directly from the map to check if it's already cached
	cacheKey := buildClientLabelsCacheKeyFromMap(m.config.ClientLabels, labels)

	// Try to load existing combination from global cache
	if combo, ok := m.clientLabelCombinationsCache.Load(cacheKey); ok {
		return combo.(*clientLabelCombination)
	}

	// Not cached - now build the values slice (only done once per unique combination)
	labelValues := make([]string, len(m.config.ClientLabels))
	if labels != nil {
		for i, label := range m.config.ClientLabels {
			labelValues[i] = labels[label]
		}
	}

	// Create new combination
	combo := &clientLabelCombination{
		labelValues: labelValues,
		cacheKey:    cacheKey,
	}

	// Store in global cache (even if another goroutine stored it first, we'll use theirs)
	actual, _ := m.clientLabelCombinationsCache.LoadOrStore(cacheKey, combo)
	return actual.(*clientLabelCombination)
}

// getCachedClientLabelCombination returns the cached combination for the given client.
// The combination is pre-cached during client connect. This is used only in non-hot paths
// and for tests. Hot paths should call c.labelCombinationCached.Load() directly.
func (m *metrics) getCachedClientLabelCombination(c *Client) *clientLabelCombination {
	if len(m.config.ClientLabels) == 0 || c == nil {
		return nil
	}

	// Load pre-cached combination from client (set during connect)
	if cached := c.labelCombinationCached.Load(); cached != nil {
		return cached
	}

	// Fallback: shouldn't happen in normal flow, but handle gracefully
	// This can happen if metrics are recorded before client is fully connected or in tests
	return nil
}

// extractClientLabelValues extracts client label values from a client, returning empty strings for missing labels.
// This is a helper for non-hot-path uses. For hot paths, call c.labelCombinationCached.Load() directly.
func (m *metrics) extractClientLabelValues(c *Client) []string {
	if len(m.config.ClientLabels) == 0 || c == nil {
		return nil
	}

	// Try to get the cached combination first
	combo := m.getCachedClientLabelCombination(c)
	if combo != nil {
		return combo.labelValues
	}

	// Fallback: client doesn't have combination cached (e.g., in tests)
	// Build label values directly from client.labels map
	// Note: c.labels is set once during connect and never modified, so safe to read without lock
	values := make([]string, len(m.config.ClientLabels))
	if c.labels != nil {
		for i, label := range m.config.ClientLabels {
			values[i] = c.labels[label]
		}
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

// buildClientLabelsCacheKeyFromMap builds a cache key directly from a labels map.
// This avoids allocating the intermediate values slice.
func buildClientLabelsCacheKeyFromMap(labelNames []string, labelsMap map[string]string) string {
	if len(labelNames) == 0 {
		return ""
	}
	// Pre-calculate size to avoid Builder growth allocations
	size := len(labelNames) - 1 // separators
	if labelsMap != nil {
		for _, name := range labelNames {
			size += len(labelsMap[name])
		}
	}
	var b strings.Builder
	b.Grow(size)
	for i, name := range labelNames {
		if i > 0 {
			b.WriteByte(0) // Use null byte as separator
		}
		if labelsMap != nil {
			b.WriteString(labelsMap[name])
		}
	}
	return b.String()
}

// dualObserver fans observations out to both a Summary and a Histogram
// observer. Used when a metric is exposed as both instrument types — the
// Summary preserves existing {quantile="..."} dashboards while the Histogram
// supplies the histogram_quantile()- and OpenTelemetry-friendly form. When
// EnableNativeHistograms is true the Summary side is a no-op, so only the
// Histogram records data.
type dualObserver struct {
	summary, histogram prometheus.Observer
}

func (d dualObserver) Observe(v float64) {
	d.summary.Observe(v)
	d.histogram.Observe(v)
}

// noopObserverVec implements prometheus.ObserverVec with all no-op methods.
// Assigned to a Summary accessor when EnableNativeHistograms is true so the
// Summary side of a dual-instrument metric is not exposed and contributes no
// observation cost. Callers that cache observers via WithLabelValues do not
// need nil-checks — they get a noopObserver that silently drops Observe()
// calls.
type noopObserverVec struct{}

func (noopObserverVec) Describe(chan<- *prometheus.Desc)              {}
func (noopObserverVec) Collect(chan<- prometheus.Metric)              {}
func (noopObserverVec) WithLabelValues(...string) prometheus.Observer { return noopObserver{} }
func (noopObserverVec) With(prometheus.Labels) prometheus.Observer    { return noopObserver{} }
func (noopObserverVec) GetMetricWith(prometheus.Labels) (prometheus.Observer, error) {
	return noopObserver{}, nil
}
func (noopObserverVec) GetMetricWithLabelValues(...string) (prometheus.Observer, error) {
	return noopObserver{}, nil
}
func (noopObserverVec) CurryWith(prometheus.Labels) (prometheus.ObserverVec, error) {
	return noopObserverVec{}, nil
}
func (noopObserverVec) MustCurryWith(prometheus.Labels) prometheus.ObserverVec {
	return noopObserverVec{}
}

type noopObserver struct{}

func (noopObserver) Observe(float64) {}

// nativeHistogramOpts returns opts unchanged when native is false. When true,
// it enables Prometheus native histogram schema with no explicit buckets —
// the metric exposes only _count, _sum, and the native histogram chunk.
func nativeHistogramOpts(opts prometheus.HistogramOpts, native bool) prometheus.HistogramOpts {
	if !native {
		return opts
	}
	opts.Buckets = nil
	opts.NativeHistogramBucketFactor = 1.1
	opts.NativeHistogramMaxBucketNumber = 200
	opts.NativeHistogramMinResetDuration = time.Hour
	return opts
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
			nsCache = otter.Must(&otter.Options[string, string]{
				MaximumSize:      cacheSize,
				ExpiryCalculator: otter.ExpiryWriting[string, string](cacheTTL),
			})
		}
	}

	codeStrings := make(map[uint32]string)
	for i := uint32(0); i <= 5000; i++ {
		codeStrings[i] = strconv.FormatUint(uint64(i), 10)
	}

	m := &metrics{
		config:      config,
		nsCache:     nsCache,
		codeStrings: codeStrings,
	}

	m.actionCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: "node",
		Name:      "action_count",
		Help:      "Number of various actions called.",
	}, []string{"action", "channel_namespace"})

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
	}, []string{"version"})

	m.numChannelsGauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Subsystem: "node",
		Name:      "num_channels",
		Help:      "Number of channels with one or more subscribers.",
	})

	if config.EnableNativeHistograms {
		m.surveyDurationSummary = noopObserverVec{}
	} else {
		m.surveyDurationSummary = prometheus.NewSummaryVec(prometheus.SummaryOpts{
			Namespace:  metricsNamespace,
			Subsystem:  "node",
			Name:       "survey_duration_seconds",
			Objectives: map[float64]float64{0.5: 0.05, 0.99: 0.001, 0.999: 0.0001},
			Help:       "DEPRECATED — use survey_duration_seconds_histogram. Will be removed in future releases. Survey duration summary.",
		}, []string{"op"})
	}
	m.surveyDurationHistogram = prometheus.NewHistogramVec(nativeHistogramOpts(prometheus.HistogramOpts{
		Namespace: metricsNamespace,
		Subsystem: "node",
		Name:      "survey_duration_seconds_histogram",
		Help:      "Survey duration histogram. Use for histogram_quantile() and OpenTelemetry export.",
		Buckets: []float64{
			0.001, 0.005, 0.010, 0.025, 0.050, 0.100, 0.250, 0.500,
			1.0, 2.5, 5.0, 10.0,
		},
	}, config.EnableNativeHistograms), []string{"op"})

	m.messagesSentCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: "node",
		Name:      "messages_sent_count",
		Help:      "Number of messages sent by node to broker.",
	}, []string{"type", "channel_namespace"})

	m.messagesReceivedCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: "node",
		Name:      "messages_received_count",
		Help:      "Number of messages received from broker.",
	}, []string{"type", "channel_namespace"})

	if config.EnableNativeHistograms {
		m.commandDurationSummary = noopObserverVec{}
	} else {
		m.commandDurationSummary = prometheus.NewSummaryVec(prometheus.SummaryOpts{
			Namespace:  metricsNamespace,
			Subsystem:  metricClientCommandDuration.Subsystem,
			Name:       metricClientCommandDuration.Name,
			Objectives: map[float64]float64{0.5: 0.05, 0.99: 0.001, 0.999: 0.0001},
			Help:       "DEPRECATED — use command_duration_seconds_histogram. Will be removed in future releases. Client command duration summary.",
		}, m.buildMetricLabels([]string{"method", "channel_namespace"}))
	}
	m.commandDurationHistogram = prometheus.NewHistogramVec(nativeHistogramOpts(prometheus.HistogramOpts{
		Namespace: metricsNamespace,
		Subsystem: metricClientCommandDuration.Subsystem,
		Name:      metricClientCommandDuration.Name + "_histogram",
		Help:      "Client command duration histogram. Use for histogram_quantile() and OpenTelemetry export.",
		Buckets: []float64{
			0.000100, 0.000250, 0.000500,
			0.001, 0.005, 0.010, 0.025, 0.050, 0.100, 0.250, 0.500,
			1.0, 2.5, 5.0, 10.0,
		},
	}, config.EnableNativeHistograms), m.buildMetricLabels([]string{"method", "channel_namespace"}))

	m.replyErrorCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: metricClientNumReplyErrors.Subsystem,
		Name:      metricClientNumReplyErrors.Name,
		Help:      "Number of errors in replies sent to clients.",
	}, m.buildMetricLabels([]string{"method", "code", "channel_namespace"}))

	m.serverUnsubscribeCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: metricClientNumServerUnsubscribes.Subsystem,
		Name:      metricClientNumServerUnsubscribes.Name,
		Help:      "Number of server initiated unsubscribes.",
	}, m.buildMetricLabels([]string{"code", "channel_namespace"}))

	m.serverDisconnectCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: metricClientNumServerDisconnects.Subsystem,
		Name:      metricClientNumServerDisconnects.Name,
		Help:      "Number of server initiated disconnects.",
	}, m.buildMetricLabels([]string{"code"}))

	m.recoverCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: "client",
		Name:      "recover",
		Help:      "Count of recover operations with success/fail resolution.",
	}, m.buildMetricLabels([]string{"recovered", "channel_namespace", "has_recovered_publications"}))

	if config.EnableRecoveredPublicationsHistogram {
		m.recoveredPublications = prometheus.NewHistogramVec(
			nativeHistogramOpts(prometheus.HistogramOpts{
				Namespace: metricsNamespace,
				Subsystem: "client",
				Name:      "recovered_publications",
				Help:      "Number of publications recovered during subscription recovery.",
				Buckets:   []float64{0, 1, 2, 3, 5, 10, 20, 50, 100, 250, 500, 1000, 2000, 5000, 10000},
			}, config.EnableNativeHistograms),
			m.buildMetricLabels([]string{"channel_namespace"}),
		)
	}

	m.pingPongDurationHistogram = prometheus.NewHistogramVec(nativeHistogramOpts(prometheus.HistogramOpts{
		Namespace: metricsNamespace,
		Subsystem: "client",
		Name:      "ping_pong_duration_seconds",
		Help:      "Ping/Pong duration in seconds",
		Buckets: []float64{
			0.000100, 0.000250, 0.000500, // Microsecond resolution.
			0.001, 0.005, 0.010, 0.025, 0.050, 0.100, 0.250, 0.500, // Millisecond resolution.
			1.0, 2.5, 5.0, 10.0, // Second resolution.
		}}, config.EnableNativeHistograms), m.buildMetricLabels([]string{"transport"}))

	m.connectionsAccepted = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: metricClientConnectionsAccepted.Subsystem,
		Name:      metricClientConnectionsAccepted.Name,
		Help:      "Count of accepted transports.",
	}, m.buildMetricLabels([]string{"transport", "accept_protocol", "client_name", "client_version"}))

	m.connectionsInflight = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Subsystem: metricClientConnectionsInflight.Subsystem,
		Name:      metricClientConnectionsInflight.Name,
		Help:      "Number of inflight client connections.",
	}, m.buildMetricLabels([]string{"transport", "accept_protocol", "client_name", "client_version"}))

	m.subscriptionsAccepted = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: metricClientSubscriptionsAccepted.Subsystem,
		Name:      metricClientSubscriptionsAccepted.Name,
		Help:      "Count of accepted client subscriptions.",
	}, m.buildMetricLabels([]string{"client_name", "channel_namespace"}))

	m.subscriptionsInflight = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Subsystem: metricClientSubscriptionsInflight.Subsystem,
		Name:      metricClientSubscriptionsInflight.Name,
		Help:      "Number of inflight client subscriptions.",
	}, m.buildMetricLabels([]string{"client_name", "channel_namespace"}))

	m.transportMessagesSent = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: metricTransportMessagesSent.Subsystem,
		Name:      metricTransportMessagesSent.Name,
		Help:      "Number of messages sent to client connections over specific transport.",
	}, m.buildMetricLabels([]string{"transport", "frame_type", "channel_namespace"}))

	m.transportMessagesSentSize = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: metricTransportMessagesSentSize.Subsystem,
		Name:      metricTransportMessagesSentSize.Name,
		Help:      "MaxSize in bytes of messages sent to client connections over specific transport (uncompressed and does not include framing overhead).",
	}, m.buildMetricLabels([]string{"transport", "frame_type", "channel_namespace"}))

	m.transportMessagesReceived = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: metricTransportMessagesReceived.Subsystem,
		Name:      metricTransportMessagesReceived.Name,
		Help:      "Number of messages received from client connections over specific transport.",
	}, m.buildMetricLabels([]string{"transport", "frame_type", "channel_namespace"}))

	m.transportMessagesReceivedSize = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: metricTransportMessagesReceivedSize.Subsystem,
		Name:      metricTransportMessagesReceivedSize.Name,
		Help:      "MaxSize in bytes of messages received from client connections over specific transport (uncompressed and does not include framing overhead).",
	}, m.buildMetricLabels([]string{"transport", "frame_type", "channel_namespace"}))

	m.tagsFilterDroppedCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: "node",
		Name:      "tags_filter_dropped_publications",
		Help:      "Number of publications dropped due to tags filtering.",
	}, []string{"channel_namespace"})

	m.pubSubLagHistogram = prometheus.NewHistogramVec(nativeHistogramOpts(prometheus.HistogramOpts{
		Namespace: metricsNamespace,
		Subsystem: "node",
		Name:      "pub_sub_lag_seconds",
		Help:      "Pub sub lag in seconds",
		Buckets:   []float64{0.001, 0.005, 0.010, 0.025, 0.050, 0.100, 0.200, 0.500, 1.000, 2.000, 5.000, 10.000},
	}, config.EnableNativeHistograms), []string{"channel_namespace"})

	m.broadcastDurationHistogram = prometheus.NewHistogramVec(nativeHistogramOpts(prometheus.HistogramOpts{
		Namespace: metricsNamespace,
		Subsystem: "node",
		Name:      "broadcast_duration_seconds",
		Help:      "Broadcast duration in seconds",
		Buckets: []float64{
			0.000001, 0.000005, 0.000010, 0.000050, 0.000100, 0.000250, 0.000500, // Microsecond resolution.
			0.001, 0.005, 0.010, 0.025, 0.050, 0.100, 0.250, 0.500, // Millisecond resolution.
			1.0, 2.5, 5.0, 10.0, // Second resolution.
		}}, config.EnableNativeHistograms), []string{"type", "channel_namespace"})

	m.mapPublishSuppressedCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: "map_broker",
		Name:      "publish_suppressed_count",
		Help:      "Number of suppressed map publish/remove operations.",
	}, []string{"reason", "channel_namespace"})

	m.mapBrokerCleanupLag = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Subsystem: "map_broker",
		Name:      "cleanup_lag_seconds",
		Help:      "Lag between now and the oldest expired entry awaiting cleanup. 0 means caught up.",
	}, []string{"broker_name"})

	m.mapBrokerCleanupKeysRemoved = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: "map_broker",
		Name:      "cleanup_keys_removed_count",
		Help:      "Total number of keys removed by cleanup.",
	}, []string{"broker_name"})

	m.mapBrokerCleanupErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: "map_broker",
		Name:      "cleanup_errors_count",
		Help:      "Total number of cleanup errors.",
	}, []string{"broker_name"})

	sharedPollDurationBuckets := []float64{
		0.010, 0.025, 0.050, 0.100, 0.250, 0.500, // Millisecond resolution.
		1.0, 2.5, 5.0, 10.0, 30.0, 60.0, // Second resolution.
	}
	sharedPollHandlerBuckets := []float64{
		0.001, 0.005, 0.010, 0.025, 0.050, 0.100, 0.250, 0.500, // Millisecond resolution.
		1.0, 2.5, 5.0, 10.0, 30.0, // Second resolution.
	}
	sharedPollSemWaitBuckets := []float64{
		0.0001, 0.0005, 0.001, 0.005, 0.010, 0.025, 0.050, 0.100, 0.250, 0.500, // Millisecond resolution.
		1.0, 2.5, 5.0, 10.0, 30.0, // Second resolution.
	}

	m.sharedPollCycleDurationHistogram = prometheus.NewHistogramVec(nativeHistogramOpts(prometheus.HistogramOpts{
		Namespace: metricsNamespace,
		Subsystem: "shared_poll",
		Name:      "cycle_duration_seconds",
		Help:      "Full timer cycle duration in seconds.",
		Buckets:   sharedPollDurationBuckets,
	}, config.EnableNativeHistograms), []string{"channel_namespace"})

	m.sharedPollCycleWorkDurationHistogram = prometheus.NewHistogramVec(nativeHistogramOpts(prometheus.HistogramOpts{
		Namespace: metricsNamespace,
		Subsystem: "shared_poll",
		Name:      "cycle_work_duration_seconds",
		Help:      "Cycle work time in seconds (minus spread delay).",
		Buckets:   sharedPollDurationBuckets,
	}, config.EnableNativeHistograms), []string{"channel_namespace"})

	m.sharedPollHandlerDurationHistogram = prometheus.NewHistogramVec(nativeHistogramOpts(prometheus.HistogramOpts{
		Namespace: metricsNamespace,
		Subsystem: "shared_poll",
		Name:      "handler_duration_seconds",
		Help:      "Handler call latency in seconds.",
		Buckets:   sharedPollHandlerBuckets,
	}, config.EnableNativeHistograms), []string{"trigger", "channel_namespace"})

	m.sharedPollSemWaitDurationHistogram = prometheus.NewHistogramVec(nativeHistogramOpts(prometheus.HistogramOpts{
		Namespace: metricsNamespace,
		Subsystem: "shared_poll",
		Name:      "sem_wait_duration_seconds",
		Help:      "Semaphore wait duration in seconds.",
		Buckets:   sharedPollSemWaitBuckets,
	}, config.EnableNativeHistograms), []string{"trigger", "channel_namespace"})

	m.sharedPollHandlerErrorCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: "shared_poll",
		Name:      "handler_error_count",
		Help:      "Number of handler errors.",
	}, []string{"trigger", "channel_namespace"})

	m.sharedPollItemsCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: "shared_poll",
		Name:      "items_count",
		Help:      "Number of items by result.",
	}, []string{"trigger", "result", "channel_namespace"})

	m.sharedPollNotifyCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: "shared_poll",
		Name:      "notify_count",
		Help:      "Number of notifications received.",
	}, []string{"channel_namespace"})

	m.sharedPollDroppedNotifyCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: "shared_poll",
		Name:      "dropped_notify_count",
		Help:      "Number of notifications dropped due to full buffer.",
	}, []string{"channel_namespace"})

	m.sharedPollPublishCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: "shared_poll",
		Name:      "publish_count",
		Help:      "Number of direct publish operations by result.",
	}, []string{"result", "channel_namespace"})

	m.sharedPollNumChannelsGauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Subsystem: "shared_poll",
		Name:      "num_channels",
		Help:      "Number of active shared poll channels.",
	})

	m.sharedPollNumKeysGauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Subsystem: "shared_poll",
		Name:      "num_keys",
		Help:      "Total number of tracked keys.",
	})

	m.redisBrokerPubSubErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: "broker",
		Name:      "redis_pub_sub_errors",
		Help:      "Number of times there was an error in Redis PUB/SUB connection.",
	}, []string{"broker_name", "error"})

	m.redisBrokerPubSubDroppedMessages = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: "broker",
		Name:      "redis_pub_sub_dropped_messages",
		Help:      "Number of dropped messages on application level in Redis PUB/SUB.",
	}, []string{"broker_name", "channel_type"})

	m.redisBrokerPubSubBufferedMessages = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Subsystem: "broker",
		Name:      "redis_pub_sub_buffered_messages",
		Help:      "Number of messages buffered in Redis PUB/SUB.",
	}, []string{"broker_name", "channel_type", "pub_sub_processor"})

	m.redisBrokerPubSubDroppedMessages.WithLabelValues("", "control").Add(0)
	m.redisBrokerPubSubDroppedMessages.WithLabelValues("", "client").Add(0)

	m.brokerPubSub = redisPubSubMetrics{
		errors:   m.redisBrokerPubSubErrors,
		dropped:  m.redisBrokerPubSubDroppedMessages,
		buffered: m.redisBrokerPubSubBufferedMessages,
	}

	// RedisMapBroker reports the same Redis PUB/SUB metrics under its own
	// map_broker subsystem so its series never collide with the stream broker's.
	m.mapBrokerPubSub = redisPubSubMetrics{
		errors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Subsystem: "map_broker",
			Name:      "redis_pub_sub_errors",
			Help:      "Number of times there was an error in Redis PUB/SUB connection.",
		}, []string{"broker_name", "error"}),
		dropped: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Subsystem: "map_broker",
			Name:      "redis_pub_sub_dropped_messages",
			Help:      "Number of dropped messages on application level in Redis PUB/SUB.",
		}, []string{"broker_name", "channel_type"}),
		buffered: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Subsystem: "map_broker",
			Name:      "redis_pub_sub_buffered_messages",
			Help:      "Number of messages buffered in Redis PUB/SUB.",
		}, []string{"broker_name", "channel_type", "pub_sub_processor"}),
	}
	m.mapBrokerPubSub.dropped.WithLabelValues("", "client").Add(0)

	// Helper to build message labels for node-level broker message metrics
	// These metrics don't support client labels as they track node-to-broker communication
	buildMessageLabels := func(msgType string) []string {
		return []string{msgType, ""}
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

	// Helper to build initial label values with empty client labels if configured.
	// Client labels are added as empty strings since these pre-cached observers
	// are only used when no per-call ClientLabels lookup is needed.
	buildCommandLabels := func(method string) []string {
		labels := []string{method, ""}
		for range m.config.ClientLabels {
			labels = append(labels, "")
		}
		return labels
	}

	makeCommandObserver := func(method string) prometheus.Observer {
		labels := buildCommandLabels(method)
		return dualObserver{
			summary:   m.commandDurationSummary.WithLabelValues(labels...),
			histogram: m.commandDurationHistogram.WithLabelValues(labels...),
		}
	}
	m.commandDurationConnect = makeCommandObserver(labelForMethod(protocol.FrameTypeConnect))
	m.commandDurationSubscribe = makeCommandObserver(labelForMethod(protocol.FrameTypeSubscribe))
	m.commandDurationUnsubscribe = makeCommandObserver(labelForMethod(protocol.FrameTypeUnsubscribe))
	m.commandDurationPublish = makeCommandObserver(labelForMethod(protocol.FrameTypePublish))
	m.commandDurationPresence = makeCommandObserver(labelForMethod(protocol.FrameTypePresence))
	m.commandDurationPresenceStats = makeCommandObserver(labelForMethod(protocol.FrameTypePresenceStats))
	m.commandDurationHistory = makeCommandObserver(labelForMethod(protocol.FrameTypeHistory))
	m.commandDurationSend = makeCommandObserver(labelForMethod(protocol.FrameTypeSend))
	m.commandDurationRPC = makeCommandObserver(labelForMethod(protocol.FrameTypeRPC))
	m.commandDurationRefresh = makeCommandObserver(labelForMethod(protocol.FrameTypeRefresh))
	m.commandDurationSubRefresh = makeCommandObserver(labelForMethod(protocol.FrameTypeSubRefresh))
	m.commandDurationUnknown = makeCommandObserver("unknown")

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
		m.commandDurationHistogram,
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
		m.surveyDurationHistogram,
		m.pubSubLagHistogram,
		m.broadcastDurationHistogram,
		m.mapPublishSuppressedCount,
		m.mapBrokerCleanupLag,
		m.mapBrokerCleanupKeysRemoved,
		m.mapBrokerCleanupErrors,
		m.redisBrokerPubSubErrors,
		m.redisBrokerPubSubDroppedMessages,
		m.redisBrokerPubSubBufferedMessages,
		m.mapBrokerPubSub.errors,
		m.mapBrokerPubSub.dropped,
		m.mapBrokerPubSub.buffered,
		m.sharedPollCycleDurationHistogram,
		m.sharedPollCycleWorkDurationHistogram,
		m.sharedPollHandlerDurationHistogram,
		m.sharedPollSemWaitDurationHistogram,
		m.sharedPollHandlerErrorCount,
		m.sharedPollItemsCount,
		m.sharedPollNotifyCount,
		m.sharedPollDroppedNotifyCount,
		m.sharedPollPublishCount,
		m.sharedPollNumChannelsGauge,
		m.sharedPollNumKeysGauge,
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

// redisPubSubMetrics bundles the Redis PUB/SUB metric vectors for one broker kind
// so the shared pub/sub loop (runPubSubLoop) reports under the calling broker's
// own subsystem — broker_* for RedisBroker, map_broker_* for RedisMapBroker.
type redisPubSubMetrics struct {
	errors   *prometheus.CounterVec
	dropped  *prometheus.CounterVec
	buffered *prometheus.GaugeVec
}

func (p redisPubSubMetrics) incErrors(name, errType string) {
	p.errors.WithLabelValues(name, errType).Inc()
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
	if nsLabel, cached = m.nsCache.GetIfPresent(ch); cached {
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

	if (ch != "" && m.config.GetChannelNamespaceLabel != nil) || len(m.config.ClientLabels) > 0 {
		labels := commandDurationLabels{
			ChannelNamespace: channelNamespace,
			FrameType:        frameType,
		}
		observer, ok := m.commandDurationCache.Load(labels)
		if !ok {
			baseLabels := []string{frameType.String(), channelNamespace}
			labelValues := m.appendClientLabels(baseLabels, c)
			observer = dualObserver{
				summary:   m.commandDurationSummary.WithLabelValues(labelValues...),
				histogram: m.commandDurationHistogram.WithLabelValues(labelValues...),
			}
			m.commandDurationCache.Store(labels, observer)
		}
		observer.(prometheus.Observer).Observe(d.Seconds())
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

func (m *metrics) observePubSubDeliveryLag(lagTimeMilli int64, ch string) {
	if lagTimeMilli < 0 {
		lagTimeMilli = -lagTimeMilli
	}
	channelNamespace := m.getChannelNamespaceLabel(ch)
	observer, ok := m.pubSubLagCache.Load(channelNamespace)
	if !ok {
		observer = m.pubSubLagHistogram.WithLabelValues(channelNamespace)
		m.pubSubLagCache.Store(channelNamespace, observer)
	}
	observer.(prometheus.Observer).Observe(float64(lagTimeMilli) / 1000)
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
		baseLabels := []string{frameType.String(), labels.Code, channelNamespace}
		labelValues := m.appendClientLabels(baseLabels, c)
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
	// Apply client labels if they are configured
	useClientLabels := len(m.config.ClientLabels) > 0
	if useClientLabels {
		if clientLabelValues == nil {
			// Create empty strings for missing client labels
			clientLabelValues = make([]string, len(m.config.ClientLabels))
			clientLabelCacheKey = buildClientLabelsCacheKey(clientLabelValues)
		}
	} else {
		// Client labels not configured - don't use them even if provided
		clientLabelValues = nil
		clientLabelCacheKey = ""
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
		if useClientLabels {
			labelValues = append(labelValues, clientLabelValues...)
		}

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

	// Use pre-cached combination from client (set during connect)
	var clientLabelValues []string
	var clientLabelCacheKey string
	if c != nil {
		if combo := c.labelCombinationCached.Load(); combo != nil {
			clientLabelValues = combo.labelValues
			clientLabelCacheKey = combo.cacheKey
		}
	}

	counters := m.getTransportMessagesSentCounters(transport, frameType.String(), channelNamespace, clientLabelValues, clientLabelCacheKey)
	counters.counterSent.Inc()
	counters.counterSentSize.Add(float64(size))
}

func (m *metrics) incTransportMessagesReceived(transport string, frameType protocol.FrameType, channel string, size int, c *Client) {
	channelNamespace := m.getChannelNamespaceLabel(channel)
	clientLabelValues := m.extractClientLabelValues(c)

	// Apply client labels if they are configured
	useClientLabels := len(m.config.ClientLabels) > 0
	if useClientLabels {
		if clientLabelValues == nil {
			clientLabelValues = make([]string, len(m.config.ClientLabels))
		}
	} else {
		// Client labels not configured - don't use them even if provided
		clientLabelValues = nil
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
		if useClientLabels && len(clientLabelValues) > 0 {
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
		baseLabels := []string{labels.Code}
		labelValues := m.appendClientLabels(baseLabels, c)
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
		baseLabels := []string{labels.Code, labels.ChannelNamespace}
		labelValues := m.appendClientLabels(baseLabels, c)
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
	seconds := d.Seconds()
	m.surveyDurationSummary.WithLabelValues(op).Observe(seconds)
	m.surveyDurationHistogram.WithLabelValues(op).Observe(seconds)
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

type mapPublishSuppressedLabels struct {
	Reason           string
	ChannelNamespace string
}

func (m *metrics) incMapPublishSuppressed(reason SuppressReason, ch string) {
	channelNamespace := m.getChannelNamespaceLabel(ch)
	labels := mapPublishSuppressedLabels{
		Reason:           string(reason),
		ChannelNamespace: channelNamespace,
	}
	counter, ok := m.mapPublishSuppressedCache.Load(labels)
	if !ok {
		counter = m.mapPublishSuppressedCount.WithLabelValues(string(reason), channelNamespace)
		m.mapPublishSuppressedCache.Store(labels, counter)
	}
	counter.(prometheus.Counter).Inc()
}

func (m *metrics) setMapBrokerCleanupLag(name string, seconds float64) {
	m.mapBrokerCleanupLag.WithLabelValues(name).Set(seconds)
}

func (m *metrics) addMapBrokerCleanupKeysRemoved(name string, count int64) {
	m.mapBrokerCleanupKeysRemoved.WithLabelValues(name).Add(float64(count))
}

func (m *metrics) incMapBrokerCleanupErrors(name string) {
	m.mapBrokerCleanupErrors.WithLabelValues(name).Inc()
}

// Shared poll cached metric bundles and helpers.

type sharedPollTriggerNsLabels struct {
	Trigger          string
	ChannelNamespace string
}

type sharedPollHandlerCached struct {
	errorCount  prometheus.Counter
	itemsPolled prometheus.Counter
	duration    prometheus.Observer
	semWait     prometheus.Observer
}

type sharedPollResultCached struct {
	changed   prometheus.Counter
	unchanged prometheus.Counter
	removed   prometheus.Counter
}

type sharedPollChannelCached struct {
	cycleDuration      prometheus.Observer
	cycleWorkDuration  prometheus.Observer
	notifyCount        prometheus.Counter
	droppedNotifyCount prometheus.Counter
}

type sharedPollPublishCached struct {
	applied prometheus.Counter
	skipped prometheus.Counter
}

func (m *metrics) getSharedPollHandlerCached(trigger, ch string) sharedPollHandlerCached {
	channelNamespace := m.getChannelNamespaceLabel(ch)
	labels := sharedPollTriggerNsLabels{Trigger: trigger, ChannelNamespace: channelNamespace}
	cached, ok := m.sharedPollHandlerCache.Load(labels)
	if !ok {
		cached = sharedPollHandlerCached{
			errorCount:  m.sharedPollHandlerErrorCount.WithLabelValues(trigger, channelNamespace),
			itemsPolled: m.sharedPollItemsCount.WithLabelValues(trigger, "polled", channelNamespace),
			duration:    m.sharedPollHandlerDurationHistogram.WithLabelValues(trigger, channelNamespace),
			semWait:     m.sharedPollSemWaitDurationHistogram.WithLabelValues(trigger, channelNamespace),
		}
		m.sharedPollHandlerCache.Store(labels, cached)
	}
	return cached.(sharedPollHandlerCached)
}

func (m *metrics) getSharedPollResultCached(trigger, ch string) sharedPollResultCached {
	channelNamespace := m.getChannelNamespaceLabel(ch)
	labels := sharedPollTriggerNsLabels{Trigger: trigger, ChannelNamespace: channelNamespace}
	cached, ok := m.sharedPollResultCache.Load(labels)
	if !ok {
		cached = sharedPollResultCached{
			changed:   m.sharedPollItemsCount.WithLabelValues(trigger, "changed", channelNamespace),
			unchanged: m.sharedPollItemsCount.WithLabelValues(trigger, "unchanged", channelNamespace),
			removed:   m.sharedPollItemsCount.WithLabelValues(trigger, "removed", channelNamespace),
		}
		m.sharedPollResultCache.Store(labels, cached)
	}
	return cached.(sharedPollResultCached)
}

func (m *metrics) getSharedPollChannelCached(ch string) sharedPollChannelCached {
	channelNamespace := m.getChannelNamespaceLabel(ch)
	cached, ok := m.sharedPollChannelCache.Load(channelNamespace)
	if !ok {
		cached = sharedPollChannelCached{
			cycleDuration:      m.sharedPollCycleDurationHistogram.WithLabelValues(channelNamespace),
			cycleWorkDuration:  m.sharedPollCycleWorkDurationHistogram.WithLabelValues(channelNamespace),
			notifyCount:        m.sharedPollNotifyCount.WithLabelValues(channelNamespace),
			droppedNotifyCount: m.sharedPollDroppedNotifyCount.WithLabelValues(channelNamespace),
		}
		m.sharedPollChannelCache.Store(channelNamespace, cached)
	}
	return cached.(sharedPollChannelCached)
}

func (m *metrics) getSharedPollPublishCached(ch string) sharedPollPublishCached {
	channelNamespace := m.getChannelNamespaceLabel(ch)
	cached, ok := m.sharedPollPublishCache.Load(channelNamespace)
	if !ok {
		cached = sharedPollPublishCached{
			applied: m.sharedPollPublishCount.WithLabelValues("applied", channelNamespace),
			skipped: m.sharedPollPublishCount.WithLabelValues("skipped", channelNamespace),
		}
		m.sharedPollPublishCache.Store(channelNamespace, cached)
	}
	return cached.(sharedPollPublishCached)
}

func (m *metrics) setSharedPollNumChannels(n float64) {
	m.sharedPollNumChannelsGauge.Set(n)
}

func (m *metrics) setSharedPollNumKeys(n float64) {
	m.sharedPollNumKeysGauge.Set(n)
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
