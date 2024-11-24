package centrifuge

import (
	"errors"
	"strconv"
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
	serverUnsubscribeCount        *prometheus.CounterVec
	serverDisconnectCount         *prometheus.CounterVec
	commandDurationSummary        *prometheus.SummaryVec
	surveyDurationSummary         *prometheus.SummaryVec
	recoverCount                  *prometheus.CounterVec
	transportConnectCount         *prometheus.CounterVec
	transportMessagesSent         *prometheus.CounterVec
	transportMessagesSentSize     *prometheus.CounterVec
	transportMessagesReceived     *prometheus.CounterVec
	transportMessagesReceivedSize *prometheus.CounterVec

	messagesReceivedCountPublication prometheus.Counter
	messagesReceivedCountJoin        prometheus.Counter
	messagesReceivedCountLeave       prometheus.Counter
	messagesReceivedCountControl     prometheus.Counter

	messagesSentCountPublication prometheus.Counter
	messagesSentCountJoin        prometheus.Counter
	messagesSentCountLeave       prometheus.Counter
	messagesSentCountControl     prometheus.Counter

	actionCountSurvey              prometheus.Counter
	actionCountNotify              prometheus.Counter
	actionCountAddClient           prometheus.Counter
	actionCountRemoveClient        prometheus.Counter
	actionCountAddSub              prometheus.Counter
	actionCountBrokerSubscribe     prometheus.Counter
	actionCountRemoveSub           prometheus.Counter
	actionCountBrokerUnsubscribe   prometheus.Counter
	actionCountAddPresence         prometheus.Counter
	actionCountRemovePresence      prometheus.Counter
	actionCountPresence            prometheus.Counter
	actionCountPresenceStats       prometheus.Counter
	actionCountHistory             prometheus.Counter
	actionCountHistoryRecover      prometheus.Counter
	actionCountHistoryRecoverCache prometheus.Counter
	actionCountHistoryStreamTop    prometheus.Counter
	actionCountHistoryRemove       prometheus.Counter

	recoverCountYes prometheus.Counter
	recoverCountNo  prometheus.Counter

	transportConnectCountWebsocket  prometheus.Counter
	transportConnectCountSSE        prometheus.Counter
	transportConnectCountHTTPStream prometheus.Counter

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

	pubSubLagHistogram        prometheus.Histogram
	pingPongDurationHistogram *prometheus.HistogramVec

	config MetricsConfig

	nsCache     *otter.Cache[string, string]
	codeStrings map[uint32]string
}

func newMetricsRegistry(config MetricsConfig) (*metrics, error) {
	registryMu.Lock()
	defer registryMu.Unlock()

	metricsNamespace := config.MetricsNamespace
	if metricsNamespace == "" {
		metricsNamespace = defaultMetricsNamespace
	}

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

	m := &metrics{
		config:      config,
		nsCache:     nsCache,
		codeStrings: codeStrings,
	}

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

	m.surveyDurationSummary = prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Namespace:  metricsNamespace,
		Subsystem:  "node",
		Name:       "survey_duration_seconds",
		Objectives: map[float64]float64{0.5: 0.05, 0.99: 0.001, 0.999: 0.0001},
		Help:       "Survey duration summary.",
	}, []string{"op"})

	m.commandDurationSummary = prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Namespace:  metricsNamespace,
		Subsystem:  "client",
		Name:       "command_duration_seconds",
		Objectives: map[float64]float64{0.5: 0.05, 0.99: 0.001, 0.999: 0.0001},
		Help:       "Client command duration summary.",
	}, []string{"method", "channel_namespace"})

	m.replyErrorCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: "client",
		Name:      "num_reply_errors",
		Help:      "Number of errors in replies sent to clients.",
	}, []string{"method", "code", "channel_namespace"})

	m.serverUnsubscribeCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: "client",
		Name:      "num_server_unsubscribes",
		Help:      "Number of server initiated unsubscribes.",
	}, []string{"code", "channel_namespace"})

	m.serverDisconnectCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: "client",
		Name:      "num_server_disconnects",
		Help:      "Number of server initiated disconnects.",
	}, []string{"code"})

	m.recoverCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: "client",
		Name:      "recover",
		Help:      "Count of recover operations.",
	}, []string{"recovered", "channel_namespace"})

	m.pingPongDurationHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: metricsNamespace,
		Subsystem: "client",
		Name:      "ping_pong_duration_seconds",
		Help:      "Ping/Pong duration in seconds",
		Buckets: []float64{
			0.000100, 0.000250, 0.000500, // Microsecond resolution.
			0.001, 0.005, 0.010, 0.025, 0.050, 0.100, 0.250, 0.500, // Millisecond resolution.
			1.0, 2.5, 5.0, 10.0, // Second resolution.
		}}, []string{"transport"})

	m.transportConnectCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: "transport",
		Name:      "connect_count",
		Help:      "Number of connections to specific transport.",
	}, []string{"transport"})

	m.transportMessagesSent = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: "transport",
		Name:      "messages_sent",
		Help:      "Number of messages sent to client connections over specific transport.",
	}, []string{"transport", "frame_type", "channel_namespace"})

	m.transportMessagesSentSize = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: "transport",
		Name:      "messages_sent_size",
		Help:      "Size in bytes of messages sent to client connections over specific transport.",
	}, []string{"transport", "frame_type", "channel_namespace"})

	m.transportMessagesReceived = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: "transport",
		Name:      "messages_received",
		Help:      "Number of messages received from client connections over specific transport.",
	}, []string{"transport", "frame_type", "channel_namespace"})

	m.transportMessagesReceivedSize = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: "transport",
		Name:      "messages_received_size",
		Help:      "Size in bytes of messages received from client connections over specific transport.",
	}, []string{"transport", "frame_type", "channel_namespace"})

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
		}}, []string{"type", "channel_namespace"})

	m.messagesReceivedCountPublication = m.messagesReceivedCount.WithLabelValues("publication", "")
	m.messagesReceivedCountJoin = m.messagesReceivedCount.WithLabelValues("join", "")
	m.messagesReceivedCountLeave = m.messagesReceivedCount.WithLabelValues("leave", "")
	m.messagesReceivedCountControl = m.messagesReceivedCount.WithLabelValues("control", "")

	m.messagesSentCountPublication = m.messagesSentCount.WithLabelValues("publication", "")
	m.messagesSentCountJoin = m.messagesSentCount.WithLabelValues("join", "")
	m.messagesSentCountLeave = m.messagesSentCount.WithLabelValues("leave", "")
	m.messagesSentCountControl = m.messagesSentCount.WithLabelValues("control", "")

	m.actionCountAddClient = m.actionCount.WithLabelValues("add_client", "")
	m.actionCountRemoveClient = m.actionCount.WithLabelValues("remove_client", "")
	m.actionCountAddSub = m.actionCount.WithLabelValues("add_subscription", "")
	m.actionCountBrokerSubscribe = m.actionCount.WithLabelValues("broker_subscribe", "")
	m.actionCountRemoveSub = m.actionCount.WithLabelValues("remove_subscription", "")
	m.actionCountBrokerUnsubscribe = m.actionCount.WithLabelValues("broker_unsubscribe", "")
	m.actionCountAddPresence = m.actionCount.WithLabelValues("add_presence", "")
	m.actionCountRemovePresence = m.actionCount.WithLabelValues("remove_presence", "")
	m.actionCountPresence = m.actionCount.WithLabelValues("presence", "")
	m.actionCountPresenceStats = m.actionCount.WithLabelValues("presence_stats", "")
	m.actionCountHistory = m.actionCount.WithLabelValues("history", "")
	m.actionCountHistoryRecover = m.actionCount.WithLabelValues("history_recover", "")
	m.actionCountHistoryRecoverCache = m.actionCount.WithLabelValues("history_recover_cache", "")
	m.actionCountHistoryStreamTop = m.actionCount.WithLabelValues("history_stream_top", "")
	m.actionCountHistoryRemove = m.actionCount.WithLabelValues("history_remove", "")
	m.actionCountSurvey = m.actionCount.WithLabelValues("survey", "")
	m.actionCountNotify = m.actionCount.WithLabelValues("notify", "")

	m.recoverCountYes = m.recoverCount.WithLabelValues("yes", "")
	m.recoverCountNo = m.recoverCount.WithLabelValues("no", "")

	m.transportConnectCountWebsocket = m.transportConnectCount.WithLabelValues(transportWebsocket)
	m.transportConnectCountHTTPStream = m.transportConnectCount.WithLabelValues(transportHTTPStream)
	m.transportConnectCountSSE = m.transportConnectCount.WithLabelValues(transportSSE)

	labelForMethod := func(frameType protocol.FrameType) string {
		return frameType.String()
	}

	m.commandDurationConnect = m.commandDurationSummary.WithLabelValues(labelForMethod(protocol.FrameTypeConnect), "")
	m.commandDurationSubscribe = m.commandDurationSummary.WithLabelValues(labelForMethod(protocol.FrameTypeSubscribe), "")
	m.commandDurationUnsubscribe = m.commandDurationSummary.WithLabelValues(labelForMethod(protocol.FrameTypeUnsubscribe), "")
	m.commandDurationPublish = m.commandDurationSummary.WithLabelValues(labelForMethod(protocol.FrameTypePublish), "")
	m.commandDurationPresence = m.commandDurationSummary.WithLabelValues(labelForMethod(protocol.FrameTypePresence), "")
	m.commandDurationPresenceStats = m.commandDurationSummary.WithLabelValues(labelForMethod(protocol.FrameTypePresenceStats), "")
	m.commandDurationHistory = m.commandDurationSummary.WithLabelValues(labelForMethod(protocol.FrameTypeHistory), "")
	m.commandDurationSend = m.commandDurationSummary.WithLabelValues(labelForMethod(protocol.FrameTypeSend), "")
	m.commandDurationRPC = m.commandDurationSummary.WithLabelValues(labelForMethod(protocol.FrameTypeRPC), "")
	m.commandDurationRefresh = m.commandDurationSummary.WithLabelValues(labelForMethod(protocol.FrameTypeRefresh), "")
	m.commandDurationSubRefresh = m.commandDurationSummary.WithLabelValues(labelForMethod(protocol.FrameTypeSubRefresh), "")
	m.commandDurationUnknown = m.commandDurationSummary.WithLabelValues("unknown", "")

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
		m.serverUnsubscribeCount,
		m.serverDisconnectCount,
		m.recoverCount,
		m.pingPongDurationHistogram,
		m.transportConnectCount,
		m.transportMessagesSent,
		m.transportMessagesSentSize,
		m.transportMessagesReceived,
		m.transportMessagesReceivedSize,
		m.buildInfoGauge,
		m.surveyDurationSummary,
		m.pubSubLagHistogram,
		m.broadcastDurationHistogram,
	} {
		if err := registerer.Register(collector); err != nil && !errors.As(err, &alreadyRegistered) {
			return nil, err
		}
	}
	return m, nil
}

func (m *metrics) observeCommandDuration(frameType protocol.FrameType, d time.Duration, ch string) {
	if ch != "" && m.config.GetChannelNamespaceLabel != nil {
		m.commandDurationSummary.WithLabelValues(frameType.String(), m.getChannelNamespaceLabel(ch)).Observe(d.Seconds())
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

func (m *metrics) incReplyError(frameType protocol.FrameType, code uint32, ch string) {
	m.replyErrorCount.WithLabelValues(frameType.String(), m.getCodeLabel(code), m.getChannelNamespaceLabel(ch)).Inc()
}

func (m *metrics) incRecover(success bool, ch string) {
	if m.config.GetChannelNamespaceLabel != nil {
		if success {
			m.recoverCount.WithLabelValues("yes", m.getChannelNamespaceLabel(ch)).Inc()
		} else {
			m.recoverCount.WithLabelValues("no", m.getChannelNamespaceLabel(ch)).Inc()
		}
		return
	}
	if success {
		m.recoverCountYes.Inc()
	} else {
		m.recoverCountNo.Inc()
	}
}

func (m *metrics) incTransportConnect(transport string) {
	switch transport {
	case transportWebsocket:
		m.transportConnectCountWebsocket.Inc()
	case transportSSE:
		m.transportConnectCountSSE.Inc()
	case transportHTTPStream:
		m.transportConnectCountHTTPStream.Inc()
	default:
		m.transportConnectCount.WithLabelValues(transport).Inc()
	}
}

type transportMessageLabels struct {
	Transport        string
	ChannelNamespace string
	FrameType        string
}

type transportMessagesSent struct {
	counterSent     prometheus.Counter
	counterSentSize prometheus.Counter
}

type transportMessagesReceived struct {
	counterReceived     prometheus.Counter
	counterReceivedSize prometheus.Counter
}

var (
	transportMessagesSentCache     sync.Map
	transportMessagesReceivedCache sync.Map
)

func (m *metrics) incTransportMessagesSent(transport string, frameType protocol.FrameType, channel string, size int) {
	channelNamespace := m.getChannelNamespaceLabel(channel)
	labels := transportMessageLabels{
		Transport:        transport,
		ChannelNamespace: channelNamespace,
		FrameType:        frameType.String(),
	}
	counters, ok := transportMessagesSentCache.Load(labels)
	if !ok {
		counterSent := m.transportMessagesSent.WithLabelValues(transport, labels.FrameType, channelNamespace)
		counterSentSize := m.transportMessagesSentSize.WithLabelValues(transport, labels.FrameType, channelNamespace)
		counters = transportMessagesSent{
			counterSent:     counterSent,
			counterSentSize: counterSentSize,
		}
		transportMessagesSentCache.Store(labels, counters)
	}
	counters.(transportMessagesSent).counterSent.Inc()
	counters.(transportMessagesSent).counterSentSize.Add(float64(size))
}

func (m *metrics) incTransportMessagesReceived(transport string, frameType protocol.FrameType, channel string, size int) {
	channelNamespace := m.getChannelNamespaceLabel(channel)
	labels := transportMessageLabels{
		Transport:        transport,
		ChannelNamespace: channelNamespace,
		FrameType:        frameType.String(),
	}
	counters, ok := transportMessagesReceivedCache.Load(labels)
	if !ok {
		counterReceived := m.transportMessagesReceived.WithLabelValues(transport, labels.FrameType, channelNamespace)
		counterReceivedSize := m.transportMessagesReceivedSize.WithLabelValues(transport, labels.FrameType, channelNamespace)
		counters = transportMessagesReceived{
			counterReceived:     counterReceived,
			counterReceivedSize: counterReceivedSize,
		}
		transportMessagesReceivedCache.Store(labels, counters)
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

func (m *metrics) incServerDisconnect(code uint32) {
	m.serverDisconnectCount.WithLabelValues(m.getCodeLabel(code)).Inc()
}

func (m *metrics) incServerUnsubscribe(code uint32, ch string) {
	m.serverUnsubscribeCount.WithLabelValues(m.getCodeLabel(code), m.getChannelNamespaceLabel(ch)).Inc()
}

func (m *metrics) incMessagesSent(msgType string, ch string) {
	if m.config.GetChannelNamespaceLabel != nil {
		m.messagesSentCount.WithLabelValues(msgType, m.getChannelNamespaceLabel(ch)).Inc()
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

func (m *metrics) incMessagesReceived(msgType string, ch string) {
	if m.config.GetChannelNamespaceLabel != nil {
		m.messagesReceivedCount.WithLabelValues(msgType, m.getChannelNamespaceLabel(ch)).Inc()
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

func (m *metrics) incActionCount(action string, ch string) {
	if m.config.GetChannelNamespaceLabel != nil {
		m.actionCount.WithLabelValues(action, m.getChannelNamespaceLabel(ch)).Inc()
		return
	}
	switch action {
	case "survey":
		m.actionCountSurvey.Inc()
	case "notify":
		m.actionCountNotify.Inc()
	case "add_client":
		m.actionCountAddClient.Inc()
	case "remove_client":
		m.actionCountRemoveClient.Inc()
	case "add_subscription":
		m.actionCountAddSub.Inc()
	case "broker_subscribe":
		m.actionCountBrokerSubscribe.Inc()
	case "remove_subscription":
		m.actionCountRemoveSub.Inc()
	case "broker_unsubscribe":
		m.actionCountBrokerUnsubscribe.Inc()
	case "add_presence":
		m.actionCountAddPresence.Inc()
	case "remove_presence":
		m.actionCountRemovePresence.Inc()
	case "presence":
		m.actionCountPresence.Inc()
	case "presence_stats":
		m.actionCountPresenceStats.Inc()
	case "history":
		m.actionCountHistory.Inc()
	case "history_recover":
		m.actionCountHistoryRecover.Inc()
	case "history_recover_cache":
		m.actionCountHistoryRecoverCache.Inc()
	case "history_stream_top":
		m.actionCountHistoryStreamTop.Inc()
	case "history_remove":
		m.actionCountHistoryRemove.Inc()
	default:
		m.actionCount.WithLabelValues(action, "").Inc()
	}
}

func (m *metrics) observeSurveyDuration(op string, d time.Duration) {
	m.surveyDurationSummary.WithLabelValues(op).Observe(d.Seconds())
}
