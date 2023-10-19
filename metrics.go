package centrifuge

import (
	"errors"
	"strconv"
	"strings"
	"sync"
	"time"

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
	serverDisconnectCount         *prometheus.CounterVec
	commandDurationSummary        *prometheus.SummaryVec
	surveyDurationSummary         *prometheus.SummaryVec
	recoverCount                  *prometheus.CounterVec
	transportConnectCount         *prometheus.CounterVec
	transportMessagesSent         *prometheus.CounterVec
	transportMessagesSentSize     *prometheus.CounterVec
	transportMessagesReceived     *prometheus.CounterVec
	transportMessagesReceivedSize *prometheus.CounterVec
	messagesBroadcastedCount      *prometheus.CounterVec

	messagesBroadcastedPublication prometheus.Counter
	messagesBroadcastedJoin        prometheus.Counter
	messagesBroadcastedLeave       prometheus.Counter

	messagesReceivedCountPublication prometheus.Counter
	messagesReceivedCountJoin        prometheus.Counter
	messagesReceivedCountLeave       prometheus.Counter
	messagesReceivedCountControl     prometheus.Counter

	messagesSentCountPublication prometheus.Counter
	messagesSentCountJoin        prometheus.Counter
	messagesSentCountLeave       prometheus.Counter
	messagesSentCountControl     prometheus.Counter

	actionCountAddClient        prometheus.Counter
	actionCountRemoveClient     prometheus.Counter
	actionCountAddSub           prometheus.Counter
	actionCountRemoveSub        prometheus.Counter
	actionCountAddPresence      prometheus.Counter
	actionCountRemovePresence   prometheus.Counter
	actionCountPresence         prometheus.Counter
	actionCountPresenceStats    prometheus.Counter
	actionCountHistory          prometheus.Counter
	actionCountHistoryRecover   prometheus.Counter
	actionCountHistoryStreamTop prometheus.Counter
	actionCountHistoryRemove    prometheus.Counter
	actionCountSurvey           prometheus.Counter
	actionCountNotify           prometheus.Counter

	recoverCountYes prometheus.Counter
	recoverCountNo  prometheus.Counter

	transportConnectCountWebsocket  prometheus.Counter
	transportConnectCountSockJS     prometheus.Counter
	transportConnectCountSSE        prometheus.Counter
	transportConnectCountHTTPStream prometheus.Counter

	commandDurationConnect       prometheus.Observer
	commandDurationSubscribe     prometheus.Observer
	commandDurationUnsubscribe   prometheus.Observer
	commandDurationPublish       prometheus.Observer
	commandDurationPresence      prometheus.Observer
	commandDurationPresenceStats prometheus.Observer
	commandDurationHistory       prometheus.Observer
	commandDurationPing          prometheus.Observer
	commandDurationSend          prometheus.Observer
	commandDurationRPC           prometheus.Observer
	commandDurationRefresh       prometheus.Observer
	commandDurationSubRefresh    prometheus.Observer
	commandDurationUnknown       prometheus.Observer
}

type commandMethodType int32

const (
	commandConnect       commandMethodType = 0
	commandSubscribe     commandMethodType = 1
	commandUnsubscribe   commandMethodType = 2
	commandPublish       commandMethodType = 3
	commandPresence      commandMethodType = 4
	commandPresenceStats commandMethodType = 5
	commandHistory       commandMethodType = 6
	commandPing          commandMethodType = 7
	commandSend          commandMethodType = 8
	commandRpc           commandMethodType = 9
	commandRefresh       commandMethodType = 10
	commandSubRefresh    commandMethodType = 11
)

var (
	commandMethodTypeName = map[int32]string{
		0:  "connect",
		1:  "subscribe",
		2:  "unsubscribe",
		3:  "publish",
		4:  "presence",
		5:  "presence_stats",
		6:  "history",
		7:  "ping",
		8:  "send",
		9:  "rpc",
		10: "refresh",
		11: "sub_refresh",
	}
)

func (m *metrics) observeCommandDuration(method commandMethodType, d time.Duration) {
	registryMu.RLock()
	defer registryMu.RUnlock()

	var observer prometheus.Observer

	switch method {
	case commandConnect:
		observer = m.commandDurationConnect
	case commandSubscribe:
		observer = m.commandDurationSubscribe
	case commandUnsubscribe:
		observer = m.commandDurationUnsubscribe
	case commandPublish:
		observer = m.commandDurationPublish
	case commandPresence:
		observer = m.commandDurationPresence
	case commandPresenceStats:
		observer = m.commandDurationPresenceStats
	case commandHistory:
		observer = m.commandDurationHistory
	case commandPing:
		observer = m.commandDurationPing
	case commandSend:
		observer = m.commandDurationSend
	case commandRpc:
		observer = m.commandDurationRPC
	case commandRefresh:
		observer = m.commandDurationRefresh
	case commandSubRefresh:
		observer = m.commandDurationSubRefresh
	default:
		observer = m.commandDurationUnknown
	}
	observer.Observe(d.Seconds())
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

func (m *metrics) incReplyError(method commandMethodType, code uint32) {
	m.replyErrorCount.WithLabelValues(commandMethodTypeName[int32(method)], strconv.FormatUint(uint64(code), 10)).Inc()
}

func (m *metrics) incRecover(success bool) {
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
	case transportSockJS:
		m.transportConnectCountSockJS.Inc()
	case transportSSE:
		m.transportConnectCountSSE.Inc()
	case transportHTTPStream:
		m.transportConnectCountHTTPStream.Inc()
	default:
		m.transportConnectCount.WithLabelValues(transport).Inc()
	}
}

type transportMessageLabels struct {
	Transport    string
	ChannelGroup string
}

var (
	transportMessagesSentCache     map[transportMessageLabels]prometheus.Counter
	transportMessagesSentSizeCache map[transportMessageLabels]prometheus.Counter
	messagesSentCacheMu            sync.RWMutex

	transportMessagesReceivedCache     map[transportMessageLabels]prometheus.Counter
	transportMessagesReceivedSizeCache map[transportMessageLabels]prometheus.Counter
	messagesReceivedCacheMu            sync.RWMutex
)

func init() {
	transportMessagesSentCache = make(map[transportMessageLabels]prometheus.Counter)
	transportMessagesSentSizeCache = make(map[transportMessageLabels]prometheus.Counter)
	transportMessagesReceivedCache = make(map[transportMessageLabels]prometheus.Counter)
	transportMessagesReceivedSizeCache = make(map[transportMessageLabels]prometheus.Counter)
}

func (m *metrics) incTransportMessagesSent(transport string, channelGroup string, size int) {
	labels := transportMessageLabels{
		Transport:    transport,
		ChannelGroup: channelGroup,
	}

	messagesSentCacheMu.RLock()
	counterSent, okSent := transportMessagesSentCache[labels]
	counterSentSize, okSentSize := transportMessagesSentSizeCache[labels]
	messagesSentCacheMu.RUnlock()

	if !okSent {
		counterSent = m.transportMessagesSent.WithLabelValues(transport, channelGroup)
		messagesSentCacheMu.Lock()
		transportMessagesSentCache[labels] = counterSent
		messagesSentCacheMu.Unlock()
	}

	if !okSentSize {
		counterSentSize = m.transportMessagesSentSize.WithLabelValues(transport, channelGroup)
		messagesSentCacheMu.Lock()
		transportMessagesSentSizeCache[labels] = counterSentSize
		messagesSentCacheMu.Unlock()
	}
	counterSent.Inc()
	counterSentSize.Add(float64(size))
}

func (m *metrics) incTransportMessagesReceived(transport string, channelGroup string, size int) {
	registryMu.RLock()
	defer registryMu.RUnlock()

	labels := transportMessageLabels{
		Transport:    transport,
		ChannelGroup: channelGroup,
	}

	messagesReceivedCacheMu.RLock()
	counterReceived, okReceived := transportMessagesReceivedCache[labels]
	counterReceivedSize, okReceivedSize := transportMessagesReceivedSizeCache[labels]
	messagesReceivedCacheMu.RUnlock()

	if !okReceived {
		counterReceived = m.transportMessagesReceived.WithLabelValues(transport, channelGroup)
		messagesReceivedCacheMu.Lock()
		transportMessagesReceivedCache[labels] = counterReceived
		messagesReceivedCacheMu.Unlock()
	}

	if !okReceivedSize {
		counterReceivedSize = m.transportMessagesReceivedSize.WithLabelValues(transport, channelGroup)
		messagesReceivedCacheMu.Lock()
		transportMessagesReceivedSizeCache[labels] = counterReceivedSize
		messagesReceivedCacheMu.Unlock()
	}
	counterReceived.Inc()
	counterReceivedSize.Add(float64(size))
}

func (m *metrics) incServerDisconnect(code uint32) {
	m.serverDisconnectCount.WithLabelValues(strconv.FormatUint(uint64(code), 10)).Inc()
}

func (m *metrics) incMessagesSent(msgType string) {
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
		m.messagesSentCount.WithLabelValues(msgType).Inc()
	}
}

func (m *metrics) incMessagesReceived(msgType string) {
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
		m.messagesReceivedCount.WithLabelValues(msgType).Inc()
	}
}

func (m *metrics) incMessagesBroadcasted(msgType string, numSubscribers int) {
	switch msgType {
	case "publication":
		m.messagesBroadcastedPublication.Add(float64(numSubscribers))
	case "join":
		m.messagesBroadcastedJoin.Add(float64(numSubscribers))
	case "leave":
		m.messagesBroadcastedLeave.Add(float64(numSubscribers))
	default:
		m.messagesBroadcastedCount.WithLabelValues(msgType).Add(float64(numSubscribers))
	}
}

func (m *metrics) incActionCount(action string) {
	switch action {
	case "add_client":
		m.actionCountAddClient.Inc()
	case "remove_client":
		m.actionCountRemoveClient.Inc()
	case "add_subscription":
		m.actionCountAddSub.Inc()
	case "remove_subscription":
		m.actionCountRemoveSub.Inc()
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
	case "history_stream_top":
		m.actionCountHistoryStreamTop.Inc()
	case "history_remove":
		m.actionCountHistoryRemove.Inc()
	case "survey":
		m.actionCountSurvey.Inc()
	case "notify":
		m.actionCountNotify.Inc()
	}
}

func (m *metrics) observeSurveyDuration(op string, d time.Duration) {
	m.surveyDurationSummary.WithLabelValues(op).Observe(d.Seconds())
}

func initMetricsRegistry(registry prometheus.Registerer, metricsNamespace string) (*metrics, error) {
	registryMu.Lock()
	defer registryMu.Unlock()

	if metricsNamespace == "" {
		metricsNamespace = defaultMetricsNamespace
	}
	if registry == nil {
		registry = prometheus.DefaultRegisterer
	}

	m := &metrics{}

	m.messagesSentCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: "node",
		Name:      "messages_sent_count",
		Help:      "Number of messages sent.",
	}, []string{"type"})

	m.messagesReceivedCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: "node",
		Name:      "messages_received_count",
		Help:      "Number of messages received from engine.",
	}, []string{"type"})

	m.messagesBroadcastedCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: "node",
		Name:      "messages_broadcasted_count",
		Help:      "Number of messages broadcasted to subscribers.",
	}, []string{"type"})

	m.actionCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: "node",
		Name:      "action_count",
		Help:      "Number of node actions called.",
	}, []string{"action"})

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
		Help:      "Number of nodes in cluster.",
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

	m.replyErrorCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: "client",
		Name:      "num_reply_errors",
		Help:      "Number of errors in replies sent to clients.",
	}, []string{"method", "code"})

	m.serverDisconnectCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: "client",
		Name:      "num_server_disconnects",
		Help:      "Number of server initiated disconnects.",
	}, []string{"code"})

	m.commandDurationSummary = prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Namespace:  metricsNamespace,
		Subsystem:  "client",
		Name:       "command_duration_seconds",
		Objectives: map[float64]float64{0.5: 0.05, 0.99: 0.001, 0.999: 0.0001},
		Help:       "Client command duration summary.",
	}, []string{"method"})

	m.recoverCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: "client",
		Name:      "recover",
		Help:      "Count of recover operations.",
	}, []string{"recovered"})

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
		Help:      "Number of messages sent over specific transport.",
	}, []string{"transport", "channel_namespace"})

	m.transportMessagesSentSize = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: "transport",
		Name:      "messages_sent_size",
		Help:      "Size in bytes of messages sent over specific transport.",
	}, []string{"transport", "channel_namespace"})

	m.transportMessagesReceived = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: "transport",
		Name:      "messages_received",
		Help:      "Number of messages received over specific transport.",
	}, []string{"transport", "channel_namespace"})

	m.transportMessagesReceivedSize = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: "transport",
		Name:      "messages_received_size",
		Help:      "Size in bytes of messages received over specific transport.",
	}, []string{"transport", "channel_namespace"})

	m.messagesReceivedCountPublication = m.messagesReceivedCount.WithLabelValues("publication")
	m.messagesReceivedCountJoin = m.messagesReceivedCount.WithLabelValues("join")
	m.messagesReceivedCountLeave = m.messagesReceivedCount.WithLabelValues("leave")
	m.messagesReceivedCountControl = m.messagesReceivedCount.WithLabelValues("control")

	m.messagesSentCountPublication = m.messagesSentCount.WithLabelValues("publication")
	m.messagesSentCountJoin = m.messagesSentCount.WithLabelValues("join")
	m.messagesSentCountLeave = m.messagesSentCount.WithLabelValues("leave")
	m.messagesSentCountControl = m.messagesSentCount.WithLabelValues("control")

	m.messagesBroadcastedPublication = m.messagesBroadcastedCount.WithLabelValues("publication")
	m.messagesBroadcastedJoin = m.messagesBroadcastedCount.WithLabelValues("join")
	m.messagesBroadcastedLeave = m.messagesBroadcastedCount.WithLabelValues("leave")

	m.actionCountAddClient = m.actionCount.WithLabelValues("add_client")
	m.actionCountRemoveClient = m.actionCount.WithLabelValues("remove_client")
	m.actionCountAddSub = m.actionCount.WithLabelValues("add_subscription")
	m.actionCountRemoveSub = m.actionCount.WithLabelValues("remove_subscription")
	m.actionCountAddPresence = m.actionCount.WithLabelValues("add_presence")
	m.actionCountRemovePresence = m.actionCount.WithLabelValues("remove_presence")
	m.actionCountPresence = m.actionCount.WithLabelValues("presence")
	m.actionCountPresenceStats = m.actionCount.WithLabelValues("presence_stats")
	m.actionCountHistory = m.actionCount.WithLabelValues("history")
	m.actionCountHistoryRecover = m.actionCount.WithLabelValues("history_recover")
	m.actionCountHistoryStreamTop = m.actionCount.WithLabelValues("history_stream_top")
	m.actionCountHistoryRemove = m.actionCount.WithLabelValues("history_remove")
	m.actionCountSurvey = m.actionCount.WithLabelValues("survey")
	m.actionCountNotify = m.actionCount.WithLabelValues("notify")

	m.recoverCountYes = m.recoverCount.WithLabelValues("yes")
	m.recoverCountNo = m.recoverCount.WithLabelValues("no")

	m.transportConnectCountWebsocket = m.transportConnectCount.WithLabelValues(transportWebsocket)
	m.transportConnectCountSockJS = m.transportConnectCount.WithLabelValues(transportSockJS)
	m.transportConnectCountHTTPStream = m.transportConnectCount.WithLabelValues(transportHTTPStream)
	m.transportConnectCountSSE = m.transportConnectCount.WithLabelValues(transportSSE)

	labelForMethod := func(methodType commandMethodType) string {
		return strings.ToLower(commandMethodTypeName[int32(methodType)])
	}

	m.commandDurationConnect = m.commandDurationSummary.WithLabelValues(labelForMethod(commandConnect))
	m.commandDurationSubscribe = m.commandDurationSummary.WithLabelValues(labelForMethod(commandSubscribe))
	m.commandDurationUnsubscribe = m.commandDurationSummary.WithLabelValues(labelForMethod(commandUnsubscribe))
	m.commandDurationPublish = m.commandDurationSummary.WithLabelValues(labelForMethod(commandPublish))
	m.commandDurationPresence = m.commandDurationSummary.WithLabelValues(labelForMethod(commandPresence))
	m.commandDurationPresenceStats = m.commandDurationSummary.WithLabelValues(labelForMethod(commandPresenceStats))
	m.commandDurationHistory = m.commandDurationSummary.WithLabelValues(labelForMethod(commandHistory))
	m.commandDurationPing = m.commandDurationSummary.WithLabelValues(labelForMethod(commandPing))
	m.commandDurationSend = m.commandDurationSummary.WithLabelValues(labelForMethod(commandSend))
	m.commandDurationRPC = m.commandDurationSummary.WithLabelValues(labelForMethod(commandRpc))
	m.commandDurationRefresh = m.commandDurationSummary.WithLabelValues(labelForMethod(commandRefresh))
	m.commandDurationSubRefresh = m.commandDurationSummary.WithLabelValues(labelForMethod(commandSubRefresh))
	m.commandDurationUnknown = m.commandDurationSummary.WithLabelValues("unknown")

	var alreadyRegistered prometheus.AlreadyRegisteredError

	if err := registry.Register(m.messagesBroadcastedCount); err != nil && !errors.As(err, &alreadyRegistered) {
		return nil, err
	}
	if err := registry.Register(m.messagesSentCount); err != nil && !errors.As(err, &alreadyRegistered) {
		return nil, err
	}
	if err := registry.Register(m.messagesReceivedCount); err != nil && !errors.As(err, &alreadyRegistered) {
		return nil, err
	}
	if err := registry.Register(m.actionCount); err != nil && !errors.As(err, &alreadyRegistered) {
		return nil, err
	}
	if err := registry.Register(m.numClientsGauge); err != nil && !errors.As(err, &alreadyRegistered) {
		return nil, err
	}
	if err := registry.Register(m.numUsersGauge); err != nil && !errors.As(err, &alreadyRegistered) {
		return nil, err
	}
	if err := registry.Register(m.numSubsGauge); err != nil && !errors.As(err, &alreadyRegistered) {
		return nil, err
	}
	if err := registry.Register(m.numChannelsGauge); err != nil && !errors.As(err, &alreadyRegistered) {
		return nil, err
	}
	if err := registry.Register(m.numNodesGauge); err != nil && !errors.As(err, &alreadyRegistered) {
		return nil, err
	}
	if err := registry.Register(m.commandDurationSummary); err != nil && !errors.As(err, &alreadyRegistered) {
		return nil, err
	}
	if err := registry.Register(m.replyErrorCount); err != nil && !errors.As(err, &alreadyRegistered) {
		return nil, err
	}
	if err := registry.Register(m.serverDisconnectCount); err != nil && !errors.As(err, &alreadyRegistered) {
		return nil, err
	}
	if err := registry.Register(m.recoverCount); err != nil && !errors.As(err, &alreadyRegistered) {
		return nil, err
	}
	if err := registry.Register(m.transportConnectCount); err != nil && !errors.As(err, &alreadyRegistered) {
		return nil, err
	}
	if err := registry.Register(m.transportMessagesSent); err != nil && !errors.As(err, &alreadyRegistered) {
		return nil, err
	}
	if err := registry.Register(m.transportMessagesSentSize); err != nil && !errors.As(err, &alreadyRegistered) {
		return nil, err
	}
	if err := registry.Register(m.transportMessagesReceived); err != nil && !errors.As(err, &alreadyRegistered) {
		return nil, err
	}
	if err := registry.Register(m.transportMessagesReceivedSize); err != nil && !errors.As(err, &alreadyRegistered) {
		return nil, err
	}
	if err := registry.Register(m.buildInfoGauge); err != nil && !errors.As(err, &alreadyRegistered) {
		return nil, err
	}
	if err := registry.Register(m.surveyDurationSummary); err != nil && !errors.As(err, &alreadyRegistered) {
		return nil, err
	}
	return m, nil
}
