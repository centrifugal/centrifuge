package centrifuge

import (
	"github.com/prometheus/client_golang/prometheus"
)

// default namespace for prometheus metrics.
var defaultMetricsNamespace = "centrifuge"

var (
	messagesSentCount      *prometheus.CounterVec
	messagesReceivedCount  *prometheus.CounterVec
	actionCount            *prometheus.CounterVec
	numClientsGauge        prometheus.Gauge
	numUsersGauge          prometheus.Gauge
	buildInfoGauge         *prometheus.GaugeVec
	numChannelsGauge       prometheus.Gauge
	numNodesGauge          prometheus.Gauge
	replyErrorCount        *prometheus.CounterVec
	serverDisconnectCount  *prometheus.CounterVec
	commandDurationSummary *prometheus.SummaryVec
	recoverCount           *prometheus.CounterVec
	transportConnectCount  *prometheus.CounterVec
	transportMessagesSent  *prometheus.CounterVec

	messagesReceivedCountPublication prometheus.Counter
	messagesReceivedCountJoin        prometheus.Counter
	messagesReceivedCountLeave       prometheus.Counter
	messagesReceivedCountControl     prometheus.Counter

	messagesSentCountPublication prometheus.Counter
	messagesSentCountJoin        prometheus.Counter
	messagesSentCountLeave       prometheus.Counter
	messagesSentCountControl     prometheus.Counter
)

func initMetricsRegistry(registry prometheus.Registerer, metricsNamespace string) error {
	if metricsNamespace == "" {
		metricsNamespace = defaultMetricsNamespace
	}
	if registry == nil {
		registry = prometheus.DefaultRegisterer
	}

	messagesSentCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: "node",
		Name:      "messages_sent_count",
		Help:      "Number of messages sent.",
	}, []string{"type"})

	messagesReceivedCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: "node",
		Name:      "messages_received_count",
		Help:      "Number of messages received.",
	}, []string{"type"})

	actionCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: "node",
		Name:      "action_count",
		Help:      "Number of node actions called.",
	}, []string{"action"})

	numClientsGauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Subsystem: "node",
		Name:      "num_clients",
		Help:      "Number of clients connected.",
	})

	numUsersGauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Subsystem: "node",
		Name:      "num_users",
		Help:      "Number of unique users connected.",
	})

	numNodesGauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Subsystem: "node",
		Name:      "num_nodes",
		Help:      "Number of nodes in cluster.",
	})

	buildInfoGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Subsystem: "node",
		Name:      "build",
		Help:      "Node build info.",
	}, []string{"version"})

	numChannelsGauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Subsystem: "node",
		Name:      "num_channels",
		Help:      "Number of channels with one or more subscribers.",
	})

	replyErrorCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: "client",
		Name:      "num_reply_errors",
		Help:      "Number of errors in replies sent to clients.",
	}, []string{"method", "code"})

	serverDisconnectCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: "client",
		Name:      "num_server_disconnects",
		Help:      "Number of server initiated disconnects.",
	}, []string{"code"})

	commandDurationSummary = prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Namespace:  metricsNamespace,
		Subsystem:  "client",
		Name:       "command_duration_seconds",
		Objectives: map[float64]float64{0.5: 0.05, 0.99: 0.001, 0.999: 0.0001},
		Help:       "Client command duration summary.",
	}, []string{"method"})

	recoverCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: "client",
		Name:      "recover",
		Help:      "Count of recover operations.",
	}, []string{"recovered"})

	transportConnectCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: "transport",
		Name:      "connect_count",
		Help:      "Number of connections to specific transport.",
	}, []string{"transport"})

	transportMessagesSent = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: "transport",
		Name:      "messages_sent",
		Help:      "Number of messages sent over specific transport.",
	}, []string{"transport"})

	if err := registry.Register(messagesSentCount); err != nil {
		return err
	}
	if err := registry.Register(messagesReceivedCount); err != nil {
		return err
	}
	if err := registry.Register(actionCount); err != nil {
		return err
	}
	if err := registry.Register(numClientsGauge); err != nil {
		return err
	}
	if err := registry.Register(numUsersGauge); err != nil {
		return err
	}
	if err := registry.Register(numChannelsGauge); err != nil {
		return err
	}
	if err := registry.Register(numNodesGauge); err != nil {
		return err
	}
	if err := registry.Register(commandDurationSummary); err != nil {
		return err
	}
	if err := registry.Register(replyErrorCount); err != nil {
		return err
	}
	if err := registry.Register(serverDisconnectCount); err != nil {
		return err
	}
	if err := registry.Register(recoverCount); err != nil {
		return err
	}
	if err := registry.Register(transportConnectCount); err != nil {
		return err
	}
	if err := registry.Register(transportMessagesSent); err != nil {
		return err
	}
	if err := registry.Register(buildInfoGauge); err != nil {
		return err
	}

	messagesReceivedCountPublication = messagesReceivedCount.WithLabelValues("publication")
	messagesReceivedCountJoin = messagesReceivedCount.WithLabelValues("join")
	messagesReceivedCountLeave = messagesReceivedCount.WithLabelValues("leave")
	messagesReceivedCountControl = messagesReceivedCount.WithLabelValues("control")

	messagesSentCountPublication = messagesSentCount.WithLabelValues("publication")
	messagesSentCountJoin = messagesReceivedCount.WithLabelValues("join")
	messagesSentCountLeave = messagesReceivedCount.WithLabelValues("leave")
	messagesSentCountControl = messagesReceivedCount.WithLabelValues("control")
	return nil
}
