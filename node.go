package centrifuge

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/centrifugal/centrifuge/internal/controlpb"
	"github.com/centrifugal/centrifuge/internal/controlproto"
	"github.com/centrifugal/centrifuge/internal/dissolve"
	"github.com/centrifugal/centrifuge/internal/nowtime"

	"github.com/FZambia/eagle"
	"github.com/centrifugal/protocol"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/singleflight"
)

// Node is a heart of Centrifuge library – it keeps and manages client connections,
// maintains information about other Centrifuge nodes in cluster, keeps references
// to common things (like Broker and PresenceManager, Hub) etc.
// By default Node uses in-memory implementations of Broker and PresenceManager -
// MemoryBroker and MemoryPresenceManager which allow running a single Node only.
// To scale use other implementations of Broker and PresenceManager like builtin
// RedisBroker and RedisPresenceManager.
type Node struct {
	mu sync.RWMutex
	// unique id for this node.
	uid string
	// startedAt is unix time of node start.
	startedAt int64
	// config for node.
	config Config
	// hub to manage client connections.
	hub *Hub
	// broker is responsible for PUB/SUB and history streaming mechanics.
	broker Broker
	// presenceManager is responsible for presence information management.
	presenceManager PresenceManager
	// nodes contains registry of known nodes.
	nodes *nodeRegistry
	// shutdown is a flag which is only true when node is going to shut down.
	shutdown bool
	// shutdownCh is a channel which is closed when node shutdown initiated.
	shutdownCh chan struct{}
	// clientEvents to manage event handlers attached to node.
	clientEvents *eventHub
	// logger allows to log throughout library code and proxy log entries to
	// configured log handler.
	logger *logger
	// cache control encoder in Node.
	controlEncoder controlproto.Encoder
	// cache control decoder in Node.
	controlDecoder controlproto.Decoder
	// subLocks synchronizes access to adding/removing subscriptions.
	subLocks map[int]*sync.Mutex

	metricsMu       sync.Mutex
	metricsExporter *eagle.Eagle
	metricsSnapshot *eagle.Metrics

	// subDissolver used to reliably clear unused subscriptions in Broker.
	subDissolver *dissolve.Dissolver

	// nowTimeGetter provides access to current time.
	nowTimeGetter nowtime.Getter

	surveyHandler  SurveyHandler
	surveyRegistry map[uint64]chan survey
	surveyMu       sync.RWMutex
	surveyID       uint64

	notificationHandler   NotificationHandler
	transportWriteHandler TransportWriteHandler
	nodeInfoSendHandler   NodeInfoSendHandler
}

const (
	numSubLocks            = 16384
	numSubDissolverWorkers = 64
)

// New creates Node with provided Config.
func New(c Config) (*Node, error) {
	uid := uuid.Must(uuid.NewRandom()).String()

	subLocks := make(map[int]*sync.Mutex, numSubLocks)
	for i := 0; i < numSubLocks; i++ {
		subLocks[i] = &sync.Mutex{}
	}

	if c.Name == "" {
		hostname, err := os.Hostname()
		if err != nil {
			return nil, err
		}
		c.Name = hostname
	}

	n := &Node{
		uid:            uid,
		nodes:          newNodeRegistry(uid),
		config:         c,
		hub:            newHub(),
		startedAt:      time.Now().Unix(),
		shutdownCh:     make(chan struct{}),
		logger:         nil,
		controlEncoder: controlproto.NewProtobufEncoder(),
		controlDecoder: controlproto.NewProtobufDecoder(),
		clientEvents:   &eventHub{},
		subLocks:       subLocks,
		subDissolver:   dissolve.New(numSubDissolverWorkers),
		nowTimeGetter:  nowtime.Get,
		surveyRegistry: make(map[uint64]chan survey),
	}

	if c.LogHandler != nil {
		n.logger = newLogger(c.LogLevel, c.LogHandler)
	}

	b, err := NewMemoryBroker(n, MemoryBrokerConfig{})
	if err != nil {
		return nil, err
	}
	n.SetBroker(b)

	m, err := NewMemoryPresenceManager(n, MemoryPresenceManagerConfig{})
	if err != nil {
		return nil, err
	}
	n.SetPresenceManager(m)

	if err := initMetricsRegistry(prometheus.DefaultRegisterer, c.MetricsNamespace); err != nil {
		switch err.(type) {
		case prometheus.AlreadyRegisteredError:
			// Can happens when node initialized several times since we use DefaultRegisterer,
			// skip for now.
		default:
			return nil, err
		}
	}

	return n, nil
}

// index chooses bucket number in range [0, numBuckets).
func index(s string, numBuckets int) int {
	if numBuckets == 1 {
		return 0
	}
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(s))
	return int(hash.Sum64() % uint64(numBuckets))
}

// ID returns unique Node identifier. This is a UUID v4 value.
func (n *Node) ID() string {
	return n.uid
}

func (n *Node) subLock(ch string) *sync.Mutex {
	return n.subLocks[index(ch, numSubLocks)]
}

// SetBroker allows to set Broker implementation to use.
func (n *Node) SetBroker(b Broker) {
	n.broker = b
}

// SetPresenceManager allows to set PresenceManager to use.
func (n *Node) SetPresenceManager(m PresenceManager) {
	n.presenceManager = m
}

// Hub returns node's Hub.
func (n *Node) Hub() *Hub {
	return n.hub
}

// Run performs node startup actions. At moment must be called once on start
// after Broker set to Node.
func (n *Node) Run() error {
	if err := n.broker.Run(&brokerEventHandler{n}); err != nil {
		return err
	}
	err := n.initMetrics()
	if err != nil {
		n.logger.log(newLogEntry(LogLevelError, "error on init metrics", map[string]interface{}{"error": err.Error()}))
		return err
	}
	err = n.pubNode("")
	if err != nil {
		n.logger.log(newLogEntry(LogLevelError, "error publishing node control command", map[string]interface{}{"error": err.Error()}))
		return err
	}
	go n.sendNodePing()
	go n.cleanNodeInfo()
	go n.updateMetrics()
	return n.subDissolver.Run()
}

// Log allows to log entry.
func (n *Node) Log(entry LogEntry) {
	n.logger.log(entry)
}

// LogEnabled allows to log entry.
func (n *Node) LogEnabled(level LogLevel) bool {
	return n.logger.enabled(level)
}

// Shutdown sets shutdown flag to Node so handlers could stop accepting
// new requests and disconnects clients with shutdown reason.
func (n *Node) Shutdown(ctx context.Context) error {
	n.mu.Lock()
	if n.shutdown {
		n.mu.Unlock()
		return nil
	}
	n.shutdown = true
	close(n.shutdownCh)
	n.mu.Unlock()
	cmd := &controlpb.Command{
		Uid:    n.uid,
		Method: controlpb.Command_SHUTDOWN,
	}
	_ = n.publishControl(cmd, "")
	if closer, ok := n.broker.(Closer); ok {
		defer func() { _ = closer.Close(ctx) }()
	}
	if n.presenceManager != nil {
		if closer, ok := n.presenceManager.(Closer); ok {
			defer func() { _ = closer.Close(ctx) }()
		}
	}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_ = n.subDissolver.Close()
	}()
	go func() {
		defer wg.Done()
		_ = n.hub.shutdown(ctx)
	}()
	wg.Wait()
	return ctx.Err()
}

// NotifyShutdown returns a channel which will be closed on node shutdown.
func (n *Node) NotifyShutdown() chan struct{} {
	return n.shutdownCh
}

func (n *Node) updateGauges() {
	setNumClients(float64(n.hub.NumClients()))
	setNumUsers(float64(n.hub.NumUsers()))
	setNumSubscriptions(float64(n.hub.NumSubscriptions()))
	setNumChannels(float64(n.hub.NumChannels()))
	setNumNodes(float64(len(n.nodes.list())))
	version := n.config.Version
	if version == "" {
		version = "_"
	}
	setBuildInfo(version)
}

func (n *Node) updateMetrics() {
	n.updateGauges()
	for {
		select {
		case <-n.shutdownCh:
			return
		case <-time.After(10 * time.Second):
			n.updateGauges()
		}
	}
}

// Centrifuge library uses Prometheus metrics for instrumentation. But we also try to
// aggregate Prometheus metrics periodically and share this information between Nodes.
func (n *Node) initMetrics() error {
	if n.config.NodeInfoMetricsAggregateInterval == 0 {
		return nil
	}
	metricsSink := make(chan eagle.Metrics)
	n.metricsExporter = eagle.New(eagle.Config{
		Gatherer: prometheus.DefaultGatherer,
		Interval: n.config.NodeInfoMetricsAggregateInterval,
		Sink:     metricsSink,
	})
	metrics, err := n.metricsExporter.Export()
	if err != nil {
		return err
	}
	n.metricsMu.Lock()
	n.metricsSnapshot = &metrics
	n.metricsMu.Unlock()
	go func() {
		for {
			select {
			case <-n.NotifyShutdown():
				return
			case metrics := <-metricsSink:
				n.metricsMu.Lock()
				n.metricsSnapshot = &metrics
				n.metricsMu.Unlock()
			}
		}
	}()
	return nil
}

func (n *Node) sendNodePing() {
	for {
		select {
		case <-n.shutdownCh:
			return
		case <-time.After(nodeInfoPublishInterval):
			err := n.pubNode("")
			if err != nil {
				n.logger.log(newLogEntry(LogLevelError, "error publishing node control command", map[string]interface{}{"error": err.Error()}))
			}
		}
	}
}

func (n *Node) cleanNodeInfo() {
	for {
		select {
		case <-n.shutdownCh:
			return
		case <-time.After(nodeInfoCleanInterval):
			n.mu.RLock()
			delay := nodeInfoMaxDelay
			n.mu.RUnlock()
			n.nodes.clean(delay)
		}
	}
}

func (n *Node) handleNotification(fromNodeID string, req *controlpb.Notification) error {
	if n.notificationHandler == nil {
		return nil
	}
	n.notificationHandler(NotificationEvent{
		FromNodeID: fromNodeID,
		Op:         req.Op,
		Data:       req.Data,
	})
	return nil
}

func (n *Node) handleSurveyRequest(fromNodeID string, req *controlpb.SurveyRequest) error {
	if n.surveyHandler == nil {
		return nil
	}
	cb := func(reply SurveyReply) {
		surveyResponse := &controlpb.SurveyResponse{
			Id:   req.Id,
			Code: reply.Code,
			Data: reply.Data,
		}
		params, _ := n.controlEncoder.EncodeSurveyResponse(surveyResponse)

		cmd := &controlpb.Command{
			Uid:    n.uid,
			Method: controlpb.Command_SURVEY_RESPONSE,
			Params: params,
		}
		_ = n.publishControl(cmd, fromNodeID)
	}
	n.surveyHandler(SurveyEvent{Op: req.Op, Data: req.Data}, cb)
	return nil
}

func (n *Node) handleSurveyResponse(uid string, resp *controlpb.SurveyResponse) error {
	n.surveyMu.RLock()
	defer n.surveyMu.RUnlock()
	if ch, ok := n.surveyRegistry[resp.Id]; ok {
		select {
		case ch <- survey{
			UID: uid,
			Result: SurveyResult{
				Code: resp.Code,
				Data: resp.Data,
			},
		}:
		default:
			// Survey channel allocated with capacity enough to receive all survey replies,
			// default case here means that channel has no reader anymore, so it's safe to
			// skip message. This extra survey reply can come from extra node that just
			// joined.
		}
	}
	return nil
}

// SurveyResult from node.
type SurveyResult struct {
	Code uint32
	Data []byte
}

type survey struct {
	UID    string
	Result SurveyResult
}

var errSurveyHandlerNotRegistered = errors.New("no survey handler registered")

const defaultSurveyTimeout = 10 * time.Second

// Survey allows collecting data from all running Centrifuge nodes. This method publishes
// control messages, then waits for replies from all running nodes. The maximum time to wait
// can be controlled over context timeout. If provided context does not have a deadline for
// survey then this method uses default 10 seconds timeout. Keep in mind that Survey does not
// scale very well as number of Centrifuge Node grows. Though it has reasonably good performance
// to perform rare tasks even with relatively large number of nodes.
func (n *Node) Survey(ctx context.Context, op string, data []byte) (map[string]SurveyResult, error) {
	if n.surveyHandler == nil {
		return nil, errSurveyHandlerNotRegistered
	}

	incActionCount("survey")

	if _, ok := ctx.Deadline(); !ok {
		// If no timeout provided then fallback to defaultSurveyTimeout to avoid endless surveys.
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, defaultSurveyTimeout)
		defer cancel()
	}

	numNodes := len(n.nodes.list())

	n.surveyMu.Lock()
	n.surveyID++
	surveyRequest := &controlpb.SurveyRequest{
		Id:   n.surveyID,
		Op:   op,
		Data: data,
	}
	params, err := n.controlEncoder.EncodeSurveyRequest(surveyRequest)
	if err != nil {
		n.surveyMu.Unlock()
		return nil, err
	}
	surveyChan := make(chan survey, numNodes)
	n.surveyRegistry[surveyRequest.Id] = surveyChan
	n.surveyMu.Unlock()

	defer func() {
		n.surveyMu.Lock()
		defer n.surveyMu.Unlock()
		delete(n.surveyRegistry, surveyRequest.Id)
	}()

	results := map[string]SurveyResult{}

	// Invoke handler on this node since control message handler
	// ignores those sent from the current Node.
	n.surveyHandler(SurveyEvent{Op: op, Data: data}, func(reply SurveyReply) {
		surveyChan <- survey{
			UID:    n.uid,
			Result: SurveyResult(reply),
		}
	})

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		for {
			select {
			case resp := <-surveyChan:
				results[resp.UID] = resp.Result
				if len(results) == numNodes {
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	cmd := &controlpb.Command{
		Uid:    n.uid,
		Method: controlpb.Command_SURVEY_REQUEST,
		Params: params,
	}
	err = n.publishControl(cmd, "")
	if err != nil {
		return nil, err
	}

	wg.Wait()
	return results, ctx.Err()
}

// Info contains information about all known server nodes.
type Info struct {
	Nodes []NodeInfo
}

// Metrics aggregation over time interval for node.
type Metrics struct {
	Interval float64
	Items    map[string]float64
}

// NodeInfo contains information about node.
type NodeInfo struct {
	UID         string
	Name        string
	Version     string
	NumClients  uint32
	NumUsers    uint32
	NumSubs     uint32
	NumChannels uint32
	Uptime      uint32
	Metrics     *Metrics
	Data        []byte
}

// Info returns aggregated stats from all nodes.
func (n *Node) Info() (Info, error) {
	nodes := n.nodes.list()
	nodeResults := make([]NodeInfo, len(nodes))
	for i, nd := range nodes {
		info := NodeInfo{
			UID:         nd.Uid,
			Name:        nd.Name,
			Version:     nd.Version,
			NumClients:  nd.NumClients,
			NumUsers:    nd.NumUsers,
			NumSubs:     nd.NumSubs,
			NumChannels: nd.NumChannels,
			Uptime:      nd.Uptime,
			Data:        nd.Data,
		}
		if nd.Metrics != nil {
			info.Metrics = &Metrics{
				Interval: nd.Metrics.Interval,
				Items:    nd.Metrics.Items,
			}
		}
		nodeResults[i] = info
	}

	return Info{
		Nodes: nodeResults,
	}, nil
}

// handleControl handles messages from control channel - control messages used for internal
// communication between nodes to share state or proto.
func (n *Node) handleControl(data []byte) error {
	incMessagesReceived("control")

	cmd, err := n.controlDecoder.DecodeCommand(data)
	if err != nil {
		n.logger.log(newLogEntry(LogLevelError, "error decoding control command", map[string]interface{}{"error": err.Error()}))
		return err
	}

	if cmd.Uid == n.uid {
		// Sent by this node.
		return nil
	}

	uid := cmd.Uid
	method := cmd.Method
	params := cmd.Params

	switch method {
	case controlpb.Command_NODE:
		cmd, err := n.controlDecoder.DecodeNode(params)
		if err != nil {
			n.logger.log(newLogEntry(LogLevelError, "error decoding node control params", map[string]interface{}{"error": err.Error()}))
			return err
		}
		return n.nodeCmd(cmd)
	case controlpb.Command_SHUTDOWN:
		return n.shutdownCmd(uid)
	case controlpb.Command_UNSUBSCRIBE:
		cmd, err := n.controlDecoder.DecodeUnsubscribe(params)
		if err != nil {
			n.logger.log(newLogEntry(LogLevelError, "error decoding unsubscribe control params", map[string]interface{}{"error": err.Error()}))
			return err
		}
		return n.hub.unsubscribe(cmd.User, cmd.Channel, cmd.Client)
	case controlpb.Command_SUBSCRIBE:
		cmd, err := n.controlDecoder.DecodeSubscribe(params)
		if err != nil {
			n.logger.log(newLogEntry(LogLevelError, "error decoding subscribe control params", map[string]interface{}{"error": err.Error()}))
			return err
		}
		var recoverSince *StreamPosition
		if cmd.RecoverSince != nil {
			recoverSince = &StreamPosition{Offset: cmd.RecoverSince.Offset, Epoch: cmd.RecoverSince.Epoch}
		}
		return n.hub.subscribe(cmd.User, cmd.Channel, cmd.Client, WithExpireAt(cmd.ExpireAt), WithChannelInfo(cmd.ChannelInfo), WithPresence(cmd.Presence), WithJoinLeave(cmd.JoinLeave), WithPosition(cmd.Position), WithRecover(cmd.Recover), WithSubscribeData(cmd.Data), WithRecoverSince(recoverSince))
	case controlpb.Command_DISCONNECT:
		cmd, err := n.controlDecoder.DecodeDisconnect(params)
		if err != nil {
			n.logger.log(newLogEntry(LogLevelError, "error decoding disconnect control params", map[string]interface{}{"error": err.Error()}))
			return err
		}
		return n.hub.disconnect(cmd.User, &Disconnect{Code: cmd.Code, Reason: cmd.Reason, Reconnect: cmd.Reconnect}, cmd.Client, cmd.Whitelist)
	case controlpb.Command_SURVEY_REQUEST:
		cmd, err := n.controlDecoder.DecodeSurveyRequest(params)
		if err != nil {
			n.logger.log(newLogEntry(LogLevelError, "error decoding survey request control params", map[string]interface{}{"error": err.Error()}))
			return err
		}
		return n.handleSurveyRequest(uid, cmd)
	case controlpb.Command_SURVEY_RESPONSE:
		cmd, err := n.controlDecoder.DecodeSurveyResponse(params)
		if err != nil {
			n.logger.log(newLogEntry(LogLevelError, "error decoding survey response control params", map[string]interface{}{"error": err.Error()}))
			return err
		}
		return n.handleSurveyResponse(uid, cmd)
	case controlpb.Command_NOTIFICATION:
		cmd, err := n.controlDecoder.DecodeNotification(params)
		if err != nil {
			n.logger.log(newLogEntry(LogLevelError, "error decoding notification control params", map[string]interface{}{"error": err.Error()}))
			return err
		}
		return n.handleNotification(uid, cmd)
	case controlpb.Command_REFRESH:
		cmd, err := n.controlDecoder.DecodeRefresh(params)
		if err != nil {
			n.logger.log(newLogEntry(LogLevelError, "error decoding refresh control params", map[string]interface{}{"error": err.Error()}))
			return err
		}
		return n.hub.refresh(cmd.User, cmd.Client, WithRefreshExpired(cmd.Expired), WithRefreshExpireAt(cmd.ExpireAt), WithRefreshInfo(cmd.Info))
	default:
		n.logger.log(newLogEntry(LogLevelError, "unknown control message method", map[string]interface{}{"method": method}))
		return fmt.Errorf("control method not found: %d", method)
	}
}

// handlePublication handles messages published into channel and
// coming from Broker. The goal of method is to deliver this message
// to all clients on this node currently subscribed to channel.
func (n *Node) handlePublication(ch string, pub *Publication, sp StreamPosition) error {
	incMessagesReceived("publication")
	numSubscribers := n.hub.NumSubscribers(ch)
	hasCurrentSubscribers := numSubscribers > 0
	if !hasCurrentSubscribers {
		return nil
	}
	return n.hub.BroadcastPublication(ch, pub, sp)
}

// handleJoin handles join messages - i.e. broadcasts it to
// interested local clients subscribed to channel.
func (n *Node) handleJoin(ch string, info *ClientInfo) error {
	incMessagesReceived("join")
	hasCurrentSubscribers := n.hub.NumSubscribers(ch) > 0
	if !hasCurrentSubscribers {
		return nil
	}
	return n.hub.broadcastJoin(ch, info)
}

// handleLeave handles leave messages - i.e. broadcasts it to
// interested local clients subscribed to channel.
func (n *Node) handleLeave(ch string, info *ClientInfo) error {
	incMessagesReceived("leave")
	hasCurrentSubscribers := n.hub.NumSubscribers(ch) > 0
	if !hasCurrentSubscribers {
		return nil
	}
	return n.hub.broadcastLeave(ch, info)
}

func (n *Node) publish(ch string, data []byte, opts ...PublishOption) (PublishResult, error) {
	pubOpts := &PublishOptions{}
	for _, opt := range opts {
		opt(pubOpts)
	}
	incMessagesSent("publication")
	streamPos, err := n.broker.Publish(ch, data, *pubOpts)
	if err != nil {
		return PublishResult{}, err
	}
	return PublishResult{StreamPosition: streamPos}, nil
}

// PublishResult returned from Publish operation.
type PublishResult struct {
	StreamPosition
}

// Publish sends data to all clients subscribed on channel at this moment. All running
// nodes will receive Publication and send it to all local channel subscribers.
//
// Data expected to be valid marshaled JSON or any binary payload.
// Connections that work over JSON protocol can not handle binary payloads.
// Connections that work over Protobuf protocol can work both with JSON and binary payloads.
//
// So the rule here: if you have channel subscribers that work using JSON
// protocol then you can not publish binary data to these channel.
//
// Channels in Centrifuge are ephemeral and its settings not persisted over different
// publish operations. So if you want to have channel with history stream behind you
// need to provide WithHistory option on every publish. To simplify working with different
// channels you can make some type of publish wrapper in your own code.
//
// The returned PublishResult contains embedded StreamPosition that describes
// position inside stream Publication was added too. For channels without history
// enabled (i.e. when Publications only sent to PUB/SUB system) StreamPosition will
// be an empty struct (i.e. PublishResult.Offset will be zero).
func (n *Node) Publish(channel string, data []byte, opts ...PublishOption) (PublishResult, error) {
	return n.publish(channel, data, opts...)
}

// publishJoin allows to publish join message into channel when someone subscribes on it
// or leave message when someone unsubscribes from channel.
func (n *Node) publishJoin(ch string, info *ClientInfo) error {
	incMessagesSent("join")
	return n.broker.PublishJoin(ch, info)
}

// publishLeave allows to publish join message into channel when someone subscribes on it
// or leave message when someone unsubscribes from channel.
func (n *Node) publishLeave(ch string, info *ClientInfo) error {
	incMessagesSent("leave")
	return n.broker.PublishLeave(ch, info)
}

var errNotificationHandlerNotRegistered = errors.New("notification handler not registered")

// Notify allows sending an asynchronous notification to all other nodes
// (or to a single specific node). Unlike Survey it does not wait for any
// response. If toNodeID is not an empty string then a notification will
// be sent to a concrete node in cluster, otherwise a notification sent to
// all running nodes. See a corresponding Node.OnNotification method to
// handle received notifications.
func (n *Node) Notify(op string, data []byte, toNodeID string) error {
	if n.notificationHandler == nil {
		return errNotificationHandlerNotRegistered
	}

	incActionCount("notify")

	if toNodeID == "" || n.ID() == toNodeID {
		// Invoke handler on this node since control message handler
		// ignores those sent from the current Node.
		n.notificationHandler(NotificationEvent{
			FromNodeID: n.ID(),
			Op:         op,
			Data:       data,
		})
	}
	if n.ID() == toNodeID {
		// Already on this node and called notificationHandler above, no
		// need to send notification over network.
		return nil
	}
	notification := &controlpb.Notification{
		Op:   op,
		Data: data,
	}
	params, err := n.controlEncoder.EncodeNotification(notification)
	if err != nil {
		return err
	}
	cmd := &controlpb.Command{
		Uid:    n.uid,
		Method: controlpb.Command_NOTIFICATION,
		Params: params,
	}
	return n.publishControl(cmd, toNodeID)
}

// publishControl publishes message into control channel so all running
// nodes will receive and handle it.
func (n *Node) publishControl(cmd *controlpb.Command, nodeID string) error {
	incMessagesSent("control")
	data, err := n.controlEncoder.EncodeCommand(cmd)
	if err != nil {
		return err
	}
	return n.broker.PublishControl(data, nodeID, "")
}

func (n *Node) getMetrics(metrics eagle.Metrics) *controlpb.Metrics {
	return &controlpb.Metrics{
		Interval: n.config.NodeInfoMetricsAggregateInterval.Seconds(),
		Items:    metrics.Flatten("."),
	}
}

// pubNode sends control message to all nodes - this message
// contains information about current node.
func (n *Node) pubNode(nodeID string) error {
	var data []byte
	if n.nodeInfoSendHandler != nil {
		reply := n.nodeInfoSendHandler()
		data = reply.Data
	}
	n.mu.RLock()
	node := &controlpb.Node{
		Uid:         n.uid,
		Name:        n.config.Name,
		Version:     n.config.Version,
		NumClients:  uint32(n.hub.NumClients()),
		NumUsers:    uint32(n.hub.NumUsers()),
		NumChannels: uint32(n.hub.NumChannels()),
		NumSubs:     uint32(n.hub.NumSubscriptions()),
		Uptime:      uint32(time.Now().Unix() - n.startedAt),
		Data:        data,
	}

	n.metricsMu.Lock()
	if n.metricsSnapshot != nil {
		node.Metrics = n.getMetrics(*n.metricsSnapshot)
	}
	// We only send metrics once when updated.
	n.metricsSnapshot = nil
	n.metricsMu.Unlock()

	n.mu.RUnlock()

	params, _ := n.controlEncoder.EncodeNode(node)

	cmd := &controlpb.Command{
		Uid:    n.uid,
		Method: controlpb.Command_NODE,
		Params: params,
	}

	err := n.nodeCmd(node)
	if err != nil {
		n.logger.log(newLogEntry(LogLevelError, "error handling node command", map[string]interface{}{"error": err.Error()}))
	}

	return n.publishControl(cmd, nodeID)
}

func (n *Node) pubSubscribe(user string, ch string, opts SubscribeOptions) error {
	subscribe := &controlpb.Subscribe{
		User:        user,
		Channel:     ch,
		Presence:    opts.Presence,
		JoinLeave:   opts.JoinLeave,
		ChannelInfo: opts.ChannelInfo,
		Position:    opts.Position,
		Recover:     opts.Recover,
		ExpireAt:    opts.ExpireAt,
		Client:      opts.clientID,
		Data:        opts.Data,
	}
	if opts.RecoverSince != nil {
		subscribe.RecoverSince = &controlpb.StreamPosition{
			Offset: opts.RecoverSince.Offset,
			Epoch:  opts.RecoverSince.Epoch,
		}
	}
	params, _ := n.controlEncoder.EncodeSubscribe(subscribe)
	cmd := &controlpb.Command{
		Uid:    n.uid,
		Method: controlpb.Command_SUBSCRIBE,
		Params: params,
	}
	return n.publishControl(cmd, "")
}

func (n *Node) pubRefresh(user string, opts RefreshOptions) error {
	refresh := &controlpb.Refresh{
		User:     user,
		Expired:  opts.Expired,
		ExpireAt: opts.ExpireAt,
		Client:   opts.clientID,
		Info:     opts.Info,
	}
	params, _ := n.controlEncoder.EncodeRefresh(refresh)
	cmd := &controlpb.Command{
		Uid:    n.uid,
		Method: controlpb.Command_REFRESH,
		Params: params,
	}
	return n.publishControl(cmd, "")
}

// pubUnsubscribe publishes unsubscribe control message to all nodes – so all
// nodes could unsubscribe user from channel.
func (n *Node) pubUnsubscribe(user string, ch string, opts UnsubscribeOptions) error {
	unsubscribe := &controlpb.Unsubscribe{
		User:    user,
		Channel: ch,
		Client:  opts.clientID,
	}
	params, _ := n.controlEncoder.EncodeUnsubscribe(unsubscribe)
	cmd := &controlpb.Command{
		Uid:    n.uid,
		Method: controlpb.Command_UNSUBSCRIBE,
		Params: params,
	}
	return n.publishControl(cmd, "")
}

// pubDisconnect publishes disconnect control message to all nodes – so all
// nodes could disconnect user from server.
func (n *Node) pubDisconnect(user string, disconnect *Disconnect, clientID string, whitelist []string) error {
	protoDisconnect := &controlpb.Disconnect{
		User:      user,
		Whitelist: whitelist,
		Code:      disconnect.Code,
		Reason:    disconnect.Reason,
		Reconnect: disconnect.Reconnect,
		Client:    clientID,
	}
	params, _ := n.controlEncoder.EncodeDisconnect(protoDisconnect)
	cmd := &controlpb.Command{
		Uid:    n.uid,
		Method: controlpb.Command_DISCONNECT,
		Params: params,
	}
	return n.publishControl(cmd, "")
}

// addClient registers authenticated connection in clientConnectionHub
// this allows to make operations with user connection on demand.
func (n *Node) addClient(c *Client) error {
	incActionCount("add_client")
	return n.hub.add(c)
}

// removeClient removes client connection from connection registry.
func (n *Node) removeClient(c *Client) error {
	incActionCount("remove_client")
	return n.hub.remove(c)
}

// addSubscription registers subscription of connection on channel in both
// Hub and Broker.
func (n *Node) addSubscription(ch string, c *Client) error {
	incActionCount("add_subscription")
	mu := n.subLock(ch)
	mu.Lock()
	defer mu.Unlock()
	first, err := n.hub.addSub(ch, c)
	if err != nil {
		return err
	}
	if first {
		err := n.broker.Subscribe(ch)
		if err != nil {
			_, _ = n.hub.removeSub(ch, c)
			return err
		}
	}
	return nil
}

// removeSubscription removes subscription of connection on channel
// from Hub and Broker.
func (n *Node) removeSubscription(ch string, c *Client) error {
	incActionCount("remove_subscription")
	mu := n.subLock(ch)
	mu.Lock()
	defer mu.Unlock()
	empty, err := n.hub.removeSub(ch, c)
	if err != nil {
		return err
	}
	if empty {
		submittedAt := time.Now()
		_ = n.subDissolver.Submit(func() error {
			timeSpent := time.Since(submittedAt)
			if timeSpent < time.Second {
				time.Sleep(time.Second - timeSpent)
			}
			mu := n.subLock(ch)
			mu.Lock()
			defer mu.Unlock()
			empty := n.hub.NumSubscribers(ch) == 0
			if empty {
				return n.broker.Unsubscribe(ch)
			}
			return nil
		})
	}
	return nil
}

// nodeCmd handles node control command i.e. updates information about known nodes.
func (n *Node) nodeCmd(node *controlpb.Node) error {
	isNewNode := n.nodes.add(node)
	if isNewNode && node.Uid != n.uid {
		// New Node in cluster
		_ = n.pubNode(node.Uid)
	}
	return nil
}

// shutdownCmd handles shutdown control command sent when node leaves cluster.
func (n *Node) shutdownCmd(nodeID string) error {
	n.nodes.remove(nodeID)
	return nil
}

// Subscribe subscribes user to a channel.
// Note, that OnSubscribe event won't be called in this case
// since this is a server-side subscription. If user have been already
// subscribed to a channel then its subscription will be updated and
// subscribe notification will be sent to a client-side.
func (n *Node) Subscribe(userID string, channel string, opts ...SubscribeOption) error {
	subscribeOpts := &SubscribeOptions{}
	for _, opt := range opts {
		opt(subscribeOpts)
	}
	// Subscribe on this node.
	err := n.hub.subscribe(userID, channel, subscribeOpts.clientID, opts...)
	if err != nil {
		return err
	}
	// Send subscribe control message to other nodes.
	return n.pubSubscribe(userID, channel, *subscribeOpts)
}

// Unsubscribe unsubscribes user from a channel.
// If a channel is empty string then user will be unsubscribed from all channels.
func (n *Node) Unsubscribe(userID string, channel string, opts ...UnsubscribeOption) error {
	unsubscribeOpts := &UnsubscribeOptions{}
	for _, opt := range opts {
		opt(unsubscribeOpts)
	}
	// Unsubscribe on this node.
	err := n.hub.unsubscribe(userID, channel, unsubscribeOpts.clientID)
	if err != nil {
		return err
	}
	// Send unsubscribe control message to other nodes.
	return n.pubUnsubscribe(userID, channel, *unsubscribeOpts)
}

// Disconnect allows closing all user connections on all nodes.
func (n *Node) Disconnect(userID string, opts ...DisconnectOption) error {
	disconnectOpts := &DisconnectOptions{}
	for _, opt := range opts {
		opt(disconnectOpts)
	}
	// Disconnect user from this node
	customDisconnect := disconnectOpts.Disconnect
	if customDisconnect == nil {
		customDisconnect = DisconnectForceNoReconnect
	}
	err := n.hub.disconnect(userID, customDisconnect, disconnectOpts.clientID, disconnectOpts.ClientWhitelist)
	if err != nil {
		return err
	}
	// Send disconnect control message to other nodes
	return n.pubDisconnect(userID, customDisconnect, disconnectOpts.clientID, disconnectOpts.ClientWhitelist)
}

// Refresh user connection.
// Without any options will make user connections non-expiring.
// Note, that OnRefresh event won't be called in this case
// since this is a server-side refresh.
func (n *Node) Refresh(userID string, opts ...RefreshOption) error {
	refreshOpts := &RefreshOptions{}
	for _, opt := range opts {
		opt(refreshOpts)
	}
	// Refresh on this node.
	err := n.hub.refresh(userID, refreshOpts.clientID, opts...)
	if err != nil {
		return err
	}
	// Send refresh control message to other nodes.
	return n.pubRefresh(userID, *refreshOpts)
}

// addPresence proxies presence adding to PresenceManager.
func (n *Node) addPresence(ch string, uid string, info *ClientInfo) error {
	if n.presenceManager == nil {
		return nil
	}
	incActionCount("add_presence")
	return n.presenceManager.AddPresence(ch, uid, info)
}

// removePresence proxies presence removing to PresenceManager.
func (n *Node) removePresence(ch string, uid string) error {
	if n.presenceManager == nil {
		return nil
	}
	incActionCount("remove_presence")
	return n.presenceManager.RemovePresence(ch, uid)
}

var (
	presenceGroup      singleflight.Group
	presenceStatsGroup singleflight.Group
	historyGroup       singleflight.Group
)

// PresenceResult wraps presence.
type PresenceResult struct {
	Presence map[string]*ClientInfo
}

func (n *Node) presence(ch string) (PresenceResult, error) {
	presence, err := n.presenceManager.Presence(ch)
	if err != nil {
		return PresenceResult{}, err
	}
	return PresenceResult{Presence: presence}, nil
}

// Presence returns a map with information about active clients in channel.
func (n *Node) Presence(ch string) (PresenceResult, error) {
	if n.presenceManager == nil {
		return PresenceResult{}, ErrorNotAvailable
	}
	incActionCount("presence")
	if n.config.UseSingleFlight {
		result, err, _ := presenceGroup.Do(ch, func() (interface{}, error) {
			return n.presence(ch)
		})
		return result.(PresenceResult), err
	}
	return n.presence(ch)
}

func infoFromProto(v *protocol.ClientInfo) *ClientInfo {
	if v == nil {
		return nil
	}
	info := &ClientInfo{
		ClientID: v.GetClient(),
		UserID:   v.GetUser(),
	}
	if len(v.ConnInfo) > 0 {
		info.ConnInfo = v.ConnInfo
	}
	if len(v.ChanInfo) > 0 {
		info.ChanInfo = v.ChanInfo
	}
	return info
}

func infoToProto(v *ClientInfo) *protocol.ClientInfo {
	if v == nil {
		return nil
	}
	info := &protocol.ClientInfo{
		Client: v.ClientID,
		User:   v.UserID,
	}
	if len(v.ConnInfo) > 0 {
		info.ConnInfo = v.ConnInfo
	}
	if len(v.ChanInfo) > 0 {
		info.ChanInfo = v.ChanInfo
	}
	return info
}

func pubToProto(pub *Publication) *protocol.Publication {
	if pub == nil {
		return nil
	}
	return &protocol.Publication{
		Offset: pub.Offset,
		Data:   pub.Data,
		Info:   infoToProto(pub.Info),
	}
}

func pubFromProto(pub *protocol.Publication) *Publication {
	if pub == nil {
		return nil
	}
	return &Publication{
		Offset: pub.GetOffset(),
		Data:   pub.Data,
		Info:   infoFromProto(pub.GetInfo()),
	}
}

// PresenceStatsResult wraps presence stats.
type PresenceStatsResult struct {
	PresenceStats
}

func (n *Node) presenceStats(ch string) (PresenceStatsResult, error) {
	presenceStats, err := n.presenceManager.PresenceStats(ch)
	if err != nil {
		return PresenceStatsResult{}, err
	}
	return PresenceStatsResult{PresenceStats: presenceStats}, nil
}

// PresenceStats returns presence stats from PresenceManager.
func (n *Node) PresenceStats(ch string) (PresenceStatsResult, error) {
	if n.presenceManager == nil {
		return PresenceStatsResult{}, ErrorNotAvailable
	}
	incActionCount("presence_stats")
	if n.config.UseSingleFlight {
		result, err, _ := presenceStatsGroup.Do(ch, func() (interface{}, error) {
			return n.presenceStats(ch)
		})
		return result.(PresenceStatsResult), err
	}
	return n.presenceStats(ch)
}

// HistoryResult contains Publications and current stream top StreamPosition.
type HistoryResult struct {
	// StreamPosition embedded here describes current stream top offset and epoch.
	StreamPosition
	// Publications extracted from history storage according to HistoryFilter.
	Publications []*Publication
}

func (n *Node) history(ch string, opts *HistoryOptions) (HistoryResult, error) {
	if opts.Reverse && opts.Since != nil && opts.Since.Offset == 0 {
		return HistoryResult{}, ErrorBadRequest
	}
	pubs, streamTop, err := n.broker.History(ch, HistoryFilter{
		Limit:   opts.Limit,
		Since:   opts.Since,
		Reverse: opts.Reverse,
	})
	if err != nil {
		return HistoryResult{}, err
	}
	if opts.Since != nil {
		sinceEpoch := opts.Since.Epoch
		epochOK := sinceEpoch == "" || sinceEpoch == streamTop.Epoch
		if !epochOK {
			return HistoryResult{
				StreamPosition: streamTop,
				Publications:   pubs,
			}, ErrorUnrecoverablePosition
		}
	}
	return HistoryResult{
		StreamPosition: streamTop,
		Publications:   pubs,
	}, nil
}

// History allows to extract Publications in channel.
// The channel must belong to namespace where history is on.
func (n *Node) History(ch string, opts ...HistoryOption) (HistoryResult, error) {
	incActionCount("history")
	historyOpts := &HistoryOptions{}
	for _, opt := range opts {
		opt(historyOpts)
	}
	if n.config.UseSingleFlight {
		var builder strings.Builder
		builder.WriteString("channel:")
		builder.WriteString(ch)
		if historyOpts.Since != nil {
			builder.WriteString(",offset:")
			builder.WriteString(strconv.FormatUint(historyOpts.Since.Offset, 10))
			builder.WriteString(",epoch:")
			builder.WriteString(historyOpts.Since.Epoch)
		}
		builder.WriteString(",limit:")
		builder.WriteString(strconv.Itoa(historyOpts.Limit))
		builder.WriteString(",reverse:")
		builder.WriteString(strconv.FormatBool(historyOpts.Reverse))
		key := builder.String()

		result, err, _ := historyGroup.Do(key, func() (interface{}, error) {
			return n.history(ch, historyOpts)
		})
		return result.(HistoryResult), err
	}
	return n.history(ch, historyOpts)
}

// recoverHistory recovers publications since StreamPosition last seen by client.
func (n *Node) recoverHistory(ch string, since StreamPosition) (HistoryResult, error) {
	incActionCount("history_recover")
	limit := NoLimit
	maxPublicationLimit := n.config.RecoveryMaxPublicationLimit
	if maxPublicationLimit > 0 {
		limit = maxPublicationLimit
	}
	return n.History(ch, WithLimit(limit), WithSince(&since))
}

// streamTop returns current stream top StreamPosition for a channel.
func (n *Node) streamTop(ch string) (StreamPosition, error) {
	incActionCount("history_stream_top")
	historyResult, err := n.History(ch)
	if err != nil {
		return StreamPosition{}, err
	}
	return historyResult.StreamPosition, nil
}

// RemoveHistory removes channel history.
func (n *Node) RemoveHistory(ch string) error {
	incActionCount("history_remove")
	return n.broker.RemoveHistory(ch)
}

type nodeRegistry struct {
	// mu allows to synchronize access to node registry.
	mu sync.RWMutex
	// currentUID keeps uid of current node
	currentUID string
	// nodes is a map with information about known nodes.
	nodes map[string]*controlpb.Node
	// updates track time we last received ping from node. Used to clean up nodes map.
	updates map[string]int64
}

func newNodeRegistry(currentUID string) *nodeRegistry {
	return &nodeRegistry{
		currentUID: currentUID,
		nodes:      make(map[string]*controlpb.Node),
		updates:    make(map[string]int64),
	}
}

func (r *nodeRegistry) list() []*controlpb.Node {
	r.mu.RLock()
	nodes := make([]*controlpb.Node, len(r.nodes))
	i := 0
	for _, info := range r.nodes {
		nodes[i] = info
		i++
	}
	r.mu.RUnlock()
	return nodes
}

func (r *nodeRegistry) get(uid string) *controlpb.Node {
	r.mu.RLock()
	info := r.nodes[uid]
	r.mu.RUnlock()
	return info
}

func (r *nodeRegistry) add(info *controlpb.Node) bool {
	var isNewNode bool
	r.mu.Lock()
	if node, ok := r.nodes[info.Uid]; ok {
		if info.Metrics != nil {
			r.nodes[info.Uid] = info
		} else {
			node.Version = info.Version
			node.NumChannels = info.NumChannels
			node.NumClients = info.NumClients
			node.NumUsers = info.NumUsers
			node.NumSubs = info.NumSubs
			node.Uptime = info.Uptime
			node.Data = info.Data
			r.nodes[info.Uid] = node
		}
	} else {
		r.nodes[info.Uid] = info
		isNewNode = true
	}
	r.updates[info.Uid] = time.Now().Unix()
	r.mu.Unlock()
	return isNewNode
}

func (r *nodeRegistry) remove(uid string) {
	r.mu.Lock()
	delete(r.nodes, uid)
	delete(r.updates, uid)
	r.mu.Unlock()
}

func (r *nodeRegistry) clean(delay time.Duration) {
	r.mu.Lock()
	for uid := range r.nodes {
		if uid == r.currentUID {
			// No need to clean info for current node.
			continue
		}
		updated, ok := r.updates[uid]
		if !ok {
			// As we do all operations with nodes under lock this should never happen.
			delete(r.nodes, uid)
			continue
		}
		if time.Now().Unix()-updated > int64(delay.Seconds()) {
			// Too many seconds since this node have been last seen - remove it from map.
			delete(r.nodes, uid)
			delete(r.updates, uid)
		}
	}
	r.mu.Unlock()
}

// OnSurvey allows setting SurveyHandler. This should be done before Node.Run called.
func (n *Node) OnSurvey(handler SurveyHandler) {
	n.surveyHandler = handler
}

// OnNotification allows setting NotificationHandler. This should be done before Node.Run called.
func (n *Node) OnNotification(handler NotificationHandler) {
	n.notificationHandler = handler
}

// OnTransportWrite allows setting TransportWriteHandler. This should be done before Node.Run called.
func (n *Node) OnTransportWrite(handler TransportWriteHandler) {
	n.transportWriteHandler = handler
}

// OnNodeInfoSend allows setting NodeInfoSendHandler. This should be done before Node.Run called.
func (n *Node) OnNodeInfoSend(handler NodeInfoSendHandler) {
	n.nodeInfoSendHandler = handler
}

// eventHub allows binding client event handlers.
// All eventHub methods are not goroutine-safe and supposed
// to be called once before Node Run called.
type eventHub struct {
	connectingHandler ConnectingHandler
	connectHandler    ConnectHandler
}

// OnConnecting allows setting ConnectingHandler.
// ConnectingHandler will be called when client sends Connect command to server.
// In this handler server can reject connection or provide Credentials for it.
func (n *Node) OnConnecting(handler ConnectingHandler) {
	n.clientEvents.connectingHandler = handler
}

// OnConnect allows setting ConnectHandler.
// ConnectHandler called after client connection successfully established,
// authenticated and Connect Reply already sent to client. This is a place where
// application can start communicating with client.
func (n *Node) OnConnect(handler ConnectHandler) {
	n.clientEvents.connectHandler = handler
}

type brokerEventHandler struct {
	node *Node
}

// HandlePublication coming from Broker.
func (h *brokerEventHandler) HandlePublication(ch string, pub *Publication, sp StreamPosition) error {
	if pub == nil {
		panic("nil Publication received, this must never happen")
	}
	return h.node.handlePublication(ch, pub, sp)
}

// HandleJoin coming from Broker.
func (h *brokerEventHandler) HandleJoin(ch string, info *ClientInfo) error {
	if info == nil {
		panic("nil join ClientInfo received, this must never happen")
	}
	return h.node.handleJoin(ch, info)
}

// HandleLeave coming from Broker.
func (h *brokerEventHandler) HandleLeave(ch string, info *ClientInfo) error {
	if info == nil {
		panic("nil leave ClientInfo received, this must never happen")
	}
	return h.node.handleLeave(ch, info)
}

// HandleControl coming from Broker.
func (h *brokerEventHandler) HandleControl(data []byte) error {
	return h.node.handleControl(data)
}
