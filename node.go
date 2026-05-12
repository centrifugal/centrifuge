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
	"github.com/centrifugal/centrifuge/internal/filter"
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
// By default, Node uses in-memory implementations of Broker and PresenceManager -
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
	// controller is responsible for inter-node communication.
	controller Controller
	// broker is responsible for PUB/SUB and history streaming mechanics.
	broker Broker
	// mapBroker is responsible for map subscriptions.
	mapBroker MapBroker
	// presenceManager is responsible for presence information management.
	presenceManager PresenceManager
	// nodes contains registry of known nodes.
	nodes *nodeRegistry
	// metrics registry.
	metrics *metrics
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

	notificationHandler NotificationHandler
	nodeInfoSendHandler NodeInfoSendHandler

	emulationSurveyHandler *emulationSurveyHandler

	mediums     map[string]*channelMedium
	mediumLocks map[int]*sync.Mutex // Sharded locks for mediums map.

	timerScheduler TimerScheduler

	// keyedManager manages keyed channel state (track/untrack, reverse index).
	keyedManager *keyedManager
	// sharedPollManager manages shared poll refresh workers.
	sharedPollManager *SharedPollManager
}

const (
	numSubLocks            = 16384
	numMediumLocks         = 16384
	numSubDissolverWorkers = 64
)

// TransportAcceptedLabels contains labels for transport connection metrics.
// This struct is designed to be extensible - additional label fields can be
// added in the future without breaking compatibility.
type TransportAcceptedLabels struct {
	// Transport is the transport type (e.g., "websocket", "http_stream", "sse").
	Transport string
	// AcceptProtocol is the transport protocol used to accept connection (can be "h1", "h2", "h3").
	AcceptProtocol string
}

// New creates Node with provided Config.
func New(c Config) (*Node, error) {
	if c.NodeInfoMetricsAggregateInterval == 0 {
		c.NodeInfoMetricsAggregateInterval = 60 * time.Second
	}
	if c.ClientPresenceUpdateInterval == 0 {
		c.ClientPresenceUpdateInterval = 25 * time.Second
	}
	if c.ClientChannelPositionCheckDelay == 0 {
		c.ClientChannelPositionCheckDelay = 40 * time.Second
	}
	if c.ClientExpiredCloseDelay == 0 {
		c.ClientExpiredCloseDelay = 25 * time.Second
	}
	if c.ClientExpiredSubCloseDelay == 0 {
		c.ClientExpiredSubCloseDelay = 25 * time.Second
	}
	if c.ClientStaleCloseDelay == 0 {
		c.ClientStaleCloseDelay = 15 * time.Second
	}
	if c.ClientQueueMaxSize == 0 {
		c.ClientQueueMaxSize = 1048576 // 1MB by default.
	}
	if c.ClientChannelLimit == 0 {
		c.ClientChannelLimit = 128
	}
	if c.ChannelMaxLength == 0 {
		c.ChannelMaxLength = 255
	}
	if c.HistoryMetaTTL == 0 {
		c.HistoryMetaTTL = 30 * 24 * time.Hour // 30 days by default.
	}

	uidObj, err := uuid.NewRandom()
	if err != nil {
		return nil, err
	}
	uid := uidObj.String()

	subLocks := make(map[int]*sync.Mutex, numSubLocks)
	for i := 0; i < numSubLocks; i++ {
		subLocks[i] = &sync.Mutex{}
	}

	mediumLocks := make(map[int]*sync.Mutex, numMediumLocks)
	for i := 0; i < numMediumLocks; i++ {
		mediumLocks[i] = &sync.Mutex{}
	}

	if c.Name == "" {
		hostname, err := os.Hostname()
		if err != nil {
			return nil, err
		}
		c.Name = hostname
	}

	var lg *logger
	if c.LogHandler != nil {
		lg = newLogger(c.LogLevel, c.LogHandler)
	}

	n := &Node{
		uid:            uid,
		nodes:          newNodeRegistry(uid),
		config:         c,
		startedAt:      time.Now().Unix(),
		shutdownCh:     make(chan struct{}),
		logger:         lg,
		controlEncoder: controlproto.NewProtobufEncoder(),
		controlDecoder: controlproto.NewProtobufDecoder(),
		clientEvents:   &eventHub{},
		subLocks:       subLocks,
		subDissolver:   dissolve.New(numSubDissolverWorkers),
		nowTimeGetter:  nowtime.Get,
		surveyRegistry: make(map[uint64]chan survey),
		mediums:        map[string]*channelMedium{},
		mediumLocks:    mediumLocks,
		timerScheduler: c.ClientTimerScheduler,
	}
	n.emulationSurveyHandler = newEmulationSurveyHandler(n)
	n.keyedManager = newKeyedManager(n)

	m, err := newMetricsRegistry(c.Metrics)
	if err != nil {
		return nil, fmt.Errorf("error initializing metrics: %v", err)
	}
	n.metrics = m

	n.hub = newHub(lg, n.metrics, c.ClientChannelPositionMaxTimeLag.Milliseconds())

	b, err := NewMemoryBroker(n, MemoryBrokerConfig{})
	if err != nil {
		return nil, err
	}
	n.SetBroker(b)

	mb, err := NewMemoryMapBroker(n, MemoryMapBrokerConfig{})
	if err != nil {
		return nil, err
	}
	n.SetMapBroker(mb)

	pm, err := NewMemoryPresenceManager(n, MemoryPresenceManagerConfig{})
	if err != nil {
		return nil, err
	}
	n.SetPresenceManager(pm)

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

// Config returns Node's Config.
func (n *Node) Config() Config {
	return n.config
}

// ID returns unique Node identifier. This is a UUID v4 value.
func (n *Node) ID() string {
	return n.uid
}

func (n *Node) subLock(ch string) *sync.Mutex {
	return n.subLocks[index(ch, numSubLocks)]
}

func (n *Node) mediumLock(ch string) *sync.Mutex {
	return n.mediumLocks[index(ch, numMediumLocks)]
}

// SetController allows setting Controller implementation to use.
func (n *Node) SetController(c Controller) {
	n.controller = c
}

// SetBroker allows setting Broker implementation to use.
// For historical reasons and to keep existing API, we also check if Broker implements Controller
// and if so we set it as Node's Controller (but only if Controller not explicitly set).
func (n *Node) SetBroker(b Broker) {
	n.broker = b
	if n.controller == nil {
		if c, ok := b.(Controller); ok {
			n.controller = c
		}
	}
}

// SetPresenceManager allows setting PresenceManager to use.
func (n *Node) SetPresenceManager(m PresenceManager) {
	n.presenceManager = m
}

// SetMapBroker allows setting MapBroker to use.
func (n *Node) SetMapBroker(e MapBroker) {
	n.mapBroker = e
}

// resolveMapChannelOptions returns validated channel options for a map channel.
// Returns an error if GetMapChannelOptions is not configured or the channel
// options are invalid.
func (n *Node) resolveMapChannelOptions(channel string) (MapChannelOptions, error) {
	return ResolveAndValidateMapChannelOptions(n.config.Map.GetMapChannelOptions, channel)
}

// Hub returns node's Hub.
func (n *Node) Hub() *Hub {
	return n.hub
}

// Run performs node startup actions. At moment must be called once on start
// after Controller and Broker set to Node.
func (n *Node) Run() error {
	if n.controller != nil {
		if err := n.controller.RegisterControlEventHandler(n); err != nil {
			return err
		}
	}
	if err := n.broker.RegisterBrokerEventHandler(n); err != nil {
		return err
	}
	if n.mapBroker != nil {
		if err := n.mapBroker.RegisterEventHandler(n); err != nil {
			return err
		}
	}
	err := n.initMetrics()
	if err != nil {
		n.logger.log(newErrorLogEntry(err, "error on init metrics", map[string]any{"error": err.Error()}))
		return err
	}
	err = n.pubNode("")
	if err != nil {
		n.logger.log(newErrorLogEntry(err, "error publishing node control command", map[string]any{"error": err.Error()}))
		return err
	}
	// Initialize shared poll manager if configured.
	if n.config.SharedPoll.GetSharedPollChannelOptions != nil {
		if n.clientEvents.sharedPollHandler == nil {
			return errors.New("GetSharedPollChannelOptions is set but OnSharedPoll handler is not registered")
		}
		n.sharedPollManager = newSharedPollManager(n)
	}

	go n.sendNodePing()
	go n.cleanNodeInfo()
	go n.updateMetrics()
	return n.subDissolver.Run()
}

// logEnabled allows check whether a LogLevel enabled or not.
func (n *Node) logEnabled(level LogLevel) bool {
	return n.logger.enabled(level)
}

// IncMapBrokerCleanupErrors increments the map broker cleanup error counter for observability.
func (n *Node) IncMapBrokerCleanupErrors(name string) {
	if n.metrics != nil {
		n.metrics.incMapBrokerCleanupErrors(name)
	}
}

// AddMapBrokerCleanupKeysRemoved adds to the map broker cleanup keys removed counter for observability.
func (n *Node) AddMapBrokerCleanupKeysRemoved(name string, count int64) {
	if n.metrics != nil {
		n.metrics.addMapBrokerCleanupKeysRemoved(name, count)
	}
}

// SetMapBrokerCleanupLag sets the map broker cleanup lag gauge for observability.
func (n *Node) SetMapBrokerCleanupLag(name string, seconds float64) {
	if n.metrics != nil {
		n.metrics.setMapBrokerCleanupLag(name, seconds)
	}
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
		Uid:      n.uid,
		Shutdown: &controlpb.Shutdown{},
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
	if n.mapBroker != nil {
		if closer, ok := n.mapBroker.(Closer); ok {
			defer func() { _ = closer.Close(ctx) }()
		}
	}
	// Stop shared poll workers before hub shutdown.
	if n.sharedPollManager != nil {
		n.sharedPollManager.close()
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
	n.metrics.setNumClients(float64(n.hub.NumClients()))
	n.metrics.setNumUsers(float64(n.hub.NumUsers()))
	n.metrics.setNumSubscriptions(float64(n.hub.NumSubscriptions()))
	n.metrics.setNumChannels(float64(n.hub.NumChannels()))
	n.metrics.setNumNodes(float64(n.nodes.size()))
	if n.sharedPollManager != nil {
		numCh, numKeys := n.sharedPollManager.stats()
		n.metrics.setSharedPollNumChannels(float64(numCh))
		n.metrics.setSharedPollNumKeys(float64(numKeys))
	}
	version := n.config.Version
	if version == "" {
		version = "_"
	}
	n.metrics.setBuildInfo(version)
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

	var gatherer prometheus.Gatherer
	if n.metrics.config.RegistererGatherer != nil {
		gatherer = n.metrics.config.RegistererGatherer
	} else {
		gatherer = prometheus.DefaultGatherer
	}

	metricsSink := make(chan eagle.Metrics)
	n.metricsExporter = eagle.New(eagle.Config{
		Gatherer:        gatherer,
		Interval:        n.config.NodeInfoMetricsAggregateInterval,
		Sink:            metricsSink,
		PrefixWhitelist: []string{getMetricsNamespace(n.config.Metrics)},
	})
	initialMetricsSnapshot, err := n.metricsExporter.Export()
	if err != nil {
		return err
	}
	n.metricsMu.Lock()
	n.metricsSnapshot = &initialMetricsSnapshot
	n.metricsMu.Unlock()
	go func() {
		for {
			select {
			case <-n.NotifyShutdown():
				return
			case metricsSnapshot := <-metricsSink:
				n.metricsMu.Lock()
				n.metricsSnapshot = &metricsSnapshot
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
				n.logger.log(newErrorLogEntry(err, "error publishing node control command", map[string]any{"error": err.Error()}))
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
			n.nodes.clean(nodeInfoMaxDelay)
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
	if n.surveyHandler == nil && n.emulationSurveyHandler == nil {
		return nil
	}
	cb := func(reply SurveyReply) {
		surveyResponse := &controlpb.SurveyResponse{
			Id:   req.Id,
			Code: reply.Code,
			Data: reply.Data,
		}
		cmd := &controlpb.Command{
			Uid:            n.uid,
			SurveyResponse: surveyResponse,
		}
		_ = n.publishControl(cmd, fromNodeID)
	}
	if req.Op == emulationOp && n.emulationSurveyHandler != nil {
		n.emulationSurveyHandler.HandleEmulation(SurveyEvent{Op: req.Op, Data: req.Data}, cb)
		return nil
	}
	if n.surveyHandler == nil {
		return nil
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
// If toNodeID is not an empty string then a survey will be sent only to the concrete node in
// a cluster, otherwise a survey sent to all running nodes. See a corresponding Node.OnSurvey
// method to handle received surveys.
// Survey ops starting with `centrifuge_` are reserved by Centrifuge library.
func (n *Node) Survey(ctx context.Context, op string, data []byte, toNodeID string) (map[string]SurveyResult, error) {
	if n.surveyHandler == nil && op != emulationOp {
		return nil, errSurveyHandlerNotRegistered
	}

	n.metrics.incActionCount("survey", "")
	started := time.Now()
	defer func() {
		n.metrics.observeSurveyDuration(op, time.Since(started))
	}()

	if _, ok := ctx.Deadline(); !ok {
		// If no timeout provided then fallback to defaultSurveyTimeout to avoid endless surveys.
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, defaultSurveyTimeout)
		defer cancel()
	}

	var numNodes int
	if toNodeID != "" {
		numNodes = 1
	} else {
		numNodes = n.nodes.size()
	}

	n.surveyMu.Lock()
	n.surveyID++
	surveyRequest := &controlpb.SurveyRequest{
		Id:   n.surveyID,
		Op:   op,
		Data: data,
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

	needDistributedPublish := true

	// Invoke handler on this node since control message handler
	// ignores those sent from the current Node.
	if toNodeID == "" || toNodeID == n.ID() {
		if toNodeID == n.ID() || (toNodeID == "" && numNodes == 1) {
			needDistributedPublish = false
		}
		if op == emulationOp {
			n.emulationSurveyHandler.HandleEmulation(SurveyEvent{Op: op, Data: data}, func(reply SurveyReply) {
				surveyChan <- survey{
					UID:    n.uid,
					Result: SurveyResult(reply),
				}
			})
		} else {
			n.surveyHandler(SurveyEvent{Op: op, Data: data}, func(reply SurveyReply) {
				surveyChan <- survey{
					UID:    n.uid,
					Result: SurveyResult(reply),
				}
			})
		}
	}

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

	if needDistributedPublish {
		cmd := &controlpb.Command{
			Uid:           n.uid,
			SurveyRequest: surveyRequest,
		}
		err := n.publishControl(cmd, toNodeID)
		if err != nil {
			return nil, err
		}
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
	n.metrics.incMessagesReceived("control", "")

	cmd, err := n.controlDecoder.DecodeCommand(data)
	if err != nil {
		n.logger.log(newErrorLogEntry(err, "error decoding control command", map[string]any{"error": err.Error()}))
		return err
	}

	if cmd.Uid == n.uid {
		// Sent by this node.
		return nil
	}

	uid := cmd.Uid

	// control proto v2.
	if cmd.Node != nil {
		return n.nodeCmd(cmd.Node)
	} else if cmd.Shutdown != nil {
		return n.shutdownCmd(uid)
	} else if cmd.Unsubscribe != nil {
		cmd := cmd.Unsubscribe
		return n.hub.unsubscribe(cmd.User, cmd.Channel, Unsubscribe{Code: cmd.Code, Reason: cmd.Reason}, cmd.Client, cmd.Session)
	} else if cmd.Subscribe != nil {
		cmd := cmd.Subscribe
		var recoverSince *StreamPosition
		if cmd.RecoverSince != nil {
			recoverSince = &StreamPosition{Offset: cmd.RecoverSince.Offset, Epoch: cmd.RecoverSince.Epoch}
		}
		return n.hub.subscribe(cmd.User, cmd.Channel, cmd.Client, cmd.Session, WithExpireAt(cmd.ExpireAt), WithChannelInfo(cmd.ChannelInfo), WithEmitPresence(cmd.EmitPresence), WithEmitJoinLeave(cmd.EmitJoinLeave), WithPushJoinLeave(cmd.PushJoinLeave), WithPositioning(cmd.Position), WithRecovery(cmd.Recover), WithSubscribeData(cmd.Data), WithRecoverSince(recoverSince), WithSubscribeSource(uint8(cmd.Source)))
	} else if cmd.Disconnect != nil {
		cmd := cmd.Disconnect
		return n.hub.disconnect(cmd.User, Disconnect{Code: cmd.Code, Reason: cmd.Reason}, cmd.Client, cmd.Session, cmd.Whitelist)
	} else if cmd.SurveyRequest != nil {
		cmd := cmd.SurveyRequest
		return n.handleSurveyRequest(uid, cmd)
	} else if cmd.SurveyResponse != nil {
		cmd := cmd.SurveyResponse
		return n.handleSurveyResponse(uid, cmd)
	} else if cmd.Notification != nil {
		cmd := cmd.Notification
		return n.handleNotification(uid, cmd)
	} else if cmd.Refresh != nil {
		cmd := cmd.Refresh
		return n.hub.refresh(cmd.User, cmd.Client, cmd.Session, WithRefreshExpired(cmd.Expired), WithRefreshExpireAt(cmd.ExpireAt), WithRefreshInfo(cmd.Info))
	}
	n.logger.log(newErrorLogEntry(err, "unknown control command", map[string]any{"command": fmt.Sprintf("%#v", cmd)}))
	return nil
}

// handlePublication handles messages published into channel and
// coming from Broker. The goal of method is to deliver this message
// to all clients on this node currently subscribed to channel.
func (n *Node) handlePublication(ch string, sp StreamPosition, pub, prevPub, localPrevPub *Publication) error {
	n.metrics.incMessagesReceived("publication", ch)
	numSubscribers := n.hub.NumSubscribers(ch)
	hasCurrentSubscribers := numSubscribers > 0
	if !hasCurrentSubscribers {
		return nil
	}
	return n.hub.broadcastPublication(ch, sp, pub, prevPub, localPrevPub, n.getBatchConfig(ch))
}

func (n *Node) getBatchConfig(channel string) ChannelBatchConfig {
	if n.config.GetChannelBatchConfig != nil {
		return n.config.GetChannelBatchConfig(channel)
	}
	return ChannelBatchConfig{}
}

// handleJoin handles join messages - i.e. broadcasts it to
// interested local clients subscribed to channel.
func (n *Node) handleJoin(ch string, info *ClientInfo) error {
	n.metrics.incMessagesReceived("join", ch)
	numSubscribers := n.hub.NumSubscribers(ch)
	hasCurrentSubscribers := numSubscribers > 0
	if !hasCurrentSubscribers {
		return nil
	}
	return n.hub.broadcastJoin(ch, info, n.getBatchConfig(ch))
}

// handleLeave handles leave messages - i.e. broadcasts it to
// interested local clients subscribed to channel.
func (n *Node) handleLeave(ch string, info *ClientInfo) error {
	n.metrics.incMessagesReceived("leave", ch)
	numSubscribers := n.hub.NumSubscribers(ch)
	hasCurrentSubscribers := numSubscribers > 0
	if !hasCurrentSubscribers {
		return nil
	}
	return n.hub.broadcastLeave(ch, info, n.getBatchConfig(ch))
}

func (n *Node) publish(ch string, data []byte, opts ...PublishOption) (PublishResult, error) {
	pubOpts := &PublishOptions{}
	for _, opt := range opts {
		opt(pubOpts)
	}
	n.metrics.incMessagesSent("publication", ch)
	result, err := n.getBroker(ch).Publish(ch, data, *pubOpts)
	if err != nil {
		return PublishResult{}, err
	}
	return result, nil
}

// PublishResult returned from Publish operation.
type PublishResult struct {
	StreamPosition
	// Suppressed is true when the operation was suppressed (e.g. due to idempotency key deduplication).
	Suppressed bool
	// SuppressReason explains why the operation was suppressed (empty when Suppressed is false).
	SuppressReason SuppressReason
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
// publish operations. So if you want to have a channel with history stream behind you
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

// publishJoin allows publishing join message into channel when someone subscribes on it
// or leave message when someone unsubscribes from channel.
func (n *Node) publishJoin(ch string, info *ClientInfo) error {
	n.metrics.incMessagesSent("join", ch)
	return n.getBroker(ch).PublishJoin(ch, info)
}

// publishLeave allows publishing join message into channel when someone subscribes on it
// or leave message when someone unsubscribes from channel.
func (n *Node) publishLeave(ch string, info *ClientInfo) error {
	n.metrics.incMessagesSent("leave", ch)
	return n.getBroker(ch).PublishLeave(ch, info)
}

var errNotificationHandlerNotRegistered = errors.New("notification handler not registered")

// Notify allows sending an asynchronous notification to all other nodes
// (or to a single specific node). Unlike Survey, it does not wait for any
// response. If toNodeID is not an empty string then a notification will
// be sent to a concrete node in cluster, otherwise a notification sent to
// all running nodes. See a corresponding Node.OnNotification method to
// handle received notifications.
func (n *Node) Notify(op string, data []byte, toNodeID string) error {
	if n.notificationHandler == nil {
		return errNotificationHandlerNotRegistered
	}

	n.metrics.incActionCount("notify", "")

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
	cmd := &controlpb.Command{
		Uid:          n.uid,
		Notification: notification,
	}
	return n.publishControl(cmd, toNodeID)
}

// publishControl publishes message into control channel so all running
// nodes will receive and handle it.
func (n *Node) publishControl(cmd *controlpb.Command, nodeID string) error {
	n.metrics.incMessagesSent("control", "")
	data, err := n.controlEncoder.EncodeCommand(cmd)
	if err != nil {
		return err
	}
	if n.controller == nil {
		return n.HandleControl(data)
	}
	return n.controller.PublishControl(data, nodeID, "")
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

	cmd := &controlpb.Command{
		Uid:  n.uid,
		Node: node,
	}

	err := n.nodeCmd(node)
	if err != nil {
		n.logger.log(newErrorLogEntry(err, "error handling node command", map[string]any{"error": err.Error()}))
	}

	return n.publishControl(cmd, nodeID)
}

func (n *Node) pubSubscribe(user string, ch string, opts SubscribeOptions) error {
	subscribe := &controlpb.Subscribe{
		User:          user,
		Channel:       ch,
		EmitPresence:  opts.EmitPresence,
		EmitJoinLeave: opts.EmitJoinLeave,
		PushJoinLeave: opts.PushJoinLeave,
		ChannelInfo:   opts.ChannelInfo,
		Position:      opts.EnablePositioning,
		Recover:       opts.EnableRecovery,
		ExpireAt:      opts.ExpireAt,
		Client:        opts.clientID,
		Session:       opts.sessionID,
		Data:          opts.Data,
		Source:        uint32(opts.Source),
	}
	if opts.RecoverSince != nil {
		subscribe.RecoverSince = &controlpb.StreamPosition{
			Offset: opts.RecoverSince.Offset,
			Epoch:  opts.RecoverSince.Epoch,
		}
	}
	cmd := &controlpb.Command{
		Uid:       n.uid,
		Subscribe: subscribe,
	}
	return n.publishControl(cmd, "")
}

func (n *Node) pubRefresh(user string, opts RefreshOptions) error {
	refresh := &controlpb.Refresh{
		User:     user,
		Expired:  opts.Expired,
		ExpireAt: opts.ExpireAt,
		Client:   opts.clientID,
		Session:  opts.sessionID,
		Info:     opts.Info,
	}
	cmd := &controlpb.Command{
		Uid:     n.uid,
		Refresh: refresh,
	}
	return n.publishControl(cmd, "")
}

// pubUnsubscribe publishes unsubscribe control message to all nodes – so all
// nodes could unsubscribe user from channel.
func (n *Node) pubUnsubscribe(user string, ch string, unsubscribe Unsubscribe, clientID, sessionID string) error {
	unsub := &controlpb.Unsubscribe{
		User:    user,
		Channel: ch,
		Code:    unsubscribe.Code,
		Reason:  unsubscribe.Reason,
		Client:  clientID,
		Session: sessionID,
	}
	cmd := &controlpb.Command{
		Uid:         n.uid,
		Unsubscribe: unsub,
	}
	return n.publishControl(cmd, "")
}

// pubDisconnect publishes disconnect control message to all nodes – so all
// nodes could disconnect user from server.
func (n *Node) pubDisconnect(user string, disconnect Disconnect, clientID string, sessionID string, whitelist []string) error {
	protoDisconnect := &controlpb.Disconnect{
		User:      user,
		Whitelist: whitelist,
		Code:      disconnect.Code,
		Reason:    disconnect.Reason,
		Client:    clientID,
		Session:   sessionID,
	}
	cmd := &controlpb.Command{
		Uid:        n.uid,
		Disconnect: protoDisconnect,
	}
	return n.publishControl(cmd, "")
}

// addClient registers authenticated connection in clientConnectionHub
// this allows to make operations with user connection on demand.
func (n *Node) addClient(c *Client) {
	n.metrics.incActionCount("add_client", "")
	var acceptProtocol string
	if n.config.Metrics.ExposeTransportAcceptProtocol {
		acceptProtocol = c.transport.AcceptProtocol()
	}
	n.metrics.connectionsAccepted.WithLabelValues(c.transport.Name(), acceptProtocol, c.metricName, c.metricVersion).Inc()
	n.metrics.connectionsInflight.WithLabelValues(c.transport.Name(), acceptProtocol, c.metricName, c.metricVersion).Inc()
	n.hub.add(c)
}

// removeClient removes client connection from connection registry.
func (n *Node) removeClient(c *Client) {
	n.metrics.incActionCount("remove_client", "")
	removed := n.hub.remove(c)
	if removed {
		var acceptProtocol string
		if n.config.Metrics.ExposeTransportAcceptProtocol {
			acceptProtocol = c.transport.AcceptProtocol()
		}
		n.metrics.connectionsInflight.WithLabelValues(c.transport.Name(), acceptProtocol, c.metricName, c.metricVersion).Dec()
	}
}

// addSubscription registers subscription of connection on channel in both
// Hub and Broker.
func (n *Node) addSubscription(ch string, sub subInfo) (int64, error) {
	n.metrics.incActionCount("add_subscription", ch)
	n.metrics.subscriptionsInflight.WithLabelValues(sub.client.metricName, n.metrics.getChannelNamespaceLabel(ch)).Inc()
	mu := n.subLock(ch)
	mu.Lock()
	defer mu.Unlock()
	chanID, first, err := n.hub.addSub(ch, sub)
	if err != nil {
		return 0, err
	}
	if first {
		if n.config.GetChannelMediumOptions != nil {
			mediumOptions := n.config.GetChannelMediumOptions(ch)
			if mediumOptions.isMediumEnabled() {
				medium, err := newChannelMedium(ch, n, mediumOptions)
				if err != nil {
					_, _, _ = n.hub.removeSub(ch, sub.client)
					return 0, err
				}
				medium.isMap = sub.isMap
				mediumMu := n.mediumLock(ch)
				mediumMu.Lock()
				n.mediums[ch] = medium
				mediumMu.Unlock()
			}
		}

		// Subscribe to appropriate broker based on subscription type.
		if sub.isMap {
			if mapBroker := n.getMapBroker(ch); mapBroker != nil {
				n.metrics.incActionCount("map_broker_subscribe", ch)
				err := mapBroker.Subscribe(ch)
				if err != nil {
					_, _, _ = n.hub.removeSub(ch, sub.client)
					if n.config.GetChannelMediumOptions != nil {
						mediumMu := n.mediumLock(ch)
						mediumMu.Lock()
						medium, ok := n.mediums[ch]
						if ok {
							medium.close()
							delete(n.mediums, ch)
						}
						mediumMu.Unlock()
					}
					return 0, err
				}
			}
		} else {
			n.metrics.incActionCount("broker_subscribe", ch)
			err := n.getBroker(ch).Subscribe(ch)
			if err != nil {
				_, _, _ = n.hub.removeSub(ch, sub.client)
				if n.config.GetChannelMediumOptions != nil {
					mediumMu := n.mediumLock(ch)
					mediumMu.Lock()
					medium, ok := n.mediums[ch]
					if ok {
						medium.close()
						delete(n.mediums, ch)
					}
					mediumMu.Unlock()
				}
				return 0, err
			}
		}
	}
	return chanID, nil
}

// removeSubscription removes subscription of connection on channel
// from Hub and Broker (or MapBroker for map channels).
func (n *Node) removeSubscription(ch string, c *Client) error {
	n.metrics.incActionCount("remove_subscription", ch)
	mu := n.subLock(ch)
	mu.Lock()
	defer mu.Unlock()
	empty, wasRemoved, wasMap := n.hub.removeSub(ch, c)
	if wasRemoved {
		n.metrics.subscriptionsInflight.WithLabelValues(c.metricName, n.metrics.getChannelNamespaceLabel(ch)).Dec()
	}
	if empty {
		submittedAt := time.Now()
		_ = n.subDissolver.Submit(func() error {
			timeSpent := time.Since(submittedAt)
			if timeSpent < time.Second {
				time.Sleep(time.Second - timeSpent)
			}
			subMu := n.subLock(ch)
			subMu.Lock()
			defer subMu.Unlock()
			noSubscribers := n.hub.NumSubscribers(ch) == 0
			if noSubscribers {
				// Unsubscribe from appropriate broker based on channel type.
				if wasMap {
					if mapBroker := n.getMapBroker(ch); mapBroker != nil {
						n.metrics.incActionCount("map_broker_unsubscribe", ch)
						err := mapBroker.Unsubscribe(ch)
						if err != nil {
							time.Sleep(500 * time.Millisecond)
							return err
						}
					}
				} else {
					n.metrics.incActionCount("broker_unsubscribe", ch)
					err := n.getBroker(ch).Unsubscribe(ch)
					if err != nil {
						// Cool down a bit since broker is not ready to process unsubscription.
						time.Sleep(500 * time.Millisecond)
						return err
					}
				}
				n.hub.removeSubID(ch)
				if n.config.GetChannelMediumOptions != nil {
					mediumMu := n.mediumLock(ch)
					mediumMu.Lock()
					medium, ok := n.mediums[ch]
					if ok {
						medium.close()
						delete(n.mediums, ch)
					}
					mediumMu.Unlock()
				}
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
	// Send subscribe control message to other nodes.
	err := n.pubSubscribe(userID, channel, *subscribeOpts)
	if err != nil {
		return err
	}
	// Subscribe on this node.
	return n.hub.subscribe(userID, channel, subscribeOpts.clientID, subscribeOpts.sessionID, opts...)
}

// Unsubscribe unsubscribes user from a channel.
// If a channel is empty string then user will be unsubscribed from all channels.
func (n *Node) Unsubscribe(userID string, channel string, opts ...UnsubscribeOption) error {
	unsubscribeOpts := &UnsubscribeOptions{}
	for _, opt := range opts {
		opt(unsubscribeOpts)
	}
	customUnsubscribe := unsubscribeServer
	if unsubscribeOpts.unsubscribe != nil {
		customUnsubscribe = *unsubscribeOpts.unsubscribe
	}
	// Send unsubscribe control message to other nodes.
	err := n.pubUnsubscribe(userID, channel, customUnsubscribe, unsubscribeOpts.clientID, unsubscribeOpts.sessionID)
	if err != nil {
		return err
	}
	// Unsubscribe on this node.
	return n.hub.unsubscribe(userID, channel, customUnsubscribe, unsubscribeOpts.clientID, unsubscribeOpts.sessionID)
}

// Disconnect allows closing all user connections on all nodes.
func (n *Node) Disconnect(userID string, opts ...DisconnectOption) error {
	disconnectOpts := &DisconnectOptions{}
	for _, opt := range opts {
		opt(disconnectOpts)
	}
	// Disconnect user from this node
	customDisconnect := DisconnectForceNoReconnect
	if disconnectOpts.Disconnect != nil {
		customDisconnect = *disconnectOpts.Disconnect
	}
	// Send disconnect control message to other nodes.
	err := n.pubDisconnect(userID, customDisconnect, disconnectOpts.clientID, disconnectOpts.sessionID, disconnectOpts.ClientWhitelist)
	if err != nil {
		return err
	}
	// Disconnect on this node.
	return n.hub.disconnect(userID, customDisconnect, disconnectOpts.clientID, disconnectOpts.sessionID, disconnectOpts.ClientWhitelist)
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
	err := n.pubRefresh(userID, *refreshOpts)
	if err != nil {
		return err
	}
	// Refresh on this node.
	return n.hub.refresh(userID, refreshOpts.clientID, refreshOpts.sessionID, opts...)
}

func (n *Node) getPresenceManager(ch string) PresenceManager {
	if n.config.GetPresenceManager != nil {
		if presenceManager, ok := n.config.GetPresenceManager(ch); ok {
			return presenceManager
		}
	}
	if n.presenceManager == nil {
		return nil
	}
	return n.presenceManager
}

// addPresence proxies presence adding to PresenceManager.
func (n *Node) addPresence(ch string, uid string, info *ClientInfo) error {
	presenceManager := n.getPresenceManager(ch)
	if presenceManager == nil {
		return nil
	}
	n.metrics.incActionCount("add_presence", ch)
	return presenceManager.AddPresence(ch, uid, info)
}

// removePresence proxies presence removing to PresenceManager.
func (n *Node) removePresence(ch string, clientID string, userID string) error {
	presenceManager := n.getPresenceManager(ch)
	if presenceManager == nil {
		return nil
	}
	n.metrics.incActionCount("remove_presence", ch)
	return presenceManager.RemovePresence(ch, clientID, userID)
}

var (
	presenceGroup      singleflight.Group
	presenceStatsGroup singleflight.Group
	historyGroup       singleflight.Group
	mapStateGroup      singleflight.Group
	mapStreamGroup     singleflight.Group
	mapStatsGroup      singleflight.Group
)

// PresenceResult wraps presence.
type PresenceResult struct {
	Presence map[string]*ClientInfo
}

func (n *Node) presence(ch string, presenceManager PresenceManager) (PresenceResult, error) {
	presence, err := presenceManager.Presence(ch)
	if err != nil {
		return PresenceResult{}, err
	}
	return PresenceResult{Presence: presence}, nil
}

// Presence returns a map with information about active clients in channel.
func (n *Node) Presence(ch string) (PresenceResult, error) {
	presenceManager := n.getPresenceManager(ch)
	if presenceManager == nil {
		return PresenceResult{}, ErrorNotAvailable
	}
	n.metrics.incActionCount("presence", ch)
	if n.config.UseSingleFlight {
		result, err, _ := presenceGroup.Do(ch, func() (any, error) {
			return n.presence(ch, presenceManager)
		})
		return result.(PresenceResult), err
	}
	return n.presence(ch, presenceManager)
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
		Offset:  pub.Offset,
		Epoch:   pub.Epoch,
		Data:    pub.Data,
		Info:    infoToProto(pub.Info),
		Tags:    pub.Tags,
		Channel: pub.Channel,
		Removed: pub.Removed,
		Key:     pub.Key,
		Score:   pub.Score,
		Version: pub.Version,
	}
}

func pubFromProto(pub *protocol.Publication) *Publication {
	if pub == nil {
		return nil
	}
	return &Publication{
		Offset:  pub.GetOffset(),
		Epoch:   pub.GetEpoch(),
		Data:    pub.Data,
		Info:    infoFromProto(pub.GetInfo()),
		Tags:    pub.GetTags(),
		Time:    pub.Time,
		Channel: pub.GetChannel(),
		Key:     pub.GetKey(),
		Removed: pub.GetRemoved(),
		Score:   pub.GetScore(),
		Version: pub.GetVersion(),
	}
}

// PresenceStatsResult wraps presence stats.
type PresenceStatsResult struct {
	PresenceStats
}

func (n *Node) presenceStats(ch string, presenceManager PresenceManager) (PresenceStatsResult, error) {
	presenceStats, err := presenceManager.PresenceStats(ch)
	if err != nil {
		return PresenceStatsResult{}, err
	}
	return PresenceStatsResult{PresenceStats: presenceStats}, nil
}

// PresenceStats returns presence stats from PresenceManager.
func (n *Node) PresenceStats(ch string) (PresenceStatsResult, error) {
	presenceManager := n.getPresenceManager(ch)
	if presenceManager == nil {
		return PresenceStatsResult{}, ErrorNotAvailable
	}
	n.metrics.incActionCount("presence_stats", ch)
	if n.config.UseSingleFlight {
		result, err, _ := presenceStatsGroup.Do(ch, func() (any, error) {
			return n.presenceStats(ch, presenceManager)
		})
		return result.(PresenceStatsResult), err
	}
	return n.presenceStats(ch, presenceManager)
}

// HistoryResult contains Publications and current stream top StreamPosition.
type HistoryResult struct {
	// StreamPosition embedded here describes current stream top offset and epoch.
	StreamPosition
	// Publications extracted from history storage according to HistoryFilter.
	Publications []*Publication
}

func (n *Node) getBroker(ch string) Broker {
	if n.config.GetBroker != nil {
		if broker, ok := n.config.GetBroker(ch); ok {
			return broker
		}
	}
	return n.broker
}

func (n *Node) getMapBroker(ch string) MapBroker {
	if n.config.Map.GetMapBroker != nil {
		if broker, ok := n.config.Map.GetMapBroker(ch); ok {
			return broker
		}
	}
	return n.mapBroker
}

func (n *Node) history(ch string, opts *HistoryOptions) (HistoryResult, error) {
	if opts.Filter.Reverse && opts.Filter.Since != nil && opts.Filter.Since.Offset == 0 {
		return HistoryResult{}, ErrorBadRequest
	}

	pubs, streamTop, err := n.getBroker(ch).History(ch, *opts)
	if err != nil {
		return HistoryResult{}, err
	}
	if opts.Filter.Since != nil {
		sinceEpoch := opts.Filter.Since.Epoch
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

// History allows extracting Publications in channel.
// The channel must belong to namespace where history is on.
func (n *Node) History(ch string, opts ...HistoryOption) (HistoryResult, error) {
	n.metrics.incActionCount("history", ch)
	historyOpts := &HistoryOptions{}
	for _, opt := range opts {
		opt(historyOpts)
	}
	if n.config.UseSingleFlight {
		var builder strings.Builder
		builder.WriteString("channel:")
		builder.WriteString(ch)
		if historyOpts.Filter.Since != nil {
			builder.WriteString(",offset:")
			builder.WriteString(strconv.FormatUint(historyOpts.Filter.Since.Offset, 10))
			builder.WriteString(",epoch:")
			builder.WriteString(historyOpts.Filter.Since.Epoch)
		}
		builder.WriteString(",limit:")
		builder.WriteString(strconv.Itoa(historyOpts.Filter.Limit))
		builder.WriteString(",reverse:")
		builder.WriteString(strconv.FormatBool(historyOpts.Filter.Reverse))
		builder.WriteString(",meta_ttl:")
		builder.WriteString(historyOpts.MetaTTL.String())
		key := builder.String()

		result, err, _ := historyGroup.Do(key, func() (any, error) {
			return n.history(ch, historyOpts)
		})
		return result.(HistoryResult), err
	}
	return n.history(ch, historyOpts)
}

// recoverHistory recovers publications since StreamPosition last seen by client.
func (n *Node) recoverHistory(ch string, since StreamPosition, historyMetaTTL time.Duration) (HistoryResult, error) {
	n.metrics.incActionCount("history_recover", ch)
	limit := NoLimit
	maxPublicationLimit := n.config.RecoveryMaxPublicationLimit
	if maxPublicationLimit > 0 {
		limit = maxPublicationLimit
	}
	return n.History(ch, WithHistoryFilter(HistoryFilter{
		Limit: limit,
		Since: &since,
	}), WithHistoryMetaTTL(historyMetaTTL))
}

// recoverCache recovers last publication in channel.
func (n *Node) recoverCache(ch string, historyMetaTTL time.Duration, tf *tagsFilter) (*Publication, *Publication, StreamPosition, error) {
	n.metrics.incActionCount("history_recover_cache", ch)
	if tf == nil {
		hr, err := n.History(ch, WithHistoryFilter(HistoryFilter{
			Limit:   1,
			Reverse: true,
		}), WithHistoryMetaTTL(historyMetaTTL))
		if err != nil {
			return nil, nil, StreamPosition{}, err
		}
		var latestPublication *Publication
		if len(hr.Publications) > 0 {
			latestPublication = hr.Publications[0]
		}
		return latestPublication, latestPublication, hr.StreamPosition, nil
	}

	limit := NoLimit
	maxPublicationLimit := n.config.RecoveryMaxPublicationLimit
	if maxPublicationLimit > 0 {
		limit = maxPublicationLimit
	}

	hr, err := n.History(ch, WithHistoryFilter(HistoryFilter{
		Limit:   limit,
		Reverse: true,
	}), WithHistoryMetaTTL(historyMetaTTL))
	if err != nil {
		return nil, nil, StreamPosition{}, err
	}
	var latestPublication *Publication
	if len(hr.Publications) > 0 {
		latestPublication = hr.Publications[0]
	}
	for _, pub := range hr.Publications {
		match, _ := filter.Match(tf.filter, pub.Tags)
		if match {
			return latestPublication, pub, hr.StreamPosition, nil
		}
	}
	return nil, nil, hr.StreamPosition, nil
}

// streamTop returns current stream top StreamPosition for a channel.
func (n *Node) streamTop(ch string, historyMetaTTL time.Duration) (StreamPosition, error) {
	n.metrics.incActionCount("history_stream_top", ch)
	historyResult, err := n.History(ch, WithHistoryMetaTTL(historyMetaTTL))
	if err != nil {
		return StreamPosition{}, err
	}
	return historyResult.StreamPosition, nil
}

func (n *Node) mapStreamTop(ch string) (StreamPosition, error) {
	mapBroker := n.getMapBroker(ch)
	if mapBroker == nil {
		return StreamPosition{}, nil
	}
	streamResult, err := mapBroker.ReadStream(context.Background(), ch, MapReadStreamOptions{
		Filter: StreamFilter{Limit: 0},
	})
	if err != nil {
		return StreamPosition{}, err
	}
	return streamResult.Position, nil
}

func (n *Node) checkPosition(ch string, clientPosition StreamPosition, historyMetaTTL time.Duration, isMap bool) (bool, error) {
	if isMap {
		mapBroker := n.getMapBroker(ch)
		if mapBroker == nil {
			return true, nil
		}
		// If the map broker guarantees no-gaps delivery to local subscribers,
		// the periodic position sync is redundant — trust the broker.
		if rd, ok := mapBroker.(reliableDeliverer); ok && rd.ReliableDelivery() {
			return true, nil
		}
	} else {
		// If the stream broker guarantees no-gaps delivery to local subscribers,
		// skip the position sync request entirely.
		if rd, ok := n.getBroker(ch).(reliableDeliverer); ok && rd.ReliableDelivery() {
			return true, nil
		}
	}
	mu := n.subLock(ch)
	mu.Lock()
	medium, ok := n.mediums[ch]
	mu.Unlock()
	if ok && medium.options.SharedPositionSync {
		validPosition := medium.CheckPosition(historyMetaTTL, clientPosition, n.config.ClientChannelPositionCheckDelay)
		return validPosition, nil
	}
	// No medium for channel or position sync disabled – check position over Broker.
	if isMap {
		streamTop, err := n.mapStreamTop(ch)
		if err != nil {
			return false, err
		}
		return streamTop.Epoch == clientPosition.Epoch && clientPosition.Offset == streamTop.Offset, nil
	}
	streamTop, err := n.streamTop(ch, historyMetaTTL)
	if err != nil {
		return false, err
	}
	return streamTop.Epoch == clientPosition.Epoch && clientPosition.Offset == streamTop.Offset, nil
}

// RemoveHistory removes channel history.
func (n *Node) RemoveHistory(ch string) error {
	n.metrics.incActionCount("history_remove", ch)
	return n.getBroker(ch).RemoveHistory(ch)
}

type nodeRegistry struct {
	// mu allows synchronizing access to node registry.
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

func (r *nodeRegistry) size() int {
	r.mu.RLock()
	size := len(r.nodes)
	r.mu.RUnlock()
	return size
}

func (r *nodeRegistry) get(uid string) (*controlpb.Node, bool) {
	r.mu.RLock()
	info, ok := r.nodes[uid]
	r.mu.RUnlock()
	return info, ok
}

func (r *nodeRegistry) add(info *controlpb.Node) bool {
	var isNewNode bool
	r.mu.Lock()
	if node, ok := r.nodes[info.Uid]; ok {
		if info.Metrics != nil {
			r.nodes[info.Uid] = info
		} else {
			r.nodes[info.Uid] = &controlpb.Node{
				Uid:         info.Uid,
				Name:        info.Name,
				Version:     info.Version,
				NumClients:  info.NumClients,
				NumUsers:    info.NumUsers,
				NumChannels: info.NumChannels,
				Uptime:      info.Uptime,
				Data:        info.Data,
				NumSubs:     info.NumSubs,
				Metrics:     node.Metrics,
			}
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

// OnNodeInfoSend allows setting NodeInfoSendHandler. This should be done before Node.Run called.
func (n *Node) OnNodeInfoSend(handler NodeInfoSendHandler) {
	n.nodeInfoSendHandler = handler
}

// eventHub allows binding client event handlers.
// All eventHub methods are not goroutine-safe and supposed
// to be called once before Node Run called.
type eventHub struct {
	connectingHandler       ConnectingHandler
	connectHandler          ConnectHandler
	transportWriteHandler   TransportWriteHandler
	commandReadHandler      CommandReadHandler
	commandProcessedHandler CommandProcessedHandler
	cacheEmptyHandler       CacheEmptyHandler
	sharedPollHandler       SharedPollHandler
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

// OnTransportWrite allows setting TransportWriteHandler. This should be done before Node.Run called.
func (n *Node) OnTransportWrite(handler TransportWriteHandler) {
	n.clientEvents.transportWriteHandler = handler
}

// OnCommandRead allows setting CommandReadHandler. This should be done before Node.Run called.
func (n *Node) OnCommandRead(handler CommandReadHandler) {
	n.clientEvents.commandReadHandler = handler
}

// OnCommandProcessed allows setting CommandProcessedHandler. This should be done before Node.Run called.
func (n *Node) OnCommandProcessed(handler CommandProcessedHandler) {
	n.clientEvents.commandProcessedHandler = handler
}

// OnCacheEmpty allows setting CacheEmptyHandler.
// CacheEmptyHandler called when client subscribes on a channel with RecoveryModeCache but there is no
// cached value in channel. In response to this handler it's possible to tell Centrifuge what to do with
// subscribe request – keep it, or return error.
func (n *Node) OnCacheEmpty(h CacheEmptyHandler) {
	n.clientEvents.cacheEmptyHandler = h
}

// OnSharedPoll allows setting SharedPollHandler.
// SharedPollHandler is called by the refresh worker to fetch current item
// data from the backend. Called per-channel, not per-client.
func (n *Node) OnSharedPoll(handler SharedPollHandler) {
	n.clientEvents.sharedPollHandler = handler
}

// SharedPollNotify submits notifications that trigger immediate backend polls
// for the specified keys. Notifications are batched per channel according to
// SharedPollChannelOptions before triggering polls. Safe for concurrent use.
// Notifications for unknown channels are silently dropped.
func (n *Node) SharedPollNotify(notifications []SharedPollNotificationItem) {
	if n.sharedPollManager == nil {
		return
	}
	for i := range notifications {
		n.sharedPollManager.notify(notifications[i].Channel, notifications[i].Key)
	}
}

// SharedPollPublish pushes data directly to a SharedPoll channel for a specific key.
// The version must be in the same space as versions returned by the OnSharedPoll handler —
// stale versions (≤ current) are ignored within a given epoch. When PublishEnabled is set
// in channel options, the publication is distributed to all nodes via Broker PUB/SUB.
// Otherwise, local-only.
//
// epoch is an optional channel-level string identifying the publisher's epoch.
// If it differs from the channel's stored epoch, all current subscribers are
// unsubscribed with insufficient-state code so they re-track from version 0 on
// resubscribe — this lets a publisher that resets its in-memory version counter
// (e.g., after a process restart) deliver fresh state without freezing connected
// clients. Use empty epoch to skip this check (pure version comparison).
func (n *Node) SharedPollPublish(ctx context.Context, channel string, key string, version uint64, epoch string, data []byte) error {
	if n.sharedPollManager == nil {
		return errors.New("shared poll manager not initialized")
	}
	return n.sharedPollManager.publish(ctx, channel, key, version, epoch, data)
}

// HandlePublication coming from Broker.
func (n *Node) HandlePublication(ch string, pub *Publication, sp StreamPosition, delta bool, prevPub *Publication) error {
	if pub == nil {
		panic("nil Publication received, this must never happen")
	}
	// Route shared poll key-scoped publications to SharedPollManager.
	if n.sharedPollManager != nil && pub.Key != "" {
		if baseCh, key := parseSharedPollKeyChannel(ch); baseCh != "" {
			n.sharedPollManager.handlePublishedData(baseCh, key, pub.Version, pub.Epoch, pub.Data)
			return nil
		}
		// Fallback: check if it's a direct channel match (local-only path).
		if n.sharedPollManager.hasChannel(ch) {
			n.sharedPollManager.handlePublishedData(ch, pub.Key, pub.Version, pub.Epoch, pub.Data)
			return nil
		}
	}
	// Deliver epoch in the first publication (offset==1) so clients learn
	// the channel epoch. This covers first-ever publish and post-Clear
	// scenarios. Subsequent publications omit epoch to save wire bytes.
	if pub.Offset == 1 && sp.Epoch != "" {
		pub.Epoch = sp.Epoch
	}
	if n.config.GetChannelMediumOptions != nil {
		mu := n.mediumLock(ch) // Note, avoid using subLock in HandlePublication – this leads to the deadlock.
		mu.Lock()
		medium, ok := n.mediums[ch]
		mu.Unlock()
		if ok {
			medium.broadcastPublication(pub, sp, delta, prevPub)
			return nil
		}
	}
	return n.handlePublication(ch, sp, pub, prevPub, nil)
}

// HandleJoin coming from Broker.
func (n *Node) HandleJoin(ch string, info *ClientInfo) error {
	if info == nil {
		panic("nil join ClientInfo received, this must never happen")
	}
	return n.handleJoin(ch, info)
}

// HandleLeave coming from Broker.
func (n *Node) HandleLeave(ch string, info *ClientInfo) error {
	if info == nil {
		panic("nil leave ClientInfo received, this must never happen")
	}
	return n.handleLeave(ch, info)
}

// HandleControl coming from Broker.
func (n *Node) HandleControl(data []byte) error {
	return n.handleControl(data)
}

// MapStateRead retrieves keyed snapshot for a channel.
func (n *Node) MapStateRead(ctx context.Context, ch string, opts MapReadStateOptions) (MapStateResult, error) {
	mapBroker := n.getMapBroker(ch)
	if mapBroker == nil {
		return MapStateResult{}, ErrorNotAvailable
	}

	n.metrics.incActionCount("map_state_read", ch)
	if n.config.UseSingleFlight {
		key := n.mapStateKey(ch, opts)
		result, err, _ := mapStateGroup.Do(key, func() (any, error) {
			return mapBroker.ReadState(ctx, ch, opts)
		})
		if err != nil {
			return MapStateResult{}, err
		}
		return result.(MapStateResult), nil
	}

	return mapBroker.ReadState(ctx, ch, opts)
}

func (n *Node) mapStateKey(ch string, opts MapReadStateOptions) string {
	var builder strings.Builder
	builder.WriteString(ch)
	builder.WriteString(",cursor:")
	builder.WriteString(opts.Cursor)
	builder.WriteString(",limit:")
	builder.WriteString(strconv.Itoa(opts.Limit))
	builder.WriteString(",key:")
	builder.WriteString(opts.Key)
	if opts.Asc {
		builder.WriteString(",asc:1")
	}
	if opts.AllowCached {
		builder.WriteString(",cached:1")
	}
	if opts.Revision != nil {
		builder.WriteString(",rev_offset:")
		builder.WriteString(strconv.FormatUint(opts.Revision.Offset, 10))
		builder.WriteString(",rev_epoch:")
		builder.WriteString(opts.Revision.Epoch)
	}
	return builder.String()
}

// MapStreamRead retrieves keyed stream for a channel.
func (n *Node) MapStreamRead(ctx context.Context, ch string, opts MapReadStreamOptions) (MapStreamResult, error) {
	mapBroker := n.getMapBroker(ch)
	if mapBroker == nil {
		return MapStreamResult{}, ErrorNotAvailable
	}

	n.metrics.incActionCount("map_stream_read", ch)

	var result MapStreamResult
	var err error
	if n.config.UseSingleFlight {
		key := n.mapStreamKey(ch, opts)
		r, e, _ := mapStreamGroup.Do(key, func() (any, error) {
			return mapBroker.ReadStream(ctx, ch, opts)
		})
		if e != nil {
			return MapStreamResult{}, e
		}
		result = r.(MapStreamResult)
	} else {
		result, err = mapBroker.ReadStream(ctx, ch, opts)
		if err != nil {
			return MapStreamResult{}, err
		}
	}

	// Detect unrecoverable position: if we requested entries after a known offset
	// but the first returned entry has a higher offset, entries were lost due to
	// stream trimming — the client cannot recover cleanly.
	if !opts.Filter.Reverse && opts.Filter.Since != nil && opts.Filter.Since.Offset > 0 &&
		len(result.Publications) > 0 && result.Publications[0].Offset > opts.Filter.Since.Offset+1 {
		return MapStreamResult{}, ErrorUnrecoverablePosition
	}

	return result, nil
}

func (n *Node) mapStreamKey(ch string, opts MapReadStreamOptions) string {
	var builder strings.Builder
	builder.WriteString(ch)
	if opts.Filter.Since != nil {
		builder.WriteString(",since_offset:")
		builder.WriteString(strconv.FormatUint(opts.Filter.Since.Offset, 10))
		builder.WriteString(",since_epoch:")
		builder.WriteString(opts.Filter.Since.Epoch)
	}
	builder.WriteString(",limit:")
	builder.WriteString(strconv.Itoa(opts.Filter.Limit))
	builder.WriteString(",reverse:")
	builder.WriteString(strconv.FormatBool(opts.Filter.Reverse))
	return builder.String()
}

// mapStreamPosition returns the current stream position for a map channel.
// This is useful for capturing the stream top before starting stream pagination.
func (n *Node) mapStreamPosition(ctx context.Context, ch string) (StreamPosition, error) {
	mapBroker := n.getMapBroker(ch)
	if mapBroker == nil {
		return StreamPosition{}, ErrorNotAvailable
	}
	n.metrics.incActionCount("map_stream_position", ch)
	// ReadStream with Limit=0 returns only the current stream position.
	result, err := mapBroker.ReadStream(ctx, ch, MapReadStreamOptions{
		Filter: StreamFilter{Limit: 0},
	})
	if err != nil {
		return StreamPosition{}, err
	}
	return result.Position, nil
}

// MapStatsResult wraps keyed stats result.
type MapStatsResult struct {
	MapStats
}

// MapStats retrieves stats for a map channel.
func (n *Node) MapStats(ctx context.Context, ch string) (MapStatsResult, error) {
	mapBroker := n.getMapBroker(ch)
	if mapBroker == nil {
		return MapStatsResult{}, ErrorNotAvailable
	}

	n.metrics.incActionCount("map_stats", ch)
	if n.config.UseSingleFlight {
		result, err, _ := mapStatsGroup.Do(ch, func() (any, error) {
			stats, err := mapBroker.Stats(ctx, ch)
			if err != nil {
				return MapStatsResult{}, err
			}
			return MapStatsResult{MapStats: stats}, nil
		})
		if err != nil {
			return MapStatsResult{}, err
		}
		return result.(MapStatsResult), nil
	}

	stats, err := mapBroker.Stats(ctx, ch)
	if err != nil {
		return MapStatsResult{}, err
	}
	return MapStatsResult{MapStats: stats}, nil
}

// MapPublish publishes data to a map channel.
// This updates the snapshot and optionally broadcasts to subscribers.
func (n *Node) MapPublish(ctx context.Context, ch string, key string, opts MapPublishOptions) (MapUpdateResult, error) {
	if key == "" {
		return MapUpdateResult{}, ErrorBadRequest
	}
	mapBroker := n.getMapBroker(ch)
	if mapBroker == nil {
		return MapUpdateResult{}, ErrorNotAvailable
	}
	n.metrics.incActionCount("map_publish", ch)
	n.metrics.incMessagesSent("map_publication", ch)
	result, err := mapBroker.Publish(ctx, ch, key, opts)
	if err == nil && result.Suppressed {
		n.metrics.incMapPublishSuppressed(result.SuppressReason, ch)
	}
	return result, err
}

// MapRemove removes a key from a map channel.
// This removes the key from snapshot and optionally broadcasts removal to subscribers.
func (n *Node) MapRemove(ctx context.Context, ch string, key string, opts MapRemoveOptions) (MapUpdateResult, error) {
	if key == "" {
		return MapUpdateResult{}, ErrorBadRequest
	}
	mapBroker := n.getMapBroker(ch)
	if mapBroker == nil {
		return MapUpdateResult{}, ErrorNotAvailable
	}
	n.metrics.incActionCount("map_remove", ch)
	n.metrics.incMessagesSent("map_removal", ch)
	result, err := mapBroker.Remove(ctx, ch, key, opts)
	if err == nil && result.Suppressed {
		n.metrics.incMapPublishSuppressed(result.SuppressReason, ch)
	}
	return result, err
}

// MapClear deletes all data for a map channel (state and stream).
// Use for cleanup when a channel data is no longer needed.
func (n *Node) MapClear(ctx context.Context, ch string, opts MapClearOptions) error {
	mapBroker := n.getMapBroker(ch)
	if mapBroker == nil {
		return ErrorNotAvailable
	}
	n.metrics.incActionCount("map_clear", ch)
	return mapBroker.Clear(ctx, ch, opts)
}
