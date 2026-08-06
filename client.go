package centrifuge

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/centrifugal/centrifuge/internal/bpool"
	"github.com/centrifugal/centrifuge/internal/convert"
	"github.com/centrifugal/centrifuge/internal/filter"
	"github.com/centrifugal/centrifuge/internal/queue"
	"github.com/centrifugal/centrifuge/internal/recovery"
	"github.com/centrifugal/centrifuge/internal/saferand"
	"github.com/centrifugal/centrifuge/internal/timers"

	"github.com/centrifugal/protocol"
	"github.com/google/uuid"
	"github.com/segmentio/encoding/json"
	fdelta "github.com/shadowspore/fossil-delta"
	"google.golang.org/protobuf/encoding/protojson"
)

// Empty Replies/Pushes for pings.
var jsonPingReply = []byte(`{}`)
var protobufPingReply []byte
var jsonPingPush = []byte(`{}`)
var protobufPingPush []byte

var randSource *saferand.Rand

func init() {
	protobufPingReply, _ = protocol.DefaultProtobufReplyEncoder.Encode(&protocol.Reply{})
	protobufPingPush, _ = protocol.DefaultProtobufPushEncoder.Encode(&protocol.Push{})
	randSource = saferand.New(time.Now().UnixNano())
}

// clientEventHub allows dealing with client event handlers.
// All its methods are not goroutine-safe and supposed to be called
// once inside Node ConnectHandler.
type clientEventHub struct {
	aliveHandler         AliveHandler
	disconnectHandler    DisconnectHandler
	subscribeHandler     SubscribeHandler
	unsubscribeHandler   UnsubscribeHandler
	publishHandler       PublishHandler
	mapPublishHandler    MapPublishHandler
	mapRemoveHandler     MapRemoveHandler
	refreshHandler       RefreshHandler
	subRefreshHandler    SubRefreshHandler
	rpcHandler           RPCHandler
	messageHandler       MessageHandler
	presenceHandler      PresenceHandler
	presenceStatsHandler PresenceStatsHandler
	historyHandler       HistoryHandler
	stateSnapshotHandler StateSnapshotHandler
	trackHandler         TrackHandler
	untrackHandler       UntrackHandler
}

// OnAlive allows setting AliveHandler.
// AliveHandler called periodically for active client connection.
func (c *Client) OnAlive(h AliveHandler) {
	c.eventHub.aliveHandler = h
}

// OnRefresh allows setting RefreshHandler.
// RefreshHandler called when it's time to refresh expiring client connection.
func (c *Client) OnRefresh(h RefreshHandler) {
	c.eventHub.refreshHandler = h
}

// OnDisconnect allows setting DisconnectHandler.
// DisconnectHandler called when client disconnected.
func (c *Client) OnDisconnect(h DisconnectHandler) {
	c.eventHub.disconnectHandler = h
}

// OnMessage allows setting MessageHandler.
// MessageHandler called when client sent asynchronous message.
func (c *Client) OnMessage(h MessageHandler) {
	c.eventHub.messageHandler = h
}

// OnRPC allows setting RPCHandler.
// RPCHandler will be executed on every incoming RPC call.
func (c *Client) OnRPC(h RPCHandler) {
	c.eventHub.rpcHandler = h
}

// OnSubRefresh allows setting SubRefreshHandler.
// SubRefreshHandler called when it's time to refresh client subscription.
func (c *Client) OnSubRefresh(h SubRefreshHandler) {
	c.eventHub.subRefreshHandler = h
}

// OnSubscribe allows setting SubscribeHandler.
// SubscribeHandler called when client subscribes on a channel.
func (c *Client) OnSubscribe(h SubscribeHandler) {
	c.eventHub.subscribeHandler = h
}

// OnUnsubscribe allows setting UnsubscribeHandler.
// UnsubscribeHandler called when client unsubscribes from channel.
func (c *Client) OnUnsubscribe(h UnsubscribeHandler) {
	c.eventHub.unsubscribeHandler = h
}

// OnPublish allows setting PublishHandler.
// PublishHandler called when client publishes message into channel.
func (c *Client) OnPublish(h PublishHandler) {
	c.eventHub.publishHandler = h
}

// OnMapPublish allows setting MapPublishHandler.
// MapPublishHandler called when client publishes into map channel.
func (c *Client) OnMapPublish(h MapPublishHandler) {
	c.eventHub.mapPublishHandler = h
}

// OnMapRemove allows setting MapRemoveHandler.
// MapRemoveHandler called when client wants to remove a key from map channel.
func (c *Client) OnMapRemove(h MapRemoveHandler) {
	c.eventHub.mapRemoveHandler = h
}

// OnPresence allows setting PresenceHandler.
// PresenceHandler called when Presence request from client received.
// At this moment you can only return a custom error or disconnect client.
func (c *Client) OnPresence(h PresenceHandler) {
	c.eventHub.presenceHandler = h
}

// OnPresenceStats allows settings PresenceStatsHandler.
// PresenceStatsHandler called when Presence Stats request from client received.
// At this moment you can only return a custom error or disconnect client.
func (c *Client) OnPresenceStats(h PresenceStatsHandler) {
	c.eventHub.presenceStatsHandler = h
}

// OnHistory allows settings HistoryHandler.
// HistoryHandler called when History request from client received.
// At this moment you can only return a custom error or disconnect client.
func (c *Client) OnHistory(h HistoryHandler) {
	c.eventHub.historyHandler = h
}

// OnTrack allows setting TrackHandler.
// TrackHandler called when client sends a track request on a keyed channel.
func (c *Client) OnTrack(h TrackHandler) {
	c.eventHub.trackHandler = h
}

// OnUntrack allows setting UntrackHandler.
// UntrackHandler called when client untracks keys on a keyed channel.
func (c *Client) OnUntrack(h UntrackHandler) {
	c.eventHub.untrackHandler = h
}

const (
	// flagSubscribed will be set upon successful Subscription to a channel.
	// Until that moment channel exists in client Channels map only to track
	// duplicate subscription requests.
	flagSubscribed uint16 = 1 << iota
	flagEmitPresence
	flagEmitJoinLeave
	flagPushJoinLeave
	flagPositioning
	flagServerSide
	flagClientSideRefresh
	flagDeltaAllowed
	flagMap                  // Channel uses map subscription (presence via MapBroker)
	flagMapPresence          // Presence subscription (:clients or :users suffix)
	flagMapClientPresence    // Emit to {channel}:clients, key=clientId, full ClientInfo
	flagMapUserPresence      // Emit to {channel}:users, key=userId, no info
	flagCleanupOnUnsubscribe // Clean up keys by client_id when subscription ends
	flagKeyed                // Channel uses keyed subscription (shared poll track/untrack)
	// flagServerTagsFilter marks a subscription narrowed by a server-controlled
	// tags filter. Such a subscriber must not be offered the channel's compression
	// dictionary: the filter withholds publications from them, but the dictionary
	// is built from all of them.
	flagServerTagsFilter
)

// ChannelContext contains extra context for channel connection subscribed to.
// Note: this struct is aligned to consume less memory.
type ChannelContext struct {
	subscribingCh            chan struct{}
	info                     []byte
	streamPosition           StreamPosition
	expireAt                 int64
	positionCheckTime        int64
	metaTTLSeconds           int64
	flags                    uint16
	mapClientPresenceChannel string
	mapUserPresenceChannel   string
	// subGen identifies this subscription generation; it matches the subGen the
	// subscription carries in the hub. Unsubscribe reads it here and passes it to
	// hub removal so a stale unsubscribe cannot remove a newer resubscribe's hub
	// entry. See subInfo.subGen.
	subGen uint64
	// Source is a source of subscription application can set in SubscribeHandler.
	Source uint8
}

func channelHasFlag(flags, flag uint16) bool {
	return flags&flag != 0
}

type timerOp uint8

const (
	timerOpStale    timerOp = 1
	timerOpPresence timerOp = 2
	timerOpExpire   timerOp = 3
	timerOpPing     timerOp = 4
	timerOpPong     timerOp = 5
)

type status uint8

const (
	statusConnecting status = 1
	statusConnected  status = 2
	statusClosed     status = 3
)

// ConnectRequest can be used in a unidirectional connection case to
// pass initial connection information from a client-side.
type ConnectRequest struct {
	// Token is an optional token from a client.
	Token string
	// Data is an optional custom data from a client.
	Data []byte
	// Name of a client.
	Name string
	// Version of a client.
	Version string
	// Subs is a map with channel subscription state (for recovery on connect).
	Subs map[string]SubscribeRequest
	// Headers represent headers which may be used for headers emulation feature.
	Headers map[string]string
}

// SubscribeRequest contains state of subscription to a channel.
type SubscribeRequest struct {
	// Recover enables publication recovery for a channel.
	Recover bool
	// Epoch last seen by a client.
	Epoch string
	// Offset last seen by a client.
	Offset uint64
}

func (r *ConnectRequest) toProto() *protocol.ConnectRequest {
	if r == nil {
		return nil
	}
	req := &protocol.ConnectRequest{
		Token:   r.Token,
		Data:    r.Data,
		Name:    r.Name,
		Version: r.Version,
		Headers: r.Headers,
	}
	if len(r.Subs) > 0 {
		subs := make(map[string]*protocol.SubscribeRequest, len(r.Subs))
		for k, v := range r.Subs {
			subs[k] = &protocol.SubscribeRequest{
				Recover: v.Recover,
				Epoch:   v.Epoch,
				Offset:  v.Offset,
			}
		}
		req.Subs = subs
	}
	return req
}

// Client represents client connection to server.
type Client struct {
	mu                     sync.RWMutex
	connectMu              sync.Mutex    // allows syncing connect with disconnect.
	presenceMu             sync.Mutex    // allows syncing presence routine with client closing.
	presenceInFlight       atomic.Bool   // guards against overlapping presence ticks.
	closing                atomic.Bool   // set before close blocks on presenceMu.
	subGenCounter          atomic.Uint64 // monotonic per-subscription generation, see subInfo.subGen.
	ctx                    context.Context
	transport              Transport
	node                   *Node
	exp                    int64
	channels               map[string]ChannelContext
	messageWriter          *writer
	perChannelWriter       *perChannelWriter
	pubSubSync             *recovery.PubSubSync
	uid                    string
	session                string
	user                   string
	info                   []byte
	storage                map[string]any
	storageMu              sync.Mutex
	metricName             string // Make a unique.Handle.
	metricVersion          string // Make a unique.Handle.
	labels                 map[string]string
	labelCombinationCached atomic.Pointer[clientLabelCombination] // Cached pointer to shared label combination
	authenticated          bool
	clientSideRefresh      bool
	status                 status
	timerOp                timerOp
	nextPresence           int64
	nextExpire             int64
	nextPing               int64
	nextPong               int64
	lastSeen               int64
	lastPing               int64
	pingInterval           time.Duration
	pongTimeout            time.Duration
	eventHub               *clientEventHub
	timer                  *time.Timer
	timerCanceler          TimerCanceler // TimerCanceler if TimerScheduler is used.
	startWriterOnce        sync.Once
	pingPongLatency        atomic.Int64
	connectedAtMS          int64
	replyWithoutQueue      bool
	unusable               bool

	// mapSubscribing tracks map subscriptions that are still loading (not yet live).
	mapSubscribing map[string]*mapSubscribeState
	// mapPaginationLocks tracks channels currently being paginated to prevent concurrent pagination.
	mapPaginationLocks map[string]struct{}

	// keyed holds per-connection keyed subscription state (shared poll).
	// nil until first keyed subscribe.
	keyed *keyedState
}

// ClientCloseFunc must be called on Transport handler close to clean up Client.
type ClientCloseFunc func() error

// NewClient initializes new Client.
func NewClient(ctx context.Context, n *Node, t Transport) (*Client, ClientCloseFunc, error) {
	uidObject, err := uuid.NewRandom()
	if err != nil {
		return nil, nil, err
	}
	uid := uidObject.String()

	var session string
	if t.Unidirectional() || t.Emulation() {
		sessionObject, err := uuid.NewRandom()
		if err != nil {
			return nil, nil, err
		}
		session = sessionObject.String()
	}

	client := &Client{
		ctx:           ctx,
		uid:           uid,
		session:       session,
		node:          n,
		transport:     t,
		channels:      make(map[string]ChannelContext),
		pubSubSync:    recovery.NewPubSubSync(),
		status:        statusConnecting,
		eventHub:      &clientEventHub{},
		connectedAtMS: time.Now().UnixMilli(),
	}
	client.pingPongLatency.Store(-1)

	staleCloseDelay := n.config.ClientStaleCloseDelay
	if staleCloseDelay > 0 {
		client.mu.Lock()
		client.timerOp = timerOpStale
		if client.node.timerScheduler != nil {
			client.timerCanceler = client.node.timerScheduler.ScheduleTimer(staleCloseDelay, client.onTimerOp)
		} else {
			client.timer = time.AfterFunc(staleCloseDelay, client.onTimerOp)
		}
		client.mu.Unlock()
	}
	return client, func() error { return client.close(DisconnectConnectionClosed) }, nil
}

var defaultUniErrorCodeToDisconnect = map[uint32]Disconnect{
	ErrorExpired.Code:          DisconnectExpired,
	ErrorTokenExpired.Code:     DisconnectExpired,
	ErrorTooManyRequests.Code:  DisconnectTooManyRequests,
	ErrorPermissionDenied.Code: DisconnectPermissionDenied,
}

func (c *Client) extractUnidirectionalDisconnect(err error) Disconnect {
	disconnect, ok := disconnectFromError(err)
	if ok {
		return *disconnect
	}
	var clientErr *Error
	if errors.As(err, &clientErr) {
		if c.node.config.UnidirectionalCodeToDisconnect != nil {
			if d, found := c.node.config.UnidirectionalCodeToDisconnect[clientErr.Code]; found {
				return d
			}
		}
		if d, found := defaultUniErrorCodeToDisconnect[clientErr.Code]; found {
			return d
		}
		return DisconnectServerError
	}
	return DisconnectServerError
}

// Connect supposed to be called only from a unidirectional transport layer
// to pass initial information about connection and thus initiate Node.OnConnecting
// event. Bidirectional transport initiate connecting workflow automatically
// since client passes Connect command upon successful connection establishment
// with a server. If there is an error during connect method processing Centrifuge
// extracts Disconnect from it and closes the connection with that Disconnect message.
func (c *Client) Connect(req ConnectRequest) {
	c.ProtocolConnect(req.toProto())
}

// ConnectNoErrorToDisconnect is the same as Client.Connect but does not try to extract
// Disconnect code from the error returned by the connect logic, instead it just returns
// the error to the caller. This error must be handled by the caller on the Transport level,
// and the connection must be closed on Transport level upon receiving an error.
func (c *Client) ConnectNoErrorToDisconnect(req ConnectRequest) error {
	return c.ProtocolConnectNoErrorToDisconnect(req.toProto())
}

// ProtocolConnect accepts protocol.ConnectRequest directly. It adds dependency to protocol package,
// so prefer using Connect or ConnectNoErrorToDisconnect methods until necessary.
func (c *Client) ProtocolConnect(req *protocol.ConnectRequest) {
	// unidirectionalConnect never returns errors when errorToDisconnect is true.
	_ = c.unidirectionalConnect(req, req.SizeVT(), true)
}

// ProtocolConnectNoErrorToDisconnect accepts protocol.ConnectRequest directly. It adds dependency to
// protocol package, so prefer ConnectNoErrorToDisconnect methods until necessary.
func (c *Client) ProtocolConnectNoErrorToDisconnect(req *protocol.ConnectRequest) error {
	return c.unidirectionalConnect(req, req.SizeVT(), false)
}

func (c *Client) getDisconnectPushReply(d Disconnect) ([]byte, error) {
	disconnect := &protocol.Disconnect{
		Code:   d.Code,
		Reason: d.Reason,
	}
	push := &protocol.Push{
		Disconnect: disconnect,
	}
	if c.node.logEnabled(LogLevelTrace) {
		c.traceOutPush(push)
	}
	return c.encodeReply(&protocol.Reply{
		Push: push,
	})
}

func hasFlag(flags, flag uint64) bool {
	return flags&flag != 0
}

func (c *Client) issueCommandReadEvent(cmd *protocol.Command, size int) error {
	if c.node.clientEvents.commandReadHandler != nil {
		return c.node.clientEvents.commandReadHandler(c, CommandReadEvent{
			Command:     cmd,
			CommandSize: size,
		})
	}
	return nil
}

func (c *Client) issueCommandProcessedEvent(event CommandProcessedEvent) {
	if c.node.clientEvents.commandProcessedHandler != nil {
		c.node.clientEvents.commandProcessedHandler(c, event)
	}
}

func (c *Client) unidirectionalConnect(connectRequest *protocol.ConnectRequest, connectCmdSize int, errorToDisconnect bool) error {
	started := time.Now()

	var cmd *protocol.Command

	if c.node.logEnabled(LogLevelTrace) {
		cmd = &protocol.Command{Id: 1, Connect: connectRequest}
		c.traceInCmd(cmd)
	}

	if c.node.clientEvents.commandReadHandler != nil {
		cmd = &protocol.Command{Id: 1, Connect: connectRequest}
		err := c.issueCommandReadEvent(cmd, connectCmdSize)
		if err != nil {
			if c.node.clientEvents.commandProcessedHandler != nil {
				c.handleCommandFinished(cmd, protocol.FrameTypeConnect, err, nil, started, "")
			}
			if errorToDisconnect {
				d := c.extractUnidirectionalDisconnect(err)
				go func() { _ = c.close(d) }()
				return nil
			}
			return err
		}
	}
	err := c.connectCmd(connectRequest, nil, time.Time{}, nil)
	if err != nil {
		if c.node.clientEvents.commandProcessedHandler != nil {
			c.handleCommandFinished(cmd, protocol.FrameTypeConnect, err, nil, started, "")
		}
		if errorToDisconnect {
			d := c.extractUnidirectionalDisconnect(err)
			go func() { _ = c.close(d) }()
			return nil
		}
		return err
	}
	if c.node.clientEvents.commandProcessedHandler != nil {
		c.handleCommandFinished(cmd, protocol.FrameTypeConnect, nil, nil, started, "")
	}
	c.triggerConnect()
	c.scheduleOnConnectTimers()
	return nil
}

func (c *Client) onTimerOp() {
	c.mu.Lock()
	if c.status == statusClosed {
		c.mu.Unlock()
		return
	}
	op := c.timerOp
	c.mu.Unlock()

	// A TimerScheduler may run callbacks on a goroutine shared with other
	// connections (Centrifugo PRO's timer wheel runs a batch of a bucket's
	// callbacks per goroutine). Timer ops that can block on the network must not
	// run there, or one connection delays unrelated connections' pings:
	//
	//   - presence: one PresenceManager/MapBroker round trip per channel.
	//   - expire:   RefreshHandler, which applications commonly back with an HTTP
	//               call (Centrifugo's refresh proxy runs it inline unless
	//               client concurrency is configured).
	//   - stale:    close() -> unsubscribe -> presence removal round trips.
	//
	// Ping and pong stay inline: they only touch local state and the write queue,
	// and they are the frequent ops a batching scheduler exists to amortize.
	//
	// On the default path time.AfterFunc already invokes this callback on its own
	// goroutine, so nothing is spawned.
	offload := c.node.timerScheduler != nil

	switch op {
	case timerOpStale:
		if offload {
			go c.closeStale()
		} else {
			c.closeStale()
		}
	case timerOpPresence:
		// presenceInFlight is only a hint here — updatePresence re-checks it with a
		// CAS. It avoids spawning a goroutine just to discover a tick is already
		// running, which is the common case when PresenceManager is slow.
		if offload && !c.presenceInFlight.Load() {
			go c.updatePresence()
		} else {
			c.updatePresence()
		}
	case timerOpExpire:
		if offload {
			go c.expire()
		} else {
			c.expire()
		}
	case timerOpPing:
		c.sendPing()
	case timerOpPong:
		c.checkPong()
	}
}

// Lock must be held outside.
func (c *Client) scheduleNextTimer() {
	if c.status == statusClosed {
		return
	}
	c.stopTimer() // Cancel any existing timer.
	var minEventTime int64
	var nextTimerOp timerOp
	var needTimer bool
	if c.nextExpire > 0 {
		nextTimerOp = timerOpExpire
		minEventTime = c.nextExpire
		needTimer = true
	}
	if c.nextPresence > 0 && (minEventTime == 0 || c.nextPresence < minEventTime) {
		nextTimerOp = timerOpPresence
		minEventTime = c.nextPresence
		needTimer = true
	}
	if c.nextPing > 0 && (minEventTime == 0 || c.nextPing < minEventTime) {
		nextTimerOp = timerOpPing
		minEventTime = c.nextPing
		needTimer = true
	}
	if c.nextPong > 0 && (minEventTime == 0 || c.nextPong < minEventTime) {
		nextTimerOp = timerOpPong
		minEventTime = c.nextPong
		needTimer = true
	}
	if needTimer {
		c.timerOp = nextTimerOp
		afterDuration := time.Duration(minEventTime-time.Now().UnixNano()) * time.Nanosecond
		if c.node.timerScheduler != nil {
			c.timerCanceler = c.node.timerScheduler.ScheduleTimer(afterDuration, c.onTimerOp)
		} else {
			if c.timer != nil {
				c.timer.Reset(afterDuration)
			} else {
				c.timer = time.AfterFunc(afterDuration, c.onTimerOp)
			}
		}
	}
}

// Lock must be held outside.
func (c *Client) stopTimer() {
	if c.node.timerScheduler != nil {
		if c.timerCanceler != nil {
			c.timerCanceler.Cancel()
			c.timerCanceler = nil
		}
	} else if c.timer != nil {
		c.timer.Stop()
	}
}

func getPingData(uni bool, protoType ProtocolType) []byte {
	if uni {
		if protoType == ProtocolTypeJSON {
			return jsonPingPush
		}
		return protobufPingPush
	}
	if protoType == ProtocolTypeJSON {
		return jsonPingReply
	}
	return protobufPingReply
}

func (c *Client) sendPing() {
	c.mu.Lock()
	c.lastPing = time.Now().UnixNano()
	c.mu.Unlock()
	unidirectional := c.transport.Unidirectional()
	// TODO: can/should we write pings directly without going through messageWriter?
	//err := c.messageWriter.config.WriteFn(queue.Item{
	//	Data:      getPingData(unidirectional, c.transport.Protocol()),
	//	FrameType: protocol.FrameTypeServerPing,
	//})
	//if err != nil {
	//	go func() { _ = c.close(DisconnectWriteError) }()
	//	return
	//}
	_ = c.writeEncodedPushData(getPingData(unidirectional, c.transport.Protocol()), "", "", protocol.FrameTypeServerPing, ChannelBatchConfig{})
	if c.node.logEnabled(LogLevelTrace) {
		c.traceOutReply(emptyReply)
	}
	c.mu.Lock()
	if c.pongTimeout > 0 && !unidirectional {
		c.nextPong = time.Now().Add(c.pongTimeout).UnixNano()
	}
	c.addPingUpdate(false, true)
	c.mu.Unlock()
}

func (c *Client) checkPong() {
	c.mu.RLock()
	lastPing := c.lastPing
	if lastPing < 0 {
		lastPing = -lastPing
	}
	lastSeen := c.lastSeen
	c.mu.RUnlock()
	if lastSeen < lastPing {
		go func() { c.Disconnect(DisconnectNoPong) }()
		return
	}
	diff := lastSeen - lastPing
	c.node.metrics.observePingPongDuration(time.Duration(diff)*time.Nanosecond, c.transport.Name())
	c.pingPongLatency.Store(diff)
	c.mu.Lock()
	c.nextPong = 0
	c.scheduleNextTimer()
	c.mu.Unlock()
}

// Lock must be held outside.
func (c *Client) addPingUpdate(isFirst bool, scheduleNext bool) {
	delay := c.pingInterval
	if isFirst {
		// Send first ping in random interval between PingInterval/2 and PingInterval to
		// spread ping-pongs in time (useful when many connections reconnect
		// almost immediately).
		pingNanoseconds := c.pingInterval.Nanoseconds()
		delay = time.Duration(pingNanoseconds/2) + time.Duration(randSource.Int63n(pingNanoseconds/2))*time.Nanosecond
	}
	c.nextPing = time.Now().Add(delay).UnixNano()
	if scheduleNext {
		c.scheduleNextTimer()
	}
}

// Lock must be held outside.
func (c *Client) addPresenceUpdate(isFirst bool, scheduleNext bool) {
	delay := c.node.config.ClientPresenceUpdateInterval
	if isFirst {
		// Spread in time first presence update.
		intervalNanoseconds := c.node.config.ClientPresenceUpdateInterval.Nanoseconds()
		delay = time.Duration(intervalNanoseconds/2) + time.Duration(randSource.Int63n(intervalNanoseconds/2))*time.Nanosecond
	}
	c.nextPresence = time.Now().Add(delay).UnixNano()
	if scheduleNext {
		c.scheduleNextTimer()
	}
}

// Lock must be held outside.
func (c *Client) addExpireUpdate(after time.Duration, scheduleNext bool) {
	c.nextExpire = time.Now().Add(after).UnixNano()
	if scheduleNext {
		c.scheduleNextTimer()
	}
}

// closeStale closes connection if it's not authenticated yet, or it's
// unusable but still not closed. At moment used to close client connections
// which have not sent valid connect command in a reasonable time interval after
// establishing connection with a server.
func (c *Client) closeStale() {
	c.mu.RLock()
	authenticated := c.authenticated
	unusable := c.unusable
	closed := c.status == statusClosed
	c.mu.RUnlock()
	if (!authenticated || unusable) && !closed {
		_ = c.close(DisconnectStale)
	}
}

func (c *Client) writeEncodedPushData(data []byte, ch string, key string, frameType protocol.FrameType, batchConfig ChannelBatchConfig) error {
	item := queue.Item{
		Data:      data,
		Key:       key,
		FrameType: frameType,
	}
	if c.node.config.Metrics.GetChannelNamespaceLabel != nil {
		item.Channel = ch
	}
	if ch != "" && (batchConfig.MaxSize > 0 || batchConfig.MaxDelay > 0) &&
		(item.FrameType == protocol.FrameTypePushPublication ||
			item.FrameType == protocol.FrameTypePushJoin ||
			item.FrameType == protocol.FrameTypePushLeave) {
		// Per channel writer helps to batch messages on the channel level working as
		// an intermediary buffer before client's connection writer.
		c.perChannelWriter.Add(item, ch, batchConfig)
		return nil
	}
	disconnect := c.messageWriter.enqueue(item)
	if disconnect != nil {
		// close in goroutine to not block message broadcast.
		c.spawnCloseUnlessClosing(*disconnect)
		return io.EOF
	}
	return nil
}

// spawnCloseUnlessClosing starts close() in a goroutine for a broadcast-path
// write failure, unless a close is already running. The guard matters because a
// closing connection stays in the hub's subscription shards until close() gets
// to the per-channel cleanup - and that cleanup makes PresenceManager/Broker
// round trips, so under slow Redis the window is long. Every publication landing
// in it fails to enqueue (the writer is already closed) and would otherwise
// spawn a goroutine that just blocks on presenceMu until the in-flight close
// finishes: one goroutine per message per closing connection, worst exactly when
// Redis is slowest. The in-flight close already performs the teardown, so there
// is nothing for these to do.
func (c *Client) spawnCloseUnlessClosing(disconnect Disconnect) {
	if c.closing.Load() {
		return
	}
	go func() { _ = c.close(disconnect) }()
}

// publishJoinAndPresence publishes join notification and sets up map presence
// for a channel after subscribe. Must be called with non-nil clientInfo.
func (c *Client) publishJoinAndPresence(channel string, chCtx ChannelContext, clientInfo *ClientInfo) {
	if channelHasFlag(chCtx.flags, flagEmitJoinLeave) {
		_ = c.node.publishJoin(channel, clientInfo)
	}
	if chCtx.mapClientPresenceChannel != "" {
		if err := c.addMapClientPresence(chCtx.mapClientPresenceChannel, clientInfo); err != nil {
			c.node.logger.log(newErrorLogEntry(err, "error adding map client presence", map[string]any{
				"channel": channel, "user": c.user, "client": c.uid,
			}))
		}
	}
	if chCtx.mapUserPresenceChannel != "" {
		if err := c.addMapUserPresence(chCtx.mapUserPresenceChannel); err != nil {
			c.node.logger.log(newErrorLogEntry(err, "error adding map user presence", map[string]any{
				"channel": channel, "user": c.user, "client": c.uid,
			}))
		}
	}
}

// updateChannelPresence updates client presence info for channel so it
// won't expire until client disconnect.
//
// attempted reports whether any presence add was issued: once true, an entry
// may exist even when err is non-nil (the node-level add can succeed before the
// map presence update fails, and a failed call may still have landed
// server-side), so the caller must treat the channel as compensation-eligible.
func (c *Client) updateChannelPresence(ch string, chCtx ChannelContext) (attempted bool, err error) {
	// Check if any presence is enabled for this channel.
	hasAnyPresence := channelHasFlag(chCtx.flags, flagEmitPresence) ||
		channelHasFlag(chCtx.flags, flagMapClientPresence) ||
		channelHasFlag(chCtx.flags, flagMapUserPresence)
	if !hasAnyPresence {
		return false, nil
	}

	c.mu.RLock()
	if _, ok := c.channels[ch]; !ok {
		c.mu.RUnlock()
		return false, nil
	}
	c.mu.RUnlock()

	info := &ClientInfo{
		ClientID: c.uid,
		UserID:   c.user,
		ConnInfo: c.info,
		ChanInfo: chCtx.info,
	}

	if channelHasFlag(chCtx.flags, flagEmitPresence) {
		err := c.node.addPresence(ch, c.uid, info)
		if err != nil {
			return true, err
		}
	}

	if channelHasFlag(chCtx.flags, flagMapClientPresence) || channelHasFlag(chCtx.flags, flagMapUserPresence) {
		err := c.updateMapPresence(info, chCtx)
		if err != nil {
			return true, err
		}
	}

	return true, nil
}

// tickDutyParams carries the values a duty needs from Config. It exists so the
// duty runner takes no callback: a closure would capture Config, and copying
// that struct to the heap on every tick of every connection costs more than the
// work it dispatches.
type tickDutyParams struct {
	positionCheckDelay time.Duration
}

// runTickDutyOn performs a single duty for one channel. Duties are dispatched by
// constant rather than by function value so the sequential path stays
// allocation-free.
func (c *Client) runTickDutyOn(duty tickDuty, item *channelTickItem, params tickDutyParams) {
	switch duty {
	case dutyPresence:
		c.updateChannelPresenceItem(item)
	case dutyPosition:
		item.positionInvalid = !c.checkPosition(params.positionCheckDelay, item.channel, item.ctx)
	}
}

// runTickDuty performs duty for every snapshot entry carrying it, with at most
// concurrency of them in flight.
//
// With concurrency <= 1 — the default — this is a plain sequential loop: no
// counting pass, no goroutines, no allocations. Connections with a single
// channel carrying the duty also stay sequential, since there is nothing to
// overlap. Each entry is only ever touched by one worker, so a duty may write to
// its item without synchronization.
func (c *Client) runTickDuty(snapshot []channelTickItem, duty tickDuty, concurrency int, params tickDutyParams) {
	if concurrency <= 1 {
		c.runTickDutySequential(snapshot, duty, params)
		return
	}

	var count int
	for i := range snapshot {
		if snapshot[i].duties&duty != 0 {
			count++
		}
	}
	if count == 0 {
		return
	}
	if count == 1 || concurrency > count {
		if count == 1 {
			c.runTickDutySequential(snapshot, duty, params)
			return
		}
		concurrency = count
	}

	var next atomic.Int64
	var wg sync.WaitGroup
	wg.Add(concurrency)
	for w := 0; w < concurrency; w++ {
		go func() {
			defer wg.Done()
			for {
				i := int(next.Add(1)) - 1
				if i >= len(snapshot) {
					return
				}
				if c.closing.Load() {
					return
				}
				if snapshot[i].duties&duty != 0 {
					c.runTickDutyOn(duty, &snapshot[i], params)
				}
			}
		}()
	}
	wg.Wait()
}

func (c *Client) runTickDutySequential(snapshot []channelTickItem, duty tickDuty, params tickDutyParams) {
	for i := range snapshot {
		if c.closing.Load() {
			return
		}
		if snapshot[i].duties&duty != 0 {
			c.runTickDutyOn(duty, &snapshot[i], params)
		}
	}
}

func (c *Client) updateChannelPresenceItem(item *channelTickItem) {
	attempted, err := c.updateChannelPresence(item.channel, item.ctx)
	if err != nil {
		c.node.logger.log(newErrorLogEntry(err, "error updating presence for channel", map[string]any{
			"channel": item.channel, "user": c.user, "client": c.uid, "error": err.Error()}))
	}
	// Marked even on error: a partial success (node-level add landed, map
	// presence update failed) must still be compensated if the channel was
	// concurrently unsubscribed — removal is idempotent, so over-marking is safe.
	item.presenceAdded = attempted
}

// compensateRacedPresence undoes presence entries this tick added for channels
// that were concurrently unsubscribed.
//
// unsubscribe() deletes from c.channels first and removes presence only after
// that, and — unlike close() — it does not take presenceMu. So an add issued by
// the tick can land after that removal and leave an entry behind until
// PresenceTTL (60s by default). The existence check updateChannelPresence makes
// before adding is not enough: it happens before a network call.
//
// The membership re-check is done once for the whole snapshot rather than per
// channel, so a tick pays a single RLock instead of one per channel for a race
// that almost never fires.
func (c *Client) compensateRacedPresence(snapshot []channelTickItem) {
	var raced bool
	c.mu.RLock()
	for i := range snapshot {
		if !snapshot[i].presenceAdded {
			continue
		}
		if _, ok := c.channels[snapshot[i].channel]; !ok {
			snapshot[i].raced = true
			raced = true
		}
	}
	c.mu.RUnlock()
	if !raced {
		return
	}
	for i := range snapshot {
		if snapshot[i].raced {
			c.removeRacedPresence(snapshot[i].channel, snapshot[i].ctx)
		}
	}
}

// removeRacedPresence undoes presence entries added by a presence tick for a
// channel that was concurrently unsubscribed. It mirrors exactly what
// updateChannelPresence adds — no more — so it stays symmetric with
// removeMapPresence.
//
// Removing twice (here and in unsubscribe) is harmless: removal is idempotent.
// If a fast re-subscribe raced this removal the entry can be dropped while the
// client is subscribed, but the next presence tick re-adds it, and unsubscribe's
// own removePresence already has that same race with a re-subscribe today.
func (c *Client) removeRacedPresence(ch string, chCtx ChannelContext) {
	if channelHasFlag(chCtx.flags, flagEmitPresence) {
		if err := c.node.removePresence(ch, c.uid, c.user); err != nil {
			c.node.logger.log(newErrorLogEntry(err, "error removing raced channel presence", map[string]any{
				"channel": ch, "user": c.user, "client": c.uid, "error": err.Error()}))
		}
	}
	if chCtx.mapClientPresenceChannel != "" {
		if _, err := c.node.MapRemove(context.Background(), chCtx.mapClientPresenceChannel, c.uid, MapRemoveOptions{}); err != nil {
			c.node.logger.log(newErrorLogEntry(err, "error removing raced map client presence", map[string]any{
				"channel": ch, "user": c.user, "client": c.uid, "error": err.Error()}))
		}
	}
	// User presence is intentionally not removed: unsubscribe does not remove it
	// either — it expires via TTL as a debounce for quick reconnects.
}

// Context returns client Context. This context will be canceled
// as soon as client connection closes.
func (c *Client) Context() context.Context {
	return c.ctx
}

func (c *Client) checkSubscriptionExpiration(channel string, channelContext ChannelContext, delay time.Duration, resultCB func(bool)) {
	nowUnix := c.node.nowTimeGetter().Unix()
	expireAt := channelContext.expireAt
	clientSideRefresh := channelHasFlag(channelContext.flags, flagClientSideRefresh)
	if expireAt > 0 && nowUnix > expireAt+int64(delay.Seconds()) {
		// Subscription expired.
		if clientSideRefresh || c.eventHub.subRefreshHandler == nil {
			// The only way subscription could be refreshed in this case is via
			// SUB_REFRESH command sent from client but looks like that command
			// with new refreshed token have not been received in configured window.
			resultCB(false)
			return
		}
		cb := func(reply SubRefreshReply, err error) {
			if err != nil {
				resultCB(false)
				return
			}
			newExpireAt := min(reply.ExpireAt, nowUnix+maxTTLSeconds)
			if reply.Expired || (newExpireAt > 0 && newExpireAt < nowUnix) {
				resultCB(false)
				return
			}
			c.mu.Lock()
			// Gen-match the write-back: subRefreshHandler is async, so an
			// unsubscribe+resubscribe in the window installs a fresh subGen. This
			// refresh was validated against channelContext.subGen — applying its
			// expireAt/info to a newer subscription would corrupt one its token never
			// validated (mirrors handleSubRefresh and checkPosition).
			if ctx, ok := c.channels[channel]; ok && ctx.subGen == channelContext.subGen {
				if len(reply.Info) > 0 {
					ctx.info = reply.Info
				}
				ctx.expireAt = newExpireAt
				c.channels[channel] = ctx
			}
			c.mu.Unlock()
			resultCB(true)
		}
		// Give subscription a chance to be refreshed via SubRefreshHandler.
		event := SubRefreshEvent{Channel: channel}
		c.eventHub.subRefreshHandler(event, cb)
		return
	}
	resultCB(true)
}

// tickDuty is a bitmask of the periodic work a channel needs on a presence tick.
type tickDuty uint8

const (
	dutyPresence tickDuty = 1 << iota
	dutySubExpire
	dutyTrackExpire
	dutyPosition
)

// channelTickItem is a channel snapshot entry used by the periodic presence tick.
type channelTickItem struct {
	channel string
	ctx     ChannelContext
	duties  tickDuty
	// presenceAdded records that this tick issued a presence add for the channel,
	// so a racing unsubscribe can be compensated for. See compensateRacedPresence.
	presenceAdded bool
	raced         bool
	// positionInvalid records the outcome of the stream position check. The check
	// itself may run concurrently; acting on the result (unsubscribe or
	// disconnect) is left to the sequential pass.
	positionInvalid bool
}

var presenceSnapshotPool = sync.Pool{
	New: func() any {
		s := make([]channelTickItem, 0, 16)
		return &s
	},
}

// channelTickDuties is the single source of truth for what the periodic tick
// does to a channel: it decides both whether a channel is worth snapshotting
// (duties != 0) and which steps the tick loop then runs for it. The loop must
// key off the returned mask rather than re-deriving these conditions — deriving
// them twice lets the two drift, and a channel silently dropped from the
// snapshot fails no test.
//
// The config-dependent conditions are passed in as booleans: they are the same
// for every channel, and Config is a large struct that must not be copied per
// channel.
func channelTickDuties(chCtx ChannelContext, positionCheckEnabled, sharedPollEnabled bool) tickDuty {
	var duties tickDuty
	if channelHasFlag(chCtx.flags, flagEmitPresence|flagMapClientPresence|flagMapUserPresence) {
		duties |= dutyPresence
	}
	if chCtx.expireAt > 0 {
		duties |= dutySubExpire
	}
	if channelHasFlag(chCtx.flags, flagKeyed) && sharedPollEnabled {
		duties |= dutyTrackExpire
	}
	if channelHasFlag(chCtx.flags, flagPositioning) && positionCheckEnabled {
		duties |= dutyPosition
	}
	return duties
}

// updatePresence used for various periodic actions we need to do with client connections.
func (c *Client) updatePresence() {
	// A previous tick may still be draining PresenceManager calls. Skip this one
	// rather than queueing on presenceMu: the timer callback may run on a shared
	// goroutine (see TimerScheduler), and blocking it would stall unrelated
	// connections. Re-arm so ticks keep going.
	if !c.presenceInFlight.CompareAndSwap(false, true) {
		c.mu.Lock()
		if c.status != statusClosed {
			c.addPresenceUpdate(false, true)
		}
		c.mu.Unlock()
		return
	}
	defer c.presenceInFlight.Store(false)

	c.presenceMu.Lock()
	defer c.presenceMu.Unlock()
	config := c.node.config
	c.mu.Lock()
	unusable := c.unusable
	if c.status == statusClosed {
		c.mu.Unlock()
		return
	}
	// Re-arm before doing any work. Presence updates below call into
	// PresenceManager/MapBroker, which may perform network round trips, and the
	// client's single timer is shared with ping. Arming after the loop would
	// delay the ping by the whole loop duration. Arming here also gives a fixed
	// cadence rather than one that drifts by the loop duration each tick.
	c.addPresenceUpdate(false, true)
	// Snapshot channels that actually need periodic work. A snapshot is required
	// because the loop below calls into PresenceManager/MapBroker and we must not
	// hold c.mu across those. A pooled slice is used instead of a map: it needs
	// no hashing and no bucket allocation, and ticks are serialized per client
	// (presenceInFlight + presenceMu) and spread across connections, so the pool
	// holds roughly one entry per concurrently running tick rather than one per
	// connection.
	positionCheckEnabled := config.ClientChannelPositionCheckDelay > 0
	sharedPollEnabled := config.SharedPoll.GetSharedPollChannelOptions != nil
	snapshotPtr := presenceSnapshotPool.Get().(*[]channelTickItem)
	snapshot := (*snapshotPtr)[:0]
	for channel, channelContext := range c.channels {
		if !channelHasFlag(channelContext.flags, flagSubscribed) {
			continue
		}
		duties := channelTickDuties(channelContext, positionCheckEnabled, sharedPollEnabled)
		if duties == 0 {
			continue
		}
		snapshot = append(snapshot, channelTickItem{channel: channel, ctx: channelContext, duties: duties})
	}
	c.mu.Unlock()
	defer func() {
		// Clear before pooling so the retained backing array does not keep
		// ChannelContext strings/byte slices alive.
		clear(snapshot)
		*snapshotPtr = snapshot[:0]
		presenceSnapshotPool.Put(snapshotPtr)
	}()
	// Runs even when the loop below bails early (client closing), otherwise a
	// presence entry resurrected by a racing unsubscribe would survive: close()
	// only removes presence for channels still in c.channels.
	defer c.compensateRacedPresence(snapshot)

	if unusable {
		go c.closeStale()
		return
	}

	if c.eventHub.aliveHandler != nil {
		c.eventHub.aliveHandler()
	}

	// Presence updates and stream position checks are independent per channel and
	// are pure network calls — one round trip each — so they may run with a
	// bounded number in flight, which is what lets the Redis client pipeline them
	// instead of paying channels * RTT. Their consequences are not run here:
	// deciding what an invalid position means (unsubscribe or disconnect) happens
	// sequentially in the loop below, together with the duties that invoke
	// application callbacks.
	dutyParams := tickDutyParams{positionCheckDelay: config.ClientChannelPositionCheckDelay}
	c.runTickDuty(snapshot, dutyPresence, config.clientPresenceUpdateConcurrency, dutyParams)
	c.runTickDuty(snapshot, dutyPosition, config.clientPositionCheckConcurrency, dutyParams)

	for i := range snapshot {
		item := &snapshot[i]
		channel, channelContext := item.channel, item.ctx
		// Client is closing — stop issuing work so close() does not wait for the
		// rest of the loop to drain.
		if c.closing.Load() {
			return
		}

		if item.duties&dutySubExpire != 0 {
			c.checkSubscriptionExpiration(channel, channelContext, config.ClientExpiredSubCloseDelay, func(result bool) {
				if !result {
					serverSide := channelHasFlag(channelContext.flags, flagServerSide)
					if c.isAsyncUnsubscribe(serverSide) {
						go func(ch string) { c.handleAsyncUnsubscribe(ch, unsubscribeExpired) }(channel)
					} else {
						go func() { _ = c.close(DisconnectSubExpired) }()
					}
				}
			})
		}

		if item.duties&dutyTrackExpire != 0 {
			if spOpts, ok := config.SharedPoll.GetSharedPollChannelOptions(channel); ok {
				trackExpiredDelay := spOpts.TrackExpiredExtraDelay
				if trackExpiredDelay <= 0 {
					trackExpiredDelay = 25 * time.Second
				}
				c.checkTrackExpiration(channel, trackExpiredDelay)
			}
		}

		if item.positionInvalid {
			serverSide := channelHasFlag(channelContext.flags, flagServerSide)
			if c.node.logger.enabled(LogLevelDebug) {
				c.node.logger.log(newLogEntry(LogLevelDebug, "client insufficient state from periodic check", map[string]any{"channel": channel, "user": c.user, "client": c.uid}))
			}
			if c.isAsyncUnsubscribe(serverSide) {
				go func(ch string) { c.handleAsyncUnsubscribe(ch, unsubscribeInsufficientState) }(channel)
				continue
			} else {
				go func() { c.handleInsufficientStateDisconnect() }()
				// No need to proceed after close.
				return
			}
		}
	}
}

func (c *Client) checkPosition(checkDelay time.Duration, ch string, chCtx ChannelContext) bool {
	if !channelHasFlag(chCtx.flags, flagPositioning) {
		return true
	}
	nowUnix := c.node.nowTimeGetter().Unix()

	needCheckPosition := nowUnix-chCtx.positionCheckTime > int64(checkDelay.Seconds())
	if !needCheckPosition {
		return true
	}

	var historyMetaTTL time.Duration
	if chCtx.metaTTLSeconds > 0 {
		historyMetaTTL = time.Duration(chCtx.metaTTLSeconds) * time.Second
	}

	c.mu.Lock()
	if c.status == statusClosed {
		c.mu.Unlock()
		return true
	}
	chCtx, ok := c.channels[ch]
	if !ok || !channelHasFlag(chCtx.flags, flagSubscribed) {
		c.mu.Unlock()
		return true
	}
	position := chCtx.streamPosition
	checkedSubGen := chCtx.subGen
	c.mu.Unlock()

	isMap := channelHasFlag(chCtx.flags, flagMap)
	validPosition, err := c.node.checkPosition(ch, position, historyMetaTTL, isMap)
	if err != nil {
		// Check later.
		return true
	}
	if validPosition {
		c.mu.Lock()
		// Only stamp the generation we actually validated: a resubscribe in the
		// window installs a new subGen with its own unvalidated position, and
		// marking that as freshly checked would defer its first real check.
		if chContext, ok := c.channels[ch]; ok && chContext.subGen == checkedSubGen {
			chContext.positionCheckTime = nowUnix
			c.channels[ch] = chContext
		}
		c.mu.Unlock()
	}
	return validPosition
}

// ID returns unique client connection id.
func (c *Client) ID() string {
	return c.uid
}

// sessionID returns unique client session id. Session ID is not shared to other
// connections in any way.
func (c *Client) sessionID() string {
	return c.session
}

// UserID returns user id associated with client connection.
func (c *Client) UserID() string {
	return c.user
}

// Info returns connection info.
func (c *Client) Info() []byte {
	c.mu.Lock()
	info := make([]byte, len(c.info))
	copy(info, c.info)
	c.mu.Unlock()
	return info
}

// ConnectedAtMS returns timestamp in milliseconds when client connected.
func (c *Client) ConnectedAtMS() int64 {
	return c.connectedAtMS
}

// LatestPingPongLatency returns latest ping-pong latency duration. It may be not
// available if no ping-pong messages were exchanged yet, or in case of unidirectional
// transport. In that case second return value will be false.
func (c *Client) LatestPingPongLatency() (time.Duration, bool) {
	val := c.pingPongLatency.Load()
	if val < 0 {
		// If ping-pong latency is negative then it means that we have not sent
		// any ping yet, and thus we do not have any latency info. Or in case of
		// unidirectional connection we do not have this info also.
		return 0, false
	}
	return time.Duration(c.pingPongLatency.Load()) * time.Nanosecond, true
}

// Transport returns client connection transport information.
func (c *Client) Transport() TransportInfo {
	return c.transport
}

// Channels returns a slice of channels client connection currently subscribed to.
func (c *Client) Channels() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	channels := make([]string, 0, len(c.channels))
	for ch, ctx := range c.channels {
		if !channelHasFlag(ctx.flags, flagSubscribed) {
			continue
		}
		channels = append(channels, ch)
	}
	return channels
}

// ChannelsWithContext returns a map of channels client connection currently subscribed to
// with a ChannelContext.
func (c *Client) ChannelsWithContext() map[string]ChannelContext {
	c.mu.RLock()
	defer c.mu.RUnlock()
	channels := make(map[string]ChannelContext, len(c.channels))
	for ch, ctx := range c.channels {
		if !channelHasFlag(ctx.flags, flagSubscribed) {
			continue
		}
		channels[ch] = ctx
	}
	return channels
}

// IsSubscribed returns true if client subscribed to a channel.
func (c *Client) IsSubscribed(ch string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	ctx, ok := c.channels[ch]
	return ok && channelHasFlag(ctx.flags, flagSubscribed)
}

// Labels returns custom labels attached to the client connection.
// These labels are set via ConnectReply.Labels. The returned map must not be modified
// by the caller - use it for reading only. Labels are set once during connect and never
// change.
func (c *Client) Labels() map[string]string {
	return c.labels
}

// Send data to client. This sends an asynchronous message – data will be
// just written to connection. on client side this message can be handled
// with Message handler.
func (c *Client) Send(data []byte) error {
	if hasFlag(c.transport.DisabledPushFlags(), PushFlagMessage) {
		return nil
	}
	replyData, err := c.getSendPushReply(data)
	if err != nil {
		return err
	}
	return c.writeEncodedPushData(replyData, "", "", protocol.FrameTypePushMessage, ChannelBatchConfig{})
}

func (c *Client) encodeReply(reply *protocol.Reply) ([]byte, error) {
	protoType := c.transport.Protocol().toProto()
	if c.transport.Unidirectional() {
		encoder := protocol.GetPushEncoder(protoType)
		return encoder.Encode(reply.Push)
	}
	encoder := protocol.GetReplyEncoder(protoType)
	return encoder.Encode(reply)
}

func (c *Client) getSendPushReply(data []byte) ([]byte, error) {
	p := &protocol.Message{
		Data: data,
	}
	return c.encodeReply(&protocol.Reply{
		Push: &protocol.Push{
			Message: p,
		},
	})
}

// Unsubscribe allows unsubscribing client from channel.
func (c *Client) Unsubscribe(ch string, unsubscribe ...Unsubscribe) {
	if len(unsubscribe) > 1 {
		panic("Client.Unsubscribe called with more than 1 unsubscribe argument")
	}
	c.mu.RLock()
	if c.status == statusClosed {
		c.mu.RUnlock()
		return
	}
	c.mu.RUnlock()

	unsub := unsubscribeServer
	if len(unsubscribe) > 0 {
		unsub = unsubscribe[0]
	}

	err := c.unsubscribe(ch, unsub, nil)
	if err != nil {
		c.node.logger.log(newErrorLogEntry(err, "error unsubscribe", map[string]any{"channel": ch, "user": c.user, "client": c.uid, "error": err.Error()}))
		go c.Disconnect(DisconnectServerError)
		return
	}
	_ = c.sendUnsubscribe(ch, unsub)
}

func (c *Client) sendUnsubscribe(ch string, unsub Unsubscribe) error {
	if hasFlag(c.transport.DisabledPushFlags(), PushFlagUnsubscribe) {
		return nil
	}
	replyData, err := c.getUnsubscribePushReply(ch, unsub)
	if err != nil {
		return err
	}
	_ = c.writeEncodedPushData(replyData, ch, "", protocol.FrameTypePushUnsubscribe, ChannelBatchConfig{})
	c.node.metrics.incServerUnsubscribe(unsub.Code, ch, c)
	return nil
}

func (c *Client) getUnsubscribePushReply(ch string, unsub Unsubscribe) ([]byte, error) {
	p := &protocol.Unsubscribe{
		Code:   unsub.Code,
		Reason: unsub.Reason,
	}
	return c.encodeReply(&protocol.Reply{
		Push: &protocol.Push{
			Channel:     ch,
			Unsubscribe: p,
		},
	})
}

// Disconnect client connection with specific disconnect code and reason.
// If zero args or nil passed then DisconnectForceNoReconnect is used.
//
// This method internally creates a new goroutine at the moment to do
// closing stuff. An extra goroutine is required to solve disconnect
// and alive callback ordering/sync problems. Will be a noop if client
// already closed. As this method runs a separate goroutine client
// connection will be closed eventually (i.e. not immediately).
func (c *Client) Disconnect(disconnect ...Disconnect) {
	if len(disconnect) > 1 {
		panic("Client.Disconnect called with more than 1 argument")
	}
	go func() {
		if len(disconnect) == 0 {
			_ = c.close(DisconnectForceNoReconnect)
		} else {
			_ = c.close(disconnect[0])
		}
	}()
}

func (c *Client) close(disconnect Disconnect) error {
	c.startWriter(0, 0, 0, 0, false)
	// Signal an in-flight presence tick to stop: that tick may be draining many
	// PresenceManager round trips, and the cleanup below waits for all of them
	// on presenceMu.
	c.closing.Store(true)
	// presenceMu is deliberately NOT taken here - it is taken further down, once
	// the transport is closed. Taking it up front would put an in-flight presence
	// tick's Redis round trip (or, worse, its timeout) in front of the socket
	// teardown, which is what we are trying to keep the teardown clear of. Only
	// close() takes both mutexes, so this connectMu -> presenceMu order cannot
	// invert against anything else.
	c.connectMu.Lock()
	defer c.connectMu.Unlock()
	c.mu.Lock()
	if c.status == statusClosed {
		c.mu.Unlock()
		return nil
	}
	prevStatus := c.status
	// Capture c.user under c.mu: connectCmd writes it under c.mu, and close can
	// race connectCmd (e.g. the stale-connection timer firing during a slow
	// connect), so the disconnect logs below must not read it lock-free.
	user := c.user
	authenticated := c.authenticated
	c.status = statusClosed

	c.stopTimer()

	channels := make(map[string]ChannelContext, len(c.channels))
	for channel, channelContext := range c.channels {
		channels[channel] = channelContext
	}
	c.mu.Unlock()

	// Tear the transport down FIRST, before the per-channel cleanup below.
	// Presence removal and leave publishing (in the unsubscribe loop) are
	// synchronous PresenceManager/Broker round trips; under load they can be
	// slow, and if the socket were closed only after them the connection would
	// linger on the server while a reconnecting client that already gave up
	// opens a new one - producing several concurrent connections per client
	// (centrifuge-js #371). None of the cleanup below writes to the transport,
	// so closing it here is safe and keeps teardown off the Redis-bound path.
	if authenticated {
		c.node.removeClient(c)
	}

	if disconnect.Code != DisconnectConnectionClosed.Code && !hasFlag(c.transport.DisabledPushFlags(), PushFlagDisconnect) {
		if replyData, err := c.getDisconnectPushReply(disconnect); err == nil {
			_ = c.writeEncodedPushData(replyData, "", "", protocol.FrameTypePushDisconnect, ChannelBatchConfig{})
		}
	}

	// close writer and send messages remaining in writer queue if any.
	flushRemaining := disconnect.Code != DisconnectConnectionClosed.Code && disconnect.Code != DisconnectSlow.Code
	if c.perChannelWriter != nil {
		c.perChannelWriter.Close(flushRemaining)
	}
	_ = c.messageWriter.close(flushRemaining)

	_ = c.transport.Close(disconnect)

	// Transport is closed; do the (potentially slow) channel cleanup now, off the
	// connection's critical path. Only now do we block on presenceMu: the cleanup
	// removes presence entries, so it must not overlap an in-flight tick that is
	// still adding them. c.closing is already set, so that tick stops at its next
	// per-channel check instead of draining every round trip.
	c.presenceMu.Lock()
	defer c.presenceMu.Unlock()

	// Unsubscribe from all channels (handles both normal and map subscriptions).
	for channel := range channels {
		err := c.unsubscribe(channel, unsubscribeDisconnect, &disconnect)
		if err != nil {
			c.node.logger.log(newErrorLogEntry(err, "error unsubscribing client from channel", map[string]any{"channel": channel, "user": user, "client": c.uid, "error": err.Error()}))
		}
	}

	// Clean up any in-progress map subscriptions that aren't in channels yet.
	c.cleanupMapSubscribingAll()

	if disconnect.Code != DisconnectConnectionClosed.Code {
		c.node.logger.log(newLogEntry(LogLevelDebug, "closing client connection", map[string]any{"client": c.uid, "user": user, "reason": disconnect.Reason}))
	}
	if disconnect.Code != DisconnectConnectionClosed.Code {
		c.node.metrics.incServerDisconnect(disconnect.Code, c)
	}
	if c.eventHub.disconnectHandler != nil && prevStatus == statusConnected {
		c.eventHub.disconnectHandler(DisconnectEvent{
			Disconnect: disconnect,
		})
	}
	return nil
}

func (c *Client) traceInCmd(cmd *protocol.Command) {
	c.mu.RLock()
	user := c.user
	c.mu.RUnlock()
	jsonBytes, err := json.Marshal(cmd)
	if err != nil {
		jsonBytes, _ = protojson.Marshal(cmd)
	}
	c.node.logger.log(newLogEntry(LogLevelTrace, "<-in--", map[string]any{"client": c.ID(), "user": user, "command": string(jsonBytes)}))
}

func (c *Client) traceOutReply(rep *protocol.Reply) {
	c.mu.RLock()
	user := c.user
	c.mu.RUnlock()
	jsonBytes, err := json.Marshal(rep)
	if err != nil {
		jsonBytes, _ = protojson.Marshal(rep)
	}
	c.node.logger.log(newLogEntry(LogLevelTrace, "-out->", map[string]any{"client": c.ID(), "user": user, "reply": string(jsonBytes)}))
}

func (c *Client) traceOutPush(push *protocol.Push) {
	c.mu.RLock()
	user := c.user
	c.mu.RUnlock()
	jsonBytes, err := json.Marshal(push)
	if err != nil {
		jsonBytes, _ = protojson.Marshal(push)
	}
	c.node.logger.log(newLogEntry(LogLevelTrace, "-out->", map[string]any{"client": c.ID(), "user": user, "push": string(jsonBytes)}))
}

// Lock must be held outside.
func (c *Client) clientInfo(ch string) *ClientInfo {
	var channelInfo protocol.Raw
	channelContext, ok := c.channels[ch]
	if ok && channelHasFlag(channelContext.flags, flagSubscribed) {
		channelInfo = channelContext.info
	}
	return &ClientInfo{
		ClientID: c.uid,
		UserID:   c.user,
		ConnInfo: c.info,
		ChanInfo: channelInfo,
	}
}

const redacted = "<REDACTED>"

func redactCommand(cmd *protocol.Command) *protocol.Command {
	if cmd.Connect != nil && cmd.Connect.Token != "" {
		cmd.Connect.Token = redacted
	}
	if cmd.Connect != nil && len(cmd.Connect.Headers) > 0 {
		for k := range cmd.Connect.Headers {
			cmd.Connect.Headers[k] = redacted
		}
	}
	if cmd.Subscribe != nil && cmd.Subscribe.Token != "" {
		cmd.Subscribe.Token = redacted
	}
	if cmd.Refresh != nil && cmd.Refresh.Token != "" {
		cmd.Refresh.Token = redacted
	}
	if cmd.SubRefresh != nil && cmd.SubRefresh.Token != "" {
		cmd.SubRefresh.Token = redacted
	}

	return cmd
}

// HandleCommand processes a single protocol.Command. Supposed to be called only
// from a transport connection reader.
func (c *Client) HandleCommand(cmd *protocol.Command, cmdProtocolSize int) bool {
	c.mu.Lock()
	if c.status == statusClosed {
		c.mu.Unlock()
		return false
	}
	unusable := c.unusable
	c.mu.Unlock()

	if unusable {
		go func() { _ = c.close(DisconnectBadRequest) }()
		return false
	}

	if c.node.logEnabled(LogLevelTrace) {
		c.traceInCmd(cmd)
	}

	select {
	case <-c.ctx.Done():
		return false
	default:
	}

	disconnect, proceed := c.dispatchCommand(cmd, cmdProtocolSize)

	select {
	case <-c.ctx.Done():
		return false
	default:
	}
	if disconnect != nil {
		if disconnect.Code != DisconnectConnectionClosed.Code {
			c.node.logger.log(newLogEntry(LogLevelInfo, "disconnect after handling command", map[string]any{"command": fmt.Sprintf("%v", redactCommand(cmd)), "client": c.ID(), "user": c.UserID(), "reason": disconnect.Reason}))
		}
		go func() { _ = c.close(*disconnect) }()
		return false
	}
	return proceed
}

// isPong is a helper method to check whether the command from the client
// is a pong to server ping. It's actually an empty command.
func isPong(cmd *protocol.Command) bool {
	return cmd.Id == 0 && cmd.Send == nil
}

func (c *Client) handleCommandFinished(cmd *protocol.Command, frameType protocol.FrameType, err error, reply *protocol.Reply, started time.Time, ch string) {
	defer func() {
		c.node.metrics.observeCommandDuration(frameType, time.Since(started), ch, c)
	}()
	if c.node.clientEvents.commandProcessedHandler != nil {
		event := newCommandProcessedEvent(cmd, err, reply, started)
		c.issueCommandProcessedEvent(event)
	}
}

func (c *Client) handleCommandDispatchError(ch string, cmd *protocol.Command, frameType protocol.FrameType, err error, started time.Time) (*Disconnect, bool) {
	defer func() {
		c.node.metrics.observeCommandDuration(frameType, time.Since(started), ch, c)
	}()
	disconnect, ok := disconnectFromError(err)
	if ok {
		if c.node.clientEvents.commandProcessedHandler != nil {
			event := newCommandProcessedEvent(cmd, err, nil, started)
			c.issueCommandProcessedEvent(event)
		}
		return disconnect, false
	}
	if cmd.Connect != nil {
		c.mu.Lock()
		c.unusable = true
		c.mu.Unlock()
	}
	errorReply := &protocol.Reply{Error: toClientErr(err).toProto()}
	c.writeError(ch, frameType, cmd, errorReply, nil)
	if c.node.clientEvents.commandProcessedHandler != nil {
		event := newCommandProcessedEvent(cmd, err, errorReply, started)
		c.issueCommandProcessedEvent(event)
	}
	return nil, cmd.Connect == nil
}

func (c *Client) dispatchCommand(cmd *protocol.Command, cmdSize int) (*Disconnect, bool) {
	c.mu.Lock()
	if c.status == statusClosed {
		c.mu.Unlock()
		return nil, false
	}
	c.mu.Unlock()
	isConnect := cmd.Connect != nil
	if !c.authenticated && !isConnect {
		if c.node.logEnabled(LogLevelDebug) {
			c.node.logger.log(newLogEntry(LogLevelDebug, "unexpected frame before connect", map[string]any{"client": c.ID()}))
		}
		return &DisconnectBadRequest, false
	}

	var metricChannel string
	var frameType protocol.FrameType
	defer func() {
		c.node.metrics.incTransportMessagesReceived(c.transport.Name(), frameType, metricChannel, cmdSize, c)
	}()

	if isPong(cmd) {
		frameType = protocol.FrameTypeClientPong
		c.mu.Lock()
		if c.status == statusClosed {
			c.mu.Unlock()
			return nil, false
		}
		if c.lastPing <= 0 {
			// No ping was issued, unnecessary pong.
			c.mu.Unlock()
			if c.node.logEnabled(LogLevelDebug) {
				c.node.logger.log(newLogEntry(LogLevelDebug, "disconnect client due to unnecessary pong", map[string]any{"client": c.ID(), "user": c.UserID()}))
			}
			return &DisconnectBadRequest, false
		}
		// upon receiving pong we change a sign of lastPing value. This way we can handle
		// unnecessary pongs sent by the client and still use lastPing value in Client.checkPong.
		c.lastPing = -c.lastPing
		c.lastSeen = time.Now().UnixNano()
		c.mu.Unlock()
		return nil, true
	}

	if cmd.Id == 0 && cmd.Send == nil {
		// Now as pong processed make sure that command has id > 0 (except Send).
		return &DisconnectBadRequest, false
	}

	started := time.Now()

	if cmd.Connect != nil {
		frameType = protocol.FrameTypeConnect
	} else if cmd.Subscribe != nil {
		metricChannel = cmd.Subscribe.Channel
		frameType = protocol.FrameTypeSubscribe
	} else if cmd.Unsubscribe != nil {
		metricChannel = cmd.Unsubscribe.Channel
		frameType = protocol.FrameTypeUnsubscribe
	} else if cmd.Publish != nil {
		metricChannel = cmd.Publish.Channel
		frameType = protocol.FrameTypePublish
	} else if cmd.Presence != nil {
		metricChannel = cmd.Presence.Channel
		frameType = protocol.FrameTypePresence
	} else if cmd.PresenceStats != nil {
		metricChannel = cmd.PresenceStats.Channel
		frameType = protocol.FrameTypePresenceStats
	} else if cmd.History != nil {
		metricChannel = cmd.History.Channel
		frameType = protocol.FrameTypeHistory
	} else if cmd.Rpc != nil {
		frameType = protocol.FrameTypeRPC
	} else if cmd.Send != nil {
		frameType = protocol.FrameTypeSend
	} else if cmd.Refresh != nil {
		frameType = protocol.FrameTypeRefresh
	} else if cmd.SubRefresh != nil {
		metricChannel = cmd.SubRefresh.Channel
		frameType = protocol.FrameTypeSubRefresh
	} else {
		return &DisconnectBadRequest, false
	}

	var handleErr error

	handleErr = c.issueCommandReadEvent(cmd, cmdSize)
	if handleErr != nil {
		return c.handleCommandDispatchError(metricChannel, cmd, frameType, handleErr, started)
	}

	if cmd.Connect != nil {
		handleErr = c.handleConnect(cmd.Connect, cmd, started, nil)
	} else if cmd.Ping != nil {
		handleErr = c.handlePing(cmd, started, nil)
	} else if cmd.Subscribe != nil {
		handleErr = c.handleSubscribe(cmd.Subscribe, cmd, started, nil)
	} else if cmd.Unsubscribe != nil {
		handleErr = c.handleUnsubscribe(cmd.Unsubscribe, cmd, started, nil)
	} else if cmd.Publish != nil {
		if cmd.Publish.Type == 1 {
			if cmd.Publish.Removed {
				handleErr = c.handleMapRemove(cmd.Publish, cmd, started, nil)
			} else {
				handleErr = c.handleMapPublish(cmd.Publish, cmd, started, nil)
			}
		} else {
			handleErr = c.handlePublish(cmd.Publish, cmd, started, nil)
		}
	} else if cmd.Presence != nil {
		handleErr = c.handlePresence(cmd.Presence, cmd, started, nil)
	} else if cmd.PresenceStats != nil {
		handleErr = c.handlePresenceStats(cmd.PresenceStats, cmd, started, nil)
	} else if cmd.History != nil {
		handleErr = c.handleHistory(cmd.History, cmd, started, nil)
	} else if cmd.Rpc != nil {
		handleErr = c.handleRPC(cmd.Rpc, cmd, started, nil)
	} else if cmd.Send != nil {
		handleErr = c.handleSend(cmd.Send, cmd, started)
	} else if cmd.Refresh != nil {
		handleErr = c.handleRefresh(cmd.Refresh, cmd, started, nil)
	} else if cmd.SubRefresh != nil {
		handleErr = c.handleSubRefresh(cmd.SubRefresh, cmd, started, nil)
	} else {
		return &DisconnectBadRequest, false
	}
	if handleErr != nil {
		return c.handleCommandDispatchError(metricChannel, cmd, frameType, handleErr, started)
	}
	return nil, true
}

func (c *Client) writeEncodedCommandReply(ch string, frameType protocol.FrameType, cmd *protocol.Command, rep *protocol.Reply, rw *replyWriter) {
	rep.Id = cmd.Id
	if rep.Error != nil {
		if c.node.logEnabled(LogLevelInfo) {
			c.node.logger.log(newLogEntry(LogLevelInfo, "client command error", map[string]any{"reply": fmt.Sprintf("%v", rep), "command": fmt.Sprintf("%v", redactCommand(cmd)), "client": c.ID(), "user": c.UserID(), "error": rep.Error.Message, "code": rep.Error.Code}))
		}
		c.node.metrics.incReplyError(frameType, rep.Error.Code, ch, c)
	}

	protoType := c.transport.Protocol().toProto()
	replyEncoder := protocol.GetReplyEncoder(protoType)

	replyData, err := replyEncoder.Encode(rep)
	if err != nil {
		c.node.logger.log(newErrorLogEntry(err, "error encoding reply", map[string]any{"reply": fmt.Sprintf("%v", rep), "client": c.ID(), "user": c.UserID(), "error": err.Error()}))
		go func() { _ = c.close(DisconnectInappropriateProtocol) }()
		return
	}

	// Note: avoid adding *Reply to item since it's pooled.
	item := queue.Item{
		Data:      replyData,
		FrameType: frameType,
	}
	if ch != "" && c.node.config.Metrics.GetChannelNamespaceLabel != nil {
		item.Channel = ch
	}

	if c.replyWithoutQueue {
		err = c.messageWriter.config.WriteFn(item)
		if err != nil {
			go func() { _ = c.close(DisconnectWriteError) }()
		}
	} else {
		disconnect := c.messageWriter.enqueue(item)
		if disconnect != nil {
			go func() { _ = c.close(*disconnect) }()
		}
	}
	if rw != nil {
		rw.write(rep)
	}
	if c.node.logEnabled(LogLevelTrace) {
		c.traceOutReply(rep)
	}
}

func (c *Client) checkExpired() {
	c.mu.RLock()
	closed := c.status == statusClosed
	clientSideRefresh := c.clientSideRefresh
	exp := c.exp
	c.mu.RUnlock()
	if closed || exp == 0 {
		return
	}
	now := time.Now().Unix()
	ttl := exp - now

	if !clientSideRefresh && c.eventHub.refreshHandler != nil {
		if ttl > 0 {
			c.mu.Lock()
			if c.status != statusClosed {
				c.addExpireUpdate(time.Duration(ttl)*time.Second, true)
			}
			c.mu.Unlock()
		}
	}

	if ttl > 0 {
		// Connection was successfully refreshed.
		return
	}

	_ = c.close(DisconnectExpired)
}

// A protection against too long connection and subscription TTL which is likely
// a bug and may result into overflow of time.Duration type usage (~292 years max in Go).
// It's possible to go without expiration at all rather than having longer TTL.
const maxTTLSeconds = 365 * 24 * 3600

func (c *Client) expire() {
	c.mu.RLock()
	closed := c.status == statusClosed
	clientSideRefresh := c.clientSideRefresh
	exp := c.exp
	c.mu.RUnlock()
	if closed || exp == 0 {
		return
	}
	if !clientSideRefresh && c.eventHub.refreshHandler != nil {
		cb := func(reply RefreshReply, err error) {
			if err != nil {
				disconnect, ok := disconnectFromError(err)
				if ok {
					_ = c.close(*disconnect)
					return
				}
				c.node.logger.log(newErrorLogEntry(err, "unexpected error from refresh handler", map[string]any{"user": c.user, "client": c.uid, "error": err.Error()}))
				_ = c.close(DisconnectServerError)
				return
			}
			if reply.Expired {
				_ = c.close(DisconnectExpired)
				return
			}
			nowUnix := time.Now().Unix()
			expireAt := min(reply.ExpireAt, nowUnix+maxTTLSeconds)
			if expireAt > 0 {
				c.mu.Lock()
				c.exp = expireAt
				if reply.Info != nil {
					c.info = reply.Info
				}
				c.mu.Unlock()
			}
			c.checkExpired()
		}
		c.eventHub.refreshHandler(RefreshEvent{}, cb)
	} else {
		c.checkExpired()
	}
}

func (c *Client) handleConnect(req *protocol.ConnectRequest, cmd *protocol.Command, started time.Time, rw *replyWriter) error {
	err := c.connectCmd(req, cmd, started, rw)
	if err != nil {
		return err
	}
	c.triggerConnect()
	c.scheduleOnConnectTimers()
	return nil
}

func (c *Client) triggerConnect() {
	c.connectMu.Lock()
	defer c.connectMu.Unlock()
	// c.status is guarded by c.mu, not connectMu — other code (e.g. Unsubscribe
	// reached from a server-side Node.Unsubscribe/Disconnect) reads it under c.mu
	// while the connecting client is already registered in the hub. Take c.mu for
	// the status access; connectMu (held here and by close) serializes
	// triggerConnect with close, and the connectMu -> c.mu order matches close().
	c.mu.RLock()
	connecting := c.status == statusConnecting
	c.mu.RUnlock()
	if !connecting {
		return
	}
	if c.node.clientEvents.connectHandler != nil {
		c.node.clientEvents.connectHandler(c)
	}
	c.mu.Lock()
	c.status = statusConnected
	c.mu.Unlock()
}

func (c *Client) scheduleOnConnectTimers() {
	// Make presence and refresh handlers always run after client connect event.
	c.mu.Lock()
	c.addPresenceUpdate(true, false)
	if c.exp > 0 {
		expireAfter := time.Duration(c.exp-time.Now().Unix()) * time.Second
		if c.clientSideRefresh {
			conf := c.node.config
			expireAfter += conf.ClientExpiredCloseDelay
		}
		c.addExpireUpdate(expireAfter, false)
	}
	if c.pingInterval > 0 {
		c.addPingUpdate(true, false)
	}
	// Only schedule next timer once here after setting required points in time for ops.
	c.scheduleNextTimer()
	c.mu.Unlock()
}

func (c *Client) Refresh(opts ...RefreshOption) error {
	refreshOptions := &RefreshOptions{}
	for _, opt := range opts {
		opt(refreshOptions)
	}
	if refreshOptions.Expired {
		go func() { _ = c.close(DisconnectExpired) }()
		return nil
	}

	nowUnix := time.Now().Unix()
	expireAt := min(refreshOptions.ExpireAt, nowUnix+maxTTLSeconds)
	info := refreshOptions.Info

	res := &protocol.Refresh{
		Expires: expireAt > 0,
	}

	ttl := expireAt - nowUnix

	if ttl > 0 {
		res.Ttl = uint32(ttl)
	}

	if expireAt > 0 {
		// connection check enabled
		if ttl > 0 {
			// connection refreshed, update client timestamp and set new expiration timeout
			c.mu.Lock()
			c.exp = expireAt
			if len(info) > 0 {
				c.info = info
			}
			duration := time.Duration(ttl)*time.Second + c.node.config.ClientExpiredCloseDelay
			c.addExpireUpdate(duration, true)
			c.mu.Unlock()
		} else {
			go func() { _ = c.close(DisconnectExpired) }()
			return nil
		}
	} else {
		c.mu.Lock()
		c.exp = 0
		c.mu.Unlock()
	}

	replyData, err := c.getRefreshPushReply(res)
	if err != nil {
		return err
	}
	return c.writeEncodedPushData(replyData, "", "", protocol.FrameTypePushRefresh, ChannelBatchConfig{})
}

func (c *Client) getRefreshPushReply(res *protocol.Refresh) ([]byte, error) {
	return c.encodeReply(&protocol.Reply{
		Push: &protocol.Push{
			Refresh: res,
		},
	})
}

func (c *Client) releaseRefreshCommandReply(reply *protocol.Reply) {
	protocol.ReplyPool.ReleaseRefreshReply(reply)
}

func (c *Client) getRefreshCommandReply(res *protocol.RefreshResult) (*protocol.Reply, error) {
	return protocol.ReplyPool.AcquireRefreshReply(res), nil
}

func (c *Client) handleRefresh(req *protocol.RefreshRequest, cmd *protocol.Command, started time.Time, rw *replyWriter) error {
	if c.eventHub.refreshHandler == nil {
		return ErrorNotAvailable
	}

	if req.Token == "" {
		return c.logDisconnectBadRequest("client token required to refresh")
	}

	c.mu.RLock()
	clientSideRefresh := c.clientSideRefresh
	c.mu.RUnlock()

	if !clientSideRefresh {
		// Client not supposed to send refresh command in case of server-side refresh mechanism.
		return c.logDisconnectBadRequest("server-side refresh expected")
	}

	event := RefreshEvent{
		ClientSideRefresh: true,
		Token:             req.Token,
	}

	cb := func(reply RefreshReply, err error) {
		if err != nil {
			c.writeDisconnectOrErrorFlush("", protocol.FrameTypeRefresh, cmd, err, started, rw)
			return
		}

		if reply.Expired {
			c.writeDisconnectOrErrorFlush("", protocol.FrameTypeRefresh, cmd, DisconnectExpired, started, rw)
			return
		}

		nowUnix := time.Now().Unix()
		expireAt := min(reply.ExpireAt, nowUnix+maxTTLSeconds)
		info := reply.Info

		res := &protocol.RefreshResult{
			Expires: expireAt > 0,
		}

		ttl := expireAt - nowUnix

		if ttl > 0 {
			res.Ttl = uint32(ttl)
		}

		if expireAt > 0 {
			// connection check enabled
			if ttl > 0 {
				// connection refreshed, update client timestamp and set new expiration timeout
				c.mu.Lock()
				c.exp = expireAt
				if len(info) > 0 {
					c.info = info
				}
				duration := time.Duration(ttl)*time.Second + c.node.config.ClientExpiredCloseDelay
				c.addExpireUpdate(duration, true)
				c.mu.Unlock()
			} else {
				c.writeDisconnectOrErrorFlush("", protocol.FrameTypeRefresh, cmd, ErrorExpired, started, rw)
				return
			}
		}

		protoReply, err := c.getRefreshCommandReply(res)
		if err != nil {
			c.logWriteInternalErrorFlush("", protocol.FrameTypeRefresh, cmd, err, "error encoding refresh", started, rw)
			return
		}
		c.writeEncodedCommandReply("", protocol.FrameTypeRefresh, cmd, protoReply, rw)
		c.handleCommandFinished(cmd, protocol.FrameTypeRefresh, nil, protoReply, started, "")
		c.releaseRefreshCommandReply(protoReply)
	}

	c.eventHub.refreshHandler(event, cb)
	return nil
}

// onSubscribeErrorGen undoes a failed subscribe attempt's channel reservation
// and hub entry, restricted to the generation that attempt reserved.
//
// The generation is required rather than optional: a subscribe whose callback is
// asynchronous (proxy auth) can outlive its own reservation — the unsubscribe
// wait-gate timeout drops it and a fresh subscribe may re-reserve the channel —
// so undoing by channel name alone would delete that fresh reservation and close
// a subscribingCh its waiters still need. Encoding "match any" as generation 0
// was tried and removed: it silently turned a caller's zero-valued generation
// into an unconditional delete.
//
// The hub entry is removed generation-matched either way, so it is cleaned up
// even when the reservation is already gone (the subscribe may have registered
// in the hub before stalling).
func (c *Client) onSubscribeErrorGen(channel string, expectGen uint64) {
	if expectGen == anySubGen {
		// Programming error: 0 means "any generation" to the hub, so passing it
		// here would unconditionally remove whatever subscription is registered —
		// the exact clobber this function exists to prevent. Every subscribe path
		// mints a non-zero generation before it can fail, so this is unreachable;
		// refuse rather than corrupt state if a future caller gets it wrong.
		c.node.logger.log(newLogEntry(LogLevelError, "subscribe rollback with zero generation", map[string]any{
			"channel": channel, "user": c.user, "client": c.uid}))
		return
	}
	c.mu.Lock()
	chCtx, ok := c.channels[channel]
	owns := ok && chCtx.subGen == expectGen
	var subscribingCh chan struct{}
	if owns {
		subscribingCh = chCtx.subscribingCh
		delete(c.channels, channel)
	}
	c.mu.Unlock()
	_ = c.node.removeSubscription(channel, c, expectGen)
	if subscribingCh != nil {
		close(subscribingCh)
	}
}

// rollbackConnectServerSideSubs undoes connect-time server-side subscriptions
// when connectCmd aborts between subscribeCmd and the finalize loop. At that
// point each channel still holds its reservation in c.channels (finalize has
// not replaced it) plus a hub entry, and — for channels whose own subscribe
// succeeded — a node-level presence entry. Reservation and hub entry are
// dropped generation-matched, exactly as the finalize loop does, so an abort
// cannot tear down a fresh subscribe that took over the channel. Presence must
// be removed explicitly because close() snapshots c.channels only after the
// reservations are gone; for a channel whose own subscribe failed, subscribeCmd
// already removed its own presence on the way out.
func (c *Client) rollbackConnectServerSideSubs(subCtxMap map[string]subscribeContext, reservedGens map[string]uint64) {
	for channel, subCtx := range subCtxMap {
		c.onSubscribeErrorGen(channel, reservedGens[channel])
		c.removeSubscribePresence(channel, subCtx.channelContext.flags)
	}
}

func (c *Client) handleSubscribe(req *protocol.SubscribeRequest, cmd *protocol.Command, started time.Time, rw *replyWriter) error {
	if req.Channel == "" {
		return c.logDisconnectBadRequest("channel required for subscribe")
	}

	// Shared poll check — must be before map routing.
	var isSharedPoll bool
	if c.node.config.SharedPoll.GetSharedPollChannelOptions != nil {
		_, isSharedPoll = c.node.config.SharedPoll.GetSharedPollChannelOptions(req.Channel)
	}
	if req.Type == int32(SubscriptionTypeSharedPoll) && !isSharedPoll {
		return ErrorNotAvailable
	}
	if isSharedPoll && req.Type != int32(SubscriptionTypeSharedPoll) {
		return ErrorBadRequest
	}
	if req.Type == int32(SubscriptionTypeSharedPoll) {
		return c.handleSharedPollSubscribe(req, cmd, started, rw)
	}

	// Route map subscription types (map, client presence, user presence) to map handler.
	if req.Type == int32(SubscriptionTypeMap) || req.Type == int32(SubscriptionTypeMapClients) || req.Type == int32(SubscriptionTypeMapUsers) {
		return c.handleMapSubscribeCommand(req, cmd, started, rw)
	}

	if c.eventHub.subscribeHandler == nil {
		return ErrorNotAvailable
	}

	if req.Delta != "" {
		_, ok := stringToDeltaType[req.Delta]
		if !ok {
			return c.logDisconnectBadRequest("unknown delta type in subscribe request: " + req.Delta)
		}
	}

	subGen, replyError, disconnect := c.validateSubscribeRequest(req)
	if disconnect != nil || replyError != nil {
		if disconnect != nil {
			return *disconnect
		}
		return replyError
	}

	event := SubscribeEvent{
		Channel:     req.Channel,
		Token:       req.Token,
		Data:        req.Data,
		Positioned:  req.Positioned,
		Recoverable: req.Recoverable,
		JoinLeave:   req.JoinLeave,
	}

	cb := func(reply SubscribeReply, err error) {
		if reply.SubscriptionReady != nil {
			defer close(reply.SubscriptionReady)
		}

		// Gen-matched cleanup: this callback may be asynchronous (proxy auth), so
		// it can outlive its own reservation — the unsubscribe wait-gate timeout
		// path drops it and a fresh subscribe may re-reserve the channel. Undoing
		// by channel name alone would delete that fresh reservation and close a
		// subscribingCh its waiters still need.
		if err != nil {
			c.onSubscribeErrorGen(req.Channel, subGen)
			c.writeDisconnectOrErrorFlush(req.Channel, protocol.FrameTypeSubscribe, cmd, err, started, rw)
			return
		}

		// Regular subscription flow.
		ctx := c.subscribeCmd(req, reply, cmd, false, started, rw)
		if ctx.disconnect != nil {
			c.onSubscribeErrorGen(req.Channel, subGen)
			c.writeDisconnectOrErrorFlush(req.Channel, protocol.FrameTypeSubscribe, cmd, ctx.disconnect, started, rw)
			return
		}
		if ctx.err != nil {
			c.onSubscribeErrorGen(req.Channel, subGen)
			c.writeDisconnectOrErrorFlush(req.Channel, protocol.FrameTypeSubscribe, cmd, ctx.err, started, rw)
			return
		}

		if ctx.clientInfo != nil {
			if channelHasFlag(ctx.channelContext.flags, flagEmitJoinLeave) || ctx.channelContext.mapClientPresenceChannel != "" || ctx.channelContext.mapUserPresenceChannel != "" {
				// Synchronous (NOT `go`) so publishJoin reaches the broker before
				// this subscribe callback returns. If we spawned a goroutine, a
				// quick client disconnect could fire the synchronous publishLeave
				// (from close → unsubscribe loop) ahead of our Join, leaving
				// observers with [leave, join] on the wire.
				c.publishJoinAndPresence(req.Channel, ctx.channelContext, ctx.clientInfo)
			}
		}
	}
	c.eventHub.subscribeHandler(event, cb)
	return nil
}

func (c *Client) getSubscribedChannelContext(channel string) (ChannelContext, bool) {
	c.mu.RLock()
	ctx, okChannel := c.channels[channel]
	c.mu.RUnlock()
	if !okChannel || !channelHasFlag(ctx.flags, flagSubscribed) {
		return ChannelContext{}, false
	}
	return ctx, true
}

func (c *Client) handleSubRefresh(req *protocol.SubRefreshRequest, cmd *protocol.Command, started time.Time, rw *replyWriter) error {
	channel := req.Channel
	if channel == "" {
		return c.logDisconnectBadRequest("channel required for sub refresh")
	}

	ctx, okChannel := c.getSubscribedChannelContext(channel)
	if !okChannel {
		// Must be subscribed to refresh subscription.
		return ErrorPermissionDenied
	}

	// Route by type for keyed channels (shared poll track/untrack).
	if channelHasFlag(ctx.flags, flagKeyed) {
		switch req.Type {
		case typeTrack:
			return c.handleTrack(req, cmd, started, rw)
		case typeUntrack:
			return c.handleUntrack(req, cmd, started, rw)
		}
	}

	if c.eventHub.subRefreshHandler == nil {
		return ErrorNotAvailable
	}

	clientSideRefresh := channelHasFlag(ctx.flags, flagClientSideRefresh)
	if !clientSideRefresh {
		// Client not supposed to send sub refresh command in case of server-side
		// subscription refresh mechanism.
		return c.logDisconnectBadRequest("server-side sub refresh expected")
	}

	if req.Token == "" {
		c.node.logger.log(newLogEntry(LogLevelInfo, "subscription refresh token required", map[string]any{"channel": req.Channel, "client": c.uid, "user": c.UserID()}))
		return ErrorBadRequest
	}

	event := SubRefreshEvent{
		ClientSideRefresh: true,
		Channel:           req.Channel,
		Token:             req.Token,
	}

	c.eventHub.subRefreshHandler(event, func(reply SubRefreshReply, err error) {
		if err != nil {
			c.writeDisconnectOrErrorFlush(req.Channel, protocol.FrameTypeSubRefresh, cmd, err, started, rw)
			return
		}

		res := &protocol.SubRefreshResult{}

		nowUnix := time.Now().Unix()
		expireAt := min(reply.ExpireAt, nowUnix+maxTTLSeconds)
		if expireAt > 0 {
			res.Expires = true

			if expireAt < nowUnix {
				c.writeDisconnectOrErrorFlush(req.Channel, protocol.FrameTypeSubRefresh, cmd, ErrorExpired, started, rw)
				return
			}
			res.Ttl = uint32(expireAt - nowUnix)
		}

		c.mu.Lock()
		channelContext, okChan := c.channels[channel]
		// Gen-match the write-back (mirrors checkPosition): the SubRefreshHandler is
		// async, so an unsubscribe+resubscribe in the window installs a fresh subGen.
		// This refresh was validated against ctx.subGen — applying its expireAt/info
		// (or the tags filter below) to a different (newer) subscription would corrupt
		// a subscription its token never validated.
		sameSub := okChan && channelHasFlag(channelContext.flags, flagSubscribed) && channelContext.subGen == ctx.subGen
		if sameSub {
			channelContext.info = reply.Info
			channelContext.expireAt = expireAt
			c.channels[channel] = channelContext
		}
		isMapSub := sameSub && channelHasFlag(channelContext.flags, flagMap)
		c.mu.Unlock()

		if sameSub && reply.ServerTagsFilter != nil {
			newTf := &tagsFilter{
				filter: reply.ServerTagsFilter,
				hash:   filter.Hash(reply.ServerTagsFilter),
			}
			_, changed := c.node.hub.updateServerTagsFilter(channel, c.ID(), newTf)
			if changed && isMapSub {
				c.Unsubscribe(channel, Unsubscribe{
					Code:   UnsubscribeCodeStateInvalidated,
					Reason: "server tags filter changed",
				})
				return
			}
		}

		protoReply, err := c.getSubRefreshCommandReply(res)
		if err != nil {
			c.logWriteInternalErrorFlush(channel, protocol.FrameTypeSubRefresh, cmd, err, "error encoding sub refresh", started, rw)
			return
		}

		c.writeEncodedCommandReply(channel, protocol.FrameTypeSubRefresh, cmd, protoReply, rw)
		c.handleCommandFinished(cmd, protocol.FrameTypeSubRefresh, nil, protoReply, started, channel)
		c.releaseSubRefreshCommandReply(protoReply)
	})
	return nil
}

func (c *Client) releaseSubRefreshCommandReply(reply *protocol.Reply) {
	protocol.ReplyPool.ReleaseSubRefreshReply(reply)
}

func (c *Client) getSubRefreshCommandReply(res *protocol.SubRefreshResult) (*protocol.Reply, error) {
	return protocol.ReplyPool.AcquireSubRefreshReply(res), nil
}

func (c *Client) handleUnsubscribe(req *protocol.UnsubscribeRequest, cmd *protocol.Command, started time.Time, rw *replyWriter) error {
	channel := req.Channel
	if channel == "" {
		return c.logDisconnectBadRequest("channel required for unsubscribe")
	}

	if err := c.unsubscribe(channel, unsubscribeClient, nil); err != nil {
		return err
	}

	protoReply, err := c.getUnsubscribeCommandReply(&protocol.UnsubscribeResult{})
	if err != nil {
		c.node.logger.log(newErrorLogEntry(err, "error encoding unsubscribe", map[string]any{"error": err.Error()}))
		return DisconnectServerError
	}
	c.writeEncodedCommandReply(channel, protocol.FrameTypeUnsubscribe, cmd, protoReply, rw)
	c.handleCommandFinished(cmd, protocol.FrameTypeUnsubscribe, nil, protoReply, started, channel)
	c.releaseUnsubscribeCommandReply(protoReply)
	return nil
}

func (c *Client) releaseUnsubscribeCommandReply(reply *protocol.Reply) {
	protocol.ReplyPool.ReleaseUnsubscribeReply(reply)
}

func (c *Client) getUnsubscribeCommandReply(res *protocol.UnsubscribeResult) (*protocol.Reply, error) {
	return protocol.ReplyPool.AcquireUnsubscribeReply(res), nil
}

func (c *Client) handlePublish(req *protocol.PublishRequest, cmd *protocol.Command, started time.Time, rw *replyWriter) error {
	if c.eventHub.publishHandler == nil {
		return ErrorNotAvailable
	}

	channel := req.Channel
	data := req.Data

	if channel == "" {
		return c.logDisconnectBadRequest("channel is required for publish")
	}

	c.mu.RLock()
	info := c.clientInfo(channel)
	c.mu.RUnlock()

	event := PublishEvent{
		Channel:    channel,
		Data:       data,
		ClientInfo: info,
	}

	cb := func(reply PublishReply, err error) {
		if err != nil {
			c.writeDisconnectOrErrorFlush(channel, protocol.FrameTypePublish, cmd, err, started, rw)
			return
		}

		if reply.Result == nil {
			publishOpts := []PublishOption{
				WithHistory(reply.Options.HistorySize, reply.Options.HistoryTTL, reply.Options.HistoryMetaTTL),
				WithClientInfo(reply.Options.ClientInfo),
			}
			if reply.Options.Key != "" {
				publishOpts = append(publishOpts, WithKey(reply.Options.Key))
			}
			_, err := c.node.Publish(
				event.Channel, event.Data,
				publishOpts...,
			)
			if err != nil {
				c.logWriteInternalErrorFlush(channel, protocol.FrameTypePublish, cmd, err, "error publish", started, rw)
				return
			}
		}

		res := &protocol.PublishResult{}
		protoReply, err := c.getPublishCommandReply(res)
		if err != nil {
			c.logWriteInternalErrorFlush(channel, protocol.FrameTypePublish, cmd, err, "error encoding publish", started, rw)
			return
		}
		c.writeEncodedCommandReply(channel, protocol.FrameTypePublish, cmd, protoReply, rw)
		c.handleCommandFinished(cmd, protocol.FrameTypePublish, nil, protoReply, started, channel)
		c.releasePublishCommandReply(protoReply)
	}

	c.eventHub.publishHandler(event, cb)
	return nil
}

func (c *Client) handleMapPublish(req *protocol.PublishRequest, cmd *protocol.Command, started time.Time, rw *replyWriter) error {
	if c.eventHub.mapPublishHandler == nil {
		return ErrorNotAvailable
	}

	channel := req.Channel
	if channel == "" {
		return c.logDisconnectBadRequest("channel required for map publish")
	}

	c.mu.RLock()
	info := c.clientInfo(channel)
	c.mu.RUnlock()

	event := MapPublishEvent{
		Channel:    channel,
		Key:        req.Key,
		Data:       req.Data,
		ClientInfo: info,
	}

	cb := func(reply MapPublishReply, err error) {
		if err != nil {
			c.writeDisconnectOrErrorFlush(channel, protocol.FrameTypePublish, cmd, err, started, rw)
			return
		}

		// Handler must return the key explicitly. There is no fallback to the
		// client-supplied event.Key — for namespaces where the client supplies
		// the key, the handler should pass it through (reply.Key = event.Key).
		// For namespaces with server-driven keying (e.g. client_id / user_id),
		// the handler resolves the key and returns it; if the resolution yields
		// an empty key (e.g. user_id for an anonymous user), the handler should
		// reject the publish itself with an explicit error.
		key := reply.Key
		if key == "" {
			c.writeDisconnectOrErrorFlush(channel, protocol.FrameTypePublish, cmd, ErrorBadRequest, started, rw)
			return
		}

		if reply.Result == nil {
			opts := reply.Options
			if opts.Data == nil {
				opts.Data = event.Data
			}
			if opts.ClientInfo == nil {
				opts.ClientInfo = event.ClientInfo
			}
			_, err = c.node.MapPublish(context.Background(), event.Channel, key, opts)
			if err != nil {
				c.logWriteInternalErrorFlush(channel, protocol.FrameTypePublish, cmd, err, "error map publish", started, rw)
				return
			}
		}

		res := &protocol.PublishResult{}
		protoReply, err := c.getPublishCommandReply(res)
		if err != nil {
			c.logWriteInternalErrorFlush(channel, protocol.FrameTypePublish, cmd, err, "error encoding map publish", started, rw)
			return
		}
		c.writeEncodedCommandReply(channel, protocol.FrameTypePublish, cmd, protoReply, rw)
		c.handleCommandFinished(cmd, protocol.FrameTypePublish, nil, protoReply, started, channel)
		c.releasePublishCommandReply(protoReply)
	}

	c.eventHub.mapPublishHandler(event, cb)
	return nil
}

func (c *Client) handleMapRemove(req *protocol.PublishRequest, cmd *protocol.Command, started time.Time, rw *replyWriter) error {
	if c.eventHub.mapRemoveHandler == nil {
		return ErrorNotAvailable
	}

	channel := req.Channel
	if channel == "" {
		return c.logDisconnectBadRequest("channel required for map remove")
	}

	c.mu.RLock()
	info := c.clientInfo(channel)
	c.mu.RUnlock()

	event := MapRemoveEvent{
		Channel:    channel,
		Key:        req.Key,
		ClientInfo: info,
	}

	cb := func(reply MapRemoveReply, err error) {
		if err != nil {
			c.writeDisconnectOrErrorFlush(channel, protocol.FrameTypePublish, cmd, err, started, rw)
			return
		}

		// Handler must return the key explicitly. There is no fallback to the
		// client-supplied event.Key — see handleMapPublish for the reasoning.
		key := reply.Key
		if key == "" {
			c.writeDisconnectOrErrorFlush(channel, protocol.FrameTypePublish, cmd, ErrorBadRequest, started, rw)
			return
		}

		if reply.Result == nil {
			_, err = c.node.MapRemove(context.Background(), event.Channel, key, reply.Options)
			if err != nil {
				c.logWriteInternalErrorFlush(channel, protocol.FrameTypePublish, cmd, err, "error map remove", started, rw)
				return
			}
		}

		res := &protocol.PublishResult{}
		protoReply, err := c.getPublishCommandReply(res)
		if err != nil {
			c.logWriteInternalErrorFlush(channel, protocol.FrameTypePublish, cmd, err, "error encoding map remove", started, rw)
			return
		}
		c.writeEncodedCommandReply(channel, protocol.FrameTypePublish, cmd, protoReply, rw)
		c.handleCommandFinished(cmd, protocol.FrameTypePublish, nil, protoReply, started, channel)
		c.releasePublishCommandReply(protoReply)
	}

	c.eventHub.mapRemoveHandler(event, cb)
	return nil
}

func (c *Client) releasePublishCommandReply(reply *protocol.Reply) {
	protocol.ReplyPool.ReleasePublishReply(reply)
}

func (c *Client) getPublishCommandReply(res *protocol.PublishResult) (*protocol.Reply, error) {
	return protocol.ReplyPool.AcquirePublishReply(res), nil
}

func (c *Client) handlePresence(req *protocol.PresenceRequest, cmd *protocol.Command, started time.Time, rw *replyWriter) error {
	if c.eventHub.presenceHandler == nil {
		return ErrorNotAvailable
	}

	channel := req.Channel
	if channel == "" {
		return c.logDisconnectBadRequest("channel required for presence")
	}

	event := PresenceEvent{
		Channel: channel,
	}

	cb := func(reply PresenceReply, err error) {
		if err != nil {
			c.writeDisconnectOrErrorFlush(channel, protocol.FrameTypePresence, cmd, err, started, rw)
			return
		}

		var presence map[string]*ClientInfo
		if reply.Result == nil {
			result, err := c.node.Presence(event.Channel)
			if err != nil {
				c.logWriteInternalErrorFlush(channel, protocol.FrameTypePresence, cmd, err, "error getting presence", started, rw)
				return
			}
			presence = result.Presence
		} else {
			presence = reply.Result.Presence
		}

		protoPresence := make(map[string]*protocol.ClientInfo, len(presence))
		for k, v := range presence {
			protoPresence[k] = infoToProto(v)
		}

		protoReply, err := c.getPresenceCommandReply(&protocol.PresenceResult{
			Presence: protoPresence,
		})
		if err != nil {
			c.logWriteInternalErrorFlush(channel, protocol.FrameTypePresence, cmd, err, "error encoding presence", started, rw)
			return
		}
		c.writeEncodedCommandReply(channel, protocol.FrameTypePresence, cmd, protoReply, rw)
		c.handleCommandFinished(cmd, protocol.FrameTypePresence, nil, protoReply, started, channel)
		c.releasePresenceCommandReply(protoReply)
	}

	c.eventHub.presenceHandler(event, cb)
	return nil
}

func (c *Client) releasePresenceCommandReply(reply *protocol.Reply) {
	protocol.ReplyPool.ReleasePresenceReply(reply)
}

func (c *Client) getPresenceCommandReply(res *protocol.PresenceResult) (*protocol.Reply, error) {
	return protocol.ReplyPool.AcquirePresenceReply(res), nil
}

func (c *Client) handlePresenceStats(req *protocol.PresenceStatsRequest, cmd *protocol.Command, started time.Time, rw *replyWriter) error {
	if c.eventHub.presenceStatsHandler == nil {
		return ErrorNotAvailable
	}

	channel := req.Channel
	if channel == "" {
		return c.logDisconnectBadRequest("channel required for presence stats")
	}

	event := PresenceStatsEvent{
		Channel: channel,
	}

	cb := func(reply PresenceStatsReply, err error) {
		if err != nil {
			c.writeDisconnectOrErrorFlush(channel, protocol.FrameTypePresenceStats, cmd, err, started, rw)
			return
		}

		var presenceStats PresenceStats
		if reply.Result == nil {
			result, err := c.node.PresenceStats(event.Channel)
			if err != nil {
				c.logWriteInternalErrorFlush(channel, protocol.FrameTypePresenceStats, cmd, err, "error getting presence stats", started, rw)
				return
			}
			presenceStats = result.PresenceStats
		} else {
			presenceStats = reply.Result.PresenceStats
		}

		protoReply, err := c.getPresenceStatsCommandReply(&protocol.PresenceStatsResult{
			NumClients: uint32(presenceStats.NumClients),
			NumUsers:   uint32(presenceStats.NumUsers),
		})
		if err != nil {
			c.logWriteInternalErrorFlush(channel, protocol.FrameTypePresence, cmd, err, "error encoding presence stats", started, rw)
			return
		}
		c.writeEncodedCommandReply(channel, protocol.FrameTypePresenceStats, cmd, protoReply, rw)
		c.handleCommandFinished(cmd, protocol.FrameTypePresenceStats, nil, protoReply, started, channel)
		c.releasePresenceStatsCommandReply(protoReply)
	}

	c.eventHub.presenceStatsHandler(event, cb)
	return nil
}

func (c *Client) releasePresenceStatsCommandReply(reply *protocol.Reply) {
	protocol.ReplyPool.ReleasePresenceStatsReply(reply)
}

func (c *Client) getPresenceStatsCommandReply(res *protocol.PresenceStatsResult) (*protocol.Reply, error) {
	return protocol.ReplyPool.AcquirePresenceStatsReply(res), nil
}

func (c *Client) handleHistory(req *protocol.HistoryRequest, cmd *protocol.Command, started time.Time, rw *replyWriter) error {
	if c.eventHub.historyHandler == nil {
		return ErrorNotAvailable
	}

	channel := req.Channel
	if channel == "" {
		return c.logDisconnectBadRequest("channel required for history")
	}

	var historyFilter HistoryFilter
	if req.Since != nil {
		historyFilter.Since = &StreamPosition{
			Offset: req.Since.Offset,
			Epoch:  req.Since.Epoch,
		}
	}
	historyFilter.Limit = int(req.Limit)

	maxPublicationLimit := c.node.config.HistoryMaxPublicationLimit
	if maxPublicationLimit > 0 && (historyFilter.Limit < 0 || historyFilter.Limit > maxPublicationLimit) {
		historyFilter.Limit = maxPublicationLimit
	}

	historyFilter.Reverse = req.Reverse

	event := HistoryEvent{
		Channel: channel,
		Filter:  historyFilter,
	}

	cb := func(reply HistoryReply, err error) {
		if err != nil {
			c.writeDisconnectOrErrorFlush(channel, protocol.FrameTypeHistory, cmd, err, started, rw)
			return
		}

		var pubs []*Publication
		var offset uint64
		var epoch string
		if reply.Result == nil {
			result, err := c.node.History(event.Channel,
				WithHistoryFilter(event.Filter),
			)
			if err != nil {
				c.logWriteInternalErrorFlush(channel, protocol.FrameTypeHistory, cmd, err, "error getting history", started, rw)
				return
			}
			pubs = result.Publications
			offset = result.Offset
			epoch = result.Epoch
		} else {
			pubs = reply.Result.Publications
			offset = reply.Result.Offset
			epoch = reply.Result.Epoch
		}

		protoPubs := make([]*protocol.Publication, 0, len(pubs))
		for _, pub := range pubs {
			protoPub := pubToProto(pub)
			protoPubs = append(protoPubs, protoPub)
		}

		protoReply, err := c.getHistoryCommandReply(&protocol.HistoryResult{
			Publications: protoPubs,
			Offset:       offset,
			Epoch:        epoch,
		})
		if err != nil {
			c.logWriteInternalErrorFlush(channel, protocol.FrameTypeHistory, cmd, err, "error encoding history", started, rw)
			return
		}
		c.writeEncodedCommandReply(channel, protocol.FrameTypeHistory, cmd, protoReply, rw)
		c.handleCommandFinished(cmd, protocol.FrameTypeHistory, nil, protoReply, started, channel)
		c.releaseHistoryCommandReply(protoReply)
	}

	c.eventHub.historyHandler(event, cb)
	return nil
}

func (c *Client) releaseHistoryCommandReply(reply *protocol.Reply) {
	protocol.ReplyPool.ReleaseHistoryReply(reply)
}

func (c *Client) getHistoryCommandReply(res *protocol.HistoryResult) (*protocol.Reply, error) {
	return protocol.ReplyPool.AcquireHistoryReply(res), nil
}

var emptyReply = &protocol.Reply{}

func (c *Client) handlePing(_ *protocol.Command, _ time.Time, _ *replyWriter) error {
	// Ping not supported by protocol v2 at the moment. Supporting it requires adding
	// ping method to SDKs first. But nobody asked yet.
	return ErrorNotAvailable
}

func (c *Client) writeError(ch string, frameType protocol.FrameType, cmd *protocol.Command, errorReply *protocol.Reply, rw *replyWriter) {
	c.writeEncodedCommandReply(ch, frameType, cmd, errorReply, rw)
}

func (c *Client) writeDisconnectOrErrorFlush(ch string, frameType protocol.FrameType, cmd *protocol.Command, err error, started time.Time, rw *replyWriter) {
	defer func() {
		c.node.metrics.observeCommandDuration(frameType, time.Since(started), ch, c)
	}()
	disconnect, ok := disconnectFromError(err)
	if ok {
		go func() { _ = c.close(*disconnect) }()
		if c.node.clientEvents.commandProcessedHandler != nil {
			event := newCommandProcessedEvent(cmd, err, nil, started)
			c.issueCommandProcessedEvent(event)
		}
		return
	}
	errorReply := &protocol.Reply{Error: toClientErr(err).toProto()}
	c.writeError(ch, frameType, cmd, errorReply, rw)
	if c.node.clientEvents.commandProcessedHandler != nil {
		event := newCommandProcessedEvent(cmd, err, errorReply, started)
		c.issueCommandProcessedEvent(event)
	}
}

type replyWriter struct {
	write func(*protocol.Reply)
}

func (c *Client) handleRPC(req *protocol.RPCRequest, cmd *protocol.Command, started time.Time, rw *replyWriter) error {
	if c.eventHub.rpcHandler == nil {
		return ErrorNotAvailable
	}

	event := RPCEvent{
		Method: req.Method,
		Data:   req.Data,
	}

	cb := func(reply RPCReply, err error) {
		if err != nil {
			c.writeDisconnectOrErrorFlush("", protocol.FrameTypeRPC, cmd, err, started, rw)
			return
		}
		result := &protocol.RPCResult{
			Data: reply.Data,
		}
		protoReply, err := c.getRPCCommandReply(result)
		if err != nil {
			c.logWriteInternalErrorFlush("", protocol.FrameTypeRPC, cmd, err, "error encoding rpc", started, rw)
			return
		}
		c.writeEncodedCommandReply("", protocol.FrameTypeRPC, cmd, protoReply, rw)
		c.handleCommandFinished(cmd, protocol.FrameTypeRPC, nil, protoReply, started, "")
		c.releaseRPCCommandReply(protoReply)
	}

	c.eventHub.rpcHandler(event, cb)
	return nil
}

func (c *Client) releaseRPCCommandReply(r *protocol.Reply) {
	protocol.ReplyPool.ReleaseRPCReply(r)
}

func (c *Client) getRPCCommandReply(res *protocol.RPCResult) (*protocol.Reply, error) {
	return protocol.ReplyPool.AcquireRPCReply(res), nil
}

func (c *Client) handleSend(req *protocol.SendRequest, cmd *protocol.Command, started time.Time) error {
	// Send handler is a bit special since it's a one way command: client does not expect any reply.
	if c.eventHub.messageHandler == nil {
		c.node.metrics.observeCommandDuration(protocol.FrameTypeSend, time.Since(started), "", c)
		// Return DisconnectNotAvailable here since otherwise client won't even know
		// server does not have asynchronous message handler set.
		return DisconnectNotAvailable
	}
	c.eventHub.messageHandler(MessageEvent{
		Data: req.Data,
	})
	c.handleCommandFinished(cmd, protocol.FrameTypeSend, nil, nil, started, "")
	return nil
}

func (c *Client) unlockServerSideSubscriptions(subCtxMap map[string]subscribeContext) {
	for channel := range subCtxMap {
		c.pubSubSync.StopBuffering(channel)
	}
}

// isInTest may be true during Centrifuge test run. We use it to inject code required to
// cover various edge case scenarios.
var isInTest = false

const (
	testChannelRedisClientSubscribeRecoveryDeadlock1 = "TestRedisClientSubscribeRecoveryDeadlock1"
	testChannelRedisClientSubscribeRecoveryDeadlock2 = "TestRedisClientSubscribeRecoveryDeadlock2"
)

// connectCmd handles connect command from client - client must send connect
// command immediately after establishing connection with server.
func (c *Client) connectCmd(req *protocol.ConnectRequest, cmd *protocol.Command, started time.Time, rw *replyWriter) error {
	// Client name/version metric labels use fixed sentinels to keep metric
	// cardinality bounded: a reported value is only used verbatim when it's
	// explicitly registered, otherwise it collapses to "unregistered". A client
	// that reports nothing at all gets "unnamed"/"unversioned" so the series stays
	// visible and self-describing rather than carrying an empty label value.
	metricClientName := "unnamed"
	if req.Name != "" {
		metricClientName = "unregistered"
		if slices.Contains(c.node.config.Metrics.RegisteredClientNames, req.Name) {
			metricClientName = req.Name
		}
	}
	metricClientVersion := "unversioned"
	if req.Version != "" {
		metricClientVersion = "unregistered"
		if c.node.config.Metrics.CheckRegisteredClientVersion != nil && c.node.config.Metrics.CheckRegisteredClientVersion(req.Name, req.Version) {
			metricClientVersion = req.Version
		}
	}
	// Codec negotiation belongs to the engine: it is handed the client's
	// advertised capability flags and returns nil when it has nothing that client
	// can decode. That keeps adding a codec a change in one place rather than
	// here, and leaves every pre-feature client uncompressed.
	var acceptedFlags int64
	if engine := c.node.config.DictionaryCompression; engine != nil {
		if ca, ok := c.transport.(compressionAware); ok {
			params := ConnectionParams{
				ProtocolType: c.transport.Protocol(),
				ClientFlags:  req.Flag,
			}
			if st := req.GetState(); st != nil {
				// Dictionaries this client kept from an earlier connection. Ids are
				// content hashes, so anything the engine recognises can be activated
				// by name rather than re-sent; anything it does not is ignored.
				params.HeldDictionaryIDs = st.GetDictionaryIds()
			}
			if cc := engine.NewConnection(params); cc != nil {
				ca.setConnectionCompression(cc)
				acceptedFlags |= ConnectionFlagDictionaryCompression
			}
		}
	}

	c.mu.RLock()
	authenticated := c.authenticated
	closed := c.status == statusClosed
	c.mu.RUnlock()

	if closed {
		return DisconnectConnectionClosed
	}

	if authenticated {
		return c.logDisconnectBadRequest("client already authenticated")
	}

	config := c.node.config
	version := config.Version
	userConnectionLimit := config.UserConnectionLimit
	channelLimit := config.ClientChannelLimit

	var (
		credentials       *Credentials
		authData          protocol.Raw
		subscriptions     map[string]SubscribeOptions
		clientSideRefresh bool
		labels            map[string]string
	)

	if c.node.clientEvents.connectingHandler != nil {
		e := ConnectEvent{
			ClientID:  c.ID(),
			Data:      req.Data,
			Token:     req.Token,
			Name:      req.Name,
			Version:   req.Version,
			Transport: c.transport,
			Headers:   req.Headers,
		}
		if len(req.Subs) > 0 {
			channels := make([]string, 0, len(req.Subs))
			for ch := range req.Subs {
				channels = append(channels, ch)
			}
			e.Channels = channels
		}
		reply, err := c.node.clientEvents.connectingHandler(c.ctx, e)
		if err != nil {
			c.startWriter(0, 0, 0, 0, false)
			return err
		}
		if reply.PingPongConfig != nil {
			c.pingInterval, c.pongTimeout = getPingPongPeriodValues(*reply.PingPongConfig)
		} else {
			c.pingInterval, c.pongTimeout = getPingPongPeriodValues(c.transport.PingPongConfig())
		}
		c.replyWithoutQueue = reply.ReplyWithoutQueue
		c.startWriter(reply.WriteDelay, reply.MaxMessagesInFrame, reply.QueueInitialCap, reply.QueueShrinkDelay, reply.WriteWithTimer)

		if reply.Credentials != nil {
			credentials = reply.Credentials
		}
		c.storage = reply.Storage
		if reply.Context != nil {
			c.mu.Lock()
			c.ctx = reply.Context
			c.mu.Unlock()
		}
		if reply.Data != nil {
			authData = reply.Data
		}
		clientSideRefresh = reply.ClientSideRefresh
		labels = reply.Labels
		if len(reply.Subscriptions) > 0 {
			subscriptions = make(map[string]SubscribeOptions, len(reply.Subscriptions))
			for ch, opts := range reply.Subscriptions {
				if ch == "" {
					continue
				}
				subscriptions[ch] = opts
			}
		}
	} else {
		c.startWriter(0, 0, 0, 0, false)
		c.pingInterval, c.pongTimeout = getPingPongPeriodValues(c.transport.PingPongConfig())
	}

	if channelLimit > 0 && len(subscriptions) > channelLimit {
		return DisconnectChannelLimit
	}

	if credentials == nil {
		// Try to find Credentials in context.
		if cred, ok := GetCredentials(c.ctx); ok {
			credentials = cred
		}
	}

	var (
		expires bool
		ttl     uint32
	)

	c.mu.Lock()
	c.clientSideRefresh = clientSideRefresh
	c.mu.Unlock()

	if credentials == nil {
		return c.logDisconnectBadRequest("client credentials not found")
	}

	c.mu.Lock()
	c.user = credentials.UserID
	c.info = credentials.Info
	c.exp = min(credentials.ExpireAt, time.Now().Unix()+maxTTLSeconds)

	user := c.user
	exp := c.exp
	closed = c.status == statusClosed
	c.mu.Unlock()

	if closed {
		return DisconnectConnectionClosed
	}

	if c.node.logEnabled(LogLevelDebug) {
		c.node.logger.log(newLogEntry(LogLevelDebug, "client authenticated", map[string]any{"client": c.uid, "user": c.user}))
	}

	if userConnectionLimit > 0 && user != "" && len(c.node.hub.UserConnections(user)) >= userConnectionLimit {
		c.node.logger.log(newLogEntry(LogLevelInfo, "limit of connections for user reached", map[string]any{"user": user, "client": c.uid, "limit": userConnectionLimit}))
		return DisconnectConnectionLimit
	}

	c.mu.RLock()
	if exp > 0 {
		expires = true
		now := time.Now().Unix()
		if exp < now {
			c.mu.RUnlock()
			c.node.logger.log(newLogEntry(LogLevelInfo, "connection expiration must be greater than now", map[string]any{"client": c.uid, "user": c.UserID()}))
			return ErrorExpired
		}
		ttl = uint32(exp - now)
	}
	c.mu.RUnlock()

	res := &protocol.ConnectResult{}
	res.Version = version
	res.Expires = expires
	res.Ttl = ttl
	// Tell the client which of the features it advertised were actually enabled.
	// Advertising one is not enough: the node may have it turned off, or the
	// engine may decline this particular connection.
	res.Flag = acceptedFlags

	if c.pingInterval > 0 {
		res.Ping = uint32(c.pingInterval.Seconds())
	}
	if !c.transport.Unidirectional() && c.pongTimeout > 0 {
		res.Pong = true
	}

	if c.transport.Unidirectional() || c.transport.Emulation() {
		res.Session = c.session
	}
	if c.transport.Emulation() {
		res.Node = c.node.ID()
	}
	if c.node.config.ClientConnectIncludeServerTime {
		res.Time = time.Now().UnixMilli()
	}

	// Client successfully connected.
	c.mu.Lock()
	if c.status == statusClosed {
		c.mu.Unlock()
		return DisconnectConnectionClosed
	}
	c.authenticated = true
	c.metricName = metricClientName
	c.metricVersion = metricClientVersion
	c.labels = labels

	// Precompute and cache the label combination pointer for zero-allocation metrics hot path
	// Do this before addClient so metrics recorded by addClient can use the cached combination
	if len(c.node.metrics.config.ClientLabels) > 0 {
		combo := c.node.metrics.getOrCreateClientLabelCombinationFromLabels(labels)
		c.labelCombinationCached.Store(combo)
	}

	c.node.addClient(c)
	// Reserve the connect-time server-side subscription channels under the same
	// lock as addClient. The client is now hub-registered, so a server-side
	// Node.Subscribe can reach it; without a reservation a concurrent
	// Client.Subscribe on one of these channels would land between the connect-time
	// subscribe's hub-add and its c.channels write and clobber its generation,
	// leaking a hub entry. With the reservation that Client.Subscribe instead gets
	// ErrorAlreadySubscribed. subscribeCmd below reuses this generation, and the
	// finalize replaces the reservation with the live context.
	reservedGens := make(map[string]uint64, len(subscriptions))
	for ch := range subscriptions {
		gen := c.subGenCounter.Add(1)
		reservedGens[ch] = gen
		c.channels[ch] = ChannelContext{subscribingCh: make(chan struct{}), subGen: gen}
	}
	c.mu.Unlock()

	if !clientSideRefresh {
		// Server will do refresh itself.
		res.Expires = false
		res.Ttl = 0
	}

	res.Client = c.uid
	if authData != nil {
		res.Data = authData
	}

	var subCtxMap map[string]subscribeContext
	if len(subscriptions) > 0 {
		var subMu sync.Mutex
		subCtxMap = make(map[string]subscribeContext, len(subscriptions))
		subs := make(map[string]*protocol.SubscribeResult, len(subscriptions))
		var subDisconnect *Disconnect
		var subError *Error
		var wg sync.WaitGroup

		wg.Add(len(subscriptions))
		for ch, opts := range subscriptions {
			go func(ch string, opts SubscribeOptions) {
				defer wg.Done()
				subCmd := &protocol.SubscribeRequest{
					Channel: ch,
				}
				if subReq, ok := req.Subs[ch]; ok {
					subCmd.Recover = subReq.Recover
					subCmd.Offset = subReq.Offset
					subCmd.Epoch = subReq.Epoch
					subCmd.Delta = subReq.Delta
				}
				if isInTest && ch == testChannelRedisClientSubscribeRecoveryDeadlock2 { // Only for tests.
					select {
					case <-time.After(time.Second):
					case <-c.Context().Done():
					}
				}
				subCtx := c.subscribeCmd(subCmd, SubscribeReply{Options: opts}, nil, true, started, nil)
				subMu.Lock()
				subs[ch] = subCtx.result
				subCtxMap[ch] = subCtx
				if subCtx.disconnect != nil {
					subDisconnect = subCtx.disconnect
				}
				if subCtx.err != nil {
					subError = subCtx.err
				}
				subMu.Unlock()
			}(ch, opts)
		}
		wg.Wait()

		if subDisconnect != nil || subError != nil {
			c.unlockServerSideSubscriptions(subCtxMap)
			c.rollbackConnectServerSideSubs(subCtxMap, reservedGens)
			if subDisconnect != nil {
				return subDisconnect
			}
			return subError
		}
		res.Subs = subs
	}

	if c.transport.Unidirectional() {
		if !hasFlag(c.transport.DisabledPushFlags(), PushFlagConnect) {
			protoReply, err := c.getConnectPushReply(res)
			if err != nil {
				c.unlockServerSideSubscriptions(subCtxMap)
				c.rollbackConnectServerSideSubs(subCtxMap, reservedGens)
				c.node.logger.log(newErrorLogEntry(err, "error encoding connect", map[string]any{"error": err.Error()}))
				return DisconnectServerError
			}
			encoder := protocol.GetPushEncoder(c.transport.Protocol().toProto())
			data, err := encoder.Encode(protoReply.Push)
			if err != nil {
				c.node.logger.log(newErrorLogEntry(err, "error encoding connect push", map[string]any{"push": fmt.Sprintf("%v", protoReply.Push), "client": c.ID(), "user": c.UserID(), "error": err.Error()}))
				go func() { _ = c.close(DisconnectInappropriateProtocol) }()
			} else {
				if err = c.writeEncodedPushData(data, "", "", protocol.FrameTypePushConnect, ChannelBatchConfig{}); err == nil {
					if rw != nil {
						rw.write(protoReply)
					}
					if c.node.logEnabled(LogLevelTrace) {
						c.traceOutPush(&protocol.Push{Connect: protoReply.Push.Connect})
					}
				}
			}
		}
	} else {
		protoReply, err := c.getConnectCommandReply(res)
		if err != nil {
			c.unlockServerSideSubscriptions(subCtxMap)
			c.rollbackConnectServerSideSubs(subCtxMap, reservedGens)
			c.node.logger.log(newErrorLogEntry(err, "error encoding connect", map[string]any{"error": err.Error()}))
			return DisconnectServerError
		}
		c.writeEncodedCommandReply("", protocol.FrameTypeConnect, cmd, protoReply, rw)
		defer c.releaseConnectCommandReply(protoReply)
		defer c.handleCommandFinished(cmd, protocol.FrameTypeConnect, nil, protoReply, started, "")
	}

	c.mu.Lock()
	closedDuringConnect := c.status == statusClosed
	var reservedSubChs []chan struct{}
	var lostReservations map[string]bool
	for channel, subCtx := range subCtxMap {
		// Take over the reservation installed above: capture its subscribingCh (to
		// release any unsubscribe waiting on the in-flight subscribe) and replace it
		// with the live context, or drop it if the client closed mid-connect.
		// Identity-match on subGen first (mirrors commitSubscription): a subscribe
		// stalled past the unsubscribe wait timeout can lose its reservation, and
		// the entry now present — if any — may be a fresh client-side subscribe's
		// reservation with a newer generation. Overwriting it (or closing its
		// subscribingCh) would corrupt that fresh subscription; leave it alone and
		// roll back this connect-time subscription below instead.
		resv, ok := c.channels[channel]
		if !ok || resv.subGen != reservedGens[channel] {
			if lostReservations == nil {
				lostReservations = make(map[string]bool, 1)
			}
			lostReservations[channel] = true
			continue
		}
		if resv.subscribingCh != nil {
			reservedSubChs = append(reservedSubChs, resv.subscribingCh)
		}
		if closedDuringConnect {
			delete(c.channels, channel)
		} else {
			c.channels[channel] = subCtx.channelContext
		}
	}
	c.mu.Unlock()

	for _, sc := range reservedSubChs {
		close(sc)
	}

	c.unlockServerSideSubscriptions(subCtxMap)

	// Roll back connect-time subscriptions whose reservation was lost: remove the
	// hub entry this attempt registered (gen-matched, no-op if the unsubscribe
	// already removed it) and its pre-commit presence entry. Runs after
	// unlockServerSideSubscriptions so buffers are released before subShard locks
	// are taken. The connection is already being force-closed by the unsubscribe
	// timeout path that consumed the reservation.
	for channel := range lostReservations {
		_ = c.node.removeSubscription(channel, c, reservedGens[channel])
		c.removeSubscribePresence(channel, subCtxMap[channel].channelContext.flags)
	}

	if closedDuringConnect {
		// The client closed while connect was applying these server-side
		// subscriptions. close() snapshotted c.channels before they were installed,
		// so it did not tear down their hub registrations — they would leak. Roll
		// them back here (subLocks are released above, so removeSubscription can
		// take them). Mirrors commitSubscription's closed path, including the
		// pre-commit presence entry subscribeCmd added for each channel.
		for channel, subCtx := range subCtxMap {
			_ = c.node.removeSubscription(channel, c, reservedGens[channel])
			c.removeSubscribePresence(channel, subCtx.channelContext.flags)
		}
		return DisconnectConnectionClosed
	}

	for channel, subCtx := range subCtxMap {
		if lostReservations[channel] {
			// Rolled back above — publishing join / adding map presence here would
			// resurrect state for a subscription that was never installed.
			continue
		}
		if subCtx.clientInfo != nil {
			// Synchronous: see comment on the same call in handleSubscribe.
			// A spawned goroutine racing a disconnect's publishLeave can land
			// Join after Leave on the wire.
			c.publishJoinAndPresence(channel, subCtx.channelContext, subCtx.clientInfo)
		}
	}

	return nil
}

func (c *Client) getConnectPushReply(res *protocol.ConnectResult) (*protocol.Reply, error) {
	p := &protocol.Connect{
		Version: res.GetVersion(),
		Client:  res.GetClient(),
		Data:    res.Data,
		Subs:    res.Subs,
		Expires: res.Expires,
		Ttl:     res.Ttl,
		Ping:    res.Ping,
		Pong:    res.Pong,
		Session: res.Session,
		Node:    res.Node,
		Time:    res.Time,
	}
	return &protocol.Reply{
		Push: &protocol.Push{
			Connect: p,
		},
	}, nil
}

func (c *Client) startWriter(batchDelay time.Duration, maxMessagesInFrame int, queueInitialCap int, queueShrinkDelay time.Duration, writeWithTimer bool) {
	c.startWriterOnce.Do(func() {
		var writeMu sync.Mutex
		messageWriterConf := writerConfig{
			MaxQueueSize: c.node.config.ClientQueueMaxSize,
			WriteFn: func(item queue.Item) error {
				c.node.metrics.incTransportMessagesSent(c.transport.Name(), item.FrameType, item.Channel, len(item.Data), c)

				if c.node.clientEvents.transportWriteHandler != nil {
					pass := c.node.clientEvents.transportWriteHandler(c, TransportWriteEvent(item))
					if !pass {
						return nil
					}
				}
				writeMu.Lock()
				defer writeMu.Unlock()
				if err := c.transport.Write(item.Data); err != nil {
					if c.node.logger.enabled(LogLevelTrace) {
						c.node.logger.log(newLogEntry(LogLevelTrace, "client write failed", map[string]any{"client": c.uid, "user": c.user, "error": err.Error()}))
					}
					disconnect, ok := disconnectFromError(err)
					if ok {
						c.spawnCloseUnlessClosing(*disconnect)
					} else {
						c.spawnCloseUnlessClosing(DisconnectWriteError)
					}
					return err
				}
				return nil
			},
			WriteManyFn: func(items ...queue.Item) error {
				messagesBuf := bpool.GetByteSlicesBuf(len(items))
				defer bpool.PutByteSlicesBuf(messagesBuf)

				// Batch metric updates - accumulate counts locally first
				type metricKey struct {
					frameType string
					namespace string
				}
				metricCounts := make(map[metricKey]struct {
					count int
					size  int
				})
				transportName := c.transport.Name()

				for i := 0; i < len(items); i++ {
					if c.node.clientEvents.transportWriteHandler != nil {
						pass := c.node.clientEvents.transportWriteHandler(c, TransportWriteEvent(items[i]))
						if !pass {
							continue
						}
					}
					messagesBuf.B = append(messagesBuf.B, items[i].Data)

					// Accumulate metrics locally
					key := metricKey{
						frameType: items[i].FrameType.String(),
						namespace: c.node.metrics.getChannelNamespaceLabel(items[i].Channel),
					}
					stats := metricCounts[key]
					stats.count++
					stats.size += len(items[i].Data)
					metricCounts[key] = stats
				}

				// Update metrics once per unique label combination
				// Use cached client label combination for zero-allocation hot path
				// The combination is pre-cached during connect, so just load it directly
				var clientLabelValues []string
				var clientLabelCacheKey string
				if combo := c.labelCombinationCached.Load(); combo != nil {
					clientLabelValues = combo.labelValues
					clientLabelCacheKey = combo.cacheKey
				}

				for key, stats := range metricCounts {
					counters := c.node.metrics.getTransportMessagesSentCounters(transportName, key.frameType, key.namespace, clientLabelValues, clientLabelCacheKey)
					// Batch update - add count and size together
					counters.counterSent.Add(float64(stats.count))
					counters.counterSentSize.Add(float64(stats.size))
				}

				writeMu.Lock()
				err := c.transport.WriteMany(messagesBuf.B...)
				writeMu.Unlock()

				if err != nil {
					if c.node.logger.enabled(LogLevelTrace) {
						c.node.logger.log(newLogEntry(LogLevelTrace, "client write failed", map[string]any{"client": c.uid, "user": c.user, "error": err.Error()}))
					}
					disconnect, ok := disconnectFromError(err)
					if ok {
						c.spawnCloseUnlessClosing(*disconnect)
					} else {
						c.spawnCloseUnlessClosing(DisconnectWriteError)
					}
					return err
				}
				return nil
			},
		}

		c.messageWriter = newWriter(messageWriterConf, queueInitialCap)
		if batchDelay > 0 && writeWithTimer {
			// Timer-driven mode: non-blocking, triggered by enqueue operations.
			c.messageWriter.run(batchDelay, maxMessagesInFrame, queueShrinkDelay, true)
		} else {
			// Traditional mode: dedicated goroutine for immediate writes.
			go c.messageWriter.run(batchDelay, maxMessagesInFrame, queueShrinkDelay, false)
		}
		if c.node.config.GetChannelBatchConfig != nil {
			c.perChannelWriter = newPerChannelWriter(c.writeQueueItems)
		}
	})
}

func (c *Client) releaseConnectCommandReply(reply *protocol.Reply) {
	protocol.ReplyPool.ReleaseConnectReply(reply)
}

func (c *Client) getConnectCommandReply(res *protocol.ConnectResult) (*protocol.Reply, error) {
	return protocol.ReplyPool.AcquireConnectReply(res), nil
}

// Subscribe client to a channel.
func (c *Client) Subscribe(channel string, opts ...SubscribeOption) error {
	if channel == "" {
		return fmt.Errorf("channel is empty")
	}
	channelLimit := c.node.config.ClientChannelLimit

	// Reserve the channel before doing any subscribe work. This mirrors the
	// client-command path (validateSubscribeRequest) and gives server-side
	// subscribes the same "one in-flight subscribe per channel" guarantee: the
	// atomic check-and-reserve under c.mu rejects a duplicate/concurrent Subscribe
	// for the same channel, so two subscribes can't race their hub-add and
	// c.channels write into an inconsistent state. The reservation carries a
	// subscribingCh (with flags==0, so flagServerSide is not yet set), which the
	// existing unsubscribe wait-gate uses to synchronize a racing Unsubscribe.
	c.mu.Lock()
	if c.status == statusClosed {
		c.mu.Unlock()
		return nil
	}
	numChannels := len(c.channels)
	if channelLimit > 0 && numChannels >= channelLimit {
		c.mu.Unlock()
		go func() { _ = c.close(DisconnectChannelLimit) }()
		return nil
	}
	if _, ok := c.channels[channel]; ok {
		c.mu.Unlock()
		return ErrorAlreadySubscribed
	}
	// Also reject while a map subscribe is loading this channel: it reserves
	// c.mapSubscribing rather than c.channels (mirrors validateSubscribeRequest).
	// Without this both can commit for the same channel. Nothing leaks — the hub
	// holds one entry per (channel, client) and the later addSub overwrites it,
	// so c.channels and the hub stay in agreement — but whichever subscription
	// commits second silently replaces the other: its options, flags and stream
	// position are discarded and its OnUnsubscribe never fires. Rejecting here
	// keeps "one subscription per channel per client" true at the source.
	if _, ok := c.mapSubscribing[channel]; ok {
		c.mu.Unlock()
		return ErrorAlreadySubscribed
	}
	subGen := c.subGenCounter.Add(1)
	c.channels[channel] = ChannelContext{subscribingCh: make(chan struct{}), subGen: subGen}
	c.mu.Unlock()

	subCmd := &protocol.SubscribeRequest{
		Channel: channel,
	}
	subscribeOpts := &SubscribeOptions{}
	for _, opt := range opts {
		opt(subscribeOpts)
	}
	if subscribeOpts.RecoverSince != nil {
		subCmd.Recover = true
		subCmd.Offset = subscribeOpts.RecoverSince.Offset
		subCmd.Epoch = subscribeOpts.RecoverSince.Epoch
	}
	subCtx := c.subscribeCmd(subCmd, SubscribeReply{
		Options: *subscribeOpts,
	}, nil, true, time.Time{}, nil)
	// Gen-matched: undo only this attempt's reservation (see onSubscribeErrorGen).
	if subCtx.disconnect != nil {
		c.onSubscribeErrorGen(subCmd.Channel, subGen)
		return subCtx.disconnect
	}
	if subCtx.err != nil {
		c.onSubscribeErrorGen(subCmd.Channel, subGen)
		return subCtx.err
	}
	defer c.pubSubSync.StopBuffering(channel)
	// Commit the reservation into c.channels, or roll back the hub entry if the
	// client closed mid-subscribe. Same commit the map path uses. The reservation
	// guaranteed no competing subscribe ran, so c.channels and the hub entry
	// subscribeCmd added describe the same generation.
	subscribingCh, committed := c.commitSubscription(channel, subCtx.channelContext, reservationChannels)
	if !committed {
		return DisconnectServerError
	}
	if subscribingCh != nil {
		close(subscribingCh)
	}
	if hasFlag(c.transport.DisabledPushFlags(), PushFlagSubscribe) {
		return nil
	}
	replyData, err := c.getSubscribePushReply(channel, subCtx.result)
	if err != nil {
		return err
	}
	err = c.writeEncodedPushData(replyData, channel, "", protocol.FrameTypePushSubscribe, ChannelBatchConfig{})
	if err != nil {
		return err
	}
	if subCtx.clientInfo != nil {
		c.publishJoinAndPresence(channel, subCtx.channelContext, subCtx.clientInfo)
	}
	return nil
}

func (c *Client) getSubscribePushReply(channel string, res *protocol.SubscribeResult) ([]byte, error) {
	sub := &protocol.Subscribe{
		Offset:      res.GetOffset(),
		Epoch:       res.GetEpoch(),
		Recoverable: res.GetRecoverable(),
		Positioned:  res.GetPositioned(),
		Data:        res.Data,
	}
	push := &protocol.Push{
		Channel:   channel,
		Subscribe: sub,
	}
	if c.node.logEnabled(LogLevelTrace) {
		c.traceOutPush(push)
	}
	return c.encodeReply(&protocol.Reply{
		Push: push,
	})
}

// validateSubscribeRequest validates a subscribe request and, for the regular
// (non-map) flow, installs the channel reservation. It returns the generation
// minted for that reservation so error paths can undo exactly their own
// attempt; map subscribes reserve c.mapSubscribing instead and get 0.
func (c *Client) validateSubscribeRequest(cmd *protocol.SubscribeRequest) (uint64, *Error, *Disconnect) {
	channel := cmd.Channel
	if channel == "" {
		c.node.logger.log(newLogEntry(LogLevelInfo, "channel required for subscribe", map[string]any{"user": c.user, "client": c.uid}))
		return 0, nil, &DisconnectBadRequest
	}

	config := c.node.config
	channelMaxLength := config.ChannelMaxLength
	channelLimit := config.ClientChannelLimit

	if channelMaxLength > 0 && len(channel) > channelMaxLength {
		c.node.logger.log(newLogEntry(LogLevelInfo, "channel too long", map[string]any{"max": channelMaxLength, "channel": channel, "user": c.user, "client": c.uid}))
		return 0, ErrorBadRequest, nil
	}

	c.mu.Lock()

	// Check for map subscription continuation (pagination or live join).
	// These requests should be allowed even if mapSubscribing entry exists.
	// Type 1 = map data, Type 2 = clients presence, Type 3 = users presence.
	// All map-based subscriptions (type >= 1) share the same validation logic.
	if cmd.Type >= 1 {
		_, inMapSubscribing := c.mapSubscribing[channel]
		_, inChannels := c.channels[channel]

		// Allow continuation requests (cursor set or non-state phase).
		if inMapSubscribing && (cmd.Cursor != "" || cmd.Phase != MapPhaseState) {
			c.mu.Unlock()
			return 0, nil, nil
		}

		// If already fully subscribed, reject.
		if inChannels {
			c.mu.Unlock()
			c.node.logger.log(newLogEntry(LogLevelInfo, "client already subscribed on channel", map[string]any{"channel": channel, "user": c.user, "client": c.uid}))
			return 0, ErrorAlreadySubscribed, nil
		}

		// If already in map subscribing and this is an initial request, reject.
		if inMapSubscribing && cmd.Cursor == "" && cmd.Phase == MapPhaseState {
			c.mu.Unlock()
			c.node.logger.log(newLogEntry(LogLevelInfo, "client already subscribing on channel", map[string]any{"channel": channel, "user": c.user, "client": c.uid}))
			return 0, ErrorAlreadySubscribed, nil
		}

		// New map subscription - check channel limit.
		numChannels := len(c.channels) + len(c.mapSubscribing)
		if channelLimit > 0 && numChannels >= channelLimit {
			c.mu.Unlock()
			c.node.logger.log(newLogEntry(LogLevelInfo, "maximum limit of channels per client reached", map[string]any{"limit": channelLimit, "user": c.user, "client": c.uid}))
			return 0, ErrorLimitExceeded, nil
		}
		c.mu.Unlock()
		return 0, nil, nil
	} // TODO: it would be better to combine with normal flow, including having subscribingCh for map subs also.

	// Regular subscription validation.
	numChannels := len(c.channels) + len(c.mapSubscribing)
	_, ok := c.channels[channel]
	if !ok {
		// Also reject if a map subscribe is loading this channel: it reserves
		// mapSubscribing (not c.channels), and letting a normal subscribe reserve
		// c.channels in parallel breaks the one-in-flight-subscribe-per-channel
		// invariant commitSubscription relies on — the map sub going live would
		// overwrite the normal reservation and orphan its subscribingCh.
		_, ok = c.mapSubscribing[channel]
	}
	if ok {
		c.mu.Unlock()
		c.node.logger.log(newLogEntry(LogLevelInfo, "client already subscribed on channel", map[string]any{"channel": channel, "user": c.user, "client": c.uid}))
		return 0, ErrorAlreadySubscribed, nil
	}
	if channelLimit > 0 && numChannels >= channelLimit {
		c.mu.Unlock()
		c.node.logger.log(newLogEntry(LogLevelInfo, "maximum limit of channels per client reached", map[string]any{"limit": channelLimit, "user": c.user, "client": c.uid}))
		return 0, ErrorLimitExceeded, nil
	}
	// Put channel to a map to track duplicate subscriptions. This channel should
	// be removed from a map upon an error during subscribe. Also initialize subscribingCh
	// which is used to sync unsubscribe requests with inflight subscriptions (useful when
	// subscribe is performed in a separate goroutine).
	subGen := c.subGenCounter.Add(1)
	c.channels[channel] = ChannelContext{
		subscribingCh: make(chan struct{}),
		subGen:        subGen,
	}
	c.mu.Unlock()

	return subGen, nil, nil
}

func errorDisconnectContext(replyError *Error, disconnect *Disconnect) subscribeContext {
	ctx := subscribeContext{}
	if disconnect != nil {
		ctx.disconnect = disconnect
		return ctx
	}
	ctx.err = replyError
	return ctx
}

type subscribeContext struct {
	result         *protocol.SubscribeResult
	clientInfo     *ClientInfo
	err            *Error
	disconnect     *Disconnect
	channelContext ChannelContext
}

// publicationFiltered reports whether a publication with the given tags is
// excluded by the filter. A nil filter never excludes.
func publicationFiltered(tags map[string]string, tf *tagsFilter) bool {
	if tf == nil {
		return false
	}
	match, _ := filter.Match(tf.filter, tags)
	return !match
}

func isStreamRecovered(
	historyResult HistoryResult, cmdOffset uint64, cmdEpoch string, tf *tagsFilter, serverTf *tagsFilter,
) ([]*protocol.Publication, bool) {
	latestOffset := historyResult.Offset
	latestEpoch := historyResult.Epoch

	if cmdEpoch != "" && latestEpoch != cmdEpoch {
		// Epochs do not match, cannot recover.
		return nil, false
	}

	var recovered bool
	if len(historyResult.Publications) == 0 {
		recovered = latestOffset == cmdOffset
	} else {
		nextOffset := cmdOffset + 1
		recovered = historyResult.Publications[0].Offset == nextOffset &&
			historyResult.Publications[len(historyResult.Publications)-1].Offset == latestOffset
	}
	if !recovered {
		return nil, false
	}

	recoveredPubs := make([]*protocol.Publication, 0, len(historyResult.Publications))
	for _, pub := range historyResult.Publications {
		// Apply BOTH the server-enforced and the client-requested tags filters,
		// matching the live broadcast (hub.broadcastPublication). A publication
		// excluded by either is marked (Time == -1, offset only) rather than
		// dropped, so MergePublications can account for the missing offset in its
		// gap check instead of treating it as lost — again mirroring how the live
		// path buffers filtered publications.
		if publicationFiltered(pub.Tags, serverTf) || publicationFiltered(pub.Tags, tf) {
			recoveredPubs = append(recoveredPubs, &protocol.Publication{Offset: pub.Offset, Time: -1})
			continue
		}
		recoveredPubs = append(recoveredPubs, pubToProto(pub))
	}

	return recoveredPubs, recovered
}

func isCacheRecovered(
	latestPub *Publication, recoveredPub *Publication, currentSP StreamPosition, cmdOffset uint64, cmdEpoch string,
) ([]*protocol.Publication, bool) {
	latestOffset := currentSP.Offset
	latestEpoch := currentSP.Epoch

	// Check if client state matches current state.
	clientHasSameState := cmdOffset > 0 && cmdOffset == latestOffset && cmdEpoch == latestEpoch

	if latestPub == nil {
		return nil, clientHasSameState
	}

	recovered := latestPub.Offset == latestOffset
	if recovered && !clientHasSameState {
		return []*protocol.Publication{pubToProto(recoveredPub)}, true
	}

	return nil, recovered
}

const (
	subscriptionFlagChannelCompression = 1 << iota
	subscriptionFlagRejectUnrecovered
)

// reservationKind identifies where an in-flight subscribe parked its
// placeholder reservation so commitSubscription can release it uniformly.
type reservationKind uint8

const (
	// reservationChannels — the reservation is the placeholder ChannelContext in
	// c.channels (normal subscriptions).
	reservationChannels reservationKind = iota
	// reservationMap — the reservation is the mapSubscribeState in c.mapSubscribing
	// (map/keyed subscriptions load through a separate two-phase pipeline).
	reservationMap
)

// removeSubscribePresence undoes the node-level presence entry subscribeCmd
// added before the commit point (map presence and join are only set up after a
// successful commit, so they need no rollback there). Without this, a rolled
// back subscribe leaves a presence entry behind until PresenceTTL: close()
// cannot remove it because the channel never entered its snapshot — that is
// exactly why the rollback runs. Removal is idempotent. Must be called without
// c.mu held (network call).
func (c *Client) removeSubscribePresence(channel string, flags uint16) {
	if !channelHasFlag(flags, flagEmitPresence) {
		return
	}
	if err := c.node.removePresence(channel, c.uid, c.user); err != nil {
		c.node.logger.log(newErrorLogEntry(err, "error removing presence on subscribe rollback", map[string]any{
			"channel": channel, "user": c.user, "client": c.uid, "error": err.Error()}))
	}
}

// commitSubscription is the single consistency-critical commit point shared by
// normal and map subscriptions. Under c.mu it either installs the finished
// subscription's channel context into c.channels, or — if the client closed
// while the subscribe was in flight — rolls it back.
//
// On success it returns the reservation's subscribingCh (or nil) and true. The
// caller is responsible for closing that channel to release any unsubscribe
// waiting on the in-flight subscribe, at the point that preserves its own
// ordering (inline, or deferred until after presence/join work so an unsubscribe
// cannot remove presence before this subscribe added it).
//
// On a closed client it drops the reservation, removes the hub entry registered
// earlier in the subscribe (ctx.subGen must match), closes its subscribingCh —
// in that order, so a woken waiter never observes a half-rolled-back attempt —
// and returns (nil, false); the caller adds any path-specific cleanup
// (reply/buffer release) and returns its own error. Lock must NOT be held.
//
// The reservation is consumed only if it still carries this attempt's generation
// (ctx.subGen). A subscribe stalled past the unsubscribe wait timeout can lose
// its reservation: the timeout path nils the reservation's subscribingCh, a
// second unsubscribe then gen-matches and deletes the entry, and a fresh
// subscribe may reserve the channel with a new generation. Without the gen check
// this late commit would close the fresh reservation's subscribingCh (waking its
// waiters early), overwrite c.channels with a context that has no hub routing —
// or, with no fresh reservation, resurrect the unsubscribed channel outright.
// On mismatch the commit rolls back its own hub entry and reports not committed;
// the connection is already being force-closed by the timeout path.
func (c *Client) commitSubscription(channel string, ctx ChannelContext, kind reservationKind) (chan struct{}, bool) {
	c.mu.Lock()
	var subscribingCh chan struct{}
	reservationLost := false
	switch kind {
	case reservationChannels:
		if resv, ok := c.channels[channel]; ok && resv.subGen == ctx.subGen {
			subscribingCh = resv.subscribingCh
		} else {
			reservationLost = true
		}
	case reservationMap:
		// Only the map reservation is identity-checked; c.channels is written
		// unconditionally on purpose. The hub keeps exactly one entry per
		// (channel, client) and addSub overwrites it, so this subscribe's own
		// addSubscription already replaced whatever entry was there. Refusing to
		// install here would leave c.channels carrying an older subscription while
		// the hub carries this generation — and the rollback would then remove the
		// hub entry, producing a channel that looks subscribed but receives
		// nothing. Racing subscribes are prevented at the source instead:
		// validateSubscribeRequest and Client.Subscribe both reject a channel that
		// is already in c.channels or c.mapSubscribing.
		if st, ok := c.mapSubscribing[channel]; ok && st.subGen == ctx.subGen {
			subscribingCh = st.subscribingCh
			delete(c.mapSubscribing, channel)
		} else {
			reservationLost = true
		}
	default:
		// Unreachable: reservationKind is internal with exactly these two values.
		// Panic rather than silently defaulting to a store, so a future kind that
		// forgets to extend this switch fails loudly instead of corrupting state.
		panic("centrifuge: unknown reservationKind")
	}
	if reservationLost {
		// The entry (if any) belongs to a different subscription attempt — do not
		// touch it or its subscribingCh. Roll back only what this attempt owns:
		// its recovery buffer and its hub entry (gen-matched, so a no-op if the
		// unsubscribe already removed it). StopBuffering-before-removeSubscription
		// ordering as in the closed path below.
		c.mu.Unlock()
		c.pubSubSync.StopBuffering(channel)
		_ = c.node.removeSubscription(channel, c, ctx.subGen)
		if kind == reservationChannels {
			// The normal path adds node-level presence before the commit; the
			// gen-matched unsubscribe that took our reservation may have run its
			// presence removal before that add landed, so remove it again here.
			c.removeSubscribePresence(channel, ctx.flags)
		}
		return nil, false
	}
	if c.status == statusClosed {
		// close() snapshotted c.channels before this subscribe finalized, so the
		// hub entry added earlier would leak (a routing entry to a dead client).
		// Drop the reservation and undo the hub registration for this generation.
		if kind == reservationChannels {
			delete(c.channels, channel)
		}
		c.mu.Unlock()
		// Release the recovery buffer before removing the hub entry. A positioned/
		// recovering subscribe holds pubBufferMu (LockBufferAndReadBuffered) until
		// StopBuffering, and removeSubscription takes subShard.mu; the broadcast
		// path holds subShard.mu while taking pubBufferMu, so removing the hub entry
		// with the buffer still locked inverts that order and deadlocks. StopBuffering
		// is idempotent, so calling it here (and again in the caller) is safe.
		c.pubSubSync.StopBuffering(channel)
		_ = c.node.removeSubscription(channel, c, ctx.subGen)
		if kind == reservationChannels {
			// Presence was added before the commit on the normal path, and close()
			// snapshotted c.channels before this channel entered it — remove the
			// entry here or it lingers until PresenceTTL.
			c.removeSubscribePresence(channel, ctx.flags)
		}
		// Wake any waiter only after this attempt's rollback is complete (same
		// ordering as onSubscribeErrorGen). The waiter is typically the unsubscribe
		// loop inside close(), and close() fires OnDisconnect right after it — so
		// closing this first would let a caller observe a "fully disconnected"
		// client while the hub entry we are about to remove is still registered.
		if subscribingCh != nil {
			close(subscribingCh)
		}
		return nil, false
	}
	ctx.subscribingCh = nil
	c.channels[channel] = ctx
	c.mu.Unlock()
	// Let the compression engine see what this connection is subscribed to. A
	// connection may only use a dictionary built from a channel it can read, so
	// this is what keeps dictionary content inside its trust boundary.
	if ca, ok := c.transport.(compressionAware); ok {
		if cc := ca.connectionCompression(); cc != nil {
			cc.OnSubscribe(channel)
		}
	}
	return subscribingCh, true
}

// subscribeCmd handles subscribe command - clients send this when subscribe
// on channel, if channel is private then we must validate provided sign here before
// actually subscribe client on channel. Optionally we can send missed messages to
// client if it provided last message id seen in channel.
func (c *Client) subscribeCmd(req *protocol.SubscribeRequest, reply SubscribeReply, cmd *protocol.Command, serverSide bool, started time.Time, rw *replyWriter) (retCtx subscribeContext) {
	ctx := subscribeContext{}

	// Presence is added partway through, before the subscription is committed, so
	// any failure after that point must undo it. Callers only clean up the
	// reservation and the hub entry (onSubscribeErrorGen), and close() cannot help —
	// the channel never became a committed subscription — so without this the
	// entry lingers until PresenceTTL. Covers the failure paths that return a
	// zero-valued channelContext (recovery/history errors, encode errors), which
	// carry no flags for the commit-time rollback to act on. Removal is
	// idempotent, so overlapping with commitSubscription's own rollback is safe.
	presenceAdded := false
	defer func() {
		if presenceAdded && (retCtx.disconnect != nil || retCtx.err != nil) {
			c.removeSubscribePresence(req.Channel, flagEmitPresence)
		}
	}()

	res := &protocol.SubscribeResult{}

	if reply.Options.ExpireAt > 0 {
		ttl := reply.Options.ExpireAt - time.Now().Unix()
		if ttl <= 0 {
			c.node.logger.log(newLogEntry(LogLevelInfo, "subscription expiration must be greater than now", map[string]any{"client": c.uid, "user": c.UserID()}))
			return errorDisconnectContext(ErrorExpired, nil)
		}
		if reply.ClientSideRefresh {
			res.Expires = true
			res.Ttl = uint32(ttl)
		}
	}

	if reply.Options.Data != nil {
		res.Data = reply.Options.Data
	}

	if d := reply.Options.ClientPublishDebounceInterval; d > 0 {
		res.PublishDebounce = uint32(d.Milliseconds())
	}

	channel := req.Channel

	info := &ClientInfo{
		ClientID: c.uid,
		UserID:   c.user,
		ConnInfo: c.info,
		ChanInfo: reply.Options.ChannelInfo,
	}

	useID := reply.Options.AllowChannelCompaction && req.Flag&subscriptionFlagChannelCompression != 0
	// One generation per subscription attempt, carried into both the hub entry
	// (subInfo.subGen) and the client channel context (ChannelContext.subGen), so
	// unsubscribe can remove exactly the generation it saw. When the caller already
	// reserved the channel (Client.Subscribe / validateSubscribeRequest), reuse the
	// reservation's generation so it is stable from reservation through finalize —
	// that lets a racing unsubscribe identity-match and avoid clobbering a fresh
	// concurrent subscribe. Paths without a reservation (initial server-side
	// subscribe on connect) get a fresh generation here.
	c.mu.Lock()
	subGen := c.channels[channel].subGen
	if subGen == 0 {
		// No generation on the reservation (or no reservation at all, for the
		// connect-time server-side path that subscribes before reserving). Mint
		// one and stamp it back so the commit's identity check — reservation gen
		// == context gen — holds for this attempt. Done under the write lock
		// because the read and the write-back must be atomic against a concurrent
		// unsubscribe reading the same field.
		subGen = c.subGenCounter.Add(1)
		if resv, ok := c.channels[channel]; ok {
			resv.subGen = subGen
			c.channels[channel] = resv
		}
	}
	c.mu.Unlock()
	sub := subInfo{client: c, deltaType: deltaTypeNone, useID: useID, subGen: subGen}
	if req.Tf != nil {
		if !reply.Options.AllowTagsFilter {
			c.node.logger.log(newLogEntry(LogLevelInfo, "tags filter not allowed", map[string]any{"channel": channel, "user": c.user, "client": c.uid}))
			return errorDisconnectContext(ErrorBadRequest, nil)
		}
		if err := filter.Validate(req.Tf); err != nil {
			c.node.logger.log(newLogEntry(LogLevelInfo, "invalid tags filter", map[string]any{"channel": channel, "user": c.user, "client": c.uid}))
			return errorDisconnectContext(ErrorBadRequest, nil)
		}
		sub.tagsFilter = &tagsFilter{
			filter: req.Tf,
			hash:   filter.Hash(req.Tf),
		}
	}
	if reply.Options.ServerTagsFilter != nil {
		sub.serverTagsFilter = &tagsFilter{
			filter: reply.Options.ServerTagsFilter,
			hash:   filter.Hash(reply.Options.ServerTagsFilter),
		}
	}
	hasServerTagsFilter := reply.Options.ServerTagsFilter != nil

	needPubSubSync := reply.Options.EnablePositioning || reply.Options.EnableRecovery
	if needPubSubSync {
		// Start syncing recovery and PUB/SUB.
		// The important thing is to call StopBuffering for this channel
		// after response with Publications written to connection.
		c.pubSubSync.StartBuffering(channel)
	}

	if req.Delta != "" {
		dt := DeltaType(req.Delta)
		if slices.Contains(reply.Options.AllowedDeltaTypes, dt) {
			res.Delta = true
			sub.deltaType = dt
		}
	}

	if !serverSide {
		c.mu.Lock()
		_, ok := c.channels[channel]
		if !ok || c.status == statusClosed {
			c.mu.Unlock()
			c.pubSubSync.StopBuffering(channel)
			c.node.logger.log(newLogEntry(LogLevelInfo, "client closed or unsubscribed before adding subscription", map[string]any{"channel": channel, "user": c.user, "client": c.uid}))
			ctx.disconnect = &DisconnectServerError
			return ctx
		}
		c.mu.Unlock()
	}
	chanID, err := c.node.addSubscription(channel, sub)
	if err != nil {
		c.node.logger.log(newErrorLogEntry(err, "error adding subscription", map[string]any{"channel": channel, "user": c.user, "client": c.uid, "error": err.Error()}))
		c.pubSubSync.StopBuffering(channel)
		var clientErr *Error
		if errors.As(err, &clientErr) && !errors.Is(clientErr, ErrorInternal) {
			return errorDisconnectContext(clientErr, nil)
		}
		ctx.disconnect = &DisconnectServerError
		return ctx
	}
	if chanID > 0 {
		res.Id = chanID
	}
	if !serverSide {
		c.mu.Lock()
		_, ok := c.channels[channel]
		if !ok || c.status == statusClosed {
			c.mu.Unlock()
			c.pubSubSync.StopBuffering(channel)
			c.node.logger.log(newErrorLogEntry(err, "client closed or unsubscribed after adding subscription", map[string]any{"channel": channel, "user": c.user, "client": c.uid}))
			ctx.disconnect = &DisconnectServerError
			return ctx
		}
		c.mu.Unlock()
	}

	if reply.Options.EmitPresence {
		err = c.node.addPresence(channel, c.uid, info)
		// Marked even on error: the add may have landed server-side before
		// reporting failure, and removal is idempotent.
		presenceAdded = true
		if err != nil {
			c.node.logger.log(newErrorLogEntry(err, "error adding presence", map[string]any{"channel": channel, "user": c.user, "client": c.uid, "error": err.Error()}))
			c.pubSubSync.StopBuffering(channel)
			ctx.disconnect = &DisconnectServerError
			return ctx
		}
	}

	var (
		latestOffset  uint64
		latestEpoch   string
		recoveredPubs []*protocol.Publication
		maxSeenOffset uint64
	)

	// autoCacheRecover reflects server-forced cache recovery via SubscribeOptions.AutoCacheRecover
	// (no client position). It only applies in cache recovery mode, where it delivers the latest
	// publication regardless of position. In stream recovery mode it is intentionally ignored:
	// forcing recovery without a position can't preserve continuity, and delivering history
	// there would break the "recovered=false implies no publications" contract. Stream
	// subscriptions that want to recover from a specific position must use req.Recover (client)
	// or SubscribeOptions.RecoverSince (server).
	autoCacheRecover := reply.Options.AutoCacheRecover && reply.Options.RecoveryMode == RecoveryModeCache

	if reply.Options.EnablePositioning || reply.Options.EnableRecovery {
		handleErr := func(err error) subscribeContext {
			c.pubSubSync.StopBuffering(channel)
			var clientErr *Error
			if errors.As(err, &clientErr) && !errors.Is(clientErr, ErrorInternal) {
				return errorDisconnectContext(clientErr, nil)
			}
			ctx.disconnect = &DisconnectServerError
			return ctx
		}

		res.Positioned = true
		if reply.Options.EnableRecovery {
			res.Recoverable = true
		}

		// Recovery is attempted either when the client itself asked for it (req.Recover)
		// or when the server forces it in cache recovery mode via SubscribeOptions.AutoCacheRecover
		// (e.g. for server-side subscriptions of unidirectional clients, to deliver the latest
		// publication without a client-provided position).
		if reply.Options.EnableRecovery && (req.Recover || autoCacheRecover) {
			cmdOffset := req.Offset
			cmdEpoch := req.Epoch
			recoveryMode := reply.Options.RecoveryMode

			// Try to recover missed publications automatically from history (we assume here
			// that the history configured wisely).

			if recoveryMode == RecoveryModeCache {
				var latestPub *Publication
				var recoveredPub *Publication
				var currentSP StreamPosition
				latestPub, recoveredPub, currentSP, err = c.node.recoverCache(channel, reply.Options.HistoryMetaTTL, sub.tagsFilter, sub.serverTagsFilter)
				if err != nil {
					c.node.logger.log(newErrorLogEntry(err, "error on cache recover", map[string]any{"channel": channel, "user": c.user, "client": c.uid, "error": err.Error()}))
					return handleErr(err)
				}
				latestOffset = currentSP.Offset
				latestEpoch = currentSP.Epoch
				var recovered bool
				recoveredPubs, recovered = isCacheRecovered(latestPub, recoveredPub, currentSP, cmdOffset, cmdEpoch)
				res.Recovered = recovered
				if latestPub == nil && c.node.clientEvents.cacheEmptyHandler != nil {
					cacheReply, err := c.node.clientEvents.cacheEmptyHandler(CacheEmptyEvent{Channel: channel})
					if err != nil {
						c.node.logger.log(newErrorLogEntry(err, "error on cache empty", map[string]any{"channel": channel, "user": c.user, "client": c.uid, "error": err.Error()}))
						return handleErr(err)
					}
					if cacheReply.Populated && !recovered {
						// One more chance to recover in case we know cache was populated.
						latestPub, recoveredPub, currentSP, err = c.node.recoverCache(channel, reply.Options.HistoryMetaTTL, sub.tagsFilter, sub.serverTagsFilter)
						if err != nil {
							c.node.logger.log(newErrorLogEntry(err, "error on populated cache recover", map[string]any{"channel": channel, "user": c.user, "client": c.uid, "error": err.Error()}))
							return handleErr(err)
						}
						latestOffset = currentSP.Offset
						latestEpoch = currentSP.Epoch
						recoveredPubs, recovered = isCacheRecovered(latestPub, recoveredPub, currentSP, cmdOffset, cmdEpoch)
						res.Recovered = recovered
						c.node.metrics.incRecover(res.Recovered, channel, len(recoveredPubs) > 0)
						if res.Recovered {
							c.node.metrics.observeRecoveredPublications(len(recoveredPubs), channel)
						}
					} else {
						c.node.metrics.incRecover(res.Recovered, channel, len(recoveredPubs) > 0)
						if res.Recovered {
							c.node.metrics.observeRecoveredPublications(len(recoveredPubs), channel)
						}
					}
				} else {
					c.node.metrics.incRecover(res.Recovered, channel, len(recoveredPubs) > 0)
					if res.Recovered {
						c.node.metrics.observeRecoveredPublications(len(recoveredPubs), channel)
					}
				}
			} else {
				historyResult, err := c.node.recoverHistory(channel, StreamPosition{Offset: cmdOffset, Epoch: cmdEpoch}, reply.Options.HistoryMetaTTL)
				if err != nil {
					if errors.Is(err, ErrorUnrecoverablePosition) {
						if req.Flag&subscriptionFlagRejectUnrecovered != 0 {
							c.pubSubSync.StopBuffering(channel)
							return errorDisconnectContext(ErrorUnrecoverablePosition, nil)
						}
						// Result contains stream position in case of ErrorUnrecoverablePosition
						// during recovery.
						latestOffset = historyResult.Offset
						latestEpoch = historyResult.Epoch
						res.Recovered = false
						c.node.metrics.incRecover(res.Recovered, channel, false)
					} else {
						c.node.logger.log(newErrorLogEntry(err, "error on recover", map[string]any{"channel": channel, "user": c.user, "client": c.uid, "error": err.Error()}))
						return handleErr(err)
					}
				} else {
					latestOffset = historyResult.Offset
					latestEpoch = historyResult.Epoch
					var recovered bool
					recoveredPubs, recovered = isStreamRecovered(historyResult, cmdOffset, cmdEpoch, sub.tagsFilter, sub.serverTagsFilter)
					if !recovered && req.Flag&subscriptionFlagRejectUnrecovered != 0 {
						c.pubSubSync.StopBuffering(channel)
						return errorDisconnectContext(ErrorUnrecoverablePosition, nil)
					}
					res.Recovered = recovered
					// isStreamRecovered marks filtered publications with Time == -1
					// (stripped later by MergePublications). Exclude those markers from
					// the recovery metrics so the recovered-publications histogram and
					// the has-publications flag reflect what is actually delivered.
					realRecovered := 0
					for _, p := range recoveredPubs {
						if p.Time != -1 {
							realRecovered++
						}
					}
					c.node.metrics.incRecover(res.Recovered, channel, realRecovered > 0)
					if res.Recovered {
						c.node.metrics.observeRecoveredPublications(realRecovered, channel)
					}
				}
			}
		} else {
			streamTop, err := c.node.streamTop(channel, reply.Options.HistoryMetaTTL)
			if err != nil {
				c.node.logger.log(newErrorLogEntry(err, "error getting stream state for channel", map[string]any{"channel": channel, "user": c.user, "client": c.uid, "error": err.Error()}))
				return handleErr(err)
			}
			latestOffset = streamTop.Offset
			latestEpoch = streamTop.Epoch
		}

		res.Epoch = latestEpoch
		res.Offset = latestOffset

		bufferedPubs := c.pubSubSync.LockBufferAndReadBuffered(channel)
		var okMerge bool
		recoveredPubs, maxSeenOffset, okMerge = recovery.MergePublications(recoveredPubs, bufferedPubs)
		if !okMerge {
			c.pubSubSync.StopBuffering(channel)
			ctx.disconnect = &DisconnectInsufficientState
			return ctx
		}
		if reply.Options.RecoveryMode == RecoveryModeCache && len(recoveredPubs) > 1 && req.Delta == "" {
			// In RecoveryModeCache case client is only interested in last message. So if delta encoding is
			// not used then we can only send the last publication.
			recoveredPubs = recoveredPubs[len(recoveredPubs)-1:]
		}
	}

	if len(recoveredPubs) > 0 {
		lastPubOffset := recoveredPubs[len(recoveredPubs)-1].Offset
		if lastPubOffset > res.Offset {
			// There can be a case when recovery returned a limited set of publications
			// thus last publication offset will be smaller than history current offset.
			// In this case res.Recovered will be false. So we take a maximum here.
			latestOffset = lastPubOffset
			res.Offset = latestOffset
		}
	}

	if maxSeenOffset > latestOffset {
		// Some entries could be filtered, but it's normal so we put max seen offset as the latest.
		latestOffset = maxSeenOffset
		res.Offset = latestOffset
	}

	var channelFlags uint16

	if res.Recovered {
		// Only append recovered publications in case continuity in a channel can be achieved.
		if res.Delta && req.Delta == string(DeltaTypeFossil) {
			res.Publications = c.makeRecoveredPubsDeltaFossil(recoveredPubs)
			// Allow delta for the following real-time publications since recovery is successful
			// and makeRecoveredPubsDeltaFossil already created publication with base data if required.
			channelFlags |= flagDeltaAllowed
		} else {
			res.Publications = recoveredPubs
		}
		// In case of successful recovery attach stream offset from request to subscribe response.
		// This simplifies client implementation as it doesn't need to distinguish between cases when
		// subscribe response has recovered publications, or it has no recovered publications.
		// Valid stream position will be then caught up upon processing publications.
		res.Offset = req.Offset
	}
	// WasRecovering reflects whether a recovery attempt was made for this subscription –
	// either requested by the client (req.Recover) or forced by the server in cache recovery
	// mode via SubscribeOptions.AutoCacheRecover. This keeps the documented invariant that
	// Recovered can only be true when WasRecovering is true, and makes a server-forced recovery
	// look the same to the client as a client-initiated one.
	res.WasRecovering = req.Recover || (reply.Options.EnableRecovery && autoCacheRecover)

	// Append publications from subscribe reply (e.g., initial full state).
	if len(reply.Publications) > 0 && len(res.Publications) == 0 {
		protoPubs := make([]*protocol.Publication, 0, len(reply.Publications))
		for _, pub := range reply.Publications {
			protoPubs = append(protoPubs, pubToProto(pub))
		}
		res.Publications = protoPubs
	}

	if !serverSide {
		// Write subscription reply only if initiated by client.
		protoReply, err := c.getSubscribeCommandReply(res)
		if err != nil {
			c.node.logger.log(newErrorLogEntry(err, "error encoding subscribe", map[string]any{"error": err.Error()}))
			c.pubSubSync.StopBuffering(channel) // Will be called later in case of server side sub.
			ctx.disconnect = &DisconnectServerError
			return ctx
		}

		// Need to flush data from writer so subscription response is
		// sent before any subscription publication.
		c.writeEncodedCommandReply(channel, protocol.FrameTypeSubscribe, cmd, protoReply, rw)
		defer c.releaseSubscribeCommandReply(protoReply)
		defer c.handleCommandFinished(cmd, protocol.FrameTypeSubscribe, nil, protoReply, started, "")
	}

	channelFlags |= flagSubscribed
	if serverSide {
		channelFlags |= flagServerSide
	}
	if reply.ClientSideRefresh {
		channelFlags |= flagClientSideRefresh
	}
	if reply.Options.EnablePositioning || reply.Options.EnableRecovery {
		channelFlags |= flagPositioning
	}
	if reply.Options.EmitPresence {
		channelFlags |= flagEmitPresence
	}
	if reply.Options.EmitJoinLeave {
		channelFlags |= flagEmitJoinLeave
	}
	if reply.Options.PushJoinLeave {
		channelFlags |= flagPushJoinLeave
	}
	if reply.Options.MapClientPresenceChannel != "" {
		channelFlags |= flagMapClientPresence
	}
	if reply.Options.MapUserPresenceChannel != "" {
		channelFlags |= flagMapUserPresence
	}
	if hasServerTagsFilter {
		channelFlags |= flagServerTagsFilter
	}

	channelContext := ChannelContext{
		info:     reply.Options.ChannelInfo,
		flags:    channelFlags,
		expireAt: reply.Options.ExpireAt,
		streamPosition: StreamPosition{
			Offset: latestOffset,
			Epoch:  latestEpoch,
		},
		metaTTLSeconds:           int64(reply.Options.HistoryMetaTTL.Seconds()),
		Source:                   reply.Options.Source,
		mapClientPresenceChannel: reply.Options.MapClientPresenceChannel,
		mapUserPresenceChannel:   reply.Options.MapUserPresenceChannel,
		subGen:                   subGen,
	}
	if reply.Options.EnableRecovery || reply.Options.EnablePositioning {
		channelContext.positionCheckTime = time.Now().Unix()
	}

	if !serverSide {
		// In case of server-side sub this will be done later by the caller.
		// Same commit as the server-side and map paths. If the client closed
		// mid-subscribe the reservation and hub entry are rolled back and we abort
		// so the caller runs onSubscribeErrorGen.
		subscribingCh, committed := c.commitSubscription(channel, channelContext, reservationChannels)
		if !committed {
			// Client closed mid-subscribe. Release the recovery/PUB-SUB buffer
			// StartBuffering opened above before aborting — every other return path
			// in subscribeCmd pairs StartBuffering with StopBuffering, and the
			// success path below does too.
			c.pubSubSync.StopBuffering(channel)
			ctx.disconnect = &DisconnectServerError
			return ctx
		}
		if subscribingCh != nil {
			// Release any unsubscribe waiting on this in-flight subscribe. Deferred
			// to subscribeCmd's return (matching the prior behavior); by this point
			// addPresence above has already run, so a woken unsubscribe's
			// removePresence cannot precede this subscribe's presence add. This is
			// our own captured channel — no identity-match is needed, and
			// commitSubscription already cleared subscribingCh in c.channels, so a
			// racing unsubscribe cannot double-close it.
			defer close(subscribingCh)
		}
		// Stop syncing recovery and PUB/SUB.
		// In case of server side subscription we will do this later.
		c.pubSubSync.StopBuffering(channel)
	}

	if c.node.logger.enabled(LogLevelDebug) {
		c.node.logger.log(newLogEntry(LogLevelDebug, "client subscribed to channel", map[string]any{"client": c.uid, "user": c.user, "channel": req.Channel}))
	}

	ctx.result = res
	ctx.clientInfo = info
	ctx.channelContext = channelContext
	return ctx
}

func (c *Client) makeRecoveredPubsDeltaFossil(recoveredPubs []*protocol.Publication) []*protocol.Publication {
	if len(recoveredPubs) == 0 {
		return nil
	}
	prevPub := recoveredPubs[0]
	if c.transport.Protocol() == ProtocolTypeJSON {
		// For JSON case we need to use JSON string (js) for data.
		pub := &protocol.Publication{
			Offset: prevPub.Offset,
			Info:   prevPub.Info,
			Tags:   prevPub.Tags,
			Data:   json.Escape(convert.BytesToString(prevPub.Data)),
			Delta:  false,
		}
		recoveredPubs[0] = pub
	}
	// Probably during recovery we should not make deltas? This is something to investigate, in
	// RecoveryModeCache case this won't be used since there is only one publication max recovered.
	if len(recoveredPubs) > 1 {
		for i, pub := range recoveredPubs[1:] {
			patch := fdelta.Create(prevPub.Data, pub.Data)
			delta := true
			deltaData := patch
			if len(patch) >= len(pub.Data) {
				delta = false
				deltaData = pub.Data
			}
			if c.transport.Protocol() == ProtocolTypeJSON {
				deltaData = json.Escape(convert.BytesToString(deltaData))
			}
			deltaPub := &protocol.Publication{
				Offset: pub.Offset,
				Data:   deltaData,
				Info:   pub.Info,
				Tags:   pub.Tags,
				Delta:  delta,
			}
			prevPub = recoveredPubs[i+1]
			recoveredPubs[i+1] = deltaPub
		}
	}
	return recoveredPubs
}

// makeRecoveredMapPubsDeltaFossil is a per-key variant of makeRecoveredPubsDeltaFossil
// for map subscriptions. Live publications use per-key delta (delta from previous state
// value for the same key), so recovery publications must use the same strategy. The
// sequential delta used by makeRecoveredPubsDeltaFossil would produce wrong bases when
// publications for different keys are interleaved.
func (c *Client) makeRecoveredMapPubsDeltaFossil(recoveredPubs []*protocol.Publication) []*protocol.Publication {
	if len(recoveredPubs) == 0 {
		return nil
	}
	isJSON := c.transport.Protocol() == ProtocolTypeJSON
	prevByKey := make(map[string]*protocol.Publication)
	for i, pub := range recoveredPubs {
		key := pub.Key
		if pub.Removed {
			// Removals are not delta-encoded (matches live behavior).
			delete(prevByKey, key)
			if isJSON && len(pub.Data) > 0 {
				recoveredPubs[i] = copyMapPubWithData(pub, json.Escape(convert.BytesToString(pub.Data)), false) //nolint:gosec // i is from range recoveredPubs
			}
			continue
		}
		prev, hasPrev := prevByKey[key]
		if !hasPrev {
			// First occurrence of this key — send full data.
			prevByKey[key] = pub
			if isJSON {
				recoveredPubs[i] = copyMapPubWithData(pub, json.Escape(convert.BytesToString(pub.Data)), false)
			}
			continue
		}
		// Subsequent occurrence — compute per-key delta.
		patch := fdelta.Create(prev.Data, pub.Data)
		delta := true
		deltaData := patch
		if len(patch) >= len(pub.Data) {
			delta = false
			deltaData = pub.Data
		}
		if isJSON {
			deltaData = json.Escape(convert.BytesToString(deltaData))
		}
		prevByKey[key] = pub
		recoveredPubs[i] = copyMapPubWithData(pub, deltaData, delta)
	}
	return recoveredPubs
}

func copyMapPubWithData(pub *protocol.Publication, data []byte, delta bool) *protocol.Publication {
	return &protocol.Publication{
		Offset:  pub.Offset,
		Data:    data,
		Info:    pub.Info,
		Tags:    pub.Tags,
		Delta:   delta,
		Key:     pub.Key,
		Removed: pub.Removed,
		Score:   pub.Score,
	}
}

func (c *Client) releaseSubscribeCommandReply(reply *protocol.Reply) {
	protocol.ReplyPool.ReleaseSubscribeReply(reply)
}

func (c *Client) getSubscribeCommandReply(res *protocol.SubscribeResult) (*protocol.Reply, error) {
	return protocol.ReplyPool.AcquireSubscribeReply(res), nil
}

func (c *Client) handleInsufficientState(ch string, serverSide bool) {
	if c.isAsyncUnsubscribe(serverSide) {
		c.handleAsyncUnsubscribe(ch, unsubscribeInsufficientState)
	} else {
		c.handleInsufficientStateDisconnect()
	}
}

func (c *Client) isAsyncUnsubscribe(serverSide bool) bool {
	return !serverSide
}

func (c *Client) handleInsufficientStateDisconnect() {
	_ = c.close(DisconnectInsufficientState)
}

func (c *Client) handleAsyncUnsubscribe(ch string, unsub Unsubscribe) {
	err := c.unsubscribe(ch, unsub, nil)
	if err != nil {
		c.node.logger.log(newErrorLogEntry(err, "error async unsubscribing", map[string]any{"channel": ch, "user": c.user, "client": c.uid, "error": err.Error()}))
		_ = c.close(DisconnectServerError)
		return
	}
	err = c.sendUnsubscribe(ch, unsub)
	if err != nil {
		_ = c.close(DisconnectWriteError)
		return
	}
}

func (c *Client) writePublicationUpdatePosition(
	ch string, pub *protocol.Publication, prep preparedData, sp StreamPosition, maxLagExceeded bool,
	batchConfig ChannelBatchConfig,
) error {
	c.mu.Lock()
	channelContext, ok := c.channels[ch]
	if !ok || !channelHasFlag(channelContext.flags, flagSubscribed) {
		c.mu.Unlock()
		return nil
	}
	deltaAllowed := channelHasFlag(channelContext.flags, flagDeltaAllowed)
	if !channelHasFlag(channelContext.flags, flagPositioning) {
		// Publication with Offset, but client does not use positioning.
		if hasFlag(c.transport.DisabledPushFlags(), PushFlagPublication) {
			c.mu.Unlock()
			return nil
		}
		c.mu.Unlock()
		if pub.Offset == math.MaxUint64 {
			// This is a special pub to trigger insufficient state. Noop in non-positioning case.
			return nil
		}

		// For non-positioning case, if publication should be filtered, skip it
		if prep.wasFiltered && !prep.deltaSub {
			return nil
		}

		if prep.deltaSub {
			if deltaAllowed {
				return c.writeEncodedPushData(prep.localDeltaData, ch, pub.Key, protocol.FrameTypePushPublication, batchConfig)
			}
			c.mu.Lock()
			if chCtx, chCtxOK := c.channels[ch]; chCtxOK {
				chCtx.flags |= flagDeltaAllowed
				c.channels[ch] = chCtx
			}
			c.mu.Unlock()
		}

		if c.node.logEnabled(LogLevelTrace) {
			c.traceOutPush(&protocol.Push{Channel: ch, Pub: pub})
		}
		return c.writeEncodedPushData(prep.fullData, ch, pub.Key, protocol.FrameTypePushPublication, batchConfig)
	}
	serverSide := channelHasFlag(channelContext.flags, flagServerSide)
	currentPositionOffset := channelContext.streamPosition.Offset
	nextExpectedOffset := currentPositionOffset + 1
	pubOffset := pub.Offset
	pubEpoch := sp.Epoch
	if maxLagExceeded {
		// PUB/SUB lag is too big.
		// We can introduce an option to mark connection with insufficient state flag instead
		// of disconnecting it immediately. In that case connection will eventually reconnect
		// due to periodic sync. While connection channel is in the insufficient state we must
		// skip publications coming to it. This mode may be useful to spread the resubscribe load.
		if c.node.logger.enabled(LogLevelDebug) {
			c.node.logger.log(newLogEntry(LogLevelDebug, "client insufficient state (lag)", map[string]any{"channel": ch, "user": c.user, "client": c.uid}))
		}
		// Tell client about insufficient state, can reconnect/resubscribe to recover the state.
		go func() { c.handleInsufficientState(ch, serverSide) }()
		c.mu.Unlock()
		return nil
	}
	if pubEpoch != channelContext.streamPosition.Epoch {
		if channelContext.streamPosition.Epoch == "" {
			// Channel subscribed when no data existed (empty epoch) — e.g. due to
			// a lagging read replica that didn't have the meta row yet. The first
			// publication carries the real epoch — adopt it. This avoids a needless
			// re-subscribe and is safe: the only way to have epoch="" is "no data
			// existed at subscribe time", so there is no stale state to protect.
			// Applies to both stream and map subscriptions.
			channelContext.streamPosition.Epoch = pubEpoch
			c.channels[ch] = channelContext
		} else {
			// Real epoch mismatch (e.g. after Clear) — insufficient state.
			if c.node.logger.enabled(LogLevelDebug) {
				c.node.logger.log(newLogEntry(LogLevelDebug, "client insufficient state (epoch)", map[string]any{"channel": ch, "user": c.user, "client": c.uid, "epoch": pubEpoch, "expectedEpoch": channelContext.streamPosition.Epoch}))
			}
			go func() { c.handleInsufficientState(ch, serverSide) }()
			c.mu.Unlock()
			return nil
		}
	}
	if pubOffset > nextExpectedOffset {
		// Missed message detected.
		// We can introduce an option to mark connection with insufficient state flag instead
		// of disconnecting it immediately. In that case connection will eventually reconnect
		// due to periodic sync. While connection channel is in the insufficient state we must
		// skip publications coming to it. This mode may be useful to spread the resubscribe load.
		if c.node.logger.enabled(LogLevelDebug) {
			c.node.logger.log(newLogEntry(LogLevelDebug, "client insufficient state (offset)", map[string]any{"channel": ch, "user": c.user, "client": c.uid, "offset": pubOffset, "expectedOffset": nextExpectedOffset}))
		}
		// Tell client about insufficient state, can reconnect/resubscribe to recover the state.
		go func() { c.handleInsufficientState(ch, serverSide) }()
		c.mu.Unlock()
		return nil
	} else if pubOffset < nextExpectedOffset {
		// Epoch is correct, but due to the lag in PUB/SUB processing we received non-actual update
		// here. Safe to just skip for the subscriber.
		c.mu.Unlock()
		return nil
	}
	channelContext.positionCheckTime = time.Now().Unix()
	channelContext.streamPosition.Offset = pub.Offset
	c.channels[ch] = channelContext
	c.mu.Unlock()
	if hasFlag(c.transport.DisabledPushFlags(), PushFlagPublication) {
		return nil
	}

	// If publication should be filtered, skip sending it but keep the offset updated
	if prep.wasFiltered && !prep.deltaSub {
		return nil
	}
	if prep.deltaSub {
		if deltaAllowed {
			if c.node.logEnabled(LogLevelTrace) {
				c.traceOutPush(&protocol.Push{Channel: ch, Pub: pub})
			}
			return c.writeEncodedPushData(prep.brokerDeltaData, ch, pub.Key, protocol.FrameTypePushPublication, batchConfig)
		}
		c.mu.Lock()
		if chCtx, chCtxOK := c.channels[ch]; chCtxOK {
			chCtx.flags |= flagDeltaAllowed
			c.channels[ch] = chCtx
		}
		c.mu.Unlock()
	}

	if c.node.logEnabled(LogLevelTrace) {
		c.traceOutPush(&protocol.Push{Channel: ch, Pub: pub})
	}
	return c.writeEncodedPushData(prep.fullData, ch, pub.Key, protocol.FrameTypePushPublication, batchConfig)
}

func (c *Client) writePublicationNoDelta(ch string, pub *protocol.Publication, data []byte, sp StreamPosition, batchConfig ChannelBatchConfig) error {
	return c.writePublication(
		ch, pub, preparedData{
			fullData: data, brokerDeltaData: nil, localDeltaData: nil, deltaSub: false, wasFiltered: false,
		},
		sp, false, batchConfig)
}

func (c *Client) writePublication(ch string, pub *protocol.Publication, prep preparedData, sp StreamPosition, maxLagExceeded bool, batchConfig ChannelBatchConfig) error {
	if pub.Offset == 0 {
		if hasFlag(c.transport.DisabledPushFlags(), PushFlagPublication) {
			return nil
		}

		// For publications without offset, if filtering is needed, we can skip them
		// early since there's no position tracking to maintain.
		if prep.wasFiltered && !prep.deltaSub {
			return nil
		}

		if prep.deltaSub {
			// For this path (no Offset) delta may come from channel medium layer, so that we can use it
			// here if allowed for the connection.
			c.mu.RLock()
			channelContext, ok := c.channels[ch]
			if !ok {
				c.mu.RUnlock()
				return nil
			}
			deltaAllowed := channelHasFlag(channelContext.flags, flagDeltaAllowed)
			c.mu.RUnlock()

			if deltaAllowed {
				if c.node.logEnabled(LogLevelTrace) {
					c.traceOutPush(&protocol.Push{Channel: ch, Pub: pub})
				}
				return c.writeEncodedPushData(prep.localDeltaData, ch, pub.Key, protocol.FrameTypePushPublication, batchConfig)
			}
			// Set flagDeltaAllowed so subsequent pubs use delta.
			c.mu.Lock()
			if chCtx, chCtxOK := c.channels[ch]; chCtxOK {
				chCtx.flags |= flagDeltaAllowed
				c.channels[ch] = chCtx
			}
			c.mu.Unlock()
		}

		if c.node.logEnabled(LogLevelTrace) {
			c.traceOutPush(&protocol.Push{Channel: ch, Pub: pub})
		}
		return c.writeEncodedPushData(prep.fullData, ch, pub.Key, protocol.FrameTypePushPublication, batchConfig)
	}
	syncPub := pub
	if prep.wasFiltered {
		syncPub = prep.filteredPub
	}
	c.pubSubSync.SyncPublication(ch, syncPub, func() {
		_ = c.writePublicationUpdatePosition(ch, pub, prep, sp, maxLagExceeded, batchConfig)
	})
	return nil
}

func (c *Client) writeJoin(ch string, join *protocol.Join, data []byte, batchConfig ChannelBatchConfig) error {
	if c.node.logEnabled(LogLevelTrace) {
		c.traceOutPush(&protocol.Push{Channel: ch, Join: join})
	}
	if hasFlag(c.transport.DisabledPushFlags(), PushFlagJoin) {
		return nil
	}
	c.mu.RLock()
	channelContext, ok := c.channels[ch]
	if !ok || !channelHasFlag(channelContext.flags, flagSubscribed) {
		c.mu.RUnlock()
		return nil
	}
	if !channelHasFlag(channelContext.flags, flagPushJoinLeave) {
		c.mu.RUnlock()
		return nil
	}
	c.mu.RUnlock()
	return c.writeEncodedPushData(data, ch, "", protocol.FrameTypePushJoin, batchConfig)
}

func (c *Client) writeLeave(ch string, leave *protocol.Leave, data []byte, batchConfig ChannelBatchConfig) error {
	if c.node.logEnabled(LogLevelTrace) {
		c.traceOutPush(&protocol.Push{Channel: ch, Leave: leave})
	}
	if hasFlag(c.transport.DisabledPushFlags(), PushFlagLeave) {
		return nil
	}
	c.mu.RLock()
	channelContext, ok := c.channels[ch]
	if !ok || !channelHasFlag(channelContext.flags, flagSubscribed) {
		c.mu.RUnlock()
		return nil
	}
	if !channelHasFlag(channelContext.flags, flagPushJoinLeave) {
		c.mu.RUnlock()
		return nil
	}
	c.mu.RUnlock()
	return c.writeEncodedPushData(data, ch, "", protocol.FrameTypePushLeave, batchConfig)
}

// Lock must be held outside.
func (c *Client) unsubscribe(channel string, unsubscribe Unsubscribe, disconnect *Disconnect) error {
	// Tell the compression engine first. A dictionary is built over time, so one
	// handed out after this point could carry content published after the
	// connection lost access - including when an admin revoked it.
	if ca, ok := c.transport.(compressionAware); ok {
		if cc := ca.connectionCompression(); cc != nil {
			cc.OnUnsubscribe(channel)
		}
	}
	c.mu.RLock()
	info := c.clientInfo(channel)
	chCtx, ok := c.channels[channel]
	subscribingCh := chCtx.subscribingCh
	// Identity of the subscription this unsubscribe targets. subGen is stable from
	// reservation through finalize, so we can tear down exactly this subscription
	// and never a fresh one that a concurrent subscribe installed in the meantime.
	targetSubGen := chCtx.subGen
	isSubscribed := channelHasFlag(chCtx.flags, flagSubscribed)
	serverSide := channelHasFlag(chCtx.flags, flagServerSide)
	// Also check for in-progress map subscriptions.
	keyedState, hasKeyedState := c.mapSubscribing[channel]
	var mapSubscribingCh chan struct{}
	if hasKeyedState {
		mapSubscribingCh = keyedState.subscribingCh
		if !ok {
			// The subscription is a map sub still loading (in mapSubscribing, not
			// yet in c.channels). It will finalize into c.channels carrying this
			// reservation's generation, so target that: after the wait below the
			// delete matches the live entry, and a fresh map resubscribe with a
			// different generation is left untouched.
			targetSubGen = keyedState.subGen
		}
	}
	c.mu.RUnlock()

	// If channel is not in channels map, check if it's only in mapSubscribing.
	if !ok && !hasKeyedState {
		return nil
	}

	// Wait for normal subscription in progress.
	if ok && !serverSide && !isSubscribed && subscribingCh != nil {
		// If client is not yet subscribed on a client-side channel, and subscribe
		// command is in progress - we need to wait for it to finish before proceeding.
		// We hang no longer than maxWaitTimeout here, if timeout happens - it's a signal
		// of server malfunction since long subscribes should not happen. In this case,
		// we disconnect client to let it re-init the state from scratch.
		maxWaitTimeout := 5 * time.Second
		tm := timers.AcquireTimer(maxWaitTimeout)
		select {
		case <-subscribingCh:
			timers.ReleaseTimer(tm)
			c.mu.RLock()
			chCtx, ok = c.channels[channel]
			c.mu.RUnlock()
			// Keep the original targetSubGen: subGen is stable from reservation
			// through finalize, so our target still identifies the same subscription
			// once it finalizes. If instead a fresh subscribe reserved the channel
			// after ours was removed, its generation differs and we must not adopt
			// it as our target and tear it down.
			if !ok {
				return nil
			}
		case <-tm.C:
			timers.ReleaseTimer(tm)
			c.mu.Lock()
			currentChCtx, ok := c.channels[channel]
			if ok && currentChCtx.subscribingCh != nil {
				close(currentChCtx.subscribingCh)
				currentChCtx.subscribingCh = nil
				c.channels[channel] = currentChCtx
			}
			c.mu.Unlock()
			go func() {
				_ = c.close(DisconnectServerError)
			}()
			c.node.logger.log(newLogEntry(LogLevelInfo, "timeout waiting for subscribe to finish", map[string]any{"channel": channel, "user": c.user, "client": c.uid}))
			return nil
		}
	}

	// Wait for map subscription in progress.
	if hasKeyedState && mapSubscribingCh != nil {
		maxWaitTimeout := 5 * time.Second
		tm := timers.AcquireTimer(maxWaitTimeout)
		select {
		case <-mapSubscribingCh:
			timers.ReleaseTimer(tm)
			c.mu.RLock()
			chCtx, ok = c.channels[channel]
			c.mu.RUnlock()
			if !ok {
				return nil
			}
		case <-tm.C:
			timers.ReleaseTimer(tm)
			// Identity-match: only close/delete if the entry still corresponds to
			// the *mapSubscribeState we were waiting on. A fresh resubscribe may
			// have replaced it between the RUnlock and this Lock — clobbering
			// that fresh entry would leak its subscribingCh waiters.
			c.mu.Lock()
			if state, exists := c.mapSubscribing[channel]; exists && state == keyedState {
				if state.subscribingCh != nil {
					close(state.subscribingCh)
				}
				delete(c.mapSubscribing, channel)
			}
			c.mu.Unlock()
			go func() {
				_ = c.close(DisconnectServerError)
			}()
			c.node.logger.log(newLogEntry(LogLevelInfo, "timeout waiting for keyed subscribe to finish", map[string]any{"channel": channel, "user": c.user, "client": c.uid}))
			return nil
		}
	}

	c.mu.Lock()
	// Clean up normal subscription. Identity-match on subGen (mirrors the
	// mapSubscribing cleanup below): only tear down the entry if it is still the
	// subscription this unsubscribe targeted. A concurrent subscribe may have
	// removed our target and installed a fresh reservation with a newer generation
	// between the wait above and this lock — deleting that would strand it in
	// c.channels with no hub routing (and, for a gen-0 reservation, unconditionally
	// remove its hub entry).
	removedNow := false
	var removedSubGen uint64
	if currentChCtx, exists := c.channels[channel]; exists && currentChCtx.subGen == targetSubGen {
		if currentChCtx.subscribingCh != nil {
			close(currentChCtx.subscribingCh)
		}
		// Hub removal only affects this generation — a concurrent resubscribe has a
		// newer generation and must not be torn down by this unsubscribe.
		removedSubGen = currentChCtx.subGen
		delete(c.channels, channel)
		removedNow = true
	}
	// Clean up map subscribing state. Identity-match against the snapshot so we
	// don't close a fresh entry installed by a concurrent resubscribe.
	if hasKeyedState {
		if state, exists := c.mapSubscribing[channel]; exists && state == keyedState {
			if state.subscribingCh != nil {
				close(state.subscribingCh)
			}
			delete(c.mapSubscribing, channel)
			removedNow = true
		}
	}
	if removedNow && disconnect == nil && c.perChannelWriter != nil {
		c.perChannelWriter.delWriter(channel, false)
	}
	c.mu.Unlock()

	// Multiple goroutines can reach this point for the same channel — e.g. the
	// presence ticker spawns a fresh handleAsyncUnsubscribe goroutine on every
	// tick until the channel is actually removed, and ticks can overlap if the
	// first goroutine is still blocked on a network call or the write lock.
	// Each goroutine captured its own chCtx snapshot under RLock and would
	// otherwise re-run presence/leave/removeSubscription/unsubscribeHandler
	// (which double-fires user callbacks and panics on patterns like a single
	// `close(doneCh)` in OnUnsubscribe). Only the goroutine that won the delete
	// race owns the cleanup; the rest exit here.
	if !removedNow {
		return nil
	}

	// Remove presence and/or run map cleanup on unsubscribe.
	hasMapPresenceOrCleanup := channelHasFlag(chCtx.flags, flagMapClientPresence) ||
		channelHasFlag(chCtx.flags, flagMapUserPresence) ||
		channelHasFlag(chCtx.flags, flagCleanupOnUnsubscribe)

	if channelHasFlag(chCtx.flags, flagSubscribed) {
		if hasMapPresenceOrCleanup {
			err := c.removeMapPresence(channel, chCtx)
			if err != nil {
				c.node.logger.log(newErrorLogEntry(err, "error removing channel presence", map[string]any{"channel": channel, "user": c.user, "client": c.uid, "error": err.Error()}))
			}
		} else if channelHasFlag(chCtx.flags, flagEmitPresence) {
			err := c.node.removePresence(channel, c.uid, c.user)
			if err != nil {
				c.node.logger.log(newErrorLogEntry(err, "error removing channel presence", map[string]any{"channel": channel, "user": c.user, "client": c.uid, "error": err.Error()}))
			}
		}
	}

	// Clean up keyed subscription state (shared poll).
	if channelHasFlag(chCtx.flags, flagKeyed) {
		c.cleanupKeyed(channel)
	}

	if channelHasFlag(chCtx.flags, flagEmitJoinLeave) && channelHasFlag(chCtx.flags, flagSubscribed) {
		_ = c.node.publishLeave(channel, info)
	}

	if err := c.node.removeSubscription(channel, c, removedSubGen); err != nil {
		c.node.logger.log(newErrorLogEntry(err, "error removing subscription", map[string]any{"channel": channel, "user": c.user, "client": c.uid, "error": err.Error()}))
		return err
	}

	if channelHasFlag(chCtx.flags, flagSubscribed) {
		if c.eventHub.unsubscribeHandler != nil {
			c.eventHub.unsubscribeHandler(UnsubscribeEvent{
				Channel: channel,
				// Recomputed from the post-wait context: the pre-wait `serverSide`
				// snapshot may come from a reservation with flags==0 that finalized
				// into a server-side subscription while the wait gate was blocked —
				// the event must describe the subscription actually torn down.
				ServerSide:  channelHasFlag(chCtx.flags, flagServerSide),
				Unsubscribe: unsubscribe,
				Disconnect:  disconnect,
			})
		}
	}

	if c.node.logger.enabled(LogLevelDebug) {
		c.node.logger.log(newLogEntry(LogLevelDebug, "client unsubscribed from channel", map[string]any{"channel": channel, "user": c.user, "client": c.uid}))
	}

	return nil
}

func (c *Client) logDisconnectBadRequest(message string) error {
	c.node.logger.log(newLogEntry(LogLevelInfo, message, map[string]any{"user": c.user, "client": c.uid}))
	return DisconnectBadRequest
}

func (c *Client) logWriteInternalErrorFlush(ch string, frameType protocol.FrameType, cmd *protocol.Command, err error, message string, started time.Time, rw *replyWriter) {
	defer func() {
		c.node.metrics.observeCommandDuration(frameType, time.Since(started), ch, c)
	}()
	var clientErr *Error
	if errors.As(err, &clientErr) {
		errorReply := &protocol.Reply{Error: clientErr.toProto()}
		c.writeError(ch, frameType, cmd, errorReply, rw)
		return
	}
	c.node.logger.log(newErrorLogEntry(err, message, map[string]any{"error": err.Error()}))

	errorReply := &protocol.Reply{Error: ErrorInternal.toProto()}
	c.writeError(ch, frameType, cmd, errorReply, rw)
	if c.node.clientEvents.commandProcessedHandler != nil {
		event := newCommandProcessedEvent(cmd, nil, errorReply, started)
		c.issueCommandProcessedEvent(event)
	}
}

func toClientErr(err error) *Error {
	var clientErr *Error
	if errors.As(err, &clientErr) {
		return clientErr
	}
	return ErrorInternal
}

func disconnectFromError(err error) (*Disconnect, bool) {
	var disconnect *Disconnect
	if errors.As(err, &disconnect) {
		return disconnect, true
	}
	var disconnectValue Disconnect
	if errors.As(err, &disconnectValue) {
		return &disconnectValue, true
	}
	return nil, false
}
