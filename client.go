package centrifuge

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/centrifugal/centrifuge/internal/prepared"
	"github.com/centrifugal/centrifuge/internal/queue"
	"github.com/centrifugal/centrifuge/internal/recovery"

	"github.com/centrifugal/protocol"
	"github.com/google/uuid"
)

// clientEventHub allows to deal with client event handlers.
// All its methods are not goroutine-safe and supposed to be called
// once inside Node ConnectHandler.
type clientEventHub struct {
	aliveHandler         AliveHandler
	disconnectHandler    DisconnectHandler
	subscribeHandler     SubscribeHandler
	unsubscribeHandler   UnsubscribeHandler
	publishHandler       PublishHandler
	refreshHandler       RefreshHandler
	subRefreshHandler    SubRefreshHandler
	rpcHandler           RPCHandler
	messageHandler       MessageHandler
	presenceHandler      PresenceHandler
	presenceStatsHandler PresenceStatsHandler
	historyHandler       HistoryHandler
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

// We poll current position in channel from history storage periodically.
// If client position is wrong maxCheckPositionFailures times in a row
// then client will be disconnected with InsufficientState reason. Polling
// not used in channels with high frequency updates since we can check position
// comparing client offset with offset in incoming Publication.
const maxCheckPositionFailures uint8 = 2

// Note: up to 8 possible flags here.
const (
	// flagSubscribed will be set upon successful Subscription to a channel.
	// Until that moment channel exists in client Channels map only to track
	// duplicate subscription requests.
	flagSubscribed uint8 = 1 << iota
	flagPresence
	flagJoinLeave
	flagPosition
	flagRecover
	flagServerSide
	flagClientSideRefresh
)

// channelContext contains extra context for channel connection subscribed to.
// Note: this struct is aligned to consume less memory.
type channelContext struct {
	Info                  []byte
	expireAt              int64
	positionCheckTime     int64
	streamPosition        StreamPosition
	positionCheckFailures uint8
	flags                 uint8
}

func channelHasFlag(flags, flag uint8) bool {
	return flags&flag != 0
}

type timerOp uint8

const (
	timerOpStale    timerOp = 1
	timerOpPresence timerOp = 2
	timerOpExpire   timerOp = 3
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
	mu                sync.RWMutex
	connectMu         sync.Mutex // allows to sync connect with disconnect.
	presenceMu        sync.Mutex // allows to sync presence routine with client closing.
	ctx               context.Context
	transport         Transport
	node              *Node
	exp               int64
	channels          map[string]channelContext
	messageWriter     *writer
	pubSubSync        *recovery.PubSubSync
	uid               string
	user              string
	info              []byte
	authenticated     bool
	clientSideRefresh bool
	status            status
	timerOp           timerOp
	nextPresence      int64
	nextExpire        int64
	eventHub          *clientEventHub
	timer             *time.Timer
}

// ClientCloseFunc must be called on Transport handler close to clean up Client.
type ClientCloseFunc func() error

// NewClient initializes new Client.
func NewClient(ctx context.Context, n *Node, t Transport) (*Client, ClientCloseFunc, error) {
	uuidObject, err := uuid.NewRandom()
	if err != nil {
		return nil, nil, err
	}

	client := &Client{
		ctx:        ctx,
		uid:        uuidObject.String(),
		node:       n,
		transport:  t,
		channels:   make(map[string]channelContext),
		pubSubSync: recovery.NewPubSubSync(),
		status:     statusConnecting,
		eventHub:   &clientEventHub{},
	}

	messageWriterConf := writerConfig{
		MaxQueueSize: n.config.ClientQueueMaxSize,
		WriteFn: func(item queue.Item) error {
			if client.node.transportWriteHandler != nil {
				pass := client.node.transportWriteHandler(client, TransportWriteEvent(item))
				if !pass {
					return nil
				}
			}
			if err := t.Write(item.Data); err != nil {
				switch v := err.(type) {
				case *Disconnect:
					go func() { _ = client.close(v) }()
				default:
					go func() { _ = client.close(DisconnectWriteError) }()
				}
				return err
			}
			incTransportMessagesSent(t.Name())
			return nil
		},
		WriteManyFn: func(items ...queue.Item) error {
			messages := make([][]byte, 0, len(items))
			for i := 0; i < len(items); i++ {
				if client.node.transportWriteHandler != nil {
					pass := client.node.transportWriteHandler(client, TransportWriteEvent(items[i]))
					if !pass {
						continue
					}
				}
				messages = append(messages, items[i].Data)
			}
			if err := t.WriteMany(messages...); err != nil {
				switch v := err.(type) {
				case *Disconnect:
					go func() { _ = client.close(v) }()
				default:
					go func() { _ = client.close(DisconnectWriteError) }()
				}
				return err
			}
			addTransportMessagesSent(t.Name(), float64(len(items)))
			return nil
		},
	}

	client.messageWriter = newWriter(messageWriterConf)
	go client.messageWriter.run()

	staleCloseDelay := n.config.ClientStaleCloseDelay
	if staleCloseDelay > 0 && !client.authenticated {
		client.mu.Lock()
		client.timerOp = timerOpStale
		client.timer = time.AfterFunc(staleCloseDelay, client.onTimerOp)
		client.mu.Unlock()
	}
	return client, func() error { return client.close(nil) }, nil
}

func extractUnidirectionalDisconnect(err error) *Disconnect {
	if err == nil {
		return nil
	}
	var d *Disconnect
	switch t := err.(type) {
	case *Disconnect:
		d = t
	case *Error:
		switch t.Code {
		case ErrorExpired.Code:
			d = DisconnectExpired
		case ErrorTokenExpired.Code:
			d = DisconnectExpired
		default:
			d = DisconnectServerError
		}
	default:
		d = DisconnectServerError
	}
	return d
}

// Connect supposed to be called from unidirectional transport layer to pass
// initial information about connection and thus initiate Node.OnConnecting
// event. Bidirectional transport initiate connecting workflow automatically
// since client passes Connect command upon successful connection establishment
// with a server.
func (c *Client) Connect(req ConnectRequest) {
	err := c.unidirectionalConnect(req.toProto())
	if err != nil {
		d := extractUnidirectionalDisconnect(err)
		go func() { _ = c.close(d) }()
	}
}

func (c *Client) encodeDisconnect(d *Disconnect) (*prepared.Reply, error) {
	disconnect := &protocol.Disconnect{
		Code:      d.Code,
		Reason:    d.Reason,
		Reconnect: d.Reconnect,
	}
	pushBytes, err := protocol.EncodeDisconnectPush(c.transport.Protocol().toProto(), disconnect)
	if err != nil {
		return nil, err
	}
	return prepared.NewReply(&protocol.Reply{
		Result: pushBytes,
	}, c.transport.Protocol().toProto()), nil
}

func (c *Client) encodeConnectPush(res *protocol.ConnectResult) ([]byte, error) {
	p := &protocol.Connect{
		Version: res.GetVersion(),
		Client:  res.GetClient(),
		Data:    res.Data,
		Subs:    res.Subs,
		Expires: res.Expires,
		Ttl:     res.Ttl,
	}
	return protocol.EncodeConnectPush(c.transport.Protocol().toProto(), p)
}

func hasFlag(flags, flag uint64) bool {
	return flags&flag != 0
}

func (c *Client) unidirectionalConnect(connectRequest *protocol.ConnectRequest) error {
	write := func(rep *protocol.Reply) error {
		if hasFlag(c.transport.DisabledPushFlags(), PushFlagConnect) {
			return nil
		}
		c.trace("-->", rep.Result)
		disconnect := c.messageWriter.enqueue(queue.Item{Data: rep.Result, IsPush: false})
		if disconnect != nil {
			if c.node.logger.enabled(LogLevelDebug) {
				c.node.logger.log(newLogEntry(LogLevelDebug, "disconnect after connect push", map[string]interface{}{"client": c.ID(), "user": c.UserID(), "reason": disconnect.Reason}))
			}
			go func() { _ = c.close(disconnect) }()
		}
		return disconnect
	}

	rw := &replyWriter{write, func() {}}

	_, err := c.connectCmd(connectRequest, rw)
	if err != nil {
		return err
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
	timerOp := c.timerOp
	c.mu.Unlock()
	switch timerOp {
	case timerOpStale:
		c.closeUnauthenticated()
	case timerOpPresence:
		c.updatePresence()
	case timerOpExpire:
		c.expire()
	}
}

// Lock must be held outside.
func (c *Client) scheduleNextTimer() {
	if c.status == statusClosed {
		return
	}
	c.stopTimer()
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
	if needTimer {
		c.timerOp = nextTimerOp
		afterDuration := time.Duration(minEventTime-time.Now().UnixNano()) * time.Nanosecond
		c.timer = time.AfterFunc(afterDuration, c.onTimerOp)
	}
}

// Lock must be held outside.
func (c *Client) stopTimer() {
	if c.timer != nil {
		c.timer.Stop()
	}
}

// Lock must be held outside.
func (c *Client) addPresenceUpdate() {
	config := c.node.config
	presenceInterval := config.ClientPresenceUpdateInterval
	c.nextPresence = time.Now().Add(presenceInterval).UnixNano()
	c.scheduleNextTimer()
}

// Lock must be held outside.
func (c *Client) addExpireUpdate(after time.Duration) {
	c.nextExpire = time.Now().Add(after).UnixNano()
	c.scheduleNextTimer()
}

// closeUnauthenticated closes connection if it's not authenticated yet.
// At moment used to close client connections which have not sent valid
// connect command in a reasonable time interval after established connection
// with server.
func (c *Client) closeUnauthenticated() {
	c.mu.RLock()
	authenticated := c.authenticated
	closed := c.status == statusClosed
	c.mu.RUnlock()
	if !authenticated && !closed {
		_ = c.close(DisconnectStale)
	}
}

func (c *Client) transportEnqueue(reply *prepared.Reply) error {
	var data []byte
	if c.transport.Unidirectional() {
		data = reply.Reply.Result
	} else {
		data = reply.Data()
	}
	c.trace("-->", data)
	disconnect := c.messageWriter.enqueue(queue.Item{
		Data:   data,
		IsPush: reply.Reply.Id == 0,
	})
	if disconnect != nil {
		// close in goroutine to not block message broadcast.
		go func() { _ = c.close(disconnect) }()
		return io.EOF
	}
	return nil
}

// updateChannelPresence updates client presence info for channel so it
// won't expire until client disconnect.
func (c *Client) updateChannelPresence(ch string, chCtx channelContext) error {
	if !channelHasFlag(chCtx.flags, flagPresence) {
		return nil
	}
	return c.node.addPresence(ch, c.uid, &ClientInfo{
		ClientID: c.uid,
		UserID:   c.user,
		ConnInfo: c.info,
		ChanInfo: chCtx.Info,
	})
}

// Context returns client Context. This context will be canceled
// as soon as client connection closes.
func (c *Client) Context() context.Context {
	return c.ctx
}

func (c *Client) checkSubscriptionExpiration(channel string, channelContext channelContext, delay time.Duration, resultCB func(bool)) {
	now := c.node.nowTimeGetter().Unix()
	expireAt := channelContext.expireAt
	clientSideRefresh := channelHasFlag(channelContext.flags, flagClientSideRefresh)
	if expireAt > 0 && now > expireAt+int64(delay.Seconds()) {
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
			if reply.Expired || (reply.ExpireAt > 0 && reply.ExpireAt < now) {
				resultCB(false)
				return
			}
			c.mu.Lock()
			if ctx, ok := c.channels[channel]; ok {
				if len(reply.Info) > 0 {
					ctx.Info = reply.Info
				}
				ctx.expireAt = reply.ExpireAt
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

// updatePresence used for various periodic actions we need to do with client connections.
func (c *Client) updatePresence() {
	c.presenceMu.Lock()
	defer c.presenceMu.Unlock()
	config := c.node.config
	c.mu.Lock()
	if c.status == statusClosed {
		c.mu.Unlock()
		return
	}
	channels := make(map[string]channelContext, len(c.channels))
	for channel, channelContext := range c.channels {
		if !channelHasFlag(channelContext.flags, flagSubscribed) {
			continue
		}
		channels[channel] = channelContext
	}
	c.mu.Unlock()
	if c.eventHub.aliveHandler != nil {
		c.eventHub.aliveHandler()
	}
	for channel, channelContext := range channels {
		c.checkSubscriptionExpiration(channel, channelContext, config.ClientExpiredSubCloseDelay, func(result bool) {
			// Ideally we should deal with single expired subscription in this
			// case - i.e. unsubscribe client from channel and give an advice
			// to resubscribe. But there is scenario when browser goes online
			// after computer was in sleeping mode which I have not managed to
			// handle reliably on client side when unsubscribe with resubscribe
			// flag was used. So I decided to stick with disconnect for now -
			// it seems to work fine and drastically simplifies client code.
			if !result {
				go func() { _ = c.close(DisconnectSubExpired) }()
			}
		})

		checkDelay := config.ClientChannelPositionCheckDelay
		if checkDelay > 0 && !c.checkPosition(checkDelay, channel, channelContext) {
			go func() { _ = c.close(DisconnectInsufficientState) }()
			// No need to proceed after close.
			return
		}

		err := c.updateChannelPresence(channel, channelContext)
		if err != nil {
			c.node.logger.log(newLogEntry(LogLevelError, "error updating presence for channel", map[string]interface{}{"channel": channel, "user": c.user, "client": c.uid, "error": err.Error()}))
		}
	}
	c.mu.Lock()
	c.addPresenceUpdate()
	c.mu.Unlock()
}

func (c *Client) checkPosition(checkDelay time.Duration, ch string, chCtx channelContext) bool {
	if !channelHasFlag(chCtx.flags, flagRecover|flagPosition) {
		return true
	}
	nowUnix := c.node.nowTimeGetter().Unix()

	isInitialCheck := chCtx.positionCheckTime == 0
	isTimeToCheck := nowUnix-chCtx.positionCheckTime > int64(checkDelay.Seconds())
	needCheckPosition := isInitialCheck || isTimeToCheck

	if !needCheckPosition {
		return true
	}
	position := chCtx.streamPosition
	streamTop, err := c.node.streamTop(ch)
	if err != nil {
		return true
	}

	isValidPosition := streamTop.Offset == position.Offset && streamTop.Epoch == position.Epoch
	keepConnection := true
	c.mu.Lock()
	if chContext, ok := c.channels[ch]; ok {
		chContext.positionCheckTime = nowUnix
		if !isValidPosition {
			chContext.positionCheckFailures++
			keepConnection = chContext.positionCheckFailures < maxCheckPositionFailures
		} else {
			chContext.positionCheckFailures = 0
		}
		c.channels[ch] = chContext
	}
	c.mu.Unlock()
	return keepConnection
}

// ID returns unique client connection id.
func (c *Client) ID() string {
	return c.uid
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

// IsSubscribed returns true if client subscribed to a channel.
func (c *Client) IsSubscribed(ch string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	ctx, ok := c.channels[ch]
	return ok && channelHasFlag(ctx.flags, flagSubscribed)
}

// Send data to client. This sends an asynchronous message – data will be
// just written to connection. on client side this message can be handled
// with Message handler.
func (c *Client) Send(data []byte) error {
	if hasFlag(c.transport.DisabledPushFlags(), PushFlagMessage) {
		return nil
	}
	p := &protocol.Message{
		Data: data,
	}
	pushBytes, err := protocol.EncodeMessagePush(c.transport.Protocol().toProto(), p)
	if err != nil {
		return err
	}
	reply := prepared.NewReply(&protocol.Reply{
		Result: pushBytes,
	}, c.transport.Protocol().toProto())
	return c.transportEnqueue(reply)
}

// Unsubscribe allows to unsubscribe client from channel.
func (c *Client) Unsubscribe(ch string) error {
	c.mu.RLock()
	if c.status == statusClosed {
		c.mu.RUnlock()
		return nil
	}
	c.mu.RUnlock()

	err := c.unsubscribe(ch)
	if err != nil {
		return err
	}
	return c.sendUnsubscribe(ch)
}

func (c *Client) sendUnsubscribe(ch string) error {
	if hasFlag(c.transport.DisabledPushFlags(), PushFlagUnsubscribe) {
		return nil
	}
	pushBytes, err := protocol.EncodeUnsubscribePush(c.transport.Protocol().toProto(), ch, &protocol.Unsubscribe{})
	if err != nil {
		return err
	}
	reply := prepared.NewReply(&protocol.Reply{
		Result: pushBytes,
	}, c.transport.Protocol().toProto())

	_ = c.transportEnqueue(reply)
	return nil
}

// Disconnect client connection with specific disconnect code and reason.
// This method internally creates a new goroutine at moment to do
// closing stuff. An extra goroutine is required to solve disconnect
// and alive callback ordering/sync problems. Will be a noop if client
// already closed. As this method runs a separate goroutine client
// connection will be closed eventually (i.e. not immediately).
func (c *Client) Disconnect(disconnect *Disconnect) {
	go func() {
		_ = c.close(disconnect)
	}()
}

func (c *Client) close(disconnect *Disconnect) error {
	c.presenceMu.Lock()
	defer c.presenceMu.Unlock()
	c.connectMu.Lock()
	defer c.connectMu.Unlock()
	c.mu.Lock()
	if c.status == statusClosed {
		c.mu.Unlock()
		return nil
	}
	prevStatus := c.status
	c.status = statusClosed

	c.stopTimer()

	channels := make(map[string]channelContext, len(c.channels))
	for channel, channelContext := range c.channels {
		channels[channel] = channelContext
	}
	c.mu.Unlock()

	if len(channels) > 0 {
		// Unsubscribe from all channels.
		for channel := range channels {
			err := c.unsubscribe(channel)
			if err != nil {
				c.node.logger.log(newLogEntry(LogLevelError, "error unsubscribing client from channel", map[string]interface{}{"channel": channel, "user": c.user, "client": c.uid, "error": err.Error()}))
			}
		}
	}

	c.mu.RLock()
	authenticated := c.authenticated
	c.mu.RUnlock()

	if authenticated {
		err := c.node.removeClient(c)
		if err != nil {
			c.node.logger.log(newLogEntry(LogLevelError, "error removing client", map[string]interface{}{"user": c.user, "client": c.uid, "error": err.Error()}))
		}
	}

	if disconnect != nil && !hasFlag(c.transport.DisabledPushFlags(), PushFlagDisconnect) {
		if reply, err := c.encodeDisconnect(disconnect); err == nil {
			_ = c.transportEnqueue(reply)
		}
	}

	// close writer and send messages remaining in writer queue if any.
	_ = c.messageWriter.close()

	_ = c.transport.Close(disconnect)

	if disconnect != nil && disconnect.Reason != "" {
		c.node.logger.log(newLogEntry(LogLevelDebug, "closing client connection", map[string]interface{}{"client": c.uid, "user": c.user, "reason": disconnect.Reason, "reconnect": disconnect.Reconnect}))
	}
	if disconnect != nil {
		incServerDisconnect(disconnect.Code)
	}
	if c.eventHub.disconnectHandler != nil && prevStatus == statusConnected {
		c.eventHub.disconnectHandler(DisconnectEvent{
			Disconnect: disconnect,
		})
	}
	return nil
}

func (c *Client) trace(msg string, data []byte) {
	if !c.node.LogEnabled(LogLevelTrace) {
		return
	}
	c.mu.RLock()
	user := c.user
	c.mu.RUnlock()
	c.node.logger.log(newLogEntry(LogLevelTrace, msg, map[string]interface{}{"client": c.ID(), "user": user, "data": fmt.Sprintf("%#v", string(data))}))
}

// Lock must be held outside.
func (c *Client) clientInfo(ch string) *ClientInfo {
	var channelInfo protocol.Raw
	channelContext, ok := c.channels[ch]
	if ok && channelHasFlag(channelContext.flags, flagSubscribed) {
		channelInfo = channelContext.Info
	}
	return &ClientInfo{
		ClientID: c.uid,
		UserID:   c.user,
		ConnInfo: c.info,
		ChanInfo: channelInfo,
	}
}

// Handle raw data encoded with Centrifuge protocol.
// Not goroutine-safe. Supposed to be called only from a transport connection reader.
func (c *Client) Handle(data []byte) bool {
	c.mu.Lock()
	if c.status == statusClosed {
		c.mu.Unlock()
		return false
	}
	c.mu.Unlock()

	if c.transport.Unidirectional() {
		c.node.logger.log(newLogEntry(LogLevelInfo, "can't handle data for unidirectional client", map[string]interface{}{"client": c.ID(), "user": c.UserID()}))
		go func() { _ = c.close(DisconnectBadRequest) }()
		return false
	}

	if len(data) == 0 {
		c.node.logger.log(newLogEntry(LogLevelInfo, "empty client request received", map[string]interface{}{"client": c.ID(), "user": c.UserID()}))
		go func() { _ = c.close(DisconnectBadRequest) }()
		return false
	}

	c.trace("<--", data)

	protoType := c.transport.Protocol().toProto()
	decoder := protocol.GetCommandDecoder(protoType, data)
	defer protocol.PutCommandDecoder(protoType, decoder)

	for {
		cmd, err := decoder.Decode()
		if err != nil && err != io.EOF {
			c.node.logger.log(newLogEntry(LogLevelInfo, "error decoding command", map[string]interface{}{"data": string(data), "client": c.ID(), "user": c.UserID(), "error": err.Error()}))
			go func() { _ = c.close(DisconnectBadRequest) }()
			return false
		}
		if cmd != nil {
			ok := c.handleCommand(cmd)
			if !ok {
				return false
			}
		}
		if err == io.EOF {
			break
		}
	}
	return true
}

// handleCommand processes a single protocol.Command.
func (c *Client) handleCommand(cmd *protocol.Command) bool {
	if cmd.Method != protocol.Command_CONNECT && !c.authenticated {
		// Client must send connect command to authenticate itself first.
		c.node.logger.log(newLogEntry(LogLevelInfo, "client not authenticated to handle command", map[string]interface{}{"client": c.ID(), "user": c.UserID(), "command": fmt.Sprintf("%v", cmd)}))
		go func() { _ = c.close(DisconnectBadRequest) }()
		return false
	}

	if cmd.Id == 0 && cmd.Method != protocol.Command_SEND {
		// Only send command from client can be sent without incremental ID.
		c.node.logger.log(newLogEntry(LogLevelInfo, "command ID required for commands with reply expected", map[string]interface{}{"client": c.ID(), "user": c.UserID()}))
		go func() { _ = c.close(DisconnectBadRequest) }()
		return false
	}

	select {
	case <-c.ctx.Done():
		return false
	default:
	}

	disconnect := c.dispatchCommand(cmd)

	select {
	case <-c.ctx.Done():
		return false
	default:
	}
	if disconnect != nil {
		if disconnect != DisconnectNormal {
			c.node.logger.log(newLogEntry(LogLevelInfo, "disconnect after handling command", map[string]interface{}{"command": fmt.Sprintf("%v", cmd), "client": c.ID(), "user": c.UserID(), "reason": disconnect.Reason}))
		}
		go func() { _ = c.close(disconnect) }()
		return false
	}
	return true
}

type replyWriter struct {
	write func(*protocol.Reply) error
	done  func()
}

// dispatchCommand dispatches Command into correct command handler.
func (c *Client) dispatchCommand(cmd *protocol.Command) *Disconnect {
	c.mu.Lock()
	if c.status == statusClosed {
		c.mu.Unlock()
		return nil
	}
	c.mu.Unlock()

	method := cmd.Method
	params := cmd.Params

	protoType := c.transport.Protocol().toProto()
	replyEncoder := protocol.GetReplyEncoder(protoType)

	var encodeErr error

	started := time.Now()

	write := func(rep *protocol.Reply) error {
		rep.Id = cmd.Id
		if rep.Error != nil {
			if c.node.LogEnabled(LogLevelInfo) {
				c.node.logger.log(newLogEntry(LogLevelInfo, "client command error", map[string]interface{}{"reply": fmt.Sprintf("%v", rep), "command": fmt.Sprintf("%v", cmd), "client": c.ID(), "user": c.UserID(), "error": rep.Error.Message, "code": rep.Error.Code}))
			}
			incReplyError(cmd.Method, rep.Error.Code)
		}

		var replyData []byte
		replyData, encodeErr = replyEncoder.Encode(rep)
		if encodeErr != nil {
			c.node.logger.log(newLogEntry(LogLevelError, "error encoding reply", map[string]interface{}{"reply": fmt.Sprintf("%v", rep), "client": c.ID(), "user": c.UserID(), "error": encodeErr.Error()}))
			return encodeErr
		}
		c.trace("-->", replyData)
		disconnect := c.messageWriter.enqueue(queue.Item{Data: replyData, IsPush: false})
		if disconnect != nil {
			if c.node.logger.enabled(LogLevelDebug) {
				c.node.logger.log(newLogEntry(LogLevelDebug, "disconnect after sending reply", map[string]interface{}{"client": c.ID(), "user": c.UserID(), "reason": disconnect.Reason}))
			}
			go func() { _ = c.close(disconnect) }()
		}
		return disconnect
	}

	// done should be called after command fully processed.
	done := func() {
		observeCommandDuration(method, time.Since(started))
	}

	// The rule is as follows: if command handler returns an
	// error then we handle it here: write error into connection
	// or return disconnect further to caller and call rw.done()
	// in the end.
	// If handler returned nil error then we assume that all
	// rw operations will be executed inside handler itself.
	rw := &replyWriter{write, done}

	var handleErr error

	switch method {
	case protocol.Command_CONNECT:
		handleErr = c.handleConnect(params, rw)
	case protocol.Command_PING:
		handleErr = c.handlePing(params, rw)
	case protocol.Command_SUBSCRIBE:
		handleErr = c.handleSubscribe(params, rw)
	case protocol.Command_UNSUBSCRIBE:
		handleErr = c.handleUnsubscribe(params, rw)
	case protocol.Command_PUBLISH:
		handleErr = c.handlePublish(params, rw)
	case protocol.Command_PRESENCE:
		handleErr = c.handlePresence(params, rw)
	case protocol.Command_PRESENCE_STATS:
		handleErr = c.handlePresenceStats(params, rw)
	case protocol.Command_HISTORY:
		handleErr = c.handleHistory(params, rw)
	case protocol.Command_RPC:
		handleErr = c.handleRPC(params, rw)
	case protocol.Command_SEND:
		handleErr = c.handleSend(params, rw)
	case protocol.Command_REFRESH:
		handleErr = c.handleRefresh(params, rw)
	case protocol.Command_SUB_REFRESH:
		handleErr = c.handleSubRefresh(params, rw)
	default:
		handleErr = ErrorMethodNotFound
	}
	if encodeErr != nil {
		return DisconnectServerError
	}
	if handleErr != nil {
		defer rw.done()
		switch t := handleErr.(type) {
		case *Disconnect:
			return t
		default:
			c.writeError(rw, toClientErr(handleErr))
		}
	}
	return nil
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
				c.addExpireUpdate(time.Duration(ttl) * time.Second)
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
				switch t := err.(type) {
				case *Disconnect:
					_ = c.close(t)
					return
				default:
					_ = c.close(DisconnectServerError)
					return
				}
			}
			if reply.Expired {
				_ = c.close(DisconnectExpired)
				return
			}
			if reply.ExpireAt > 0 {
				c.mu.Lock()
				c.exp = reply.ExpireAt
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

func (c *Client) handleConnect(params protocol.Raw, rw *replyWriter) error {
	cmd, err := protocol.GetParamsDecoder(c.transport.Protocol().toProto()).DecodeConnect(params)
	if err != nil {
		return c.logDisconnectBadRequestWithError(err, "error decoding connect")
	}
	_, disconnect := c.connectCmd(cmd, rw)
	if disconnect != nil {
		return disconnect
	}
	c.triggerConnect()
	c.scheduleOnConnectTimers()
	return nil
}

func (c *Client) triggerConnect() {
	c.connectMu.Lock()
	defer c.connectMu.Unlock()
	if c.status != statusConnecting {
		return
	}
	if c.node.clientEvents.connectHandler == nil {
		c.status = statusConnected
		return
	}
	c.node.clientEvents.connectHandler(c)
	c.status = statusConnected
}

func (c *Client) scheduleOnConnectTimers() {
	// Make presence and refresh handlers always run after client connect event.
	c.mu.Lock()
	c.addPresenceUpdate()
	if c.exp > 0 {
		expireAfter := time.Duration(c.exp-time.Now().Unix()) * time.Second
		if c.clientSideRefresh {
			conf := c.node.config
			expireAfter += conf.ClientExpiredCloseDelay
		}
		c.addExpireUpdate(expireAfter)
	}
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

	expireAt := refreshOptions.ExpireAt
	info := refreshOptions.Info

	res := &protocol.Refresh{
		Expires: expireAt > 0,
	}

	ttl := expireAt - time.Now().Unix()

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
			c.addExpireUpdate(duration)
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

	pushBytes, err := protocol.EncodeRefreshPush(c.transport.Protocol().toProto(), res)
	if err != nil {
		return err
	}
	reply := prepared.NewReply(&protocol.Reply{
		Result: pushBytes,
	}, c.transport.Protocol().toProto())

	return c.transportEnqueue(reply)
}

func (c *Client) handleRefresh(params protocol.Raw, rw *replyWriter) error {
	if c.eventHub.refreshHandler == nil {
		return ErrorNotAvailable
	}

	cmd, err := protocol.GetParamsDecoder(c.transport.Protocol().toProto()).DecodeRefresh(params)
	if err != nil {
		return c.logDisconnectBadRequestWithError(err, "error decoding refresh")
	}

	if cmd.Token == "" {
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
		Token:             cmd.Token,
	}

	cb := func(reply RefreshReply, err error) {
		defer rw.done()

		if err != nil {
			c.writeDisconnectOrErrorFlush(rw, err)
			return
		}

		if reply.Expired {
			c.Disconnect(DisconnectExpired)
			return
		}

		expireAt := reply.ExpireAt
		info := reply.Info

		res := &protocol.RefreshResult{
			Version: c.node.config.Version,
			Expires: expireAt > 0,
			Client:  c.uid,
		}

		ttl := expireAt - time.Now().Unix()

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
				c.addExpireUpdate(duration)
				c.mu.Unlock()
			} else {
				c.writeError(rw, ErrorExpired)
				return
			}
		}

		replyRes, err := protocol.GetResultEncoder(c.transport.Protocol().toProto()).EncodeRefreshResult(res)
		if err != nil {
			c.logWriteInternalErrorFlush(rw, err, "error encoding refresh")
			return
		}

		_ = writeReply(rw, &protocol.Reply{Result: replyRes})
	}

	c.eventHub.refreshHandler(event, cb)
	return nil
}

// onSubscribeError cleans up a channel from client channels if an error during subscribe happened.
// Channel kept in a map during subscribe request to check for duplicate subscription attempts.
func (c *Client) onSubscribeError(channel string) {
	c.mu.Lock()
	_, ok := c.channels[channel]
	delete(c.channels, channel)
	c.mu.Unlock()
	if ok {
		_ = c.node.removeSubscription(channel, c)
	}
}

func (c *Client) handleSubscribe(params protocol.Raw, rw *replyWriter) error {
	if c.eventHub.subscribeHandler == nil {
		return ErrorNotAvailable
	}

	cmd, err := protocol.GetParamsDecoder(c.transport.Protocol().toProto()).DecodeSubscribe(params)
	if err != nil {
		return c.logDisconnectBadRequestWithError(err, "error decoding subscribe")
	}

	replyError, disconnect := c.validateSubscribeRequest(cmd)
	if disconnect != nil || replyError != nil {
		if disconnect != nil {
			return disconnect
		}
		return replyError
	}

	event := SubscribeEvent{
		Channel: cmd.Channel,
		Token:   cmd.Token,
	}

	cb := func(reply SubscribeReply, err error) {
		defer rw.done()

		if err != nil {
			c.onSubscribeError(cmd.Channel)
			c.writeDisconnectOrErrorFlush(rw, err)
			return
		}

		ctx := c.subscribeCmd(cmd, reply, rw, false)

		if ctx.disconnect != nil {
			c.onSubscribeError(cmd.Channel)
			c.Disconnect(ctx.disconnect)
			return
		}
		if ctx.err != nil {
			c.onSubscribeError(cmd.Channel)
			c.writeDisconnectOrErrorFlush(rw, ctx.err)
			return
		}

		if channelHasFlag(ctx.channelContext.flags, flagJoinLeave) && ctx.clientInfo != nil {
			go func() { _ = c.node.publishJoin(cmd.Channel, ctx.clientInfo) }()
		}
	}
	c.eventHub.subscribeHandler(event, cb)
	return nil
}

func (c *Client) getSubscribedChannelContext(channel string) (channelContext, bool) {
	c.mu.RLock()
	ctx, okChannel := c.channels[channel]
	c.mu.RUnlock()
	if !okChannel || !channelHasFlag(ctx.flags, flagSubscribed) {
		return channelContext{}, false
	}
	return ctx, true
}

func (c *Client) handleSubRefresh(params protocol.Raw, rw *replyWriter) error {
	if c.eventHub.subRefreshHandler == nil {
		return ErrorNotAvailable
	}

	cmd, err := protocol.GetParamsDecoder(c.transport.Protocol().toProto()).DecodeSubRefresh(params)
	if err != nil {
		return c.logDisconnectBadRequestWithError(err, "error decoding sub refresh")
	}

	channel := cmd.Channel
	if channel == "" {
		return c.logDisconnectBadRequest("channel required for sub refresh")
	}

	ctx, okChannel := c.getSubscribedChannelContext(channel)
	if !okChannel {
		// Must be subscribed to refresh subscription.
		return ErrorPermissionDenied
	}

	clientSideRefresh := channelHasFlag(ctx.flags, flagClientSideRefresh)
	if !clientSideRefresh {
		// Client not supposed to send sub refresh command in case of server-side
		// subscription refresh mechanism.
		return c.logDisconnectBadRequest("server-side sub refresh expected")
	}

	if cmd.Token == "" {
		c.node.logger.log(newLogEntry(LogLevelInfo, "subscription refresh token required", map[string]interface{}{"client": c.uid, "user": c.UserID()}))
		return ErrorBadRequest
	}

	event := SubRefreshEvent{
		ClientSideRefresh: true,
		Channel:           cmd.Channel,
		Token:             cmd.Token,
	}

	cb := func(reply SubRefreshReply, err error) {
		defer rw.done()

		if err != nil {
			c.writeDisconnectOrErrorFlush(rw, err)
			return
		}

		res := &protocol.SubRefreshResult{}

		if reply.ExpireAt > 0 {
			res.Expires = true
			now := time.Now().Unix()
			if reply.ExpireAt < now {
				c.writeError(rw, ErrorExpired)
				return
			}
			res.Ttl = uint32(reply.ExpireAt - now)
		}

		c.mu.Lock()
		channelContext, okChan := c.channels[channel]
		if okChan && channelHasFlag(channelContext.flags, flagSubscribed) {
			channelContext.Info = reply.Info
			channelContext.expireAt = reply.ExpireAt
			c.channels[channel] = channelContext
		}
		c.mu.Unlock()

		replyRes, err := protocol.GetResultEncoder(c.transport.Protocol().toProto()).EncodeSubRefreshResult(res)
		if err != nil {
			c.logWriteInternalErrorFlush(rw, err, "error encoding sub refresh")
			return
		}
		_ = writeReply(rw, &protocol.Reply{Result: replyRes})
	}

	c.eventHub.subRefreshHandler(event, cb)
	return nil
}

func (c *Client) handleUnsubscribe(params protocol.Raw, rw *replyWriter) error {
	cmd, err := protocol.GetParamsDecoder(c.transport.Protocol().toProto()).DecodeUnsubscribe(params)
	if err != nil {
		return c.logDisconnectBadRequestWithError(err, "error decoding unsubscribe")
	}

	channel := cmd.Channel
	if channel == "" {
		return c.logDisconnectBadRequest("channel required for unsubscribe")
	}

	if err := c.unsubscribe(channel); err != nil {
		return err
	}

	replyRes, err := protocol.GetResultEncoder(c.transport.Protocol().toProto()).EncodeUnsubscribeResult(&protocol.UnsubscribeResult{})
	if err != nil {
		c.node.logger.log(newLogEntry(LogLevelError, "error encoding unsubscribe", map[string]interface{}{"error": err.Error()}))
		return DisconnectServerError
	}

	_ = writeReply(rw, &protocol.Reply{Result: replyRes})
	rw.done()
	return nil
}

func (c *Client) handlePublish(params protocol.Raw, rw *replyWriter) error {
	if c.eventHub.publishHandler == nil {
		return ErrorNotAvailable
	}

	cmd, err := protocol.GetParamsDecoder(c.transport.Protocol().toProto()).DecodePublish(params)
	if err != nil {
		return c.logDisconnectBadRequestWithError(err, "error decoding publish")
	}

	channel := cmd.Channel
	data := cmd.Data

	if channel == "" || len(data) == 0 {
		return c.logDisconnectBadRequest("channel and data required for publish")
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
		defer rw.done()

		if err != nil {
			c.writeDisconnectOrErrorFlush(rw, err)
			return
		}

		if reply.Result == nil {
			_, err := c.node.Publish(
				event.Channel, event.Data,
				WithHistory(reply.Options.HistorySize, reply.Options.HistoryTTL),
				WithClientInfo(reply.Options.ClientInfo),
			)
			if err != nil {
				c.logWriteInternalErrorFlush(rw, err, "error publish")
				return
			}
		}

		replyRes, err := protocol.GetResultEncoder(c.transport.Protocol().toProto()).EncodePublishResult(&protocol.PublishResult{})
		if err != nil {
			c.logWriteInternalErrorFlush(rw, err, "error encoding publish")
			return
		}
		_ = writeReply(rw, &protocol.Reply{Result: replyRes})
	}

	c.eventHub.publishHandler(event, cb)
	return nil
}

func (c *Client) handlePresence(params protocol.Raw, rw *replyWriter) error {
	if c.eventHub.presenceHandler == nil {
		return ErrorNotAvailable
	}

	cmd, err := protocol.GetParamsDecoder(c.transport.Protocol().toProto()).DecodePresence(params)
	if err != nil {
		return c.logDisconnectBadRequestWithError(err, "error decoding presence")
	}

	channel := cmd.Channel
	if channel == "" {
		return c.logDisconnectBadRequest("channel required for presence")
	}

	event := PresenceEvent{
		Channel: channel,
	}

	cb := func(reply PresenceReply, err error) {
		defer rw.done()
		if err != nil {
			c.writeDisconnectOrErrorFlush(rw, err)
			return
		}

		var presence map[string]*ClientInfo
		if reply.Result == nil {
			result, err := c.node.Presence(event.Channel)
			if err != nil {
				c.logWriteInternalErrorFlush(rw, err, "error getting presence")
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

		replyRes, err := protocol.GetResultEncoder(c.transport.Protocol().toProto()).EncodePresenceResult(&protocol.PresenceResult{
			Presence: protoPresence,
		})
		if err != nil {
			c.logWriteInternalErrorFlush(rw, err, "error encoding presence")
			return
		}
		_ = writeReply(rw, &protocol.Reply{Result: replyRes})
	}

	c.eventHub.presenceHandler(event, cb)
	return nil
}

func (c *Client) handlePresenceStats(params protocol.Raw, rw *replyWriter) error {
	if c.eventHub.presenceStatsHandler == nil {
		return ErrorNotAvailable
	}

	cmd, err := protocol.GetParamsDecoder(c.transport.Protocol().toProto()).DecodePresenceStats(params)
	if err != nil {
		return c.logDisconnectBadRequestWithError(err, "error decoding presence stats")
	}

	channel := cmd.Channel
	if channel == "" {
		return c.logDisconnectBadRequest("channel required for presence stats")
	}

	event := PresenceStatsEvent{
		Channel: channel,
	}

	cb := func(reply PresenceStatsReply, err error) {
		defer rw.done()
		if err != nil {
			c.writeDisconnectOrErrorFlush(rw, err)
			return
		}

		var presenceStats PresenceStats
		if reply.Result == nil {
			result, err := c.node.PresenceStats(event.Channel)
			if err != nil {
				c.logWriteInternalErrorFlush(rw, err, "error getting presence stats")
				return
			}
			presenceStats = result.PresenceStats
		} else {
			presenceStats = reply.Result.PresenceStats
		}

		replyRes, err := protocol.GetResultEncoder(c.transport.Protocol().toProto()).EncodePresenceStatsResult(&protocol.PresenceStatsResult{
			NumClients: uint32(presenceStats.NumClients),
			NumUsers:   uint32(presenceStats.NumUsers),
		})
		if err != nil {
			c.logWriteInternalErrorFlush(rw, err, "error encoding presence stats")
			return
		}
		_ = writeReply(rw, &protocol.Reply{Result: replyRes})
	}

	c.eventHub.presenceStatsHandler(event, cb)
	return nil
}

func (c *Client) handleHistory(params protocol.Raw, rw *replyWriter) error {
	if c.eventHub.historyHandler == nil {
		return ErrorNotAvailable
	}

	cmd, err := protocol.GetParamsDecoder(c.transport.Protocol().toProto()).DecodeHistory(params)
	if err != nil {
		return c.logDisconnectBadRequestWithError(err, "error decoding history")
	}

	channel := cmd.Channel
	if channel == "" {
		return c.logDisconnectBadRequest("channel required for history")
	}

	var filter HistoryFilter
	if cmd.Since != nil {
		filter.Since = &StreamPosition{
			Offset: cmd.Since.Offset,
			Epoch:  cmd.Since.Epoch,
		}
	}
	filter.Limit = int(cmd.Limit)

	maxPublicationLimit := c.node.config.HistoryMaxPublicationLimit
	if maxPublicationLimit > 0 && (filter.Limit < 0 || filter.Limit > maxPublicationLimit) {
		filter.Limit = maxPublicationLimit
	}

	filter.Reverse = cmd.Reverse

	event := HistoryEvent{
		Channel: channel,
		Filter:  filter,
	}

	cb := func(reply HistoryReply, err error) {
		defer rw.done()
		if err != nil {
			c.writeDisconnectOrErrorFlush(rw, err)
			return
		}

		var pubs []*Publication
		var offset uint64
		var epoch string
		if reply.Result == nil {
			result, err := c.node.History(event.Channel, WithLimit(event.Filter.Limit), WithSince(event.Filter.Since), WithReverse(event.Filter.Reverse))
			if err != nil {
				c.logWriteInternalErrorFlush(rw, err, "error getting history")
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

		replyRes, err := protocol.GetResultEncoder(c.transport.Protocol().toProto()).EncodeHistoryResult(&protocol.HistoryResult{
			Publications: protoPubs,
			Offset:       offset,
			Epoch:        epoch,
		})
		if err != nil {
			c.logWriteInternalErrorFlush(rw, err, "error encoding history")
			return
		}
		_ = writeReply(rw, &protocol.Reply{Result: replyRes})
	}

	c.eventHub.historyHandler(event, cb)
	return nil
}

func (c *Client) handlePing(params protocol.Raw, rw *replyWriter) error {
	_, err := protocol.GetParamsDecoder(c.transport.Protocol().toProto()).DecodePing(params)
	if err != nil {
		return c.logDisconnectBadRequestWithError(err, "error decoding ping")
	}
	_ = writeReply(rw, &protocol.Reply{})
	defer rw.done()
	return nil
}

func (c *Client) writeError(rw *replyWriter, error *Error) {
	_ = rw.write(&protocol.Reply{Error: error.toProto()})
}

func (c *Client) writeDisconnectOrErrorFlush(rw *replyWriter, replyError error) {
	switch t := replyError.(type) {
	case *Disconnect:
		go func() { _ = c.close(t) }()
		return
	default:
		c.writeError(rw, toClientErr(replyError))
	}
}

func writeReply(rw *replyWriter, reply *protocol.Reply) error {
	return rw.write(reply)
}

func (c *Client) handleRPC(params protocol.Raw, rw *replyWriter) error {
	if c.eventHub.rpcHandler == nil {
		return ErrorNotAvailable
	}
	cmd, err := protocol.GetParamsDecoder(c.transport.Protocol().toProto()).DecodeRPC(params)
	if err != nil {
		return c.logDisconnectBadRequestWithError(err, "error decoding rpc")
	}

	event := RPCEvent{
		Method: cmd.Method,
		Data:   cmd.Data,
	}

	cb := func(reply RPCReply, err error) {
		defer rw.done()
		if err != nil {
			c.writeDisconnectOrErrorFlush(rw, err)
			return
		}
		result := &protocol.RPCResult{
			Data: reply.Data,
		}
		var replyRes []byte
		replyRes, err = protocol.GetResultEncoder(c.transport.Protocol().toProto()).EncodeRPCResult(result)
		if err != nil {
			c.logWriteInternalErrorFlush(rw, err, "error encoding rpc")
			return
		}
		_ = writeReply(rw, &protocol.Reply{Result: replyRes})
	}

	c.eventHub.rpcHandler(event, cb)
	return nil
}

func (c *Client) handleSend(params protocol.Raw, rw *replyWriter) error {
	if c.eventHub.messageHandler == nil {
		// send handler is a bit special since it is only one way
		// request: client does not expect any reply.
		rw.done()
		return nil
	}
	cmd, err := protocol.GetParamsDecoder(c.transport.Protocol().toProto()).DecodeSend(params)
	if err != nil {
		return c.logDisconnectBadRequestWithError(err, "error decoding message")
	}
	defer rw.done()
	c.eventHub.messageHandler(MessageEvent{
		Data: cmd.Data,
	})
	return nil
}

func (c *Client) unlockServerSideSubscriptions(subCtxMap map[string]subscribeContext) {
	for channel := range subCtxMap {
		c.pubSubSync.StopBuffering(channel)
	}
}

// connectCmd handles connect command from client - client must send connect
// command immediately after establishing connection with server.
func (c *Client) connectCmd(cmd *protocol.ConnectRequest, rw *replyWriter) (*protocol.ConnectResult, error) {
	c.mu.RLock()
	authenticated := c.authenticated
	closed := c.status == statusClosed
	c.mu.RUnlock()

	if closed {
		return nil, DisconnectNormal
	}

	if authenticated {
		return nil, c.logDisconnectBadRequest("client already authenticated")
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
	)

	if c.node.clientEvents.connectingHandler != nil {
		e := ConnectEvent{
			ClientID:  c.ID(),
			Data:      cmd.Data,
			Token:     cmd.Token,
			Name:      cmd.Name,
			Version:   cmd.Version,
			Transport: c.transport,
		}
		if len(cmd.Subs) > 0 {
			channels := make([]string, 0, len(cmd.Subs))
			for ch := range cmd.Subs {
				channels = append(channels, ch)
			}
			e.Channels = channels
		}
		reply, err := c.node.clientEvents.connectingHandler(c.ctx, e)
		if err != nil {
			return nil, err
		}
		if reply.Credentials != nil {
			credentials = reply.Credentials
		}
		if reply.Context != nil {
			c.mu.Lock()
			c.ctx = reply.Context
			c.mu.Unlock()
		}
		if reply.Data != nil {
			authData = reply.Data
		}
		clientSideRefresh = reply.ClientSideRefresh
		if len(reply.Subscriptions) > 0 {
			subscriptions = make(map[string]SubscribeOptions, len(reply.Subscriptions))
			for ch, opts := range reply.Subscriptions {
				if ch == "" {
					continue
				}
				subscriptions[ch] = opts
			}
		}
	}

	if channelLimit > 0 && len(subscriptions) > channelLimit {
		return nil, DisconnectChannelLimit
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
		return nil, c.logDisconnectBadRequest("client credentials not found")
	}

	c.mu.Lock()
	c.user = credentials.UserID
	c.info = credentials.Info
	c.exp = credentials.ExpireAt

	user := c.user
	exp := c.exp
	closed = c.status == statusClosed
	c.mu.Unlock()

	if closed {
		return nil, DisconnectNormal
	}

	c.node.logger.log(newLogEntry(LogLevelDebug, "client authenticated", map[string]interface{}{"client": c.uid, "user": c.user}))

	if userConnectionLimit > 0 && user != "" && len(c.node.hub.UserConnections(user)) >= userConnectionLimit {
		c.node.logger.log(newLogEntry(LogLevelInfo, "limit of connections for user reached", map[string]interface{}{"user": user, "client": c.uid, "limit": userConnectionLimit}))
		return nil, DisconnectConnectionLimit
	}

	c.mu.RLock()
	if exp > 0 {
		expires = true
		now := time.Now().Unix()
		if exp < now {
			c.mu.RUnlock()
			c.node.logger.log(newLogEntry(LogLevelInfo, "connection expiration must be greater than now", map[string]interface{}{"client": c.uid, "user": c.UserID()}))
			return nil, ErrorExpired
		}
		ttl = uint32(exp - now)
	}
	c.mu.RUnlock()

	res := &protocol.ConnectResult{
		Version: version,
		Expires: expires,
		Ttl:     ttl,
	}

	// Client successfully connected.
	c.mu.Lock()
	c.authenticated = true
	c.mu.Unlock()

	err := c.node.addClient(c)
	if err != nil {
		c.node.logger.log(newLogEntry(LogLevelError, "error adding client", map[string]interface{}{"client": c.uid, "error": err.Error()}))
		return nil, DisconnectServerError
	}

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
				if subReq, ok := cmd.Subs[ch]; ok {
					subCmd.Recover = subReq.Recover
					subCmd.Offset = subReq.Offset
					subCmd.Epoch = subReq.Epoch
				}
				subCtx := c.subscribeCmd(subCmd, SubscribeReply{Options: opts}, rw, true)
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
			for channel := range subCtxMap {
				c.onSubscribeError(channel)
			}
			if subDisconnect != nil {
				return nil, subDisconnect
			}
			return nil, subError
		}
		res.Subs = subs
	}

	if c.transport.Unidirectional() {
		connectPushBytes, err := c.encodeConnectPush(res)
		if err != nil {
			c.unlockServerSideSubscriptions(subCtxMap)
			c.node.logger.log(newLogEntry(LogLevelError, "error encoding connect", map[string]interface{}{"error": err.Error()}))
			return nil, DisconnectServerError
		}
		_ = writeReply(rw, &protocol.Reply{Result: connectPushBytes})
		defer rw.done()
	} else {
		replyRes, err := protocol.GetResultEncoder(c.transport.Protocol().toProto()).EncodeConnectResult(res)
		if err != nil {
			c.unlockServerSideSubscriptions(subCtxMap)
			c.node.logger.log(newLogEntry(LogLevelError, "error encoding connect", map[string]interface{}{"error": err.Error()}))
			return nil, DisconnectServerError
		}
		_ = writeReply(rw, &protocol.Reply{Result: replyRes})
		defer rw.done()
	}

	c.mu.Lock()
	for channel, subCtx := range subCtxMap {
		c.channels[channel] = subCtx.channelContext
	}
	c.mu.Unlock()

	c.unlockServerSideSubscriptions(subCtxMap)

	if len(subCtxMap) > 0 {
		for channel, subCtx := range subCtxMap {
			go func(channel string, subCtx subscribeContext) {
				if channelHasFlag(subCtx.channelContext.flags, flagJoinLeave) && subCtx.clientInfo != nil {
					_ = c.node.publishJoin(channel, subCtx.clientInfo)
				}
			}(channel, subCtx)
		}
	}

	return res, nil
}

// Subscribe client to a channel.
func (c *Client) Subscribe(channel string, opts ...SubscribeOption) error {
	if channel == "" {
		return fmt.Errorf("channel is empty")
	}
	channelLimit := c.node.config.ClientChannelLimit
	c.mu.RLock()
	numChannels := len(c.channels)
	c.mu.RUnlock()
	if channelLimit > 0 && numChannels >= channelLimit {
		go func() { _ = c.close(DisconnectChannelLimit) }()
		return nil
	}

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
	}, nil, true)
	if subCtx.err != nil {
		c.onSubscribeError(subCmd.Channel)
		return subCtx.err
	}
	defer c.pubSubSync.StopBuffering(channel)
	c.mu.Lock()
	c.channels[channel] = subCtx.channelContext
	c.mu.Unlock()
	if hasFlag(c.transport.DisabledPushFlags(), PushFlagSubscribe) {
		return nil
	}
	sub := &protocol.Subscribe{
		Offset:      subCtx.result.GetOffset(),
		Epoch:       subCtx.result.GetEpoch(),
		Recoverable: subCtx.result.GetRecoverable(),
		Positioned:  subCtx.result.GetPositioned(),
		Data:        subCtx.result.Data,
	}
	pushBytes, err := protocol.EncodeSubscribePush(c.transport.Protocol().toProto(), channel, sub)
	if err != nil {
		return err
	}
	reply := prepared.NewReply(&protocol.Reply{
		Result: pushBytes,
	}, c.transport.Protocol().toProto())
	return c.transportEnqueue(reply)
}

func (c *Client) validateSubscribeRequest(cmd *protocol.SubscribeRequest) (*Error, *Disconnect) {
	channel := cmd.Channel
	if channel == "" {
		c.node.logger.log(newLogEntry(LogLevelInfo, "channel required for subscribe", map[string]interface{}{"user": c.user, "client": c.uid}))
		return nil, DisconnectBadRequest
	}

	config := c.node.config
	channelMaxLength := config.ChannelMaxLength
	channelLimit := config.ClientChannelLimit

	if channelMaxLength > 0 && len(channel) > channelMaxLength {
		c.node.logger.log(newLogEntry(LogLevelInfo, "channel too long", map[string]interface{}{"max": channelMaxLength, "channel": channel, "user": c.user, "client": c.uid}))
		return ErrorBadRequest, nil
	}

	c.mu.Lock()
	numChannels := len(c.channels)
	_, ok := c.channels[channel]
	if ok {
		c.mu.Unlock()
		c.node.logger.log(newLogEntry(LogLevelInfo, "client already subscribed on channel", map[string]interface{}{"channel": channel, "user": c.user, "client": c.uid}))
		return ErrorAlreadySubscribed, nil
	}
	if channelLimit > 0 && numChannels >= channelLimit {
		c.mu.Unlock()
		c.node.logger.log(newLogEntry(LogLevelInfo, "maximum limit of channels per client reached", map[string]interface{}{"limit": channelLimit, "user": c.user, "client": c.uid}))
		return ErrorLimitExceeded, nil
	}
	// Put channel to a map to track duplicate subscriptions. This channel should
	// be removed from a map upon an error during subscribe.
	c.channels[channel] = channelContext{}
	c.mu.Unlock()

	return nil, nil
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
	channelContext channelContext
}

func isRecovered(historyResult HistoryResult, cmdOffset uint64, cmdEpoch string) ([]*protocol.Publication, bool) {
	latestOffset := historyResult.Offset
	latestEpoch := historyResult.Epoch

	recoveredPubs := make([]*protocol.Publication, 0, len(historyResult.Publications))
	for _, pub := range historyResult.Publications {
		protoPub := pubToProto(pub)
		recoveredPubs = append(recoveredPubs, protoPub)
	}

	nextOffset := cmdOffset + 1
	var recovered bool
	if len(recoveredPubs) == 0 {
		recovered = latestOffset == cmdOffset && (cmdEpoch == "" || latestEpoch == cmdEpoch)
	} else {
		recovered = recoveredPubs[0].Offset == nextOffset &&
			recoveredPubs[len(recoveredPubs)-1].Offset == latestOffset &&
			(cmdEpoch == "" || latestEpoch == cmdEpoch)
	}

	return recoveredPubs, recovered
}

// subscribeCmd handles subscribe command - clients send this when subscribe
// on channel, if channel if private then we must validate provided sign here before
// actually subscribe client on channel. Optionally we can send missed messages to
// client if it provided last message id seen in channel.
func (c *Client) subscribeCmd(cmd *protocol.SubscribeRequest, reply SubscribeReply, rw *replyWriter, serverSide bool) subscribeContext {

	ctx := subscribeContext{}
	res := &protocol.SubscribeResult{}

	if reply.Options.ExpireAt > 0 {
		ttl := reply.Options.ExpireAt - time.Now().Unix()
		if ttl <= 0 {
			c.node.logger.log(newLogEntry(LogLevelInfo, "subscription expiration must be greater than now", map[string]interface{}{"client": c.uid, "user": c.UserID()}))
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

	channel := cmd.Channel

	info := &ClientInfo{
		ClientID: c.uid,
		UserID:   c.user,
		ConnInfo: c.info,
		ChanInfo: reply.Options.ChannelInfo,
	}

	if reply.Options.Recover {
		// Start syncing recovery and PUB/SUB.
		// The important thing is to call StopBuffering for this channel
		// after response with Publications written to connection.
		c.pubSubSync.StartBuffering(channel)
	}

	err := c.node.addSubscription(channel, c)
	if err != nil {
		c.node.logger.log(newLogEntry(LogLevelError, "error adding subscription", map[string]interface{}{"channel": channel, "user": c.user, "client": c.uid, "error": err.Error()}))
		c.pubSubSync.StopBuffering(channel)
		if clientErr, ok := err.(*Error); ok && clientErr != ErrorInternal {
			return errorDisconnectContext(clientErr, nil)
		}
		ctx.disconnect = DisconnectServerError
		return ctx
	}

	if reply.Options.Presence {
		err = c.node.addPresence(channel, c.uid, info)
		if err != nil {
			c.node.logger.log(newLogEntry(LogLevelError, "error adding presence", map[string]interface{}{"channel": channel, "user": c.user, "client": c.uid, "error": err.Error()}))
			c.pubSubSync.StopBuffering(channel)
			ctx.disconnect = DisconnectServerError
			return ctx
		}
	}

	var (
		latestOffset  uint64
		latestEpoch   string
		recoveredPubs []*protocol.Publication
	)

	if reply.Options.Recover {
		res.Recoverable = true
		res.Positioned = true // recoverable subscriptions are automatically positioned.
		if cmd.Recover {
			cmdOffset := cmd.Offset

			// Client provided subscribe request with recover flag on. Try to recover missed
			// publications automatically from history (we suppose here that history configured wisely).
			historyResult, err := c.node.recoverHistory(channel, StreamPosition{cmdOffset, cmd.Epoch})
			if err != nil {
				if errors.Is(err, ErrorUnrecoverablePosition) {
					// Result contains stream position in case of ErrorUnrecoverablePosition
					// during recovery.
					latestOffset = historyResult.Offset
					latestEpoch = historyResult.Epoch
					res.Recovered = false
					incRecover(res.Recovered)
				} else {
					c.node.logger.log(newLogEntry(LogLevelError, "error on recover", map[string]interface{}{"channel": channel, "user": c.user, "client": c.uid, "error": err.Error()}))
					c.pubSubSync.StopBuffering(channel)
					if clientErr, ok := err.(*Error); ok && clientErr != ErrorInternal {
						return errorDisconnectContext(clientErr, nil)
					}
					ctx.disconnect = DisconnectServerError
					return ctx
				}
			} else {
				latestOffset = historyResult.Offset
				latestEpoch = historyResult.Epoch
				var recovered bool
				recoveredPubs, recovered = isRecovered(historyResult, cmdOffset, cmd.Epoch)
				res.Recovered = recovered
				incRecover(res.Recovered)
			}
		} else {
			streamTop, err := c.node.streamTop(channel)
			if err != nil {
				c.node.logger.log(newLogEntry(LogLevelError, "error getting recovery state for channel", map[string]interface{}{"channel": channel, "user": c.user, "client": c.uid, "error": err.Error()}))
				c.pubSubSync.StopBuffering(channel)
				if clientErr, ok := err.(*Error); ok && clientErr != ErrorInternal {
					return errorDisconnectContext(clientErr, nil)
				}
				ctx.disconnect = DisconnectServerError
				return ctx
			}
			latestOffset = streamTop.Offset
			latestEpoch = streamTop.Epoch
		}

		res.Epoch = latestEpoch
		res.Offset = latestOffset

		bufferedPubs := c.pubSubSync.LockBufferAndReadBuffered(channel)
		var okMerge bool
		recoveredPubs, okMerge = recovery.MergePublications(recoveredPubs, bufferedPubs)
		if !okMerge {
			c.pubSubSync.StopBuffering(channel)
			ctx.disconnect = DisconnectInsufficientState
			return ctx
		}
	} else if reply.Options.Position {
		streamTop, err := c.node.streamTop(channel)
		if err != nil {
			c.node.logger.log(newLogEntry(LogLevelError, "error getting stream top for channel", map[string]interface{}{"channel": channel, "user": c.user, "client": c.uid, "error": err.Error()}))
			if clientErr, ok := err.(*Error); ok && clientErr != ErrorInternal {
				return errorDisconnectContext(clientErr, nil)
			}
			ctx.disconnect = DisconnectServerError
			return ctx
		}

		latestOffset = streamTop.Offset
		latestEpoch = streamTop.Epoch

		res.Positioned = true
		res.Offset = streamTop.Offset
		res.Epoch = streamTop.Epoch
	}

	if len(recoveredPubs) > 0 {
		lastPubOffset := recoveredPubs[len(recoveredPubs)-1].Offset
		if lastPubOffset > res.Offset {
			// There can be a case when recovery returned a limited set of publications
			// thus last publication offset will be smaller than history current offset.
			// In this case res.Recovered will be false. So we take a maximum here.
			latestOffset = recoveredPubs[len(recoveredPubs)-1].Offset
			res.Offset = latestOffset
		}
	}

	res.Publications = recoveredPubs

	if !serverSide {
		// Write subscription reply only if initiated by client.
		replyRes, err := protocol.GetResultEncoder(c.transport.Protocol().toProto()).EncodeSubscribeResult(res)
		if err != nil {
			c.node.logger.log(newLogEntry(LogLevelError, "error encoding subscribe", map[string]interface{}{"error": err.Error()}))
			if !serverSide {
				// Will be called later in case of server side sub.
				c.pubSubSync.StopBuffering(channel)
			}
			ctx.disconnect = DisconnectServerError
			return ctx
		}
		// Need to flush data from writer so subscription response is
		// sent before any subscription publication.
		_ = writeReply(rw, &protocol.Reply{Result: replyRes})
	}

	var channelFlags uint8
	channelFlags |= flagSubscribed
	if serverSide {
		channelFlags |= flagServerSide
	}
	if reply.ClientSideRefresh {
		channelFlags |= flagClientSideRefresh
	}
	if reply.Options.Recover {
		channelFlags |= flagRecover
	}
	if reply.Options.Position {
		channelFlags |= flagPosition
	}
	if reply.Options.Presence {
		channelFlags |= flagPresence
	}
	if reply.Options.JoinLeave {
		channelFlags |= flagJoinLeave
	}

	channelContext := channelContext{
		Info:     reply.Options.ChannelInfo,
		flags:    channelFlags,
		expireAt: reply.Options.ExpireAt,
		streamPosition: StreamPosition{
			Offset: latestOffset,
			Epoch:  latestEpoch,
		},
	}
	if reply.Options.Recover || reply.Options.Position {
		channelContext.positionCheckTime = time.Now().Unix()
	}

	if !serverSide {
		// In case of server-side sub this will be done later by the caller.
		c.mu.Lock()
		c.channels[channel] = channelContext
		c.mu.Unlock()
		// Stop syncing recovery and PUB/SUB.
		// In case of server side subscription we will do this later.
		c.pubSubSync.StopBuffering(channel)
	}

	if c.node.logger.enabled(LogLevelDebug) {
		c.node.logger.log(newLogEntry(LogLevelDebug, "client subscribed to channel", map[string]interface{}{"client": c.uid, "user": c.user, "channel": cmd.Channel}))
	}

	ctx.result = res
	ctx.clientInfo = info
	ctx.channelContext = channelContext
	return ctx
}

func (c *Client) writePublicationUpdatePosition(ch string, pub *protocol.Publication, reply *prepared.Reply, sp StreamPosition) error {
	c.mu.Lock()
	channelContext, ok := c.channels[ch]
	if !ok || !channelHasFlag(channelContext.flags, flagSubscribed) {
		c.mu.Unlock()
		return nil
	}
	if !channelHasFlag(channelContext.flags, flagRecover|flagPosition) {
		if hasFlag(c.transport.DisabledPushFlags(), PushFlagPublication) {
			c.mu.Unlock()
			return nil
		}
		c.mu.Unlock()
		return c.transportEnqueue(reply)
	}
	currentPositionOffset := channelContext.streamPosition.Offset
	nextExpectedOffset := currentPositionOffset + 1
	pubOffset := pub.Offset
	pubEpoch := sp.Epoch
	if pubEpoch != channelContext.streamPosition.Epoch {
		if c.node.logger.enabled(LogLevelDebug) {
			c.node.logger.log(newLogEntry(LogLevelDebug, "client insufficient state", map[string]interface{}{"channel": ch, "user": c.user, "client": c.uid, "epoch": pubEpoch, "expectedEpoch": channelContext.streamPosition.Epoch}))
		}
		// Oops: sth lost, let client reconnect to recover its state.
		go func() { _ = c.close(DisconnectInsufficientState) }()
		c.mu.Unlock()
		return nil
	}
	if pubOffset != nextExpectedOffset {
		if c.node.logger.enabled(LogLevelDebug) {
			c.node.logger.log(newLogEntry(LogLevelDebug, "client insufficient state", map[string]interface{}{"channel": ch, "user": c.user, "client": c.uid, "offset": pubOffset, "expectedOffset": nextExpectedOffset}))
		}
		// Oops: sth lost, let client reconnect to recover its state.
		go func() { _ = c.close(DisconnectInsufficientState) }()
		c.mu.Unlock()
		return nil
	}
	channelContext.positionCheckTime = time.Now().Unix()
	channelContext.positionCheckFailures = 0
	channelContext.streamPosition.Offset = pub.Offset
	c.channels[ch] = channelContext
	c.mu.Unlock()
	if hasFlag(c.transport.DisabledPushFlags(), PushFlagPublication) {
		return nil
	}
	return c.transportEnqueue(reply)
}

func (c *Client) writePublication(ch string, pub *protocol.Publication, reply *prepared.Reply, sp StreamPosition) error {
	if pub.Offset == 0 {
		if hasFlag(c.transport.DisabledPushFlags(), PushFlagPublication) {
			return nil
		}
		return c.transportEnqueue(reply)
	}
	c.pubSubSync.SyncPublication(ch, pub, func() {
		_ = c.writePublicationUpdatePosition(ch, pub, reply, sp)
	})
	return nil
}

func (c *Client) writeJoin(_ string, reply *prepared.Reply) error {
	if hasFlag(c.transport.DisabledPushFlags(), PushFlagJoin) {
		return nil
	}
	return c.transportEnqueue(reply)
}

func (c *Client) writeLeave(_ string, reply *prepared.Reply) error {
	if hasFlag(c.transport.DisabledPushFlags(), PushFlagLeave) {
		return nil
	}
	return c.transportEnqueue(reply)
}

// Lock must be held outside.
func (c *Client) unsubscribe(channel string) error {
	c.mu.RLock()
	info := c.clientInfo(channel)
	chCtx, ok := c.channels[channel]
	serverSide := channelHasFlag(chCtx.flags, flagServerSide)
	c.mu.RUnlock()

	if ok {
		c.mu.Lock()
		delete(c.channels, channel)
		c.mu.Unlock()

		if channelHasFlag(chCtx.flags, flagPresence) && channelHasFlag(chCtx.flags, flagSubscribed) {
			err := c.node.removePresence(channel, c.uid)
			if err != nil {
				c.node.logger.log(newLogEntry(LogLevelError, "error removing channel presence", map[string]interface{}{"channel": channel, "user": c.user, "client": c.uid, "error": err.Error()}))
			}
		}

		if channelHasFlag(chCtx.flags, flagJoinLeave) && channelHasFlag(chCtx.flags, flagSubscribed) {
			_ = c.node.publishLeave(channel, info)
		}

		if err := c.node.removeSubscription(channel, c); err != nil {
			c.node.logger.log(newLogEntry(LogLevelError, "error removing subscription", map[string]interface{}{"channel": channel, "user": c.user, "client": c.uid, "error": err.Error()}))
			return err
		}

		if channelHasFlag(chCtx.flags, flagSubscribed) {
			if c.eventHub.unsubscribeHandler != nil {
				c.eventHub.unsubscribeHandler(UnsubscribeEvent{
					Channel:    channel,
					ServerSide: serverSide,
				})
			}
		}
	}
	if c.node.logger.enabled(LogLevelDebug) {
		c.node.logger.log(newLogEntry(LogLevelDebug, "client unsubscribed from channel", map[string]interface{}{"channel": channel, "user": c.user, "client": c.uid}))
	}
	return nil
}

func (c *Client) logDisconnectBadRequest(message string) *Disconnect {
	c.node.logger.log(newLogEntry(LogLevelInfo, message, map[string]interface{}{"user": c.user, "client": c.uid}))
	return DisconnectBadRequest
}

func (c *Client) logDisconnectBadRequestWithError(err error, message string) *Disconnect {
	c.node.logger.log(newLogEntry(LogLevelInfo, message, map[string]interface{}{"error": err.Error(), "user": c.user, "client": c.uid}))
	return DisconnectBadRequest
}

func (c *Client) logWriteInternalErrorFlush(rw *replyWriter, err error, message string) {
	if clientErr, ok := err.(*Error); ok {
		c.writeError(rw, clientErr)
		return
	}
	c.node.logger.log(newLogEntry(LogLevelError, message, map[string]interface{}{"error": err.Error()}))
	c.writeError(rw, ErrorInternal)
}

func toClientErr(err error) *Error {
	if clientErr, ok := err.(*Error); ok {
		return clientErr
	}
	return ErrorInternal
}

func errLogLevel(err error) LogLevel {
	logLevel := LogLevelInfo
	if err != ErrorNotAvailable {
		logLevel = LogLevelError
	}
	return logLevel
}
