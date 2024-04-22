package centrifuge

import (
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/centrifugal/centrifuge/internal/timers"
)

// ChannelCacheOptions is an EXPERIMENTAL way to provide a channel cache layer options to Centrifuge.
// This is very unstable at the moment, do not use in production.
type ChannelCacheOptions struct {
	// BroadcastDelay controls delay before Publication broadcast. On time tick Centrifugo broadcasts
	// only the latest publication in the channel. Useful to reduce the number of messages sent to clients
	// when publication contains the entire state. If zero, all publications will be sent to clients without
	// delay logic involved on channel cache level. This option requires (!) UseQueue to be enabled, as we
	// can not afford delays during synchronous broadcast.
	BroadcastDelay time.Duration
	// PositionSyncInterval is a time interval to check if we need to sync stream position state with Broker
	// to detect PUB/SUB layer message loss. By default, no sync is performed – in that case each individual
	// connection syncs position separately.
	// TODO: need a mechanism to communicate with Clients that sync is done in cache layer.
	PositionSyncInterval time.Duration
	// UseQueue enables queue for incoming publications. This can be useful to reduce PUB/SUB message
	// processing time (as we put it into a single cache layer queue) and also opens a road to broadcast
	// tweaks – such as BroadcastDelay and delta between several publications (deltas require both
	// BroadcastDelay and KeepLatestPublication to be enabled).
	UseQueue bool
	// KeepLatestPublication enables keeping latest publication in channel cache. This is required
	// for supporting deltas when BroadcastDelay > 0. Also, this enables fast recovery after reconnect
	// in RecoveryModeCache case.
	// TODO: make sure we use cache for fast recovery in RecoveryModeCache case.
	// TODO: make sure we use cache for fast recovery in RecoveryModeStream case.
	KeepLatestPublication bool
}

// channelCache is an optional intermediary layer between broker PUB/SUB and client connections.
// It costs up to two additional goroutines depending on ChannelCacheOptions used.
//
// This layer optionally keeps latestPublication in channel (when ChannelCacheOptions.KeepLatestPublication is on)
// and optionally queues incoming publications to process them later (broadcast to active subscribers) in a separate
// goroutine (when ChannelCacheOptions.UseQueue is on). Also, it may have a goroutine for periodic position checks
// (if ChannelCacheOptions.PositionSyncInterval is set to non-zero value).
//
// When ChannelCacheOptions.PositionSyncInterval is used it periodically syncs stream position with a Broker if
// there were no new publications for a long time. If it finds that a continuity in a channel stream is
// broken it marks channel subscribers with insufficient state flag. This way Centrifuge can drastically
// reduce the number of calls to Broker for the mostly idle streams in channels with many subscribers.
//
// When ChannelCacheOptions.KeepLatestPublication is used clients can load latest stream Publication from
// memory instead of remote broker, so connect/reconnect in RecoveryModeCache case is faster and more efficient.
//
// Cache layer may also be used with RecoveryModeStream to only go to the Broker if recovery is not possible
// from the cached state. Thus making quick massive reconnect less expensive.
//
// With ChannelCacheOptions.BroadcastDelay option it can send latest publications to clients skipping intermediate
// publications. Together with ChannelCacheOptions.KeepLatestPublication cache layer can also handle delta
// updates and send deltas between several publications.
//
// Cache is dropped as soon as last subscriber leaves the channel on the node. This generally makes it possible to
// keep latest publication without TTL, but probably we still need to handle TTL to match broker behaviour. BTW it's
// possible to clean up the local cache latest publication by looking at the result from a broker in the periodic
// position sync.
//
// When using cache layer we need to make sure that all synchronizations in channel are made through the cache layer.
// Connection may join with an offset in the future – in that case we need to make sure that we don't send publications
// with lower offset to the client. This also affects using delays and deltas - the delta may be broken.
// The question is - what if client reconnects to a node where cache layer is behind another node? Client may pass
// larger offset. What should we do then? Maybe return an insufficient state error to client in that case?
type channelCache struct {
	initialized atomic.Int64
	channel     string
	node        node
	options     ChannelCacheOptions

	mu sync.Mutex

	messages    *cacheQueue
	broadcastMu sync.Mutex // When queue is not used need to protect broadcast method from concurrent execution.

	closeCh chan struct{}

	// latestPublication is an initial publication in channel or publication last sent.
	latestPublication *Publication
	// currentStreamPosition is an initial stream position or stream position lastly sent.
	currentStreamPosition StreamPosition
	// latestQueuedStreamPosition is a stream position of the latest queued publication.
	latestQueuedStreamPosition StreamPosition

	positionCheckTime int64
	nowTimeGetter     func() time.Time
	metaTTLSeconds    time.Duration // TODO: not used yet
}

type node interface {
	handlePublication(
		channel string, pub *Publication, sp StreamPosition, delta bool,
		prevPublication *Publication, bypassOffset bool,
	) error
	streamTopLatestPub(ch string, historyMetaTTL time.Duration) (*Publication, StreamPosition, error)
}

func newChannelCache(
	channel string,
	node node,
	options ChannelCacheOptions,
) (*channelCache, error) {
	c := &channelCache{
		channel: channel,
		node:    node,
		options: options,
		closeCh: make(chan struct{}),
		nowTimeGetter: func() time.Time {
			return time.Now()
		},
		positionCheckTime: time.Now().Unix(),
	}
	if options.UseQueue {
		c.messages = newCacheQueue(2)
	}
	if options.BroadcastDelay > 0 && !options.UseQueue {
		return nil, fmt.Errorf("broadcast delay can only be used with queue enabled")
	}
	return c, nil
}

type queuedPub struct {
	pub                 *Publication
	sp                  StreamPosition
	delta               bool
	prevPub             *Publication
	isInsufficientState bool
}

func (c *channelCache) initState(latestPublication *Publication, currentStreamPosition StreamPosition) {
	if c.options.KeepLatestPublication {
		c.latestPublication = latestPublication
	}
	c.currentStreamPosition = currentStreamPosition
	c.latestQueuedStreamPosition = currentStreamPosition
	if c.options.UseQueue {
		go c.writer()
	}
	if c.options.PositionSyncInterval > 0 {
		go c.runChecks()
	}
	c.initialized.Store(1)
}

func (c *channelCache) recoverLatestPublication() (*Publication, StreamPosition, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.options.KeepLatestPublication {
		return nil, StreamPosition{}, errors.New("keep latest publication option is not enabled")
	}
	return c.latestPublication.shallowCopy(), c.currentStreamPosition, nil
}

func (c *channelCache) processPublication(pub *Publication, sp StreamPosition, delta bool, prevPub *Publication) {
	if c.initialized.Load() == 0 {
		// Skip publications while cache is not initialized.
		return
	}
	bp := queuedPub{pub: pub, sp: sp, delta: delta, prevPub: prevPub}
	c.mu.Lock()
	c.latestQueuedStreamPosition = sp
	c.positionCheckTime = c.nowTimeGetter().Unix()
	c.mu.Unlock()

	if c.options.UseQueue {
		c.messages.Add(queuedItem{Publication: bp})
		// TODO: do we need to limit queue size here?
	} else {
		c.broadcastMu.Lock()
		defer c.broadcastMu.Unlock()
		c.broadcast(bp)
	}
}

func (c *channelCache) processInsufficientState(currentStreamTop StreamPosition, latestPublication *Publication) {
	bp := queuedPub{pub: latestPublication, sp: currentStreamTop, delta: false, isInsufficientState: true, prevPub: nil}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.latestQueuedStreamPosition = currentStreamTop
	c.positionCheckTime = c.nowTimeGetter().Unix()
	if c.options.UseQueue {
		// TODO: possibly support c.messages.dropQueued() for this path ?
		c.messages.Add(queuedItem{Publication: bp})
	} else {
		c.broadcastMu.Lock()
		defer c.broadcastMu.Unlock()
		c.broadcast(bp)
	}
}

func (c *channelCache) broadcast(qp queuedPub) {
	bypassOffset := c.options.BroadcastDelay > 0 && !qp.isInsufficientState
	pubToBroadcast := qp.pub
	spToBroadcast := qp.sp
	if qp.isInsufficientState {
		pubToBroadcast = &Publication{
			Offset: math.MaxUint64,
		}
		spToBroadcast.Offset = math.MaxUint64
	}

	prevPub := qp.prevPub
	if c.options.KeepLatestPublication && c.options.BroadcastDelay > 0 {
		prevPub = c.latestPublication
	}
	delta := qp.delta
	if c.options.BroadcastDelay > 0 && !c.options.KeepLatestPublication {
		delta = false
	}
	_ = c.node.handlePublication(
		c.channel, pubToBroadcast, spToBroadcast, delta, prevPub, bypassOffset)
	c.mu.Lock()
	defer c.mu.Unlock()
	if qp.sp.Offset > c.currentStreamPosition.Offset {
		c.currentStreamPosition = qp.sp
		if c.options.KeepLatestPublication {
			c.latestPublication = qp.pub
		}
	}
}

func (c *channelCache) writer() {
	for {
		if ok := c.waitSendPub(c.options.BroadcastDelay); !ok {
			return
		}
	}
}

func (c *channelCache) waitSendPub(delay time.Duration) bool {
	// Wait for message from the queue.
	ok := c.messages.Wait()
	if !ok {
		return false
	}

	if delay > 0 {
		tm := timers.AcquireTimer(delay)
		select {
		case <-tm.C:
		case <-c.closeCh:
			timers.ReleaseTimer(tm)
			return false
		}
		timers.ReleaseTimer(tm)
	}

	msg, ok := c.messages.Remove()
	if !ok {
		return !c.messages.Closed()
	}
	if delay == 0 || msg.Publication.isInsufficientState {
		c.broadcast(msg.Publication)
		return true
	}
	messageCount := c.messages.Len()
	for messageCount > 0 {
		messageCount--
		var ok bool
		msg, ok = c.messages.Remove()
		if !ok {
			if c.messages.Closed() {
				return false
			}
			break
		}
		if msg.Publication.isInsufficientState {
			break
		}
	}
	c.broadcast(msg.Publication)
	return true
}

func (c *channelCache) checkPosition() (*Publication, StreamPosition, bool) {
	nowUnix := c.nowTimeGetter().Unix()
	needCheckPosition := nowUnix-c.positionCheckTime >= int64(c.options.PositionSyncInterval.Seconds())

	if !needCheckPosition {
		return nil, StreamPosition{}, true
	}

	var historyMetaTTL time.Duration
	if c.metaTTLSeconds > 0 {
		historyMetaTTL = c.metaTTLSeconds * time.Second
	}

	latestPublication, currentStreamPosition, err := c.node.streamTopLatestPub(c.channel, historyMetaTTL)
	if err != nil {
		// Will result into position check later.
		return nil, StreamPosition{}, true
	}

	return latestPublication, currentStreamPosition, c.isValidPosition(currentStreamPosition, nowUnix)
}

func (c *channelCache) isValidPosition(streamTop StreamPosition, nowUnix int64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	position := c.latestQueuedStreamPosition
	isValidPosition := streamTop.Epoch == position.Epoch && position.Offset >= streamTop.Offset
	if isValidPosition {
		c.positionCheckTime = nowUnix
		return true
	}
	return false
}

func (c *channelCache) runChecks() {
	var syncCh <-chan time.Time
	if c.options.PositionSyncInterval > 0 {
		ticker := time.NewTicker(c.options.PositionSyncInterval)
		syncCh = ticker.C
		defer ticker.Stop()
	}
	for {
		select {
		case <-c.closeCh:
			return
		case <-syncCh:
			// Sync state with Broker.
			_, _, validPosition := c.checkPosition()
			if !validPosition {
				// One retry.
				var (
					latestPublication *Publication
					streamTop         StreamPosition
				)
				latestPublication, streamTop, validPosition = c.checkPosition()
				if !validPosition {
					c.processInsufficientState(streamTop, latestPublication)
				}
			}
		}
	}
}

func (c *channelCache) close() {
	close(c.closeCh)
}

type queuedItem struct {
	Publication queuedPub
}

// cacheQueue is an unbounded queue of queuedItem.
// The queue is goroutine safe.
// Inspired by http://blog.dubbelboer.com/2015/04/25/go-faster-queue.html (MIT)
type cacheQueue struct {
	mu      sync.RWMutex
	cond    *sync.Cond
	nodes   []queuedItem
	head    int
	tail    int
	cnt     int
	size    int
	closed  bool
	initCap int
}

// newCacheQueue returns a new queuedItem queue with initial capacity.
func newCacheQueue(initialCapacity int) *cacheQueue {
	sq := &cacheQueue{
		initCap: initialCapacity,
		nodes:   make([]queuedItem, initialCapacity),
	}
	sq.cond = sync.NewCond(&sq.mu)
	return sq
}

// Mutex must be held when calling.
func (q *cacheQueue) resize(n int) {
	nodes := make([]queuedItem, n)
	if q.head < q.tail {
		copy(nodes, q.nodes[q.head:q.tail])
	} else {
		copy(nodes, q.nodes[q.head:])
		copy(nodes[len(q.nodes)-q.head:], q.nodes[:q.tail])
	}

	q.tail = q.cnt % n
	q.head = 0
	q.nodes = nodes
}

// Add an queuedItem to the back of the queue
// will return false if the queue is closed.
// In that case the queuedItem is dropped.
func (q *cacheQueue) Add(i queuedItem) bool {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return false
	}
	if q.cnt == len(q.nodes) {
		// Also tested a growth rate of 1.5, see: http://stackoverflow.com/questions/2269063/buffer-growth-strategy
		// In Go this resulted in a higher memory usage.
		q.resize(q.cnt * 2)
	}
	q.nodes[q.tail] = i
	q.tail = (q.tail + 1) % len(q.nodes)
	if i.Publication.pub != nil {
		q.size += len(i.Publication.pub.Data)
	}
	q.cnt++
	q.cond.Signal()
	q.mu.Unlock()
	return true
}

// Close the queue and discard all entries in the queue
// all goroutines in wait() will return
func (q *cacheQueue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.closed = true
	q.cnt = 0
	q.nodes = nil
	q.size = 0
	q.cond.Broadcast()
}

// CloseRemaining will close the queue and return all entries in the queue.
// All goroutines in wait() will return.
func (q *cacheQueue) CloseRemaining() []queuedItem {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return []queuedItem{}
	}
	rem := make([]queuedItem, 0, q.cnt)
	for q.cnt > 0 {
		i := q.nodes[q.head]
		q.head = (q.head + 1) % len(q.nodes)
		q.cnt--
		rem = append(rem, i)
	}
	q.closed = true
	q.cnt = 0
	q.nodes = nil
	q.size = 0
	q.cond.Broadcast()
	return rem
}

// Closed returns true if the queue has been closed
// The call cannot guarantee that the queue hasn't been
// closed while the function returns, so only "true" has a definite meaning.
func (q *cacheQueue) Closed() bool {
	q.mu.RLock()
	c := q.closed
	q.mu.RUnlock()
	return c
}

// Wait for a message to be added.
// If there are items on the queue will return immediately.
// Will return false if the queue is closed.
// Otherwise, returns true.
func (q *cacheQueue) Wait() bool {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return false
	}
	if q.cnt != 0 {
		q.mu.Unlock()
		return true
	}
	q.cond.Wait()
	q.mu.Unlock()
	return true
}

// Remove will remove an queuedItem from the queue.
// If false is returned, it either means 1) there were no items on the queue
// or 2) the queue is closed.
func (q *cacheQueue) Remove() (queuedItem, bool) {
	q.mu.Lock()
	if q.cnt == 0 {
		q.mu.Unlock()
		return queuedItem{}, false
	}
	i := q.nodes[q.head]
	q.head = (q.head + 1) % len(q.nodes)
	q.cnt--
	if i.Publication.pub != nil {
		q.size -= len(i.Publication.pub.Data)
	}

	if n := len(q.nodes) / 2; n >= q.initCap && q.cnt <= n {
		q.resize(n)
	}

	q.mu.Unlock()
	return i, true
}

// Cap returns the capacity (without allocations)
func (q *cacheQueue) Cap() int {
	q.mu.RLock()
	c := cap(q.nodes)
	q.mu.RUnlock()
	return c
}

// Len returns the current length of the queue.
func (q *cacheQueue) Len() int {
	q.mu.RLock()
	l := q.cnt
	q.mu.RUnlock()
	return l
}

// Size returns the current size of the queue.
func (q *cacheQueue) Size() int {
	q.mu.RLock()
	s := q.size
	q.mu.RUnlock()
	return s
}
