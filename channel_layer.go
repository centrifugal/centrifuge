package centrifuge

import (
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/centrifugal/centrifuge/internal/timers"
)

// ChannelLayerOptions is an EXPERIMENTAL way to enable using a channel layer in Centrifuge.
// Note, channel layer is very unstable at the moment – do not use it in production!
// Channel layer is an optional per-channel intermediary between Broker PUB/SUB and Client connections.
// This intermediary layer may be used for various per-channel tweaks and optimizations. Channel layer
// comes with memory overhead depending on ChannelLayerOptions, and may consume one additional goroutine
// per channel if ChannelLayerOptions.EnableQueue is used. At the same time it can provide significant
// benefits in terms of overall system efficiency and flexibility.
type ChannelLayerOptions struct {
	// EnableQueue for incoming publications. This can be useful to reduce PUB/SUB message processing time
	// (as we put it into a single cache layer queue instead of each individual connection queue), reduce
	// channel broadcast contention (when one channel waits for broadcast of another channel to finish),
	// and also opens a road to broadcast tweaks – such as BroadcastDelay and delta between several
	// publications (deltas require both BroadcastDelay and KeepLatestPublication to be enabled).
	EnableQueue bool
	// QueueMaxSize is a maximum size of the queue used in channel cache (in bytes). If zero, 16MB default
	// is used. If max size reached, new publications will be dropped.
	QueueMaxSize int

	// BroadcastDelay controls the delay before Publication broadcast. On time tick Centrifugo broadcasts
	// only the latest publication in the channel if any. Useful to reduce/smooth the number of messages sent
	// to clients when publication contains the entire state. If zero, all publications will be sent to clients
	// without delay logic involved on channel cache level. BroadcastDelay option requires (!) EnableQueue to be
	// enabled, as we can not afford delays during broadcast from the PUB/SUB layer. BroadcastDelay must not be
	// used in channels with positioning/recovery on.
	BroadcastDelay time.Duration

	// KeepLatestPublication enables keeping latest publication in channel cache layer. This is required
	// for supporting deltas when BroadcastDelay > 0.
	// Probably it may be used for fast recovery also, but need to consider edge cases for races.
	KeepLatestPublication bool

	// EnablePositionSync when true delegates connection position checks to the channel cache. In that case check
	// is only performed no more often than once in Config.ClientChannelPositionCheckDelay thus reducing the load
	// on broker in cases when channel has many subscribers. When message loss is detected cache layer tells caller
	// about this and also marks all channel subscribers with insufficient state flag. By default, cache is not used
	// for sync – in that case each individual connection syncs position independently.
	EnablePositionSync bool
}

// Keep global to not allocate per-channel. Must be only changed by tests.
var channelLayerTimeNow = time.Now

// channelLayer is initialized when first subscriber comes into channel, and dropped as soon as last
// subscriber leaves the channel on the Node.
type channelLayer struct {
	channel string
	node    node
	options ChannelLayerOptions

	mu      sync.RWMutex
	closeCh chan struct{}
	// optional queue for publications.
	messages    *cacheQueue
	broadcastMu sync.Mutex // When queue is not used need to protect broadcast method from concurrent execution.
	// latestPublication is an initial publication in channel or publication last sent to connections.
	latestPublication *Publication
	// latestStreamPosition is an initial stream position or stream position lastly sent.
	latestStreamPosition StreamPosition
	// latestQueuedStreamPosition is a stream position of the latest queued publication.
	latestQueuedStreamPosition StreamPosition
	// positionCheckTime is a time (Unix Nanoseconds) when last position check was performed.
	positionCheckTime int64
}

type node interface {
	handlePublication(channel string, pub *Publication, sp StreamPosition, delta bool, prevPublication *Publication) error
	streamTopLatestPub(ch string, historyMetaTTL time.Duration) (*Publication, StreamPosition, error)
}

func newChannelInterlayer(
	channel string,
	node node,
	options ChannelLayerOptions,
) (*channelLayer, error) {
	c := &channelLayer{
		channel:           channel,
		node:              node,
		options:           options,
		closeCh:           make(chan struct{}),
		positionCheckTime: channelLayerTimeNow().UnixNano(),
	}
	if options.EnableQueue {
		c.messages = newCacheQueue(2)
	}
	if options.BroadcastDelay > 0 && !options.EnableQueue {
		return nil, fmt.Errorf("broadcast delay can only be used with queue enabled")
	}
	if c.options.EnableQueue {
		go c.writer()
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

const defaultChannelLayerQueueMaxSize = 16 * 1024 * 1024

func (c *channelLayer) broadcastPublication(pub *Publication, sp StreamPosition, delta bool, prevPub *Publication) {
	bp := queuedPub{pub: pub, sp: sp, delta: delta, prevPub: prevPub}
	c.mu.Lock()
	c.latestQueuedStreamPosition = sp
	c.positionCheckTime = channelLayerTimeNow().UnixNano()
	c.mu.Unlock()

	if c.options.EnableQueue {
		queueMaxSize := defaultChannelLayerQueueMaxSize
		if c.options.QueueMaxSize > 0 {
			queueMaxSize = c.options.QueueMaxSize
		}
		if c.messages.Size() > queueMaxSize {
			return
		}
		c.messages.Add(queuedItem{Publication: bp})
	} else {
		c.broadcastMu.Lock()
		defer c.broadcastMu.Unlock()
		c.broadcast(bp)
	}
}

func (c *channelLayer) broadcastInsufficientState(currentStreamTop StreamPosition, latestPublication *Publication) {
	bp := queuedPub{pub: latestPublication, sp: currentStreamTop, delta: false, isInsufficientState: true, prevPub: nil}
	c.mu.Lock()
	c.latestQueuedStreamPosition = currentStreamTop
	c.positionCheckTime = channelLayerTimeNow().UnixNano()
	c.mu.Unlock()
	if c.options.EnableQueue {
		// TODO: possibly support c.messages.dropQueued() for this path ?
		c.messages.Add(queuedItem{Publication: bp})
	} else {
		c.broadcastMu.Lock()
		defer c.broadcastMu.Unlock()
		c.broadcast(bp)
	}
}

func (c *channelLayer) broadcast(qp queuedPub) {
	pubToBroadcast := qp.pub
	spToBroadcast := qp.sp
	if qp.isInsufficientState {
		pubToBroadcast = &Publication{
			Offset: math.MaxUint64,
		}
		spToBroadcast.Offset = math.MaxUint64
	}

	prevPub := qp.prevPub
	if qp.delta && c.options.KeepLatestPublication {
		prevPub = c.latestPublication
	}
	delta := qp.delta
	if c.options.BroadcastDelay > 0 && !c.options.KeepLatestPublication {
		delta = false
	}
	if qp.isInsufficientState {
		delta = false
		prevPub = nil
	}
	_ = c.node.handlePublication(
		c.channel, pubToBroadcast, spToBroadcast, delta, prevPub)
	c.mu.Lock()
	defer c.mu.Unlock()
	if qp.sp.Offset > c.latestStreamPosition.Offset {
		c.latestStreamPosition = qp.sp
	}
	if c.options.KeepLatestPublication {
		c.latestPublication = qp.pub
	}
}

func (c *channelLayer) writer() {
	for {
		if ok := c.waitSendPub(c.options.BroadcastDelay); !ok {
			return
		}
	}
}

func (c *channelLayer) waitSendPub(delay time.Duration) bool {
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

func (c *channelLayer) CheckPosition(historyMetaTTL time.Duration, clientPosition StreamPosition, checkDelay time.Duration) bool {
	nowUnixNano := channelLayerTimeNow().UnixNano()
	c.mu.Lock()
	needCheckPosition := nowUnixNano-c.positionCheckTime >= checkDelay.Nanoseconds()
	if needCheckPosition {
		c.positionCheckTime = nowUnixNano
	}
	c.mu.Unlock()
	if !needCheckPosition {
		return true
	}
	latestPublication, streamTop, validPosition, err := c.checkPositionWithRetry(historyMetaTTL, clientPosition)
	if err != nil {
		// Will be checked later.
		return true
	}
	if !validPosition {
		c.broadcastInsufficientState(streamTop, latestPublication)
	}
	return validPosition
}

func (c *channelLayer) checkPositionWithRetry(historyMetaTTL time.Duration, clientPosition StreamPosition) (*Publication, StreamPosition, bool, error) {
	latestPub, sp, validPosition, err := c.checkPositionOnce(historyMetaTTL, clientPosition)
	if err != nil || !validPosition {
		return c.checkPositionOnce(historyMetaTTL, clientPosition)
	}
	return latestPub, sp, validPosition, err
}

func (c *channelLayer) checkPositionOnce(historyMetaTTL time.Duration, clientPosition StreamPosition) (*Publication, StreamPosition, bool, error) {
	latestPublication, streamTop, err := c.node.streamTopLatestPub(c.channel, historyMetaTTL)
	if err != nil {
		return nil, StreamPosition{}, false, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	position := c.latestQueuedStreamPosition
	if position.Offset == 0 {
		position = clientPosition
	}
	isValidPosition := streamTop.Epoch == position.Epoch && position.Offset == streamTop.Offset
	return latestPublication, streamTop, isValidPosition, nil
}

func (c *channelLayer) close() {
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
