package centrifuge

import (
	"github.com/centrifugal/centrifuge/internal/timers"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

type ChannelCacheOptions struct {
	// Delay broadcasting. In this case intermediate publications may be skipped. May be used to
	// reduce number of messages sent to clients. If zero, then all publications will be sent to clients.
	Delay time.Duration
	// KeepLatestPublication enables keeping latest publication in channel cache. This is required
	// for using deltas when delay > 0. Also, this enables fast recovery after reconnect.
	KeepLatestPublication bool
	// SyncInterval is a time interval to check if we need to sync state with Broker.
	// By default, no sync will be done. In this case each individual connection will
	// sync separately.
	SyncInterval time.Duration
}

// channelCache is an optional intermediary layer between broker and client connections.
// It may periodically sync its state with a Broker if there were no new publications for a long time.
// If it finds that a continuity in a channel stream is broken it marks channel subscribers with
// insufficient state flag.
// It may be used by clients to check proper stream position in channel thus drastically reduce
// load on Broker.
// It may keep last publication – and with Delay option only send latest publication to clients skipping
// intermediate publications.
// It may also handle delta updates and send deltas (between several publications if Delay is used).
type channelCache struct {
	initialized atomic.Int64
	channel     string
	node        node
	options     ChannelCacheOptions

	mu       sync.Mutex
	messages *cacheQueue

	closeCh chan struct{}

	// latestPublication is an initial publication in channel or publication last sent.
	latestPublication *Publication
	// currentStreamPosition is an initial stream position or stream position lastly sent.
	currentStreamPosition StreamPosition

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
	History(ch string, opts ...HistoryOption) (HistoryResult, error)
}

func newChannelCache(
	channel string,
	node node,
	options ChannelCacheOptions,
) *channelCache {
	c := &channelCache{
		channel:  channel,
		node:     node,
		options:  options,
		closeCh:  make(chan struct{}),
		messages: newCacheQueue(2),
		nowTimeGetter: func() time.Time {
			return time.Now()
		},
		positionCheckTime: time.Now().Unix(),
	}
	return c
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
	go c.runChecks()
	go c.writer()
	c.initialized.Store(1)
}

func (c *channelCache) handlePublication(pub *Publication, sp StreamPosition, delta bool, prevPub *Publication) {
	if c.initialized.Load() == 0 {
		// Skip publications while cache is not initialized.
		return
	}
	bp := queuedPub{pub: pub, sp: sp, delta: delta, prevPub: prevPub}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.latestQueuedStreamPosition = sp
	c.positionCheckTime = c.nowTimeGetter().Unix()
	c.messages.Add(queuedItem{Publication: bp})
}

func (c *channelCache) handleInsufficientState(currentStreamTop StreamPosition, latestPublication *Publication) {
	bp := queuedPub{pub: latestPublication, sp: currentStreamTop, delta: false, isInsufficientState: true, prevPub: nil}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.latestQueuedStreamPosition = currentStreamTop
	c.positionCheckTime = c.nowTimeGetter().Unix()
	// TODO: possibly c.messages.dropQueued() ?
	c.messages.Add(queuedItem{Publication: bp})
}

func (c *channelCache) broadcast(qp queuedPub) {
	bypassOffset := c.options.Delay > 0 && !qp.isInsufficientState
	pubToBroadcast := qp.pub
	if qp.isInsufficientState {
		pubToBroadcast = &Publication{
			Offset: math.MaxUint64,
		}
	}
	prevPub := qp.prevPub
	if c.options.KeepLatestPublication && c.options.Delay > 0 {
		prevPub = c.latestPublication
	}
	delta := qp.delta
	if c.options.Delay > 0 && !c.options.KeepLatestPublication {
		delta = false
	}
	_ = c.node.handlePublication(
		c.channel, pubToBroadcast, qp.sp, delta, prevPub, bypassOffset)
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
		if ok := c.waitSendPub(c.options.Delay); !ok {
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
	needCheckPosition := nowUnix-c.positionCheckTime >= int64(c.options.SyncInterval.Seconds())

	if !needCheckPosition {
		return nil, StreamPosition{}, true
	}

	var historyMetaTTL time.Duration
	if c.metaTTLSeconds > 0 {
		historyMetaTTL = c.metaTTLSeconds * time.Second
	}

	hr, err := c.node.History(c.channel, WithHistoryFilter(HistoryFilter{
		Limit:   1,
		Reverse: true,
	}), WithHistoryMetaTTL(historyMetaTTL))

	currentStreamPosition := hr.StreamPosition
	var latestPublication *Publication
	if len(hr.Publications) > 0 {
		latestPublication = hr.Publications[0]
	}
	if err != nil {
		// Check later.
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
	if c.options.SyncInterval > 0 {
		ticker := time.NewTicker(c.options.SyncInterval)
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
					c.handleInsufficientState(streamTop, latestPublication)
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

// WriteMany mutex must be held when calling
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
