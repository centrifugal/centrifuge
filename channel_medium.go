package centrifuge

import (
	"errors"
	"math"
	"sync"
	"time"

	"github.com/centrifugal/centrifuge/internal/queue"
	"github.com/centrifugal/centrifuge/internal/timers"
)

// ChannelMediumOptions is an EXPERIMENTAL way to enable using a channel medium layer in Centrifuge.
// Note, channel medium layer is very unstable at the moment – do not use it in production!
// Channel medium layer is an optional per-channel intermediary between Broker PUB/SUB and Client
// connections. This intermediary layer may be used for various per-channel tweaks and optimizations.
// Channel medium comes with memory overhead depending on ChannelMediumOptions. At the same time, it
// can provide significant benefits in terms of overall system efficiency and flexibility.
type ChannelMediumOptions struct {
	// KeepLatestPublication enables keeping latest publication which was broadcasted to channel subscribers on
	// this Node in the channel medium layer. This is helpful for supporting deltas in at most once scenario.
	KeepLatestPublication bool

	// SharedPositionSync when true delegates connection position checks to the channel medium. In that case
	// check is only performed no more often than once in Config.ClientChannelPositionCheckDelay thus reducing
	// the load on broker in cases when channel has many subscribers. When message loss is detected medium layer
	// tells caller about this and also marks all channel subscribers with insufficient state flag. By default,
	// medium is not used for sync – in that case each individual connection syncs position independently.
	SharedPositionSync bool

	// EnableQueue for incoming publications. This can be useful to reduce PUB/SUB message processing time
	// (as we put it into a single medium layer queue instead of each individual connection queue), reduce
	// channel broadcast contention (when one channel waits for broadcast of another channel to finish),
	// and also opens a road for broadcast tweaks – such as BroadcastDelay and delta between several
	// publications (deltas require both BroadcastDelay and KeepLatestPublication to be enabled). This costs
	// additional goroutine.
	enableQueue bool
	// QueueMaxSize is a maximum size of the queue used in channel medium (in bytes). If zero, 16MB default
	// is used. If max size reached, new publications will be dropped.
	queueMaxSize int

	// BroadcastDelay controls the delay before Publication broadcast. On time tick Centrifugo broadcasts
	// only the latest publication in the channel if any. Useful to reduce/smooth the number of messages sent
	// to clients when publication contains the entire state. If zero, all publications will be sent to clients
	// without delay logic involved on channel medium level. BroadcastDelay option requires (!) EnableQueue to be
	// enabled, as we can not afford delays during broadcast from the PUB/SUB layer. BroadcastDelay must not be
	// used in channels with positioning/recovery on since it skips publications.
	broadcastDelay time.Duration
}

func (o ChannelMediumOptions) isMediumEnabled() bool {
	return o.SharedPositionSync || o.KeepLatestPublication || o.enableQueue || o.broadcastDelay > 0
}


// channelMedium is initialized when first subscriber comes into channel, and dropped as soon as last
// subscriber leaves the channel on the Node.
type channelMedium struct {
	channel string
	node    nodeSubset
	options ChannelMediumOptions
	isMap   bool

	mu      sync.RWMutex
	closeCh chan struct{}
	// optional queue for publications.
	messages *queue.Queue[queuedPublication]
	// We must synchronize broadcast method between general publications and insufficient state notifications.
	// Only used when queue is disabled.
	broadcastMu sync.Mutex
	// latestPublication is a publication last sent to connections on this Node.
	latestPublication *Publication
	// positionCheckTime is a time (Unix Nanoseconds) when last position check was performed.
	positionCheckTime int64
	// nowFn is the clock used by this medium. Defaults to time.Now in
	// newChannelMedium; tests override it on the medium instance instead of
	// mutating a package-level variable (which races with parallel readers).
	nowFn func() time.Time
}

type nodeSubset interface {
	handlePublication(ch string, sp StreamPosition, pub, prevPub *Publication, localPrevPub *Publication) error
	streamTop(ch string, historyMetaTTL time.Duration) (StreamPosition, error)
	mapStreamTop(ch string) (StreamPosition, error)
}

func newChannelMedium(channel string, node nodeSubset, options ChannelMediumOptions) (*channelMedium, error) {
	if options.broadcastDelay > 0 && !options.enableQueue {
		return nil, errors.New("broadcast delay can only be used with queue enabled")
	}
	c := &channelMedium{
		channel: channel,
		node:    node,
		options: options,
		closeCh: make(chan struct{}),
		nowFn:   time.Now,
	}
	c.positionCheckTime = c.nowFn().UnixNano()
	if options.enableQueue {
		c.messages = queue.New(2, queuedPublicationSize)
		go c.writer()
	}
	return c, nil
}

type queuedPub struct {
	pub                 *Publication
	sp                  StreamPosition
	prevPub             *Publication
	delta               bool
	isInsufficientState bool
}

const defaultChannelLayerQueueMaxSize = 16 * 1024 * 1024

func (c *channelMedium) broadcastPublication(pub *Publication, sp StreamPosition, delta bool, prevPub *Publication) {
	bp := queuedPub{pub: pub, sp: sp, prevPub: prevPub, delta: delta}
	c.mu.Lock()
	c.positionCheckTime = c.nowFn().UnixNano()
	c.mu.Unlock()

	if c.options.enableQueue {
		queueMaxSize := defaultChannelLayerQueueMaxSize
		if c.options.queueMaxSize > 0 {
			queueMaxSize = c.options.queueMaxSize
		}
		if c.messages.Size() > queueMaxSize {
			return
		}
		c.messages.Add(queuedPublication{Publication: bp})
	} else {
		c.broadcastMu.Lock()
		defer c.broadcastMu.Unlock()
		c.broadcast(bp)
	}
}

func (c *channelMedium) broadcastInsufficientState() {
	bp := queuedPub{prevPub: nil, isInsufficientState: true}
	c.mu.Lock()
	c.positionCheckTime = c.nowFn().UnixNano()
	c.mu.Unlock()
	if c.options.enableQueue {
		// TODO: possibly support c.messages.dropQueued() for this path ?
		c.messages.Add(queuedPublication{Publication: bp})
	} else {
		c.broadcastMu.Lock()
		defer c.broadcastMu.Unlock()
		c.broadcast(bp)
	}
}

func (c *channelMedium) broadcast(qp queuedPub) {
	pubToBroadcast := qp.pub
	spToBroadcast := qp.sp
	if qp.isInsufficientState {
		// using math.MaxUint64 as a special offset to trigger insufficient state.
		pubToBroadcast = &Publication{Offset: math.MaxUint64}
		spToBroadcast.Offset = math.MaxUint64
	}

	prevPub := qp.prevPub
	var localPrevPub *Publication
	useLocalLatestPub := c.options.KeepLatestPublication && !qp.isInsufficientState
	if useLocalLatestPub && qp.delta && qp.pub.Key == "" {
		// Only provide localPrevPub for non-map publications. For map subs,
		// keys are independent streams — a single latestPublication can't serve
		// as a correct delta base across different keys. Map subs rely on the
		// broker-level prevPub (positioned path) for delta instead.
		localPrevPub = c.latestPublication
	}
	if c.options.broadcastDelay > 0 && !c.options.KeepLatestPublication {
		prevPub = nil
	}
	if qp.isInsufficientState {
		prevPub = nil
	}
	_ = c.node.handlePublication(c.channel, spToBroadcast, pubToBroadcast, prevPub, localPrevPub)
	if useLocalLatestPub && qp.pub.Key == "" {
		c.latestPublication = qp.pub
	}
}

func (c *channelMedium) writer() {
	for {
		if ok := c.waitSendPub(c.options.broadcastDelay); !ok {
			return
		}
	}
}

func (c *channelMedium) waitSendPub(delay time.Duration) bool {
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

func (c *channelMedium) CheckPosition(historyMetaTTL time.Duration, clientPosition StreamPosition, checkDelay time.Duration) bool {
	nowUnixNano := c.nowFn().UnixNano()
	c.mu.Lock()
	needCheckPosition := nowUnixNano-c.positionCheckTime >= checkDelay.Nanoseconds()
	if needCheckPosition {
		c.positionCheckTime = nowUnixNano
	}
	c.mu.Unlock()
	if !needCheckPosition {
		return true
	}
	_, validPosition, err := c.checkPositionWithRetry(historyMetaTTL, clientPosition)
	if err != nil {
		// Position will be checked again later.
		return true
	}
	if !validPosition {
		c.broadcastInsufficientState()
	}
	return validPosition
}

func (c *channelMedium) checkPositionWithRetry(historyMetaTTL time.Duration, clientPosition StreamPosition) (StreamPosition, bool, error) {
	sp, validPosition, err := c.checkPositionOnce(historyMetaTTL, clientPosition)
	if err != nil || !validPosition {
		return c.checkPositionOnce(historyMetaTTL, clientPosition)
	}
	return sp, validPosition, err
}

func (c *channelMedium) checkPositionOnce(historyMetaTTL time.Duration, clientPosition StreamPosition) (StreamPosition, bool, error) {
	var streamTop StreamPosition
	var err error
	if c.isMap {
		streamTop, err = c.node.mapStreamTop(c.channel)
	} else {
		streamTop, err = c.node.streamTop(c.channel, historyMetaTTL)
	}
	if err != nil {
		return StreamPosition{}, false, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	isValidPosition := streamTop.Epoch == clientPosition.Epoch && clientPosition.Offset == streamTop.Offset
	return streamTop, isValidPosition, nil
}

func (c *channelMedium) close() {
	close(c.closeCh)
	// Unblock the writer goroutine. queue.Queue.Wait parks on its own
	// internal closeCh + notify chan and is woken only by an Add
	// (cnt > 0) or by the queue's Close. channelMedium's own closeCh
	// is checked only inside the broadcastDelay timer branch, which is
	// unreachable until Wait returns — so without closing the queue
	// the writer goroutine sits forever on an empty queue and leaks
	// for every channelMedium that ever existed.
	if c.messages != nil {
		c.messages.Close()
	}
}

type queuedPublication struct {
	Publication queuedPub
}

// queuedPublicationSize is the byte-size accounting used by the
// channelMedium's publication queue. Empty publications (e.g. the
// insufficient-state marker) contribute zero.
func queuedPublicationSize(qp queuedPublication) int {
	if qp.Publication.pub == nil {
		return 0
	}
	return len(qp.Publication.pub.Data)
}
