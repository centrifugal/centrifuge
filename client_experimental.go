package centrifuge

import (
	"errors"
	"io"
	"sync"
	"time"

	"github.com/centrifugal/centrifuge/internal/queue"

	"github.com/centrifugal/protocol"
)

var errNoSubscription = errors.New("no subscription to a channel")

// WritePublication allows sending publications to Client subscription directly
// without HUB and Broker semantics. The possible use case is to turn subscription
// to a channel into an individual data stream.
// This API is EXPERIMENTAL and may be changed/removed.
func (c *Client) WritePublication(channel string, publication *Publication, sp StreamPosition) error {
	if !c.IsSubscribed(channel) {
		return errNoSubscription
	}

	pub := pubToProto(publication)
	protoType := c.transport.Protocol().toProto()

	if protoType == protocol.TypeJSON {
		if c.transport.Unidirectional() {
			push := &protocol.Push{Channel: channel, Pub: pub}
			var err error
			jsonPush, err := protocol.DefaultJsonPushEncoder.Encode(push)
			if err != nil {
				go func(c *Client) { c.Disconnect(DisconnectInappropriateProtocol) }(c)
				return err
			}
			return c.writePublicationNoDelta(channel, pub, jsonPush, sp, c.node.getBatchConfig(channel))
		} else {
			push := &protocol.Push{Channel: channel, Pub: pub}
			var err error
			jsonReply, err := protocol.DefaultJsonReplyEncoder.Encode(&protocol.Reply{Push: push})
			if err != nil {
				go func(c *Client) { c.Disconnect(DisconnectInappropriateProtocol) }(c)
				return err
			}
			return c.writePublicationNoDelta(channel, pub, jsonReply, sp, c.node.getBatchConfig(channel))
		}
	} else if protoType == protocol.TypeProtobuf {
		if c.transport.Unidirectional() {
			push := &protocol.Push{Channel: channel, Pub: pub}
			var err error
			protobufPush, err := protocol.DefaultProtobufPushEncoder.Encode(push)
			if err != nil {
				return err
			}
			return c.writePublicationNoDelta(channel, pub, protobufPush, sp, c.node.getBatchConfig(channel))
		} else {
			push := &protocol.Push{Channel: channel, Pub: pub}
			var err error
			protobufReply, err := protocol.DefaultProtobufReplyEncoder.Encode(&protocol.Reply{Push: push})
			if err != nil {
				return err
			}
			return c.writePublicationNoDelta(channel, pub, protobufReply, sp, c.node.getBatchConfig(channel))
		}
	}

	return errors.New("unknown protocol type")
}

// AcquireStorage returns an attached connection storage (a map) and a function to be
// called when the application finished working with the storage map. Be accurate when
// using this API – avoid acquiring storage for a long time - i.e. on the time of IO operations.
// Do the work fast and release with the updated map. The API designed this way to allow
// reading, modifying or fully overriding storage map and avoid making deep copies each time.
// Note, that if storage map has not been initialized yet - i.e. if it's nil - then it will
// be initialized to an empty map and then returned – so you never receive nil map when
// acquiring. The purpose of this map is to simplify handling user-defined state during the
// lifetime of connection. Try to keep this map reasonably small.
// This API is EXPERIMENTAL and may be changed/removed.
func (c *Client) AcquireStorage() (map[string]any, func(map[string]any)) {
	c.storageMu.Lock()
	if c.storage == nil {
		c.storage = map[string]any{}
	}
	return c.storage, func(updatedStorage map[string]any) {
		c.storage = updatedStorage
		c.storageMu.Unlock()
	}
}

// OnStateSnapshot allows settings StateSnapshotHandler.
// This API is EXPERIMENTAL and may be changed/removed.
func (c *Client) OnStateSnapshot(h StateSnapshotHandler) {
	c.mu.Lock()
	c.eventHub.stateSnapshotHandler = h
	c.mu.Unlock()
}

// StateSnapshot allows collecting current state copy.
// Mostly useful for connection introspection from the outside.
// This API is EXPERIMENTAL and may be changed/removed.
func (c *Client) StateSnapshot() (any, error) {
	// May be called from another goroutine while ConnectHandler sets the handler.
	// The handler is called without holding c.mu as it may use Client methods.
	c.mu.RLock()
	stateSnapshotHandler := c.eventHub.stateSnapshotHandler
	c.mu.RUnlock()
	if stateSnapshotHandler != nil {
		return stateSnapshotHandler()
	}
	return nil, nil
}

func (c *Client) writeQueueItems(items []queue.Item) error {
	disconnect := c.messageWriter.enqueueMany(items...)
	if disconnect != nil {
		// close in goroutine to not block message broadcast.
		c.spawnCloseUnlessClosing(*disconnect)
		return io.EOF
	}
	return nil
}

// ChannelBatchConfig allows configuring how to write push messages to a channel
// during broadcasts (applied for publication, join and leave pushes).
// This API is EXPERIMENTAL and may be changed/removed.
// If MaxSize is set to 0 then no batching by size will be performed.
// If MaxDelay is set to 0 then no batching by time will be performed.
// If both MaxSize and MaxDelay are set to 0 then no batching will be performed.
type ChannelBatchConfig struct {
	// MaxSize is the maximum number of messages to batch before flushing.
	MaxSize int64
	// MaxDelay is the maximum time to wait before flushing.
	MaxDelay time.Duration
	// FlushLatestPublication if true, then Centrifuge flushes only the latest publication
	// in the batch upon reaching the MaxSize or MaxDelay. Skipping on this level does
	// not work with delta compression.
	FlushLatestPublication bool
}

// channelWriter buffers queue.Item objects of one channel for one connection
// and flushes them after a delay or when a specific batch size is reached.
type channelWriter struct {
	mu      sync.Mutex
	channel string
	buffer  []queue.Item
	// inWindow is true while the writer waits in a batch window of its
	// channel, which flushes it when the delay has passed.
	inWindow   bool
	flushFn    func([]queue.Item) error
	latestOnly bool
	// latestPubs tracks the latest publication per key for FlushLatestPublication mode.
	// Items are ordered by last-update time so that offsets are emitted in ascending order.
	// For non-map publications (Key=""), all collapse into a single entry under "".
	latestPubs []queue.Item
}

// newChannelWriter creates a new channelWriter with the given flush callback.
func newChannelWriter(flushFn func([]queue.Item) error) *channelWriter {
	return &channelWriter{flushFn: flushFn}
}

// close optionally flushes remaining items and drops the rest. A batch window
// the writer waits in may still flush it later: there is nothing left to
// flush then.
func (w *channelWriter) close(flushRemaining bool) {
	w.mu.Lock()
	if flushRemaining && (len(w.buffer) > 0 || len(w.latestPubs) > 0) {
		w.flushLocked()
	}
	w.buffer = nil
	w.latestPubs = nil
	w.mu.Unlock()
}

// flushWindow flushes the writer when the batch window it waited in has
// passed its delay.
func (w *channelWriter) flushWindow() {
	w.mu.Lock()
	w.inWindow = false
	if len(w.buffer) > 0 || len(w.latestPubs) > 0 {
		w.flushLocked()
	}
	w.mu.Unlock()
}

// Add appends an item to the buffer or records it as the latest publication per key.
// When FlushLatestPublication is enabled, publications are coalesced by key — only the
// latest publication for each key is kept. For non-map publications (Key=""), all collapse
// into a single entry. Items are ordered by last-update time so offsets stay ascending.
// It flushes immediately if the batch size is reached, otherwise the writer joins the
// batch window of its channel, which flushes it once the delay has passed.
func (w *channelWriter) Add(item queue.Item, config ChannelBatchConfig) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.latestOnly = config.FlushLatestPublication

	if config.FlushLatestPublication && item.FrameType == protocol.FrameTypePushPublication {
		// Remove existing entry with the same key (if any) to maintain offset order.
		for i, existing := range w.latestPubs {
			if existing.Key == item.Key {
				w.latestPubs = append(w.latestPubs[:i], w.latestPubs[i+1:]...)
				break
			}
		}
		// Append to the end — latest update has the highest offset.
		w.latestPubs = append(w.latestPubs, item)
	} else {
		w.buffer = append(w.buffer, item)
	}

	// Total items count includes all latest pubs.
	totalCount := len(w.buffer) + len(w.latestPubs)

	// Flush immediately if batch size is reached.
	if config.MaxSize > 0 && int64(totalCount) >= config.MaxSize {
		w.flushLocked()
		return
	}

	// Wait for the delay in the batch window of the channel. A writer which
	// flushed for size stays in its window, and what it collects next is
	// flushed with the window, before the delay has passed for it.
	if config.MaxDelay > 0 && !w.inWindow {
		w.inWindow = true
		channelBatchWindows.join(w.channel, config.MaxDelay, w)
	}
}

// flushLocked flushes the current batch. Caller must hold the lock.
func (w *channelWriter) flushLocked() {
	if len(w.buffer) == 0 && len(w.latestPubs) == 0 {
		return
	}

	var batch []queue.Item
	if w.latestOnly && len(w.latestPubs) > 0 {
		// Emit non-publication items first, then per-key latest publications
		// in last-updated order (ascending offsets).
		batch = append(batch, w.buffer...)
		batch = append(batch, w.latestPubs...)
	} else {
		batch = w.buffer
	}

	w.buffer = w.buffer[:0]
	w.latestPubs = w.latestPubs[:0]
	_ = w.flushFn(batch)
}

// perChannelWriter groups items by configuration (batch size and delay).
type perChannelWriter struct {
	mu      sync.RWMutex
	writers map[string]*channelWriter
	flushFn func([]queue.Item) error
}

// newPerChannelWriter creates a new channel writer.
func newPerChannelWriter(flushFn func([]queue.Item) error) *perChannelWriter {
	return &perChannelWriter{
		writers: make(map[string]*channelWriter),
		flushFn: flushFn,
	}
}

// Close cancels all active timers in each channelWriter and discards any pending items.
func (pcw *perChannelWriter) Close(flushRemaining bool) {
	pcw.mu.Lock()
	defer pcw.mu.Unlock()
	for _, w := range pcw.writers {
		w.close(flushRemaining)
	}
}

// getWriter returns the channelWriter for the given channel's configuration,
// creating one if necessary.
func (pcw *perChannelWriter) getWriter(channel string) *channelWriter {
	pcw.mu.RLock()
	w, exists := pcw.writers[channel]
	pcw.mu.RUnlock()
	if !exists {
		pcw.mu.Lock()
		// Double-check existence after acquiring write lock.
		w, exists = pcw.writers[channel]
		if !exists {
			w = newChannelWriter(pcw.flushFn)
			w.channel = channel
			pcw.writers[channel] = w
		}
		pcw.mu.Unlock()
	}
	return w
}

func (pcw *perChannelWriter) delWriter(channel string, flushRemaining bool) {
	pcw.mu.Lock()
	w, exists := pcw.writers[channel]
	if exists {
		w.close(flushRemaining)
		delete(pcw.writers, channel)
	}
	pcw.mu.Unlock()
}

// Add routes an item to its configuration-specific aggregator.
func (pcw *perChannelWriter) Add(item queue.Item, ch string, config ChannelBatchConfig) {
	w := pcw.getWriter(ch)
	w.Add(item, config)
}

// batchWindows holds the open batch windows of channels. The writers of all
// connections subscribed to a channel which wait for the same delay wait in
// one window, flushed by one timer, rather than each arming a timer of its
// own: a publication into a channel with many subscribers would otherwise
// start a timer and a goroutine per subscriber.
//
// It is shared by all nodes in the process: a window is identified by the
// channel and the delay, so writers only share a window when they wait for
// the same delay.
type batchWindows struct {
	shards [batchWindowShards]batchWindowShard
}

const batchWindowShards = 64

type batchWindowShard struct {
	mu      sync.Mutex
	windows map[batchWindowKey]*batchWindow
}

type batchWindowKey struct {
	channel string
	delay   time.Duration
}

type batchWindow struct {
	writers []*channelWriter
}

var channelBatchWindows = &batchWindows{}

// join adds the writer to the open window of its channel and delay, opening
// one when there is none. The window flushes its writers once the delay has
// passed since it opened, so a writer waits no longer than the delay.
func (b *batchWindows) join(channel string, delay time.Duration, w *channelWriter) {
	key := batchWindowKey{channel: channel, delay: delay}
	shard := &b.shards[index(channel, batchWindowShards)]
	shard.mu.Lock()
	if shard.windows == nil {
		shard.windows = make(map[batchWindowKey]*batchWindow)
	}
	window, ok := shard.windows[key]
	if !ok {
		window = &batchWindow{}
		shard.windows[key] = window
		time.AfterFunc(delay, func() { shard.flush(key, window) })
	}
	window.writers = append(window.writers, w)
	shard.mu.Unlock()
}

// flush closes the window, so writers which collect items from now on open a
// new one, and flushes the writers which waited in it. The shard lock is not
// held while writers flush: a writer joins holding its own lock.
func (s *batchWindowShard) flush(key batchWindowKey, window *batchWindow) {
	s.mu.Lock()
	delete(s.windows, key)
	writers := window.writers
	window.writers = nil
	s.mu.Unlock()
	for _, w := range writers {
		w.flushWindow()
	}
}

// TimerCanceler is the interface returned from ScheduleTimer which allows the task to be cancelled.
// EXPERIMENTAL API.
type TimerCanceler interface {
	// Cancel the timer.
	Cancel()
}

// TimerScheduler is the interface for scheduling timers.
//
// Callbacks may block: some client periodic operations perform network calls
// (subscription refresh via RefreshHandler, and — for schedulers that run
// callbacks on a goroutine shared between connections — anything the
// application does in AliveHandler). An implementation that runs several
// connections' callbacks on one goroutine therefore lets a single slow
// connection delay the others' pings, so it should bound how many callbacks
// share a goroutine.
//
// Centrifuge does not rely on this for presence updates specifically: when a
// TimerScheduler is set it runs the presence tick on its own goroutine, since
// that tick calls into PresenceManager/MapBroker on every subscribed channel.
//
// EXPERIMENTAL API.
type TimerScheduler interface {
	// ScheduleTimer adds a callback for later execution. The TimerCanceler is returned.
	ScheduleTimer(duration time.Duration, callback func()) TimerCanceler
}
