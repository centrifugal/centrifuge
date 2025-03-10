package centrifuge

import (
	"errors"
	"io"
	"sync"
	"time"

	"github.com/centrifugal/centrifuge/internal/queue"
	"github.com/centrifugal/centrifuge/internal/timers"

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
			return c.writePublicationNoDelta(channel, pub, jsonPush, sp)
		} else {
			push := &protocol.Push{Channel: channel, Pub: pub}
			var err error
			jsonReply, err := protocol.DefaultJsonReplyEncoder.Encode(&protocol.Reply{Push: push})
			if err != nil {
				go func(c *Client) { c.Disconnect(DisconnectInappropriateProtocol) }(c)
				return err
			}
			return c.writePublicationNoDelta(channel, pub, jsonReply, sp)
		}
	} else if protoType == protocol.TypeProtobuf {
		if c.transport.Unidirectional() {
			push := &protocol.Push{Channel: channel, Pub: pub}
			var err error
			protobufPush, err := protocol.DefaultProtobufPushEncoder.Encode(push)
			if err != nil {
				return err
			}
			return c.writePublicationNoDelta(channel, pub, protobufPush, sp)
		} else {
			push := &protocol.Push{Channel: channel, Pub: pub}
			var err error
			protobufReply, err := protocol.DefaultProtobufReplyEncoder.Encode(&protocol.Reply{Push: push})
			if err != nil {
				return err
			}
			return c.writePublicationNoDelta(channel, pub, protobufReply, sp)
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
	c.eventHub.stateSnapshotHandler = h
}

// StateSnapshot allows collecting current state copy.
// Mostly useful for connection introspection from the outside.
// This API is EXPERIMENTAL and may be changed/removed.
func (c *Client) StateSnapshot() (any, error) {
	if c.eventHub.stateSnapshotHandler != nil {
		return c.eventHub.stateSnapshotHandler()
	}
	return nil, nil
}

func (c *Client) writeQueueItems(items []queue.Item) error {
	disconnect := c.messageWriter.enqueueMany(items...)
	if disconnect != nil {
		// close in goroutine to not block message broadcast.
		go func() { _ = c.close(*disconnect) }()
		return io.EOF
	}
	return nil
}

func (c *Client) getChannelWriteConfig(channel string) (ChannelBatchConfig, bool) {
	if c.node.config.GetChannelBatchConfig == nil {
		return ChannelBatchConfig{}, false
	}
	return c.node.config.GetChannelBatchConfig(channel)
}

// ChannelBatchConfig allows configuring how to write push messages to a channel.
type ChannelBatchConfig struct {
	MaxBatchSize int
	WriteDelay   time.Duration
}

// channelWriter buffers queue.Item objects and flushes them after a fixed delay
// or when a specific batch size is reached.
type channelWriter struct {
	mu           sync.Mutex
	delay        time.Duration
	maxBatchSize int
	buffer       []queue.Item
	timer        *time.Timer
	flushFn      func([]queue.Item) error
}

// newChannelWriter creates a new channelWriter with the given delay and maxBatchSize.
func newChannelWriter(delay time.Duration, maxBatchSize int, flushFn func([]queue.Item) error) *channelWriter {
	return &channelWriter{
		delay:        delay,
		maxBatchSize: maxBatchSize,
		flushFn:      flushFn,
	}
}

// Add appends an item to the buffer. It starts a delay timer if this is the first item,
// and flushes immediately if the batch size is reached.
func (b *channelWriter) Add(item queue.Item) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buffer = append(b.buffer, item)
	if b.delay > 0 && len(b.buffer) == 1 && b.timer == nil { // Start the flush timer for the first item and if no active timer.
		b.timer = timers.AcquireTimer(b.delay)
		go b.waitTimer(b.timer)
	}
	// Flush immediately if batch size is reached.
	if b.maxBatchSize > 0 && len(b.buffer) >= b.maxBatchSize {
		if b.timer != nil {
			tm := b.timer
			b.timer = nil // Set timer to nil so waitTimer knows it was cancelled.
			timers.ReleaseTimer(tm)
		}
		b.flushLocked()
	}
}

// waitTimer waits for the timer to fire, then flushes the batch.
func (b *channelWriter) waitTimer(tm *time.Timer) {
	<-tm.C // Wait for the timer to fire.
	b.mu.Lock()
	// Check if the timer is still active. If it’s not, it was already stopped and handled.
	if b.timer != tm {
		b.mu.Unlock()
		return
	}
	if len(b.buffer) > 0 { // If there are any items, flush the buffer.
		b.flushLocked()
	}
	b.timer = nil // Mark the timer as no longer active.
	b.mu.Unlock()
	timers.ReleaseTimer(tm)
}

// flushLocked flushes the current buffer. It assumes the caller holds the lock.
func (b *channelWriter) flushLocked() {
	batch := b.buffer
	b.buffer = b.buffer[:0]
	_ = b.flushFn(batch)
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
func (c *perChannelWriter) Close(flushRemaining bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, w := range c.writers {
		w.mu.Lock()
		if w.timer != nil {
			timers.ReleaseTimer(w.timer)
			w.timer = nil
		}
		if flushRemaining && len(w.buffer) > 0 { // If there are any buffered items, flush them.
			w.flushLocked()
		}
		w.buffer = nil
		w.mu.Unlock()
	}
}

// getWriter returns the channelWriter for the given channel's configuration,
// creating one if necessary.
func (c *perChannelWriter) getWriter(channel string, delay time.Duration, maxBatchSize int) *channelWriter {
	c.mu.RLock()
	w, exists := c.writers[channel]
	c.mu.RUnlock()
	if !exists {
		c.mu.Lock()
		// Double-check existence after acquiring write lock.
		w, exists = c.writers[channel]
		if !exists {
			w = newChannelWriter(delay, maxBatchSize, c.flushFn)
			c.writers[channel] = w
		}
		c.mu.Unlock()
	}
	return w
}

// Add routes an item to its configuration-specific aggregator.
func (c *perChannelWriter) Add(item queue.Item, delay time.Duration, maxBatchSize int) {
	w := c.getWriter(item.Channel, delay, maxBatchSize)
	w.Add(item)
}
