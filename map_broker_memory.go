package centrifuge

import (
	"container/heap"
	"context"
	"errors"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/centrifugal/centrifuge/internal/memstream"
	"github.com/centrifugal/centrifuge/internal/priority"
)

// MemoryMapBroker is builtin default MapBroker which allows running Centrifuge-based
// server without any external storage. All data managed inside process memory.
//
// With this MapBroker you can only run single Centrifuge node. If you need to scale
// you should consider using another MapBroker implementation instead – for example
// RedisMapBroker.
type MemoryMapBroker struct {
	node              *Node
	eventHandler      BrokerEventHandler
	mapHub            *mapHub
	closeOnce         sync.Once
	closeCh           chan struct{}
	pubLocks          map[int]*sync.Mutex
	resultCache       map[string]map[string]resultCacheEntry // ch -> idempotencyKey -> entry
	resultCacheMu     sync.RWMutex
	nextExpireCheck   int64
	resultExpireQueue priority.Queue
}

type resultCacheEntry struct {
	Position StreamPosition
	ExpireAt int64 // UnixMilli
}

var _ MapBroker = (*MemoryMapBroker)(nil)

// MemoryMapBrokerConfig is a memory map broker config.
type MemoryMapBrokerConfig struct{}

// NewMemoryMapBroker initializes MemoryMapBroker.
func NewMemoryMapBroker(n *Node, _ MemoryMapBrokerConfig) (*MemoryMapBroker, error) {
	pubLocks := make(map[int]*sync.Mutex, numPubLocks)
	for i := 0; i < numPubLocks; i++ {
		pubLocks[i] = &sync.Mutex{}
	}
	closeCh := make(chan struct{})
	mapHub := newMapHub(n, pubLocks, closeCh)
	mapHub.setChannelOptionsResolver(n.config.Map.GetMapChannelOptions)
	e := &MemoryMapBroker{
		node:        n,
		mapHub:      mapHub,
		pubLocks:    pubLocks,
		closeCh:     closeCh,
		resultCache: make(map[string]map[string]resultCacheEntry),
	}
	return e, nil
}

// RegisterEventHandler registers event handler and runs memory map broker.
func (e *MemoryMapBroker) RegisterEventHandler(h BrokerEventHandler) error {
	e.eventHandler = h
	e.mapHub.setEventHandler(h)
	go e.expireResultCache()
	e.mapHub.runCleanups()
	return nil
}

// Close shuts down the broker.
func (e *MemoryMapBroker) Close(_ context.Context) error {
	e.closeOnce.Do(func() {
		close(e.closeCh)
	})
	return nil
}

func (e *MemoryMapBroker) pubLock(ch string) *sync.Mutex {
	return e.pubLocks[index(ch, numPubLocks)]
}

func (e *MemoryMapBroker) Clear(_ context.Context, ch string, _ MapClearOptions) error {
	mu := e.pubLock(ch)
	mu.Lock()
	defer mu.Unlock()
	e.mapHub.clear(ch)
	e.clearResultCache(ch)
	return nil
}

// Subscribe is noop here.
func (e *MemoryMapBroker) Subscribe(_ ...string) error {
	return nil
}

// Unsubscribe is noop here.
func (e *MemoryMapBroker) Unsubscribe(_ ...string) error {
	return nil
}

// Publish publishes data to channel with optional key for keyed state.
func (e *MemoryMapBroker) Publish(ctx context.Context, ch string, key string, opts MapPublishOptions) (MapUpdateResult, error) {
	mu := e.pubLock(ch)
	mu.Lock()
	defer mu.Unlock()

	// Resolve and validate channel options.
	chOpts, err := ResolveAndValidateMapChannelOptions(e.node.config.Map.GetMapChannelOptions, ch)
	if err != nil {
		return MapUpdateResult{}, err
	}

	// Reject CAS and Version in ephemeral mode.
	if chOpts.Mode.IsEphemeral() {
		if opts.ExpectedPosition != nil {
			return MapUpdateResult{}, errors.New("CAS (ExpectedPosition) requires recoverable or persistent mode")
		}
		if opts.Version > 0 {
			return MapUpdateResult{}, errors.New("version-based dedup requires recoverable or persistent mode")
		}
	}

	if opts.IdempotencyKey != "" {
		if res, ok := e.getResultFromCache(ch, opts.IdempotencyKey); ok {
			return MapUpdateResult{Position: res, Suppressed: true, SuppressReason: SuppressReasonIdempotency}, nil
		}
	}

	now := time.Now().UnixMilli()

	// state publication stores full state (Data).
	statePub := &Publication{
		Data:  opts.Data,
		Info:  opts.ClientInfo,
		Tags:  opts.Tags,
		Time:  now,
		Key:   key,
		Score: opts.score,
	}

	streamPub := statePub

	var prevPub *Publication
	streamTop, prevPub, suppressReason, err := e.mapHub.add(ch, key, statePub, streamPub, chOpts, opts)
	if err != nil {
		return MapUpdateResult{}, err
	}
	if suppressReason != "" {
		result := MapUpdateResult{Position: streamTop, Suppressed: true, SuppressReason: suppressReason}
		// For CAS mismatch, include current key state for immediate retry.
		// Client uses: CurrentEntry.Offset + Position.Epoch for the next CAS attempt.
		if suppressReason == SuppressReasonPositionMismatch && prevPub != nil {
			result.CurrentEntry = &MapCurrentEntry{Offset: prevPub.Offset, Data: prevPub.Data}
		}
		return result, nil
	}

	if opts.IdempotencyKey != "" {
		resultExpireMs := int64(defaultIdempotentResultExpireSeconds) * 1000
		if opts.IdempotentResultTTL != 0 {
			resultExpireMs = opts.IdempotentResultTTL.Milliseconds()
		}
		e.saveResultToCache(ch, opts.IdempotencyKey, streamTop, resultExpireMs)
	}

	if e.eventHandler != nil {
		// Publish streamPub to subscribers.
		return MapUpdateResult{Position: streamTop}, e.eventHandler.HandlePublication(ch, streamPub, streamTop, opts.UseDelta, prevPub)
	}

	return MapUpdateResult{Position: streamTop}, nil
}

// Remove removes a key from keyed state.
func (e *MemoryMapBroker) Remove(ctx context.Context, ch string, key string, opts MapRemoveOptions) (MapUpdateResult, error) {
	mu := e.pubLock(ch)
	mu.Lock()
	defer mu.Unlock()

	// Resolve and validate channel options.
	chOpts, err := ResolveAndValidateMapChannelOptions(e.node.config.Map.GetMapChannelOptions, ch)
	if err != nil {
		return MapUpdateResult{}, err
	}

	// Reject CAS in ephemeral mode.
	if chOpts.Mode.IsEphemeral() {
		if opts.ExpectedPosition != nil {
			return MapUpdateResult{}, errors.New("CAS (ExpectedPosition) requires recoverable or persistent mode")
		}
	}

	if opts.IdempotencyKey != "" {
		if res, ok := e.getResultFromCache(ch, opts.IdempotencyKey); ok {
			return MapUpdateResult{Position: res, Suppressed: true, SuppressReason: SuppressReasonIdempotency}, nil
		}
	}

	streamTop, removePub, suppressReason, err := e.mapHub.remove(ch, key, chOpts, opts)
	if err != nil {
		return MapUpdateResult{}, err
	}

	if suppressReason != "" {
		result := MapUpdateResult{Position: streamTop, Suppressed: true, SuppressReason: suppressReason}
		if suppressReason == SuppressReasonPositionMismatch && removePub != nil {
			result.CurrentEntry = &MapCurrentEntry{Offset: removePub.Offset, Data: removePub.Data}
		}
		return result, nil
	}

	if opts.IdempotencyKey != "" {
		resultExpireMs := int64(defaultIdempotentResultExpireSeconds) * 1000
		if opts.IdempotentResultTTL != 0 {
			resultExpireMs = opts.IdempotentResultTTL.Milliseconds()
		}
		e.saveResultToCache(ch, opts.IdempotencyKey, streamTop, resultExpireMs)
	}

	if e.eventHandler != nil {
		return MapUpdateResult{Position: streamTop}, e.eventHandler.HandlePublication(ch, removePub, streamTop, false, nil)
	}

	return MapUpdateResult{Position: streamTop}, nil
}

// ReadStream retrieves publications from stream.
func (e *MemoryMapBroker) ReadStream(ctx context.Context, ch string, opts MapReadStreamOptions) (MapStreamResult, error) {
	return e.mapHub.getStream(ch, opts)
}

// ReadState retrieves keyed state with revisions.
func (e *MemoryMapBroker) ReadState(ctx context.Context, ch string, opts MapReadStateOptions) (MapStateResult, error) {
	_, err := ResolveAndValidateMapChannelOptions(e.node.config.Map.GetMapChannelOptions, ch)
	if err != nil {
		return MapStateResult{}, err
	}
	return e.mapHub.getState(ch, opts)
}

// Stats returns state statistics.
func (e *MemoryMapBroker) Stats(ctx context.Context, ch string) (MapStats, error) {
	return e.mapHub.getStats(ch)
}

func (e *MemoryMapBroker) getResultFromCache(ch string, key string) (StreamPosition, bool) {
	e.resultCacheMu.RLock()
	defer e.resultCacheMu.RUnlock()
	chCache, ok := e.resultCache[ch]
	if !ok {
		return StreamPosition{}, false
	}
	entry, ok := chCache[key]
	if !ok || entry.ExpireAt <= time.Now().UnixMilli() {
		return StreamPosition{}, false
	}
	return entry.Position, true
}

func (e *MemoryMapBroker) saveResultToCache(ch string, key string, sp StreamPosition, resultExpireMs int64) {
	e.resultCacheMu.Lock()
	defer e.resultCacheMu.Unlock()
	chCache, ok := e.resultCache[ch]
	if !ok {
		chCache = make(map[string]resultCacheEntry)
		e.resultCache[ch] = chCache
	}
	expireAt := time.Now().UnixMilli() + resultExpireMs
	chCache[key] = resultCacheEntry{Position: sp, ExpireAt: expireAt}
	cacheKey := ch + "\x00" + key
	heap.Push(&e.resultExpireQueue, &priority.Item{Value: cacheKey, Priority: expireAt})
	if e.nextExpireCheck == 0 || e.nextExpireCheck > expireAt {
		e.nextExpireCheck = expireAt
	}
}

func (e *MemoryMapBroker) clearResultCache(ch string) {
	e.resultCacheMu.Lock()
	defer e.resultCacheMu.Unlock()
	delete(e.resultCache, ch)
}

func (e *MemoryMapBroker) expireResultCache() {
	var nextExpireCheck int64
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	for {
		select {
		case <-timer.C:
		case <-e.closeCh:
			return
		}
		e.resultCacheMu.Lock()
		now := time.Now().UnixMilli()
		if e.nextExpireCheck == 0 || e.nextExpireCheck > now {
			e.resultCacheMu.Unlock()
			timer.Reset(time.Second)
			continue
		}
		nextExpireCheck = 0
		for e.resultExpireQueue.Len() > 0 {
			item := heap.Pop(&e.resultExpireQueue).(*priority.Item)
			expireAt := item.Priority
			if expireAt > now {
				heap.Push(&e.resultExpireQueue, item)
				nextExpireCheck = expireAt
				break
			}
			combined := item.Value
			if idx := strings.IndexByte(combined, '\x00'); idx >= 0 {
				ch := combined[:idx]
				key := combined[idx+1:]
				if chCache, ok := e.resultCache[ch]; ok {
					if entry, keyOk := chCache[key]; keyOk && entry.ExpireAt <= now {
						delete(chCache, key)
						if len(chCache) == 0 {
							delete(e.resultCache, ch)
						}
					}
				}
			}
		}
		// Compact heap when stale entries accumulate excessively.
		var totalCacheEntries int
		for _, chCache := range e.resultCache {
			totalCacheEntries += len(chCache)
		}
		if e.resultExpireQueue.Len() > 2*totalCacheEntries+100 {
			e.resultExpireQueue = priority.MakeQueue()
			for ch, chCache := range e.resultCache {
				for key, entry := range chCache {
					cacheKey := ch + "\x00" + key
					heap.Push(&e.resultExpireQueue, &priority.Item{Value: cacheKey, Priority: entry.ExpireAt})
				}
			}
			if e.resultExpireQueue.Len() > 0 {
				nextExpireCheck = e.resultExpireQueue[0].Priority
			}
		}
		e.nextExpireCheck = nextExpireCheck
		e.resultCacheMu.Unlock()
		timer.Reset(time.Second)
	}
}

// mapHub manages keyed state for all channels.
//
// Lock ordering (acquire in this order to prevent deadlock):
//
//	pubLock(ch)  →  mapHub.Lock/RLock  →  mapChannel.mu
//
// pubLock: serializes Publish/Remove per channel (including stream.Add and HandlePublication).
// mapHub.Lock: protects channels map, expiration queues, and channel creation/deletion.
// mapHub.RLock: concurrent reads of channel state.
// mapChannel.mu: protects sortedKeys rebuild during getState (held under mapHub.RLock).
//
// expireKeysIteration uses two phases to respect this ordering:
//
//	Phase 1: mapHub.Lock — collect expired keys, remove from state.
//	Phase 2: pubLock → mapHub.Lock — add to stream, deliver events.
type mapHub struct {
	sync.RWMutex
	node            *Node
	channels        map[string]*mapChannel
	nextExpireCheck int64
	expireQueue     priority.Queue
	expires         map[string]int64
	nextRemoveCheck int64
	removeQueue     priority.Queue
	removes         map[string]int64
	closeCh         chan struct{}
	// Key TTL tracking
	nextKeyExpireCheck     int64
	keyExpireQueue         priority.Queue                         // priority queue of {ch:key, expireAt}
	keyExpires             map[string]int64                       // "ch:key" -> expireAt
	eventHandler           BrokerEventHandler                     // for publishing removal events
	channelOptionsResolver func(channel string) MapChannelOptions // for key expiration events
	pubLocks               map[int]*sync.Mutex                    // for ordering HandlePublication calls
}

// mapChannel represents keyed state for a single channel.
type mapChannel struct {
	mu              sync.Mutex // protects sortedKeys rebuild in getState
	stream          *memstream.Stream
	state           map[string]*stateEntry // key -> entry
	ordered         bool
	scores          map[string]int64 // key -> score (for ordered state)
	sortedKeys      []string         // cached sorted keys
	sortedKeysDirty bool             // true if sortedKeys needs rebuilding
	lastSortOrdered bool             // tracks whether last sort used ordered or unordered
	lastSortAsc     bool             // tracks last sort direction for ordered state
}

type stateEntry struct {
	Key          string
	Revision     StreamPosition
	Publication  *Publication
	Score        int64  // For ordered state
	ExpireAt     int64  // Millisecond timestamp (UnixMilli) for key TTL expiration (0 = no expiration)
	Version      uint64 // Per-key version for ordering (0 = disabled)
	VersionEpoch string // Per-key version epoch
}

func newMapHub(node *Node, pubLocks map[int]*sync.Mutex, closeCh chan struct{}) *mapHub {
	return &mapHub{
		node:           node,
		channels:       make(map[string]*mapChannel),
		expireQueue:    priority.MakeQueue(),
		expires:        make(map[string]int64),
		removeQueue:    priority.MakeQueue(),
		removes:        make(map[string]int64),
		closeCh:        closeCh,
		keyExpireQueue: priority.MakeQueue(),
		keyExpires:     make(map[string]int64),
		pubLocks:       pubLocks,
	}
}

func (h *mapHub) setChannelOptionsResolver(r func(channel string) MapChannelOptions) {
	h.Lock()
	defer h.Unlock()
	h.channelOptionsResolver = r
}

func (h *mapHub) setEventHandler(handler BrokerEventHandler) {
	h.Lock()
	defer h.Unlock()
	h.eventHandler = handler
}

func (h *mapHub) runCleanups() {
	go h.expireStreams()
	go h.removeChannels()
	go h.expireKeys()
}

func (h *mapHub) expireStreams() {
	var nextExpireCheck int64
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	for {
		select {
		case <-timer.C:
		case <-h.closeCh:
			return
		}
		h.Lock()
		if h.nextExpireCheck == 0 || h.nextExpireCheck > time.Now().UnixMilli() {
			h.Unlock()
			timer.Reset(time.Second)
			continue
		}
		nextExpireCheck = 0
		for h.expireQueue.Len() > 0 {
			item := heap.Pop(&h.expireQueue).(*priority.Item)
			expireAt := item.Priority
			if expireAt > time.Now().UnixMilli() {
				heap.Push(&h.expireQueue, item)
				nextExpireCheck = expireAt
				break
			}
			ch := item.Value
			exp, ok := h.expires[ch]
			if !ok {
				continue
			}
			if exp <= expireAt {
				delete(h.expires, ch)
				if channel, ok := h.channels[ch]; ok && channel.stream != nil {
					channel.stream.Clear()
				}
			} else {
				heap.Push(&h.expireQueue, &priority.Item{Value: ch, Priority: exp})
			}
		}
		h.nextExpireCheck = nextExpireCheck
		h.Unlock()
		timer.Reset(time.Second)
	}
}

func (h *mapHub) removeChannels() {
	var nextRemoveCheck int64
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	for {
		select {
		case <-timer.C:
		case <-h.closeCh:
			return
		}
		h.Lock()
		if h.nextRemoveCheck == 0 || h.nextRemoveCheck > time.Now().UnixMilli() {
			h.Unlock()
			timer.Reset(time.Second)
			continue
		}
		nextRemoveCheck = 0
		for h.removeQueue.Len() > 0 {
			item := heap.Pop(&h.removeQueue).(*priority.Item)
			expireAt := item.Priority
			if expireAt > time.Now().UnixMilli() {
				heap.Push(&h.removeQueue, item)
				nextRemoveCheck = expireAt
				break
			}
			ch := item.Value
			exp, ok := h.removes[ch]
			if !ok {
				continue
			}
			if exp <= expireAt {
				delete(h.removes, ch)
				delete(h.channels, ch)
			} else {
				heap.Push(&h.removeQueue, &priority.Item{Value: ch, Priority: exp})
			}
		}
		h.nextRemoveCheck = nextRemoveCheck
		h.Unlock()
		timer.Reset(time.Second)
	}
}

// expiredKeyEvent holds a Phase 1 snapshot of an expired key candidate. Phase 2
// re-validates the entry under pubLock(ch) → hub lock before deleting state and
// appending the removal to the stream atomically.
type expiredKeyEvent struct {
	channel    string
	key        string
	expireAt   int64
	tags       map[string]string
	streamSize int
}

// expireKeys handles TTL-based expiration of individual state keys.
// When a key expires, it removes it from the state, updates aggregation counts,
// and publishes a removal event.
func (h *mapHub) expireKeys() {
	var nextKeyExpireCheck int64
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	for {
		select {
		case <-timer.C:
		case <-h.closeCh:
			return
		}
		h.expireKeysIteration(&nextKeyExpireCheck)
		timer.Reset(time.Second)
	}
}

func (h *mapHub) expireKeysIteration(nextKeyExpireCheck *int64) {
	// Phase 1: Under hub lock — collect expired key candidates only. State is NOT
	// mutated here. Mutating state in Phase 1 without holding pubLock(ch) would
	// expose subscribers to an inconsistent ReadState→ReadStream window where the
	// key is gone from state but the corresponding removal event is not yet on the
	// stream — they would later receive a removal for a key they never saw. Phase 2
	// acquires pubLock(ch) → hub lock per channel and atomically deletes state +
	// appends the removal to the stream + invokes the handler, matching the lock
	// ordering and atomicity of Remove() and Publish().
	var expiredEvents []expiredKeyEvent
	var eventHandler BrokerEventHandler
	var oldestExpireAt int64 // Track oldest expired timestamp for lag metric.

	h.Lock()
	if h.nextKeyExpireCheck == 0 || h.nextKeyExpireCheck > time.Now().UnixMilli() {
		h.Unlock()
		return
	}
	*nextKeyExpireCheck = 0
	now := time.Now().UnixMilli()
	eventHandler = h.eventHandler

	for h.keyExpireQueue.Len() > 0 {
		item := heap.Pop(&h.keyExpireQueue).(*priority.Item)
		expireAt := item.Priority
		if expireAt > now {
			heap.Push(&h.keyExpireQueue, item)
			*nextKeyExpireCheck = expireAt
			break
		}

		chKey := item.Value // format: "channel\x00key"
		storedExpireAt, ok := h.keyExpires[chKey]
		if !ok {
			continue
		}

		// Check if expiration time was updated (key was refreshed)
		if storedExpireAt > expireAt {
			// Re-queue with updated expiration
			heap.Push(&h.keyExpireQueue, &priority.Item{Value: chKey, Priority: storedExpireAt})
			continue
		}

		// Parse channel and key from combined string
		ch, key := h.parseChKey(chKey)
		if ch == "" || key == "" {
			delete(h.keyExpires, chKey)
			continue
		}

		channel, ok := h.channels[ch]
		if !ok {
			delete(h.keyExpires, chKey)
			continue
		}

		entry, ok := channel.state[key]
		if !ok {
			delete(h.keyExpires, chKey)
			continue
		}

		// Verify entry's expiration matches (wasn't refreshed)
		if entry.ExpireAt != expireAt {
			if entry.ExpireAt > now { // now is UnixMilli
				// Entry was refreshed, re-queue
				h.keyExpires[chKey] = entry.ExpireAt
				heap.Push(&h.keyExpireQueue, &priority.Item{Value: chKey, Priority: entry.ExpireAt})
			}
			continue
		}

		// Track the oldest expired timestamp for the lag metric.
		if oldestExpireAt == 0 || expireAt < oldestExpireAt {
			oldestExpireAt = expireAt
		}

		var streamSize int
		if h.channelOptionsResolver != nil {
			chOpts, err := ResolveAndValidateMapChannelOptions(h.channelOptionsResolver, ch)
			if err == nil && chOpts.Mode.HasStream() {
				streamSize = chOpts.StreamSize
			}
		}
		expiredEvents = append(expiredEvents, expiredKeyEvent{
			channel:    ch,
			key:        key,
			expireAt:   expireAt,
			tags:       entry.Publication.Tags,
			streamSize: streamSize,
		})
	}
	// Compact heap when stale entries exceed 2x live entries.
	// Stale entries accumulate from TTL refreshes that push new items without
	// removing old ones. Periodic compaction rebuilds the queue from h.keyExpires,
	// the source of truth for per-key deadlines. This is correct because every
	// key insertion/refresh updates h.keyExpires with the latest deadline, and
	// every key removal deletes from h.keyExpires. The queue may contain outdated
	// entries (old deadlines for refreshed keys), but they are harmlessly skipped
	// at pop time when the deadline doesn't match h.keyExpires.
	if h.keyExpireQueue.Len() > 2*len(h.keyExpires)+100 {
		h.keyExpireQueue = priority.MakeQueue()
		for chKey, exp := range h.keyExpires {
			heap.Push(&h.keyExpireQueue, &priority.Item{Value: chKey, Priority: exp})
		}
		if h.keyExpireQueue.Len() > 0 {
			*nextKeyExpireCheck = h.keyExpireQueue[0].Priority
		}
	}
	h.nextKeyExpireCheck = *nextKeyExpireCheck
	h.Unlock()

	// Report cleanup lag metric outside the lock.
	if h.node != nil && h.node.metrics != nil {
		if oldestExpireAt > 0 {
			lagSeconds := float64(now-oldestExpireAt) / 1000.0
			if lagSeconds < 0 {
				lagSeconds = 0
			}
			h.node.metrics.setMapBrokerCleanupLag("", lagSeconds)
		} else {
			h.node.metrics.setMapBrokerCleanupLag("", 0)
		}
	}

	// Phase 2: Under pubLock(ch) → hub lock — delete state, append removal stream
	// entry, and dispatch the event atomically per channel. Re-validate the entry
	// to handle refreshes or removals that landed after the Phase 1 snapshot.
	var keysRemoved int64
	for _, event := range expiredEvents {
		var mu *sync.Mutex
		if h.pubLocks != nil {
			mu = h.pubLocks[index(event.channel, numPubLocks)]
			mu.Lock()
		}

		removePub := &Publication{
			Key:     event.key,
			Removed: true,
			Time:    time.Now().UnixMilli(),
			Tags:    event.tags,
		}
		var streamPos StreamPosition
		var dispatch bool

		h.Lock()
		channel, ok := h.channels[event.channel]
		if ok {
			entry, exists := channel.state[event.key]
			chKey := h.makeChKey(event.channel, event.key)
			if exists && entry.ExpireAt == event.expireAt {
				// Still expired with the same deadline — delete state and stream-append atomically.
				delete(channel.state, event.key)
				delete(h.keyExpires, chKey)
				keysRemoved++
				channel.sortedKeysDirty = true
				if channel.ordered {
					delete(channel.scores, event.key)
				}
				if channel.stream != nil {
					if event.streamSize > 0 {
						offset, _ := channel.stream.Add(removePub, event.streamSize, 0, "")
						removePub.Offset = offset
						streamPos = StreamPosition{Offset: offset, Epoch: channel.stream.Epoch()}
					} else {
						streamPos = StreamPosition{Offset: channel.stream.Top(), Epoch: channel.stream.Epoch()}
					}
				}
				dispatch = eventHandler != nil
			} else if exists && entry.ExpireAt > now {
				// Entry was refreshed between Phase 1 and Phase 2 — re-queue.
				h.keyExpires[chKey] = entry.ExpireAt
				heap.Push(&h.keyExpireQueue, &priority.Item{Value: chKey, Priority: entry.ExpireAt})
				if h.nextKeyExpireCheck == 0 || entry.ExpireAt < h.nextKeyExpireCheck {
					h.nextKeyExpireCheck = entry.ExpireAt
				}
			}
		}
		h.Unlock()

		if dispatch {
			err := eventHandler.HandlePublication(event.channel, removePub, streamPos, false, nil)
			if err != nil && h.node != nil {
				h.node.logger.log(newErrorLogEntry(err, "error handling expired key publication", map[string]any{"channel": event.channel, "key": event.key}))
				if h.node.metrics != nil {
					h.node.metrics.incMapBrokerCleanupErrors("")
				}
			}
		}
		if mu != nil {
			mu.Unlock()
		}
	}

	if h.node != nil && h.node.metrics != nil && keysRemoved > 0 {
		h.node.metrics.addMapBrokerCleanupKeysRemoved("", keysRemoved)
	}
}

// makeChKey creates a combined channel:key string for the expiration map.
func (h *mapHub) makeChKey(ch, key string) string {
	return ch + "\x00" + key
}

// parseChKey splits a combined channel:key string.
func (h *mapHub) parseChKey(chKey string) (string, string) {
	for i := 0; i < len(chKey); i++ {
		if chKey[i] == '\x00' {
			return chKey[:i], chKey[i+1:]
		}
	}
	return "", ""
}

func (h *mapHub) add(ch string, key string, statePub *Publication, streamPub *Publication, chOpts MapChannelOptions, opts MapPublishOptions) (StreamPosition, *Publication, SuppressReason, error) {
	h.Lock()
	defer h.Unlock()

	var prevPub *Publication
	if opts.UseDelta && key != "" {
		// Get previous publication for delta (key-based: same key's previous state).
		if channel, ok := h.channels[ch]; ok {
			if entry, ok := channel.state[key]; ok {
				prevPub = entry.Publication
			}
		}
	}

	channel, ok := h.channels[ch]
	if !ok {
		channel = &mapChannel{
			stream:  memstream.New(),
			state:   make(map[string]*stateEntry),
			ordered: chOpts.ordered,
			scores:  make(map[string]int64),
		}
		h.channels[ch] = channel
	} else if chOpts.ordered && !channel.ordered {
		channel.ordered = true
		channel.sortedKeysDirty = true
	}

	// Canonical check order across brokers: Version → KeyMode → CAS.
	// Dedup checks (version) drop duplicates first; constraint checks
	// (KeyMode, CAS) report meaningful intent failures last.
	// Version is gated by HasStream — streamless channels skip dedup.
	if chOpts.Mode.HasStream() && key != "" && opts.Version > 0 {
		if existing, ok := channel.state[key]; ok {
			if (opts.VersionEpoch == "" || opts.VersionEpoch == existing.VersionEpoch) &&
				opts.Version <= existing.Version {
				var pos StreamPosition
				if channel.stream != nil {
					pos = StreamPosition{Offset: channel.stream.Top(), Epoch: channel.stream.Epoch()}
				}
				return pos, nil, SuppressReasonVersion, nil
			}
		}
	}

	// Check KeyMode condition before proceeding
	if key != "" && opts.KeyMode != KeyModeReplace {
		existingEntry, keyExists := channel.state[key]
		if opts.KeyMode == KeyModeIfNew && keyExists {
			// KeyModeIfNew but key already exists - suppress publish
			// But optionally refresh TTL if RefreshTTLOnSuppress is set
			if opts.RefreshTTLOnSuppress && chOpts.KeyTTL > 0 {
				expireAt := time.Now().UnixMilli() + chOpts.KeyTTL.Milliseconds()
				existingEntry.ExpireAt = expireAt
				// Update TTL tracking
				chKey := h.makeChKey(ch, key)
				heap.Push(&h.keyExpireQueue, &priority.Item{Value: chKey, Priority: expireAt})
				h.keyExpires[chKey] = expireAt
				if h.nextKeyExpireCheck == 0 || h.nextKeyExpireCheck > expireAt {
					h.nextKeyExpireCheck = expireAt
				}
				// Keepalive must extend MetaTTL too — without this the channel
				// can be garbage-collected by removeChannels even while keys are
				// being refreshed, forcing an epoch reset on the next publish.
				if chOpts.MetaTTL > 0 {
					removeAt := time.Now().UnixMilli() + chOpts.MetaTTL.Milliseconds()
					if _, ok := h.removes[ch]; !ok {
						heap.Push(&h.removeQueue, &priority.Item{Value: ch, Priority: removeAt})
					}
					h.removes[ch] = removeAt
					if h.nextRemoveCheck == 0 || h.nextRemoveCheck > removeAt {
						h.nextRemoveCheck = removeAt
					}
				}
			}
			var pos StreamPosition
			if channel.stream != nil {
				pos = StreamPosition{Offset: channel.stream.Top(), Epoch: channel.stream.Epoch()}
			}
			return pos, nil, SuppressReasonKeyExists, nil
		}
		if opts.KeyMode == KeyModeIfExists && !keyExists {
			// KeyModeIfExists but key doesn't exist - skip
			var pos StreamPosition
			if channel.stream != nil {
				pos = StreamPosition{Offset: channel.stream.Top(), Epoch: channel.stream.Epoch()}
			}
			return pos, nil, SuppressReasonKeyNotFound, nil
		}
	}

	// CAS check: verify expected position (offset + epoch)
	if key != "" && opts.ExpectedPosition != nil {
		existing, exists := channel.state[key]
		var pos StreamPosition
		if channel.stream != nil {
			pos = StreamPosition{Offset: channel.stream.Top(), Epoch: channel.stream.Epoch()}
		}
		if !exists {
			// Key doesn't exist - position mismatch
			return pos, nil, SuppressReasonPositionMismatch, nil
		}
		// Check both offset AND epoch
		if existing.Publication.Offset != opts.ExpectedPosition.Offset ||
			pos.Epoch != opts.ExpectedPosition.Epoch {
			// Return current publication for immediate retry.
			// Client uses: CurrentEntry.Offset + Position.Epoch for the next CAS attempt.
			return pos, existing.Publication, SuppressReasonPositionMismatch, nil
		}
	}

	var streamPosition StreamPosition

	// Handle stream
	if chOpts.Mode.HasStream() {
		expireAt := time.Now().UnixMilli() + chOpts.StreamTTL.Milliseconds()
		if _, ok := h.expires[ch]; !ok {
			heap.Push(&h.expireQueue, &priority.Item{Value: ch, Priority: expireAt})
		}
		h.expires[ch] = expireAt
		if h.nextExpireCheck == 0 || h.nextExpireCheck > expireAt {
			h.nextExpireCheck = expireAt
		}

		if chOpts.MetaTTL > 0 {
			removeAt := time.Now().UnixMilli() + chOpts.MetaTTL.Milliseconds()
			if _, ok := h.removes[ch]; !ok {
				heap.Push(&h.removeQueue, &priority.Item{Value: ch, Priority: removeAt})
			}
			h.removes[ch] = removeAt
			if h.nextRemoveCheck == 0 || h.nextRemoveCheck > removeAt {
				h.nextRemoveCheck = removeAt
			}
		}

		offset, _ := channel.stream.Add(streamPub, chOpts.StreamSize, 0, "")
		streamPub.Offset = offset // Set offset on publication for delivery
		streamPosition = StreamPosition{
			Offset: offset,
			Epoch:  channel.stream.Epoch(),
		}
	} else {
		// No stream, just use current position
		if channel.stream != nil {
			streamPosition = StreamPosition{
				Offset: channel.stream.Top(),
				Epoch:  channel.stream.Epoch(),
			}
		}
	}

	// Handle keyed state.
	if key != "" {
		// Calculate expiration time (milliseconds for sub-second TTL precision).
		var expireAt int64
		if chOpts.KeyTTL > 0 {
			expireAt = time.Now().UnixMilli() + chOpts.KeyTTL.Milliseconds()
		}

		// Store statePub in state (contains full state Data).
		// Preserve stored version when caller publishes without one (matches Redis).
		// Overwriting with 0 would erase dedup protection against late-arriving
		// older versions from a concurrent producer.
		version := opts.Version
		versionEpoch := opts.VersionEpoch
		if version == 0 {
			if existing, ok := channel.state[key]; ok {
				version = existing.Version
				versionEpoch = existing.VersionEpoch
			}
		}
		statePub.Offset = streamPosition.Offset
		entry := &stateEntry{
			Key:          key,
			Revision:     streamPosition,
			Publication:  statePub,
			Score:        opts.score,
			ExpireAt:     expireAt,
			Version:      version,
			VersionEpoch: versionEpoch,
		}

		channel.state[key] = entry

		// Mark sorted keys as dirty for any state change
		channel.sortedKeysDirty = true
		if chOpts.ordered {
			channel.scores[key] = opts.score
		}

		// Handle key TTL expiration tracking
		if chOpts.KeyTTL > 0 {
			chKey := h.makeChKey(ch, key)
			// Always push new heap entry. When a key is refreshed, the old heap entry
			// becomes stale and will be discarded in expireKeysIteration (which checks
			// storedExpireAt > poppedExpireAt). Pushing unconditionally ensures the heap
			// has an entry with the correct (latest) expiration time.
			heap.Push(&h.keyExpireQueue, &priority.Item{Value: chKey, Priority: expireAt})
			h.keyExpires[chKey] = expireAt
			if h.nextKeyExpireCheck == 0 || h.nextKeyExpireCheck > expireAt {
				h.nextKeyExpireCheck = expireAt
			}
		}
	}

	return streamPosition, prevPub, SuppressReasonNone, nil
}

func (h *mapHub) remove(ch string, key string, chOpts MapChannelOptions, opts MapRemoveOptions) (StreamPosition, *Publication, SuppressReason, error) {
	h.Lock()
	defer h.Unlock()

	channel, ok := h.channels[ch]
	if !ok {
		// Channel doesn't exist — no position to report. All implementations
		// (Redis, Postgres) return empty position here since no meta exists.
		return StreamPosition{}, nil, SuppressReasonKeyNotFound, nil
	}

	var removeTags map[string]string
	entry, keyExists := channel.state[key]
	if !keyExists {
		// Key doesn't exist, nothing to remove
		var streamPosition StreamPosition
		if channel.stream != nil {
			streamPosition = StreamPosition{
				Offset: channel.stream.Top(),
				Epoch:  channel.stream.Epoch(),
			}
		}
		return streamPosition, nil, SuppressReasonKeyNotFound, nil
	}

	// CAS check: verify expected position (offset + epoch) before removal
	if opts.ExpectedPosition != nil {
		var pos StreamPosition
		if channel.stream != nil {
			pos = StreamPosition{Offset: channel.stream.Top(), Epoch: channel.stream.Epoch()}
		}
		if entry.Publication.Offset != opts.ExpectedPosition.Offset ||
			pos.Epoch != opts.ExpectedPosition.Epoch {
			return pos, entry.Publication, SuppressReasonPositionMismatch, nil
		}
	}

	// Capture tags before deletion for the removal publication.
	removeTags = entry.Publication.Tags

	// Remove from state
	delete(channel.state, key)
	channel.sortedKeysDirty = true // Mark dirty for any removal
	if channel.ordered {
		delete(channel.scores, key)
	}

	// Clean up key expiration tracking
	chKey := h.makeChKey(ch, key)
	delete(h.keyExpires, chKey)

	if opts.Tags != nil {
		removeTags = opts.Tags
	}

	// Create removal publication (reused for both stream and eventHandler).
	removePub := &Publication{
		Key:     key,
		Removed: true,
		Time:    time.Now().UnixMilli(),
		Tags:    removeTags,
	}

	var streamPosition StreamPosition

	// Add to stream if converging mode
	if chOpts.Mode.HasStream() {
		expireAt := time.Now().UnixMilli() + chOpts.StreamTTL.Milliseconds()
		if _, ok := h.expires[ch]; !ok {
			heap.Push(&h.expireQueue, &priority.Item{Value: ch, Priority: expireAt})
		}
		h.expires[ch] = expireAt
		if h.nextExpireCheck == 0 || h.nextExpireCheck > expireAt {
			h.nextExpireCheck = expireAt
		}

		// Refresh MetaTTL so the channel isn't garbage-collected while active.
		if chOpts.MetaTTL > 0 {
			removeAt := time.Now().UnixMilli() + chOpts.MetaTTL.Milliseconds()
			if _, ok := h.removes[ch]; !ok {
				heap.Push(&h.removeQueue, &priority.Item{Value: ch, Priority: removeAt})
			}
			h.removes[ch] = removeAt
			if h.nextRemoveCheck == 0 || h.nextRemoveCheck > removeAt {
				h.nextRemoveCheck = removeAt
			}
		}

		offset, _ := channel.stream.Add(removePub, chOpts.StreamSize, 0, "")
		removePub.Offset = offset
		streamPosition = StreamPosition{
			Offset: offset,
			Epoch:  channel.stream.Epoch(),
		}
	} else if channel.stream != nil {
		streamPosition = StreamPosition{
			Offset: channel.stream.Top(),
			Epoch:  channel.stream.Epoch(),
		}
	}

	return streamPosition, removePub, SuppressReasonNone, nil
}

func (h *mapHub) clear(ch string) {
	h.Lock()
	defer h.Unlock()

	channel, ok := h.channels[ch]
	if !ok {
		return
	}

	// Clean up key expiration tracking for all keys in the channel.
	for key := range channel.state {
		chKey := h.makeChKey(ch, key)
		delete(h.keyExpires, chKey)
	}

	// Remove channel and associated tracking entries.
	delete(h.channels, ch)
	delete(h.expires, ch)
	delete(h.removes, ch)
}

func (h *mapHub) getStream(ch string, opts MapReadStreamOptions) (MapStreamResult, error) {
	// Resolve MetaTTL from channel config.
	var metaTTL time.Duration
	if h.channelOptionsResolver != nil {
		chOpts, err := ResolveAndValidateMapChannelOptions(h.channelOptionsResolver, ch)
		if err == nil {
			metaTTL = chOpts.MetaTTL
		}
	}

	if metaTTL > 0 {
		h.Lock()
		h.updateMetaTTL(ch, metaTTL)
		h.Unlock()
	}

	h.RLock()

	channel, ok := h.channels[ch]
	if !ok {
		h.RUnlock()
		// Channel not found — need write lock to create stream position.
		h.Lock()
		if _, exists := h.channels[ch]; !exists {
			// Still not found under write lock — create and return.
			streamPos := h.createStreamPosition(ch)
			h.Unlock()
			return MapStreamResult{Position: streamPos}, nil
		}
		// Channel was created by another goroutine between RUnlock and Lock.
		// Release write lock and re-acquire read lock for the read path.
		h.Unlock()
		h.RLock()
		channel, ok = h.channels[ch]
		if !ok {
			// Deleted in the tiny window between Unlock and RLock.
			h.RUnlock()
			h.Lock()
			streamPos := h.createStreamPosition(ch)
			h.Unlock()
			return MapStreamResult{Position: streamPos}, nil
		}
	}

	filter := opts.Filter

	stream := channel.stream
	if stream == nil {
		h.RUnlock()
		h.Lock()
		streamPos := h.createStreamPosition(ch)
		h.Unlock()
		return MapStreamResult{Position: streamPos}, nil
	}

	streamPosition := StreamPosition{
		Offset: stream.Top(),
		Epoch:  stream.Epoch(),
	}

	if filter.Since == nil {
		if filter.Limit == 0 {
			h.RUnlock()
			return MapStreamResult{Position: streamPosition}, nil
		}
		items, _, err := stream.Get(0, false, filter.Limit, filter.Reverse)
		if err != nil {
			h.RUnlock()
			return MapStreamResult{}, err
		}
		pubs := make([]*Publication, len(items))
		for i, item := range items {
			pubs[i] = item.Value.(*Publication)
		}
		h.RUnlock()
		return MapStreamResult{Publications: pubs, Position: streamPosition}, nil
	}

	since := filter.Since

	// Validate epoch if provided.
	if since.Epoch != "" && since.Epoch != stream.Epoch() {
		h.RUnlock()
		return MapStreamResult{}, ErrorUnrecoverablePosition
	}

	if !filter.Reverse {
		if streamPosition.Offset == since.Offset {
			h.RUnlock()
			return MapStreamResult{Position: streamPosition}, nil
		}
	}

	streamOffset := since.Offset + 1
	if filter.Reverse {
		streamOffset = since.Offset - 1
	}

	items, _, err := stream.Get(streamOffset, true, filter.Limit, filter.Reverse)
	if err != nil {
		h.RUnlock()
		return MapStreamResult{}, err
	}

	pubs := make([]*Publication, len(items))
	for i, item := range items {
		pubs[i] = item.Value.(*Publication)
	}
	h.RUnlock()
	return MapStreamResult{Publications: pubs, Position: streamPosition}, nil
}

func (h *mapHub) getState(ch string, opts MapReadStateOptions) (MapStateResult, error) {
	// Resolve MetaTTL from channel config.
	var metaTTL time.Duration
	if h.channelOptionsResolver != nil {
		chOpts, err := ResolveAndValidateMapChannelOptions(h.channelOptionsResolver, ch)
		if err == nil {
			metaTTL = chOpts.MetaTTL
		}
	}
	if metaTTL > 0 {
		h.Lock()
		h.updateMetaTTL(ch, metaTTL)
		h.Unlock()
	}

	// Acquire read lock to find the channel. If not found, upgrade to write lock
	// to create stream position (establishing the epoch for consistency).
	h.RLock()
	channel, ok := h.channels[ch]
	if !ok {
		h.RUnlock()
		// Channel not found — need write lock to create stream position.
		h.Lock()
		if _, exists := h.channels[ch]; !exists {
			// Still not found under write lock — create and return.
			if opts.Revision != nil && opts.Revision.Epoch != "" {
				pos := h.createStreamPosition(ch)
				h.Unlock()
				return MapStateResult{Position: pos}, ErrorUnrecoverablePosition
			}
			pos := h.createStreamPosition(ch)
			h.Unlock()
			return MapStateResult{Position: pos}, nil
		}
		// Channel was created by another goroutine between RUnlock and Lock.
		// Release write lock and re-acquire read lock for the read path.
		h.Unlock()
		h.RLock()
		channel, ok = h.channels[ch]
		if !ok {
			// Deleted in the tiny window between Unlock and RLock.
			h.RUnlock()
			h.Lock()
			streamPos := h.createStreamPosition(ch)
			h.Unlock()
			return MapStateResult{Position: streamPos}, nil
		}
	}

	// Channel found — hold RLock for reads, channel.mu for sorted keys.
	channel.mu.Lock()
	defer channel.mu.Unlock()
	defer h.RUnlock()

	var streamPosition StreamPosition
	if channel.stream != nil {
		streamPosition = StreamPosition{
			Offset: channel.stream.Top(),
			Epoch:  channel.stream.Epoch(),
		}
	}

	// Check if client requested specific state revision
	if opts.Revision != nil {
		if streamPosition.Epoch != opts.Revision.Epoch {
			return MapStateResult{Position: streamPosition}, ErrorUnrecoverablePosition
		}
	}

	// Handle single key lookup (Key filter) — takes priority over Limit.
	if opts.Key != "" {
		entry, exists := channel.state[opts.Key]
		if !exists {
			return MapStateResult{Position: streamPosition}, nil
		}
		return MapStateResult{Publications: []*Publication{entry.Publication}, Position: streamPosition}, nil
	}

	// Limit=0: return only stream position (no entries).
	if opts.Limit == 0 {
		return MapStateResult{Position: streamPosition}, nil
	}

	var pubs []*Publication

	// Rebuild sorted keys if dirty or sort order/direction changed since last call.
	wantOrdered := channel.ordered
	wantAsc := opts.Asc
	if channel.sortedKeysDirty || len(channel.sortedKeys) != len(channel.state) ||
		channel.lastSortOrdered != wantOrdered || (wantOrdered && channel.lastSortAsc != wantAsc) {
		channel.sortedKeys = make([]string, 0, len(channel.state))
		for key := range channel.state {
			channel.sortedKeys = append(channel.sortedKeys, key)
		}

		if wantOrdered {
			sort.Slice(channel.sortedKeys, func(i, j int) bool {
				si := channel.scores[channel.sortedKeys[i]]
				sj := channel.scores[channel.sortedKeys[j]]
				if si != sj {
					if wantAsc {
						return si < sj
					}
					return si > sj
				}
				if wantAsc {
					return channel.sortedKeys[i] < channel.sortedKeys[j]
				}
				return channel.sortedKeys[i] > channel.sortedKeys[j]
			})
		} else {
			sort.Strings(channel.sortedKeys)
		}
		channel.sortedKeysDirty = false
		channel.lastSortOrdered = wantOrdered
		channel.lastSortAsc = wantAsc
	}

	totalKeys := len(channel.sortedKeys)
	if totalKeys == 0 {
		return MapStateResult{Position: streamPosition}, nil
	}

	// Key-based cursor pagination for continuity during concurrent modifications.
	// Instead of integer offset (which shifts when entries are added/removed),
	// we use the last seen key as cursor. This ensures:
	// - Entries that move across cursor boundary are modified (new offset) -> in stream
	// - No entries are permanently skipped
	// - Duplicates are filtered by offset > streamPos or deduped by key

	var startIdx int
	if opts.Cursor != "" {
		if channel.ordered {
			startIdx = findOrderedCursorPosition(channel.sortedKeys, channel.scores, opts.Cursor, opts.Asc)
		} else {
			// Unordered cursor: just the key (lexicographic)
			startIdx = findUnorderedCursorPosition(channel.sortedKeys, opts.Cursor)
		}
	}

	if startIdx >= totalKeys {
		return MapStateResult{Position: streamPosition}, nil
	}

	endIdx := totalKeys
	cursor := ""

	if opts.Limit > 0 {
		endIdx = startIdx + opts.Limit
		if endIdx > totalKeys {
			endIdx = totalKeys
		}

		// Set cursor to last key in this page (for next page)
		if endIdx < totalKeys {
			lastKey := channel.sortedKeys[endIdx-1]
			if channel.ordered {
				// ordered cursor: "score\x00key"
				lastScore := channel.scores[lastKey]
				cursor = MakeOrderedCursor(strconv.FormatInt(lastScore, 10), lastKey)
			} else {
				// Unordered cursor: just the key
				cursor = lastKey
			}
		}
	}

	// Build only the pubs we need (avoids allocating full slice then slicing)
	pubs = make([]*Publication, endIdx-startIdx)
	for i := startIdx; i < endIdx; i++ {
		key := channel.sortedKeys[i]
		entry := channel.state[key]
		// Return the stored Publication pointer directly - it already has Key and Offset set
		pubs[i-startIdx] = entry.Publication
	}

	return MapStateResult{Publications: pubs, Position: streamPosition, Cursor: cursor}, nil
}

func (h *mapHub) getStats(ch string) (MapStats, error) {
	h.RLock()
	defer h.RUnlock()

	channel, ok := h.channels[ch]
	if !ok {
		return MapStats{}, nil
	}

	return MapStats{
		NumKeys: len(channel.state),
	}, nil
}

// createStreamPosition initializes a channel with an empty stream if it doesn't exist.
// This is intentionally called from read paths (ReadState, ReadStream) because we need
// a stable epoch for the channel — clients use it to detect position invalidation.
// Without this, a channel created lazily on the first Publish could have its epoch change
// between a ReadState and the subsequent ReadStream, breaking recovery.
func (h *mapHub) createStreamPosition(ch string) StreamPosition {
	channel, ok := h.channels[ch]
	if !ok {
		stream := memstream.New()
		h.channels[ch] = &mapChannel{
			stream: stream,
			state:  make(map[string]*stateEntry),
			scores: make(map[string]int64),
		}
		return StreamPosition{
			Offset: 0,
			Epoch:  stream.Epoch(),
		}
	}
	if channel.stream == nil {
		channel.stream = memstream.New()
	}
	return StreamPosition{
		Offset: channel.stream.Top(),
		Epoch:  channel.stream.Epoch(),
	}
}

func (h *mapHub) updateMetaTTL(ch string, metaTTL time.Duration) {
	removeAt := time.Now().UnixMilli() + metaTTL.Milliseconds()
	if _, ok := h.removes[ch]; !ok {
		heap.Push(&h.removeQueue, &priority.Item{Value: ch, Priority: removeAt})
	}
	h.removes[ch] = removeAt
	if h.nextRemoveCheck == 0 || h.nextRemoveCheck > removeAt {
		h.nextRemoveCheck = removeAt
	}
}
