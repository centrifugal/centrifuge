package centrifuge

import (
	"container/heap"
	"context"
	"errors"
	"sort"
	"strconv"
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
	resultCache       map[string]StreamPosition
	resultCacheMu     sync.RWMutex
	nextExpireCheck   int64
	resultExpireQueue priority.Queue
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
	mapHub := newMapHub(n, n.config.HistoryMetaTTL, closeCh)
	mapHub.setChannelOptionsResolver(n.ResolveMapChannelOptions)
	e := &MemoryMapBroker{
		node:        n,
		mapHub:      mapHub,
		pubLocks:    pubLocks,
		closeCh:     closeCh,
		resultCache: map[string]StreamPosition{},
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
func (e *MemoryMapBroker) Subscribe(_ string) error {
	return nil
}

// Unsubscribe is noop here.
func (e *MemoryMapBroker) Unsubscribe(_ string) error {
	return nil
}

// Publish publishes data to channel with optional key for keyed state.
func (e *MemoryMapBroker) Publish(ctx context.Context, ch string, key string, opts MapPublishOptions) (MapPublishResult, error) {
	mu := e.pubLock(ch)
	mu.Lock()
	defer mu.Unlock()

	// Apply channel options defaults from node config.
	chOpts := applyChannelOptionsDefaults(MapChannelOptions{
		StreamSize: opts.StreamSize, StreamTTL: opts.StreamTTL, MetaTTL: opts.MetaTTL, KeyTTL: opts.KeyTTL,
	}, e.node.ResolveMapChannelOptions, ch)
	opts.StreamSize, opts.StreamTTL, opts.MetaTTL, opts.KeyTTL = chOpts.StreamSize, chOpts.StreamTTL, chOpts.MetaTTL, chOpts.KeyTTL

	// Reject CAS and Version in streamless mode.
	if opts.StreamSize <= 0 || opts.StreamTTL <= 0 {
		if opts.ExpectedPosition != nil {
			return MapPublishResult{}, errors.New("CAS (ExpectedPosition) requires stream (StreamSize > 0)")
		}
		if opts.Version > 0 {
			return MapPublishResult{}, errors.New("version-based dedup requires stream (StreamSize > 0)")
		}
	}

	if opts.IdempotencyKey != "" {
		if res, ok := e.getResultFromCache(ch, opts.IdempotencyKey); ok {
			return MapPublishResult{Position: res, Suppressed: true, SuppressReason: SuppressReasonIdempotency}, nil
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
		Score: opts.Score,
	}

	// Stream publication may have different data (StreamData) for incremental updates.
	var streamPub *Publication
	if len(opts.StreamData) > 0 {
		streamPub = &Publication{
			Data:  opts.StreamData,
			Info:  opts.ClientInfo,
			Tags:  opts.Tags,
			Time:  now,
			Key:   key,
			Score: opts.Score,
		}
	} else {
		streamPub = statePub
	}

	ordered := e.node.ResolveMapChannelOptions(ch).Ordered

	var prevPub *Publication
	streamTop, prevPub, suppressReason, err := e.mapHub.add(ch, key, statePub, streamPub, ordered, opts)
	if err != nil {
		return MapPublishResult{}, err
	}
	if suppressReason != "" {
		result := MapPublishResult{Position: streamTop, Suppressed: true, SuppressReason: suppressReason}
		// For CAS mismatch, include current publication for immediate retry.
		// Client uses: CurrentPublication.Offset + Position.Epoch for next CAS attempt.
		if suppressReason == SuppressReasonPositionMismatch {
			result.CurrentPublication = prevPub
		}
		return result, nil
	}

	statePub.Offset = streamTop.Offset
	streamPub.Offset = streamTop.Offset

	if opts.IdempotencyKey != "" {
		resultExpireSeconds := int64(defaultIdempotentResultExpireSeconds)
		if opts.IdempotentResultTTL != 0 {
			resultExpireSeconds = int64(opts.IdempotentResultTTL.Seconds())
		}
		e.saveResultToCache(ch, opts.IdempotencyKey, streamTop, resultExpireSeconds)
	}

	if e.eventHandler != nil {
		// Publish streamPub (with StreamData if set) to subscribers.
		err = e.eventHandler.HandlePublication(ch, streamPub, streamTop, opts.UseDelta, prevPub)
		if err != nil {
			e.node.logger.log(newErrorLogEntry(err, "error handling publication in channel", map[string]any{"channel": ch}))
		}
	}

	return MapPublishResult{Position: streamTop}, nil
}

// Remove removes a key from keyed state.
func (e *MemoryMapBroker) Remove(ctx context.Context, ch string, key string, opts MapRemoveOptions) (MapPublishResult, error) {
	mu := e.pubLock(ch)
	mu.Lock()
	defer mu.Unlock()

	// Apply channel options defaults from node config.
	chOpts := applyChannelOptionsDefaults(MapChannelOptions{
		StreamSize: opts.StreamSize, StreamTTL: opts.StreamTTL, MetaTTL: opts.MetaTTL,
	}, e.node.ResolveMapChannelOptions, ch)
	opts.StreamSize, opts.StreamTTL, opts.MetaTTL = chOpts.StreamSize, chOpts.StreamTTL, chOpts.MetaTTL

	// Reject CAS in streamless mode.
	if opts.StreamSize <= 0 || opts.StreamTTL <= 0 {
		if opts.ExpectedPosition != nil {
			return MapPublishResult{}, errors.New("CAS (ExpectedPosition) requires stream (StreamSize > 0)")
		}
	}

	if opts.IdempotencyKey != "" {
		if res, ok := e.getResultFromCache(ch, opts.IdempotencyKey); ok {
			return MapPublishResult{Position: res, Suppressed: true, SuppressReason: SuppressReasonIdempotency}, nil
		}
	}

	streamTop, removePub, suppressReason, err := e.mapHub.remove(ch, key, opts)
	if err != nil {
		return MapPublishResult{}, err
	}

	if suppressReason != "" {
		result := MapPublishResult{Position: streamTop, Suppressed: true, SuppressReason: suppressReason}
		if suppressReason == SuppressReasonPositionMismatch {
			result.CurrentPublication = removePub
		}
		return result, nil
	}

	if opts.IdempotencyKey != "" {
		resultExpireSeconds := int64(defaultIdempotentResultExpireSeconds)
		if opts.IdempotentResultTTL != 0 {
			resultExpireSeconds = int64(opts.IdempotentResultTTL.Seconds())
		}
		e.saveResultToCache(ch, opts.IdempotencyKey, streamTop, resultExpireSeconds)
	}

	if e.eventHandler != nil {
		return MapPublishResult{Position: streamTop}, e.eventHandler.HandlePublication(ch, removePub, streamTop, false, nil)
	}

	return MapPublishResult{Position: streamTop}, nil
}

// ReadStream retrieves publications from stream.
func (e *MemoryMapBroker) ReadStream(ctx context.Context, ch string, opts MapReadStreamOptions) (MapStreamResult, error) {
	return e.mapHub.getStream(ch, opts)
}

// ReadState retrieves keyed state with revisions.
func (e *MemoryMapBroker) ReadState(ctx context.Context, ch string, opts MapReadStateOptions) (MapStateResult, error) {
	return e.mapHub.getState(ch, opts)
}

// Stats returns state statistics.
func (e *MemoryMapBroker) Stats(ctx context.Context, ch string) (MapStats, error) {
	return e.mapHub.getStats(ch)
}

func (e *MemoryMapBroker) getResultFromCache(ch string, key string) (StreamPosition, bool) {
	e.resultCacheMu.RLock()
	defer e.resultCacheMu.RUnlock()
	res, ok := e.resultCache[ch+"\x00"+key]
	return res, ok
}

func (e *MemoryMapBroker) saveResultToCache(ch string, key string, sp StreamPosition, resultExpireSeconds int64) {
	e.resultCacheMu.Lock()
	defer e.resultCacheMu.Unlock()
	cacheKey := ch + "\x00" + key
	e.resultCache[cacheKey] = sp
	expireAt := time.Now().Unix() + resultExpireSeconds
	heap.Push(&e.resultExpireQueue, &priority.Item{Value: cacheKey, Priority: expireAt})
	if e.nextExpireCheck == 0 || e.nextExpireCheck > expireAt {
		e.nextExpireCheck = expireAt
	}
}

func (e *MemoryMapBroker) clearResultCache(ch string) {
	e.resultCacheMu.Lock()
	defer e.resultCacheMu.Unlock()
	prefix := ch + "\x00"
	for key := range e.resultCache {
		if len(key) >= len(prefix) && key[:len(prefix)] == prefix {
			delete(e.resultCache, key)
		}
	}
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
		if e.nextExpireCheck == 0 || e.nextExpireCheck > time.Now().Unix() {
			e.resultCacheMu.Unlock()
			timer.Reset(time.Second)
			continue
		}
		nextExpireCheck = 0
		for e.resultExpireQueue.Len() > 0 {
			item := heap.Pop(&e.resultExpireQueue).(*priority.Item)
			expireAt := item.Priority
			if expireAt > time.Now().Unix() {
				heap.Push(&e.resultExpireQueue, item)
				nextExpireCheck = expireAt
				break
			}
			key := item.Value
			delete(e.resultCache, key)
		}
		e.nextExpireCheck = nextExpireCheck
		e.resultCacheMu.Unlock()
		timer.Reset(time.Second)
	}
}

// mapHub manages keyed state for all channels.
type mapHub struct {
	sync.RWMutex
	node            *Node
	channels        map[string]*mapChannel
	nextExpireCheck int64
	expireQueue     priority.Queue
	expires         map[string]int64
	historyMetaTTL  time.Duration
	nextRemoveCheck int64
	removeQueue     priority.Queue
	removes         map[string]int64
	closeCh         chan struct{}
	// Key TTL tracking
	nextKeyExpireCheck     int64
	keyExpireQueue         priority.Queue     // priority queue of {ch:key, expireAt}
	keyExpires             map[string]int64   // "ch:key" -> expireAt
	eventHandler           BrokerEventHandler // for publishing removal events
	channelOptionsResolver MapChannelOptionsResolver
}

// mapChannel represents keyed state for a single channel.
type mapChannel struct {
	mu                sync.Mutex       // protects sortedKeys rebuild in getState
	stream            *memstream.Stream
	state             map[string]*stateEntry // key -> entry
	ordered           bool
	scores            map[string]int64 // key -> score (for ordered state)
	sortedKeys        []string         // cached sorted keys by score (descending) for ordered state
	sortedKeysDirty   bool             // true if sortedKeys needs rebuilding
	lastSortedOrdered bool             // tracks whether last sort used ordered (score) or unordered (lexicographic)
}

type stateEntry struct {
	Key         string
	Revision    StreamPosition
	Publication *Publication
	Score       int64 // For ordered state
	ExpireAt    int64 // Unix timestamp for key TTL expiration (0 = no expiration)
}

func newMapHub(node *Node, historyMetaTTL time.Duration, closeCh chan struct{}) *mapHub {
	return &mapHub{
		node:           node,
		channels:       make(map[string]*mapChannel),
		expireQueue:    priority.MakeQueue(),
		expires:        make(map[string]int64),
		historyMetaTTL: historyMetaTTL,
		removeQueue:    priority.MakeQueue(),
		removes:        make(map[string]int64),
		closeCh:        closeCh,
		keyExpireQueue: priority.MakeQueue(),
		keyExpires:     make(map[string]int64),
	}
}

func (h *mapHub) setChannelOptionsResolver(r MapChannelOptionsResolver) {
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
		if h.nextExpireCheck == 0 || h.nextExpireCheck > time.Now().Unix() {
			h.Unlock()
			timer.Reset(time.Second)
			continue
		}
		nextExpireCheck = 0
		for h.expireQueue.Len() > 0 {
			item := heap.Pop(&h.expireQueue).(*priority.Item)
			expireAt := item.Priority
			if expireAt > time.Now().Unix() {
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
		if h.nextRemoveCheck == 0 || h.nextRemoveCheck > time.Now().Unix() {
			h.Unlock()
			timer.Reset(time.Second)
			continue
		}
		nextRemoveCheck = 0
		for h.removeQueue.Len() > 0 {
			item := heap.Pop(&h.removeQueue).(*priority.Item)
			expireAt := item.Priority
			if expireAt > time.Now().Unix() {
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

// expiredKeyEvent holds data for publishing removal events after lock is released.
type expiredKeyEvent struct {
	channel   string
	pub       *Publication
	streamPos StreamPosition
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
	// Collect expired keys while holding lock
	var expiredEvents []expiredKeyEvent
	var eventHandler BrokerEventHandler

	h.Lock()
	if h.nextKeyExpireCheck == 0 || h.nextKeyExpireCheck > time.Now().Unix() {
		h.Unlock()
		return
	}
	*nextKeyExpireCheck = 0
	now := time.Now().Unix()
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
			if entry.ExpireAt > now {
				// Entry was refreshed, re-queue
				h.keyExpires[chKey] = entry.ExpireAt
				heap.Push(&h.keyExpireQueue, &priority.Item{Value: chKey, Priority: entry.ExpireAt})
			}
			continue
		}

		// Remove the expired key
		delete(channel.state, key)
		delete(h.keyExpires, chKey)
		channel.sortedKeysDirty = true
		if channel.ordered {
			delete(channel.scores, key)
		}

		// Prepare removal event (add to stream while holding lock)
		if eventHandler != nil {
			// Get channel options for this channel
			opts := DefaultMapChannelOptions()
			if h.channelOptionsResolver != nil {
				opts = h.channelOptionsResolver(ch)
			}
			removePub := &Publication{
				Key:     key,
				Removed: true,
				Time:    time.Now().UnixMilli(),
				Info:    entry.Publication.Info,
			}
			var streamPos StreamPosition
			if opts.StreamSize > 0 && channel.stream != nil {
				// Add removal to stream only in streamed mode
				offset, _ := channel.stream.Add(removePub, opts.StreamSize, 0, "")
				removePub.Offset = offset
				streamPos = StreamPosition{
					Offset: offset,
					Epoch:  channel.stream.Epoch(),
				}
			} else if channel.stream != nil {
				// Streamless: use current position without writing
				streamPos = StreamPosition{
					Offset: channel.stream.Top(),
					Epoch:  channel.stream.Epoch(),
				}
			}
			expiredEvents = append(expiredEvents, expiredKeyEvent{
				channel:   ch,
				pub:       removePub,
				streamPos: streamPos,
			})
		}
	}
	h.nextKeyExpireCheck = *nextKeyExpireCheck
	h.Unlock()

	// Publish removal events after releasing lock to avoid deadlock
	for _, event := range expiredEvents {
		if err := eventHandler.HandlePublication(event.channel, event.pub, event.streamPos, false, nil); err != nil {
			h.node.logger.log(newErrorLogEntry(err, "error handling expired key publication", map[string]any{"channel": event.channel, "key": event.pub.Key}))
		}
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

func (h *mapHub) add(ch string, key string, statePub *Publication, streamPub *Publication, ordered bool, opts MapPublishOptions) (StreamPosition, *Publication, SuppressReason, error) {
	h.Lock()
	defer h.Unlock()

	var prevPub *Publication
	if opts.UseDelta && len(opts.StreamData) == 0 && key != "" {
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
			ordered: ordered,
			scores:  make(map[string]int64),
		}
		h.channels[ch] = channel
	}

	// Check KeyMode condition before proceeding
	if key != "" && opts.KeyMode != KeyModeReplace {
		existingEntry, keyExists := channel.state[key]
		if opts.KeyMode == KeyModeIfNew && keyExists {
			// KeyModeIfNew but key already exists - suppress publish
			// But optionally refresh TTL if RefreshTTLOnSuppress is set
			if opts.RefreshTTLOnSuppress && opts.KeyTTL > 0 {
				expireAt := time.Now().Unix() + int64(opts.KeyTTL.Seconds())
				existingEntry.ExpireAt = expireAt
				// Update TTL tracking
				chKey := h.makeChKey(ch, key)
				if _, exists := h.keyExpires[chKey]; !exists {
					heap.Push(&h.keyExpireQueue, &priority.Item{Value: chKey, Priority: expireAt})
				}
				h.keyExpires[chKey] = expireAt
				if h.nextKeyExpireCheck == 0 || h.nextKeyExpireCheck > expireAt {
					h.nextKeyExpireCheck = expireAt
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
			// Client uses: CurrentPublication.Offset + Position.Epoch for next CAS attempt.
			return pos, existing.Publication, SuppressReasonPositionMismatch, nil
		}
	}

	var streamPosition StreamPosition

	// Handle stream
	if opts.StreamSize > 0 && opts.StreamTTL > 0 {
		expireAt := time.Now().Unix() + int64(opts.StreamTTL.Seconds())
		if _, ok := h.expires[ch]; !ok {
			heap.Push(&h.expireQueue, &priority.Item{Value: ch, Priority: expireAt})
		}
		h.expires[ch] = expireAt
		if h.nextExpireCheck == 0 || h.nextExpireCheck > expireAt {
			h.nextExpireCheck = expireAt
		}

		historyMetaTTL := opts.MetaTTL
		if historyMetaTTL == 0 {
			historyMetaTTL = h.historyMetaTTL
		}

		if historyMetaTTL > 0 {
			removeAt := time.Now().Unix() + int64(historyMetaTTL.Seconds())
			if _, ok := h.removes[ch]; !ok {
				heap.Push(&h.removeQueue, &priority.Item{Value: ch, Priority: removeAt})
			}
			h.removes[ch] = removeAt
			if h.nextRemoveCheck == 0 || h.nextRemoveCheck > removeAt {
				h.nextRemoveCheck = removeAt
			}
		}

		if opts.Version > 0 {
			topVersion := channel.stream.TopVersion()
			topVersionEpoch := channel.stream.TopVersionEpoch()
			if (opts.VersionEpoch == "" || opts.VersionEpoch == topVersionEpoch) &&
				opts.Version <= topVersion {
				// Skip unordered publication
				return StreamPosition{Offset: channel.stream.Top(), Epoch: channel.stream.Epoch()}, nil, SuppressReasonVersion, nil
			}
		}

		offset, _ := channel.stream.Add(streamPub, opts.StreamSize, opts.Version, opts.VersionEpoch)
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

	// Handle keyed state
	if key != "" {
		// Calculate expiration time
		var expireAt int64
		if opts.KeyTTL > 0 {
			expireAt = time.Now().Unix() + int64(opts.KeyTTL.Seconds())
		}

		// Store statePub in state (contains full state Data)
		statePub.Offset = streamPosition.Offset
		entry := &stateEntry{
			Key:         key,
			Revision:    streamPosition,
			Publication: statePub,
			Score:       opts.Score,
			ExpireAt:    expireAt,
		}

		channel.state[key] = entry

		// Mark sorted keys as dirty for any state change
		channel.sortedKeysDirty = true
		if ordered {
			channel.scores[key] = opts.Score
		}

		// Handle key TTL expiration tracking
		if opts.KeyTTL > 0 {
			chKey := h.makeChKey(ch, key)
			if _, exists := h.keyExpires[chKey]; !exists {
				heap.Push(&h.keyExpireQueue, &priority.Item{Value: chKey, Priority: expireAt})
			}
			h.keyExpires[chKey] = expireAt
			if h.nextKeyExpireCheck == 0 || h.nextKeyExpireCheck > expireAt {
				h.nextKeyExpireCheck = expireAt
			}
		}
	}

	return streamPosition, prevPub, SuppressReasonNone, nil
}

func (h *mapHub) remove(ch string, key string, opts MapRemoveOptions) (StreamPosition, *Publication, SuppressReason, error) {
	h.Lock()
	defer h.Unlock()

	channel, ok := h.channels[ch]
	if !ok {
		return StreamPosition{}, nil, SuppressReasonKeyNotFound, nil
	}

	// Get the entry before removing to extract Info if present
	var info *ClientInfo
	entry, keyExists := channel.state[key]
	if keyExists {
		// Get ClientInfo from Publication (for presence)
		info = entry.Publication.Info
	} else {
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

	// Remove from state
	delete(channel.state, key)
	channel.sortedKeysDirty = true // Mark dirty for any removal
	if channel.ordered {
		delete(channel.scores, key)
	}

	// Clean up key expiration tracking
	chKey := h.makeChKey(ch, key)
	delete(h.keyExpires, chKey)

	// Create removal publication (reused for both stream and eventHandler).
	removePub := &Publication{
		Key:     key,
		Removed: true,
		Time:    time.Now().UnixMilli(),
		Info:    info,
	}

	var streamPosition StreamPosition

	// Add to stream if requested
	if opts.StreamSize > 0 && opts.StreamTTL > 0 {
		expireAt := time.Now().Unix() + int64(opts.StreamTTL.Seconds())
		if _, ok := h.expires[ch]; !ok {
			heap.Push(&h.expireQueue, &priority.Item{Value: ch, Priority: expireAt})
		}
		h.expires[ch] = expireAt
		if h.nextExpireCheck == 0 || h.nextExpireCheck > expireAt {
			h.nextExpireCheck = expireAt
		}

		// Refresh MetaTTL so the channel isn't garbage-collected while active.
		historyMetaTTL := opts.MetaTTL
		if historyMetaTTL == 0 {
			historyMetaTTL = h.historyMetaTTL
		}
		if historyMetaTTL > 0 {
			removeAt := time.Now().Unix() + int64(historyMetaTTL.Seconds())
			if _, ok := h.removes[ch]; !ok {
				heap.Push(&h.removeQueue, &priority.Item{Value: ch, Priority: removeAt})
			}
			h.removes[ch] = removeAt
			if h.nextRemoveCheck == 0 || h.nextRemoveCheck > removeAt {
				h.nextRemoveCheck = removeAt
			}
		}

		offset, _ := channel.stream.Add(removePub, opts.StreamSize, 0, "")
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
	// Update meta TTL before acquiring read lock
	historyMetaTTL := opts.MetaTTL
	if historyMetaTTL == 0 {
		historyMetaTTL = h.historyMetaTTL
	}

	if historyMetaTTL > 0 {
		h.Lock()
		h.updateMetaTTL(ch, historyMetaTTL)
		h.Unlock()
	}

	h.RLock()

	channel, ok := h.channels[ch]
	if !ok {
		h.RUnlock()
		h.Lock()
		streamPos := h.createStreamPosition(ch)
		h.Unlock()
		return MapStreamResult{Position: streamPos}, nil
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
			pub := item.Value.(*Publication)
			pub.Offset = item.Offset // Set offset from stream item
			pubs[i] = pub
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
		pub := item.Value.(*Publication)
		pub.Offset = item.Offset // Set offset from stream item
		pubs[i] = pub
	}
	h.RUnlock()
	return MapStreamResult{Publications: pubs, Position: streamPosition}, nil
}

func (h *mapHub) getState(ch string, opts MapReadStateOptions) (MapStateResult, error) {
	// Update meta TTL under write lock (brief).
	metaTTL := opts.MetaTTL
	if metaTTL == 0 {
		metaTTL = h.historyMetaTTL
	}
	if metaTTL > 0 {
		h.Lock()
		h.updateMetaTTL(ch, metaTTL)
		h.Unlock()
	}

	// Acquire read lock to get channel pointer.
	h.RLock()
	channel, ok := h.channels[ch]
	if !ok {
		h.RUnlock()
		// Channel not found — need write lock to create stream position.
		h.Lock()
		// Re-check after acquiring write lock (TOCTOU: another goroutine may have created it).
		if _, exists := h.channels[ch]; exists {
			h.Unlock()
			// Channel was created between RUnlock and Lock — retry under RLock.
			return h.getState(ch, opts)
		}
		if opts.Revision != nil && opts.Revision.Epoch != "" {
			pos := h.createStreamPosition(ch)
			h.Unlock()
			return MapStateResult{Position: pos}, ErrorUnrecoverablePosition
		}
		pos := h.createStreamPosition(ch)
		h.Unlock()
		return MapStateResult{Position: pos}, nil
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

	// Handle single key lookup (Key filter)
	if opts.Key != "" {
		entry, exists := channel.state[opts.Key]
		if !exists {
			return MapStateResult{Position: streamPosition}, nil
		}
		return MapStateResult{Publications: []*Publication{entry.Publication}, Position: streamPosition}, nil
	}

	var pubs []*Publication

	// Rebuild sorted keys if dirty or sort order changed since last call.
	wantOrdered := channel.ordered
	if channel.sortedKeysDirty || len(channel.sortedKeys) != len(channel.state) || channel.lastSortedOrdered != wantOrdered {
		channel.sortedKeys = make([]string, 0, len(channel.state))
		for key := range channel.state {
			channel.sortedKeys = append(channel.sortedKeys, key)
		}

		// Sort by score (descending) for ordered state, lexicographically for non-ordered
		// For ordered: use (score DESC, key DESC) to match Redis native ZREVRANGE ordering
		if wantOrdered {
			sort.Slice(channel.sortedKeys, func(i, j int) bool {
				si := channel.scores[channel.sortedKeys[i]]
				sj := channel.scores[channel.sortedKeys[j]]
				if si != sj {
					return si > sj // Primary: score descending
				}
				return channel.sortedKeys[i] > channel.sortedKeys[j] // Secondary: key descending
			})
		} else {
			sort.Strings(channel.sortedKeys) // Lexicographic sort for consistency
		}
		channel.sortedKeysDirty = false
		channel.lastSortedOrdered = wantOrdered
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
			// Ordered cursor format: "score\x00key"
			startIdx = findOrderedCursorPosition(channel.sortedKeys, channel.scores, opts.Cursor)
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
				// Ordered cursor: "score\x00key"
				lastScore := channel.scores[lastKey]
				cursor = makeOrderedCursor(strconv.FormatInt(lastScore, 10), lastKey)
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

func (h *mapHub) createStreamPosition(ch string) StreamPosition {
	// Create a new stream if needed
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
	removeAt := time.Now().Unix() + int64(metaTTL.Seconds())
	if _, ok := h.removes[ch]; !ok {
		heap.Push(&h.removeQueue, &priority.Item{Value: ch, Priority: removeAt})
	}
	h.removes[ch] = removeAt
	if h.nextRemoveCheck == 0 || h.nextRemoveCheck > removeAt {
		h.nextRemoveCheck = removeAt
	}
}
