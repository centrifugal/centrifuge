package centrifuge

import (
	"container/heap"
	"context"
	"encoding/json"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/centrifugal/centrifuge/internal/memstream"
	"github.com/centrifugal/centrifuge/internal/priority"
)

// MemoryKeyedEngine is builtin default KeyedEngine which allows running Centrifuge-based
// server without any external storage. All data managed inside process memory.
//
// With this KeyedEngine you can only run single Centrifuge node. If you need to scale
// you should consider using another KeyedEngine implementation instead – for example
// RedisKeyedEngine.
type MemoryKeyedEngine struct {
	node              *Node
	eventHandler      BrokerEventHandler
	keyedHub          *keyedHub
	closeOnce         sync.Once
	closeCh           chan struct{}
	pubLocks          map[int]*sync.Mutex
	resultCache       map[string]StreamPosition
	resultCacheMu     sync.RWMutex
	nextExpireCheck   int64
	resultExpireQueue priority.Queue
}

var _ KeyedEngine = (*MemoryKeyedEngine)(nil)

// MemoryKeyedEngineConfig is a memory keyed engine config.
type MemoryKeyedEngineConfig struct{}

// NewMemoryKeyedEngine initializes MemoryKeyedEngine.
func NewMemoryKeyedEngine(n *Node, _ MemoryKeyedEngineConfig) (*MemoryKeyedEngine, error) {
	pubLocks := make(map[int]*sync.Mutex, numPubLocks)
	for i := 0; i < numPubLocks; i++ {
		pubLocks[i] = &sync.Mutex{}
	}
	closeCh := make(chan struct{})
	e := &MemoryKeyedEngine{
		node:        n,
		keyedHub:    newKeyedHub(n.config.HistoryMetaTTL, closeCh),
		pubLocks:    pubLocks,
		closeCh:     closeCh,
		resultCache: map[string]StreamPosition{},
	}
	return e, nil
}

// RegisterBrokerEventHandler runs memory keyed engine.
func (e *MemoryKeyedEngine) RegisterBrokerEventHandler(h BrokerEventHandler) error {
	e.eventHandler = h
	go e.expireResultCache()
	e.keyedHub.runCleanups()
	return nil
}

// Close shuts down the engine.
func (e *MemoryKeyedEngine) Close(_ context.Context) error {
	e.closeOnce.Do(func() {
		close(e.closeCh)
	})
	return nil
}

func (e *MemoryKeyedEngine) pubLock(ch string) *sync.Mutex {
	return e.pubLocks[index(ch, numPubLocks)]
}

func (e *MemoryKeyedEngine) Remove(ctx context.Context, ch string, opts KeyedRemoveOptions) error {
	// TODO: implement.
	return nil
}

// Subscribe is noop here.
func (e *MemoryKeyedEngine) Subscribe(_ string) error {
	return nil
}

// Unsubscribe is noop here.
func (e *MemoryKeyedEngine) Unsubscribe(_ string) error {
	return nil
}

// Publish publishes data to channel with optional key for keyed state.
func (e *MemoryKeyedEngine) Publish(ctx context.Context, ch string, key string, data []byte, opts KeyedPublishOptions) (StreamPosition, bool, error) {
	mu := e.pubLock(ch)
	mu.Lock()
	defer mu.Unlock()

	if opts.IdempotencyKey != "" {
		if res, ok := e.getResultFromCache(ch, opts.IdempotencyKey); ok {
			return res, true, nil
		}
	}

	pub := &Publication{
		Data: data,
		Info: opts.ClientInfo,
		Tags: opts.Tags,
		Time: time.Now().UnixMilli(),
		Key:  key,
	}

	var prevPub *Publication
	streamTop, prevPub, skip, err := e.keyedHub.add(ch, key, pub, opts)
	if err != nil {
		return StreamPosition{}, false, err
	}
	if skip {
		return streamTop, false, nil
	}

	pub.Offset = streamTop.Offset

	if opts.IdempotencyKey != "" {
		resultExpireSeconds := int64(defaultIdempotentResultExpireSeconds)
		if opts.IdempotentResultTTL != 0 {
			resultExpireSeconds = int64(opts.IdempotentResultTTL.Seconds())
		}
		e.saveResultToCache(ch, opts.IdempotencyKey, streamTop, resultExpireSeconds)
	}

	if e.eventHandler != nil {
		return streamTop, false, e.eventHandler.HandlePublication(ch, pub, streamTop, opts.UseDelta, prevPub)
	}

	return streamTop, false, nil
}

// Unpublish removes a key from keyed state.
func (e *MemoryKeyedEngine) Unpublish(ctx context.Context, ch string, key string, opts KeyedUnpublishOptions) (StreamPosition, error) {
	mu := e.pubLock(ch)
	mu.Lock()
	defer mu.Unlock()

	streamTop, err := e.keyedHub.remove(ch, key, opts)
	if err != nil {
		return StreamPosition{}, err
	}

	if opts.Publish && e.eventHandler != nil {
		pub := &Publication{
			Key:     key,
			Removed: true,
			Offset:  streamTop.Offset,
			Time:    time.Now().UnixMilli(),
		}
		return streamTop, e.eventHandler.HandlePublication(ch, pub, streamTop, false, nil)
	}

	return streamTop, nil
}

// ReadStream retrieves publications from stream.
func (e *MemoryKeyedEngine) ReadStream(ctx context.Context, ch string, opts KeyedReadStreamOptions) ([]*Publication, StreamPosition, error) {
	return e.keyedHub.getStream(ch, opts)
}

// ReadSnapshot retrieves keyed snapshot with revisions.
func (e *MemoryKeyedEngine) ReadSnapshot(ctx context.Context, ch string, opts KeyedReadSnapshotOptions) ([]*Publication, StreamPosition, string, error) {
	return e.keyedHub.getSnapshot(ch, opts)
}

// Stats returns snapshot statistics.
func (e *MemoryKeyedEngine) Stats(ctx context.Context, ch string) (KeyedStats, error) {
	return e.keyedHub.getStats(ch)
}

// AddMember adds a client to presence in a channel.
func (e *MemoryKeyedEngine) AddMember(ctx context.Context, ch string, info ClientInfo, opts EnginePresenceOptions) error {
	mu := e.pubLock(ch)
	mu.Lock()
	defer mu.Unlock()

	data, err := json.Marshal(info)
	if err != nil {
		return err
	}

	keyedOpts := KeyedPublishOptions{
		StreamSize: 10000,             // Default stream size for presence
		StreamTTL:  300 * time.Second, // Default TTL
		KeyTTL:     60 * time.Second,  // Presence TTL (auto-expire)
	}

	streamTop, _, _, err := e.keyedHub.add(ch, info.ClientID, &Publication{
		Key:  info.ClientID,
		Data: data,
		Info: &info,
	}, keyedOpts)
	if err != nil {
		return err
	}

	if opts.Publish && e.eventHandler != nil {
		pub := &Publication{
			Key:    info.ClientID,
			Data:   data,
			Info:   &info,
			Offset: streamTop.Offset,
			Time:   time.Now().UnixMilli(),
		}
		return e.eventHandler.HandlePublication(ch, pub, streamTop, false, nil)
	}

	return nil
}

// RemoveMember removes a client from presence in a channel.
func (e *MemoryKeyedEngine) RemoveMember(ctx context.Context, ch string, info ClientInfo, opts EnginePresenceOptions) error {
	mu := e.pubLock(ch)
	mu.Lock()
	defer mu.Unlock()

	removeOpts := KeyedUnpublishOptions{
		Publish:    opts.Publish,
		StreamSize: 10000,
		StreamTTL:  300 * time.Second,
	}

	streamTop, err := e.keyedHub.remove(ch, info.ClientID, removeOpts)
	if err != nil {
		return err
	}

	if opts.Publish && e.eventHandler != nil {
		pub := &Publication{
			Key:     info.ClientID,
			Removed: true,
			Info:    &info,
			Offset:  streamTop.Offset,
			Time:    time.Now().UnixMilli(),
		}
		return e.eventHandler.HandlePublication(ch, pub, streamTop, false, nil)
	}

	return nil
}

// Members returns all members in a presence channel.
func (e *MemoryKeyedEngine) Members(ctx context.Context, ch string) (map[string]*ClientInfo, error) {
	entries, _, _, err := e.keyedHub.getSnapshot(ch, KeyedReadSnapshotOptions{
		Limit: 0, // Get all
	})
	if err != nil {
		return nil, err
	}

	members := make(map[string]*ClientInfo, len(entries))
	for _, pub := range entries {
		if pub.Info != nil {
			members[pub.Key] = pub.Info
		} else {
			// Fallback: unmarshal from Data if Info is not set
			var info ClientInfo
			if err := json.Unmarshal(pub.Data, &info); err != nil {
				continue
			}
			members[pub.Key] = &info
		}
	}

	return members, nil
}

// ReadPresenceSnapshot retrieves presence snapshot with revisions.
func (e *MemoryKeyedEngine) ReadPresenceSnapshot(ctx context.Context, ch string, opts KeyedReadSnapshotOptions) ([]*Publication, StreamPosition, error) {
	pubs, streamPos, _, err := e.keyedHub.getSnapshot(ch, opts)
	if err != nil {
		return nil, StreamPosition{}, err
	}
	// getSnapshot already returns Publications with Key and Offset set
	return pubs, streamPos, nil
}

// ReadPresenceStream retrieves presence event stream.
func (e *MemoryKeyedEngine) ReadPresenceStream(ctx context.Context, ch string, opts KeyedReadStreamOptions) ([]*Publication, StreamPosition, error) {
	return e.keyedHub.getStream(ch, opts)
}

// RegisterEventHandler registers event handler.
func (e *MemoryKeyedEngine) RegisterEventHandler(h BrokerEventHandler) error {
	return e.RegisterBrokerEventHandler(h)
}

func (e *MemoryKeyedEngine) getResultFromCache(ch string, key string) (StreamPosition, bool) {
	e.resultCacheMu.RLock()
	defer e.resultCacheMu.RUnlock()
	res, ok := e.resultCache[ch+"_"+key]
	return res, ok
}

func (e *MemoryKeyedEngine) saveResultToCache(ch string, key string, sp StreamPosition, resultExpireSeconds int64) {
	e.resultCacheMu.Lock()
	defer e.resultCacheMu.Unlock()
	cacheKey := ch + "_" + key
	e.resultCache[cacheKey] = sp
	expireAt := time.Now().Unix() + resultExpireSeconds
	heap.Push(&e.resultExpireQueue, &priority.Item{Value: cacheKey, Priority: expireAt})
	if e.nextExpireCheck == 0 || e.nextExpireCheck > expireAt {
		e.nextExpireCheck = expireAt
	}
}

func (e *MemoryKeyedEngine) expireResultCache() {
	var nextExpireCheck int64
	for {
		select {
		case <-time.After(time.Second):
		case <-e.closeCh:
			return
		}
		e.resultCacheMu.Lock()
		if e.nextExpireCheck == 0 || e.nextExpireCheck > time.Now().Unix() {
			e.resultCacheMu.Unlock()
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
	}
}

// keyedHub manages keyed state for all channels.
type keyedHub struct {
	sync.RWMutex
	channels        map[string]*keyedChannel
	nextExpireCheck int64
	expireQueue     priority.Queue
	expires         map[string]int64
	historyMetaTTL  time.Duration
	nextRemoveCheck int64
	removeQueue     priority.Queue
	removes         map[string]int64
	closeCh         chan struct{}
}

// keyedChannel represents keyed state for a single channel.
type keyedChannel struct {
	stream          *memstream.Stream
	snapshot        map[string]*snapshotEntry // key -> entry
	ordered         bool
	scores          map[string]int64 // key -> score (for ordered snapshots)
	sortedKeys      []string         // cached sorted keys by score (descending) for ordered snapshots
	sortedKeysDirty bool             // true if sortedKeys needs rebuilding
}

type snapshotEntry struct {
	Key         string
	Revision    StreamPosition
	Publication *Publication
	Score       int64 // For ordered snapshots
}

func newKeyedHub(historyMetaTTL time.Duration, closeCh chan struct{}) *keyedHub {
	return &keyedHub{
		channels:       make(map[string]*keyedChannel),
		expireQueue:    priority.MakeQueue(),
		expires:        make(map[string]int64),
		historyMetaTTL: historyMetaTTL,
		removeQueue:    priority.MakeQueue(),
		removes:        make(map[string]int64),
		closeCh:        closeCh,
	}
}

func (h *keyedHub) runCleanups() {
	go h.expireStreams()
	go h.removeChannels()
}

func (h *keyedHub) expireStreams() {
	var nextExpireCheck int64
	for {
		select {
		case <-time.After(time.Second):
		case <-h.closeCh:
			return
		}
		h.Lock()
		if h.nextExpireCheck == 0 || h.nextExpireCheck > time.Now().Unix() {
			h.Unlock()
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
	}
}

func (h *keyedHub) removeChannels() {
	var nextRemoveCheck int64
	for {
		select {
		case <-time.After(time.Second):
		case <-h.closeCh:
			return
		}
		h.Lock()
		if h.nextRemoveCheck == 0 || h.nextRemoveCheck > time.Now().Unix() {
			h.Unlock()
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
	}
}

func (h *keyedHub) add(ch string, key string, pub *Publication, opts KeyedPublishOptions) (StreamPosition, *Publication, bool, error) {
	h.Lock()
	defer h.Unlock()

	var prevPub *Publication
	if opts.UseDelta && key != "" {
		// Get previous publication for delta
		if channel, ok := h.channels[ch]; ok {
			if entry, ok := channel.snapshot[key]; ok {
				prevPub = entry.Publication
			}
		}
	}

	channel, ok := h.channels[ch]
	if !ok {
		channel = &keyedChannel{
			stream:   memstream.New(),
			snapshot: make(map[string]*snapshotEntry),
			ordered:  opts.Ordered,
			scores:   make(map[string]int64),
		}
		h.channels[ch] = channel
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

		historyMetaTTL := opts.StreamMetaTTL
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
				return StreamPosition{Offset: channel.stream.Top(), Epoch: channel.stream.Epoch()}, nil, true, nil
			}
		}

		offset, _ := channel.stream.Add(pub, opts.StreamSize, opts.Version, opts.VersionEpoch)
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

	// Handle keyed snapshot
	if key != "" {
		entry := &snapshotEntry{
			Key:         key,
			Revision:    streamPosition,
			Publication: pub,
			Score:       opts.Score,
		}

		channel.snapshot[key] = entry

		// Mark sorted keys as dirty for any snapshot change
		channel.sortedKeysDirty = true
		if opts.Ordered {
			channel.scores[key] = opts.Score
		}

		// Handle key TTL
		if opts.KeyTTL > 0 {
			// In a real implementation, we'd need a separate cleanup goroutine for key TTLs
			// For simplicity, we'll skip this for now
		}
	}

	return streamPosition, prevPub, false, nil
}

func (h *keyedHub) remove(ch string, key string, opts KeyedUnpublishOptions) (StreamPosition, error) {
	h.Lock()
	defer h.Unlock()

	channel, ok := h.channels[ch]
	if !ok {
		return StreamPosition{}, nil
	}

	// Get the entry before removing to extract Info if present
	var info *ClientInfo
	if entry, ok := channel.snapshot[key]; ok {
		// Get ClientInfo from Publication (for presence)
		info = entry.Publication.Info
	}

	// Remove from snapshot
	delete(channel.snapshot, key)
	channel.sortedKeysDirty = true // Mark dirty for any removal
	if channel.ordered {
		delete(channel.scores, key)
	}

	var streamPosition StreamPosition

	// Add to stream if requested
	if opts.StreamSize > 0 && opts.StreamTTL > 0 {
		removePub := &Publication{
			Key:     key,
			Removed: true,
			Time:    time.Now().UnixMilli(),
			Info:    info, // Include ClientInfo if available
		}

		expireAt := time.Now().Unix() + int64(opts.StreamTTL.Seconds())
		if _, ok := h.expires[ch]; !ok {
			heap.Push(&h.expireQueue, &priority.Item{Value: ch, Priority: expireAt})
		}
		h.expires[ch] = expireAt
		if h.nextExpireCheck == 0 || h.nextExpireCheck > expireAt {
			h.nextExpireCheck = expireAt
		}

		offset, _ := channel.stream.Add(removePub, opts.StreamSize, 0, "")
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

	return streamPosition, nil
}

func (h *keyedHub) getStream(ch string, opts KeyedReadStreamOptions) ([]*Publication, StreamPosition, error) {
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
	defer h.RUnlock()

	channel, ok := h.channels[ch]
	if !ok {
		h.RUnlock()
		h.Lock()
		defer h.Unlock()
		return nil, h.createStreamPosition(ch), nil
	}

	filter := opts.Filter

	stream := channel.stream
	if stream == nil {
		h.RUnlock()
		h.Lock()
		defer h.Unlock()
		return nil, h.createStreamPosition(ch), nil
	}

	streamPosition := StreamPosition{
		Offset: stream.Top(),
		Epoch:  stream.Epoch(),
	}

	if filter.Since == nil {
		if filter.Limit == 0 {
			return nil, streamPosition, nil
		}
		items, _, err := stream.Get(0, false, filter.Limit, filter.Reverse)
		if err != nil {
			return nil, StreamPosition{}, err
		}
		pubs := make([]*Publication, len(items))
		for i, item := range items {
			pub := item.Value.(*Publication)
			pub.Offset = item.Offset // Set offset from stream item
			pubs[i] = pub
		}
		return pubs, streamPosition, nil
	}

	since := filter.Since

	if !filter.Reverse {
		if streamPosition.Offset == since.Offset && since.Epoch == stream.Epoch() {
			return nil, streamPosition, nil
		}
	}

	streamOffset := since.Offset + 1
	if filter.Reverse {
		streamOffset = since.Offset - 1
	}

	items, _, err := stream.Get(streamOffset, true, filter.Limit, filter.Reverse)
	if err != nil {
		return nil, StreamPosition{}, err
	}

	pubs := make([]*Publication, len(items))
	for i, item := range items {
		pub := item.Value.(*Publication)
		pub.Offset = item.Offset // Set offset from stream item
		pubs[i] = pub
	}
	return pubs, streamPosition, nil
}

func (h *keyedHub) getSnapshot(ch string, opts KeyedReadSnapshotOptions) ([]*Publication, StreamPosition, string, error) {
	// Always use write lock since we cache sorted keys for all snapshots
	h.Lock()
	defer h.Unlock()

	// Update meta TTL
	if opts.SnapshotTTL > 0 {
		h.updateMetaTTL(ch, opts.SnapshotTTL)
	}

	channel, ok := h.channels[ch]
	if !ok {
		return nil, h.createStreamPosition(ch), "", nil
	}

	var streamPosition StreamPosition
	if channel.stream != nil {
		streamPosition = StreamPosition{
			Offset: channel.stream.Top(),
			Epoch:  channel.stream.Epoch(),
		}
	}

	// Check if client requested specific snapshot revision
	if opts.SnapshotRevision != nil {
		if streamPosition.Epoch != opts.SnapshotRevision.Epoch {
			// Epoch changed, return empty to force client restart
			return nil, streamPosition, "", nil
		}
	}

	var pubs []*Publication

	// Rebuild sorted keys if dirty (applies to both ordered and non-ordered)
	if channel.sortedKeysDirty || len(channel.sortedKeys) != len(channel.snapshot) {
		channel.sortedKeys = make([]string, 0, len(channel.snapshot))
		for key := range channel.snapshot {
			channel.sortedKeys = append(channel.sortedKeys, key)
		}

		// Sort by score (descending) for ordered snapshots, lexicographically for non-ordered
		if opts.Ordered && channel.ordered {
			sort.Slice(channel.sortedKeys, func(i, j int) bool {
				return channel.scores[channel.sortedKeys[i]] > channel.scores[channel.sortedKeys[j]]
			})
		} else {
			sort.Strings(channel.sortedKeys) // Lexicographic sort for consistency
		}
		channel.sortedKeysDirty = false
	}

	// Calculate pagination range first
	totalKeys := len(channel.sortedKeys)
	startIdx := 0
	endIdx := totalKeys
	cursor := ""

	if opts.Limit > 0 {
		offset := opts.Offset
		if opts.Cursor != "" {
			// Parse cursor - using strconv is faster than fmt
			if parsed, err := strconv.Atoi(opts.Cursor); err == nil {
				offset = parsed
			}
		}

		if offset >= totalKeys {
			return []*Publication{}, streamPosition, "", nil
		}

		startIdx = offset
		endIdx = offset + opts.Limit
		if endIdx > totalKeys {
			endIdx = totalKeys
		}

		if endIdx < totalKeys {
			cursor = strconv.Itoa(endIdx)
		}
	}

	// Build only the pubs we need (avoids allocating full slice then slicing)
	pubs = make([]*Publication, endIdx-startIdx)
	for i := startIdx; i < endIdx; i++ {
		key := channel.sortedKeys[i]
		entry := channel.snapshot[key]
		// Return the stored Publication pointer directly - it already has Key and Offset set
		pubs[i-startIdx] = entry.Publication
	}

	return pubs, streamPosition, cursor, nil
}

func (h *keyedHub) getStats(ch string) (KeyedStats, error) {
	h.RLock()
	defer h.RUnlock()

	channel, ok := h.channels[ch]
	if !ok {
		return KeyedStats{}, nil
	}

	return KeyedStats{
		NumKeys:           len(channel.snapshot),
		NumAggregatedKeys: 0, // Not applicable for memory engine
	}, nil
}

func (h *keyedHub) createStreamPosition(ch string) StreamPosition {
	// Create a new stream if needed
	channel, ok := h.channels[ch]
	if !ok {
		stream := memstream.New()
		h.channels[ch] = &keyedChannel{
			stream:   stream,
			snapshot: make(map[string]*snapshotEntry),
			scores:   make(map[string]int64),
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

func (h *keyedHub) updateMetaTTL(ch string, metaTTL time.Duration) {
	removeAt := time.Now().Unix() + int64(metaTTL.Seconds())
	if _, ok := h.removes[ch]; !ok {
		heap.Push(&h.removeQueue, &priority.Item{Value: ch, Priority: removeAt})
	}
	h.removes[ch] = removeAt
	if h.nextRemoveCheck == 0 || h.nextRemoveCheck > removeAt {
		h.nextRemoveCheck = removeAt
	}
}
