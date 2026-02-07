package centrifuge

import (
	"container/heap"
	"context"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/centrifugal/centrifuge/internal/memstream"
	"github.com/centrifugal/centrifuge/internal/priority"
	"golang.org/x/sync/singleflight"
)

// MapCacheConfig configures the in-memory cache for keyed channels.
type MapCacheConfig struct {
	// MaxChannels is the maximum number of channels to cache.
	// When exceeded, least recently used channels are evicted.
	// 0 means unlimited.
	MaxChannels int

	// ChannelIdleTimeout evicts channels not accessed for this duration.
	// 0 means no idle timeout.
	ChannelIdleTimeout time.Duration

	// StreamSize is the max stream entries to keep per channel in cache.
	// Should match or exceed backend StreamSize.
	StreamSize int
}

// DefaultMapCacheConfig returns the default cache configuration.
func DefaultMapCacheConfig() MapCacheConfig {
	return MapCacheConfig{
		MaxChannels:        10000,
		ChannelIdleTimeout: 5 * time.Minute,
		StreamSize:         1000,
	}
}

// ChannelLoader loads channel data from backend storage.
// Receives resolved channel options to load correct amount of stream history.
// Returns snapshot publications, stream publications (for recovery), and current position.
type ChannelLoader func(ctx context.Context, ch string, opts MapChannelOptions) (snapshot []*Publication, stream []*Publication, pos StreamPosition, err error)

// MapCache provides in-memory caching for keyed channel data.
// It supports lazy loading, LRU eviction, and applying updates.
type MapCache interface {
	// EnsureLoaded ensures the channel is loaded into cache.
	// Uses singleflight to prevent thundering herd on concurrent loads.
	// Channel options determine stream size/TTL and snapshot key TTL.
	EnsureLoaded(ctx context.Context, ch string, opts MapChannelOptions, loader ChannelLoader) error

	// Evict removes a channel from the cache.
	Evict(ch string)

	// IsLoaded returns true if the channel is currently in cache.
	IsLoaded(ch string) bool

	// IsLoading returns true if the channel is currently being loaded.
	IsLoading(ch string) bool

	// BufferPublication buffers a publication for a channel that is being loaded.
	// Returns true if buffered, false if the channel is not loading.
	BufferPublication(ch string, pub *Publication, pos StreamPosition, removed bool) bool

	// MarkLoading marks a channel as loading so publications are buffered.
	MarkLoading(ch string)

	// ClearLoading clears the loading state for a channel.
	ClearLoading(ch string)

	// LoadedChannels returns a list of all currently loaded channels.
	LoadedChannels() []string

	// GetState retrieves state data from cache.
	GetState(ch string, opts MapReadStateOptions) ([]*Publication, StreamPosition, string, error)

	// GetStream retrieves stream data from cache.
	GetStream(ch string, opts MapReadStreamOptions) ([]*Publication, StreamPosition, error)

	// GetStats returns statistics about a cached channel.
	GetStats(ch string) (MapStats, error)

	// GetEpoch returns the epoch for a cached channel, or empty string if not loaded.
	GetEpoch(ch string) string

	// ApplyPublication applies a publication to the cache.
	// If removed is true, the publication represents a key removal.
	ApplyPublication(ch string, pub *Publication, pos StreamPosition, removed bool) error

	// ApplyState replaces the entire state for a channel.
	// If opts is non-nil, uses those options; otherwise preserves existing options
	// or uses defaults for new channels.
	ApplyState(ch string, pubs []*Publication, pos StreamPosition, opts *MapChannelOptions) error

	// PopulateStream populates the stream with initial data from backend.
	// Unlike ApplyPublication, this doesn't check position offset since we're
	// loading historical data that may have lower offsets than the current position.
	PopulateStream(ch string, pubs []*Publication, epoch string)

	// Close shuts down the cache and releases resources.
	Close() error
}

// bufferedPub holds a publication that arrived during channel loading.
type bufferedPub struct {
	pub     *Publication
	pos     StreamPosition
	removed bool
}

// mapCacheImpl implements MapCache.
type mapCacheImpl struct {
	mu            sync.RWMutex
	channels      map[string]*cachedChannel
	loadGroup     singleflight.Group
	loaded        map[string]bool
	loading       map[string]bool           // Channels currently being loaded
	loadingBuffer map[string][]*bufferedPub // Publications buffered during loading
	lastAccess    map[string]time.Time
	conf          MapCacheConfig
	closeCh       chan struct{}
	closeOnce     sync.Once
}

// cachedChannel holds cached data for a single channel.
type cachedChannel struct {
	stream          *memstream.Stream
	snapshot        map[string]*cachedSnapshotEntry
	ordered         bool
	scores          map[string]int64
	sortedKeys      []string
	sortedKeysDirty bool
	position        StreamPosition
	options         MapChannelOptions // Per-channel options for TTL/size
}

// cachedSnapshotEntry represents a single entry in the cached snapshot.
type cachedSnapshotEntry struct {
	Key         string
	Publication *Publication
	Score       int64
}

// newMapCache creates a new MapCache with the given configuration.
func newMapCache(conf MapCacheConfig) *mapCacheImpl {
	c := &mapCacheImpl{
		channels:      make(map[string]*cachedChannel),
		loaded:        make(map[string]bool),
		loading:       make(map[string]bool),
		loadingBuffer: make(map[string][]*bufferedPub),
		lastAccess:    make(map[string]time.Time),
		conf:          conf,
		closeCh:       make(chan struct{}),
	}

	// Start idle timeout eviction if configured
	if conf.ChannelIdleTimeout > 0 {
		go c.runIdleEviction()
	}

	return c
}

// EnsureLoaded ensures the channel is loaded into cache.
func (c *mapCacheImpl) EnsureLoaded(ctx context.Context, ch string, opts MapChannelOptions, loader ChannelLoader) error {
	// Fast path: already loaded
	c.mu.RLock()
	loaded := c.loaded[ch]
	c.mu.RUnlock()

	if loaded {
		// Update last access under write lock
		c.mu.Lock()
		c.lastAccess[ch] = time.Now()
		c.mu.Unlock()
		return nil
	}

	// Slow path: load with singleflight to prevent thundering herd
	_, err, _ := c.loadGroup.Do(ch, func() (any, error) {
		// Double-check after acquiring singleflight
		c.mu.Lock()
		if c.loaded[ch] {
			c.mu.Unlock()
			return nil, nil
		}
		// Mark as loading so incoming publications are buffered
		c.loading[ch] = true
		c.mu.Unlock()

		// Load from backend (snapshot + stream for recovery)
		snapshotPubs, streamPubs, pos, err := loader(ctx, ch, opts)
		if err != nil {
			// Clean up loading state on error
			c.mu.Lock()
			delete(c.loading, ch)
			delete(c.loadingBuffer, ch)
			c.mu.Unlock()
			return nil, err
		}

		// Apply to cache
		c.mu.Lock()
		defer c.mu.Unlock()

		// Check eviction before adding
		c.maybeEvictLocked()

		// Create channel entry with per-channel options
		channel := &cachedChannel{
			stream:   memstream.New(),
			snapshot: make(map[string]*cachedSnapshotEntry),
			scores:   make(map[string]int64),
			position: pos,
			options:  opts,
		}

		// Populate snapshot and detect if ordered (any non-zero score means ordered)
		for _, pub := range snapshotPubs {
			entry := &cachedSnapshotEntry{
				Key:         pub.Key,
				Publication: pub,
				Score:       pub.Score,
			}
			channel.snapshot[pub.Key] = entry
			if pub.Score != 0 {
				channel.ordered = true
				channel.scores[pub.Key] = pub.Score
			}
		}

		// Populate stream for client recovery during reconnect storms.
		// Use channel's StreamSize and StreamTTL from resolved options.
		streamSize := opts.StreamSize
		if streamSize <= 0 {
			streamSize = c.conf.StreamSize // Fallback to cache config
		}
		streamTTLSeconds := uint64(opts.StreamTTL.Seconds())

		if channel.stream != nil && streamSize > 0 {
			for _, pub := range streamPubs {
				channel.stream.Add(pub, streamSize, streamTTLSeconds, "")
				// Update position to reflect actual stream content
				if pub.Offset > channel.position.Offset {
					channel.position.Offset = pub.Offset
				}
			}
		}

		c.channels[ch] = channel
		c.loaded[ch] = true
		c.lastAccess[ch] = time.Now()

		// Apply any publications that arrived during loading
		buffered := c.loadingBuffer[ch]
		delete(c.loadingBuffer, ch)
		delete(c.loading, ch)

		for _, bp := range buffered {
			// Only apply if offset is newer than what we loaded
			if bp.pub.Offset > channel.position.Offset {
				c.applyPublicationLocked(ch, bp.pub, bp.pos, bp.removed)
			}
		}

		return nil, nil
	})

	return err
}

// Evict removes a channel from the cache.
func (c *mapCacheImpl) Evict(ch string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.evictLocked(ch)
}

func (c *mapCacheImpl) evictLocked(ch string) {
	delete(c.channels, ch)
	delete(c.loaded, ch)
	delete(c.loading, ch)
	delete(c.loadingBuffer, ch)
	delete(c.lastAccess, ch)
}

// IsLoaded returns true if the channel is currently in cache.
func (c *mapCacheImpl) IsLoaded(ch string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.loaded[ch]
}

// IsLoading returns true if the channel is currently being loaded.
func (c *mapCacheImpl) IsLoading(ch string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.loading[ch]
}

// BufferPublication buffers a publication for a channel that is being loaded.
// Returns true if buffered, false if the channel is not loading.
func (c *mapCacheImpl) BufferPublication(ch string, pub *Publication, pos StreamPosition, removed bool) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.loading[ch] {
		return false
	}
	c.loadingBuffer[ch] = append(c.loadingBuffer[ch], &bufferedPub{
		pub:     pub,
		pos:     pos,
		removed: removed,
	})
	return true
}

// MarkLoading marks a channel as loading so publications are buffered.
// Call this BEFORE subscribing to pub/sub to prevent race conditions.
func (c *mapCacheImpl) MarkLoading(ch string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.loaded[ch] {
		c.loading[ch] = true
	}
}

// ClearLoading clears the loading state for a channel.
// Used when Subscribe fails before EnsureLoaded runs.
func (c *mapCacheImpl) ClearLoading(ch string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.loading, ch)
	delete(c.loadingBuffer, ch)
}

// LoadedChannels returns a list of all currently loaded channels.
func (c *mapCacheImpl) LoadedChannels() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	channels := make([]string, 0, len(c.loaded))
	for ch := range c.loaded {
		channels = append(channels, ch)
	}
	return channels
}

// GetState retrieves state data from cache.
func (c *mapCacheImpl) GetState(ch string, opts MapReadStateOptions) ([]*Publication, StreamPosition, string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	channel, ok := c.channels[ch]
	if !ok {
		// Channel not loaded - return empty with zero position
		if opts.Revision != nil && opts.Revision.Epoch != "" {
			return nil, StreamPosition{}, "", ErrorUnrecoverablePosition
		}
		return nil, StreamPosition{}, "", nil
	}

	c.lastAccess[ch] = time.Now()

	// Check revision epoch
	if opts.Revision != nil && opts.Revision.Epoch != "" {
		if opts.Revision.Epoch != channel.position.Epoch {
			return nil, channel.position, "", ErrorUnrecoverablePosition
		}
	}

	// Handle single key lookup
	if opts.Key != "" {
		entry, exists := channel.snapshot[opts.Key]
		if !exists {
			return []*Publication{}, channel.position, "", nil
		}
		return []*Publication{entry.Publication}, channel.position, "", nil
	}

	// Rebuild sorted keys if dirty
	if channel.sortedKeysDirty || len(channel.sortedKeys) != len(channel.snapshot) {
		channel.sortedKeys = make([]string, 0, len(channel.snapshot))
		for key := range channel.snapshot {
			channel.sortedKeys = append(channel.sortedKeys, key)
		}

		if opts.Ordered && channel.ordered {
			sort.Slice(channel.sortedKeys, func(i, j int) bool {
				si := channel.scores[channel.sortedKeys[i]]
				sj := channel.scores[channel.sortedKeys[j]]
				if si != sj {
					return si > sj
				}
				return channel.sortedKeys[i] > channel.sortedKeys[j]
			})
		} else {
			sort.Strings(channel.sortedKeys)
		}
		channel.sortedKeysDirty = false
	}

	totalKeys := len(channel.sortedKeys)
	if totalKeys == 0 {
		return []*Publication{}, channel.position, "", nil
	}

	// Handle cursor pagination
	var startIdx int
	if opts.Cursor != "" {
		if opts.Ordered && channel.ordered {
			startIdx = c.findOrderedCursorPosition(channel, opts.Cursor)
		} else {
			startIdx = c.findUnorderedCursorPosition(channel.sortedKeys, opts.Cursor)
		}
	}

	if startIdx >= totalKeys {
		return []*Publication{}, channel.position, "", nil
	}

	endIdx := totalKeys
	cursor := ""

	if opts.Limit > 0 {
		endIdx = startIdx + opts.Limit
		if endIdx > totalKeys {
			endIdx = totalKeys
		}

		if endIdx < totalKeys {
			lastKey := channel.sortedKeys[endIdx-1]
			if opts.Ordered && channel.ordered {
				lastScore := channel.scores[lastKey]
				cursor = c.makeOrderedCursor(lastScore, lastKey)
			} else {
				cursor = lastKey
			}
		}
	}

	pubs := make([]*Publication, endIdx-startIdx)
	for i := startIdx; i < endIdx; i++ {
		key := channel.sortedKeys[i]
		entry := channel.snapshot[key]
		pubs[i-startIdx] = entry.Publication
	}

	return pubs, channel.position, cursor, nil
}

// GetStream retrieves stream data from cache.
func (c *mapCacheImpl) GetStream(ch string, opts MapReadStreamOptions) ([]*Publication, StreamPosition, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	channel, ok := c.channels[ch]
	if !ok {
		return nil, StreamPosition{}, nil
	}

	stream := channel.stream
	if stream == nil {
		return nil, channel.position, nil
	}

	// Use the backend position for epoch consistency.
	// The stream's internal epoch is auto-generated and differs from backend.
	streamPosition := channel.position

	filter := opts.Filter

	// Get all items from stream - we'll filter by backend offset below.
	// The memstream's internal offsets don't match backend offsets, so we can't
	// use offset-based retrieval directly.
	// Note: limit=-1 means unlimited (limit >= 0 check in memstream.Get)
	items, _, err := stream.Get(0, false, -1, false) // Get all items
	if err != nil {
		return nil, StreamPosition{}, err
	}

	if filter.Since == nil {
		// No Since filter - return all or limited items
		if filter.Limit == 0 {
			return nil, streamPosition, nil
		}
		limit := filter.Limit
		if limit < 0 || limit > len(items) {
			limit = len(items) // -1 means all
		}
		pubs := make([]*Publication, limit)
		for i := 0; i < limit; i++ {
			idx := i
			if filter.Reverse {
				idx = len(items) - 1 - i
			}
			pubs[i] = items[idx].Value.(*Publication)
		}
		return pubs, streamPosition, nil
	}

	since := filter.Since

	// Check if already up to date
	if !filter.Reverse {
		if streamPosition.Offset == since.Offset && since.Epoch == streamPosition.Epoch {
			return nil, streamPosition, nil
		}
	}

	// Filter items by backend offset stored in each Publication
	var filtered []*Publication
	for _, item := range items {
		pub := item.Value.(*Publication)
		if !filter.Reverse {
			// Forward: get items with offset > since.Offset
			if pub.Offset > since.Offset {
				filtered = append(filtered, pub)
			}
		} else {
			// Reverse: get items with offset < since.Offset
			if pub.Offset < since.Offset {
				filtered = append(filtered, pub)
			}
		}
	}

	// Apply limit
	if filter.Limit > 0 && len(filtered) > filter.Limit {
		if filter.Reverse {
			// Take last N items for reverse
			filtered = filtered[len(filtered)-filter.Limit:]
		} else {
			filtered = filtered[:filter.Limit]
		}
	}

	// Reverse the order if requested
	if filter.Reverse {
		for i, j := 0, len(filtered)-1; i < j; i, j = i+1, j-1 {
			filtered[i], filtered[j] = filtered[j], filtered[i]
		}
	}

	return filtered, streamPosition, nil
}

// GetStats returns statistics about a cached channel.
func (c *mapCacheImpl) GetStats(ch string) (MapStats, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	channel, ok := c.channels[ch]
	if !ok {
		return MapStats{}, nil
	}

	return MapStats{
		NumKeys: len(channel.snapshot),
	}, nil
}

// GetEpoch returns the epoch for a cached channel, or empty string if not loaded.
func (c *mapCacheImpl) GetEpoch(ch string) string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	channel, ok := c.channels[ch]
	if !ok {
		return ""
	}
	return channel.position.Epoch
}

// GetPosition returns the current stream position for a cached channel.
func (c *mapCacheImpl) GetPosition(ch string) StreamPosition {
	c.mu.RLock()
	defer c.mu.RUnlock()

	channel, ok := c.channels[ch]
	if !ok {
		return StreamPosition{}
	}
	return channel.position
}

// ApplyPublication applies a publication to the cache.
func (c *mapCacheImpl) ApplyPublication(ch string, pub *Publication, pos StreamPosition, removed bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.applyPublicationLocked(ch, pub, pos, removed)
}

// applyPublicationLocked applies a publication while holding the lock.
func (c *mapCacheImpl) applyPublicationLocked(ch string, pub *Publication, pos StreamPosition, removed bool) error {
	channel, ok := c.channels[ch]
	if !ok {
		// Channel not loaded, nothing to update
		return nil
	}

	// Skip if this offset was already applied (prevents duplicates from concurrent paths)
	if pub.Offset <= channel.position.Offset {
		return nil
	}

	// Update position
	channel.position = pos

	// Add to stream using channel's configured options
	streamSize := channel.options.StreamSize
	if streamSize <= 0 {
		streamSize = c.conf.StreamSize // Fallback to cache config
	}
	if channel.stream != nil && streamSize > 0 {
		streamPub := &Publication{
			Offset:  pub.Offset,
			Key:     pub.Key,
			Data:    pub.Data,
			Tags:    pub.Tags,
			Info:    pub.Info,
			Time:    pub.Time,
			Removed: removed,
			Score:   pub.Score,
		}
		streamTTL := uint64(channel.options.StreamTTL.Seconds())
		channel.stream.Add(streamPub, streamSize, streamTTL, "")
	}

	// Update snapshot
	if removed {
		delete(channel.snapshot, pub.Key)
		delete(channel.scores, pub.Key)
	} else {
		channel.snapshot[pub.Key] = &cachedSnapshotEntry{
			Key:         pub.Key,
			Publication: pub,
			Score:       pub.Score,
		}
		// Update scores and ordered flag if publication has a score
		if pub.Score != 0 {
			channel.ordered = true
			channel.scores[pub.Key] = pub.Score
		}
	}
	channel.sortedKeysDirty = true

	c.lastAccess[ch] = time.Now()

	return nil
}

// PopulateStream populates the stream with initial data from backend.
// Unlike ApplyPublication, this doesn't check position offset since we're
// loading historical data that may have lower offsets than the current position.
// This method preserves any entries added by HandlePublication (newer offsets)
// and only adds missing historical entries.
func (c *mapCacheImpl) PopulateStream(ch string, pubs []*Publication, epoch string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	channel, ok := c.channels[ch]
	if !ok {
		// Channel not loaded, nothing to update
		return
	}

	// Get stream configuration
	streamSize := channel.options.StreamSize
	if streamSize <= 0 {
		streamSize = c.conf.StreamSize
	}
	if streamSize <= 0 {
		return
	}

	streamTTL := uint64(channel.options.StreamTTL.Seconds())

	// Get existing entries from stream (added by HandlePublication)
	var existingPubs []*Publication
	if channel.stream != nil {
		items, _, _ := channel.stream.Get(0, false, -1, false)
		for _, item := range items {
			existingPubs = append(existingPubs, item.Value.(*Publication))
		}
	}

	// Build a set of existing offsets and track max offset
	existingOffsets := make(map[uint64]bool)
	var maxOffset uint64
	for _, pub := range existingPubs {
		existingOffsets[pub.Offset] = true
		if pub.Offset > maxOffset {
			maxOffset = pub.Offset
		}
	}

	// Create fresh stream
	channel.stream = memstream.New()

	// Add backend publications (historical data)
	for _, pub := range pubs {
		if pub.Offset > maxOffset {
			maxOffset = pub.Offset
		}
		if existingOffsets[pub.Offset] {
			continue // Skip if already exists (from HandlePublication)
		}
		streamPub := &Publication{
			Offset:  pub.Offset,
			Key:     pub.Key,
			Data:    pub.Data,
			Tags:    pub.Tags,
			Info:    pub.Info,
			Time:    pub.Time,
			Removed: pub.Removed,
			Score:   pub.Score,
		}
		channel.stream.Add(streamPub, streamSize, streamTTL, "")
	}

	// Re-add existing entries (from HandlePublication - these are newer)
	for _, pub := range existingPubs {
		channel.stream.Add(pub, streamSize, streamTTL, "")
	}

	// Update channel position to reflect the highest offset in the stream.
	// This is critical - without it, GetStream may return empty when checking
	// "already up to date" even though new publications exist.
	if maxOffset > channel.position.Offset {
		channel.position.Offset = maxOffset
		if epoch != "" {
			channel.position.Epoch = epoch
		}
	}

	c.lastAccess[ch] = time.Now()
}

// ApplySnapshot replaces the entire snapshot for a channel.
func (c *mapCacheImpl) ApplyState(ch string, pubs []*Publication, pos StreamPosition, opts *MapChannelOptions) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check eviction before adding
	c.maybeEvictLocked()

	// Determine options: use provided, preserve existing, or use defaults
	var channelOpts MapChannelOptions
	if opts != nil {
		channelOpts = *opts
	} else if existing, ok := c.channels[ch]; ok {
		channelOpts = existing.options
	} else {
		channelOpts = DefaultMapChannelOptions()
	}

	channel := &cachedChannel{
		stream:          memstream.New(),
		snapshot:        make(map[string]*cachedSnapshotEntry),
		scores:          make(map[string]int64),
		position:        pos,
		sortedKeysDirty: true,
		options:         channelOpts,
	}

	for _, pub := range pubs {
		channel.snapshot[pub.Key] = &cachedSnapshotEntry{
			Key:         pub.Key,
			Publication: pub,
			Score:       pub.Score,
		}
		if pub.Score != 0 {
			channel.ordered = true
			channel.scores[pub.Key] = pub.Score
		}
	}

	c.channels[ch] = channel
	c.loaded[ch] = true
	c.lastAccess[ch] = time.Now()

	return nil
}

// Close shuts down the cache.
func (c *mapCacheImpl) Close() error {
	c.closeOnce.Do(func() {
		close(c.closeCh)
	})
	return nil
}

// maybeEvictLocked evicts channels if MaxChannels is exceeded.
// Must be called with lock held.
func (c *mapCacheImpl) maybeEvictLocked() {
	if c.conf.MaxChannels <= 0 {
		return
	}

	for len(c.channels) >= c.conf.MaxChannels {
		// Find least recently used channel
		var oldestCh string
		var oldestTime time.Time
		for ch, t := range c.lastAccess {
			if oldestCh == "" || t.Before(oldestTime) {
				oldestCh = ch
				oldestTime = t
			}
		}
		if oldestCh != "" {
			c.evictLocked(oldestCh)
		} else {
			break
		}
	}
}

// runIdleEviction runs the idle channel eviction loop.
func (c *mapCacheImpl) runIdleEviction() {
	ticker := time.NewTicker(c.conf.ChannelIdleTimeout / 2)
	defer ticker.Stop()

	for {
		select {
		case <-c.closeCh:
			return
		case <-ticker.C:
			c.evictIdleChannels()
		}
	}
}

// evictIdleChannels evicts channels that haven't been accessed recently.
func (c *mapCacheImpl) evictIdleChannels() {
	c.mu.Lock()
	defer c.mu.Unlock()

	cutoff := time.Now().Add(-c.conf.ChannelIdleTimeout)
	for ch, t := range c.lastAccess {
		if t.Before(cutoff) {
			c.evictLocked(ch)
		}
	}
}

// makeOrderedCursor creates a cursor for ordered snapshots.
func (c *mapCacheImpl) makeOrderedCursor(score int64, key string) string {
	return strconv.FormatInt(score, 10) + "\x00" + key
}

// parseOrderedCursor parses an ordered cursor into score and key.
func (c *mapCacheImpl) parseOrderedCursor(cursor string) (int64, string) {
	for i := 0; i < len(cursor); i++ {
		if cursor[i] == '\x00' {
			score, _ := strconv.ParseInt(cursor[:i], 10, 64)
			return score, cursor[i+1:]
		}
	}
	return 0, ""
}

// findUnorderedCursorPosition finds the position after the cursor key.
func (c *mapCacheImpl) findUnorderedCursorPosition(sortedKeys []string, cursor string) int {
	return sort.Search(len(sortedKeys), func(i int) bool {
		return sortedKeys[i] > cursor
	})
}

// findOrderedCursorPosition finds the position after the cursor in ordered snapshot.
func (c *mapCacheImpl) findOrderedCursorPosition(channel *cachedChannel, cursor string) int {
	cursorScore, cursorKey := c.parseOrderedCursor(cursor)

	return sort.Search(len(channel.sortedKeys), func(i int) bool {
		key := channel.sortedKeys[i]
		score := channel.scores[key]
		if score != cursorScore {
			return score < cursorScore
		}
		return key < cursorKey
	})
}

// mapCacheWithEviction extends mapCacheImpl with priority-based eviction.
type mapCacheWithEviction struct {
	*mapCacheImpl
	accessQueue priority.Queue
	accessMap   map[string]int64 // ch -> last access timestamp
}

// newMapCacheWithEviction creates a cache with LRU eviction support.
func newMapCacheWithEviction(conf MapCacheConfig) *mapCacheWithEviction {
	c := &mapCacheWithEviction{
		mapCacheImpl: newMapCache(conf),
		accessQueue:  priority.MakeQueue(),
		accessMap:    make(map[string]int64),
	}
	return c
}

// updateAccess updates the access time for a channel in the priority queue.
func (c *mapCacheWithEviction) updateAccess(ch string) {
	now := time.Now().UnixNano()
	if _, exists := c.accessMap[ch]; !exists {
		heap.Push(&c.accessQueue, &priority.Item{Value: ch, Priority: now})
	}
	c.accessMap[ch] = now
}
