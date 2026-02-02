package centrifuge

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"time"
)

// CachedKeyedEngineConfig configures the cached keyed engine wrapper.
type CachedKeyedEngineConfig struct {
	// Cache configuration for the in-memory cache layer.
	Cache KeyedCacheConfig

	// SyncInterval is how often each channel syncs with backend.
	// Each channel syncs independently on its own timer.
	// Default: 30s
	SyncInterval time.Duration

	// SyncJitter is the relative jitter factor (0.0 to 1.0) for sync timing.
	// Actual interval is SyncInterval * (1 ± random(0, SyncJitter)).
	// For example, 0.1 means ±10% jitter, so 30s becomes 27s-33s.
	// Default: 0.1 (10%)
	SyncJitter float64

	// SyncConcurrency limits parallel sync operations.
	// 0 means unbounded (spawn goroutine per sync).
	// Default: 0
	SyncConcurrency int

	// SyncBatchSize is max publications to fetch per sync poll.
	// Default: 1000
	SyncBatchSize int

	// LoadTimeout is the timeout for lazy-loading a channel from backend.
	// Default: 5s
	LoadTimeout time.Duration
}

// DefaultCachedKeyedEngineConfig returns the default configuration.
func DefaultCachedKeyedEngineConfig() CachedKeyedEngineConfig {
	return CachedKeyedEngineConfig{
		Cache:           DefaultKeyedCacheConfig(),
		SyncInterval:    30 * time.Second,
		SyncJitter:      0.1, // 10% jitter
		SyncConcurrency: 0,
		SyncBatchSize:   1000,
		LoadTimeout:     5 * time.Second,
	}
}

func (c CachedKeyedEngineConfig) setDefaults() CachedKeyedEngineConfig {
	if c.SyncInterval <= 0 {
		c.SyncInterval = 30 * time.Second
	}
	if c.SyncJitter < 0 {
		c.SyncJitter = 0
	}
	if c.SyncJitter > 1 {
		c.SyncJitter = 1
	}
	if c.SyncBatchSize <= 0 {
		c.SyncBatchSize = 1000
	}
	if c.LoadTimeout <= 0 {
		c.LoadTimeout = 5 * time.Second
	}
	if c.Cache.MaxChannels == 0 && c.Cache.ChannelIdleTimeout == 0 && c.Cache.StreamSize == 0 {
		c.Cache = DefaultKeyedCacheConfig()
	}
	return c
}

// CachedKeyedEngine wraps a KeyedEngine with an in-memory cache layer.
// It provides:
// - Read-your-own-writes consistency on the publishing node
// - Low-latency reads from memory instead of database
// - Cross-node eventual consistency via stream synchronization
// - Backward compatibility - existing engines work unchanged
type CachedKeyedEngine struct {
	node         *Node
	backend      KeyedEngine
	cache        *keyedCacheImpl
	conf         CachedKeyedEngineConfig
	eventHandler BrokerEventHandler

	// Stream synchronization - per-channel state
	syncState   map[string]*channelSyncState
	syncStateMu sync.RWMutex

	// Per-channel locks for ordered cache updates
	channelLocks   map[string]*sync.Mutex
	channelLocksMu sync.Mutex

	// Worker pool for bounded concurrency (nil if unbounded)
	syncWorkerCh chan string

	closeCh                chan struct{}
	closeOnce              sync.Once
	wg                     sync.WaitGroup
	channelOptionsResolver ChannelOptionsResolver
}

// channelSyncState tracks sync state for a single channel.
type channelSyncState struct {
	lastOffset   uint64
	nextSyncTime time.Time
	syncing      bool // Prevents concurrent syncs of the same channel
}

var _ KeyedEngine = (*CachedKeyedEngine)(nil)

// NewCachedKeyedEngine creates a cached wrapper around any KeyedEngine.
// The cache layer provides read-your-own-writes consistency and low-latency
// reads while the backend provides durability and cross-node consistency.
func NewCachedKeyedEngine(n *Node, backend KeyedEngine, conf CachedKeyedEngineConfig) (*CachedKeyedEngine, error) {
	if backend == nil {
		return nil, errors.New("cached keyed engine: backend is required")
	}
	conf = conf.setDefaults()

	cache := newKeyedCache(conf.Cache)

	e := &CachedKeyedEngine{
		node:         n,
		backend:      backend,
		cache:        cache,
		conf:         conf,
		syncState:    make(map[string]*channelSyncState),
		channelLocks: make(map[string]*sync.Mutex),
		closeCh:      make(chan struct{}),
	}

	// Create worker channel if concurrency is bounded
	if conf.SyncConcurrency > 0 {
		e.syncWorkerCh = make(chan string, conf.SyncConcurrency*2)
	}

	return e, nil
}

// cachedEventHandler wraps the original event handler to intercept publications
// and update the cache before forwarding to the original handler.
type cachedEventHandler struct {
	engine  *CachedKeyedEngine
	cache   *keyedCacheImpl
	backend KeyedEngine
	handler BrokerEventHandler
	conf    CachedKeyedEngineConfig
}

func (h *cachedEventHandler) HandlePublication(ch string, pub *Publication, sp StreamPosition, delta bool, prevPub *Publication) error {
	// Update cache if channel is loaded, using per-channel lock for ordering
	if h.cache.IsLoaded(ch) {
		h.engine.withChannelLock(ch, func() {
			cachedPos := h.cache.GetPosition(ch)

			// Check for gap - if incoming offset > cached + 1, we missed publications
			if cachedPos.Offset > 0 && pub.Offset > cachedPos.Offset+1 {
				// Check epoch first - if different, need full reload
				if cachedPos.Epoch != "" && sp.Epoch != "" && cachedPos.Epoch != sp.Epoch {
					// Epoch changed - evict and let next read reload
					h.cache.Evict(ch)
					return
				}
				// Same epoch - fill the gap with ONE backend call
				h.fillGap(ch, cachedPos, pub.Offset, sp.Epoch)
			}

			// Skip if this offset was already applied (can happen with concurrent local writes)
			if pub.Offset <= h.cache.GetPosition(ch).Offset {
				return
			}

			// Now apply the incoming publication
			_ = h.cache.ApplyPublication(ch, pub, sp, pub.Removed)
		})
	}
	// Forward to original handler
	if h.handler != nil {
		return h.handler.HandlePublication(ch, pub, sp, delta, prevPub)
	}
	return nil
}

// fillGap fetches missing publications from backend and applies them to cache.
// Must be called with channel lock held.
func (h *cachedEventHandler) fillGap(ch string, cachedPos StreamPosition, incomingOffset uint64, epoch string) {
	ctx, cancel := context.WithTimeout(context.Background(), h.conf.LoadTimeout)
	defer cancel()

	// Fetch missing publications (from cachedPos to incomingOffset-1)
	missing, _, err := h.backend.ReadStream(ctx, ch, KeyedReadStreamOptions{
		Filter: StreamFilter{
			Since: &cachedPos,
			Limit: int(incomingOffset - cachedPos.Offset - 1), // Only what's missing
		},
	})
	if err != nil {
		// On error, evict cache to force reload on next read
		h.cache.Evict(ch)
		return
	}

	// Apply missing publications in order
	for _, m := range missing {
		pubPos := StreamPosition{Offset: m.Offset, Epoch: epoch}
		_ = h.cache.ApplyPublication(ch, m, pubPos, m.Removed)
	}
}

func (h *cachedEventHandler) HandleJoin(ch string, info *ClientInfo) error {
	if h.handler != nil {
		return h.handler.HandleJoin(ch, info)
	}
	return nil
}

func (h *cachedEventHandler) HandleLeave(ch string, info *ClientInfo) error {
	if h.handler != nil {
		return h.handler.HandleLeave(ch, info)
	}
	return nil
}

// backendRegistrar is an interface for backends that can register event handlers.
type backendRegistrar interface {
	RegisterEventHandler(BrokerEventHandler) error
}

// RegisterEventHandler registers the event handler and starts background workers.
// It also registers a wrapper handler with the backend to intercept publications
// and update the cache.
func (e *CachedKeyedEngine) RegisterEventHandler(h BrokerEventHandler) error {
	e.eventHandler = h

	// Create wrapper that updates cache and forwards to original handler
	wrapper := &cachedEventHandler{
		engine:  e,
		cache:   e.cache,
		backend: e.backend,
		handler: h,
		conf:    e.conf,
	}

	// Register with backend if it supports event handler registration
	if registrar, ok := e.backend.(backendRegistrar); ok {
		if err := registrar.RegisterEventHandler(wrapper); err != nil {
			return err
		}
	}

	// Start sync loop (handles cross-node updates for channels loaded before
	// backend started delivering publications)
	go e.runSyncLoop()
	return nil
}

// Close shuts down the cached engine.
func (e *CachedKeyedEngine) Close(ctx context.Context) error {
	e.closeOnce.Do(func() {
		close(e.closeCh)
		// Wait for sync workers to finish
		e.wg.Wait()
		_ = e.cache.Close()
	})
	return nil
}

// withChannelLock executes fn while holding the per-channel lock.
// This ensures ordered cache updates for concurrent writes to the same channel.
func (e *CachedKeyedEngine) withChannelLock(ch string, fn func()) {
	e.channelLocksMu.Lock()
	lock, ok := e.channelLocks[ch]
	if !ok {
		lock = &sync.Mutex{}
		e.channelLocks[ch] = lock
	}
	e.channelLocksMu.Unlock()

	lock.Lock()
	defer lock.Unlock()
	fn()
}

// Subscribe registers this server node to receive pub/sub messages for the channel.
func (e *CachedKeyedEngine) Subscribe(ch string) error {
	// Subscribe to backend PUB/SUB first so we receive updates
	if err := e.backend.Subscribe(ch); err != nil {
		return err
	}
	// Preload cache - this ensures cache is populated before first read
	// and HandlePublication can update it for real-time changes
	ctx, cancel := context.WithTimeout(context.Background(), e.conf.LoadTimeout)
	defer cancel()
	_ = e.ensureLoaded(ctx, ch)
	return nil
}

// Unsubscribe removes this server node from receiving pub/sub messages for the channel.
func (e *CachedKeyedEngine) Unsubscribe(ch string) error {
	// Evict from cache on unsubscribe
	e.cache.Evict(ch)
	return e.backend.Unsubscribe(ch)
}

// SetChannelOptionsResolver sets the callback for resolving channel options per channel.
func (e *CachedKeyedEngine) SetChannelOptionsResolver(r ChannelOptionsResolver) {
	e.channelOptionsResolver = r
	e.backend.SetChannelOptionsResolver(r)
}

// Publish updates the snapshot and broadcasts the change to subscribers.
// Write path: backend first, then update local cache for read-your-own-writes.
// Uses per-channel lock to ensure serialized cache updates under concurrency.
func (e *CachedKeyedEngine) Publish(ctx context.Context, ch string, key string, opts KeyedPublishOptions) (KeyedPublishResult, error) {
	// Write to backend (source of truth).
	// Backend calls HandlePublication which updates the cache with proper gap detection.
	return e.backend.Publish(ctx, ch, key, opts)
}

// Unpublish removes a key from the snapshot and notifies subscribers.
// Backend calls HandlePublication which updates the cache with proper gap detection.
func (e *CachedKeyedEngine) Unpublish(ctx context.Context, ch string, key string, opts KeyedUnpublishOptions) (KeyedPublishResult, error) {
	return e.backend.Unpublish(ctx, ch, key, opts)
}

// ReadSnapshot retrieves the current key-value state for a channel.
// Read path: ensure loaded, then read from cache.
func (e *CachedKeyedEngine) ReadSnapshot(ctx context.Context, ch string, opts KeyedReadSnapshotOptions) ([]*Publication, StreamPosition, string, error) {
	// Ensure channel is loaded into cache
	if err := e.ensureLoaded(ctx, ch); err != nil {
		return nil, StreamPosition{}, "", err
	}

	// Read from cache
	return e.cache.GetSnapshot(ch, opts)
}

// ReadStream retrieves publications from the channel's history stream.
func (e *CachedKeyedEngine) ReadStream(ctx context.Context, ch string, opts KeyedReadStreamOptions) ([]*Publication, StreamPosition, error) {
	// Ensure channel is loaded into cache
	if err := e.ensureLoaded(ctx, ch); err != nil {
		return nil, StreamPosition{}, err
	}

	// Read from cache
	return e.cache.GetStream(ch, opts)
}

// Stats returns statistics about the channel's snapshot.
func (e *CachedKeyedEngine) Stats(ctx context.Context, ch string) (KeyedStats, error) {
	// If loaded, return from cache
	if e.cache.IsLoaded(ch) {
		return e.cache.GetStats(ch)
	}
	// Otherwise query backend
	return e.backend.Stats(ctx, ch)
}

// Remove deletes all data for a channel (snapshot and stream).
func (e *CachedKeyedEngine) Remove(ctx context.Context, ch string, opts KeyedRemoveOptions) error {
	// Remove from cache
	e.cache.Evict(ch)

	// Remove sync state
	e.syncStateMu.Lock()
	delete(e.syncState, ch)
	e.syncStateMu.Unlock()

	// Remove from backend
	return e.backend.Remove(ctx, ch, opts)
}

// ensureLoaded loads channel data from backend into cache if not already loaded.
// Loads both snapshot and stream for client recovery during reconnect storms.
// Uses resolved channel options for stream size/TTL configuration.
func (e *CachedKeyedEngine) ensureLoaded(ctx context.Context, ch string) error {
	// Resolve channel options for stream size/TTL
	opts := e.resolveChannelOptions(ch)

	return e.cache.EnsureLoaded(ctx, ch, opts, func(ctx context.Context, ch string, opts KeyedChannelOptions) ([]*Publication, []*Publication, StreamPosition, error) {
		loadCtx, cancel := context.WithTimeout(ctx, e.conf.LoadTimeout)
		defer cancel()

		// Load full snapshot from backend
		snapshotPubs, pos, _, err := e.backend.ReadSnapshot(loadCtx, ch, KeyedReadSnapshotOptions{
			Limit: 0, // All entries (unlimited)
		})
		if err != nil {
			return nil, nil, StreamPosition{}, err
		}

		// Load stream for client recovery using channel's configured StreamSize.
		// This enables clients to recover from the in-memory cache during
		// reconnect storms without hitting the backend.
		// Use Reverse=true to get the most recent entries (for recovery scenarios).
		var streamPubs []*Publication
		if opts.StreamSize > 0 {
			streamPubs, _, err = e.backend.ReadStream(loadCtx, ch, KeyedReadStreamOptions{
				Filter: StreamFilter{
					Limit:   opts.StreamSize,
					Reverse: true, // Get most recent entries
				},
			})
			if err != nil {
				// Stream load failure is not fatal - cache can still work
				// without pre-populated stream (gap filling will handle it)
				streamPubs = nil
			}
			// Reverse the slice to restore chronological order for cache
			for i, j := 0, len(streamPubs)-1; i < j; i, j = i+1, j-1 {
				streamPubs[i], streamPubs[j] = streamPubs[j], streamPubs[i]
			}
		}

		// Initialize sync offset to current position
		e.updateSyncOffset(ch, pos.Offset)

		return snapshotPubs, streamPubs, pos, nil
	})
}

// resolveChannelOptions resolves keyed channel options for a channel.
func (e *CachedKeyedEngine) resolveChannelOptions(ch string) KeyedChannelOptions {
	if e.channelOptionsResolver != nil {
		return e.channelOptionsResolver(ch)
	}
	return DefaultKeyedChannelOptions()
}

// runSyncLoop runs the background sync scheduler.
// It checks which channels need syncing and dispatches them to workers.
func (e *CachedKeyedEngine) runSyncLoop() {
	// Start worker pool if bounded concurrency
	if e.conf.SyncConcurrency > 0 {
		for i := 0; i < e.conf.SyncConcurrency; i++ {
			e.wg.Add(1)
			go e.syncWorker()
		}
	}

	// Scheduler tick interval - check for due channels frequently
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-e.closeCh:
			// Close worker channel to signal workers to stop
			if e.syncWorkerCh != nil {
				close(e.syncWorkerCh)
			}
			return
		case <-ticker.C:
			e.dispatchDueSyncs()
		}
	}
}

// dispatchDueSyncs finds channels due for sync and dispatches them.
func (e *CachedKeyedEngine) dispatchDueSyncs() {
	now := time.Now()
	channels := e.cache.LoadedChannels()

	for _, ch := range channels {
		e.syncStateMu.Lock()
		state, exists := e.syncState[ch]
		if !exists {
			// New channel - initialize sync state with jittered first sync
			state = &channelSyncState{
				lastOffset:   0,
				nextSyncTime: now.Add(e.jitteredInterval()),
				syncing:      false,
			}
			e.syncState[ch] = state
			e.syncStateMu.Unlock()
			continue
		}

		// Check if due for sync and not already syncing
		if now.Before(state.nextSyncTime) || state.syncing {
			e.syncStateMu.Unlock()
			continue
		}

		// Mark as syncing and schedule next sync
		state.syncing = true
		state.nextSyncTime = now.Add(e.jitteredInterval())
		e.syncStateMu.Unlock()

		// Dispatch sync
		if e.syncWorkerCh != nil {
			// Bounded concurrency - send to worker pool
			select {
			case e.syncWorkerCh <- ch:
			default:
				// Worker pool full, skip this cycle
				e.syncStateMu.Lock()
				if s, ok := e.syncState[ch]; ok {
					s.syncing = false
				}
				e.syncStateMu.Unlock()
			}
		} else {
			// Unbounded concurrency - spawn goroutine
			e.wg.Add(1)
			go func(channel string) {
				defer e.wg.Done()
				e.syncChannel(channel)
			}(ch)
		}
	}
}

// syncWorker is a worker goroutine that processes sync requests.
func (e *CachedKeyedEngine) syncWorker() {
	defer e.wg.Done()
	for ch := range e.syncWorkerCh {
		e.syncChannel(ch)
	}
}

// jitteredInterval returns the sync interval with random relative jitter.
func (e *CachedKeyedEngine) jitteredInterval() time.Duration {
	interval := e.conf.SyncInterval
	if e.conf.SyncJitter > 0 {
		// Calculate max jitter as a fraction of the interval
		maxJitter := float64(interval) * e.conf.SyncJitter
		// Random jitter between -maxJitter and +maxJitter
		jitter := time.Duration((rand.Float64()*2 - 1) * maxJitter)
		interval += jitter
	}
	return interval
}

// syncChannel syncs a single channel with backend.
func (e *CachedKeyedEngine) syncChannel(ch string) {
	defer func() {
		// Mark sync as complete
		e.syncStateMu.Lock()
		if state, ok := e.syncState[ch]; ok {
			state.syncing = false
		}
		e.syncStateMu.Unlock()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), e.conf.LoadTimeout)
	defer cancel()

	// Get last synced offset and epoch for this channel
	since := e.getSyncOffset(ch)
	cachedEpoch := e.cache.GetEpoch(ch)

	// Fetch new publications from backend
	pubs, pos, err := e.backend.ReadStream(ctx, ch, KeyedReadStreamOptions{
		Filter: StreamFilter{
			Since: &StreamPosition{Offset: since},
			Limit: e.conf.SyncBatchSize,
		},
	})
	if err != nil {
		// Sync errors are expected during transient failures, don't log to avoid noise
		return
	}

	// Check for epoch mismatch - backend epoch changed, need full reload
	if cachedEpoch != "" && pos.Epoch != "" && cachedEpoch != pos.Epoch {
		e.reloadChannel(ctx, ch)
		return
	}

	// Check for gap in stream continuity - if first pub offset > since + 1, we missed publications
	if len(pubs) > 0 && since > 0 && pubs[0].Offset > since+1 {
		e.reloadChannel(ctx, ch)
		return
	}

	// Apply each publication to cache
	for _, pub := range pubs {
		// Use publication's own offset for position, preserving epoch from stream position
		pubPos := StreamPosition{Offset: pub.Offset, Epoch: pos.Epoch}
		_ = e.cache.ApplyPublication(ch, pub, pubPos, pub.Removed)
	}

	// Update sync cursor
	if len(pubs) > 0 {
		e.updateSyncOffset(ch, pubs[len(pubs)-1].Offset)
	} else if pos.Offset > since {
		// Update cursor even if no new pubs (position advanced)
		e.updateSyncOffset(ch, pos.Offset)
	}
}

// reloadChannel evicts the channel from cache and reloads it from backend.
func (e *CachedKeyedEngine) reloadChannel(ctx context.Context, ch string) {
	// Evict from cache
	e.cache.Evict(ch)

	// Clear sync state
	e.syncStateMu.Lock()
	delete(e.syncState, ch)
	e.syncStateMu.Unlock()

	// Reload by ensuring it's loaded again
	_ = e.ensureLoaded(ctx, ch)
}

// getSyncOffset returns the last synced offset for a channel.
func (e *CachedKeyedEngine) getSyncOffset(ch string) uint64 {
	e.syncStateMu.RLock()
	defer e.syncStateMu.RUnlock()
	if state, ok := e.syncState[ch]; ok {
		return state.lastOffset
	}
	return 0
}

// updateSyncOffset updates the sync offset for a channel.
func (e *CachedKeyedEngine) updateSyncOffset(ch string, offset uint64) {
	e.syncStateMu.Lock()
	defer e.syncStateMu.Unlock()
	if state, ok := e.syncState[ch]; ok {
		if offset > state.lastOffset {
			state.lastOffset = offset
		}
	} else {
		e.syncState[ch] = &channelSyncState{
			lastOffset:   offset,
			nextSyncTime: time.Now().Add(e.jitteredInterval()),
		}
	}
}

// Backend returns the underlying backend KeyedEngine.
// Useful for advanced operations that bypass the cache.
func (e *CachedKeyedEngine) Backend() KeyedEngine {
	return e.backend
}
