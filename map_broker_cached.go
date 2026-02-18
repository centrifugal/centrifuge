package centrifuge

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"time"
)

// CachedMapBrokerConfig configures the cached map broker wrapper.
type CachedMapBrokerConfig struct {
	// Cache configuration for the in-memory cache layer.
	Cache MapCacheConfig

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
	// 0 means use default (64 workers).
	// -1 means unbounded (spawn goroutine per sync, use with caution).
	// Default: 64
	SyncConcurrency int

	// SyncBatchSize is max publications to fetch per sync poll.
	// Default: 1000
	SyncBatchSize int

	// LoadTimeout is the timeout for lazy-loading a channel from backend.
	// Default: 5s
	LoadTimeout time.Duration
}

// DefaultCachedMapBrokerConfig returns the default configuration.
func DefaultCachedMapBrokerConfig() CachedMapBrokerConfig {
	return CachedMapBrokerConfig{
		Cache:           DefaultMapCacheConfig(),
		SyncInterval:    30 * time.Second,
		SyncJitter:      0.1, // 10% jitter
		SyncConcurrency: 64,
		SyncBatchSize:   1000,
		LoadTimeout:     5 * time.Second,
	}
}

func (c CachedMapBrokerConfig) setDefaults() CachedMapBrokerConfig {
	if c.SyncInterval <= 0 {
		c.SyncInterval = 30 * time.Second
	}
	if c.SyncJitter < 0 {
		c.SyncJitter = 0
	}
	if c.SyncJitter > 1 {
		c.SyncJitter = 1
	}
	if c.SyncConcurrency == 0 {
		c.SyncConcurrency = 64
	}
	if c.SyncBatchSize <= 0 {
		c.SyncBatchSize = 1000
	}
	if c.LoadTimeout <= 0 {
		c.LoadTimeout = 5 * time.Second
	}
	if c.Cache.MaxChannels == 0 && c.Cache.ChannelIdleTimeout == 0 && c.Cache.StreamSize == 0 {
		c.Cache = DefaultMapCacheConfig()
	}
	return c
}

// CachedMapBroker wraps a MapBroker with an in-memory cache layer.
// It provides:
// - Read-your-own-writes consistency on the publishing node
// - Low-latency reads from memory instead of database
// - Cross-node eventual consistency via stream synchronization
type CachedMapBroker struct {
	node         *Node
	backend      MapBroker
	cache        *mapCacheImpl
	conf         CachedMapBrokerConfig
	eventHandler BrokerEventHandler

	// Stream synchronization - per-channel state
	syncState   map[string]*channelSyncState
	syncStateMu sync.RWMutex

	// Track which channels have had their stream initialized from backend.
	// First ReadStream call always goes to backend; subsequent calls use cache.
	streamInitialized   map[string]bool
	streamInitializedMu sync.RWMutex

	// Per-channel locks for ordered cache updates
	channelLocks   map[string]*sync.Mutex
	channelLocksMu sync.Mutex

	// Worker pool for bounded concurrency (nil if unbounded)
	syncWorkerCh chan string

	closeCh   chan struct{}
	closeOnce sync.Once
	wg        sync.WaitGroup
}

// channelSyncState tracks sync state for a single channel.
type channelSyncState struct {
	lastOffset   uint64
	nextSyncTime time.Time
	syncing      bool // Prevents concurrent syncs of the same channel
}

var _ MapBroker = (*CachedMapBroker)(nil)

// NewCachedMapBroker creates a cached wrapper around any MapBroker.
// The cache layer provides read-your-own-writes consistency and low-latency
// reads while the backend provides durability and cross-node consistency.
func NewCachedMapBroker(n *Node, backend MapBroker, conf CachedMapBrokerConfig) (*CachedMapBroker, error) {
	if backend == nil {
		return nil, errors.New("cached map broker: backend is required")
	}
	conf = conf.setDefaults()

	cache := newMapCache(conf.Cache)

	e := &CachedMapBroker{
		node:              n,
		backend:           backend,
		cache:             cache,
		conf:              conf,
		syncState:         make(map[string]*channelSyncState),
		channelLocks:      make(map[string]*sync.Mutex),
		streamInitialized: make(map[string]bool),
		closeCh:           make(chan struct{}),
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
	broker  *CachedMapBroker
	cache   *mapCacheImpl
	backend MapBroker
	handler BrokerEventHandler
	conf    CachedMapBrokerConfig
}

func (h *cachedEventHandler) HandlePublication(ch string, pub *Publication, sp StreamPosition, delta bool, prevPub *Publication) error {
	// If channel is being loaded, try to buffer the publication for replay after load completes.
	// BufferPublication returns false if loading just finished (race condition) - in that case
	// fall through to check if channel is now loaded.
	buffered := false
	if h.cache.IsLoading(ch) {
		buffered = h.cache.BufferPublication(ch, pub, sp, pub.Removed)
	}
	if !buffered && h.cache.IsLoaded(ch) {
		// Update cache if channel is loaded, using per-channel lock for ordering
		h.broker.withChannelLock(ch, func() {
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

			// Skip if this offset was already applied (can happen with concurrent local writes).
			// For streamless channels (Offset=0), always apply — no offsets to compare.
			if pub.Offset > 0 && pub.Offset <= h.cache.GetPosition(ch).Offset {
				return
			}

			// Now apply the incoming publication
			_, _ = h.cache.ApplyPublication(ch, pub, sp, pub.Removed)

			// Update sync offset so syncChannel doesn't re-fetch publications
			// already delivered via pub/sub, avoiding duplicate delivery.
			h.broker.updateSyncOffset(ch, pub.Offset)
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
	missingResult, err := h.backend.ReadStream(ctx, ch, MapReadStreamOptions{
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
	for _, m := range missingResult.Publications {
		pubPos := StreamPosition{Offset: m.Offset, Epoch: epoch}
		_, _ = h.cache.ApplyPublication(ch, m, pubPos, m.Removed)
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
func (e *CachedMapBroker) RegisterEventHandler(h BrokerEventHandler) error {
	e.eventHandler = h

	// Create wrapper that updates cache and forwards to original handler
	wrapper := &cachedEventHandler{
		broker:  e,
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
	// backend started delivering publications).
	// Track in wg so the counter never reaches zero while dispatchDueSyncs
	// may call wg.Add(1) — prevents race between wg.Wait() and wg.Add().
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		e.runSyncLoop()
	}()
	return nil
}

// Close shuts down the cached broker and the underlying backend (if it implements Closer).
func (e *CachedMapBroker) Close(ctx context.Context) error {
	e.closeOnce.Do(func() {
		close(e.closeCh)
		// Wait for sync workers to finish
		e.wg.Wait()
		_ = e.cache.Close()
	})
	type Closer interface {
		Close(context.Context) error
	}
	if closer, ok := e.backend.(Closer); ok {
		return closer.Close(ctx)
	}
	return nil
}

// withChannelLock executes fn while holding the per-channel lock.
// This ensures ordered cache updates for concurrent writes to the same channel.
func (e *CachedMapBroker) withChannelLock(ch string, fn func()) {
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
func (e *CachedMapBroker) Subscribe(ch string) error {
	// Check if channel is already loaded - we'll need to evict BEFORE subscribing
	// to force a fresh load, catching any publications we might have missed.
	wasLoaded := e.cache.IsLoaded(ch)
	if wasLoaded {
		// Evict before subscribing so the fresh load happens after pub/sub is active.
		// This ensures we don't miss publications between the evict and subscribe.
		e.cache.Evict(ch)
		// Also clear stream initialized flag so ReadStream loads fresh from backend
		e.streamInitializedMu.Lock()
		delete(e.streamInitialized, ch)
		e.streamInitializedMu.Unlock()
	}

	// Mark as loading BEFORE subscribing to pub/sub, so any incoming
	// publications are buffered rather than dropped.
	e.cache.MarkLoading(ch)

	// Subscribe to backend PUB/SUB - now we'll receive updates
	if err := e.backend.Subscribe(ch); err != nil {
		e.cache.ClearLoading(ch)
		return err
	}

	// Preload cache - this ensures cache is populated before first read
	// and HandlePublication can update it for real-time changes.
	ctx, cancel := context.WithTimeout(context.Background(), e.conf.LoadTimeout)
	defer cancel()
	if err := e.ensureLoaded(ctx, ch); err != nil {
		// Cache load failed — unsubscribe from pub/sub to avoid a state where
		// pub/sub is active but cache is empty (publications would be dropped).
		_ = e.backend.Unsubscribe(ch)
		e.cache.ClearLoading(ch)
		return err
	}
	return nil
}

// Unsubscribe removes this server node from receiving pub/sub messages for the channel.
func (e *CachedMapBroker) Unsubscribe(ch string) error {
	// Evict from cache on unsubscribe
	e.cache.Evict(ch)

	// Clear stream initialized flag
	e.streamInitializedMu.Lock()
	delete(e.streamInitialized, ch)
	e.streamInitializedMu.Unlock()

	// Clean up per-channel lock
	e.channelLocksMu.Lock()
	delete(e.channelLocks, ch)
	e.channelLocksMu.Unlock()

	return e.backend.Unsubscribe(ch)
}

// Publish updates the state and broadcasts the change to subscribers.
// Write path: backend first, then update local cache for read-your-own-writes.
// Uses per-channel lock to ensure serialized cache updates under concurrency.
func (e *CachedMapBroker) Publish(ctx context.Context, ch string, key string, opts MapPublishOptions) (MapPublishResult, error) {
	// Write to backend (source of truth).
	// Backend calls HandlePublication which updates the cache with proper gap detection.
	return e.backend.Publish(ctx, ch, key, opts)
}

// Remove removes a key from the state and notifies subscribers.
// Backend calls HandlePublication which updates the cache with proper gap detection.
func (e *CachedMapBroker) Remove(ctx context.Context, ch string, key string, opts MapRemoveOptions) (MapPublishResult, error) {
	return e.backend.Remove(ctx, ch, key, opts)
}

// ReadState retrieves the current key-value state for a channel.
// By default, reads from backend for consistency (safe for CAS operations).
// If opts.Cached is true (internal subscription flow), reads from cache for performance.
func (e *CachedMapBroker) ReadState(ctx context.Context, ch string, opts MapReadStateOptions) (MapStateResult, error) {
	// Default: read from backend for consistency (safe for CAS, always fresh).
	// Application code should always get fresh data.
	if !opts.Cached {
		return e.backend.ReadState(ctx, ch, opts)
	}

	// Cached path - only used by internal subscription flow for optimized delivery.
	// Mark as loading BEFORE ensureLoaded to buffer any concurrent pub/sub messages.
	// This handles the race where pub/sub starts delivering before cache is fully loaded.
	e.cache.MarkLoading(ch)

	// Ensure channel is loaded into cache
	if err := e.ensureLoaded(ctx, ch); err != nil {
		e.cache.ClearLoading(ch)
		return MapStateResult{}, err
	}

	// Read from cache
	return e.cache.GetState(ch, opts)
}

// ReadStream retrieves publications from the channel's history stream.
// First call per channel always reads from backend to ensure we catch publications
// that happened between state load and pub/sub subscription (critical for presence).
// Subsequent calls use the cached stream (kept updated via pub/sub and sync).
func (e *CachedMapBroker) ReadStream(ctx context.Context, ch string, opts MapReadStreamOptions) (MapStreamResult, error) {
	// Ensure channel state is loaded into cache
	if err := e.ensureLoaded(ctx, ch); err != nil {
		return MapStreamResult{}, err
	}

	// Check if stream has been initialized for this channel
	e.streamInitializedMu.RLock()
	initialized := e.streamInitialized[ch]
	e.streamInitializedMu.RUnlock()

	if initialized {
		// Stream already initialized - read from cache (eventual consistency via pub/sub and sync)
		return e.cache.GetStream(ch, opts)
	}

	// First call for this channel - MUST read from backend.
	// This is critical during subscription to catch publications that happened
	// between state load and pub/sub subscription (e.g., presence notifications).
	fullResult, err := e.backend.ReadStream(ctx, ch, MapReadStreamOptions{
		Filter: StreamFilter{
			Limit: -1, // Get full stream for caching
		},
	})
	if err != nil {
		return MapStreamResult{}, err
	}

	// Populate cache with backend data, preserving any entries added by HandlePublication
	e.cache.PopulateStream(ch, fullResult.Publications, fullResult.Position.Epoch)

	// Mark stream as initialized - subsequent calls will use cache
	e.streamInitializedMu.Lock()
	e.streamInitialized[ch] = true
	e.streamInitializedMu.Unlock()

	// Return from cache with user's filter applied
	return e.cache.GetStream(ch, opts)
}

// Stats returns statistics about the channel's state.
func (e *CachedMapBroker) Stats(ctx context.Context, ch string) (MapStats, error) {
	// If loaded, return from cache
	if e.cache.IsLoaded(ch) {
		return e.cache.GetStats(ch)
	}
	// Otherwise query backend
	return e.backend.Stats(ctx, ch)
}

// Clear deletes all data for a channel (state and stream).
func (e *CachedMapBroker) Clear(ctx context.Context, ch string, opts MapClearOptions) error {
	// Remove from cache
	e.cache.Evict(ch)

	// Remove sync state
	e.syncStateMu.Lock()
	delete(e.syncState, ch)
	e.syncStateMu.Unlock()

	// Clear stream initialized flag
	e.streamInitializedMu.Lock()
	delete(e.streamInitialized, ch)
	e.streamInitializedMu.Unlock()

	// Clean up per-channel lock
	e.channelLocksMu.Lock()
	delete(e.channelLocks, ch)
	e.channelLocksMu.Unlock()

	// Clear from backend
	return e.backend.Clear(ctx, ch, opts)
}

// ensureLoaded loads channel data from backend into cache if not already loaded.
// Loads both state and stream for client recovery during reconnect storms.
// Uses resolved channel options for stream size/TTL configuration.
func (e *CachedMapBroker) ensureLoaded(ctx context.Context, ch string) error {
	// Resolve channel options for stream size/TTL
	opts, err := e.resolveChannelOptions(ch)
	if err != nil {
		return err
	}

	return e.cache.EnsureLoaded(ctx, ch, opts, func(ctx context.Context, ch string, opts MapChannelOptions) ([]*Publication, []*Publication, StreamPosition, error) {
		loadCtx, cancel := context.WithTimeout(ctx, e.conf.LoadTimeout)
		defer cancel()

		// Load full state from backend
		stateResult, err := e.backend.ReadState(loadCtx, ch, MapReadStateOptions{
			Limit: -1, // All entries (unlimited)
		})
		if err != nil {
			return nil, nil, StreamPosition{}, err
		}
		statePubs, pos := stateResult.Publications, stateResult.Position

		if e.node.logEnabled(LogLevelTrace) {
			keys := make([]string, 0, len(statePubs))
			for _, pub := range statePubs {
				keys = append(keys, pub.Key)
			}
			e.node.logger.log(newLogEntry(LogLevelTrace, "ensureLoaded from backend", map[string]any{
				"channel":  ch,
				"numPubs":  len(statePubs),
				"position": pos,
				"keys":     keys,
			}))
		}

		// Don't pre-load stream here - it will be loaded fresh from backend
		// when ReadStream is called. This ensures we get the latest stream data
		// during subscription, avoiding races where publications happen between
		// state load and stream read.
		// The stream cache will be populated by pub/sub messages after subscription.
		var streamPubs []*Publication

		// Initialize sync offset to current position
		e.updateSyncOffset(ch, pos.Offset)

		return statePubs, streamPubs, pos, nil
	})
}

// resolveChannelOptions resolves map channel options for a channel.
func (e *CachedMapBroker) resolveChannelOptions(ch string) (MapChannelOptions, error) {
	return resolveAndValidateMapChannelOptions(e.node.config.GetMapChannelOptions, ch)
}

// runSyncLoop runs the background sync scheduler.
// It checks which channels need syncing and dispatches them to workers.
func (e *CachedMapBroker) runSyncLoop() {
	// Start worker pool (SyncConcurrency > 0 = bounded, < 0 = unbounded/no pool)
	if e.conf.SyncConcurrency > 0 {
		for i := 0; i < e.conf.SyncConcurrency; i++ {
			e.wg.Add(1)
			go e.syncWorker()
		}
	}

	// Scheduler tick interval - check for due channels frequently
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	// Less frequent cleanup of stale per-channel state (locks, sync state)
	// for channels evicted from cache by LRU or idle timeout.
	cleanupTicker := time.NewTicker(time.Minute)
	defer cleanupTicker.Stop()

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
		case <-cleanupTicker.C:
			e.cleanupStaleChannelState()
		}
	}
}

// cleanupStaleChannelState removes per-channel locks and sync state
// for channels no longer in cache (evicted by LRU or idle timeout).
func (e *CachedMapBroker) cleanupStaleChannelState() {
	loaded := make(map[string]struct{})
	for _, ch := range e.cache.LoadedChannels() {
		loaded[ch] = struct{}{}
	}

	e.channelLocksMu.Lock()
	for ch := range e.channelLocks {
		if _, ok := loaded[ch]; !ok {
			delete(e.channelLocks, ch)
		}
	}
	e.channelLocksMu.Unlock()

	e.syncStateMu.Lock()
	for ch := range e.syncState {
		if _, ok := loaded[ch]; !ok {
			delete(e.syncState, ch)
		}
	}
	e.syncStateMu.Unlock()

	e.streamInitializedMu.Lock()
	for ch := range e.streamInitialized {
		if _, ok := loaded[ch]; !ok {
			delete(e.streamInitialized, ch)
		}
	}
	e.streamInitializedMu.Unlock()
}

// dispatchDueSyncs finds channels due for sync and dispatches them.
func (e *CachedMapBroker) dispatchDueSyncs() {
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
func (e *CachedMapBroker) syncWorker() {
	defer e.wg.Done()
	for ch := range e.syncWorkerCh {
		e.syncChannel(ch)
	}
}

// jitteredInterval returns the sync interval with random relative jitter.
func (e *CachedMapBroker) jitteredInterval() time.Duration {
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
func (e *CachedMapBroker) syncChannel(ch string) {
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
	syncResult, err := e.backend.ReadStream(ctx, ch, MapReadStreamOptions{
		Filter: StreamFilter{
			Since: &StreamPosition{Offset: since, Epoch: cachedEpoch},
			Limit: e.conf.SyncBatchSize,
		},
	})
	if err != nil {
		if errors.Is(err, ErrorUnrecoverablePosition) {
			e.reloadChannel(ctx, ch)
		}
		// Other sync errors are expected during transient failures, don't log to avoid noise
		return
	}

	// Check for epoch mismatch - backend epoch changed, need full reload
	if cachedEpoch != "" && syncResult.Position.Epoch != "" && cachedEpoch != syncResult.Position.Epoch {
		e.reloadChannel(ctx, ch)
		return
	}

	// Check for gap in stream continuity - if first pub offset > since + 1, we missed publications
	if len(syncResult.Publications) > 0 && since > 0 && syncResult.Publications[0].Offset > since+1 {
		e.reloadChannel(ctx, ch)
		return
	}

	// Apply each publication to cache and deliver to subscribers
	for _, pub := range syncResult.Publications {
		// Use publication's own offset for position, preserving epoch from stream position
		pubPos := StreamPosition{Offset: pub.Offset, Epoch: syncResult.Position.Epoch}
		applied, _ := e.cache.ApplyPublication(ch, pub, pubPos, pub.Removed)

		// Only deliver to subscribers if the publication was actually applied.
		// If skipped (already applied by pub/sub), avoid duplicate delivery.
		if applied && e.eventHandler != nil {
			_ = e.eventHandler.HandlePublication(ch, pub, pubPos, false, nil)
		}
	}

	// Update sync cursor
	if len(syncResult.Publications) > 0 {
		e.updateSyncOffset(ch, syncResult.Publications[len(syncResult.Publications)-1].Offset)
	} else if syncResult.Position.Offset > since {
		// Update cursor even if no new pubs (position advanced)
		e.updateSyncOffset(ch, syncResult.Position.Offset)
	}
}

// reloadChannel evicts the channel from cache and reloads it from backend.
func (e *CachedMapBroker) reloadChannel(ctx context.Context, ch string) {
	// Atomically evict and mark as loading so publications arriving between
	// evict and ensureLoaded are buffered rather than silently dropped.
	e.cache.EvictAndMarkLoading(ch)

	// Clear sync state
	e.syncStateMu.Lock()
	delete(e.syncState, ch)
	e.syncStateMu.Unlock()

	// Clear stream initialized flag so next ReadStream loads fresh from backend
	e.streamInitializedMu.Lock()
	delete(e.streamInitialized, ch)
	e.streamInitializedMu.Unlock()

	// Clean up per-channel lock
	e.channelLocksMu.Lock()
	delete(e.channelLocks, ch)
	e.channelLocksMu.Unlock()

	// Reload from backend
	if err := e.ensureLoaded(ctx, ch); err != nil {
		e.node.logger.log(newErrorLogEntry(err, "error reloading channel", map[string]any{"channel": ch}))
		e.cache.ClearLoading(ch)
	}
}

// getSyncOffset returns the last synced offset for a channel.
func (e *CachedMapBroker) getSyncOffset(ch string) uint64 {
	e.syncStateMu.RLock()
	defer e.syncStateMu.RUnlock()
	if state, ok := e.syncState[ch]; ok {
		return state.lastOffset
	}
	return 0
}

// updateSyncOffset updates the sync offset for a channel.
func (e *CachedMapBroker) updateSyncOffset(ch string, offset uint64) {
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

// underlyingBackend returns the underlying backend MapBroker.
// Useful for advanced operations that bypass the cache.
func (e *CachedMapBroker) underlyingBackend() MapBroker {
	return e.backend
}
