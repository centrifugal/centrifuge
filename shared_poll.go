package centrifuge

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/centrifugal/protocol"
	fdelta "github.com/shadowspore/fossil-delta"
)

// SharedPollManager manages all shared poll channels for a Node.
// One instance per Node. Thread-safe.
type SharedPollManager struct {
	node       *Node
	shutdownCh chan struct{}
	wg         sync.WaitGroup // tracks running refresh workers for graceful shutdown
	sem        chan struct{}   // shared concurrency limiter for backend calls

	mu       sync.RWMutex
	channels map[string]*sharedPollChannelState
}

type sharedPollChannelState struct {
	mu   sync.Mutex // protects all fields below
	opts SharedPollChannelOptions

	// itemIndex: key → tracked entry (version, data).
	itemIndex map[string]*sharedPollTrackedEntry

	// notifCh receives individual key notifications from SharedPollNotify.
	// Buffered, non-blocking send — drops if full.
	notifCh chan string

	// refreshWorker lifecycle.
	workerRunning bool
	workerCancel  context.CancelFunc
	workerCtx     context.Context // checked by track() to detect cancelled worker
	workerGen     uint64          // incremented on each worker start, checked on exit
	// shutdownTimer delays channel cleanup after last item removed.
	shutdownTimer *time.Timer
	removed       bool // set by shutdown timer under mu; track() checks this
}

type sharedPollTrackedEntry struct {
	version             uint64 // last version from backend
	data                []byte // only when KeepLatestData is true
	consecutiveAbsences int    // incremented when item is absent from response
}

func newSharedPollManager(node *Node) *SharedPollManager {
	concurrencyLimit := node.config.SharedPollConcurrencyLimit
	if concurrencyLimit <= 0 {
		concurrencyLimit = 64
	}
	return &SharedPollManager{
		node:       node,
		shutdownCh: make(chan struct{}),
		channels:   make(map[string]*sharedPollChannelState),
		sem:        make(chan struct{}, concurrencyLimit),
	}
}

// track registers an item in the shared poll channel state and ensures
// a refresh worker is running. Hub registration (addSubscriber) is handled
// by the generic keyed layer in handleKeyedTrack — NOT here.
func (m *SharedPollManager) track(channel string, opts SharedPollChannelOptions, key string) {
	// Check global shutdown.
	select {
	case <-m.shutdownCh:
		return
	default:
	}

	// Get or create channel state.
	m.mu.Lock()
	s, ok := m.channels[channel]
	if !ok {
		s = &sharedPollChannelState{
			opts:      opts,
			itemIndex: make(map[string]*sharedPollTrackedEntry),
			notifCh:   make(chan string, 1000),
		}
		m.channels[channel] = s
	}
	m.mu.Unlock()

	s.mu.Lock()
	// Re-check shutdown under state lock.
	select {
	case <-m.shutdownCh:
		s.mu.Unlock()
		return
	default:
	}

	// If this state was removed by shutdown timer, replace it.
	if s.removed {
		s.mu.Unlock()
		m.mu.Lock()
		s = &sharedPollChannelState{
			opts:      opts,
			itemIndex: make(map[string]*sharedPollTrackedEntry),
			notifCh:   make(chan string, 1000),
		}
		m.channels[channel] = s
		m.mu.Unlock()
		s.mu.Lock()
		select {
		case <-m.shutdownCh:
			s.mu.Unlock()
			return
		default:
		}
	}

	// Cancel any pending shutdown timer.
	s.cancelShutdown()

	// Get or create itemIndex entry. New entries start at version=0.
	// Client-provided versions never enter itemIndex.
	if s.itemIndex[key] == nil {
		s.itemIndex[key] = &sharedPollTrackedEntry{}
	}

	// Ensure refresh worker is running.
	startWorker := false
	if !s.workerRunning {
		startWorker = true
	} else if s.workerCtx != nil {
		select {
		case <-s.workerCtx.Done():
			startWorker = true
		default:
		}
	}
	if startWorker {
		s.workerRunning = true
		s.workerGen++
		ctx, cancel := context.WithCancel(context.Background())
		s.workerCancel = cancel
		s.workerCtx = ctx
		gen := s.workerGen
		m.wg.Add(1)
		go s.runRefreshWorker(ctx, m.node, channel, gen, m)
	}
	s.mu.Unlock()
}

// untrack removes an item from shared poll tracking when no connections remain.
func (m *SharedPollManager) untrack(channel string, key string) {
	m.mu.RLock()
	s, ok := m.channels[channel]
	m.mu.RUnlock()
	if !ok {
		return
	}

	hub := m.node.keyedManager.getHub(channel)

	s.mu.Lock()
	// Check if any connections remain for this key.
	if hub != nil && hub.subscriberCount(key) > 0 {
		s.mu.Unlock()
		return
	}
	delete(s.itemIndex, key)
	empty := len(s.itemIndex) == 0
	s.mu.Unlock()

	if empty {
		s.scheduleShutdown(m, channel, s.opts.ChannelShutdownDelay)
	}
}

// notify sends a key notification to the channel's notification channel.
// Non-blocking: drops the notification if the buffer is full.
// Silently ignores unknown channels (channel must already be tracked).
func (m *SharedPollManager) notify(channel string, key string) {
	m.mu.RLock()
	s, ok := m.channels[channel]
	m.mu.RUnlock()
	if !ok {
		return
	}
	// Non-blocking send — drop if full.
	select {
	case s.notifCh <- key:
	default:
	}
}

// hasChannel reports whether the SharedPollManager has state for the given channel.
func (m *SharedPollManager) hasChannel(channel string) bool {
	m.mu.RLock()
	_, ok := m.channels[channel]
	m.mu.RUnlock()
	return ok
}

// close stops all refresh workers and waits for them to finish.
func (m *SharedPollManager) close() {
	close(m.shutdownCh)
	m.mu.Lock()
	for _, ch := range m.channels {
		ch.mu.Lock()
		if ch.workerCancel != nil {
			ch.workerCancel()
		}
		if ch.shutdownTimer != nil {
			ch.shutdownTimer.Stop()
		}
		ch.mu.Unlock()
	}
	m.mu.Unlock()
	m.wg.Wait()
}

// SharedPollRevokeKeys removes items for matching connections. Sends
// Publication{Removed: true} to affected subscribers and cleans up hub + itemIndex.
func (m *SharedPollManager) SharedPollRevokeKeys(channel string, keys []string, users []string, excludeUsers []string) {
	m.mu.RLock()
	s, ok := m.channels[channel]
	m.mu.RUnlock()
	if !ok {
		return
	}

	hub := m.node.keyedManager.getHub(channel)
	if hub == nil {
		return
	}

	// Broadcast removals to affected subscribers.
	for _, key := range keys {
		if len(users) == 0 && len(excludeUsers) == 0 {
			hub.broadcastRemoval(channel, key)
		} else {
			hub.broadcastRemovalToUsers(channel, key, users, excludeUsers)
		}
	}

	// Clean up hub and itemIndex.
	s.mu.Lock()
	for _, key := range keys {
		if len(users) == 0 && len(excludeUsers) == 0 {
			hub.removeAllSubscribers(key)
		} else {
			hub.removeSubscribersForUsers(key, users, excludeUsers)
		}
		// Remove from itemIndex if no subscribers remain.
		if hub.subscriberCount(key) == 0 {
			delete(s.itemIndex, key)
		}
	}
	empty := len(s.itemIndex) == 0
	s.mu.Unlock()

	if empty {
		s.scheduleShutdown(m, channel, s.opts.ChannelShutdownDelay)
	}
}

// scheduleShutdown starts a delayed cleanup timer.
func (s *sharedPollChannelState) scheduleShutdown(m *SharedPollManager, channel string, delay time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.shutdownTimer != nil {
		s.shutdownTimer.Stop()
	}

	if delay <= 0 {
		// Immediate shutdown.
		s.doShutdown(m, channel)
		return
	}

	s.shutdownTimer = time.AfterFunc(delay, func() {
		select {
		case <-m.shutdownCh:
			return
		default:
		}
		s.mu.Lock()
		if len(s.itemIndex) > 0 {
			s.shutdownTimer = nil
			s.mu.Unlock()
			return
		}
		s.doShutdownLocked(m, channel)
		s.mu.Unlock()

		m.mu.Lock()
		if m.channels[channel] == s {
			delete(m.channels, channel)
			m.mu.Unlock()
			m.node.keyedManager.removeChannel(channel)
		} else {
			m.mu.Unlock()
		}
	})
}

// doShutdown performs shutdown under s.mu. For immediate shutdown (no delay).
func (s *sharedPollChannelState) doShutdown(m *SharedPollManager, channel string) {
	if s.workerCancel != nil {
		s.workerCancel()
	}
	s.removed = true
	s.shutdownTimer = nil
	// Release s.mu before acquiring m.mu (lock ordering).
	s.mu.Unlock()

	m.mu.Lock()
	if m.channels[channel] == s {
		delete(m.channels, channel)
		m.mu.Unlock()
		m.node.keyedManager.removeChannel(channel)
	} else {
		m.mu.Unlock()
	}

	// Re-acquire s.mu since caller expects it held (deferred unlock).
	s.mu.Lock()
}

// doShutdownLocked performs shutdown steps under s.mu (for timer callback).
func (s *sharedPollChannelState) doShutdownLocked(m *SharedPollManager, channel string) {
	if s.workerCancel != nil {
		s.workerCancel()
	}
	s.removed = true
	s.shutdownTimer = nil
}

// cancelShutdown stops a pending shutdown timer. Caller must hold s.mu.
func (s *sharedPollChannelState) cancelShutdown() {
	if s.shutdownTimer != nil {
		s.shutdownTimer.Stop()
		s.shutdownTimer = nil
	}
}

func (s *sharedPollChannelState) runRefreshWorker(ctx context.Context, node *Node, channel string, gen uint64, m *SharedPollManager) {
	defer m.wg.Done()
	defer func() {
		s.mu.Lock()
		if s.workerGen == gen {
			s.workerRunning = false
		}
		s.mu.Unlock()
	}()

	interval := s.opts.RefreshInterval
	if interval <= 0 {
		interval = 10 * time.Second
	}
	timer := time.NewTimer(interval)
	defer timer.Stop()

	// Notification batching state.
	batchMaxSize := s.opts.NotificationBatchMaxSize
	batchMaxDelay := s.opts.NotificationBatchMaxDelay
	// When size-based batching is configured without a delay, use the
	// refresh interval as the cap so notifications don't sit indefinitely.
	if batchMaxSize > 0 && batchMaxDelay <= 0 {
		batchMaxDelay = interval
	}
	batchEnabled := batchMaxSize > 0 || batchMaxDelay > 0
	var batchKeys map[string]struct{}
	var batchTimer *time.Timer
	var batchTimerC <-chan time.Time
	defer func() {
		if batchTimer != nil {
			batchTimer.Stop()
		}
	}()

	fireBatch := func() {
		if len(batchKeys) == 0 {
			return
		}
		keys := make([]string, 0, len(batchKeys))
		for k := range batchKeys {
			keys = append(keys, k)
		}
		batchKeys = nil
		if batchTimer != nil {
			batchTimer.Stop()
			batchTimerC = nil
		}
		s.runNotifiedRefreshCycle(ctx, node, channel, keys, m.sem)
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			cycleStart := time.Now()
			spreadDelay := s.runRefreshCycle(ctx, node, channel, m.sem)
			cycleDuration := time.Since(cycleStart)
			if s.opts.RefreshIntervalFn != nil {
				// Pass work time (excluding intentional spread delays)
				// so backpressure reacts to actual backend load.
				workTime := cycleDuration - spreadDelay
				if workTime < 0 {
					workTime = 0
				}
				interval = s.opts.RefreshIntervalFn(workTime)
				if interval <= 0 {
					interval = s.opts.RefreshInterval
					if interval <= 0 {
						interval = 10 * time.Second
					}
				}
			}
			// Period-based timer: subtract cycle duration so the
			// period stays close to the configured interval.
			remaining := interval - cycleDuration
			if remaining < 0 {
				remaining = 0
			}
			timer.Reset(remaining)
		case key := <-s.notifCh:
			if !batchEnabled {
				// No batching — fire immediately.
				s.runNotifiedRefreshCycle(ctx, node, channel, []string{key}, m.sem)
				continue
			}
			if batchKeys == nil {
				batchKeys = make(map[string]struct{})
			}
			batchKeys[key] = struct{}{}
			if batchMaxSize > 0 && len(batchKeys) >= batchMaxSize {
				fireBatch()
			} else if batchMaxDelay > 0 && batchTimerC == nil {
				batchTimer = time.NewTimer(batchMaxDelay)
				batchTimerC = batchTimer.C
			}
		case <-batchTimerC:
			fireBatch()
		}
	}
}

// runNotifiedRefreshCycle runs an immediate backend poll for just the notified keys.
// Unlike runRefreshCycle, there's no spread delay or chunking — the batch is already bounded.
func (s *sharedPollChannelState) runNotifiedRefreshCycle(ctx context.Context, node *Node, channel string, keys []string, sem chan struct{}) {
	// Filter to keys still in itemIndex.
	s.mu.Lock()
	filtered := keys[:0]
	for _, k := range keys {
		if _, ok := s.itemIndex[k]; ok {
			filtered = append(filtered, k)
		}
	}
	s.mu.Unlock()
	if len(filtered) == 0 {
		return
	}

	hub := node.keyedManager.getHub(channel)
	if hub == nil {
		return
	}

	handler := node.clientEvents.sharedPollHandler
	if handler == nil {
		return
	}

	callTimeout := s.opts.CallTimeout
	if callTimeout <= 0 {
		callTimeout = 30 * time.Second
	}

	// Acquire semaphore.
	select {
	case sem <- struct{}{}:
	case <-ctx.Done():
		return
	}
	defer func() { <-sem }()

	// Build event items.
	items := make([]SharedPollItem, len(filtered))
	if s.opts.RefreshMode == SharedPollRefreshModeDiff {
		s.mu.Lock()
		for i, key := range filtered {
			items[i] = SharedPollItem{Key: key}
			if entry, ok := s.itemIndex[key]; ok {
				items[i].Version = entry.version
			}
		}
		s.mu.Unlock()
	} else {
		for i, key := range filtered {
			items[i] = SharedPollItem{Key: key}
		}
	}

	callCtx, cancel := context.WithTimeout(ctx, callTimeout)
	result, err := handler(callCtx, SharedPollEvent{
		Channel: channel,
		Items:   items,
	})
	cancel()

	if err != nil {
		if node.logEnabled(LogLevelWarn) {
			node.logger.log(newLogEntry(LogLevelWarn, "shared poll notified refresh error", map[string]any{
				"channel": channel,
				"error":   err.Error(),
			}))
		}
		return
	}

	// Notified refresh does not track absences — absence tracking is only
	// meaningful for full-channel timer-based polls.
	s.onNotifiedRefreshResponse(channel, result.Items, hub)
}

func (s *sharedPollChannelState) runRefreshCycle(ctx context.Context, node *Node, channel string, sem chan struct{}) time.Duration {
	// 1. Collect all item keys from itemIndex.
	s.mu.Lock()
	if len(s.itemIndex) == 0 {
		s.mu.Unlock()
		return 0
	}
	keys := make([]string, 0, len(s.itemIndex))
	for k := range s.itemIndex {
		keys = append(keys, k)
	}
	s.mu.Unlock()

	sort.Strings(keys)

	batchSize := s.opts.RefreshBatchSize
	if batchSize <= 0 {
		batchSize = 1000
	}

	// Split into chunks.
	chunks := make([][]string, 0, (len(keys)+batchSize-1)/batchSize)
	for i := 0; i < len(keys); i += batchSize {
		end := i + batchSize
		if end > len(keys) {
			end = len(keys)
		}
		chunks = append(chunks, keys[i:end])
	}

	hub := node.keyedManager.getHub(channel)
	if hub == nil {
		return 0
	}

	callTimeout := s.opts.CallTimeout
	if callTimeout <= 0 {
		callTimeout = 30 * time.Second
	}

	handler := node.clientEvents.sharedPollHandler
	if handler == nil {
		return 0
	}

	// Spread chunk dispatches evenly over the refresh interval to avoid
	// bursting all backend calls at once. The delay between dispatches is
	// interval / num_chunks. The semaphore still limits actual concurrency.
	var dispatchDelay time.Duration
	if len(chunks) > 1 {
		interval := s.opts.RefreshInterval
		if interval <= 0 {
			interval = 10 * time.Second
		}
		dispatchDelay = interval / time.Duration(len(chunks))
	}

	var wg sync.WaitGroup

	for i, chunk := range chunks {
		if i > 0 && dispatchDelay > 0 {
			select {
			case <-ctx.Done():
				break
			case <-time.After(dispatchDelay):
			}
		}

		sem <- struct{}{}
		wg.Add(1)
		go func(chunk []string) {
			defer func() {
				<-sem
				wg.Done()
			}()

			// Build event items.
			items := make([]SharedPollItem, len(chunk))
			if s.opts.RefreshMode == SharedPollRefreshModeDiff {
				s.mu.Lock()
				for i, key := range chunk {
					items[i] = SharedPollItem{Key: key}
					if entry, ok := s.itemIndex[key]; ok {
						items[i].Version = entry.version
					}
				}
				s.mu.Unlock()
			} else {
				for i, key := range chunk {
					items[i] = SharedPollItem{Key: key}
				}
			}

			callCtx, cancel := context.WithTimeout(ctx, callTimeout)
			result, err := handler(callCtx, SharedPollEvent{
				Channel: channel,
				Items:   items,
			})
			cancel()

			if err != nil {
				if node.logEnabled(LogLevelWarn) {
					node.logger.log(newLogEntry(LogLevelWarn, "shared poll refresh error", map[string]any{
						"channel": channel,
						"error":   err.Error(),
					}))
				}
				return
			}

			s.onRefreshResponse(channel, chunk, result.Items, hub)
		}(chunk)
	}

	wg.Wait()

	// Return total intentional spread delay so callers (e.g. backpressure)
	// can distinguish work time from spread time.
	totalSpreadDelay := time.Duration(len(chunks)-1) * dispatchDelay
	return totalSpreadDelay
}

type pendingBroadcast struct {
	key     string
	version uint64
	pub     *protocol.Publication
	prep    preparedData
	removal bool
}

// buildPreparedPollData computes delta data for a shared poll publication.
// If prevData is available, computes a fossil delta patch; otherwise returns
// an empty preparedData (no delta). The actual per-client encoding (JSON escaping,
// protocol framing) is done lazily in keyedWritePublication.
func buildPreparedPollData(pub *protocol.Publication, prevData []byte) preparedData {
	if len(prevData) == 0 {
		return preparedData{}
	}
	patch := fdelta.Create(prevData, pub.Data)
	isReal := len(patch) < len(pub.Data)
	deltaData := patch
	if !isReal {
		deltaData = pub.Data
	}
	return preparedData{
		deltaSub:         true,
		keyedDeltaPatch:  deltaData,
		keyedDeltaIsReal: isReal,
	}
}

// onNotifiedRefreshResponse processes backend response for notified keys.
// Unlike onRefreshResponse, it does not track absences since this is a targeted poll.
func (s *sharedPollChannelState) onNotifiedRefreshResponse(channel string, items []SharedPollRefreshItem, hub *keyedHub) {
	type pendingUpdate struct {
		key      string
		version  uint64
		data     []byte
		prevData []byte
	}

	var updates []pendingUpdate
	var removals []string

	s.mu.Lock()

	for _, e := range items {
		if e.Removed {
			removals = append(removals, e.Key)
			continue
		}

		entry := s.itemIndex[e.Key]
		if entry == nil {
			continue
		}

		if e.Version <= entry.version {
			if s.opts.KeepLatestData {
				entry.data = e.Data
			}
			continue
		}

		var prevData []byte
		if s.opts.KeepLatestData {
			prevData = entry.data
			entry.data = e.Data
		}
		if len(e.PrevData) > 0 {
			prevData = e.PrevData
		}
		entry.version = e.Version

		updates = append(updates, pendingUpdate{
			key: e.Key, version: e.Version, data: e.Data, prevData: prevData,
		})
	}

	s.mu.Unlock()

	// Build publications outside the lock.
	broadcasts := make([]pendingBroadcast, 0, len(updates)+len(removals))
	for _, u := range updates {
		pub := &protocol.Publication{Key: u.key, Data: u.data, Version: u.version}
		prep := buildPreparedPollData(pub, u.prevData)
		broadcasts = append(broadcasts, pendingBroadcast{key: u.key, version: u.version, pub: pub, prep: prep})
	}
	for _, key := range removals {
		broadcasts = append(broadcasts, pendingBroadcast{key: key, removal: true})
	}

	// Fan out outside the lock.
	for _, b := range broadcasts {
		if b.removal {
			hub.broadcastRemoval(channel, b.key)
		} else {
			hub.broadcastToKey(channel, b.key, b.version, b.pub, b.prep)
		}
	}

	// Clean up removed items from hub and itemIndex.
	var removedKeys []string
	for _, b := range broadcasts {
		if b.removal {
			removedKeys = append(removedKeys, b.key)
		}
	}
	if len(removedKeys) > 0 {
		s.mu.Lock()
		for _, key := range removedKeys {
			hub.removeAllSubscribers(key)
			delete(s.itemIndex, key)
		}
		s.mu.Unlock()
	}
}

func (s *sharedPollChannelState) onRefreshResponse(channel string, queriedKeys []string, items []SharedPollRefreshItem, hub *keyedHub) {
	type pendingUpdate struct {
		key      string
		version  uint64
		data     []byte
		prevData []byte
	}

	var updates []pendingUpdate
	var removals []string

	s.mu.Lock()

	for _, e := range items {
		if e.Removed {
			removals = append(removals, e.Key)
			continue
		}

		entry := s.itemIndex[e.Key]
		if entry == nil {
			continue
		}

		if e.Version <= entry.version {
			if s.opts.KeepLatestData {
				entry.data = e.Data
			}
			continue
		}

		// Capture prevData before updating — this is the delta base.
		var prevData []byte
		if s.opts.KeepLatestData {
			prevData = entry.data
			entry.data = e.Data
		}
		if len(e.PrevData) > 0 {
			prevData = e.PrevData
		}
		entry.version = e.Version

		updates = append(updates, pendingUpdate{
			key: e.Key, version: e.Version, data: e.Data, prevData: prevData,
		})
	}

	// Track absent items.
	maxAbsences := s.opts.MaxConsecutiveAbsences
	if maxAbsences <= 0 {
		maxAbsences = 2
	}
	respondedKeys := make(map[string]bool, len(items))
	for _, e := range items {
		respondedKeys[e.Key] = true
	}
	for _, key := range queriedKeys {
		entry := s.itemIndex[key]
		if entry == nil {
			continue
		}
		if respondedKeys[key] {
			entry.consecutiveAbsences = 0
			continue
		}
		entry.consecutiveAbsences++
		if entry.consecutiveAbsences >= maxAbsences {
			removals = append(removals, key)
		}
	}

	s.mu.Unlock()

	// Build publications outside the lock.
	broadcasts := make([]pendingBroadcast, 0, len(updates)+len(removals))
	for _, u := range updates {
		pub := &protocol.Publication{Key: u.key, Data: u.data, Version: u.version}
		prep := buildPreparedPollData(pub, u.prevData)
		broadcasts = append(broadcasts, pendingBroadcast{key: u.key, version: u.version, pub: pub, prep: prep})
	}
	for _, key := range removals {
		broadcasts = append(broadcasts, pendingBroadcast{key: key, removal: true})
	}

	// Fan out outside the lock.
	for _, b := range broadcasts {
		if b.removal {
			hub.broadcastRemoval(channel, b.key)
		} else {
			hub.broadcastToKey(channel, b.key, b.version, b.pub, b.prep)
		}
	}

	// Clean up removed items from hub and itemIndex.
	var removedKeys []string
	for _, b := range broadcasts {
		if b.removal {
			removedKeys = append(removedKeys, b.key)
		}
	}
	if len(removedKeys) > 0 {
		s.mu.Lock()
		for _, key := range removedKeys {
			hub.removeAllSubscribers(key)
			delete(s.itemIndex, key)
		}
		s.mu.Unlock()
	}
}
