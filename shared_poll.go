package centrifuge

import (
	"bytes"
	"context"
	"errors"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/centrifugal/centrifuge/internal/epoch"
	"github.com/centrifugal/protocol"
	"github.com/cespare/xxhash/v2"
	fdelta "github.com/shadowspore/fossil-delta"
)

// notifChCapacity bounds the per-channel notification queue used by
// SharedPollManager.notify. Each entry is one tracked-key notification waiting
// to be batched into the next backend poll. Sized to absorb typical bursts
// (large reconnect waves, cache-invalidation storms). When the buffer is full,
// extra notifications are dropped and counted by the droppedNotifyCount metric
// — the next timer-based poll cycle covers anything that was lost.
const notifChCapacity = 4096

// SharedPollManager manages all shared poll channels for a Node.
// One instance per Node. Thread-safe.
type SharedPollManager struct {
	node       *Node
	shutdownCh chan struct{}
	wg         sync.WaitGroup // tracks running refresh workers for graceful shutdown
	sem        chan struct{}  // shared concurrency limiter for backend calls
	epoch      string         // random string generated at startup, used for versionless mode

	mu       sync.RWMutex
	channels map[string]*sharedPollChannelState

	brokerSubMu    sync.RWMutex
	brokerSubChans map[string]Broker              // key-channel → subscribed broker
	brokerSubKeys  map[string]map[string]struct{} // base channel → set of key-channels
}

type sharedPollChannelState struct {
	mu sync.Mutex // protects all fields below

	// opts is captured at first track for this channel state and treated as
	// immutable for the lifetime of the state. The refresh worker reads opts
	// (RefreshInterval, batching, KeepLatestData, versioned/versionless mode, …)
	// without re-resolving — runtime config changes via GetSharedPollChannelOptions
	// only take effect after the channel state fully drains and is recreated
	// (i.e., after all subscribers leave and the shutdown delay expires).
	opts SharedPollChannelOptions

	// epoch is a random string generated when this channel state is created.
	// Clients compare epochs on reconnect and reset stored versions when
	// the epoch changes. This covers channel state recreation (e.g., after
	// shutdown delay expires and all items are cleaned up) without requiring
	// a full server restart.
	epoch string

	// itemIndex: key → tracked entry (version, data).
	itemIndex map[string]*sharedPollTrackedEntry

	// versionCounter is a monotonic counter for synthetic versions (versionless mode).
	versionCounter uint64

	// notifCh receives individual key notifications from SharedPollNotify.
	// Buffered, non-blocking send — drops if full and increments the
	// droppedNotifyCount metric. Capacity is sized for typical bursts; under
	// extreme load drops are surfaced via the metric and the next timer-based
	// poll closes the gap.
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
	version          uint64 // last version from backend (or synthetic in versionless mode)
	data             []byte // only when KeepLatestData is true
	dataHash         uint64 // xxhash64 hash of data, only used in versionless mode
	freshFromPublish bool   // set by SharedPollPublish, cleared each timer poll cycle
	needsBroadcast   bool   // set when a version=0 subscriber joins an existing key; cleared after broadcast
}

func xxHash64(data []byte) uint64 {
	return xxhash.Sum64(data)
}

// sharedPollKeyChannel builds a PUB/SUB channel name scoped to a specific key.
// Format: "<len>:<channel><key>" — length-prefix encoding, safe for any broker
// (no null bytes, no special characters in the framing).
func sharedPollKeyChannel(channel, key string) string {
	return strconv.Itoa(len(channel)) + ":" + channel + key
}

// parseSharedPollKeyChannel splits a key-scoped PUB/SUB channel into base channel and key.
// Returns ("", "") if the channel is not in the expected length-prefix format.
func parseSharedPollKeyChannel(keyCh string) (string, string) {
	i := strings.IndexByte(keyCh, ':')
	if i <= 0 {
		return "", ""
	}
	n, err := strconv.Atoi(keyCh[:i])
	if err != nil || n < 0 || i+1+n > len(keyCh) {
		return "", ""
	}
	return keyCh[i+1 : i+1+n], keyCh[i+1+n:]
}

func newSharedPollManager(node *Node) *SharedPollManager {
	concurrencyLimit := node.config.SharedPoll.ConcurrencyLimit
	if concurrencyLimit <= 0 {
		concurrencyLimit = 64
	}
	return &SharedPollManager{
		node:           node,
		shutdownCh:     make(chan struct{}),
		channels:       make(map[string]*sharedPollChannelState),
		sem:            make(chan struct{}, concurrencyLimit),
		brokerSubChans: make(map[string]Broker),
		brokerSubKeys:  make(map[string]map[string]struct{}),
		epoch:          epoch.Generate(),
	}
}

// track registers an item in the shared poll channel state and ensures
// a refresh worker is running. Hub registration (addSubscriber) is handled
// by the generic keyed layer in handleTrack — NOT here.
func (m *SharedPollManager) track(channel string, opts SharedPollChannelOptions, key string) (bool, uint64, error) {
	// Check global shutdown.
	select {
	case <-m.shutdownCh:
		return false, 0, nil
	default:
	}

	// Get or create channel state.
	m.mu.Lock()
	s, ok := m.channels[channel]
	if !ok {
		s = &sharedPollChannelState{
			opts:      opts,
			epoch:     initialChannelEpoch(opts),
			itemIndex: make(map[string]*sharedPollTrackedEntry),
			notifCh:   make(chan string, notifChCapacity),
		}
		m.channels[channel] = s
	}
	m.mu.Unlock()

	s.mu.Lock()
	// Re-check shutdown under state lock.
	select {
	case <-m.shutdownCh:
		s.mu.Unlock()
		return false, 0, nil
	default:
	}

	// If this state was removed by shutdown timer, replace it and loop
	// until we hold the lock on a non-removed state.
	//
	// Re-check m.channels[channel] under m.mu and reuse any fresh state a
	// concurrent caller installed — an unconditional overwrite would
	// orphan that caller's worker (nothing outside m.channels keeps a
	// reference for cancellation, so the lost worker would stay parked in
	// select with an uncancelled context, and m.close()'s wg.Wait would
	// hang forever).
	//
	// Loop, because the cur we ended up with may itself be removed by a
	// third goroutine between when it was placed and when we acquire
	// cur.mu — that would otherwise fall through to add a key + restart
	// the worker on an about-to-be-finalized state, leaking the new
	// worker the same way.
	for s.removed {
		s.mu.Unlock()
		m.mu.Lock()
		cur, ok := m.channels[channel]
		if !ok || cur == s {
			cur = &sharedPollChannelState{
				opts:      opts,
				epoch:     initialChannelEpoch(opts),
				itemIndex: make(map[string]*sharedPollTrackedEntry),
				notifCh:   make(chan string, notifChCapacity),
			}
			m.channels[channel] = cur
		}
		s = cur
		m.mu.Unlock()
		s.mu.Lock()
		select {
		case <-m.shutdownCh:
			s.mu.Unlock()
			return false, 0, nil
		default:
		}
	}

	// Cancel any pending shutdown timer.
	s.cancelShutdown()

	// Get or create itemIndex entry. New entries start at version=0.
	// Client-provided versions never enter itemIndex.
	isNewKey := s.itemIndex[key] == nil
	if isNewKey {
		s.itemIndex[key] = &sharedPollTrackedEntry{}
	}
	entryVersion := s.itemIndex[key].version

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

	if isNewKey && opts.PublishEnabled {
		if err := m.subscribeToBrokerKeys(channel, []string{key}); err != nil {
			// Broker subscribe failed — clean up our key. Only tear down the channel
			// if no other goroutine added keys in the meantime.
			s.mu.Lock()
			delete(s.itemIndex, key)
			empty := len(s.itemIndex) == 0
			if empty && s.workerCancel != nil {
				s.workerCancel()
			}
			s.mu.Unlock()
			if empty {
				m.mu.Lock()
				// Verify the channel wasn't replaced by another goroutine.
				if m.channels[channel] == s {
					delete(m.channels, channel)
				}
				m.mu.Unlock()
			}
			return false, 0, err
		}
	}

	return isNewKey, entryVersion, nil
}

// trackKeyResult holds the outcome for a single key tracked via trackKeys.
type trackKeyResult struct {
	isNew        bool
	entryVersion uint64
}

// trackKeys registers multiple items in the shared poll channel state in one batch.
// It subscribes to all new keys with a single broker.Subscribe call.
func (m *SharedPollManager) trackKeys(channel string, opts SharedPollChannelOptions, keys []string) ([]trackKeyResult, error) {
	if len(keys) == 0 {
		return nil, nil
	}

	// Check global shutdown.
	select {
	case <-m.shutdownCh:
		return make([]trackKeyResult, len(keys)), nil
	default:
	}

	// Get or create channel state.
	m.mu.Lock()
	s, ok := m.channels[channel]
	if !ok {
		s = &sharedPollChannelState{
			opts:      opts,
			epoch:     initialChannelEpoch(opts),
			itemIndex: make(map[string]*sharedPollTrackedEntry),
			notifCh:   make(chan string, notifChCapacity),
		}
		m.channels[channel] = s
	}
	m.mu.Unlock()

	s.mu.Lock()
	// Re-check shutdown under state lock.
	select {
	case <-m.shutdownCh:
		s.mu.Unlock()
		return make([]trackKeyResult, len(keys)), nil
	default:
	}

	// If this state was removed by shutdown timer, replace it. See the
	// matching loop in track() for the race rationale: both an
	// unconditional overwrite and a single-shot re-check would still
	// orphan a worker — a fresh state we land on may itself be in the
	// process of being shut down by a third goroutine.
	for s.removed {
		s.mu.Unlock()
		m.mu.Lock()
		cur, ok := m.channels[channel]
		if !ok || cur == s {
			cur = &sharedPollChannelState{
				opts:      opts,
				epoch:     initialChannelEpoch(opts),
				itemIndex: make(map[string]*sharedPollTrackedEntry),
				notifCh:   make(chan string, notifChCapacity),
			}
			m.channels[channel] = cur
		}
		s = cur
		m.mu.Unlock()
		s.mu.Lock()
		select {
		case <-m.shutdownCh:
			s.mu.Unlock()
			return make([]trackKeyResult, len(keys)), nil
		default:
		}
	}

	// Cancel any pending shutdown timer.
	s.cancelShutdown()

	// Register all keys and collect results + new keys list.
	results := make([]trackKeyResult, len(keys))
	var newKeys []string
	for i, key := range keys {
		isNewKey := s.itemIndex[key] == nil
		if isNewKey {
			s.itemIndex[key] = &sharedPollTrackedEntry{}
			newKeys = append(newKeys, key)
		}
		results[i] = trackKeyResult{
			isNew:        isNewKey,
			entryVersion: s.itemIndex[key].version,
		}
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

	if len(newKeys) > 0 && opts.PublishEnabled {
		if err := m.subscribeToBrokerKeys(channel, newKeys); err != nil {
			// Broker subscribe failed — clean up new keys.
			s.mu.Lock()
			for _, key := range newKeys {
				delete(s.itemIndex, key)
			}
			empty := len(s.itemIndex) == 0
			if empty && s.workerCancel != nil {
				s.workerCancel()
			}
			s.mu.Unlock()
			if empty {
				m.mu.Lock()
				if m.channels[channel] == s {
					delete(m.channels, channel)
				}
				m.mu.Unlock()
			}
			return nil, err
		}
	}

	return results, nil
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
	publishEnabled := s.opts.PublishEnabled
	s.mu.Unlock()

	if publishEnabled {
		m.unsubscribeFromBrokerKeys(channel, []string{key})
	}
	if empty {
		s.scheduleShutdown(m, channel, s.opts.ChannelShutdownDelay)
	}
}

// warmKeyData holds cached data for a warm key that can be delivered directly.
type warmKeyData struct {
	key             string
	internalVersion uint64                // synthetic/real version for per-connection dedup
	pub             *protocol.Publication // publication with wire version
}

// getWarmKeyData returns cached data for warm keys. Only returns entries when
// KeepLatestData is enabled and the entry has data (version > 0).
func (m *SharedPollManager) getWarmKeyData(channel string, keys []string) []warmKeyData {
	m.mu.RLock()
	s, ok := m.channels[channel]
	m.mu.RUnlock()
	if !ok {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.opts.KeepLatestData || s.opts.isVersionless() {
		return nil
	}
	var result []warmKeyData
	for _, key := range keys {
		entry := s.itemIndex[key]
		if entry == nil || entry.version == 0 || entry.data == nil {
			continue
		}
		result = append(result, warmKeyData{
			key:             key,
			internalVersion: entry.version,
			pub: &protocol.Publication{
				Key:     key,
				Data:    entry.data,
				Version: entry.version,
			},
		})
	}
	return result
}

// markNeedsBroadcast flags existing keys so polls re-broadcast their data even
// if unchanged. Also triggers a notify for keys that have data (version > 0) and
// were not already flagged — this provides near-immediate delivery via the
// notification fast path instead of waiting for the next timer poll. The
// at-most-once notify per key ensures mass reconnect (1000 clients, same key)
// causes only one backend call per "wave", not one per client.
//
// Must be called AFTER addSubscriber so that broadcasts from the triggered notify
// (or a concurrent timer poll) can reach the subscribing client.
func (m *SharedPollManager) markNeedsBroadcast(channel string, keys []string) {
	m.mu.RLock()
	s, ok := m.channels[channel]
	m.mu.RUnlock()
	if !ok {
		return
	}
	s.mu.Lock()
	var toNotify []string
	for _, key := range keys {
		if entry := s.itemIndex[key]; entry != nil {
			if !entry.needsBroadcast {
				entry.needsBroadcast = true
				// Only notify keys that already have data — version=0 keys are
				// being handled by an in-flight cold key notify.
				if entry.version > 0 {
					toNotify = append(toNotify, key)
				}
			}
		}
	}
	s.mu.Unlock()

	// Notify outside the lock — triggers backend call for near-immediate delivery.
	// Keys are combined by the worker's batch dedup when batching is configured.
	for _, key := range toNotify {
		m.notify(channel, key)
	}
}

// notify sends a key notification to the channel's notification channel.
// Non-blocking: drops the notification if the buffer is full.
// Silently ignores unknown channels (channel must already be tracked).
//
// The read lock is held across the send so that a concurrent shutdown cannot
// replace m.channels[channel] with a fresh state while we send into the now-
// orphaned notifCh (whose worker has been cancelled and will never drain it).
// The send is non-blocking (select with default) and the metric increments are
// cheap, so holding the RLock here does not introduce contention.
func (m *SharedPollManager) notify(channel string, key string) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	s, ok := m.channels[channel]
	if !ok {
		return
	}
	// Non-blocking send — drop if full.
	select {
	case s.notifCh <- key:
		m.node.metrics.getSharedPollChannelCached(channel).notifyCount.Inc()
	default:
		m.node.metrics.getSharedPollChannelCached(channel).droppedNotifyCount.Inc()
	}
}

// getCachedData returns cached publications for items where the server has a newer
// version than the client. Returns nil when nothing to return (omitted from protobuf).
// Only returns data when KeepLatestData is enabled for the channel.
func (m *SharedPollManager) getCachedData(channel string, items []TrackItem) []*protocol.Publication {
	m.mu.RLock()
	s, ok := m.channels[channel]
	m.mu.RUnlock()
	if !ok {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.opts.KeepLatestData || s.opts.isVersionless() {
		return nil
	}
	var pubs []*protocol.Publication
	for _, item := range items {
		entry := s.itemIndex[item.Key]
		if entry == nil || entry.version == 0 || entry.data == nil {
			continue
		}
		if entry.version <= item.Version {
			continue
		}
		pubs = append(pubs, &protocol.Publication{
			Key:     item.Key,
			Data:    entry.data,
			Version: entry.version,
		})
	}
	return pubs
}

// hasChannel reports whether the SharedPollManager has state for the given channel.
func (m *SharedPollManager) hasChannel(channel string) bool {
	m.mu.RLock()
	_, ok := m.channels[channel]
	m.mu.RUnlock()
	return ok
}

// initialChannelEpoch returns the epoch a freshly-created sharedPollChannelState
// starts with. For versionless mode the server generates a per-channel epoch
// (changes when state is recreated). For versioned mode the epoch is supplied
// by the publisher on each publish/refresh, so initial state is empty and
// gets populated via flipEpoch on the first incoming publish.
func initialChannelEpoch(opts SharedPollChannelOptions) string {
	if opts.isVersionless() {
		return epoch.Generate()
	}
	return ""
}

// Epoch returns the epoch string for subscribe replies.
//
// Versionless channels: server-generated, stable for the lifetime of channel
// state. Changes when state is recreated after shutdown delay expires.
//
// Versioned channels: publisher-supplied, set on first publish/refresh.
// Changes (epoch flip) trigger unsubscribe of all current subscribers with
// insufficient-state code so they re-track from version 0 on resubscribe.
func (m *SharedPollManager) Epoch(channel string, isVersionless bool) string {
	m.mu.RLock()
	s, ok := m.channels[channel]
	m.mu.RUnlock()
	if !ok {
		if isVersionless {
			return m.epoch
		}
		return ""
	}
	s.mu.Lock()
	e := s.epoch
	s.mu.Unlock()
	return e
}

// flipEpochAndCollectClients atomically updates the stored channel epoch,
// resets all per-key state, and collects the *Client references currently
// subscribed (via the keyed hub). Caller must invoke Client.Unsubscribe on
// each returned client with no locks held — calling Unsubscribe under
// s.mu would deadlock since Unsubscribe's cleanup path takes s.mu via
// untrack.
//
// All state mutation (epoch + entries) and the client snapshot are taken
// under s.mu to prevent a window where a client subscribing concurrently
// with the flip could be unsubbed despite having seen the new epoch in
// its subscribe reply.
//
// If newEpoch == s.epoch, returns nil and performs no mutation.
func (s *sharedPollChannelState) flipEpochAndCollectClients(hub *keyedHub, newEpoch string) []*Client {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.epoch == newEpoch {
		return nil
	}
	s.epoch = newEpoch
	for _, e := range s.itemIndex {
		e.version = 0
		e.data = nil
		e.dataHash = 0
		e.freshFromPublish = false
		e.needsBroadcast = false
	}
	if hub == nil {
		return nil
	}
	// Nested s.mu -> h.mu is consistent with existing patterns in this file
	// (e.g. SharedPollRevokeKeys uses removeAllSubscribers under s.mu).
	return hub.collectAllClients()
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

	// Unsubscribe all broker subscriptions, batched by broker.
	m.brokerSubMu.Lock()
	brokerChannels := make(map[Broker][]string)
	for kc, broker := range m.brokerSubChans {
		brokerChannels[broker] = append(brokerChannels[broker], kc)
	}
	for broker, channels := range brokerChannels {
		if err := broker.Unsubscribe(channels...); err != nil {
			m.node.logger.log(newLogEntry(LogLevelError, "error unsubscribing from broker for shared poll", map[string]any{"error": err.Error()}))
		}
	}
	m.brokerSubChans = make(map[string]Broker)
	m.brokerSubKeys = make(map[string]map[string]struct{})
	m.brokerSubMu.Unlock()
}

func (m *SharedPollManager) subscribeToBrokerKeys(channel string, keys []string) error {
	if len(keys) == 0 {
		return nil
	}
	broker := m.node.getBroker(channel)
	keyChannels := make([]string, len(keys))
	for i, key := range keys {
		keyChannels[i] = sharedPollKeyChannel(channel, key)
	}
	m.node.metrics.incActionCount("broker_subscribe", channel)
	if err := broker.Subscribe(keyChannels...); err != nil {
		return err
	}
	m.brokerSubMu.Lock()
	if m.brokerSubKeys[channel] == nil {
		m.brokerSubKeys[channel] = make(map[string]struct{})
	}
	for _, kc := range keyChannels {
		m.brokerSubChans[kc] = broker
		m.brokerSubKeys[channel][kc] = struct{}{}
	}
	m.brokerSubMu.Unlock()
	return nil
}

func (m *SharedPollManager) unsubscribeFromBrokerKeys(channel string, keys []string) {
	if len(keys) == 0 {
		return
	}
	keyChannels := make([]string, len(keys))
	for i, key := range keys {
		keyChannels[i] = sharedPollKeyChannel(channel, key)
	}
	_ = m.node.subDissolver.Submit(func() error {
		m.node.metrics.incActionCount("broker_unsubscribe", channel)
		// Filter out keys that were re-tracked between queueing and execution.
		m.mu.RLock()
		s, chExists := m.channels[channel]
		m.mu.RUnlock()
		var toUnsub []string
		if chExists {
			s.mu.Lock()
			for i, kc := range keyChannels {
				if _, tracked := s.itemIndex[keys[i]]; !tracked {
					toUnsub = append(toUnsub, kc)
				}
			}
			s.mu.Unlock()
		} else {
			toUnsub = keyChannels
		}
		if len(toUnsub) == 0 {
			return nil
		}
		m.brokerSubMu.RLock()
		broker, ok := m.brokerSubChans[toUnsub[0]]
		m.brokerSubMu.RUnlock()
		if !ok {
			return nil
		}
		if err := broker.Unsubscribe(toUnsub...); err != nil {
			time.Sleep(500 * time.Millisecond)
			return err
		}
		m.brokerSubMu.Lock()
		for _, kc := range toUnsub {
			delete(m.brokerSubChans, kc)
		}
		if ks, ok := m.brokerSubKeys[channel]; ok {
			for _, kc := range toUnsub {
				delete(ks, kc)
			}
			if len(ks) == 0 {
				delete(m.brokerSubKeys, channel)
			}
		}
		m.brokerSubMu.Unlock()
		return nil
	})
}

// unsubscribeAllBrokerKeys unsubscribes all key-channels for a base channel.
// Used on channel shutdown.
func (m *SharedPollManager) unsubscribeAllBrokerKeys(channel string) {
	_ = m.node.subDissolver.Submit(func() error {
		m.node.metrics.incActionCount("broker_unsubscribe", channel)
		// Check if channel was re-created while shutting down.
		m.mu.RLock()
		_, stillActive := m.channels[channel]
		m.mu.RUnlock()
		if stillActive {
			return nil
		}
		m.brokerSubMu.RLock()
		ks, ok := m.brokerSubKeys[channel]
		if !ok || len(ks) == 0 {
			m.brokerSubMu.RUnlock()
			return nil
		}
		keyChannels := make([]string, 0, len(ks))
		for kc := range ks {
			keyChannels = append(keyChannels, kc)
		}
		var broker Broker
		for _, kc := range keyChannels {
			if b, ok := m.brokerSubChans[kc]; ok {
				broker = b
				break
			}
		}
		m.brokerSubMu.RUnlock()
		if broker == nil {
			return nil
		}
		if err := broker.Unsubscribe(keyChannels...); err != nil {
			time.Sleep(500 * time.Millisecond)
			return err
		}
		m.brokerSubMu.Lock()
		for _, kc := range keyChannels {
			delete(m.brokerSubChans, kc)
		}
		if ks, ok := m.brokerSubKeys[channel]; ok {
			for _, kc := range keyChannels {
				delete(ks, kc)
			}
			if len(ks) == 0 {
				delete(m.brokerSubKeys, channel)
			}
		}
		m.brokerSubMu.Unlock()
		return nil
	})
}

func (m *SharedPollManager) publish(ctx context.Context, channel string, key string, version uint64, epoch string, data []byte) error {
	m.mu.RLock()
	s, ok := m.channels[channel]
	m.mu.RUnlock()
	if ok {
		s.mu.Lock()
		versionless := s.opts.isVersionless()
		s.mu.Unlock()
		if versionless {
			return errors.New("SharedPollPublish not supported in versionless refresh mode")
		}
	}

	// Check if publish is enabled for this channel (broker subscribed).
	keyCh := sharedPollKeyChannel(channel, key)
	m.brokerSubMu.RLock()
	broker := m.brokerSubChans[keyCh]
	m.brokerSubMu.RUnlock()

	if broker != nil {
		// For shared-poll keyed channels, the broker's Publication.Epoch
		// carries the publisher's per-channel epoch (the wire field is
		// shared with stream channels' stream-position epoch — semantics
		// differ per channel type, but the wire is the same).
		_, err := broker.Publish(keyCh, data, PublishOptions{Key: key, Version: version, Epoch: epoch})
		return err
	}
	// Local-only (PublishEnabled=false or channel not active): apply directly.
	m.handlePublishedData(channel, key, version, epoch, data)
	return nil
}

// stats returns the number of active channels and total tracked keys.
func (m *SharedPollManager) stats() (int, int) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	numChannels := len(m.channels)
	numKeys := 0
	for _, s := range m.channels {
		s.mu.Lock()
		numKeys += len(s.itemIndex)
		s.mu.Unlock()
	}
	return numChannels, numKeys
}

func (m *SharedPollManager) handlePublishedData(channel string, key string, version uint64, epoch string, data []byte) {
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

	if s.opts.isVersionless() {
		// SharedPollPublish should not reach here in versionless mode
		// (rejected by publish()), but guard defensively.
		return
	}

	// Epoch comparison runs *before* the entry-lookup early-return so the
	// channel's stored epoch stays current even on nodes that don't track
	// this specific key. Otherwise the first subscriber to ever track a
	// key on a quiet node would trigger an unnecessary unsubscribe cycle
	// when the cold-key auto-poll detects the existing publisher epoch.
	if clients := s.flipEpochAndCollectClients(hub, epoch); len(clients) > 0 {
		for _, c := range clients {
			c.Unsubscribe(channel, unsubscribeInsufficientState)
		}
	}

	s.mu.Lock()
	entry := s.itemIndex[key]
	if entry == nil {
		s.mu.Unlock()
		return // Key not tracked on this node.
	}
	if version <= entry.version {
		s.mu.Unlock()
		m.node.metrics.getSharedPollPublishCached(channel).skipped.Inc()
		return
	}
	entry.version = version
	entry.freshFromPublish = true
	var prevData []byte
	if s.opts.KeepLatestData {
		prevData = entry.data
		entry.data = data
	}
	s.mu.Unlock()

	m.node.metrics.getSharedPollPublishCached(channel).applied.Inc()

	pub := &protocol.Publication{Key: key, Data: data, Version: version, Epoch: epoch}
	prep := buildPreparedPollData(pub, prevData)
	hub.broadcastToKey(channel, key, version, pub, prep)
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
// A zero delay means default (1s). Use a negative delay (e.g. -1) for immediate shutdown.
func (s *sharedPollChannelState) scheduleShutdown(m *SharedPollManager, channel string, delay time.Duration) {
	s.mu.Lock()

	if s.shutdownTimer != nil {
		s.shutdownTimer.Stop()
	}

	if delay == 0 {
		delay = 1 * time.Second
	}

	if delay < 0 {
		// Immediate shutdown: mirror the timer body. Release s.mu before
		// touching m.mu — lock ordering is m.mu → s.mu everywhere.
		s.doShutdownLocked(m, channel)
		s.mu.Unlock()
		s.finalizeShutdown(m, channel)
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
		s.finalizeShutdown(m, channel)
	})
	s.mu.Unlock()
}

// doShutdownLocked marks the state as removed and cancels the worker. Caller
// must hold s.mu and must not call any m.mu operation while holding s.mu —
// lock ordering is m.mu → s.mu. The follow-up m.mu work is done in
// finalizeShutdown after the caller releases s.mu.
func (s *sharedPollChannelState) doShutdownLocked(m *SharedPollManager, channel string) {
	if s.workerCancel != nil {
		s.workerCancel()
	}
	s.removed = true
	s.shutdownTimer = nil
}

// finalizeShutdown removes the channel from the manager and unsubscribes broker
// keys. Must be called with s.mu released (acquires m.mu and may call back into
// keyedManager / dissolver). Safe to call when m.channels[channel] no longer
// points to s — that branch is a no-op.
func (s *sharedPollChannelState) finalizeShutdown(m *SharedPollManager, channel string) {
	m.mu.Lock()
	if m.channels[channel] == s {
		delete(m.channels, channel)
		m.mu.Unlock()
		m.node.keyedManager.removeChannel(channel)
	} else {
		m.mu.Unlock()
	}
	m.unsubscribeAllBrokerKeys(channel)
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
			cc := node.metrics.getSharedPollChannelCached(channel)
			cc.cycleDuration.Observe(cycleDuration.Seconds())
			workTime := cycleDuration - spreadDelay
			if workTime < 0 {
				workTime = 0
			}
			cc.cycleWorkDuration.Observe(workTime.Seconds())
			if s.opts.RefreshIntervalFn != nil {
				// Pass work time (excluding intentional spread delays)
				// so backpressure reacts to actual backend load.
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

	hc := node.metrics.getSharedPollHandlerCached("notification", channel)

	// Acquire semaphore.
	semStart := time.Now()
	select {
	case sem <- struct{}{}:
	case <-ctx.Done():
		return
	}
	defer func() { <-sem }()
	hc.semWait.Observe(time.Since(semStart).Seconds())

	// Build event items.
	items := make([]SharedPollItem, len(filtered))
	if !s.opts.isVersionless() {
		s.mu.Lock()
		for i, key := range filtered {
			items[i] = SharedPollItem{Key: key}
			if entry, ok := s.itemIndex[key]; ok {
				if entry.needsBroadcast {
					items[i].Version = 0
				} else {
					items[i].Version = entry.version
				}
			}
		}
		s.mu.Unlock()
	} else {
		for i, key := range filtered {
			items[i] = SharedPollItem{Key: key}
		}
	}

	hc.itemsPolled.Add(float64(len(items)))

	callCtx, cancel := context.WithTimeout(ctx, callTimeout)
	callStart := time.Now()
	result, err := handler(callCtx, SharedPollEvent{
		Channel: channel,
		Items:   items,
	})
	cancel()
	hc.duration.Observe(time.Since(callStart).Seconds())

	if err != nil {
		hc.errorCount.Inc()
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
	s.onNotifiedRefreshResponse(channel, result.Epoch, result.Items, hub, node)
}

func (s *sharedPollChannelState) runRefreshCycle(ctx context.Context, node *Node, channel string, sem chan struct{}) time.Duration {
	// 1. Collect all item keys from itemIndex.
	s.mu.Lock()
	if len(s.itemIndex) == 0 {
		s.mu.Unlock()
		return 0
	}
	keys := make([]string, 0, len(s.itemIndex))
	for k, entry := range s.itemIndex {
		if entry.freshFromPublish {
			entry.freshFromPublish = false // Clear flag.
			continue                       // Skip — data is fresh from publish.
		}
		keys = append(keys, k)
	}
	s.mu.Unlock()

	if len(keys) == 0 {
		return 0
	}

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
		if ctx.Err() != nil {
			break
		}
		if i > 0 && dispatchDelay > 0 {
			// time.NewTimer + explicit Stop avoids leaking a timer per iteration
			// when ctx is cancelled (time.After leaves the unselected timer to
			// fire later, garbage-collected only after the duration elapses).
			timer := time.NewTimer(dispatchDelay)
			select {
			case <-ctx.Done():
				if !timer.Stop() {
					<-timer.C
				}
			case <-timer.C:
			}
			if ctx.Err() != nil {
				break
			}
		}

		hc := node.metrics.getSharedPollHandlerCached("timer", channel)
		semStart := time.Now()
		sem <- struct{}{}
		wg.Add(1)
		go func(chunk []string, semStart time.Time) {
			defer func() {
				<-sem
				wg.Done()
			}()
			hc.semWait.Observe(time.Since(semStart).Seconds())

			// Build event items.
			items := make([]SharedPollItem, len(chunk))
			if !s.opts.isVersionless() {
				s.mu.Lock()
				for i, key := range chunk {
					items[i] = SharedPollItem{Key: key}
					if entry, ok := s.itemIndex[key]; ok {
						if entry.needsBroadcast {
							items[i].Version = 0
						} else {
							items[i].Version = entry.version
						}
					}
				}
				s.mu.Unlock()
			} else {
				for i, key := range chunk {
					items[i] = SharedPollItem{Key: key}
				}
			}

			hc.itemsPolled.Add(float64(len(items)))

			callCtx, cancel := context.WithTimeout(ctx, callTimeout)
			callStart := time.Now()
			result, err := handler(callCtx, SharedPollEvent{
				Channel: channel,
				Items:   items,
			})
			cancel()
			hc.duration.Observe(time.Since(callStart).Seconds())

			if err != nil {
				hc.errorCount.Inc()
				if node.logEnabled(LogLevelWarn) {
					node.logger.log(newLogEntry(LogLevelWarn, "shared poll refresh error", map[string]any{
						"channel": channel,
						"error":   err.Error(),
					}))
				}
				return
			}

			s.onRefreshResponse(channel, result.Epoch, result.Items, hub, node)
		}(chunk, semStart)
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

// onNotifiedRefreshResponse processes a backend response triggered by a
// notification (targeted poll). Identical to onRefreshResponse except for the
// metric source label.
func (s *sharedPollChannelState) onNotifiedRefreshResponse(channel string, respEpoch string, items []SharedPollRefreshItem, hub *keyedHub, node *Node) {
	s.applyRefreshResponse(channel, respEpoch, items, hub, node, "notification")
}

// onRefreshResponse processes a backend response from the periodic timer poll.
func (s *sharedPollChannelState) onRefreshResponse(channel string, respEpoch string, items []SharedPollRefreshItem, hub *keyedHub, node *Node) {
	s.applyRefreshResponse(channel, respEpoch, items, hub, node, "timer")
}

// applyRefreshResponse is the shared implementation for both notified and
// timer-driven refresh responses. The two source kinds differ only in the
// metric label used for instrumentation — all per-item handling (versioned vs
// versionless change detection, epoch flip, broadcast, removals, prev_data /
// KeepLatestData delta base capture) is identical.
func (s *sharedPollChannelState) applyRefreshResponse(channel string, respEpoch string, items []SharedPollRefreshItem, hub *keyedHub, node *Node, source string) {
	// Versioned channels: detect publisher epoch change before any per-item
	// processing. A flip resets all per-key state and unsubscribes current
	// subscribers; the items in this response then repopulate state under
	// the new epoch via the standard path below.
	if !s.opts.isVersionless() {
		if clients := s.flipEpochAndCollectClients(hub, respEpoch); len(clients) > 0 {
			for _, c := range clients {
				c.Unsubscribe(channel, unsubscribeInsufficientState)
			}
		}
	}

	type pendingUpdate struct {
		key      string
		version  uint64
		data     []byte
		prevData []byte
	}

	updates := make([]pendingUpdate, 0, len(items))
	var removals []string
	var changedCount, unchangedCount int

	s.mu.Lock()

	isVersionless := s.opts.isVersionless()

	for _, e := range items {
		if e.Removed {
			removals = append(removals, e.Key)
			continue
		}

		entry := s.itemIndex[e.Key]
		if entry == nil {
			continue
		}

		if isVersionless && e.Version == 0 {
			// Versionless backend response: detect changes by content hash.
			changed := false
			if s.opts.KeepLatestData && entry.version > 0 {
				changed = !bytes.Equal(entry.data, e.Data)
			} else if entry.version > 0 {
				newHash := xxHash64(e.Data)
				changed = newHash != entry.dataHash
				if changed {
					entry.dataHash = newHash
				}
			} else {
				// First data for this key.
				changed = true
				if !s.opts.KeepLatestData {
					entry.dataHash = xxHash64(e.Data)
				}
			}
			if !changed {
				unchangedCount++
				if entry.needsBroadcast && entry.version > 0 {
					entry.needsBroadcast = false
					updates = append(updates, pendingUpdate{
						key: e.Key, version: entry.version, data: e.Data,
					})
				}
				continue
			}
			entry.needsBroadcast = false
			changedCount++
			s.versionCounter++
			syntheticVersion := s.versionCounter
			var prevData []byte
			if s.opts.KeepLatestData {
				prevData = entry.data
				entry.data = e.Data
			}
			entry.version = syntheticVersion
			updates = append(updates, pendingUpdate{
				key: e.Key, version: syntheticVersion, data: e.Data, prevData: prevData,
			})
			continue
		}

		if e.Version <= entry.version {
			unchangedCount++
			if entry.needsBroadcast && entry.version > 0 {
				entry.needsBroadcast = false
				updates = append(updates, pendingUpdate{
					key: e.Key, version: entry.version, data: e.Data,
				})
			}
			continue
		}

		entry.needsBroadcast = false
		changedCount++
		// Capture prevData before updating — this is the delta base.
		var prevData []byte
		if s.opts.KeepLatestData {
			prevData = entry.data
			entry.data = e.Data
		}
		if len(e.PrevData) > 0 && !s.opts.KeepLatestData {
			prevData = e.PrevData
		}
		entry.version = e.Version

		updates = append(updates, pendingUpdate{
			key: e.Key, version: e.Version, data: e.Data, prevData: prevData,
		})
	}

	s.mu.Unlock()

	rc := node.metrics.getSharedPollResultCached(source, channel)
	rc.changed.Add(float64(changedCount))
	rc.unchanged.Add(float64(unchangedCount))
	rc.removed.Add(float64(len(removals)))

	// Build publications outside the lock. Batch-allocate Publication structs
	// in a single slice to avoid one heap allocation per changed key.
	broadcasts := make([]pendingBroadcast, 0, len(updates)+len(removals))
	pubs := make([]protocol.Publication, len(updates))
	for i, u := range updates {
		pubs[i] = protocol.Publication{Key: u.key, Data: u.data, Version: u.version}
		prep := buildPreparedPollData(&pubs[i], u.prevData)
		broadcasts = append(broadcasts, pendingBroadcast{key: u.key, version: u.version, pub: &pubs[i], prep: prep})
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
	if len(removals) > 0 {
		s.mu.Lock()
		for _, key := range removals {
			hub.removeAllSubscribers(key)
			delete(s.itemIndex, key)
		}
		s.mu.Unlock()
	}
}
