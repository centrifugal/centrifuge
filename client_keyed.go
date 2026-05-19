package centrifuge

import (
	"time"

	"github.com/centrifugal/centrifuge/internal/convert"

	"github.com/centrifugal/protocol"
	"github.com/segmentio/encoding/json"
)

// encodeKeyedPush encodes a publication as a Push (or Reply wrapping a Push) for this
// client's transport protocol. Used by keyed (shared poll) writes which bypass the
// Hub's per-protocol-key encoding.
func (c *Client) encodeKeyedPush(channel string, pub *protocol.Publication) ([]byte, error) {
	push := &protocol.Push{Channel: channel, Pub: pub}
	protoType := c.transport.Protocol().toProto()
	if protoType == protocol.TypeJSON {
		if c.transport.Unidirectional() {
			return protocol.DefaultJsonPushEncoder.Encode(push)
		}
		return protocol.DefaultJsonReplyEncoder.Encode(&protocol.Reply{Push: push})
	}
	if c.transport.Unidirectional() {
		return protocol.DefaultProtobufPushEncoder.Encode(push)
	}
	return protocol.DefaultProtobufReplyEncoder.Encode(&protocol.Reply{Push: push})
}

// keyedChannelDeltaState holds per-channel delta configuration for keyed subscriptions.
type keyedChannelDeltaState struct {
	deltaType DeltaType // negotiated delta type for this channel
}

// keyedKeyState holds per-key state for a keyed subscription.
type keyedKeyState struct {
	version    uint64 // per-connection version (from client track() or updated on delivery)
	deltaReady bool   // true after first full publication delivered for this key
	expireAt   int64  // unix timestamp when track signature expires; 0 = no expiry
}

// keyedState holds per-connection keyed subscription state.
type keyedState struct {
	// channels: channel → delta config. Only set when delta is negotiated.
	channels map[string]*keyedChannelDeltaState
	// trackedKeys: channel → (itemKey → per-key state).
	// The version here is the per-connection version (from client track()
	// or updated on publication delivery). NOT the server-side itemIndex version.
	trackedKeys map[string]map[string]*keyedKeyState
	// minTrackExpireAt: channel → lower bound on earliest key expiry.
	// Used as fast-path to skip key iteration when nothing can be expired.
	// 0 means no keys have expiry set (skip check entirely).
	minTrackExpireAt map[string]int64
}

// Keyed sub-refresh request types (wire protocol values).
const (
	typeTrack   int32 = 1
	typeUntrack int32 = 2
)

// handleTrack processes SubRefreshRequest with type=typeTrack (track).
// A request can carry multiple signed batches (req.Track) — the SDK packs
// every cached signature library entry into a single sub_refresh frame on
// reconnect replay, so one handler invocation may cover N signatures.
//
// Duplicate keys across batches are deduped with last-batch-wins semantics:
// version and per-batch ExpireAt come from the LAST batch the key appears
// in. Both batches' signatures are still validated, so the client is fully
// authorized for the key either way.
func (c *Client) handleTrack(req *protocol.SubRefreshRequest, cmd *protocol.Command, started time.Time, rw *replyWriter) error {
	channel := req.Channel

	if len(req.Track) == 0 {
		return ErrorBadRequest
	}
	// Build a deduped key index up front. Used to (a) drive the optimistic
	// limit check against DISTINCT-key count (matches the per-connection map
	// shape) and (b) flatten items inside the trackHandler callback.
	type flatItem struct {
		key      string
		version  uint64
		batchIdx int // index into req.Track / eventBatches — picks the per-batch ExpireAt.
	}
	flatIdx := make(map[string]int, 16)
	var flat []flatItem
	for i, b := range req.Track {
		for _, it := range b.Items {
			fi := flatItem{key: it.Key, version: it.Version, batchIdx: i}
			if existing, ok := flatIdx[it.Key]; ok {
				flat[existing] = fi // last batch wins for version + batchIdx
			} else {
				flatIdx[it.Key] = len(flat)
				flat = append(flat, fi)
			}
		}
	}
	if len(flat) == 0 {
		return ErrorBadRequest
	}

	// Build a set of keys that will be immediately removed by Step 8 (inline
	// untrack). Both limit checks below net these out so a replay that tracks
	// N keys but untracks M of them only consumes N-M slots — not N.
	inlineUntrackSet := make(map[string]struct{}, len(req.Untrack))
	for _, k := range req.Untrack {
		inlineUntrackSet[k] = struct{}{}
	}

	// Optimistic limit check — counts DISTINCT new keys minus those that will
	// be immediately removed via inline untrack. Also subtracts already-tracked
	// keys appearing in inlineUntrackSet, since Step 8 removes them and frees
	// slots. Re-checked under write lock.
	c.mu.RLock()
	var currentCount, newKeyCountOpt, inlineRemovedExistingOpt int
	if c.keyed != nil {
		chanKeys := c.keyed.trackedKeys[channel]
		currentCount = len(chanKeys)
		for _, f := range flat {
			if _, exists := chanKeys[f.key]; !exists {
				if _, willUntrack := inlineUntrackSet[f.key]; !willUntrack {
					newKeyCountOpt++
				}
			}
		}
		for k := range inlineUntrackSet {
			if _, exists := chanKeys[k]; exists {
				inlineRemovedExistingOpt++
			}
		}
	} else {
		for _, f := range flat {
			if _, willUntrack := inlineUntrackSet[f.key]; !willUntrack {
				newKeyCountOpt++
			}
		}
	}
	c.mu.RUnlock()

	maxTracked := c.node.keyedManager.maxTrackedPerConnection(channel)
	if currentCount-inlineRemovedExistingOpt+newKeyCountOpt > maxTracked {
		return ErrorLimitExceeded
	}

	// Call OnTrack handler (Centrifugo validates HMAC for every batch).
	if c.eventHub.trackHandler == nil {
		return ErrorNotAvailable
	}

	eventBatches := make([]TrackBatch, len(req.Track))
	for i, b := range req.Track {
		items := make([]TrackItem, len(b.Items))
		for j, it := range b.Items {
			items[j] = TrackItem{Key: it.Key, Version: it.Version}
		}
		eventBatches[i] = TrackBatch{Items: items, Signature: b.Signature}
	}
	event := TrackEvent{Channel: channel, Batches: eventBatches}

	c.eventHub.trackHandler(event, func(reply TrackReply, err error) {
		if err != nil {
			c.writeDisconnectOrErrorFlush(channel, protocol.FrameTypeSubRefresh, cmd, err, started, rw)
			return
		}
		// Handlers that don't care about per-batch TTL may return an empty
		// reply.Batches — treat that as "no expiry to record" for every batch.
		// A non-empty Batches slice of the wrong length is a programmer error.
		batchReplies := reply.Batches
		if len(batchReplies) == 0 {
			batchReplies = make([]TrackBatchReply, len(eventBatches))
		} else if len(batchReplies) != len(eventBatches) {
			c.writeDisconnectOrErrorFlush(channel, protocol.FrameTypeSubRefresh, cmd, ErrorInternal, started, rw)
			return
		}

		// Build per-call helper slices from the deduped flat index.
		items := make([]TrackItem, len(flat))
		allKeys := make([]string, len(flat))
		for i, f := range flat {
			items[i] = TrackItem{Key: f.key, Version: f.version}
			allKeys[i] = f.key
		}

		// Get or create keyed channel state.
		opts, ok := c.node.config.SharedPoll.GetSharedPollChannelOptions(channel)
		if !ok {
			c.writeDisconnectOrErrorFlush(channel, protocol.FrameTypeSubRefresh, cmd, ErrorNotAvailable, started, rw)
			return
		}
		keyedOpts := opts.toKeyedChannelOptions()
		c.node.keyedManager.getOrCreateChannel(channel, keyedOpts)

		// Step 1: Register in SharedPollManager FIRST (before any per-connection
		// state is written). This ensures per-connection state never points to
		// keys the server isn't tracking — fixing the state-divergence bug
		// where a failed broker subscribe left phantom keys in trackedKeys.
		// Do NOT addSubscriber yet — client must not receive broadcasts before response.
		// Classification:
		//   cold: new to server → auto-poll (backend call) after addSubscriber.
		//   warm: existing key, client needs data (version=0 or stale version) →
		//         direct delivery from cache if KeepLatestData, else notify +
		//         needsBroadcast for near-immediate backend poll.
		//   (none): existing key, client up to date → no action.
		var coldKeys []string
		var warmKeys []string
		if c.node.sharedPollManager != nil {
			trackResults, err := c.node.sharedPollManager.trackKeys(channel, opts, allKeys)
			if err != nil {
				c.writeDisconnectOrErrorFlush(channel, protocol.FrameTypeSubRefresh, cmd, ErrorInternal, started, rw)
				return
			}
			for i, f := range flat {
				tr := trackResults[i]
				if tr.isNew && f.version == 0 {
					coldKeys = append(coldKeys, f.key)
				} else if !tr.isNew && f.version == 0 {
					warmKeys = append(warmKeys, f.key)
				} else if tr.entryVersion > f.version {
					warmKeys = append(warmKeys, f.key)
				}
			}
		}

		// Step 2: Commit per-connection state under the write lock with a
		// final limit re-check. This re-check is the authoritative gate —
		// concurrent track calls that passed the optimistic RLock check at the
		// top all converge here and only the first to fit wins. On failure we
		// roll back the server-side track from Step 1.
		c.mu.Lock()
		if c.keyed == nil {
			c.keyed = &keyedState{
				channels:    make(map[string]*keyedChannelDeltaState),
				trackedKeys: make(map[string]map[string]*keyedKeyState),
			}
		}
		if c.keyed.trackedKeys[channel] == nil {
			c.keyed.trackedKeys[channel] = make(map[string]*keyedKeyState)
		}
		chanKeys := c.keyed.trackedKeys[channel]

		// Re-tracking an existing key is a version update, not a new slot —
		// only count new keys, and exclude those being inline-untracked. Also
		// subtract already-tracked keys that will be removed by Step 8, since
		// they free up slots.
		var newKeyCount, inlineRemovedExisting int
		for _, f := range flat {
			if _, exists := chanKeys[f.key]; !exists {
				if _, willUntrack := inlineUntrackSet[f.key]; !willUntrack {
					newKeyCount++
				}
			}
		}
		for k := range inlineUntrackSet {
			if _, exists := chanKeys[k]; exists {
				inlineRemovedExisting++
			}
		}
		if len(chanKeys)-inlineRemovedExisting+newKeyCount > maxTracked {
			c.mu.Unlock()
			// Roll back the server-side track from Step 1 so we don't leak
			// per-channel itemIndex entries for keys the client doesn't hold.
			if c.node.sharedPollManager != nil {
				for _, key := range allKeys {
					c.node.sharedPollManager.untrack(channel, key)
				}
			}
			c.writeDisconnectOrErrorFlush(channel, protocol.FrameTypeSubRefresh, cmd, ErrorLimitExceeded, started, rw)
			return
		}

		// Commit: store client-provided versions + per-batch expireAt in per-connection state.
		var minExpireAt int64
		for _, f := range flat {
			expireAt := batchReplies[f.batchIdx].ExpireAt
			chanKeys[f.key] = &keyedKeyState{version: f.version, expireAt: expireAt}
			if expireAt > 0 && (minExpireAt == 0 || expireAt < minExpireAt) {
				minExpireAt = expireAt
			}
		}
		// Update fast-path hint for track expiry checks.
		if minExpireAt > 0 {
			if c.keyed.minTrackExpireAt == nil {
				c.keyed.minTrackExpireAt = make(map[string]int64)
			}
			existing := c.keyed.minTrackExpireAt[channel]
			if existing == 0 || minExpireAt < existing {
				c.keyed.minTrackExpireAt[channel] = minExpireAt
			}
		}
		c.mu.Unlock()

		// Step 2: Collect cached data for items where server has newer version.
		var cachedItems []*protocol.Publication
		if c.node.sharedPollManager != nil {
			cachedItems = c.node.sharedPollManager.getCachedData(channel, items)
		}

		// Step 3: Update per-connection versions for cached items to prevent
		// duplicate delivery via subsequent broadcasts.
		// Capture (keyState, prevVersion, prevDeltaReady) so we can roll back
		// if the response encode fails below — without rollback the connection
		// would mark cached items as delivered while the SDK never received them.
		type versionRollback struct {
			ks              *keyedKeyState
			prevVersion     uint64
			prevDeltaReady  bool
		}
		var rollbacks []versionRollback
		if len(cachedItems) > 0 {
			c.mu.Lock()
			if c.keyed != nil {
				chanKeys := c.keyed.trackedKeys[channel]
				for _, pub := range cachedItems {
					if ks, ok := chanKeys[pub.Key]; ok {
						if pub.Version > ks.version {
							rollbacks = append(rollbacks, versionRollback{ks: ks, prevVersion: ks.version, prevDeltaReady: ks.deltaReady})
							ks.version = pub.Version
							ks.deltaReady = true
						}
					}
				}
			}
			c.mu.Unlock()
		}

		// Step 4: Build and write response (enqueued before any broadcasts).
		// For type=1 (track) the response carries the MIN TTL across all
		// batches in the request — the SDK schedules its consolidating
		// refresh at the earliest deadline received across all responses
		// (single global timer, no per-entry expiry tracking needed).
		res := &protocol.SubRefreshResult{}
		if minExpireAt > 0 {
			nowUnix := time.Now().Unix()
			res.Expires = true
			if minExpireAt > nowUnix {
				res.Ttl = uint32(minExpireAt - nowUnix)
			}
		}
		if len(cachedItems) > 0 {
			res.Items = cachedItems
		}

		protoReply, err := c.getSubRefreshCommandReply(res)
		if err != nil {
			// Roll back per-connection version updates from Step 3 — the SDK
			// never received the reply, so we must not pretend it has the
			// cached versions. Without this, the next live broadcast at the
			// same version is filtered out and the client misses a publication.
			if len(rollbacks) > 0 {
				c.mu.Lock()
				for _, r := range rollbacks {
					r.ks.version = r.prevVersion
					r.ks.deltaReady = r.prevDeltaReady
				}
				c.mu.Unlock()
			}
			c.logWriteInternalErrorFlush(channel, protocol.FrameTypeSubRefresh, cmd, err, "error encoding sub refresh", started, rw)
			return
		}
		c.writeEncodedCommandReply(channel, protocol.FrameTypeSubRefresh, cmd, protoReply, rw)
		c.handleCommandFinished(cmd, protocol.FrameTypeSubRefresh, nil, protoReply, started, channel)
		c.releaseSubRefreshCommandReply(protoReply)

		// Step 5: NOW register in hub — client starts receiving broadcasts.
		// Response is already enqueued, so broadcasts are ordered after it.
		// keyedWritePublication checks pubVersion <= keyState.version, so cached
		// items won't be re-delivered.
		//
		// keyedManager.addSubscribers performs "ensure-state-then-add"
		// atomically under the manager's lock. This is required because a
		// concurrent finalizeShutdown of an older sharedPollChannelState may
		// race here: a non-atomic getOrCreateChannel + addSubscriber
		// sequence could end up with the client subscribed to a hub that
		// the finalizeShutdown then deletes from the manager, leaving the
		// client orphaned from future broadcasts (which look up via
		// getHub).
		c.node.keyedManager.addSubscribers(channel, allKeys, c, keyedOpts)
		hub := c.node.keyedManager.getHub(channel)

		// Compute warm key delivery plan AFTER addSubscriber. KeepLatestData →
		// direct delivery from cache (zero backend calls). Otherwise → deferred
		// via notify + needsBroadcast (one backend call per key per reconnect
		// wave).
		//
		// Snapshotting after addSubscriber closes a race: if a publish lands
		// between the snapshot and addSubscriber, the broadcast goes only to
		// existing subscribers (not us yet), so we would deliver a stale
		// snapshot and the version filter would suppress later same-version
		// broadcasts — silently pinning the client to a stale value until the
		// next entry update. With the snapshot taken after addSubscriber, any
		// concurrent broadcast reaches us via the hub and advances the per-
		// connection version; our direct-delivery call then no-ops, leaving
		// client and server in sync.
		var warmCachedData []warmKeyData
		var deferredWarmKeys []string
		if c.node.sharedPollManager != nil && len(warmKeys) > 0 {
			warmCachedData = c.node.sharedPollManager.getWarmKeyData(channel, warmKeys)
			if len(warmCachedData) < len(warmKeys) {
				directKeys := make(map[string]struct{}, len(warmCachedData))
				for _, wd := range warmCachedData {
					directKeys[wd.key] = struct{}{}
				}
				for _, key := range warmKeys {
					if _, ok := directKeys[key]; !ok {
						deferredWarmKeys = append(deferredWarmKeys, key)
					}
				}
			}
		}

		// Step 5.5: Direct delivery for warm keys with cached data.
		// Uses internal version for per-connection dedup — keyedWritePublication
		// updates keyState.version to the internal version, so subsequent broadcasts
		// with the same version are skipped (no double delivery).
		for _, wd := range warmCachedData {
			c.keyedWritePublication(channel, wd.key, wd.internalVersion, wd.pub, preparedData{})
		}

		// Step 6: Auto-notify cold keys AFTER addSubscriber so the broadcast
		// from the notified refresh can reach this client.
		if c.node.sharedPollManager != nil && len(coldKeys) > 0 {
			for _, key := range coldKeys {
				c.node.sharedPollManager.notify(channel, key)
			}
		}

		// Step 7: Deferred warm keys — flag + notify AFTER addSubscriber.
		// markNeedsBroadcast sets the flag and sends at-most-one notify per
		// key, triggering a backend call for near-immediate delivery. Keys
		// already flagged by a concurrent client are skipped (deduplication).
		if c.node.sharedPollManager != nil && len(deferredWarmKeys) > 0 {
			c.node.sharedPollManager.markNeedsBroadcast(channel, deferredWarmKeys)
		}

		// Step 8: Process inline untrack — keys that were part of the signed
		// batch but have been locally untracked by the client since the
		// signature was obtained. HMAC validation above covers the full batch;
		// we remove these keys now so the client receives no broadcasts for them.
		// Placed after addSubscriber (step 5) so hub state is coherent: we add
		// then immediately remove, never leaving a gap where a key is absent.
		// Only keys that were actually tracked are acted on — random keys sent
		// by the client are silently ignored.
		if len(req.Untrack) > 0 {
			var actualUntrack []string
			c.mu.Lock()
			if c.keyed != nil {
				chanKeys := c.keyed.trackedKeys[channel]
				if chanKeys != nil {
					for _, key := range req.Untrack {
						if _, exists := chanKeys[key]; exists {
							delete(chanKeys, key)
							actualUntrack = append(actualUntrack, key)
						}
					}
					if len(chanKeys) == 0 {
						delete(c.keyed.trackedKeys, channel)
						if c.keyed.minTrackExpireAt != nil {
							delete(c.keyed.minTrackExpireAt, channel)
						}
					}
				}
			}
			c.mu.Unlock()

			for _, key := range actualUntrack {
				keyEmpty := hub.removeSubscriber(key, c)
				if c.node.sharedPollManager != nil && keyEmpty {
					c.node.sharedPollManager.untrack(channel, key)
				}
			}

			if len(actualUntrack) > 0 && c.eventHub.untrackHandler != nil {
				c.eventHub.untrackHandler(UntrackEvent{
					Channel: channel,
					Keys:    actualUntrack,
				})
			}
		}
	})
	return nil
}

// handleUntrack processes SubRefreshRequest with type=2 (untrack).
func (c *Client) handleUntrack(req *protocol.SubRefreshRequest, cmd *protocol.Command, started time.Time, rw *replyWriter) error {
	channel := req.Channel

	if len(req.Untrack) == 0 {
		return ErrorBadRequest
	}

	var actualUntrack []string
	c.mu.Lock()
	if c.keyed != nil {
		chanKeys := c.keyed.trackedKeys[channel]
		if chanKeys != nil {
			for _, key := range req.Untrack {
				if _, exists := chanKeys[key]; exists {
					delete(chanKeys, key)
					actualUntrack = append(actualUntrack, key)
				}
			}
			if len(chanKeys) == 0 {
				delete(c.keyed.trackedKeys, channel)
				if c.keyed.minTrackExpireAt != nil {
					delete(c.keyed.minTrackExpireAt, channel)
				}
			}
		}
	}
	c.mu.Unlock()

	hub := c.node.keyedManager.getHub(channel)
	if hub != nil {
		for _, key := range actualUntrack {
			keyEmpty := hub.removeSubscriber(key, c)
			if c.node.sharedPollManager != nil && keyEmpty {
				c.node.sharedPollManager.untrack(channel, key)
			}
		}
	}

	if len(actualUntrack) > 0 && c.eventHub.untrackHandler != nil {
		c.eventHub.untrackHandler(UntrackEvent{
			Channel: channel,
			Keys:    actualUntrack,
		})
	}

	res := &protocol.SubRefreshResult{}
	protoReply, err := c.getSubRefreshCommandReply(res)
	if err != nil {
		c.logWriteInternalErrorFlush(channel, protocol.FrameTypeSubRefresh, cmd, err, "error encoding sub refresh", started, rw)
		return nil
	}
	c.writeEncodedCommandReply(channel, protocol.FrameTypeSubRefresh, cmd, protoReply, rw)
	c.handleCommandFinished(cmd, protocol.FrameTypeSubRefresh, nil, protoReply, started, channel)
	c.releaseSubRefreshCommandReply(protoReply)
	return nil
}

// cleanupKeyed removes all keyed tracking for a channel when a client
// unsubscribes or disconnects.
func (c *Client) cleanupKeyed(channel string) {
	c.mu.Lock()
	if c.keyed == nil {
		c.mu.Unlock()
		return
	}
	chanKeys := c.keyed.trackedKeys[channel]
	// Copy keys to avoid holding lock during cleanup.
	keys := make([]string, 0, len(chanKeys))
	for k := range chanKeys {
		keys = append(keys, k)
	}
	delete(c.keyed.trackedKeys, channel)
	delete(c.keyed.channels, channel)
	if c.keyed.minTrackExpireAt != nil {
		delete(c.keyed.minTrackExpireAt, channel)
	}
	c.mu.Unlock()

	hub := c.node.keyedManager.getHub(channel)
	if hub == nil {
		return
	}
	for _, key := range keys {
		keyEmpty := hub.removeSubscriber(key, c)
		if c.node.sharedPollManager != nil && keyEmpty {
			c.node.sharedPollManager.untrack(channel, key)
		}
	}
}

// checkTrackExpiration silently removes tracked keys whose signatures have expired.
// No removal publications are sent — the client SDK handles expiry via its refresh flow.
func (c *Client) checkTrackExpiration(channel string, delay time.Duration) {
	nowUnix := c.node.nowTimeGetter().Unix()

	// Fast path: check per-channel hint under read lock.
	c.mu.RLock()
	if c.keyed == nil {
		c.mu.RUnlock()
		return
	}
	minExpire := c.keyed.minTrackExpireAt[channel]
	c.mu.RUnlock()
	if minExpire == 0 || nowUnix <= minExpire+int64(delay.Seconds()) {
		return // Nothing can be expired yet.
	}

	// Slow path: write lock, iterate keys, find and remove expired.
	c.mu.Lock()
	if c.keyed == nil {
		c.mu.Unlock()
		return
	}
	minExpire = c.keyed.minTrackExpireAt[channel]
	if minExpire == 0 || nowUnix <= minExpire+int64(delay.Seconds()) {
		c.mu.Unlock()
		return
	}
	chanKeys := c.keyed.trackedKeys[channel]
	if len(chanKeys) == 0 {
		c.mu.Unlock()
		return
	}
	var expiredKeys []string
	newMin := int64(0)
	for key, state := range chanKeys {
		if state.expireAt > 0 && nowUnix > state.expireAt+int64(delay.Seconds()) {
			expiredKeys = append(expiredKeys, key)
			delete(chanKeys, key)
		} else if state.expireAt > 0 {
			if newMin == 0 || state.expireAt < newMin {
				newMin = state.expireAt
			}
		}
	}
	// Recompute accurate min after removing expired keys.
	if c.keyed.minTrackExpireAt != nil {
		if newMin > 0 {
			c.keyed.minTrackExpireAt[channel] = newMin
		} else {
			delete(c.keyed.minTrackExpireAt, channel)
		}
	}
	c.mu.Unlock()

	if len(expiredKeys) == 0 {
		return
	}

	// Clean up hub and SharedPollManager (no removal publications sent).
	hub := c.node.keyedManager.getHub(channel)
	if hub != nil {
		for _, key := range expiredKeys {
			keyEmpty := hub.removeSubscriber(key, c)
			if c.node.sharedPollManager != nil && keyEmpty {
				c.node.sharedPollManager.untrack(channel, key)
			}
		}
	}

	if c.node.logger.enabled(LogLevelInfo) {
		c.node.logger.log(newLogEntry(LogLevelInfo, "track keys expired",
			map[string]any{"channel": channel, "client": c.uid, "user": c.user, "num_keys": len(expiredKeys)}))
	}
}

// keyedWritePublication writes a publication to a client for a keyed channel.
// It checks the per-connection version and only delivers if the publication
// version is newer. Updates per-connection version on delivery.
// Handles per-key delta readiness: first publication per key is always full,
// subsequent publications may use delta if available.
//
// Encoding runs outside c.mu so a slow encode does not block other client
// operations. The version re-check, the enqueue, and the per-connection
// state update all run under c.mu in a single critical section: this
// serializes concurrent broadcasts to the same client at the queue
// boundary, so anything that lands in the wire queue is the freshest
// version observed under the lock and any concurrent broadcast carrying an
// older-or-equal version is filtered out — preventing wire-order inversion
// where a slower-encoding older version would otherwise enqueue behind a
// faster-encoding newer one.
//
// State (keyState.version / keyState.deltaReady) is updated only after the
// publication is successfully enqueued. If encode fails, no-write conditions
// trigger, or enqueue returns an error, state stays unchanged — otherwise
// the client would silently miss the publication and subsequent broadcasts
// at lower/equal versions would be filtered out, leaving server and client
// out of sync (and, for delta channels, the next broadcast would send a
// delta against a base the client never received).
func (c *Client) keyedWritePublication(channel string, key string, pubVersion uint64, pub *protocol.Publication, prep preparedData) {
	c.mu.Lock()
	if c.keyed == nil {
		c.mu.Unlock()
		return
	}
	chanKeys, ok := c.keyed.trackedKeys[channel]
	if !ok {
		c.mu.Unlock()
		return
	}
	keyState, tracked := chanKeys[key]
	if !tracked || pubVersion <= keyState.version {
		c.mu.Unlock()
		return
	}

	// Compute tentative delta decision against current state — do NOT mutate
	// state yet. The final decision is re-checked under the lock in Phase 3
	// because keyState.version can advance between phases.
	chState := c.keyed.channels[channel]
	channelDelta := chState != nil && chState.deltaType != deltaTypeNone
	deltaPossible := channelDelta && prep.deltaSub && keyState.deltaReady
	c.mu.Unlock()

	isJSON := c.transport.Protocol().toProto() == protocol.TypeJSON

	// Encode outside the lock. Encoding can JSON-escape large payloads or
	// build a delta frame — keeping it outside c.mu avoids stalling other
	// operations on this client. Two concurrent broadcasts for the same key
	// can therefore encode in parallel; the lock-protected enqueue below
	// resolves their ordering.
	//
	// We always encode the FULL form so Phase 3 can fall back to it without
	// re-encoding under the lock. The fallback is needed when the delta's
	// base version (prep.keyedDeltaPrevVersion) doesn't match the client's
	// current keyState.version — a sign that an intermediate broadcast was
	// missed, so applying this patch would produce garbage.
	pubFullToEncode := pub
	if channelDelta && isJSON {
		// JSON+delta: must JSON-escape data so client stores bytes for delta base.
		pubFullToEncode = &protocol.Publication{
			Data:    json.Escape(convert.BytesToString(pub.Data)),
			Key:     pub.Key,
			Version: pub.Version,
		}
	}
	encodedFull, err := c.encodeKeyedPush(channel, pubFullToEncode)
	if err != nil {
		return
	}

	var encodedDelta []byte
	if deltaPossible {
		deltaData := prep.keyedDeltaPatch
		if isJSON {
			deltaData = json.Escape(convert.BytesToString(deltaData))
		}
		deltaPub := &protocol.Publication{
			Data:    deltaData,
			Delta:   prep.keyedDeltaIsReal,
			Key:     pub.Key,
			Version: pub.Version,
		}
		data, encErr := c.encodeKeyedPush(channel, deltaPub)
		if encErr != nil {
			return
		}
		encodedDelta = data
	}

	// Mirror writePublication's no-write conditions so we don't enqueue or
	// advance state when the publication would be dropped (these return nil
	// from writePublication, indistinguishable from a real write).
	if hasFlag(c.transport.DisabledPushFlags(), PushFlagPublication) {
		return
	}
	if prep.wasFiltered && !deltaPossible {
		return
	}

	// Resolve batch config — user-supplied callback, must run outside c.mu.
	var batchConfig ChannelBatchConfig
	if c.node.config.GetChannelBatchConfig != nil {
		batchConfig = c.node.config.GetChannelBatchConfig(channel)
	}

	// Trace before the critical section to avoid a c.mu.RLock acquisition
	// inside traceOutPush while we're holding c.mu.Lock below. Tracing
	// before enqueue is consistent with writePublication's existing pattern.
	if c.node.logEnabled(LogLevelTrace) {
		c.traceOutPush(&protocol.Push{Channel: channel, Pub: pub})
	}

	// Critical section: re-check version, decide delta-vs-full, enqueue,
	// and update state under c.mu. Holding the lock through enqueue is what
	// serializes concurrent broadcasts to this client and preserves causal
	// version order on the wire. writeEncodedPushData uses messageWriter /
	// perChannelWriter, each of which has its own internal mutex that does
	// NOT touch c.mu — safe to call while holding it. (Going through
	// writePublication here would deadlock on its c.mu.RLock for the
	// deltaSub branch.)
	c.mu.Lock()
	if c.keyed == nil {
		c.mu.Unlock()
		return
	}
	chanKeys, ok = c.keyed.trackedKeys[channel]
	if !ok {
		c.mu.Unlock()
		return
	}
	keyState, tracked = chanKeys[key]
	if !tracked || pubVersion <= keyState.version {
		// A concurrent broadcast already delivered this or a newer version.
		c.mu.Unlock()
		return
	}

	// Decide delta vs full under the lock. The delta is only safe to send
	// when the client's current keyState.version equals the version of the
	// bytes the patch was computed against — otherwise the client doesn't
	// hold the right base and applying the patch yields garbage. This
	// condition is broken by concurrent broadcasts that update the server's
	// entry between when this broadcast's prep was built and when we
	// observe keyState here.
	useDelta := deltaPossible && encodedDelta != nil && keyState.deltaReady && keyState.version == prep.keyedDeltaPrevVersion
	var dataToSend []byte
	if useDelta {
		dataToSend = encodedDelta
	} else {
		dataToSend = encodedFull
	}

	if err := c.writeEncodedPushData(dataToSend, channel, pub.Key, protocol.FrameTypePushPublication, batchConfig); err != nil {
		// Enqueue failed (queue closed/overflow); client is being torn down.
		// Don't advance state — cleanupKeyed on close will drop it anyway.
		c.mu.Unlock()
		return
	}

	// Advance state. For delta channels, a successful FULL delivery also
	// flips deltaReady so subsequent broadcasts can use delta.
	if channelDelta && !keyState.deltaReady {
		keyState.deltaReady = true
	}
	keyState.version = pubVersion
	c.mu.Unlock()
}

// keyedWriteRemoval writes a removal publication and removes the key from
// per-connection tracking.
func (c *Client) keyedWriteRemoval(channel string, key string, pub *protocol.Publication) {
	c.mu.Lock()
	if c.keyed == nil {
		c.mu.Unlock()
		return
	}
	chanKeys, ok := c.keyed.trackedKeys[channel]
	if !ok {
		c.mu.Unlock()
		return
	}
	delete(chanKeys, key)
	c.mu.Unlock()

	data, err := c.encodeKeyedPush(channel, pub)
	if err != nil {
		return
	}

	var batchConfig ChannelBatchConfig
	if c.node.config.GetChannelBatchConfig != nil {
		batchConfig = c.node.config.GetChannelBatchConfig(channel)
	}
	_ = c.writePublication(channel, pub, preparedData{fullData: data}, StreamPosition{}, false, batchConfig)
}
