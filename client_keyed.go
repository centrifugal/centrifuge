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
	keyedTypeTrack   int32 = 1
	keyedTypeUntrack int32 = 2
)

// handleKeyedTrack processes SubRefreshRequest with type=keyedTypeTrack (track).
func (c *Client) handleKeyedTrack(req *protocol.SubRefreshRequest, cmd *protocol.Command, started time.Time, rw *replyWriter) error {
	channel := req.Channel

	if len(req.Items) == 0 {
		return ErrorBadRequest
	}

	// Check MaxKeysPerConnection limit.
	c.mu.RLock()
	var currentCount int
	if c.keyed != nil {
		currentCount = len(c.keyed.trackedKeys[channel])
	}
	c.mu.RUnlock()

	maxTracked := c.node.keyedManager.maxTrackedPerConnection(channel)
	if currentCount+len(req.Items) > maxTracked {
		return ErrorLimitExceeded
	}

	// Call OnKeyedTrack handler (Centrifugo validates HMAC).
	if c.eventHub.keyedTrackHandler == nil {
		return ErrorNotAvailable
	}

	items := make([]KeyedItem, len(req.Items))
	for i, it := range req.Items {
		items[i] = KeyedItem{Key: it.Key, Version: it.Version}
	}

	event := KeyedTrackEvent{
		Channel:   channel,
		UserID:    c.UserID(),
		Items:     items,
		Signature: req.Signature,
	}

	c.eventHub.keyedTrackHandler(event, func(reply KeyedTrackReply, err error) {
		if err != nil {
			c.writeDisconnectOrErrorFlush(channel, protocol.FrameTypeSubRefresh, cmd, err, started, rw)
			return
		}

		// Initialize keyed state if needed.
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

		// Store client-provided versions in per-connection state.
		for _, it := range req.Items {
			chanKeys[it.Key] = &keyedKeyState{version: it.Version, expireAt: reply.ExpireAt}
		}
		// Update fast-path hint for track expiry checks.
		if reply.ExpireAt > 0 {
			if c.keyed.minTrackExpireAt == nil {
				c.keyed.minTrackExpireAt = make(map[string]int64)
			}
			existing := c.keyed.minTrackExpireAt[channel]
			if existing == 0 || reply.ExpireAt < existing {
				c.keyed.minTrackExpireAt[channel] = reply.ExpireAt
			}
		}
		c.mu.Unlock()

		// Get or create keyed channel state.
		opts, ok := c.node.config.GetSharedPollChannelOptions(channel)
		if !ok {
			c.writeDisconnectOrErrorFlush(channel, protocol.FrameTypeSubRefresh, cmd, ErrorNotAvailable, started, rw)
			return
		}
		keyedOpts := opts.toKeyedChannelOptions()
		c.node.keyedManager.getOrCreateChannel(channel, keyedOpts)

		// Step 1: Register in SharedPollManager (ensures channel/key state exists).
		// Do NOT addSubscriber yet — client must not receive broadcasts before response.
		// Collect new (cold) keys with version=0 so we can auto-notify AFTER
		// addSubscriber. Only version=0 ("I have no data") warrants an immediate
		// backend call — clients with existing data can wait for the regular poll cycle.
		var newKeys []string
		for _, it := range req.Items {
			if c.node.sharedPollManager != nil {
				isNew, err := c.node.sharedPollManager.track(channel, opts, it.Key)
				if err != nil {
					c.writeDisconnectOrErrorFlush(channel, protocol.FrameTypeSubRefresh, cmd, ErrorInternal, started, rw)
					return
				}
				if isNew && it.Version == 0 {
					newKeys = append(newKeys, it.Key)
				}
			}
		}

		// Step 2: Collect cached data for items where server has newer version.
		var cachedItems []*protocol.Publication
		if c.node.sharedPollManager != nil {
			cachedItems = c.node.sharedPollManager.getCachedData(channel, items)
		}

		// Step 3: Update per-connection versions for cached items to prevent
		// duplicate delivery via subsequent broadcasts.
		if len(cachedItems) > 0 {
			c.mu.Lock()
			if c.keyed != nil {
				chanKeys := c.keyed.trackedKeys[channel]
				for _, pub := range cachedItems {
					if ks, ok := chanKeys[pub.Key]; ok {
						if pub.Version > ks.version {
							ks.version = pub.Version
							ks.deltaReady = true
						}
					}
				}
			}
			c.mu.Unlock()
		}

		// Step 4: Build and write response (enqueued before any broadcasts).
		res := &protocol.SubRefreshResult{}
		if reply.ExpireAt > 0 {
			nowUnix := time.Now().Unix()
			res.Expires = true
			if reply.ExpireAt > nowUnix {
				res.Ttl = uint32(reply.ExpireAt - nowUnix)
			}
		}
		if len(cachedItems) > 0 {
			res.Items = cachedItems
		}

		protoReply, err := c.getSubRefreshCommandReply(res)
		if err != nil {
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
		hub := c.node.keyedManager.getHub(channel)
		for _, it := range req.Items {
			hub.addSubscriber(it.Key, c)
		}

		// Step 6: Auto-notify cold keys AFTER addSubscriber so the broadcast
		// from the notified refresh can reach this client.
		if c.node.sharedPollManager != nil && len(newKeys) > 0 {
			for _, key := range newKeys {
				c.node.sharedPollManager.notify(channel, key)
			}
		}
	})
	return nil
}

// handleKeyedUntrack processes SubRefreshRequest with type=2 (untrack).
func (c *Client) handleKeyedUntrack(req *protocol.SubRefreshRequest, cmd *protocol.Command, started time.Time, rw *replyWriter) error {
	channel := req.Channel

	if len(req.UntrackKeys) == 0 {
		return ErrorBadRequest
	}

	c.mu.Lock()
	if c.keyed != nil {
		chanKeys := c.keyed.trackedKeys[channel]
		if chanKeys != nil {
			for _, key := range req.UntrackKeys {
				delete(chanKeys, key)
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
		for _, key := range req.UntrackKeys {
			keyEmpty := hub.removeSubscriber(key, c)
			if c.node.sharedPollManager != nil && keyEmpty {
				c.node.sharedPollManager.untrack(channel, key)
			}
		}
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

	// Determine delta behavior for this client+key.
	chState := c.keyed.channels[channel]
	channelDelta := chState != nil && chState.deltaType != deltaTypeNone
	sendDelta := channelDelta && prep.deltaSub && keyState.deltaReady
	if channelDelta && !keyState.deltaReady {
		keyState.deltaReady = true
	}
	keyState.version = pubVersion
	c.mu.Unlock()

	isJSON := c.transport.Protocol().toProto() == protocol.TypeJSON

	if sendDelta {
		// Delta path: lazily encode delta publication per-client protocol.
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
		var err error
		prep.localDeltaData, err = c.encodeKeyedPush(channel, deltaPub)
		if err != nil {
			return
		}
		// prep.deltaSub already true from buildPreparedPollData.
	} else {
		// Full path.
		prep.deltaSub = false
		pubToEncode := pub
		if channelDelta && isJSON {
			// JSON+delta: must JSON-escape data so client stores bytes for delta base.
			pubToEncode = &protocol.Publication{
				Data:    json.Escape(convert.BytesToString(pub.Data)),
				Key:     pub.Key,
				Version: pub.Version,
			}
		}
		var err error
		prep.fullData, err = c.encodeKeyedPush(channel, pubToEncode)
		if err != nil {
			return
		}
	}

	var batchConfig ChannelBatchConfig
	if c.node.config.GetChannelBatchConfig != nil {
		batchConfig = c.node.config.GetChannelBatchConfig(channel)
	}
	_ = c.writePublication(channel, pub, prep, StreamPosition{}, false, batchConfig)
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
