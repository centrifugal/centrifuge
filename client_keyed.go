package centrifuge

import (
	"context"
	"errors"
	"time"

	"github.com/centrifugal/centrifuge/internal/recovery"
	"github.com/centrifugal/protocol"
)

// Keyed subscription phase constants.
const (
	KeyedPhaseSnapshot int32 = 0 // Paginating over snapshot (keyed state)
	KeyedPhaseStream   int32 = 1 // Paginating over stream (history catch-up)
	KeyedPhaseLive     int32 = 2 // Join pub/sub, switch to real-time streaming
)

// keyedSubscribeState tracks state for keyed subscriptions that are still loading.
type keyedSubscribeState struct {
	options SubscribeOptions // From OnSubscribe callback
	epoch   string           // Epoch from first response (for validation)
}

// handleKeyedSubscribe routes keyed subscription requests to the appropriate phase handler.
// This is called after OnSubscribe callback has authorized the keyed subscription.
func (c *Client) handleKeyedSubscribe(
	req *protocol.SubscribeRequest,
	reply SubscribeReply,
	cmd *protocol.Command,
	started time.Time,
	rw *replyWriter,
) error {
	channel := req.Channel

	// Route based on phase.
	switch req.KeyedPhase {
	case KeyedPhaseSnapshot:
		return c.handleKeyedSnapshotPhase(req, reply, cmd, started, rw)
	case KeyedPhaseStream:
		return c.handleKeyedStreamPhase(req, reply, cmd, started, rw)
	case KeyedPhaseLive:
		return c.handleKeyedLivePhase(req, reply, cmd, started, rw)
	default:
		c.node.logger.log(newLogEntry(LogLevelInfo, "invalid keyed phase", map[string]any{
			"channel": channel, "phase": req.KeyedPhase, "user": c.user, "client": c.uid,
		}))
		return ErrorBadRequest
	}
}

// handleKeyedSnapshotPhase handles stateless snapshot pagination.
func (c *Client) handleKeyedSnapshotPhase(
	req *protocol.SubscribeRequest,
	reply SubscribeReply,
	cmd *protocol.Command,
	started time.Time,
	rw *replyWriter,
) error {
	channel := req.Channel

	keyedEngine := c.node.keyedEngine
	if keyedEngine == nil {
		return ErrorNotAvailable
	}

	// Acquire pagination lock for this channel.
	if !c.acquireKeyedPaginationLock(channel) {
		return ErrorConcurrentPagination
	}
	defer c.releaseKeyedPaginationLock(channel)

	// Track keyed subscription state on first snapshot request (no cursor).
	if req.KeyedCursor == "" {
		c.mu.Lock()
		if c.keyedSubscribing == nil {
			c.keyedSubscribing = make(map[string]*keyedSubscribeState)
		}
		c.keyedSubscribing[channel] = &keyedSubscribeState{
			options: reply.Options,
		}
		c.mu.Unlock()
	} else {
		// Subsequent request - verify we have authorization.
		c.mu.RLock()
		state, ok := c.keyedSubscribing[channel]
		c.mu.RUnlock()
		if !ok {
			c.node.logger.log(newLogEntry(LogLevelInfo, "keyed subscription not authorized", map[string]any{
				"channel": channel, "user": c.user, "client": c.uid,
			}))
			return ErrorPermissionDenied
		}
		// Use stored options.
		reply.Options = state.options
	}

	// Build read options.
	limit := int(req.KeyedLimit)
	if limit <= 0 {
		limit = 100 // Default limit.
	}
	// Cap at server max if configured.
	if c.node.config.KeyedMaxPaginationLimit > 0 && limit > c.node.config.KeyedMaxPaginationLimit {
		limit = c.node.config.KeyedMaxPaginationLimit
	}

	opts := KeyedReadSnapshotOptions{
		Cursor:  req.KeyedCursor,
		Limit:   limit,
		Ordered: req.KeyedOrdered,
	}

	// If client provided position, validate epoch.
	if req.KeyedOffset > 0 || req.KeyedEpoch != "" {
		opts.Revision = &StreamPosition{
			Offset: req.KeyedOffset,
			Epoch:  req.KeyedEpoch,
		}
	}

	// Read snapshot page.
	pubs, streamPos, nextCursor, err := keyedEngine.ReadSnapshot(c.ctx, channel, opts)
	if err != nil {
		if errors.Is(err, ErrorUnrecoverablePosition) {
			c.cleanupKeyedSubscribing(channel)
			return ErrorUnrecoverablePosition
		}
		c.node.logger.log(newErrorLogEntry(err, "error reading keyed snapshot", map[string]any{
			"channel": channel, "user": c.user, "client": c.uid,
		}))
		return ErrorInternal
	}

	// Update epoch in state on first page.
	if req.KeyedCursor == "" {
		c.mu.Lock()
		if state, ok := c.keyedSubscribing[channel]; ok {
			state.epoch = streamPos.Epoch
		}
		c.mu.Unlock()
	}

	// Build response.
	res := &protocol.SubscribeResult{
		Keyed:                  true,
		KeyedPhase:             KeyedPhaseSnapshot,
		KeyedCursor:            nextCursor,
		Epoch:                  streamPos.Epoch,
		Offset:                 streamPos.Offset,
		KeyedPresenceAvailable: reply.Options.KeyedPresenceAvailable,
	}

	// Convert publications.
	protoPubs := make([]*protocol.Publication, 0, len(pubs))
	for _, pub := range pubs {
		protoPubs = append(protoPubs, pubToProto(pub))
	}
	res.Publications = protoPubs

	return c.writeKeyedSubscribeReply(channel, cmd, res, started, rw)
}

// handleKeyedStreamPhase handles stateless stream pagination (history catch-up).
func (c *Client) handleKeyedStreamPhase(
	req *protocol.SubscribeRequest,
	reply SubscribeReply,
	cmd *protocol.Command,
	started time.Time,
	rw *replyWriter,
) error {
	channel := req.Channel

	keyedEngine := c.node.keyedEngine
	if keyedEngine == nil {
		return ErrorNotAvailable
	}

	// Verify we have authorization (must have started keyed subscription).
	c.mu.RLock()
	state, ok := c.keyedSubscribing[channel]
	c.mu.RUnlock()
	if !ok {
		c.node.logger.log(newLogEntry(LogLevelInfo, "keyed subscription not authorized for stream", map[string]any{
			"channel": channel, "user": c.user, "client": c.uid,
		}))
		return ErrorPermissionDenied
	}

	// Acquire pagination lock for this channel.
	if !c.acquireKeyedPaginationLock(channel) {
		return ErrorConcurrentPagination
	}
	defer c.releaseKeyedPaginationLock(channel)

	// Validate epoch if provided.
	if req.KeyedEpoch != "" && state.epoch != "" && req.KeyedEpoch != state.epoch {
		c.cleanupKeyedSubscribing(channel)
		return ErrorUnrecoverablePosition
	}

	// Build read options.
	limit := int(req.KeyedLimit)
	if limit <= 0 {
		limit = 500 // Default stream limit.
	}
	if c.node.config.KeyedMaxPaginationLimit > 0 && limit > c.node.config.KeyedMaxPaginationLimit {
		limit = c.node.config.KeyedMaxPaginationLimit
	}

	opts := KeyedReadStreamOptions{
		Filter: StreamFilter{
			Since: &StreamPosition{
				Offset: req.KeyedOffset,
				Epoch:  req.KeyedEpoch,
			},
			Limit: limit,
		},
		MetaTTL: state.options.HistoryMetaTTL,
	}

	// Read stream.
	pubs, streamPos, err := keyedEngine.ReadStream(c.ctx, channel, opts)
	if err != nil {
		if errors.Is(err, ErrorUnrecoverablePosition) {
			c.cleanupKeyedSubscribing(channel)
			return ErrorUnrecoverablePosition
		}
		c.node.logger.log(newErrorLogEntry(err, "error reading keyed stream", map[string]any{
			"channel": channel, "user": c.user, "client": c.uid,
		}))
		return ErrorInternal
	}

	// Build response.
	res := &protocol.SubscribeResult{
		Keyed:      true,
		KeyedPhase: KeyedPhaseStream,
		Epoch:      streamPos.Epoch,
		Offset:     streamPos.Offset,
	}

	// Convert publications.
	protoPubs := make([]*protocol.Publication, 0, len(pubs))
	for _, pub := range pubs {
		protoPubs = append(protoPubs, pubToProto(pub))
	}
	res.Publications = protoPubs

	return c.writeKeyedSubscribeReply(channel, cmd, res, started, rw)
}

// handleKeyedLivePhase handles joining pub/sub with coordination.
func (c *Client) handleKeyedLivePhase(
	req *protocol.SubscribeRequest,
	reply SubscribeReply,
	cmd *protocol.Command,
	started time.Time,
	rw *replyWriter,
) error {
	channel := req.Channel

	keyedEngine := c.node.keyedEngine
	if keyedEngine == nil {
		return ErrorNotAvailable
	}

	// Get stored state if exists (for two-phase), or use reply options (for direct live).
	var opts SubscribeOptions
	c.mu.RLock()
	state, hasState := c.keyedSubscribing[channel]
	c.mu.RUnlock()

	if hasState {
		opts = state.options
		// Validate epoch if client provided one.
		if req.KeyedEpoch != "" && state.epoch != "" && req.KeyedEpoch != state.epoch {
			c.cleanupKeyedSubscribing(channel)
			return ErrorUnrecoverablePosition
		}
	} else {
		// Direct live subscription (no prior pagination).
		opts = reply.Options
	}

	// Start coordination: buffer -> subscribe -> read remaining -> merge.
	c.pubSubSync.StartBuffering(channel)

	// Subscribe to pub/sub via KeyedEngine.
	if err := keyedEngine.Subscribe(channel); err != nil {
		c.pubSubSync.StopBuffering(channel)
		c.node.logger.log(newErrorLogEntry(err, "error subscribing to keyed channel", map[string]any{
			"channel": channel, "user": c.user, "client": c.uid,
		}))
		return ErrorInternal
	}

	// Add subscription to node hub.
	useID := opts.AllowChannelCompaction && req.Flag&subscriptionFlagChannelCompression != 0
	sub := subInfo{client: c, deltaType: deltaTypeNone, useID: useID}
	chanID, err := c.node.addSubscription(channel, sub)
	if err != nil {
		c.pubSubSync.StopBuffering(channel)
		_ = keyedEngine.Unsubscribe(channel)
		c.node.logger.log(newErrorLogEntry(err, "error adding keyed subscription", map[string]any{
			"channel": channel, "user": c.user, "client": c.uid,
		}))
		var clientErr *Error
		if errors.As(err, &clientErr) && !errors.Is(clientErr, ErrorInternal) {
			return clientErr
		}
		return ErrorInternal
	}

	// Read remaining stream publications since client's offset.
	var recoveredPubs []*protocol.Publication
	streamOpts := KeyedReadStreamOptions{
		Filter: StreamFilter{
			Since: &StreamPosition{
				Offset: req.KeyedOffset,
				Epoch:  req.KeyedEpoch,
			},
			Limit: -1, // No limit for final catch-up.
		},
		MetaTTL: opts.HistoryMetaTTL,
	}

	pubs, streamPos, err := keyedEngine.ReadStream(c.ctx, channel, streamOpts)
	if err != nil && !errors.Is(err, ErrorUnrecoverablePosition) {
		c.pubSubSync.StopBuffering(channel)
		_ = c.node.removeSubscription(channel, c)
		_ = keyedEngine.Unsubscribe(channel)
		c.node.logger.log(newErrorLogEntry(err, "error reading stream for live phase", map[string]any{
			"channel": channel, "user": c.user, "client": c.uid,
		}))
		return ErrorInternal
	}

	// Convert to protocol publications.
	for _, pub := range pubs {
		recoveredPubs = append(recoveredPubs, pubToProto(pub))
	}

	// Lock buffer and read buffered publications.
	bufferedPubs := c.pubSubSync.LockBufferAndReadBuffered(channel)

	// Merge recovered and buffered publications.
	var maxSeenOffset uint64
	var okMerge bool
	recoveredPubs, maxSeenOffset, okMerge = recovery.MergePublications(recoveredPubs, bufferedPubs)
	if !okMerge {
		c.pubSubSync.StopBuffering(channel)
		_ = c.node.removeSubscription(channel, c)
		_ = keyedEngine.Unsubscribe(channel)
		c.cleanupKeyedSubscribing(channel)
		return &DisconnectInsufficientState
	}

	// Update offset if we saw higher.
	latestOffset := streamPos.Offset
	if maxSeenOffset > latestOffset {
		latestOffset = maxSeenOffset
	}
	if len(recoveredPubs) > 0 {
		lastPubOffset := recoveredPubs[len(recoveredPubs)-1].Offset
		if lastPubOffset > latestOffset {
			latestOffset = lastPubOffset
		}
	}

	// Build response.
	res := &protocol.SubscribeResult{
		Keyed:                  true,
		KeyedPhase:             KeyedPhaseLive,
		Epoch:                  streamPos.Epoch,
		Offset:                 latestOffset,
		KeyedPresenceAvailable: opts.KeyedPresenceAvailable,
	}
	if chanID > 0 {
		res.Id = chanID
	}
	res.Publications = recoveredPubs

	// Write response before stopping buffer.
	protoReply, err := c.getSubscribeCommandReply(res)
	if err != nil {
		c.pubSubSync.StopBuffering(channel)
		_ = c.node.removeSubscription(channel, c)
		_ = keyedEngine.Unsubscribe(channel)
		c.cleanupKeyedSubscribing(channel)
		c.node.logger.log(newErrorLogEntry(err, "error encoding keyed subscribe reply", map[string]any{
			"channel": channel, "user": c.user, "client": c.uid,
		}))
		return ErrorInternal
	}

	c.writeEncodedCommandReply(channel, protocol.FrameTypeSubscribe, cmd, protoReply, rw)
	c.handleCommandFinished(cmd, protocol.FrameTypeSubscribe, nil, protoReply, started, "")
	c.releaseSubscribeCommandReply(protoReply)

	// Build channel context with keyed flag.
	var channelFlags uint16
	channelFlags |= flagSubscribed
	channelFlags |= flagPositioning // Keyed subscriptions are always positioned.
	channelFlags |= flagKeyed       // Mark as keyed subscription.
	if opts.EmitPresence {
		channelFlags |= flagEmitPresence
	}
	if opts.EmitJoinLeave {
		channelFlags |= flagEmitJoinLeave
	}
	if opts.PushJoinLeave {
		channelFlags |= flagPushJoinLeave
	}
	if reply.ClientSideRefresh {
		channelFlags |= flagClientSideRefresh
	}

	channelContext := ChannelContext{
		flags:    channelFlags,
		expireAt: opts.ExpireAt,
		info:     opts.ChannelInfo,
		streamPosition: StreamPosition{
			Offset: latestOffset,
			Epoch:  streamPos.Epoch,
		},
		metaTTLSeconds:    int64(opts.HistoryMetaTTL.Seconds()),
		positionCheckTime: time.Now().Unix(),
		Source:            opts.Source,
	}

	// Move from keyedSubscribing to channels.
	c.mu.Lock()
	delete(c.keyedSubscribing, channel)
	c.channels[channel] = channelContext
	c.mu.Unlock()

	// Stop buffering after response written.
	c.pubSubSync.StopBuffering(channel)

	// Add presence if enabled (uses KeyedEngine for keyed channels).
	if opts.EmitPresence {
		info := &ClientInfo{
			ClientID: c.uid,
			UserID:   c.user,
			ConnInfo: c.info,
			ChanInfo: opts.ChannelInfo,
		}
		if err := c.addKeyedPresence(channel, info); err != nil {
			c.node.logger.log(newErrorLogEntry(err, "error adding keyed presence", map[string]any{
				"channel": channel, "user": c.user, "client": c.uid,
			}))
		}
	}

	// Emit join event if enabled.
	if opts.EmitJoinLeave {
		info := &ClientInfo{
			ClientID: c.uid,
			UserID:   c.user,
			ConnInfo: c.info,
			ChanInfo: opts.ChannelInfo,
		}
		go func() { _ = c.node.publishJoin(channel, info) }()
	}

	return nil
}

// handlePresenceSubscribe handles presence subscriptions as first-class independent subscriptions.
// Presence subscriptions have their own lifecycle and permission callback.
func (c *Client) handlePresenceSubscribe(
	req *protocol.SubscribeRequest,
	cmd *protocol.Command,
	started time.Time,
	rw *replyWriter,
) error {
	channel := req.Channel

	if c.eventHub.presenceSubscribeHandler == nil {
		c.writeDisconnectOrErrorFlush(channel, protocol.FrameTypeSubscribe, cmd, ErrorNotAvailable, started, rw)
		return nil
	}

	keyedEngine := c.node.keyedEngine
	if keyedEngine == nil {
		c.writeDisconnectOrErrorFlush(channel, protocol.FrameTypeSubscribe, cmd, ErrorNotAvailable, started, rw)
		return nil
	}

	// Presence channel is the original channel - presence data is stored in derived channel.
	presenceChannel := channel + ":presence"

	// Validate presence subscription request.
	replyError := c.validatePresenceSubscribeRequest(req)
	if replyError != nil {
		c.writeDisconnectOrErrorFlush(channel, protocol.FrameTypeSubscribe, cmd, replyError, started, rw)
		return nil
	}

	event := PresenceSubscribeEvent{
		Channel: channel,
		Data:    req.Data,
	}

	cb := func(reply PresenceSubscribeReply, err error) {
		if err != nil {
			c.writeDisconnectOrErrorFlush(channel, protocol.FrameTypeSubscribe, cmd, err, started, rw)
			return
		}

		if !reply.Allowed {
			c.writeDisconnectOrErrorFlush(channel, protocol.FrameTypeSubscribe, cmd, ErrorPermissionDenied, started, rw)
			return
		}

		// Route to snapshot or live handler based on phase.
		var handleErr error
		switch req.KeyedPhase {
		case KeyedPhaseSnapshot:
			handleErr = c.handlePresenceSnapshotPhase(req, cmd, started, rw, presenceChannel)
		case KeyedPhaseLive:
			handleErr = c.handlePresenceLivePhase(req, cmd, started, rw, presenceChannel)
		default:
			handleErr = ErrorBadRequest
		}

		if handleErr != nil {
			c.writeDisconnectOrErrorFlush(channel, protocol.FrameTypeSubscribe, cmd, handleErr, started, rw)
		}
	}

	c.eventHub.presenceSubscribeHandler(event, cb)
	return nil
}

// validatePresenceSubscribeRequest validates presence subscription request.
func (c *Client) validatePresenceSubscribeRequest(req *protocol.SubscribeRequest) *Error {
	channel := req.Channel
	presenceChannel := channel + ":presence"

	config := c.node.config
	channelMaxLength := config.ChannelMaxLength

	if channelMaxLength > 0 && len(channel) > channelMaxLength {
		c.node.logger.log(newLogEntry(LogLevelInfo, "channel too long for presence", map[string]any{
			"max": channelMaxLength, "channel": channel, "user": c.user, "client": c.uid,
		}))
		return ErrorBadRequest
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if already subscribing to presence on this channel.
	if _, inSubscribing := c.keyedPresenceSubscribing[presenceChannel]; inSubscribing {
		// Allow continuation requests (cursor set or live phase).
		if req.KeyedCursor != "" || req.KeyedPhase != KeyedPhaseSnapshot {
			return nil
		}
		c.node.logger.log(newLogEntry(LogLevelInfo, "client already subscribing to presence", map[string]any{
			"channel": channel, "user": c.user, "client": c.uid,
		}))
		return ErrorAlreadySubscribed
	}

	// Check if already subscribed to presence on this channel.
	if _, inSubs := c.keyedPresenceSubs[presenceChannel]; inSubs {
		c.node.logger.log(newLogEntry(LogLevelInfo, "client already subscribed to presence", map[string]any{
			"channel": channel, "user": c.user, "client": c.uid,
		}))
		return ErrorAlreadySubscribed
	}

	// Track that we're subscribing to presence (for initial snapshot request).
	if req.KeyedCursor == "" && req.KeyedPhase == KeyedPhaseSnapshot {
		if c.keyedPresenceSubscribing == nil {
			c.keyedPresenceSubscribing = make(map[string]struct{})
		}
		c.keyedPresenceSubscribing[presenceChannel] = struct{}{}
	}

	return nil
}

// handlePresenceSnapshotPhase handles presence snapshot pagination.
func (c *Client) handlePresenceSnapshotPhase(
	req *protocol.SubscribeRequest,
	cmd *protocol.Command,
	started time.Time,
	rw *replyWriter,
	presenceChannel string,
) error {
	channel := req.Channel

	keyedEngine := c.node.keyedEngine
	if keyedEngine == nil {
		return ErrorNotAvailable
	}

	// Acquire pagination lock.
	if !c.acquireKeyedPaginationLock(presenceChannel) {
		return ErrorConcurrentPagination
	}
	defer c.releaseKeyedPaginationLock(presenceChannel)

	// Build read options.
	limit := int(req.KeyedLimit)
	if limit <= 0 {
		limit = 100
	}
	if c.node.config.KeyedMaxPaginationLimit > 0 && limit > c.node.config.KeyedMaxPaginationLimit {
		limit = c.node.config.KeyedMaxPaginationLimit
	}

	opts := KeyedReadSnapshotOptions{
		Cursor:  req.KeyedCursor,
		Limit:   limit,
		Ordered: req.KeyedOrdered,
	}

	// Read presence snapshot.
	pubs, streamPos, nextCursor, err := keyedEngine.ReadSnapshot(c.ctx, presenceChannel, opts)
	if err != nil {
		c.node.logger.log(newErrorLogEntry(err, "error reading presence snapshot", map[string]any{
			"channel": channel, "user": c.user, "client": c.uid,
		}))
		return ErrorInternal
	}

	res := &protocol.SubscribeResult{
		Keyed:       true,
		KeyedPhase:  KeyedPhaseSnapshot,
		KeyedCursor: nextCursor,
		Epoch:       streamPos.Epoch,
		Offset:      streamPos.Offset,
	}

	protoPubs := make([]*protocol.Publication, 0, len(pubs))
	for _, pub := range pubs {
		protoPubs = append(protoPubs, pubToProto(pub))
	}
	res.Publications = protoPubs

	return c.writeKeyedSubscribeReply(channel, cmd, res, started, rw)
}

// handlePresenceLivePhase handles joining presence pub/sub.
func (c *Client) handlePresenceLivePhase(
	req *protocol.SubscribeRequest,
	cmd *protocol.Command,
	started time.Time,
	rw *replyWriter,
	presenceChannel string,
) error {
	channel := req.Channel

	keyedEngine := c.node.keyedEngine
	if keyedEngine == nil {
		return ErrorNotAvailable
	}

	// Subscribe to presence pub/sub.
	if err := keyedEngine.Subscribe(presenceChannel); err != nil {
		c.node.logger.log(newErrorLogEntry(err, "error subscribing to presence channel", map[string]any{
			"channel": channel, "user": c.user, "client": c.uid,
		}))
		return ErrorInternal
	}

	// Move from subscribing to subscribed.
	c.mu.Lock()
	delete(c.keyedPresenceSubscribing, presenceChannel)
	if c.keyedPresenceSubs == nil {
		c.keyedPresenceSubs = make(map[string]struct{})
	}
	c.keyedPresenceSubs[presenceChannel] = struct{}{}
	c.mu.Unlock()

	res := &protocol.SubscribeResult{
		Keyed:      true,
		KeyedPhase: KeyedPhaseLive,
	}

	return c.writeKeyedSubscribeReply(channel, cmd, res, started, rw)
}

// writeKeyedSubscribeReply writes a keyed subscribe result to the client.
func (c *Client) writeKeyedSubscribeReply(
	channel string,
	cmd *protocol.Command,
	res *protocol.SubscribeResult,
	started time.Time,
	rw *replyWriter,
) error {
	protoReply, err := c.getSubscribeCommandReply(res)
	if err != nil {
		c.node.logger.log(newErrorLogEntry(err, "error encoding keyed subscribe reply", map[string]any{
			"channel": channel, "user": c.user, "client": c.uid,
		}))
		return ErrorInternal
	}
	c.writeEncodedCommandReply(channel, protocol.FrameTypeSubscribe, cmd, protoReply, rw)
	c.handleCommandFinished(cmd, protocol.FrameTypeSubscribe, nil, protoReply, started, "")
	c.releaseSubscribeCommandReply(protoReply)
	return nil
}

// acquireKeyedPaginationLock tries to acquire a pagination lock for the channel.
// Returns true if lock acquired, false if another pagination is in progress.
func (c *Client) acquireKeyedPaginationLock(channel string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.keyedPaginationLocks == nil {
		c.keyedPaginationLocks = make(map[string]struct{})
	}
	if _, locked := c.keyedPaginationLocks[channel]; locked {
		return false
	}
	c.keyedPaginationLocks[channel] = struct{}{}
	return true
}

// releaseKeyedPaginationLock releases the pagination lock for the channel.
func (c *Client) releaseKeyedPaginationLock(channel string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.keyedPaginationLocks, channel)
}

// cleanupKeyedSubscribing removes keyed subscribing state for a channel.
func (c *Client) cleanupKeyedSubscribing(channel string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.keyedSubscribing, channel)
}

// keyedPresenceTTL returns TTL for keyed presence entries.
// Uses 3x the presence update interval to allow for some delay while still expiring if updates stop.
func (c *Client) keyedPresenceTTL() time.Duration {
	ttl := 3 * c.node.config.ClientPresenceUpdateInterval
	if ttl < 30*time.Second {
		ttl = 30 * time.Second // Minimum 30 seconds.
	}
	return ttl
}

// addKeyedPresence adds presence for a keyed channel using KeyedEngine.
// This is called when a keyed subscription becomes live with EmitPresence enabled.
func (c *Client) addKeyedPresence(channel string, info *ClientInfo) error {
	keyedEngine := c.node.keyedEngine
	if keyedEngine == nil {
		return ErrorNotAvailable
	}

	presenceChannel := channel + ":presence"

	// Use KeyedPublishOptions.ClientInfo - data is not used for keyed presence.
	_, err := keyedEngine.Publish(c.ctx, presenceChannel, c.uid, nil, KeyedPublishOptions{
		Publish:    true,
		ClientInfo: info,
		KeyTTL:     c.keyedPresenceTTL(),
	})
	return err
}

// updateKeyedPresence updates presence for a keyed channel using KeyedEngine.
// This is called periodically by updateChannelPresence.
func (c *Client) updateKeyedPresence(channel string, info *ClientInfo) error {
	keyedEngine := c.node.keyedEngine
	if keyedEngine == nil {
		return ErrorNotAvailable
	}

	presenceChannel := channel + ":presence"

	// Use KeyedPublishOptions.ClientInfo - data is not used for keyed presence.
	_, err := keyedEngine.Publish(c.ctx, presenceChannel, c.uid, nil, KeyedPublishOptions{
		Publish:    true,
		ClientInfo: info,
		KeyTTL:     c.keyedPresenceTTL(),
	})
	return err
}

// removeKeyedPresence removes presence for a keyed channel using KeyedEngine.
func (c *Client) removeKeyedPresence(channel string) error {
	keyedEngine := c.node.keyedEngine
	if keyedEngine == nil {
		return nil
	}

	presenceChannel := channel + ":presence"
	_, err := keyedEngine.Unpublish(context.Background(), presenceChannel, c.uid, KeyedUnpublishOptions{
		Publish: true,
	})
	return err
}

// cleanupKeyedPresence removes presence for all keyed channels on disconnect.
func (c *Client) cleanupKeyedPresence() {
	keyedEngine := c.node.keyedEngine
	if keyedEngine == nil {
		return
	}

	c.mu.RLock()
	channels := make([]string, 0, len(c.channels))
	for ch, ctx := range c.channels {
		// Only clean up keyed channels with EmitPresence enabled.
		if channelHasFlag(ctx.flags, flagKeyed) && channelHasFlag(ctx.flags, flagEmitPresence) {
			channels = append(channels, ch)
		}
	}
	c.mu.RUnlock()

	// Remove presence for channels where it was maintained.
	for _, ch := range channels {
		_ = c.removeKeyedPresence(ch)
	}
}

// cleanupKeyedPresenceSubs unsubscribes from all presence subscriptions.
func (c *Client) cleanupKeyedPresenceSubs() {
	keyedEngine := c.node.keyedEngine
	if keyedEngine == nil {
		return
	}

	c.mu.RLock()
	// Collect presence channels to unsubscribe from.
	presenceSubs := make([]string, 0, len(c.keyedPresenceSubs))
	for presenceChannel := range c.keyedPresenceSubs {
		presenceSubs = append(presenceSubs, presenceChannel)
	}
	// Also clean up any in-progress presence subscriptions.
	for presenceChannel := range c.keyedPresenceSubscribing {
		presenceSubs = append(presenceSubs, presenceChannel)
	}
	c.mu.RUnlock()

	// Unsubscribe from all presence channels.
	for _, presenceChannel := range presenceSubs {
		_ = keyedEngine.Unsubscribe(presenceChannel)
	}
}
