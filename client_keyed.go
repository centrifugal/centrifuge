package centrifuge

import (
	"context"
	"errors"
	"slices"
	"strings"
	"time"

	"github.com/centrifugal/centrifuge/internal/filter"
	"github.com/centrifugal/centrifuge/internal/recovery"
	"github.com/centrifugal/protocol"
)

// Keyed Subscriptions
//
// Keyed subscriptions provide synchronized state across clients. Unlike normal pub/sub
// subscriptions, keyed subscriptions maintain a snapshot of key-value state plus a stream
// of changes for recovery.
//
// Subscription protocol phases:
//
//  1. Snapshot phase: Client paginates through current key-value state. Each response
//     includes a stream position (offset/epoch) marking the snapshot's point in time.
//
//  2. Stream phase (optional): Client paginates through stream history to catch up on
//     changes since the snapshot was taken. May skip if snapshot is up-to-date.
//
//  3. Live phase: Server coordinates the transition to real-time:
//     - Starts buffering pub/sub messages before subscribing
//     - Subscribes to pub/sub channel
//     - Reads final stream catch-up since client's position
//     - Merges stream with buffered messages (deduplication by offset)
//     - Sends merged publications to client and enables live updates
//
// This buffering mechanism ensures no messages are lost during the gap between
// the final stream read and pub/sub subscription becoming active.
//
// Key differences from normal subscriptions:
//
// EnablePositioning and EnableRecovery options DO NOT APPLY to keyed subscriptions.
// These capabilities are inherent to the keyed subscription model:
//
//   - Positioning is always enabled: Keyed subscriptions always track stream position
//     (offset/epoch) because the snapshot+stream protocol requires it for consistency.
//
//   - Recovery is always enabled: The entire keyed subscription model is built around
//     state recovery. Clients receive a snapshot plus stream catch-up, ensuring they
//     can always recover the complete current state.
//
// When EnableKeyed is true in SubscribeOptions, the subscription automatically gets:
//   - Stream position tracking (equivalent to EnablePositioning)
//   - State recovery via snapshot + stream (superior to EnableRecovery)
//   - Delta compression support for stream catch-up (if negotiated)
//   - Tags filtering for stream and live publications (if allowed)
//
// Keyed Presence Subscriptions
//
// Keyed presence subscriptions allow clients to watch who is online in a channel.
// They are a special type of keyed subscription that tracks client or user presence.
//
// Two presence channel types (suffixes appended to base channel name):
//
//   - {channel}:clients - Tracks individual client connections. Each entry is keyed
//     by client ID and contains full ClientInfo. Use for tracking all connections,
//     including multiple connections from the same user.
//
//   - {channel}:users - Tracks unique users. Each entry is keyed by user ID with
//     minimal data (just the key). Provides natural deduplication when users have
//     multiple connections. TTL-based expiration provides grace period for reconnects.
//
// Enabling presence emission (server-side, in OnSubscribe for base channel):
//
//   - EmitKeyedClientPresence: true - Emits presence to {channel}:clients
//   - EmitKeyedUserPresence: true - Emits presence to {channel}:users
//   - KeyedPresenceAvailable: true - Tells client that presence channels exist
//
// Authorization flow:
//
// Presence subscriptions go through OnPresenceSubscribe handler (separate from OnSubscribe)
// to allow different permission checks. For example, a user might be allowed to subscribe
// to a chat channel but not see who else is in it.
//
// Client subscribes to presence channel -> OnPresenceSubscribe called with base channel
// -> Handler returns PresenceSubscribeReply{Allowed: true} -> Subscription proceeds
//
// Presence data lifecycle:
//
//   - On subscribe (with EmitKeyedClientPresence/EmitKeyedUserPresence): presence published
//   - Periodically refreshed via TTL to handle connection drops
//   - On unsubscribe/disconnect: presence removed (with stream entry for real-time notification)
//   - TTL expiration: automatic cleanup if client disappears without clean disconnect

// Keyed subscription phase constants.
const (
	KeyedPhaseSnapshot int32 = 0 // Paginating over snapshot (keyed state)
	KeyedPhaseStream   int32 = 1 // Paginating over stream (history catch-up)
	KeyedPhaseLive     int32 = 2 // Join live pub/sub, switch to real-time streaming
)

// keyedSubscribeState tracks state for keyed subscriptions that are still loading.
type keyedSubscribeState struct {
	options       SubscribeOptions // From OnSubscribe callback
	epoch         string           // Epoch from first response (for validation)
	isPresence    bool             // True if this is a presence subscription (:clients or :users suffix)
	subscribingCh chan struct{}    // Closed when subscription completes (for race handling)
	tagsFilter    *tagsFilter      // Tags filter for snapshot/stream publications
}

// handlePresenceSubscribe handles presence subscriptions (watching who is online).
// This is a separate permission scope from data subscriptions.
// Presence subscriptions use KeyedEngine internally but go through OnPresenceSubscribe handler.
func (c *Client) handlePresenceSubscribe(
	req *protocol.SubscribeRequest,
	cmd *protocol.Command,
	started time.Time,
	rw *replyWriter,
) error {
	presenceChannel := req.Channel

	// Validate that channel ends with proper suffix.
	if !strings.HasSuffix(presenceChannel, clientsSuffix) && !strings.HasSuffix(presenceChannel, usersSuffix) {
		c.writeDisconnectOrErrorFlush(presenceChannel, protocol.FrameTypeSubscribe, cmd, ErrorBadRequest, started, rw)
		return nil
	}

	// Extract base channel from presence channel.
	var baseChannel string
	if strings.HasSuffix(presenceChannel, clientsSuffix) {
		baseChannel = strings.TrimSuffix(presenceChannel, clientsSuffix)
	} else {
		baseChannel = strings.TrimSuffix(presenceChannel, usersSuffix)
	}

	keyedEngine := c.node.keyedEngine
	if keyedEngine == nil {
		c.writeDisconnectOrErrorFlush(presenceChannel, protocol.FrameTypeSubscribe, cmd, ErrorNotAvailable, started, rw)
		return nil
	}

	// Validate the request.
	replyError := c.validatePresenceSubscribeRequest(req)
	if replyError != nil {
		c.writeDisconnectOrErrorFlush(presenceChannel, protocol.FrameTypeSubscribe, cmd, replyError, started, rw)
		return nil
	}

	// Check if this is a continuation request (pagination or live phase).
	c.mu.RLock()
	state, hasState := c.keyedSubscribing[presenceChannel]
	c.mu.RUnlock()

	// For continuation requests, bypass the OnPresenceSubscribe callback.
	if hasState {
		if req.KeyedCursor != "" || req.KeyedPhase != KeyedPhaseSnapshot {
			reply := SubscribeReply{Options: state.options}
			if handleErr := c.handleKeyedSubscribe(req, reply, cmd, started, rw); handleErr != nil {
				c.writeDisconnectOrErrorFlush(presenceChannel, protocol.FrameTypeSubscribe, cmd, handleErr, started, rw)
			}
			return nil
		}
	}

	// Also check if already fully subscribed.
	c.mu.RLock()
	_, inChannels := c.channels[presenceChannel]
	c.mu.RUnlock()
	if inChannels {
		c.writeDisconnectOrErrorFlush(presenceChannel, protocol.FrameTypeSubscribe, cmd, ErrorAlreadySubscribed, started, rw)
		return nil
	}

	// Initial request - need to call handler for authorization.
	if c.eventHub.presenceSubscribeHandler == nil {
		c.writeDisconnectOrErrorFlush(presenceChannel, protocol.FrameTypeSubscribe, cmd, ErrorNotAvailable, started, rw)
		return nil
	}

	event := PresenceSubscribeEvent{
		Channel: baseChannel,
		Data:    req.Data,
	}

	cb := func(reply PresenceSubscribeReply, err error) {
		if err != nil {
			c.cleanupKeyedSubscribing(presenceChannel)
			c.writeDisconnectOrErrorFlush(presenceChannel, protocol.FrameTypeSubscribe, cmd, err, started, rw)
			return
		}

		// Build SubscribeReply with keyed options for presence.
		subscribeReply := SubscribeReply{
			Options: SubscribeOptions{
				EnableKeyed: true,
				ExpireAt:    reply.ExpireAt,
			},
		}

		// Track that we're subscribing (for continuation requests).
		c.mu.Lock()
		if c.keyedSubscribing == nil {
			c.keyedSubscribing = make(map[string]*keyedSubscribeState)
		}
		c.keyedSubscribing[presenceChannel] = &keyedSubscribeState{
			options:       subscribeReply.Options,
			isPresence:    true,
			subscribingCh: make(chan struct{}),
		}
		c.mu.Unlock()

		// Route to keyed subscription handler.
		if handleErr := c.handleKeyedSubscribe(req, subscribeReply, cmd, started, rw); handleErr != nil {
			c.cleanupKeyedSubscribing(presenceChannel)
			c.writeDisconnectOrErrorFlush(presenceChannel, protocol.FrameTypeSubscribe, cmd, handleErr, started, rw)
		}
	}

	c.eventHub.presenceSubscribeHandler(event, cb)
	return nil
}

// validatePresenceSubscribeRequest validates presence subscription request.
func (c *Client) validatePresenceSubscribeRequest(req *protocol.SubscribeRequest) *Error {
	presenceChannel := req.Channel

	config := c.node.config
	channelMaxLength := config.ChannelMaxLength
	channelLimit := config.ClientChannelLimit

	if channelMaxLength > 0 && len(presenceChannel) > channelMaxLength {
		c.node.logger.log(newLogEntry(LogLevelInfo, "channel too long for presence", map[string]any{
			"max": channelMaxLength, "channel": presenceChannel, "user": c.user, "client": c.uid,
		}))
		return ErrorBadRequest
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Check channel limit.
	numChannels := len(c.channels) + len(c.keyedSubscribing)
	if channelLimit > 0 && numChannels >= channelLimit {
		c.node.logger.log(newLogEntry(LogLevelInfo, "maximum limit of channels per client reached", map[string]any{
			"limit": channelLimit, "user": c.user, "client": c.uid,
		}))
		return ErrorLimitExceeded
	}

	return nil
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
		c.cleanupKeyedSubscribing(channel)
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
		// Validate and store tags filter on first request.
		var tf *tagsFilter
		if req.Tf != nil {
			if !reply.Options.AllowTagsFilter {
				c.node.logger.log(newLogEntry(LogLevelInfo, "tags filter not allowed for keyed channel", map[string]any{
					"channel": channel, "user": c.user, "client": c.uid,
				}))
				return ErrorBadRequest
			}
			if err := filter.Validate(req.Tf); err != nil {
				c.node.logger.log(newLogEntry(LogLevelInfo, "invalid tags filter for keyed channel", map[string]any{
					"channel": channel, "user": c.user, "client": c.uid,
				}))
				return ErrorBadRequest
			}
			tf = &tagsFilter{
				filter: req.Tf,
				hash:   filter.Hash(req.Tf),
			}
		}
		c.mu.Lock()
		if c.keyedSubscribing == nil {
			c.keyedSubscribing = make(map[string]*keyedSubscribeState)
		}
		c.keyedSubscribing[channel] = &keyedSubscribeState{
			options:       reply.Options,
			isPresence:    req.KeyedPresence,
			subscribingCh: make(chan struct{}),
			tagsFilter:    tf,
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
		c.cleanupKeyedSubscribing(channel)
		return ErrorInternal
	}

	// Get state for tags filter and epoch update.
	c.mu.RLock()
	state := c.keyedSubscribing[channel]
	c.mu.RUnlock()

	// Update epoch in state on first page.
	if req.KeyedCursor == "" && state != nil {
		c.mu.Lock()
		state.epoch = streamPos.Epoch
		c.mu.Unlock()
	}

	// Apply tags filter to snapshot publications.
	if state != nil && state.tagsFilter != nil {
		filteredPubs := make([]*Publication, 0, len(pubs))
		for _, pub := range pubs {
			match, _ := filter.Match(state.tagsFilter.filter, pub.Tags)
			if match {
				filteredPubs = append(filteredPubs, pub)
			}
		}
		pubs = filteredPubs
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
	_ SubscribeReply,
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
		c.cleanupKeyedSubscribing(channel)
		return ErrorInternal
	}

	// Apply tags filter to stream publications.
	if state.tagsFilter != nil {
		filteredPubs := make([]*Publication, 0, len(pubs))
		for _, pub := range pubs {
			match, _ := filter.Match(state.tagsFilter.filter, pub.Tags)
			if match {
				filteredPubs = append(filteredPubs, pub)
			}
		}
		pubs = filteredPubs
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
	var isPresence bool
	c.mu.RLock()
	state, hasState := c.keyedSubscribing[channel]
	c.mu.RUnlock()

	if hasState {
		opts = state.options
		isPresence = state.isPresence
		// Validate epoch if client provided one.
		if req.KeyedEpoch != "" && state.epoch != "" && req.KeyedEpoch != state.epoch {
			c.cleanupKeyedSubscribing(channel)
			return ErrorUnrecoverablePosition
		}
	} else {
		// Direct live subscription (no prior pagination).
		opts = reply.Options
		isPresence = req.KeyedPresence
	}

	// Start coordination: buffer -> subscribe -> read remaining -> merge.
	c.pubSubSync.StartBuffering(channel)

	// Subscribe to pub/sub via KeyedEngine.
	if err := keyedEngine.Subscribe(channel); err != nil {
		c.pubSubSync.StopBuffering(channel)
		c.cleanupKeyedSubscribing(channel)
		c.node.logger.log(newErrorLogEntry(err, "error subscribing to keyed channel", map[string]any{
			"channel": channel, "user": c.user, "client": c.uid,
		}))
		return ErrorInternal
	}

	// Add subscription to node hub.
	useID := opts.AllowChannelCompaction && req.Flag&subscriptionFlagChannelCompression != 0
	sub := subInfo{client: c, deltaType: deltaTypeNone, useID: useID}

	// Process tags filter if provided.
	if req.Tf != nil {
		if !opts.AllowTagsFilter {
			c.pubSubSync.StopBuffering(channel)
			_ = keyedEngine.Unsubscribe(channel)
			c.cleanupKeyedSubscribing(channel)
			c.node.logger.log(newLogEntry(LogLevelInfo, "tags filter not allowed for keyed channel", map[string]any{"channel": channel, "user": c.user, "client": c.uid}))
			return ErrorBadRequest
		}
		if err := filter.Validate(req.Tf); err != nil {
			c.pubSubSync.StopBuffering(channel)
			_ = keyedEngine.Unsubscribe(channel)
			c.cleanupKeyedSubscribing(channel)
			c.node.logger.log(newLogEntry(LogLevelInfo, "invalid tags filter for keyed channel", map[string]any{"channel": channel, "user": c.user, "client": c.uid}))
			return ErrorBadRequest
		}
		sub.tagsFilter = &tagsFilter{
			filter: req.Tf,
			hash:   filter.Hash(req.Tf),
		}
	}

	// Negotiate delta type if requested.
	var deltaEnabled bool
	if req.Delta != "" {
		dt := DeltaType(req.Delta)
		if slices.Contains(opts.AllowedDeltaTypes, dt) {
			deltaEnabled = true
			sub.deltaType = dt
		}
	}

	chanID, err := c.node.addSubscription(channel, sub)
	if err != nil {
		c.pubSubSync.StopBuffering(channel)
		_ = keyedEngine.Unsubscribe(channel)
		c.cleanupKeyedSubscribing(channel)
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
	if err != nil {
		c.pubSubSync.StopBuffering(channel)
		_ = c.node.removeSubscription(channel, c)
		_ = keyedEngine.Unsubscribe(channel)
		c.cleanupKeyedSubscribing(channel)
		if errors.Is(err, ErrorUnrecoverablePosition) {
			return ErrorUnrecoverablePosition
		}
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

	// Apply tags filter to stream publications (after offset calculation).
	if sub.tagsFilter != nil {
		filteredPubs := make([]*protocol.Publication, 0, len(recoveredPubs))
		for _, pub := range recoveredPubs {
			match, _ := filter.Match(sub.tagsFilter.filter, pub.Tags)
			if match {
				filteredPubs = append(filteredPubs, pub)
			}
		}
		recoveredPubs = filteredPubs
	}

	// Apply delta compression to recovered publications if enabled.
	// Unlike normal subs, we don't check for "Recovered" flag here because keyed subs
	// always error out on failed recovery (never reach this point with failed recovery).
	if deltaEnabled && req.Delta == string(DeltaTypeFossil) {
		recoveredPubs = c.makeRecoveredPubsDeltaFossil(recoveredPubs)
	}

	// Build response.
	res := &protocol.SubscribeResult{
		Keyed:                  true,
		KeyedPhase:             KeyedPhaseLive,
		Epoch:                  streamPos.Epoch,
		Offset:                 latestOffset,
		KeyedPresenceAvailable: opts.KeyedPresenceAvailable,
		Delta:                  deltaEnabled,
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
	if deltaEnabled && req.Delta == string(DeltaTypeFossil) {
		// Allow delta for following real-time publications. Recovery is implicitly
		// successful here - keyed subs error out earlier if recovery fails.
		channelFlags |= flagDeltaAllowed
	}
	if isPresence {
		channelFlags |= flagKeyedPresence
	}
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
	if opts.EmitKeyedClientPresence {
		channelFlags |= flagEmitKeyedClientPresence
	}
	if opts.EmitKeyedUserPresence {
		channelFlags |= flagEmitKeyedUserPresence
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
	if state, ok := c.keyedSubscribing[channel]; ok && state.subscribingCh != nil {
		close(state.subscribingCh)
	}
	delete(c.keyedSubscribing, channel)
	c.channels[channel] = channelContext
	c.mu.Unlock()

	// Stop buffering after response written.
	c.pubSubSync.StopBuffering(channel)

	// Add presence if enabled (uses KeyedEngine for keyed channels).
	// EmitPresence uses :presence suffix (legacy behavior for PresenceManager-like usage).
	if opts.EmitPresence {
		info := &ClientInfo{
			ClientID: c.uid,
			UserID:   c.user,
			ConnInfo: c.info,
			ChanInfo: opts.ChannelInfo,
		}
		if err = c.node.addPresence(channel, c.uid, info); err != nil {
			c.node.logger.log(newErrorLogEntry(err, "error adding client presence", map[string]any{
				"channel": channel, "user": c.user, "client": c.uid,
			}))
		}
	}

	// Add keyed client presence to {channel}:clients (key=clientId, full info).
	if opts.EmitKeyedClientPresence {
		info := &ClientInfo{
			ClientID: c.uid,
			UserID:   c.user,
			ConnInfo: c.info,
			ChanInfo: opts.ChannelInfo,
		}
		if err := c.addKeyedClientPresence(channel, info); err != nil {
			c.node.logger.log(newErrorLogEntry(err, "error adding keyed client presence", map[string]any{
				"channel": channel, "user": c.user, "client": c.uid,
			}))
		}
	}

	// Add keyed user presence to {channel}:users (key=userId, no info).
	if opts.EmitKeyedUserPresence {
		if err := c.addKeyedUserPresence(channel); err != nil {
			c.node.logger.log(newErrorLogEntry(err, "error adding keyed user presence", map[string]any{
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

const (
	clientsSuffix = ":clients" // For EmitKeyedClientPresence (key=clientId, full info)
	usersSuffix   = ":users"   // For EmitKeyedUserPresence (key=userId, no info)
)

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
	if state, ok := c.keyedSubscribing[channel]; ok {
		if state.subscribingCh != nil {
			close(state.subscribingCh)
		}
		delete(c.keyedSubscribing, channel)
	}
}

// cleanupKeyedSubscribingAll removes all in-progress keyed subscriptions on disconnect.
func (c *Client) cleanupKeyedSubscribingAll() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for channel, state := range c.keyedSubscribing {
		if state.subscribingCh != nil {
			close(state.subscribingCh)
		}
		delete(c.keyedSubscribing, channel)
	}
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

// addKeyedClientPresence adds client presence to {channel}:clients.
// Key is clientId, stores full ClientInfo.
func (c *Client) addKeyedClientPresence(channel string, info *ClientInfo) error {
	keyedEngine := c.node.keyedEngine
	if keyedEngine == nil {
		return ErrorNotAvailable
	}

	clientsChannel := channel + clientsSuffix

	// Use KeyModeIfNew with RefreshTTLOnSuppress to:
	// - Publish JOIN event only if this is a new presence entry
	// - Refresh TTL without publishing if entry already exists (quick reconnect)
	_, err := keyedEngine.Publish(c.ctx, clientsChannel, c.uid, KeyedPublishOptions{
		Publish:              true,
		ClientInfo:           info,
		KeyTTL:               c.keyedPresenceTTL(),
		StreamSize:           1000,
		StreamTTL:            300 * time.Second,
		StreamMetaTTL:        time.Hour,
		KeyMode:              KeyModeIfNew,
		RefreshTTLOnSuppress: true,
	})
	if err != nil {
		return err
	}
	return nil
}

// addKeyedUserPresence adds user presence to {channel}:users.
// Key is userId, no ClientInfo stored (just the key for uniqueness).
func (c *Client) addKeyedUserPresence(channel string) error {
	keyedEngine := c.node.keyedEngine
	if keyedEngine == nil {
		return ErrorNotAvailable
	}

	usersChannel := channel + usersSuffix

	// Use KeyModeIfNew with RefreshTTLOnSuppress to:
	// - Publish JOIN event only if this is a new user
	// - Refresh TTL without publishing if user already exists
	_, err := keyedEngine.Publish(c.ctx, usersChannel, c.user, KeyedPublishOptions{
		Publish:              true,
		KeyTTL:               c.keyedPresenceTTL(),
		StreamSize:           1000,
		StreamTTL:            300 * time.Second,
		StreamMetaTTL:        time.Hour,
		KeyMode:              KeyModeIfNew,
		RefreshTTLOnSuppress: true,
	})
	return err
}

// updateKeyedPresence updates presence for a keyed channel using KeyedEngine.
// This is called periodically by updateChannelPresence to refresh the TTL.
// Handles :clients and :users channels based on flags.
func (c *Client) updateKeyedPresence(channel string, info *ClientInfo, flags uint16) error {
	keyedEngine := c.node.keyedEngine
	if keyedEngine == nil {
		return ErrorNotAvailable
	}

	// Use KeyModeIfNew with RefreshTTLOnSuppress for TTL refresh:
	// - Since key already exists, publish is suppressed (no offset increment)
	// - TTL is refreshed without generating stream entries

	// Update :clients if EmitKeyedClientPresence is enabled.
	if channelHasFlag(flags, flagEmitKeyedClientPresence) {
		clientsChannel := channel + clientsSuffix
		_, err := keyedEngine.Publish(c.ctx, clientsChannel, c.uid, KeyedPublishOptions{
			Publish:              true,
			ClientInfo:           info,
			KeyTTL:               c.keyedPresenceTTL(),
			StreamSize:           1000,
			StreamTTL:            300 * time.Second,
			StreamMetaTTL:        time.Hour,
			KeyMode:              KeyModeIfNew,
			RefreshTTLOnSuppress: true,
		})
		if err != nil {
			return err
		}
	}

	// Update :users if EmitKeyedUserPresence is enabled.
	if channelHasFlag(flags, flagEmitKeyedUserPresence) {
		usersChannel := channel + usersSuffix
		_, err := keyedEngine.Publish(c.ctx, usersChannel, c.user, KeyedPublishOptions{
			Publish:              true,
			KeyTTL:               c.keyedPresenceTTL(),
			StreamSize:           1000,
			StreamTTL:            300 * time.Second,
			StreamMetaTTL:        time.Hour,
			KeyMode:              KeyModeIfNew,
			RefreshTTLOnSuppress: true,
		})
		if err != nil {
			return err
		}
	}

	return nil
}

// removeKeyedPresence removes presence for a keyed channel using KeyedEngine.
// Called on explicit unsubscribe or disconnect. Only removes from :presence and :clients,
// :users entries expire via TTL (acts as debounce for quick reconnects).
func (c *Client) removeKeyedPresence(channel string, flags uint16) error {
	keyedEngine := c.node.keyedEngine
	if keyedEngine == nil {
		return nil
	}

	// Remove from :presence if EmitPresence is enabled.
	if channelHasFlag(flags, flagEmitPresence) {
		if err := c.node.removePresence(channel, c.uid, c.user); err != nil {
			c.node.logger.log(newErrorLogEntry(err, "error removing channel presence", map[string]any{"channel": channel, "user": c.user, "client": c.uid, "error": err.Error()}))
		}
	}

	// Remove from :clients if EmitKeyedClientPresence is enabled.
	if channelHasFlag(flags, flagEmitKeyedClientPresence) {
		clientsChannel := channel + clientsSuffix
		_, err := keyedEngine.Unpublish(context.Background(), clientsChannel, c.uid, KeyedUnpublishOptions{
			Publish:       true,
			StreamSize:    1000,
			StreamTTL:     300 * time.Second,
			StreamMetaTTL: time.Hour,
		})
		if err != nil {
			c.node.logger.log(newErrorLogEntry(err, "error removing keyed clients presence", map[string]any{"channel": channel, "user": c.user, "client": c.uid, "error": err.Error()}))
		}
	}

	// :users entries are NOT removed on disconnect - they only expire via TTL.
	// This provides debounce/grace period for quick reconnects.

	return nil
}
