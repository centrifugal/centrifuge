package centrifuge

import (
	"context"
	"errors"
	"slices"
	"time"

	"github.com/centrifugal/centrifuge/internal/filter"
	"github.com/centrifugal/centrifuge/internal/recovery"
	"github.com/centrifugal/protocol"
)

// Map Subscriptions
//
// Map subscriptions provide synchronized state across clients. Unlike normal pub/sub
// subscriptions, map subscriptions maintain a state of key-value entries plus a stream
// of changes for recovery.
//
// Subscription protocol phases:
//
//  1. State phase: Client paginates through current key-value state. Each response
//     includes a stream position (offset/epoch) marking the state's point in time.
//
//  2. Stream phase (optional): Client paginates through stream history to catch up on
//     changes since the state was taken. May skip if state is up-to-date.
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
// EnablePositioning and EnableRecovery options DO NOT APPLY to map subscriptions.
// These capabilities are inherent to the map subscription model:
//
//   - Positioning is always enabled: Map subscriptions always track stream position
//     (offset/epoch) because the state+stream protocol requires it for consistency.
//
//   - Recovery is always enabled: The entire map subscription model is built around
//     state recovery. Clients receive a state plus stream catch-up, ensuring they
//     can always recover the complete current state.
//
// When EnableMap is true in SubscribeOptions, the subscription automatically gets:
//   - Stream position tracking (equivalent to EnablePositioning)
//   - State recovery via state + stream (superior to EnableRecovery)
//   - Delta compression support for stream catch-up (if negotiated)
//   - Tags filtering for stream and live publications (if allowed)
//
// Map Presence Subscriptions
//
// Map presence subscriptions allow clients to watch who is online in a channel.
// They are a special type of map subscription that tracks client or user presence.
//
// Presence is configured using channel prefixes:
//
//   - MapClientPresenceChannelPrefix (e.g., "$clients:") - When set, client presence
//     is published to {prefix}{channel}. Each entry is keyed by client ID and contains
//     full ClientInfo. Use for tracking individual connections.
//
//   - MapUserPresenceChannelPrefix (e.g., "$users:") - When set, user presence is
//     published to {prefix}{channel}. Each entry is keyed by user ID with minimal data.
//     Provides natural deduplication when users have multiple connections.
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
//   - On subscribe (with configured presence prefix): presence published
//   - Periodically refreshed via TTL to handle connection drops
//   - On unsubscribe/disconnect: presence removed (with stream entry for real-time notification)
//   - TTL expiration: automatic cleanup if client disappears without clean disconnect

// Map subscription phase constants.
const (
	MapPhaseLive   int32 = 0 // Join live pub/sub, switch to real-time streaming (default)
	MapPhaseStream int32 = 1 // Paginating over stream (history catch-up)
	MapPhaseState  int32 = 2 // Paginating over state (map state)
)

// mapSubscribeState tracks state for map subscriptions that are still loading.
type mapSubscribeState struct {
	options             SubscribeOptions // From OnSubscribe callback
	epoch               string           // Epoch from first response (for validation)
	streamStart         uint64           // Stream top captured on first stream request
	streamStartCaptured bool             // True after streamStart is captured (since 0 is valid offset)
	isPresence          bool             // True if this is a presence subscription
	subscribingCh       chan struct{}    // Closed when subscription completes (for race handling)
	tagsFilter          *tagsFilter      // Tags filter for state/stream publications
}

// handleMapSubscribe routes map subscription requests to the appropriate phase handler.
// This is called after OnSubscribe callback has authorized the map subscription.
func (c *Client) handleMapSubscribe(
	req *protocol.SubscribeRequest,
	reply SubscribeReply,
	cmd *protocol.Command,
	started time.Time,
	rw *replyWriter,
) error {
	channel := req.Channel

	// Route based on phase.
	switch req.Phase {
	case MapPhaseState:
		return c.handleMapStatePhase(req, reply, cmd, started, rw)
	case MapPhaseStream:
		return c.handleMapStreamPhase(req, reply, cmd, started, rw)
	case MapPhaseLive:
		return c.handleMapLivePhase(req, reply, cmd, started, rw)
	default:
		c.cleanupMapSubscribing(channel)
		c.node.logger.log(newLogEntry(LogLevelInfo, "invalid map phase", map[string]any{
			"channel": channel, "phase": req.Phase, "user": c.user, "client": c.uid,
		}))
		return ErrorBadRequest
	}
}

// handleMapStatePhase handles stateless state pagination.
func (c *Client) handleMapStatePhase(
	req *protocol.SubscribeRequest,
	reply SubscribeReply,
	cmd *protocol.Command,
	started time.Time,
	rw *replyWriter,
) error {
	channel := req.Channel

	// Acquire pagination lock for this channel.
	if !c.acquireMapPaginationLock(channel) {
		return ErrorConcurrentPagination
	}
	defer c.releaseMapPaginationLock(channel)

	// Track map subscription state on first state request (no cursor).
	if req.Cursor == "" {
		// Validate and store tags filter on first request.
		var tf *tagsFilter
		if req.Tf != nil {
			if !reply.Options.AllowTagsFilter {
				c.node.logger.log(newLogEntry(LogLevelInfo, "tags filter not allowed for map channel", map[string]any{
					"channel": channel, "user": c.user, "client": c.uid,
				}))
				return ErrorBadRequest
			}
			if err := filter.Validate(req.Tf); err != nil {
				c.node.logger.log(newLogEntry(LogLevelInfo, "invalid tags filter for map channel", map[string]any{
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
		if c.mapSubscribing == nil {
			c.mapSubscribing = make(map[string]*mapSubscribeState)
		}
		c.mapSubscribing[channel] = &mapSubscribeState{
			options:       reply.Options,
			isPresence:    req.Type >= 2,
			subscribingCh: make(chan struct{}),
			tagsFilter:    tf,
		}
		c.mu.Unlock()
	} else {
		// Subsequent request - verify we have authorization.
		c.mu.RLock()
		state, ok := c.mapSubscribing[channel]
		c.mu.RUnlock()
		if !ok {
			c.node.logger.log(newLogEntry(LogLevelInfo, "map subscription not authorized", map[string]any{
				"channel": channel, "user": c.user, "client": c.uid,
			}))
			return ErrorPermissionDenied
		}
		// Use stored options.
		reply.Options = state.options
	}

	// Build read options.
	limit := int(req.Limit)
	if limit <= 0 {
		limit = 100 // Default limit.
	}
	// Cap at server max if configured.
	if c.node.config.MapMaxPaginationLimit > 0 && limit > c.node.config.MapMaxPaginationLimit {
		limit = c.node.config.MapMaxPaginationLimit
	}

	opts := MapReadStateOptions{
		Cached:  true, // Use cache for subscription state delivery
		Cursor:  req.Cursor,
		Limit:   limit,
		Ordered: req.Ordered,
	}

	// If client provided position, validate epoch.
	if req.Offset > 0 || req.Epoch != "" {
		opts.Revision = &StreamPosition{
			Offset: req.Offset,
			Epoch:  req.Epoch,
		}
	}

	// Read state page.
	stateResult, err := c.node.MapStateRead(c.ctx, channel, opts)
	if err != nil {
		if errors.Is(err, ErrorUnrecoverablePosition) {
			c.cleanupMapSubscribing(channel)
			return ErrorUnrecoverablePosition
		}
		c.node.logger.log(newErrorLogEntry(err, "error reading map state", map[string]any{
			"channel": channel, "user": c.user, "client": c.uid,
		}))
		c.cleanupMapSubscribing(channel)
		return ErrorInternal
	}
	pubs := stateResult.Publications
	streamPos := stateResult.Position
	nextCursor := stateResult.Cursor

	// Get state for tags filter and epoch update.
	c.mu.RLock()
	state := c.mapSubscribing[channel]
	c.mu.RUnlock()

	// Update epoch in state on first page.
	if req.Cursor == "" && state != nil {
		c.mu.Lock()
		state.epoch = streamPos.Epoch
		c.mu.Unlock()
	}

	// Filter state entries modified after client's position (for subsequent pages).
	// This ensures entries that were updated after the first page was read don't appear
	// in later pages, which would cause duplicates when client catches up from stream.
	if opts.Revision != nil {
		filteredPubs := make([]*Publication, 0, len(pubs))
		for _, pub := range pubs {
			// Keep entries with offset <= client's saved offset.
			// These are guaranteed to not appear in stream catch-up.
			if pub.Offset <= opts.Revision.Offset {
				filteredPubs = append(filteredPubs, pub)
			}
		}
		pubs = filteredPubs
	}

	// Apply tags filter to state publications.
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

	// Check for direct STATE→LIVE transition on last page.
	// If stream hasn't advanced much, skip STREAM phase entirely.
	if c.node.config.MapStateToLiveEnabled && nextCursor == "" {
		// Get current stream position to check if we can go LIVE directly.
		currentStreamPos, err := c.node.MapStreamPosition(c.ctx, channel, state.options.HistoryMetaTTL)
		if err == nil {
			// Use limit as threshold - if stream is within one page, go LIVE.
			if streamPos.Offset+uint64(limit) >= currentStreamPos.Offset {
				// Go LIVE directly - skip STREAM phase.
				return c.handleMapStateToLive(req, reply, state, cmd, started, rw, pubs, streamPos)
			}
		}
		// If error or stream too far ahead, fall through to normal STATE response.
	}

	// Build response.
	res := &protocol.SubscribeResult{
		Type:   1, // MAP type
		Phase:  MapPhaseState,
		Cursor: nextCursor,
		Epoch:  streamPos.Epoch,
		Offset: streamPos.Offset,
	}

	// Convert state entries (use State field, not Publications).
	stateProtos := make([]*protocol.Publication, 0, len(pubs))
	for _, pub := range pubs {
		stateProtos = append(stateProtos, pubToProto(pub))
	}
	res.State = stateProtos

	return c.writeMapSubscribeReply(channel, cmd, res, started, rw)
}

// handleMapStateToLive handles direct transition from STATE to LIVE phase.
// This is called on the last state page when stream is close enough to go LIVE directly.
func (c *Client) handleMapStateToLive(
	req *protocol.SubscribeRequest,
	reply SubscribeReply,
	state *mapSubscribeState,
	cmd *protocol.Command,
	started time.Time,
	rw *replyWriter,
	statePubs []*Publication,
	statePos StreamPosition,
) error {
	channel := req.Channel
	opts := state.options
	isPresence := state.isPresence

	// Build subscription info first, validate before subscribing.
	useID := opts.AllowChannelCompaction && req.Flag&subscriptionFlagChannelCompression != 0
	sub := subInfo{client: c, deltaType: deltaTypeNone, useID: useID, isMap: true}

	// Process tags filter if provided.
	if req.Tf != nil {
		if !opts.AllowTagsFilter {
			c.cleanupMapSubscribing(channel)
			c.node.logger.log(newLogEntry(LogLevelInfo, "tags filter not allowed for map channel", map[string]any{"channel": channel, "user": c.user, "client": c.uid}))
			return ErrorBadRequest
		}
		if err := filter.Validate(req.Tf); err != nil {
			c.cleanupMapSubscribing(channel)
			c.node.logger.log(newLogEntry(LogLevelInfo, "invalid tags filter for map channel", map[string]any{"channel": channel, "user": c.user, "client": c.uid}))
			return ErrorBadRequest
		}
		sub.tagsFilter = &tagsFilter{
			filter: req.Tf,
			hash:   filter.Hash(req.Tf),
		}
	} else if state.tagsFilter != nil {
		// Use tags filter from state pagination phase.
		sub.tagsFilter = state.tagsFilter
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

	// Start coordination: buffer -> add subscription -> read stream -> merge.
	c.pubSubSync.StartBuffering(channel)

	chanID, err := c.node.addSubscription(channel, sub)
	if err != nil {
		c.pubSubSync.StopBuffering(channel)
		c.cleanupMapSubscribing(channel)
		c.node.logger.log(newErrorLogEntry(err, "error adding map subscription", map[string]any{
			"channel": channel, "user": c.user, "client": c.uid,
		}))
		var clientErr *Error
		if errors.As(err, &clientErr) && !errors.Is(clientErr, ErrorInternal) {
			return clientErr
		}
		return ErrorInternal
	}

	// Read stream from statePos to catch any updates since state was read.
	streamLimit := -1 // No limit by default.
	if c.node.config.MapRecoveryMaxPublicationLimit > 0 {
		streamLimit = c.node.config.MapRecoveryMaxPublicationLimit
	}

	var recoveredPubs []*protocol.Publication
	streamOpts := MapReadStreamOptions{
		Filter: StreamFilter{
			Since: &StreamPosition{
				Offset: statePos.Offset,
				Epoch:  statePos.Epoch,
			},
			Limit: streamLimit,
		},
		MetaTTL: opts.HistoryMetaTTL,
	}

	streamResult, err := c.node.MapStreamRead(c.ctx, channel, streamOpts)
	if err != nil {
		c.pubSubSync.StopBuffering(channel)
		_ = c.node.removeSubscription(channel, c)
		c.cleanupMapSubscribing(channel)
		if errors.Is(err, ErrorUnrecoverablePosition) {
			return ErrorUnrecoverablePosition
		}
		c.node.logger.log(newErrorLogEntry(err, "error reading stream for state-to-live phase", map[string]any{
			"channel": channel, "user": c.user, "client": c.uid,
		}))
		return ErrorInternal
	}
	pubs := streamResult.Publications
	streamPos := streamResult.Position

	// Check if we got exactly the limit - client may be too far behind.
	if streamLimit > 0 && len(pubs) >= streamLimit {
		c.pubSubSync.StopBuffering(channel)
		_ = c.node.removeSubscription(channel, c)
		c.cleanupMapSubscribing(channel)
		return ErrorUnrecoverablePosition
	}

	// Convert stream publications to protocol format.
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
		c.cleanupMapSubscribing(channel)
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
	if deltaEnabled && req.Delta == string(DeltaTypeFossil) {
		recoveredPubs = c.makeRecoveredPubsDeltaFossil(recoveredPubs)
	}

	// Convert state publications to protocol format.
	stateProtos := make([]*protocol.Publication, 0, len(statePubs))
	for _, pub := range statePubs {
		stateProtos = append(stateProtos, pubToProto(pub))
	}

	// Build response with phase=0 (LIVE), both state and stream publications.
	res := &protocol.SubscribeResult{
		Type:   1, // MAP type
		Phase:  MapPhaseLive,
		Epoch:  streamPos.Epoch,
		Offset: latestOffset,
		Delta:  deltaEnabled,
	}
	if chanID > 0 {
		res.Id = chanID
	}
	res.State = stateProtos          // Last page state entries
	res.Publications = recoveredPubs // Stream catch-up publications

	// Write response before stopping buffer.
	protoReply, err := c.getSubscribeCommandReply(res)
	if err != nil {
		c.pubSubSync.StopBuffering(channel)
		_ = c.node.removeSubscription(channel, c)
		c.cleanupMapSubscribing(channel)
		c.node.logger.log(newErrorLogEntry(err, "error encoding map subscribe reply", map[string]any{
			"channel": channel, "user": c.user, "client": c.uid,
		}))
		return ErrorInternal
	}

	c.writeEncodedCommandReply(channel, protocol.FrameTypeSubscribe, cmd, protoReply, rw)
	c.handleCommandFinished(cmd, protocol.FrameTypeSubscribe, nil, protoReply, started, "")
	c.releaseSubscribeCommandReply(protoReply)

	// Build channel context with map flag.
	channelFlags := c.buildMapChannelFlags(deltaEnabled, req.Delta, isPresence, opts, reply)

	channelContext := ChannelContext{
		flags:    channelFlags,
		expireAt: opts.ExpireAt,
		info:     opts.ChannelInfo,
		streamPosition: StreamPosition{
			Offset: latestOffset,
			Epoch:  streamPos.Epoch,
		},
		metaTTLSeconds:                 int64(opts.HistoryMetaTTL.Seconds()),
		positionCheckTime:              time.Now().Unix(),
		Source:                         opts.Source,
		mapClientPresenceChannelPrefix: opts.MapClientPresenceChannelPrefix,
		mapUserPresenceChannelPrefix:   opts.MapUserPresenceChannelPrefix,
	}

	// Move from mapSubscribing to channels.
	c.mu.Lock()
	if state.subscribingCh != nil {
		close(state.subscribingCh)
	}
	delete(c.mapSubscribing, channel)
	c.channels[channel] = channelContext
	c.mu.Unlock()

	// Stop buffering after response written.
	c.pubSubSync.StopBuffering(channel)

	// Add presence and join handling.
	c.setupMapPresenceAndJoin(channel, opts)

	return nil
}

// handleMapStreamPhase handles stateless stream pagination (history catch-up).
// Server controls when to transition to LIVE based on captured streamStart.
func (c *Client) handleMapStreamPhase(
	req *protocol.SubscribeRequest,
	reply SubscribeReply,
	cmd *protocol.Command,
	started time.Time,
	rw *replyWriter,
) error {
	channel := req.Channel

	// Check for existing subscription state or recovery mode.
	c.mu.RLock()
	state, hasState := c.mapSubscribing[channel]
	c.mu.RUnlock()

	if !hasState {
		if !req.Recover {
			// No existing state and not recovering - permission denied.
			c.node.logger.log(newLogEntry(LogLevelInfo, "map subscription not authorized for stream", map[string]any{
				"channel": channel, "user": c.user, "client": c.uid,
			}))
			return ErrorPermissionDenied
		}
		// Recovery mode: create subscription state on the fly.
		// Client is reconnecting with offset/epoch, skip state phase.
		state = &mapSubscribeState{
			options:       reply.Options,
			isPresence:    req.Type >= 2,
			subscribingCh: make(chan struct{}),
			epoch:         req.Epoch,
		}
		c.mu.Lock()
		if c.mapSubscribing == nil {
			c.mapSubscribing = make(map[string]*mapSubscribeState)
		}
		c.mapSubscribing[channel] = state
		c.mu.Unlock()
	}

	// Acquire pagination lock for this channel.
	if !c.acquireMapPaginationLock(channel) {
		return ErrorConcurrentPagination
	}
	defer c.releaseMapPaginationLock(channel)

	// Validate epoch if provided.
	if req.Epoch != "" && state.epoch != "" && req.Epoch != state.epoch {
		c.cleanupMapSubscribing(channel)
		return ErrorUnrecoverablePosition
	}

	// Capture stream top on first stream request.
	if !state.streamStartCaptured {
		streamPos, err := c.node.MapStreamPosition(c.ctx, channel, state.options.HistoryMetaTTL)
		if err != nil {
			if errors.Is(err, ErrorUnrecoverablePosition) {
				c.cleanupMapSubscribing(channel)
				return ErrorUnrecoverablePosition
			}
			c.node.logger.log(newErrorLogEntry(err, "error getting stream position", map[string]any{
				"channel": channel, "user": c.user, "client": c.uid,
			}))
			c.cleanupMapSubscribing(channel)
			return ErrorInternal
		}
		c.mu.Lock()
		state.streamStart = streamPos.Offset
		state.streamStartCaptured = true
		c.mu.Unlock()
	}

	// Build read options.
	limit := int(req.Limit)
	if limit <= 0 {
		limit = 500 // Default stream limit.
	}
	// Enforce minimum pagination limit to prevent excessive round trips.
	if c.node.config.MapMinStreamPaginationLimit > 0 && limit < c.node.config.MapMinStreamPaginationLimit {
		limit = c.node.config.MapMinStreamPaginationLimit
	}
	if c.node.config.MapMaxPaginationLimit > 0 && limit > c.node.config.MapMaxPaginationLimit {
		limit = c.node.config.MapMaxPaginationLimit
	}

	// Check if close enough to go LIVE: offset + limit >= streamStart
	// This means client can catch up to streamStart in one more read.
	if req.Offset+uint64(limit) >= state.streamStart {
		// Transition to LIVE - do the buffering/merge flow.
		return c.handleMapStreamToLive(req, reply, state, cmd, started, rw)
	}

	// Not close enough yet - return stream page with phase=1 so client continues.
	opts := MapReadStreamOptions{
		Filter: StreamFilter{
			Since: &StreamPosition{
				Offset: req.Offset,
				Epoch:  req.Epoch,
			},
			Limit: limit,
		},
		MetaTTL: state.options.HistoryMetaTTL,
	}

	// Read stream.
	streamResult, err := c.node.MapStreamRead(c.ctx, channel, opts)
	if err != nil {
		if errors.Is(err, ErrorUnrecoverablePosition) {
			c.cleanupMapSubscribing(channel)
			return ErrorUnrecoverablePosition
		}
		c.node.logger.log(newErrorLogEntry(err, "error reading map stream", map[string]any{
			"channel": channel, "user": c.user, "client": c.uid,
		}))
		c.cleanupMapSubscribing(channel)
		return ErrorInternal
	}
	pubs := streamResult.Publications
	streamPos := streamResult.Position

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
		Type:   1, // MAP type
		Phase:  MapPhaseStream,
		Epoch:  streamPos.Epoch,
		Offset: streamPos.Offset,
	}

	// Convert publications.
	protoPubs := make([]*protocol.Publication, 0, len(pubs))
	for _, pub := range pubs {
		protoPubs = append(protoPubs, pubToProto(pub))
	}
	res.Publications = protoPubs

	return c.writeMapSubscribeReply(channel, cmd, res, started, rw)
}

// handleMapStreamToLive transitions from stream pagination to live phase.
// This is called when client is close enough to catch up in one read.
func (c *Client) handleMapStreamToLive(
	req *protocol.SubscribeRequest,
	reply SubscribeReply,
	state *mapSubscribeState,
	cmd *protocol.Command,
	started time.Time,
	rw *replyWriter,
) error {
	channel := req.Channel
	opts := state.options
	isPresence := state.isPresence

	// Build subscription info first, validate before subscribing.
	useID := opts.AllowChannelCompaction && req.Flag&subscriptionFlagChannelCompression != 0
	sub := subInfo{client: c, deltaType: deltaTypeNone, useID: useID, isMap: true}

	// Process tags filter if provided.
	if req.Tf != nil {
		if !opts.AllowTagsFilter {
			c.cleanupMapSubscribing(channel)
			c.node.logger.log(newLogEntry(LogLevelInfo, "tags filter not allowed for map channel", map[string]any{"channel": channel, "user": c.user, "client": c.uid}))
			return ErrorBadRequest
		}
		if err := filter.Validate(req.Tf); err != nil {
			c.cleanupMapSubscribing(channel)
			c.node.logger.log(newLogEntry(LogLevelInfo, "invalid tags filter for map channel", map[string]any{"channel": channel, "user": c.user, "client": c.uid}))
			return ErrorBadRequest
		}
		sub.tagsFilter = &tagsFilter{
			filter: req.Tf,
			hash:   filter.Hash(req.Tf),
		}
	} else if state.tagsFilter != nil {
		// Use tags filter from state pagination phase if not provided in this request.
		sub.tagsFilter = state.tagsFilter
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

	// Start coordination: buffer -> add subscription (which subscribes to map broker) -> read remaining -> merge.
	c.pubSubSync.StartBuffering(channel)

	chanID, err := c.node.addSubscription(channel, sub)
	if err != nil {
		c.pubSubSync.StopBuffering(channel)
		c.cleanupMapSubscribing(channel)
		c.node.logger.log(newErrorLogEntry(err, "error adding map subscription", map[string]any{
			"channel": channel, "user": c.user, "client": c.uid,
		}))
		var clientErr *Error
		if errors.As(err, &clientErr) && !errors.Is(clientErr, ErrorInternal) {
			return clientErr
		}
		return ErrorInternal
	}

	// Read remaining stream publications since client's offset with limit.
	streamLimit := -1 // No limit by default for final catch-up.
	if c.node.config.MapRecoveryMaxPublicationLimit > 0 {
		streamLimit = c.node.config.MapRecoveryMaxPublicationLimit
	}

	var recoveredPubs []*protocol.Publication
	streamOpts := MapReadStreamOptions{
		Filter: StreamFilter{
			Since: &StreamPosition{
				Offset: req.Offset,
				Epoch:  req.Epoch,
			},
			Limit: streamLimit,
		},
		MetaTTL: opts.HistoryMetaTTL,
	}

	streamResult, err := c.node.MapStreamRead(c.ctx, channel, streamOpts)
	if err != nil {
		c.pubSubSync.StopBuffering(channel)
		_ = c.node.removeSubscription(channel, c)
		c.cleanupMapSubscribing(channel)
		if errors.Is(err, ErrorUnrecoverablePosition) {
			return ErrorUnrecoverablePosition
		}
		c.node.logger.log(newErrorLogEntry(err, "error reading stream for live phase", map[string]any{
			"channel": channel, "user": c.user, "client": c.uid,
		}))
		return ErrorInternal
	}
	pubs := streamResult.Publications
	streamPos := streamResult.Position

	// Check if we got exactly the limit - client may be too far behind.
	if streamLimit > 0 && len(pubs) >= streamLimit {
		c.pubSubSync.StopBuffering(channel)
		_ = c.node.removeSubscription(channel, c)
		c.cleanupMapSubscribing(channel)
		return ErrorUnrecoverablePosition
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
		c.cleanupMapSubscribing(channel)
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
	if deltaEnabled && req.Delta == string(DeltaTypeFossil) {
		recoveredPubs = c.makeRecoveredPubsDeltaFossil(recoveredPubs)
	}

	// Build response with phase=0 (LIVE).
	res := &protocol.SubscribeResult{
		Type:   1, // MAP type
		Phase:  MapPhaseLive,
		Epoch:  streamPos.Epoch,
		Offset: latestOffset,
		Delta:  deltaEnabled,
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
		c.cleanupMapSubscribing(channel)
		c.node.logger.log(newErrorLogEntry(err, "error encoding map subscribe reply", map[string]any{
			"channel": channel, "user": c.user, "client": c.uid,
		}))
		return ErrorInternal
	}

	c.writeEncodedCommandReply(channel, protocol.FrameTypeSubscribe, cmd, protoReply, rw)
	c.handleCommandFinished(cmd, protocol.FrameTypeSubscribe, nil, protoReply, started, "")
	c.releaseSubscribeCommandReply(protoReply)

	// Build channel context with map flag.
	var channelFlags uint16
	channelFlags |= flagSubscribed
	channelFlags |= flagPositioning
	channelFlags |= flagMap
	if deltaEnabled && req.Delta == string(DeltaTypeFossil) {
		channelFlags |= flagDeltaAllowed
	}
	if isPresence {
		channelFlags |= flagMapPresence
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
	if opts.MapClientPresenceChannelPrefix != "" {
		channelFlags |= flagMapClientPresence
	}
	if opts.MapUserPresenceChannelPrefix != "" {
		channelFlags |= flagMapUserPresence
	}
	if opts.MapRemoveOnUnsubscribe {
		channelFlags |= flagCleanupOnUnsubscribe
	}

	channelContext := ChannelContext{
		flags:    channelFlags,
		expireAt: opts.ExpireAt,
		info:     opts.ChannelInfo,
		streamPosition: StreamPosition{
			Offset: latestOffset,
			Epoch:  streamPos.Epoch,
		},
		metaTTLSeconds:                 int64(opts.HistoryMetaTTL.Seconds()),
		positionCheckTime:              time.Now().Unix(),
		Source:                         opts.Source,
		mapClientPresenceChannelPrefix: opts.MapClientPresenceChannelPrefix,
		mapUserPresenceChannelPrefix:   opts.MapUserPresenceChannelPrefix,
	}

	// Move from mapSubscribing to channels.
	c.mu.Lock()
	if state.subscribingCh != nil {
		close(state.subscribingCh)
	}
	delete(c.mapSubscribing, channel)
	c.channels[channel] = channelContext
	c.mu.Unlock()

	// Stop buffering after response written.
	c.pubSubSync.StopBuffering(channel)

	// Add presence if enabled.
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

	// Add map client presence if prefix is configured.
	if opts.MapClientPresenceChannelPrefix != "" {
		info := &ClientInfo{
			ClientID: c.uid,
			UserID:   c.user,
			ConnInfo: c.info,
			ChanInfo: opts.ChannelInfo,
		}
		if err := c.addMapClientPresence(channel, opts.MapClientPresenceChannelPrefix, info); err != nil {
			c.node.logger.log(newErrorLogEntry(err, "error adding map client presence", map[string]any{
				"channel": channel, "user": c.user, "client": c.uid,
			}))
		}
	}

	// Add map user presence if prefix is configured.
	if opts.MapUserPresenceChannelPrefix != "" {
		if err := c.addMapUserPresence(channel, opts.MapUserPresenceChannelPrefix); err != nil {
			c.node.logger.log(newErrorLogEntry(err, "error adding map user presence", map[string]any{
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

// handleMapLivePhase handles joining pub/sub with coordination.
// Supports two modes:
//   - Immediate join (Scenario B): Fresh subscription with recover=false, returns state + stream.
//   - Recovery/paginated join: After pagination or reconnect, returns only stream catch-up.
func (c *Client) handleMapLivePhase(
	req *protocol.SubscribeRequest,
	reply SubscribeReply,
	cmd *protocol.Command,
	started time.Time,
	rw *replyWriter,
) error {
	channel := req.Channel

	if c.node.getMapBroker(channel) == nil {
		return ErrorNotAvailable
	}

	// Get stored state if exists (for two-phase), or use reply options (for direct live).
	var opts SubscribeOptions
	var isPresence bool
	var tagsFilterFromState *tagsFilter
	c.mu.RLock()
	state, hasState := c.mapSubscribing[channel]
	c.mu.RUnlock()

	if hasState {
		opts = state.options
		isPresence = state.isPresence
		tagsFilterFromState = state.tagsFilter
		// Validate epoch if client provided one.
		if req.Epoch != "" && state.epoch != "" && req.Epoch != state.epoch {
			c.cleanupMapSubscribing(channel)
			return ErrorUnrecoverablePosition
		}
	} else {
		// Direct live subscription (no prior pagination).
		opts = reply.Options
		isPresence = req.Type >= 2
	}

	// Immediate join: Fresh subscription (not recovering) without prior pagination.
	// recover=false means fresh subscription → server returns state + stream.
	// recover=true means reconnection → server returns only stream catch-up.
	if !hasState && !req.Recover {
		return c.handleMapImmediateJoin(req, reply, opts, isPresence, cmd, started, rw)
	}

	// Recovery or paginated join - only stream catch-up needed.
	return c.handleMapRecoveryJoin(req, reply, opts, isPresence, tagsFilterFromState, cmd, started, rw)
}

// handleMapImmediateJoin handles immediate join mode (Scenario B).
// Returns full state + stream in one response.
func (c *Client) handleMapImmediateJoin(
	req *protocol.SubscribeRequest,
	reply SubscribeReply,
	opts SubscribeOptions,
	isPresence bool,
	cmd *protocol.Command,
	started time.Time,
	rw *replyWriter,
) error {
	channel := req.Channel

	// Build subscription info first, validate before subscribing.
	useID := opts.AllowChannelCompaction && req.Flag&subscriptionFlagChannelCompression != 0
	sub := subInfo{client: c, deltaType: deltaTypeNone, useID: useID, isMap: true}

	// Process tags filter if provided.
	if req.Tf != nil {
		if !opts.AllowTagsFilter {
			c.node.logger.log(newLogEntry(LogLevelInfo, "tags filter not allowed for map channel", map[string]any{"channel": channel, "user": c.user, "client": c.uid}))
			return ErrorBadRequest
		}
		if err := filter.Validate(req.Tf); err != nil {
			c.node.logger.log(newLogEntry(LogLevelInfo, "invalid tags filter for map channel", map[string]any{"channel": channel, "user": c.user, "client": c.uid}))
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

	// Start coordination: buffer -> add subscription -> read state -> read stream -> merge.
	c.pubSubSync.StartBuffering(channel)

	chanID, err := c.node.addSubscription(channel, sub)
	if err != nil {
		c.pubSubSync.StopBuffering(channel)
		c.node.logger.log(newErrorLogEntry(err, "error adding map subscription", map[string]any{
			"channel": channel, "user": c.user, "client": c.uid,
		}))
		var clientErr *Error
		if errors.As(err, &clientErr) && !errors.Is(clientErr, ErrorInternal) {
			return clientErr
		}
		return ErrorInternal
	}

	// Read full state (no pagination, enforce size limit).
	stateLimit := 0 // No limit by default.
	if c.node.config.MapMaxImmediateJoinStateSize > 0 {
		stateLimit = c.node.config.MapMaxImmediateJoinStateSize
	}

	stateResult, err := c.node.MapStateRead(c.ctx, channel, MapReadStateOptions{
		Cached: true, // Use cache for subscription state delivery
		Limit:  stateLimit,
	})
	if err != nil {
		c.pubSubSync.StopBuffering(channel)
		_ = c.node.removeSubscription(channel, c)
		if errors.Is(err, ErrorUnrecoverablePosition) {
			return ErrorUnrecoverablePosition
		}
		c.node.logger.log(newErrorLogEntry(err, "error reading state for immediate join", map[string]any{
			"channel": channel, "user": c.user, "client": c.uid,
		}))
		return ErrorInternal
	}

	// Check if state is too large for immediate join.
	if stateResult.Cursor != "" {
		c.pubSubSync.StopBuffering(channel)
		_ = c.node.removeSubscription(channel, c)
		return ErrorStateTooLarge
	}

	statePubs := stateResult.Publications
	statePos := stateResult.Position

	// Apply tags filter to state publications.
	if sub.tagsFilter != nil {
		filteredPubs := make([]*Publication, 0, len(statePubs))
		for _, pub := range statePubs {
			match, _ := filter.Match(sub.tagsFilter.filter, pub.Tags)
			if match {
				filteredPubs = append(filteredPubs, pub)
			}
		}
		statePubs = filteredPubs
	}

	// Read stream from statePos to catch any updates that happened during state read.
	streamLimit := -1 // No limit by default.
	if c.node.config.MapRecoveryMaxPublicationLimit > 0 {
		streamLimit = c.node.config.MapRecoveryMaxPublicationLimit
	}

	var recoveredPubs []*protocol.Publication
	streamOpts := MapReadStreamOptions{
		Filter: StreamFilter{
			Since: &StreamPosition{
				Offset: statePos.Offset,
				Epoch:  statePos.Epoch,
			},
			Limit: streamLimit,
		},
		MetaTTL: opts.HistoryMetaTTL,
	}

	streamResult, err := c.node.MapStreamRead(c.ctx, channel, streamOpts)
	if err != nil {
		c.pubSubSync.StopBuffering(channel)
		_ = c.node.removeSubscription(channel, c)
		if errors.Is(err, ErrorUnrecoverablePosition) {
			return ErrorUnrecoverablePosition
		}
		c.node.logger.log(newErrorLogEntry(err, "error reading stream for immediate join", map[string]any{
			"channel": channel, "user": c.user, "client": c.uid,
		}))
		return ErrorInternal
	}
	pubs := streamResult.Publications
	streamPos := streamResult.Position

	// Check if we got exactly the limit - client may be too far behind.
	if streamLimit > 0 && len(pubs) >= streamLimit {
		c.pubSubSync.StopBuffering(channel)
		_ = c.node.removeSubscription(channel, c)
		return ErrorUnrecoverablePosition
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
	if deltaEnabled && req.Delta == string(DeltaTypeFossil) {
		recoveredPubs = c.makeRecoveredPubsDeltaFossil(recoveredPubs)
	}

	// Convert state publications to protocol format.
	stateProtos := make([]*protocol.Publication, 0, len(statePubs))
	for _, pub := range statePubs {
		stateProtos = append(stateProtos, pubToProto(pub))
	}

	// Build response with both state and stream.
	res := &protocol.SubscribeResult{
		Type:         1, // MAP type
		Phase:        MapPhaseLive,
		Epoch:        streamPos.Epoch,
		Offset:       latestOffset,
		Delta:        deltaEnabled,
		State:        stateProtos,   // Full state for immediate join
		Publications: recoveredPubs, // Stream publications
	}
	if chanID > 0 {
		res.Id = chanID
	}

	// Write response before stopping buffer.
	protoReply, err := c.getSubscribeCommandReply(res)
	if err != nil {
		c.pubSubSync.StopBuffering(channel)
		_ = c.node.removeSubscription(channel, c)
		c.node.logger.log(newErrorLogEntry(err, "error encoding map subscribe reply", map[string]any{
			"channel": channel, "user": c.user, "client": c.uid,
		}))
		return ErrorInternal
	}

	c.writeEncodedCommandReply(channel, protocol.FrameTypeSubscribe, cmd, protoReply, rw)
	c.handleCommandFinished(cmd, protocol.FrameTypeSubscribe, nil, protoReply, started, "")
	c.releaseSubscribeCommandReply(protoReply)

	// Build channel context with map flag.
	channelFlags := c.buildMapChannelFlags(deltaEnabled, req.Delta, isPresence, opts, reply)

	channelContext := ChannelContext{
		flags:    channelFlags,
		expireAt: opts.ExpireAt,
		info:     opts.ChannelInfo,
		streamPosition: StreamPosition{
			Offset: latestOffset,
			Epoch:  streamPos.Epoch,
		},
		metaTTLSeconds:                 int64(opts.HistoryMetaTTL.Seconds()),
		positionCheckTime:              time.Now().Unix(),
		Source:                         opts.Source,
		mapClientPresenceChannelPrefix: opts.MapClientPresenceChannelPrefix,
		mapUserPresenceChannelPrefix:   opts.MapUserPresenceChannelPrefix,
	}

	// Add to channels (no mapSubscribing state exists for immediate join).
	c.mu.Lock()
	c.channels[channel] = channelContext
	c.mu.Unlock()

	// Stop buffering after response written.
	c.pubSubSync.StopBuffering(channel)

	// Add presence and emit join events.
	c.setupMapPresenceAndJoin(channel, opts)

	return nil
}

// handleMapRecoveryJoin handles recovery or paginated join (stream catch-up only).
func (c *Client) handleMapRecoveryJoin(
	req *protocol.SubscribeRequest,
	reply SubscribeReply,
	opts SubscribeOptions,
	isPresence bool,
	tagsFilterFromState *tagsFilter,
	cmd *protocol.Command,
	started time.Time,
	rw *replyWriter,
) error {
	channel := req.Channel

	// Build subscription info first, validate before subscribing.
	useID := opts.AllowChannelCompaction && req.Flag&subscriptionFlagChannelCompression != 0
	sub := subInfo{client: c, deltaType: deltaTypeNone, useID: useID, isMap: true}

	// Process tags filter if provided.
	if req.Tf != nil {
		if !opts.AllowTagsFilter {
			c.cleanupMapSubscribing(channel)
			c.node.logger.log(newLogEntry(LogLevelInfo, "tags filter not allowed for map channel", map[string]any{"channel": channel, "user": c.user, "client": c.uid}))
			return ErrorBadRequest
		}
		if err := filter.Validate(req.Tf); err != nil {
			c.cleanupMapSubscribing(channel)
			c.node.logger.log(newLogEntry(LogLevelInfo, "invalid tags filter for map channel", map[string]any{"channel": channel, "user": c.user, "client": c.uid}))
			return ErrorBadRequest
		}
		sub.tagsFilter = &tagsFilter{
			filter: req.Tf,
			hash:   filter.Hash(req.Tf),
		}
	} else if tagsFilterFromState != nil {
		// Use tags filter from state pagination phase if not provided in this request.
		sub.tagsFilter = tagsFilterFromState
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

	// Start coordination: buffer -> add subscription (which subscribes to map broker) -> read remaining -> merge.
	// IMPORTANT: addSubscription adds client to hub AND subscribes to map broker.
	// This ensures the client is in hub before pub/sub messages arrive.
	c.pubSubSync.StartBuffering(channel)

	chanID, err := c.node.addSubscription(channel, sub)
	if err != nil {
		c.pubSubSync.StopBuffering(channel)
		c.cleanupMapSubscribing(channel)
		c.node.logger.log(newErrorLogEntry(err, "error adding map subscription", map[string]any{
			"channel": channel, "user": c.user, "client": c.uid,
		}))
		var clientErr *Error
		if errors.As(err, &clientErr) && !errors.Is(clientErr, ErrorInternal) {
			return clientErr
		}
		return ErrorInternal
	}

	// Read remaining stream publications since client's offset.
	streamLimit := -1 // No limit by default.
	if c.node.config.MapRecoveryMaxPublicationLimit > 0 {
		streamLimit = c.node.config.MapRecoveryMaxPublicationLimit
	}

	var recoveredPubs []*protocol.Publication
	streamOpts := MapReadStreamOptions{
		Filter: StreamFilter{
			Since: &StreamPosition{
				Offset: req.Offset,
				Epoch:  req.Epoch,
			},
			Limit: streamLimit,
		},
		MetaTTL: opts.HistoryMetaTTL,
	}

	streamResult, err := c.node.MapStreamRead(c.ctx, channel, streamOpts)
	if err != nil {
		c.pubSubSync.StopBuffering(channel)
		_ = c.node.removeSubscription(channel, c) // This handles map broker unsubscribe.
		c.cleanupMapSubscribing(channel)
		if errors.Is(err, ErrorUnrecoverablePosition) {
			return ErrorUnrecoverablePosition
		}
		c.node.logger.log(newErrorLogEntry(err, "error reading stream for live phase", map[string]any{
			"channel": channel, "user": c.user, "client": c.uid,
		}))
		return ErrorInternal
	}
	pubs := streamResult.Publications
	streamPos := streamResult.Position

	// Check if we got exactly the limit - client may be too far behind.
	if streamLimit > 0 && len(pubs) >= streamLimit {
		c.pubSubSync.StopBuffering(channel)
		_ = c.node.removeSubscription(channel, c)
		c.cleanupMapSubscribing(channel)
		return ErrorUnrecoverablePosition
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
		_ = c.node.removeSubscription(channel, c) // This handles map broker unsubscribe.
		c.cleanupMapSubscribing(channel)
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
	// Unlike normal subs, we don't check for "Recovered" flag here because map subs
	// always error out on failed recovery (never reach this point with failed recovery).
	if deltaEnabled && req.Delta == string(DeltaTypeFossil) {
		recoveredPubs = c.makeRecoveredPubsDeltaFossil(recoveredPubs)
	}

	// Build response.
	res := &protocol.SubscribeResult{
		Type:   1, // MAP type
		Phase:  MapPhaseLive,
		Epoch:  streamPos.Epoch,
		Offset: latestOffset,
		Delta:  deltaEnabled,
	}
	if chanID > 0 {
		res.Id = chanID
	}
	res.Publications = recoveredPubs

	// Write response before stopping buffer.
	protoReply, err := c.getSubscribeCommandReply(res)
	if err != nil {
		c.pubSubSync.StopBuffering(channel)
		_ = c.node.removeSubscription(channel, c) // This handles map broker unsubscribe.
		c.cleanupMapSubscribing(channel)
		c.node.logger.log(newErrorLogEntry(err, "error encoding map subscribe reply", map[string]any{
			"channel": channel, "user": c.user, "client": c.uid,
		}))
		return ErrorInternal
	}

	c.writeEncodedCommandReply(channel, protocol.FrameTypeSubscribe, cmd, protoReply, rw)
	c.handleCommandFinished(cmd, protocol.FrameTypeSubscribe, nil, protoReply, started, "")
	c.releaseSubscribeCommandReply(protoReply)

	// Build channel context with map flag.
	channelFlags := c.buildMapChannelFlags(deltaEnabled, req.Delta, isPresence, opts, reply)

	channelContext := ChannelContext{
		flags:    channelFlags,
		expireAt: opts.ExpireAt,
		info:     opts.ChannelInfo,
		streamPosition: StreamPosition{
			Offset: latestOffset,
			Epoch:  streamPos.Epoch,
		},
		metaTTLSeconds:                 int64(opts.HistoryMetaTTL.Seconds()),
		positionCheckTime:              time.Now().Unix(),
		Source:                         opts.Source,
		mapClientPresenceChannelPrefix: opts.MapClientPresenceChannelPrefix,
		mapUserPresenceChannelPrefix:   opts.MapUserPresenceChannelPrefix,
	}

	// Move from mapSubscribing to channels.
	c.mu.Lock()
	if state, ok := c.mapSubscribing[channel]; ok && state.subscribingCh != nil {
		close(state.subscribingCh)
	}
	delete(c.mapSubscribing, channel)
	c.channels[channel] = channelContext
	c.mu.Unlock()

	// Stop buffering after response written.
	c.pubSubSync.StopBuffering(channel)

	// Add presence and emit join events.
	c.setupMapPresenceAndJoin(channel, opts)

	return nil
}

// buildMapChannelFlags builds channel flags for map subscriptions.
func (c *Client) buildMapChannelFlags(deltaEnabled bool, delta string, isPresence bool, opts SubscribeOptions, reply SubscribeReply) uint16 {
	var channelFlags uint16
	channelFlags |= flagSubscribed
	channelFlags |= flagPositioning // Map subscriptions are always positioned.
	channelFlags |= flagMap         // Mark as map subscription.
	if deltaEnabled && delta == string(DeltaTypeFossil) {
		channelFlags |= flagDeltaAllowed
	}
	if isPresence {
		channelFlags |= flagMapPresence
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
	if opts.MapClientPresenceChannelPrefix != "" {
		channelFlags |= flagMapClientPresence
	}
	if opts.MapUserPresenceChannelPrefix != "" {
		channelFlags |= flagMapUserPresence
	}
	if opts.MapRemoveOnUnsubscribe {
		channelFlags |= flagCleanupOnUnsubscribe
	}
	return channelFlags
}

// setupMapPresenceAndJoin handles presence and join event setup for map subscriptions.
func (c *Client) setupMapPresenceAndJoin(channel string, opts SubscribeOptions) {
	// Add presence if enabled (uses MapBroker for map channels).
	if opts.EmitPresence {
		info := &ClientInfo{
			ClientID: c.uid,
			UserID:   c.user,
			ConnInfo: c.info,
			ChanInfo: opts.ChannelInfo,
		}
		if err := c.node.addPresence(channel, c.uid, info); err != nil {
			c.node.logger.log(newErrorLogEntry(err, "error adding client presence", map[string]any{
				"channel": channel, "user": c.user, "client": c.uid,
			}))
		}
	}

	// Add map client presence if prefix is configured.
	if opts.MapClientPresenceChannelPrefix != "" {
		info := &ClientInfo{
			ClientID: c.uid,
			UserID:   c.user,
			ConnInfo: c.info,
			ChanInfo: opts.ChannelInfo,
		}
		if err := c.addMapClientPresence(channel, opts.MapClientPresenceChannelPrefix, info); err != nil {
			c.node.logger.log(newErrorLogEntry(err, "error adding map client presence", map[string]any{
				"channel": channel, "user": c.user, "client": c.uid,
			}))
		}
	}

	// Add map user presence if prefix is configured.
	if opts.MapUserPresenceChannelPrefix != "" {
		if err := c.addMapUserPresence(channel, opts.MapUserPresenceChannelPrefix); err != nil {
			c.node.logger.log(newErrorLogEntry(err, "error adding map user presence", map[string]any{
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
}

// writeMapSubscribeReply writes a map subscribe result to the client.
func (c *Client) writeMapSubscribeReply(
	channel string,
	cmd *protocol.Command,
	res *protocol.SubscribeResult,
	started time.Time,
	rw *replyWriter,
) error {
	protoReply, err := c.getSubscribeCommandReply(res)
	if err != nil {
		c.node.logger.log(newErrorLogEntry(err, "error encoding map subscribe reply", map[string]any{
			"channel": channel, "user": c.user, "client": c.uid,
		}))
		return ErrorInternal
	}
	c.writeEncodedCommandReply(channel, protocol.FrameTypeSubscribe, cmd, protoReply, rw)
	c.handleCommandFinished(cmd, protocol.FrameTypeSubscribe, nil, protoReply, started, "")
	c.releaseSubscribeCommandReply(protoReply)
	return nil
}

// acquireMapPaginationLock tries to acquire a pagination lock for the channel.
// Returns true if lock acquired, false if another pagination is in progress.
func (c *Client) acquireMapPaginationLock(channel string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.mapPaginationLocks == nil {
		c.mapPaginationLocks = make(map[string]struct{})
	}
	if _, locked := c.mapPaginationLocks[channel]; locked {
		return false
	}
	c.mapPaginationLocks[channel] = struct{}{}
	return true
}

// releaseMapPaginationLock releases the pagination lock for the channel.
func (c *Client) releaseMapPaginationLock(channel string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.mapPaginationLocks, channel)
}

// cleanupMapSubscribing removes map subscribing state for a channel.
func (c *Client) cleanupMapSubscribing(channel string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if state, ok := c.mapSubscribing[channel]; ok {
		if state.subscribingCh != nil {
			close(state.subscribingCh)
		}
		delete(c.mapSubscribing, channel)
	}
}

// cleanupMapSubscribingAll removes all in-progress map subscriptions on disconnect.
func (c *Client) cleanupMapSubscribingAll() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for channel, state := range c.mapSubscribing {
		if state.subscribingCh != nil {
			close(state.subscribingCh)
		}
		delete(c.mapSubscribing, channel)
	}
}

// mapPresenceTTL returns TTL for map presence entries.
// Uses 3x the presence update interval to allow for some delay while still expiring if updates stop.
func (c *Client) mapPresenceTTL() time.Duration {
	ttl := 3 * c.node.config.ClientPresenceUpdateInterval
	if ttl < 30*time.Second {
		ttl = 30 * time.Second // Minimum 30 seconds.
	}
	return ttl
}

// addMapClientPresence adds client presence to {prefix}{channel}.
// Key is clientId, stores full ClientInfo.
func (c *Client) addMapClientPresence(channel string, prefix string, info *ClientInfo) error {
	presenceChannel := prefix + channel

	// Use KeyModeIfNew with RefreshTTLOnSuppress to:
	// - Publish JOIN event only if this is a new presence entry
	// - Refresh TTL without publishing if entry already exists (quick reconnect)
	// Stream options (StreamSize/TTL/MetaTTL) use defaults from GetMapChannelOptions.
	_, err := c.node.MapPublish(c.ctx, presenceChannel, c.uid, MapPublishOptions{
		ClientInfo:           info,
		KeyTTL:               c.mapPresenceTTL(),
		KeyMode:              KeyModeIfNew,
		RefreshTTLOnSuppress: true,
	})
	return err
}

// addMapUserPresence adds user presence to {prefix}{channel}.
// Key is userId, no ClientInfo stored (just the key for uniqueness).
func (c *Client) addMapUserPresence(channel string, prefix string) error {
	presenceChannel := prefix + channel

	// Use KeyModeIfNew with RefreshTTLOnSuppress to:
	// - Publish JOIN event only if this is a new user
	// - Refresh TTL without publishing if user already exists
	// Stream options (StreamSize/TTL/MetaTTL) use defaults from GetMapChannelOptions.
	_, err := c.node.MapPublish(c.ctx, presenceChannel, c.user, MapPublishOptions{
		KeyTTL:               c.mapPresenceTTL(),
		KeyMode:              KeyModeIfNew,
		RefreshTTLOnSuppress: true,
	})
	return err
}

// updateMapPresence updates presence for a map channel using MapBroker.
// This is called periodically by updateChannelPresence to refresh the TTL.
// Handles presence channels based on configured prefixes.
func (c *Client) updateMapPresence(channel string, info *ClientInfo, ctx ChannelContext) error {
	// Use KeyModeIfNew with RefreshTTLOnSuppress for TTL refresh:
	// - Since key already exists, publish is suppressed (no offset increment)
	// - TTL is refreshed without generating stream entries
	// Stream options (StreamSize/TTL/MetaTTL) use defaults from GetMapChannelOptions.

	// Update client presence if prefix is configured.
	if ctx.mapClientPresenceChannelPrefix != "" {
		presenceChannel := ctx.mapClientPresenceChannelPrefix + channel
		_, err := c.node.MapPublish(c.ctx, presenceChannel, c.uid, MapPublishOptions{
			ClientInfo:           info,
			KeyTTL:               c.mapPresenceTTL(),
			KeyMode:              KeyModeIfNew,
			RefreshTTLOnSuppress: true,
		})
		if err != nil {
			return err
		}
	}

	// Update user presence if prefix is configured.
	if ctx.mapUserPresenceChannelPrefix != "" {
		presenceChannel := ctx.mapUserPresenceChannelPrefix + channel
		_, err := c.node.MapPublish(c.ctx, presenceChannel, c.user, MapPublishOptions{
			KeyTTL:               c.mapPresenceTTL(),
			KeyMode:              KeyModeIfNew,
			RefreshTTLOnSuppress: true,
		})
		if err != nil {
			return err
		}
	}

	return nil
}

// removeMapPresence removes presence for a map channel using MapBroker.
// Called on explicit unsubscribe or disconnect. Only removes client presence,
// user presence entries expire via TTL (acts as debounce for quick reconnects).
func (c *Client) removeMapPresence(channel string, ctx ChannelContext) error {
	// Remove from :presence if EmitPresence is enabled.
	if channelHasFlag(ctx.flags, flagEmitPresence) {
		if err := c.node.removePresence(channel, c.uid, c.user); err != nil {
			c.node.logger.log(newErrorLogEntry(err, "error removing channel presence", map[string]any{"channel": channel, "user": c.user, "client": c.uid, "error": err.Error()}))
		}
	}

	// Remove client presence if prefix is configured.
	// Stream options (StreamSize/TTL/MetaTTL) use defaults from GetMapChannelOptions.
	if ctx.mapClientPresenceChannelPrefix != "" {
		presenceChannel := ctx.mapClientPresenceChannelPrefix + channel
		_, err := c.node.MapRemove(context.Background(), presenceChannel, c.uid, MapRemoveOptions{})
		if err != nil {
			c.node.logger.log(newErrorLogEntry(err, "error removing map clients presence", map[string]any{"channel": channel, "user": c.user, "client": c.uid, "error": err.Error()}))
		}
	}

	// User presence entries are NOT removed on disconnect - they only expire via TTL.
	// This provides debounce/grace period for quick reconnects.

	// Remove key=clientId from channel if MapRemoveOnUnsubscribe is enabled.
	// This is for ephemeral state like cursors where each client publishes
	// to a key that equals their client ID.
	// Stream options (StreamSize/TTL/MetaTTL) use defaults from GetMapChannelOptions.
	if channelHasFlag(ctx.flags, flagCleanupOnUnsubscribe) {
		_, err := c.node.MapRemove(context.Background(), channel, c.uid, MapRemoveOptions{})
		if err != nil {
			c.node.logger.log(newErrorLogEntry(err, "error cleaning up map state on unsubscribe", map[string]any{"channel": channel, "user": c.user, "client": c.uid, "error": err.Error()}))
		}
	}

	return nil
}

// handleMapPresenceSubscribe handles subscriptions to map presence channels.
// This is called for subscription types 2 (clients) and 3 (users).
// These subscriptions are handled differently:
// - Authorization goes through OnPresenceSubscribe instead of OnSubscribe
// - The channel name should be the full presence channel (e.g., "$clients:games")
// - Server uses the subscription type to determine handling
func (c *Client) handleMapPresenceSubscribe(
	req *protocol.SubscribeRequest,
	cmd *protocol.Command,
	started time.Time,
	rw *replyWriter,
) error {
	channel := req.Channel

	// Check if presence subscribe handler is set.
	if c.eventHub.presenceSubscribeHandler == nil {
		return ErrorNotAvailable
	}

	// Validate request.
	replyError, disconnect := c.validateSubscribeRequest(req)
	if disconnect != nil || replyError != nil {
		if disconnect != nil {
			return *disconnect
		}
		return replyError
	}

	// Build presence subscribe event.
	event := PresenceSubscribeEvent{
		Channel: channel,
		Data:    req.Data,
	}

	cb := func(reply PresenceSubscribeReply, err error) {
		if err != nil {
			c.onSubscribeError(channel)
			c.writeDisconnectOrErrorFlush(channel, protocol.FrameTypeSubscribe, cmd, err, started, rw)
			return
		}

		// Build subscribe options for the presence channel (treated as map subscription).
		opts := SubscribeOptions{
			EnableMap: true,
		}

		subscribeReply := SubscribeReply{
			Options: opts,
		}

		// Route to map subscribe handler - presence subscriptions use the same
		// map subscription flow but with presence flag set.
		if handleErr := c.handleMapSubscribe(req, subscribeReply, cmd, started, rw); handleErr != nil {
			c.onSubscribeError(channel)
			c.writeDisconnectOrErrorFlush(channel, protocol.FrameTypeSubscribe, cmd, handleErr, started, rw)
		}
	}

	c.eventHub.presenceSubscribeHandler(event, cb)
	return nil
}
