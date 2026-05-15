package centrifuge

import (
	"context"
	"errors"
	"slices"
	"time"

	"github.com/centrifugal/centrifuge/internal/convert"
	"github.com/centrifugal/centrifuge/internal/filter"
	"github.com/centrifugal/centrifuge/internal/recovery"

	"github.com/centrifugal/protocol"
	"github.com/segmentio/encoding/json"
)

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
// EnablePositioning and EnableRecovery are auto-set from the channel's Mode
// (configured in MapChannelOptions via GetMapChannelOptions resolver):
//
//   - MapModeEphemeral: Streamless mode. No stream history is maintained.
//     State is always available, but recovery on reconnect requires a full state re-sync.
//     CAS (ExpectedPosition) and Version-based dedup are not available.
//     EnablePositioning and EnableRecovery are both set to false.
//
//   - MapModeRecoverable / MapModePersistent: Stream mode. Publications are tracked with
//     offsets, stream history is maintained, and clients can recover missed publications
//     on reconnect. CAS and Version features are available.
//     EnablePositioning and EnableRecovery are both set to true.
//
// When Type is SubscriptionTypeMap in SubscribeOptions, the subscription gets:
//   - State delivery (always)
//   - Stream position tracking (if Mode.HasStream())
//   - Stream-based recovery (if Mode.HasStream())
//   - Delta compression support for stream catch-up (if negotiated)
//   - Tags filtering for stream and live publications (if allowed)
//
// Map Presence Subscriptions
//
// Map presence subscriptions allow clients to watch who is online in a channel.
// They are a special type of map subscription that tracks client or user presence.
//
// Presence is configured using full channel names:
//
//   - MapClientPresenceChannel (e.g., "presence-clients:game1") - When set, client presence
//     is published to this channel. Each entry is keyed by client ID and contains
//     full ClientInfo. Use for tracking individual connections.
//
//   - MapUserPresenceChannel (e.g., "presence-users:game1") - When set, user presence is
//     published to this channel. Each entry is keyed by user ID with minimal data.
//     Provides natural deduplication when users have multiple connections.
//
// Authorization flow:
//
// Presence subscriptions go through OnSubscribe handler with SubscribeEvent.Type set
// to SubscriptionTypeMap (same as map data subscriptions). The handler can use the
// channel name to distinguish presence channels from data channels.
//
// Client subscribes to presence channel -> OnSubscribe called with Type=SubscriptionTypeMap
// -> Handler returns SubscribeReply{Options: SubscribeOptions{Type: SubscriptionTypeMap}}
// -> Subscription proceeds
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

// subscribeResultTypeMap is the Type value for map subscriptions in protocol.SubscribeResult.
const subscribeResultTypeMap = 1

const (
	// defaultMapPageSize is the default page size when client does not specify one.
	defaultMapPageSize = 100
	// defaultMapMinPageSize is the default minimum page size for map pagination.
	defaultMapMinPageSize = 100
	// defaultMapMaxPageSize is the default maximum page size for map pagination.
	defaultMapMaxPageSize = 1000
)

// validateAndCreateTagsFilter validates the tags filter from the request and creates a tagsFilter.
// Returns (nil, nil) if req.Tf is nil. Returns (nil, error) if validation fails.
func (c *Client) validateAndCreateTagsFilter(req *protocol.SubscribeRequest, allowTagsFilter bool, channel string) (*tagsFilter, error) {
	if req.Tf == nil {
		return nil, nil
	}
	if !allowTagsFilter {
		c.node.logger.log(newLogEntry(LogLevelInfo, "tags filter not allowed for map channel", map[string]any{
			"channel": channel, "user": c.user, "client": c.uid,
		}))
		return nil, ErrorBadRequest
	}
	if err := filter.Validate(req.Tf); err != nil {
		c.node.logger.log(newLogEntry(LogLevelInfo, "invalid tags filter for map channel", map[string]any{
			"channel": channel, "user": c.user, "client": c.uid,
		}))
		return nil, ErrorBadRequest
	}
	return &tagsFilter{
		filter: req.Tf,
		hash:   filter.Hash(req.Tf),
	}, nil
}

// escapeStateForDelta JSON-escapes Data in state publications for delta-enabled JSON
// transport. This ensures the client receives data as JSON strings (matching the format
// used for real-time and recovered publications), so it can store exact bytes for delta.
func escapeStateForDelta(pubs []*protocol.Publication, deltaEnabled bool, isJSON bool) []*protocol.Publication {
	if !deltaEnabled || !isJSON {
		return pubs
	}
	for i, pub := range pubs {
		if len(pub.Data) > 0 {
			pubs[i] = copyMapPubWithData(pub, json.Escape(convert.BytesToString(pub.Data)), false)
		}
	}
	return pubs
}

// mapSubscribeState tracks state for map subscriptions that are still loading.
type mapSubscribeState struct {
	options             SubscribeOptions // From OnSubscribe callback
	epoch               string           // Epoch from first response (for validation)
	offset              uint64           // Offset from first state page (frozen for consistency)
	startedAt           int64            // UnixNano when catch-up started (for timeout)
	streamStart         uint64           // Stream top captured on first stream request
	offsetCaptured      bool             // True after offset is captured (since 0 is valid offset)
	streamStartCaptured bool             // True after streamStart is captured (since 0 is valid offset)
	isPresence          bool             // True if this is a presence subscription
	subscribingCh       chan struct{}    // Closed when subscription completes (for race handling)
	tagsFilter          *tagsFilter      // Client tags filter for state/stream publications
	serverTagsFilter    *tagsFilter      // Server tags filter for state/stream publications
}

// handleMapSubscribeCommand handles the full map subscribe command flow:
// validates, checks for continuation, calls OnSubscribe handler for initial requests.
func (c *Client) handleMapSubscribeCommand(
	req *protocol.SubscribeRequest,
	cmd *protocol.Command,
	started time.Time,
	rw *replyWriter,
) error {
	if c.eventHub.subscribeHandler == nil {
		return ErrorNotAvailable
	}

	replyError, disconnect := c.validateSubscribeRequest(req)
	if disconnect != nil || replyError != nil {
		if disconnect != nil {
			return *disconnect
		}
		return replyError
	}

	// Sweep expired catch-ups on other channels. Handles abandoned catch-ups where
	// the client stopped sending requests but stayed connected. The current channel
	// is skipped — its expiry is checked below with a proper DisconnectStale.
	c.sweepExpiredMapSubscribing(req.Channel)

	// For map subscription continuation requests (pagination or non-state phase with existing state),
	// bypass the OnSubscribe callback - we already authorized on the first request.
	c.mu.RLock()
	state, hasState := c.mapSubscribing[req.Channel]
	c.mu.RUnlock()

	if req.Cursor != "" {
		if !hasState {
			return ErrorPermissionDenied
		}
		catchUpChOpts, _ := c.node.resolveMapChannelOptions(req.Channel)
		if c.isMapCatchUpExpired(state, catchUpChOpts) {
			c.node.logger.log(newLogEntry(LogLevelInfo, "map subscribe catch-up timeout", map[string]any{
				"channel": req.Channel, "user": c.user, "client": c.uid,
			}))
			c.cleanupMapSubscribing(req.Channel)
			return DisconnectSlow
		}
		reply := SubscribeReply{Options: state.options}
		if handleErr := c.handleMapSubscribe(req, reply, cmd, started, rw); handleErr != nil {
			c.writeDisconnectOrErrorFlush(req.Channel, protocol.FrameTypeSubscribe, cmd, handleErr, started, rw)
		}
		return nil
	}

	if req.Phase != MapPhaseState && hasState {
		catchUpChOpts, _ := c.node.resolveMapChannelOptions(req.Channel)
		if c.isMapCatchUpExpired(state, catchUpChOpts) {
			c.node.logger.log(newLogEntry(LogLevelInfo, "map subscribe catch-up timeout", map[string]any{
				"channel": req.Channel, "user": c.user, "client": c.uid,
			}))
			c.cleanupMapSubscribing(req.Channel)
			return DisconnectSlow
		}
		reply := SubscribeReply{Options: state.options}
		if handleErr := c.handleMapSubscribe(req, reply, cmd, started, rw); handleErr != nil {
			c.writeDisconnectOrErrorFlush(req.Channel, protocol.FrameTypeSubscribe, cmd, handleErr, started, rw)
		}
		return nil
	}

	event := SubscribeEvent{
		Channel: req.Channel,
		Token:   req.Token,
		Data:    req.Data,
		Type:    SubscriptionType(req.Type),
	}

	cb := func(reply SubscribeReply, err error) {
		if err != nil {
			c.onSubscribeError(req.Channel)
			c.writeDisconnectOrErrorFlush(req.Channel, protocol.FrameTypeSubscribe, cmd, err, started, rw)
			return
		}

		if reply.Options.Type != event.Type {
			c.onSubscribeError(req.Channel)
			c.writeDisconnectOrErrorFlush(req.Channel, protocol.FrameTypeSubscribe, cmd, ErrorBadRequest, started, rw)
			return
		}

		if handleErr := c.handleMapSubscribe(req, reply, cmd, started, rw); handleErr != nil {
			c.onSubscribeError(req.Channel)
			c.writeDisconnectOrErrorFlush(req.Channel, protocol.FrameTypeSubscribe, cmd, handleErr, started, rw)
		}
	}

	c.eventHub.subscribeHandler(event, cb)
	return nil
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

	// Auto-set positioning flags from Mode.
	chOpts, err := c.node.resolveMapChannelOptions(channel)
	if err != nil {
		c.cleanupMapSubscribing(channel)
		return err
	}
	if chOpts.Mode.HasStream() {
		reply.Options.EnablePositioning = true
		reply.Options.EnableRecovery = true
	} else {
		reply.Options.EnablePositioning = false
		reply.Options.EnableRecovery = false
	}

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
		tf, err := c.validateAndCreateTagsFilter(req, reply.Options.AllowTagsFilter, channel)
		if err != nil {
			return err
		}
		c.mu.Lock()
		if c.mapSubscribing == nil {
			c.mapSubscribing = make(map[string]*mapSubscribeState)
		}
		var stf *tagsFilter
		if reply.Options.ServerTagsFilter != nil {
			stf = &tagsFilter{
				filter: reply.Options.ServerTagsFilter,
				hash:   filter.Hash(reply.Options.ServerTagsFilter),
			}
		}
		c.mapSubscribing[channel] = &mapSubscribeState{
			options:          reply.Options,
			startedAt:        time.Now().UnixNano(),
			isPresence:       reply.Options.Type.IsMapPresence(),
			subscribingCh:    make(chan struct{}),
			tagsFilter:       tf,
			serverTagsFilter: stf,
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
	chOpts, _ := c.node.resolveMapChannelOptions(channel)
	limit := c.getMapPageSize(req, chOpts)

	opts := MapReadStateOptions{
		AllowCached: true, // Use cache for subscription state delivery
		Cursor:      req.Cursor,
		Limit:       limit,
		Asc:         req.Asc,
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

	// Capture epoch and offset on first page (frozen for consistency).
	// The offset is used to return a consistent value on all subsequent pages,
	// ensuring the stream catch-up starts from where the first state page was read.
	if req.Cursor == "" && state != nil {
		c.mu.Lock()
		state.epoch = streamPos.Epoch
		state.offset = streamPos.Offset
		state.offsetCaptured = true
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

	// Apply server tags filter to state publications.
	if state != nil && state.serverTagsFilter != nil {
		filteredPubs := make([]*Publication, 0, len(pubs))
		for _, pub := range pubs {
			match, _ := filter.Match(state.serverTagsFilter.filter, pub.Tags)
			if match {
				filteredPubs = append(filteredPubs, pub)
			}
		}
		pubs = filteredPubs
	}
	// Apply client tags filter to state publications.
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
	if nextCursor == "" {
		if state == nil {
			// Disconnect raced with MapStateRead — subscription is being cleaned up.
			return nil
		}
		positioning := state.options.EnablePositioning || state.options.EnableRecovery

		// Use frozen offset from first page when available (multi-page pagination).
		// stateResult.Position reflects the stream top at the time of THIS page read,
		// but publications made during pagination won't appear in state pages AND would
		// be missed by stream catch-up if we use the current page's offset. The frozen
		// offset from the first page ensures stream catch-up covers the full gap.
		effectivePos := streamPos
		if state.offsetCaptured && req.Cursor != "" {
			effectivePos = StreamPosition{Offset: state.offset, Epoch: state.epoch}
		}

		if !positioning {
			// Streamless: always go LIVE on last page (no stream to paginate through).
			return c.handleMapStateToLive(req, reply, state, cmd, started, rw, pubs, effectivePos)
		}
		// Positioned: skip STREAM phase if stream hasn't advanced much.
		currentStreamPos, err := c.node.mapStreamPosition(c.ctx, channel)
		if err == nil {
			// Use limit as threshold - if stream is within one page, go LIVE.
			if effectivePos.Offset+uint64(limit) >= currentStreamPos.Offset {
				return c.handleMapStateToLive(req, reply, state, cmd, started, rw, pubs, effectivePos)
			}
		}
		// If error or stream too far ahead, fall through to normal STATE response.
	}

	// Use frozen offset from first page for consistency. On subsequent pages,
	// stream.Top() may have advanced, but we return the first page's offset so
	// the client's stream catch-up starts from a consistent point.
	responseOffset := streamPos.Offset
	if state != nil && state.offsetCaptured && req.Cursor != "" {
		responseOffset = state.offset
	}

	// Build response.
	res := &protocol.SubscribeResult{
		Type:   subscribeResultTypeMap,
		Phase:  MapPhaseState,
		Cursor: nextCursor,
		Epoch:  streamPos.Epoch,
		Offset: responseOffset,
	}

	// Convert state entries (use State field, not Publications).
	stateProtos := make([]*protocol.Publication, 0, len(pubs))
	for _, pub := range pubs {
		stateProtos = append(stateProtos, pubToProto(pub))
	}
	// JSON-escape state data for delta-enabled JSON transport so the client can
	// store exact bytes for subsequent delta application.
	deltaWillBeEnabled := req.Delta != "" && state != nil && slices.Contains(state.options.AllowedDeltaTypes, DeltaType(req.Delta))
	res.State = escapeStateForDelta(stateProtos, deltaWillBeEnabled, c.transport.Protocol() == ProtocolTypeJSON)

	return c.writeMapSubscribeReply(channel, cmd, res, started, rw)
}

// mapTransitionToLiveParams holds parameters that differ between the four
// methods that transition a map subscription to the live phase. The shared
// protocol (buffer -> subscribe -> stream-read -> merge -> respond -> finalize)
// is implemented once in handleMapTransitionToLive.
type mapTransitionToLiveParams struct {
	sincePosition             StreamPosition // Stream position to read from
	statePubs                 []*Publication // State publications for the response (nil when not applicable)
	allowStreamless           bool           // Whether streamless mode is allowed
	isRecovery                bool           // Whether this is a recovery (sets WasRecovering/Recovered)
	tagsFilterFromState       *tagsFilter    // Inherited client tags filter from prior phase
	serverTagsFilterFromState *tagsFilter    // Inherited server tags filter from prior phase
	metricsAction             string         // Metrics action string
}

// handleMapTransitionToLive implements the shared buffer-subscribe-read-merge protocol
// used by all four methods that transition a map subscription to the live phase:
// handleMapStateToLive, handleMapStreamToLive, and handleMapRecoveryJoin.
func (c *Client) handleMapTransitionToLive(
	req *protocol.SubscribeRequest,
	reply SubscribeReply,
	opts SubscribeOptions,
	isPresence bool,
	cmd *protocol.Command,
	started time.Time,
	rw *replyWriter,
	params mapTransitionToLiveParams,
) error {
	channel := req.Channel

	// Build subscription info first, validate before subscribing.
	useID := opts.AllowChannelCompaction && req.Flag&subscriptionFlagChannelCompression != 0
	sub := subInfo{client: c, deltaType: deltaTypeNone, useID: useID, isMap: true}

	// Process tags filter if provided.
	if req.Tf != nil {
		tf, err := c.validateAndCreateTagsFilter(req, opts.AllowTagsFilter, channel)
		if err != nil {
			c.cleanupMapSubscribing(channel)
			return err
		}
		sub.tagsFilter = tf
	} else if params.tagsFilterFromState != nil {
		// Use tags filter from prior phase if not provided in this request.
		sub.tagsFilter = params.tagsFilterFromState
	}
	// Server tags filter is always inherited from the state set at subscribe time.
	if params.serverTagsFilterFromState != nil {
		sub.serverTagsFilter = params.serverTagsFilterFromState
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

	positioning := opts.EnablePositioning || opts.EnableRecovery

	var recoveredPubs []*protocol.Publication
	var latestOffset uint64
	streamPos := params.sincePosition

	if positioning {
		// Positioned mode: read stream from sincePosition to catch any updates.
		chOpts, _ := c.node.resolveMapChannelOptions(channel)
		liveTransitionLimit := chOpts.LiveTransitionMaxPublicationLimit
		if liveTransitionLimit == 0 {
			// Default to MaxPageSize.
			liveTransitionLimit = chOpts.MaxPageSize
			if liveTransitionLimit <= 0 {
				liveTransitionLimit = defaultMapMaxPageSize
			}
		}
		streamLimit := -1 // No limit by default.
		if liveTransitionLimit > 0 {
			streamLimit = liveTransitionLimit
		}

		// Read limit+1 to distinguish "exactly at limit" from "too far behind".
		readLimit := streamLimit
		if readLimit > 0 {
			readLimit = streamLimit + 1
		}

		streamOpts := MapReadStreamOptions{
			Filter: StreamFilter{
				Since: &StreamPosition{
					Offset: params.sincePosition.Offset,
					Epoch:  params.sincePosition.Epoch,
				},
				Limit: readLimit,
			},
			AllowCached: true,
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
		streamPos = streamResult.Position

		// If recovering and the epoch doesn't match, force full re-subscribe.
		// This covers: empty→real (client never had epoch), real→empty (after MapClear
		// deleted meta row), and real→different (after Clear + new publications).
		//
		// For state→live (isRecovery=false), params.sincePosition.Epoch is the
		// epoch returned by the broker during the state phase. A mismatch here
		// means the broker flipped epochs between state and stream reads (e.g.
		// MapClear or meta-TTL eviction landed in between); without this check
		// the client would merge the prior-epoch state with new-epoch live pubs
		// and silently lose any keys not republished. The `!= ""` guard keeps
		// ephemeral-mode subscribes (which have no epoch) working.
		if (params.isRecovery || params.sincePosition.Epoch != "") && params.sincePosition.Epoch != streamPos.Epoch {
			c.pubSubSync.StopBuffering(channel)
			_ = c.node.removeSubscription(channel, c)
			c.cleanupMapSubscribing(channel)
			return ErrorUnrecoverablePosition
		}

		// If we got more than the limit, client is too far behind.
		if streamLimit > 0 && len(pubs) > streamLimit {
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
		latestOffset = streamPos.Offset
		if maxSeenOffset > latestOffset {
			latestOffset = maxSeenOffset
		}
		if len(recoveredPubs) > 0 {
			lastPubOffset := recoveredPubs[len(recoveredPubs)-1].Offset
			if lastPubOffset > latestOffset {
				latestOffset = lastPubOffset
			}
		}

		// Apply server tags filter to stream publications (after offset calculation).
		if sub.serverTagsFilter != nil {
			filteredPubs := make([]*protocol.Publication, 0, len(recoveredPubs))
			for _, pub := range recoveredPubs {
				match, _ := filter.Match(sub.serverTagsFilter.filter, pub.Tags)
				if match {
					filteredPubs = append(filteredPubs, pub)
				}
			}
			recoveredPubs = filteredPubs
		}
		// Apply client tags filter to stream publications.
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
			recoveredPubs = c.makeRecoveredMapPubsDeltaFossil(recoveredPubs)
		}
	} else if params.allowStreamless {
		// Streamless mode: use buffered publications directly (no stream read, no merge).
		bufferedPubs := c.pubSubSync.LockBufferAndReadBuffered(channel)
		recoveredPubs = bufferedPubs

		// Apply tags filter to buffered publications.
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
	}

	// Convert state publications to protocol format (if any).
	isJSON := c.transport.Protocol() == ProtocolTypeJSON
	var protoStatePubs []*protocol.Publication
	if len(params.statePubs) > 0 {
		protoStatePubs = make([]*protocol.Publication, 0, len(params.statePubs))
		for _, pub := range params.statePubs {
			protoStatePubs = append(protoStatePubs, pubToProto(pub))
		}
		protoStatePubs = escapeStateForDelta(protoStatePubs, deltaEnabled, isJSON)
	}

	// Build response with phase=0 (LIVE).
	res := &protocol.SubscribeResult{
		Type:         subscribeResultTypeMap,
		Phase:        MapPhaseLive,
		Epoch:        streamPos.Epoch,
		Offset:       latestOffset,
		Delta:        deltaEnabled,
		State:        protoStatePubs,
		Publications: recoveredPubs,
	}
	if d := opts.ClientPublishDebounceInterval; d > 0 {
		res.PublishDebounce = uint32(d.Milliseconds())
	}
	if positioning {
		res.Recoverable = true
	}
	if params.isRecovery {
		res.WasRecovering = req.Recover
		if req.Recover {
			res.Recovered = true
		}
	}
	if chanID > 0 {
		res.Id = chanID
	}

	// Encode reply first so an encode failure is surfaced before we touch
	// c.channels — keeps the rollback path simple.
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
		metaTTLSeconds:           int64(opts.HistoryMetaTTL.Seconds()),
		positionCheckTime:        time.Now().Unix(),
		Source:                   opts.Source,
		mapClientPresenceChannel: opts.MapClientPresenceChannel,
		mapUserPresenceChannel:   opts.MapUserPresenceChannel,
	}

	// Install channelContext BEFORE writing the reply so that any follow-up
	// commands from the SDK that arrive between the reply send and StopBuffering
	// observe the channel as subscribed. Buffered PUB/SUB publications stay
	// queued until StopBuffering below — they cannot reach the client yet.
	// Move from mapSubscribing to channels. Always look up from the map under
	// the lock to avoid closing a subscribingCh that was already closed by a
	// concurrent disconnect handler (cleanupMapSubscribingAll).
	//
	// Re-check c.status under the lock — Client.close() may have run between
	// addSubscription and here. If it has, c.channels was snapshotted before our
	// entry existed (so close() did NOT remove our hub subscription), and
	// cleanupMapSubscribingAll dropped the mapSubscribing entry. Writing
	// channelContext now would leave a ghost subscription in the hub with no
	// cleanup path. Roll back instead. Mirrors the pattern in subscribeCmd.
	c.mu.Lock()
	if c.status == statusClosed {
		c.mu.Unlock()
		_ = c.node.removeSubscription(channel, c)
		c.releaseSubscribeCommandReply(protoReply)
		c.pubSubSync.StopBuffering(channel)
		return ErrorInternal
	}
	if st, ok := c.mapSubscribing[channel]; ok && st.subscribingCh != nil {
		close(st.subscribingCh)
	}
	delete(c.mapSubscribing, channel)
	c.channels[channel] = channelContext
	c.mu.Unlock()

	c.writeEncodedCommandReply(channel, protocol.FrameTypeSubscribe, cmd, protoReply, rw)
	c.handleCommandFinished(cmd, protocol.FrameTypeSubscribe, nil, protoReply, started, "")
	c.releaseSubscribeCommandReply(protoReply)
	c.node.metrics.incActionCount(params.metricsAction, channel)
	if params.isRecovery && req.Recover {
		c.node.metrics.incRecover(true, channel, len(recoveredPubs) > 0)
		c.node.metrics.observeRecoveredPublications(len(recoveredPubs), channel)
	}

	// Stop buffering after response written.
	c.pubSubSync.StopBuffering(channel)

	// Add presence and join handling.
	c.setupMapPresenceAndJoin(channel, opts)

	return nil
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
	return c.handleMapTransitionToLive(req, reply, state.options, state.isPresence, cmd, started, rw, mapTransitionToLiveParams{
		sincePosition:             statePos,
		statePubs:                 statePubs,
		allowStreamless:           true,
		isRecovery:                false,
		tagsFilterFromState:       state.tagsFilter,
		serverTagsFilterFromState: state.serverTagsFilter,
		metricsAction:             "map_subscribe_state_to_live",
	})
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

	// Reject stream phase in streamless mode.
	if !reply.Options.EnablePositioning && !reply.Options.EnableRecovery {
		c.cleanupMapSubscribing(channel)
		return ErrorBadRequest
	}

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
			startedAt:     time.Now().UnixNano(),
			isPresence:    reply.Options.Type.IsMapPresence(),
			subscribingCh: make(chan struct{}),
			epoch:         req.Epoch,
		}
		tf, err := c.validateAndCreateTagsFilter(req, reply.Options.AllowTagsFilter, channel)
		if err != nil {
			return err
		}
		state.tagsFilter = tf
		if reply.Options.ServerTagsFilter != nil {
			state.serverTagsFilter = &tagsFilter{
				filter: reply.Options.ServerTagsFilter,
				hash:   filter.Hash(reply.Options.ServerTagsFilter),
			}
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
		streamPos, err := c.node.mapStreamPosition(c.ctx, channel)
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

	chOpts, _ := c.node.resolveMapChannelOptions(channel)
	limit := c.getMapPageSize(req, chOpts)

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
		AllowCached: true,
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

	// For intermediate STREAM pages, use the last publication's offset rather than
	// stream.Top(). Returning stream.Top() would cause the client to jump ahead,
	// skipping publications between the last returned pub and stream.Top().
	responseOffset := req.Offset
	if len(pubs) > 0 {
		responseOffset = pubs[len(pubs)-1].Offset
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
		Type:   subscribeResultTypeMap,
		Phase:  MapPhaseStream,
		Epoch:  streamPos.Epoch,
		Offset: responseOffset,
	}

	// Convert publications.
	protoPubs := make([]*protocol.Publication, 0, len(pubs))
	for _, pub := range pubs {
		protoPubs = append(protoPubs, pubToProto(pub))
	}
	// JSON-escape stream publication data for delta-enabled JSON transport so the client
	// can store exact bytes for subsequent delta application.
	deltaWillBeEnabled := req.Delta != "" && state != nil && slices.Contains(state.options.AllowedDeltaTypes, DeltaType(req.Delta))
	res.Publications = escapeStateForDelta(protoPubs, deltaWillBeEnabled, c.transport.Protocol() == ProtocolTypeJSON)

	return c.writeMapSubscribeReply(channel, cmd, res, started, rw)
}

func (c *Client) getMapPageSize(req *protocol.SubscribeRequest, chOpts MapChannelOptions) int {
	limit := int(req.Limit)
	defaultLimit := chOpts.DefaultPageSize
	if defaultLimit <= 0 {
		defaultLimit = defaultMapPageSize
	}
	minLimit := chOpts.MinPageSize
	if minLimit <= 0 {
		minLimit = defaultMapMinPageSize
	}
	maxLimit := chOpts.MaxPageSize
	if maxLimit <= 0 {
		maxLimit = defaultMapMaxPageSize
	}
	if limit <= 0 {
		limit = defaultLimit
	}
	if limit < minLimit {
		limit = minLimit
	}
	if limit > maxLimit {
		limit = maxLimit
	}
	return limit
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
	return c.handleMapTransitionToLive(req, reply, state.options, state.isPresence, cmd, started, rw, mapTransitionToLiveParams{
		sincePosition:             StreamPosition{Offset: req.Offset, Epoch: req.Epoch},
		statePubs:                 nil,
		allowStreamless:           false,
		isRecovery:                true,
		tagsFilterFromState:       state.tagsFilter,
		serverTagsFilterFromState: state.serverTagsFilter,
		metricsAction:             "map_subscribe_stream_to_live",
	})
}

// handleMapLivePhase handles joining pub/sub with coordination.
// Used for recovery or paginated join: after pagination or reconnect, returns only stream catch-up.
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
	var serverTagsFilterFromState *tagsFilter
	c.mu.RLock()
	state, hasState := c.mapSubscribing[channel]
	c.mu.RUnlock()

	if hasState {
		opts = state.options
		isPresence = state.isPresence
		tagsFilterFromState = state.tagsFilter
		serverTagsFilterFromState = state.serverTagsFilter
		// Validate epoch if client provided one.
		if req.Epoch != "" && state.epoch != "" && req.Epoch != state.epoch {
			c.cleanupMapSubscribing(channel)
			return ErrorUnrecoverablePosition
		}
	} else {
		opts = reply.Options
		isPresence = opts.Type.IsMapPresence()
		// Direct-LIVE recovery: no STATE phase ran, so the server filter from
		// reply.Options never landed in mapSubscribing. Inherit it here so it
		// applies to recovered + live publications. Without this the server-side
		// RBAC filter is bypassed on clean reconnect.
		if opts.ServerTagsFilter != nil {
			serverTagsFilterFromState = &tagsFilter{
				filter: opts.ServerTagsFilter,
				hash:   filter.Hash(opts.ServerTagsFilter),
			}
		}
	}

	// Recovery or paginated join - stream catch-up needed.
	return c.handleMapRecoveryJoin(req, reply, opts, isPresence, tagsFilterFromState, serverTagsFilterFromState, cmd, started, rw)
}

// handleMapRecoveryJoin handles recovery or paginated join (stream catch-up only).
func (c *Client) handleMapRecoveryJoin(
	req *protocol.SubscribeRequest,
	reply SubscribeReply,
	opts SubscribeOptions,
	isPresence bool,
	tagsFilterFromState *tagsFilter,
	serverTagsFilterFromState *tagsFilter,
	cmd *protocol.Command,
	started time.Time,
	rw *replyWriter,
) error {
	return c.handleMapTransitionToLive(req, reply, opts, isPresence, cmd, started, rw, mapTransitionToLiveParams{
		sincePosition:             StreamPosition{Offset: req.Offset, Epoch: req.Epoch},
		statePubs:                 nil,
		allowStreamless:           false,
		isRecovery:                true,
		tagsFilterFromState:       tagsFilterFromState,
		serverTagsFilterFromState: serverTagsFilterFromState,
		metricsAction:             "map_subscribe_recovery_join",
	})
}

// buildMapChannelFlags builds channel flags for map subscriptions.
func (c *Client) buildMapChannelFlags(deltaEnabled bool, delta string, isPresence bool, opts SubscribeOptions, reply SubscribeReply) uint16 {
	var channelFlags uint16
	channelFlags |= flagSubscribed
	if opts.EnablePositioning || opts.EnableRecovery {
		channelFlags |= flagPositioning
	}
	channelFlags |= flagMap // Mark as map subscription.
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
	if opts.MapClientPresenceChannel != "" {
		channelFlags |= flagMapClientPresence
	}
	if opts.MapUserPresenceChannel != "" {
		channelFlags |= flagMapUserPresence
	}
	if opts.MapRemoveClientOnUnsubscribe {
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

	// Add map client presence if channel is configured.
	if opts.MapClientPresenceChannel != "" {
		info := &ClientInfo{
			ClientID: c.uid,
			UserID:   c.user,
			ConnInfo: c.info,
			ChanInfo: opts.ChannelInfo,
		}
		if err := c.addMapClientPresence(opts.MapClientPresenceChannel, info); err != nil {
			c.node.logger.log(newErrorLogEntry(err, "error adding map client presence", map[string]any{
				"channel": channel, "user": c.user, "client": c.uid,
			}))
		}
	}

	// Add map user presence if channel is configured.
	if opts.MapUserPresenceChannel != "" {
		if err := c.addMapUserPresence(opts.MapUserPresenceChannel); err != nil {
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

// isMapCatchUpExpired checks whether the map catch-up has exceeded the configured timeout.
func (c *Client) isMapCatchUpExpired(state *mapSubscribeState, chOpts MapChannelOptions) bool {
	timeout := chOpts.SubscribeCatchUpTimeout
	if timeout == 0 {
		timeout = 5 * time.Second
	}
	if timeout < 0 {
		return false // Disabled.
	}
	return time.Duration(time.Now().UnixNano()-state.startedAt) > timeout
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

// sweepExpiredMapSubscribing removes expired mapSubscribing entries for channels
// other than skipChannel. The skipped channel is handled by the caller with a
// proper DisconnectSlow. Cleans up abandoned catch-ups where the client stopped
// sending requests but stayed connected. Called at the start of every map subscribe
// command — O(n) where n is in-progress catch-ups (typically 0–2), no-op when map
// is empty.
func (c *Client) sweepExpiredMapSubscribing(skipChannel string) {
	// Snapshot under read lock so we can resolve channel options without holding
	// c.mu — resolveMapChannelOptions invokes a user-supplied callback and must
	// not run under c.mu (deadlock / priority-inversion risk if the callback
	// touches per-client state or external services).
	c.mu.RLock()
	n := len(c.mapSubscribing)
	if n == 0 {
		c.mu.RUnlock()
		return
	}
	type sweepExpired struct {
		ch    string
		state *mapSubscribeState
	}
	candidates := make([]sweepExpired, 0, n)
	for ch, state := range c.mapSubscribing {
		if ch == skipChannel {
			continue
		}
		candidates = append(candidates, sweepExpired{ch: ch, state: state})
	}
	c.mu.RUnlock()

	// Resolve options + check expiry outside any lock.
	var expired []sweepExpired
	for _, cand := range candidates {
		sweepChOpts, _ := c.node.resolveMapChannelOptions(cand.ch)
		if c.isMapCatchUpExpired(cand.state, sweepChOpts) {
			expired = append(expired, cand)
		}
	}
	if len(expired) == 0 {
		return
	}

	// Delete only entries whose *mapSubscribeState pointer still matches the
	// snapshot — guards against a resubscribe that replaced the entry between
	// the RUnlock and the Lock.
	c.mu.Lock()
	for _, e := range expired {
		if state, ok := c.mapSubscribing[e.ch]; ok && state == e.state {
			if state.subscribingCh != nil {
				close(state.subscribingCh)
			}
			delete(c.mapSubscribing, e.ch)
		}
	}
	c.mu.Unlock()
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

// addMapClientPresence adds client presence to the given channel.
// Key is clientId, stores full ClientInfo.
func (c *Client) addMapClientPresence(presenceChannel string, info *ClientInfo) error {
	// Use KeyModeIfNew with RefreshTTLOnSuppress to:
	// - Publish JOIN event only if this is a new presence entry
	// - Refresh TTL without publishing if entry already exists (quick reconnect)
	// KeyTTL is configured in GetMapChannelOptions for presence channels.
	_, err := c.node.MapPublish(c.ctx, presenceChannel, c.uid, MapPublishOptions{
		ClientInfo:           info,
		KeyMode:              KeyModeIfNew,
		RefreshTTLOnSuppress: true,
	})
	return err
}

// addMapUserPresence adds user presence to the given channel.
// Key is userId, no ClientInfo stored (just the key for uniqueness).
// No-op for anonymous connections (empty user ID) — user-presence has no
// meaningful key without a user ID, and MapPublish would reject the empty key.
func (c *Client) addMapUserPresence(presenceChannel string) error {
	if c.user == "" {
		return nil
	}
	// Use KeyModeIfNew with RefreshTTLOnSuppress to:
	// - Publish JOIN event only if this is a new user
	// - Refresh TTL without publishing if user already exists
	// KeyTTL is configured in GetMapChannelOptions for presence channels.
	_, err := c.node.MapPublish(c.ctx, presenceChannel, c.user, MapPublishOptions{
		KeyMode:              KeyModeIfNew,
		RefreshTTLOnSuppress: true,
	})
	return err
}

// updateMapPresence updates presence for a map channel using MapBroker.
// This is called periodically by updateChannelPresence to refresh the TTL.
// Handles presence channels based on configured prefixes.
func (c *Client) updateMapPresence(info *ClientInfo, ctx ChannelContext) error {
	// Use KeyModeIfNew with RefreshTTLOnSuppress for TTL refresh:
	// - Since key already exists, publish is suppressed (no offset increment)
	// - TTL is refreshed without generating stream entries
	// Stream options (StreamSize/TTL/MetaTTL) use defaults from GetMapChannelOptions.

	// Update client presence if channel is configured.
	if ctx.mapClientPresenceChannel != "" {
		_, err := c.node.MapPublish(c.ctx, ctx.mapClientPresenceChannel, c.uid, MapPublishOptions{
			ClientInfo:           info,
			KeyMode:              KeyModeIfNew,
			RefreshTTLOnSuppress: true,
		})
		if err != nil {
			return err
		}
	}

	// Update user presence if channel is configured. Skip for anonymous
	// connections (empty user ID) — user-presence is keyless without a user.
	if ctx.mapUserPresenceChannel != "" && c.user != "" {
		_, err := c.node.MapPublish(c.ctx, ctx.mapUserPresenceChannel, c.user, MapPublishOptions{
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

	// Remove client presence if channel is configured.
	// Stream options (StreamSize/TTL/MetaTTL) use defaults from GetMapChannelOptions.
	if ctx.mapClientPresenceChannel != "" {
		_, err := c.node.MapRemove(context.Background(), ctx.mapClientPresenceChannel, c.uid, MapRemoveOptions{})
		if err != nil {
			c.node.logger.log(newErrorLogEntry(err, "error removing map clients presence", map[string]any{"channel": channel, "user": c.user, "client": c.uid, "error": err.Error()}))
		}
	}

	// User presence entries are NOT removed on disconnect - they only expire via TTL.
	// This provides debounce/grace period for quick reconnects.

	// Remove key=clientId from channel if MapRemoveClientOnUnsubscribe is enabled.
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
