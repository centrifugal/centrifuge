package centrifuge

import (
	"context"
	"sort"
	"strconv"
	"time"
)

// MapBroker Overview.
//
// MapBroker manages synchronized state for map subscriptions. It provides:
//
//   - State storage: Key-value state where each key maps to a publication.
//     Clients receive the full state when subscribing.
//
//   - Stream (history): Ordered log of all changes for recovery. Clients use
//     this to catch up on changes that occurred during subscription setup or
//     after temporary disconnection.
//
//   - Pub/Sub integration: Real-time delivery of changes to subscribed clients.
//
// Data flow for publish operations:
//
//	Publish() -> Update state -> Append to stream -> Broadcast via pub/sub
//
// Client subscription phases:
//
//  1. State phase: Client calls ReadState() repeatedly to paginate through
//     the current state. Each response includes a stream position for consistency.
//
//  2. Stream phase (optional): Client calls ReadStream() to catch up on changes
//     that occurred since the state read. May require multiple calls for pagination.
//
//  3. Live phase: Server coordinates transition to real-time with buffering:
//     a) Start buffering incoming pub/sub messages
//     b) Subscribe to pub/sub channel
//     c) ReadStream() for final catch-up since client's last position
//     d) Merge stream results with buffered messages (deduplication by offset)
//     e) Send merged publications to client
//     f) Stop buffering - client now receives live pub/sub updates
//
// This coordination ensures no messages are lost during the transition from
// historical catch-up to live streaming, even if new messages arrive during
// the final ReadStream() call.
//
// The broker supports several advanced features:
//
//   - Idempotency: Duplicate detection via IdempotencyKey to safely retry operations.
//   - Versioning: Optimistic concurrency control via Version/VersionEpoch fields.
//   - Conditional writes: KeyMode (IfNew, IfExists) for atomic check-and-set operations.
//   - TTL: Automatic key expiration with KeyTTL for presence and ephemeral data.
//   - Ordering: Optional score-based ordering for leaderboards and sorted collections.
//
// Implementations must ensure:
//
//   - State and stream consistency (same epoch, contiguous offsets)
//   - Atomic state updates (no partial writes visible to readers)
//   - Stream position tracking for recovery validation

// KeyMode controls conditional publishing based on key existence in the state.
type KeyMode string

const (
	// KeyModeReplace always writes the key (default behavior, backward compatible).
	KeyModeReplace KeyMode = ""

	// KeyModeIfNew only writes if the key does NOT exist in the state.
	// Use cases: lobby slots, distributed locks, first-claim resources.
	// When key already exists, Suppressed=true with SuppressReason="key_exists".
	KeyModeIfNew KeyMode = "if_new"

	// KeyModeIfExists only writes if the key ALREADY exists in the state.
	// Use cases: heartbeats, update-only operations, presence renewal.
	// When key does not exist, Suppressed=true with SuppressReason="key_not_found".
	KeyModeIfExists KeyMode = "if_exists"
)

// MapBroker is the interface for managing map subscription state.
// See "MapBroker Overview" above for detailed documentation.
type MapBroker interface {
	// RegisterEventHandler registers a BrokerEventHandler to handle publications
	// received from pub/sub. Called by Node.Run() during startup.
	RegisterEventHandler(BrokerEventHandler) error

	// Subscribe registers this server node to receive pub/sub messages for the channel.
	// Called when a client subscribes to a map channel on this node.
	Subscribe(ch string) error

	// Unsubscribe removes this server node from receiving pub/sub messages for the channel.
	// Called when the last client on this node unsubscribes from the channel.
	Unsubscribe(ch string) error

	// Publish updates the state and broadcasts the change to subscribers.
	//
	// The operation:
	//  1. Updates the key in the state (subject to KeyMode conditions)
	//  2. Appends the publication to the stream (if StreamSize > 0)
	//  3. Broadcasts to pub/sub subscribers
	//
	// Returns MapPublishResult with Suppressed=false when the state was changed.
	// When Suppressed=true (idempotency, version conflict, or KeyMode condition),
	// no stream entry is created and no pub/sub broadcast occurs.
	Publish(ctx context.Context, ch string, key string, opts MapPublishOptions) (MapPublishResult, error)

	// Remove removes a key from the state and notifies subscribers.
	//
	// The operation:
	//  1. Removes the key from the state
	//  2. Appends a removal publication to the stream (if StreamSize > 0)
	//  3. Broadcasts removal to pub/sub subscribers
	//
	// Returns MapPublishResult with Suppressed=false when the key was removed.
	// When Suppressed=true (key not found or idempotency), no stream entry or broadcast occurs.
	Remove(ctx context.Context, ch string, key string, opts MapRemoveOptions) (MapPublishResult, error)

	// ReadStream retrieves publications from the channel's history stream.
	//
	// Used by clients to catch up on changes after receiving the state.
	// Supports filtering by position (Since) and pagination (Limit).
	// Returns publications, current stream position, and error.
	//
	// Special Limit values:
	//  - Limit = 0: Only return current stream position (no publications)
	//  - Limit = -1: Return all publications (no limit)
	ReadStream(ctx context.Context, ch string, opts MapReadStreamOptions) (MapStreamResult, error)

	// ReadState retrieves the current key-value state for a channel.
	//
	// Returns all keys in the state as publications (each with Key field set).
	// Supports cursor-based pagination for large states.
	//
	// If opts.Revision is provided, validates that the epoch hasn't changed.
	// Returns ErrorUnrecoverablePosition if epoch changed (client must restart).
	ReadState(ctx context.Context, ch string, opts MapReadStateOptions) (MapStateResult, error)

	// Stats returns statistics about the channel in storage.
	Stats(ctx context.Context, ch string) (MapStats, error)

	// Clear deletes all data for a channel (state and stream).
	// Use for cleanup when a channel is no longer needed.
	Clear(ctx context.Context, ch string, opts MapClearOptions) error
}

// MapPublishOptions defines options for publishing to a map channel.
type MapPublishOptions struct {
	// IdempotencyKey enables duplicate detection. If the same key is seen within
	// IdempotentResultTTL, the operation is suppressed (Suppressed=true).
	// Use for safe client retries without duplicating state changes.
	IdempotencyKey      string
	IdempotentResultTTL time.Duration

	// StreamSize sets the maximum number of entries in the stream (history).
	// Zero value means using default from MapChannelOptionsResolver (which defaults
	// to 0, i.e. streamless). Negative values are treated as 0 (disabled).
	// When StreamSize is 0 after defaults are applied, no stream history is maintained.
	// CAS (ExpectedPosition) and Version-based dedup require StreamSize > 0.
	StreamSize int
	// StreamTTL sets how long stream entries are retained.
	// Zero value means using default from MapChannelOptionsResolver.
	StreamTTL time.Duration
	// MetaTTL sets how long stream metadata (epoch, top offset) is retained
	// after all entries expire. Allows position validation even with empty stream.
	// Zero value means using default from MapChannelOptionsResolver.
	MetaTTL time.Duration

	// KeyTTL sets automatic expiration for this key. After TTL expires, the key
	// is removed from state and a removal event is published to stream.
	// Zero value means using default from MapChannelOptionsResolver (1 minute by default).
	// Use negative value (-1) to explicitly disable key expiration.
	KeyTTL time.Duration
	// Data is the publication payload (usually JSON, but can be binary also).
	// Stored in state and published to stream/pub-sub (unless StreamData is set).
	Data []byte
	// StreamData, if set, overrides Data for stream and pub-sub publication.
	// Use when you want to store full state in state but publish incremental updates.
	// Example: state stores {"count":105}, stream publishes {"delta":5}.
	StreamData []byte
	// Tags are key-value metadata for filtering. Subscribers can filter by tags
	// to receive only relevant publications.
	Tags map[string]string
	// ClientInfo attaches client information to the publication.
	ClientInfo *ClientInfo
	// UseDelta enables delta compression. When true, only the difference from
	// the previous value is stored/sent (requires client support).
	// Note: delta is automatically disabled when StreamData is set, because
	// StreamData provides explicit incremental data, making delta redundant.
	UseDelta bool

	// Version enables optimistic concurrency control. If set, the publish only
	// succeeds if the current version is lower. Use with VersionEpoch for
	// cross-epoch version tracking.
	Version uint64
	// VersionEpoch scopes the Version. Different epochs reset version comparison.
	VersionEpoch string

	// Score is the sort value when Ordered=true (configured in MapChannelOptions).
	// Higher scores appear first.
	// Use for leaderboards, priority queues, and sorted collections.
	Score int64

	// KeyMode controls conditional publishing based on key existence.
	// Default (empty/KeyModeReplace) always writes.
	// KeyModeIfNew only writes if key doesn't exist (for claiming resources).
	// KeyModeIfExists only writes if key exists (for updates/heartbeats).
	KeyMode KeyMode

	// RefreshTTLOnSuppress when true will refresh the TTL of existing key even when
	// publish is suppressed due to KeyMode (e.g., KeyModeIfNew when key exists).
	// Useful for presence keep-alive without generating new stream entries.
	RefreshTTLOnSuppress bool

	// ExpectedPosition enables Compare-And-Swap semantics.
	// If set, publish only succeeds if the key's current position (offset+epoch) matches.
	// Returns Suppressed=true with SuppressReasonPositionMismatch on mismatch.
	// Use with ReadState Key filter for atomic read-modify-write.
	ExpectedPosition *StreamPosition
}

// MapRemoveOptions defines options for removing a key from a map channel.
type MapRemoveOptions struct {
	// IdempotencyKey enables duplicate detection for removal operations.
	IdempotencyKey      string
	IdempotentResultTTL time.Duration

	// StreamSize sets the maximum stream size. The removal event is appended
	// to the stream so clients can recover the removal during catch-up.
	// Zero value means using default from MapChannelOptionsResolver.
	StreamSize int
	// StreamTTL sets how long the removal entry is retained in stream.
	// Zero value means using default from MapChannelOptionsResolver.
	StreamTTL time.Duration
	// MetaTTL sets how long stream metadata is retained.
	// Zero value means using default from MapChannelOptionsResolver.
	MetaTTL time.Duration

	// ExpectedPosition enables Compare-And-Swap semantics for removal.
	// If set, remove only succeeds if the key's current position (offset+epoch) matches.
	// Returns Suppressed=true with SuppressReasonPositionMismatch on mismatch.
	ExpectedPosition *StreamPosition
}

// StreamFilter controls which publications are returned from ReadStream.
type StreamFilter struct {
	// Since filters to publications after this position. Only publications
	// with Offset > Since.Offset are returned. The epoch must match.
	Since *StreamPosition

	// Limit controls maximum publications returned:
	//  -1 = no limit (return all publications in stream)
	//   0 = return only current stream position (no publications)
	//  >0 = return at most this many publications
	Limit int

	// Reverse returns publications in reverse order (newest first).
	// Default is oldest first (ascending offset order).
	Reverse bool
}

// MapReadStreamOptions defines options for reading a channel's history stream.
type MapReadStreamOptions struct {
	// Filter controls which publications are returned (position, limit, direction).
	Filter StreamFilter

	// MetaTTL extends the stream metadata TTL when reading.
	// Overrides the default Config.HistoryMetaTTL for this operation.
	MetaTTL time.Duration
}

// MapReadStateOptions defines options for reading a channel's state.
type MapReadStateOptions struct {
	// Revision is the stream position from a previous state read.
	// If provided, the server validates that the epoch hasn't changed.
	// Returns ErrorUnrecoverablePosition if epoch changed (state invalidated).
	Revision *StreamPosition

	// Cursor is the pagination cursor from a previous ReadState call.
	// Empty string starts from the beginning.
	Cursor string

	// Limit controls maximum entries returned per page:
	//  -1 = no limit (return all entries in state)
	//   0 = return only current stream position (no entries)
	//  >0 = return at most this many entries
	Limit int

	// MetaTTL extends the state metadata TTL when reading.
	MetaTTL time.Duration

	// Key filters to a single entry by exact key match.
	// When set, returns at most one publication (or empty if key not found).
	// Cursor/Limit are ignored when Key is set.
	Key string

	// Cached allows reading from cache instead of primary storage.
	// This is an internal option used by the subscription flow for optimized delivery.
	// Application code should NOT set this - leave it false to always read fresh data
	// from the backend (safe for CAS operations, always consistent).
	// Only affects CachedMapBroker; other brokers ignore this option.
	Cached bool
}

// MapStats provides statistics about a channel's state.
type MapStats struct {
	// NumKeys is the current number of keys in the state.
	NumKeys int
}

// SuppressReason explains why a Publish or Remove operation was suppressed.
type SuppressReason string

const (
	// SuppressReasonNone indicates the operation was not suppressed.
	SuppressReasonNone SuppressReason = ""

	// SuppressReasonIdempotency indicates duplicate detected via idempotency key.
	SuppressReasonIdempotency SuppressReason = "idempotency"

	// SuppressReasonVersion indicates skipped due to out-of-order version.
	SuppressReasonVersion SuppressReason = "version"

	// SuppressReasonKeyExists indicates KeyModeIfNew was used but key already exists.
	SuppressReasonKeyExists SuppressReason = "key_exists"

	// SuppressReasonKeyNotFound indicates KeyModeIfExists was used but key doesn't exist,
	// or Remove was called on a non-existent key.
	SuppressReasonKeyNotFound SuppressReason = "key_not_found"

	// SuppressReasonPositionMismatch indicates CAS failed - key's position changed.
	SuppressReasonPositionMismatch SuppressReason = "position_mismatch"
)

// MapPublishResult contains the result of Publish or Remove operation.
type MapPublishResult struct {
	// Position is the current stream position after the operation.
	Position StreamPosition
	// Suppressed is true when the operation did NOT change the state.
	// When Suppressed is true, no message is appended to the stream and no
	// publication is sent to PUB/SUB.
	Suppressed bool
	// SuppressReason explains why the operation was suppressed (empty when Suppressed is false).
	SuppressReason SuppressReason
	// CurrentPublication contains the current state when suppressed due to CAS mismatch.
	// Allows immediate retry without extra ReadState call.
	// Only set when SuppressReason is SuppressReasonPositionMismatch.
	CurrentPublication *Publication
}

// MapStreamResult contains the result of a ReadStream operation.
type MapStreamResult struct {
	// Publications contains the stream entries.
	Publications []*Publication
	// Position is the current stream position.
	Position StreamPosition
}

// MapStateResult contains the result of a ReadState operation.
type MapStateResult struct {
	// Publications contains the state entries (each with Key field set).
	Publications []*Publication
	// Position is the current stream position at the time of reading.
	Position StreamPosition
	// Cursor is the pagination cursor for the next page ("" if done).
	Cursor string
}

// MapClearOptions defines options for removing all channel data.
// Currently empty but reserved for future options (e.g., selective removal).
type MapClearOptions struct{}

// resolveChannelOptions resolves channel options from the resolver, falling back to defaults.
// Used by broker implementations to resolve once per operation and avoid repeated resolver calls.
func resolveChannelOptions(resolver MapChannelOptionsResolver, channel string) MapChannelOptions {
	if resolver != nil {
		return resolver(channel)
	}
	return DefaultMapChannelOptions()
}

// applyChannelOptionsDefaults fills in missing channel options from the resolver.
// If a value is 0, it's replaced with the default from resolver.
// If a value is -1 (for durations) or negative (for StreamSize), it's set to 0 (explicitly disabled).
// Positive values are kept as-is.
func applyChannelOptionsDefaults(
	opts MapChannelOptions,
	resolved MapChannelOptions,
) MapChannelOptions {
	defaults := resolved

	// Apply StreamSize: negative means explicitly disabled (use 0), 0 means use default.
	if opts.StreamSize < 0 {
		opts.StreamSize = 0
	} else if opts.StreamSize == 0 {
		opts.StreamSize = defaults.StreamSize
	}

	// Apply StreamTTL: -1 means explicitly disabled (use 0), 0 means use default.
	if opts.StreamTTL < 0 {
		opts.StreamTTL = 0
	} else if opts.StreamTTL == 0 {
		opts.StreamTTL = defaults.StreamTTL
	}

	// Apply MetaTTL: -1 means explicitly disabled (use 0), 0 means use default.
	if opts.MetaTTL < 0 {
		opts.MetaTTL = 0
	} else if opts.MetaTTL == 0 {
		opts.MetaTTL = defaults.MetaTTL
	}

	// Apply KeyTTL: -1 means explicitly disabled (use 0), 0 means use default.
	if opts.KeyTTL < 0 {
		opts.KeyTTL = 0
	} else if opts.KeyTTL == 0 {
		opts.KeyTTL = defaults.KeyTTL
	}

	// Ensure MetaTTL is never less than StreamTTL or KeyTTL.
	// Meta must outlive the data it describes to avoid stale reads.
	if opts.StreamTTL > 0 && opts.MetaTTL < opts.StreamTTL {
		opts.MetaTTL = opts.StreamTTL
	}
	if opts.KeyTTL > 0 && opts.MetaTTL < opts.KeyTTL {
		opts.MetaTTL = opts.KeyTTL
	}

	return opts
}

// makeOrderedCursor creates a cursor for ordered state: "score\x00key".
func makeOrderedCursor(score, key string) string {
	return score + "\x00" + key
}

// parseOrderedCursor parses an ordered cursor into score and key strings.
func parseOrderedCursor(cursor string) (string, string) {
	for i := 0; i < len(cursor); i++ {
		if cursor[i] == '\x00' {
			return cursor[:i], cursor[i+1:]
		}
	}
	return "", ""
}

// findUnorderedCursorPosition finds the position after the cursor key using binary search.
// Returns the index of the first key > cursor in a sorted slice.
func findUnorderedCursorPosition(sortedKeys []string, cursor string) int {
	return sort.Search(len(sortedKeys), func(i int) bool {
		return sortedKeys[i] > cursor
	})
}

// findOrderedCursorPosition finds the position after the cursor (score, key) in ordered state.
// For ordered state sorted by (score DESC, key DESC), finds first entry where:
// - score < cursorScore, OR
// - score == cursorScore AND key < cursorKey
func findOrderedCursorPosition(sortedKeys []string, scores map[string]int64, cursor string) int {
	cursorScoreStr, cursorKey := parseOrderedCursor(cursor)
	cursorScore, _ := strconv.ParseInt(cursorScoreStr, 10, 64)

	return sort.Search(len(sortedKeys), func(i int) bool {
		key := sortedKeys[i]
		score := scores[key]
		if score != cursorScore {
			return score < cursorScore // Score descending: looking for score < cursorScore
		}
		return key < cursorKey // Same score, key descending: looking for key < cursorKey
	})
}
