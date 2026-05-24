package centrifuge

import (
	"context"
	"errors"
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
//   - Stream (history): ordered log of all changes for recovery. Clients use
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
// Epoch lifecycle:
//
// Each channel has an epoch — a unique string identifying the current generation of
// state and stream. Epoch changes when the channel's state is fully reset:
//
//   - Memory broker: epoch changes on server restart (state is lost)
//   - Redis broker: epoch changes when channel meta key expires or is deleted
//     (controlled by MetaTTL), or on Redis flush/restart
//   - PostgreSQL broker: epoch changes when channel is explicitly cleared via Clear(),
//     or when meta row expires (MetaTTL for Expiring retention)
//
// When epoch changes, clients receive ErrorUnrecoverablePosition and automatically
// re-sync from scratch — full state pagination followed by stream catch-up. No data
// is lost from the client's perspective; they simply get a fresh snapshot. Epoch does
// NOT change on normal Publish/Remove operations or on server rolling restarts when
// using Redis or PostgreSQL backends.
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

	// Subscribe registers this server node to receive pub/sub messages for the channels.
	// Called when a client subscribes to a map channel on this node.
	Subscribe(channels ...string) error

	// Unsubscribe removes this server node from receiving pub/sub messages for the channels.
	// Called when the last client on this node unsubscribes from the channel.
	Unsubscribe(channels ...string) error

	// Publish updates the state and broadcasts the change to subscribers.
	//
	// The operation:
	//  1. Updates the key in the state (subject to KeyMode conditions)
	//  2. Appends the publication to the stream (if StreamSize > 0)
	//  3. Broadcasts to pub/sub subscribers
	//
	// Returns MapUpdateResult with Suppressed=false when the state was changed.
	// When Suppressed=true (idempotency, version conflict, or KeyMode condition),
	// no stream entry is created and no pub/sub broadcast occurs.
	Publish(ctx context.Context, ch string, key string, opts MapPublishOptions) (MapUpdateResult, error)

	// Remove removes a key from the state and notifies subscribers.
	//
	// The operation:
	//  1. Removes the key from the state
	//  2. Appends a removal publication to the stream (if StreamSize > 0)
	//  3. Broadcasts removal to pub/sub subscribers
	//
	// Returns MapUpdateResult with Suppressed=false when the key was removed.
	// When Suppressed=true (key not found or idempotency), no stream entry or broadcast occurs.
	Remove(ctx context.Context, ch string, key string, opts MapRemoveOptions) (MapUpdateResult, error)

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

	// Data is the publication payload (usually JSON, but can be binary also).
	// Stored in state and published to stream/pub-sub.
	Data []byte
	// Tags are key-value metadata for filtering. Subscribers can filter by tags
	// to receive only relevant publications.
	Tags map[string]string
	// ClientInfo attaches client information to the publication.
	ClientInfo *ClientInfo
	// UseDelta enables delta compression. When true, only the difference from
	// the previous value is stored/sent (requires client support).
	UseDelta bool

	// Version enables per-key version-based ordering. If set, the publish only
	// succeeds if the key's current version is lower. Each key tracks its own
	// version independently. Requires a non-empty key and StreamSize > 0.
	Version uint64
	// VersionEpoch scopes the Version per key. Different epochs reset version
	// comparison. Empty string is valid — use when only incremental version matters.
	VersionEpoch string

	// score is the sort value when ordered=true (configured in MapChannelOptions).
	// Higher scores appear first.
	// Use for leaderboards, priority queues, and sorted collections.
	score int64

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

	// ExpectedPosition enables Compare-And-Swap semantics for removal.
	// If set, remove only succeeds if the key's current position (offset+epoch) matches.
	// Returns Suppressed=true with SuppressReasonPositionMismatch on mismatch.
	ExpectedPosition *StreamPosition

	// Tags to include in the removal publication. When not set, tags are automatically
	// read from the existing state entry (if available).
	Tags map[string]string
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
	// AllowCached allows reading from a read replica or cache instead of primary
	// storage. When false (default), reads always go to the primary for strong
	// consistency. Position checks must NOT set this flag.
	AllowCached bool
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
	//
	// Note: for unordered state in Redis, pagination uses HSCAN with COUNT hint.
	// Redis may return more entries than Limit on some pages (especially for small
	// hashes in listpack encoding). Callers should not rely on exact Limit enforcement
	// for unordered reads. ordered state uses ZRANGEBYSCORE with LIMIT — exact page sizes.
	Limit int

	// Key filters to a single entry by exact key match.
	// When set, returns at most one publication (or empty if key not found).
	// Cursor/Limit are ignored when Key is set.
	Key string

	// Asc requests ascending sort direction for ordered channels.
	// When false (default), ordered state is sorted by score DESC.
	// When true, ordered state is sorted by score ASC.
	// Ignored for unordered channels.
	Asc bool

	// AllowCached allows reading from cache instead of primary storage if
	// available. This option is always set by the subscriptions flow for
	// optimized delivery.
	// Application code should NOT set this if consistent results are important
	// (for example, when making CAS operations).
	// Broker can theoretically not provide this functionality.
	AllowCached bool
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

// MapCurrentEntry contains the current state of a map entry when an operation
// is suppressed due to a CAS position mismatch (SuppressReasonPositionMismatch).
// It lets the caller retry the CAS without an extra ReadState round-trip:
//
//	newData := compute(result.CurrentEntry.Data)
//	retry := MapPublishOptions{
//	    Data:             newData,
//	    ExpectedPosition: &StreamPosition{
//	        Offset: result.CurrentEntry.Offset,
//	        Epoch:  result.Position.Epoch,  // channel epoch lives on the outer Position
//	    },
//	}
//
// Set only when MapUpdateResult.SuppressReason == SuppressReasonPositionMismatch.
// Treat as read-only — implementations may point Data at internal storage.
type MapCurrentEntry struct {
	// Offset is the current offset of the entry — use as ExpectedPosition.Offset
	// when retrying CAS.
	Offset uint64
	// Data is the current value of the entry — typically used to compute the
	// next value in a read-modify-write CAS pattern.
	Data []byte
}

// MapUpdateResult contains the result of Publish operation.
type MapUpdateResult struct {
	// Position is the current stream position after the operation.
	Position StreamPosition
	// Suppressed is true when the operation did NOT change the state.
	// When Suppressed is true, no message is appended to the stream and no
	// publication is sent to PUB/SUB.
	Suppressed bool
	// SuppressReason explains why the operation was suppressed (empty when Suppressed is false).
	SuppressReason SuppressReason
	// CurrentEntry — set only when SuppressReason is SuppressReasonPositionMismatch.
	// See the MapCurrentEntry doc for usage in CAS-retry patterns.
	CurrentEntry *MapCurrentEntry
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

// ResolveAndValidateMapChannelOptions resolves and validates map channel options
// for a channel. Returns an error if the resolver is nil, Mode is not set,
// or the configuration is invalid. Auto-derives stream defaults for stream-backed modes.
func ResolveAndValidateMapChannelOptions(resolver func(channel string) MapChannelOptions, channel string) (MapChannelOptions, error) {
	if resolver == nil {
		return MapChannelOptions{}, errors.New("map channel options resolver not configured")
	}

	opts := resolver(channel)

	// Validate mode.
	if opts.Mode == 0 {
		return MapChannelOptions{}, errors.New("map channel not configured: set Mode")
	}
	if opts.Mode != MapModeEphemeral && opts.Mode != MapModeRecoverable && opts.Mode != MapModePersistent {
		return MapChannelOptions{}, errors.New("invalid Mode value")
	}

	// Validate expiry/TTL.
	if opts.Mode.HasExpiry() {
		if opts.KeyTTL == 0 {
			return MapChannelOptions{}, errors.New("KeyTTL required for mode with expiry")
		}
		if opts.KeyTTL < 0 {
			return MapChannelOptions{}, errors.New("KeyTTL must be positive")
		}
	}
	if !opts.Mode.HasExpiry() {
		if opts.KeyTTL != 0 {
			return MapChannelOptions{}, errors.New("KeyTTL must be 0 for persistent mode (entries don't expire)")
		}
	}

	// Validate stream fields.
	if opts.Mode.IsEphemeral() {
		if opts.StreamSize > 0 {
			return MapChannelOptions{}, errors.New("StreamSize requires recoverable or persistent mode")
		}
		if opts.StreamTTL > 0 {
			return MapChannelOptions{}, errors.New("StreamTTL requires recoverable or persistent mode")
		}
		if opts.MetaTTL > 0 {
			return MapChannelOptions{}, errors.New("MetaTTL requires recoverable or persistent mode")
		}
	}
	if opts.Mode.HasStream() {
		if opts.StreamSize < 0 {
			return MapChannelOptions{}, errors.New("StreamSize must be non-negative")
		}
		if opts.StreamTTL < 0 {
			return MapChannelOptions{}, errors.New("StreamTTL must be non-negative")
		}
		if opts.MetaTTL < 0 {
			return MapChannelOptions{}, errors.New("MetaTTL must be non-negative")
		}
		// Auto-derive defaults for stream-backed modes.
		if opts.StreamSize == 0 {
			opts.StreamSize = 100
		}
		if opts.StreamTTL == 0 {
			opts.StreamTTL = time.Minute
		}
		// Auto-derive MetaTTL.
		if opts.MetaTTL == 0 {
			if opts.Mode.HasExpiry() {
				opts.MetaTTL = opts.StreamTTL * 10
				// Ensure auto-derived MetaTTL is at least KeyTTL.
				if opts.KeyTTL > 0 && opts.MetaTTL < opts.KeyTTL {
					opts.MetaTTL = opts.KeyTTL
				}
			}
			// For Persistent, MetaTTL stays 0 (permanent).
		}
		// Validate MetaTTL >= StreamTTL when both explicit.
		if opts.MetaTTL > 0 && opts.MetaTTL < opts.StreamTTL {
			return MapChannelOptions{}, errors.New("MetaTTL must be >= StreamTTL (metadata must outlive stream)")
		}
		// Validate MetaTTL >= KeyTTL. When KeyTTL is 0 (permanent keys),
		// MetaTTL must also be 0 (permanent) — metadata can't expire
		// before keys that never expire.
		if opts.MetaTTL > 0 && opts.KeyTTL == 0 {
			return MapChannelOptions{}, errors.New("MetaTTL must be 0 (permanent) when KeyTTL is 0 (permanent keys)")
		}
		if opts.MetaTTL > 0 && opts.KeyTTL > 0 && opts.MetaTTL < opts.KeyTTL {
			return MapChannelOptions{}, errors.New("MetaTTL must be >= KeyTTL (metadata must outlive keys)")
		}
	}

	return opts, nil
}

// MakeOrderedCursor creates a cursor for ordered state: "score\x00key".
func MakeOrderedCursor(score, key string) string {
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
// For DESC (asc=false), sorted by (score DESC, key DESC), finds first entry where:
//   - score < cursorScore, OR score == cursorScore AND key < cursorKey
//
// For ASC (asc=true), sorted by (score ASC, key ASC), finds first entry where:
//   - score > cursorScore, OR score == cursorScore AND key > cursorKey
func findOrderedCursorPosition(sortedKeys []string, scores map[string]int64, cursor string, asc bool) int {
	cursorScoreStr, cursorKey := parseOrderedCursor(cursor)
	cursorScore, _ := strconv.ParseInt(cursorScoreStr, 10, 64)

	return sort.Search(len(sortedKeys), func(i int) bool {
		key := sortedKeys[i]
		score := scores[key]
		if score != cursorScore {
			if asc {
				return score > cursorScore
			}
			return score < cursorScore
		}
		if asc {
			return key > cursorKey
		}
		return key < cursorKey
	})
}
