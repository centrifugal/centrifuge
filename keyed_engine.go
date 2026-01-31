package centrifuge

import (
	"context"
	"time"
)

// KeyedEngine Overview
//
// KeyedEngine manages synchronized state for keyed subscriptions. It provides:
//
//   - Snapshot storage: Key-value state where each key maps to a publication.
//     Clients receive the full snapshot when subscribing.
//
//   - Stream (history): Ordered log of all changes for recovery. Clients use
//     this to catch up on changes that occurred during subscription setup or
//     after temporary disconnection.
//
//   - Pub/Sub integration: Real-time delivery of changes to subscribed clients.
//
// Data flow for publish operations:
//
//	Publish() -> Update snapshot -> Append to stream -> Broadcast via pub/sub
//
// Client subscription phases:
//
//  1. Snapshot phase: Client calls ReadSnapshot() repeatedly to paginate through
//     the current state. Each response includes a stream position for consistency.
//
//  2. Stream phase (optional): Client calls ReadStream() to catch up on changes
//     that occurred since the snapshot. May require multiple calls for pagination.
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
// The engine supports several advanced features:
//
//   - Idempotency: Duplicate detection via IdempotencyKey to safely retry operations.
//   - Versioning: Optimistic concurrency control via Version/VersionEpoch fields.
//   - Conditional writes: KeyMode (IfNew, IfExists) for atomic check-and-set operations.
//   - TTL: Automatic key expiration with KeyTTL for presence and ephemeral data.
//   - Ordering: Optional score-based ordering for leaderboards and sorted collections.
//
// Implementations must ensure:
//
//   - Snapshot and stream consistency (same epoch, contiguous offsets)
//   - Atomic snapshot updates (no partial writes visible to readers)
//   - Stream position tracking for recovery validation

// KeyMode controls conditional publishing based on key existence in the snapshot.
type KeyMode string

const (
	// KeyModeReplace always writes the key (default behavior, backward compatible).
	KeyModeReplace KeyMode = ""

	// KeyModeIfNew only writes if the key does NOT exist in the snapshot.
	// Use cases: lobby slots, distributed locks, first-claim resources.
	// When key already exists, Suppressed=true with SuppressReason="key_exists".
	KeyModeIfNew KeyMode = "if_new"

	// KeyModeIfExists only writes if the key ALREADY exists in the snapshot.
	// Use cases: heartbeats, update-only operations, presence renewal.
	// When key does not exist, Suppressed=true with SuppressReason="key_not_found".
	KeyModeIfExists KeyMode = "if_exists"
)

// KeyedEngine is the interface for managing keyed subscription state.
// See "KeyedEngine Overview" above for detailed documentation.
type KeyedEngine interface {
	// Subscribe registers this server node to receive pub/sub messages for the channel.
	// Called when a client subscribes to a keyed channel on this node.
	Subscribe(ch string) error

	// Unsubscribe removes this server node from receiving pub/sub messages for the channel.
	// Called when the last client on this node unsubscribes from the channel.
	Unsubscribe(ch string) error

	// Publish updates the snapshot and broadcasts the change to subscribers.
	//
	// The operation:
	//  1. Updates the key in the snapshot (subject to KeyMode conditions)
	//  2. Appends the publication to the stream (if StreamSize > 0)
	//  3. Broadcasts to pub/sub subscribers (if Publish option is true)
	//
	// Returns KeyedPublishResult with Suppressed=false when the snapshot was changed.
	// When Suppressed=true (idempotency, version conflict, or KeyMode condition),
	// no stream entry is created and no pub/sub broadcast occurs.
	Publish(ctx context.Context, ch string, key string, opts KeyedPublishOptions) (KeyedPublishResult, error)

	// Unpublish removes a key from the snapshot and notifies subscribers.
	//
	// The operation:
	//  1. Removes the key from the snapshot
	//  2. Appends a removal publication to the stream (if StreamSize > 0)
	//  3. Broadcasts removal to pub/sub subscribers (if Publish option is true)
	//
	// Returns KeyedPublishResult with Suppressed=false when the key was removed.
	// When Suppressed=true (key not found), no stream entry or broadcast occurs.
	Unpublish(ctx context.Context, ch string, key string, opts KeyedUnpublishOptions) (KeyedPublishResult, error)

	// ReadStream retrieves publications from the channel's history stream.
	//
	// Used by clients to catch up on changes after receiving the snapshot.
	// Supports filtering by position (Since) and pagination (Limit).
	// Returns publications, current stream position, and error.
	//
	// Special Limit values:
	//  - Limit = 0: Only return current stream position (no publications)
	//  - Limit = -1: Return all publications (no limit)
	ReadStream(ctx context.Context, ch string, opts KeyedReadStreamOptions) ([]*Publication, StreamPosition, error)

	// ReadSnapshot retrieves the current key-value state for a channel.
	//
	// Returns all keys in the snapshot as publications (each with Key field set).
	// Supports cursor-based pagination for large snapshots.
	//
	// If opts.Revision is provided, validates that the epoch hasn't changed.
	// Returns ErrorUnrecoverablePosition if epoch changed (client must restart).
	//
	// Return values: publications, stream position, next cursor ("" if done), error.
	ReadSnapshot(ctx context.Context, ch string, opts KeyedReadSnapshotOptions) ([]*Publication, StreamPosition, string, error)

	// Stats returns statistics about the channel's snapshot.
	Stats(ctx context.Context, ch string) (KeyedStats, error)

	// Remove deletes all data for a channel (snapshot and stream).
	// Use for cleanup when a channel is no longer needed.
	Remove(ctx context.Context, ch string, opts KeyedRemoveOptions) error
}

// KeyedPublishOptions defines options for publishing to a keyed channel.
type KeyedPublishOptions struct {
	// Publish controls whether to broadcast this update to pub/sub subscribers.
	// Set to true for real-time delivery, false for silent snapshot updates.
	Publish bool

	// IdempotencyKey enables duplicate detection. If the same key is seen within
	// IdempotentResultTTL, the operation is suppressed (Suppressed=true).
	// Use for safe client retries without duplicating state changes.
	IdempotencyKey      string
	IdempotentResultTTL time.Duration

	// StreamSize sets the maximum number of entries in the stream (history).
	// Set to 0 to disable stream (no history, snapshot-only mode).
	// Older entries are evicted when the limit is reached.
	StreamSize int
	// StreamTTL sets how long stream entries are retained.
	StreamTTL time.Duration
	// StreamMetaTTL sets how long stream metadata (epoch, top offset) is retained
	// after all entries expire. Allows position validation even with empty stream.
	StreamMetaTTL time.Duration

	// Data is the publication payload (usually JSON, but can be binary also).
	// Stored in snapshot and published to stream/pub-sub (unless StreamData is set).
	Data []byte
	// StreamData, if set, overrides Data for stream and pub-sub publication.
	// Use when you want to store full state in snapshot but publish incremental updates.
	// Example: snapshot stores {"count":105}, stream publishes {"delta":5}.
	StreamData []byte
	// Tags are key-value metadata for filtering. Subscribers can filter by tags
	// to receive only relevant publications.
	Tags map[string]string
	// ClientInfo attaches client information to the publication.
	ClientInfo *ClientInfo
	// UseDelta enables delta compression. When true, only the difference from
	// the previous value is stored/sent (requires client support).
	UseDelta bool

	// Version enables optimistic concurrency control. If set, the publish only
	// succeeds if the current version is lower. Use with VersionEpoch for
	// cross-epoch version tracking.
	Version uint64
	// VersionEpoch scopes the Version. Different epochs reset version comparison.
	VersionEpoch string

	// KeyTTL sets automatic expiration for this key. After TTL expires, the key
	// is removed from snapshot and a removal event is published to stream.
	// Use for presence, ephemeral cursors, and auto-cleanup scenarios.
	KeyTTL time.Duration

	// Ordered enables score-based ordering in the snapshot. When true, entries
	// are returned sorted by Score (descending) instead of insertion order.
	Ordered bool
	// Score is the sort value when Ordered=true. Higher scores appear first.
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
	// Use with ReadSnapshot Key filter for atomic read-modify-write.
	ExpectedPosition *StreamPosition
}

// KeyedUnpublishOptions defines options for removing a key from a keyed channel.
type KeyedUnpublishOptions struct {
	// Publish controls whether to broadcast the removal to pub/sub subscribers.
	// Set to true so clients receive real-time notification of key removal.
	Publish bool

	// IdempotencyKey enables duplicate detection for removal operations.
	IdempotencyKey      string
	IdempotentResultTTL time.Duration

	// StreamSize sets the maximum stream size. The removal event is appended
	// to the stream so clients can recover the removal during catch-up.
	// Set to 0 to skip stream (removal won't be recoverable).
	StreamSize int
	// StreamTTL sets how long the removal entry is retained in stream.
	StreamTTL time.Duration
	// StreamMetaTTL sets how long stream metadata is retained.
	StreamMetaTTL time.Duration
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

// KeyedReadStreamOptions defines options for reading a channel's history stream.
type KeyedReadStreamOptions struct {
	// Filter controls which publications are returned (position, limit, direction).
	Filter StreamFilter

	// MetaTTL extends the stream metadata TTL when reading.
	// Overrides the default Config.HistoryMetaTTL for this operation.
	MetaTTL time.Duration
}

// KeyedReadSnapshotOptions defines options for reading a channel's snapshot.
type KeyedReadSnapshotOptions struct {
	// Revision is the stream position from a previous snapshot read.
	// If provided, the server validates that the epoch hasn't changed.
	// Returns ErrorUnrecoverablePosition if epoch changed (snapshot invalidated).
	Revision *StreamPosition

	// Ordered returns entries sorted by Score (descending) instead of by key.
	// Only meaningful for channels using ordered publishing (Ordered=true in publish).
	Ordered bool

	// Cursor is the pagination cursor from a previous ReadSnapshot call.
	// Empty string starts from the beginning.
	Cursor string

	// Limit is the maximum number of entries to return per page.
	Limit int

	// Offset skips entries in the result (alternative to cursor pagination).
	Offset int

	// SnapshotTTL extends the snapshot metadata TTL when reading.
	SnapshotTTL time.Duration

	// Key filters to a single entry by exact key match.
	// When set, returns at most one publication (or empty if key not found).
	// Cursor/Limit are ignored when Key is set.
	Key string
}

// KeyedStats provides statistics about a channel's snapshot.
type KeyedStats struct {
	// NumKeys is the current number of keys in the snapshot.
	NumKeys int
}

// SuppressReason explains why a Publish or Unpublish operation was suppressed.
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
	// or Unpublish was called on a non-existent key.
	SuppressReasonKeyNotFound SuppressReason = "key_not_found"

	// SuppressReasonPositionMismatch indicates CAS failed - key's position changed.
	SuppressReasonPositionMismatch SuppressReason = "position_mismatch"
)

// KeyedPublishResult contains the result of Publish or Unpublish operation.
type KeyedPublishResult struct {
	// Position is the current stream position after the operation.
	Position StreamPosition
	// Suppressed is true when the operation did NOT change the snapshot.
	// When Suppressed is true, no message is appended to the stream and no
	// publication is sent to PUB/SUB.
	Suppressed bool
	// SuppressReason explains why the operation was suppressed (empty when Suppressed is false).
	SuppressReason SuppressReason
	// CurrentPublication contains the current state when suppressed due to CAS mismatch.
	// Allows immediate retry without extra ReadSnapshot call.
	// Only set when SuppressReason is SuppressReasonPositionMismatch.
	CurrentPublication *Publication
}

// KeyedRemoveOptions defines options for removing all channel data.
// Currently empty but reserved for future options (e.g., selective removal).
type KeyedRemoveOptions struct{}
