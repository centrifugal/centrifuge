package centrifuge

import (
	"context"
	"time"
)

// KeyMode controls conditional publishing based on key existence in the snapshot.
type KeyMode string

const (
	// KeyModeReplace always writes the key (default behavior, backward compatible).
	KeyModeReplace KeyMode = ""

	// KeyModeIfNew only writes if the key does NOT exist in the snapshot.
	// Use cases: lobby slots, distributed locks, first-claim resources.
	// When key already exists, Applied=false is returned.
	KeyModeIfNew KeyMode = "if_new"

	// KeyModeIfExists only writes if the key ALREADY exists in the snapshot.
	// Use cases: heartbeats, update-only operations, presence renewal.
	// When key does not exist, Applied=false is returned.
	KeyModeIfExists KeyMode = "if_exists"
)

type KeyedEngine interface {
	// Subscribe subscribes server to channel and returns error if it fails.
	Subscribe(ch string) error
	// Unsubscribe unsubscribes server from channel and returns error if it fails.
	Unsubscribe(ch string) error

	// Publish allows sending data into a channel. It can optionally use stream
	// and keyed snapshots. Returns KeyedPublishResult with Applied=true only when
	// the snapshot was actually changed. When Applied=false (due to idempotency
	// suppression or out-of-order version), no message is appended to stream
	// and no publication is sent to PUB/SUB.
	Publish(ctx context.Context, ch string, key string, data []byte, opts KeyedPublishOptions) (KeyedPublishResult, error)
	// Unpublish removes a key from keyed state snapshot and optionally sends remove
	// Publication to stream. Returns KeyedPublishResult with Applied=true only when
	// the key was actually removed from the snapshot. When Applied=false (key not
	// found), no message is appended to stream and no publication is sent.
	Unpublish(ctx context.Context, ch string, key string, opts KeyedUnpublishOptions) (KeyedPublishResult, error)

	// ReadStream retrieves publications from stream for a channel, with cursor
	// pagination support.
	ReadStream(ctx context.Context, ch string, opts KeyedReadStreamOptions) ([]*Publication, StreamPosition, error)
	// ReadSnapshot retrieves a snapshot for a channel – list of saved Publication (usually latest per key).
	// Returns publications (each with Key and Offset set), current stream position (top offset/epoch),
	// next cursor for pagination, and error.
	// Client must filter entries where pub.Offset >= first_page_revision.Offset.
	// If opts.Revision is provided and epoch changed, must return ErrorUnrecoverablePosition.
	// Cursor "" means end of iteration.
	ReadSnapshot(ctx context.Context, ch string, opts KeyedReadSnapshotOptions) ([]*Publication, StreamPosition, string, error)

	// Stats returns short stats of snapshot.
	Stats(ctx context.Context, ch string) (KeyedStats, error)

	// Remove channel data from storage: stream and snapshot.
	Remove(ctx context.Context, ch string, opts KeyedRemoveOptions) error
}

// KeyedPublishOptions defines options for publishing.
type KeyedPublishOptions struct {
	// Publish is whether to publish to subscribers.
	Publish bool

	// IdempotencyKey for suppressing duplicates in IdempotentResultTTL window.
	IdempotencyKey      string
	IdempotentResultTTL time.Duration

	// StreamSize for appending removal event to stream (0 to disable).
	StreamSize int
	// StreamTTL for stream entries.
	StreamTTL time.Duration
	// StreamMetaTTL for stream metadata.
	StreamMetaTTL time.Duration

	Tags         map[string]string
	ClientInfo   *ClientInfo
	UseDelta     bool
	Version      uint64
	VersionEpoch string
	KeyTTL       time.Duration
	Ordered      bool
	Score        int64

	// KeyMode controls conditional publishing based on key existence.
	// Default (empty) always writes. See KeyModeIfNew and KeyModeIfExists.
	KeyMode KeyMode

	// AggregationKey is an identifier for grouping keys by a common attribute.
	// For example, "user_id" for presence to track unique users across connections.
	// When set, enables per-aggregation counting in the snapshot.
	AggregationKey string
	// AggregationValue is the value to aggregate by (e.g., actual user ID).
	// Used together with AggregationKey for counting unique values.
	AggregationValue string
}

// KeyedUnpublishOptions defines options for unpublishing (removing a key from keyed state).
type KeyedUnpublishOptions struct {
	// Publish whether to publish removal to subscribers.
	Publish bool

	// IdempotencyKey for suppressing duplicates in IdempotentResultTTL window.
	IdempotencyKey      string
	IdempotentResultTTL time.Duration

	// StreamSize for appending removal event to stream (0 to disable).
	StreamSize int
	// StreamTTL for stream entries.
	StreamTTL time.Duration
	// StreamMetaTTL for stream metadata.
	StreamMetaTTL time.Duration

	// AggregationKey is an identifier for grouping keys by a common attribute.
	// Optional for Unpublish - if not provided, the aggregation value is auto-discovered
	// from the stored mapping (set during Publish). Providing it is an optimization
	// that skips the lookup.
	AggregationKey string
	// AggregationValue is the value to aggregate by.
	// Optional for Unpublish - auto-discovered from stored mapping if not provided.
	AggregationValue string
}

// StreamFilter allows filtering stream according to fields set.
type StreamFilter struct {
	// Since if set is used to extract publications from stream since provided StreamPosition.
	Since *StreamPosition
	// Limit number of publications to return.
	// -1 means no limit - i.e. return all publications currently in stream.
	// 0 means that caller only interested in current stream top position so
	// Broker should not return any publications.
	Limit int
	// Reverse direction.
	Reverse bool
}

// KeyedReadStreamOptions define some fields to alter ReadStream method behavior.
type KeyedReadStreamOptions struct {
	// Filter for history publications.
	Filter StreamFilter
	// MetaTTL allows overriding default (set in Config.HistoryMetaTTL) history
	// meta information expiration time.
	MetaTTL time.Duration
}

// KeyedReadSnapshotOptions defines options for reading a snapshot.
type KeyedReadSnapshotOptions struct {
	// Revision is the revision client received during subscribe.
	// Server validates snapshot epoch matches this revision's epoch.
	// If epoch changed, server returns error.
	Revision    *StreamPosition
	Ordered     bool
	Cursor      string
	Limit       int
	Offset      int
	SnapshotTTL time.Duration
}

// KeyedStats provide current statistics for channel in KeyedEngine.
type KeyedStats struct {
	NumKeys            int
	NumAggregationKeys int
}

// KeyedPublishResult contains the result of Publish or Unpublish operation.
type KeyedPublishResult struct {
	// Position is the current stream position after the operation.
	Position StreamPosition
	// Applied is true only when the operation actually changed the snapshot.
	// It is false when:
	// - Suppressed due to idempotency key (duplicate detected)
	// - Skipped due to out-of-order version
	// - KeyMode condition not met (KeyModeIfNew when key exists, KeyModeIfExists when key absent)
	// - Key not found (for Unpublish)
	// When Applied is false, no message is appended to the stream and no
	// publication is sent to PUB/SUB.
	Applied bool
}

type KeyedRemoveOptions struct{}
