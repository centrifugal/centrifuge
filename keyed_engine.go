package centrifuge

import (
	"context"
	"time"
)

type KeyedEngine interface {
	// Subscribe subscribes server to channel and returns error if it fails.
	Subscribe(ch string) error
	// Unsubscribe unsubscribes server from channel and returns error if it fails.
	Unsubscribe(ch string) error

	// Publish allows sending data into a channel. It can optionally use stream
	// and keyed snapshots.
	Publish(ctx context.Context, ch string, key string, data []byte, opts KeyedPublishOptions) (StreamPosition, bool, error)
	// Unpublish removes a key from keyed state snapshot and optionally sends remove
	// Publication to stream.
	Unpublish(ctx context.Context, ch string, key string, opts KeyedUnpublishOptions) (StreamPosition, error)

	// ReadStream retrieves publications from stream for a channel, with cursor
	// pagination support.
	ReadStream(ctx context.Context, ch string, opts KeyedReadStreamOptions) ([]*Publication, StreamPosition, error)
	// ReadSnapshot retrieves a snapshot for a channel with per-entry revisions.
	// Returns publications (each with Key and Offset set), current stream position (with Epoch),
	// next cursor for pagination, and error.
	// Client must filter entries where pub.Offset <= snapshot_revision.Offset and compare epochs.
	// If opts.SnapshotRevision is provided and epoch changed, returns empty entries.
	// Cursor "" means end of iteration.
	ReadSnapshot(ctx context.Context, ch string, opts KeyedReadSnapshotOptions) ([]*Publication, StreamPosition, string, error)

	// Stats returns short stats of snapshot.
	Stats(ctx context.Context, ch string) (KeyedStats, error)

	// Remove channel data from storage: stream and snapshot.
	Remove(ctx context.Context, ch string, opts KeyedRemoveOptions) error
}

type KeyedRemoveOptions struct{}

// KeyedPublishOptions defines options for publishing.
type KeyedPublishOptions struct {
	Tags       map[string]string
	ClientInfo *ClientInfo

	IdempotencyKey      string
	IdempotentResultTTL time.Duration

	UseDelta     bool
	Version      uint64
	VersionEpoch string

	StreamSize    int
	StreamTTL     time.Duration
	StreamMetaTTL time.Duration

	KeyTTL time.Duration

	Ordered bool
	Score   int64
}

// EnginePresenceOptions defines options for presence operations.
type EnginePresenceOptions struct {
	Publish bool
}

// KeyedUnpublishOptions defines options for unpublishing (removing a key from keyed state).
type KeyedUnpublishOptions struct {
	// Publish whether to publish removal notification to subscribers.
	Publish bool

	// StreamSize for appending removal event to stream (0 to disable).
	StreamSize int
	// StreamTTL for stream entries.
	StreamTTL time.Duration
	// StreamMetaTTL for stream metadata.
	StreamMetaTTL time.Duration
}

type KeyedStats struct {
	NumKeys           int
	NumAggregatedKeys int
}

// KeyedReadStreamOptions define some fields to alter ReadStream method behavior.
type KeyedReadStreamOptions struct {
	// Filter for history publications.
	Filter HistoryFilter
	// MetaTTL allows overriding default (set in Config.HistoryMetaTTL) history
	// meta information expiration time.
	MetaTTL time.Duration
}

// KeyedReadSnapshotOptions defines options for reading a snapshot.
type KeyedReadSnapshotOptions struct {
	// SnapshotRevision is the revision client received during subscribe.
	// Server validates snapshot epoch matches this revision's epoch.
	// If epoch changed, server returns empty result forcing client to restart.
	SnapshotRevision *StreamPosition
	Ordered          bool
	Cursor           string
	Limit            int
	Offset           int
	SnapshotTTL      time.Duration
}
