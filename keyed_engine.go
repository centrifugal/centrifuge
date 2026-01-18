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
	// Remove removes a key from keyed state snapshot and optionally sends remove
	// Publication to stream.
	Remove(ctx context.Context, ch string, key string, opts KeyedRemoveOptions) (StreamPosition, error)
	// ReadStream retrieves publications from stream for a channel, with cursor
	// pagination support.
	ReadStream(ctx context.Context, ch string, opts KeyedReadStreamOptions) ([]*Publication, StreamPosition, error)
	// ReadSnapshot retrieves a snapshot for a channel with per-entry revisions.
	// Each entry includes its revision so client can filter: entry.Revision <= snapshot_revision.
	// Returns entries, current stream position, next cursor for pagination, and error.
	// If opts.SnapshotRevision is provided and epoch changed, returns empty entries.
	// Cursor "" means end of iteration.
	ReadSnapshot(ctx context.Context, ch string, opts KeyedReadSnapshotOptions) ([]SnapshotEntry, StreamPosition, string, error)
	// Stats returns short stats of snapshot.
	Stats(ctx context.Context, ch string) (KeyedSnapshotStats, error)

	// NOT NEEDED FOR KeyedEngine - since may be built on core methods.
	//// AddMember adds a client to presence in a channel.
	//AddMember(ctx context.Context, ch string, info ClientInfo, opts EnginePresenceOptions) error
	//// RemoveMember removes a client from presence in a channel.
	//RemoveMember(ctx context.Context, ch string, info ClientInfo, opts EnginePresenceOptions) error
	//// ReadPresenceStream retrieves presence event stream (joins/leaves) for recovery.
	//// Returns Publications with Info field (ClientInfo) and Removed flag (true for leave, false for join).
	//ReadPresenceStream(ctx context.Context, ch string, opts KeyedReadStreamOptions) ([]*Publication, StreamPosition, error)
	//// ReadPresenceSnapshot retrieves presence snapshot with per-entry revisions for converged membership.
	//// Returns Publications with Key=ClientID, Info=ClientInfo, and Offset/Epoch for revision tracking.
	//// Presence doesn't use cursor pagination - returns all entries.
	//ReadPresenceSnapshot(ctx context.Context, ch string, opts KeyedReadSnapshotOptions) ([]*Publication, StreamPosition, error)
}

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

// KeyedRemoveOptions defines options for unpublishing (removing a key from keyed state).
type KeyedRemoveOptions struct {
	// Publish whether to publish removal notification to subscribers.
	Publish bool

	// StreamSize for appending removal event to stream (0 to disable).
	StreamSize int
	// StreamTTL for stream entries.
	StreamTTL time.Duration
	// StreamMetaTTL for stream metadata.
	StreamMetaTTL time.Duration
}

type KeyedSnapshotStats struct {
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

// SnapshotEntry represents a single entry in a snapshot with its revision.
// Client MUST filter entries where entry.Revision <= requested snapshot_revision.
type SnapshotEntry struct {
	Key      string         // Key of entry.
	Revision StreamPosition // StreamPosition of corresponding publication (offset, epoch).
	State    []byte         // State of key – in many cases the latest Publication.
}
