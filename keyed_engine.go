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
	// When key already exists, Suppressed=true with SuppressReason="key_exists".
	KeyModeIfNew KeyMode = "if_new"

	// KeyModeIfExists only writes if the key ALREADY exists in the snapshot.
	// Use cases: heartbeats, update-only operations, presence renewal.
	// When key does not exist, Suppressed=true with SuppressReason="key_not_found".
	KeyModeIfExists KeyMode = "if_exists"
)

type KeyedEngine interface {
	// Subscribe subscribes server to channel and returns error if it fails.
	Subscribe(ch string) error
	// Unsubscribe unsubscribes server from channel and returns error if it fails.
	Unsubscribe(ch string) error

	// Publish allows sending data into a channel. It can optionally use stream
	// and keyed snapshots. Returns KeyedPublishResult with Suppressed=false when
	// the snapshot was actually changed. When Suppressed=true (due to idempotency,
	// out-of-order version, or key mode conditions), no message is appended to stream
	// and no publication is sent to PUB/SUB. SuppressReason contains the reason.
	Publish(ctx context.Context, ch string, key string, opts KeyedPublishOptions) (KeyedPublishResult, error)
	// Unpublish removes a key from keyed state snapshot and optionally sends remove
	// Publication to stream. Returns KeyedPublishResult with Suppressed=false when
	// the key was actually removed from the snapshot. When Suppressed=true (key not
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

	Data         []byte
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

	// RefreshTTLOnSuppress when true will refresh the TTL of existing key even when
	// publish is suppressed due to KeyMode (e.g., KeyModeIfNew when key exists).
	// This is useful for presence keep-alive without generating new stream entries.
	RefreshTTLOnSuppress bool
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
}

type KeyedRemoveOptions struct{}
