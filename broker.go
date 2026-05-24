package centrifuge

import (
	"context"
	"time"
)

// Publication is a data sent to a channel.
type Publication struct {
	// Offset is an incremental position number inside a history stream.
	// Zero value means that channel does not maintain Publication stream.
	Offset uint64
	// Epoch is a multi-purpose channel-level epoch string. For stream
	// subscriptions it pairs with Offset for stream-position tracking
	// (history loss / stream recreation detection). For shared-poll
	// versioned subscriptions it carries the publisher's per-channel
	// epoch — a change in epoch resets per-key versions and unsubscribes
	// current subscribers with insufficient-state code so they re-track
	// from version 0 on resubscribe.
	Epoch string
	// Data to be published to a channel (to be delivered to subscribers).
	Data []byte
	// Info is optional information about client connection published this data.
	Info *ClientInfo
	// Tags contains a map with custom key-values attached to a Publication. Tags map
	// will be delivered to a client.
	Tags map[string]string
	// Optional time of publication as Unix timestamp milliseconds. At this point
	// we use it for calculating PUB/SUB time lag, it's not exposed to the client
	// protocol.
	Time int64
	// Channel is only set when subscription channel does not match channel in Publication.
	// This is a case for wildcard subscriptions. Client SDK then should use this channel
	// in PublicationContext.
	Channel string
	// Key is used for map subscriptions (state and presence/membership).
	// For presence: Key = ClientID.
	// For map state: Key = arbitrary string key.
	Key string
	// Removed indicates this is a remove/leave event (true) vs add/join event (false).
	// Used in map subscriptions to distinguish event types.
	Removed bool
	// Score is used for ordered map subscriptions (leaderboards, priority queues).
	Score int64
	// Version of the publication. Used by SharedPoll for stale-check.
	Version uint64
}

// ClientInfo contains information about client connection.
type ClientInfo struct {
	// ClientID is a client unique id.
	ClientID string
	// UserID is an ID of authenticated user. Zero value means anonymous user.
	UserID string
	// ConnInfo is additional information about connection.
	ConnInfo []byte
	// ChanInfo is additional information about connection in context of
	// channel subscription.
	ChanInfo []byte
}

// BrokerEventHandler can handle messages received from PUB/SUB system.
type BrokerEventHandler interface {
	// HandlePublication to handle received Publications.
	HandlePublication(ch string, pub *Publication, sp StreamPosition, useDelta bool, prevPub *Publication) error
	// HandleJoin to handle received Join messages.
	HandleJoin(ch string, info *ClientInfo) error
	// HandleLeave to handle received Leave messages.
	HandleLeave(ch string, info *ClientInfo) error
}

// HistoryFilter allows filtering history according to fields set.
type HistoryFilter struct {
	// Since used to extract publications from stream since provided StreamPosition.
	Since *StreamPosition
	// Limit number of publications to return.
	// -1 means no limit - i.e. return all publications currently in stream.
	// 0 means that caller only interested in current stream top position so
	// Broker should not return any publications.
	Limit int
	// Reverse direction.
	Reverse bool
}

// HistoryOptions define some fields to alter History method behaviour.
type HistoryOptions struct {
	// Filter for history publications.
	Filter HistoryFilter
	// MetaTTL allows overriding default (set in Config.HistoryMetaTTL) history
	// meta information expiration time.
	MetaTTL time.Duration
}

// StreamPosition contains fields to describe position in stream.
// At moment this is used for automatic recovery mechanics. More info about stream
// recovery in Centrifugo docs: https://centrifugal.dev/docs/server/history_and_recovery.
type StreamPosition struct {
	// Offset defines publication incremental offset inside a stream.
	Offset uint64
	// Epoch allows handling situations when storage
	// lost stream entirely for some reason (expired or lost after restart) and we
	// want to track this fact to prevent successful recovery from another stream.
	// I.e. for example we have a stream [1, 2, 3], then it's lost and new stream
	// contains [1, 2, 3, 4], client that recovers from position 3 will only receive
	// publication 4 missing 1, 2, 3 from new stream. With epoch, we can tell client
	// that correct recovery is not possible.
	Epoch string
}

// Closer is an interface that Broker and PresenceManager can optionally implement
// if they need to close any resources on Centrifuge Node graceful shutdown.
type Closer interface {
	// Close when called should clean up used resources.
	Close(ctx context.Context) error
}

// PublishOptions define some fields to alter behaviour of Publish operation.
type PublishOptions struct {
	// HistoryTTL sets history ttl to expire inactive history streams.
	// Current Broker implementations only work with seconds resolution for TTL.
	HistoryTTL time.Duration
	// HistorySize sets history size limit to prevent infinite stream growth.
	HistorySize int
	// HistoryMetaTTL allows overriding default (set in Config.HistoryMetaTTL)
	// history meta information expiration time upon publish.
	HistoryMetaTTL time.Duration
	// ClientInfo to include into Publication. By default, no ClientInfo will be appended.
	ClientInfo *ClientInfo
	// Tags to set Publication.Tags.
	Tags map[string]string
	// IdempotencyKey is an optional key for idempotent publish. Broker implementation
	// may cache these keys for some time to prevent duplicate publications. In this case
	// the returned result is the same as from the previous publication with the same key.
	IdempotencyKey string
	// IdempotentResultTTL sets the time of expiration for results of idempotent publications
	// (publications with idempotency key provided). Memory and Redis brokers implement this TTL
	// with second precision, so don't set something less than one second here. By default,
	// Centrifuge uses 5 minutes as idempotent result TTL.
	IdempotentResultTTL time.Duration
	// UseDelta enables using delta encoding for the publication.
	UseDelta bool
	// Version of Publication. This is a tip to Centrifuge to skip non-actual
	// publications. Mostly useful for cases when Publication contains the entire
	// state. Version only used when history is configured.
	Version uint64
	// VersionEpoch is a string that is used to identify the epoch of version of the
	// publication. Use it if version may be reused in the future. For example, if
	// version comes from in-memory system which can lose data, or due to eviction, etc.
	VersionEpoch string
	// Key associates the publication with a specific key within the channel.
	// For stream subscriptions: enables per-key debouncing via DebouncingBroker
	// and is delivered to subscribers on the Publication for client-side use.
	// For map subscriptions: plays central role – state is compacted by key,
	// delta compression per key, channel batching latest publication is per key.
	Key string

	// Removed indicates this is a removal event in map subscriptions.
	Removed bool
	// score is used for ordered map subscriptions (leaderboards, priority queues).
	score int64
	// Offset is the stream offset for map publications. When set, this offset
	// is used instead of broker-assigned offset (used for map broker fan-out).
	Offset uint64
	// Epoch is the stream epoch for map publications. When set along with Offset,
	// the broker will include position information in the message for fan-out.
	Epoch string
	// PrevData carries previous publication data for delta computation.
	// Used when map broker fans out through a standard Broker (e.g. Redis).
	// The receiving node uses this as prevPub in HandlePublication.
	PrevData []byte
}

// Broker is responsible for PUB/SUB mechanics.
type Broker interface {
	// RegisterBrokerEventHandler called once on start when Broker already set to Node. At
	// this moment node is ready to process broker events.
	RegisterBrokerEventHandler(BrokerEventHandler) error

	// Subscribe node on channels to listen all messages coming from them.
	Subscribe(channels ...string) error
	// Unsubscribe node from channels to stop listening messages from them.
	Unsubscribe(channels ...string) error

	// Publish allows sending data into channel. Data should be
	// delivered to all clients subscribed to this channel at moment on any
	// Centrifuge node (with at most once delivery guarantee).
	//
	// Broker can optionally maintain publication history inside channel according
	// to PublishOptions provided. See History method for rules that should be implemented
	// for accessing publications from history stream.
	//
	// Saving message to a history stream and publish to PUB/SUB should be an atomic
	// operation per channel. If this is not true – then publication to one channel
	// must be serialized on the caller side, i.e. publish requests must be issued one
	// after another. Otherwise, the order of publications and stable behaviour of
	// subscribers with positioning/recovery enabled can't be guaranteed.
	//
	// PublishResult.StreamPosition describes stream epoch and offset assigned to
	// the publication. For channels without history this should be zero value.
	// PublishResult.Suppressed and PublishResult.SuppressReason indicate whether
	// publication was suppressed and why (e.g. idempotency deduplication, version skip).
	Publish(ch string, data []byte, opts PublishOptions) (PublishResult, error)
	// PublishJoin publishes Join Push message into channel.
	PublishJoin(ch string, info *ClientInfo) error
	// PublishLeave publishes Leave Push message into channel.
	PublishLeave(ch string, info *ClientInfo) error

	// History used to extract Publications from history stream.
	// Publications returned according to HistoryFilter which allows to set several
	// filtering options. StreamPosition returned describes current history stream
	// top offset and epoch.
	History(ch string, opts HistoryOptions) ([]*Publication, StreamPosition, error)
	// RemoveHistory removes history from channel. This is in general not
	// needed as history expires automatically (based on history_lifetime)
	// but sometimes can be useful for application logic.
	RemoveHistory(ch string) error
}

// reliableDeliverer is an optional interface a Broker or MapBroker may implement
// to declare that it guarantees no-gaps delivery of publications to local
// subscribers. When a broker reports true, the node skips periodic position
// sync requests for channels served by this broker, eliminating needless
// storage load for brokers that can't lose messages between worker and client.
//
// Examples of reliable delivery:
//   - PostgreSQL broker polling its own outbox table without fan-out: the
//     shard lock guarantees BIGSERIAL order matches commit order and the
//     worker reads rows in id order, so no gaps are possible.
//
// Examples where delivery is NOT reliable (interface should not be implemented
// or should return false):
//   - Redis/Nats brokers using PUB/SUB: subscribers can drop messages under
//     buffer pressure or brief network blips without the node noticing.
//   - PostgreSQL broker with broker fan-out enabled: the fan-out leg through
//     Redis/Nats reintroduces the same at-most-once semantics.
type reliableDeliverer interface {
	ReliableDelivery() bool
}
