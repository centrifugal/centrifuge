package centrifuge

import (
	"time"
)

// Config contains Node configuration options.
type Config struct {
	// Version of server – if set will be sent to a client on connection establishment
	// phase in reply to connect command from a client.
	Version string
	// Name is a unique name of the current server Node. Name used as human-readable
	// and meaningful node identifier. If not set then os.Hostname will be used.
	Name string
	// LogLevel is a log level. By default, nothing will be logged by Centrifuge.
	LogLevel LogLevel
	// LogHandler is a handler function Node will send logs to.
	LogHandler LogHandler
	// NodeInfoMetricsAggregateInterval sets interval for automatic metrics
	// aggregation. It's not reasonable to have it less than one second.
	// Zero value means 60 * time.Second.
	NodeInfoMetricsAggregateInterval time.Duration
	// ClientPresenceUpdateInterval sets an interval how often connected
	// clients update presence information.
	// Zero value means 27 * time.Second.
	ClientPresenceUpdateInterval time.Duration
	// ClientExpiredCloseDelay is an extra time given to client to refresh
	// its connection in the end of connection TTL. At moment only used for
	// a client-side refresh workflow.
	// Zero value means 25 * time.Second.
	ClientExpiredCloseDelay time.Duration
	// ClientExpiredSubCloseDelay is an extra time given to client to
	// refresh its expiring subscription in the end of subscription TTL.
	// At the moment only used for a client-side subscription refresh workflow.
	// Zero value means 25 * time.Second.
	ClientExpiredSubCloseDelay time.Duration
	// ClientStaleCloseDelay is a timeout after which connection will be
	// closed if still not authenticated (i.e. no valid connect command
	// received yet).
	// Zero value means 25 * time.Second.
	ClientStaleCloseDelay time.Duration
	// ClientChannelPositionCheckDelay defines minimal time from previous
	// client position check in channel. If client does not pass check it will
	// be disconnected with DisconnectInsufficientState.
	// Zero value means 40 * time.Second.
	ClientChannelPositionCheckDelay time.Duration
	// ClientQueueMaxSize is a maximum size of client's message queue in bytes.
	// After this queue size exceeded Centrifuge closes client's connection.
	// Zero value means 1048576 bytes (1MB).
	ClientQueueMaxSize int
	// ClientChannelLimit sets upper limit of client-side channels each client
	// can subscribe to. Client-side subscriptions attempts will get an ErrorLimitExceeded
	// in subscribe reply. Server-side subscriptions above limit will result into
	// DisconnectChannelLimit.
	// Zero value means 128.
	ClientChannelLimit int
	// UserConnectionLimit limits number of client connections to single Node
	// from user with the same ID. Zero value means unlimited. Anonymous users
	// can't be tracked.
	UserConnectionLimit int
	// ChannelMaxLength is the maximum length of a channel name. This is only checked
	// for client-side subscription requests.
	// Zero value means 255.
	ChannelMaxLength int
	// MetricsNamespace is a Prometheus metrics namespace to use for internal metrics.
	// If not set then the default namespace name `centrifuge` will be used.
	MetricsNamespace string
	// HistoryMaxPublicationLimit allows limiting the maximum number of publications to be
	// asked over client API history call. This is useful when you have large streams and
	// want to prevent a massive number of missed messages to be sent to a client when
	// calling history without any limit explicitly set. By default, no limit used.
	// This option does not affect Node.History method. See also RecoveryMaxPublicationLimit.
	HistoryMaxPublicationLimit int
	// RecoveryMaxPublicationLimit allows limiting the number of Publications that could be
	// restored during the automatic recovery process. See also HistoryMaxPublicationLimit.
	// By default, no limit used.
	RecoveryMaxPublicationLimit int
	// UseSingleFlight allows turning on mode where singleflight will be automatically used for
	// Node.History (including recovery) and Node.Presence/Node.PresenceStats calls.
	UseSingleFlight bool
}

const (
	// nodeInfoPublishInterval is an interval how often node must publish
	// node control message.
	nodeInfoPublishInterval = 3 * time.Second
	// nodeInfoCleanInterval is an interval in seconds, how often node must
	// clean information about other running nodes.
	nodeInfoCleanInterval = nodeInfoPublishInterval * 3
	// nodeInfoMaxDelay is an interval in seconds how long node info is
	// considered actual.
	nodeInfoMaxDelay = nodeInfoPublishInterval*2 + time.Second
)
