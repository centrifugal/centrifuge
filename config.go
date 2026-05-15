package centrifuge

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// MapMode controls the synchronization, recovery, and data lifecycle behavior of a map channel.
type MapMode int

const (
	// MapModeEphemeral – PUB/SUB only, no stream. Missed updates during disconnect
	// are lost — reconnect triggers full state resync. Entries expire after KeyTTL.
	MapModeEphemeral MapMode = iota + 1
	// MapModeRecoverable – stream-backed with offset-based recovery. Entries expire after
	// KeyTTL. TTL removal events are logged to the stream (recoverable delivery).
	MapModeRecoverable
	// MapModePersistent – stream-backed with offset-based recovery. Entries live
	// forever (until explicitly removed). No TTL, no expiry.
	MapModePersistent
)

// IsEphemeral returns true for MapModeEphemeral.
func (m MapMode) IsEphemeral() bool { return m == MapModeEphemeral }

// HasStream returns true for modes that maintain a recovery stream (Recoverable, Persistent).
func (m MapMode) HasStream() bool { return m == MapModeRecoverable || m == MapModePersistent }

// HasExpiry returns true for modes where entries expire via TTL (Ephemeral, Recoverable).
func (m MapMode) HasExpiry() bool { return m == MapModeEphemeral || m == MapModeRecoverable }

// MapChannelOptions contains configuration for map channels. Every map channel
// must have Mode explicitly set — zero value is an error.
//
// Note on Ephemeral mode: TTL removal events are delivered via pub/sub but
// NOT logged to a stream (there is no stream). If a client misses the removal
// pub/sub message (e.g., during a brief disconnect), it will retain stale entries
// until the next full state resync. This is expected behavior for ephemeral use
// cases like cursor tracking — use Recoverable mode for data requiring consistency.
type MapChannelOptions struct {
	// Mode controls synchronization, recovery, and data lifecycle.
	// Required. Zero value = not configured = error.
	Mode MapMode
	// KeyTTL sets automatic expiration for entries in this channel.
	// Required when Mode.HasExpiry() (must be > 0).
	// Must be 0 when Mode is Persistent.
	KeyTTL time.Duration
	// StreamSize sets the maximum number of entries in the recovery stream.
	// Zero = auto-derived (100) for Recoverable/Persistent. Must be 0 for Ephemeral.
	StreamSize int
	// StreamTTL sets how long stream entries are retained.
	// Zero = auto-derived (1 minute) for Recoverable/Persistent. Must be 0 for Ephemeral.
	StreamTTL time.Duration
	// MetaTTL sets how long stream metadata (epoch, offset) is retained.
	// Zero = auto-derived: Recoverable: StreamTTL*10, Persistent: permanent (no expiry).
	// Must be 0 for Ephemeral.
	MetaTTL time.Duration
	// DefaultPageSize sets the default number of items per page when
	// the client does not specify a page size. Zero means default (100).
	DefaultPageSize int
	// MinPageSize sets the minimum number of items per page in map
	// subscription pagination requests. This prevents excessive round trips when
	// clients send very small page sizes. Zero means default (100).
	MinPageSize int
	// MaxPageSize sets the maximum number of items a client can request
	// per page in map subscription pagination requests. Zero means default (1000).
	MaxPageSize int
	// LiveTransitionMaxPublicationLimit sets the maximum number of stream publications
	// to recover during map subscription live transition. If limit is reached,
	// ErrorUnrecoverablePosition is returned. Zero means MaxPageSize.
	LiveTransitionMaxPublicationLimit int
	// SubscribeCatchUpTimeout sets the maximum time a client can spend paginating
	// through state and stream phases before going live. If exceeded, the client is
	// disconnected with DisconnectSlow. Zero means 5 seconds (default). Negative
	// means no timeout.
	SubscribeCatchUpTimeout time.Duration
	// ordered enables score-based ordering in the state. When true, entries
	// are returned sorted by Score (descending).
	ordered bool
}

// Config contains Node configuration options.
type Config struct {
	// Version of server – if set will be sent to a client on connection
	// establishment phase in reply to connect command from a client.
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
	// ClientConnectIncludeServerTime tells Centrifuge to append `time` field to Connect result of client protocol.
	// This field contains Unix timestamp in milliseconds and represents current server time. By default, server time
	// is not included.
	ClientConnectIncludeServerTime bool
	// ClientPresenceUpdateInterval sets an interval how often connected
	// clients update presence information.
	// Zero value means 25 * time.Second.
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
	// Zero value means 15 * time.Second.
	ClientStaleCloseDelay time.Duration
	// ClientChannelPositionCheckDelay defines minimal time from previous
	// client position check in channel. If client does not pass check it
	// will be disconnected with DisconnectInsufficientState.
	// Zero value means 40 * time.Second.
	ClientChannelPositionCheckDelay time.Duration
	// Maximum allowed time lag for publications for subscribers with positioning on.
	// When exceeded we mark connection with insufficient state. By default, not used - i.e.
	// Centrifuge does not take lag into account for positioning.
	// See also pub_sub_time_lag_seconds as a helpful metric.
	ClientChannelPositionMaxTimeLag time.Duration
	// ClientQueueMaxSize is a maximum size of client's message queue in
	// bytes. After this queue size exceeded Centrifuge closes client's connection.
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
	// UseSingleFlight allows turning on mode where singleflight will be automatically used
	// for Node.History (including recovery), Node.Presence/Node.PresenceStats,
	// and Node.MapStateRead/Node.MapStreamRead/Node.MapStats calls.
	UseSingleFlight bool
	// HistoryMetaTTL sets a time of stream meta key expiration in Redis. Stream
	// meta key is a Redis HASH that contains top offset in channel and epoch value.
	// In some cases – when channels created for а short time and then
	// not used anymore – created stream meta keys can stay in memory while
	// not actually useful. For example, you can have a personal user channel but
	// after using your app for a while user left it forever. In long-term
	// perspective this can be an unwanted memory leak. Setting a reasonable
	// value to this option (usually much bigger than history retention period)
	// can help. In this case unused channel stream metadata will eventually expire.
	//
	// Keep this value much larger than history stream TTL used when publishing.
	// When zero Centrifuge uses default 30 days which we believe is more than enough
	// for most use cases.
	HistoryMetaTTL time.Duration
	// Metrics is MetricsConfig to configure Prometheus metrics provided by Centrifuge.
	Metrics MetricsConfig
	// GetChannelMediumOptions is a way to provide ChannelMediumOptions for specific channel.
	// This function is called each time new channel appears on the Node.
	// See the doc comment for ChannelMediumOptions for more details about channel medium concept.
	GetChannelMediumOptions func(channel string) ChannelMediumOptions
	// GetBroker when set allows returning a custom Broker to use for a specific channel. If not set
	// then the default Node's Broker is always used for all channels. Also, Node's default Broker is
	// always used for control channels. It's the responsibility of an application to call Broker.Run
	// method of all brokers except the default one (called automatically inside Node.Run). Also, a
	// proper Broker shutdown is the responsibility of application because Node does not know about
	// custom Broker instances. When GetBroker returns false as the second argument then Node will
	// use the default Broker for the channel.
	GetBroker func(channel string) (Broker, bool)
	// GetPresenceManager when set allows returning a custom PresenceManager to use for a specific
	// channel. If not set then the default Node's PresenceManager is always used for all channels.
	// A proper PresenceManager shutdown is the responsibility of application because Node does not
	// know about custom PresenceManager instances. When GetPresenceManager returns false as the second
	// argument then Node will use the default PresenceManager for the channel.
	GetPresenceManager func(channel string) (PresenceManager, bool)
	// Tell Centrifuge how to transform connect error codes to disconnect objects for unidirectional
	// transports. If not set or code not found in the mapping then Centrifuge falls back to the default
	// mapping defined internally.
	UnidirectionalCodeToDisconnect map[uint32]Disconnect
	// GetChannelBatchConfig allows configuring per-channel write batching. Batching config if
	// returned is applied for publications and join/leave channel pushes for all channel subscribers.
	// The cost of batching are extra goroutines, buffers and extra timers for each channel used in
	// batching, so you can expect memory overhead. But batching may be useful for reducing CPU usage
	// coming from write system calls in channels with high publication rate. If GetChannelBatchConfig
	// not set then no batching is used on per-channel level. This function may be called in the hot
	// broadcast path, so must be fast. This is an EXPERIMENTAL feature.
	GetChannelBatchConfig func(channel string) ChannelBatchConfig
	// ClientTimerScheduler if set will be used for scheduling client timers.
	// This is an EXPERIMENTAL API.
	ClientTimerScheduler TimerScheduler

	// Map contains options for map subscriptions.
	// This is an EXPERIMENTAL API.
	Map MapConfig
	// SharedPoll contains options for shared poll subscriptions.
	// This is an EXPERIMENTAL API.
	SharedPoll SharedPollConfig
}

type MapConfig struct {
	// GetMapBroker when set allows returning a custom MapBroker to use for a specific channel.
	// This enables sharding map state across multiple databases based on channel. If not set
	// then the default Node's MapBroker (set via SetMapBroker) is always used for all channels.
	// It's the responsibility of an application to properly initialize and shutdown all MapBroker
	// instances. When GetMapBroker returns false as the second argument then Node will use the
	// default MapBroker for the channel.
	GetMapBroker func(channel string) (MapBroker, bool)
	// GetMapChannelOptions returns channel options for map channels.
	// Required for map channel operations. If nil, any map operation returns an error.
	// Each channel must have SyncMode and RetentionMode explicitly set.
	// NOTE: this callback is invoked on every broker operation (Publish, Remove,
	// ReadState, ReadStream, etc.) — it must be fast and should not perform I/O.
	// This is an EXPERIMENTAL API.
	GetMapChannelOptions func(channel string) MapChannelOptions
}

type SharedPollConfig struct {
	// GetSharedPollChannelOptions returns per-channel options for shared poll
	// channels. Required for shared poll operations — if nil, any shared poll
	// subscribe returns an error. Called when a new channel first appears in
	// SharedPollManager. Must be fast, no I/O.
	// This is an EXPERIMENTAL API.
	GetSharedPollChannelOptions func(channel string) (SharedPollChannelOptions, bool)
	// ConcurrencyLimit sets the maximum number of concurrent backend
	// calls across all shared poll channels. All channels share this pool.
	// Zero means 64.
	ConcurrencyLimit int
}

// SharedPollNotificationItem represents a lightweight notification that a specific
// key in a channel has changed. Used to trigger immediate backend polls for
// just the affected keys instead of waiting for the next timer-based cycle.
type SharedPollNotificationItem struct {
	Channel string `json:"channel"`
	Key     string `json:"key"`
}

// SharedPollNotification is the top-level JSON envelope for notifications
// published to the notification pub/sub channel.
type SharedPollNotification struct {
	Items []SharedPollNotificationItem `json:"items"`
}

const (
	// SharedPollModeVersionless is the versionless mode where the backend
	// returns {key, data} without versions. Centrifugo detects changes via content hash
	// and generates internal synthetic versions. This is the default mode (also "").
	SharedPollModeVersionless = "versionless"
	// SharedPollModeVersioned is the versioned mode where the backend
	// returns items with {key, data, version}. Versions are included in requests,
	// allowing the backend to skip unchanged items. Enables direct publish and
	// cached initial data.
	SharedPollModeVersioned = "versioned"
)

// SharedPollChannelOptions configures a shared poll channel.
type SharedPollChannelOptions struct {
	// MaxKeysPerConnection limits how many keys a single connection
	// can track in this channel. Zero value means 5000.
	MaxKeysPerConnection int
	// RefreshInterval sets how often the refresh worker calls OnSharedPoll.
	// Zero value means 10 * time.Second.
	RefreshInterval time.Duration
	// RefreshBatchSize sets the maximum number of item keys per OnSharedPoll call.
	// Zero value means 1000.
	RefreshBatchSize int
	// Mode controls request format.
	// "versionless" (default, also ""): backend returns {key, data} without versions.
	//   Centrifugo detects changes via content hash and generates internal versions.
	// "versioned": backend returns {key, data, version}. Versions are included in
	//   requests, allowing the backend to skip unchanged items.
	Mode string
	// KeepLatestData controls whether TrackedEntry stores the last data payload
	// per item. Default false.
	KeepLatestData bool
	// RefreshIntervalFn, if set, is called after each refresh cycle with the
	// cycle's wall time, and returns the interval before the next cycle. Overrides
	// static RefreshInterval. Used by Centrifugo PRO for adaptive backpressure.
	RefreshIntervalFn func(cycleDuration time.Duration) time.Duration
	// CallTimeout sets the maximum duration for each OnSharedPoll callback invocation.
	// Zero value means 30 * time.Second.
	CallTimeout time.Duration
	// ChannelShutdownDelay is the delay before shutting down a channel
	// state after the last item is untracked. Zero means default (1s).
	// Set to -1 for immediate shutdown with no delay.
	ChannelShutdownDelay time.Duration
	// NotificationBatchMaxSize sets the maximum number of notified keys to
	// accumulate before triggering an immediate backend poll. The batch fires
	// when either this size or NotificationBatchMaxDelay is reached first.
	// Zero disables size-based batching (batching still fires on delay if
	// NotificationBatchMaxDelay is set).
	NotificationBatchMaxSize int
	// NotificationBatchMaxDelay sets the maximum time to wait while
	// accumulating notified keys before triggering an immediate backend poll.
	// Zero disables delay-based batching (batching still fires on size if
	// NotificationBatchMaxSize is set). When neither is set, notifications
	// are dispatched immediately (no batching).
	NotificationBatchMaxDelay time.Duration
	// TrackExpiredExtraDelay is extra time given to client to refresh track signature
	// after it expires. Keys not refreshed within this delay are silently removed
	// from server state. Zero means 25 * time.Second.
	TrackExpiredExtraDelay time.Duration
	// PublishEnabled enables cross-node distribution for SharedPollPublish.
	// When true, SharedPollManager subscribes to the Broker (via node.getBroker)
	// for this channel, allowing SharedPollPublish to distribute publications
	// to all nodes. When false, SharedPollPublish is local-only.
	PublishEnabled bool
}

func (o SharedPollChannelOptions) isVersionless() bool {
	return o.Mode == "" || o.Mode == SharedPollModeVersionless
}

func (o SharedPollChannelOptions) toKeyedChannelOptions() keyedChannelOptions {
	return keyedChannelOptions{
		MaxTrackedPerConnection: o.MaxKeysPerConnection,
	}
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

// RegistererGatherer defines an interface that combines Registerer and Gatherer from Prometheus.
// Prometheus Registry implements both interfaces.
type RegistererGatherer interface {
	prometheus.Registerer
	prometheus.Gatherer
}

type MetricsConfig struct {
	// MetricsNamespace is a Prometheus metrics namespace to use for Centrifuge metrics.
	// If not set then the default metrics namespace name "centrifuge" will be used.
	MetricsNamespace string
	// RegistererGatherer is a Prometheus registerer and gatherer. If not set then a
	// prometheus.DefaultRegisterer and prometheus.DefaultGatherer will be used.
	RegistererGatherer RegistererGatherer

	// GetChannelNamespaceLabel if set will be used by Centrifuge to extract channel_namespace
	// label for channel related metrics. Make sure to maintain low cardinality of returned values
	// to avoid issues with Prometheus performance. This function may introduce sufficient overhead
	// since it's called in hot paths - so it should be fast. By default, Centrifuge uses cache
	// of resolved channel namespace labels to avoid calling this function too often. See below
	// ChannelNamespaceCacheSize and ChannelNamespaceCacheTTL options to tweak the cache behavior.
	GetChannelNamespaceLabel func(channel string) string
	// ChannelNamespaceCacheSize sets the size of the cache for channel namespace label resolution.
	// Zero value will use cache size equal to 4096. Set -1 to disable cache (in that case make sure
	// your GetChannelNamespaceLabel is fast and ideally does not allocate because it's called in hot
	// paths).
	ChannelNamespaceCacheSize int
	// ChannelNamespaceCacheTTL sets the time after which resolved channel namespace for a channel
	// will expire in the cache. If zero – default TTL 10 seconds is used.
	ChannelNamespaceCacheTTL time.Duration

	// RegisteredClientNames is an optional list of known client names which will be allowed to be
	// attached as labels to metrics. If client passed a name which is not in the list – then Centrifuge
	// will use string "unregistered" as a client_name label. We need to be strict here to avoid
	// Prometheus cardinality issues.
	RegisteredClientNames []string
	// CheckRegisteredClientVersion is a function to check whether the version passed by a client with a
	// particular name is valid and can be used in metric values. When function is not set or returns
	// false Centrifuge will use "unregistered" value for a client version. Note, the name argument here
	// is an original name of client passed to Centrifuge.
	CheckRegisteredClientVersion func(clientName string, clientVersion string) bool
	// EnableRecoveredPublicationsHistogram enables histogram tracking of number of publications
	// recovered during subscription successful recovery operations.
	EnableRecoveredPublicationsHistogram bool
	// ExposeTransportAcceptProtocol enables exposing in labels the accept protocol used by client's transport.
	// If not enabled - empty string will be used as a label value.
	ExposeTransportAcceptProtocol bool
}

// PingPongConfig allows configuring application level ping-pong behavior.
// Note that in current implementation PingPongConfig.PingInterval must be greater than PingPongConfig.PongTimeout.
type PingPongConfig struct {
	// PingInterval tells how often to issue server-to-client pings.
	// For zero value 25 secs will be used. To disable sending app-level pings use -1.
	PingInterval time.Duration
	// PongTimeout sets time for pong check after issuing a ping.
	// For zero value 10 seconds will be used. To disable pong checks use -1.
	// PongTimeout must be less than PingInterval in current implementation.
	PongTimeout time.Duration
}

// Defaults applied by getPingPongPeriodValues when callers pass a zero value
// (the "use library default" sentinel). Declared as vars rather than inline
// constants so test binaries can override them in an init() hook to keep the
// test suite fast — production behavior is unchanged.
var (
	defaultPingInterval = 25 * time.Second
	defaultPongTimeout  = 10 * time.Second
)

func getPingPongPeriodValues(config PingPongConfig) (time.Duration, time.Duration) {
	pingInterval := config.PingInterval
	if pingInterval < 0 {
		pingInterval = 0
	} else if pingInterval == 0 {
		pingInterval = defaultPingInterval
	}
	pongTimeout := config.PongTimeout
	if pongTimeout < 0 {
		pongTimeout = 0
	} else if pongTimeout == 0 {
		pongTimeout = defaultPongTimeout
	}
	return pingInterval, pongTimeout
}

func warnAboutIncorrectPingPongConfig(node *Node, config PingPongConfig, transportName string) {
	pingInterval, pongTimeout := getPingPongPeriodValues(config)
	if pingInterval > 0 && pongTimeout > 0 && pongTimeout >= pingInterval {
		node.logger.log(newLogEntry(
			LogLevelWarn,
			"ping interval must be greater than pong timeout to work properly",
			map[string]any{
				"transport":     transportName,
				"ping_interval": pingInterval.String(),
				"pong_timeout":  pongTimeout.String(),
			},
		))
	}
}
