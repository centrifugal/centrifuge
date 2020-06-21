package centrifuge

import (
	"crypto/rsa"
	"time"
)

// Config contains Node configuration options.
type Config struct {
	// Version of server – will be sent to client on connection establishment
	// phase in response to connect request.
	Version string
	// Name of this server node - must be unique, used as human readable
	// and meaningful node identifier.
	Name string
	// TokenHMACSecretKey is a secret key used to validate connection and subscription
	// tokens generated using HMAC. Zero value means that HMAC tokens won't be allowed.
	TokenHMACSecretKey string
	// TokenRSAPublicKey is a public key used to validate connection and subscription
	// tokens generated using RSA. Zero value means that RSA tokens won't be allowed.
	TokenRSAPublicKey *rsa.PublicKey
	// ClientPresenceUpdateInterval is an interval how often connected clients
	// must update presence info.
	ClientPresenceUpdateInterval time.Duration
	// ClientPresenceExpireInterval is an interval how long to consider
	// presence info valid after receiving presence ping.
	ClientPresenceExpireInterval time.Duration
	// ClientExpiredCloseDelay is an extra time given to client to
	// refresh its connection in the end of connection lifetime.
	ClientExpiredCloseDelay time.Duration
	// ClientExpiredSubCloseDelay is an extra time given to client to
	// refresh its expiring subscription in the end of subscription lifetime.
	ClientExpiredSubCloseDelay time.Duration
	// ClientStaleCloseDelay is a timeout after which connection will be
	// closed if still not authenticated (i.e. no valid connect command
	// received yet).
	ClientStaleCloseDelay time.Duration
	// ClientChannelPositionCheckDelay defines minimal time from previous
	// client position check in channel. If client does not pass check it will
	// be disconnected with DisconnectInsufficientState.
	ClientChannelPositionCheckDelay time.Duration
	// NodeInfoMetricsAggregateInterval sets interval for automatic metrics aggregation.
	// It's not very reasonable to have it less than one second.
	NodeInfoMetricsAggregateInterval time.Duration
	// LogLevel is a log level to use. By default nothing will be logged.
	LogLevel LogLevel
	// LogHandler is a handler func node will send logs to.
	LogHandler LogHandler
	// ClientQueueMaxSize is a maximum size of client's message queue in bytes.
	// After this queue size exceeded Centrifugo closes client's connection.
	ClientQueueMaxSize int
	// ClientChannelLimit sets upper limit of channels each client can subscribe to.
	ClientChannelLimit int
	// ClientUserConnectionLimit limits number of client connections from user with the
	// same ID. 0 - unlimited.
	ClientUserConnectionLimit int
	// ClientInsecure turns on insecure mode for client connections - when it's
	// turned on then no authentication required at all when connecting to Centrifugo,
	// anonymous access and publish allowed for all channels, no connection expire
	// performed. This can be suitable for demonstration or personal usage.
	ClientInsecure bool
	// ClientAnonymous when set to true, allows connect requests without specifying
	// a token or setting Credentials in authentication middleware. The resulting
	// user will have empty string for user ID, meaning user can only subscribe
	// to anonymous channels.
	ClientAnonymous bool
	// UserSubscribeToPersonal enables automatic subscribing to personal channel by user.
	// Only users with user ID defined will subscribe to personal channels, anonymous
	// users are ignored.
	UserSubscribeToPersonal bool
	// ChannelMaxLength is a maximum length of channel name.
	ChannelMaxLength int
}

// Validate validates config and returns error if problems found
func (c *Config) Validate() error {
	return nil
}

const (
	// nodeInfoPublishInterval is an interval how often node must publish
	// node control message.
	nodeInfoPublishInterval = 3 * time.Second
	// nodeInfoCleanInterval is an interval in seconds, how often node must
	// clean information about other running nodes.
	nodeInfoCleanInterval = nodeInfoPublishInterval * 3
	// nodeInfoMaxDelay is an interval in seconds – how many seconds node
	// info considered actual.
	nodeInfoMaxDelay = nodeInfoPublishInterval*2 + time.Second
)

// DefaultConfig is Config initialized with default values for all fields.
var DefaultConfig = Config{
	Name: "centrifuge",

	ChannelMaxLength: 255,

	NodeInfoMetricsAggregateInterval: 60 * time.Second,

	ClientPresenceUpdateInterval:    25 * time.Second,
	ClientPresenceExpireInterval:    60 * time.Second,
	ClientExpiredCloseDelay:         25 * time.Second,
	ClientExpiredSubCloseDelay:      25 * time.Second,
	ClientStaleCloseDelay:           25 * time.Second,
	ClientChannelPositionCheckDelay: 40 * time.Second,
	ClientQueueMaxSize:              10485760, // 10MB by default
	ClientChannelLimit:              128,
}
