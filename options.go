package centrifuge

import "time"

// PublishOption is a type to represent various Publish options.
type PublishOption func(*PublishOptions)

// WithHistory tells Broker to save message to history stream with provided size and ttl.
func WithHistory(size int, ttl time.Duration) PublishOption {
	return func(opts *PublishOptions) {
		opts.HistorySize = size
		opts.HistoryTTL = ttl
	}
}

// WithClientInfo adds ClientInfo to Publication.
func WithClientInfo(info *ClientInfo) PublishOption {
	return func(opts *PublishOptions) {
		opts.ClientInfo = info
	}
}

// SubscribeOptions define per-subscription options.
type SubscribeOptions struct {
	// ExpireAt defines time in future when subscription should expire,
	// zero value means no expiration.
	ExpireAt int64
	// ChannelInfo defines custom channel information, zero value means no channel information.
	ChannelInfo []byte
	// Presence turns on participating in channel presence.
	Presence bool
	// JoinLeave enables sending Join and Leave messages for this client in channel.
	JoinLeave bool
	// When position is on client will additionally sync its position inside
	// a stream to prevent message loss. Make sure you are enabling Position in channels
	// that maintain Publication history stream. When Position is on  Centrifuge will
	// include StreamPosition information to subscribe response - for a client to be able
	// to manually track its position inside a stream.
	Position bool
	// Recover turns on recovery option for a channel. In this case client will try to
	// recover missed messages automatically upon resubscribe to a channel after reconnect
	// to a server. This option also enables client position tracking inside a stream
	// (like Position option) to prevent occasional message loss. Make sure you are using
	// Recover in channels that maintain Publication history stream.
	Recover bool
}

// SubscribeOption is a type to represent various Subscribe options.
type SubscribeOption func(*SubscribeOptions)

// WithExpireAt allows to set ExpireAt field.
func WithExpireAt(expireAt int64) SubscribeOption {
	return func(opts *SubscribeOptions) {
		opts.ExpireAt = expireAt
	}
}

// WithChannelInfo ...
func WithChannelInfo(chanInfo []byte) SubscribeOption {
	return func(opts *SubscribeOptions) {
		opts.ChannelInfo = chanInfo
	}
}

// WithPresence ...
func WithPresence(enabled bool) SubscribeOption {
	return func(opts *SubscribeOptions) {
		opts.Presence = enabled
	}
}

// WithJoinLeave ...
func WithJoinLeave(enabled bool) SubscribeOption {
	return func(opts *SubscribeOptions) {
		opts.JoinLeave = enabled
	}
}

// WithPosition ...
func WithPosition(enabled bool) SubscribeOption {
	return func(opts *SubscribeOptions) {
		opts.Position = enabled
	}
}

// WithRecover ...
func WithRecover(enabled bool) SubscribeOption {
	return func(opts *SubscribeOptions) {
		opts.Recover = enabled
	}
}

// DisconnectOptions define some fields to alter behaviour of Disconnect operation.
type DisconnectOptions struct {
	// Disconnect represents custom disconnect to use.
	// By default DisconnectForceNoReconnect will be used.
	Disconnect *Disconnect
	// ClientWhitelist contains client IDs to keep.
	ClientWhitelist []string
}

// DisconnectOption is a type to represent various Disconnect options.
type DisconnectOption func(options *DisconnectOptions)

// WithDisconnect allows to set custom Disconnect.
func WithDisconnect(disconnect *Disconnect) DisconnectOption {
	return func(opts *DisconnectOptions) {
		opts.Disconnect = disconnect
	}
}

// WithClientWhitelist allows to set ClientWhitelist.
func WithClientWhitelist(whitelist []string) DisconnectOption {
	return func(opts *DisconnectOptions) {
		opts.ClientWhitelist = whitelist
	}
}

// HistoryOptions define some fields to alter History method behaviour.
type HistoryOptions struct {
	// Since used to extract publications from stream since provided StreamPosition.
	Since *StreamPosition
	// Limit number of publications to return.
	// -1 means no limit - i.e. return all publications currently in stream.
	// 0 means that caller only interested in current stream top position so
	// Broker should not return any publications in result.
	// Positive integer does what it should.
	Limit int
}

// HistoryOption is a type to represent various History options.
type HistoryOption func(options *HistoryOptions)

// NoLimit defines that limit should not be applied.
const NoLimit = -1

// WithLimit allows to set limit.
func WithLimit(limit int) HistoryOption {
	return func(opts *HistoryOptions) {
		opts.Limit = limit
	}
}

// Since allows to set Since option.
func Since(sp *StreamPosition) HistoryOption {
	return func(opts *HistoryOptions) {
		opts.Since = sp
	}
}
