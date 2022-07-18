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

// WithTags allows setting Publication.Tags.
func WithTags(meta map[string]string) PublishOption {
	return func(opts *PublishOptions) {
		opts.Tags = meta
	}
}

// SubscribeOptions define per-subscription options.
type SubscribeOptions struct {
	// ExpireAt defines time in future when subscription should expire,
	// zero value means no expiration.
	ExpireAt int64
	// ChannelInfo defines custom channel information, zero value means no channel information.
	ChannelInfo []byte
	// EmitPresence turns on participating in channel presence - i.e. client
	// subscription will emit presence updates to PresenceManager and will be visible
	// in a channel presence result.
	EmitPresence bool
	// EmitJoinLeave turns on emitting Join and Leave events from the subscribing client.
	// See also PushJoinLeave if you want current client to receive join/leave messages.
	EmitJoinLeave bool
	// PushJoinLeave turns on receiving channel Join and Leave events by the client.
	// Subscriptions which emit join/leave events should have EmitJoinLeave on.
	PushJoinLeave bool
	// When position is on client will additionally sync its position inside a stream
	// to prevent publication loss. The loss can happen due to at most once guarantees
	// of PUB/SUB model. Make sure you are enabling EnablePositioning in channels that
	// maintain Publication history stream. When EnablePositioning is on Centrifuge will
	// include StreamPosition information to subscribe response - for a client to be
	// able to manually track its position inside a stream.
	EnablePositioning bool
	// EnableRecovery turns on automatic recovery for a channel. In this case
	// client will try to recover missed messages upon resubscribe to a channel
	// after reconnect to a server. This option also enables client position
	// tracking inside a stream (i.e. enabling EnableRecovery will automatically
	// enable EnablePositioning option) to prevent occasional publication loss.
	// Make sure you are using EnableRecovery in channels that maintain Publication
	// history stream.
	EnableRecovery bool
	// Data to send to a client with Subscribe Push.
	Data []byte
	// RecoverSince will try to subscribe a client and recover from a certain StreamPosition.
	RecoverSince *StreamPosition
	// clientID to subscribe.
	clientID string
	// sessionID to subscribe.
	sessionID string
}

// SubscribeOption is a type to represent various Subscribe options.
type SubscribeOption func(*SubscribeOptions)

// WithExpireAt allows setting ExpireAt field.
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

// WithEmitPresence ...
func WithEmitPresence(enabled bool) SubscribeOption {
	return func(opts *SubscribeOptions) {
		opts.EmitPresence = enabled
	}
}

// WithEmitJoinLeave ...
func WithEmitJoinLeave(enabled bool) SubscribeOption {
	return func(opts *SubscribeOptions) {
		opts.EmitJoinLeave = enabled
	}
}

// WithPushJoinLeave ...
func WithPushJoinLeave(enabled bool) SubscribeOption {
	return func(opts *SubscribeOptions) {
		opts.PushJoinLeave = enabled
	}
}

// WithPositioning ...
func WithPositioning(enabled bool) SubscribeOption {
	return func(opts *SubscribeOptions) {
		opts.EnablePositioning = enabled
	}
}

// WithRecovery ...
func WithRecovery(enabled bool) SubscribeOption {
	return func(opts *SubscribeOptions) {
		opts.EnableRecovery = enabled
	}
}

// WithSubscribeClient allows setting client ID that should be subscribed.
// This option not used when Client.Subscribe called.
func WithSubscribeClient(clientID string) SubscribeOption {
	return func(opts *SubscribeOptions) {
		opts.clientID = clientID
	}
}

// WithSubscribeSession allows setting session ID that should be subscribed.
// This option not used when Client.Subscribe called.
func WithSubscribeSession(sessionID string) SubscribeOption {
	return func(opts *SubscribeOptions) {
		opts.sessionID = sessionID
	}
}

// WithSubscribeData allows setting custom data to send with subscribe push.
func WithSubscribeData(data []byte) SubscribeOption {
	return func(opts *SubscribeOptions) {
		opts.Data = data
	}
}

// WithRecoverSince allows setting SubscribeOptions.RecoverFrom.
func WithRecoverSince(since *StreamPosition) SubscribeOption {
	return func(opts *SubscribeOptions) {
		opts.RecoverSince = since
	}
}

// RefreshOptions ...
type RefreshOptions struct {
	// Expired can close connection with expired reason.
	Expired bool
	// ExpireAt defines time in future when subscription should expire,
	// zero value means no expiration.
	ExpireAt int64
	// Info defines custom channel information, zero value means no channel information.
	Info []byte
	// clientID to refresh.
	clientID string
	// sessionID to refresh.
	sessionID string
}

// RefreshOption is a type to represent various Refresh options.
type RefreshOption func(options *RefreshOptions)

// WithRefreshClient to limit refresh only for specified client ID.
func WithRefreshClient(clientID string) RefreshOption {
	return func(opts *RefreshOptions) {
		opts.clientID = clientID
	}
}

// WithRefreshSession to limit refresh only for specified session ID.
func WithRefreshSession(sessionID string) RefreshOption {
	return func(opts *RefreshOptions) {
		opts.sessionID = sessionID
	}
}

// WithRefreshExpired to set expired flag - connection will be closed with DisconnectExpired.
func WithRefreshExpired(expired bool) RefreshOption {
	return func(opts *RefreshOptions) {
		opts.Expired = expired
	}
}

// WithRefreshExpireAt to set unix seconds in the future when connection should expire.
// Zero value means no expiration.
func WithRefreshExpireAt(expireAt int64) RefreshOption {
	return func(opts *RefreshOptions) {
		opts.ExpireAt = expireAt
	}
}

// WithRefreshInfo to override connection info.
func WithRefreshInfo(info []byte) RefreshOption {
	return func(opts *RefreshOptions) {
		opts.Info = info
	}
}

// UnsubscribeOptions ...
type UnsubscribeOptions struct {
	// clientID to unsubscribe.
	clientID string
	// sessionID to unsubscribe.
	sessionID string
	// custom unsubscribe object.
	unsubscribe *Unsubscribe
}

// UnsubscribeOption is a type to represent various Unsubscribe options.
type UnsubscribeOption func(options *UnsubscribeOptions)

// WithUnsubscribeClient allows setting client ID that should be unsubscribed.
// This option not used when Client.Unsubscribe called.
func WithUnsubscribeClient(clientID string) UnsubscribeOption {
	return func(opts *UnsubscribeOptions) {
		opts.clientID = clientID
	}
}

// WithUnsubscribeSession allows setting session ID that should be unsubscribed.
// This option not used when Client.Unsubscribe called.
func WithUnsubscribeSession(sessionID string) UnsubscribeOption {
	return func(opts *UnsubscribeOptions) {
		opts.sessionID = sessionID
	}
}

// WithCustomUnsubscribe allows setting custom Unsubscribe.
func WithCustomUnsubscribe(unsubscribe Unsubscribe) UnsubscribeOption {
	return func(opts *UnsubscribeOptions) {
		opts.unsubscribe = &unsubscribe
	}
}

// DisconnectOptions define some fields to alter behaviour of Disconnect operation.
type DisconnectOptions struct {
	// Disconnect represents custom disconnect to use.
	// By default, DisconnectForceNoReconnect will be used.
	Disconnect *Disconnect
	// ClientWhitelist contains client IDs to keep.
	ClientWhitelist []string
	// clientID to disconnect.
	clientID string
	// sessionID to disconnect.
	sessionID string
}

// DisconnectOption is a type to represent various Disconnect options.
type DisconnectOption func(options *DisconnectOptions)

// WithCustomDisconnect allows setting custom Disconnect.
func WithCustomDisconnect(disconnect Disconnect) DisconnectOption {
	return func(opts *DisconnectOptions) {
		opts.Disconnect = &disconnect
	}
}

// WithDisconnectClient allows setting Client.
func WithDisconnectClient(clientID string) DisconnectOption {
	return func(opts *DisconnectOptions) {
		opts.clientID = clientID
	}
}

// WithDisconnectSession allows setting session ID to disconnect.
func WithDisconnectSession(sessionID string) DisconnectOption {
	return func(opts *DisconnectOptions) {
		opts.sessionID = sessionID
	}
}

// WithDisconnectClientWhitelist allows setting ClientWhitelist.
func WithDisconnectClientWhitelist(whitelist []string) DisconnectOption {
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
	// Reverse direction
	Reverse bool
}

// HistoryOption is a type to represent various History options.
type HistoryOption func(options *HistoryOptions)

// NoLimit defines that limit should not be applied.
const NoLimit = -1

// WithLimit allows setting HistoryOptions.Limit.
func WithLimit(limit int) HistoryOption {
	return func(opts *HistoryOptions) {
		opts.Limit = limit
	}
}

// WithSince allows setting HistoryOptions.Since option.
func WithSince(sp *StreamPosition) HistoryOption {
	return func(opts *HistoryOptions) {
		opts.Since = sp
	}
}

// WithReverse allows setting HistoryOptions.Reverse option.
func WithReverse(reverse bool) HistoryOption {
	return func(opts *HistoryOptions) {
		opts.Reverse = reverse
	}
}
