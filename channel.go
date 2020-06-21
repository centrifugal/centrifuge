package centrifuge

type ChannelOptionsGetter interface {
	ChannelOptions(channel string) (ChannelOptions, error)
}

// ChannelOptions represent channel specific configuration for namespace
// or global channel options if set on top level of configuration.
type ChannelOptions struct {
	// JoinLeave turns on join/leave messages for channels.
	// When client subscribes on channel join message sent to all
	// clients in this channel. When client leaves channel (unsubscribes)
	// leave message sent. This option does not fit well for channels with
	// many subscribers because every subscribe/unsubscribe event results
	// into join/leave event broadcast to all other active subscribers.
	JoinLeave bool `mapstructure:"join_leave" json:"join_leave"`

	// Presence turns on presence information for channels.
	// Presence is a structure with clients currently subscribed on channel.
	Presence bool `json:"presence"`

	// PresenceDisableForClient prevents presence to be asked by clients.
	// In this case it's available only over server-side presence call.
	PresenceDisableForClient bool `mapstructure:"presence_disable_for_client" json:"presence_disable_for_client"`

	// HistorySize determines max amount of history messages for channel,
	// 0 means no history for channel. Centrifugo history has auxiliary
	// role – it can not replace your backend persistent storage.
	HistorySize int `mapstructure:"history_size" json:"history_size"`

	// HistoryLifetime determines time in seconds until expiration for
	// history messages. As Centrifuge-based server keeps history in memory
	// (for example in process memory or in Redis process memory) it's
	// important to remove old messages to prevent infinite memory grows.
	HistoryLifetime int `mapstructure:"history_lifetime" json:"history_lifetime"`

	// Recover enables recover mechanism for channels. This means that
	// server will try to recover missed messages for resubscribing
	// client. This option uses publications from history and must be used
	// with reasonable HistorySize and HistoryLifetime configuration.
	HistoryRecover bool `mapstructure:"history_recover" json:"history_recover"`

	// HistoryDisableForClient prevents history to be asked by clients.
	// In this case it's available only over server-side history call.
	// History recover mechanism if enabled will continue to work for
	// clients anyway.
	HistoryDisableForClient bool `mapstructure:"history_disable_for_client" json:"history_disable_for_client"`
}
