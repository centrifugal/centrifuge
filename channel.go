package centrifuge

// ChannelOptionsFunc is a function that Centrifuge will call every time
// it needs to get ChannelOptions for a channel. Calls to this func can happen
// concurrently – so you need to synchronize code inside function implementation.
type ChannelOptionsFunc func(channel string) (ChannelOptions, error)

// ChannelOptions represent channel specific configuration for namespace
// or global channel options if set on top level of configuration.
type ChannelOptions struct {
	// JoinLeave turns on join/leave messages for channel.
	// When client subscribes on channel join message sent to all
	// clients in this channel. When client leaves channel (unsubscribes)
	// leave message sent. This option does not fit well for channels with
	// many subscribers because every subscribe/unsubscribe event results
	// into join/leave event broadcast to all other active subscribers.
	JoinLeave bool `mapstructure:"join_leave" json:"join_leave"`

	// Presence turns on presence information for channel.
	// Presence is a structure with clients currently subscribed on channel.
	Presence bool `json:"presence"`

	// HistorySize determines max amount of history messages for channel,
	// 0 means no history for channel. Centrifuge history has an auxiliary
	// role with current Engines – it can not replace your backend persistent
	// storage.
	HistorySize int `mapstructure:"history_size" json:"history_size"`

	// HistoryLifetime determines time in seconds until expiration for
	// history messages. As Centrifuge-based server maintains a window of
	// messages in memory (or in Redis with Redis engine), to prevent
	// infinite memory grows it's important to remove history for inactive
	// channels.
	HistoryLifetime int `mapstructure:"history_lifetime" json:"history_lifetime"`

	// Recover enables recovery mechanism for channels. This means that
	// server will try to recover missed messages for resubscribing
	// client. This option uses publications from history and must be used
	// with reasonable HistorySize and HistoryLifetime configuration.
	HistoryRecover bool `mapstructure:"history_recover" json:"history_recover"`
}
