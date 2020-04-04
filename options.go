package centrifuge

// PublishOptions define some fields to alter behaviour of Publish operation.
type PublishOptions struct {
	// SkipHistory allows to prevent saving specific Publication to channel history.
	SkipHistory bool
}

// PublishOption is a type to represent various Publish options.
type PublishOption func(*PublishOptions)

// SkipHistory allows to set SkipHistory to true.
func SkipHistory() PublishOption {
	return func(opts *PublishOptions) {
		opts.SkipHistory = true
	}
}

// UnsubscribeOptions define some fields to alter behaviour of Unsubscribe operation.
type UnsubscribeOptions struct {
	// Resubscribe allows to set resubscribe protocol flag.
	Resubscribe bool
}

// UnsubscribeOption is a type to represent various Unsubscribe options.
type UnsubscribeOption func(*UnsubscribeOptions)

// WithResubscribe allows to set Resubscribe flag to true.
func WithResubscribe() UnsubscribeOption {
	return func(opts *UnsubscribeOptions) {
		opts.Resubscribe = true
	}
}

// DisconnectOptions define some fields to alter behaviour of Disconnect operation.
type DisconnectOptions struct {
	// Reconnect allows to set reconnect flag.
	Reconnect bool
}

// DisconnectOption is a type to represent various Unsubscribe options.
type DisconnectOption func(options *DisconnectOptions)

// WithReconnect allows to set Reconnect flag to true.
func WithReconnect() DisconnectOption {
	return func(opts *DisconnectOptions) {
		opts.Reconnect = true
	}
}
