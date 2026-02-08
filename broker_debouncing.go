package centrifuge

import (
	"context"
	"time"
)

// DebouncingBrokerConfig configures the DebouncingBroker.
type DebouncingBrokerConfig struct {
	// Debounce returns the debounce duration for a given channel.
	// Return 0 to disable debouncing (passthrough) for that channel.
	Debounce func(channel string) time.Duration
}

// DebouncingBroker wraps a Broker and debounces Publish calls per (channel, key)
// pair. When debouncing is active, only the latest Publish is forwarded to the
// backend after the debounce duration elapses. A Publish with Removed=true cancels
// any pending debounced Publish for the same (channel, key).
type DebouncingBroker struct {
	Broker
	conf      DebouncingBrokerConfig
	debouncer *debouncer[brokerDebounceValue]
}

type brokerDebounceValue struct {
	data []byte
	opts PublishOptions
}

var _ Broker = (*DebouncingBroker)(nil)

// NewDebouncingBroker creates a new DebouncingBroker wrapping the given backend.
func NewDebouncingBroker(backend Broker, conf DebouncingBrokerConfig) *DebouncingBroker {
	b := &DebouncingBroker{
		Broker: backend,
		conf:   conf,
	}
	b.debouncer = newDebouncer(func(channel, key string, v brokerDebounceValue) {
		_, _ = b.Broker.Publish(channel, v.data, v.opts)
	})
	return b
}

// Publish debounces the publish if configured, otherwise passes through.
// When opts.Removed is true and a pending debounced publish exists for the same
// (channel, opts.Key), the pending publish is cancelled before forwarding the
// removal to the backend.
func (b *DebouncingBroker) Publish(ch string, data []byte, opts PublishOptions) (PublishResult, error) {
	if b.conf.Debounce == nil {
		return b.Broker.Publish(ch, data, opts)
	}
	d := b.conf.Debounce(ch)
	if d == 0 {
		return b.Broker.Publish(ch, data, opts)
	}
	key := opts.Key
	// Removal cancels pending debounced publish and passes through.
	if opts.Removed {
		b.debouncer.Cancel(ch, key)
		return b.Broker.Publish(ch, data, opts)
	}
	b.debouncer.Debounce(ch, key, d, brokerDebounceValue{data: data, opts: opts})
	return PublishResult{}, nil
}

// Close stops all pending debounce timers and releases resources.
func (b *DebouncingBroker) Close() {
	b.debouncer.Close()
	if closer, ok := b.Broker.(Closer); ok {
		defer func() { _ = closer.Close(context.Background()) }()
	}
}
