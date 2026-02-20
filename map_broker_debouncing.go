package centrifuge

import (
	"context"
	"time"
)

// DebouncingMapBrokerConfig configures the DebouncingMapBroker.
type DebouncingMapBrokerConfig struct {
	// Debounce returns the debounce duration for a given channel.
	// Return 0 to disable debouncing (passthrough) for that channel.
	Debounce func(channel string) time.Duration
}

// DebouncingMapBroker wraps a MapBroker and debounces Publish calls
// per (channel, key) pair. When debouncing is active, only the latest
// Publish is forwarded to the backend after the debounce duration elapses.
// Remove cancels any pending debounced Publish for the same (channel, key).
//
// When a Publish is debounced (not passed through), it returns a zero-value
// MapPublishResult and nil error immediately. The actual backend Publish
// happens asynchronously when the debounce timer fires. Callers should not
// rely on MapPublishResult fields (like Position) for debounced calls.
type DebouncingMapBroker struct {
	MapBroker
	node      *Node
	conf      DebouncingMapBrokerConfig
	debouncer *debouncer[mapDebounceValue]
}

type mapDebounceValue struct {
	opts MapPublishOptions
}

var _ MapBroker = (*DebouncingMapBroker)(nil)

// NewDebouncingMapBroker creates a new DebouncingMapBroker wrapping the given backend.
func NewDebouncingMapBroker(node *Node, backend MapBroker, conf DebouncingMapBrokerConfig) *DebouncingMapBroker {
	b := &DebouncingMapBroker{
		MapBroker: backend,
		node:      node,
		conf:      conf,
	}
	b.debouncer = newDebouncer(func(channel, key string, v mapDebounceValue) {
		_, err := b.MapBroker.Publish(context.Background(), channel, key, v.opts)
		if err != nil && b.node != nil {
			b.node.logger.log(newErrorLogEntry(err, "error in debounced publish", map[string]any{
				"channel": channel,
				"key":     key,
			}))
		}
	})
	return b
}

// Publish debounces publish if configured, otherwise passes through.
func (b *DebouncingMapBroker) Publish(ctx context.Context, ch string, key string, opts MapPublishOptions) (MapPublishResult, error) {
	if b.conf.Debounce == nil {
		return b.MapBroker.Publish(ctx, ch, key, opts)
	}
	d := b.conf.Debounce(ch)
	if d == 0 {
		return b.MapBroker.Publish(ctx, ch, key, opts)
	}
	b.debouncer.Debounce(ch, key, d, mapDebounceValue{opts: opts})
	return MapPublishResult{}, nil
}

// Remove cancels any pending debounced Publish and forwards to backend.
func (b *DebouncingMapBroker) Remove(ctx context.Context, ch string, key string, opts MapRemoveOptions) (MapPublishResult, error) {
	if b.conf.Debounce != nil && b.conf.Debounce(ch) > 0 {
		b.debouncer.Cancel(ch, key)
	}
	return b.MapBroker.Remove(ctx, ch, key, opts)
}

// Close stops all pending debounce timers and releases resources.
func (b *DebouncingMapBroker) Close(ctx context.Context) error {
	b.debouncer.Close()
	if closer, ok := b.MapBroker.(Closer); ok {
		return closer.Close(ctx)
	}
	return nil
}
