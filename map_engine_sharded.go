package centrifuge

import (
	"context"
	"errors"
)

// ShardedMapEngineConfig configures the sharded map engine wrapper.
type ShardedMapEngineConfig struct {
	// Shards is the list of map engine implementations to shard across.
	// At least one shard is required.
	Shards []MapEngine

	// ShardFunc determines which shard handles a channel.
	// If nil, DefaultShardFunc (Jump Consistent Hash) is used.
	ShardFunc ShardFunc
}

// ShardedMapEngine wraps multiple MapEngine instances with consistent sharding.
// All channel-scoped operations are routed to a single shard based on the channel name.
// This enables horizontal scaling across multiple map engine backends.
//
// Example usage:
//
//	// Shard across multiple Redis instances
//	redis1 := NewRedisMapEngine(node, RedisMapEngineConfig{Shards: []*RedisShard{shard1}})
//	redis2 := NewRedisMapEngine(node, RedisMapEngineConfig{Shards: []*RedisShard{shard2}})
//	engine := NewShardedMapEngine(ShardedMapEngineConfig{Shards: []MapEngine{redis1, redis2}})
//	node.SetMapEngine(engine)
type ShardedMapEngine struct {
	shards    []MapEngine
	shardFunc ShardFunc
}

var _ MapEngine = (*ShardedMapEngine)(nil)

// NewShardedMapEngine creates a new sharded map engine wrapper.
// Returns error if no shards are provided.
func NewShardedMapEngine(config ShardedMapEngineConfig) (*ShardedMapEngine, error) {
	if len(config.Shards) == 0 {
		return nil, errors.New("sharded map engine: at least one shard is required")
	}
	shardFunc := config.ShardFunc
	if shardFunc == nil {
		shardFunc = DefaultShardFunc
	}
	return &ShardedMapEngine{
		shards:    config.Shards,
		shardFunc: shardFunc,
	}, nil
}

// getShard returns the map engine shard for the given channel.
func (s *ShardedMapEngine) getShard(channel string) MapEngine {
	if len(s.shards) == 1 {
		return s.shards[0]
	}
	return s.shards[s.shardFunc(channel, len(s.shards))]
}

// mapEngineEventRegistrar is an interface for map engines that can register event handlers.
type mapEngineEventRegistrar interface {
	RegisterEventHandler(BrokerEventHandler) error
}

// RegisterEventHandler registers the event handler on ALL shards.
// This ensures events from any shard are delivered to the handler.
// Only shards that implement RegisterEventHandler will be registered.
func (s *ShardedMapEngine) RegisterEventHandler(h BrokerEventHandler) error {
	for _, shard := range s.shards {
		if registrar, ok := shard.(mapEngineEventRegistrar); ok {
			if err := registrar.RegisterEventHandler(h); err != nil {
				return err
			}
		}
	}
	return nil
}

// Subscribe subscribes to a channel on the appropriate shard.
func (s *ShardedMapEngine) Subscribe(ch string) error {
	return s.getShard(ch).Subscribe(ch)
}

// Unsubscribe unsubscribes from a channel on the appropriate shard.
func (s *ShardedMapEngine) Unsubscribe(ch string) error {
	return s.getShard(ch).Unsubscribe(ch)
}

// Publish publishes to a channel on the appropriate shard.
func (s *ShardedMapEngine) Publish(ctx context.Context, ch string, key string, opts MapPublishOptions) (MapPublishResult, error) {
	return s.getShard(ch).Publish(ctx, ch, key, opts)
}

// Remove removes a key from a channel on the appropriate shard.
func (s *ShardedMapEngine) Remove(ctx context.Context, ch string, key string, opts MapRemoveOptions) (MapPublishResult, error) {
	return s.getShard(ch).Remove(ctx, ch, key, opts)
}

// ReadStream reads the stream from the appropriate shard.
func (s *ShardedMapEngine) ReadStream(ctx context.Context, ch string, opts MapReadStreamOptions) ([]*Publication, StreamPosition, error) {
	return s.getShard(ch).ReadStream(ctx, ch, opts)
}

// ReadState reads the state from the appropriate shard.
func (s *ShardedMapEngine) ReadState(ctx context.Context, ch string, opts MapReadStateOptions) ([]*Publication, StreamPosition, string, error) {
	return s.getShard(ch).ReadState(ctx, ch, opts)
}

// Stats returns stats from the appropriate shard.
func (s *ShardedMapEngine) Stats(ctx context.Context, ch string) (MapStats, error) {
	return s.getShard(ch).Stats(ctx, ch)
}

// Clear clears data from the appropriate shard.
func (s *ShardedMapEngine) Clear(ctx context.Context, ch string, opts MapClearOptions) error {
	return s.getShard(ch).Clear(ctx, ch, opts)
}

// Close closes all shards. Implements the Closer interface.
func (s *ShardedMapEngine) Close(ctx context.Context) error {
	var firstErr error
	for _, shard := range s.shards {
		if closer, ok := shard.(Closer); ok {
			if err := closer.Close(ctx); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}
