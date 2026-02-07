package centrifuge

import (
	"context"
	"errors"
)

// ShardedBrokerConfig configures the sharded broker wrapper.
type ShardedBrokerConfig struct {
	// Shards is the list of broker implementations to shard across.
	// At least one shard is required.
	Shards []Broker

	// ShardFunc determines which shard handles a channel.
	// If nil, DefaultShardFunc (Jump Consistent Hash) is used.
	ShardFunc ShardFunc
}

// ShardedBroker wraps multiple Broker instances with consistent sharding.
// All channel-scoped operations are routed to a single shard based on the channel name.
// This enables horizontal scaling across multiple broker backends.
//
// Example usage:
//
//	// Shard across multiple Redis instances
//	redis1 := NewRedisBroker(node, RedisBrokerConfig{Shards: []*RedisShard{shard1}})
//	redis2 := NewRedisBroker(node, RedisBrokerConfig{Shards: []*RedisShard{shard2}})
//	broker := NewShardedBroker(ShardedBrokerConfig{Shards: []Broker{redis1, redis2}})
//	node.SetBroker(broker)
type ShardedBroker struct {
	shards    []Broker
	shardFunc ShardFunc
}

var _ Broker = (*ShardedBroker)(nil)

// NewShardedBroker creates a new sharded broker wrapper.
// Returns error if no shards are provided.
func NewShardedBroker(config ShardedBrokerConfig) (*ShardedBroker, error) {
	if len(config.Shards) == 0 {
		return nil, errors.New("sharded broker: at least one shard is required")
	}
	shardFunc := config.ShardFunc
	if shardFunc == nil {
		shardFunc = DefaultShardFunc
	}
	return &ShardedBroker{
		shards:    config.Shards,
		shardFunc: shardFunc,
	}, nil
}

// getShard returns the broker shard for the given channel.
func (s *ShardedBroker) getShard(channel string) Broker {
	if len(s.shards) == 1 {
		return s.shards[0]
	}
	return s.shards[s.shardFunc(channel, len(s.shards))]
}

// RegisterBrokerEventHandler registers the event handler on ALL shards.
// This ensures events from any shard are delivered to the handler.
func (s *ShardedBroker) RegisterBrokerEventHandler(h BrokerEventHandler) error {
	for _, shard := range s.shards {
		if err := shard.RegisterBrokerEventHandler(h); err != nil {
			return err
		}
	}
	return nil
}

// Subscribe subscribes to a channel on the appropriate shard.
func (s *ShardedBroker) Subscribe(ch string) error {
	return s.getShard(ch).Subscribe(ch)
}

// Unsubscribe unsubscribes from a channel on the appropriate shard.
func (s *ShardedBroker) Unsubscribe(ch string) error {
	return s.getShard(ch).Unsubscribe(ch)
}

// Publish publishes to a channel on the appropriate shard.
func (s *ShardedBroker) Publish(ch string, data []byte, opts PublishOptions) (StreamPosition, bool, error) {
	return s.getShard(ch).Publish(ch, data, opts)
}

// PublishJoin publishes a join event on the appropriate shard.
func (s *ShardedBroker) PublishJoin(ch string, info *ClientInfo) error {
	return s.getShard(ch).PublishJoin(ch, info)
}

// PublishLeave publishes a leave event on the appropriate shard.
func (s *ShardedBroker) PublishLeave(ch string, info *ClientInfo) error {
	return s.getShard(ch).PublishLeave(ch, info)
}

// History retrieves history from the appropriate shard.
func (s *ShardedBroker) History(ch string, opts HistoryOptions) ([]*Publication, StreamPosition, error) {
	return s.getShard(ch).History(ch, opts)
}

// RemoveHistory removes history from the appropriate shard.
func (s *ShardedBroker) RemoveHistory(ch string) error {
	return s.getShard(ch).RemoveHistory(ch)
}

// Close closes all shards. Implements the Closer interface.
func (s *ShardedBroker) Close(ctx context.Context) error {
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
