package centrifuge

import (
	"context"
	"errors"
)

// ShardedPresenceManagerConfig configures the sharded presence manager wrapper.
type ShardedPresenceManagerConfig struct {
	// Shards is the list of presence manager implementations to shard across.
	// At least one shard is required.
	Shards []PresenceManager

	// ShardFunc determines which shard handles a channel.
	// If nil, DefaultShardFunc (Jump Consistent Hash) is used.
	ShardFunc ShardFunc
}

// ShardedPresenceManager wraps multiple PresenceManager instances with consistent sharding.
// All channel-scoped operations are routed to a single shard based on the channel name.
// This enables horizontal scaling across multiple presence backends.
//
// Example usage:
//
//	// Shard across multiple Redis instances
//	redis1 := NewRedisPresenceManager(node, RedisPresenceManagerConfig{Shards: []*RedisShard{shard1}})
//	redis2 := NewRedisPresenceManager(node, RedisPresenceManagerConfig{Shards: []*RedisShard{shard2}})
//	pm := NewShardedPresenceManager(ShardedPresenceManagerConfig{Shards: []PresenceManager{redis1, redis2}})
//	node.SetPresenceManager(pm)
type ShardedPresenceManager struct {
	shards    []PresenceManager
	shardFunc ShardFunc
}

var _ PresenceManager = (*ShardedPresenceManager)(nil)

// NewShardedPresenceManager creates a new sharded presence manager wrapper.
// Returns error if no shards are provided.
func NewShardedPresenceManager(config ShardedPresenceManagerConfig) (*ShardedPresenceManager, error) {
	if len(config.Shards) == 0 {
		return nil, errors.New("sharded presence manager: at least one shard is required")
	}
	shardFunc := config.ShardFunc
	if shardFunc == nil {
		shardFunc = DefaultShardFunc
	}
	return &ShardedPresenceManager{
		shards:    config.Shards,
		shardFunc: shardFunc,
	}, nil
}

// getShard returns the presence manager shard for the given channel.
func (s *ShardedPresenceManager) getShard(channel string) PresenceManager {
	if len(s.shards) == 1 {
		return s.shards[0]
	}
	return s.shards[s.shardFunc(channel, len(s.shards))]
}

// Presence returns presence information for a channel from the appropriate shard.
func (s *ShardedPresenceManager) Presence(ch string) (map[string]*ClientInfo, error) {
	return s.getShard(ch).Presence(ch)
}

// PresenceStats returns presence stats for a channel from the appropriate shard.
func (s *ShardedPresenceManager) PresenceStats(ch string) (PresenceStats, error) {
	return s.getShard(ch).PresenceStats(ch)
}

// AddPresence adds presence information on the appropriate shard.
func (s *ShardedPresenceManager) AddPresence(ch string, clientID string, info *ClientInfo) error {
	return s.getShard(ch).AddPresence(ch, clientID, info)
}

// RemovePresence removes presence information from the appropriate shard.
func (s *ShardedPresenceManager) RemovePresence(ch string, clientID string, userID string) error {
	return s.getShard(ch).RemovePresence(ch, clientID, userID)
}

// Close closes all shards. Implements the Closer interface.
func (s *ShardedPresenceManager) Close(ctx context.Context) error {
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
