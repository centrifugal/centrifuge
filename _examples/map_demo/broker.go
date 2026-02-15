package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/centrifugal/centrifuge"
)

// setupMapBroker creates either a memory, Redis, or PostgreSQL map broker.
// When enableCache is true, wraps Redis/Postgres brokers with CachedMapBroker
// for read-your-own-writes consistency and low-latency reads.
func setupMapBroker(node *centrifuge.Node, redisAddr, postgresAddr string, enableCache bool) (centrifuge.MapBroker, error) {
	var backend centrifuge.MapBroker

	// PostgreSQL takes priority if specified
	if postgresAddr != "" {
		pgConfig := centrifuge.PostgresMapBrokerConfig{
			ConnString: postgresAddr,
		}

		// If Redis is also specified, use it as broker for multi-node fan-out
		if redisAddr != "" {
			log.Printf("Using PostgreSQL map broker with Redis broker for fan-out")
			redisShard, err := centrifuge.NewRedisShard(node, centrifuge.RedisShardConfig{
				Address: redisAddr,
			})
			if err != nil {
				return nil, fmt.Errorf("error creating Redis shard for broker: %w", err)
			}

			broker, err := centrifuge.NewRedisBroker(node, centrifuge.RedisBrokerConfig{
				Shards: []*centrifuge.RedisShard{redisShard},
				Prefix: "map_demo",
			})
			if err != nil {
				return nil, fmt.Errorf("error creating Redis broker: %w", err)
			}
			pgConfig.Broker = broker
		} else {
			log.Printf("Using PostgreSQL map broker (single-node, local delivery only)")
		}

		broker, err := centrifuge.NewPostgresMapBroker(node, pgConfig)
		if err != nil {
			return nil, fmt.Errorf("error creating PostgreSQL map broker: %w", err)
		}
		if err := broker.EnsureSchema(context.Background()); err != nil {
			return nil, fmt.Errorf("error ensuring PostgreSQL schema: %w", err)
		}
		backend = broker
	} else if redisAddr != "" {
		// Redis if specified
		log.Printf("Using Redis map broker at %s", redisAddr)

		redisShard, err := centrifuge.NewRedisShard(node, centrifuge.RedisShardConfig{
			Address: redisAddr,
		})
		if err != nil {
			return nil, fmt.Errorf("error creating Redis shard: %w", err)
		}

		broker, err := centrifuge.NewRedisMapBroker(node, centrifuge.RedisMapBrokerConfig{
			Shards: []*centrifuge.RedisShard{redisShard},
			Prefix: "map_demo",
		})
		if err != nil {
			return nil, err
		}
		backend = broker
	} else {
		// Default to memory - cache not applicable (already in-memory)
		log.Println("Using in-memory map broker")
		broker, err := centrifuge.NewMemoryMapBroker(node, centrifuge.MemoryMapBrokerConfig{})
		if err != nil {
			return nil, err
		}
		return broker, nil
	}

	// Wrap with cache layer if enabled (only for Redis/Postgres)
	if enableCache {
		log.Println("Enabling memory cache layer")
		cached, err := centrifuge.NewCachedMapBroker(node, backend, centrifuge.CachedMapBrokerConfig{
			Cache: centrifuge.MapCacheConfig{
				MaxChannels:        10000,
				ChannelIdleTimeout: 5 * time.Minute,
				StreamSize:         1000,
			},
			SyncInterval:  10000 * time.Millisecond,
			SyncBatchSize: 1000,
		})
		if err != nil {
			return nil, fmt.Errorf("error creating cached map broker: %w", err)
		}
		return cached, nil
	}

	return backend, nil
}
