package main

import (
	"fmt"
	"log"

	"github.com/centrifugal/centrifuge"
)

// setupMapBroker creates either a memory or Redis map broker based on the
// --redis flag.
func setupMapBroker(node *centrifuge.Node, redisAddr string) (centrifuge.MapBroker, error) {
	if redisAddr != "" {
		shard, err := centrifuge.NewRedisShard(node, centrifuge.RedisShardConfig{
			Address: redisAddr,
		})
		if err != nil {
			return nil, fmt.Errorf("create Redis shard: %w", err)
		}
		broker, err := centrifuge.NewRedisMapBroker(node, centrifuge.RedisMapBrokerConfig{
			Shards: []*centrifuge.RedisShard{shard},
			Prefix: "map_massive",
		})
		if err != nil {
			return nil, err
		}
		log.Printf("Map broker: Redis at %s", redisAddr)
		return broker, nil
	}

	broker, err := centrifuge.NewMemoryMapBroker(node, centrifuge.MemoryMapBrokerConfig{})
	if err != nil {
		return nil, err
	}
	log.Printf("Map broker: in-memory")
	return broker, nil
}
