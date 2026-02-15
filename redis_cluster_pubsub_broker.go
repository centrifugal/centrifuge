package centrifuge

import (
	"context"
	"fmt"
	"time"
)

// initBrokerNodeGroupedPubSub initializes per-node PubSub connection grouping for RedisBroker.
// Called from NewRedisBroker when GroupPubSubByNode is true.
func (b *RedisBroker) initBrokerNodeGroupedPubSub(wrapper *shardWrapper, shard *RedisShard) error {
	client := shard.client
	if b.config.SubscribeOnReplica {
		client = shard.replicaClient
	}

	nm, err := buildNodeMapping(client)
	if err != nil {
		return err
	}

	slotRanges, err := queryClusterSlots(context.Background(), shard.client)
	if err != nil {
		return fmt.Errorf("CLUSTER SLOTS: %w", err)
	}

	partitionToNodeIdx, err := buildPartitionMapping(slotRanges, nm.addrToIdx, b.config.NumShardedPubSubPartitions)
	if err != nil {
		return err
	}

	numNodes := len(nm.addrs)
	nodePartitions := buildNodePartitions(partitionToNodeIdx, numNodes)
	subClients, pubSubStartChannels := allocPubSubArrays(numNodes, b.config.numSubscribeShards)

	wrapper.subClients = subClients
	wrapper.pubSubStartChannels = pubSubStartChannels
	wrapper.partitionToNodeIdx = partitionToNodeIdx
	wrapper.nodePartitions = nodePartitions
	wrapper.nodeClients = nm.nodeClients
	wrapper.topologyDone = make(chan struct{})
	wrapper.topologyRebuildCh = make(chan struct{}, 1)

	return nil
}

// runBrokerNodeGroupedPubSubShard launches per-node PubSub goroutines for RedisBroker.
// Replaces runPubSubShard when GroupPubSubByNode is enabled.
func (b *RedisBroker) runBrokerNodeGroupedPubSubShard(s *shardWrapper, h BrokerEventHandler) error {
	if b.config.SkipPubSub {
		return nil
	}

	numNodes := len(s.nodeClients)
	s.eventHandler = h
	s.maxNodeGoroutines = numNodes

	for i := 0; i < numNodes; i++ {
		nodeIdx := i
		for j := 0; j < b.config.numSubscribeShards; j++ {
			psShardIdx := j
			logFields := getBaseLogFields(s)
			logFields["node_idx"] = nodeIdx
			logFields["pub_sub_shard"] = psShardIdx
			logFields["num_nodes"] = numNodes
			logFields["pub_sub_shards"] = b.config.numSubscribeShards
			logFields["node_grouped"] = true
			go b.runForever(func() {
				select {
				case <-b.closeCh:
					return
				default:
				}
				b.runBrokerNodeGroupedPubSub(s, logFields, h, nodeIdx, psShardIdx, func(err error) {
					s.subClientsMu.Lock()
					if nodeIdx < len(s.pubSubStartChannels) {
						s.pubSubStartChannels[nodeIdx][psShardIdx].once.Do(func() {
							s.pubSubStartChannels[nodeIdx][psShardIdx].errCh <- err
						})
					}
					s.subClientsMu.Unlock()
				})
			})
		}
	}

	// Start topology refresh goroutine.
	go b.runBrokerTopologyRefreshLoop(s)

	return nil
}

// runBrokerNodeGroupedPubSub is the per-node PubSub goroutine for RedisBroker.
// Subscribes to shard channels for ALL partitions assigned to this node.
func (b *RedisBroker) runBrokerNodeGroupedPubSub(
	s *shardWrapper,
	logFields map[string]any,
	eventHandler BrokerEventHandler,
	nodeIdx, psShardIdx int,
	startOnce func(error),
) {
	cb := pubSubCallbacks{
		handleMessage: func(isCluster bool, handler BrokerEventHandler, ch string, data []byte) error {
			return b.handleRedisClientMessage(isCluster, handler, channelID(ch), data)
		},
		shardChannelID: func(clusterIdx, psIdx int, sharded bool) string {
			return string(b.pubSubShardChannelID(clusterIdx, psIdx, sharded))
		},
		messageChannelID: func(ch string) string {
			return string(b.messageChannelID(s.shard, ch))
		},
		shardForChannel: func(ch string) *RedisShard {
			return b.getShard(ch).shard
		},
	}
	runNodeGroupedPubSubLoop(
		s.shard,
		&s.subClientsMu,
		s.subClients,
		s.nodeClients,
		s.nodePartitions,
		s.topologyDone,
		s.topologyRebuildCh,
		cb,
		b.node,
		b.config.Name,
		b.config.numPubSubProcessors,
		b.config.numResubscribeShards,
		b.config.numSubscribeShards,
		b.config.NumShardedPubSubPartitions,
		logFields,
		eventHandler,
		nodeIdx, psShardIdx,
		startOnce,
	)
}

// closeBrokerTopologyDone atomically closes the topologyDone channel.
func (b *RedisBroker) closeBrokerTopologyDone(s *shardWrapper) {
	s.subClientsMu.Lock()
	defer s.subClientsMu.Unlock()
	select {
	case <-s.topologyDone:
		// Already closed.
	default:
		close(s.topologyDone)
	}
}

// runBrokerTopologyRefreshLoop periodically polls CLUSTER SLOTS and triggers a restart
// of all PubSub goroutines if the topology has changed. Also listens on
// topologyRebuildCh for immediate rebuild signals from sunsubscribe events.
func (b *RedisBroker) runBrokerTopologyRefreshLoop(s *shardWrapper) {
	ticker := time.NewTicker(topologyRefreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-b.closeCh:
			return
		case <-s.shard.closeCh:
			return
		case <-s.topologyRebuildCh:
			b.node.logger.log(newLogEntry(LogLevelInfo, "sunsubscribe triggered topology rebuild (broker)", map[string]any{
				"shard": s.shard.string(),
			}))
			b.closeBrokerTopologyDone(s)
			// Wait briefly to let goroutines drain, then rebuild.
			time.Sleep(500 * time.Millisecond)
			b.rebuildBrokerNodeGroupedPubSub(s)
		case <-ticker.C:
			if b.refreshBrokerTopology(s) {
				b.node.logger.log(newLogEntry(LogLevelInfo, "topology changed, restarting node-grouped PubSub (broker)", map[string]any{
					"shard": s.shard.string(),
				}))
				b.closeBrokerTopologyDone(s)
				// Wait briefly to let goroutines drain, then rebuild.
				time.Sleep(500 * time.Millisecond)
				b.rebuildBrokerNodeGroupedPubSub(s)
			}
		}
	}
}

// refreshBrokerTopology re-queries CLUSTER SLOTS + Nodes(), rebuilds mappings.
// Returns true if the topology changed.
func (b *RedisBroker) refreshBrokerTopology(s *shardWrapper) bool {
	client := s.shard.client
	if b.config.SubscribeOnReplica {
		client = s.shard.replicaClient
	}

	slotRanges, err := queryClusterSlots(context.Background(), s.shard.client)
	if err != nil {
		if b.node.logEnabled(LogLevelError) {
			b.node.logger.log(newLogEntry(LogLevelError, "topology refresh: CLUSTER SLOTS error", map[string]any{
				"error": err.Error(),
			}))
		}
		return false
	}

	nm, err := buildNodeMapping(client)
	if err != nil {
		return false
	}

	partitionToNodeIdx, err := buildPartitionMapping(slotRanges, nm.addrToIdx, b.config.NumShardedPubSubPartitions)
	if err != nil {
		pokeSlotOwners(s.shard.client, slotRanges, nm.addrToIdx, b.config.Prefix)
		return false
	}

	s.subClientsMu.Lock()
	oldMapping := s.partitionToNodeIdx
	oldNodeCount := len(s.nodeClients)
	s.subClientsMu.Unlock()

	return topologyChanged(oldMapping, partitionToNodeIdx, oldNodeCount, len(nm.nodeClients))
}

// rebuildBrokerNodeGroupedPubSub rebuilds mappings and creates a new topologyDone channel
// so that restarted goroutines can use updated state.
func (b *RedisBroker) rebuildBrokerNodeGroupedPubSub(s *shardWrapper) {
	client := s.shard.client
	if b.config.SubscribeOnReplica {
		client = s.shard.replicaClient
	}

	nm, err := buildNodeMapping(client)
	if err != nil {
		return
	}

	slotRanges, err := queryClusterSlots(context.Background(), s.shard.client)
	if err != nil {
		return
	}

	partitionToNodeIdx, err := buildPartitionMapping(slotRanges, nm.addrToIdx, b.config.NumShardedPubSubPartitions)
	if err != nil {
		return
	}

	numNodes := len(nm.addrs)
	nodePartitions := buildNodePartitions(partitionToNodeIdx, numNodes)

	s.subClientsMu.Lock()
	if numNodes != len(s.subClients) {
		s.subClients, s.pubSubStartChannels = allocPubSubArrays(numNodes, b.config.numSubscribeShards)
	}

	s.partitionToNodeIdx = partitionToNodeIdx
	s.nodePartitions = nodePartitions
	s.nodeClients = nm.nodeClients
	s.topologyDone = make(chan struct{})
	s.subClientsMu.Unlock()

	// Launch new goroutines for added nodes.
	if numNodes > s.maxNodeGoroutines && s.eventHandler != nil {
		for i := s.maxNodeGoroutines; i < numNodes; i++ {
			nodeIdx := i
			for j := 0; j < b.config.numSubscribeShards; j++ {
				psShardIdx := j
				logFields := getBaseLogFields(s)
				logFields["node_idx"] = nodeIdx
				logFields["pub_sub_shard"] = psShardIdx
				logFields["num_nodes"] = numNodes
				logFields["pub_sub_shards"] = b.config.numSubscribeShards
				logFields["node_grouped"] = true
				h := s.eventHandler
				go b.runForever(func() {
					select {
					case <-b.closeCh:
						return
					default:
					}
					b.runBrokerNodeGroupedPubSub(s, logFields, h, nodeIdx, psShardIdx, func(err error) {
						s.subClientsMu.Lock()
						if nodeIdx < len(s.pubSubStartChannels) {
							s.pubSubStartChannels[nodeIdx][psShardIdx].once.Do(func() {
								s.pubSubStartChannels[nodeIdx][psShardIdx].errCh <- err
							})
						}
						s.subClientsMu.Unlock()
					})
				})
			}
		}
		s.maxNodeGoroutines = numNodes
	}
}
