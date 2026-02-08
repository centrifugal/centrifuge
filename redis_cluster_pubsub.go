package centrifuge

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/redis/rueidis"
)

// topologyRefreshInterval is how often the topology refresh goroutine polls CLUSTER SLOTS.
const topologyRefreshInterval = 30 * time.Second

// slotRange represents a single Redis Cluster slot range assigned to a node.
type slotRange struct {
	start uint16
	end   uint16
	addr  string
}

// initNodeGroupedPubSub initializes per-node PubSub connection grouping.
// Called from NewRedisMapBroker when GroupPubSubByNode is true.
func (e *RedisMapBroker) initNodeGroupedPubSub(wrapper *brokerShardWrapper, shard *RedisShard) error {
	client := shard.client
	if e.conf.SubscribeOnReplica {
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

	partitionToNodeIdx, err := buildPartitionMapping(slotRanges, nm.addrToIdx, e.conf.NumShardedPubSubPartitions)
	if err != nil {
		return err
	}

	numNodes := len(nm.addrs)
	nodePartitions := buildNodePartitions(partitionToNodeIdx, numNodes)
	subClients, pubSubStartChannels := allocPubSubArrays(numNodes, e.conf.numSubscribeShards)

	wrapper.subClients = subClients
	wrapper.pubSubStartChannels = pubSubStartChannels
	wrapper.partitionToNodeIdx = partitionToNodeIdx
	wrapper.nodePartitions = nodePartitions
	wrapper.nodeClients = nm.nodeClients
	wrapper.topologyDone = make(chan struct{})
	wrapper.topologyRebuildCh = make(chan struct{}, 1)

	return nil
}

// runNodeGroupedPubSubShard launches per-node PubSub goroutines.
// Replaces runPubSubShard when GroupPubSubByNode is enabled.
func (e *RedisMapBroker) runNodeGroupedPubSubShard(s *brokerShardWrapper, h BrokerEventHandler) error {
	if e.conf.SkipPubSub {
		return nil
	}

	numNodes := len(s.nodeClients)
	s.eventHandler = h
	s.maxNodeGoroutines = numNodes

	for i := 0; i < numNodes; i++ {
		nodeIdx := i
		for j := 0; j < e.conf.numSubscribeShards; j++ {
			psShardIdx := j
			logFields := map[string]any{
				"shard":          s.shard.string(),
				"node_idx":       nodeIdx,
				"pub_sub_shard":  psShardIdx,
				"num_nodes":      numNodes,
				"pub_sub_shards": e.conf.numSubscribeShards,
				"node_grouped":   true,
			}
			go e.runForever(func() {
				select {
				case <-e.closeCh:
					return
				default:
				}
				e.runNodeGroupedPubSub(s, logFields, h, nodeIdx, psShardIdx, func(err error) {
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
	go e.runTopologyRefreshLoop(s)

	return nil
}

// runNodeGroupedPubSub is the per-node PubSub goroutine.
// Subscribes to shard channels for ALL partitions assigned to this node.
func (e *RedisMapBroker) runNodeGroupedPubSub(
	s *brokerShardWrapper,
	logFields map[string]any,
	eventHandler BrokerEventHandler,
	nodeIdx, psShardIdx int,
	startOnce func(error),
) {
	cb := pubSubCallbacks{
		handleMessage: func(isCluster bool, handler BrokerEventHandler, ch string, data []byte) error {
			return e.handleRedisClientMessage(isCluster, handler, ch, data)
		},
		shardChannelID: func(clusterIdx, psIdx int, sharded bool) string {
			return e.pubSubShardChannelID(clusterIdx, psIdx, sharded)
		},
		messageChannelID: func(ch string) string {
			return e.messageChannelID(s.shard, ch)
		},
		shardForChannel: func(ch string) *RedisShard {
			return e.getShard(ch).shard
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
		e.node,
		e.conf.Name,
		e.conf.numPubSubProcessors,
		e.conf.numResubscribeShards,
		e.conf.numSubscribeShards,
		e.conf.NumShardedPubSubPartitions,
		logFields,
		eventHandler,
		nodeIdx, psShardIdx,
		startOnce,
	)
}

// closeTopologyDone atomically closes the topologyDone channel and creates a new one.
func (e *RedisMapBroker) closeTopologyDone(s *brokerShardWrapper) {
	s.subClientsMu.Lock()
	defer s.subClientsMu.Unlock()
	select {
	case <-s.topologyDone:
		// Already closed.
	default:
		close(s.topologyDone)
	}
}

// runTopologyRefreshLoop periodically polls CLUSTER SLOTS and triggers a restart
// of all PubSub goroutines if the topology has changed. Also listens on
// topologyRebuildCh for immediate rebuild signals from sunsubscribe events.
func (e *RedisMapBroker) runTopologyRefreshLoop(s *brokerShardWrapper) {
	ticker := time.NewTicker(topologyRefreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-e.closeCh:
			return
		case <-s.shard.closeCh:
			return
		case <-s.topologyRebuildCh:
			e.node.logger.log(newLogEntry(LogLevelInfo, "sunsubscribe triggered topology rebuild", map[string]any{
				"shard": s.shard.string(),
			}))
			e.closeTopologyDone(s)
			// Wait briefly to let goroutines drain, then rebuild.
			time.Sleep(500 * time.Millisecond)
			e.rebuildNodeGroupedPubSub(s)
		case <-ticker.C:
			if e.refreshTopology(s) {
				e.node.logger.log(newLogEntry(LogLevelInfo, "topology changed, restarting node-grouped PubSub", map[string]any{
					"shard": s.shard.string(),
				}))
				e.closeTopologyDone(s)
				// Wait briefly to let goroutines drain, then rebuild.
				time.Sleep(500 * time.Millisecond)
				e.rebuildNodeGroupedPubSub(s)
			}
		}
	}
}

// pokeSlotOwners issues lightweight GET commands for keys hashing to slots owned
// by addresses unknown to the rueidis client. This triggers MOVED redirects, which
// cause rueidis to call lazyRefresh() and discover new cluster nodes. Without this,
// an idle cluster would never update the Nodes() map when nodes are added or removed.
func pokeSlotOwners(client rueidis.Client, slotRanges []slotRange, knownAddrs map[string]int) {
	poked := make(map[string]bool)
	for _, r := range slotRanges {
		if _, ok := knownAddrs[r.addr]; ok {
			continue
		}
		if poked[r.addr] {
			continue
		}
		poked[r.addr] = true
		// Issue a GET for a key in this slot range to trigger a MOVED redirect.
		slot := r.start
		key := "__centrifuge_topology_probe__{" + strconv.Itoa(int(slot)) + "}"
		_ = client.Do(context.Background(), client.B().Get().Key(key).Build()).Error()
	}
}

// refreshTopology re-queries CLUSTER SLOTS + Nodes(), rebuilds mappings.
// Returns true if the topology changed.
func (e *RedisMapBroker) refreshTopology(s *brokerShardWrapper) bool {
	client := s.shard.client
	if e.conf.SubscribeOnReplica {
		client = s.shard.replicaClient
	}

	slotRanges, err := queryClusterSlots(context.Background(), s.shard.client)
	if err != nil {
		if e.node.logEnabled(LogLevelError) {
			e.node.logger.log(newLogEntry(LogLevelError, "topology refresh: CLUSTER SLOTS error", map[string]any{
				"error": err.Error(),
			}))
		}
		return false
	}

	nm, err := buildNodeMapping(client)
	if err != nil {
		return false
	}

	partitionToNodeIdx, err := buildPartitionMapping(slotRanges, nm.addrToIdx, e.conf.NumShardedPubSubPartitions)
	if err != nil {
		pokeSlotOwners(s.shard.client, slotRanges, nm.addrToIdx)
		return false
	}

	s.subClientsMu.Lock()
	oldMapping := s.partitionToNodeIdx
	oldNodeCount := len(s.nodeClients)
	s.subClientsMu.Unlock()

	return topologyChanged(oldMapping, partitionToNodeIdx, oldNodeCount, len(nm.nodeClients))
}

// rebuildNodeGroupedPubSub rebuilds mappings and creates a new topologyDone channel
// so that restarted goroutines can use updated state.
func (e *RedisMapBroker) rebuildNodeGroupedPubSub(s *brokerShardWrapper) {
	client := s.shard.client
	if e.conf.SubscribeOnReplica {
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

	partitionToNodeIdx, err := buildPartitionMapping(slotRanges, nm.addrToIdx, e.conf.NumShardedPubSubPartitions)
	if err != nil {
		return
	}

	numNodes := len(nm.addrs)
	nodePartitions := buildNodePartitions(partitionToNodeIdx, numNodes)

	s.subClientsMu.Lock()
	if numNodes != len(s.subClients) {
		s.subClients, s.pubSubStartChannels = allocPubSubArrays(numNodes, e.conf.numSubscribeShards)
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
			for j := 0; j < e.conf.numSubscribeShards; j++ {
				psShardIdx := j
				logFields := map[string]any{
					"shard":          s.shard.string(),
					"node_idx":       nodeIdx,
					"pub_sub_shard":  psShardIdx,
					"num_nodes":      numNodes,
					"pub_sub_shards": e.conf.numSubscribeShards,
					"node_grouped":   true,
				}
				h := s.eventHandler
				go e.runForever(func() {
					select {
					case <-e.closeCh:
						return
					default:
					}
					e.runNodeGroupedPubSub(s, logFields, h, nodeIdx, psShardIdx, func(err error) {
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

// nodeMapping holds the result of building a stable node ordering from rueidis.Client.Nodes().
type nodeMapping struct {
	addrs       []string
	addrToIdx   map[string]int
	nodeClients []rueidis.Client
}

// buildNodeMapping queries the rueidis client for cluster nodes and returns a stable
// (sorted by address) mapping of node addresses to indices and per-node clients.
func buildNodeMapping(client rueidis.Client) (nodeMapping, error) {
	nodes := client.Nodes()
	if len(nodes) == 0 {
		return nodeMapping{}, fmt.Errorf("no cluster nodes returned by client")
	}
	addrs := make([]string, 0, len(nodes))
	for addr := range nodes {
		addrs = append(addrs, addr)
	}
	sort.Strings(addrs)
	addrToIdx := make(map[string]int, len(addrs))
	nodeClients := make([]rueidis.Client, len(addrs))
	for i, addr := range addrs {
		addrToIdx[addr] = i
		nodeClients[i] = nodes[addr]
	}
	return nodeMapping{addrs: addrs, addrToIdx: addrToIdx, nodeClients: nodeClients}, nil
}

// buildPartitionMapping maps each partition index to the node index that owns its hash slot.
// Returns an error if any slot owner is not found in addrToIdx.
func buildPartitionMapping(slotRanges []slotRange, addrToIdx map[string]int, numPartitions int) ([]int, error) {
	partitionToNodeIdx := make([]int, numPartitions)
	for i := 0; i < numPartitions; i++ {
		key := strconv.Itoa(i)
		slot := redisSlot("{" + key + "}")
		addr := findSlotOwner(slotRanges, slot)
		idx, ok := addrToIdx[addr]
		if !ok {
			return nil, fmt.Errorf("slot %d owner %q not found in Nodes() map", slot, addr)
		}
		partitionToNodeIdx[i] = idx
	}
	return partitionToNodeIdx, nil
}

// buildNodePartitions creates the reverse mapping: nodeIdx → list of partition indices.
func buildNodePartitions(partitionToNodeIdx []int, numNodes int) [][]int {
	nodePartitions := make([][]int, numNodes)
	for partIdx, nodeIdx := range partitionToNodeIdx {
		nodePartitions[nodeIdx] = append(nodePartitions[nodeIdx], partIdx)
	}
	return nodePartitions
}

// allocPubSubArrays creates 2D arrays for subClients and pubSubStartChannels
// dimensioned by [numNodes][numSubscribeShards].
func allocPubSubArrays(numNodes, numSubscribeShards int) ([][]rueidis.DedicatedClient, [][]*pubSubStart) {
	subClients := make([][]rueidis.DedicatedClient, numNodes)
	pubSubStartChannels := make([][]*pubSubStart, numNodes)
	for i := 0; i < numNodes; i++ {
		subClients[i] = make([]rueidis.DedicatedClient, numSubscribeShards)
		pubSubStartChannels[i] = make([]*pubSubStart, numSubscribeShards)
		for j := 0; j < numSubscribeShards; j++ {
			pubSubStartChannels[i][j] = &pubSubStart{errCh: make(chan error, 1)}
		}
	}
	return subClients, pubSubStartChannels
}

// topologyChanged compares old and new partition→node mappings and node counts.
// Returns true if the topology has changed.
func topologyChanged(oldMapping, newMapping []int, oldNodeCount, newNodeCount int) bool {
	if oldNodeCount != newNodeCount {
		return true
	}
	if len(oldMapping) != len(newMapping) {
		return true
	}
	for i, v := range newMapping {
		if oldMapping[i] != v {
			return true
		}
	}
	return false
}

// queryClusterSlots runs CLUSTER SLOTS and parses the result into slotRange entries.
func queryClusterSlots(ctx context.Context, client rueidis.Client) ([]slotRange, error) {
	resp, err := client.Do(ctx, client.B().ClusterSlots().Build()).ToArray()
	if err != nil {
		return nil, err
	}

	var ranges []slotRange
	for _, entry := range resp {
		arr, err := entry.ToArray()
		if err != nil || len(arr) < 3 {
			continue
		}
		start, err := arr[0].AsInt64()
		if err != nil {
			continue
		}
		end, err := arr[1].AsInt64()
		if err != nil {
			continue
		}

		// arr[2] is the master node info: [host, port, node-id, ...]
		nodeInfo, err := arr[2].ToArray()
		if err != nil || len(nodeInfo) < 2 {
			continue
		}
		host, err := nodeInfo[0].ToString()
		if err != nil {
			continue
		}
		port, err := nodeInfo[1].AsInt64()
		if err != nil {
			continue
		}

		addr := host + ":" + strconv.FormatInt(port, 10)
		ranges = append(ranges, slotRange{
			start: uint16(start),
			end:   uint16(end),
			addr:  addr,
		})
	}

	if len(ranges) == 0 {
		return nil, fmt.Errorf("no slot ranges found")
	}
	return ranges, nil
}

// findSlotOwner returns the address of the node owning the given slot.
// Returns empty string if not found.
func findSlotOwner(ranges []slotRange, slot uint16) string {
	for _, r := range ranges {
		if slot >= r.start && slot <= r.end {
			return r.addr
		}
	}
	return ""
}
