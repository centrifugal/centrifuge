# KeyedEngine In-Memory Architecture with NATS

> **Note**: This document focuses specifically on KeyedEngine implementation. For a unified architecture covering **KeyedEngine, Broker, and PresenceManager**, see [distributed_in_memory_architecture.md](distributed_in_memory_architecture.md).

## Architecture Overview

```
┌─────────────────────────────────────────────┐
│         Load Balancer                       │
└──────────────┬──────────────────────────────┘
               │
       ┌───────┴────────┐
       │                │
┌──────▼─────┐    ┌────▼───────┐
│Connection  │    │Connection  │  STATELESS
│  Node 1    │    │  Node 2    │  - Auto-scale
│            │    │            │  - Rollout anytime
│ [HashRing] │    │ [HashRing] │  - CPU-optimized
│            │    │            │
│ Shards:    │    │ Shards:    │  - N shards per node
│  shard 0   │    │  shard 0   │  - Load distribution
│  shard 1   │    │  shard 1   │  - Fixed subscriptions
│  ...       │    │  ...       │
│  shard 15  │    │  shard 15  │
└──────┬─────┘    └────┬───────┘
       │               │
       └───────┬───────┘
               │
       ┌───────▼────────┐
       │                │
       │  NATS Server   │  COMMUNICATION LAYER
       │                │  - Request-reply for ops
       │  Subjects:     │  - Pub/Sub for updates
       │  data.0.*      │  - Ordering guarantees
       │  data.1.*      │  - ReconnectBufSize(-1)
       │  data.2.*      │
       │                │  - Targeted updates
       │  conn.abc.0    │  - conn.<node-id>.<shard>
       │  conn.abc.1    │  - Only interested data
       │  conn.xyz.0    │
       └───────┬────────┘
               │
       ┌───────┴────────┐
       │                │
┌──────▼─────┐    ┌────▼───────┐
│Data Node 0 │    │Data Node 1 │  STATEFUL
│(Memory)    │    │(Memory)    │  - StatefulSet
│Channels:   │    │Channels:   │  - Memory-optimized
│0,3,6,9...  │    │1,4,7,10... │  - Stable identity
│            │    │            │  - Rare rollouts
│Listen:     │    │Listen:     │
│data.0.*    │    │data.1.*    │
│            │    │            │  - Track subscribers
│Subscribers:│    │Subscribers:│  - with shard info
│{abc: 16}   │    │{xyz: 16}   │  - (nodeID: numShards)
└────────────┘    └────────────┘
```

## Why NATS Instead of gRPC?

### Advantages
1. **No Connection Management**: Connection nodes don't maintain gRPC client pools
2. **Service Discovery Free**: Data nodes announce via NATS subjects
3. **Built-in Load Balancing**: NATS queue groups for high availability
4. **Automatic Reconnection**: NATS handles reconnects with ordering guarantees
5. **PubSub Built-in**: Same infrastructure for updates and RPCs
6. **Simpler Code**: No protobuf, no server management
7. **Targeted Updates**: Each connection node receives only interested data

### Trade-offs
- Slightly higher latency (~1-2ms vs direct gRPC)
- Requires NATS cluster (but simpler than service mesh)

## Connection Node Sharding

### Why Shard Connection Nodes?

**Problem**: Without sharding, each connection node would need either:
- 1 subscription (no load distribution)
- 1000s of subscriptions (one per active channel, high overhead)

**Solution**: Fixed N subscriptions per connection node for load distribution

### Sharding Benefits

```
Connection Node with 10,000 active channels and 16 shards:
├─ Shard 0:  ~625 channels (hash % 16 = 0)
├─ Shard 1:  ~625 channels (hash % 16 = 1)
├─ ...
└─ Shard 15: ~625 channels (hash % 16 = 15)

NATS Subscriptions: 16 (constant, regardless of channel count)
Memory overhead: ~16KB for subscriptions
Load distribution: Perfect (hash-based)
```

## Consistent Hashing for Minimal Resharding

### Hash Ring Implementation

```go
package engine

import (
	"hash/crc32"
	"sort"
	"sync"
)

// ConsistentHash provides consistent hashing with minimal key movement on scaling
type ConsistentHash struct {
	mu           sync.RWMutex
	ring         []uint32          // Sorted hash values
	nodeMap      map[uint32]string // Hash → NodeID
	vnodes       int               // Virtual nodes per physical node (default: 150)
	nodes        []string          // Current node list
}

func NewConsistentHash(vnodes int) *ConsistentHash {
	if vnodes == 0 {
		vnodes = 150 // Good balance between distribution and lookup speed
	}
	return &ConsistentHash{
		nodeMap: make(map[uint32]string),
		vnodes:  vnodes,
		ring:    make([]uint32, 0),
	}
}

// UpdateNodes updates the hash ring when data nodes scale
// Returns channels that need to be migrated
func (ch *ConsistentHash) UpdateNodes(newNodes []string) map[string]Migration {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	oldNodes := ch.nodes

	// Build new ring
	newRing := make([]uint32, 0, len(newNodes)*ch.vnodes)
	newNodeMap := make(map[uint32]string, len(newNodes)*ch.vnodes)

	for _, node := range newNodes {
		for i := 0; i < ch.vnodes; i++ {
			// Hash: "node-0:0", "node-0:1", ..., "node-0:149"
			key := node + ":" + strconv.Itoa(i)
			hash := crc32.ChecksumIEEE([]byte(key))
			newRing = append(newRing, hash)
			newNodeMap[hash] = node
		}
	}

	sort.Slice(newRing, func(i, j int) bool {
		return newRing[i] < newRing[j]
	})

	// Calculate migrations (only for channels that moved)
	migrations := ch.calculateMigrations(oldNodes, newNodes, newRing, newNodeMap)

	ch.ring = newRing
	ch.nodeMap = newNodeMap
	ch.nodes = newNodes

	return migrations
}

type Migration struct {
	Channel string
	From    string // Old data node
	To      string // New data node
}

func (ch *ConsistentHash) calculateMigrations(
	oldNodes, newNodes []string,
	newRing []uint32,
	newNodeMap map[uint32]string,
) map[string]Migration {
	// This would be called with a sample of known channels
	// or connection nodes track which channels they've seen
	migrations := make(map[string]Migration)

	// For each channel we know about, check if it moved
	// This is populated by connection nodes reporting active channels
	return migrations
}

// GetNode returns the data node responsible for a channel
// This is O(log N) with binary search
func (ch *ConsistentHash) GetNode(channel string) string {
	ch.mu.RLock()
	defer ch.mu.RUnlock()

	if len(ch.ring) == 0 {
		return ""
	}

	// Hash the channel
	hash := crc32.ChecksumIEEE([]byte(channel))

	// Binary search for the first node with hash >= channel hash
	idx := sort.Search(len(ch.ring), func(i int) bool {
		return ch.ring[i] >= hash
	})

	// Wrap around if we're past the end
	if idx == len(ch.ring) {
		idx = 0
	}

	return ch.nodeMap[ch.ring[idx]]
}

// GetNodeIndex returns node index (0, 1, 2, ...) from node ID
func (ch *ConsistentHash) GetNodeIndex(nodeID string) int {
	ch.mu.RLock()
	defer ch.mu.RUnlock()

	for i, node := range ch.nodes {
		if node == nodeID {
			return i
		}
	}
	return -1
}
```

### Consistent Hashing Properties

With 3 nodes → 4 nodes scaling:
- **Keys moved**: ~25% (only keys in the new node's range)
- **Keys stable**: ~75% (stay on same nodes)
- **Lookup time**: O(log N) = O(log 450) ≈ 9 comparisons for 3 nodes * 150 vnodes

With 150 virtual nodes per physical node:
- **Standard deviation**: <5% (excellent distribution)
- **Memory overhead**: ~7KB for 3 nodes (negligible)

## NATS Subject Design

### Subject Hierarchy

```
data.<node-index>.<operation>
│    │            │
│    │            └─ Operation: subscribe, publish, snapshot, stream, etc.
│    └─ Node index: 0, 1, 2, ...
└─ Namespace: data nodes

conn.<node-id>.<shard>
│    │         │
│    │         └─ Shard index: 0-15 (or 0-N)
│    └─ Connection node ID
└─ Namespace: connection nodes

Examples:
- data.0.subscribe     → Request-reply for subscribe on data node 0
- data.1.publish       → Request-reply for publish on data node 1
- data.2.snapshot      → Request-reply for snapshot on data node 2
- conn.abc123.0        → Updates for connection node abc123, shard 0
- conn.abc123.7        → Updates for connection node abc123, shard 7
- conn.xyz789.15       → Updates for connection node xyz789, shard 15
```

### Why This Design?

1. **data.<node-index>.***: Each data node listens to its own prefix
2. **Request-reply pattern**: Connection nodes send requests, data nodes reply
3. **conn.<node-id>.<shard>**: Targeted updates to specific connection node shards
4. **No wildcards on hot path**: Exact subject matching for performance
5. **Connection nodes receive ONLY interested data**: No filtering needed

## Connection Node Implementation

### Structure

```go
type ConnectionNode struct {
	nodeID     string
	numShards  int  // e.g., 16 shards per connection node

	// Shard handlers - each handles a subset of channels
	shards     []*ConnectionShard

	nats       *nats.Conn
	hashRing   *ConsistentHash  // For data node selection
}

type ConnectionShard struct {
	index         int
	channels      map[string]*ChannelSubscribers  // channels this shard handles
	mu            sync.RWMutex
}
```

### Startup: Create N NATS Subscriptions

```go
func (c *ConnectionNode) Start() error {
	// Create N shards
	c.shards = make([]*ConnectionShard, c.numShards)
	for i := 0; i < c.numShards; i++ {
		c.shards[i] = &ConnectionShard{
			index:    i,
			channels: make(map[string]*ChannelSubscribers),
		}
	}

	// Subscribe to N subjects (one per shard)
	// This is FIXED - doesn't grow with channel count
	for i := 0; i < c.numShards; i++ {
		subject := fmt.Sprintf("conn.%s.%d", c.nodeID, i)
		shardIndex := i  // Capture for closure

		_, err := c.nats.Subscribe(subject, func(msg *nats.Msg) {
			c.shards[shardIndex].handleUpdate(msg)
		})
		if err != nil {
			return err
		}

		log.Printf("Connection node %s: subscribed to %s", c.nodeID, subject)
	}

	log.Printf("Connection node %s started with %d shards", c.nodeID, c.numShards)
	return nil
}

// Determine which shard handles this channel (locally)
func (c *ConnectionNode) getShardForChannel(channel string) int {
	hash := crc32.ChecksumIEEE([]byte(channel))
	return int(hash % uint32(c.numShards))
}
```

### Subscribe: Tell Data Node About Shards

```go
func (c *ConnectionNode) Subscribe(ctx context.Context, channel string) (*SubscribeResponse, error) {
	// 1. Find which data node owns this channel
	dataNodeID := c.hashRing.GetNode(channel)
	nodeIndex := c.hashRing.GetNodeIndex(dataNodeID)

	// 2. Find which local shard handles this channel
	shardIndex := c.getShardForChannel(channel)

	// 3. Send subscribe request to data node
	subject := fmt.Sprintf("data.%d.subscribe", nodeIndex)

	req := &SubscribeRequest{
		Channel:         channel,
		ConnNodeID:      c.nodeID,
		ConnNumShards:   c.numShards,  // ← Tell data node our shard count!
		SnapshotOptions: SnapshotOptions{
			Limit: 100,
		},
	}
	reqData, _ := json.Marshal(req)

	// 4. Send request-reply via NATS
	msg, err := c.nats.Request(subject, reqData, 5*time.Second)
	if err != nil {
		return nil, fmt.Errorf("data node %s not responding: %w", dataNodeID, err)
	}

	// 5. Parse response
	var resp SubscribeResponse
	json.Unmarshal(msg.Data, &resp)

	// 6. Track locally in the correct shard
	c.shards[shardIndex].addChannel(channel)

	log.Printf("Subscribed to %s via data node %d, local shard %d", channel, nodeIndex, shardIndex)

	return &resp, nil
}
```

### Handle Updates (Shard Level)

```go
func (cs *ConnectionShard) handleUpdate(msg *nats.Msg) {
	var pub Publication
	json.Unmarshal(msg.Data, &pub)

	cs.mu.RLock()
	channelSubs, exists := cs.channels[pub.Channel]
	cs.mu.RUnlock()

	if !exists {
		// We're not subscribed to this channel (shouldn't happen)
		log.Printf("Shard %d received update for untracked channel: %s", cs.index, pub.Channel)
		return
	}

	// Send to all clients subscribed to this channel
	channelSubs.broadcast(&pub)
}

func (cs *ConnectionShard) addChannel(channel string) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if cs.channels[channel] == nil {
		cs.channels[channel] = &ChannelSubscribers{
			channel: channel,
			clients: make(map[string]*Client),
		}
	}
}
```

### Publish

```go
func (c *ConnectionNode) Publish(ctx context.Context, channel, key string, data []byte, opts EnginePublishOptions) (StreamPosition, bool, error) {
	// 1. Hash to find data node
	dataNodeID := c.hashRing.GetNode(channel)
	nodeIndex := c.hashRing.GetNodeIndex(dataNodeID)

	// 2. Build NATS subject
	subject := fmt.Sprintf("data.%d.publish", nodeIndex)

	// 3. Prepare request
	req := &PublishRequest{
		Channel: channel,
		Key:     key,
		Data:    data,
		Options: opts,
	}
	reqData, _ := json.Marshal(req)

	// 4. Send request-reply via NATS
	msg, err := c.nats.Request(subject, reqData, 5*time.Second)
	if err != nil {
		return StreamPosition{}, false, err
	}

	// 5. Parse response
	var resp PublishResponse
	json.Unmarshal(msg.Data, &resp)

	return resp.Position, resp.Cached, resp.Error
}
```

## Data Node Implementation

### Structure: Track Subscriber Shards

```go
type DataNode struct {
	nodeIndex int

	// Track subscribers: channel → list of connection nodes with their shard counts
	channelSubscribers map[string][]ConnectionNodeInfo
	mu                 sync.RWMutex

	engine *MemoryKeyedEngine
	nats   *nats.Conn
}

type ConnectionNodeInfo struct {
	NodeID    string
	NumShards int  // ← Store shard count for each connection node
}
```

### Startup

```go
func (d *DataNode) Start() error {
	// Subscribe to our node's requests
	subject := fmt.Sprintf("data.%d.*", d.nodeIndex)

	_, err := d.nats.Subscribe(subject, func(msg *nats.Msg) {
		// Route based on operation
		parts := strings.Split(msg.Subject, ".")
		if len(parts) < 3 {
			return
		}

		operation := parts[2] // subscribe, publish, snapshot, etc.

		switch operation {
		case "subscribe":
			d.handleSubscribe(msg)
		case "publish":
			d.handlePublish(msg)
		case "snapshot":
			d.handleReadSnapshot(msg)
		case "stream":
			d.handleReadStream(msg)
		case "remove":
			d.handleRemove(msg)
		}
	})

	log.Printf("Data node %d: listening on %s", d.nodeIndex, subject)
	return err
}
```

### Handle Subscribe: Store Shard Info

```go
func (d *DataNode) handleSubscribe(msg *nats.Msg) {
	var req SubscribeRequest
	json.Unmarshal(msg.Data, &req)

	ctx := context.Background()

	// Subscribe in engine
	err := d.engine.Subscribe(req.Channel)
	if err != nil {
		d.replyError(msg, err)
		return
	}

	// Track subscriber with shard info
	d.mu.Lock()
	subscribers := d.channelSubscribers[req.Channel]

	// Check if this connection node is already subscribed
	found := false
	for i, sub := range subscribers {
		if sub.NodeID == req.ConnNodeID {
			// Update shard count if changed
			subscribers[i].NumShards = req.ConnNumShards
			found = true
			break
		}
	}

	if !found {
		subscribers = append(subscribers, ConnectionNodeInfo{
			NodeID:    req.ConnNodeID,
			NumShards: req.ConnNumShards,  // ← Store shard count
		})
		log.Printf("Data node %d: registered subscriber %s (shards=%d) for channel %s",
			d.nodeIndex, req.ConnNodeID, req.ConnNumShards, req.Channel)
	}

	d.channelSubscribers[req.Channel] = subscribers
	d.mu.Unlock()

	// Read snapshot and reply
	entries, pos, _, err := d.engine.ReadSnapshot(ctx, req.Channel, req.SnapshotOptions)
	if err != nil {
		d.replyError(msg, err)
		return
	}

	resp := &SubscribeResponse{
		Snapshot: entries,
		Position: pos,
	}
	respData, _ := json.Marshal(resp)
	msg.Respond(respData)
}
```

### Handle Publish & Broadcast

```go
func (d *DataNode) handlePublish(msg *nats.Msg) {
	var req PublishRequest
	json.Unmarshal(msg.Data, &req)

	ctx := context.Background()

	// Publish to engine
	pos, cached, err := d.engine.Publish(ctx, req.Channel, req.Key, req.Data, req.Options)
	if err != nil {
		d.replyError(msg, err)
		return
	}

	// Reply immediately
	resp := &PublishResponse{
		Position: pos,
		Cached:   cached,
	}
	respData, _ := json.Marshal(resp)
	msg.Respond(respData)

	// Broadcast update to subscribers (async, fire-and-forget)
	go d.broadcastUpdate(req.Channel, &Publication{
		Channel: req.Channel,
		Key:     req.Key,
		Data:    req.Data,
		Offset:  pos.Offset,
		Epoch:   pos.Epoch,
	})
}
```

### Broadcast: Calculate Shard Per Connection Node

```go
func (d *DataNode) broadcastUpdate(channel string, pub *Publication) {
	d.mu.RLock()
	subscribers := d.channelSubscribers[channel]
	d.mu.RUnlock()

	if len(subscribers) == 0 {
		return  // No subscribers
	}

	pubData, _ := json.Marshal(pub)

	// For each connection node subscribed to this channel
	for _, connNode := range subscribers {
		// Calculate which shard on that connection node handles this channel
		shard := d.calculateShard(channel, connNode.NumShards)

		// Publish to that specific shard
		subject := fmt.Sprintf("conn.%s.%d", connNode.NodeID, shard)
		d.nats.Publish(subject, pubData)

		// This is fire-and-forget, but NATS guarantees delivery
		// if the connection node is connected
	}
}

func (d *DataNode) calculateShard(channel string, numShards int) int {
	hash := crc32.ChecksumIEEE([]byte(channel))
	return int(hash % uint32(numShards))
}
```

### ReadSnapshot

```go
func (d *DataNode) handleReadSnapshot(msg *nats.Msg) {
	var req ReadSnapshotRequest
	json.Unmarshal(msg.Data, &req)

	ctx := context.Background()
	entries, pos, cursor, err := d.engine.ReadSnapshot(ctx, req.Channel, req.Options)

	resp := &ReadSnapshotResponse{
		Entries:  entries,
		Position: pos,
		Cursor:   cursor,
		Error:    err,
	}
	respData, _ := json.Marshal(resp)
	msg.Respond(respData)
}
```

## Complete Flow Example

### Setup

```
Connection Node "abc123":
- numShards = 16
- Creates 16 NATS subscriptions:
  conn.abc123.0
  conn.abc123.1
  ...
  conn.abc123.15

Connection Node "xyz789":
- numShards = 16
- Creates 16 NATS subscriptions:
  conn.xyz789.0
  conn.xyz789.1
  ...
  conn.xyz789.15

Data Nodes:
- centrifugo-data-0 (listens: data.0.*)
- centrifugo-data-1 (listens: data.1.*)
- centrifugo-data-2 (listens: data.2.*)
```

### Flow 1: Client Subscribes to "chat:123"

```
1. Client → Connection Node "abc123"
   WebSocket: {subscribe: "chat:123"}

2. Connection Node "abc123"
   - hash("chat:123") % 3 data nodes = 1 → Data Node 1
   - hash("chat:123") % 16 shards = 7 → Local Shard 7

3. Connection Node → NATS
   Subject: data.1.subscribe
   Payload: {
     channel: "chat:123",
     connNodeID: "abc123",
     connNumShards: 16  ← Tells data node shard count
   }

4. Data Node 1
   - engine.Subscribe("chat:123")
   - Records: channelSubscribers["chat:123"] = [
       {NodeID: "abc123", NumShards: 16}
     ]
   - engine.ReadSnapshot("chat:123")
   - Replies with snapshot

5. Connection Node "abc123"
   - Shard 7 tracks "chat:123"
   - Sends snapshot to client
```

### Flow 2: Another Client Subscribes to Same Channel

```
1. Client → Connection Node "xyz789"
   WebSocket: {subscribe: "chat:123"}

2. Connection Node "xyz789"
   - hash("chat:123") % 3 = 1 → Data Node 1 (same!)
   - hash("chat:123") % 16 = 7 → Local Shard 7 (same index!)

3. Connection Node → NATS
   Subject: data.1.subscribe
   Payload: {
     channel: "chat:123",
     connNodeID: "xyz789",
     connNumShards: 16
   }

4. Data Node 1
   - Already subscribed to "chat:123"
   - Updates: channelSubscribers["chat:123"] = [
       {NodeID: "abc123", NumShards: 16},
       {NodeID: "xyz789", NumShards: 16}
     ]
   - Replies with snapshot
```

### Flow 3: Publish to "chat:123"

```
1. Client → Connection Node "xyz789"
   WebSocket: {publish: "chat:123", data: {...}}

2. Connection Node → NATS
   Subject: data.1.publish
   Payload: {channel: "chat:123", ...}

3. Data Node 1
   - engine.Publish("chat:123", ...)
   - Gets offset: 43
   - Replies: {position: {offset: 43}}

4. Data Node 1 → Broadcast (async)
   For subscriber "abc123" (numShards=16):
     - hash("chat:123") % 16 = 7
     - Publish to: conn.abc123.7

   For subscriber "xyz789" (numShards=16):
     - hash("chat:123") % 16 = 7
     - Publish to: conn.xyz789.7

5. NATS
   → Delivers to conn.abc123.7
   → Delivers to conn.xyz789.7

6. Connection Nodes
   - abc123 Shard 7: receives update, broadcasts to clients
   - xyz789 Shard 7: receives update, broadcasts to clients
```

## Two-Level Hashing Visualization

```
Channel "chat:123"  (hash = 0x7f3a2b1c)

Level 1: Data Node Selection (Consistent Hashing)
├─ hash("chat:123") % 3 data nodes = 1
└─ Data Node 1 owns this channel's state

Level 2: Connection Node Shard Selection (Modulo Hashing)
├─ For Connection Node "abc123" (numShards=16):
│  └─ hash("chat:123") % 16 = 7 → Shard 7
│     Subject: conn.abc123.7
│
├─ For Connection Node "xyz789" (numShards=16):
│  └─ hash("chat:123") % 16 = 7 → Shard 7
│     Subject: conn.xyz789.7
│
└─ For Connection Node "def456" (numShards=32):
   └─ hash("chat:123") % 32 = 23 → Shard 23
      Subject: conn.def456.23

Result: Data node sends targeted updates to specific shards
```

## Automatic Resharding on Scale

### Scenario: Scale from 3 → 4 Data Nodes

```
Initial State (3 nodes):
┌─────────┬─────────┬─────────┐
│ Node 0  │ Node 1  │ Node 2  │
├─────────┼─────────┼─────────┤
│ 33.3%   │ 33.3%   │ 33.3%   │
│ of keys │ of keys │ of keys │
└─────────┴─────────┴─────────┘

After Scaling (4 nodes):
┌────────┬────────┬────────┬────────┐
│ Node 0 │ Node 1 │ Node 2 │ Node 3 │
├────────┼────────┼────────┼────────┤
│ 25%    │ 25%    │ 25%    │ 25%    │
│ stable │ stable │ stable │  new   │
└────────┴────────┴────────┴────────┘

Keys moved: ~25% (to Node 3)
Keys stable: ~75% (remain on Node 0, 1, 2)
```

### Resharding Process

**Step 1: New Data Node Joins**

```yaml
# K8s scales StatefulSet
kubectl scale statefulset centrifugo-data --replicas=4

# New pod starts: centrifugo-data-3
```

**Step 2: Connection Nodes Detect Change**

```go
// Connection Node watches K8s StatefulSet
func (c *ConnectionNode) watchStatefulSet(ctx context.Context, name string) error {
	watcher, _ := c.k8s.AppsV1().StatefulSets("default").Watch(ctx, metav1.ListOptions{
		FieldSelector: fmt.Sprintf("metadata.name=%s", name),
	})

	for event := range watcher.ResultChan() {
		sts := event.Object.(*appsv1.StatefulSet)
		newReplicas := int(*sts.Spec.Replicas)

		if newReplicas != c.currentReplicas {
			log.Printf("Replica count changed: %d → %d", c.currentReplicas, newReplicas)
			c.currentReplicas = newReplicas

			// Update node list
			newNodes := make([]string, newReplicas)
			for i := 0; i < newReplicas; i++ {
				newNodes[i] = fmt.Sprintf("centrifugo-data-%d", i)
			}

			// Update hash ring - this returns which channels moved
			migrations := c.hashRing.UpdateNodes(newNodes)

			// Handle migrations (optional - can use lazy migration)
			c.handleMigrations(migrations)
		}
	}

	return nil
}
```

**Step 3: Lazy Migration**

New operations automatically route to new data nodes based on updated hash ring. Old subscriptions stay on old nodes until client reconnects.

## Performance Characteristics

### NATS Subscription Overhead

```
Connection Node with different shard counts:

numShards=4:   4 NATS subscriptions   (~4KB memory)
numShards=8:   8 NATS subscriptions   (~8KB memory)
numShards=16:  16 NATS subscriptions  (~16KB memory)
numShards=32:  32 NATS subscriptions  (~32KB memory)

Recommended: 16 shards for most deployments
- Good load distribution
- Low memory overhead
- ~625 channels per shard (10K channels total)
```

### Broadcast Efficiency

```
Scenario: 1 channel, 5 connection nodes subscribed

Old approach (hash-prefix filtering):
- Data node: 1 NATS publish to updates.3f7a2b1c
- NATS: broadcasts to all 5 connection nodes
- Each connection node: filters, only 1 processes
- Wasted: 4 nodes receive irrelevant data

New approach (targeted sharding):
- Data node: 5 NATS publishes (one per connection node shard)
  - conn.abc123.7
  - conn.xyz789.7
  - conn.def456.7
  - conn.ghi012.7
  - conn.jkl345.7
- NATS: delivers each to specific shard
- Each connection node: processes immediately, no filtering
- Wasted: 0 nodes receive irrelevant data

Trade-off:
- More NATS publishes (5 vs 1)
- But each is ~10-50µs, so 5 * 50µs = 250µs total
- Benefit: No wasted bandwidth, perfect targeting
```

### Latency

```
Operation         Direct gRPC    NATS (sharded)    Difference
Subscribe         2-3ms          3-5ms             +1-2ms
Publish           1-2ms          2-4ms             +1-2ms
ReadSnapshot      3-5ms          4-6ms             +1-2ms
Update broadcast  <1ms           <1ms              ~same (targeted)
```

### Throughput

- **NATS**: 11M msgs/sec (single server)
- **Per data node**: ~100K ops/sec
- **Bottleneck**: Memory engine, not NATS
- **Per connection node shard**: ~10K ops/sec (well distributed)

### Scaling Limits

- **Connection nodes**: 100+ (limited by NATS connections, but still scalable)
- **Data nodes**: 20-30 (limited by channel distribution)
- **NATS cluster**: 3-5 nodes for HA
- **Channels per shard**: 1K-10K (good distribution)

## Code Structure

```
centrifuge/
├── engine/
│   ├── keyed_memory.go          # MemoryKeyedEngine (in-memory impl)
│   ├── consistent_hash.go       # Consistent hashing for data nodes
│   └── keyed_memory_test.go
├── node/
│   ├── connection_node.go       # Connection node (stateless, sharded)
│   ├── connection_shard.go      # Connection shard (handles subset of channels)
│   ├── data_node.go             # Data node (stateful)
│   └── nats_transport.go        # NATS request-reply wrapper
├── proto/
│   └── messages.go              # Request/response types (JSON)
│       ├── SubscribeRequest     # includes ConnNumShards
│       ├── SubscribeResponse
│       ├── PublishRequest
│       ├── PublishResponse
│       └── ConnectionNodeInfo   # NodeID + NumShards
└── cmd/
    └── centrifugo/
        └── main.go              # Mode: connection or data
```

## Configuration

```yaml
# Connection Node
mode: connection
node_id: ${POD_NAME}        # Unique ID for this connection node
num_shards: 16              # Number of shards (NATS subscriptions)
nats:
  url: nats://nats:4222
  reconnect_buf_size: -1    # Disable buffering for ordering
discovery:
  type: kubernetes
  statefulset: centrifugo-data
  namespace: default

# Data Node
mode: data
node_index: ${POD_INDEX}    # From StatefulSet ordinal
nats:
  url: nats://nats:4222
  reconnect_buf_size: -1
engine:
  type: memory
  stream_size: 1000
  snapshot_ttl: 300s
```

## Summary

### Key Decisions

1. ✅ **NATS for transport**: Simpler than gRPC, built-in pub/sub
2. ✅ **Consistent hashing**: Minimal data movement on scaling (75% stable)
3. ✅ **Connection node sharding**: N fixed subscriptions per node (e.g., 16)
4. ✅ **Targeted updates**: conn.<node-id>.<shard> - only interested data
5. ✅ **Request-reply pattern**: Each data node has dedicated subject prefix
6. ✅ **Lazy migration**: New operations automatically go to new nodes
7. ✅ **K8s StatefulSet**: Predictable names for data nodes
8. ✅ **Two-level hashing**:
   - Level 1: Channel → Data Node (consistent hashing)
   - Level 2: Channel → Connection Shard (modulo hashing)

### Connection Node Sharding Benefits

- **Fixed subscriptions**: 16 NATS subscriptions regardless of channel count
- **Perfect load distribution**: Channels evenly distributed across shards
- **No filtering**: Each shard receives only relevant updates
- **Low overhead**: ~16KB memory for subscriptions
- **Scalable**: Can increase numShards for higher throughput

### Migration Strategy

- **75% channels stable** when scaling 3→4 nodes
- **O(log N) lookup** for channel→node mapping
- **Automatic rebalancing** via consistent hash ring updates
- **Zero downtime** - old subscriptions stay on old nodes until reconnect

### Next Steps

1. Implement `ConsistentHash` with 150 vnodes
2. Implement `ConnectionNode` with sharded NATS subscriptions
3. Implement `ConnectionShard` for channel handling
4. Implement `DataNode` with subscriber tracking (NodeID + NumShards)
5. Add K8s StatefulSet discovery
6. Add migration mechanism (optional, can be lazy)
7. Add monitoring for:
   - Channel distribution per data node
   - Channel distribution per connection shard
   - NATS message rates per subject
