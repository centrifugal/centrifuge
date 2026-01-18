# Distributed In-Memory Architecture for Centrifuge

This document describes a distributed architecture where Centrifuge separates stateless connection nodes from stateful data nodes, enabling horizontal scaling while maintaining in-memory performance.

> ⚠️ **IMPORTANT**: This architecture trades durability and strong consistency for extreme performance and scalability. Many "downsides" (data loss, no replication) are acceptable for real-time messaging use cases. Read the analysis documents:
> - [distributed_in_memory_architecture_downsides.md](distributed_in_memory_architecture_downsides.md) - Complete list of all trade-offs
> - [distributed_in_memory_real_issues.md](distributed_in_memory_real_issues.md) - **Actual problems to solve** for production use

## Overview

### Components

Three core Centrifuge components can be distributed using this architecture:

1. **KeyedEngine** - Manages keyed state with revisions (ordered/unordered snapshots + streams)
2. **Broker** - Handles PUB/SUB and history streams
3. **PresenceManager** - Tracks online presence in channels

### Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Connection Nodes                         │
│                      (Stateless)                             │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐   │
│  │ WebSocket│  │ WebSocket│  │ WebSocket│  │ WebSocket│   │
│  │ Handler  │  │ Handler  │  │ Handler  │  │ Handler  │   │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘   │
│       │             │             │             │           │
│       └─────────────┴─────────────┴─────────────┘           │
│                          │                                   │
│                    Hash Ring Lookup                          │
│                    (Consistent Hashing)                      │
└──────────────────────────┼──────────────────────────────────┘
                           │
                      NATS Cluster
                           │
┌──────────────────────────┼──────────────────────────────────┐
│                     Data Nodes                               │
│                     (Stateful)                               │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  Data Node 0                                         │   │
│  │  ├─ In-Memory KeyedEngine (channels 0, 3, 6, ...)   │   │
│  │  ├─ In-Memory Broker (channels 0, 3, 6, ...)        │   │
│  │  └─ In-Memory PresenceManager (channels 0, 3, 6...)  │   │
│  └──────────────────────────────────────────────────────┘   │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  Data Node 1                                         │   │
│  │  ├─ In-Memory KeyedEngine (channels 1, 4, 7, ...)   │   │
│  │  ├─ In-Memory Broker (channels 1, 4, 7, ...)        │   │
│  │  └─ In-Memory PresenceManager (channels 1, 4, 7...)  │   │
│  └──────────────────────────────────────────────────────┘   │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  Data Node 2                                         │   │
│  │  ├─ In-Memory KeyedEngine (channels 2, 5, 8, ...)   │   │
│  │  ├─ In-Memory Broker (channels 2, 5, 8, ...)        │   │
│  │  └─ In-Memory PresenceManager (channels 2, 5, 8...)  │   │
│  └──────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────┘
```

## NATS Subject Hierarchy

### Request-Reply Operations (Synchronous)

All three components use the same routing pattern:

```
data.<node-index>.<component>.<operation>
```

Examples:
- `data.0.keyed.subscribe`
- `data.0.keyed.publish`
- `data.0.keyed.read_snapshot`
- `data.0.broker.publish`
- `data.0.broker.history`
- `data.0.presence.add`
- `data.0.presence.get`
```

### Targeted Updates (Asynchronous)

Updates are published to specific connection node shards:

```
conn.<node-id>.<shard>.<component>.<event-type>
```

Examples:
- `conn.abc123.7.keyed.publication`
- `conn.abc123.7.broker.publication`
- `conn.abc123.7.broker.join`
- `conn.abc123.7.broker.leave`
- `conn.abc123.7.presence.join`
- `conn.abc123.7.presence.leave`
```

## Component Operations

### 1. KeyedEngine (Keyed State with Revisions)

**Interface:**
```go
type KeyedEngine interface {
    Subscribe(ch string) error
    Publish(ctx context.Context, ch string, key string, data []byte, opts EnginePublishOptions) (StreamPosition, bool, error)
    Remove(ctx context.Context, ch string, key string, opts EngineUnpublishOptions) (StreamPosition, error)
    ReadSnapshot(ctx context.Context, ch string, opts ReadSnapshotOptions) ([]SnapshotEntry, StreamPosition, string, error)
    ReadStream(ctx context.Context, ch string, opts ReadStreamOptions) ([]*Publication, StreamPosition, error)
}
```

**NATS Operations:**

| Operation | NATS Subject | Type | Description |
|-----------|--------------|------|-------------|
| Subscribe | `data.<idx>.keyed.subscribe` | Request-Reply | Register interest in channel |
| Publish | `data.<idx>.keyed.publish` | Request-Reply | Add/update entry in snapshot + stream |
| Remove | `data.<idx>.keyed.remove` | Request-Reply | Remove entry from snapshot (adds to stream) |
| ReadSnapshot | `data.<idx>.keyed.read_snapshot` | Request-Reply | Get current snapshot (paginated) |
| ReadStream | `data.<idx>.keyed.read_stream` | Request-Reply | Get stream history |
| **Broadcast** | `conn.<id>.<shard>.keyed.publication` | Publish | Real-time updates to subscribers |

**In-Memory State per Channel:**
- **Snapshot (unordered)**: `map[string]*SnapshotEntry` (~53KB for 100 entries)
- **Snapshot (ordered)**: `map[string]*SnapshotEntry` + sorted set (~59KB for 100 entries)
- **Stream**: Circular buffer (~600KB for 1000 entries)
- **Metadata**: Offset, epoch, TTL timers (~124B)

### 2. Broker (PUB/SUB + History)

**Interface:**
```go
type Broker interface {
    Subscribe(ch string) error
    Unsubscribe(ch string) error
    Publish(ch string, data []byte, opts PublishOptions) (StreamPosition, bool, error)
    PublishJoin(ch string, info *ClientInfo) error
    PublishLeave(ch string, info *ClientInfo) error
    History(ch string, opts HistoryOptions) ([]*Publication, StreamPosition, error)
    RemoveHistory(ch string) error
}
```

**NATS Operations:**

| Operation | NATS Subject | Type | Description |
|-----------|--------------|------|-------------|
| Subscribe | `data.<idx>.broker.subscribe` | Request-Reply | Register interest in channel |
| Unsubscribe | `data.<idx>.broker.unsubscribe` | Request-Reply | Unregister from channel |
| Publish | `data.<idx>.broker.publish` | Request-Reply | Publish message to channel + history |
| PublishJoin | `data.<idx>.broker.publish_join` | Request-Reply | Broadcast join event |
| PublishLeave | `data.<idx>.broker.publish_leave` | Request-Reply | Broadcast leave event |
| History | `data.<idx>.broker.history` | Request-Reply | Get history from stream |
| RemoveHistory | `data.<idx>.broker.remove_history` | Request-Reply | Clear history stream |
| **Broadcast Pub** | `conn.<id>.<shard>.broker.publication` | Publish | Real-time message delivery |
| **Broadcast Join** | `conn.<id>.<shard>.broker.join` | Publish | Join event delivery |
| **Broadcast Leave** | `conn.<id>.<shard>.broker.leave` | Publish | Leave event delivery |

**In-Memory State per Channel:**
- **History Stream**: Circular buffer of publications (~600KB for 1000 entries)
- **Metadata**: Offset, epoch, TTL timers, version tracking (~150B)
- **Idempotency Cache**: Map of idempotency keys to results (varies)

### 3. PresenceManager (Online Presence)

**Interface:**
```go
type PresenceManager interface {
    Presence(ch string) (map[string]*ClientInfo, error)
    PresenceStats(ch string) (PresenceStats, error)
    AddPresence(ch string, clientID string, info *ClientInfo) error
    RemovePresence(ch string, clientID string, userID string) error
}
```

**NATS Operations:**

| Operation | NATS Subject | Type | Description |
|-----------|--------------|------|-------------|
| AddPresence | `data.<idx>.presence.add` | Request-Reply | Add client to presence set |
| RemovePresence | `data.<idx>.presence.remove` | Request-Reply | Remove client from presence set |
| Presence | `data.<idx>.presence.get` | Request-Reply | Get full presence map |
| PresenceStats | `data.<idx>.presence.stats` | Request-Reply | Get presence statistics |
| **Broadcast Join** | `conn.<id>.<shard>.presence.join` | Publish | Presence add notification |
| **Broadcast Leave** | `conn.<id>.<shard>.presence.leave` | Publish | Presence remove notification |

**In-Memory State per Channel:**
- **Presence Map**: `map[clientID]*ClientInfo` (~200B per client)
- **TTL Timers**: Expiration tracking for inactive clients (~50B per client)

## Connection Node Sharding

Each connection node creates a fixed number of NATS subscriptions (e.g., 16 shards) to receive targeted updates:

```go
type ConnectionNode struct {
    nodeID    string
    numShards int  // Fixed: 16 shards per connection node
    hashRing  *ConsistentHash
}

func (c *ConnectionNode) Start() error {
    // Subscribe to all component events across all shards
    for i := 0; i < c.numShards; i++ {
        // KeyedEngine updates
        c.nats.Subscribe(
            fmt.Sprintf("conn.%s.%d.keyed.publication", c.nodeID, i),
            c.handleKeyedPublication,
        )

        // Broker updates
        c.nats.Subscribe(
            fmt.Sprintf("conn.%s.%d.broker.publication", c.nodeID, i),
            c.handleBrokerPublication,
        )
        c.nats.Subscribe(
            fmt.Sprintf("conn.%s.%d.broker.join", c.nodeID, i),
            c.handleBrokerJoin,
        )
        c.nats.Subscribe(
            fmt.Sprintf("conn.%s.%d.broker.leave", c.nodeID, i),
            c.handleBrokerLeave,
        )

        // Presence updates
        c.nats.Subscribe(
            fmt.Sprintf("conn.%s.%d.presence.join", c.nodeID, i),
            c.handlePresenceJoin,
        )
        c.nats.Subscribe(
            fmt.Sprintf("conn.%s.%d.presence.leave", c.nodeID, i),
            c.handlePresenceLeave,
        )
    }
    return nil
}
```

**Total subscriptions per connection node**: 16 shards × 6 event types = **96 NATS subscriptions**

## Data Node Broadcasting

When a data node needs to broadcast an update, it calculates the target shard for each subscriber:

```go
type DataNode struct {
    index     int
    nats      *nats.Conn
    keyed     *InMemoryKeyedEngine
    broker    *InMemoryBroker
    presence  *InMemoryPresenceManager
}

type Subscriber struct {
    NodeID    string
    NumShards int
}

// KeyedEngine: Broadcast publication to all subscribers
func (d *DataNode) broadcastKeyedPublication(channel string, pub *Publication) {
    subscribers := d.keyed.GetSubscribers(channel)
    for _, sub := range subscribers {
        shard := hash(channel) % sub.NumShards
        subject := fmt.Sprintf("conn.%s.%d.keyed.publication", sub.NodeID, shard)
        d.nats.Publish(subject, encode(pub))
    }
}

// Broker: Broadcast publication to all channel subscribers
func (d *DataNode) broadcastBrokerPublication(channel string, pub *Publication) {
    subscribers := d.broker.GetSubscribers(channel)
    for _, sub := range subscribers {
        shard := hash(channel) % sub.NumShards
        subject := fmt.Sprintf("conn.%s.%d.broker.publication", sub.NodeID, shard)
        d.nats.Publish(subject, encode(pub))
    }
}

// Broker: Broadcast join event
func (d *DataNode) broadcastJoin(channel string, info *ClientInfo) {
    subscribers := d.broker.GetSubscribers(channel)
    for _, sub := range subscribers {
        shard := hash(channel) % sub.NumShards
        subject := fmt.Sprintf("conn.%s.%d.broker.join", sub.NodeID, shard)
        d.nats.Publish(subject, encode(info))
    }
}

// Presence: Broadcast presence join
func (d *DataNode) broadcastPresenceJoin(channel string, info *ClientInfo) {
    subscribers := d.presence.GetSubscribers(channel)
    for _, sub := range subscribers {
        shard := hash(channel) % sub.NumShards
        subject := fmt.Sprintf("conn.%s.%d.presence.join", sub.NodeID, shard)
        d.nats.Publish(subject, encode(info))
    }
}
```

## Request-Reply Flow Examples

### Example 1: Client subscribes to channel (Broker)

```
1. Client → Connection Node: SUBSCRIBE to "chat:lobby"
2. Connection Node:
   - Calculate: nodeIndex = hashRing.GetNode("chat:lobby") → 2
   - Send NATS request to: data.2.broker.subscribe
   - Include: {nodeID: "abc123", numShards: 16, channel: "chat:lobby"}
3. Data Node 2:
   - Add subscriber: subscribers["chat:lobby"]["abc123"] = {NumShards: 16}
   - Return: {success: true}
4. Connection Node → Client: Subscribe confirmed
```

### Example 2: Server publishes message to channel (Broker)

```
1. Server → Connection Node: PUBLISH to "chat:lobby"
2. Connection Node:
   - Calculate: nodeIndex = hashRing.GetNode("chat:lobby") → 2
   - Send NATS request to: data.2.broker.publish
   - Include: {channel: "chat:lobby", data: "Hello", opts: {...}}
3. Data Node 2:
   - Add to history stream: offset = 42, epoch = "abc"
   - Get subscribers: ["abc123" (16 shards), "def456" (16 shards)]
   - Broadcast:
     * Calculate: shard = hash("chat:lobby") % 16 → 7
     * Publish to: conn.abc123.7.broker.publication
     * Publish to: conn.def456.7.broker.publication
   - Return: {offset: 42, epoch: "abc"}
4. All Connection Nodes:
   - Receive on shard 7
   - Deliver to local WebSocket clients subscribed to "chat:lobby"
```

### Example 3: Add presence to channel (PresenceManager)

```
1. Client connects → Connection Node: JOIN "chat:lobby"
2. Connection Node:
   - Calculate: nodeIndex = hashRing.GetNode("chat:lobby") → 2
   - Send NATS request to: data.2.presence.add
   - Include: {channel: "chat:lobby", clientID: "user123", info: {...}}
3. Data Node 2:
   - Add to presence map: presence["chat:lobby"]["user123"] = ClientInfo{...}
   - Set TTL timer for expiration
   - Get presence subscribers (if enabled)
   - Broadcast (optional):
     * Calculate: shard = hash("chat:lobby") % 16 → 7
     * Publish to: conn.abc123.7.presence.join (for all subscribers)
   - Return: {success: true}
4. Connection Nodes with presence subscribers:
   - Receive on shard 7
   - Deliver presence join event to interested clients
```

### Example 4: Publish to KeyedEngine

```
1. Server → Connection Node: PUBLISH key "user:123" to channel "users"
2. Connection Node:
   - Calculate: nodeIndex = hashRing.GetNode("users") → 1
   - Send NATS request to: data.1.keyed.publish
   - Include: {channel: "users", key: "user:123", data: {...}, opts: {...}}
3. Data Node 1:
   - Update snapshot: snapshot["user:123"] = SnapshotEntry{data, score}
   - Append to stream: offset = 15, epoch = "xyz"
   - Get subscribers: ["abc123" (16 shards)]
   - Broadcast:
     * Calculate: shard = hash("users") % 16 → 3
     * Publish to: conn.abc123.3.keyed.publication
   - Return: {offset: 15, epoch: "xyz"}
4. Connection Node:
   - Receive on shard 3
   - Deliver to clients subscribed to "users" channel
```

## Consistent Hashing

All three components use the same consistent hashing for channel-to-node routing:

```go
type ConsistentHash struct {
    circle       []uint32          // Sorted hash values
    nodeMap      map[uint32]int    // Hash → node index
    virtualNodes int               // 150 per physical node
}

func (h *ConsistentHash) GetNode(channel string) int {
    hash := crc32.ChecksumIEEE([]byte(channel))
    idx := sort.Search(len(h.circle), func(i int) bool {
        return h.circle[i] >= hash
    })
    if idx >= len(h.circle) {
        idx = 0
    }
    return h.nodeMap[h.circle[idx]]
}
```

**Properties:**
- **Deterministic**: Same channel always routes to same node
- **Uniform distribution**: ~33% of channels per node (3 nodes)
- **Minimal rebalancing**: Only ~25% of channels move when adding a node
- **Fast lookup**: O(log N) binary search (~300ns)

## Service Discovery via K8s

Connection nodes detect data node scaling events using K8s API Watch:

```go
func (c *ConnectionNode) watchStatefulSet(ctx context.Context) {
    watcher, _ := c.k8s.AppsV1().StatefulSets(c.namespace).Watch(ctx,
        metav1.ListOptions{
            FieldSelector: fmt.Sprintf("metadata.name=%s", "centrifugo-data"),
        })

    for event := range watcher.ResultChan() {
        sts, ok := event.Object.(*appsv1.StatefulSet)
        newReplicas := int(*sts.Spec.Replicas)

        if newReplicas != currentReplicas {
            log.Printf("Data nodes changed: %d → %d", currentReplicas, newReplicas)

            // Build new node list
            newNodes := make([]string, newReplicas)
            for i := 0; i < newReplicas; i++ {
                newNodes[i] = fmt.Sprintf("centrifugo-data-%d", i)
            }

            // Atomic hash ring update
            c.hashRing.UpdateNodes(newNodes)
        }
    }
}
```

**Timeline:**
- T0: Stable state (3 data nodes)
- T1: `kubectl scale statefulset centrifugo-data --replicas=4`
- T2 (~500ms): All connection nodes receive watch event, update hash rings
- T3 (~10-30s): K8s creates `centrifugo-data-3` pod
- T4: New requests route to updated topology

## Performance Characteristics

### Memory Usage per Channel

| Component | State | Size (approximate) |
|-----------|-------|-------------------|
| **KeyedEngine** | Snapshot (100 entries) | ~53KB |
| | Ordered snapshot (100 entries) | ~6KB (index) |
| | Stream (1000 entries) | ~600KB |
| | Metadata | ~124B |
| | **Total** | **~660KB** |
| **Broker** | History stream (1000 entries) | ~600KB |
| | Metadata | ~150B |
| | **Total** | **~600KB** |
| **PresenceManager** | Presence map (100 clients) | ~20KB |
| | TTL timers | ~5KB |
| | **Total** | **~25KB** |

**Combined per channel (all three)**: ~1.3MB

### Throughput per Data Node

Assuming CPU-bound operations on ordered structures:

| Component | Operation | Throughput |
|-----------|-----------|------------|
| **KeyedEngine** | Ordered Publish | ~180K ops/sec |
| | Unordered Publish | ~500K ops/sec |
| | ReadSnapshot (paginated) | ~200K ops/sec |
| **Broker** | Publish (with history) | ~300K ops/sec |
| | Publish (no history) | ~1M ops/sec |
| **PresenceManager** | AddPresence | ~800K ops/sec |
| | RemovePresence | ~800K ops/sec |
| | Presence (100 clients) | ~100K ops/sec |

### Latency Breakdown (Subscribe + Initial Data)

**KeyedEngine Subscribe (with snapshot):**
```
├─ Client → Connection Node:        ~1ms    (network)
├─ Hash ring lookup:                 ~0.3µs (in-memory)
├─ Connection → NATS:                ~0.5ms (network)
├─ NATS routing:                     ~0.1ms (routing)
├─ Data node subscribe:              ~1µs   (register subscriber)
├─ Data node ReadSnapshot:           ~50µs  (100 entries)
├─ NATS → Connection:                ~0.5ms (network)
├─ Connection → Client:              ~1ms   (network)
└─ TOTAL:                            ~3.25ms
```

**Broker Subscribe (with history):**
```
├─ Client → Connection Node:        ~1ms    (network)
├─ Hash ring lookup:                 ~0.3µs (in-memory)
├─ Connection → NATS:                ~0.5ms (network)
├─ NATS routing:                     ~0.1ms (routing)
├─ Data node subscribe:              ~1µs   (register subscriber)
├─ Data node History:                ~30µs  (100 publications)
├─ NATS → Connection:                ~0.5ms (network)
├─ Connection → Client:              ~1ms   (network)
└─ TOTAL:                            ~3.2ms
```

**PresenceManager Presence:**
```
├─ Client → Connection Node:        ~1ms    (network)
├─ Hash ring lookup:                 ~0.3µs (in-memory)
├─ Connection → NATS:                ~0.5ms (network)
├─ NATS routing:                     ~0.1ms (routing)
├─ Data node get presence:           ~20µs  (100 clients)
├─ NATS → Connection:                ~0.5ms (network)
├─ Connection → Client:              ~1ms   (network)
└─ TOTAL:                            ~3.2ms
```

**Network dominates**: ~92% of latency is network overhead.

## Scaling Characteristics

### Linear Scaling (Up to NATS Cluster Limits)

| Data Nodes | Total Memory | Total Ops/sec | Channels |
|------------|--------------|---------------|----------|
| 1          | 10GB         | ~180K         | ~7,500   |
| 3          | 30GB         | ~540K         | ~22,500  |
| 10         | 100GB        | ~1.8M         | ~75,000  |

**Bottlenecks:**
- Below 10 nodes: CPU-bound on data nodes (ordered operations)
- Beyond 10 nodes: NATS cluster throughput (~2M msgs/sec for 3-node cluster)

### NATS Cluster Scaling

For >10 data nodes, scale NATS cluster:

| NATS Nodes | Throughput | Cost |
|------------|------------|------|
| 3          | ~2M msgs/sec | Standard HA setup |
| 5          | ~3M msgs/sec | High-volume setup |
| 7          | ~4M msgs/sec | Very high-volume |

## Recommended Configuration

### Small Deployment (10K channels, 100K ops/sec)

**Data Nodes**: 3 × (2 CPU, 4GB RAM, 1Gbps NIC)
- StatefulSet with 3 replicas
- Each handles ~3,333 channels
- Combined: ~33K ops/sec per node

**Connection Nodes**: 5 × (2 CPU, 2GB RAM)
- Deployment with auto-scaling (2-10)
- Handle WebSocket connections only

**NATS Cluster**: 3 × (4 CPU, 8GB RAM, 10Gbps NIC)
- Standard HA configuration
- ~15% utilization

### Medium Deployment (50K channels, 500K ops/sec)

**Data Nodes**: 10 × (4 CPU, 8GB RAM, 10Gbps NIC)
- StatefulSet with 10 replicas
- Each handles ~5,000 channels
- Combined: ~50K ops/sec per node

**Connection Nodes**: 20 × (2 CPU, 4GB RAM)
- Deployment with auto-scaling (10-30)

**NATS Cluster**: 3 × (8 CPU, 16GB RAM, 10Gbps NIC)
- Standard HA configuration
- ~45% utilization

### Large Deployment (200K channels, 2M ops/sec)

**Data Nodes**: 30 × (8 CPU, 16GB RAM, 10Gbps NIC)
- StatefulSet with 30 replicas
- Each handles ~6,666 channels
- Combined: ~66K ops/sec per node

**Connection Nodes**: 50 × (4 CPU, 8GB RAM)
- Deployment with auto-scaling (30-100)

**NATS Cluster**: 5 × (16 CPU, 32GB RAM, 25Gbps NIC)
- High-volume configuration
- ~65% utilization

## Tuning Recommendations

### Data Node Configuration

```yaml
# In-memory engine limits
maxChannelsPerNode: 10000
snapshotMaxSize: 100        # Pagination required
historyMaxSize: 1000
streamMaxSize: 1000
presenceTTL: 60s

# Memory limits
memoryLimit: 16GB           # 1.3MB per channel × 10K = ~13GB + overhead
```

### NATS Configuration

```yaml
# Message size limits
max_payload: 10MB           # For large snapshots/history

# Connection limits
max_connections: 1000       # 30 data nodes + 100 connection nodes

# Performance tuning
write_deadline: 10s
max_pending_size: 256MB     # Per connection buffer
```

### Connection Node Sharding

```yaml
# Shard configuration
numShardsPerNode: 16        # Fixed for all connection nodes

# Scaling
minReplicas: 10
maxReplicas: 100
targetCPUUtilization: 70%
```

## Operational Considerations

### Rolling Updates

**Connection Nodes** (Stateless):
- Standard rolling update (zero downtime)
- Clients reconnect automatically
- No data loss

**Data Nodes** (Stateful):
- Use lazy migration strategy:
  - Deploy new nodes with updated hash ring
  - Old subscriptions remain on old nodes
  - New subscriptions go to new topology
  - Clients naturally migrate on reconnect
- Gradual drain over hours/days
- No forced reconnections

### Monitoring Metrics

**Per Data Node:**
- Channels hosted (gauge)
- Operations per second (counter)
- Memory usage per component (gauge)
- NATS publish latency (histogram)

**Per Connection Node:**
- Active connections (gauge)
- Messages delivered per second (counter)
- Hash ring lookup latency (histogram)

**Per NATS Cluster:**
- Message throughput (counter)
- Queue depth (gauge)
- Connection count (gauge)

### Failure Scenarios

**Connection Node Failure:**
- Clients reconnect to healthy nodes
- No data loss (stateless)
- Recovery time: ~1-5 seconds (client reconnect)

**Data Node Failure:**
- Affected channels become unavailable
- Clients receive errors, enter reconnect loop
- Once node recovers, data is intact (if not evicted)
- Alternative: Replicate to backup node (adds complexity)

**NATS Node Failure:**
- Other NATS nodes handle traffic (HA)
- Brief message delay (~100-500ms)
- No data loss (in-memory state on data nodes)

## Comparison: Single-Node vs Distributed

| Aspect | Single-Node (Memory) | Distributed (NATS) |
|--------|---------------------|-------------------|
| **Latency** | ~50µs (all in-memory) | ~3.25ms (network overhead) |
| **Throughput** | 180K ops/sec (1 node) | 1.8M ops/sec (10 nodes) |
| **Scalability** | Vertical only | Horizontal (linear) |
| **Availability** | Single point of failure | Multiple nodes (HA) |
| **Rolling Updates** | Downtime required | Zero downtime (stateless) |
| **Memory** | 16GB limit per node | 100GB+ distributed |
| **Complexity** | Low | Medium |

**Use Single-Node when:**
- <10K channels
- <200K ops/sec
- Latency critical (<1ms)
- Simple operations preferred

**Use Distributed when:**
- >10K channels
- >200K ops/sec
- Horizontal scaling needed
- Zero-downtime deploys required

## Next Steps

1. **Prototype**: Implement NATS-based KeyedEngine adapter
2. **Benchmark**: Compare single-node vs distributed latency/throughput
3. **K8s Manifests**: Create StatefulSet + Deployment + Service definitions
4. **Migration Guide**: Document transition from single-node to distributed
5. **Monitoring**: Set up Prometheus metrics and Grafana dashboards
