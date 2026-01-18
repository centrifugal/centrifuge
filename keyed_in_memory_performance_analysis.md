# Performance Analysis: In-Memory KeyedEngine with NATS

> **Note**: This document focuses on KeyedEngine performance. For a unified architecture covering **KeyedEngine, Broker, and PresenceManager**, see [distributed_in_memory_architecture.md](distributed_in_memory_architecture.md).

## Architecture Summary

```
Client ←→ Connection Node (stateless) ←→ NATS ←→ Data Node (in-memory KeyedEngine)
         [Hash ring lookup]              [Msg routing]  [Memory operations]
         [16 shards]                                     [Snapshot/Stream]
```

**Key Components**:
1. **Connection Nodes**: Stateless, handle WebSocket, maintain hash ring
2. **NATS**: Message routing between layers
3. **Data Nodes**: Stateful, in-memory KeyedEngine (snapshot + stream)
4. **Consistent Hashing**: O(log N) channel→data node mapping

## Operation Latency Breakdown

### Subscribe Operation

```
Client                Connection Node              NATS                Data Node
  │                         │                         │                      │
  │ WebSocket Subscribe     │                         │                      │
  │────────────────────────>│                         │                      │
  │   [~1ms network]        │                         │                      │
  │                         │                         │                      │
  │                    ┌────┴────┐                    │                      │
  │                    │Hash ring│                    │                      │
  │                    │ lookup  │                    │                      │
  │                    └────┬────┘                    │                      │
  │                    [~300ns]                       │                      │
  │                         │                         │                      │
  │                         │  NATS Request           │                      │
  │                         │  data.0.subscribe       │                      │
  │                         │─────────────────────────>│                     │
  │                         │   [~500µs network]      │  Forward            │
  │                         │                          │──────────────────>  │
  │                         │                          │  [~100µs routing]   │
  │                         │                          │                     │
  │                         │                          │    ┌────────────┐  │
  │                         │                          │    │ Subscribe  │  │
  │                         │                          │    │ (memory)   │  │
  │                         │                          │    └────────────┘  │
  │                         │                          │    [~1µs]          │
  │                         │                          │                     │
  │                         │                          │    ┌────────────┐  │
  │                         │                          │    │ReadSnapshot│  │
  │                         │                          │    │(100 entries│  │
  │                         │                          │    └────────────┘  │
  │                         │                          │    [~50µs]         │
  │                         │                          │                     │
  │                         │                          │  Reply              │
  │                         │                          │<────────────────────│
  │                         │                          │  [~100µs routing]   │
  │                         │  NATS Reply             │                     │
  │                         │<─────────────────────────│                     │
  │                         │   [~500µs network]      │                     │
  │                         │                         │                      │
  │  WebSocket Response     │                         │                      │
  │<────────────────────────│                         │                      │
  │   [~1ms network]        │                         │                      │

Total Latency Breakdown:
├─ Client → Connection Node:     ~1ms    (network)
├─ Hash ring lookup:              ~0.3µs (in-memory)
├─ Connection → NATS:             ~0.5ms (network)
├─ NATS routing:                  ~0.1ms (message routing)
├─ Data node subscribe:           ~1µs   (memory operation)
├─ Data node ReadSnapshot:        ~50µs  (100 entries from memory)
├─ NATS routing (reply):          ~0.1ms (message routing)
├─ NATS → Connection:             ~0.5ms (network)
├─ Connection → Client:           ~1ms   (network)
└─ TOTAL:                         ~3.25ms

Breakdown by category:
- Network: ~3ms (92%)
- NATS routing: ~0.2ms (6%)
- Memory ops: ~51µs (2%)
- Hash lookup: ~0.3µs (<0.01%)
```

### Publish Operation

```
Client → Connection Node → NATS → Data Node → In-Memory Update
                                            → NATS Broadcast → All interested shards

Total Latency Breakdown:
├─ Client → Connection Node:     ~1ms    (network)
├─ Hash ring lookup:              ~0.3µs (in-memory)
├─ Connection → NATS:             ~0.5ms (network)
├─ NATS routing:                  ~0.1ms (message routing)
├─ Data node Publish:             ~5µs   (update snapshot + stream)
├─ NATS routing (reply):          ~0.1ms (message routing)
├─ NATS → Connection:             ~0.5ms (network)
├─ Connection → Client:           ~1ms   (network)
└─ TOTAL (publish ACK):           ~3.2ms

Async broadcast (parallel):
├─ Data node calculates shards:   ~1µs   (hash calculation per subscriber)
├─ NATS publish (per shard):      ~50µs  (fire-and-forget)
├─ NATS routing to conn nodes:    ~0.1ms (message routing)
├─ NATS → Connection nodes:       ~0.5ms (network)
├─ Connection node → Clients:     ~1ms   (WebSocket push)
└─ TOTAL (update delivery):       ~1.65ms (from publish completion)

End-to-end (publish to update):   ~4.85ms
```

### ReadSnapshot Operation (Pagination)

```
Total Latency Breakdown:
├─ Client → Connection Node:     ~1ms    (network)
├─ Hash ring lookup:              ~0.3µs (in-memory)
├─ Connection → NATS:             ~0.5ms (network)
├─ NATS routing:                  ~0.1ms (message routing)
├─ Data node ReadSnapshot:
│   ├─ Ordered (ZRANGE):          ~20µs  (per 100 entries)
│   └─ Unordered (HSCAN):         ~15µs  (per 100 entries)
├─ NATS routing (reply):          ~0.1ms (message routing)
├─ NATS → Connection:             ~0.5ms (network)
├─ Connection → Client:           ~1ms   (network)
└─ TOTAL:                         ~3.22ms (ordered)
                                  ~3.22ms (unordered)

Scaling with entry count (ordered snapshot):
- 100 entries:   ~3.22ms
- 1000 entries:  ~3.40ms (+180µs for data transfer + processing)
- 10000 entries: ~5.00ms (+1.8ms for data transfer + processing)
```

## Throughput Analysis

### Single Data Node Capacity

**In-Memory KeyedEngine Operations** (assuming memstream-like implementation):

```go
// Snapshot operations (hash map + sorted set)
type MemoryKeyedEngine struct {
    channels map[string]*ChannelState
}

type ChannelState struct {
    snapshot  map[string][]byte    // Key → State (unordered)
    ordered   *OrderedSet           // Key → Score (for ordered)
    stream    *LinkedList           // Offset → Publication
    offset    uint64
    epoch     string
}
```

**Operation Throughput** (single CPU core):

```
Operation               Ops/sec        Latency     Notes
────────────────────────────────────────────────────────────────
Subscribe               1,000,000      ~1µs        Map lookup
Publish (snapshot)      500,000        ~2µs        Map insert + offset increment
Publish (ordered)       300,000        ~3µs        Map insert + sorted set insert
ReadSnapshot (100)      200,000        ~5µs        Map iteration / sorted set range
ReadStream (100)        150,000        ~7µs        Linked list traversal
Remove                  500,000        ~2µs        Map delete
```

**With NATS overhead**:

```
Operation               Direct Memory  With NATS   Overhead
────────────────────────────────────────────────────────────────
Subscribe               1,000,000/s    ~300/s      3,333x slower
Publish                 500,000/s      ~310/s      1,613x slower
ReadSnapshot            200,000/s      ~310/s      645x slower
```

**But this is misleading!** The overhead is network latency, not CPU. The actual bottleneck analysis:

### Bottleneck Analysis (Single Data Node)

**What limits throughput?**

```
Component               Max Throughput    Bottleneck
────────────────────────────────────────────────────────────────
Data Node CPU           500K ops/sec      Publish operations
NATS (single server)    11M msgs/sec      Message routing
Network (1Gbps)         ~125MB/s          Bandwidth
Connection Node CPU     1M ops/sec        WebSocket + JSON
```

**Realistic single data node limits**:

```
Workload: Mixed operations (60% publish, 30% subscribe, 10% snapshot reads)

Data Node capacity:
├─ CPU-bound operations:
│   ├─ Publish: 300K ops/sec (ordered snapshot)
│   ├─ Subscribe: 1M ops/sec (map operations)
│   └─ ReadSnapshot: 200K ops/sec (iteration)
│
└─ Mixed workload: ~250K ops/sec (60% publish dominates)

NATS overhead:
├─ Request-reply: 2 messages per operation
│   └─ 250K ops × 2 = 500K msgs/sec
├─ Broadcasts: Variable (depends on subscribers)
│   ├─ Average 3 connection nodes per channel
│   └─ 150K publishes × 3 broadcasts = 450K msgs/sec
└─ Total NATS load: ~950K msgs/sec (8.6% of NATS capacity)

Network bandwidth:
├─ Average message size: ~1KB (with snapshot data)
├─ Throughput: 250K ops/sec × 1KB × 2 (req+reply) = 500MB/s
└─ 1Gbps link utilization: ~40%

Conclusion: Data Node CPU is the bottleneck at ~250K ops/sec
```

### Horizontal Scaling

**With 3 Data Nodes**:

```
Total capacity: 3 × 250K = 750K ops/sec

Consistent hashing distribution:
├─ Each node: ~33% of channels
├─ Load balancing: Automatic (hash-based)
└─ No coordination overhead

NATS load:
├─ Total messages: ~2.85M msgs/sec (26% of capacity)
└─ Still comfortable headroom
```

**With 10 Data Nodes**:

```
Total capacity: 10 × 250K = 2.5M ops/sec

NATS load:
├─ Total messages: ~9.5M msgs/sec (86% of capacity)
└─ Getting close to NATS limit, may need NATS cluster

Connection nodes:
├─ 20 connection nodes (auto-scaled)
├─ Each handling: ~2.5M / 20 = 125K ops/sec
└─ Well within capacity (1M ops/sec per node)
```

## Memory Usage Analysis

### Connection Node Memory

```go
type ConnectionNode struct {
    // Fixed overhead
    hashRing     *ConsistentHash        // ~7KB (4 nodes × 150 vnodes)
    shards       []*ConnectionShard     // 16 shards

    // Per-shard state
    // Each shard handles ~625 channels (10K total / 16 shards)
}

type ConnectionShard struct {
    channels map[string]*ChannelSubscribers  // ~625 entries
}

type ChannelSubscribers struct {
    channel string
    clients map[string]*Client  // ~10 clients per channel average
}
```

**Memory calculation** (single connection node with 10K active channels):

```
Component                  Count      Size/Entry    Total
────────────────────────────────────────────────────────────────
Hash ring                  1          7KB           7KB
Shards                     16         1KB           16KB
Channel tracking           10,000     ~200B         2MB
Client connections         100,000    ~4KB          400MB
WebSocket buffers          100,000    ~16KB         1.6GB
NATS connection            1          ~1MB          1MB
────────────────────────────────────────────────────────────────
TOTAL:                                              ~2GB

Per-client breakdown:
- Client struct:          ~4KB
- WebSocket buffers:      ~16KB (8KB read + 8KB write)
- Channel tracking:       ~20B (amortized)
────────────────────────────────────────────────────────────────
Total per client:         ~20KB
```

### Data Node Memory

```go
type DataNode struct {
    // In-memory KeyedEngine
    engine *MemoryKeyedEngine

    // Subscriber tracking
    channelSubscribers map[string][]ConnectionNodeInfo
}

type MemoryKeyedEngine struct {
    channels map[string]*ChannelState
}

type ChannelState struct {
    // Snapshot (unordered)
    snapshot  map[string][]byte  // Key → State

    // Snapshot (ordered) - additional index
    ordered   *SortedSet         // Key → Score (using skiplist or btree)

    // Stream
    stream    *CircularBuffer    // Recent publications
    offset    uint64
    epoch     string

    // Subscriber tracking
    subscribers map[string]bool  // Connection node IDs
}
```

**Memory calculation** (single data node with 3,333 channels):

```
Scenario: 10K total channels, 3 data nodes, ~3,333 channels per node

Per-channel state:
├─ Snapshot (unordered):
│   ├─ 100 entries average
│   ├─ Entry size: ~32B (key) + ~500B (state) = ~532B
│   └─ Total: 100 × 532B = ~53KB
│
├─ Snapshot (ordered) - additional index:
│   ├─ Skiplist node: ~64B (key + score + pointers)
│   └─ Total: 100 × 64B = ~6.4KB
│
├─ Stream (circular buffer):
│   ├─ Stream size: 1000 entries
│   ├─ Entry size: ~600B (key + data + metadata)
│   └─ Total: 1000 × 600B = ~600KB
│
├─ Metadata:
│   ├─ Offset: 8B
│   ├─ Epoch: 16B
│   └─ Subscriber tracking: ~100B (few connection nodes)
│
└─ Per-channel total: ~660KB

Total memory (3,333 channels):
├─ Channel state: 3,333 × 660KB = ~2.2GB
├─ Map overhead: ~500KB
├─ NATS connection: ~1MB
├─ Subscriber tracking: ~1MB
└─ TOTAL: ~2.7GB

Memory scaling:
- 1K channels:   ~660MB
- 10K channels:  ~6.6GB
- 100K channels: ~66GB (need multiple data nodes!)
```

**Memory efficiency vs Redis**:

```
Component           In-Memory (Go)    Redis            Difference
────────────────────────────────────────────────────────────────────
Snapshot entry      ~532B             ~700B            25% smaller ✅
Ordered index       ~64B              ~80B (ZSET)      20% smaller ✅
Stream entry        ~600B             ~800B            25% smaller ✅
Metadata overhead   ~124B/channel     ~200B/channel    38% smaller ✅
────────────────────────────────────────────────────────────────────
Total per channel   ~660KB            ~850KB           22% smaller ✅

Why smaller?
- No Redis protocol overhead
- No separate process memory
- Custom data structures (no generic hash table overhead)
- Direct pointers (no string keys everywhere)
```

## Network Bandwidth Analysis

### Bandwidth Requirements

**Single operation bandwidth**:

```
Operation           Request Size    Response Size    Total
────────────────────────────────────────────────────────────────
Subscribe           ~100B           ~50KB            ~50KB
Publish             ~1KB            ~200B            ~1.2KB
ReadSnapshot (100)  ~100B           ~50KB            ~50KB
Update broadcast    ~1KB (per sub)  N/A              ~1KB
```

**Aggregate bandwidth** (single data node at 250K ops/sec):

```
Mixed workload (60% publish, 30% subscribe, 10% read):

Incoming:
├─ Publishes:  150K × 1KB =      150MB/s
├─ Subscribes:  75K × 100B =     7.5MB/s
├─ Reads:       25K × 100B =     2.5MB/s
└─ Total incoming:               160MB/s

Outgoing:
├─ Publish replies:   150K × 200B =     30MB/s
├─ Subscribe replies:  75K × 50KB =   3,750MB/s
├─ Read replies:       25K × 50KB =   1,250MB/s
├─ Broadcasts:        150K × 1KB × 3 = 450MB/s (avg 3 conn nodes)
└─ Total outgoing:                    5,480MB/s

Total bandwidth: 5,640MB/s = ~45 Gbps

⚠️ PROBLEM: Subscribe/Read responses dominate!
```

**Optimization: Snapshot size**:

```
If we limit snapshot to 20 entries per subscribe (not 100):

Subscribe replies: 75K × 10KB = 750MB/s (vs 3,750MB/s)
Read replies:      25K × 10KB = 250MB/s (vs 1,250MB/s)

Total outgoing: 1,480MB/s
Total bandwidth: 1,640MB/s = ~13 Gbps

Still high, but more reasonable with 10Gbps NICs
```

**Better optimization: Client-side pagination**:

```
Client requests snapshot in pages:
├─ Initial subscribe: Small snapshot (20 entries) + position
├─ Client requests more: Paginated ReadSnapshot calls
└─ Spreads bandwidth over time

Result:
├─ Subscribe replies: 75K × 10KB = 750MB/s
├─ Snapshot pages: Amortized over time
└─ Total bandwidth: ~1.5GB/s = ~12 Gbps (manageable)
```

## Comparison: Direct vs Distributed

### Latency Comparison

```
Operation           Direct Memory    With NATS       Overhead
────────────────────────────────────────────────────────────────
Subscribe           ~1µs             ~3.25ms         3,250x
Publish             ~3µs             ~3.2ms          1,067x
ReadSnapshot        ~5µs             ~3.22ms         644x
Update delivery     0 (same node)    ~1.65ms         ∞
```

**Analysis**: Latency overhead is almost entirely network (3ms), not NATS or processing.

### Throughput Comparison

```
Configuration       Channels    Ops/sec      Cost
────────────────────────────────────────────────────────────────
Single node         10K         250K         1 node
3 data nodes        30K         750K         3 nodes + NATS
10 data nodes       100K        2.5M         10 nodes + NATS

Efficiency: Linear scaling ✅
```

### Availability Comparison

```
Configuration       Failure Scenario           Impact
────────────────────────────────────────────────────────────────
Single node         Node crashes               100% downtime
Distributed         1 data node crashes        33% channels affected (3 nodes)
                                               10% channels affected (10 nodes)
                    Connection node crashes    0% (stateless, clients reconnect)
                    NATS node crashes          0% (NATS cluster)
```

## Performance Tuning Recommendations

### 1. NATS Configuration

```yaml
# NATS Server config
max_payload: 10MB        # Large snapshots
max_pending_size: 256MB  # Buffer for connection nodes
write_deadline: "5s"     # Timeout for slow clients

# NATS Client config (connection nodes)
reconnect_buf_size: -1   # Disable buffering (ordering)
max_reconnect: -1        # Infinite reconnects
reconnect_wait: 1s       # Fast reconnect

# NATS Client config (data nodes)
reconnect_buf_size: -1   # Disable buffering (ordering)
max_reconnect: -1        # Infinite reconnects
```

### 2. Connection Node Sharding

```yaml
# Adjust based on channel count
num_shards: 16           # For 10K channels: ~625 per shard
num_shards: 32           # For 100K channels: ~3,125 per shard
num_shards: 64           # For 1M channels: ~15,625 per shard

# Trade-off:
# More shards = better distribution, more NATS subscriptions
# Fewer shards = simpler, less overhead
# Recommended: 16-32 for most deployments
```

### 3. Data Node Memory Limits

```yaml
# Kubernetes Pod resources
resources:
  requests:
    memory: 4Gi      # For ~5K channels
    cpu: 2000m       # For ~500K ops/sec
  limits:
    memory: 8Gi      # Headroom for spikes
    cpu: 4000m       # Burst capacity

# Scale horizontally at:
# - 80% memory utilization (~6.4GB for 8GB limit)
# - ~4K channels per node (conservative)
```

### 4. Snapshot Size Limits

```go
// Limit snapshot size to reduce bandwidth
const (
    MaxSnapshotEntriesOnSubscribe = 20    // Small initial snapshot
    MaxSnapshotEntriesPerPage     = 100   // Pagination limit
    MaxSnapshotSize               = 1MB   // Byte limit
)

// Client-side pagination
func (c *Client) Subscribe(channel string) error {
    // Get initial small snapshot
    resp, _ := c.engine.ReadSnapshot(channel, ReadSnapshotOptions{
        Limit: 20,
    })

    // If more entries exist, paginate
    if resp.HasMore {
        // Fetch in background
        go c.fetchRemainingSnapshot(channel, resp.Position)
    }
}
```

### 5. Connection Pooling

```go
// NATS connection per data node (not shared)
// Avoids contention on single connection

type DataNode struct {
    natsConns []*nats.Conn  // Pool of connections (e.g., 4)
}

func (d *DataNode) publishUpdate(channel string, pub *Publication) {
    // Round-robin across connections
    connIdx := d.nextConnIndex()
    d.natsConns[connIdx].Publish(subject, data)
}
```

## Benchmarks: Expected Performance

### Single Data Node

```
Hardware: 4 CPU cores, 8GB RAM, 1Gbps network

Workload: Mixed (60% publish, 30% subscribe, 10% read)
Channels: 5,000
Entries per channel: 100 (snapshot), 1000 (stream)

Results:
├─ Throughput: 180K ops/sec
├─ CPU utilization: 70%
├─ Memory usage: 3.3GB
├─ Network (outbound): 1.2GB/s
├─ P50 latency: 3.1ms
├─ P99 latency: 8.5ms
└─ P99.9 latency: 25ms

Bottleneck: Network bandwidth (outbound)
```

### 3 Data Nodes

```
Hardware: 4 CPU cores, 8GB RAM, 10Gbps network per node

Workload: Mixed (60% publish, 30% subscribe, 10% read)
Channels: 15,000 (5K per node)
Entries per channel: 100 (snapshot), 1000 (stream)

Results:
├─ Throughput: 540K ops/sec (3 × 180K)
├─ CPU utilization: 70% per node
├─ Memory usage: 3.3GB per node
├─ Network (outbound): 1.2GB/s per node
├─ P50 latency: 3.2ms
├─ P99 latency: 9.0ms
└─ P99.9 latency: 28ms

Scaling efficiency: 100% (linear)
```

### 10 Data Nodes + NATS Cluster

```
Hardware:
- 10 data nodes: 4 CPU cores, 16GB RAM, 10Gbps network
- 3 NATS nodes: 8 CPU cores, 16GB RAM, 10Gbps network

Workload: Mixed (60% publish, 30% subscribe, 10% read)
Channels: 50,000 (5K per node)
Entries per channel: 100 (snapshot), 1000 (stream)

Results:
├─ Throughput: 1.8M ops/sec (10 × 180K)
├─ CPU utilization: 70% per data node, 40% per NATS node
├─ Memory usage: 3.3GB per data node
├─ NATS load: ~7.2M msgs/sec (65% of cluster capacity)
├─ P50 latency: 3.3ms
├─ P99 latency: 10ms
└─ P99.9 latency: 35ms

Bottleneck: Approaching NATS cluster limit
Recommendation: Add more NATS nodes or optimize broadcast fan-out
```

## Summary

### Key Performance Characteristics

**Latency**:
- ✅ **~3ms end-to-end** for most operations (dominated by network)
- ✅ **In-memory operations are negligible** (<1% of latency)
- ⚠️ **Network hops add ~3ms** (acceptable for distributed system)

**Throughput**:
- ✅ **~180K ops/sec per data node** (CPU-bound on publish)
- ✅ **Linear scaling** with data nodes (up to NATS limit)
- ⚠️ **NATS becomes bottleneck at ~10 data nodes** (need cluster)

**Memory**:
- ✅ **~660KB per channel** (22% smaller than Redis)
- ✅ **~3.3GB for 5K channels** (reasonable for modern servers)
- ⚠️ **~66GB for 50K channels** (need horizontal scaling)

**Network**:
- ⚠️ **~12 Gbps per data node** (with snapshot pagination)
- ⚠️ **Snapshot size dominates bandwidth** (need pagination)
- ✅ **Optimizations available** (smaller snapshots, compression)

### Recommended Configuration

```yaml
# For 50K channels, 500K ops/sec

Data Nodes: 10
├─ CPU: 4 cores each
├─ Memory: 8GB each
├─ Network: 10Gbps each
└─ Capacity: ~5K channels, ~50K ops/sec each

Connection Nodes: 20 (auto-scaled)
├─ CPU: 2 cores each
├─ Memory: 4GB each
├─ Shards: 16 per node
└─ Capacity: ~5K connections, ~25K ops/sec each

NATS Cluster: 3 nodes
├─ CPU: 8 cores each
├─ Memory: 16GB each
├─ Network: 10Gbps each
└─ Capacity: ~11M msgs/sec total

Expected performance:
├─ Total throughput: ~500K ops/sec
├─ P99 latency: <10ms
├─ NATS utilization: ~45%
└─ Cost: ~13 nodes + NATS cluster
```

### When to Use This Architecture

**Good fit**:
- ✅ Need horizontal scaling (>10K channels)
- ✅ Need high availability (stateless connection nodes)
- ✅ Rolling updates without downtime
- ✅ Separate scaling of connections vs data
- ✅ Can tolerate 3ms latency overhead

**Not a good fit**:
- ❌ Need ultra-low latency (<1ms)
- ❌ Small scale (<5K channels, single node sufficient)
- ❌ Cannot run NATS cluster
- ❌ Very large snapshots (>1MB per channel)
