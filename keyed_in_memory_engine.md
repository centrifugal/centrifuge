Your Architecture

┌─────────────────────────────────────────────┐
│         Load Balancer                       │
└──────────────┬──────────────────────────────┘
│
┌──────────┴──────────┐
│                     │
┌───▼────────┐      ┌────▼───────┐
│ Connection │      │ Connection │  ← STATELESS
│  Node 1    │      │  Node 2    │  ← Rollout anytime
└───┬────┬───┘      └───┬────┬───┘  ← Auto-scale
│    │              │    │
│    └──────┬───────┘    │
│           │            │
│      ┌────▼────┐       │
│      │ Consul/ │       │      ← Sharding map
│      │ K8s API │       │      ← channel → data node
│      └────┬────┘       │
│           │            │
│    ┌──────┴───────┐    │
│    │              │    │
┌───▼────▼───┐   ┌─────▼────▼────┐
│ Data Node 0│   │ Data Node 1   │  ← STATEFUL
│ (Memory)   │   │ (Memory)      │  ← Rare rollouts
│ Channels   │   │ Channels      │  ← StatefulSet
│ 0,3,6...   │   │ 1,4,7...      │  ← Dedicated memory
└────────────┘   └───────────────┘

Why This is Superior
┌──────────────────────┬──────────────────────────────┬─────────────────────────────────┐
│        Aspect        │ Peer-to-Peer (My suggestion) │        Your Architecture        │
├──────────────────────┼──────────────────────────────┼─────────────────────────────────┤
│ Deployment           │ Single type (stateful)       │ Two types: stateless + stateful │
├──────────────────────┼──────────────────────────────┼─────────────────────────────────┤
│ Rollouts             │ Careful (data loss risk)     │ ✅ Connection nodes anytime!    │
├──────────────────────┼──────────────────────────────┼─────────────────────────────────┤
│ Scaling              │ Together                     │ ✅ Independent scaling          │
├──────────────────────┼──────────────────────────────┼─────────────────────────────────┤
│ Failure blast radius │ All nodes are critical       │ Only data nodes are critical    │
├──────────────────────┼──────────────────────────────┼─────────────────────────────────┤
│ Resource allocation  │ One-size-fits-all            │ ✅ Optimize each tier           │
├──────────────────────┼──────────────────────────────┼─────────────────────────────────┤
│ Complexity           │ Peer mesh + rebalancing      │ ✅ Simpler: client-server       │
└──────────────────────┴──────────────────────────────┴─────────────────────────────────┘
Implementation

1. Sharding Map in Consul/K8s

Option A: K8s StatefulSet (Simplest)

apiVersion: apps/v1
kind: StatefulSet
metadata:
name: centrifugo-data
spec:
serviceName: centrifugo-data
replicas: 3
selector:
matchLabels:
app: centrifugo-data
template:
metadata:
labels:
app: centrifugo-data
role: data
spec:
containers:
- name: centrifugo
image: centrifugo:latest
args:
- --mode=data
- --node-id=$(POD_NAME)
env:
- name: POD_NAME
valueFrom:
fieldRef:
fieldPath: metadata.name
  ---
apiVersion: apps/v1
kind: Deployment
metadata:
name: centrifugo-connection
spec:
replicas: 10  # Scale freely!
selector:
matchLabels:
app: centrifugo-connection
template:
metadata:
labels:
app: centrifugo-connection
role: connection
spec:
containers:
- name: centrifugo
image: centrifugo:latest
args:
- --mode=connection

Why StatefulSet for data nodes?
- ✅ Stable network identities: centrifugo-data-0, centrifugo-data-1, etc.
- ✅ Predictable names for sharding
- ✅ Ordered rollout (one at a time)
- ✅ Persistent storage (optional, for snapshots)

2. Connection Node Implementation

package main

type ConnectionNode struct {
nodeID   string

      // Discovery of data nodes
      dataNodes *DataNodeRegistry
      hashRing  *ConsistentHash

      // RPC client pool to data nodes
      dataClients map[string]*DataNodeClient
}

type DataNodeRegistry struct {
mu    sync.RWMutex
nodes []DataNodeInfo
}

type DataNodeInfo struct {
ID      string
Address string
Status  string  // "ready", "draining", "down"
}

// Initialize - discover data nodes
func (c *ConnectionNode) Start(ctx context.Context) error {
// Option 1: K8s Service discovery
if c.isKubernetes() {
return c.watchK8sDataNodes(ctx)
}

      // Option 2: Consul
      if c.hasConsul() {
          return c.watchConsulDataNodes(ctx)
      }

      // Option 3: Static config
      return c.loadStaticDataNodes()
}

// K8s discovery - predictable StatefulSet names
func (c *ConnectionNode) watchK8sDataNodes(ctx context.Context) error {
// Data nodes are: centrifugo-data-0, centrifugo-data-1, ...
// Each resolves to: centrifugo-data-0.centrifugo-data.default.svc.cluster.local

      // Get number of replicas from StatefulSet
      replicas := c.getStatefulSetReplicas("centrifugo-data")

      nodes := []DataNodeInfo{}
      for i := 0; i < replicas; i++ {
          nodeID := fmt.Sprintf("centrifugo-data-%d", i)
          address := fmt.Sprintf("%s.centrifugo-data.default.svc.cluster.local:8000", nodeID)

          nodes = append(nodes, DataNodeInfo{
              ID:      nodeID,
              Address: address,
              Status:  "ready",
          })
      }

      c.updateDataNodes(nodes)

      // Watch for changes (replica count, pod health)
      return c.watchStatefulSet(ctx, "centrifugo-data")
}

func (c *ConnectionNode) updateDataNodes(nodes []DataNodeInfo) {
c.dataNodes.mu.Lock()
defer c.dataNodes.mu.Unlock()

      c.dataNodes.nodes = nodes

      // Update hash ring
      nodeIDs := make([]string, len(nodes))
      for i, node := range nodes {
          nodeIDs[i] = node.ID
      }
      c.hashRing.UpdateNodes(nodeIDs)

      // Update client pool
      c.updateClientPool(nodes)
}

func (c *ConnectionNode) updateClientPool(nodes []DataNodeInfo) {
// Create/update gRPC connections to data nodes
for _, node := range nodes {
if _, ok := c.dataClients[node.ID]; !ok {
client, err := NewDataNodeClient(node.Address)
if err == nil {
c.dataClients[node.ID] = client
}
}
}

      // Remove old connections
      for id := range c.dataClients {
          found := false
          for _, node := range nodes {
              if node.ID == id {
                  found = true
                  break
              }
          }
          if !found {
              c.dataClients[id].Close()
              delete(c.dataClients, id)
          }
      }
}

// Handle client subscription
func (c *ConnectionNode) Subscribe(channel string) error {
// 1. Hash to find data node
dataNodeID := c.hashRing.GetNode(channel)

      // 2. Get client for that data node
      client := c.dataClients[dataNodeID]
      if client == nil {
          return fmt.Errorf("data node %s not available", dataNodeID)
      }

      // 3. Forward subscribe request
      return client.Subscribe(channel)
}

// Handle publish
func (c *ConnectionNode) Publish(ctx context.Context, channel string, data []byte, opts PublishOptions) (StreamPosition, error) {
dataNodeID := c.hashRing.GetNode(channel)
client := c.dataClients[dataNodeID]

      return client.Publish(ctx, channel, data, opts)
}

3. Data Node Implementation

package main

type DataNode struct {
nodeID string

      // In-memory engine
      engine *MemoryKeyedEngine

      // gRPC server to accept requests from connection nodes
      server *grpc.Server

      // Track which connection nodes have subscribers for which channels
      channelSubscribers map[string]map[string]bool  // channel → set of connection node IDs
      mu                 sync.RWMutex
}

func (d *DataNode) Start() error {
// Register in service discovery
if d.isKubernetes() {
// StatefulSet - nothing to do, DNS is automatic
} else if d.hasConsul() {
d.registerInConsul()
}

      // Start gRPC server
      return d.startGRPCServer()
}

// RPC: Connection node requests subscription
func (d *DataNode) Subscribe(ctx context.Context, req *SubscribeRequest) (*SubscribeResponse, error) {
d.mu.Lock()
defer d.mu.Unlock()

      channel := req.Channel
      connectionNodeID := req.ConnectionNodeId

      // Subscribe locally
      err := d.engine.Subscribe(channel)
      if err != nil {
          return nil, err
      }

      // Track this connection node as subscriber
      if d.channelSubscribers[channel] == nil {
          d.channelSubscribers[channel] = make(map[string]bool)
      }
      d.channelSubscribers[channel][connectionNodeID] = true

      // Get current snapshot
      snapshot, pos, _, err := d.engine.ReadSnapshot(ctx, channel, ReadSnapshotOptions{})

      return &SubscribeResponse{
          Snapshot: snapshot,
          Position: pos,
      }, err
}

// RPC: Publish
func (d *DataNode) Publish(ctx context.Context, req *PublishRequest) (*PublishResponse, error) {
pos, cached, err := d.engine.Publish(ctx, req.Channel, req.Key, req.Data, req.Options)
if err != nil {
return nil, err
}

      // Broadcast update to all connection nodes with subscribers
      d.broadcastUpdate(req.Channel, &Publication{
          Data:   req.Data,
          Offset: pos.Offset,
          // ... other fields
      })

      return &PublishResponse{
          Position: pos,
          Cached:   cached,
      }, nil
}

func (d *DataNode) broadcastUpdate(channel string, pub *Publication) {
d.mu.RLock()
subscribers := d.channelSubscribers[channel]
d.mu.RUnlock()

      if subscribers == nil {
          return
      }

      // Send to all connection nodes with subscribers (parallel)
      var wg sync.WaitGroup
      for connectionNodeID := range subscribers {
          wg.Add(1)
          go func(nodeID string) {
              defer wg.Done()
              d.sendUpdateToConnectionNode(nodeID, channel, pub)
          }(connectionNodeID)
      }
      wg.Wait()
}

4. Simple K8s Discovery

The beauty of StatefulSet:

// Connection node code - ultra simple!
func discoverDataNodes() []string {
// If 3 replicas in StatefulSet, we know the names:
replicas := 3  // Could read from K8s API or config

      nodes := make([]string, replicas)
      for i := 0; i < replicas; i++ {
          // Predictable DNS name
          nodes[i] = fmt.Sprintf("centrifugo-data-%d.centrifugo-data:8000", i)
      }

      return nodes
}

// That's it! No complex service discovery needed.

Even simpler with headless service:

func discoverDataNodes() []string {
// K8s headless service returns ALL pod IPs
addrs, _ := net.LookupHost("centrifugo-data.default.svc.cluster.local")
return addrs  // ["10.1.1.1", "10.1.1.2", "10.1.1.3"]
}

Scaling Examples

Scale Connection Nodes (Easy)

# Black Friday traffic spike
kubectl scale deployment centrifugo-connection --replicas=50

# Zero data migration needed!

Scale Data Nodes (Rare, but managed)

# Growing data/channels
kubectl scale statefulset centrifugo-data --replicas=5

# Connection nodes detect new data nodes
# Hash ring rebalances (some channels move)
# Gradual rebalancing happens

Resource Allocation

# Connection nodes: CPU-heavy (WebSocket handling)
resources:
requests:
cpu: 2000m
memory: 512Mi
limits:
cpu: 4000m
memory: 1Gi

  ---
# Data nodes: Memory-heavy (channel state)
resources:
requests:
cpu: 500m
memory: 8Gi
limits:
cpu: 2000m
memory: 16Gi

Advantages Over Peer-to-Peer

1. Rolling updates: Connection nodes can roll freely
2. Autoscaling: Connection nodes can autoscale on CPU/connections
3. Cost optimization: Different instance types for each tier
4. Simpler failure handling: Connection node failure = just reconnect clients
5. Cleaner separation: Connection logic vs data logic
6. Easier testing: Can test data nodes separately
7. Better monitoring: Separate metrics for each tier

Complete Flow Example

1. Client connects → Load Balancer → Connection Node 7

2. Client subscribes to "chat:123"
   ↓
   Connection Node 7: hash("chat:123") → Data Node 1
   ↓
   gRPC call: DataNode1.Subscribe("chat:123", connNode7)
   ↓
   Data Node 1: stores snapshot in memory, tracks connNode7
   ↓
   Response: snapshot + StreamPosition
   ↓
   Connection Node 7 → Client: snapshot via WebSocket

3. External API publishes to "chat:123" → Connection Node 3
   ↓
   Connection Node 3: hash("chat:123") → Data Node 1
   ↓
   gRPC call: DataNode1.Publish("chat:123", data)
   ↓
   Data Node 1: updates snapshot, gets subscriber list [connNode7, connNode12]
   ↓
   Parallel gRPC calls:
    - connNode7.SendUpdate("chat:123", pub)
    - connNode12.SendUpdate("chat:123", pub)
      ↓
      Connection Nodes → Clients via WebSocket

This is Actually Production Pattern

This architecture is used by:

- Kafka: Brokers (data) + Producers/Consumers (connections)
- Pulsar: Bookies (storage) + Brokers (serving)
- Vitess: vttablets (MySQL) + vtgate (query routing)
- ClickHouse: Distributed tables (data) + Query nodes (serving)

Final Comparison
┌─────────────────┬──────────────────────────────────────┬────────────────────────────┐
│  Architecture   │              Your Idea               │      My Peer-to-Peer       │
├─────────────────┼──────────────────────────────────────┼────────────────────────────┤
│ Simplicity      │ ⭐⭐⭐⭐⭐ Simple client-server      │ ⭐⭐⭐ Complex mesh        │
├─────────────────┼──────────────────────────────────────┼────────────────────────────┤
│ Operability     │ ⭐⭐⭐⭐⭐ Easy rollouts             │ ⭐⭐⭐ Careful rollouts    │
├─────────────────┼──────────────────────────────────────┼────────────────────────────┤
│ Scalability     │ ⭐⭐⭐⭐⭐ Independent scaling       │ ⭐⭐⭐ Scale together      │
├─────────────────┼──────────────────────────────────────┼────────────────────────────┤
│ Performance     │ ⭐⭐⭐⭐ One extra hop               │ ⭐⭐⭐⭐ Direct (if local) │
├─────────────────┼──────────────────────────────────────┼────────────────────────────┤
│ Fault tolerance │ ⭐⭐⭐⭐ Connection nodes disposable │ ⭐⭐⭐ All nodes critical  │
└─────────────────┴──────────────────────────────────────┴────────────────────────────┘
Verdict: Your architecture is superior! 🏆

Go with:
- Connection nodes: Stateless Deployment (auto-scale)
- Data nodes: Stateful StatefulSet (stable, memory-optimized)
- Discovery: K8s API for StatefulSet member list
- Communication: gRPC between tiers

This is cleaner, simpler, and more scalable than peer-to-peer mesh.