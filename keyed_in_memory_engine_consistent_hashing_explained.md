# Consistent Hashing: Data Node Selection Explained

> **Note**: This document focuses on consistent hashing mechanics. For a unified architecture covering **KeyedEngine, Broker, and PresenceManager**, see [distributed_in_memory_architecture.md](distributed_in_memory_architecture.md).

## Overview

**Goal**: Distribute channels across data nodes such that when nodes are added/removed, **only the minimum necessary channels need to move**.

**Solution**: Consistent hashing with virtual nodes (vnodes).

## Basic Concept

### Without Consistent Hashing (Naive Approach)

```go
// BAD: Simple modulo hashing
func getDataNode(channel string, numNodes int) int {
    hash := crc32.ChecksumIEEE([]byte(channel))
    return int(hash % uint32(numNodes))
}

// Example with 3 nodes:
getDataNode("chat:1", 3)  → hash=0x12345678 % 3 = 0  → Node 0
getDataNode("chat:2", 3)  → hash=0xabcdef12 % 3 = 1  → Node 1
getDataNode("chat:3", 3)  → hash=0x87654321 % 3 = 2  → Node 2

// Scaling to 4 nodes - DISASTER:
getDataNode("chat:1", 4)  → hash=0x12345678 % 4 = 0  → Node 0 ✅ stayed
getDataNode("chat:2", 4)  → hash=0xabcdef12 % 4 = 2  → Node 2 ❌ moved!
getDataNode("chat:3", 4)  → hash=0x87654321 % 4 = 1  → Node 1 ❌ moved!

Result: ~75% of channels moved! (All channels are reshuffled)
```

**Problem**: Changing `numNodes` changes the modulo result for most channels. Almost all channels need migration.

### With Consistent Hashing

```go
// GOOD: Consistent hashing
type ConsistentHash struct {
    ring    []uint32          // Sorted vnode hashes
    nodeMap map[uint32]string // vnode hash → physical node
    vnodes  int               // Virtual nodes per physical node (150)
}

// Example with 3 nodes, 150 vnodes each = 450 total vnodes:
// Ring: [0x00001234, 0x00005678, ..., 0xffffffff] (450 sorted hashes)
//
// Each hash points to a node:
// 0x00001234 → "node-0" (vnode 0 of node-0)
// 0x00005678 → "node-1" (vnode 0 of node-1)
// etc.

// Scaling to 4 nodes adds 150 new vnodes
// Ring: [0x00001234, 0x00005678, ..., 0xffffffff] (600 sorted hashes)
//
// Only channels that land in the "gaps" created by new vnodes will move
// Result: ~25% of channels move (only to new node)
```

## Detailed Example: 3 Nodes

### Step 1: Create Virtual Nodes

```go
nodes := []string{"node-0", "node-1", "node-2"}
vnodes := 150  // Per physical node

ring := []uint32{}
nodeMap := map[uint32]string{}

for _, node := range nodes {
    for i := 0; i < vnodes; i++ {
        // Hash: "node-0:0", "node-0:1", ..., "node-0:149"
        key := fmt.Sprintf("%s:%d", node, i)
        hash := crc32.ChecksumIEEE([]byte(key))

        ring = append(ring, hash)
        nodeMap[hash] = node
    }
}

// Sort the ring
sort.Slice(ring, func(i, j int) bool {
    return ring[i] < ring[j]
})

// Result: 450 vnodes distributed across the hash space
```

### Step 2: Visualize the Hash Ring

```
Hash Space: 0x00000000 → 0xffffffff (4.3 billion values)

Ring with 3 nodes (450 vnodes):
┌────────────────────────────────────────────────────────────┐
│ 0x00000000                                      0xffffffff │
└────────────────────────────────────────────────────────────┘
  ↓         ↓    ↓      ↓   ↓    ↓         ↓    ↓      ↓
  node-0    node-1      node-2   node-0    node-1      node-2
  (vn:0)    (vn:0)      (vn:0)   (vn:1)    (vn:1)      (vn:1)

Actual hash ring (simplified, showing first 10 vnodes):
[
  0x0012ab34 → node-1 (vnode 47 of node-1)
  0x0034cd56 → node-0 (vnode 12 of node-0)
  0x0056ef78 → node-2 (vnode 89 of node-2)
  0x00789012 → node-0 (vnode 134 of node-0)
  0x009abc34 → node-1 (vnode 3 of node-1)
  0x00bcde56 → node-2 (vnode 56 of node-2)
  0x00def678 → node-0 (vnode 91 of node-0)
  0x0123ab90 → node-1 (vnode 127 of node-1)
  0x0145cd12 → node-2 (vnode 14 of node-2)
  0x0167ef34 → node-0 (vnode 68 of node-0)
  ...
  (450 total vnodes)
]
```

### Step 3: Map Channels to Nodes

```go
func GetNode(channel string) string {
    // Hash the channel
    channelHash := crc32.ChecksumIEEE([]byte(channel))

    // Binary search for first vnode with hash >= channelHash
    idx := sort.Search(len(ring), func(i int) bool {
        return ring[i] >= channelHash
    })

    // Wrap around if past the end
    if idx == len(ring) {
        idx = 0
    }

    return nodeMap[ring[idx]]
}
```

**Example Mappings:**

```
Channel: "chat:1"
├─ hash("chat:1") = 0x3f7a2b1c
├─ Binary search in ring: finds first vnode >= 0x3f7a2b1c
├─ Found: ring[87] = 0x3f7b0000 → node-1
└─ Result: "chat:1" maps to node-1

Channel: "chat:2"
├─ hash("chat:2") = 0x8a4f6d3e
├─ Binary search: finds ring[234] = 0x8a500000 → node-0
└─ Result: "chat:2" maps to node-0

Channel: "chat:3"
├─ hash("chat:3") = 0xc2d1e5f0
├─ Binary search: finds ring[389] = 0xc2d20000 → node-2
└─ Result: "chat:3" maps to node-2
```

### Visual Representation

```
Hash Ring (3 nodes, simplified):
┌────────────────────────────────────────────────────────────┐
│ 0x00000000                                      0xffffffff │
└────────────────────────────────────────────────────────────┘
                      ↓                    ↓              ↓
                  "chat:1"            "chat:2"       "chat:3"
                  0x3f7a2b1c          0x8a4f6d3e     0xc2d1e5f0
                      ↓                    ↓              ↓
            Next vnode: 0x3f7b0000  0x8a500000     0xc2d20000
                      ↓                    ↓              ↓
                   node-1               node-0         node-2

Distribution (with 150 vnodes per node):
node-0: ~150 channels (33%)
node-1: ~150 channels (33%)
node-2: ~150 channels (33%)
```

## Scaling: 3 Nodes → 4 Nodes

### Step 1: Add New Node's Virtual Nodes

```go
// Old nodes
nodes := []string{"node-0", "node-1", "node-2"}

// New nodes (scaling)
newNodes := []string{"node-0", "node-1", "node-2", "node-3"}

// Add 150 vnodes for node-3
for i := 0; i < 150; i++ {
    key := fmt.Sprintf("node-3:%d", i)
    hash := crc32.ChecksumIEEE([]byte(key))

    ring = append(ring, hash)
    nodeMap[hash] = "node-3"
}

// Re-sort the ring
sort.Slice(ring, func(i, j int) bool {
    return ring[i] < ring[j]
})

// Result: 600 vnodes (150 * 4 nodes)
```

### Step 2: Visualize What Changed

```
Old Ring (450 vnodes - 3 nodes):
┌────────────────────────────────────────────────────────────┐
│ 0x00000000                                      0xffffffff │
└────────────────────────────────────────────────────────────┘
  ↓         ↓         ↓         ↓         ↓         ↓
  node-0    node-1    node-2    node-0    node-1    node-2
  33%       33%       33%       coverage

New Ring (600 vnodes - 4 nodes):
┌────────────────────────────────────────────────────────────┐
│ 0x00000000                                      0xffffffff │
└────────────────────────────────────────────────────────────┘
  ↓    ↓    ↓    ↓    ↓    ↓    ↓    ↓    ↓    ↓    ↓    ↓
  n-0  n-3  n-1  n-3  n-2  n-3  n-0  n-3  n-1  n-3  n-2  n-3
  25%  25%  25%  25%  evenly distributed

New vnodes from node-3 inserted throughout the ring
```

### Step 3: Channel Remapping

**Key Insight**: A channel only moves if a new vnode is inserted **before** its previous vnode in the ring.

```
Example 1: "chat:1" stays on node-1
────────────────────────────────────────
Old ring:
  ... → 0x3f700000 (node-0) → 0x3f7b0000 (node-1) → 0x3f900000 (node-2) → ...
                                   ↑
                            "chat:1" lands here
                            hash = 0x3f7a2b1c

New ring:
  ... → 0x3f700000 (node-0) → 0x3f7b0000 (node-1) → 0x3f900000 (node-2) → ...
                                   ↑
                            "chat:1" still lands here
                            (no new vnode inserted before 0x3f7b0000)

Result: "chat:1" stays on node-1 ✅


Example 2: "chat:2" moves to node-3
────────────────────────────────────────
Old ring:
  ... → 0x8a400000 (node-2) → 0x8a500000 (node-0) → 0x8a600000 (node-1) → ...
                                   ↑
                            "chat:2" lands here
                            hash = 0x8a4f6d3e

New ring:
  ... → 0x8a400000 (node-2) → 0x8a4f0000 (node-3) → 0x8a500000 (node-0) → ...
                                   ↑
                            "chat:2" now lands here!
                            (new vnode 0x8a4f0000 inserted)

Result: "chat:2" moves from node-0 → node-3 ❌


Example 3: "chat:3" stays on node-2
────────────────────────────────────────
Old ring:
  ... → 0xc2d00000 (node-1) → 0xc2d20000 (node-2) → 0xc2e00000 (node-0) → ...
                                   ↑
                            "chat:3" lands here
                            hash = 0xc2d1e5f0

New ring:
  ... → 0xc2d00000 (node-1) → 0xc2d20000 (node-2) → 0xc2e00000 (node-0) → ...
                                   ↑
                            "chat:3" still lands here
                            (no new vnode inserted before 0xc2d20000)

Result: "chat:3" stays on node-2 ✅
```

### Step 4: Statistical Analysis

With 150 vnodes per node:

```
Total vnodes in old ring: 450 (3 nodes * 150)
Total vnodes in new ring: 600 (4 nodes * 150)
New vnodes added: 150 (node-3's vnodes)

Hash space coverage per vnode (on average):
Old: 4,294,967,296 / 450 ≈ 9,544,372 hash values per vnode
New: 4,294,967,296 / 600 ≈ 7,158,279 hash values per vnode

Channels affected:
- For each new vnode inserted, channels in its coverage range move
- 150 new vnodes inserted → ~25% of hash space now covered by node-3
- Therefore: ~25% of channels move to node-3
- Remaining: ~75% of channels stay on original nodes

Expected distribution after scaling:
node-0: 33% → 25% (lost ~8% to node-3)
node-1: 33% → 25% (lost ~8% to node-3)
node-2: 33% → 25% (lost ~8% to node-3)
node-3:  0% → 25% (gained ~25% from others)

Total moved: ~25% of all channels
```

## Concrete Example with Real Channels

### Initial State: 3 Nodes

```
Channels and their mappings (sample of 12 channels):

chat:1    → hash=0x12ab3456 → node-0
chat:2    → hash=0x34cd7890 → node-1
chat:3    → hash=0x56ef1234 → node-2
chat:4    → hash=0x78905678 → node-0
chat:5    → hash=0x9abc9012 → node-1
chat:6    → hash=0xbcde3456 → node-2
chat:7    → hash=0xdef07890 → node-0
chat:8    → hash=0x0123bcde → node-1
chat:9    → hash=0x23450123 → node-2
chat:10   → hash=0x45674567 → node-0
chat:11   → hash=0x6789890a → node-1
chat:12   → hash=0x89abcdef → node-2

Distribution:
node-0: [chat:1, chat:4, chat:7, chat:10]     (4 channels)
node-1: [chat:2, chat:5, chat:8, chat:11]     (4 channels)
node-2: [chat:3, chat:6, chat:9, chat:12]     (4 channels)
```

### After Scaling to 4 Nodes

```
Same channels, new mappings (node-3 vnodes inserted):

chat:1    → hash=0x12ab3456 → node-0  ✅ stayed
chat:2    → hash=0x34cd7890 → node-3  ❌ moved (from node-1)
chat:3    → hash=0x56ef1234 → node-2  ✅ stayed
chat:4    → hash=0x78905678 → node-0  ✅ stayed
chat:5    → hash=0x9abc9012 → node-1  ✅ stayed
chat:6    → hash=0xbcde3456 → node-3  ❌ moved (from node-2)
chat:7    → hash=0xdef07890 → node-0  ✅ stayed
chat:8    → hash=0x0123bcde → node-1  ✅ stayed
chat:9    → hash=0x23450123 → node-3  ❌ moved (from node-2)
chat:10   → hash=0x45674567 → node-0  ✅ stayed
chat:11   → hash=0x6789890a → node-1  ✅ stayed
chat:12   → hash=0x89abcdef → node-2  ✅ stayed

New distribution:
node-0: [chat:1, chat:4, chat:7, chat:10]     (4 channels) ← no change
node-1: [chat:5, chat:8, chat:11]             (3 channels) ← lost chat:2
node-2: [chat:3, chat:12]                     (2 channels) ← lost chat:6, chat:9
node-3: [chat:2, chat:6, chat:9]              (3 channels) ← new channels

Channels moved: 3 out of 12 = 25% ✅
Channels stable: 9 out of 12 = 75% ✅
```

## How Connection Nodes Handle Scaling

### Connection Node Code

```go
type ConnectionNode struct {
    hashRing *ConsistentHash
    k8s      *kubernetes.Clientset
}

func (c *ConnectionNode) Start(ctx context.Context) error {
    // Initialize hash ring with current data nodes
    c.hashRing = NewConsistentHash(150)

    dataNodes := c.getDataNodesFromK8s()
    c.hashRing.UpdateNodes(dataNodes)

    // Watch for changes
    go c.watchStatefulSet(ctx, "centrifugo-data")

    return nil
}

func (c *ConnectionNode) watchStatefulSet(ctx context.Context, name string) error {
    watcher, _ := c.k8s.AppsV1().StatefulSets("default").Watch(ctx, metav1.ListOptions{
        FieldSelector: fmt.Sprintf("metadata.name=%s", name),
    })

    for event := range watcher.ResultChan() {
        sts := event.Object.(*appsv1.StatefulSet)
        newReplicas := int(*sts.Spec.Replicas)

        if newReplicas != c.currentReplicas {
            log.Printf("Data nodes changed: %d → %d", c.currentReplicas, newReplicas)

            // Get new node list
            newNodes := make([]string, newReplicas)
            for i := 0; i < newReplicas; i++ {
                newNodes[i] = fmt.Sprintf("centrifugo-data-%d", i)
            }

            // Update hash ring - ATOMICALLY
            c.hashRing.UpdateNodes(newNodes)

            log.Printf("Hash ring updated with %d nodes", newReplicas)
            c.currentReplicas = newReplicas
        }
    }

    return nil
}

func (c *ConnectionNode) Subscribe(ctx context.Context, channel string) (*SubscribeResponse, error) {
    // This call uses the CURRENT hash ring
    dataNodeID := c.hashRing.GetNode(channel)
    nodeIndex := c.hashRing.GetNodeIndex(dataNodeID)

    // Send subscribe to correct data node
    subject := fmt.Sprintf("data.%d.subscribe", nodeIndex)
    // ... rest of subscribe logic
}
```

### Timeline of Events During Scaling

```
Time T0: Stable state with 3 data nodes
─────────────────────────────────────────
Connection Node:
  hashRing.nodes = ["centrifugo-data-0", "centrifugo-data-1", "centrifugo-data-2"]

Client request: Subscribe("chat:1")
  ├─ hashRing.GetNode("chat:1") = "centrifugo-data-1"
  ├─ Send NATS request to: data.1.subscribe
  └─ Data Node 1 handles request ✅


Time T1: Operator scales StatefulSet
─────────────────────────────────────────
$ kubectl scale statefulset centrifugo-data --replicas=4

K8s:
  ├─ Creates new pod: centrifugo-data-3
  ├─ Pod starts, connects to NATS
  └─ StatefulSet watch event fires


Time T2: Connection nodes detect change (within 1-2 seconds)
─────────────────────────────────────────
Connection Node:
  ├─ Receives StatefulSet watch event
  ├─ Sees: replicas changed from 3 → 4
  ├─ Calls: hashRing.UpdateNodes(["centrifugo-data-0", ..., "centrifugo-data-3"])
  └─ Hash ring rebuilt with 600 vnodes

  Old: 450 vnodes (3 nodes)
  New: 600 vnodes (4 nodes)

  Log: "Hash ring updated with 4 nodes"


Time T3: New client requests use new hash ring
─────────────────────────────────────────
Client request: Subscribe("chat:2")
  ├─ hashRing.GetNode("chat:2") = "centrifugo-data-3"  ← NEW NODE!
  ├─ Send NATS request to: data.3.subscribe
  └─ Data Node 3 handles request ✅

Client request: Subscribe("chat:1")
  ├─ hashRing.GetNode("chat:1") = "centrifugo-data-1"  ← SAME NODE
  ├─ Send NATS request to: data.1.subscribe
  └─ Data Node 1 handles request ✅


Time T4: Old subscriptions remain active
─────────────────────────────────────────
Clients already subscribed to "chat:2" on Data Node 1:
  ├─ Still receiving updates from Data Node 1
  ├─ Updates still published to: conn.abc123.7
  └─ Will migrate when client reconnects OR channel becomes idle


Time T5: Channel migration (optional/lazy)
─────────────────────────────────────────
Option 1: Client reconnects
  ├─ Client reconnects, subscribes to "chat:2"
  ├─ New hashRing.GetNode("chat:2") = "centrifugo-data-3"
  ├─ Subscribe goes to Data Node 3
  └─ Data Node 1 eventually unsubscribes (no more clients)

Option 2: Proactive migration (if implemented)
  ├─ Data Node 1 detects "chat:2" should be on Node 3
  ├─ Migrates state via NATS: data.3.migrate
  ├─ Data Node 3 takes over
  └─ Updates subscriber mapping
```

## Why This Works

### 1. Minimal Data Movement

```
3 → 4 nodes:  25% of channels move
4 → 5 nodes:  20% of channels move
5 → 6 nodes:  16.7% of channels move

Formula: 1 / new_node_count
```

### 2. Deterministic Mapping

```
All connection nodes have identical hash rings
└─ Same channel always maps to same data node
   └─ No coordination needed between connection nodes
```

### 3. Atomic Updates

```
hashRing.UpdateNodes() is atomic
├─ Old ring used until update completes
├─ New ring used immediately after
└─ No intermediate state where nodes disagree
```

### 4. Lazy Migration

```
Channel "chat:2" mapped to different node after scaling:

Old subscriptions (Data Node 1):
├─ Continue working
├─ Still receive updates
└─ Migrate on reconnect or idle timeout

New subscriptions (Data Node 3):
├─ Go to new node immediately
├─ Build new state
└─ Eventually become authoritative
```

## Implementation Details

### Hash Ring Update Algorithm

```go
func (ch *ConsistentHash) UpdateNodes(newNodes []string) {
    ch.mu.Lock()
    defer ch.mu.Unlock()

    // Build new ring from scratch
    newRing := make([]uint32, 0, len(newNodes)*ch.vnodes)
    newNodeMap := make(map[uint32]string)

    for _, node := range newNodes {
        for i := 0; i < ch.vnodes; i++ {
            key := fmt.Sprintf("%s:%d", node, i)
            hash := crc32.ChecksumIEEE([]byte(key))
            newRing = append(newRing, hash)
            newNodeMap[hash] = node
        }
    }

    // Sort
    sort.Slice(newRing, func(i, j int) bool {
        return newRing[i] < newRing[j]
    })

    // ATOMIC SWAP
    ch.ring = newRing
    ch.nodeMap = newNodeMap
    ch.nodes = newNodes

    // After this point, all GetNode() calls use the new ring
}
```

### GetNode is Lock-Free Read

```go
func (ch *ConsistentHash) GetNode(channel string) string {
    ch.mu.RLock()
    defer ch.mu.RUnlock()

    if len(ch.ring) == 0 {
        return ""
    }

    hash := crc32.ChecksumIEEE([]byte(channel))

    // Binary search: O(log N) where N = total vnodes (e.g., 600)
    // O(log 600) ≈ 9 comparisons
    idx := sort.Search(len(ch.ring), func(i int) bool {
        return ch.ring[i] >= hash
    })

    if idx == len(ch.ring) {
        idx = 0  // Wrap around
    }

    return ch.nodeMap[ch.ring[idx]]
}
```

### Performance

```
Operation: GetNode(channel)
├─ Hash channel: ~50ns
├─ Binary search: ~200ns (log 600 comparisons)
├─ Map lookup: ~10ns
└─ Total: ~260ns per call

With RWMutex:
├─ Read lock: ~20ns
├─ Read unlock: ~20ns
└─ Total: ~300ns per call

Throughput: ~3.3 million GetNode() calls/sec per CPU core
```

## Service Discovery: How Connection Nodes Learn About Data Nodes

### The Problem

Connection nodes need to:
1. **Discover** which data nodes exist (initial state)
2. **Detect changes** when data nodes are added/removed/scaled
3. **Update hash ring** to reflect the new topology
4. **Do this fast** (within 1-2 seconds of change)

### Solution Options

#### Option 1: Kubernetes API Watch (Recommended for K8s)

**How it works**: Connection nodes watch the StatefulSet resource for changes.

```go
import (
    metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
    "k8s.io/client-go/kubernetes"
)

type ConnectionNode struct {
    k8s          *kubernetes.Clientset
    hashRing     *ConsistentHash
    statefulSet  string  // "centrifugo-data"
    namespace    string  // "default"
}

func (c *ConnectionNode) Start(ctx context.Context) error {
    // 1. Get initial state
    sts, err := c.k8s.AppsV1().StatefulSets(c.namespace).Get(ctx, c.statefulSet, metav1.GetOptions{})
    if err != nil {
        return err
    }

    initialReplicas := int(*sts.Spec.Replicas)
    initialNodes := make([]string, initialReplicas)
    for i := 0; i < initialReplicas; i++ {
        initialNodes[i] = fmt.Sprintf("centrifugo-data-%d", i)
    }

    // Initialize hash ring
    c.hashRing = NewConsistentHash(150)
    c.hashRing.UpdateNodes(initialNodes)

    log.Printf("Connection node started: discovered %d data nodes", initialReplicas)

    // 2. Watch for changes
    go c.watchStatefulSet(ctx)

    return nil
}

func (c *ConnectionNode) watchStatefulSet(ctx context.Context) {
    for {
        watcher, err := c.k8s.AppsV1().StatefulSets(c.namespace).Watch(ctx, metav1.ListOptions{
            FieldSelector: fmt.Sprintf("metadata.name=%s", c.statefulSet),
        })
        if err != nil {
            log.Printf("Failed to create StatefulSet watch: %v", err)
            time.Sleep(5 * time.Second)
            continue
        }

        for event := range watcher.ResultChan() {
            sts, ok := event.Object.(*appsv1.StatefulSet)
            if !ok {
                continue
            }

            newReplicas := int(*sts.Spec.Replicas)
            currentReplicas := len(c.hashRing.nodes)

            if newReplicas != currentReplicas {
                log.Printf("Data nodes changed: %d → %d", currentReplicas, newReplicas)

                // Build new node list
                newNodes := make([]string, newReplicas)
                for i := 0; i < newReplicas; i++ {
                    newNodes[i] = fmt.Sprintf("centrifugo-data-%d", i)
                }

                // Update hash ring atomically
                c.hashRing.UpdateNodes(newNodes)

                log.Printf("Hash ring updated: %d vnodes (%d nodes × 150)",
                    len(c.hashRing.ring), newReplicas)
            }
        }

        // Watch connection lost, reconnect
        log.Printf("StatefulSet watch connection lost, reconnecting...")
        time.Sleep(1 * time.Second)
    }
}
```

**Advantages**:
- ✅ **Native to K8s**: No extra infrastructure
- ✅ **Real-time**: Watch API fires events within ~100-500ms
- ✅ **Reliable**: K8s guarantees event delivery
- ✅ **Simple**: Just watch one resource
- ✅ **Source of truth**: StatefulSet replica count is authoritative

**Disadvantages**:
- ❌ **K8s-only**: Doesn't work outside Kubernetes
- ❌ **RBAC required**: Connection pods need `get` and `watch` permissions

**K8s RBAC Setup**:

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: centrifugo-connection
  namespace: default
rules:
- apiGroups: ["apps"]
  resources: ["statefulsets"]
  verbs: ["get", "watch"]
  resourceNames: ["centrifugo-data"]  # Specific StatefulSet only
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: centrifugo-connection
  namespace: default
subjects:
- kind: ServiceAccount
  name: centrifugo-connection
  namespace: default
roleRef:
  kind: Role
  name: centrifugo-connection
  apiGroup: rbac.authorization.k8s.io
```

#### Option 2: Headless Service DNS (K8s Alternative)

**How it works**: Query DNS for all pod IPs in a headless service.

```go
func (c *ConnectionNode) discoverDataNodesViaDNS() ([]string, error) {
    // Headless service DNS returns ALL pod IPs
    // centrifugo-data.default.svc.cluster.local
    ips, err := net.LookupHost("centrifugo-data.default.svc.cluster.local")
    if err != nil {
        return nil, err
    }

    // Sort IPs to get deterministic ordering (matches StatefulSet ordinals)
    sort.Strings(ips)

    nodes := make([]string, len(ips))
    for i, ip := range ips {
        // Assuming StatefulSet pods are created in order
        nodes[i] = fmt.Sprintf("centrifugo-data-%d", i)
    }

    return nodes, nil
}

func (c *ConnectionNode) pollDNS(ctx context.Context, interval time.Duration) {
    ticker := time.NewTicker(interval)
    defer ticker.Stop()

    for {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
            nodes, err := c.discoverDataNodesViaDNS()
            if err != nil {
                log.Printf("DNS lookup failed: %v", err)
                continue
            }

            currentNodes := c.hashRing.nodes
            if !reflect.DeepEqual(nodes, currentNodes) {
                log.Printf("Data nodes changed via DNS: %d → %d",
                    len(currentNodes), len(nodes))
                c.hashRing.UpdateNodes(nodes)
            }
        }
    }
}
```

**Advantages**:
- ✅ **No RBAC needed**: Just DNS queries
- ✅ **Simple**: No K8s API client
- ✅ **Universal**: Works in any K8s environment

**Disadvantages**:
- ❌ **Polling**: Not real-time (typically poll every 5-10 seconds)
- ❌ **DNS caching**: May have stale results
- ❌ **Ordering assumptions**: Assumes IPs map to ordinals

#### Option 3: Consul Service Discovery (For Non-K8s)

**How it works**: Data nodes register in Consul, connection nodes watch the service.

```go
import (
    consulapi "github.com/hashicorp/consul/api"
)

type ConnectionNode struct {
    consul   *consulapi.Client
    hashRing *ConsistentHash
}

func (c *ConnectionNode) Start(ctx context.Context) error {
    // Get initial state
    services, _, err := c.consul.Health().Service("centrifugo-data", "", true, nil)
    if err != nil {
        return err
    }

    nodes := make([]string, len(services))
    for i, svc := range services {
        // Assuming service meta includes node index
        nodeIndex := svc.Service.Meta["node_index"]
        nodes[i] = fmt.Sprintf("centrifugo-data-%s", nodeIndex)
    }

    c.hashRing = NewConsistentHash(150)
    c.hashRing.UpdateNodes(nodes)

    // Watch for changes
    go c.watchConsulService(ctx)

    return nil
}

func (c *ConnectionNode) watchConsulService(ctx context.Context) {
    var waitIndex uint64

    for {
        // Blocking query with long polling
        services, meta, err := c.consul.Health().Service(
            "centrifugo-data",
            "",
            true, // passing only
            &consulapi.QueryOptions{
                WaitIndex: waitIndex,
                WaitTime:  5 * time.Minute,
            },
        )
        if err != nil {
            log.Printf("Consul watch error: %v", err)
            time.Sleep(5 * time.Second)
            continue
        }

        waitIndex = meta.LastIndex

        nodes := make([]string, len(services))
        for i, svc := range services {
            nodeIndex := svc.Service.Meta["node_index"]
            nodes[i] = fmt.Sprintf("centrifugo-data-%s", nodeIndex)
        }

        currentNodes := c.hashRing.nodes
        if !reflect.DeepEqual(nodes, currentNodes) {
            log.Printf("Data nodes changed via Consul: %d → %d",
                len(currentNodes), len(nodes))
            c.hashRing.UpdateNodes(nodes)
        }
    }
}
```

**Data node registration**:

```go
func (d *DataNode) registerInConsul() error {
    registration := &consulapi.AgentServiceRegistration{
        ID:   fmt.Sprintf("centrifugo-data-%d", d.nodeIndex),
        Name: "centrifugo-data",
        Port: 8000,
        Meta: map[string]string{
            "node_index": strconv.Itoa(d.nodeIndex),
        },
        Check: &consulapi.AgentServiceCheck{
            NATS: "nats://localhost:4222",  // Health check via NATS
            Interval: "10s",
            Timeout:  "2s",
        },
    }

    return d.consul.Agent().ServiceRegister(registration)
}
```

**Advantages**:
- ✅ **Platform-agnostic**: Works anywhere (VMs, bare metal, K8s)
- ✅ **Health checks**: Automatic unhealthy node removal
- ✅ **Real-time**: Blocking queries provide instant updates
- ✅ **Service mesh ready**: Integrates with Consul Connect

**Disadvantages**:
- ❌ **Extra infrastructure**: Requires Consul cluster
- ❌ **More complex**: Registration + deregistration logic needed

#### Option 4: NATS-Based Discovery

**How it works**: Data nodes send heartbeats via NATS, connection nodes track them.

```go
// Data Node: Send heartbeats
func (d *DataNode) sendHeartbeats(ctx context.Context) {
    ticker := time.NewTicker(5 * time.Second)
    defer ticker.Stop()

    for {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
            heartbeat := &Heartbeat{
                NodeID:    fmt.Sprintf("centrifugo-data-%d", d.nodeIndex),
                NodeIndex: d.nodeIndex,
                Timestamp: time.Now().Unix(),
            }
            data, _ := json.Marshal(heartbeat)
            d.nats.Publish("data.heartbeat", data)
        }
    }
}

// Connection Node: Track heartbeats
func (c *ConnectionNode) trackHeartbeats(ctx context.Context) {
    c.nats.Subscribe("data.heartbeat", func(msg *nats.Msg) {
        var hb Heartbeat
        json.Unmarshal(msg.Data, &hb)

        c.mu.Lock()
        c.liveNodes[hb.NodeID] = time.Now()
        c.mu.Unlock()
    })

    // Expire stale nodes every 10 seconds
    ticker := time.NewTicker(10 * time.Second)
    defer ticker.Stop()

    for {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
            c.mu.Lock()
            now := time.Now()
            currentNodes := []string{}

            for nodeID, lastSeen := range c.liveNodes {
                if now.Sub(lastSeen) < 15*time.Second {
                    currentNodes = append(currentNodes, nodeID)
                } else {
                    delete(c.liveNodes, nodeID)
                }
            }
            c.mu.Unlock()

            sort.Strings(currentNodes)
            if !reflect.DeepEqual(currentNodes, c.hashRing.nodes) {
                log.Printf("Data nodes changed via heartbeats: %d → %d",
                    len(c.hashRing.nodes), len(currentNodes))
                c.hashRing.UpdateNodes(currentNodes)
            }
        }
    }
}
```

**Advantages**:
- ✅ **No extra infrastructure**: Uses existing NATS
- ✅ **Platform-agnostic**: Works anywhere
- ✅ **Simple**: Just pub/sub

**Disadvantages**:
- ❌ **Not instant**: Relies on timeout detection (15s lag)
- ❌ **Heartbeat overhead**: Extra NATS messages
- ❌ **Split-brain risk**: Network partition can cause confusion

### Comparison Table

```
┌──────────────────┬──────────────┬────────────┬──────────────┬─────────────┐
│     Method       │  Platform    │ Latency    │ Complexity   │ Reliability │
├──────────────────┼──────────────┼────────────┼──────────────┼─────────────┤
│ K8s API Watch    │ K8s only     │ ~500ms     │ Low          │ Excellent   │
├──────────────────┼──────────────┼────────────┼──────────────┼─────────────┤
│ Headless DNS     │ K8s only     │ 5-10s      │ Very Low     │ Good        │
├──────────────────┼──────────────┼────────────┼──────────────┼─────────────┤
│ Consul           │ Any          │ ~100ms     │ Medium       │ Excellent   │
├──────────────────┼──────────────┼────────────┼──────────────┼─────────────┤
│ NATS Heartbeats  │ Any          │ 5-15s      │ Low          │ Fair        │
└──────────────────┴──────────────┴────────────┴──────────────┴─────────────┘
```

### Recommended Approach: K8s API Watch

**For Kubernetes deployments** (which your architecture targets), use StatefulSet watch:

```go
type ConnectionNode struct {
    k8s      *kubernetes.Clientset
    hashRing *ConsistentHash

    // Config
    dataStatefulSet string  // "centrifugo-data"
    namespace       string  // "default"
}

func (c *ConnectionNode) Start(ctx context.Context) error {
    // 1. Initialize K8s client
    config, err := rest.InClusterConfig()
    if err != nil {
        return err
    }
    c.k8s, err = kubernetes.NewForConfig(config)
    if err != nil {
        return err
    }

    // 2. Get initial data node count
    sts, err := c.k8s.AppsV1().StatefulSets(c.namespace).Get(
        ctx, c.dataStatefulSet, metav1.GetOptions{})
    if err != nil {
        return err
    }

    replicas := int(*sts.Spec.Replicas)
    nodes := make([]string, replicas)
    for i := 0; i < replicas; i++ {
        nodes[i] = fmt.Sprintf("%s-%d", c.dataStatefulSet, i)
    }

    // 3. Initialize hash ring
    c.hashRing = NewConsistentHash(150)
    c.hashRing.UpdateNodes(nodes)

    log.Printf("Initialized with %d data nodes: %v", replicas, nodes)

    // 4. Watch for changes (non-blocking)
    go c.watchStatefulSetForever(ctx)

    return nil
}

func (c *ConnectionNode) watchStatefulSetForever(ctx context.Context) {
    backoff := 1 * time.Second
    maxBackoff := 60 * time.Second

    for {
        select {
        case <-ctx.Done():
            return
        default:
        }

        err := c.watchStatefulSetOnce(ctx)
        if err != nil {
            log.Printf("StatefulSet watch error: %v, retrying in %v", err, backoff)
            time.Sleep(backoff)

            // Exponential backoff
            backoff *= 2
            if backoff > maxBackoff {
                backoff = maxBackoff
            }
        } else {
            // Reset backoff on successful watch
            backoff = 1 * time.Second
        }
    }
}

func (c *ConnectionNode) watchStatefulSetOnce(ctx context.Context) error {
    watcher, err := c.k8s.AppsV1().StatefulSets(c.namespace).Watch(ctx,
        metav1.ListOptions{
            FieldSelector: fmt.Sprintf("metadata.name=%s", c.dataStatefulSet),
        })
    if err != nil {
        return err
    }
    defer watcher.Stop()

    log.Printf("StatefulSet watch established")

    for event := range watcher.ResultChan() {
        sts, ok := event.Object.(*appsv1.StatefulSet)
        if !ok {
            continue
        }

        newReplicas := int(*sts.Spec.Replicas)
        currentNodes := c.hashRing.nodes

        if newReplicas != len(currentNodes) {
            log.Printf("Data nodes scaling: %d → %d", len(currentNodes), newReplicas)

            newNodes := make([]string, newReplicas)
            for i := 0; i < newReplicas; i++ {
                newNodes[i] = fmt.Sprintf("%s-%d", c.dataStatefulSet, i)
            }

            // Atomic update
            c.hashRing.UpdateNodes(newNodes)

            log.Printf("Hash ring updated: %d nodes, %d vnodes",
                newReplicas, len(c.hashRing.ring))
        }
    }

    return fmt.Errorf("watch channel closed")
}
```

### Timeline: Complete Scaling Flow

```
Time T0: Stable state
─────────────────────────────────────────
K8s:
  StatefulSet centrifugo-data: replicas=3
  Pods: centrifugo-data-0, centrifugo-data-1, centrifugo-data-2

Connection Nodes (all instances):
  hashRing.nodes = [centrifugo-data-0, centrifugo-data-1, centrifugo-data-2]
  hashRing.ring = [450 vnodes]


Time T1: Operator initiates scaling
─────────────────────────────────────────
$ kubectl scale statefulset centrifugo-data --replicas=4

K8s API Server:
  ├─ Updates StatefulSet spec: replicas=4
  ├─ Fires watch event to all watchers
  └─ Starts creating centrifugo-data-3


Time T2: Connection nodes receive watch event (~100-500ms later)
─────────────────────────────────────────
Connection Node A:
  ├─ Watch handler receives event
  ├─ Extracts: sts.Spec.Replicas = 4
  ├─ Compares: 4 != 3 (current)
  ├─ Builds: newNodes = [centrifugo-data-0, ..., centrifugo-data-3]
  ├─ Calls: hashRing.UpdateNodes(newNodes)
  │   ├─ Generates 600 vnodes (4 nodes × 150)
  │   ├─ Sorts ring
  │   └─ Atomic swap
  └─ Log: "Hash ring updated: 4 nodes, 600 vnodes"

Connection Node B:
  ├─ (Same process, independent)
  └─ Hash ring updated identically

Connection Node C:
  ├─ (Same process, independent)
  └─ Hash ring updated identically

All connection nodes now agree: 4 data nodes


Time T3: K8s creates new pod (~10-30s later)
─────────────────────────────────────────
K8s:
  ├─ Schedules centrifugo-data-3
  ├─ Pod starts
  ├─ Container runs
  └─ Data node connects to NATS, listens on data.3.*

Connection nodes already expect node-3 (hash ring updated at T2)


Time T4: New client requests
─────────────────────────────────────────
Client → Connection Node A:
  Subscribe("chat:2")
  ├─ hashRing.GetNode("chat:2") = "centrifugo-data-3"
  ├─ nodeIndex = 3
  ├─ NATS request to: data.3.subscribe
  └─ Data Node 3 handles ✅

Client → Connection Node B (different node, same result):
  Subscribe("chat:2")
  ├─ hashRing.GetNode("chat:2") = "centrifugo-data-3"
  └─ NATS request to: data.3.subscribe ✅

Consistency: All connection nodes route "chat:2" to node-3
```

### Key Points

1. **StatefulSet is source of truth**: Replica count determines node count
2. **Watch API is real-time**: Events arrive within ~500ms
3. **All connection nodes update independently**: No coordination needed
4. **Hash rings are identical**: Same input → same ring → same routing
5. **New nodes ready before clients**: Pod creation slower than ring update

## Summary

### How Data Node Selection Works

1. **Hash the channel name** → Get a 32-bit hash
2. **Binary search the ring** → Find first vnode with hash ≥ channel hash
3. **Map vnode to physical node** → Return the data node ID
4. **Use node index for NATS** → Send request to `data.<index>.subscribe`

### How Scaling is Reflected

1. **K8s scales StatefulSet** → New pod starts
2. **Connection nodes detect change** → StatefulSet watch fires
3. **Hash ring updates** → Add 150 new vnodes for new node
4. **New requests use new mapping** → ~25% of channels now map to new node
5. **Old subscriptions migrate lazily** → On reconnect or timeout

### Key Properties

- ✅ **Minimal movement**: Only 25% of channels move when adding 1 node to 3
- ✅ **Deterministic**: All connection nodes agree on channel→node mapping
- ✅ **Fast lookups**: O(log N) binary search, ~300ns per lookup
- ✅ **Atomic updates**: Hash ring swaps atomically, no intermediate state
- ✅ **Lazy migration**: Old state stays active until naturally migrated

### Why 150 Virtual Nodes?

```
More vnodes = better distribution, but larger ring

vnodes=10:   Standard deviation ~15% (poor)
vnodes=50:   Standard deviation ~7%  (acceptable)
vnodes=150:  Standard deviation ~5%  (excellent) ✅
vnodes=500:  Standard deviation ~3%  (overkill, larger memory)

150 vnodes is the sweet spot:
- Excellent distribution (~5% std dev)
- Small memory overhead (~7KB for 4 nodes)
- Fast binary search (log 600 ≈ 9 comparisons)
```
