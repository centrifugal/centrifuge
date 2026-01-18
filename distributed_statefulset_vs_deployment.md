# StatefulSet vs Deployment for Data Nodes

## The Question

With immediate re-subscription rebalancing (no state migration), do we still need StatefulSet? Or can we use a simpler Deployment?

## What We Use from StatefulSet

Looking at the current design:

1. **Stable indices**: Pods are numbered 0, 1, 2, 3...
2. **Predictable scaling**: Scale up adds highest index, scale down removes highest index
3. **Sequential indices**: No gaps (always 0,1,2,3... not 0,2,5,7)
4. **Pod name pattern**: `centrifugo-data-{index}` is predictable

## Current Architecture Dependencies

### Data Nodes Use Their Index

```go
func (d *DataNode) Start() {
    hostname := os.Hostname()  // "centrifugo-data-2"
    d.index = parseIndexFromHostname(hostname)  // 2

    // Listen on NATS subjects with this index
    d.nats.Subscribe(fmt.Sprintf("data.%d.keyed.*", d.index), ...)
    d.nats.Subscribe(fmt.Sprintf("data.%d.broker.*", d.index), ...)
    d.nats.Subscribe(fmt.Sprintf("data.%d.presence.*", d.index), ...)
}
```

### Connection Nodes Build Hash Ring from Replica Count

```go
func (c *ConnectionNode) buildHashRing(replicas int) *ConsistentHash {
    // Assumes indices are 0, 1, 2, ... replicas-1
    nodes := make([]string, replicas)
    for i := 0; i < replicas; i++ {
        nodes[i] = fmt.Sprintf("data-%d", i)
    }
    return NewConsistentHash(nodes, 150)
}

// Route to data node by index
nodeIndex := c.hashRing.GetNode(channel)  // Returns 0, 1, 2, or 3
subject := fmt.Sprintf("data.%d.broker.publish", nodeIndex)
c.nats.Request(subject, req, 5*time.Second)
```

**Key insight**: We rely on sequential indices (0,1,2,3...) without gaps.

---

## Option 1: Keep StatefulSet ✅ RECOMMENDED

### Pros

1. **Simple index assignment**
   - Pod ordinal IS the index
   - No coordination needed
   - No external dependencies
   ```go
   // In pod centrifugo-data-2
   hostname := os.Hostname()  // "centrifugo-data-2"
   index := 2  // Parse from hostname
   ```

2. **Predictable scaling**
   ```
   Scale 3 → 4:
   - Creates: centrifugo-data-3 (index 3)
   - Hash ring: [0, 1, 2] → [0, 1, 2, 3]

   Scale 4 → 3:
   - Deletes: centrifugo-data-3 (highest index)
   - Hash ring: [0, 1, 2, 3] → [0, 1, 2]
   ```

3. **No index gaps**
   - Always sequential: 0, 1, 2, 3, 4...
   - Connection nodes can build hash ring with simple loop

4. **Connection nodes only need replica count**
   ```go
   // Watch StatefulSet for replica count changes
   replicas := statefulSet.Spec.Replicas  // 4

   // Build hash ring - simple!
   for i := 0; i < replicas; i++ {
       nodes[i] = fmt.Sprintf("data-%d", i)
   }
   ```

5. **Fast rollouts with `podManagementPolicy: Parallel`**
   ```yaml
   apiVersion: apps/v1
   kind: StatefulSet
   metadata:
     name: centrifugo-data
   spec:
     podManagementPolicy: Parallel  # ← Updates all pods in parallel
     replicas: 3
   ```
   - Removes StatefulSet's traditional "ordered, slow" rollout
   - Pods update in parallel like Deployment
   - Still keeps stable indices

### Cons

1. ❌ **Name implies persistent storage** (but we don't use it)
2. ❌ **Default ordered rollout** (but we can use Parallel policy)

### Recommended StatefulSet Config

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: centrifugo-data
  namespace: centrifugo
spec:
  serviceName: centrifugo-data
  replicas: 3

  # ⚡ Parallel updates - no ordered rollout!
  podManagementPolicy: Parallel

  # 🚀 Fast rolling updates
  updateStrategy:
    type: RollingUpdate
    rollingUpdate:
      partition: 0  # Update all pods

  selector:
    matchLabels:
      app: centrifugo-data

  template:
    metadata:
      labels:
        app: centrifugo-data
    spec:
      containers:
      - name: centrifugo
        image: centrifugo:v5
        env:
        - name: POD_NAME
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        # Pod gets its index from POD_NAME
```

---

## Option 2: Use Deployment (More Complex)

If we wanted to use Deployment, we'd need to solve the index assignment problem.

### Challenge: Index Assignment

Deployment pods have random names:
```
centrifugo-data-7d8f9b-x7k2l
centrifugo-data-7d8f9b-p3m9w
centrifugo-data-7d8f9b-q8n4t
```

No inherent index. Need to assign somehow.

### Sub-option 2a: External Index Registry (Etcd/Consul)

```go
func (d *DataNode) Start(ctx context.Context) error {
    // Need to acquire an index from external registry
    d.index = d.acquireIndexFromEtcd(ctx)

    // Register in etcd: "node-{index}" → "pod-name"
    d.registerInEtcd(ctx, d.index, os.Hostname())

    // Listen on NATS subjects
    d.nats.Subscribe(fmt.Sprintf("data.%d.*", d.index), ...)

    // On shutdown: release index
    defer d.releaseIndexFromEtcd(ctx, d.index)
}

func (d *DataNode) acquireIndexFromEtcd(ctx context.Context) int {
    // Race with other pods to get lowest available index
    for i := 0; i < 100; i++ {
        key := fmt.Sprintf("/centrifugo/indices/%d", i)

        // Try to acquire lock
        lease, err := d.etcd.Grant(ctx, 60) // 60 second TTL
        txn := d.etcd.Txn(ctx).
            If(clientv3.Compare(clientv3.CreateRevision(key), "=", 0)).
            Then(clientv3.OpPut(key, os.Hostname(), clientv3.WithLease(lease.ID))).
            Else()

        resp, err := txn.Commit()
        if err == nil && resp.Succeeded {
            // Got index i!
            go d.keepAliveIndex(ctx, lease.ID)
            return i
        }
    }

    panic("could not acquire index")
}
```

**Pros:**
- ✅ Works with Deployment
- ✅ Dynamic index assignment

**Cons:**
- ❌ Requires etcd or Consul (external dependency)
- ❌ Complex startup logic with race conditions
- ❌ Need keepalive/heartbeat for index lease
- ❌ What if etcd is down? Pods can't start
- ❌ Need index cleanup on pod crash
- ❌ Connection nodes need to query etcd for active indices

### Sub-option 2b: Leader Election + Index Assignment

```go
// One pod becomes leader and assigns indices to others
func (d *DataNode) Start(ctx context.Context) {
    // Try to become leader
    election := d.etcd.NewElection("/centrifugo/leader")

    if d.becomeLeader(election) {
        // I'm the leader - assign indices to all pods
        go d.runIndexAssignmentService(ctx)
    }

    // All pods (including leader) request index from leader
    d.index = d.requestIndexFromLeader(ctx)

    // Listen on NATS
    d.nats.Subscribe(fmt.Sprintf("data.%d.*", d.index), ...)
}
```

**Cons:**
- ❌ Even more complex
- ❌ Still needs etcd for leader election
- ❌ Leader is SPOF for index assignment
- ❌ What if leader crashes during assignment?

### Sub-option 2c: Service Discovery (No Fixed Indices)

Completely different approach: Don't use fixed indices at all.

```go
// Data node uses its pod IP as identity
func (d *DataNode) Start() {
    d.identity = os.Getenv("POD_IP")  // "10.244.1.5"

    // Register with service discovery
    d.registerWithNATS(d.identity)

    // Listen on NATS with identity
    d.nats.Subscribe(fmt.Sprintf("data.%s.*", d.identity), ...)
}

// Connection nodes discover data nodes dynamically
func (c *ConnectionNode) discoverDataNodes() []string {
    // Query NATS for all registered data nodes
    nodes := c.nats.DiscoverServices("centrifugo-data")

    // Build hash ring from discovered nodes
    return NewConsistentHash(nodes, 150)
}
```

**Pros:**
- ✅ No index coordination needed
- ✅ Dynamic discovery
- ✅ Works with Deployment

**Cons:**
- ❌ Needs service discovery protocol in NATS
- ❌ Connection nodes must constantly poll for node changes
- ❌ Hash ring based on IPs/random IDs (less predictable)
- ❌ Scaling changes hash ring unpredictably
  ```
  Scale 3 → 4:
  Old ring: [10.244.1.5, 10.244.1.6, 10.244.1.7]
  New ring: [10.244.1.5, 10.244.1.6, 10.244.1.7, 10.244.1.8]

  Scale 4 → 3 (Deployment kills random pod):
  Could become: [10.244.1.5, 10.244.1.7, 10.244.1.8]  ← Missing .6
  Or: [10.244.1.5, 10.244.1.6, 10.244.1.7]  ← Missing .8
  Unpredictable which pod dies!
  ```

---

## Scaling Behavior Comparison

### StatefulSet

**Scale Up (3 → 4):**
```
Before: centrifugo-data-0, centrifugo-data-1, centrifugo-data-2
After:  centrifugo-data-0, centrifugo-data-1, centrifugo-data-2, centrifugo-data-3
        ✅ Adds index 3 (highest)
```

**Scale Down (4 → 3):**
```
Before: centrifugo-data-0, centrifugo-data-1, centrifugo-data-2, centrifugo-data-3
After:  centrifugo-data-0, centrifugo-data-1, centrifugo-data-2
        ✅ Removes index 3 (highest)
        ✅ Indices remain sequential
```

### Deployment

**Scale Up (3 → 4):**
```
Before: centrifugo-data-7d8f9b-x7k2l, centrifugo-data-7d8f9b-p3m9w, centrifugo-data-7d8f9b-q8n4t
After:  centrifugo-data-7d8f9b-x7k2l, centrifugo-data-7d8f9b-p3m9w, centrifugo-data-7d8f9b-q8n4t, centrifugo-data-7d8f9b-z9r5s
        ❌ Which index does new pod get?
        Need external coordination to assign index 3
```

**Scale Down (4 → 3):**
```
Before: Pods with indices [0, 1, 2, 3]
After:  Deployment kills random pod (could be any!)
        ❌ Could end up with indices [0, 1, 3] (gap at 2)
        ❌ Or [0, 2, 3] (gap at 1)
        ❌ Hash ring logic breaks with gaps
```

---

## Decision Matrix

| Aspect | StatefulSet + Parallel | Deployment + External Registry | Deployment + Service Discovery |
|--------|----------------------|-------------------------------|-------------------------------|
| **Index assignment** | ✅ Automatic (pod ordinal) | ❌ Manual (etcd coordination) | ❌ Dynamic (IP-based) |
| **Sequential indices** | ✅ Always 0,1,2,3... | ⚠️ Can have gaps on scale down | ❌ Random IDs |
| **Predictable scaling** | ✅ Always add/remove highest | ❌ Random pod deletion | ❌ Random pod deletion |
| **External dependencies** | ✅ None | ❌ Etcd/Consul required | ⚠️ NATS service discovery |
| **Setup complexity** | ✅ Simple YAML | ❌ Complex startup logic | ⚠️ Medium (discovery protocol) |
| **Connection node logic** | ✅ Simple (just replica count) | ❌ Query etcd for active indices | ❌ Poll NATS for active nodes |
| **Rollout speed** | ✅ Parallel (with config) | ✅ Parallel | ✅ Parallel |
| **Failure handling** | ✅ Pod recreates with same index | ⚠️ Index must be reclaimed | ⚠️ Node leaves/rejoins ring |

---

## Recommendation: Keep StatefulSet with `podManagementPolicy: Parallel`

**Reasons:**

1. **Simplicity** - Pod ordinal = node index, no external coordination needed
2. **Predictable scaling** - Always add highest index, remove highest index
3. **No gaps** - Indices always sequential (0,1,2,3...)
4. **No external dependencies** - Self-contained, no etcd/Consul needed
5. **Simple connection node logic** - Just watch replica count, build hash ring with loop
6. **Fast rollouts** - Use `podManagementPolicy: Parallel` to update all pods at once

**The key insight:**

Even though we don't use persistent storage (the traditional StatefulSet use case), we DO benefit from **stable network identity** - specifically, the predictable sequential indices. This is exactly what StatefulSet provides.

Using Deployment would require reinventing this index coordination ourselves with external systems, adding complexity and failure modes.

---

## Final Config

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: centrifugo-data
spec:
  serviceName: centrifugo-data
  replicas: 3

  # Key: Parallel pod management for fast updates
  podManagementPolicy: Parallel

  updateStrategy:
    type: RollingUpdate

  selector:
    matchLabels:
      app: centrifugo-data
      role: data-node

  template:
    metadata:
      labels:
        app: centrifugo-data
        role: data-node
    spec:
      # Anti-affinity: spread across nodes
      affinity:
        podAntiAffinity:
          preferredDuringSchedulingIgnoredDuringExecution:
          - weight: 100
            podAffinityTerm:
              labelSelector:
                matchLabels:
                  app: centrifugo-data
              topologyKey: kubernetes.io/hostname

      containers:
      - name: centrifugo
        image: centrifugo:v5

        env:
        # Pod gets its index from hostname
        - name: POD_NAME
          valueFrom:
            fieldRef:
              fieldPath: metadata.name

        # Memory-optimized
        resources:
          requests:
            memory: 8Gi
            cpu: 4
          limits:
            memory: 16Gi
            cpu: 8

        # Startup probe
        startupProbe:
          httpGet:
            path: /health
            port: 8000
          initialDelaySeconds: 5
          periodSeconds: 5
          failureThreshold: 30

        # Liveness probe
        livenessProbe:
          httpGet:
            path: /health
            port: 8000
          periodSeconds: 10

        # Readiness probe
        readinessProbe:
          httpGet:
            path: /ready
            port: 8000
          periodSeconds: 5
```

**Summary**: StatefulSet with `podManagementPolicy: Parallel` gives us the best of both worlds - stable indices (simple) + fast parallel rollouts (Deployment-like).
