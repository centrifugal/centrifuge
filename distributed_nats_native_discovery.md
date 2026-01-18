# NATS-Native Service Discovery

## Key Insight

Data nodes already announce themselves to NATS by subscribing to `data.{index}.*` subjects. Connection nodes can discover active data nodes directly through NATS - no external service discovery needed!

## NATS Service API Approach

NATS has a built-in **Service API** (micro services feature) for service discovery.

### Data Node: Register as NATS Service

```go
import "github.com/nats-io/nats.go/micro"

func (d *DataNode) Start(ctx context.Context) error {
    // Claim index (we still need some mechanism for this)
    d.index = d.claimIndex(ctx)

    // Register as NATS microservice
    config := micro.Config{
        Name:        "centrifugo-data",
        Version:     "1.0.0",
        Description: "Centrifuge data node",
        Metadata: map[string]string{
            "index":    fmt.Sprintf("%d", d.index),
            "hostname": os.Hostname(),
        },
    }

    svc, err := micro.AddService(d.nats, config)
    if err != nil {
        return err
    }

    // Add service endpoints
    group := svc.AddGroup(fmt.Sprintf("data.%d", d.index))

    group.AddEndpoint("keyed.subscribe",
        micro.HandlerFunc(d.handleKeyedSubscribe))
    group.AddEndpoint("keyed.publish",
        micro.HandlerFunc(d.handleKeyedPublish))
    group.AddEndpoint("broker.publish",
        micro.HandlerFunc(d.handleBrokerPublish))
    // ... other endpoints

    log.Infof("Data node %d registered with NATS", d.index)
    return nil
}
```

### Connection Node: Discover Data Nodes via NATS

```go
func (c *ConnectionNode) Start(ctx context.Context) error {
    // Discover data nodes via NATS service API
    go c.watchDataNodes(ctx)
    return nil
}

func (c *ConnectionNode) watchDataNodes(ctx context.Context) {
    ticker := time.NewTicker(5 * time.Second)
    defer ticker.Stop()

    for {
        select {
        case <-ticker.C:
            c.discoverDataNodes()
        case <-ctx.Done():
            return
        }
    }
}

func (c *ConnectionNode) discoverDataNodes() {
    // Query NATS for all instances of "centrifugo-data" service
    req := &micro.InfoRequest{Name: "centrifugo-data"}
    reqData, _ := json.Marshal(req)

    // Send info request
    msg, err := c.nats.Request("$SRV.INFO", reqData, 2*time.Second)
    if err != nil {
        log.Errorf("Failed to discover services: %v", err)
        return
    }

    var info micro.Info
    json.Unmarshal(msg.Data, &info)

    // Extract indices from service metadata
    var indices []int
    for _, endpoint := range info.Endpoints {
        if idx, ok := endpoint.Metadata["index"]; ok {
            i, _ := strconv.Atoi(idx)
            indices = append(indices, i)
        }
    }

    // Update hash ring if topology changed
    maxIndex := 0
    for _, idx := range indices {
        if idx > maxIndex {
            maxIndex = idx
        }
    }

    newCount := maxIndex + 1
    if newCount != c.currentNodeCount {
        log.Infof("Data node topology changed: %d → %d nodes",
            c.currentNodeCount, newCount)
        c.updateHashRing(newCount)
        c.currentNodeCount = newCount
    }
}
```

**Pros:**
- ✅ Uses NATS built-in service discovery
- ✅ No external dependencies
- ✅ Auto-discovery through NATS
- ✅ Works everywhere NATS works

**Cons:**
- ⚠️ Polling-based (5s delay)
- ⚠️ Still need index claiming mechanism

---

## Simpler Approach: Heartbeat Subject

Even simpler - just use NATS pub/sub for heartbeats:

### Data Node: Publish Heartbeat

```go
func (d *DataNode) Start(ctx context.Context) error {
    // Claim index somehow
    d.index = d.claimIndex(ctx)

    // Subscribe to request subjects
    d.nats.Subscribe(fmt.Sprintf("data.%d.keyed.*", d.index), ...)
    d.nats.Subscribe(fmt.Sprintf("data.%d.broker.*", d.index), ...)
    d.nats.Subscribe(fmt.Sprintf("data.%d.presence.*", d.index), ...)

    // Publish heartbeat every 5 seconds
    go d.publishHeartbeat(ctx)

    return nil
}

func (d *DataNode) publishHeartbeat(ctx context.Context) {
    ticker := time.NewTicker(5 * time.Second)
    defer ticker.Stop()

    for {
        select {
        case <-ticker.C:
            hb := &Heartbeat{
                NodeIndex: d.index,
                Hostname:  os.Hostname(),
                StartTime: d.startTime,
                Timestamp: time.Now().Unix(),
            }
            d.nats.Publish("centrifugo.data.heartbeat", encode(hb))

        case <-ctx.Done():
            return
        }
    }
}
```

### Connection Node: Subscribe to Heartbeats

```go
func (c *ConnectionNode) Start(ctx context.Context) error {
    // Subscribe to data node heartbeats
    c.nats.Subscribe("centrifugo.data.heartbeat", c.handleHeartbeat)

    // Periodically check for dead nodes
    go c.detectDeadNodes(ctx)

    return nil
}

type dataNodeState struct {
    index     int
    hostname  string
    lastSeen  time.Time
}

func (c *ConnectionNode) handleHeartbeat(msg *nats.Msg) {
    var hb Heartbeat
    decode(msg.Data, &hb)

    c.mu.Lock()
    defer c.mu.Unlock()

    // Update node state
    c.dataNodes[hb.NodeIndex] = &dataNodeState{
        index:    hb.NodeIndex,
        hostname: hb.Hostname,
        lastSeen: time.Now(),
    }

    // Rebuild hash ring if needed
    c.rebuildHashRingIfNeeded()
}

func (c *ConnectionNode) detectDeadNodes(ctx context.Context) {
    ticker := time.NewTicker(10 * time.Second)
    defer ticker.Stop()

    for {
        select {
        case <-ticker.C:
            c.mu.Lock()

            changed := false
            for idx, state := range c.dataNodes {
                // Remove nodes not seen in 30 seconds
                if time.Since(state.lastSeen) > 30*time.Second {
                    log.Warnf("Data node %d is dead (last seen %v ago)",
                        idx, time.Since(state.lastSeen))
                    delete(c.dataNodes, idx)
                    changed = true
                }
            }

            c.mu.Unlock()

            if changed {
                c.rebuildHashRingIfNeeded()
            }

        case <-ctx.Done():
            return
        }
    }
}

func (c *ConnectionNode) rebuildHashRingIfNeeded() {
    // Find highest active index
    maxIndex := -1
    for idx := range c.dataNodes {
        if idx > maxIndex {
            maxIndex = idx
        }
    }

    newCount := maxIndex + 1

    if newCount != c.currentNodeCount {
        log.Infof("Data node count changed: %d → %d", c.currentNodeCount, newCount)
        c.updateHashRing(newCount)
        c.currentNodeCount = newCount
    }
}
```

**Timeline:**
```
T0: Data node 3 starts
T0: Data node 3 subscribes to data.3.* subjects
T0: Data node 3 starts publishing heartbeats
T5: Connection nodes receive first heartbeat from node 3
T5: Connection nodes update hash ring: 3 nodes → 4 nodes
T5: Connection nodes start rebalancing affected channels

Total discovery time: ~5 seconds (one heartbeat interval)
```

**Characteristics:**
- ✅ **No external dependencies**: Just NATS
- ✅ **Auto-discovery**: Connection nodes find data nodes automatically
- ✅ **Failure detection**: Dead nodes detected within 30s
- ✅ **Simple**: ~100 lines of code
- ✅ **Works everywhere**: Any NATS deployment

---

## But What About Index Assignment?

We still need a way for data nodes to claim indices. Options:

### Option 1: NATS JetStream KV (Recommended)

```go
func (d *DataNode) claimIndex(ctx context.Context) int {
    js, err := d.nats.JetStream()
    if err != nil {
        return 0, err
    }

    // Create/get KV bucket for indices
    kv, err := js.CreateKeyValue(&nats.KeyValueConfig{
        Bucket: "centrifugo_data_indices",
        TTL:    60 * time.Second,  // Index expires if not kept alive
    })
    if err != nil {
        // Bucket might already exist
        kv, err = js.KeyValue("centrifugo_data_indices")
        if err != nil {
            return 0, err
        }
    }

    hostname := os.Hostname()

    // Try to claim indices 0, 1, 2, ... until success
    for i := 0; i < 100; i++ {
        key := fmt.Sprintf("index.%d", i)

        // Try to create (fails if exists)
        _, err := kv.Create(key, []byte(hostname))
        if err == nil {
            log.Infof("Claimed index %d", i)

            // Keep index alive
            go d.keepIndexAlive(ctx, kv, key)

            return i
        }

        // If error is not "already exists", fail
        if !errors.Is(err, nats.ErrKeyExists) {
            return 0, err
        }

        // Index already claimed, try next one
    }

    return 0, errors.New("no available index")
}

func (d *DataNode) keepIndexAlive(ctx context.Context, kv nats.KeyValue, key string) {
    ticker := time.NewTicker(30 * time.Second)
    defer ticker.Stop()

    hostname := os.Hostname()

    for {
        select {
        case <-ticker.C:
            // Update TTL by writing same value
            kv.Put(key, []byte(hostname))

        case <-ctx.Done():
            // Release index on shutdown
            kv.Delete(key)
            return
        }
    }
}
```

**Pros:**
- ✅ Uses NATS JetStream (often already deployed with NATS)
- ✅ Atomic create operation (no race conditions)
- ✅ TTL support (dead nodes release indices automatically)
- ✅ No external dependencies beyond NATS

**Cons:**
- ⚠️ Requires NATS JetStream enabled

### Option 2: Static Assignment (Simplest)

```go
func (d *DataNode) claimIndex(ctx context.Context) int {
    // Read from env var or config
    if idx := os.Getenv("NODE_INDEX"); idx != "" {
        return mustParseInt(idx)
    }

    // Or parse from hostname (StatefulSet style)
    hostname := os.Hostname()
    return parseIndexFromHostname(hostname)
}
```

**Pros:**
- ✅ Extremely simple
- ✅ No coordination needed
- ✅ Works with StatefulSet or manual assignment

**Cons:**
- ❌ Manual assignment required

### Option 3: First-Come-First-Served via Heartbeat

```go
func (d *DataNode) claimIndex(ctx context.Context) int {
    // Request index from other data nodes
    inbox := nats.NewInbox()
    sub, _ := d.nats.SubscribeSync(inbox)
    defer sub.Unsubscribe()

    // Ask: "What indices are taken?"
    d.nats.PublishRequest("centrifugo.data.index_query", inbox, nil)

    // Collect responses for 1 second
    time.Sleep(1 * time.Second)

    takenIndices := make(map[int]bool)
    for {
        msg, err := sub.NextMsg(100 * time.Millisecond)
        if err != nil {
            break
        }

        var resp IndexQueryResponse
        decode(msg.Data, &resp)
        takenIndices[resp.Index] = true
    }

    // Claim lowest available index
    for i := 0; i < 100; i++ {
        if !takenIndices[i] {
            log.Infof("Claimed index %d", i)
            return i
        }
    }

    panic("no available index")
}

// Other data nodes respond to index queries
func (d *DataNode) handleIndexQuery(msg *nats.Msg) {
    resp := &IndexQueryResponse{Index: d.index}
    d.nats.Publish(msg.Reply, encode(resp))
}
```

**Pros:**
- ✅ No external storage
- ✅ Pure NATS pub/sub

**Cons:**
- ⚠️ 1 second delay on startup
- ⚠️ Race condition if two nodes start simultaneously

---

## Recommended Architecture: NATS-Only Discovery

**For data nodes:**
1. Claim index via NATS JetStream KV (or static env var)
2. Subscribe to `data.{index}.*` subjects
3. Publish heartbeat to `centrifugo.data.heartbeat` every 5s

**For connection nodes:**
1. Subscribe to `centrifugo.data.heartbeat`
2. Track active data nodes (remove if no heartbeat for 30s)
3. Rebuild hash ring when topology changes
4. No external config needed!

**Complete example:**

```go
// Data node startup
func (d *DataNode) Start(ctx context.Context) error {
    // Option 1: NATS JetStream KV (auto)
    d.index = d.claimIndexFromNATSKV(ctx)

    // Option 2: Static (manual)
    // d.index = mustParseInt(os.Getenv("NODE_INDEX"))

    // Subscribe to subjects
    d.nats.Subscribe(fmt.Sprintf("data.%d.keyed.*", d.index), ...)
    d.nats.Subscribe(fmt.Sprintf("data.%d.broker.*", d.index), ...)
    d.nats.Subscribe(fmt.Sprintf("data.%d.presence.*", d.index), ...)

    // Publish heartbeat
    go d.publishHeartbeat(ctx)

    log.Infof("Data node %d started", d.index)
    return nil
}

// Connection node startup
func (c *ConnectionNode) Start(ctx context.Context) error {
    // Subscribe to heartbeats - discover data nodes automatically
    c.nats.Subscribe("centrifugo.data.heartbeat", c.handleHeartbeat)

    // Detect dead nodes
    go c.detectDeadNodes(ctx)

    log.Info("Connection node started, discovering data nodes...")
    return nil
}
```

**No configuration needed beyond NATS connection string!**

---

## Comparison: Old vs New

### Old Approach (Environment-Specific)

```yaml
# Kubernetes
kubectl get statefulset centrifugo-data -o json | jq '.spec.replicas'

# Static
DATA_NODE_COUNT=3  # Must configure

# Consul
consul catalog service centrifugo-data  # External dependency
```

### New Approach (NATS-Native)

```go
// Just connect to NATS and discover automatically
nats.Connect("nats://localhost:4222")

// Connection nodes auto-discover data nodes via heartbeats
// No config needed!
```

**Benefits:**
- ✅ Works in **any environment** (K8s, VMs, Docker, bare metal)
- ✅ **No external dependencies** (just NATS)
- ✅ **No configuration** (besides NATS URL)
- ✅ **Auto-discovery** through heartbeats
- ✅ **Automatic failover** detection (30s heartbeat timeout)

---

## Summary

**You're absolutely right!** We don't need `DATA_NODE_COUNT` or K8s API watch or Consul.

**NATS-native discovery:**
1. Data nodes publish heartbeats to `centrifugo.data.heartbeat`
2. Connection nodes subscribe and track active data nodes
3. Hash ring rebuilds automatically when nodes join/leave
4. Index claiming via NATS JetStream KV (or static env var)

**Result:** Truly deployment-agnostic architecture with zero external dependencies beyond NATS.

```bash
# Start data nodes (any environment)
NODE_INDEX=0 centrifugo --role=data --nats=nats://nats:4222
NODE_INDEX=1 centrifugo --role=data --nats=nats://nats:4222
NODE_INDEX=2 centrifugo --role=data --nats=nats://nats:4222

# Start connection nodes (auto-discover data nodes)
centrifugo --role=connection --nats=nats://nats:4222
centrifugo --role=connection --nats=nats://nats:4222

# No DATA_NODE_COUNT needed!
```
