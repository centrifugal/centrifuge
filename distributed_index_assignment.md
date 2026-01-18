# Index Assignment Strategies

## Kubernetes StatefulSet: Automatic Index from Pod Name

StatefulSet pods have predictable names with ordinal indices:
- `centrifugo-data-0` → index 0
- `centrifugo-data-1` → index 1
- `centrifugo-data-2` → index 2

Extract the index automatically:

```go
func (d *DataNode) Start(ctx context.Context) error {
    // Get pod name from hostname
    hostname := os.Hostname()  // "centrifugo-data-2"

    // Parse index from hostname
    d.index = parseIndexFromHostname(hostname)

    log.Infof("Starting data node %d", d.index)

    // Subscribe to NATS subjects
    d.nats.Subscribe(fmt.Sprintf("data.%d.*", d.index), ...)

    // Publish heartbeat
    go d.publishHeartbeat(ctx)

    return nil
}

func parseIndexFromHostname(hostname string) int {
    // "centrifugo-data-2" → "2"
    parts := strings.Split(hostname, "-")
    if len(parts) == 0 {
        return 0
    }

    lastPart := parts[len(parts)-1]
    index, err := strconv.Atoi(lastPart)
    if err != nil {
        log.Fatalf("Failed to parse index from hostname %s: %v", hostname, err)
    }

    return index
}
```

### Kubernetes Manifest (No Manual Index!)

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: centrifugo-data
spec:
  serviceName: centrifugo-data
  replicas: 3
  podManagementPolicy: Parallel

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
        args:
        - --role=data

        # No NODE_INDEX needed!
        # Pod name provides the index automatically

        env:
        - name: NATS_URL
          value: nats://nats:4222
```

**That's it!** No environment variables needed. The pod parses its own index from hostname.

---

## Kubernetes: Automatic NODE_INDEX via Downward API (Recommended)

**Kubernetes 1.28+**: Use the pod index field directly - no parsing needed!

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: centrifugo-data
spec:
  template:
    spec:
      containers:
      - name: centrifugo
        image: centrifugo:v5
        env:
        # K8s automatically sets NODE_INDEX from pod ordinal!
        - name: NODE_INDEX
          valueFrom:
            fieldRef:
              fieldPath: metadata.labels['statefulset.kubernetes.io/pod-index']
```

**That's it!** Kubernetes sets `NODE_INDEX` to 0, 1, 2, 3... automatically.

Then in code:

```go
func (d *DataNode) Start(ctx context.Context) error {
    // Just read NODE_INDEX - it's already set by Kubernetes!
    d.index = mustParseInt(os.Getenv("NODE_INDEX"))

    log.Infof("Starting data node %d", d.index)

    // ... rest of startup
}
```

### For Older Kubernetes (< 1.28): Inject Pod Name

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: centrifugo-data
spec:
  template:
    spec:
      containers:
      - name: centrifugo
        image: centrifugo:v5
        env:
        # Inject pod name via Downward API
        - name: POD_NAME
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
```

Then parse in code:

```go
func (d *DataNode) Start(ctx context.Context) error {
    // Option 1: From POD_NAME env var
    if podName := os.Getenv("POD_NAME"); podName != "" {
        d.index = parseIndexFromHostname(podName)  // "centrifugo-data-2" → 2
        return
    }

    // Option 2: From hostname (also works)
    d.index = parseIndexFromHostname(os.Hostname())

    log.Infof("Starting data node %d", d.index)
}
```

---

## Non-Kubernetes: Multiple Strategies

### Strategy 1: Environment Variable (Docker, VMs)

```bash
# Docker
docker run -e NODE_INDEX=0 centrifugo --role=data
docker run -e NODE_INDEX=1 centrifugo --role=data

# Systemd
NODE_INDEX=0 centrifugo --role=data
NODE_INDEX=1 centrifugo --role=data
```

```go
func (d *DataNode) Start(ctx context.Context) error {
    if idx := os.Getenv("NODE_INDEX"); idx != "" {
        d.index = mustParseInt(idx)
        return nil
    }

    // Fallback to other methods...
}
```

### Strategy 2: NATS JetStream KV (Auto-Assignment)

```go
func (d *DataNode) claimIndexFromNATS(ctx context.Context) int {
    js, _ := d.nats.JetStream()
    kv, _ := js.KeyValue("centrifugo_indices")

    hostname := os.Hostname()

    // Try to claim indices 0, 1, 2, ... until success
    for i := 0; i < 100; i++ {
        key := fmt.Sprintf("index.%d", i)

        // Atomic create (fails if exists)
        _, err := kv.Create(key, []byte(hostname))
        if err == nil {
            log.Infof("Claimed index %d", i)

            // Keep alive
            go d.keepIndexAlive(ctx, kv, key)

            return i
        }
    }

    panic("no available index")
}

func (d *DataNode) keepIndexAlive(ctx context.Context, kv nats.KeyValue, key string) {
    ticker := time.NewTicker(30 * time.Second)
    defer ticker.Stop()

    for {
        select {
        case <-ticker.C:
            kv.Put(key, []byte(os.Hostname()))
        case <-ctx.Done():
            kv.Delete(key)  // Release on shutdown
            return
        }
    }
}
```

### Strategy 3: Hostname Pattern

```go
func (d *DataNode) Start(ctx context.Context) error {
    hostname := os.Hostname()

    // Match patterns:
    // - "centrifugo-data-2" → 2 (StatefulSet)
    // - "data-node-1" → 1
    // - "node1" → 1
    d.index = parseIndexFromHostname(hostname)

    // ... rest of startup
}

func parseIndexFromHostname(hostname string) int {
    // Try to extract number from hostname
    re := regexp.MustCompile(`\d+$`)
    match := re.FindString(hostname)

    if match == "" {
        log.Fatalf("Could not extract index from hostname: %s", hostname)
    }

    index, _ := strconv.Atoi(match)
    return index
}
```

---

## Unified Index Assignment Logic

Support all strategies with fallback chain:

```go
func (d *DataNode) determineIndex(ctx context.Context) int {
    // Strategy 1: Explicit NODE_INDEX env var (highest priority)
    if idx := os.Getenv("NODE_INDEX"); idx != "" {
        log.Infof("Using NODE_INDEX from env: %s", idx)
        return mustParseInt(idx)
    }

    // Strategy 2: K8s POD_ORDINAL (K8s 1.28+)
    if idx := os.Getenv("POD_ORDINAL"); idx != "" {
        log.Infof("Using POD_ORDINAL from K8s: %s", idx)
        return mustParseInt(idx)
    }

    // Strategy 3: Parse from hostname (StatefulSet, manual naming)
    hostname := os.Hostname()
    if idx := tryParseIndexFromHostname(hostname); idx >= 0 {
        log.Infof("Parsed index %d from hostname: %s", idx, hostname)
        return idx
    }

    // Strategy 4: NATS JetStream KV auto-claim
    if d.config.AutoClaimIndex {
        log.Info("Auto-claiming index from NATS JetStream KV")
        return d.claimIndexFromNATS(ctx)
    }

    log.Fatal("Could not determine node index. Set NODE_INDEX env var or enable auto-claim.")
    return -1
}

func tryParseIndexFromHostname(hostname string) int {
    re := regexp.MustCompile(`\d+$`)
    match := re.FindString(hostname)

    if match == "" {
        return -1  // No index in hostname
    }

    index, err := strconv.Atoi(match)
    if err != nil {
        return -1
    }

    return index
}
```

---

## Complete Example: Kubernetes StatefulSet

### Manifest

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: centrifugo-data
  namespace: centrifugo
spec:
  serviceName: centrifugo-data
  replicas: 3
  podManagementPolicy: Parallel

  selector:
    matchLabels:
      app: centrifugo-data
      role: data

  template:
    metadata:
      labels:
        app: centrifugo-data
        role: data
    spec:
      containers:
      - name: centrifugo
        image: centrifugo:v5

        args:
        - --role=data
        - --nats=nats://nats:4222

        # No NODE_INDEX needed!
        # Automatically parsed from pod name

        resources:
          requests:
            memory: 8Gi
            cpu: 4
          limits:
            memory: 16Gi
            cpu: 8

        livenessProbe:
          httpGet:
            path: /health
            port: 8000
          periodSeconds: 10

        readinessProbe:
          httpGet:
            path: /ready
            port: 8000
          periodSeconds: 5
```

### Application Code

```go
func (d *DataNode) Start(ctx context.Context) error {
    // Automatically determine index
    d.index = d.determineIndex(ctx)

    log.Infof("Starting data node %d (hostname: %s)", d.index, os.Hostname())

    // Connect to NATS
    nc, err := nats.Connect(d.config.NATSURL)
    if err != nil {
        return err
    }
    d.nats = nc

    // Subscribe to NATS subjects for this index
    d.nats.Subscribe(fmt.Sprintf("data.%d.keyed.*", d.index), d.handleKeyed)
    d.nats.Subscribe(fmt.Sprintf("data.%d.broker.*", d.index), d.handleBroker)
    d.nats.Subscribe(fmt.Sprintf("data.%d.presence.*", d.index), d.handlePresence)

    // Publish heartbeat so connection nodes can discover us
    go d.publishHeartbeat(ctx)

    log.Infof("Data node %d ready", d.index)
    return nil
}
```

### Deployment

```bash
# Apply manifests
kubectl apply -f nats.yaml
kubectl apply -f centrifugo-data-statefulset.yaml
kubectl apply -f centrifugo-connection-deployment.yaml

# Pods created automatically with indices:
# centrifugo-data-0 → NODE_INDEX=0 (automatic)
# centrifugo-data-1 → NODE_INDEX=1 (automatic)
# centrifugo-data-2 → NODE_INDEX=2 (automatic)

# Scale up
kubectl scale statefulset centrifugo-data --replicas=5

# Creates:
# centrifugo-data-3 → NODE_INDEX=3 (automatic)
# centrifugo-data-4 → NODE_INDEX=4 (automatic)

# No manual intervention needed!
```

---

## Summary: Index Assignment Per Environment

| Environment | Method | Configuration Needed |
|-------------|--------|---------------------|
| **Kubernetes StatefulSet** | Parse from hostname | ✅ None (automatic) |
| **Docker Compose** | Environment variable | Set `NODE_INDEX` per service |
| **Docker** | Environment variable | `-e NODE_INDEX=0` |
| **VMs / Bare Metal** | Environment variable | `NODE_INDEX=0 centrifugo` |
| **Any (auto)** | NATS JetStream KV | Enable auto-claim in config |

**Best Practice for K8s**: Use StatefulSet with automatic hostname parsing. Zero configuration needed!

```go
// Simple and works everywhere
d.index = d.determineIndex(ctx)

// Fallback chain:
// 1. NODE_INDEX env var (explicit)
// 2. POD_ORDINAL env var (K8s 1.28+)
// 3. Parse from hostname (StatefulSet)
// 4. NATS JetStream KV auto-claim (if enabled)
```
