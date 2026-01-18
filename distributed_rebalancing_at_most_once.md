# Rebalancing Strategy for At-Most-Once Delivery

## Requirements

- ✅ **At-most-once delivery** - No duplicates
- ✅ **Fast rebalancing** - Complete in ~1 second
- ✅ **Simple implementation** - Minimal complexity
- ✅ **Accept brief gaps** - Messages lost during rebalance are OK

## The Simple Solution: Immediate Re-subscription

### Key Principle

**When hash ring changes, connection nodes immediately re-subscribe to correct data nodes. Accept brief message loss during the switch.**

No forwarding, no dual-routing, no state migration. Just cut over cleanly.

---

## Implementation

### Connection Node: Immediate Re-subscription on Topology Change

```go
type ConnectionNode struct {
    nodeID       string
    hashRing     *ConsistentHash
    subscriptions map[string]*Subscription
    nats         *nats.Conn
}

type Subscription struct {
    Channel      string
    DataNode     int           // Currently subscribed data node
    NATSSub      *nats.Subscription
    Clients      []*Client
}

// Called when K8s watch event detects StatefulSet replica change
func (c *ConnectionNode) OnTopologyChange(newNodes []string) {
    log.Infof("Topology change detected: %v", newNodes)

    newHashRing := NewConsistentHash(newNodes, 150)

    // Find channels that need to move
    var toRebalance []string
    for channel, sub := range c.subscriptions {
        oldNode := sub.DataNode
        newNode := newHashRing.GetNode(channel)

        if oldNode != newNode {
            toRebalance = append(toRebalance, channel)
        }
    }

    // Update hash ring atomically
    c.hashRing = newHashRing

    // Rebalance affected channels immediately
    log.Infof("Rebalancing %d channels", len(toRebalance))
    for _, channel := range toRebalance {
        c.rebalanceChannel(channel)
    }
}

func (c *ConnectionNode) rebalanceChannel(channel string) {
    sub := c.subscriptions[channel]
    oldNode := sub.DataNode
    newNode := c.hashRing.GetNode(channel)

    log.Infof("Rebalancing %s: node %d → node %d", channel, oldNode, newNode)

    // 1. Unsubscribe from old data node
    c.unsubscribeFromDataNode(oldNode, channel)

    // 2. Unsubscribe from old NATS subjects
    if sub.NATSSub != nil {
        sub.NATSSub.Unsubscribe()
    }

    // 3. Subscribe to new data node
    err := c.subscribeToDataNode(newNode, channel)
    if err != nil {
        log.Errorf("Failed to subscribe to new node %d for %s: %v",
            newNode, channel, err)
        // TODO: Retry logic
        return
    }

    // 4. Subscribe to new NATS subjects (for receiving broadcasts)
    shard := hash(channel) % c.numShards
    subjects := []string{
        fmt.Sprintf("conn.%s.%d.keyed.publication", c.nodeID, shard),
        fmt.Sprintf("conn.%s.%d.broker.publication", c.nodeID, shard),
        fmt.Sprintf("conn.%s.%d.broker.join", c.nodeID, shard),
        fmt.Sprintf("conn.%s.%d.broker.leave", c.nodeID, shard),
        fmt.Sprintf("conn.%s.%d.presence.join", c.nodeID, shard),
        fmt.Sprintf("conn.%s.%d.presence.leave", c.nodeID, shard),
    }

    natsSub, err := c.nats.Subscribe(subjects[0], c.handleBroadcast)
    if err != nil {
        log.Errorf("Failed to subscribe to NATS: %v", err)
        return
    }

    // 5. Update subscription record
    sub.DataNode = newNode
    sub.NATSSub = natsSub

    log.Infof("Rebalanced %s successfully", channel)
}

func (c *ConnectionNode) subscribeToDataNode(nodeIndex int, channel string) error {
    req := &SubscribeRequest{
        Channel:        channel,
        ConnectionNode: c.nodeID,
        NumShards:      c.numShards,
    }

    subject := fmt.Sprintf("data.%d.broker.subscribe", nodeIndex)
    reply, err := c.nats.Request(subject, encode(req), 5*time.Second)
    if err != nil {
        return err
    }

    // Parse reply, get current stream position, etc.
    return nil
}

func (c *ConnectionNode) unsubscribeFromDataNode(nodeIndex int, channel string) {
    req := &UnsubscribeRequest{
        Channel:        channel,
        ConnectionNode: c.nodeID,
    }

    subject := fmt.Sprintf("data.%d.broker.unsubscribe", nodeIndex)
    // Fire and forget - don't wait for reply
    c.nats.Publish(subject, encode(req))
}
```

**Timeline:**
```
T0: kubectl scale statefulset centrifugo-data --replicas=4
T0+500ms: Connection node receives K8s watch event
T0+500ms: OnTopologyChange() called
          - Update hash ring
          - Identify 2,500 channels that need to move (25% of 10K)
T0+501ms: Start rebalancing all 2,500 channels in parallel
          - Unsubscribe from old nodes
          - Subscribe to new nodes
T0+600ms: Rebalancing complete for most channels
T0+800ms: All channels rebalanced

Gap: ~300ms where publications to affected channels are lost
```

**Characteristics:**
- ✅ **Fast**: ~300ms total rebalancing time
- ✅ **Simple**: ~100 lines of code
- ✅ **No duplicates**: Clean cutover from old to new node
- ✅ **At-most-once**: Messages during switch are lost (acceptable)

---

### Data Node: Reject Misrouted Requests

```go
type DataNode struct {
    index    int
    hashRing *ConsistentHash
    channels map[string]*ChannelState
}

// Called when K8s watch event detects StatefulSet replica change
func (d *DataNode) OnTopologyChange(newNodes []string) {
    log.Infof("Topology change detected: %v", newNodes)

    newHashRing := NewConsistentHash(newNodes, 150)

    // Mark channels that should migrate away
    for channel := range d.channels {
        oldOwner := d.index
        newOwner := newHashRing.GetNode(channel)

        if oldOwner != newOwner {
            log.Infof("Channel %s should migrate: node %d → node %d",
                channel, oldOwner, newOwner)
            // Don't actively migrate - just mark for cleanup
        }
    }

    // Update hash ring atomically
    d.hashRing = newHashRing
}

// Handle publish request
func (d *DataNode) handlePublish(req *PublishRequest) (*PublishReply, error) {
    channel := req.Channel
    correctOwner := d.hashRing.GetNode(channel)

    // Reject if we're not the correct owner
    if correctOwner != d.index {
        return nil, &WrongNodeError{
            Channel:      channel,
            CurrentNode:  d.index,
            ExpectedNode: correctOwner,
        }
    }

    // We're the correct owner, process normally
    return d.processPublish(req)
}

// Handle subscribe request
func (d *DataNode) handleSubscribe(req *SubscribeRequest) (*SubscribeReply, error) {
    channel := req.Channel
    correctOwner := d.hashRing.GetNode(channel)

    // Accept subscription even if we're not the "correct" owner
    // This handles the case where connection node has stale hash ring
    // Better to accept and let connection node re-subscribe later
    // than reject and create gaps

    if correctOwner != d.index {
        log.Warnf("Accepting subscription for %s (belongs to node %d, we are %d)",
            channel, correctOwner, d.index)
    }

    return d.processSubscribe(req)
}
```

**Key Decision: Accept Subscriptions, Reject Publishes**

- **Subscribe requests**: Accept even if wrong node
  - Handles race condition where connection node has stale hash ring
  - Connection node will re-subscribe soon anyway
  - Avoids gaps in subscription

- **Publish requests**: Reject if wrong node
  - Ensures at-most-once delivery
  - Forces publisher to retry with correct node
  - Clean ownership semantics

---

### Periodic Cleanup (Background)

```go
// Data nodes clean up channels that don't belong to them
func (d *DataNode) startPeriodicCleanup() {
    ticker := time.NewTicker(60 * time.Second)

    go func() {
        for range ticker.C {
            d.cleanupMisalignedChannels()
        }
    }()
}

func (d *DataNode) cleanupMisalignedChannels() {
    for channel, state := range d.channels {
        correctOwner := d.hashRing.GetNode(channel)

        if correctOwner != d.index {
            // This channel doesn't belong to us anymore

            if len(state.Subscribers) == 0 {
                // No subscribers, safe to delete
                log.Infof("Cleaning up misaligned channel %s (no subscribers)",
                    channel)
                delete(d.channels, channel)
            } else {
                // Still have subscribers - they haven't migrated yet
                // Keep the channel but log warning
                log.Warnf("Channel %s has %d subscribers but belongs to node %d",
                    channel, len(state.Subscribers), correctOwner)
            }
        }
    }
}
```

---

## Handling Edge Cases

### Case 1: Connection Node Crashes During Rebalancing

**Scenario:**
- Connection node starts rebalancing 2,500 channels
- Crashes after rebalancing 1,000 channels
- Remaining 1,500 channels still subscribed to old node

**Outcome:**
- Those 1,500 channels have no subscribers (connection node is dead)
- Data nodes clean them up after 60 seconds (no subscribers)
- When connection node restarts, subscribes to correct nodes from scratch

**Impact:** None - connection node death is already handled by reconnect logic.

---

### Case 2: Data Node Crashes During Rebalancing

**Scenario:**
- Data node hosting 3,333 channels crashes
- Connection nodes are rebalancing some of those channels to other nodes

**Outcome:**
- Channels on crashed node are unavailable regardless
- Connection nodes' rebalance operations to other nodes succeed
- Channels still on crashed node become unavailable (expected)

**Impact:** None - data node crash is already catastrophic (in-memory, all data lost).

---

### Case 3: NATS Partition During Rebalancing

**Scenario:**
- Connection node can't reach NATS during rebalancing
- Cannot send subscribe/unsubscribe requests

**Outcome:**
- Rebalancing fails for affected channels
- Connection node logs errors
- Retry logic attempts rebalancing again
- Eventually succeeds when NATS connectivity restored

**Mitigation:**
```go
func (c *ConnectionNode) rebalanceChannel(channel string) {
    maxRetries := 5
    backoff := 1 * time.Second

    for i := 0; i < maxRetries; i++ {
        err := c.attemptRebalance(channel)
        if err == nil {
            return
        }

        log.Warnf("Rebalance attempt %d failed for %s: %v", i+1, channel, err)
        time.Sleep(backoff)
        backoff *= 2
    }

    log.Errorf("Failed to rebalance %s after %d attempts", channel, maxRetries)
}
```

---

## Performance Analysis

### Rebalancing 10,000 Channels (Scale 3 → 4 nodes)

**Affected channels**: ~2,500 (25% move to new node)

**Per-channel rebalancing:**
- Unsubscribe from old node: ~5ms (NATS request-reply)
- Subscribe to new node: ~5ms (NATS request-reply)
- Total: ~10ms per channel

**Sequential rebalancing**: 2,500 × 10ms = **25 seconds** ❌ Too slow

**Parallel rebalancing** (100 concurrent workers):
- 2,500 channels / 100 workers = 25 channels per worker
- 25 × 10ms = 250ms per worker
- Total: **~300ms** ✅ Fast enough

```go
func (c *ConnectionNode) OnTopologyChange(newNodes []string) {
    // ... find channels to rebalance ...

    // Rebalance in parallel with worker pool
    numWorkers := 100
    channelsChan := make(chan string, len(toRebalance))
    var wg sync.WaitGroup

    for i := 0; i < numWorkers; i++ {
        wg.Add(1)
        go func() {
            defer wg.Done()
            for channel := range channelsChan {
                c.rebalanceChannel(channel)
            }
        }()
    }

    for _, channel := range toRebalance {
        channelsChan <- channel
    }
    close(channelsChan)

    wg.Wait()
    log.Infof("Rebalancing complete")
}
```

---

## Message Loss Analysis

**Gap duration**: ~300ms (from hash ring update to rebalancing complete)

**Messages lost**: Depends on publish rate

| Publish Rate | Messages Lost During 300ms Gap |
|--------------|-------------------------------|
| 100 msg/s    | 30 messages                   |
| 1,000 msg/s  | 300 messages                  |
| 10,000 msg/s | 3,000 messages                |

**For real-time messaging (chat, notifications):**
- 300ms gap is barely noticeable
- ~30-300 messages lost across thousands of channels
- Clients see brief "message sent but not delivered"
- Acceptable for ephemeral data

**Recovery:**
- Clients don't know messages were lost (at-most-once)
- No duplicate messages
- No manual intervention needed
- System automatically converges to correct state

---

## Comparison to Other Approaches

| Approach | Speed | Duplicates | Losses | Complexity |
|----------|-------|------------|--------|------------|
| **Immediate Re-subscription** | **300ms** | **None** | **Yes (300ms gap)** | **Low** |
| Forwarding + Hints | 1s | Brief period | None | Medium |
| Active State Migration | 30s | During dual-route | None | High |
| Lazy Migration | Hours/days | No | Split-brain | Low |

**For at-most-once delivery**: Immediate re-subscription is the clear winner.

---

## Production Checklist

**Required:**
1. ✅ K8s API watch for StatefulSet replica changes
2. ✅ Hash ring atomic update
3. ✅ Parallel rebalancing (100 workers)
4. ✅ Retry logic for failed rebalances
5. ✅ Data node periodic cleanup (60s)

**Monitoring:**
1. ✅ Rebalancing duration metric
2. ✅ Channels rebalanced counter
3. ✅ Failed rebalance errors
4. ✅ Misaligned channels gauge (data nodes)

**Testing:**
1. ✅ Scale up (3 → 4 nodes)
2. ✅ Scale down (4 → 3 nodes)
3. ✅ Multiple rapid scaling events
4. ✅ NATS partition during rebalance
5. ✅ Connection node crash during rebalance

---

## Summary

**For at-most-once delivery with no duplicates:**

✅ **Use Immediate Re-subscription**
- Connection nodes re-subscribe immediately on topology change
- Data nodes reject publishes to wrong nodes
- Accept ~300ms gap during rebalancing
- Parallel workers ensure fast completion
- Periodic cleanup removes stale state

**Code Size:** ~200 lines total
**Rebalancing Time:** ~300ms for 10K channels
**Message Loss:** Brief (300ms gap)
**Duplicates:** None
**Complexity:** Low

**Perfect for:** Real-time messaging where brief gaps are acceptable and duplicates must be avoided.
