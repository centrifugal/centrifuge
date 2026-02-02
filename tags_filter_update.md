# Dynamic Tags Filter Update

## Overview

This document describes how to implement dynamic tags filter updates for active subscriptions. Currently, tags filters are set once during subscription and cannot be changed without resubscribing.

## Use Case

**Top-N Subscriptions with Score Filtering**:

1. Client subscribes to leaderboard
2. Gets snapshot, determines 5th place score = 600
3. Sets filter: `{score: {$gte: 600}}`
4. When rankings change, updates filter to new boundary (e.g., 650)

Without dynamic updates, client must unsubscribe/resubscribe, risking gaps.

---

## Current Architecture

### Filter Storage (hub.go)

```go
type tagsFilter struct {
    filter *protocol.FilterNode  // The actual filter tree
    hash   [32]byte              // SHA256 hash for caching/comparison
}

type subInfo struct {
    client     *Client
    deltaType  DeltaType
    useID      bool
    tagsFilter *tagsFilter  // Stores filter per subscription
}
```

Stored in hub's subscription registry:
```go
subs map[string]map[string]subInfo  // [channel][clientID] -> subInfo
```

### Filter Applied During Broadcast (hub.go lines 817-827)

```go
for _, sub := range channelSubscribers {
    wasFiltered := false
    if sub.tagsFilter != nil {
        match, _ := filter.Match(sub.tagsFilter.filter, pub.Tags)
        wasFiltered = !match
        if wasFiltered {
            tagsFilterDropped++
        }
    }
}
```

### Initial Subscribe Flow (client.go lines 3136-3149)

```go
if req.Tf != nil {
    if !reply.Options.AllowTagsFilter {
        // Return error if tags filtering not allowed
    }
    if err := filter.Validate(req.Tf); err != nil {
        // Validate filter structure
    }
    sub.tagsFilter = &tagsFilter{
        filter: req.Tf,
        hash:   filter.Hash(req.Tf),
    }
}
```

### Existing SubRefresh Mechanism (client.go lines 1914-1990)

SubRefresh exists but only supports:
- `ExpireAt` - refresh subscription expiration
- `Info` - update channel info

**No filter update support.**

---

## Implementation Plan

### 1. Protocol Level

Add `Tf` field to `SubRefreshRequest`:

```protobuf
message SubRefreshRequest {
    string channel = 1;
    string token = 2;
    FilterNode tf = 3;  // NEW: optional filter update
}
```

### 2. Handler Level (client.go)

Extend `subRefreshCmd` handler:

```go
func (c *Client) subRefreshCmd(req *protocol.SubRefreshRequest, ...) {
    // ... existing validation ...

    // NEW: Handle filter update
    if req.Tf != nil {
        if !sub.Options.AllowTagsFilter {
            return centrifuge.ErrorPermissionDenied
        }
        if err := filter.Validate(req.Tf); err != nil {
            return err
        }

        newFilter := &tagsFilter{
            filter: req.Tf,
            hash:   filter.Hash(req.Tf),
        }

        // Update in hub
        c.node.hub.updateSubFilter(channel, c.ID(), newFilter)
    }

    // ... existing logic ...
}
```

### 3. Hub Level (hub.go)

Add method to update filter:

```go
func (h *Hub) updateSubFilter(channel, clientID string, tf *tagsFilter) error {
    shard := h.subShard(channel)
    shard.mu.Lock()
    defer shard.mu.Unlock()

    channelSubs, ok := shard.subs[channel]
    if !ok {
        return ErrorNotSubscribed
    }

    sub, ok := channelSubs[clientID]
    if !ok {
        return ErrorNotSubscribed
    }

    sub.tagsFilter = tf
    channelSubs[clientID] = sub

    return nil
}
```

### 4. Keyed Subscriptions (client_keyed.go)

For keyed subscriptions, also update `keyedSubscribeState.tagsFilter` if subscription is still in snapshot/stream phase:

```go
func (c *Client) updateKeyedSubFilter(channel string, tf *tagsFilter) {
    c.mu.Lock()
    defer c.mu.Unlock()

    if state, ok := c.keyedSubscribeStates[channel]; ok {
        state.tagsFilter = tf
    }
}
```

---

## Filtering Locations

Filter updates must be considered at all filtering points:

| Location | File | Lines | Phase |
|----------|------|-------|-------|
| Publication broadcast | hub.go | 817-827 | Live |
| History recovery | client.go | 3060-3067 | Subscribe |
| Keyed snapshot | client_keyed.go | 399-409 | Snapshot |
| Keyed stream | client_keyed.go | 503-513 | Stream |
| Keyed live | client_keyed.go | 693-703 | Live |

All use `filter.Match()` with `sub.tagsFilter` - no changes needed to filtering logic itself.

---

## Challenges & Considerations

### Atomicity

Hub's `subShard.mu` RWMutex provides atomic updates. Filter swap is safe.

### In-Flight Publications

1-2 publications may be delivered with old filter during update. Acceptable trade-off.

Options if stricter consistency needed:
- Acknowledge filter update with current stream position
- Client ignores publications before that position with old filter

### Keyed Subscriptions

Must update filter in both places:
- `keyedSubscribeState.tagsFilter` (if still subscribing)
- `subInfo.tagsFilter` in hub (if live)

### Client SDK

Client SDK needs:
```javascript
subscription.updateFilter({
    score: { $gte: newBoundary }
});
```

This sends SubRefresh with new `Tf` field.

---

## Filter Operations Reference

Supported operations in `internal/filter/filter.go`:

| Type | Operations |
|------|------------|
| String | `eq`, `neq`, `in`, `nin`, `sw` (starts with), `ew` (ends with), `ct` (contains) |
| Existence | `ex` (exists), `nex` (not exists) |
| Numeric | `gt`, `gte`, `lt`, `lte` |
| Logical | `and`, `or`, `not` |

Example filter for top-N:
```json
{
    "op": "gte",
    "field": "score",
    "value": "600"
}
```

---

## Testing Considerations

1. **Basic update**: Change filter, verify new publications filtered correctly
2. **Concurrent updates**: Multiple rapid filter changes
3. **Keyed subscription phases**: Update filter during snapshot/stream/live
4. **Invalid filter**: Ensure validation rejects malformed filters
5. **Permission check**: Ensure `AllowTagsFilter` still enforced on update

---

## Related Features

- **TopN field**: Add explicit `TopN` to `KeyedReadSnapshotOptions` for clarity
- **Snapshot hook**: Client SDK hook to set initial filter based on snapshot values
- **Score field**: Separate Score field enables efficient server-side filtering

---

## Files to Modify

| File | Changes |
|------|---------|
| `protocol/*.proto` | Add `Tf` to SubRefreshRequest |
| `client.go` | Handle filter in subRefreshCmd |
| `hub.go` | Add updateSubFilter method |
| `client_keyed.go` | Update keyedSubscribeState filter |
| Client SDKs | Add updateFilter() method |
