# Keyed Two-Phase Subscribe Implementation Plan

## Overview

Implement server-side support for keyed two-phase subscribe protocol while maintaining full backwards compatibility with existing subscribe/recovery mechanisms.

## Protocol Changes

### SubscribeRequest (fields 15+)

```protobuf
message SubscribeRequest {
  // ... existing fields 1-14 unchanged ...

  bool keyed = 15;              // Indicates keyed subscription mode
  int32 keyed_phase = 16;       // 0=SNAPSHOT, 1=STREAM
  string keyed_cursor = 17;     // Pagination cursor for snapshot phase
  int32 keyed_limit = 18;       // Max entries per page (0 = server default)
  uint64 keyed_offset = 19;     // Stream position offset for validation/streaming
  string keyed_epoch = 20;      // Stream position epoch for validation/streaming
  bool keyed_ordered = 21;      // Request ordered snapshot (by score)
  bool keyed_presence = 22;     // Indicates this is a presence sub-subscription
}
```

### SubscribeResult (fields 15+)

```protobuf
message SubscribeResult {
  // ... existing fields 1-14 unchanged ...

  bool keyed = 15;              // Server confirms keyed mode
  int32 keyed_phase = 16;       // 0=SNAPSHOT, 1=STREAMING (result phase)
  string keyed_cursor = 17;     // Next page cursor (empty = last page)
  bool keyed_presence_available = 18;  // Server indicates presence is available for this channel
  // Note: publications (7), offset (9), epoch (6) reuse existing fields
}
```

### Phase Constants

```go
const (
    KeyedPhaseSnapshot  int32 = 0  // Paginating over snapshot (keyed state)
    KeyedPhaseStream    int32 = 1  // Paginating over stream (history catch-up)
    KeyedPhaseStreaming int32 = 2  // Join pub/sub, switch to real-time streaming
)
```

**Request phases:**
- `SNAPSHOT` = "give me next snapshot page"
- `STREAM` = "give me next stream page (history catch-up)"
- `STREAMING` = "I'm ready to join pub/sub" (triggers coordination point)

**Response phases:**
- `SNAPSHOT` = "here's snapshot data, may be more"
- `STREAM` = "here's stream data, may be more"
- `STREAMING` = "joined pub/sub, now streaming real-time"

## Server-Side Implementation

### Handler Structure

```
client.go
├── handleSubscribe()                   // Entry point - detect keyed mode early
│   ├── handleKeyedSubscribe()          // NEW: Router for keyed subscriptions
│   │   ├── handleKeyedSnapshotPhase()  // Snapshot pagination (stateless)
│   │   ├── handleKeyedStreamPhase()    // Stream pagination (stateless)
│   │   ├── handleKeyedStreamingPhase() // Join pub/sub (COORDINATION)
│   │   └── handleKeyedPresenceSubscribe() // Presence sub-subscription
│   └── [existing callback path]        // UNCHANGED: Regular subscriptions
```

### Validation via OnSubscribe Callback

Keyed subscription validation uses the existing `OnSubscribe` callback mechanism:

```go
// SubscribeEvent - add keyed flag so handler knows client requested keyed mode
type SubscribeEvent struct {
    Channel     string
    Token       string
    // ... existing fields ...
    Keyed       bool   // NEW: Client requested keyed subscription
    Presence    bool   // NEW: Client requested presence sub-subscription
}

// SubscribeOptions - add keyed options
type SubscribeOptions struct {
    // ... existing fields ...
    EnableKeyed           bool          // NEW: Allow keyed subscription for this channel
    KeyedPresenceAvailable bool         // NEW: Server indicates presence is available
}

// Application handler validates keyed mode
node.OnSubscribing(func(ctx context.Context, client *Client, e SubscribeEvent) (SubscribeReply, error) {
    // If client requested keyed mode, validate it's allowed
    if e.Keyed {
        if !channelAllowsKeyed(e.Channel) {
            return SubscribeReply{}, ErrorNotAvailable  // Reject keyed subscription
        }
        return SubscribeReply{
            Options: SubscribeOptions{
                EnableKeyed:            true,
                KeyedPresenceAvailable: true,  // Optional: indicate presence is available
            },
        }, nil
    }
    // Regular subscription
    return SubscribeReply{}, nil
})
```

**Flow:**
1. Client sends `keyed=true` in SubscribeRequest (initial request)
2. Server creates SubscribeEvent with `Keyed: true`
3. OnSubscribe callback validates if keyed is allowed for channel
4. If rejected: return error (client gets ErrorNotAvailable)
5. If allowed: set `EnableKeyed: true` in SubscribeOptions
6. Server proceeds with keyed subscription, tracks authorization

**Important: OnSubscribe is called ONLY ONCE (initial request)**
- Initial request (first SNAPSHOT or STREAMING) → OnSubscribe callback runs
- Server tracks authorized subscription (e.g., in `c.keyedSubscribing` map)
- Subsequent requests (pagination) → No callback, just verify authorization exists
- On STREAMING phase → Move from `keyedSubscribing` to `c.channels` (fully subscribed)

### Subscription State Tracking

```go
type Client struct {
    // ... existing fields ...

    // Tracks keyed subscriptions that are still loading (not yet streaming)
    // Key: channel name, Value: authorization info from OnSubscribe
    keyedSubscribing map[string]*keyedSubscribeState

    // Existing: fully subscribed channels (including keyed after STREAMING phase)
    channels map[string]ChannelContext
}

type keyedSubscribeState struct {
    options   SubscribeOptions  // From OnSubscribe callback
    epoch     string            // Epoch from first response (for validation)
    // ... other state needed during pagination
}
```

**State transitions:**
1. Initial request → OnSubscribe callback → add to `keyedSubscribing`
2. Pagination requests → verify in `keyedSubscribing`, no callback
3. STREAMING request → coordination → move to `channels`, remove from `keyedSubscribing`

### Key Implementation Details

1. **Routing in handleSubscribe()** - After OnSubscribe callback returns, check if `reply.Options.EnableKeyed` is true and route to keyed handlers

2. **handleKeyedSnapshotPhase()** - Stateless pagination:
   - Build `KeyedReadSnapshotOptions` from request
   - Call `keyedEngine.ReadSnapshot()` with cursor/limit
   - Return publications, streamPos, cursor in result
   - No pub/sub subscription during this phase
   - Server validates epoch if `keyed_offset`/`keyed_epoch` provided

3. **handleKeyedStreamPhase()** - Stream pagination (no coordination):
   - Read up to `limit` publications from stream since client offset
   - Return publications with `phase=STREAM`
   - No buffering, no pub/sub subscription - stateless like snapshot
   - Client continues with updated offset until ready to join

4. **handleKeyedStreamingPhase()** - Join pub/sub (coordination point):
   - `StartBuffering()` before any reads
   - `Subscribe()` to pub/sub via KeyedEngine
   - `ReadStream()` for remaining missed publications
   - `LockBufferAndReadBuffered()` + merge
   - Add to `c.channels` with keyed flag
   - Send response with `phase=STREAMING`, then `StopBuffering()`

## Three Subscribe Modes

### Mode 1: Latest Only (no recovery)
```
Client → Server: keyed=true, phase=STREAMING, offset=0, epoch=""
Server → Client: keyed=true, phase=STREAMING, offset=<current>, epoch=<current>
```
- Client immediately requests STREAMING phase (no pagination)
- Server runs coordination, joins pub/sub, returns current position

### Mode 2: Recovery from Position (stream catch-up with pagination)

When client has missed many publications (e.g., 5000), use offset-based pagination:

```
Client → Server: keyed=true, phase=STREAM, offset=5000, limit=500, epoch="abc"
Server → Client: publications=[5001-5500], offset=5500, phase=STREAM (more to fetch)

Client → Server: keyed=true, phase=STREAM, offset=5500, limit=500, epoch="abc"
Server → Client: publications=[5501-6000], offset=6000, phase=STREAM

... continue until client decides they're caught up enough ...

Client → Server: keyed=true, phase=STREAMING, offset=9500, epoch="abc"  ← REQUEST TO JOIN
Server → Client: publications=[9501-9800], offset=9800, phase=STREAMING (joined pub/sub)
```

**Key behavior:**
- `phase=STREAM` (request) = "give me stream data, not joining yet"
- `phase=STREAMING` (request) = "join pub/sub now" - **this triggers coordination point**
- Server returns `phase=STREAM` while paginating (not yet joined)
- Server returns `phase=STREAMING` only when joined pub/sub
- Client controls when to trigger coordination point by sending `phase=STREAMING`
- No cursor needed - offset-based continuation works naturally for ordered streams

### Mode 3: Full Two-Phase (snapshot + stream + streaming)
```
Phase 1 - Snapshot pagination:
  Client → Server: keyed=true, phase=SNAPSHOT, limit=100, cursor=""
  Server → Client: publications=[100], cursor="page2", offset=5000, epoch="abc", phase=SNAPSHOT
  ... continue until cursor=""

Phase 2 (optional) - Stream pagination (if client has old position to catch up):
  Client → Server: keyed=true, phase=STREAM, offset=5000, limit=500, epoch="abc"
  Server → Client: publications=[500], offset=5500, phase=STREAM
  ... continue until caught up enough ...

Phase 3 - Join streaming:
  Client → Server: keyed=true, phase=STREAMING, offset=5500, epoch="abc"
  Server → Client: publications=[remaining], phase=STREAMING (joined pub/sub)
```

## Presence Sub-Subscription

Presence uses the **same two-phase protocol** but as a sub-subscription of the main channel.

### Presence Flow

```
Client → Server: keyed=true, presence=true, phase=SNAPSHOT, limit=100, cursor=""
Server:
  1. Verify main subscription exists for base channel
  2. Use {channel}:presence as actual keyed channel
  3. Return presence snapshot page

... pagination continues ...

Client → Server: keyed=true, presence=true, phase=STREAM, offset=X, epoch="Y"
Server:
  1. Join pub/sub for {channel}:presence
  2. Start auto-maintaining this client's presence (backend-managed)
  3. Return phase=STREAMING
```

### Server-Side Presence Maintenance (Backend-Managed)

When client enters STREAM phase for presence:
```go
func (c *Client) startPresenceMaintenance(presenceChannel string) {
    // 1. Publish initial presence via KeyedEngine
    presenceInfo := &ClientInfo{
        ClientID: c.ID(),
        UserID:   c.UserID(),
        // ...
    }
    c.node.keyedEngine.Publish(ctx, presenceChannel, c.ID(), data,
        KeyedPublishOptions{KeyTTL: 60 * time.Second, Publish: true})

    // 2. Start keepalive goroutine (refreshes presence every ~25s)
    go c.presenceKeepalive(presenceChannel)
}

func (c *Client) onDisconnect() {
    // Remove presence for all active presence subscriptions
    for ch := range c.presenceSubs {
        c.node.keyedEngine.Unpublish(ctx, ch+":presence", c.ID(),
            KeyedUnpublishOptions{Publish: true})
    }
}
```

### Handler Implementation

```go
func (c *Client) handleKeyedPresenceSubscribe(req) (*SubscribeResult, error) {
    // 1. Verify main subscription exists
    if _, exists := c.channels[req.Channel]; !exists {
        return nil, ErrorNotSubscribed  // Must subscribe to main first
    }

    // 2. Presence is always keyed
    presenceChannel := req.Channel + ":presence"

    // 3. Use same snapshot/stream handlers with presenceChannel
    if req.KeyedPhase == KeyedPhaseSnapshot {
        return c.handleKeyedSnapshotPhase(presenceChannel, req)
    }

    // 4. On STREAM phase, also start auto-maintenance
    result, err := c.handleKeyedStreamPhase(presenceChannel, req)
    if err == nil {
        c.startPresenceMaintenance(presenceChannel)
        c.presenceSubs[req.Channel] = true
    }
    return result, err
}
```

## Coordination Point

The coordination point (buffering + subscribe + merge) is **fast and contained within a single request** - triggered by **client sending `phase=STREAMING`**.

### How Server Determines Coordination Point

**Server does NOT guess** - client explicitly requests joining pub/sub:

```
phase=SNAPSHOT  → Server: just read snapshot, return snapshot data
phase=STREAM    → Server: just read stream, return stream data
phase=STREAMING → Server: RUN COORDINATION (buffering + subscribe + merge)
```

**The trigger is simple:** When server receives `phase=STREAMING`, it runs the coordination logic.

### During Pagination (stateless, no coordination)

- **Snapshot phase requests** (`phase=SNAPSHOT`):
  - Just call `ReadSnapshot()` - stateless
  - No buffering, no pub/sub subscription
  - Server maintains no state between requests
  - Returns `phase=SNAPSHOT` with data

- **Stream phase requests** (`phase=STREAM`):
  - Just call `ReadStream()` with limit - stateless
  - No buffering, no pub/sub subscription
  - Client uses returned offset for next request
  - Returns `phase=STREAM` with data

### Coordination Request (fast, single request)

When client sends `phase=STREAMING`:

```
Single Request Timeline:
├── 1. StartBuffering(channel)          // Begin buffering incoming pubs
├── 2. Subscribe(channel)               // Join pub/sub
├── 3. ReadStream(since: clientOffset)  // Get remaining missed pubs
├── 4. LockBufferAndReadBuffered()      // Get buffered pubs
├── 5. MergePublications()              // Combine recovered + buffered
├── 6. WriteResponse(phase=STREAMING)   // Send to client
└── 7. StopBuffering(channel)           // Release buffer
```

This is the same fast coordination pattern as regular subscription recovery - the only difference is we paginated historical data first.

## Error Handling

| Error | When | Client Action |
|-------|------|---------------|
| `ErrorUnrecoverablePosition` | Epoch changed during pagination or stream read | Restart from snapshot beginning |
| `ErrorBadRequest` | Invalid phase or parameters | Fix request |
| `ErrorNotAvailable` | No KeyedEngine configured | Fall back or error |

## Files to Modify

1. **`/Users/fz/centrifugal/protocol/client.proto`**
   - Add fields 15-22 to SubscribeRequest (keyed, phase, cursor, limit, offset, epoch, ordered, presence)
   - Add fields 15-18 to SubscribeResult (keyed, phase, cursor, presence_available)

2. **`/Users/fz/centrifugal/centrifuge/events.go`**
   - Add `Keyed` and `Presence` fields to SubscribeEvent

3. **`/Users/fz/centrifugal/centrifuge/options.go`**
   - Add `EnableKeyed` and `KeyedPresenceAvailable` fields to SubscribeOptions

4. **`/Users/fz/centrifugal/centrifuge/client.go`**
   - Add `handleKeyedSubscribe()` function - router for keyed subscriptions
   - Add `handleKeyedSnapshotPhase()` function - stateless snapshot pagination
   - Add `handleKeyedStreamPhase()` function - stream recovery + join pub/sub
   - Add `handleKeyedPresenceSubscribe()` function - presence sub-subscription
   - Add `startPresenceMaintenance()` / `stopPresenceMaintenance()` functions
   - Modify `handleSubscribe()` to route keyed requests after OnSubscribe validates
   - Add keyed flag and presenceSubs tracking to Client struct
   - Handle presence cleanup on disconnect

5. **`/Users/fz/centrifugal/centrifuge/node.go`** (if needed)
   - Ensure KeyedEngine is accessible from Client

## Backwards Compatibility

- All new protocol fields use numbers 15+ (existing 1-14 unchanged)
- Regular subscriptions (`keyed=false`) follow existing path entirely
- No changes to SubscribeHandler callback interface
- Existing SubscribeEvent/SubscribeReply structures unchanged

## Verification

1. Run existing subscribe tests to ensure no regression
2. Add new tests for keyed subscribe:
   - Keyed snapshot pagination (multiple pages)
   - Epoch change during pagination (ErrorUnrecoverablePosition)
   - Stream recovery with pagination (offset-based continuation)
   - Stream phase final join (buffering/merge)
   - Latest only mode (no recovery)
   - Full two-phase flow (snapshot → stream)
3. Add new tests for presence sub-subscription:
   - Presence requires main subscription first (ErrorNotSubscribed)
   - Presence snapshot pagination
   - Presence auto-maintenance on STREAM phase join
   - Presence cleanup on disconnect
   - Presence cleanup on main unsubscribe
4. Test protocol compatibility with old clients (should ignore new fields)
