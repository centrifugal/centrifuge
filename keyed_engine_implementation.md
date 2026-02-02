# Keyed Two-Phase Subscribe Implementation Plan

## Overview

Implement server-side support for keyed two-phase subscribe protocol while maintaining full backwards compatibility with existing subscribe/recovery mechanisms.

## Protocol Changes

### SubscribeRequest (fields 15+)

```protobuf
message SubscribeRequest {
  // ... existing fields 1-14 unchanged ...

  bool keyed = 15;              // Indicates keyed subscription mode
  int32 keyed_phase = 16;       // Request phase: 0=SNAPSHOT, 1=STREAM, 2=LIVE
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
  int32 keyed_phase = 16;       // The result phase of the operation (SNAPSHOT, STREAM, or LIVE)
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
    KeyedPhaseLive      int32 = 2  // Join pub/sub, switch to real-time streaming
)
```

**Request phases:**
- `SNAPSHOT` = "give me next snapshot page"
- `STREAM` = "give me next stream page (history catch-up)"
- `LIVE` = "I'm ready to join pub/sub" (triggers coordination point)

**Response phases:**
- `SNAPSHOT` = "here's snapshot data, may be more"
- `STREAM` = "here's stream data, may be more"
- `LIVE` = "joined pub/sub, now streaming real-time"

## Server-Side Implementation

### Handler Structure

```
client.go
├── handleSubscribe()                   // Entry point - detect keyed mode early
│   ├── handleKeyedSubscribe()          // NEW: Router for keyed subscriptions
│   │   ├── handleKeyedSnapshotPhase()  // Snapshot pagination (stateless)
│   │   ├── handleKeyedStreamPhase()    // Stream pagination (stateless)
│   │   ├── handleKeyedLivePhase()      // Join pub/sub (COORDINATION)
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
    EnableKeyed            bool // NEW: Allow keyed subscription for this channel
    KeyedPresenceAvailable bool // NEW: Server indicates presence is available for sub-subscription
    MaintainPresence       bool // NEW: Maintain this client's presence in the channel
}

// Application handler validates keyed mode and enables presence
node.OnSubscribing(func(ctx context.Context, client *Client, e SubscribeEvent) (SubscribeReply, error) {
    // If client requested keyed mode, validate it's allowed
    if e.Keyed {
        if !channelAllowsKeyed(e.Channel) {
            return SubscribeReply{}, ErrorNotAvailable  // Reject keyed subscription
        }
        return SubscribeReply{
            Options: SubscribeOptions{
                EnableKeyed:            true,
                // Optional: indicate presence data is available for a sub-subscription
                KeyedPresenceAvailable: true,
                // Optional: maintain this client's presence in the channel
                MaintainPresence:       true,
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
- Initial request (first SNAPSHOT or LIVE) → OnSubscribe callback runs
- Server tracks authorized subscription (e.g., in `c.keyedSubscribing` map)
- Subsequent requests (pagination) → No callback, just verify authorization exists
- On LIVE phase → Move from `keyedSubscribing` to `c.channels` (fully subscribed)

### Subscription State Tracking

```go
type Client struct {
    // ... existing fields ...

    // Tracks keyed subscriptions that are still loading (not yet live)
    // Key: channel name, Value: authorization info from OnSubscribe
    keyedSubscribing map[string]*keyedSubscribeState

    // Existing: fully subscribed channels (including keyed after LIVE phase)
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
3. LIVE request → coordination → move to `channels`, remove from `keyedSubscribing`

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

4. **handleKeyedLivePhase()** - Join pub/sub (coordination point):
   - `StartBuffering()` before any reads
   - `Subscribe()` to pub/sub via KeyedEngine
   - `ReadStream()` for remaining missed publications
   - `LockBufferAndReadBuffered()` + merge
   - Add to `c.channels` with keyed flag
   - Send response with `phase=LIVE`, then `StopBuffering()`

## Controlling Pagination Behavior

Once a keyed subscription is authorized, the client can make many subsequent pagination requests. To protect the server from abuse and provide application-level control, we will introduce a combination of automatic concurrency control, a configurable request limit, and an optional callback hook.

### 1. Automatic Concurrency Control

To prevent a single client from overwhelming the server by sending multiple pagination requests for the same channel simultaneously, the server will enforce serial processing.

*   **Mechanism:** The server will internally maintain a lock for each `(client, channel)` pair for the duration of a `SNAPSHOT` or `STREAM` phase request.
*   **Behavior:** If a client sends a pagination request for a channel while another pagination request for that same channel is already in progress, the new request will be immediately rejected with an `ErrorConcurrentPagination`. This forces any given client to wait for a response before sending the next pagination request for a channel, effectively ensuring serial operation.

### 2. Configuration-Based Request Limit

To protect against requests for an excessive number of items in a single page, a new server-wide configuration option will be added.

```go
// In node.Config
type Config struct {
    // ...
    KeyedMaxPaginationLimit int // Max items a client can request per page.
}
```

The server will automatically cap the `keyed_limit` from any client's pagination request to this value. This prevents oversized queries that could strain the backend `KeyedEngine`.

### 3. Optional `OnPagination` Hook for Custom Logic

For maximum flexibility, we will introduce a new optional callback, `OnPagination`. If this hook is registered, it will be called for every `SNAPSHOT` and `STREAM` request before the concurrency lock is taken. This allows for dynamic, fine-grained validation.

```go
// Event sent to the hook
type PaginationEvent struct {
    Client  *Client
    Channel string
    Phase   int32 // SNAPSHOT or STREAM
    Limit   int32 // The limit requested by the client
    // ... plus other relevant request fields like Cursor, Offset
}

// Reply from the hook
type PaginationReply struct {
    // Override the limit for this specific request.
    // The server will still cap this against KeyedMaxPaginationLimit.
    OverrideLimit int32
}

// Example usage
node.OnPagination(func(ctx context.Context, e PaginationEvent) (PaginationReply, error) {
    // Example: Implement stricter limits for free-tier users
    if e.Client.IsFreeTier() && e.Limit > 100 {
        return PaginationReply{OverrideLimit: 100}, nil
    }

    // Example: Reject requests to a specific channel pattern
    if strings.HasPrefix(e.Channel, "system:") {
        return PaginationReply{}, errors.New("permission denied")
    }

    return PaginationReply{}, nil
})
```

## Three Subscribe Modes

### Mode 1: Latest Only (no recovery)
```
Client → Server: keyed=true, phase=LIVE, offset=0, epoch=""
Server → Client: keyed=true, phase=LIVE, offset=<current>, epoch=<current>
```
- Client immediately requests LIVE phase (no pagination)
- Server runs coordination, joins pub/sub, returns current position

### Mode 2: Recovery from Position (stream catch-up with pagination)

When client has missed many publications (e.g., 5000), use offset-based pagination:

```
Client → Server: keyed=true, phase=STREAM, offset=5000, limit=500, epoch="abc"
Server → Client: publications=[5001-5500], offset=5500, phase=STREAM (more to fetch)

Client → Server: keyed=true, phase=STREAM, offset=5500, limit=500, epoch="abc"
Server → Client: publications=[5501-6000], offset=6000, phase=STREAM

... continue until client decides they're caught up enough ...

Client → Server: keyed=true, phase=LIVE, offset=9500, epoch="abc"  ← REQUEST TO JOIN
Server → Client: publications=[9501-9800], offset=9800, phase=LIVE (joined pub/sub)
```

**Key behavior:**
- `phase=STREAM` (request) = "give me stream data, not joining yet"
- `phase=LIVE` (request) = "join pub/sub now" - **this triggers coordination point**
- Server returns `phase=STREAM` while paginating (not yet joined)
- Server returns `phase=LIVE` only when joined pub/sub
- Client controls when to trigger coordination point by sending `phase=LIVE`
- No cursor needed - offset-based continuation works naturally for ordered streams

### Mode 3: Full Two-Phase (snapshot + stream + live)
```
Phase 1 - Snapshot pagination:
  Client → Server: keyed=true, phase=SNAPSHOT, limit=100, cursor=""
  Server → Client: publications=[100], cursor="page2", offset=5000, epoch="abc", phase=SNAPSHOT
  ... continue until cursor=""

Phase 2 (optional) - Stream pagination (if client has old position to catch up):
  Client → Server: keyed=true, phase=STREAM, offset=5000, limit=500, epoch="abc"
  Server → Client: publications=[500], offset=5500, phase=STREAM
  ... continue until caught up enough ...

Phase 3 - Join live:
  Client → Server: keyed=true, phase=LIVE, offset=5500, epoch="abc"
  Server → Client: publications=[remaining], phase=LIVE (joined pub/sub)
```

## Presence

Presence information (i.e., who is currently subscribed to a channel) is exposed as a separate, special-purpose keyed channel. This allows clients to subscribe to presence information using the same two-phase protocol.

### Presence Maintenance (Server-Side)

A client's own presence is maintained by the server as part of the **main channel subscription**, not the presence sub-subscription. This is controlled by the application via the `OnSubscribe` callback. When the main subscription becomes active, the server automatically publishes and refreshes the client's presence information to a derived presence channel (e.g., `{channel}:presence`).

**Flow:**
1. Client subscribes to the main channel (e.g., `chat:123`).
2. The `OnSubscribe` handler returns a `SubscribeReply` with `MaintainPresence: true`.
3. When the client's subscription becomes fully active (i.e., after the coordination point), the server calls `startPresenceMaintenance()`.
4. On disconnect, `onDisconnect()` removes the client's presence from all channels where it was being maintained.

```go
func (c *Client) startPresenceMaintenance(channel string) {
    presenceChannel := channel + ":presence"
    // 1. Publish initial presence via KeyedEngine
    presenceInfo := &ClientInfo{
        ClientID: c.ID(),
        UserID:   c.UserID(),
        // ...
    }
    c.node.keyedEngine.Publish(ctx, presenceChannel, c.ID(), data,
        KeyedPublishOptions{KeyTTL: 60 * time.Second})

    // 2. Start keepalive goroutine (refreshes presence every ~25s)
    go c.presenceKeepalive(presenceChannel)
}

func (c *Client) onDisconnect() {
    // Remove presence for all channels where it was maintained
    for ch := range c.presenceMaintainedChannels {
        c.node.keyedEngine.Unpublish(ctx, ch+":presence", c.ID(),
            KeyedUnpublishOptions{})
    }
}
```

### Presence Sub-Subscription (Read-Only View)

To *read* the presence information, a client uses a sub-subscription. This is a read-only operation and does not affect the client's own presence status. It follows the standard two-phase subscribe protocol on the derived presence channel.

**Flow:**
1. Client must have an active subscription to the main channel.
2. Client sends a `SubscribeRequest` with `keyed=true`, `presence=true`, and the desired `phase` (`SNAPSHOT` or `LIVE`).

```
// Example: paginate through presence snapshot, then go live
Client → Server: keyed=true, presence=true, phase=SNAPSHOT, channel="chat:123"
Server → Client: publications=[presence data], cursor="page2", phase=SNAPSHOT

Client → Server: keyed=true, presence=true, phase=LIVE, channel="chat:123"
Server → Client: publications=[remaining], phase=LIVE (joined presence stream)
```

The server handler becomes a simple wrapper that validates the main subscription and routes to the standard phase handlers.

```go
func (c *Client) handleKeyedPresenceSubscribe(req) (*SubscribeResult, error) {
    // 1. Verify main subscription exists on the base channel
    if _, exists := c.channels[req.Channel]; !exists {
        return nil, ErrorNotSubscribed
    }

    // 2. Presence is always a keyed subscription on a derived channel name
    presenceChannel := req.Channel + ":presence"

    // 3. Use same snapshot/live handlers
    if req.KeyedPhase == KeyedPhaseSnapshot {
        return c.handleKeyedSnapshotPhase(presenceChannel, req)
    }
    if req.KeyedPhase == KeyedPhaseLive {
        // Here we just join the pub/sub, no special maintenance logic
        return c.handleKeyedLivePhase(presenceChannel, req)
    }
    return nil, ErrorBadRequest
}
```

This design cleanly separates the act of *being present* (part of the main subscription) from the act of *observing presence* (a read-only sub-subscription).

## Coordination Point

The coordination point (buffering + subscribe + merge) is **fast and contained within a single request** - triggered by **client sending `phase=LIVE`**.

### How Server Determines Coordination Point

**Server does NOT guess** - client explicitly requests joining pub/sub:

```
phase=SNAPSHOT  → Server: just read snapshot, return snapshot data
phase=STREAM    → Server: just read stream, return stream data
phase=LIVE      → Server: RUN COORDINATION (buffering + subscribe + merge)
```

**The trigger is simple:** When server receives `phase=LIVE`, it runs the coordination logic.

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

When client sends `phase=LIVE`:

```
Single Request Timeline:
├── 1. StartBuffering(channel)          // Begin buffering incoming pubs
├── 2. Subscribe(channel)               // Join pub/sub
├── 3. ReadStream(since: clientOffset)  // Get remaining missed pubs
├── 4. LockBufferAndReadBuffered()      // Get buffered pubs
├── 5. MergePublications()              // Combine recovered + buffered
├── 6. WriteResponse(phase=LIVE)        // Send to client
└── 7. StopBuffering(channel)           // Release buffer
```

This is the same fast coordination pattern as regular subscription recovery - the only difference is we paginated historical data first.

## Error Handling

| Error | When | Client Action |
|-------|------|---------------|
| `ErrorUnrecoverablePosition` | Epoch changed during pagination or stream read | Restart from snapshot beginning |
| `ErrorConcurrentPagination`| Client sends a pagination request while another is in progress for the same channel | Wait for the outstanding request to finish, then retry |
| `ErrorBadRequest` | Invalid phase or parameters | Fix request |
| `ErrorNotAvailable` | No KeyedEngine configured | Fall back or error |

## Files to Modify

1. **`/Users/fz/centrifugal/protocol/client.proto`**
   - Add fields 15-22 to SubscribeRequest (keyed, phase, cursor, limit, offset, epoch, ordered, presence)
   - Add fields 15-18 to SubscribeResult (keyed, phase, cursor, presence_available)

2. **`/Users/fz/centrifugal/centrifuge/events.go`**
   - Add `Keyed` and `Presence` fields to SubscribeEvent

3. **`/Users/fz/centrifugal/centrifuge/options.go`**
   - Add `EnableKeyed`, `KeyedPresenceAvailable`, and `MaintainPresence` fields to SubscribeOptions

4. **`/Users/fz/centrifugal/centrifuge/client.go`**
   - Add `handleKeyedSubscribe()` function - router for keyed subscriptions
   - Add `handleKeyedSnapshotPhase()` function - stateless snapshot pagination
   - Add `handleKeyedStreamPhase()` function - stream recovery
   - Add `handleKeyedLivePhase()` function - join pub/sub
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
   - Presence maintenance (from main subscription) starts on LIVE phase join
   - Presence cleanup on disconnect
   - Presence cleanup on main unsubscribe
4. Test protocol compatibility with old clients (should ignore new fields)

## SDK flow

SNAPSHOT phase:
Server returns all snapshot entries (possibly paginated internally)
SDK buffers internally until snapshot is complete

STREAM phase (if catching up):
Server returns incremental stream entries
SDK buffers until LIVE

LIVE phase:
Server returns remaining stream entries
SDK fires onSubscribed with all snapshot publications
SDK flushes buffered stream entries via onPublication

Post-subscription:
All new publications flow directly to onPublication
