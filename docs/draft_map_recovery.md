# Map Subscription Synchronization Protocol

This document describes the synchronization protocol for map real-time subscriptions. Map subscriptions provide recoverable state with real-time updates for key-value like data structures.

## Overview

Map subscriptions support two join modes:

1. **Paginated Join (Scenario A)**: Client fetches state in pages, then catches up from stream before going live. Best for large states.
2. **Immediate Join (Scenario B)**: Client gets full state + stream in a single request. Best for small to moderate states.

Both modes provide strict server-driven consistency guarantees.

## Protocol Phases

The client declares intent using a numeric phase value in the subscribe request:

| Phase | Name   | Description                                    |
|-------|--------|------------------------------------------------|
| 2     | STATE  | Paginating over map state (key-value snapshot) |
| 1     | STREAM | Paginating over stream (history catch-up)      |
| 0     | LIVE   | Join live pub/sub, or request immediate join   |

## Key Definitions

- **offset**: Last stream offset known to the client
- **limit**: Maximum number of entries per response
- **STATE_start**: Server-captured stream top at the start of STATE pagination
- **STREAM_start**: Server-captured stream top at the start of STREAM pagination

## Core Invariant

Stream catch-up is a finite, bounded process. The client may paginate stream entries only up to STREAM_start, then must go LIVE. If the client cannot catch up in time (entries evicted), the subscription fails with ErrorUnrecoverablePosition.

---

## Scenario A: Paginated Join

Used when the client wants full control over state/stream pagination, or when state is too large for immediate join.

### Phase 2: STATE Pagination

**Client Request:**
```
phase = 2
cursor = "" (empty on first page)
limit = N
```

**Server Behavior:**
1. On first request: capture current stream position as STATE_start, initialize subscription state
2. Read state page
3. On subsequent pages: filter entries where `offset > STATE_start` (consistency filtering)
4. Return state publications, stream position, next cursor

**Client Behavior:**
1. Save stream position from first response
2. Continue pagination until cursor is empty
3. Switch to phase 1 with saved position

### Phase 1: STREAM Pagination

**Client Request:**
```
phase = 1
offset = saved offset from STATE phase
epoch = saved epoch from STATE phase
limit = N
```

**Server Behavior:**
1. On first STREAM request: capture current stream top as STREAM_start
2. Check LIVE condition: `offset + limit >= STREAM_start`
   - If true: transition to LIVE, return merged publications with `phase = 0`
   - If false: return stream page with `phase = 1`
3. Read stream page and return

**Client Behavior:**
1. If response has `phase = 0`: handle as LIVE response (server-controlled transition)
2. Otherwise: append publications to stream buffer, continue pagination
3. Update offset from publications

### Phase 0: LIVE

After pagination, server automatically transitions client to LIVE when close enough to catch up.

**Server Behavior (LIVE transition):**
1. Start buffering real-time publications
2. Add client to pub/sub
3. Read remaining stream (with RecoveryMaxPublicationLimit)
4. Merge stream with buffered publications
5. Return merged publications

---

## Scenario B: Immediate Join

Used when client prefers simplicity and state is small enough. Client goes directly to LIVE without pagination.

**Client Request:**
```
phase = 0
recover = false  // Fresh subscription
```

**Server Behavior:**
1. Read full state (enforce MapMaxImmediateJoinStateSize limit)
2. If state too large: return ErrorStateTooLarge (114)
3. Start buffering, add to pub/sub
4. Read stream from state position
5. Merge stream with buffered
6. Return both `state` (full state) and `publications` (stream)

**Client Behavior:**
1. Handle `state` field as initial state entries
2. Handle `publications` as stream catch-up
3. Emit subscribed event with state

---

## Recovery (Reconnection)

When a client reconnects with a known position, two approaches are supported:

### Option 1: Direct to LIVE (Recommended)

**Client Request:**
```
phase = 0
recover = true   // Reconnection
offset = last known offset
epoch = last known epoch
```

**Server Behavior:**
1. Skip state read (client already has state)
2. Read stream from client's position
3. Merge with buffered, return only `publications`

### Option 2: Stream Phase Recovery

For clients that prefer explicit stream pagination before going live:

**Client Request:**
```
phase = 1
recover = true   // Reconnection mode
offset = last known offset
epoch = last known epoch
limit = N
```

**Server Behavior:**
1. Create subscription state on the fly (skip STATE phase authorization)
2. Capture stream top as STREAM_start
3. Check LIVE condition: `offset + limit >= STREAM_start`
   - If true: transition to LIVE, return merged publications
   - If false: return stream page, client continues pagination
4. Same flow as normal STREAM phase from here

This option is useful when client needs to paginate through a large stream gap before going live.

---

## State Filtering During Pagination

During STATE pagination (Scenario A), entries may be modified after the first page was read. To ensure consistency:

1. First STATE page: server returns current position
2. Subsequent STATE pages: server filters entries where `entry.offset > saved_position`

This prevents duplicates when client catches up from stream.

---

## Direct STATE→LIVE Transition

When `MapStateToLiveEnabled` is true (configurable), the server can skip the STREAM phase entirely on the last STATE page if the stream hasn't advanced much.

### Trigger Condition

On the last STATE page (cursor would be empty):
```
state_position + limit >= current_stream_top
```

### Server Behavior

When condition is met:
1. Start buffering real-time publications
2. Add client to pub/sub
3. Read stream from state position to current
4. Merge stream with buffered publications
5. Return response with:
   - `phase = 0` (LIVE)
   - `state` field: remaining state entries from last page
   - `publications` field: stream catch-up publications

### Client Behavior

Client handles `phase != 2` response during STATE pagination:
1. Add any state entries from response to state buffer
2. Handle as LIVE response (same as STREAM→LIVE transition)

### Benefits

- Reduces round trips for slowly-updating channels
- Faster subscription when state fits in one page and stream activity is low
- Seamless - client handles automatically

### Configuration

```go
type Config struct {
    // MapStateToLiveEnabled: Allow direct STATE→LIVE transition
    // When true, server may skip STREAM phase on last state page
    // Default: false
    MapStateToLiveEnabled bool
}
```

---

## Error Conditions

| Error | Code | Description |
|-------|------|-------------|
| ErrorUnrecoverablePosition | 112 | Stream entries evicted, epoch changed, or position invalid |
| ErrorStateTooLarge | 114 | State exceeds MapMaxImmediateJoinStateSize (immediate join only) |

When ErrorStateTooLarge is returned, client should fall back to paginated join.

---

## Configuration Options

### Server Configuration (Go)

```go
type Config struct {
    // MapMaxPaginationLimit: Max entries per page (0 = no limit)
    MapMaxPaginationLimit int

    // MapMinStreamPaginationLimit: Minimum stream pagination limit
    // Prevents excessive round trips from tiny limits
    MapMinStreamPaginationLimit int

    // MapMaxImmediateJoinStateSize: Max state entries for immediate join
    // If state exceeds this, ErrorStateTooLarge is returned
    MapMaxImmediateJoinStateSize int

    // MapRecoveryMaxPublicationLimit: Max publications for stream catch-up
    // If limit reached, ErrorUnrecoverablePosition is returned
    MapRecoveryMaxPublicationLimit int

    // MapStateToLiveEnabled: Allow direct STATE→LIVE transition
    // When true, server may skip STREAM phase on last state page
    // if stream is close enough to go LIVE directly
    // Default: false
    MapStateToLiveEnabled bool
}
```

### Client Options (JS SDK)

```typescript
interface MapSubscriptionOptions {
    // Page size for state/stream pagination
    limit: number;

    // Enable immediate join mode (Scenario B)
    immediateJoin: boolean;

    // Strategy for handling ErrorStateTooLarge (code 114)
    // 'fatal': (default) go to error state
    // 'paginate': automatically fall back to paginated join
    stateTooLargeFallback: 'fatal' | 'paginate';

    // Strategy for handling unrecoverable position
    // 'from_scratch': auto-recover by resubscribing from snapshot
    // 'fatal': go to unsubscribed state
    unrecoverableStrategy: 'from_scratch' | 'fatal';
}
```

---

## Examples

### Paginated Join (Large State)

```typescript
const sub = centrifuge.newMapSubscription('large-channel', {
    limit: 100  // Page size
});

sub.on('subscribed', (ctx) => {
    // ctx.state contains initial state entries (from pagination)
    for (const entry of ctx.state || []) {
        processStateEntry(entry.key, entry.data);
    }
});

sub.on('publication', (ctx) => {
    // Real-time updates (stream publications)
    if (ctx.removed) {
        removeEntry(ctx.key);
    } else {
        updateEntry(ctx.key, ctx.data);
    }
});

sub.subscribe();
```

### Immediate Join (Small State)

```typescript
const sub = centrifuge.newMapSubscription('small-channel', {
    immediateJoin: true  // Single request for state + live
});

sub.on('subscribed', (ctx) => {
    // ctx.state contains full state
    for (const entry of ctx.state || []) {
        processStateEntry(entry.key, entry.data);
    }
});

sub.on('publication', (ctx) => {
    // Real-time updates (stream publications)
    updateEntry(ctx.key, ctx.data);
});

sub.subscribe();
```

### Handling ErrorStateTooLarge

Option 1: Automatic fallback to paginated join:

```typescript
const sub = centrifuge.newMapSubscription('channel', {
    immediateJoin: true,
    stateTooLargeFallback: 'paginate'  // Automatic fallback
});

sub.on('subscribed', (ctx) => {
    // Works for both immediate join and fallback to pagination
    for (const entry of ctx.state || []) {
        processStateEntry(entry.key, entry.data);
    }
});

sub.subscribe();
```

Option 2: Manual handling (default 'fatal' behavior):

```typescript
const sub = centrifuge.newMapSubscription('channel', {
    immediateJoin: true
    // stateTooLargeFallback: 'fatal'  // Default - error goes to error event
});

sub.on('error', (ctx) => {
    if (ctx.error.code === 114) {
        // State too large - manually fall back to paginated join
        sub.unsubscribe();
        centrifuge.removeSubscription(sub);

        const paginatedSub = centrifuge.newMapSubscription('channel', {
            limit: 100  // Use pagination
        });
        paginatedSub.subscribe();
    }
});

sub.subscribe();
```

---

## Protocol Messages

### SubscribeRequest

```protobuf
message SubscribeRequest {
    string channel = 1;
    int32 type = 2;      // 1=MAP, 2=MAP_CLIENTS, 3=MAP_USERS
    int32 phase = 3;     // 0=LIVE, 1=STREAM, 2=STATE
    string cursor = 4;   // Pagination cursor
    int32 limit = 5;     // Page size
    int64 offset = 6;    // Stream offset
    string epoch = 7;    // Stream epoch
    bool recover = 8;    // Recovery mode flag
    bool ordered = 9;    // Request ordered state
    // ... other fields
}
```

### SubscribeResult

```protobuf
message SubscribeResult {
    int32 type = 1;                     // 1=MAP
    int32 phase = 2;                    // Current phase
    string cursor = 3;                  // Next page cursor
    int64 offset = 4;                   // Stream offset
    string epoch = 5;                   // Stream epoch
    repeated Publication publications = 6;  // Stream publications
    repeated Publication state = 7;     // State publications (immediate join)
    // ... other fields
}
```

---

## Design Properties

1. **Server-controlled LIVE transition**: Server decides when client is close enough to go live
2. **Bounded stream recovery**: Stream catch-up is deterministic and finite
3. **One-RTT fast path**: Immediate join provides single round-trip for small states
4. **Slow client detection**: Clients too far behind fail fast with clear error
5. **Consistency during pagination**: State filtering prevents duplicates
