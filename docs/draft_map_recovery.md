# Map Subscription Synchronization Protocol

This document describes the synchronization protocol for map real-time subscriptions. Map subscriptions provide state with real-time updates for key-value data structures.

## Overview

Map subscriptions operate in two consistency modes:

- **Streamless** (default): No stream history. Clients read state and go live. Reconnection requires full state re-sync. Suitable for presence, leaderboards, ephemeral data.
- **Positioned** (opt-in via `EnablePositioning`/`EnableRecovery`): Stream-based consistency with offset tracking, recovery, and CAS support. Requires `StreamSize > 0` in `MapChannelOptions`. Suitable for inventory, auctions, collaborative editing.

Within both modes, two join strategies are supported:

1. **Paginated join** — client fetches state in pages (phase 2), then goes live. In positioned mode, an intermediate stream catch-up phase (phase 1) may be needed. Works for any state size.
2. **Immediate join** — client sends phase=0 directly; server returns full state + stream in one response. Only works when state fits within `MapMaxImmediateJoinStateSize`.

## Protocol Phases

The client declares its current phase via a numeric field in the subscribe request:

| Phase | Name | Description |
|-------|------|-------------|
| 2 | STATE | Paginating over map state snapshot |
| 1 | STREAM | Paginating over stream history (positioned mode only) |
| 0 | LIVE | Go live (immediate join, recovery, or final transition) |

Requesting phase=1 in streamless mode returns `ErrorBadRequest`.

## Protocol Messages

### SubscribeRequest (map-relevant fields)

```protobuf
message SubscribeRequest {
    string channel = 1;
    bool recover = 3;        // Recovery mode flag
    string epoch = 6;        // Stream epoch
    uint64 offset = 7;       // Stream offset
    int32 type = 15;         // 1=MAP, 2=MAP_CLIENTS, 3=MAP_USERS
    int32 phase = 16;        // 0=LIVE, 1=STREAM, 2=STATE
    string cursor = 17;      // Pagination cursor
    int32 limit = 18;        // Page size
    bool asc = 19;           // Ascending order (for ordered/scored channels)
}
```

### SubscribeResult (map-relevant fields)

```protobuf
message SubscribeResult {
    bool recoverable = 3;                    // True when positioned mode
    string epoch = 6;                        // Stream epoch
    repeated Publication publications = 7;   // Stream/buffered publications
    bool recovered = 8;                      // True if recovery succeeded
    uint64 offset = 9;                       // Current stream offset
    int32 type = 15;                         // Echo of request type
    int32 phase = 16;                        // Response phase
    string cursor = 17;                      // Next page cursor (empty = last page)
    repeated Publication state = 18;         // State entries
}
```

### Publication (map-relevant fields)

```protobuf
message Publication {
    bytes data = 4;
    ClientInfo info = 5;
    uint64 offset = 6;
    map<string, string> tags = 7;
    string key = 11;          // Map key
    bool removed = 12;        // Removal indicator
    sint64 score = 13;        // Sort score
}
```

Response field usage by phase:

| Phase | `state` | `publications` | `cursor` | `recoverable` |
|-------|---------|----------------|----------|----------------|
| STATE (2) | state entries | — | next cursor | — |
| STREAM (1) | — | stream entries | — | — |
| LIVE (0) | full state (immediate join, STATE→LIVE) | stream catch-up + buffered | — | yes/no |

---

## Streamless Mode

When neither `EnablePositioning` nor `EnableRecovery` is set.

### Paginated Join

```
Client                                   Server
  |                                        |
  |── phase=2, cursor="" ────────────────>|  STATE page 1
  |<── state entries, cursor="abc" ───────|  phase=2
  |                                        |
  |── phase=2, cursor="abc" ─────────────>|  STATE last page
  |<── state entries, phase=0 ────────────|  → LIVE (always on last page)
  |                                        |
  |<══ real-time publications ════════════|  LIVE
```

In streamless mode, the server **always** transitions to LIVE on the last state page (when cursor would be empty). This happens regardless of the `MapStateToLiveEnabled` setting — that flag only governs positioned mode.

On the last page the server:
1. Starts buffering real-time publications
2. Subscribes client to pub/sub
3. Reads buffered publications
4. Returns state entries + buffered publications with `phase=0`

No offset-based dedup is needed — keyed data provides natural dedup (last writer wins per key).

The response includes `recoverable: false`. The client sets `_recover = false`, meaning future reconnections will do fresh state re-sync rather than stream recovery.

### Immediate Join

```
Client                                   Server
  |                                        |
  |── phase=0, recover=false ────────────>|
  |<── state + buffered pubs, phase=0 ────|  LIVE (recoverable: false)
  |                                        |
  |<══ real-time publications ════════════|  LIVE
```

Server reads full state and returns it in the `state` field. If state exceeds `MapMaxImmediateJoinStateSize`, returns `ErrorStateTooLarge` (114).

### Reconnection

On reconnect, the JS client has `_recover = false` (set from the streamless LIVE response). It calls `_mapSubscribe()` which starts a fresh paginated join from STATE phase — a full state re-sync.

If a streamless client somehow sends `recover=true` (e.g., `since` option set manually), the server's `handleMapRecoveryJoin` detects streamless mode and redirects to immediate join with full state. The response has `recoverable: false` and `recovered: false`.

### Restrictions

- `phase=1` (STREAM) returns `ErrorBadRequest`
- `ExpectedPosition` (CAS) is not available
- `Version`-based concurrency control is not available

---

## Positioned Mode

Applies when `EnablePositioning: true` or `EnableRecovery: true`. The publish side must also set `StreamSize > 0` in `MapChannelOptions`.

The subscribe response includes `recoverable: true`, enabling stream-based recovery on reconnect.

### Paginated Join (Scenario A)

#### Phase 2: STATE Pagination

**Client sends:**
```
phase = 2
cursor = ""       (empty on first page)
limit = N         (default: 100)
```

**Server behavior:**
1. First request: create subscription state, capture current stream position
2. Read state page, return entries in `state` field with stream position in `epoch`/`offset`
3. Subsequent pages: client sends saved `offset`/`epoch`; server filters entries where `entry.offset > saved_offset` (prevents duplicates with stream catch-up)
4. Return next cursor (empty on last page)

**Client behavior:**
1. Save `epoch` and `offset` from first response
2. Continue pagination until cursor is empty (or server responds with `phase=0`)
3. If response has `phase=0`: handle as LIVE (server-triggered STATE→LIVE transition)
4. Otherwise: switch to phase=1 with saved position

#### Phase 1: STREAM Pagination

**Client sends:**
```
phase = 1
offset = saved offset from STATE phase
epoch = saved epoch from STATE phase
limit = N         (default: 100)
```

**Server behavior:**
1. First STREAM request: capture current stream top as `streamStart`
2. Check LIVE condition: `offset + limit >= streamStart`
   - If true: transition to LIVE, return merged publications with `phase=0`
   - If false: return stream page with `phase=1`
3. Subsequent pages: same condition check with updated offset

**Client behavior:**
1. If response has `phase=0`: handle as LIVE (server-controlled transition)
2. If epoch changed: restart from STATE phase (fresh snapshot)
3. Otherwise: buffer publications, update offset, continue pagination

#### Phase 0: LIVE Transition

When the LIVE condition is met, the server:
1. Starts buffering real-time publications
2. Subscribes client to pub/sub
3. Reads remaining stream entries (bounded by `MapRecoveryMaxPublicationLimit`)
4. Calls `MergePublications()` — concatenates stream + buffered pubs, sorts by offset, deduplicates, validates no gaps
5. Returns merged publications with `phase=0`, `recoverable=true`

If the stream read exceeds `MapRecoveryMaxPublicationLimit`, returns `ErrorUnrecoverablePosition` (112).

If merge validation fails (gaps in offsets), returns `DisconnectInsufficientState` (3010) — this disconnects the entire transport connection (see [Merge Failure](#merge-failure)).

### Immediate Join (Scenario B)

**Client sends:**
```
phase = 0
recover = false
```

**Server behavior:**
1. Start buffering, subscribe to pub/sub
2. Read full state (enforce `MapMaxImmediateJoinStateSize` limit)
3. If state too large (cursor returned): return `ErrorStateTooLarge` (114)
4. In positioned mode: read stream from state position, merge with buffered publications
5. Return `state` (full state) + `publications` (stream catch-up), `recoverable=true`

---

## Recovery (Reconnection)

When the client reconnects after a transport disconnect, it has `_recover = true` (set from the previous `recoverable: true` response) and saved `offset`/`epoch`.

### Client Recovery Decision

The JS client's `_mapSubscribe()` checks `_recover` and saved position:

```
if (_recover && _offset !== null && _epoch !== null) {
    // Skip STATE, go directly to STREAM phase
    _mapPhase = Stream
    _fetchStream()
} else {
    // Fresh subscription from STATE phase
    _mapPhase = State
    _fetchSnapshot()
}
```

So by default, a recovering positioned client enters at **STREAM phase** (not LIVE), which gives the server the opportunity to paginate the catch-up if the gap is large.

### Server Recovery Handling

#### Via STREAM Phase (default client behavior)

**Client sends:**
```
phase = 1
recover = true
offset = last known offset
epoch = last known epoch
limit = N
```

**Server behavior:**
1. Detects `recover=true` without prior STATE phase — creates subscription state on the fly (skips STATE phase authorization)
2. Validates epoch (mismatch → `ErrorUnrecoverablePosition`)
3. Captures `streamStart`, checks LIVE condition: `offset + limit >= streamStart`
   - If true: transition to LIVE with merged publications, `recovered=true`
   - If false: return stream page, client continues pagination
4. Same flow as normal STREAM phase from here

#### Via LIVE Phase (direct recovery)

If the client sends `phase=0, recover=true` directly:

**Server behavior:**
1. Skip state read (client already has state)
2. Start buffering, subscribe to pub/sub
3. Read stream from client's position
4. Merge stream + buffered publications
5. Return `publications` only, `recovered=true`, `recoverable=true`

If stream exceeds `MapRecoveryMaxPublicationLimit`, returns `ErrorUnrecoverablePosition` (112).

### Recovery Failure Scenarios

#### ErrorUnrecoverablePosition (code 112)

Triggered when:
- Stream epoch mismatch (server restarted, stream reset)
- Client position too old (stream entries already expired)
- Stream read exceeds `MapRecoveryMaxPublicationLimit`
- Recovery with `epoch=""` when server has a real epoch (PG broker empty epoch guard — see [Empty Epoch Handling](draft_map_postgres.md#empty-epoch-handling))

**Client behavior** depends on `unrecoverableStrategy` option:

- **`'from_scratch'`** (default): Client clears saved position (`_offset = null`, `_epoch = null`, `_recover = false`), then schedules a resubscribe from scratch. This starts a fresh paginated join from STATE phase with exponential backoff.
- **`'fatal'`**: Subscription moves to `Unsubscribed` state. Application must handle the error and decide next action.

#### DisconnectInsufficientState (code 3010)

Triggered when `MergePublications()` detects gaps in the publication offset sequence. This indicates a server-side issue (publication missed during buffering transition).

**Server behavior:** Disconnects the entire transport connection (not just the subscription).

**Client behavior:** The transport reconnects automatically. On reconnect, the client will attempt recovery again. If recovery keeps failing:
- The stream advances, eventually exceeding `MapRecoveryMaxPublicationLimit` → `ErrorUnrecoverablePosition` → `from_scratch` strategy kicks in.
- Or the merge succeeds on retry (the transient condition resolved).

#### ErrorStateTooLarge (code 114)

Only applies to immediate join mode (`immediateJoin: true`). State has more entries than `MapMaxImmediateJoinStateSize`.

**Client behavior** depends on `stateTooLargeFallback` option:

- **`'paginate'`**: Client disables immediate join, switches to STATE phase, and schedules a resubscribe with pagination.
- **`'fatal'`** (default): Subscription moves to `Unsubscribed` state.

---

## STATE-to-LIVE Direct Transition

In **streamless mode**, the server **always** transitions to LIVE on the last state page. The `MapStateToLiveEnabled` setting has no effect on streamless mode.

In **positioned mode**, when `MapStateToLiveEnabled` is set, the server can skip the STREAM phase on the last state page. The transition triggers when:
```
state_position + limit >= current_stream_top
```

The server:
1. Starts buffering
2. Subscribes to pub/sub
3. Reads stream from state position
4. Merges with buffered publications
5. Returns `phase=0` with last-page state entries in `state` and stream catch-up in `publications`

If the condition is not met (stream advanced too far during pagination), the server returns a normal STATE response (`phase=2`) and the client proceeds to STREAM phase.

The client handles `phase=0` during STATE pagination by treating it as a LIVE response (server-controlled transition).

---

## Buffering Mechanism

The buffering system (`pubSubSync`) ensures no publications are lost during the transition to LIVE:

1. **StartBuffering** — creates buffer, sets `inSubscribe` flag; incoming pub/sub messages are accumulated
2. **addSubscription** — subscribes client to broker; real-time publications now buffered
3. **LockBufferAndReadBuffered** — locks buffer, returns accumulated publications
4. **Final stream read + merge** — while buffer is locked, no new publications added
5. **StopBuffering** — clears flag, unlocks buffer; publications now delivered directly to client

Between `addSubscription` and `LockBufferAndReadBuffered`, all real-time publications are captured. Between lock and response write, the buffer is frozen. After response, live delivery bypasses the buffer.

### MergePublications

Used in all positioned mode LIVE transitions. Algorithm:
1. Concatenate recovered (stream) + buffered publications
2. Sort by offset ascending
3. Deduplicate by offset
4. Validate no gaps (or all gaps are filtered publications)
5. Return `ok=false` if validation fails

In streamless mode, merge is not used — buffered publications are returned directly (no offset validation needed, keyed data provides natural dedup).

---

## Recovery Limit Boundary

The `MapRecoveryMaxPublicationLimit` controls how many stream publications the server reads during LIVE transitions. The server uses a **read limit+1** pattern to distinguish "exactly at limit" from "too far behind":

```go
readLimit := streamLimit
if readLimit > 0 {
    readLimit = streamLimit + 1  // Read one extra
}
// ... read with readLimit ...
if streamLimit > 0 && len(pubs) > streamLimit {
    return ErrorUnrecoverablePosition  // More than limit → too far behind
}
```

- If configured limit is 100, server reads with limit=101
- If result has ≤100 publications → recovery succeeds
- If result has 101 publications → client is too far behind → `ErrorUnrecoverablePosition`

This pattern is used in all four LIVE transition paths: STATE→LIVE, STREAM→LIVE, immediate join, and recovery join.

---

## Client-Side Epoch Validation

The JS client validates epoch consistency between phases. If the epoch changes between responses, the client restarts from STATE phase:

- **During STREAM phase:** If `result.epoch !== saved_epoch`, clears buffers and restarts `_fetchSnapshot()`
- **During LIVE transition:** Same check — if epoch changed, restarts from STATE (or retries immediate join)

This handles the case where the server's stream was reset (e.g., server restart) during multi-phase subscription. The client detects the inconsistency and starts over rather than merging data from different epochs.

---

## Server-Side Epoch Adoption (Map Channels)

When using the PostgreSQL broker, map channels may have `epoch=""` at subscribe time (no data has been published yet). The server handles this via epoch adoption in the publication delivery path:

- **Map channel with `epoch=""`**: When the first publication arrives with a real epoch, the server adopts it — updates the channel context epoch without triggering insufficient state. Subsequent publications use the adopted epoch for normal offset/epoch validation.
- **Non-map channels**: Epoch adoption is **not** applied. A non-map channel with `epoch=""` receiving a publication with a real epoch triggers `handleInsufficientState` as before.
- **Map channel with real epoch**: Normal epoch mismatch handling applies — triggers `handleInsufficientState`.

The adoption is safe because `epoch=""` only means "no data existed at subscribe time" — there is no stale state to protect.

**Important limitation:** The client SDK stores the epoch from the subscribe response and never updates it from publication deliveries. So after epoch adoption on the server, the SDK still has `epoch=""`. On reconnect, the SDK sends `epoch=""` in the recovery request. The server's [empty epoch recovery guard](draft_map_postgres.md#recovery-guard-for-empty-epoch) detects this and returns `ErrorUnrecoverablePosition`, forcing a clean re-subscribe.

---

## Error Conditions

| Error | Code | Description | Client default behavior |
|-------|------|-------------|------------------------|
| `ErrorBadRequest` | 107 | Invalid request (STREAM in streamless, invalid filter) | Unsubscribed |
| `ErrorUnrecoverablePosition` | 112 | Epoch mismatch, position too old, or recovery limit exceeded | Resubscribe from scratch |
| `ErrorConcurrentPagination` | 113 | Pagination request while another is in progress | Unsubscribed |
| `ErrorStateTooLarge` | 114 | State exceeds `MapMaxImmediateJoinStateSize` | Depends on `stateTooLargeFallback` |
| `DisconnectInsufficientState` | 3010 | Merge gap detected (transport disconnect) | Auto-reconnect, retry recovery |

---

## Server Configuration

```go
type Config struct {
    // MapMaxPaginationLimit: max entries per page (0 = no limit).
    MapMaxPaginationLimit int

    // MapMinStreamPaginationLimit: minimum stream page size.
    // Prevents excessive round trips from tiny limits. 0 = no minimum.
    MapMinStreamPaginationLimit int

    // MapMaxImmediateJoinStateSize: max state entries for immediate join.
    // Exceeding triggers ErrorStateTooLarge (114). 0 = no limit.
    MapMaxImmediateJoinStateSize int

    // MapRecoveryMaxPublicationLimit: max stream publications during LIVE transition.
    // Exceeding limit triggers ErrorUnrecoverablePosition (112). 0 = no limit.
    MapRecoveryMaxPublicationLimit int

    // MapStateToLiveEnabled: allow STATE→LIVE direct transition on last state page
    // in positioned mode. Skips STREAM phase when stream hasn't advanced much.
    // Only applies to positioned mode — streamless always transitions to LIVE
    // on last page regardless of this setting.
    MapStateToLiveEnabled bool
}
```

## Client Configuration (JS SDK)

```typescript
const sub = centrifuge.newMapSubscription('channel', {
    limit: 100,                              // Page size (default: 100)
    immediateJoin: false,                     // true = Scenario B (default: false)
    stateTooLargeFallback: 'fatal',           // 'fatal' | 'paginate' (default: 'fatal')
    unrecoverableStrategy: 'from_scratch',    // 'from_scratch' | 'fatal' (default: 'from_scratch')
});

sub.on('subscribed', (ctx) => {
    // ctx.state: MapPublicationContext[] — initial state entries
    for (const entry of ctx.state || []) {
        processEntry(entry.key, entry.data, entry.score);
    }
});

sub.on('publication', (ctx) => {
    // ctx: MapPublicationContext — real-time update
    if (ctx.removed) {
        removeEntry(ctx.key);
    } else {
        updateEntry(ctx.key, ctx.data, ctx.score);
    }
});

sub.subscribe();
```

Three factory methods on the client:
- `newMapSubscription(channel, opts)` — generic keyed state (type=1)
- `newMapClientsSubscription(channel, opts)` — per-client presence (type=2, key=clientId)
- `newMapUsersSubscription(channel, opts)` — per-user presence (type=3, key=userId)

All three types support both streamless and positioned modes — the mode is determined by the server's `SubscribeOptions` (e.g., presence subscriptions can be positioned if the server sets `EnablePositioning: true`).

### Error Handling

```typescript
// Automatic fallback from immediate join to pagination:
const sub = centrifuge.newMapSubscription('channel', {
    immediateJoin: true,
    stateTooLargeFallback: 'paginate',  // Auto-retry with pagination on code 114
});

// Automatic recovery on unrecoverable position (default):
const sub = centrifuge.newMapSubscription('channel', {
    unrecoverableStrategy: 'from_scratch',  // Re-subscribe from scratch on code 112
});
```
