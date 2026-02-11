# Map Subscriptions

Map Subscriptions is a feature for building real-time collaborative applications with shared state. It provides a synchronized key-value data structure where clients join at any time and receive both current state and real-time updates.

## Consistency Modes

Map subscriptions support two consistency modes, controlled by `EnablePositioning` and `EnableRecovery` in `SubscribeOptions`:

### Streamless Mode (Default)

When neither `EnablePositioning` nor `EnableRecovery` is set (the default), map subscriptions operate without a stream:

- **No stream history**: Publishes update state and broadcast via pub/sub, but no ordered log is maintained
- **No offset tracking**: Publications have offset=0
- **Reconnection**: Client receives full state re-sync (no incremental recovery)
- **Duplicate tolerance**: Keyed data provides natural dedup — last writer wins per key
- **Performance**: ~35-40% less work per publish (no XADD, no offset increment, no stream reads)

Best for: presence, leaderboards, ephemeral state, IoT dashboards — any use case where state is always available and a page reload gives consistent state.

### Positioned Mode (Opt-in)

When `EnablePositioning: true` or `EnableRecovery: true` is set in subscribe options, the full stream-based consistency model is enabled:

- **Stream history**: Every publish appends to an ordered stream for recovery
- **Offset tracking**: Each publication gets a monotonically increasing offset
- **Reconnection recovery**: Client catches up from last known offset without full re-sync
- **CAS operations**: `ExpectedPosition` for atomic read-modify-write
- **Version-based dedup**: `Version`/`VersionEpoch` for optimistic concurrency control

Best for: inventory management, auction bidding, collaborative editing — use cases requiring strict consistency and atomic operations.

## Motivation

Traditional pub/sub systems deliver messages only to currently connected clients. This creates challenges for applications requiring:

- **Late joiners**: New clients need the current state, not just future updates
- **Reconnection recovery**: Clients disconnecting briefly shouldn't miss updates
- **Consistency guarantees**: All clients should eventually see the same state
- **Atomic operations**: Updates to shared state need coordination (e.g., inventory management)

Map Subscriptions solve these problems by combining:
1. A **key-value snapshot** representing current state
2. An **ordered stream** of changes for recovery (when positioned mode is enabled)
3. A **pub/sub layer** for real-time delivery

## Use Cases

### Collaborative Applications
- **Shared cursors**: Track mouse positions of all users on a canvas
- **Collaborative editing**: Sync document state across editors
- **Shared whiteboards**: Real-time drawing with late-join support

### Presence and Activity
- **Who's online**: Track connected users per channel
- **Typing indicators**: Show who's currently typing
- **Live activity feeds**: Recent actions with full history

### Gaming and Competition
- **Live leaderboards**: Ranked scores with real-time updates
- **Game lobbies**: Available games with player counts
- **Match state**: Synchronized game state across players

### E-commerce and Inventory
- **Limited inventory**: Atomic stock decrements with CAS
- **Auction bidding**: Consistent bid ordering
- **Flash sales**: Race-condition-free purchases

### IoT and Monitoring
- **Device status**: Last-known state of all devices
- **Sensor dashboards**: Current readings with historical context
- **Alert systems**: Active alerts with acknowledgment tracking

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                      Map Subscription                        │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐  │
│  │   Snapshot   │    │    Stream    │    │   Pub/Sub    │  │
│  │  (State)     │    │  (History)   │    │  (Live)      │  │
│  │              │    │  (optional)  │    │              │  │
│  │  key → data  │    │  [offset 1]  │    │  Real-time   │  │
│  │  key → data  │    │  [offset 2]  │    │  delivery    │  │
│  │  key → data  │    │  [offset 3]  │    │              │  │
│  └──────────────┘    └──────────────┘    └──────────────┘  │
│         │                   │                   │           │
│         └───────────────────┴───────────────────┘           │
│                             │                                │
│             Streamless Mode (default):                       │
│                    1. Read state (paginated)                 │
│                    2. Go live (pub/sub)                      │
│                                                              │
│             Positioned Mode (opt-in):                        │
│                    1. Read state (paginated)                 │
│                    2. Catch up from stream                   │
│                    3. Go live (pub/sub)                      │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

## MapBroker Interface

The `MapBroker` interface defines the storage backend for map subscriptions:

### Core Operations

```go
type MapBroker interface {
    // Pub/sub coordination
    Subscribe(ch string) error      // Register node for channel messages
    Unsubscribe(ch string) error    // Unregister node from channel

    // State mutations
    Publish(ctx context.Context, ch string, key string, opts MapPublishOptions) (MapPublishResult, error)
    Remove(ctx context.Context, ch string, key string, opts MapRemoveOptions) (MapPublishResult, error)

    // State reads
    ReadState(ctx context.Context, ch string, opts MapReadStateOptions) (pubs []*Publication, pos StreamPosition, cursor string, err error)
    ReadStream(ctx context.Context, ch string, opts MapReadStreamOptions) (pubs []*Publication, pos StreamPosition, err error)

    // Metadata
    Stats(ctx context.Context, ch string) (MapStats, error)
    Clear(ctx context.Context, ch string, opts MapClearOptions) error
}
```

### Publish Options

```go
type MapPublishOptions struct {
    Data       []byte          // Value to store
    StreamData []byte          // Optional different payload for stream

    // Key behavior
    KeyMode    KeyMode         // Replace (default), IfNew, IfExists
    KeyTTL     time.Duration   // Auto-expire key after duration

    // Stream retention (only meaningful when StreamSize > 0)
    StreamSize int             // Max entries in stream (default 0 = streamless)
    StreamTTL  time.Duration   // Auto-expire stream entries (default 0 = disabled)
    MetaTTL    time.Duration   // Keep metadata after stream expires

    // Concurrency control (requires StreamSize > 0)
    ExpectedPosition *StreamPosition  // CAS: fail if position doesn't match
    Version          uint64           // Optimistic: reject if version <= current
    VersionEpoch     string           // Version namespace

    // Ordering
    Score float64              // For sorted retrieval (leaderboards)

    // Idempotency
    IdempotencyKey string      // Deduplicate within IdempotencyTTL
    IdempotencyTTL time.Duration

    // TTL behavior
    RefreshTTLOnSuppress bool  // Refresh key TTL even if write suppressed
}
```

> **Note**: `ExpectedPosition` (CAS) and `Version`-based dedup require `StreamSize > 0`. Using them with streamless publishing returns an error.

### Key Modes

- **`KeyModeReplace`** (default): Always update the key
- **`KeyModeIfNew`**: Only create if key doesn't exist (lobby slots, unique claims)
- **`KeyModeIfExists`**: Only update if key exists (heartbeats, presence refresh)

### Suppression Reasons

When a write doesn't modify state, the broker returns a suppression reason:

| Reason | Description |
|--------|-------------|
| `SuppressReasonIdempotency` | Duplicate idempotency key within TTL window |
| `SuppressReasonKeyExists` | KeyModeIfNew but key already exists |
| `SuppressReasonKeyNotFound` | KeyModeIfExists but key doesn't exist |
| `SuppressReasonPositionMismatch` | CAS failed (offset/epoch mismatch) |
| `SuppressReasonVersion` | Version <= current stream version |

## Map Presence Subscriptions

Map subscriptions support automatic presence tracking via special channel prefixes. This enables tracking who is subscribed to a channel without manual presence management.

### Subscription Types

| Type | Value | Purpose |
|------|-------|---------|
| `MAP` | 1 | Regular map subscription |
| `MAP_CLIENTS` | 2 | Track individual client connections |
| `MAP_USERS` | 3 | Track unique users |

### Client Presence (`MAP_CLIENTS`)

Configured via `MapClientPresenceChannelPrefix` (e.g., `"clients:"`):

- **Key**: Client ID
- **Value**: Full `ClientInfo` (user ID, connection info, channel info)
- **Lifecycle**: Published on subscribe, removed on unsubscribe
- **Use case**: See all connected clients (multiple entries per user if multi-connection)

### User Presence (`MAP_USERS`)

Configured via `MapUserPresenceChannelPrefix` (e.g., `"users:"`):

- **Key**: User ID
- **Value**: Minimal data
- **Lifecycle**: Published on subscribe, expires via TTL (not removed on unsubscribe)
- **Use case**: See unique users (one entry per user regardless of connections)

The TTL-based expiration for user presence provides a grace period for quick reconnections, avoiding presence flicker.

### Configuration Example

```go
node.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
    cb(SubscribeReply{
        Options: SubscribeOptions{
            EnableMap:                      true,
            MapClientPresenceChannelPrefix: "clients:",
            MapUserPresenceChannelPrefix:   "users:",
            // Presence channels work well in streamless mode (default).
            // No need for EnablePositioning or EnableRecovery here.
        },
    }, nil)
})
```

Clients can then subscribe to presence channels to see who's connected:

```javascript
// Track individual clients
const clientsSub = centrifuge.newMapClientsSubscription('mychannel');
clientsSub.on('subscribed', (ctx) => {
    for (const entry of ctx.state || []) {
        console.log('Client connected:', entry.key, entry.data);
    }
});

// Track unique users
const usersSub = centrifuge.newMapUsersSubscription('mychannel');
usersSub.on('subscribed', (ctx) => {
    for (const entry of ctx.state || []) {
        console.log('User online:', entry.key);
    }
});
```

## Broker Implementations

### Memory Broker

**Use case**: Development, testing, single-node deployments

```go
broker, _ := NewMemoryMapBroker(node, MemoryMapBrokerConfig{})
node.SetMapBroker(broker)
```

**Characteristics**:
- Zero external dependencies
- Lowest latency
- All data in process memory
- Lost on restart
- Single node only

**Internal structure**:
- Hash map for key-value state
- In-memory stream with epoch tracking (when StreamSize > 0)
- Background goroutines for TTL cleanup
- Supports score-based ordering

### Redis Broker

**Use case**: Distributed deployments, horizontal scaling

```go
broker, _ := NewRedisMapBroker(node, RedisMapBrokerConfig{
    Shards: []redis.UniversalClient{redisClient},
})
node.SetMapBroker(broker)
```

**Characteristics**:
- Distributed across Redis cluster
- Pub/sub for real-time fan-out
- Lua scripts for atomic operations
- Supports sharding across multiple Redis instances

**Storage model**:
- `HASH` for unordered state
- `HASH` + `ZSET` for ordered state (leaderboards)
- `XADD/XREAD` for stream (Redis Streams, when StreamSize > 0)
- `HASH` with TTL for idempotency tracking

**Atomic operations** via embedded Lua scripts:
- `map_broker_add.lua`: Publish with all validation checks
- `map_broker_read_ordered.lua` / `map_broker_read_unordered.lua`: State pagination
- `map_broker_stream_read.lua`: Stream pagination
- `map_broker_cleanup.lua`: TTL cleanup

### PostgreSQL Broker

**Use case**: ACID transactions, persistence, SQL-based publishing

```go
broker, _ := NewPostgresMapBroker(node, PostgresMapBrokerConfig{
    DSN: "postgres://user:pass@localhost/db",
})
node.SetMapBroker(broker)
```

**Characteristics**:
- Full ACID transactions
- Persistent across restarts
- CAS operations within database transactions
- Publish directly from application SQL

**Schema** (4 tables):
- `cf_map_stream`: Change history with per-channel offsets (when StreamSize > 0)
- `cf_map_state`: Current key-value snapshot
- `cf_map_meta`: Stream metadata (epoch, top offset, when StreamSize > 0)
- `cf_map_idempotency`: Duplicate detection

**Real-time delivery**: Uses PostgreSQL logical replication (WAL) with 16 sharded publications. A WAL reader process delivers changes to connected clients.

#### Transactional Publishing

The PostgreSQL broker provides SQL functions for publishing within application transactions:

```sql
-- Publish or update a key
SELECT * FROM cf_map_publish(
    p_channel := 'leaderboard',
    p_key := 'user123',
    p_data := '{"name": "Alice", "score": 100}'::jsonb,
    p_score := 100,  -- For ordered retrieval
    p_stream_ttl := '1 hour'::interval
);

-- Remove a key
SELECT * FROM cf_map_remove(
    p_channel := 'leaderboard',
    p_key := 'user123'
);
```

**Return values**:
```sql
(result_id, channel_offset, epoch, suppressed, suppress_reason, current_data, current_offset)
```

**Strict variants** (`cf_map_publish_strict`, `cf_map_remove_strict`) raise an exception on suppression, enabling automatic transaction rollback:

```sql
BEGIN;
    -- Deduct from balance
    UPDATE accounts SET balance = balance - 100 WHERE user_id = 'user123';

    -- Publish purchase to real-time channel (rolls back if CAS fails)
    SELECT cf_map_publish_strict(
        p_channel := 'inventory',
        p_key := 'item_42',
        p_data := '{"stock": 9}'::jsonb,
        p_expected_offset := 5,  -- CAS check
        p_expected_epoch := 'abc123'
    );
COMMIT;
```

This enables atomic operations across your application data and real-time state.

#### Leaderboard Example

The `_examples/map_demo/leaderboard.html` demonstrates PostgreSQL-backed leaderboards:

```go
// Server-side HTTP handler
func handleLeaderboardJoin(w http.ResponseWriter, r *http.Request) {
    var req struct {
        UserID string `json:"userId"`
        Name   string `json:"name"`
        Color  string `json:"color"`
    }
    json.NewDecoder(r.Body).Decode(&req)

    // Publish via SQL function
    row := db.QueryRow(`
        SELECT * FROM cf_map_publish(
            p_channel := 'leaderboard',
            p_key := $1,
            p_data := $2,
            p_score := 0,
            p_stream_ttl := '1 hour'::interval
        )
    `, req.UserID, leaderboardEntryJSON(req))

    // Real-time update delivered automatically via WAL
}
```

Clients subscribe and receive both initial state and updates:

```javascript
const sub = centrifuge.newMapSubscription('leaderboard', {
    limit: 100,
    ordered: true  // Sorted by score
});

sub.on('subscribed', (ctx) => {
    // Initial leaderboard state (sorted by score descending)
    renderLeaderboard(ctx.state);
});

sub.on('publication', (ctx) => {
    // Real-time score updates
    if (ctx.removed) {
        removePlayer(ctx.key);
    } else {
        updatePlayer(ctx.key, ctx.data);
    }
});
```

### Cached Broker

**Use case**: Wrap any backend for low-latency reads and read-your-own-writes consistency

```go
cachedBroker, _ := NewCachedMapBroker(backendBroker, CachedMapBrokerConfig{
    Cache: MapCacheConfig{
        MaxChannels: 10000,
        MaxKeys:     1000000,
    },
    SyncInterval: 30 * time.Second,
})
node.SetMapBroker(cachedBroker)
```

**Characteristics**:
- Wraps any `MapBroker` (Redis, PostgreSQL)
- In-memory cache with lazy loading
- Immediate visibility of local writes
- Background sync with backend
- Gap detection and automatic refill

**How it works**:
1. First read loads state from backend into cache
2. Local writes update cache immediately (read-your-own-writes)
3. Pub/sub messages update cache in real-time
4. Background goroutines periodically sync with backend
5. Gap detection: if offset jumps, fills missing entries from backend

**Configuration**:
```go
type CachedMapBrokerConfig struct {
    Cache           MapCacheConfig    // Size limits
    SyncInterval    time.Duration     // Backend sync frequency (default 30s)
    SyncJitter      float64           // Prevents thundering herd (default 10%)
    SyncConcurrency int               // Parallel sync workers (0 = unbounded)
    LoadTimeout     time.Duration     // Backend read timeout (default 5s)
}
```

## Client Protocol

The client subscription protocol uses three phases to ensure consistency:

| Phase | Value | Description |
|-------|-------|-------------|
| STATE | 2 | Paginate through current key-value snapshot |
| STREAM | 1 | Catch up on changes since snapshot (positioned mode only) |
| LIVE | 0 | Real-time pub/sub delivery |

### Subscription Flow (Streamless Mode)

In streamless mode (default), the STREAM phase is skipped entirely:

```
Client                                    Server
   │                                         │
   │──── phase=2, cursor="" ────────────────>│  STATE (page 1)
   │<─── state entries, cursor="abc" ────────│
   │                                         │
   │──── phase=2, cursor="abc" ─────────────>│  STATE (page 2)
   │<─── state entries, phase=0 ─────────────│  (last page → LIVE)
   │                                         │
   │<════ real-time publications ════════════│  LIVE
```

On the last state page, the server automatically transitions to LIVE. Any publications buffered during the state read are included. Requesting `phase=1` (STREAM) in streamless mode returns an error.

On reconnect, the client performs a full state re-sync (no incremental recovery).

### Subscription Flow (Positioned Mode)

When `EnablePositioning` or `EnableRecovery` is set, the full three-phase flow is used:

```
Client                                    Server
   │                                         │
   │──── phase=2, cursor="" ────────────────>│  STATE (page 1)
   │<─── state entries, cursor="abc" ────────│
   │                                         │
   │──── phase=2, cursor="abc" ─────────────>│  STATE (page 2)
   │<─── state entries, cursor="" ───────────│  (last page)
   │                                         │
   │──── phase=1, offset=X ─────────────────>│  STREAM catch-up
   │<─── publications, phase=0 ──────────────│  Server triggers LIVE
   │                                         │
   │<════ real-time publications ════════════│  LIVE
```

On reconnect with `recover=true`, the client catches up from its last known stream offset.

For detailed protocol specification including server-controlled LIVE transitions, state filtering, immediate join mode, and recovery, see [Map Recovery Protocol](draft_map_recovery.md).

### JavaScript SDK Usage

```typescript
// Streamless map subscription (default)
const sub = centrifuge.newMapSubscription('channel', {
    limit: 100,           // Page size for pagination
    ordered: true,        // Score-based ordering
});

sub.on('subscribed', (ctx) => {
    // ctx.state contains initial key-value entries
    for (const entry of ctx.state || []) {
        console.log(entry.key, entry.data);
    }
    // ctx.recoverable indicates whether stream recovery is available
    console.log('recoverable:', ctx.recoverable);
});

sub.on('publication', (ctx) => {
    // Real-time updates
    if (ctx.removed) {
        handleRemove(ctx.key);
    } else {
        handleUpdate(ctx.key, ctx.data);
    }
});

sub.subscribe();
```

On reconnect, the client SDK automatically respects `recoverable`: when `false` (streamless), reconnection does a fresh subscribe instead of attempting stream recovery.

## Configuration Reference

### Server Configuration

```go
type Config struct {
    // Pagination limits
    MapMaxPaginationLimit       int   // Max entries per page (0 = no limit)
    MapMinStreamPaginationLimit int   // Minimum stream page size

    // Immediate join
    MapMaxImmediateJoinStateSize int  // Max state size for single-request join

    // Recovery limits
    MapRecoveryMaxPublicationLimit int  // Max publications for catch-up

    // Optimization
    MapStateToLiveEnabled bool  // Allow STATE→LIVE direct transition
}
```

### Subscribe Options

```go
node.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
    cb(SubscribeReply{
        Options: SubscribeOptions{
            EnableMap: true,

            // Streamless mode (default) — no stream, no recovery:
            // EnablePositioning: false,
            // EnableRecovery: false,

            // Positioned mode — stream history + recovery:
            // EnablePositioning: true,
            // EnableRecovery: true,
        },
    }, nil)
})
```

- `EnablePositioning`: Enables stream offset tracking. Required for CAS and version-based dedup.
- `EnableRecovery`: Enables stream-based recovery on reconnect. Implies positioning.

The subscribe response includes `Recoverable: true` when positioning is enabled, allowing the client SDK to optimize reconnection behavior.

### Channel Options

```go
type MapChannelOptions struct {
    StreamSize int             // Max stream entries (default 0 = streamless)
    StreamTTL  time.Duration   // Stream entry lifetime (default 0 = disabled)
    MetaTTL    time.Duration   // Metadata lifetime (default 1 hour)
    KeyTTL     time.Duration   // Key auto-expiration (default 1 minute)
}
```

Default values (`DefaultMapChannelOptions()`):
- `StreamSize: 0` — no stream history (streamless)
- `StreamTTL: 0` — no stream entry TTL
- `MetaTTL: 1 hour` — metadata safety net for container-level expiration
- `KeyTTL: 1 minute` — automatic key expiration

For positioned mode, configure stream settings via the resolver:

```go
// Configure via callback
node.Config().GetMapChannelOptions = func(channel string) MapChannelOptions {
    if strings.HasPrefix(channel, "leaderboard:") {
        return MapChannelOptions{
            StreamSize: 1000,
            StreamTTL:  time.Hour,
            MetaTTL:    24 * time.Hour,
            KeyTTL:     time.Minute,
        }
    }
    return DefaultMapChannelOptions()
}
```

> **Important**: Align publish-side `MapChannelOptions` (StreamSize > 0) with subscribe-side flags (`EnablePositioning`/`EnableRecovery`). A positioned subscriber on a streamless channel will fail during subscription setup.

## Positioning in the Real-Time Ecosystem

Map Subscriptions occupy a unique position in the real-time technology landscape:

### vs. Traditional Pub/Sub

Traditional pub/sub (Redis Pub/Sub, NATS, etc.) provides fire-and-forget message delivery. Messages are lost if no subscriber is listening. Map Subscriptions add:
- State persistence (snapshot)
- Change history (stream)
- Recovery on reconnection
- Late-join support

### vs. Event Sourcing

Event sourcing systems (Kafka, EventStore) focus on durable event logs. Map Subscriptions complement these by providing:
- Real-time delivery to WebSocket clients
- Materialized views (snapshot) for instant state access
- Subscription-scoped retention (not infinite logs)

### vs. Operational Databases with Change Streams

Databases like MongoDB (Change Streams) and PostgreSQL (logical replication) provide change capture. Map Subscriptions build on this pattern with:
- WebSocket delivery to browsers/mobile
- Client-side SDK with automatic recovery
- Pub/sub fan-out for horizontal scaling
- Purpose-built consistency model

### vs. CRDTs and Distributed State

CRDT libraries (Yjs, Automerge) handle conflict-free merging of concurrent edits. Map Subscriptions provide:
- Server-authoritative state (simpler consistency model)
- CAS operations for atomic updates
- No client-side merge logic required
- Works with any data format

### When to Use Map Subscriptions

**Choose Map Subscriptions when you need**:
- Real-time updates to browser/mobile clients
- Late-join support (new clients see current state)
- Reconnection recovery (no missed updates)
- Atomic operations (CAS, conditional writes)
- Presence tracking
- Ordered data (leaderboards, feeds)

**Consider alternatives when**:
- You need infinite event retention → Event sourcing
- Offline-first with conflict resolution → CRDTs
- Server-to-server messaging only → Traditional message queues
- Simple request-response → REST/GraphQL

### The Complete Picture

Map Subscriptions bridge the gap between traditional databases and real-time delivery:

```
┌─────────────────────────────────────────────────────────────────┐
│                     Application Architecture                     │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│   ┌──────────────┐         ┌──────────────┐                     │
│   │   Primary    │         │  Map Broker  │                     │
│   │   Database   │────────>│  (Postgres)  │                     │
│   │              │  SQL    │              │                     │
│   └──────────────┘  Func   └──────────────┘                     │
│                                   │                              │
│                                   │ WAL / Pub/Sub                │
│                                   ▼                              │
│                          ┌──────────────┐                       │
│                          │  Centrifuge  │                       │
│                          │    Node      │                       │
│                          └──────────────┘                       │
│                                   │                              │
│                                   │ WebSocket                    │
│                                   ▼                              │
│   ┌──────────────┐  ┌──────────────┐  ┌──────────────┐         │
│   │   Browser    │  │    Mobile    │  │   Desktop    │         │
│   │   Client     │  │    Client    │  │   Client     │         │
│   └──────────────┘  └──────────────┘  └──────────────┘         │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

By combining snapshot state, change streams, and real-time pub/sub, Map Subscriptions provide a cohesive solution for building responsive, collaborative applications where every client stays synchronized with shared state.
