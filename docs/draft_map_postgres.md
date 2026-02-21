# PostgreSQL MapBroker Architecture

## Overview

`PostgresMapBroker` is a `MapBroker` implementation backed by PostgreSQL. All writes go through SQL functions (`cf_map_publish`, `cf_map_remove`) that atomically update state and append to a stream table within a single transaction.

Every node independently polls the stream table for new entries — SQL SELECT is reliable, unlike lossy pub/sub mechanisms. This eliminates the need for advisory locks, cursor persistence, shard claiming, or an external broker (Redis) for multi-node fan-out. Optionally, PostgreSQL `LISTEN/NOTIFY` can be used for low-latency wakeup of outbox workers.

## Tables

All tables use prefix `cf_map_` (JSONB mode) or `cf_binary_map_` (BYTEA mode). Schema is created by `EnsureSchema()` and is fully idempotent.

### cf_map_stream — Change log

Append-only log. Every publish and remove appends a row. Outbox workers poll this table.

| Column | Type | Purpose |
|--------|------|---------|
| `id` | BIGSERIAL PK | Global monotonic ID for cursor-based polling |
| `channel` | TEXT | Channel name |
| `channel_offset` | BIGINT | Per-channel sequence number (Centrifuge offset) |
| `epoch` | TEXT | Channel epoch |
| `key` | TEXT | Map key |
| `data` | JSONB/BYTEA | Payload (NULL for removals) |
| `tags` | JSONB | Optional key-value tags |
| `removed` | BOOLEAN | TRUE for removal events |
| `score` | BIGINT | Sort score (nullable) |
| `previous_data` | JSONB/BYTEA | Previous value for delta compression |
| `shard_id` | SMALLINT | `abs(hashtext(channel)) % num_shards` |
| `client_id`, `user_id` | TEXT | Client metadata |
| `conn_info`, `chan_info` | JSONB/BYTEA | Connection/channel info |
| `subscribed_at` | TIMESTAMPTZ | Subscription timestamp |
| `created_at` | TIMESTAMPTZ | Insert time |

Key indexes: `(channel, channel_offset)`, `(channel, id DESC)`, `(shard_id, id)` for outbox polling, `(created_at)` for time-based cleanup.

### cf_map_state — Current snapshot

One row per (channel, key). Updated on every publish, deleted on remove.

| Column | Type | Purpose |
|--------|------|---------|
| `channel`, `key` | TEXT | Composite PK |
| `data` | JSONB/BYTEA | Current value |
| `tags` | JSONB | Current tags |
| `score` | BIGINT | Sort score for ordered reads |
| `key_offset` | BIGINT | Last channel_offset that touched this key |
| `key_version`, `key_version_epoch` | BIGINT, TEXT | Key-level versioning |
| `expires_at` | TIMESTAMPTZ | Key TTL |
| `client_id`, `user_id`, `conn_info`, `chan_info`, `subscribed_at` | — | Client metadata |
| `created_at`, `updated_at` | TIMESTAMPTZ | Timestamps |

Indexes: `(channel, score DESC, key)` for ordered reads, `(expires_at)` for TTL worker.

### cf_map_meta — Channel metadata

One row per channel. Holds the current top offset and epoch.

| Column | Type | Purpose |
|--------|------|---------|
| `channel` | TEXT PK | Channel name |
| `top_offset` | BIGINT | Current max offset (incremented on each publish/remove) |
| `epoch` | TEXT | Random epoch generated on first publish |
| `version`, `version_epoch` | BIGINT, TEXT | Stream-level version for optimistic concurrency |
| `expires_at` | TIMESTAMPTZ | Meta TTL (for channel cleanup) |

**Note:** The meta row is created lazily on the first publish (by `cf_map_publish`). Read operations (ReadState, ReadStream) do **not** create meta rows — they return zero-value results `(offset=0, epoch="")` when no meta exists. See [Empty Epoch Handling](#empty-epoch-handling) for details.

### cf_map_idempotency — Deduplication

Prevents duplicate publishes within a TTL window.

| Column | Type |
|--------|------|
| `channel`, `idempotency_key` | Composite PK |
| `result_offset`, `result_id` | BIGINT |
| `expires_at` | TIMESTAMPTZ |

## SQL Functions

### cf_map_publish

Atomic publish within a single SQL call. Steps:

1. **Get or create meta** — UPSERT into `cf_map_meta`, lock row `FOR UPDATE`
2. **Idempotency check** — if `p_idempotency_key` provided, check `cf_map_idempotency`
3. **CAS check** — if `p_expected_offset` set, verify `state.key_offset` matches
4. **Key mode check** — `'if_new'` (suppress if key exists) or `'if_exists'` (suppress if key missing)
5. **Version check** — stream-level optimistic concurrency via `p_version`/`p_version_epoch`
6. **Increment offset** — `top_offset + 1` in meta
7. **Fetch previous data** — for delta compression (if `p_use_delta`)
8. **Upsert state** — INSERT ... ON CONFLICT DO UPDATE into `cf_map_state`
9. **Append to stream** — INSERT into `cf_map_stream` with `shard_id`
10. **Notify** — `PERFORM pg_notify(...)` to wake outbox workers (when `UseNotify` enabled)
11. **Save idempotency** — if key provided

Returns: `(result_id, channel_offset, epoch, suppressed, suppress_reason, current_data, current_offset)`.

Suppression reasons: `idempotency`, `key_exists`, `key_not_found`, `position_mismatch`, `version`.

### cf_map_publish_strict

Wrapper around `cf_map_publish` that raises PostgreSQL exceptions on suppression instead of returning suppress flags. Uses standard error codes (`unique_violation`, `no_data_found`, `serialization_failure`). This causes the transaction to roll back automatically.

### cf_map_remove

Removes a key. Similar flow to publish but simpler:
1. Get meta (error if channel doesn't exist)
2. Idempotency check
3. CAS check (`p_expected_offset`)
4. Check key exists in state
5. Increment offset, delete from state, append removal to stream
6. Notify outbox workers

### cf_map_expire_keys

Called by the TTL worker. Finds keys where `expires_at <= NOW()`, removes them, and emits removal events to the stream. Lock ordering matches publish (meta `FOR UPDATE` first, then state `FOR UPDATE SKIP LOCKED`) to prevent deadlocks. Notifies outbox workers after each removal.

## Delivery

### Sharding

Every stream row gets `shard_id = abs(hashtext(channel)) % NumShards`. This determines which outbox worker (when using replicas) handles the entry.

### Outbox Worker

Every node runs outbox workers that independently poll the stream table. No leader election or advisory locks are needed — every node sees every event.

**Without replicas (default):** one outbox worker per node, polling all shards:
```sql
SELECT ... FROM cf_map_stream WHERE id > $1 ORDER BY id LIMIT $2
```

**With replicas:** one outbox worker per replica, each filtering by assigned shards:
```sql
SELECT ... FROM cf_map_stream WHERE id > $1 AND shard_id = ANY($2) ORDER BY id LIMIT $3
```

Worker lifecycle:
1. Startup: `SELECT COALESCE(MAX(id), 0) FROM cf_map_stream` → initial in-memory cursor
2. Main loop: waits on `LISTEN/NOTIFY` notification (if `UseNotify=true`) OR poll interval ticker
3. Inner loop: fetch batch → deliver via `eventHandler.HandlePublication()` → advance cursor → repeat until batch < BatchSize

No persistent cursor, no advisory locks.

### LISTEN/NOTIFY (optional)

When `UseNotify=true`, a dedicated goroutine runs `LISTEN {prefix}stream_notify` on the primary. On notification, it wakes the outbox workers for immediate processing. This reduces delivery latency from `PollInterval` (default 50ms) to near-zero when the system is idle.

Falls back to poll interval on notification listener failure (auto-reconnect with backoff).

## Background Workers

### TTL Expiration Worker

Runs every `TTLCheckInterval` (default 1s). Queries for channels with expired keys, calls `cf_map_expire_keys()`, and delivers the resulting removal events.

### Cleanup Worker

Runs every `CleanupInterval` (default 1m). Time-based cleanup:

1. **Stream entries** — `DELETE FROM cf_map_stream WHERE created_at < NOW() - $retention`
2. **Expired meta** — `DELETE FROM cf_map_meta WHERE expires_at IS NOT NULL AND expires_at < NOW()`
3. **Expired idempotency keys** — `DELETE FROM cf_map_idempotency WHERE expires_at < NOW()`

### Partition Worker (optional)

When `Partitioning=true`, manages daily partitions of the stream table:
1. **Ensure lookahead partitions** — create partitions for the next N days
2. **Drop old partitions** — `DROP TABLE` for partitions older than `PartitionRetentionDays`

This is purely an optimization — `DROP TABLE` is instant vs `DELETE + VACUUM`.

## Read Path

### Pure Reads (No Writes)

Read operations (ReadState, ReadStream) are pure reads — they never write to the database. When a channel has no meta row (never published to), reads return zero-value results: `offset=0, epoch=""`. This is critical for replica compatibility — reads can be routed to read replicas without risking write conflicts.

### ReadState

Reads current snapshot from `cf_map_state`. Uses pgx Batch pipelining to execute `BEGIN REPEATABLE READ` + meta query + data query + `COMMIT` in a single TCP round trip. Supports:
- Single key lookup (CAS reads)
- Paginated reads with keyset pagination (no OFFSET)
- Ordered channels (by `score DESC, key`)
- Filters expired keys (`expires_at IS NULL OR expires_at > NOW()`)

### ReadStream

Reads change history from `cf_map_stream`. Uses pgx Batch pipelining with `REPEATABLE READ` isolation. Supports:
- Filter by `Since` position (channel_offset-based)
- Forward and reverse order
- Pagination with limit

## Read Replica Support

### Configuration

```go
PostgresMapBrokerConfig{
    ReadReplicaConnStrings: []string{"postgres://replica1:5432/db", "postgres://replica2:5432/db"},
    ReadReplicaPoolSize:    16,
}
```

### Routing

Reads route to replicas **only when `AllowCached=true`** in read options — same semantics as `CachedMapBroker`. Without `AllowCached`, reads always go to the primary for strong consistency.

```go
func (e *PostgresMapBroker) getReadPool(channel string, allowCached bool) *pgxpool.Pool {
    if !allowCached || len(e.readPools) == 0 {
        return e.pool  // primary
    }
    shardID := abs(hashtext(channel)) % e.conf.NumShards
    replicaIdx := shardID % len(e.readPools)
    return e.readPools[replicaIdx]
}
```

Shard-based routing ensures that outbox worker and client reads for the same channel always hit the same replica, providing a consistent view.

### What uses AllowCached

| Operation | AllowCached | Why |
|-----------|-------------|-----|
| Client ReadState (subscription) | true | State reads during subscribe |
| Client ReadStream (subscription) | true | Stream catch-up during subscribe |
| Position checks (`ReadStream(Limit:0)`) | false | Must see latest position from primary |
| `CachedMapBroker` internal reads | false | Gap-fill and sync always hit primary |

### Outbox Workers with Replicas

With N replicas, N outbox workers run — one per replica. Each worker handles shards where `shard_id % N == workerIdx`. This ensures the outbox worker and `AllowCached` reads for the same channel hit the same replica.

## Empty Epoch Handling

The PostgreSQL broker returns `epoch=""` for channels that have never been published to (no meta row exists). This is different from the MemoryMapBroker which always creates an epoch, even for empty channels. The empty epoch requires special handling in the client protocol.

### Problem

When a client subscribes to a channel with no data, it receives `epoch=""`. When the first publication arrives (via outbox), it carries a real epoch (created by `cf_map_publish`). Without special handling, this epoch mismatch triggers `handleInsufficientState`, causing a needless re-subscribe.

### Solution: Epoch Adoption (Map Channels Only)

In the publication delivery path (`client.go`), when a **map channel** client has `epoch=""` and receives a publication with a real epoch, the client **adopts the real epoch** instead of triggering insufficient state:

```go
if pubEpoch != channelContext.streamPosition.Epoch {
    if channelContext.streamPosition.Epoch == "" && channelHasFlag(channelContext.flags, flagMap) {
        // Adopt the real epoch — safe because epoch="" means "no data existed at subscribe time"
        channelContext.streamPosition.Epoch = pubEpoch
        c.channels[ch] = channelContext
    } else {
        // Real epoch mismatch — insufficient state
        go func() { c.handleInsufficientState(ch, serverSide) }()
    }
}
```

This is **map-specific** — non-map (regular) channels with `epoch=""` still trigger insufficient state on epoch mismatch. The `flagMap` guard ensures the behavior only applies to map subscriptions.

### Recovery Guard for Empty Epoch

The client SDK stores the epoch from the subscribe response and never updates it from publication deliveries. When the client reconnects after a disconnect, it sends the original `epoch=""` in the recovery request — even though the server-side epoch was adopted.

This creates a correctness problem: if a `Clear` event happened between subscribe and reconnect, the channel gets a new epoch. A recovery request with `epoch=""` would bypass epoch validation (since `epoch=""` matches nothing) and could return stale state.

The recovery guard in `handleMapTransitionToLive` prevents this:

```go
if params.isRecovery && params.sincePosition.Epoch == "" && streamPos.Epoch != "" {
    return ErrorUnrecoverablePosition
}
```

When recovery fails with `ErrorUnrecoverablePosition`, the client SDK performs a fresh re-subscribe from scratch, which correctly gets the new state.

### Scenario Walkthrough

1. **Subscribe to empty channel** → client gets `epoch=""`, offset=0
2. **First publish** → outbox delivers with `epoch="abc"` → client adopts epoch
3. **More publishes** → normal delivery, offsets advance
4. **Client disconnects and reconnects** → sends `Recover=true, epoch="", offset=N`
5. **Server has `epoch="abc"`** → `sinceEpoch="" && serverEpoch!=""` → `ErrorUnrecoverablePosition`
6. **Client re-subscribes from scratch** → gets full state with `epoch="abc"`

After step 6, the client has a real epoch and future recoveries work normally.

### Edge Case: Clear Between Subscribe and Reconnect

1. Subscribe to empty channel → `epoch=""`
2. Publish data → epoch="abc" adopted by client
3. Clear channel → epoch changes to "xyz"
4. Publish new data
5. Client reconnects with `epoch=""` → guard fires → `ErrorUnrecoverablePosition`
6. Client re-subscribes → gets correct new state

Without the guard, step 5 would succeed with stale state (pre-Clear data missing).

## Position Checks

Position checks (`ReadStream(Limit:0)`) run periodically (every ~40s) to detect missed publications from the outbox. This is important for the PG broker because BIGSERIAL id gaps can cause outbox polling to miss entries: if TX1 gets id=100 and TX2 gets id=101, but TX2 commits first, the outbox cursor advances past 100 before TX1 commits.

Position checks always use the primary connection (never `AllowCached`) to see the latest stream position. When a position mismatch is detected, it triggers `handleInsufficientState` which causes the client to re-subscribe and recover.

## Configuration

```go
PostgresMapBrokerConfig{
    ConnString:          "postgres://...",
    PoolSize:            32,              // default
    NumShards:           16,              // default
    TTLCheckInterval:    time.Second,     // default
    CleanupInterval:     time.Minute,     // default
    IdempotentResultTTL: 5 * time.Minute, // default
    BinaryData:          false,           // true → BYTEA columns, "cf_binary_map_" prefix

    StreamRetention:     24 * time.Hour,  // how long stream entries are kept
    UseNotify:           false,           // LISTEN/NOTIFY for low-latency wakeup

    Outbox: OutboxConfig{
        PollInterval: 50 * time.Millisecond,
        BatchSize:    1000,
    },

    // Optional: read replica support
    ReadReplicaConnStrings: []string{"postgres://replica1/db"},
    ReadReplicaPoolSize:    16,

    // Optional: stream table partitioning
    Partitioning:           false,
    PartitionRetentionDays: 3,
    PartitionLookaheadDays: 2,
}
```

## Data Modes

Two table sets can coexist in the same database:

| Mode | Prefix | Data columns | Use case |
|------|--------|-------------|----------|
| JSONB (default) | `cf_map_` | `JSONB` | JSON payloads, enables PG JSON queries |
| Binary | `cf_binary_map_` | `BYTEA` | Protobuf / binary payloads |

Set `BinaryData: true` in config to use binary mode. Schema template generates both variants via `make pg-schemas`.
