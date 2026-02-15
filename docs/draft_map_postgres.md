# PostgreSQL MapBroker Architecture

## Overview

`PostgresMapBroker` is a `MapBroker` implementation backed by PostgreSQL. All writes go through SQL functions (`cf_map_publish`, `cf_map_remove`) that atomically update state and append to a stream table within a single transaction. A background worker detects new stream entries and delivers them to subscribers — either locally (single-node) or via a broker like Redis (multi-node).

Two delivery modes are supported:

1. **Outbox (default)** — cursor-based polling of `cf_map_stream`. No special PG setup.
2. **WAL (opt-in)** — logical replication streaming. Requires `wal_level = logical`.

Both modes use the same tables. The only difference is how workers discover new rows.

## Tables

All tables use prefix `cf_map_` (JSONB mode) or `cf_binary_map_` (BYTEA mode). Schema is created by `EnsureSchema()` and is fully idempotent.

### cf_map_stream — Change log

Append-only log. Every publish and remove appends a row. Workers read from here.

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
| `expires_at` | TIMESTAMPTZ | Row TTL (for stream cleanup) |
| `created_at` | TIMESTAMPTZ | Insert time |

Key indexes: `(channel, channel_offset)`, `(channel, id DESC)`, `(shard_id, id)` for cursor polling, `(expires_at)` for cleanup.

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

### cf_map_outbox_cursor — Delivery progress (outbox mode only)

One row per shard. Tracks the last delivered `stream.id` for each shard.

| Column | Type |
|--------|------|
| `shard_id` | INTEGER PK |
| `last_processed_id` | BIGINT |

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
10. **Trim stream** — if `p_stream_size` set, delete old entries (cursor-aware: only rows already delivered)
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

### cf_map_expire_keys

Called by the TTL worker. Finds keys where `expires_at <= NOW()`, removes them, and emits removal events to the stream. Lock ordering matches publish (meta `FOR UPDATE` first, then state `FOR UPDATE SKIP LOCKED`) to prevent deadlocks.

## Delivery

### Sharding

Every stream row gets `shard_id = abs(hashtext(channel)) % NumShards`. Workers claim individual shards, so all events for a channel are processed in order by a single worker.

### Advisory Locks

Both delivery modes use `pg_try_advisory_lock(baseID + shardID)` for leader election. Only one worker (across all nodes) processes each shard at a time. If a worker dies, the lock is released and another worker picks up the shard.

Outbox and WAL use different `baseID` values (72616653 vs 72616654) to avoid conflicts when switching modes.

### Outbox Mode

Each worker:
1. Acquires advisory lock for its shard (non-blocking, retries with backoff)
2. UPSERTs a cursor row in `cf_map_outbox_cursor`
3. Polls: `SELECT ... FROM cf_map_stream WHERE shard_id = $1 AND id > $2 ORDER BY id LIMIT $3`
4. Delivers each entry (see below)
5. Advances cursor: `UPDATE cf_map_outbox_cursor SET last_processed_id = $1`
6. If no rows, sleeps for `PollInterval` (default 50ms)

The worker holds its advisory lock connection for the entire lifetime. This is why `PoolSize` must exceed `NumShards`.

### WAL Mode

`EnsureSchema()` creates per-shard publications:
```sql
CREATE PUBLICATION cf_map_stream_shard_0
  FOR TABLE cf_map_stream WHERE (shard_id = 0)
  WITH (publish = 'insert')
```

Each WAL reader:
1. Acquires advisory lock for its shard
2. Opens a replication connection (`replication=database`)
3. Creates or reuses a replication slot (`cf_map_shard_N`)
4. Starts streaming: `START_REPLICATION SLOT ... PUBLICATION ...`
5. Processes `InsertMessage` WAL events, parses column data
6. Sends standby heartbeats every `HeartbeatInterval`

### Single-Node vs Multi-Node

- **Single-node (no Broker configured):** worker calls `eventHandler.HandlePublication()` directly
- **Multi-node (Broker configured):** worker calls `broker.Publish()` which fans out to all nodes via pub/sub (e.g., Redis)

```
Publish → cf_map_stream → Worker → eventHandler.HandlePublication  (single-node)
Publish → cf_map_stream → Worker → broker.Publish → All Nodes      (multi-node)
```

## Background Workers

### TTL Expiration Worker

Runs every `TTLCheckInterval` (default 1s). Queries for channels with expired keys, calls `cf_map_expire_keys()`, and delivers the resulting removal events.

### Stream Cleanup Worker

Runs every `CleanupInterval` (default 1m). Four cleanup paths:

1. **Expired stream entries** — `DELETE ... WHERE expires_at < NOW()` but only rows already delivered (`id <= last_processed_id`)
2. **Orphaned stream entries** — entries from channels with no meta row, already delivered, no explicit TTL
3. **Expired meta** — `DELETE FROM cf_map_meta WHERE expires_at < NOW()`
4. **Expired idempotency keys** — `DELETE FROM cf_map_idempotency WHERE expires_at < NOW()`

All stream deletions are cursor-aware: in outbox mode, only rows below the shard's cursor are deleted. In WAL mode (no cursor rows), all expired entries are deleted freely.

## Configuration

```go
PostgresMapBrokerConfig{
    ConnString:          "postgres://...",
    PoolSize:            32,              // default; must be > NumShards in outbox mode
    NumShards:           16,              // default
    TTLCheckInterval:    time.Second,     // default
    CleanupInterval:     time.Minute,     // default
    IdempotentResultTTL: 5 * time.Minute, // default
    BinaryData:          false,           // true → BYTEA columns, "cf_binary_map_" prefix

    Outbox: OutboxConfig{
        PollInterval:       50 * time.Millisecond,
        BatchSize:          1000,
        AdvisoryLockBaseID: 72616653,     // default
    },

    // OR (mutually exclusive with Outbox usage):
    WAL: WALConfig{
        Enabled:            true,
        HeartbeatInterval:  10 * time.Second,
        AdvisoryLockBaseID: 72616654,     // default
        ShardIDs:           nil,          // nil = claim all shards
    },

    Broker: redisBroker, // nil for single-node
}
```

## Data Modes

Two table sets can coexist in the same database:

| Mode | Prefix | Data columns | Use case |
|------|--------|-------------|----------|
| JSONB (default) | `cf_map_` | `JSONB` | JSON payloads, enables PG JSON queries |
| Binary | `cf_binary_map_` | `BYTEA` | Protobuf / binary payloads |

Set `BinaryData: true` in config to use binary mode. Schema template generates both variants via `make pg-schemas`.

## Read Path

### ReadState

Reads current snapshot from `cf_map_state`. Uses `REPEATABLE READ` isolation. Supports:
- Single key lookup
- Paginated reads with keyset pagination (no OFFSET)
- Ordered channels (by `score DESC, key`)
- Filters expired keys (`expires_at IS NULL OR expires_at > NOW()`)

### ReadStream

Reads change history from `cf_map_stream`. Uses `REPEATABLE READ` isolation. Supports:
- Filter by `Since` position (channel_offset-based)
- Forward and reverse order
- Pagination with limit
