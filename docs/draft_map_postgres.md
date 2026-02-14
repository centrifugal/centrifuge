# PostgreSQL MapBroker Architecture

## Overview

The PostgreSQL MapBroker provides durable, transactional map state with real-time delivery. It supports two delivery modes:

1. **Outbox Mode (Default)** - Simple setup, works with any PostgreSQL
2. **WAL Mode (Opt-in)** - Uses logical replication, requires PostgreSQL configuration

Both modes read from the same `cf_map_stream` table. The difference is how workers detect new entries: outbox mode polls using cursors, WAL mode streams via logical replication.

## Delivery Modes

### Outbox Mode (Default)

Outbox mode uses cursor-based polling of `cf_map_stream` for new publications. Per-shard delivery progress is tracked in `cf_map_outbox_cursor`. This mode requires no special PostgreSQL setup.

**How it works:**
1. `Publish`/`Remove` inserts into `cf_map_stream` atomically (single table)
2. Outbox workers poll their assigned shards: `WHERE shard_id = $1 AND id > cursor`
3. Workers deliver publications via broker (multi-node) or locally (single-node)
4. Workers advance cursor in `cf_map_outbox_cursor` after processing each batch

**Configuration:**
```go
PostgresMapBrokerConfig{
    ConnString: "postgres://...",
    PoolSize:   32,  // Must be > NumShards
    NumShards:  16,  // Parallel workers (default: 16)
    Outbox: OutboxConfig{
        PollInterval: 50*time.Millisecond,
        BatchSize:    1000,
    },
}
```

**Important:** Each outbox worker holds a database connection for its advisory lock. `PoolSize` must be greater than `NumShards` to leave connections available for queries.

### WAL Mode (Opt-in)

WAL mode uses PostgreSQL logical replication to stream changes. This requires:
- `wal_level = logical` in postgresql.conf
- Publications created for each shard
- Replication slots (created automatically)

**Configuration:**
```go
PostgresMapBrokerConfig{
    ConnString: "postgres://...",
    NumShards:  16,  // Must match number of publications
    WAL: WALConfig{
        Enabled:           true,
        HeartbeatInterval: 10*time.Second,
    },
}
```

When `WAL.Enabled = true`:
- WAL readers stream INSERTs from logical replication on `cf_map_stream`
- Lower latency but requires PostgreSQL setup

## Database Schema

### Core Tables

```sql
-- Stream: Change history + delivery source (used by both outbox and WAL modes)
cf_map_stream (id, channel, channel_offset, epoch, key, data, tags, removed, score, shard_id, ...)

-- Outbox Cursor: Per-shard delivery progress (outbox mode)
cf_map_outbox_cursor (shard_id, last_processed_id, updated_at)

-- State: Current snapshot
cf_map_state (channel, key, data, tags, score, key_offset, expires_at, ...)

-- Meta: Channel metadata
cf_map_meta (channel, top_offset, epoch, version, ...)

-- Idempotency: Deduplication
cf_map_idempotency (channel, idempotency_key, result_offset, expires_at)
```

### Sharding

Channels are distributed across shards using: `shard_id = abs(hashtext(channel)) % num_shards`

This enables:
- Parallel processing across workers
- Ordering guarantees within a channel
- Distributed lock coordination via advisory locks

## SQL Functions

### cf_map_publish

Main publishing function with parameters:
- `p_channel`, `p_key`, `p_data` - Core publication data
- `p_tags`, `p_score` - Optional metadata
- `p_key_mode` - `'if_new'` or `'if_exists'` for conditional publish
- `p_expected_offset` - CAS (Compare-And-Swap) support
- `p_num_shards` - Shard count for shard_id calculation

### cf_map_remove

Removes a key from state and emits removal event to stream.
- `p_expected_offset` - CAS support (optional)
- Returns `current_data` and `current_offset` on position mismatch

## Advisory Lock Pattern

Both outbox and WAL workers use PostgreSQL advisory locks for shard coordination:

```go
// Try to claim shard
SELECT pg_try_advisory_lock(baseID + shardID)

// If acquired, hold connection and process
// On exit, release lock
SELECT pg_advisory_unlock(baseID + shardID)
```

This ensures:
- Only one worker processes each shard at a time
- Automatic failover if a worker dies (lock released)
- No duplicate processing across nodes

## Cursor-Based Delivery

Outbox workers use a cursor pattern instead of deleting/marking processed rows:

1. **Initialize**: `INSERT INTO cf_map_outbox_cursor (shard_id, last_processed_id) VALUES ($1, 0) ON CONFLICT DO NOTHING`
2. **Poll**: `SELECT ... FROM cf_map_stream WHERE shard_id = $1 AND id > $2 ORDER BY id LIMIT $3`
3. **Advance**: `UPDATE cf_map_outbox_cursor SET last_processed_id = $1 WHERE shard_id = $2`

Benefits:
- No row deletion by workers — cursor advancement is the progress marker
- Stream entries persist for history/recovery regardless of delivery status
- Simpler error recovery — just re-read from cursor position

### Cursor-Aware Cleanup

All cleanup paths respect delivery cursors to prevent deleting undelivered entries:

- **Stream size trimming** (in `cf_map_publish`): only trims entries below `MIN(last_processed_id)` from all cursors
- **TTL-based cleanup**: only deletes expired entries below the minimum cursor
- **Delivered entry cleanup**: removes old delivered entries with no retention need (streamless channels) after a grace period
- **WAL mode**: when no cursor rows exist, all cleanup paths operate freely

## Message Delivery

### Single-Node (No Broker)

```
Publish -> cf_map_stream -> Outbox Worker -> eventHandler.HandlePublication
```

### Multi-Node (With Broker)

```
Publish -> cf_map_stream -> Outbox Worker -> broker.Publish -> All Nodes
```

The broker (e.g., Redis) fans out publications to all subscribed nodes.

## Configuration Defaults

| Setting | Default | Description |
|---------|---------|-------------|
| `PoolSize` | 32 | Max database connections |
| `NumShards` | 16 | Parallel workers (both modes) |
| `Outbox.PollInterval` | 50ms | Polling frequency when idle |
| `Outbox.BatchSize` | 1000 | Max rows per batch |
| `WAL.HeartbeatInterval` | 10s | Replication heartbeat |

## Performance Characteristics

Typical latencies (measured on Apple M4):
- **Publish only**: ~140 microseconds
- **End-to-end delivery**: ~180 microseconds

Throughput scales with `NumShards` but each shard requires a connection.

## Error Handling

- **Transient errors**: Workers retry with exponential backoff
- **Lock contention**: Workers sleep and retry acquiring locks
- **Processing errors**: Logged but don't block other entries in batch

## Migration from WAL to Outbox

If switching from WAL mode to outbox mode:
1. Stop all nodes
2. Ensure `cf_map_outbox_cursor` table exists
3. Update config to remove `WAL.Enabled = true`
4. Restart nodes

Pending stream entries will be picked up by outbox workers from cursor position 0.

## Best Practices

1. **Set PoolSize appropriately**: `PoolSize > NumShards + expected concurrent queries`
2. **Use fewer shards for low volume**: Reduces connection overhead
3. **Monitor delivery lag**: `SELECT shard_id, last_processed_id FROM cf_map_outbox_cursor` compared to `SELECT MAX(id) FROM cf_map_stream`
4. **Use broker for multi-node**: Required for consistent delivery across nodes
