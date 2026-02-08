# PostgreSQL MapBroker Architecture

## Overview

The PostgreSQL MapBroker provides durable, transactional map state with real-time delivery. It supports two delivery modes:

1. **Outbox Mode (Default)** - Simple setup, works with any PostgreSQL
2. **WAL Mode (Opt-in)** - Uses logical replication, requires PostgreSQL configuration

## Delivery Modes

### Outbox Mode (Default)

Outbox mode polls the `cf_map_outbox` table for new publications. This mode requires no special PostgreSQL setup.

**How it works:**
1. `Publish`/`Remove` inserts into `cf_map_stream` and `cf_map_outbox` atomically
2. Outbox workers poll their assigned shards using `FOR UPDATE SKIP LOCKED`
3. Workers deliver publications via broker (multi-node) or locally (single-node)
4. Processed rows are deleted (or marked if `MarkProcessed` is enabled)

**Configuration:**
```go
PostgresMapBrokerConfig{
    ConnString: "postgres://...",
    PoolSize:   32,  // Must be > Outbox.NumShards
    Outbox: OutboxConfig{
        NumShards:       16,              // Parallel workers (default: 16)
        PollInterval:    50*time.Millisecond,
        BatchSize:       1000,
        MarkProcessed:   false,           // If true, mark rows instead of delete
        CleanupInterval: time.Hour,       // For mark-processed mode
        CleanupAge:      time.Hour,
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
    WAL: WALConfig{
        Enabled:           true,
        NumShards:         16,
        HeartbeatInterval: 10*time.Second,
    },
}
```

When `WAL.Enabled = true`:
- Outbox inserts are skipped (`p_skip_outbox = true`)
- WAL readers stream changes from logical replication
- Lower latency but requires PostgreSQL setup

## Database Schema

### Core Tables

```sql
-- Stream: Change history for recovery
cf_map_stream (id, channel, channel_offset, epoch, key, data, tags, removed, score, shard_id, ...)

-- State: Current snapshot
cf_map_state (channel, key, data, tags, score, key_offset, expires_at, ...)

-- Meta: Channel metadata
cf_map_meta (channel, top_offset, epoch, version, ...)

-- Outbox: Pending deliveries (outbox mode only)
cf_map_outbox (id, shard_id, channel, channel_offset, epoch, key, data, removed, processed_at, ...)

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
- `p_skip_outbox` - Skip outbox insert (for WAL mode)
- `p_num_shards` - Shard count for shard_id calculation

### cf_map_remove

Removes a key from state and emits removal event to stream.

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

## Message Delivery

### Single-Node (No Broker)

```
Publish -> cf_map_outbox -> Outbox Worker -> eventHandler.HandlePublication
```

### Multi-Node (With Broker)

```
Publish -> cf_map_outbox -> Outbox Worker -> broker.Publish -> All Nodes
```

The broker (e.g., Redis) fans out publications to all subscribed nodes.

## Configuration Defaults

| Setting | Default | Description |
|---------|---------|-------------|
| `PoolSize` | 32 | Max database connections |
| `Outbox.NumShards` | 16 | Parallel outbox workers |
| `Outbox.PollInterval` | 50ms | Polling frequency when idle |
| `Outbox.BatchSize` | 1000 | Max rows per batch |
| `WAL.NumShards` | 16 | Parallel WAL readers |
| `WAL.HeartbeatInterval` | 10s | Replication heartbeat |

## Performance Characteristics

Typical latencies (measured on Apple M4):
- **Publish only**: ~140 microseconds
- **End-to-end delivery**: ~180 microseconds

Throughput scales with `NumShards` but each shard requires a connection.

## Mark-Processed Mode

For high-volume systems, enable `MarkProcessed` to support table partitioning:

```go
Outbox: OutboxConfig{
    MarkProcessed:   true,
    CleanupInterval: time.Hour,
    CleanupAge:      time.Hour,
}
```

Instead of deleting processed rows, they're marked with `processed_at` timestamp. Old partitions can be dropped periodically for efficient cleanup.

## Error Handling

- **Transient errors**: Workers retry with exponential backoff
- **Lock contention**: Workers sleep and retry acquiring locks
- **Processing errors**: Logged but don't block other entries in batch

## Migration from WAL to Outbox

If switching from WAL mode to outbox mode:
1. Stop all nodes
2. Ensure `cf_map_outbox` table exists
3. Update config to remove `WAL.Enabled = true`
4. Restart nodes

Pending stream entries won't be redelivered - only new publishes go through outbox.

## Best Practices

1. **Set PoolSize appropriately**: `PoolSize > NumShards + expected concurrent queries`
2. **Use fewer shards for low volume**: Reduces connection overhead
3. **Enable MarkProcessed for high volume**: Allows efficient partition-based cleanup
4. **Monitor outbox lag**: `SELECT COUNT(*) FROM cf_map_outbox WHERE processed_at IS NULL`
5. **Use broker for multi-node**: Required for consistent delivery across nodes
