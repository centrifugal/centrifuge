# Keyed Subscriptions Demo

This demo showcases Centrifuge's map subscriptions feature with PostgreSQL as the persistent backend. It includes several interactive examples: collaborative cursors, game lobby, leaderboard, and inventory management.

## Prerequisites

- Go 1.21+
- Docker and Docker Compose (for quick start)
- PostgreSQL 14+ with logical replication enabled (for manual setup)
- Redis (optional, for multi-node fan-out)

## Quick Start with Docker Compose

The easiest way to run the demo:

```bash
# Start PostgreSQL and Redis
docker-compose up -d

# Wait for PostgreSQL to be ready
docker-compose exec postgres pg_isready -U centrifuge -d centrifuge

# Run the demo
go build -o map_demo . && ./map_demo -postgres "postgres://centrifuge:centrifuge@localhost:5432/centrifuge?sslmode=disable"
```

Open http://localhost:3000 in your browser.

## PostgreSQL Configuration

### Required Settings

PostgreSQL must be configured for logical replication. These settings are required:

```
wal_level = logical
max_replication_slots = 20   # At least num_shards + buffer
max_wal_senders = 20         # At least num_shards + buffer
```

For Docker, these are set via command arguments in `docker-compose.yml`.

For a standalone PostgreSQL installation, edit `postgresql.conf`:

```bash
# Find your postgresql.conf location
psql -U postgres -c "SHOW config_file"

# Edit and add/modify these lines
wal_level = logical
max_replication_slots = 20
max_wal_senders = 20

# Restart PostgreSQL
sudo systemctl restart postgresql
```

### Schema Setup

Initialize the database schema by running `postgres/init.sql`:

```bash
psql -U centrifuge -d centrifuge -f postgres/init.sql
```

This creates:
- `cf_keyed_stream` - Change history table with sharded logical replication
- `cf_keyed_snapshot` - Current state table
- `cf_keyed_meta` - Stream metadata (offsets, epochs)
- `cf_keyed_idempotency` - Idempotency keys for deduplication
- `cf_keyed_publish()` / `cf_keyed_unpublish()` - SQL functions for publishing
- 16 sharded publications for parallel WAL readers

### Verify Setup

Check that logical replication is properly configured:

```sql
-- Check WAL level
SHOW wal_level;  -- Should be 'logical'

-- Check publications exist
SELECT * FROM pg_publication WHERE pubname LIKE 'cf_keyed%';

-- Check replication slots (created dynamically by Centrifuge)
SELECT slot_name, plugin, slot_type, active
FROM pg_replication_slots
WHERE slot_name LIKE 'cf_keyed%';
```

## Running the Demo

### Single-Node Mode (PostgreSQL only)

```bash
./map_demo -postgres "postgres://centrifuge:centrifuge@localhost:5432/centrifuge?sslmode=disable"
```

In this mode:
- WAL reader delivers publications directly to local subscribers
- No Redis required
- Suitable for single-server deployments

### Multi-Node Mode (PostgreSQL + Redis)

```bash
./map_demo \
  -postgres "postgres://centrifuge:centrifuge@localhost:5432/centrifuge?sslmode=disable" \
  -redis "localhost:6379"
```

In this mode:
- WAL reader publishes to Redis broker for fan-out
- All Centrifuge nodes receive publications via Redis PUB/SUB
- Suitable for horizontally scaled deployments

### With Memory Cache

Add `-cache` flag for read-your-own-writes consistency:

```bash
./map_demo -postgres "..." -cache
```

## Monitoring

### Check WAL Reader Status

The WAL reader uses PostgreSQL's logical replication. Monitor it with:

```sql
-- Active replication slots and their lag
SELECT
    slot_name,
    active,
    pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS lag,
    pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS pending
FROM pg_replication_slots
WHERE slot_name LIKE 'cf_keyed%'
ORDER BY slot_name;

-- Replication connections
SELECT
    pid, usename, application_name, client_addr, state,
    pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), sent_lsn)) AS send_lag,
    pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), flush_lsn)) AS flush_lag
FROM pg_stat_replication
WHERE application_name LIKE 'cf_keyed%';
```

### Check Stream and Snapshot Data

```sql
-- Recent stream entries
SELECT channel, channel_offset, key, removed, created_at
FROM cf_keyed_stream
ORDER BY id DESC
LIMIT 20;

-- Snapshot state
SELECT channel, key, score, updated_at
FROM cf_keyed_snapshot
WHERE channel = 'leaderboard'
ORDER BY score DESC
LIMIT 10;

-- Channel metadata
SELECT channel, top_offset, epoch, updated_at
FROM cf_keyed_meta;
```

### Cleanup Stale Replication Slots

If slots become orphaned (e.g., after crashes), clean them up:

```sql
-- List inactive slots
SELECT slot_name, active, restart_lsn
FROM pg_replication_slots
WHERE NOT active AND slot_name LIKE 'cf_keyed%';

-- Drop a specific slot
SELECT pg_drop_replication_slot('cf_keyed_shard_0');

-- Drop all inactive keyed slots
SELECT pg_drop_replication_slot(slot_name)
FROM pg_replication_slots
WHERE NOT active AND slot_name LIKE 'cf_keyed%';
```

## Architecture

### Sharded WAL Reading

The demo uses 16 shards by default. Channels are assigned to shards based on hash:

```
shard = abs(hashtext(channel)) % 16
```

Each shard:
- Has its own publication (`cf_keyed_stream_shard_N`)
- Has its own replication slot (`cf_keyed_shard_N`)
- Can be claimed by any Centrifuge node via advisory locks
- Processes WAL changes independently

Benefits:
- **Multi-node scaling**: Different nodes claim different shards
- **Fault isolation**: Slow channels don't block others
- **Parallel processing**: Multiple goroutines handle different channels

### Data Flow

1. **Publish**: Client calls HTTP endpoint → `cf_keyed_publish()` SQL function
2. **WAL Capture**: PostgreSQL writes to WAL → Logical replication streams to Centrifuge
3. **Fan-out**:
   - Single-node: WAL reader delivers directly to subscribers
   - Multi-node: WAL reader publishes to Redis → All nodes receive via PUB/SUB
4. **Delivery**: Subscribers receive real-time updates

## Troubleshooting

### "max_replication_slots" exceeded

Increase `max_replication_slots` in PostgreSQL config and restart.

### WAL reader not claiming shards

Check if another process holds the advisory locks:

```sql
SELECT classid, objid, mode, granted, pid
FROM pg_locks
WHERE locktype = 'advisory' AND classid = 1735289160;
```

### Publications not delivered (multi-node mode)

1. Ensure Redis is running and accessible
2. Check that both `-postgres` and `-redis` flags are provided
3. Verify broker subscription in Redis:

```bash
redis-cli PUBSUB CHANNELS "map_demo*"
```

### High replication lag

```sql
-- Check which slot is lagging
SELECT slot_name,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS lag
FROM pg_replication_slots
WHERE slot_name LIKE 'cf_keyed%'
ORDER BY pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) DESC;
```

Possible causes:
- Network latency to PostgreSQL
- Slow publication processing
- Too many channels on one shard

## Configuration Reference

### Command-Line Flags

| Flag        | Description                          | Default |
|-------------|--------------------------------------|---------|
| `-port`     | HTTP server port                     | `3000`  |
| `-postgres` | PostgreSQL connection string         | (none)  |
| `-redis`    | Redis address for multi-node fan-out | (none)  |
| `-cache`    | Enable memory cache layer            | `false` |

### PostgreSQL Engine Options

When using `PostgresMapEngineConfig`:

| Option               | Description                          | Default    |
|----------------------|--------------------------------------|------------|
| `ConnString`         | PostgreSQL connection string         | (required) |
| `NumShards`          | Number of WAL reader shards          | `16`       |
| `ShardIDs`           | Specific shards to claim (nil = all) | `nil`      |
| `WALReaderHeartbeat` | Standby status interval              | `10s`      |
| `Broker`             | Redis broker for multi-node fan-out  | `nil`      |

## License

Same as Centrifuge library.
