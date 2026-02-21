# Map Subscriptions Demo

This demo showcases Centrifuge's map subscriptions feature with multiple backend options. It includes several interactive examples: collaborative cursors, game lobby, leaderboard, inventory management, live polls, scoreboard, and more.

## Prerequisites

- Go 1.21+
- Docker and Docker Compose (for quick start)
- PostgreSQL 14+ (for persistent backend)
- Redis (optional, alternative backend)

## Quick Start with Docker Compose

```bash
# Start PostgreSQL (and Redis if needed)
docker-compose up -d

# Wait for PostgreSQL to be ready
docker-compose exec postgres pg_isready -U centrifuge -d centrifuge

# Run the demo
go build -o map_demo . && ./map_demo -postgres "postgres://centrifuge:centrifuge@localhost:5432/centrifuge?sslmode=disable"
```

Open http://localhost:3000 in your browser.

## PostgreSQL Configuration

No special PostgreSQL configuration is required. The broker uses standard SQL polling — no logical replication, no WAL-level settings, no replication slots needed.

### Schema Setup

The schema is created automatically via `EnsureSchema()` on application startup — no manual SQL scripts needed.

### Verify Data

```sql
-- Recent stream entries
SELECT channel, channel_offset, key, removed, created_at
FROM cf_map_stream
ORDER BY id DESC
LIMIT 20;

-- Current state
SELECT channel, key, score, updated_at
FROM cf_map_state
WHERE channel = 'leaderboard'
ORDER BY score DESC
LIMIT 10;

-- Channel metadata
SELECT channel, top_offset, epoch
FROM cf_map_meta;
```

## Running the Demo

### PostgreSQL Backend

```bash
./map_demo -postgres "postgres://centrifuge:centrifuge@localhost:5432/centrifuge?sslmode=disable"
```

Every node independently polls `cf_map_stream` for new entries — no special coordination needed for multi-node deployments.

### PostgreSQL with Read Replicas

```bash
./map_demo \
  -postgres "postgres://centrifuge:centrifuge@localhost:5432/centrifuge?sslmode=disable" \
  -replicas "postgres://centrifuge:centrifuge@localhost:5433/centrifuge?sslmode=disable"
```

Read replicas offload ReadState and ReadStream queries from the primary. Reads are routed using shard-based routing: `hash(channel) % NumShards % len(replicas)`. Writes always go to the primary.

### Redis Backend

```bash
./map_demo -redis "localhost:6379"
```

### With Memory Cache

Add `-cache` flag for read-your-own-writes consistency and low-latency reads:

```bash
./map_demo -postgres "..." -cache
```

### In-Memory Backend (default)

```bash
./map_demo
```

No external dependencies. Data is lost on restart.

## Architecture

### Outbox-Based Delivery

The PostgreSQL broker uses outbox-based delivery:

1. **Publish**: `cf_map_publish()` SQL function atomically updates state and appends to `cf_map_stream`
2. **Poll**: Each node's outbox worker polls `cf_map_stream` for new entries (default: every 50ms)
3. **Deliver**: Worker calls `HandlePublication()` to deliver to local subscribers

With `UseNotify: true`, PostgreSQL `LISTEN/NOTIFY` wakes the outbox worker immediately on new entries, reducing delivery latency to near-zero.

### Sharding

Every stream row gets `shard_id = abs(hashtext(channel)) % NumShards`. When read replicas are configured, each outbox worker handles a subset of shards, routing reads to the same replica for consistency.

### Stream Cleanup

Stream entries are retained for `StreamRetention` (default: 24h) and cleaned up by a periodic worker. For high-throughput deployments, `Partitioning: true` uses daily PostgreSQL table partitions with instant `DROP TABLE` cleanup instead of `DELETE + VACUUM`.

## Configuration Reference

### Command-Line Flags

| Flag        | Description                          | Default |
|-------------|--------------------------------------|---------|
| `-port`     | HTTP server port                     | `3000`  |
| `-postgres` | PostgreSQL connection string         | (none)  |
| `-redis`    | Redis address                        | (none)  |
| `-replicas` | Comma-separated PG replica conn strings | (none)  |
| `-cache`    | Enable memory cache layer            | `false` |

### PostgreSQL Broker Options

When using `PostgresMapBrokerConfig`:

| Option                   | Description                              | Default    |
|--------------------------|------------------------------------------|------------|
| `ConnString`             | PostgreSQL connection string             | (required) |
| `PoolSize`               | Max connections in pool                  | `32`       |
| `NumShards`              | Shards for parallel delivery             | `16`       |
| `StreamRetention`        | How long to keep stream entries          | `24h`      |
| `UseNotify`              | LISTEN/NOTIFY for low-latency wakeup    | `false`    |
| `ReadReplicaConnStrings` | Read replica connection strings          | (none)     |
| `Partitioning`           | Daily table partitioning for cleanup     | `false`    |
| `Outbox.PollInterval`    | How often to poll for new entries        | `50ms`     |
| `Outbox.BatchSize`       | Max rows per outbox batch                | `1000`     |

## License

Same as Centrifuge library.
