# PostgreSQL KeyedEngine Implementation

This document describes the PostgreSQL implementation of the KeyedEngine interface for persistent keyed subscriptions.

## Use Cases

PostgresKeyedEngine is ideal for:
- **Collaborative boards** (Miro, Figma-like) - objects must persist across sessions
- **Document editing** - persistent state with real-time sync
- **Inventory/booking systems** - CAS operations for atomic updates
- **Game lobbies with persistent state** - saved game rooms
- **Any keyed subscription requiring durability**

For ephemeral use cases (presence, cursors), use MemoryKeyedEngine or RedisKeyedEngine.

## Architecture

The design uses **direct polling** from Centrifuge nodes to PostgreSQL, eliminating the need for an intermediate pub/sub layer (Redis, NATS, etc.).

```
┌─────────────────────────────────────────────────────────────────┐
│                         PostgreSQL                              │
│                                                                 │
│  cf_keyed_snapshot         cf_keyed_stream                      │
│  ┌──────────────┐         ┌─────────────────────────────────┐   │
│  │ channel, key │         │ id (global), channel, offset,   │   │
│  │ data, score  │         │ key, data, removed, expires_at  │   │
│  └──────────────┘         └─────────────────────────────────┘   │
│         │                              │                        │
│   (new subscriber)            (real-time + recovery)            │
└─────────┼──────────────────────────────┼────────────────────────┘
          │                              │
          │         NOTIFY (wake-up)     │
          │              +               │
          │    Poll WHERE id > last_id   │
          │                              │
          ▼                              ▼
      ┌──────────┐  ┌──────────┐  ┌──────────┐
      │  Node 1  │  │  Node 2  │  │  Node N  │
      │ last_id: │  │ last_id: │  │ last_id: │
      │  98760   │  │  98755   │  │  98762   │
      └────┬─────┘  └────┬─────┘  └────┬─────┘
           │             │             │
           ▼             ▼             ▼
       Clients       Clients       Clients
       (offset)      (offset)      (offset)
```

### Key Design Decisions

1. **Dual ID System**
   - `id` (global BIGSERIAL): For efficient node polling across all channels
   - `channel_offset` (per-channel): For Centrifuge recovery semantics

2. **No Intermediate Pub/Sub**
   - Nodes poll PostgreSQL directly
   - NOTIFY used only as wake-up signal (not for data)
   - Simpler operations, fewer failure points

3. **Read Replicas for Scale**
   - Writes go to primary
   - 250 nodes distribute reads across replicas

4. **SQL Functions for Publishing**
   - Single function call instead of multiple queries
   - All logic encapsulated in `cf_keyed_publish()` and `cf_keyed_unpublish()`
   - Enables transactional publishing from any language with a PostgreSQL driver

5. **Split Client Info**
   - Individual columns (client_id, user_id, conn_info, chan_info, subscribed_at) instead of single BYTEA
   - Enables SQL queries filtering by user/client

## Database Schema

All tables and functions use the `cf_` prefix to avoid naming collisions.

### cf_keyed_stream (Change History + Fan-out)

The stream table has two IDs:
- `id`: Global auto-increment for efficient node polling
- `channel_offset`: Per-channel sequence for Centrifuge client recovery

```sql
CREATE TABLE cf_keyed_stream (
    id              BIGSERIAL PRIMARY KEY,
    channel         TEXT NOT NULL,
    channel_offset  BIGINT NOT NULL,
    key             TEXT NOT NULL,
    data            BYTEA,
    tags            JSONB,
    client_id       TEXT,
    user_id         TEXT,
    conn_info       BYTEA,
    chan_info       BYTEA,
    subscribed_at   TIMESTAMPTZ,
    removed         BOOLEAN DEFAULT FALSE,
    expires_at      TIMESTAMPTZ,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX cf_keyed_stream_channel_offset_idx ON cf_keyed_stream (channel, channel_offset);
CREATE INDEX cf_keyed_stream_expires_idx ON cf_keyed_stream (expires_at) WHERE expires_at IS NOT NULL;
```

### cf_keyed_snapshot (Current State)

```sql
CREATE TABLE cf_keyed_snapshot (
    channel             TEXT NOT NULL,
    key                 TEXT NOT NULL,
    data                BYTEA,
    tags                JSONB,
    client_id           TEXT,
    user_id             TEXT,
    conn_info           BYTEA,
    chan_info           BYTEA,
    subscribed_at       TIMESTAMPTZ,
    score               BIGINT,
    key_version         BIGINT DEFAULT 0,
    key_version_epoch   TEXT,
    key_offset          BIGINT NOT NULL,
    expires_at          TIMESTAMPTZ,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (channel, key)
);

CREATE INDEX cf_keyed_snapshot_ordered_idx
    ON cf_keyed_snapshot (channel, score DESC, key)
    WHERE score IS NOT NULL;
CREATE INDEX cf_keyed_snapshot_expires_idx
    ON cf_keyed_snapshot (expires_at)
    WHERE expires_at IS NOT NULL;
```

### cf_keyed_stream_meta (Stream Metadata)

```sql
CREATE TABLE cf_keyed_stream_meta (
    channel         TEXT PRIMARY KEY,
    top_offset      BIGINT NOT NULL DEFAULT 0,
    epoch           TEXT NOT NULL DEFAULT '',
    version         BIGINT DEFAULT 0,
    version_epoch   TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW(),
    expires_at      TIMESTAMPTZ
);

CREATE INDEX cf_keyed_stream_meta_expires_idx
    ON cf_keyed_stream_meta (expires_at)
    WHERE expires_at IS NOT NULL;
```

### cf_keyed_idempotency (Duplicate Detection)

```sql
CREATE TABLE cf_keyed_idempotency (
    channel         TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    result_offset   BIGINT NOT NULL,
    result_id       BIGINT NOT NULL,
    expires_at      TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (channel, idempotency_key)
);

CREATE INDEX cf_keyed_idempotency_expires_idx ON cf_keyed_idempotency (expires_at);
```

## SQL Functions

The Go implementation uses SQL functions for Publish and Unpublish operations. This provides:
- Single RTT instead of multiple queries
- Transactional guarantees handled by PostgreSQL
- Same code path as users calling SQL functions directly

### cf_keyed_publish

Main publishing function with all suppression checks:

```sql
CREATE OR REPLACE FUNCTION cf_keyed_publish(
    p_channel TEXT,
    p_key TEXT,
    p_data BYTEA,
    p_tags JSONB DEFAULT NULL,
    p_client_id TEXT DEFAULT NULL,
    p_user_id TEXT DEFAULT NULL,
    p_conn_info BYTEA DEFAULT NULL,
    p_chan_info BYTEA DEFAULT NULL,
    p_subscribed_at TIMESTAMPTZ DEFAULT NULL,
    p_key_mode TEXT DEFAULT NULL,
    p_key_ttl INTERVAL DEFAULT NULL,
    p_stream_ttl INTERVAL DEFAULT NULL,
    p_meta_ttl INTERVAL DEFAULT NULL,
    p_expected_offset BIGINT DEFAULT NULL,
    p_score BIGINT DEFAULT NULL,
    p_version BIGINT DEFAULT NULL,
    p_version_epoch TEXT DEFAULT NULL,
    p_key_version BIGINT DEFAULT NULL,
    p_key_version_epoch TEXT DEFAULT NULL,
    p_idempotency_key TEXT DEFAULT NULL,
    p_idempotency_ttl INTERVAL DEFAULT NULL,
    p_stream_size INT DEFAULT NULL,
    p_refresh_ttl_on_suppress BOOLEAN DEFAULT FALSE
) RETURNS TABLE(
    id BIGINT,
    channel_offset BIGINT,
    epoch TEXT,
    suppressed BOOLEAN,
    suppress_reason TEXT,
    current_data BYTEA,
    current_offset BIGINT
) AS $$
-- Implementation handles:
-- 1. Get or create stream metadata
-- 2. Check idempotency
-- 3. CAS check (ExpectedPosition)
-- 4. KeyMode check (if_new, if_exists)
-- 5. Stream-level version check
-- 6. Increment offset and update version
-- 7. Update snapshot
-- 8. Insert into stream
-- 9. Trim stream if needed
-- 10. Save idempotency key
-- 11. NOTIFY polling nodes
$$ LANGUAGE plpgsql;
```

### cf_keyed_publish_strict

Auto-rollback version that raises exceptions on suppression:

```sql
CREATE OR REPLACE FUNCTION cf_keyed_publish_strict(
    -- Same parameters as cf_keyed_publish
) RETURNS TABLE(id BIGINT, channel_offset BIGINT, epoch TEXT) AS $$
-- Calls cf_keyed_publish and raises exception if suppressed
$$ LANGUAGE plpgsql;
```

### cf_keyed_unpublish

Remove a key from the snapshot:

```sql
CREATE OR REPLACE FUNCTION cf_keyed_unpublish(
    p_channel TEXT,
    p_key TEXT,
    p_client_id TEXT DEFAULT NULL,
    p_user_id TEXT DEFAULT NULL,
    p_stream_ttl INTERVAL DEFAULT NULL,
    p_idempotency_key TEXT DEFAULT NULL,
    p_idempotency_ttl INTERVAL DEFAULT NULL,
    p_meta_ttl INTERVAL DEFAULT NULL
) RETURNS TABLE(
    id BIGINT,
    channel_offset BIGINT,
    epoch TEXT,
    suppressed BOOLEAN,
    suppress_reason TEXT
) AS $$
-- Implementation handles:
-- 1. Get stream metadata
-- 2. Check idempotency
-- 3. Check if key exists
-- 4. Increment offset
-- 5. Delete from snapshot
-- 6. Insert removal into stream
-- 7. Save idempotency key
-- 8. NOTIFY polling nodes
$$ LANGUAGE plpgsql;
```

## Go Implementation

The Go code uses the SQL functions for Publish and Unpublish:

```go
func (e *PostgresKeyedEngine) Publish(ctx context.Context, ch string, key string, opts KeyedPublishOptions) (KeyedPublishResult, error) {
    // Prepare parameters...

    err := e.pool.QueryRow(ctx, `
        SELECT id, channel_offset, epoch, suppressed, suppress_reason, current_data, current_offset
        FROM cf_keyed_publish($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::interval, $12::interval, $13::interval, $14, $15, $16, $17, $18, $19, $20, $21::interval, $22, $23)
    `,
        ch, key, opts.Data, tagsJSON,
        clientID, userID, connInfo, chanInfo, subscribedAt,
        keyMode, keyTTL, streamTTL, metaTTL,
        expectedOffset, score, version, versionEpoch,
        keyVersion, keyVersionEpoch,
        idempotencyKey, idempotencyTTL, streamSize, opts.RefreshTTLOnSuppress,
    ).Scan(&id, &channelOffset, &epoch, &suppressed, &suppressReason, &currentData, &currentOffset)

    // Handle result...
}
```

## Transactional Publishing

Applications can publish real-time events atomically with their business logic:

```sql
BEGIN;
    -- Business logic
    INSERT INTO orders (id, user_id, total) VALUES ('ord_123', 'user_1', 99.99);

    -- Real-time publish - SAME TRANSACTION!
    SELECT * FROM cf_keyed_publish(
        'orders:user_1',
        'order_ord_123',
        '{"id": "ord_123", "total": 99.99}'::bytea
    );
COMMIT;
```

If either operation fails, both are rolled back.

## Docker Setup

Quick start with Docker Compose:

```yaml
version: '3.8'
services:
  postgres:
    image: postgres:16
    container_name: keyed_engine_postgres
    environment:
      POSTGRES_USER: centrifuge
      POSTGRES_PASSWORD: centrifuge
      POSTGRES_DB: centrifuge
    ports:
      - "5432:5432"
    volumes:
      - ./postgres/init.sql:/docker-entrypoint-initdb.d/init.sql:ro
```

```bash
cd _examples/keyed_demo
docker-compose up -d
```

Connection string:
```
postgres://centrifuge:centrifuge@localhost:5432/centrifuge?sslmode=disable
```

## Testing

```bash
# Run integration tests
CENTRIFUGE_POSTGRES_URL="postgres://centrifuge:centrifuge@localhost:5432/centrifuge?sslmode=disable" \
  go test -v -tags=integration -run TestPostgresKeyed ./...
```

Verify installation:
```bash
docker exec -it keyed_engine_postgres psql -U centrifuge -d centrifuge

# Test publish
SELECT * FROM cf_keyed_publish('test', 'key1', 'hello'::bytea);

# Check snapshot
SELECT * FROM cf_keyed_snapshot WHERE channel = 'test';

# Check stream
SELECT * FROM cf_keyed_stream WHERE channel = 'test';
```

## Migration Notes

This schema uses the `cf_` prefix for all tables and functions. If migrating from a previous version:

1. Create new tables with `cf_` prefix
2. Migrate data from old tables
3. Drop old tables and functions

For new deployments, the `init.sql` script handles everything automatically.

## Summary

PostgresKeyedEngine provides:

1. **Durability** - Full ACID transactions, WAL-based persistence
2. **CAS Support** - ExpectedPosition for atomic read-modify-write
3. **Simple Architecture** - Direct polling, no intermediate pub/sub
4. **Scalability** - Read replicas for 250+ nodes
5. **Dual ID System** - Global `id` for efficient fan-out, per-channel `channel_offset` for Centrifuge semantics
6. **SQL Functions** - Single-call Publish/Unpublish via `cf_keyed_publish()` and `cf_keyed_unpublish()`
7. **Transactional Publishing** - Atomic real-time events with business logic, any language
8. **Split Client Info** - Individual columns for client_id, user_id, etc. enabling SQL queries
