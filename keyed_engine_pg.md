# PostgreSQL KeyedEngine Implementation

This document describes the PostgreSQL implementation of the KeyedEngine interface for persistent keyed subscriptions.

## Use Cases

PostgresKeyedEngine is ideal for:
- **Collaborative boards** (Miro, Figma-like) - objects must persist across sessions
- **Document editing** - persistent state with real-time sync
- **Game lobbies with persistent state** - saved game rooms
- **Any keyed subscription requiring durability**

For ephemeral use cases (presence, cursors), use MemoryKeyedEngine or RedisKeyedEngine.

## Channel Routing

Users can choose implementation per channel:

```go
node.config.GetKeyedEngine = func(channel string) KeyedEngine {
    if strings.HasPrefix(channel, "board:") {
        return postgresKeyedEngine  // Persistent
    }
    return redisKeyedEngine  // Ephemeral
}
```

## Database Schema

### Snapshot Table (Current State)

```sql
CREATE TABLE keyed_snapshot (
    channel         TEXT NOT NULL,
    key             TEXT NOT NULL,
    data            BYTEA,
    tags            JSONB,
    client_info     BYTEA,              -- Serialized ClientInfo
    score           BIGINT,             -- For ordered snapshots
    version         BIGINT DEFAULT 0,
    version_epoch   TEXT,
    offset          BIGINT NOT NULL,    -- Stream offset when written
    expires_at      TIMESTAMPTZ,        -- For KeyTTL
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (channel, key)
);

-- For ordered snapshot queries
CREATE INDEX keyed_snapshot_ordered_idx
    ON keyed_snapshot (channel, score DESC, key)
    WHERE score IS NOT NULL;

-- For TTL expiration cleanup
CREATE INDEX keyed_snapshot_expires_idx
    ON keyed_snapshot (expires_at)
    WHERE expires_at IS NOT NULL;
```

### Stream Table (Change History)

```sql
CREATE TABLE keyed_stream (
    channel     TEXT NOT NULL,
    offset      BIGINT NOT NULL,
    epoch       TEXT NOT NULL,
    key         TEXT NOT NULL,
    data        BYTEA,
    tags        JSONB,
    client_info BYTEA,
    removed     BOOLEAN DEFAULT FALSE,
    created_at  TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (channel, offset)
);

-- For reading stream since offset
CREATE INDEX keyed_stream_lookup_idx
    ON keyed_stream (channel, offset);
```

### Stream Metadata Table

```sql
CREATE TABLE keyed_stream_meta (
    channel         TEXT PRIMARY KEY,
    epoch           TEXT NOT NULL,
    top_offset      BIGINT NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    meta_expires_at TIMESTAMPTZ           -- For StreamMetaTTL
);
```

### Idempotency Table

```sql
CREATE TABLE keyed_idempotency (
    channel         TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    result_offset   BIGINT NOT NULL,
    expires_at      TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (channel, idempotency_key)
);

CREATE INDEX keyed_idempotency_expires_idx
    ON keyed_idempotency (expires_at);
```

## Interface Method Implementations

### Subscribe / Unsubscribe

```go
func (e *PostgresKeyedEngine) Subscribe(ch string) error {
    // PostgreSQL LISTEN for real-time notifications
    _, err := e.conn.Exec(ctx, "LISTEN $1", sanitizeChannel(ch))
    return err
}

func (e *PostgresKeyedEngine) Unsubscribe(ch string) error {
    _, err := e.conn.Exec(ctx, "UNLISTEN $1", sanitizeChannel(ch))
    return err
}
```

Note: LISTEN/NOTIFY channel names have restrictions. Use a sanitization function or hash.

### Publish

```go
func (e *PostgresKeyedEngine) Publish(ctx context.Context, ch, key string, opts KeyedPublishOptions) (KeyedPublishResult, error) {
    tx, err := e.pool.Begin(ctx)
    if err != nil {
        return KeyedPublishResult{}, err
    }
    defer tx.Rollback(ctx)

    // 1. Check idempotency
    if opts.IdempotencyKey != "" {
        var existingOffset int64
        err := tx.QueryRow(ctx, `
            SELECT result_offset FROM keyed_idempotency
            WHERE channel = $1 AND idempotency_key = $2 AND expires_at > NOW()
        `, ch, opts.IdempotencyKey).Scan(&existingOffset)

        if err == nil {
            // Duplicate - return cached result
            return KeyedPublishResult{
                Position:       StreamPosition{Offset: existingOffset, Epoch: epoch},
                Suppressed:     true,
                SuppressReason: SuppressReasonIdempotency,
            }, nil
        }
    }

    // 2. Get or create stream meta, increment offset
    var epoch string
    var newOffset int64
    err = tx.QueryRow(ctx, `
        INSERT INTO keyed_stream_meta (channel, epoch, top_offset)
        VALUES ($1, $2, 1)
        ON CONFLICT (channel) DO UPDATE
        SET top_offset = keyed_stream_meta.top_offset + 1
        RETURNING epoch, top_offset
    `, ch, generateEpoch()).Scan(&epoch, &newOffset)
    if err != nil {
        return KeyedPublishResult{}, err
    }

    // 3. Check KeyMode conditions
    if opts.KeyMode == KeyModeIfNew || opts.KeyMode == KeyModeIfExists {
        var exists bool
        err = tx.QueryRow(ctx, `
            SELECT EXISTS(SELECT 1 FROM keyed_snapshot WHERE channel = $1 AND key = $2)
        `, ch, key).Scan(&exists)
        if err != nil {
            return KeyedPublishResult{}, err
        }

        if opts.KeyMode == KeyModeIfNew && exists {
            if opts.RefreshTTLOnSuppress && opts.KeyTTL > 0 {
                // Refresh TTL without updating data
                tx.Exec(ctx, `
                    UPDATE keyed_snapshot SET expires_at = $3
                    WHERE channel = $1 AND key = $2
                `, ch, key, time.Now().Add(opts.KeyTTL))
            }
            tx.Commit(ctx)
            return KeyedPublishResult{
                Position:       StreamPosition{Offset: newOffset, Epoch: epoch},
                Suppressed:     true,
                SuppressReason: SuppressReasonKeyExists,
            }, nil
        }

        if opts.KeyMode == KeyModeIfExists && !exists {
            tx.Commit(ctx)
            return KeyedPublishResult{
                Position:       StreamPosition{Offset: newOffset, Epoch: epoch},
                Suppressed:     true,
                SuppressReason: SuppressReasonKeyNotFound,
            }, nil
        }
    }

    // 4. Check version (optimistic concurrency)
    if opts.Version > 0 {
        var currentVersion int64
        err = tx.QueryRow(ctx, `
            SELECT COALESCE(version, 0) FROM keyed_snapshot
            WHERE channel = $1 AND key = $2
        `, ch, key).Scan(&currentVersion)

        if err == nil && currentVersion >= opts.Version {
            tx.Commit(ctx)
            return KeyedPublishResult{
                Position:       StreamPosition{Offset: newOffset, Epoch: epoch},
                Suppressed:     true,
                SuppressReason: SuppressReasonVersion,
            }, nil
        }
    }

    // 5. Update snapshot
    var expiresAt *time.Time
    if opts.KeyTTL > 0 {
        t := time.Now().Add(opts.KeyTTL)
        expiresAt = &t
    }

    _, err = tx.Exec(ctx, `
        INSERT INTO keyed_snapshot (channel, key, data, tags, score, version, version_epoch, offset, expires_at, updated_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW())
        ON CONFLICT (channel, key) DO UPDATE SET
            data = EXCLUDED.data,
            tags = EXCLUDED.tags,
            score = EXCLUDED.score,
            version = EXCLUDED.version,
            version_epoch = EXCLUDED.version_epoch,
            offset = EXCLUDED.offset,
            expires_at = EXCLUDED.expires_at,
            updated_at = NOW()
    `, ch, key, opts.Data, opts.Tags, opts.Score, opts.Version, opts.VersionEpoch, newOffset, expiresAt)
    if err != nil {
        return KeyedPublishResult{}, err
    }

    // 6. Append to stream (if StreamSize > 0)
    if opts.StreamSize > 0 {
        _, err = tx.Exec(ctx, `
            INSERT INTO keyed_stream (channel, offset, epoch, key, data, tags, removed)
            VALUES ($1, $2, $3, $4, $5, $6, FALSE)
        `, ch, newOffset, epoch, key, opts.Data, opts.Tags)
        if err != nil {
            return KeyedPublishResult{}, err
        }

        // Trim old entries
        _, _ = tx.Exec(ctx, `
            DELETE FROM keyed_stream
            WHERE channel = $1 AND offset <= $2 - $3
        `, ch, newOffset, opts.StreamSize)
    }

    // 7. Save idempotency key
    if opts.IdempotencyKey != "" {
        _, _ = tx.Exec(ctx, `
            INSERT INTO keyed_idempotency (channel, idempotency_key, result_offset, expires_at)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT DO NOTHING
        `, ch, opts.IdempotencyKey, newOffset, time.Now().Add(opts.IdempotentResultTTL))
    }

    // 8. Notify (best effort, outside transaction is fine)
    if opts.Publish {
        // Notify with minimal payload - clients fetch actual data
        payload := fmt.Sprintf(`{"ch":"%s","k":"%s","o":%d}`, ch, key, newOffset)
        _, _ = tx.Exec(ctx, "SELECT pg_notify($1, $2)", notifyChannel(ch), payload)
    }

    if err := tx.Commit(ctx); err != nil {
        return KeyedPublishResult{}, err
    }

    return KeyedPublishResult{
        Position:   StreamPosition{Offset: newOffset, Epoch: epoch},
        Suppressed: false,
    }, nil
}
```

### Unpublish

```go
func (e *PostgresKeyedEngine) Unpublish(ctx context.Context, ch, key string, opts KeyedUnpublishOptions) (KeyedPublishResult, error) {
    tx, err := e.pool.Begin(ctx)
    if err != nil {
        return KeyedPublishResult{}, err
    }
    defer tx.Rollback(ctx)

    // 1. Get stream meta
    var epoch string
    var newOffset int64
    err = tx.QueryRow(ctx, `
        UPDATE keyed_stream_meta
        SET top_offset = top_offset + 1
        WHERE channel = $1
        RETURNING epoch, top_offset
    `, ch).Scan(&epoch, &newOffset)
    if err != nil {
        return KeyedPublishResult{}, err
    }

    // 2. Delete from snapshot
    result, err := tx.Exec(ctx, `
        DELETE FROM keyed_snapshot WHERE channel = $1 AND key = $2
    `, ch, key)
    if err != nil {
        return KeyedPublishResult{}, err
    }

    if result.RowsAffected() == 0 {
        // Key didn't exist
        tx.Commit(ctx)
        return KeyedPublishResult{
            Position:       StreamPosition{Offset: newOffset, Epoch: epoch},
            Suppressed:     true,
            SuppressReason: SuppressReasonKeyNotFound,
        }, nil
    }

    // 3. Append removal to stream
    if opts.StreamSize > 0 {
        _, err = tx.Exec(ctx, `
            INSERT INTO keyed_stream (channel, offset, epoch, key, removed)
            VALUES ($1, $2, $3, $4, TRUE)
        `, ch, newOffset, epoch, key)
        if err != nil {
            return KeyedPublishResult{}, err
        }
    }

    // 4. Notify
    if opts.Publish {
        payload := fmt.Sprintf(`{"ch":"%s","k":"%s","o":%d,"r":true}`, ch, key, newOffset)
        _, _ = tx.Exec(ctx, "SELECT pg_notify($1, $2)", notifyChannel(ch), payload)
    }

    if err := tx.Commit(ctx); err != nil {
        return KeyedPublishResult{}, err
    }

    return KeyedPublishResult{
        Position:   StreamPosition{Offset: newOffset, Epoch: epoch},
        Suppressed: false,
    }, nil
}
```

### ReadSnapshot

```go
func (e *PostgresKeyedEngine) ReadSnapshot(ctx context.Context, ch string, opts KeyedReadSnapshotOptions) ([]*Publication, StreamPosition, string, error) {
    // Get current stream position
    var epoch string
    var topOffset int64
    err := e.pool.QueryRow(ctx, `
        SELECT epoch, top_offset FROM keyed_stream_meta WHERE channel = $1
    `, ch).Scan(&epoch, &topOffset)
    if err == pgx.ErrNoRows {
        // No data yet
        return nil, StreamPosition{}, "", nil
    }
    if err != nil {
        return nil, StreamPosition{}, "", err
    }

    // Validate epoch if revision provided
    if opts.Revision != nil && opts.Revision.Epoch != "" && opts.Revision.Epoch != epoch {
        return nil, StreamPosition{}, "", ErrorUnrecoverablePosition
    }

    // Build query
    var query string
    var args []interface{}

    if opts.Ordered {
        query = `
            SELECT key, data, tags, offset, score
            FROM keyed_snapshot
            WHERE channel = $1 AND (expires_at IS NULL OR expires_at > NOW())
            ORDER BY score DESC, key
            LIMIT $2 OFFSET $3
        `
    } else {
        query = `
            SELECT key, data, tags, offset, score
            FROM keyed_snapshot
            WHERE channel = $1 AND (expires_at IS NULL OR expires_at > NOW())
            ORDER BY key
            LIMIT $2 OFFSET $3
        `
    }

    limit := opts.Limit
    if limit <= 0 {
        limit = 100
    }
    args = []interface{}{ch, limit + 1, opts.Offset} // +1 to detect more pages

    rows, err := e.pool.Query(ctx, query, args...)
    if err != nil {
        return nil, StreamPosition{}, "", err
    }
    defer rows.Close()

    var pubs []*Publication
    for rows.Next() {
        var p Publication
        var score *int64
        if err := rows.Scan(&p.Key, &p.Data, &p.Tags, &p.Offset, &score); err != nil {
            return nil, StreamPosition{}, "", err
        }
        pubs = append(pubs, &p)
    }

    // Determine next cursor
    var nextCursor string
    if len(pubs) > limit {
        pubs = pubs[:limit]
        nextCursor = fmt.Sprintf("%d", opts.Offset+limit)
    }

    return pubs, StreamPosition{Offset: topOffset, Epoch: epoch}, nextCursor, nil
}
```

### ReadStream

```go
func (e *PostgresKeyedEngine) ReadStream(ctx context.Context, ch string, opts KeyedReadStreamOptions) ([]*Publication, StreamPosition, error) {
    // Get current stream position
    var epoch string
    var topOffset int64
    err := e.pool.QueryRow(ctx, `
        SELECT epoch, top_offset FROM keyed_stream_meta WHERE channel = $1
    `, ch).Scan(&epoch, &topOffset)
    if err == pgx.ErrNoRows {
        // Channel doesn't exist - create with new epoch
        epoch = generateEpoch()
        return nil, StreamPosition{Offset: 0, Epoch: epoch}, nil
    }
    if err != nil {
        return nil, StreamPosition{}, err
    }

    streamPos := StreamPosition{Offset: topOffset, Epoch: epoch}

    // Limit = 0 means only return position
    if opts.Filter.Limit == 0 {
        return nil, streamPos, nil
    }

    // Validate epoch if Since provided
    if opts.Filter.Since != nil && opts.Filter.Since.Epoch != "" && opts.Filter.Since.Epoch != epoch {
        return nil, StreamPosition{}, ErrorUnrecoverablePosition
    }

    // Build query
    var query string
    sinceOffset := int64(0)
    if opts.Filter.Since != nil {
        sinceOffset = int64(opts.Filter.Since.Offset)
    }

    limit := opts.Filter.Limit
    if limit < 0 {
        limit = 10000 // Reasonable max
    }

    if opts.Filter.Reverse {
        query = `
            SELECT key, data, tags, offset, removed
            FROM keyed_stream
            WHERE channel = $1 AND offset > $2
            ORDER BY offset DESC
            LIMIT $3
        `
    } else {
        query = `
            SELECT key, data, tags, offset, removed
            FROM keyed_stream
            WHERE channel = $1 AND offset > $2
            ORDER BY offset ASC
            LIMIT $3
        `
    }

    rows, err := e.pool.Query(ctx, query, ch, sinceOffset, limit)
    if err != nil {
        return nil, StreamPosition{}, err
    }
    defer rows.Close()

    var pubs []*Publication
    for rows.Next() {
        var p Publication
        var removed bool
        if err := rows.Scan(&p.Key, &p.Data, &p.Tags, &p.Offset, &removed); err != nil {
            return nil, StreamPosition{}, err
        }
        // Mark removed publications (client handles this)
        if removed {
            p.Data = nil // Convention: nil data means removed
        }
        pubs = append(pubs, &p)
    }

    return pubs, streamPos, nil
}
```

### Stats

```go
func (e *PostgresKeyedEngine) Stats(ctx context.Context, ch string) (KeyedStats, error) {
    var count int
    err := e.pool.QueryRow(ctx, `
        SELECT COUNT(*) FROM keyed_snapshot
        WHERE channel = $1 AND (expires_at IS NULL OR expires_at > NOW())
    `, ch).Scan(&count)
    if err != nil {
        return KeyedStats{}, err
    }
    return KeyedStats{NumKeys: count}, nil
}
```

### Remove

```go
func (e *PostgresKeyedEngine) Remove(ctx context.Context, ch string, opts KeyedRemoveOptions) error {
    tx, err := e.pool.Begin(ctx)
    if err != nil {
        return err
    }
    defer tx.Rollback(ctx)

    _, _ = tx.Exec(ctx, "DELETE FROM keyed_snapshot WHERE channel = $1", ch)
    _, _ = tx.Exec(ctx, "DELETE FROM keyed_stream WHERE channel = $1", ch)
    _, _ = tx.Exec(ctx, "DELETE FROM keyed_stream_meta WHERE channel = $1", ch)
    _, _ = tx.Exec(ctx, "DELETE FROM keyed_idempotency WHERE channel = $1", ch)

    return tx.Commit(ctx)
}
```

## Background Workers

### TTL Expiration Worker

Periodically removes expired keys and emits removal events:

```go
func (e *PostgresKeyedEngine) runTTLExpirationWorker(ctx context.Context) {
    ticker := time.NewTicker(time.Second)
    defer ticker.Stop()

    for {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
            e.expireKeys(ctx)
        }
    }
}

func (e *PostgresKeyedEngine) expireKeys(ctx context.Context) {
    // Find and remove expired keys, emitting removal events
    rows, err := e.pool.Query(ctx, `
        DELETE FROM keyed_snapshot
        WHERE expires_at IS NOT NULL AND expires_at <= NOW()
        RETURNING channel, key
    `)
    if err != nil {
        return
    }
    defer rows.Close()

    for rows.Next() {
        var ch, key string
        if err := rows.Scan(&ch, &key); err != nil {
            continue
        }
        // Emit removal to stream (fire and forget)
        e.Unpublish(ctx, ch, key, KeyedUnpublishOptions{
            Publish:    true,
            StreamSize: 1000, // Use default
        })
    }
}
```

### Idempotency Cleanup Worker

```go
func (e *PostgresKeyedEngine) runIdempotencyCleanupWorker(ctx context.Context) {
    ticker := time.NewTicker(time.Minute)
    defer ticker.Stop()

    for {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
            e.pool.Exec(ctx, "DELETE FROM keyed_idempotency WHERE expires_at < NOW()")
        }
    }
}
```

## LISTEN/NOTIFY Handling

### Notification Listener

```go
func (e *PostgresKeyedEngine) listenForNotifications(ctx context.Context) {
    conn, err := e.pool.Acquire(ctx)
    if err != nil {
        return
    }
    defer conn.Release()

    for {
        notification, err := conn.Conn().WaitForNotification(ctx)
        if err != nil {
            return
        }

        // Parse minimal payload
        var msg struct {
            Channel string `json:"ch"`
            Key     string `json:"k"`
            Offset  int64  `json:"o"`
            Removed bool   `json:"r"`
        }
        json.Unmarshal([]byte(notification.Payload), &msg)

        // Forward to Centrifuge broker for pub/sub delivery
        e.eventHandler.HandlePublication(msg.Channel, &Publication{
            Key:    msg.Key,
            Offset: uint64(msg.Offset),
            // Data fetched by client via ReadStream if needed
        }, StreamPosition{Offset: uint64(msg.Offset)}, false, nil)
    }
}
```

## Advantages Over Redis Implementation

| Aspect | PostgreSQL | Redis |
|--------|------------|-------|
| Durability | Built-in WAL | Requires AOF config |
| Consistency | Full ACID | Eventual (pub/sub) |
| Position checks | Not needed (transactions) | Required (buffering) |
| Rich queries | Full SQL | Limited |
| Existing infra | Often already have PG | Additional system |

## Scalability

### Expected Performance (Single Node)

- **Writes:** 5,000-20,000/sec
- **Reads:** 50,000-100,000/sec
- **Concurrent boards:** 10,000+
- **Objects per board:** 10,000+

### Scaling Strategies

1. **Read replicas** - Route reads to replicas
2. **Connection pooling** - PgBouncer
3. **Table partitioning** - Partition by channel hash
4. **Citus** - Distributed PostgreSQL, shard by channel

## Configuration

```go
type PostgresKeyedEngineConfig struct {
    // Connection
    ConnString string
    PoolSize   int

    // Defaults
    DefaultStreamSize    int
    DefaultStreamTTL     time.Duration
    DefaultStreamMetaTTL time.Duration

    // Workers
    TTLCheckInterval        time.Duration
    IdempotencyCheckInterval time.Duration
}
```

## Migration

```sql
-- Initial migration
CREATE TABLE keyed_snapshot (...);
CREATE TABLE keyed_stream (...);
CREATE TABLE keyed_stream_meta (...);
CREATE TABLE keyed_idempotency (...);

-- Create indexes
CREATE INDEX ...;
```
