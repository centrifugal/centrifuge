# PostgreSQL MapBroker Simplification

## Context

The current PG MapBroker has complex architecture: single-leader-per-shard outbox delivery with advisory locks, persistent cursors, cursor-aware stream trimming, WAL logical replication, and Redis broker dependency for multi-node fan-out. Key insight: **every node can independently poll the stream table** — SQL SELECT is reliable (unlike lossy Redis pub/sub). Stream retention becomes time-based bulk cleanup (with optional PG partitioning). This eliminates advisory locks, cursor persistence, shard claiming, WAL reader, and Redis dependency entirely.

## Files to Modify

1. `internal/postgres_sql/schema.sql` — template schema
2. `internal/postgres_sql/schema_jsonb.sql` — generated (via `make pg-schemas`)
3. `internal/postgres_sql/schema_binary.sql` — generated (via `make pg-schemas`)
4. `map_broker_postgres.go` — main implementation (~2165 → ~1500 lines)
5. `map_broker_postgres_test.go` — tests (~13 tests removed, benchmarks + new tests added)
6. `go.mod` / `go.sum` — remove `pglogrepl` dependency

---

## Step 0: Benchmarks (Before Refactoring)

Add benchmarks to `map_broker_postgres_test.go` **before** starting any refactoring, then re-run them after to compare.

### Benchmarks to add
```go
BenchmarkPostgresMapBroker_Publish          // single publish
BenchmarkPostgresMapBroker_PublishParallel   // concurrent publishes (b.RunParallel)
BenchmarkPostgresMapBroker_ReadState         // read full state (pre-populated channel)
BenchmarkPostgresMapBroker_ReadStream        // read stream (pre-populated channel)
BenchmarkPostgresMapBroker_Remove            // single remove
BenchmarkPostgresMapBroker_PublishAndDeliver // publish + wait for outbox delivery
```

Run before refactoring:
```
CENTRIFUGE_POSTGRES_URL=... go test -bench=BenchmarkPostgresMapBroker -benchmem -count=5 -run=^$ . | tee bench_before.txt
```

Run after refactoring:
```
CENTRIFUGE_POSTGRES_URL=... go test -bench=BenchmarkPostgresMapBroker -benchmem -count=5 -run=^$ . | tee bench_after.txt
benchstat bench_before.txt bench_after.txt
```

---

## Step 1: Schema Template Changes (`schema.sql`)

### Remove
- **`__PREFIX__outbox_cursor` table** (lines 40-48)
- **`expires_at` column** from `__PREFIX__stream` (line 27)
- **`__PREFIX__stream_expires_idx` index** (line 34)
- **`__PREFIX__stream_shard_cursor_idx` index** (line 35)
- **WAL replica identity comment** (lines 37-38)
- **`p_stream_size`** param from `__PREFIX__publish` (line 132) and `__PREFIX__publish_strict` (line 313)
- **Stream trimming block** in `__PREFIX__publish` (lines 273-286)
- **`p_stream_ttl`** param from all functions: `__PREFIX__publish` (line 131), `__PREFIX__publish_strict` (line 312), `__PREFIX__remove` (line 380), `__PREFIX__remove_strict` (line 482), `__PREFIX__expire_keys` (line 517)
- All `expires_at` references in stream INSERTs

### Add
- **`__PREFIX__stream_created_at_idx`** index for time-based cleanup
- **`pg_notify`** calls at end of `__PREFIX__publish`, `__PREFIX__remove`, inside `__PREFIX__expire_keys` loop (these only fire when `UseNotify` is conceptually on, but since it's in SQL the notify always fires — the Go side just ignores it if not listening)

### Detailed SQL Function Cleanup

**`__PREFIX__publish` (currently 26 params → ~24 params):**
- Remove `p_stream_ttl` (param 12) — no per-row stream TTL
- Remove `p_stream_size` (param 13) — no in-publish trimming
- Remove step 9 entirely (cursor-aware stream trimming block, lines 273-286)
- Stream INSERT (step 8): remove `expires_at` column and its `CASE WHEN p_stream_ttl` expression
- Add `PERFORM pg_notify('{prefix}stream_notify', '')` before RETURN QUERY
- Re-number all `$N` positional parameters

**`__PREFIX__publish_strict` (currently 26 params → ~24 params):**
- Remove `p_stream_ttl` and `p_stream_size` from parameter list
- Remove them from pass-through call to `__PREFIX__publish`

**`__PREFIX__remove` (currently 10 params → ~9 params):**
- Remove `p_stream_ttl` (param 5)
- Stream INSERT (step 7): remove `expires_at` column and `CASE WHEN p_stream_ttl` expression
- Add `PERFORM pg_notify('{prefix}stream_notify', '')` before RETURN QUERY
- Re-number positional parameters

**`__PREFIX__remove_strict` (currently 10 params → ~9 params):**
- Remove `p_stream_ttl` from parameter list and pass-through

**`__PREFIX__expire_keys` (currently 5 params → ~4 params):**
- Remove `p_stream_ttl` (param 3)
- Stream INSERT: remove `expires_at` column and `CASE WHEN p_stream_ttl` expression
- Add `PERFORM pg_notify('{prefix}stream_notify', '')` inside the loop after stream INSERT

Then run `make pg-schemas`.

---

## Step 2: Go Config & Struct Changes (`map_broker_postgres.go`)

### Remove
- **`WALConfig` struct** (lines 125-147)
- **`OutboxConfig.AdvisoryLockBaseID`** (line 119)
- **`PostgresMapBrokerConfig.Broker`** (line 183)
- **`PostgresMapBrokerConfig.WAL`** (lines 189-191)
- **Struct fields**: `walClaimedShards`, `walClaimedShardMu`, `outboxClaimedShards`, `outboxClaimedShardMu` (lines 93-99)
- **`pgNames` fields** for cursor/WAL: `outboxCursor`, `pubPrefix`, `slotPrefix`

### Add
```go
type PostgresMapBrokerConfig struct {
    // ... existing fields (ConnString, PoolSize, NumShards, etc.) ...

    // StreamRetention controls how long stream entries are kept.
    // Cleanup worker deletes entries older than this. Default: 24h.
    StreamRetention time.Duration

    // UseNotify enables LISTEN/NOTIFY for low-latency outbox wakeup.
    // When false (default), outbox worker uses PollInterval-based polling only.
    // When true, a listener goroutine wakes the worker immediately on new entries.
    UseNotify bool

    // ReadReplicaConnStrings is an optional list of read replica connection strings.
    // When set, ReadState/ReadStream/outbox polling queries are distributed
    // across replicas using shard-based routing for consistency:
    //   hash(channel) % NumShards → shard_id % len(replicas) → replica
    // This ensures outbox and client reads for the same channel always
    // hit the same replica. Default: empty (all reads go to primary).
    ReadReplicaConnStrings []string

    // ReadReplicaPoolSize sets max connections per replica pool.
    // Default: same as PoolSize.
    ReadReplicaPoolSize int

    // Partitioning enables automatic daily partitioning of the stream table.
    // This is purely an optimization — without it, the broker works correctly
    // using simple DELETE-based cleanup. Partitioning helps at scale where
    // DROP TABLE (instant) is better than DELETE + VACUUM overhead.
    // Default: false.
    Partitioning bool

    // PartitionRetentionDays: how many days of partitions to keep.
    // Only used when Partitioning is true. Default: 3.
    PartitionRetentionDays int

    // PartitionLookaheadDays: how many future partitions to pre-create.
    // Only used when Partitioning is true. Default: 2.
    PartitionLookaheadDays int
}
```

New struct fields:
```go
type PostgresMapBroker struct {
    // ... existing fields ...
    readPools []*pgxpool.Pool  // one per replica; empty = use primary
    notifyCh  chan struct{}     // nil when UseNotify is false
}
```

### Shard-based replica routing (gated by `AllowCached`)

Reads route to replicas **only when `AllowCached=true`** in read options — same semantics as CachedMapBroker. Without `AllowCached`, reads always go to primary for strong consistency.

```go
// getReadPool returns the pool for reading the given channel.
// Only routes to replica when allowCached is true AND replicas are configured.
// Routes by shard: hash(channel) % NumShards % len(readPools) → replica index.
func (e *PostgresMapBroker) getReadPool(channel string, allowCached bool) *pgxpool.Pool {
    if !allowCached || len(e.readPools) == 0 {
        return e.pool
    }
    shardID := abs(hashtext(channel)) % e.conf.NumShards
    replicaIdx := shardID % len(e.readPools)
    return e.readPools[replicaIdx]
}
```

Usage in `ReadState`: `pool := e.getReadPool(ch, opts.AllowCached)`
Usage in `ReadStream`: `pool := e.getReadPool(ch, false)` (stream reads for position checks must be from primary)

For outbox polling with replicas: each node runs one outbox worker **per replica**, each filtering by its assigned shard_ids. This ensures outbox and client reads (via `AllowCached`) for the same channel hit the same replica:
```go
// outboxWorkerConfig returns (pool, shardIDs) for outbox worker #i.
// With N replicas: worker i handles shards where shard_id % N == i.
func (e *PostgresMapBroker) outboxWorkerConfig(replicaIdx int) (*pgxpool.Pool, []int) {
    n := len(e.readPools)
    if n == 0 {
        return e.pool, nil // nil = all shards (no filter)
    }
    var shards []int
    for s := 0; s < e.conf.NumShards; s++ {
        if s % n == replicaIdx {
            shards = append(shards, s)
        }
    }
    return e.readPools[replicaIdx], shards
}
```

Outbox query when shards are assigned:
```sql
-- With replica routing (shards = [0, 3, 6, 9, 12, 15] for replica 0, NumShards=16, 3 replicas)
WHERE id > $1 AND shard_id = ANY($2) ORDER BY id LIMIT $3
-- Without replicas (single worker, all shards)
WHERE id > $1 ORDER BY id LIMIT $2
```

---

## Step 3: Lifecycle Changes

### `NewPostgresMapBroker`
- Remove `PoolSize > NumShards` validation
- Remove `walClaimedShards`/`outboxClaimedShards` init
- If `ReadReplicaConnStrings` set: create pool for each replica
- If `UseNotify`: create `notifyCh = make(chan struct{}, 1)`

### `RegisterEventHandler` (rewrite)
```go
func (e *PostgresMapBroker) RegisterEventHandler(h BrokerEventHandler) error {
    e.eventHandler = h
    if e.running.Swap(true) {
        return errors.New("postgres map broker: already running")
    }
    if e.conf.UseNotify {
        go e.runNotificationListener()
    }
    // Start outbox workers: one per replica (or one for primary)
    numWorkers := max(len(e.readPools), 1)
    for i := 0; i < numWorkers; i++ {
        go e.runOutboxWorker(i)
    }
    go e.runTTLExpirationWorker()
    go e.runCleanupWorker()
    if e.conf.Partitioning {
        go e.runPartitionWorker()
    }
    return nil
}
```

### `Close`
- Close all `readPools`
- Remove inner broker close

### `Subscribe`/`Unsubscribe` — true no-ops (return nil)

---

## Step 4: New Outbox Worker

### `runNotificationListener` (new, only started when `UseNotify=true`)
- Acquires connection from primary pool, runs `LISTEN {prefix}stream_notify`
- Loops on `WaitForNotification(ctx)`
- Non-blocking send to `e.notifyCh`
- Reconnect on error with backoff

### `runOutboxWorker(replicaIdx int)` (new, replaces per-shard workers)
- Gets pool and shard filter from `outboxWorkerConfig(replicaIdx)`
- Startup: `SELECT COALESCE(MAX(id), 0) FROM {stream}` on its pool → initial cursor
- Main loop: waits on `e.notifyCh` (if non-nil) OR ticker (PollInterval)
- Inner loop: `processOutboxBatch(ctx, pool, cursor, shards, buf)` until batch < BatchSize
- No advisory locks, no cursor persistence

### `processOutboxBatch` (simplified)
- Params: `ctx, pool, cursor, shardIDs, buf`
- Query:
  - With shardIDs: `WHERE id > $1 AND shard_id = ANY($2) ORDER BY id LIMIT $3`
  - Without: `WHERE id > $1 ORDER BY id LIMIT $2`
- Always deliver via `eventHandler.HandlePublication()`
- In-memory cursor only
- Keep: zero-copy RawValues + byteArena, outboxBatchBuf reuse, delta handling

---

## Step 5: Cleanup Worker

### Non-partitioned mode (`Partitioning=false`)
`runCleanupWorker` at `CleanupInterval` (default 1m):
1. `DELETE FROM {stream} WHERE created_at < NOW() - $retention`
2. `DELETE FROM {meta} WHERE expires_at IS NOT NULL AND expires_at < NOW()`
3. `DELETE FROM {idempotency} WHERE expires_at < NOW()`

### Partitioned mode (`Partitioning=true`)
`runPartitionWorker` at `CleanupInterval`:
1. `ensureLookaheadPartitions()` — create next N days
2. `dropOldPartitions()` — drop partitions older than `PartitionRetentionDays`

Still runs meta/idempotency cleanup via `runCleanupWorker` (without stream DELETE).

---

## Step 6: Partitioning Support (Optional Optimization)

Partitioning is **not required** — without it, cleanup uses `DELETE ... WHERE created_at < retention` which works fine. Partitioning is an optimization for large-scale deployments where `DROP TABLE` (instant, no VACUUM) is better.

### Schema
Base `schema.sql` defines the table as non-partitioned (the default). When `Partitioning=true`, `EnsureSchema` converts it:

```go
func (e *PostgresMapBroker) EnsureSchema(ctx context.Context) error {
    // ... execute base schema ...
    if e.conf.Partitioning {
        return e.ensurePartitionedStream(ctx)
    }
    return nil
}
```

`ensurePartitionedStream`:
1. Check if table is already partitioned (`pg_partitioned_table` query)
2. If not partitioned: drop and recreate with `PARTITION BY RANGE (created_at)` (fresh DB assumption — no migration needed)
3. PK becomes `PRIMARY KEY (id, created_at)` for partitioned table
4. Create indexes on parent (auto-propagate to partitions)
5. Ensure lookahead partitions

`ensureLookaheadPartitions`: create `{stream}_{YYYY}_{MM}_{DD}` with `IF NOT EXISTS`

`dropOldPartitions`: list via `pg_inherits` + `pg_class`, parse date from suffix, `DROP TABLE IF EXISTS`

---

## Step 7: Remove WAL Reader + Shard Methods

Delete entirely (~500 lines):
- `runWALReaderForShard`, `runWALReaderLoop`, `processWALMessage`, `processInsertMessage`
- `decodePgBytea`
- `IsWALShardClaimed`, `WALClaimedShards`, `IsOutboxShardClaimed`, `OutboxClaimedShards`, `ClaimedShards`
- `runStreamCleanupWorker`, `cleanupExpiredEntries`
- `runOutboxWorkerForShard`, current `processOutboxLoop`
- `ensurePublications`

Remove imports: `github.com/jackc/pglogrepl`, `github.com/jackc/pgx/v5/pgproto3`

---

## Step 8: Simplify `EnsureSchema`

Remove WAL publication branch. Add partitioning call (see Step 6).

---

## Step 9: Update `Publish`, `Remove`, `ReadState`, `ReadStream`

### `Publish` (lines 586-752)
- Remove `streamSize` variable and preparation (lines 690-694)
- Remove `streamTTL` variable and preparation
- Remove both from `pool.QueryRow` args (line 721: `streamTTL, streamSize`)
- Update SQL format string: remove `$12::interval, $13`, renumber `$14` onward
- Total: 26 params → 24 params in SQL call

### `Remove` (lines 755-846)
- Remove `streamTTL` variable and preparation
- Remove from `pool.QueryRow` args, renumber params

### `ReadState` (lines 849-1061)
- Use `pool := e.getReadPool(ch, opts.AllowCached)` — routes to replica when `AllowCached=true`
- Replace `e.pool` with `pool` throughout method

### `ReadStream` (lines 1064-1201)
- Add `AllowCached bool` field to `MapReadStreamOptions` in `map_broker.go` (currently only has `Filter`)
- Use `pool := e.getReadPool(ch, opts.AllowCached)` — replica when allowed
- Replace `e.pool` with `pool` throughout method

### Client subscription flow — set `AllowCached: true` on stream reads
- `client_map.go:583` (stream phase catch-up): add `AllowCached: true` to `MapReadStreamOptions`
- `client_map.go:903` (live phase stream read): add `AllowCached: true` to `MapReadStreamOptions`
- State reads already set `AllowCached: true` (lines 353, 1069 — no change needed)
- Position checks (`node.go:1626` — `ReadStream(Limit:0)`) must NOT set `AllowCached` (needs primary)

### CachedMapBroker compatibility — no consistency issues
- CachedMapBroker **never forwards** `AllowCached` to backend — it always constructs fresh `MapReadStreamOptions` without the flag (lines 246, 451, 712)
- The `AllowCached` from the client request is consumed by CachedMapBroker itself (serve from cache vs. go to backend)
- CachedMapBroker's internal backend reads (gap fill, sync, initial load) always hit PG primary — conservative and correct
- When client sets `AllowCached: true` on the outer call → CachedMapBroker serves from in-memory cache → PG broker not called at all
- **Important**: do NOT modify CachedMapBroker to forward `AllowCached` to backend — this would break consistency (CachedMapBroker receives publications from outbox which may be ahead of a replica)

### `expireKeys` (lines 1724-1783)
- Remove `streamTTL` from the `cf_map_expire_keys` call args

---

## Step 10: Position Checks — Keep As-Is

Position checks **remain active** for PG broker. The BIGSERIAL id gap problem means outbox polling can miss entries: TX1 gets id=100, TX2 gets id=101, TX2 commits first, cursor advances past 100, TX1's entry is lost. Periodic position checks (`ReadStream(Limit:0)` every 40s) catch these gaps and trigger recovery.

No changes to `map_broker.go`, `node.go`, or `client_map.go` for position checks.

---

## Step 11: Update Examples (`_examples/map_demo/`)

### `broker.go`
- Remove `pgConfig.Broker` / Redis fan-out logic (lines 24-44) — PG broker no longer needs Redis
- Add optional replica configuration:
  ```go
  if replicaAddrs != "" {
      pgConfig.ReadReplicaConnStrings = strings.Split(replicaAddrs, ",")
  }
  ```
- Remove `import` of Redis broker when only PG is used (keep Redis for RedisMapBroker path)
- Log replica count if configured

### `docker-compose.yml`
- Remove WAL-level settings from primary (`wal_level=logical`, `max_replication_slots`, `max_wal_senders`) — no longer needed for basic setup
- Add optional `postgres-replica` service (commented out by default) with streaming replication:
  ```yaml
  # Uncomment to enable read replica
  # postgres-replica:
  #   image: postgres:16
  #   environment:
  #     PGUSER: centrifuge
  #     PGPASSWORD: centrifuge
  #   command: >
  #     bash -c "
  #     pg_basebackup -h postgres -U centrifuge -D /var/lib/postgresql/data -Fp -Xs -P -R
  #     && postgres
  #     "
  #   depends_on:
  #     postgres:
  #       condition: service_healthy
  #   ports:
  #     - "5433:5432"
  ```
  When replica is enabled, primary needs `wal_level=replica` and `max_wal_senders` — add these as commented-out config matching the replica service.

### `main.go`
- Add `-replicas` flag: comma-separated replica connection strings
  ```go
  replicas = flag.String("replicas", "", "Comma-separated PostgreSQL replica connection strings")
  ```
- Pass to `setupMapBroker`

### `README.md`
- Update usage instructions: remove Redis requirement for PG broker
- Add replica usage example:
  ```
  # With replica:
  docker compose --profile replica up -d
  go run . -postgres "postgres://..." -replicas "postgres://...@localhost:5433/..."
  ```

---

## Step 12: Test Changes (`map_broker_postgres_test.go`)

### Delete tests (13)
- WAL: `WALReader`, `WALReaderOrdering`, `WALReaderMetadata`, `WALReaderWithBroker`, `ClientInfoDelivery_WAL`, `ClientInfoDelivery_WALWithBroker`
- Broker-dependent: `OutboxWithBroker`, `ClientInfoDelivery_OutboxWithBroker`
- Cursor: `CursorBasedDelivery`
- Schema/WAL: `EnsureSchema_WALPublications`, `EnsureSchema_NumShardsMismatch`, `EnsureSchema_OutboxNoPublications`

### Delete helpers
- `newTestPostgresMapBrokerWithWAL`, `dropStaleReplicationSlots`

### Update helpers
- Remove `AdvisoryLockBaseID` from outbox config

### Add tests
- `TestPostgresMapBroker_ReliableDelivery` — verify interface
- `TestPostgresMapBroker_StreamRetentionCleanup` — verify cleanup

---

## Step 13: Dependency Cleanup

Remove `github.com/jackc/pglogrepl` from `go.mod`. Run `go mod tidy`.

---

## Verification

1. Run benchmarks before refactoring (Step 0) → save to `bench_before.txt`
2. `make pg-schemas` — regenerate SQL files
3. `go build ./...` — compilation
4. `CENTRIFUGE_POSTGRES_URL=... go test -v -run TestPostgresMapBroker ./...` — PG tests
5. Run benchmarks after → save to `bench_after.txt`
6. `benchstat bench_before.txt bench_after.txt` — compare
7. `go test ./...` — all tests
8. `go mod tidy`
9. Verify: no references to `pglogrepl`, `advisory_lock`, `outbox_cursor`, `WALConfig`
