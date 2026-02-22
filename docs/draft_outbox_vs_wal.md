Scalability Analysis: Three Architectures

Shared Constraints

- Write throughput ceiling: ~20K pub/s on a single PG master (CPU-bound on shard locks + WAL flush)
- Variable: number of Centrifuge nodes (C), channels, subscribers

  ---
Case 1: Current — Every Node Polls Stream Table (16 workers each)

How it works: Each of C Centrifuge nodes runs 16 outbox workers. Every worker polls its shard from the stream table using SELECT ... WHERE id > cursor AND
shard_id = ANY($1).

DB load per poll cycle (idle):

┌─────────────────────┬──────────┬─────────────────┐
│      Component      │ Per node │ Total (C nodes) │
├─────────────────────┼──────────┼─────────────────┤
│ Outbox poll queries │ 16/cycle │ 16×C / cycle    │
├─────────────────────┼──────────┼─────────────────┤
│ LISTEN connections  │ 1        │ C               │
└─────────────────────┴──────────┴─────────────────┘

DB load under 20K pub/s (non-idle, workers loop continuously):
- Each pub triggers pg_notify → all C nodes wake up → all 16×C workers query simultaneously
- Each worker reads the same rows from the stream table (filtered by its shard)
- Total queries: ~16×C per batch (every few ms under load)
- For C=10 nodes: 160 concurrent polling queries, all hitting the same stream table
- For C=50 nodes: 800 concurrent polling queries

Read amplification: Every row is read C times (once per node). At 20K pub/s with C=10 nodes, that's 200K row-reads/s from the stream table.

Position checks (without SharedPositionSync):
- Each client checks every 40s: SELECT top_offset, epoch FROM cf_map_meta
- 1M clients × 1 channel each = 25K queries/s to meta table
- With SharedPositionSync: 1 query per channel per 40s = negligible

State/stream reads (subscribe, recovery):
- Routed to replicas when AllowCached=true
- But outbox workers hit primary (or replicas, but independently per node)

Bottleneck: The stream table becomes a hot read path — C×16 workers all polling the same index range. PG's shared buffer cache helps, but connection count
and query processing overhead scale linearly with C.

Practical ceiling: Works well up to ~10-20 nodes. Beyond that, connection pool exhaustion and query scheduling overhead on PG become the limiting factor.
Not the query cost itself (it's a simple index scan), but the sheer concurrency.

  ---
Case 2: Advisory Locks — 16 Workers Total Across All Nodes, Poll from Replicas → Redis

How it works: Instead of every node running its own 16 workers, use pg_advisory_lock(shard_id) so that exactly one worker per shard exists cluster-wide.
That worker reads the outbox from a replica and publishes to Redis. All other nodes receive data via Redis PUB/SUB.

DB load per poll cycle:

┌──────────────────────────┬─────────────────────────┐
│        Component         │      Total (any C)      │
├──────────────────────────┼─────────────────────────┤
│ Outbox poll queries      │ 16 (fixed!)             │
├──────────────────────────┼─────────────────────────┤
│ Advisory lock heartbeats │ ~16                     │
├──────────────────────────┼─────────────────────────┤
│ Redis PUB/SUB            │ Standard Redis overhead │
└──────────────────────────┴─────────────────────────┘

DB load under 20K pub/s:
- 16 workers total, each polls its shard: 16 queries per batch cycle
- Each row read exactly once (1× amplification, vs C× in Case 1)
- At 20K pub/s: 20K row-reads/s (vs 200K for 10 nodes in Case 1)

Read from replicas: Workers read the outbox from replicas, so zero read load on master for delivery. Master only handles writes.

Position checks: Same as before — use SharedPositionSync or hit replicas.

State/stream reads for clients: From replicas as before.

What changes:
- Need advisory lock acquisition/release logic
- Need failover: if the node holding lock for shard 5 dies, another node must acquire it
- Need Redis infrastructure for PUB/SUB fan-out
- Slightly higher latency: outbox poll → Redis publish → Redis subscribe → deliver (extra network hop)

The shard lock question: You still need shard locks on the write path (FOR UPDATE in cf_map_publish). That's about write ordering, not delivery. The
advisory locks are only for coordinating which node does the delivery polling.

Can you read outbox from replicas? Yes, with one caveat. The cursor-based approach (id > cursor) works because shard locks guarantee no gaps. But on a
replica, there's replication lag — a committed row on master might not be visible on replica yet. This means:
- Delivery latency = replication lag + poll interval (instead of just poll interval)
- No correctness issue: rows appear in order, just delayed
- Typical async replication lag: 1-10ms → negligible impact

Practical ceiling: The PG read load is constant regardless of C. Adding Centrifuge nodes only adds Redis subscriptions (which Redis handles trivially).
Scales to 100+ nodes easily.

  ---
Case 3: WAL Reader — Like Case 2 but Read WAL Instead of Outbox

How it works: A logical replication slot reads WAL changes to the stream table. Publishes to Redis. No polling at all.

DB load:

┌─────────────────────┬──────────────┐
│      Component      │    Total     │
├─────────────────────┼──────────────┤
│ WAL streaming       │ 1 connection │
├─────────────────────┼──────────────┤
│ Advisory locks      │ 0            │
├─────────────────────┼──────────────┤
│ Outbox poll queries │ 0            │
└─────────────────────┴──────────────┘

Advantages over Case 2:
- Zero query overhead (WAL is a stream, not a poll)
- Lower latency (WAL delivery is push-based, no poll interval)
- No advisory lock coordination needed (single WAL reader)
- No index scans on stream table at all

Disadvantages vs Case 2:
- Logical replication slot consumes WAL and prevents WAL cleanup until consumed
- If WAL reader falls behind or disconnects, WAL retention grows → disk pressure
- More complex implementation (logical decoding protocol vs simple SQL)
- Operational burden: replication slots need monitoring
- Harder to shard (one slot reads all shards unless you use publication filters)
- Failover is trickier (replication slot is tied to a connection)

The real question: is it worth it over Case 2?

At 20K pub/s, 16 simple index-scan queries running continuously on a replica is trivial load. The replica is already doing the heavy lifting of applying WAL
changes — adding 16 polling queries on top is negligible. The efficiency gain from WAL is real but small in absolute terms.

  ---
Summary Matrix

┌───────────────────────────┬────────────────────────────────────────┬───────────────────────────────────────┬───────────────────────────────────────┐
│         Dimension         │            Case 1 (Current)            │  Case 2 (Advisory + Replica + Redis)  │         Case 3 (WAL + Redis)          │
├───────────────────────────┼────────────────────────────────────────┼───────────────────────────────────────┼───────────────────────────────────────┤
│ Outbox read queries/s     │ 16 × C × (pub_rate / batch_size)       │ 16 × (pub_rate / batch_size)          │ 0                                     │
├───────────────────────────┼────────────────────────────────────────┼───────────────────────────────────────┼───────────────────────────────────────┤
│ Row reads/s at 20K pub/s  │ 20K × C                                │ 20K                                   │ 20K (via WAL)                         │
├───────────────────────────┼────────────────────────────────────────┼───────────────────────────────────────┼───────────────────────────────────────┤
│ Master read load          │ High (default)                         │ Zero (reads from replicas)            │ Zero                                  │
├───────────────────────────┼────────────────────────────────────────┼───────────────────────────────────────┼───────────────────────────────────────┤
│ Scales with C nodes?      │ No (linear growth)                     │ Yes (constant DB load)                │ Yes (constant)                        │
├───────────────────────────┼────────────────────────────────────────┼───────────────────────────────────────┼───────────────────────────────────────┤
│ Delivery latency          │ poll_interval (~50ms) or NOTIFY (<1ms) │ repl_lag + poll_interval (~5-60ms)    │ repl_lag (for Redis PUB/SUB, ~1ms)    │
├───────────────────────────┼────────────────────────────────────────┼───────────────────────────────────────┼───────────────────────────────────────┤
│ Shard locks still needed? │ Yes                                    │ Yes (write path unchanged)            │ Could skip (WAL is ordered)           │
├───────────────────────────┼────────────────────────────────────────┼───────────────────────────────────────┼───────────────────────────────────────┤
│ Implementation complexity │ Exists today                           │ Medium (advisory locks, Redis pub)    │ High (logical decoding, slot mgmt)    │
├───────────────────────────┼────────────────────────────────────────┼───────────────────────────────────────┼───────────────────────────────────────┤
│ Failure mode              │ Node crash → other nodes still deliver │ Lock released → another node picks up │ Slot disconnect → WAL retention grows │
├───────────────────────────┼────────────────────────────────────────┼───────────────────────────────────────┼───────────────────────────────────────┤
│ Operational overhead      │ Low                                    │ Low-Medium                            │ Medium-High (slot monitoring)         │
└───────────────────────────┴────────────────────────────────────────┴───────────────────────────────────────┴───────────────────────────────────────┘
