# stress_runtime

A self-contained runtime stress / regression tool for the `centrifuge` server
library. It boots four in-process `Node`s:

- **main** — memory engine, generous limits, wired for the full feature matrix;
- **strict** — memory engine, every limit small enough that a misbehaving client
  trips it;
- **redis-a** and **redis-b** — the Redis engine, sharing one Redis instance and
  one key prefix, so they form a **real two-node cluster**: PUB/SUB fanout,
  history, presence and the control plane all go through Redis.

It hammers them with real
[`centrifuge-go`](https://github.com/centrifugal/centrifuge-go) clients over
WebSocket, with **raw protocol clients** over WebSocket / SSE / HTTP-streaming +
emulation, and with the node's own server-side APIs.

Every feature is a **self-checking scenario** with hard invariants — exact
message counts and ordering, recovered ranges, delta reconstruction,
cache-recovery semantics, idempotency suppression, presence sets, exact protocol
error codes, exact disconnect codes, and no leaked connections or subscriptions.
A green run is strong evidence the runtime is not broken.

It runs hard — hundreds of thousands of connections churned, a megabyte payload,
reconnect storms on both engines — yet the whole suite finishes in about
**20 seconds** (~25s under `-race`) because scenarios run concurrently under a
bounded worker pool.

## Redis is required

A local Redis is **required by default**. If it cannot be reached the suite
fails immediately with a clear message rather than silently losing coverage —
the Redis engine is where most of the interesting concurrency lives, and a green
run is meant to say something about it.

```bash
redis-server            # anything reachable at 127.0.0.1:6379
go run .                # or: -redis host:port
go run . -no-redis      # memory-only run: skips every redis_* scenario
```

Each run uses a fresh key prefix (`stress-<seed>`) and never writes a key
without a TTL, so it leaves nothing permanent behind — everything expires within
about two minutes.

```bash
go run .                       # full suite (~20s)
go run -race .                 # same, race-checking the real server under load
go run . -list                 # list scenario names
go run . -only delta_correctness,cache_recovery
go run . -skip connect_churn,mixed_chaos
go run . -v                    # log server-side disconnects (debugging)
go run . -p 4                  # cap how many scenarios run at once
go run . -load 30s -d 300s     # longer load window / suite deadline
go run . -repeat 5             # repeat the selection (flake hunting)
go run . -seed 12345           # replay a specific randomized run
go run . -redis host:port      # point at a different Redis
go run . -no-redis             # skip the Redis nodes and every redis_* scenario
```

Exit code is non-zero if any scenario fails; each failure prints exactly what
was expected vs. observed and where. Failures are listed first in the report.

## What it checks

### Sustained load

| Scenario | Invariant |
|---|---|
| `connect_churn` | hundreds of thousands of connect/close cycles all succeed |
| `mixed_chaos` | many clients doing random mixed ops produce no errors |

### Core feature matrix

| Scenario | Invariant |
|---|---|
| `pubsub_fanout` | every subscriber gets every publication exactly once, in offset order |
| `subscribe_churn` | subscribe/unsubscribe symmetry; hub retains no subscribers afterward |
| `history_api` | full history is contiguous; `since`-offset returns the correct suffix |
| `positioning` | live publications arrive with strictly increasing, gap-free offsets |
| `recovery_reconnect` | after a forced reconnect the client recovers every missed publication — no gap, no duplicate |
| `delta_correctness` | fossil-delta subscription reconstructs every payload exactly |
| `presence_joinleave` | presence membership, presence stats, and join/leave push events are correct |
| `tags_filter` | client-requested **and** server-enforced tags filters deliver only matching publications |
| `rpc_concurrent` | thousands of concurrent RPC round-trips return the correct echo |
| `client_publish_fanout` | client-initiated publishes reach all other subscribers |
| `same_conn_concurrency` | thousands of concurrent publish/history/presence/RPC ops on one connection all succeed |
| `refresh_connection` | a short-TTL connection stays alive via token refresh, with zero reconnects |
| `ping_pong` | a pinged connection stays alive and functional across many ping/pong cycles |

### Server-driven subscriptions and user-targeted APIs

| Scenario | Invariant |
|---|---|
| `server_side_subs` | `ConnectReply.Subscriptions` deliver pushes and fully recover across a forced reconnect |
| `server_sub_api_churn` | `node.Subscribe`/`node.Unsubscribe` cycles reach every connection of a user; publications land only while subscribed |
| `multi_conn_same_user` | user-targeted subscribe/publish/unsubscribe/refresh fan out to all connections; the disconnect whitelist keeps exactly the right one alive |

### Alternative codecs and transports

| Scenario | Invariant |
|---|---|
| `protobuf_delta_recovery` | Protobuf codec + fossil delta + stream recovery reconstruct every payload exactly |
| `sse_transport` | SSE framing delivers every publication in order |
| `http_stream_emulation` | HTTP-stream downlink plus emulation uplink round-trips subscribe, publish and RPC |

### Broker and stream semantics

| Scenario | Invariant |
|---|---|
| `cache_recovery` | `RecoveryModeCache` delivers the **latest publication only**, on subscribe and on recovery — never the backlog |
| `concurrent_publish_offsets` | thousands of concurrent publishes keep offsets unique and contiguous, delivered in order, with no forced resubscribe |
| `idempotent_publish` | sequential **and** racing duplicates with one idempotency key collapse to a single publication and position |
| `history_pagination` | forward paging and a reverse read both reconstruct the stream exactly; since-top returns nothing |
| `unrecoverable_position` | a position lost past the retention window is reported as unrecovered, and the subscription stays healthy; `RemoveHistory` empties the stream |
| `sub_refresh_expiry` | a 2s-TTL subscription survives several TTL windows via server-side sub refresh |
| `recovery_storm` | subscribers kicked off repeatedly during continuous publishing end with **zero gaps and zero duplicates** |

### Node APIs and payload edges

| Scenario | Invariant |
|---|---|
| `survey_notify` | concurrent `node.Survey` calls and `node.Notify` deliveries round-trip correctly |
| `async_send_echo` | thousands of async messages are echoed back exactly once each |
| `large_payloads` | payloads from 1 KiB to 1 MiB survive fanout, history, delta and a client publish byte-for-byte |
| `many_channels_one_conn` | 2000 channels on one connection all deliver and all drain |
| `error_paths` | denied subscribe/publish/RPC/presence/history return exact protocol codes and leave the connection usable |

### Adversarial clients and limit enforcement

| Scenario | Invariant |
|---|---|
| `malformed_protocol` | 16 invalid/edge-case frames each produce the documented reaction (exact close or error code) — never a hang; a healthy connection alongside is unaffected |
| `stale_connection` | a connection that never authenticates is closed with `3502` |
| `oversized_frame` | a frame over the transport size limit closes the connection |
| `slow_client` | a client that stops reading is dropped (`3008`) and the hub releases its channel |
| `no_pong` | a client that never pongs is dropped with `3012` |
| `channel_limit` | client-side subscribe past the limit → error `106`; too many server-side subs → close `3505` |
| `expired_connection` | a refresh handler reporting expiry closes the connection with `3005`, repeatedly |
| `subscribe_unsubscribe_race` | client-side and server-side subscribe/unsubscribe racing on one channel never wedge it; the channel drains and the connection works |
| `disconnect_during_subscribe` | connections torn down mid-subscribe leave nothing behind in the hub |

### Redis engine (two nodes, one Redis)

Wherever it matters the publisher and the subscriber sit on **different nodes**,
because that is the path that can actually break. Every fanout assertion uses a
positioned/recoverable channel on purpose: Redis PUB/SUB is at-most-once, and
the server's own gap detection plus recovery is exactly what is under test — a
gap that survives to the client is a real defect, not flakiness.

| Scenario | Invariant |
|---|---|
| `redis_cluster_view` | both nodes see each other; surveys are answered by both; broadcast and node-targeted notifications land on exactly the right nodes |
| `redis_pubsub_fanout` | subscribers split across both nodes each receive every publication, in order, exactly once |
| `redis_cross_node_recovery` | a disconnect issued on node A reaches a connection on node B, and everything missed is recovered from the Redis stream with no duplicates |
| `redis_cross_node_control` | subscribe / publish / refresh / unsubscribe / disconnect issued on node A all take effect on node B's connection, with the right disconnect code |
| `redis_presence_cross_node` | presence and presence stats agree from either node; join/leave events cross nodes |
| `redis_delta_recovery` | fossil delta over the Redis broker (previous publication read back from the Redis stream) reconstructs exactly, including across recovery |
| `redis_concurrent_publish_offsets` | publishes racing from **both** nodes into one stream keep offsets unique and contiguous; the subscriber sees every one in order |
| `redis_idempotent_publish` | duplicates racing from both nodes under one idempotency key collapse to a single publication and position |
| `redis_cache_recovery` | `RecoveryModeCache` over Redis delivers latest-only on subscribe and on recovery |
| `redis_history_pagination` | a stream written on node A pages forward and reads back in reverse from node B |
| `redis_recovery_storm` | subscribers on both nodes kicked repeatedly during continuous publishing end with zero gaps and zero duplicates |
| `redis_chaos` | mixed operations from many clients on both nodes for the whole load window, no errors |

### Final

| Scenario | Invariant |
|---|---|
| `no_leaks_final` | after everything closes, **all four** hubs drain to zero connections and channels |
| `no_goroutine_leaks` | the number of goroutines running **centrifuge library code** returns to the post-warm-up baseline |

`no_goroutine_leaks` deliberately does not look at `runtime.NumGoroutine()` — the
harness, the SDK and `net/http` keep goroutines of their own and that number
moves for reasons unrelated to the library. Instead it counts only goroutines
whose stack is inside `github.com/centrifugal/centrifuge` (the SDK's
`centrifuge-go` and the harness's own `main` are excluded by construction), and
compares against a baseline taken after a warm-up that connects, subscribes,
publishes, reads presence and history and closes on **every** node — so anything
the library starts lazily on first use is already counted.

Measured sensitivity: 100 open connections add exactly 200 library goroutines
(`internal/websocket.(*Conn).read` and `internal/queue.(*Queue).Wait`, one each
per connection) and the count returns to the baseline when they close. Since
`connect_churn` alone opens hundreds of thousands of connections, even a rare
per-connection leak shows up as a large number. On failure the check names the
frames that grew, biggest first — that list *is* the leak.

## Notes

- The servers are in-process, so `go run -race .` race-checks the **real
  centrifuge server** under the full concurrent feature load — a useful gate on
  its own.
- Layout: `main.go` (flags, worker pool, reporting), `server.go` (all node
  configs, storage wiring and event handlers), `client.go` (SDK client helpers
  and bounded wait helpers), `rawproto.go` (raw WebSocket / SSE / HTTP-stream /
  emulation clients), and four scenario files — `scenarios_core.go`,
  `scenarios_advanced.go`, `scenarios_adversarial.go`, `scenarios_redis.go`.
  Adding a check means writing one function and appending it to `allScenarios`
  in `main.go`; a name starting with `redis_` is dropped automatically under
  `-no-redis`.
- Channel behaviour is selected by name prefix (`recov:`, `delta:`, `pos:`,
  `cache:`, `pres:`, `plain:`, `tags:`, `stags:`, `subexp:`, `deny:`, `nopub:`)
  via `subOptions`, and per-connection behaviour by user-id prefix (`refresh:`,
  `expire:`, `ping:`, `ssub:<channel>[,<channel>…]`), so one set of handlers
  serves every scenario.
- Every wait in the suite is bounded, and every scenario has its own timeout, so
  a hang fails with a precise message instead of consuming the suite deadline.
- The goroutine baseline is printed at startup, so a change in the library's
  steady-state goroutine count is visible run to run.
- Two counters matter when asserting a connection stayed up: the SDK reports
  *reconnecting* drops as `Connecting` events and only terminal ones as
  `Disconnected`, so the harness watches `connectings`, not `disconnects`.
- Still **not** covered: Redis Cluster and Sentinel topologies, sharded PUB/SUB,
  multiple Redis shards, the map broker / keyed / shared-poll subsystem,
  graceful shutdown and drain, JWT auth, compression, WebTransport and
  unidirectional GRPC.
