# stress_runtime

A self-contained runtime stress / regression tool for the `centrifuge` server
library. It boots an in-process `Node` wired for the full feature matrix and
hammers it with real [`centrifuge-go`](https://github.com/centrifugal/centrifuge-go)
clients over WebSocket. Every feature is a **self-checking scenario** with hard
invariants — so a green run is strong evidence the runtime is not broken.

It runs hard — the sustained scenarios churn ~350k connections and run tens of
concurrent clients for ~18s — but the whole suite stays well under a minute
because scenarios run in parallel.

```bash
go run .            # run the whole suite (~18s)
go run -race .      # same, with the race detector on the in-process server
go run . -v         # also log server-side disconnects (debugging)
go run . -only delta_correctness   # run a single scenario
go run . -d 60s     # change the overall suite deadline
```

The suite runs all scenarios **in parallel** and finishes well under a minute.
Exit code is non-zero if any scenario fails; each failure prints exactly what
was expected vs. observed and where.

## What it checks

| Scenario | Invariant |
|---|---|
| `connect_churn` | many connect/close cycles all succeed |
| `pubsub_fanout` | every subscriber gets every publication exactly once, in offset order |
| `subscribe_churn` | subscribe/unsubscribe symmetry; hub retains no subscribers afterward |
| `history_api` | full history is contiguous; `since`-offset returns the correct suffix |
| `positioning` | live publications arrive with strictly increasing, gap-free offsets |
| `recovery_reconnect` | after a forced reconnect the client recovers every missed publication — no gap, no duplicate |
| `delta_correctness` | fossil-delta subscription reconstructs every payload exactly |
| `presence_joinleave` | presence membership, presence stats, and join/leave push events are correct |
| `tags_filter` | client-requested **and** server-enforced tags filters deliver only matching publications |
| `rpc_concurrent` | many concurrent RPC round-trips return the correct echo |
| `client_publish_fanout` | client-initiated publishes reach all other subscribers |
| `same_conn_concurrency` | hundreds of concurrent publish/history/presence/RPC ops on one connection all succeed |
| `refresh_connection` | a short-TTL connection stays alive via token refresh |
| `ping_pong` | a fast-pinged connection stays alive and functional across many ping/pong cycles (a broken pong drops it server-side, a broken ping drops it client-side) |
| `mixed_chaos` | many clients doing random mixed ops produce no errors |
| `no_leaks_final` | after everything closes, the hub drains to zero connections and channels |

## Notes

- The server is in-process, so `go run -race .` race-checks the **real
  centrifuge server** under the full concurrent feature load — a useful gate on
  its own.
- The whole harness is one file (`main.go`): the server config lives in
  `buildNode`, and each scenario is a small self-contained function. Add a
  feature check by writing one more scenario and appending it to the list in
  `main`.
- Channel behaviour is selected by name prefix (`recov:`, `delta:`, `pres:`,
  `tags:`, `stags:`, `pos:`) via `subOptions`, so one `OnSubscribe` handler
  serves every scenario.
