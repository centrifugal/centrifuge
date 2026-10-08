# TLA+ specs

Formal models of parts of Centrifuge, checked with the TLC model checker.

| Spec | What |
|---|---|
| [`client`](client/README.md) | the client subscription protocol: subscribe/unsubscribe of a connection, handler calls, frames on the connection, recovery buffer, presence and join/leave |

## Running

```sh
make tla                                                   # default set of every spec
make tla SPEC=client                                       # default set of one spec
make tla SPEC=client CONFIGS=all                           # every config of a spec
make tla SPEC=client CONFIGS="MC_small bug_no_join_gate"   # chosen configs
sh tla/run.sh client MC_small                              # one config, prints a counterexample
```

`make tla` runs `sh tla/check.sh [spec [all | config ...]]`. Configs named
`MC_*` must pass and `bug_*` configs (old, fixed behaviours) must fail; the
check fails on any other result. The full TLC output of a config is in
`tla/out/<spec>/<config>.txt`.

Each spec lives in its own directory with a `README.md`, its modules, a
`gen_configs.py` which writes `configs/*.cfg`, and `spec.conf` naming the main
module and the default set.

## Requirements

Java 11 or newer, and `curl` on first use: `run.sh` downloads `tla2tools.jar`
(TLA+ release v1.7.4) to `tla/tla2tools.jar` if it is missing.

Environment: `JAVA` (default `java`), `TLA2TOOLS` (path of the jar),
`TLC_WORKERS` (default `auto`).
