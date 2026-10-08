#!/bin/sh
# Usage: sh tla/run.sh <spec> <config> [extra TLC arguments]
#   e.g. sh tla/run.sh client MC_small
#
# Runs TLC on one config of a spec (tla/<spec>/configs/<config>.cfg, the .cfg
# suffix is optional). The full output goes to tla/out/<spec>/<config>.txt; the
# summary is printed: the violated property, the actions of the counterexample
# (if the spec records them in an `act` variable), state counts and time. Exits
# with TLC's exit code (0: no error, 12: invariant violated, 13: temporal
# property violated, 11: deadlock, other: error in the spec or the run).
#
# Environment:
#   JAVA         java binary (default: java)
#   TLA2TOOLS    path of tla2tools.jar (default: tla/tla2tools.jar, downloaded
#                from the TLA+ v1.7.4 release if missing)
#   TLC_WORKERS  number of TLC workers (default: auto)
set -u
if [ $# -lt 2 ]; then
    echo "usage: sh tla/run.sh <spec> <config> [TLC arguments]" >&2
    exit 2
fi
TLA_DIR=$(cd "$(dirname "$0")" && pwd)
spec=$1
cfg=$(basename "$2" .cfg)
shift 2
SPEC_DIR="$TLA_DIR/$spec"
if [ ! -f "$SPEC_DIR/spec.conf" ]; then
    echo "unknown spec: $spec" >&2
    exit 2
fi
MODULE=
. "$SPEC_DIR/spec.conf"
if [ ! -f "$SPEC_DIR/configs/$cfg.cfg" ]; then
    echo "unknown config: $spec/configs/$cfg.cfg" >&2
    exit 2
fi

JAVA=${JAVA:-java}
TLA2TOOLS=${TLA2TOOLS:-$TLA_DIR/tla2tools.jar}
TLC_VERSION=v1.7.4
if [ ! -f "$TLA2TOOLS" ]; then
    echo "downloading tla2tools.jar $TLC_VERSION to $TLA2TOOLS" >&2
    curl -fsSL -o "$TLA2TOOLS.tmp" \
        "https://github.com/tlaplus/tlaplus/releases/download/$TLC_VERSION/tla2tools.jar" &&
        mv "$TLA2TOOLS.tmp" "$TLA2TOOLS" || { rm -f "$TLA2TOOLS.tmp"; exit 1; }
fi
case "$TLA2TOOLS" in /*) ;; *) TLA2TOOLS="$(pwd)/$TLA2TOOLS" ;; esac

OUT_DIR="$TLA_DIR/out/$spec"
mkdir -p "$OUT_DIR"
cd "$SPEC_DIR" || exit 1
"$JAVA" -XX:+UseParallelGC -cp "$TLA2TOOLS" tlc2.TLC -workers "${TLC_WORKERS:-auto}" \
    -metadir "$OUT_DIR/states_$cfg" "$@" -config "configs/$cfg.cfg" "$MODULE.tla" \
    > "$OUT_DIR/$cfg.txt" 2>&1
code=$?
rm -rf "$OUT_DIR/states_$cfg"
grep -E "^Error:|is violated|^/\\\\ act = |states generated|depth of|Finished in|No error|Deadlock" "$OUT_DIR/$cfg.txt" |
    grep -v "behavior up to\|Progress(" | sed -E 's/^\/\\ act = /  -> /'
exit $code
