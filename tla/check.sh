#!/bin/sh
# Usage: sh tla/check.sh [<spec> [all | config ...]]
#
#   sh tla/check.sh                          the default set of every spec
#   sh tla/check.sh client                   the default set of the client spec
#   sh tla/check.sh client all               every config of the client spec
#   sh tla/check.sh client MC_small bug_no_join_gate
#
# `make tla` runs it (make tla SPEC=client CONFIGS="..."). Each MC_* config must
# pass, each bug_* config (an old behaviour) must fail with a property violation
# (TLC exit code 12 or 13). Prints the time and result of each config and exits
# non-zero on any unexpected result. Uses the same environment as run.sh (JAVA, TLA2TOOLS,
# TLC_WORKERS). The default set of a spec is DEFAULT_CONFIGS in its spec.conf.
set -u
TLA_DIR=$(cd "$(dirname "$0")" && pwd)

configs_of() { # spec [all | config ...]
    spec=$1
    shift
    DEFAULT_CONFIGS=
    . "$TLA_DIR/$spec/spec.conf"
    if [ $# -eq 0 ]; then
        echo $DEFAULT_CONFIGS
    elif [ "$1" = "all" ]; then
        (cd "$TLA_DIR/$spec/configs" && ls MC_*.cfg bug_*.cfg | sed 's/\.cfg$//')
    else
        echo "$@"
    fi
}

if [ $# -eq 0 ]; then
    SPECS=$(cd "$TLA_DIR" && for d in */; do [ -f "$d/spec.conf" ] && echo "${d%/}"; done)
else
    SPECS=$1
    shift
    if [ ! -f "$TLA_DIR/$SPECS/spec.conf" ]; then
        echo "unknown spec: $SPECS" >&2
        exit 2
    fi
fi

failed=""
for spec in $SPECS; do
    for cfg in $(configs_of "$spec" "$@"); do
        cfg=$(basename "$cfg" .cfg)
        start=$(date +%s)
        sh "$TLA_DIR/run.sh" "$spec" "$cfg" > /dev/null
        code=$?
        took=$(($(date +%s) - start))
        case "$cfg" in
        MC_*) if [ "$code" -eq 0 ]; then result=pass; else result="UNEXPECTED: fails (exit $code)"; fi ;;
        *) if [ "$code" -eq 12 ] || [ "$code" -eq 13 ]; then result="fails as expected"; else result="UNEXPECTED: exit $code"; fi ;;
        esac
        printf '%-50s %5ss  %s\n' "$spec/$cfg" "$took" "$result"
        case "$result" in UNEXPECTED*)
            failed="$failed $spec/$cfg"
            [ -f "$TLA_DIR/out/$spec/$cfg.txt" ] && tail -30 "$TLA_DIR/out/$spec/$cfg.txt"
            ;;
        esac
    done
done
if [ -n "$failed" ]; then
    echo "unexpected results:$failed"
    exit 1
fi
echo "all results as expected"
