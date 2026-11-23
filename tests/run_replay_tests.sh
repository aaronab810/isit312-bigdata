#!/usr/bin/env bash
# Runs the replay fixtures through spark/replay_snapshot.sc in the course VM's Spark shell.
#
#   bash tests/run_replay_tests.sh                    # start from fixtures/replay/base_visits.csv
#   bash tests/run_replay_tests.sh <accepted folder>  # start from the baseline, e.g. the smoke run's Accepted path
#
# Set ISIT312_HDFS_ROOT (default /isit312/vet), REPLAY_RUN_ID or SPARK_MASTER (default local[2]) to override.
set -euo pipefail

repo=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)

find_tool() {
    local name=$1 home=$2 candidate
    shift 2
    if [[ -n $home && -x $home/bin/$name ]]; then
        printf '%s\n' "$home/bin/$name"
        return
    fi
    if type -P "$name"; then
        return
    fi
    for candidate in "$@"; do
        if [[ -x $candidate/bin/$name ]]; then
            printf '%s\n' "$candidate/bin/$name"
            return
        fi
    done
    return 1
}

if ! spark_shell=$(find_tool spark-shell "${SPARK_HOME:-}" \
    "$HOME"/spark* /usr/local/spark* /opt/spark* /usr/share/spark); then
    printf 'Spark shell not found. Set SPARK_HOME to the course Spark directory and rerun.\n' >&2
    exit 1
fi

export ISIT312_REPO=$repo
export ISIT312_HDFS_ROOT=${ISIT312_HDFS_ROOT:-/isit312/vet}
export REPLAY_RUN_ID=${REPLAY_RUN_ID:-replay-test-$(date -u +%Y%m%dT%H%M%S)}
if [[ $# -gt 0 ]]; then
    export ISIT312_BASELINE=$1
else
    unset ISIT312_BASELINE
fi
# The job script only defines runReplay(...) when REPLAY_PRIOR is unset.
unset REPLAY_PRIOR BATCH_ID REPLAY_INPUT

log="$repo/tests/${REPLAY_RUN_ID}.log"
printf 'Spark shell: %s\nRun: %s\nBaseline: %s\nLog: %s\n' \
    "$spark_shell" "$REPLAY_RUN_ID" "${ISIT312_BASELINE:-fixtures/replay/base_visits.csv}" "$log"
printf ':paste %s\n:paste %s\n:quit\n' "$repo/spark/replay_snapshot.sc" "$repo/tests/run_replay_tests.sc" \
    | "$spark_shell" --master "${SPARK_MASTER:-local[2]}" --conf spark.ui.showConsoleProgress=false 2>&1 | tee "$log"
# The Spark shell can return zero after a Scala exception. Require the final marker.
grep -q '^ISIT312_REPLAY_PASS$' "$log"
