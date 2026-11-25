#!/usr/bin/env bash
# Applies one batch of visit change events to a saved visits snapshot.
#
#   bash spark/run_replay.sh <batch id> <prior visits snapshot> [local events CSV]
#
# With a local CSV the script first copies it to <root>/raw/<batch id>/replay/visit_events.csv.
# Without one, that HDFS file must already exist. The new snapshot is written to
# <root>/accepted/<batch id>/visits. Set ISIT312_HDFS_ROOT (default /isit312/vet) to override.
set -euo pipefail

if [[ $# -lt 2 ]]; then
    printf 'Usage: bash spark/run_replay.sh <batch id> <prior visits snapshot> [local events CSV]\n' >&2
    exit 2
fi
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

export BATCH_ID=$1
export REPLAY_PRIOR=$2
export ISIT312_HDFS_ROOT=${ISIT312_HDFS_ROOT:-/isit312/vet}
export REPLAY_INPUT="$ISIT312_HDFS_ROOT/raw/$BATCH_ID/replay/visit_events.csv"

if [[ $# -ge 3 ]]; then
    if ! hdfs_bin=$(find_tool hdfs "${HADOOP_HOME:-}" \
        "$HOME"/hadoop* /usr/local/hadoop* /opt/hadoop* /usr/share/hadoop); then
        printf 'Hadoop client not found. Set HADOOP_HOME to the course Hadoop directory and rerun.\n' >&2
        exit 1
    fi
    fs=$("$hdfs_bin" getconf -confKey fs.defaultFS)
    [[ $fs == hdfs://* ]] || { printf 'Expected HDFS, found %s\n' "$fs" >&2; exit 1; }
    "$hdfs_bin" dfs -mkdir -p "$(dirname "$REPLAY_INPUT")"
    if "$hdfs_bin" dfs -test -f "$REPLAY_INPUT"; then
        existing=$("$hdfs_bin" dfs -cat "$REPLAY_INPUT" | sha256sum)
        supplied=$(sha256sum < "$3")
        if [[ $existing != "$supplied" ]]; then
            printf 'Replay input differs for batch %s. Use a new batch ID.\n' "$BATCH_ID" >&2
            exit 1
        fi
    else
        "$hdfs_bin" dfs -put "$3" "$REPLAY_INPUT"
    fi
fi

log_dir=${ISIT312_LOG_DIR:-$repo/spark}
mkdir -p "$log_dir"
log="$log_dir/replay_${BATCH_ID}.log"
printf 'Batch: %s\nPrior: %s\nInput: %s\nLog: %s\n' "$BATCH_ID" "$REPLAY_PRIOR" "$REPLAY_INPUT" "$log"
printf ':paste %s\n:quit\n' "$repo/spark/replay_snapshot.sc" \
    | "$spark_shell" --master "${SPARK_MASTER:-local[2]}" --conf spark.ui.showConsoleProgress=false 2>&1 | tee "$log"
# The Spark shell can return zero after a Scala exception. Require the final marker.
tr '\r' '\n' < "$log" | grep -Fx ISIT312_REPLAY_DONE > /dev/null
