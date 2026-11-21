#!/usr/bin/env bash
set -euo pipefail

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
archive=${1:-"$here/../../tailsignal_vet_dataset.zip"}
batch="smoke-$(date -u +%Y%m%dT%H%M%S)-$$"
hdfs_root=${ISIT312_HDFS_ROOT:-/isit312/vet}

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

if ! hdfs_bin=$(find_tool hdfs "${HADOOP_HOME:-}" \
    "$HOME"/hadoop* /usr/local/hadoop* /opt/hadoop*); then
    printf 'Hadoop client not found. Set HADOOP_HOME to the course Hadoop directory and rerun.\n' >&2
    exit 1
fi
if ! spark_shell=$(find_tool spark-shell "${SPARK_HOME:-}" \
    "$HOME"/spark* /usr/local/spark* /opt/spark*); then
    printf 'Spark shell not found. Set SPARK_HOME to the course Spark directory and rerun.\n' >&2
    exit 1
fi
printf 'HDFS client: %s\nSpark shell: %s\n' "$hdfs_bin" "$spark_shell"

fs=$("$hdfs_bin" getconf -confKey fs.defaultFS)
if [[ $fs != hdfs://* ]]; then
    printf 'Expected an HDFS configuration, got %s. Check the course Hadoop configuration.\n' "$fs" >&2
    exit 1
fi
if ! "$hdfs_bin" dfs -ls / > /dev/null; then
    printf 'Cannot reach HDFS at %s. Start Hadoop as shown in the course lab and rerun.\n' "$fs" >&2
    exit 1
fi

stage=$(mktemp -d)
trap 'rm -rf -- "$stage"' EXIT

unzip -q "$archive" -d "$stage"
fs=${fs%/}
raw="$hdfs_root/raw/$batch"
"$hdfs_bin" dfs -mkdir -p "$raw/data" "$raw/derived" "$hdfs_root/accepted"
"$hdfs_bin" dfs -put "$stage/data/pims" "$stage/data/ehr" "$raw/data/"
"$hdfs_bin" dfs -put "$stage/manifest.json" "$raw/"
"$hdfs_bin" dfs -put "$stage/derived/visit_crosswalk.csv" "$raw/derived/"

export ISIT312_RAW_URI="$fs$raw"
export ISIT312_ACCEPTED_URI="$fs$hdfs_root/accepted/$batch"
export ISIT312_CHECKS_SQL="$here/../sql/checks/integration.sql"
log="$here/first_visit_${batch}.log"
printf 'Raw: %s\nAccepted: %s\nLog: %s\n' "$ISIT312_RAW_URI" "$ISIT312_ACCEPTED_URI" "$log"
printf ':paste %s\n:quit\n' "$here/first_visit.sc" | "$spark_shell" --master local[2] 2>&1 | tee "$log"
# The Spark shell can return zero after a Scala exception. Require the final marker.
grep -q '^ISIT312_SMOKE_PASS$' "$log"
