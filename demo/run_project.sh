#!/usr/bin/env bash
set -euo pipefail

repo=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
zip=${1:-}
[[ $# == 1 && -f $zip ]] || { echo 'Usage: bash demo/run_project.sh <TailSignal ZIP>' >&2; exit 2; }
export HADOOP_HOME=${HADOOP_HOME:-/usr/share/hadoop}
export HADOOP_CONF_DIR=${HADOOP_CONF_DIR:-$HADOOP_HOME/etc/hadoop}
export SPARK_HOME=${SPARK_HOME:-/usr/share/spark}
export ISIT312_HDFS_ROOT=${ISIT312_HDFS_ROOT:-/isit312/vet}
export ISIT312_LOG_DIR=${ISIT312_LOG_DIR:-$repo/logs}
export NOTE_LOG_DIR=$ISIT312_LOG_DIR
root=${ISIT312_HDFS_ROOT%/}
batch=project-$(date -u +%Y%m%dT%H%M%S)-$$
db=${ISIT312_HIVE_DB:-isit312_$(printf '%s' "$batch" | tr '-' '_')}
[[ $db =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]] || { echo 'Invalid Hive database name' >&2; exit 2; }
hdfs=$HADOOP_HOME/bin/hdfs
beeline=${BEELINE_BIN:-${HIVE_HOME:+$HIVE_HOME/bin/}beeline}
if ! command -v "$beeline" >/dev/null && [[ -x /usr/share/hive/bin/beeline ]]; then
    beeline=/usr/share/hive/bin/beeline
fi
[[ -x $hdfs && -x $SPARK_HOME/bin/spark-shell ]] || { echo 'Set HADOOP_HOME and SPARK_HOME to the course VM paths.' >&2; exit 1; }
command -v "$beeline" >/dev/null || { echo 'Set HIVE_HOME or BEELINE_BIN.' >&2; exit 1; }
fs=$("$hdfs" getconf -confKey fs.defaultFS)
[[ $fs == hdfs://* ]] || { printf 'Expected HDFS, found %s\n' "$fs" >&2; exit 1; }
accepted="$root/accepted/$batch"
mkdir -p "$ISIT312_LOG_DIR"
log="$ISIT312_LOG_DIR/$batch.log"
stage=$(mktemp -d)
trap 'rm -rf -- "$stage"' EXIT
hive=("$beeline" --force=false -u "${ISIT312_HIVE_URL:-jdbc:hive2://localhost:10000}" --hivevar "db=$db")

{
    printf 'Commit: %s\nBatch: %s\nLog: %s\n' "$(git -C "$repo" rev-parse --short HEAD 2>/dev/null || echo unknown)" "$batch" "$log"
    "$hdfs" dfs -ls / >/dev/null
    "${hive[@]}" -e 'SELECT 1;' >/dev/null

    echo '=== HDFS ingestion ==='
    unzip -q "$zip" -d "$stage"
    bash "$repo/ingest/land_batch.sh" --source "$stage" --hdfs-root "$root" \
        --batch-id "$batch" --manifest "$repo/manifests/tailsignal_2026_11_22.tsv"
    "$hdfs" dfs -test -f "$root/raw/$batch/_READY"
    "$hdfs" dfs -ls "$root/raw/$batch"

    echo '=== Practice parser and clinical integration ==='
    RUN_PARSER=1 bash "$repo/spark/run_integrate_clinical.sh" full "$batch"
    "$hdfs" dfs -ls "$accepted"

    echo '=== Clinical note extraction and held-out evaluation ==='
    bash "$repo/spark/run_note_mentions.sh" batch "$batch" "$zip"

    echo '=== Hive reports and visit drill-down ==='
    "${hive[@]}" --hivevar "accepted=$accepted" \
        --hivevar "exceptions=$root/exceptions/$batch/integrate_clinical" \
        -f "$repo/sql/hive/integration_tables.sql"
    "${hive[@]}" -f "$repo/sql/hive/project_report.sql"
    "${hive[@]}" -f "$repo/sql/hive/integration_checks.sql"

    echo '=== Incremental correction ==='
    bash "$repo/spark/run_replay.sh" "$batch-replay" "${fs%/}$accepted/visits" \
        "$repo/fixtures/replay/02_correction.csv"

    echo '=== Integration regression ==='
    regression="$ISIT312_LOG_DIR/$batch-regression.log"
    export ISIT312_BASELINE="${fs%/}$accepted" ISIT312_CHECKS_SQL="$repo/sql/checks/integration.sql"
    printf ':paste %s\n:quit\n' "$repo/tests/run_integration_checks.sc" \
        | "$SPARK_HOME/bin/spark-shell" --master "${SPARK_MASTER:-local[2]}" \
          --conf spark.ui.showConsoleProgress=false 2>&1 | tee "$regression"
    tr '\r' '\n' < "$regression" | grep -Fx ISIT312_REGRESSION_PASS >/dev/null
    printf 'ISIT312_PROJECT_PASS\nBatch: %s\nAccepted: %s\nHive database: %s\nLog: %s\n' \
        "$batch" "${fs%/}$accepted" "$db" "$log"
} 2>&1 | tee "$log"
