#!/usr/bin/env bash
# Runs the clinical integration job in Scala spark-shell and saves a log (owner: Hani).
#
#   bash spark/run_integrate_clinical.sh full    <batch_id>  link the batch using Alina's normalized practice visits
#   bash spark/run_integrate_clinical.sh fixture <batch_id>  use fixtures/practice_visits.csv, write to <batch_id>-fixture
#   bash spark/run_integrate_clinical.sh faults  <batch_id>  inject faults into a small copy and check each one is caught
#
# The raw batch must already be on HDFS under $ISIT312_HDFS_ROOT/raw/<batch_id>. smoke/run_first_visit.sh
# prints it as "Raw:". In full mode, RUN_PARSER=1 runs and checks the parser before starting integration.
# Optional: ISIT312_HDFS_ROOT (default /isit312/vet), ISIT312_LOG_DIR (default logs/), SPARK_MASTER (default local[2]),
# SPARK_SHUFFLE_PARTITIONS (default 16; Spark's 200 is slow for 61,527 visits on a two-core VM).
set -euo pipefail

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repo=$(cd "$here/.." && pwd)
mode=${1:-}
batch=${2:-}

if [[ ! $mode =~ ^(full|fixture|faults)$ || -z $batch ]]; then
    printf 'Usage: %s <full|fixture|faults> <batch_id>\n' "$0" >&2
    exit 1
fi
if [[ ! $batch =~ ^[A-Za-z0-9._-]+$ ]]; then
    printf 'Batch ID has unsupported characters: %s\n' "$batch" >&2
    exit 1
fi

find_spark_shell() {
    local candidate
    if [[ -n ${SPARK_HOME:-} && -x $SPARK_HOME/bin/spark-shell ]]; then
        printf '%s\n' "$SPARK_HOME/bin/spark-shell"
        return
    fi
    if type -P spark-shell; then
        return
    fi
    for candidate in "$HOME"/spark* /usr/local/spark* /opt/spark*; do
        if [[ -x $candidate/bin/spark-shell ]]; then
            printf '%s\n' "$candidate/bin/spark-shell"
            return
        fi
    done
    return 1
}
if ! spark_shell=$(find_spark_shell); then
    printf 'Spark shell not found. Set SPARK_HOME to the course Spark directory and rerun.\n' >&2
    exit 1
fi

hdfs_root=${ISIT312_HDFS_ROOT:-/isit312/vet}
hdfs_root=${hdfs_root%/}
scripts=()
markers=()
case $mode in
    full)
        export BATCH_ID=$batch ISIT312_HDFS_ROOT=$hdfs_root ISIT312_PRACTICE_SCOPE=full
        scripts+=("$repo/spark/integrate_clinical.sc")
        markers+=(ISIT312_INTEGRATION_PASS)
        ;;
    fixture)
        export BATCH_ID="$batch-fixture" ISIT312_HDFS_ROOT=$hdfs_root ISIT312_RAW_URI="$hdfs_root/raw/$batch" \
            ISIT312_PRACTICE_URI="file://$repo/fixtures/practice_visits.csv" ISIT312_PRACTICE_SCOPE=sample
        unset ISIT312_ACCEPTED_URI ISIT312_EXCEPTIONS_URI ISIT312_EXPECT_REFERENCE
        scripts+=("$repo/spark/integrate_clinical.sc")
        markers+=(ISIT312_INTEGRATION_PASS)
        ;;
    faults)
        # The fault batch lives under <root>/_test so it never mixes with real batches.
        export ISIT312_SOURCE_RAW_URI="$hdfs_root/raw/$batch" ISIT312_HDFS_ROOT="$hdfs_root/_test" \
            BATCH_ID="$batch-faults" ISIT312_PRACTICE_SCOPE=full ISIT312_EXPECT_REFERENCE=0
        unset ISIT312_RAW_URI ISIT312_PRACTICE_URI ISIT312_ACCEPTED_URI ISIT312_EXCEPTIONS_URI
        scripts+=("$repo/tests/make_integration_faults.sc" "$repo/spark/integrate_clinical.sc" \
            "$repo/tests/check_integration_faults.sc")
        markers+=(ISIT312_FAULT_BATCH_READY ISIT312_INTEGRATION_PASS ISIT312_FAULTS_PASS)
        ;;
esac

log_dir=${ISIT312_LOG_DIR:-$repo/logs}
mkdir -p "$log_dir"
log="$log_dir/integrate_clinical_${mode}_${batch}_$(date -u +%Y%m%dT%H%M%SZ).log"
printf 'Spark shell: %s\nMode: %s\nBatch: %s\nLog: %s\n' "$spark_shell" "$mode" "$BATCH_ID" "$log"
printf 'Commit: %s\n' "$(git -C "$repo" rev-parse --short HEAD 2>/dev/null || echo unknown)" | tee "$log"

run_spark() {
    {
        printf ':paste %s\n' "$@"
        printf ':quit\n'
    } | "$spark_shell" --master "${SPARK_MASTER:-local[2]}" \
        --conf "spark.sql.shuffle.partitions=${SPARK_SHUFFLE_PARTITIONS:-16}" 2>&1 | tee -a "$log"
}

# spark-shell can exit with status 0 after a Scala exception, so require the final marker too.
require_marker() {
    local marker=$1
    if ! tr '\r' '\n' < "$log" | grep -Fx "$marker" > /dev/null; then
        printf '\nFAILED: %s was not printed. Check the log: %s\n' "$marker" "$log" >&2
        exit 1
    fi
}

if [[ "$mode" == full && ${RUN_PARSER:-0} == 1 ]]; then
    if [[ ${ISIT312_RAW_URI:-"$hdfs_root/raw/$batch"} != "$hdfs_root/raw/$batch" ||
          ${ISIT312_PRACTICE_URI:-"$hdfs_root/normalized/$batch/practice_visits"} != "$hdfs_root/normalized/$batch/practice_visits" ]]; then
        printf 'FAILED: RUN_PARSER=1 requires the default input paths for this batch.\n' >&2
        exit 1
    fi
    run_spark "$repo/spark/normalize_practice.sc"
    require_marker ISIT312_PRACTICE_PASS
fi
run_spark "${scripts[@]}"
for marker in "${markers[@]}"; do
    require_marker "$marker"
done
printf '\nPASS (%s): %s\n' "$mode" "$log"
