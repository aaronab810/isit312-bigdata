#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
dataset_zip=${1:-}
if [[ $# != 1 || $dataset_zip == -h || $dataset_zip == --help ]]; then
    printf 'Usage: bash %s <path-to-tailsignal-dataset.zip>\n' "$0" >&2
    exit 2
fi
if [[ ! -f "$dataset_zip" ]]; then
    printf 'ERROR: Dataset archive not found at: %s\n' "$dataset_zip" >&2
    exit 1
fi

if [[ ${RUN_SMOKE_BASELINE:-1} != 1 ]]; then
    printf 'ERROR: The baseline is required by this launcher.\n' >&2
    exit 1
fi
for stage in RUN_HDFS_INGEST RUN_PIMS_PARSERS RUN_CLINICAL_INTEGRATION \
    RUN_NOTE_EXTRACTION RUN_INCREMENTAL_REPLAY RUN_HIVE_REPORT RUN_BENCHMARK; do
    if [[ ${!stage:-0} != 0 ]]; then
        printf 'ERROR: %s is not wired into the baseline launcher.\n' "$stage" >&2
        exit 1
    fi
done

log_dir=${ISIT312_LOG_DIR:-$repo_root/logs}
log_file="$log_dir/demo-$(date -u +%Y%m%dT%H%M%S)-$$.log"
mkdir -p "$log_dir"
{
    printf 'Baseline run | archive: %s | log: %s\n' "$dataset_zip" "$log_file"
    if bash "$repo_root/smoke/run_first_visit.sh" "$dataset_zip"; then
        printf 'ISIT312_BASELINE_PASS | Log: %s\n' "$log_file"
    else
        printf 'FAILED: Baseline run. See %s\n' "$log_file" >&2
        exit 1
    fi
} 2>&1 | tee "$log_file"
