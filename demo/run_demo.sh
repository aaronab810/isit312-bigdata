
#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
dataset_zip=${1:-}
run_id="demo-$(date -u +%Y%m%dT%H%M%S)-$$"
log_dir="${ISIT312_LOG_DIR:-$repo_root/logs}"
log_file="$log_dir/${run_id}.log"

# Feature flags for incremental team PR integration (0 = disabled until PR merges, 1 = active)
RUN_SMOKE_BASELINE=${RUN_SMOKE_BASELINE:-1}
RUN_HDFS_INGEST=${RUN_HDFS_INGEST:-0}
RUN_PIMS_PARSERS=${RUN_PIMS_PARSERS:-0}
RUN_CLINICAL_INTEGRATION=${RUN_CLINICAL_INTEGRATION:-0}
RUN_NOTE_EXTRACTION=${RUN_NOTE_EXTRACTION:-0}
RUN_INCREMENTAL_REPLAY=${RUN_INCREMENTAL_REPLAY:-0}
RUN_HIVE_REPORT=${RUN_HIVE_REPORT:-0}
RUN_BENCHMARK=${RUN_BENCHMARK:-0}

usage() {
    printf 'Usage: %s <path-to-tailsignal-dataset.zip>\n' "$0" >&2
    printf 'Optional env vars: ISIT312_HDFS_ROOT, ISIT312_LOG_DIR, RUN_* stage flags\n' >&2
}

if [[ -z "$dataset_zip" || "$dataset_zip" == "-h" || "$dataset_zip" == "--help" ]]; then
    usage
    exit 1
fi

if [[ ! -f "$dataset_zip" ]]; then
    printf 'ERROR: Dataset archive not found at: %s\n' "$dataset_zip" >&2
    exit 1
fi

dataset_zip=$(cd "$(dirname "$dataset_zip")" && pwd)/$(basename "$dataset_zip")
mkdir -p "$log_dir"


exec > >(tee -a "$log_file") 2>&1

current_stage="initialization"
on_error() {
    local exit_code=$?
    printf '\n[FAILED] Stage "%s" failed with exit code %d. Log: %s\n' \
        "$current_stage" "$exit_code" "$log_file" >&2
    exit "$exit_code"
}
trap on_error ERR


run_stage() {
    local stage_num=$1
    local stage_name=$2
    shift 2
    current_stage="$stage_name"
    printf '\n============================================================\n'
    printf '[STAGE %s] %s (%s)\n' "$stage_num" "$stage_name" "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    printf '============================================================\n'
    "$@"
    printf '[OK] Completed stage: %s\n' "$stage_name"
}


stage_preflight() {
    printf 'Repository root : %s\n' "$repo_root"
    printf 'Dataset archive : %s\n' "$dataset_zip"
    printf 'HDFS root       : %s\n' "${ISIT312_HDFS_ROOT:-/isit312/vet}"
    printf 'Run log         : %s\n' "$log_file"
}

stage_smoke_baseline() {
    bash "$repo_root/smoke/run_first_visit.sh" "$dataset_zip"
}


stage_hdfs_ingest()          { printf 'Placeholder: wire Abdallah HDFS ingestion command here.\n'; }
stage_pims_parsers()         { printf 'Placeholder: wire Alina Scala parsers here.\n'; }
stage_clinical_integration() { printf 'Placeholder: wire Hani clinical join .sc here.\n'; }
stage_note_extraction()      { printf 'Placeholder: wire Prajwal note extraction .sc here.\n'; }
stage_incremental_replay()   { printf 'Placeholder: wire Anuj snapshot replay .sc here.\n'; }
stage_hive_report()          { printf 'Placeholder: wire Sarib Beeline report .sql here.\n'; }
stage_benchmark()            { printf 'Placeholder: wire Ashrafur Spark vs MapReduce run here.\n'; }

run_stage "1" "Preflight & Configuration Check" stage_preflight

if [[ "$RUN_SMOKE_BASELINE" == "1" ]]; then
    run_stage "2" "Baseline Smoke Pipeline (HDFS + Spark SQL + Integration Checks)" stage_smoke_baseline
fi

[[ "$RUN_HDFS_INGEST" == "1" ]]          && run_stage "3" "HDFS Batch Ingestion (Abdallah)" stage_hdfs_ingest
[[ "$RUN_PIMS_PARSERS" == "1" ]]         && run_stage "4" "PIMS Source Parsers (Alina)" stage_pims_parsers
[[ "$RUN_CLINICAL_INTEGRATION" == "1" ]] && run_stage "5" "Clinical Integration (Hani)" stage_clinical_integration
[[ "$RUN_NOTE_EXTRACTION" == "1" ]]      && run_stage "6" "Clinical Note Extraction (Prajwal)" stage_note_extraction
[[ "$RUN_INCREMENTAL_REPLAY" == "1" ]]   && run_stage "7" "Incremental Replay (Anuj)" stage_incremental_replay
[[ "$RUN_HIVE_REPORT" == "1" ]]          && run_stage "8" "Hive / Beeline Reporting (Sarib)" stage_hive_report
[[ "$RUN_BENCHMARK" == "1" ]]            && run_stage "9" "Spark vs MapReduce Benchmark (Ashrafur)" stage_benchmark

current_stage="complete"
printf '\nISIT312_DEMO_PASS | Log saved to: %s\n' "$log_file"
