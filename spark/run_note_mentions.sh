#!/usr/bin/env bash
# Run note mention extraction and its held-out evaluation in the course VM.
#
#   bash spark/run_note_mentions.sh fixture
#       Synthetic fixture only. Reads fixtures/note_mentions/*.csv from local disk (file://),
#       writes to a temporary local folder and evaluates against the hand-labelled gold CSV.
#
#   bash spark/run_note_mentions.sh batch <batch_id> [/path/to/tailsignal_vet_dataset.zip]
#       Reads <root>/accepted/<batch_id>/clinical_notes (from smoke/run_first_visit.sh),
#       writes <root>/accepted/<batch_id>/note_mentions and <root>/exceptions/<batch_id>/note_mentions.
#       With the ZIP, copies evaluation/ehr/note_labels.parquet to <root>/evaluation/<batch_id>/
#       (outside raw/ and accepted/) and runs the held-out evaluation.
#
# Environment: ISIT312_HDFS_ROOT (default /isit312/vet), HADOOP_HOME, SPARK_HOME,
#              SPARK_MASTER (default local[2]), NOTE_LOG_DIR (default $HOME/isit312_logs),
#              TEST_PERCENT (default 30).
set -euo pipefail

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repo=$(cd "$here/.." && pwd)
mode=${1:-}
hdfs_root=${ISIT312_HDFS_ROOT:-/isit312/vet}
master=${SPARK_MASTER:-local[2]}
log_dir=${NOTE_LOG_DIR:-$HOME/isit312_logs}
mkdir -p "$log_dir"
stamp=$(date -u +%Y%m%dT%H%M%S)

find_tool() {
    local name=$1 home=$2 candidate
    shift 2
    if [[ -n $home && -x $home/bin/$name ]]; then printf '%s\n' "$home/bin/$name"; return; fi
    if type -P "$name"; then return; fi
    for candidate in "$@"; do
        if [[ -x $candidate/bin/$name ]]; then printf '%s\n' "$candidate/bin/$name"; return; fi
    done
    return 1
}

if ! spark_shell=$(find_tool spark-shell "${SPARK_HOME:-}" "$HOME"/spark* /usr/local/spark* /opt/spark* /usr/share/spark); then
    printf 'Spark shell not found. Set SPARK_HOME to the course Spark directory and rerun.\n' >&2
    exit 1
fi

# Runs one .sc file and fails unless its pass marker is printed.
run_sc() {
    local script=$1 marker=$2 log=$3
    printf 'Running %s\nLog: %s\n' "$script" "$log"
    printf ':paste %s\n:quit\n' "$script" | "$spark_shell" --master "$master" 2>&1 | tee "$log"
    # The Spark shell can return zero after a Scala exception. Require the marker.
    if ! tr '\r' '\n' < "$log" | grep -Fx "$marker" > /dev/null; then
        printf 'FAILED: %s not found in %s\n' "$marker" "$log" >&2
        exit 1
    fi
}

case $mode in
fixture)
    stage=$(mktemp -d)
    trap 'rm -rf -- "$stage"' EXIT
    export BATCH_ID=fixture
    export NOTE_MENTIONS_INPUT="file://$repo/fixtures/note_mentions/notes.csv"
    export NOTE_MENTIONS_OUTPUT="file://$stage/note_mentions"
    export NOTE_MENTIONS_EXCEPTIONS="file://$stage/exceptions/note_mentions"
    export NOTE_SOURCE_PATH="fixtures/note_mentions/notes.csv"
    export NOTE_MENTIONS_EXPECT_NOTES=26
    export NOTE_MENTIONS_EXPECT_EXCEPTIONS=2
    run_sc "$here/extract_note_mentions.sc" ISIT312_NOTE_MENTIONS_PASS "$log_dir/note_mentions_fixture_$stamp.log"

    export NOTE_MENTIONS_URI="$NOTE_MENTIONS_OUTPUT"
    export NOTES_URI="$NOTE_MENTIONS_INPUT"
    export GOLD_URI="file://$repo/fixtures/note_mentions/gold_labels.csv"
    export EVAL_SPLITS=all
    run_sc "$here/evaluate_note_mentions.sc" ISIT312_NOTE_EVAL_PASS "$log_dir/note_eval_fixture_$stamp.log"
    ;;
batch)
    batch=${2:?Usage: run_note_mentions.sh batch <batch_id> [dataset.zip]}
    archive=${3:-}
    if ! hdfs_bin=$(find_tool hdfs "${HADOOP_HOME:-}" "$HOME"/hadoop* /usr/local/hadoop* /opt/hadoop* /usr/share/hadoop); then
        printf 'Hadoop client not found. Set HADOOP_HOME to the course Hadoop directory and rerun.\n' >&2
        exit 1
    fi
    fs=$("$hdfs_bin" getconf -confKey fs.defaultFS)
    [[ $fs == hdfs://* ]] || { printf 'Expected HDFS, found %s\n' "$fs" >&2; exit 1; }
    fs=${fs%/}
    notes="$hdfs_root/accepted/$batch/clinical_notes"
    if ! "$hdfs_bin" dfs -test -d "$notes"; then
        printf 'No clinical_notes at %s. Run smoke/run_first_visit.sh first and pass its batch ID.\n' "$notes" >&2
        exit 1
    fi
    export BATCH_ID=$batch
    export ISIT312_HDFS_ROOT=$hdfs_root
    export NOTE_MENTIONS_INPUT="$fs$notes"
    export NOTE_MENTIONS_OUTPUT="$fs$hdfs_root/accepted/$batch/note_mentions"
    export NOTE_MENTIONS_EXCEPTIONS="$fs$hdfs_root/exceptions/$batch/note_mentions"
    run_sc "$here/extract_note_mentions.sc" ISIT312_NOTE_MENTIONS_PASS "$log_dir/note_mentions_${batch}_$stamp.log"

    if [[ -n $archive ]]; then
        stage=$(mktemp -d)
        trap 'rm -rf -- "$stage"' EXIT
        unzip -q "$archive" 'evaluation/ehr/note_labels.parquet' -d "$stage"
        labels="$hdfs_root/evaluation/$batch/note_labels.parquet"
        "$hdfs_bin" dfs -mkdir -p "$hdfs_root/evaluation/$batch"
        "$hdfs_bin" dfs -put -f "$stage/evaluation/ehr/note_labels.parquet" "$labels"
        export NOTE_MENTIONS_URI="$NOTE_MENTIONS_OUTPUT"
        export NOTES_URI="$NOTE_MENTIONS_INPUT"
        export GOLD_URI="$fs$labels"
        export EVAL_SPLITS=${EVAL_SPLITS:-test}
        export EVAL_OUTPUT="$fs$hdfs_root/evaluation/$batch/note_mentions_metrics"
        run_sc "$here/evaluate_note_mentions.sc" ISIT312_NOTE_EVAL_PASS "$log_dir/note_eval_${batch}_$stamp.log"
    fi
    ;;
*)
    printf 'Usage: %s fixture | batch <batch_id> [dataset.zip]\n' "$0" >&2
    exit 2
    ;;
esac
printf 'Logs are in %s\n' "$log_dir"
