#!/usr/bin/env bash
# Verified, resumable TailSignal HDFS ingestion.
set -euo pipefail
IFS=$'\n\t'

usage() {
  cat <<'EOF'
Usage: ingest_hdfs.sh --source DIR --hdfs-root PATH --batch-id ID --manifest FILE

The source directory must contain paths listed in the TSV manifest. Accepted
batches appear only at PATH/raw/ID after every file has passed size and SHA-256
checks. Staging batches at PATH/staging/ID are never inputs for downstream jobs.
EOF
}

SOURCE=""; HDFS_ROOT=""; BATCH_ID=""; MANIFEST=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --source) SOURCE=${2:?}; shift 2 ;;
    --hdfs-root) HDFS_ROOT=${2:?}; shift 2 ;;
    --batch-id) BATCH_ID=${2:?}; shift 2 ;;
    --manifest) MANIFEST=${2:?}; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown option: $1" >&2; usage >&2; exit 2 ;;
  esac
done
[[ -n "$SOURCE" && -n "$HDFS_ROOT" && -n "$BATCH_ID" && -n "$MANIFEST" ]] || { usage >&2; exit 2; }
[[ "$BATCH_ID" =~ ^[A-Za-z0-9][A-Za-z0-9._-]*$ ]] || { echo "Unsafe batch ID: $BATCH_ID" >&2; exit 2; }
[[ "$HDFS_ROOT" == /* && "$HDFS_ROOT" != / && "$HDFS_ROOT" != *' '* && "$HDFS_ROOT" != *'..'* ]] || { echo "Use an absolute HDFS root without spaces or .." >&2; exit 2; }
[[ -d "$SOURCE" && -f "$MANIFEST" ]] || { echo "Source directory or manifest not found" >&2; exit 2; }
command -v hadoop >/dev/null || { echo "hadoop command is not available" >&2; exit 127; }
command -v sha256sum >/dev/null || { echo "sha256sum command is not available" >&2; exit 127; }

SOURCE=$(cd "$SOURCE" && pwd -P)
MANIFEST=$(cd "$(dirname "$MANIFEST")" && pwd -P)/$(basename "$MANIFEST")
HDFS_ROOT=${HDFS_ROOT%/}
STAGE="$HDFS_ROOT/staging/$BATCH_ID"
ACCEPTED="$HDFS_ROOT/raw/$BATCH_ID"

valid_relpath() { [[ "$1" != /* && "$1" != *".."* && "$1" != *"//"* && "$1" != "" ]]; }
local_sha() { sha256sum "$1" | awk '{print $1}'; }
hdfs_sha() { hadoop fs -cat "$1" | sha256sum | awk '{print $1}'; }

validate_local_manifest() {
  local rel bytes expected actual size count=0
  declare -A seen=()
  while IFS=$'\t' read -r rel bytes expected; do
    [[ -z "$rel" || "$rel" == \#* ]] && continue
    valid_relpath "$rel" || { echo "Unsafe manifest path: $rel" >&2; return 1; }
    [[ "$rel" =~ ^[A-Za-z0-9_./-]+$ && "$rel" != *.uploading && "$rel" != *._COPYING ]] || { echo "Unsupported manifest path: $rel" >&2; return 1; }
    [[ -z "${seen[$rel]+present}" ]] || { echo "Duplicate manifest path: $rel" >&2; return 1; }
    seen[$rel]=1
    count=$((count + 1))
    [[ "$bytes" =~ ^[0-9]+$ && "$expected" =~ ^[a-f0-9]{64}$ ]] || { echo "Invalid manifest row: $rel" >&2; return 1; }
    [[ -f "$SOURCE/$rel" ]] || { echo "Missing local source file: $rel" >&2; return 1; }
    size=$(wc -c < "$SOURCE/$rel" | tr -d ' ')
    actual=$(local_sha "$SOURCE/$rel")
    [[ "$size" == "$bytes" && "$actual" == "$expected" ]] || {
      echo "LOCAL_MANIFEST_MISMATCH $rel expected=$bytes/$expected actual=$size/$actual" >&2; return 1;
    }
  done < "$MANIFEST"
  (( count > 0 )) || { echo "Manifest has no files" >&2; return 1; }
}

validate_hdfs_tree() {
  local base=$1 rel bytes expected actual size
  while IFS=$'\t' read -r rel bytes expected; do
    [[ -z "$rel" || "$rel" == \#* ]] && continue
    hadoop fs -test -f "$base/$rel" || { echo "Missing HDFS file: $base/$rel" >&2; return 1; }
    size=$(hadoop fs -stat '%b' "$base/$rel")
    actual=$(hdfs_sha "$base/$rel")
    [[ "$size" == "$bytes" && "$actual" == "$expected" ]] || {
      echo "HDFS_MANIFEST_MISMATCH $rel expected=$bytes/$expected actual=$size/$actual" >&2; return 1;
    }
  done < "$MANIFEST"
}

validate_file_set() {
  local base=$1 include_ready=$2 path relative
  local expected_paths actual_paths
  expected_paths=$( { awk -F '\t' 'NF && $1 !~ /^#/ {print $1}' "$MANIFEST"; printf '%s\n' batch_manifest.tsv; [[ "$include_ready" != yes ]] || printf '%s\n' _READY; } | LC_ALL=C sort )
  actual_paths=$(hadoop fs -ls -R "$base" | awk '$1 ~ /^-/ {print $NF}' | while IFS= read -r path; do
    case "$path" in
      "$base"/*) relative=${path#"$base"/} ;;
      *"$base"/*) relative=${path#*"$base"/} ;;
      *) echo "UNEXPECTED_LOCATION $path" >&2; exit 1 ;;
    esac
    printf '%s\n' "$relative"
  done | LC_ALL=C sort)
  [[ "$expected_paths" == "$actual_paths" ]] || { echo "HDFS_FILE_SET_MISMATCH $base" >&2; return 1; }
}

validate_local_manifest
hadoop fs -mkdir -p "$HDFS_ROOT/staging" "$HDFS_ROOT/raw"

if hadoop fs -test -e "$ACCEPTED"; then
  hadoop fs -test -f "$ACCEPTED/batch_manifest.tsv" || { echo "Accepted batch manifest missing" >&2; exit 1; }
  [[ "$(hdfs_sha "$ACCEPTED/batch_manifest.tsv")" == "$(local_sha "$MANIFEST")" ]] || { echo "BATCH_MANIFEST_MISMATCH $BATCH_ID" >&2; exit 1; }
  validate_hdfs_tree "$ACCEPTED"
  hadoop fs -test -f "$ACCEPTED/_READY" || { echo "Accepted path lacks _READY marker: $ACCEPTED" >&2; exit 1; }
  validate_file_set "$ACCEPTED" yes
  echo "REPLAY_ALREADY_ACCEPTED batch=$BATCH_ID path=$ACCEPTED"
  exit 0
fi

hadoop fs -mkdir -p "$STAGE"
if hadoop fs -test -f "$STAGE/batch_manifest.tsv"; then
  [[ "$(hdfs_sha "$STAGE/batch_manifest.tsv")" == "$(local_sha "$MANIFEST")" ]] || { echo "STAGING_MANIFEST_MISMATCH $BATCH_ID" >&2; exit 1; }
else
  manifest_temp="$STAGE/batch_manifest.tsv.uploading"; if hadoop fs -test -e "$manifest_temp"; then hadoop fs -rm "$manifest_temp"; fi; if hadoop fs -test -e "$manifest_temp._COPYING"; then hadoop fs -rm "$manifest_temp._COPYING"; fi; hadoop fs -copyFromLocal "$MANIFEST" "$manifest_temp"; [[ "$(hdfs_sha "$manifest_temp")" == "$(local_sha "$MANIFEST")" ]] || { echo "UPLOAD_MANIFEST_MISMATCH" >&2; exit 1; }; hadoop fs -mv "$manifest_temp" "$STAGE/batch_manifest.tsv"
fi
while IFS=$'\t' read -r rel bytes expected; do
  [[ -z "$rel" || "$rel" == \#* ]] && continue
  target="$STAGE/$rel"
  if hadoop fs -test -f "$target"; then
    actual=$(hdfs_sha "$target")
    [[ "$actual" == "$expected" ]] || { echo "STAGING_CHECKSUM_MISMATCH $target" >&2; exit 1; }
    echo "RESUME_VERIFIED $rel"
  else
    hadoop fs -mkdir -p "$(dirname "$target")"
    temporary="$target.uploading"
    if hadoop fs -test -e "$temporary"; then
      hadoop fs -rm "$temporary"
    fi
    if hadoop fs -test -e "$temporary._COPYING"; then
      hadoop fs -rm "$temporary._COPYING"
    fi
    hadoop fs -copyFromLocal "$SOURCE/$rel" "$temporary"
    actual=$(hdfs_sha "$temporary")
    [[ "$actual" == "$expected" ]] || { echo "UPLOAD_CHECKSUM_MISMATCH $temporary" >&2; exit 1; }
    hadoop fs -mv "$temporary" "$target"
    echo "UPLOADED_AND_VERIFIED $rel"
  fi
done < "$MANIFEST"

validate_hdfs_tree "$STAGE"
if hadoop fs -test -e "$STAGE/_READY"; then
  validate_file_set "$STAGE" yes
else
  validate_file_set "$STAGE" no
fi
hadoop fs -test -e "$STAGE/_READY" || hadoop fs -touchz "$STAGE/_READY"
hadoop fs -test -e "$ACCEPTED" && { echo "Accepted batch appeared during upload; stop for review" >&2; exit 1; }
hadoop fs -mv "$STAGE" "$ACCEPTED"
echo "BATCH_ACCEPTED batch=$BATCH_ID path=$ACCEPTED"
