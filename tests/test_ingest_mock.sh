#!/usr/bin/env bash
set -euo pipefail
repo=$(cd "$(dirname "$0")/.." && pwd)
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
mkdir -p "$tmp/bin" "$tmp/input/data" "$tmp/hdfs"
cp "$repo/tests/mock_hadoop.sh" "$tmp/bin/hadoop"
chmod +x "$tmp/bin/hadoop"
export PATH="$tmp/bin:$PATH"
printf 'one\n' > "$tmp/input/data/a"
printf 'two\n' > "$tmp/input/data/b"
for f in data/a data/b; do
  printf '%s\t%s\t%s\n' "$f" "$(wc -c < "$tmp/input/$f" | tr -d ' ')" "$(sha256sum "$tmp/input/$f" | awk '{print $1}')"
done > "$tmp/manifest"
loader="$repo/ingest/land_batch.sh"
[[ -f "$loader" ]] || loader="$repo/scripts/ingest_hdfs.sh"
run() { bash "$loader" --source "$tmp/input" --hdfs-root "$tmp/hdfs" --batch-id "$1" --manifest "$tmp/manifest"; }
run clean
run clean | grep REPLAY_ALREADY_ACCEPTED
mkdir -p "$tmp/hdfs/staging/retry/data"
cp "$tmp/manifest" "$tmp/hdfs/staging/retry/batch_manifest.tsv"
cp "$tmp/input/data/a" "$tmp/hdfs/staging/retry/data/a"
printf 'partial' > "$tmp/hdfs/staging/retry/data/b.uploading._COPYING"
run retry | grep RESUME_VERIFIED
test -f "$tmp/hdfs/raw/retry/data/b"
test ! -e "$tmp/hdfs/raw/retry/data/b.uploading._COPYING"
printf 'changed\n' >> "$tmp/input/data/a"
if run clean; then echo 'FAIL: changed file accepted' >&2; exit 1; fi
cmp "$tmp/hdfs/raw/clean/data/a" "$tmp/hdfs/raw/retry/data/a"
echo 'MOCK_TEST_PASS'
