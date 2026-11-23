#!/usr/bin/env bash
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
project=$(cd "$here/.." && pwd)
archive=${1:?Usage: bash benchmark/run_all.sh /absolute/path/to/dataset.zip}
[[ -f "$archive" ]] || { echo "Dataset ZIP not found: $archive" >&2; exit 1; }
archive=$(cd "$(dirname "$archive")" && pwd)/$(basename "$archive")
export ISIT312_HDFS_ROOT=${ISIT312_HDFS_ROOT:-/isit312/vet}
export SPARK_MASTER=${SPARK_MASTER:-local[2]}
export HADOOP_HOME=${HADOOP_HOME:-/usr/share/hadoop}
export SPARK_HOME=${SPARK_HOME:-/usr/share/spark}
hadoop="$HADOOP_HOME/bin/hadoop"
spark="$SPARK_HOME/bin/spark-shell"
hdfs="$HADOOP_HOME/bin/hdfs"
for tool in "$hadoop" "$spark" "$hdfs" /usr/bin/time; do
  [[ -x "$tool" ]] || { echo "Missing tool: $tool. Check course paths." >&2; exit 1; }
done
command -v javac > /dev/null
command -v python3 > /dev/null
command -v unzip > /dev/null
"$hdfs" dfsadmin -safemode wait
run_id="ashrafur-$(date -u +%Y%m%dT%H%M%S)-$$"
export BENCH_ROOT="$ISIT312_HDFS_ROOT/benchmark/$run_id"
work="$here/local/$run_id"
mkdir -p "$work"/{logs,results,build/classes}
exec > >(tee "$work/run.log") 2>&1
exec 3>"$work/logs/commands.log"
export BASH_XTRACEFD=3
set -x
printf 'RUN_ID=%s\nBENCH_ROOT=%s\n' "$run_id" "$BENCH_ROOT"
passed() { tr '\r' '\n' < "$2" | grep -Fx "$1" > /dev/null; }
if [[ -z ${ACCEPTED_VISITS:-} ]]; then
  echo 'STEP 1 Run the supplied baseline'
  bash "$project/smoke/run_first_visit.sh" "$archive" | tee "$work/logs/baseline-console.log"
  passed ISIT312_SMOKE_PASS "$work/logs/baseline-console.log"
  accepted=$(sed -n 's/^Accepted: //p' "$work/logs/baseline-console.log" | tail -n 1)
  [[ -n "$accepted" ]]
  export ACCEPTED_VISITS="$accepted/visits"
else
  echo 'Using configured accepted visits; baseline launch skipped'
fi
export BATCH_ID=$(basename "$(dirname "$ACCEPTED_VISITS")")
{
  date -u
  echo "BATCH_ID=$BATCH_ID"
  echo "ACCEPTED_VISITS=$ACCEPTED_VISITS"
  echo "BENCH_ROOT=$BENCH_ROOT"
  echo "SPARK_MASTER=$SPARK_MASTER"
  echo 'SPARK_SHUFFLE_PARTITIONS=1'
  echo 'MAPREDUCE_REDUCERS=1'
  java -version
  "$hadoop" version
  "$SPARK_HOME/bin/spark-submit" --version
  lscpu
  free -h
  git -C "$project" rev-parse HEAD || echo 'Extracted code copy without Git metadata'
  sha256sum "$here"/*.sc "$here"/*.java "$here"/*.sh "$here"/*.py
} > "$work/logs/environment.txt" 2>&1
echo 'STEP 2 Prepare both HDFS inputs and compile Java outside timing'
printf ':paste %s\n' "$here/prepare_input.sc" | "$spark" --master "$SPARK_MASTER" \
  > "$work/logs/prepare.log" 2>&1
passed ASHRAFUR_PREPARE_PASS "$work/logs/prepare.log"
javac -cp "$("$hadoop" classpath)" -d "$work/build/classes" "$here/MonthlyVisits.java"
jar cf "$work/build/monthly.jar" -C "$work/build/classes" .
for size in original larger; do
  "$hadoop" fs -cat "$BENCH_ROOT/input/$size/part-*" | wc -l > "$work/results/$size.rows"
  "$hadoop" fs -du -s "$BENCH_ROOT/input/$size" > "$work/results/$size.bytes"
  "$hadoop" fs -ls "$BENCH_ROOT/input/$size" > "$work/results/$size.files"
done
test "$(cat "$work/results/original.rows")" -eq 61527
test "$(cat "$work/results/larger.rows")" -eq 615270
spark_run() {
  local name=$1 input=$2
  export BENCH_INPUT="$input" BENCH_OUTPUT="$BENCH_ROOT/output/$name"
  printf ':paste %s\n' "$here/monthly.sc" | \
    /usr/bin/time -f '%e' -o "$work/logs/$name.time" \
    "$spark" --master "$SPARK_MASTER" > "$work/logs/$name.log" 2>&1
  passed ASHRAFUR_SPARK_PASS "$work/logs/$name.log"
  "$hadoop" fs -test -e "$BENCH_OUTPUT/_SUCCESS"
}
mr_run() {
  local name=$1 input=$2
  /usr/bin/time -f '%e' -o "$work/logs/$name.time" \
    "$hadoop" jar "$work/build/monthly.jar" MonthlyVisits \
    "$input" "$BENCH_ROOT/output/$name" > "$work/logs/$name.log" 2>&1
  "$hadoop" fs -test -e "$BENCH_ROOT/output/$name/_SUCCESS"
}
echo 'STEP 3 Check normal and malformed fixtures'
"$hadoop" fs -mkdir -p "$BENCH_ROOT/fixtures/valid" "$BENCH_ROOT/fixtures/invalid"
"$hadoop" fs -copyFromLocal "$here/fixtures/valid.tsv" "$BENCH_ROOT/fixtures/valid/"
"$hadoop" fs -copyFromLocal "$here/fixtures/invalid.tsv" "$BENCH_ROOT/fixtures/invalid/"
spark_run fixture_spark "$BENCH_ROOT/fixtures/valid"
mr_run fixture_mr "$BENCH_ROOT/fixtures/valid"
for engine in spark mr; do
  "$hadoop" fs -cat "$BENCH_ROOT/output/fixture_$engine/part-*" | \
    LC_ALL=C sort > "$work/results/fixture_$engine.tsv"
  diff -u "$here/fixtures/expected.tsv" "$work/results/fixture_$engine.tsv"
done
export BENCH_INPUT="$BENCH_ROOT/fixtures/invalid" BENCH_OUTPUT="$BENCH_ROOT/output/invalid_spark"
# The error fixture is expected to fail. Keep these errors outside measured results.
set +e
printf ':paste %s\n' "$here/monthly.sc" | "$spark" --master "$SPARK_MASTER" \
  > "$work/logs/invalid_spark.log" 2>&1
spark_status=$?
"$hadoop" jar "$work/build/monthly.jar" MonthlyVisits "$BENCH_INPUT" \
  "$BENCH_ROOT/output/invalid_mr" > "$work/logs/invalid_mr.log" 2>&1
mr_status=$?
set -e
[[ $spark_status -ne 0 && $mr_status -ne 0 ]]
grep -q 'Invalid benchmark row' "$work/logs/invalid_spark.log"
grep -q 'Invalid benchmark row' "$work/logs/invalid_mr.log"
echo 'PASS fixture output and invalid-row rejection'
echo 'STEP 4 Run 12 measured jobs'
for size in original larger; do
  for trial in 1 2 3; do
    if [[ $trial -eq 2 ]]; then order='mr spark'; else order='spark mr'; fi
    for engine in $order; do
      name="${engine}_${size}_${trial}"
      "${engine}_run" "$name" "$BENCH_ROOT/input/$size"
      echo "PASS $name"
    done
  done
done
echo 'STEP 5 Compare results and generate statistics and chart'
for size in original larger; do
  for trial in 1 2 3; do
    for engine in spark mr; do
      name="${engine}_${size}_${trial}"
      "$hadoop" fs -cat "$BENCH_ROOT/output/$name/part-*" | \
        LC_ALL=C sort > "$work/results/$name.tsv"
    done
  done
done
python3 "$here/report.py" "$work"
printf '%s\n' "$work" > "$here/local/latest-run.txt"
echo 'ASHRAFUR_BENCHMARK_PASS'
echo "All results: $work"
echo 'Add VirtualBox CPU/RAM allocation and review comparison.md before submission.'
