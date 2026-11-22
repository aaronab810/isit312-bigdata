# Ashrafur Monthly Visit Benchmark

Compares Scala Spark SQL and Java Hadoop MapReduce ToolRunner.

- Input sizes: 61,527 and 615,270 rows.
- Three trials per engine per size.
- All twelve outputs matched.
- Spark mode: local[2].
- MapReduce mode: YARN.

Run from the repository root:
bash benchmark/run_all.sh /path/to/dataset.zip

Timing logs, results, screenshots and report are shared separately.
