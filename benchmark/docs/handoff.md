# Ashrafur handoff

Fill this after ASHRAFUR_BENCHMARK_PASS appears on your VM.

- Base and PR target: project-start
- Component: benchmark/
- Commit: record the code commit used, or source hashes from environment.txt
- Accepted baseline batch and visits path: copy from environment.txt
- Benchmark run ID: copy from run.log
- Actual Spark master and MapReduce mode: copy from logs
- VirtualBox CPU/RAM: add your actual settings
- Original rows / larger rows: must be 61,527 / 615,270
- Validation: copy matching_totals.txt and the fixture pass message
- Timing summary and recommendation: review results/comparison.md
- Chart and raw logs: link the shared evidence folder
- Rerun: bash benchmark/run_all.sh "/absolute/path/to/TailSignal.zip"

Send Hashim the PR, rerun command, full evidence folder, report section and chart.
Send Aaron the PR, accepted batch/path, checks and actual modes for integration review.
Share the sample SITE-08 July 2024 procedure count of 20 with Sarib for reconciliation.
