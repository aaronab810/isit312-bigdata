# ISIT312 Demo Launcher Runbook

## Overview
This runbook explains how to run the baseline smoke check and the demo launcher (`demo/run_demo.sh`) on the course Big Data VM, how failures and logs are handled, and how upcoming teammate PRs plug into the launcher.

## Prerequisites
1. Course Big Data VM running in VirtualBox with HDFS available (`hdfs dfs -ls /` succeeds).
2. `HADOOP_HOME` and `SPARK_HOME` configured (or `hdfs` and `spark-shell` available on `PATH`).
3. Local path to the TailSignal dataset archive (`ISIT312 TailSignal veterinary dataset.zip`).

## Commands

### 1. Run Baseline Smoke Check Directly
```bash
./smoke/run_first_visit.sh "/path/to/ISIT312 TailSignal veterinary dataset.zip"
