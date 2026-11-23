# ISIT312 Demo Launcher Runbook

## Overview
The launcher runs the shared baseline on the course Big Data VM and saves its output. The remaining jobs will be wired into the final launcher as their entry points become available.

## Prerequisites
1. Course Big Data VM running in VirtualBox with HDFS available (`hdfs dfs -ls /` succeeds).
2. `HADOOP_HOME` and `SPARK_HOME` configured (or `hdfs` and `spark-shell` available on `PATH`).
3. Local path to the TailSignal dataset archive (`ISIT312 TailSignal veterinary dataset.zip`).

## Commands

Run from the repository root:

```bash
bash demo/run_demo.sh "/path/to/tailsignal_vet_dataset.zip"
```

The launcher prints `ISIT312_BASELINE_PASS` only after the baseline passes. The log is saved under `logs/demo-*.log`, or under `ISIT312_LOG_DIR` if set. A failed baseline returns a nonzero status. The launcher rejects requests to enable stages that have yet to be connected.

To run the same baseline directly, use `bash smoke/run_first_visit.sh /path/to/tailsignal_vet_dataset.zip`.
