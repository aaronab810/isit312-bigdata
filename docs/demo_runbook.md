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

## Full project run

Start HDFS, the Hive metastore and HiveServer2 using the course lab steps. Run from the checked-out project directory. This command creates a fresh HDFS batch and Hive database, publishes the Hive reports, replays a visit correction and checks the results.

```bash
export HADOOP_HOME=/usr/share/hadoop SPARK_HOME=/usr/share/spark
export HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop"
bash demo/run_project.sh /media/sf_vmshare/tailsignal_vet_dataset.zip
```

The run prints its Git revision, batch ID and log path. Save the log and capture the accepted HDFS batch, the note-evaluation metrics, the Hive total of 61,527 and the SITE-08 drill-down (20 procedure visits in July 2024, one visit, one original note and seven lab rows), and the replay correction and final `ISIT312_PROJECT_PASS` lines. The benchmark runs separately against the same accepted visits:

```bash
ACCEPTED_VISITS='<Accepted URI printed above>/visits' \
  bash benchmark/run_all.sh /media/sf_vmshare/tailsignal_vet_dataset.zip
```
