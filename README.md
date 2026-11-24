# Veterinary Clinic Data Lake

An ISIT312 Big Data Management project at the University of Wollongong in Dubai. We built a data lake for a case study with Doctor Dolittle Veterinary Clinic, a UAE clinic with one branch and plans to expand.

## The problem

The clinic’s practice system records visits, clinical notes and billing. Laboratory results arrive separately. When a lab export cannot be matched to a visit, staff may need to reconcile it manually. The project brings those sources together so a reader can check which records contributed to a report and see which links need review.

## How it works

| Stage | What it does |
| --- | --- |
| **HDFS** | Keeps the original exports and verifies each file against the batch manifest. |
| **Scala Spark SQL** | Reads three practice export layouts, standardises their fields, and links encounters to clinical visits, notes and lab measurements. It records unmatched entries for review. |
| **Hive** | Uses the prepared Parquet tables to count visits by site, month and type and to look up the source records behind an individual visit. |

The project dataset includes CSV and pipe-delimited files, nested JSON Lines, and free-text clinical notes. It covers nine example sites to test branch-level reporting if the clinic expands.

## What the run showed

- The combined batch verified **14 source files** and reconciled the Hive totals to **61,527 clinical visits**.
- At example site SITE-08, Hive counted **181 visits in July 2024**. The team traced one procedure visit to its original note and seven lab measurements. That encounter still counted as one visit.
- Spark SQL and Hadoop MapReduce produced the same monthly counts in the benchmark. Their timing ranges overlapped. Spark SQL fit the pipeline because it also handled parsing, record linking and checks.

## Explore or run the project

- [Project scope](https://github.com/aaronab810/isit312-bigdata/blob/project-start/docs/project_scope.md) explains the study and evaluation.
- [Data contract](https://github.com/aaronab810/isit312-bigdata/blob/project-start/docs/data_contract.md) describes the files, shared fields and record keys.
- [End-to-end runbook](https://github.com/aaronab810/isit312-bigdata/blob/aaron/final-vm-run/docs/demo_runbook.md) gives the commands for running the pipeline and benchmark. Use the [`aaron/final-vm-run` branch](https://github.com/aaronab810/isit312-bigdata/tree/aaron/final-vm-run) for the full runner. It needs the project dataset ZIP, HDFS, Spark and Hive. Small fixtures are included in the repository.

The project data package is available through [Google Drive](https://drive.google.com/file/d/1Y_SqKYZ3ThxQLYBDuKsIx6bzmYkospM6/view?usp=drivesdk). The generated source data uses the [TailSignal generator](https://github.com/RonCom/tailsignal). Keep full archives, VM images, credentials and clinic-provided records out of Git.
