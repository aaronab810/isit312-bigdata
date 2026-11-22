# ISIT312 Veterinary Data Lake

We’re building a data lake for a UAE veterinary clinic as part of ISIT312 Big Data Management at the University of Wollongong in Dubai. The demo brings practice-management exports, structured clinical visits, original free-text notes and lab results together. It reports visits by site, month and visit type, then traces one visit back to its source record, note and lab results.

## Start here

Use the [project-start branch](https://github.com/aaronab810/isit312-bigdata/tree/project-start) for the course VM scripts, test files and team work instructions.

Read the [full project scope](https://github.com/aaronab810/isit312-bigdata/blob/project-start/docs/project_scope.md) for the clinic study, data lake implementation, tool comparison, company evaluation and submission. Use the [parallel work packages](https://github.com/aaronab810/isit312-bigdata/blob/project-start/docs/work_packages.md) to pick up your build and report contribution. The [data contract](https://github.com/aaronab810/isit312-bigdata/blob/project-start/docs/data_contract.md) defines the shared interfaces, and the [baseline evidence](https://github.com/aaronab810/isit312-bigdata/blob/project-start/docs/baseline_evidence.md) records the passing course VM run.

1. Find your name in the [GitHub work instructions](https://github.com/aaronab810/isit312-bigdata/blob/project-start/docs/work_packages.md) or the [Google Doc](https://docs.google.com/document/d/1-GTuj_-7AlCtucJzKBMentBkKPHAQFW1Ps9vZycVRSc). Your assignment tells you what to start with, what to build on your VM and what to submit.
2. Download the [TailSignal dataset ZIP from Google Drive](https://drive.google.com/file/d/1Y_SqKYZ3ThxQLYBDuKsIx6bzmYkospM6/view?usp=drivesdk). Extract it locally and read `DATA_GUIDE.md` and `manifest.json`. Use the original ZIP files as inputs. The flat CSV is for browsing.
3. Build, run and test your assigned component in your own ISIT312 Big Data VM in Oracle VirtualBox. Follow the course labs for HDFS, Scala `spark-shell` with Spark SQL, and Hive through Beeline.
4. Start your branch from `project-start` and open your pull request against it. Include the VM commands, inputs, actual output or log, and a screenshot. Everyone can build, test and submit using the shared ZIP, test files or local baseline. Connect the finished jobs for one final live run.

## Dataset and pipeline

| Input | Role |
| --- | --- |
| Alpha CSV, Beta JSON Lines, Gamma pipe-delimited text | Three synthetic practice exports with distinct schemas and source IDs |
| Clinical Parquet | Structured patients, visits and labs, plus original free-text notes |
| `derived/visit_crosswalk.csv` | Links practice-export encounters to clinical visits using TailSignal’s generated IDs |

### Types of data

- **Structured**. Fixed columns such as visit IDs, dates and lab values. Examples include Alpha CSV, Gamma pipe-delimited records and the clinical Parquet tables.
- **Semi-structured**. Beta JSON Lines has one encounter per line, with nested patient fields and arrays of diagnoses and charges.
- **Unstructured**. The original clinical notes are free-text narratives. Their content is unstructured even though it sits in a Parquet table and links to a visit by ID.

**Pipeline:** Original files → HDFS → Scala Spark SQL → separate Parquet tables for visits, notes and labs → Hive reporting with Beeline. Start with batch processing. Use the same monthly aggregation to compare Spark SQL with the lab’s Java MapReduce `ToolRunner` approach.

## Expected result for the final live run

Trace Alpha `V533959` to clinical visit `EV0033960` at `SITE-08` on 14 July 2024. Show one visit, one original note and seven lab rows. The visit counts once in the monthly procedure report, which has 20 visits for that site and month.

The full TailSignal dataset contains 61,527 clinical visits, 61,527 linked notes and 99,687 lab rows. Use those totals to check the full batch run in the course VM. Capture all component run evidence, integration results, benchmark timings and demo outputs in the course VM.

## Data handling

Keep full archives, extracted datasets, VM images, credentials and clinic-provided records out of Git. Use small synthetic fixtures for review. Source: [TailSignal generator](https://github.com/RonCom/tailsignal).
