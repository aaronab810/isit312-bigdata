# ISIT312 Veterinary Data Lake

We’re building a data lake for a UAE veterinary clinic as part of ISIT312 Big Data Management at the University of Wollongong in Dubai. The demo brings practice-management exports, structured clinical visits, original free-text notes and lab results together. It reports visits by site, month and visit type, then traces one visit back to its source record, note and lab results.

## Start here

Use the [parallel work packages](docs/work_packages.md) to pick up your build. The [data contract](docs/data_contract.md) defines the shared interfaces, and the [baseline evidence](docs/baseline_evidence.md) records the passing course VM run.

1. Open the [team tracker and task guide](https://docs.google.com/document/d/1-GTuj_-7AlCtucJzKBMentBkKPHAQFW1Ps9vZycVRSc). Pick up your assigned component and check its inputs, outputs and handoff.
2. Download the shared TailSignal ZIP from the [project Page](https://chatgpt.com/space/page_ec6f4a543b088191a8b7fac2d9b51bbc). Extract it locally and read `DATA_GUIDE.md` and `manifest.json`. Use the original ZIP files as inputs. The flat CSV is for browsing.
3. Build, run and test your assigned component in your own ISIT312 Big Data VM in Oracle VirtualBox. Follow the course labs for HDFS, Scala `spark-shell` with Spark SQL, and Hive through Beeline.
4. Work on a component branch. Include the commands you ran in the VM, the input used and the actual output in your PR. Keep local paths out of shared code so the next teammate can rerun it.

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

## First demo checkpoint

Trace Alpha `V533959` to clinical visit `EV0033960` at `SITE-08` on 14 July 2024. Show one visit, one original note and seven lab rows. The visit counts once in the monthly procedure report, which has 20 visits for that site and month.

The full TailSignal dataset contains 61,527 clinical visits, 61,527 linked notes and 99,687 lab rows. Use those totals to check the full batch run in the course VM. Capture all component run evidence, integration results, benchmark timings and demo outputs in the course VM.

## Data handling

Keep full archives, extracted datasets, VM images, credentials and clinic-provided records out of Git. Use small synthetic fixtures for review. Source: [TailSignal generator](https://github.com/RonCom/tailsignal).
