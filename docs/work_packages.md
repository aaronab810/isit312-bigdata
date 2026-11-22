# Team work instructions

Everyone can start today. Use the [shared TailSignal ZIP](https://drive.google.com/file/d/1Y_SqKYZ3ThxQLYBDuKsIx6bzmYkospM6/view?usp=drivesdk), the test files in this repository or the cleaned tables you can make on your own VM. The [Google Doc](https://docs.google.com/document/d/1-GTuj_-7AlCtucJzKBMentBkKPHAQFW1Ps9vZycVRSc) has the same assignments. Connect the finished jobs for one final live run.

## Set up your own VM

1. Start your branch from `project-start`. Download the ZIP and read its `DATA_GUIDE.md` and `manifest.json`.
2. If your task uses the baseline, start the NameNode and DataNode and wait for HDFS safe mode to clear. From the repository folder, run `bash smoke/run_first_visit.sh /path/to/tailsignal_vet_dataset.zip`. Replace the example path with the path to your ZIP in the VM. Set `HADOOP_HOME` and `SPARK_HOME` to the lab installation folders if the script cannot find the tools.
3. Look for `ISIT312_SMOKE_PASS`. The printed **Raw** path points to the original files on HDFS. The **Accepted** path contains the cleaned `visits`, `clinical_notes` and `lab_results` Parquet tables. See the [saved VM results](baseline_evidence.md) and [shared table definitions and rules](data_contract.md).
4. Open your pull request against `project-start`. Include your code, what it reads and writes, the exact VM commands, the actual output or log, and a screenshot. Keep the full dataset and generated tables out of Git.

## Aaron

- **Start with** the baseline Parquet, `sql/checks/integration.sql` and the [clinic workflow draft](clinic_workflow.md).
- **Build and run** `tests/run_integration_checks.sc` in Scala Spark shell. The clean data should pass all eight checks. In temporary views, test a duplicate visit, a blank note, a lab without a visit and a wrong source ID. Each should trigger its expected check.
- **Submit** the script, VM log and screenshots. For the report, document the clinic’s data volume, collection, storage, processing, connections between systems and recurring problems. Record the clinic feedback and resulting decisions. Coordinate the final combined run.

## Alina

- **Start with** the three original exports in `data/pims/` in the ZIP or on HDFS.
- **Build and run** `spark/normalize_practice.sc` in Scala Spark shell. Read Alpha CSV, Beta JSON Lines and Gamma pipe-delimited text. Split Alpha’s packed charges and Beta’s nested charges and diagnoses. Keep Gamma’s total on its visit row and its missing patient ID null. Keep original source IDs. Expect 18,881 Alpha, 17,571 Beta and 16,715 Gamma accepted rows in the full input. Use a malformed record to show the reason it was rejected.
- **Submit** the parser, field list, input formats and sizes, the output schemas and counts, rejection reasons and a check that charges match the source values where available. Trace Alpha visit `V533959` into the cleaned output.

## Abdallah

- **Start with** the ZIP and `manifest.json`.
- **Build and run** `ingest/land_batch.sh`. Take the local dataset folder, HDFS root and batch ID as inputs. Copy files to a temporary HDFS location, check names, sizes and checksums against the manifest, then make the completed batch available. Test a clean upload, an interruption and retry, an identical repeat, and a changed file under an existing batch ID. The changed file should be rejected, and the original accepted files should survive.
- **Submit** the loader, retry test, batch manifest, VM evidence and a diagram showing where the raw and accepted files live.

## Hani

- **Start with** the original clinical Parquet, `derived/visit_crosswalk.csv` and `fixtures/practice_visits.csv`. Use the baseline for reference totals.
- **Build and run** `spark/integrate_clinical.sc`. Link notes and labs using the clinic, clinic-patient and visit IDs. Use the crosswalk to connect practice visits to clinical visits. Write separate visits, notes and lab tables, and record broken or missing links. Use the supplied fixture to show how `V533959` links to `EV0033960` and how a practice-only row is kept. The full dataset reference is 61,527 clinical visits, 53,123 source links, 44 practice-only encounters and 8,404 clinical-only visits.
- **Submit** the job, table definitions, a diagram of which IDs connect the records, a count of matched, unmatched and invalid links, and the trace for `EV0033960`.

## Prajwal

- **Start with** the original notes and the separate reference labels in `evaluation/ehr/note_labels.parquet`. Label a small synthetic sample if the supplied labels do not cover the categories.
- **Build and run** `spark/extract_note_mentions.sc`. Find whether the note says a symptom is present now, happened in the past or was ruled out. Write the labels to `note_mentions` with the visit ID. Keep the original text available. Set aside labelled notes before tuning the rules and test on those notes.
- **Submit** the job, labelled test set, field list, and counts, precision and recall by category where applicable. Include one example per category, examples the job got wrong and a short explanation of its limits.

## Sarib

- **Start with** the `visits`, `clinical_notes` and `lab_results` tables produced by the baseline on your VM.
- **Build and run** `sql/hive/register_tables.sql`, `sql/hive/monthly_report.sql` and `sql/hive/visit_drilldown.sql` in Beeline. Count visits by site, month and visit type, show how many visits have notes, and open a visit with its original note, source IDs and lab results. The monthly counts should add up to 61,527. SITE-08 should have 20 procedure visits in July 2024. `EV0033960` should count as one visit and return one note and seven lab rows.
- **Submit** the SQL files, Beeline screenshots, a sample month’s expected output and the count check showing that linked rows leave the visit total unchanged.

## Anuj

- **Start with** the baseline visits and the [replay rules](data_contract.md) for deciding which version of a record to keep.
- **Build and run** `spark/replay_snapshot.sc` and small test inputs under `fixtures/replay/`. Save an updated Parquet snapshot and keep the previous one. Test a new visit, the same input twice, a late older version, a valid correction and two different records with the same version. A repeat should give the same result, an older version should leave the current record intact, and conflicting records should be set aside.
- **Submit** the job, test inputs, before-and-after results, exact commands and example visit IDs. Explain when scheduled file updates meet the clinic’s needs and when faster updates would matter.

## Ashrafur

- **Start with** the baseline visits on your VM and the monthly count definition in the [data contract](data_contract.md). Create one HDFS benchmark input with one row per visit. Include clinic, visit type and the year and month from `visit_date`.
- **Build and run** the same monthly count in Scala Spark SQL and Java Hadoop MapReduce under `benchmark/`. Follow the Week 4 Lab 3 `ToolRunner` example and use `hadoop jar`. Run both against the same input at the original size and a larger synthetic size. At each size, sort both results and check that the counts match. Run each job at least three times per size on the same VM. Time the jobs after the input is ready.
- **Submit** both jobs, matching outputs, VM settings, commands and timing logs. Include input sizes, median and range, rows per second and a chart. Use the results to explain which approach fits the clinic’s workload.

## Hashim

- **Start with** the shared ZIP, `smoke/run_first_visit.sh` and the [saved baseline results](baseline_evidence.md).
- **Build and run** `demo/run_demo.sh` on your VM. Start by calling the baseline script. Let the user set the ZIP path, stop if a command fails or the pass marker is missing, and save the log. Capture the result for `EV0033960` with one visit, its original note and seven lab rows. Write down the commands for the HDFS, Scala Spark and Hive stages and draft the slides from the baseline output.
- **Submit** the launcher, runbook, VM log, screenshot, commit ID, batch ID, counts and draft slides. Add the merged job commands to the launcher for the one final live run. Include the team’s results, clinic feedback and lessons learned in the finished slides.
