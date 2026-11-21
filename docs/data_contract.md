# Veterinary data lake interface

TailSignal v1 contains fictional records generated with seed 42. Use the archive's `manifest.json` to verify file paths, checksums, and row counts. The assignment pipeline uses Scala `spark-shell` with Spark SQL for transformation, HDFS for storage, and Hive through Beeline for reporting.

## Raw inputs

Land the original files under `/isit312/vet/raw/<batch_id>`, preserving the archive-relative paths. Record the batch ID, manifest, and the files and row counts supplied to the batch. The derived flat view can help inspect the dataset. Build the accepted tables from the original inputs and the supplied crosswalk. Keep `evaluation/` out of the pipeline.

| File | Grain and identifiers |
| --- | --- |
| `data/pims/vet_alpha_visits.csv` | One encounter per row. Source system `vet_alpha`, encounter `visit_id`, site `clinic_id`, date `visit_date`, owner `client_id`, patient `patient_id`. `line_items` packs charge lines and `invoice_total` gives the encounter total. |
| `data/pims/vet_beta_encounters.jsonl` | One encounter per line. Source system `vet_beta`, encounter `encounter_id`, site `site`, date `date`, owner `client.id`, patient `patient.id`. `diagnoses` and `charges` are arrays. |
| `data/pims/vet_gamma_visits.txt` | One pipe-delimited encounter per row. Source system `vet_gamma`, encounter `VISIT_NO`, site `CLINIC`, date `VISIT_DT`, owner `ACCT`. `TOTAL` gives the encounter total. The pet appears by name and attributes only. |
| `data/pims/locations.csv` | One row per site, keyed by `location_id`. |
| `derived/visit_crosswalk.csv` | One row per supplied link. Fields are `source_system`, `source_visit_id`, `clinic`, `source_date`, `ehr_visit_id`, and `link_method`. |
| `data/ehr/ehr_patients.parquet` | One patient per `(clinic, clinic_patient_id)`. |
| `data/ehr/ehr_visits.parquet` | One clinical visit per `(clinic, clinic_patient_id, visit_id)`. Includes `day`, `visit_type`, `dx`, and `procedure`. |
| `data/ehr/ehr_notes.parquet` | One original narrative per clinical visit in this release. Fields are the visit key, `day`, and `text`. |
| `data/ehr/ehr_labs.parquet` | One analyte result per row, with the visit key, `day`, `reason`, `analyte`, and numeric `value`. There is no lab result ID. Preserve the analyte name and check its units before comparisons. |

The archive also contains separate procedure and prescription Parquet files. A prescription with a missing `visit_id` has no supplied encounter link.

## Linking and accepted tables

Treat identifiers as strings. Link a practice row by `(source_system, source_visit_id)`, then validate the site and date against the crosswalk and target visit. The crosswalk's `ehr_visit_id` refers to the EHR's `visit_id`. Join EHR notes and labs to visits using all three parts of `(clinic, clinic_patient_id, visit_id)`. Match the EHR patient using `(clinic, clinic_patient_id)`. Derive the visit date with `date_add(DATE '2022-01-01', CAST(day AS INT))`. The source gives a whole-day offset.

Write Parquet under `/isit312/vet/accepted/<batch_id>/<table>`. These are the minimum columns consumed by checks and reporting. Identifiers and other text fields are strings unless a type is specified.

| Table | Grain | Required columns |
| --- | --- | --- |
| `visits` | One row per accepted EHR visit | `clinic`, `clinic_patient_id`, `visit_id`, `visit_date` (date), `visit_type`, `source_system`, `source_visit_id`. The source pair may be null together for EHR-only visits. |
| `clinical_notes` | One source note per accepted visit for TailSignal v1 | `clinic`, `clinic_patient_id`, `visit_id`, `text` as supplied. |
| `lab_results` | One source analyte row per accepted visit and measured analyte in TailSignal v1 | `clinic`, `clinic_patient_id`, `visit_id`, `analyte`, `value` (double). A visit can have zero or many rows. |

Retain the batch ID and originating file or archive-relative path for each accepted or excepted row. For matched visits, keep enough provenance to trace the link back to the crosswalk and practice export, including the source ID pair and link method. Keep PIMS-only encounters traceable to the source file and IDs. Include EHR-only visits in the accepted clinical counts.

## Interfaces for parallel development

All jobs take `ISIT312_HDFS_ROOT` and `BATCH_ID` as run configuration. The default root is `/isit312/vet`. Job-specific paths such as the ZIP, the Hive database and the prior snapshot are explicit arguments or environment variables. The complete smoke batch can be generated independently in each VM. See [work packages](work_packages.md) for where each owner starts.

| Output | Path | Columns and rules |
| --- | --- | --- |
| `practice_visits` | `normalized/<batch_id>/practice_visits` | `source_system STRING`, `source_visit_id STRING`, `clinic STRING`, `source_date DATE`, `source_owner_id STRING`, `source_patient_id STRING`, `invoice_total DECIMAL(12,2)`, `source_path STRING`, `batch_id STRING`. Source system and source visit ID form the key. `source_patient_id` is null for Gamma. |
| `practice_charge_lines` | `normalized/<batch_id>/practice_charge_lines` | `source_system STRING`, `source_visit_id STRING`, `line_number INT`, `item STRING`, `amount DECIMAL(12,2)`. Number lines from 1 in source order. Alpha and Beta supply lines. Gamma's total stays on the visit. |
| `note_mentions` | `accepted/<batch_id>/note_mentions` | `clinic STRING`, `clinic_patient_id STRING`, `visit_id STRING`, `topic STRING`, `mention_status STRING`, `evidence STRING`. Use `current`, `historical`, or `negated`. Keep original notes in `clinical_notes`. |
| `monthly_visits` | Hive view in the working database | `clinic STRING`, `visit_year INT`, `visit_month INT`, `visit_type STRING`, `visit_count BIGINT`. Group the accepted visits by the first four columns. |

Use `fixtures/practice_visits.csv` to develop the integration adapter. It matches the normalized visit columns and contains a linked source row and a practice-only row. Load it using an explicit schema. Fixture mode exercises the join and unmatched-record behavior. Use the complete normalized dataset for full-batch assertions.

For replay fixtures use one accepted-visit key `(clinic, clinic_patient_id, visit_id)` and event metadata `event_id STRING`, `event_version BIGINT`, and `received_at TIMESTAMP`. The highest `event_version` wins. An exact repeat is idempotent. A lower version arriving late has no effect. Conflicting payloads with the same key and version are quarantined and the prior accepted version stays in the snapshot.

## Exceptions, replay, and handoff

Keep batch-scoped exceptions under `/isit312/vet/exceptions/<batch_id>/<component>` with `batch_id`, `source_path`, `reason_code`, and available keys. Report malformed inputs or missing keys, duplicate keys, invalid links, unexpected orphan notes or labs, and expected unmatched encounters separately. Keep originals available in raw HDFS. Reconcile source rows to accepted, unmatched, or rejected outcomes at the source's own grain. The full archive has 53,167 PIMS encounters and 53,123 crosswalk matches, leaving 44 PIMS-only and 8,404 EHR-only visits. Expected unmatched encounters are a separate category from invalid links.

Finish the full batch before incremental replay. Treat `batch_id` as the identity of the archived inputs for a run. Replaying an identical batch must reproduce its accepted and exception outputs without appending duplicate rows. If the supplied file content changes, use a new batch ID. Register or expose only a completed batch for downstream reads. Use one explicit batch per report or benchmark.

- Ingestion hands the batch ID, raw paths, manifest, and file and row counts to integration.
- Integration hands accepted Parquet locations, table schemas, reconciliation counts, and exceptions to the Hive and QA teammates. Use the three table names above for Hive tables and Spark check views. Pass the Hive database and HDFS paths as configuration.
- Beeline reports count rows from `visits` grouped by `clinic`, month of `visit_date`, and `visit_type`. For visit-level output with note or lab metrics, aggregate child rows before joining them. Compare Scala Spark SQL and Java MapReduce `ToolRunner` on the same accepted visits and grouping.

## Demo checkpoint

Trace `vet_alpha` encounter `V533959` through the crosswalk to clinical visit `EV0033960` at `SITE-08`. Its clinical `day` is 925, or `2024-07-14`. Expect one procedure visit, one original note, and seven lab rows. That visit contributes one of 20 `SITE-08` procedure visits in July 2024. The complete archive contains 61,527 clinical visits, 61,527 notes, and 99,687 lab results. Confirm the accepted totals and checkpoint in the course VM.
