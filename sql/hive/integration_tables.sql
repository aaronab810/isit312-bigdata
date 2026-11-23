-- Hive external tables over the clinical integration outputs (owner: Hani).
-- spark/integrate_clinical.sc writes the Parquet; these definitions only register it, so dropping a
-- table leaves the files in place. Rerun with another batch path to point the tables at that batch.
--
--   $HIVE_HOME/bin/beeline -u jdbc:hive2://localhost:10000 \
--     --hivevar db=isit312_vet \
--     --hivevar accepted=/isit312/vet/accepted/<batch_id> \
--     --hivevar exceptions=/isit312/vet/exceptions/<batch_id>/integrate_clinical \
--     -f sql/hive/integration_tables.sql
--
-- The visits, clinical_notes and lab_results columns are a superset of the baseline tables, so
-- reporting queries written against the baseline keep working.

CREATE DATABASE IF NOT EXISTS ${hivevar:db};
USE ${hivevar:db};

-- One row per accepted clinical visit. The source pair is null when no practice encounter is linked.
DROP TABLE IF EXISTS visits;
CREATE EXTERNAL TABLE visits (
  clinic               STRING COMMENT 'Site, e.g. SITE-08',
  clinic_patient_id    STRING COMMENT 'Clinic-scoped EHR patient ID',
  visit_id             STRING COMMENT 'EHR visit ID, e.g. EV0033960',
  visit_date           DATE   COMMENT '2022-01-01 plus day',
  day                  INT    COMMENT 'Original whole-day offset',
  visit_type           STRING COMMENT 'wellness, sick, recheck or procedure',
  dx                   STRING,
  `procedure`          STRING,
  source_system        STRING COMMENT 'vet_alpha, vet_beta or vet_gamma when matched',
  source_visit_id      STRING COMMENT 'Practice encounter ID when matched',
  link_method          STRING COMMENT 'Crosswalk link_method when matched',
  link_status          STRING COMMENT 'matched, clinical_only, link_rejected or source_not_loaded',
  practice_source_path STRING COMMENT 'Practice export file of the linked encounter',
  crosswalk_path       STRING COMMENT 'derived/visit_crosswalk.csv when matched',
  source_path          STRING COMMENT 'data/ehr/ehr_visits.parquet',
  batch_id             STRING
)
STORED AS PARQUET
LOCATION '${hivevar:accepted}/visits';

-- One original note per accepted visit, text as supplied.
DROP TABLE IF EXISTS clinical_notes;
CREATE EXTERNAL TABLE clinical_notes (
  clinic            STRING,
  clinic_patient_id STRING,
  visit_id          STRING,
  day               INT,
  text              STRING COMMENT 'Original free-text narrative',
  source_path       STRING COMMENT 'data/ehr/ehr_notes.parquet',
  batch_id          STRING
)
STORED AS PARQUET
LOCATION '${hivevar:accepted}/clinical_notes';

-- One row per accepted visit and analyte. A visit has zero or more rows; the unit is part of the analyte name.
DROP TABLE IF EXISTS lab_results;
CREATE EXTERNAL TABLE lab_results (
  clinic            STRING,
  clinic_patient_id STRING,
  visit_id          STRING,
  day               INT,
  reason            STRING COMMENT 'Why the panel was run, e.g. pre_anesthetic',
  analyte           STRING COMMENT 'Analyte and unit, e.g. creatinine_mg_dl',
  value             DOUBLE,
  source_path       STRING COMMENT 'data/ehr/ehr_labs.parquet',
  batch_id          STRING
)
STORED AS PARQUET
LOCATION '${hivevar:accepted}/lab_results';

-- Every input row that was not accepted as supplied, at the grain of its own input.
DROP TABLE IF EXISTS integration_exceptions;
CREATE EXTERNAL TABLE integration_exceptions (
  batch_id          STRING,
  component         STRING COMMENT 'integrate_clinical',
  record_type       STRING COMMENT 'practice_visit, crosswalk, ehr_visit, clinical_note or lab_result',
  source_path       STRING COMMENT 'Archive-relative file the row came from',
  reason_code       STRING,
  clinic            STRING,
  clinic_patient_id STRING,
  visit_id          STRING COMMENT 'EHR visit ID, or the crosswalk target for link exceptions',
  source_system     STRING,
  source_visit_id   STRING,
  detail            STRING,
  category          STRING COMMENT 'expected_unmatched, invalid_link, duplicate_key, missing_key or orphan_child'
)
STORED AS PARQUET
LOCATION '${hivevar:exceptions}';
