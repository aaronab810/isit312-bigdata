-- Sarib: register the baseline Parquet outputs as Hive external tables.
-- Run with --hiveconf accepted_path=/isit312/vet/accepted/<batch_id>
-- and --hivevar db=isit312.
-- These tables point to existing Parquet data; dropping their metadata does not
-- delete the underlying files.

CREATE DATABASE IF NOT EXISTS ${hivevar:db};
USE ${hivevar:db};

DROP TABLE IF EXISTS visits;
CREATE EXTERNAL TABLE visits (
  clinic            STRING,
  clinic_patient_id STRING,
  visit_id          STRING,
  visit_date        DATE,
  visit_type        STRING,
  source_system     STRING,
  source_visit_id   STRING
)
STORED AS PARQUET
LOCATION '${hiveconf:accepted_path}/visits';

DROP TABLE IF EXISTS clinical_notes;
CREATE EXTERNAL TABLE clinical_notes (
  clinic            STRING,
  clinic_patient_id STRING,
  visit_id          STRING,
  day               INT,
  text              STRING
)
STORED AS PARQUET
LOCATION '${hiveconf:accepted_path}/clinical_notes';

DROP TABLE IF EXISTS lab_results;
CREATE EXTERNAL TABLE lab_results (
  clinic            STRING,
  clinic_patient_id STRING,
  visit_id          STRING,
  day               INT,
  reason            STRING,
  analyte           STRING,
  value             DOUBLE
)
STORED AS PARQUET
LOCATION '${hiveconf:accepted_path}/lab_results';
