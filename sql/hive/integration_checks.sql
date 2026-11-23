-- Beeline evidence for the clinical integration tables (owner: Hani).
-- Run after sql/hive/integration_tables.sql:
--   $HIVE_HOME/bin/beeline -u jdbc:hive2://localhost:10000 --hivevar db=isit312_vet -f sql/hive/integration_checks.sql
USE ${hivevar:db};

-- Rows in each table. Full TailSignal v1: 61,527 visits, 61,527 notes, 99,687 lab rows, 44 exceptions.
SELECT 'visits' AS table_name, COUNT(*) AS row_count FROM visits
UNION ALL SELECT 'clinical_notes', COUNT(*) FROM clinical_notes
UNION ALL SELECT 'lab_results', COUNT(*) FROM lab_results
UNION ALL SELECT 'integration_exceptions', COUNT(*) FROM integration_exceptions;

-- Link outcome per clinical visit. Full v1: 53,123 matched and 8,404 clinical-only.
SELECT link_status, source_system, COUNT(*) AS visits
FROM visits
GROUP BY link_status, source_system
ORDER BY link_status, source_system;

-- Rows set aside or left unmatched, by input and reason. Full v1: 44 PRACTICE_ONLY rows.
SELECT record_type, category, reason_code, COUNT(*) AS row_count
FROM integration_exceptions
GROUP BY record_type, category, reason_code
ORDER BY record_type, category, reason_code;

-- Trace the demo visit: practice encounter, clinical visit, note and lab counts.
-- Child rows are counted before the join, so the visit stays one row.
SELECT v.clinic, v.visit_id, v.visit_date, v.visit_type, v.source_system, v.source_visit_id,
       v.link_status, v.practice_source_path, n.notes, l.lab_rows
FROM visits v
LEFT JOIN (SELECT clinic, clinic_patient_id, visit_id, COUNT(*) AS notes
           FROM clinical_notes GROUP BY clinic, clinic_patient_id, visit_id) n
  ON n.clinic = v.clinic AND n.clinic_patient_id = v.clinic_patient_id AND n.visit_id = v.visit_id
LEFT JOIN (SELECT clinic, clinic_patient_id, visit_id, COUNT(*) AS lab_rows
           FROM lab_results GROUP BY clinic, clinic_patient_id, visit_id) l
  ON l.clinic = v.clinic AND l.clinic_patient_id = v.clinic_patient_id AND l.visit_id = v.visit_id
WHERE v.clinic = 'SITE-08' AND v.visit_id = 'EV0033960';

SELECT n.visit_id, n.text
FROM clinical_notes n
WHERE n.clinic = 'SITE-08' AND n.visit_id = 'EV0033960';

SELECT l.visit_id, l.reason, l.analyte, l.value
FROM lab_results l
WHERE l.clinic = 'SITE-08' AND l.visit_id = 'EV0033960'
ORDER BY l.analyte;

-- A practice encounter with no clinical visit, kept with its source file and ID.
SELECT record_type, reason_code, source_system, source_visit_id, clinic, source_path, detail
FROM integration_exceptions
WHERE reason_code = 'PRACTICE_ONLY' AND source_visit_id = 'V517829';
