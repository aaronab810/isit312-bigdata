-- Sarib: visit-level lookup showing source identifiers, original note and labs.
-- Run with --hivevar visit_id=EV0033960 and --hivevar clinic=SITE-08.

USE ${hivevar:db};

-- Visit identity and linked source identifiers (expected: one row).
SELECT clinic, clinic_patient_id, visit_id, visit_date, visit_type,
       source_system, source_visit_id
FROM visits
WHERE clinic = '${hivevar:clinic}'
  AND visit_id = '${hivevar:visit_id}';

-- Original clinical note (expected: one row for EV0033960).
SELECT clinic, clinic_patient_id, visit_id, day,
       text AS original_clinical_note
FROM clinical_notes
WHERE clinic = '${hivevar:clinic}'
  AND visit_id = '${hivevar:visit_id}';

-- Lab results (expected: seven rows for EV0033960).
SELECT clinic, clinic_patient_id, visit_id, day,
       reason, analyte, value
FROM lab_results
WHERE clinic = '${hivevar:clinic}'
  AND visit_id = '${hivevar:visit_id}'
ORDER BY analyte;

-- Independent row counts make the visit/note/lab grain explicit.
SELECT 'visits' AS record_type, COUNT(*) AS row_count
FROM visits
WHERE clinic = '${hivevar:clinic}' AND visit_id = '${hivevar:visit_id}'
UNION ALL
SELECT 'clinical_notes', COUNT(*)
FROM clinical_notes
WHERE clinic = '${hivevar:clinic}' AND visit_id = '${hivevar:visit_id}'
UNION ALL
SELECT 'lab_results', COUNT(*)
FROM lab_results
WHERE clinic = '${hivevar:clinic}' AND visit_id = '${hivevar:visit_id}';
