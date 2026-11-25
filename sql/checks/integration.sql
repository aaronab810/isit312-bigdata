-- Run as one Spark SQL query after registering visits, clinical_notes, and lab_results.
-- A zero violation count passes. The three demo checks expect data containing the TailSignal demo.
WITH valid_visits AS (
    SELECT clinic, clinic_patient_id, visit_id
    FROM visits
    WHERE TRIM(clinic) <> '' AND TRIM(clinic_patient_id) <> '' AND TRIM(visit_id) <> ''
),
visit_keys AS (
    SELECT clinic, clinic_patient_id, visit_id, COUNT(*) AS visit_count
    FROM valid_visits
    GROUP BY clinic, clinic_patient_id, visit_id
),
note_counts AS (
    SELECT clinic, clinic_patient_id, visit_id, COUNT(*) AS note_count,
           COUNT(CASE WHEN TRIM(text) <> '' THEN 1 END) AS nonblank_note_count
    FROM clinical_notes
    GROUP BY clinic, clinic_patient_id, visit_id
),
demo_keys AS (
    SELECT clinic, clinic_patient_id, visit_id
    FROM visit_keys
    WHERE clinic = 'SITE-08' AND visit_id = 'EV0033960'
)
SELECT 'visits_missing_key' AS check_name, COUNT(*) AS violation_count
FROM visits
WHERE clinic IS NULL OR TRIM(clinic) = ''
   OR clinic_patient_id IS NULL OR TRIM(clinic_patient_id) = ''
   OR visit_id IS NULL OR TRIM(visit_id) = ''

UNION ALL
SELECT 'duplicate_visit_keys', COUNT(*)
FROM visit_keys
WHERE visit_count > 1

UNION ALL
SELECT 'visits_without_one_original_note', COUNT(*)
FROM visit_keys v
LEFT JOIN note_counts n
  ON n.clinic = v.clinic AND n.clinic_patient_id = v.clinic_patient_id AND n.visit_id = v.visit_id
WHERE COALESCE(n.note_count, 0) <> 1 OR COALESCE(n.nonblank_note_count, 0) <> 1

UNION ALL
SELECT 'orphan_notes', COUNT(*)
FROM clinical_notes n
LEFT JOIN visit_keys v
  ON n.clinic = v.clinic AND n.clinic_patient_id = v.clinic_patient_id AND n.visit_id = v.visit_id
WHERE v.visit_id IS NULL

UNION ALL
SELECT 'orphan_labs', COUNT(*)
FROM lab_results l
LEFT JOIN visit_keys v
  ON l.clinic = v.clinic AND l.clinic_patient_id = v.clinic_patient_id AND l.visit_id = v.visit_id
WHERE v.visit_id IS NULL

UNION ALL
SELECT 'demo_visit_identity_mismatch',
       CASE WHEN COUNT(*) = 1 AND COUNT(CASE
           WHEN visit_date = CAST('2024-07-14' AS DATE)
            AND source_system = 'vet_alpha'
            AND source_visit_id = 'V533959'
            AND TRIM(clinic_patient_id) <> '' THEN 1
       END) = 1 THEN 0 ELSE 1 END
FROM visits
WHERE clinic = 'SITE-08' AND visit_id = 'EV0033960'

UNION ALL
SELECT 'demo_note_count_mismatch',
       CASE WHEN COALESCE(SUM(n.note_count), 0) = 1
             AND COALESCE(SUM(n.nonblank_note_count), 0) = 1 THEN 0 ELSE 1 END
FROM demo_keys d
JOIN note_counts n
  ON n.clinic = d.clinic AND n.clinic_patient_id = d.clinic_patient_id AND n.visit_id = d.visit_id

UNION ALL
SELECT 'demo_lab_count_mismatch', CASE WHEN COUNT(*) = 7 THEN 0 ELSE 1 END
FROM demo_keys d
JOIN lab_results l
  ON l.clinic = d.clinic AND l.clinic_patient_id = d.clinic_patient_id AND l.visit_id = d.visit_id
