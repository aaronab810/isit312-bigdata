-- Sarib: monthly visit counts and note coverage.
-- Child rows are aggregated to one row per visit before joining, so multiple
-- notes/labs cannot multiply the visit count.

USE ${hivevar:db};

DROP VIEW IF EXISTS monthly_visits;
CREATE VIEW monthly_visits AS
SELECT
  v.clinic,
  YEAR(v.visit_date) AS visit_year,
  MONTH(v.visit_date) AS visit_month,
  v.visit_type,
  COUNT(*) AS visit_count,
  SUM(CASE WHEN COALESCE(n.note_count, 0) > 0 THEN 1 ELSE 0 END) AS visits_with_notes
FROM visits v
LEFT JOIN (
  SELECT clinic, clinic_patient_id, visit_id, COUNT(*) AS note_count
  FROM clinical_notes
  GROUP BY clinic, clinic_patient_id, visit_id
) n
  ON n.clinic = v.clinic
 AND n.clinic_patient_id = v.clinic_patient_id
 AND n.visit_id = v.visit_id
GROUP BY
  v.clinic, YEAR(v.visit_date), MONTH(v.visit_date), v.visit_type;

-- Monthly report by clinic, year, month and visit type.
SELECT clinic, visit_year, visit_month, visit_type,
       visit_count, visits_with_notes
FROM monthly_visits
ORDER BY clinic, visit_year, visit_month, visit_type;

-- Reconciliation: expected baseline total is 61,527.
SELECT SUM(visit_count) AS monthly_visit_total,
       SUM(visits_with_notes) AS monthly_visits_with_notes
FROM monthly_visits;

-- Required reference bucket: SITE-08, July 2024, procedure = 20 visits.
SELECT clinic, visit_year, visit_month, visit_type, visit_count, visits_with_notes
FROM monthly_visits
WHERE clinic = 'SITE-08'
  AND visit_year = 2024
  AND visit_month = 7
  AND visit_type = 'procedure';

-- Assertions against the saved baseline evidence.
SELECT assert_true(COUNT(*) = 61527) AS visit_count_check
FROM visits;

SELECT assert_true(SUM(visit_count) = 61527) AS monthly_total_check
FROM monthly_visits;

SELECT assert_true(COUNT(*) = 1 AND MAX(visit_count) = 20) AS site_month_check
FROM monthly_visits
WHERE clinic = 'SITE-08'
  AND visit_year = 2024
  AND visit_month = 7
  AND visit_type = 'procedure';
