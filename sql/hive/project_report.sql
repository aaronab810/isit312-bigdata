USE ${hivevar:db};

CREATE VIEW IF NOT EXISTS monthly_visits AS
SELECT clinic, YEAR(visit_date) AS visit_year, MONTH(visit_date) AS visit_month,
       visit_type, COUNT(*) AS visit_count
FROM visits
GROUP BY clinic, YEAR(visit_date), MONTH(visit_date), visit_type;

SELECT SUM(visit_count) AS monthly_total
FROM monthly_visits;

SELECT clinic, visit_year, visit_month, visit_type, visit_count
FROM monthly_visits
WHERE clinic = 'SITE-08' AND visit_year = 2024 AND visit_month = 7
ORDER BY visit_type;

SELECT assert_true(COUNT(*) = 61527) FROM visits;
SELECT assert_true(SUM(visit_count) = 61527) FROM monthly_visits;
SELECT assert_true(COUNT(*) = 1 AND MAX(visit_count) = 20)
FROM monthly_visits
WHERE clinic = 'SITE-08' AND visit_year = 2024 AND visit_month = 7 AND visit_type = 'procedure';
