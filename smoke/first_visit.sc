import org.apache.spark.sql.types._

val raw = sys.env("ISIT312_RAW_URI")
val accepted = sys.env("ISIT312_ACCEPTED_URI")

spark.read.option("header", "true").csv(raw + "/data/pims/vet_alpha_visits.csv")
  .createOrReplaceTempView("alpha_raw")
val betaSchema = StructType(Seq("encounter_id", "site", "date").map(name => StructField(name, StringType)))
spark.read.schema(betaSchema).json(raw + "/data/pims/vet_beta_encounters.jsonl")
  .createOrReplaceTempView("beta_raw")
spark.read.option("header", "true").option("sep", "|").csv(raw + "/data/pims/vet_gamma_visits.txt")
  .createOrReplaceTempView("gamma_raw")
spark.read.option("header", "true").csv(raw + "/derived/visit_crosswalk.csv")
  .createOrReplaceTempView("crosswalk")
spark.read.parquet(raw + "/data/ehr/ehr_visits.parquet").createOrReplaceTempView("clinical_raw")

spark.sql("""
  SELECT 'vet_alpha' AS source_system, visit_id AS source_visit_id, clinic_id AS clinic,
         CAST(visit_date AS DATE) AS source_date FROM alpha_raw
  UNION ALL
  SELECT 'vet_beta', encounter_id, site, CAST(`date` AS DATE) FROM beta_raw
  UNION ALL
  SELECT 'vet_gamma', VISIT_NO, CLINIC, CAST(VISIT_DT AS DATE) FROM gamma_raw
""").createOrReplaceTempView("practice_visits")

spark.sql("""
  WITH clinical AS (
    SELECT clinic, clinic_patient_id, visit_id, visit_type,
           date_add(CAST('2022-01-01' AS DATE), CAST(day AS INT)) AS visit_date
    FROM clinical_raw
  )
  SELECT c.*, p.source_system, p.source_visit_id
  FROM clinical c
  LEFT JOIN crosswalk x ON x.ehr_visit_id = c.visit_id AND x.clinic = c.clinic
                        AND CAST(x.source_date AS DATE) = c.visit_date
  LEFT JOIN practice_visits p ON p.source_system = x.source_system AND p.source_visit_id = x.source_visit_id
                             AND p.clinic = c.clinic AND p.source_date = c.visit_date
""").write.parquet(accepted + "/visits")
spark.read.parquet(raw + "/data/ehr/ehr_notes.parquet")
  .select("clinic", "clinic_patient_id", "visit_id", "day", "text").write.parquet(accepted + "/clinical_notes")
spark.read.parquet(raw + "/data/ehr/ehr_labs.parquet")
  .select("clinic", "clinic_patient_id", "visit_id", "day", "reason", "analyte", "value")
  .write.parquet(accepted + "/lab_results")

Seq("visits", "clinical_notes", "lab_results").foreach { table =>
  spark.read.parquet(accepted + "/" + table).createOrReplaceTempView(table)
}

val checks = spark.sql(new String(java.nio.file.Files.readAllBytes(
  java.nio.file.Paths.get(sys.env("ISIT312_CHECKS_SQL"))), java.nio.charset.StandardCharsets.UTF_8))
checks.show(50, false)
require(checks.filter("violation_count <> 0").count() == 0, "Integration checks failed")

val monthly = spark.sql("""
  SELECT clinic, year(visit_date) AS visit_year, month(visit_date) AS visit_month,
         visit_type, count(*) AS visit_count
  FROM visits GROUP BY clinic, year(visit_date), month(visit_date), visit_type
""")
monthly.createOrReplaceTempView("monthly_visits")
require(spark.table("visits").count() == 61527L, "Unexpected full dataset visit count")
require(spark.table("clinical_notes").count() == 61527L, "Unexpected full dataset note count")
require(spark.table("lab_results").count() == 99687L, "Unexpected full dataset lab count")
require(spark.sql("SELECT sum(visit_count) FROM monthly_visits").first().getLong(0) == 61527L,
        "Monthly report does not reconcile to visits")
val demoBucket = spark.sql("""
  SELECT * FROM monthly_visits
  WHERE clinic = 'SITE-08' AND visit_year = 2024 AND visit_month = 7 AND visit_type = 'procedure'
""")
demoBucket.show(false)
require(demoBucket.first().getAs[Long]("visit_count") == 20L, "Unexpected demo report count")
spark.sql("""SELECT v.clinic, v.visit_date, v.source_visit_id, v.visit_id, n.text
  FROM visits v JOIN clinical_notes n ON v.clinic = n.clinic AND v.clinic_patient_id = n.clinic_patient_id
  AND v.visit_id = n.visit_id WHERE v.visit_id = 'EV0033960'""").show(false)
spark.sql("SELECT analyte, value FROM lab_results WHERE visit_id = 'EV0033960'").show(false)
println("ISIT312_SMOKE_PASS")
