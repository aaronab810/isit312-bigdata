import org.apache.spark.sql.types._

spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")

val hdfsRoot = sys.env.getOrElse("ISIT312_HDFS_ROOT", "/isit312/vet")
val batchId  = sys.env.getOrElse("BATCH_ID", "alina-dev-01")
val rawUri   = s"$hdfsRoot/raw/$batchId"


val alphaSchema = StructType(Array(
  StructField("clinic_id", StringType, true),
  StructField("client_id", StringType, true),
  StructField("patient_id", StringType, true),
  StructField("patient_name", StringType, true),
  StructField("species", StringType, true),
  StructField("breed", StringType, true),
  StructField("sex", StringType, true),
  StructField("dob", StringType, true),
  StructField("visit_id", StringType, true),
  StructField("visit_date", StringType, true),
  StructField("dx_code", StringType, true),
  StructField("dx_desc", StringType, true),
  StructField("line_items", StringType, true),
  StructField("invoice_total", StringType, true),
  StructField("weight_kg", StringType, true),
  StructField("_corrupt_record", StringType, true)
))


val alphaDF = spark.read
  .schema(alphaSchema)
  .option("header", "true")
  .option("mode", "PERMISSIVE")
  .option("columnNameOfCorruptRecord", "_corrupt_record")
  .csv(s"$rawUri/data/pims/vet_alpha_visits.csv")
  .cache()

alphaDF.printSchema()
alphaDF.select("visit_id", "clinic_id", "visit_date", "line_items", "invoice_total").show(3, false)
println("Alpha rows: " + alphaDF.count())


alphaDF.createOrReplaceTempView("alpha_raw")

val alphaVisits = spark.sql("""
  SELECT 'vet_alpha'                                AS source_system,
         TRIM(visit_id)                             AS source_visit_id,
         TRIM(clinic_id)                            AS clinic,
         to_date(TRIM(visit_date), 'yyyy-MM-dd')    AS source_date,
         TRIM(client_id)                            AS source_owner_id,
         TRIM(patient_id)                           AS source_patient_id,
	 CAST(TRIM(invoice_total) AS DECIMAL(12,2)) AS invoice_total,
         'data/pims/vet_alpha_visits.csv'           AS source_path,
         line_items,
         _corrupt_record
  FROM alpha_raw
""")

alphaVisits.createOrReplaceTempView("alpha_visits")

alphaVisits.printSchema()
alphaVisits.show(3, false)

spark.sql("""
  SELECT COUNT(*)                                                     AS total_rows,
         COUNT(_corrupt_record)                                       AS malformed,
         COUNT(CASE WHEN source_visit_id IS NULL OR source_visit_id = '' THEN 1 END) AS missing_visit_id,
         COUNT(CASE WHEN source_date IS NULL THEN 1 END)              AS bad_date,
         COUNT(CASE WHEN invoice_total IS NULL THEN 1 END)            AS bad_total
  FROM alpha_visits
""").show()




val alphaLines = spark.sql("""
  SELECT source_system,
         source_visit_id,
         pos + 1                                       AS line_number,
         token 					       AS raw_line,
	 TRIM(split(token, ':')[0])                    AS item,
         CAST(TRIM(split(token, ':')[1]) AS DECIMAL(12,2)) AS amount,
         size(split(token, ':'))                       AS parts
  FROM alpha_visits
  LATERAL VIEW posexplode(split(line_items, ';')) t AS pos, token
  WHERE _corrupt_record IS NULL
""")
alphaLines.createOrReplaceTempView("alpha_lines")
alphaLines.show(6, false)

spark.sql("""
  SELECT COUNT(*) AS total_lines,
         COUNT(CASE WHEN parts <> 2 OR item = '' OR amount IS NULL THEN 1 END) AS bad_lines
  FROM alpha_lines
""").show()

spark.sql("""
  SELECT COUNT(*) AS visits_checked,
         COUNT(CASE WHEN l.line_sum <> v.invoice_total THEN 1 END) AS total_mismatches
  FROM alpha_visits v
  JOIN (SELECT source_visit_id, SUM(amount) AS line_sum
        FROM alpha_lines GROUP BY source_visit_id) l
    ON l.source_visit_id = v.source_visit_id
""").show()

//== step 7
val betaSchema = StructType(Array(
  StructField("encounter_id", StringType, true),
  StructField("site", StringType, true),
  StructField("date", StringType, true),
  StructField("client", StructType(Array(
   StructField("id", StringType, true))), true),
  StructField("patient", StructType(Array(
    StructField("id", StringType, true))), true),
  StructField("diagnoses", ArrayType(StructType(Array(
    StructField("code", StringType, true),
    StructField("text", StringType, true)))), true),
  StructField("charges", ArrayType(StructType(Array(
    StructField("item", StringType, true),
    StructField("amount", StringType, true)))), true),
  StructField("_corrupt_record", StringType, true)
))

val betaDF = spark.read
  .schema(betaSchema)
  .option("mode", "PERMISSIVE")
  .option("columnNameOfCorruptRecord", "_corrupt_record")
  .json(s"$rawUri/data/pims/vet_beta_encounters.jsonl")
  .cache()

betaDF.printSchema()
betaDF.show(3, false)
println("Beta rows: " + betaDF.count())
betaDF.createOrReplaceTempView("beta_raw")

val betaLines = spark.sql("""
  SELECT 'vet_beta'                                AS source_system,
         TRIM(encounter_id)                        AS source_visit_id,
         pos + 1                                   AS line_number,
         TRIM(c.item)                              AS item,
         CAST(TRIM(c.amount) AS DECIMAL(12,2))     AS amount
  FROM beta_raw
  LATERAL VIEW posexplode(charges) t AS pos, c
  WHERE _corrupt_record IS NULL
""")
betaLines.createOrReplaceTempView("beta_lines")

val betaVisits = spark.sql("""
  SELECT 'vet_beta'                                AS source_system,
         TRIM(b.encounter_id)                      AS source_visit_id,
         TRIM(b.site)                              AS clinic,
         to_date(TRIM(b.`date`), 'yyyy-MM-dd')     AS source_date,
         TRIM(b.client.id)                         AS source_owner_id,
	 TRIM(b.patient.id)                        AS source_patient_id,
         l.line_sum                                AS invoice_total,
         'data/pims/vet_beta_encounters.jsonl'     AS source_path,
         l.line_count,
         b._corrupt_record
  FROM beta_raw b
  LEFT JOIN (SELECT source_visit_id, SUM(amount) AS line_sum, COUNT(*) AS line_count
             FROM beta_lines GROUP BY source_visit_id) l
    ON l.source_visit_id = TRIM(b.encounter_id)
""")
betaVisits.createOrReplaceTempView("beta_visits")
betaVisits.show(3, false)

spark.sql("""
  SELECT COUNT(*)                                                     AS total_rows,
         COUNT(_corrupt_record)                                       AS malformed,
         COUNT(CASE WHEN source_visit_id IS NULL OR source_visit_id = '' THEN 1 END) AS missing_visit_id,
         COUNT(CASE WHEN source_date IS NULL THEN 1 END)              AS bad_date,
         COUNT(CASE WHEN line_count IS NULL THEN 1 END)               AS no_charges
  FROM beta_visits
""").show()
spark.sql("""
  SELECT COUNT(*) AS total_lines,
         COUNT(CASE WHEN item IS NULL OR item = '' OR amount IS NULL THEN 1 END) AS bad_lines
  FROM beta_lines
""").show()

// ===== Step 8:
val gammaCols = "CLINIC,ACCT,PET_NAME,SPECIES,BREED,BIRTH,SEX,VISIT_NO,VISIT_DT,REASON,TOTAL".split(",")
val gammaSchema = StructType(
  gammaCols.map(c => StructField(c, StringType, true)) :+ StructField("_corrupt_record", StringType, true)
)
val gammaDF = spark.read
  .schema(gammaSchema)
  .option("header", "true")
  .option("sep", "|")
  .option("mode", "PERMISSIVE")
  .option("columnNameOfCorruptRecord", "_corrupt_record")
  .csv(s"$rawUri/data/pims/vet_gamma_visits.txt")
  .cache()

gammaDF.printSchema()
gammaDF.show(3, false)
println("Gamma rows: " + gammaDF.count())
gammaDF.createOrReplaceTempView("gamma_raw")

val gammaVisits = spark.sql("""
  SELECT 'vet_gamma'                               AS source_system,
         TRIM(VISIT_NO)                            AS source_visit_id,
         TRIM(CLINIC)                              AS clinic,
         to_date(TRIM(VISIT_DT), 'yyyy-MM-dd')     AS source_date,
         TRIM(ACCT)                                AS source_owner_id,
         CAST(NULL AS STRING)                      AS source_patient_id,
         CAST(TRIM(TOTAL) AS DECIMAL(12,2))        AS invoice_total,
         'data/pims/vet_gamma_visits.txt'          AS source_path,
         _corrupt_record
  FROM gamma_raw
""")
gammaVisits.createOrReplaceTempView("gamma_visits")
gammaVisits.show(3, false)

spark.sql("""
  SELECT COUNT(*)                                                     AS total_rows,
         COUNT(_corrupt_record)                                       AS malformed,
         COUNT(CASE WHEN source_visit_id IS NULL OR source_visit_id = '' THEN 1 END) AS missing_visit_id,
         COUNT(CASE WHEN source_date IS NULL THEN 1 END)              AS bad_date,
         COUNT(CASE WHEN invoice_total IS NULL THEN 1 END)            AS bad_total,
         COUNT(CASE WHEN source_patient_id IS NULL THEN 1 END)        AS no_patient_id
  FROM gamma_visits
""").show()


// ===== Step 9:

val normalizedUri = s"$hdfsRoot/normalized/$batchId"
val exceptionsUri = s"$hdfsRoot/exceptions/$batchId/normalize_practice"

spark.sql("""
  SELECT source_system, source_visit_id, clinic, source_date, source_owner_id,
         source_patient_id, CAST(invoice_total AS DECIMAL(12,2)) AS invoice_total,
         source_path, _corrupt_record
  FROM alpha_visits
  UNION ALL
  SELECT source_system, source_visit_id, clinic, source_date, source_owner_id,
         source_patient_id, CAST(invoice_total AS DECIMAL(12,2)) AS invoice_total,
         source_path, _corrupt_record
  FROM beta_visits
  UNION ALL
  SELECT source_system, source_visit_id, clinic, source_date, source_owner_id,
         source_patient_id, CAST(invoice_total AS DECIMAL(12,2)) AS invoice_total,
         source_path, _corrupt_record
  FROM gamma_visits
""").createOrReplaceTempView("all_visits")

spark.sql("""
  SELECT source_system, source_visit_id FROM alpha_lines
  WHERE parts <> 2 OR item IS NULL OR item = '' OR amount IS NULL
  UNION
  SELECT source_system, source_visit_id FROM beta_lines
  WHERE item IS NULL OR item = '' OR amount IS NULL
""").createOrReplaceTempView("bad_line_visits")


spark.sql("""
  SELECT source_system, source_visit_id
  FROM all_visits
  WHERE source_visit_id IS NOT NULL AND source_visit_id <> ''
  GROUP BY source_system, source_visit_id
  HAVING COUNT(*) > 1
""").createOrReplaceTempView("dup_keys")

spark.sql("""
  SELECT v.*,
         CASE
           WHEN v._corrupt_record IS NOT NULL                  THEN 'MALFORMED_RECORD'
           WHEN v.source_visit_id IS NULL OR v.source_visit_id = '' THEN 'MISSING_SOURCE_VISIT_ID'
           WHEN v.clinic IS NULL OR v.clinic = ''              THEN 'MISSING_CLINIC'
           WHEN v.source_date IS NULL                          THEN 'INVALID_DATE'
           WHEN b.source_visit_id IS NOT NULL                  THEN 'INVALID_CHARGE_LINE'
           WHEN v.invoice_total IS NULL                        THEN 'INVALID_AMOUNT'
           WHEN d.source_visit_id IS NOT NULL                  THEN 'DUPLICATE_KEY'
         END AS reason_code
  FROM all_visits v
  LEFT JOIN bad_line_visits b
    ON b.source_system = v.source_system AND b.source_visit_id = v.source_visit_id
  LEFT JOIN dup_keys d
    ON d.source_system = v.source_system AND d.source_visit_id = v.source_visit_id
""").createOrReplaceTempView("checked_visits")


spark.sql(s"""
  SELECT source_system, source_visit_id, clinic, source_date, source_owner_id,
         source_patient_id, invoice_total, source_path, '$batchId' AS batch_id
  FROM checked_visits
  WHERE reason_code IS NULL
""").write.mode("overwrite").parquet(s"$normalizedUri/practice_visits")


spark.sql("""
  SELECT l.source_system, l.source_visit_id, CAST(l.line_number AS INT) AS line_number,
         l.item, l.amount
  FROM (SELECT source_system, source_visit_id, line_number, item, amount FROM alpha_lines
        UNION ALL
	SELECT source_system, source_visit_id, line_number, item, amount FROM beta_lines) l
  LEFT SEMI JOIN (SELECT source_system, source_visit_id FROM checked_visits
                  WHERE reason_code IS NULL) a
    ON l.source_system = a.source_system AND l.source_visit_id = a.source_visit_id
""").write.mode("overwrite").parquet(s"$normalizedUri/practice_charge_lines")

spark.sql(s"""
  SELECT '$batchId' AS batch_id, 'normalize_practice' AS component,
         source_system, source_path, source_visit_id, clinic, reason_code,
         _corrupt_record AS raw_record
  FROM checked_visits
  WHERE reason_code IS NOT NULL
""").write.mode("overwrite").parquet(exceptionsUri)

spark.read.parquet(s"$normalizedUri/practice_visits").createOrReplaceTempView("practice_visits")
spark.read.parquet(s"$normalizedUri/practice_charge_lines").createOrReplaceTempView("practice_charge_lines")
spark.read.parquet(exceptionsUri).createOrReplaceTempView("practice_exceptions")
println("Reconciliation: every source row is accepted or rejected")

spark.sql("""
  SELECT c.source_system,
         COUNT(*)                                              AS source_rows,
         COUNT(CASE WHEN c.reason_code IS NULL THEN 1 END)     AS accepted,
         COUNT(CASE WHEN c.reason_code IS NOT NULL THEN 1 END) AS rejected
  FROM checked_visits c
  GROUP BY c.source_system
  ORDER BY c.source_system
""").show()

println("Written outputs")
spark.sql("""
  SELECT v.source_system, v.visits, v.invoice_total_sum, l.charge_lines, l.charge_amount_sum
  FROM (SELECT source_system, COUNT(*) AS visits, SUM(invoice_total) AS invoice_total_sum
        FROM practice_visits GROUP BY source_system) v
  LEFT JOIN (SELECT source_system, COUNT(*) AS charge_lines, SUM(amount) AS charge_amount_sum
             FROM practice_charge_lines GROUP BY source_system) l
    ON l.source_system = v.source_system
  ORDER BY v.source_system
""").show()

println("Exceptions by reaso")
spark.sql("""
  SELECT source_system, reason_code, COUNT(*) AS rows
  FROM practice_exceptions GROUP BY source_system, reason_code ORDER BY source_system, reason_code
""").show(false)


println("Demo trace: vet_alpha V533959:")
spark.sql("SELECT * FROM practice_visits WHERE source_visit_id = 'V533959'").show(false)
spark.sql("SELECT * FROM practice_charge_lines WHERE source_visit_id = 'V533959' ORDER BY line_number").show(false)

// ===== Step 12:

spark.sql("""
  SELECT 'vet_alpha' AS source_system, TRIM(visit_id) AS source_visit_id,
         1 AS diagnosis_number, TRIM(dx_code) AS dx_code, TRIM(dx_desc) AS dx_text
  FROM alpha_raw
  WHERE _corrupt_record IS NULL
  UNION ALL
  SELECT 'vet_beta', TRIM(encounter_id), pos + 1, TRIM(d.code), TRIM(d.text)
  FROM beta_raw
  LATERAL VIEW posexplode(diagnoses) t AS pos, d
  WHERE _corrupt_record IS NULL
  UNION ALL
  SELECT 'vet_gamma', TRIM(VISIT_NO), 1, CAST(NULL AS STRING), TRIM(REASON)
  FROM gamma_raw
  WHERE _corrupt_record IS NULL
""").createOrReplaceTempView("all_diagnoses")

spark.sql("""
  SELECT d.source_system, d.source_visit_id, CAST(d.diagnosis_number AS INT) AS diagnosis_number,
         d.dx_code, d.dx_text
  FROM all_diagnoses d
  LEFT SEMI JOIN (SELECT source_system, source_visit_id FROM checked_visits
                  WHERE reason_code IS NULL) a
    ON d.source_system = a.source_system AND d.source_visit_id = a.source_visit_id
""").write.mode("overwrite").parquet(s"$normalizedUri/practice_diagnoses")

spark.read.parquet(s"$normalizedUri/practice_diagnoses").createOrReplaceTempView("practice_diagnoses")

println("Diagnoses written")
spark.sql("""
  SELECT source_system,
         COUNT(*)                                      AS diagnoses,
         COUNT(DISTINCT source_visit_id)               AS visits_with_dx,
         MAX(diagnosis_number)                         AS max_per_visit,
         COUNT(CASE WHEN dx_code IS NULL THEN 1 END)   AS no_code
  FROM practice_diagnoses
  GROUP BY source_system
  ORDER BY source_system
""").show()
spark.sql("""
  SELECT * FROM practice_diagnoses
  WHERE source_visit_id IN ('V533959', 'E500001')
  ORDER BY source_system, diagnosis_number
""").show(false)
println("ISIT312_PRACTICE_PASS")
