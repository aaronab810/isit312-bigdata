// Clinical integration and row accounting (owner: Hani).
//
// Links practice-management encounters to clinical (EHR) visits through the TailSignal
// crosswalk, attaches original notes and lab rows on (clinic, clinic_patient_id, visit_id),
// and writes one row per accepted visit plus separate note and lab tables. Every input row
// ends up accepted or in the exceptions table with a reason, and the job prints a
// reconciliation for each input.
//
// Run inside spark-shell:  :paste spark/integrate_clinical.sc
// Or use the wrapper:      bash spark/run_integrate_clinical.sh
//
// Configuration (environment variables):
//   BATCH_ID                 required, the batch being integrated
//   ISIT312_HDFS_ROOT        default /isit312/vet
//   ISIT312_RAW_URI          default <root>/raw/<batch>  (original data/ehr and derived/ files)
//   ISIT312_PRACTICE_URI     default <root>/normalized/<batch>/practice_visits (Alina's Parquet).
//                            A path ending in .csv is read as the practice fixture with an explicit schema.
//   ISIT312_PRACTICE_SCOPE   full (default) or sample. Use sample when the practice input is a fixture,
//                            so crosswalk rows for practice records outside the sample are not invalid links.
//   ISIT312_ACCEPTED_URI     default <root>/accepted/<batch>
//   ISIT312_EXCEPTIONS_URI   default <root>/exceptions/<batch>/integrate_clinical
//   ISIT312_EXPECT_REFERENCE 1 asserts the TailSignal v1 reference totals. Defaults to 1 for full scope.

def setting(name: String, default: => String): String =
  sys.env.get(name).map(_.trim).filter(_.nonEmpty).getOrElse(default)

val hdfsRoot      = setting("ISIT312_HDFS_ROOT", "/isit312/vet").stripSuffix("/")
val batchId       = setting("BATCH_ID", sys.error("Set BATCH_ID to the batch you are integrating"))
val rawUri        = setting("ISIT312_RAW_URI", s"$hdfsRoot/raw/$batchId").stripSuffix("/")
val practiceUri   = setting("ISIT312_PRACTICE_URI", s"$hdfsRoot/normalized/$batchId/practice_visits")
val practiceScope = setting("ISIT312_PRACTICE_SCOPE", "full").toLowerCase
val acceptedUri   = setting("ISIT312_ACCEPTED_URI", s"$hdfsRoot/accepted/$batchId").stripSuffix("/")
val exceptionsUri = setting("ISIT312_EXCEPTIONS_URI", s"$hdfsRoot/exceptions/$batchId/integrate_clinical").stripSuffix("/")
require(Set("full", "sample").contains(practiceScope), "ISIT312_PRACTICE_SCOPE must be full or sample")
val expectReference = setting("ISIT312_EXPECT_REFERENCE", if (practiceScope == "full") "1" else "0") == "1"
require(batchId.matches("[A-Za-z0-9._-]+"), s"BATCH_ID has unsupported characters: $batchId")

println(s"""ISIT312 clinical integration
  batch      : $batchId
  raw        : $rawUri
  practice   : $practiceUri ($practiceScope scope)
  accepted   : $acceptedUri
  exceptions : $exceptionsUri""")

// ---------------------------------------------------------------------------------------------
// 1. Read the original inputs and set types. Identifiers are strings; visit_date comes from day.
// ---------------------------------------------------------------------------------------------
Seq(s"$rawUri/data/ehr/ehr_visits.parquet", s"$rawUri/data/ehr/ehr_notes.parquet",
    s"$rawUri/data/ehr/ehr_labs.parquet", s"$rawUri/derived/visit_crosswalk.csv", practiceUri).foreach { p =>
  val path = new org.apache.hadoop.fs.Path(p)
  require(path.getFileSystem(spark.sparkContext.hadoopConfiguration).exists(path),
    s"Input not found: $p. Check BATCH_ID and the ISIT312_* paths. For the practice input, run " +
    "spark/normalize_practice.sc for this batch first or point ISIT312_PRACTICE_URI at the fixture.")
}

spark.read.parquet(s"$rawUri/data/ehr/ehr_visits.parquet").createOrReplaceTempView("ehr_visits_raw")
spark.read.parquet(s"$rawUri/data/ehr/ehr_notes.parquet").createOrReplaceTempView("ehr_notes_raw")
spark.read.parquet(s"$rawUri/data/ehr/ehr_labs.parquet").createOrReplaceTempView("ehr_labs_raw")
// CSV inputs use an explicit schema. enforceSchema=false makes Spark check the header against it,
// so a file with renamed or reordered columns stops the job instead of shifting values into the wrong fields.
spark.read.option("header", "true").option("enforceSchema", "false")
  .schema("source_system STRING, source_visit_id STRING, clinic STRING, source_date STRING, ehr_visit_id STRING, link_method STRING")
  .csv(s"$rawUri/derived/visit_crosswalk.csv").createOrReplaceTempView("crosswalk_raw")

val practiceSchema = "source_system STRING, source_visit_id STRING, clinic STRING, source_date DATE, " +
  "source_owner_id STRING, source_patient_id STRING, invoice_total DECIMAL(12,2), source_path STRING, batch_id STRING"
val practiceInput =
  if (practiceUri.toLowerCase.endsWith(".csv"))
    spark.read.option("header", "true").option("enforceSchema", "false").option("mode", "PERMISSIVE")
      .schema(practiceSchema).csv(practiceUri)
  else spark.read.parquet(practiceUri)
practiceInput.createOrReplaceTempView("practice_raw")

spark.sql("""
  SELECT TRIM(CAST(clinic AS STRING))            AS clinic,
         TRIM(CAST(clinic_patient_id AS STRING)) AS clinic_patient_id,
         TRIM(CAST(visit_id AS STRING))          AS visit_id,
         CAST(day AS INT)                        AS day,
         date_add(DATE '2022-01-01', CAST(day AS INT)) AS visit_date,
         CAST(visit_type AS STRING)              AS visit_type,
         CAST(dx AS STRING)                      AS dx,
         CAST(procedure AS STRING)               AS procedure
  FROM ehr_visits_raw
""").createOrReplaceTempView("ehr_visits_typed")

spark.sql("""
  SELECT TRIM(CAST(clinic AS STRING))            AS clinic,
         TRIM(CAST(clinic_patient_id AS STRING)) AS clinic_patient_id,
         TRIM(CAST(visit_id AS STRING))          AS visit_id,
         CAST(day AS INT)                        AS day,
         CAST(text AS STRING)                    AS text
  FROM ehr_notes_raw
""").createOrReplaceTempView("ehr_notes_typed")

spark.sql("""
  SELECT TRIM(CAST(clinic AS STRING))            AS clinic,
         TRIM(CAST(clinic_patient_id AS STRING)) AS clinic_patient_id,
         TRIM(CAST(visit_id AS STRING))          AS visit_id,
         CAST(day AS INT)                        AS day,
         CAST(reason AS STRING)                  AS reason,
         TRIM(CAST(analyte AS STRING))           AS analyte,
         CAST(value AS DOUBLE)                   AS value
  FROM ehr_labs_raw
""").createOrReplaceTempView("ehr_labs_typed")

spark.sql("""
  SELECT TRIM(source_system)   AS source_system,
         TRIM(source_visit_id) AS source_visit_id,
         TRIM(clinic)          AS clinic,
         to_date(TRIM(source_date), 'yyyy-MM-dd') AS source_date,
         TRIM(ehr_visit_id)    AS ehr_visit_id,
         TRIM(link_method)     AS link_method
  FROM crosswalk_raw
""").createOrReplaceTempView("crosswalk_typed")

spark.sql(s"""
  SELECT TRIM(CAST(source_system AS STRING))   AS source_system,
         TRIM(CAST(source_visit_id AS STRING)) AS source_visit_id,
         TRIM(CAST(clinic AS STRING))          AS clinic,
         CAST(source_date AS DATE)             AS source_date,
         TRIM(CAST(source_owner_id AS STRING))   AS source_owner_id,
         TRIM(CAST(source_patient_id AS STRING)) AS source_patient_id,
         CAST(invoice_total AS DECIMAL(12,2))  AS invoice_total,
         COALESCE(CAST(source_path AS STRING), '$practiceUri') AS source_path
  FROM practice_raw
""").createOrReplaceTempView("practice_typed")

// ---------------------------------------------------------------------------------------------
// 2. One rule for every input: a row with a blank required field is set aside, an exact repeat
//    of a key keeps one copy and logs the extras, and copies of a key with different content are
//    all set aside because the job cannot tell which one is right.
// ---------------------------------------------------------------------------------------------
def splitByKey(input: String, output: String, keys: Seq[String], required: Seq[String]): Unit = {
  val cols = spark.table(input).columns.toSeq
  val keyList = keys.mkString(", ")
  val blank = (c: String) => s"($c IS NULL OR TRIM(CAST($c AS STRING)) = '')"
  val firstMissing = required.map(c => s"WHEN ${blank(c)} THEN '$c'").mkString("CASE ", " ", " END")
  val content = cols.filterNot(keys.contains).map(c => s"'$c', $c").mkString("to_json(named_struct(", ", ", "))")
  spark.sql(s"""
    SELECT *, $firstMissing AS _missing, $content AS _content FROM $input
  """).createOrReplaceTempView(s"${output}_marked")
  spark.sql(s"""
    SELECT ${cols.mkString(", ")},
           CASE WHEN _missing IS NOT NULL THEN 'MISSING_REQUIRED_FIELD'
                WHEN size(collect_set(_content) OVER (PARTITION BY $keyList)) > 1 THEN 'CONFLICTING_DUPLICATE'
                WHEN row_number() OVER (PARTITION BY $keyList ORDER BY _content) > 1 THEN 'EXACT_REPEAT'
           END AS reason_code,
           CASE WHEN _missing IS NOT NULL THEN concat('blank or unreadable ', _missing)
                ELSE concat('copies of key: ', CAST(count(*) OVER (PARTITION BY $keyList) AS STRING))
           END AS detail
    FROM ${output}_marked
  """).createOrReplaceTempView(s"${output}_checked")
  spark.sql(s"SELECT ${cols.mkString(", ")} FROM ${output}_checked WHERE reason_code IS NULL")
    .createOrReplaceTempView(output)
  spark.sql(s"SELECT * FROM ${output}_checked WHERE reason_code IS NOT NULL")
    .createOrReplaceTempView(s"${output}_rejects")
}

val visitKey = Seq("clinic", "clinic_patient_id", "visit_id")
splitByKey("ehr_visits_typed", "ehr_visits_ok", visitKey, visitKey :+ "day")
splitByKey("ehr_notes_typed", "ehr_notes_ok", visitKey, visitKey :+ "day" :+ "text")
splitByKey("ehr_labs_typed", "ehr_labs_ok", visitKey :+ "analyte", visitKey ++ Seq("day", "analyte", "value"))
splitByKey("crosswalk_typed", "crosswalk_keyed", Seq("source_system", "source_visit_id"),
  Seq("source_system", "source_visit_id", "clinic", "source_date", "ehr_visit_id"))
splitByKey("practice_typed", "practice_ok", Seq("source_system", "source_visit_id"),
  Seq("source_system", "source_visit_id", "clinic", "source_date"))

// A clinical visit can carry only one practice encounter. If two crosswalk rows point at the same
// visit, both are set aside as conflicting references.
spark.sql("""
  SELECT *, count(*) OVER (PARTITION BY clinic, ehr_visit_id) AS claims FROM crosswalk_keyed
""").createOrReplaceTempView("crosswalk_claims")
spark.sql("SELECT source_system, source_visit_id, clinic, source_date, ehr_visit_id, link_method FROM crosswalk_claims WHERE claims = 1")
  .createOrReplaceTempView("crosswalk_ok")
spark.sql("""
  SELECT source_system, source_visit_id, clinic, source_date, ehr_visit_id, link_method,
         'CONFLICTING_TARGET' AS reason_code,
         concat('crosswalk rows pointing at this visit: ', CAST(claims AS STRING)) AS detail
  FROM crosswalk_claims WHERE claims > 1
  UNION ALL
  SELECT source_system, source_visit_id, clinic, source_date, ehr_visit_id, link_method, reason_code, detail
  FROM crosswalk_keyed_rejects
""").createOrReplaceTempView("crosswalk_rejects")

// ---------------------------------------------------------------------------------------------
// 3. Link each practice encounter: practice key -> crosswalk -> clinical visit. The site and date
//    must agree across the practice row, the crosswalk row and the target visit.
// ---------------------------------------------------------------------------------------------
spark.sql("""
  SELECT clinic, visit_id, count(*) AS candidates,
         min(clinic_patient_id) AS clinic_patient_id, min(visit_date) AS visit_date
  FROM ehr_visits_ok GROUP BY clinic, visit_id
""").createOrReplaceTempView("visit_targets")

spark.sql("""
  SELECT p.source_system, p.source_visit_id, p.clinic, p.source_date, p.source_patient_id, p.source_path,
         x.ehr_visit_id, x.link_method, x.clinic AS xw_clinic, x.source_date AS xw_date,
         t.clinic_patient_id, t.visit_date,
         CASE
           WHEN x.source_visit_id IS NULL AND xr.source_visit_id IS NULL THEN 'PRACTICE_ONLY'
           WHEN x.source_visit_id IS NULL     THEN 'CROSSWALK_ROW_REJECTED'
           WHEN x.clinic <> p.clinic          THEN 'CLINIC_MISMATCH'
           WHEN x.source_date <> p.source_date THEN 'DATE_MISMATCH'
           WHEN t.visit_id IS NULL            THEN 'TARGET_VISIT_NOT_FOUND'
           WHEN t.candidates > 1              THEN 'TARGET_VISIT_AMBIGUOUS'
           WHEN t.visit_date <> p.source_date THEN 'DATE_MISMATCH'
           ELSE 'MATCHED'
         END AS link_outcome,
         CASE
           WHEN x.source_visit_id IS NULL AND xr.source_visit_id IS NULL THEN 'no crosswalk row for this encounter'
           WHEN x.source_visit_id IS NULL     THEN concat('crosswalk row set aside: ', xr.reason_code)
           WHEN x.clinic <> p.clinic          THEN concat('practice ', p.clinic, ' vs crosswalk ', x.clinic)
           WHEN x.source_date <> p.source_date
             THEN concat('practice ', CAST(p.source_date AS STRING), ' vs crosswalk ', CAST(x.source_date AS STRING))
           WHEN t.visit_id IS NULL            THEN concat(x.ehr_visit_id, ' is not an accepted visit at ', x.clinic)
           WHEN t.candidates > 1              THEN concat(x.ehr_visit_id, ' matches several patients at ', x.clinic)
           WHEN t.visit_date <> p.source_date
             THEN concat('practice ', CAST(p.source_date AS STRING), ' vs visit ', CAST(t.visit_date AS STRING))
         END AS detail
  FROM practice_ok p
  LEFT JOIN crosswalk_ok x
    ON x.source_system = p.source_system AND x.source_visit_id = p.source_visit_id
  LEFT JOIN (SELECT source_system, source_visit_id, min(reason_code) AS reason_code
             FROM crosswalk_rejects GROUP BY source_system, source_visit_id) xr
    ON xr.source_system = p.source_system AND xr.source_visit_id = p.source_visit_id
  LEFT JOIN visit_targets t
    ON t.visit_id = x.ehr_visit_id AND t.clinic = x.clinic
""").createOrReplaceTempView("practice_links")

// Crosswalk rows whose practice encounter is not in the practice input. In full scope these are
// invalid links; in sample scope the practice record was simply not loaded.
spark.sql("""
  SELECT x.* FROM crosswalk_ok x
  LEFT ANTI JOIN practice_ok p ON p.source_system = x.source_system AND p.source_visit_id = x.source_visit_id
""").createOrReplaceTempView("crosswalk_unused")

// Visits whose link was attempted but failed keep a null source pair and say why in link_status.
// The target visit sits at the crosswalk's clinic, which can differ from a mismatched practice row.
spark.sql("""
  SELECT xw_clinic AS clinic, ehr_visit_id AS visit_id, 'link_rejected' AS status FROM practice_links
  WHERE link_outcome NOT IN ('MATCHED', 'PRACTICE_ONLY') AND ehr_visit_id IS NOT NULL
  UNION
  SELECT clinic, ehr_visit_id, 'link_rejected' FROM crosswalk_rejects WHERE ehr_visit_id IS NOT NULL
""").createOrReplaceTempView("rejected_targets")

// ---------------------------------------------------------------------------------------------
// 4. Build the accepted tables. visits stays at one row per clinical visit.
// ---------------------------------------------------------------------------------------------
val unusedStatus = if (practiceScope == "full") "link_rejected" else "source_not_loaded"
spark.sql(s"""
  SELECT v.clinic, v.clinic_patient_id, v.visit_id, v.visit_date, v.day, v.visit_type, v.dx, v.procedure,
         m.source_system, m.source_visit_id, m.link_method,
         CASE WHEN m.source_visit_id IS NOT NULL THEN 'matched'
              WHEN r.visit_id IS NOT NULL THEN 'link_rejected'
              WHEN u.ehr_visit_id IS NOT NULL THEN '$unusedStatus'
              ELSE 'clinical_only' END                                  AS link_status,
         m.source_path                                                  AS practice_source_path,
         CASE WHEN m.source_visit_id IS NOT NULL THEN 'derived/visit_crosswalk.csv' END AS crosswalk_path,
         'data/ehr/ehr_visits.parquet'                                  AS source_path,
         '$batchId'                                                     AS batch_id
  FROM ehr_visits_ok v
  LEFT JOIN (SELECT * FROM practice_links WHERE link_outcome = 'MATCHED') m
    ON m.clinic = v.clinic AND m.clinic_patient_id = v.clinic_patient_id AND m.ehr_visit_id = v.visit_id
  LEFT JOIN (SELECT DISTINCT clinic, visit_id FROM rejected_targets) r
    ON r.clinic = v.clinic AND r.visit_id = v.visit_id
  LEFT JOIN (SELECT DISTINCT clinic, ehr_visit_id FROM crosswalk_unused) u
    ON u.clinic = v.clinic AND u.ehr_visit_id = v.visit_id
""").createOrReplaceTempView("visits_out")

// A note or lab row is accepted when its visit is accepted and both rows agree on the day.
spark.sql(s"""
  SELECT n.*, CASE WHEN v.visit_id IS NULL THEN 'ORPHAN_NOTE'
                   WHEN v.day <> n.day THEN 'DAY_MISMATCH' END AS reason_code,
         CASE WHEN v.visit_id IS NULL THEN 'no accepted visit with this key'
              WHEN v.day <> n.day THEN concat('note day ', CAST(n.day AS STRING), ' vs visit day ', CAST(v.day AS STRING))
         END AS detail
  FROM ehr_notes_ok n
  LEFT JOIN visits_out v
    ON v.clinic = n.clinic AND v.clinic_patient_id = n.clinic_patient_id AND v.visit_id = n.visit_id
""").createOrReplaceTempView("notes_checked")
spark.sql(s"""
  SELECT clinic, clinic_patient_id, visit_id, day, text,
         'data/ehr/ehr_notes.parquet' AS source_path, '$batchId' AS batch_id
  FROM notes_checked WHERE reason_code IS NULL
""").createOrReplaceTempView("clinical_notes_out")

spark.sql(s"""
  SELECT l.*, CASE WHEN v.visit_id IS NULL THEN 'ORPHAN_LAB'
                   WHEN v.day <> l.day THEN 'DAY_MISMATCH' END AS reason_code,
         CASE WHEN v.visit_id IS NULL THEN 'no accepted visit with this key'
              WHEN v.day <> l.day THEN concat('lab day ', CAST(l.day AS STRING), ' vs visit day ', CAST(v.day AS STRING))
         END AS detail
  FROM ehr_labs_ok l
  LEFT JOIN visits_out v
    ON v.clinic = l.clinic AND v.clinic_patient_id = l.clinic_patient_id AND v.visit_id = l.visit_id
""").createOrReplaceTempView("labs_checked")
spark.sql(s"""
  SELECT clinic, clinic_patient_id, visit_id, day, reason, analyte, value,
         'data/ehr/ehr_labs.parquet' AS source_path, '$batchId' AS batch_id
  FROM labs_checked WHERE reason_code IS NULL
""").createOrReplaceTempView("lab_results_out")

// ---------------------------------------------------------------------------------------------
// 5. One exceptions table for every input, at the input's own grain.
// ---------------------------------------------------------------------------------------------
val unusedCrosswalkExceptions =
  if (practiceScope == "full") """
  UNION ALL
  SELECT 'crosswalk', 'derived/visit_crosswalk.csv', 'SOURCE_RECORD_NOT_FOUND',
         clinic, CAST(NULL AS STRING), ehr_visit_id, source_system, source_visit_id,
         'practice encounter is not in the practice input'
  FROM crosswalk_unused"""
  else ""

spark.sql(s"""
  SELECT '$batchId' AS batch_id, 'integrate_clinical' AS component, e.*,
         CASE WHEN reason_code = 'PRACTICE_ONLY' THEN 'expected_unmatched'
              WHEN reason_code = 'MISSING_REQUIRED_FIELD' THEN 'missing_key'
              WHEN reason_code IN ('EXACT_REPEAT', 'CONFLICTING_DUPLICATE', 'CONFLICTING_TARGET') THEN 'duplicate_key'
              WHEN reason_code IN ('ORPHAN_NOTE', 'ORPHAN_LAB') THEN 'orphan_child'
              ELSE 'invalid_link' END AS category
  FROM (
    SELECT 'practice_visit' AS record_type, source_path, reason_code,
           clinic, CAST(NULL AS STRING) AS clinic_patient_id, CAST(NULL AS STRING) AS visit_id,
           source_system, source_visit_id, detail
    FROM practice_ok_rejects
    UNION ALL
    SELECT 'practice_visit', source_path, link_outcome, clinic, clinic_patient_id, ehr_visit_id,
           source_system, source_visit_id, detail
    FROM practice_links WHERE link_outcome <> 'MATCHED'
    UNION ALL
    SELECT 'crosswalk', 'derived/visit_crosswalk.csv', reason_code, clinic, CAST(NULL AS STRING), ehr_visit_id,
           source_system, source_visit_id, detail
    FROM crosswalk_rejects
    UNION ALL
    SELECT 'ehr_visit', 'data/ehr/ehr_visits.parquet', reason_code, clinic, clinic_patient_id, visit_id,
           CAST(NULL AS STRING), CAST(NULL AS STRING), detail
    FROM ehr_visits_ok_rejects
    UNION ALL
    SELECT 'clinical_note', 'data/ehr/ehr_notes.parquet', reason_code, clinic, clinic_patient_id, visit_id,
           CAST(NULL AS STRING), CAST(NULL AS STRING), detail
    FROM (SELECT clinic, clinic_patient_id, visit_id, reason_code, detail FROM ehr_notes_ok_rejects
          UNION ALL
          SELECT clinic, clinic_patient_id, visit_id, reason_code, detail FROM notes_checked WHERE reason_code IS NOT NULL)
    UNION ALL
    SELECT 'lab_result', 'data/ehr/ehr_labs.parquet', reason_code, clinic, clinic_patient_id, visit_id,
           CAST(NULL AS STRING), CAST(NULL AS STRING), concat(COALESCE(analyte, '?'), ': ', detail)
    FROM (SELECT clinic, clinic_patient_id, visit_id, analyte, reason_code, detail FROM ehr_labs_ok_rejects
          UNION ALL
          SELECT clinic, clinic_patient_id, visit_id, analyte, reason_code, detail FROM labs_checked WHERE reason_code IS NOT NULL)
    $unusedCrosswalkExceptions
  ) e
""").createOrReplaceTempView("exceptions_out")

// ---------------------------------------------------------------------------------------------
// 6. Write. Overwrite makes a rerun of the same batch reproduce its outputs without duplicates.
// ---------------------------------------------------------------------------------------------
Seq("visits" -> "visits_out", "clinical_notes" -> "clinical_notes_out", "lab_results" -> "lab_results_out")
  .foreach { case (table, view) => spark.table(view).write.mode("overwrite").parquet(s"$acceptedUri/$table") }
spark.table("exceptions_out").write.mode("overwrite").parquet(exceptionsUri)

// Everything below reads back what was written.
Seq("visits", "clinical_notes", "lab_results").foreach { t =>
  spark.read.parquet(s"$acceptedUri/$t").createOrReplaceTempView(t)
}
spark.read.parquet(exceptionsUri).createOrReplaceTempView("integration_exceptions")

// ---------------------------------------------------------------------------------------------
// 7. Row accounting. Each input reconciles to accepted, linked, unmatched or set-aside rows.
// ---------------------------------------------------------------------------------------------
def scalar(sql: String): Long = spark.sql(sql).first().getLong(0)
def exc(recordType: String, where: String = "TRUE"): Long =
  scalar(s"SELECT count(*) FROM integration_exceptions WHERE record_type = '$recordType' AND $where")

val in = Map(
  "practice_visit" -> scalar("SELECT count(*) FROM practice_typed"),
  "crosswalk"      -> scalar("SELECT count(*) FROM crosswalk_typed"),
  "ehr_visit"      -> scalar("SELECT count(*) FROM ehr_visits_typed"),
  "clinical_note"  -> scalar("SELECT count(*) FROM ehr_notes_typed"),
  "lab_result"     -> scalar("SELECT count(*) FROM ehr_labs_typed"))
val acceptedVisits = scalar("SELECT count(*) FROM visits")
val acceptedNotes  = scalar("SELECT count(*) FROM clinical_notes")
val acceptedLabs   = scalar("SELECT count(*) FROM lab_results")
val matched        = scalar("SELECT count(*) FROM visits WHERE link_status = 'matched'")
val clinicalOnly   = scalar("SELECT count(*) FROM visits WHERE link_status = 'clinical_only'")
val linkRejected   = scalar("SELECT count(*) FROM visits WHERE link_status = 'link_rejected'")
val notLoaded      = scalar("SELECT count(*) FROM visits WHERE link_status = 'source_not_loaded'")
val practiceOnly   = exc("practice_visit", "reason_code = 'PRACTICE_ONLY'")
val practiceInvalid = exc("practice_visit", "category = 'invalid_link'")
val practiceSetAside = exc("practice_visit", "category IN ('missing_key', 'duplicate_key')")
val crosswalkSetAside = exc("crosswalk", "reason_code <> 'SOURCE_RECORD_NOT_FOUND'")
val crosswalkUnused = scalar("SELECT count(*) FROM crosswalk_unused")
val crosswalkUsedNotMatched = scalar("""SELECT count(*) FROM practice_links
  WHERE ehr_visit_id IS NOT NULL AND link_outcome NOT IN ('MATCHED', 'PRACTICE_ONLY', 'CROSSWALK_ROW_REJECTED')""")

val reconciliation = Seq(
  ("practice encounters", "input rows", in("practice_visit")),
  ("practice encounters", "matched to a clinical visit", matched),
  ("practice encounters", "practice-only (no crosswalk row, expected)", practiceOnly),
  ("practice encounters", "invalid link (set aside)", practiceInvalid),
  ("practice encounters", "duplicate or missing key (set aside)", practiceSetAside),
  ("crosswalk rows", "input rows", in("crosswalk")),
  ("crosswalk rows", "used for a matched visit", matched),
  ("crosswalk rows", "pointed at by a practice row but failed checks", crosswalkUsedNotMatched),
  ("crosswalk rows", "duplicate, conflicting or missing key (set aside)", crosswalkSetAside),
  ("crosswalk rows", s"practice encounter not in input ($practiceScope scope)", crosswalkUnused),
  ("clinical visits", "input rows", in("ehr_visit")),
  ("clinical visits", "accepted: matched", matched),
  ("clinical visits", "accepted: clinical-only (no crosswalk row)", clinicalOnly),
  ("clinical visits", "accepted: link rejected (source pair left null)", linkRejected),
  ("clinical visits", "accepted: practice record not loaded (sample scope)", notLoaded),
  ("clinical visits", "set aside", exc("ehr_visit")),
  ("clinical notes", "input rows", in("clinical_note")),
  ("clinical notes", "accepted", acceptedNotes),
  ("clinical notes", "set aside", exc("clinical_note")),
  ("lab results", "input rows", in("lab_result")),
  ("lab results", "accepted", acceptedLabs),
  ("lab results", "set aside", exc("lab_result")))
println("\nRow accounting by input")
reconciliation.toDF("input", "outcome", "rows").show(50, false)

println("Exceptions by input and reason")
spark.sql("""
  SELECT record_type, category, reason_code, count(*) AS rows FROM integration_exceptions
  GROUP BY record_type, category, reason_code ORDER BY record_type, category, reason_code
""").show(50, false)

println("Visit count before and after linking (one row per clinical visit)")
spark.sql("""
  SELECT (SELECT count(*) FROM ehr_visits_typed) AS ehr_visit_rows_read,
         count(*)                                AS accepted_visit_rows,
         count(DISTINCT clinic, clinic_patient_id, visit_id) AS distinct_visit_keys,
         count(source_visit_id)                  AS visits_with_source_pair,
         count(DISTINCT source_system, source_visit_id) AS distinct_source_pairs
  FROM visits
""").show(false)

// Accounting identities: every input row has exactly one outcome.
require(in("practice_visit") == matched + practiceOnly + practiceInvalid + practiceSetAside,
  "Practice encounters do not reconcile")
require(in("crosswalk") == matched + crosswalkUsedNotMatched + crosswalkSetAside + crosswalkUnused,
  "Crosswalk rows do not reconcile")
require(in("ehr_visit") == acceptedVisits + exc("ehr_visit"), "Clinical visits do not reconcile")
require(acceptedVisits == matched + clinicalOnly + linkRejected + notLoaded, "Visit link statuses do not add up")
require(in("clinical_note") == acceptedNotes + exc("clinical_note"), "Clinical notes do not reconcile")
require(in("lab_result") == acceptedLabs + exc("lab_result"), "Lab results do not reconcile")
require(acceptedVisits == scalar("SELECT count(DISTINCT clinic, clinic_patient_id, visit_id) FROM visits"),
  "visits has more than one row for a clinical visit")
require(matched == scalar("SELECT count(DISTINCT source_system, source_visit_id) FROM visits WHERE source_visit_id IS NOT NULL"),
  "A practice encounter is linked to more than one visit")

if (expectReference) {
  require(in("ehr_visit") == 61527L && acceptedVisits == 61527L, s"Expected 61,527 visits, got $acceptedVisits")
  require(acceptedNotes == 61527L, s"Expected 61,527 notes, got $acceptedNotes")
  require(acceptedLabs == 99687L, s"Expected 99,687 lab rows, got $acceptedLabs")
  require(matched == 53123L, s"Expected 53,123 matched links, got $matched")
  require(practiceOnly == 44L, s"Expected 44 practice-only encounters, got $practiceOnly")
  require(clinicalOnly == 8404L, s"Expected 8,404 clinical-only visits, got $clinicalOnly")
  println("TailSignal v1 reference totals: 61,527 visits, 61,527 notes, 99,687 labs, 53,123 links, 44 practice-only, 8,404 clinical-only")
}

// ---------------------------------------------------------------------------------------------
// 8. Trace the demo visit: practice export -> crosswalk -> clinical visit -> note and labs.
// ---------------------------------------------------------------------------------------------
println("Trace EV0033960: practice export row")
spark.sql("SELECT * FROM practice_raw WHERE source_system = 'vet_alpha' AND source_visit_id = 'V533959'").show(false)
println("Trace EV0033960: crosswalk row")
spark.sql("SELECT * FROM crosswalk_typed WHERE ehr_visit_id = 'EV0033960'").show(false)
println("Trace EV0033960: accepted visit")
spark.sql("""SELECT clinic, clinic_patient_id, visit_id, visit_date, day, visit_type, source_system, source_visit_id,
  link_method, link_status, practice_source_path, source_path, batch_id FROM visits WHERE visit_id = 'EV0033960'""").show(false)
println("Trace EV0033960: original note")
spark.sql("SELECT clinic, clinic_patient_id, visit_id, day, text FROM clinical_notes WHERE visit_id = 'EV0033960'").show(false)
println("Trace EV0033960: lab rows")
spark.sql("SELECT clinic, clinic_patient_id, visit_id, day, reason, analyte, value FROM lab_results WHERE visit_id = 'EV0033960' ORDER BY analyte").show(false)
println("Trace EV0033960: visit-level summary (children counted before the join, so the visit counts once)")
val demo = spark.sql("""
  SELECT v.clinic, v.visit_id, v.visit_date, v.visit_type, v.source_system, v.source_visit_id,
         COALESCE(n.notes, 0) AS notes, COALESCE(l.labs, 0) AS lab_rows
  FROM visits v
  LEFT JOIN (SELECT clinic, clinic_patient_id, visit_id, count(*) AS notes FROM clinical_notes
             GROUP BY clinic, clinic_patient_id, visit_id) n
    ON n.clinic = v.clinic AND n.clinic_patient_id = v.clinic_patient_id AND n.visit_id = v.visit_id
  LEFT JOIN (SELECT clinic, clinic_patient_id, visit_id, count(*) AS labs FROM lab_results
             GROUP BY clinic, clinic_patient_id, visit_id) l
    ON l.clinic = v.clinic AND l.clinic_patient_id = v.clinic_patient_id AND l.visit_id = v.visit_id
  WHERE v.clinic = 'SITE-08' AND v.visit_id = 'EV0033960'
""")
demo.show(false)

val practiceHasDemo = scalar("SELECT count(*) FROM practice_typed WHERE source_system = 'vet_alpha' AND source_visit_id = 'V533959'") > 0
if (practiceHasDemo) {
  val d = demo.collect()
  require(d.length == 1, s"Expected one EV0033960 visit row, got ${d.length}")
  require(d(0).getAs[String]("source_visit_id") == "V533959" && d(0).getAs[String]("source_system") == "vet_alpha",
    "EV0033960 is not linked to vet_alpha V533959")
  require(d(0).getAs[java.sql.Date]("visit_date").toString == "2024-07-14", "EV0033960 visit date is not 2024-07-14")
  require(d(0).getAs[Long]("notes") == 1L && d(0).getAs[Long]("lab_rows") == 7L,
    "EV0033960 should have one note and seven lab rows")
}

println("Example practice-only encounter kept in the exceptions table")
spark.sql("""SELECT record_type, reason_code, category, source_system, source_visit_id, clinic, source_path, detail
  FROM integration_exceptions WHERE reason_code = 'PRACTICE_ONLY'
  ORDER BY source_system, source_visit_id LIMIT 5""").show(false)
println("Example clinical-only visit (no practice export)")
spark.sql("""SELECT clinic, clinic_patient_id, visit_id, visit_date, visit_type, source_system, source_visit_id, link_status
  FROM visits WHERE link_status = 'clinical_only' ORDER BY clinic, visit_id LIMIT 3""").show(false)

println(s"Accepted: $acceptedUri")
println(s"Exceptions: $exceptionsUri")
println("ISIT312_INTEGRATION_PASS")
