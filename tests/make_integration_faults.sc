// Builds a small fault batch for spark/integrate_clinical.sc (owner: Hani).
//
// Takes the SITE-08 July 2024 clinical visits (which include the demo visit EV0033960) from a real
// raw batch, injects one example of each problem the integration job must catch, and writes:
//   <ISIT312_RAW_URI>/data/ehr/ehr_{visits,notes,labs}.parquet and derived/visit_crosswalk.csv
//   <ISIT312_PRACTICE_URI>                     practice visits in the normalized schema
//   <ISIT312_RAW_URI>/expected/exception_counts expected exceptions, read by check_integration_faults.sc
// Every synthetic ID starts with FAULT so the injected rows are easy to spot.
//
// Run through spark/run_integrate_clinical.sh faults <batch_id>, which sets:
//   ISIT312_SOURCE_RAW_URI  the real raw batch to copy from
//   ISIT312_HDFS_ROOT, BATCH_ID, ISIT312_RAW_URI, ISIT312_PRACTICE_URI for the fault batch
import org.apache.spark.sql.functions._

def setting(name: String, default: => String): String =
  sys.env.get(name).map(_.trim).filter(_.nonEmpty).getOrElse(default)

val sourceRaw     = setting("ISIT312_SOURCE_RAW_URI", sys.error("Set ISIT312_SOURCE_RAW_URI to a real raw batch")).stripSuffix("/")
val hdfsRoot      = setting("ISIT312_HDFS_ROOT", "/isit312/vet").stripSuffix("/")
val batchId       = setting("BATCH_ID", sys.error("Set BATCH_ID for the fault batch"))
val faultRaw      = setting("ISIT312_RAW_URI", s"$hdfsRoot/raw/$batchId").stripSuffix("/")
val faultPractice = setting("ISIT312_PRACTICE_URI", s"$hdfsRoot/normalized/$batchId/practice_visits")
require(faultRaw != sourceRaw, "The fault batch must not overwrite the source raw batch")

val srcVisits = spark.read.parquet(s"$sourceRaw/data/ehr/ehr_visits.parquet")
val srcNotes  = spark.read.parquet(s"$sourceRaw/data/ehr/ehr_notes.parquet")
val srcLabs   = spark.read.parquet(s"$sourceRaw/data/ehr/ehr_labs.parquet")
val srcXw     = spark.read.option("header", "true")
  .schema("source_system STRING, source_visit_id STRING, clinic STRING, source_date STRING, ehr_visit_id STRING, link_method STRING")
  .csv(s"$sourceRaw/derived/visit_crosswalk.csv")

// SITE-08, July 2024: day 912 is 2024-07-01 and day 942 is 2024-07-31.
val v0 = srcVisits.filter(col("clinic") === "SITE-08" && col("day").between(912, 942))
val keys = Seq("clinic", "clinic_patient_id", "visit_id")
val n0 = srcNotes.join(v0.select(keys.map(col): _*), keys, "left_semi")
val l0 = srcLabs.join(v0.select(keys.map(col): _*), keys, "left_semi")
val x0 = srcXw.join(v0.select(col("clinic"), col("visit_id").as("ehr_visit_id")), Seq("clinic", "ehr_visit_id"), "left_semi")
  .select(srcXw.columns.map(col): _*)  // a join on named columns moves them first; keep the file's column order

// Pick target visits deterministically, keeping the demo visit out of the fault list.
val withLabs = l0.select("visit_id").distinct()
val linked = x0.select(col("ehr_visit_id").as("visit_id")).distinct()
val m = v0.join(linked, Seq("visit_id"), "left_semi").filter(col("visit_id") =!= "EV0033960")
  .select("visit_id").orderBy("visit_id").limit(11).collect().map(_.getString(0))
val c = v0.join(linked, Seq("visit_id"), "left_anti").join(withLabs, Seq("visit_id"), "left_anti")
  .select("visit_id").orderBy("visit_id").limit(2).collect().map(_.getString(0))
val labVisit = v0.join(withLabs, Seq("visit_id"), "left_semi").filter(!col("visit_id").isin(("EV0033960" +: m): _*))
  .select("visit_id").orderBy("visit_id").first().getString(0)
require(m.length == 11 && c.length == 2, "Not enough SITE-08 July 2024 visits to build the fault batch")
val is = (id: String) => col("visit_id") === id
val xwIs = (id: String) => col("ehr_visit_id") === id

// Clinical visits
val faultVisits = v0
  .withColumn("clinic_patient_id", when(is(c(1)), lit("")).otherwise(col("clinic_patient_id")))  // blank key
  .unionByName(v0.filter(is(m(0))))                                                             // duplicate import
  .unionByName(v0.filter(is(c(0))).withColumn("visit_type", lit("FAULT_conflicting_copy")))     // conflicting copy

// Notes
val faultNotes = n0
  .withColumn("day", when(is(m(1)), col("day") + 1).otherwise(col("day")))                      // day disagrees with visit
  .withColumn("text", when(is(m(2)), lit("")).otherwise(col("text")))                           // blank note

// Labs
val demoLab = l0.filter(is("EV0033960")).orderBy("analyte").limit(1)
val labRow  = l0.filter(is(labVisit)).orderBy("analyte").limit(1)
val faultLabs = l0
  .unionByName(demoLab)                                                                         // duplicate import
  .unionByName(labRow.withColumn("value", col("value") + 1))                                    // conflicting value
  .unionByName(labRow.withColumn("visit_id", lit("FAULT_NO_VISIT")))                            // orphan

// Crosswalk
val faultXw = x0
  .withColumn("ehr_visit_id", when(xwIs(m(3)), lit("FAULT_MISSING_VISIT"))                      // target does not exist
                              .when(xwIs(m(4)), lit(m(5)))                                      // two rows claim one visit
                              .otherwise(col("ehr_visit_id")))
  .unionByName(x0.filter(xwIs(m(9))))                                                           // duplicate row

// Practice visits in the normalized schema, one per original crosswalk row.
val practice0 = x0.select(
  col("source_system"), col("source_visit_id"), col("clinic"), to_date(col("source_date")).as("source_date"),
  lit(null).cast("string").as("source_owner_id"), lit(null).cast("string").as("source_patient_id"),
  lit("100.00").cast("decimal(12,2)").as("invoice_total"),
  concat(lit("data/pims/"), col("source_system"),
    when(col("source_system") === "vet_beta", lit("_encounters.jsonl"))
      .when(col("source_system") === "vet_gamma", lit("_visits.txt")).otherwise(lit("_visits.csv"))).as("source_path"),
  lit(batchId).as("batch_id"), col("ehr_visit_id"))
val faultPracticeRows = practice0
  .filter(!xwIs(m(8)))                                                                          // crosswalk row left without its encounter
  .withColumn("source_date", when(xwIs(m(6)), date_add(col("source_date"), 1)).otherwise(col("source_date")))
  .withColumn("clinic", when(xwIs(m(7)), lit("SITE-07")).otherwise(col("clinic")))
  .unionByName(practice0.filter(xwIs("EV0033960")))                                             // duplicate import
  .unionByName(practice0.filter(xwIs(m(10))).withColumn("invoice_total", lit("999.99").cast("decimal(12,2)")))
  .unionByName(practice0.filter(xwIs(m(0))).withColumn("source_visit_id", lit("")))             // blank key
  .unionByName(practice0.filter(xwIs("EV0033960"))
    .withColumn("source_visit_id", lit("FAULT_PRACTICE_ONLY")))                                 // no crosswalk row
  .drop("ehr_visit_id")

faultVisits.write.mode("overwrite").parquet(s"$faultRaw/data/ehr/ehr_visits.parquet")
faultNotes.write.mode("overwrite").parquet(s"$faultRaw/data/ehr/ehr_notes.parquet")
faultLabs.write.mode("overwrite").parquet(s"$faultRaw/data/ehr/ehr_labs.parquet")
faultXw.write.mode("overwrite").option("header", "true").csv(s"$faultRaw/derived/visit_crosswalk.csv")
faultPracticeRows.write.mode("overwrite").parquet(faultPractice)

val expected = Seq(
  ("ehr_visit",      "EXACT_REPEAT",            1L, s"${m(0)} imported twice"),
  ("ehr_visit",      "CONFLICTING_DUPLICATE",   2L, s"${c(0)} has two different rows"),
  ("ehr_visit",      "MISSING_REQUIRED_FIELD",  1L, s"${c(1)} has a blank clinic_patient_id"),
  ("clinical_note",  "ORPHAN_NOTE",             2L, s"notes of the set-aside visits ${c(0)} and ${c(1)}"),
  ("clinical_note",  "DAY_MISMATCH",            1L, s"${m(1)} note day differs from its visit"),
  ("clinical_note",  "MISSING_REQUIRED_FIELD",  1L, s"${m(2)} note text is blank"),
  ("lab_result",     "EXACT_REPEAT",            1L, "one EV0033960 lab row imported twice"),
  ("lab_result",     "CONFLICTING_DUPLICATE",   2L, s"$labVisit has two values for one analyte"),
  ("lab_result",     "ORPHAN_LAB",              1L, "lab row for FAULT_NO_VISIT"),
  ("crosswalk",      "EXACT_REPEAT",            1L, s"crosswalk row for ${m(9)} supplied twice"),
  ("crosswalk",      "CONFLICTING_TARGET",      2L, s"crosswalk rows for ${m(4)} and ${m(5)} both point at ${m(5)}"),
  ("crosswalk",      "SOURCE_RECORD_NOT_FOUND", 2L, s"encounters for ${m(8)} (missing) and ${m(10)} (set aside)"),
  ("practice_visit", "EXACT_REPEAT",            1L, "V533959 imported twice"),
  ("practice_visit", "CONFLICTING_DUPLICATE",   2L, s"encounter for ${m(10)} has two invoice totals"),
  ("practice_visit", "MISSING_REQUIRED_FIELD",  1L, "blank source_visit_id"),
  ("practice_visit", "PRACTICE_ONLY",           1L, "FAULT_PRACTICE_ONLY has no crosswalk row"),
  ("practice_visit", "TARGET_VISIT_NOT_FOUND",  1L, s"encounter for ${m(3)} points at FAULT_MISSING_VISIT"),
  ("practice_visit", "CROSSWALK_ROW_REJECTED",  2L, s"encounters for ${m(4)} and ${m(5)}"),
  ("practice_visit", "DATE_MISMATCH",           1L, s"encounter for ${m(6)} is one day later than the crosswalk"),
  ("practice_visit", "CLINIC_MISMATCH",         1L, s"encounter for ${m(7)} says SITE-07"),
  ("visits",         "link_rejected",           5L, s"${m(5)}, ${m(6)}, ${m(7)}, ${m(8)}, ${m(10)}"),
  ("visits",         "accepted",                v0.count() - 2, "July 2024 SITE-08 visits minus the two set aside")
).toDF("record_type", "reason_code", "expected_rows", "injected")
expected.write.mode("overwrite").parquet(s"$faultRaw/expected/exception_counts")

println(s"Fault batch $batchId written from $sourceRaw")
expected.show(30, false)
println("ISIT312_FAULT_BATCH_READY")
