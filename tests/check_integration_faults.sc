// Compares the integration job's output for the fault batch with the injected faults (owner: Hani).
// Run after tests/make_integration_faults.sc and spark/integrate_clinical.sc with the same settings.
import org.apache.spark.sql.functions._

def setting(name: String, default: => String): String =
  sys.env.get(name).map(_.trim).filter(_.nonEmpty).getOrElse(default)

val hdfsRoot      = setting("ISIT312_HDFS_ROOT", "/isit312/vet").stripSuffix("/")
val batchId       = setting("BATCH_ID", sys.error("Set BATCH_ID for the fault batch"))
val faultRaw      = setting("ISIT312_RAW_URI", s"$hdfsRoot/raw/$batchId").stripSuffix("/")
val acceptedUri   = setting("ISIT312_ACCEPTED_URI", s"$hdfsRoot/accepted/$batchId").stripSuffix("/")
val exceptionsUri = setting("ISIT312_EXCEPTIONS_URI", s"$hdfsRoot/exceptions/$batchId/integrate_clinical").stripSuffix("/")

val expected = spark.read.parquet(s"$faultRaw/expected/exception_counts")
val visits = spark.read.parquet(s"$acceptedUri/visits")
val actual = spark.read.parquet(exceptionsUri)
  .groupBy("record_type", "reason_code").agg(count(lit(1)).as("actual_rows"))
  .unionByName(visits.filter(col("link_status") === "link_rejected")
    .agg(count(lit(1)).as("actual_rows")).select(lit("visits").as("record_type"), lit("link_rejected").as("reason_code"), col("actual_rows")))
  .unionByName(visits.agg(count(lit(1)).as("actual_rows"))
    .select(lit("visits").as("record_type"), lit("accepted").as("reason_code"), col("actual_rows")))

val comparison = expected.join(actual, Seq("record_type", "reason_code"), "full_outer")
  .withColumn("result", when(coalesce(col("expected_rows"), lit(0L)) === coalesce(col("actual_rows"), lit(0L)), "ok").otherwise("MISMATCH"))
  .orderBy("record_type", "reason_code")
println("Injected faults compared with the integration output")
comparison.select("record_type", "reason_code", "expected_rows", "actual_rows", "result", "injected").show(40, false)
val mismatches = comparison.filter(col("result") =!= "ok").count()

println("The demo visit still counts once with one note and seven labs")
spark.read.parquet(s"$acceptedUri/clinical_notes").createOrReplaceTempView("fault_notes")
spark.read.parquet(s"$acceptedUri/lab_results").createOrReplaceTempView("fault_labs")
visits.createOrReplaceTempView("fault_visits")
val demo = spark.sql("""
  SELECT v.visit_id, v.source_system, v.source_visit_id, v.link_status,
         (SELECT count(*) FROM fault_visits WHERE visit_id = 'EV0033960') AS visit_rows,
         (SELECT count(*) FROM fault_notes WHERE visit_id = 'EV0033960') AS notes,
         (SELECT count(*) FROM fault_labs WHERE visit_id = 'EV0033960') AS lab_rows
  FROM fault_visits v WHERE v.visit_id = 'EV0033960'
""")
demo.show(false)
val d = demo.collect()

println("Sample exception rows")
spark.read.parquet(exceptionsUri)
  .select("record_type", "reason_code", "category", "clinic", "visit_id", "source_system", "source_visit_id", "detail")
  .orderBy("record_type", "reason_code", "visit_id", "source_visit_id").show(40, false)

require(mismatches == 0, s"$mismatches exception counts differ from the injected faults")
require(d.length == 1 && d(0).getAs[Long]("visit_rows") == 1L && d(0).getAs[String]("source_visit_id") == "V533959" &&
        d(0).getAs[Long]("notes") == 1L && d(0).getAs[Long]("lab_rows") == 7L,
        "EV0033960 should stay one matched visit with one note and seven labs")
println("ISIT312_FAULTS_PASS")
