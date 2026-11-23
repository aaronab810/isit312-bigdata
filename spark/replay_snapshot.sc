// Incremental replay for the accepted visits table.
//
// Applies one batch of visit change events to a saved visits snapshot and writes a new snapshot
// for that batch. The prior snapshot is only read, so every earlier snapshot stays available.
//
// Rules (docs/data_contract.md): the key is (clinic, clinic_patient_id, visit_id). The highest
// event_version wins. An exact repeat changes nothing. A lower version that arrives late changes
// nothing. Different payloads with the same key and version are set aside and the prior accepted
// row stays. Rows in a snapshot without an event_version (the baseline) count as version 0.
// A new key that reuses a visit ID another patient already has at that clinic is set aside, so a
// visit is never counted twice. docs/replay.md lists every outcome.
//
// Run configuration (environment variables):
//   ISIT312_HDFS_ROOT    HDFS root, default /isit312/vet
//   BATCH_ID             name of this replay batch (required to run)
//   REPLAY_PRIOR         completed visits snapshot to start from (required to run),
//                        e.g. /isit312/vet/accepted/<baseline batch>/visits
//   REPLAY_INPUT         change events CSV, default <root>/raw/<BATCH_ID>/replay/visit_events.csv
//   REPLAY_OUTPUT_FILES  Parquet files in the new snapshot, default 1
//
// Outputs:
//   <root>/accepted/<BATCH_ID>/visits        new snapshot, one row per visit key
//   <root>/accepted/<BATCH_ID>/replay_audit  one row per input event with its outcome
//   <root>/exceptions/<BATCH_ID>/replay      rejected, conflicting and held events
//
// Running the same BATCH_ID again recomputes the result and compares it with the saved output.
// Identical results leave the saved files untouched. Different results stop the job.
// Without REPLAY_PRIOR this script only defines runReplay(...), which tests/run_replay_tests.sc uses.

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.hadoop.fs.Path

val replayKeyCols = Seq("clinic", "clinic_patient_id", "visit_id")
val replayPayloadCols = Seq("visit_date", "visit_type", "source_system", "source_visit_id")
val replayInputCols = Seq("event_id", "event_version", "received_at") ++ replayKeyCols ++ replayPayloadCols
val replaySetAside = Seq("rejected", "conflict_same_version", "conflict_with_snapshot", "held_by_conflict",
  "visit_id_on_other_patient")

case class ReplayResult(batchId: String, status: String, priorPath: String, snapshotPath: String,
                        auditPath: String, exceptionsPath: String, priorRows: Long, snapshotRows: Long,
                        changedKeys: Long, outcomes: Map[String, Long])

def replayFs(p: String) = new Path(p).getFileSystem(spark.sparkContext.hadoopConfiguration)
def replayQualified(p: String): String = replayFs(p).makeQualified(new Path(p)).toString
def replayIsComplete(dir: String): Boolean = replayFs(dir).exists(new Path(dir, "_SUCCESS"))
def replaySqlString(s: String): String = "'" + s.replace("\\", "\\\\").replace("'", "\\'") + "'"
def replayKeysEqual(a: String, b: String): String =
  replayKeyCols.map(c => s"$a.$c = $b.$c").mkString(" AND ")
def replayPayloadHash(alias: String): String =
  replayPayloadCols.map(c => s"COALESCE(CAST($alias.$c AS STRING), '<null>')")
    .mkString("sha2(concat_ws('|', ", ", ", "), 256)")

// Same columns, same types and the same rows, counting duplicates.
def replaySameRows(a: DataFrame, b: DataFrame): Boolean = {
  val cols = a.columns.sorted
  val types = (df: DataFrame) => cols.toSeq.map(c => df.schema(c).dataType)
  cols.sameElements(b.columns.sorted) && types(a) == types(b) && {
    val x = a.select(cols.map(col): _*)
    val y = b.select(cols.map(col): _*)
    x.exceptAll(y).isEmpty && y.exceptAll(x).isEmpty
  }
}

def replayWithDefault(df: DataFrame, name: String, value: Column): DataFrame =
  if (df.columns.contains(name)) df else df.withColumn(name, value)

// Reads the events file as text so malformed values reach the audit instead of failing the job.
def replayReadEvents(inputPath: String): DataFrame = {
  val expected = replayInputCols.mkString(",")
  val header = spark.read.textFile(inputPath).take(1).headOption.map(_.trim.stripPrefix("﻿"))
  require(header.contains(expected),
    s"$inputPath must start with the header $expected but starts with ${header.getOrElse("nothing")}")
  val schema = StructType(replayInputCols.map(StructField(_, StringType)) :+
    StructField("_corrupt_record", StringType))
  spark.read.schema(schema)
    .option("header", "true")
    .option("mode", "PERMISSIVE")
    .option("columnNameOfCorruptRecord", "_corrupt_record")
    .csv(inputPath)
    .withColumn("source_path", input_file_name())
    .withColumn("row_id", monotonically_increasing_id())
    .cache()
}

def runReplay(batchId: String, priorPath: String, inputPath: String,
              hdfsRoot: String = "/isit312/vet", outputFiles: Int = 1,
              verbose: Boolean = true): ReplayResult = {
  require(batchId.matches("[A-Za-z0-9][A-Za-z0-9._-]*"), s"BATCH_ID must be a plain folder name, got '$batchId'")
  val snapshotPath = s"$hdfsRoot/accepted/$batchId/visits"
  val auditPath = s"$hdfsRoot/accepted/$batchId/replay_audit"
  val exceptionsPath = s"$hdfsRoot/exceptions/$batchId/replay"
  val priorQualified = replayQualified(priorPath)
  require(priorQualified != replayQualified(snapshotPath),
    "REPLAY_PRIOR is this batch's own output. Point it at the previous batch's snapshot.")
  require(replayIsComplete(priorPath), s"$priorPath has no _SUCCESS marker, so it is not a completed snapshot.")

  // Prior snapshot. Baseline rows have no event metadata, so they start at version 0 and keep
  // the batch and path they came from.
  val priorRead = spark.read.parquet(priorPath)
  val missing = (replayKeyCols ++ replayPayloadCols).filterNot(priorRead.columns.contains)
  require(missing.isEmpty, s"$priorPath is missing ${missing.mkString(", ")}")
  val priorBatch = new Path(priorPath).getParent.getName
  val withVersion =
    if (priorRead.columns.contains("event_version"))
      priorRead.withColumn("event_version", coalesce(col("event_version").cast(LongType), lit(0L)))
    else priorRead.withColumn("event_version", lit(0L))
  val prior = Seq(
    "event_id" -> lit(null).cast(StringType),
    "received_at" -> lit(null).cast(TimestampType),
    "batch_id" -> lit(priorBatch),
    "source_path" -> lit(priorQualified)
  ).foldLeft(withVersion) { case (df, (name, value)) => replayWithDefault(df, name, value) }
  prior.createOrReplaceTempView("replay_prior")
  val keyList = replayKeyCols.mkString(", ")
  val priorDuplicates = spark.sql(
    s"SELECT $keyList FROM replay_prior GROUP BY $keyList HAVING COUNT(*) > 1").count()
  require(priorDuplicates == 0, s"$priorPath has $priorDuplicates duplicate visit keys")

  // Parse and validate every event row.
  replayReadEvents(inputPath).createOrReplaceTempView("replay_input")
  spark.sql("""
    SELECT *,
           CASE
             WHEN raw_record IS NOT NULL THEN 'malformed_row'
             WHEN clinic IS NULL OR clinic_patient_id IS NULL OR visit_id IS NULL THEN 'missing_key'
             WHEN event_id IS NULL THEN 'missing_event_id'
             WHEN event_version IS NULL OR event_version < 1 THEN 'invalid_event_version'
             WHEN received_at IS NULL THEN 'invalid_received_at'
             WHEN visit_date IS NULL THEN 'invalid_visit_date'
             WHEN visit_type IS NULL THEN 'missing_visit_type'
             WHEN (source_system IS NULL) <> (source_visit_id IS NULL) THEN 'partial_source_pair'
           END AS reason_code
    FROM (
      SELECT row_id, source_path, _corrupt_record AS raw_record,
             NULLIF(TRIM(event_id), '') AS event_id,
             CASE WHEN TRIM(event_version) RLIKE '^[0-9]{1,18}$'
                  THEN CAST(TRIM(event_version) AS BIGINT) END AS event_version,
             CAST(TRIM(received_at) AS TIMESTAMP) AS received_at,
             NULLIF(TRIM(clinic), '') AS clinic,
             NULLIF(TRIM(clinic_patient_id), '') AS clinic_patient_id,
             NULLIF(TRIM(visit_id), '') AS visit_id,
             CASE WHEN TRIM(visit_date) RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
                  THEN CAST(TRIM(visit_date) AS DATE) END AS visit_date,
             NULLIF(TRIM(visit_type), '') AS visit_type,
             NULLIF(TRIM(source_system), '') AS source_system,
             NULLIF(TRIM(source_visit_id), '') AS source_visit_id
      FROM replay_input
    ) typed
  """).createOrReplaceTempView("replay_checked")

  // Valid events. dup_rank picks one row among exact copies of the same version.
  spark.sql(s"""
    SELECT e.*, ${replayPayloadHash("e")} AS payload_hash,
           ROW_NUMBER() OVER (PARTITION BY e.clinic, e.clinic_patient_id, e.visit_id, e.event_version
                              ORDER BY e.received_at, e.event_id, e.row_id) AS dup_rank
    FROM replay_checked e
    WHERE e.reason_code IS NULL
  """).createOrReplaceTempView("replay_valid")

  // One row per key and version. More than one payload at the same version is a conflict.
  spark.sql("""
    SELECT clinic, clinic_patient_id, visit_id, event_version,
           COUNT(DISTINCT payload_hash) > 1 AS is_conflict
    FROM replay_valid
    GROUP BY clinic, clinic_patient_id, visit_id, event_version
  """).createOrReplaceTempView("replay_versions")

  // One row per key: the highest version in the batch against the prior snapshot. If that
  // version conflicts, the key is held at its prior row for this batch. A new key whose visit ID
  // already belongs to another patient at the clinic is also held, so the visit is not counted twice.
  spark.sql(s"""
    SELECT k.*,
           (NOT k.best_conflict AND NOT k.id_clash
            AND (NOT k.in_prior OR k.best_version > k.prior_version)) AS applies
    FROM (
      SELECT g.clinic, g.clinic_patient_id, g.visit_id,
             g.event_version AS best_version, g.is_conflict AS best_conflict,
             p.visit_id IS NOT NULL AS in_prior, p.event_version AS prior_version,
             CASE WHEN p.visit_id IS NOT NULL THEN ${replayPayloadHash("p")} END AS prior_hash,
             (p.visit_id IS NULL AND (pv.visit_id IS NOT NULL OR bv.patients > 1)) AS id_clash
      FROM (
        SELECT v.*, ROW_NUMBER() OVER (PARTITION BY clinic, clinic_patient_id, visit_id
                                       ORDER BY event_version DESC) AS rn
        FROM replay_versions v
      ) g
      LEFT JOIN replay_prior p ON ${replayKeysEqual("g", "p")}
      LEFT JOIN (SELECT DISTINCT clinic, visit_id FROM replay_prior) pv
        ON pv.clinic = g.clinic AND pv.visit_id = g.visit_id
      LEFT JOIN (SELECT clinic, visit_id, COUNT(DISTINCT clinic_patient_id) AS patients
                 FROM replay_versions GROUP BY clinic, visit_id) bv
        ON bv.clinic = g.clinic AND bv.visit_id = g.visit_id
      WHERE g.rn = 1
    ) k
  """).createOrReplaceTempView("replay_keys")

  spark.sql(s"""
    SELECT e.row_id, k.prior_version,
           CASE WHEN k.applies THEN k.best_version ELSE k.prior_version END AS result_version,
           CASE
             WHEN g.is_conflict THEN 'conflict_same_version'
             WHEN k.id_clash THEN 'visit_id_on_other_patient'
             WHEN k.applies AND e.event_version = k.best_version AND e.dup_rank = 1
               THEN CASE WHEN k.in_prior THEN 'updated' ELSE 'inserted' END
             WHEN k.applies AND e.event_version = k.best_version THEN 'duplicate_in_batch'
             WHEN k.in_prior AND e.event_version = k.prior_version AND e.payload_hash = k.prior_hash
               THEN 'repeat_no_change'
             WHEN k.in_prior AND e.event_version = k.prior_version THEN 'conflict_with_snapshot'
             WHEN k.in_prior AND e.event_version < k.prior_version THEN 'stale_late_arrival'
             WHEN k.applies THEN 'superseded_in_batch'
             ELSE 'held_by_conflict'
           END AS outcome
    FROM replay_valid e
    JOIN replay_versions g ON ${replayKeysEqual("e", "g")} AND e.event_version = g.event_version
    JOIN replay_keys k ON ${replayKeysEqual("e", "k")}
  """).createOrReplaceTempView("replay_outcomes")

  spark.sql(s"""
    SELECT e.*
    FROM replay_valid e
    JOIN replay_keys k ON ${replayKeysEqual("e", "k")}
    WHERE k.applies AND e.event_version = k.best_version AND e.dup_rank = 1
  """).createOrReplaceTempView("replay_applied")

  // New snapshot: untouched prior rows plus the applied events, in the prior snapshot's column
  // order. Columns the events do not carry keep the prior value, or stay null for a new visit.
  val fromEvent = (replayKeyCols ++ replayPayloadCols ++ Seq("event_id", "event_version", "received_at", "source_path")).toSet
  val appliedSelect = prior.columns.map { c =>
    val t = prior.schema(c).dataType.sql
    if (c == "batch_id") s"CAST(${replaySqlString(batchId)} AS $t) AS `$c`"
    else if (fromEvent(c)) s"CAST(a.`$c` AS $t) AS `$c`"
    else s"p.`$c`"
  }.mkString(", ")
  val snapshot = spark.sql(s"""
    SELECT ${prior.columns.map(c => s"p.`$c`").mkString(", ")}
    FROM replay_prior p LEFT ANTI JOIN replay_applied a ON ${replayKeysEqual("p", "a")}
    UNION ALL
    SELECT $appliedSelect
    FROM replay_applied a LEFT JOIN replay_prior p ON ${replayKeysEqual("a", "p")}
  """)

  // Audit keeps every input row as received, with its outcome.
  val audit = spark.sql(s"""
    SELECT CAST(${replaySqlString(batchId)} AS STRING) AS batch_id, i.source_path,
           CAST(${replaySqlString(priorQualified)} AS STRING) AS prior_snapshot,
           i.event_id, i.event_version, i.received_at, i.clinic, i.clinic_patient_id, i.visit_id,
           i.visit_date, i.visit_type, i.source_system, i.source_visit_id,
           o.prior_version, o.result_version,
           COALESCE(o.outcome, 'rejected') AS outcome,
           COALESCE(c.reason_code, o.outcome) AS reason_code,
           c.raw_record
    FROM replay_input i
    JOIN replay_checked c ON c.row_id = i.row_id
    LEFT JOIN replay_outcomes o ON o.row_id = i.row_id
  """)
  audit.createOrReplaceTempView("replay_audit_new")
  val exceptions = spark.sql(s"""
    SELECT batch_id, source_path, reason_code, clinic, clinic_patient_id, visit_id,
           event_id, event_version, received_at, visit_date, visit_type, source_system,
           source_visit_id, prior_version, outcome, raw_record
    FROM replay_audit_new
    WHERE outcome IN (${replaySetAside.map(replaySqlString).mkString(", ")})
  """)

  val status =
    if (replayIsComplete(snapshotPath)) {
      val same = replaySameRows(snapshot, spark.read.parquet(snapshotPath)) &&
        replayIsComplete(auditPath) && replaySameRows(audit, spark.read.parquet(auditPath)) &&
        replayIsComplete(exceptionsPath) && replaySameRows(exceptions, spark.read.parquet(exceptionsPath))
      require(same, s"Batch $batchId already has different saved results under $hdfsRoot. " +
        "Its inputs or prior snapshot changed, so run them under a new BATCH_ID.")
      "unchanged_rerun"
    } else {
      // The snapshot goes last. Its _SUCCESS marker means the whole batch is complete.
      exceptions.coalesce(1).write.mode("overwrite").parquet(exceptionsPath)
      audit.coalesce(1).write.mode("overwrite").parquet(auditPath)
      snapshot.coalesce(outputFiles).write.mode("overwrite").parquet(snapshotPath)
      "written"
    }

  spark.read.parquet(snapshotPath).createOrReplaceTempView("replay_snapshot_new")
  val outcomes = audit.groupBy("outcome").count().collect().map(r => r.getString(0) -> r.getLong(1)).toMap
  val result = ReplayResult(batchId, status, priorQualified, replayQualified(snapshotPath),
    replayQualified(auditPath), replayQualified(exceptionsPath), spark.table("replay_prior").count(),
    spark.table("replay_snapshot_new").count(), spark.table("replay_applied").count(), outcomes)

  if (verbose) {
    println(s"REPLAY batch=$batchId status=$status prior_rows=${result.priorRows} " +
      s"snapshot_rows=${result.snapshotRows} changed_keys=${result.changedKeys}")
    println("REPLAY outcomes: " + outcomes.toSeq.sorted.map { case (k, v) => s"$k=$v" }.mkString(", "))
    println(s"REPLAY prior: $priorQualified")
    println(s"REPLAY snapshot: ${result.snapshotPath}")
    spark.sql("""
      SELECT event_id, visit_id, event_version, prior_version, result_version, outcome, reason_code
      FROM replay_audit_new ORDER BY source_path, event_id, outcome
    """).show(50, false)
    println("Changed visits, before and after:")
    spark.sql(s"""
      SELECT a.clinic, a.visit_id,
             b.visit_date AS before_date, b.visit_type AS before_type, b.event_version AS before_version,
             a.visit_date AS after_date, a.visit_type AS after_type, a.event_version AS after_version
      FROM replay_applied x
      JOIN replay_snapshot_new a ON ${replayKeysEqual("x", "a")}
      LEFT JOIN replay_prior b ON ${replayKeysEqual("x", "b")}
      ORDER BY a.clinic, a.visit_id
    """).show(50, false)
    println("Monthly report rows that change (clinic, month, visit_type):")
    spark.sql("""
      WITH b AS (
        SELECT clinic, year(visit_date) AS visit_year, month(visit_date) AS visit_month, visit_type,
               COUNT(*) AS n
        FROM replay_prior GROUP BY clinic, year(visit_date), month(visit_date), visit_type
      ), a AS (
        SELECT clinic, year(visit_date) AS visit_year, month(visit_date) AS visit_month, visit_type,
               COUNT(*) AS n
        FROM replay_snapshot_new GROUP BY clinic, year(visit_date), month(visit_date), visit_type
      )
      SELECT COALESCE(a.clinic, b.clinic) AS clinic, COALESCE(a.visit_year, b.visit_year) AS visit_year,
             COALESCE(a.visit_month, b.visit_month) AS visit_month,
             COALESCE(a.visit_type, b.visit_type) AS visit_type,
             COALESCE(b.n, 0) AS before_count, COALESCE(a.n, 0) AS after_count
      FROM a FULL OUTER JOIN b
        ON a.clinic <=> b.clinic AND a.visit_year <=> b.visit_year
       AND a.visit_month <=> b.visit_month AND a.visit_type <=> b.visit_type
      WHERE COALESCE(a.n, 0) <> COALESCE(b.n, 0)
      ORDER BY clinic, visit_year, visit_month, visit_type
    """).show(50, false)
  }
  result
}

sys.env.get("REPLAY_PRIOR") match {
  case Some(priorPath) =>
    val replayRoot = sys.env.getOrElse("ISIT312_HDFS_ROOT", "/isit312/vet")
    val replayBatch = sys.env.getOrElse("BATCH_ID",
      throw new IllegalArgumentException("Set BATCH_ID to the replay batch name"))
    val replayInput = sys.env.getOrElse("REPLAY_INPUT", s"$replayRoot/raw/$replayBatch/replay/visit_events.csv")
    val replayFiles = sys.env.getOrElse("REPLAY_OUTPUT_FILES", "1").toInt
    runReplay(replayBatch, priorPath, replayInput, replayRoot, replayFiles)
    println("ISIT312_REPLAY_DONE")
  case None =>
    println("Loaded runReplay(...). Set REPLAY_PRIOR and BATCH_ID to apply a batch.")
}
