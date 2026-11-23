// Replays fixtures/replay in order through runReplay(...) and checks every expected result.
// Paste spark/replay_snapshot.sc first. tests/run_replay_tests.sh pastes both.
//
// Configuration (environment variables):
//   ISIT312_REPO       repository folder, default the current folder
//   ISIT312_HDFS_ROOT  HDFS root, default /isit312/vet
//   ISIT312_BASELINE   accepted baseline folder, e.g. /isit312/vet/accepted/<smoke batch>.
//                      Without it the test starts from fixtures/replay/base_visits.csv.
//   REPLAY_RUN_ID      prefix for this run's batch IDs, default replay-test-<UTC time>

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.functions.lit
import scala.util.Try

val testRepo = sys.env.getOrElse("ISIT312_REPO", new java.io.File(".").getCanonicalPath)
val testFixtures = new java.io.File(testRepo, "fixtures/replay").getCanonicalFile
def testLocal(name: String): String = new java.io.File(testFixtures, name).toURI.toString
val testRoot = sys.env.getOrElse("ISIT312_HDFS_ROOT", "/isit312/vet").stripSuffix("/")
val testRunId = sys.env.getOrElse("REPLAY_RUN_ID", "replay-test-" +
  java.time.LocalDateTime.now(java.time.ZoneOffset.UTC)
    .format(java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss")))
val testBatches = Seq("01_new_visit", "02_correction", "03_repeat", "04_late_older", "05_conflict")
def testBatchId(name: String): String = s"$testRunId-${name.replace('_', '-')}"
def testInput(name: String): String = s"$testRoot/raw/${testBatchId(name)}/replay/visit_events.csv"

var testFailures = Seq.empty[String]
def testCheck(ok: Boolean, label: String, detail: => String = ""): Unit =
  if (ok) println(s"PASS $label")
  else {
    println(s"FAIL $label $detail")
    testFailures :+= label
  }

// Starting snapshot: the full baseline, or the five-row fixture copy of it.
val (testBasePath, testMode) = sys.env.get("ISIT312_BASELINE").filter(_.trim.nonEmpty) match {
  case Some(b) => (s"${b.stripSuffix("/")}/visits", "baseline")
  case None =>
    val path = s"$testRoot/accepted/$testRunId-base/visits"
    spark.read.option("header", "true").csv(testLocal("base_visits.csv"))
      .createOrReplaceTempView("replay_test_base")
    spark.sql("""
      SELECT clinic, clinic_patient_id, visit_id, visit_type, CAST(visit_date AS DATE) AS visit_date,
             source_system, source_visit_id
      FROM replay_test_base
    """).coalesce(1).write.mode("overwrite").parquet(path)
    (path, "fixture")
}
val testBaseBatch = new Path(testBasePath).getParent.getName
println(s"REPLAY TEST run=$testRunId mode=$testMode base=$testBasePath")

// Land each fixture where the job expects a completed batch.
val testFs = new Path(testRoot).getFileSystem(spark.sparkContext.hadoopConfiguration)
testBatches.foreach { name =>
  val target = new Path(testInput(name))
  testFs.mkdirs(target.getParent)
  testFs.copyFromLocalFile(false, true, new Path(testLocal(s"$name.csv")), target)
}

def testCsv(name: String) =
  spark.read.option("header", "true").csv(testLocal(name)).collect().toSeq
val testExpectedOutcomes = testCsv("expected_outcomes.csv")
val testExpectedChanges = testCsv("expected_report_changes.csv")
val testExpectedVisits = testCsv("expected_visits.csv")
def testStr(r: org.apache.spark.sql.Row, c: String): String = Option(r.getAs[String](c)).getOrElse("")

// Row count and checksum of every column, to show a snapshot was not modified.
def testFingerprint(path: String): (Long, Long) = {
  val df = spark.read.parquet(path)
  df.createOrReplaceTempView("replay_test_fp")
  val cols = df.columns.sorted.map(c => s"COALESCE(CAST(`$c` AS STRING), '<null>')").mkString(", ")
  val r = spark.sql(s"SELECT COUNT(*), COALESCE(SUM(crc32(concat_ws('|', $cols))), 0) FROM replay_test_fp").first()
  (r.getLong(0), r.getLong(1))
}

// Monthly report changes between two snapshots, as (clinic, year, month, type) -> change.
def testReportChanges(before: String, after: String): Map[String, Long] = {
  spark.read.parquet(before).createOrReplaceTempView("replay_test_before")
  spark.read.parquet(after).createOrReplaceTempView("replay_test_after")
  spark.sql("""
    SELECT clinic, visit_year, visit_month, visit_type, SUM(n) AS change
    FROM (
      SELECT clinic, year(visit_date) AS visit_year, month(visit_date) AS visit_month, visit_type, -1 AS n
      FROM replay_test_before
      UNION ALL
      SELECT clinic, year(visit_date), month(visit_date), visit_type, 1 FROM replay_test_after
    ) t
    GROUP BY clinic, visit_year, visit_month, visit_type
    HAVING SUM(n) <> 0
  """).collect().map(r => s"${r.getString(0)}|${r.getInt(1)}|${r.getInt(2)}|${r.getString(3)}" -> r.getLong(4)).toMap
}

def testRun(name: String, prior: String, expectedStatus: String): ReplayResult = {
  println(s"\n===== $name: batch ${testBatchId(name)} =====")
  val priorBefore = testFingerprint(prior)
  val r = runReplay(testBatchId(name), prior, testInput(name), testRoot)
  testCheck(r.status == expectedStatus, s"$name status $expectedStatus", s"got ${r.status}")
  testCheck(testFingerprint(prior) == priorBefore, s"$name prior snapshot unchanged")

  val outcomes = spark.read.parquet(r.auditPath).collect()
    .map(a => s"${testStr(a, "event_id")}|${testStr(a, "outcome")}|${testStr(a, "reason_code")}").sorted.toSeq
  val expectedOutcomes = testExpectedOutcomes.filter(e => testStr(e, "batch") == name)
    .map(e => s"${testStr(e, "event_id")}|${testStr(e, "outcome")}|${testStr(e, "reason_code")}").sorted
  testCheck(outcomes == expectedOutcomes, s"$name outcomes ${outcomes.mkString(", ")}",
    s"expected ${expectedOutcomes.mkString(", ")}")

  val setAside = spark.read.parquet(r.exceptionsPath).count()
  val expectedSetAside = expectedOutcomes.count(o => replaySetAside.exists(s => o.contains(s"|$s|")))
  testCheck(setAside == expectedSetAside, s"$name set-aside rows $setAside")

  val changes = testReportChanges(prior, r.snapshotPath)
  val expectedChanges = testExpectedChanges.filter(e => testStr(e, "batch") == name).map(e =>
    Seq("clinic", "visit_year", "visit_month", "visit_type").map(testStr(e, _)).mkString("|") ->
      testStr(e, "count_change").toLong).toMap
  testCheck(changes == expectedChanges, s"$name report changes $changes", s"expected $expectedChanges")
  testCheck(r.snapshotRows - r.priorRows == expectedChanges.values.sum,
    s"$name visit rows ${r.priorRows} -> ${r.snapshotRows}")

  val snapshot = spark.read.parquet(r.snapshotPath)
  snapshot.createOrReplaceTempView("replay_test_snapshot")
  val duplicateKeys = spark.sql("""
    SELECT 1 FROM replay_test_snapshot GROUP BY clinic, clinic_patient_id, visit_id HAVING COUNT(*) > 1
  """).count()
  testCheck(duplicateKeys == 0, s"$name one row per visit key")

  testExpectedVisits.filter(e => testStr(e, "after_batch") == name).foreach { e =>
    val changedBy = testStr(e, "changed_by")
    val expected = Seq(testStr(e, "event_version"), testStr(e, "visit_date"), testStr(e, "visit_type"),
      testStr(e, "source_system"), testStr(e, "source_visit_id"),
      if (changedBy == "base") testBaseBatch else testBatchId(changedBy)).mkString("|")
    val actual = spark.sql(s"""
      SELECT CAST(event_version AS STRING), CAST(visit_date AS STRING), visit_type,
             source_system, source_visit_id, batch_id
      FROM replay_test_snapshot
      WHERE clinic = '${testStr(e, "clinic")}' AND clinic_patient_id = '${testStr(e, "clinic_patient_id")}'
        AND visit_id = '${testStr(e, "visit_id")}'
    """).collect().map(a => (0 until 6).map(i => Option(a.getString(i)).getOrElse("")).mkString("|")).toSeq
    testCheck(actual == Seq(expected), s"$name ${testStr(e, "visit_id")} is $expected",
      s"got ${actual.mkString(" / ")}")
  }
  r
}

// Run the batches in order. Each one starts from the snapshot the previous one wrote.
val testBaseBefore = testFingerprint(testBasePath)
val r01 = testRun("01_new_visit", testBasePath, "written")
val r02 = testRun("02_correction", r01.snapshotPath, "written")

println("\n===== 02_correction again with the same batch ID =====")
val r02Saved = testFingerprint(r02.snapshotPath)
val r02Again = testRun("02_correction", r01.snapshotPath, "unchanged_rerun")
testCheck(testFingerprint(r02.snapshotPath) == r02Saved, "02 rerun left the saved snapshot untouched")

val r03 = testRun("03_repeat", r02.snapshotPath, "written")
testCheck(replaySameRows(spark.read.parquet(r03.snapshotPath), spark.read.parquet(r02.snapshotPath)),
  "03 repeat snapshot is identical to the 02 snapshot")
val r04 = testRun("04_late_older", r03.snapshotPath, "written")
val r05 = testRun("05_conflict", r04.snapshotPath, "written")

println("\n===== Changed input under an existing batch ID =====")
val changedInput = Try(runReplay(testBatchId("02_correction"), r01.snapshotPath, testInput("04_late_older"),
  testRoot, verbose = false))
testCheck(changedInput.isFailure &&
  changedInput.failed.get.getMessage.contains("already has different saved results"),
  "changed input under batch 02's ID is refused", changedInput.toString)
testCheck(testFingerprint(r02.snapshotPath) == r02Saved, "02 snapshot unchanged after the refused run")

val testSnapshots = Seq("00-base" -> testBasePath) ++ Seq(r01, r02, r03, r04, r05).map(r =>
  r.batchId.stripPrefix(testRunId + "-") -> r.snapshotPath)
testCheck(testFingerprint(testBasePath) == testBaseBefore, "base snapshot unchanged after all batches")
testCheck(testSnapshots.forall { case (_, p) => replayIsComplete(p) }, "every snapshot is still saved and complete")

// The base snapshot has no replay columns yet. Its rows count as version 0 from the base batch.
def testWithReplayColumns(path: String) = {
  val df = spark.read.parquet(path)
  val withVersion = if (df.columns.contains("event_version")) df else df.withColumn("event_version", lit(0L))
  if (withVersion.columns.contains("batch_id")) withVersion
  else withVersion.withColumn("batch_id", lit(new Path(path).getParent.getName))
}

println("\nChanged visits in each saved snapshot:")
testSnapshots.map { case (label, path) =>
  testWithReplayColumns(path).where("visit_id IN ('EV0041789', 'EV0058369', 'EV9000001')")
    .selectExpr(s"'$label' AS snapshot", "visit_id", "visit_type", "CAST(event_version AS STRING) AS version",
      "batch_id")
}.reduce(_ union _).orderBy("visit_id", "snapshot").show(50, false)

println("SITE-03 March 2025 monthly counts in each saved snapshot:")
testSnapshots.map { case (label, path) =>
  spark.read.parquet(path).where("clinic = 'SITE-03' AND visit_date BETWEEN '2025-03-01' AND '2025-03-31'")
    .groupBy("visit_type").count().selectExpr(s"'$label' AS snapshot", "visit_type", "count")
}.reduce(_ union _).groupBy("snapshot").pivot("visit_type").sum("count").orderBy("snapshot").show(false)

println("Snapshot paths:")
testSnapshots.foreach { case (label, path) => println(s"  $label  $path") }
require(testFailures.isEmpty, s"Replay tests failed: ${testFailures.mkString("; ")}")
println("ISIT312_REPLAY_PASS")
