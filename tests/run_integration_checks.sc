import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

val baseline = sys.env("ISIT312_BASELINE")
val checksPath = java.nio.file.Paths.get(sys.env("ISIT312_CHECKS_SQL"))
val checksSql = new String(
  java.nio.file.Files.readAllBytes(checksPath),
  java.nio.charset.StandardCharsets.UTF_8
)

val visits = spark.read.parquet(s"$baseline/visits")
val notes = spark.read.parquet(s"$baseline/clinical_notes")
val labs = spark.read.parquet(s"$baseline/lab_results")
val demo = col("clinic") === "SITE-08" && col("visit_id") === "EV0033960"

def runCase(
    label: String,
    caseVisits: DataFrame,
    caseNotes: DataFrame,
    caseLabs: DataFrame,
    expected: Map[String, Long]
): Unit = {
  caseVisits.createOrReplaceTempView("visits")
  caseNotes.createOrReplaceTempView("clinical_notes")
  caseLabs.createOrReplaceTempView("lab_results")

  val rows = spark.sql(checksSql).collect()
  val counts = rows.map { row => row.getString(0) -> row.getLong(1) }.toMap
  val violations = counts.filter { case (_, count) => count != 0L }

  require(rows.length == 8 && counts.size == 8, s"$label: expected eight distinct checks")
  require(violations == expected, s"$label: expected $expected, got $violations")

  val result = if (violations.isEmpty) "8 checks at zero"
               else violations.toSeq.map { case (name, count) => s"$name=$count" }.mkString(", ")
  println(s"PASS $label | $result")
}

runCase("clean baseline", visits, notes, labs, Map.empty[String, Long])

val duplicate = visits.filter(col("visit_id") =!= "EV0033960").limit(1)
runCase("duplicate visit", visits.unionByName(duplicate), notes, labs,
  Map("duplicate_visit_keys" -> 1L))

val blankedNotes = notes.withColumn("text", when(demo, lit("")).otherwise(col("text")))
runCase("blank note", visits, blankedNotes, labs,
  Map("visits_without_one_original_note" -> 1L, "demo_note_count_mismatch" -> 1L))

val orphan = labs.limit(1).withColumn("visit_id", lit("__regression_missing_visit__"))
runCase("orphan lab", visits, notes, labs.unionByName(orphan),
  Map("orphan_labs" -> 1L))

val wrongSource = visits.withColumn(
  "source_visit_id",
  when(demo, lit("__wrong_source_id__")).otherwise(col("source_visit_id"))
)
runCase("wrong source ID", wrongSource, notes, labs,
  Map("demo_visit_identity_mismatch" -> 1L))

println("ISIT312_REGRESSION_PASS")
