// Held-out evaluation of note_mentions (Prajwal).
//
// This is the only job that reads evaluation labels. It never feeds the pipeline.
// Labels are compared at note level: a category is predicted for a note when the note has
// at least one note_mentions row with that topic and mention_status.
//
// Configuration (environment variables):
//   NOTE_MENTIONS_URI   note_mentions Parquet written by extract_note_mentions.sc
//   NOTES_URI           the notes the extraction read (Parquet, or .csv fixture); used for the note
//                       universe and for printing example notes
//   GOLD_URI            labels. A .parquet path is read as TailSignal evaluation/ehr/note_labels.parquet
//                       (visit_id + boolean label_* columns). A .csv path is read as long-format gold:
//                       visit_id,topic,mention_status (one row per labelled mention, absent = not present).
//   TEST_PERCENT        default 30. Notes with pmod(crc32(visit_id),100) < TEST_PERCENT form the test set.
//                       The split is fixed before rule tuning and only uses the visit ID.
//   EVAL_SPLITS         default "test". Use "dev,test" to print both, "all" for a small fixture.
//   EVAL_OUTPUT         optional; directory for the metrics table (CSV, overwrite)
//   EVAL_EXAMPLES       default 5; examples per category and error type
//   EVAL_MIN_F1         optional; fail if any evaluated category scores below this F1 on the reported split

import org.apache.spark.sql.types._

val mentionsUri = sys.env.getOrElse("NOTE_MENTIONS_URI", sys.error("Set NOTE_MENTIONS_URI"))
val notesUri    = sys.env.getOrElse("NOTES_URI", sys.error("Set NOTES_URI"))
val goldUri     = sys.env.getOrElse("GOLD_URI", sys.error("Set GOLD_URI"))
val testPercent = sys.env.getOrElse("TEST_PERCENT", "30").toInt
val splits      = sys.env.getOrElse("EVAL_SPLITS", "test").split(",").map(_.trim).filter(_.nonEmpty)
val nExamples   = sys.env.getOrElse("EVAL_EXAMPLES", "5").toInt
val goldIsWide  = goldUri.toLowerCase.endsWith(".parquet")

println(s"Mentions: $mentionsUri\nNotes: $notesUri\nGold: $goldUri (${if (goldIsWide) "TailSignal flags" else "long CSV"})")
println(s"Test split: pmod(crc32(visit_id),100) < $testPercent; reporting ${splits.mkString(",")}")

spark.read.parquet(mentionsUri).createOrReplaceTempView("note_mentions")

val noteSchema = StructType(Seq(
  StructField("clinic", StringType), StructField("clinic_patient_id", StringType),
  StructField("visit_id", StringType), StructField("day", IntegerType), StructField("text", StringType)))
val notes =
  if (notesUri.toLowerCase.endsWith(".csv"))
    spark.read.schema(noteSchema).option("header", "true").option("multiLine", "true")
      .option("escape", "\"").csv(notesUri)
  else spark.read.parquet(notesUri)
notes.createOrReplaceTempView("notes_all")

// Notes in scope: the same rule as the extraction job (keys present, text not blank).
spark.sql(s"""
  SELECT CAST(visit_id AS STRING) AS visit_id, text,
         CASE WHEN pmod(crc32(CAST(visit_id AS STRING)), 100) < $testPercent THEN 'test' ELSE 'dev' END AS split
  FROM notes_all
  WHERE visit_id IS NOT NULL AND TRIM(visit_id) <> '' AND clinic IS NOT NULL AND TRIM(clinic) <> ''
    AND clinic_patient_id IS NOT NULL AND TRIM(clinic_patient_id) <> ''
    AND text IS NOT NULL AND TRIM(text) <> ''
""").cache().createOrReplaceTempView("eval_notes")

// Gold labels in long form, plus the categories the labels can support.
if (goldIsWide) {
  spark.read.parquet(goldUri).createOrReplaceTempView("note_labels")
  spark.sql("""
    SELECT visit_id, 'seizure' AS topic, 'current' AS mention_status FROM note_labels WHERE label_seizure
    UNION ALL SELECT visit_id, 'seizure', 'historical' FROM note_labels WHERE label_seizure_history
    UNION ALL SELECT visit_id, 'seizure', 'negated' FROM note_labels WHERE label_seizure_negated
    UNION ALL SELECT visit_id, 'gi', 'current' FROM note_labels WHERE label_gi
    UNION ALL SELECT visit_id, 'neuro', 'current' FROM note_labels WHERE label_neuro
  """).createOrReplaceTempView("gold")
  Seq(("seizure", "current"), ("seizure", "historical"), ("seizure", "negated"), ("gi", "current"), ("neuro", "current"))
    .toDF("topic", "mention_status").createOrReplaceTempView("categories")
  val unlabelled = spark.sql(
    "SELECT count(*) FROM eval_notes e LEFT ANTI JOIN note_labels l ON e.visit_id = l.visit_id").first().getLong(0)
  println(s"Notes without a label row: $unlabelled")
  require(unlabelled == 0L, "Every evaluated note needs a label row")
} else {
  spark.read.option("header", "true").csv(goldUri).createOrReplaceTempView("gold")
  (for (t <- Seq("seizure", "gi", "neuro"); s <- Seq("current", "historical", "negated")) yield (t, s))
    .toDF("topic", "mention_status").createOrReplaceTempView("categories")
}

// Note-level predictions.
spark.sql("SELECT DISTINCT visit_id, topic, mention_status FROM note_mentions").createOrReplaceTempView("pred")

spark.sql("""
  SELECT e.visit_id, e.split, c.topic, c.mention_status,
         g.visit_id IS NOT NULL AS gold_flag, p.visit_id IS NOT NULL AS pred_flag
  FROM eval_notes e CROSS JOIN categories c
  LEFT JOIN (SELECT DISTINCT visit_id, topic, mention_status FROM gold) g
    ON g.visit_id = e.visit_id AND g.topic = c.topic AND g.mention_status = c.mention_status
  LEFT JOIN pred p
    ON p.visit_id = e.visit_id AND p.topic = c.topic AND p.mention_status = c.mention_status
""").cache().createOrReplaceTempView("eval_grid")

val splitFilter = if (splits.contains("all")) "TRUE"
                  else splits.map(s => s"'${s.replace("'", "")}'").mkString("split IN (", ",", ")")
val splitCol    = if (splits.contains("all")) "'all'" else "split"

val metrics = spark.sql(s"""
  WITH c AS (
    SELECT $splitCol AS split, topic, mention_status,
           count(*) AS notes,
           sum(CASE WHEN gold_flag THEN 1 ELSE 0 END) AS support,
           sum(CASE WHEN pred_flag THEN 1 ELSE 0 END) AS predicted,
           sum(CASE WHEN gold_flag AND pred_flag THEN 1 ELSE 0 END) AS tp,
           sum(CASE WHEN NOT gold_flag AND pred_flag THEN 1 ELSE 0 END) AS fp,
           sum(CASE WHEN gold_flag AND NOT pred_flag THEN 1 ELSE 0 END) AS fn
    FROM eval_grid WHERE $splitFilter
    GROUP BY $splitCol, topic, mention_status
  )
  SELECT *, round(tp / nullif(tp + fp, 0), 4) AS precision,
            round(tp / nullif(tp + fn, 0), 4) AS recall,
            round(2 * tp / nullif(2 * tp + fp + fn, 0), 4) AS f1
  FROM c ORDER BY split, topic, mention_status
""").cache()
metrics.createOrReplaceTempView("eval_metrics")
println("Precision and recall by category (note level):")
metrics.show(50, false)

spark.sql(s"""SELECT split, count(*) AS notes FROM eval_notes GROUP BY split ORDER BY split""").show(false)

// Why rules instead of keywords: a keyword-only seizure detector on the same notes.
val naive = """seiz|siez|\bsz\b|\bfit|convuls|tonic.clonic|grand mal"""
Seq(naive).toDF("re").createOrReplaceTempView("naive_rule")
spark.sql(s"""
  WITH k AS (
    SELECT e.visit_id, e.split, e.text RLIKE concat('(?i)', r.re) AS pred_flag,
           g.visit_id IS NOT NULL AS gold_flag
    FROM eval_notes e CROSS JOIN naive_rule r
    LEFT JOIN (SELECT DISTINCT visit_id FROM gold WHERE topic = 'seizure' AND mention_status = 'current') g
      ON g.visit_id = e.visit_id
  )
  SELECT $splitCol AS split, 'keyword only: seizure/current' AS rule,
         sum(CASE WHEN gold_flag AND pred_flag THEN 1 ELSE 0 END) AS tp,
         sum(CASE WHEN NOT gold_flag AND pred_flag THEN 1 ELSE 0 END) AS fp,
         sum(CASE WHEN gold_flag AND NOT pred_flag THEN 1 ELSE 0 END) AS fn,
         round(sum(CASE WHEN gold_flag AND pred_flag THEN 1 ELSE 0 END) /
               nullif(sum(CASE WHEN pred_flag THEN 1 ELSE 0 END), 0), 4) AS precision,
         round(sum(CASE WHEN gold_flag AND pred_flag THEN 1 ELSE 0 END) /
               nullif(sum(CASE WHEN gold_flag THEN 1 ELSE 0 END), 0), 4) AS recall
  FROM k WHERE $splitFilter GROUP BY $splitCol
""").show(false)

// Examples: one correct note per category, then missed (FN) and false (FP) mentions.
def examples(kind: String, cond: String, n: Int): Unit = {
  println(s"== $kind ==")
  spark.sql(s"""
    WITH picked AS (
      SELECT g.visit_id, g.topic, g.mention_status,
             row_number() OVER (PARTITION BY g.topic, g.mention_status ORDER BY crc32(g.visit_id)) AS rn
      FROM eval_grid g WHERE $splitFilter AND $cond
    )
    SELECT p.topic, p.mention_status, p.visit_id,
           concat_ws(' | ', collect_list(concat(m.topic, '/', m.mention_status, ': ', m.evidence))) AS predicted_evidence,
           first(n.text) AS note_text
    FROM picked p JOIN eval_notes n ON n.visit_id = p.visit_id
    LEFT JOIN note_mentions m ON m.visit_id = p.visit_id
    WHERE p.rn <= $n
    GROUP BY p.topic, p.mention_status, p.visit_id ORDER BY p.topic, p.mention_status, p.visit_id
  """).show(200, false)
}
examples("Correct example per category (true positive)", "gold_flag AND pred_flag", 1)
examples("Missed by the job (false negative)", "gold_flag AND NOT pred_flag", nExamples)
examples("Found by mistake (false positive)", "NOT gold_flag AND pred_flag", nExamples)

sys.env.get("EVAL_OUTPUT").foreach { out =>
  metrics.coalesce(1).write.mode("overwrite").option("header", "true").csv(out)
  println(s"Metrics written to $out")
}
sys.env.get("EVAL_MIN_F1").foreach { minF1 =>
  val weak = spark.sql(s"SELECT count(*) FROM eval_metrics WHERE support > 0 AND coalesce(f1, 0) < $minF1")
    .first().getLong(0)
  require(weak == 0L, s"$weak categories scored below F1 $minF1")
}

println("ISIT312_NOTE_EVAL_PASS")
