// Note mention extraction (Prajwal).
//
// Reads the original clinical notes and writes one row per symptom mention to
// accepted/<batch_id>/note_mentions with mention_status current, historical or negated.
// The rules are plain Java regular expressions applied inside Spark SQL.
// Evaluation labels are never read here. See spark/evaluate_note_mentions.sc.
//
// Run: printf ':paste spark/extract_note_mentions.sc\n:quit\n' | spark-shell --master local[2]
// or use spark/run_note_mentions.sh, which sets the variables below.
//
// Configuration (environment variables):
//   BATCH_ID                     required; identifies the accepted batch
//   ISIT312_HDFS_ROOT            default /isit312/vet
//   NOTE_MENTIONS_INPUT          default <root>/accepted/<batch>/clinical_notes (Parquet).
//                                A path ending in .csv is read as a fixture with an explicit schema.
//   NOTE_MENTIONS_OUTPUT         default <root>/accepted/<batch>/note_mentions
//   NOTE_MENTIONS_EXCEPTIONS     default <root>/exceptions/<batch>/note_mentions
//   NOTE_SOURCE_PATH             archive-relative origin recorded on each row, default data/ehr/ehr_notes.parquet
//   NOTE_MENTIONS_EXPECT_NOTES       optional; assert the number of notes read
//   NOTE_MENTIONS_EXPECT_EXCEPTIONS  optional; assert the number of excepted notes

import org.apache.spark.sql.types._

val ruleVersion = "note-rules-v2"
val hdfsRoot    = sys.env.getOrElse("ISIT312_HDFS_ROOT", "/isit312/vet")
val batchId     = sys.env.getOrElse("BATCH_ID", sys.error("Set BATCH_ID"))
val inputUri    = sys.env.getOrElse("NOTE_MENTIONS_INPUT", s"$hdfsRoot/accepted/$batchId/clinical_notes")
val outputUri   = sys.env.getOrElse("NOTE_MENTIONS_OUTPUT", s"$hdfsRoot/accepted/$batchId/note_mentions")
val exceptUri   = sys.env.getOrElse("NOTE_MENTIONS_EXCEPTIONS", s"$hdfsRoot/exceptions/$batchId/note_mentions")
val sourcePath  = sys.env.getOrElse("NOTE_SOURCE_PATH", "data/ehr/ehr_notes.parquet")

println(s"Batch: $batchId\nInput: $inputUri\nOutput: $outputUri\nExceptions: $exceptUri\nRules: $ruleVersion")

// ---------------------------------------------------------------------------
// 1. Original notes. Only the key columns and text are used; the text is not changed.
// ---------------------------------------------------------------------------
val noteSchema = StructType(Seq(
  StructField("clinic", StringType), StructField("clinic_patient_id", StringType),
  StructField("visit_id", StringType), StructField("day", IntegerType), StructField("text", StringType)))

val notesIn =
  if (inputUri.toLowerCase.endsWith(".csv"))
    spark.read.schema(noteSchema).option("header", "true").option("multiLine", "true")
      .option("escape", "\"").csv(inputUri)
  else spark.read.parquet(inputUri).select("clinic", "clinic_patient_id", "visit_id", "day", "text")
notesIn.createOrReplaceTempView("notes_in")

// Keys are strings. A note needs all three key parts and non-blank text. Duplicate keys are set aside.
spark.sql("""
  SELECT CAST(clinic AS STRING) AS clinic, CAST(clinic_patient_id AS STRING) AS clinic_patient_id,
         CAST(visit_id AS STRING) AS visit_id, text,
         CASE WHEN clinic IS NULL OR TRIM(clinic) = '' OR clinic_patient_id IS NULL
                   OR TRIM(clinic_patient_id) = '' OR visit_id IS NULL OR TRIM(visit_id) = ''
              THEN 'MISSING_KEY'
              WHEN text IS NULL OR TRIM(text) = '' THEN 'BLANK_TEXT'
              WHEN COUNT(*) OVER (PARTITION BY clinic, clinic_patient_id, visit_id) > 1
              THEN 'DUPLICATE_NOTE_KEY'
         END AS reason_code
  FROM notes_in
""").cache().createOrReplaceTempView("notes_checked")

// ---------------------------------------------------------------------------
// 2. Rules. One row per topic. Patterns are case-insensitive Java regex.
//    keyword_re   words that name the symptom (abbreviations and common misspellings included)
//    exclude_re   phrases removed before matching because they contain a keyword but mean something else
//    neg_re       cues that negate a keyword when they appear before it in the same sentence
//    post_neg_re  cues that negate a keyword when they appear after it ("seizure ruled out")
//    hist_re      cues that place the mention in the past
// ---------------------------------------------------------------------------
// v2 (tuned on the dev split only): "\bO\s+d[a-z]{2,5}" accepts misspelt "O denies" such as
// "O dnies" or "O deines", the most common dev-split error in v1.
val negRe     = """(\b(no|nil|denies|denied|deny|not|without|negative for|absence of|ruled out|r/o)\b|\bO\s+d[a-z]{2,5}\b)"""
val postNegRe = """\b(ruled out|negative|absent|not seen|not observed)\b"""
val histRe    = """\b(hx|history|historical|known epileptic|previous|previously|prior|resolved|last month|last year|in the past)\b"""

val rules = Seq(
  ("seizure",
   """seiz|siez|sezi|eizure|\bsz\b|\bfit(s|ting)?\b|convuls|tonic.clonic|grand mal|ictal|epilep""",
   """\bfit\s*(and|&|n)\s*well\b|\bfitted\b"""),
  ("gi",
   """vomit|vommit|\bv\+|\bd\+|\bv/d\b|diarr|diarh|melena|melaena|loose stool|\bgi upset|gastroent|\bemesis\b""",
   ""),
  ("neuro",
   """ataxi|tremor|twitch|wobbl|head tilt|nystagmus|paresis|proprioceptive deficit""",
   "")
).map { case (topic, kw, ex) => (topic, kw, ex, negRe, postNegRe, histRe) }
  .toDF("topic", "keyword_re", "exclude_re", "neg_re", "post_neg_re", "hist_re")
rules.createOrReplaceTempView("note_rules")
rules.show(false)

// ---------------------------------------------------------------------------
// 3. Split each accepted note into sentences, keeping the sentence number.
// ---------------------------------------------------------------------------
spark.sql("""
  SELECT n.clinic, n.clinic_patient_id, n.visit_id, s.pos + 1 AS sentence_no, TRIM(s.sentence) AS sentence
  FROM notes_checked n
  LATERAL VIEW posexplode(split(n.text, '(?<=[.!?;])\\s+')) s AS pos, sentence
  WHERE n.reason_code IS NULL AND TRIM(s.sentence) <> ''
""").createOrReplaceTempView("note_sentences")

// ---------------------------------------------------------------------------
// 4. Apply the rules. Status precedence inside one sentence: negated, then historical, then current.
//    Negation counts only when the cue is before the keyword (within 40 characters, same sentence)
//    or a post-negation cue follows it, so "Mild ataxia, no other neuro deficits" stays current.
// ---------------------------------------------------------------------------
spark.sql("""
  WITH scanned AS (
    SELECT s.*, r.topic, r.keyword_re, r.neg_re, r.post_neg_re, r.hist_re,
           CASE WHEN r.exclude_re = '' THEN s.sentence
                ELSE regexp_replace(s.sentence, concat('(?i)', r.exclude_re), ' ') END AS scan_text
    FROM note_sentences s CROSS JOIN note_rules r
  ),
  matched AS (
    SELECT *,
           regexp_extract(scan_text, concat('(?i)(', keyword_re, ')'), 1) AS matched_term,
           scan_text RLIKE concat('(?i)', neg_re, '[^.;]{0,40}?(', keyword_re, ')')
             OR scan_text RLIKE concat('(?i)(', keyword_re, ')[^.;]{0,40}?', post_neg_re) AS is_negated,
           scan_text RLIKE concat('(?i)', hist_re) AS is_historical
    FROM scanned
    WHERE scan_text RLIKE concat('(?i)', keyword_re)
  )
  SELECT clinic, clinic_patient_id, visit_id, topic,
         CASE WHEN is_negated THEN 'negated' WHEN is_historical THEN 'historical' ELSE 'current' END
           AS mention_status,
         sentence AS evidence, CAST(sentence_no AS INT) AS sentence_no, lower(matched_term) AS matched_term
  FROM matched
""").createOrReplaceTempView("mentions_raw")

val batchLit = batchId.replace("'", "''")
val srcLit   = sourcePath.replace("'", "''")
val mentions = spark.sql(s"""
  SELECT clinic, clinic_patient_id, visit_id, topic, mention_status, evidence,
         sentence_no, matched_term, '$ruleVersion' AS rule_version,
         '$batchLit' AS batch_id, '$srcLit' AS source_path
  FROM mentions_raw
""")

val exceptions = spark.sql(s"""
  SELECT '$batchLit' AS batch_id, '$srcLit' AS source_path, reason_code,
         clinic, clinic_patient_id, visit_id
  FROM notes_checked WHERE reason_code IS NOT NULL
""")

// Overwrite keeps a rerun of the same batch idempotent.
mentions.write.mode("overwrite").parquet(outputUri)
exceptions.write.mode("overwrite").parquet(exceptUri)

// ---------------------------------------------------------------------------
// 5. Read back and report counts.
// ---------------------------------------------------------------------------
spark.read.parquet(outputUri).createOrReplaceTempView("note_mentions")
spark.read.parquet(exceptUri).createOrReplaceTempView("note_mention_exceptions")

val notesRead     = spark.table("notes_checked").count()
val notesAccepted = spark.sql("SELECT count(*) FROM notes_checked WHERE reason_code IS NULL").first().getLong(0)
val exceptCount   = spark.table("note_mention_exceptions").count()
val mentionCount  = spark.table("note_mentions").count()
val notesWithHit  = spark.sql(
  "SELECT count(*) FROM (SELECT DISTINCT clinic, clinic_patient_id, visit_id FROM note_mentions)").first().getLong(0)

println(s"Notes read: $notesRead")
println(s"Notes accepted for extraction: $notesAccepted")
println(s"Notes excepted: $exceptCount")
println(s"Mention rows: $mentionCount")
println(s"Notes with at least one mention: $notesWithHit")

spark.sql("""
  SELECT topic, mention_status, count(*) AS mention_rows, count(DISTINCT visit_id) AS notes
  FROM note_mentions GROUP BY topic, mention_status ORDER BY topic, mention_status
""").show(false)
spark.sql("SELECT reason_code, count(*) AS notes FROM note_mention_exceptions GROUP BY reason_code").show(false)

// Every mention must point back to an accepted original note.
val orphans = spark.sql("""
  SELECT count(*) FROM note_mentions m LEFT ANTI JOIN
    (SELECT * FROM notes_checked WHERE reason_code IS NULL) n
  ON m.clinic = n.clinic AND m.clinic_patient_id = n.clinic_patient_id AND m.visit_id = n.visit_id
""").first().getLong(0)
println(s"Mentions without an accepted note: $orphans")
require(orphans == 0L, "note_mentions contains rows that do not link to an accepted note")
require(notesAccepted + exceptCount == notesRead, "Notes do not reconcile to accepted + excepted")
val badStatus = spark.sql(
  "SELECT count(*) FROM note_mentions WHERE mention_status NOT IN ('current','historical','negated')").first().getLong(0)
require(badStatus == 0L, "Unexpected mention_status value")

sys.env.get("NOTE_MENTIONS_EXPECT_NOTES").foreach { n =>
  require(notesRead == n.toLong, s"Expected $n notes, read $notesRead") }
sys.env.get("NOTE_MENTIONS_EXPECT_EXCEPTIONS").foreach { n =>
  require(exceptCount == n.toLong, s"Expected $n excepted notes, got $exceptCount") }

// Sample output, including the demo visit when it is present.
spark.sql("""
  SELECT visit_id, topic, mention_status, matched_term, evidence FROM note_mentions
  ORDER BY visit_id, sentence_no, topic LIMIT 20
""").show(false)
spark.sql("SELECT * FROM note_mentions WHERE visit_id = 'EV0033960'").show(false)

println("ISIT312_NOTE_MENTIONS_PASS")
