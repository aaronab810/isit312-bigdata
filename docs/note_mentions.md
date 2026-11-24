# Note mention extraction and evaluation (Prajwal)

This component reads the original free-text clinical notes and records, for each symptom mention, whether the note says it is happening now, happened before or was ruled out. The output is the shared `note_mentions` table. The original note text stays unchanged in `clinical_notes`.

| File | Purpose |
| --- | --- |
| [`spark/extract_note_mentions.sc`](../spark/extract_note_mentions.sc) | Pipeline job. Scala `spark-shell` with the rules in Spark SQL. It does not read labels. |
| [`spark/evaluate_note_mentions.sc`](../spark/evaluate_note_mentions.sc) | Evaluation job. It reads the labels, splits off the held-out test set and reports precision, recall and example errors. |
| [`spark/run_note_mentions.sh`](../spark/run_note_mentions.sh) | VM launcher for fixture mode and batch mode. It fails if a pass marker is missing. |
| [`fixtures/note_mentions/notes.csv`](../fixtures/note_mentions/notes.csv) | 26 synthetic notes. These include traps, three cases the rules get wrong on purpose and two notes that should be rejected. |
| [`fixtures/note_mentions/gold_labels.csv`](../fixtures/note_mentions/gold_labels.csv) | Hand labels for the fixture in long form (`visit_id,topic,mention_status`). |

## Categories

The work is defined at sentence level and evaluated at note level.

| Topic | Words that count as the topic |
| --- | --- |
| `seizure` | seizure (including misspellings such as *siezure*), `sz`, fit, fits, fitting, convulsing, tonic-clonic, grand mal, ictal, epilepsy/epileptic. *Fit and well*, *fit & well* and *fitted* (as in *fitted e-collar*) are removed before matching. |
| `gi` | vomit, vomitting, `V+`, `D+`, `V/D`, diarrhoea/diarrhea, melena, loose stool, GI upset, gastroenteritis, emesis |
| `neuro` | ataxia/ataxic, tremor, twitching, wobbly, head tilt, nystagmus, paresis, proprioceptive deficit |

| `mention_status` | Definition | Cues |
| --- | --- | --- |
| `negated` | The note says the symptom is absent or ruled out. | A cue up to 40 characters before the keyword in the same sentence: *no, nil, denies, not, without, negative for, absence of, ruled out, r/o*, or *O* (owner) followed by a word starting with *d*, which catches misspelt *O denies*. A cue may also follow the keyword: *ruled out, negative, absent*. |
| `historical` | The symptom belongs to the patient's past rather than this visit. | *hx, history, known epileptic, previous, prior, resolved, last month, last year, in the past* |
| `current` | The symptom is mentioned for this visit. | Any other keyword match. |

If a sentence matches more than one status for the same topic, the job keeps the first match in this order: negated, historical, current. For example, *No seizure activity since last visit* is negated.

**Where keywords alone fail.** *Mild ataxia, no other neuro deficits* contains "no", but the cue follows *ataxia*, so the neuro mention stays current. *O reports fit and well* and *Fitted e-collar* contain "fit", but neither describes a seizure. In the fixture, FX0004 and FX0005 produce no seizure mention. A keyword-only seizure detector would flag both. The evaluation job prints the keyword-only precision next to the rule results.

## Output: `accepted/<batch_id>/note_mentions`

The table has one row per sentence and topic match. A note can have zero or more rows. Join it to `clinical_notes` or `visits` on `(clinic, clinic_patient_id, visit_id)`. For a visit-level report, aggregate it to one row per visit before joining, for example `max(CASE WHEN topic='seizure' AND mention_status='current' THEN 1 ELSE 0 END)`.

| Column | Type | Meaning |
| --- | --- | --- |
| `clinic` | STRING | Clinic key from the note |
| `clinic_patient_id` | STRING | Patient key from the note |
| `visit_id` | STRING | Clinical visit ID |
| `topic` | STRING | `seizure`, `gi` or `neuro` |
| `mention_status` | STRING | `current`, `historical` or `negated` |
| `evidence` | STRING | The sentence that triggered the row, unchanged |
| `sentence_no` | INT | 1-based sentence position in the note |
| `matched_term` | STRING | The keyword that matched, in lower case |
| `rule_version` | STRING | `note-rules-v2` |
| `batch_id` | STRING | Batch that produced the row |
| `source_path` | STRING | Archive-relative origin, `data/ehr/ehr_notes.parquet` |

The first six columns follow the [data contract](data_contract.md). The other columns add provenance only.

Notes that the job cannot read go to `exceptions/<batch_id>/note_mentions` with `batch_id`, `source_path`, `reason_code` (`MISSING_KEY`, `BLANK_TEXT`, `DUPLICATE_NOTE_KEY`) and the available keys. The job checks that accepted notes plus excepted notes equal the notes read, and that every mention links to an accepted note.

Hive DDL for Sarib, with the location set to the batch:

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS note_mentions (
  clinic STRING, clinic_patient_id STRING, visit_id STRING, topic STRING,
  mention_status STRING, evidence STRING, sentence_no INT, matched_term STRING,
  rule_version STRING, batch_id STRING, source_path STRING)
STORED AS PARQUET
LOCATION '/isit312/vet/accepted/<batch_id>/note_mentions';
```

## Held-out evaluation

- **Labels.** TailSignal's `evaluation/ehr/note_labels.parquet` has one row per note. It contains `visit_id` and the boolean flags `label_seizure`, `label_seizure_history`, `label_seizure_negated`, `label_gi`, `label_neuro` and `label_fit_and_well`. These flags support five categories: seizure current, historical and negated, GI current and neuro current. The supplied labels do not cover negated or historical GI mentions, so the [synthetic fixture](../fixtures/note_mentions/gold_labels.csv) labels those cases by hand.
- **Split.** A note is in the test set when `pmod(crc32(visit_id), 100) < 30`. The split depends only on the visit ID and was fixed before any tuning. Rules may be tuned on the 70% dev split only. Report results from the test split. The labelled test set is defined by that rule and by the labels file in the ZIP, so no label data is committed.
- **Scoring.** For each category, a note counts as predicted if it has at least one matching `note_mentions` row. Precision is TP / (TP + FP), recall is TP / (TP + FN), and F1 is also reported. The job prints one correct example per category and up to five missed and false examples.
- **Where labels came from during design.** The keyword lists were written from the public [TailSignal generator](https://github.com/RonCom/tailsignal) phrase lists, not from the test notes. Results on TailSignal will therefore be better than on real clinic notes, which use more varied wording. See the limitations below.
- **Tuning history.** There was one tuning round, using the dev split only.
  - `v1` on dev (43,295 notes): seizure current P 0.81 / R 0.98, seizure negated R 0.98 and GI current P 0.985. Nearly all errors came from TailSignal's random typos in *denies* ("O dnies seizures", "O deines vomiting or diarrhoea"). These lost the negation and became false current mentions.
  - `v2` adds the owner + *d*-word negation cue. On dev it scores P 1.00 in every category, with R between 0.92 and 0.99.
  - Keyword typos such as *Vmited* or *Atxic* were left alone. Fuzzy keyword matching would risk false hits on real clinic text.
  - The rules were then frozen and scored once on the test split.

## Commands (course VM)

Run fixture mode first. It needs only Spark.

```bash
cd ~/isit312-bigdata-project-start
bash spark/run_note_mentions.sh fixture
```

Expected fixture result: 26 notes read, 24 accepted and 2 excepted (FX0022 `MISSING_KEY`, FX0023 `BLANK_TEXT`). The evaluation prints `ISIT312_NOTE_EVAL_PASS` and finds three planted errors:

| Note | Text | Gold | Predicted | Why |
| --- | --- | --- | --- | --- |
| FX0019 | *No vomiting since Tuesday but D+ this AM.* | gi negated + gi current | gi negated | The job assigns one status per topic per sentence. Clauses joined by *but* are not split. |
| FX0020 | *Sezure x2 overnight per O.* | seizure current | none | This misspelling is not on the keyword list. |
| FX0021 | *Not eating, vomited x2 overnight.* | gi current | gi negated | The negation cue *not* applies to *eating*. The job treats it as negating *vomited* because the cue is within 40 characters. |

Expected fixture metrics. These came from running `bash spark/run_note_mentions.sh fixture` with Spark 3.0.3 and Scala 2.12.10 in `local[2]` on Java 11, outside the course VM. Confirm them in the VM log:

| Category | TP | FP | FN | Precision | Recall |
| --- | --- | --- | --- | --- | --- |
| seizure current | 5 | 0 | 1 | 1.00 | 0.83 |
| seizure historical | 3 | 0 | 0 | 1.00 | 1.00 |
| seizure negated | 4 | 0 | 0 | 1.00 | 1.00 |
| gi current | 2 | 0 | 2 | 1.00 | 0.50 |
| gi historical | 2 | 0 | 0 | 1.00 | 1.00 |
| gi negated | 3 | 1 | 0 | 0.75 | 1.00 |
| neuro current | 2 | 0 | 0 | 1.00 | 1.00 |

Full batch: first run the baseline, which prints the batch ID and `Accepted:` path. Then run:

```bash
bash smoke/run_first_visit.sh /path/to/tailsignal_vet_dataset.zip
bash spark/run_note_mentions.sh batch <batch_id_from_smoke> /path/to/tailsignal_vet_dataset.zip
```

Batch mode expects 61,527 notes read, 0 exceptions, 27,761 mention rows and 24,665 notes with at least one mention (see the expected results below). It writes metrics as CSV to `/isit312/vet/evaluation/<batch_id>/note_mentions_metrics`. Logs go to `~/isit312_logs/`. Rerunning the same batch overwrites `note_mentions` and gives the same rows.

Useful checks after a run:

```bash
spark-shell --master local[2]
scala> val b = "<batch_id>"
scala> spark.read.parquet(s"/isit312/vet/accepted/$b/note_mentions").createOrReplaceTempView("note_mentions")
scala> spark.sql("SELECT topic, mention_status, count(*) FROM note_mentions GROUP BY 1,2 ORDER BY 1,2").show()
scala> spark.sql("SELECT * FROM note_mentions WHERE visit_id = 'EV0033960'").show(false)
```

## Expected results

These numbers came from running `extract_note_mentions.sc` and `evaluate_note_mentions.sc` unchanged on the full TailSignal v1 notes and labels. The run used Spark 3.0.3 and Scala 2.12.10 in `local[2]` on Java 11, outside the course VM. It read the original `ehr_notes.parquet`, which the baseline copies unchanged into `clinical_notes`, from local disk instead of HDFS. Confirm them in the VM log and record the batch ID, Hadoop 2.7.3, Spark 3.0.3, Scala 2.12.10 and `local[2]`, as in the [baseline](baseline_evidence.md). If the VM numbers differ, the VM log is the evidence. Investigate the difference before submitting.

Held-out test split: 18,232 notes (dev: 43,295).

| Category | Support | TP | FP | FN | Precision | Recall | F1 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| seizure current | 150 | 146 | 0 | 4 | 1.0000 | 0.9733 | 0.9865 |
| seizure historical | 102 | 99 | 0 | 3 | 1.0000 | 0.9706 | 0.9851 |
| seizure negated | 2,773 | 2,748 | 0 | 25 | 1.0000 | 0.9910 | 0.9955 |
| gi current | 1,762 | 1,742 | 0 | 20 | 1.0000 | 0.9886 | 0.9943 |
| neuro current | 26 | 24 | 0 | 2 | 1.0000 | 0.9231 | 0.9600 |
| keyword-only seizure current (baseline) | 150 | 146 | 3,887 | 4 | 0.0362 | 0.9733 | — |

The keyword-only detector finds almost every seizure note but is wrong 96% of the time. Most of its hits are negations such as *O denies seizures*, history such as *hx sz*, and *fit and well* or *Fitted e-collar*.

Full-batch extraction counts:

| Topic | Status | Mention rows | Notes |
| --- | --- | --- | --- |
| gi | current | 5,632 | 5,632 |
| gi | historical | 1,664 | 1,664 |
| gi | negated | 9,863 | 9,863 |
| neuro | current | 78 | 78 |
| seizure | current | 518 | 515 |
| seizure | historical | 342 | 342 |
| seizure | negated | 9,664 | 9,664 |
| **total** | | **27,761** | **24,665 distinct notes** |

The demo visit `EV0033960` is a dental procedure (*COHAT/dental under GA. ASA 2. Smooth recovery. …*). It has no seizure, GI or neuro mention, so `note_mentions` returns no rows for it. That result is correct, and its original note is still available in `clinical_notes`.

Examples of test-split errors. All 54 errors on the test split are false negatives.

| Visit | Category | Error | Note text (excerpt) | Cause |
| --- | --- | --- | --- | --- |
| EV0005428 | gi current | missed | *Voimted x4 overnight.* | Typo in the keyword |
| EV0057589 | neuro current | missed | *Ataxc on hind limbs.* | Typo in the keyword |
| EV0056941 | seizure current | missed | *fittng while asleep, paddling, salivating.* | Typo in the keyword |
| EV0057047 | seizure historical | missed | *Known epieptic - on levetiracetam.* | A typo breaks both the keyword and the history cue |
| EV0003301 | seizure negated | missed | *No sezure activity since last visit.* | Typo in the keyword |

Fixture example where a keyword plus "no" gives the wrong answer: FX0021 *Not eating, vomited x2 overnight.* is gold GI current, but the job says negated.

## Limitations

- **Spelling.** The keyword lists include the misspellings the generator uses, such as *siezure* and *vomitting*. TailSignal also adds one random typo to about 15% of notes. Misspelt *O denies* is handled since v2. A typo inside a keyword is still missed, and this causes every test-split error.
- **The owner negation cue is narrow.** *O* followed by a *d*-word matches TailSignal's shorthand. In real notes it would also match a sentence like *O describes vomiting*, so it needs checking on clinic-approved samples.
- **Clause scope.** There is one status per topic per sentence. A sentence that negates one symptom and reports another under the same topic gets only the negated status (FX0019). A cue that negates a different word nearby can still flip the status (FX0021).
- **Diagnosis-only mentions.** *Presented for seizure.* or *Recheck: seizure.* would be extracted as a current seizure. TailSignal v1 does not contain this wording, so it caused no false positives here. Real notes may use it.
- **Labels coverage.** Negated and historical GI or neuro mentions have no supplied labels. Their scores come only from the small hand-labelled fixture.
- **Fitted to synthetic text.** The rules came from the generator's phrase lists. Real clinic notes would need new keywords, abbreviations and evaluation on clinic-approved labelled samples. Those notes must not leave the clinic's environment.
- **Rules, not a model.** Regex rules are transparent and run in Spark SQL without extra libraries. They do not generalise to unseen wording.
