# Incremental processing and replay

`spark/replay_snapshot.sc` applies a batch of new or corrected visit records to a saved copy of the `visits` table. It writes a new Parquet snapshot for that batch and only reads the previous one, so every earlier snapshot stays available. It follows the replay rules in the [data contract](data_contract.md).

## Inputs and outputs

| | Path | Contents |
| --- | --- | --- |
| Prior snapshot | `REPLAY_PRIOR`, e.g. `/isit312/vet/accepted/<baseline batch>/visits` | A completed `visits` table with a `_SUCCESS` marker. It can be the baseline or an earlier replay snapshot. |
| Change events | `<root>/raw/<batch_id>/replay/visit_events.csv` (or `REPLAY_INPUT`) | CSV with the header `event_id,event_version,received_at,clinic,clinic_patient_id,visit_id,visit_date,visit_type,source_system,source_visit_id`. Each row is the full new version of one visit. |
| New snapshot | `<root>/accepted/<batch_id>/visits` | One row per `(clinic, clinic_patient_id, visit_id)`. It keeps the prior snapshot's columns and adds `event_id`, `event_version`, `received_at`, `batch_id` and `source_path`. |
| Audit | `<root>/accepted/<batch_id>/replay_audit` | One row per input row as received, with `prior_version`, `result_version`, `outcome` and `reason_code`. |
| Set-aside rows | `<root>/exceptions/<batch_id>/replay` | Rejected, conflicting and held rows with `batch_id`, `source_path`, `reason_code` and the keys. |

`<root>` is `ISIT312_HDFS_ROOT`, which defaults to `/isit312/vet`. Baseline rows have no event columns, so they count as version 0 and keep the baseline batch ID and path. In each snapshot, `batch_id` and `source_path` show which batch and file last changed that row.

## Rules and outcomes

The key is `(clinic, clinic_patient_id, visit_id)`. For each key, the job takes the highest `event_version` in the batch and compares it with the version in the prior snapshot.

| Outcome | When | Snapshot | Set aside |
| --- | --- | --- | --- |
| `inserted` | The key is new. | New row added | No |
| `updated` | The version is higher than the saved one. | Row replaced | No |
| `duplicate_in_batch` | An exact copy of the applied row arrived in the same batch. | No change | No |
| `superseded_in_batch` | A higher version of the same key in the same batch was applied. | No change | No |
| `repeat_no_change` | The same version and values are already saved. | No change | No |
| `stale_late_arrival` | The version is lower than the saved one. | No change | No |
| `conflict_same_version` | Two rows in the batch have the same key and version but different values. | Prior row stays | Yes |
| `held_by_conflict` | A valid version was not applied because a higher version of the key is in conflict. | Prior row stays | Yes |
| `conflict_with_snapshot` | The version equals the saved one but the values differ. | Prior row stays | Yes |
| `visit_id_on_other_patient` | A new key reuses a visit ID that another patient already has at that clinic. Applying it would count the visit twice. | No change | Yes |
| `rejected` | The row is malformed. `reason_code` is one of `malformed_row`, `missing_key`, `missing_event_id`, `invalid_event_version`, `invalid_received_at`, `invalid_visit_date`, `missing_visit_type` or `partial_source_pair`. | No change | Yes |

`event_version` must be a whole number of at least 1, because version 0 is reserved for baseline rows. `visit_date` must be `yyyy-MM-dd`. `source_system` and `source_visit_id` must both be present or both be empty, as for EHR-only visits. `received_at` only breaks ties between exact copies.

### Reruns and changed inputs

- If you rerun a batch ID that already has a complete snapshot, the job recomputes the result and compares it with the saved files. If they are identical, it prints `status=unchanged_rerun` and writes nothing, so no rows are appended.
- If the input file or the prior snapshot has changed, the job stops with `Batch <id> already has different saved results`. Use a new batch ID for changed content.
- The snapshot is written last, so a complete batch is one whose `visits` folder has `_SUCCESS`. If a run is interrupted, rerunning the same batch ID rewrites it.

## Test fixtures

`fixtures/replay/` has five batches. They run in order, and each one starts from the snapshot the previous batch wrote. Every change is at SITE-03 in March 2025, so the demo result never moves: `EV0033960` and the 20 SITE-08 procedure visits in July 2024 are untouched. `base_visits.csv` holds five baseline rows, so the test can run without the full baseline.

| Batch | Input | Expected result | Monthly report change |
| --- | --- | --- | --- |
| `01_new_visit` | New recheck visit `EV9000001` for patient `SITE-03-00128` on 15 Mar 2025, delivered twice | Inserted once, with one `duplicate_in_batch`. Visits go from 61,527 to 61,528. | SITE-03 Mar 2025 recheck +1 |
| `02_correction` | `EV0041789` corrected from wellness to sick, version 2 | Updated to version 2 | SITE-03 Mar 2025 wellness −1, sick +1 |
| `02_correction` again | The same batch ID rerun | `unchanged_rerun`, saved files untouched | None |
| `03_repeat` | The same file as 02 under a new batch ID | `repeat_no_change`. The snapshot is identical to 02. | None |
| `04_late_older` | `EV0041789` version 1 (recheck) arriving after version 2 | `stale_late_arrival`. Version 2 (sick) stays. | None |
| `05_conflict` | Two different version 3 rows for `EV0041789`, a valid correction of `EV0058369` from sick to recheck, a row with no `visit_id`, and `EV0041764` claimed by another patient | Two `conflict_same_version` rows, one `updated`, one `rejected` (`missing_key`) and one `visit_id_on_other_patient`, so 4 set-aside rows. `EV0041789` stays at version 2. | SITE-03 Mar 2025 sick −1, recheck +1 |

The test also re-sends batch 04's file under batch 02's ID. That run must be refused, and the batch 02 snapshot must stay unchanged. The expected values are in `expected_outcomes.csv`, `expected_report_changes.csv` and `expected_visits.csv`.

On the full baseline, the SITE-03 March 2025 counts are:

| Snapshot | procedure | recheck | sick | wellness |
| --- | ---: | ---: | ---: | ---: |
| baseline | 9 | 24 | 53 | 98 |
| after 01 | 9 | 25 | 53 | 98 |
| after 02, 03, 04 | 9 | 25 | 54 | 97 |
| after 05 | 9 | 26 | 53 | 97 |

## Commands for the course VM

Start HDFS as in the course lab, then run these from the repository folder.

```bash
# Fixture test (five-row base). Prints PASS lines and ISIT312_REPLAY_PASS.
bash tests/run_replay_tests.sh

# The same test on the full baseline. Use the Accepted path that smoke/run_first_visit.sh printed.
bash tests/run_replay_tests.sh hdfs://localhost:8020/isit312/vet/accepted/<smoke batch>

# The saved snapshots, audits and set-aside rows
hdfs dfs -ls /isit312/vet/accepted | grep replay-test
hdfs dfs -ls -R /isit312/vet/exceptions | grep replay
```

Both test commands save a log as `tests/<run id>.log`. To apply one batch, for example in the final live run:

```bash
# Usage: bash spark/run_replay.sh <batch id> <prior visits snapshot> [local events CSV]
bash spark/run_replay.sh replay-live-01 \
  hdfs://localhost:8020/isit312/vet/accepted/<smoke batch>/visits \
  fixtures/replay/02_correction.csv
```

With a local CSV, the script copies it to `/isit312/vet/raw/<batch id>/replay/visit_events.csv` and then runs the job. It prints the audit, the changed visits before and after, and the monthly report rows that change. It ends with `ISIT312_REPLAY_DONE`. The job can also run directly in `spark-shell`:

```bash
export BATCH_ID=replay-live-01 REPLAY_PRIOR=/isit312/vet/accepted/<smoke batch>/visits
printf ':paste %s\n:quit\n' "$PWD/spark/replay_snapshot.sc" | spark-shell --master 'local[2]'
```

To report on a replay snapshot, point the Hive `visits` table or the Spark checks at `/isit312/vet/accepted/<batch id>/visits`. Keep `clinical_notes` and `lab_results` from the baseline. A correction-only snapshot passes all eight integration checks. A snapshot with a new visit and no note fails `visits_without_one_original_note`, as it should.

## Scheduled batches and faster updates

The clinic's reports are monthly visit counts by site and visit type, plus lookups of one visit with its note and labs. A scheduled batch fits that. For example, a nightly export landed in HDFS and replayed gives next-day figures. Corrections and late arrivals are absorbed by the next batch. A re-sent or repeated file changes nothing, so staff can resend exports without cleaning up duplicates. Conflicts are set aside for someone to check, and earlier snapshots stay, so any past month's report can be rebuilt as it was.

Faster updates would matter only if someone needs a change visible within minutes. Examples are a live front-desk view of today's visits, an alert when a critical lab result arrives, or same-day billing reconciliation. Those needs would justify Kafka with a streaming job applying the same rules. They are operational tasks the practice system already handles, so add streaming only if the clinic's feedback names one as a reporting requirement.

Questions for the clinic walkthrough: How often do visit records get corrected after the day of the visit? How late can a lab result or correction arrive? How fresh does the monthly report need to be?

## Limitations

- Only `visits` is replayed. Notes and lab results for a new visit need the same treatment in their own tables before the integration checks pass on that snapshot.
- There are no deletes. Moving a visit to another patient changes its key, so the job sets it aside as `visit_id_on_other_patient` instead of counting the visit twice. Fixing it needs a delete or a key change agreed with the team.
- Each row must carry the whole visit. Columns the events do not carry, such as provenance columns added by integration, keep their prior value on an update and are empty for a new visit.
- The audit keeps input values exactly as received. The snapshot stores trimmed, typed values.
