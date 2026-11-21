# ISIT312 project instructions

## Before changing files

- Read [README.md](README.md) and the assigned work package in the [team tracker](https://docs.google.com/document/d/1-GTuj_-7AlCtucJzKBMentBkKPHAQFW1Ps9vZycVRSc). Check the current branch and working tree. Scope changes to the assigned component and preserve teammates’ work.
- Read the dataset’s `DATA_GUIDE.md` and `manifest.json` before inferring fields, row counts or joins. Ask Aaron to coordinate changes to shared schemas, keys, paths and handoffs.
- Keep full archives, extracted records, generated outputs, VM images and credentials out of Git. Use synthetic fixtures. Keep clinic-provided records out of AI prompts, repos, screenshots and third-party tools unless the clinic explicitly approves that use.

## Course VM workflow

- Each teammate builds, runs and tests their assigned component in their own ISIT312 Big Data VM in Oracle VirtualBox. Follow the course labs for starting services and running jobs.
- Use HDFS for raw inputs and Parquet outputs. Use Scala `spark-shell` and `spark.sql(...)` for transformations. Deliver reusable Scala `.sc` jobs and Hive `.sql` files. Use Beeline for Hive reporting. Keep PySpark out of the submitted pipeline.
- Keep paths, batch IDs and Hive database names configurable. Record the versions and execution modes used for runs and benchmarks.
- Complete the batch path before incremental replay. Use Java MapReduce `ToolRunner` for the agreed comparison. Add Kafka only when a documented reporting requirement calls for it.

## Integration rules

- Preserve original source IDs and distinguish source system, site, source visit ID and clinical visit ID. Use the supplied TailSignal crosswalk for the synthetic demo. Retain and count unmatched rows.
- Link clinical notes and labs on `clinic`, `clinic_patient_id` and `visit_id`. Match patients on `clinic` and `clinic_patient_id`. Convert the clinical `day` field using the dataset’s `2022-01-01` base date.
- Keep the visits table at one row per clinical visit. Keep the original note text and lab rows accessible separately. Aggregate child rows before joining them into a visit-level report. Keep evaluation labels out of matching and report inputs.
- Report duplicates, missing keys, invalid links and row-count changes. Reconcile monthly report counts to the accepted visits. For the demo visit `EV0033960`, expect one visit, one original note and seven lab rows.

## Handoff and evidence

- Work on a component branch. Document inputs, outputs, schema, rerun behavior and exact commands. Include a small synthetic fixture or reproducible example that covers the normal path and a relevant error case.
- Capture the exact commands, inputs and actual outputs from the course VM. Run integration checks, benchmarks and the live demo there as well. Report clinic feedback only after receiving it.
- Keep each PR focused. Include the result counts and commands the teammate receiving your output can use to rerun it before merge.
