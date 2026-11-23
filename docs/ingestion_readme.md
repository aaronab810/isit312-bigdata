# HDFS batch ingestion

Run with Bash on the course VM from the repository root:

```bash
bash ingest/land_batch.sh --source /path/to/extracted/dataset --hdfs-root /isit312/vet --batch-id tailsignal_2026_11_22 --manifest manifests/tailsignal_2026_11_22.tsv
```

Each listed file is checked locally against the selected manifest and copied under `staging/<batch-id>`, preserving relative paths. Completed staged files are reused after checksum verification; a partial `.uploading` or `.uploading._COPYING` file is removed on retry. All listed files are rechecked before the directory is moved to `raw/<batch-id>`. The manifest and `_READY` marker accompany the raw batch. Replays check the stored manifest and file hashes. A changed local file or manifest is rejected. Use a new batch ID for changed inputs.

The manifest records supplied reference row counts, not independently computed counts. Evaluation labels are not included. Downstream cleaned Parquet belongs separately under `accepted/<batch-id>/<table>`.

The loader checks that Hadoop points to HDFS before copying files. If `hdfs` is outside your PATH, set `HADOOP_HOME` to the lab installation directory. Downstream jobs should use `raw/<batch-id>/batch_manifest.tsv` for the landed file list and `_READY` for completion.

The loader assumes a single writer for each batch ID and a dedicated staging directory. It checks the staged file list and stops if unexpected files are present. HDFS service availability, permissions, and actual interruption/replay behavior must be tested in the course VM. `tests/test_ingest_mock.sh` is a simulated local test, not HDFS evidence.
