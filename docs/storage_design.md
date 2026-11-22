# TailSignal storage and collection design

```text
TailSignal extracted source files and source manifest
                |
                | local bytes and SHA-256 checked against selected batch manifest
                v
/isit312/vet/staging/<batch_id>/
                |
                | verify each file and the complete file set
                | publish the completed directory with _READY
                v
/isit312/vet/raw/<batch_id>/
                |
                +-- data/pims/ for Alina's source parsers
                +-- data/ehr/ and derived/visit_crosswalk.csv for Hani
                |
                v
Spark integration writes /isit312/vet/accepted/<batch_id>/<table>/
```

Raw is the verified original input, not the cleaned Parquet output. The batch manifest records selected paths, byte sizes and SHA-256 values; its comments record source-provided row counts. Evaluation labels are excluded from this pipeline batch. The loader assumes one writer per batch ID and rejects a changed manifest or source under an existing ID. Keep incomplete staging out of downstream reads. Each team member loads the batch in their own VM unless shared HDFS is explicitly configured.

The dataset is fictional; clinic-specific collection, retention, access and recovery needs remain subject to the clinic study and feedback. Hashes establish byte integrity against the supplied manifest, not clinical correctness.
