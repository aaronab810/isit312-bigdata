# First course VM run

- Batch `smoke-20261121T140127-2970` ran in the ISIT312 Big Data VM with Hadoop 2.7.3, Spark 3.0.3, Scala 2.12.10 and `local[2]`.
- Run `bash smoke/run_first_visit.sh /path/to/tailsignal_vet_dataset.zip` after the NameNode and DataNode are running and HDFS safe mode clears.
- The job wrote `visits`, `clinical_notes` and `lab_results` Parquet under `/isit312/vet/accepted/smoke-20261121T140127-2970/` and printed `ISIT312_SMOKE_PASS`.
- All eight check counts were zero. The job asserted 61,527 visits, 61,527 notes and 99,687 lab rows. The monthly counts sum to 61,527. The SITE-08 July 2024 procedure count was 20.
- Alpha `V533959` links to clinical visit `EV0033960` at SITE-08 on 14 July 2024. The drill-down shows one visit, one original note and seven lab rows.

The baseline provides each teammate with runnable inputs for their component. The final acceptance run will use the team’s ingestion, normalization, integration and Hive jobs.
