# Veterinary clinic workflow

The clinic uses its practice-management system for visits, notes and billing. Lab results arrive through a separate feed or export. The project brings those records together so the clinic can count visits by site, month and type, then open the original note and linked lab results for a specific encounter.

| ISIT312 study area | Current workflow and project response |
| --- | --- |
| Data volume | Visits, notes, invoices and lab results have different counting units. Track visits and notes per month, billing rows per visit and analyte rows per lab report. On the TailSignal demo, the reference volumes are 53,167 practice visits, 61,527 clinical visits and notes, and 99,687 lab result rows. |
| Collection | Clinic staff create visits, enter notes and record charges in the practice-management system. Lab results arrive as a separate input. The pipeline retains the source files by batch and keeps the original note text. |
| Storage | Daily records stay in the practice-management system and laboratory result source. The proposed data lake keeps input files in HDFS, writes typed Parquet tables for reporting and registers the completed batch in Hive. |
| Processing | Scala and Spark SQL normalize the three demo export formats, standardize dates and keep one row per accepted clinical visit. Hive reports visit counts at that grain. Notes, charges and lab results keep their own grain for drill-down. |
| Integration | In the demo, the supplied crosswalk links practice visits to clinical visits. The site and date must match the source. Notes and lab results link using the clinic, clinic patient and clinical visit IDs. The clinic’s incoming lab feed determines which order or encounter IDs will be available in the real adapter. |
| Data issues | The pipeline handles missing keys, duplicate imports, broken links and unmatched records explicitly. A visit with seven lab rows still counts once in the monthly report. Replay tests cover late and amended records. |

The clinic walkthrough will use a monthly report and one encounter drill-down. Ask which measures and refresh interval support daily operations, then record the feedback and resulting changes. Add clinic-specific volumes, product names, retention and feed details when the clinic provides them. TailSignal figures describe the demo input.
