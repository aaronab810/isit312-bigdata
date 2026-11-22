# Veterinary clinic workflow

The clinic uses its practice-management system for visits, notes and billing. Lab results arrive through a separate feed or export. The project brings those records together so the clinic can count visits by site, month and type, then open the original note and linked lab results for a specific encounter.

| ISIT312 study area | Current workflow and project response |
| --- | --- |
| Data volume | Visits, notes, invoices and lab results have different counting units. Track visits and notes per month, billing rows per visit and analyte rows per lab report. On the TailSignal demo, the reference volumes are 53,167 practice visits, 61,527 clinical visits and notes, and 99,687 lab result rows. |
| Collection | Clinic staff create visits, enter notes and record charges in the practice-management system. Lab results arrive as a separate input. The pipeline retains the source files by batch and keeps the original note text. |
| Storage | Daily records stay in the practice-management system and laboratory result source. The proposed data lake keeps input files in HDFS, writes typed Parquet tables for reporting and registers the completed batch in Hive. |
| Processing | Scala and Spark SQL put the three synthetic exports into a common format and standardize the dates. Hive counts one row per accepted clinical visit. Notes, charges and lab results stay in separate tables so they can be opened for a visit. |
| Integration | In the demo, the supplied crosswalk links practice visits to clinical visits. The site and date must match the source. Notes and lab results link using the clinic, clinic patient and clinical visit IDs. The clinic’s incoming lab feed determines which order or encounter IDs will be available in the real adapter. |
| Data issues | The pipeline handles missing keys, duplicate imports, broken links and unmatched records explicitly. A visit with seven lab rows still counts once in the monthly report. Replay tests cover late and amended records. |

In the final walkthrough, show the monthly report and open one visit with its original note and lab results. Ask which report fields the clinic would use and how often the report should update. Record the clinic’s response and any design changes. Add the clinic’s volumes, system names, how long it keeps records and how lab results arrive when the clinic provides those details. The TailSignal figures describe the synthetic input.
