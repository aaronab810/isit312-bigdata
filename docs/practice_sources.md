# Practice exports: source inventory, dictionary and parser evaluation

Component: `spark/normalize_practice.sc` (owner: Alina). Input: the shared TailSignal ZIP (`tailsignal_vet_dataset.zip`). Runs in the ISIT312 Big Data VM: Hadoop 2.7.3, Spark 3.0.3, Scala 2.12.10, `spark-shell --master local[*]`.

## 1. Source inventory

| Source system | File | Format | Size (bytes) | Rows | Sites (per `locations.csv`) |
| --- | --- | --- | --- | --- | --- |
| `vet_alpha` | `data/pims/vet_alpha_visits.csv` | CSV, comma, header row, quoted fields | 3,710,038 | 18,881 | SITE-04, SITE-07, SITE-08 |
| `vet_beta` | `data/pims/vet_beta_encounters.jsonl` | JSON Lines, one nested object per line | 8,483,031 | 17,571 | SITE-01, SITE-05, SITE-09 |
| `vet_gamma` | `data/pims/vet_gamma_visits.txt` | Pipe-delimited text, header row | 1,485,299 | 16,715 | SITE-02, SITE-03, SITE-06 |
| site lookup | `data/pims/locations.csv` | CSV, header row | 256 | 9 | all |

Row counts exclude header rows (`wc -l` minus 1 for the CSV and pipe files). Together the three exports hold 53,167 encounters.

### Fields per source

**Alpha (15 columns):** `clinic_id`, `client_id`, `patient_id`, `patient_name`, `species`, `breed`, `sex`, `dob`, `visit_id`, `visit_date`, `dx_code`, `dx_desc`, `line_items`, `invoice_total`, `weight_kg`.
`line_items` packs every charge into one field as `item:amount;item:amount`, e.g. `Sick visit exam:108.59;Diagnostics/treatment:141.70`. Some text fields are quoted because they contain commas, e.g. `"Mass, cutaneous/subcutaneous"`.

**Beta (nested JSON):** `encounter_id`, `site`, `date`, `client{id}`, `patient{id, name, species, breed, sex, age_years_at_reg, registered, weight_kg}`, `diagnoses[{code, text}]`, `charges[{item, amount}]`. Beta has no invoice total field. `diagnoses.code` is numeric (e.g. `1104`).

**Gamma (11 columns):** `CLINIC`, `ACCT`, `PET_NAME`, `SPECIES`, `BREED`, `BIRTH`, `SEX`, `VISIT_NO`, `VISIT_DT`, `REASON`, `TOTAL`.
Gamma has no patient ID and no charge breakdown. `VISIT_NO` has no letter prefix (e.g. `500003`), and owner accounts start with `A` instead of `C`. `REASON` is free text (e.g. `V/D`, `perio dz`).

**locations.csv:** `location_id`, `channel`, `source_system`, `open_day`.

All three exports use `yyyy-MM-dd` dates.

## 2. Source dictionary

How each normalized column is filled from each source.

| Normalized column | Alpha | Beta | Gamma |
| --- | --- | --- | --- |
| `source_system` | `'vet_alpha'` | `'vet_beta'` | `'vet_gamma'` |
| `source_visit_id` | `visit_id` | `encounter_id` | `VISIT_NO` |
| `clinic` | `clinic_id` | `site` | `CLINIC` |
| `source_date` | `visit_date` | `date` | `VISIT_DT` |
| `source_owner_id` | `client_id` | `client.id` | `ACCT` |
| `source_patient_id` | `patient_id` | `patient.id` | NULL (not in source) |
| `invoice_total` | `invoice_total` | sum of `charges[].amount` | `TOTAL` |
| `source_path` | `data/pims/vet_alpha_visits.csv` | `data/pims/vet_beta_encounters.jsonl` | `data/pims/vet_gamma_visits.txt` |
| charge lines | split `line_items` on `;`, then on `:` | `posexplode(charges)` | none: total stays on the visit |
| diagnoses | `dx_code`, `dx_desc` (one per visit) | `posexplode(diagnoses)` | `REASON` as text, no code |

All identifiers are kept as strings. Text values are trimmed. Dates are parsed with `to_date(..., 'yyyy-MM-dd')`, and amounts are cast to `DECIMAL(12,2)`.

## 3. Output schema

Written as Parquet with `mode("overwrite")` under `ISIT312_HDFS_ROOT` (default `/isit312/vet`).

### `normalized/<batch>/practice_visits`: one row per accepted source encounter

| Column | Type | Notes |
| --- | --- | --- |
| `source_system` | STRING | Part of the key |
| `source_visit_id` | STRING | Part of the key |
| `clinic` | STRING | Site, e.g. SITE-08 |
| `source_date` | DATE | |
| `source_owner_id` | STRING | |
| `source_patient_id` | STRING | NULL for every Gamma visit |
| `invoice_total` | DECIMAL(12,2) | |
| `source_path` | STRING | Archive-relative input file |
| `batch_id` | STRING | |

Key: `(source_system, source_visit_id)`, unique in the output.

### `normalized/<batch>/practice_charge_lines`: one row per charge line (Alpha and Beta)

| Column | Type | Notes |
| --- | --- | --- |
| `source_system` | STRING | |
| `source_visit_id` | STRING | Joins to `practice_visits` |
| `line_number` | INT | 1, 2, … in source order |
| `item` | STRING | |
| `amount` | DECIMAL(12,2) | |

### `normalized/<batch>/practice_diagnoses`: one row per source diagnosis

| Column | Type | Notes |
| --- | --- | --- |
| `source_system` | STRING | |
| `source_visit_id` | STRING | Joins to `practice_visits` |
| `diagnosis_number` | INT | 1, 2, … in source order |
| `dx_code` | STRING | NULL for Gamma |
| `dx_text` | STRING | |

### `exceptions/<batch>/normalize_practice`: one row per rejected source row

`batch_id`, `component`, `source_system`, `source_path`, `source_visit_id`, `clinic`, `reason_code`, `raw_record` (all STRING). `raw_record` holds the original text of a malformed row.

| Reason code | Meaning |
| --- | --- |
| `MALFORMED_RECORD` | Wrong number of fields, or invalid JSON |
| `MISSING_SOURCE_VISIT_ID` | Visit ID blank |
| `MISSING_CLINIC` | Site blank |
| `INVALID_DATE` | Date blank or not a valid `yyyy-MM-dd` date |
| `INVALID_CHARGE_LINE` | A charge has no item, no amount or a non-numeric amount |
| `INVALID_AMOUNT` | Invoice total missing or not numeric |
| `DUPLICATE_KEY` | The same `(source_system, source_visit_id)` appears more than once; all copies are rejected |

Charge lines and diagnoses are written only for accepted visits.

## 4. Parser evaluation and reconciliation

### Full batch (`alina-dev-01`)

| Source | Source rows | Accepted | Rejected | Charge lines | Invoice total | Charge line total | Diagnoses |
| --- | --- | --- | --- | --- | --- | --- | --- |
| vet_alpha | 18,881 | 18,881 | 0 | 57,638 | 5,622,219.13 | 5,622,219.13 | 18,881 |
| vet_beta | 17,571 | 17,571 | 0 | 53,702 | 5,194,007.11 | 5,194,007.11 | 17,571 |
| vet_gamma | 16,715 | 16,715 | 0 | n/a | 4,744,347.03 | n/a | 16,715 (text only) |
| **Total** | **53,167** | **53,167** | **0** | **111,340** | | | **53,167** |

- Every source row is accounted for: accepted plus rejected equals source rows for each input.
- For each of the 18,881 Alpha visits, the charge lines add up exactly to `invoice_total` (0 mismatches), so there are no discrepancies to explain. Beta has no source total, so its total is the sum of its charges by definition.
- No rows were malformed, and no row had a missing ID, a bad date or a bad amount. Quoted Alpha fields containing commas parsed correctly.
- All 16,715 Gamma visits keep `source_patient_id` NULL.
- Every Beta visit in this dataset has exactly one diagnosis.
- A second run of the same batch produced identical counts, because outputs are overwritten, not appended.

**Demo trace, Alpha `V533959`:** SITE-08, 2024-07-14, owner C568564, patient 1988096, `invoice_total` 650.50; one charge line (Dental cleaning under anesthesia, 650.50); diagnosis DEN-PD Periodontal disease.

### Error-case test (`alina-test-01`, `fixtures/practice_test`)

The test batch contains 3 real rows per source plus deliberately broken rows:
- Alpha: a row with too few fields, a date with month 13, a charge `Recheck exam:abc`, and a second copy of `V500000`
- Beta: an unfinished JSON line
- Gamma: a blank `VISIT_NO`, and a total of `12,5`

| Source | Rows | Accepted | Rejected |
| --- | --- | --- | --- |
| vet_alpha | 7 | 2 | 5 |
| vet_beta | 4 | 3 | 1 |
| vet_gamma | 5 | 3 | 2 |

| Source | Reason code | Rows |
| --- | --- | --- |
| vet_alpha | DUPLICATE_KEY | 2 |
| vet_alpha | INVALID_CHARGE_LINE | 1 |
| vet_alpha | INVALID_DATE | 1 |
| vet_alpha | MALFORMED_RECORD | 1 |
| vet_beta | MALFORMED_RECORD | 1 |
| vet_gamma | INVALID_AMOUNT | 1 |
| vet_gamma | MISSING_SOURCE_VISIT_ID | 1 |

Every broken row was caught with the expected reason code, and only the accepted visits' charge lines were written (Alpha 4 lines totalling 1,458.75; Beta 6 lines totalling 1,033.42, both equal to the accepted invoice totals).

## 5. How to rerun

Land the batch under `/isit312/vet/raw/<batch>/data/pims/` (keeping the ZIP's paths), then:

```bash
export BATCH_ID=<batch>          # ISIT312_HDFS_ROOT defaults to /isit312/vet
$SPARK_HOME/bin/spark-shell --master local[*]
scala> :paste spark/normalize_practice.sc
```

Error-case test:

```bash
hadoop fs -mkdir -p /isit312/vet/raw/alina-test-01/data
hadoop fs -put fixtures/practice_test/data/pims /isit312/vet/raw/alina-test-01/data/
export BATCH_ID=alina-test-01
```

## 6. Limitations

- Duplicate source keys are all rejected rather than resolved, because the parser cannot tell which copy is correct. None occur in the TailSignal data.
- Gamma has no patient ID or charge breakdown, so these stay NULL or visit-level. Gamma diagnoses are free text only.
- Beta's invoice total is derived from its charges, because the source has no total to check against.
- `locations.csv` is inventoried but not used to validate sites, so a visit at a site registered to a different system would not be flagged.
