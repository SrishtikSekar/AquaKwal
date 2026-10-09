# AquaKwal Test Report

**Test date:** 9 October 2026  
**Result:** PASS after corrections  
**Scope:** converter, Pig ETL and enrichment, HDFS row/field flow, Hive external
tables and views, Spark ML training/output, orchestration, and Docker Compose.

## Acceptance results

| Stage | Result | Measured evidence |
|---|---:|---|
| Source CSV | PASS | 10,006 logical data rows; every row has 22 fields; years 2012–2023 |
| Converter | PASS | 10,007 physical lines including header; every line has 23 pipe-delimited fields |
| Pig clean | PASS | 9,621 rows; every row has 24 fields |
| Pig enrichment | PASS | 9,621 rows; every row has 25 fields; violation counts range 0–4 |
| Hive clean table | PASS | 9,621 rows, years 2012–2023, 45 distinct source state-name strings |
| Hive labels | PASS | 3,383 GOOD and 6,238 POOR |
| Hive enriched table/views | PASS | 9,621 rows; queried state/water-body/year/violation views returned 45/1/10/45 rows |
| Spark split | PASS | 7,774 training and 1,847 test rows, seed 42 |
| Spark model | PASS | AUC 0.9992; F1 0.9962; accuracy 0.9962 |
| Spark outputs | PASS | Predictions Parquet plus metrics and feature-importance CSV datasets in HDFS |
| Demo | PASS | Cluster, HDFS, Pig row count, Hive view, and Spark metrics displayed |
| Automated regression suite | PASS | 6/6 tests passed in 9.267 seconds |

The test-set confusion counts were 1,226 correct class-0 predictions, 614
correct class-1 predictions, and 7 class-0 rows predicted as class 1. Class
indices are assigned by Spark's frequency-based `StringIndexer`; in this run
class 0 was POOR and class 1 was GOOD.

The three highest Random Forest importances were `bod_max` (0.3343),
`fecal_coliform_max` (0.2057), and `ph_max` (0.1452).

## Input/output flow

| Boundary | Input | Output | Validation |
|---|---|---|---|
| CSV converter | Public 22-column CSV | Canonical 23-column pipe data | Width, header mapping, embedded newline/pipe sanitization |
| Pig ETL | 10,006 source rows | 9,621 clean rows | Nonblank station filter, numeric casts, mean imputation, deduplication, CPCB label |
| Pig enrichment | 9,621 clean rows + one threshold row | 9,621 enriched rows | Row preservation, 25-field width, four-rule violation count |
| Hive | Clean/enriched HDFS paths | Two external tables + four views | Counts, year bounds, labels, summaries |
| Spark | 24-field clean rows | Predictions, metrics, importances | Two classes, nonempty seeded split, 3-fold CV, finite test metrics |

The source had 21 blank station codes, which Pig intentionally filtered. It
then removed 364 duplicate clean rows. The public source contains 362 exact raw
duplicates; two additional records became duplicates after missing-value
imputation. Thus `10,006 - 21 - 364 = 9,621` clean rows.

## Automated and fixture tests

`python3 -m unittest discover -s tests -v` covers:

- native 23-column conversion, embedded commas, invalid width, and unsafe pipe sanitization;
- public 22-column normalization;
- a real local Pig clean/enrichment flow, including imputation, deduplication, labels, and violation counts;
- Pig/Hive/Spark delimiter contracts;
- shell and Python syntax;
- Docker Compose validation.

The separate local Spark fixture smoke test exercises loading, cross-validation,
prediction, and all output writes. Its 1.0 fixture scores only validate program
flow and are not real-data performance results.

## Defects found and corrected

1. Pig enrichment still expected an obsolete 11-column schema; it now consumes
   the actual 24-column clean output and emits 25 columns.
2. The runner omitted enrichment and ignored Hive errors; it now runs both Pig
   stages under strict failure handling and validates row counts and widths.
3. Comma-delimited intermediate records broke locations containing commas;
   Pig, Hive, and Spark now consistently use `|`.
4. The converter used a machine-specific path and could not read the public
   22-column layout; it now has a CLI, schema validation, normalization, and
   safe single-line output.
5. HiveServer2 started against an uninitialized Derby schema and the runner
   invoked a second embedded client. Startup now initializes the schema and all
   queries use Beeline through HiveServer2.
6. The stock Spark image lacked NumPy and the worker used incompatible
   environment variables. Custom images add NumPy and the worker now registers
   with the master.
7. Spark's parameter grid referenced the wrong estimator and its displayed
   confusion matrix omitted counts. Both are corrected.
8. Spark metric/importances CSVs are coalesced to one part so consumers see one
   header per dataset.
9. The demo queried a nonexistent view; it now uses `v_state_summary`.

## Interpretation and limitations

The downloaded public source is the MIT-licensed CPCB-derived
`RiverWaterQualityOverYears.csv` from
<https://github.com/KunalLatkar/RiverWaterQualityDataset>. Its layout omits
water-body type, so conversion explicitly assumes `RIVER` for every record.
The downloaded source SHA-256 was
`6c9102b296f20abdbf82a06ef018bf01e526b19ba42ea1ba109f461110fd0332`.
State names are not normalized: capitalization, spacing, and values such as
`CPCB, DELHI` remain source values, explaining the 45 distinct strings.

The GOOD/POOR target is derived from dissolved oxygen, pH, BOD, and fecal
coliform in each row, while the model uses those same measurements as features.
The high metrics therefore show that the model learned the labeling rule; they
are not independent evidence of environmental prediction accuracy.

## Reproduction

```bash
docker compose -f docker/docker-compose.yml up -d --build
./scripts/run_pipeline.sh
./scripts/demo.sh
python3 -m unittest discover -s tests -v
```

Outputs are written beneath `/data/clean` and `/data/output/ml_results` in HDFS.
