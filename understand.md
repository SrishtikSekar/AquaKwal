# Understanding AquaKwal

## What this project is

AquaKwal is an end-to-end big-data pipeline for analyzing Indian river-water
quality measurements derived from CPCB data. It demonstrates how raw monitoring
data can move through a distributed data stack:

```text
CSV source
  -> validated canonical data
  -> HDFS storage
  -> Pig cleaning and rule-based enrichment
  -> Hive analytical tables and views
  -> Spark ML classification
  -> predictions, metrics, and feature importance
```

The project is not only a classifier. It is a complete, testable data workflow
with explicit contracts between ingestion, ETL, warehousing, reporting, and
machine learning.

## The main idea that sets AquaKwal apart

AquaKwal combines **transparent environmental rules** with **distributed
machine learning**.

Most similar student projects stop after either:

- calculating a water-quality label with fixed thresholds; or
- training a model from a prepared CSV.

AquaKwal does both and keeps the two results useful for different purposes:

1. **The rules explain why a sample is poor.** Pig applies CPCB-style limits for
   dissolved oxygen, pH, BOD, and fecal coliform. The enriched dataset contains
   `standards_violation_count`, ranging from 0 to 4.
2. **The model learns the overall classification pattern.** Spark trains a
   cross-validated Random Forest and produces predictions, evaluation metrics,
   and ranked feature importance.
3. **Hive makes the results reportable.** External tables and views summarize
   state-level quality, water-body quality, yearly trends, and individual rule
   violations.
4. **Every boundary is checked.** The runner validates row preservation and
   field counts so a successful command means the complete workflow actually
   ran, rather than merely starting its containers.

This hybrid design is AquaKwal's clearest differentiator: it provides the
scalability and pattern discovery of ML without giving up the understandable
threshold violations needed to interpret water-quality decisions.

## Technology stack

| Component | Responsibility |
|---|---|
| Python | Validates and normalizes source CSV data |
| HDFS | Stores raw, clean, enriched, and model-output datasets |
| Apache Pig | Cleans data, imputes missing values, removes duplicates, labels samples, and counts violations |
| Apache Hive | Exposes external tables and reusable reporting views |
| Apache Spark MLlib | Trains and evaluates a Random Forest classifier |
| Docker Compose | Reproduces and connects the complete environment |
| `unittest` | Tests converters, contracts, scripts, Compose, and real Pig input/output behavior |

## Data and schema flow

### 1. Source input

The source file is expected at:

```text
data/Indian_water_data.csv
```

The converter supports:

- the native 23-column project layout; and
- the public 22-column river dataset with `Year` as its final field.

For the 22-column source, the converter moves the year to the canonical
position and inserts `RIVER` as `water_body_type`. It also removes embedded
newlines and replaces pipe characters so every record is safe for downstream
pipe-delimited processing.

The canonical converter output has 23 fields.

### 2. Pig clean output

Pig performs:

- header removal;
- rejection of rows without a station code;
- numeric type conversion;
- mean imputation for missing numeric values;
- GOOD/POOR labeling using water-quality thresholds; and
- exact deduplication after cleaning.

The output has 24 fields: the 23 canonical input fields plus
`water_quality_label`.

### 3. Pig enriched output

The clean data is combined with `data/parameter_thresholds.csv`. Pig counts
violations of these conditions:

| Measurement | Poor-quality condition |
|---|---|
| Dissolved oxygen | Minimum below 4.0 |
| pH | Minimum below 6.5 or maximum above 8.5 |
| BOD | Maximum above 3.0 |
| Fecal coliform | Maximum above 2,500 |

The enriched output has 25 fields: the clean record plus
`standards_violation_count`.

### 4. Hive warehouse

Hive exposes two external tables:

- `water_quality_clean`
- `water_quality_enriched`

It also creates four analytical views:

- `v_state_summary`
- `v_water_body_summary`
- `v_yearly_trend`
- `v_violation_analysis`

### 5. Spark ML output

Spark uses 18 numeric water measurements as features. It performs an 80/20
seeded split, uses three-fold cross-validation over four Random Forest
configurations, and writes:

```text
/data/output/ml_results/predictions
/data/output/ml_results/metrics
/data/output/ml_results/feature_importances
```

Predictions are stored as Parquet. Metrics and feature importances are stored
as single-part CSV datasets.

## Verified execution results

The full Docker pipeline was executed successfully on 9 October 2026.

| Check | Result |
|---|---:|
| Logical source rows | 10,006 |
| Rows without station code | 21 removed |
| Duplicate clean rows | 364 removed |
| Final clean rows | 9,621 |
| Final enriched rows | 9,621 |
| GOOD samples | 3,383 |
| POOR samples | 6,238 |
| Training rows | 7,774 |
| Test rows | 1,847 |
| Test AUC | 0.9992 |
| Test F1 | 0.9962 |
| Test accuracy | 0.9962 |
| Automated tests | 6/6 passed |

The three most important model features in that run were:

1. `bod_max` — 0.3343
2. `fecal_coliform_max` — 0.2057
3. `ph_max` — 0.1452

## Important interpretation warning

The GOOD/POOR label is generated from dissolved oxygen, pH, BOD, and fecal
coliform values. Those measurements are also model features. Therefore, the
very high model score mainly proves that the Random Forest learned the same
labeling rules; it is not evidence that the model independently predicts future
environmental conditions.

This distinction should be stated clearly in reports and presentations. A
future predictive study would need an independently observed target, a
time-aware split, and validation on unseen locations or later years.

## Run the project

From the repository root:

```bash
docker compose -f docker/docker-compose.yml up -d --build
./scripts/run_pipeline.sh
./scripts/demo.sh
```

Run the regression tests with:

```bash
python3 -m unittest discover -s tests -v
```

Useful interfaces while the cluster is running:

- HDFS NameNode: <http://localhost:9870>
- Spark master: <http://localhost:8080>
- HiveServer2: `localhost:10000`

## Important project files

| Path | Purpose |
|---|---|
| `data/convert_csv.py` | Source validation and normalization |
| `data/parameter_thresholds.csv` | Rule thresholds used for enrichment |
| `pig/etl_clean.pig` | Cleaning, imputation, labeling, and deduplication |
| `pig/sample_source_join.pig` | Threshold join and violation counting |
| `hive/warehouse.hql` | External tables and reporting views |
| `spark/ml_quality.py` | Random Forest training and output generation |
| `scripts/run_pipeline.sh` | Complete validated pipeline runner |
| `scripts/demo.sh` | Short operational demonstration |
| `docker/docker-compose.yml` | Distributed service configuration |
| `tests/test_pipeline.py` | Converter, Pig, syntax, schema, and Compose tests |
| `docs/testing.md` | Detailed test evidence and limitations |

## Known data-quality limitations

- The public source contains inconsistent state spelling, capitalization, and
  spacing. These values are intentionally preserved, so 45 distinct state-name
  strings do not necessarily represent 45 administrative regions.
- The source lacks an explicit water-body type; `RIVER` is an ingestion
  assumption for this dataset.
- Mean imputation is useful for demonstrating ETL but may hide meaningful
  missingness in scientific analysis.
- Thresholds are configuration data, but their regulatory interpretation
  should be revalidated before using the project for real operational decisions.

## One-sentence project summary

**AquaKwal is a reproducible HDFS–Pig–Hive–Spark pipeline that turns messy CPCB
river measurements into both explainable standards violations and scalable ML
analytics, with tested schema and row-flow guarantees from input to output.**
