# AquaKwal Project Proposal

> **Legacy proposal:** this planned the earlier Kaggle dataset. The implemented
> pipeline uses the 23-column CPCB dataset documented in the root `README.md`.

**Title:** Large-Scale Water Quality Analytics and Pattern Discovery Using Distributed Big Data and Machine Learning

**Team:** 3 students (Team AquaKwal)

---

## 1. Problem Statement

Safe drinking water is a fundamental human right, yet water quality monitoring
generates datasets that are too large and too messy for single-machine tools.
The Water Quality Data Portal and similar repositories publish thousands of
sensor readings with missing values, inconsistent types, and varying quality.
A single tool — whether a SQL database or a standalone ML library — struggles
to simultaneously **clean** messy multi-source data, **query** it for reporting,
and **train ML models** at scale.

We build a pipeline that leverages three complementary distributed tools:
- **Apache Pig** for ETL (imputation, type casting, deduplication, multi-source join)
- **Apache Hive** for a queryable data warehouse with reporting views
- **Spark MLlib** for distributed machine learning at scale

### The Dataset

We use the **Water Potability** dataset (available on Kaggle / data.gov), which
contains 3,276 water sample records from various monitoring stations. Each
record has 9 physico-chemical measurements and a binary `Potability` label
(0 = not potable, 1 = potable). The dataset contains significant missing values
in `ph` (491 nulls), `Sulfate` (781 nulls), and `Trihalomethanes` (162 nulls),
making it a realistic ETL challenge.

---

## 2. Dataset Source

**Primary source:**
- Water Potability Dataset — Kaggle:
  https://www.kaggle.com/datasets/adityadesai13/water-potability
  (3,276 rows × 10 columns, ~525 KB)

**Secondary source (reference):**
- WHO Drinking-Water Quality Guidelines — parameter thresholds:
  `data/parameter_thresholds.csv`

### Schema

| Column | Type | Description |
|--------|------|-------------|
| ph | float | pH level (0–14) |
| Hardness | float | mg/L as CaCO3 |
| Solids | float | mg/L |
| Chloramines | float | mg/L |
| Sulfate | float | mg/L |
| Conductivity | float | µmho/cm |
| Organic_carbon | float | mg/L |
| Trihalomethanes | float | µg/L |
| Turbidity | float | NTU |
| Potability | int (0/1) | 1 = potable, 0 = not potable |

**Class balance:** 1,278 potable (39%) / 1,998 not-potable (61%).

---

## 3. Chosen Tool Combination

**Option E — Cross-Cutting Pipeline: Pig → Hive → Spark MLlib**

The most complete story of the five options. Each tool plays a distinct role:
- **Pig** — scripting-style ETL (impute, cast, join, dedup)
- **Hive** — SQL-style warehouse for validation + reporting
- **Spark MLlib** — distributed ML at scale

### Data Flow

```
HDFS /data/raw/water_potability.csv
   │
   ▼
Pig (etl_clean.pig)
   ├─ Load as chararray (defensive)
   ├─ Cast numerics (regex-guarded)
   ├─ Impute NULLs with column means (GROUP ALL + CROSS back)
   ├─ Derive water_quality_label
   ├─ Deduplicate via DISTINCT
   └─ Store to /data/clean/water_quality_clean
   │
   ▼  (Pig stage 2)
Pig (sample_source_join.pig)
   ├─ CROSS with WHO thresholds reference
   └─ Compute standards_violation_count
   └─ Store to /data/clean/water_quality_enriched
   │
   ▼
Hive (warehouse.hql)
   ├─ External tables over HDFS data
   ├─ v_potability_summary (avg metrics per class)
   ├─ v_violation_analysis (violation distribution)
   └─ v_param_stats (per-parameter statistics)
   │
   ▼
Spark (ml_quality.py)
   ├─ Distributed load from HDFS
   ├─ VectorAssembler (10 features)
   ├─ StandardScaler
   ├─ RandomForest w/ 5-fold CV
   ├─ Evaluate: AUC, F1, Accuracy
   └─ Store predictions + metrics to /data/output/ml_results
```

---

## 4. Expected Outcome

| Metric | Target |
|--------|--------|
| Input rows | 3,276 |
| Clean rows after ETL | ≥3,200 (≥95% retained) |
| AUC (test set) | ≥ 0.75 |
| F1 Score | ≥ 0.70 |
| Top predictors | ph, sulfate, solids |
| Violations range | 0–8 standards violations per sample |

---

## 5. Architecture Diagram

```
┌──────────────────────────────────────────────────────────────┐
│ HDFS (storage)                                               │
│  /data/raw/      — water_potability.csv + parameter_thresholds  │
│  /data/clean/    — Pig output (cleaned + enriched)           │
│  /data/output/   — Spark predictions + metrics              │
└─────────▲────────────────────────────────────────────────────┘
          │
          ▼
┌──────────────────┐
│ Apache Pig       │
│ ETL:             │
│  - chararray load│
│  - type casting  │
│  - mean imputation│
│  - dedup DISTINCT│
│  - CROSS threshold│
└──────┬───────────┘
       │
       ▼  HDFS
       /data/clean/
       │
       ▼
┌──────────────────┐
│ Apache Hive      │
│ Tables:          │
│  water_quality_  │
│  clean / enriched│
│ Views:           │
│  v_potability_   │
│  summary, etc    │
└──────┬───────────┘
       │
       ▼  HDFS
       /data/clean/
       │
       ▼
┌──────────────────┐
│ Apache Spark     │
│ MLlib:           │
│  - RandomForest   │
│  - 5-fold CV      │
│  - AUC, F1        │
│  - Feature imp    │
└──────────────────┘
```

---

## 6. Feasibility

All tools run as pre-built Docker images (`bde2020` for Hadoop/Pig/Hive,
`bitnami` for Spark). The dataset is local CSV (no network access needed).
Pipeline is fully containerized for any Linux/macOS/Windows host with Docker.

**Risks:**
- Image pull is ~4 GB; first-time setup takes 5–10 minutes.
- Spark + Hive integration needs `fs.defaultFS=hdfs://namenode:9000`.
- Pig CROSS with a 1-row reference file is efficient (≈3,276 rows output).

---

## 7. Timeline

| Week | Milestone |
|------|-----------|
| 1 | Docker cluster up, raw data uploaded |
| 2 | Pig ETL complete with imputation + join |
| 3 | Hive tables + views validated via queries |
| 4 | Spark ML model trained with CV evaluation |
| 5 | Report + demo video finalized |
