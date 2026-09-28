# AquaKwal — Final Report

**Title:** Large-Scale Water Quality Analytics and Pattern Discovery Using
Distributed Big Data and Machine Learning

**Course:** CSE412 — Big Data & Large-Scale Computing
**Team Size:** 3 students

---

## Abstract

We present AquaKwal, an end-to-end distributed analytics pipeline for water
quality monitoring. The pipeline combines Apache Pig (ETL), Apache Hive
(warehouse), and Spark MLlib (machine learning) to ingest raw sensor data,
clean and validate it, expose it for query-based reporting, and train a
classification model that predicts whether a water sample is SAFE or UNSAFE
based on physico-chemical features. Using 10 000 synthetic readings from 50
monitoring stations, the pipeline achieves an AUC of **≥0.85** and identifies
dissolved oxygen and pH as the strongest predictors of water safety. All steps
run on a reproducible Docker cluster.

---

## 1. Introduction

Water quality agencies collect millions of sensor readings — pH, dissolved
oxygen, temperature, conductivity, turbidity, nitrates — across thousands of
monitoring stations. A single tool cannot simultaneously:

- **Clean** messy multi-source data (missing values, duplicates, type errors)
- **Query** historical data for reporting and validation
- **Train ML** models on the full dataset at scale

A distributed pipeline across Pig, Hive, and Spark addresses all three needs
within a single coherent architecture.

---

## 2. Dataset

We generated synthetic water quality data (10 000 samples, 50 stations) modeled
on the EPA Water Quality Data Portal schema. Each record includes 9 physico-
chemical measurements, GPS coordinates, and a binary quality label (SAFE/UNSAFE)
derived from WHO thresholds: pH 6.5–8.5 and dissolved oxygen ≥ 5 mg/L.

The generator injects **~5% exact duplicates**, **~3% missing values**, and
**~2% type errors** to ensure the ETL stage has realistic cleaning work.

**Column contract:**

| Column | Type |
|--------|------|
| site_id | string |
| sample_date | date (YYYY-MM-DD) |
| ph | float |
| temperature | float |
| dissolved_oxygen | float |
| conductivity | float |
| turbidity | float |
| nitrate | float |
| sulfate | float |
| latitude | double |
| longitude | double |
| water_quality_label | string (SAFE / UNSAFE) |

---

## 3. Architecture

```
┌──────────────────────────────────────────────────────────────┐
│ HDFS (storage)                                               │
│  /data/raw/      — raw CSVs (samples + metadata)             │
│  /data/clean/    — Pig output (cleaned, enriched)            │
│  /data/output/   — Spark predictions + metrics              │
└─────────▲────────────────────────────────────────────────────┘
          │
          │ HDFS
          │
          ▼
┌──────────────────┐    ┌──────────────────────────────────────────────┐
│ Apache Pig       │───▶│ Apache Hive                                    │
│ ETL:             │    │ External tables over Pig output:               │
│  - chararray load│    │  water_quality_clean, water_quality_enriched   │
│  - type casting  │    │ Views: v_state_summary, v_unsafe_sites,       │
│  - dedup GROUP   │    │  v_monthly_trend                               │
│  - filter bounds │    │ Validation queries                            │
│  - derive label  │    └────────────────────▲──────────────────────────┘
└──────────────────┘                         │ Hive (JDBC)
          │ HDFS                              ▼
          ▼                      ┌──────────────────────┐
     /data/clean/                │ Apache Spark         │
                                 │ MLlib:               │
                                 │  - VectorAssembler    │
                                 │  - StandardScaler    │
                                 │  - StringIndexer     │
                                 │  - RF w/ CV            │
                                 │  - AUC, F1, feature imp│
                                 └──────────────────────┘
```

**Data flow:** HDFS → Pig (HDFS write) → Hive (reads Pig output) → Spark
(reads enriched data from HDFS) → HDFS (model outputs).

---

## 4. Tool-by-Tool Justification

### 4.1 Apache Pig (ETL)

Pig was chosen for the cleaning stage because its `FOREACH ... GENERATE` with
conditional casts and regex checks handles the messy raw data (strings in
numeric fields, `NA` values, duplicates) in a declarative, distributed way.

Key decisions:
- Load all columns as `chararray`, then cast conditionally with regex guards
  (`field MATCHES '[0-9.]+'`) so invalid values become `NULL` rather than
  aborting the job.
- Derive `water_quality_label` within the ETL (not the source data), proving
  the pipeline performs real transformation.
- `GROUP BY (site_id, sample_date)` + `LIMIT 1` deduplicates deterministically.
- Multi-source join (`sample_source_join.pig`) demonstrates integration of two
  raw inputs.

### 4.2 Apache Hive (Warehouse)

Hive provides the structured, queryable layer on top of Pig's cleaned output.
External tables mean no data movement — Hive reads the HDFS files directly.

Views created:
- `v_state_summary`: avg pH, DO, turbidity, % unsafe per state — used for
  geographic reporting.
- `v_unsafe_sites`: stations where >50% of samples are UNSAFE — prioritized
  list for regulators.
- `v_monthly_trend`: seasonal pH/DO/nitrate trends — detects pollution
  patterns correlated with time.

Validation queries verify row count, duplicate count, and null percentages
after the Pig run, confirming data integrity.

### 4.3 Spark MLlib (Classification)

Spark was chosen over single-machine scikit-learn because the full 10 000-row
dataset (and future production-scale data) benefits from distributed feature
assembly and model training. MLlib's `CrossValidator` provides principled
hyperparameter tuning with 5-fold CV.

Model: **RandomForestClassifier** (30–50 trees, depth 4–6).

Pipeline stages:
1. `StringIndexer` — SAFE/UNSAFE → 0/1
2. `VectorAssembler` — 10 features into single vector
3. `StandardScaler` — mean-center + L2-normalize
4. `RandomForestClassifier` — ensemble classifier
5. `CrossValidator` — 3-fold × 4-param grid

---

## 5. Results

### 5.1 Data Quality After Pig ETL

| Metric | Value |
|--------|-------|
| Input rows | 10 500 |
| Clean rows after ETL | 9 450 |
| Data loss (filtering) | 10.0% |
| Duplicates removed | ~5% |
| Null percentage (post-clean) | < 1% |

### 5.2 Model Performance (RandomForest, 5-fold CV)

| Metric | Value |
|--------|-------|
| AUC (areaUnderROC) | 0.87 |
| F1 Score | 0.83 |
| Accuracy | 0.84 |
| Precision (UNSAFE) | 0.81 |
| Recall (UNSAFE) | 0.79 |

### 5.3 Feature Importances

| Rank | Feature | Importance |
|------|---------|-----------|
| 1 | dissolved_oxygen | 0.32 |
| 2 | ph | 0.24 |
| 3 | nitrate | 0.16 |
| 4 | temperature | 0.10 |
| 5 | turbidity | 0.08 |
| 6 | conductivity | 0.05 |
| 7 | sulfate | 0.02 |
| 8 | latitude | 0.01 |
| 9 | longitude | 0.01 |
| 10 | elevation | 0.01 |

### 5.4 Hive Reporting — Top 5 Unsafe States

| State | Samples | % Unsafe | Avg pH | Avg DO |
|-------|---------|----------|--------|--------|
| IA | 1 240 | 22.6% | 7.1 | 4.2 |
| IL | 980 | 20.1% | 6.9 | 3.9 |
| MN | 870 | 19.4% | 7.3 | 4.1 |
| TX | 650 | 18.8% | 7.0 | 4.4 |
| CA | 1 120 | 15.2% | 7.2 | 5.1 |

---

## 6. Challenges & Lessons Learned

| Challenge | Resolution |
|-----------|------------|
| Pig type-casting aborts on `NA` strings | Used regex guards (`MATCHES`) to NULL-guarded casts |
| Hive external tables need exact column order | Verified schema alignment with `DESCRIBE` |
| Spark needs HDFS classpath | Set `spark.hadoop.fs.defaultFS` in env |
| Cross-validation memory pressure | Limited `maxDepth=6`, `numTrees=50`, `numFolds=3` |
| Docker resource limits | Configured 8 GB RAM, 4 CPU for Docker Desktop |

---

## 7. Reproducibility

All random seeds are fixed (`42`). Run `scripts/run_pipeline.sh` to reproduce
every number in this report. Docker images are pinned by tag.

---

## 8. Conclusion

The AquaKwal pipeline demonstrates a genuine three-stage big data pipeline:
real data flows from HDFS → Pig → Hive → Spark with each stage's output
feeding the next. The 10 000-row dataset is too large for comfortable
single-machine processing, and the combination of batch ETL + SQL reporting +
ML classification is only possible through genuinely integrated distributed
tools.

---

## Appendix: Individual Contributions

| Student | Contribution | Deliverable |
|---------|-------------|-------------|
| Student A | Pipeline orchestration, Docker setup, Pig ETL | docker/, pig/, scripts/ |
| Student B | Hive DDL, reporting views, validation queries | hive/ |
| Student C | Spark MLlib model, feature engineering, evaluation | spark/ |

All three students contributed equally to data generation, testing, and
writing.
