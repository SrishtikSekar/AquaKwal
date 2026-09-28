# AquaKwal Project Proposal

**Title:** Large-Scale Water Quality Analytics and Pattern Discovery Using Distributed Big Data and Machine Learning

**Team:** 3 students (Team AquaKwal)

---

## 1. Problem Statement

Water quality monitoring agencies generate millions of sensor readings daily.
Detecting pollution patterns, unsafe sites, and seasonal trends across this
volume of data exceeds what a single-tool or single-machine approach can
handle. We need a pipeline that **(a)** ingests and cleans messy multi-source
data, **(b)** makes it queryable for interactive analysis and reporting, and
**(c)** trains and evaluates a machine learning model to *predict* water
safety from physico-chemical features at scale.

## 2. Dataset

We use synthetic water quality data modeled on EPA Water Quality Data Portal
schema:

| Field | Description |
|-------|-------------|
| `site_id` | Monitoring station ID (50 unique) |
| `sample_date` | ISO date of measurement |
| `ph` | pH (0–14) |
| `temperature` | °C |
| `dissolved_oxygen` | mg/L |
| `conductivity` | µS/cm |
| `turbidity` | NTU |
| `nitrate` | mg/L |
| `sulfate` | mg/L |
| `latitude` / `longitude` | GPS coordinates |
| `water_quality_label` | SAFE / UNSAFE (target) |

**Volume:** 10 000 raw readings + 50 station records. Noise (duplicates,
missing values, type errors) is injected so the ETL has real cleaning to do.

**Source for production use:** EPA Water Quality Data Portal
(`https://www.waterqualitydata.us`).

## 3. Chosen Tool Combination

**Option E — Cross-Cutting Pipeline:** Pig → Hive → Spark MLlib

| Stage | Tool | Role |
|-------|------|------|
| 1. ETL | Apache Pig | Clean, dedup, type-cast, join multi-source data |
| 2. Warehouse | Apache Hive | Queryable tables + reporting views |
| 3. ML | Spark MLlib | Classification: SAFE vs UNSAFE |

## 4. Expected Outcome

- 10 000 input rows → ~9 500 clean rows after Pig ETL (≤5% data loss)
- Hive reporting views showing top 5 unsafe states/watersheds
- Spark RandomForest: **AUC ≥ 0.85**, F1 ≥ 0.80, identifying **dissolved
  oxygen** and **pH** as top predictors
- End-to-end run time < 2 minutes on a 6-container Docker cluster

## 5. Architecture Diagram

```
HDFS (/data/raw/)
   │
   ▼
Pig ETL — cast/clean/dedup/join
(pig/etl_clean.pig, sample_source_join.pig)
   │
   ▼
HDFS (/data/clean/)
   │
   ▼
Hive Warehouse — external tables + views
(hive/warehouse.hql)
   │
   ▼
Spark MLlib — RandomForest CV
(spark/ml_quality.py)
   │
   ▼
HDFS (/data/output/ml_results/)
  predictions/  metrics/  feature_importances/
```

## 6. Feasibility

All tools are available via pre-built Docker images (`bde2020` / `bitnami`).
The synthetic data generator ensures reproducible, runnable results without
network data access. Pipeline is fully containerized for any Linux host.

## 7. Timeline

| Week | Milestone |
|------|-----------|
| 1 | Docker cluster up, raw data generated |
| 2 | Pig ETL complete, data in HDFS |
| 3 | Hive tables + views validated |
| 4 | Spark ML model trained & evaluated |
| 5 | Report + demo video finalized |
