# AquaKwal — Water Quality Analytics Pipeline

A distributed big-data analytics pipeline that ingests raw water quality data,
cleans and transforms it with Apache Pig, makes it queryable through Apache Hive,
and trains a machine learning classifier with Spark MLlib to predict water
potability from physico-chemical features.

**Pipeline architecture:** HDFS → Pig (ETL) → Hive (warehouse) → Spark MLlib (classification)

---

## Team

| Name | Role |
|------|------|
| Student A | Pipeline orchestration & Pig ETL |
| Student B | Hive warehouse, reporting views & validation |
| Student C | Spark MLlib model & feature analysis |

*(Update with real names.)*

---

## Tools & Versions

| Tool | Version | Docker Image |
|------|---------|--------------|
| Hadoop (HDFS, NameNode, DataNode) | 3.2.1 | bde2020/hadoop-namenode:2.0.3-hadoop3.2.1-java8 |
| Apache Pig | 0.17.0 | bde2020/hadoop-pig:2.0.3-hadoop3.2.1-java8 |
| Apache Hive | 2.3.2 | bde2020/hive-server2:2.0.0-hive2.3.2-spark2.4.0-hadoop3.2.1-java8 |
| Apache Spark | 3.5.0 | bitnami/spark:3.5.0 |

---

## Project Structure

```
AquaKwal/
├── docker/
│   ├── docker-compose.yml    # 6-service cluster definition
│   ├── bootstrap.sh          # HDFS init + daemon startup script
│   └── hive-site.xml         # Hive metastore config override
├── pig/
│   ├── etl_clean.pig         # ETL: cast, impute means, dedup, derive label
│   └── sample_source_join.pig # Join cleaned data with WHO thresholds
├── hive/
│   └── warehouse.hql         # External table DDL + reporting views + validation
├── spark/
│   └── ml_quality.py         # Spark MLlib RandomForest classifier w/ CV
├── data/
│   ├── water_potability.csv  # Real Kaggle water quality dataset (3,276 rows)
│   └── parameter_thresholds.csv  # WHO drinking-water guideline thresholds
├── scripts/
│   ├── run_pipeline.sh       # End-to-end pipeline runner
│   └── demo.sh               # Per-stage verification script
├── docs/
│   ├── proposal.md           # Project proposal
│   ├── report.md             # Final report (6-10 pages)
│   └── operations.md         # Detailed ops/runbook
├── README.md
└── .gitignore
```

---

## Prerequisites

- Docker Engine + Docker Compose v2
- 6+ GB RAM allocated to Docker
- Ports 9870, 50070, 9000, 10000, 8080, 7077, 4040 free

---

## Quick Start

```bash
# 1. Start the cluster
docker compose up -d

# 2. Wait ~60 s for HDFS to be healthy
#    Check: docker exec namenode hdfs dfsadmin -report

# 3. Run the full pipeline
./scripts/run_pipeline.sh
```

**Expected output (console tail):**
- HDFS shows `/data/raw/`, `/data/clean/`, `/data/output/ml_results/` populated
- Hive reports potability summary with avg pH and sample counts
- Spark prints `AUC`, `F1`, `Accuracy`, and feature importances

---

## Pipeline Stages in Detail

### Stage 1 — HDFS (Storage)
The raw `water_potability.csv` (3,276 rows, 9 physico-chemical features + 1
binary label) and `parameter_thresholds.csv` (WHO guidelines) are uploaded to
HDFS at `/data/raw/`.

### Stage 2 — Pig (ETL)
`pig/etl_clean.pig`:
1. Loads raw CSV as chararray (defensive parsing).
2. Strips the header row.
3. Casts 9 numeric columns to float/int with regex guards (invalid → NULL).
4. Computes column means via `GROUP ALL` + `AVG`.
5. CROSSes means back to each row and imputes NULLs (mean imputation).
6. Derives `water_quality_label` (POTABLE/NOT_POTABLE) from the Potability flag.
7. Deduplicates exact duplicates via `DISTINCT`.
8. Stores cleaned data to `/data/clean/water_quality_clean`.

`pig/sample_source_join.pig` joins the cleaned data with the WHO thresholds
reference via CROSS (lookup pattern) and computes a `standards_violation_count`
per sample — how many parameters exceed guideline limits.

### Stage 3 — Hive (Warehouse)
`hive/warehouse.hql` defines external tables over Pig's output and creates
reporting views:
- `v_potability_summary` — avg metrics + violation count per potability class
- `v_violation_analysis` — violation count distribution
- `v_param_stats` — min/mean/max per parameter

Validation queries check row counts, duplicates, null percentages, and class balance.

### Stage 4 — Spark MLlib (Classification)
`spark/ml_quality.py`:
1. Reads the Pig-cleaned, Hive-backed enriched dataset from HDFS.
2. Assembles a 10-feature vector (9 water quality parameters + violation count).
3. Standard-scales features; StringIndexes the label.
4. 5-fold cross-validated RandomForest (grid over numTrees, maxDepth).
5. Evaluates AUC, F1, accuracy, and emits feature importances.
6. Writes predictions + metrics to `/data/output/ml_results/`.

---

## Demo Script

```bash
# Run a quick check of each pipeline stage
./scripts/demo.sh
```

The demo script:
1. Lists running containers
2. Checks HDFS raw data presence
3. Counts Pig cleaned rows
4. Runs the Hive summary view
5. Reads Spark metrics from HDFS

---

## Progress Check-in Artifacts

| Check-in | Evidence |
|----------|----------|
| 1 — Ingestion | `hdfs dfs -ls /data/raw/` shows water_potability.csv |
| 2 — Draft results | Hive `v_potability_summary` + Spark AUC output |

---

## Reproducing Results

All random seeds are fixed (`42`) in:
- `spark/ml_quality.py` (train/test split, CV, RandomForest)

Data is the real `water_potability.csv` — no synthetic generation needed.

---

## License

Apache 2.0
