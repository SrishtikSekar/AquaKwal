# AquaKwal — Large-Scale Water Quality Analytics & Pattern Discovery

A distributed big-data analytics pipeline that ingests raw water quality data,
cleans and transforms it with Apache Pig, makes it queryable through Apache Hive,
and trains a machine learning classifier with Spark MLlib to predict water
safety from physico-chemical features.

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
│   ├── etl_clean.pig         # ETL: cast, dedup, filter, derive quality label
│   └── sample_source_join.pig # Multi-source join (samples + site metadata)
├── hive/
│   └── warehouse.hql         # External table DDL + reporting views
├── spark/
│   └── ml_quality.py         # Spark MLlib RandomForest classifier w/ CV
├── data/
│   ├── generate_data.py      # Synthetic dataset generator
│   ├── water_quality_samples.csv
│   └── site_metadata.csv
├── scripts/
│   └── run_pipeline.sh       # End-to-end pipeline runner
├── docs/
│   ├── proposal.md
│   └── report.md
├── README.md
└── .gitignore
```

---

## Prerequisites

- Docker Engine + Docker Compose v2
- 8+ GB RAM allocated to Docker
- Ports 9870, 50070, 10000, 8080, 7077 free

---

## Quick Start

```bash
# 1. Generate/replace sample data (or use your own CSV at data/)
cd data && python3 generate_data.py

# 2. Build and start the cluster
docker compose up -d

# 3. Wait ~30 s, then run the full pipeline
./scripts/run_pipeline.sh
```

**Expected output** (console tail):
- HDFS shows `/data/raw/`, `/data/clean/`, `/data/output/ml_results/` populated
- Hive reports 10-state summary with avg pH and sample counts
- Spark prints `AUC`, `F1`, `Accuracy`, and top-5 feature importances

---

## Pipeline Stages in Detail

### Stage 1 — HDFS (Storage)
Raw CSV files (`water_quality_samples.csv`, `site_metadata.csv`) are uploaded
to HDFS at `/data/raw/`. HDFS provides the durable, distributed backing store.

### Stage 2 — Pig (ETL)
`pig/etl_clean.pig`:
1. Loads raw records as chararray (defensive).
2. Casts numerics, replacing `NA` / empty / invalid strings with `NULL`.
3. Derives `water_quality_label` (SAFE/UNSAFE) from WHO pH (6.5–8.5) and
   dissolved-oxygen (≥5 mg/L) thresholds.
4. Filters records with impossible values (pH > 14, etc.).
5. Deduplicates by `(site_id, sample_date)` via GROUP + LIMIT.
6. Stores cleaned data to `/data/clean/water_quality_clean`.

`pig/sample_source_join.pig` joins samples with station metadata on `site_id`.

### Stage 3 — Hive (Warehouse)
`hive/warehouse.hql` defines two external tables over the Pig output and
creates reporting views:
- `v_state_summary` — avg metrics + % unsafe per state
- `v_unsafe_sites` — stations where >50% of samples are UNSAFE
- `v_monthly_trend` — seasonal pH/DO/nitrate trends

### Stage 4 — Spark MLlib (Classification)
`spark/ml_quality.py`:
1. Reads the Pig-cleaned, Hive-enriched dataset from HDFS.
2. Assembles a 10-feature vector (pH, DO, temperature, conductivity, turbidity,
   nitrate, sulfate, lat, lon, elevation).
3. Standard-scales features; StringIndexes the label.
4. 5-fold cross-validated RandomForest (grid over numTrees, maxDepth).
5. Evaluates AUC, F1, accuracy, and emits feature importances.
6. Writes predictions + metrics to `/data/output/ml_results/`.

---

## Demo Script

```bash
# Run a 30-second demo of each stage
./scripts/demo.sh
```

The demo script:
1. Confirms HDFS raw data presence
2. Shows Pig output row count & null report
3. Runs a Hive summary query
4. Triggers the Spark model and prints metrics

---

## Progress Check-in Artifacts

| Check-in | Evidence |
|----------|----------|
| 1 — Ingestion | `hdfs dfs -ls /data/raw/` shows both CSVs |
| 2 — Draft results | Hive `v_state_summary` query + Spark AUC output |

---

## Reproducing Results

All results are reproducible with the fixed random seed (`42`) in:
- `data/generate_data.py`
- `spark/ml_quality.py`
- `scripts/run_pipeline.sh`

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| `Service 'namenode' is unhealthy` | Stop cluster, `docker system prune`, retry |
| `Permission denied` on HDFS | Run `hdfs dfs -chmod -R 777 /data` inside namenode |
| Spark can't resolve `namenode` | Ensure `fs.defaultFS=hdfs://namenode:9000` in env |

---

## License

Apache 2.0 — see LICENSE.
