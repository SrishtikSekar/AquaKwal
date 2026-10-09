# AquaKwal — Water Quality Analytics Pipeline

AquaKwal processes CPCB Indian water-monitoring data through HDFS, Apache Pig,
Apache Hive, and Spark MLlib.

## Data flow

```text
Indian_water_data.csv (supported 22- or 23-column layout)
  -> data/convert_csv.py
  -> Indian_water_data_pipe.csv
  -> HDFS /data/raw
  -> Pig clean/impute/deduplicate (24 columns, pipe-delimited)
  -> Pig threshold enrichment (25 columns, pipe-delimited)
  -> Hive external tables and reporting views
  -> Spark RandomForest over the 24-column clean dataset
  -> predictions, metrics, and feature importances in HDFS
```

The clean row adds `water_quality_label` (`GOOD` or `POOR`). The enriched row
adds `standards_violation_count`. Pipe-delimited intermediate files are used so
commas in monitoring-location names do not shift downstream fields.

## Required input

Place the source file at `data/Indian_water_data.csv`. The converter accepts
the native 23-column layout documented in `pig/etl_clean.pig`, or the public
22-column river layout whose year is last. For that public layout it inserts
`RIVER` as `water_body_type` and moves the year into the canonical position.
Both layouts must include a header. The repository contains
`data/parameter_thresholds.csv`; do not change its five-column schema.

The pipeline runner automatically converts the source CSV. You can also run:

```bash
python3 data/convert_csv.py \
  data/Indian_water_data.csv \
  data/Indian_water_data_pipe.csv
```

## Run the distributed pipeline

Requirements: Docker Engine, Docker Compose v2, at least 6 GB RAM, and the
source dataset above.

```bash
docker compose -f docker/docker-compose.yml up -d --build
./scripts/run_pipeline.sh
./scripts/demo.sh
```

The custom NameNode image installs Pig 0.17.0, the custom Spark images add the
NumPy runtime required by PySpark ML, and Hive startup initializes its embedded
metastore. The runner stops on Pig, Hive, Spark, or pipe failures instead of
reporting a false success.

## Tests

The standard suite uses Python's built-in `unittest` and runs a real local Pig
input/output test when Pig is installed:

```bash
python3 -m unittest discover -s tests -v
```

Run the local Spark model/output smoke test separately:

```bash
python3 tests/run_spark_integration.py
```

See `docs/testing.md` for the test matrix, measured real-data results, fixed
defects, and data-quality limitations.

## Important interpretation note

The `GOOD`/`POOR` target is derived from dissolved oxygen, pH, BOD, and fecal
coliform values in the same row. A RandomForest trained on those same features
is learning the CPCB rule, so a very high test score is expected and must not
be presented as validation against an independently observed outcome.
