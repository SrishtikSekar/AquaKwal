#!/bin/bash
# ============================================================================
# AquaKwal: End-to-End Pipeline Runner
# Runs the full Pig -> Hive -> Spark ML pipeline inside the Docker cluster.
# Assumes: docker compose up -d has been executed and services are healthy.
# ============================================================================

set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
DATA_DIR="$PROJECT_DIR/data"
RAW_CSV="$DATA_DIR/Indian_water_data.csv"
PIPE_INPUT="$DATA_DIR/Indian_water_data_pipe.csv"
THRESHOLDS="$DATA_DIR/parameter_thresholds.csv"

if [[ ! -f "$PIPE_INPUT" && -f "$RAW_CSV" ]]; then
  echo "[preflight] Creating pipe-delimited input from $(basename "$RAW_CSV")..."
  python3 "$DATA_DIR/convert_csv.py" "$RAW_CSV" "$PIPE_INPUT"
fi

for required_file in "$PIPE_INPUT" "$THRESHOLDS"; do
  if [[ ! -s "$required_file" ]]; then
    echo "ERROR: required input is missing or empty: $required_file" >&2
    exit 1
  fi
done

if ! head -n 1 "$THRESHOLDS" | grep -qx \
    'dissolved_min,ph_min,ph_max,bod_max,fecal_coliform_max'; then
  echo "ERROR: parameter_thresholds.csv has an unexpected schema" >&2
  exit 1
fi
if ! awk -F, 'NR > 1 && NF == 5 { rows++ } END { exit rows == 1 ? 0 : 1 }' \
    "$THRESHOLDS"; then
  echo "ERROR: parameter_thresholds.csv must contain exactly one five-value row" >&2
  exit 1
fi

echo "=========================================================="
echo "  AquaKwal Pipeline: Pig ETL -> Hive DDL -> Spark MLlib"
echo "  Dataset: Indian_water_data_pipe.csv"
echo "=========================================================="

# ---------------------------------------------------------------------------
# 0. Wait for HDFS to be ready (max 90 s)
# ---------------------------------------------------------------------------
echo "[0] Waiting for HDFS namenode + datanode..."
docker exec namenode bash -c '
  for i in $(seq 1 30); do
    report=$(hdfs dfsadmin -report 2>/dev/null)
    live=$(echo "$report" | grep "Live datanodes" | grep -oP "\d+")
    if [ "$live" = "1" ] 2>/dev/null || [ "$live" = "2" ] 2>/dev/null; then
      echo "HDFS is up (datanodes: $live)."
      exit 0
    fi
    echo "  waiting... ($i)"
    sleep 3
  done
  echo "ERROR: HDFS not ready"
  exit 1
'

# ---------------------------------------------------------------------------
# 1. Upload raw data to HDFS
# ---------------------------------------------------------------------------
echo "[1/4] Uploading raw data to HDFS..."
docker exec namenode hdfs dfs -rm -f -r /data/clean 2>/dev/null || true
docker exec namenode hdfs dfs -rm -f -r /data/output 2>/dev/null || true

docker exec namenode hdfs dfs -mkdir -p /data/raw
docker cp "$PIPE_INPUT" namenode:/tmp/iwd_pipe.csv
docker cp "$THRESHOLDS" namenode:/tmp/th.csv
docker exec namenode hdfs dfs -put -f /tmp/iwd_pipe.csv /data/raw/Indian_water_data_pipe.csv
docker exec namenode hdfs dfs -put -f /tmp/th.csv /data/raw/parameter_thresholds.csv
docker exec namenode hdfs dfs -chmod -R 777 /data
echo "  Raw data uploaded."
docker exec namenode hdfs dfs -ls /data/raw/

# ---------------------------------------------------------------------------
# 2. Run Pig ETL
# ---------------------------------------------------------------------------
echo "[2/4] Running Pig ETL (etl_clean.pig)..."
docker exec namenode pig /pig-scripts/etl_clean.pig
echo "  -- Stage 1 (clean) complete"
docker exec namenode pig /pig-scripts/sample_source_join.pig
echo "  -- Stage 2 (threshold enrichment) complete"
docker exec namenode hdfs dfs -ls /data/clean/

clean_count=$(docker exec namenode hdfs dfs -cat \
  '/data/clean/water_quality_clean/part-*' | wc -l)
enriched_count=$(docker exec namenode hdfs dfs -cat \
  '/data/clean/water_quality_enriched/part-*' | wc -l)
if [[ "$clean_count" -eq 0 || "$clean_count" -ne "$enriched_count" ]]; then
  echo "ERROR: invalid Pig row counts (clean=$clean_count, enriched=$enriched_count)" >&2
  exit 1
fi
clean_bad_fields=$(docker exec namenode bash -c \
  "hdfs dfs -cat '/data/clean/water_quality_clean/part-*' | awk -F'|' 'NF != 24 {bad++} END {print bad+0}'")
enriched_bad_fields=$(docker exec namenode bash -c \
  "hdfs dfs -cat '/data/clean/water_quality_enriched/part-*' | awk -F'|' 'NF != 25 {bad++} END {print bad+0}'")
if [[ "$clean_bad_fields" -ne 0 || "$enriched_bad_fields" -ne 0 ]]; then
  echo "ERROR: malformed Pig rows (clean=$clean_bad_fields, enriched=$enriched_bad_fields)" >&2
  exit 1
fi
echo "  Verified row flow: $clean_count clean -> $enriched_count enriched"

# ---------------------------------------------------------------------------
# 3. Initialize Hive metastore and load warehouse tables
# ---------------------------------------------------------------------------
echo "[3/4] Loading Hive tables (warehouse.hql)..."
docker exec hive-server2 bash -c '
  for i in $(seq 1 30); do
    if nc -z localhost 10000; then
      exit 0
    fi
    echo "  waiting for HiveServer2... ($i)"
    sleep 3
  done
  echo "ERROR: HiveServer2 not ready" >&2
  exit 1
'
docker exec hive-server2 /opt/hive/bin/beeline \
  -u 'jdbc:hive2://localhost:10000/default' -n root \
  -f /hive-scripts/warehouse.hql 2>&1 | tail -20
echo "  Hive tables loaded."
echo "  Running sample reporting query..."
docker exec hive-server2 /opt/hive/bin/beeline \
  -u 'jdbc:hive2://localhost:10000/default' -n root --silent=true -e "
  SELECT state_name, sample_count, poor_count, pct_poor
  FROM v_state_summary ORDER BY pct_poor DESC LIMIT 10;
" 2>/dev/null

# ---------------------------------------------------------------------------
# 4. Run Spark MLlib pipeline
# ---------------------------------------------------------------------------
echo "[4/4] Running Spark MLlib classifier..."
docker exec spark-master /spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.hadoop.fs.defaultFS=hdfs://namenode:8020 \
  --conf spark.hadoop.dfs.replication=1 \
  --conf spark.sql.warehouse.dir=/user/hive/warehouse \
  --conf spark.driver.host=spark-master \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --executor-memory 2G \
  --driver-memory 2G \
  /spark-scripts/ml_quality.py 2>&1 | tail -60

echo "=========================================================="
echo "  Pipeline complete. Results in HDFS: /data/output/ml_results"
echo "=========================================================="
docker exec namenode hdfs dfs -ls -R /data/output/ml_results/
