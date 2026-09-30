#!/bin/bash
# ============================================================================
# AquaKwal: End-to-End Pipeline Runner
# Runs the full Pig -> Hive -> Spark ML pipeline inside the Docker cluster.
# Assumes: docker compose up -d has been executed and services are healthy.
# ============================================================================

set -e

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
docker cp data/Indian_water_data_pipe.csv namenode:/tmp/iwd_pipe.csv
docker cp data/parameter_thresholds.csv namenode:/tmp/th.csv
docker exec namenode hdfs dfs -put /tmp/iwd_pipe.csv /data/raw/Indian_water_data_pipe.csv
docker exec namenode hdfs dfs -put /tmp/th.csv /data/raw/parameter_thresholds.csv
docker exec namenode hdfs dfs -chmod -R 777 /data
echo "  Raw data uploaded."
docker exec namenode hdfs dfs -ls /data/raw/

# ---------------------------------------------------------------------------
# 2. Run Pig ETL
# ---------------------------------------------------------------------------
echo "[2/4] Running Pig ETL (etl_clean.pig)..."
docker exec namenode pig /pig-scripts/etl_clean.pig
echo "  -- Stage 1 (clean) complete"
docker exec namenode hdfs dfs -ls /data/clean/

# ---------------------------------------------------------------------------
# 3. Initialize Hive metastore and load warehouse tables
# ---------------------------------------------------------------------------
echo "[3/4] Loading Hive tables (warehouse.hql)..."
docker exec -i hive-server2 hive -f /hive-scripts/warehouse.hql 2>&1 | tail -20 || true
echo "  Hive tables loaded."
echo "  Running sample reporting query..."
docker exec hive-server2 hive -e "
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