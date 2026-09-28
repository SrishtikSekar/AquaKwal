#!/bin/bash
# ============================================================================
# AquaKwal End-to-End Pipeline Runner
# Runs the full Pig -> Hive -> Spark ML pipeline inside the Docker cluster.
# Assumes: docker-compose up -d has been executed and services are healthy.
# ============================================================================

set -e

echo "=========================================================="
echo "  AquaKwal Pipeline: Pig ETL -> Hive DDL -> Spark MLlib"
echo "=========================================================="

# ---------------------------------------------------------------------------
# 0. Wait for HDFS to be ready (max 60 s)
# ---------------------------------------------------------------------------
echo "[0] Waiting for HDFS namenode..."
docker exec namenode bash -c '
  for i in $(seq 1 30); do
    if hdfs dfsadmin -report 2>/dev/null | grep -q "Live datanodes"; then
      echo "HDFS is up."
      exit 0
    fi
    echo "  waiting... ($i)"
    sleep 2
  done
  echo "ERROR: HDFS not ready"
  exit 1
'

# ---------------------------------------------------------------------------
# 1. Upload raw data to HDFS
# ---------------------------------------------------------------------------
echo "[1/4] Uploading raw data to HDFS..."
docker exec namenode hdfs dfs -rm -f -r /data/raw 2>/dev/null || true
docker exec namenode hdfs dfs -mkdir -p /data/raw
docker cp data/water_quality_samples.csv namenode:/tmp/upload.csv
docker exec namenode hdfs dfs -put /tmp/upload.csv /data/raw/water_quality_samples.csv
docker cp data/site_metadata.csv namenode:/tmp/upload_meta.csv
docker exec namenode hdfs dfs -put /tmp/upload_meta.csv /data/raw/site_metadata.csv
docker exec namenode hdfs dfs -chmod -R 777 /data
echo "  Raw data uploaded."
docker exec namenode hdfs dfs -ls /data/raw/

# ---------------------------------------------------------------------------
# 2. Run Pig ETL
# ---------------------------------------------------------------------------
echo "[2/4] Running Pig ETL (etl_clean.pig + sample_source_join.pig)..."
docker exec pig pig -x -4 /dev/stdin <<'PIGSCRIPT'
RUN /pig-scripts/etl_clean.pig
RUN /pig-scripts/sample_source_join.pig
PIGSCRIPT
echo "  Pig ETL complete."
docker exec namenode hdfs dfs -ls /data/clean/

# ---------------------------------------------------------------------------
# 3. Load Hive warehouse tables and run validation queries
# ---------------------------------------------------------------------------
echo "[3/4] Loading Hive tables (warehouse.hql)..."
docker exec -i hive-server2 hive -f /hive-scripts/warehouse.hql 2>&1 | tail -20 || true
echo "  Hive tables loaded."
echo "  Running sample validation query..."
docker exec hive-server2 hive -e "
  SELECT state, COUNT(*) AS cnt, ROUND(AVG(ph),2) AS avg_ph FROM water_quality_enriched GROUP BY state ORDER BY cnt DESC LIMIT 10;
" 2>/dev/null

# ---------------------------------------------------------------------------
# 4. Run Spark MLlib pipeline
# ---------------------------------------------------------------------------
echo "[4/4] Running Spark MLlib classifier..."
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --jars /opt/spark/jars/spark-hive_2.12-3.5.0.jar \
  --conf spark.sql.warehouse.dir=/user/hive/warehouse \
  --conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 \
  --deploy-mode client \
  /data/ml_quality.py 2>&1 | tail -50

echo "=========================================================="
echo "  Pipeline complete. Results in HDFS: /data/output/ml_results"
echo "=========================================================="
docker exec namenode hdfs dfs -ls -R /data/output/ml_results/
