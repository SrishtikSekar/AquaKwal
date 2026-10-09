#!/bin/bash
# ============================================================================
# AquaKwal: Quick Demo Script
# Verifies that each pipeline stage has produced output in HDFS.
# Run AFTER: ./scripts/run_pipeline.sh
# ============================================================================

set -Eeuo pipefail

echo "=========================================================="
echo "  AquaKwal DEMO — verifying pipeline stages"
echo "=========================================================="

# 0. Cluster health
echo "--- [0] Cluster status ---"
docker ps --format "table {{.Names}}\t{{.Status}}" | grep -E 'namenode|datanode|hive|spark'

# 1. HDFS raw data
echo "--- [1] HDFS raw data present? ---"
docker exec namenode hdfs dfs -ls /data/raw/ 2>/dev/null || echo "  (upload first: run ./scripts/run_pipeline.sh)"

# 2. Pig output
echo "--- [2] Pig ETL clean output? ---"
docker exec namenode hdfs dfs -ls /data/clean/ 2>/dev/null || echo "  (run Pig: docker exec namenode pig -x /pig-scripts/etl_clean.pig)"

# 3. Cleaned row count
echo "--- [3] Pig cleaned row count ---"
docker exec namenode hdfs dfs -cat /data/clean/water_quality_clean/part-* 2>/dev/null | wc -l

# 4. Hive summary
echo "--- [4] Hive reporting view: v_state_summary ---"
docker exec hive-server2 /opt/hive/bin/beeline \
  -u 'jdbc:hive2://localhost:10000/default' -n root --silent=true -e \
  "SELECT state_name, sample_count, poor_count, pct_poor FROM v_state_summary ORDER BY pct_poor DESC LIMIT 10" \
  2>/dev/null || echo "  (load Hive with scripts/run_pipeline.sh)"

# 5. Spark model metrics
echo "--- [5] Spark ML metrics ---"
docker exec namenode hdfs dfs -cat /data/output/ml_results/metrics/*.csv 2>/dev/null || echo "  (run Spark: see run_pipeline.sh)"

echo "=========================================================="
echo "  Demo complete."
echo "=========================================================="
