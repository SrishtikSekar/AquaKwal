#!/bin/bash
# ============================================================================
# AquaKwal: 30-second Demo Script
# File: scripts/demo.sh
# Shows that each pipeline stage has real, queryable output.
# ============================================================================

set -e

echo "=========================================================="
echo "  AquaKwal DEMO — verifying pipeline stages"
echo "=========================================================="

# 0. Cluster health
echo "--- [0] Cluster status ---"
docker ps --format "table {{.Names}}\t{{.Status}}" | grep -E 'namenode|datanode|hive|spark|pig'

# 1. HDFS raw data
echo "--- [1] HDFS raw data present? ---"
docker exec namenode hdfs dfs -ls /data/raw/ 2>/dev/null || echo "  (upload first: run ./scripts/run_pipeline.sh)"

# 2. Pig output
echo "--- [2] Pig ETL clean output? ---"
docker exec namenode hdfs dfs -ls /data/clean/ 2>/dev/null || echo "  (run Pig: docker exec pig pig -x /pig-scripts/etl_clean.pig)"

# 3. Hive summary
echo "--- [3] Hive reporting view sample ---"
docker exec hive-server2 hive -e "SELECT state, sample_count, avg_ph FROM v_state_summary ORDER BY sample_count DESC LIMIT 5" 2>/dev/null || echo "  (load Hive: docker exec -i hive-server2 hive -f /hive-scripts/warehouse.hql)"

# 4. Spark model metrics
echo "--- [4] Spark ML metrics ---"
docker exec namenode hdfs dfs -cat /data/output/ml_results/metrics/*.csv 2>/dev/null || echo "  (run Spark: see run_pipeline.sh)"

echo "=========================================================="
echo "  Demo complete."
echo "=========================================================="
