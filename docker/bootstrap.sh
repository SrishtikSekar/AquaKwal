#!/bin/bash
# ============================================================================
# AquaKwal bootstrap script for Hadoop NameNode container.
# Initializes HDFS if needed, starts SSH + Hadoop daemons, and waits for
# readiness before returning.  Designed to run in the bde2020/hadoop-namenode
# image which expects /bootstrap.sh.
# ============================================================================

set -e

# Start SSH (required by Hadoop scripts)
if [ ! -f /usr/sbin/sshd ]; then
  echo "[bootstrap] sshd not found; skipping"
else
  service ssh start
fi

# Initialize HDFS namenode directory if not present
if [ ! -d /tmp/hadoop-root/name ]; then
  echo "[bootstrap] Formatting HDFS NameNode..."
  mkdir -p /tmp/hadoop-root/name /tmp/hadoop-root/data
  $HADOOP_PREFIX/bin/hdfs namenode -format -force
fi

# Start Hadoop daemons
echo "[bootstrap] Starting Hadoop daemons..."
$HADOOP_PREFIX/sbin/hadoop-daemon.sh --config $HADOOP_CONF_DIR --script hdfs start namenode
$HADOOP_PREFIX/sbin/hadoop-daemon.sh --config $HADOOP_CONF_DIR --script hdfs start datanode

# Wait for HDFS to be ready
echo "[bootstrap] Waiting for HDFS to be ready..."
until $HADOOP_PREFIX/bin/hdfs dfsadmin -report 2>/dev/null | grep "Live datanodes" | grep -q "1"; do
  sleep 2
  echo "  still waiting..."
done

# Create required HDFS directories with permissions disabled
echo "[bootstrap] Creating HDFS directories..."
hdfs dfs -mkdir -p /data/raw
hdfs dfs -mkdir -p /data/clean
hdfs dfs -mkdir -p /data/output/ml_results
hdfs dfs -mkdir -p /user/hive/warehouse
hdfs dfs -chmod -R 777 /data
hdfs dfs -chmod -R 777 /user

echo "[bootstrap] HDFS is ready."
echo "[bootstrap] Namenode web UI: http://localhost:9870"

# Keep container alive
exec tail -f /dev/null
