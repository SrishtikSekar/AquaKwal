#!/bin/bash
# ============================================================================
# AquaKwal bootstrap script for Hadoop NameNode container.
# Runs inside bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8
# 1. Installs Apache Pig 0.17.0 from a pre-downloaded tarball (or downloads)
# 2. Formats HDFS if needed
# 3. Starts NameNode daemon
# 4. Waits for DataNode registration
# 5. Creates HDFS directories
# 6. Keeps container alive
# ============================================================================

set -e

# ---------------------------------------------------------------------------
# Hadoop / Java environment
# ---------------------------------------------------------------------------
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HADOOP_HOME=/opt/hadoop-3.2.1
export HADOOP_PREFIX=/opt/hadoop-3.2.1
export HADOOP_CONF_DIR=/etc/hadoop
export PATH=$HADOOP_PREFIX/bin:$HADOOP_PREFIX/sbin:$PATH

# ---------------------------------------------------------------------------
# 1. INSTALL APACHE PIG 0.17.0
#    The tarball is mounted at /opt/pig-0.17.0.tar.gz (230MB binary dist).
#    If not mounted (e.g., dev mode without Docker), fall back to download.
# ---------------------------------------------------------------------------
if [ ! -d /opt/pig ]; then
  echo "[bootstrap] Installing Apache Pig 0.17.0..."

  if [ -f /opt/pig-0.17.0.tar.gz ]; then
    echo "  Using pre-downloaded tarball (mounted)."
    tar -xzf /opt/pig-0.17.0.tar.gz -C /opt/
  else
    echo "  Downloading from Apache archive..."
    curl -fsSL -o /tmp/pig-0.17.0.tar.gz \
      "https://archive.apache.org/dist/pig/pig-0.17.0/pig-0.17.0.tar.gz"
    tar -xzf /tmp/pig-0.17.0.tar.gz -C /opt/
    rm /tmp/pig-0.17.0.tar.gz
  fi

  ln -s /opt/pig-0.17.0 /opt/pig

  # Configure Pig to find Hadoop
  cat > /opt/pig/conf/pig-env.sh << 'PEOF'
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HADOOP_HOME=/opt/hadoop-3.2.1
export HADOOP_PREFIX=/opt/hadoop-3.2.1
export HADOOP_CONF_DIR=/etc/hadoop
export PIG_HOME=/opt/pig
export PIG_CLASSPATH=\$PIG_HOME/lib/*:\$PIG_HOME/*:\$HADOOP_PREFIX/share/hadoop/common/lib/*:\$HADOOP_PREFIX/share/hadoop/common/*:\$HADOOP_PREFIX/share/hadoop/hdfs/*:\$HADOOP_PREFIX/share/hadoop/hdfs/lib/*:\$HADOOP_PREFIX/share/hadoop/mapreduce/*:\$HADOOP_PREFIX/share/hadoop/mapreduce/lib/*:\$HADOOP_PREFIX/share/hadoop/yarn/*:\$HADOOP_PREFIX/share/hadoop/yarn/lib/*
PEOF

  echo "  Pig installed at /opt/pig"
fi

export PIG_HOME=/opt/pig
export PATH=$PIG_HOME/bin:$PATH

# Verify Pig
echo "[bootstrap] Verifying Pig installation:"
pig -x version 2>&1 | head -5 || echo "WARNING: pig version check failed"

# ---------------------------------------------------------------------------
# 2. START SSH (required by Hadoop scripts)
# ---------------------------------------------------------------------------
if [ -f /usr/sbin/sshd ]; then
  service ssh start
fi

# ---------------------------------------------------------------------------
# 3. FORMAT NAME NODE if not already formatted
#    The bde2020 image defaults to /hadoop/dfs/name
# ---------------------------------------------------------------------------
if [ ! -d /hadoop/dfs/name/current ]; then
  echo "[bootstrap] Formatting HDFS NameNode..."
  mkdir -p /hadoop/dfs/name /hadoop/dfs/data
  $HADOOP_PREFIX/bin/hdfs namenode -format -force
else
  echo "[bootstrap] NameNode already formatted."
fi

# ---------------------------------------------------------------------------
# 4. START NAMENODE DAEMON
# ---------------------------------------------------------------------------
echo "[bootstrap] Starting NameNode daemon..."
$HADOOP_PREFIX/sbin/hadoop-daemon.sh \
  --config $HADOOP_CONF_DIR --script hdfs start namenode

# ---------------------------------------------------------------------------
# 5. WAIT FOR DATANODE REGISTRATION
# ---------------------------------------------------------------------------
echo "[bootstrap] Waiting for DataNode to register..."
for i in $(seq 1 30); do
  live_dn=$($HADOOP_PREFIX/bin/hdfs dfsadmin -report 2>/dev/null \
    | grep "Live datanodes" | grep -oP '\d+')
  if [ "$live_dn" = "1" ] 2>/dev/null || [ "$live_dn" = "2" ] 2>/dev/null; then
    echo "  DataNode registered ($live_dn live)."
    break
  fi
  echo "  waiting... ($i)"
  sleep 3
done

# ---------------------------------------------------------------------------
# 6. CREATE HDFS DIRECTORIES
# ---------------------------------------------------------------------------
echo "[bootstrap] Creating HDFS directories..."
hdfs dfs -mkdir -p /data/raw
hdfs dfs -mkdir -p /data/clean
hdfs dfs -mkdir -p /data/output/ml_results
hdfs dfs -mkdir -p /user/hive/warehouse
hdfs dfs -chmod -R 777 /data
hdfs dfs -chmod -R 777 /user

echo "[bootstrap] HDFS is ready."
echo "[bootstrap] NameNode UI:  http://localhost:9870"
echo "[bootstrap] Pig ready:    docker exec namenode pig -x /pig-scripts/etl_clean.pig"

# Keep container alive
exec tail -f /dev/null
