# Operations Guide — AquaKwal Pipeline

> **Legacy draft:** this file still describes the retired Kaggle schema. Use
> the root `README.md` for current CPCB run instructions and `docs/testing.md`
> for verified commands/results.

Detailed instructions for environment setup, running each stage, and verifying
that data flows correctly between tools.

---

## 1. Prerequisites Check

### 1.1 Docker & Docker Compose

```bash
docker version          # Engine
docker compose version  # Compose v2 plugin
```

If Compose is missing:
```bash
sudo apt-get install docker-compose-v2   # Debian/Ubuntu
```

All commands use `docker compose` (v2 syntax).

### 1.2 System Resources

| Requirement | Minimum | Recommended |
|-------------|---------|-------------|
| RAM | 6 GB | 8 GB |
| CPU cores | 2 | 4 |
| Disk free | 5 GB | 10 GB |

```bash
free -h        # memory
df -h /        # disk
nproc          # CPU cores
```

### 1.3 Port Availability

The cluster exposes these ports. Verify none are in use:

```bash
for port in 9870 50070 9000 10000 8080 7077; do
  if ss -tlnp | grep -q ":$port "; then
    echo "WARNING: Port $port is in use"
  fi
done
```

### 1.4 Data Files Present

```bash
ls -la data/
# Expected:
#   water_potability.csv       (~525 KB, 3277 lines)
#   parameter_thresholds.csv   (~80 bytes, 2 lines)
```

Verify the dataset:
```bash
wc -l data/water_potability.csv                # expect 3277 (header + 3276)
head -3 data/water_potability.csv             # header + first 2 data rows
cat data/parameter_thresholds.csv             # WHO thresholds reference
```

---

## 2. Step-by-Step Setup

### 2.1 Start the Docker Cluster

```bash
docker compose up -d
```

First run pulls ~3 GB of Docker images. Subsequent starts are instant.

### 2.2 Monitor Startup

```bash
# Watch container status (containers start "up" before they're ready)
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

# Follow namenode logs to see HDFS format + daemon startup
docker compose logs -f namenode

# Check HDFS web UI in browser:
# http://localhost:9870  → "Datanodes Available: 1"
```

### 2.3 Verify HDFS Health

```bash
docker exec namenode hdfs dfsadmin -report

# Expected output includes:
#   Live datanodes: 1
#   Configured Capacity: > 0
#   DFS Remaining:    > 0
```

---

## 3. Running the Pipeline

### 3.1 Full Automated Run

```bash
./scripts/run_pipeline.sh
```

**What this does:**
1. Waits for HDFS to be healthy
2. Uploads `water_potability.csv` + `parameter_thresholds.csv` to `/data/raw/`
3. Runs `etl_clean.pig` (clean + impute + dedup)
4. Runs `sample_source_join.pig` (CROSS with thresholds → violation count)
5. Loads `warehouse.hql` into Hive (tables + views)
6. Runs a Hive summary query
7. Submits the Spark ML job

**Expected final lines (console):**
```
AquaKwal Pipeline complete. Results in HDFS: /data/output/ml_results
```

### 3.2 Stage-by-Stage (Manual / Debug Mode)

Use this when debugging a specific stage.

#### Stage 1: Upload Raw Data to HDFS

```bash
# Clean previous runs (optional)
docker exec namenode hdfs dfs -rm -f -r /data/raw /data/clean /data/output 2>/dev/null

# Create directories and upload
docker exec namenode hdfs dfs -mkdir -p /data/raw
docker cp data/water_potability.csv namenode:/tmp/wp.csv
docker cp data/parameter_thresholds.csv namenode:/tmp/th.csv
docker exec namenode hdfs dfs -put /tmp/wp.csv /data/raw/water_potability.csv
docker exec namenode hdfs dfs -put /tmp/th.csv /data/raw/parameter_thresholds.csv
docker exec namenode hdfs dfs -chmod -R 777 /data

# Verify
docker exec namenode hdfs dfs -ls -l /data/raw/
# Expected:
#   -rw-r--r--  ... /data/raw/parameter_thresholds.csv
#   -rw-r--r--  ... /data/raw/water_potability.csv
```

#### Stage 2: Run Pig ETL

```bash
# Stage 2a: Clean + impute + dedup
docker exec pig pig -x /pig-scripts/etl_clean.pig
echo "Exit code: $?"

# Stage 2b: Join with thresholds
docker exec pig pig -x /pig-scripts/sample_source_join.pig
echo "Exit code: $?"

# Verify Pig output
docker exec namenode hdfs dfs -ls -l /data/clean/
# Expected: water_quality_clean/ and water_quality_enriched/ directories

# Count cleaned rows
docker exec namenode hdfs dfs -cat /data/clean/water_quality_clean/part-* | wc -l
# Expected: ~3,240 or fewer (originals minus header and filtered)

# Spot-check first 3 rows
docker exec namenode hdfs dfs -cat /data/clean/water_quality_enriched/part-* | head -3
# Should show 12 columns: ph, hardness, solids, ..., violation_count
```

**Verify cleaning worked:**
```bash
# No "NA" strings remain in cleaned output
docker exec namenode hdfs dfs -cat /data/clean/water_quality_clean/part-* | grep -c "NA"
# Expected: 0

# No empty fields (consecutive commas)
docker exec namenode hdfs dfs -cat /data/clean/water_quality_clean/part-* | grep -c ",,"
# Expected: 0

# Labels are only POTABLE or NOT_POTABLE
docker exec namenode hdfs dfs -cat /data/clean/water_quality_clean/part-* | tail -1 | grep -o "POTABLE\|NOT_POTABLE"
```

#### Stage 3: Load Hive Tables

```bash
# Load all DDL + views
docker exec -i hive-server2 hive -f /hive-scripts/warehouse.hql

# Verify tables exist
docker exec -i hive-server2 hive -e "SHOW TABLES;"
# Expected:
#   water_quality_clean
#   water_quality_enriched

# Run a reporting query
docker exec hive-server2 hive -e "SELECT * FROM v_potability_summary;"
# Expected: 2 rows (POTABLE, NOT_POTABLE) with avg metrics

# Run a validation query
docker exec hive-server2 hive -e "
  SELECT 'rows_clean' AS metric, COUNT(*) AS value FROM water_quality_clean;
  SELECT 'rows_enriched' AS metric, COUNT(*) AS value FROM water_quality_enriched;
  SELECT 'class_balance' AS metric, water_quality_label, COUNT(*) AS value
  FROM water_quality_enriched GROUP BY water_quality_label;
"
```

#### Stage 4: Run Spark MLlib

```bash
# Submit the Spark job to the Spark cluster
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 \
  --conf spark.sql.warehouse.dir=/user/hive/warehouse \
  --executor-memory 2G \
  --driver-memory 2G \
  /spark-scripts/ml_quality.py
```

**Watch the progress:**
- Spark Master UI: `http://localhost:8080`
- Spark application UI: `http://localhost:4040` (during job execution)

**Verify Spark output:**
```bash
# Check output directory
docker exec namenode hdfs dfs -ls -R /data/output/ml_results/
# Expected:
#   /data/output/ml_results/predictions/   (Parquet)
#   /data/output/ml_results/metrics/       (CSV)
#   /data/output/ml_results/feature_importances/  (CSV)

# Read metrics
docker exec namenode hdfs dfs -cat /data/output/ml_results/metrics/*.csv
# Expected:
#   metric,value
#   AUC,0.78xx
#   F1,0.72xx
#   Accuracy,0.74xx

# Read feature importances (sorted by importance)
docker exec namenode hdfs dfs -cat /data/output/ml_results/feature_importances/*.csv | sort -t, -k2 -rn
# Expected: ph, sulfate, solids, turbidity, ... (top 5 listed)
```

---

## 4. Verification Checklist

After a successful `run_pipeline.sh`, confirm each of these:

| # | Check | Command | Expected |
|---|-------|---------|----------|
| 1 | Raw data in HDFS | `hdfs dfs -ls /data/raw/` | 2 files |
| 2 | Clean data exists | `hdfs dfs -ls /data/clean/` | 2 dirs |
| 3 | Clean rows | `hdfs dfs -cat /data/clean/water_quality_clean/part-* \| wc -l` | ~3,240 |
| 4 | No NA in clean | `hdfs dfs -cat /data/clean/... \| grep -c NA` | 0 |
| 5 | No empty fields | `hdfs dfs -cat /data/clean/... \| grep -c ",,"` | 0 |
| 6 | Hive tables | `hive -e "SHOW TABLES"` | 2 tables |
| 7 | Hive view works | `hive -e "SELECT * FROM v_potability_summary"` | 2 rows |
| 8 | Spark output | `hdfs dfs -ls /data/output/ml_results/` | 3 dirs |
| 9 | Metrics file | `hdfs dfs -cat /data/output/ml_results/metrics/*.csv` | AUC, F1, Accuracy |
| 10 | Feature importances | `hdfs dfs -cat .../feature_importances/*.csv` | 10 features |

---

## 5. Quick One-Command Demo

```bash
# Assumes cluster is already running and pipeline has been executed
./scripts/demo.sh
```

**Expected:**
```
--- [0] Cluster status ---
  namenode    Up  ...
  datanode    Up  ...
  ...
--- [1] HDFS raw data present? ---
  Found 2 items
--- [2] Pig ETL clean output? ---
  Found 2 items
--- [3] Pig cleaned row count ---
  3240
--- [4] Hive reporting view: v_potability_summary ---
  NOT_POTABLE  1998  6.8  180.2  3.4
  POTABLE      1278  7.2  195.1  2.1
--- [5] Spark ML metrics ---
  metric,value
  AUC,0.78xx
  F1,0.72xx
  Accuracy,0.74xx
```

---

## 6. Stopping & Cleanup

### 6.1 Stop the cluster (keeps HDFS data)

```bash
docker compose stop

# Restart
docker compose up -d
```

### 6.2 Full reset (removes all data)

```bash
# Stop everything and remove volumes
docker compose down -v

# Remove HDFS data directories from the host
rm -rf data/output

# Restart from scratch
docker compose up -d
./scripts/run_pipeline.sh
```

---

## 7. Troubleshooting Reference

### 7.1 Cluster won't start / containers crash-looping

```bash
docker compose ps    # see which containers are failing
docker compose logs namenode    # read logs of failing service
docker compose logs datanode    # especially check datanode
docker compose logs hive-server2
docker compose logs spark-master

# Common fix: insufficient memory — allocate 8+ GB to Docker
# Docker Desktop: Settings → Resources → Memory: 8 GB
```

### 7.2 `namenode` health check fails

```bash
docker compose restart namenode
docker compose logs namenode
# Check for "Formatting NameNode" then "Started NameNode"
```

### 7.3 Pig: "Unable to connect to the server"

```bash
# Verify HDFS is up
docker exec namenode hdfs dfsadmin -report | head -5

# Test HDFS from Pig container
docker exec pig hdfs dfs -ls /data/raw/
# If this fails, check CORE_CONF_fs_defaultFS in docker-compose.yml

# Verify Pig script is accessible
docker exec pig ls -la /pig-scripts/
```

### 7.4 Pig: regex MATCHES syntax error

```bash
# The MATCHES operator requires regex in single quotes
# Check: pig/etl_clean.pig uses: (field MATCHES '[-0-9.]+' ? (float)field : NULL)

# Test with a tiny file
docker exec pig pig -x -4 /dev/stdin <<'EOF'
test = LOAD '/data/raw/water_potability.csv' USING PigStorage(',') AS (ph:chararray);
filtered = FILTER test BY ph MATCHES '[0-9.]+';
DUMP filtered;
EOF
```

### 7.5 Hive: "Table not found" or empty results

```bash
# 1. Check Hive metastore is running
docker exec hive-server2 ps aux | grep hive

# 2. Check the script path
docker exec hive-server2 ls -la /hive-scripts/warehouse.hql

# 3. Hive loads the script and then queries the tables. If the tables
#    show up in SHOW TABLES but queries return 0 rows:
#    a) Check that HDFS output exists:
docker exec namenode hdfs dfs -ls /data/clean/
#    b) Re-run Pig if HDFS output is missing

# 4. Run the HQL with verbose output
docker exec -i hive-server2 hive -f /hive-scripts/warehouse.hql 2>&1 | tail -30
```

### 7.6 Spark: "UnknownHostException: namenode"

```bash
# Spark can't resolve the HDFS namenode. Check config:
docker exec spark-master env | grep fs.defaultFS

# In docker-compose.yml, ensure spark-master has:
#   - SPARK_MODE=master
#   - CORE_CONF_fs_defaultFS=hdfs://namenode:9000

# Test HDFS access from Spark:
docker exec spark-master bash -c 'hdfs dfs -ls /data/raw/' 2>&1 || \
  echo "HDFS unreachable — check spark-master volume mounts"
```

### 7.7 Spark: OutOfMemoryError during cross-validation

```bash
# Edit run_pipeline.sh to add more memory:
# --executor-memory 4G --driver-memory 4G

# Or reduce the cross-validation grid:
# Edit spark/ml_quality.py → ParamGridBuilder → remove one grid option
```

### 7.8 Spark: ClassNotFoundException for Hive/Hadoop configs

```bash
# The Spark image (bitnami/spark) should include Hadoop client JARs.
# If Hive integration fails, add the hive-exec JAR:
docker exec spark-master ls /opt/spark/jars/

# Ensure the HDFS classpath is set:
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 \
  --conf spark.sql.warehouse.dir=/user/hive/warehouse \
  ...
```

### 7.9 Data looks wrong / columns misaligned after Pig

```bash
# The CSV has a header row. Verify Pig stripped it:
docker exec namenode hdfs dfs -cat /data/clean/water_quality_clean/part-* | head -1
# Should show numeric data, NOT the header string

# If header leaks through, check the FILTER:
# In etl_clean.pig: FILTER raw_data BY ph != 'ph'
```

---

## 8. Understanding the Output

### Pipeline Output Locations in HDFS

```
/data/raw/                          # Stage 1 input
  ├── water_potability.csv          # 3,276 rows, 10 cols
  └── parameter_thresholds.csv       # WHO thresholds (1 data row)
/data/clean/                        # Stage 2 output (Pig)
  ├── water_quality_clean/          # 9 features + label + potability
  └── water_quality_enriched/        # + standards_violation_count
/data/output/ml_results/            # Stage 4 output (Spark)
  ├── predictions/                  # Parquet: test predictions
  ├── metrics/                       # CSV: AUC, F1, Accuracy
  └── feature_importances/          # CSV: per-feature score
```

---

## 9. Replacing the Dataset

To use your own water quality data:

1. Replace `data/water_potability.csv` with your CSV (same 10-column schema).
2. Update `data/parameter_thresholds.csv` if your parameters differ.
3. Re-run `./scripts/run_pipeline.sh`.
