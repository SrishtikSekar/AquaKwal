# Operations Guide — AquaKwal Pipeline

Detailed instructions for environment setup, running each stage, and verifying
that data flows correctly between tools.

---

## 1. Prerequisites Check

Before starting, verify these conditions:

### 1.1 Docker & Docker Compose

```bash
# Docker engine
docker version
docker info

# Docker Compose v2 plugin (NOT the python v1 package)
docker compose version

# If compose is missing:
# sudo apt-get install docker-compose-v2   # Debian/Ubuntu
# or install via Docker Desktop
```

All commands below use `docker compose` (v2 syntax). If you have only v1,
replace `docker compose` with `docker-compose`.

### 1.2 System Resources

| Requirement | Minimum | Recommended |
|-------------|---------|-------------|
| RAM | 8 GB | 12 GB |
| CPU cores | 4 | 6 |
| Disk free | 5 GB | 10 GB |

Check:
```bash
free -h        # memory
df -h /        # disk
nproc          # CPU cores
```

### 1.3 Port Availability

The cluster exposes these ports. Verify none are in use:

```bash
for port in 9870 50070 9000 10000 10002 8080 7077 4040; do
  if ss -tlnp | grep -q ":$port "; then
    echo "WARNING: Port $port is in use"
  fi
done
```

If a port is occupied, either stop the conflicting process or edit
`docker/docker-compose.yml` to remap that port.

### 1.4 Python (for data generation)

```bash
python3 --version  # must be 3.8+
python3 -c "import csv, random; print('stdlib OK')"
```

The data generator uses only Python standard library — no `pip install` needed.

---

## 2. Step-by-Step Setup

### 2.1 Clone / Navigate

```bash
cd /path/to/AquaKwal    # this project root
```

### 2.2 Generate the Dataset

```bash
cd data
python3 generate_data.py
```

**Expected output:**
```
Generating synthetic water quality dataset...
  Samples: 10500 (with ~5% duplicates & noise)
  Stations: 50
  Written to: data/water_quality_samples.csv
  Metadata: data/site_metadata.csv
Done.
```

**Quick-verify the generated files:**
```bash
wc -l data/water_quality_samples.csv   # expect ~10501 (header + 10500 rows)
wc -l data/site_metadata.csv          # expect 51 (header + 50 stations)
head -3 data/water_quality_samples.csv
head -3 data/site_metadata.csv
```

**Verify noise is present** (proves Pig has something to clean):
```bash
# Count rows with missing values (empty fields)
grep -c ",," data/water_quality_samples.csv    # expect > 0

# Count exact duplicate rows
sort data/water_quality_samples.csv | uniq -d | wc -l  # expect > 0

# Check for "NA" type errors
grep -c "NA" data/water_quality_samples.csv     # expect > 0
```

### 2.3 Start the Docker Cluster

```bash
docker compose up -d
```

This pulls ~4 GB of Docker images on first run. Subsequent starts are instant.

**Monitor startup:**
```bash
# Watch container status
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

# Or watch logs in real-time
docker compose logs -f namenode
```

**Wait for namenode to be healthy** (~30–60 seconds):
```bash
until docker compose ps --format "{{.State}}" namenode | grep -q "running"; do
  echo "waiting for namenode..." && sleep 3
done
echo "namenode is up"
```

You can also check the HDFS web UI at `http://localhost:9870` — you should
see **1 live datanode** under "Datanodes Available".

### 2.4 Verify Cluster Health

```bash
# HDFS cluster summary
docker exec namenode hdfs dfsadmin -report

# Expected:
#   1 datanodes available
#   Configured Capacity > 0

# Spark Master UI
curl -s http://localhost:8080 | grep "Workers"
# Should show worker count >= 1
```

---

## 3. Running the Pipeline

### 3.1 Full Automated Run

```bash
./scripts/run_pipeline.sh
```

This script:
1. Waits for HDFS readiness
2. Uploads raw CSVs to `/data/raw/` in HDFS
3. Runs both Pig scripts
4. Loads Hive DDL and runs a sample Hive query
5. Submits the Spark ML job

**Expected final output (last 5–10 lines):**
```
HDFS is ready...
Raw data uploaded.
Pig ETL complete.
Hive tables loaded.
  AUC (areaUnderROC) on test set: ~0.87
  F1 score on test set: ~0.83
  Accuracy on test set: ~0.84
Pipeline complete. Results in HDFS: /data/output/ml_results
```

### 3.2 Stage-by-Stage (Manual Mode)

Use this when debugging or demonstrating each stage independently.

#### Stage 1: Upload Raw Data to HDFS

```bash
# Clean slate (optional — removes previous runs)
docker exec namenode hdfs dfs -rm -f -r /data/raw /data/clean /data/output 2>/dev/null

# Create directories
docker exec namenode hdfs dfs -mkdir -p /data/raw

# Upload using docker cp + hdfs put (since containers are isolated)
docker cp data/water_quality_samples.csv namenode:/tmp/wqs.csv
docker cp data/site_metadata.csv namenode:/tmp/meta.csv
docker exec namenode hdfs dfs -put /tmp/wqs.csv /data/raw/water_quality_samples.csv
docker exec namenode hdfs dfs -put /tmp/meta.csv /data/raw/site_metadata.csv

# Make readable
docker exec namenode hdfs dfs -chmod -R 777 /data/raw
```

**Verify upload:**
```bash
docker exec namenode hdfs dfs -ls -l /data/raw/
# Expected:
#   /data/raw/site_metadata.csv      (~1.5 KB)
#   /data/raw/water_quality_samples.csv  (~300 KB)
```

#### Stage 2: Run Pig ETL

```bash
# Run the main ETL script
docker exec -i pig pig -x /pig-scripts/etl_clean.pig

# Run the enrichment join
docker exec -i pig pig -x /pig-scripts/sample_source_join.pig
```

> **Note:** The `Pig` container mounts `./pig` as `/pig-scripts`. The scripts
> use absolute HDFS paths, so no argument passing is needed.

**Verify Pig output:**
```bash
# Check cleaned data exists
docker exec namenode hdfs dfs -ls -l /data/clean/
# Expected: water_quality_clean and water_quality_enriched directories

# Count rows in cleaned output
docker exec namenode hdfs dfs -cat /data/clean/water_quality_clean/part-* | wc -l
# Expect < 10500 (some rows filtered for impossible values)

# Spot-check first few cleaned rows
docker exec namenode hdfs dfs -cat /data/clean/water_quality_enriched/part-* | head -5
```

**Check that cleaning worked:**
```bash
# No "NA" or empty strings in cleaned output
docker exec namenode hdfs dfs -cat /data/clean/water_quality_clean/part-* | grep -c "NA"
# Expect: 0

# water_quality_label should be SAFE or UNSAFE
docker exec namenode hdfs dfs -cat /data/clean/water_quality_clean/part-* | tail -1 | grep -o "SAFE\|UNSAFE"
```

#### Stage 3: Load Hive Tables

```bash
# Load all DDL + views
docker exec -i hive-server2 hive -f /hive-scripts/warehouse.hql

# Verify tables exist
docker exec -i hive-server2 hive -e "SHOW TABLES;"
# Expected: water_quality_clean, water_quality_enriched

# Run a reporting query
docker exec -i hive-server2 hive -e "
  SELECT state, sample_count, avg_ph, pct_unsafe
  FROM v_state_summary
  ORDER BY sample_count DESC
  LIMIT 10;
"
# Expected: table of 10 states with metrics

# Run validation queries
docker exec -i hive-server2 hive -e "
  SELECT 'clean_rows' AS metric, COUNT(*) AS value FROM water_quality_clean;
  SELECT 'enriched_rows' AS metric, COUNT(*) AS value FROM water_quality_enriched;
"
```

**Check data integrity via Hive:**
```bash
docker exec -i hive-server2 hive -e "
  SELECT
    'null_ph' AS metric,
    CAST(SUM(CASE WHEN ph IS NULL THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*) * 100 AS pct
  FROM water_quality_enriched;

  SELECT 'unsafe_ratio' AS metric,
    ROUND(AVG(CASE WHEN water_quality_label = 'UNSAFE' THEN 1.0 ELSE 0.0 END), 4) AS value
  FROM water_quality_enriched;
"
```

#### Stage 4: Run Spark MLlib

```bash
# The Spark container must be able to reach HDFS at namenode:9000
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 \
  --conf spark.sql.warehouse.dir=/user/hive/warehouse \
  spark/ml_quality.py 2>&1 | tee /tmp/spark_output.log
```

> **Note:** If running outside Docker, replace `spark-master:7077` with
> `spark://localhost:7077` and add `--conf spark.driver.host=$(hostname -I | awk '{print $1}')`
> for network visibility.

**Verify Spark output:**
```bash
# Check output directory exists
docker exec namenode hdfs dfs -ls -R /data/output/ml_results/

# Read metrics
docker exec namenode hdfs dfs -cat /data/output/ml_results/metrics/*.csv

# Read feature importances
docker exec namenode hdfs dfs -cat /data/output/ml_results/feature_importances/*.csv | sort -t, -k2 -rn

# Sample predictions (first 10)
docker exec namenode hdfs dfs -cat /data/output/ml_results/predictions/*.parquet 2>/dev/null || \
  docker exec spark-master spark-submit --master spark://spark-master:7077 -e "
    SELECT site_id, ph, dissolved_oxygen, prediction, probability
    FROM parquet.'/data/output/ml_results/predictions'
    LIMIT 10;"
```

---

## 4. Verification Checklist

After a successful run, confirm each of these:

| # | Check | Command | Expected |
|---|-------|---------|----------|
| 1 | Raw data in HDFS | `hdfs dfs -ls /data/raw/` | 2 files |
| 2 | Clean data exists | `hdfs dfs -ls /data/clean/` | 2 dirs |
| 3 | Clean row count | `hdfs dfs -cat /data/clean/water_quality_clean/part-* \| wc -l` | ~9400–9500 |
| 4 | No NA in clean | `hdfs dfs -cat /data/clean/... \| grep -c NA` | 0 |
| 5 | Hive tables loaded | `hive -e "SHOW TABLES;"` | 2 tables |
| 6 | Hive view works | `hive -e "SELECT * FROM v_state_summary LIMIT 5;"` | 5 rows |
| 7 | Spark output exists | `hdfs dfs -ls /data/output/ml_results/` | 3 dirs |
| 8 | Metrics file | `hdfs dfs -cat /data/output/ml_results/metrics/*.csv` | AUC, F1, Accuracy |
| 9 | Feature importances | `hdfs dfs -cat /data/output/.../feature_importances/*.csv` | 10 features sorted |

---

## 5. Quick One-Command Demo

```bash
# Assumes cluster is already running from a previous `docker compose up -d`
./scripts/demo.sh
```

Expected:
```
--- [0] Cluster status ---
  namenode    Up ...
  datanode    Up ...
  ...
--- [1] HDFS raw data present? ---
  Found 2 items
  -rw-r--r--   3 root supergroup  ... /data/raw/site_metadata.csv
  -rw-r--r--   3 root supergroup  ... /data/raw/water_quality_samples.csv
--- [2] Pig ETL clean output? ---
  Found 2 items
  drwxrwxrwx   - ... /data/clean/water_quality_clean
  drwxrwxrwx   - ... /data/clean/water_quality_enriched
--- [3] Hive reporting view sample ---
  ...state | sample_count | avg_ph | pct_unsafe...
--- [4] Spark ML metrics ---
  AUC,0.87
  F1,0.83
  Accuracy,0.84
```

---

## 6. Stopping & Cleanup

### 6.1 Stop the cluster (keeps data)

```bash
docker compose stop
```

Containers stop but HDFS data (in Docker volumes) persists. Restart with:
```bash
docker compose up -d
```

### 6.2 Full reset (removes all data)

```bash
# Stop everything
docker compose down -v    # -v removes anonymous volumes

# Remove generated dataset (regenerate with data/generate_data.py)
rm data/water_quality_samples.csv data/site_metadata.csv

# Remove output directories from any leftover HDFS
rm -rf data/output

# Restart from scratch
python3 data/generate_data.py
docker compose up -d
./scripts/run_pipeline.sh
```

---

## 7. Troubleshooting Reference

### 7.1 Cluster won't start / containers crash-looping

```bash
# Check which containers are failing
docker compose ps

# Read the logs of the failing service
docker compose logs namenode
docker compose logs datanode
docker compose logs hive-server2

# Common fix: insufficient memory — allocate more Docker RAM
# Docker Desktop: Settings → Resources → Memory: 8 GB+
# Linux: the containers use the host kernel, so ensure free memory
```

### 7.2 `namenode` health check fails

```bash
# Force-restart the namenode
docker compose restart namenode

# Then check if it formats properly
docker compose logs namenode | grep "Starting DataNode"
```

### 7.3 Pig: "Unable to connect to the server"

```bash
# Verify HDFS is up and reachable
docker exec namenode hdfs dfsadmin -report | head -5

# Verify the Pig container sees namenode
docker exec pig bash -c 'hdfs dfs -ls /data/raw/'
# If this fails, ensure CORE_CONF_fs_defaultFS=hdfs://namenode:9000 is set
# in docker-compose.yml under the pig service environment
```

### 7.4 Hive: "Table not found" or empty results

```bash
# 1. Check Hive metastore is running
docker exec hive-server2 ps aux | grep hive

# 2. Check the script path inside container
docker exec hive-server2 ls -la /hive-scripts/

# 3. Re-run the HQL manually
docker exec -i hive-server2 hive -f /hive-scripts/warehouse.hql

# 4. Verify the HDFS path exists (external table points here)
docker exec namenode hdfs dfs -ls /data/clean/
# If missing, re-run Pig first
```

### 7.5 Spark: "UnknownHostException: namenode" or connection refused

```bash
# The Spark container must have fs.defaultFS set:
docker exec spark-master env | grep -i "fs.defaultFS\|SPARK"

# Test connectivity from Spark Master to HDFS
docker exec spark-master bash -c '
  hdfs dfs -ls /data/raw/ 2>/dev/null || \
  echo "HDFS unreachable — check Docker network"
'

# If running spark-submit locally (not in Docker), add:
# --conf spark.driver.host=$(hostname -I | awk "{print \$1}")
# --conf spark.hadoop.fs.defaultFS=hdfs://localhost:9000
# (forward HDFS port 9000 in docker-compose)
```

### 7.6 Spark: OutOfMemoryError during cross-validation

```bash
# Edit the spark-submit invocation in scripts/run_pipeline.sh:
# Add: --conf spark.driver.memory=4g --conf spark.executor.memory=4g

docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --driver-memory 4g \
  --executor-memory 4g \
  ...
```

### 7.7 Data looks wrong after Pig (wrong columns)

The Pig scripts load CSVs positionally (no header support in PigStorage
by default). If your CSV has a header row, it will be treated as a data row.

Verify the raw file has a header:
```bash
head -1 data/water_quality_samples.csv
```

The generator already adds a header. Pig scripts use positional field names
in the LOAD statement, so the header row gets loaded as data and filtered
out by the `site_id != ''` or invalid-value filter. If you swap in real EPA
data, check the column order matches the AS clause in `etl_clean.pig`.

### 7.8 "File not found" when running from project directory

```bash
# The run_pipeline.sh script assumes it is run from the project root:
pwd
# Should be: /path/to/AquaKwal

# Fix:
cd /path/to/AquaKwal
./scripts/run_pipeline.sh
```

---

## 8. Understanding the Output

### Pipeline Output Locations in HDFS

```
/data/raw/                          # Stage 1 input
  ├── water_quality_samples.csv
  └── site_metadata.csv
/data/clean/                        # Stage 2 output (Pig)
  ├── water_quality_clean/          # Deduplicated, cleaned, typed
  └── water_quality_enriched/        # + joined with station metadata
/data/output/ml_results/            # Stage 4 output (Spark)
  ├── predictions/                  # Parquet: all test predictions
  ├── metrics/                       # CSV: AUC, F1, Accuracy
  └── feature_importances/          # CSV: per-feature importance scores
```

### Reading Spark Parquet Predictions

```bash
# Option A: from inside Spark container (simplest):
docker exec -i spark-master spark-submit --master spark://spark-master:7077 -e "
  SELECT
    site_id,
    ROUND(ph, 2) AS pH,
    ROUND(dissolved_oxygen, 2) AS do_mg_l,
    water_quality_label,
    CAST(prediction AS INT) AS predicted_label,
    ROUND(CAST(prediction AS DOUBLE) * 100, 1) AS confidence_pct
  FROM parquet.'/data/output/ml_results/predictions'
  LIMIT 20;
"
```

---

## 9. Replacing the Dataset with Real EPA Data

1. Download CSV from the [EPA Water Quality Data Portal](https://www.waterqualitydata.us).
2. Place at `data/water_quality_samples.csv` with at least these columns
   (in order): `site_id, sample_date, ph, temperature, dissolved_oxygen,
   conductivity, turbidity, nitrate, sulfate, latitude, longitude, source_flag,
   water_quality_label`.
3. Ensure `data/site_metadata.csv` has: `site_id, watershed_name, county, state, elevation`.
4. Run `docker compose exec namenode hdfs dfs -rm -f -r /data/raw` then re-run
   `scripts/run_pipeline.sh`.
