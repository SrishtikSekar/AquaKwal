-- ============================================================================
-- AquaKwal: Water Quality Analytics Pipeline
-- File: hive/warehouse.hql
-- Purpose: Create external Hive tables over Pig-cleaned data, then expose
--          queryable views.  Hive sits on top of HDFS data as a queryable
--          warehouse for reporting and validation.
-- ============================================================================

-- ---------------------------------------------------------------------------
-- 1. Cleaned samples table (output of pig/etl_clean.pig)
-- ---------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS water_quality_clean (
    site_id            STRING,
    sample_date        STRING,
    ph                 FLOAT,
    temperature        FLOAT,
    dissolved_oxygen   FLOAT,
    conductivity       FLOAT,
    turbidity          FLOAT,
    nitrate            FLOAT,
    sulfate            FLOAT,
    latitude           DOUBLE,
    longitude          DOUBLE,
    source_flag        STRING,
    water_quality_label STRING
)
ROW FORMAT DELIMITED
    FIELDS TERIMINATED BY ','
STORED AS TEXTFILE
LOCATION '/data/clean/water_quality_clean';

-- ---------------------------------------------------------------------------
-- 2. Enriched dataset table (output of pig/sample_source_join.pig)
--    Adds watershed / county / state / elevation context.
-- ---------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS water_quality_enriched (
    site_id            STRING,
    sample_date        STRING,
    ph                 FLOAT,
    temperature        FLOAT,
    dissolved_oxygen   FLOAT,
    conductivity       FLOAT,
    turbidity          FLOAT,
    nitrate            FLOAT,
    sulfate            FLOAT,
    latitude           DOUBLE,
    longitude          DOUBLE,
    water_quality_label STRING,
    watershed_name     STRING,
    county             STRING,
    state              STRING,
    elevation          FLOAT
)
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/data/clean/water_quality_enriched';

-- ---------------------------------------------------------------------------
-- 3. Reporting views
-- ---------------------------------------------------------------------------

-- Average water quality metrics by state
CREATE OR REPLACE VIEW v_state_summary AS
SELECT
    state,
    COUNT(*)                           AS sample_count,
    ROUND(AVG(ph), 2)                  AS avg_ph,
    ROUND(AVG(dissolved_oxygen), 2)    AS avg_do,
    ROUND(AVG(temperature), 2)         AS avg_temp,
    ROUND(AVG(turbidity), 2)           AS avg_turbidity,
    ROUND(AVG(nitrate), 2)             AS avg_nitrate,
    SUM(CASE WHEN water_quality_label = 'UNSAFE' THEN 1 ELSE 0 END) AS unsafe_count,
    ROUND(AVG(CAST(water_quality_label = 'UNSAFE' AS DOUBLE)) * 100, 2) AS pct_unsafe
FROM water_quality_enriched
GROUP BY state;

-- Unsafe sites — stations where >50% of samples are UNSAFE
CREATE OR REPLACE VIEW v_unsafe_sites AS
SELECT
    site_id,
    watershed_name,
    county,
    state,
    COUNT(*)                                                   AS total_samples,
    SUM(CASE WHEN water_quality_label = 'UNSAFE' THEN 1 ELSE 0 END) AS unsafe_samples,
    ROUND(AVG(ph), 2)                                          AS avg_ph,
    ROUND(AVG(dissolved_oxygen), 2)                            AS avg_do
FROM water_quality_enriched
GROUP BY site_id, watershed_name, county, state
HAVING (SUM(CASE WHEN water_quality_label = 'UNSAFE' THEN 1 ELSE 0 END) * 1.0 / COUNT(*)) > 0.5
ORDER BY unsafe_samples DESC;

-- Trend analysis by month (requires Hive 0.12+ date functions)
CREATE OR REPLACE VIEW v_monthly_trend AS
SELECT
    SUBSTR(sample_date, 1, 7)    AS year_month,
    COUNT(*)                     AS total_samples,
    ROUND(AVG(ph), 2)            AS avg_ph,
    ROUND(AVG(dissolved_oxygen), 2) AS avg_do,
    ROUND(AVG(nitrate), 2)       AS avg_nitrate,
    ROUND(AVG(sulfate), 2)       AS avg_sulfate
FROM water_quality_enriched
WHERE sample_date RLIKE '^[0-9]{4}-[0-9]{2}'
GROUP BY SUBSTR(sample_date, 1, 7)
ORDER BY year_month;

-- ---------------------------------------------------------------------------
-- 4. Sample validation queries (run after ETL to confirm data integrity)
-- ---------------------------------------------------------------------------

-- Row count check
SELECT 'row_count' AS metric, COUNT(*) AS value FROM water_quality_clean;

-- Duplicate check
SELECT 'duplicate_keys' AS metric,
    (COUNT(*) - COUNT(DISTINCT CONCAT(site_id, '|', sample_date))) AS value
FROM water_quality_clean;

-- Null percentage per numeric column
SELECT 'null_pct_ph' AS metric,
    ROUND(AVG(CASE WHEN ph IS NULL THEN 1 ELSE 0 END) * 100, 2) AS value
FROM water_quality_clean
UNION ALL
SELECT 'null_pct_do' AS metric,
    ROUND(AVG(CASE WHEN dissolved_oxygen IS NULL THEN 1 ELSE 0 END) * 100, 2) AS value
FROM water_quality_clean
UNION ALL
SELECT 'null_pct_temp' AS metric,
    ROUND(AVG(CASE WHEN temperature IS NULL THEN 1 ELSE 0 END) * 100, 2) AS value
FROM water_quality_clean;
