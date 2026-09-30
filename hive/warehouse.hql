-- ============================================================================
-- AquaKwal: Indian Water Quality Analytics Pipeline
-- File: hive/warehouse.hql
-- Purpose: Create external Hive tables over Pig-cleaned Indian water data,
--          and expose queryable views for reporting and validation.
-- ============================================================================

-- ---------------------------------------------------------------------------
-- 1. Cleaned water quality table (output of pig/etl_clean.pig)
-- ---------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS water_quality_clean (
    stn_code              STRING,
    monitoring_location   STRING,
    year                  INT,
    water_body_type       STRING,
    state_name            STRING,
    temp_min              FLOAT,
    temp_max              FLOAT,
    dissolved_min         FLOAT,
    dissolved_max         FLOAT,
    ph_min                FLOAT,
    ph_max                FLOAT,
    conductivity_min      FLOAT,
    conductivity_max      FLOAT,
    bod_min               FLOAT,
    bod_max               FLOAT,
    nitrate_min           FLOAT,
    nitrate_max           FLOAT,
    fecal_coliform_min    FLOAT,
    fecal_coliform_max    FLOAT,
    total_coliform_min    FLOAT,
    total_coliform_max    FLOAT,
    fecal_min             FLOAT,
    fecal_max             FLOAT,
    water_quality_label   STRING
)
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/data/clean/water_quality_clean';

-- ---------------------------------------------------------------------------
-- 2. Enriched dataset table (placeholder for future joins)
-- ---------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS water_quality_enriched (
    stn_code              STRING,
    monitoring_location   STRING,
    year                  INT,
    water_body_type       STRING,
    state_name            STRING,
    temp_min              FLOAT,
    temp_max              FLOAT,
    dissolved_min         FLOAT,
    dissolved_max         FLOAT,
    ph_min                FLOAT,
    ph_max                FLOAT,
    conductivity_min      FLOAT,
    conductivity_max      FLOAT,
    bod_min               FLOAT,
    bod_max               FLOAT,
    nitrate_min           FLOAT,
    nitrate_max           FLOAT,
    fecal_coliform_min    FLOAT,
    fecal_coliform_max    FLOAT,
    total_coliform_min    FLOAT,
    total_coliform_max    FLOAT,
    fecal_min             FLOAT,
    fecal_max             FLOAT,
    water_quality_label   STRING,
    standards_violation_count INT
)
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/data/clean/water_quality_enriched';

-- ---------------------------------------------------------------------------
-- 3. Reporting views
-- ---------------------------------------------------------------------------

-- State-wise water quality summary
CREATE OR REPLACE VIEW v_state_summary AS
SELECT
    state_name,
    COUNT(*)                                          AS sample_count,
    ROUND(AVG(temp_min), 2)                          AS avg_temp_min,
    ROUND(AVG(temp_max), 2)                          AS avg_temp_max,
    ROUND(AVG(dissolved_min), 2)                     AS avg_dissolved_min,
    ROUND(AVG(ph_min), 2)                            AS avg_ph_min,
    ROUND(AVG(ph_max), 2)                            AS avg_ph_max,
    ROUND(AVG(bod_max), 2)                           AS avg_bod_max,
    ROUND(AVG(nitrate_max), 2)                       AS avg_nitrate_max,
    ROUND(AVG(fecal_coliform_max), 2)                AS avg_fecal_coliform_max,
    SUM(CASE WHEN water_quality_label = 'POOR' THEN 1 ELSE 0 END) AS poor_count,
    ROUND(AVG(CASE WHEN water_quality_label = 'POOR' THEN 1.0 ELSE 0.0 END) * 100, 2) AS pct_poor
FROM water_quality_clean
GROUP BY state_name;

-- Water body type summary
CREATE OR REPLACE VIEW v_water_body_summary AS
SELECT
    water_body_type,
    COUNT(*)                                          AS sample_count,
    ROUND(AVG(dissolved_min), 2)                     AS avg_dissolved_min,
    ROUND(AVG(bod_max), 2)                           AS avg_bod_max,
    ROUND(AVG(fecal_coliform_max), 2)                AS avg_fecal_coliform_max,
    SUM(CASE WHEN water_quality_label = 'POOR' THEN 1 ELSE 0 END) AS poor_count,
    ROUND(AVG(CASE WHEN water_quality_label = 'POOR' THEN 1.0 ELSE 0.0 END) * 100, 2) AS pct_poor
FROM water_quality_clean
GROUP BY water_body_type;

-- Yearly trend
CREATE OR REPLACE VIEW v_yearly_trend AS
SELECT
    year,
    COUNT(*)                                          AS sample_count,
    ROUND(AVG(dissolved_min), 2)                     AS avg_dissolved_min,
    ROUND(AVG(bod_max), 2)                           AS avg_bod_max,
    ROUND(AVG(nitrate_max), 2)                       AS avg_nitrate_max,
    SUM(CASE WHEN water_quality_label = 'POOR' THEN 1 ELSE 0 END) AS poor_count,
    ROUND(AVG(CASE WHEN water_quality_label = 'POOR' THEN 1.0 ELSE 0.0 END) * 100, 2) AS pct_poor
FROM water_quality_clean
GROUP BY year
ORDER BY year;

-- Violation analysis (CPCB standards)
CREATE OR REPLACE VIEW v_violation_analysis AS
SELECT
    state_name,
    water_body_type,
    SUM(CASE WHEN dissolved_min < 4.0 THEN 1 ELSE 0 END)         AS dissolved_violations,
    SUM(CASE WHEN ph_min < 6.5 OR ph_max > 8.5 THEN 1 ELSE 0 END) AS ph_violations,
    SUM(CASE WHEN bod_max > 3.0 THEN 1 ELSE 0 END)               AS bod_violations,
    SUM(CASE WHEN fecal_coliform_max > 2500 THEN 1 ELSE 0 END)   AS fecal_violations,
    COUNT(*)                                                     AS total_samples
FROM water_quality_clean
GROUP BY state_name, water_body_type
ORDER BY state_name, water_body_type;