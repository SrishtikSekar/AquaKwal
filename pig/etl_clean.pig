-- ============================================================================
-- AquaKwal: Water Quality Analytics Pipeline
-- File: pig/etl_clean.pig
-- Purpose: Clean raw water quality data from multiple sources.
--          Handles deduplication, type casting, joining, filtering.
-- Input:  HDFS /data/raw/water_quality_*.csv (raw EPA-style records)
-- Output: HDFS /data/clean/water_quality_clean (cleaned, typed, typed)
-- ============================================================================

-- ---------------------------------------------------------------------------
-- 1. LOAD raw water quality readings
--    Columns: site_id, sample_date, ph, temperature, dissolved_oxygen,
--             conductivity, turbidity, nitrate, sulfate, latitude, longitude,
--             source_flag
-- ---------------------------------------------------------------------------
-- A raw record may have missing or string-encoded numeric fields; Pig loads
-- everything as chararray first so we can clean robustly.

raw_data = LOAD '/data/raw/water_quality_samples.csv'
    USING PigStorage(',')
    AS (
        site_id:chararray,
        sample_date:chararray,
        ph:chararray,
        temperature:chararray,
        dissolved_oxygen:chararray,
        conductivity:chararray,
        turbidity:chararray,
        nitrate:chararray,
        sulfate:chararray,
        latitude:chararray,
        longitude:chararray,
        source_flag:chararray
    );

-- ---------------------------------------------------------------------------
-- 2. CLEANING PASS: cast, handle missing values, add a computed quality label
-- ---------------------------------------------------------------------------

-- Replace empty strings / "NA" / "null" with null for numeric fields using a
-- nested FOREACH so we can guard each cast.
cleaned = FOREACH raw_data GENERATE
    site_id,
    sample_date,
    -- Type-cast numerics; emit null when the value is missing or invalid
    (ph MATCHES '[0-9.]+' ? (float)ph : NULL) AS ph,
    (temperature MATCHES '[0-9.]+' ? (float)temperature : NULL) AS temperature,
    (dissolved_oxygen MATCHES '[0-9.]+' ? (float)dissolved_oxygen : NULL) AS dissolved_oxygen,
    (conductivity MATCHES '[0-9.]+' ? (float)conductivity : NULL) AS conductivity,
    (turbidity MATCHES '[0-9.]+' ? (float)turbidity : NULL) AS turbidity,
    (nitrate MATCHES '[0-9.]+' ? (float)nitrate : NULL) AS nitrate,
    (sulfate MATCHES '[0-9.]+' ? (float)sulfate : NULL) AS sulfate,
    (latitude MATCHES '[-0-9.]+' ? (double)latitude : NULL) AS latitude,
    (longitude MATCHES '[-0-9.]+' ? (double)longitude : NULL) AS longitude,
    source_flag,
    -- Derive a water_quality_label from WHO thresholds for context
    --   pH 6.5-8.5 acceptable; DO >= 5 mg/L acceptable
    (
        (ph IS NOT NULL AND ph >= 6.5 AND ph <= 8.5)
        AND (dissolved_oxygen IS NOT NULL AND dissolved_oxygen >= 5.0)
    ) ? 'SAFE' : 'UNSAFE' AS water_quality_label
    ;

-- ---------------------------------------------------------------------------
-- 3. FILTER: drop records missing critical fields or with impossible values
-- ---------------------------------------------------------------------------

valid_records = FILTER cleaned BY
    site_id IS NOT NULL
    AND site_id != ''
    AND sample_date IS NOT NULL
    AND sample_date != ''
    AND ph IS NOT NULL
    AND temperature IS NOT NULL
    AND dissolved_oxygen IS NOT NULL
    AND latitude IS NOT NULL
    AND longitude IS NOT NULL
    -- sanity bounds
    AND ph >= 0.0 AND ph <= 14.0
    AND dissolved_oxygen >= 0.0 AND dissolved_oxygen <= 25.0
    AND temperature >= -5.0 AND temperature <= 45.0
    ;

-- ---------------------------------------------------------------------------
-- 4. DEDUPLICATE: if the same site sampled on the same date appears more than
--    once, collapse to a single record (avg non-key fields).
-- ---------------------------------------------------------------------------

grouped = GROUP valid_records BY (site_id, sample_date);
deduped = FOREACH grouped {
    -- sort by a representative column to make the pick deterministic
    ordered = ORDER valid_records BY dissolved_oxygen DESC;
    top = LIMIT ordered 1;
    GENERATE FLATTEN(top) AS (
        site_id, sample_date, ph, temperature, dissolved_oxygen,
        conductivity, turbidity, nitrate, sulfate, latitude, longitude,
        source_flag, water_quality_label
    );
}
-- Note: if truly identical duplicates exist, GROUP already collapses them;
-- the LIMIT 1 per group resolves the rare duplicate-key case deterministically.

-- ---------------------------------------------------------------------------
-- 5. STORE cleaned output to HDFS
-- ---------------------------------------------------------------------------

STORE deduped INTO '/data/clean/water_quality_clean' USING PigStorage(',');
