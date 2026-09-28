-- ============================================================================
-- AquaKwal: Water Quality Analytics Pipeline
-- File: pig/sample_source_join.pig
-- Purpose: Demonstrate joining a second raw source (site metadata / watershed
--          info) onto cleaned samples — required to show multi-source joins.
-- ---------------------------------------------------------------------------
-- Input  1: /data/raw/water_quality_samples.csv   (read in main script)
-- Input 2: /data/raw/site_metadata.csv             (station / watershed info)
-- Output : /data/clean/water_quality_enriched
-- ============================================================================

-- Re-load the cleaned results of the main ETL (idempotent join)
samples = LOAD '/data/clean/water_quality_clean'
    USING PigStorage(',')
    AS (
        site_id:chararray, sample_date:chararray, ph:float, temperature:float,
        dissolved_oxygen:float, conductivity:float, turbidity:float,
        nitrate:float, sulfate:float, latitude:double, longitude:double,
        source_flag:chararray, water_quality_label:chararray
    );

-- Load secondary metadata source: site_id, watershed_name, county, state, elevation
metadata = LOAD '/data/raw/site_metadata.csv'
    USING PigStorage(',')
    AS (
        site_id:chararray, watershed_name:chararray, county:chararray,
        state:chararray, elevation:float
    );

-- INNER JOIN on site_id so only stations with metadata are retained
enriched = JOIN samples BY site_id, metadata BY site_id;

-- Project final enriched schema
result = FOREACH enriched GENERATE
    samples::site_id      AS site_id,
    samples::sample_date  AS sample_date,
    samples::ph           AS ph,
    samples::temperature  AS temperature,
    samples::dissolved_oxygen AS dissolved_oxygen,
    samples::conductivity AS conductivity,
    samples::turbidity    AS turbidity,
    samples::nitrate      AS nitrate,
    samples::sulfate      AS sulfate,
    samples::latitude     AS latitude,
    samples::longitude    AS longitude,
    samples::water_quality_label AS water_quality_label,
    metadata::watershed_name AS watershed_name,
    metadata::county      AS county,
    metadata::state       AS state,
    metadata::elevation   AS elevation
    ;

STORE result INTO '/data/clean/water_quality_enriched' USING PigStorage(',');

-- Note: output is consumed directly by Spark for ML (see spark/ml_quality.py)
