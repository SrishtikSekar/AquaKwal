-- ============================================================================
-- AquaKwal: Indian Water Quality Analytics Pipeline
-- File: pig/etl_clean.pig
-- Purpose: Clean Indian water quality monitoring data (CPCB format).
--          Handles categorical variables, missing values, derives water quality label.
--
-- Input:  HDFS /data/raw/Indian_water_data_pipe.csv
-- Output: HDFS /data/clean/water_quality_clean
-- ============================================================================

%default INPUT '/data/raw/Indian_water_data_pipe.csv'
%default CLEAN_OUTPUT '/data/clean/water_quality_clean'

raw_data = LOAD '$INPUT'
    USING PigStorage('|')
    AS (
        stn_code:chararray,
        monitoring_location:chararray,
        year:chararray,
        water_body_type:chararray,
        state_name:chararray,
        temp_min:chararray,
        temp_max:chararray,
        dissolved_min:chararray,
        dissolved_max:chararray,
        ph_min:chararray,
        ph_max:chararray,
        conductivity_min:chararray,
        conductivity_max:chararray,
        bod_min:chararray,
        bod_max:chararray,
        nitrate_min:chararray,
        nitrate_max:chararray,
        fecal_coliform_min:chararray,
        fecal_coliform_max:chararray,
        total_coliform_min:chararray,
        total_coliform_max:chararray,
        fecal_min:chararray,
        fecal_max:chararray
    );

-- Strip header
no_header = FILTER raw_data BY stn_code != 'STN code' AND stn_code IS NOT NULL;

-- Type casting with NULL guards
typed = FOREACH no_header GENERATE
    stn_code,
    monitoring_location,
    (year MATCHES '[0-9]+' ? (int)year : NULL) AS year,
    water_body_type,
    state_name,
    (temp_min MATCHES '[0-9.]+' ? (float)temp_min : NULL) AS temp_min,
    (temp_max MATCHES '[0-9.]+' ? (float)temp_max : NULL) AS temp_max,
    (dissolved_min MATCHES '[0-9.]+' ? (float)dissolved_min : NULL) AS dissolved_min,
    (dissolved_max MATCHES '[0-9.]+' ? (float)dissolved_max : NULL) AS dissolved_max,
    (ph_min MATCHES '[0-9.]+' ? (float)ph_min : NULL) AS ph_min,
    (ph_max MATCHES '[0-9.]+' ? (float)ph_max : NULL) AS ph_max,
    (conductivity_min MATCHES '[0-9.]+' ? (float)conductivity_min : NULL) AS conductivity_min,
    (conductivity_max MATCHES '[0-9.]+' ? (float)conductivity_max : NULL) AS conductivity_max,
    (bod_min MATCHES '[0-9.]+' ? (float)bod_min : NULL) AS bod_min,
    (bod_max MATCHES '[0-9.]+' ? (float)bod_max : NULL) AS bod_max,
    (nitrate_min MATCHES '[0-9.]+' ? (float)nitrate_min : NULL) AS nitrate_min,
    (nitrate_max MATCHES '[0-9.]+' ? (float)nitrate_max : NULL) AS nitrate_max,
    (fecal_coliform_min MATCHES '[0-9.]+' ? (float)fecal_coliform_min : NULL) AS fecal_coliform_min,
    (fecal_coliform_max MATCHES '[0-9.]+' ? (float)fecal_coliform_max : NULL) AS fecal_coliform_max,
    (total_coliform_min MATCHES '[0-9.]+' ? (float)total_coliform_min : NULL) AS total_coliform_min,
    (total_coliform_max MATCHES '[0-9.]+' ? (float)total_coliform_max : NULL) AS total_coliform_max,
    (fecal_min MATCHES '[0-9.]+' ? (float)fecal_min : NULL) AS fecal_min,
    (fecal_max MATCHES '[0-9.]+' ? (float)fecal_max : NULL) AS fecal_max
    ;

-- Compute column means for imputation
grouped = GROUP typed ALL;
col_means = FOREACH grouped GENERATE
    AVG(typed.temp_min) AS mean_temp_min,
    AVG(typed.temp_max) AS mean_temp_max,
    AVG(typed.dissolved_min) AS mean_dissolved_min,
    AVG(typed.dissolved_max) AS mean_dissolved_max,
    AVG(typed.ph_min) AS mean_ph_min,
    AVG(typed.ph_max) AS mean_ph_max,
    AVG(typed.conductivity_min) AS mean_conductivity_min,
    AVG(typed.conductivity_max) AS mean_conductivity_max,
    AVG(typed.bod_min) AS mean_bod_min,
    AVG(typed.bod_max) AS mean_bod_max,
    AVG(typed.nitrate_min) AS mean_nitrate_min,
    AVG(typed.nitrate_max) AS mean_nitrate_max,
    AVG(typed.fecal_coliform_min) AS mean_fecal_coliform_min,
    AVG(typed.fecal_coliform_max) AS mean_fecal_coliform_max,
    AVG(typed.total_coliform_min) AS mean_total_coliform_min,
    AVG(typed.total_coliform_max) AS mean_total_coliform_max,
    AVG(typed.fecal_min) AS mean_fecal_min,
    AVG(typed.fecal_max) AS mean_fecal_max
    ;

-- Impute NULLs with column means
with_means = CROSS typed, col_means;

imputed = FOREACH with_means GENERATE
    typed::stn_code AS stn_code,
    typed::monitoring_location AS monitoring_location,
    typed::year AS year,
    typed::water_body_type AS water_body_type,
    typed::state_name AS state_name,
    (typed::temp_min IS NULL ? col_means::mean_temp_min : typed::temp_min) AS temp_min,
    (typed::temp_max IS NULL ? col_means::mean_temp_max : typed::temp_max) AS temp_max,
    (typed::dissolved_min IS NULL ? col_means::mean_dissolved_min : typed::dissolved_min) AS dissolved_min,
    (typed::dissolved_max IS NULL ? col_means::mean_dissolved_max : typed::dissolved_max) AS dissolved_max,
    (typed::ph_min IS NULL ? col_means::mean_ph_min : typed::ph_min) AS ph_min,
    (typed::ph_max IS NULL ? col_means::mean_ph_max : typed::ph_max) AS ph_max,
    (typed::conductivity_min IS NULL ? col_means::mean_conductivity_min : typed::conductivity_min) AS conductivity_min,
    (typed::conductivity_max IS NULL ? col_means::mean_conductivity_max : typed::conductivity_max) AS conductivity_max,
    (typed::bod_min IS NULL ? col_means::mean_bod_min : typed::bod_min) AS bod_min,
    (typed::bod_max IS NULL ? col_means::mean_bod_max : typed::bod_max) AS bod_max,
    (typed::nitrate_min IS NULL ? col_means::mean_nitrate_min : typed::nitrate_min) AS nitrate_min,
    (typed::nitrate_max IS NULL ? col_means::mean_nitrate_max : typed::nitrate_max) AS nitrate_max,
    (typed::fecal_coliform_min IS NULL ? col_means::mean_fecal_coliform_min : typed::fecal_coliform_min) AS fecal_coliform_min,
    (typed::fecal_coliform_max IS NULL ? col_means::mean_fecal_coliform_max : typed::fecal_coliform_max) AS fecal_coliform_max,
    (typed::total_coliform_min IS NULL ? col_means::mean_total_coliform_min : typed::total_coliform_min) AS total_coliform_min,
    (typed::total_coliform_max IS NULL ? col_means::mean_total_coliform_max : typed::total_coliform_max) AS total_coliform_max,
    (typed::fecal_min IS NULL ? col_means::mean_fecal_min : typed::fecal_min) AS fecal_min,
    (typed::fecal_max IS NULL ? col_means::mean_fecal_max : typed::fecal_max) AS fecal_max
    ;

-- Derive water quality label based on CPCB criteria
-- GOOD if: DO >= 4, pH 6.5-8.5, BOD <= 3, Fecal Coliform <= 2500
-- POOR otherwise
labeled = FOREACH imputed GENERATE
    stn_code,
    monitoring_location,
    year,
    water_body_type,
    state_name,
    temp_min,
    temp_max,
    dissolved_min,
    dissolved_max,
    ph_min,
    ph_max,
    conductivity_min,
    conductivity_max,
    bod_min,
    bod_max,
    nitrate_min,
    nitrate_max,
    fecal_coliform_min,
    fecal_coliform_max,
    total_coliform_min,
    total_coliform_max,
    fecal_min,
    fecal_max,
    (dissolved_min >= 4.0 AND ph_min >= 6.5 AND ph_max <= 8.5 AND bod_max <= 3.0 AND fecal_coliform_max <= 2500.0 ? 'GOOD' : 'POOR') AS water_quality_label
    ;

-- Deduplicate
deduped = DISTINCT labeled;

-- Keep a pipe delimiter end-to-end. Monitoring locations frequently contain
-- commas, so comma-delimited intermediate output would shift downstream fields.
STORE deduped INTO '$CLEAN_OUTPUT' USING PigStorage('|');
