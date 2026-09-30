-- ============================================================================
-- AquaKwal: Water Quality Analytics Pipeline
-- File: pig/sample_source_join.pig
-- Purpose: Join the Pig-cleaned water quality data with a regulatory
--          parameter-thresholds reference file (WHO drinking-water standards)
--          to compute a standards_violation_count per sample.
-- ---------------------------------------------------------------------------
-- Input 1: /data/clean/water_quality_clean  (output of etl_clean.pig)
-- Input 2: /data/raw/parameter_thresholds.csv (WHO guideline thresholds)
-- Output : /data/clean/water_quality_enriched
--          Adds: standards_violation_count
-- ---------------------------------------------------------------------------

-- ---------------------------------------------------------------------------
-- 1. LOAD cleaned water quality data (written by etl_clean.pig)
--    Schema matches the STORE output: ph, hardness, solids, chloramines,
--    sulfate, conductivity, organic_carbon, trihalomethanes, turbidity,
--    potability, water_quality_label
-- ---------------------------------------------------------------------------

clean = LOAD '/data/clean/water_quality_clean'
    USING PigStorage(',')
    AS (
        ph:float,
        hardness:float,
        solids:float,
        chloramines:float,
        sulfate:float,
        conductivity:float,
        organic_carbon:float,
        trihalomethanes:float,
        turbidity:float,
        potability:int,
        water_quality_label:chararray
    );

-- ---------------------------------------------------------------------------
-- 2. LOAD reference thresholds (single data row + header)
--    Columns: ph_min, ph_max, hardness_max, solids_max, chloramines_max,
--             sulfate_max, conductivity_max, organic_carbon_max,
--             trihalomethanes_max, turbidity_max
-- ---------------------------------------------------------------------------

thresholds_raw = LOAD '/data/raw/parameter_thresholds.csv'
    USING PigStorage(',')
    AS (
        ph_min:float, ph_max:float, hardness_max:float, solids_max:float,
        chloramines_max:float, sulfate_max:float, conductivity_max:float,
        organic_carbon_max:float, trihalomethanes_max:float, turbidity_max:float
    );

-- Strip header (header row has non-numeric ph_min that parses as NULL)
thresholds = FILTER thresholds_raw BY ph_min IS NOT NULL;

-- ---------------------------------------------------------------------------
-- 3. CROSS join: attach WHO thresholds to every sample row
--    (lookup pattern — thresholds is a 1-row reference table)
-- ---------------------------------------------------------------------------

joined = CROSS clean, thresholds;

-- ---------------------------------------------------------------------------
-- 4. COMPUTE standards_violation_count per sample.
--    Each parameter is compared against its WHO guideline.
--    Uses nested ternary + integer addition (Pig has no CASE).
-- ---------------------------------------------------------------------------

enriched = FOREACH joined GENERATE
    clean::ph               AS ph,
    clean::hardness         AS hardness,
    clean::solids           AS solids,
    clean::chloramines      AS chloramines,
    clean::sulfate          AS sulfate,
    clean::conductivity     AS conductivity,
    clean::organic_carbon   AS organic_carbon,
    clean::trihalomethanes  AS trihalomethanes,
    clean::turbidity        AS turbidity,
    clean::potability       AS potability,
    clean::water_quality_label AS water_quality_label,
    (
        (clean::ph < thresholds::ph_min OR clean::ph > thresholds::ph_max ? 1 : 0)
        + (clean::hardness      > thresholds::hardness_max      ? 1 : 0)
        + (clean::solids        > thresholds::solids_max        ? 1 : 0)
        + (clean::chloramines   > thresholds::chloramines_max   ? 1 : 0)
        + (clean::sulfate       > thresholds::sulfate_max       ? 1 : 0)
        + (clean::conductivity  > thresholds::conductivity_max  ? 1 : 0)
        + (clean::organic_carbon > thresholds::organic_carbon_max ? 1 : 0)
        + (clean::trihalomethanes > thresholds::trihalomethanes_max ? 1 : 0)
        + (clean::turbidity     > thresholds::turbidity_max     ? 1 : 0)
    ) AS standards_violation_count
    ;

-- ---------------------------------------------------------------------------
-- 5. STORE enriched output to HDFS
-- ---------------------------------------------------------------------------

STORE enriched INTO '/data/clean/water_quality_enriched' USING PigStorage(',');
