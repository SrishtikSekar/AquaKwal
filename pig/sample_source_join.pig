-- Enrich CPCB rows with a count of regulatory threshold violations.
-- The one-row threshold file has this schema:
-- dissolved_min,ph_min,ph_max,bod_max,fecal_coliform_max

%default CLEAN_INPUT '/data/clean/water_quality_clean'
%default THRESHOLDS_INPUT '/data/raw/parameter_thresholds.csv'
%default ENRICHED_OUTPUT '/data/clean/water_quality_enriched'

clean = LOAD '$CLEAN_INPUT'
    USING PigStorage('|')
    AS (
        stn_code:chararray,
        monitoring_location:chararray,
        year:int,
        water_body_type:chararray,
        state_name:chararray,
        temp_min:float,
        temp_max:float,
        dissolved_min:float,
        dissolved_max:float,
        ph_min:float,
        ph_max:float,
        conductivity_min:float,
        conductivity_max:float,
        bod_min:float,
        bod_max:float,
        nitrate_min:float,
        nitrate_max:float,
        fecal_coliform_min:float,
        fecal_coliform_max:float,
        total_coliform_min:float,
        total_coliform_max:float,
        fecal_min:float,
        fecal_max:float,
        water_quality_label:chararray
    );

thresholds_raw = LOAD '$THRESHOLDS_INPUT'
    USING PigStorage(',')
    AS (
        dissolved_min:chararray,
        ph_min:chararray,
        ph_max:chararray,
        bod_max:chararray,
        fecal_coliform_max:chararray
    );

-- Reject the header and incomplete threshold rows.
thresholds = FOREACH (
    FILTER thresholds_raw BY
        dissolved_min MATCHES '[0-9.]+' AND
        ph_min MATCHES '[0-9.]+' AND
        ph_max MATCHES '[0-9.]+' AND
        bod_max MATCHES '[0-9.]+' AND
        fecal_coliform_max MATCHES '[0-9.]+'
) GENERATE
    (float)dissolved_min AS dissolved_min,
    (float)ph_min AS ph_min,
    (float)ph_max AS ph_max,
    (float)bod_max AS bod_max,
    (float)fecal_coliform_max AS fecal_coliform_max;

joined = CROSS clean, thresholds;

enriched = FOREACH joined GENERATE
    clean::stn_code,
    clean::monitoring_location,
    clean::year,
    clean::water_body_type,
    clean::state_name,
    clean::temp_min,
    clean::temp_max,
    clean::dissolved_min,
    clean::dissolved_max,
    clean::ph_min,
    clean::ph_max,
    clean::conductivity_min,
    clean::conductivity_max,
    clean::bod_min,
    clean::bod_max,
    clean::nitrate_min,
    clean::nitrate_max,
    clean::fecal_coliform_min,
    clean::fecal_coliform_max,
    clean::total_coliform_min,
    clean::total_coliform_max,
    clean::fecal_min,
    clean::fecal_max,
    clean::water_quality_label,
    (
        (clean::dissolved_min < thresholds::dissolved_min ? 1 : 0)
        + (clean::ph_min < thresholds::ph_min OR clean::ph_max > thresholds::ph_max ? 1 : 0)
        + (clean::bod_max > thresholds::bod_max ? 1 : 0)
        + (clean::fecal_coliform_max > thresholds::fecal_coliform_max ? 1 : 0)
    ) AS standards_violation_count;

STORE enriched INTO '$ENRICHED_OUTPUT' USING PigStorage('|');
