-- 04_transform_staging.sql
CREATE OR REPLACE TABLE STG.WEATHER_HOURLY AS
SELECT
    LOCATION_NAME,
    TO_TIMESTAMP_NTZ(time.value::string) AS TIME,
    temperature.value::FLOAT AS TEMPERATURE,
    precipitation.value::FLOAT AS PRECIPITATION,
    LOAD_TS
FROM RAW.WEATHER_JSON r,
     LATERAL FLATTEN(input => r.PAYLOAD:hourly:time) time,
     LATERAL FLATTEN(input => r.PAYLOAD:hourly:temperature_2m) temperature,
     LATERAL FLATTEN(input => r.PAYLOAD:hourly:precipitation) precipitation
WHERE time.index = temperature.index
  AND temperature.index = precipitation.index;

-- Check results
SELECT * FROM STG.WEATHER_HOURLY LIMIT 10;
