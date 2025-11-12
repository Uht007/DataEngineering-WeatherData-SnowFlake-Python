USE DATABASE DE_2;

-- 03_test_and_ingest.sql
-- Call the stored procedure to fetch data
CALL RAW.LOAD_WEATHER();

-- Verify raw JSON landed
SELECT LOCATION_NAME, LOAD_TS, PAYLOAD
FROM RAW.WEATHER_JSON
ORDER BY LOAD_TS DESC
LIMIT 10;
