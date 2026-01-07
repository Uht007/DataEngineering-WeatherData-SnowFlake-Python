# DataEngineer-SnowFlake-WeatherData
Weather Data Project for Data Engineering using Python and SnowFlake.  
Fetches historical hourly weather data from the Open-Meteo API, loads it into Snowflake, and models it for analytics.

Pipeline Flow
- Open-Meteo API → RAW → STG → DW

Setup

- Login to Snowflake Web UI.
- Create/Select Warehouse (COMPUTE_WH) and Database (DE_2).
- Run SQL worksheets in order:
- 01_create_schemas_and_raw.sql - Run once to create Schemas
- 02_create_network_rule_access.sql - Run Once to create network rule and allow external access to Open-metro API                         
- 03_create_python_proc.sql - Run once to create Python API procedure                        
- 04_transform_staging.sql - Run once to create Staging procedure after API  
- 05_test_and_ingest.sql - Run on-demand
- 06_model_dw.sql - Run once to create DW procedure after Staging 
- 07_create_task_and_quality.sql - Run once to configure automation

 
  # Notes

Design:
 - Since the pipeline pulls historical hourly weather data and does not require real-time processing, a batch ETL architecture was chosen as the most appropriate solution.

Ingestion:
 - A Snowpark Python stored procedure calls the Open-Meteo API, retrieves the raw JSON payloads, and loads them into the RAW layer.

Data Modeling & Warehouse:
 - A 3-tier approach is used, with separate schemas for RAW, STG, and DW/Analytics layers.
 - A stored procedure transforms the raw JSON payload, loads it into the staging layer, and performs data cleansing and flattening.
 - Another stored procedure aggregates and models the data into the data warehouse for analytics.

Snowflake Automation & Quality:
 - An hourly Snowflake Task (cron-based) triggers the RAW ingestion procedure, which then orchestrates the STG and DW transformation procedures.
 - Incremental loading is achieved by using the latest timestamp from each layer and applying MERGE/UPDATE logic to avoid duplicates.
 - Basic data quality checks include row counts per location, duplicate timestamps, and detection of missing timestamps.
