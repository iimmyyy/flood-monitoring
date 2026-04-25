-- ============================================================
-- Silver External Tables in Hive
-- Run AFTER bronze_to_silver.py has written Parquet to HDFS.
-- These tables let Hive query Silver Parquet directly.
-- ============================================================

CREATE DATABASE IF NOT EXISTS silver
LOCATION '/silver';

USE silver;

-- ── silver_reservoir (partitioned Parquet) ────────────────────────────────────
CREATE EXTERNAL TABLE IF NOT EXISTS silver_reservoir (
    reservoir_id        STRING,
    reservoir_name      STRING,
    region              STRING,
    source              STRING,
    owner               STRING,
    record_date         STRING,
    capacity_mcm        DOUBLE,
    volume_mcm          DOUBLE,
    percent_storage     DOUBLE,
    inflow_mcm          DOUBLE,
    outflow_mcm         DOUBLE,
    active_storage_mcm  DOUBLE,
    dead_storage_mcm    DOUBLE,
    fetched_at          STRING,
    silver_loaded_at    STRING
)
PARTITIONED BY (dt STRING)
STORED AS PARQUET
LOCATION '/silver/reservoir'
TBLPROPERTIES ('parquet.compress'='SNAPPY');

-- Register all existing partitions
MSCK REPAIR TABLE silver_reservoir;

-- ── silver_flood_risk (single Parquet file) ───────────────────────────────────
CREATE EXTERNAL TABLE IF NOT EXISTS silver_flood_risk (
    geocode             STRING,
    month               INT,
    tambon_th           STRING,
    tambon_en           STRING,
    amphoe_code         INT,
    amphoe_th           STRING,
    amphoe_en           STRING,
    prov_code           INT,
    prov_th             STRING,
    prov_en             STRING,
    flood_count_17yr    INT,
    flood_criteria      STRING,
    risk_level          STRING,
    risk_score          INT,
    silver_loaded_at    STRING
)
STORED AS PARQUET
LOCATION '/silver/flood_risk'
TBLPROPERTIES ('parquet.compress'='SNAPPY');

-- ── silver_weather (partitioned Parquet) ──────────────────────────────────────
CREATE EXTERNAL TABLE IF NOT EXISTS silver_weather (
    prov_code               INT,
    prov_th                 STRING,
    prov_en                 STRING,
    region                  STRING,
    forecast_date           STRING,
    temp_max_c              DOUBLE,
    temp_min_c              DOUBLE,
    rainfall_forecast_mm    DOUBLE,
    rain_intensity          STRING,
    humidity_pct            DOUBLE,
    wind_speed_kmh          DOUBLE,
    is_heavy_rain_24h       BOOLEAN,
    data_source             STRING,
    silver_loaded_at        STRING
)
PARTITIONED BY (dt STRING)
STORED AS PARQUET
LOCATION '/silver/weather'
TBLPROPERTIES ('parquet.compress'='SNAPPY');

MSCK REPAIR TABLE silver_weather;

SHOW TABLES;
