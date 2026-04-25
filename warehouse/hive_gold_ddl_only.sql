-- ============================================================
-- Gold Layer: CREATE TABLE ONLY (no inserts)
-- Run this ONCE to initialise the schema.
-- Airflow (silver_to_gold task) handles all INSERT OVERWRITE.
-- ============================================================

CREATE DATABASE IF NOT EXISTS flood_monitoring
COMMENT 'Thailand Flood Risk Monitoring System — Gold Layer'
LOCATION '/gold';

USE flood_monitoring;

-- ── Dim_Date ──────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS Dim_Date (
    date_key        INT     COMMENT 'Surrogate key YYYYMMDD',
    full_date       DATE,
    year            INT,
    month           INT,
    month_name_th   STRING,
    day             INT,
    quarter         INT,
    day_of_week     INT,
    day_name_en     STRING,
    is_weekend      BOOLEAN,
    thai_year       INT     COMMENT 'Buddhist Era (CE+543)'
)
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');

-- ── Dim_Location ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS Dim_Location (
    location_key    INT,
    geocode         STRING  COMMENT '6-digit sub-district code',
    tambon_th       STRING,
    tambon_en       STRING,
    amphoe_code     INT,
    amphoe_th       STRING,
    amphoe_en       STRING,
    prov_code       INT,
    prov_th         STRING,
    prov_en         STRING,
    region          STRING  COMMENT 'N/NE/C/E/W/S'
)
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');

-- ── Dim_Reservoir ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS Dim_Reservoir (
    reservoir_key   INT,
    reservoir_id    STRING,
    reservoir_name  STRING,
    region          STRING,
    owner           STRING,
    reservoir_type  STRING  COMMENT 'dam | reservoir',
    capacity_mcm    DOUBLE,
    dead_storage_mcm DOUBLE
)
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');

-- ── Dim_Weather ───────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS Dim_Weather (
    weather_key             INT,
    date_key                INT,
    prov_code               INT,
    prov_th                 STRING,
    prov_en                 STRING,
    region                  STRING,
    temp_max_c              DOUBLE,
    temp_min_c              DOUBLE,
    rainfall_forecast_mm    DOUBLE,
    rain_intensity          STRING,
    humidity_pct            DOUBLE,
    wind_speed_kmh          DOUBLE,
    is_heavy_rain_24h       BOOLEAN,
    data_source             STRING
)
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');

-- ── Fact_Water_Monitoring ─────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS Fact_Water_Monitoring (
    fact_key            BIGINT,
    date_key            INT,
    reservoir_key       INT,
    record_date         DATE,
    volume_mcm          DOUBLE,
    percent_storage     DOUBLE,
    inflow_mcm          DOUBLE,
    outflow_mcm         DOUBLE,
    storage_mcm         DOUBLE,
    active_storage_mcm  DOUBLE,
    loaded_at           STRING
)
PARTITIONED BY (dt STRING COMMENT 'YYYY-MM-DD')
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');

-- ── Fact_Flood_Risk ───────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS Fact_Flood_Risk (
    fact_key                BIGINT,
    date_key                INT,
    location_key            INT,
    weather_key             INT,
    geocode                 STRING,
    risk_month              INT,
    historical_flood_count  INT,
    risk_level              STRING,
    risk_score              INT,
    flood_criteria          STRING,
    rain_forecast_mm        DOUBLE,
    is_heavy_rain_forecast  BOOLEAN,
    loaded_at               STRING
)
PARTITIONED BY (dt STRING COMMENT 'YYYY-MM-DD')
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');

SHOW TABLES;
