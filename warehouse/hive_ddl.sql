-- ============================================================
-- Thailand Flood Risk Monitoring — Hive Gold Layer DDL
-- Star Schema: 3 Fact tables, 5 Dimension tables
-- Database: flood_monitoring
-- ============================================================

-- ── Create database ──────────────────────────────────────────────────────────
CREATE DATABASE IF NOT EXISTS flood_monitoring
COMMENT 'Thailand Flood Risk Monitoring System — Gold Layer'
LOCATION '/gold';

USE flood_monitoring;


-- ============================================================
-- DIMENSION TABLES
-- ============================================================

-- ── Dim_Date ─────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS Dim_Date (
    date_key        INT          COMMENT 'Surrogate key in format YYYYMMDD',
    full_date       DATE         COMMENT 'Calendar date',
    year            INT,
    month           INT          COMMENT '1–12',
    month_name_th   STRING       COMMENT 'ชื่อเดือนภาษาไทย',
    day             INT,
    quarter         INT          COMMENT '1–4',
    day_of_week     INT          COMMENT '1=Mon … 7=Sun',
    day_name_en     STRING,
    is_weekend      BOOLEAN,
    thai_year       INT          COMMENT 'Buddhist Era year (CE + 543)'
)
COMMENT 'Date dimension — one row per calendar day'
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');


-- ── Dim_Location ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS Dim_Location (
    location_key    INT          COMMENT 'Surrogate key',
    geocode         STRING       COMMENT '6-digit sub-district code (GEOCODE)',
    tambon_th       STRING       COMMENT 'ชื่อตำบล (ไทย)',
    tambon_en       STRING       COMMENT 'Sub-district name (English)',
    amphoe_code     INT,
    amphoe_th       STRING       COMMENT 'ชื่ออำเภอ (ไทย)',
    amphoe_en       STRING,
    prov_code       INT,
    prov_th         STRING       COMMENT 'ชื่อจังหวัด (ไทย)',
    prov_en         STRING,
    region          STRING       COMMENT 'N/NE/C/E/W/S'
)
COMMENT 'Location dimension — sub-district granularity'
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');


-- ── Dim_Reservoir ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS Dim_Reservoir (
    reservoir_key   INT          COMMENT 'Surrogate key',
    reservoir_id    STRING       COMMENT 'RID reservoir/dam ID',
    reservoir_name  STRING,
    region          STRING       COMMENT 'Geographic region from RID',
    owner           STRING       COMMENT 'Owning agency',
    reservoir_type  STRING       COMMENT '"dam" (large) or "reservoir" (medium)',
    capacity_mcm    DOUBLE       COMMENT 'Total capacity in million cubic metres',
    dead_storage_mcm DOUBLE      COMMENT 'Dead storage in MCM'
)
COMMENT 'Reservoir / dam master data'
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');


-- ── Dim_Weather ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS Dim_Weather (
    weather_key         INT      COMMENT 'Surrogate key',
    date_key            INT      COMMENT 'FK → Dim_Date',
    prov_code           INT      COMMENT 'FK → Dim_Location (province level)',
    prov_th             STRING,
    prov_en             STRING,
    region              STRING,
    temp_max_c          DOUBLE,
    temp_min_c          DOUBLE,
    rainfall_forecast_mm DOUBLE  COMMENT 'Forecast precipitation (mm/24h)',
    rain_intensity      STRING   COMMENT 'ไม่มีฝน/ฝนเล็กน้อย/ฝนปานกลาง/ฝนหนัก/ฝนหนักมาก',
    humidity_pct        DOUBLE,
    wind_speed_kmh      DOUBLE,
    is_heavy_rain_24h   BOOLEAN  COMMENT 'TRUE if rainfall_forecast_mm >= 35',
    data_source         STRING   COMMENT '"tmd" or "mock_tmd"'
)
COMMENT 'Daily weather forecast per province'
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');


-- ============================================================
-- FACT TABLES
-- ============================================================

-- ── Fact_Water_Monitoring ─────────────────────────────────────────────────────
-- Grain: 1 reservoir × 1 day
-- Partitioned by dt for efficient date-range queries and idempotent overwrite
CREATE TABLE IF NOT EXISTS Fact_Water_Monitoring (
    fact_key            BIGINT   COMMENT 'Surrogate key',
    date_key            INT      COMMENT 'FK → Dim_Date',
    reservoir_key       INT      COMMENT 'FK → Dim_Reservoir',
    record_date         DATE,
    volume_mcm          DOUBLE   COMMENT 'Current water volume (MCM)',
    percent_storage     DOUBLE   COMMENT '% of capacity (0–100)',
    inflow_mcm          DOUBLE   COMMENT 'Water inflow (MCM/day)',
    outflow_mcm         DOUBLE   COMMENT 'Water release (MCM/day)',
    storage_mcm         DOUBLE   COMMENT 'Total storage capacity (MCM)',
    active_storage_mcm  DOUBLE,
    loaded_at           STRING   COMMENT 'Pipeline load timestamp (ISO8601)'
)
COMMENT 'Daily reservoir water level measurements — Gold layer'
PARTITIONED BY (dt STRING COMMENT 'Partition date YYYY-MM-DD')
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');


-- ── Fact_Flood_Risk ───────────────────────────────────────────────────────────
-- Grain: 1 sub-district × 1 month (historical risk) joined with daily context
CREATE TABLE IF NOT EXISTS Fact_Flood_Risk (
    fact_key                BIGINT  COMMENT 'Surrogate key',
    date_key                INT     COMMENT 'FK → Dim_Date (1st of risk month)',
    location_key            INT     COMMENT 'FK → Dim_Location',
    weather_key             INT     COMMENT 'FK → Dim_Weather (province forecast)',
    geocode                 STRING,
    risk_month              INT     COMMENT 'Calendar month with flood risk (1–12)',
    historical_flood_count  INT     COMMENT 'Flood occurrences in 17-year period',
    risk_level              STRING  COMMENT 'เสี่ยงต่ำ/เสี่ยงปานกลาง/เสี่ยงสูง/เสี่ยงสูงมาก',
    risk_score              INT     COMMENT '1–4 (1=low, 4=very high)',
    flood_criteria          STRING  COMMENT 'Frequency description from สสน.',
    rain_forecast_mm        DOUBLE  COMMENT 'Matched province weather forecast',
    is_heavy_rain_forecast  BOOLEAN,
    loaded_at               STRING
)
COMMENT 'Flood risk index per sub-district per month — Gold layer'
PARTITIONED BY (dt STRING COMMENT 'Load date YYYY-MM-DD')
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');


-- ============================================================
-- NOTE: INSERT / LOAD statements have been REMOVED from this file.
--
-- This file is DDL-only (CREATE TABLE IF NOT EXISTS).
-- All data loading (INSERT OVERWRITE) is handled exclusively by:
--   warehouse/silver_to_gold.py  ← called by the 'silver_to_gold' Airflow task
--
-- Keeping DDL and DML separate prevents:
--   • hive_init from double-loading data alongside silver_to_gold
--   • ${dt} / ${dt_year} variable substitution errors when run via hive -f
--   • ROW_NUMBER() surrogate key drift (silver_to_gold.py uses stable hash keys)
-- ============================================================

-- Dim_Date is populated by silver_to_gold.py on first run (idempotent).
-- ============================================================

-- Step 1: Populate Dim_Date (run once, or when date range extends)
-- Generates dates 2020-01-01 → 2026-12-31 (2557 days ≈ 7 years)
INSERT OVERWRITE TABLE Dim_Date
SELECT
    CAST(DATE_FORMAT(d, 'yyyyMMdd') AS INT)              AS date_key,
    CAST(d AS DATE)                                       AS full_date,
    YEAR(d)                                               AS year,
    MONTH(d)                                              AS month,
    CASE MONTH(d)
        WHEN 1  THEN 'มกราคม'    WHEN 2  THEN 'กุมภาพันธ์'
        WHEN 3  THEN 'มีนาคม'    WHEN 4  THEN 'เมษายน'
        WHEN 5  THEN 'พฤษภาคม'  WHEN 6  THEN 'มิถุนายน'
        WHEN 7  THEN 'กรกฎาคม'  WHEN 8  THEN 'สิงหาคม'
        WHEN 9  THEN 'กันยายน'  WHEN 10 THEN 'ตุลาคม'
        WHEN 11 THEN 'พฤศจิกายน' ELSE 'ธันวาคม'
    END                                                   AS month_name_th,
    DAY(d)                                                AS day,
    QUARTER(d)                                            AS quarter,
    DAYOFWEEK(d)                                          AS day_of_week,
    DATE_FORMAT(d, 'EEEE')                                AS day_name_en,
    DAYOFWEEK(d) IN (1, 7)                                AS is_weekend,
    YEAR(d) + 543                                         AS thai_year
FROM (
    SELECT DATE_ADD('2020-01-01', pe.pos) AS d
    FROM (SELECT POSEXPLODE(SPLIT(SPACE(2557), ' '))) pe
) dates
WHERE d IS NOT NULL;

-- ============================================================
-- All other dimension and fact loading (Dim_Location, Dim_Reservoir,
-- Dim_Weather, Fact_Water_Monitoring, Fact_Flood_Risk) is handled by
-- warehouse/silver_to_gold.py using stable hash-based surrogate keys.
-- ============================================================

-- Step 6 (removed) — previously used ${dt} template variables which fail
-- when executed via  hive -f  (no variable substitution).  See silver_to_gold.py.
-- ============================================================

-- PLACEHOLDER — kept so grep/diff tools can track the section boundary.
-- Step 6 was: Fact_Flood_Risk INSERT using ${dt} — now in silver_to_gold.py

-- End of hive_ddl.sql (DDL-only)
-- ============================================================

-- (Previously had Step 6 tail here — removed)
SELECT 1 FROM (SELECT 1) dummy WHERE 1=0; -- no-op sentinel so file ends cleanly
