-- ============================================================
-- Thailand Flood Alert — Hive DDL additions
-- Creates:
--   1. silver.silver_reservoir_features  (external Silver table)
--   2. flood_monitoring.Fact_Flood_Alert (Gold fact table)
-- ============================================================

USE silver;

-- ── Silver external table: reservoir features ─────────────────────────────────
-- Populated by transformation/compute_reservoir_features.py
CREATE EXTERNAL TABLE IF NOT EXISTS silver_reservoir_features (
    feature_date        STRING   COMMENT 'Date features were computed for (YYYY-MM-DD)',
    reservoir_id        STRING,
    reservoir_name      STRING,
    source              STRING   COMMENT '"dam" or "reservoir"',
    region              STRING,
    capacity_mcm        DOUBLE,
    volume_mcm          DOUBLE,
    percent_storage     DOUBLE,
    inflow_mcm          DOUBLE,
    outflow_mcm         DOUBLE,
    net_flow_mcm        DOUBLE   COMMENT 'inflow - outflow (MCM/day)',
    storage_trend_7d    DOUBLE   COMMENT 'Average % storage over 7 days',
    storage_delta_1d    DOUBLE   COMMENT 'Change in % storage vs. yesterday (pp)',
    days_to_full        DOUBLE   COMMENT 'Days until full at current net flow; NaN if flow ≈ 0',
    reservoir_status    STRING   COMMENT 'CRITICAL/HIGH/NORMAL/LOW',
    filling_rate        STRING   COMMENT 'FAST/MODERATE/STABLE/DRAINING',
    computed_at         STRING   COMMENT 'ISO8601 timestamp'
)
COMMENT 'Reservoir rolling features — computed by compute_reservoir_features.py'
PARTITIONED BY (dt STRING COMMENT 'Feature date partition YYYY-MM-DD')
STORED AS PARQUET
LOCATION '/silver/reservoir_features'
TBLPROPERTIES ('parquet.compress'='SNAPPY');

MSCK REPAIR TABLE silver_reservoir_features;


-- ── Gold fact table: Flood Alert ──────────────────────────────────────────────
USE flood_monitoring;

CREATE TABLE IF NOT EXISTS Fact_Flood_Alert (
    alert_key                   BIGINT   COMMENT 'Surrogate key',
    date_key                    INT      COMMENT 'FK → Dim_Date',
    location_key                INT      COMMENT 'FK → Dim_Location (province)',
    weather_key                 INT      COMMENT 'FK → Dim_Weather',
    alert_date                  DATE     COMMENT 'Date of the alert',
    prov_code                   INT      COMMENT 'Province code (2-digit)',
    prov_th                     STRING   COMMENT 'Province name (Thai)',
    alert_level                 STRING   COMMENT 'ปกติ / เฝ้าระวัง / เตือนภัย / วิกฤต',
    alert_score                 DOUBLE   COMMENT 'Composite score 0–100',
    reservoir_score             DOUBLE   COMMENT 'Upstream reservoir component (0–40)',
    weather_score               DOUBLE   COMMENT 'Rainfall forecast component (0–35)',
    historical_risk_score       DOUBLE   COMMENT 'Historical flood risk component (0–25)',
    trigger_reservoirs          STRING   COMMENT 'Names of dams in critical/high status',
    trigger_reservoir_statuses  STRING   COMMENT 'Statuses matching trigger_reservoirs',
    max_reservoir_pct           DOUBLE   COMMENT 'Highest % storage among upstream dams',
    rainfall_forecast_mm        DOUBLE   COMMENT 'Province 24h rainfall forecast',
    historical_risk_level       STRING   COMMENT 'เสี่ยงต่ำ/เสี่ยงปานกลาง/เสี่ยงสูง/เสี่ยงสูงมาก',
    affected_subdistricts_count INT      COMMENT 'Number of sub-districts in province at ≥ moderate risk',
    loaded_at                   STRING   COMMENT 'ISO8601 pipeline load timestamp'
)
COMMENT 'Daily flood alert level per province — Gold layer'
PARTITIONED BY (dt STRING COMMENT 'Alert date YYYY-MM-DD')
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');
