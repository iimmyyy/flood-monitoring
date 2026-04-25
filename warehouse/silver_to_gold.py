"""
silver_to_gold.py — Load Silver → Hive Gold Star Schema

Uses docker exec hive-server hive -e "..." (no network SASL issues).

Key design decisions to avoid MapReduce failures on small clusters:
  - NO ROW_NUMBER() OVER () window functions — replaced with CAST-based natural keys
  - All window functions would require 2 MR jobs; Hive2 + YARN 2GB can't handle that
  - location_key  = CAST(geocode AS INT)       — unique 6-digit sub-district code
  - weather_key   = prov_code                  — unique per province per overwrite
  - fact_key      = hash(concat(...)) AS BIGINT — hash of natural composite key

Run:
    python silver_to_gold.py [YYYY-MM-DD]
"""

import logging
import os
import subprocess
import sys
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [silver_to_gold] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

HIVE_CONTAINER = os.getenv("HIVE_CONTAINER", "hive-server")
HIVE_DB        = os.getenv("HIVE_DATABASE", "flood_monitoring")

# Hive SET statements applied at the start of every query
# These help Hive 2.x / MapReduce on small YARN clusters
HIVE_SETTINGS = """
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.vectorized.execution.enabled=false;
SET hive.vectorized.execution.reduce.enabled=false;
SET mapreduce.map.memory.mb=512;
SET mapreduce.reduce.memory.mb=512;
SET mapreduce.map.java.opts=-Xmx400m;
SET mapreduce.reduce.java.opts=-Xmx400m;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
"""


def run_hql(hql: str, desc: str = "") -> None:
    """Run HiveQL inside hive-server container via docker exec."""
    log.info("Running: %s", desc or hql[:80].replace("\n", " "))
    full_hql = HIVE_SETTINGS + "\n" + hql
    result = subprocess.run(
        ["docker", "exec", HIVE_CONTAINER, "hive", "-e", full_hql],
        capture_output=True,
        text=True,
        timeout=600,
    )
    if result.stdout:
        log.info(result.stdout[:800])
    if result.returncode != 0:
        # Show full stderr so the actual Hive error is visible
        log.error("HiveQL FAILED [%s]:\n%s", desc, result.stderr[:5000])
        raise RuntimeError(f"HiveQL failed: {desc}")
    log.info("OK: %s", desc)


def init_hive_schema() -> None:
    """
    Idempotently create the Silver external tables and Gold database.
    Must run before any INSERT OVERWRITE into Gold tables.
    """
    # Silver database + external tables
    run_hql(
        "CREATE DATABASE IF NOT EXISTS silver LOCATION '/silver';",
        "Create silver database",
    )
    run_hql("""
CREATE EXTERNAL TABLE IF NOT EXISTS silver.silver_reservoir (
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
""", "Create silver_reservoir external table")

    run_hql("""
CREATE EXTERNAL TABLE IF NOT EXISTS silver.silver_flood_risk (
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
""", "Create silver_flood_risk external table")

    run_hql("""
CREATE EXTERNAL TABLE IF NOT EXISTS silver.silver_weather (
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
""", "Create silver_weather external table")

    # Repair partitioned Silver tables so new Parquet files are visible
    run_hql("MSCK REPAIR TABLE silver.silver_reservoir;", "MSCK REPAIR silver_reservoir")
    run_hql("MSCK REPAIR TABLE silver.silver_weather;", "MSCK REPAIR silver_weather")

    # Gold database
    run_hql(
        f"CREATE DATABASE IF NOT EXISTS {HIVE_DB} LOCATION '/gold';",
        "Create gold database",
    )


def load_gold(dt: str) -> None:
    year  = dt.split("-")[0]
    month = dt.split("-")[1]

    # ── Initialise Hive schema (idempotent) ───────────────────────────────────
    init_hive_schema()

    # ── Dim_Reservoir ─────────────────────────────────────────────────────────
    # KEY DESIGN: use PMOD(ABS(HASH(...)), 2^30) as reservoir_key.
    # This is STABLE across pipeline re-runs — the same reservoir always gets
    # the same key — so Fact table FK references never go stale.
    # ROW_NUMBER() OVER() was the previous approach; it changes keys whenever
    # new reservoirs are added, silently breaking every Fact join.
    #
    # Aggregation: take MAX of slowly-changing attributes (name, region, owner)
    # and MAX(capacity_mcm) so the dimension reflects the most recent metadata.
    run_hql(f"""
USE {HIVE_DB};
INSERT OVERWRITE TABLE Dim_Reservoir
SELECT
    CAST(PMOD(ABS(HASH(CONCAT(reservoir_id, '_', source))), 1073741824) AS INT) AS reservoir_key,
    reservoir_id,
    MAX(reservoir_name)      AS reservoir_name,
    MAX(region)              AS region,
    MAX(owner)               AS owner,
    source                   AS reservoir_type,
    MAX(capacity_mcm)        AS capacity_mcm,
    MAX(dead_storage_mcm)    AS dead_storage_mcm
FROM silver.silver_reservoir
GROUP BY reservoir_id, source
""", "Load Dim_Reservoir (stable hash keys)")

    # ── Dim_Location ──────────────────────────────────────────────────────────
    # location_key = CAST(geocode AS INT)
    # Thai geocode is a guaranteed-unique 6-digit code → natural INT key.
    # No window function, no key drift.
    run_hql(f"""
USE {HIVE_DB};
INSERT OVERWRITE TABLE Dim_Location
SELECT
    CAST(geocode AS INT)             AS location_key,
    geocode,
    MAX(tambon_th)                   AS tambon_th,
    MAX(tambon_en)                   AS tambon_en,
    CAST(MAX(amphoe_code) AS INT)    AS amphoe_code,
    MAX(amphoe_th)                   AS amphoe_th,
    MAX(amphoe_en)                   AS amphoe_en,
    CAST(MAX(prov_code)   AS INT)    AS prov_code,
    MAX(prov_th)                     AS prov_th,
    MAX(prov_en)                     AS prov_en,
    CASE
        WHEN CAST(MAX(prov_code) AS INT) BETWEEN 10 AND 19 THEN 'C'
        WHEN CAST(MAX(prov_code) AS INT) BETWEEN 20 AND 27 THEN 'E'
        WHEN CAST(MAX(prov_code) AS INT) BETWEEN 30 AND 49 THEN 'NE'
        WHEN CAST(MAX(prov_code) AS INT) BETWEEN 50 AND 69 THEN 'N'
        WHEN CAST(MAX(prov_code) AS INT) BETWEEN 70 AND 79 THEN 'W'
        WHEN CAST(MAX(prov_code) AS INT) BETWEEN 80 AND 99 THEN 'S'
        ELSE 'Unknown'
    END                              AS region
FROM silver.silver_flood_risk
GROUP BY geocode
""", "Load Dim_Location (geocode natural key)")

    # ── Dim_Weather ───────────────────────────────────────────────────────────
    # weather_key = prov_code (stable natural key — unique per province per
    # INSERT OVERWRITE run). No window function needed.
    run_hql(f"""
USE {HIVE_DB};
INSERT OVERWRITE TABLE Dim_Weather
SELECT
    prov_code                                                          AS weather_key,
    CAST(DATE_FORMAT(CAST(forecast_date AS DATE), 'yyyyMMdd') AS INT) AS date_key,
    prov_code,
    prov_th,
    prov_en,
    region,
    temp_max_c,
    temp_min_c,
    rainfall_forecast_mm,
    rain_intensity,
    humidity_pct,
    wind_speed_kmh,
    is_heavy_rain_24h,
    data_source
FROM silver.silver_weather
WHERE dt = '{dt}'
""", "Load Dim_Weather (prov_code natural key)")

    # ── Fact_Water_Monitoring ─────────────────────────────────────────────────
    # fact_key = ABS(HASH(reservoir_id + record_date)) — stable, no window func.
    # percent_storage: use Silver value (now reliably computed from volume/capacity
    # by bronze_to_silver.py). Guard with CASE for any residual zeros.
    run_hql(f"""
USE {HIVE_DB};
INSERT OVERWRITE TABLE Fact_Water_Monitoring
PARTITION (dt='{dt}')
SELECT
    CAST(ABS(HASH(CONCAT(sr.reservoir_id, '_', CAST(sr.record_date AS STRING)))) AS BIGINT)
                                                                   AS fact_key,
    CAST(DATE_FORMAT(CAST(sr.record_date AS DATE), 'yyyyMMdd') AS INT)
                                                                   AS date_key,
    dr.reservoir_key,
    CAST(sr.record_date AS DATE)                                   AS record_date,
    sr.volume_mcm,
    -- Recompute percent_storage if Silver still has 0 but capacity is valid
    CASE
        WHEN sr.percent_storage = 0 AND sr.capacity_mcm > 0
        THEN ROUND(sr.volume_mcm / sr.capacity_mcm * 100.0, 2)
        ELSE sr.percent_storage
    END                                                            AS percent_storage,
    sr.inflow_mcm,
    sr.outflow_mcm,
    sr.capacity_mcm                                                AS storage_mcm,
    sr.active_storage_mcm,
    sr.silver_loaded_at                                            AS loaded_at
FROM silver.silver_reservoir sr
JOIN Dim_Reservoir dr
    ON  sr.reservoir_id = dr.reservoir_id
    AND sr.source       = dr.reservoir_type
WHERE sr.dt = '{dt}'
""", "Load Fact_Water_Monitoring")

    # ── Fact_Flood_Risk ───────────────────────────────────────────────────────
    # fact_key = ABS(HASH(geocode + month)) — stable, no window func.
    # JOIN condition: flood_risk month matches pipeline run month so daily runs
    # only refresh the current month's risk partition.
    run_hql(f"""
USE {HIVE_DB};
INSERT OVERWRITE TABLE Fact_Flood_Risk
PARTITION (dt='{dt}')
SELECT
    CAST(ABS(HASH(CONCAT(sfr.geocode, '_', CAST(sfr.month AS STRING)))) AS BIGINT)
                                                                   AS fact_key,
    CAST(DATE_FORMAT(
        CAST(CONCAT('{year}', '-', LPAD(CAST(sfr.month AS STRING), 2, '0'), '-01') AS DATE),
        'yyyyMMdd') AS INT)                                        AS date_key,
    dl.location_key,
    dw.weather_key,
    sfr.geocode,
    sfr.month                                                      AS risk_month,
    sfr.flood_count_17yr                                           AS historical_flood_count,
    sfr.risk_level,
    sfr.risk_score,
    sfr.flood_criteria,
    dw.rainfall_forecast_mm                                        AS rain_forecast_mm,
    dw.is_heavy_rain_24h                                           AS is_heavy_rain_forecast,
    CURRENT_TIMESTAMP()                                            AS loaded_at
FROM silver.silver_flood_risk sfr
JOIN Dim_Location dl
    ON  sfr.geocode  = dl.geocode
JOIN Dim_Weather dw
    ON  dl.prov_code = dw.prov_code
WHERE sfr.month = CAST('{month}' AS INT)
""", "Load Fact_Flood_Risk")

    log.info("silver_to_gold complete for dt=%s", dt)


if __name__ == "__main__":
    dt = sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime("%Y-%m-%d")
    load_gold(dt)

