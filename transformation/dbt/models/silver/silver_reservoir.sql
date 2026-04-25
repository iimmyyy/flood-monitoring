-- Silver model: clean reservoir + dam records from Hive external table
-- Deduplicates on (reservoir_id, record_date, source) and casts types

{{ config(
    materialized='table',
    schema='silver'
) }}

WITH raw AS (
    SELECT *
    FROM bronze_reservoir
),

deduped AS (
    SELECT
        reservoir_id,
        reservoir_name,
        region,
        source,
        owner,
        CAST(record_date AS DATE)             AS record_date,
        COALESCE(CAST(capacity_mcm   AS DOUBLE), 0.0) AS capacity_mcm,
        COALESCE(CAST(volume_mcm     AS DOUBLE), 0.0) AS volume_mcm,
        CASE
            WHEN CAST(percent_storage AS DOUBLE) < 0   THEN 0.0
            WHEN CAST(percent_storage AS DOUBLE) > 100 THEN 100.0
            ELSE COALESCE(CAST(percent_storage AS DOUBLE), 0.0)
        END                                           AS percent_storage,
        COALESCE(CAST(inflow_mcm     AS DOUBLE), 0.0) AS inflow_mcm,
        COALESCE(CAST(outflow_mcm    AS DOUBLE), 0.0) AS outflow_mcm,
        COALESCE(CAST(dead_storage_mcm AS DOUBLE), 0.0) AS dead_storage_mcm,
        fetched_at,
        dt,
        ROW_NUMBER() OVER (
            PARTITION BY reservoir_id, record_date, source
            ORDER BY fetched_at DESC
        ) AS rn
    FROM raw
    WHERE reservoir_id IS NOT NULL
      AND reservoir_id <> ''
)

SELECT
    reservoir_id,
    reservoir_name,
    region,
    source,
    owner,
    record_date,
    capacity_mcm,
    volume_mcm,
    percent_storage,
    inflow_mcm,
    outflow_mcm,
    dead_storage_mcm,
    fetched_at,
    dt,
    CURRENT_TIMESTAMP AS silver_loaded_at
FROM deduped
WHERE rn = 1
