-- Silver model: clean mock weather forecast records

{{ config(
    materialized='table',
    schema='silver'
) }}

WITH raw AS (
    SELECT *
    FROM bronze_weather
),

cleaned AS (
    SELECT
        CAST(prov_code AS INT)                           AS prov_code,
        prov_th,
        prov_en,
        region,
        CAST(forecast_date AS DATE)                      AS forecast_date,
        COALESCE(CAST(temp_max_c AS DOUBLE), 0.0)        AS temp_max_c,
        COALESCE(CAST(temp_min_c AS DOUBLE), 0.0)        AS temp_min_c,
        COALESCE(CAST(rainfall_forecast_mm AS DOUBLE), 0.0) AS rainfall_forecast_mm,
        rain_intensity,
        COALESCE(CAST(humidity_pct AS DOUBLE), 0.0)      AS humidity_pct,
        COALESCE(CAST(wind_speed_kmh AS DOUBLE), 0.0)    AS wind_speed_kmh,
        CAST(is_heavy_rain_24h AS BOOLEAN)               AS is_heavy_rain_24h,
        data_source,
        dt,
        ROW_NUMBER() OVER (
            PARTITION BY prov_code, forecast_date
            ORDER BY generated_at DESC
        ) AS rn
    FROM raw
    WHERE prov_code IS NOT NULL
      AND forecast_date IS NOT NULL
)

SELECT
    prov_code,
    prov_th,
    prov_en,
    region,
    forecast_date,
    temp_max_c,
    temp_min_c,
    rainfall_forecast_mm,
    rain_intensity,
    humidity_pct,
    wind_speed_kmh,
    is_heavy_rain_24h,
    data_source,
    dt,
    CURRENT_TIMESTAMP AS silver_loaded_at
FROM cleaned
WHERE rn = 1
