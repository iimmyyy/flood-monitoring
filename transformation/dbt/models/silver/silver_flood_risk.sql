-- Silver model: clean flood risk area records
-- Source: HDFS Bronze CSV loaded as Hive external table

{{ config(
    materialized='table',
    schema='silver'
) }}

WITH raw AS (
    SELECT *
    FROM bronze_flood_risk
),

cleaned AS (
    SELECT
        LPAD(CAST(geocode AS STRING), 6, '0')   AS geocode,
        CAST(month AS INT)                        AS month,
        tambon_th,
        tambon_en,
        CAST(amphoe_code AS INT)                  AS amphoe_code,
        amphoe_th,
        amphoe_en,
        CAST(prov_code AS INT)                    AS prov_code,
        prov_th,
        prov_en,
        COALESCE(CAST(flood_count_17yr AS INT), 0) AS flood_count_17yr,
        flood_criteria,
        risk_level,
        CASE risk_level
            WHEN 'เสี่ยงต่ำ'       THEN 1
            WHEN 'เสี่ยงปานกลาง'   THEN 2
            WHEN 'เสี่ยงสูง'        THEN 3
            WHEN 'เสี่ยงสูงมาก'    THEN 4
            ELSE 0
        END                                       AS risk_score
    FROM raw
    WHERE geocode IS NOT NULL
      AND month BETWEEN 1 AND 12
)

SELECT
    geocode,
    month,
    tambon_th,
    tambon_en,
    amphoe_code,
    amphoe_th,
    amphoe_en,
    prov_code,
    prov_th,
    prov_en,
    flood_count_17yr,
    flood_criteria,
    risk_level,
    risk_score,
    CURRENT_TIMESTAMP AS silver_loaded_at
FROM cleaned
