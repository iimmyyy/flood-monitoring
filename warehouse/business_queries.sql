-- ============================================================
-- Thailand Flood Risk Monitoring — Business HiveQL Queries
-- Database: flood_monitoring
-- ============================================================

USE flood_monitoring;


-- ============================================================
-- Query 1: จังหวัดที่มีอ่างเก็บน้ำเกิน 80% และพยากรณ์ฝนหนักในอีก 24 ชั่วโมง
-- Provinces with reservoirs > 80% capacity AND heavy rain forecast
-- ============================================================
SELECT
    dl.prov_th                              AS จังหวัด,
    dl.prov_en                              AS Province,
    dl.region                               AS ภาค,
    COUNT(DISTINCT dr.reservoir_id)         AS จำนวนอ่างเกิน80Pct,
    ROUND(AVG(fwm.percent_storage), 1)      AS เฉลี่ยPctStorage,
    ROUND(MAX(fwm.percent_storage), 1)      AS สูงสุดPctStorage,
    ROUND(dw.rainfall_forecast_mm, 1)       AS พยากรณ์ฝน_มม,
    dw.rain_intensity                       AS ความรุนแรงฝน,
    dd.full_date                            AS วันที่ข้อมูล
FROM Fact_Water_Monitoring fwm
JOIN Dim_Reservoir     dr  ON fwm.reservoir_key  = dr.reservoir_key
JOIN Dim_Date          dd  ON fwm.date_key        = dd.date_key
JOIN Dim_Location      dl  ON dr.region           = dl.region      -- match by region
JOIN Dim_Weather       dw  ON dl.prov_code        = dw.prov_code
                           AND fwm.date_key        = dw.date_key
WHERE fwm.percent_storage  > 80
  AND dw.is_heavy_rain_24h = TRUE
  AND fwm.dt = DATE_FORMAT(CURRENT_DATE(), 'yyyy-MM-dd')
GROUP BY
    dl.prov_th, dl.prov_en, dl.region,
    dw.rainfall_forecast_mm, dw.rain_intensity, dd.full_date
ORDER BY
    เฉลี่ยPctStorage DESC,
    พยากรณ์ฝน_มม DESC;


-- ============================================================
-- Query 2: Top 10 ตำบลที่มีประวัติน้ำท่วมสูงสุดในรอบ 17 ปี
-- Top 10 sub-districts by historical flood frequency (17 years)
-- ============================================================
SELECT
    dl.geocode                          AS รหัสตำบล,
    dl.tambon_th                        AS ตำบล,
    dl.amphoe_th                        AS อำเภอ,
    dl.prov_th                          AS จังหวัด,
    dl.region                           AS ภาค,
    MAX(ffr.historical_flood_count)     AS ครั้งน้ำท่วม17ปี,
    ffr.risk_level                      AS ระดับความเสี่ยง,
    ffr.flood_criteria                  AS เกณฑ์ความเสี่ยง,
    ROUND(AVG(dw.rainfall_forecast_mm), 1) AS เฉลี่ยฝนพยากรณ์_มม
FROM Fact_Flood_Risk ffr
JOIN Dim_Location  dl  ON ffr.location_key = dl.location_key
JOIN Dim_Weather   dw  ON dl.prov_code     = dw.prov_code
LEFT JOIN (
    -- Most recent heavy rain flag per province
    SELECT prov_code, is_heavy_rain_24h
    FROM Dim_Weather
    WHERE date_key = CAST(DATE_FORMAT(CURRENT_DATE(), 'yyyyMMdd') AS INT)
) latest_wx ON dl.prov_code = latest_wx.prov_code
GROUP BY
    dl.geocode, dl.tambon_th, dl.amphoe_th, dl.prov_th, dl.region,
    ffr.risk_level, ffr.flood_criteria
ORDER BY ครั้งน้ำท่วม17ปี DESC
LIMIT 10;


-- ============================================================
-- Query 3: แนวโน้มปริมาณน้ำรายเขื่อนในช่วง 30 วันที่ผ่านมา
-- 30-day water level trend per reservoir
-- ============================================================
SELECT
    dr.reservoir_id                         AS รหัสเขื่อน,
    dr.reservoir_name                       AS ชื่อเขื่อน,
    dr.reservoir_type                       AS ประเภท,
    dr.region                               AS ภาค,
    ROUND(dr.capacity_mcm, 2)               AS ความจุสูงสุด_ลบม,
    dd.full_date                            AS วันที่,
    ROUND(fwm.volume_mcm, 2)               AS ปริมาณน้ำ_ลบม,
    ROUND(fwm.percent_storage, 1)           AS Pct_Storage,
    ROUND(fwm.inflow_mcm, 2)               AS น้ำไหลเข้า_ลบม,
    ROUND(fwm.outflow_mcm, 2)              AS น้ำระบาย_ลบม,
    ROUND(fwm.inflow_mcm - fwm.outflow_mcm, 2) AS net_flow_mcm,
    -- 7-day rolling average of percent_storage
    ROUND(AVG(fwm.percent_storage) OVER (
        PARTITION BY fwm.reservoir_key
        ORDER BY dd.full_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 1)                                   AS MA7_PctStorage,
    -- trend direction (positive = filling, negative = draining)
    ROUND(fwm.percent_storage - LAG(fwm.percent_storage, 1) OVER (
        PARTITION BY fwm.reservoir_key
        ORDER BY dd.full_date
    ), 2)                                   AS daily_change_pct
FROM Fact_Water_Monitoring fwm
JOIN Dim_Reservoir dr ON fwm.reservoir_key = dr.reservoir_key
JOIN Dim_Date      dd ON fwm.date_key      = dd.date_key
WHERE dd.full_date >= DATE_SUB(CURRENT_DATE(), 30)
  AND dd.full_date <= CURRENT_DATE()
ORDER BY
    dr.reservoir_name ASC,
    dd.full_date       ASC;
