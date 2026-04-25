# Thailand Flood Risk Monitoring — Data Warehouse Architecture

ระบบใช้ **Medallion Architecture** (Bronze → Silver → Gold) บน HDFS โดยมี Apache Hive เป็น metastore, Apache Kafka เป็น message bus, และ Apache Airflow เป็น orchestrator

---

## Infrastructure Stack

| Component | Image / Version | Role |
|-----------|----------------|------|
| HDFS NameNode | `bde2020/hadoop-namenode:2.0.0-hadoop3.2.1` | Distributed storage |
| HDFS DataNode | `bde2020/hadoop-datanode:2.0.0-hadoop3.2.1` | Block storage |
| Apache Hive | `bde2020/hive:2.3.2-postgresql-metastore` | SQL query engine + metastore |
| PostgreSQL | (Hive metastore backend) | Schema registry |
| Apache Kafka | `confluentinc/cp-kafka` | Message bus |
| Apache Airflow | `apache/airflow:2.x` + LocalExecutor | Pipeline orchestration |
| Python 3.x | pyarrow, pandas, hdfs, confluent-kafka | Transform + scoring |

---

## Data Sources

| Source | API / File | Protocol | Partition |
|--------|-----------|----------|-----------|
| RID Dam API | `https://app.rid.go.th/reservoir/api/dam/public[/YYYY-MM-DD]` | REST → Kafka | รายวัน |
| RID Reservoir API | `https://app.rid.go.th/reservoir/api/reservoir/public[/YYYY-MM-DD]` | REST → Kafka | รายวัน |
| TMD Weather NWP | TMD API (mock fallback รายจังหวัดตามปฏิทินมรสุม) | REST → HDFS | รายวัน |
| Flood Risk Static | `flood_risk_area.csv` (สสน. 17-year historical, 7,356 ตำบล) | CSV → HDFS | ครั้งเดียว |
| Downstream Map | `reservoir_downstream_map.csv` (90 อ่าง → 77 จังหวัด) | CSV → local + HDFS | Static |

---

## Pipeline DAG — Task Flow

```
ingest_static ──┐
ingest_kafka    ├──► bronze_to_silver ──► hive_init ──► silver_to_gold ──► compute_features
generate_weather┘                                                                │
                                                                                 ▼
                                                                          flood_alert
                                                                                 │
                                                                                 ▼
                                                                      data_quality_check
                                                                                 │
                                                                                 ▼
                                                                        export_dashboard
```

Schedule: ทุก 6 ชั่วโมง (`0 */6 * * *`) · Retry: 2 ครั้ง · Delay: 5 นาที

---

## Bronze Layer — Raw Ingestion

> **จุดประสงค์:** เก็บข้อมูลดิบจาก external sources โดยไม่แปลงอะไร  
> **Format:** NDJSON (reservoir/dam/weather), CSV (flood_risk)  
> **Storage:** HDFS  
> **Partition:** `/bronze/{domain}/dt=YYYY-MM-DD/`

### HDFS Paths

```
/bronze/
├── dam/
│   └── dt=YYYY-MM-DD/
│       └── dam_YYYYMMDD_HHMMSS.json        ← NDJSON, 1 record per line
├── reservoir/
│   └── dt=YYYY-MM-DD/
│       └── reservoir_YYYYMMDD_HHMMSS.json  ← NDJSON, 1 record per line
├── weather/
│   └── dt=YYYY-MM-DD/
│       └── weather_YYYYMMDD_HHMMSS.json    ← NDJSON, 77 จังหวัด
├── flood_risk/
│   └── flood_risk_area.csv                 ← static, overwrite
└── static/
    └── reservoir_downstream_map.csv        ← static reference
```

### Raw Record Schema — Dam / Reservoir (Bronze)

| Field | Type | Source | หมายเหตุ |
|-------|------|--------|----------|
| `reservoir_id` | string | RID API | `dam01`–`damN` หรือ `rsv001`–`rsvN` |
| `reservoir_name` | string | RID API | ชื่อภาษาไทย |
| `source` | string | producer | `"dam"` หรือ `"reservoir"` |
| `region` | string | RID API | ภาค (ภาคเหนือ, ภาคกลาง, ...) |
| `record_date` | string | RID API | `YYYY-MM-DD` |
| `capacity_mcm` | float | RID API | ความจุปกติ (ล้านลูกบาศก์เมตร) |
| `volume_mcm` | float | RID API | ปริมาณน้ำปัจจุบัน |
| `percent_storage` | float | RID API | อาจเป็น 0 ถ้า capacity_mcm = 0 |
| `inflow_mcm` | float | RID API | น้ำไหลเข้า |
| `outflow_mcm` | float | RID API | น้ำไหลออก |
| `fetched_at` | string | producer | ISO 8601 timestamp |

### Kafka Topic

```
Topic:          reservoir_updates
Partitions:     3
Replication:    1
Bootstrap:      kafka:29092
Consumer group: flood-monitoring-group
Watermark file: /opt/airflow/data/.watermark.json   ← กัน duplicate ข้ามรอบ
Producer state: /opt/airflow/data/.producer_state.json ← backfill tracking
```

---

## Silver Layer — Cleaned & Typed

> **จุดประสงค์:** Clean, type-cast, deduplicate, เติม null  
> **Format:** Parquet (Snappy compression)  
> **Storage:** HDFS  
> **Hive DB:** `silver`  
> **Partition:** `/silver/{domain}/dt=YYYY-MM-DD/`

### silver.silver_reservoir

```
HDFS: /silver/reservoir/dt=YYYY-MM-DD/reservoir.parquet
Hive: EXTERNAL TABLE, PARTITIONED BY (dt STRING), STORED AS PARQUET
```

| Column | Type | คำอธิบาย |
|--------|------|---------|
| `reservoir_id` | STRING | PK จาก RID (e.g. `dam01`, `rsv239`) |
| `reservoir_name` | STRING | ชื่อภาษาไทย |
| `region` | STRING | ภาค |
| `source` | STRING | `dam` / `reservoir` |
| `owner` | STRING | หน่วยงานดูแล (ถ้ามี) |
| `record_date` | STRING | วันที่ข้อมูล |
| `capacity_mcm` | DOUBLE | ความจุอ่าง |
| `volume_mcm` | DOUBLE | ปริมาณน้ำ |
| `percent_storage` | DOUBLE | % เต็ม (คำนวณใหม่ถ้า API ส่ง 0) |
| `inflow_mcm` | DOUBLE | น้ำไหลเข้า |
| `outflow_mcm` | DOUBLE | น้ำไหลออก |
| `active_storage_mcm` | DOUBLE | ส่วนที่ใช้งานได้ |
| `dead_storage_mcm` | DOUBLE | ส่วน dead storage |
| `fetched_at` | STRING | เวลา fetch จาก API |
| `silver_loaded_at` | STRING | เวลา load เข้า Silver |
| **`dt`** | STRING | **Partition key** (YYYY-MM-DD) |

### silver.silver_weather

```
HDFS: /silver/weather/dt=YYYY-MM-DD/weather.parquet
Hive: EXTERNAL TABLE, PARTITIONED BY (dt STRING), STORED AS PARQUET
```

| Column | Type | คำอธิบาย |
|--------|------|---------|
| `prov_code` | INT | รหัสจังหวัด (10–96) |
| `prov_th` | STRING | ชื่อจังหวัดภาษาไทย |
| `prov_en` | STRING | ชื่อจังหวัดภาษาอังกฤษ |
| `region` | STRING | ภาค |
| `forecast_date` | STRING | วันที่พยากรณ์ |
| `temp_max_c` | DOUBLE | อุณหภูมิสูงสุด (°C) |
| `temp_min_c` | DOUBLE | อุณหภูมิต่ำสุด (°C) |
| `rainfall_forecast_mm` | DOUBLE | ปริมาณฝนพยากรณ์ 24 ชม. (มม.) |
| `rain_intensity` | STRING | ระดับฝน (ฝนเล็กน้อย / ปานกลาง / หนัก / หนักมาก) |
| `humidity_pct` | DOUBLE | ความชื้นสัมพัทธ์ (%) |
| `wind_speed_kmh` | DOUBLE | ความเร็วลม (กม./ชม.) |
| `is_heavy_rain_24h` | BOOLEAN | ฝนตกหนักใน 24 ชม. |
| `data_source` | STRING | `"tmd_api"` / `"mock"` |
| `silver_loaded_at` | STRING | เวลา load เข้า Silver |
| **`dt`** | STRING | **Partition key** (YYYY-MM-DD) |

### silver.silver_flood_risk

```
HDFS: /silver/flood_risk/flood_risk.parquet
Hive: EXTERNAL TABLE (ไม่มี partition — static dataset), STORED AS PARQUET
```

| Column | Type | คำอธิบาย |
|--------|------|---------|
| `geocode` | STRING | รหัสตำบล 6 หลัก (unique key) |
| `month` | INT | เดือน (1–12) |
| `tambon_th` | STRING | ชื่อตำบลภาษาไทย |
| `tambon_en` | STRING | ชื่อตำบลภาษาอังกฤษ |
| `amphoe_code` | INT | รหัสอำเภอ |
| `amphoe_th` | STRING | ชื่ออำเภอภาษาไทย |
| `amphoe_en` | STRING | ชื่ออำเภอภาษาอังกฤษ |
| `prov_code` | INT | รหัสจังหวัด |
| `prov_th` | STRING | ชื่อจังหวัดภาษาไทย |
| `prov_en` | STRING | ชื่อจังหวัดภาษาอังกฤษ |
| `flood_count_17yr` | INT | จำนวนปีที่เกิดน้ำท่วมใน 17 ปี |
| `flood_criteria` | STRING | เกณฑ์การจัดระดับ |
| `risk_level` | STRING | เสี่ยงสูง / เสี่ยงปานกลาง / เสี่ยงต่ำ |
| `risk_score` | INT | คะแนนความเสี่ยง (1–4) |
| `silver_loaded_at` | STRING | เวลา load เข้า Silver |

### silver.silver_reservoir_features  *(computed)*

```
HDFS: /silver/reservoir_features/dt=YYYY-MM-DD/features.parquet
Hive: EXTERNAL TABLE, PARTITIONED BY (dt STRING), STORED AS PARQUET
สร้างโดย: compute_reservoir_features.py (rolling 7-day window)
```

| Column | Type | คำอธิบาย |
|--------|------|---------|
| `reservoir_id` | STRING | FK → silver_reservoir |
| `reservoir_name` | STRING | ชื่ออ่าง |
| `record_date` | STRING | วันที่คำนวณ |
| `region` | STRING | ภาค |
| `capacity_mcm` | DOUBLE | ความจุอ่าง |
| `volume_mcm` | DOUBLE | ปริมาณน้ำวันนี้ |
| `percent_storage` | DOUBLE | % เต็ม |
| `inflow_mcm` | DOUBLE | น้ำไหลเข้า |
| `outflow_mcm` | DOUBLE | น้ำไหลออก |
| `net_flow_mcm` | DOUBLE | `inflow − outflow` |
| `storage_trend_7d` | DOUBLE | เปลี่ยนแปลง % ใน 7 วัน |
| `storage_delta_1d` | DOUBLE | เปลี่ยนแปลง % ใน 1 วัน |
| `days_to_full` | DOUBLE | วันที่คาดว่าน้ำเต็ม (ถ้า trend > 0) |
| `reservoir_status` | STRING | `CRITICAL` / `HIGH` / `NORMAL` / `LOW` |
| `filling_rate` | STRING | `FAST` / `MODERATE` / `STABLE` / `DRAINING` |
| **`dt`** | STRING | **Partition key** (YYYY-MM-DD) |

**Status thresholds:**

| Status | เงื่อนไข |
|--------|---------|
| `CRITICAL` | percent_storage ≥ 85% |
| `HIGH` | percent_storage ≥ 70% |
| `NORMAL` | percent_storage ≥ 30% |
| `LOW` | percent_storage < 30% |

---

## Gold Layer — Star Schema + Alert

> **จุดประสงค์:** Analytical layer สำหรับ reporting และ alert  
> **Format:** ORC (Dimension tables) / Parquet (Fact_Water_Monitoring, Fact_Flood_Risk) / ORC (Fact_Flood_Alert)  
> **Storage:** HDFS `/gold/`  
> **Hive DB:** `flood_monitoring`

### Star Schema Overview

```
                    ┌──────────────────┐
                    │   Dim_Date       │
                    │  (date_key INT)  │
                    └────────┬─────────┘
                             │
         ┌───────────────────┼───────────────────┐
         │                   │                   │
┌────────┴────────┐ ┌────────┴────────┐ ┌────────┴────────┐
│  Dim_Reservoir  │ │  Dim_Location   │ │   Dim_Weather   │
│ (reservoir_key) │ │ (location_key)  │ │  (weather_key)  │
└────────┬────────┘ └────────┬────────┘ └────────┬────────┘
         │                   │                   │
         ▼                   ▼                   ▼
┌─────────────────────────────────────────────────────────┐
│              Fact_Water_Monitoring                       │
│         (fact_key, date_key, reservoir_key, dt)         │
└─────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────┐
│                  Fact_Flood_Risk                         │
│    (fact_key, date_key, location_key, weather_key, dt)  │
└─────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────┐
│                  Fact_Flood_Alert        ← Gold ORC      │
│           (alert_key, prov_code, dt)                    │
└─────────────────────────────────────────────────────────┘
```

---

### Dim_Reservoir

```
HDFS: /gold/flood_monitoring/Dim_Reservoir/
Key: reservoir_key = PMOD(ABS(HASH(reservoir_id + '_' + source)), 2^30)
     → stable hash key ไม่เปลี่ยนเมื่อเพิ่มอ่างใหม่
```

| Column | Type | คำอธิบาย |
|--------|------|---------|
| `reservoir_key` | INT | Surrogate key (stable hash) |
| `reservoir_id` | STRING | Natural key จาก RID |
| `reservoir_name` | STRING | ชื่ออ่าง |
| `region` | STRING | ภาค |
| `owner` | STRING | หน่วยงาน |
| `reservoir_type` | STRING | `dam` / `reservoir` |
| `capacity_mcm` | DOUBLE | ความจุสูงสุด (ล้าน ลบ.ม.) |
| `dead_storage_mcm` | DOUBLE | Dead storage |

---

### Dim_Location

```
HDFS: /gold/flood_monitoring/Dim_Location/
Key: location_key = CAST(geocode AS INT)
     → geocode 6 หลักของกรมส่งเสริมการปกครอง ไม่ซ้ำในระดับตำบล
```

| Column | Type | คำอธิบาย |
|--------|------|---------|
| `location_key` | INT | Natural key (geocode) |
| `geocode` | STRING | รหัสตำบล 6 หลัก |
| `tambon_th` / `tambon_en` | STRING | ชื่อตำบล |
| `amphoe_code` | INT | รหัสอำเภอ |
| `amphoe_th` / `amphoe_en` | STRING | ชื่ออำเภอ |
| `prov_code` | INT | รหัสจังหวัด (10–96) |
| `prov_th` / `prov_en` | STRING | ชื่อจังหวัด |
| `region` | STRING | C / E / NE / N / W / S |

---

### Dim_Weather

```
HDFS: /gold/flood_monitoring/Dim_Weather/
Key: weather_key = prov_code
     → INSERT OVERWRITE ทุกรอบ → ข้อมูลล่าสุดต่อจังหวัด
```

| Column | Type | คำอธิบาย |
|--------|------|---------|
| `weather_key` | INT | = prov_code |
| `date_key` | INT | YYYYMMDD |
| `prov_code` | INT | รหัสจังหวัด |
| `prov_th` / `prov_en` | STRING | ชื่อจังหวัด |
| `region` | STRING | ภาค |
| `temp_max_c` / `temp_min_c` | DOUBLE | อุณหภูมิ (°C) |
| `rainfall_forecast_mm` | DOUBLE | ฝนพยากรณ์ (มม./24 ชม.) |
| `rain_intensity` | STRING | ระดับฝน |
| `humidity_pct` | DOUBLE | ความชื้น (%) |
| `wind_speed_kmh` | DOUBLE | ความเร็วลม |
| `is_heavy_rain_24h` | BOOLEAN | ฝนหนักหรือไม่ |
| `data_source` | STRING | `tmd_api` / `mock` |

---

### Fact_Water_Monitoring

```
HDFS: /gold/flood_monitoring/Fact_Water_Monitoring/dt=YYYY-MM-DD/
Partition: dt STRING
Key: fact_key = ABS(HASH(reservoir_id + '_' + record_date))
```

| Column | Type | คำอธิบาย |
|--------|------|---------|
| `fact_key` | BIGINT | Surrogate key |
| `date_key` | INT | FK → Dim_Date (YYYYMMDD) |
| `reservoir_key` | INT | FK → Dim_Reservoir |
| `record_date` | DATE | วันที่วัด |
| `volume_mcm` | DOUBLE | ปริมาณน้ำ |
| `percent_storage` | DOUBLE | % เต็ม |
| `inflow_mcm` | DOUBLE | น้ำไหลเข้า |
| `outflow_mcm` | DOUBLE | น้ำไหลออก |
| `storage_mcm` | DOUBLE | = capacity_mcm |
| `active_storage_mcm` | DOUBLE | ปริมาณน้ำที่ใช้งานได้ |
| `loaded_at` | STRING | เวลา load |
| **`dt`** | STRING | **Partition key** |

---

### Fact_Flood_Risk

```
HDFS: /gold/flood_monitoring/Fact_Flood_Risk/dt=YYYY-MM-DD/
Partition: dt STRING
Key: fact_key = ABS(HASH(geocode + '_' + month))
```

| Column | Type | คำอธิบาย |
|--------|------|---------|
| `fact_key` | BIGINT | Surrogate key |
| `date_key` | INT | FK → Dim_Date |
| `location_key` | INT | FK → Dim_Location (geocode) |
| `weather_key` | INT | FK → Dim_Weather (prov_code) |
| `geocode` | STRING | รหัสตำบล |
| `risk_month` | INT | เดือนที่วิเคราะห์ |
| `historical_flood_count` | INT | จำนวนปีที่เคยท่วมใน 17 ปี |
| `risk_level` | STRING | เสี่ยงสูง / ปานกลาง / ต่ำ |
| `risk_score` | INT | 1–4 |
| `flood_criteria` | STRING | เกณฑ์ |
| `rain_forecast_mm` | DOUBLE | ฝนพยากรณ์จาก Dim_Weather |
| `is_heavy_rain_forecast` | BOOLEAN | ฝนหนักหรือไม่ |
| `loaded_at` | STRING | เวลา load |
| **`dt`** | STRING | **Partition key** |

---

### Fact_Flood_Alert  *(Gold ORC — computed by scoring engine)*

```
HDFS: /gold/Fact_Flood_Alert/dt=YYYY-MM-DD/flood_alerts.orc
Format: ORC (เขียนโดย flood_alert_scoring.py ผ่าน WebHDFS โดยตรง)
Grain: 1 row ต่อ 1 จังหวัด ต่อ 1 วัน (77 rows/day)
```

| Column | Type | คำอธิบาย |
|--------|------|---------|
| `alert_key` | INT | Sequential key |
| `date_key` | INT | YYYYMMDD |
| `location_key` | INT | = prov_code (placeholder FK) |
| `weather_key` | INT | = prov_code (placeholder FK) |
| `alert_date` | STRING | วันที่ประเมิน |
| `prov_code` | INT | รหัสจังหวัด |
| `prov_th` | STRING | ชื่อจังหวัด |
| `alert_level` | STRING | ปกติ / เฝ้าระวัง / เตือนภัย / วิกฤต |
| `alert_score` | DOUBLE | คะแนนรวม (0–100) |
| `reservoir_score` | DOUBLE | คะแนนส่วนอ่าง (0–40) |
| `weather_score` | DOUBLE | คะแนนส่วนฝน (0–35) |
| `historical_risk_score` | DOUBLE | คะแนนส่วนประวัติ (0–25) |
| `trigger_reservoirs` | STRING | ชื่ออ่างที่ trigger (top 3, comma-sep) |
| `trigger_reservoir_statuses` | STRING | สถานะอ่าง trigger (comma-sep) |
| `max_reservoir_pct` | DOUBLE | % เต็มของอ่างที่วิกฤตที่สุด |
| `rainfall_forecast_mm` | DOUBLE | ฝนพยากรณ์ |
| `historical_risk_level` | STRING | ระดับความเสี่ยงประวัติศาสตร์ |
| `affected_subdistricts_count` | INT | จำนวนตำบลที่เสี่ยง |
| `loaded_at` | STRING | เวลา load |

---

## Alert Scoring Engine

`flood_alert_scoring.py` คำนวณ `alert_score` จาก 3 components:

```
alert_score (0–100) = reservoir_score + weather_score + historical_risk_score
```

### Reservoir Score (0–40)

อ้างอิงจาก `reservoir_downstream_map.csv` (90 entries, ครอบ 77 จังหวัด)  
ใช้อ่างที่วิกฤตที่สุด upstream ของจังหวัดนั้น:

| reservoir_status | Base Score | + filling_rate bonus |
|-----------------|-----------|---------------------|
| `CRITICAL` | 30 | `FAST` +10, `MODERATE` +5, `STABLE` ±0, `DRAINING` −5 |
| `HIGH` | 18 | เช่นเดียวกัน |
| `NORMAL` | 5 | เช่นเดียวกัน |
| `LOW` | 0 | เช่นเดียวกัน |

### Weather Score (0–35)

| rainfall_forecast_mm | Score |
|---------------------|-------|
| ≥ 90 mm (ฝนหนักมากพิเศษ) | 35 |
| ≥ 35 mm (ฝนหนัก) | 25 |
| ≥ 10 mm (ฝนปานกลาง) | 15 |
| ≥ 1 mm (ฝนเล็กน้อย) | 5 |
| < 1 mm | 0 |

### Historical Risk Score (0–25)

| risk_score (1–4) | Score |
|-----------------|-------|
| 4 (เสี่ยงสูงมาก) | 25 |
| 3 (เสี่ยงสูง) | 18 |
| 2 (เสี่ยงปานกลาง) | 10 |
| 1 (เสี่ยงต่ำ) | 5 |

### Alert Levels

| Level | เงื่อนไข | สี |
|-------|---------|---|
| **วิกฤต** | score ≥ 75 | 🔴 |
| **เตือนภัย** | score ≥ 50 | 🟠 |
| **เฝ้าระวัง** | score ≥ 25 | 🟡 |
| **ปกติ** | score < 25 | 🟢 |

---

## Data Quality — 8 Rules

ตรวจสอบใน `data_quality.py` ทุกรอบ pipeline บน Silver reservoir:

| # | Rule | ประเภท | เงื่อนไข fail |
|---|------|--------|--------------|
| 1 | NULL check — reservoir_id | Critical | null > 0% |
| 2 | NULL check — record_date | Critical | null > 0% |
| 3 | NULL check — percent_storage | Warning | null > 5% |
| 4 | Duplicate check — (reservoir_id, record_date, source) | Critical | dup > 0 |
| 5 | Range check — percent_storage ∈ [0, 100] | Critical | out-of-range > 0 |
| 6 | Range check — volume_mcm ≥ 0 | Warning | negative > 0 |
| 7 | Freshness check — max(record_date) ≥ today − 1 | Critical | stale data |
| 8 | Row count — total rows ≥ 100 | Critical | < 100 rows |

ผล DQ เก็บใน `/opt/airflow/data/dq_reports/dq_report_YYYYMMDD_HHMMSS.json`  
และ export เป็น `data/exports/dq_latest.json` พร้อม `pipeline_meta.json`

---

## Export Layer — JSON Snapshots

`export_gold_to_json.py` ผลิต 4 ไฟล์ใน `data/exports/` หลังทุก pipeline run:

| File | Source | Content |
|------|--------|---------|
| `alerts.json` | `Fact_Flood_Alert` (ORC) | 77 จังหวัด, alert level, score breakdown |
| `reservoirs.json` | `silver_reservoir_features` (Parquet) | ~518 อ่าง, status, % เต็ม, filling rate |
| `dq_latest.json` | DQ report files | ประวัติ DQ runs ล่าสุด |
| `pipeline_meta.json` | Computed | จำนวนอ่าง, alert summary, DQ overall |

อ่านได้ทั้ง `.parquet` และ `.orc` ผ่าน `pyarrow` ตรงๆ จาก HDFS ผ่าน `hdfs.InsecureClient`

---

## Reference — Static Data Files

```
data/
├── reservoir_downstream_map.csv   ← 90 อ่าง → 77 จังหวัด downstream mapping
│                                     columns: reservoir_name_th, reservoir_name_en,
│                                              river_basin_th, river_basin_en,
│                                              dam_province_code, dam_province_th,
│                                              lat, lon, downstream_prov_codes,
│                                              downstream_prov_th, capacity_mcm_ref,
│                                              source_type
└── flood_risk_area.csv            ← 17-year historical flood risk, 7,356 ตำบล
                                      columns: geocode, month, tambon_th/en,
                                               amphoe_code, amphoe_th/en,
                                               prov_code, prov_th/en,
                                               flood_count_17yr, flood_criteria,
                                               risk_level, risk_score
```
