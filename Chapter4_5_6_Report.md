# Chapter 4: Data Pipeline Implementation

---

## 4.1 Data Ingestion

ระบบดึงข้อมูลจาก 4 แหล่งหลักผ่านกลไกที่แตกต่างกัน

| แหล่งข้อมูล | API / ไฟล์ | โปรโตคอล | ความถี่ | รูปแบบดิบ |
|------------|-----------|----------|---------|----------|
| RID Dam API | `https://app.rid.go.th/reservoir/api/dam/public[/YYYY-MM-DD]` | REST → Kafka → HDFS | ทุก 6 ชม. | NDJSON |
| RID Reservoir API | `https://app.rid.go.th/reservoir/api/reservoir/public[/YYYY-MM-DD]` | REST → Kafka → HDFS | ทุก 6 ชม. | NDJSON |
| TMD Weather NWP | TMD API (fallback: mock รายจังหวัดตามปฏิทินมรสุม) | REST → HDFS | ทุก 6 ชม. | NDJSON |
| Flood Risk Static | `flood_risk_area.csv` (สสน. 17-year historical, 7,356 ตำบล) | CSV → HDFS | ครั้งเดียว (idempotent) | CSV |

### กระบวนการ Kafka Ingestion (ingest_kafka)

การดึงข้อมูลอ่างเก็บน้ำและเขื่อนแบ่งเป็น 2 ขั้นตอน

**Step 1 — kafka_producer.py:**
- เรียก RID API แบบ real-time (`/public`) ทุกรอบ
- รอบแรก: backfill ย้อนหลัง 7 วัน ผ่าน `/public/YYYY-MM-DD`
- รอบถัดไป: ดึงเมื่อวานเพิ่มเสมอ เพื่อรับข้อมูลที่อาจ lag จาก API
- serialize แต่ละ record เป็น JSON แล้ว publish ไปยัง topic `reservoir_updates` (3 partitions)
- ติดตาม state ใน `.producer_state.json`

**Step 2 — kafka_consumer.py:**
- consume messages จาก Kafka โดย filter เฉพาะ `fetched_at > watermark` ล่าสุด
- เขียนเป็นไฟล์ NDJSON ลง HDFS ตาม path `/bronze/{dam|reservoir}/dt=YYYY-MM-DD/`
- อัปเดต `.watermark.json` หลัง consume สำเร็จ

**Weather Generator — mock_weather_generator.py:**
- เรียก TMD NWP API ทีละจังหวัด ครบ 77 จังหวัด
- ถ้า API ล้มเหลว: fallback เป็น mock ที่อิงปฏิทินมรสุมรายจังหวัด
- เขียน NDJSON ลง `/bronze/weather/dt=YYYY-MM-DD/`

---

## 4.2 Incremental Processing และกลไก Idempotency

ระบบใช้กลไกหลายชั้นเพื่อให้ pipeline สามารถรันซ้ำได้โดยไม่เกิดข้อมูลซ้ำซ้อน

| กลไก | Component | รายละเอียด |
|-----|----------|-----------|
| Kafka Watermark | kafka_consumer.py | เก็บ `max(fetched_at)` ใน `.watermark.json` — consume เฉพาะ message ใหม่กว่า watermark |
| Producer State File | kafka_producer.py | เก็บวันที่ fetch ล่าสุดใน `.producer_state.json` — backfill อัตโนมัติ 7 วันในรอบแรก |
| HDFS Partition Overwrite | bronze_to_silver.py | เขียน Parquet ด้วย `overwrite=True` ต่อ partition — รัน DAG ซ้ำทับได้ไม่มีผลข้างเคียง |
| Multi-root Date Discovery | bronze_to_silver.py | `_list_bronze_dates(['/bronze/dam', '/bronze/reservoir'])` — scan ทุก `dt=` partition ที่มีข้อมูล |
| INSERT OVERWRITE PARTITION | silver_to_gold.py | Hive `INSERT OVERWRITE` ต่อ partition `dt` — รัน pipeline ซ้ำแทนที่ partition เดิม |
| Static Ingest Idempotency | ingest_static.py | เขียน flood_risk_area.csv ลง HDFS ด้วย `overwrite=True` — ปลอดภัยต่อการรันซ้ำ |

---

## 4.3 Data Transformation

`bronze_to_silver.py` แปลงข้อมูลดิบ Bronze ให้อยู่ในรูปแบบที่สะอาด มี schema ชัดเจน พร้อมสำหรับการวิเคราะห์

| ขั้นตอน | การดำเนินการ | Domain |
|--------|------------|--------|
| 1. อ่าน NDJSON จาก HDFS | list ไฟล์ทุก partition ใน `/bronze/{source}/dt=YYYY-MM-DD/` แล้ว parse แต่ละบรรทัดเป็น dict | Reservoir / Weather |
| 2. Type Casting | แปลง capacity_mcm, volume_mcm, percent_storage, inflow_mcm, outflow_mcm เป็น DOUBLE; record_date เป็น DATE | Reservoir |
| 3. คำนวณ percent_storage | ถ้า percent_storage = 0 และ capacity_mcm > 0 → คำนวณใหม่ = `volume_mcm / capacity_mcm × 100` | Reservoir |
| 4. Deduplication | `drop_duplicates` บน (reservoir_id, record_date, source) เก็บ record ล่าสุด | Reservoir |
| 5. Null Filling | เติม `0.0` ใน numeric columns ที่ขาดหาย, เติม `'unknown'` ใน string columns | Reservoir / Weather |
| 6. เพิ่ม Metadata | เพิ่ม `silver_loaded_at` = UTC timestamp ปัจจุบัน และ `dt` = partition date | ทั้งหมด |
| 7. Schema Enforcement | select เฉพาะ column ที่กำหนด — ตัด column เกิน, เติม column ขาด | ทั้งหมด |
| 8. เขียน Parquet (Snappy) | เขียนลง `/silver/{domain}/dt=YYYY-MM-DD/*.parquet` ผ่าน WebHDFS | ทั้งหมด |
| 9. Rename Columns (Flood Risk) | แปลงชื่อ column จาก Thai header ใน CSV ให้เป็น snake_case ภาษาอังกฤษ | Flood Risk |

### Compute Reservoir Features (Silver → Silver)

หลัง `silver_to_gold` เสร็จ `compute_reservoir_features.py` คำนวณ rolling features จากข้อมูล Silver 7 วันย้อนหลัง แล้วเขียนผลลัพธ์กลับลง Silver ที่ `/silver/reservoir_features/dt=YYYY-MM-DD/`

| Feature | สูตร / Logic | ชนิด |
|---------|------------|------|
| `net_flow_mcm` | `inflow_mcm − outflow_mcm` | DOUBLE |
| `storage_delta_1d` | `percent_storage วันนี้ − เมื่อวาน` | DOUBLE |
| `storage_trend_7d` | `percent_storage วันนี้ − 7 วันก่อน` | DOUBLE |
| `days_to_full` | `(100 − percent_storage) / storage_delta_1d` ถ้า delta > 0 มิฉะนั้น 365 | DOUBLE |
| `reservoir_status` | CRITICAL ≥85% / HIGH ≥70% / NORMAL ≥30% / LOW <30% | STRING |
| `filling_rate` | FAST (delta>2%) / MODERATE (>0.5%) / STABLE (>−0.5%) / DRAINING | STRING |

---

## 4.4 Data Quality Report

`data_quality.py` ตรวจสอบคุณภาพข้อมูลบน Silver reservoir ทุกรอบ pipeline ด้วยกฎ 8 ข้อ หาก Critical rule ใดล้มเหลว pipeline จะ fail ที่ขั้นตอนนี้

| # | Rule | ประเภท | เงื่อนไข Fail | ผลทดสอบ (2026-04-23) |
|---|------|--------|-------------|---------------------|
| 1 | NULL check — `reservoir_id` | Critical | null > 0% | **PASS** (0 nulls / 483 rows) |
| 2 | NULL check — `record_date` | Critical | null > 0% | **PASS** (0 nulls) |
| 3 | NULL check — `percent_storage` | Warning | null > 5% | **PASS** (0 nulls) |
| 4 | Duplicate check — (reservoir_id, record_date, source) | Critical | duplicate > 0 | **PASS** (0 duplicates) |
| 5 | Range check — `percent_storage` ∈ [0, 100] | Critical | out-of-range > 0 | **PASS** (all in range) |
| 6 | Range check — `volume_mcm` ≥ 0 | Warning | negative > 0 | **PASS** (0 negative) |
| 7 | Freshness check — `max(record_date)` ≥ today − 1 | Critical | stale data | **PASS** (2026-04-23) |
| 8 | Row count — total rows ≥ 100 | Critical | < 100 rows | **PASS** (483 rows) |

**ผลสรุป:** `dq_overall = PASS` — กฎทั้ง 8 ข้อผ่านทุกข้อ  
รายงาน DQ เก็บไว้ที่ `data/dq_reports/dq_report_20260423_112114.json` และ export เป็น `data/exports/dq_latest.json` ทุกรอบ pipeline

---

## 4.5 Data Warehouse — Star Schema

Gold layer ใช้ Star Schema บน Apache Hive (database: `flood_monitoring`) ประกอบด้วย Dimension 3 ตารางและ Fact 3 ตาราง

| ตาราง | ประเภท | Grain | Key Design | Format | HDFS Path |
|------|--------|-------|-----------|--------|----------|
| Dim_Reservoir | Dimension | 1 row / อ่าง | `PMOD(ABS(HASH(id+'_'+source)), 2^30)` — stable hash | ORC | `/gold/flood_monitoring/Dim_Reservoir/` |
| Dim_Location | Dimension | 1 row / ตำบล | `CAST(geocode AS INT)` — 6-digit natural key | ORC | `/gold/flood_monitoring/Dim_Location/` |
| Dim_Weather | Dimension | 1 row / จังหวัด | `weather_key = prov_code` — INSERT OVERWRITE ทุกรอบ | ORC | `/gold/flood_monitoring/Dim_Weather/` |
| Fact_Water_Monitoring | Fact | 1 row / อ่าง / วัน | `ABS(HASH(reservoir_id + '_' + record_date))` | Parquet | `/gold/flood_monitoring/Fact_Water_Monitoring/dt=YYYY-MM-DD/` |
| Fact_Flood_Risk | Fact | 1 row / ตำบล / เดือน | `ABS(HASH(geocode + '_' + month))` | Parquet | `/gold/flood_monitoring/Fact_Flood_Risk/dt=YYYY-MM-DD/` |
| Fact_Flood_Alert | Fact | 1 row / จังหวัด / วัน | Sequential `alert_key` | ORC | `/gold/Fact_Flood_Alert/dt=YYYY-MM-DD/` |

### Business Queries จาก Gold Layer

**Query 1 — สรุปสถานะแจ้งเตือนน้ำท่วมรายภาค**

```sql
SELECT
  d.region,
  f.alert_level,
  COUNT(*) AS province_count,
  ROUND(AVG(f.alert_score), 2) AS avg_score
FROM Fact_Flood_Alert f
JOIN Dim_Location d ON f.prov_code = d.prov_code
WHERE f.alert_date = '2026-04-23'
GROUP BY d.region, f.alert_level
ORDER BY d.region, f.alert_level DESC;
```

| region | alert_level | province_count | avg_score |
|--------|------------|---------------|-----------|
| NE (ภาคอีสาน) | เฝ้าระวัง | 2 | 33.0 |
| NE (ภาคอีสาน) | ปกติ | 18 | 12.4 |
| E (ภาคตะวันออก) | เฝ้าระวัง | 1 | 25.0 |
| E (ภาคตะวันออก) | ปกติ | 6 | 11.8 |
| N (ภาคเหนือ) | ปกติ | 17 | 10.2 |
| C (ภาคกลาง) | ปกติ | 18 | 9.5 |
| S (ภาคใต้) | ปกติ | 14 | 8.3 |
| W (ภาคตะวันตก) | ปกติ | 5 | 7.6 |

จังหวัดที่อยู่ในระดับเฝ้าระวังทั้งหมดกระจุกตัวในภาคอีสานและภาคตะวันออก

---

**Query 2 — Top 5 อ่างเก็บน้ำที่มีระดับน้ำสูงสุด พร้อมสถานะ**

```sql
SELECT
  r.reservoir_name,
  r.region,
  f.percent_storage,
  cf.reservoir_status,
  cf.filling_rate
FROM Fact_Water_Monitoring f
JOIN Dim_Reservoir r ON f.reservoir_key = r.reservoir_key
JOIN silver.silver_reservoir_features cf
  ON r.reservoir_id = cf.reservoir_id AND cf.dt = '2026-04-23'
WHERE f.dt = '2026-04-23'
ORDER BY f.percent_storage DESC
LIMIT 5;
```

| reservoir_name | region | percent_storage | reservoir_status | filling_rate |
|---------------|--------|----------------|-----------------|-------------|
| อ่างเก็บน้ำแม่จอกหลวง | ภาคเหนือ | 100.00% | CRITICAL | FAST |
| อ่างเก็บน้ำลำพันชาดน้อย | ภาคอีสาน | 100.00% | CRITICAL | FAST |
| อ่างเก็บน้ำห้วยคันแทใหญ่ | ภาคอีสาน | 97.86% | CRITICAL | FAST |
| อ่างเก็บน้ำห้วยไร่ | ภาคอีสาน | 97.68% | CRITICAL | FAST |
| อ่างเก็บน้ำห้วยหินลับ | ภาคอีสาน | 97.50% | CRITICAL | FAST |

อ่างเก็บน้ำขนาดเล็ก-กลางในภาคอีสานมีระดับน้ำสูงถึง CRITICAL ทุกแห่ง สอดคล้องกับช่วงต้นฤดูฝน

---

**Query 3 — จังหวัดที่มีคะแนนน้ำท่วมสูงสุด พร้อม breakdown รายองค์ประกอบ**

```sql
SELECT
  prov_th,
  alert_level,
  alert_score,
  reservoir_score,
  weather_score,
  historical_risk_score,
  trigger_reservoirs
FROM Fact_Flood_Alert
WHERE alert_date = '2026-04-23'
  AND alert_level != 'ปกติ'
ORDER BY alert_score DESC;
```

| prov_th | alert_level | alert_score | reservoir_score | weather_score | historical_risk_score | trigger_reservoirs |
|---------|------------|------------|----------------|--------------|---------------------|-------------------|
| นครราชสีมา | เฝ้าระวัง | 38.0 | 20.0 | 5.0 | 13.0 | เขื่อนลำตะคอง, เขื่อนลำพระเพลิง |
| สุรินทร์ | เฝ้าระวัง | 28.0 | 10.0 | 5.0 | 13.0 | อ่างเก็บน้ำห้วยเสนง |
| จันทบุรี | เฝ้าระวัง | 25.0 | 5.0 | 5.0 | 15.0 | เขื่อนประแสร์ |

นครราชสีมามีคะแนนสูงสุดจาก reservoir_score ของเขื่อนลำตะคองและลำพระเพลิงที่อยู่ในสถานะ CRITICAL พร้อม FAST filling rate

---

# Chapter 5: Automation & Data Product

---

## 5.1 Orchestration

### Apache Airflow DAG

ระบบใช้ Apache Airflow 2.x พร้อม LocalExecutor โดย DAG ชื่อ `flood_risk_monitoring_pipeline`

| การตั้งค่า | ค่า | เหตุผล |
|----------|-----|-------|
| `schedule_interval` | `0 */6 * * *` (ทุก 6 ชม.) | สมดุลระหว่างความสดของข้อมูลกับ load บน RID API |
| `max_active_runs` | 1 | ป้องกัน race condition บน HDFS partition เดียวกัน |
| `retries` | 2 ครั้ง | รองรับ transient error จาก RID API หรือ HDFS |
| `retry_delay` | 5 นาที | ให้เวลา external service recover |
| `execution_timeout` | 30 นาที (ทั่วไป), 15–20 นาที (compute/alert) | ป้องกัน task hang นาน |
| `catchup` | False | ไม่ backfill DAG run ที่พลาดระหว่าง downtime |
| `executor` | LocalExecutor | เหมาะกับ single-node Docker Compose setup |

### Workflow Diagram

```
ingest_static ──┐
ingest_kafka    ├──► bronze_to_silver ──► hive_init ──► silver_to_gold
generate_weather┘                                             │
                                                              ▼
                                                      compute_features
                                                              │
                                                              ▼
                                                        flood_alert
                                                              │
                                                              ▼
                                                   data_quality_check
                                                              │
                                                              ▼
                                                     export_dashboard
```

| Task | Script | เวลาเฉลี่ย | หน้าที่ |
|------|--------|-----------|--------|
| ingest_static | ingest_static.py | < 1 นาที | โหลด flood_risk_area.csv → HDFS Bronze (idempotent) |
| ingest_kafka | kafka_producer.py + kafka_consumer.py | 2–5 นาที | ดึง RID API → Kafka → HDFS Bronze พร้อม watermark |
| generate_weather | mock_weather_generator.py | 1–2 นาที | ดึง TMD API / mock → HDFS Bronze ครบ 77 จังหวัด |
| bronze_to_silver | bronze_to_silver.py | 3–8 นาที | Clean + type-cast Bronze → Silver Parquet ทุก partition |
| hive_init | hive_silver_external.sql + hive_ddl.sql | 1–2 นาที | CREATE TABLE IF NOT EXISTS — Silver/Gold DDL (idempotent) |
| silver_to_gold | silver_to_gold.py | 5–10 นาที | INSERT OVERWRITE → Dim_* + Fact_Water_Monitoring + Fact_Flood_Risk |
| compute_features | compute_reservoir_features.py | 3–5 นาที | Rolling 7-day features → Silver reservoir_features |
| flood_alert | flood_alert_scoring.py | 2–4 นาที | Composite scoring 77 จังหวัด → Gold Fact_Flood_Alert (ORC) |
| data_quality_check | data_quality.py | 1–2 นาที | 8 DQ rules บน Silver — fail pipeline ถ้า Critical rule ไม่ผ่าน |
| export_dashboard | export_gold_to_json.py | 1–2 นาที | Gold ORC + Parquet → 4 JSON files สำหรับ Dashboard |

### Logging & Monitoring

- **Airflow UI (port 8080):** ดู task status, log, และ Graph View แบบ real-time
- **DQ Report JSON:** เก็บผล 8 rules ทุกรอบ พร้อม `dq_overall` flag (PASS/FAIL) ที่ `data/dq_reports/`
- **pipeline_meta.json:** เก็บ `export_date`, `reservoir_count`, `alert_summary`, `last_fetched_at` ทุกรอบ
- **Python Logging:** ทุก script ใส่ prefix ชื่อ module เช่น `[kafka_producer]`, `[bronze_to_silver]` เพื่อกรองใน Airflow log ได้ง่าย

### ตัวอย่างการจัดการ Error

| Error ที่พบ | สาเหตุ | การแก้ไข |
|-----------|-------|---------|
| silver_to_gold SIGTERM | restart container ขณะ task กำลังรัน | Re-trigger DAG หลัง scheduler stable |
| Gold ORC ไม่ถูกอ่าน — reservoir_score = 0 ทุกจังหวัด | `load_partition()` filter เฉพาะ `.parquet` ไม่รับ `.orc` | เพิ่ม `read_file()` รองรับ ORC ผ่าน `pyarrow.orc`; เพิ่ม `.orc` ใน filter |
| export_gold_to_json.py truncated (SyntaxError) | Docker volume filesystem cache บน Windows (NTFS↔Linux) | เขียนไฟล์ใหม่ผ่าน bash Python + `os.replace()` เพื่อ force inode ใหม่ |
| PowerShell `2>/dev/null` ไม่ทำงาน | Bash null redirect ไม่ใช่ PowerShell syntax | เปลี่ยนเป็น `2>$null` สำหรับ PowerShell |
| container `airflow-worker` not found | ใช้ LocalExecutor ไม่มี worker container แยก | เปลี่ยนเป็น `docker exec airflow-scheduler` |

---

## 5.2 Data Product — Flood Monitoring Dashboard

### รายละเอียด

Dashboard พัฒนาเป็น Cowork Artifact (HTML/JavaScript + Chart.js) ดึงข้อมูลจาก 4 JSON files ที่ export ออกมาจาก Gold layer ทุกรอบ pipeline

| ส่วน | Use Case | Data Source | Visualization |
|-----|---------|------------|---------------|
| Alert Banner | แสดงจังหวัดที่ต้องเฝ้าระวังทันที | Fact_Flood_Alert | Colored cards พร้อม score breakdown |
| Alert Donut Chart | ภาพรวม distribution ระดับเตือนภัยทั้ง 77 จังหวัด | Fact_Flood_Alert | Donut chart (Chart.js) |
| Score Component Bar | เปรียบเทียบ reservoir/weather/historical score รายจังหวัด | Fact_Flood_Alert | Stacked bar chart |
| Province Table | ค้นหา/เรียงลำดับจังหวัดตาม score พร้อม trigger reservoir | Fact_Flood_Alert | Searchable sortable table (77 rows) |
| Reservoir Table | ดูสถานะอ่างเก็บน้ำทั้งหมด 518 แห่ง + ค้นหาชื่อ | silver_reservoir_features | Searchable table พร้อม status badge |
| DQ History | ติดตาม data quality ย้อนหลัง | dq_latest.json | Timeline table |

### ผลการวิเคราะห์ข้อมูลจาก Dashboard (2026-04-23)

จากข้อมูลวันที่ 23 เมษายน 2569 ระบบประมวลผลอ่างเก็บน้ำ 518 แห่งทั่วประเทศ ได้ผลดังนี้

- **ระดับน้ำในอ่าง:** อ่างเก็บน้ำกว่า 40% มีสถานะ CRITICAL (≥85% เต็ม) โดยเฉพาะในภาคอีสานและภาคเหนือ สอดคล้องกับฤดูฝนต้นปี
- **Alert Summary:** ปกติ 74 จังหวัด / เฝ้าระวัง 3 จังหวัด (นครราชสีมา, สุรินทร์, จันทบุรี) / ไม่มี เตือนภัย หรือ วิกฤต
- **Score Analysis:** คะแนนสูงสุด (38.0) เป็นของนครราชสีมา ซึ่งมีเขื่อนลำตะคองและลำพระเพลิงอยู่ในสถานะ CRITICAL พร้อม FAST filling rate ส่งผลให้ reservoir_score สูง
- **Data Quality:** PASS ทั้ง 8 rules บนข้อมูล 483 rows

---

# Chapter 6: Conclusion & Challenges

---

## 6.1 สรุปผลการดำเนินงาน

| เป้าหมาย | ผลการดำเนินงาน | สถานะ |
|---------|--------------|------|
| ดึงข้อมูลอ่างเก็บน้ำและเขื่อนจาก RID API | ดึงได้ครบ ทั้งแบบ real-time และ historical backfill 7 วัน ผ่าน Kafka pipeline | ✅ สำเร็จ |
| ดึงข้อมูลสภาพอากาศรายจังหวัด | ครอบ 77 จังหวัดครบถ้วน มี mock fallback รายจังหวัดตามปฏิทินมรสุม | ✅ สำเร็จ |
| สร้าง Silver layer ที่สะอาดและมี schema | bronze_to_silver.py ผ่านขั้นตอน 9 steps รองรับ multi-date backfill | ✅ สำเร็จ |
| สร้าง Gold Star Schema บน Hive | 3 Dimension + 3 Fact tables พร้อม stable hash keys ทุกตาราง | ✅ สำเร็จ |
| คำนวณ Flood Alert Score รายจังหวัด | 77 จังหวัดครบ คะแนน 0–100 จาก 3 components ครอบ downstream map 77/77 จังหวัด | ✅ สำเร็จ |
| ระบบ Data Quality Checks | 8 rules ครอบ null/duplicate/range/freshness/count — PASS ทุกข้อในการทดสอบ | ✅ สำเร็จ |
| Dashboard แสดงผลแบบ real-time | Cowork Artifact แสดง alert, reservoir status, DQ history พร้อม search | ✅ สำเร็จ |
| Automation ด้วย Airflow | DAG ทำงานทุก 6 ชม. อัตโนมัติ ทั้ง 10 tasks ผ่าน SUCCESS ในการทดสอบจริง | ✅ สำเร็จ |

---

## 6.2 ปัญหาที่พบและแนวทางการแก้ไข

### ปัญหาด้าน Data Architecture

| ปัญหา | ผลกระทบ | แนวทางแก้ไข |
|------|--------|-----------|
| Gold ORC ไม่ถูกอ่านโดย export script | reservoir_score = 0 ทุกจังหวัด — dashboard แสดงข้อมูลผิดพลาดทั้งหมด | เพิ่ม `read_file()` ใน export_gold_to_json.py รองรับทั้ง `.parquet` และ `.orc` ผ่าน pyarrow.orc |
| Downstream map ครอบเพียง ~53 จังหวัด | 24 จังหวัดมี reservoir_score = 0 เสมอแม้มีอ่างเก็บน้ำในพื้นที่ | ขยาย reservoir_downstream_map.csv จาก 66 เป็น 90 entries ครอบ 77/77 จังหวัด |
| bronze_to_silver อ่านเฉพาะวันปัจจุบัน | ข้อมูล historical ที่ kafka_producer backfill ไม่ถูกประมวลผล | เพิ่ม `_list_bronze_dates()` scan ทุก `dt=` partition ใน `/bronze/dam` และ `/bronze/reservoir` |

### ปัญหาด้าน Infrastructure

| ปัญหา | ผลกระทบ | แนวทางแก้ไข |
|------|--------|-----------|
| Windows NTFS ↔ Linux filesystem cache (Docker volume) | sandbox เห็น stale file — แก้ไขสคริปต์แล้วแต่ container ยังใช้เวอร์ชันเก่า | เขียนไฟล์ผ่าน bash Python + `os.replace()` เพื่อ force inode ใหม่ |
| LocalExecutor — ไม่มี airflow-worker container | `docker restart airflow-worker` ล้มเหลว — task ถูก SIGTERM ระหว่างรัน | ใช้ `airflow-scheduler` แทน; หลีกเลี่ยง restart container ขณะมี active task |
| Hive MapReduce memory บน small cluster | window function (ROW_NUMBER OVER) ทำให้ job ล้มเหลวเพราะ YARN memory ไม่พอ | เปลี่ยน surrogate key เป็น HASH-based (`PMOD/ABS/HASH`) ไม่ใช้ window function |

### ปัญหาด้านข้อมูล

| ปัญหา | ผลกระทบ | แนวทางแก้ไข |
|------|--------|-----------|
| RID API ส่ง percent_storage = 0 เมื่อ capacity_mcm = 0 | อ่างขนาดเล็กทุกแห่งดูเหมือน "ว่าง" ทั้งที่มีน้ำจริง | คำนวณ percent_storage ใหม่ใน bronze_to_silver ถ้า volume > 0 และ capacity = 0 |
| Weather API ขาดข้อมูล 15 จังหวัด | pipeline ล้มเหลวเพราะจำนวนจังหวัดไม่ครบ 77 | เพิ่ม 15 จังหวัดใน mock_weather_generator.py |
| NaN/Inf ใน JSON export | `json.dump()` ล้มเหลวด้วย ValueError | เพิ่ม `sanitize_for_json()` แทนที่ NaN/Inf ด้วย 0 ก่อน dump |

---

## 6.3 สิ่งที่ได้เรียนรู้จากการทำโครงงาน

### ด้านสถาปัตยกรรม Data Engineering

**Medallion Architecture** ช่วยให้แต่ละ layer มีจุดประสงค์ชัดเจน สามารถ debug ได้ทีละชั้น และ re-process เฉพาะ layer ที่มีปัญหาได้โดยไม่กระทบ layer อื่น หากข้อมูล Bronze ผิดพลาด ก็ re-run เฉพาะ bronze_to_silver โดยไม่ต้องดึงข้อมูลใหม่จาก API

**Idempotency** เป็นสิ่งสำคัญมากสำหรับ production pipeline การออกแบบให้ทุก task รันซ้ำได้โดยให้ผลลัพธ์เหมือนเดิม ช่วยลด complexity ของ error handling ได้อย่างมาก โดยเฉพาะเมื่อระบบต้องการ retry อัตโนมัติ

**Key Design ใน Star Schema** ต้องเลือกวิธีที่ stable ตลอดเวลา HASH-based key ดีกว่า `ROW_NUMBER()` เพราะไม่เปลี่ยนเมื่อข้อมูลใหม่เข้ามา ทำให้ FK references ใน Fact tables ไม่หลุด

### ด้านเทคนิค

**Apache Hive บน small cluster** มีข้อจำกัดสูง โดยเฉพาะ window function ที่ต้องการ 2 MapReduce jobs ต้องเลือก HiveQL ที่ใช้ทรัพยากรน้อยและเหมาะกับขนาดข้อมูลจริง ไม่ใช่ copy query จาก environment ที่มีทรัพยากรมากกว่า

**Docker volume mount บน Windows** มีปัญหา filesystem caching ที่ซับซ้อน การเข้าใจกลไก inode และ overlay filesystem ช่วยให้แก้ปัญหา stale file ได้อย่างถูกต้อง แทนที่จะ restart container ซ้ำๆ

**File format compatibility** ORC และ Parquet ต้องการ library ต่างกัน (`pyarrow.orc` vs `pd.read_parquet`) และต้องจัดการ explicitly ไม่สามารถใช้ฟังก์ชันเดียวกันได้

### ด้านกระบวนการ

**Data Quality ควรออกแบบตั้งแต่แรก** ไม่ใช่เพิ่มทีหลัง การมี DQ checks ใน pipeline ช่วยตรวจพบปัญหาในข้อมูลต้นทางได้เร็วก่อนที่จะส่งผลกระทบต่อ downstream โดยเฉพาะ freshness check ที่ช่วยระบุเมื่อ API มีปัญหา

**Downstream mapping เป็น business knowledge ที่สำคัญที่สุด** ในระบบนี้ การขยาย map ให้ครบ 77 จังหวัดทำให้ผลลัพธ์ของ scoring มีความน่าเชื่อถือและสะท้อนความเป็นจริงมากขึ้น ความถูกต้องของ domain knowledge ส่งผลโดยตรงต่อคุณภาพของ output

**Incremental processing ต้องการ state management ที่ชัดเจน** watermark file และ producer state file ช่วยให้ระบบรู้ว่าข้อมูลไหนประมวลผลไปแล้ว และไม่ต้อง re-process ข้อมูลเก่า แต่ต้องระวังกรณีที่ state file เสียหายหรือหายไป ซึ่งอาจทำให้ระบบ skip หรือ re-process ข้อมูลผิดพลาดได้
