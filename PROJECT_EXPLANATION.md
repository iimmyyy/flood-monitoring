# Thailand Flood Risk Monitoring — อธิบาย Structure & การทำงานของระบบ

---

## 1. ภาพรวมระบบ (System Overview)

ระบบนี้เป็น **Data Engineering Pipeline** สำหรับติดตามสถานการณ์น้ำท่วมในประเทศไทย ทำงานแบบ **End-to-End Automated Pipeline** ทุก 6 ชั่วโมง โดยดึงข้อมูลจาก API จริง 2 แหล่ง + ไฟล์ CSV ประวัติน้ำท่วม แล้วผ่านกระบวนการทำความสะอาด จัดเก็บ และตรวจสอบคุณภาพข้อมูลอัตโนมัติ

### แหล่งข้อมูล (Data Sources)
| แหล่ง | ประเภท | คำอธิบาย |
|-------|--------|-----------|
| RID Dam API | REST API (Real-time) | ข้อมูลเขื่อนขนาดใหญ่ของกรมชลประทาน ปริมาณน้ำ / % storage / inflow / outflow |
| RID Reservoir API | REST API (Real-time) | ข้อมูลอ่างเก็บน้ำขนาดเล็ก-กลาง |
| TMD NWP API | REST API (Daily) | พยากรณ์อากาศรายจังหวัด (อุณหภูมิ / ฝน / ความชื้น) จากกรมอุตุนิยมวิทยา |
| flood_risk_area.csv | Static File | ข้อมูลพื้นที่เสี่ยงน้ำท่วมรายตำบล ย้อนหลัง 17 ปี จาก สสน. |

---

## 2. Architecture & Data Flow (Medallion Architecture)

```
┌─────────────────────────────────────────────────────────────────┐
│                         DATA SOURCES                            │
│  RID Dam API  │  RID Reservoir API  │  TMD API  │  CSV File    │
└────────┬──────────────┬─────────────────┬────────────┬──────────┘
         │              │                 │            │
         ▼              ▼                 ▼            ▼
┌─────────────────────────────────────────────────────────────────┐
│                    INGESTION LAYER                              │
│   kafka_producer.py   mock_weather_generator.py  ingest_static │
│         │                      │                      │         │
│         ▼                      │                      │         │
│   [Kafka Topic]                │                      │         │
│  reservoir_updates             │                      │         │
│         │                      │                      │         │
│         ▼ kafka_consumer.py    ▼                      ▼         │
└─────────────────────────────────────────────────────────────────┘
         │                      │                      │
         ▼                      ▼                      ▼
┌─────────────────────────────────────────────────────────────────┐
│                    BRONZE LAYER (HDFS)                          │
│  /bronze/dam/YYYY-MM-DD/*.json                                  │
│  /bronze/reservoir/YYYY-MM-DD/*.json    (NDJSON raw files)      │
│  /bronze/weather/YYYY-MM-DD/*.json                              │
│  /bronze/flood_risk/flood_risk_area.csv                         │
└─────────────────────┬───────────────────────────────────────────┘
                      │
                      ▼  bronze_to_silver.py
┌─────────────────────────────────────────────────────────────────┐
│                    SILVER LAYER (HDFS - Parquet)                │
│  /silver/reservoir/dt=YYYY-MM-DD/reservoir.parquet              │
│  /silver/weather/dt=YYYY-MM-DD/weather.parquet    (cleaned /    │
│  /silver/flood_risk/flood_risk.parquet             deduplicated)│
└─────────────────────┬───────────────────────────────────────────┘
                      │
                      ▼  silver_to_gold.py (via Hive)
┌─────────────────────────────────────────────────────────────────┐
│                    GOLD LAYER (Hive - ORC)                      │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐  │
│  │  Dim_Date    │  │ Dim_Location │  │   Dim_Reservoir      │  │
│  └──────────────┘  └──────────────┘  └──────────────────────┘  │
│  ┌──────────────┐  ┌──────────────────────────────────────────┐ │
│  │  Dim_Weather │  │       Fact_Water_Monitoring              │ │
│  └──────────────┘  │       Fact_Flood_Risk                    │ │
│                    └──────────────────────────────────────────┘ │
└─────────────────────┬───────────────────────────────────────────┘
                      │
                      ▼  data_quality.py
┌─────────────────────────────────────────────────────────────────┐
│                    DATA QUALITY REPORTS                         │
│  /opt/airflow/data/dq_reports/dq_report_YYYYMMDD_HHMMSS.json   │
└─────────────────────────────────────────────────────────────────┘
```

---

## 3. โครงสร้างโฟลเดอร์ (Folder Structure)

```
flood-monitoring/
├── docker-compose.yml          ← กำหนด services ทั้งหมด (11 containers)
├── Dockerfile                  ← build image Airflow + ติดตั้ง libraries
├── hadoop.env                  ← config Hadoop / HDFS / YARN / Hive
├── .env                        ← environment variables (API keys, ports)
│
├── dags/
│   └── flood_pipeline_dag.py   ← DAG หลัก: กำหนดลำดับและตาราง task
│
├── ingestion/
│   ├── kafka_producer.py       ← ดึงข้อมูล RID API → ส่งเข้า Kafka
│   ├── kafka_consumer.py       ← อ่านจาก Kafka → เขียนลง HDFS Bronze
│   ├── ingest_static.py        ← อัปโหลด CSV ขึ้น HDFS Bronze
│   ├── mock_weather_generator.py ← ดึง TMD API → เขียนลง HDFS Bronze
│   └── hdfs_util.py            ← utility functions สำหรับ write ไป HDFS
│
├── transformation/
│   ├── bronze_to_silver.py     ← แปลง Bronze → Silver (clean + Parquet)
│   └── dbt/                    ← dbt models (alternative SQL transformation)
│       ├── dbt_project.yml
│       ├── profiles.yml
│       └── models/silver/
│           ├── silver_reservoir.sql
│           ├── silver_flood_risk.sql
│           ├── silver_weather.sql
│           └── schema.yml
│
├── warehouse/
│   ├── hive_ddl.sql            ← DDL สร้าง Gold tables (ทำครั้งแรก)
│   ├── hive_silver_external.sql← DDL สร้าง Silver external tables ใน Hive
│   ├── hive_gold_ddl_only.sql  ← DDL Gold tables อย่างเดียว (ไม่มี INSERT)
│   ├── silver_to_gold.py       ← โหลด Silver → Gold via Hive (docker exec)
│   └── business_queries.sql    ← HiveQL สำหรับ query เชิงธุรกิจ
│
├── quality/
│   └── data_quality.py         ← ตรวจสอบ 8 DQ rules บน Silver layer
│
└── data/
    ├── flood_risk_area.csv     ← ข้อมูล 17 ปี รายตำบล จาก สสน.
    ├── .watermark.json         ← บันทึก timestamp ล่าสุดที่ consume แล้ว
    └── dq_reports/             ← JSON reports ผล DQ check แต่ละรอบ
```

---

## 4. อธิบายการทำงานของแต่ละไฟล์

---

### 4.1 `docker-compose.yml` — Infrastructure Definition

**หน้าที่:** กำหนด container ทั้งหมด 11 ตัว ที่รันบนเครื่องเดียว (single-node cluster)

| Service | Image | Port | บทบาท |
|---------|-------|------|-------|
| **namenode** | bde2020/hadoop-namenode | 9870, 9000 | ศูนย์กลาง HDFS ติดตาม metadata ของไฟล์ทั้งหมด |
| **datanode** | bde2020/hadoop-datanode | — | เก็บข้อมูลจริงใน HDFS |
| **resourcemanager** | bde2020/hadoop-resourcemanager | 8088 | จัดการ YARN resource สำหรับ MapReduce |
| **nodemanager** | bde2020/hadoop-nodemanager | — | รัน MapReduce jobs จริง |
| **zookeeper** | confluentinc/cp-zookeeper | 2181 | coordinator ของ Kafka |
| **kafka** | confluentinc/cp-kafka | 9092 | message broker สำหรับ streaming data |
| **hive-metastore-postgresql** | bde2020/hive-metastore-postgresql | — | ฐานข้อมูล PostgreSQL สำหรับเก็บ Hive metadata |
| **hive-metastore** | bde2020/hive | 9083 | Hive Metastore service (Thrift) |
| **hive-server** | bde2020/hive | 10000, 10002 | HiveServer2 รับคำสั่ง HiveQL |
| **airflow-postgres** | postgres:13 | — | ฐานข้อมูล Airflow (DAG state, task logs) |
| **airflow-webserver** | flood-airflow:latest | 8080 | Airflow Web UI |
| **airflow-scheduler** | flood-airflow:latest | — | ตรวจ DAG และ trigger tasks ตามเวลา |

**จุดสำคัญ:**
- Airflow container mount folder `./dags`, `./ingestion`, `./transformation`, `./quality`, `./warehouse` เข้าไปใน `/opt/airflow/...` ทำให้แก้ไขโค้ดบน host แล้ว Airflow เห็นทันที
- Mount `/var/run/docker.sock` เพื่อให้ Airflow สั่ง `docker exec hive-server hive -e "..."` ได้จากภายใน container
- ทุก container อยู่ใน network เดียวกัน (`flood-net`) คุยกันด้วยชื่อ service เช่น `namenode:9870`, `kafka:29092`

---

### 4.2 `Dockerfile` — Airflow Custom Image

**หน้าที่:** Build Docker image สำหรับ Airflow โดยติดตั้ง dependencies เพิ่มเติม

```dockerfile
FROM apache/airflow:2.8.1-python3.10
# ติดตั้ง Docker CLI → ให้ airflow สั่ง docker exec ได้
RUN apt-get install -y docker.io
# ติดตั้ง Python libraries
RUN pip install requests confluent-kafka hdfs pandas pyarrow
```

**Libraries ที่ติดตั้ง:**
- `requests` — HTTP client สำหรับ RID / TMD API และ WebHDFS
- `confluent-kafka` — Kafka producer + consumer
- `hdfs` — Python client สำหรับ HDFS (ใช้อ่านไฟล์)
- `pandas` — data manipulation
- `pyarrow` — เขียน Parquet files

---

### 4.3 `hadoop.env` — Hadoop/Hive Configuration

**หน้าที่:** file env สำหรับตั้งค่า Hadoop cluster ทั้งหมด ถูก load โดยทุก container ที่เป็น Hadoop/Hive

**ค่าสำคัญ:**
- `CORE_CONF_fs_defaultFS=hdfs://namenode:9000` → ที่อยู่ HDFS หลัก
- `HDFS_CONF_dfs_webhdfs_enabled=true` → เปิด WebHDFS REST API ที่ port 9870
- `HDFS_CONF_dfs_replication=1` → เก็บข้อมูล 1 copy (เพราะ single node)
- `YARN_CONF_yarn_nodemanager_resource_memory___mb=2048` → RAM จำกัดสำหรับ MapReduce
- `HIVE_SITE_CONF_hive_execution_engine=mr` → Hive ใช้ MapReduce (ไม่ใช่ Tez/Spark)

---

### 4.4 `dags/flood_pipeline_dag.py` — Airflow DAG (Pipeline Orchestrator)

**หน้าที่:** กำหนดลำดับการทำงานของ pipeline ทั้งหมด ทำงานทุก 6 ชั่วโมง

**Task flow:**
```
ingest_static ──┐
ingest_kafka   ──┼──► bronze_to_silver ──► hive_init ──► silver_to_gold ──► data_quality_check
generate_weather┘
```

| Task | ประเภท | บทบาท |
|------|--------|-------|
| `ingest_static` | BashOperator | รัน `ingest_static.py` อัปโหลด CSV ขึ้น HDFS |
| `ingest_kafka` | BashOperator | รัน `kafka_producer.py` แล้ว `kafka_consumer.py` |
| `generate_weather` | BashOperator | รัน `mock_weather_generator.py` |
| `bronze_to_silver` | BashOperator | รัน `bronze_to_silver.py` |
| `hive_init` | BashOperator | สร้าง Silver external tables + Gold DDL ใน Hive |
| `silver_to_gold` | BashOperator | รัน `silver_to_gold.py` |
| `data_quality_check` | BashOperator | รัน `data_quality.py` |

**จุดสำคัญ:**
- `ingest_static`, `ingest_kafka`, `generate_weather` รันพร้อมกัน (parallel) เพื่อลดเวลา
- ถ้า task ใด fail → ลอง retry 2 ครั้ง ห่างกัน 5 นาที
- `catchup=False` → ไม่ย้อน run DAG ที่ผ่านมา
- `max_active_runs=1` → ป้องกัน 2 runs ทับกัน

---

### 4.5 `ingestion/hdfs_util.py` — HDFS Utility Functions

**หน้าที่:** helper functions สำหรับเขียนไฟล์ขึ้น HDFS โดยตรงผ่าน WebHDFS REST API

**ปัญหาที่แก้:** bde2020 Hadoop DataNode ไม่รองรับ chunked Transfer-Encoding ซึ่ง library `hdfs` ใช้เป็นค่า default → ต้องใช้ `requests.put(data=bytes)` แทน ซึ่ง requests จะ set `Content-Length` ให้อัตโนมัติ

**2-step WebHDFS PUT:**
```
1. PUT namenode:9870/webhdfs/v1/path?op=CREATE
   → NameNode ตอบ 307 Redirect พร้อม URL ของ DataNode
2. PUT datanode_url  (ส่ง bytes จริง)
   → DataNode รับไฟล์และเก็บข้อมูล
```

**Functions หลัก:**
- `hdfs_makedirs(path)` → สร้าง directory ใน HDFS
- `hdfs_write(path, bytes)` → เขียนไฟล์ขึ้น HDFS
- `hdfs_status(path)` → ตรวจสอบว่าไฟล์มีอยู่ไหม และขนาดเท่าไร

---

### 4.6 `ingestion/ingest_static.py` — Static CSV Uploader

**หน้าที่:** อัปโหลดไฟล์ `flood_risk_area.csv` ขึ้น HDFS Bronze layer

**ขั้นตอน:**
1. อ่านไฟล์ CSV จาก `/opt/airflow/data/flood_risk_area.csv`
2. สร้าง directory `/bronze/flood_risk/` ใน HDFS
3. อัปโหลดไฟล์ → `/bronze/flood_risk/flood_risk_area.csv` (overwrite=True → idempotent)
4. ตรวจสอบ file size ที่ HDFS ว่าตรงกับ local file
5. เขียน metadata sidecar `_meta.json` พร้อม timestamp

**Idempotent:** รัน task นี้กี่ครั้งก็ได้ ผลลัพธ์เหมือนกันเสมอ เพราะ overwrite=True

---

### 4.7 `ingestion/kafka_producer.py` — RID API → Kafka

**หน้าที่:** ดึงข้อมูลเขื่อนจาก RID API แล้วส่งเข้า Kafka topic `reservoir_updates`

**ขั้นตอนทำงาน:**
1. **สร้าง Kafka topic** ถ้ายังไม่มี (3 partitions, replication=1)
2. **ดึง Dam data** จาก `https://app.rid.go.th/reservoir/api/dam/public`
   - Response มีโครงสร้าง: `{data: [{region: "ภาคเหนือ", dam: [{...},...]},...], date: "..."}`
   - Flatten: แยก dam แต่ละตัวออกมาพร้อม region และ report_date
3. **ดึง Reservoir data** จาก `https://app.rid.go.th/reservoir/api/reservoir/public`
   - Response เป็น list แบน: `[{...}, {...}, ...]`
4. **Normalize** ทั้งสอง source ให้เป็น schema เดียวกัน 14 fields:
   `source, reservoir_id, reservoir_name, region, owner, record_date, capacity_mcm, volume_mcm, percent_storage, inflow_mcm, outflow_mcm, active_storage_mcm, dead_storage_mcm, fetched_at`
5. **Publish** ไป Kafka ทีละ record (key = `{reservoir_id}_{date}_{source}`)
6. ถ้า publish ได้ 0 records → exit code 1 (DAG fail)

**หมายเหตุ:** RID Dam API ต้องการ Browser-like User-Agent header ไม่งั้น server ปฏิเสธ connection

---

### 4.8 `ingestion/kafka_consumer.py` — Kafka → HDFS Bronze

**หน้าที่:** อ่านข้อความจาก Kafka และเขียนลง HDFS Bronze layer พร้อมระบบ watermark

**Watermark system:**
- บันทึก `last_fetched_at` timestamp ใน `/opt/airflow/data/.watermark.json`
- ทุกครั้งที่ consume จะข้าม records ที่ `fetched_at <= watermark` (ข้อมูลเก่าที่เคยประมวลผลแล้ว)
- อัปเดต watermark หลัง write ลง HDFS เสร็จ
- ทำให้ pipeline ทำงานได้แบบ **idempotent** ไม่มีข้อมูลซ้ำ

**HDFS output path:**
```
/bronze/{source}/{record_date}/{source}_{HHMMSS}.json
ตัวอย่าง: /bronze/dam/2026-04-22/dam_103045.json
```

**Stop condition:** หยุด consume เมื่อได้ 6 consecutive empty polls (30 วินาที ไม่มีข้อความ)

---

### 4.9 `ingestion/mock_weather_generator.py` — TMD API → HDFS Bronze

**หน้าที่:** ดึงพยากรณ์อากาศจาก TMD NWP API ทุก 77 จังหวัด → เขียนลง HDFS Bronze

**Strategy แบบ Fallback:**
```
สำหรับแต่ละจังหวัด:
  ├── ลอง call TMD API (lat, lon)
  │   ├── สำเร็จ → ใช้ข้อมูลจริง (data_source = "tmd_api")
  │   └── ล้มเหลว → ใช้ Mock ที่สร้างจาก Monsoon Calendar (data_source = "mock_tmd")
  └── เขียน record ลง HDFS
```

**Monsoon Calendar:** ตารางความน่าจะเป็นฝนตกรายเดือน แยกตาม 6 ภาค (C/N/NE/E/W/S) ใช้ generate ค่า random ที่สมจริงตามฤดูกาล

**Output record (ต่อจังหวัด):** prov_code, prov_th, prov_en, region, latitude, longitude, forecast_date, temp_max_c, temp_min_c, rainfall_forecast_mm, rain_intensity, humidity_pct, wind_speed_kmh, is_heavy_rain_24h, data_source

**HDFS path:** `/bronze/weather/{date}/weather_{HHMMSS}.json`

---

### 4.10 `transformation/bronze_to_silver.py` — Bronze → Silver Transformation

**หน้าที่:** อ่าน raw data จาก HDFS Bronze → clean → แปลงเป็น Parquet → เขียนลง HDFS Silver

ประมวลผล 3 dataset พร้อมกัน:

#### 4.10.1 `process_reservoir()` — ข้อมูลเขื่อน/อ่างเก็บน้ำ

**Input:** `/bronze/dam/{date}/*.json` + `/bronze/reservoir/{date}/*.json`

**Transformations:**
1. อ่าน NDJSON files ทั้งหมดในวันนั้น รวมเป็น DataFrame เดียว
2. Cast numeric columns (`capacity_mcm`, `volume_mcm`, `percent_storage`, ฯลฯ) ด้วย `pd.to_numeric(errors='coerce')`
3. Fill nulls: `reservoir_id=""` → drop, `reservoir_name="Unknown"`, `region="Unknown"`
4. Clip `percent_storage` ให้อยู่ใน [0, 100]
5. Normalize date: รองรับทั้ง `record_date` และ `date` field
6. Deduplicate บน key `(reservoir_id, record_date, source)` เก็บ record ล่าสุด
7. เพิ่ม metadata: `silver_loaded_at`, `dt` (partition)

**Output:** `/silver/reservoir/dt={date}/reservoir.parquet`

#### 4.10.2 `process_flood_risk()` — ข้อมูลพื้นที่เสี่ยงน้ำท่วม

**Input:** `/bronze/flood_risk/flood_risk_area.csv`

**Transformations:**
1. Rename columns: `PROV_T → prov_th`, `COUNT 17 YEAR → flood_count_17yr`, ฯลฯ
2. Cast: `geocode` → zero-padded 6 digits, `prov_code`/`amphoe_code` → int
3. Deduplicate บน `(geocode, month)`
4. เพิ่ม `risk_score`: เสี่ยงต่ำ=1, เสี่ยงปานกลาง=2, เสี่ยงสูง=3, เสี่ยงสูงมาก=4

**Output:** `/silver/flood_risk/flood_risk.parquet`

#### 4.10.3 `process_weather()` — ข้อมูลพยากรณ์อากาศ

**Input:** `/bronze/weather/{date}/*.json`

**Transformations:**
1. Cast numeric weather fields
2. Cast `is_heavy_rain_24h` → boolean
3. Deduplicate บน `(prov_code, forecast_date)`

**Output:** `/silver/weather/dt={date}/weather.parquet`

**WebHDFS Write:** ใช้ `_webhdfs_write()` แทน HDFS library เพื่อหลีกเลี่ยง chunked encoding

---

### 4.11 `transformation/dbt/` — dbt Models (Alternative)

**หน้าที่:** SQL transformation models ที่เขียนด้วย dbt framework เป็น alternative ของ `bronze_to_silver.py`

ปัจจุบัน pipeline หลักใช้ `bronze_to_silver.py` (Python/Pandas) แต่มี dbt models เตรียมไว้สำหรับใช้กับ Hive ถ้าต้องการ scale

**Models:**
- `silver_reservoir.sql` — clean reservoir data พร้อม `ROW_NUMBER() OVER (PARTITION BY reservoir_id, record_date, source ORDER BY fetched_at DESC)` เพื่อ dedup
- `silver_weather.sql` — clean weather data
- `silver_flood_risk.sql` — clean flood risk data
- `schema.yml` — กำหนด tests: `not_null` สำหรับ key fields

---

### 4.12 `warehouse/hive_silver_external.sql` — Silver External Tables DDL

**หน้าที่:** สร้าง Hive external tables ที่ชี้ไปยัง Silver Parquet files ใน HDFS

```sql
CREATE DATABASE IF NOT EXISTS silver LOCATION '/silver';

CREATE EXTERNAL TABLE IF NOT EXISTS silver.silver_reservoir (...)
PARTITIONED BY (dt STRING)
STORED AS PARQUET
LOCATION '/silver/reservoir';

MSCK REPAIR TABLE silver_reservoir;  -- ลงทะเบียน partition ใหม่
```

**สำคัญ:** เป็น EXTERNAL table → ลบ table ออกจาก Hive แต่ไฟล์ Parquet ใน HDFS ยังอยู่ครบ  
**MSCK REPAIR TABLE:** บังคับ Hive Metastore ให้ scan HDFS และ register partition ใหม่ที่เพิ่งเขียน

---

### 4.13 `warehouse/hive_ddl.sql` — Gold Layer DDL

**หน้าที่:** สร้าง Star Schema ใน Hive Gold layer (ทำครั้งแรก / ทำซ้ำได้)

#### Star Schema Design:

```
                    ┌─────────────┐
                    │  Dim_Date   │
                    │ date_key PK │
                    └──────┬──────┘
                           │
┌──────────────┐           │           ┌────────────────┐
│ Dim_Location │     ┌─────▼──────┐    │  Dim_Reservoir │
│location_key  ├────►│Fact_Water  │◄───┤ reservoir_key  │
│ geocode      │     │Monitoring  │    │ reservoir_id   │
│ tambon/amphoe│     │ fact_key   │    │ reservoir_name │
│ prov_code    │     │ date_key   │    │ capacity_mcm   │
│ region       │     │reservoir_key    └────────────────┘
└──────┬───────┘     │volume_mcm  │
       │             │pct_storage │    ┌────────────────┐
       │             │inflow_mcm  │    │  Dim_Weather   │
       │             │outflow_mcm │    │ weather_key    │
       │             │ dt (part.) │    │ prov_code      │
       │             └────────────┘    │ rainfall_mm    │
       │                              │is_heavy_rain   │
       │      ┌─────────────────┐     └──────┬─────────┘
       └─────►│  Fact_Flood_Risk│◄────────────┘
              │ fact_key        │
              │ location_key    │
              │ weather_key     │
              │ geocode         │
              │ risk_month      │
              │ flood_count_17yr│
              │ risk_level      │
              │ risk_score      │
              │ dt (partition)  │
              └─────────────────┘
```

**Dimension Tables:**
| Table | Grain | Key |
|-------|-------|-----|
| `Dim_Date` | 1 row ต่อวัน | `date_key` = YYYYMMDD |
| `Dim_Location` | 1 row ต่อตำบล | `location_key` = CAST(geocode AS INT) |
| `Dim_Reservoir` | 1 row ต่อ reservoir+source | `reservoir_key` = ROW_NUMBER() |
| `Dim_Weather` | 1 row ต่อจังหวัด | `weather_key` = prov_code |

**Fact Tables:**
| Table | Grain | Partition |
|-------|-------|-----------|
| `Fact_Water_Monitoring` | 1 เขื่อน × 1 วัน | `dt` = YYYY-MM-DD |
| `Fact_Flood_Risk` | 1 ตำบล × 1 เดือน | `dt` = load date |

**Storage:** ORC format + Snappy compression (เหมาะกับ Hive analytical queries)

---

### 4.14 `warehouse/silver_to_gold.py` — Silver → Gold (Hive Loader)

**หน้าที่:** โหลดข้อมูลจาก Silver Parquet → Hive Gold Star Schema ผ่าน `docker exec hive-server hive -e "..."`

**ทำงานอย่างไร:**
```python
subprocess.run(["docker", "exec", "hive-server", "hive", "-e", hql])
```
ส่ง HiveQL เข้าไปรันในตัว Hive server โดยตรง ไม่ต้องผ่าน JDBC/SASL

**ลำดับการ load:**
1. `init_hive_schema()` — สร้าง Silver external tables + Gold DDL (idempotent)
2. **Load Dim_Reservoir** — GROUP BY แล้วใส่ ROW_NUMBER() ผ่าน subquery
3. **Load Dim_Location** — ใช้ CAST(geocode AS INT) เป็น location_key (ไม่ต้อง ROW_NUMBER → เร็วกว่า)
4. **Load Dim_Weather** — ใช้ prov_code เป็น weather_key (unique ต่อจังหวัด)
5. **Load Fact_Water_Monitoring** — JOIN Silver reservoir + Dim_Reservoir → Fact
6. **Load Fact_Flood_Risk** — JOIN Silver flood_risk + Dim_Location + Dim_Weather

**Idempotent:** ทุก INSERT ใช้ `INSERT OVERWRITE TABLE ... PARTITION (dt='...')` → ทับ partition เดิม รัน 2 ครั้งได้ผลเหมือนกัน

**Hive 2.x settings ที่ set ทุก query:**
```sql
SET hive.exec.dynamic.partition=true;
SET mapreduce.map.memory.mb=512;  -- จำกัด memory สำหรับ single-node
SET hive.vectorized.execution.enabled=false;  -- ปิดเพราะ bde2020 Hive ไม่ stable
```

---

### 4.15 `warehouse/business_queries.sql` — Analytical Queries

**หน้าที่:** HiveQL queries สำเร็จรูปสำหรับใช้วิเคราะห์ข้อมูล เชื่อมต่อ Power BI / Tableau ได้ผ่าน port 10000

**Query 1: จังหวัดที่เสี่ยงน้ำท่วมตอนนี้**
```
เขื่อน > 80% capacity AND พยากรณ์ฝนหนัก
→ Early warning สำหรับพื้นที่ท้ายน้ำ
```

**Query 2: Top 10 ตำบลที่มีประวัติน้ำท่วมสูงสุด**
```
ประวัติ 17 ปี → จัดลำดับความเสี่ยงสะสม
→ ใช้วางแผนเตรียมรับมือ
```

**Query 3: แนวโน้มปริมาณน้ำ 30 วัน**
```
7-day moving average ของ % storage
+ daily_change (filling vs draining)
→ วิเคราะห์ trend แต่ละเขื่อน
```

---

### 4.16 `quality/data_quality.py` — Data Quality Checks

**หน้าที่:** ตรวจสอบคุณภาพข้อมูล Silver layer และสร้าง DQ report

**8 Rules ที่ตรวจสอบ:**

| Rule | Field | Critical? | เงื่อนไข |
|------|-------|-----------|---------|
| NULL_CHECK | reservoir_id | ✅ | ห้าม null หรือ empty string |
| NULL_CHECK | record_date | ✅ | ห้าม null |
| NULL_CHECK | percent_storage | ✅ | ห้าม null |
| DUPLICATE_CHECK | (reservoir_id, record_date, source) | ✅ | ห้าม duplicate |
| RANGE_CHECK | percent_storage | ✅ | ต้องอยู่ใน [0, 100] |
| RANGE_CHECK | volume_mcm | ✅ | ต้องอยู่ใน [0, 99,999] |
| RANGE_CHECK | inflow_mcm | ⚠️ WARN | [-10, 9999] (ค่าลบเล็กน้อยยอมรับได้) |
| RANGE_CHECK | outflow_mcm | ⚠️ WARN | [-10, 9999] |

**การ report:** บันทึก JSON report ทุกรอบ พร้อม pass_rate %, ตัวอย่าง rows ที่ fail

**Exit code:**
- `sys.exit(0)` → ทุก critical check ผ่าน → Airflow mark PASS
- `sys.exit(1)` → มี critical failure → Airflow mark FAIL → หยุด pipeline

---

## 5. Data Flow สรุป (End-to-End)

```
[ทุก 6 ชั่วโมง — Airflow trigger]
       │
       ├── [parallel] ingest_static.py
       │     CSV → /bronze/flood_risk/flood_risk_area.csv
       │
       ├── [parallel] kafka_producer.py → kafka_consumer.py
       │     RID API → Kafka → /bronze/{dam,reservoir}/{date}/*.json
       │
       └── [parallel] mock_weather_generator.py
             TMD API (+ mock fallback) → /bronze/weather/{date}/*.json
                   │
                   ▼ [รอทั้ง 3 เสร็จ]
             bronze_to_silver.py
             Clean + Cast + Dedup + Parquet
             → /silver/reservoir/dt={date}/reservoir.parquet
             → /silver/weather/dt={date}/weather.parquet
             → /silver/flood_risk/flood_risk.parquet
                   │
                   ▼
             hive_init (CREATE TABLE IF NOT EXISTS + MSCK REPAIR)
                   │
                   ▼
             silver_to_gold.py (docker exec hive-server)
             Dim_Reservoir → Dim_Location → Dim_Weather
             → Fact_Water_Monitoring → Fact_Flood_Risk
                   │
                   ▼
             data_quality.py
             8 checks บน Silver → DQ Report JSON
             [PASS → pipeline success]
             [FAIL → pipeline halt, alert]
```

---

## 6. Port Summary (สำหรับเชื่อมต่อ Tool ภายนอก)

| Service | Port | ใช้ทำอะไร |
|---------|------|-----------|
| Airflow Web UI | http://localhost:8080 | monitor / trigger DAG (user: admin / pass: admin) |
| HDFS NameNode UI | http://localhost:9870 | ดู files ใน HDFS ได้เลย |
| YARN Resource Manager | http://localhost:8088 | ดู MapReduce jobs |
| Hive JDBC | localhost:10000 | เชื่อม Power BI / DBeaver / Tableau |
| Kafka | localhost:9092 | debug Kafka messages |

---

## 7. สรุปเทคโนโลยีที่ใช้

| Layer | Technology | เหตุผลที่เลือก |
|-------|-----------|--------------|
| Orchestration | Apache Airflow 2.8 | industry standard, web UI, retry logic |
| Message Queue | Apache Kafka | รองรับ streaming, watermark-based dedup |
| Storage | HDFS (Hadoop 3.2) | distributed storage, WebHDFS REST API |
| Processing (Bronze→Silver) | Python + Pandas + PyArrow | ยืดหยุ่น, ไม่ต้องการ Spark |
| Serving Layer | Apache Hive 2.3 | HiveQL, เชื่อม BI tools ผ่าน JDBC |
| File Format | NDJSON (Bronze) → Parquet/Snappy (Silver) → ORC/Snappy (Gold) | ประสิทธิภาพดีขึ้นในแต่ละ layer |
| Containerization | Docker Compose | deploy ง่าย, reproducible |
| Data Quality | Custom Python | ตรวจสอบ 8 rules, JSON report |
