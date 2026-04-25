# Thailand Flood Risk Monitoring System

ITDS344 Data Engineering and Infrastructures — Mahidol University

End-to-end data pipeline integrating reservoir water levels, weather forecasts, and historical flood risk data into a Hive Gold Star Schema with province-level flood alert scoring, orchestrated by Apache Airflow.

---

## Quick Start (after cloning)

```bash
# 1. Clone the repo
git clone https://github.com/<your-username>/flood-monitoring.git
cd flood-monitoring

# 2. Copy environment template and fill in your TMD API token
cp .env.example .env
# Edit .env → set TMD_API_TOKEN (all other values work out-of-the-box with Docker)

# 3. Start all services
docker compose up -d

# 5. Wait ~3-5 min, then init HDFS directories
docker exec namenode hdfs dfs -mkdir -p /bronze/reservoir /bronze/dam /bronze/weather /bronze/flood_risk /bronze/static
docker exec namenode hdfs dfs -mkdir -p /silver/reservoir /silver/flood_risk /silver/weather /silver/reservoir_features
docker exec namenode hdfs dfs -mkdir -p /gold/Fact_Flood_Alert
docker exec namenode hdfs dfs -chmod -R 777 /bronze /silver /gold

# 6. Open Airflow UI → http://localhost:8080  (admin / admin)
#    Enable DAG: flood_risk_monitoring_pipeline → click ▶ Trigger DAG
```

> **Prerequisites:** Docker Desktop ≥ 4.20 · Windows 10/11 64-bit · 12 GB RAM free

---

## Architecture

```
                 ┌──────────────────────────────────────────┐
                 │              DATA SOURCES                 │
                 │  RID Dam API   │  RID Reservoir API       │
                 │  TMD Weather   │  Flood Risk CSV (สสน.)   │
                 └──────────────────────┬───────────────────┘
                                        │
               ┌────────────────────────▼──────────────────┐
               │           KAFKA (Streaming)                │
               │   topic: reservoir_updates (3 partitions) │
               │   watermark: .watermark.json               │
               └────────────────────────┬──────────────────┘
                                        │
          ┌─────────────────────────────▼─────────────────────────┐
          │                 HDFS BRONZE LAYER                      │
          │  /bronze/dam/dt=YYYY-MM-DD/*.json        (NDJSON)      │
          │  /bronze/reservoir/dt=YYYY-MM-DD/*.json  (NDJSON)      │
          │  /bronze/weather/dt=YYYY-MM-DD/*.json    (NDJSON)      │
          │  /bronze/flood_risk/flood_risk_area.csv  (static CSV)  │
          │  /bronze/static/reservoir_downstream_map.csv           │
          └─────────────────────────────┬─────────────────────────┘
                                        │  bronze_to_silver.py
          ┌─────────────────────────────▼─────────────────────────┐
          │              HDFS SILVER LAYER (Parquet/Snappy)        │
          │  /silver/reservoir/dt=YYYY-MM-DD/                      │
          │  /silver/weather/dt=YYYY-MM-DD/                        │
          │  /silver/flood_risk/                                    │
          │  /silver/reservoir_features/dt=YYYY-MM-DD/  ← rolling  │
          └──────────────┬──────────────────────┬─────────────────┘
                         │  Hive INSERT OVERWRITE│  flood_alert_scoring.py
          ┌──────────────▼─────────────┐  ┌──────▼──────────────────────┐
          │  HIVE GOLD — Star Schema   │  │  HDFS GOLD — Fact_Flood_Alert│
          │  Dim_Reservoir             │  │  /gold/Fact_Flood_Alert/     │
          │  Dim_Location              │  │  dt=YYYY-MM-DD/              │
          │  Dim_Weather               │  │  flood_alerts.orc (ORC)      │
          │  Fact_Water_Monitoring     │  │  77 rows/day                 │
          │  Fact_Flood_Risk           │  └──────────────────────────────┘
          └──────────────┬─────────────┘
                         │  export_gold_to_json.py
          ┌──────────────▼──────────────────────────────────────────┐
          │              JSON EXPORT (data/exports/)                 │
          │  alerts.json · reservoirs.json · dq_latest.json          │
          │  pipeline_meta.json                                      │
          └──────────────┬──────────────────────────────────────────┘
                         │
          ┌──────────────▼──────────────────────────────────────────┐
          │         COWORK DASHBOARD (Flood Monitoring)              │
          │  Alert levels · Score breakdown · Reservoir status       │
          │  Province search · DQ history                            │
          └─────────────────────────────────────────────────────────┘
```

**Big Data tools used:** Apache Kafka (streaming ingestion) · Apache Hive 2.x (Gold DW) · HDFS (storage) · Apache Airflow 2.x (orchestration)

---

## Prerequisites

| Tool | Version / Spec |
|------|---------------|
| Docker Desktop | ≥ 4.20 |
| Windows 10/11 | 64-bit |
| RAM | 12 GB free recommended |
| Disk | 20 GB free |

---

## Project Structure

```
flood-monitoring/
├── docker-compose.yml                  # All services (Hadoop, Kafka, Hive, Airflow)
├── hadoop.env                          # Hadoop/Hive environment variables
├── Dockerfile                          # Airflow custom image
│
├── dags/
│   └── flood_pipeline_dag.py           # Airflow DAG — 10 tasks, every 6 hours
│
├── ingestion/
│   ├── kafka_producer.py               # RID API → Kafka (real-time + historical backfill)
│   ├── kafka_consumer.py               # Kafka → HDFS Bronze (watermark-filtered)
│   ├── ingest_static.py                # Flood Risk CSV → HDFS Bronze (idempotent)
│   ├── mock_weather_generator.py       # TMD API / mock fallback → HDFS Bronze (77 จังหวัด)
│   └── hdfs_util.py                    # Shared HDFS WebHDFS helpers
│
├── transformation/
│   ├── bronze_to_silver.py             # Clean + type-cast → Silver Parquet (multi-date)
│   └── compute_reservoir_features.py   # 7-day rolling features → Silver reservoir_features
│
├── warehouse/
│   ├── silver_to_gold.py               # Silver → Hive Gold Star Schema (Dim_* + Fact_*)
│   ├── flood_alert_scoring.py          # Composite scoring → Gold Fact_Flood_Alert (ORC)
│   ├── hive_ddl.sql                    # Gold DDL: Dim_* + Fact_Water_Monitoring + Fact_Flood_Risk
│   ├── hive_silver_external.sql        # Silver external tables DDL
│   └── hive_alert_ddl.sql              # Fact_Flood_Alert + silver_reservoir_features DDL
│
├── quality/
│   └── data_quality.py                 # 8 DQ rules on Silver reservoir — Critical/Warning
│
├── scripts/
│   ├── export_gold_to_json.py          # Gold ORC + Silver Parquet → JSON exports
│   └── export_gold_to_csv.py           # JSON exports → CSV for Power BI (alerts/reservoirs/pipeline_meta)
│
├── data/
│   ├── flood_risk_area.csv             # ← place your CSV here (สสน. 17-year historical)
│   ├── reservoir_downstream_map.csv    # 90 reservoirs → 77 provinces downstream mapping
│   ├── dq_reports/                     # DQ JSON reports (per pipeline run)
│   └── exports/                        # JSON snapshots for dashboard
│       ├── alerts.json                 # 77 province flood alerts
│       ├── reservoirs.json             # ~518 reservoir statuses
│       ├── dq_latest.json              # DQ history
│       ├── pipeline_meta.json          # Run metadata
│       └── csv/                        # CSV exports for Power BI
│           ├── alerts.csv              # 77 rows + lat/lon + alert_level_en
│           ├── reservoirs.csv          # ~483 rows
│           └── pipeline_meta.csv       # 1 row (flattened run metadata)
│
├── DATA_WAREHOUSE.md                   # Full data warehouse schema documentation
├── Chapter4_5_6_Report.md             # Project report (Ch. 4–6)
└── PROJECT_EXPLANATION.md              # High-level project explanation
```

---

## Pipeline — Task Flow

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

| # | Task | Script | หน้าที่ |
|---|------|--------|--------|
| 0a | ingest_static | ingest_static.py | โหลด flood_risk_area.csv → HDFS Bronze (idempotent) |
| 0b | generate_weather | mock_weather_generator.py | TMD API / mock fallback → Bronze ครบ 77 จังหวัด |
| 1 | ingest_kafka | kafka_producer.py + kafka_consumer.py | RID API → Kafka → HDFS Bronze (watermark) |
| 2 | bronze_to_silver | bronze_to_silver.py | Clean + transform ทุก Bronze partition → Silver Parquet |
| 2b | hive_init | hive_*.sql (3 files) | CREATE TABLE IF NOT EXISTS — Silver/Gold DDL |
| 3 | silver_to_gold | silver_to_gold.py | INSERT OVERWRITE → Dim_* + Fact_Water_Monitoring + Fact_Flood_Risk |
| 3b | compute_features | compute_reservoir_features.py | Rolling 7-day features → Silver reservoir_features |
| 3c | flood_alert | flood_alert_scoring.py | Composite scoring 77 จังหวัด → Gold Fact_Flood_Alert (ORC) |
| 4 | data_quality_check | data_quality.py | 8 DQ rules — fail pipeline ถ้า Critical rule ไม่ผ่าน |
| 5 | export_dashboard | export_gold_to_json.py | Gold ORC + Parquet → 4 JSON files ใน data/exports/ |

**Schedule:** `0 */6 * * *` (ทุก 6 ชั่วโมง) · **Retry:** 2 ครั้ง, delay 5 นาที · **Executor:** LocalExecutor

---

## Step-by-Step Setup

### Step 1 — Place your CSV file

Copy `monthly-flood-risk-area.csv` (from `Data source/risk_area/`) into the `data/` folder and rename it:

```
flood-monitoring\data\flood_risk_area.csv
```

### Step 2 — Open Docker Desktop

Make sure Docker Desktop is running. In the system tray, the whale icon should be steady (not animating).

### Step 3 — Open a terminal in the project folder

```powershell
cd "D:\Study\Data_en y-3\proj\flood-monitoring"
```

### Step 4 — Start all services

```powershell
docker compose up -d
```

This pulls ~4 GB of images on first run. Wait about 3–5 minutes for everything to start.

### Step 5 — Check service health

```powershell
docker compose ps
```

All services should show `running` or `healthy`. If any show `restarting`, check logs:

```powershell
docker compose logs <service-name> --tail 50
```

Expected healthy services: `namenode`, `datanode`, `resourcemanager`, `nodemanager`, `zookeeper`, `kafka`, `hive-metastore-postgresql`, `hive-metastore`, `hive-server`, `airflow-postgres`, `airflow-webserver`, `airflow-scheduler`

### Step 6 — Initialise HDFS directories

```powershell
docker exec namenode hdfs dfs -mkdir -p /bronze/reservoir /bronze/dam /bronze/weather /bronze/flood_risk /bronze/static
docker exec namenode hdfs dfs -mkdir -p /silver/reservoir /silver/flood_risk /silver/weather /silver/reservoir_features
docker exec namenode hdfs dfs -mkdir -p /gold/Fact_Flood_Alert
docker exec namenode hdfs dfs -chmod -R 777 /bronze /silver /gold
```

### Step 7 — Access Airflow UI

Open your browser: **http://localhost:8080**

- Username: `admin`
- Password: `admin`

Find the DAG **`flood_risk_monitoring_pipeline`** and click the toggle to enable it.

To run immediately: click the **▶ Trigger DAG** button.

### Step 8 — Monitor pipeline execution

Check task logs in the Airflow UI: click a task box → **Log** tab.

All 10 tasks should reach `success` (green). First run takes ~25–40 minutes due to Hive MapReduce cold start.

### Step 9 — Verify Gold layer

```powershell
docker exec -it hive-server beeline -u "jdbc:hive2://localhost:10000/flood_monitoring"
```

```sql
-- Check flood alerts
SELECT prov_th, alert_level, alert_score, reservoir_score
FROM Fact_Flood_Alert
WHERE alert_date = '2026-04-23'
ORDER BY alert_score DESC
LIMIT 10;

-- Check reservoir count
SELECT COUNT(*) FROM Fact_Water_Monitoring WHERE dt = '2026-04-23';
```

### Step 10 — Export CSV and view in Power BI

After `export_dashboard` task completes, run the CSV export script:

```powershell
python flood-monitoring/scripts/export_gold_to_csv.py
```

This writes 3 files to `data/exports/csv/`:
- `alerts.csv` — 77 province alert rows with lat/lon for Map visual
- `reservoirs.csv` — ~483 reservoir status rows
- `pipeline_meta.csv` — 1-row pipeline summary

Then open **Power BI Desktop → Home → Refresh** to update all visuals.
See [`POWERBI_SETUP.md`](POWERBI_SETUP.md) for full setup instructions.

---

## Running Scripts Manually

```powershell
# Open shell in Airflow container (NOTE: use airflow-scheduler, not airflow-worker)
docker exec -it airflow-scheduler bash

# Run individual steps (with date argument)
python /opt/airflow/ingestion/mock_weather_generator.py 2026-04-23
python /opt/airflow/ingestion/kafka_producer.py
python /opt/airflow/ingestion/kafka_consumer.py
python /opt/airflow/transformation/bronze_to_silver.py 2026-04-23
python /opt/airflow/transformation/compute_reservoir_features.py 2026-04-23
python /opt/airflow/warehouse/silver_to_gold.py 2026-04-23
python /opt/airflow/warehouse/flood_alert_scoring.py 2026-04-23
python /opt/airflow/quality/data_quality.py 2026-04-23
python /opt/airflow/scripts/export_gold_to_json.py 2026-04-23
```

---

## Alert Scoring

`flood_alert_scoring.py` คำนวณ `alert_score` (0–100) จาก 3 components ต่อจังหวัด:

```
alert_score = reservoir_score (0–40) + weather_score (0–35) + historical_risk_score (0–25)
```

| Alert Level | เงื่อนไข |
|------------|---------|
| **วิกฤต** | score ≥ 75 |
| **เตือนภัย** | score ≥ 50 |
| **เฝ้าระวัง** | score ≥ 25 |
| **ปกติ** | score < 25 |

Reservoir score อ้างอิงจาก `data/reservoir_downstream_map.csv` ซึ่งมี 90 entries ครอบ **77/77 จังหวัด**

---

## Data Quality — 8 Rules

`data_quality.py` รัน 8 rules บน Silver reservoir ทุก pipeline cycle:

| # | Rule | ประเภท |
|---|------|--------|
| 1 | NULL check — reservoir_id | Critical |
| 2 | NULL check — record_date | Critical |
| 3 | NULL check — percent_storage | Warning |
| 4 | Duplicate check — (reservoir_id, record_date, source) | Critical |
| 5 | Range check — percent_storage ∈ [0, 100] | Critical |
| 6 | Range check — volume_mcm ≥ 0 | Warning |
| 7 | Freshness check — max(record_date) ≥ today − 1 | Critical |
| 8 | Row count ≥ 100 | Critical |

รายงาน DQ ถูกเก็บที่ `data/dq_reports/dq_report_YYYYMMDD_HHMMSS.json` และ export เป็น `data/exports/dq_latest.json`

---

## Incremental Processing & Idempotency

| กลไก | Component | รายละเอียด |
|-----|----------|-----------|
| Kafka Watermark | kafka_consumer.py | `data/.watermark.json` — consume เฉพาะ message ใหม่กว่า watermark |
| Producer State | kafka_producer.py | `data/.producer_state.json` — backfill 7 วันในรอบแรก, ดึงเมื่อวานในรอบถัดไป |
| Multi-date Scan | bronze_to_silver.py | `_list_bronze_dates(['/bronze/dam', '/bronze/reservoir'])` — ประมวลผลทุก partition |
| HDFS Overwrite | bronze_to_silver.py | Parquet เขียนด้วย `overwrite=True` — รัน DAG ซ้ำได้ปลอดภัย |
| Hive Partition | silver_to_gold.py | `INSERT OVERWRITE PARTITION(dt=...)` — แทนที่ partition เดิมทุกรอบ |

---

## Web UIs

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow | http://localhost:8080 | admin / admin |
| HDFS NameNode | http://localhost:9870 | — |
| YARN ResourceManager | http://localhost:8088 | — |
| Hive Web UI | http://localhost:10002 | — |

---

## Stopping the System

```powershell
# Stop all containers (data preserved in volumes)
docker compose stop

# Stop and remove containers + volumes (full reset)
docker compose down -v
```

---

## Troubleshooting

| Symptom | Fix |
|---------|-----|
| `namenode` keeps restarting | Run `docker compose down -v` then `docker compose up -d` |
| `airflow-init` exits immediately | Normal — runs once to initialise the DB then exits |
| Kafka producer returns 0 records | RID API may be unreachable; check internet connectivity |
| Hive query timeout | MapReduce cold start is slow; wait 2–3 minutes and retry |
| Port conflict (8080 in use) | Edit `docker-compose.yml` → change `"8080:8080"` to `"8081:8080"` |
| `No such container: airflow-worker` | ใช้ LocalExecutor — ไม่มี worker container แยก ใช้ `airflow-scheduler` แทน |
| reservoir_score = 0 ทุกจังหวัด | Gold ORC ไม่ถูกอ่าน — ตรวจสอบ `export_gold_to_json.py` ว่ารองรับ `.orc` |
| silver_to_gold SIGTERM | อย่า restart container ขณะ task กำลังรัน — re-trigger DAG แทน |
| Windows NTFS file cache | เขียนไฟล์ Python script ผ่าน `os.replace()` เพื่อ force inode ใหม่ |
| PowerShell `2>/dev/null` error | ใช้ `2>$null` แทน สำหรับ PowerShell |

---

## Documentation

| ไฟล์ | เนื้อหา |
|-----|--------|
| `DATA_WAREHOUSE.md` | Full schema documentation — Bronze/Silver/Gold paths, table schemas, scoring logic, DQ rules |
| `Chapter4_5_6_Report.md` | Project report chapters 4–6: pipeline implementation, automation, conclusion |
| `POWERBI_SETUP.md` | Power BI Desktop setup guide — loading CSVs, building 4 visuals, color coding, refresh |
| `PROJECT_EXPLANATION.md` | High-level project explanation |
