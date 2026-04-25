import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.utils.dates import days_ago

# ── Default task arguments ────────────────────────────────────────────────────
default_args = {
    "owner": "flood-monitoring",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}

# ── Python paths inside the Airflow container ─────────────────────────────────
INGESTION_DIR = "/opt/airflow/ingestion"
TRANSFORM_DIR = "/opt/airflow/transformation"
QUALITY_DIR   = "/opt/airflow/quality"
WAREHOUSE_DIR = "/opt/airflow/warehouse"
SCRIPTS_DIR   = "/opt/airflow/scripts"

PYTHON_CMD = "python"

# ── Hive connection (beeline) ─────────────────────────────────────────────────
HIVE_SERVER = os.getenv("HIVE_HOST", "hive-server")
HIVE_PORT = os.getenv("HIVE_PORT", "10000")
HIVE_DB = os.getenv("HIVE_DATABASE", "flood_monitoring")
BEELINE_URL = f"jdbc:hive2://{HIVE_SERVER}:{HIVE_PORT}/{HIVE_DB}"


def get_run_date(**context) -> str:
    """Return the logical date of the DAG run as YYYY-MM-DD."""
    return context["logical_date"].strftime("%Y-%m-%d")


def get_run_year(**context) -> str:
    return context["logical_date"].strftime("%Y")


def build_hive_cmd(hql: str) -> str:
    """Wrap a HiveQL statement for execution via beeline."""
    safe_hql = hql.replace('"', '\\"')
    return (
        f'beeline -u "{BEELINE_URL}" --silent=true '
        f'--hiveconf hive.cli.print.header=false '
        f'-e "{safe_hql}"'
    )


# ── DAG definition ────────────────────────────────────────────────────────────
with DAG(
    dag_id="flood_risk_monitoring_pipeline",
    description="Thailand Flood Risk Monitoring — end-to-end data pipeline",
    default_args=default_args,
    schedule_interval="0 */6 * * *",    # Every 6 hours
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,                   # Prevent overlapping runs
    tags=["flood-monitoring", "production"],
    doc_md="""
## Thailand Flood Risk Monitoring Pipeline

**Runs every 6 hours.** Full pipeline:

1. **ingest_static** — load flood_risk_area.csv → HDFS Bronze (idempotent)
2. **ingest_kafka** — fetch RID API → Kafka → HDFS Bronze (watermark-based)
3. **generate_weather** — fetch TMD NWP weather forecast → HDFS Bronze (mock fallback per province)
4. **bronze_to_silver** — clean + transform Bronze → Silver Parquet
5. **hive_init** — create Silver external tables + Gold DDL + Alert DDL (idempotent)
6. **silver_to_gold** — load Silver → Hive Gold Star Schema (Dim_* + Fact_Water_Monitoring + Fact_Flood_Risk)
7. **compute_features** — 7-day rolling reservoir features → Silver (net_flow, trend, status, filling_rate)
8. **flood_alert** — composite scoring (reservoir + weather + historical risk) → Gold Fact_Flood_Alert
9. **data_quality_check** — 8 DQ rules on Silver; fails pipeline if any critical rule violated

**Alert Levels:** ปกติ (<25) | เฝ้าระวัง (≥25) | เตือนภัย (≥50) | วิกฤต (≥75)

**Data Sources:**
- RID Dam API: `https://app.rid.go.th/reservoir/api/dam/public`
- RID Reservoir API: `https://app.rid.go.th/reservoir/api/reservoir/public`
- Weather: TMD NWP API (real data, mock monsoon-calendar fallback per province if API fails)
- Flood Risk CSV: Monthly historical 17-year records (สสน.)
- Downstream Map: 33 major dams → downstream province mapping (researcher-compiled)
    """,
) as dag:

    # ── Task 0a: Ingest static flood risk CSV ─────────────────────────────────
    ingest_static = BashOperator(
        task_id="ingest_static",
        bash_command=(
            f"cd {INGESTION_DIR} && "
            f"HDFS_NAMENODE_URL=${{HDFS_NAMENODE_URL:-http://namenode:9870}} "
            f"HDFS_USER=${{HDFS_USER:-root}} "
            f"{PYTHON_CMD} ingest_static.py /opt/airflow/data/flood_risk_area.csv"
        ),
        doc_md="Load static flood risk CSV to HDFS Bronze. Idempotent (overwrite=True).",
    )

    # ── Task 0b: Generate mock weather ───────────────────────────────────────
    generate_weather = BashOperator(
        task_id="generate_weather",
        bash_command=(
            f"cd {INGESTION_DIR} && "
            f"HDFS_NAMENODE_URL=${{HDFS_NAMENODE_URL:-http://namenode:9870}} "
            f"HDFS_USER=${{HDFS_USER:-root}} "
            f"{PYTHON_CMD} mock_weather_generator.py "
            f"$(date -d 'today' +'%Y-%m-%d')"
        ),
        doc_md="Fetch real TMD NWP weather forecast per province → HDFS Bronze. Falls back to monsoon-based mock per province if API fails.",
    )

    # ── Task 1: Kafka ingest (producer + consumer) ────────────────────────────
    ingest_kafka = BashOperator(
        task_id="ingest_kafka",
        bash_command=(
            # Step A: Publish to Kafka
            f"echo '[ingest_kafka] Step 1/2: Publishing to Kafka...' && "
            f"cd {INGESTION_DIR} && "
            f"KAFKA_BOOTSTRAP_SERVERS=${{KAFKA_BOOTSTRAP_SERVERS:-kafka:29092}} "
            f"KAFKA_TOPIC=${{KAFKA_TOPIC:-reservoir_updates}} "
            f"RID_DAM_API_URL=${{RID_DAM_API_URL:-https://app.rid.go.th/reservoir/api/dam/public}} "
            f"RID_RESERVOIR_API_URL=${{RID_RESERVOIR_API_URL:-https://app.rid.go.th/reservoir/api/reservoir/public}} "
            f"PRODUCER_STATE_FILE=${{PRODUCER_STATE_FILE:-/opt/airflow/data/.producer_state.json}} "
            f"BACKFILL_DAYS=${{BACKFILL_DAYS:-7}} "
            f"{PYTHON_CMD} kafka_producer.py && "
            # Step B: Consume from Kafka → HDFS Bronze (watermark-filtered)
            f"echo '[ingest_kafka] Step 2/2: Consuming from Kafka → HDFS...' && "
            f"KAFKA_BOOTSTRAP_SERVERS=${{KAFKA_BOOTSTRAP_SERVERS:-kafka:29092}} "
            f"KAFKA_TOPIC=${{KAFKA_TOPIC:-reservoir_updates}} "
            f"KAFKA_CONSUMER_GROUP=${{KAFKA_CONSUMER_GROUP:-flood-monitoring-group}} "
            f"HDFS_NAMENODE_URL=${{HDFS_NAMENODE_URL:-http://namenode:9870}} "
            f"HDFS_USER=${{HDFS_USER:-root}} "
            f"PIPELINE_WATERMARK_FILE=${{PIPELINE_WATERMARK_FILE:-/opt/airflow/data/.watermark.json}} "
            f"{PYTHON_CMD} kafka_consumer.py"
        ),
        doc_md=(
            "1. kafka_producer.py: fetch RID API → publish to Kafka topic 'reservoir_updates'\n"
            "   - Always fetches real-time /public (both dam + reservoir)\n"
            "   - First run: also backfills BACKFILL_DAYS historical days (default 7)\n"
            "   - Subsequent runs: also fetches yesterday's date-based API\n"
            "   - State tracked in PRODUCER_STATE_FILE\n"
            "2. kafka_consumer.py: consume messages > watermark → write to HDFS Bronze"
        ),
    )

    # ── Task 2: Bronze → Silver ───────────────────────────────────────────────
    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command=(
            f"echo '[bronze_to_silver] Transforming Bronze → Silver...' && "
            f"cd {TRANSFORM_DIR} && "
            f"HDFS_NAMENODE_URL=${{HDFS_NAMENODE_URL:-http://namenode:9870}} "
            f"HDFS_USER=${{HDFS_USER:-root}} "
            f"{PYTHON_CMD} bronze_to_silver.py "
            f"$(date -d 'today' +'%Y-%m-%d')"
        ),
        doc_md="Clean Bronze NDJSON/CSV → write Silver Parquet with type casting, dedup, null fill.",
    )

    # ── Task 2b: Initialise Hive schemas (Gold DDL + Silver external tables) ──
    # Runs docker exec hive-server hive -f ... to create all tables IF NOT EXISTS.
    # Idempotent: safe to run every pipeline cycle.
    hive_init = BashOperator(
        task_id="hive_init",
        bash_command=(
            # Silver external tables (reservoir, weather, flood_risk, reservoir_features)
            f"docker cp {WAREHOUSE_DIR}/hive_silver_external.sql {os.getenv('HIVE_CONTAINER', 'hive-server')}:/tmp/hive_silver_external.sql && "
            f"docker exec {os.getenv('HIVE_CONTAINER', 'hive-server')} hive -f /tmp/hive_silver_external.sql && "
            # Gold star schema DDL (Dim_* + Fact_Water_Monitoring + Fact_Flood_Risk)
            f"docker cp {WAREHOUSE_DIR}/hive_ddl.sql {os.getenv('HIVE_CONTAINER', 'hive-server')}:/tmp/hive_ddl.sql && "
            f"docker exec {os.getenv('HIVE_CONTAINER', 'hive-server')} hive -f /tmp/hive_ddl.sql && "
            # Alert DDL (silver_reservoir_features external table + Fact_Flood_Alert)
            f"docker cp {WAREHOUSE_DIR}/hive_alert_ddl.sql {os.getenv('HIVE_CONTAINER', 'hive-server')}:/tmp/hive_alert_ddl.sql && "
            f"docker exec {os.getenv('HIVE_CONTAINER', 'hive-server')} hive -f /tmp/hive_alert_ddl.sql || true"
        ),
        doc_md=(
            "Create Silver external tables and Gold DDL in Hive (idempotent).\n"
            "Also creates silver_reservoir_features and Fact_Flood_Alert tables.\n"
            "Must complete before silver_to_gold writes to Gold layer."
        ),
    )

    # ── Task 3: Silver → Gold (Hive via Python pyhive) ───────────────────────
    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command=(
            f"HIVE_HOST=${{HIVE_HOST:-hive-server}} "
            f"HIVE_PORT=${{HIVE_PORT:-10000}} "
            f"HIVE_DATABASE=${{HIVE_DATABASE:-flood_monitoring}} "
            f"{PYTHON_CMD} {WAREHOUSE_DIR}/silver_to_gold.py "
            f"$(date -d 'today' +'%Y-%m-%d')"
        ),
        doc_md=(
            "Load Silver Parquet into Hive Gold Star Schema via INSERT OVERWRITE PARTITION. "
            "Uses pyhive (no SASL). Idempotent: re-running replaces the partition."
        ),
    )

    # ── Task 3b: Compute Reservoir Rolling Features ───────────────────────────
    compute_features = BashOperator(
        task_id="compute_features",
        bash_command=(
            f"echo '[compute_features] Computing 7-day rolling reservoir features...' && "
            f"cd {TRANSFORM_DIR} && "
            f"HDFS_NAMENODE_URL=${{HDFS_NAMENODE_URL:-http://namenode:9870}} "
            f"HDFS_USER=${{HDFS_USER:-root}} "
            f"{PYTHON_CMD} compute_reservoir_features.py "
            f"$(date -d 'today' +'%Y-%m-%d')"
        ),
        execution_timeout=timedelta(minutes=15),
        doc_md=(
            "Read Silver Parquet for the last 7 days → compute reservoir features:\n"
            "  net_flow_mcm, storage_trend_7d, storage_delta_1d, days_to_full,\n"
            "  reservoir_status (CRITICAL/HIGH/NORMAL/LOW), filling_rate (FAST/MODERATE/STABLE/DRAINING).\n"
            "Writes to /silver/reservoir_features/dt=YYYY-MM-DD/"
        ),
    )

    # ── Task 3c: Flood Alert Scoring ──────────────────────────────────────────
    flood_alert = BashOperator(
        task_id="flood_alert",
        bash_command=(
            f"echo '[flood_alert] Computing province-level flood alerts...' && "
            f"HDFS_NAMENODE_URL=${{HDFS_NAMENODE_URL:-http://namenode:9870}} "
            f"HDFS_USER=${{HDFS_USER:-root}} "
            f"HIVE_HOST=${{HIVE_HOST:-hive-server}} "
            f"HIVE_PORT=${{HIVE_PORT:-10000}} "
            f"HIVE_CONTAINER={os.getenv('HIVE_CONTAINER', 'hive-server')} "
            f"{PYTHON_CMD} {WAREHOUSE_DIR}/flood_alert_scoring.py "
            f"$(date -d 'today' +'%Y-%m-%d')"
        ),
        execution_timeout=timedelta(minutes=20),
        doc_md=(
            "Combine reservoir features + weather forecast + historical flood risk\n"
            "into a province-level composite alert score (0–100).\n"
            "Alert levels: ปกติ (<25) | เฝ้าระวัง (≥25) | เตือนภัย (≥50) | วิกฤต (≥75).\n"
            "Writes Gold ORC to /gold/Fact_Flood_Alert/dt=YYYY-MM-DD/ and runs MSCK REPAIR."
        ),
    )

    # ── Task 4: Data Quality Check ────────────────────────────────────────────
    data_quality_check = BashOperator(
        task_id="data_quality_check",
        bash_command=(
            f"echo '[data_quality] Running DQ checks...' && "
            f"cd {QUALITY_DIR} && "
            f"HDFS_NAMENODE_URL=${{HDFS_NAMENODE_URL:-http://namenode:9870}} "
            f"HDFS_USER=${{HDFS_USER:-root}} "
            f"DQ_REPORT_DIR=/opt/airflow/data/dq_reports "
            f"{PYTHON_CMD} data_quality.py "
            f"$(date -d 'today' +'%Y-%m-%d')"
        ),
        doc_md=(
            "Run 3 data quality rules on Silver reservoir data:\n"
            "  1. NULL CHECK on (reservoir_id, record_date, percent_storage)\n"
            "  2. DUPLICATE CHECK on (reservoir_id, record_date, source)\n"
            "  3. RANGE CHECK: percent_storage in [0, 100]\n"
            "Exits non-zero (pipeline fails) if any critical rule is violated."
        ),
    )

    # ── Task 5: Export Gold → JSON (for dashboard) ───────────────────────────
    export_dashboard = BashOperator(
        task_id="export_dashboard",
        bash_command=(
            f"HDFS_NAMENODE_URL=${{HDFS_NAMENODE_URL:-http://namenode:9870}} "
            f"HDFS_USER=${{HDFS_USER:-root}} "
            f"{PYTHON_CMD} {SCRIPTS_DIR}/export_gold_to_json.py "
            f"$(date -d 'today' +'%Y-%m-%d')"
        ),
        execution_timeout=timedelta(minutes=10),
        doc_md=(
            "Query Gold layer (Fact_Water_Monitoring, Fact_Flood_Alert) → "
            "write JSON snapshots to data/exports/ for the Cowork dashboard. "
            "Also exports DQ history and pipeline metadata."
        ),
    )

    # ── Task dependencies ─────────────────────────────────────────────────────
    # ingest_static / ingest_kafka / generate_weather run in parallel
    # → bronze_to_silver
    # → hive_init     (creates Silver external tables + Gold DDL + Alert DDL)
    # → silver_to_gold (loads Dim_* + Fact_Water_Monitoring + Fact_Flood_Risk)
    # → compute_features (7-day rolling reservoir features → Silver)
    # → flood_alert      (composite scoring → Gold Fact_Flood_Alert)
    # → data_quality_check
    # → export_dashboard (JSON snapshots for Cowork dashboard)
    [ingest_static, ingest_kafka, generate_weather] >> bronze_to_silver
    bronze_to_silver >> hive_init >> silver_to_gold >> compute_features >> flood_alert >> data_quality_check >> export_dashboard
