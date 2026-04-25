"""
kafka_producer.py — Fetch reservoir & dam data from RID API → publish to Kafka topic 'reservoir_updates'

RID API structure:
  Dam (large) real-time:   GET /dam/public
  Dam (large) historical:  GET /dam/public/YYYY-MM-DD
  Response: {'data': [{'region': '...', 'dam': [{dam_fields...}]}, ...], 'date': '...', 'total': N}

  Reservoir real-time:     GET /reservoir/public
  Reservoir historical:    GET /reservoir/public/YYYY-MM-DD
  Response: {'data': [{reservoir_fields...}, ...]}

  When called with today's date (or no date) → real-time endpoint
  When called with a past date             → historical endpoint /YYYY-MM-DD

Run:
    python kafka_producer.py [YYYY-MM-DD]   (default: today)
"""

import json
import logging
import os
import sys
import warnings
from datetime import datetime, timedelta, timezone

import requests
import urllib3
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

# Suppress SSL warnings (RID API has cert issues)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [kafka_producer] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "reservoir_updates")

# Base URLs — date suffix appended automatically for historical requests
RID_DAM_BASE = os.getenv(
    "RID_DAM_API_URL", "https://app.rid.go.th/reservoir/api/dam/public"
)
RID_RESERVOIR_BASE = os.getenv(
    "RID_RESERVOIR_API_URL", "https://app.rid.go.th/reservoir/api/reservoir/public"
)

# State file — tracks last historical date fetched so we know first-run vs incremental
PRODUCER_STATE_FILE = os.getenv(
    "PRODUCER_STATE_FILE",
    "/opt/airflow/data/.producer_state.json",
)
# How many days to backfill on the very first run
BACKFILL_DAYS = int(os.getenv("BACKFILL_DAYS", "7"))

REQUEST_TIMEOUT = 30


def _build_rid_url(base: str, target_date: str) -> str:
    """
    Return the real-time or historical RID endpoint URL.
    - today / no date  → base URL  (e.g. /dam/public)
    - past date        → base/YYYY-MM-DD (e.g. /dam/public/2026-04-20)
    """
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if target_date and target_date != today:
        return f"{base}/{target_date}"
    return base


# Browser-like User-Agent required by RID dam endpoint
BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
}


# ── Kafka helpers ─────────────────────────────────────────────────────────────
def ensure_topic(bootstrap: str, topic: str) -> None:
    admin = AdminClient({"bootstrap.servers": bootstrap})
    existing = admin.list_topics(timeout=10).topics
    if topic not in existing:
        log.info("Creating Kafka topic: %s", topic)
        fs = admin.create_topics(
            [NewTopic(topic, num_partitions=3, replication_factor=1)]
        )
        for t, f in fs.items():
            try:
                f.result()
                log.info("Topic '%s' created.", t)
            except Exception as exc:
                log.warning("Topic creation warning: %s", exc)
    else:
        log.info("Kafka topic '%s' already exists.", topic)


def delivery_report(err, msg):
    if err is not None:
        log.error("Kafka delivery failed: %s", err)
    else:
        log.debug(
            "Delivered to %s [%d] @ offset %d",
            msg.topic(),
            msg.partition(),
            msg.offset(),
        )


# ── RID Dam API (large reservoirs) ───────────────────────────────────────────
def fetch_rid_dam_data(target_date: str = "") -> list[dict]:
    """
    Fetch large dam data.
    Response: {'data': [{'region': 'ภาคเหนือ', 'dam': [{...}, ...]}, ...], 'date': '2026-04-22', 'total': N}
    We flatten region → individual dam records.
    Uses historical endpoint (/dam/public/YYYY-MM-DD) when target_date is a past date.
    """
    url = _build_rid_url(RID_DAM_BASE, target_date)
    log.info("Fetching RID Dam data from: %s", url)
    try:
        resp = requests.get(
            url,
            headers=BROWSER_HEADERS,
            verify=False,
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        payload = resp.json()
    except requests.RequestException as exc:
        log.error("RID Dam API request failed: %s", exc)
        return []
    except json.JSONDecodeError as exc:
        log.error("RID Dam API JSON parse error: %s", exc)
        return []

    report_date = payload.get("date", "")
    region_list = payload.get("data", [])

    if not isinstance(region_list, list):
        log.warning("RID Dam API: unexpected 'data' type: %s", type(region_list))
        return []

    records = []
    for region_obj in region_list:
        region_name = region_obj.get("region", "")
        dam_list = region_obj.get("dam", [])
        if not isinstance(dam_list, list):
            continue
        for dam in dam_list:
            dam_copy = dict(dam)
            dam_copy["region"] = region_name
            dam_copy["report_date"] = report_date  # date from top-level payload
            records.append(dam_copy)

    log.info(
        "RID Dam API: fetched %d dam records across %d regions.",
        len(records),
        len(region_list),
    )
    return records


# ── RID Reservoir API (small/medium reservoirs) ───────────────────────────────
def fetch_rid_reservoir_data(target_date: str = "") -> list[dict]:
    """
    Fetch small/medium reservoir data.
    Flattens the nested region -> reservoir structure into a flat list.
    """
    url = _build_rid_url(RID_RESERVOIR_BASE, target_date)
    log.info("Fetching RID Reservoir data from: %s", url)
    try:
        resp = requests.get(url, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        payload = resp.json()
    except requests.RequestException as exc:
        log.error("RID Reservoir API request failed: %s", exc)
        return []
    except json.JSONDecodeError as exc:
        log.error("RID Reservoir API JSON parse error: %s", exc)
        return []

    report_date = payload.get("date", "")
    region_list = payload.get("data", [])

    if not isinstance(region_list, list):
        log.warning("RID Reservoir API: unexpected 'data' type: %s", type(region_list))
        return []

    records = []
    # Flatten the data exactly like we do for the large dams
    for region_obj in region_list:
        region_name = region_obj.get("region", "")
        # The key here is "reservoir", not "dam"
        reservoir_list = region_obj.get("reservoir", [])

        if not isinstance(reservoir_list, list):
            continue

        for rsv in reservoir_list:
            rsv_copy = dict(rsv)
            rsv_copy["region"] = region_name
            rsv_copy["report_date"] = report_date
            records.append(rsv_copy)

    log.info(
        "RID Reservoir API: fetched %d reservoir records across %d regions.",
        len(records),
        len(region_list),
    )
    return records


# ── Normalize to common schema ────────────────────────────────────────────────
def safe_float(value, default: float = 0.0) -> float:
    try:
        return float(value) if value not in (None, "", "N/A") else default
    except (ValueError, TypeError):
        return default


def normalize_dam_record(raw: dict, fetched_at: str) -> dict:
    """Normalize a large dam record (from /dam/public) to common schema."""
    # RID dam fields (from big_dam.py example):
    #   name, storage (current volume MCM), percent_storage, inflow, outflow
    #   May also have: id, code, capacity, dead_storage, owner, lat, lon
    reservoir_id = str(raw.get("id") or raw.get("code") or raw.get("name", "")).strip()
    return {
        "source": "dam",
        "reservoir_id": reservoir_id,
        "reservoir_name": str(raw.get("name", "")),
        "region": str(raw.get("region", "")),
        "owner": str(raw.get("owner", "กรมชลประทาน")),
        "record_date": str(raw.get("report_date") or raw.get("date", "")),
        "capacity_mcm": safe_float(raw.get("capacity")),
        "volume_mcm": safe_float(raw.get("storage")),  # 'storage' = current volume
        "percent_storage": safe_float(raw.get("percent_storage")),
        "inflow_mcm": safe_float(raw.get("inflow")),
        "outflow_mcm": safe_float(raw.get("outflow")),
        "active_storage_mcm": safe_float(raw.get("active_storage")),
        "dead_storage_mcm": safe_float(raw.get("dead_storage")),
        "fetched_at": fetched_at,
    }


def normalize_reservoir_record(raw: dict, fetched_at: str) -> dict:
    """Normalize a small/medium reservoir record (from /reservoir/public) to common schema."""
    reservoir_id = str(
        raw.get("id") or raw.get("code") or raw.get("reservoir_id", "")
    ).strip()
    return {
        "source": "reservoir",
        "reservoir_id": reservoir_id,
        "reservoir_name": str(raw.get("name") or raw.get("reservoir_name", "")),
        "region": str(raw.get("region", "")),
        "owner": str(raw.get("owner", "กรมชลประทาน")),
        "record_date": str(raw.get("date") or raw.get("record_date", "")),
        "capacity_mcm": safe_float(raw.get("capacity")),
        "volume_mcm": safe_float(raw.get("volume") or raw.get("storage")),
        "percent_storage": safe_float(raw.get("percent_storage")),
        "inflow_mcm": safe_float(raw.get("inflow")),
        "outflow_mcm": safe_float(raw.get("outflow")),
        "active_storage_mcm": safe_float(raw.get("active_storage")),
        "dead_storage_mcm": safe_float(raw.get("dead_storage")),
        "fetched_at": fetched_at,
    }


# ── Producer state (first-run detection) ─────────────────────────────────────
def _load_producer_state() -> dict:
    if os.path.exists(PRODUCER_STATE_FILE):
        try:
            with open(PRODUCER_STATE_FILE) as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def _save_producer_state(state: dict) -> None:
    os.makedirs(os.path.dirname(PRODUCER_STATE_FILE) or ".", exist_ok=True)
    tmp = PRODUCER_STATE_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(state, f)
    os.replace(tmp, PRODUCER_STATE_FILE)


def _get_historical_dates() -> list[str]:
    """
    Return the list of past dates to fetch from the historical API.

    First run  (state file absent or no last_historical_date):
        → backfill last BACKFILL_DAYS days (default 7)
    Subsequent run:
        → only yesterday (keep Bronze incrementally up-to-date)
    """
    state = _load_producer_state()
    today = datetime.now(timezone.utc).date()
    yesterday = today - timedelta(days=1)

    if not state.get("last_historical_date"):
        # First run — backfill N days (oldest → newest so Bronze is ordered)
        dates = [
            (today - timedelta(days=i)).strftime("%Y-%m-%d")
            for i in range(BACKFILL_DAYS, 0, -1)
        ]
        log.info(
            "First run detected — backfilling %d historical days: %s to %s",
            len(dates), dates[0], dates[-1],
        )
    else:
        dates = [yesterday.strftime("%Y-%m-%d")]
        log.info("Incremental run — fetching historical date: %s", dates[0])

    return dates


# ── Publish helpers ───────────────────────────────────────────────────────────
def _publish_batch(
    producer,
    raw_records: list[dict],
    normalize_fn,
    fetched_at: str,
    source_label: str,
) -> int:
    """Normalize and publish a list of raw API records. Returns count published."""
    count = 0
    for raw in raw_records:
        record = normalize_fn(raw, fetched_at)
        key = (
            f"{record['reservoir_id']}_{record['record_date']}_{record['source']}"
        )
        producer.produce(
            KAFKA_TOPIC,
            key=key,
            value=json.dumps(record, ensure_ascii=False),
            callback=delivery_report,
        )
        count += 1
    log.info("Published %d %s records.", count, source_label)
    return count


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    fetched_at = datetime.now(timezone.utc).isoformat()

    # Determine which historical dates to fetch
    hist_dates = _get_historical_dates()

    log.info(
        "Starting Kafka producer — fetched_at=%s | real-time + %d historical date(s)",
        fetched_at, len(hist_dates),
    )

    ensure_topic(KAFKA_BOOTSTRAP, KAFKA_TOPIC)

    producer = Producer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "acks": "all",
            "retries": 3,
            "retry.backoff.ms": 500,
        }
    )

    total_published = 0

    # -- Phase 1: Real-time snapshot (/public -- no date suffix) ---------------
    # Always fetched on every run; represents the current-day live reading.
    # Daily historical data is more complete (API fills in nulls only after day closes),
    # so this real-time record is mainly used for today's dashboard view.
    log.info("Phase 1/2 -- Fetching real-time data from /public endpoints...")
    total_published += _publish_batch(
        producer, fetch_rid_dam_data(""),
        normalize_dam_record, fetched_at, "dam (real-time)",
    )
    total_published += _publish_batch(
        producer, fetch_rid_reservoir_data(""),
        normalize_reservoir_record, fetched_at, "reservoir (real-time)",
    )

    # -- Phase 2: Historical daily snapshots (/public/YYYY-MM-DD) --------------
    # More complete than real-time -- the API fills missing fields after day closes.
    # First run: BACKFILL_DAYS days.  Subsequent runs: yesterday only.
    log.info("Phase 2/2 -- Fetching historical data for %d date(s): %s",
             len(hist_dates), hist_dates)
    for date_str in hist_dates:
        log.info("  Historical date: %s", date_str)
        total_published += _publish_batch(
            producer, fetch_rid_dam_data(date_str),
            normalize_dam_record, fetched_at, f"dam ({date_str})",
        )
        total_published += _publish_batch(
            producer, fetch_rid_reservoir_data(date_str),
            normalize_reservoir_record, fetched_at, f"reservoir ({date_str})",
        )

    producer.flush()
    log.info(
        "Producer done -- %d total records published to topic '%s'.",
        total_published, KAFKA_TOPIC,
    )

    # Persist state so next run knows it is incremental
    state = _load_producer_state()
    state["last_historical_date"] = hist_dates[-1]
    state["last_run_at"] = fetched_at
    state["total_historical_dates_fetched"] = (
        state.get("total_historical_dates_fetched", 0) + len(hist_dates)
    )
    _save_producer_state(state)
    log.info("Producer state saved — last_historical_date=%s", hist_dates[-1])


if __name__ == "__main__":
    main()
