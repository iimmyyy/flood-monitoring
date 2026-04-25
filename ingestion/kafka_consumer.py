"""
kafka_consumer.py — Consume reservoir_updates from Kafka → write NEW records to HDFS Bronze

Watermark: last processed fetched_at timestamp is persisted in WATERMARK_FILE.
Only records with fetched_at > watermark are written (idempotent re-runs safe).

Run inside Airflow or standalone:
    python kafka_consumer.py
"""

import json
import logging
import os
import sys
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaError, KafkaException

from hdfs_util import hdfs_makedirs, hdfs_write

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [kafka_consumer] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC     = os.getenv("KAFKA_TOPIC", "reservoir_updates")
KAFKA_GROUP     = os.getenv("KAFKA_CONSUMER_GROUP", "flood-monitoring-group")

HDFS_URL  = os.getenv("HDFS_NAMENODE_URL", "http://namenode:9870")
HDFS_USER = os.getenv("HDFS_USER", "root")

WATERMARK_FILE    = os.getenv("PIPELINE_WATERMARK_FILE", "/opt/airflow/data/.watermark.json")
POLL_TIMEOUT_SECONDS = 5
MAX_EMPTY_POLLS   = 6   # stop after this many consecutive empty polls


# ── Watermark helpers ─────────────────────────────────────────────────────────
def load_watermark() -> str:
    if os.path.exists(WATERMARK_FILE):
        try:
            with open(WATERMARK_FILE) as f:
                ts = json.load(f).get("last_fetched_at", "1970-01-01T00:00:00+00:00")
                log.info("Watermark loaded: %s", ts)
                return ts
        except Exception as exc:
            log.warning("Could not read watermark file: %s", exc)
    return "1970-01-01T00:00:00+00:00"


def save_watermark(fetched_at: str) -> None:
    os.makedirs(os.path.dirname(WATERMARK_FILE) or ".", exist_ok=True)
    with open(WATERMARK_FILE, "w") as f:
        json.dump(
            {"last_fetched_at": fetched_at, "updated_at": datetime.now(timezone.utc).isoformat()},
            f,
        )
    log.info("Watermark updated to: %s", fetched_at)


# ── HDFS write ────────────────────────────────────────────────────────────────
def write_to_hdfs_bronze(records: list[dict]) -> None:
    """
    Write records to HDFS Bronze, partitioned by source & date:
      /bronze/{source}/YYYY-MM-DD/{source}_HHMMSS.json  (NDJSON)
    """
    # Group by (source, date)
    buckets: dict[tuple, list[dict]] = {}
    for r in records:
        date_val = r.get("record_date") or r.get("date") or datetime.now().strftime("%Y-%m-%d")
        source   = r.get("source", "reservoir")
        buckets.setdefault((source, date_val), []).append(r)

    ts = datetime.now().strftime("%H%M%S")

    for (source, date_val), bucket_records in buckets.items():
        hdfs_dir  = f"/bronze/{source}/{date_val}"
        hdfs_path = f"{hdfs_dir}/{source}_{ts}.json"

        hdfs_makedirs(hdfs_dir, hdfs_url=HDFS_URL, user=HDFS_USER)

        content = "\n".join(json.dumps(r, ensure_ascii=False) for r in bucket_records)
        try:
            hdfs_write(hdfs_path, content.encode("utf-8"), hdfs_url=HDFS_URL, user=HDFS_USER, overwrite=True)
            log.info("Wrote %d records → HDFS %s", len(bucket_records), hdfs_path)
        except Exception as exc:
            log.error("HDFS write failed for %s: %s", hdfs_path, exc)
            raise


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    watermark_ts = load_watermark()

    log.info(
        "Starting Kafka consumer — bootstrap=%s, topic=%s, watermark=%s",
        KAFKA_BOOTSTRAP, KAFKA_TOPIC, watermark_ts,
    )

    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": KAFKA_GROUP,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([KAFKA_TOPIC])

    new_records:   list[dict] = []
    max_fetched_at = watermark_ts
    empty_polls    = 0

    try:
        while empty_polls < MAX_EMPTY_POLLS:
            msg = consumer.poll(timeout=POLL_TIMEOUT_SECONDS)

            if msg is None:
                empty_polls += 1
                log.debug("Empty poll %d/%d", empty_polls, MAX_EMPTY_POLLS)
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    empty_polls += 1
                    continue
                raise KafkaException(msg.error())

            empty_polls = 0  # reset on successful message

            try:
                record = json.loads(msg.value().decode("utf-8"))
            except json.JSONDecodeError as exc:
                log.warning("Skipping malformed Kafka message: %s", exc)
                consumer.commit(message=msg)
                continue

            record_ts = record.get("fetched_at", "")

            # Watermark filter
            if record_ts <= watermark_ts:
                log.debug("Skipping already-processed record (fetched_at=%s)", record_ts)
                consumer.commit(message=msg)
                continue

            new_records.append(record)
            if record_ts > max_fetched_at:
                max_fetched_at = record_ts

            consumer.commit(message=msg)

    except KeyboardInterrupt:
        log.info("Consumer interrupted.")
    finally:
        consumer.close()

    log.info("Consumer finished — %d new records above watermark.", len(new_records))

    if not new_records:
        log.info("No new records to write. Pipeline is up-to-date.")
        return

    write_to_hdfs_bronze(new_records)
    save_watermark(max_fetched_at)
    log.info("Successfully processed %d new records.", len(new_records))


if __name__ == "__main__":
    main()
