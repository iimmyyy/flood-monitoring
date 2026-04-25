"""
ingest_static.py — Load the static Flood Risk Area CSV to HDFS Bronze

CSV columns:
    Month, GEOCODE, TAMBON_T, TAMBON_E, AMPHOE_CODE, AMPHOE_T, AMPHOE_E,
    PROV_CODE, PROV_T, PROV_E, COUNT 17 YEAR, CRITERIA, RISK

Target HDFS path:  /bronze/flood_risk/flood_risk_area.csv

Run:
    python ingest_static.py [/path/to/flood_risk_area.csv]
"""

import json
import logging
import os
import sys
from datetime import datetime

from hdfs_util import hdfs_makedirs, hdfs_status, hdfs_write

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [ingest_static] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
HDFS_URL        = os.getenv("HDFS_NAMENODE_URL", "http://namenode:9870")
HDFS_USER       = os.getenv("HDFS_USER", "root")
HDFS_BRONZE_PATH = "/bronze/flood_risk/flood_risk_area.csv"

DEFAULT_CSV_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "flood_risk_area.csv")


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    csv_path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_CSV_PATH
    csv_path = os.path.abspath(csv_path)

    if not os.path.exists(csv_path):
        log.error("CSV file not found: %s", csv_path)
        log.error(
            "Please place flood_risk_area.csv in the data/ folder "
            "or pass its path as an argument."
        )
        sys.exit(1)

    file_size_mb = os.path.getsize(csv_path) / (1024 * 1024)
    log.info("Loading CSV: %s (%.2f MB)", csv_path, file_size_mb)

    # Ensure bronze directory exists
    bronze_dir = os.path.dirname(HDFS_BRONZE_PATH)
    hdfs_makedirs(bronze_dir, hdfs_url=HDFS_URL, user=HDFS_USER)
    log.info("HDFS directory ensured: %s", bronze_dir)

    # Read entire file into memory
    log.info("Reading CSV into memory...")
    with open(csv_path, "rb") as f:
        csv_bytes = f.read()
    log.info("Read %.2f MB — uploading to HDFS: %s", len(csv_bytes) / 1024**2, HDFS_BRONZE_PATH)

    # Upload using direct WebHDFS REST (no chunked encoding)
    try:
        hdfs_write(HDFS_BRONZE_PATH, csv_bytes, hdfs_url=HDFS_URL, user=HDFS_USER, overwrite=True)
    except Exception as exc:
        log.error("HDFS upload failed: %s", exc)
        sys.exit(1)

    # Verify
    try:
        status = hdfs_status(HDFS_BRONZE_PATH, hdfs_url=HDFS_URL, user=HDFS_USER)
        if status:
            hdfs_size_mb = status.get("length", 0) / (1024 * 1024)
            log.info(
                "Upload verified — HDFS path: %s (%.2f MB, %d bytes)",
                HDFS_BRONZE_PATH,
                hdfs_size_mb,
                status.get("length", 0),
            )
    except Exception as exc:
        log.warning("Could not verify HDFS file status: %s", exc)

    # Write metadata sidecar
    meta_path = bronze_dir + "/_meta.json"
    meta = {
        "source_file": os.path.basename(csv_path),
        "loaded_at": datetime.utcnow().isoformat() + "Z",
        "hdfs_path": HDFS_BRONZE_PATH,
        "description": "Monthly flood risk area — 17-year historical records per sub-district (สสน.)",
    }
    try:
        hdfs_write(
            meta_path,
            json.dumps(meta, ensure_ascii=False, indent=2).encode("utf-8"),
            hdfs_url=HDFS_URL,
            user=HDFS_USER,
            overwrite=True,
        )
        log.info("Metadata written to %s", meta_path)
    except Exception as exc:
        log.warning("Could not write metadata: %s", exc)

    log.info("Static ingestion complete.")


if __name__ == "__main__":
    main()
