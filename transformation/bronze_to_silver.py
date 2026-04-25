"""
bronze_to_silver.py — Read Bronze HDFS data → clean → write Silver Parquet to HDFS

Transformations applied:
  - Type casting and null handling
  - Deduplication: sort by fetched_at DESC → keep latest record per key (deterministic)
  - percent_storage fallback: computed from volume_mcm / capacity_mcm when API returns 0
  - Standardise column names (snake_case)
  - Add ingestion metadata columns (silver_loaded_at, dt partition)
  - Validation report logged at end of each domain

Run:
    python bronze_to_silver.py [YYYY-MM-DD]   (default: today)

Multi-date behaviour:
  - Always processes today's Bronze partition (real-time data).
  - Also discovers ALL date partitions under /bronze/dam/ and /bronze/weather/ via
    WebHDFS LIST and processes every one that contains data.
  - This ensures historical backfill dates written by kafka_producer are picked up
    without the DAG needing to be re-run for past dates.
"""

import io
import json
import logging
import os
import sys
from datetime import datetime, timezone

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from hdfs import InsecureClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [bronze_to_silver] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
HDFS_URL = os.getenv("HDFS_NAMENODE_URL", "http://namenode:9870")
HDFS_USER = os.getenv("HDFS_USER", "root")


# ── Direct WebHDFS write (avoids chunked encoding issue with bde2020 DataNode) ──
def _webhdfs_write(hdfs_path: str, content: bytes, overwrite: bool = True) -> None:
    """
    PUT bytes to HDFS via WebHDFS REST API using requests.put(data=bytes).
    requests sets Content-Length automatically → no chunked Transfer-Encoding.
    """
    ov = "true" if overwrite else "false"
    nn_url = f"{HDFS_URL}/webhdfs/v1{hdfs_path}?op=CREATE&user.name={HDFS_USER}&overwrite={ov}"

    # Step 1: NameNode → 307 redirect to DataNode
    r1 = requests.put(nn_url, allow_redirects=False, timeout=30)
    if r1.status_code != 307:
        raise RuntimeError(f"WebHDFS CREATE step-1 failed for {hdfs_path}: HTTP {r1.status_code} — {r1.text[:300]}")

    dn_url = r1.headers.get("Location", "")
    if not dn_url:
        raise RuntimeError(f"WebHDFS CREATE step-1: missing Location header for {hdfs_path}")

    # Step 2: DataNode PUT — bytes body → Content-Length set, NOT chunked
    r2 = requests.put(
        dn_url,
        data=content,
        headers={"Content-Type": "application/octet-stream"},
        timeout=120,
    )
    if r2.status_code not in (200, 201):
        raise RuntimeError(f"WebHDFS CREATE step-2 failed for {hdfs_path}: HTTP {r2.status_code} — {r2.text[:300]}")

    log.info("HDFS write OK: %s (%d bytes)", hdfs_path, len(content))


def _webhdfs_makedirs(hdfs_path: str) -> None:
    url = f"{HDFS_URL}/webhdfs/v1{hdfs_path}?op=MKDIRS&user.name={HDFS_USER}&permission=755"
    requests.put(url, timeout=15)  # ignore errors (already exists is fine)


# ── HDFS helpers ──────────────────────────────────────────────────────────────
def list_hdfs_files(client: InsecureClient, path: str, suffix: str = "") -> list[str]:
    try:
        entries = client.list(path, status=False)
        return [f"{path}/{e}" for e in entries if e.endswith(suffix)]
    except Exception as exc:
        log.warning("Cannot list HDFS path %s: %s", path, exc)
        return []


def read_hdfs_ndjson(client: InsecureClient, hdfs_path: str) -> list[dict]:
    try:
        with client.read(hdfs_path) as reader:
            raw = reader.read().decode("utf-8")
        records = []
        for line in raw.splitlines():
            line = line.strip()
            if line:
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    log.warning("Skipping malformed JSON line in %s", hdfs_path)
        return records
    except Exception as exc:
        log.warning("Cannot read HDFS file %s: %s", hdfs_path, exc)
        return []


def write_hdfs_parquet(df: pd.DataFrame, hdfs_path: str) -> None:
    """Write DataFrame as Parquet to HDFS using direct WebHDFS (no chunked encoding)."""
    table = pa.Table.from_pandas(df, preserve_index=False)
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    parquet_bytes = buf.getvalue()

    parent = "/".join(hdfs_path.split("/")[:-1])
    _webhdfs_makedirs(parent)
    _webhdfs_write(hdfs_path, parquet_bytes, overwrite=True)
    log.info("Wrote %d rows → HDFS %s (Parquet/Snappy, %d bytes)", len(df), hdfs_path, len(parquet_bytes))


def read_hdfs_csv(client: InsecureClient, hdfs_path: str, encoding: str = "utf-8-sig") -> pd.DataFrame:
    with client.read(hdfs_path) as reader:
        raw = reader.read()
    return pd.read_csv(io.BytesIO(raw), encoding=encoding)


# ── Validation reporter ───────────────────────────────────────────────────────
def _report_quality(df: pd.DataFrame, domain: str, key_cols: list[str]) -> None:
    """Log a brief data-quality summary for a Silver DataFrame."""
    total = len(df)
    null_counts = {c: int(df[c].isna().sum()) for c in key_cols if c in df.columns}
    bad_keys = {c: v for c, v in null_counts.items() if v > 0}
    log.info(
        "[DQ/%s] rows=%d | nulls in key cols: %s",
        domain, total,
        bad_keys if bad_keys else "none",
    )


# ── Reservoir Silver ──────────────────────────────────────────────────────────
def process_reservoir(client: InsecureClient, date_str: str) -> int:
    """Read Bronze dam + reservoir NDJSON → clean → write Silver Parquet."""
    all_records: list[dict] = []

    for source in ("dam", "reservoir"):
        bronze_dir = f"/bronze/{source}/{date_str}"
        files = list_hdfs_files(client, bronze_dir, suffix=".json")
        for f in files:
            records = read_hdfs_ndjson(client, f)
            all_records.extend(records)
            log.info("Bronze %s/%s: %d records from %s", source, date_str, len(records), f)

    if not all_records:
        log.warning("No bronze reservoir/dam records found for %s.", date_str)
        return 0

    df = pd.DataFrame(all_records)
    log.info("Raw reservoir records: %d (dam + reservoir combined)", len(df))

    # ── Type casting ──────────────────────────────────────────────────────────
    numeric_cols = [
        "capacity_mcm", "volume_mcm", "percent_storage",
        "inflow_mcm", "outflow_mcm", "active_storage_mcm", "dead_storage_mcm",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    # ── Null / string handling ────────────────────────────────────────────────
    df["reservoir_id"]   = df["reservoir_id"].fillna("").astype(str).str.strip()
    df["reservoir_name"] = df["reservoir_name"].fillna("Unknown").astype(str).str.strip()
    df["owner"]          = df.get("owner", pd.Series("กรมชลประทาน", index=df.index)).fillna("กรมชลประทาน").astype(str).str.strip()

    # region: normalise empty-string → "Unknown" so downstream can filter reliably
    df["region"] = df.get("region", pd.Series("", index=df.index)).fillna("").astype(str).str.strip()
    df["region"] = df["region"].replace("", "Unknown")

    # Drop rows with no reservoir_id — cannot be keyed, joined, or tracked
    empty_id_mask = df["reservoir_id"] == ""
    if empty_id_mask.any():
        log.warning("Dropping %d rows with empty reservoir_id.", int(empty_id_mask.sum()))
        df = df[~empty_id_mask].reset_index(drop=True)

    # ── Normalise date column ─────────────────────────────────────────────────
    # kafka_producer uses 'record_date'; legacy raw records may use 'date'.
    if "record_date" in df.columns:
        date_src = df["record_date"]
    elif "date" in df.columns:
        date_src = df["date"]
    else:
        date_src = pd.Series([date_str] * len(df), index=df.index)

    df["record_date"] = pd.to_datetime(date_src, errors="coerce").dt.strftime("%Y-%m-%d")
    df["record_date"] = df["record_date"].fillna(date_str)

    # ── percent_storage: fallback computation ─────────────────────────────────
    # The RID API often returns percent_storage=0 even when volume_mcm and
    # capacity_mcm are valid (non-zero).  Recompute when that happens.
    needs_pct = (df["percent_storage"] == 0.0) & (df["capacity_mcm"] > 0)
    if needs_pct.any():
        computed = (df.loc[needs_pct, "volume_mcm"] / df.loc[needs_pct, "capacity_mcm"] * 100.0).clip(0, 100).round(2)
        df.loc[needs_pct, "percent_storage"] = computed
        log.info(
            "percent_storage recomputed from volume/capacity for %d rows (API returned 0).",
            int(needs_pct.sum()),
        )
    df["percent_storage"] = df["percent_storage"].clip(lower=0, upper=100)

    # ── Deterministic deduplication ───────────────────────────────────────────
    # Sort by fetched_at DESC so drop_duplicates(keep="first") always picks the
    # latest record — predictable behaviour regardless of ingestion order.
    if "fetched_at" in df.columns:
        df = df.sort_values("fetched_at", ascending=False, na_position="last")
    before = len(df)
    df = df.drop_duplicates(subset=["reservoir_id", "record_date", "source"], keep="first")
    log.info("Deduplication: %d → %d rows (kept latest by fetched_at)", before, len(df))

    # ── Add metadata ──────────────────────────────────────────────────────────
    df["silver_loaded_at"] = datetime.now(timezone.utc).isoformat()
    df["dt"] = date_str

    # ── Select & enforce final schema ─────────────────────────────────────────
    final_cols = [
        "reservoir_id", "reservoir_name", "region", "source", "owner",
        "record_date", "capacity_mcm", "volume_mcm", "percent_storage",
        "inflow_mcm", "outflow_mcm", "active_storage_mcm", "dead_storage_mcm",
        "fetched_at", "silver_loaded_at", "dt",
    ]
    for col in final_cols:
        if col not in df.columns:
            df[col] = None
    df = df[final_cols]

    # ── Validation report ─────────────────────────────────────────────────────
    _report_quality(df, "reservoir", ["reservoir_id", "record_date", "percent_storage", "capacity_mcm"])
    zero_pct = int((df["percent_storage"] == 0).sum())
    if zero_pct:
        log.warning("[DQ/reservoir] %d rows still have percent_storage=0 (capacity may also be 0).", zero_pct)
    log.info(
        "[DQ/reservoir] percent_storage — min=%.1f max=%.1f mean=%.1f",
        df["percent_storage"].min(), df["percent_storage"].max(), df["percent_storage"].mean(),
    )

    hdfs_path = f"/silver/reservoir/dt={date_str}/reservoir.parquet"
    write_hdfs_parquet(df, hdfs_path)
    return len(df)


# ── Flood Risk Silver ─────────────────────────────────────────────────────────
def process_flood_risk(client: InsecureClient) -> int:
    """Read Bronze flood_risk CSV → clean → write Silver Parquet."""
    bronze_path = "/bronze/flood_risk/flood_risk_area.csv"

    try:
        df = read_hdfs_csv(client, bronze_path, encoding="utf-8-sig")
    except Exception as exc:
        log.error("Cannot read flood risk CSV from HDFS: %s", exc)
        return 0

    log.info("Raw flood risk records: %d", len(df))

    # ── Rename columns to snake_case ──────────────────────────────────────────
    df = df.rename(columns={
        "Month":          "month",
        "GEOCODE":        "geocode",
        "TAMBON_T":       "tambon_th",
        "TAMBON_E":       "tambon_en",
        "AMPHOE_CODE":    "amphoe_code",
        "AMPHOE_T":       "amphoe_th",
        "AMPHOE_E":       "amphoe_en",
        "PROV_CODE":      "prov_code",
        "PROV_T":         "prov_th",
        "PROV_E":         "prov_en",
        "COUNT 17 YEAR":  "flood_count_17yr",
        "CRITERIA":       "flood_criteria",
        "RISK":           "risk_level",
    })

    # ── Type casting ──────────────────────────────────────────────────────────
    df["month"] = pd.to_numeric(df["month"], errors="coerce").fillna(0).astype(int)
    df["geocode"] = df["geocode"].astype(str).str.zfill(6)
    df["prov_code"] = pd.to_numeric(df["prov_code"], errors="coerce").fillna(0).astype(int)
    df["amphoe_code"] = pd.to_numeric(df["amphoe_code"], errors="coerce").fillna(0).astype(int)
    df["flood_count_17yr"] = pd.to_numeric(df["flood_count_17yr"], errors="coerce").fillna(0).astype(int)

    # ── Null handling ─────────────────────────────────────────────────────────
    df["tambon_th"] = df["tambon_th"].fillna("").str.strip()
    df["tambon_en"] = df["tambon_en"].fillna("").str.strip()
    df["risk_level"] = df["risk_level"].fillna("ไม่ระบุ").str.strip()

    # ── Deduplication (flood_risk CSV is static; dedup is a safety net) ─────────
    before = len(df)
    df = df.sort_values(["geocode", "month"])   # stable order for reproducibility
    df = df.drop_duplicates(subset=["geocode", "month"], keep="first")
    log.info("Flood risk deduplication: %d → %d rows", before, len(df))

    # ── Risk level numerical encoding ─────────────────────────────────────────
    risk_map = {"เสี่ยงต่ำ": 1, "เสี่ยงปานกลาง": 2, "เสี่ยงสูง": 3, "เสี่ยงสูงมาก": 4}
    df["risk_score"] = df["risk_level"].map(risk_map).fillna(0).astype(int)

    # ── Add metadata ──────────────────────────────────────────────────────────
    df["silver_loaded_at"] = datetime.now(timezone.utc).isoformat()

    # ── Validation report ─────────────────────────────────────────────────────
    _report_quality(df, "flood_risk", ["geocode", "month", "prov_code", "risk_score"])
    log.info(
        "[DQ/flood_risk] risk_score distribution: %s",
        df["risk_score"].value_counts().to_dict(),
    )

    hdfs_path = "/silver/flood_risk/flood_risk.parquet"
    write_hdfs_parquet(df, hdfs_path)
    return len(df)


# ── Weather Silver ────────────────────────────────────────────────────────────
def process_weather(client: InsecureClient, date_str: str) -> int:
    """Read Bronze weather NDJSON → clean → write Silver Parquet."""
    bronze_dir = f"/bronze/weather/{date_str}"
    files = list_hdfs_files(client, bronze_dir, suffix=".json")

    all_records: list[dict] = []
    for f in files:
        all_records.extend(read_hdfs_ndjson(client, f))

    if not all_records:
        log.warning("No bronze weather records found for %s.", date_str)
        return 0

    df = pd.DataFrame(all_records)
    log.info("Raw weather records: %d", len(df))

    # ── Type casting ──────────────────────────────────────────────────────────
    numeric_cols = ["temp_max_c", "temp_min_c", "rainfall_forecast_mm", "humidity_pct", "wind_speed_kmh"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    df["prov_code"] = pd.to_numeric(df.get("prov_code", 0), errors="coerce").fillna(0).astype(int)

    # is_heavy_rain_24h: handle Python bool (True/False), JSON bool (true/false),
    # and any string representation ("True", "1", "yes") that may come from
    # different ingestion paths.
    if "is_heavy_rain_24h" in df.columns:
        df["is_heavy_rain_24h"] = (
            df["is_heavy_rain_24h"]
            .map(lambda x: str(x).strip().lower() in ("true", "1", "yes", "t"))
            .fillna(False)
        )
    else:
        # Derive from rainfall if the field is missing entirely
        df["is_heavy_rain_24h"] = df.get("rainfall_forecast_mm", pd.Series(0.0, index=df.index)) >= 35.0

    df["forecast_date"] = df.get("forecast_date", pd.Series(date_str, index=df.index)).fillna(date_str)
    df["dt"] = date_str

    # Ensure required string columns exist
    for col in ("prov_th", "prov_en", "region", "rain_intensity", "data_source"):
        if col not in df.columns:
            df[col] = ""
        df[col] = df[col].fillna("").astype(str).str.strip()

    # ── Deterministic deduplication ───────────────────────────────────────────
    if "fetched_at" in df.columns:
        df = df.sort_values("fetched_at", ascending=False, na_position="last")
    before = len(df)
    df = df.drop_duplicates(subset=["prov_code", "forecast_date"], keep="first")
    log.info("Weather deduplication: %d → %d rows (kept latest by fetched_at)", before, len(df))

    df["silver_loaded_at"] = datetime.now(timezone.utc).isoformat()

    # ── Select final columns ──────────────────────────────────────────────────
    final_cols = [
        "prov_code", "prov_th", "prov_en", "region", "forecast_date",
        "temp_max_c", "temp_min_c", "rainfall_forecast_mm",
        "rain_intensity", "humidity_pct", "wind_speed_kmh",
        "is_heavy_rain_24h", "data_source", "silver_loaded_at", "dt",
    ]
    for col in final_cols:
        if col not in df.columns:
            df[col] = None
    df = df[final_cols]

    # ── Validation report ─────────────────────────────────────────────────────
    _report_quality(df, "weather", ["prov_code", "forecast_date", "rainfall_forecast_mm"])
    heavy = int(df["is_heavy_rain_24h"].sum())
    log.info(
        "[DQ/weather] rainfall — min=%.1f max=%.1f mean=%.1f | heavy_rain provinces=%d/%d",
        df["rainfall_forecast_mm"].min(), df["rainfall_forecast_mm"].max(),
        df["rainfall_forecast_mm"].mean(), heavy, len(df),
    )

    hdfs_path = f"/silver/weather/dt={date_str}/weather.parquet"
    write_hdfs_parquet(df, hdfs_path)
    return len(df)


# ── Bronze date discovery ──────────────────────────────────────────────────────
def _list_bronze_dates(roots):
    """
    Return sorted union of YYYY-MM-DD dates found under one or more HDFS roots.
    Pass a list to union across e.g. ["/bronze/dam", "/bronze/reservoir"].
    Uses WebHDFS LISTSTATUS -- no InsecureClient dependency.
    """
    if isinstance(roots, str):
        roots = [roots]
    dates = set()
    for root in roots:
        url = f"{HDFS_URL}/webhdfs/v1{root}?op=LISTSTATUS&user.name={HDFS_USER}"
        try:
            r = requests.get(url, timeout=15)
            if r.status_code == 404:
                continue
            r.raise_for_status()
            entries = r.json().get("FileStatuses", {}).get("FileStatus", [])
            for entry in entries:
                name = entry.get("pathSuffix", "").replace("dt=", "")
                try:
                    datetime.strptime(name, "%Y-%m-%d")
                    dates.add(name)
                except ValueError:
                    pass
        except Exception as exc:
            log.warning("Cannot list Bronze dates at %s: %s", root, exc)
    return sorted(dates)
def main():
    date_str = sys.argv[1] if len(sys.argv) > 1 else datetime.now(timezone.utc).strftime("%Y-%m-%d")
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        log.error("Invalid date: %s. Use YYYY-MM-DD.", date_str)
        sys.exit(1)

    log.info("Starting Bronze → Silver transformation — primary date: %s", date_str)
    client = InsecureClient(HDFS_URL, user=HDFS_USER)

    # ── Reservoir: process ALL available Bronze date partitions ───────────────
    # This handles both today's real-time data and backfilled historical dates.
    reservoir_dates = _list_bronze_dates(["/bronze/dam", "/bronze/reservoir"])
    if date_str not in reservoir_dates:
        reservoir_dates.append(date_str)  # always include today even if dir is empty
    reservoir_dates = sorted(set(reservoir_dates))
    log.info("Reservoir Bronze dates to process: %s", reservoir_dates)

    total_res = 0
    for dt in reservoir_dates:
        count = process_reservoir(client, dt)
        total_res += count
        log.info("Reservoir Silver dt=%s: %d rows written", dt, count)

    # ── Flood risk: static, process once ─────────────────────────────────────
    flood_count = process_flood_risk(client)

    # ── Weather: process ALL available Bronze date partitions ─────────────────
    weather_dates = _list_bronze_dates("/bronze/weather")
    if date_str not in weather_dates:
        weather_dates.append(date_str)
    weather_dates = sorted(set(weather_dates))
    log.info("Weather Bronze dates to process: %s", weather_dates)

    total_weather = 0
    for dt in weather_dates:
        count = process_weather(client, dt)
        total_weather += count
        log.info("Weather Silver dt=%s: %d rows written", dt, count)

    log.info(
        "Bronze → Silver complete — reservoir=%d rows across %d dates | "
        "flood_risk=%d | weather=%d rows across %d dates",
        total_res, len(reservoir_dates),
        flood_count,
        total_weather, len(weather_dates),
    )

    if total_res == 0 and flood_count == 0 and total_weather == 0:
        log.error("All transformations produced 0 rows — check Bronze layer.")
        sys.exit(1)


if __name__ == "__main__":
    main()