"""
compute_reservoir_features.py — Read Silver Parquet (last 7 days) → compute rolling
reservoir features → write to /silver/reservoir_features/dt=YYYY-MM-DD/

Features computed per reservoir:
  net_flow_mcm       inflow - outflow (today)
  storage_trend_7d   average percent_storage over last 7 days
  storage_delta_1d   percent_storage change vs. yesterday (pp)
  days_to_full       (capacity - volume) / net_flow  — positive = filling; negative = draining
  reservoir_status   CRITICAL (>=90%) | HIGH (>=75%) | NORMAL (>=40%) | LOW (<40%)
  filling_rate       FAST (delta>3pp/day) | MODERATE (0..3) | STABLE (-1..0) | DRAINING (<-1)

Run:
    python compute_reservoir_features.py [YYYY-MM-DD]   (default: today)
"""

import io
import logging
import os
import sys
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from hdfs import InsecureClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [compute_features] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
HDFS_URL  = os.getenv("HDFS_NAMENODE_URL", "http://namenode:9870")
HDFS_USER = os.getenv("HDFS_USER", "root")

SILVER_RESERVOIR_ROOT  = "/silver/reservoir"
SILVER_FEATURES_ROOT   = "/silver/reservoir_features"
LOOKBACK_DAYS          = 7   # rolling window for trend


# ── WebHDFS helpers ───────────────────────────────────────────────────────────
def _webhdfs_makedirs(hdfs_path: str) -> None:
    url = f"{HDFS_URL}/webhdfs/v1{hdfs_path}?op=MKDIRS&user.name={HDFS_USER}&permission=755"
    requests.put(url, timeout=15)


def _webhdfs_write(hdfs_path: str, content: bytes, overwrite: bool = True) -> None:
    ov = "true" if overwrite else "false"
    nn_url = f"{HDFS_URL}/webhdfs/v1{hdfs_path}?op=CREATE&user.name={HDFS_USER}&overwrite={ov}"
    r1 = requests.put(nn_url, allow_redirects=False, timeout=30)
    if r1.status_code != 307:
        raise RuntimeError(
            f"WebHDFS CREATE step-1 failed for {hdfs_path}: "
            f"HTTP {r1.status_code} — {r1.text[:300]}"
        )
    dn_url = r1.headers.get("Location", "")
    if not dn_url:
        raise RuntimeError(f"WebHDFS CREATE step-1: missing Location header for {hdfs_path}")
    r2 = requests.put(
        dn_url,
        data=content,
        headers={"Content-Type": "application/octet-stream"},
        timeout=120,
    )
    if r2.status_code not in (200, 201):
        raise RuntimeError(
            f"WebHDFS CREATE step-2 failed for {hdfs_path}: "
            f"HTTP {r2.status_code} — {r2.text[:300]}"
        )
    log.info("HDFS write OK: %s (%d bytes)", hdfs_path, len(content))


def write_hdfs_parquet(df: pd.DataFrame, hdfs_path: str) -> None:
    table = pa.Table.from_pandas(df, preserve_index=False)
    buf   = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    parquet_bytes = buf.getvalue()
    parent = "/".join(hdfs_path.split("/")[:-1])
    _webhdfs_makedirs(parent)
    _webhdfs_write(hdfs_path, parquet_bytes, overwrite=True)
    log.info(
        "Wrote %d rows → HDFS %s (Parquet/Snappy, %d bytes)",
        len(df), hdfs_path, len(parquet_bytes),
    )


# ── Read Silver Parquet from HDFS ─────────────────────────────────────────────
def _read_parquet_from_hdfs(client: InsecureClient, hdfs_path: str) -> pd.DataFrame:
    try:
        with client.read(hdfs_path) as reader:
            raw = reader.read()
        return pd.read_parquet(io.BytesIO(raw))
    except Exception as exc:
        log.warning("Cannot read parquet %s: %s", hdfs_path, exc)
        return pd.DataFrame()


def load_silver_window(client: InsecureClient, target_date: str) -> pd.DataFrame:
    """
    Load Silver reservoir Parquet for target_date and up to LOOKBACK_DAYS-1 days prior.
    Returns combined DataFrame with all available rows.
    """
    end_dt   = datetime.strptime(target_date, "%Y-%m-%d")
    all_dfs  = []

    for offset in range(LOOKBACK_DAYS):
        day_str = (end_dt - timedelta(days=offset)).strftime("%Y-%m-%d")
        part_dir = f"{SILVER_RESERVOIR_ROOT}/dt={day_str}"

        # List parquet files in the partition dir
        try:
            entries = client.list(part_dir, status=False)
        except Exception:
            log.debug("Silver partition not found: %s", part_dir)
            continue

        for entry in entries:
            if not entry.endswith(".parquet"):
                continue
            df = _read_parquet_from_hdfs(client, f"{part_dir}/{entry}")
            if not df.empty:
                df["_partition_date"] = day_str
                all_dfs.append(df)

    if not all_dfs:
        return pd.DataFrame()

    combined = pd.concat(all_dfs, ignore_index=True)
    log.info(
        "Loaded %d Silver rows across %d-day window ending %s.",
        len(combined), LOOKBACK_DAYS, target_date,
    )
    return combined


# ── Feature computation ───────────────────────────────────────────────────────
def _safe_div(num: float, denom: float, default: float = float("nan")) -> float:
    if denom == 0 or np.isnan(denom):
        return default
    return num / denom


def classify_status(pct: float) -> str:
    if pct >= 90.0:
        return "CRITICAL"
    if pct >= 75.0:
        return "HIGH"
    if pct >= 40.0:
        return "NORMAL"
    return "LOW"


def classify_filling_rate(delta_1d: float) -> str:
    """delta_1d in percentage points per day."""
    if delta_1d > 3.0:
        return "FAST"
    if delta_1d > 0.0:
        return "MODERATE"
    if delta_1d >= -1.0:
        return "STABLE"
    return "DRAINING"


def compute_features(df: pd.DataFrame, target_date: str) -> pd.DataFrame:
    """
    Compute rolling features for each reservoir based on the 7-day window.
    Returns one row per (reservoir_id, source) for target_date.
    """
    if df.empty:
        log.warning("Empty dataframe — no features to compute.")
        return pd.DataFrame()

    # Ensure types
    df["percent_storage"] = pd.to_numeric(df["percent_storage"], errors="coerce").fillna(0.0)
    df["volume_mcm"]      = pd.to_numeric(df["volume_mcm"],      errors="coerce").fillna(0.0)
    df["capacity_mcm"]    = pd.to_numeric(df["capacity_mcm"],    errors="coerce").fillna(0.0)
    df["inflow_mcm"]      = pd.to_numeric(df["inflow_mcm"],      errors="coerce").fillna(0.0)
    df["outflow_mcm"]     = pd.to_numeric(df["outflow_mcm"],     errors="coerce").fillna(0.0)
    df["record_date"]     = pd.to_datetime(df["record_date"],    errors="coerce")

    # Keep only the latest record per (reservoir_id, source, partition_date) to avoid dupes
    df = df.sort_values("record_date", ascending=True)
    df = df.drop_duplicates(subset=["reservoir_id", "source", "_partition_date"], keep="last")

    # Separate today's slice vs. historical
    target_dt = pd.Timestamp(target_date)
    today_df  = df[df["record_date"] == target_dt].copy()

    if today_df.empty:
        log.warning("No Silver rows found for target_date=%s — using latest available.", target_date)
        # Fall back to the latest date present
        today_df = df[df["record_date"] == df["record_date"].max()].copy()

    yesterday_dt  = target_dt - pd.Timedelta(days=1)
    yesterday_df  = df[df["record_date"] == yesterday_dt][
        ["reservoir_id", "source", "percent_storage"]
    ].rename(columns={"percent_storage": "pct_yesterday"})

    # 7-day mean per reservoir
    trend_df = (
        df.groupby(["reservoir_id", "source"])["percent_storage"]
        .mean()
        .reset_index()
        .rename(columns={"percent_storage": "storage_trend_7d"})
    )

    # Join enrichments onto today's slice
    out = today_df.merge(yesterday_df, on=["reservoir_id", "source"], how="left")
    out = out.merge(trend_df,           on=["reservoir_id", "source"], how="left")

    # ── Derived features ──────────────────────────────────────────────────────
    out["net_flow_mcm"] = out["inflow_mcm"] - out["outflow_mcm"]

    out["storage_delta_1d"] = out["percent_storage"] - out.get("pct_yesterday", pd.Series(dtype=float))
    out["storage_delta_1d"] = out["storage_delta_1d"].fillna(0.0)

    # days_to_full: how many days until capacity is reached at current net flow rate
    # Positive → filling; NaN → net flow ≈ 0 (stable)
    def _days_to_full(row) -> float:
        net = row["net_flow_mcm"]
        remaining = row["capacity_mcm"] - row["volume_mcm"]
        if abs(net) < 0.001:   # near-zero flow → undefined
            return float("nan")
        days = remaining / net
        # Cap at ±365 to avoid unbounded values
        return max(-365.0, min(365.0, days))

    out["days_to_full"] = out.apply(_days_to_full, axis=1)

    out["reservoir_status"] = out["percent_storage"].apply(classify_status)
    out["filling_rate"]     = out["storage_delta_1d"].apply(classify_filling_rate)

    # ── Output schema ──────────────────────────────────────────────────────────
    out["feature_date"]   = target_date
    out["computed_at"]    = datetime.now(timezone.utc).isoformat()

    keep_cols = [
        "feature_date",
        "reservoir_id",
        "reservoir_name",
        "source",
        "region",
        "capacity_mcm",
        "volume_mcm",
        "percent_storage",
        "inflow_mcm",
        "outflow_mcm",
        "net_flow_mcm",
        "storage_trend_7d",
        "storage_delta_1d",
        "days_to_full",
        "reservoir_status",
        "filling_rate",
        "computed_at",
    ]
    # Only keep columns that exist
    keep_cols = [c for c in keep_cols if c in out.columns]
    out = out[keep_cols].reset_index(drop=True)

    log.info(
        "Computed features for %d reservoirs on %s. "
        "Status breakdown: %s",
        len(out),
        target_date,
        out["reservoir_status"].value_counts().to_dict(),
    )
    return out


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    target_date = (
        sys.argv[1]
        if len(sys.argv) > 1
        else datetime.now(timezone.utc).strftime("%Y-%m-%d")
    )
    log.info("compute_reservoir_features — target_date=%s", target_date)

    client = InsecureClient(HDFS_URL, user=HDFS_USER)

    # 1. Load 7-day Silver window
    df = load_silver_window(client, target_date)
    if df.empty:
        log.error("No Silver data available for the last %d days. Exiting.", LOOKBACK_DAYS)
        sys.exit(1)

    # 2. Compute features
    features_df = compute_features(df, target_date)
    if features_df.empty:
        log.error("Feature computation returned empty DataFrame. Exiting.")
        sys.exit(1)

    # 3. Write to Silver features partition
    out_path = f"{SILVER_FEATURES_ROOT}/dt={target_date}/reservoir_features.parquet"
    write_hdfs_parquet(features_df, out_path)

    log.info(
        "Done — %d feature rows written to %s.",
        len(features_df), out_path,
    )


if __name__ == "__main__":
    main()
