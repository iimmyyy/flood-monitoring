"""
export_gold_to_json.py — Read HDFS Silver Parquet / Gold ORC → write JSON snapshots for dashboard

Reads Silver layer via WebHDFS (pyarrow + pandas) — same stack as bronze_to_silver.py.
No pyhive / beeline dependency required.

Outputs (in data/exports/):
  reservoirs.json    — latest Silver reservoir data + features
  alerts.json        — alert scoring per province (Gold Fact_Flood_Alert, or Silver weather fallback)
  pipeline_meta.json — last run metadata (watermark + DQ summary)
  dq_latest.json     — latest DQ report rules + run history

Run:
    python export_gold_to_json.py [YYYY-MM-DD]   (default: today)
"""

import glob
import io
import json
import logging
import math
import os
import sys
from datetime import datetime, timedelta, timezone

import pandas as pd
import requests
from hdfs import InsecureClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [export_gold] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────
HDFS_URL  = os.getenv("HDFS_NAMENODE_URL", "http://namenode:9870")
HDFS_USER = os.getenv("HDFS_USER", "root")

_SCRIPT_DIR    = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT   = os.path.join(_SCRIPT_DIR, "..")
EXPORT_DIR     = os.path.join(PROJECT_ROOT, "data", "exports")
DQ_DIR         = os.path.join(PROJECT_ROOT, "data", "dq_reports")
WATERMARK_FILE = os.path.join(PROJECT_ROOT, "data", ".watermark.json")

SILVER_RESERVOIR_ROOT = "/silver/reservoir"
SILVER_FEATURES_ROOT  = "/silver/reservoir_features"
SILVER_WEATHER_ROOT   = "/silver/weather"
SILVER_RISK_ROOT      = "/silver/flood_risk"
GOLD_ALERT_ROOT       = "/gold/Fact_Flood_Alert"


# ── HDFS helpers ──────────────────────────────────────────────────
def read_file(client: InsecureClient, path: str) -> pd.DataFrame:
    """Read a Parquet or ORC file from HDFS into a DataFrame."""
    try:
        with client.read(path) as r:
            raw = r.read()
        if path.endswith(".orc"):
            import pyarrow.orc as orc_mod
            return orc_mod.read_table(io.BytesIO(raw)).to_pandas()
        return pd.read_parquet(io.BytesIO(raw))
    except Exception as exc:
        log.warning("Cannot read %s: %s", path, exc)
        return pd.DataFrame()


def read_parquet(client: InsecureClient, path: str) -> pd.DataFrame:
    return read_file(client, path)


def load_partition(client: InsecureClient, root: str, dt: str) -> pd.DataFrame:
    part_dir = f"{root}/dt={dt}"
    try:
        entries = client.list(part_dir, status=False)
    except Exception:
        log.warning("Partition not found: %s", part_dir)
        return pd.DataFrame()
    dfs = [
        read_file(client, f"{part_dir}/{e}")
        for e in entries
        if e.endswith(".parquet") or e.endswith(".orc")
    ]
    dfs = [d for d in dfs if not d.empty]
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


def load_latest_partition(client: InsecureClient, root: str) -> pd.DataFrame:
    """Load most recent partition from an HDFS root."""
    try:
        parts = sorted(client.list(root, status=False), reverse=True)
    except Exception:
        return pd.DataFrame()
    for part in parts:
        if not part.startswith("dt="):
            continue
        dt = part.replace("dt=", "")
        df = load_partition(client, root, dt)
        if not df.empty:
            return df
    return pd.DataFrame()


# ── Reservoir export ──────────────────────────────────────────────
def export_reservoirs(client: InsecureClient, dt: str) -> list:
    """Load Silver reservoir + features, merge, return as list of dicts."""
    res_df  = load_partition(client, SILVER_RESERVOIR_ROOT, dt)
    feat_df = load_partition(client, SILVER_FEATURES_ROOT,  dt)

    if res_df.empty:
        log.warning("No reservoir Silver data for dt=%s", dt)
        return []

    # Merge features if available
    if not feat_df.empty:
        feat_cols = ["reservoir_id", "source", "net_flow_mcm",
                     "storage_trend_7d", "storage_delta_1d",
                     "days_to_full", "reservoir_status", "filling_rate"]
        feat_cols = [c for c in feat_cols if c in feat_df.columns]
        df = res_df.merge(
            feat_df[feat_cols],
            on=["reservoir_id", "source"],
            how="left",
        )
    else:
        df = res_df.copy()
        for col in ["net_flow_mcm", "storage_trend_7d", "storage_delta_1d",
                    "days_to_full", "reservoir_status", "filling_rate"]:
            if col not in df.columns:
                df[col] = None

    # Compute percent_storage from volume/capacity when API returns 0
    if "capacity_mcm" in df.columns and "volume_mcm" in df.columns:
        zero_pct_mask = (
            (df["percent_storage"].isna() | (df["percent_storage"] == 0.0))
            & (df["capacity_mcm"] > 0)
        )
        if zero_pct_mask.any():
            log.info("Recomputing percent_storage from volume/capacity for %d rows",
                     int(zero_pct_mask.sum()))
            df.loc[zero_pct_mask, "percent_storage"] = (
                df.loc[zero_pct_mask, "volume_mcm"]
                / df.loc[zero_pct_mask, "capacity_mcm"]
                * 100.0
            ).clip(0, 100).round(1)

    def _status(pct):
        try:
            p = float(pct)
            if p >= 90: return "CRITICAL"
            if p >= 75: return "HIGH"
            if p >= 40: return "NORMAL"
            return "LOW"
        except Exception:
            return "NORMAL"

    df["reservoir_status"] = df["percent_storage"].apply(_status)

    df = df.sort_values("percent_storage", ascending=False)

    out_cols = [
        "reservoir_id", "reservoir_name", "source", "region",
        "record_date", "capacity_mcm", "volume_mcm", "percent_storage",
        "inflow_mcm", "outflow_mcm",
        "net_flow_mcm", "storage_trend_7d", "storage_delta_1d",
        "days_to_full", "reservoir_status", "filling_rate",
    ]
    out_cols = [c for c in out_cols if c in df.columns]
    df = df[out_cols].fillna("")

    rows = df.to_dict(orient="records")
    log.info("Reservoirs exported: %d rows", len(rows))
    return rows


# ── Alert export ──────────────────────────────────────────────────
def _gold_alert_to_dict(row: pd.Series) -> dict:
    """Normalise a Gold Fact_Flood_Alert row for JSON output."""
    def _f(col, default=0.0):
        try: return float(row.get(col, default) or default)
        except Exception: return default
    def _s(col, default=""):
        v = row.get(col, default)
        return str(v) if v is not None else default

    return {
        "prov_code":              int(_f("prov_code")),
        "prov_th":                _s("prov_th"),
        "alert_level":            _s("alert_level", "ปกติ"),
        "alert_score":            round(_f("alert_score"), 1),
        "reservoir_score":        round(_f("reservoir_score"), 1),
        "weather_score":          round(_f("weather_score"), 1),
        "historical_risk_score":  round(_f("historical_risk_score"), 1),
        "trigger_reservoirs":     _s("trigger_reservoirs"),
        "max_reservoir_pct":      round(_f("max_reservoir_pct"), 1),
        "rainfall_forecast_mm":   round(_f("rainfall_forecast_mm"), 1),
        "historical_risk_level":  _s("historical_risk_level"),
    }


def classify_alert_from_weather(row: pd.Series) -> dict:
    """Fallback: compute simple province alert from Silver weather only."""
    rain = float(row.get("rainfall_forecast_mm", 0) or 0)
    if rain >= 90: w_score = 35
    elif rain >= 35: w_score = 25
    elif rain >= 10: w_score = 15
    elif rain >= 1:  w_score = 5
    else:            w_score = 0

    score = w_score
    if score >= 75: level = "วิกฤต"
    elif score >= 50: level = "เตือนภัย"
    elif score >= 25: level = "เฝ้าระวัง"
    else: level = "ปกติ"

    return {
        "prov_code":             int(row.get("prov_code", 0) or 0),
        "prov_th":               str(row.get("prov_th", "") or ""),
        "alert_level":           level,
        "alert_score":           round(score, 1),
        "reservoir_score":       0.0,
        "weather_score":         float(w_score),
        "historical_risk_score": 0.0,
        "trigger_reservoirs":    "",
        "max_reservoir_pct":     0.0,
        "rainfall_forecast_mm":  round(rain, 1),
        "historical_risk_level": "",
    }


def export_alerts(client: InsecureClient, dt: str) -> list:
    """
    Priority:
      1. Gold Fact_Flood_Alert ORC (written by flood_alert_scoring.py) — full 77 provinces
      2. Silver weather Parquet (fallback) — weather-only scoring
    """
    # 1. Try Gold Fact_Flood_Alert (supports both .orc and .parquet)
    gold_df = load_partition(client, GOLD_ALERT_ROOT, dt)
    if gold_df.empty:
        gold_df = load_latest_partition(client, GOLD_ALERT_ROOT)

    if not gold_df.empty:
        log.info("Alerts from Gold Fact_Flood_Alert: %d rows", len(gold_df))
        rows = gold_df.apply(_gold_alert_to_dict, axis=1).tolist()
        rows = sorted(rows, key=lambda x: x["alert_score"], reverse=True)
        return rows

    # 2. Fallback: Silver weather
    log.warning("Gold Fact_Flood_Alert not found — falling back to Silver weather")
    weather_df = load_partition(client, SILVER_WEATHER_ROOT, dt)
    if weather_df.empty:
        weather_df = load_latest_partition(client, SILVER_WEATHER_ROOT)
    if weather_df.empty:
        log.warning("No weather data available for alerts.")
        return []

    weather_df["rainfall_forecast_mm"] = pd.to_numeric(
        weather_df.get("rainfall_forecast_mm", 0), errors="coerce"
    ).fillna(0)
    rows = weather_df.apply(classify_alert_from_weather, axis=1).tolist()
    rows = sorted(rows, key=lambda x: x["alert_score"], reverse=True)
    log.info("Alerts (weather fallback): %d provinces", len(rows))
    return rows


# ── DQ latest + history ───────────────────────────────────────────
def export_dq_latest() -> dict:
    files = sorted(glob.glob(os.path.join(DQ_DIR, "dq_report_*.json")))
    if not files:
        return {}
    with open(files[-1], encoding="utf-8") as f:
        data = json.load(f)
    history = []
    for fp in files:
        with open(fp, encoding="utf-8") as f:
            d = json.load(f)
        passed = sum(1 for r in d.get("results", []) if r.get("status") == "PASS")
        total  = len(d.get("results", []))
        ts     = os.path.basename(fp).replace("dq_report_", "").replace(".json", "")
        history.append({
            "file":      os.path.basename(fp),
            "run_date":  d.get("run_date", ""),
            "timestamp": ts,
            "overall":   d.get("overall", "UNKNOWN"),
            "passed":    passed,
            "total":     total,
        })
    data["history"] = history
    log.info("DQ latest: %s (overall=%s, %d runs)", os.path.basename(files[-1]),
             data.get("overall"), len(history))
    return data


# ── Pipeline meta ─────────────────────────────────────────────────
def export_pipeline_meta(dt: str, reservoirs: list, alerts: list) -> dict:
    meta = {
        "export_date": dt,
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "reservoir_count": len(reservoirs),
        "alert_province_count": len(alerts),
    }
    if os.path.exists(WATERMARK_FILE):
        with open(WATERMARK_FILE, encoding="utf-8") as f:
            wm = json.load(f)
        meta["last_fetched_at"]       = wm.get("last_fetched_at", "")
        meta["watermark_updated_at"]  = wm.get("updated_at", "")

    files = sorted(glob.glob(os.path.join(DQ_DIR, "dq_report_*.json")))
    if files:
        with open(files[-1], encoding="utf-8") as f:
            dq = json.load(f)
        results = dq.get("results", [])
        meta["dq_overall"]     = dq.get("overall", "UNKNOWN")
        meta["dq_run_date"]    = dq.get("run_date", "")
        meta["dq_total_rows"]  = results[0].get("total_rows", 0) if results else 0
        meta["dq_rules_pass"]  = sum(1 for r in results if r.get("status") == "PASS")
        meta["dq_rules_total"] = len(results)
        meta["dq_file"]        = os.path.basename(files[-1])

    if alerts:
        level_counts: dict = {}
        for a in alerts:
            lvl = a.get("alert_level", "ปกติ")
            level_counts[lvl] = level_counts.get(lvl, 0) + 1
        meta["alert_summary"] = level_counts

    return meta


# ── NaN / Inf sanitizer ───────────────────────────────────────────
def _sanitize(obj):
    """
    Recursively replace float NaN and Inf values with None so that
    json.dump never writes invalid JSON tokens (NaN, Infinity).
    Also converts numpy scalar types to plain Python types.
    """
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj
    if isinstance(obj, dict):
        return {k: _sanitize(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_sanitize(v) for v in obj]
    try:
        import numpy as np
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            v = float(obj)
            return None if (math.isnan(v) or math.isinf(v)) else v
        if isinstance(obj, np.bool_):
            return bool(obj)
    except ImportError:
        pass
    return obj


# ── Main ──────────────────────────────────────────────────────────
def main():
    target_date = (
        sys.argv[1] if len(sys.argv) > 1
        else datetime.now(timezone.utc).strftime("%Y-%m-%d")
    )
    log.info("export_gold_to_json -- target_date=%s", target_date)
    os.makedirs(EXPORT_DIR, exist_ok=True)

    client = InsecureClient(HDFS_URL, user=HDFS_USER)

    reservoirs = export_reservoirs(client, target_date)
    alerts     = export_alerts(client, target_date)

    def _atomic_write(path: str, data: object) -> None:
        """Write JSON atomically: sanitize NaN/Inf, dump to .tmp then os.replace()."""
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(_sanitize(data), f, ensure_ascii=False, indent=2, default=str)
        os.replace(tmp, path)
        log.info("Written: %s", path)

    # reservoirs.json
    _atomic_write(
        os.path.join(EXPORT_DIR, "reservoirs.json"),
        {"date": target_date, "count": len(reservoirs), "data": reservoirs},
    )
    log.info("Reservoirs: %d rows", len(reservoirs))

    # alerts.json
    _atomic_write(
        os.path.join(EXPORT_DIR, "alerts.json"),
        {"date": target_date, "count": len(alerts), "data": alerts},
    )
    log.info("Alerts: %d provinces", len(alerts))

    # dq_latest.json
    dq_data = export_dq_latest()
    _atomic_write(os.path.join(EXPORT_DIR, "dq_latest.json"), dq_data)

    # pipeline_meta.json
    meta = export_pipeline_meta(target_date, reservoirs, alerts)
    _atomic_write(os.path.join(EXPORT_DIR, "pipeline_meta.json"), meta)

    log.info("Export complete -- %s", EXPORT_DIR)
    if reservoirs:
        log.info("Top 3: %s", [r.get("reservoir_name","?") for r in reservoirs[:3]])

    hi = [a for a in alerts if a.get("alert_level") in ("เตือนภัย", "วิกฤต")]
    if hi:
        log.warning("High/Crisis provinces: %s", [a.get("prov_th","?") for a in hi])


if __name__ == "__main__":
    main()
