"""
flood_alert_scoring.py — Combine reservoir features + weather + historical flood risk
into a province-level daily flood alert. Writes results to Gold layer (HDFS ORC via Hive).

Alert scoring (total 0–100):
  Reservoir component  (0–40): based on upstream dam status & filling rate
  Weather component    (0–35): based on 24h rainfall forecast
  Historical component (0–25): based on historical flood risk score for this month

Alert levels:
  วิกฤต      (CRISIS)  — score ≥ 75
  เตือนภัย   (ALERT)   — score ≥ 50
  เฝ้าระวัง  (WATCH)   — score ≥ 25
  ปกติ       (NORMAL)  — score < 25

Run:
    python flood_alert_scoring.py [YYYY-MM-DD]   (default: today)
"""

import io
import json
import logging
import os
import subprocess
import sys
import tempfile
from datetime import datetime, timezone

import pandas as pd
import pyarrow as pa
import pyarrow.orc as orc
import requests
from hdfs import InsecureClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [flood_alert] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
HDFS_URL       = os.getenv("HDFS_NAMENODE_URL", "http://namenode:9870")
HDFS_USER      = os.getenv("HDFS_USER", "root")
HIVE_CONTAINER = os.getenv("HIVE_CONTAINER", "hive-server")

SILVER_FEATURES_ROOT  = "/silver/reservoir_features"
SILVER_WEATHER_ROOT   = "/silver/weather"
SILVER_FLOOD_RISK_ROOT = "/silver/flood_risk"
GOLD_ALERT_ROOT       = "/gold/Fact_Flood_Alert"

# Static mapping — local path (also available in HDFS if ingest_static uploaded it)
_SCRIPT_DIR   = os.path.dirname(os.path.abspath(__file__))
DOWNSTREAM_MAP_LOCAL = os.path.join(
    _SCRIPT_DIR, "..", "data", "reservoir_downstream_map.csv"
)
DOWNSTREAM_MAP_HDFS  = "/bronze/static/reservoir_downstream_map.csv"


# ── WebHDFS helpers ───────────────────────────────────────────────────────────
def _webhdfs_makedirs(hdfs_path: str) -> None:
    url = f"{HDFS_URL}/webhdfs/v1{hdfs_path}?op=MKDIRS&user.name={HDFS_USER}&permission=755"
    requests.put(url, timeout=15)


def _webhdfs_write(hdfs_path: str, content: bytes, overwrite: bool = True) -> None:
    ov = "true" if overwrite else "false"
    nn_url = (
        f"{HDFS_URL}/webhdfs/v1{hdfs_path}"
        f"?op=CREATE&user.name={HDFS_USER}&overwrite={ov}"
    )
    r1 = requests.put(nn_url, allow_redirects=False, timeout=30)
    if r1.status_code != 307:
        raise RuntimeError(
            f"WebHDFS step-1 failed for {hdfs_path}: "
            f"HTTP {r1.status_code} — {r1.text[:300]}"
        )
    dn_url = r1.headers.get("Location", "")
    r2 = requests.put(
        dn_url,
        data=content,
        headers={"Content-Type": "application/octet-stream"},
        timeout=120,
    )
    if r2.status_code not in (200, 201):
        raise RuntimeError(
            f"WebHDFS step-2 failed for {hdfs_path}: "
            f"HTTP {r2.status_code} — {r2.text[:300]}"
        )
    log.info("HDFS write OK: %s (%d bytes)", hdfs_path, len(content))


def _read_parquet_from_hdfs(client: InsecureClient, hdfs_path: str) -> pd.DataFrame:
    try:
        with client.read(hdfs_path) as reader:
            raw = reader.read()
        return pd.read_parquet(io.BytesIO(raw))
    except Exception as exc:
        log.warning("Cannot read parquet %s: %s", hdfs_path, exc)
        return pd.DataFrame()


def _read_csv_from_hdfs(client: InsecureClient, hdfs_path: str) -> pd.DataFrame:
    try:
        with client.read(hdfs_path) as reader:
            raw = reader.read()
        return pd.read_csv(io.BytesIO(raw), encoding="utf-8-sig")
    except Exception as exc:
        log.warning("Cannot read CSV from HDFS %s: %s", hdfs_path, exc)
        return pd.DataFrame()


def list_partition_parquets(client: InsecureClient, root: str, date_str: str) -> list[str]:
    part_dir = f"{root}/dt={date_str}"
    try:
        entries = client.list(part_dir, status=False)
        return [f"{part_dir}/{e}" for e in entries if e.endswith(".parquet")]
    except Exception:
        return []


def load_parquet_partition(
    client: InsecureClient, root: str, date_str: str
) -> pd.DataFrame:
    paths = list_partition_parquets(client, root, date_str)
    if not paths:
        log.warning("No parquet files in %s/dt=%s", root, date_str)
        return pd.DataFrame()
    dfs = [_read_parquet_from_hdfs(client, p) for p in paths]
    dfs = [d for d in dfs if not d.empty]
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


# ── Load downstream mapping ───────────────────────────────────────────────────
def load_downstream_map(client: InsecureClient) -> pd.DataFrame:
    """
    Load reservoir_downstream_map.csv from local path (fallback: HDFS).
    Returns DataFrame with columns: reservoir_name_en, downstream_prov_codes (list).
    """
    df = pd.DataFrame()
    local_path = os.path.normpath(DOWNSTREAM_MAP_LOCAL)
    if os.path.exists(local_path):
        log.info("Loading downstream map from local: %s", local_path)
        df = pd.read_csv(local_path, encoding="utf-8-sig")
    else:
        log.info("Local downstream map not found — trying HDFS: %s", DOWNSTREAM_MAP_HDFS)
        df = _read_csv_from_hdfs(client, DOWNSTREAM_MAP_HDFS)

    if df.empty:
        log.error("Downstream map unavailable — cannot compute reservoir scores.")
        return df

    # Parse downstream_prov_codes from "63,62,60,..." → list of ints
    def _parse_codes(val) -> list:
        if pd.isna(val) or str(val).strip() == "":
            return []
        return [int(c.strip()) for c in str(val).split(",") if c.strip().isdigit()]

    df["downstream_prov_list"] = df["downstream_prov_codes"].apply(_parse_codes)
    log.info("Loaded downstream map: %d reservoirs.", len(df))
    return df


# ── Scoring functions ─────────────────────────────────────────────────────────
# Reservoir status weights → base score contribution
_STATUS_SCORE = {
    "CRITICAL": 30,
    "HIGH":     18,
    "NORMAL":    5,
    "LOW":       0,
}

# Filling rate bonus
_FILLING_BONUS = {
    "FAST":     10,
    "MODERATE":  5,
    "STABLE":    0,
    "DRAINING": -5,   # draining = lower risk
}


def reservoir_component(reservoir_rows: pd.DataFrame) -> tuple:
    """
    Given all reservoir rows upstream of a province,
    return (score 0-40, trigger_names, trigger_statuses, max_pct).
    """
    if reservoir_rows.empty:
        return 0.0, "", "", 0.0

    max_pct = float(reservoir_rows["percent_storage"].max())

    # Score from the single worst upstream reservoir
    def _row_score(row) -> float:
        base  = _STATUS_SCORE.get(row.get("reservoir_status", "NORMAL"), 5)
        bonus = _FILLING_BONUS.get(row.get("filling_rate", "STABLE"), 0)
        return float(min(40, max(0, base + bonus)))

    reservoir_rows = reservoir_rows.copy()
    reservoir_rows["_rscore"] = reservoir_rows.apply(_row_score, axis=1)

    # Sort by score desc, take up to 3 for reporting
    top = reservoir_rows.sort_values("_rscore", ascending=False).head(3)
    score = float(top["_rscore"].iloc[0]) if not top.empty else 0.0

    triggers   = ",".join(top["reservoir_name"].astype(str).tolist())
    statuses   = ",".join(top["reservoir_status"].astype(str).tolist())

    return score, triggers, statuses, max_pct


def weather_component(rain_mm: float) -> float:
    """Map rainfall forecast (mm/24h) to a score 0–35."""
    if rain_mm >= 90.0:   # ฝนหนักมากพิเศษ
        return 35.0
    if rain_mm >= 35.0:   # ฝนหนัก
        return 25.0
    if rain_mm >= 10.0:   # ฝนปานกลาง
        return 15.0
    if rain_mm >= 1.0:    # ฝนเล็กน้อย
        return 5.0
    return 0.0


def historical_component(risk_score: float) -> float:
    """Map historical risk_score (1–4) to a score 0–25."""
    mapping = {4: 25.0, 3: 18.0, 2: 10.0, 1: 5.0}
    return mapping.get(int(risk_score) if not pd.isna(risk_score) else 0, 0.0)


def alert_level(total_score: float) -> str:
    if total_score >= 75:
        return "วิกฤต"
    if total_score >= 50:
        return "เตือนภัย"
    if total_score >= 25:
        return "เฝ้าระวัง"
    return "ปกติ"


# ── Main scoring logic ────────────────────────────────────────────────────────
def compute_alerts(
    features_df: pd.DataFrame,
    weather_df: pd.DataFrame,
    flood_risk_df: pd.DataFrame,
    downstream_map: pd.DataFrame,
    target_date: str,
) -> pd.DataFrame:
    """
    Compute province-level flood alerts by combining 3 data sources.
    """
    target_month = int(target_date.split("-")[1])

    # ── Build province list from weather data ─────────────────────────────────
    if weather_df.empty:
        log.error("Weather data missing — cannot compute alerts.")
        return pd.DataFrame()

    weather_df["prov_code"] = pd.to_numeric(weather_df["prov_code"], errors="coerce")
    weather_df["rainfall_forecast_mm"] = pd.to_numeric(
        weather_df["rainfall_forecast_mm"], errors="coerce"
    ).fillna(0.0)

    provinces = weather_df[["prov_code", "prov_th"]].drop_duplicates()

    # ── Historical risk: aggregate to province, filter to current month ───────
    hist_prov = pd.DataFrame()
    if not flood_risk_df.empty and "prov_code" in flood_risk_df.columns:
        flood_risk_df["prov_code"]   = pd.to_numeric(flood_risk_df["prov_code"],   errors="coerce")
        flood_risk_df["risk_score"]  = pd.to_numeric(flood_risk_df["risk_score"],  errors="coerce").fillna(1.0)
        flood_risk_df["month"]       = pd.to_numeric(flood_risk_df.get("month", flood_risk_df.get("risk_month", None)), errors="coerce")

        month_risk = flood_risk_df[flood_risk_df["month"] == target_month]
        if not month_risk.empty:
            hist_prov = (
                month_risk.groupby("prov_code")
                .agg(
                    max_risk_score=("risk_score", "max"),
                    risk_level=("risk_level", lambda x: x.mode().iloc[0] if len(x) > 0 else "เสี่ยงต่ำ"),
                    subdistrict_count=("geocode", "nunique") if "geocode" in month_risk.columns else ("risk_score", "count"),
                )
                .reset_index()
            )
        log.info(
            "Historical risk: %d provinces for month %d.",
            len(hist_prov), target_month,
        )

    # ── Build province → upstream reservoirs mapping ──────────────────────────
    if features_df.empty:
        log.warning("Reservoir features missing — reservoir score will be 0.")

    def _get_upstream_reservoirs(prov_code: int) -> pd.DataFrame:
        if downstream_map.empty or features_df.empty:
            return pd.DataFrame()
        # Find all map rows whose downstream provinces include this prov_code
        mask = downstream_map["downstream_prov_list"].apply(
            lambda lst: int(prov_code) in lst
        )
        sub = downstream_map[mask]
        if sub.empty:
            return pd.DataFrame()

        # Build name lists for matching — use BOTH Thai (primary) and English (fallback).
        # Silver reservoir_name comes from the RID API in Thai, so Thai names match first.
        upstream_names_th = (
            sub["reservoir_name_th"].dropna().tolist()
            if "reservoir_name_th" in sub.columns else []
        )
        upstream_names_en = (
            sub["reservoir_name_en"].dropna().tolist()
            if "reservoir_name_en" in sub.columns else []
        )

        def _name_matches(silver_name: str) -> bool:
            n = str(silver_name).strip()
            # Thai name: check if map entry is a substring of Silver name (or vice-versa)
            for th in upstream_names_th:
                th = str(th).strip()
                if th and (th in n or n in th):
                    return True
            # English name: case-insensitive substring check (fallback for English sources)
            n_lower = n.lower()
            for en in upstream_names_en:
                en_clean = str(en).strip().lower()
                if en_clean and (en_clean in n_lower or n_lower in en_clean):
                    return True
            return False

        matched = features_df[features_df["reservoir_name"].apply(_name_matches)]
        return matched

    # ── Score each province ───────────────────────────────────────────────────
    rows = []
    loaded_at = datetime.now(timezone.utc).isoformat()

    for _, prov_row in provinces.iterrows():
        pcode    = int(prov_row["prov_code"])
        prov_th  = str(prov_row["prov_th"])

        # Weather
        w_row      = weather_df[weather_df["prov_code"] == pcode]
        rain_mm    = float(w_row["rainfall_forecast_mm"].iloc[0]) if not w_row.empty else 0.0
        w_score    = weather_component(rain_mm)

        # Reservoir
        upstream   = _get_upstream_reservoirs(pcode)
        r_score, triggers, trigger_statuses, max_pct = reservoir_component(upstream)

        # Historical
        h_row      = hist_prov[hist_prov["prov_code"] == pcode] if not hist_prov.empty else pd.DataFrame()
        if not h_row.empty:
            h_score         = historical_component(float(h_row["max_risk_score"].iloc[0]))
            hist_risk_level = str(h_row["risk_level"].iloc[0])
            subdist_count   = int(h_row.get("subdistrict_count", h_row.iloc[:, -1]).iloc[0])
        else:
            h_score         = 5.0   # mild default — province exists but no data
            hist_risk_level = "เสี่ยงต่ำ"
            subdist_count   = 0

        total_score = r_score + w_score + h_score
        level       = alert_level(total_score)

        rows.append({
            "alert_date":                  target_date,
            "prov_code":                   pcode,
            "prov_th":                     prov_th,
            "alert_level":                 level,
            "alert_score":                 round(total_score, 2),
            "reservoir_score":             round(r_score, 2),
            "weather_score":               round(w_score, 2),
            "historical_risk_score":       round(h_score, 2),
            "trigger_reservoirs":          triggers,
            "trigger_reservoir_statuses":  trigger_statuses,
            "max_reservoir_pct":           round(max_pct, 2),
            "rainfall_forecast_mm":        round(rain_mm, 2),
            "historical_risk_level":       hist_risk_level,
            "affected_subdistricts_count": subdist_count,
            "loaded_at":                   loaded_at,
        })

    out = pd.DataFrame(rows)

    # Add surrogate key
    out.insert(0, "alert_key", range(1, len(out) + 1))

    # Placeholder FK columns (will be joined by Hive if needed)
    out["date_key"]      = int(target_date.replace("-", ""))
    out["location_key"]  = -1   # resolved by Hive join on prov_code
    out["weather_key"]   = -1   # resolved by Hive join on prov_code

    log.info(
        "Computed alerts for %d provinces. Distribution: %s",
        len(out),
        out["alert_level"].value_counts().to_dict(),
    )
    return out


# ── Write ORC to HDFS + register with Hive ───────────────────────────────────
def write_gold_orc(df: pd.DataFrame, target_date: str) -> None:
    """Write alert DataFrame as ORC to HDFS Gold layer."""
    out_path = f"{GOLD_ALERT_ROOT}/dt={target_date}/flood_alerts.orc"

    table = pa.Table.from_pandas(df, preserve_index=False)
    buf   = io.BytesIO()
    orc.write_table(table, buf)
    orc_bytes = buf.getvalue()

    parent = "/".join(out_path.split("/")[:-1])
    _webhdfs_makedirs(parent)
    _webhdfs_write(out_path, orc_bytes, overwrite=True)
    log.info(
        "Wrote %d alert rows → HDFS %s (ORC, %d bytes)",
        len(df), out_path, len(orc_bytes),
    )


def register_hive_partition(target_date: str) -> None:
    """Run MSCK REPAIR on Fact_Flood_Alert so Hive picks up the new partition."""
    hql = (
        "USE flood_monitoring; "
        "MSCK REPAIR TABLE Fact_Flood_Alert;"
    )
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".hql", delete=False, encoding="utf-8"
    ) as f:
        f.write(hql)
        tmp_hql = f.name

    hive_hql = f"/tmp/alert_repair_{target_date}.hql"

    try:
        subprocess.run(
            ["docker", "cp", tmp_hql, f"{HIVE_CONTAINER}:{hive_hql}"],
            check=True, capture_output=True,
        )
        result = subprocess.run(
            ["docker", "exec", HIVE_CONTAINER, "hive", "-f", hive_hql],
            check=True, capture_output=True, text=True,
        )
        log.info("Hive MSCK REPAIR done: %s", result.stdout[-200:] if result.stdout else "OK")
    except subprocess.CalledProcessError as exc:
        log.warning(
            "Hive MSCK REPAIR warning (non-fatal): %s",
            exc.stderr[-300:] if exc.stderr else str(exc),
        )
    finally:
        os.unlink(tmp_hql)


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    target_date = (
        sys.argv[1]
        if len(sys.argv) > 1
        else datetime.now(timezone.utc).strftime("%Y-%m-%d")
    )
    log.info("flood_alert_scoring — target_date=%s", target_date)

    client = InsecureClient(HDFS_URL, user=HDFS_USER)

    # 1. Load Silver reservoir features
    features_df = load_parquet_partition(client, SILVER_FEATURES_ROOT, target_date)
    log.info("Reservoir features: %d rows", len(features_df))

    # 2. Load Silver weather
    weather_df = load_parquet_partition(client, SILVER_WEATHER_ROOT, target_date)
    log.info("Weather data: %d rows", len(weather_df))

    # 3. Load Silver flood risk (latest partition — static data, load most recent)
    flood_risk_df = pd.DataFrame()
    try:
        partitions = client.list(SILVER_FLOOD_RISK_ROOT, status=False)
        partitions = sorted(partitions, reverse=True)  # most recent first
        for part in partitions:
            if not part.startswith("dt="):
                continue
            part_date = part.replace("dt=", "")
            flood_risk_df = load_parquet_partition(client, SILVER_FLOOD_RISK_ROOT, part_date)
            if not flood_risk_df.empty:
                log.info("Flood risk data from partition %s: %d rows", part, len(flood_risk_df))
                break
    except Exception as exc:
        log.warning("Could not load flood risk data: %s", exc)

    # 4. Load downstream mapping
    downstream_map = load_downstream_map(client)

    # 5. Compute alerts
    alert_df = compute_alerts(
        features_df   = features_df,
        weather_df    = weather_df,
        flood_risk_df = flood_risk_df,
        downstream_map= downstream_map,
        target_date   = target_date,
    )

    if alert_df.empty:
        log.error("No alerts computed — check upstream data availability.")
        sys.exit(1)

    # 6. Write Gold ORC
    write_gold_orc(alert_df, target_date)

    # 7. Register with Hive
    register_hive_partition(target_date)

    # Print summary
    summary = alert_df.groupby("alert_level")["prov_code"].count().to_dict()
    log.info(
        "Alert summary for %s: ปกติ=%d, เฝ้าระวัง=%d, เตือนภัย=%d, วิกฤต=%d",
        target_date,
        summary.get("ปกติ", 0),
        summary.get("เฝ้าระวัง", 0),
        summary.get("เตือนภัย", 0),
        summary.get("วิกฤต", 0),
    )

    high_alert = alert_df[alert_df["alert_level"].isin(["เตือนภัย", "วิกฤต"])]
    if not high_alert.empty:
        log.warning(
            "⚠  HIGH/CRISIS provinces (%d): %s",
            len(high_alert),
            ", ".join(high_alert["prov_th"].tolist()),
        )


if __name__ == "__main__":
    main()
