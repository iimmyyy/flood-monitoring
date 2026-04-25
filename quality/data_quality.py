"""
data_quality.py — 3 data quality rules on Silver Parquet data

Rules:
  1. NULL CHECK   — critical fields must not be null (reservoir_id, record_date, percent_storage)
  2. DUPLICATE CHECK — (reservoir_id, record_date) composite key must be unique
  3. RANGE CHECK  — percent_storage must be in [0, 100]

Exits with code 1 if any critical rule fails (so Airflow marks task as failed).
Exits with code 0 on success or non-critical warnings.
"""

import io
import json
import logging
import os
import sys
from datetime import datetime, timezone

import pandas as pd
import pyarrow.parquet as pq
from hdfs import InsecureClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [data_quality] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
HDFS_URL = os.getenv("HDFS_NAMENODE_URL", "http://namenode:9870")
HDFS_USER = os.getenv("HDFS_USER", "root")
DQ_REPORT_DIR = os.getenv("DQ_REPORT_DIR", "/opt/airflow/data/dq_reports")


# ── HDFS helper ───────────────────────────────────────────────────────────────
def read_silver_parquet(client: InsecureClient, hdfs_path: str) -> pd.DataFrame:
    try:
        with client.read(hdfs_path) as reader:
            raw_bytes = reader.read()
        table = pq.read_table(io.BytesIO(raw_bytes))
        return table.to_pandas()
    except Exception as exc:
        log.error("Cannot read Parquet from HDFS %s: %s", hdfs_path, exc)
        return pd.DataFrame()


def list_silver_files(client: InsecureClient, path: str) -> list[str]:
    try:
        return [f"{path}/{e}" for e in client.list(path, status=False) if e.endswith(".parquet")]
    except Exception:
        return []


def load_silver_reservoir(client: InsecureClient, date_str: str) -> pd.DataFrame:
    hdfs_path = f"/silver/reservoir/dt={date_str}/reservoir.parquet"
    df = read_silver_parquet(client, hdfs_path)
    if df.empty:
        log.warning("Silver reservoir empty for %s — trying all partitions.", date_str)
        # /silver/reservoir contains partition dirs (dt=YYYY-MM-DD), not files directly.
        # List each partition dir and collect .parquet files inside.
        all_parquets: list[str] = []
        try:
            partition_dirs = client.list("/silver/reservoir", status=False)
            for pdir in partition_dirs:
                all_parquets.extend(
                    list_silver_files(client, f"/silver/reservoir/{pdir}")
                )
        except Exception as exc:
            log.warning("Could not list Silver reservoir partitions: %s", exc)

        dfs = []
        for f in all_parquets:
            tmp = read_silver_parquet(client, f)
            if not tmp.empty:
                dfs.append(tmp)
        if dfs:
            df = pd.concat(dfs, ignore_index=True)
    return df


# ── Result tracking ───────────────────────────────────────────────────────────
class QualityReport:
    def __init__(self, run_date: str):
        self.run_date = run_date
        self.results: list[dict] = []
        self.has_critical_failure = False

    def record(self, rule: str, table: str, passed: bool, critical: bool,
               total: int, failed_count: int, details: str = "") -> None:
        status = "PASS" if passed else ("FAIL" if critical else "WARN")
        entry = {
            "rule": rule,
            "table": table,
            "status": status,
            "critical": critical,
            "total_rows": total,
            "failed_rows": failed_count,
            "pass_rate_pct": round(100 * (1 - failed_count / max(total, 1)), 2),
            "details": details,
            "checked_at": datetime.now(timezone.utc).isoformat(),
        }
        self.results.append(entry)
        if not passed and critical:
            self.has_critical_failure = True
        log.log(
            logging.ERROR if (not passed and critical) else
            logging.WARNING if (not passed) else logging.INFO,
            "[%s] %s on %s — %d/%d rows failed. %s",
            status, rule, table, failed_count, total, details,
        )

    def save(self) -> None:
        os.makedirs(DQ_REPORT_DIR, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        path = os.path.join(DQ_REPORT_DIR, f"dq_report_{ts}.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(
                {"run_date": self.run_date, "results": self.results,
                 "overall": "PASS" if not self.has_critical_failure else "FAIL"},
                f, ensure_ascii=False, indent=2,
            )
        log.info("DQ report saved: %s", path)

    def summary(self) -> None:
        passed = sum(1 for r in self.results if r["status"] == "PASS")
        warned = sum(1 for r in self.results if r["status"] == "WARN")
        failed = sum(1 for r in self.results if r["status"] == "FAIL")
        log.info("=" * 60)
        log.info("DATA QUALITY SUMMARY — %s", self.run_date)
        log.info("  PASS: %d  WARN: %d  FAIL: %d  (Total checks: %d)",
                 passed, warned, failed, len(self.results))
        log.info("  Overall: %s", "FAIL" if self.has_critical_failure else "PASS")
        log.info("=" * 60)


# ── Rule 1: NULL CHECK ────────────────────────────────────────────────────────
def check_nulls(df: pd.DataFrame, report: QualityReport, date_str: str) -> None:
    """Critical fields must not be null."""
    critical_fields = ["reservoir_id", "record_date", "percent_storage"]
    table_name = f"silver_reservoir (dt={date_str})"

    if df.empty:
        report.record("NULL_CHECK", table_name, passed=False, critical=True,
                      total=0, failed_count=0, details="DataFrame is empty — no data to check.")
        return

    for field in critical_fields:
        if field not in df.columns:
            report.record(f"NULL_CHECK:{field}", table_name, passed=False, critical=True,
                          total=len(df), failed_count=len(df),
                          details=f"Column '{field}' does not exist in Silver table.")
            continue

        null_mask = df[field].isnull() | (df[field].astype(str).str.strip() == "")
        null_count = int(null_mask.sum())
        passed = null_count == 0

        sample = ""
        if not passed:
            sample_ids = df.loc[null_mask, "reservoir_id"].head(3).tolist() if "reservoir_id" in df.columns else []
            sample = f"Sample reservoir_ids with null '{field}': {sample_ids}"

        report.record(
            rule=f"NULL_CHECK:{field}",
            table=table_name,
            passed=passed,
            critical=True,
            total=len(df),
            failed_count=null_count,
            details=sample,
        )


# ── Rule 2: DUPLICATE CHECK ───────────────────────────────────────────────────
def check_duplicates(df: pd.DataFrame, report: QualityReport, date_str: str) -> None:
    """(reservoir_id, record_date) composite key must be unique within each source."""
    table_name = f"silver_reservoir (dt={date_str})"
    composite_key = ["reservoir_id", "record_date", "source"]

    if df.empty:
        report.record("DUPLICATE_CHECK", table_name, passed=False, critical=True,
                      total=0, failed_count=0, details="DataFrame is empty.")
        return

    # Check which key columns exist
    key_cols = [c for c in composite_key if c in df.columns]
    if not key_cols:
        report.record("DUPLICATE_CHECK", table_name, passed=False, critical=True,
                      total=len(df), failed_count=0, details="Key columns missing from DataFrame.")
        return

    dup_mask = df.duplicated(subset=key_cols, keep=False)
    dup_count = int(dup_mask.sum())
    passed = dup_count == 0

    sample = ""
    if not passed:
        sample_dupes = (
            df.loc[dup_mask, key_cols].drop_duplicates().head(3).to_dict("records")
        )
        sample = f"Duplicate key examples: {sample_dupes}"

    report.record(
        rule="DUPLICATE_CHECK:(reservoir_id, record_date, source)",
        table=table_name,
        passed=passed,
        critical=True,
        total=len(df),
        failed_count=dup_count,
        details=sample,
    )


# ── Rule 3: RANGE CHECK ───────────────────────────────────────────────────────
def check_ranges(df: pd.DataFrame, report: QualityReport, date_str: str) -> None:
    """Numeric fields must fall within valid physical ranges."""
    table_name = f"silver_reservoir (dt={date_str})"

    range_rules = [
        ("percent_storage",  0.0,    100.0,  True,  "Physical capacity percentage"),
        # Thailand's largest dams exceed 9,999 MCM:
        #   Bhumibol Dam  ≈ 13,462 MCM  |  Sirikit Dam ≈ 9,510 MCM
        # Upper bound set to 99,999 MCM to accommodate all Thai dams.
        ("volume_mcm",       0.0,  99999.0,  True,  "Water volume cannot be negative"),
        ("inflow_mcm",      -10.0,  9999.0, False,  "Minor negatives acceptable (measurement error)"),
        ("outflow_mcm",     -10.0,  9999.0, False,  "Minor negatives acceptable"),
    ]

    if df.empty:
        report.record("RANGE_CHECK", table_name, passed=False, critical=True,
                      total=0, failed_count=0, details="DataFrame is empty.")
        return

    for col, low, high, is_critical, note in range_rules:
        if col not in df.columns:
            log.warning("Column '%s' not found — skipping range check.", col)
            continue

        numeric_col = pd.to_numeric(df[col], errors="coerce")
        out_of_range = numeric_col.notna() & ((numeric_col < low) | (numeric_col > high))
        fail_count = int(out_of_range.sum())
        passed = fail_count == 0

        sample = ""
        if not passed:
            bad_vals = numeric_col[out_of_range].head(5).tolist()
            sample = f"{note}. Out-of-range values: {bad_vals} (expected [{low}, {high}])"

        report.record(
            rule=f"RANGE_CHECK:{col}[{low},{high}]",
            table=table_name,
            passed=passed,
            critical=is_critical,
            total=len(df),
            failed_count=fail_count,
            details=sample,
        )


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    date_str = sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime("%Y-%m-%d")
    log.info("Starting data quality checks for date: %s", date_str)

    report = QualityReport(run_date=date_str)

    # Connect to HDFS and load data
    hdfs_client = InsecureClient(HDFS_URL, user=HDFS_USER)
    df_reservoir = load_silver_reservoir(hdfs_client, date_str)

    if df_reservoir.empty:
        log.error("No Silver reservoir data found for %s. Is the pipeline running?", date_str)
        report.record("DATA_AVAILABILITY", f"silver_reservoir (dt={date_str})",
                      passed=False, critical=True, total=0, failed_count=0,
                      details="No data in Silver layer — pipeline may not have run yet.")
        report.summary()
        report.save()
        sys.exit(1)

    log.info("Loaded %d rows from Silver reservoir for %s.", len(df_reservoir), date_str)

    # Run all 3 rules
    check_nulls(df_reservoir, report, date_str)
    check_duplicates(df_reservoir, report, date_str)
    check_ranges(df_reservoir, report, date_str)

    report.summary()
    report.save()

    if report.has_critical_failure:
        log.error("Critical data quality failures detected — pipeline halted.")
        sys.exit(1)

    log.info("All critical data quality checks passed.")


if __name__ == "__main__":
    main()
