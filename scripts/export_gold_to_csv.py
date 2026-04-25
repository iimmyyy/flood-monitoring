"""
export_gold_to_csv.py — Convert Gold JSON exports → CSV files for Power BI

Reads from data/exports/alerts.json and reservoirs.json,
adds province lat/lon centroids for Map visual,
writes to data/exports/csv/

Usage:
    python export_gold_to_csv.py
    (run from any directory — uses paths relative to this script)
"""

import json
import csv
import os
from pathlib import Path

SCRIPT_DIR   = Path(__file__).parent
EXPORTS_DIR  = SCRIPT_DIR / ".." / "data" / "exports"
CSV_DIR      = EXPORTS_DIR / "csv"
CSV_DIR.mkdir(parents=True, exist_ok=True)

# ── Province centroids (lat, lon) for Power BI Map visual ─────────────────────
PROVINCE_COORDS = {
    "กรุงเทพมหานคร":       (13.7563, 100.5018),
    "กระบี่":              (8.0863,  98.9063),
    "กาญจนบุรี":           (14.0023, 99.5328),
    "กาฬสินธุ์":           (16.4314, 103.5060),
    "กำแพงเพชร":           (16.4827, 99.5228),
    "ขอนแก่น":             (16.4419, 102.8360),
    "จันทบุรี":            (12.6105, 102.1038),
    "ฉะเชิงเทรา":          (13.6905, 101.0778),
    "ชลบุรี":              (13.3611, 100.9847),
    "ชัยนาท":              (15.1851, 100.1251),
    "ชัยภูมิ":             (15.8068, 102.0317),
    "ชุมพร":               (10.4930, 99.1800),
    "เชียงราย":            (19.9105, 99.8406),
    "เชียงใหม่":           (18.7883, 98.9853),
    "ตรัง":                (7.5590,  99.6113),
    "ตราด":                (12.2427, 102.5175),
    "ตาก":                 (16.8839, 99.1258),
    "นครนายก":             (14.2069, 101.2130),
    "นครปฐม":              (13.8199, 100.0624),
    "นครพนม":              (17.3922, 104.7691),
    "นครราชสีมา":          (14.9799, 102.0978),
    "นครศรีธรรมราช":       (8.4322,  99.9630),
    "นครสวรรค์":           (15.7030, 100.1370),
    "นนทบุรี":             (13.8622, 100.5131),
    "นราธิวาส":            (6.4255,  101.8253),
    "น่าน":                (18.7756, 100.7730),
    "บึงกาฬ":              (18.3609, 103.6461),
    "บุรีรัมย์":           (14.9950, 103.1029),
    "ปทุมธานี":            (14.0208, 100.5250),
    "ประจวบคีรีขันธ์":     (11.8126, 99.7957),
    "ปราจีนบุรี":          (14.0519, 101.3687),
    "ปัตตานี":             (6.8694,  101.2502),
    "พระนครศรีอยุธยา":     (14.3692, 100.5877),
    "พะเยา":               (19.1665, 99.9018),
    "พังงา":               (8.4510,  98.5253),
    "พัทลุง":              (7.6170,  100.0740),
    "พิจิตร":              (16.4395, 100.3490),
    "พิษณุโลก":            (16.8211, 100.2659),
    "เพชรบุรี":            (13.1119, 99.9390),
    "เพชรบูรณ์":           (16.4189, 101.1591),
    "แพร่":                (18.1445, 100.1403),
    "ภูเก็ต":              (7.8804,  98.3923),
    "มหาสารคาม":           (16.1851, 103.3021),
    "มุกดาหาร":            (16.5436, 104.7235),
    "แม่ฮ่องสอน":          (19.3020, 97.9654),
    "ยโสธร":               (15.7922, 104.1455),
    "ยะลา":                (6.5407,  101.2801),
    "ร้อยเอ็ด":            (16.0538, 103.6520),
    "ระนอง":               (9.9528,  98.6084),
    "ระยอง":               (12.6814, 101.2816),
    "ราชบุรี":             (13.5283, 99.8134),
    "ลพบุรี":              (14.7995, 100.6534),
    "ลำปาง":               (18.2888, 99.4908),
    "ลำพูน":               (18.5744, 99.0087),
    "เลย":                 (17.4860, 101.7223),
    "ศรีสะเกษ":            (15.1186, 104.3220),
    "สกลนคร":              (17.1549, 104.1348),
    "สงขลา":               (7.1896,  100.5950),
    "สตูล":                (6.6238,  100.0673),
    "สมุทรปราการ":         (13.5991, 100.5998),
    "สมุทรสงคราม":         (13.4098, 100.0022),
    "สมุทรสาคร":           (13.5475, 100.2747),
    "สระแก้ว":             (13.8240, 102.0645),
    "สระบุรี":             (14.5289, 100.9100),
    "สิงห์บุรี":           (14.8936, 100.3967),
    "สุโขทัย":             (17.0070, 99.8266),
    "สุพรรณบุรี":          (14.4745, 100.1177),
    "สุราษฎร์ธานี":        (9.1382,  99.3217),
    "สุรินทร์":            (14.8820, 103.4937),
    "หนองคาย":             (17.8783, 102.7420),
    "หนองบัวลำภู":         (17.2218, 102.4260),
    "อ่างทอง":             (14.5896, 100.4550),
    "อำนาจเจริญ":          (15.8656, 104.6259),
    "อุดรธานี":            (17.4138, 102.7872),
    "อุตรดิตถ์":           (17.6200, 100.0993),
    "อุทัยธานี":           (15.3835, 100.0255),
    "อุบลราชธานี":         (15.2448, 104.8473),
}

# Region label mapping by prov_code range
def get_region_en(prov_code: int) -> str:
    if 10 <= prov_code <= 19: return "Central"
    if 20 <= prov_code <= 27: return "Eastern"
    if 30 <= prov_code <= 49: return "Northeastern"
    if 50 <= prov_code <= 69: return "Northern"
    if 70 <= prov_code <= 79: return "Western"
    if 80 <= prov_code <= 99: return "Southern"
    return "Unknown"


def export_alerts():
    src = EXPORTS_DIR / "alerts.json"
    dst = CSV_DIR / "alerts.csv"

    with open(src, encoding="utf-8") as f:
        raw = json.load(f)

    records = raw.get("data", raw) if isinstance(raw, dict) else raw

    fieldnames = [
        "alert_date", "prov_code", "prov_th",
        "alert_level", "alert_level_en",
        "alert_score", "reservoir_score", "weather_score", "historical_risk_score",
        "trigger_reservoirs", "max_reservoir_pct", "rainfall_forecast_mm",
        "historical_risk_level", "affected_subdistricts_count",
        "lat", "lon", "region_en",
    ]

    # Map Thai alert level → English for Power BI color encoding
    LEVEL_EN = {
        "วิกฤต": "CRISIS",
        "เตือนภัย": "ALERT",
        "เฝ้าระวัง": "WATCH",
        "ปกติ": "NORMAL",
    }

    rows = []
    for rec in records:
        prov = rec.get("prov_th", "")
        lat, lon = PROVINCE_COORDS.get(prov, (None, None))
        prov_code = int(rec.get("prov_code", 0))
        level = rec.get("alert_level", "ปกติ")
        rows.append({
            "alert_date":                  rec.get("alert_date", ""),
            "prov_code":                   prov_code,
            "prov_th":                     prov,
            "alert_level":                 level,
            "alert_level_en":              LEVEL_EN.get(level, "NORMAL"),
            "alert_score":                 rec.get("alert_score", 0),
            "reservoir_score":             rec.get("reservoir_score", 0),
            "weather_score":               rec.get("weather_score", 0),
            "historical_risk_score":       rec.get("historical_risk_score", 0),
            "trigger_reservoirs":          rec.get("trigger_reservoirs", ""),
            "max_reservoir_pct":           rec.get("max_reservoir_pct", 0),
            "rainfall_forecast_mm":        rec.get("rainfall_forecast_mm", 0),
            "historical_risk_level":       rec.get("historical_risk_level", ""),
            "affected_subdistricts_count": rec.get("affected_subdistricts_count", 0),
            "lat":                         lat,
            "lon":                         lon,
            "region_en":                   get_region_en(prov_code),
        })

    with open(dst, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"[alerts] {len(rows)} rows → {dst}")


def export_reservoirs():
    src = EXPORTS_DIR / "reservoirs.json"
    dst = CSV_DIR / "reservoirs.csv"

    with open(src, encoding="utf-8") as f:
        raw = json.load(f)

    records = raw.get("data", raw) if isinstance(raw, dict) else raw

    fieldnames = [
        "reservoir_id", "reservoir_name", "source", "region",
        "record_date", "capacity_mcm", "volume_mcm", "percent_storage",
        "inflow_mcm", "outflow_mcm", "net_flow_mcm",
        "storage_trend_7d", "storage_delta_1d", "days_to_full",
        "reservoir_status", "filling_rate",
    ]

    rows = []
    for rec in records:
        row = {f: rec.get(f, "") for f in fieldnames}
        # Clamp days_to_full display (365 = sentinel for "no trend")
        dtf = row.get("days_to_full", "")
        try:
            if abs(float(dtf)) >= 365:
                row["days_to_full"] = ""
        except (TypeError, ValueError):
            row["days_to_full"] = ""
        rows.append(row)

    with open(dst, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"[reservoirs] {len(rows)} rows → {dst}")


def export_pipeline_meta():
    src = EXPORTS_DIR / "pipeline_meta.json"
    dst = CSV_DIR / "pipeline_meta.csv"

    with open(src, encoding="utf-8") as f:
        meta = json.load(f)

    # Flatten alert_summary into separate columns
    summary = meta.pop("alert_summary", {})
    meta["alert_crisis"]  = summary.get("วิกฤต", 0)
    meta["alert_warning"] = summary.get("เตือนภัย", 0)
    meta["alert_watch"]   = summary.get("เฝ้าระวัง", 0)
    meta["alert_normal"]  = summary.get("ปกติ", 0)

    with open(dst, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(meta.keys()))
        writer.writeheader()
        writer.writerow(meta)

    print(f"[pipeline_meta] 1 row → {dst}")


if __name__ == "__main__":
    print("Exporting Gold layer → CSV for Power BI...")
    export_alerts()
    export_reservoirs()
    export_pipeline_meta()
    print(f"\nDone. CSV files saved to: {CSV_DIR.resolve()}")
    print("Next: open Power BI Desktop → Get Data → Text/CSV → load these files")
