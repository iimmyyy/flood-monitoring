"""
mock_weather_generator.py — Fetch real TMD weather forecast → HDFS Bronze
(filename kept as-is for DAG compatibility)

Data source: TMD NWP API (กรมอุตุนิยมวิทยา)
  Endpoint: https://data.tmd.go.th/nwpapi/v1/forecast/location/daily/at
  Auth:     Bearer JWT token
  Params:   lat, lon, fields, duration

Strategy:
  - Call TMD API for each province (lat/lon lookup).
  - If API returns valid data  → use real values.
  - If API fails / rate-limit  → fall back to monsoon-based mock (same as before).

Output HDFS path: /bronze/weather/YYYY-MM-DD/weather_HHMMSS.json
Each record is one NDJSON line (one province per line).

Run:
    python mock_weather_generator.py [YYYY-MM-DD]   (default: today)
"""

import json
import logging
import math
import os
import random
import sys
import time
from datetime import datetime, timezone

import requests

from hdfs_util import hdfs_makedirs, hdfs_write

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [weather_ingest] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
HDFS_URL  = os.getenv("HDFS_NAMENODE_URL", "http://namenode:9870")
HDFS_USER = os.getenv("HDFS_USER", "root")

TMD_API_URL = "https://data.tmd.go.th/nwpapi/v1/forecast/location/daily/at"
TMD_API_KEY = os.getenv(
    "TMD_API_KEY",
    "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsImp0aSI6ImZlNjBlYzE1NmFiNTcxOWE3MGY4ZjY0MGE0YjVmYTQ3ZDhhMDNhM2MyZGYyNzBkNTk0ZTUyNGJkYWU0YzhiMDQ5YTU5ZmVjMjI5NGZjOTczIn0.eyJhdWQiOiIyIiwianRpIjoiZmU2MGVjMTU2YWI1NzE5YTcwZjhmNjQwYTRiNWZhNDdkOGEwM2EzYzJkZjI3MGQ1OTRlNTI0YmRhZTRjOGIwNDlhNTlmZWMyMjk0ZmM5NzMiLCJpYXQiOjE3NzY3NjYzMTMsIm5iZiI6MTc3Njc2NjMxMywiZXhwIjoxODA4MzAyMzEzLCJzdWIiOiI1MjA4Iiwic2NvcGVzIjpbXX0.EAWn08CSOopVlD_i97ns9otueZ1_m29VNg83HTU-G07hQ8uIqX86CG-IoVSwut9yaghr_DGX1-ZiUD6kH1NMaJikHPZ-i6g48XErgVagUEAwS-6tD2LB8asaPce169NPfU5QLZTqNd6AsbGez3nzf35zzMJAmmKe9L_aNHqVJwKwTht3pBI9uyY58MY2E3na_O3mWRvfm95apn8KzvWEj9W-rwxGlKaElU47q1R9JWzxhKpzkMIsgnp_4LrXZEDu71nko4pX2E_4XXm2I8N5XqrNaeroo6fYzwoZaptxW5gonlApvKXoRHKiULassuMV5aPO9hG09Y4yDcEe6OXENZw6tcPX1p25vkiTVS5Riu6bNS7jTyfsRSF16uPorZA3CqwU6MAlvD1LRUY1sC1duGlfQaS2uBt0q6QuH_U_eUZGSd_tIwfhK8n3zJTrZSI64nZBPUx9N9Qsz1kA7dUCAkluJVHjxLTx8VbihsvMhoQawtJ6yhKlCjOTAJfJ2t4GUZTFGyadynoqsID0HjtPm2irtlkS8G51lxxV1eGMvf1KdWY6eDwuZWHi0HrLI1CMFqNV3euRWZBFazLfRrLr43vv4NaIyySmzUzsSban048vFALShrIZ6dqVKYRsV8NLe9SPmiDyPHB5gg4JBkixSY_wN2f8SFhrp2bslWiA8cs",
)

# Request timeout per province (seconds)
TMD_TIMEOUT = 10
# Delay between API calls to avoid rate limiting
TMD_INTER_CALL_DELAY = 0.3

# ── Province catalogue ────────────────────────────────────────────────────────
# (prov_code, prov_th, prov_en, region, lat, lon)
THAI_PROVINCES = [
    (10, "กรุงเทพมหานคร",     "Bangkok",                  "C",  13.75,  100.52),
    (11, "สมุทรปราการ",       "Samut Prakan",             "C",  13.60,  100.60),
    (12, "นนทบุรี",           "Nonthaburi",               "C",  13.86,  100.51),
    (13, "ปทุมธานี",          "Pathum Thani",             "C",  14.02,  100.52),
    (14, "พระนครศรีอยุธยา",   "Phra Nakhon Si Ayutthaya", "C",  14.36,  100.56),
    (15, "อ่างทอง",           "Ang Thong",                "C",  14.59,  100.46),
    (16, "ลพบุรี",            "Lop Buri",                 "C",  14.80,  100.65),
    (17, "สิงห์บุรี",         "Sing Buri",                "C",  14.89,  100.40),
    (18, "ชัยนาท",            "Chai Nat",                 "C",  15.19,  100.13),
    (19, "สระบุรี",           "Saraburi",                 "C",  14.52,  100.91),
    (20, "ชลบุรี",            "Chon Buri",                "E",  13.36,  100.99),
    (21, "ระยอง",             "Rayong",                   "E",  12.68,  101.28),
    (22, "จันทบุรี",          "Chanthaburi",              "E",  12.61,  102.10),
    (23, "ตราด",              "Trat",                     "E",  12.24,  102.52),
    (24, "ฉะเชิงเทรา",        "Chachoengsao",             "C",  13.69,  101.08),
    (25, "ปราจีนบุรี",        "Prachin Buri",             "E",  14.05,  101.37),
    (26, "นครนายก",           "Nakhon Nayok",             "C",  14.21,  101.21),
    (27, "สระแก้ว",           "Sa Kaeo",                  "E",  13.82,  102.06),
    (30, "นครราชสีมา",        "Nakhon Ratchasima",        "NE", 14.97,  102.10),
    (31, "บุรีรัมย์",         "Buri Ram",                 "NE", 14.99,  103.10),
    (32, "สุรินทร์",          "Surin",                    "NE", 14.88,  103.50),
    (33, "ศรีสะเกษ",          "Si Sa Ket",                "NE", 15.12,  104.33),
    (34, "อุบลราชธานี",       "Ubon Ratchathani",         "NE", 15.23,  104.86),
    (35, "ยโสธร",             "Yasothon",                 "NE", 15.79,  104.14),
    (36, "ชัยภูมิ",           "Chaiyaphum",               "NE", 15.81,  102.03),
    (37, "อำนาจเจริญ",        "Amnat Charoen",            "NE", 15.86,  104.63),
    (38, "บึงกาฬ",            "Bueng Kan",                "NE", 18.36,  103.65),
    (39, "หนองบัวลำภู",       "Nong Bua Lam Phu",         "NE", 17.20,  102.43),
    (40, "ขอนแก่น",           "Khon Kaen",                "NE", 16.44,  102.84),
    (41, "อุดรธานี",          "Udon Thani",               "NE", 17.41,  102.79),
    (42, "เลย",               "Loei",                     "NE", 17.49,  101.74),
    (43, "หนองคาย",           "Nong Khai",                "NE", 17.88,  102.74),
    (44, "มหาสารคาม",         "Maha Sarakham",            "NE", 16.18,  103.30),
    (45, "ร้อยเอ็ด",          "Roi Et",                   "NE", 16.05,  103.65),
    (46, "กาฬสินธุ์",         "Kalasin",                  "NE", 16.43,  103.51),
    (47, "สกลนคร",            "Sakon Nakhon",             "NE", 17.16,  104.15),
    (48, "นครพนม",            "Nakhon Phanom",            "NE", 17.40,  104.77),
    (49, "มุกดาหาร",          "Mukdahan",                 "NE", 16.54,  104.72),
    (50, "เชียงใหม่",         "Chiang Mai",               "N",  18.79,   98.98),
    (51, "ลำพูน",             "Lamphun",                  "N",  18.57,   99.00),
    (52, "ลำปาง",             "Lampang",                  "N",  18.29,   99.50),
    (53, "อุตรดิตถ์",         "Uttaradit",                "N",  17.62,  100.10),
    (54, "แพร่",              "Phrae",                    "N",  18.15,  100.14),
    (55, "น่าน",              "Nan",                      "N",  18.78,  100.78),
    (56, "พะเยา",             "Phayao",                   "N",  19.14,   99.90),
    (57, "เชียงราย",          "Chiang Rai",               "N",  19.91,   99.84),
    (58, "แม่ฮ่องสอน",        "Mae Hong Son",             "N",  19.30,   97.97),
    (60, "นครสวรรค์",         "Nakhon Sawan",             "N",  15.71,  100.13),
    (61, "อุทัยธานี",         "Uthai Thani",              "N",  15.38,  100.02),
    (62, "กำแพงเพชร",         "Kamphaeng Phet",           "N",  16.48,   99.52),
    (63, "ตาก",               "Tak",                      "N",  16.88,   99.13),
    (64, "สุโขทัย",           "Sukhothai",                "N",  17.01,   99.83),
    (65, "พิษณุโลก",          "Phitsanulok",              "N",  16.83,  100.26),
    (66, "พิจิตร",            "Phichit",                  "N",  16.44,  100.35),
    (67, "เพชรบูรณ์",         "Phetchabun",               "N",  16.42,  101.16),
    (70, "ราชบุรี",           "Ratchaburi",               "W",  13.54,   99.82),
    (71, "กาญจนบุรี",         "Kanchanaburi",             "W",  14.00,   99.55),
    (72, "สุพรรณบุรี",        "Suphan Buri",              "W",  14.47,  100.12),
    (73, "นครปฐม",            "Nakhon Pathom",            "W",  13.82,  100.04),
    (74, "สมุทรสาคร",         "Samut Sakhon",             "W",  13.55,  100.27),
    (75, "สมุทรสงคราม",       "Samut Songkhram",          "W",  13.41,  100.00),
    (76, "เพชรบุรี",          "Phetchaburi",              "W",  13.11,   99.94),
    (77, "ประจวบคีรีขันธ์",   "Prachuap Khiri Khan",      "W",  11.81,   99.80),
    (80, "นครศรีธรรมราช",     "Nakhon Si Thammarat",      "S",   8.43,  100.00),
    (81, "กระบี่",            "Krabi",                    "S",   8.07,   98.91),
    (82, "พังงา",             "Phang Nga",                "S",   8.45,   98.53),
    (83, "ภูเก็ต",            "Phuket",                   "S",   7.89,   98.40),
    (84, "สุราษฎร์ธานี",      "Surat Thani",              "S",   9.14,   99.33),
    (85, "ระนอง",             "Ranong",                   "S",   9.96,   98.61),
    (86, "ชุมพร",             "Chumphon",                 "S",  10.50,   99.18),
    (90, "สงขลา",             "Songkhla",                 "S",   7.19,  100.59),
    (91, "สตูล",              "Satun",                    "S",   6.62,  100.07),
    (92, "ตรัง",              "Trang",                    "S",   7.56,   99.61),
    (93, "พัทลุง",            "Phatthalung",              "S",   7.62,  100.08),
    (94, "ปัตตานี",           "Pattani",                  "S",   6.87,  101.25),
    (95, "ยะลา",              "Yala",                     "S",   6.55,  101.28),
    (96, "นราธิวาส",          "Narathiwat",               "S",   6.42,  101.82),
]

# Rain intensity thresholds (กรมอุตุ standard, mm/24h)
RAIN_THRESHOLDS = [
    (0.1,         "ไม่มีฝน"),
    (10.0,        "ฝนเล็กน้อย"),
    (35.0,        "ฝนปานกลาง"),
    (90.0,        "ฝนหนัก"),
    (float("inf"),"ฝนหนักมาก"),
]


def classify_rain(mm: float) -> str:
    for threshold, label in RAIN_THRESHOLDS:
        if mm < threshold:
            return label
    return "ฝนหนักมาก"


# ── TMD API helper ────────────────────────────────────────────────────────────
def extract_value(v):
    """TMD API may return float or {'value': float, 'unit': '...'}."""
    if isinstance(v, dict):
        return v.get("value", 0.0) or 0.0
    try:
        return float(v) if v is not None else 0.0
    except (TypeError, ValueError):
        return 0.0


def fetch_tmd_province(lat: float, lon: float, target_date: str) -> dict | None:
    """
    Call TMD NWP daily forecast API for a single lat/lon.

    Returns dict with weather fields on success, None on any failure.

    TMD response structure (WeatherForecasts endpoint):
    {
      "WeatherForecasts": [{
        "location": {"lat": ..., "lon": ...},
        "forecasts": [{
          "time": "YYYY-MM-DDTHH:MM:SS+07:00",
          "data": {
            "tc_max": <float>,   # max temp °C
            "tc_min": <float>,   # min temp °C
            "rh":     <float>,   # relative humidity %
            "rain":   <float>,   # rainfall mm/24h
            "ws10m":  <float>,   # wind speed m/s at 10m
            "wd10m":  <float>,   # wind direction degrees
            "cond":   <int>,     # weather condition code
          }
        }]
      }]
    }
    """
    headers = {
        "Authorization": f"Bearer {TMD_API_KEY}",
        "Accept": "application/json",
    }
    params = {
        "lat":      lat,
        "lon":      lon,
        "fields":   "tc_max,tc_min,rh,rain,ws10m,cond",
        "duration": "1",
    }
    try:
        resp = requests.get(TMD_API_URL, headers=headers, params=params, timeout=TMD_TIMEOUT)
        if resp.status_code != 200:
            log.debug("TMD API HTTP %d for lat=%.2f lon=%.2f", resp.status_code, lat, lon)
            return None
        payload = resp.json()
    except Exception as exc:
        log.debug("TMD API error for lat=%.2f lon=%.2f: %s", lat, lon, exc)
        return None

    # Navigate: WeatherForecasts[0].forecasts[0].data
    try:
        wf = payload.get("WeatherForecasts", [])
        if not wf:
            return None
        forecasts = wf[0].get("forecasts", [])
        if not forecasts:
            return None
        data = forecasts[0].get("data", {})
        if not data:
            return None

        return {
            "temp_max_c":            round(extract_value(data.get("tc_max")), 1),
            "temp_min_c":            round(extract_value(data.get("tc_min")), 1),
            "rainfall_forecast_mm":  round(extract_value(data.get("rain")), 1),
            "humidity_pct":          round(extract_value(data.get("rh")), 1),
            "wind_speed_kmh":        round(extract_value(data.get("ws10m")) * 3.6, 1),  # m/s → km/h
            "data_source":           "tmd_api",
        }
    except (KeyError, IndexError, TypeError) as exc:
        log.debug("TMD API parse error: %s | payload keys: %s", exc, list(payload.keys()))
        return None


# ── Mock fallback (monsoon-based) ─────────────────────────────────────────────
MONSOON_CALENDAR = {
    "C":  [0.1, 0.1, 0.15, 0.3, 0.5, 0.7, 0.8, 0.85, 0.85, 0.7, 0.3, 0.1],
    "N":  [0.1, 0.1, 0.2,  0.3, 0.5, 0.7, 0.8, 0.8,  0.8,  0.6, 0.2, 0.1],
    "NE": [0.1, 0.1, 0.15, 0.3, 0.45,0.65,0.75,0.8,  0.8,  0.65,0.3, 0.1],
    "E":  [0.3, 0.2, 0.2,  0.3, 0.5, 0.6, 0.6, 0.7,  0.8,  0.7, 0.5, 0.4],
    "W":  [0.1, 0.1, 0.2,  0.35,0.55,0.7, 0.8, 0.85, 0.85, 0.7, 0.3, 0.1],
    "S":  [0.5, 0.4, 0.3,  0.3, 0.4, 0.5, 0.6, 0.65, 0.7,  0.75,0.8, 0.6],
}


def mock_weather(region: str, month: int, rng: random.Random) -> dict:
    rain_prob  = MONSOON_CALENDAR.get(region, MONSOON_CALENDAR["C"])[month - 1]
    base_temp  = 32.0 if region != "S" else 31.0
    temp_offset = -2 * math.cos(2 * math.pi * (month - 1) / 12)
    temp_max   = round(base_temp + temp_offset + rng.gauss(0, 1.5), 1)
    temp_min   = round(temp_max - rng.uniform(5, 10), 1)
    rain_mm    = (
        round(min(rng.gammavariate(2, 15) * rain_prob * 2, 300.0), 1)
        if rng.random() < rain_prob else 0.0
    )
    humidity   = round(max(40.0, min(100.0, 60 + rain_prob * 30 + rng.gauss(0, 5))), 1)
    wind_speed = round(max(0.0, 10 + rain_prob * 15 + rng.gauss(0, 3)), 1)
    return {
        "temp_max_c":           temp_max,
        "temp_min_c":           temp_min,
        "rainfall_forecast_mm": rain_mm,
        "humidity_pct":         humidity,
        "wind_speed_kmh":       wind_speed,
        "data_source":          "mock_tmd",
    }


# ── Assemble full weather record ──────────────────────────────────────────────
def build_record(
    prov_code: int,
    prov_th: str,
    prov_en: str,
    region: str,
    lat: float,
    lon: float,
    target_date: str,
    fetched_at: str,
    weather: dict,
) -> dict:
    rain_mm = weather["rainfall_forecast_mm"]
    return {
        "prov_code":             prov_code,
        "prov_th":               prov_th,
        "prov_en":               prov_en,
        "region":                region,
        "latitude":              lat,
        "longitude":             lon,
        "forecast_date":         target_date,
        "temp_max_c":            weather["temp_max_c"],
        "temp_min_c":            weather["temp_min_c"],
        "rainfall_forecast_mm":  rain_mm,
        "rain_intensity":        classify_rain(rain_mm),
        "humidity_pct":          weather["humidity_pct"],
        "wind_speed_kmh":        weather["wind_speed_kmh"],
        "is_heavy_rain_24h":     rain_mm >= 35.0,
        "data_source":           weather["data_source"],
        "generated_at":          fetched_at,
    }


# ── HDFS writer ───────────────────────────────────────────────────────────────
def write_to_hdfs(records: list[dict], target_date: str) -> None:
    hdfs_dir  = f"/bronze/weather/{target_date}"
    hdfs_makedirs(hdfs_dir, hdfs_url=HDFS_URL, user=HDFS_USER)

    timestamp = datetime.now().strftime("%H%M%S")
    hdfs_path = f"{hdfs_dir}/weather_{timestamp}.json"
    content   = "\n".join(json.dumps(r, ensure_ascii=False) for r in records)

    hdfs_write(hdfs_path, content.encode("utf-8"), hdfs_url=HDFS_URL, user=HDFS_USER, overwrite=True)
    log.info("Wrote %d records → HDFS %s", len(records), hdfs_path)


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    date_arg = sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime("%Y-%m-%d")
    try:
        datetime.strptime(date_arg, "%Y-%m-%d")
    except ValueError:
        log.error("Invalid date: %s. Use YYYY-MM-DD.", date_arg)
        sys.exit(1)

    fetched_at = datetime.now(timezone.utc).isoformat()
    month      = int(date_arg.split("-")[1])

    # Use date as RNG seed (mock fallback reproducible per day)
    rng = random.Random(date_arg)

    log.info(
        "Weather ingestion started -- date=%s | %d provinces | TMD API key: %s",
        date_arg, len(THAI_PROVINCES), TMD_API_KEY[:20],
    )

    records       = []
    tmd_success   = 0
    mock_fallback = 0

    for prov_code, prov_th, prov_en, region, lat, lon in THAI_PROVINCES:
        # Try real TMD API first
        tmd_data = fetch_tmd_province(lat, lon, date_arg)

        if tmd_data:
            weather = tmd_data
            tmd_success += 1
        else:
            weather = mock_weather(region, month, rng)
            mock_fallback += 1

        records.append(
            build_record(prov_code, prov_th, prov_en, region, lat, lon,
                         date_arg, fetched_at, weather)
        )

        # Small delay to be polite to the API
        time.sleep(TMD_INTER_CALL_DELAY)

    heavy_rain = sum(1 for r in records if r["is_heavy_rain_24h"])
    log.info(
        "Weather done -- %d provinces | TMD API: %d | mock fallback: %d | heavy rain: %d",
        len(records), tmd_success, mock_fallback, heavy_rain,
    )

    write_to_hdfs(records, date_arg)
    log.info("Weather ingestion complete for %s.", date_arg)


if __name__ == "__main__":
    main()
