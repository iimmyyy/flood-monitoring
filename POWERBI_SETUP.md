# Power BI Setup Guide — Thailand Flood Risk Dashboard

> **Prerequisites**: Power BI Desktop (free) installed on Windows  
> **Data path**: `flood-monitoring/data/exports/csv/`  
> **Last updated**: 2026-04-24

---

## Overview

This guide walks through loading the Gold-layer CSV exports into Power BI Desktop and building four visuals:

| # | Visual | Data source |
|---|--------|-------------|
| 1 | Province alert map (filled/bubble) | `alerts.csv` |
| 2 | Alert level summary (donut chart) | `alerts.csv` |
| 3 | Score breakdown by province (stacked bar) | `alerts.csv` |
| 4 | Reservoir status table | `reservoirs.csv` |

A fifth table (`pipeline_meta.csv`) is used for a header card showing last-run time and DQ status.

---

## Step 1 — Generate the CSV files

Run from the project root (or any directory):

```bash
python flood-monitoring/scripts/export_gold_to_csv.py
```

Expected output:
```
[alerts]        77 rows  → data/exports/csv/alerts.csv
[reservoirs]   483 rows  → data/exports/csv/reservoirs.csv
[pipeline_meta]  1 row   → data/exports/csv/pipeline_meta.csv
```

Re-run this script each time you want to refresh the dashboard data.

---

## Step 2 — Load CSV files into Power BI Desktop

### 2.1 Open Power BI Desktop

**Home → Get data → Text/CSV**

Load all three files one by one:

| File | Suggested table name |
|------|---------------------|
| `data/exports/csv/alerts.csv` | `Alerts` |
| `data/exports/csv/reservoirs.csv` | `Reservoirs` |
| `data/exports/csv/pipeline_meta.csv` | `PipelineMeta` |

> **Encoding tip**: Files use UTF-8 with BOM (`utf-8-sig`). Power BI detects this automatically — Thai characters will render correctly without any extra steps.

### 2.2 Set column data types (Power Query Editor)

After loading, click **Transform data** to open Power Query.

**Alerts table** — set these types:

| Column | Type |
|--------|------|
| `alert_date` | Date |
| `prov_code` | Whole Number |
| `alert_score`, `reservoir_score`, `weather_score`, `historical_risk_score` | Decimal Number |
| `max_reservoir_pct`, `rainfall_forecast_mm` | Decimal Number |
| `affected_subdistricts_count` | Whole Number |
| `lat`, `lon` | Decimal Number |
| All others | Text |

**Reservoirs table** — set these types:

| Column | Type |
|--------|------|
| `record_date` | Date |
| `capacity_mcm`, `volume_mcm`, `inflow_mcm`, `outflow_mcm`, `net_flow_mcm` | Decimal Number |
| `percent_storage`, `storage_delta_1d`, `storage_trend_7d` | Decimal Number |
| `days_to_full`, `filling_rate` | Text (has empty strings) |
| All others | Text |

Click **Close & Apply** when done.

### 2.3 Set geographic data categories (for Map visual)

In the **Data** pane (right panel), click the `Alerts` table:

- Select `lat` column → **Column tools → Data category → Latitude**
- Select `lon` column → **Column tools → Data category → Longitude**
- Select `prov_th` column → **Column tools → Data category → City** (closest match)

---

## Step 3 — Create a color measure for alert levels

Power BI Map and bar visuals need a consistent color scheme. Create a DAX measure:

1. **Home → New measure** (with `Alerts` table selected)
2. Paste this measure:

```dax
AlertColor = 
SWITCH(
    SELECTEDVALUE(Alerts[alert_level_en]),
    "CRISIS",  "#D32F2F",   -- Red
    "ALERT",   "#F57C00",   -- Orange
    "WATCH",   "#FBC02D",   -- Yellow
    "NORMAL",  "#388E3C",   -- Green
    "#9E9E9E"               -- Grey (unknown)
)
```

> **Note**: This measure works best with single-value selections. For chart colors, use the `alert_level_en` field with a custom color palette applied manually (see Visual 2 below).

---

## Step 4 — Visual 1: Province Alert Map

### Option A — Bubble Map (recommended, easier setup)

1. **Visualizations pane → Map** (blue globe icon)
2. Drag fields:

| Field well | Column |
|-----------|--------|
| Latitude | `Alerts[lat]` |
| Longitude | `Alerts[lon]` |
| Size | `Alerts[alert_score]` |
| Legend | `Alerts[alert_level_en]` |
| Tooltips | `Alerts[prov_th]`, `Alerts[alert_level_en]`, `Alerts[alert_score]` |

3. Set legend colors (**Format visual → Legend → Colors**):

| Value | Color |
|-------|-------|
| CRISIS | `#D32F2F` |
| ALERT | `#F57C00` |
| WATCH | `#FBC02D` |
| NORMAL | `#388E3C` |

4. **Format visual → Bubbles → Min size**: 5, **Max size**: 20

### Option B — Filled Map (province choropleth)

1. **Visualizations pane → Filled map**
2. Drag fields:

| Field well | Column |
|-----------|--------|
| Location | `Alerts[prov_th]` |
| Color saturation | `Alerts[alert_score]` |
| Tooltips | `Alerts[alert_level_en]`, `Alerts[alert_score]` |

> Power BI may not recognize Thai province names for the filled map. Bubble Map (Option A) is more reliable since it uses explicit lat/lon coordinates.

---

## Step 5 — Visual 2: Alert Level Summary (Donut Chart)

1. **Visualizations pane → Donut chart**
2. Drag fields:

| Field well | Column |
|-----------|--------|
| Legend | `Alerts[alert_level_en]` |
| Values | `Alerts[prov_code]` (set aggregation to **Count**) |

3. **Format visual → Slices → Colors** — set manually:

| Slice | Color |
|-------|-------|
| CRISIS | `#D32F2F` |
| ALERT | `#F57C00` |
| WATCH | `#FBC02D` |
| NORMAL | `#388E3C` |

4. **Format visual → Detail labels** → Show category + value

**Alternative**: Use a **Clustered bar chart** instead:
- Y-axis: `alert_level_en`
- X-axis: `prov_code` (Count)
- Sort bars: CRISIS → ALERT → WATCH → NORMAL

To force the sort order, create a sort-order column in Power Query:

```m
= Table.AddColumn(Source, "alert_order", each 
    if [alert_level_en] = "CRISIS" then 1
    else if [alert_level_en] = "ALERT" then 2
    else if [alert_level_en] = "WATCH" then 3
    else 4)
```

Then: right-click `alert_level_en` → **Sort by column → alert_order**.

---

## Step 6 — Visual 3: Score Breakdown by Province (Stacked Bar)

This visual shows which score component is driving each province's alert — useful for explaining *why* a province is flagged.

1. **Visualizations pane → Stacked bar chart**
2. Drag fields:

| Field well | Column |
|-----------|--------|
| Y-axis | `Alerts[prov_th]` |
| X-axis (Values) | `Alerts[reservoir_score]`, `Alerts[weather_score]`, `Alerts[historical_risk_score]` |
| Tooltips | `Alerts[alert_level_en]`, `Alerts[alert_score]`, `Alerts[trigger_reservoirs]` |

3. Filter to non-normal provinces only:
   - **Filters pane → Add filter → alert_level_en**
   - Check: CRISIS, ALERT, WATCH (uncheck NORMAL)

4. **Format visual → Bars → Colors**:

| Series | Color |
|--------|-------|
| `reservoir_score` | `#1565C0` (blue) |
| `weather_score` | `#00838F` (teal) |
| `historical_risk_score` | `#6A1B9A` (purple) |

5. **Format visual → Y-axis → Sort** → sort by `alert_score` descending (highest risk at top)

---

## Step 7 — Visual 4: Reservoir Status Table

1. **Visualizations pane → Table**
2. Add columns:

| Column | Notes |
|--------|-------|
| `Reservoirs[reservoir_name]` | Thai name |
| `Reservoirs[region]` | Thai region (ภาคเหนือ etc.) |
| `Reservoirs[percent_storage]` | Show as percentage |
| `Reservoirs[reservoir_status]` | CRITICAL / HIGH / NORMAL / LOW |
| `Reservoirs[filling_rate]` | FAST / MODERATE / STABLE / DRAINING |
| `Reservoirs[inflow_mcm]` | Inflow |
| `Reservoirs[outflow_mcm]` | Outflow |
| `Reservoirs[days_to_full]` | Empty = no trend |

3. Add conditional formatting on `percent_storage`:
   - Click `percent_storage` column → **Format → Conditional formatting → Background color**
   - Rules:
     - ≥ 85 → `#D32F2F` (red — CRITICAL)
     - ≥ 70 → `#F57C00` (orange — HIGH)
     - ≥ 30 → `#388E3C` (green — NORMAL)
     - < 30 → `#FBC02D` (yellow — LOW)

4. Add a slicer for `reservoir_status` to filter by CRITICAL/HIGH/NORMAL/LOW.

5. Add a search box:
   - **Format visual → Search** → On  
   This lets users type a reservoir name to filter the table.

---

## Step 8 — Pipeline status card (optional)

Shows when the pipeline last ran and DQ status.

1. Add a **Card** visual
2. Field: `PipelineMeta[exported_at]`
3. Title: "Last updated"

Add a second **Card**:
- Field: `PipelineMeta[dq_overall]`
- Title: "DQ Status"

Add conditional color: if value = "PASS" → green text, else red.

---

## Step 9 — Layout and slicers

### Recommended layout

```
┌──────────────────────────────────────────────┐
│  Thailand Flood Risk Dashboard  [Last: ...]  │
├──────────────────┬───────────────────────────┤
│                  │  Alert Summary (donut)    │
│  Province Map    ├───────────────────────────┤
│  (Visual 1)      │  Score Breakdown (bar)    │
│                  │  (Visual 3)               │
├──────────────────┴───────────────────────────┤
│  Reservoir Status Table (Visual 4)           │
└──────────────────────────────────────────────┘
```

### Add cross-filter slicers

Add these slicers (Visualizations → Slicer) on the left or top:

| Slicer | Field |
|--------|-------|
| Region | `Alerts[region_en]` |
| Alert level | `Alerts[alert_level_en]` |
| Date | `Alerts[alert_date]` |

Enable **cross-filtering**: clicking a province on the map automatically filters the score breakdown and reservoir table.

---

## Step 10 — Refresh data

Each time you run the pipeline:

1. Run `python scripts/export_gold_to_csv.py` to regenerate CSVs
2. In Power BI Desktop: **Home → Refresh**

### Auto-refresh (scheduled)

For automated refresh, set up the export script as a scheduled task:

```powershell
# Windows Task Scheduler — run daily at 07:00
schtasks /create /tn "FloodCSVExport" /tr "python D:\Study\Data_en y-3\proj\flood-monitoring\scripts\export_gold_to_csv.py" /sc daily /st 07:00
```

Then configure Power BI Desktop to auto-refresh on open:
**File → Options → Current file → Data load → Refresh data when opening the file** ✓

---

## CSV Column Reference

### alerts.csv (77 rows — one per province)

| Column | Type | Description |
|--------|------|-------------|
| `alert_date` | Date | Pipeline run date |
| `prov_code` | Int | Province code (10–96) |
| `prov_th` | Text | Province name (Thai) |
| `alert_level` | Text | Thai level (วิกฤต / เตือนภัย / เฝ้าระวัง / ปกติ) |
| `alert_level_en` | Text | English level (CRISIS / ALERT / WATCH / NORMAL) |
| `alert_score` | Float | Total score 0–100 |
| `reservoir_score` | Float | Reservoir component 0–40 |
| `weather_score` | Float | Weather component 0–35 |
| `historical_risk_score` | Float | Historical component 0–25 |
| `trigger_reservoirs` | Text | Comma-separated reservoir names that triggered alert |
| `max_reservoir_pct` | Float | Highest reservoir % among triggers |
| `rainfall_forecast_mm` | Float | Forecasted rainfall (mm) |
| `historical_risk_level` | Text | เสี่ยงสูง / เสี่ยงปานกลาง / เสี่ยงต่ำ |
| `affected_subdistricts_count` | Int | Count of affected sub-districts |
| `lat` | Float | Province centroid latitude |
| `lon` | Float | Province centroid longitude |
| `region_en` | Text | Central / Eastern / Northeastern / Northern / Western / Southern |

### reservoirs.csv (~483 rows)

| Column | Type | Description |
|--------|------|-------------|
| `reservoir_id` | Text | Unique ID (rsvXXX) |
| `reservoir_name` | Text | Reservoir name (Thai) |
| `source` | Text | Data source |
| `region` | Text | Thai region name |
| `record_date` | Date | Observation date |
| `capacity_mcm` | Float | Design capacity (million m³) |
| `volume_mcm` | Float | Current volume (million m³) |
| `percent_storage` | Float | % of capacity (0–100+) |
| `inflow_mcm` | Float | Inflow (million m³/day) |
| `outflow_mcm` | Float | Outflow (million m³/day) |
| `net_flow_mcm` | Float | Net flow (inflow − outflow) |
| `storage_trend_7d` | Float | 7-day % change trend |
| `storage_delta_1d` | Float | 1-day % change |
| `days_to_full` | Text | Estimated days to full (empty = no trend) |
| `reservoir_status` | Text | CRITICAL / HIGH / NORMAL / LOW |
| `filling_rate` | Text | FAST / MODERATE / STABLE / DRAINING |

### pipeline_meta.csv (1 row)

| Column | Description |
|--------|-------------|
| `export_date` | Date of export |
| `exported_at` | Full timestamp |
| `reservoir_count` | Total reservoirs in export |
| `alert_province_count` | Total provinces with alerts |
| `last_fetched_at` | When raw data was last fetched |
| `dq_overall` | PASS / FAIL |
| `dq_run_date` | DQ check date |
| `dq_rules_pass` / `dq_rules_total` | DQ rule counts |
| `alert_crisis` / `alert_warning` / `alert_watch` / `alert_normal` | Province counts per level |

---

## Troubleshooting

| Problem | Cause | Fix |
|---------|-------|-----|
| Thai text shows as `???` or boxes | Wrong encoding detected | In Power Query: **File origin → 65001: Unicode (UTF-8)** |
| Map shows no bubbles | lat/lon not set as Decimal Number | Set column type in Power Query, set Data category in Column tools |
| Map pins all cluster at (0, 0) | lat/lon type is Text | Change to Decimal Number in Power Query |
| Filled map doesn't recognize Thai provinces | Power BI geocoder doesn't know Thai names | Use Bubble Map (Option A) with lat/lon instead |
| Alerts CSV has empty `alert_date` | Pipeline ran without writing date | Check `pipeline_meta.csv` `export_date` column; re-run pipeline |
| `days_to_full` shows blanks | Sentinel value (≥365) was stripped | Expected — blank means "no meaningful trend" |
| Reservoir table shows 0 rows after status slicer | `reservoir_status` is blank in some rows | Filter to non-blank: Filters pane → `reservoir_status` is not blank |
