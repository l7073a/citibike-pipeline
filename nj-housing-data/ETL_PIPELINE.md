# ETL Pipeline Documentation

## Overview

This document describes the Extract, Transform, Load pipeline for the NJ Housing Data analysis.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           DATA PIPELINE                                      │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   EXTRACT                    TRANSFORM                      LOAD            │
│   ───────                    ─────────                      ────            │
│                                                                             │
│   Zillow CSVs ──────────┐                                                   │
│   (ZHVI, ZORI)          │                                                   │
│                         │    ┌──────────────────┐     ┌──────────────────┐  │
│   Census BPS ───────────┼───>│ analyze_housing  │────>│ analysis/tables/ │  │
│   (State, Place .txt)   │    │     .py          │     │ analysis/charts/ │  │
│                         │    └──────────────────┘     └──────────────────┘  │
│   NJ DCA Excel ─────────┘                                                   │
│   (manual reference)                                                        │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 1. EXTRACT: Raw Data Sources

### 1.1 Zillow Research Data

| File | Source URL | Size | Format |
|------|------------|------|--------|
| `Metro_zhvi_*.csv` | files.zillowstatic.com/research/public_csvs/zhvi/ | 4.1 MB | CSV |
| `City_zhvi_*.csv` | files.zillowstatic.com/research/public_csvs/zhvi/ | 86 MB | CSV |
| `Metro_zori_*.csv` | files.zillowstatic.com/research/public_csvs/zori/ | 921 KB | CSV |
| `City_zori_*.csv` | files.zillowstatic.com/research/public_csvs/zori/ | 4.0 MB | CSV |

**Schema:**
```
RegionID,SizeRank,RegionName,RegionType,StateName,2000-01-31,2000-02-29,...
102001,0,United States,country,,122095.33,122310.14,...
394913,1,"New York, NY",msa,NY,219565.59,220498.62,...
```

**Notes:**
- ZHVI: Monthly home values from 2000-present
- ZORI: Monthly rents from 2015-present (shorter history)
- Values are in dollars (home values) or dollars/month (rent)

### 1.2 Census Building Permits Survey

| File Pattern | Source URL | Count | Format |
|--------------|------------|-------|--------|
| `st{YYYY}a.txt` | www2.census.gov/econ/bps/State/ | 15 files | Comma-delimited |
| `ne{YYYY}a.txt` | www2.census.gov/econ/bps/Place/Northeast%20Region/ | 15 files | Comma-delimited |
| `so{YYYY}a.txt` | www2.census.gov/econ/bps/Place/South%20Region/ | 15 files | Comma-delimited |
| `mw{YYYY}a.txt` | www2.census.gov/econ/bps/Place/Midwest%20Region/ | 15 files | Comma-delimited |
| `we{YYYY}a.txt` | www2.census.gov/econ/bps/Place/West%20Region/ | 15 files | Comma-delimited |

**State File Schema (st*.txt):**
```
Row 1: Survey,FIPS,Region,Division,State,,1-unit,,,2-units,,,3-4 units,,,5+ units,...
Row 2: Date,State,Code,Code,Name,Bldgs,Units,Value,Bldgs,Units,Value,...
Row 3: (blank)
Row 4+: Data rows

Example:
202499,34,1,2,New Jersey,14807,14807,6789543,254,508,89234,...
```

**Column Positions (0-indexed):**
- 0: Survey Date (YYYYMM or YYYY99 for annual)
- 1: FIPS State Code (2-digit)
- 4: State Name
- 6: 1-unit Units
- 9: 2-unit Units
- 12: 3-4 unit Units
- 15: 5+ unit Units

**Place File Schema (ne/so/mw/we*.txt):**
```
Row 1: Survey,State,6-Digit,County,Census Place,FIPS Place,...,Place,1-unit,...
Row 2: Date,Code,ID,Code,Code,Code,...,Name,Bldgs,Units,Value,...

Key columns:
- 1: State Code
- 2: 6-Digit ID (unique place identifier)
- 16: Place Name
- 18: 1-unit Units
- 21: 2-unit Units
- 24: 3-4 unit Units
- 27: 5+ unit Units
```

### 1.3 NJ DCA Construction Reporter

| File | Source URL | Size | Format |
|------|------------|------|--------|
| `Development_Trend_Viewer.xlsb` | nj.gov/dca/codes/reporter/ | 9.8 MB | Excel Binary |

**Notes:**
- Interactive Excel workbook with pivot tables
- Covers 2004-present for all NJ municipalities
- Not currently parsed by Python script (used as reference)

### 1.4 HUD SOCDS (Not Downloaded)

**Status:** Requires manual query via web interface
**URL:** https://socds.huduser.gov/permits/
**Alternative:** HUD Open Data county-level dataset

---

## 2. TRANSFORM: Processing Logic

### 2.1 Census State Data (`load_census_state_data()`)

```python
Input:  census-bps/st{2010-2024}a.txt (15 files)
Output: DataFrame with columns:
        [year, state_fips, state_name, units_1, units_2, units_3_4,
         units_5plus, total_units, single_family, small_multi, large_multi]

Logic:
1. Read each file, skip header rows (first 3 lines)
2. Parse comma-delimited fields
3. Extract unit counts from columns 6, 9, 12, 15
4. Calculate aggregates:
   - total_units = units_1 + units_2 + units_3_4 + units_5plus
   - single_family = units_1
   - small_multi = units_2 + units_3_4
   - large_multi = units_5plus
```

### 2.2 Census Place Data - NJ Cities (`load_census_place_data_nj()`)

```python
Input:  census-bps/ne{2010-2024}a.txt (15 files)
Output: DataFrame filtered to specific NJ cities

Filter criteria:
- State Code = '34' (NJ)
- 6-Digit ID in: {246000: Jersey City, 228000: Hoboken,
                   026000: Bayonne, 357000: Newark}

Logic:
1. Read Northeast region files
2. Filter to NJ (state_code == '34')
3. Match 6-digit IDs to city names
4. Extract permit counts from columns 18, 21, 24, 27
```

### 2.3 Census Place Data - Comparison Cities (`load_census_place_data_comparison()`)

```python
Input:  All regional place files
Output: DataFrame with Austin, Minneapolis, San Francisco,
        Los Angeles, New York City

Mapping:
- Austin: South region (so*.txt), state '48', name contains 'Austin'
- Minneapolis: Midwest (mw*.txt), state '27', name contains 'Minneapolis'
- San Francisco: West (we*.txt), state '06', name contains 'San Francisco'
- Los Angeles: West (we*.txt), state '06', name contains 'Los Angeles city'
- NYC: Northeast (ne*.txt), state '36', name contains 'New York city'
```

### 2.4 Zillow Data (`get_annual_zillow_values()`)

```python
Input:  Zillow CSV with monthly columns
Output: Annual averages by region

Logic:
1. Filter to specified RegionIDs
2. Identify date columns (format: YYYY-MM-DD)
3. Extract year from column names
4. Calculate annual mean for each region
```

### 2.5 Per Capita Calculations

```python
Population data (2020 Census, in thousands):
STATE_POP = {'NJ': 9289, 'NY': 20201, 'TX': 29145, 'MN': 5707, 'CA': 39538}

CITY_POP = {
    'Jersey City': 292449,
    'Hoboken': 60419,
    'Bayonne': 71852,
    'Newark': 311549,
    'Austin': 978908,
    'Minneapolis': 429954,
    ...
}

Formula: permits_per_1000 = total_units / population * 1000
```

---

## 3. LOAD: Output Files

### 3.1 Tables (CSV)

| Output File | Source | Description |
|-------------|--------|-------------|
| `nj_permits_2010_2024.csv` | Census State | NJ annual permits by type |
| `hudson_county_permits.csv` | Census Place NE | JC/Hoboken/Bayonne/Newark by year |
| `hudson_county_summary.csv` | Derived | Summary stats per city |
| `state_comparisons.csv` | Census State | NJ/NY/TX/MN/CA totals |
| `state_comparisons_per_capita.csv` | Derived | Per 1,000 residents |
| `city_comparisons.csv` | Census Place (all) | All comparison cities |
| `metro_home_values.csv` | Zillow Metro ZHVI | Annual avg home values |
| `metro_rents.csv` | Zillow Metro ZORI | Annual avg rents |
| `city_home_values.csv` | Zillow City ZHVI | City-level home values |
| `city_rents.csv` | Zillow City ZORI | City-level rents |
| `austin_supply_rent.csv` | Census + Zillow | Austin permits + rent merged |
| `minneapolis_2040_comparison.csv` | Census Place | Pre/post 2040 plan |

### 3.2 Charts (PNG)

| Output File | Contents |
|-------------|----------|
| `nj_permits_trend.png` | Stacked area + per capita line |
| `hudson_cities_comparison.png` | 4-panel comparison |
| `state_permits_per_capita.png` | 5-state trend lines |
| `rent_price_trends.png` | 4-panel ZHVI/ZORI |
| `austin_supply_vs_rent.png` | Dual-axis permits vs rent |
| `minneapolis_pre_post_2040.png` | Stacked area + bar comparison |

---

## 4. Data Quality Issues

### Known Issues

1. **Census Place Data Parsing:**
   - Some files have inconsistent column counts
   - Place names may contain commas (handled by position-based parsing)
   - Some jurisdictions report annually vs monthly (affects completeness)

2. **Zillow Coverage:**
   - Newark NJ metro doesn't exist separately (part of NYC metro)
   - ZORI data only from 2015 (vs ZHVI from 2000)
   - Some cities have missing months

3. **Population Data:**
   - Using static 2020 Census figures for all years
   - Per capita calculations don't account for population growth

4. **NJ DCA Data:**
   - .xlsb format requires Excel (not parsed in Python)
   - Could be incorporated with openpyxl or xlrd

### Validation Checks

```bash
# Verify file counts
ls census-bps/*.txt | wc -l  # Should be 75

# Check Zillow row counts
wc -l zillow/*.csv

# Verify NJ in state data
grep ",34," census-bps/st2024a.txt | head -1

# Verify NJ cities in place data
grep "Jersey City\|Hoboken\|Bayonne\|Newark" census-bps/ne2024a.txt | grep "^2024,34"
```

---

## 5. Reproducibility

### To Recreate from Scratch

```bash
# 1. Create directory
mkdir nj-housing-data && cd nj-housing-data

# 2. Download raw data
chmod +x scripts/download_data.sh
./scripts/download_data.sh

# 3. Run analysis
python3 analysis/analyze_housing.py

# 4. View outputs
ls analysis/tables/
ls analysis/charts/
cat analysis/findings.md
```

### Dependencies

```
Python 3.8+
pandas
numpy
matplotlib
seaborn
```

### File Manifest

```
nj-housing-data/
├── zillow/                    # Raw Zillow CSVs (4 files, ~95 MB)
├── census-bps/                # Raw Census text files (75 files, ~44 MB)
├── nj-dca/                    # Raw NJ DCA Excel (1 file, ~10 MB)
├── hud/                       # Manual download instructions
├── scripts/
│   └── download_data.sh       # Reproducible download script
├── analysis/
│   ├── analyze_housing.py     # Main ETL + analysis script
│   ├── tables/                # Output CSVs
│   ├── charts/                # Output PNGs
│   ├── data_inventory.md
│   ├── findings.md
│   └── full_report.md
├── ETL_PIPELINE.md            # This document
└── README.md                  # Project overview
```
