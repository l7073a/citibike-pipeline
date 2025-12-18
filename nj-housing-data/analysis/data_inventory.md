# Data Inventory

## Census Building Permits Survey (census-bps/)

### State-Level Data (st2010a.txt - st2024a.txt)
- **Format:** Comma-delimited text
- **Columns:** Survey Date, FIPS State Code, Region Code, Division Code, State Name,
  then for each structure type (1-unit, 2-unit, 3-4 unit, 5+ unit): Buildings, Units, Value
- **Geographic coverage:** All 50 states + DC, PR, VI
- **Time range:** Annual data, 2010-2024
- **Key states:** NJ (34), NY (36), TX (48), MN (27), CA (06)

### Place-Level Data (ne/so/mw/we + year + a.txt)
- **Format:** Comma-delimited text
- **Columns:** Survey Date, State Code, 6-Digit ID, County Code, Census Place Code,
  FIPS Place Code, FIPS MCD Code, Population, CSA Code, CBSA Code, Footnote Code,
  Central City, Zip Code, Region Code, Division Code, Months Reported, Place Name,
  then permit counts by structure type
- **Geographic coverage:** All permit-issuing jurisdictions by region
- **Time range:** Annual data, 2010-2024

### NJ City Identifiers:
- Jersey City: State 34, 6-Digit ID 246000, County 017 (Hudson)
- Hoboken: State 34, 6-Digit ID 228000, County 017 (Hudson)
- Bayonne: State 34, 6-Digit ID 026000, County 017 (Hudson)
- Newark: State 34, 6-Digit ID 357000, County 013 (Essex)

---

## Zillow Research Data (zillow/)

### ZHVI (Home Value Index)
- **Files:** Metro_zhvi_*.csv, City_zhvi_*.csv
- **Format:** CSV with RegionID, SizeRank, RegionName, RegionType, StateName,
  then monthly columns (YYYY-MM-DD format)
- **Time range:** 2000-01 to 2025-10 (monthly)
- **Metric:** Typical home value for middle-tier homes (35th-65th percentile)
- **Key RegionIDs:**
  - Metros: New York (394913), Los Angeles (753899), San Francisco (395057),
    Minneapolis (394865), Austin (394355), Trenton (395164)
  - Cities: Newark (12970), Jersey City (25320), Hoboken (25146), Bayonne (21685),
    Austin (10221), Minneapolis (5983), San Francisco (20330), Los Angeles (12447)

### ZORI (Rent Index)
- **Files:** Metro_zori_*.csv, City_zori_*.csv
- **Format:** Same structure as ZHVI
- **Time range:** 2015-01 to 2025-10 (monthly)
- **Metric:** Typical observed market rent (40th-60th percentile)

---

## NJ DCA Construction Reporter (nj-dca/)

### Development_Trend_Viewer.xlsb
- **Format:** Excel Binary Workbook (.xlsb)
- **Content:** Interactive pivot tables and charts
- **Geographic coverage:** All NJ counties and municipalities
- **Time range:** 2004-present
- **Metrics:** Building permits, demolitions, residential and non-residential

---

## Data Quality Notes

1. **Census BPS:**
   - Some jurisdictions report annually vs monthly (affects completeness)
   - "Reported" vs "imputed" values - imputed values are Census estimates
   - Population figures in place files may be outdated

2. **Zillow:**
   - ZORI only available from 2015 forward
   - Some cities/metros have missing months
   - NJ metros are limited (only Trenton separate; Newark/Hudson in NY metro)

3. **NJ DCA:**
   - Requires Excel to open .xlsb format
   - Most comprehensive source for NJ municipal-level data
