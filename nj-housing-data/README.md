# NJ Housing Data Collection

**Download Date:** December 12, 2025

This directory contains housing production and cost data from multiple public sources for analyzing New Jersey housing trends and comparing with other regions.

## Directory Structure

```
nj-housing-data/
├── zillow/          # Zillow Research home values and rent indices
├── census-bps/      # US Census Building Permits Survey data
├── nj-dca/          # NJ DCA Construction Reporter data
├── hud/             # HUD SOCDS data (requires manual download)
└── README.md
```

---

## Successfully Downloaded Data

### 1. Zillow Research Data (`zillow/`) - 95MB total

| File | Size | Description |
|------|------|-------------|
| `Metro_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv` | 4.1MB | ZHVI (Home Value Index) - Metro level, all homes, smoothed seasonally adjusted |
| `City_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv` | 86MB | ZHVI - City level, all homes, smoothed seasonally adjusted |
| `Metro_zori_uc_sfrcondomfr_sm_sa_month.csv` | 921KB | ZORI (Rent Index) - Metro level, smoothed seasonally adjusted |
| `City_zori_uc_sfrcondomfr_sm_sa_month.csv` | 4.0MB | ZORI - City level, smoothed seasonally adjusted |

**Coverage:** Monthly data from 2000-present for ZHVI, 2015-present for ZORI
**Geographies included:** All US metros and cities, including:
- New Jersey metros (Newark, Trenton, Atlantic City)
- NYC metro area
- Hudson County cities (Jersey City, Hoboken, Bayonne, etc.)
- Comparison cities: Austin, Minneapolis, San Francisco, Los Angeles, San Jose

**Source:** https://www.zillow.com/research/data/

---

### 2. US Census Building Permits Survey (`census-bps/`) - 44MB total, 75 files

#### State-Level Annual Data (2010-2024)
Files: `st2010a.txt` through `st2024a.txt` (15 files, ~10KB each)

**Format:** Comma-delimited text with columns:
- Survey Date, FIPS State Code, Region/Division Codes, State Name
- Building permits by type: 1-unit, 2-units, 3-4 units, 5+ units
- For each type: Buildings, Units, Value
- Reported vs imputed breakdowns

**States included:** All 50 states + DC, PR, VI (includes NJ, NY, TX, MN, CA)

#### Place-Level Annual Data (2010-2024)
- **Northeast Region:** `ne2010a.txt` - `ne2024a.txt` (15 files, ~800KB each)
- **South Region:** `so2010a.txt` - `so2024a.txt` (15 files)
- **Midwest Region:** `mw2010a.txt` - `mw2024a.txt` (15 files, ~1.1MB each)
- **West Region:** `we2010a.txt` - `we2024a.txt` (15 files)

**Format:** Comma-delimited text with columns:
- Survey Date, State Code, 6-Digit ID, County Code, Place Code
- FIPS Place/MCD codes, Population, CSA/CBSA codes
- Building permits by type (same categories as state-level)

**Cities included:** All permit-issuing jurisdictions, including:
- NJ: Jersey City, Hoboken, Bayonne, Newark, and all municipalities
- NY: New York City boroughs, all municipalities
- TX: Austin, Houston, Dallas, San Antonio, etc.
- MN: Minneapolis, St. Paul, etc.
- CA: San Francisco, Los Angeles, San Jose, etc.

**Source:** https://www2.census.gov/econ/bps/

---

### 3. NJ DCA Construction Reporter (`nj-dca/`) - 9.8MB

| File | Size | Description |
|------|------|-------------|
| `Development_Trend_Viewer.xlsb` | 9.8MB | Excel Binary workbook with interactive data |

**Coverage:** Building permit and demolition activity for every NJ county and municipality from 2004 through most recent release

**Contents:**
- Interactive tables and graphs
- County-level summaries
- Municipal-level detail
- Residential and non-residential construction

**Source:** https://www.nj.gov/dca/codes/reporter/

---

## Requires Manual Download

### 4. HUD SOCDS Building Permits Database (`hud/`)

The HUD SOCDS database is **query-based** and requires interactive access.

**How to access:**
1. Visit https://socds.huduser.gov/permits/
2. Accept the terms and conditions
3. Use the Query Tool to select:
   - Geography: States & Counties or CBSA
   - States: NJ, NY, TX, MN, CA
   - Survey type: Annual
   - Years: 2010-2024
   - Permit types: All or specific (Single-Family, Multifamily, etc.)
4. Click "Report" to generate results
5. Click "CSV" button to download

**Recommended queries for your analysis:**
- NJ counties (all) - annual data 2010-2024
- Hudson County, NJ specifically
- NYC metro CBSA
- Austin-Round Rock CBSA
- Minneapolis-St. Paul CBSA
- San Francisco-Oakland-Berkeley CBSA
- Los Angeles-Long Beach-Anaheim CBSA

**Alternative:** HUD Open Data has county-level permits at:
https://hudgis-hud.opendata.arcgis.com/datasets/HUD::residential-construction-permits-by-county/

---

## Data Notes

### File Formats
- **Zillow:** Standard CSV with date columns (YYYY-MM-DD format)
- **Census BPS:** Comma-delimited text files with fixed-width header rows
- **NJ DCA:** Excel Binary Workbook (.xlsb) - requires Excel or compatible software

### Geographic Identifiers
- **FIPS codes:** Used in Census and HUD data for states (2-digit), counties (5-digit), places (7-digit)
- **CBSA codes:** Core-Based Statistical Area codes for metros
- **RegionID:** Zillow's internal geographic identifiers

### Key NJ FIPS Codes
- New Jersey: 34
- Hudson County: 34017
- Essex County (Newark): 34013
- Bergen County: 34003

### Comparison Geographies
| City | State FIPS | Notes |
|------|------------|-------|
| Jersey City | 34 | Hudson County |
| New York City | 36 | 5 boroughs |
| Austin | 48 | Travis County |
| Minneapolis | 27 | Hennepin County |
| San Francisco | 06 | San Francisco County |
| Los Angeles | 06 | Los Angeles County |
| San Jose | 06 | Santa Clara County |

---

## Total Download Size

| Source | Size |
|--------|------|
| Zillow | 95 MB |
| Census BPS | 44 MB |
| NJ DCA | 10 MB |
| **Total** | **~150 MB** |

---

## Contact/Sources

- **Zillow Research:** https://www.zillow.com/research/data/
- **Census Building Permits:** https://www.census.gov/construction/bps/
- **NJ DCA:** CodeAssist@dca.nj.gov, (609) 292-7899
- **HUD SOCDS:** https://socds.huduser.gov/permits/
