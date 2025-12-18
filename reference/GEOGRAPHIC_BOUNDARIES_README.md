# Geographic Boundaries for Citi Bike Analysis

Complete reference for using 2010 and 2020 vintage boundaries with the Citi Bike trip dataset (2013-2025).

## Quick Start

### For Time Series Analysis (2013-2025)

```python
import geopandas as gpd
import duckdb

# Load appropriate vintage for your time period
nta_2010 = gpd.read_file("data/geo/processed/nta_2010_nyc.geojson")  # For 2013-2019
nta_2020 = gpd.read_file("data/geo/processed/nta_2020_nyc.geojson")  # For 2020-2025

# Load crosswalk to link vintages
import pandas as pd
nta_xwalk = pd.read_csv("reference/nta_crosswalk_2010_to_2020.csv")
```

### For Jersey City Data

```python
# Hudson County NJ boundaries
puma_hudson = gpd.read_file("data/geo/processed/puma_2020_hudson_nj.geojson")
tracts_hudson = gpd.read_file("data/geo/processed/census_tracts_2020_hudson_nj.geojson")
```

---

## File Inventory

### NYC 2010 Vintage (for 2013-2019 Analysis)

| File | Features | Size | Usage |
|------|----------|------|-------|
| `nta_2010_nyc.geojson` | 195 | 5.1 MB | Neighborhood analysis |
| `puma_2010_nyc.geojson` | 55 | 4.3 MB | Census PUMS data joins (ACS 2012-2021) |
| `census_tracts_2010_nyc.geojson` | 2,168 | 4.7 MB | Fine-grained demographic analysis |

### NYC 2020 Vintage (for 2020-2025 Analysis)

| File | Features | Size | Usage |
|------|----------|------|-------|
| `nta_2020_nyc.geojson` | 262 | 4.9 MB | Neighborhood analysis |
| `puma_2020_nyc.geojson` | 66 | 0.6 MB | Census PUMS data joins (ACS 2022+) |
| `census_tracts_2020_nyc.geojson` | 2,325 | 6.6 MB | Fine-grained demographic analysis |

### Hudson County NJ 2020

| File | Features | Size | Coverage |
|------|----------|------|----------|
| `puma_2020_hudson_nj.geojson` | 10 | 0.2 MB | Jersey City, Hoboken, Union City, etc. |
| `census_tracts_2020_hudson_nj.geojson` | 183 | 0.4 MB | All Hudson County census tracts |

### Crosswalk Tables

| File | Mappings | Description |
|------|----------|-------------|
| `nta_crosswalk_2010_to_2020.csv` | 1,335 | Links 195 old NTAs to 262 new NTAs |
| `puma_crosswalk_2010_to_2020.csv` | 295 | Links 55 old PUMAs to 66 new PUMAs |
| `census_tract_crosswalk_2010_to_2020.csv` | 1,791 | Links 2010 tracts to 2020 tracts |

---

## Understanding Boundary Changes

### Why Boundaries Change

Census boundaries are redrawn every 10 years to:
- Maintain target population counts per area (e.g., ~4,000 per census tract)
- Reflect population shifts and growth patterns
- Align with updated street networks and administrative changes

### Key Changes: 2010 → 2020

| Boundary Type | 2010 Count | 2020 Count | Change | Impact |
|--------------|------------|------------|--------|---------|
| **NTA** | 195 | 262 | +67 (+34%) | Significant reconfiguration |
| **PUMA** | 55 | 66 | +11 (+20%) | Moderate expansion |
| **Census Tracts** | 2,168 | 2,325 | +157 (+7%) | Population growth, tract splits |

---

## Crosswalk Usage Guide

### Understanding Crosswalk Columns

#### NTA Crosswalk (`nta_crosswalk_2010_to_2020.csv`)

| Column | Description |
|--------|-------------|
| `nta_2010_code` | 2010 NTA code (e.g., "BK09") |
| `nta_2010_name` | 2010 NTA name |
| `nta_2020_code` | 2020 NTA code (e.g., "BK0201") |
| `nta_2020_name` | 2020 NTA name |
| `intersection_area_sqft` | Overlapping area in square feet |
| `pct_of_2010_area` | What % of the 2010 NTA is in this 2020 NTA |
| `pct_of_2020_area` | What % of the 2020 NTA came from this 2010 NTA |
| `borough` | Borough name |

**Key Insight**: Most 2010 NTAs split into multiple 2020 NTAs. Only 1 out of 195 remained completely unchanged.

#### Example: NTA Split Pattern

```
2010 NTA: BK19 (Brighton Beach)
  → BK1303 (Brighton Beach)         78.9% of old area
  → BK1301 (Gravesend South)        21.0% of old area
  → BK1302 (Coney Island-Sea Gate)   0.1% of old area
```

### Mapping Strategies

#### Strategy 1: Use Dominant Match

For each 2010 area, use the 2020 area with the largest overlap:

```python
import pandas as pd

nta_xwalk = pd.read_csv("reference/nta_crosswalk_2010_to_2020.csv")

# Get primary mapping for each 2010 NTA
primary_mapping = nta_xwalk.loc[
    nta_xwalk.groupby('nta_2010_code')['pct_of_2010_area'].idxmax()
]

# Result: 195 rows, one per 2010 NTA
# 94% have >50% overlap with their primary 2020 match
```

#### Strategy 2: Multi-Way Mapping

For accurate area-weighted aggregation:

```python
# Calculate ridership by 2010 NTA, then distribute to 2020 NTAs
trips_2010_nta = pd.DataFrame({
    'nta_2010_code': ['BK19'],
    'total_trips': [100000]
})

# Merge with crosswalk
merged = trips_2010_nta.merge(nta_xwalk, on='nta_2010_code')

# Distribute trips by area percentage
merged['trips_distributed'] = merged['total_trips'] * merged['pct_of_2010_area'] / 100

# Aggregate to 2020 NTAs
trips_2020_nta = merged.groupby('nta_2020_code')['trips_distributed'].sum()
```

#### Strategy 3: Filter to Stable Areas

For continuous time series, use only areas with minimal changes:

```python
# Find 2010→2020 mappings with >95% overlap
stable_ntas = nta_xwalk[nta_xwalk['pct_of_2010_area'] > 95]

# These 94 NTAs had minimal boundary changes
# Use these for time series 2013-2025
```

---

## PUMA Crosswalk Details

**Findings:**
- 26 out of 55 PUMAs (47%) have >99% exact match to 2020 equivalents
- Most stable of the three boundary types
- 4 new 2020 PUMAs have no 2010 predecessor (created for new development/population growth)

**Usage:**

```python
puma_xwalk = pd.read_csv("reference/puma_crosswalk_2010_to_2020.csv")

# Find near-exact matches
exact_matches = puma_xwalk[puma_xwalk['pct_of_2010_area'] > 99]
# 26 PUMAs - use these for continuous time series

# Example: PUMA 4014 → 4312 (100% match, Borough Park)
```

---

## Census Tract Crosswalk Details

**Findings:**
- 1,682 out of 1,791 (94%) kept the same tract ID between 2010 and 2020
- 109 tracts (6%) changed IDs due to splits, merges, or renumbering
- 209 tracts (10.5%) could not be matched (likely deleted or merged into adjacent tracts)

**Usage:**

```python
tract_xwalk = pd.read_csv("reference/census_tract_crosswalk_2010_to_2020.csv")

# Filter to unchanged tracts
unchanged = tract_xwalk[tract_xwalk['tract_2010_code'] == tract_xwalk['tract_2020_code']]
# 1,682 tracts - safest for time series

# Example of unchanged:
# 1010200 → 1010200 (Manhattan tract - no change)

# Example of changed:
# 3010400 → 3010402 (Brooklyn tract - split or renumbered)
```

---

## Recommended Workflow

### For Neighborhood-Level Analysis

```python
import geopandas as gpd
import duckdb

con = duckdb.connect()

# Spatial join 2013-2019 trips to 2010 NTAs
con.execute("""
    SELECT
        t.started_at,
        n.nta_code,
        n.nta_name,
        COUNT(*) as trips
    FROM 'data/processed/*201[3-9]*.parquet' t
    JOIN ST_Read('data/geo/processed/nta_2010_nyc.geojson') n
        ON ST_Within(ST_Point(t.start_lon, t.start_lat), n.geometry)
    WHERE YEAR(t.started_at) BETWEEN 2013 AND 2019
    GROUP BY 1, 2, 3
""")

# Spatial join 2020-2025 trips to 2020 NTAs
con.execute("""
    SELECT
        t.started_at,
        n.nta_code,
        n.nta_name,
        COUNT(*) as trips
    FROM 'data/processed/*202[0-5]*.parquet' t
    JOIN ST_Read('data/geo/processed/nta_2020_nyc.geojson') n
        ON ST_Within(ST_Point(t.start_lon, t.start_lat), n.geometry)
    WHERE YEAR(t.started_at) BETWEEN 2020 AND 2025
    GROUP BY 1, 2, 3
""")

# Link the two periods using crosswalk
```

### For Continuous Time Series (All Years)

**Option A: Use borough boundaries** (most stable)

```python
# Boroughs haven't changed - safe for 2013-2025 analysis
boroughs = gpd.read_file("data/geo/processed/boroughs.geojson")
```

**Option B: Filter to stable NTA/PUMA areas**

```python
# Use only areas with >95% boundary overlap
nta_xwalk = pd.read_csv("reference/nta_crosswalk_2010_to_2020.csv")
stable_2010_codes = nta_xwalk[nta_xwalk['pct_of_2010_area'] > 95]['nta_2010_code'].unique()
stable_2020_codes = nta_xwalk[nta_xwalk['pct_of_2010_area'] > 95]['nta_2020_code'].unique()

# Filter your analysis to these codes
```

---

## Jersey City / Hudson County Usage

```python
import geopandas as gpd

# Load Hudson County boundaries
puma_hudson = gpd.read_file("data/geo/processed/puma_2020_hudson_nj.geojson")
tracts_hudson = gpd.read_file("data/geo/processed/census_tracts_2020_hudson_nj.geojson")

# Spatial join Jersey City trips
import duckdb
con = duckdb.connect()

con.execute("""
    SELECT
        t.*,
        tr.tract_geoid,
        tr.tract_name
    FROM 'data/jc/processed/*.parquet' t
    JOIN ST_Read('data/geo/processed/census_tracts_2020_hudson_nj.geojson') tr
        ON ST_Within(ST_Point(t.start_lon, t.start_lat), tr.geometry)
""")
```

---

## Common Questions

### Q: Which vintage should I use for my analysis?

**A:** Match the vintage to your data period:
- **2013-2019 data** → Use 2010 boundaries
- **2020-2025 data** → Use 2020 boundaries
- **Entire period** → Use crosswalks to link, or use borough-level only

### Q: Can I analyze 2013-2025 as a continuous time series?

**A:** Yes, but with caveats:
1. **Best approach**: Use borough boundaries (no changes)
2. **Good approach**: Filter to stable NTA/PUMA areas (94 NTAs with >95% overlap)
3. **Advanced approach**: Use crosswalks to proportionally redistribute 2010 areas to 2020 areas

### Q: Why do NTAs have more dramatic changes than PUMAs or census tracts?

**A:** NTAs were completely redesigned in 2020 to:
- Better reflect neighborhood identities
- Align with updated census tracts
- Incorporate community feedback

PUMAs and census tracts follow stricter Census Bureau rules with more grandfathering.

### Q: What about the 209 census tracts that couldn't be matched?

**A:** These tracts likely:
- Merged with adjacent tracts (population declined below threshold)
- Were renumbered without a clear spatial match
- Had centroids fall outside 2020 tract boundaries due to shape changes

For most analyses, 89.5% coverage is sufficient. If you need 100%, use full spatial overlay instead of centroid matching (slower but complete).

---

## Data Sources

- **NYC 2010 NTA**: NYC Planning ArcGIS Server (NYC_2010_NTA)
- **NYC 2020 NTA**: NYC Planning ArcGIS Server (NYC_2020_NTA)
- **NYC 2010 PUMA**: NYC Planning ArcGIS Server (NYC_2010_PUMA)
- **NYC 2020 PUMA**: US Census TIGER/Line 2022
- **NYC 2010 Census Tracts**: NYC Planning ArcGIS Server (NYC_Census_Tracts_for_2010_US_Census)
- **NYC 2020 Census Tracts**: NYC Planning ArcGIS Server (NYC_Census_Tracts_for_2020_US_Census)
- **Hudson County NJ PUMA**: US Census TIGER/Line 2022 (State 34)
- **Hudson County NJ Census Tracts**: US Census TIGER/Line 2020 (County 34017)

---

## Validation Summary

✓ All 11 files present and validated
✓ Feature counts match expectations (195→262 NTA, 55→66 PUMA, 2168→2325 tracts)
✓ Geographic area coverage: 302.2 sq mi (2010) vs 302.3 sq mi (2020) - 0.01% difference
✓ Crosswalk coverage: 100% NTA, 100% PUMA, 89.5% census tracts
✓ Strong mappings: 94% of NTAs have >50% overlap with primary 2020 match

**Date Created**: 2025-12-10
**Pipeline Version**: Session 8 (Geographic Boundaries Extension)
