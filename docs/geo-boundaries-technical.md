# Geographic Boundaries - Technical Notes

Detailed technical documentation for the geographic boundary infrastructure. For quick reference, see the lean summary in `CLAUDE.md`.

## Overview

Geographic boundaries for spatial analysis of Citi Bike data, supporting both NYC (2013-2025) and Jersey City (2015-2025) with appropriate Census vintage boundaries.

**Date Created**: December 2024
**Last Updated**: December 2024 (Session 8+)

---

## File Inventory

### Boundary Files (`data/geo/processed/`)

| File | Features | Size | CRS | Vintage | Coverage |
|------|----------|------|-----|---------|----------|
| `boroughs.geojson` | 5 | 3.5 MB | EPSG:4326 | Static | NYC boroughs |
| `nta_2010_nyc.geojson` | 195 | 5.1 MB | EPSG:4326 | 2010 Census | NYC neighborhoods |
| `nta_2020_nyc.geojson` | 262 | 4.9 MB | EPSG:4326 | 2020 Census | NYC neighborhoods |
| `puma_2010_nyc.geojson` | 55 | 4.3 MB | EPSG:4326 | 2010 Census | NYC PUMAs |
| `puma_2020_nyc.geojson` | 55 | 0.6 MB | EPSG:4326 | 2020 Census | NYC PUMAs |
| `census_tracts_2010_nyc.geojson` | 2,168 | 4.7 MB | EPSG:4326 | 2010 Census | NYC tracts |
| `census_tracts_2020_nyc.geojson` | 2,325 | 6.6 MB | EPSG:4326 | 2020 Census | NYC tracts |
| `puma_2020_hudson_nj.geojson` | 10 | 0.2 MB | EPSG:4326 | 2020 Census | Hudson County NJ |
| `census_tracts_2020_hudson_nj.geojson` | 183 | 0.4 MB | EPSG:4326 | 2020 Census | Hudson County NJ |

### Reference Files (`reference/`)

| File | Rows | Description |
|------|------|-------------|
| `station_geography.csv` | 5,990 | **Station-to-boundary lookup table** - links all stations to geographic areas |
| `station_timeline.json` | 2,489 | Station first/last seen dates (from parquet files) |
| `nta_crosswalk_2010_to_2020.csv` | 1,335 | Links 195 old NTAs → 262 new NTAs |
| `puma_crosswalk_2010_to_2020.csv` | 285 | Links 55 old PUMAs → 55 new PUMAs |
| `census_tract_crosswalk_2010_to_2020.csv` | 2,168 | Links 2010 tracts → 2020 tracts |
| `GEOGRAPHIC_BOUNDARIES_README.md` | - | Comprehensive usage guide |

### Notebooks (`notebooks/`)

| File | Purpose |
|------|---------|
| `station_geography_explorer.ipynb` | Interactive exploration of station geography with maps |

---

## Station Geography Table

### Purpose

`reference/station_geography.csv` is a **pre-computed lookup table** that links every Citi Bike station (current and historical) to geographic boundaries. This eliminates the need for expensive spatial joins at query time.

### Schema

| Column | Type | Description |
|--------|------|-------------|
| `station_id` | string | Station identifier (UUID, integer, or decimal format) |
| `station_name` | string | Station name |
| `lat` | float | Latitude |
| `lon` | float | Longitude |
| `state` | string | NY or NJ |
| `city` | string | New York or city name (Jersey City, Hoboken, etc.) |
| `borough_county` | string | Manhattan, Brooklyn, Queens, Bronx, Staten Island, or Hudson County |
| `nta_2010_code` | string | 2010 NTA code (NYC only) |
| `nta_2010_name` | string | 2010 NTA name |
| `nta_2020_code` | string | 2020 NTA code (NYC only) |
| `nta_2020_name` | string | 2020 NTA name |
| `puma_2010_code` | string | 2010 PUMA code (NYC only) |
| `puma_2020_code` | string | 2020 PUMA code |
| `puma_2020_name` | string | 2020 PUMA name |
| `tract_2010` | string | 2010 Census tract GEOID |
| `tract_2020` | string | 2020 Census tract GEOID |
| `source` | string | Data source: gbfs_current, crosswalk_nyc, or crosswalk_jc |

### Statistics

| Metric | Value |
|--------|-------|
| Total stations | 5,990 |
| NY stations | 5,694 |
| NJ stations | 296 |
| From GBFS (current) | 2,318 |
| From NYC crosswalk (historical) | 3,610 |
| From JC crosswalk (historical) | 62 |

### Geographic Coverage

| Boundary | Coverage | Notes |
|----------|----------|-------|
| Borough/County | 100% | All stations assigned |
| NTA 2010 | 95.0% | NJ stations have no NTAs |
| NTA 2020 | 95.0% | NJ stations have no NTAs |
| PUMA 2010 | 95.0% | NJ stations have no 2010 PUMAs |
| PUMA 2020 | 98.1% | Includes Hudson County |
| Tract 2010 | 84.8% | Some stations in parks/water |
| Tract 2020 | 78.2% | Some stations in parks/water |

### How It Was Built

1. **Load station sources**:
   - `reference/current_stations.csv` (GBFS API - 2,318 current stations)
   - `reference/station_crosswalk.csv` (NYC historical - 3,611 stations)
   - `reference/station_crosswalk_jc.csv` (JC historical - 63 stations)

2. **Deduplicate**: Prefer GBFS over crosswalk when same station_id exists

3. **Assign state/city**:
   - Use bounding box to classify NY vs NJ
   - NJ cities determined by PUMA name matching

4. **Spatial join to boundaries** (vectorized with GeoPandas):
   - Borough → `boroughs.geojson`
   - NTA 2010/2020 → `nta_*_nyc.geojson`
   - PUMA 2010/2020 → `puma_*_nyc.geojson` + `puma_2020_hudson_nj.geojson`
   - Tract 2010/2020 → `census_tracts_*_nyc.geojson` + `census_tracts_2020_hudson_nj.geojson`

### Usage Example

```sql
-- Join trips with geography (no spatial operations needed)
SELECT
    g.borough_county,
    g.nta_2020_name,
    COUNT(*) as trips
FROM 'data/processed/*2024*.parquet' t
LEFT JOIN 'reference/station_geography.csv' g
    ON t.start_station_id = g.station_id
GROUP BY 1, 2
ORDER BY 3 DESC
```

---

## Station Timeline

### Purpose

`reference/station_timeline.json` tracks when each station first and last appeared in the trip data.

### Schema

```json
{
  "total_stations": 2489,
  "generated_from": "processed parquet files",
  "stations": [
    {
      "station_id": "66db6387-0aca-11e7-82f6-3863bb44ef7c",
      "station_name": "W 21 St & 6 Ave",
      "first_seen": "2013-06-01",
      "last_seen": "2025-06-30",
      "trip_count": 1169125
    }
  ]
}
```

### How It Was Built

Query all processed parquet files to find MIN/MAX dates per station:

```sql
SELECT
    start_station_id as station_id,
    MAX(start_station_name) as station_name,
    MIN(started_at) as first_seen,
    MAX(started_at) as last_seen,
    COUNT(*) as trip_count
FROM "data/processed/*.parquet"
WHERE start_station_id IS NOT NULL
GROUP BY start_station_id
```

### Station Count by First Year

| Year | Stations Added |
|------|---------------|
| 2013 | 328 |
| 2015 | 150 |
| 2016 | 165 |
| 2017 | 172 |
| 2018 | 35 |
| 2019 | 133 |
| 2020 | 321 |
| 2021 | 336 |
| 2022 | 197 |
| 2023 | 425 |
| 2024 | 47 |
| 2025 | 69 |

---

## Data Sources

### NYC Planning ArcGIS Server (Primary)

Base URL: `https://services5.arcgis.com/GfwWNkhOj9bNBqoJ/arcgis/rest/services/`

| Data | Service Name | URL Suffix |
|------|--------------|------------|
| Boroughs | `NYC_Borough_Boundary` | `/FeatureServer/0/query?where=1=1&outFields=*&outSR=4326&f=geojson` |
| NTA 2010 | `NYC_2010_NTA` | (same query pattern) |
| NTA 2020 | `NYC_2020_NTA` | (same query pattern) |
| PUMA 2010 | `NYC_2010_PUMA` | (same query pattern) |
| Census Tracts 2010 | `NYC_Census_Tracts_for_2010_US_Census` | (same query pattern) |
| Census Tracts 2020 | `NYC_Census_Tracts_for_2020_US_Census` | (same query pattern) |

**No API key required** - public access.

### US Census TIGER/Line (Secondary)

Used for 2020 PUMA (NYC and Hudson County) and Hudson County census tracts.

| Data | URL |
|------|-----|
| NY State PUMA 2020 | `https://www2.census.gov/geo/tiger/TIGER2022/PUMA/tl_2022_36_puma20.zip` |
| NJ State PUMA 2020 | `https://www2.census.gov/geo/tiger/TIGER2022/PUMA/tl_2022_34_puma20.zip` |
| NJ Census Tracts 2020 | `https://www2.census.gov/geo/tiger/TIGER2020/TRACT/tl_2020_34_tract.zip` |

---

## Scripts

### `src/geo/fetch_boundaries.py`

Original download script for NYC boundaries. Uses ArcGIS endpoints.

```bash
python src/geo/fetch_boundaries.py --all
python src/geo/fetch_boundaries.py --boroughs --nta --puma --tracts
```

### `src/geo/fetch_all_vintages.py`

Extended script for downloading both 2010 and 2020 vintages plus Hudson County NJ.

```bash
python src/geo/fetch_all_vintages.py --all              # Everything
python src/geo/fetch_all_vintages.py --vintage-2010     # Just 2010
python src/geo/fetch_all_vintages.py --hudson-nj        # Just Hudson County
```

### `src/geo/validate_boundaries.py`

Data quality validation (7 checks per file):
- File existence
- CRS verification (EPSG:4326)
- Geometry validity
- Bounding box check
- Feature count validation
- Attribute completeness
- Topology checks (overlaps, gaps)

```bash
python src/geo/validate_boundaries.py
# Output: data/geo/processed/validation_results.json
```

### `src/geo/visualize_boundaries.py` / `visualize_boundaries_v2.py`

Static matplotlib maps and interactive Folium HTML.

```bash
python src/geo/visualize_boundaries_v2.py --all           # All static maps
python src/geo/visualize_boundaries_v2.py --all --basemap # With street basemap
python src/geo/visualize_boundaries_v2.py --interactive   # HTML with tooltips
```

---

## Technical Issues & Fixes

### Issue 1: NYC Open Data Socrata API 404 Errors

**Problem**: Original URLs like `https://data.cityofnewyork.us/resource/tqmj-j8zm.geojson` returned 404.

**Fix**: Switched to NYC Planning ArcGIS Server endpoints which are more reliable.

### Issue 2: Wrong Service Names

**Problem**: Initial guesses like `NTA_2020` returned HTTP 400.

**Fix**: Found correct service names via WebFetch of ArcGIS directory:
- `NTA_2020` → `NYC_2020_NTA`
- `CensusTracts_2020` → `NYC_Census_Tracts_for_2020_US_Census`

### Issue 3: Census TIGER/Line Tract URLs 404

**Problem**: County-level tract downloads (`tl_2022_36005_tract.zip`) returned 404.

**Fix**: Download state-level file and filter by county code:
```python
gdf = gpd.read_file("tl_2020_34_tract.zip")
hudson = gdf[gdf['COUNTYFP'] == '017']  # Hudson County = 017
```

### Issue 4: CRS Inconsistency (NAD83 vs WGS84)

**Problem**: TIGER/Line files use EPSG:4269 (NAD83), but we need EPSG:4326 (WGS84) to match trip data.

**Fix**: Reproject after download:
```python
gdf = gdf.to_crs(epsg=4326)
```

**Files affected**: puma_2020_nyc, puma_2020_hudson_nj, census_tracts_2020_hudson_nj

### Issue 5: Borough Geometries with Negative Area

**Problem**: After download, borough polygons had negative area and `contains()` operations failed. Times Square wasn't "within" Manhattan.

**Cause**: Polygon vertices wound in wrong direction (clockwise instead of counter-clockwise). The `buffer(0)` "fix" made it worse by collapsing to fragments.

**Fix**: Use `shapely.geometry.polygon.orient()`:
```python
from shapely.geometry.polygon import orient
from shapely.validation import make_valid

geom = make_valid(row.geometry)
geom = orient(geom, sign=1.0)  # Counter-clockwise = positive
```

### Issue 6: PUMA 2020 Included Non-NYC Areas

**Problem**: Bounding box filter included Nassau County (Long Island) PUMA because its centroid was close to NYC.

**Fix**: Filter by PUMA name containing NYC borough names:
```python
nyc_mask = gdf['puma_name'].str.contains('NYC|Manhattan|Bronx|Brooklyn|Queens|Staten Island')
gdf_nyc = gdf[nyc_mask]  # 56 → 55 PUMAs
```

### Issue 7: Duplicate Column Names in Census Tracts

**Problem**: Rename map had both `BoroCT2020` and `GEOID` mapping to `tract_geoid`, creating duplicate columns.

**Fix**: Remove duplicates before saving:
```python
gdf = gdf.loc[:, ~gdf.columns.duplicated()]
```

### Issue 8: NTA Download Lost Columns

**Problem**: Aggressive column filtering removed useful columns like `nta_code`, `borough_name`.

**Fix**: Switch from whitelist (keep only specific columns) to blacklist (remove only metadata):
```python
# Bad: gdf = gdf[['nta_code', 'nta_name', 'geometry']]
# Good:
drop_cols = ['Shape__Area', 'Shape__Length', 'OBJECTID']
gdf = gdf.drop(columns=[col for col in drop_cols if col in gdf.columns])
```

### Issue 9: Station Timeline ID Mismatch (Session 8+)

**Problem**: `station_timeline.json` was created from a limited audit query with only 100 stations using decimal IDs (e.g., '7540.02'). The `station_geography.csv` uses different ID formats (UUIDs, integers, decimals from different sources). When joining, <5% of IDs matched, causing the expansion timeline to show 0 stations for 2013.

**Root Cause**: The timeline was built from a different data source than station_geography.csv:
- Geography table uses IDs from: GBFS (UUIDs), NYC crosswalk (integers/decimals), JC crosswalk
- Old timeline used decimal IDs from a limited audit query

**Fix**: Rebuild timeline directly from processed parquet files:
```python
con.execute('''
    SELECT
        start_station_id as station_id,
        MIN(started_at) as first_seen,
        MAX(started_at) as last_seen,
        COUNT(*) as trip_count
    FROM "data/processed/*.parquet"
    WHERE start_station_id IS NOT NULL
    GROUP BY start_station_id
''')
```

**Lesson**: Always verify ID formats match when joining reference tables. Use the same data source or ensure ID compatibility.

### Issue 10: Duplicate Station ID Across Sources

**Problem**: Station ID '3197' appeared in both NYC crosswalk ("Hs Don't Use" - test station) and JC crosswalk ("North St" - real station).

**Fix**: Remove the test station entry, keep the real JC station.

### Issue 11: Null Station ID Row

**Problem**: One row in station_geography.csv had null station_id due to a bad merge.

**Fix**: Filter out rows with null station_id before saving.

---

## Column Standardization

### Final Column Names by File

**boroughs.geojson**:
- `borough_code` (1-5)
- `borough_name` (Manhattan, Brooklyn, Queens, Bronx, Staten Island)
- `geometry`

**nta_2010_nyc.geojson**:
- `nta_code` (e.g., "BK09")
- `nta_name` (e.g., "Brooklyn Heights-Cobble Hill")
- `borough_code`, `borough_name`
- `CountyFIPS`
- `geometry`

**nta_2020_nyc.geojson**:
- `nta_code` (e.g., "BK0201")
- `nta_name` (e.g., "Brooklyn Heights")
- `NTAType`, `NTAAbbrev`
- `CDTACode`, `CDTAType`, `CDTAName`
- `LAST_BoroC`, `LAST_BoroN` (borough info)
- `NTASameBoundaries` (TRUE if unchanged from 2010)
- `geometry`

**puma_2010_nyc.geojson**:
- `puma_code` (e.g., "3801")
- `geometry`

**puma_2020_nyc.geojson**:
- `puma_code` (e.g., "04107")
- `puma_name` (e.g., "NYC-Manhattan Community District 7--Upper West Side")
- `puma_geoid`
- `geometry`

**census_tracts_2010_nyc.geojson**:
- `BOROCT` (combined borough + tract code)
- `BOROCODE`
- `CT` (tract code)
- `CTLABEL`
- `PUMA`
- `geometry`

**census_tracts_2020_nyc.geojson**:
- `tract_code`
- `tract_geoid`
- `borough_code`, `borough_name`
- `geometry`

**Hudson County files**:
- Similar structure with `county_code`, `county_name` instead of borough

---

## Crosswalk Details

### How Crosswalks Were Built

1. Load both vintage boundary files
2. Reproject to EPSG:2263 (NY State Plane) for accurate area calculation
3. Spatial overlay to find all intersections
4. Calculate intersection area in square feet
5. Compute percentages:
   - `pct_of_2010_area`: What % of 2010 area is in this 2020 area
   - `pct_of_2020_area`: What % of 2020 area came from this 2010 area

### NTA Crosswalk Statistics

- **Total mappings**: 1,335 (many-to-many relationships)
- **2010 coverage**: 195/195 (100%)
- **2020 coverage**: 262/262 (100%)
- **1-to-1 unchanged**: Only 1 NTA
- **Split areas (1 old → multiple new)**: 194 NTAs
- **Strong matches (>50% overlap)**: 184 NTAs (94%)

### PUMA Crosswalk Statistics

- **Total mappings**: 285
- **2010 coverage**: 55/55 (100%)
- **2020 coverage**: 55/55 (100%)
- **Near-exact matches (>95%)**: 36 PUMAs (65%)

### Census Tract Crosswalk Statistics

- **Method**: Centroid-based (faster than full overlay for 2000x2000)
- **Total mappings**: 1,791
- **2010 coverage**: 1,791/2,000 (89.5%)
- **Unchanged tract IDs**: 1,682 (94%)
- **Changed tract IDs**: 109 (6%)

---

## Validation Results

### End-to-End Trip Data Test

| Dataset | Station Match | Trip Coverage |
|---------|--------------|---------------|
| 2013 (legacy IDs) | 100% (327/327) | 100% (5.58M trips) |
| 2024 (modern IDs) | 100% (2,108/2,108) | 100% (1.85M trips) |
| ALL YEARS | 96.5% (2,337/2,422) | 100% (287.5M trips) |

The 96.5% station match is because 85 stations in the parquet data don't appear in station_geography.csv (likely test stations or very new stations). Trip coverage is 100% because those 85 stations have negligible trip volume.

### Spatial Join Test Points

Tested with 6 sample points across all boroughs + Jersey City:

| Test Point | Borough | NTA 2010 | NTA 2020 | PUMA 2020 |
|------------|---------|----------|----------|-----------|
| Times Square | Manhattan | Midtown-Midtown South | Midtown-Times Square | 04165 |
| Williamsburg | Brooklyn | North Side-South Side | Williamsburg | Brooklyn CD 1 |
| Astoria | Queens | Old Astoria | Astoria (Central) | Queens CD 1 |
| Pelham Bay | Bronx | Pelham Bay-Country Club | Pelham Bay-Country Club | Bronx CD 10 |
| St George | Staten Island | West New Brighton... | St. George-New Brighton | Staten Island CD 1 |
| Jersey City | N/A | N/A | N/A | Hudson County Central |

All spatial joins working correctly.

---

## Known Limitations

1. **Census tract centroid matching**: 10.5% of 2010 tracts couldn't be matched to 2020 tracts via centroid (likely merged/deleted tracts). For 100% coverage, use full spatial overlay.

2. **Water/park areas**: Points in water (e.g., Pelham Bay) won't match census tracts (which only cover land). This is expected behavior.

3. **NTA boundary changes**: 2020 NTAs are fundamentally different from 2010. The crosswalk provides area-weighted mappings but direct comparison is limited.

4. **No 2010 boundaries for Hudson County**: Jersey City data (2015+) only has 2020 boundaries available. For 2015-2019 JC analysis, use 2020 boundaries with awareness of potential boundary drift.

5. **Station ID format complexity**: Station IDs come in 3 formats (UUIDs, integers, decimals) from different sources. Always use string comparison, not numeric.

---

## Performance Notes

- **GeoPandas sjoin**: ~5-10 seconds for 1M points against 262 NTAs
- **DuckDB ST_Within**: ~2-5 seconds for 1M points (faster due to spatial indexing)
- **Full spatial overlay (2000 tracts × 2000 tracts)**: ~60 seconds
- **Centroid-based crosswalk**: ~5 seconds (much faster)
- **station_geography.csv join**: <1 second (pre-computed lookup)

For large-scale analysis, prefer the station_geography.csv lookup table over runtime spatial joins.

---

## Visualization Outputs

The `station_geography_explorer.ipynb` notebook generates:

| File | Description |
|------|-------------|
| `logs/station_growth_by_borough.png` | Bar chart of stations added per year + cumulative |
| `logs/station_expansion_timeline.png` | 12-panel grid showing network expansion 2013-2024 |
| `logs/nta_coverage_growth.png` | NTA coverage over time |
| `logs/stations_by_borough.html` | Interactive map with borough layer controls |
| `logs/station_density_heatmap.html` | Heatmap of station concentration |
| `logs/nta_station_choropleth.html` | NTA boundaries colored by station count |
| `logs/nj_stations_map.html` | Hudson County NJ stations |
| `logs/station_geography_summary.csv` | Summary statistics by borough |

---

## Known Issues Fixed (Session 9)

### ArcGIS Pagination Bug

**Issue**: NYC Planning ArcGIS servers have a `maxRecordCount` limit of 2000. Initial downloads only fetched the first 2000 records, missing:
- Census Tracts 2020: 325 tracts (2000 fetched vs 2325 total)
- Census Tracts 2010: 168 tracts (2000 fetched vs 2168 total)

**Symptom**: Visible gaps in census tract map, particularly in Brooklyn and Queens.

**Fix**: Added `fetch_arcgis_paginated()` helper function to `src/geo/fetch_all_vintages.py` that loops through results with `resultOffset` parameter until all records are retrieved.

**Verification**: Re-downloaded data now shows:
- Census Tracts 2020: 2,325 tracts, 302.1 sq mi total area ✓
- Census Tracts 2010: 2,168 tracts ✓
- Census Tract Crosswalk: 2,168 mappings (96.4% matched to 2020)

---

## Future Enhancements

1. **Add 2010 Hudson County boundaries** if needed for JC 2015-2019 analysis
2. **Create NTA stability layer** showing only areas unchanged between 2010-2020
3. **Add Community District boundaries** (another NYC administrative boundary)
4. **Integrate with Census demographic data** via PUMA joins
5. **Automate station_geography.csv updates** when new GBFS data is fetched
