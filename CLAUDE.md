# Citi Bike ETL Pipeline

ETL pipeline for NYC Citi Bike trip data (2013-2025, 287M trips). Cleans, standardizes, and links historical station IDs to modern GBFS stations via a crosswalk.

## Status: Complete

- **NYC data**: 287M trips processed, 352 parquet files, 11 GB
- **JC/Hoboken data**: 6M trips processed, separate crosswalk
- **Weather/holidays**: Integrated, join at query time

## Directory Structure

```
citibike-pipeline/
├── CLAUDE.md              # THIS FILE
├── src/                   # ETL scripts
├── data/
│   ├── processed/         # NYC parquet output (287M trips)
│   ├── jc/processed/      # JC parquet output (6M trips)
│   ├── weather/           # Weather + holidays parquet
│   └── geo/processed/     # Geographic boundaries (GeoJSON)
├── reference/
│   ├── current_stations.csv      # GBFS stations (2,318)
│   ├── station_crosswalk.csv     # NYC legacy→modern mapping
│   ├── station_crosswalk_jc.csv  # JC legacy→modern mapping
│   └── *_crosswalk_2010_to_2020.csv  # NTA/PUMA/tract crosswalks
├── notebooks/             # Analysis notebooks
├── logs/                  # Processing logs
├── docs/                  # Detailed documentation (not auto-loaded)
│
├── mta/                   # MTA SUBPROJECT - see mta/CLAUDE.md
└── ferry/                 # FERRY SUBPROJECT - see ferry/CLAUDE.md
```

## Common Commands

```bash
# Query processed data
python3 -c "
import duckdb
con = duckdb.connect()
print(con.execute('''
    SELECT YEAR(started_at) as year, COUNT(*) as trips
    FROM \"data/processed/*.parquet\"
    GROUP BY 1 ORDER BY 1
''').fetchdf())
"

# Reprocess a year (use --force to overwrite)
python src/pipeline.py --year 2024

# Rebuild crosswalk from raw CSVs
python src/build_crosswalk.py

# Fetch fresh station data from GBFS API
python src/fetch_stations.py
```

## Key Conventions

### Schema Eras
- **Legacy (2013-2019)**: Integer station IDs (e.g., `519`)
- **Modern (2020-2025)**: Decimal station IDs (e.g., `5636.13`)
- All data uses crosswalk to resolve to canonical stations

### Data Filters Applied
- Duration: 90 seconds to 4 hours
- Required: start_station_id, end_station_id
- Test stations filtered (depot, mobile, lab, demo patterns)
- Date validation: parsed date must match filename month

### Output Columns
Key columns in parquet output:
- `started_at`, `ended_at` - timestamps (NYC local time)
- `start_station_id`, `end_station_id` - original IDs from raw data
- `start_station_name`, `end_station_name` - canonical names
- `start_lat`, `start_lon`, `end_lat`, `end_lon` - canonical coordinates
- `duration_sec` - trip duration in seconds
- `member_casual` - "member" or "casual"
- `rideable_type` - "classic_bike" or "electric_bike" (2020+ only)
- `start_match_type`, `end_match_type` - "crosswalk", "direct", or "ghost"

### Ghost Stations
Stations that no longer exist are preserved with original coordinates and tagged `match_type='ghost'`.

## Geographic Boundaries

Census boundaries for spatial analysis. Both 2010 and 2020 vintages available.

**Boundary Files** (`data/geo/processed/`):
- `boroughs.geojson` - 5 NYC boroughs (stable, use for any year)
- `nta_{2010,2020}_nyc.geojson` - Neighborhood Tabulation Areas (195→262)
- `puma_{2010,2020}_nyc.geojson` - Public Use Microdata Areas (55→55)
- `census_tracts_{2010,2020}_nyc.geojson` - Census tracts (2000 each)
- `*_hudson_nj.geojson` - Hudson County NJ for Jersey City analysis

**Which vintage to use**:
- 2013-2019 trips → Use 2010 boundaries
- 2020-2025 trips → Use 2020 boundaries
- Cross-vintage analysis → Use crosswalk tables in `reference/`

### Station Geography Lookup Table

`reference/station_geography.csv` - Pre-computed lookup linking all 5,990 stations to geographic boundaries. **Use this instead of spatial joins** for most analyses.

| Column | Description |
|--------|-------------|
| `station_id` | Station identifier (joins to trip data) |
| `state`, `city`, `borough_county` | Location hierarchy |
| `nta_2010_code/name`, `nta_2020_code/name` | Neighborhood (NYC only) |
| `puma_2010_code`, `puma_2020_code/name` | PUMA codes |
| `tract_2010`, `tract_2020` | Census tract GEOIDs |

**Usage** (faster than spatial joins):
```sql
SELECT g.borough_county, g.nta_2020_name, COUNT(*) as trips
FROM 'data/processed/*2024*.parquet' t
LEFT JOIN 'reference/station_geography.csv' g ON t.start_station_id = g.station_id
GROUP BY 1, 2 ORDER BY 3 DESC
```

**Station Timeline**: `reference/station_timeline.json` - First/last seen dates for 2,489 stations.

**Key Lesson**: Station IDs come in 3 formats (UUIDs, integers, decimals). Always verify ID formats match when joining reference tables.

**Details**: `docs/geo-boundaries-technical.md`

## Subprojects

### MTA Subway Data
- **Location**: `mta/`
- **Status**: Planned (documentation complete, scripts not implemented)
- **Purpose**: Comparative transit analysis
- **Details**: See `mta/CLAUDE.md`

### Ferry Data
- **Location**: `ferry/`
- **Status**: In progress
- **Purpose**: Multi-modal waterfront analysis
- **Details**: See `ferry/CLAUDE.md`

## Detailed Documentation

For technical deep-dives (NOT loaded automatically):
- `docs/pipeline-learnings.md` - Schema details, DuckDB fixes, data quality findings
- `docs/geo-boundaries-technical.md` - Geographic boundary sources, issues, crosswalk details
- `docs/session-history.md` - Development session logs

## Notebooks

| Notebook | Purpose |
|----------|---------|
| `explore_data.ipynb` | General data exploration |
| `station_geography_explorer.ipynb` | Station geography with interactive maps |
| `demographics_analysis.ipynb` | Age/gender analysis (legacy data) |
| `wfh_and_patterns.ipynb` | Work-from-home impact |
| `routes_and_corridors.ipynb` | Popular routes analysis |
| `jc_exploration.ipynb` | Jersey City data exploration |
| `cross_hudson_analysis.ipynb` | NYC ↔ JC cross-Hudson trips |

## Quick Reference

### Weather Join
```sql
FROM 'data/processed/*.parquet' t
LEFT JOIN 'data/weather/hourly_weather.parquet' w
    ON DATE_TRUNC('hour', t.started_at) = w.datetime
```

### JC Cross-Hudson Query
```sql
-- Trips from JC to NYC
SELECT * FROM 'data/jc/processed/*.parquet'
WHERE start_lon < -74.01 AND end_lon > -74.01

-- Trips from NYC to JC
SELECT * FROM 'data/processed/*.parquet'
WHERE start_lon > -74.01 AND end_lon < -74.01
  AND end_lat BETWEEN 40.68 AND 40.78
```

### Demographics Filter (Legacy 2013-2019 only)
```sql
WHERE birth_year_valid = TRUE AND gender_valid = TRUE
```
