# Claude Code Session Notes

This file documents learnings, decisions, and context from working on the Citi Bike ETL pipeline. Reference this when starting a new Claude Code session.

## Project Overview

This is an ETL pipeline for cleaning and standardizing NYC Citi Bike trip data from 2013 to present. The main challenge is that station IDs changed format over time, requiring a "crosswalk" to link legacy data to modern station information.

## Key Learnings

### 1. Schema Eras (Corrected)

The original documentation said the schema changed in Feb 2021, but testing revealed:

| Era | Years | Schema | Station ID Format |
|-----|-------|--------|-------------------|
| Legacy | 2013-2019 | `tripduration,starttime,stoptime,start station id...` | Integer (e.g., `519`) |
| Modern | 2020-2025 | `ride_id,rideable_type,started_at,ended_at,start_station_id...` | Numeric with decimals (e.g., `5636.13`) |

**Important**: The schema change happened at **2020**, not 2021 as originally documented.

### 2. Station ID Complexity

Even "modern" data (2020+) doesn't use the same UUIDs as the GBFS API:
- Trip data has IDs like `5636.13`, `8397.02`
- GBFS API returns UUIDs like `66dd056e-0aca-11e7-82f6-3863bb44ef7c`

This means **ALL trip data** (2013-2025) needs the crosswalk to resolve station IDs to current GBFS stations.

### 3. Datetime Format Variations (Important!)

There are **4 different datetime formats** across the dataset:

| Format | Files | Years | Example |
|--------|-------|-------|---------|
| `YYYY-MM-DD HH:MM:SS` | 49 | 2013-2017 | `2013-06-01 00:00:01` |
| `M/D/YYYY HH:MM:SS` | 31 | 2014-2016 | `11/1/2014 00:00:11` |
| `M/D/YYYY H:MM` | 4 | 2015 | `1/1/2015 0:01` |
| `YYYY-MM-DD HH:MM:SS.fff` | 208 | 2018-2025 | `2018-01-01 13:50:57.434` |

**Within the same year**, different months can have different formats! For example:
- Jan-Oct 2014: `YYYY-MM-DD HH:MM:SS`
- Nov-Dec 2014: `M/D/YYYY HH:MM:SS`

**CRITICAL**: DuckDB's `read_csv_auto` will auto-detect datetime formats and can get it wrong when dates are ambiguous (e.g., `9/1/2014` could be Sept 1 or Jan 9). See "DuckDB Best Practices" section below for the fix.

### 4. Other Data Quality Issues

- **2013**: Has literal `NULL` strings in coordinate columns (not null values)
- **2013**: Has quoted headers (`"tripduration"`) while later years don't
- **2017**: Some files have extra columns (16 vs 15) - use `ignore_errors=true`

Fixes applied:
- Use `TRY_CAST()` instead of `::TYPE` for timestamps and coordinates
- DuckDB's `ignore_errors=true` handles malformed rows

### 5. Zip File Structures by Year

| Years | Structure |
|-------|-----------|
| 2013-2019 | Full year bundled as one zip (e.g., `2014-citibike-tripdata.zip`) |
| 2020-2023 | Full year with nested monthly zips inside |
| 2024+ | Individual monthly zips (e.g., `202406-citibike-tripdata.zip`) |

The `ingest.py` script handles nested zips automatically.

### 6. Crosswalk Statistics

After scanning all years (2013-2025):
- 3,531 unique legacy station IDs found
- 3,391 matched to modern stations (96%)
- 140 "ghost stations" (closed/moved, no modern equivalent)

Match confidence levels:
- **High**: Distance < 50m AND name similarity > 80%
- **Medium**: Distance < 150m OR name similarity > 60%
- **Low**: Matched but with lower confidence

### 7. Ghost Station Handling (Historical Data Preservation)

**Ghost stations** are stations that existed historically but no longer exist in the current GBFS data. The pipeline **preserves these trips** with their original station names and coordinates.

How it works:
1. The crosswalk stores legacy station info (ID, name, lat, lon) even when no modern match exists
2. Trips are tagged with `start_match_type` / `end_match_type` = `'ghost'`
3. Original coordinates from the trip data are preserved
4. Station names come from the crosswalk's historical record

Example output for a ghost station trip:
```
Station ID: 519
Station Name: "Pershing Square North"
Coordinates: 40.751873, -73.977706
Match Type: ghost
```

This ensures the historical dataset remains complete and accurate - you can still analyze trips to/from stations that have since closed.

**Top Ghost Stations by Trip Volume:**
| Legacy ID | Name | Historical Trips |
|-----------|------|------------------|
| 519 | Pershing Square North | 1,075,680 |
| 518 | E 39 St & 2 Ave | 429,419 |
| 517 | Pershing Square South | 419,741 |
| 345 | W 13 St & 6 Ave | 304,290 |
| 377 | 6 Ave & Canal St | 277,504 |

### 8. Processing Match Rates by Era

| Year | Match Rate | Notes |
|------|------------|-------|
| 2013 | 93.5% | Oldest data, some stations closed |
| 2017 | 95.3% | Legacy schema |
| 2019 | 96.6% | Last legacy year |
| 2020 | 97.1% | First modern schema year |
| 2023 | 98.7% | Modern data |
| 2025 | 99.3% | Most recent, best alignment |

### 9. Data Quality Filters

The pipeline applies these filters to remove erroneous trips:

| Filter | Threshold | Rationale |
|--------|-----------|-----------|
| Missing start station ID | Required | Cannot analyze origin |
| Missing end station ID | Required | Cannot analyze destination |
| Duration too short | >= 90 seconds | Likely errors or false starts |
| Duration too long | <= 4 hours | Likely lost/stolen bikes or errors |
| Invalid timestamps | Must parse | Data integrity |

**Filter Impact by Year (sample):**
| Year | Total Rows | Filtered | % Filtered |
|------|------------|----------|------------|
| 2013 | 5,614,888 | ~30,749 | 0.55% |
| 2014 | 8,081,216 | ~40,000 | 0.50% |
| 2024 | 4,783,277 | ~84,606 | 1.77% |

Modern data (2020+) has slightly higher filter rates due to e-bikes sometimes being returned without proper station assignment.

### 10. Coordinate Handling Strategy

**Problem**: Coordinates in the raw data can vary slightly for the same station, and some are missing or invalid.

**Solution**: The pipeline uses a **canonical coordinate** approach with fallbacks:

1. **For matched stations (direct or crosswalk)**:
   - Use coordinates from `current_stations.csv` (GBFS API source)
   - These are the official, current coordinates

2. **For ghost stations**:
   - Use coordinates from `station_crosswalk.csv` (historical record)
   - Preserves original location data for closed stations

3. **For unmatched stations**:
   - Use raw coordinates from trip data as-is
   - Tagged with `match_type='unmatched'` for identification

**Priority Order** (implemented via COALESCE):
```
start_lat = COALESCE(
    current_stations.lat,      -- If matched to modern station
    crosswalk.legacy_lat,      -- If in crosswalk (even as ghost)
    raw_trip_data.start_lat    -- Fallback to raw data
)
```

**Coordinate Quality Stats (2024 sample)**:
- Invalid coordinates (outside NYC bbox): 19 trips (0.0004%)
- Missing start coordinates: 1,755 trips (0.04%)
- Missing end coordinates: 13,663 trips (0.29%)

### 11. Duplicate File Handling

**Issue Found**: The 2013 zip file contained duplicate data in two formats:
- Original files (quoted headers): `201306-citibike-tripdata.csv`
- Split files (unquoted headers): `201306-citibike-tripdata_1.csv`, `_2.csv`

These were the same trips in different formats, causing ~5M duplicate rows.

**Solution**: `src/cleanup_duplicates.py` removes redundant split files, keeping only the original complete files.

**Prevention**: Run `cleanup_duplicates.py --dry-run` after ingestion to check for duplicates before processing.

### 12. Audit System

The `src/audit.py` script provides comprehensive data quality analysis:

```bash
# Audit a specific year
python src/audit.py --year 2024

# Audit all years
python src/audit.py --all

# Generate station timeline (first/last appearance)
python src/audit.py --station-timeline
```

**What it tracks**:
- Filtered rows by reason (missing station, duration, timestamps)
- Coordinate quality and variations
- Anomalies (future dates, pre-launch dates, round trips)
- Station appearances over time

All audit results are saved to `logs/audit_*.json` for reproducibility.

### 13. Station Mapping Validation

The pipeline automatically validates station mappings after processing by comparing raw coordinates to canonical coordinates.

**Logic**: If a mapping is wrong, the distance discrepancy should be consistent across ALL trips for that legacy station ID. If only a few trips are off, it's likely bad raw data, not a bad mapping.

**Metrics tracked per station**:
- `median_distance_m`: Median distance between raw and canonical coords
- `pct_over_threshold`: Percentage of trips where distance > 200m

**Classification**:
- **Suspicious mapping**: High median distance (>200m) → likely wrong mapping, affects all trips
- **Bad raw data**: Low median but some outliers → mapping is correct, just a few bad data points
- **Good mapping**: Everything within tolerance

Run standalone: `python src/validate_mappings.py --year 2014`

### 14. DuckDB Best Practices

**Problem: Ambiguous date formats**

DuckDB's `read_csv_auto` samples the first ~20K rows to detect types. For dates like `9/1/2014`, it might guess `%d/%m/%Y` (European) instead of `%m/%d/%Y` (US). This causes:
- Silent data corruption (wrong dates)
- Errors when day > 12 (e.g., `9/13/2014` fails to parse as day 13, month 9)

**Solution: Force timestamp columns to VARCHAR, then parse explicitly**

```python
# BAD - DuckDB may mis-detect the format
read_csv_auto('file.csv', ignore_errors=true)

# GOOD - Force VARCHAR, parse with explicit formats
read_csv_auto('file.csv',
    ignore_errors=true,
    types={'starttime': 'VARCHAR', 'stoptime': 'VARCHAR'}
)
```

Then in SQL, try multiple formats with COALESCE:
```sql
COALESCE(
    TRY_STRPTIME(CAST(starttime AS VARCHAR), '%Y-%m-%d %H:%M:%S'),
    TRY_STRPTIME(CAST(starttime AS VARCHAR), '%Y-%m-%d %H:%M:%S.%g'),
    TRY_STRPTIME(CAST(starttime AS VARCHAR), '%m/%d/%Y %H:%M:%S'),
    TRY_STRPTIME(CAST(starttime AS VARCHAR), '%m/%d/%Y %H:%M')
) as started_at
```

**Other DuckDB tips**:
- `TRY_CAST()` returns NULL on failure instead of erroring
- `TRY_STRPTIME()` returns NULL if format doesn't match
- `ignore_errors=true` skips malformed rows entirely
- Use `DESCRIBE SELECT * FROM read_csv_auto('file.csv')` to see inferred types
- Cast to VARCHAR first when column type is uncertain: `CAST(col AS VARCHAR)`

## File Locations

```
citibike-pipeline/
├── src/
│   ├── fetch_stations.py    # Get current stations from GBFS API
│   ├── download.py          # Download trip data from S3
│   ├── ingest.py            # Extract zips, handle nesting
│   ├── build_crosswalk.py   # Build legacy→modern station mapping
│   ├── pipeline.py          # Main ETL transformation
│   ├── cleanup_duplicates.py # Remove redundant duplicate files
│   ├── audit.py             # Data quality analysis and auditing
│   └── validate_mappings.py # Station mapping validation
├── reference/
│   ├── current_stations.csv  # 2,318 stations from GBFS
│   ├── station_crosswalk.csv # Legacy ID → Modern UUID mapping
│   └── station_timeline.json # Station first/last appearance dates
├── data/
│   ├── raw_zips/            # Downloaded zip files (not in git)
│   ├── raw_csvs/            # Extracted CSVs (not in git)
│   └── processed/           # Output parquet files (not in git)
└── logs/                    # Processing logs (JSON)
    ├── pipeline_run_*.json  # Processing stats per file
    ├── audit_*.json         # Data quality audit results
    └── cleanup_*.json       # Duplicate file removal logs
```

## Common Commands

```bash
# Full workflow
python src/fetch_stations.py              # Get current station data
python src/download.py --year 2024        # Download a year
python src/ingest.py                      # Extract all zips
python src/build_crosswalk.py             # Build/rebuild crosswalk
python src/pipeline.py --year 2024        # Process a year

# Test a single file
python src/pipeline.py --year 2024 --limit 1

# Query processed data
python3 -c "
import duckdb
con = duckdb.connect()
con.execute('''
    SELECT YEAR(started_at) as year, COUNT(*) as trips
    FROM \"data/processed/*.parquet\"
    GROUP BY 1 ORDER BY 1
''').fetchall()
"
```

## Future Work

1. **Full processing**: Run pipeline on all 373 CSV files (~150M+ trips)
2. **Manual overrides**: Add `reference/manual_overrides.csv` for known mis-matches
3. **Incremental updates**: Add logic to skip already-processed files

## Session History

### Dec 9, 2024 - Session 1
- Set up GitHub repo
- Downloaded data from all years (2013-2025)
- Discovered schema change was at 2020 not 2021
- Fixed pipeline bugs (TRY_CAST, timestamp formats, NULL strings)
- Built crosswalk with 96% match rate
- Tested pipeline on representative years from each era

### Dec 9, 2024 - Session 2 (continued)
- Surveyed datetime formats across all 293 CSV files
- Found 4 distinct datetime formats, varying within years
- Documented ghost station handling in detail
- Verified historical trips are preserved (not deleted)
- Updated CLAUDE_NOTES.md with comprehensive learnings

### Dec 9, 2024 - Session 3 (continued)
- Added data quality filters (90s-4h duration, required station IDs)
- Discovered 2013 duplicate files issue (~5M duplicate rows)
- Created `cleanup_duplicates.py` to remove redundant split files
- Created `audit.py` for comprehensive data quality analysis
- Added station timeline tracking (first/last appearance)
- Documented coordinate handling strategy (canonical coordinates)
- All filter stats now logged for auditability
- Current CSV count after cleanup: 373 files

### Dec 9, 2024 - Session 4 (continued)
- Downloaded remaining 2024 and 2025 months (was only June from each)
- Added `bike_id`, `birth_year`, `gender`, `rideable_type` columns to preserve all data
- Legacy data has demographics (birth_year, gender, bike_id); modern has rideable_type
- NULL used for columns not present in a given schema era
- Created `validate_mappings.py` for station mapping validation
- Integrated validation into pipeline (runs automatically after processing)
- Fixed critical DuckDB datetime parsing bug:
  - `read_csv_auto` was mis-detecting `M/D/YYYY` as `D/M/YYYY` for dates where day ≤ 12
  - Caused 60% of Sep-Dec 2014 rows to be silently corrupted or filtered
  - Fix: Force timestamp columns to VARCHAR, parse explicitly with TRY_STRPTIME
- Successfully processed 2013 and 2014 with ~0.5% filter rate, 94% station match rate
- Renamed CLAUDE_NOTES.md to CLAUDE.md (best practice)
