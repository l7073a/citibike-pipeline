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

### 15. Pipeline Architecture Improvements (Session 4 Additions)

Four key improvements were made to the pipeline for better data quality and validation:

**1. Date Sanity Check**

Problem: Even after fixing datetime parsing, there's no guarantee the parsed date is correct. A corrupt row could parse as a valid date from the wrong month/year.

Solution: Extract expected year/month from filename (e.g., `201409-citibike-tripdata.csv` → Sept 2014), then filter out any trips where the parsed date doesn't match.

```python
def extract_expected_month(filename: str) -> Tuple[Optional[int], Optional[int]]:
    """Extract expected year and month from filename."""
    match = re.search(r'(\d{4})(\d{2})-citibike', filename)
    if match:
        return int(match.group(1)), int(match.group(2))
    return None, None
```

In the WHERE clause:
```sql
AND EXTRACT(YEAR FROM started_at) = {expected_year}
AND EXTRACT(MONTH FROM started_at) = {expected_month}
```

**2. Raw Coordinate Preservation**

Problem: The pipeline was only outputting canonical coordinates (from GBFS or crosswalk), losing the original raw data for validation purposes.

Solution: Added `start_lat_raw`, `start_lon_raw`, `end_lat_raw`, `end_lon_raw` columns to preserve original trip coordinates alongside canonical ones.

Why this matters:
- Enables validation of station mappings (compare raw to canonical)
- Preserves data provenance
- Allows detection of GPS drift or systematic coordinate errors

**3. Single-Pass Filter Statistics**

Problem: The pipeline was reading each CSV file twice - once to count filtered rows, once to process. This doubled I/O overhead.

Solution: Combined filter stats into the main query using conditional aggregation:

```sql
SELECT
    COUNT(*) as total_rows,
    SUM(CASE WHEN <filter_conditions> THEN 1 ELSE 0 END) as filtered_count,
    filtered_data.*
FROM ...
```

This provides the same stats with half the file reads.

**4. Standardized Coordinate Naming**

Problem: Inconsistent naming - some files used `lng`, others `lon` for longitude columns. This caused confusion and potential bugs.

Solution: Standardized on `lon` everywhere:
- `start_lon` / `end_lon` (canonical coordinates)
- `start_lon_raw` / `end_lon_raw` (raw coordinates)
- Updated `validate_mappings.py` to match

`lon` is the more common convention in GIS tools (PostGIS, QGIS, Leaflet).

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
- Implemented 4 architecture improvements after extended thinking review:
  1. **Date sanity check**: Validates parsed dates match expected month from filename
  2. **Raw coordinate preservation**: Added `*_lat_raw` / `*_lon_raw` columns for validation
  3. **Single-pass filter stats**: Combined stats query with main query (halved I/O)
  4. **Coordinate naming standardization**: Unified on `lon` instead of mixing `lng`/`lon`

### Dec 9, 2024 - Session 5 (continued)
- Added **incremental processing**: Pipeline skips files with existing parquet output
  - Use `--force` flag to reprocess all files
  - Allows resuming interrupted runs without reprocessing
- Fixed **4-digit milliseconds** datetime format (2018+ data)
  - 2018 timestamps have `.4340` not `.434` - needed `%f` format specifier
  - Without fix, 100% of 2018+ rows were filtered as invalid timestamps
- Fixed **schema consistency** across legacy/modern eras:
  - `duration_sec`: Now INTEGER everywhere (was DOUBLE in modern)
  - `rideable_type`: Now VARCHAR everywhere (was untyped NULL in legacy)
- **Successfully processed 2013-2015**: 23.5M trips, 35 parquet files
  - 0.5% filter rate, 94% station match rate, 0 suspicious mappings
  - Output: 966 MB parquet (vs 3.8 GB raw CSV = 75% compression)
- **Sanity check results** (cross-year query test):
  - Median trip duration: 10.6 minutes (typical for bike share)
  - Member/casual split: 87% / 13%
  - Top station: 8 Ave & W 31 St (253K trips)
  - Station growth: 332 (2013) → 474 (2015)
- Added **Jupyter notebook** for data exploration (`notebooks/explore_data.ipynb`)
  - Time series charts, station maps, pattern analysis
  - Uses DuckDB glob queries on parquet files
- Installed **DBeaver** for SQL GUI exploration
- **Environment fix**: Added `~/Library/Python/3.9/bin` to PATH in `~/.zshrc`
  - Fixes `jupyter notebook` command not found issue

### Dec 9, 2024 - Session 6 (continued)
- **Fixed 2018 duplicate data issue**:
  - 2018 had both combined files AND split files (_1, _2) = doubled trip count
  - Removed 21 split files, reprocessed with 12 combined files only
  - 2018 now correctly shows ~17.4M trips (was incorrectly showing ~35M)
- **Data quality checks** on 2013-2018 (71M trips):
  - Duration: Consistent 10-14 min median across all years
  - Coordinates: <200 trips outside NYC bounds, 0 missing
  - Round trips: 1.8-2.5% (normal for bike share)
  - Member split: 84-90% members
- **Station mapping investigation**:
  - 95.26% crosswalk, 4.74% ghost, 0.00% unmatched
  - Identified "false ghost" stations (renamed but not matched due to low name similarity)
  - Example: "Pershing Square North" → "Park Ave & E 42 St" (0.0m distance, 36% name similarity)
- **Fixed crosswalk builder** (`src/build_crosswalk.py`):
  - **Tier 1**: <20m = match regardless of name (high confidence)
  - **Tier 2 (NEW)**: 20-50m + >50% name similarity = match
  - **Tier 3**: 50-150m + >60% name similarity = match (low confidence)
  - **Tier 4**: >150m OR low similarity = ghost (no match)
- **Added test station filtering** to pipeline:
  - Filters out depot, test, "don't use", mobile, QC, and internal stations
  - 13 patterns in `TEST_STATION_PATTERNS` list
- **Created mapping report** (`src/mapping_report.py`):
  - Analyzes station mappings for audit/transparency
  - Shows match types: coordinate-only (renamed), name-only (moved), both, ghost
  - Reports coordinate drift and name similarity distributions
  - Identifies renamed stations and true ghost stations

#### Session 6 - Bug Found in Mapping Report (NEEDS FIX)

**Bug discovered:** The mapping report (`src/mapping_report.py`) has a year filtering bug.

**Problem:**
- The DuckDB query groups by (station_id, station_name, lat, lon) across ALL years
- Uses `MIN(filename) as sample_file` to get a representative file
- Python then filters observations where `extract_year(sample_file) == requested_year`
- BUT: If station 519 exists in 2013 AND 2014, `MIN(filename)` returns the 2013 file
- So when filtering for year=2014, station 519 is excluded (its sample_file shows 2013)

**Result:** Year-specific reports are broken:
- 2014 report showed only 3 stations (should be ~330+)
- 2018 report showed 66.9% ghost (way too high)

**Fix needed:** Filter by year in SQL BEFORE grouping, not in Python after grouping.

**Location:** `src/mapping_report.py`, function `get_unique_station_observations()`

**Proposed fix:** Add year filter to the DuckDB query WHERE clause:
```python
# In the SQL query, add:
WHERE filename LIKE '%{year}%'  # or use regex to extract year from filename
```

Or better: Extract year in SQL and filter there:
```sql
WHERE CAST(REGEXP_EXTRACT(filename, '(\d{4})', 1) AS INTEGER) = {year}
```

#### Mapping Report Results (2013-2019) - PARTIALLY INVALID

Reports were generated but year filtering was broken:

| Year | Stations Found | Status |
|------|---------------|--------|
| 2013 | 338 | ✓ Valid (first year, no prior data) |
| 2014 | 3 | ✗ BUG - should be ~330+ |
| 2015 | 156 | ⚠ Likely only new stations |
| 2016 | 176 | ⚠ Likely only new stations |
| 2017 | 199 | ⚠ Likely only new stations |
| 2018 | 63 | ⚠ Likely only new stations |
| 2019 | 221 | ⚠ Likely only new stations |

**Files generated (in logs/):**
- `station_profiles_YEAR_*.csv` - detailed profiles per station ID
- `mapping_report_*.csv` - observation-level mapping details
- `mapping_report_*.json` - summary statistics

#### Next Steps (After Compacting)

1. **Fix the mapping report bug** in `src/mapping_report.py`:
   - Modify `get_unique_station_observations()` to filter by year in SQL
   - Test with 2014 to verify fix (should see ~330+ stations, not 3)

2. **Re-run mapping reports** for 2013-2019:
   ```bash
   python3 src/mapping_report.py --years 2013 --detail
   python3 src/mapping_report.py --years 2014 --detail
   # ... etc for 2015-2019
   ```

3. **Analyze cross-year trends** after valid reports generated

4. **Consider duration filter change**: Currently 90s minimum, could increase to 120s (2 min)
   - 90-120s trips are ~0.6-1.2% of data
   - These are likely false starts/errors
   - Can decide after reviewing mapping reports

5. **Process pipeline** (create parquet files) AFTER mapping analysis complete

#### Other Changes Made in Session 6

1. **Crosswalk builder** (`src/build_crosswalk.py`) - improved matching tiers:
   - Tier 1: <20m = match regardless of name
   - Tier 2 (NEW): 20-50m + >50% name similarity = match
   - Tier 3: 50-150m + >60% name similarity = match
   - Tier 4: >150m OR low similarity = ghost

2. **Pipeline** (`src/pipeline.py`) - added test station filtering:
   - `TEST_STATION_PATTERNS` list with 13 patterns
   - Filters: depot, test, "don't use", mobile, QC, tech stations
   - Applied in WHERE clause of main query

3. **Mapping report** (`src/mapping_report.py`) - added `--detail` flag:
   - Generates `station_profiles_*.csv` with per-station-ID analysis
   - Shows name variants, coordinate spread, mapping details
   - Identifies stations with name changes or coordinate drift

4. **Notebook** (`notebooks/explore_data.ipynb`) - updated map colors:
   - Now has colors for years 2013-2025 (was only 2013-2015)
   - Dynamic legend based on years present in data

#### Data State

- **Raw CSVs**: 352 files in `data/raw_csvs/`
- **Processed parquet**: Currently only 2013 (from test earlier), need to clear and reprocess
- **Crosswalk**: Updated with improved matching (3,480 matched, 131 ghosts)

#### Commands for Next Session

```bash
# 1. Fix mapping report (edit src/mapping_report.py)

# 2. Test fix on 2014
python3 src/mapping_report.py --years 2014 --detail

# 3. If fix works, run all years
for year in 2013 2014 2015 2016 2017 2018 2019; do
  python3 src/mapping_report.py --years $year --detail
done

# 4. After analysis, process pipeline
python3 src/pipeline.py --year 2013
python3 src/pipeline.py --year 2014
# ... etc
```

### Dec 9, 2024 - Session 7 (continued)

#### Mapping Report Bug Fixed

Fixed the year filtering bug in `src/mapping_report.py`:
- **Problem**: Year filter happened AFTER grouping, so `MIN(filename)` returned earliest year
- **Fix**: Added `file_year` column extraction in SQL, filter in `cleaned` CTE BEFORE grouping
- **Result**: 2014 now shows 345 observations (was 3)

#### Mapping Reports Re-run (2013-2019) - Valid Results

| Year | Observations | Matched | Ghost | Trip Coverage |
|------|-------------|---------|-------|---------------|
| 2013 | 377 | 92.3% | 7.7% | 95.8% |
| 2014 | 345 | 93.9% | 6.1% | 96.1% |
| 2015 | 504 | 94.8% | 5.2% | 96.3% |
| 2016 | 663 | 93.5% | 6.5% | 96.8% |
| 2017 | 819 | 93.5% | 6.5% | 97.2% |
| 2018 | 922 | 84.6% | 15.4% | 97.5% |
| 2019 | 1002 | 96.8% | 3.2% | 98.1% |

2018 shows higher ghost observation count due to test/internal stations; trip coverage remains high.

#### Test Station Filtering Added to Mapping Report

Added `--include-test` flag (default: filter out test stations):
- Filters NULL station names
- Filters test patterns: depot, mobile, 8D ops, kiosk, etc.
- Reduces noise in analysis

#### Station ID Reuse Analysis - Key Findings

Deep temporal analysis revealed different categories of "ID issues":

**1. TRUE ID REUSE (Same ID, Different Locations, Overlapping Time)**

| ID | Location 1 | Location 2 | Distance | Trips Affected |
|----|------------|------------|----------|----------------|
| 279 | Peck Slip & Front St | Sands St & Gold St | 1.8km | ~3,700 |
| 2001 | Sands St & Navy St | 7 Ave & Farragut St | 565m | ~1,400 |

These are the ONLY cases where trips may be mapped to wrong station. Total: ~5,000 trips out of 90M+ (0.005%).

**2. Station Renaming (NOT a problem)**
- IDs 517, 519: "Pershing Square" ↔ "E 41/42 St & Madison/Vanderbilt" - same location, name changed
- Crosswalk handles these correctly via coordinate matching

**3. ID Recycling (NOT a problem)**
- ID 3016: "Mobile 01" (test) → "Kent Ave & N 7 St" (real) - test retired before reuse

**4. Data Entry Errors (negligible)**
- ID 160: 8 trips with lat=40.44 instead of 40.74 (33km error)

**5. NULL Stations**
- 2,497 trips in 2018 with NULL names scattered in Bronx - likely test/e-bike data

#### Current Pipeline Architecture (ID-First Matching)

```
trip.station_id → crosswalk lookup → canonical_station
```

**Issue**: For ID reuse cases (279, 2001), the crosswalk has ONE entry per ID built from the most common usage. Trips to the "other" location get wrong canonical coords.

**Raw coordinates exist in output** (`start_lat_raw`, `start_lon_raw`) but are NOT used for matching.

#### Proposed Fix: Coordinate-First Matching (Not Yet Implemented)

```
trip.raw_coords → nearest canonical station (within 200m) → canonical_station
                  ↓ (fallback if no match)
trip.station_id → crosswalk lookup → canonical_station
```

**Benefits**:
- Fixes ID reuse cases by using actual trip coordinates
- Handles GPS precision variations naturally
- Better for post-2020 data with high-precision coords

**Tradeoffs**:
- Requires Python spatial index (scipy cKDTree) preprocessing
- ~2x slower than pure SQL joins
- Added complexity

**Decision**: Document and defer. Impact is 0.005% of trips. Will revisit after exploring 2020+ data.

#### Data Quality Filtering Summary

| Filter | Action | Trips Affected |
|--------|--------|----------------|
| NULL station names | Remove | ~2,500 |
| Test stations | Remove | ~21,000 |
| Data entry errors (>30km from any station) | Remove | ~10 |
| Duration <90s or >4h | Remove | ~0.5-1% |
| Date mismatch (parsed ≠ file month) | Remove | varies |

#### Files Modified in Session 7

1. `src/mapping_report.py`:
   - Fixed year filtering bug (filter in SQL before grouping)
   - Added `TEST_STATION_PATTERNS` and `build_test_station_sql_filter()`
   - Added `--include-test` flag (default: exclude test stations)
   - Added NULL station name filtering

#### 2020-2025 Data Analysis

**2020-2021 Mapping Results:**

| Year | Observations | Matched | Ghost | Trip Coverage | Coord Drift Median |
|------|-------------|---------|-------|---------------|-------------------|
| 2020 | 1,760 | 97.7% | 2.3% | 98.3% | 0.1m |
| 2021 | 2,205 | 97.7% | 2.3% | 98.5% | 0.1m |

**2022-2025 Spot Check:**

| Year | Trips | Stations | Bike Types | Crosswalk Coverage |
|------|-------|----------|------------|-------------------|
| 2022 | 29.8M | 1,749 | 2 | 100% |
| 2023 | 35.1M | 2,347 | 2 | 100% |
| 2024 | 44.3M | 2,378 | 2 | 100% |
| 2025 | 43.7M | 2,342 | 2 | 100% |

**Key findings:**
- Station IDs in 2020+ are decimal format (e.g., `6432.09`), not integers
- All 2020+ station IDs exist in crosswalk - 100% coverage
- Coordinate precision is consistent (~11 chars)
- Classic bikes have slightly MORE coord variation than e-bikes (contrary to expectation)

**GPS Variation by Bike Type (2021):**

| Type | Stations | Trips | Avg Coord Variants | % With Variants |
|------|----------|-------|-------------------|-----------------|
| classic_bike | 1,556 | 18.5M | 1.10 | 9.2% |
| electric_bike | 1,550 | 8.6M | 1.08 | 7.8% |

#### Coordinate Outliers Analysis

**Trips outside NYC bbox (2020-2025):** Only 17 trips out of 199M (0.000009%)

| Year | Outside NYC | What They Are |
|------|-------------|---------------|
| 2020 | 1 | Montreal lab station (MTL-ECO5-LAB) |
| 2021-2024 | 0 | None |
| 2025 | 16 | LA Metro demo stations |

**These are test stations, not GPS glitches.** Added to filter patterns:
- `mtl-eco`, `lab` - Montreal lab stations
- `la metro`, `demo` - LA Metro demo stations

**High coordinate "drift" explained:**
- The 14km drift in station profiles was caused by single outlier trips (1 out of 17K)
- These are GPS glitches during trip recording, not systematic issues
- Example: Station 4214.03 had 17,214 trips at correct location, 1 trip 14km away
- Impact: ~50 trips across entire dataset - negligible

#### Data Quality Issues Found in Modern Data

1. **Tab characters in station names**: `"Clinton St\t& Cherry St"` - literal `\t`
2. **Station name variations**: Same ID with slight typos (extra spaces, etc.)
3. **Pershing Square still renaming**: Same pattern as legacy data

#### Final Test Station Filter Patterns

```python
TEST_STATION_PATTERNS = [
    "don't use", "dont use", "do not use",  # Explicitly marked
    "nycbs depot", "nycbs test",             # NYC Bike Share internal
    "mobile 01", "mobile 02",                # Mobile test stations
    "8d ops", "8d qc", "8d mobile",          # 8D (vendor) test stations
    "gow tech", "tech shop", "ssp tech",     # Tech/maintenance stations
    "kiosk in a box", "mlswkiosk",           # Test kiosks
    "facility", "warehouse",                  # Internal facilities
    "temp", ".temp",                          # Temporary stations
    "deployment",                             # Deployment testing
    "mtl-eco", "lab",                         # Montreal lab stations (2020)
    "la metro", "demo",                       # LA Metro demo stations (2025)
]
```

#### Architecture Decision: Keep ID-First Matching

**Rationale:**
- Coordinate-first matching would only help ~5K trips (ID reuse cases 279, 2001)
- That's 0.005% of total data
- Modern data (2020+) has no ID reuse issues
- GPS variation is minimal and not systematically tied to e-bikes
- Added complexity not justified by benefit

**Current pipeline remains:**
```
trip.station_id → crosswalk lookup → canonical_station
```

**Known limitations (documented, not fixed):**
- IDs 279, 2001: ~5K trips may map to wrong station due to historical ID reuse
- Single-trip GPS glitches: ~50 trips with coords far from their station

#### Files Modified in Session 7

1. `src/mapping_report.py`:
   - Fixed year filtering bug
   - Added test station filtering
   - Added `--include-test` flag

2. `src/pipeline.py`:
   - Added `mtl-eco`, `lab`, `la metro`, `demo` to test station patterns

3. `CLAUDE.md`:
   - Comprehensive documentation of all findings

#### Data Ready for Processing

**Total raw data:**
- 352 CSV files in `data/raw_csvs/`
- Years 2013-2025
- ~200M trips estimated

**Crosswalk:**
- 3,611 entries (1,077 integer + 2,534 decimal IDs)
- 100% coverage of all years

**Processing command:**
```bash
# Process all years
python3 src/pipeline.py --year 2013
python3 src/pipeline.py --year 2014
# ... through 2025

# Or process a range
for year in {2013..2025}; do
    python3 src/pipeline.py --year $year
done
```

### Dec 9, 2024 - Session 8 (Final Processing)

#### Complete Dataset Processed

Successfully processed all 13 years of Citi Bike data:

| Year | Trips | Filtered | Match Rate | Ghost % | Suspicious Mappings |
|------|------:|----------|------------|---------|---------------------|
| 2013 | 5,580,769 | 0.6% | 95.9% | 4.1% | 0 |
| 2014 | 8,042,497 | 0.5% | 96.1% | 3.9% | 0 |
| 2015 | 9,878,177 | 0.6% | 96.3% | 3.7% | 0 |
| 2016 | 13,762,788 | 0.6% | 96.8% | 3.2% | 0 |
| 2017 | 16,255,958 | 0.7% | 97.2% | 2.8% | 0 |
| 2018 | 17,404,586 | 0.8% | 97.5% | 2.5% | 0 |
| 2019 | 20,391,559 | 0.8% | 98.1% | 1.9% | 0 |
| 2020 | 19,309,183 | 1.3% | 98.3% | 1.7% | 0 |
| 2021 | 26,724,796 | 1.5% | 98.5% | 1.5% | 0 |
| 2022 | 29,324,199 | 1.7% | 98.5% | 1.5% | 0 |
| 2023 | 34,395,973 | 2.0% | 98.9% | 1.1% | 0 |
| 2024 | 43,515,161 | 1.8% | 99.4% | 0.6% | 0 |
| 2025 | 42,976,831 | 1.6% | 99.8% | 0.2% | 0 |
| **TOTAL** | **287,562,477** | | | | **0** |

#### Output Statistics

- **352 parquet files** in `data/processed/`
- **11 GB** total size (compressed from ~40 GB raw CSV)
- **122 log files** tracking all processing runs

#### Sanity Checks Passed

1. **Growth trend**: 10x increase from 2013 to 2025 ✓
2. **Seasonal pattern**: Peak in September (2.6M), trough in February (1.0M) ✓
3. **COVID impact**: Clear dip in April 2020 ✓
4. **E-bike adoption**: 71% of trips in last 12 months ✓
5. **Reproducibility**: Scripts produce identical output when re-run ✓

#### Visualization Created

`logs/citibike_analysis.png` - Three-panel chart showing:
1. Total monthly ridership (2013-2025)
2. Seasonal pattern by month of year
3. Classic vs electric bike split

#### Scripts Verified as Reproducible

| Script | Test | Result |
|--------|------|--------|
| `fetch_stations.py` | Fresh API fetch | Same 2,318 stations |
| `build_crosswalk.py` | Rebuild from CSVs | Same 3,611 mappings |
| `pipeline.py` | Reprocess sample | Identical output |

#### Data Quality Confidence: HIGH

**What we filtered (appropriately):**
- Test/internal stations (depot, mobile, lab, demo)
- Invalid trips (<90s or >4h duration)
- Missing station IDs
- Date parsing errors

**Known limitations (documented, acceptable):**
- ~5K trips (0.002%) may map to wrong station due to ID reuse
- ~50 trips have GPS glitches
- 131 ghost stations preserved with historical coords

#### Next Steps

The ETL pipeline is complete. Data is ready for:
1. Analysis and visualization
2. Building predictive models
3. Creating dashboards
4. Answering research questions

#### Querying the Data

```python
import duckdb
con = duckdb.connect()

# Basic query
con.execute('''
    SELECT YEAR(started_at) as year, COUNT(*) as trips
    FROM "data/processed/*.parquet"
    GROUP BY 1 ORDER BY 1
''').fetchall()

# Station popularity
con.execute('''
    SELECT start_station_name, COUNT(*) as trips
    FROM "data/processed/*.parquet"
    GROUP BY 1 ORDER BY 2 DESC
    LIMIT 10
''').fetchall()
```
