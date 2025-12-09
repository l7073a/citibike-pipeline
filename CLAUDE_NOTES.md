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

The pipeline uses `TRY_CAST()` to handle this gracefully.

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

## File Locations

```
citibike-pipeline/
├── src/
│   ├── fetch_stations.py    # Get current stations from GBFS API
│   ├── download.py          # Download trip data from S3
│   ├── ingest.py            # Extract zips, handle nesting
│   ├── build_crosswalk.py   # Build legacy→modern station mapping
│   └── pipeline.py          # Main ETL transformation
├── reference/
│   ├── current_stations.csv # 2,318 stations from GBFS
│   └── station_crosswalk.csv # Legacy ID → Modern UUID mapping
├── data/
│   ├── raw_zips/            # Downloaded zip files (not in git)
│   ├── raw_csvs/            # Extracted CSVs (not in git)
│   └── processed/           # Output parquet files (not in git)
└── logs/                    # Processing logs (JSON)
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

1. **Full processing**: Run pipeline on all 245 CSV files (~150M+ trips)
2. **Manual overrides**: Add `reference/manual_overrides.csv` for known mis-matches
3. **Validation**: Query to verify station resolution quality
4. **Incremental updates**: Add logic to skip already-processed files

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
