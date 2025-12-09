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

### 3. Data Quality Issues Found

- **2013**: Has literal `NULL` strings in coordinate columns (not null values)
- **Nov 2014**: Uses `M/D/YYYY` timestamp format instead of `YYYY-MM-DD`
- **2013**: Has quoted headers (`"tripduration"`) while later years don't

Fixes applied:
- Use `TRY_CAST()` instead of `::TYPE` for timestamps and coordinates
- DuckDB's `ignore_errors=true` handles malformed rows

### 4. Zip File Structures by Year

| Years | Structure |
|-------|-----------|
| 2013-2019 | Full year bundled as one zip (e.g., `2014-citibike-tripdata.zip`) |
| 2020-2023 | Full year with nested monthly zips inside |
| 2024+ | Individual monthly zips (e.g., `202406-citibike-tripdata.zip`) |

The `ingest.py` script handles nested zips automatically.

### 5. Crosswalk Statistics

After scanning all years (2013-2025):
- 3,531 unique legacy station IDs found
- 3,391 matched to modern stations (96%)
- 140 "ghost stations" (closed/moved, no modern equivalent)

Match confidence levels:
- **High**: Distance < 50m AND name similarity > 80%
- **Medium**: Distance < 150m OR name similarity > 60%
- **Low**: Matched but with lower confidence

### 6. Processing Match Rates by Era

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

## Ghost Stations (Notable)

These high-traffic stations no longer exist:
- `519` "Pershing Square North" (1.07M trips) - near Grand Central
- `518` "E 39 St & 2 Ave" (429K trips)
- `517` "Pershing Square South" (420K trips)
- `345` "W 13 St & 6 Ave" (304K trips)

Their coordinates are preserved in the crosswalk for historical analysis.

## Future Work

1. **Full processing**: Run pipeline on all 245 CSV files (~150M+ trips)
2. **Manual overrides**: Add `reference/manual_overrides.csv` for known mis-matches
3. **Validation**: Query to verify station resolution quality
4. **Incremental updates**: Add logic to skip already-processed files

## Session History

### Dec 9, 2024
- Set up GitHub repo
- Downloaded data from all years (2013-2025)
- Discovered schema change was at 2020 not 2021
- Fixed pipeline bugs (TRY_CAST, timestamp formats, NULL strings)
- Built crosswalk with 96% match rate
- Tested pipeline on representative years from each era
