# Session History

Historical log of development sessions on the Citi Bike ETL pipeline. This file is for reference only and is NOT loaded by Claude Code automatically.

## Dec 9, 2024 - Session 1
- Set up GitHub repo
- Downloaded data from all years (2013-2025)
- Discovered schema change was at 2020 not 2021
- Fixed pipeline bugs (TRY_CAST, timestamp formats, NULL strings)
- Built crosswalk with 96% match rate
- Tested pipeline on representative years from each era

## Dec 9, 2024 - Session 2
- Surveyed datetime formats across all 293 CSV files
- Found 4 distinct datetime formats, varying within years
- Documented ghost station handling in detail
- Verified historical trips are preserved (not deleted)

## Dec 9, 2024 - Session 3
- Added data quality filters (90s-4h duration, required station IDs)
- Discovered 2013 duplicate files issue (~5M duplicate rows)
- Created `cleanup_duplicates.py` to remove redundant split files
- Created `audit.py` for comprehensive data quality analysis
- Added station timeline tracking (first/last appearance)
- Documented coordinate handling strategy (canonical coordinates)
- Current CSV count after cleanup: 373 files

## Dec 9, 2024 - Session 4
- Downloaded remaining 2024 and 2025 months
- Added `bike_id`, `birth_year`, `gender`, `rideable_type` columns
- Created `validate_mappings.py` for station mapping validation
- Fixed critical DuckDB datetime parsing bug (M/D/YYYY vs D/M/YYYY)
- Implemented 4 architecture improvements:
  1. Date sanity check (validates parsed dates match filename month)
  2. Raw coordinate preservation (added `*_lat_raw` / `*_lon_raw` columns)
  3. Single-pass filter stats (halved I/O)
  4. Coordinate naming standardization (unified on `lon`)

## Dec 9, 2024 - Session 5
- Added incremental processing (skip files with existing parquet)
- Fixed 4-digit milliseconds datetime format (2018+ data)
- Fixed schema consistency across legacy/modern eras
- Successfully processed 2013-2015: 23.5M trips, 35 parquet files
- Added Jupyter notebook for data exploration
- Environment fix: Added `~/Library/Python/3.9/bin` to PATH

## Dec 9, 2024 - Session 6
- Fixed 2018 duplicate data issue (had both combined and split files)
- Fixed crosswalk builder with improved matching tiers
- Added test station filtering to pipeline
- Created mapping report (`src/mapping_report.py`)
- Found and documented mapping report year filtering bug

## Dec 9, 2024 - Session 7
- Fixed mapping report year filtering bug
- Re-ran mapping reports for 2013-2019 with valid results
- Deep analysis of station ID reuse (only ~5K trips affected, 0.005%)
- Analyzed 2020-2025 data quality (100% crosswalk coverage)
- Decided to keep ID-first matching (coordinate-first not worth complexity)
- Added Montreal lab and LA Metro demo station filters

## Dec 9, 2024 - Session 8 (Final Processing)
- Processed all 13 years: 287,562,477 trips total
- Output: 352 parquet files, 11 GB (compressed from ~40 GB CSV)
- Added 3 demographic validity columns (birth_year_valid, gender_valid, age_at_trip)
- Created visualization: `logs/citibike_analysis.png`
- Verified reproducibility of all scripts

## Dec 10, 2024 - Session 9
- Reorganized project into subprojects (mta/, ferry/)
- Added Jersey City/Hoboken data support
- Created JC crosswalk with 85.7% match rate (54 matched, 9 ghost stations)
- Built cross-Hudson analysis notebook with multiple visualizations

## Dec 10, 2024 - Session 10
- Restructured CLAUDE.md per Anthropic best practices
- Moved session history to `docs/session-history.md`
- Moved technical learnings to `docs/pipeline-learnings.md`
- Reduced CLAUDE.md from ~1,400 lines to ~250 lines
