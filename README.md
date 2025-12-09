# Citi Bike Data Pipeline

A reproducible ETL pipeline for cleaning and standardizing NYC Citi Bike trip data from 2013 to present.

## The Problem

Citi Bike data has three major challenges:

### 1. Schema Changes (Feb 2021)
| Feature | Legacy (2013-2020) | Modern (2021+) |
|---------|-------------------|----------------|
| Station ID | Integers (`72`, `3255`) | UUIDs (`66db3606-0aca-11e7...`) |
| Timestamps | `starttime`, `stoptime` | `started_at`, `ended_at` |
| Duration | `tripduration` (seconds) | Must calculate |
| User Type | `Subscriber`, `Customer` | `member`, `casual` |
| Bike Type | Not recorded | `rideable_type` |
| Demographics | `birth year`, `gender` | Removed |

### 2. No ID Bridge
The GBFS API's `short_name` field contains grid codes (`7141.07`), NOT legacy integer IDs. 
**There is no common key linking 2013 data to 2025 data.**

### 3. GPS Noise
Modern bikes report their own GPS coordinates. The same station appears as a cloud of slightly different points (±10 meters).

## The Solution

We build a **Station Crosswalk** that maps legacy IDs to modern UUIDs using:
1. **Spatial proximity** (must be within 150m)
2. **Name similarity** (fuzzy matching handles "W 21 St" vs "West 21st Street")

This crosswalk is version-controlled - it becomes your institutional knowledge.

## Directory Structure

```
citibike-pipeline/
├── reference/
│   ├── current_stations.csv      # GBFS API snapshot (ground truth)
│   ├── station_crosswalk.csv     # Legacy ID → Modern UUID mapping
│   └── manual_overrides.csv      # Hand-fixed edge cases
├── data/
│   ├── raw_zips/                 # Downloaded archives (gitignored)
│   ├── raw_csvs/                 # Extracted CSVs (gitignored)
│   └── processed/                # Clean parquet files (gitignored)
├── logs/                         # Processing audit logs
└── src/
    ├── fetch_stations.py         # Pull GBFS ground truth
    ├── download.py               # Download trip data
    ├── ingest.py                 # Extract zips (handles nesting)
    ├── build_crosswalk.py        # Spatial matching
    └── pipeline.py               # Main ETL
```

## Usage

```bash
# 1. Get current station data (run once, periodically refresh)
python src/fetch_stations.py

# 2. Download trip data
python src/download.py --year 2014 --month 6
python src/download.py --year 2022 --month 3

# 3. Extract all zips
python src/ingest.py

# 4. Build the crosswalk (run once, then maintain)
python src/build_crosswalk.py

# 5. Process trips
python src/pipeline.py --year 2014

# 6. Query results
duckdb -c "SELECT count(*) FROM 'data/processed/*.parquet'"
```

## Why GitHub?

- **Auditability**: Every cleaning decision is a commit
- **Reproducibility**: Anyone can clone and re-run
- **The crosswalk is code**: `station_crosswalk.csv` captures decisions that would otherwise be tribal knowledge
- **CI/CD**: GitHub Actions can automate monthly data pulls

## Key Technical Decisions

1. **Station matching threshold**: 150m radius + 70% name similarity
2. **Coordinate precision**: 6 decimal places (~10cm)
3. **Ghost stations**: Closed stations keep their legacy coordinates
4. **Output format**: Parquet (fast, compressed, typed)
