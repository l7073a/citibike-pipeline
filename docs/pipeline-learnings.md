# Pipeline Technical Learnings

Detailed technical findings from building the Citi Bike ETL pipeline. This file is for reference and is NOT loaded by Claude Code automatically.

## Schema Eras

| Era | Years | Schema | Station ID Format |
|-----|-------|--------|-------------------|
| Legacy | 2013-2019 | `tripduration,starttime,stoptime,start station id...` | Integer (e.g., `519`) |
| Modern | 2020-2025 | `ride_id,rideable_type,started_at,ended_at,start_station_id...` | Numeric with decimals (e.g., `5636.13`) |

**Important**: The schema change happened at **2020**, not 2021 as originally documented.

## Station ID Complexity

Even "modern" data (2020+) doesn't use the same UUIDs as the GBFS API:
- Trip data has IDs like `5636.13`, `8397.02`
- GBFS API returns UUIDs like `66dd056e-0aca-11e7-82f6-3863bb44ef7c`

This means **ALL trip data** (2013-2025) needs the crosswalk to resolve station IDs.

## Datetime Format Variations

There are **4 different datetime formats** across the dataset:

| Format | Files | Years | Example |
|--------|-------|-------|---------|
| `YYYY-MM-DD HH:MM:SS` | 49 | 2013-2017 | `2013-06-01 00:00:01` |
| `M/D/YYYY HH:MM:SS` | 31 | 2014-2016 | `11/1/2014 00:00:11` |
| `M/D/YYYY H:MM` | 4 | 2015 | `1/1/2015 0:01` |
| `YYYY-MM-DD HH:MM:SS.fff` | 208 | 2018-2025 | `2018-01-01 13:50:57.434` |

**Within the same year**, different months can have different formats!

## DuckDB Date Parsing Fix

**Problem**: DuckDB's `read_csv_auto` samples the first ~20K rows to detect types. For dates like `9/1/2014`, it might guess `%d/%m/%Y` (European) instead of `%m/%d/%Y` (US).

**Solution**: Force timestamp columns to VARCHAR, then parse explicitly:

```python
read_csv_auto('file.csv',
    ignore_errors=true,
    types={'starttime': 'VARCHAR', 'stoptime': 'VARCHAR'}
)
```

Then in SQL:
```sql
COALESCE(
    TRY_STRPTIME(CAST(starttime AS VARCHAR), '%Y-%m-%d %H:%M:%S'),
    TRY_STRPTIME(CAST(starttime AS VARCHAR), '%Y-%m-%d %H:%M:%S.%g'),
    TRY_STRPTIME(CAST(starttime AS VARCHAR), '%m/%d/%Y %H:%M:%S'),
    TRY_STRPTIME(CAST(starttime AS VARCHAR), '%m/%d/%Y %H:%M')
) as started_at
```

## Other Data Quality Issues

- **2013**: Has literal `NULL` strings in coordinate columns (not null values)
- **2013**: Has quoted headers (`"tripduration"`) while later years don't
- **2017**: Some files have extra columns (16 vs 15) - use `ignore_errors=true`

## Crosswalk Statistics

After scanning all years (2013-2025):
- 3,531 unique legacy station IDs found
- 3,391 matched to modern stations (96%)
- 140 "ghost stations" (closed/moved, no modern equivalent)

Match confidence tiers:
- **Tier 1**: <20m = match regardless of name (high confidence)
- **Tier 2**: 20-50m + >50% name similarity = match
- **Tier 3**: 50-150m + >60% name similarity = match (low confidence)
- **Tier 4**: >150m OR low similarity = ghost (no match)

## Ghost Station Handling

Ghost stations are preserved with their original names and coordinates:
1. Crosswalk stores legacy station info even when no modern match exists
2. Trips tagged with `start_match_type` / `end_match_type` = `'ghost'`
3. Original coordinates from trip data preserved

**Top Ghost Stations by Trip Volume:**
| Legacy ID | Name | Historical Trips |
|-----------|------|------------------|
| 519 | Pershing Square North | 1,075,680 |
| 518 | E 39 St & 2 Ave | 429,419 |
| 517 | Pershing Square South | 419,741 |

## Data Quality Filters

| Filter | Threshold | Rationale |
|--------|-----------|-----------|
| Missing start station ID | Required | Cannot analyze origin |
| Missing end station ID | Required | Cannot analyze destination |
| Duration too short | >= 90 seconds | Likely errors or false starts |
| Duration too long | <= 4 hours | Likely lost/stolen bikes |
| Invalid timestamps | Must parse | Data integrity |

## Test Station Filter Patterns

```python
TEST_STATION_PATTERNS = [
    "don't use", "dont use", "do not use",
    "nycbs depot", "nycbs test",
    "mobile 01", "mobile 02",
    "8d ops", "8d qc", "8d mobile",
    "gow tech", "tech shop", "ssp tech",
    "kiosk in a box", "mlswkiosk",
    "facility", "warehouse",
    "temp", ".temp",
    "deployment",
    "mtl-eco", "lab",
    "la metro", "demo",
]
```

## Coordinate Handling Strategy

**Priority Order** (implemented via COALESCE):
```sql
start_lat = COALESCE(
    current_stations.lat,      -- If matched to modern station
    crosswalk.legacy_lat,      -- If in crosswalk (even as ghost)
    raw_trip_data.start_lat    -- Fallback to raw data
)
```

## Station ID Reuse (Known Issue)

Only 2 cases of true ID reuse found:

| ID | Location 1 | Location 2 | Distance | Trips Affected |
|----|------------|------------|----------|----------------|
| 279 | Peck Slip & Front St | Sands St & Gold St | 1.8km | ~3,700 |
| 2001 | Sands St & Navy St | 7 Ave & Farragut St | 565m | ~1,400 |

Total: ~5,000 trips out of 287M (0.002%) - acceptable limitation.

## Demographics Data Quality (Legacy 2013-2019)

### Issue 1: 1969 Default Birth Year
Starting in 2018, casual riders without birth year were assigned `1969` as default.
- Detection: `birth_year = 1969 AND member_casual = 'casual' AND YEAR(started_at) >= 2018`
- Affected: 2.39M trips (2.6% of legacy data)

### Issue 2: Unknown Gender
Gender field uses: 0=unknown, 1=male, 2=female
- Unknown (0): 9.29M trips (10.2% of legacy data)

### Issue 3: Implausible Birth Years
~38K trips have birth years making riders 100+ years old.

### Recommended Filters
```sql
-- Clean birth year filter
WHERE birth_year IS NOT NULL
  AND NOT (birth_year = 1969 AND member_casual = 'casual' AND YEAR(started_at) >= 2018)
  AND (YEAR(started_at) - birth_year) BETWEEN 16 AND 80

-- Clean gender filter
WHERE gender IN (1, 2)
```

## Weather Data Integration

Weather and calendar data stored separately, joined at query time:

| Data | Source | File |
|------|--------|------|
| Hourly weather | Open-Meteo Archive API | `data/weather/hourly_weather.parquet` |
| Daily (sunrise/sunset) | Open-Meteo Daily API | `data/weather/daily_weather.parquet` |
| US Federal Holidays | Python `holidays` library | `data/weather/holidays.parquet` |

Example join:
```sql
FROM 'data/processed/*.parquet' t
LEFT JOIN 'data/weather/hourly_weather.parquet' w
    ON DATE_TRUNC('hour', t.started_at) = w.datetime
LEFT JOIN 'data/weather/daily_weather.parquet' d
    ON DATE(t.started_at) = d.date
LEFT JOIN 'data/weather/holidays.parquet' h
    ON DATE(t.started_at) = h.date
```

## Final Processing Results

| Year | Trips | Filtered | Match Rate | Ghost % |
|------|------:|----------|------------|---------|
| 2013 | 5,580,769 | 0.6% | 95.9% | 4.1% |
| 2014 | 8,042,497 | 0.5% | 96.1% | 3.9% |
| 2015 | 9,878,177 | 0.6% | 96.3% | 3.7% |
| 2016 | 13,762,788 | 0.6% | 96.8% | 3.2% |
| 2017 | 16,255,958 | 0.7% | 97.2% | 2.8% |
| 2018 | 17,404,586 | 0.8% | 97.5% | 2.5% |
| 2019 | 20,391,559 | 0.8% | 98.1% | 1.9% |
| 2020 | 19,309,183 | 1.3% | 98.3% | 1.7% |
| 2021 | 26,724,796 | 1.5% | 98.5% | 1.5% |
| 2022 | 29,324,199 | 1.7% | 98.5% | 1.5% |
| 2023 | 34,395,973 | 2.0% | 98.9% | 1.1% |
| 2024 | 43,515,161 | 1.8% | 99.4% | 0.6% |
| 2025 | 42,976,831 | 1.6% | 99.8% | 0.2% |
| **TOTAL** | **287,562,477** | | | |

Output: 352 parquet files, 11 GB (compressed from ~40 GB raw CSV)
