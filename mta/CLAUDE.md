# MTA Subway Data Integration

This subproject handles MTA subway ridership data for comparative analysis with Citi Bike trips.

## Project Status

**Documentation complete, scripts not yet implemented**

The infrastructure below was planned in Session 19 of the main Citi Bike project. Directory structure and scripts described here do not yet exist - this is a blueprint for future implementation.

## Overview

MTA subway ridership data provides a baseline for understanding NYC transit patterns. Comparing subway and Citi Bike usage helps identify:
- Mode substitution patterns (bike vs subway choice)
- First/last mile connections
- Weather sensitivity differences
- Commute corridor preferences

## Available Datasets

| Dataset | Coverage | Granularity | Portal |
|---------|----------|-------------|--------|
| **MTA Subway Hourly Ridership** | 2020-present | By hour, station, fare type | data.ny.gov |
| **MTA Origin-Destination Estimates** | 2023-present | By hour, O-D pair | data.ny.gov |
| **MTA Daily Ridership** | 2020-present | System-wide daily total | data.ny.gov |
| **Legacy Turnstile Data** | 2010-2022 | ~4 hour intervals | web.mta.info |

## Dataset 1: MTA Subway Hourly Ridership

**URL**: https://data.ny.gov/Transportation/MTA-Subway-Hourly-Ridership-Beginning-February-202/wujg-7c2s

**Schema:**

| Column | Type | Description |
|--------|------|-------------|
| `transit_timestamp` | TIMESTAMP | Hour of entry (e.g., 2024-07-15 08:00:00) |
| `transit_mode` | VARCHAR | Always 'subway' |
| `station_complex_id` | VARCHAR | Station ID |
| `station_complex` | VARCHAR | Station name |
| `borough` | VARCHAR | Manhattan, Brooklyn, Queens, Bronx, Staten Island |
| `payment_method` | VARCHAR | 'metrocard', 'omny', or 'other' |
| `fare_class_category` | VARCHAR | 'Metrocard - Full Fare', 'OMNY', 'Senior/Disabled', etc. |
| `ridership` | INTEGER | Number of entries |
| `transfers` | INTEGER | Number of free transfers from another mode |
| `latitude` | FLOAT | Station latitude |
| `longitude` | FLOAT | Station longitude |
| `georeference` | POINT | Geo point for mapping |

**Notes:**
- Starts February 2022 (when OMNY data became complete)
- Hourly granularity is excellent for commute analysis
- Distinguishes payment types (OMNY vs MetroCard)
- Fare class shows reduced fare usage (seniors, students, disabled)

## Dataset 2: MTA Origin-Destination Estimates

**URL**: https://data.ny.gov/Transportation/MTA-Subway-Origin-Destination-Ridership-Estimate-2/jsu2-fbtj

**Schema:**

| Column | Type | Description |
|--------|------|-------------|
| `month` | DATE | Month of data |
| `day_of_week` | VARCHAR | 'Weekday', 'Saturday', 'Sunday' |
| `hour` | INTEGER | Hour (0-23) |
| `origin_station_complex_id` | VARCHAR | Entry station |
| `origin_station_complex_name` | VARCHAR | Entry station name |
| `destination_station_complex_id` | VARCHAR | Exit station (estimated) |
| `destination_station_complex_name` | VARCHAR | Exit station name |
| `estimated_average_ridership` | FLOAT | Average trips per day |

**Limitations:**
- **Estimated, not actual**: Destinations are inferred from subsequent swipes, not from exit data (subway has no exit turnstiles)
- **Aggregated**: Monthly averages by day-type, not daily actuals
- **Recent data only**: 2023+
- **Confidence varies**: Popular routes have better estimates than rare ones

## Dataset 3: Legacy Turnstile Data

**URL**: http://web.mta.info/developers/turnstile.html

**Schema (weekly files):**

| Column | Description |
|--------|-------------|
| `C/A` | Control Area ID |
| `UNIT` | Remote Unit ID |
| `SCP` | Subunit Channel Position (specific turnstile) |
| `STATION` | Station name |
| `LINENAME` | Lines serving station (e.g., "456NQR") |
| `DIVISION` | Original transit company |
| `DATE` | Date (MM/DD/YYYY) |
| `TIME` | Time of reading |
| `DESC` | Description (usually "REGULAR") |
| `ENTRIES` | Cumulative entry count |
| `EXITS` | Cumulative exit count |

**Notes:**
- **Cumulative counters**: Must compute differences between readings
- **~4 hour intervals**: Readings at irregular times (varies by station)
- **Per-turnstile data**: Requires aggregation to station level
- **Data quality issues**: Counter resets, missing readings, duplicate rows
- **Discontinued**: Replaced by hourly ridership dataset in 2022

## API Access via Socrata

All datasets on data.ny.gov use the Socrata Open Data API (SODA):

```bash
# Example: Get hourly ridership for specific station
curl "https://data.ny.gov/resource/wujg-7c2s.json?\$where=station_complex like '%42 St%'&\$limit=1000"

# Download as CSV
curl "https://data.ny.gov/resource/wujg-7c2s.csv?\$limit=50000" > mta_hourly.csv

# With date filter
curl "https://data.ny.gov/resource/wujg-7c2s.json?\$where=transit_timestamp>='2024-01-01'&\$limit=10000"
```

**Rate Limits:**
- Unauthenticated: 1,000 requests/hour
- With app token: 10,000 requests/hour
- Max rows per request: 50,000 (use `$offset` for pagination)

## Comparison: Citi Bike vs MTA Subway Data

| Aspect | Citi Bike | MTA Subway |
|--------|-----------|------------|
| **Origin-Destination** | Exact (dock to dock) | Estimated (entry only) |
| **Time granularity** | Trip start/end timestamps | Hourly aggregates |
| **Historical depth** | 2013-present (12 years) | 2020/2022-present (~3 years detailed) |
| **User identification** | Anonymous, member/casual only | None (just aggregate counts) |
| **Geographic coverage** | Manhattan + parts of Brooklyn/Queens | All 5 boroughs |
| **Scale** | ~300K rides/day peak | ~4M rides/day |
| **Weather impact** | High (exposed travel) | Low (underground) |

## Use Cases for Combined Analysis

1. **Mode substitution**: Do Citi Bike trips increase on nice days when subway ridership is flat?
2. **First/last mile**: Which subway stations have high Citi Bike activity nearby?
3. **Commute corridor comparison**: Compare popular routes between modes
4. **Service disruption**: How does Citi Bike usage change during subway outages?
5. **Normalize for transit trends**: Use subway as a baseline to identify bike-specific patterns

## Example Query: Compare Morning Rush

```sql
-- Citi Bike morning rush
SELECT DATE(started_at) as date,
       COUNT(*) as bike_trips
FROM "../data/processed/*.parquet"
WHERE EXTRACT(HOUR FROM started_at) BETWEEN 7 AND 9
  AND member_casual = 'member'
  AND EXTRACT(DOW FROM started_at) BETWEEN 1 AND 5
GROUP BY 1;

-- MTA Subway morning rush (from downloaded data)
SELECT transit_timestamp::DATE as date,
       SUM(ridership) as subway_entries
FROM "data/ridership/mta_hourly.parquet"
WHERE EXTRACT(HOUR FROM transit_timestamp) BETWEEN 7 AND 9
  AND EXTRACT(DOW FROM transit_timestamp) BETWEEN 1 AND 5
GROUP BY 1;
```

## Downloading Recommendations

For analysis, download to local parquet:

```python
import pandas as pd
import requests

# Download MTA hourly ridership (may take a while - millions of rows)
url = "https://data.ny.gov/resource/wujg-7c2s.csv?$limit=2000000"
df = pd.read_csv(url)
df.to_parquet("data/ridership/mta_hourly_ridership.parquet")

# Or use DuckDB directly
import duckdb
con = duckdb.connect()
con.execute("""
    COPY (SELECT * FROM read_csv_auto('https://data.ny.gov/resource/wujg-7c2s.csv?$limit=2000000'))
    TO 'data/ridership/mta_hourly_ridership.parquet' (FORMAT PARQUET)
""")
```

**Note:** The full hourly dataset is 50M+ rows. Consider filtering by date range or borough when downloading.

## MTA GTFS Data (Schedule/Station Info)

### Directory Structure

```
mta/
├── src/
│   ├── fetch_gtfs.py         # Download GTFS feed
│   ├── fetch_ridership.py    # Download hourly ridership
│   └── build_reference.py    # Process GTFS → reference tables
├── data/
│   ├── gtfs/                 # Raw GTFS feed files
│   │   ├── stops.txt         # Stations, platforms, entrances
│   │   ├── routes.txt        # Subway lines (A, B, C, 1, 2, 3...)
│   │   ├── trips.txt         # Individual scheduled trips
│   │   ├── stop_times.txt    # Arrival times at each stop
│   │   ├── shapes.txt        # Line geometry for mapping
│   │   ├── transfers.txt     # Transfer connections
│   │   └── metadata.json     # Download timestamp
│   ├── ridership/            # Hourly ridership data
│   │   ├── mta_hourly_ridership.parquet
│   │   └── metadata.json
│   └── reference/            # Processed reference tables
│       ├── stations.parquet  # Station complexes (472 stations)
│       ├── entrances.parquet # Physical entrance locations
│       ├── routes.parquet    # Subway lines with colors
│       ├── station_routes.parquet  # Lines serving each station
│       ├── service_frequency.parquet # Trains per hour estimates
│       └── metadata.json
└── CLAUDE.md                 # This file
```

### Scripts (To Be Created)

| Script | Purpose | Output |
|--------|---------|--------|
| `src/fetch_gtfs.py` | Download MTA GTFS static feed | `data/gtfs/*.txt` |
| `src/fetch_ridership.py` | Download hourly ridership | `data/ridership/*.parquet` |
| `src/build_reference.py` | Process GTFS → parquet | `data/reference/*.parquet` |

### Workflow (When Implemented)

```bash
# Step 1: Download GTFS feed (~10MB)
python src/fetch_gtfs.py

# Step 2: Build reference tables
python src/build_reference.py

# Step 3: Download ridership (optional - large dataset)
# Full dataset (50M+ rows):
python src/fetch_ridership.py

# Or filtered by date:
python src/fetch_ridership.py --start 2024-01-01 --end 2024-12-31

# Or sample for testing:
python src/fetch_ridership.py --limit 100000
```

### Reference Table Schemas

**stations.parquet**
| Column | Type | Description |
|--------|------|-------------|
| `station_id` | VARCHAR | GTFS stop_id |
| `station_name` | VARCHAR | Station complex name |
| `latitude` | DOUBLE | Station centroid |
| `longitude` | DOUBLE | Station centroid |
| `borough` | VARCHAR | Manhattan/Brooklyn/Queens/Bronx (if available) |

**entrances.parquet**
| Column | Type | Description |
|--------|------|-------------|
| `entrance_id` | VARCHAR | GTFS entrance ID |
| `entrance_name` | VARCHAR | Entrance description |
| `latitude` | DOUBLE | Entrance coordinates |
| `longitude` | DOUBLE | Entrance coordinates |
| `station_id` | VARCHAR | Parent station |
| `station_name` | VARCHAR | Parent station name |

**routes.parquet**
| Column | Type | Description |
|--------|------|-------------|
| `route_id` | VARCHAR | GTFS route ID |
| `line_name` | VARCHAR | Short name (A, B, C, 1, 2, 3...) |
| `route_name` | VARCHAR | Full route name |
| `route_color` | VARCHAR | Hex color (e.g., 0039A6 for A/C/E) |
| `route_text_color` | VARCHAR | Text color for contrast |
| `line_group` | VARCHAR | Line family (8th Ave, Lexington, etc.) |

**station_routes.parquet**
| Column | Type | Description |
|--------|------|-------------|
| `station_id` | VARCHAR | Station complex ID |
| `station_name` | VARCHAR | Station name |
| `line_name` | VARCHAR | Line serving this station |
| `route_color` | VARCHAR | Line color |

**service_frequency.parquet**
| Column | Type | Description |
|--------|------|-------------|
| `station_id` | VARCHAR | Station ID |
| `station_name` | VARCHAR | Station name |
| `line_name` | VARCHAR | Subway line |
| `day_type` | VARCHAR | Weekday/Saturday/Sunday |
| `time_period` | VARCHAR | AM Peak/Midday/PM Peak/Evening/Night |
| `trips_in_period` | INTEGER | Scheduled trips |
| `trains_per_hour` | DOUBLE | Estimated frequency |

## GTFS Background

**What is GTFS?**
General Transit Feed Specification - a standard format for public transit schedules. Contains:
- Static data: Routes, stops, schedules (what we download)
- Real-time data: Live positions, delays (separate feed, not included)

**MTA GTFS Feeds:**
| Feed | URL | Contents |
|------|-----|----------|
| Subway | `web.mta.info/developers/data/nyct/subway/google_transit.zip` | All subway lines |
| Bus (Manhattan) | `web.mta.info/developers/data/nyct/bus/google_transit_manhattan.zip` | Manhattan buses |
| Bus (other) | Similar URLs for each borough | Borough buses |
| LIRR | `web.mta.info/developers/data/lirr/google_transit.zip` | Long Island Rail Road |
| Metro-North | `web.mta.info/developers/data/mnr/google_transit.zip` | Metro-North Railroad |

**GTFS File Reference:**

| File | Purpose | Key Fields |
|------|---------|------------|
| `stops.txt` | All stop locations | stop_id, stop_name, stop_lat, stop_lon, location_type, parent_station |
| `routes.txt` | Transit routes | route_id, route_short_name, route_color |
| `trips.txt` | Scheduled trips | trip_id, route_id, service_id, direction_id |
| `stop_times.txt` | Arrival/departure times | trip_id, stop_id, arrival_time, departure_time |
| `calendar.txt` | Service schedules | service_id, monday...sunday, start_date, end_date |
| `shapes.txt` | Route geometry | shape_id, shape_pt_lat, shape_pt_lon, shape_pt_sequence |
| `transfers.txt` | Transfer points | from_stop_id, to_stop_id, transfer_type, min_transfer_time |

**location_type in stops.txt:**
| Value | Meaning |
|-------|---------|
| 0 or blank | Platform (specific track) |
| 1 | Station (complex/parent) |
| 2 | Entrance/Exit |
| 3 | Generic node |
| 4 | Boarding area |

## Linking MTA Data to Citi Bike

Station matching between MTA subway and Citi Bike requires spatial joins since IDs are unrelated:

```sql
-- Find Citi Bike stations within 200m of subway entrances
SELECT
    cb.station_name as citibike_station,
    mta.station_name as subway_station,
    mta.line_name as subway_lines,
    ST_Distance_Sphere(
        ST_Point(cb.start_lon, cb.start_lat),
        ST_Point(mta.longitude, mta.latitude)
    ) as distance_m
FROM '../data/processed/*.parquet' cb
CROSS JOIN 'data/reference/entrances.parquet' mta
WHERE ST_Distance_Sphere(
    ST_Point(cb.start_lon, cb.start_lat),
    ST_Point(mta.longitude, mta.latitude)
) < 200
GROUP BY 1, 2, 3, 4
ORDER BY distance_m;
```

Or simpler with station centroids:
```sql
-- Match by proximity to station centroid
SELECT DISTINCT
    cb.start_station_name as citibike_station,
    mta.station_name as nearest_subway,
    ROUND(ST_Distance_Sphere(
        ST_Point(cb.start_lon, cb.start_lat),
        ST_Point(mta.longitude, mta.latitude)
    )) as distance_m
FROM (SELECT DISTINCT start_station_name, start_lat, start_lon
      FROM '../data/processed/*2024*.parquet') cb
CROSS JOIN 'data/reference/stations.parquet' mta
WHERE ST_Distance_Sphere(...) < 300
ORDER BY citibike_station, distance_m;
```

## Next Steps

1. Create `src/fetch_gtfs.py` script
2. Create `src/fetch_ridership.py` script
3. Create `src/build_reference.py` script
4. Download and process MTA data
5. Build spatial index for Citi Bike ↔ MTA station matching
6. Analyze first/last mile patterns
