# Ferry Data Subproject

Download and analyze NYC ferry ridership data for multi-modal transit analysis.

## Quick Start

```bash
# 1. Download NYC Ferry ridership data (hourly, 2017-present)
python src/fetch_nyc_ferry.py

# 2. Download private ferry monthly counts (NY Waterway, etc.)
python src/fetch_ny_waterway.py

# 3. Download ferry GTFS feeds (schedules, station locations)
python src/fetch_gtfs.py
```

## Scripts

### fetch_nyc_ferry.py

Download NYC Ferry ridership data (hourly boardings by landing).

**Coverage**: June 2017 - present
**Granularity**: Hourly boardings by date, route, landing
**Output**: `data/nyc_ferry/ridership.parquet`

```bash
# Download all data
python src/fetch_nyc_ferry.py

# Download specific date range
python src/fetch_nyc_ferry.py --start 2024-01-01 --end 2024-12-31

# Download specific route
python src/fetch_nyc_ferry.py --route "Astoria"
```

### fetch_ny_waterway.py

Download private ferry monthly passenger counts (NY Waterway, SeaStreak, etc.).

**Coverage**: 2015+ (varies by operator)
**Granularity**: Monthly totals by operator
**Output**: `data/ny_waterway/monthly_counts.parquet`

```bash
# Download all data
python src/fetch_ny_waterway.py

# Download specific date range
python src/fetch_ny_waterway.py --start 2024-01 --end 2024-12

# Download specific operator
python src/fetch_ny_waterway.py --operator "NY Waterway"
```

### fetch_gtfs.py

Download ferry GTFS feeds (schedule and station location data).

**Systems**:
- NYC Ferry (Astoria, Rockaway, Soundview, etc.)
- Staten Island Ferry

**Output**: `data/gtfs/{system}/`

```bash
# Download all GTFS feeds
python src/fetch_gtfs.py

# Download specific system
python src/fetch_gtfs.py --system nyc_ferry
python src/fetch_gtfs.py --system staten_island
```

## Data Structure

```
ferry/
├── src/
│   ├── fetch_nyc_ferry.py      # NYC Ferry ridership
│   ├── fetch_ny_waterway.py    # Private ferry counts
│   └── fetch_gtfs.py           # GTFS feeds
├── data/
│   ├── nyc_ferry/
│   │   ├── ridership.parquet   # Hourly boardings
│   │   └── metadata.json       # Download info
│   ├── ny_waterway/
│   │   ├── monthly_counts.parquet
│   │   └── metadata.json
│   └── gtfs/
│       ├── nyc_ferry/
│       │   ├── routes.txt      # Ferry routes
│       │   ├── stops.txt       # Landing locations
│       │   └── ...
│       └── staten_island/
│           └── ...
├── CLAUDE.md                   # Session notes & documentation
└── README.md                   # This file
```

## Dependencies

```bash
pip install pandas pyarrow requests
```

## Documentation

See [CLAUDE.md](CLAUDE.md) for:
- Data source details and schemas
- API access patterns
- Analysis use cases
- Cross-modal research questions

## Related Projects

- **Main project**: [Citi Bike Pipeline](../CLAUDE.md)
- **Related**: [MTA Subway Data](../mta/CLAUDE.md)

## Data Sources

- **NYC Ferry**: https://data.cityofnewyork.us/Transportation/NYC-Ferry-Ridership/t5n6-gx8c
- **Private Ferries**: https://data.cityofnewyork.us/Transportation/Private-Ferry-Monthly-Passenger-Counts/hn6c-5qkb
- **NYC Ferry GTFS**: https://www.ferry.nyc/developer-tools/
- **Staten Island Ferry GTFS**: https://data.cityofnewyork.us/Transportation/Staten-Island-Ferry-Schedule-General-Transit-Feed-/b57i-ri22
