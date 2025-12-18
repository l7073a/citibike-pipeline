# NYC Ferry & NY Waterway Data Integration

This subproject handles ferry ridership data for multi-modal transit analysis alongside Citi Bike and MTA subway data.

## Project Status (Updated Dec 10, 2024)

| Item | Status | Notes |
|------|--------|-------|
| NYC Ferry ridership data | ✅ Downloaded | 2.76M rows, July 2017 - Oct 2025 |
| NYC Ferry GTFS | ✅ Downloaded | 28 stops, 10 files |
| Private ferry data (NY Waterway) | ❌ **BLOCKED** | API returns "non-tabular table" error |
| Staten Island Ferry GTFS | ❌ **BLOCKED** | 404 error, URL broken |
| Exploratory analysis | ✅ Complete | See FINDINGS.md |
| ETL pipeline | ⏳ **NOT STARTED** | Needed to clean data |

---

## CRITICAL: Data Quality Issue Found

**30% of observations (822K rows) have ZERO boardings**

This is likely:
- Canceled ferry trips (boat didn't run)
- Data collection gaps
- Off-peak hours with no riders

**Impact**: Must filter/flag these for ridership analysis. An ETL pipeline is needed.

---

## Actual Schema (CORRECTED)

The API schema differs from documentation:

| Column | Type | Description |
|--------|------|-------------|
| `date` | TIMESTAMP | Date of service |
| `hour` | INTEGER | Hour (0-23, some missing) |
| `route` | VARCHAR | Route code (ER, AS, SB, etc.) |
| `direction` | VARCHAR | NB/SB/EB/WB |
| `stop` | VARCHAR | **NOT `landing`** - stop name |
| `boardings` | INTEGER | Passengers (30% are zero!) |
| `typeday` | VARCHAR | **NOT `day_of_week`** - Weekday/Weekend |

**Route Codes**:
| Code | Full Name | % of Ridership |
|------|-----------|----------------|
| ER | East River | 38.4% |
| AS | Astoria | 20.2% |
| SB | South Brooklyn | 11.8% |
| RW | Rockaway | 11.8% |
| SV | Soundview | 10.9% |
| SG | St. George | 4.6% |
| LE | Lower East Side | 1.2% (discontinued May 2020) |
| GI | Governors Island | 1.0% (seasonal) |
| RR | Rockaway Rocket | 0.1% |

---

## Key Findings from Exploratory Analysis

### Data Coverage
- **2.76M rows** covering July 2017 - October 2025 (8.3 years)
- **9 routes**, **28 stops**
- **48.3M total boardings** (after filtering zeros)
- One date gap detected

### Top Stops
1. Wall St/Pier 11: 10.8M boardings (22%)
2. East 34th Street: 6.5M (13%)
3. North Williamsburg: 3.3M (7%)

### Temporal Patterns
- **Peak hour**: 5 PM (afternoon rush, not morning!)
- **Peak season**: Summer (Aug 2025 = 1M+ boardings)
- **Weekends average 44% MORE boardings per trip** (tourists vs commuters)

### Growth Trend
- 2017 (6 months): ~2.2M boardings
- 2025 (10 months): ~6.5M boardings (on track for 8M annually)

---

## What Works

1. **`fetch_nyc_ferry.py`** - Downloads full ridership data
   - Handles NaN values properly
   - Pagination works (50K rows at a time)
   - Saves to parquet with metadata

2. **`fetch_gtfs.py`** - Downloads NYC Ferry GTFS
   - 10 files including stops, routes, schedules
   - Stop coordinates available for spatial analysis

3. **Data quality** - Generally clean except zero boardings

---

## What Doesn't Work

### 1. NY Waterway / Private Ferry Data
**Problem**: API returns `"no row or column access to non-tabular tables"`

```bash
curl "https://data.cityofnewyork.us/resource/hn6c-5qkb.json"
# Returns: {"error": true, "message": "no row or column access to non-tabular tables"}
```

**Workarounds to try**:
- Download CSV manually from web interface
- Check if there's an alternative endpoint
- Contact NYC Open Data support
- Look for NY Waterway's own data releases

### 2. Staten Island Ferry GTFS
**Problem**: 404 error on file download URL

The URL in the dataset metadata is broken. Need to:
- Find updated GTFS file URL
- Or download manually from NYC DOT

---

## ETL Pipeline Requirements

**Yes, we need a pipeline** - simpler than Citi Bike but essential.

### Why
1. **30% zero boardings** need filtering/flagging
2. **Route codes** need expansion (ER → "East River")
3. **GTFS integration** for stop coordinates
4. **Derived fields** (year, month, season, is_weekend)
5. **Weather join** capability
6. **Cross-modal analysis prep**

### Proposed Pipeline

```
Raw API Data (2.76M rows)
        ↓
[1. Quality Filters]
   - Flag/filter zero boardings
   - Remove invalid hours (-1)
        ↓
[2. Reference Joins]
   - GTFS stops → coordinates
   - Route codes → full names
        ↓
[3. Derived Fields]
   - year, month, day_of_week
   - is_weekend, is_holiday, season
   - time_period (AM_RUSH, PM_RUSH, etc.)
        ↓
[4. Output]
   - Parquet, partitioned by year
   - ~1.9M "valid" rows (after zero filter)
```

### Output Schema (Proposed)

```sql
-- Original fields
date, hour, route, direction, stop, boardings, typeday

-- From GTFS
stop_lat, stop_lon, route_name

-- Derived
year, month, day_of_week, is_weekend, is_holiday, season, time_period

-- Quality flags
is_valid_ridership  -- FALSE if boardings = 0
```

---

## Files Created This Session

```
ferry/
├── src/
│   ├── fetch_nyc_ferry.py      # ✅ Working
│   ├── fetch_ny_waterway.py    # Created, API broken
│   └── fetch_gtfs.py           # ✅ Partially working
├── data/
│   ├── nyc_ferry/
│   │   ├── ridership.parquet   # ✅ 2.76M rows
│   │   └── metadata.json       # ✅ Download info
│   └── gtfs/
│       └── nyc_ferry/          # ✅ 10 GTFS files
├── CLAUDE.md                   # This file
├── FINDINGS.md                 # Full analysis report
└── README.md                   # Quick start guide
```

---

## NEXT STEPS (After Compact)

### Priority 1: Get NY Waterway Data (BLOCKED)

1. **Try manual CSV download** from NYC Open Data web interface
2. **Search for alternative sources**:
   - NY Waterway website/press releases
   - Port Authority reports
   - Academic datasets
3. **If still blocked**: Document as unavailable, proceed with NYC Ferry only

### Priority 2: Build ETL Pipeline

1. **Create `src/pipeline.py`**:
   ```bash
   python src/pipeline.py  # Process raw → clean parquet
   ```

2. **Implement filters**:
   - `boardings > 0` for ridership analysis
   - `hour >= 0` for valid hours
   - Date range validation

3. **Add GTFS reference joins**:
   - Parse `data/gtfs/nyc_ferry/stops.txt`
   - Create `reference/stops.parquet` with coordinates
   - Join to ridership data

4. **Add derived fields**:
   - `year`, `month`, `day_of_week`
   - `is_weekend`, `season`
   - `time_period` (AM_RUSH = 7-9, PM_RUSH = 16-19, etc.)

### Priority 3: Reference Data

1. **Create `reference/route_names.csv`**:
   ```csv
   route,route_name,start_date,end_date
   ER,East River,2017-07-01,
   AS,Astoria,2017-08-01,
   ...
   ```

2. **Create `reference/stops.parquet`** from GTFS

3. **Build Citi Bike ↔ Ferry stop mapping**:
   - Find bike stations within 400m of ferry stops
   - Create crosswalk table

### Priority 4: Analysis

1. **Seasonal patterns** - summer vs winter
2. **Route performance** - which are growing?
3. **Cross-modal** - ferry + bike first/last mile
4. **Weather impact** - join with weather data

---

## Lessons Learned

1. **Always check actual API schema** - documentation was wrong (`stop` not `landing`)
2. **NYC Open Data APIs can be flaky** - private ferry data inaccessible
3. **Data quality varies** - 30% zeros is significant
4. **Ferry data is much simpler than Citi Bike** - no station ID crosswalk needed
5. **PM rush dominates** - ferries are afternoon-oriented (tourists + commuters going home)

---

## Quick Reference

### Download Commands
```bash
# NYC Ferry ridership (working)
python src/fetch_nyc_ferry.py

# NYC Ferry GTFS (working)
python src/fetch_gtfs.py --system nyc_ferry

# Private ferry (BROKEN)
python src/fetch_ny_waterway.py  # Will fail
```

### Query Examples
```sql
-- Total ridership by route (excluding zeros)
SELECT route, SUM(boardings) as total
FROM 'data/nyc_ferry/ridership.parquet'
WHERE boardings > 0
GROUP BY route ORDER BY total DESC;

-- Monthly trends
SELECT YEAR(date) as year, MONTH(date) as month, SUM(boardings)
FROM 'data/nyc_ferry/ridership.parquet'
WHERE boardings > 0
GROUP BY 1, 2 ORDER BY 1, 2;
```

---

## Session History

### Dec 10, 2024 - Session 1: Research & Initial Downloads
- Created ferry subproject structure
- Researched data sources (NYC Ferry, NY Waterway, Staten Island)
- Created download scripts
- **Downloaded 2.76M rows of NYC Ferry data**
- **Downloaded NYC Ferry GTFS (28 stops)**
- **Discovered 30% zero boardings issue**
- Completed exploratory analysis
- Created FINDINGS.md with full analysis
- **Blocked on**: NY Waterway API, Staten Island GTFS

---

**Parent Project**: [Citi Bike Pipeline](../CLAUDE.md)
**Related Subproject**: [MTA Subway Data](../mta/CLAUDE.md)
**Full Analysis**: [FINDINGS.md](FINDINGS.md)
