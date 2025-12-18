# NYC Ferry Data: Exploratory Analysis & ETL Recommendations

**Date**: December 10, 2024
**Analysis Period**: July 2017 - October 2025
**Dataset**: NYC Ferry Hourly Ridership (Socrata API)

---

## Executive Summary

Successfully downloaded and analyzed **2.76M rows** of NYC Ferry ridership data spanning **8.3 years**. The data reveals strong seasonal patterns, afternoon rush hour peaks, and **significant data quality issues** requiring an ETL pipeline.

**Key Finding**: **30% of observations have zero boardings** - this needs investigation and potentially filtering in an ETL pipeline.

---

## 1. Data Download Results

### ✅ Successfully Downloaded

| Dataset | Status | Records | Date Range |
|---------|--------|---------|------------|
| **NYC Ferry Ridership** | ✅ Complete | 2,760,654 | July 2017 - Oct 2025 |
| **NYC Ferry GTFS** | ✅ Complete | 10 files, 28 stops | Current schedules |
| **Private Ferry Counts** | ❌ API Error | N/A | "Non-tabular table" error |
| **Staten Island Ferry GTFS** | ❌ 404 Error | N/A | URL broken |

### Scripts Created

- `fetch_nyc_ferry.py` - ✅ Working, handles NaN values
- `fetch_ny_waterway.py` - Created but API inaccessible
- `fetch_gtfs.py` - ✅ Partially working (NYC Ferry only)

---

## 2. Data Coverage

**Timeline**: July 1, 2017 → October 31, 2025 (3,044 days = 8.3 years)

**Date Gap Found**: 1 date gap detected in continuous service

**Route Evolution**:
- **2017-07**: Launch with 3 routes (ER, SB, RW)
- **2017-08**: Astoria (AS) route added
- **2018-06**: Governors Island (GI) route
- **2018-08**: Soundview (SV) and Lower East Side (LE) routes
- **2021-08**: St. George (SG) route
- **2022-07**: Rockaway Rocket (RR) route

---

## 3. Data Quality Issues

### Critical Issue: Zero Boardings

**Problem**: 822,678 rows (29.8%) have `boardings = 0`

**Potential Causes**:
1. **Canceled trips** - Ferry didn't run but observation was logged
2. **No-show trips** - Ferry ran but nobody boarded
3. **Data collection errors** - Missing or failed readings
4. **Off-peak hours** - Legitimate zero ridership during very early/late hours

**Recommendation**:
- **Filter out for ridership analysis** (count calculations)
- **Keep for operational analysis** (service frequency, cancellations)
- **Add `is_valid_ridership` flag** in ETL pipeline

### Minor Issues

| Issue | Count | % | Recommendation |
|-------|-------|---|----------------|
| Missing/invalid hour | 56 | 0.00% | Filter or impute from adjacent observations |
| NULL route | 0 | 0% | No action needed |
| NULL stop | 0 | 0% | No action needed |
| NULL direction | 0 | 0% | No action needed |

**Overall Assessment**: Data is generally clean except for the zero boardings issue.

---

## 4. Route Analysis

### Ridership by Route (2017-2025)

| Route | Total Boardings | % of Total | Observations | Operational Period |
|-------|----------------|------------|--------------|-------------------|
| **ER** (East River) | 18,539,609 | 38.4% | 514,456 | Jul 2017 - Present |
| **AS** (Astoria) | 9,779,835 | 20.2% | 449,680 | Aug 2017 - Present |
| **SB** (South Brooklyn) | 5,708,527 | 11.8% | 378,090 | Jul 2017 - Present |
| **RW** (Rockaway) | 5,701,038 | 11.8% | 162,859 | Jul 2017 - Present |
| **SV** (Soundview) | 5,272,060 | 10.9% | 286,153 | Aug 2018 - Present |
| **SG** (St. George) | 2,230,655 | 4.6% | 85,645 | Aug 2021 - Present |
| **LE** (Lower East Side) | 563,882 | 1.2% | 55,008 | Aug 2018 - May 2020 |
| **GI** (Governors Island) | 483,794 | 1.0% | 5,706 | Jun 2018 - Present |
| **RR** (Rockaway Rocket) | 41,735 | 0.1% | 379 | Jul 2022 - Present |

**Key Insights**:
- **East River route dominates**: 38% of all ridership
- **Top 3 routes** (ER, AS, SB) account for **70% of total ridership**
- **Lower East Side route discontinued** in May 2020 (possibly COVID-related)
- **Governors Island** has very sparse observations (seasonal/weekend-only service)

---

## 5. Stop Analysis

### Top 10 Busiest Stops

| Rank | Stop | Total Boardings | % of Total | Observations |
|------|------|----------------|------------|--------------|
| 1 | Wall St/Pier 11 | 10,802,133 | 22.4% | 243,933 |
| 2 | East 34th Street | 6,497,418 | 13.4% | 242,537 |
| 3 | North Williamsburg | 3,259,083 | 6.7% | 92,728 |
| 4 | Dumbo/Fulton Ferry | 2,597,466 | 5.4% | 72,591 |
| 5 | Rockaway | 2,548,512 | 5.3% | 45,645 |
| 6 | East 90th St | 1,988,875 | 4.1% | 96,762 |
| 7 | Dumbo/BBP Pier 1 | 1,923,275 | 4.0% | 73,478 |
| 8 | Long Island City | 1,881,649 | 3.9% | 95,931 |
| 9 | Hunters Point South | 1,838,428 | 3.8% | 59,841 |
| 10 | Astoria | 1,780,476 | 3.7% | 71,036 |

**Geographic Patterns**:
- **Manhattan hubs**: Wall St and E 34th St are by far the busiest (36% combined)
- **Brooklyn waterfront**: North Williamsburg and Dumbo are major destinations
- **Queens stops**: LIC, Hunters Point, Astoria serve commuters
- **Rockaway**: Beach destination with strong seasonal ridership

---

## 6. Temporal Patterns

### Seasonal Ridership

**Peak Season**: Summer (June-September)
- **Aug 2025**: 1,014,828 boardings (highest month on record)
- **Jul 2025**: 909,733 boardings

**Low Season**: Winter (December-February)
- **Jan 2018**: 155,681 boardings (early system)
- **Feb 2025**: 309,627 boardings

**Growth Trend**: Strong year-over-year growth
- **2017** (6 months): ~2.2M boardings
- **2025** (10 months): ~6.5M boardings (on track for ~8M annually)

### Hourly Patterns

**Peak Hours** (all-time totals):
| Hour | Total Boardings | Avg per Observation |
|------|----------------|---------------------|
| 5 PM | 5,404,534 | 39.4 |
| 4 PM | 4,864,883 | 35.6 |
| 6 PM | 4,357,723 | 33.2 |
| 3 PM | 4,174,668 | 30.8 |
| 2 PM | 3,708,608 | 27.7 |

**Pattern**: Clear afternoon/evening rush hour peak (3-6 PM). This suggests:
- **Commuter-oriented service** - people returning from Manhattan
- **Tourist usage** - afternoon sightseeing trips
- **Limited morning rush** - ferries not primary commute mode

### Day Type Patterns

| Day Type | Total Boardings | Avg per Observation | Interpretation |
|----------|----------------|---------------------|----------------|
| **Weekday** | 32,193,381 | 22.4 | More frequent service, lower per-trip ridership |
| **Weekend** | 16,127,754 | 32.2 | Less frequent service, higher per-trip ridership |

**Surprising Finding**: Weekend trips average **44% more boardings** than weekday trips!

**Possible Explanations**:
1. **Tourist demand** - Weekends attract leisure riders
2. **Reduced service frequency** - Fewer weekend trips = more crowded boats
3. **Event-driven ridership** - Governors Island, Rockaway beaches, etc.

---

## 7. ETL Pipeline Recommendations

### Yes, We Need an ETL Pipeline

**Reasons**:
1. **30% of data needs filtering/flagging** (zero boardings)
2. **Route codes need expansion** (ER → "East River Ferry")
3. **GTFS integration needed** (stop coordinates, route details)
4. **Date-derived fields required** (year, month, day of week, is_weekend, season)
5. **Weather integration** (similar to Citi Bike pipeline)
6. **Cross-modal analysis prep** (join keys for Citi Bike, MTA)

### Proposed ETL Pipeline Architecture

Similar to Citi Bike pipeline but simpler (no station ID crosswalk needed):

```
Raw Socrata API Data
        ↓
[1. Ingest & Type Conversion]
        ↓
[2. Data Quality Filters]
   - Remove/flag zero boardings
   - Filter invalid hours
   - Validate date ranges
        ↓
[3. Reference Data Joins]
   - GTFS stops (coordinates, full names)
   - GTFS routes (full route names)
        ↓
[4. Derived Fields]
   - Extract year, month, day_of_week
   - Add is_weekend, is_holiday
   - Add season (spring/summer/fall/winter)
   - Calculate time_period (AM_RUSH, MIDDAY, PM_RUSH, EVENING)
        ↓
[5. Output to Parquet]
   - Partitioned by year/month
   - Compressed, columnar format
```

### Schema Proposal: Processed Ferry Data

```sql
CREATE TABLE processed_ferry_ridership (
    -- Original fields (cleaned)
    date DATE,
    hour INTEGER,
    route_code VARCHAR(4),
    direction VARCHAR(2),
    stop_code VARCHAR(50),
    boardings INTEGER,
    typeday VARCHAR(10),

    -- Reference data (from GTFS)
    route_name VARCHAR(100),
    stop_name VARCHAR(100),
    stop_lat DOUBLE,
    stop_lon DOUBLE,

    -- Derived date/time fields
    year INTEGER,
    month INTEGER,
    day_of_week INTEGER,
    week_of_year INTEGER,
    is_weekend BOOLEAN,
    is_holiday BOOLEAN,
    season VARCHAR(10),
    time_period VARCHAR(20),

    -- Data quality flags
    is_valid_ridership BOOLEAN,  -- FALSE if boardings = 0
    has_valid_hour BOOLEAN,       -- FALSE if hour = -1

    -- Metadata
    processed_at TIMESTAMP
);
```

### Recommended Filters for ETL

| Filter | Threshold | Rationale |
|--------|-----------|-----------|
| Zero boardings | `boardings > 0` | For ridership analysis only |
| Invalid hour | `hour >= 0 AND hour <= 23` | Remove data quality errors |
| Valid date range | `date >= '2017-07-01'` | System launch date |

**Keep Zero Boardings for**:
- Service reliability analysis
- Cancellation tracking
- Operational metrics

---

## 8. Integration Opportunities

### Ferry ↔ Citi Bike Analysis

**Questions to Answer**:
1. **First/last mile**: Do Citi Bike trips cluster near ferry terminals?
2. **Temporal correlation**: Do ferry arrivals predict Citi Bike departures?
3. **Weather sensitivity**: Which mode is more affected by rain/cold?
4. **Seasonal patterns**: Do both peak in summer?
5. **Route competition**: Williamsburg Bridge (bike) vs. East River Ferry?

**Spatial Matching Needed**:
- Find Citi Bike stations within 400m of ferry stops
- Use DuckDB spatial functions (`ST_Distance_Sphere`)

### Ferry ↔ MTA Subway Analysis

**Questions**:
1. **Commute substitution**: Ferry as alternative to crowded L train?
2. **Transit deserts**: Do ferries serve areas with poor subway access?
3. **Cross-modal trips**: Ferry + subway for longer commutes?

---

## 9. Data Gaps & Limitations

### Missing Data

| Dataset | Status | Impact |
|---------|--------|--------|
| **Private ferry counts** (NY Waterway) | ❌ API inaccessible | Can't compare public vs private ferry usage |
| **Staten Island Ferry** | ❌ GTFS broken | Can't include free ferry in analysis |
| **Real-time data** | Not downloaded | Can't analyze delays, cancellations |

### Schema Limitations

| Issue | Description | Workaround |
|-------|-------------|------------|
| **No trip_id** | Can't track individual ferry trips | Infer from route + direction + hour |
| **No capacity data** | Don't know boat size | Estimate from peak boardings |
| **No fare data** | Don't know revenue | Assume $4/ride standard fare |
| **Aggregate hourly data** | Can't see sub-hour patterns | Accept hourly granularity |

### Date Gap

One date gap was detected in continuous service. Need to investigate:
- Which date(s) are missing?
- Was service actually suspended?
- Is this a data collection error?

---

## 10. Next Steps

### Immediate Actions

1. **Build ETL pipeline** (similar to Citi Bike)
   - Script: `ferry/src/process_ferry_data.py`
   - Output: `ferry/data/processed/*.parquet`

2. **Integrate GTFS reference data**
   - Parse `stops.txt` for coordinates
   - Parse `routes.txt` for full route names
   - Create `ferry/reference/` directory

3. **Investigate zero boardings**
   - Query: Which hours/routes have most zeros?
   - Check: Correlation with weather, holidays?
   - Decide: Filter or flag?

4. **Document route codes**
   - Create mapping: ER → "East River Ferry"
   - Add to `ferry/reference/route_names.csv`

### Analysis Projects

1. **Seasonal deep dive**
   - Compare summer vs winter ridership
   - Weather impact analysis
   - Holiday ridership patterns

2. **Route performance**
   - Which routes are growing?
   - Which stops underperform?
   - Optimal frequency analysis

3. **Cross-modal integration**
   - Ferry + Citi Bike first/last mile
   - Ferry + subway connections
   - Multi-modal commute patterns

### Documentation Updates

1. Update `ferry/CLAUDE.md` with:
   - Correct schema (stop, typeday, not landing, day_of_week)
   - Data quality findings
   - ETL pipeline design

2. Create `ferry/reference/README.md`:
   - Route code mappings
   - Stop name standardization
   - Data quality notes

---

## 11. Conclusion

The NYC Ferry dataset is **high quality** with good temporal coverage (8.3 years) and reasonable data integrity. The main data quality issue (30% zero boardings) is addressable with filtering/flagging in an ETL pipeline.

**Key Takeaways**:
- ✅ Strong seasonal patterns (2-3x ridership swing)
- ✅ Clear commuter orientation (PM rush peak)
- ✅ Weekend tourist demand (higher avg boardings)
- ✅ Growth trajectory (1M+ monthly boardings in 2025)
- ⚠️ Need ETL pipeline for zero boardings handling
- ⚠️ Need GTFS integration for full stop metadata
- ⚠️ Private ferry data unavailable (API broken)

**Recommendation**: Proceed with building a **simplified ETL pipeline** modeled after the Citi Bike pipeline but without the complexity of station ID crosswalks. Focus on data quality filtering, reference data integration, and derived field generation.

---

**Analysis by**: Claude Code
**Generated**: December 10, 2024
**Data Source**: NYC Open Data (Socrata API)
