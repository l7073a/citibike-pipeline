#!/usr/bin/env python3
"""
Main ETL pipeline for Citi Bike trip data.

Reads raw CSVs, normalizes schemas, resolves stations via crosswalk,
and outputs clean Parquet files.

Schema normalization:
- Legacy (2013-2020): tripduration, starttime, usertype, birth year, gender
- Modern (2021+): ride_id, rideable_type, started_at, member_casual

Station resolution:
- Modern IDs (UUIDs with dashes): Join directly to current_stations
- Legacy IDs (integers): Join to crosswalk → then to current_stations
- Ghost stations: Use legacy coordinates from crosswalk

Demographics validity flags (added to output):
- birth_year_valid: TRUE if birth_year is usable for analysis
    - FALSE if 1969 default (casual riders 2018+), age <10, or age >100
    - NULL for modern data (2020+) where birth_year doesn't exist
- gender_valid: TRUE if gender is known (1=male, 2=female)
    - FALSE if gender=0 (unknown)
    - NULL for modern data (2020+) where gender doesn't exist
- age_at_trip: Pre-calculated age at time of trip
    - NULL if birth_year_valid is FALSE or NULL
"""

import argparse
import csv
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple

try:
    import duckdb
except ImportError:
    print("Missing dependency: duckdb")
    print("Run: pip install duckdb")
    exit(1)


def extract_expected_month(filename: str) -> Tuple[Optional[int], Optional[int]]:
    """
    Extract expected year and month from filename.
    Returns (year, month) or (None, None) if not found.

    Examples:
        '201409-citibike-tripdata.csv' -> (2014, 9)
        '2014-citibike-tripdata_201401-citibike-tripdata_1.csv' -> (2014, 1)
    """
    # Look for YYYYMM pattern
    match = re.search(r'(\d{4})(\d{2})-citibike', filename)
    if match:
        return int(match.group(1)), int(match.group(2))

    # Fallback: look for any YYYYMM pattern
    match = re.search(r'(\d{4})(\d{2})', filename)
    if match:
        year, month = int(match.group(1)), int(match.group(2))
        if 2013 <= year <= 2030 and 1 <= month <= 12:
            return year, month

    return None, None

REFERENCE_DIR = Path(__file__).parent.parent / "reference"
DATA_DIR = Path(__file__).parent.parent / "data"
LOGS_DIR = Path(__file__).parent.parent / "logs"

# Test/internal station patterns to filter out
# These are depot locations, test kiosks, valet services, and internal operations stations
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

def is_test_station(name: str) -> bool:
    """Check if a station name matches test/internal patterns."""
    if not name:
        return False
    name_lower = name.lower()
    return any(pattern in name_lower for pattern in TEST_STATION_PATTERNS)

def get_test_station_sql_filter() -> str:
    """Generate SQL filter to exclude test stations."""
    conditions = []
    for pattern in TEST_STATION_PATTERNS:
        # Escape single quotes for SQL
        escaped = pattern.replace("'", "''")
        conditions.append(f"LOWER(start_station_name) NOT LIKE '%{escaped}%'")
        conditions.append(f"LOWER(end_station_name) NOT LIKE '%{escaped}%'")
    return " AND ".join(conditions)


def detect_schema(csv_path: Path) -> str:
    """Detect which schema a CSV file uses."""
    with open(csv_path, 'r') as f:
        header = f.readline().strip()

    header_lower = header.lower()

    if 'ride_id' in header_lower or 'member_casual' in header_lower:
        return 'modern'
    elif 'Trip Duration' in header:  # Title Case variant
        return 'legacy_titlecase'
    elif 'tripduration' in header_lower:
        return 'legacy'
    else:
        # Try to infer from content
        return 'unknown'


def load_reference_tables(con: duckdb.DuckDBPyConnection, ref_dir: Path):
    """Load station reference tables into DuckDB."""
    
    # Current stations from GBFS
    stations_path = ref_dir / "current_stations.csv"
    if stations_path.exists():
        con.execute(f"""
            CREATE OR REPLACE TABLE current_stations AS 
            SELECT * FROM read_csv_auto('{stations_path}')
        """)
        print(f"  Loaded current_stations: {con.execute('SELECT COUNT(*) FROM current_stations').fetchone()[0]} rows")
    
    # Station crosswalk
    crosswalk_path = ref_dir / "station_crosswalk.csv"
    if crosswalk_path.exists():
        con.execute(f"""
            CREATE OR REPLACE TABLE crosswalk AS 
            SELECT * FROM read_csv_auto('{crosswalk_path}')
        """)
        print(f"  Loaded crosswalk: {con.execute('SELECT COUNT(*) FROM crosswalk').fetchone()[0]} rows")
    
    # Manual overrides (merge into crosswalk)
    overrides_path = ref_dir / "manual_overrides.csv"
    if overrides_path.exists():
        override_count = con.execute(f"""
            SELECT COUNT(*) FROM read_csv_auto('{overrides_path}')
        """).fetchone()[0]
        
        if override_count > 0:
            con.execute(f"""
                CREATE OR REPLACE TABLE crosswalk AS
                SELECT * FROM crosswalk
                WHERE legacy_id NOT IN (SELECT legacy_id FROM read_csv_auto('{overrides_path}'))
                UNION ALL
                SELECT * FROM read_csv_auto('{overrides_path}')
            """)
            print(f"  Applied {override_count} manual overrides")


def process_file(
    con: duckdb.DuckDBPyConnection,
    csv_path: Path,
    output_dir: Path
) -> dict:
    """Process a single CSV file and return stats."""

    schema = detect_schema(csv_path)
    output_path = output_dir / f"{csv_path.stem}.parquet"

    # Extract expected year/month from filename for date sanity check
    expected_year, expected_month = extract_expected_month(csv_path.name)

    stats = {
        'input_file': csv_path.name,
        'schema': schema,
        'expected_year': expected_year,
        'expected_month': expected_month,
        'rows_in': 0,
        'rows_out': 0,
        'rows_filtered': {
            'missing_station': 0,
            'duration_too_short': 0,
            'duration_too_long': 0,
            'invalid_timestamp': 0,
            'wrong_month': 0,  # New: dates that don't match expected month
        },
        'station_match': {'direct': 0, 'crosswalk': 0, 'ghost': 0, 'unmatched': 0},
        'date_sanity': {
            'dates_in_expected_month': 0,
            'dates_outside_expected_month': 0,
        },
    }
    
    # Build schema-specific SELECT
    # Note: We standardize on "lon" (not "lng") for longitude columns
    if schema == 'modern':
        select_clause = """
            ride_id,
            rideable_type,
            started_at::TIMESTAMP as started_at,
            ended_at::TIMESTAMP as ended_at,
            CAST(EPOCH(ended_at::TIMESTAMP - started_at::TIMESTAMP) AS INTEGER) as duration_sec,
            CAST(start_station_id AS VARCHAR) as start_station_id_raw,
            start_station_name as start_station_name_raw,
            start_lat as start_lat_raw,
            start_lng as start_lon_raw,
            CAST(end_station_id AS VARCHAR) as end_station_id_raw,
            end_station_name as end_station_name_raw,
            end_lat as end_lat_raw,
            end_lng as end_lon_raw,
            member_casual,
            NULL::VARCHAR as bike_id,
            NULL::INTEGER as birth_year,
            NULL::INTEGER as gender
        """
    elif schema == 'legacy':
        # Lowercase column names (most 2014-2020 data)
        # Handle multiple datetime formats: YYYY-MM-DD HH:MM:SS and M/D/YYYY HH:MM:SS
        select_clause = """
            -- Generate synthetic ride_id from row data
            MD5(CONCAT(
                COALESCE(starttime, ''),
                COALESCE(CAST("start station id" AS VARCHAR), ''),
                COALESCE(CAST(bikeid AS VARCHAR), '')
            )) as ride_id,
            NULL::VARCHAR as rideable_type,
            COALESCE(
                TRY_STRPTIME(CAST(starttime AS VARCHAR), '%Y-%m-%d %H:%M:%S'),
                TRY_STRPTIME(CAST(starttime AS VARCHAR), '%Y-%m-%d %H:%M:%S.%g'),
                TRY_STRPTIME(CAST(starttime AS VARCHAR), '%Y-%m-%d %H:%M:%S.%f'),
                TRY_STRPTIME(CAST(starttime AS VARCHAR), '%m/%d/%Y %H:%M:%S'),
                TRY_STRPTIME(CAST(starttime AS VARCHAR), '%m/%d/%Y %H:%M')
            ) as started_at,
            COALESCE(
                TRY_STRPTIME(CAST(stoptime AS VARCHAR), '%Y-%m-%d %H:%M:%S'),
                TRY_STRPTIME(CAST(stoptime AS VARCHAR), '%Y-%m-%d %H:%M:%S.%g'),
                TRY_STRPTIME(CAST(stoptime AS VARCHAR), '%Y-%m-%d %H:%M:%S.%f'),
                TRY_STRPTIME(CAST(stoptime AS VARCHAR), '%m/%d/%Y %H:%M:%S'),
                TRY_STRPTIME(CAST(stoptime AS VARCHAR), '%m/%d/%Y %H:%M')
            ) as ended_at,
            tripduration::INTEGER as duration_sec,
            REGEXP_REPLACE(CAST("start station id" AS VARCHAR), '\\.0$', '') as start_station_id_raw,
            "start station name" as start_station_name_raw,
            TRY_CAST("start station latitude" AS DOUBLE) as start_lat_raw,
            TRY_CAST("start station longitude" AS DOUBLE) as start_lon_raw,
            REGEXP_REPLACE(CAST("end station id" AS VARCHAR), '\\.0$', '') as end_station_id_raw,
            "end station name" as end_station_name_raw,
            TRY_CAST("end station latitude" AS DOUBLE) as end_lat_raw,
            TRY_CAST("end station longitude" AS DOUBLE) as end_lon_raw,
            CASE
                WHEN usertype = 'Subscriber' THEN 'member'
                WHEN usertype = 'Customer' THEN 'casual'
                ELSE usertype
            END as member_casual,
            CAST(bikeid AS VARCHAR) as bike_id,
            TRY_CAST("birth year" AS INTEGER) as birth_year,
            TRY_CAST(gender AS INTEGER) as gender
        """
    elif schema == 'legacy_titlecase':
        # Title Case column names (some older data)
        select_clause = """
            -- Generate synthetic ride_id from row data
            MD5(CONCAT(
                COALESCE("Start Time", ''),
                COALESCE(CAST("Start Station ID" AS VARCHAR), ''),
                COALESCE(CAST("Bike ID" AS VARCHAR), '')
            )) as ride_id,
            NULL::VARCHAR as rideable_type,
            "Start Time"::TIMESTAMP as started_at,
            "Stop Time"::TIMESTAMP as ended_at,
            "Trip Duration"::INTEGER as duration_sec,
            REGEXP_REPLACE(CAST("Start Station ID" AS VARCHAR), '\\.0$', '') as start_station_id_raw,
            "Start Station Name" as start_station_name_raw,
            "Start Station Latitude"::DOUBLE as start_lat_raw,
            "Start Station Longitude"::DOUBLE as start_lon_raw,
            REGEXP_REPLACE(CAST("End Station ID" AS VARCHAR), '\\.0$', '') as end_station_id_raw,
            "End Station Name" as end_station_name_raw,
            "End Station Latitude"::DOUBLE as end_lat_raw,
            "End Station Longitude"::DOUBLE as end_lon_raw,
            CASE
                WHEN "User Type" = 'Subscriber' THEN 'member'
                WHEN "User Type" = 'Customer' THEN 'casual'
                ELSE "User Type"
            END as member_casual,
            CAST("Bike ID" AS VARCHAR) as bike_id,
            TRY_CAST("Birth Year" AS INTEGER) as birth_year,
            TRY_CAST("Gender" AS INTEGER) as gender
        """
    else:
        print(f"    ⚠ Unknown schema, skipping")
        return stats
    
    # Determine read options based on schema
    # For legacy schema, force timestamp columns to VARCHAR to prevent mis-parsing
    if schema in ('legacy', 'legacy_titlecase'):
        read_options = "ignore_errors=true, types={'starttime': 'VARCHAR', 'stoptime': 'VARCHAR', 'Start Time': 'VARCHAR', 'Stop Time': 'VARCHAR'}"
    else:
        read_options = "ignore_errors=true"

    # Build date sanity check clause if we have expected year/month
    if expected_year and expected_month:
        date_sanity_filter = f"""
      AND EXTRACT(YEAR FROM started_at) = {expected_year}
      AND EXTRACT(MONTH FROM started_at) = {expected_month}"""
    else:
        date_sanity_filter = ""

    # Build test station filter
    test_station_filter = f"""
      AND {get_test_station_sql_filter()}"""

    # Main transformation query with station resolution
    query = f"""
    WITH raw AS (
        SELECT {select_clause}
        FROM read_csv_auto('{csv_path}', {read_options})
    ),
    -- Classify station IDs as modern (UUID) or legacy (integer)
    classified AS (
        SELECT *,
            CASE WHEN start_station_id_raw LIKE '%-%' THEN 'modern' ELSE 'legacy' END as start_id_type,
            CASE WHEN end_station_id_raw LIKE '%-%' THEN 'modern' ELSE 'legacy' END as end_id_type
        FROM raw
    ),
    -- Resolve start stations
    with_start AS (
        SELECT c.*,
            CASE
                -- Modern ID: direct lookup
                WHEN c.start_id_type = 'modern' THEN c.start_station_id_raw
                -- Legacy ID: use crosswalk
                WHEN cw.modern_id IS NOT NULL AND cw.modern_id != '' THEN cw.modern_id
                -- Ghost station: keep legacy ID
                ELSE c.start_station_id_raw
            END as start_station_id,
            CASE
                WHEN c.start_id_type = 'modern' THEN COALESCE(cs.name, c.start_station_name_raw)
                WHEN cw.modern_id IS NOT NULL AND cw.modern_id != '' THEN COALESCE(cs2.name, cw.modern_name)
                ELSE COALESCE(cw.legacy_name, c.start_station_name_raw)
            END as start_station_name,
            CASE
                WHEN c.start_id_type = 'modern' THEN COALESCE(cs.lat, c.start_lat_raw)
                WHEN cw.modern_id IS NOT NULL AND cw.modern_id != '' THEN COALESCE(cs2.lat, cw.legacy_lat)
                ELSE COALESCE(cw.legacy_lat, c.start_lat_raw)
            END as start_lat,
            CASE
                WHEN c.start_id_type = 'modern' THEN COALESCE(cs.lon, c.start_lon_raw)
                WHEN cw.modern_id IS NOT NULL AND cw.modern_id != '' THEN COALESCE(cs2.lon, cw.legacy_lon)
                ELSE COALESCE(cw.legacy_lon, c.start_lon_raw)
            END as start_lon,
            CASE
                WHEN c.start_id_type = 'modern' AND cs.station_id IS NOT NULL THEN 'direct'
                WHEN cw.modern_id IS NOT NULL AND cw.modern_id != '' THEN 'crosswalk'
                WHEN cw.legacy_id IS NOT NULL THEN 'ghost'
                ELSE 'unmatched'
            END as start_match_type
        FROM classified c
        LEFT JOIN crosswalk cw ON c.start_station_id_raw = cw.legacy_id
        LEFT JOIN current_stations cs ON c.start_station_id_raw = cs.station_id
        LEFT JOIN current_stations cs2 ON cw.modern_id = cs2.station_id
    ),
    -- Resolve end stations (similar logic)
    with_end AS (
        SELECT w.*,
            CASE
                WHEN w.end_id_type = 'modern' THEN w.end_station_id_raw
                WHEN cw.modern_id IS NOT NULL AND cw.modern_id != '' THEN cw.modern_id
                ELSE w.end_station_id_raw
            END as end_station_id,
            CASE
                WHEN w.end_id_type = 'modern' THEN COALESCE(cs.name, w.end_station_name_raw)
                WHEN cw.modern_id IS NOT NULL AND cw.modern_id != '' THEN COALESCE(cs2.name, cw.modern_name)
                ELSE COALESCE(cw.legacy_name, w.end_station_name_raw)
            END as end_station_name,
            CASE
                WHEN w.end_id_type = 'modern' THEN COALESCE(cs.lat, w.end_lat_raw)
                WHEN cw.modern_id IS NOT NULL AND cw.modern_id != '' THEN COALESCE(cs2.lat, cw.legacy_lat)
                ELSE COALESCE(cw.legacy_lat, w.end_lat_raw)
            END as end_lat,
            CASE
                WHEN w.end_id_type = 'modern' THEN COALESCE(cs.lon, w.end_lon_raw)
                WHEN cw.modern_id IS NOT NULL AND cw.modern_id != '' THEN COALESCE(cs2.lon, cw.legacy_lon)
                ELSE COALESCE(cw.legacy_lon, w.end_lon_raw)
            END as end_lon,
            CASE
                WHEN w.end_id_type = 'modern' AND cs.station_id IS NOT NULL THEN 'direct'
                WHEN cw.modern_id IS NOT NULL AND cw.modern_id != '' THEN 'crosswalk'
                WHEN cw.legacy_id IS NOT NULL THEN 'ghost'
                ELSE 'unmatched'
            END as end_match_type
        FROM with_start w
        LEFT JOIN crosswalk cw ON w.end_station_id_raw = cw.legacy_id
        LEFT JOIN current_stations cs ON w.end_station_id_raw = cs.station_id
        LEFT JOIN current_stations cs2 ON cw.modern_id = cs2.station_id
    )
    SELECT
        ride_id,
        started_at,
        ended_at,
        duration_sec,
        -- Canonical station info
        start_station_id,
        start_station_name,
        start_lat,
        start_lon,
        end_station_id,
        end_station_name,
        end_lat,
        end_lon,
        -- Raw coordinates for validation
        start_lat_raw,
        start_lon_raw,
        end_lat_raw,
        end_lon_raw,
        -- Metadata
        member_casual,
        rideable_type,
        bike_id,
        birth_year,
        gender,
        -- Demographics validity flags
        CASE
            WHEN birth_year IS NULL THEN NULL  -- Not applicable (modern data)
            WHEN birth_year = 1969
                 AND member_casual = 'casual'
                 AND EXTRACT(YEAR FROM started_at) >= 2018 THEN FALSE  -- Default value
            WHEN (EXTRACT(YEAR FROM started_at) - birth_year) < 10 THEN FALSE  -- Too young
            WHEN (EXTRACT(YEAR FROM started_at) - birth_year) > 100 THEN FALSE  -- Implausible
            ELSE TRUE
        END as birth_year_valid,
        CASE
            WHEN gender IS NULL THEN NULL  -- Not applicable (modern data)
            WHEN gender IN (1, 2) THEN TRUE
            ELSE FALSE  -- Unknown (0)
        END as gender_valid,
        CASE
            WHEN birth_year IS NULL THEN NULL
            WHEN birth_year = 1969
                 AND member_casual = 'casual'
                 AND EXTRACT(YEAR FROM started_at) >= 2018 THEN NULL
            WHEN (EXTRACT(YEAR FROM started_at) - birth_year) < 10 THEN NULL
            WHEN (EXTRACT(YEAR FROM started_at) - birth_year) > 100 THEN NULL
            ELSE CAST(EXTRACT(YEAR FROM started_at) - birth_year AS INTEGER)
        END as age_at_trip,
        '{csv_path.name}' as source_file,
        start_match_type,
        end_match_type
    FROM with_end
    WHERE started_at IS NOT NULL
      AND ended_at IS NOT NULL
      AND start_station_id_raw IS NOT NULL
      AND CAST(start_station_id_raw AS VARCHAR) != ''
      AND end_station_id_raw IS NOT NULL
      AND CAST(end_station_id_raw AS VARCHAR) != ''
      AND duration_sec >= 90           -- At least 90 seconds
      AND duration_sec <= 14400        -- At most 4 hours (14400 sec)
      {date_sanity_filter}
      {test_station_filter}
    """
    
    # Execute and save to parquet
    con.execute(f"""
        COPY ({query}) TO '{output_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)

    # Build filter stats query - reuses the same raw CTE to count filtered rows efficiently
    # This reads the file only once since DuckDB caches the read
    if schema == 'modern':
        filter_stats_query = f"""
            WITH raw AS (
                SELECT
                    started_at::TIMESTAMP as started_at,
                    ended_at::TIMESTAMP as ended_at,
                    EPOCH(ended_at::TIMESTAMP - started_at::TIMESTAMP) as duration_sec,
                    CAST(start_station_id AS VARCHAR) as start_station_id_raw,
                    CAST(end_station_id AS VARCHAR) as end_station_id_raw
                FROM read_csv_auto('{csv_path}', ignore_errors=true)
            )
            SELECT
                COUNT(*) as total_rows,
                SUM(CASE WHEN started_at IS NULL OR ended_at IS NULL THEN 1 ELSE 0 END) as invalid_timestamp,
                SUM(CASE WHEN start_station_id_raw IS NULL OR start_station_id_raw = ''
                         OR end_station_id_raw IS NULL OR end_station_id_raw = '' THEN 1 ELSE 0 END) as missing_station,
                SUM(CASE WHEN duration_sec < 90 AND duration_sec >= 0 THEN 1 ELSE 0 END) as duration_too_short,
                SUM(CASE WHEN duration_sec > 14400 THEN 1 ELSE 0 END) as duration_too_long,
                SUM(CASE WHEN started_at IS NOT NULL AND EXTRACT(YEAR FROM started_at) = {expected_year or 0}
                         AND EXTRACT(MONTH FROM started_at) = {expected_month or 0} THEN 1 ELSE 0 END) as dates_in_expected,
                SUM(CASE WHEN started_at IS NOT NULL AND (EXTRACT(YEAR FROM started_at) != {expected_year or 0}
                         OR EXTRACT(MONTH FROM started_at) != {expected_month or 0}) THEN 1 ELSE 0 END) as dates_outside_expected
            FROM raw
        """
    else:
        # Legacy schema - use the same parsing logic as the main query
        filter_stats_query = f"""
            WITH raw AS (
                SELECT
                    COALESCE(
                        TRY_STRPTIME(CAST(starttime AS VARCHAR), '%Y-%m-%d %H:%M:%S'),
                        TRY_STRPTIME(CAST(starttime AS VARCHAR), '%Y-%m-%d %H:%M:%S.%g'),
                        TRY_STRPTIME(CAST(starttime AS VARCHAR), '%m/%d/%Y %H:%M:%S'),
                        TRY_STRPTIME(CAST(starttime AS VARCHAR), '%m/%d/%Y %H:%M')
                    ) as started_at,
                    tripduration::INTEGER as duration_sec,
                    CAST("start station id" AS VARCHAR) as start_station_id_raw,
                    CAST("end station id" AS VARCHAR) as end_station_id_raw
                FROM read_csv_auto('{csv_path}', {read_options})
            )
            SELECT
                COUNT(*) as total_rows,
                SUM(CASE WHEN started_at IS NULL THEN 1 ELSE 0 END) as invalid_timestamp,
                SUM(CASE WHEN start_station_id_raw IS NULL OR start_station_id_raw = ''
                         OR end_station_id_raw IS NULL OR end_station_id_raw = '' THEN 1 ELSE 0 END) as missing_station,
                SUM(CASE WHEN duration_sec < 90 AND duration_sec >= 0 THEN 1 ELSE 0 END) as duration_too_short,
                SUM(CASE WHEN duration_sec > 14400 THEN 1 ELSE 0 END) as duration_too_long,
                SUM(CASE WHEN started_at IS NOT NULL AND EXTRACT(YEAR FROM started_at) = {expected_year or 0}
                         AND EXTRACT(MONTH FROM started_at) = {expected_month or 0} THEN 1 ELSE 0 END) as dates_in_expected,
                SUM(CASE WHEN started_at IS NOT NULL AND (EXTRACT(YEAR FROM started_at) != {expected_year or 0}
                         OR EXTRACT(MONTH FROM started_at) != {expected_month or 0}) THEN 1 ELSE 0 END) as dates_outside_expected
            FROM raw
        """

    try:
        filter_result = con.execute(filter_stats_query).fetchone()
        stats['rows_in'] = filter_result[0] or 0
        stats['rows_filtered'] = {
            'invalid_timestamp': filter_result[1] or 0,
            'missing_station': filter_result[2] or 0,
            'duration_too_short': filter_result[3] or 0,
            'duration_too_long': filter_result[4] or 0,
            'wrong_month': filter_result[6] or 0 if expected_year else 0,
        }
        stats['date_sanity'] = {
            'dates_in_expected_month': filter_result[5] or 0,
            'dates_outside_expected_month': filter_result[6] or 0,
        }
    except Exception as e:
        # Fallback: just get row count
        stats['rows_in'] = con.execute(f"""
            SELECT COUNT(*) FROM read_csv_auto('{csv_path}', ignore_errors=true)
        """).fetchone()[0]
    
    # Get output stats
    result = con.execute(f"""
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN start_match_type = 'direct' THEN 1 ELSE 0 END) as direct,
            SUM(CASE WHEN start_match_type = 'crosswalk' THEN 1 ELSE 0 END) as crosswalk,
            SUM(CASE WHEN start_match_type = 'ghost' THEN 1 ELSE 0 END) as ghost,
            SUM(CASE WHEN start_match_type = 'unmatched' THEN 1 ELSE 0 END) as unmatched
        FROM '{output_path}'
    """).fetchone()
    
    stats['rows_out'] = result[0]
    stats['station_match'] = {
        'direct': result[1],
        'crosswalk': result[2],
        'ghost': result[3],
        'unmatched': result[4],
    }
    
    return stats


def main():
    parser = argparse.ArgumentParser(description="Process Citi Bike trip data")
    parser.add_argument("--input-dir", type=Path, default=DATA_DIR / "raw_csvs",
                        help="Directory with raw CSVs")
    parser.add_argument("--output-dir", type=Path, default=DATA_DIR / "processed",
                        help="Directory for output Parquet files")
    parser.add_argument("--reference-dir", type=Path, default=REFERENCE_DIR,
                        help="Directory with reference tables")
    parser.add_argument("--limit", type=int, help="Process only first N files")
    parser.add_argument("--year", type=int, help="Process only files from this year")
    parser.add_argument("--force", action="store_true",
                        help="Reprocess files even if output already exists")

    args = parser.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    
    # Find CSV files
    csv_files = sorted(args.input_dir.glob("*.csv"))
    
    if args.year:
        csv_files = [f for f in csv_files if str(args.year) in f.name]
    
    if args.limit:
        csv_files = csv_files[:args.limit]
    
    if not csv_files:
        print(f"✗ No CSV files found in {args.input_dir}")
        exit(1)
    
    print(f"Processing {len(csv_files)} files...")
    
    # Initialize DuckDB
    con = duckdb.connect()
    
    # Load reference tables
    print("\nLoading reference tables...")
    load_reference_tables(con, args.reference_dir)
    
    # Process files
    all_stats = []
    total_in = 0
    total_out = 0
    skipped = 0

    for i, csv_path in enumerate(csv_files):
        output_path = args.output_dir / f"{csv_path.stem}.parquet"

        # Skip if output already exists (unless --force)
        if output_path.exists() and not args.force:
            print(f"\n[{i+1}/{len(csv_files)}] {csv_path.name} (skipped - output exists)")
            skipped += 1
            all_stats.append({'input_file': csv_path.name, 'skipped': True})
            continue

        print(f"\n[{i+1}/{len(csv_files)}] {csv_path.name}")

        try:
            stats = process_file(con, csv_path, args.output_dir)
            all_stats.append(stats)
            total_in += stats['rows_in']
            total_out += stats['rows_out']

            match_pct = 100 * (stats['station_match']['direct'] + stats['station_match']['crosswalk']) / max(stats['rows_out'], 1)
            filtered = stats['rows_in'] - stats['rows_out']
            filter_pct = 100 * filtered / max(stats['rows_in'], 1)
            print(f"    {stats['rows_in']:,} → {stats['rows_out']:,} rows ({filter_pct:.1f}% filtered) | {match_pct:.1f}% stations matched")

        except Exception as e:
            print(f"    ✗ Error: {e}")
            all_stats.append({'input_file': csv_path.name, 'error': str(e)})
    
    # Save run log
    processed = len(csv_files) - skipped
    log_path = LOGS_DIR / f"pipeline_run_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    log_data = {
        'timestamp': datetime.now().isoformat(),
        'files_total': len(csv_files),
        'files_processed': processed,
        'files_skipped': skipped,
        'total_rows_in': total_in,
        'total_rows_out': total_out,
        'file_stats': all_stats,
    }

    with open(log_path, 'w') as f:
        json.dump(log_data, f, indent=2)

    print(f"\n{'='*50}")
    if skipped > 0:
        print(f"✓ Processed {processed} files ({skipped} skipped - already exist)")
    else:
        print(f"✓ Processed {processed} files")
    print(f"✓ {total_in:,} rows in → {total_out:,} rows out")
    print(f"✓ Output: {args.output_dir}")
    print(f"✓ Log: {log_path}")

    # Run validation on processed data
    print(f"\n{'='*50}")
    print("Running station mapping validation...")

    from validate_mappings import validate_mappings, print_validation_report

    validation_results = validate_mappings(
        processed_dir=args.output_dir,
        year=args.year,
        distance_threshold_m=200,
        outlier_pct_threshold=5.0,
    )

    print_validation_report(validation_results)

    # Save validation results to log
    validation_log_path = LOGS_DIR / f"validation_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(validation_log_path, 'w') as f:
        json.dump(validation_results, f, indent=2)


if __name__ == "__main__":
    main()
