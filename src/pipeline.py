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
"""

import argparse
import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Optional

try:
    import duckdb
except ImportError:
    print("Missing dependency: duckdb")
    print("Run: pip install duckdb")
    exit(1)

REFERENCE_DIR = Path(__file__).parent.parent / "reference"
DATA_DIR = Path(__file__).parent.parent / "data"
LOGS_DIR = Path(__file__).parent.parent / "logs"


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
    
    stats = {
        'input_file': csv_path.name,
        'schema': schema,
        'rows_in': 0,
        'rows_out': 0,
        'station_match': {'direct': 0, 'crosswalk': 0, 'ghost': 0, 'unmatched': 0},
    }
    
    # Build schema-specific SELECT
    if schema == 'modern':
        select_clause = """
            ride_id,
            rideable_type,
            started_at::TIMESTAMP as started_at,
            ended_at::TIMESTAMP as ended_at,
            EPOCH(ended_at::TIMESTAMP - started_at::TIMESTAMP) as duration_sec,
            CAST(start_station_id AS VARCHAR) as start_station_id_raw,
            start_station_name as start_station_name_raw,
            start_lat as start_lat_raw,
            start_lng as start_lng_raw,
            CAST(end_station_id AS VARCHAR) as end_station_id_raw,
            end_station_name as end_station_name_raw,
            end_lat as end_lat_raw,
            end_lng as end_lng_raw,
            member_casual,
            rideable_type
        """
    elif schema == 'legacy':
        # Lowercase column names (most 2014-2020 data)
        select_clause = """
            -- Generate synthetic ride_id from row data
            MD5(CONCAT(
                COALESCE(starttime, ''),
                COALESCE(CAST("start station id" AS VARCHAR), ''),
                COALESCE(CAST(bikeid AS VARCHAR), '')
            )) as ride_id,
            NULL as rideable_type,
            TRY_CAST(starttime AS TIMESTAMP) as started_at,
            TRY_CAST(stoptime AS TIMESTAMP) as ended_at,
            tripduration::INTEGER as duration_sec,
            REGEXP_REPLACE(CAST("start station id" AS VARCHAR), '\\.0$', '') as start_station_id_raw,
            "start station name" as start_station_name_raw,
            TRY_CAST("start station latitude" AS DOUBLE) as start_lat_raw,
            TRY_CAST("start station longitude" AS DOUBLE) as start_lng_raw,
            REGEXP_REPLACE(CAST("end station id" AS VARCHAR), '\\.0$', '') as end_station_id_raw,
            "end station name" as end_station_name_raw,
            TRY_CAST("end station latitude" AS DOUBLE) as end_lat_raw,
            TRY_CAST("end station longitude" AS DOUBLE) as end_lng_raw,
            CASE
                WHEN usertype = 'Subscriber' THEN 'member'
                WHEN usertype = 'Customer' THEN 'casual'
                ELSE usertype
            END as member_casual,
            NULL as rideable_type
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
            NULL as rideable_type,
            "Start Time"::TIMESTAMP as started_at,
            "Stop Time"::TIMESTAMP as ended_at,
            "Trip Duration"::INTEGER as duration_sec,
            REGEXP_REPLACE(CAST("Start Station ID" AS VARCHAR), '\\.0$', '') as start_station_id_raw,
            "Start Station Name" as start_station_name_raw,
            "Start Station Latitude"::DOUBLE as start_lat_raw,
            "Start Station Longitude"::DOUBLE as start_lng_raw,
            REGEXP_REPLACE(CAST("End Station ID" AS VARCHAR), '\\.0$', '') as end_station_id_raw,
            "End Station Name" as end_station_name_raw,
            "End Station Latitude"::DOUBLE as end_lat_raw,
            "End Station Longitude"::DOUBLE as end_lng_raw,
            CASE
                WHEN "User Type" = 'Subscriber' THEN 'member'
                WHEN "User Type" = 'Customer' THEN 'casual'
                ELSE "User Type"
            END as member_casual,
            NULL as rideable_type
        """
    else:
        print(f"    ⚠ Unknown schema, skipping")
        return stats
    
    # Main transformation query with station resolution
    query = f"""
    WITH raw AS (
        SELECT {select_clause}
        FROM read_csv_auto('{csv_path}', ignore_errors=true)
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
                WHEN c.start_id_type = 'modern' THEN COALESCE(cs.lon, c.start_lng_raw)
                WHEN cw.modern_id IS NOT NULL AND cw.modern_id != '' THEN COALESCE(cs2.lon, cw.legacy_lon)
                ELSE COALESCE(cw.legacy_lon, c.start_lng_raw)
            END as start_lng,
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
                WHEN w.end_id_type = 'modern' THEN COALESCE(cs.lon, w.end_lng_raw)
                WHEN cw.modern_id IS NOT NULL AND cw.modern_id != '' THEN COALESCE(cs2.lon, cw.legacy_lon)
                ELSE COALESCE(cw.legacy_lon, w.end_lng_raw)
            END as end_lng,
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
        start_station_id,
        start_station_name,
        start_lat,
        start_lng,
        end_station_id,
        end_station_name,
        end_lat,
        end_lng,
        member_casual,
        rideable_type,
        '{csv_path.name}' as source_file,
        start_match_type,
        end_match_type
    FROM with_end
    WHERE started_at IS NOT NULL
      AND ended_at IS NOT NULL
      AND duration_sec > 0
      AND duration_sec < 86400  -- Less than 24 hours
    """
    
    # Get input row count
    stats['rows_in'] = con.execute(f"""
        SELECT COUNT(*) FROM read_csv_auto('{csv_path}', ignore_errors=true)
    """).fetchone()[0]
    
    # Execute and save to parquet
    con.execute(f"""
        COPY ({query}) TO '{output_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)
    
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
    
    for i, csv_path in enumerate(csv_files):
        print(f"\n[{i+1}/{len(csv_files)}] {csv_path.name}")
        
        try:
            stats = process_file(con, csv_path, args.output_dir)
            all_stats.append(stats)
            total_in += stats['rows_in']
            total_out += stats['rows_out']
            
            match_pct = 100 * (stats['station_match']['direct'] + stats['station_match']['crosswalk']) / max(stats['rows_out'], 1)
            print(f"    {stats['rows_in']:,} → {stats['rows_out']:,} rows | {match_pct:.1f}% stations matched")
            
        except Exception as e:
            print(f"    ✗ Error: {e}")
            all_stats.append({'input_file': csv_path.name, 'error': str(e)})
    
    # Save run log
    log_path = LOGS_DIR / f"pipeline_run_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    log_data = {
        'timestamp': datetime.now().isoformat(),
        'files_processed': len(csv_files),
        'total_rows_in': total_in,
        'total_rows_out': total_out,
        'file_stats': all_stats,
    }
    
    with open(log_path, 'w') as f:
        json.dump(log_data, f, indent=2)
    
    print(f"\n{'='*50}")
    print(f"✓ Processed {len(csv_files)} files")
    print(f"✓ {total_in:,} rows in → {total_out:,} rows out")
    print(f"✓ Output: {args.output_dir}")
    print(f"✓ Log: {log_path}")


if __name__ == "__main__":
    main()
