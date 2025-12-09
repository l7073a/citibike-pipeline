#!/usr/bin/env python3
"""
Audit and quality analysis for Citi Bike data pipeline.

This script:
1. Analyzes filtered/discarded rows and why
2. Tracks station appearances over time
3. Documents coordinate handling
4. Identifies anomalies and issues
"""

import argparse
import json
from datetime import datetime
from pathlib import Path

import duckdb

DATA_DIR = Path(__file__).parent.parent / "data"
REFERENCE_DIR = Path(__file__).parent.parent / "reference"
LOGS_DIR = Path(__file__).parent.parent / "logs"


def analyze_filtered_rows(con: duckdb.DuckDBPyConnection, csv_pattern: str) -> dict:
    """
    Analyze why rows would be filtered out.
    Returns detailed breakdown.
    """
    # Detect schema from first file
    import glob
    first_file = glob.glob(csv_pattern)[0] if glob.glob(csv_pattern) else None
    if not first_file:
        return {'error': 'No files found'}

    with open(first_file) as f:
        header = f.readline().lower()

    is_modern = 'ride_id' in header
    is_titlecase = 'Trip Duration' in open(first_file).readline()

    if is_modern:
        query = f"""
            WITH raw AS (
                SELECT
                    started_at::TIMESTAMP as started_at,
                    ended_at::TIMESTAMP as ended_at,
                    EPOCH(ended_at::TIMESTAMP - started_at::TIMESTAMP) as duration_sec,
                    CAST(start_station_id AS VARCHAR) as start_station_id,
                    CAST(end_station_id AS VARCHAR) as end_station_id,
                    start_lat, start_lng, end_lat, end_lng
                FROM read_csv_auto('{csv_pattern}', ignore_errors=true)
            )
            SELECT
                COUNT(*) as total_rows,
                SUM(CASE WHEN started_at IS NULL OR ended_at IS NULL THEN 1 ELSE 0 END) as invalid_timestamp,
                SUM(CASE WHEN start_station_id IS NULL OR start_station_id = '' THEN 1 ELSE 0 END) as missing_start_station,
                SUM(CASE WHEN end_station_id IS NULL OR end_station_id = '' THEN 1 ELSE 0 END) as missing_end_station,
                SUM(CASE WHEN duration_sec < 0 THEN 1 ELSE 0 END) as negative_duration,
                SUM(CASE WHEN duration_sec >= 0 AND duration_sec < 90 THEN 1 ELSE 0 END) as duration_under_90s,
                SUM(CASE WHEN duration_sec > 14400 THEN 1 ELSE 0 END) as duration_over_4h,
                SUM(CASE WHEN start_lat IS NULL OR start_lng IS NULL THEN 1 ELSE 0 END) as missing_start_coords,
                SUM(CASE WHEN end_lat IS NULL OR end_lng IS NULL THEN 1 ELSE 0 END) as missing_end_coords,
                SUM(CASE WHEN start_lat NOT BETWEEN 40.4 AND 41.0 OR start_lng NOT BETWEEN -74.3 AND -73.7 THEN 1 ELSE 0 END) as invalid_start_coords,
                SUM(CASE WHEN end_lat NOT BETWEEN 40.4 AND 41.0 OR end_lng NOT BETWEEN -74.3 AND -73.7 THEN 1 ELSE 0 END) as invalid_end_coords
            FROM raw
        """
    else:
        # Legacy schema
        duration_col = '"Trip Duration"' if is_titlecase else 'tripduration'
        start_id_col = '"Start Station ID"' if is_titlecase else '"start station id"'
        end_id_col = '"End Station ID"' if is_titlecase else '"end station id"'
        start_lat_col = '"Start Station Latitude"' if is_titlecase else '"start station latitude"'
        start_lng_col = '"Start Station Longitude"' if is_titlecase else '"start station longitude"'
        end_lat_col = '"End Station Latitude"' if is_titlecase else '"end station latitude"'
        end_lng_col = '"End Station Longitude"' if is_titlecase else '"end station longitude"'
        time_col = '"Start Time"' if is_titlecase else 'starttime'

        query = f"""
            WITH raw AS (
                SELECT
                    TRY_CAST({time_col} AS TIMESTAMP) as started_at,
                    {duration_col}::INTEGER as duration_sec,
                    CAST({start_id_col} AS VARCHAR) as start_station_id,
                    CAST({end_id_col} AS VARCHAR) as end_station_id,
                    TRY_CAST({start_lat_col} AS DOUBLE) as start_lat,
                    TRY_CAST({start_lng_col} AS DOUBLE) as start_lng,
                    TRY_CAST({end_lat_col} AS DOUBLE) as end_lat,
                    TRY_CAST({end_lng_col} AS DOUBLE) as end_lng
                FROM read_csv_auto('{csv_pattern}', ignore_errors=true)
            )
            SELECT
                COUNT(*) as total_rows,
                SUM(CASE WHEN started_at IS NULL THEN 1 ELSE 0 END) as invalid_timestamp,
                SUM(CASE WHEN start_station_id IS NULL OR start_station_id = '' THEN 1 ELSE 0 END) as missing_start_station,
                SUM(CASE WHEN end_station_id IS NULL OR end_station_id = '' THEN 1 ELSE 0 END) as missing_end_station,
                SUM(CASE WHEN duration_sec < 0 THEN 1 ELSE 0 END) as negative_duration,
                SUM(CASE WHEN duration_sec >= 0 AND duration_sec < 90 THEN 1 ELSE 0 END) as duration_under_90s,
                SUM(CASE WHEN duration_sec > 14400 THEN 1 ELSE 0 END) as duration_over_4h,
                SUM(CASE WHEN start_lat IS NULL OR start_lng IS NULL THEN 1 ELSE 0 END) as missing_start_coords,
                SUM(CASE WHEN end_lat IS NULL OR end_lng IS NULL THEN 1 ELSE 0 END) as missing_end_coords,
                SUM(CASE WHEN start_lat NOT BETWEEN 40.4 AND 41.0 OR start_lng NOT BETWEEN -74.3 AND -73.7 THEN 1 ELSE 0 END) as invalid_start_coords,
                SUM(CASE WHEN end_lat NOT BETWEEN 40.4 AND 41.0 OR end_lng NOT BETWEEN -74.3 AND -73.7 THEN 1 ELSE 0 END) as invalid_end_coords
            FROM raw
        """

    result = con.execute(query).fetchone()
    cols = ['total_rows', 'invalid_timestamp', 'missing_start_station', 'missing_end_station',
            'negative_duration', 'duration_under_90s', 'duration_over_4h',
            'missing_start_coords', 'missing_end_coords', 'invalid_start_coords', 'invalid_end_coords']

    return dict(zip(cols, result))


def track_station_appearances(con: duckdb.DuckDBPyConnection, csv_dir: Path) -> dict:
    """
    Track when stations first/last appear in the data.
    Returns station timeline info.
    """
    # Get all unique station IDs with their first and last appearance
    query = f"""
        WITH all_stations AS (
            -- Modern schema files
            SELECT
                COALESCE(CAST(start_station_id AS VARCHAR), '') as station_id,
                start_station_name as station_name,
                start_lat as lat,
                start_lng as lng,
                DATE_TRUNC('month', started_at::TIMESTAMP) as month
            FROM read_csv_auto('{csv_dir}/*202*.csv', ignore_errors=true)
            WHERE start_station_id IS NOT NULL AND CAST(start_station_id AS VARCHAR) != ''

            UNION ALL

            SELECT
                COALESCE(CAST(end_station_id AS VARCHAR), '') as station_id,
                end_station_name as station_name,
                end_lat as lat,
                end_lng as lng,
                DATE_TRUNC('month', started_at::TIMESTAMP) as month
            FROM read_csv_auto('{csv_dir}/*202*.csv', ignore_errors=true)
            WHERE end_station_id IS NOT NULL AND CAST(end_station_id AS VARCHAR) != ''
        )
        SELECT
            station_id,
            ANY_VALUE(station_name) as station_name,
            ANY_VALUE(lat) as lat,
            ANY_VALUE(lng) as lng,
            MIN(month) as first_seen,
            MAX(month) as last_seen,
            COUNT(DISTINCT month) as months_active
        FROM all_stations
        WHERE station_id != ''
        GROUP BY station_id
        ORDER BY first_seen
    """

    try:
        result = con.execute(query).fetchall()
        return {
            'total_stations': len(result),
            'stations': [
                {
                    'station_id': r[0],
                    'name': r[1],
                    'lat': r[2],
                    'lng': r[3],
                    'first_seen': str(r[4]),
                    'last_seen': str(r[5]),
                    'months_active': r[6],
                }
                for r in result[:100]  # Limit for display
            ]
        }
    except Exception as e:
        return {'error': str(e)}


def analyze_coordinate_quality(con: duckdb.DuckDBPyConnection, csv_pattern: str) -> dict:
    """
    Analyze coordinate data quality and variations.
    """
    # Check for coordinate variations for the same station ID
    query = f"""
        WITH station_coords AS (
            SELECT
                CAST(start_station_id AS VARCHAR) as station_id,
                start_lat as lat,
                start_lng as lng
            FROM read_csv_auto('{csv_pattern}', ignore_errors=true)
            WHERE start_station_id IS NOT NULL
        )
        SELECT
            station_id,
            COUNT(DISTINCT ROUND(lat, 4)) as unique_lats,
            COUNT(DISTINCT ROUND(lng, 4)) as unique_lngs,
            MIN(lat) as min_lat, MAX(lat) as max_lat,
            MIN(lng) as min_lng, MAX(lng) as max_lng,
            COUNT(*) as trips
        FROM station_coords
        GROUP BY station_id
        HAVING COUNT(DISTINCT ROUND(lat, 4)) > 1 OR COUNT(DISTINCT ROUND(lng, 4)) > 1
        ORDER BY (MAX(lat) - MIN(lat)) + (MAX(lng) - MIN(lng)) DESC
        LIMIT 20
    """

    try:
        result = con.execute(query).fetchall()
        return {
            'stations_with_coord_variations': len(result),
            'examples': [
                {
                    'station_id': r[0],
                    'unique_lats': r[1],
                    'unique_lngs': r[2],
                    'lat_range': f"{r[3]:.6f} to {r[4]:.6f}",
                    'lng_range': f"{r[5]:.6f} to {r[6]:.6f}",
                    'trips': r[7],
                }
                for r in result
            ]
        }
    except Exception as e:
        return {'error': str(e)}


def identify_anomalies(con: duckdb.DuckDBPyConnection, csv_pattern: str) -> dict:
    """
    Identify various data anomalies.
    """
    anomalies = {}

    # Check for future dates
    try:
        result = con.execute(f"""
            SELECT COUNT(*) FROM read_csv_auto('{csv_pattern}', ignore_errors=true)
            WHERE TRY_CAST(started_at AS TIMESTAMP) > CURRENT_TIMESTAMP
        """).fetchone()[0]
        anomalies['future_dates'] = result
    except:
        anomalies['future_dates'] = 'N/A'

    # Check for very old dates (before Citi Bike launch in 2013)
    try:
        result = con.execute(f"""
            SELECT COUNT(*) FROM read_csv_auto('{csv_pattern}', ignore_errors=true)
            WHERE TRY_CAST(started_at AS TIMESTAMP) < '2013-01-01'
        """).fetchone()[0]
        anomalies['pre_launch_dates'] = result
    except:
        anomalies['pre_launch_dates'] = 'N/A'

    # Check for same start/end station (round trips)
    try:
        result = con.execute(f"""
            SELECT COUNT(*) FROM read_csv_auto('{csv_pattern}', ignore_errors=true)
            WHERE CAST(start_station_id AS VARCHAR) = CAST(end_station_id AS VARCHAR)
              AND start_station_id IS NOT NULL
        """).fetchone()[0]
        anomalies['round_trips'] = result
    except:
        anomalies['round_trips'] = 'N/A'

    return anomalies


def main():
    parser = argparse.ArgumentParser(description="Audit Citi Bike data quality")
    parser.add_argument("--year", type=int, help="Year to audit")
    parser.add_argument("--all", action="store_true", help="Audit all years")
    parser.add_argument("--station-timeline", action="store_true", help="Generate station timeline")

    args = parser.parse_args()
    LOGS_DIR.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()

    if args.station_timeline:
        print("Generating station timeline...")
        result = track_station_appearances(con, DATA_DIR / "raw_csvs")
        print(f"  Total stations tracked: {result.get('total_stations', 'N/A')}")

        # Save to reference
        output_path = REFERENCE_DIR / "station_timeline.json"
        with open(output_path, 'w') as f:
            json.dump(result, f, indent=2, default=str)
        print(f"  Saved to {output_path}")
        return

    years = []
    if args.year:
        years = [args.year]
    elif args.all:
        years = list(range(2013, 2026))
    else:
        print("Please specify --year YYYY or --all")
        return

    all_results = {}

    for year in years:
        print(f"\n{'='*50}")
        print(f"Auditing {year}")
        print('='*50)

        csv_pattern = str(DATA_DIR / "raw_csvs" / f"*{year}*.csv")

        # Filter analysis
        print("\nFiltered rows analysis:")
        filter_stats = analyze_filtered_rows(con, csv_pattern)
        if 'error' not in filter_stats:
            total = filter_stats['total_rows']
            print(f"  Total rows: {total:,}")
            for key, value in filter_stats.items():
                if key != 'total_rows' and value > 0:
                    pct = 100 * value / total if total > 0 else 0
                    print(f"  {key}: {value:,} ({pct:.2f}%)")

        # Coordinate quality
        print("\nCoordinate quality:")
        coord_stats = analyze_coordinate_quality(con, csv_pattern)
        if 'error' not in coord_stats:
            print(f"  Stations with coordinate variations: {coord_stats['stations_with_coord_variations']}")
            if coord_stats['examples']:
                print("  Top variations:")
                for ex in coord_stats['examples'][:5]:
                    print(f"    Station {ex['station_id']}: lat {ex['lat_range']}, lng {ex['lng_range']}")

        # Anomalies
        print("\nAnomalies:")
        anomalies = identify_anomalies(con, csv_pattern)
        for key, value in anomalies.items():
            print(f"  {key}: {value:,}" if isinstance(value, int) else f"  {key}: {value}")

        all_results[year] = {
            'filter_stats': filter_stats,
            'coord_quality': coord_stats,
            'anomalies': anomalies,
        }

    # Save audit results
    log_path = LOGS_DIR / f"audit_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(log_path, 'w') as f:
        json.dump({
            'timestamp': datetime.now().isoformat(),
            'years_audited': years,
            'results': all_results,
        }, f, indent=2, default=str)
    print(f"\n✓ Audit log saved to {log_path}")


if __name__ == "__main__":
    main()
