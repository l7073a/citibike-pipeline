#!/usr/bin/env python3
"""
Build MTA Reference Tables from GTFS

Processes raw GTFS files into clean, queryable parquet tables:
- stations.parquet: Station complexes with coordinates
- entrances.parquet: Physical entrance locations
- routes.parquet: Subway lines with colors
- station_routes.parquet: Which lines serve which stations
- service_frequency.parquet: Trains per hour by station/line/time period

Requires: Run fetch_gtfs.py first to download GTFS files

Usage:
    python src/mta/build_reference.py
    python src/mta/build_reference.py --gtfs data/mta/gtfs --output data/mta/reference
"""

import argparse
import os
import sys
from datetime import datetime
from pathlib import Path
import json

try:
    import duckdb
except ImportError:
    print("DuckDB required. Install with: pip install duckdb")
    sys.exit(1)

DEFAULT_GTFS_DIR = "data/mta/gtfs"
DEFAULT_OUTPUT_DIR = "data/mta/reference"


def build_stations(con: duckdb.DuckDBPyConnection, gtfs_dir: str, output_dir: str) -> int:
    """
    Build stations reference table from GTFS stops.txt.

    GTFS stops.txt contains multiple types:
    - location_type=1: Station (complex)
    - location_type=0: Platform (specific track)
    - location_type=2: Entrance/Exit

    This extracts just the station complexes.

    Returns: Row count
    """
    stops_file = Path(gtfs_dir) / "stops.txt"
    output_file = Path(output_dir) / "stations.parquet"

    print("Building stations.parquet...")

    # Station complexes have location_type = 1 (or blank for legacy format)
    # Parent stations have no parent_station value
    con.execute(f"""
        COPY (
            SELECT
                stop_id as station_id,
                stop_name as station_name,
                stop_lat as latitude,
                stop_lon as longitude,
                -- Extract borough from zone_id if available
                CASE
                    WHEN stop_name LIKE '%- Manhattan%' THEN 'Manhattan'
                    WHEN stop_name LIKE '%- Brooklyn%' THEN 'Brooklyn'
                    WHEN stop_name LIKE '%- Queens%' THEN 'Queens'
                    WHEN stop_name LIKE '%- Bronx%' THEN 'Bronx'
                    ELSE NULL
                END as borough
            FROM read_csv_auto('{stops_file}')
            WHERE location_type = '1'
               OR (location_type IS NULL AND parent_station IS NULL)
            ORDER BY stop_name
        ) TO '{output_file}' (FORMAT PARQUET)
    """)

    row_count = con.execute(f"SELECT COUNT(*) FROM '{output_file}'").fetchone()[0]
    print(f"  Created {output_file}: {row_count} stations")

    return row_count


def build_entrances(con: duckdb.DuckDBPyConnection, gtfs_dir: str, output_dir: str) -> int:
    """
    Build entrances reference table from GTFS stops.txt.

    Entrances have location_type = 2 and reference their parent station.

    Returns: Row count
    """
    stops_file = Path(gtfs_dir) / "stops.txt"
    output_file = Path(output_dir) / "entrances.parquet"

    print("Building entrances.parquet...")

    con.execute(f"""
        COPY (
            SELECT
                e.stop_id as entrance_id,
                e.stop_name as entrance_name,
                e.stop_lat as latitude,
                e.stop_lon as longitude,
                e.parent_station as station_id,
                s.stop_name as station_name
            FROM read_csv_auto('{stops_file}') e
            LEFT JOIN read_csv_auto('{stops_file}') s
                ON e.parent_station = s.stop_id
            WHERE e.location_type = '2'
            ORDER BY s.stop_name, e.stop_name
        ) TO '{output_file}' (FORMAT PARQUET)
    """)

    row_count = con.execute(f"SELECT COUNT(*) FROM '{output_file}'").fetchone()[0]
    print(f"  Created {output_file}: {row_count} entrances")

    return row_count


def build_routes(con: duckdb.DuckDBPyConnection, gtfs_dir: str, output_dir: str) -> int:
    """
    Build routes reference table from GTFS routes.txt.

    Includes subway line colors for visualization.

    Returns: Row count
    """
    routes_file = Path(gtfs_dir) / "routes.txt"
    output_file = Path(output_dir) / "routes.parquet"

    print("Building routes.parquet...")

    con.execute(f"""
        COPY (
            SELECT
                route_id,
                route_short_name as line_name,
                route_long_name as route_name,
                route_color,
                route_text_color,
                -- Categorize by line group
                CASE
                    WHEN route_short_name IN ('A', 'C', 'E') THEN '8th Ave'
                    WHEN route_short_name IN ('B', 'D', 'F', 'M') THEN '6th Ave'
                    WHEN route_short_name IN ('G') THEN 'Crosstown'
                    WHEN route_short_name IN ('J', 'Z') THEN 'Nassau'
                    WHEN route_short_name IN ('L') THEN '14th St-Canarsie'
                    WHEN route_short_name IN ('N', 'Q', 'R', 'W') THEN 'Broadway'
                    WHEN route_short_name IN ('1', '2', '3') THEN '7th Ave'
                    WHEN route_short_name IN ('4', '5', '6') THEN 'Lexington'
                    WHEN route_short_name IN ('7') THEN 'Flushing'
                    WHEN route_short_name = 'S' THEN 'Shuttle'
                    ELSE 'Other'
                END as line_group
            FROM read_csv_auto('{routes_file}')
            WHERE route_type = 1  -- Subway only (1 = subway/metro)
            ORDER BY route_short_name
        ) TO '{output_file}' (FORMAT PARQUET)
    """)

    row_count = con.execute(f"SELECT COUNT(*) FROM '{output_file}'").fetchone()[0]
    print(f"  Created {output_file}: {row_count} routes")

    return row_count


def build_station_routes(con: duckdb.DuckDBPyConnection, gtfs_dir: str, output_dir: str) -> int:
    """
    Build station-routes mapping from GTFS.

    Shows which subway lines serve which stations.
    Derived from stop_times.txt → trips.txt → routes.txt

    Returns: Row count
    """
    stops_file = Path(gtfs_dir) / "stops.txt"
    stop_times_file = Path(gtfs_dir) / "stop_times.txt"
    trips_file = Path(gtfs_dir) / "trips.txt"
    routes_file = Path(gtfs_dir) / "routes.txt"
    output_file = Path(output_dir) / "station_routes.parquet"

    print("Building station_routes.parquet...")

    con.execute(f"""
        COPY (
            SELECT DISTINCT
                -- Get parent station (complex) from platform stop
                COALESCE(s.parent_station, s.stop_id) as station_id,
                ps.stop_name as station_name,
                r.route_short_name as line_name,
                r.route_color
            FROM read_csv_auto('{stop_times_file}') st
            JOIN read_csv_auto('{trips_file}') t ON st.trip_id = t.trip_id
            JOIN read_csv_auto('{routes_file}') r ON t.route_id = r.route_id
            JOIN read_csv_auto('{stops_file}') s ON st.stop_id = s.stop_id
            LEFT JOIN read_csv_auto('{stops_file}') ps
                ON COALESCE(s.parent_station, s.stop_id) = ps.stop_id
            WHERE r.route_type = 1  -- Subway only
            ORDER BY station_name, line_name
        ) TO '{output_file}' (FORMAT PARQUET)
    """)

    row_count = con.execute(f"SELECT COUNT(*) FROM '{output_file}'").fetchone()[0]
    print(f"  Created {output_file}: {row_count} station-route pairs")

    return row_count


def build_service_frequency(con: duckdb.DuckDBPyConnection, gtfs_dir: str, output_dir: str) -> int:
    """
    Build service frequency table from GTFS.

    Calculates trains per hour by:
    - Station
    - Line
    - Time period (AM Peak, PM Peak, Midday, Evening, Night)
    - Day type (Weekday, Saturday, Sunday)

    This is a simplified view - actual schedules vary by day.

    Returns: Row count
    """
    stops_file = Path(gtfs_dir) / "stops.txt"
    stop_times_file = Path(gtfs_dir) / "stop_times.txt"
    trips_file = Path(gtfs_dir) / "trips.txt"
    routes_file = Path(gtfs_dir) / "routes.txt"
    calendar_file = Path(gtfs_dir) / "calendar.txt"
    output_file = Path(output_dir) / "service_frequency.parquet"

    print("Building service_frequency.parquet...")

    # Check if calendar.txt exists (some feeds use calendar_dates.txt instead)
    if not Path(calendar_file).exists():
        print("  Warning: calendar.txt not found, using simplified frequency calculation")
        # Simplified version without day-of-week breakdown
        con.execute(f"""
            COPY (
                SELECT
                    COALESCE(s.parent_station, s.stop_id) as station_id,
                    ps.stop_name as station_name,
                    r.route_short_name as line_name,
                    -- Time period based on arrival time
                    CASE
                        WHEN CAST(SUBSTR(st.arrival_time, 1, 2) AS INTEGER) BETWEEN 7 AND 9 THEN 'AM Peak'
                        WHEN CAST(SUBSTR(st.arrival_time, 1, 2) AS INTEGER) BETWEEN 10 AND 15 THEN 'Midday'
                        WHEN CAST(SUBSTR(st.arrival_time, 1, 2) AS INTEGER) BETWEEN 16 AND 19 THEN 'PM Peak'
                        WHEN CAST(SUBSTR(st.arrival_time, 1, 2) AS INTEGER) BETWEEN 20 AND 23 THEN 'Evening'
                        ELSE 'Night'
                    END as time_period,
                    COUNT(*) as trips_in_period,
                    -- Estimate trains per hour (assuming period spans shown hours)
                    ROUND(COUNT(*) / CASE
                        WHEN CAST(SUBSTR(st.arrival_time, 1, 2) AS INTEGER) BETWEEN 7 AND 9 THEN 3.0
                        WHEN CAST(SUBSTR(st.arrival_time, 1, 2) AS INTEGER) BETWEEN 10 AND 15 THEN 6.0
                        WHEN CAST(SUBSTR(st.arrival_time, 1, 2) AS INTEGER) BETWEEN 16 AND 19 THEN 4.0
                        WHEN CAST(SUBSTR(st.arrival_time, 1, 2) AS INTEGER) BETWEEN 20 AND 23 THEN 4.0
                        ELSE 7.0
                    END, 1) as trains_per_hour
                FROM read_csv_auto('{stop_times_file}') st
                JOIN read_csv_auto('{trips_file}') t ON st.trip_id = t.trip_id
                JOIN read_csv_auto('{routes_file}') r ON t.route_id = r.route_id
                JOIN read_csv_auto('{stops_file}') s ON st.stop_id = s.stop_id
                LEFT JOIN read_csv_auto('{stops_file}') ps
                    ON COALESCE(s.parent_station, s.stop_id) = ps.stop_id
                WHERE r.route_type = 1
                GROUP BY 1, 2, 3, 4
                ORDER BY station_name, line_name, time_period
            ) TO '{output_file}' (FORMAT PARQUET)
        """)
    else:
        # Full version with day-of-week from calendar
        con.execute(f"""
            COPY (
                SELECT
                    COALESCE(s.parent_station, s.stop_id) as station_id,
                    ps.stop_name as station_name,
                    r.route_short_name as line_name,
                    -- Day type
                    CASE
                        WHEN c.monday = 1 THEN 'Weekday'
                        WHEN c.saturday = 1 THEN 'Saturday'
                        WHEN c.sunday = 1 THEN 'Sunday'
                        ELSE 'Other'
                    END as day_type,
                    -- Time period
                    CASE
                        WHEN CAST(SUBSTR(st.arrival_time, 1, 2) AS INTEGER) BETWEEN 7 AND 9 THEN 'AM Peak'
                        WHEN CAST(SUBSTR(st.arrival_time, 1, 2) AS INTEGER) BETWEEN 10 AND 15 THEN 'Midday'
                        WHEN CAST(SUBSTR(st.arrival_time, 1, 2) AS INTEGER) BETWEEN 16 AND 19 THEN 'PM Peak'
                        WHEN CAST(SUBSTR(st.arrival_time, 1, 2) AS INTEGER) BETWEEN 20 AND 23 THEN 'Evening'
                        ELSE 'Night'
                    END as time_period,
                    COUNT(*) as trips_in_period,
                    ROUND(COUNT(*) / CASE
                        WHEN CAST(SUBSTR(st.arrival_time, 1, 2) AS INTEGER) BETWEEN 7 AND 9 THEN 3.0
                        WHEN CAST(SUBSTR(st.arrival_time, 1, 2) AS INTEGER) BETWEEN 10 AND 15 THEN 6.0
                        WHEN CAST(SUBSTR(st.arrival_time, 1, 2) AS INTEGER) BETWEEN 16 AND 19 THEN 4.0
                        WHEN CAST(SUBSTR(st.arrival_time, 1, 2) AS INTEGER) BETWEEN 20 AND 23 THEN 4.0
                        ELSE 7.0
                    END, 1) as trains_per_hour
                FROM read_csv_auto('{stop_times_file}') st
                JOIN read_csv_auto('{trips_file}') t ON st.trip_id = t.trip_id
                JOIN read_csv_auto('{routes_file}') r ON t.route_id = r.route_id
                JOIN read_csv_auto('{stops_file}') s ON st.stop_id = s.stop_id
                JOIN read_csv_auto('{calendar_file}') c ON t.service_id = c.service_id
                LEFT JOIN read_csv_auto('{stops_file}') ps
                    ON COALESCE(s.parent_station, s.stop_id) = ps.stop_id
                WHERE r.route_type = 1
                GROUP BY 1, 2, 3, 4, 5
                ORDER BY station_name, line_name, day_type, time_period
            ) TO '{output_file}' (FORMAT PARQUET)
        """)

    row_count = con.execute(f"SELECT COUNT(*) FROM '{output_file}'").fetchone()[0]
    print(f"  Created {output_file}: {row_count} frequency records")

    return row_count


def main():
    parser = argparse.ArgumentParser(
        description="Build MTA reference tables from GTFS"
    )
    parser.add_argument(
        "--gtfs", "-g",
        default=DEFAULT_GTFS_DIR,
        help=f"GTFS input directory (default: {DEFAULT_GTFS_DIR})"
    )
    parser.add_argument(
        "--output", "-o",
        default=DEFAULT_OUTPUT_DIR,
        help=f"Output directory (default: {DEFAULT_OUTPUT_DIR})"
    )

    args = parser.parse_args()

    # Check GTFS files exist
    gtfs_path = Path(args.gtfs)
    if not (gtfs_path / "stops.txt").exists():
        print(f"Error: GTFS files not found in {args.gtfs}")
        print("Run 'python src/mta/fetch_gtfs.py' first")
        sys.exit(1)

    # Create output directory
    output_path = Path(args.output)
    output_path.mkdir(parents=True, exist_ok=True)

    print(f"Building MTA reference tables from {args.gtfs}...\n")

    con = duckdb.connect()

    stats = {
        "stations": build_stations(con, args.gtfs, args.output),
        "entrances": build_entrances(con, args.gtfs, args.output),
        "routes": build_routes(con, args.gtfs, args.output),
        "station_routes": build_station_routes(con, args.gtfs, args.output),
        "service_frequency": build_service_frequency(con, args.gtfs, args.output),
    }

    con.close()

    # Save metadata
    metadata = {
        "build_time": datetime.now().isoformat(),
        "gtfs_dir": str(gtfs_path),
        "output_dir": str(output_path),
        "tables": stats
    }

    metadata_file = output_path / "metadata.json"
    with open(metadata_file, 'w') as f:
        json.dump(metadata, f, indent=2)

    print(f"\nBuild complete!")
    print(f"  Total tables: {len(stats)}")
    print(f"  Output: {args.output}")
    print(f"\nExample queries:")
    print(f"  import duckdb")
    print(f"  con = duckdb.connect()")
    print(f"  con.execute(\"SELECT * FROM '{args.output}/stations.parquet' LIMIT 5\").fetchdf()")


if __name__ == "__main__":
    main()
