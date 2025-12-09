#!/usr/bin/env python3
"""
Validate station mappings by comparing raw coordinates to canonical coordinates.

Flags suspicious mappings where:
- Median distance between raw and canonical coords is high (potential bad mapping)
- vs. just a few outlier trips (bad raw data, mapping is fine)
"""

import argparse
import json
from datetime import datetime
from pathlib import Path

try:
    import duckdb
except ImportError:
    print("Missing dependency: duckdb")
    exit(1)

DATA_DIR = Path(__file__).parent.parent / "data"
LOGS_DIR = Path(__file__).parent.parent / "logs"
REFERENCE_DIR = Path(__file__).parent.parent / "reference"


def validate_mappings(
    processed_dir: Path,
    year: int = None,
    distance_threshold_m: float = 200,
    outlier_pct_threshold: float = 5.0,
) -> dict:
    """
    Validate station mappings by analyzing coordinate discrepancies.

    Returns dict with:
    - suspicious_mappings: stations where median distance is high (likely bad mapping)
    - bad_data_stations: stations where only a few trips have high distance (bad raw data)
    - summary stats
    """

    con = duckdb.connect()

    # Build file pattern
    if year:
        pattern = f"{processed_dir}/*{year}*.parquet"
    else:
        pattern = f"{processed_dir}/*.parquet"

    # Check if files exist
    file_count = con.execute(f"""
        SELECT COUNT(*) FROM glob('{pattern}')
    """).fetchone()[0]

    if file_count == 0:
        return {'error': f'No parquet files found matching {pattern}'}

    # Load crosswalk to get original legacy coordinates
    crosswalk_path = REFERENCE_DIR / "station_crosswalk.csv"
    con.execute(f"""
        CREATE TABLE crosswalk AS
        SELECT * FROM read_csv_auto('{crosswalk_path}')
    """)

    # Analyze start station coordinate discrepancies
    # Join processed data back to crosswalk to get legacy coords
    # Calculate distance between raw legacy coords and canonical coords used

    query = f"""
    WITH trips AS (
        SELECT
            start_station_id,
            start_station_name,
            start_lat,
            start_lon,
            start_match_type,
            source_file
        FROM '{pattern}'
        WHERE start_match_type IN ('crosswalk', 'ghost')
    ),
    -- Join to crosswalk to get legacy coordinates
    with_legacy AS (
        SELECT
            t.*,
            c.legacy_id,
            c.legacy_name,
            c.legacy_lat,
            c.legacy_lon,
            -- Haversine distance approximation (good enough for validation)
            -- 111139 meters per degree at NYC latitude
            SQRT(
                POWER((t.start_lat - c.legacy_lat) * 111139, 2) +
                POWER((t.start_lon - c.legacy_lon) * 111139 * COS(RADIANS(40.7)), 2)
            ) as distance_m
        FROM trips t
        LEFT JOIN crosswalk c ON t.start_station_id = c.modern_id
            OR (t.start_match_type = 'ghost' AND t.start_station_id = c.legacy_id)
    ),
    -- Aggregate by legacy station
    station_stats AS (
        SELECT
            legacy_id,
            legacy_name,
            legacy_lat,
            legacy_lon,
            start_station_id as canonical_id,
            start_station_name as canonical_name,
            start_lat as canonical_lat,
            start_lon as canonical_lon,
            start_match_type as match_type,
            COUNT(*) as trip_count,
            MEDIAN(distance_m) as median_distance_m,
            AVG(distance_m) as avg_distance_m,
            MAX(distance_m) as max_distance_m,
            PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY distance_m) as p95_distance_m,
            SUM(CASE WHEN distance_m > {distance_threshold_m} THEN 1 ELSE 0 END) as trips_over_threshold,
            SUM(CASE WHEN distance_m > {distance_threshold_m} THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as pct_over_threshold
        FROM with_legacy
        WHERE legacy_id IS NOT NULL
        GROUP BY legacy_id, legacy_name, legacy_lat, legacy_lon,
                 start_station_id, start_station_name, start_lat, start_lon, start_match_type
    )
    SELECT * FROM station_stats
    ORDER BY median_distance_m DESC
    """

    results = con.execute(query).fetchdf()

    # Categorize stations
    suspicious_mappings = []  # High median distance = likely bad mapping
    bad_data_stations = []    # Low median but some outliers = bad raw data
    good_mappings = []        # Everything looks fine

    for _, row in results.iterrows():
        station_info = {
            'legacy_id': row['legacy_id'],
            'legacy_name': row['legacy_name'],
            'legacy_lat': round(row['legacy_lat'], 6) if row['legacy_lat'] else None,
            'legacy_lon': round(row['legacy_lon'], 6) if row['legacy_lon'] else None,
            'canonical_id': row['canonical_id'],
            'canonical_name': row['canonical_name'],
            'canonical_lat': round(row['canonical_lat'], 6) if row['canonical_lat'] else None,
            'canonical_lon': round(row['canonical_lon'], 6) if row['canonical_lon'] else None,
            'match_type': row['match_type'],
            'trip_count': int(row['trip_count']),
            'median_distance_m': round(row['median_distance_m'], 1),
            'avg_distance_m': round(row['avg_distance_m'], 1),
            'max_distance_m': round(row['max_distance_m'], 1),
            'p95_distance_m': round(row['p95_distance_m'], 1),
            'pct_over_threshold': round(row['pct_over_threshold'], 2),
        }

        # High median = suspicious mapping (affects all trips)
        if row['median_distance_m'] > distance_threshold_m:
            suspicious_mappings.append(station_info)
        # Low median but some outliers = bad raw data on some trips
        elif row['pct_over_threshold'] > outlier_pct_threshold:
            bad_data_stations.append(station_info)
        else:
            good_mappings.append(station_info)

    return {
        'year': year,
        'files_analyzed': file_count,
        'distance_threshold_m': distance_threshold_m,
        'outlier_pct_threshold': outlier_pct_threshold,
        'summary': {
            'total_stations_analyzed': len(results),
            'suspicious_mappings': len(suspicious_mappings),
            'bad_data_stations': len(bad_data_stations),
            'good_mappings': len(good_mappings),
        },
        'suspicious_mappings': suspicious_mappings,
        'bad_data_stations': bad_data_stations,
        # Don't include good_mappings in output to keep it readable
    }


def print_validation_report(results: dict):
    """Print a human-readable validation report."""

    if 'error' in results:
        print(f"Error: {results['error']}")
        return

    print(f"\n{'='*60}")
    print(f"STATION MAPPING VALIDATION REPORT")
    if results['year']:
        print(f"Year: {results['year']}")
    print(f"{'='*60}")

    summary = results['summary']
    print(f"\nStations analyzed: {summary['total_stations_analyzed']}")
    print(f"  Good mappings: {summary['good_mappings']}")
    print(f"  Suspicious mappings (likely wrong): {summary['suspicious_mappings']}")
    print(f"  Bad raw data (mapping OK, some trips off): {summary['bad_data_stations']}")

    if results['suspicious_mappings']:
        print(f"\n{'─'*60}")
        print("SUSPICIOUS MAPPINGS (median distance > threshold)")
        print("These may be incorrect mappings - review manually:")
        print(f"{'─'*60}")

        for s in results['suspicious_mappings'][:20]:  # Show top 20
            print(f"\n  Legacy: {s['legacy_id']} - {s['legacy_name']}")
            print(f"  Mapped to: {s['canonical_name']}")
            print(f"  Trips: {s['trip_count']:,} | Median dist: {s['median_distance_m']:.0f}m | Match: {s['match_type']}")
            print(f"  Legacy coords: ({s['legacy_lat']}, {s['legacy_lon']})")
            print(f"  Canon coords:  ({s['canonical_lat']}, {s['canonical_lon']})")

    if results['bad_data_stations']:
        print(f"\n{'─'*60}")
        print("BAD RAW DATA (mapping OK, but some trips have wrong coords)")
        print(f"{'─'*60}")

        for s in results['bad_data_stations'][:10]:  # Show top 10
            print(f"\n  {s['legacy_id']} - {s['legacy_name']}")
            print(f"  Trips: {s['trip_count']:,} | {s['pct_over_threshold']:.1f}% over threshold")

    print(f"\n{'='*60}")


def main():
    parser = argparse.ArgumentParser(description="Validate station mappings")
    parser.add_argument("--year", type=int, help="Validate specific year")
    parser.add_argument("--processed-dir", type=Path, default=DATA_DIR / "processed",
                        help="Directory with processed parquet files")
    parser.add_argument("--threshold", type=float, default=200,
                        help="Distance threshold in meters (default: 200)")
    parser.add_argument("--outlier-pct", type=float, default=5.0,
                        help="Percentage threshold for outliers (default: 5.0)")
    parser.add_argument("--json", action="store_true", help="Output JSON instead of report")

    args = parser.parse_args()

    LOGS_DIR.mkdir(parents=True, exist_ok=True)

    results = validate_mappings(
        processed_dir=args.processed_dir,
        year=args.year,
        distance_threshold_m=args.threshold,
        outlier_pct_threshold=args.outlier_pct,
    )

    # Save to log file
    log_path = LOGS_DIR / f"validation_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(log_path, 'w') as f:
        json.dump(results, f, indent=2)

    if args.json:
        print(json.dumps(results, indent=2))
    else:
        print_validation_report(results)
        print(f"✓ Full results saved to {log_path}")

    return results


if __name__ == "__main__":
    main()
