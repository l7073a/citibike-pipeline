#!/usr/bin/env python3
"""
Generate a detailed station mapping report for auditing and understanding
how legacy stations map to canonical (modern) stations.

This report helps answer:
- Why did each station match (or not match)?
- What variations exist in names and coordinates?
- Which stations are true "ghosts" vs renamed stations?

Usage:
    python src/mapping_report.py --years 2013 2014 2015
    python src/mapping_report.py --all
"""

import argparse
import csv
import json
import re
from datetime import datetime
from pathlib import Path
from collections import defaultdict

try:
    import duckdb
    from rapidfuzz import fuzz
    import numpy as np
except ImportError as e:
    print(f"Missing dependency: {e}")
    print("Run: pip install duckdb rapidfuzz numpy")
    exit(1)

DATA_DIR = Path(__file__).parent.parent / "data"
REFERENCE_DIR = Path(__file__).parent.parent / "reference"
LOGS_DIR = Path(__file__).parent.parent / "logs"

# Test/internal station patterns to filter out (shared with pipeline.py)
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


def build_test_station_sql_filter() -> str:
    """Generate SQL filter to exclude test stations."""
    conditions = []
    for pattern in TEST_STATION_PATTERNS:
        escaped = pattern.replace("'", "''")
        conditions.append(f"LOWER(station_name) NOT LIKE '%{escaped}%'")
    return " AND ".join(conditions)


def haversine_meters(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate distance between two points in meters."""
    import math
    R = 6371000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda/2)**2
    return 2 * R * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def extract_year_from_filename(filename: str) -> int:
    """Extract year from CSV filename."""
    match = re.search(r'(\d{4})', filename)
    return int(match.group(1)) if match else 0


def get_unique_station_observations(csv_dir: Path, years: list[int], filter_test_stations: bool = True) -> list[dict]:
    """
    Extract unique station observations from raw CSVs.
    Returns all unique (station_id, station_name, lat, lon) combinations with trip counts.

    FIX (Session 6): Filter by year in SQL BEFORE grouping to avoid missing stations
    that exist in multiple years (MIN(filename) was returning earliest year).
    """
    filter_msg = " (excluding test stations)" if filter_test_stations else " (including test stations)"
    print(f"Scanning CSVs for station observations (years: {years}){filter_msg}...")

    con = duckdb.connect()

    # Build year filter for SQL (filter BEFORE grouping)
    year_list = ','.join(map(str, years))

    # Build test station filter
    test_station_filter = f"AND {build_test_station_sql_filter()}" if filter_test_stations else ""

    # Query to get unique station observations
    query = f"""
    WITH raw AS (
        SELECT
            filename,
            CAST(REGEXP_EXTRACT(filename, '(\\d{{4}})', 1) AS INTEGER) as file_year,
            COALESCE(
                CAST("start station id" AS VARCHAR),
                CAST(start_station_id AS VARCHAR)
            ) as station_id,
            COALESCE(
                "start station name",
                start_station_name
            ) as station_name,
            COALESCE(
                "start station latitude",
                start_lat
            ) as lat,
            COALESCE(
                "start station longitude",
                start_lng
            ) as lon
        FROM read_csv_auto('{csv_dir}/*.csv',
            union_by_name=True,
            ignore_errors=true,
            filename=true
        )
        WHERE COALESCE(
            CAST("start station id" AS VARCHAR),
            CAST(start_station_id AS VARCHAR)
        ) IS NOT NULL
    ),
    cleaned AS (
        SELECT
            filename,
            file_year,
            REGEXP_REPLACE(CAST(station_id AS VARCHAR), '\\.0$', '') as station_id,
            station_name,
            ROUND(CAST(lat AS DOUBLE), 6) as lat,
            ROUND(CAST(lon AS DOUBLE), 6) as lon
        FROM raw
        WHERE lat IS NOT NULL AND lon IS NOT NULL
          AND CAST(lat AS DOUBLE) BETWEEN 40.4 AND 41.0
          AND CAST(lon AS DOUBLE) BETWEEN -74.3 AND -73.7
          AND file_year IN ({year_list})  -- Filter by year BEFORE grouping
          {test_station_filter}
    )
    SELECT
        station_id,
        station_name,
        lat,
        lon,
        COUNT(*) as trip_count,
        MIN(filename) as sample_file,
        MIN(file_year) as source_year
    FROM cleaned
    WHERE station_id NOT LIKE '%-%'  -- Exclude UUIDs
      AND LENGTH(station_id) < 20
      AND station_name IS NOT NULL  -- Filter out NULL station names
    GROUP BY station_id, station_name, lat, lon
    ORDER BY trip_count DESC
    """

    result = con.execute(query).fetchall()
    columns = ['station_id', 'station_name', 'lat', 'lon', 'trip_count', 'sample_file', 'source_year']
    observations = [dict(zip(columns, row)) for row in result]

    print(f"Found {len(observations)} unique station observations")
    return observations


def load_crosswalk(csv_path: Path) -> dict:
    """Load crosswalk as a dictionary keyed by legacy_id."""
    crosswalk = {}
    with open(csv_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            crosswalk[row['legacy_id']] = row
    return crosswalk


def load_current_stations(csv_path: Path) -> dict:
    """Load current stations as a dictionary keyed by station_id."""
    stations = {}
    with open(csv_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            stations[row['station_id']] = row
    return stations


def find_nearest_station(lat: float, lon: float, current_stations: dict, n: int = 3) -> list[dict]:
    """Find the n nearest stations to given coordinates."""
    distances = []
    for sid, station in current_stations.items():
        dist = haversine_meters(lat, lon, float(station['lat']), float(station['lon']))
        distances.append({
            'station_id': sid,
            'name': station['name'],
            'lat': float(station['lat']),
            'lon': float(station['lon']),
            'distance_m': round(dist, 1)
        })
    distances.sort(key=lambda x: x['distance_m'])
    return distances[:n]


def classify_match(obs: dict, crosswalk_entry: dict, canonical: dict) -> dict:
    """
    Classify how/why a station matched.
    Returns detailed analysis of the match.
    """
    if not crosswalk_entry or not crosswalk_entry.get('modern_id'):
        return {'match_type': 'none', 'reason': 'No crosswalk entry or no modern_id'}

    modern_id = crosswalk_entry['modern_id']
    if not canonical:
        return {'match_type': 'orphan', 'reason': 'Modern ID not in current stations'}

    # Calculate metrics
    obs_lat, obs_lon = float(obs['lat']), float(obs['lon'])
    canon_lat, canon_lon = float(canonical['lat']), float(canonical['lon'])

    coord_distance = haversine_meters(obs_lat, obs_lon, canon_lat, canon_lon)
    name_similarity = fuzz.token_sort_ratio(
        obs['station_name'].lower() if obs['station_name'] else '',
        canonical['name'].lower()
    )

    # Classify match type
    is_coord_match = coord_distance < 20  # Within 20m
    is_name_match = name_similarity >= 60  # 60% similar

    if is_coord_match and is_name_match:
        match_type = 'both'
        reason = 'Matched by location AND name'
    elif is_coord_match:
        match_type = 'coordinate'
        reason = f'Same location (renamed: "{obs["station_name"]}" → "{canonical["name"]}")'
    elif is_name_match:
        match_type = 'name'
        reason = f'Similar name (coord shift: {coord_distance:.1f}m)'
    else:
        match_type = 'weak'
        reason = f'Weak match (name: {name_similarity}%, dist: {coord_distance:.1f}m)'

    return {
        'match_type': match_type,
        'reason': reason,
        'coord_distance_m': round(coord_distance, 1),
        'name_similarity_pct': name_similarity,
        'canonical_id': modern_id,
        'canonical_name': canonical['name'],
        'canonical_lat': canon_lat,
        'canonical_lon': canon_lon,
    }


def analyze_ghost(obs: dict, current_stations: dict) -> dict:
    """
    Analyze why a station didn't match (ghost station).
    Returns detailed analysis.
    """
    nearest = find_nearest_station(float(obs['lat']), float(obs['lon']), current_stations, n=3)

    if not nearest:
        return {'ghost_reason': 'No nearby stations found'}

    closest = nearest[0]
    name_sim = fuzz.token_sort_ratio(
        obs['station_name'].lower() if obs['station_name'] else '',
        closest['name'].lower()
    )

    # Classify ghost type
    if closest['distance_m'] > 200:
        ghost_type = 'removed'
        reason = f"Station removed (nearest is {closest['distance_m']:.0f}m away: \"{closest['name']}\")"
    elif closest['distance_m'] > 50 and name_sim < 40:
        ghost_type = 'moved_renamed'
        reason = f"Moved and renamed? ({closest['distance_m']:.0f}m to \"{closest['name']}\", {name_sim}% name match)"
    else:
        ghost_type = 'unclear'
        reason = f"Unclear - {closest['distance_m']:.0f}m to \"{closest['name']}\" ({name_sim}% name match)"

    return {
        'ghost_type': ghost_type,
        'ghost_reason': reason,
        'nearest_station_id': closest['station_id'],
        'nearest_station_name': closest['name'],
        'nearest_distance_m': closest['distance_m'],
        'nearest_name_similarity_pct': name_sim,
    }


def generate_report(observations: list[dict], crosswalk: dict, current_stations: dict) -> tuple[list[dict], dict]:
    """
    Generate the full mapping report.
    Returns (detailed_rows, summary_stats).
    """
    print("\nAnalyzing mappings...")

    detailed_rows = []
    stats = {
        'total_observations': len(observations),
        'total_trips': sum(o['trip_count'] for o in observations),
        'matched': {'both': 0, 'coordinate': 0, 'name': 0, 'weak': 0},
        'unmatched': {'removed': 0, 'moved_renamed': 0, 'unclear': 0},
        'matched_trips': 0,
        'unmatched_trips': 0,
        'coord_distances': [],
        'name_similarities': [],
    }

    for obs in observations:
        row = {
            'legacy_id': obs['station_id'],
            'legacy_name': obs['station_name'],
            'legacy_lat': obs['lat'],
            'legacy_lon': obs['lon'],
            'trip_count': obs['trip_count'],
            'source_year': obs.get('source_year', ''),
        }

        # Look up in crosswalk
        xw = crosswalk.get(str(obs['station_id']), {})

        if xw and xw.get('modern_id'):
            # Matched station
            canonical = current_stations.get(xw['modern_id'], {})
            analysis = classify_match(obs, xw, canonical)

            row.update({
                'status': 'matched',
                'match_type': analysis['match_type'],
                'match_reason': analysis['reason'],
                'coord_distance_m': analysis.get('coord_distance_m', ''),
                'name_similarity_pct': analysis.get('name_similarity_pct', ''),
                'canonical_id': analysis.get('canonical_id', ''),
                'canonical_name': analysis.get('canonical_name', ''),
                'canonical_lat': analysis.get('canonical_lat', ''),
                'canonical_lon': analysis.get('canonical_lon', ''),
                'ghost_type': '',
                'ghost_reason': '',
                'nearest_station': '',
                'nearest_distance_m': '',
            })

            stats['matched'][analysis['match_type']] += 1
            stats['matched_trips'] += obs['trip_count']
            if analysis.get('coord_distance_m'):
                stats['coord_distances'].append(analysis['coord_distance_m'])
            if analysis.get('name_similarity_pct'):
                stats['name_similarities'].append(analysis['name_similarity_pct'])
        else:
            # Ghost station
            analysis = analyze_ghost(obs, current_stations)

            row.update({
                'status': 'ghost',
                'match_type': '',
                'match_reason': '',
                'coord_distance_m': '',
                'name_similarity_pct': analysis.get('nearest_name_similarity_pct', ''),
                'canonical_id': '',
                'canonical_name': '',
                'canonical_lat': '',
                'canonical_lon': '',
                'ghost_type': analysis.get('ghost_type', ''),
                'ghost_reason': analysis.get('ghost_reason', ''),
                'nearest_station': analysis.get('nearest_station_name', ''),
                'nearest_distance_m': analysis.get('nearest_distance_m', ''),
            })

            stats['unmatched'][analysis.get('ghost_type', 'unclear')] += 1
            stats['unmatched_trips'] += obs['trip_count']

        detailed_rows.append(row)

    return detailed_rows, stats


def print_summary(stats: dict, detailed_rows: list[dict]):
    """Print a human-readable summary of the report."""
    print("\n" + "=" * 70)
    print("STATION MAPPING REPORT SUMMARY")
    print("=" * 70)

    total = stats['total_observations']
    matched_count = sum(stats['matched'].values())
    unmatched_count = sum(stats['unmatched'].values())

    print(f"\nTotal unique station observations: {total:,}")
    print(f"Total trips covered: {stats['total_trips']:,}")

    print(f"\n--- MATCHED STATIONS: {matched_count} ({100*matched_count/total:.1f}%) ---")
    print(f"  Trips: {stats['matched_trips']:,} ({100*stats['matched_trips']/stats['total_trips']:.1f}%)")
    print(f"  By coordinate (renamed): {stats['matched']['coordinate']}")
    print(f"  By name (coord shift): {stats['matched']['name']}")
    print(f"  By both: {stats['matched']['both']}")
    print(f"  Weak matches: {stats['matched']['weak']}")

    print(f"\n--- GHOST STATIONS: {unmatched_count} ({100*unmatched_count/total:.1f}%) ---")
    print(f"  Trips: {stats['unmatched_trips']:,} ({100*stats['unmatched_trips']/stats['total_trips']:.1f}%)")
    print(f"  Removed (>200m from any station): {stats['unmatched']['removed']}")
    print(f"  Moved + renamed: {stats['unmatched']['moved_renamed']}")
    print(f"  Unclear: {stats['unmatched']['unclear']}")

    if stats['coord_distances']:
        print(f"\n--- COORDINATE DRIFT (matched stations) ---")
        distances = np.array(stats['coord_distances'])
        print(f"  Mean: {np.mean(distances):.1f}m")
        print(f"  Median: {np.median(distances):.1f}m")
        print(f"  Max: {np.max(distances):.1f}m")
        print(f"  <10m: {100*np.sum(distances < 10)/len(distances):.1f}%")
        print(f"  10-50m: {100*np.sum((distances >= 10) & (distances < 50))/len(distances):.1f}%")
        print(f"  50-100m: {100*np.sum((distances >= 50) & (distances < 100))/len(distances):.1f}%")
        print(f"  >100m: {100*np.sum(distances >= 100)/len(distances):.1f}%")

    if stats['name_similarities']:
        print(f"\n--- NAME SIMILARITY (matched stations) ---")
        sims = np.array(stats['name_similarities'])
        print(f"  Mean: {np.mean(sims):.1f}%")
        print(f"  Median: {np.median(sims):.1f}%")
        print(f"  <50%: {100*np.sum(sims < 50)/len(sims):.1f}% (renamed stations)")
        print(f"  50-80%: {100*np.sum((sims >= 50) & (sims < 80))/len(sims):.1f}%")
        print(f"  >80%: {100*np.sum(sims >= 80)/len(sims):.1f}%")

    # Top ghost stations by trip volume
    ghosts = [r for r in detailed_rows if r['status'] == 'ghost']
    ghosts.sort(key=lambda x: x['trip_count'], reverse=True)

    if ghosts:
        print(f"\n--- TOP 10 GHOST STATIONS (by trip volume) ---")
        for g in ghosts[:10]:
            print(f"  {g['legacy_id']:>6} | {g['trip_count']:>8,} trips | {g['legacy_name'][:35]:<35} | {g['ghost_reason'][:40]}")

    # Renamed stations (coordinate match, low name similarity)
    renamed = [r for r in detailed_rows if r['match_type'] == 'coordinate']
    renamed.sort(key=lambda x: x['trip_count'], reverse=True)

    if renamed:
        print(f"\n--- TOP 10 RENAMED STATIONS (matched by location only) ---")
        for r in renamed[:10]:
            print(f"  {r['legacy_id']:>6} | {r['trip_count']:>8,} trips | \"{r['legacy_name'][:25]}\" → \"{r['canonical_name'][:25]}\"")


def save_report(detailed_rows: list[dict], stats: dict, output_dir: Path):
    """Save the detailed report to CSV and summary to JSON."""
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    # Save detailed CSV
    csv_path = output_dir / f"mapping_report_{timestamp}.csv"
    fieldnames = [
        'legacy_id', 'legacy_name', 'legacy_lat', 'legacy_lon', 'trip_count', 'source_year',
        'status', 'match_type', 'match_reason', 'coord_distance_m', 'name_similarity_pct',
        'canonical_id', 'canonical_name', 'canonical_lat', 'canonical_lon',
        'ghost_type', 'ghost_reason', 'nearest_station', 'nearest_distance_m'
    ]

    with open(csv_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(detailed_rows)

    print(f"\n✓ Detailed report saved to: {csv_path}")

    # Save summary JSON
    json_path = output_dir / f"mapping_report_{timestamp}.json"
    summary = {
        'timestamp': datetime.now().isoformat(),
        'total_observations': stats['total_observations'],
        'total_trips': stats['total_trips'],
        'matched_count': sum(stats['matched'].values()),
        'matched_trips': stats['matched_trips'],
        'unmatched_count': sum(stats['unmatched'].values()),
        'unmatched_trips': stats['unmatched_trips'],
        'match_breakdown': stats['matched'],
        'ghost_breakdown': stats['unmatched'],
    }

    with open(json_path, 'w') as f:
        json.dump(summary, f, indent=2)

    print(f"✓ Summary saved to: {json_path}")

    return csv_path


def generate_station_profiles(observations: list[dict], crosswalk: dict, current_stations: dict) -> list[dict]:
    """
    Generate detailed profiles for each station ID, showing:
    - All name variants observed
    - All coordinate variations
    - Mapping details
    """
    from collections import defaultdict

    # Group observations by station ID
    by_id = defaultdict(list)
    for obs in observations:
        by_id[obs['station_id']].append(obs)

    profiles = []

    for station_id, obs_list in by_id.items():
        # Get crosswalk info
        xw = crosswalk.get(str(station_id), {})

        # Aggregate name variants
        name_counts = defaultdict(int)
        coord_counts = defaultdict(int)
        total_trips = 0
        years_seen = set()

        for obs in obs_list:
            name_counts[obs['station_name']] += obs['trip_count']
            coord_key = f"{obs['lat']:.5f},{obs['lon']:.5f}"
            coord_counts[coord_key] += obs['trip_count']
            total_trips += obs['trip_count']
            if obs.get('source_year'):
                years_seen.add(obs['source_year'])

        # Sort by trip count
        names_sorted = sorted(name_counts.items(), key=lambda x: -x[1])
        coords_sorted = sorted(coord_counts.items(), key=lambda x: -x[1])

        # Primary (most common) name and coords
        primary_name = names_sorted[0][0] if names_sorted else ''
        primary_coord = coords_sorted[0][0] if coords_sorted else ''

        # Calculate coordinate spread
        if len(coords_sorted) > 1:
            lats = [float(c.split(',')[0]) for c, _ in coords_sorted]
            lons = [float(c.split(',')[1]) for c, _ in coords_sorted]
            lat_spread = (max(lats) - min(lats)) * 111320  # meters
            lon_spread = (max(lons) - min(lons)) * 85000   # meters at NYC latitude
            coord_spread_m = max(lat_spread, lon_spread)
        else:
            coord_spread_m = 0

        # Mapping status
        if xw and xw.get('modern_id'):
            status = 'matched'
            canonical_id = xw.get('modern_id', '')
            canonical_name = xw.get('modern_name', '')
            canonical = current_stations.get(canonical_id, {})
            canonical_lat = canonical.get('lat', '')
            canonical_lon = canonical.get('lon', '')
            match_confidence = xw.get('match_confidence', '')
            match_distance = xw.get('match_distance_m', '')
        else:
            status = 'ghost'
            canonical_id = ''
            canonical_name = ''
            canonical_lat = ''
            canonical_lon = ''
            match_confidence = ''
            match_distance = ''

        # Build profile row
        profile = {
            'station_id': station_id,
            'total_trips': total_trips,
            'years_observed': ','.join(map(str, sorted(years_seen))),
            'status': status,
            # Name info
            'name_variant_count': len(names_sorted),
            'primary_name': primary_name,
            'primary_name_trips': names_sorted[0][1] if names_sorted else 0,
            'all_names': ' | '.join([f"{n} ({c:,})" for n, c in names_sorted]),
            # Coordinate info
            'coord_variant_count': len(coords_sorted),
            'primary_coord': primary_coord,
            'coord_spread_m': round(coord_spread_m, 1),
            'all_coords': ' | '.join([f"{c} ({cnt:,})" for c, cnt in coords_sorted[:5]]),  # Top 5
            # Mapping info
            'canonical_id': canonical_id,
            'canonical_name': canonical_name,
            'canonical_lat': canonical_lat,
            'canonical_lon': canonical_lon,
            'match_confidence': match_confidence,
            'match_distance_m': match_distance,
            # Crosswalk reference
            'crosswalk_name': xw.get('legacy_name', ''),
        }

        profiles.append(profile)

    # Sort by total trips
    profiles.sort(key=lambda x: -x['total_trips'])

    return profiles


def save_detailed_profiles(profiles: list[dict], output_dir: Path, years: list[int]):
    """Save detailed station profiles to CSV."""
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    year_str = f"{min(years)}-{max(years)}" if len(years) > 1 else str(years[0])

    csv_path = output_dir / f"station_profiles_{year_str}_{timestamp}.csv"

    fieldnames = [
        'station_id', 'total_trips', 'years_observed', 'status',
        'name_variant_count', 'primary_name', 'primary_name_trips', 'all_names',
        'coord_variant_count', 'primary_coord', 'coord_spread_m', 'all_coords',
        'canonical_id', 'canonical_name', 'canonical_lat', 'canonical_lon',
        'match_confidence', 'match_distance_m', 'crosswalk_name'
    ]

    with open(csv_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(profiles)

    print(f"\n✓ Station profiles saved to: {csv_path}")

    # Print summary of interesting findings
    renamed = [p for p in profiles if p['name_variant_count'] > 1]
    moved = [p for p in profiles if p['coord_spread_m'] > 50]
    ghosts = [p for p in profiles if p['status'] == 'ghost']

    print(f"\n--- STATION PROFILE SUMMARY ---")
    print(f"  Total unique station IDs: {len(profiles)}")
    print(f"  With multiple names: {len(renamed)} ({100*len(renamed)/len(profiles):.1f}%)")
    print(f"  With coord spread >50m: {len(moved)} ({100*len(moved)/len(profiles):.1f}%)")
    print(f"  Ghost stations: {len(ghosts)} ({100*len(ghosts)/len(profiles):.1f}%)")

    if renamed:
        print(f"\n--- TOP 5 STATIONS WITH NAME CHANGES ---")
        renamed.sort(key=lambda x: -x['name_variant_count'])
        for p in renamed[:5]:
            print(f"  {p['station_id']:>5} | {p['name_variant_count']} names | {p['total_trips']:>8,} trips | {p['all_names'][:70]}")

    if moved:
        print(f"\n--- TOP 5 STATIONS WITH COORDINATE DRIFT ---")
        moved.sort(key=lambda x: -x['coord_spread_m'])
        for p in moved[:5]:
            print(f"  {p['station_id']:>5} | {p['coord_spread_m']:>6.1f}m spread | {p['total_trips']:>8,} trips | {p['primary_name'][:40]}")

    return csv_path


def main():
    parser = argparse.ArgumentParser(description="Generate station mapping report")
    parser.add_argument("--years", type=int, nargs='+', help="Years to analyze (e.g., 2013 2014 2015)")
    parser.add_argument("--all", action='store_true', help="Analyze all available years")
    parser.add_argument("--detail", action='store_true', help="Generate detailed station ID profiles")
    parser.add_argument("--include-test", action='store_true', help="Include test/internal stations (filtered by default)")
    parser.add_argument("--csv-dir", type=Path, default=DATA_DIR / "raw_csvs")
    parser.add_argument("--output-dir", type=Path, default=LOGS_DIR)

    args = parser.parse_args()
    filter_test_stations = not args.include_test

    if args.all:
        years = list(range(2013, 2026))
    elif args.years:
        years = args.years
    else:
        print("Please specify --years or --all")
        exit(1)

    # Load reference data
    crosswalk_path = REFERENCE_DIR / "station_crosswalk.csv"
    stations_path = REFERENCE_DIR / "current_stations.csv"

    if not crosswalk_path.exists():
        print(f"✗ Crosswalk not found: {crosswalk_path}")
        print("  Run: python src/build_crosswalk.py")
        exit(1)

    if not stations_path.exists():
        print(f"✗ Current stations not found: {stations_path}")
        print("  Run: python src/fetch_stations.py")
        exit(1)

    crosswalk = load_crosswalk(crosswalk_path)
    current_stations = load_current_stations(stations_path)

    print(f"Loaded crosswalk: {len(crosswalk)} entries")
    print(f"Loaded current stations: {len(current_stations)} stations")

    # Extract observations from raw CSVs
    observations = get_unique_station_observations(args.csv_dir, years, filter_test_stations)

    if not observations:
        print("No station observations found for the specified years")
        exit(1)

    # Generate report
    detailed_rows, stats = generate_report(observations, crosswalk, current_stations)

    # Print summary
    print_summary(stats, detailed_rows)

    # Save outputs
    csv_path = save_report(detailed_rows, stats, args.output_dir)

    # Generate detailed station profiles if requested
    if args.detail:
        print("\n" + "=" * 70)
        print("GENERATING DETAILED STATION PROFILES")
        print("=" * 70)
        profiles = generate_station_profiles(observations, crosswalk, current_stations)
        save_detailed_profiles(profiles, args.output_dir, years)

    print(f"\n✓ Report complete! Open {csv_path} in a spreadsheet to explore.")


if __name__ == "__main__":
    main()
