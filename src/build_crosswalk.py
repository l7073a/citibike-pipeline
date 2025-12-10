#!/usr/bin/env python3
"""
Build a crosswalk mapping legacy station IDs (integers) to modern UUIDs.

Since there is NO common key between the two eras, we must match by:
1. Spatial proximity (within 150m)
2. Name similarity (fuzzy matching)

This script:
1. Extracts unique legacy stations from raw CSVs (using MEDIAN coords to filter GPS noise)
2. Loads current stations from GBFS API
3. For each legacy station, finds the best matching modern station
4. Outputs a crosswalk CSV that gets version-controlled

IMPORTANT: Review the output! Some matches will need manual correction.
"""

import argparse
import csv
import json
import math
from datetime import datetime
from pathlib import Path
from typing import Optional

try:
    import duckdb
    from rapidfuzz import fuzz
    from scipy.spatial import cKDTree
    import numpy as np
except ImportError as e:
    print(f"Missing dependency: {e}")
    print("Run: pip install duckdb rapidfuzz scipy numpy")
    exit(1)

REFERENCE_DIR = Path(__file__).parent.parent / "reference"
DATA_DIR = Path(__file__).parent.parent / "data"
LOGS_DIR = Path(__file__).parent.parent / "logs"


def haversine_meters(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate distance between two points in meters."""
    R = 6371000  # Earth radius in meters
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    
    a = math.sin(dphi/2)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda/2)**2
    return 2 * R * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def extract_legacy_stations(csv_dir: Path) -> list[dict]:
    """
    Extract unique legacy stations from raw CSVs using DuckDB.
    Uses MEDIAN for coordinates to filter GPS noise.
    """
    print("Scanning CSVs for legacy stations...")
    
    con = duckdb.connect()
    
    # DuckDB query that:
    # 1. Reads all CSVs with union_by_name (handles schema differences)
    # 2. Filters to legacy IDs only (integers, not UUIDs with dashes)
    # 3. Groups by station ID, taking MODE of name and MEDIAN of coords
    query = f"""
    WITH raw AS (
        SELECT
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
        FROM read_csv_auto('{csv_dir}/*.csv', union_by_name=True, ignore_errors=True)
        WHERE station_id IS NOT NULL
    ),
    cleaned AS (
        SELECT
            -- Remove .0 suffix from float conversion
            REGEXP_REPLACE(CAST(station_id AS VARCHAR), '\\.0$', '') as station_id,
            station_name,
            CAST(lat AS DOUBLE) as lat,
            CAST(lon AS DOUBLE) as lon
        FROM raw
        WHERE lat IS NOT NULL AND lon IS NOT NULL
          AND lat BETWEEN 40.4 AND 41.0  -- NYC bounds
          AND lon BETWEEN -74.3 AND -73.7
    )
    SELECT 
        station_id as legacy_id,
        MODE(station_name) as legacy_name,
        MEDIAN(lat) as legacy_lat,
        MEDIAN(lon) as legacy_lon,
        COUNT(*) as trip_count
    FROM cleaned
    WHERE station_id NOT LIKE '%-%'  -- Exclude UUIDs (modern IDs contain dashes)
      AND LENGTH(station_id) < 10    -- Legacy IDs are short integers
    GROUP BY station_id
    HAVING COUNT(*) >= 10  -- Filter out rare/erroneous IDs
    ORDER BY trip_count DESC
    """
    
    try:
        result = con.execute(query).fetchall()
        columns = ['legacy_id', 'legacy_name', 'legacy_lat', 'legacy_lon', 'trip_count']
        stations = [dict(zip(columns, row)) for row in result]
        print(f"Found {len(stations)} unique legacy stations")
        return stations
    except Exception as e:
        print(f"Error querying CSVs: {e}")
        raise


def load_modern_stations(csv_path: Path) -> list[dict]:
    """Load current stations from the GBFS export."""
    stations = []
    with open(csv_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            stations.append({
                'station_id': row['station_id'],
                'name': row['name'],
                'lat': float(row['lat']),
                'lon': float(row['lon']),
            })
    print(f"Loaded {len(stations)} modern stations from {csv_path.name}")
    return stations


def build_spatial_index(stations: list[dict]) -> tuple:
    """Build a KDTree for fast spatial lookups."""
    coords = np.array([[s['lat'], s['lon']] for s in stations])
    tree = cKDTree(coords)
    return tree, coords


def match_station(
    legacy: dict,
    modern_stations: list[dict],
    tree: cKDTree,
    max_distance_m: float = 150,
    min_name_score: float = 60
) -> Optional[dict]:
    """
    Find the best matching modern station for a legacy station.
    
    Uses a "double lock" approach:
    1. Find candidates within max_distance_m
    2. Score by name similarity
    3. Return best match if it exceeds thresholds
    """
    # Query the KDTree for nearby stations
    # Convert meters to approximate degrees (at NYC latitude)
    # 1 degree latitude ≈ 111km, 1 degree longitude ≈ 85km at 40.7°N
    degree_radius = max_distance_m / 85000  # Conservative (uses smaller dimension)
    
    legacy_coords = [legacy['legacy_lat'], legacy['legacy_lon']]
    distances, indices = tree.query(legacy_coords, k=5, distance_upper_bound=degree_radius)
    
    best_match = None
    best_score = 0
    best_distance = float('inf')
    
    for dist_deg, idx in zip(distances, indices):
        if idx >= len(modern_stations):  # KDTree padding
            continue
        
        candidate = modern_stations[idx]
        
        # Calculate actual distance in meters
        distance_m = haversine_meters(
            legacy['legacy_lat'], legacy['legacy_lon'],
            candidate['lat'], candidate['lon']
        )
        
        if distance_m > max_distance_m:
            continue
        
        # Calculate name similarity
        name_score = fuzz.token_sort_ratio(
            legacy['legacy_name'].lower(),
            candidate['name'].lower()
        )
        
        # Combined score: weight name more heavily, but penalize distance
        # Score range: 0-100
        proximity_score = 100 * (1 - distance_m / max_distance_m)
        combined_score = (name_score * 0.7) + (proximity_score * 0.3)
        
        if combined_score > best_score:
            best_score = combined_score
            best_distance = distance_m
            best_match = {
                'modern_id': candidate['station_id'],
                'modern_name': candidate['name'],
                'match_score': round(combined_score, 1),
                'name_score': name_score,
                'distance_m': round(distance_m, 1),
            }
    
    # Apply matching rules with clear tiers:
    #
    # Tier 1: <20m distance = same location (match regardless of name)
    #         Example: "Pershing Square North" → "Park Ave & E 42 St" (0.0m)
    #
    # Tier 2: 20-50m distance AND >50% name similarity = likely same station
    #         Example: "W 14 St & The High Line" → "10 Ave & W 14 St" (24m, 56%)
    #
    # Tier 3: 50-150m distance AND >60% name similarity = station may have moved
    #         Example: slight relocations with similar naming
    #
    # Tier 4: >150m OR low name similarity = NO MATCH (ghost station)
    #
    if best_match:
        dist = best_match['distance_m']
        name_score = best_match['name_score']

        # Tier 1: Very close = definite match
        if dist < 20:
            best_match['match_confidence'] = 'high'
            return best_match

        # Tier 2: Close + reasonable name similarity
        if dist < 50 and name_score >= 50:
            best_match['match_confidence'] = 'high' if name_score >= 70 else 'medium'
            return best_match

        # Tier 3: Medium distance + good name similarity
        if dist < 150 and name_score >= min_name_score:
            best_match['match_confidence'] = 'medium' if name_score >= 80 else 'low'
            return best_match

        # Tier 4: Too far or names too different - no match

    return None


def build_crosswalk(
    legacy_stations: list[dict],
    modern_stations: list[dict]
) -> tuple[list[dict], list[dict]]:
    """Build the crosswalk by matching each legacy station to modern."""
    print("\nBuilding crosswalk...")
    
    tree, coords = build_spatial_index(modern_stations)
    
    crosswalk = []
    ghosts = []
    
    for i, legacy in enumerate(legacy_stations):
        if (i + 1) % 100 == 0:
            print(f"  Processed {i + 1}/{len(legacy_stations)} stations...")
        
        match = match_station(legacy, modern_stations, tree)
        
        row = {
            'legacy_id': legacy['legacy_id'],
            'legacy_name': legacy['legacy_name'],
            'legacy_lat': legacy['legacy_lat'],
            'legacy_lon': legacy['legacy_lon'],
            'trip_count': legacy['trip_count'],
            'modern_id': match['modern_id'] if match else '',
            'modern_name': match['modern_name'] if match else '',
            'match_score': match['match_score'] if match else 0,
            'match_confidence': match['match_confidence'] if match else 'none',
            'match_distance_m': match['distance_m'] if match else 0,
        }
        
        crosswalk.append(row)
        
        if not match:
            ghosts.append(legacy)
    
    return crosswalk, ghosts


def save_outputs(crosswalk: list[dict], ghosts: list[dict]):
    """Save crosswalk CSV and audit log."""
    REFERENCE_DIR.mkdir(parents=True, exist_ok=True)
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    
    # Save crosswalk CSV
    csv_path = REFERENCE_DIR / "station_crosswalk.csv"
    fieldnames = [
        'legacy_id', 'legacy_name', 'legacy_lat', 'legacy_lon', 'trip_count',
        'modern_id', 'modern_name', 'match_score', 'match_confidence', 'match_distance_m'
    ]
    
    with open(csv_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(crosswalk)
    
    print(f"\n✓ Saved crosswalk to {csv_path}")
    
    # Create empty manual overrides template if it doesn't exist
    overrides_path = REFERENCE_DIR / "manual_overrides.csv"
    if not overrides_path.exists():
        with open(overrides_path, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
        print(f"✓ Created empty {overrides_path.name}")
    
    # Calculate stats
    matched = sum(1 for r in crosswalk if r['modern_id'])
    high_conf = sum(1 for r in crosswalk if r['match_confidence'] == 'high')
    med_conf = sum(1 for r in crosswalk if r['match_confidence'] == 'medium')
    low_conf = sum(1 for r in crosswalk if r['match_confidence'] == 'low')
    
    # Save audit log
    log_path = LOGS_DIR / f"crosswalk_build_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    log_data = {
        'timestamp': datetime.now().isoformat(),
        'legacy_stations_found': len(crosswalk),
        'matched': matched,
        'match_rate': f"{100 * matched / len(crosswalk):.1f}%",
        'confidence_breakdown': {
            'high': high_conf,
            'medium': med_conf,
            'low': low_conf,
        },
        'ghost_stations': len(ghosts),
        'ghost_ids': [g['legacy_id'] for g in ghosts],
        'low_confidence_stations': [
            {'id': r['legacy_id'], 'name': r['legacy_name'], 'score': r['match_score']}
            for r in crosswalk if r['match_confidence'] == 'low'
        ],
    }
    
    with open(log_path, 'w') as f:
        json.dump(log_data, f, indent=2)
    
    # Print summary
    print(f"\n=== Crosswalk Summary ===")
    print(f"Total legacy stations: {len(crosswalk)}")
    print(f"Matched: {matched} ({100 * matched / len(crosswalk):.1f}%)")
    print(f"  High confidence: {high_conf}")
    print(f"  Medium confidence: {med_conf}")
    print(f"  Low confidence: {low_conf}")
    print(f"Ghost stations (no match): {len(ghosts)}")
    
    if low_conf > 0:
        print(f"\n⚠ Review {low_conf} low-confidence matches in the crosswalk CSV")
    if len(ghosts) > 0:
        print(f"⚠ Review {len(ghosts)} ghost stations (closed/moved)")
    
    print(f"\n✓ Audit log saved to {log_path}")


def main():
    parser = argparse.ArgumentParser(description="Build station crosswalk")
    parser.add_argument("--csv-dir", type=Path, default=DATA_DIR / "raw_csvs",
                        help="Directory with raw CSVs")
    parser.add_argument("--stations", type=Path, default=REFERENCE_DIR / "current_stations.csv",
                        help="Current stations CSV from GBFS")
    
    args = parser.parse_args()
    
    if not args.stations.exists():
        print(f"✗ Current stations not found: {args.stations}")
        print("  Run: python src/fetch_stations.py")
        exit(1)
    
    csv_files = list(args.csv_dir.glob("*.csv"))
    if not csv_files:
        print(f"✗ No CSV files found in {args.csv_dir}")
        print("  Run: python src/download.py --year 2014")
        print("  Then: python src/ingest.py")
        exit(1)
    
    print(f"Found {len(csv_files)} CSV files in {args.csv_dir}")
    
    # Extract legacy stations from raw data
    legacy_stations = extract_legacy_stations(args.csv_dir)
    
    # Load modern stations from GBFS
    modern_stations = load_modern_stations(args.stations)
    
    # Build crosswalk
    crosswalk, ghosts = build_crosswalk(legacy_stations, modern_stations)
    
    # Save outputs
    save_outputs(crosswalk, ghosts)


if __name__ == "__main__":
    main()
