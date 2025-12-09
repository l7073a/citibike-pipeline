#!/usr/bin/env python3
"""
Fetch current Citi Bike station data from the GBFS API.
This becomes the "ground truth" for station names and coordinates.
"""

import json
import os
import requests
from datetime import datetime
from pathlib import Path

GBFS_URL = "https://gbfs.citibikenyc.com/gbfs/en/station_information.json"
REFERENCE_DIR = Path(__file__).parent.parent / "reference"


def fetch_stations():
    """Fetch station data from GBFS API."""
    print(f"Fetching station data from {GBFS_URL}...")
    
    response = requests.get(GBFS_URL, timeout=30)
    response.raise_for_status()
    data = response.json()
    
    stations = data['data']['stations']
    print(f"Retrieved {len(stations)} stations")
    
    return data, stations


def save_outputs(raw_data, stations):
    """Save both raw JSON (for audit) and clean CSV (for use)."""
    REFERENCE_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # 1. Save raw JSON for audit trail
    raw_path = REFERENCE_DIR / f"gbfs_stations_raw_{timestamp}.json"
    with open(raw_path, 'w') as f:
        json.dump(raw_data, f, indent=2)
    print(f"Saved raw API response to {raw_path}")
    
    # 2. Save clean CSV for pipeline use
    csv_path = REFERENCE_DIR / "current_stations.csv"
    
    # Extract relevant fields
    rows = []
    for s in stations:
        rows.append({
            'station_id': s['station_id'],
            'short_name': s.get('short_name', ''),
            'name': s['name'],
            'lat': s['lat'],
            'lon': s['lon'],
            'capacity': s.get('capacity', 0),
            'region_id': s.get('region_id', ''),
        })
    
    # Write CSV (avoiding pandas dependency for this simple task)
    headers = ['station_id', 'short_name', 'name', 'lat', 'lon', 'capacity', 'region_id']
    with open(csv_path, 'w') as f:
        f.write(','.join(headers) + '\n')
        for row in rows:
            # Escape commas and quotes in name field
            name = row['name'].replace('"', '""')
            if ',' in name:
                name = f'"{name}"'
            values = [
                row['station_id'],
                row['short_name'],
                name,
                str(row['lat']),
                str(row['lon']),
                str(row['capacity']),
                row['region_id'],
            ]
            f.write(','.join(values) + '\n')
    
    print(f"Saved clean station list to {csv_path}")
    
    return csv_path


def analyze_stations(stations):
    """Print summary statistics about the station data."""
    print("\n=== Station Summary ===")
    print(f"Total stations: {len(stations)}")
    
    # Check short_name format to confirm it's NOT legacy IDs
    short_names = [s.get('short_name', '') for s in stations[:10]]
    print(f"\nSample short_name values (NOT legacy IDs!):")
    for sn in short_names[:5]:
        print(f"  {sn}")
    
    # Check for integer-looking short_names (there shouldn't be any)
    integer_like = sum(1 for s in stations if s.get('short_name', '').isdigit())
    print(f"\nShort names that look like integers: {integer_like}")
    
    # Lat/lon bounds
    lats = [s['lat'] for s in stations]
    lons = [s['lon'] for s in stations]
    print(f"\nCoordinate bounds:")
    print(f"  Latitude:  {min(lats):.4f} to {max(lats):.4f}")
    print(f"  Longitude: {min(lons):.4f} to {max(lons):.4f}")


def main():
    try:
        raw_data, stations = fetch_stations()
        save_outputs(raw_data, stations)
        analyze_stations(stations)
        print("\n✓ Station data fetched successfully")
    except requests.RequestException as e:
        print(f"✗ Failed to fetch station data: {e}")
        raise


if __name__ == "__main__":
    main()
