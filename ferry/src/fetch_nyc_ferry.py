#!/usr/bin/env python3
"""
Download NYC Ferry ridership data from NYC Open Data (Socrata API).

NYC Ferry dataset: https://data.cityofnewyork.us/Transportation/NYC-Ferry-Ridership/t5n6-gx8c

Schema:
- date: Date of service
- route: Ferry route name (e.g., "SV" = Soundview)
- direction: Direction (NB/SB = Northbound/Southbound, EB/WB = East/West)
- stop: Ferry stop/terminal name
- hour: Hour (0-23)
- boardings: Number of passengers
- typeday: Day type (Weekday/Saturday/Sunday)

Coverage: June 2017 - present
Granularity: Hourly boardings by landing
"""

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests


# Socrata API endpoint
API_ENDPOINT = "https://data.cityofnewyork.us/resource/t5n6-gx8c.json"
API_LIMIT = 50000  # Socrata max rows per request


def fetch_nyc_ferry(
    start_date: str = None,
    end_date: str = None,
    route: str = None,
    output_dir: Path = None
) -> pd.DataFrame:
    """
    Download NYC Ferry ridership data from Socrata API.

    Args:
        start_date: Start date (YYYY-MM-DD) for filtering
        end_date: End date (YYYY-MM-DD) for filtering
        route: Ferry route name for filtering (e.g., "Astoria")
        output_dir: Directory to save output file

    Returns:
        DataFrame with NYC Ferry ridership data
    """

    print("Fetching NYC Ferry ridership data...")
    print(f"API endpoint: {API_ENDPOINT}")

    # Build query parameters
    params = {"$limit": API_LIMIT, "$offset": 0}

    # Build WHERE clause for filtering
    where_clauses = []
    if start_date:
        where_clauses.append(f"date >= '{start_date}'")
    if end_date:
        where_clauses.append(f"date <= '{end_date}'")
    if route:
        where_clauses.append(f"route = '{route}'")

    if where_clauses:
        params["$where"] = " AND ".join(where_clauses)

    # Fetch data with pagination
    all_data = []
    offset = 0

    while True:
        params["$offset"] = offset
        print(f"  Fetching rows {offset:,} - {offset + API_LIMIT:,}...")

        response = requests.get(API_ENDPOINT, params=params)
        response.raise_for_status()

        data = response.json()
        if not data:
            break

        all_data.extend(data)
        offset += len(data)

        if len(data) < API_LIMIT:
            # Last page
            break

    print(f"Fetched {len(all_data):,} total rows")

    # Convert to DataFrame
    df = pd.DataFrame(all_data)

    # Convert date column to datetime
    df['date'] = pd.to_datetime(df['date'])

    # Convert hour to integer (handle NaN)
    df['hour'] = pd.to_numeric(df['hour'], errors='coerce').fillna(-1).astype(int)

    # Convert boardings to integer (handle NaN)
    df['boardings'] = pd.to_numeric(df['boardings'], errors='coerce').fillna(0).astype(int)

    # Track data quality issues
    missing_hour = (df['hour'] == -1).sum()
    missing_boardings = (df['boardings'] == 0).sum()

    if missing_hour > 0:
        print(f"  Warning: {missing_hour:,} rows with missing hour (set to -1)")
    if missing_boardings > 0:
        print(f"  Warning: {missing_boardings:,} rows with missing/zero boardings")

    # Sort by date, route, stop, hour
    df = df.sort_values(['date', 'route', 'stop', 'hour'])

    return df


def main():
    parser = argparse.ArgumentParser(
        description="Download NYC Ferry ridership data from NYC Open Data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Download all data
  python fetch_nyc_ferry.py

  # Download specific date range
  python fetch_nyc_ferry.py --start 2024-01-01 --end 2024-12-31

  # Download specific route
  python fetch_nyc_ferry.py --route "Astoria"

  # Download with custom output directory
  python fetch_nyc_ferry.py --output ../data/nyc_ferry
        """
    )

    parser.add_argument(
        '--start',
        type=str,
        help='Start date (YYYY-MM-DD)'
    )

    parser.add_argument(
        '--end',
        type=str,
        help='End date (YYYY-MM-DD)'
    )

    parser.add_argument(
        '--route',
        type=str,
        help='Ferry route name (e.g., "Astoria", "Rockaway")'
    )

    parser.add_argument(
        '--output',
        type=Path,
        default=Path(__file__).parent.parent / "data" / "nyc_ferry",
        help='Output directory (default: ../data/nyc_ferry)'
    )

    args = parser.parse_args()

    try:
        # Create output directory
        args.output.mkdir(parents=True, exist_ok=True)

        # Fetch data
        df = fetch_nyc_ferry(
            start_date=args.start,
            end_date=args.end,
            route=args.route,
            output_dir=args.output
        )

        # Save to parquet
        output_file = args.output / "ridership.parquet"
        df.to_parquet(output_file, index=False)
        print(f"\nSaved {len(df):,} rows to {output_file}")

        # Save metadata
        metadata = {
            "downloaded_at": datetime.now().isoformat(),
            "source": API_ENDPOINT,
            "filters": {
                "start_date": args.start,
                "end_date": args.end,
                "route": args.route
            },
            "row_count": len(df),
            "date_range": {
                "min": df['date'].min().isoformat(),
                "max": df['date'].max().isoformat()
            },
            "routes": sorted(df['route'].unique().tolist()),
            "stops": sorted(df['stop'].unique().tolist())
        }

        metadata_file = args.output / "metadata.json"
        with open(metadata_file, 'w') as f:
            json.dump(metadata, f, indent=2)
        print(f"Saved metadata to {metadata_file}")

        # Print summary
        print("\nSummary:")
        print(f"  Date range: {metadata['date_range']['min']} to {metadata['date_range']['max']}")
        print(f"  Routes: {len(metadata['routes'])}")
        print(f"  Stops: {len(metadata['stops'])}")
        print(f"  Total boardings: {df['boardings'].sum():,}")

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
