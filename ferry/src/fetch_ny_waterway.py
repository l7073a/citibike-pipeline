#!/usr/bin/env python3
"""
Download private ferry monthly passenger counts from NYC Open Data (Socrata API).

Dataset: https://data.cityofnewyork.us/Transportation/Private-Ferry-Monthly-Passenger-Counts/hn6c-5qkb

Schema:
- month: Month (date)
- operator: Ferry operator name
- route_or_terminal: Route/terminal description
- passengers: Monthly passenger count

Operators included:
- NY Waterway
- Hornblower
- SeaStreak
- Liberty Landing Ferry
- Others

Coverage: Varies by operator (NY Waterway: 2015+)
Granularity: Monthly totals by operator
"""

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests


# Socrata API endpoint
API_ENDPOINT = "https://data.cityofnewyork.us/resource/hn6c-5qkb.json"
API_LIMIT = 50000  # Socrata max rows per request


def fetch_private_ferry(
    start_month: str = None,
    end_month: str = None,
    operator: str = None,
    output_dir: Path = None
) -> pd.DataFrame:
    """
    Download private ferry monthly counts from Socrata API.

    Args:
        start_month: Start month (YYYY-MM) for filtering
        end_month: End month (YYYY-MM) for filtering
        operator: Ferry operator name for filtering (e.g., "NY Waterway")
        output_dir: Directory to save output file

    Returns:
        DataFrame with private ferry monthly passenger counts
    """

    print("Fetching private ferry monthly passenger counts...")
    print(f"API endpoint: {API_ENDPOINT}")

    # Build query parameters
    params = {"$limit": API_LIMIT, "$offset": 0}

    # Build WHERE clause for filtering
    where_clauses = []
    if start_month:
        where_clauses.append(f"month >= '{start_month}-01'")
    if end_month:
        # Add to last day of month
        where_clauses.append(f"month <= '{end_month}-31'")
    if operator:
        where_clauses.append(f"operator = '{operator}'")

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

    # Convert month column to datetime
    df['month'] = pd.to_datetime(df['month'])

    # Convert passengers to integer
    df['passengers'] = df['passengers'].astype(int)

    # Sort by month, operator
    df = df.sort_values(['month', 'operator', 'route_or_terminal'])

    return df


def main():
    parser = argparse.ArgumentParser(
        description="Download private ferry monthly counts from NYC Open Data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Download all data
  python fetch_ny_waterway.py

  # Download specific date range
  python fetch_ny_waterway.py --start 2024-01 --end 2024-12

  # Download specific operator
  python fetch_ny_waterway.py --operator "NY Waterway"

  # Download with custom output directory
  python fetch_ny_waterway.py --output ../data/ny_waterway
        """
    )

    parser.add_argument(
        '--start',
        type=str,
        help='Start month (YYYY-MM)'
    )

    parser.add_argument(
        '--end',
        type=str,
        help='End month (YYYY-MM)'
    )

    parser.add_argument(
        '--operator',
        type=str,
        help='Ferry operator name (e.g., "NY Waterway", "SeaStreak")'
    )

    parser.add_argument(
        '--output',
        type=Path,
        default=Path(__file__).parent.parent / "data" / "ny_waterway",
        help='Output directory (default: ../data/ny_waterway)'
    )

    args = parser.parse_args()

    try:
        # Create output directory
        args.output.mkdir(parents=True, exist_ok=True)

        # Fetch data
        df = fetch_private_ferry(
            start_month=args.start,
            end_month=args.end,
            operator=args.operator,
            output_dir=args.output
        )

        # Save to parquet
        output_file = args.output / "monthly_counts.parquet"
        df.to_parquet(output_file, index=False)
        print(f"\nSaved {len(df):,} rows to {output_file}")

        # Save metadata
        metadata = {
            "downloaded_at": datetime.now().isoformat(),
            "source": API_ENDPOINT,
            "filters": {
                "start_month": args.start,
                "end_month": args.end,
                "operator": args.operator
            },
            "row_count": len(df),
            "date_range": {
                "min": df['month'].min().isoformat(),
                "max": df['month'].max().isoformat()
            },
            "operators": sorted(df['operator'].unique().tolist()),
            "total_passengers": int(df['passengers'].sum())
        }

        metadata_file = args.output / "metadata.json"
        with open(metadata_file, 'w') as f:
            json.dump(metadata, f, indent=2)
        print(f"Saved metadata to {metadata_file}")

        # Print summary
        print("\nSummary:")
        print(f"  Date range: {metadata['date_range']['min'][:7]} to {metadata['date_range']['max'][:7]}")
        print(f"  Operators: {len(metadata['operators'])}")
        for op in metadata['operators']:
            op_total = df[df['operator'] == op]['passengers'].sum()
            print(f"    {op}: {op_total:,} passengers")
        print(f"  Total passengers: {metadata['total_passengers']:,}")

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
