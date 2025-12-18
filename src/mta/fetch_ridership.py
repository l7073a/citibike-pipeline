#!/usr/bin/env python3
"""
Fetch MTA Subway Hourly Ridership Data

Downloads ridership data from NY Open Data (data.ny.gov).
Data includes hourly entries by station complex, with payment method breakdown.

Data source: https://data.ny.gov/Transportation/MTA-Subway-Hourly-Ridership-Beginning-February-202/wujg-7c2s
Coverage: February 2022 - present
Update frequency: Monthly

Usage:
    # Download all available data (WARNING: 50M+ rows, may take a while)
    python src/mta/fetch_ridership.py

    # Download specific date range
    python src/mta/fetch_ridership.py --start 2024-01-01 --end 2024-12-31

    # Download specific borough only
    python src/mta/fetch_ridership.py --borough Manhattan

    # Limit rows (for testing)
    python src/mta/fetch_ridership.py --limit 100000
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

# Socrata API endpoint for MTA Hourly Ridership
DATASET_ID = "wujg-7c2s"
BASE_URL = f"https://data.ny.gov/resource/{DATASET_ID}.csv"

DEFAULT_OUTPUT_DIR = "data/mta/ridership"


def build_query_url(
    start_date: str = None,
    end_date: str = None,
    borough: str = None,
    limit: int = None,
    offset: int = 0
) -> str:
    """
    Build Socrata API query URL with filters.

    Args:
        start_date: Filter start (YYYY-MM-DD)
        end_date: Filter end (YYYY-MM-DD)
        borough: Filter by borough name
        limit: Max rows to return
        offset: Pagination offset

    Returns:
        URL string with query parameters
    """
    params = []

    # Build WHERE clause
    where_parts = []
    if start_date:
        where_parts.append(f"transit_timestamp >= '{start_date}T00:00:00'")
    if end_date:
        where_parts.append(f"transit_timestamp <= '{end_date}T23:59:59'")
    if borough:
        where_parts.append(f"borough = '{borough}'")

    if where_parts:
        where_clause = " AND ".join(where_parts)
        params.append(f"$where={where_clause}")

    # Pagination
    if limit:
        params.append(f"$limit={limit}")
    else:
        params.append("$limit=50000")  # Socrata max per request

    if offset > 0:
        params.append(f"$offset={offset}")

    # Order by time for consistent pagination
    params.append("$order=transit_timestamp")

    if params:
        return BASE_URL + "?" + "&".join(params)
    return BASE_URL


def fetch_ridership(
    output_dir: str,
    start_date: str = None,
    end_date: str = None,
    borough: str = None,
    limit: int = None,
    force: bool = False
) -> dict:
    """
    Download MTA ridership data and save as parquet.

    Uses DuckDB to stream CSV directly to parquet for efficiency.
    Handles pagination for large datasets.

    Args:
        output_dir: Directory to save parquet file
        start_date: Optional start date filter (YYYY-MM-DD)
        end_date: Optional end date filter (YYYY-MM-DD)
        borough: Optional borough filter
        limit: Optional row limit
        force: Overwrite existing files

    Returns:
        dict with download stats
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    # Build output filename based on filters
    filename_parts = ["mta_hourly_ridership"]
    if start_date and end_date:
        filename_parts.append(f"{start_date}_to_{end_date}")
    elif start_date:
        filename_parts.append(f"from_{start_date}")
    elif end_date:
        filename_parts.append(f"to_{end_date}")
    if borough:
        filename_parts.append(borough.lower())

    output_file = output_path / f"{'_'.join(filename_parts)}.parquet"

    if output_file.exists() and not force:
        print(f"Output file already exists: {output_file}")
        print("Use --force to overwrite")
        return {"status": "skipped", "reason": "file_exists"}

    print("Fetching MTA Subway Hourly Ridership...")
    print(f"  Date range: {start_date or 'all'} to {end_date or 'present'}")
    print(f"  Borough: {borough or 'all'}")
    print(f"  Row limit: {limit or 'none'}")

    con = duckdb.connect()

    # For large datasets, we need to paginate
    # Socrata has a 50,000 row limit per request

    if limit and limit <= 50000:
        # Single request
        url = build_query_url(start_date, end_date, borough, limit)
        print(f"\nDownloading from: {url[:100]}...")

        try:
            result = con.execute(f"""
                COPY (
                    SELECT * FROM read_csv_auto('{url}')
                ) TO '{output_file}' (FORMAT PARQUET, COMPRESSION ZSTD)
            """)

            # Get row count
            row_count = con.execute(f"SELECT COUNT(*) FROM '{output_file}'").fetchone()[0]

        except Exception as e:
            print(f"Error downloading: {e}")
            return {"status": "error", "error": str(e)}

    else:
        # Paginated download for large datasets
        print("\nDownloading in batches (50,000 rows each)...")

        offset = 0
        batch_size = 50000
        total_rows = 0
        batch_num = 0
        temp_files = []

        max_rows = limit if limit else float('inf')

        while total_rows < max_rows:
            batch_limit = min(batch_size, max_rows - total_rows) if limit else batch_size
            url = build_query_url(start_date, end_date, borough, batch_limit, offset)

            batch_num += 1
            temp_file = output_path / f"temp_batch_{batch_num}.parquet"

            try:
                # Check if this batch has data
                df = con.execute(f"SELECT COUNT(*) as cnt FROM read_csv_auto('{url}')").fetchone()
                batch_rows = df[0]

                if batch_rows == 0:
                    print(f"  Batch {batch_num}: No more data")
                    break

                # Save batch
                con.execute(f"""
                    COPY (SELECT * FROM read_csv_auto('{url}'))
                    TO '{temp_file}' (FORMAT PARQUET)
                """)

                temp_files.append(temp_file)
                total_rows += batch_rows
                offset += batch_size

                print(f"  Batch {batch_num}: {batch_rows:,} rows (total: {total_rows:,})")

                if batch_rows < batch_size:
                    # Last batch
                    break

            except Exception as e:
                print(f"  Batch {batch_num} error: {e}")
                break

        # Combine all batches
        if temp_files:
            print(f"\nCombining {len(temp_files)} batches...")

            temp_pattern = str(output_path / "temp_batch_*.parquet")
            con.execute(f"""
                COPY (SELECT * FROM '{temp_pattern}')
                TO '{output_file}' (FORMAT PARQUET, COMPRESSION ZSTD)
            """)

            # Clean up temp files
            for tf in temp_files:
                os.remove(tf)

            row_count = total_rows
        else:
            print("No data downloaded")
            return {"status": "error", "error": "no_data"}

    # Get file size
    file_size = os.path.getsize(output_file)

    # Save metadata
    metadata = {
        "download_time": datetime.now().isoformat(),
        "source": f"data.ny.gov dataset {DATASET_ID}",
        "filters": {
            "start_date": start_date,
            "end_date": end_date,
            "borough": borough,
            "limit": limit
        },
        "row_count": row_count,
        "file_size_mb": file_size / 1024 / 1024,
        "output_file": str(output_file)
    }

    metadata_file = output_file.with_suffix('.json')
    with open(metadata_file, 'w') as f:
        json.dump(metadata, f, indent=2)

    print(f"\nDownload complete!")
    print(f"  Rows: {row_count:,}")
    print(f"  Size: {file_size / 1024 / 1024:.1f} MB")
    print(f"  File: {output_file}")

    con.close()

    return {
        "status": "success",
        "row_count": row_count,
        "file_size_mb": file_size / 1024 / 1024,
        "output_file": str(output_file)
    }


def main():
    parser = argparse.ArgumentParser(
        description="Download MTA Subway Hourly Ridership data"
    )
    parser.add_argument(
        "--output", "-o",
        default=DEFAULT_OUTPUT_DIR,
        help=f"Output directory (default: {DEFAULT_OUTPUT_DIR})"
    )
    parser.add_argument(
        "--start", "-s",
        help="Start date (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--end", "-e",
        help="End date (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--borough", "-b",
        choices=["Manhattan", "Brooklyn", "Queens", "Bronx", "Staten Island"],
        help="Filter by borough"
    )
    parser.add_argument(
        "--limit", "-l",
        type=int,
        help="Maximum rows to download"
    )
    parser.add_argument(
        "--force", "-f",
        action="store_true",
        help="Overwrite existing files"
    )

    args = parser.parse_args()

    result = fetch_ridership(
        args.output,
        start_date=args.start,
        end_date=args.end,
        borough=args.borough,
        limit=args.limit,
        force=args.force
    )

    if result["status"] == "success":
        print("\nRidership data ready for analysis!")
        print("\nExample query:")
        print(f"  import duckdb")
        print(f"  con = duckdb.connect()")
        print(f"  con.execute(\"SELECT * FROM '{result['output_file']}' LIMIT 10\").fetchdf()")
    elif result["status"] != "skipped":
        sys.exit(1)


if __name__ == "__main__":
    main()
