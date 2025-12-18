#!/usr/bin/env python3
"""
Download Citi Bike Jersey City trip data from S3.

Jersey City data is available from September 2015 to present, covering:
- Jersey City
- Hoboken (added later)

Files are prefixed with 'JC-' and follow similar naming conventions to NYC data.

Data source: https://s3.amazonaws.com/tripdata/
Documentation: https://citibikenyc.com/system-data

Known naming quirks handled by this script:
- JC-201708 citibike-tripdata.csv.zip (space instead of dash)
- JC-202207-citbike-tripdata.csv.zip (typo: 'citbike' not 'citibike')
- JC-202510-citibike-tripdata.zip (no .csv in name)

Usage:
    python src/download_jc.py --year 2024
    python src/download_jc.py --year 2023 --month 6
    python src/download_jc.py --all
"""

import argparse
import os
from pathlib import Path
from datetime import datetime
import requests
from tqdm import tqdm

BASE_URL = "https://s3.amazonaws.com/tripdata"
OUTPUT_DIR = Path(__file__).parent.parent / "data" / "jc" / "raw_zips"

# First available month of JC data
FIRST_YEAR = 2015
FIRST_MONTH = 9  # September 2015

# Known filename quirks (year-month -> actual filename)
FILENAME_OVERRIDES = {
    (2017, 8): "JC-201708 citibike-tripdata.csv.zip",  # Space instead of dash
    (2022, 7): "JC-202207-citbike-tripdata.csv.zip",   # Typo: citbike
    (2025, 10): "JC-202510-citibike-tripdata.zip",     # No .csv
}


def get_download_url(year: int, month: int) -> tuple[str, str]:
    """
    Generate the S3 URL for a given year/month.
    Returns (url, filename).
    """
    # Check for known quirky filenames
    if (year, month) in FILENAME_OVERRIDES:
        filename = FILENAME_OVERRIDES[(year, month)]
        url = f"{BASE_URL}/{filename.replace(' ', '%20')}"
        return url, filename

    ym = f"{year}{month:02d}"

    # Standard naming: JC-YYYYMM-citibike-tripdata.csv.zip
    filename = f"JC-{ym}-citibike-tripdata.csv.zip"
    url = f"{BASE_URL}/{filename}"

    return url, filename


def download_file(url: str, dest_path: Path, skip_existing: bool = True) -> bool:
    """
    Download a file with progress bar.
    Returns True if downloaded, False if skipped or failed.
    """
    if skip_existing and dest_path.exists():
        print(f"  Skipping (exists): {dest_path.name}")
        return False

    try:
        response = requests.get(url, stream=True, timeout=30)
        response.raise_for_status()

        total_size = int(response.headers.get('content-length', 0))

        with open(dest_path, 'wb') as f:
            with tqdm(total=total_size, unit='B', unit_scale=True, desc=dest_path.name) as pbar:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
                    pbar.update(len(chunk))

        return True

    except requests.HTTPError as e:
        if e.response.status_code == 404:
            return False
        raise


def download_month(year: int, month: int, output_dir: Path) -> bool:
    """Download a single month's data."""
    # Check if this month is before JC data started
    if year < FIRST_YEAR or (year == FIRST_YEAR and month < FIRST_MONTH):
        print(f"  ✗ No JC data before September 2015")
        return False

    url, filename = get_download_url(year, month)
    dest = output_dir / filename

    success = download_file(url, dest)

    if not success and not dest.exists():
        # Try alternate naming conventions
        alt_patterns = [
            f"JC-{year}{month:02d}-citibike-tripdata.csv.zip",
            f"JC-{year}{month:02d}-citibike-tripdata.zip",  # No .csv
            f"JC-{year}{month:02d}-citbike-tripdata.csv.zip",  # Typo
            f"JC-{year}{month:02d} citibike-tripdata.csv.zip",  # Space
        ]

        for alt_filename in alt_patterns:
            alt_url = f"{BASE_URL}/{alt_filename.replace(' ', '%20')}"
            alt_dest = output_dir / alt_filename
            if alt_dest.exists():
                return True
            try:
                if download_file(alt_url, alt_dest, skip_existing=False):
                    print(f"    (used alternate filename: {alt_filename})")
                    return True
            except requests.HTTPError:
                continue

        print(f"  ✗ Could not find data for {year}-{month:02d}")
        return False

    return True


def list_available_files() -> list[tuple[int, int, str]]:
    """
    Query S3 to list all available JC files.
    Returns list of (year, month, filename) tuples.
    """
    import re

    try:
        response = requests.get(f"{BASE_URL}/", timeout=30)
        response.raise_for_status()

        # Find all JC files
        pattern = r'JC-(\d{4})(\d{2})[-\s]citi?bike-tripdata\.(?:csv\.)?zip'
        matches = re.findall(pattern, response.text)

        files = []
        for year_str, month_str in matches:
            year = int(year_str)
            month = int(month_str)
            _, filename = get_download_url(year, month)
            files.append((year, month, filename))

        return sorted(set(files))

    except Exception as e:
        print(f"Error listing files: {e}")
        return []


def main():
    parser = argparse.ArgumentParser(
        description="Download Citi Bike Jersey City trip data"
    )
    parser.add_argument(
        "--year", type=int,
        help="Year to download (e.g., 2024)"
    )
    parser.add_argument(
        "--month", type=int,
        help="Month (1-12). If omitted with --year, downloads all months."
    )
    parser.add_argument(
        "--all", action="store_true",
        help="Download all available data (2015-present)"
    )
    parser.add_argument(
        "--list", action="store_true",
        help="List available files without downloading"
    )
    parser.add_argument(
        "--output-dir", type=Path, default=OUTPUT_DIR,
        help="Output directory"
    )

    args = parser.parse_args()

    if args.list:
        print("Querying S3 for available JC files...")
        files = list_available_files()
        print(f"\nFound {len(files)} files:")
        for year, month, filename in files:
            print(f"  {year}-{month:02d}: {filename}")
        return

    if not args.year and not args.all:
        parser.error("Either --year or --all is required")

    args.output_dir.mkdir(parents=True, exist_ok=True)

    if args.all:
        print("Downloading all Jersey City data (2015-present)")
        print(f"Output directory: {args.output_dir}")

        current = datetime.now()
        years_months = []

        for year in range(FIRST_YEAR, current.year + 1):
            start_month = FIRST_MONTH if year == FIRST_YEAR else 1
            end_month = current.month if year == current.year else 12

            for month in range(start_month, end_month + 1):
                years_months.append((year, month))

        success_count = 0
        for year, month in years_months:
            print(f"\n{year}-{month:02d}:")
            if download_month(year, month, args.output_dir):
                success_count += 1

        print(f"\n✓ Downloaded {success_count}/{len(years_months)} files")

    else:
        print(f"Downloading Jersey City data for {args.year}")
        print(f"Output directory: {args.output_dir}")

        if args.month:
            months = [args.month]
        else:
            current = datetime.now()
            if args.year == current.year:
                end_month = current.month
            else:
                end_month = 12

            if args.year == FIRST_YEAR:
                start_month = FIRST_MONTH
            else:
                start_month = 1

            months = range(start_month, end_month + 1)

        success_count = 0
        for month in months:
            print(f"\n{args.year}-{month:02d}:")
            if download_month(args.year, month, args.output_dir):
                success_count += 1

        print(f"\n✓ Downloaded {success_count}/{len(list(months))} files")


if __name__ == "__main__":
    main()
