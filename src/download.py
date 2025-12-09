#!/usr/bin/env python3
"""
Download Citi Bike trip data from S3.

Handles the various naming conventions used over the years:
- 201401-citibike-tripdata.csv.zip (monthly, 2013-2017ish)
- 201801-citibike-tripdata.csv.zip (monthly, 2018-2023)
- 202401-citibike-tripdata.zip (monthly, 2024+, note: no .csv in name)
"""

import argparse
import os
from pathlib import Path
from datetime import datetime
import requests
from tqdm import tqdm

BASE_URL = "https://s3.amazonaws.com/tripdata"
OUTPUT_DIR = Path(__file__).parent.parent / "data" / "raw_zips"


def get_download_url(year: int, month: int) -> tuple[str, str]:
    """
    Generate the S3 URL for a given year/month.
    Returns (url, filename).
    """
    ym = f"{year}{month:02d}"
    
    # Try different naming conventions
    # 2024+ often uses .zip instead of .csv.zip
    if year >= 2024:
        filename = f"{ym}-citibike-tripdata.zip"
    else:
        filename = f"{ym}-citibike-tripdata.csv.zip"
    
    url = f"{BASE_URL}/{filename}"
    return url, filename


def download_file(url: str, dest_path: Path, skip_existing: bool = True) -> bool:
    """
    Download a file with progress bar.
    Returns True if downloaded, False if skipped.
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
            # Try alternate naming convention
            return False
        raise


def download_month(year: int, month: int, output_dir: Path) -> bool:
    """Download a single month's data."""
    url, filename = get_download_url(year, month)
    dest = output_dir / filename
    
    success = download_file(url, dest)
    
    if not success and not dest.exists():
        # Try alternate naming (some files use different conventions)
        alt_patterns = [
            f"{year}{month:02d}-citibike-tripdata.csv.zip",
            f"{year}{month:02d}-citibike-tripdata.zip",
            f"{year}-citibike-tripdata.zip",  # Full year bundles (older data)
        ]
        
        for alt_filename in alt_patterns:
            alt_url = f"{BASE_URL}/{alt_filename}"
            alt_dest = output_dir / alt_filename
            if alt_dest.exists():
                return True
            try:
                if download_file(alt_url, alt_dest, skip_existing=False):
                    return True
            except requests.HTTPError:
                continue
        
        print(f"  ✗ Could not find data for {year}-{month:02d}")
        return False
    
    return True


def main():
    parser = argparse.ArgumentParser(description="Download Citi Bike trip data")
    parser.add_argument("--year", type=int, required=True, help="Year to download (e.g., 2014)")
    parser.add_argument("--month", type=int, help="Month (1-12). If omitted, downloads all months.")
    parser.add_argument("--output-dir", type=Path, default=OUTPUT_DIR, help="Output directory")
    
    args = parser.parse_args()
    
    args.output_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"Downloading Citi Bike data for {args.year}")
    print(f"Output directory: {args.output_dir}")
    
    if args.month:
        months = [args.month]
    else:
        # Download all months, but cap at current month for current year
        current = datetime.now()
        if args.year == current.year:
            months = range(1, current.month + 1)
        else:
            months = range(1, 13)
    
    success_count = 0
    for month in months:
        print(f"\n{args.year}-{month:02d}:")
        if download_month(args.year, month, args.output_dir):
            success_count += 1
    
    print(f"\n✓ Downloaded {success_count}/{len(list(months))} files")


if __name__ == "__main__":
    main()
