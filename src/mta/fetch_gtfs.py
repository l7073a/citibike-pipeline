#!/usr/bin/env python3
"""
Fetch MTA Subway GTFS Static Feed

Downloads the official MTA GTFS feed containing:
- stops.txt: Station complexes, platforms, and entrances
- routes.txt: Subway lines (A, B, C, 1, 2, 3, etc.)
- trips.txt: Individual trips with route and direction
- stop_times.txt: Arrival/departure times at each stop
- shapes.txt: Line geometry for mapping
- transfers.txt: Transfer connections between stations

Data source: https://new.mta.info/developers
GTFS spec: https://gtfs.org/schedule/reference/

Usage:
    python src/mta/fetch_gtfs.py
    python src/mta/fetch_gtfs.py --output data/mta/gtfs
"""

import argparse
import os
import sys
import zipfile
from datetime import datetime
from pathlib import Path
import urllib.request
import json

# MTA GTFS feed URL (subway only)
# Note: MTA has separate feeds for subway, bus, LIRR, Metro-North
GTFS_URL = "http://web.mta.info/developers/data/nyct/subway/google_transit.zip"

# Alternative: Combined NYC transit feed (includes bus)
# GTFS_URL = "http://web.mta.info/developers/data/nyct/subway/google_transit.zip"

DEFAULT_OUTPUT_DIR = "data/mta/gtfs"


def download_gtfs(output_dir: str, force: bool = False) -> dict:
    """
    Download and extract MTA GTFS feed.

    Args:
        output_dir: Directory to extract GTFS files
        force: If True, re-download even if files exist

    Returns:
        dict with download stats
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    zip_path = output_path / "google_transit.zip"

    # Check if already downloaded
    stops_file = output_path / "stops.txt"
    if stops_file.exists() and not force:
        print(f"GTFS files already exist in {output_dir}")
        print("Use --force to re-download")
        return {"status": "skipped", "reason": "files_exist"}

    print(f"Downloading MTA GTFS feed from {GTFS_URL}...")

    try:
        # Download the zip file
        urllib.request.urlretrieve(GTFS_URL, zip_path)
        file_size = os.path.getsize(zip_path)
        print(f"Downloaded {file_size / 1024 / 1024:.1f} MB")

    except Exception as e:
        print(f"Error downloading GTFS: {e}")
        return {"status": "error", "error": str(e)}

    # Extract the zip
    print(f"Extracting to {output_dir}...")
    extracted_files = []

    with zipfile.ZipFile(zip_path, 'r') as zf:
        for name in zf.namelist():
            zf.extract(name, output_path)
            extracted_files.append(name)
            file_path = output_path / name
            size = os.path.getsize(file_path)
            print(f"  {name}: {size / 1024:.1f} KB")

    # Remove the zip file
    os.remove(zip_path)

    # Save metadata
    metadata = {
        "download_time": datetime.now().isoformat(),
        "source_url": GTFS_URL,
        "files": extracted_files,
        "output_dir": str(output_path)
    }

    metadata_path = output_path / "metadata.json"
    with open(metadata_path, 'w') as f:
        json.dump(metadata, f, indent=2)

    print(f"\nExtracted {len(extracted_files)} files")
    print(f"Metadata saved to {metadata_path}")

    return {
        "status": "success",
        "files": extracted_files,
        "download_time": metadata["download_time"]
    }


def main():
    parser = argparse.ArgumentParser(
        description="Download MTA Subway GTFS feed"
    )
    parser.add_argument(
        "--output", "-o",
        default=DEFAULT_OUTPUT_DIR,
        help=f"Output directory (default: {DEFAULT_OUTPUT_DIR})"
    )
    parser.add_argument(
        "--force", "-f",
        action="store_true",
        help="Force re-download even if files exist"
    )

    args = parser.parse_args()

    result = download_gtfs(args.output, args.force)

    if result["status"] == "success":
        print("\nGTFS download complete!")
        print("\nNext steps:")
        print("  1. Run: python src/mta/build_reference.py")
        print("     This creates stations.parquet, entrances.parquet, etc.")
    elif result["status"] == "skipped":
        print("\nTo force re-download: python src/mta/fetch_gtfs.py --force")
    else:
        sys.exit(1)


if __name__ == "__main__":
    main()
