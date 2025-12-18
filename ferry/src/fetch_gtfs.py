#!/usr/bin/env python3
"""
Download ferry GTFS feeds (General Transit Feed Specification).

Data sources:
1. NYC Ferry: http://nycferry.connexionz.net/rtt/public/utility/gtfs.aspx
2. Staten Island Ferry: https://data.cityofnewyork.us/Transportation/Staten-Island-Ferry-Schedule-General-Transit-Feed-/b57i-ri22

GTFS files include:
- routes.txt: Ferry routes
- stops.txt: Ferry landings/terminals
- trips.txt: Scheduled trips
- stop_times.txt: Arrival times at each stop
- calendar.txt: Service schedules
"""

import argparse
import json
import sys
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Dict

import requests


# GTFS feed URLs
GTFS_FEEDS = {
    "nyc_ferry": {
        "url": "http://nycferry.connexionz.net/rtt/public/utility/gtfs.aspx",
        "name": "NYC Ferry",
        "format": "zip"
    },
    "staten_island": {
        "url": "https://data.cityofnewyork.us/api/views/b57i-ri22/files/c76c5b93-c4e8-43e0-bf9e-db62e59dfa3c?download=true&filename=google_transit.zip",
        "name": "Staten Island Ferry",
        "format": "zip"
    }
}


def download_gtfs(feed_key: str, output_dir: Path) -> Dict:
    """
    Download and extract a GTFS feed.

    Args:
        feed_key: Key for the GTFS feed (e.g., "nyc_ferry", "staten_island")
        output_dir: Directory to save extracted files

    Returns:
        Dictionary with download metadata
    """

    feed_info = GTFS_FEEDS[feed_key]
    print(f"Downloading {feed_info['name']} GTFS feed...")
    print(f"  URL: {feed_info['url']}")

    # Download zip file
    response = requests.get(feed_info['url'], stream=True)
    response.raise_for_status()

    # Create output directory
    feed_dir = output_dir / feed_key
    feed_dir.mkdir(parents=True, exist_ok=True)

    # Save zip file temporarily
    zip_path = feed_dir / "gtfs.zip"
    with open(zip_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)

    print(f"  Downloaded {zip_path.stat().st_size:,} bytes")

    # Extract zip file
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(feed_dir)

    # List extracted files
    extracted_files = [f.name for f in feed_dir.iterdir() if f.is_file() and f.name != "gtfs.zip"]
    print(f"  Extracted {len(extracted_files)} files:")
    for filename in sorted(extracted_files):
        file_path = feed_dir / filename
        print(f"    {filename} ({file_path.stat().st_size:,} bytes)")

    # Remove zip file
    zip_path.unlink()

    # Create metadata
    metadata = {
        "feed": feed_info['name'],
        "feed_key": feed_key,
        "source_url": feed_info['url'],
        "downloaded_at": datetime.now().isoformat(),
        "files": extracted_files,
        "output_dir": str(feed_dir)
    }

    # Save metadata
    metadata_file = feed_dir / "metadata.json"
    with open(metadata_file, 'w') as f:
        json.dump(metadata, f, indent=2)

    print(f"  Saved metadata to {metadata_file}")

    return metadata


def main():
    parser = argparse.ArgumentParser(
        description="Download ferry GTFS feeds",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Available feeds:
  nyc_ferry        - NYC Ferry (Astoria, Rockaway, etc.)
  staten_island    - Staten Island Ferry

Examples:
  # Download all feeds
  python fetch_gtfs.py

  # Download specific feed
  python fetch_gtfs.py --system nyc_ferry

  # Download with custom output directory
  python fetch_gtfs.py --output ../data/gtfs
        """
    )

    parser.add_argument(
        '--system',
        type=str,
        choices=list(GTFS_FEEDS.keys()),
        help='Specific ferry system to download (default: all)'
    )

    parser.add_argument(
        '--output',
        type=Path,
        default=Path(__file__).parent.parent / "data" / "gtfs",
        help='Output directory (default: ../data/gtfs)'
    )

    args = parser.parse_args()

    try:
        # Determine which feeds to download
        if args.system:
            feeds_to_download = [args.system]
        else:
            feeds_to_download = list(GTFS_FEEDS.keys())

        # Download each feed
        all_metadata = {}
        for feed_key in feeds_to_download:
            print(f"\n{'='*60}")
            metadata = download_gtfs(feed_key, args.output)
            all_metadata[feed_key] = metadata

        # Print summary
        print(f"\n{'='*60}")
        print("Download complete!")
        print(f"\nSummary:")
        for feed_key, metadata in all_metadata.items():
            print(f"  {metadata['feed']}:")
            print(f"    Files: {len(metadata['files'])}")
            print(f"    Location: {metadata['output_dir']}")

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
