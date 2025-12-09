#!/usr/bin/env python3
"""
Identify and remove redundant/duplicate CSV files from raw_csvs.

The 2013 data has duplicate files:
- Original complete files (quoted headers): 201306-citibike-tripdata.csv
- Split files in subdirs (unquoted headers): 201306-citibike-tripdata_1.csv, _2.csv

We keep ONLY the original complete files for 2013 and remove the split versions.
"""

import argparse
import json
import os
import re
from datetime import datetime
from pathlib import Path

DATA_DIR = Path(__file__).parent.parent / "data" / "raw_csvs"
LOGS_DIR = Path(__file__).parent.parent / "logs"


def identify_2013_duplicates(data_dir: Path) -> dict:
    """
    Identify redundant 2013 files.
    Returns dict of files to remove and why.
    """
    files_2013 = sorted(data_dir.glob("2013*.csv"))

    # Group by month
    by_month = {}
    for f in files_2013:
        match = re.search(r'2013(\d{2})', f.name)
        if match:
            month = match.group(1)
            if month not in by_month:
                by_month[month] = {'original': None, 'splits': []}

            # Original files don't have _1 or _2 suffix before .csv
            if re.search(r'_\d\.csv$', f.name):
                by_month[month]['splits'].append(f)
            else:
                by_month[month]['original'] = f

    to_remove = {}
    for month, files in by_month.items():
        if files['original'] and files['splits']:
            for split in files['splits']:
                to_remove[split] = {
                    'reason': 'duplicate_split_file',
                    'original': files['original'].name,
                    'month': f'2013-{month}',
                }

    return to_remove


def main():
    parser = argparse.ArgumentParser(description="Clean up duplicate CSV files")
    parser.add_argument("--data-dir", type=Path, default=DATA_DIR)
    parser.add_argument("--dry-run", action="store_true", help="Show what would be removed without removing")

    args = parser.parse_args()
    LOGS_DIR.mkdir(parents=True, exist_ok=True)

    print("Scanning for duplicate/redundant files...")

    to_remove = identify_2013_duplicates(args.data_dir)

    if not to_remove:
        print("No duplicate files found.")
        return

    print(f"\nFound {len(to_remove)} redundant files:")
    for f, info in sorted(to_remove.items(), key=lambda x: x[0].name):
        print(f"  {f.name}")
        print(f"    Reason: {info['reason']}")
        print(f"    Original: {info['original']}")

    # Calculate space savings
    total_bytes = sum(f.stat().st_size for f in to_remove.keys())
    print(f"\nTotal space to reclaim: {total_bytes / 1024 / 1024:.1f} MB")

    # Log the action
    log_data = {
        'timestamp': datetime.now().isoformat(),
        'action': 'cleanup_duplicates',
        'dry_run': args.dry_run,
        'files_removed': [
            {
                'path': str(f),
                'size_bytes': f.stat().st_size,
                **info
            }
            for f, info in to_remove.items()
        ],
        'total_bytes_removed': total_bytes,
    }

    if args.dry_run:
        print("\n[DRY RUN] No files removed. Run without --dry-run to remove.")
    else:
        print("\nRemoving files...")
        for f in to_remove.keys():
            f.unlink()
            print(f"  Removed: {f.name}")
        print(f"\n✓ Removed {len(to_remove)} files")

    # Save log
    log_path = LOGS_DIR / f"cleanup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(log_path, 'w') as f:
        json.dump(log_data, f, indent=2)
    print(f"✓ Log saved to {log_path}")


if __name__ == "__main__":
    main()
