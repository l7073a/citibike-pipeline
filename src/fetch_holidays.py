#!/usr/bin/env python3
"""
Generate US Federal holiday data for trip analysis.

Creates holidays.parquet with all US federal holidays from 2013-2025.
Uses the `holidays` Python library.

Install: pip install holidays
"""

import argparse
import pandas as pd
from pathlib import Path
from datetime import date

try:
    import holidays
except ImportError:
    print("Error: 'holidays' package not installed.")
    print("Install with: pip install holidays")
    exit(1)

# Output directory
OUTPUT_DIR = Path(__file__).parent.parent / "data" / "weather"


def generate_holidays(start_year: int, end_year: int) -> pd.DataFrame:
    """Generate US federal holidays for the given year range."""

    # US federal holidays
    us_holidays = holidays.US(years=range(start_year, end_year + 1))

    records = []
    for dt, name in sorted(us_holidays.items()):
        # Determine if this is an observed holiday (shifted from weekend)
        # The holidays library includes observed dates with "(observed)" suffix
        if "(observed)" in name.lower():
            holiday_type = "observed"
            # Clean up the name
            clean_name = name.replace(" (Observed)", "").replace(" (observed)", "")
        else:
            holiday_type = "federal"
            clean_name = name

        records.append({
            "date": dt,
            "holiday_name": clean_name,
            "holiday_type": holiday_type
        })

    df = pd.DataFrame(records)
    df["date"] = pd.to_datetime(df["date"])

    return df


def main():
    parser = argparse.ArgumentParser(description="Generate US Federal holiday data")
    parser.add_argument("--start-year", type=int, default=2013,
                        help="Start year (default: 2013)")
    parser.add_argument("--end-year", type=int, default=2025,
                        help="End year (default: 2025)")
    parser.add_argument("--force", action="store_true",
                        help="Overwrite existing file")
    args = parser.parse_args()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    output_file = OUTPUT_DIR / "holidays.parquet"

    # Check if file exists
    if not args.force and output_file.exists():
        print(f"Holiday file already exists. Use --force to overwrite.")
        print(f"  {output_file}")
        return

    print(f"Generating US Federal holidays for {args.start_year}-{args.end_year}")

    df = generate_holidays(args.start_year, args.end_year)

    print(f"\nTotal holidays: {len(df)}")
    print(f"Date range: {df['date'].min().date()} to {df['date'].max().date()}")

    # Count by type
    print(f"\nBy type:")
    for holiday_type, count in df["holiday_type"].value_counts().items():
        print(f"  {holiday_type}: {count}")

    # Count unique holiday names
    print(f"\nUnique holidays: {df['holiday_name'].nunique()}")
    for name in sorted(df["holiday_name"].unique()):
        count = len(df[df["holiday_name"] == name])
        print(f"  {name}: {count} occurrences")

    # Save to parquet
    print(f"\nSaving to {output_file}...")
    df.to_parquet(output_file, index=False)
    print(f"  Size: {output_file.stat().st_size / 1024:.2f} KB")

    # Print sample
    print("\n" + "="*60)
    print("Sample data (2024):")
    sample = df[df["date"].dt.year == 2024]
    print(sample.to_string(index=False))

    print("\nDone!")


if __name__ == "__main__":
    main()
