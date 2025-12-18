#!/usr/bin/env python3
"""
Fetch historical weather data from Open-Meteo for NYC.

Creates two parquet files:
- hourly_weather.parquet: Hourly weather conditions (temp, precip, wind, etc.)
- daily_weather.parquet: Daily sunrise/sunset times

Data source: Open-Meteo Archive API (free, no API key required)
Location: Central Park, NYC (40.7829, -73.9654)
"""

import argparse
import requests
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
import time

# NYC Central Park coordinates
NYC_LAT = 40.7829
NYC_LON = -73.9654
TIMEZONE = "America/New_York"

# Output directory
OUTPUT_DIR = Path(__file__).parent.parent / "data" / "weather"


def fetch_hourly_weather(start_date: str, end_date: str) -> pd.DataFrame:
    """Fetch hourly weather data from Open-Meteo Archive API."""

    url = "https://archive-api.open-meteo.com/v1/archive"

    params = {
        "latitude": NYC_LAT,
        "longitude": NYC_LON,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": ",".join([
            "temperature_2m",
            "apparent_temperature",
            "precipitation",
            "rain",
            "snowfall",
            "wind_speed_10m",
            "relative_humidity_2m",
            "weather_code"
        ]),
        "timezone": TIMEZONE
    }

    response = requests.get(url, params=params, timeout=120)
    response.raise_for_status()
    data = response.json()

    # Convert to DataFrame
    hourly = data["hourly"]
    df = pd.DataFrame({
        "datetime": pd.to_datetime(hourly["time"]),
        "temperature_2m": hourly["temperature_2m"],
        "apparent_temperature": hourly["apparent_temperature"],
        "precipitation": hourly["precipitation"],
        "rain": hourly["rain"],
        "snowfall": hourly["snowfall"],
        "wind_speed_10m": hourly["wind_speed_10m"],
        "relative_humidity_2m": hourly["relative_humidity_2m"],
        "weather_code": hourly["weather_code"]
    })

    return df


def fetch_daily_weather(start_date: str, end_date: str) -> pd.DataFrame:
    """Fetch daily sunrise/sunset data from Open-Meteo Archive API."""

    url = "https://archive-api.open-meteo.com/v1/archive"

    params = {
        "latitude": NYC_LAT,
        "longitude": NYC_LON,
        "start_date": start_date,
        "end_date": end_date,
        "daily": "sunrise,sunset",
        "timezone": TIMEZONE
    }

    response = requests.get(url, params=params, timeout=120)
    response.raise_for_status()
    data = response.json()

    # Convert to DataFrame
    daily = data["daily"]
    df = pd.DataFrame({
        "date": pd.to_datetime(daily["time"]).date,
        "sunrise": pd.to_datetime(daily["sunrise"]),
        "sunset": pd.to_datetime(daily["sunset"])
    })

    # Calculate daylight hours
    df["daylight_hours"] = (df["sunset"] - df["sunrise"]).dt.total_seconds() / 3600

    # Convert date column to proper date type
    df["date"] = pd.to_datetime(df["date"])

    return df


def main():
    parser = argparse.ArgumentParser(description="Fetch NYC weather data from Open-Meteo")
    parser.add_argument("--start-year", type=int, default=2013,
                        help="Start year (default: 2013)")
    parser.add_argument("--end-year", type=int, default=2025,
                        help="End year (default: 2025)")
    parser.add_argument("--force", action="store_true",
                        help="Overwrite existing files")
    args = parser.parse_args()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    hourly_file = OUTPUT_DIR / "hourly_weather.parquet"
    daily_file = OUTPUT_DIR / "daily_weather.parquet"

    # Check if files exist
    if not args.force and hourly_file.exists() and daily_file.exists():
        print(f"Weather files already exist. Use --force to overwrite.")
        print(f"  {hourly_file}")
        print(f"  {daily_file}")
        return

    # Determine date range
    start_date = f"{args.start_year}-01-01"

    # For end date, use today if we're in the end year, otherwise Dec 31
    today = datetime.now()
    if args.end_year >= today.year:
        # Open-Meteo archive has ~5 day delay, so use a few days ago
        end = today - timedelta(days=5)
        end_date = end.strftime("%Y-%m-%d")
    else:
        end_date = f"{args.end_year}-12-31"

    print(f"Fetching weather data for NYC ({NYC_LAT}, {NYC_LON})")
    print(f"Date range: {start_date} to {end_date}")
    print(f"Timezone: {TIMEZONE}")
    print()

    # Fetch hourly data (need to do in chunks due to API limits)
    # Open-Meteo allows ~10 years per request for hourly data
    print("Fetching hourly weather data...")

    all_hourly = []
    current_start = datetime.strptime(start_date, "%Y-%m-%d")
    final_end = datetime.strptime(end_date, "%Y-%m-%d")

    while current_start < final_end:
        # Fetch 3 years at a time to be safe
        chunk_end = min(current_start + timedelta(days=3*365), final_end)

        chunk_start_str = current_start.strftime("%Y-%m-%d")
        chunk_end_str = chunk_end.strftime("%Y-%m-%d")

        print(f"  Fetching {chunk_start_str} to {chunk_end_str}...")

        try:
            df = fetch_hourly_weather(chunk_start_str, chunk_end_str)
            all_hourly.append(df)
            print(f"    Got {len(df):,} hourly records")
        except Exception as e:
            print(f"    ERROR: {e}")
            raise

        current_start = chunk_end + timedelta(days=1)
        time.sleep(1)  # Be nice to the API

    hourly_df = pd.concat(all_hourly, ignore_index=True)
    hourly_df = hourly_df.drop_duplicates(subset=["datetime"]).sort_values("datetime")

    print(f"\nTotal hourly records: {len(hourly_df):,}")
    print(f"Date range: {hourly_df['datetime'].min()} to {hourly_df['datetime'].max()}")

    # Fetch daily data
    print("\nFetching daily sunrise/sunset data...")

    all_daily = []
    current_start = datetime.strptime(start_date, "%Y-%m-%d")

    while current_start < final_end:
        chunk_end = min(current_start + timedelta(days=5*365), final_end)

        chunk_start_str = current_start.strftime("%Y-%m-%d")
        chunk_end_str = chunk_end.strftime("%Y-%m-%d")

        print(f"  Fetching {chunk_start_str} to {chunk_end_str}...")

        try:
            df = fetch_daily_weather(chunk_start_str, chunk_end_str)
            all_daily.append(df)
            print(f"    Got {len(df):,} daily records")
        except Exception as e:
            print(f"    ERROR: {e}")
            raise

        current_start = chunk_end + timedelta(days=1)
        time.sleep(1)

    daily_df = pd.concat(all_daily, ignore_index=True)
    daily_df = daily_df.drop_duplicates(subset=["date"]).sort_values("date")

    print(f"\nTotal daily records: {len(daily_df):,}")
    print(f"Date range: {daily_df['date'].min()} to {daily_df['date'].max()}")

    # Save to parquet
    print(f"\nSaving hourly weather to {hourly_file}...")
    hourly_df.to_parquet(hourly_file, index=False)
    print(f"  Size: {hourly_file.stat().st_size / 1024 / 1024:.2f} MB")

    print(f"\nSaving daily weather to {daily_file}...")
    daily_df.to_parquet(daily_file, index=False)
    print(f"  Size: {daily_file.stat().st_size / 1024:.2f} KB")

    # Print sample data
    print("\n" + "="*60)
    print("Sample hourly data:")
    print(hourly_df.head(10).to_string(index=False))

    print("\n" + "="*60)
    print("Sample daily data:")
    print(daily_df.head(10).to_string(index=False))

    print("\nDone!")


if __name__ == "__main__":
    main()
