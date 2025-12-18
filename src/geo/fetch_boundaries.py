#!/usr/bin/env python3
"""
Download NYC geographic boundary shapefiles.

Data sources:
- NYC Boroughs: NYC Open Data
- NTA (Neighborhood Tabulation Areas): NYC Open Data
- PUMA (Public Use Microdata Areas): US Census TIGER/Line
- Census Tracts: US Census TIGER/Line

All boundaries are in WGS84 (EPSG:4326) coordinate system.
"""

import argparse
import json
import zipfile
from datetime import datetime
from pathlib import Path
from urllib.request import urlretrieve

import geopandas as gpd

# Output directories
RAW_DIR = Path("data/geo/raw")
PROCESSED_DIR = Path("data/geo/processed")

# NYC bounding box for validation (WGS84)
NYC_BBOX = {
    "min_lat": 40.4774,
    "max_lat": 40.9176,
    "min_lon": -74.2591,
    "max_lon": -73.7004,
}


def download_borough_boundaries():
    """
    Download NYC borough boundaries from NYC Open Data.

    Source: NYC Department of City Planning
    URL: https://data.cityofnewyork.us/City-Government/Borough-Boundaries/tqmj-j8zm
    """
    print("\n=== Downloading Borough Boundaries ===")

    # Use NYC Planning's direct GeoJSON endpoint (more reliable)
    url = "https://services5.arcgis.com/GfwWNkhOj9bNBqoJ/arcgis/rest/services/NYC_Borough_Boundary/FeatureServer/0/query?where=1=1&outFields=*&outSR=4326&f=geojson"
    output_file = PROCESSED_DIR / "boroughs.geojson"

    print(f"Downloading from: NYC Planning ArcGIS Server")
    gdf = gpd.read_file(url)

    # Standardize column names (try different possible column names)
    rename_map = {
        'boro_name': 'borough_name',
        'boro_code': 'borough_code',
        'BoroName': 'borough_name',
        'BoroCode': 'borough_code'
    }
    # Only rename columns that exist
    gdf = gdf.rename(columns={k: v for k, v in rename_map.items() if k in gdf.columns})

    # Keep only essential columns if they exist
    keep_cols = ['borough_name', 'borough_code', 'geometry']
    existing_cols = [col for col in keep_cols if col in gdf.columns]
    existing_cols.append('geometry')  # Always keep geometry
    gdf = gdf[[col for col in gdf.columns if col in existing_cols]]

    # Save as GeoJSON
    gdf.to_file(output_file, driver="GeoJSON")
    print(f"Saved {len(gdf)} boroughs to: {output_file}")

    # Print borough names if column exists
    if 'borough_name' in gdf.columns:
        print(f"Boroughs: {', '.join(sorted(gdf['borough_name'].tolist()))}")
    else:
        print(f"Columns available: {list(gdf.columns)}")

    return gdf


def download_nta_boundaries():
    """
    Download NTA (Neighborhood Tabulation Area) boundaries from NYC Open Data.

    Source: NYC Department of City Planning
    URL: https://data.cityofnewyork.us/City-Government/NTA-map/d3qk-pfyz

    Note: Uses 2020 NTA boundaries. Previous versions (2010) available separately.
    """
    print("\n=== Downloading NTA Boundaries ===")

    # Use NYC Planning's direct GeoJSON endpoint for 2020 NTAs
    url = "https://services5.arcgis.com/GfwWNkhOj9bNBqoJ/arcgis/rest/services/NYC_2020_NTA/FeatureServer/0/query?where=1=1&outFields=*&outSR=4326&f=geojson"
    output_file = PROCESSED_DIR / "nta.geojson"

    print(f"Downloading from: NYC Planning ArcGIS Server")
    print("Note: This is the 2020 NTA definition (NTA2020)")
    print(f"URL: {url}")

    gdf = gpd.read_file(url)

    print(f"Original NTA columns: {list(gdf.columns)}")

    # Standardize column names (try different possible column names)
    rename_map = {
        'nta2020': 'nta_code',
        'ntaname': 'nta_name',
        'boroname': 'borough_name',
        'borocode': 'borough_code',
        'NTA2020': 'nta_code',
        'NTAName': 'nta_name',
        'BoroName': 'borough_name',
        'BoroCode': 'borough_code',
        'NTACode': 'nta_code',
        'BoroCT2020': 'borough_ct_code'
    }
    # Only rename columns that exist
    gdf = gdf.rename(columns={k: v for k, v in rename_map.items() if k in gdf.columns})

    # Keep ALL useful columns (don't filter too aggressively)
    # Only remove geometry-related metadata
    drop_cols = ['Shape__Area', 'Shape__Length', 'OBJECTID']
    gdf = gdf.drop(columns=[col for col in drop_cols if col in gdf.columns])

    print(f"After processing: {list(gdf.columns)}")

    # Save as GeoJSON
    gdf.to_file(output_file, driver="GeoJSON")
    print(f"Saved {len(gdf)} NTAs to: {output_file}")

    # Print NTA summary if columns exist
    if 'borough_name' in gdf.columns:
        print(f"NTAs by borough:")
        for boro in sorted(gdf['borough_name'].unique()):
            count = len(gdf[gdf['borough_name'] == boro])
            print(f"  {boro}: {count} NTAs")
    else:
        print(f"Columns available: {list(gdf.columns)}")

    return gdf


def download_puma_boundaries(year=2022):
    """
    Download PUMA (Public Use Microdata Area) boundaries from US Census TIGER/Line.

    Source: US Census Bureau
    URL: https://www2.census.gov/geo/tiger/TIGER{YEAR}/PUMA/

    Args:
        year: Census year (2010, 2020, 2022, etc.)
    """
    print(f"\n=== Downloading PUMA Boundaries ({year}) ===")

    # TIGER/Line PUMA shapefile URL
    base_url = f"https://www2.census.gov/geo/tiger/TIGER{year}/PUMA/"
    filename = f"tl_{year}_36_puma20.zip"  # 36 = NY state FIPS code
    url = base_url + filename

    raw_zip = RAW_DIR / filename
    output_file = PROCESSED_DIR / "puma.geojson"

    print(f"Downloading from: {url}")
    urlretrieve(url, raw_zip)
    print(f"Downloaded to: {raw_zip}")

    # Extract and read shapefile
    with zipfile.ZipFile(raw_zip, 'r') as zip_ref:
        zip_ref.extractall(RAW_DIR / f"puma_{year}")

    # Find the .shp file
    shp_file = list((RAW_DIR / f"puma_{year}").glob("*.shp"))[0]
    gdf = gpd.read_file(shp_file)

    # Filter to NYC PUMAs (based on bounding box)
    gdf_nyc = gdf.cx[NYC_BBOX['min_lon']:NYC_BBOX['max_lon'],
                      NYC_BBOX['min_lat']:NYC_BBOX['max_lat']]

    # Standardize column names
    gdf_nyc = gdf_nyc.rename(columns={
        'PUMACE20': 'puma_code',
        'NAMELSAD20': 'puma_name',
        'GEOID20': 'puma_geoid'
    })

    # Keep only essential columns
    keep_cols = ['puma_code', 'puma_name', 'puma_geoid', 'geometry']
    gdf_nyc = gdf_nyc[[col for col in keep_cols if col in gdf_nyc.columns]]

    # Save as GeoJSON
    gdf_nyc.to_file(output_file, driver="GeoJSON")
    print(f"Saved {len(gdf_nyc)} NYC PUMAs to: {output_file}")

    return gdf_nyc


def download_census_tracts(year=2020):
    """
    Download Census Tract boundaries from NYC Planning ArcGIS.

    Source: NYC Department of City Planning
    URL: https://hub.arcgis.com/datasets/DCP::nyc-census-tracts-for-2020-us-census/

    Note: Uses 2020 Census tracts (clipped to NYC shoreline).
    The year parameter is kept for API compatibility but only 2020 data is available.
    """
    print(f"\n=== Downloading Census Tract Boundaries (2020) ===")

    # Use NYC Planning's direct GeoJSON endpoint for 2020 Census Tracts
    url = "https://services5.arcgis.com/GfwWNkhOj9bNBqoJ/arcgis/rest/services/NYC_Census_Tracts_for_2020_US_Census/FeatureServer/0/query?where=1=1&outFields=*&outSR=4326&f=geojson"
    output_file = PROCESSED_DIR / "census_tracts.geojson"

    print(f"Downloading from: NYC Planning ArcGIS Server")
    print("Note: This is the 2020 Census definition (clipped to shoreline)")
    print(f"URL: {url}")

    gdf = gpd.read_file(url)

    print(f"Original columns: {list(gdf.columns)}")

    # Standardize column names (try different possible column names)
    rename_map = {
        'BoroCT2020': 'tract_geoid',
        'CT2020': 'tract_code',
        'BoroName': 'borough_name',
        'BoroCode': 'borough_code',
        'GEOID': 'tract_geoid',
        'boro_ct202': 'tract_geoid',
        'ct2020': 'tract_code',
        'boroname': 'borough_name',
        'borocode': 'borough_code'
    }
    # Only rename columns that exist (and avoid creating duplicates)
    cols_to_rename = {k: v for k, v in rename_map.items() if k in gdf.columns}
    gdf = gdf.rename(columns=cols_to_rename)

    print(f"After rename: {list(gdf.columns)}")

    # Check for duplicates
    if len(gdf.columns) != len(set(gdf.columns)):
        print("WARNING: Duplicate columns detected!")
        print(f"Duplicates: {[col for col in gdf.columns if list(gdf.columns).count(col) > 1]}")

    # Remove duplicate columns if any
    gdf = gdf.loc[:, ~gdf.columns.duplicated()]

    # Keep only essential columns if they exist
    keep_cols = ['tract_code', 'tract_geoid', 'borough_name', 'borough_code', 'geometry']
    existing_cols = [col for col in keep_cols if col in gdf.columns]
    if 'geometry' not in existing_cols:
        existing_cols.append('geometry')  # Always keep geometry
    gdf = gdf[existing_cols]

    # Save as GeoJSON
    gdf.to_file(output_file, driver="GeoJSON")
    print(f"Saved {len(gdf)} census tracts to: {output_file}")

    # Print tract summary if columns exist
    if 'borough_name' in gdf.columns:
        print(f"Tracts by borough:")
        for boro in sorted(gdf['borough_name'].unique()):
            count = len(gdf[gdf['borough_name'] == boro])
            print(f"  {boro}: {count} tracts")
    else:
        print(f"Columns available: {list(gdf.columns)}")

    return gdf


def save_metadata(datasets):
    """Save metadata about the downloaded datasets."""
    metadata = {
        "download_timestamp": datetime.now().isoformat(),
        "datasets": {},
        "coordinate_system": "WGS84 (EPSG:4326)",
        "nyc_bounding_box": NYC_BBOX
    }

    for name, gdf in datasets.items():
        if gdf is not None:
            metadata["datasets"][name] = {
                "feature_count": len(gdf),
                "columns": list(gdf.columns),
                "bbox": gdf.total_bounds.tolist(),
            }

    metadata_file = PROCESSED_DIR / "metadata.json"
    with open(metadata_file, 'w') as f:
        json.dump(metadata, f, indent=2)

    print(f"\n=== Metadata saved to: {metadata_file} ===")


def main():
    parser = argparse.ArgumentParser(
        description="Download NYC geographic boundary shapefiles",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Download all boundaries (recommended)
  python fetch_boundaries.py --all

  # Download specific boundaries
  python fetch_boundaries.py --boroughs --nta

  # Download Census data for a specific year
  python fetch_boundaries.py --census-tracts --puma --year 2020
        """
    )

    parser.add_argument('--all', action='store_true',
                        help='Download all boundary types')
    parser.add_argument('--boroughs', action='store_true',
                        help='Download borough boundaries')
    parser.add_argument('--nta', action='store_true',
                        help='Download NTA boundaries')
    parser.add_argument('--puma', action='store_true',
                        help='Download PUMA boundaries')
    parser.add_argument('--census-tracts', action='store_true',
                        help='Download census tract boundaries')
    parser.add_argument('--year', type=int, default=2022,
                        help='Year for Census data (default: 2022)')

    args = parser.parse_args()

    # Create directories
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    # Track what was downloaded
    datasets = {}

    # Download requested datasets
    if args.all or args.boroughs:
        datasets['boroughs'] = download_borough_boundaries()

    if args.all or args.nta:
        datasets['nta'] = download_nta_boundaries()

    if args.all or args.puma:
        datasets['puma'] = download_puma_boundaries(args.year)

    if args.all or args.census_tracts:
        datasets['census_tracts'] = download_census_tracts(args.year)

    # Save metadata
    if datasets:
        save_metadata(datasets)
        print("\n✓ Download complete!")
        print(f"  Processed files: {PROCESSED_DIR}")
        print(f"  Raw files: {RAW_DIR}")
    else:
        print("No datasets selected. Use --all or specify individual datasets.")
        print("Run with --help for usage information.")


if __name__ == "__main__":
    # Import pandas here to avoid import error in help message
    import pandas as pd
    main()
