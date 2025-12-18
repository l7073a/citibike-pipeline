#!/usr/bin/env python3
"""
Download ALL geographic boundary vintages needed for Citi Bike analysis.

Downloads:
- 2010 and 2020 NTA (for time series analysis 2013-2025)
- 2010 and 2020 PUMA (for Census data joins)
- 2010 and 2020 Census Tracts (NYC)
- 2020 PUMA and Census Tracts for Hudson County NJ (for Jersey City data)

File naming: {type}_{vintage}_{area}.geojson
Examples: nta_2010_nyc.geojson, puma_2020_hudson_nj.geojson
"""

import argparse
import json
from datetime import datetime
from pathlib import Path
from urllib.request import urlretrieve
import zipfile

import geopandas as gpd
import pandas as pd

# Output directories
# ArcGIS servers have a maxRecordCount limit (typically 2000)
# We need to paginate to get all features
ARCGIS_PAGE_SIZE = 2000
RAW_DIR = Path("data/geo/raw")


def fetch_arcgis_paginated(base_url: str) -> gpd.GeoDataFrame:
    """Fetch all features from ArcGIS FeatureServer with pagination.

    ArcGIS servers have maxRecordCount limits (typically 2000).
    This function paginates through all records.
    """
    all_features = []
    offset = 0

    while True:
        url = f"{base_url}&resultOffset={offset}&resultRecordCount={ARCGIS_PAGE_SIZE}"
        print(f"  Fetching records {offset} to {offset + ARCGIS_PAGE_SIZE}...")

        gdf = gpd.read_file(url)

        if len(gdf) == 0:
            break

        all_features.append(gdf)
        print(f"    Got {len(gdf)} features")

        offset += ARCGIS_PAGE_SIZE

        if len(gdf) < ARCGIS_PAGE_SIZE:
            break

    if not all_features:
        raise ValueError(f"No features returned from {base_url}")

    # Combine all pages
    full_gdf = pd.concat(all_features, ignore_index=True)

    # Remove duplicate columns if any
    full_gdf = full_gdf.loc[:, ~full_gdf.columns.duplicated()]

    return full_gdf


PROCESSED_DIR = Path("data/geo/processed")

# NYC bounding box for validation (WGS84)
NYC_BBOX = {
    "min_lat": 40.4774,
    "max_lat": 40.9176,
    "min_lon": -74.2591,
    "max_lon": -73.7004,
}

# Hudson County NJ bounding box (includes Jersey City, Hoboken, etc.)
HUDSON_NJ_BBOX = {
    "min_lat": 40.6,
    "max_lat": 40.85,
    "min_lon": -74.12,
    "max_lon": -73.95,
}


def download_nta_2010():
    """Download 2010 NTA boundaries from NYC Planning."""
    print("\n=== Downloading 2010 NTA Boundaries ===")

    url = "https://services5.arcgis.com/GfwWNkhOj9bNBqoJ/arcgis/rest/services/NYC_2010_NTA/FeatureServer/0/query?where=1=1&outFields=*&outSR=4326&f=geojson"
    output_file = PROCESSED_DIR / "nta_2010_nyc.geojson"

    print(f"Downloading from: NYC Planning ArcGIS Server")
    print("Note: This is the 2010 NTA definition (based on 2010 Census)")

    gdf = gpd.read_file(url)
    print(f"Original columns: {list(gdf.columns)}")

    # Standardize column names
    rename_map = {
        'NTACode': 'nta_code',
        'NTAName': 'nta_name',
        'BoroCode': 'borough_code',
        'BoroName': 'borough_name'
    }
    gdf = gdf.rename(columns={k: v for k, v in rename_map.items() if k in gdf.columns})

    # Remove geometry metadata
    drop_cols = ['Shape__Area', 'Shape__Length', 'OBJECTID']
    gdf = gdf.drop(columns=[col for col in drop_cols if col in gdf.columns])

    # Save
    gdf.to_file(output_file, driver="GeoJSON")
    print(f"Saved {len(gdf)} 2010 NTAs to: {output_file}")

    return gdf


def download_puma_2010():
    """Download 2010 PUMA boundaries for NY State (NYC area)."""
    print("\n=== Downloading 2010 PUMA Boundaries ===")

    url = "https://services5.arcgis.com/GfwWNkhOj9bNBqoJ/arcgis/rest/services/NYC_2010_PUMA/FeatureServer/0/query?where=1=1&outFields=*&outSR=4326&f=geojson"
    output_file = PROCESSED_DIR / "puma_2010_nyc.geojson"

    print(f"Downloading from: NYC Planning ArcGIS Server")
    print("Note: This is the 2010 PUMA definition (used in 2012-2021 ACS)")

    gdf = gpd.read_file(url)
    print(f"Original columns: {list(gdf.columns)}")

    # Standardize column names
    rename_map = {
        'PUMA': 'puma_code',
        'PUMA_Name': 'puma_name'
    }
    gdf = gdf.rename(columns={k: v for k, v in rename_map.items() if k in gdf.columns})

    # Remove geometry metadata
    drop_cols = ['Shape__Area', 'Shape__Length', 'OBJECTID']
    gdf = gdf.drop(columns=[col for col in drop_cols if col in gdf.columns])

    # Save
    gdf.to_file(output_file, driver="GeoJSON")
    print(f"Saved {len(gdf)} 2010 PUMAs to: {output_file}")

    return gdf


def download_census_tracts_2010():
    """Download 2010 Census Tracts for NYC with pagination."""
    print("\n=== Downloading 2010 Census Tracts ===")

    base_url = "https://services5.arcgis.com/GfwWNkhOj9bNBqoJ/arcgis/rest/services/NYC_Census_Tracts_for_2010_US_Census/FeatureServer/0/query?where=1=1&outFields=*&outSR=4326&f=geojson"
    output_file = PROCESSED_DIR / "census_tracts_2010_nyc.geojson"

    print(f"Downloading from: NYC Planning ArcGIS Server (with pagination)")
    print("Note: This is the 2010 Census definition")

    # Use pagination - server has maxRecordCount of 2000
    gdf = fetch_arcgis_paginated(base_url)
    print(f"Total features: {len(gdf)}")
    print(f"Original columns: {list(gdf.columns)}")

    # Map borough codes to names (2010 data uses different column names)
    boro_map = {'1': 'Manhattan', '2': 'Bronx', '3': 'Brooklyn', '4': 'Queens', '5': 'Staten Island'}
    if 'BOROCODE' in gdf.columns:
        gdf['borough_name'] = gdf['BOROCODE'].astype(str).map(boro_map)
        gdf = gdf.rename(columns={
            'CT': 'tract_code',
            'BOROCT': 'tract_geoid',
            'BOROCODE': 'borough_code',
        })
    else:
        # Alternative column names
        rename_map = {
            'CT2010': 'tract_code',
            'BoroCT2010': 'tract_geoid',
            'BoroCode': 'borough_code',
            'BoroName': 'borough_name'
        }
        gdf = gdf.rename(columns={k: v for k, v in rename_map.items() if k in gdf.columns})

    # Keep only essential columns
    keep_cols = ['tract_code', 'tract_geoid', 'borough_code', 'borough_name', 'geometry']
    gdf = gdf[[col for col in keep_cols if col in gdf.columns]]

    # Save
    gdf.to_file(output_file, driver="GeoJSON")
    print(f"Saved {len(gdf)} 2010 census tracts to: {output_file}")

    return gdf


def download_census_tracts_2020():
    """Download 2020 Census Tracts for NYC with pagination."""
    print("\n=== Downloading 2020 Census Tracts ===")

    base_url = "https://services5.arcgis.com/GfwWNkhOj9bNBqoJ/arcgis/rest/services/NYC_Census_Tracts_for_2020_US_Census/FeatureServer/0/query?where=1=1&outFields=*&outSR=4326&f=geojson"
    output_file = PROCESSED_DIR / "census_tracts_2020_nyc.geojson"

    print(f"Downloading from: NYC Planning ArcGIS Server (with pagination)")
    print("Note: This is the 2020 Census definition")

    # Use pagination - server has maxRecordCount of 2000
    gdf = fetch_arcgis_paginated(base_url)
    print(f"Total features: {len(gdf)}")

    # Standardize column names
    rename_map = {
        'CT2020': 'tract_code',
        'BoroCT2020': 'tract_geoid',
        'BoroCode': 'borough_code',
        'BoroName': 'borough_name',
    }
    gdf = gdf.rename(columns={k: v for k, v in rename_map.items() if k in gdf.columns})

    # Keep only essential columns
    keep_cols = ['tract_code', 'tract_geoid', 'borough_code', 'borough_name', 'geometry']
    gdf = gdf[[col for col in keep_cols if col in gdf.columns]]

    # Save
    gdf.to_file(output_file, driver="GeoJSON")
    print(f"Saved {len(gdf)} 2020 census tracts to: {output_file}")

    return gdf


def download_hudson_county_puma_2020():
    """Download 2020 PUMA boundaries for Hudson County NJ."""
    print("\n=== Downloading Hudson County NJ PUMA (2020) ===")

    # Download NJ state PUMAs from Census TIGER
    year = 2022
    base_url = f"https://www2.census.gov/geo/tiger/TIGER{year}/PUMA/"
    filename = f"tl_{year}_34_puma20.zip"  # 34 = NJ state FIPS
    url = base_url + filename

    raw_zip = RAW_DIR / filename
    output_file = PROCESSED_DIR / "puma_2020_hudson_nj.geojson"

    print(f"Downloading from: {url}")
    urlretrieve(url, raw_zip)

    # Extract
    extract_dir = RAW_DIR / f"puma_2020_nj"
    with zipfile.ZipFile(raw_zip, 'r') as zip_ref:
        zip_ref.extractall(extract_dir)

    # Read shapefile
    shp_file = list(extract_dir.glob("*.shp"))[0]
    gdf = gpd.read_file(shp_file)

    # Filter to Hudson County area (by bounding box)
    gdf_hudson = gdf.cx[HUDSON_NJ_BBOX['min_lon']:HUDSON_NJ_BBOX['max_lon'],
                         HUDSON_NJ_BBOX['min_lat']:HUDSON_NJ_BBOX['max_lat']]

    # Standardize columns
    rename_map = {
        'PUMACE20': 'puma_code',
        'NAMELSAD20': 'puma_name',
        'GEOID20': 'puma_geoid'
    }
    gdf_hudson = gdf_hudson.rename(columns={k: v for k, v in rename_map.items() if k in gdf_hudson.columns})

    # Keep essential columns
    keep_cols = ['puma_code', 'puma_name', 'puma_geoid', 'geometry']
    gdf_hudson = gdf_hudson[[col for col in keep_cols if col in gdf_hudson.columns]]

    # Save
    gdf_hudson.to_file(output_file, driver="GeoJSON")
    print(f"Saved {len(gdf_hudson)} Hudson County PUMAs to: {output_file}")

    return gdf_hudson


def download_hudson_county_census_tracts_2020():
    """Download 2020 Census Tracts for Hudson County NJ."""
    print("\n=== Downloading Hudson County NJ Census Tracts (2020) ===")

    # Download ALL NJ tracts, then filter to Hudson County
    year = 2020
    base_url = f"https://www2.census.gov/geo/tiger/TIGER{year}/TRACT/"
    filename = f"tl_{year}_34_tract.zip"  # 34 = NJ state FIPS
    url = base_url + filename

    raw_zip = RAW_DIR / filename
    output_file = PROCESSED_DIR / "census_tracts_2020_hudson_nj.geojson"

    print(f"Downloading from: {url}")
    print(f"Note: Downloading all NJ tracts, will filter to Hudson County (017)")
    urlretrieve(url, raw_zip)

    # Extract
    extract_dir = RAW_DIR / f"tract_2020_nj"
    with zipfile.ZipFile(raw_zip, 'r') as zip_ref:
        zip_ref.extractall(extract_dir)

    # Read shapefile
    shp_file = list(extract_dir.glob("*.shp"))[0]
    gdf = gpd.read_file(shp_file)

    # Filter to Hudson County (county code 017)
    gdf_hudson = gdf[gdf['COUNTYFP'] == '017'].copy()
    print(f"Filtered {len(gdf)} NJ tracts to {len(gdf_hudson)} Hudson County tracts")

    # Standardize columns
    rename_map = {
        'TRACTCE': 'tract_code',
        'GEOID': 'tract_geoid',
        'NAME': 'tract_name',
        'COUNTYFP': 'county_code'
    }
    gdf_hudson = gdf_hudson.rename(columns={k: v for k, v in rename_map.items() if k in gdf_hudson.columns})

    # Add county name
    gdf_hudson['county_name'] = 'Hudson'

    # Keep essential columns
    keep_cols = ['tract_code', 'tract_geoid', 'tract_name', 'county_code', 'county_name', 'geometry']
    gdf_hudson = gdf_hudson[[col for col in keep_cols if col in gdf_hudson.columns]]

    # Save
    gdf_hudson.to_file(output_file, driver="GeoJSON")
    print(f"Saved {len(gdf_hudson)} Hudson County census tracts to: {output_file}")

    return gdf_hudson


def create_inventory():
    """Create inventory of all downloaded boundaries."""
    print("\n=== Creating Inventory ===")

    inventory = {
        "download_timestamp": datetime.now().isoformat(),
        "files": {},
        "notes": {
            "2010_vintage": "Used for 2013-2019 Citi Bike data analysis",
            "2020_vintage": "Used for 2020-2025 Citi Bike data analysis",
            "hudson_nj": "Used for Jersey City Citi Bike data analysis"
        }
    }

    for geojson_file in PROCESSED_DIR.glob("*.geojson"):
        if geojson_file.stem in ['metadata', 'validation_results']:
            continue

        gdf = gpd.read_file(geojson_file)
        inventory["files"][geojson_file.name] = {
            "feature_count": len(gdf),
            "columns": list(gdf.columns),
            "bbox": gdf.total_bounds.tolist(),
            "file_size_mb": round(geojson_file.stat().st_size / 1_000_000, 2)
        }

    inventory_file = PROCESSED_DIR / "boundary_inventory.json"
    with open(inventory_file, 'w') as f:
        json.dump(inventory, f, indent=2)

    print(f"Inventory saved to: {inventory_file}")
    return inventory


def main():
    parser = argparse.ArgumentParser(
        description="Download all geographic boundary vintages for Citi Bike analysis"
    )

    parser.add_argument('--all', action='store_true',
                        help='Download all vintages and areas')
    parser.add_argument('--vintage-2010', action='store_true',
                        help='Download 2010 vintage (NTA, PUMA, Census Tracts)')
    parser.add_argument('--vintage-2020', action='store_true',
                        help='Download 2020 vintage (already have, re-download)')
    parser.add_argument('--hudson-nj', action='store_true',
                        help='Download Hudson County NJ boundaries')

    args = parser.parse_args()

    # Create directories
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    datasets = {}

    if args.all or args.vintage_2010:
        print("\n" + "="*60)
        print("DOWNLOADING 2010 VINTAGE BOUNDARIES")
        print("="*60)
        datasets['nta_2010'] = download_nta_2010()
        datasets['puma_2010'] = download_puma_2010()
        datasets['census_tracts_2010'] = download_census_tracts_2010()

    if args.all or args.vintage_2020:
        print("\n" + "="*60)
        print("DOWNLOADING 2020 VINTAGE BOUNDARIES")
        print("="*60)
        datasets['census_tracts_2020'] = download_census_tracts_2020()
        # Note: NTA 2020 and PUMA 2020 were downloaded in a previous session
        # Add those download functions here if re-download is needed

    if args.all or args.hudson_nj:
        print("\n" + "="*60)
        print("DOWNLOADING HUDSON COUNTY NJ BOUNDARIES")
        print("="*60)
        datasets['puma_hudson_nj'] = download_hudson_county_puma_2020()
        datasets['tracts_hudson_nj'] = download_hudson_county_census_tracts_2020()

    # Create inventory
    create_inventory()

    print("\n" + "="*60)
    print("✓ DOWNLOAD COMPLETE!")
    print("="*60)
    print(f"Files saved to: {PROCESSED_DIR}")


if __name__ == "__main__":
    main()
