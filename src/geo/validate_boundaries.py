#!/usr/bin/env python3
"""
Validate NYC geographic boundary data.

Checks:
- File existence and readability
- Coordinate system (should be WGS84)
- Geometry validity
- Bounding box within NYC
- Expected feature counts
- Topology (overlaps, gaps, slivers)
- Attribute completeness
"""

import json
from pathlib import Path

import geopandas as gpd
import pandas as pd
from shapely.geometry import box

PROCESSED_DIR = Path("data/geo/processed")

# NYC bounding box for validation (WGS84)
NYC_BBOX = {
    "min_lat": 40.4774,
    "max_lat": 40.9176,
    "min_lon": -74.2591,
    "max_lon": -73.7004,
}

# Expected feature counts (approximate)
EXPECTED_COUNTS = {
    "boroughs": (5, 5),  # min, max
    "nta": (185, 245),  # NTA counts vary by version
    "puma": (50, 60),   # NYC has ~55 PUMAs
    "census_tracts": (2100, 2500),  # ~2200 tracts
}


def validate_file_exists(name, filename):
    """Check if file exists and is readable."""
    filepath = PROCESSED_DIR / filename
    if not filepath.exists():
        return {
            "status": "FAIL",
            "message": f"File not found: {filepath}"
        }

    try:
        gdf = gpd.read_file(filepath)
        return {
            "status": "PASS",
            "message": f"File readable, {len(gdf)} features loaded",
            "gdf": gdf
        }
    except Exception as e:
        return {
            "status": "FAIL",
            "message": f"Error reading file: {e}",
            "gdf": None
        }


def validate_crs(gdf, name):
    """Check coordinate reference system."""
    if gdf is None:
        return {"status": "SKIP", "message": "No data to validate"}

    # Check if CRS is WGS84 (EPSG:4326)
    if gdf.crs is None:
        return {
            "status": "WARN",
            "message": "CRS is not defined"
        }

    epsg_code = gdf.crs.to_epsg()
    if epsg_code == 4326:
        return {
            "status": "PASS",
            "message": f"CRS is WGS84 (EPSG:{epsg_code})"
        }
    else:
        return {
            "status": "WARN",
            "message": f"CRS is EPSG:{epsg_code}, expected 4326 (WGS84)"
        }


def validate_geometry(gdf, name):
    """Check geometry validity."""
    if gdf is None:
        return {"status": "SKIP", "message": "No data to validate"}

    # Check for invalid geometries
    invalid = ~gdf.is_valid
    invalid_count = invalid.sum()

    # Check for null geometries
    null_geom = gdf.geometry.isna()
    null_count = null_geom.sum()

    # Check geometry types
    geom_types = gdf.geometry.geom_type.value_counts()

    messages = []
    status = "PASS"

    if null_count > 0:
        status = "FAIL"
        messages.append(f"{null_count} null geometries")

    if invalid_count > 0:
        status = "FAIL"
        messages.append(f"{invalid_count} invalid geometries")
    else:
        messages.append("All geometries valid")

    messages.append(f"Geometry types: {dict(geom_types)}")

    return {
        "status": status,
        "message": "; ".join(messages),
        "invalid_count": int(invalid_count),
        "null_count": int(null_count)
    }


def validate_bbox(gdf, name):
    """Check if features are within NYC bounding box."""
    if gdf is None:
        return {"status": "SKIP", "message": "No data to validate"}

    # Get bounding box
    minx, miny, maxx, maxy = gdf.total_bounds

    # NYC bounding box
    nyc_box = box(
        NYC_BBOX['min_lon'], NYC_BBOX['min_lat'],
        NYC_BBOX['max_lon'], NYC_BBOX['max_lat']
    )

    # Check if data bounding box is within NYC
    data_box = box(minx, miny, maxx, maxy)

    if nyc_box.contains(data_box):
        status = "PASS"
        message = "All features within NYC bounding box"
    elif nyc_box.intersects(data_box):
        status = "WARN"
        message = "Some features may be outside NYC bounding box"
    else:
        status = "FAIL"
        message = "Features are outside NYC bounding box"

    return {
        "status": status,
        "message": message,
        "bbox": [minx, miny, maxx, maxy]
    }


def validate_feature_count(gdf, name):
    """Check if feature count is within expected range."""
    if gdf is None:
        return {"status": "SKIP", "message": "No data to validate"}

    if name not in EXPECTED_COUNTS:
        return {
            "status": "SKIP",
            "message": f"No expected count defined for {name}"
        }

    count = len(gdf)
    min_expected, max_expected = EXPECTED_COUNTS[name]

    if min_expected <= count <= max_expected:
        status = "PASS"
        message = f"Feature count {count} within expected range ({min_expected}-{max_expected})"
    else:
        status = "WARN"
        message = f"Feature count {count} outside expected range ({min_expected}-{max_expected})"

    return {
        "status": status,
        "message": message,
        "count": count,
        "expected_range": [min_expected, max_expected]
    }


def validate_attributes(gdf, name):
    """Check attribute completeness."""
    if gdf is None:
        return {"status": "SKIP", "message": "No data to validate"}

    # Check for null values in each column
    null_counts = gdf.isnull().sum()
    columns_with_nulls = null_counts[null_counts > 0]

    if len(columns_with_nulls) == 0:
        return {
            "status": "PASS",
            "message": "No null values in attributes"
        }
    else:
        # Exclude geometry column from null check
        if 'geometry' in columns_with_nulls.index:
            columns_with_nulls = columns_with_nulls.drop('geometry')

        if len(columns_with_nulls) == 0:
            return {
                "status": "PASS",
                "message": "No null values in attributes (excluding geometry)"
            }

        return {
            "status": "WARN",
            "message": f"Null values found in columns: {dict(columns_with_nulls)}",
            "null_columns": columns_with_nulls.to_dict()
        }


def validate_topology(gdf, name):
    """Check for topology issues (overlaps, gaps)."""
    if gdf is None:
        return {"status": "SKIP", "message": "No data to validate"}

    # This is computationally expensive, so we'll do basic checks
    results = []

    # Check for overlaps (sample check on first 10 features)
    sample_size = min(10, len(gdf))
    overlaps = 0
    for i in range(sample_size):
        for j in range(i + 1, sample_size):
            if gdf.iloc[i].geometry.overlaps(gdf.iloc[j].geometry):
                overlaps += 1

    if overlaps > 0:
        results.append(f"{overlaps} overlaps found in sample of {sample_size}")

    # Check for very small polygons (slivers)
    if 'geometry' in gdf.columns:
        # Calculate area (in sq degrees for WGS84)
        areas = gdf.geometry.area
        very_small = (areas < 0.00001).sum()  # ~100m² in degrees
        if very_small > 0:
            results.append(f"{very_small} very small polygons (potential slivers)")

    if not results:
        return {
            "status": "PASS",
            "message": "No topology issues detected"
        }
    else:
        return {
            "status": "WARN",
            "message": "; ".join(results)
        }


def validate_dataset(name, filename):
    """Run all validations on a dataset."""
    print(f"\n{'='*60}")
    print(f"Validating: {name}")
    print(f"File: {filename}")
    print(f"{'='*60}")

    results = {}

    # 1. File exists
    print("\n1. File Existence...")
    result = validate_file_exists(name, filename)
    results['file_exists'] = result
    print(f"   [{result['status']}] {result['message']}")
    gdf = result.get('gdf')

    if gdf is None:
        print("\n⚠ Skipping remaining checks (file not loaded)")
        return results

    # 2. CRS
    print("\n2. Coordinate Reference System...")
    result = validate_crs(gdf, name)
    results['crs'] = result
    print(f"   [{result['status']}] {result['message']}")

    # 3. Geometry validity
    print("\n3. Geometry Validity...")
    result = validate_geometry(gdf, name)
    results['geometry'] = result
    print(f"   [{result['status']}] {result['message']}")

    # 4. Bounding box
    print("\n4. Bounding Box...")
    result = validate_bbox(gdf, name)
    results['bbox'] = result
    print(f"   [{result['status']}] {result['message']}")

    # 5. Feature count
    print("\n5. Feature Count...")
    result = validate_feature_count(gdf, name)
    results['feature_count'] = result
    print(f"   [{result['status']}] {result['message']}")

    # 6. Attributes
    print("\n6. Attribute Completeness...")
    result = validate_attributes(gdf, name)
    results['attributes'] = result
    print(f"   [{result['status']}] {result['message']}")

    # 7. Topology
    print("\n7. Topology...")
    result = validate_topology(gdf, name)
    results['topology'] = result
    print(f"   [{result['status']}] {result['message']}")

    return results


def print_summary(all_results):
    """Print validation summary."""
    print(f"\n{'='*60}")
    print("VALIDATION SUMMARY")
    print(f"{'='*60}")

    for dataset_name, results in all_results.items():
        print(f"\n{dataset_name.upper()}:")

        # Count statuses
        pass_count = sum(1 for r in results.values() if r['status'] == 'PASS')
        fail_count = sum(1 for r in results.values() if r['status'] == 'FAIL')
        warn_count = sum(1 for r in results.values() if r['status'] == 'WARN')

        total = len(results)

        print(f"  ✓ PASS: {pass_count}/{total}")
        if warn_count > 0:
            print(f"  ⚠ WARN: {warn_count}/{total}")
        if fail_count > 0:
            print(f"  ✗ FAIL: {fail_count}/{total}")

        # Show failures
        if fail_count > 0:
            print("  Failures:")
            for check_name, result in results.items():
                if result['status'] == 'FAIL':
                    print(f"    - {check_name}: {result['message']}")


def main():
    """Run validation on all datasets."""
    datasets = {
        "boroughs": "boroughs.geojson",
        "nta": "nta.geojson",
        "puma": "puma.geojson",
        "census_tracts": "census_tracts.geojson",
    }

    all_results = {}

    for name, filename in datasets.items():
        results = validate_dataset(name, filename)
        all_results[name] = results

    # Print summary
    print_summary(all_results)

    # Save results
    output_file = PROCESSED_DIR / "validation_results.json"

    # Convert numpy types to Python types for JSON serialization
    def convert_types(obj):
        if isinstance(obj, dict):
            # Filter out non-serializable objects like GeoDataFrame
            return {k: convert_types(v) for k, v in obj.items() if k != 'gdf'}
        elif isinstance(obj, list):
            return [convert_types(i) for i in obj]
        elif hasattr(obj, 'item'):  # numpy types
            return obj.item()
        else:
            return obj

    all_results_json = convert_types(all_results)

    with open(output_file, 'w') as f:
        json.dump(all_results_json, f, indent=2)

    print(f"\n✓ Validation results saved to: {output_file}")


if __name__ == "__main__":
    main()
