# NYC Geographic Boundaries

This directory contains tools for downloading, validating, and visualizing NYC geographic boundary data at multiple scales.

## Overview

Geographic boundaries enable spatial analysis of Citi Bike trip data, including:
- Station coverage by neighborhood/borough
- Trip patterns within and between areas
- Socioeconomic analysis (when joined with Census data)
- Service equity and accessibility studies

## Boundary Types

| Type | Count | Description | Use Cases |
|------|-------|-------------|-----------|
| **Boroughs** | 5 | NYC's five boroughs | Macro-level analysis, ridership by borough |
| **NTA** | ~190 | Neighborhood Tabulation Areas | Neighborhood-level patterns, local coverage |
| **PUMA** | ~55 | Public Use Microdata Areas | Census data analysis, demographics |
| **Census Tracts** | ~2,200 | Smallest Census geographic unit | Fine-grained analysis, equity studies |

### What is Each Boundary Type?

#### Boroughs
- **Definition**: NYC's five administrative divisions
- **Created by**: NYC Charter
- **Names**: Manhattan, Brooklyn, Queens, Bronx, Staten Island
- **Population**: 400K - 2.7M per borough
- **Area**: 23 - 178 sq mi
- **Use for**: High-level summaries, borough comparisons

#### NTA (Neighborhood Tabulation Areas)
- **Definition**: Aggregations of census tracts that approximate NYC neighborhoods
- **Created by**: NYC Department of City Planning
- **Version**: 2020 NTA boundaries (NTA2020)
- **Population**: ~15K - 100K per NTA
- **Examples**: "Upper East Side", "Williamsburg", "Astoria"
- **Use for**: Neighborhood-level analysis, community planning

#### PUMA (Public Use Microdata Areas)
- **Definition**: Geographic areas for Census Public Use Microdata Sample (PUMS)
- **Created by**: US Census Bureau
- **Constraint**: Must contain 100K+ population
- **Population**: ~100K - 200K per PUMA
- **Use for**: Joining with Census demographic/economic data, socioeconomic analysis
- **Note**: PUMS data (income, education, occupation) is only available at PUMA level

#### Census Tracts
- **Definition**: Small, relatively permanent statistical subdivisions
- **Created by**: US Census Bureau
- **Population**: ~1,200 - 8,000 per tract (target ~4,000)
- **Area**: Varies widely (small in Manhattan, large in outer boroughs)
- **Use for**: Fine-grained spatial analysis, equity studies, detailed demographic mapping
- **Note**: Boundaries change between Census years (2010 vs 2020)

## Data Sources

### NYC Open Data
- **Boroughs**: [Borough Boundaries](https://data.cityofnewyork.us/City-Government/Borough-Boundaries/tqmj-j8zm)
- **NTA**: [Neighborhood Tabulation Areas](https://data.cityofnewyork.us/City-Government/NTA-map/d3qk-pfyz)
- **License**: Public Domain
- **Format**: GeoJSON via Socrata API

### US Census TIGER/Line Shapefiles
- **PUMA**: [TIGER/Line PUMA Files](https://www2.census.gov/geo/tiger/TIGER2022/PUMA/)
- **Census Tracts**: [TIGER/Line Tract Files](https://www2.census.gov/geo/tiger/TIGER2022/TRACT/)
- **License**: Public Domain
- **Format**: Shapefile (ZIP)
- **Coverage**: NY State (filtered to NYC)

## Installation

Install required Python packages:

```bash
pip install geopandas pandas matplotlib folium
```

Or add to `requirements.txt`:

```
geopandas>=0.12.0
pandas>=1.5.0
matplotlib>=3.5.0
folium>=0.14.0  # Optional, for interactive maps
```

## Quick Start

### 1. Download All Boundaries

```bash
# Download all boundary types (recommended)
python src/geo/fetch_boundaries.py --all

# Or download specific boundaries
python src/geo/fetch_boundaries.py --boroughs --nta
```

This creates:
- `data/geo/raw/` - Raw downloaded files
- `data/geo/processed/` - Cleaned GeoJSON files
- `data/geo/processed/metadata.json` - Download metadata

### 2. Validate Data Quality

```bash
python src/geo/validate_boundaries.py
```

Checks:
- ✓ File existence and readability
- ✓ Coordinate system (WGS84)
- ✓ Geometry validity
- ✓ Bounding box within NYC
- ✓ Expected feature counts
- ✓ Attribute completeness
- ✓ Topology (overlaps, slivers)

### 3. Visualize Boundaries

```bash
# Create all visualizations
python src/geo/visualize_boundaries.py --all

# Create specific plots
python src/geo/visualize_boundaries.py --boroughs --stations

# Create interactive HTML map
python src/geo/visualize_boundaries.py --interactive
```

Outputs saved to `logs/geo_*.png` and `logs/geo_interactive.html`

## File Structure

```
citibike-pipeline/
├── src/geo/
│   ├── README.md                    # This file
│   ├── fetch_boundaries.py          # Download boundary shapefiles
│   ├── validate_boundaries.py       # Data quality checks
│   └── visualize_boundaries.py      # Create maps and visualizations
│
├── data/geo/
│   ├── raw/                         # Raw downloaded files (not in git)
│   │   ├── *.zip                    # TIGER/Line shapefiles
│   │   ├── puma_2022/               # Extracted PUMA shapefile
│   │   └── tract_2022_*/            # Extracted tract shapefiles
│   │
│   └── processed/                   # Cleaned GeoJSON files (not in git)
│       ├── boroughs.geojson         # 5 boroughs
│       ├── nta.geojson              # ~190 NTAs
│       ├── puma.geojson             # ~55 PUMAs
│       ├── census_tracts.geojson    # ~2,200 tracts
│       ├── metadata.json            # Download metadata
│       └── validation_results.json  # Validation output
│
└── logs/
    ├── geo_boroughs.png             # Borough map
    ├── geo_nta.png                  # NTA map
    ├── geo_puma.png                 # PUMA map
    ├── geo_census_tracts.png        # Census tracts map
    ├── geo_comparison.png           # Side-by-side comparison
    ├── geo_stations_overlay.png     # Boundaries + Citi Bike stations
    └── geo_interactive.html         # Interactive Folium map
```

## Usage Examples

### Example 1: Count Stations per Borough

```python
import geopandas as gpd
import pandas as pd

# Load boundaries and stations
boroughs = gpd.read_file("data/geo/processed/boroughs.geojson")
stations = pd.read_csv("reference/current_stations.csv")

# Convert stations to GeoDataFrame
stations_gdf = gpd.GeoDataFrame(
    stations,
    geometry=gpd.points_from_xy(stations.lon, stations.lat),
    crs="EPSG:4326"
)

# Spatial join
joined = gpd.sjoin(stations_gdf, boroughs, how="left", predicate="within")

# Count by borough
print(joined.groupby("borough_name").size())
```

### Example 2: Find NTA for Each Trip

```python
import duckdb
import geopandas as gpd

# Load NTA boundaries
nta = gpd.read_file("data/geo/processed/nta.geojson")

# Query trips
con = duckdb.connect()
trips = con.execute("""
    SELECT
        start_station_id,
        start_station_name,
        start_lat,
        start_lon,
        COUNT(*) as trip_count
    FROM 'data/processed/*2024*.parquet'
    GROUP BY 1, 2, 3, 4
""").df()

# Convert to GeoDataFrame
trips_gdf = gpd.GeoDataFrame(
    trips,
    geometry=gpd.points_from_xy(trips.start_lon, trips.start_lat),
    crs="EPSG:4326"
)

# Spatial join to find NTA
trips_with_nta = gpd.sjoin(trips_gdf, nta[['nta_code', 'nta_name', 'geometry']],
                           how="left", predicate="within")

# Top NTAs by ridership
print(trips_with_nta.groupby(['nta_name', 'borough_name'])['trip_count'].sum().sort_values(ascending=False).head(10))
```

### Example 3: Station Coverage Analysis

```python
import geopandas as gpd
import pandas as pd

# Load boundaries
nta = gpd.read_file("data/geo/processed/nta.geojson")
stations = pd.read_csv("reference/current_stations.csv")

# Convert stations to GeoDataFrame
stations_gdf = gpd.GeoDataFrame(
    stations,
    geometry=gpd.points_from_xy(stations.lon, stations.lat),
    crs="EPSG:4326"
)

# Count stations per NTA
stations_per_nta = gpd.sjoin(stations_gdf, nta, how="right", predicate="within")
coverage = stations_per_nta.groupby('nta_name').size().reset_index(name='station_count')

# Add NTA area (convert from sq degrees to sq km approximately)
nta_with_coverage = nta.merge(coverage, on='nta_name', how='left')
nta_with_coverage['station_count'] = nta_with_coverage['station_count'].fillna(0)
nta_with_coverage['area_sq_km'] = nta_with_coverage.geometry.area * 12100  # rough conversion

# Calculate station density
nta_with_coverage['stations_per_sq_km'] = (
    nta_with_coverage['station_count'] / nta_with_coverage['area_sq_km']
)

# Find underserved areas (no stations)
underserved = nta_with_coverage[nta_with_coverage['station_count'] == 0]
print(f"\nUnderserved NTAs (no stations): {len(underserved)}")
print(underserved[['nta_name', 'borough_name']].sort_values('borough_name'))
```

### Example 4: Joining with Census Demographic Data

```python
import geopandas as gpd
import pandas as pd

# Load PUMA boundaries
puma = gpd.read_file("data/geo/processed/puma.geojson")

# Load Census PUMS data (example - you'd download this separately)
# Source: https://data.census.gov/mdat/#/search?ds=ACSPUMS1Y2022
census_data = pd.read_csv("census_pums_2022.csv")

# Join demographic data to PUMA boundaries
puma_demographics = puma.merge(census_data, on='puma_code', how='left')

# Now you can analyze Citi Bike usage by demographics
# (after spatially joining trips to PUMAs)
```

## Coordinate System

All boundaries are in **WGS84 (EPSG:4326)**:
- Latitude/Longitude coordinates
- Matches Citi Bike trip data coordinates
- Compatible with web mapping tools (Leaflet, Folium, etc.)

NYC Bounding Box:
- Latitude: 40.4774° to 40.9176°
- Longitude: -74.2591° to -73.7004°

## Boundary Versions and Updates

### NTA
- **Current**: 2020 NTA boundaries (NTA2020)
- **Previous**: 2010 NTA boundaries (NTA2010)
- **Changes**: Boundaries redrawn to align with 2020 Census tracts
- **Impact**: Neighborhood definitions changed; use consistent version for time-series analysis

### Census Tracts
- **Current**: 2020 Census tracts
- **Previous**: 2010 Census tracts
- **Changes**: Tract boundaries updated every 10 years
- **Impact**: Tracts are NOT directly comparable across Census years

### PUMA
- **Current**: 2020 PUMAs (PUMA20)
- **Previous**: 2010 PUMAs (PUMA10)
- **Changes**: Redrawn to match 2020 Census tracts and 100K+ population requirement
- **Impact**: Different PUMA codes between 2010 and 2020

## Best Practices

### 1. Choose the Right Boundary Type

| Analysis Goal | Recommended Boundary |
|---------------|---------------------|
| High-level summaries | Boroughs |
| Neighborhood patterns | NTA |
| Census data analysis | PUMA (for PUMS data) or Census Tracts |
| Equity/access studies | Census Tracts (finest granularity) |
| Time-series (2013-2025) | Boroughs (most stable over time) |

### 2. Be Aware of Temporal Changes

- Citi Bike data spans 2013-2025
- Census boundaries changed in 2020
- Use consistent boundary definitions for time-series analysis
- Consider using boroughs for maximum temporal consistency

### 3. Spatial Join Performance

```python
# For large datasets, use spatial index
trips_gdf.sindex  # Creates spatial index automatically
result = gpd.sjoin(trips_gdf, boundaries, how="left", predicate="within")
```

### 4. Handle Edge Cases

```python
# Some stations may be outside all boundaries (e.g., Jersey City)
# Use left join to keep all trips
result = gpd.sjoin(trips_gdf, nta, how="left", predicate="within")

# Check for unmatched
unmatched = result[result['nta_name'].isna()]
print(f"Trips outside boundaries: {len(unmatched)}")
```

## Validation

Run validation checks after downloading:

```bash
python src/geo/validate_boundaries.py
```

Expected results:
- ✓ All files readable
- ✓ CRS is WGS84 (EPSG:4326)
- ✓ No invalid geometries
- ✓ Bounding box within NYC
- ✓ Feature counts within expected ranges:
  - Boroughs: 5
  - NTA: 185-245
  - PUMA: 50-60
  - Census Tracts: 2100-2500

## Advanced: Creating Custom Aggregations

### Aggregate NTA to Community Districts

```python
import geopandas as gpd

nta = gpd.read_file("data/geo/processed/nta.geojson")

# NYC has 59 community districts (example mapping would go here)
# You'd need a crosswalk file: nta_code -> community_district

cd_boundaries = nta.dissolve(by='community_district')
cd_boundaries.to_file("data/geo/processed/community_districts.geojson")
```

### Buffer Analysis (Station Catchment Areas)

```python
import geopandas as gpd
import pandas as pd

stations = pd.read_csv("reference/current_stations.csv")
stations_gdf = gpd.GeoDataFrame(
    stations,
    geometry=gpd.points_from_xy(stations.lon, stations.lat),
    crs="EPSG:4326"
)

# Convert to projected CRS for accurate buffers (meters)
stations_utm = stations_gdf.to_crs("EPSG:32618")  # UTM Zone 18N (NYC)

# 400m buffer (typical walking distance)
stations_utm['buffer'] = stations_utm.geometry.buffer(400)

# Convert back to WGS84
stations_wgs84 = stations_utm.set_geometry('buffer').to_crs("EPSG:4326")

# Save catchment areas
stations_wgs84[['station_id', 'station_name', 'buffer']].rename(
    columns={'buffer': 'geometry'}
).to_file("data/geo/processed/station_catchments.geojson")
```

## Troubleshooting

### Issue: "CRS is not defined"

```python
# If CRS is missing, set it manually
gdf.crs = "EPSG:4326"
```

### Issue: "Geometries are invalid"

```python
# Fix invalid geometries
gdf['geometry'] = gdf.geometry.make_valid()
```

### Issue: "Spatial join returns duplicates"

```python
# Use predicate="within" for point-in-polygon
# Or deduplicate after join
result = gpd.sjoin(trips, nta, how="left", predicate="within")
result = result.drop_duplicates(subset=['trip_id'])  # Adjust to your unique ID
```

### Issue: Downloads are slow

- Census TIGER files can be 10-50MB each
- NTA download is ~10MB from NYC Open Data
- Total download size: ~100MB
- Consider downloading once and archiving in cloud storage

## Resources

### Official Documentation
- [NYC Open Data](https://opendata.cityofnewyork.us/)
- [US Census TIGER/Line](https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.html)
- [NYC Department of City Planning](https://www.nyc.gov/site/planning/data-maps/open-data.page)

### GeoPandas Documentation
- [GeoPandas User Guide](https://geopandas.org/en/stable/docs/user_guide.html)
- [Spatial Joins](https://geopandas.org/en/stable/docs/user_guide/mergingdata.html#spatial-joins)
- [Projections and CRS](https://geopandas.org/en/stable/docs/user_guide/projections.html)

### Related Projects
- [NYC Planning Labs - NYC Boundaries](https://github.com/NYCPlanning/Labs-nyc-boundaries)
- [NYU Center for Urban Science - Boundaries](https://github.com/MODA-NYC/db-geodata)

## Contributing

When adding new boundary types:

1. Update `fetch_boundaries.py` with download logic
2. Update `validate_boundaries.py` with validation checks
3. Update `visualize_boundaries.py` with plotting functions
4. Add expected feature counts to validation
5. Document data source and use cases in this README

## License

- **Scripts**: Project license (see main LICENSE file)
- **Boundary data**:
  - NYC Open Data: Public Domain
  - US Census TIGER/Line: Public Domain

All boundary data is public domain and can be used without restriction.

---

Last updated: 2025-12-10
