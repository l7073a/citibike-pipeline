#!/usr/bin/env python3
"""
Visualize NYC geographic boundaries.

Creates static maps and interactive visualizations of boundary data.
"""

import argparse
from pathlib import Path

import geopandas as gpd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.colors import ListedColormap
import numpy as np

PROCESSED_DIR = Path("data/geo/processed")
OUTPUT_DIR = Path("logs")


def plot_boroughs(save=True):
    """Plot NYC borough boundaries."""
    print("\n=== Plotting Boroughs ===")

    gdf = gpd.read_file(PROCESSED_DIR / "boroughs.geojson")

    # Create figure
    fig, ax = plt.subplots(figsize=(12, 10))

    # Define colors for each borough
    borough_colors = {
        'Manhattan': '#FF6B6B',
        'Brooklyn': '#4ECDC4',
        'Queens': '#45B7D1',
        'Bronx': '#FFA07A',
        'Staten Island': '#98D8C8'
    }

    # Plot each borough with its color
    for idx, row in gdf.iterrows():
        color = borough_colors.get(row['borough_name'], '#CCCCCC')
        gdf[gdf['borough_name'] == row['borough_name']].plot(
            ax=ax,
            color=color,
            edgecolor='black',
            linewidth=2,
            alpha=0.7
        )

    # Add labels
    for idx, row in gdf.iterrows():
        # Get centroid for label placement
        centroid = row.geometry.centroid
        ax.text(
            centroid.x, centroid.y,
            row['borough_name'],
            fontsize=12,
            fontweight='bold',
            ha='center',
            va='center'
        )

    # Create legend
    legend_elements = [
        mpatches.Patch(facecolor=color, edgecolor='black', label=name)
        for name, color in borough_colors.items()
    ]
    ax.legend(handles=legend_elements, loc='upper right', fontsize=10)

    ax.set_title('NYC Borough Boundaries', fontsize=16, fontweight='bold')
    ax.set_xlabel('Longitude')
    ax.set_ylabel('Latitude')
    ax.grid(True, alpha=0.3)

    plt.tight_layout()

    if save:
        output_file = OUTPUT_DIR / "geo_boroughs.png"
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        print(f"Saved: {output_file}")

    return fig, ax


def plot_nta(save=True):
    """Plot NTA boundaries."""
    print("\n=== Plotting NTA Boundaries ===")

    gdf = gpd.read_file(PROCESSED_DIR / "nta.geojson")

    fig, ax = plt.subplots(figsize=(14, 12))

    # Color by borough if available, otherwise use single color
    if 'borough_name' in gdf.columns:
        borough_colors = {
            'Manhattan': '#FF6B6B',
            'Brooklyn': '#4ECDC4',
            'Queens': '#45B7D1',
            'Bronx': '#FFA07A',
            'Staten Island': '#98D8C8'
        }
        gdf['color'] = gdf['borough_name'].map(borough_colors)

        gdf.plot(
            ax=ax,
            color=gdf['color'],
            edgecolor='white',
            linewidth=0.5,
            alpha=0.6
        )

        # Create legend
        legend_elements = [
            mpatches.Patch(facecolor=color, edgecolor='white', label=name)
            for name, color in borough_colors.items()
        ]
        ax.legend(handles=legend_elements, loc='upper right', fontsize=10)
    else:
        # Plot without borough colors
        gdf.plot(
            ax=ax,
            color='lightblue',
            edgecolor='white',
            linewidth=0.5,
            alpha=0.6
        )

    ax.set_title(f'NYC Neighborhood Tabulation Areas (NTAs)\n{len(gdf)} areas',
                 fontsize=16, fontweight='bold')
    ax.set_xlabel('Longitude')
    ax.set_ylabel('Latitude')
    ax.grid(True, alpha=0.3)

    plt.tight_layout()

    if save:
        output_file = OUTPUT_DIR / "geo_nta.png"
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        print(f"Saved: {output_file}")

    return fig, ax


def plot_puma(save=True):
    """Plot PUMA boundaries."""
    print("\n=== Plotting PUMA Boundaries ===")

    gdf = gpd.read_file(PROCESSED_DIR / "puma.geojson")

    fig, ax = plt.subplots(figsize=(14, 12))

    # Use a colormap
    cmap = plt.cm.get_cmap('tab20', len(gdf))

    gdf.plot(
        ax=ax,
        color=[cmap(i) for i in range(len(gdf))],
        edgecolor='black',
        linewidth=1,
        alpha=0.7
    )

    # Add PUMA labels
    for idx, row in gdf.iterrows():
        centroid = row.geometry.centroid
        ax.text(
            centroid.x, centroid.y,
            row['puma_code'],
            fontsize=7,
            ha='center',
            va='center',
            bbox=dict(boxstyle='round,pad=0.3', facecolor='white', alpha=0.7)
        )

    ax.set_title(f'NYC Public Use Microdata Areas (PUMAs)\n{len(gdf)} areas',
                 fontsize=16, fontweight='bold')
    ax.set_xlabel('Longitude')
    ax.set_ylabel('Latitude')
    ax.grid(True, alpha=0.3)

    plt.tight_layout()

    if save:
        output_file = OUTPUT_DIR / "geo_puma.png"
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        print(f"Saved: {output_file}")

    return fig, ax


def plot_census_tracts(save=True):
    """Plot census tract boundaries."""
    print("\n=== Plotting Census Tracts ===")

    gdf = gpd.read_file(PROCESSED_DIR / "census_tracts.geojson")

    fig, ax = plt.subplots(figsize=(14, 12))

    # Color by borough
    borough_colors = {
        'Manhattan': '#FF6B6B',
        'Brooklyn': '#4ECDC4',
        'Queens': '#45B7D1',
        'Bronx': '#FFA07A',
        'Staten Island': '#98D8C8'
    }

    gdf['color'] = gdf['borough_name'].map(borough_colors)

    gdf.plot(
        ax=ax,
        color=gdf['color'],
        edgecolor='white',
        linewidth=0.1,
        alpha=0.4
    )

    # Create legend
    legend_elements = [
        mpatches.Patch(facecolor=color, edgecolor='white', label=name)
        for name, color in borough_colors.items()
    ]
    ax.legend(handles=legend_elements, loc='upper right', fontsize=10)

    ax.set_title(f'NYC Census Tracts\n{len(gdf)} tracts',
                 fontsize=16, fontweight='bold')
    ax.set_xlabel('Longitude')
    ax.set_ylabel('Latitude')
    ax.grid(True, alpha=0.3)

    plt.tight_layout()

    if save:
        output_file = OUTPUT_DIR / "geo_census_tracts.png"
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        print(f"Saved: {output_file}")

    return fig, ax


def plot_comparison(save=True):
    """Plot all boundaries on one map for comparison."""
    print("\n=== Plotting Boundary Comparison ===")

    fig, axes = plt.subplots(2, 2, figsize=(18, 16))

    # Load all datasets
    boroughs = gpd.read_file(PROCESSED_DIR / "boroughs.geojson")
    nta = gpd.read_file(PROCESSED_DIR / "nta.geojson")
    puma = gpd.read_file(PROCESSED_DIR / "puma.geojson")
    census_tracts = gpd.read_file(PROCESSED_DIR / "census_tracts.geojson")

    # 1. Boroughs
    ax = axes[0, 0]
    boroughs.plot(ax=ax, color='lightblue', edgecolor='black', linewidth=2)
    for idx, row in boroughs.iterrows():
        centroid = row.geometry.centroid
        ax.text(centroid.x, centroid.y, row['borough_name'],
                fontsize=10, ha='center', va='center', fontweight='bold')
    ax.set_title(f'Boroughs (n={len(boroughs)})', fontsize=12, fontweight='bold')
    ax.grid(True, alpha=0.3)

    # 2. NTA
    ax = axes[0, 1]
    nta.plot(ax=ax, color='lightgreen', edgecolor='white', linewidth=0.5)
    ax.set_title(f'NTA (n={len(nta)})', fontsize=12, fontweight='bold')
    ax.grid(True, alpha=0.3)

    # 3. PUMA
    ax = axes[1, 0]
    puma.plot(ax=ax, color='lightyellow', edgecolor='black', linewidth=1)
    ax.set_title(f'PUMA (n={len(puma)})', fontsize=12, fontweight='bold')
    ax.grid(True, alpha=0.3)

    # 4. Census Tracts
    ax = axes[1, 1]
    census_tracts.plot(ax=ax, color='lightcoral', edgecolor='white', linewidth=0.1)
    ax.set_title(f'Census Tracts (n={len(census_tracts)})', fontsize=12, fontweight='bold')
    ax.grid(True, alpha=0.3)

    plt.suptitle('NYC Geographic Boundaries Comparison',
                 fontsize=16, fontweight='bold', y=0.995)
    plt.tight_layout()

    if save:
        output_file = OUTPUT_DIR / "geo_comparison.png"
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        print(f"Saved: {output_file}")

    return fig, axes


def plot_with_citibike_stations(save=True):
    """Plot boundaries with Citi Bike stations overlaid."""
    print("\n=== Plotting Boundaries with Citi Bike Stations ===")

    # Load borough boundaries
    boroughs = gpd.read_file(PROCESSED_DIR / "boroughs.geojson")

    # Load Citi Bike stations
    stations_file = Path("reference/current_stations.csv")
    if not stations_file.exists():
        print(f"⚠ Stations file not found: {stations_file}")
        print("  Run: python src/fetch_stations.py")
        return None, None

    import pandas as pd
    stations = pd.read_csv(stations_file)

    # Create figure
    fig, ax = plt.subplots(figsize=(14, 12))

    # Plot boroughs
    boroughs.plot(
        ax=ax,
        color='lightgray',
        edgecolor='black',
        linewidth=2,
        alpha=0.5
    )

    # Plot stations
    ax.scatter(
        stations['lon'],
        stations['lat'],
        s=3,
        c='red',
        alpha=0.6,
        label=f'Citi Bike Stations (n={len(stations)})'
    )

    # Add borough labels
    for idx, row in boroughs.iterrows():
        centroid = row.geometry.centroid
        ax.text(
            centroid.x, centroid.y,
            row['borough_name'],
            fontsize=10,
            ha='center',
            va='center',
            fontweight='bold',
            bbox=dict(boxstyle='round,pad=0.5', facecolor='white', alpha=0.7)
        )

    ax.set_title('NYC Boroughs with Citi Bike Stations',
                 fontsize=16, fontweight='bold')
    ax.set_xlabel('Longitude')
    ax.set_ylabel('Latitude')
    ax.legend(loc='upper right', fontsize=10)
    ax.grid(True, alpha=0.3)

    plt.tight_layout()

    if save:
        output_file = OUTPUT_DIR / "geo_stations_overlay.png"
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        print(f"Saved: {output_file}")

    return fig, ax


def create_interactive_map(output_file="logs/geo_interactive.html"):
    """
    Create an interactive Folium map with all boundaries.

    Requires: folium
    Install with: pip install folium
    """
    try:
        import folium
    except ImportError:
        print("\n⚠ Folium not installed. Install with: pip install folium")
        return

    print("\n=== Creating Interactive Map ===")

    # Load datasets
    boroughs = gpd.read_file(PROCESSED_DIR / "boroughs.geojson")
    nta = gpd.read_file(PROCESSED_DIR / "nta.geojson")

    # Create map centered on NYC
    m = folium.Map(
        location=[40.7128, -74.0060],
        zoom_start=11,
        tiles='CartoDB positron'
    )

    # Add borough boundaries
    folium.GeoJson(
        boroughs,
        name='Boroughs',
        style_function=lambda x: {
            'fillColor': '#3388ff',
            'color': 'black',
            'weight': 3,
            'fillOpacity': 0.1
        },
        tooltip=folium.GeoJsonTooltip(fields=['borough_name'], aliases=['Borough:'])
    ).add_to(m)

    # Add NTA boundaries
    folium.GeoJson(
        nta,
        name='NTA',
        style_function=lambda x: {
            'fillColor': '#ff7800',
            'color': 'white',
            'weight': 1,
            'fillOpacity': 0.3
        },
        tooltip=folium.GeoJsonTooltip(
            fields=['nta_name', 'borough_name'],
            aliases=['NTA:', 'Borough:']
        )
    ).add_to(m)

    # Add layer control
    folium.LayerControl().add_to(m)

    # Save map
    output_path = Path(output_file)
    m.save(str(output_path))
    print(f"Saved: {output_path}")
    print(f"Open in browser: file://{output_path.absolute()}")

    return m


def main():
    parser = argparse.ArgumentParser(
        description="Visualize NYC geographic boundaries",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Create all visualizations
  python visualize_boundaries.py --all

  # Create specific visualizations
  python visualize_boundaries.py --boroughs --nta

  # Create interactive map
  python visualize_boundaries.py --interactive

  # Show plots instead of saving
  python visualize_boundaries.py --all --show
        """
    )

    parser.add_argument('--all', action='store_true',
                        help='Create all visualizations')
    parser.add_argument('--boroughs', action='store_true',
                        help='Plot borough boundaries')
    parser.add_argument('--nta', action='store_true',
                        help='Plot NTA boundaries')
    parser.add_argument('--puma', action='store_true',
                        help='Plot PUMA boundaries')
    parser.add_argument('--census-tracts', action='store_true',
                        help='Plot census tract boundaries')
    parser.add_argument('--comparison', action='store_true',
                        help='Plot all boundaries for comparison')
    parser.add_argument('--stations', action='store_true',
                        help='Plot boundaries with Citi Bike stations')
    parser.add_argument('--interactive', action='store_true',
                        help='Create interactive HTML map')
    parser.add_argument('--show', action='store_true',
                        help='Show plots instead of saving')

    args = parser.parse_args()

    # Create output directory
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    save = not args.show

    # Create requested visualizations
    if args.all or args.boroughs:
        plot_boroughs(save=save)

    if args.all or args.nta:
        plot_nta(save=save)

    if args.all or args.puma:
        plot_puma(save=save)

    if args.all or args.census_tracts:
        plot_census_tracts(save=save)

    if args.all or args.comparison:
        plot_comparison(save=save)

    if args.all or args.stations:
        plot_with_citibike_stations(save=save)

    if args.all or args.interactive:
        create_interactive_map()

    if args.show:
        plt.show()

    if not any([args.all, args.boroughs, args.nta, args.puma, args.census_tracts,
                args.comparison, args.stations, args.interactive]):
        print("No visualizations selected. Use --all or specify individual plots.")
        print("Run with --help for usage information.")
    else:
        print("\n✓ Visualization complete!")


if __name__ == "__main__":
    main()
