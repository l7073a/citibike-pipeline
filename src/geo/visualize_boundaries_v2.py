#!/usr/bin/env python3
"""
Improved visualization for NYC geographic boundaries - Version 2.

Key improvements:
- Clear year labels (2020 NTA, 2020 Census, etc.)
- Background map toggle
- Show both codes AND names
- Better labeling for auditability
- Interactive maps with tooltips
"""

import argparse
from pathlib import Path

import geopandas as gpd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import contextily as ctx
import pandas as pd

PROCESSED_DIR = Path("data/geo/processed")
OUTPUT_DIR = Path("logs")


def add_basemap(ax, zoom='auto', source=ctx.providers.CartoDB.Positron):
    """
    Add a background map to the plot.

    Args:
        ax: Matplotlib axes object
        zoom: Zoom level or 'auto'
        source: Tile source (CartoDB.Positron is clean for overlays)
    """
    try:
        ctx.add_basemap(ax, crs='EPSG:4326', source=source, zoom=zoom, alpha=0.5)
    except Exception as e:
        print(f"  Warning: Could not add basemap: {e}")


def plot_boroughs_improved(save=True, basemap=False):
    """Plot NYC borough boundaries with clear labeling."""
    print("\n=== Plotting Borough Boundaries ===")

    gdf = gpd.read_file(PROCESSED_DIR / "boroughs.geojson")

    # Create figure
    fig, ax = plt.subplots(figsize=(14, 12))

    # Define colors for each borough
    borough_colors = {
        'Manhattan': '#FF6B6B',
        'Brooklyn': '#4ECDC4',
        'Queens': '#45B7D1',
        'Bronx': '#FFA07A',
        'Staten Island': '#98D8C8'
    }

    # Plot each borough
    for idx, row in gdf.iterrows():
        color = borough_colors.get(row['borough_name'], '#CCCCCC')
        gdf[gdf['borough_name'] == row['borough_name']].plot(
            ax=ax,
            color=color,
            edgecolor='black',
            linewidth=2,
            alpha=0.7
        )

    # Add basemap if requested
    if basemap:
        print("  Adding background map...")
        add_basemap(ax)

    # Add labels with borough code + name
    for idx, row in gdf.iterrows():
        centroid = row.geometry.centroid
        label = f"{row['borough_name']}\n(Boro {row['borough_code']})"
        ax.text(
            centroid.x, centroid.y,
            label,
            fontsize=11,
            fontweight='bold',
            ha='center',
            va='center',
            bbox=dict(boxstyle='round,pad=0.5', facecolor='white', alpha=0.8)
        )

    # Create legend
    legend_elements = [
        mpatches.Patch(facecolor=color, edgecolor='black', label=name)
        for name, color in borough_colors.items()
    ]
    ax.legend(handles=legend_elements, loc='upper right', fontsize=10)

    # Improved title with data source info
    title = 'NYC Borough Boundaries\n'
    title += 'Source: NYC Department of City Planning\n'
    title += f'5 boroughs | Basemap: {"ON" if basemap else "OFF"}'
    ax.set_title(title, fontsize=14, fontweight='bold', pad=20)
    ax.set_xlabel('Longitude')
    ax.set_ylabel('Latitude')
    ax.grid(True, alpha=0.3, linestyle='--')

    plt.tight_layout()

    if save:
        suffix = '_with_basemap' if basemap else ''
        output_file = OUTPUT_DIR / f"geo_boroughs_v2{suffix}.png"
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        print(f"Saved: {output_file}")

    return fig, ax


def plot_nta_improved(save=True, basemap=False, label_major=True):
    """Plot 2020 NTA boundaries with code+name labels."""
    print("\n=== Plotting 2020 NTA Boundaries ===")

    gdf = gpd.read_file(PROCESSED_DIR / "nta.geojson")

    fig, ax = plt.subplots(figsize=(16, 14))

    # Use borough colors if available
    borough_colors = {
        'Manhattan': '#FF6B6B',
        'Brooklyn': '#4ECDC4',
        'Queens': '#45B7D1',
        'Bronx': '#FFA07A',
        'Staten Island': '#98D8C8'
    }

    # Map borough codes to colors (LAST_BoroC: 1=Manhattan, 2=Bronx, 3=Brooklyn, 4=Queens, 5=Staten Island)
    boro_code_to_name = {
        '1': 'Manhattan',
        '2': 'Bronx',
        '3': 'Brooklyn',
        '4': 'Queens',
        '5': 'Staten Island'
    }

    if 'LAST_BoroC' in gdf.columns:
        gdf['borough_name'] = gdf['LAST_BoroC'].astype(str).map(boro_code_to_name)
        gdf['color'] = gdf['borough_name'].map(borough_colors)
    else:
        gdf['color'] = 'lightblue'

    # Plot NTA boundaries
    gdf.plot(
        ax=ax,
        color=gdf['color'],
        edgecolor='white',
        linewidth=0.8,
        alpha=0.6
    )

    # Add basemap if requested
    if basemap:
        print("  Adding background map...")
        add_basemap(ax, zoom='auto')

    # Add labels for major NTAs (to avoid cluttering)
    if label_major and 'nta_code' in gdf.columns:
        # Calculate area to identify major NTAs
        gdf_projected = gdf.to_crs("EPSG:3857")  # Project to meters
        gdf['area_km2'] = gdf_projected.geometry.area / 1_000_000

        # Label top 30 largest NTAs
        major_ntas = gdf.nlargest(30, 'area_km2')

        for idx, row in major_ntas.iterrows():
            centroid = row.geometry.centroid
            label = f"{row['nta_code']}"
            if pd.notna(row.get('NTAAbbrev')):
                label += f"\n{row['NTAAbbrev']}"

            ax.text(
                centroid.x, centroid.y,
                label,
                fontsize=6,
                ha='center',
                va='center',
                bbox=dict(boxstyle='round,pad=0.2', facecolor='white', alpha=0.7, edgecolor='gray'),
                zorder=10
            )

    # Create legend
    if 'borough_name' in gdf.columns:
        legend_elements = [
            mpatches.Patch(facecolor=color, edgecolor='white', label=name)
            for name, color in borough_colors.items()
        ]
        ax.legend(handles=legend_elements, loc='upper right', fontsize=10)

    # Improved title
    title = '2020 Neighborhood Tabulation Areas (NTAs)\n'
    title += 'Source: NYC Dept of City Planning | Vintage: 2020 Census Tracts\n'
    title += f'{len(gdf)} NTAs | Basemap: {"ON" if basemap else "OFF"}'
    if label_major:
        title += ' | Labels: Top 30 by area'
    ax.set_title(title, fontsize=14, fontweight='bold', pad=20)
    ax.set_xlabel('Longitude')
    ax.set_ylabel('Latitude')
    ax.grid(True, alpha=0.3, linestyle='--')

    plt.tight_layout()

    if save:
        suffix = '_with_basemap' if basemap else ''
        output_file = OUTPUT_DIR / f"geo_nta_2020_v2{suffix}.png"
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        print(f"Saved: {output_file}")

    return fig, ax


def plot_puma_improved(save=True, basemap=False, label_all=False):
    """Plot 2020 PUMA boundaries with code+name labels."""
    print("\n=== Plotting 2020 PUMA Boundaries ===")

    gdf = gpd.read_file(PROCESSED_DIR / "puma.geojson")

    fig, ax = plt.subplots(figsize=(16, 14))

    # Use varied colors
    from matplotlib.cm import tab20
    colors = [tab20(i) for i in range(len(gdf))]

    gdf.plot(
        ax=ax,
        color=colors,
        edgecolor='black',
        linewidth=1.2,
        alpha=0.7
    )

    # Add basemap if requested
    if basemap:
        print("  Adding background map...")
        add_basemap(ax)

    # Add PUMA code labels
    for idx, row in gdf.iterrows():
        centroid = row.geometry.centroid

        # Create label with code (always) and name (if label_all)
        label = f"{row['puma_code']}"
        if label_all and pd.notna(row.get('puma_name')):
            # Truncate long names
            name = row['puma_name'][:40] + '...' if len(row['puma_name']) > 40 else row['puma_name']
            label += f"\n{name}"

        ax.text(
            centroid.x, centroid.y,
            label,
            fontsize=7 if label_all else 9,
            ha='center',
            va='center',
            bbox=dict(boxstyle='round,pad=0.3', facecolor='white', alpha=0.8, edgecolor='black'),
            zorder=10
        )

    # Improved title
    title = '2020 Public Use Microdata Areas (PUMAs)\n'
    title += 'Source: US Census Bureau | Vintage: 2020 Census (First used in 2022 ACS)\n'
    title += f'{len(gdf)} PUMAs | Basemap: {"ON" if basemap else "OFF"}'
    ax.set_title(title, fontsize=14, fontweight='bold', pad=20)
    ax.set_xlabel('Longitude')
    ax.set_ylabel('Latitude')
    ax.grid(True, alpha=0.3, linestyle='--')

    plt.tight_layout()

    if save:
        suffix = '_with_basemap' if basemap else ''
        suffix += '_with_names' if label_all else ''
        output_file = OUTPUT_DIR / f"geo_puma_2020_v2{suffix}.png"
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        print(f"Saved: {output_file}")

    return fig, ax


def plot_census_tracts_improved(save=True, basemap=False):
    """Plot 2020 Census Tracts."""
    print("\n=== Plotting 2020 Census Tracts ===")

    gdf = gpd.read_file(PROCESSED_DIR / "census_tracts.geojson")

    fig, ax = plt.subplots(figsize=(16, 14))

    # Color by borough
    borough_colors = {
        'Manhattan': '#FF6B6B',
        'Brooklyn': '#4ECDC4',
        'Queens': '#45B7D1',
        'Bronx': '#FFA07A',
        'Staten Island': '#98D8C8'
    }

    if 'borough_name' in gdf.columns:
        gdf['color'] = gdf['borough_name'].map(borough_colors)
    else:
        gdf['color'] = 'lightcoral'

    gdf.plot(
        ax=ax,
        color=gdf['color'],
        edgecolor='white',
        linewidth=0.2,
        alpha=0.5
    )

    # Add basemap if requested
    if basemap:
        print("  Adding background map...")
        add_basemap(ax)

    # Create legend
    if 'borough_name' in gdf.columns:
        legend_elements = [
            mpatches.Patch(facecolor=color, edgecolor='white', label=name)
            for name, color in borough_colors.items()
        ]
        ax.legend(handles=legend_elements, loc='upper right', fontsize=10)

    # Improved title
    title = '2020 Census Tracts\n'
    title += 'Source: US Census Bureau TIGER/Line via NYC DCP | Vintage: 2020 Census\n'
    title += f'{len(gdf)} tracts (clipped to NYC shoreline) | Basemap: {"ON" if basemap else "OFF"}'
    ax.set_title(title, fontsize=14, fontweight='bold', pad=20)
    ax.set_xlabel('Longitude')
    ax.set_ylabel('Latitude')
    ax.grid(True, alpha=0.3, linestyle='--')

    plt.tight_layout()

    if save:
        suffix = '_with_basemap' if basemap else ''
        output_file = OUTPUT_DIR / f"geo_census_tracts_2020_v2{suffix}.png"
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        print(f"Saved: {output_file}")

    return fig, ax


def create_interactive_map_with_labels(output_file="logs/geo_interactive_v2.html"):
    """
    Create interactive map with tooltips showing code+name for all features.
    """
    try:
        import folium
    except ImportError:
        print("\n⚠ Folium not installed. Install with: pip install folium")
        return

    print("\n=== Creating Interactive Map with Labels ===")

    # Create map centered on NYC
    m = folium.Map(
        location=[40.7128, -74.0060],
        zoom_start=11,
        tiles='CartoDB positron'
    )

    # Add borough boundaries
    print("  Adding boroughs...")
    boroughs = gpd.read_file(PROCESSED_DIR / "boroughs.geojson")
    folium.GeoJson(
        boroughs,
        name='Boroughs',
        style_function=lambda x: {
            'fillColor': '#3388ff',
            'color': 'black',
            'weight': 3,
            'fillOpacity': 0.1
        },
        tooltip=folium.GeoJsonTooltip(
            fields=['borough_name', 'borough_code'],
            aliases=['Borough:', 'Code:'],
            labels=True
        )
    ).add_to(m)

    # Add NTA boundaries with code+name
    print("  Adding 2020 NTAs...")
    nta = gpd.read_file(PROCESSED_DIR / "nta.geojson")

    # Prepare tooltip fields
    tooltip_fields = ['nta_name']
    tooltip_aliases = ['NTA Name:']
    if 'nta_code' in nta.columns:
        tooltip_fields.insert(0, 'nta_code')
        tooltip_aliases.insert(0, 'NTA Code:')
    if 'LAST_BoroN' in nta.columns:
        tooltip_fields.append('LAST_BoroN')
        tooltip_aliases.append('Borough:')

    folium.GeoJson(
        nta,
        name='2020 NTAs',
        style_function=lambda x: {
            'fillColor': '#ff7800',
            'color': 'white',
            'weight': 1,
            'fillOpacity': 0.4
        },
        tooltip=folium.GeoJsonTooltip(
            fields=tooltip_fields,
            aliases=tooltip_aliases,
            labels=True
        )
    ).add_to(m)

    # Add PUMA boundaries with code+name
    print("  Adding 2020 PUMAs...")
    puma = gpd.read_file(PROCESSED_DIR / "puma.geojson")
    folium.GeoJson(
        puma,
        name='2020 PUMAs',
        style_function=lambda x: {
            'fillColor': '#00ff00',
            'color': 'black',
            'weight': 1.5,
            'fillOpacity': 0.3
        },
        tooltip=folium.GeoJsonTooltip(
            fields=['puma_code', 'puma_name', 'puma_geoid'],
            aliases=['PUMA Code:', 'PUMA Name:', 'GEOID:'],
            labels=True
        )
    ).add_to(m)

    # Add layer control
    folium.LayerControl(collapsed=False).add_to(m)

    # Add title
    title_html = '''
    <div style="position: fixed;
                top: 10px; left: 50px; width: 400px; height: 90px;
                background-color:white; border:2px solid grey; z-index:9999;
                font-size:14px; padding: 10px">
    <b>NYC Geographic Boundaries - Interactive Map</b><br>
    <i>Vintage: 2020 Census (NTA, PUMA, Census Tracts)</i><br>
    Click layers to toggle. Hover over areas for codes & names.
    </div>
    '''
    m.get_root().html.add_child(folium.Element(title_html))

    # Save map
    output_path = Path(output_file)
    m.save(str(output_path))
    print(f"Saved: {output_path}")
    print(f"Open in browser: file://{output_path.absolute()}")

    return m


def main():
    parser = argparse.ArgumentParser(
        description="Visualize NYC geographic boundaries - Improved Version",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Create all maps without basemap (faster)
  python visualize_boundaries_v2.py --all

  # Create maps with background basemap
  python visualize_boundaries_v2.py --all --basemap

  # Create specific map types
  python visualize_boundaries_v2.py --nta --puma --basemap

  # Create interactive HTML map with tooltips
  python visualize_boundaries_v2.py --interactive
        """
    )

    parser.add_argument('--all', action='store_true',
                        help='Create all visualizations')
    parser.add_argument('--boroughs', action='store_true',
                        help='Plot borough boundaries')
    parser.add_argument('--nta', action='store_true',
                        help='Plot 2020 NTA boundaries')
    parser.add_argument('--puma', action='store_true',
                        help='Plot 2020 PUMA boundaries')
    parser.add_argument('--census-tracts', action='store_true',
                        help='Plot 2020 census tract boundaries')
    parser.add_argument('--interactive', action='store_true',
                        help='Create interactive HTML map with labels')
    parser.add_argument('--basemap', action='store_true',
                        help='Add background map (slower but better context)')
    parser.add_argument('--show', action='store_true',
                        help='Show plots instead of saving')

    args = parser.parse_args()

    # Create output directory
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    save = not args.show

    # Create requested visualizations
    if args.all or args.boroughs:
        plot_boroughs_improved(save=save, basemap=args.basemap)

    if args.all or args.nta:
        plot_nta_improved(save=save, basemap=args.basemap, label_major=True)

    if args.all or args.puma:
        plot_puma_improved(save=save, basemap=args.basemap, label_all=False)

    if args.all or args.census_tracts:
        plot_census_tracts_improved(save=save, basemap=args.basemap)

    if args.all or args.interactive:
        create_interactive_map_with_labels()

    if args.show:
        plt.show()

    if not any([args.all, args.boroughs, args.nta, args.puma, args.census_tracts, args.interactive]):
        print("No visualizations selected. Use --all or specify individual plots.")
        print("Run with --help for usage information.")
    else:
        print("\n✓ Visualization complete!")
        print(f"  Output directory: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
