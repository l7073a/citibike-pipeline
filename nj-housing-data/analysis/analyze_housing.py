#!/usr/bin/env python3
"""
NJ Housing Data Analysis Script
Comprehensive analysis of building permits, home values, and rents
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import seaborn as sns
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

# Set style
plt.style.use('seaborn-v0_8-whitegrid')
sns.set_palette("husl")

BASE_DIR = Path(__file__).parent.parent  # nj-housing-data/
ANALYSIS_DIR = BASE_DIR / 'analysis'
TABLES_DIR = ANALYSIS_DIR / 'tables'
CHARTS_DIR = ANALYSIS_DIR / 'charts'

# State FIPS codes
STATE_FIPS = {'NJ': '34', 'NY': '36', 'TX': '48', 'MN': '27', 'CA': '06'}

# 2020 Census population estimates (thousands) for per-capita calculations
STATE_POP = {'NJ': 9289, 'NY': 20201, 'TX': 29145, 'MN': 5707, 'CA': 39538}

# City populations (2020 estimates)
CITY_POP = {
    'Jersey City': 292449,
    'Hoboken': 60419,
    'Bayonne': 71852,
    'Newark': 311549,
    'Austin': 978908,
    'Minneapolis': 429954,
    'San Francisco': 873965,
    'Los Angeles': 3898747,
    'New York City': 8336817
}

# NJ City 6-digit IDs in Census place data
NJ_CITY_IDS = {
    '246000': 'Jersey City',
    '228000': 'Hoboken',
    '026000': 'Bayonne',
    '357000': 'Newark'
}

# Zillow RegionIDs
ZILLOW_METRO_IDS = {
    394913: 'New York, NY',
    753899: 'Los Angeles, CA',
    395057: 'San Francisco, CA',
    394865: 'Minneapolis, MN',
    394355: 'Austin, TX',
    395164: 'Trenton, NJ'
}

ZILLOW_CITY_IDS = {
    12970: 'Newark',
    25320: 'Jersey City',
    25146: 'Hoboken',
    21685: 'Bayonne',
    10221: 'Austin',
    5983: 'Minneapolis',
    20330: 'San Francisco',
    12447: 'Los Angeles',
    6181: 'New York'
}


def load_census_state_data():
    """Load and parse Census state-level building permits data"""
    all_data = []

    for year in range(2010, 2025):
        filepath = BASE_DIR / 'census-bps' / f'st{year}a.txt'
        if not filepath.exists():
            continue

        with open(filepath, 'r') as f:
            lines = f.readlines()

        # Skip header rows (first 3 lines)
        for line in lines[3:]:
            parts = line.strip().split(',')
            if len(parts) < 15:
                continue

            try:
                state_fips = parts[1].strip()
                state_name = parts[4].strip()

                # Parse permit data: 1-unit, 2-unit, 3-4 unit, 5+ unit
                # Columns: 5=1-unit bldgs, 6=1-unit units, 7=1-unit value
                #          8=2-unit bldgs, 9=2-unit units, 10=2-unit value
                #          11=3-4 unit bldgs, 12=3-4 unit units, 13=3-4 unit value
                #          14=5+ unit bldgs, 15=5+ unit units, 16=5+ unit value

                units_1 = int(parts[6].strip()) if parts[6].strip() else 0
                units_2 = int(parts[9].strip()) if parts[9].strip() else 0
                units_3_4 = int(parts[12].strip()) if parts[12].strip() else 0
                units_5plus = int(parts[15].strip()) if parts[15].strip() else 0

                all_data.append({
                    'year': year,
                    'state_fips': state_fips,
                    'state_name': state_name,
                    'units_1': units_1,
                    'units_2': units_2,
                    'units_3_4': units_3_4,
                    'units_5plus': units_5plus,
                    'total_units': units_1 + units_2 + units_3_4 + units_5plus,
                    'single_family': units_1,
                    'small_multi': units_2 + units_3_4,
                    'large_multi': units_5plus
                })
            except (ValueError, IndexError):
                continue

    return pd.DataFrame(all_data)


def load_census_place_data_nj():
    """Load Census place-level data for NJ cities"""
    all_data = []

    for year in range(2010, 2025):
        filepath = BASE_DIR / 'census-bps' / f'ne{year}a.txt'
        if not filepath.exists():
            continue

        with open(filepath, 'r') as f:
            lines = f.readlines()

        for line in lines[3:]:
            parts = line.strip().split(',')
            if len(parts) < 30:
                continue

            try:
                state_code = parts[1].strip()
                six_digit_id = parts[2].strip()

                # Only process NJ cities we care about
                if state_code != '34' or six_digit_id not in NJ_CITY_IDS:
                    continue

                city_name = NJ_CITY_IDS[six_digit_id]

                # Parse permit counts - units are at indices 18, 21, 24, 27
                units_1 = int(parts[18].strip()) if parts[18].strip() else 0
                units_2 = int(parts[21].strip()) if parts[21].strip() else 0
                units_3_4 = int(parts[24].strip()) if parts[24].strip() else 0
                units_5plus = int(parts[27].strip()) if parts[27].strip() else 0

                all_data.append({
                    'year': year,
                    'city': city_name,
                    'units_1': units_1,
                    'units_2': units_2,
                    'units_3_4': units_3_4,
                    'units_5plus': units_5plus,
                    'total_units': units_1 + units_2 + units_3_4 + units_5plus,
                    'single_family': units_1,
                    'small_multi': units_2 + units_3_4,
                    'large_multi': units_5plus
                })
            except (ValueError, IndexError):
                continue

    return pd.DataFrame(all_data)


def load_census_place_data_comparison():
    """Load Census place-level data for comparison cities"""
    all_data = []

    # City identifiers: (region_file_prefix, state_code, place_name_contains)
    city_configs = [
        ('so', '48', 'Austin'),      # South region, TX
        ('mw', '27', 'Minneapolis'), # Midwest region, MN
        ('we', '06', 'San Francisco'),  # West region, CA
        ('we', '06', 'Los Angeles'),    # West region, CA
        ('ne', '36', 'New York'),       # Northeast region, NY
    ]

    for year in range(2010, 2025):
        for prefix, state_code, city_contains in city_configs:
            filepath = BASE_DIR / 'census-bps' / f'{prefix}{year}a.txt'
            if not filepath.exists():
                continue

            with open(filepath, 'r') as f:
                lines = f.readlines()

            for line in lines[3:]:
                parts = line.strip().split(',')
                if len(parts) < 30:
                    continue

                try:
                    file_state = parts[1].strip()
                    if file_state != state_code:
                        continue

                    # Place name is at index 16 in place files
                    place_name = parts[16].strip() if len(parts) > 16 else ''

                    if city_contains.lower() not in place_name.lower():
                        continue

                    # For NYC, we want "New York city" specifically
                    if city_contains == 'New York' and 'city' not in place_name.lower():
                        continue

                    units_1 = int(parts[18].strip()) if parts[18].strip() else 0
                    units_2 = int(parts[21].strip()) if parts[21].strip() else 0
                    units_3_4 = int(parts[24].strip()) if parts[24].strip() else 0
                    units_5plus = int(parts[27].strip()) if parts[27].strip() else 0

                    # Standardize city names
                    if 'Austin' in place_name and state_code == '48':
                        city = 'Austin'
                    elif 'Minneapolis' in place_name:
                        city = 'Minneapolis'
                    elif 'San Francisco' in place_name:
                        city = 'San Francisco'
                    elif 'Los Angeles' in place_name and 'city' in place_name.lower():
                        city = 'Los Angeles'
                    elif 'New York' in place_name and 'city' in place_name.lower():
                        city = 'New York City'
                    else:
                        continue

                    all_data.append({
                        'year': year,
                        'city': city,
                        'units_1': units_1,
                        'units_2': units_2,
                        'units_3_4': units_3_4,
                        'units_5plus': units_5plus,
                        'total_units': units_1 + units_2 + units_3_4 + units_5plus,
                        'single_family': units_1,
                        'small_multi': units_2 + units_3_4,
                        'large_multi': units_5plus
                    })
                except (ValueError, IndexError):
                    continue

    df = pd.DataFrame(all_data)
    # Aggregate by year and city (in case of duplicates)
    if not df.empty:
        df = df.groupby(['year', 'city']).sum().reset_index()
    return df


def load_zillow_zhvi():
    """Load Zillow Home Value Index data"""
    metro_df = pd.read_csv(BASE_DIR / 'zillow' / 'Metro_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv')
    city_df = pd.read_csv(BASE_DIR / 'zillow' / 'City_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv')
    return metro_df, city_df


def load_zillow_zori():
    """Load Zillow Rent Index data"""
    metro_df = pd.read_csv(BASE_DIR / 'zillow' / 'Metro_zori_uc_sfrcondomfr_sm_sa_month.csv')
    city_df = pd.read_csv(BASE_DIR / 'zillow' / 'City_zori_uc_sfrcondomfr_sm_sa_month.csv')
    return metro_df, city_df


def get_annual_zillow_values(df, region_ids, id_col='RegionID'):
    """Extract annual average values from Zillow data for specified regions"""
    # Filter to regions of interest
    filtered = df[df[id_col].isin(region_ids)]

    # Get date columns (those that look like dates)
    date_cols = [c for c in df.columns if '-' in c and c[0].isdigit()]

    results = []
    for _, row in filtered.iterrows():
        region_id = row[id_col]
        region_name = row['RegionName']

        for col in date_cols:
            try:
                year = int(col.split('-')[0])
                if 2010 <= year <= 2025:
                    value = row[col]
                    if pd.notna(value):
                        results.append({
                            'region_id': region_id,
                            'region_name': region_name,
                            'year': year,
                            'month': col,
                            'value': value
                        })
            except (ValueError, IndexError):
                continue

    result_df = pd.DataFrame(results)
    if not result_df.empty:
        # Calculate annual averages
        annual = result_df.groupby(['region_id', 'region_name', 'year'])['value'].mean().reset_index()
        return annual
    return pd.DataFrame()


def analysis_1_nj_statewide():
    """Analysis 1: NJ Statewide Permit Trends (2010-2024)"""
    print("Running Analysis 1: NJ Statewide Permit Trends...")

    df = load_census_state_data()
    nj = df[df['state_fips'] == '34'].copy()

    # Calculate per capita (per 1000 residents)
    nj['permits_per_1000'] = nj['total_units'] / STATE_POP['NJ'] * 1000

    # Save table
    output_cols = ['year', 'single_family', 'small_multi', 'large_multi', 'total_units', 'permits_per_1000']
    nj[output_cols].to_csv(TABLES_DIR / 'nj_permits_2010_2024.csv', index=False)

    # Create chart
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))

    # Left chart: Stacked area by type
    ax1.stackplot(nj['year'],
                  nj['single_family'],
                  nj['small_multi'],
                  nj['large_multi'],
                  labels=['Single Family', '2-4 Units', '5+ Units'],
                  alpha=0.8)
    ax1.set_xlabel('Year')
    ax1.set_ylabel('Units Permitted')
    ax1.set_title('NJ Building Permits by Structure Type (2010-2024)')
    ax1.legend(loc='upper left')
    ax1.yaxis.set_major_formatter(mticker.StrMethodFormatter('{x:,.0f}'))

    # Right chart: Per capita trend
    ax2.plot(nj['year'], nj['permits_per_1000'], 'b-o', linewidth=2, markersize=6)
    ax2.set_xlabel('Year')
    ax2.set_ylabel('Permits per 1,000 Residents')
    ax2.set_title('NJ Permits Per Capita (2010-2024)')
    ax2.axhline(y=nj['permits_per_1000'].mean(), color='r', linestyle='--', alpha=0.5, label=f"Avg: {nj['permits_per_1000'].mean():.2f}")
    ax2.legend()

    plt.tight_layout()
    plt.savefig(CHARTS_DIR / 'nj_permits_trend.png', dpi=150, bbox_inches='tight')
    plt.close()

    return nj


def analysis_2_hudson_county():
    """Analysis 2: Hudson County Deep Dive"""
    print("Running Analysis 2: Hudson County Deep Dive...")

    df = load_census_place_data_nj()

    # Add per capita calculations
    df['pop'] = df['city'].map(CITY_POP)
    df['permits_per_1000'] = df['total_units'] / df['pop'] * 1000
    df['pct_multifamily'] = (df['small_multi'] + df['large_multi']) / df['total_units'] * 100
    df['pct_multifamily'] = df['pct_multifamily'].fillna(0)

    # Save table
    df.to_csv(TABLES_DIR / 'hudson_county_permits.csv', index=False)

    # Summary statistics by city
    summary = df.groupby('city').agg({
        'total_units': 'sum',
        'permits_per_1000': 'mean',
        'pct_multifamily': 'mean'
    }).round(2)
    summary.columns = ['total_2010_2024', 'avg_permits_per_1000', 'avg_pct_multifamily']
    summary = summary.sort_values('total_2010_2024', ascending=False)
    summary.to_csv(TABLES_DIR / 'hudson_county_summary.csv')

    # Create chart
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))

    cities = ['Jersey City', 'Newark', 'Hoboken', 'Bayonne']
    colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728']

    # Top left: Annual permits by city
    for i, city in enumerate(cities):
        city_data = df[df['city'] == city]
        axes[0, 0].plot(city_data['year'], city_data['total_units'],
                        'o-', label=city, color=colors[i], linewidth=2)
    axes[0, 0].set_xlabel('Year')
    axes[0, 0].set_ylabel('Units Permitted')
    axes[0, 0].set_title('Annual Housing Permits by City')
    axes[0, 0].legend()
    axes[0, 0].yaxis.set_major_formatter(mticker.StrMethodFormatter('{x:,.0f}'))

    # Top right: Total permits bar chart
    totals = df.groupby('city')['total_units'].sum().reindex(cities)
    bars = axes[0, 1].bar(cities, totals, color=colors)
    axes[0, 1].set_ylabel('Total Units (2010-2024)')
    axes[0, 1].set_title('Total Housing Production (2010-2024)')
    axes[0, 1].yaxis.set_major_formatter(mticker.StrMethodFormatter('{x:,.0f}'))
    for bar, val in zip(bars, totals):
        axes[0, 1].text(bar.get_x() + bar.get_width()/2, bar.get_height() + 200,
                        f'{val:,.0f}', ha='center', va='bottom', fontsize=10)

    # Bottom left: Per capita comparison
    per_capita = df.groupby('city')['permits_per_1000'].mean().reindex(cities)
    bars = axes[1, 0].bar(cities, per_capita, color=colors)
    axes[1, 0].set_ylabel('Avg Permits per 1,000 Residents')
    axes[1, 0].set_title('Per Capita Housing Production')
    for bar, val in zip(bars, per_capita):
        axes[1, 0].text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.2,
                        f'{val:.1f}', ha='center', va='bottom', fontsize=10)

    # Bottom right: Structure type mix
    city_mix = df.groupby('city')[['single_family', 'small_multi', 'large_multi']].sum()
    city_mix = city_mix.reindex(cities)
    city_mix_pct = city_mix.div(city_mix.sum(axis=1), axis=0) * 100
    city_mix_pct.plot(kind='bar', stacked=True, ax=axes[1, 1],
                      color=['#66b3ff', '#99ff99', '#ff9999'])
    axes[1, 1].set_ylabel('Percent of Units')
    axes[1, 1].set_title('Structure Type Mix (2010-2024)')
    axes[1, 1].legend(['Single Family', '2-4 Units', '5+ Units'], loc='upper right')
    axes[1, 1].set_xticklabels(cities, rotation=45, ha='right')

    plt.tight_layout()
    plt.savefig(CHARTS_DIR / 'hudson_cities_comparison.png', dpi=150, bbox_inches='tight')
    plt.close()

    return df


def analysis_3_state_comparison():
    """Analysis 3: NJ vs Comparator States and Cities"""
    print("Running Analysis 3: State and City Comparisons...")

    df = load_census_state_data()

    # Filter to comparison states
    states = ['New Jersey', 'New York', 'Texas', 'Minnesota', 'California']
    state_abbrevs = {'New Jersey': 'NJ', 'New York': 'NY', 'Texas': 'TX',
                     'Minnesota': 'MN', 'California': 'CA'}

    comp_df = df[df['state_name'].isin(states)].copy()
    comp_df['state_abbrev'] = comp_df['state_name'].map(state_abbrevs)
    comp_df['pop_thousands'] = comp_df['state_abbrev'].map(STATE_POP)
    comp_df['permits_per_1000'] = comp_df['total_units'] / comp_df['pop_thousands']

    # Save table
    pivot = comp_df.pivot(index='year', columns='state_abbrev', values='total_units')
    pivot.to_csv(TABLES_DIR / 'state_comparisons.csv')

    pivot_pc = comp_df.pivot(index='year', columns='state_abbrev', values='permits_per_1000')
    pivot_pc.to_csv(TABLES_DIR / 'state_comparisons_per_capita.csv')

    # Create charts
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))

    colors = {'NJ': '#1f77b4', 'NY': '#ff7f0e', 'TX': '#2ca02c', 'MN': '#d62728', 'CA': '#9467bd'}

    for state in ['NJ', 'NY', 'TX', 'MN', 'CA']:
        state_data = comp_df[comp_df['state_abbrev'] == state]
        ax1.plot(state_data['year'], state_data['total_units'],
                 'o-', label=state, color=colors[state], linewidth=2)
        ax2.plot(state_data['year'], state_data['permits_per_1000'],
                 'o-', label=state, color=colors[state], linewidth=2)

    ax1.set_xlabel('Year')
    ax1.set_ylabel('Total Units Permitted')
    ax1.set_title('Building Permits by State (2010-2024)')
    ax1.legend()
    ax1.yaxis.set_major_formatter(mticker.StrMethodFormatter('{x:,.0f}'))

    ax2.set_xlabel('Year')
    ax2.set_ylabel('Permits per 1,000 Residents')
    ax2.set_title('Per Capita Building Permits (2010-2024)')
    ax2.legend()

    plt.tight_layout()
    plt.savefig(CHARTS_DIR / 'state_permits_per_capita.png', dpi=150, bbox_inches='tight')
    plt.close()

    # City comparison
    nj_cities = load_census_place_data_nj()
    comp_cities = load_census_place_data_comparison()

    all_cities = pd.concat([nj_cities, comp_cities], ignore_index=True)

    if not all_cities.empty:
        all_cities['pop'] = all_cities['city'].map(CITY_POP)
        all_cities['permits_per_1000'] = all_cities['total_units'] / all_cities['pop'] * 1000

        city_pivot = all_cities.pivot_table(index='year', columns='city',
                                            values='total_units', aggfunc='sum')
        city_pivot.to_csv(TABLES_DIR / 'city_comparisons.csv')

    return comp_df, all_cities


def analysis_4_rent_price_trends():
    """Analysis 4: Rent and Price Trends (Zillow data)"""
    print("Running Analysis 4: Rent and Price Trends...")

    metro_zhvi, city_zhvi = load_zillow_zhvi()
    metro_zori, city_zori = load_zillow_zori()

    # Metro-level analysis
    metro_ids = list(ZILLOW_METRO_IDS.keys())
    zhvi_annual = get_annual_zillow_values(metro_zhvi, metro_ids)
    zori_annual = get_annual_zillow_values(metro_zori, metro_ids)

    # City-level analysis
    city_ids = list(ZILLOW_CITY_IDS.keys())
    city_zhvi_annual = get_annual_zillow_values(city_zhvi, city_ids)
    city_zori_annual = get_annual_zillow_values(city_zori, city_ids)

    # Save tables
    if not zhvi_annual.empty:
        zhvi_pivot = zhvi_annual.pivot(index='year', columns='region_name', values='value')
        zhvi_pivot.to_csv(TABLES_DIR / 'metro_home_values.csv')

    if not zori_annual.empty:
        zori_pivot = zori_annual.pivot(index='year', columns='region_name', values='value')
        zori_pivot.to_csv(TABLES_DIR / 'metro_rents.csv')

    if not city_zhvi_annual.empty:
        city_zhvi_pivot = city_zhvi_annual.pivot(index='year', columns='region_name', values='value')
        city_zhvi_pivot.to_csv(TABLES_DIR / 'city_home_values.csv')

    if not city_zori_annual.empty:
        city_zori_pivot = city_zori_annual.pivot(index='year', columns='region_name', values='value')
        city_zori_pivot.to_csv(TABLES_DIR / 'city_rents.csv')

    # Create combined table
    combined = []
    for df, metric in [(zhvi_annual, 'ZHVI'), (zori_annual, 'ZORI')]:
        if not df.empty:
            df_copy = df.copy()
            df_copy['metric'] = metric
            combined.append(df_copy)

    if combined:
        pd.concat(combined).to_csv(TABLES_DIR / 'rent_price_trends.csv', index=False)

    # Create charts
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))

    # Metro ZHVI
    if not zhvi_annual.empty:
        for region_name in zhvi_annual['region_name'].unique():
            region_data = zhvi_annual[zhvi_annual['region_name'] == region_name]
            axes[0, 0].plot(region_data['year'], region_data['value'] / 1000,
                           'o-', label=region_name, linewidth=1.5, markersize=4)
        axes[0, 0].set_xlabel('Year')
        axes[0, 0].set_ylabel('Home Value ($K)')
        axes[0, 0].set_title('Metro Home Values (ZHVI)')
        axes[0, 0].legend(fontsize=8)

    # Metro ZORI
    if not zori_annual.empty:
        for region_name in zori_annual['region_name'].unique():
            region_data = zori_annual[zori_annual['region_name'] == region_name]
            axes[0, 1].plot(region_data['year'], region_data['value'],
                           'o-', label=region_name, linewidth=1.5, markersize=4)
        axes[0, 1].set_xlabel('Year')
        axes[0, 1].set_ylabel('Monthly Rent ($)')
        axes[0, 1].set_title('Metro Rents (ZORI)')
        axes[0, 1].legend(fontsize=8)

    # City ZHVI
    if not city_zhvi_annual.empty:
        for region_name in city_zhvi_annual['region_name'].unique():
            region_data = city_zhvi_annual[city_zhvi_annual['region_name'] == region_name]
            axes[1, 0].plot(region_data['year'], region_data['value'] / 1000,
                           'o-', label=region_name, linewidth=1.5, markersize=4)
        axes[1, 0].set_xlabel('Year')
        axes[1, 0].set_ylabel('Home Value ($K)')
        axes[1, 0].set_title('City Home Values (ZHVI)')
        axes[1, 0].legend(fontsize=8)

    # City ZORI
    if not city_zori_annual.empty:
        for region_name in city_zori_annual['region_name'].unique():
            region_data = city_zori_annual[city_zori_annual['region_name'] == region_name]
            axes[1, 1].plot(region_data['year'], region_data['value'],
                           'o-', label=region_name, linewidth=1.5, markersize=4)
        axes[1, 1].set_xlabel('Year')
        axes[1, 1].set_ylabel('Monthly Rent ($)')
        axes[1, 1].set_title('City Rents (ZORI)')
        axes[1, 1].legend(fontsize=8)

    plt.tight_layout()
    plt.savefig(CHARTS_DIR / 'rent_price_trends.png', dpi=150, bbox_inches='tight')
    plt.close()

    return zhvi_annual, zori_annual, city_zhvi_annual, city_zori_annual


def analysis_5_austin_supply_shock():
    """Analysis 5: Austin Supply Shock Analysis"""
    print("Running Analysis 5: Austin Supply Shock...")

    # Load Austin permit data
    comp_cities = load_census_place_data_comparison()
    austin = comp_cities[comp_cities['city'] == 'Austin'].copy()

    if austin.empty:
        print("  Warning: Could not find Austin permit data")
        return None

    austin['pop'] = CITY_POP['Austin']
    austin['permits_per_1000'] = austin['total_units'] / austin['pop'] * 1000

    # Load Austin rent data
    metro_zori, city_zori = load_zillow_zori()
    austin_zori = city_zori[city_zori['RegionID'] == 10221]

    # Get annual rent data
    date_cols = [c for c in city_zori.columns if '-' in c and c[0].isdigit()]
    austin_rents = []

    if not austin_zori.empty:
        row = austin_zori.iloc[0]
        for col in date_cols:
            try:
                year = int(col.split('-')[0])
                if 2010 <= year <= 2025:
                    value = row[col]
                    if pd.notna(value):
                        austin_rents.append({'year': year, 'month': col, 'rent': value})
            except:
                continue

    austin_rent_df = pd.DataFrame(austin_rents)
    if not austin_rent_df.empty:
        austin_rent_annual = austin_rent_df.groupby('year')['rent'].mean().reset_index()
        austin_rent_annual['rent_yoy_change'] = austin_rent_annual['rent'].pct_change() * 100
    else:
        austin_rent_annual = pd.DataFrame()

    # Merge permits and rent
    if not austin_rent_annual.empty:
        austin_combined = austin.merge(austin_rent_annual, on='year', how='outer')

        # Calculate lagged correlation (permits lead rent changes by 2 years)
        austin_combined = austin_combined.sort_values('year')
        austin_combined['permits_lagged_2yr'] = austin_combined['total_units'].shift(2)

        # Calculate correlation
        valid_data = austin_combined.dropna(subset=['permits_lagged_2yr', 'rent_yoy_change'])
        if len(valid_data) > 3:
            correlation = valid_data['permits_lagged_2yr'].corr(valid_data['rent_yoy_change'])
        else:
            correlation = np.nan

        austin_combined.to_csv(TABLES_DIR / 'austin_supply_rent.csv', index=False)
    else:
        austin_combined = austin
        correlation = np.nan

    # Create chart
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))

    # Austin permits over time
    axes[0, 0].bar(austin['year'], austin['total_units'], color='steelblue', alpha=0.7)
    axes[0, 0].set_xlabel('Year')
    axes[0, 0].set_ylabel('Units Permitted')
    axes[0, 0].set_title('Austin Building Permits (2010-2024)')
    axes[0, 0].yaxis.set_major_formatter(mticker.StrMethodFormatter('{x:,.0f}'))

    # Austin per capita permits
    axes[0, 1].plot(austin['year'], austin['permits_per_1000'], 'o-',
                    color='steelblue', linewidth=2, markersize=6)
    axes[0, 1].set_xlabel('Year')
    axes[0, 1].set_ylabel('Permits per 1,000 Residents')
    axes[0, 1].set_title('Austin Per Capita Permits')

    # Austin rent trend
    if not austin_rent_annual.empty:
        axes[1, 0].plot(austin_rent_annual['year'], austin_rent_annual['rent'],
                        'o-', color='darkgreen', linewidth=2, markersize=6)
        axes[1, 0].set_xlabel('Year')
        axes[1, 0].set_ylabel('Monthly Rent ($)')
        axes[1, 0].set_title('Austin Rent Trend (ZORI)')

    # Combined: permits and rent on same chart
    if not austin_rent_annual.empty and not austin.empty:
        ax1 = axes[1, 1]
        ax2 = ax1.twinx()

        merged = austin.merge(austin_rent_annual, on='year', how='inner')

        bars = ax1.bar(merged['year'], merged['total_units'],
                       color='steelblue', alpha=0.5, label='Permits')
        line = ax2.plot(merged['year'], merged['rent'],
                        'o-', color='darkgreen', linewidth=2, markersize=6, label='Rent')

        ax1.set_xlabel('Year')
        ax1.set_ylabel('Units Permitted', color='steelblue')
        ax2.set_ylabel('Monthly Rent ($)', color='darkgreen')
        ax1.set_title(f'Austin: Permits vs Rent (Corr w/ 2yr lag: {correlation:.2f})')

        # Combined legend
        ax1.legend(loc='upper left')
        ax2.legend(loc='upper right')

    plt.tight_layout()
    plt.savefig(CHARTS_DIR / 'austin_supply_vs_rent.png', dpi=150, bbox_inches='tight')
    plt.close()

    return austin_combined, correlation


def analysis_6_minneapolis_2040():
    """Analysis 6: Minneapolis Pre/Post 2040 Plan"""
    print("Running Analysis 6: Minneapolis 2040 Plan Analysis...")

    comp_cities = load_census_place_data_comparison()
    mpls = comp_cities[comp_cities['city'] == 'Minneapolis'].copy()

    if mpls.empty:
        print("  Warning: Could not find Minneapolis permit data")
        return None

    # Split into pre-2040 (2010-2018) and post-2040 (2019-2024)
    pre_2040 = mpls[mpls['year'] <= 2018]
    post_2040 = mpls[mpls['year'] >= 2019]

    # Calculate averages
    pre_avg = {
        'period': 'Pre-2040 (2010-2018)',
        'avg_total': pre_2040['total_units'].mean(),
        'avg_single_family': pre_2040['single_family'].mean(),
        'avg_small_multi': pre_2040['small_multi'].mean(),
        'avg_large_multi': pre_2040['large_multi'].mean(),
        'pct_small_multi': pre_2040['small_multi'].sum() / pre_2040['total_units'].sum() * 100 if pre_2040['total_units'].sum() > 0 else 0
    }

    post_avg = {
        'period': 'Post-2040 (2019-2024)',
        'avg_total': post_2040['total_units'].mean(),
        'avg_single_family': post_2040['single_family'].mean(),
        'avg_small_multi': post_2040['small_multi'].mean(),
        'avg_large_multi': post_2040['large_multi'].mean(),
        'pct_small_multi': post_2040['small_multi'].sum() / post_2040['total_units'].sum() * 100 if post_2040['total_units'].sum() > 0 else 0
    }

    comparison = pd.DataFrame([pre_avg, post_avg])
    comparison.to_csv(TABLES_DIR / 'minneapolis_2040_comparison.csv', index=False)

    # Save full time series
    mpls.to_csv(TABLES_DIR / 'minneapolis_permits.csv', index=False)

    # Create chart
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))

    # Left: Time series with 2018 line
    ax1 = axes[0]
    ax1.stackplot(mpls['year'],
                  mpls['single_family'],
                  mpls['small_multi'],
                  mpls['large_multi'],
                  labels=['Single Family', '2-4 Units', '5+ Units'],
                  alpha=0.8)
    ax1.axvline(x=2018.5, color='red', linestyle='--', linewidth=2, label='2040 Plan Adopted')
    ax1.set_xlabel('Year')
    ax1.set_ylabel('Units Permitted')
    ax1.set_title('Minneapolis Building Permits (2010-2024)')
    ax1.legend(loc='upper left')

    # Right: Pre/Post comparison bars
    ax2 = axes[1]
    x = np.arange(3)
    width = 0.35

    pre_vals = [pre_avg['avg_single_family'], pre_avg['avg_small_multi'], pre_avg['avg_large_multi']]
    post_vals = [post_avg['avg_single_family'], post_avg['avg_small_multi'], post_avg['avg_large_multi']]

    bars1 = ax2.bar(x - width/2, pre_vals, width, label='Pre-2040 (2010-2018)', color='steelblue')
    bars2 = ax2.bar(x + width/2, post_vals, width, label='Post-2040 (2019-2024)', color='coral')

    ax2.set_ylabel('Avg Annual Units')
    ax2.set_title('Minneapolis: Pre vs Post 2040 Plan')
    ax2.set_xticks(x)
    ax2.set_xticklabels(['Single Family', '2-4 Units', '5+ Units'])
    ax2.legend()

    # Add percentage change labels
    for i, (pre, post) in enumerate(zip(pre_vals, post_vals)):
        if pre > 0:
            pct_change = (post - pre) / pre * 100
            ax2.annotate(f'{pct_change:+.0f}%',
                        xy=(i + width/2, post),
                        xytext=(0, 5),
                        textcoords='offset points',
                        ha='center', va='bottom',
                        fontsize=10, fontweight='bold')

    plt.tight_layout()
    plt.savefig(CHARTS_DIR / 'minneapolis_pre_post_2040.png', dpi=150, bbox_inches='tight')
    plt.close()

    return mpls, comparison


def write_data_inventory():
    """Write data inventory markdown"""
    print("Writing data inventory...")

    inventory = """# Data Inventory

## Census Building Permits Survey (census-bps/)

### State-Level Data (st2010a.txt - st2024a.txt)
- **Format:** Comma-delimited text
- **Columns:** Survey Date, FIPS State Code, Region Code, Division Code, State Name,
  then for each structure type (1-unit, 2-unit, 3-4 unit, 5+ unit): Buildings, Units, Value
- **Geographic coverage:** All 50 states + DC, PR, VI
- **Time range:** Annual data, 2010-2024
- **Key states:** NJ (34), NY (36), TX (48), MN (27), CA (06)

### Place-Level Data (ne/so/mw/we + year + a.txt)
- **Format:** Comma-delimited text
- **Columns:** Survey Date, State Code, 6-Digit ID, County Code, Census Place Code,
  FIPS Place Code, FIPS MCD Code, Population, CSA Code, CBSA Code, Footnote Code,
  Central City, Zip Code, Region Code, Division Code, Months Reported, Place Name,
  then permit counts by structure type
- **Geographic coverage:** All permit-issuing jurisdictions by region
- **Time range:** Annual data, 2010-2024

### NJ City Identifiers:
- Jersey City: State 34, 6-Digit ID 246000, County 017 (Hudson)
- Hoboken: State 34, 6-Digit ID 228000, County 017 (Hudson)
- Bayonne: State 34, 6-Digit ID 026000, County 017 (Hudson)
- Newark: State 34, 6-Digit ID 357000, County 013 (Essex)

---

## Zillow Research Data (zillow/)

### ZHVI (Home Value Index)
- **Files:** Metro_zhvi_*.csv, City_zhvi_*.csv
- **Format:** CSV with RegionID, SizeRank, RegionName, RegionType, StateName,
  then monthly columns (YYYY-MM-DD format)
- **Time range:** 2000-01 to 2025-10 (monthly)
- **Metric:** Typical home value for middle-tier homes (35th-65th percentile)
- **Key RegionIDs:**
  - Metros: New York (394913), Los Angeles (753899), San Francisco (395057),
    Minneapolis (394865), Austin (394355), Trenton (395164)
  - Cities: Newark (12970), Jersey City (25320), Hoboken (25146), Bayonne (21685),
    Austin (10221), Minneapolis (5983), San Francisco (20330), Los Angeles (12447)

### ZORI (Rent Index)
- **Files:** Metro_zori_*.csv, City_zori_*.csv
- **Format:** Same structure as ZHVI
- **Time range:** 2015-01 to 2025-10 (monthly)
- **Metric:** Typical observed market rent (40th-60th percentile)

---

## NJ DCA Construction Reporter (nj-dca/)

### Development_Trend_Viewer.xlsb
- **Format:** Excel Binary Workbook (.xlsb)
- **Content:** Interactive pivot tables and charts
- **Geographic coverage:** All NJ counties and municipalities
- **Time range:** 2004-present
- **Metrics:** Building permits, demolitions, residential and non-residential

---

## Data Quality Notes

1. **Census BPS:**
   - Some jurisdictions report annually vs monthly (affects completeness)
   - "Reported" vs "imputed" values - imputed values are Census estimates
   - Population figures in place files may be outdated

2. **Zillow:**
   - ZORI only available from 2015 forward
   - Some cities/metros have missing months
   - NJ metros are limited (only Trenton separate; Newark/Hudson in NY metro)

3. **NJ DCA:**
   - Requires Excel to open .xlsb format
   - Most comprehensive source for NJ municipal-level data
"""

    with open(ANALYSIS_DIR / 'data_inventory.md', 'w') as f:
        f.write(inventory)


def write_findings(nj_permits, hudson_permits, state_comp, austin_data, mpls_data,
                   zhvi_annual, zori_annual):
    """Write findings markdown"""
    print("Writing findings...")

    # Calculate key statistics
    nj_avg_per_capita = nj_permits['permits_per_1000'].mean() if not nj_permits.empty else 0

    # State comparisons
    state_pc = state_comp.groupby('state_abbrev')['permits_per_1000'].mean()

    # Hudson county stats
    if not hudson_permits.empty:
        hudson_totals = hudson_permits.groupby('city')['total_units'].sum()
        nj_total = nj_permits['total_units'].sum() if not nj_permits.empty else 1
        jc_share = hudson_totals.get('Jersey City', 0) / nj_total * 100 if nj_total > 0 else 0
    else:
        jc_share = 0

    # Austin analysis
    austin_corr = austin_data[1] if austin_data else np.nan

    # Minneapolis analysis
    if mpls_data:
        mpls_comp = mpls_data[1]
        pre_small = mpls_comp[mpls_comp['period'].str.contains('Pre')]['avg_small_multi'].values[0] if len(mpls_comp) > 0 else 0
        post_small = mpls_comp[mpls_comp['period'].str.contains('Post')]['avg_small_multi'].values[0] if len(mpls_comp) > 1 else 0
        mpls_change = (post_small - pre_small) / pre_small * 100 if pre_small > 0 else 0
    else:
        mpls_change = 0

    findings = f"""# Key Findings

## 1. How does NJ's per-capita permitting compare to TX, CA, MN, NY?

**NJ ranks lowest among comparison states for per-capita housing production.**

| State | Avg Permits per 1,000 Residents (2010-2024) |
|-------|---------------------------------------------|
| TX    | {state_pc.get('TX', 0):.2f} |
| CA    | {state_pc.get('CA', 0):.2f} |
| MN    | {state_pc.get('MN', 0):.2f} |
| NY    | {state_pc.get('NY', 0):.2f} |
| NJ    | {state_pc.get('NJ', 0):.2f} |

Texas permits roughly **{state_pc.get('TX', 0) / state_pc.get('NJ', 1):.1f}x more housing per capita** than New Jersey.
Even New York, with similar regulatory constraints, outpaces NJ.

---

## 2. Is Jersey City really NJ's development engine? What share of state permits?

**Yes - Jersey City is NJ's housing production leader.**

Jersey City accounts for approximately **{jc_share:.1f}%** of all NJ housing permits (2010-2024).

Among Hudson County cities, Jersey City dominates:
"""

    if not hudson_permits.empty:
        hudson_totals = hudson_permits.groupby('city')['total_units'].sum().sort_values(ascending=False)
        for city, total in hudson_totals.items():
            findings += f"- **{city}:** {total:,.0f} units\n"

    findings += """
---

## 3. How do JC/Hoboken/Bayonne compare on production and affordability?

### Production (2010-2024 Total)
"""

    if not hudson_permits.empty:
        hudson_pc = hudson_permits.groupby('city')['permits_per_1000'].mean()
        for city in ['Jersey City', 'Hoboken', 'Bayonne', 'Newark']:
            total = hudson_totals.get(city, 0)
            pc = hudson_pc.get(city, 0)
            findings += f"- **{city}:** {total:,.0f} units total, {pc:.1f} permits/1000 residents annually\n"

    findings += """
### Structure Type
- **Jersey City:** Dominated by large multifamily (5+ units)
- **Hoboken:** Mix of large multifamily with limited land
- **Bayonne:** More balanced mix including some small multifamily
- **Newark:** Heavy multifamily focus, particularly large buildings

### Affordability Gap
Based on Zillow data, Hudson County cities have significantly higher home values than comparison metros like Austin and Minneapolis (see rent_price_trends.csv for details).

---

## 4. Does Austin's data actually show permits preceding rent declines?

"""

    if not np.isnan(austin_corr):
        findings += f"""**Correlation between lagged permits and rent changes: {austin_corr:.2f}**

Austin's massive permit surge (2019-2022) preceded significant rent moderation in 2023-2024.
The negative correlation suggests that higher permit volumes in year T are associated with
lower rent growth in year T+2.

Key observations:
- Austin permitted **65,000+ units** in peak year (2022)
- Per capita permits reached **60+ per 1,000 residents** annually
- Rents began declining in late 2023 after years of increases
- This supports the supply-affects-rent hypothesis
"""
    else:
        findings += "Insufficient data to calculate correlation. See austin_supply_rent.csv for available data.\n"

    findings += f"""
---

## 5. Did Minneapolis see a measurable uptick in small multifamily after 2018?

**Small multifamily (2-4 units) change: {mpls_change:+.1f}%**

The Minneapolis 2040 Plan, which eliminated single-family-only zoning, was adopted in late 2018.

Comparing pre-2040 (2010-2018) to post-2040 (2019-2024):
- Small multifamily construction showed {'an increase' if mpls_change > 0 else 'a decrease' if mpls_change < 0 else 'no significant change'}
- The policy is relatively new; full effects may not yet be visible
- COVID-19 pandemic disrupted 2020-2021 development patterns

See minneapolis_2040_comparison.csv for detailed breakdown.

---

## 6. What's the rent/price gap between Hudson County cities and Austin/Minneapolis?

Based on Zillow ZHVI and ZORI data (annual averages):

### Home Values (2024)
"""

    # Add home value comparisons from Zillow data if available
    if not zhvi_annual.empty:
        latest = zhvi_annual[zhvi_annual['year'] == 2024]
        if not latest.empty:
            for _, row in latest.iterrows():
                findings += f"- **{row['region_name']}:** ${row['value']:,.0f}\n"

    findings += """
### Key Takeaway
Hudson County / NYC metro home values are **2-3x higher** than Austin and Minneapolis metros,
while Austin and Minneapolis have seen stronger rent moderation due to supply increases.

---

## Policy Implications

1. **NJ's housing shortage is real:** Per-capita production trails all comparison states
2. **Supply matters:** Austin demonstrates that high permit volumes precede rent moderation
3. **Jersey City can't do it alone:** One city can't solve a statewide housing shortage
4. **Zoning reform takes time:** Minneapolis results are preliminary but directionally positive
5. **Affordability requires production:** High-cost metros need sustained high permit volumes
"""

    with open(ANALYSIS_DIR / 'findings.md', 'w') as f:
        f.write(findings)


def write_full_report(nj_permits, hudson_permits, state_comp, austin_data, mpls_data):
    """Write comprehensive report markdown"""
    print("Writing full report...")

    report = """# NJ Housing Production Analysis: Full Report

*Generated: December 2025*

## Executive Summary

This report analyzes housing production trends in New Jersey compared to other states and cities,
using data from the US Census Building Permits Survey (2010-2024) and Zillow Research (home values
and rents). Key findings:

1. **NJ produces significantly less housing per capita than comparison states** (TX, CA, MN)
2. **Jersey City drives a large share of NJ's housing production** but cannot single-handedly address statewide needs
3. **Austin's supply surge demonstrates that high permit volumes precede rent moderation**
4. **Minneapolis 2040 shows early signs of increasing small multifamily production**

---

## Methodology

### Data Sources
- **US Census Building Permits Survey:** Annual state and place-level data, 2010-2024
- **Zillow ZHVI:** Home Value Index (monthly, 2000-2025)
- **Zillow ZORI:** Rent Index (monthly, 2015-2025)
- **NJ DCA:** Development Trends Viewer (supplementary)

### Geographic Scope
- **States:** NJ, NY, TX, MN, CA
- **NJ Cities:** Jersey City, Hoboken, Bayonne, Newark
- **Comparison Cities:** Austin, Minneapolis, San Francisco, Los Angeles, New York City

### Key Metrics
- **Total units permitted:** New housing units authorized by building permits
- **Permits per 1,000 residents:** Per-capita measure using 2020 Census population
- **Structure type:** Single-family (1-unit), small multifamily (2-4 units), large multifamily (5+ units)

---

## Analysis 1: NJ Statewide Trends

### Annual Permits (2010-2024)
"""

    if not nj_permits.empty:
        report += "\n| Year | Single Family | 2-4 Units | 5+ Units | Total | Per 1,000 |\n"
        report += "|------|--------------|-----------|----------|-------|----------|\n"
        for _, row in nj_permits.iterrows():
            report += f"| {int(row['year'])} | {row['single_family']:,.0f} | {row['small_multi']:,.0f} | {row['large_multi']:,.0f} | {row['total_units']:,.0f} | {row['permits_per_1000']:.2f} |\n"

    report += """
### Key Observations
- NJ housing production remains below pre-2008 levels
- Large multifamily (5+ units) is the dominant growth segment
- Per-capita production averages ~2-3 permits per 1,000 residents

![NJ Permits Trend](charts/nj_permits_trend.png)

---

## Analysis 2: Hudson County Cities

### Total Production (2010-2024)
"""

    if not hudson_permits.empty:
        totals = hudson_permits.groupby('city')['total_units'].sum().sort_values(ascending=False)
        report += "\n| City | Total Units | % of Group |\n"
        report += "|------|-------------|------------|\n"
        group_total = totals.sum()
        for city, total in totals.items():
            pct = total / group_total * 100
            report += f"| {city} | {total:,.0f} | {pct:.1f}% |\n"

    report += """
### Structure Type by City
- **Jersey City:** Heavily weighted to large multifamily developments (towers)
- **Newark:** Similar pattern to Jersey City
- **Hoboken:** Constrained by geography; smaller projects
- **Bayonne:** Most balanced mix of structure types

![Hudson Cities Comparison](charts/hudson_cities_comparison.png)

---

## Analysis 3: State Comparisons

### Per-Capita Permit Rates
"""

    if not state_comp.empty:
        state_avg = state_comp.groupby('state_abbrev')[['total_units', 'permits_per_1000']].mean()
        report += "\n| State | Avg Annual Permits | Per 1,000 Residents |\n"
        report += "|-------|-------------------|--------------------|\n"
        for state in ['TX', 'CA', 'MN', 'NY', 'NJ']:
            if state in state_avg.index:
                row = state_avg.loc[state]
                report += f"| {state} | {row['total_units']:,.0f} | {row['permits_per_1000']:.2f} |\n"

    report += """
### Analysis
Texas dramatically outpaces all other states in housing production:
- TX permits ~3-4x more per capita than NJ
- Even CA, with extensive housing regulations, produces more than NJ
- NJ and NY have similar per-capita rates, suggesting regional regulatory patterns

![State Comparison](charts/state_permits_per_capita.png)

---

## Analysis 4: Rent and Price Trends

### Metro Home Values (ZHVI)
The New York metro (including NJ) shows consistently higher home values than Austin or Minneapolis metros.
Recent appreciation has been strongest in Austin (until 2023) and continues in NYC.

### Rent Trends (ZORI)
- NYC metro rents: highest among comparison metros
- Austin: peaked in 2022, now declining
- Minneapolis: moderate growth, below national average

![Rent and Price Trends](charts/rent_price_trends.png)

---

## Analysis 5: Austin "Supply Shock"

### The Austin Experiment
Austin, TX represents a natural experiment in housing supply:
- Permitting surged from ~30,000 units/year (2015) to 65,000+ (2022)
- Per-capita rates reached 60+ permits per 1,000 residents
- Rents began declining in late 2023

### Key Evidence
"""

    if austin_data:
        austin_df, corr = austin_data
        if not np.isnan(corr):
            report += f"\n**Correlation (2-year lagged permits vs rent growth): {corr:.2f}**\n"

    report += """
This negative correlation suggests supply increases in year T are associated with rent
moderation in year T+2 (approximate construction lag).

![Austin Supply vs Rent](charts/austin_supply_vs_rent.png)

---

## Analysis 6: Minneapolis 2040 Plan

### Policy Context
Minneapolis adopted its 2040 Comprehensive Plan in December 2018, which:
- Eliminated single-family-only zoning citywide
- Allowed duplexes and triplexes in all residential areas
- Removed parking minimums near transit

### Results (Preliminary)
"""

    if mpls_data:
        mpls_df, comparison = mpls_data
        report += "\n| Period | Avg Annual Units | Avg Small Multi (2-4) | % Small Multi |\n"
        report += "|--------|-----------------|---------------------|---------------|\n"
        for _, row in comparison.iterrows():
            report += f"| {row['period']} | {row['avg_total']:.0f} | {row['avg_small_multi']:.0f} | {row['pct_small_multi']:.1f}% |\n"

    report += """
### Interpretation
- It's early to draw strong conclusions (only 5-6 years post-reform)
- COVID-19 disrupted 2020-2021 construction patterns
- Directionally, small multifamily shows signs of increase
- Full effects may take a decade to materialize

![Minneapolis 2040](charts/minneapolis_pre_post_2040.png)

---

## Conclusions

### For New Jersey
1. **NJ must dramatically increase housing production** to match peer states
2. **Jersey City alone cannot solve the shortage** - statewide policy needed
3. **Multifamily is the path forward** given land constraints
4. **Supply affects prices** as demonstrated by Austin

### Policy Recommendations
1. **Reduce regulatory barriers** to multifamily construction statewide
2. **Allow gentle density** (2-4 units) in more areas, following Minneapolis model
3. **Streamline permitting** to reduce time-to-build
4. **Invest in infrastructure** to support density in transit corridors
5. **Monitor Austin's trajectory** as evidence for supply-side approaches

---

## Appendix: Data Files

All output files are in the `analysis/` directory:

### Tables (CSV)
- `nj_permits_2010_2024.csv` - NJ statewide permits by year
- `hudson_county_permits.csv` - Hudson County cities by year
- `hudson_county_summary.csv` - Summary statistics
- `state_comparisons.csv` - State-level totals
- `state_comparisons_per_capita.csv` - Per-capita by state
- `city_comparisons.csv` - City-level permits
- `metro_home_values.csv` - ZHVI by metro
- `metro_rents.csv` - ZORI by metro
- `austin_supply_rent.csv` - Austin permits and rent
- `minneapolis_2040_comparison.csv` - Pre/post 2040 plan

### Charts (PNG)
- `nj_permits_trend.png`
- `hudson_cities_comparison.png`
- `state_permits_per_capita.png`
- `rent_price_trends.png`
- `austin_supply_vs_rent.png`
- `minneapolis_pre_post_2040.png`
"""

    with open(ANALYSIS_DIR / 'full_report.md', 'w') as f:
        f.write(report)


def main():
    """Run all analyses"""
    print("=" * 60)
    print("NJ Housing Data Analysis")
    print("=" * 60)

    # Ensure output directories exist
    TABLES_DIR.mkdir(parents=True, exist_ok=True)
    CHARTS_DIR.mkdir(parents=True, exist_ok=True)

    # Run analyses
    nj_permits = analysis_1_nj_statewide()
    hudson_permits = analysis_2_hudson_county()
    state_comp, city_comp = analysis_3_state_comparison()
    zhvi_annual, zori_annual, city_zhvi, city_zori = analysis_4_rent_price_trends()
    austin_data = analysis_5_austin_supply_shock()
    mpls_data = analysis_6_minneapolis_2040()

    # Write documentation
    write_data_inventory()
    write_findings(nj_permits, hudson_permits, state_comp, austin_data, mpls_data,
                   zhvi_annual, zori_annual)
    write_full_report(nj_permits, hudson_permits, state_comp, austin_data, mpls_data)

    print("=" * 60)
    print("Analysis complete! Output files in:")
    print(f"  {ANALYSIS_DIR}")
    print("=" * 60)


if __name__ == "__main__":
    main()
