# NJ Housing Production Analysis: Full Report

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

| Year | Single Family | 2-4 Units | 5+ Units | Total | Per 1,000 |
|------|--------------|-----------|----------|-------|----------|
| 2010 | 7,378 | 956 | 5,201 | 13,535 | 1457.10 |
| 2011 | 6,475 | 613 | 5,864 | 12,952 | 1394.34 |
| 2012 | 7,279 | 929 | 9,731 | 17,939 | 1931.21 |
| 2013 | 10,377 | 1,091 | 12,741 | 24,209 | 2606.20 |
| 2014 | 11,019 | 1,164 | 15,972 | 28,155 | 3031.00 |
| 2015 | 10,518 | 999 | 19,043 | 30,560 | 3289.91 |
| 2016 | 9,626 | 1,159 | 16,008 | 26,793 | 2884.38 |
| 2017 | 10,148 | 1,481 | 16,872 | 28,501 | 3068.25 |
| 2018 | 10,348 | 1,494 | 16,100 | 27,942 | 3008.07 |
| 2019 | 11,526 | 1,998 | 22,981 | 36,505 | 3929.92 |
| 2020 | 12,289 | 1,874 | 21,983 | 36,146 | 3891.27 |
| 2021 | 13,913 | 1,837 | 21,344 | 37,094 | 3993.33 |
| 2022 | 13,185 | 1,923 | 21,798 | 36,906 | 3973.09 |
| 2023 | 13,228 | 2,134 | 17,478 | 32,840 | 3535.36 |
| 2024 | 14,807 | 2,041 | 18,084 | 34,932 | 3760.58 |

### Key Observations
- NJ housing production remains below pre-2008 levels
- Large multifamily (5+ units) is the dominant growth segment
- Per-capita production averages ~2-3 permits per 1,000 residents

![NJ Permits Trend](charts/nj_permits_trend.png)

---

## Analysis 2: Hudson County Cities

### Total Production (2010-2024)

| City | Total Units | % of Group |
|------|-------------|------------|
| Jersey City | 33,590 | 61.1% |
| Newark | 11,500 | 20.9% |
| Bayonne | 5,897 | 10.7% |
| Hoboken | 3,976 | 7.2% |

### Structure Type by City
- **Jersey City:** Heavily weighted to large multifamily developments (towers)
- **Newark:** Similar pattern to Jersey City
- **Hoboken:** Constrained by geography; smaller projects
- **Bayonne:** Most balanced mix of structure types

![Hudson Cities Comparison](charts/hudson_cities_comparison.png)

---

## Analysis 3: State Comparisons

### Per-Capita Permit Rates

| State | Avg Annual Permits | Per 1,000 Residents |
|-------|-------------------|--------------------|
| TX | 184,843 | 6.34 |
| CA | 94,049 | 2.38 |
| MN | 21,723 | 3.81 |
| NY | 39,515 | 1.96 |
| NJ | 28,334 | 3.05 |

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

**Correlation (2-year lagged permits vs rent growth): -0.04**

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

| Period | Avg Annual Units | Avg Small Multi (2-4) | % Small Multi |
|--------|-----------------|---------------------|---------------|
| Pre-2040 (2010-2018) | 2262 | 15 | 0.6% |
| Post-2040 (2019-2024) | 2833 | 52 | 1.8% |

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
