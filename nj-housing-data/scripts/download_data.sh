#!/bin/bash
# =============================================================================
# NJ Housing Data - Reproducible Download Script
# =============================================================================
#
# This script downloads all raw data sources for the NJ housing analysis.
# Run from the nj-housing-data directory.
#
# Usage:
#   chmod +x scripts/download_data.sh
#   ./scripts/download_data.sh
#
# Data Sources:
#   1. Zillow Research (ZHVI, ZORI) - direct CSV downloads
#   2. US Census Building Permits Survey - text files from census.gov
#   3. NJ DCA Construction Reporter - Excel file from nj.gov
#   4. HUD SOCDS - requires manual download (query-based)
#
# =============================================================================

set -e  # Exit on error

BASE_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$BASE_DIR"

echo "=============================================="
echo "NJ Housing Data Download Script"
echo "Base directory: $BASE_DIR"
echo "=============================================="

# Create directory structure
echo -e "\n[1/5] Creating directory structure..."
mkdir -p zillow census-bps nj-dca hud scripts

# -----------------------------------------------------------------------------
# ZILLOW RESEARCH DATA
# Source: https://www.zillow.com/research/data/
# Files hosted at: files.zillowstatic.com
# -----------------------------------------------------------------------------
echo -e "\n[2/5] Downloading Zillow Research data..."

ZILLOW_BASE="https://files.zillowstatic.com/research/public_csvs"

# ZHVI (Zillow Home Value Index) - typical home values
echo "  - Metro ZHVI..."
curl -sL -o zillow/Metro_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv \
  "${ZILLOW_BASE}/zhvi/Metro_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv"

echo "  - City ZHVI..."
curl -sL -o zillow/City_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv \
  "${ZILLOW_BASE}/zhvi/City_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv"

# ZORI (Zillow Observed Rent Index) - typical rents
echo "  - Metro ZORI..."
curl -sL -o zillow/Metro_zori_uc_sfrcondomfr_sm_sa_month.csv \
  "${ZILLOW_BASE}/zori/Metro_zori_uc_sfrcondomfr_sm_sa_month.csv"

echo "  - City ZORI..."
curl -sL -o zillow/City_zori_uc_sfrcondomfr_sm_sa_month.csv \
  "${ZILLOW_BASE}/zori/City_zori_uc_sfrcondomfr_sm_sa_month.csv"

# -----------------------------------------------------------------------------
# US CENSUS BUILDING PERMITS SURVEY
# Source: https://www.census.gov/construction/bps/
# Data: https://www2.census.gov/econ/bps/
# -----------------------------------------------------------------------------
echo -e "\n[3/5] Downloading Census Building Permits Survey..."

CENSUS_BASE="https://www2.census.gov/econ/bps"
YEARS="2010 2011 2012 2013 2014 2015 2016 2017 2018 2019 2020 2021 2022 2023 2024"

# State-level annual data
echo "  - State-level data (2010-2024)..."
for year in $YEARS; do
  curl -sL -o "census-bps/st${year}a.txt" "${CENSUS_BASE}/State/st${year}a.txt"
done

# Place-level annual data by region
echo "  - Northeast region place data..."
for year in $YEARS; do
  curl -sL -o "census-bps/ne${year}a.txt" "${CENSUS_BASE}/Place/Northeast%20Region/ne${year}a.txt"
done

echo "  - South region place data..."
for year in $YEARS; do
  curl -sL -o "census-bps/so${year}a.txt" "${CENSUS_BASE}/Place/South%20Region/so${year}a.txt"
done

echo "  - Midwest region place data..."
for year in $YEARS; do
  curl -sL -o "census-bps/mw${year}a.txt" "${CENSUS_BASE}/Place/Midwest%20Region/mw${year}a.txt"
done

echo "  - West region place data..."
for year in $YEARS; do
  curl -sL -o "census-bps/we${year}a.txt" "${CENSUS_BASE}/Place/West%20Region/we${year}a.txt"
done

# -----------------------------------------------------------------------------
# NJ DCA CONSTRUCTION REPORTER
# Source: https://www.nj.gov/dca/codes/reporter/
# -----------------------------------------------------------------------------
echo -e "\n[4/5] Downloading NJ DCA data..."
curl -sL -o nj-dca/Development_Trend_Viewer.xlsb \
  "https://www.nj.gov/dca/codes/reporter/Development_Trend_Viewer.xlsb"

# -----------------------------------------------------------------------------
# HUD SOCDS (Manual download required)
# -----------------------------------------------------------------------------
echo -e "\n[5/5] HUD SOCDS data..."
echo "  NOTE: HUD SOCDS requires manual download via query interface."
echo "  Visit: https://socds.huduser.gov/permits/"
echo "  Instructions saved to: hud/README.md"

cat > hud/README.md << 'EOF'
# HUD SOCDS Building Permits Database

The HUD SOCDS database requires interactive access.

## How to Download

1. Visit https://socds.huduser.gov/permits/
2. Accept terms and conditions
3. Use Query Tool:
   - Geography: States & Counties
   - Select states: NJ, NY, TX, MN, CA
   - Survey type: Annual
   - Years: 2010-2024
   - Series: All Permits (or specific types)
4. Click "Report"
5. Click "CSV" to download

## Alternative: HUD Open Data

County-level permits available at:
https://hudgis-hud.opendata.arcgis.com/datasets/HUD::residential-construction-permits-by-county/
EOF

# -----------------------------------------------------------------------------
# VERIFICATION
# -----------------------------------------------------------------------------
echo -e "\n=============================================="
echo "Download complete! Verifying files..."
echo "=============================================="

echo -e "\nZillow files:"
ls -lh zillow/

echo -e "\nCensus BPS files (count):"
ls census-bps/*.txt | wc -l
echo "files downloaded"

echo -e "\nNJ DCA files:"
ls -lh nj-dca/

echo -e "\n=============================================="
echo "Data download complete!"
echo "Run 'python3 analysis/analyze_housing.py' to process."
echo "=============================================="
