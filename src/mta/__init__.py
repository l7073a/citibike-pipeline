# MTA Subway Data Scripts
#
# This module contains scripts for fetching and processing MTA subway data:
#
# - fetch_gtfs.py: Download GTFS static feed (stations, routes, schedules)
# - fetch_ridership.py: Download hourly ridership from data.ny.gov
# - build_reference.py: Process GTFS into clean parquet reference tables
#
# Usage:
#   python src/mta/fetch_gtfs.py          # Download GTFS feed
#   python src/mta/build_reference.py     # Create reference tables
#   python src/mta/fetch_ridership.py     # Download ridership data
