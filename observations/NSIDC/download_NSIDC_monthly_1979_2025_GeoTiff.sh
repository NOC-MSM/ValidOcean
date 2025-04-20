#!/bin/bash

# ----------------------------------------------------------------
# This script downloads the National Snow & Ice Data Centre (NSIDC)
# Sea Ice Index version 3 sea ice extent & concentration GeoTiff
# files from 1979 to 2025.
#
# Files will be downloaded into the current directory.
#
# Created By: Ollie Tooth (oliver.tooth@noc.ac.uk)
# Created On: 2025-02-21
# ----------------------------------------------------------------
# Update users via stdout:
echo "-- Downloading NSIDC SI Index GeoTiff dataset --"
echo " Oliver J. Tooth, National Oceanography Centre, 2025"
echo "Downloading: NSIDC Sea Ice Extent & Concentration GeoTiffs..."

# Define hemisphere:
hemisphere="north"
# Define the URL prefix:
url_prefix="https://noaadata.apps.nsidc.org/NOAA/G02135/"$hemisphere"/monthly/geotiff"

# Download monthly sea ice extent & concentration files from 1979 to 2025:
for month in 01_Jan 02_Feb 03_Mar 04_Apr 05_May 06_Jun 07_Jul 08_Aug 09_Sep 10_Oct 11_Nov 12_Dec
do 
    echo "Downloading NSIDC 1979-2025 Sea Ice Conc. GeoTiffs for: $month"
    wget -r -nd --no-check-certificate --reject "index.html*" -np -e robots=off $url_prefix/$month/
done

# Update users via stdout:
echo "...Completed: Downloaded NSIDC" $hemisphere "Sea Ice Extent & Concentration GeoTiffs"
