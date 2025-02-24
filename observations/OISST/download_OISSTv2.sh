#!/bin/bash

# ----------------------------------------------------------------
# This script downloads the EN.4.2.2 dataset from the Met Office
# Hadley Centre EN.4.2.2 website. The files to be downloaded are
# included in the EN.4.2.2.analyses.g10.download-list.txt file.
#
# Files will be downloaded into the current directory.
#
# Created By: Ollie Tooth (oliver.tooth@noc.ac.uk)
# Created On: 2024-12-16
# ----------------------------------------------------------------
# Update users via stdout:
echo "-- Downloading OISSTv2 Monthly dataset --"
echo " Oliver J. Tooth, National Oceanography Centre, 2025"
echo "Downloading: OISSTv2 monthly SST data..."

wget https://psl.noaa.gov/thredds/fileServer/Datasets/noaa.oisst.v2.highres/sst.mon.mean.nc
# Update users via stdout:
echo "...Completed: OISSTv2 monthly SST data downloaded"

echo "Downloading: OISSTv2 monthly SI data..."

wget https://psl.noaa.gov/thredds/fileServer/Datasets/noaa.oisst.v2.highres/icec.mon.mean.nc
# Update users via stdout:
echo "...Completed: OISSTv2 monthly SST data downloaded"