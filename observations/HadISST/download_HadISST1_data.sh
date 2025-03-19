#!/bin/bash

# ----------------------------------------------------------------
# download_HadISST1_data.sh
#
# This script downloads the HadISST1 dataset from the Met Office
# Hadley Centre HadISST website. The files to be downloaded are
# HadISST1_sst.nc.gz & HadISST_ice.nc.gz.
#
# Files will be downloaded into the current directory.
#
# Created By: Ollie Tooth (oliver.tooth@noc.ac.uk)
# Created On: 2025-03-19
# ----------------------------------------------------------------
# Update users via stdout:
echo "-- Downloading HadISST dataset --"
echo " Oliver J. Tooth, National Oceanography Centre, 2025"
echo "Downloading: HadISST1 sst & sea ice data..."

# Define the URL list:
url=https://www.metoffice.gov.uk/hadobs/hadisst/data

# Download the HadISST1 dataset:
wget $url/HadISST_sst.nc.gz
wget $url/HadISST_ice.nc.gz

# Unzip the files:
gunzip HadISST_sst.nc.gz
gunzip HadISST_ice.nc.gz

# Update users via stdout:
echo "...Completed: HadISST1 dataset downloaded"
