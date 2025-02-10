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
echo "-- Downloading EN.4.2.2 dataset --"
echo " Oliver J. Tooth, National Oceanography Centre, 2024"
echo "Downloading: files in EN.4.2.2.analyses.g10.download-list.txt..."

# Define the URL list:
urls=$(cat /dssgfs01/scratch/otooth/npd_data/observations/EN.4.2.2/src/EN.4.2.2.analyses.g10.download-list.txt)

# Download the EN.4.2.2 dataset:
for url in $urls
do
    wget $url
done

# Update users via stdout:
echo "...Completed: EN.4.2.2 dataset downloaded"

