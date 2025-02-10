#!/bin/bash

# ----------------------------------------------------------------
# This script unzips the EN.4.2.2 dataset files downloaded from
# the Met Office  Hadley Centre. The resulting monthly files are
# inflated from those listed in the
# EN.4.2.2.analyses.g10.download-list.txt file.
#
# Files will be unzipped into the:
# /dssgfs01/working/otooth/Diagnostics/proj_NPD_obs/data/EN.4.2.2
# directory.
#
# Created By: Ollie Tooth (oliver.tooth@noc.ac.uk)
# Created On: 2024-12-16
# ----------------------------------------------------------------
# Update users via stdout:
echo "-- Unzipping EN.4.2.2 dataset --"
echo " Oliver J. Tooth, National Oceanography Centre, 2024"
echo "Unzipping: files in EN.4.2.2 dataset..."

# Unzip files the EN.4.2.2 dataset
fpath="/dssgfs01/scratch/otooth/npd_data/observations/EN.4.2.2/src/"
out_dir="/dssgfs01/scratch/otooth/npd_data/observations/EN.4.2.2/data/"

for yr in {1950..2024};
do 
    fname="EN.4.2.2.analyses.g10.${yr}.zip"
    echo $fname
    unzip "${fpath}${fname}" -d ${out_dir} 
done

# Update users via stdout:
echo "...Completed: EN.4.2.2 dataset unzipped"