#!/bin/bash
#SBATCH --job-name=xfer_NSIDC
#SBATCH --partition=test
#SBATCH --time=00:20:00
#SBATCH --ntasks-per-core=1
#SBATCH --ntasks-per-node=64
#SBATCH --ntasks-per-socket=32
#SBATCH --nodes=1

# ================================================================
# run_send_NSIDC_to_ODS.slurm
#
# Description: SLURM script to send the NSIDC sea ice observations
# data to the Ocean Data Store.
#
# Created By: Ollie Tooth (oliver.tooth@noc.ac.uk)
# Created On: 2025-04-03
#
# ================================================================
# -- Input arguments to msm-os -- #
# Filepath to NSIDC monthly mean output files:
filedir=/dssgfs01/scratch/otooth/npd_data/observations/NSIDC

# Filepath to JASMIN OS credentials:
store_credentials_json=/dssgfs01/working/otooth/AtlantiS/jasmin_os/credentials/jasmin_os_credentials.json

# Define Bucket:
bucket=ocean-obs

# Define variables:
variables=consolidated

# -- Python Environment -- #
# Activate miniconda environment:
source /home/otooth/miniconda3/etc/profile.d/conda.sh
conda activate env_jasmin_os

# -- Send NSIDC Arctic monthly mean outputs to JASMIN OS -- #
filepath=$filedir/NSIDC_Sea_Ice_Index_v3_Arctic_combined_1978_2025.nc
prefix=NSIDC/NSIDC_Sea_Ice_Index_v3_Arctic

echo "In Progress: Sending NSIDC Arctic monthly dataset to JASMIN object store..."
msm_os send -f $filepath -c $store_credentials_json -b $bucket -p $prefix \
            -cs '{"x":304, "y":448, "time":60}' -v $variables \
            -at '{"description":"National Snow & Ice Data Centre (NSIDC) Sea Ice Index version 3: Arctic sea ice concentration and total extent (computed) data.","date_downloaded":"21/02/2025","date_uploaded_to_ods":"20/04/2025","temporal_range":"1978-01-01 to 2025-04-01","geographic_extent":"[-180, 180, 30, 90]","doi":"https://doi.org/10.7265/N5K072F8"}' \
            -zv 3 || { echo "Error: msm_os send failed."; exit 1; }

echo "Completed: Sent NSIDC Arctic monthly dataset to JASMIN object store."

# -- Send NSIDC Antarctic monthly mean outputs to JASMIN OS -- #
filepath=$filedir/NSIDC_Sea_Ice_Index_v3_Antarctic_combined_1978_2025.nc
prefix=NSIDC/NSIDC_Sea_Ice_Index_v3_Antarctic

echo "In Progress: Sending NSIDC Antarctic monthly dataset to JASMIN object store..."
msm_os send -f $filepath -c $store_credentials_json -b $bucket -p $prefix \
            -cs '{"x":316, "y":332, "time":60}' -v $variables \
            -at '{"description":"National Snow & Ice Data Centre (NSIDC) Sea Ice Index version 3: Antarctic sea ice concentration and total extent (computed) data.","date_downloaded":"21/02/2025","date_uploaded_to_ods":"20/04/2025","temporal_range":"1978-01-01 to 2025-04-01","geographic_extent":"[-180, 180, -30, -90]","doi":"https://doi.org/10.7265/N5K072F8"}' \
            -zv 3 || { echo "Error: msm_os send failed."; exit 1; }

echo "Completed: Sent NSIDC Antarctic monthly dataset to JASMIN object store."