#!/bin/bash
#SBATCH --job-name=xfer_GOSML
#SBATCH --partition=test
#SBATCH --time=00:20:00
#SBATCH --ntasks-per-core=1
#SBATCH --ntasks-per-node=64
#SBATCH --ntasks-per-socket=32
#SBATCH --nodes=1

# ==============================================================
# run_send_GOSML_to_ODS.slurm
#
# Description: SLURM script to send the GOSML ocean mixed layer 
# climatology to the Ocean Data Store.
#
# Created By: Ollie Tooth (oliver.tooth@noc.ac.uk)
# Created On: 2025-04-01
#
# ==============================================================
# -- Input arguments to msm-os -- #
# Filepath to GOSML monthly mean output files:
filepath=/dssgfs01/scratch/otooth/npd_data/observations/GOSML/GOSML_mixed_layer_properties_mean.nc

# Filepath to JASMIN OS credentials:
store_credentials_json=/dssgfs01/working/otooth/AtlantiS/jasmin_os/credentials/jasmin_os_credentials.json

# Bucket and object prefix:
bucket=ocean-obs
prefix=GOSML/GOSML_global_monthly_climatology_2000_2020

# Define variables:
variables=consolidated

# -- Python Environment -- #
# Activate miniconda environment:
source /home/otooth/miniconda3/etc/profile.d/conda.sh
conda activate env_jasmin_os

# -- Send GOSML monthly mean outputs to JASMIN OS -- #
echo "In Progress: Sending GOSML monthly RP dataset to JASMIN object store..."
msm_os send -f $filepath -c $store_credentials_json -b $bucket -p $prefix \
            -cs '{"latitude":180, "longitude":360, "month":12}' -v $variables \
            -at '{"description":"GOSML: Global Ocean Surface Mixed Layer Statistical Monthly Climatology (GOSML) of mean depth, temperature, and salinity.","date_downloaded":"17/12/2024","date_uploaded_to_ods":"01/04/2025","temporal_range":"1999-01-01 to 2021-01-01","geographic_extent":"[-180, 180, -90, 90]","doi":"https://doi.org/10.1029/2021JC018219"}' \
            -zv 3 || { echo "Error: msm_os send failed."; exit 1; }

echo "Completed: Sent GOSML monthly RP dataset to JASMIN object store."