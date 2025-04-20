#!/bin/bash
#SBATCH --job-name=xfer_HadISST
#SBATCH --partition=test
#SBATCH --time=00:20:00
#SBATCH --ntasks-per-core=1
#SBATCH --ntasks-per-node=64
#SBATCH --ntasks-per-socket=32
#SBATCH --nodes=1

# ==============================================================
# run_send_HadISST_to_ODS.slurm
#
# Description: SLURM script to send the HadISST SST & sea ice 
# data to the Ocean Data Store.
#
# Created By: Ollie Tooth (oliver.tooth@noc.ac.uk)
# Created On: 2025-04-02
#
# ==============================================================
# -- Input arguments to msm-os -- #
# Filepath to HadISST monthly mean output files:
filepath=/dssgfs01/scratch/otooth/npd_data/observations/HadISST/HadISST_global_monthly_1870_2025.nc

# Filepath to JASMIN OS credentials:
store_credentials_json=/dssgfs01/working/otooth/AtlantiS/jasmin_os/credentials/jasmin_os_credentials.json

# Bucket and object prefix:
bucket=ocean-obs
prefix=HadISST/HadISST_global_monthly

# Define variables:
variables=consolidated

# -- Python Environment -- #
# Activate miniconda environment:
source /home/otooth/miniconda3/etc/profile.d/conda.sh
conda activate env_jasmin_os

# -- Send HadISST monthly mean outputs to JASMIN OS -- #
echo "In Progress: Sending HadISST monthly RP dataset to JASMIN object store..."
msm_os send -f $filepath -c $store_credentials_json -b $bucket -p $prefix \
            -cs '{"lat":180, "lon":360, "time":60}' -v $variables \
            -at '{"description":"Hadley Centre Sea Ice and Sea Surface Temperature data set (HadISST) is a unique combination of monthly globally-complete fields of sea surface temperature and sea ice concentration on a 1 degree latitude-longitude grid from 1870 to present.","date_downloaded":"02/04/2025","date_uploaded_to_ods":"02/04/2025","temporal_range":"1870-01-01 to 2025-01-16","geographic_extent":"[-180, 180, -90, 90]","doi":"https://www.metoffice.gov.uk/hadobs/hadisst/data/download.html"}' \
            -zv 3 || { echo "Error: msm_os send failed."; exit 1; }

echo "Completed: Sent HadISST monthly RP dataset to JASMIN object store."