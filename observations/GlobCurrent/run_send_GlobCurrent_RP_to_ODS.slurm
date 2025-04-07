#!/bin/bash
#SBATCH --job-name=xfer_GlobCurrent
#SBATCH --partition=test
#SBATCH --time=00:20:00
#SBATCH --ntasks-per-core=1
#SBATCH --ntasks-per-node=64
#SBATCH --ntasks-per-socket=32
#SBATCH --nodes=1

# ==============================================================
# run_send_GlobCurrent_RP_to_ODS.slurm
#
# Description: SLURM script to send the GlobCurrent monthly mean 
# reprocessed data to the Ocean Data Store.
#
# Created By: Ollie Tooth (oliver.tooth@noc.ac.uk)
# Created On: 2025-04-01
#
# ==============================================================
# -- Input arguments to msm-os -- #
# Filepath to GlobCurrent monthly mean output files:
filedir=/dssgfs01/scratch/otooth/npd_data/observations/GlobCurrent/RP
filepaths=$filedir/globcurrent_my_0.25deg_P1M-m_*.nc

# Filepath to JASMIN OS credentials:
store_credentials_json=/dssgfs01/working/otooth/AtlantiS/jasmin_os/credentials/jasmin_os_credentials.json

# Filepath to Dask Configuration file:
dask_config_json=/dssgfs01/working/otooth/Diagnostics/ValidOcean/ocean_data_store/dask_config.json

# Bucket and object prefix:
bucket=ocean-obs
prefix=GlobCurrent/GlobCurrent_RP_global_monthly_1993_2023

# Define variables:
variables=consolidated

# -- Python Environment -- #
# Activate miniconda environment:
source /home/otooth/miniconda3/etc/profile.d/conda.sh
conda activate env_jasmin_os

# -- Send GlobCurrent monthly mean outputs to JASMIN OS -- #
echo "In Progress: Sending GlobCurrent monthly RP dataset to JASMIN object store..."
msm_os send_with_dask -f $filepaths -c $store_credentials_json -b $bucket -p $prefix \
                      -cs '{"latitude":720, "longitude":720, "depth":2}' -v $variables \
                      -at '{"description":"GlobCurrent reprocessed monthly mean of the total, tide, Ekman and geostrophic currents & monthly uncertainties (version 2024-11).","date_downloaded":"01/04/2025","date_uploaded_to_ods":"01/04/2025","temporal_range":"1993-01-01 to 2023-12-31","geographic_extent":"[-180, 180, -90, 90]","doi":"https://doi.org/10.48670/mds-00327"}' \
                      -dc $dask_config_json -zv 3 || { echo "Error: msm_os send_with_dask failed."; exit 1; }

echo "Completed: Sent GlobCurrent monthly RP dataset to JASMIN object store."
