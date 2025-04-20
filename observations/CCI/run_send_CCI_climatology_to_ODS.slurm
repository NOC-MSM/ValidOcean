#!/bin/bash
#SBATCH --job-name=xfer_CCI
#SBATCH --partition=test
#SBATCH --time=00:20:00
#SBATCH --ntasks-per-core=1
#SBATCH --ntasks-per-node=64
#SBATCH --ntasks-per-socket=32
#SBATCH --nodes=1

# ================================================================
# run_send_CCI_to_ODS.slurm
#
# Description: SLURM script to send the CCI sea surface temperature
# data to the Ocean Data Store.
#
# Created By: Ollie Tooth (oliver.tooth@noc.ac.uk)
# Created On: 2025-04-06
#
# ================================================================
# -- Input arguments to msm-os -- #
# Filepath to JASMIN OS credentials:
store_credentials_json=/dssgfs01/working/otooth/AtlantiS/jasmin_os/credentials/jasmin_os_credentials.json

# Filepath to Dask Configuration file:
dask_config_json=/dssgfs01/working/otooth/Diagnostics/ValidOcean/observations/dask_config.json

# Bucket:
bucket=ocean-obs

# Define variables:
variables=consolidated

# -- Python Environment -- #
# Activate miniconda environment:
source /home/otooth/miniconda3/etc/profile.d/conda.sh
conda activate env_jasmin_os

# -- Send CCI monthly mean outputs to JASMIN OS -- #
# Filepath to CCI observations:
filepath=/dssgfs01/scratch/otooth/npd_data/observations/CCI/ESACCI-L4-SST-climatology-GLOB_CDR3.0.nc
# Define object prefix:
prefix=CCI/ESACCI-v3.0-SST_global_climatology_1991_2020

echo "In Progress: Sending CCI SST climatology to JASMIN object store..."
msm_os send_with_dask -f $filepath -c $store_credentials_json -b $bucket -p $prefix -v $variables \
                      -at '{"description":"Time-average of the ESA CCI Sea Surface Temperature CDR v3.0 product providing daily climatological mean sea surface temperature (SST) and sea ice fraction on a global 0.05° latitude-longitude grid, derived from the SST CCI analysis data for the period 1991 to 2020 (30 years)","date_downloaded":"17/12/2024","date_uploaded_to_ods":"07/04/2025","temporal_range":"1991-01-01 to 2020-12-31","geographic_extent":"[-180, 180, -90, 90]","doi":"https://doi.org/10.5285/62800d3d2227449085b430b503d36b01"}' \
                      -dc $dask_config_json -zv 3 || { echo "Error: msm_os send_with_dask failed."; exit 1; }

echo "Completed: Sent CCI SST climatology to JASMIN object store."

# Filepath to CCI observations:
filepath=/dssgfs01/scratch/otooth/npd_data/observations/CCI/ESACCI-L4-SST-seasonal-climatology-GLOB_CDR3.0.nc
# Define object prefix:
prefix=CCI/ESACCI-v3.0-SST_global_seasonal_climatology_1991_2020

echo "In Progress: Sending CCI SST climatology to JASMIN object store..."
msm_os send_with_dask -f $filepath -c $store_credentials_json -b $bucket -p $prefix -v $variables \
                      -at '{"description":"Seasonal averages of the ESA CCI Sea Surface Temperature CDR v3.0 product providing daily climatological mean sea surface temperature (SST) and sea ice fraction on a global 0.05° latitude-longitude grid, derived from the SST CCI analysis data for the period 1991 to 2020 (30 years)","date_downloaded":"17/12/2024","date_uploaded_to_ods":"07/04/2025","temporal_range":"1991-01-01 to 2020-12-31","geographic_extent":"[-180, 180, -90, 90]","doi":"https://doi.org/10.5285/62800d3d2227449085b430b503d36b01"}' \
                      -dc $dask_config_json -zv 3 || { echo "Error: msm_os send_with_dask failed."; exit 1; }

echo "Completed: Sent CCI SST climatology to JASMIN object store."

# Filepath to CCI observations:
filepath=/dssgfs01/scratch/otooth/npd_data/observations/CCI/ESACCI-L4-SST-monthly-climatology-GLOB_CDR3.0.nc
# Define object prefix:
prefix=CCI/ESACCI-v3.0-SST_global_monthly_climatology_1991_2020

echo "In Progress: Sending CCI SST climatology to JASMIN object store..."
msm_os send_with_dask -f $filepath -c $store_credentials_json -b $bucket -p $prefix -v $variables \
                      -at '{"description":"Monthly averages of the ESA CCI Sea Surface Temperature CDR v3.0 product providing daily climatological mean sea surface temperature (SST) and sea ice fraction on a global 0.05° latitude-longitude grid, derived from the SST CCI analysis data for the period 1991 to 2020 (30 years)","date_downloaded":"17/12/2024","date_uploaded_to_ods":"07/04/2025","temporal_range":"1991-01-01 to 2020-12-31","geographic_extent":"[-180, 180, -90, 90]","doi":"https://doi.org/10.5285/62800d3d2227449085b430b503d36b01"}' \
                      -dc $dask_config_json -zv 3 || { echo "Error: msm_os send_with_dask failed."; exit 1; }

echo "Completed: Sent CCI SST climatology to JASMIN object store."
