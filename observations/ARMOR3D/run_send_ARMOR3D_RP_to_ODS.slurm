#!/bin/bash
#SBATCH --job-name=xfer_ARMOR3D
#SBATCH --partition=test
#SBATCH --time=00:20:00
#SBATCH --ntasks-per-core=1
#SBATCH --ntasks-per-node=64
#SBATCH --ntasks-per-socket=32
#SBATCH --nodes=1

# ==============================================================
# run_send_ARMOR3D_RP_to_ODS.slurm
#
# Description: SLURM script to send the ARMOR3D monthly mean 
# reprocessed data to the Ocean Data Store.
#
# Created By: Ollie Tooth (oliver.tooth@noc.ac.uk)
# Created On: 2025-04-01
#
# ==============================================================
# -- Input arguments to msm-os -- #
# Filepath to ARMOR3D monthly mean output files:
filedir=/dssgfs01/scratch/otooth/npd_data/observations/ARMOR3D

# Filepath to JASMIN OS credentials:
store_credentials_json=/dssgfs01/working/otooth/AtlantiS/jasmin_os/credentials/jasmin_os_credentials.json

# Filepath to Dask Configuration file:
dask_config_json=/dssgfs01/working/otooth/Diagnostics/ValidOcean/observations/dask_config.json

# Bucket and object prefix:
bucket=ocean-obs
prefix=ARMOR3D/ARMOR3D_RP_global_monthly_1993_2022

# Define variable & append dimension:
variables=consolidated
append_dim=time

# -- Python Environment -- #
# Activate miniconda environment:
source /home/otooth/miniconda3/etc/profile.d/conda.sh
conda activate env_jasmin_os

# -- Send ARMOR3D monthly mean outputs to JASMIN OS -- #
filepaths=$filedir/armor-3d_rep_monthly_NA_1993.nc

echo "In Progress: Sending ARMOR3D monthly RP dataset to JASMIN object store..."
msm_os send -f $filepaths -c $store_credentials_json -b $bucket -p $prefix \
            -cs '{"latitude":350, "longitude":720, "depth":5}' -v $variables \
            -at '{"description":"Multi Observation Global Ocean ARMOR3D L4 analysis - Monthly Reprocessed (version 2020-12).","date_downloaded":"17/12/2024","date_uploaded_to_ods":"22/04/2025","temporal_range":"1993-01-01 to 2022-12-31","geographic_extent":"[-180, 180, -90, 90]","doi":"https://doi.org/10.48670/moi-00052"}' \
            -zv 3 || { echo "Error: msm_os send failed."; exit 1; }

echo "Completed: Sent ARMOR3D monthly RP dataset to JASMIN object store."

for yr in {1994..2022}
do 
    filepaths=$filedir/armor-3d_rep_monthly_NA_${yr}.nc

    echo "In Progress: Updating ARMOR3D monthly RP dataset for ${yr} to JASMIN object store..."
    msm_os update -f $filepaths -c $store_credentials_json -b $bucket -p $prefix \
                  -cs '{"latitude":350, "longitude":720, "depth":5}' -v $variables -ad $append_dim \
                  -at '{"description":"Multi Observation Global Ocean ARMOR3D L4 analysis - Monthly Reprocessed (version 2020-12).","date_downloaded":"17/12/2024","date_uploaded_to_ods":"22/04/2025","temporal_range":"1993-01-01 to 2022-12-31","geographic_extent":"[-180, 180, -90, 90]","doi":"https://doi.org/10.48670/moi-00052"}' \
                  -zv 3 || { echo "Error: msm_os update failed."; exit 1; }

    echo "Completed: Updated ARMOR3D monthly RP dataset for ${yr} to JASMIN object store."
done