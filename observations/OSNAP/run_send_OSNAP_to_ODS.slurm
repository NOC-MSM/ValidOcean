#!/bin/bash
#SBATCH --job-name=xfer_OSNAP
#SBATCH --partition=test
#SBATCH --time=00:20:00
#SBATCH --ntasks-per-core=1
#SBATCH --ntasks-per-node=64
#SBATCH --ntasks-per-socket=32
#SBATCH --nodes=1

# ================================================================
# run_send_OSNAP_to_ODS.slurm
#
# Description: SLURM script to send the OSNAP hydrographic data
# to the Ocean Data Store.
#
# Created By: Ollie Tooth (oliver.tooth@noc.ac.uk)
# Created On: 2025-04-06
#
# ================================================================
# -- Input arguments to msm-os -- #
# Filepath to OSNAP observations:
filepath=/dssgfs01/scratch/otooth/npd_data/observations/OSNAP/OSNAP_Gridded_TSV_201408_202006_2023.nc

# Filepath to JASMIN OS credentials:
store_credentials_json=/dssgfs01/working/otooth/AtlantiS/jasmin_os/credentials/jasmin_os_credentials.json

# Bucket and object prefix:
bucket=ocean-obs
prefix=OSNAP/OSNAP_gridded_2014_2020

# Define variables:
variables=consolidated

# -- Python Environment -- #
# Activate miniconda environment:
source /home/otooth/miniconda3/etc/profile.d/conda.sh
conda activate env_jasmin_os

# -- Send OSNAP monthly mean outputs to JASMIN OS -- #
echo "In Progress: Sending OSNAP gridded dataset to JASMIN object store..."
msm_os send -f $filepath -c $store_credentials_json -b $bucket -p $prefix \
            -v $variables -zv 3 || { echo "Error: msm_os send failed."; exit 1; }

echo "Completed: Sent OSNAP gridded dataset to JASMIN object store."