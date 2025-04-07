#!/bin/bash
#SBATCH --job-name=xfer_SAMBA
#SBATCH --partition=test
#SBATCH --time=00:20:00
#SBATCH --ntasks-per-core=1
#SBATCH --ntasks-per-node=64
#SBATCH --ntasks-per-socket=32
#SBATCH --nodes=1

# ================================================================
# run_send_SAMBA_to_ODS.slurm
#
# Description: SLURM script to send the SAMBA hydrographic data
# to the Ocean Data Store.
#
# Created By: Ollie Tooth (oliver.tooth@noc.ac.uk)
# Created On: 2025-04-06
#
# ================================================================
# -- Input arguments to msm-os -- #
# Filepath to JASMIN OS credentials:
store_credentials_json=/dssgfs01/working/otooth/AtlantiS/jasmin_os/credentials/jasmin_os_credentials.json

# Bucket:
bucket=ocean-obs

# Define variables:
variables=consolidated

# -- Python Environment -- #
# Activate miniconda environment:
source /home/otooth/miniconda3/etc/profile.d/conda.sh
conda activate env_jasmin_os

# -- Send SAMBA monthly mean outputs to JASMIN OS -- #
# Filepath to SAMBA observations:
filepath=/dssgfs01/scratch/otooth/npd_data/observations/SAMBA/SAMBA_MOC_TotalAnomaly_and_constituents_ts.nc
# Define object prefix:
prefix=SAMBA/SAMBA_moc_transports_2009_2017

echo "In Progress: Sending SAMBA transports to JASMIN object store..."
msm_os send -f $filepath -c $store_credentials_json -b $bucket -p $prefix \
            -v $variables -zv 3 || { echo "Error: msm_os send failed."; exit 1; }

echo "Completed: Sent SAMBA transports to JASMIN object store."
