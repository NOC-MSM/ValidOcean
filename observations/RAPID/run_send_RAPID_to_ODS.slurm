#!/bin/bash
#SBATCH --job-name=xfer_RAPID
#SBATCH --partition=test
#SBATCH --time=00:20:00
#SBATCH --ntasks-per-core=1
#SBATCH --ntasks-per-node=64
#SBATCH --ntasks-per-socket=32
#SBATCH --nodes=1

# ================================================================
# run_send_RAPID_to_ODS.slurm
#
# Description: SLURM script to send the RAPID hydrographic data
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

# -- Send RAPID monthly mean outputs to JASMIN OS -- #
# Filepath to RAPID observations:
filepath=/dssgfs01/scratch/otooth/npd_data/observations/RAPID/FC_cable_transport_1982-2024.nc
# Define object prefix:
prefix=RAPID/fc_unadjusted_transport

echo "In Progress: Sending RAPID Florida Current transports to JASMIN object store..."
msm_os send -f $filepath -c $store_credentials_json -b $bucket -p $prefix \
            -v $variables -zv 3 || { echo "Error: msm_os send failed."; exit 1; }

echo "Completed: Sent RAPID Florida Current transports to JASMIN object store."

# Filepath to RAPID observations:
filepath=/dssgfs01/scratch/otooth/npd_data/observations/RAPID/moc_transports.nc
# Define object prefix:
prefix=RAPID/moc_transports

echo "In Progress: Sending RAPID MOC transports to JASMIN object store..."
msm_os send -f $filepath -c $store_credentials_json -b $bucket -p $prefix \
            -v $variables -zv 3 || { echo "Error: msm_os send failed."; exit 1; }

echo "Completed: Sent RAPID MOC transports dataset to JASMIN object store."

# Filepath to RAPID observations:
filepath=/dssgfs01/scratch/otooth/npd_data/observations/RAPID/moc_vertical.nc
# Define object prefix:
prefix=RAPID/moc_vertical

echo "In Progress: Sending RAPID MOC Vertical to JASMIN object store..."
msm_os send -f $filepath -c $store_credentials_json -b $bucket -p $prefix \
            -v $variables -zv 3 || { echo "Error: msm_os send failed."; exit 1; }

echo "Completed: Sent RAPID MOC Vertical to JASMIN object store."

# Filepath to RAPID observations:
filepath=/dssgfs01/scratch/otooth/npd_data/observations/RAPID/mocha_mht_data_ERA5_v2020.nc
# Define object prefix:
prefix=RAPID/mocha_mht_data_ERA5_v2020

echo "In Progress: Sending RAPID MHT to JASMIN object store..."
msm_os send -f $filepath -c $store_credentials_json -b $bucket -p $prefix \
            -v $variables -zv 3 || { echo "Error: msm_os send failed."; exit 1; }

echo "Completed: Sent RAPID MOC MHT to JASMIN object store."

# Filepath to RAPID observations:
filepath=/dssgfs01/scratch/otooth/npd_data/observations/RAPID/ts_gridded.nc
# Define object prefix:
prefix=RAPID/ts_gridded

echo "In Progress: Sending RAPID T-S gridded to JASMIN object store..."
msm_os send -f $filepath -c $store_credentials_json -b $bucket -p $prefix \
            -v $variables -zv 3 || { echo "Error: msm_os send failed."; exit 1; }

echo "Completed: Sent RAPID T-S gridded object store."
