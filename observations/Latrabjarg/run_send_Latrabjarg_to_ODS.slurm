#!/bin/bash
#SBATCH --job-name=xfer_Latrabjarg
#SBATCH --partition=test
#SBATCH --time=00:20:00
#SBATCH --ntasks-per-core=1
#SBATCH --ntasks-per-node=64
#SBATCH --ntasks-per-socket=32
#SBATCH --nodes=1

# ==============================================================
# run_send_Latrabjarg_to_ODS.slurm
#
# Description: SLURM script to send the Latrabjarg hydrographic data 
# to the Ocean Data Store.
#
# Created By: Ollie Tooth (oliver.tooth@noc.ac.uk)
# Created On: 2025-04-02
#
# ==============================================================
# -- Input arguments to msm-os -- #
# Filepath to Latrabjarg monthly mean output files:
filepath=/dssgfs01/scratch/otooth/npd_data/observations/Latrabjarg/nsv_latrabjarg_section_climatology_1990_2012.nc

# Filepath to JASMIN OS credentials:
store_credentials_json=/dssgfs01/working/otooth/AtlantiS/jasmin_os/credentials/jasmin_os_credentials.json

# Bucket and object prefix:
bucket=ocean-obs
prefix=Latrabjarg/Latrabjarg_section_climatology_1990_2012

# Define variables:
variables=consolidated

# -- Python Environment -- #
# Activate miniconda environment:
source /home/otooth/miniconda3/etc/profile.d/conda.sh
conda activate env_jasmin_os

# -- Send Latrabjarg monthly mean outputs to JASMIN OS -- #
echo "In Progress: Sending Latrabjarg climatology dataset to JASMIN object store..."
msm_os send -f $filepath -c $store_credentials_json -b $bucket -p $prefix \
            -v $variables \
            -at '{"description":"Látrabjarg hydrographic section climatology from 1990-2012 surveys. Downloaded from https://gws-access.jasmin.ac.uk/public/jmmp/NORVAL/.","date_downloaded":"19/12/2024","date_uploaded_to_ods":"02/04/2025","temporal_range":"1990-01-01 to 2012-12-31","geographic_extent":"[-29.7, -24.6, 65.4, 66.8]","doi":"https://doi.org/10.1002/2016JC012007"}' \
            -zv 3 || { echo "Error: msm_os send failed."; exit 1; }

echo "Completed: Sent Latrabjarg monthly RP dataset to JASMIN object store."