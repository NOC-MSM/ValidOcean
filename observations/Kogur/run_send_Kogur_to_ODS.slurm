#!/bin/bash
#SBATCH --job-name=xfer_Kogur
#SBATCH --partition=test
#SBATCH --time=00:20:00
#SBATCH --ntasks-per-core=1
#SBATCH --ntasks-per-node=64
#SBATCH --ntasks-per-socket=32
#SBATCH --nodes=1

# ==============================================================
# run_send_Kogur_to_ODS.slurm
#
# Description: SLURM script to send the Kogur hydrographic data 
# to the Ocean Data Store.
#
# Created By: Ollie Tooth (oliver.tooth@noc.ac.uk)
# Created On: 2025-04-02
#
# ==============================================================
# -- Input arguments to msm-os -- #
# Filepath to Kogur monthly mean output files:
filepath=/dssgfs01/scratch/otooth/npd_data/observations/Kogur/nsv_kogur_section_2011_2012.nc

# Filepath to JASMIN OS credentials:
store_credentials_json=/dssgfs01/working/otooth/AtlantiS/jasmin_os/credentials/jasmin_os_credentials.json

# Bucket and object prefix:
bucket=ocean-obs
prefix=Kogur/Kogur_whoi_section_2011_2012

# Define variables:
variables=consolidated

# -- Python Environment -- #
# Activate miniconda environment:
source /home/otooth/miniconda3/etc/profile.d/conda.sh
conda activate env_jasmin_os

# -- Send Kogur monthly mean outputs to JASMIN OS -- #
echo "In Progress: Sending Kogur hydrographic dataset to JASMIN object store..."
msm_os send -f $filepath -c $store_credentials_json -b $bucket -p $prefix \
            -v $variables \
            -at '{"description":"Kögur hydrographic section data from 2011-2012 survey conducted by WHOI. Contact Benjamin Harden (benharden27@gmail.com) for more details.","date_downloaded":"19/12/2024","date_uploaded_to_ods":"02/04/2025","temporal_range":"2011-09-01 to 2012-12-31","geographic_extent":"[-25.5, -23.5, 67.2, 68.4]","doi":"https://gws-access.jasmin.ac.uk/public/jmmp/NORVAL/"}' \
            -zv 3 || { echo "Error: msm_os send failed."; exit 1; }

echo "Completed: Sent Kogur hydrographic dataset to JASMIN object store."