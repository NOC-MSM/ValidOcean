#!/bin/bash
#SBATCH --job-name=xfer_LOPS_MLD
#SBATCH --partition=test
#SBATCH --time=00:20:00
#SBATCH --ntasks-per-core=1
#SBATCH --ntasks-per-node=64
#SBATCH --ntasks-per-socket=32
#SBATCH --nodes=1

# ================================================================
# run_send_LOPS_MLD_to_ODS.slurm
#
# Description: SLURM script to send the LOPS-MLD mixed layer depth
# climatology to the Ocean Data Store.
#
# Created By: Ollie Tooth (oliver.tooth@noc.ac.uk)
# Created On: 2025-04-03
#
# ================================================================
# -- Input arguments to msm-os -- #
# Filepath to LOPS_MLD monthly mean output files:
filepath=/dssgfs01/scratch/otooth/npd_data/observations/LOPS-MLD/mld_dr003_ref10m_v2023.nc

# Filepath to JASMIN OS credentials:
store_credentials_json=/dssgfs01/working/otooth/AtlantiS/jasmin_os/credentials/jasmin_os_credentials.json

# Bucket and object prefix:
bucket=ocean-obs
prefix=LOPS-MLD/LOPS-MLD_v2023_global_monthly_climatology

# Define variables:
variables=consolidated

# -- Python Environment -- #
# Activate miniconda environment:
source /home/otooth/miniconda3/etc/profile.d/conda.sh
conda activate env_jasmin_os

# -- Send LOPS-MLD monthly mean outputs to JASMIN OS -- #
echo "In Progress: Sending LOPS-MLD climatology dataset to JASMIN object store..."
msm_os send -f $filepath -c $store_credentials_json -b $bucket -p $prefix \
            -cs '{"lat":180, "lon":360, "time":12}' -v $variables \
            -at '{"description":"Monthly climatology (i.e. 12 months) of ocean surface Mixed Layer Depth (MLD) over the global ocean, at 1 degree x 1 degree spatial resolution computed with a density threshold criterion of 0.03 kg/m3 from 10 m depth value. See https://cerweb.ifremer.fr/mld for more details. ","date_downloaded":"18/12/2024","date_uploaded_to_ods":"03/04/2025","temporal_range":"1970-01-01 to 2021-12-31","geographic_extent":"[-180, 180, -90, 90]","doi":"https://doi.org/10.17882/91774"}' \
            -zv 3 || { echo "Error: msm_os send failed."; exit 1; }

echo "Completed: Sent LOPS_MLD monthly RP dataset to JASMIN object store."