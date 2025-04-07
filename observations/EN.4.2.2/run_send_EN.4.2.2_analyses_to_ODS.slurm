#!/bin/bash
#SBATCH --job-name=xfer_EN4.2.2
#SBATCH --partition=test
#SBATCH --time=00:20:00
#SBATCH --ntasks-per-core=1
#SBATCH --ntasks-per-node=64
#SBATCH --ntasks-per-socket=32
#SBATCH --nodes=1

# ==============================================================
# run_send_EN.4.2.2_analyses_to_ODS.slurm
#
# Description: SLURM script to send the EN4.2.2 monthly mean 
# analyses data to the Ocean Data Store.
#
# Created By: Ollie Tooth (oliver.tooth@noc.ac.uk)
# Created On: 2025-04-06
#
# ==============================================================
# -- Input arguments to msm-os -- #
# Filepath to EN.4.2.2 monthly mean output files:
filedir=/dssgfs01/scratch/otooth/npd_data/observations/EN.4.2.2/
filepaths=$filedir/EN.4.2.2.f.analysis.g10.*.nc

# Filepath to JASMIN OS credentials:
store_credentials_json=/dssgfs01/working/otooth/AtlantiS/jasmin_os/credentials/jasmin_os_credentials.json

# Filepath to Dask Configuration file:
dask_config_json=/dssgfs01/working/otooth/Diagnostics/ValidOcean/observations/dask_config.json

# Bucket and object prefix:
bucket=ocean-obs
prefix=EN4/EN4.2.2_global_monthly

# Define variables:
variables=consolidated
# Define append dimension:
append_dimension=time

# -- Python Environment -- #
# Activate miniconda environment:
source /home/otooth/miniconda3/etc/profile.d/conda.sh
conda activate env_jasmin_os

# -- Send EN.4.2.2 monthly mean outputs to JASMIN OS -- #
echo "In Progress: Sending EN.4.2.2 monthly dataset to JASMIN object store..."
msm_os send_with_dask -f $filepaths -c $store_credentials_json -b $bucket -p $prefix \
                      -cs '{"lat":173, "lon":360, "depth":42, "time":12}' \
                      -ad $append_dimension -v $variables \
                      -at '{"description":"Met Office Hadley Centre EN4.2.2 objective analyses - Gouretski and Reseghetti (2010) XBT corrections and Gouretski and Cheng (2020) MBT corrections.","date_downloaded":"06/04/2025","date_uploaded_to_ods":"06/04/2025","temporal_range":"1950-01-01 to 2025-01-31","geographic_extent":"[-180, 180, -90, 90]","doi":"https://doi.org/10.1002/2013JC009067"}' \
                      -dc $dask_config_json -zv 3 || { echo "Error: msm_os send_with_dask failed."; exit 1; }

echo "Completed: Sent EN.4.2.2 monthly dataset to JASMIN object store."
