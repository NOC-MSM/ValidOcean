###########################################################################
"""
Download global domain of the DUACS gridded sea surface height dataset from 
Copernicus Marine. This script downloads the daily fields from 1993 to 2024.

Created By: Ollie Tooth
Created On: 2025-04-08
Contact: oliver.tooth@noc.ac.uk

Note: This download script should be run using the env_npd_obs environment.
"""
###########################################################################

# Import the Copernicus Marine API toolbox:
import copernicusmarine

# Define filepath to credentials:
credentials_fpath = "/home/otooth/.copernicusmarine/.copernicusmarine-credentials"
# Define output directory:
out_fdir = "/dssgfs01/scratch/otooth/npd_data/observations/DUACS/"

# Download the DUACS L4 sea level analysis dataset:
for year in range(1993, 2024):
  copernicusmarine.subset(
    dataset_id="cmems_obs-sl_glo_phy-ssh_my_allsat-l4-duacs-0.125deg_P1D",
    variables=["adt"],
    start_datetime=f"{year}-01-01T00:00:00",
    end_datetime=f"{year}-12-31T00:00:00",
    credentials_file=credentials_fpath,
    output_directory=out_fdir,
    file_format="netcdf",
    output_filename=f"DUACS_SEALEVEL_GLO_PHY_L4_MY_{year}.nc",
  )
