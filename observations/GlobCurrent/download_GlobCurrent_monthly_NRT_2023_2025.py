#########################################################################
"""
Download Global Current dataset consisting of Ekman and
Geostrophic currents at the Surface and 15m from Copernicus Marine.
This script downloads the monthly near-real-time files from 1993 to 2022.

Created By: Ollie Tooth
Created On: 2025-02-21
Contact: oliver.tooth@noc.ac.uk

Note: This download script should be run using the env_npd_obs environment.
"""
#########################################################################

# Import the Copernicus Marine API toolbox:
import copernicusmarine

# Define filepath to credentials:
credentials_fpath = "/home/otooth/.copernicusmarine/.copernicusmarine-credentials"
# Define output directory:
out_fdir = "/dssgfs01/scratch/otooth/npd_data/observations/GlobCurrent/"

# Download the GlobCurrent analysis dataset:
for year in range(2023, 2026):
    copernicusmarine.subset(
      dataset_id="cmems_obs-mob_glo_phy-cur_nrt_0.25deg_P1M-m",
      start_datetime=f"{year}-01-01T00:00:00",
      end_datetime=f"{year}-12-01T00:00:00",
      credentials_file=credentials_fpath,
      output_directory=out_fdir,
      file_format="netcdf",
      output_filename=f"globcurrent_my_0.25deg_P1M-m_nrt_{year}.nc",
    )
