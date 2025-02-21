#########################################################################
"""
Download global domain of ARMOR3D dataset from Copernicus Marine.
This script downloads the monthly reprocessed files from 1993 to 2022.

Created By: Ollie Tooth
Created On: 2024-09-17
Contact: oliver.tooth@noc.ac.uk

Note: This download script should be run using the env_npd_obs environment.
"""
#########################################################################

# Import the Copernicus Marine API toolbox:
import copernicusmarine

# Define filepath to credentials:
credentials_fpath = "/home/otooth/.copernicusmarine/.copernicusmarine-credentials"
# Define output directory:
out_fdir = "/dssgfs01/scratch/otooth/npd_data/observations/ARMOR3D/data/"

# Download the ARMOR3D analysis dataset for North Atlantic subdomain:
for year in range(1993, 2023):
    copernicusmarine.subset(
      dataset_id="dataset-armor-3d-rep-monthly",
      variables=["mlotst", "so", "to", "ugo", "vgo", "zo"],
      start_datetime=f"{year}-01-01T00:00:00",
      end_datetime=f"{year}-12-01T00:00:00",
      credentials_file=credentials_fpath,
      output_directory=out_fdir,
      file_format="zarr",
      # netcdf_compression_enabled=True,
      output_filename=f"armor-3d_rep_monthly_NA_{year}.zarr",
      force_download=True
    )
