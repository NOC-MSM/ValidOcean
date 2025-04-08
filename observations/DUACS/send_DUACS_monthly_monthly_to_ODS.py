"""
send_DUACS_adt_monthly_to_ODS.py

Description: Python script to send the DUACS monthly sea surface
height data to the Ocean Data Store.

Python Environment: env_jasmin_os [conda]

Created by: Ollie Tooth (oliver.tooth@noc.ac.uk)
Created on: 2025-04-08
"""
# -- Import Python packages -- #
import logging
import xarray as xr
from msm_os.object_store_handler import send

# --- Configure Logging --- #
logging.basicConfig(
    filename="send_DUACS_ssh_monthly_to_ODS.log",
    encoding="utf-8",
    filemode="w",
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
    level=logging.INFO,
    )

# -- Import DUACS SSH from Copernicus -- #
# Define path to DUACS obs.
ini_fpath = "/dssgfs01/scratch/otooth/npd_data/observations/DUACS/DUACS_SEALEVEL_GLO_PHY_L4_MY_*.nc"
ds = xr.open_mfdataset(ini_fpath)

# -- Preprocess DUACS SSH -- #
# Calculate monthly means from daily data:
ds = ds.resample(time='1MS').mean(dim='time')
# Rename absolute dynamic topography to sea surface height:
ds = ds.rename({"adt": "ssh"})

# -- Define metadata for transfer to ODS -- #
# Define bucket name:
bucket = "ocean-obs"
# Define object prefix:
prefix = "DUACS/DUACS_MY_global_monthly_1993_2023"
# Define path to credentials .JSON file:
credentials_fpath = "/dssgfs01/working/otooth/AtlantiS/jasmin_os/credentials/jasmin_os_credentials.json"

# Define rechunking:
rechunk = {'time': 12, 'longitude': 1440}
# Define attributes to be added to dataset:
attrs = {"description":"DUACS multimission altimeter satellite L4 gridded absolute dynamic topography (sea surface height above geoid) computed as the sum of the sea level anomaly & mean dynamic topography. ","date_downloaded":"08/04/2025","date_uploaded_to_ods":"08/04/2025","temporal_range":"1993-01-01 to 2023-12-31","geographic_extent":"[-180, 180, -90, -90]","doi":"https://doi.org/10.48670/moi-00148"}

# -- Send DUACS SSH to ODS -- #
logging.info("In Progress: Sending DUACS SSH Observations to Ocean Data Store...")
send(file=ds,
     bucket=bucket,
     object_prefix=prefix,
     store_credentials_json=credentials_fpath,
     variables="consolidated",
     send_vars_indep=False,
     rechunk=rechunk,
     attrs=attrs,
     zarr_version=3
     )
logging.info("Completed: Sent DUACS SSH Observations to Ocean Data Store.")
