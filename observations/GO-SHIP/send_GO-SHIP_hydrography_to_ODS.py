"""
send_GO-SHIP_hydrography_to_ODS.py

Description: Python script to send the GO-SHIP
hydrographic section data to the Ocean Data Store.

Python Environment: env_jasmin_os [conda]

Created by: Ollie Tooth (oliver.tooth@noc.ac.uk)
Created on: 2025-04-07
"""
# -- Import Python packages -- #
import glob
import logging
import xarray as xr
from msm_os.object_store_handler import send

# --- Configure Logging --- #
logging.basicConfig(
    filename="send_GO-SHIP_hydrography_to_ODS.log",
    encoding="utf-8",
    filemode="a",
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
    level=logging.INFO,
    )

# -- Define common metadata for transfer to ODS -- #
# Define bucket name:
bucket = "ocean-obs"
# Define path to credentials .JSON file:
credentials_fpath = "/dssgfs01/working/otooth/AtlantiS/jasmin_os/credentials/jasmin_os_credentials.json"

# -- Send GO-SHIP Observations to Ocean Data Store -- #
logging.info('In Progress: Sending GO-SHIP Observations to Ocean Data Store...')

# Define path to GO-SHIP obs.
ini_fpath = '/dssgfs01/scratch/otooth/npd_data/observations/GO-SHIP/*.nc'
files = glob.glob(ini_fpath)

# Iterate over observation files:
for file in files:
    # Define filepath:
    filename = file.split('/')[-1]
    # Define object prefix:
    prefix = f"GO-SHIP/{filename.replace('.nc', '')}"

    # -- Import & Processing: Add metadata attributes -- #
    ds = xr.open_dataset(file)

    # -- Write to zarr store -- #
    logging.info(f'In Progress: Sending {filename} to Ocean Data Store...')
    send(file=ds,
         bucket=bucket,
         object_prefix=prefix,
         store_credentials_json=credentials_fpath,
         variables="consolidated",
         send_vars_indep=False,
         zarr_version=3
         )
    logging.info(f"Completed: Sent {filename} to Ocean Data Store.")

logging.info('Completed: Sent GO-SHIP Observations to Ocean Data Store...')
