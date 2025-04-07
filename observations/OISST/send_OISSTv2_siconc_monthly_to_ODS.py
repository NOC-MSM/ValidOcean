"""
send_OISSTv2_siconc_monthly_to_ODS.py

Description: Python script to send the OISSTv2 monthly siconc
(1981-present) data to the Ocean Data Store.

Python Environment: env_jasmin_os [conda]

Created by: Ollie Tooth (oliver.tooth@noc.ac.uk)
Created on: 2025-04-06
"""
# -- Import Python packages -- #
import logging
import xarray as xr
from msm_os.object_store_handler import send

# --- Configure Logging --- #
logging.basicConfig(
    filename="send_OISSTv2_siconc_monthly_to_ODS.log",
    encoding="utf-8",
    filemode="a",
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
    level=logging.INFO,
    )

# -- Import OISSTv2 Sea Ice Concentration from NOAA -- #
# Define path to OISSTv2 obs.
ini_fpath = "http://psl.noaa.gov/thredds/dodsC/Datasets/noaa.oisst.v2.highres/icec.mon.mean.nc"
ds = xr.open_dataset(ini_fpath, decode_times=True)

# -- Define metadata for transfer to ODS -- #
# Define bucket name:
bucket = "ocean-obs"
# Define object prefix:
prefix = "OISSTv2/OISSTv2_siconc_global_monthly"
# Define path to credentials .JSON file:
credentials_fpath = "/dssgfs01/working/otooth/AtlantiS/jasmin_os/credentials/jasmin_os_credentials.json"

# Define rechunking:
rechunk = {'time': 60, 'lat': 720, 'lon': 1440}
# Define attributes to be added to dataset:
attrs = {"description":"Monthly mean sea ice concentration from NOAA/NCEI Optimally Interpolated Sea Surface Temperature (OISST) dataset version 2.","date_downloaded":"06/04/2025","date_uploaded_to_ods":"06/04/2025","temporal_range":"1981-09-01 to 2025-03-01","geographic_extent":"[-180, 180, -90, -90]","doi":"https://doi.org/10.1175/JCLI-D-20-0166.1"}

# -- Send OISSTv2 Sea Ice Conc. to ODS -- #
logging.info("In Progress: Sending OISSTv2 SIC Observations to Ocean Data Store...")
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
logging.info("Completed: Sent OISSTv2 SIC Observations to Ocean Data Store.")
