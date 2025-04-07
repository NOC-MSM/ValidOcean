"""
send_WOA23_annual_climatology_to_ODS.py

Description: Python script to send the World Ocean Atlas
2023 annual climatologies to the Ocean Data Store.

Python Environment: env_jasmin_os [conda]

Created by: Ollie Tooth (oliver.tooth@noc.ac.uk)
Created on: 2025-04-07
"""
# -- Import Python packages -- #
import logging
import numpy as np
import xarray as xr
from msm_os.object_store_handler import send

# --- Configure Logging --- #
logging.basicConfig(
    filename="send_WOA23_annual_climatology_to_ODS.log",
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

# -- 1. WOA23 1971-2000 climate normal -- #
filedir = "/dssgfs01/scratch/otooth/npd_data/observations/WOA2023"
ds_s71 = xr.open_dataset(f"{filedir}/woa23_decav71A0_s00_04.nc", decode_times=False)
ds_t71 = xr.open_dataset(f"{filedir}/woa23_decav71A0_t00_04.nc", decode_times=False)

ds_annual71 = xr.merge([ds_s71, ds_t71])
ds_annual71 = ds_annual71.assign_coords({'time': np.array([np.datetime64('1986-06-15', 'ns')])})
ds_annual71['climatology_bounds'].values = np.array([[np.datetime64('1971-01-01', 'ns'), np.datetime64('2000-12-31', 'ns')]])

# -- Define metadata for transfer to ODS -- #
# Define object prefix:
prefix = "WOA23/WOA23_1971_2000_annual_climatology"
# Define rechunking:
rechunk = {'lat': 720, 'lon': 1440, 'depth':10}
# Define attributes to be added to dataset:
attrs = {"description":"World Ocean Atlas 2023 1971-2000 Annual Climatological mean temperature & salinity for the global ocean from objectively analysed in situ data.","date_downloaded":"02/01/2025","date_uploaded_to_ods":"06/04/2025","temporal_range":"1971-01-01 to 2000-12-31","geographic_extent":"[-180, 180, -90, -90]","doi":" https://doi.org/10.25921/va26-hv25"}

# -- Send WOA23 to ODS -- #
logging.info("In Progress: Sending WOA23 1971-2000 climate normal to Ocean Data Store...")
send(file=ds_annual71,
     bucket=bucket,
     object_prefix=prefix,
     store_credentials_json=credentials_fpath,
     variables="consolidated",
     send_vars_indep=False,
     rechunk=rechunk,
     attrs=attrs,
     zarr_version=3
     )
logging.info("Completed: Sent WOA23 1971-2000 climate normal to Ocean Data Store.")

# -- 2. WOA23 1981-2010 climate normal -- #
ds_s81 = xr.open_dataset(f"{filedir}/woa23_decav81B0_s00_04.nc", decode_times=False)
ds_t81 = xr.open_dataset(f"{filedir}/woa23_decav81B0_t00_04.nc", decode_times=False)

ds_annual81 = xr.merge([ds_s81, ds_t81])
ds_annual81 = ds_annual81.assign_coords({'time': np.array([np.datetime64('1996-06-15', 'ns')])})
ds_annual81['climatology_bounds'].values = np.array([[np.datetime64('1981-01-01', 'ns'), np.datetime64('2010-12-31', 'ns')]])

# -- Define metadata for transfer to ODS -- #
# Define object prefix:
prefix = "WOA23/WOA23_1981_2010_annual_climatology"
# Define rechunking:
rechunk = {'lat': 720, 'lon': 1440, 'depth':10}
# Define attributes to be added to dataset:
attrs = {"description":"World Ocean Atlas 2023 1981-2010 Annual Climatological mean temperature & salinity for the global ocean from objectively analysed in situ data.","date_downloaded":"02/01/2025","date_uploaded_to_ods":"06/04/2025","temporal_range":"1981-01-01 to 2010-12-31","geographic_extent":"[-180, 180, -90, -90]","doi":" https://doi.org/10.25921/va26-hv25"}

# -- Send WOA23 to ODS -- #
logging.info("In Progress: Sending WOA23 1991-2010 climate normal to Ocean Data Store...")
send(file=ds_annual81,
     bucket=bucket,
     object_prefix=prefix,
     store_credentials_json=credentials_fpath,
     variables="consolidated",
     send_vars_indep=False,
     rechunk=rechunk,
     attrs=attrs,
     zarr_version=3
     )
logging.info("Completed: Sent WOA23 1981-2010 climate normal to Ocean Data Store.")

# 3. -- WOA23 1991-2020 climate normal -- #
ds_s91 = xr.open_dataset(f"{filedir}/woa23_decav91C0_s00_04.nc", decode_times=False)
ds_t91 = xr.open_dataset(f"{filedir}/woa23_decav91C0_t00_04.nc", decode_times=False)

ds_annual91 = xr.merge([ds_s91, ds_t91])
ds_annual91 = ds_annual91.assign_coords({'time': np.array([np.datetime64('2006-06-15', 'ns')])})
ds_annual91['climatology_bounds'].values = np.array([[np.datetime64('1991-01-01', 'ns'), np.datetime64('2020-12-31', 'ns')]])

# -- Define metadata for transfer to ODS -- #
# Define object prefix:
prefix = "WOA23/WOA23_1991_2020_annual_climatology"
# Define rechunking:
rechunk = {'lat': 720, 'lon': 1440, 'depth':10}
# Define attributes to be added to dataset:
attrs = {"description":"World Ocean Atlas 2023 1991-2010 Annual Climatological mean temperature & salinity for the global ocean from objectively analysed in situ data.","date_downloaded":"02/01/2025","date_uploaded_to_ods":"06/04/2025","temporal_range":"1991-01-01 to 2020-12-31","geographic_extent":"[-180, 180, -90, -90]","doi":" https://doi.org/10.25921/va26-hv25"}

# -- Send WOA23 to ODS -- #
logging.info("In Progress: Sending WOA23 1991-2020 climate normal to Ocean Data Store...")
send(file=ds_annual91,
     bucket=bucket,
     object_prefix=prefix,
     store_credentials_json=credentials_fpath,
     variables="consolidated",
     send_vars_indep=False,
     rechunk=rechunk,
     attrs=attrs,
     zarr_version=3
     )
logging.info("Completed: Sent WOA23 1991-2020 climate normal to Ocean Data Store.")
