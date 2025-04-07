"""
send_WOA23_seasonal_climatology_to_ODS.py

Description: Python script to send the World Ocean Atlas
2023 seasonal climatologies to the Ocean Data Store.

Python Environment: env_jasmin_os [conda]

Created by: Ollie Tooth (oliver.tooth@noc.ac.uk)
Created on: 2025-04-07
"""
# -- Import Python packages -- #
import glob
import logging
import numpy as np
import xarray as xr
from msm_os.object_store_handler import send

# --- Configure Logging --- #
logging.basicConfig(
    filename="send_WOA23_monthly_climatology_to_ODS.log",
    encoding="utf-8",
    filemode="a",
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
    level=logging.INFO,
    )

# -- Define function to get filepaths for WOA2023 data -- #
def get_filepaths(file_prefix:str, file_directory:str) -> list:
    """
    Get filepaths for World Ocean Atlas 2023 Climate Normals
    temperature and salinity data. 

    Parameters
    ----------
    file_prefix : str
        Prefix of the WOA23 filepaths to be returned.
    file_directory : str
        Directory containing the WOA23 files.

    Returns
    -------
    filepaths : list
        List of WOA23 filepaths for practical salinity & potential temperature.
    """
    # Define salinity and temperature filenames:
    salinity_filename = f"{file_prefix}_s*.nc"
    temperature_filename = f"{file_prefix}_t*.nc"

    # Get filepaths for salinity and temperature data:
    filepaths_sal = glob.glob(f"{file_directory}/{salinity_filename}")
    filepaths_temp = glob.glob(f"{file_directory}/{temperature_filename}")
    filepaths_sal.sort()
    filepaths_temp.sort()

    return filepaths_sal, filepaths_temp


# Define common metadata for transfer to ODS:
# Define bucket name:
bucket = "ocean-obs"
# Define path to credentials .JSON file:
credentials_fpath = "/dssgfs01/working/otooth/AtlantiS/jasmin_os/credentials/jasmin_os_credentials.json"

# -- Processing WOA23 Climate Normal Seasonal Climatologies -- #
# Define seasons:
seasons = np.array(['JFM', 'AMJ', 'JAS', 'OND'], dtype='str')

# -- 1. WOA23 1971-2000 climate normal -- #
filedir = "/dssgfs01/scratch/otooth/npd_data/observations/WOA2023"
filepaths_sal, filepaths_temp = get_filepaths(file_directory=filedir, file_prefix='woa23_decav71A0')

ds_s71 = xr.open_mfdataset(filepaths_sal[1:13], decode_times=False)
ds_t71 = xr.open_mfdataset(filepaths_temp[1:13], decode_times=False)

ds_monthly71 = xr.merge([ds_s71, ds_t71])
ds_monthly71 = ds_monthly71.rename({'time': 'month'}).assign_coords({'month': np.arange(1, 13)})
ds_monthly71 = ds_monthly71.drop_vars('climatology_bounds')

# -- Define metadata for transfer to ODS -- #
# Define object prefix:
prefix = "WOA23/WOA23_1971_2000_monthly_climatology"
# Define rechunking:
rechunk = {'lat': 720, 'lon': 1440, 'depth':10}
# Define attributes to be added to dataset:
attrs = {"description":"World Ocean Atlas 2023 1971-2000 Monthly Climatological mean temperature & salinity for the global ocean from objectively analysed in situ data.","date_downloaded":"02/01/2025","date_uploaded_to_ods":"06/04/2025","temporal_range":"1971-01-01 to 2000-12-31","geographic_extent":"[-180, 180, -90, -90]","doi":" https://doi.org/10.25921/va26-hv25"}

# -- Send WOA23 to ODS -- #
logging.info("In Progress: Sending WOA23 1971-2000 monthly climate normal to Ocean Data Store...")
send(file=ds_monthly71,
     bucket=bucket,
     object_prefix=prefix,
     store_credentials_json=credentials_fpath,
     variables="consolidated",
     send_vars_indep=False,
     rechunk=rechunk,
     attrs=attrs,
     zarr_version=3
     )
logging.info("Completed: Sent WOA23 1971-2000 monthly climate normal to Ocean Data Store.")

# -- 2. WOA23 1981-2010 climate normal -- #
filepaths_sal, filepaths_temp = get_filepaths(file_directory=filedir, file_prefix='woa23_decav81B0')

ds_s81 = xr.open_mfdataset(filepaths_sal[1:13], decode_times=False)
ds_t81 = xr.open_mfdataset(filepaths_temp[1:13], decode_times=False)

ds_monthly81 = xr.merge([ds_s81, ds_t81])
ds_monthly81 = ds_monthly81.rename({'time': 'month'}).assign_coords({'month': np.arange(1, 13)})
ds_monthly81 = ds_monthly81.drop_vars('climatology_bounds')

# -- Define metadata for transfer to ODS -- #
# Define object prefix:
prefix = "WOA23/WOA23_1981_2010_monthly_climatology"
# Define rechunking:
rechunk = {'lat': 720, 'lon': 1440, 'depth':10}
# Define attributes to be added to dataset:
attrs = {"description":"World Ocean Atlas 2023 1981-2010 Monthly Climatological mean temperature & salinity for the global ocean from objectively analysed in situ data.","date_downloaded":"02/01/2025","date_uploaded_to_ods":"06/04/2025","temporal_range":"1981-01-01 to 2010-12-31","geographic_extent":"[-180, 180, -90, -90]","doi":" https://doi.org/10.25921/va26-hv25"}

# -- Send WOA23 to ODS -- #
logging.info("In Progress: Sending WOA23 1981-2010 monthly climate normal to Ocean Data Store...")
send(file=ds_monthly81,
     bucket=bucket,
     object_prefix=prefix,
     store_credentials_json=credentials_fpath,
     variables="consolidated",
     send_vars_indep=False,
     rechunk=rechunk,
     attrs=attrs,
     zarr_version=3
     )
logging.info("Completed: Sent WOA23 1981-2010 monthly climate normal to Ocean Data Store.")

# -- 3. WOA23 1991-2020 climate normal -- #
filepaths_sal, filepaths_temp = get_filepaths(file_directory=filedir, file_prefix='woa23_decav91C0')

ds_s91 = xr.open_mfdataset(filepaths_sal[1:13], decode_times=False)
ds_t91 = xr.open_mfdataset(filepaths_temp[1:13], decode_times=False)

ds_monthly91 = xr.merge([ds_s91, ds_t91])
ds_monthly91 = ds_monthly91.rename({'time': 'month'}).assign_coords({'month': np.arange(1, 13)})
ds_monthly91 = ds_monthly91.drop_vars('climatology_bounds')

# -- Define metadata for transfer to ODS -- #
# Define object prefix:
prefix = "WOA23/WOA23_1991_2020_monthly_climatology"
# Define rechunking:
rechunk = {'lat': 720, 'lon': 1440, 'depth':10}
# Define attributes to be added to dataset:
attrs = {"description":"World Ocean Atlas 2023 1991-2020 Monthly Climatological mean temperature & salinity for the global ocean from objectively analysed in situ data.","date_downloaded":"02/01/2025","date_uploaded_to_ods":"06/04/2025","temporal_range":"1991-01-01 to 2020-012-31","geographic_extent":"[-180, 180, -90, -90]","doi":" https://doi.org/10.25921/va26-hv25"}

# -- Send WOA23 to ODS -- #
logging.info("In Progress: Sending WOA23 1991-2020 monthly climate normal to Ocean Data Store...")
send(file=ds_monthly91,
     bucket=bucket,
     object_prefix=prefix,
     store_credentials_json=credentials_fpath,
     variables="consolidated",
     send_vars_indep=False,
     rechunk=rechunk,
     attrs=attrs,
     zarr_version=3
     )
logging.info("Completed: Sent WOA23 1991-2020 monthly climate normal to Ocean Data Store.")
