"""
send_OISSTv2_monthly_to_jasmin_os.py

Description: Python script to send the OISSTv2 monthly sst & siconc
(1981-2025) data to the JASMIN Object Store.

Python Environment: env_jasmin_os [conda]

Created by: Ollie Tooth (oliver.tooth@noc.ac.uk)
Created on: 2025-02-24
"""
# -- Import Python packages -- #
import sys
from tqdm import tqdm
import numpy as np
import xarray as xr

# Add path to msm-os package:
sys.path.append('/dssgfs01/working/otooth/AtlantiS/jasmin_os/msm-os/src/')
from msm_os.object_store import ObjectStoreS3

# -- Define JASMIN S3 object store -- #
store_credentials_json = '/dssgfs01/working/otooth/AtlantiS/jasmin_os/credentials/jasmin_os_credentials.json'
obj_store = ObjectStoreS3(anon=False, store_credentials_json=store_credentials_json)
print('Completed: Initialised JASMIN S3 object store using user credentials.')

# -- Send OISSTv2 Sea Surface Temperature to JASMIN Object Store -- #
print('In Progress: Sending OISSTv2 SST Observations to JASMIN Object Store...')

# Define path to OISSTv2 obs.
ini_fpath = "/dssgfs01/working/otooth/Diagnostics/ValidOcean/observations/OISST/sst.mon.mean.nc"

# Define bucket name:
bucket = 'npd-obs'
# Define path to destination zarr store:
dest = f"{bucket}/OISSTv2/OISSTv2_sst_global_monthly_1981_2025"

# -- Write climatology to .zarr store -- #
# Define s3fs mapper to destination:
mapper = obj_store.get_mapper(dest)

# -- Import & Processing: Add metadata attributes -- #
ds = xr.open_dataset(ini_fpath, decode_times=True)

# Add description attribute:
ds.attrs['description'] = 'Monthly mean 1991-2020 sea surface temperature from Optimally Interpolated Sea Surface Temperature (OISST) dataset version 2. Downloaded from https://psl.noaa.gov/thredds/fileServer/Datasets/noaa.oisst.v2.highres/sst.mon.mean.nc on 24/02/2025. Transferred to JASMIN Object Store on 24/02/2025.'

# -- Write to JASMIN OS as .zarr store -- #
ds.to_zarr(mapper, mode='w', consolidated=True)
print('Completed: Sent OISSTv2 SST Observations to JASMIN Object Store.')

# -- Send OISSTv2 Sea Ice Observations to JASMIN Object Store -- #
print('In Progress: Sending OISSTv2 SI Observations to JASMIN Object Store...')

# Define path to OISSTv2 obs.
ini_fpath = "/dssgfs01/working/otooth/Diagnostics/ValidOcean/observations/OISST/icec.mon.mean.nc"

# Define bucket name:
bucket = 'npd-obs'
# Define path to destination zarr store:
dest = f"{bucket}/OISSTv2/OISSTv2_siconc_global_monthly_1981_2025"

# -- Write climatology to .zarr store -- #
# Define s3fs mapper to destination:
mapper = obj_store.get_mapper(dest)

# -- Import & Processing: Add metadata attributes -- #
ds = xr.open_dataset(ini_fpath, decode_times=True).rename({'icec':'siconc'})

# Add description attribute:
ds.attrs['description'] = 'Monthly mean 1991-2020 sea ice concentration from Optimally Interpolated Sea Surface Temperature (OISST) dataset version 2. Downloaded from https://psl.noaa.gov/thredds/fileServer/Datasets/noaa.oisst.v2.highres/icec.mon.mean.nc on 24/02/2025. Transferred to JASMIN Object Store on 24/02/2025.'

# -- Write to JASMIN OS as .zarr store -- #
ds.to_zarr(mapper, mode='w', consolidated=True)
print('Completed: Sent OISSTv2 SI Observations to JASMIN Object Store.')
