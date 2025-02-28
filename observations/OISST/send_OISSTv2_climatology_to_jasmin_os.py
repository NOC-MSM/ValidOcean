"""
send_OISSTv2_climatology_to_jasmin_os.py

Description: Python script to send the OISSTv2 hydrographic
section climatology (1990-2012) data to the JASMIN Object Store.

Python Environment: env_jasmin_os [conda]

Created by: Ollie Tooth (oliver.tooth@noc.ac.uk)
Created on: 2024-12-19
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

# -- Send OISSTv2 Sea Ice Observations to JASMIN Object Store -- #
print('In Progress: Sending OISSTv2 SI Observations to JASMIN Object Store...')

# Define path to OISSTv2 obs.
ini_fpath = "http://psl.noaa.gov/thredds/dodsC/Datasets/noaa.oisst.v2.highres/icec.mon.ltm.1991-2020.nc"

# Define bucket name:
bucket = 'npd-obs'
# Define path to destination zarr store:
dest = f"{bucket}/OISSTv2/OISSTv2_siconc_global_monthly_climatology_1991_2020"

# -- Write climatology to .zarr store -- #
# Define s3fs mapper to destination:
mapper = obj_store.get_mapper(dest)

# -- Import & Processing: Add metadata attributes -- #
ds = xr.open_dataset(ini_fpath, decode_times=False).rename({'icec': 'siconc'})
# Transform time axis to datetime64:
ds['time'] = xr.DataArray((np.datetime64('2000-01', 'M') + (np.timedelta64(1, 'M') * np.arange(ds['time'].size))).astype('datetime64[ns]'), dims='time')

# Rechunking for optimal read performance:
ds = ds.chunk({'time': 12, 'lat': 720, 'lon': 1440})

# Add description attribute:
ds.attrs['description'] = 'Long-term mean 1991-2020 sea ice concentration from Optimally Interpolated Sea Surface Temperature (OISST) dataset version 2. Downloaded from http://psl.noaa.gov/thredds/dodsC/Datasets/noaa.oisst.v2.highres/icec.mon.ltm.1991-2020.nc on 19/12/2024. Transferred to JASMIN Object Store on 19/12/2024.'

# -- Write to JASMIN OS as .zarr store -- #
ds.to_zarr(mapper, mode='w', consolidated=True)

print('Completed: Sent OISSTv2 SI Observations to JASMIN Object Store.')

# -- Send OISSTv2 Sea Surface Temperature Observations to JASMIN Object Store -- #
print('In Progress: Sending OISSTv2 SST Observations to JASMIN Object Store...')

# Define path to OISSTv2 obs.
ini_fpath = "http://psl.noaa.gov/thredds/dodsC/Datasets/noaa.oisst.v2.highres/sst.mon.ltm.1991-2020.nc"

# Define path to destination zarr store:
dest = f"{bucket}/OISSTv2/OISSTv2_sst_global_monthly_climatology_1991_2020"

# -- Write climatology to .zarr store -- #
# Define s3fs mapper to destination:
mapper = obj_store.get_mapper(dest)

# -- Import & Processing: Add metadata attributes -- #
ds = xr.open_dataset(ini_fpath, decode_times=False)
# Transform time axis to datetime64:
ds['time'] = xr.DataArray((np.datetime64('2000-01', 'M') + (np.timedelta64(1, 'M') * np.arange(ds['time'].size))).astype('datetime64[ns]'), dims='time')

# Rechunking for optimal read performance:
ds = ds.chunk({'time': 12, 'lat': 720, 'lon': 1440})

# Add description attribute:
ds.attrs['description'] = 'Long-term mean 1991-2020 sea surface temperature from Optimally Interpolated Sea Surface Temperature (OISST) dataset version 2. Downloaded from http://psl.noaa.gov/thredds/dodsC/Datasets/noaa.oisst.v2.highres/icec.mon.ltm.1991-2020.nc on 19/12/2024. Transferred to JASMIN Object Store on 19/12/2024.'

# -- Write to JASMIN OS as .zarr store -- #
ds.to_zarr(mapper, mode='w', consolidated=True)

print('Completed: Sent OISSTv2 SST Observations to JASMIN Object Store.')
