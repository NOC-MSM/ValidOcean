"""
send_GOSML_to_jasmin_os.py

Description: Python script to send the GOSML global mixed
layer climatology data to the JASMIN Object Store.

Created by: Ollie Tooth (oliver.tooth@noc.ac.uk)
Created on: 2024-12-19
"""
# -- Import Python packages -- #
import sys
import glob
import xarray as xr
from tqdm import tqdm

# Add path to msm-os package:
sys.path.append('/dssgfs01/working/otooth/AtlantiS/jasmin_os/msm-os/src/')
from msm_os.object_store import ObjectStoreS3

# -- Define JASMIN S3 object store -- #
store_credentials_json = '/dssgfs01/working/otooth/AtlantiS/jasmin_os/credentials/jasmin_os_credentials.json'
obj_store = ObjectStoreS3(anon=False, store_credentials_json=store_credentials_json)
print('Completed: Initialised JASMIN S3 object store using user credentials.')

# -- Send GOSML Observations to JASMIN Object Store -- #
print('In Progress: Sending GOSML Observations to JASMIN Object Store...')

# Define path to GOSML obs.
ini_fpath = '/dssgfs01/scratch/otooth/npd_data/observations/GOSML/data/GOSML_mixed_layer_properties_mean.nc'

# Define bucket name:
bucket = 'npd-obs'
# Define path to destination zarr store:
dest = f"{bucket}/GOSML/GOSML_global_monthly_climatology_2000_2020"

# -- Write climatology to .zarr store -- #
# Define s3fs mapper to destination:
mapper = obj_store.get_mapper(dest)

# -- Import & Processing: Add metadata attributes -- #
ds = xr.open_dataset(ini_fpath)
# Add description attribute:
ds.attrs['description'] = 'A Global Ocean Surface Mixed Layer Statistical Monthly Climatology (GOSML) of mean depth, temperature, and salinity. Downloaded from https://www.pmel.noaa.gov/gosml/ on 17/12/2024. Transferred to JASMIN Object Store on 19/12/2024.'

# -- Write to JASMIN OS as .zarr store -- #
ds.to_zarr(mapper, mode='w', consolidated=True)

print('Completed: Sent GOSML Observations to JASMIN Object Store.')
