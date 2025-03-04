"""
send_NSIDC_to_jasmin_os.py

Description: Python script to send the NSIDC Sea Ice Index dataset
to the JASMIN Object Store.

Created by: Ollie Tooth (oliver.tooth@noc.ac.uk)
Created on: 2025-02-21
"""
# -- Import Python packages -- #
import sys
import glob
import xarray as xr

# Add path to msm-os package:
sys.path.append('/dssgfs01/working/otooth/AtlantiS/jasmin_os/msm-os/src/')
from msm_os.object_store import ObjectStoreS3

# -- Define JASMIN S3 object store -- #
store_credentials_json = '/dssgfs01/working/otooth/AtlantiS/jasmin_os/credentials/jasmin_os_credentials.json'
obj_store = ObjectStoreS3(anon=False, store_credentials_json=store_credentials_json)
print('Completed: Initialised JASMIN S3 object store using user credentials.')

# -- Send NSIDC Observations to JASMIN Object Store -- #
print('In Progress: Sending NSIDC Sea Ice Index Arctic Observations to JASMIN Object Store...')

# Define path to NSIDC obs.
ini_fpath = '/dssgfs01/scratch/otooth/npd_data/observations/NSIDC/data/NSIDC_Sea_Ice_Index_v3_Arctic_combined_1978_2025.nc'

# Define bucket name:
bucket = 'npd-obs'
# Define path to destination zarr store:
dest = f"{bucket}/NSIDC/NSIDC_Sea_Ice_Index_v3_Arctic_1978_2025"

# Define s3fs mapper to destination:
mapper = obj_store.get_mapper(dest)

# -- Import & Processing: Add metadata attributes -- #
ds = xr.open_dataset(ini_fpath)

# Rechunk for optimal read performance:
ds = ds.chunk({'time': 12, 'x': ds['x'].size, 'y': ds['y'].size})

# Add description attribute:
ds.attrs['description'] = 'National Snow & Ice Data Centre (NSIDC) Sea Ice Index version 3: Arctic sea ice extent, concentration and total sea ice area (computed) data downloaded from https://doi.org/10.7265/N5K072F8 on 21/02/2025. Transferred to JASMIN Object Store on 21/02/2025.'

# -- Write to JASMIN OS as .zarr store -- #
ds.to_zarr(mapper, mode='w', consolidated=True)

print('Completed: Sent NSIDC Sea Ice Index Arctic Observations to JASMIN Object Store.')

# -- Send NSIDC Observations to JASMIN Object Store -- #
print('In Progress: Sending NSIDC Sea Ice Index Antarctic Observations to JASMIN Object Store...')

# Define path to NSIDC obs.
ini_fpath = '/dssgfs01/scratch/otooth/npd_data/observations/NSIDC/data/NSIDC_Sea_Ice_Index_v3_Antarctic_combined_1978_2025.nc'

# Define path to destination zarr store:
dest = f"{bucket}/NSIDC/NSIDC_Sea_Ice_Index_v3_Antarctic_1978_2025"

# Define s3fs mapper to destination:
mapper = obj_store.get_mapper(dest)

# -- Import & Processing: Add metadata attributes -- #
ds = xr.open_dataset(ini_fpath)

# Rechunk for optimal read performance:
ds = ds.chunk({'time': 12, 'x': ds['x'].size, 'y': ds['y'].size})

# Add description attribute:
ds.attrs['description'] = 'National Snow & Ice Data Centre (NSIDC) Sea Ice Index version 3: Antarctic sea ice extent, concentration and total sea ice area (computed) data downloaded from https://doi.org/10.7265/N5K072F8 on 21/02/2025. Transferred to JASMIN Object Store on 21/02/2025.'

# -- Write to JASMIN OS as .zarr store -- #
ds.to_zarr(mapper, mode='w', consolidated=True)

print('Completed: Sent NSIDC Sea Ice Index Antarctic Observations to JASMIN Object Store.')
