"""
send_CCI_to_jasmin_os.py

Description: Python script to send the CCI sea surface temperatures
and sea ice fraction to the JASMIN Object Store.

Created by: Ollie Tooth (oliver.tooth@noc.ac.uk)
Created on: 2024-12-18
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
store_credentials_json = '.../jasmin_os/credentials/jasmin_os_credentials.json'
obj_store = ObjectStoreS3(anon=False, store_credentials_json=store_credentials_json)
print('Completed: Initialised JASMIN S3 object store using user credentials.')

# -- Send CCI Climatology Observations to JASMIN Object Store -- #
print('In Progress: Sending CCI Climatology Observations to JASMIN Object Store...')

# Define path to CCI obs.
ini_fpath = '/dssgfs01/scratch/otooth/npd_data/observations/CCI/data/ESACCI-L4-SST-climatology-GLOB_CDR3.0.nc'

# Define bucket name:
bucket = 'npd-obs'
# Define path to destination zarr store:
dest = f"{bucket}/CCI/ESACCI-v3.0-SST_global_climatology_1991_2020"

# Define s3fs mapper to destination:
mapper = obj_store.get_mapper(dest)

# -- Import & Processing: Add metadata attributes -- #
ds = xr.open_dataset(ini_fpath)
# Add description attribute:
ds.attrs['description'] = 'Time-average of the ESA CCI Sea Surface Temperature CDR v3.0 product providing daily climatological mean sea surface temperature (SST) and sea ice fraction on a global 0.05° latitude-longitude grid, derived from the SST CCI analysis data for the period 1991 to 2020 (30 years). Downloaded from https://dx.doi.org/10.5285/62800d3d2227449085b430b503d36b01 on 17/12/2024. Transferred to JASMIN Object Store on 18/12/2024.'

# -- Write to JASMIN OS as .zarr store -- #
ds.to_zarr(mapper, mode='w', consolidated=True)

print('Completed: Sent CCI Climatology Observations to JASMIN Object Store.')

# -- Send CCI Seasonal Climatology Observations to JASMIN Object Store -- #
print('In Progress: Sending CCI Seasonal Climatology Observations to JASMIN Object Store...')

# Define path to CCI obs.
ini_fpath = '/dssgfs01/scratch/otooth/npd_data/observations/CCI/data/ESACCI-L4-SST-seasonal-climatology-GLOB_CDR3.0.nc'

# Define path to destination zarr store:
dest = f"{bucket}/CCI/ESACCI-v3.0-SST_global_seasonal_climatology_1991_2020"

# Define s3fs mapper to destination:
mapper = obj_store.get_mapper(dest)

# -- Import & Processing: Add metadata attributes -- #
ds = xr.open_dataset(ini_fpath)
# Add description attribute:
ds.attrs['description'] = 'Seasonal averages of the ESA CCI Sea Surface Temperature CDR v3.0 product providing daily climatological mean sea surface temperature (SST) and sea ice fraction on a global 0.05° latitude-longitude grid, derived from the SST CCI analysis data for the period 1991 to 2020 (30 years). Downloaded from https://dx.doi.org/10.5285/62800d3d2227449085b430b503d36b01 on 17/12/2024. Transferred to JASMIN Object Store on 18/12/2024.'

# -- Write to JASMIN OS as .zarr store -- #
ds.to_zarr(mapper, mode='w', consolidated=True)

print('Completed: Sent CCI Seasonal Climatology Observations to JASMIN Object Store.')

# -- Send CCI Monthly Climatology Observations to JASMIN Object Store -- #
print('In Progress: Sending CCI Monthly Climatology Observations to JASMIN Object Store...')

# Define path to CCI obs.
ini_fpath = '/dssgfs01/scratch/otooth/npd_data/observations/CCI/data/ESACCI-L4-SST-monthly-climatology-GLOB_CDR3.0.nc'

# Define path to destination zarr store:
dest = f"{bucket}/CCI/ESACCI-v3.0-SST_global_monthly_climatology_1991_2020"

# Define s3fs mapper to destination:
mapper = obj_store.get_mapper(dest)

# -- Import & Processing: Add metadata attributes -- #
ds = xr.open_dataset(ini_fpath)
# Add description attribute:
ds.attrs['description'] = 'Monthly averages of the ESA CCI Sea Surface Temperature CDR v3.0 product providing daily climatological mean sea surface temperature (SST) and sea ice fraction on a global 0.05° latitude-longitude grid, derived from the SST CCI analysis data for the period 1991 to 2020 (30 years). Downloaded from https://dx.doi.org/10.5285/62800d3d2227449085b430b503d36b01 on 17/12/2024. Transferred to JASMIN Object Store on 18/12/2024.'

# -- Write to JASMIN OS as .zarr store -- #
ds.to_zarr(mapper, mode='w', consolidated=True)

print('Completed: Sent CCI Monthly Climatology Observations to JASMIN Object Store.')
