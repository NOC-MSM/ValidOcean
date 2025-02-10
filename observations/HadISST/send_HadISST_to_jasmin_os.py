"""
send_HadISST_to_jasmin_os.py

Description: Python script to send the HadISST global sea
surface temperature and sea ice fraction data to the JASMIN
Object Store.

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

# -- Send HadISST Observations to JASMIN Object Store -- #
print('In Progress: Sending HadISST Observations to JASMIN Object Store...')

# Define path to HadISST obs.
ini_fpath = '/dssgfs01/scratch/otooth/npd_data/observations/HadISST/data/*.nc'
# Get sorted list of files:
files = glob.glob(ini_fpath)
files.sort()

# Define bucket name:
bucket = 'npd-obs'
# Define path to destination zarr store:
dest = f"{bucket}/HadISST/HadISST_global_monthly_1870_2024"

# -- Write climatology to .zarr store -- #
# Define s3fs mapper to destination:
mapper = obj_store.get_mapper(dest)

# -- Import & Processing: Add metadata attributes -- #
ds_ice = xr.open_dataset(files[0])
ds_sst = xr.open_dataset(files[1])

# Concatenate SST and ice datasets:
ds = xr.merge([ds_sst, ds_ice])

# Add description attribute:
ds.attrs['description'] = 'Hadley Centre Sea Ice and Sea Surface Temperature data set (HadISST) is a unique combination of monthly globally-complete fields of SST and sea ice concentration on a 1 degree latitude-longitude grid from 1870 to date. Downloaded from https://www.metoffice.gov.uk/hadobs/hadisst/data/download.html on 17/12/2024. Transferred to JASMIN Object Store on 19/12/2024.'

# -- Write to JASMIN OS as .zarr store -- #
ds.to_zarr(mapper, mode='w', consolidated=True)

print('Completed: Sent HadISST Observations to JASMIN Object Store.')
