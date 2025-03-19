"""
send_GlobCurrent_to_jasmin_os.py

Description: Python script to send the GlobCurrent monthly reprocessed
surface and 15m ocean current data to the JASMIN Object Store.

Created by: Ollie Tooth (oliver.tooth@noc.ac.uk)
Created on: 2025-02-21
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

# -- Send GlobCurrent Observations to JASMIN Object Store -- #
print('In Progress: Sending GlobCurrent Observations to JASMIN Object Store...')

# Define path to GlobCurrent obs.
ini_fpath = '/dssgfs01/scratch/otooth/npd_data/observations/GlobCurrent/data/*.zarr'
# Get sorted list of files:
files = glob.glob(ini_fpath)
files.sort()

# Define bucket name:
bucket = 'npd-obs'
# Define path to destination zarr store:
dest = f"{bucket}/GlobCurrent/GlobCurrent_RP_global_monthly_1993_2023"

# -- Iterate over yearly .zarr stores -- #
for n, file in tqdm(enumerate(files)):
    # Define s3fs mapper to destination:
    mapper = obj_store.get_mapper(dest)

    # -- Import & Processing: Add metadata attributes -- #
    ds = xr.open_zarr(file)
    # Add description attribute:
    ds.attrs['description'] = 'GlobCurrent reprocessed monthly mean of the total, tide, Ekman and geostrophic currents, together with their respective monthly uncertainties (version 2024-11). Downloaded from https://doi.org/10.48670/mds-00327 on 21/02/2025. Transferred to JASMIN Object Store on 21/02/2025.'

    # -- Write to JASMIN OS as .zarr store -- #
    if n == 0:
        ds.to_zarr(mapper, mode='w', consolidated=True)
    else:
        ds.to_zarr(mapper, append_dim='time', consolidated=True)

print('Completed: Sent GlobCurrent Observations to JASMIN Object Store.')
