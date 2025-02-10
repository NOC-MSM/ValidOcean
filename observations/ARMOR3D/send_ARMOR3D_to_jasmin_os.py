"""
send_ARMOR3D_to_jasmin_os.py

Description: Python script to send the ARMOR3D reprocessed ocean
analyses to the JASMIN Object Store.

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
store_credentials_json = '/dssgfs01/working/otooth/AtlantiS/jasmin_os/credentials/jasmin_os_credentials.json'
obj_store = ObjectStoreS3(anon=False, store_credentials_json=store_credentials_json)
print('Completed: Initialised JASMIN S3 object store using user credentials.')

# -- Send ARMOR3D Observations to JASMIN Object Store -- #
print('In Progress: Sending ARMOR3D Observations to JASMIN Object Store...')

# Define path to ARMOR3D obs.
ini_fpath = '/dssgfs01/scratch/otooth/npd_data/observations/ARMOR3D/data/*.zarr'
# Get sorted list of files:
files = glob.glob(ini_fpath)
files.sort()

# Define bucket name:
bucket = 'npd-obs'
# Define path to destination zarr store:
dest = f"{bucket}/ARMOR3D/ARMOR3D_RP_global_monthly_1993_2022"

# -- Iterate over yearly .zarr stores -- #
for n, file in tqdm(enumerate(files)):
    # Define s3fs mapper to destination:
    mapper = obj_store.get_mapper(dest)

    # -- Import & Processing: Add metadata attributes -- #
    ds = xr.open_zarr(file)
    # Add description attribute:
    ds.attrs['description'] = 'ARMOR3D REP CMEMS December 2020 Release downloaded from Multi Observation Global Ocean ARMOR3D L4 analysis https://doi.org/10.48670/moi-00052 on 17/12/2024. Transferred to JASMIN Object Store on 18/12/2024.'

    # -- Write to JASMIN OS as .zarr store -- #
    if n == 0:
        ds.to_zarr(mapper, mode='w', consolidated=True)
    else:
        ds.to_zarr(mapper, append_dim='time', consolidated=True)

print('Completed: Sent ARMOR3D Observations to JASMIN Object Store.')
