"""
send_GO-SHIP_to_jasmin_os.py

Description: Python script to send the GO-SHIP repeat
hydrographic section gridded data to the JASMIN Object
Store.

Python Environment: env_jasmin_os [conda]

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

# -- Send GO-SHIP Observations to JASMIN Object Store -- #
print('In Progress: Sending GO-SHIP Observations to JASMIN Object Store...')

# Define path to GO-SHIP obs.
ini_fpath = '/dssgfs01/scratch/otooth/npd_data/observations/GO-SHIP/data/*.nc'
files = glob.glob(ini_fpath)

# Define bucket name:
bucket = 'npd-obs'

# Iterate over observation files:
for file in files:
    # Define path to destination zarr store:
    filename = file.split('/')[-1]
    dest = f"{bucket}/GO-SHIP/{filename.replace('.nc', '')}"
    print(f'--> Sending: {dest} ...')

    # -- Write climatology to .zarr store -- #
    # Define s3fs mapper to destination:
    mapper = obj_store.get_mapper(dest)

    # -- Import & Processing: Add metadata attributes -- #
    ds = xr.open_dataset(file)

    # -- Write to JASMIN OS as .zarr store -- #
    ds.to_zarr(mapper, mode='w', consolidated=True)

print('Completed: Sent GO-SHIP Observations to JASMIN Object Store.')
