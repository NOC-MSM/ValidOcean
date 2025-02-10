"""
send_Latrabjarg_to_jasmin_os.py

Description: Python script to send the Latrabjarg hydrographic
section climatology (1990-2012) data to the JASMIN Object Store.

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

# -- Send Latrabjarg Observations to JASMIN Object Store -- #
print('In Progress: Sending Latrabjarg Observations to JASMIN Object Store...')

# Define path to Latrabjarg obs.
ini_fpath = '/dssgfs01/scratch/otooth/npd_data/observations/Latrabjarg/data/nsv_latrabjarg_section_climatology_1990_2012.nc'

# Define bucket name:
bucket = 'npd-obs'
# Define path to destination zarr store:
dest = f"{bucket}/Latrabjarg/Latrabjarg_section_climatology_1990_2012"

# -- Write climatology to .zarr store -- #
# Define s3fs mapper to destination:
mapper = obj_store.get_mapper(dest)

# -- Import & Processing: Add metadata attributes -- #
ds = xr.open_dataset(ini_fpath)

# Add description attribute:
ds.attrs['description'] = 'Látrabjarg hydrographic section climatology from 1990-2012 surveys. See https://doi.org/10.1002/2016JC012007 for more details. Downloaded from https://gws-access.jasmin.ac.uk/public/jmmp/NORVAL/ using nordic-seas-validation package on 19/12/2024. Transferred to JASMIN Object Store on 19/12/2024.'

# -- Write to JASMIN OS as .zarr store -- #
ds.to_zarr(mapper, mode='w', consolidated=True)

print('Completed: Sent Latrabjarg Observations to JASMIN Object Store.')
