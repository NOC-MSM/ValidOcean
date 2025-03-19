"""
send_Kogur_to_jasmin_os.py

Description: Python script to send the Kogur hydrographic
section (2011-2012) data to the JASMIN Object Store.

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
store_credentials_json = '.../jasmin_os/credentials/jasmin_os_credentials.json'
obj_store = ObjectStoreS3(anon=False, store_credentials_json=store_credentials_json)
print('Completed: Initialised JASMIN S3 object store using user credentials.')

# -- Send Kogur Observations to JASMIN Object Store -- #
print('In Progress: Sending Kogur Observations to JASMIN Object Store...')

# Define path to Kogur obs.
ini_fpath = '/dssgfs01/scratch/otooth/npd_data/observations/Kogur/data/nsv_kogur_section_2011_2012.nc'

# Define bucket name:
bucket = 'npd-obs'
# Define path to destination zarr store:
dest = f"{bucket}/Kogur/Kogur_whoi_section_2011_2012"

# -- Write climatology to .zarr store -- #
# Define s3fs mapper to destination:
mapper = obj_store.get_mapper(dest)

# -- Import & Processing: Add metadata attributes -- #
ds = xr.open_dataset(ini_fpath)

# Add description attribute:
ds.attrs['description'] = 'Kögur hydrographic section data from 2011-2012 survey conducted by WHOI. Contact Benjamin Harden (benharden27@gmail.com) for more details. Downloaded from https://gws-access.jasmin.ac.uk/public/jmmp/NORVAL/ using nordic-seas-validation package on 19/12/2024. Transferred to JASMIN Object Store on 19/12/2024.'

# -- Write to JASMIN OS as .zarr store -- #
ds.to_zarr(mapper, mode='w', consolidated=True)

print('Completed: Sent Kogur Observations to JASMIN Object Store.')
