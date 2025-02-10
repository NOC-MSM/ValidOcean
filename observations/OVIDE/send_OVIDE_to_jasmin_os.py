"""
send_OVIDE_to_jasmin_os.py

Description: Python script to send the OVIDE hydrographic
section climatology (Daniualt et al., 2016) data to the
JASMIN Object Store.

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

# -- Send OVIDE Observations to JASMIN Object Store -- #
print('In Progress: Sending OVIDE Observations to JASMIN Object Store...')

# Define path to OVIDE obs.
ini_fpath = '/dssgfs01/scratch/otooth/npd_data/observations/OVIDE/data/nsv_ovide_section_climatology_2016.nc'

# Define bucket name:
bucket = 'npd-obs'
# Define path to destination zarr store:
dest = f"{bucket}/OVIDE/OVIDE_section_climatology_2002_2012"

# -- Write climatology to .zarr store -- #
# Define s3fs mapper to destination:
mapper = obj_store.get_mapper(dest)

# -- Import & Processing: Add metadata attributes -- #
ds = xr.open_dataset(ini_fpath)

# Add description attribute:
ds.attrs['description'] = 'Gridded hydrographic and transport data for the biennial Go-Ship A25 Greenland - Portugal OVIDE section from 2002 to 2012. Properties and transports are mapped on a 7km x 1m grid. See http://doi.org/10.1016/j.pocean.2016.06.007 for more details. Downloaded from https://gws-access.jasmin.ac.uk/public/jmmp/NORVAL/ [https://doi.org/10.17882/46446] using nordic-seas-validation package on 19/12/2024. Transferred to JASMIN Object Store on 19/12/2024.'

# -- Write to JASMIN OS as .zarr store -- #
ds.to_zarr(mapper, mode='w', consolidated=True)

print('Completed: Sent OVIDE Observations to JASMIN Object Store.')
