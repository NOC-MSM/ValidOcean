"""
send_LOPS_MLD_to_jasmin_os.py

Description: Python script to send the LOPS / Ifremer
Mixed Layer Depth climatology (1990-2012) data to the
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

# -- Send LOPS-MLD Observations to JASMIN Object Store -- #
print('In Progress: Sending LOPS-MLD Observations to JASMIN Object Store...')

# Define path to LOPS-MLD obs.
ini_fpath = '/dssgfs01/scratch/otooth/npd_data/observations/LOPS-MLD/data/mld_dr003_ref10m_v2023.nc'

# Define bucket name:
bucket = 'npd-obs'
# Define path to destination zarr store:
dest = f"{bucket}/LOPS-MLD/LOPS-MLD_v2023_global_monthly_climatology"

# -- Write climatology to .zarr store -- #
# Define s3fs mapper to destination:
mapper = obj_store.get_mapper(dest)

# -- Import & Processing: Add metadata attributes -- #
ds = xr.open_dataset(ini_fpath)

# Add description attribute:
ds.attrs['description'] = 'Monthly climatology (i.e. 12 months) of ocean surface Mixed Layer Depth (MLD) over the global ocean, at 1 degree x 1 degree spatial resolution computed with a density threshold criterion of 0.03 kg/m3 from 10 m depth value. See https://cerweb.ifremer.fr/mld for more details. Downloaded from https://doi.org/10.17882/91774 using on 18/12/2024. Transferred to JASMIN Object Store on 19/12/2024.'

# -- Write to JASMIN OS as .zarr store -- #
ds.to_zarr(mapper, mode='w', consolidated=True)

print('Completed: Sent LOPS-MLD Observations to JASMIN Object Store.')
