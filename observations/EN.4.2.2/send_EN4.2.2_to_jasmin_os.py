"""
send_EN4.2.2_to_jasmin_os.py

Description: Python script to send the EN4.2.2 global temperature
and salinity data to the JASMIN Object Store.

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

# -- Send EN4.2.2 Observations to JASMIN Object Store -- #
print('In Progress: Sending EN4.2.2 Observations to JASMIN Object Store...')

# Define path to EN4 obs.
ini_fpath = '/dssgfs01/scratch/otooth/npd_data/observations/EN.4.2.2/data/*.nc'
# Get sorted list of files:
files = glob.glob(ini_fpath)
files.sort()

# Define bucket name:
bucket = 'npd-obs'
# Define path to destination zarr store:
dest = f"{bucket}/EN4/EN4.2.2_global_monthly_1950_2024"

# -- Iterate over yearly .zarr stores -- #
for n, file in tqdm(enumerate(files)):
    # Define s3fs mapper to destination:
    mapper = obj_store.get_mapper(dest)

    # -- Import & Processing: Add metadata attributes -- #
    ds = xr.open_dataset(file).rename({'lon': 'longitude', 'lat': 'latitude'})

    # Rechunk for optimal read performance:
    ds = ds.chunk({'time': 1, 'depth': ds['depth'].size, 'latitude': ds['latitude'].size, 'longitude': ds['longitude'].size})

    # Add description attribute:
    ds.attrs['description'] = 'Met Office Hadley Centre EN4.2.2 objective analyses - Gouretski and Reseghetti (2010) XBT corrections and Gouretski and Cheng (2020) MBT corrections. Downloaded from https://www.metoffice.gov.uk/hadobs/en4/download-en4-2-2.html on 17-19/12/2024. Transferred to JASMIN Object Store on 19/12/2024.'

    # -- Write to JASMIN OS as .zarr store -- #
    if n == 0:
        ds.to_zarr(mapper, mode='w', consolidated=True)
    else:
        ds.to_zarr(mapper, append_dim='time', consolidated=True)

print('Completed: Sent EN4.2.2 Observations to JASMIN Object Store.')
