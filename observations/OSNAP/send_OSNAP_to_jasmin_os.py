"""
send_OSNAP_to_jasmin_os.py

Description: Python script to send the OSNAP array
hydrography, volume transports and stream functions 
to the JASMIN Object Store.

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
store_credentials_json = '.../jasmin_os/credentials/jasmin_os_credentials.json'
obj_store = ObjectStoreS3(anon=False, store_credentials_json=store_credentials_json)
print('Completed: Initialised JASMIN S3 object store using user credentials.')

# -- Send OSNAP Observations to JASMIN Object Store -- #
print('In Progress: Sending OSNAP Gridded TSV Observations to JASMIN Object Store...')

# Define bucket name:
bucket = 'npd-obs'

# Define path to OSNAP obs.
ini_fpath = '/dssgfs01/scratch/otooth/npd_data/observations/OSNAP/data/OSNAP_Gridded_TSV_201408_202006_2023.nc'
# Define path to destination zarr store:
dest = f"{bucket}/OSNAP/tsv_gridded_2014_2020"

# -- Write climatology to .zarr store -- #
# Define s3fs mapper to destination:
mapper = obj_store.get_mapper(dest)

# -- Import & Processing: Add metadata attributes -- #
ds = xr.open_dataset(ini_fpath)

# Add description attribute:
ds.attrs['description'] = 'OSNAP gridded 30-day mean potential temperature, salinity and velocity data 2014-2020. Properties and velocities are mapped onto a regular 0.25 degree (longitude) grid. OSNAP data were collected and made freely available by the OSNAP (Overturning in the Subpolar North Atlantic Program) project and all the national programs that contribute to it (www.o-snap.org). Downloaded from https://doi.org/10.35090/gatech/70342 on 23/10/2023. Transferred to JASMIN Object Store on 03/01/2025.'

# -- Write to JASMIN OS as .zarr store -- #
ds.to_zarr(mapper, mode='w', consolidated=True)
print('Completed: Sent OSNAP Gridded TSV Observations to JASMIN Object Store.')

# -- Send OSNAP MOC Transports to JASMIN Object Store -- #
print('In Progress: Sending OSNAP MOC Transports to JASMIN Object Store...')

# Define path to OSNAP obs.
ini_fpath = '/dssgfs01/scratch/otooth/npd_data/observations/OSNAP/data/OSNAP_MOC_MHT_MFT_TimeSeries_201408_202006_2023.nc'
# Define path to destination zarr store:
dest = f"{bucket}/OSNAP/moc_transports_2014_2020"

# -- Write climatology to .zarr store -- #
# Define s3fs mapper to destination:
mapper = obj_store.get_mapper(dest)

# -- Import & Processing: Add metadata attributes -- #
ds = xr.open_dataset(ini_fpath)

# Add description attribute:
ds.attrs['description'] = 'OSNAP 30-day mean OSNAP MOC MHT MFT 2014-2020 time series. OSNAP data were collected and made freely available by the OSNAP (Overturning in the Subpolar North Atlantic Program) project and all the national programs that contribute to it (www.o-snap.org). Downloaded from https://doi.org/10.35090/gatech/70342 on 23/10/2023. Transferred to JASMIN Object Store on 03/01/2025.'

# -- Write to JASMIN OS as .zarr store -- #
ds.to_zarr(mapper, mode='w', consolidated=True)
print('Completed: Sent OSNAP MOC Transports to JASMIN Object Store.')

# -- Send OSNAP MOC Transports to JASMIN Object Store -- #
print('In Progress: Sending OSNAP MOC Stream Functions to JASMIN Object Store...')

# Define path to OSNAP obs.
ini_fpath = '/dssgfs01/scratch/otooth/npd_data/observations/OSNAP/data/OSNAP_Streamfunction_201408_202006_2023.nc'
# Define path to destination zarr store:
dest = f"{bucket}/OSNAP/moc_streamfunctions_2014_2020"

# -- Write climatology to .zarr store -- #
# Define s3fs mapper to destination:
mapper = obj_store.get_mapper(dest)

# -- Import & Processing: Add metadata attributes -- #
ds = xr.open_dataset(ini_fpath)

# Add description attribute:
ds.attrs['description'] = 'OSNAP 30-day mean OSNAP diapycnal overturning stream functions 2014-2020. OSNAP data were collected and made freely available by the OSNAP (Overturning in the Subpolar North Atlantic Program) project and all the national programs that contribute to it (www.o-snap.org). Downloaded from https://doi.org/10.35090/gatech/70342 on 23/10/2023. Transferred to JASMIN Object Store on 03/01/2025.'

# -- Write to JASMIN OS as .zarr store -- #
ds.to_zarr(mapper, mode='w', consolidated=True)
print('Completed: Sent OSNAP MOC Stream Functions to JASMIN Object Store.')
