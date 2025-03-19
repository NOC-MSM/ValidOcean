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
import numpy as np
import xesmf as xe
import xarray as xr
from tqdm import tqdm

# -- Utility Functions -- #
def gc_distance(lat1 : xr.DataArray,
                lon1 : xr.DataArray,
                lat2 : xr.DataArray,
                lon2 : xr.DataArray
                ) -> xr.DataArray:
    """
    Calculate the Great-Circle distance between two sets of
    geographical points on the Earth's surface.

    Parameters:
    -----------
    lat1 : xr.DataArray
        Latitude of the first set of points (degrees).
    lon1 : xr.DataArray
        Longitude of the first set of points (degrees).
    lat2 : xr.DataArray
        Latitude of the second set of points (degrees).
    lon2 : xr.DataArray
        Longitude of the second set of points (degrees).
    
    Returns:
    --------
    dist : xr.DataArray
        Great-circle distance between the two sets
        of points (meters).
    
    """
    # Define the radius of the Earth in meters:
    re = 6371000

    # Convert latitudes and longitudes from degrees to radians:
    lon1, lat1, lon2, lat2 = map(np.deg2rad, [lon1, lat1, lon2, lat2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1

    # Calculate the great-circle distance between points:
    dist = (2*re*np.arcsin(np.sqrt(
            np.sin(dlat/2)**2 +
            (np.cos(lat1) *
            np.cos(lat2) *
            np.sin(dlon/2)**2)
            )))

    return dist

# -- Calculate HadISST global grid cell areas -- #
# Define regular global 1x1 degree grid:
ds_grid = xe.util.grid_global(1, 1)

# Calculate the great-circle distance between the lon/lat grid bounds:
dx = gc_distance(lat1=ds_grid['lat_b'].isel(x_b=0).values,
                 lon1=ds_grid['lon_b'].isel(x_b=0).values,
                 lat2=ds_grid['lat_b'].isel(x_b=1).values,
                 lon2=ds_grid['lon_b'].isel(x_b=1).values,
                 )

dy = gc_distance(lat1=ds_grid['lat_b'].isel(y_b=0).values,
                 lon1=ds_grid['lon_b'].isel(y_b=0).values,
                 lat2=ds_grid['lat_b'].isel(y_b=1).values,
                 lon2=ds_grid['lon_b'].isel(y_b=1).values,
                 )

# Calculate the distances at the center of each grid cell:
dx = (dx[:-1] + dx[1:]) / 2
dy = (dy[:-1] + dy[1:]) / 2

# Calculate the area of each grid cell:
dy_2d, dx_2d = np.meshgrid(dy, dx)

# -- Transfer HadISST Observations to JASMIN Object Store -- #
# Add path to msm-os package:
sys.path.append('/dssgfs01/working/otooth/AtlantiS/jasmin_os/msm-os/src/')
from msm_os.object_store import ObjectStoreS3

# -- Define JASMIN S3 object store -- #
store_credentials_json = '.../jasmin_os/credentials/jasmin_os_credentials.json'
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

# Fill missing values with NaNs:
ds_sst['sst'] = xr.where(cond=ds_sst['sst'] == -1000, x=np.nan, y=ds_sst['sst'])

# Concatenate SST and ice datasets:
ds = xr.merge([ds_sst, ds_ice])
ds = ds.rename_vars({'sic': 'siconc'})

# Add grid cell area:
ds['area'] = xr.DataArray(data=(dy_2d * dx_2d), dims=('latitude', 'longitude'))

# Add description attribute:
ds.attrs['description'] = 'Hadley Centre Sea Ice and Sea Surface Temperature data set (HadISST) is a unique combination of monthly globally-complete fields of sea surface temperature and sea ice concentration on a 1 degree latitude-longitude grid from 1870 to date. Fill values (-1000.0) in sea surface temperature have been masked to NaN values. Downloaded from https://www.metoffice.gov.uk/hadobs/hadisst/data/download.html on 19/03/2025. Transferred to JASMIN Object Store on 19/03/2025.'

# Rechunking for optimal read performance:
ds = ds.rename_dims({'latitude': 'lat', 'longitude': 'lon'})
ds = ds.chunk({'time': 60, 'lat': 180, 'lon': 360})

# -- Write to JASMIN OS as .zarr store -- #
ds.to_zarr(mapper, mode='w', consolidated=True)

print('Completed: Sent HadISST Observations to JASMIN Object Store.')
