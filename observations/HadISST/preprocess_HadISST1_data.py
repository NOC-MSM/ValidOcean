"""
preprocess_HadISST1_data.py

Description: Python script to pre-process the HadISST global sea
surface temperature and sea ice fraction data.

Created by: Ollie Tooth (oliver.tooth@noc.ac.uk)
Created on: 2024-12-19
"""
# == Import Python packages == #
import glob
import numpy as np
import xesmf as xe
import xarray as xr

# == Utility Functions == #
def compute_gc_distance(lat1 : xr.DataArray,
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


def add_grid_area(ds : xr.Dataset, resolution : float = 1) -> xr.Dataset:
    """
    Calculate the area of each horizontal grid cell
    for a regular rectilinear gridded dataset.
    
    Calculates the Great Circle distance between the
    grid cell bounds & calculates the product of dx
    and dy averaged at the grid cell center.

    Parameters:
    -----------
    ds : xr.Dataset
        Dataset containing rectilinear grid defined using
        `lon` and `lat` coordinates.
    resolution : float, default=1
        Resolution of the grid in degrees.
        Used to define the grid cell bounds.

    Returns:
    --------
    xr.Dataset
        Dataset with grid cell area (m^2) variable.
    """
    # Define regular global n-degree grid, including bounds:
    ds_grid = xe.util.grid_global(resolution, resolution)

    # Calculate the Great Circle distance between the lon/lat grid bounds:
    dx = compute_gc_distance(lat1=ds_grid['lat_b'].isel(x_b=0).values,
                            lon1=ds_grid['lon_b'].isel(x_b=0).values,
                            lat2=ds_grid['lat_b'].isel(x_b=1).values,
                            lon2=ds_grid['lon_b'].isel(x_b=1).values,
                            )

    dy = compute_gc_distance(lat1=ds_grid['lat_b'].isel(y_b=0).values,
                            lon1=ds_grid['lon_b'].isel(y_b=0).values,
                            lat2=ds_grid['lat_b'].isel(y_b=1).values,
                            lon2=ds_grid['lon_b'].isel(y_b=1).values,
                            )

    # Determine the average distances across the center of each grid cell:
    dx = (dx[:-1] + dx[1:]) / 2
    dy = (dy[:-1] + dy[1:]) / 2
    # Calculate the area from the center of the grid:
    dy_2d, dx_2d = np.meshgrid(dy, dx)

    # Add grid cell area to Dataset:
    ds['area'] = xr.DataArray(data=(dy_2d * dx_2d), dims=('latitude', 'longitude'))

    return ds


# == Import & Pre-processing: Add metadata attributes == #
# Open HadISST obs as Datasets:
ini_fpath = '/dssgfs01/scratch/otooth/npd_data/observations/HadISST'
files = sorted(glob.glob(f"{ini_fpath}/*nc"))
ds_ice = xr.open_dataset(files[0])
ds_sst = xr.open_dataset(files[1])

# Fill missing values with NaNs:
ds_sst['sst'] = xr.where(cond=ds_sst['sst'] == -1000, x=np.nan, y=ds_sst['sst'])

# Concatenate SST and sea ice Datasets:
ds = xr.merge([ds_sst, ds_ice])
ds = ds.rename_vars({'sic': 'siconc'})

# Add grid cell area to Dataset:
ds = add_grid_area(ds=ds, resolution=1)

# == Write pre-processed HadISST data to NetCDF == #
ds.to_netcdf(f"{ini_fpath}/HadISST_global_monthly_1870_2025.nc")
