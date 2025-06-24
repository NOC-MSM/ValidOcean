"""
preprocess.py

Description: This module contains functions to preprocess
ocean general circulation models outputs & observations.

Author: Ollie Tooth (oliver.tooth@noc.ac.uk)
"""

# -- Import required packages -- #
import warnings
import numpy as np
import xarray as xr

# -- Utility Functions -- #
def _get_spatial_bounds(lon: xr.DataArray,
                        lat: xr.DataArray,
                        depth: xr.DataArray | None = None
                        ) -> tuple[tuple[float, float], tuple[float, float], tuple[float, float] | None]:
    """
    Get the spatial bounds of a given model or observational
    domain rounded to nearest largest integer.

    Parameters
    ----------
    lon : xarray.DataArray
        DataArray containing the longitudes of the domain.
        Longitudes must be in the range [-180, 180].
    lat : xarray.DataArray
        DataArray containing the latitudes of the domain.
        Latitudes must be in the range [-90, 90].
    depth : xarray.DataArray, optional
        DataArray containing the depths of the domain.
        Depths must be in the range [0, 11050] m.

    Returns
    -------
    tuple[tuple[float, float], tuple[float, float], tuple[float, float] | None]
        Tuple of tuples containing the spatial bounds (float) of the domain
        in the form ((lon_min, lon_max), (lat_min, lat_max), (depth_min, depth_max)).
    """
    # -- Verify Inputs -- #
    if not isinstance(lon, xr.DataArray):
        raise TypeError("``lon`` must be an xarray DataArray.")
    if not isinstance(lat, xr.DataArray):
        raise TypeError("``lat`` must be an xarray DataArray.")
    if depth is not None:
        if not isinstance(depth, xr.DataArray):
            raise TypeError("``depth`` must be an xarray DataArray.")
    
    # -- Compute Spatial Bounds -- #
    lon_bounds = (np.floor(lon.min().item()), np.ceil(lon.max().item()))
    if (lon_bounds[0] < -180) or (lon_bounds[1] > 180):
        raise ValueError("``lon`` must be in the range [-180, 180].")

    lat_bounds = (np.floor(lat.min().item()), np.ceil(lat.max().item()))
    if (lat_bounds[0] < -90) or (lat_bounds[1] > 90):
        raise ValueError("``lat`` must be in the range [-90, 90].")
    
    if depth is not None:
        depth_bounds = (np.floor(depth.min().item()), np.ceil(depth.max().item()))
    else:
        depth_bounds = None
    
    return lon_bounds, lat_bounds, depth_bounds


def _transform_longitudes(data: xr.DataArray) -> xr.DataArray:
    """
    Transform longitudes in a given xarray DataArray
    to the range [-180, 180].

    Parameters
    ----------
    data : xarray.DataArray
        DataArray containing longitudes variable, ``lon``.

    Returns
    -------
    xarray.DataArray
        DataArray containing longitudes transformed to
        the range [-180, 180].
    """
    # -- Verify Inputs -- #
    if not isinstance(data, xr.DataArray):
        raise TypeError("``data`` must be an xarray DataArray.")
    if 'lon' not in data.dims:
        raise ValueError("``data`` must contain variable ``lon``.")
    
    # -- Transform Longitudes -- #:
    data['lon'] = xr.where(data['lon'] > 180, data['lon'] - 360, data['lon'])
    data = data.sortby('lon', ascending=True)

    return data

def _apply_time_bounds(data : xr.DataArray,
                       time_bounds : slice,
                       is_obs : bool = True,
                       ) -> xr.DataArray:
    """
    Subset a given xarray DataArray according to the
    specified time bounds.

    Parameters
    ----------
    data : xarray.DataArray
        DataArray containing the variable to subset.
    time_bounds : slice
        Time bounds to subset the data. Must be a slice
        object with start and stop datetime strings.
    is_obs : bool, default: False
        Flag to indicate if the data is from an ocean
        observations dataset. Default is True.
    
    Returns
    -------
    xarray.DataArray
        DataArray containing the variable subset to the
        specified time bounds.
    """
    # -- Verify Inputs -- #
    if not isinstance(data, xr.DataArray):
        raise TypeError("``data`` must be an xarray DataArray.")
    if 'time' not in data.dims:
        raise ValueError("``data`` must contain variable ``time``.")
    if not np.issubdtype(data.time.dtype, np.datetime64):
        raise TypeError("variable ``time`` dtype must be datetime64.")
    if not isinstance(time_bounds, slice):
        raise TypeError("``time_bounds`` must be a slice.")
    if not isinstance(time_bounds.start, str):
        raise TypeError("``time_bounds.start`` must be a datetime string (e.g., 'YYYY-MM').")
    if not isinstance(time_bounds.stop, str):
        raise TypeError("``time_bounds.stop`` must be a datetime string (e.g., 'YYYY-MM')")
    if not isinstance(is_obs, bool):
        raise TypeError("``is_obs`` flag must be a boolean.")
    
    #  -- Raise Warning if Bounds Outside Time Series -- #
    if (np.datetime64(time_bounds.start) < data.time.min().item()) | (np.datetime64(time_bounds.stop) > data.time.max().item()):
        if is_obs:
            data_type = 'observations'
        else:
            data_type = 'model'

        warning_message = f"time_bounds {time_bounds.start} - {time_bounds.stop} are outside the range of available {data_type} data {data.time.min().values.astype('datetime64[D]')} - {data.time.max().values.astype('datetime64[D]')}."
        warnings.warn(warning_message, RuntimeWarning)
    
    # -- Subset Data -- #
    data = data.sel(time=time_bounds)

    return data


def _apply_geographic_bounds(data : xr.DataArray,
                             lon_bounds : tuple[float],
                             lat_bounds : tuple[float],
                             is_obs : bool = True,
                             ) -> xr.DataArray:
    """
    Subset a given xarray DataArray according to the
    specified geographic bounds (lon, lat).

    Parameters
    ----------
    data : xarray.DataArray
        DataArray containing the variable to subset.
    lon_bounds : tuple[float]
        Longitude bounds to subset the data. Must be a
        tuple with minimum and maximum values.
    lat_bounds : tuple[float]
        Latitude bounds to subset the data. Must be a
        tuple with minimum and maximum values.
    is_obs : bool, default: False
        Flag to indicate if the data is from an ocean
        observations dataset. Default is True.
    
    Returns
    -------
    xarray.DataArray
        DataArray containing variable subset to the
        specified geographic bounds.
    """
    # -- Verify Inputs -- #
    if not isinstance(data, xr.DataArray):
        raise TypeError("``data`` must be an xarray DataArray.")
    if 'lon' not in data.coords or 'lat' not in data.coords:
        raise ValueError("``data`` must contain coordinates ``lon`` and ``lat``.")
    if not isinstance(lon_bounds, tuple):
        raise TypeError("``lon_bounds`` must be a tuple.")
    if all(list(map(isinstance, lon_bounds, [(float, int), (float, int)]))) is False:
        raise TypeError("``lon_bounds`` tuple must contain only int or float types.")
    if (lon_bounds[0] < -180) or (lon_bounds[1] > 180):
        raise ValueError("``lon_bounds`` must be within the range (-180, 180).")
    if not isinstance(lat_bounds, tuple):
        raise TypeError("``lat_bounds`` must be a tuple.")
    if all(list(map(isinstance, lat_bounds, [(float, int), (float, int)]))) is False:
        raise TypeError("``lat_bounds`` tuple must contain only int or float types.")
    if (lat_bounds[0] < -90) or (lat_bounds[1] > 90):
        raise ValueError("``lat_bounds`` must be within the range (-90, 90).")
    if not isinstance(is_obs, bool):
        raise TypeError("``is_obs`` flag must be a boolean.")

    # -- Loading Coordinates -- #
    if (data.lon.chunks is not None) or (data.lat.chunks is not None):
        lon = data['lon'].load()
        lat = data['lat'].load()
    else:
        lon = data['lon']
        lat = data['lat']
    
    # -- Raise Warning if Bounds Outside Domain -- #
    if ((lon_bounds[0] < np.floor(lon.min().item())) or
        (lon_bounds[1] > np.ceil(lon.max().item())) or
        (lat_bounds[0] < np.floor(lat.min().item())) or
        (lat_bounds[1] > np.ceil(lat.max().item()))
        ):
        if is_obs:
            data_type = 'observations'
        else:
            data_type = 'model'

        warning_message = f"[longitude: {lon_bounds[0]}, {lon_bounds[1]}; latitude: {lat_bounds[0]}, {lat_bounds[1]}] bounds are outside the range of available {data_type} data [longitude: {data.lon.min().item()}, {lon.max().item()}; latitude: {lat.min().item()}, {lat.max().item()}]. Using nearest available longitudes and latitudes."
        warnings.warn(warning_message, RuntimeWarning)
    
    # -- Subset Data -- #
    data = data.where((lon >= lon_bounds[0]) & (lon <= lon_bounds[1]) &
                      (lat >= lat_bounds[0]) & (lat <= lat_bounds[1]),
                      drop=True)

    return data


def _apply_depth_bounds(data : xr.DataArray,
                        depth_bounds : tuple[float],
                        is_obs : bool = True
                        ) -> xr.DataArray:
    """
    Subset a given xarray DataArray according to the
    specified depth bounds.

    Parameters
    ----------
    data : xarray.DataArray
        DataArray containing the variable to subset.
    depth_bounds : tuple[float]
        Depth bounds to subset the data. Must be a
        tuple with minimum and maximum values.
    is_obs : bool, default: False
        Flag to indicate if the data is from an ocean
        observations dataset. Default is True.
    
    Returns
    -------
    xarray.DataArray
        DataArray containing variable subset to the
        specified depth bounds.
    """
    # -- Verify Inputs -- #
    if not isinstance(data, xr.DataArray):
        raise TypeError("``data`` must be an xarray DataArray.")
    if 'depth' not in data.coords:
        raise ValueError("``data`` must contain coordinates ``depth``.")
    if not isinstance(depth_bounds, tuple):
        raise TypeError("``depth_bounds`` must be a tuple.")
    if all(list(map(isinstance, depth_bounds, [(float, int), (float, int)]))) is False:
        raise TypeError("``depth_bounds`` tuple must contain only int or float types.")
    if not isinstance(is_obs, bool):
        raise TypeError("``is_obs`` flag must be a boolean.")

    # -- Loading Coordinates -- #
    if (data.depth.chunks is not None):
        depth = data['depth'].load()
    else:
        depth = data['depth']
    
    # -- Raise Warning if Bounds Outside Domain -- #
    if ((depth_bounds[0] < np.floor(depth.min().item())) or
        (depth_bounds[1] > np.ceil(depth.max().item()))
        ):
        if is_obs:
            data_type = 'observations'
        else:
            data_type = 'model'

        warning_message = f"[depth: {depth_bounds[0]}, {depth_bounds[1]}] bounds are outside the range of available {data_type} data [depth: {data.depth.min().item()}, {depth.max().item()}]. Using nearest available depth levels."
        warnings.warn(warning_message, RuntimeWarning)
    
    # -- Subset Data -- #
    # Single depth level:
    if depth_bounds[0] == depth_bounds[1]:
        data = data.sel(depth=depth_bounds[0], method="nearest")
    # Multiple depth levels:
    else:
        data = data.where((depth >= depth_bounds[0]) & (depth <= depth_bounds[1]),
                          drop=True)

    return data


def _compute_climatology(data: xr.DataArray,
                         freq: str = 'total'
                         ) -> xr.DataArray:
    """
    Compute the climatology of a given xarray DataArray
    at the specified frequency.

    Parameters
    ----------
    data : xarray.DataArray
        DataArray containing the variable to compute the
        climatology from.
    freq : str, default: ``total``
        Climatology frequency to compute.
        Options include ``total``, ``seasonal``, ``monthly``,
        ``jan``, ``feb``, `mar`` etc. for individual months.

    Returns
    -------
    xarray.DataArray
        DataArray containing the climatology of the input
        data at the specified frequency.
    """
    # -- Verify Inputs -- #
    if not isinstance(data, xr.DataArray):
        raise TypeError("``data`` must be an xarray DataArray.")
    if 'time' not in data.dims:
        raise ValueError("``data`` must contain variable ``time``.")
    if not np.issubdtype(data.time.dtype, np.datetime64):
        raise TypeError("variable ``time`` dtype must be datetime64.")

    # Define climatology frequencies:
    month_names = ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec']
    months = {key: value for key, value in zip(month_names, range(1, 13))}
    freq_names = ['total', 'seasonal', 'monthly']
    freq_names.extend(month_names)

    if not isinstance(freq, str):
        raise TypeError("``freq`` must be a string.")
    if freq not in freq_names:
        raise ValueError("``freq`` must be one of 'total', 'seasonal', 'monthly', 'jan' ... 'dec'.")

    # -- Compute Climatology -- #
    if freq == 'total':
        data = data.mean(dim='time')
    elif freq == 'seasonal':
        data = data.groupby('time.season').mean()
    elif freq == 'monthly':
        data = data.groupby('time.month').mean()
    else:
        data = data.sel(time=data['time'].dt.month.isin(months[freq])).mean(dim='time')

    return data
