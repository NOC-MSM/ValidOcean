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

def _subset_data(data : xr.DataArray,
                 bounds : slice,
                 is_obs : bool = True,
                 ) -> xr.DataArray:
    """
    Subset a given xarray DataArray to the specified
    temporal bounds.

    Parameters
    ----------
    data : xarray.DataArray
        DataArray containing the variable to subset.
    bounds : slice
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
    if not isinstance(bounds, slice):
        raise TypeError("``bounds`` must be a slice.")
    if not isinstance(bounds.start, str):
        raise TypeError("``bounds.start`` must be a datetime string (e.g., 'YYYY-MM').")
    if not isinstance(bounds.stop, str):
        raise TypeError("``bounds.stop`` must be a datetime string (e.g., 'YYYY-MM')")
    if not isinstance(is_obs, bool):
        raise TypeError("``is_obs`` flag must be a boolean.")
    
    #  -- Raise Warning if Bounds Outside Data -- #
    if (np.datetime64(bounds.start) < data.time.min()) | (np.datetime64(bounds.stop) > data.time.max()):
        if is_obs:
            data_type = 'observations'
        else:
            data_type = 'model'

        warning_message = f"bounds {bounds.start} - {bounds.stop} are outside the range of available {data_type} data {data.time.min().values.astype('datetime64[D]')} - {data.time.max().values.astype('datetime64[D]')}."
        warnings.warn(warning_message, RuntimeWarning)
    
    # -- Subset Data -- #
    data = data.sel(time=bounds)

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
        Climatology frequency to compute. Options include
        ``total``, ``seasonal``, ``monthly``.

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

    if not isinstance(freq, str):
        raise TypeError("``freq`` must be a string.")
    if freq not in ['total', 'seasonal', 'monthly']:
        raise ValueError("``freq`` must be one of 'total', 'seasonal', 'monthly'.")

    # -- Compute Climatology -- #
    if freq == 'total':
        data = data.mean(dim='time')
    elif freq == 'seasonal':
        data = data.groupby('time.season').mean()
    elif freq == 'monthly':
        data = data.groupby('time.month').mean()

    return data
