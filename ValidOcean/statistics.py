"""
statistics.py

Description: This module contains functions to calculate 
aggregated & spatial statistics to validate ocean general
circulation models using observations.

Author: Ollie Tooth (oliver.tooth@noc.ac.uk)
"""

# -- Import required packages -- #
import numpy as np
import xarray as xr

def _compute_agg_stats(error: xr.DataArray) -> dict:
    """
    Compute aggregated statistics to compare ocean model output
    and ocean observations.

    Aggregated statistics include: Mean Absolute Error (MAE),
    Mean Square Error (MSE), and Root Mean Squared Error (RMSE).

    Parameters
    ----------
    error : xarray.DataArray
        Error between ocean model output and ocean observations.

    Returns
    -------
    xr.Dataset
        Dataset containing aggregated statistics.
    """
    # -- Verify Inputs -- #
    if not isinstance(error, xr.DataArray):
        raise TypeError("``error`` must be an xarray.DataArray.")

    # -- Compute Aggregated Statistics -- #
    agg_stats = xr.Dataset()
    error = error.squeeze()
    
    agg_dims = [dim for dim in error.dims if dim not in ['season', 'month']]
    agg_stats['Mean Absolute Error'] = _mean_abs_error(error=error, agg_dims=agg_dims)
    agg_stats['Mean Square Error'] = _mean_square_error(error=error, agg_dims=agg_dims)
    agg_stats['Root Mean Square Error'] = _root_mean_square_error(error=error, agg_dims=agg_dims)

    return agg_stats

def _mean_abs_error(error : xr.DataArray, agg_dims : list) -> xr.DataArray:
    """
    Compute the mean absolute error of a multi-
    dimensional (model - obs.) error DataArray.

    Parameters
    ----------
    error : xr.DataArray
        2/3-dimensional DataArray of (model - obs.) errors.
    """
    return np.abs(error).mean(dim=agg_dims, skipna=True)

def _mean_square_error(error : xr.DataArray, agg_dims : list) -> xr.DataArray:
    """
    Compute the mean squared error of a multi-
    dimensional (model - obs.) error DataArray.

    Parameters
    ----------
    error : xr.DataArray
        2/3-dimensional DataArray of (model - obs.) errors.
    """
    return np.square(np.abs(error)).mean(dim=agg_dims, skipna=True)

def _root_mean_square_error(error : xr.DataArray, agg_dims : list) -> xr.DataArray:
    """
    Compute the root mean squared error of a multi-
    dimensional (model - obs.) error DataArray.

    Parameters
    ----------
    error : xr.DataArray
        2/3-dimensional DataArray of (model - obs.) errors.
    """
    return np.sqrt(np.square(np.abs(error)).mean(dim=agg_dims, skipna=True))
