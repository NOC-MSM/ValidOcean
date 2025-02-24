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

def _compute_agg_stats(mdl_error: xr.DataArray) -> dict:
    """
    Compute aggregated statistics to compare ocean model output
    and ocean observations.

    Aggregated statistics include: Mean Absolute Error (MAE),
    Mean Square Error (MSE), and Root Mean Squared Error (RMSE).

    Parameters
    ----------
    mdl_error : xarray.DataArray
        Error between ocean model output and ocean observations.

    Returns
    -------
    dict
        Dictionary containing aggregated statistics.
    """
    # -- Verify Inputs -- #
    if not isinstance(mdl_error, xr.DataArray):
        raise TypeError("``mdl_error`` must be an xarray.DataArray.")

    # -- Compute Aggregated Statistics -- #
    agg_stats = {}
    agg_stats['Mean Absolute Error'] = mean_abs_error(mdl_error)
    agg_stats['Mean Square Error'] = mean_square_error(mdl_error)
    agg_stats['Root Mean Square Error'] = root_mean_square_error(mdl_error)

    return agg_stats

def mean_abs_error(error):
    """
    Compute the mean absolute error of a
    2-dimensional DataArray.

    Parameters
    ----------
    error : xr.DataArray
        2-dimensional DataArray of errors.
    """
    return np.abs(error).mean(skipna=True).values.item(0)

def mean_square_error(error):
    """
    Compute the mean square error of a
    2-dimensional DataArray.

    Parameters
    ----------
    error : xr.DataArray
        2-dimensional DataArray of errors.
    """
    return np.square(np.abs(error)).mean(skipna=True).values.item(0)

def root_mean_square_error(error):
    """
    Compute the root mean squared error of a
    2-dimensional DataArray.

    Parameters
    ----------
    error : xr.DataArray
        2-dimensional DataArray of errors.
    """
    return np.sqrt(np.square(np.abs(error)).mean(skipna=True)).values.item(0)
