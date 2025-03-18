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

# -- Define Statistics Utility Functions -- #
def _compute_agg_stats(mdl_data: xr.DataArray,
                       obs_data : xr.DataArray,
                       ) -> xr.Dataset:
    """
    Aggregate multi-dimensional DataArray of (model - obs.)
    errors to scalar statistics.

    Aggregator functions include: Mean Absolute Error (MAE),
    Mean Square Error (MSE), and Root Mean Squared Error (RMSE),
    Pearson Correlation Coefficient (r).

    Note that the Pearson Correlation Coefficient is only
    computed where the core time dimension is present.

    Parameters
    ----------
    mdl_data : xarray.DataArray
        Ocean model variable.
    obs_data : xarray.DataArray
        Ocean observation variable.

    Returns
    -------
    xr.Dataset
        Dataset containing scalar statistics resulting from aggregation.
    """
    # -- Verify Inputs -- #
    if not isinstance(mdl_data, xr.DataArray):
        raise TypeError("``mdl_data`` must be an xarray.DataArray.")
    if not isinstance(obs_data, xr.DataArray):
        raise TypeError("``obs_data`` must be an xarray.DataArray.")

    # -- Compute Aggregated Statistics -- #
    result = xr.Dataset()
    error = (mdl_data - obs_data).squeeze()
    
    dims = [dim for dim in error.dims if dim not in ['season', 'month']]
    result['Mean Absolute Error'] = _mean_abs_error(error=error, dims=dims)
    result['Mean Square Error'] = _mean_square_error(error=error, dims=dims)
    result['Root Mean Square Error'] = _root_mean_square_error(error=error, dims=dims)
    if 'time' in dims:
        result['Pearson Correlation Coefficient'] = _pearson_correlation(mdl_data=mdl_data, obs_data=obs_data, dims="time")

    return result


def _mean_abs_error(error : xr.DataArray,
                    dims : list,
                    ) -> xr.DataArray:
    """
    Compute the mean absolute error of a multi-
    dimensional (model - obs.) error.

    Parameters
    ----------
    error : xr.DataArray
        Multi-dimensional DataArray containing (model - obs.) errors.
    """
    return np.abs(error).mean(dim=dims, skipna=True)


def _mean_square_error(error : xr.DataArray,
                       dims : list,
                       ) -> xr.DataArray:
    """
    Compute the mean squared error of a multi-
    dimensional (model - obs.) error.

    Parameters
    ----------
    error : xr.DataArray
        Multi-dimensional DataArray of (model - obs.) errors.
    """
    return np.square(np.abs(error)).mean(dim=dims, skipna=True)


def _root_mean_square_error(error : xr.DataArray,
                            dims : list,
                            ) -> xr.DataArray:
    """
    Compute the root mean squared error of a multi-
    dimensional (model - obs.) error.

    Parameters
    ----------
    error : xr.DataArray
        Multi-dimensional DataArray of (model - obs.) errors.
    """
    return np.sqrt(np.square(np.abs(error)).mean(dim=dims, skipna=True))


def _pearson_correlation(mdl_data : xr.DataArray,
                         obs_data : xr.DataArray,
                         dim : str,
                         ) -> xr.DataArray:
    """
    Compute the Pearson Correlation Coefficient between multi-
    dimensional model & observational DataArrays.

    Parameters
    ----------
    mdl_data : xr.DataArray
        Ocean model multi-dimensional DataArray.
    obs_data : xr.DataArray
        Ocean observation multi-dimensional DataArray.
    dim : str
        Dimension to compute correlation coefficient over.
    """
    return xr.corr(da_a=mdl_data, da_b=obs_data, dim=dim)
