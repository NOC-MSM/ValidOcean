"""
aggregator.py

Description: This module contains utility functions to
calculate aggregate statistics on ocean model & observation
output for validation.

Author: Ollie Tooth (oliver.tooth@noc.ac.uk)
"""

# -- Import required packages -- #
import numpy as np
import xarray as xr

# -- Define Aggregation Utility Functions -- #
def _aggregate_to_1D(data : xr.DataArray,
                     mask : xr.DataArray | None = None,
                     weights : xr.DataArray | None = None,
                     aggregator : str = 'sum',
                     skipna : bool = True
                     ) -> xr.DataArray:
    """
    Aggregate multi-dimensional DataArray to
    1-dimensional diagnostic.

    Parameters
    ----------
    data : xarray.DataArray
        DataArray to aggregate.
    mask : xarray.DataArray, default: ``None``
        Mask to apply to DataArray prior to aggregation.
    weights : xarray.DataArray, default: ``None``
        Weights to apply to DataArray during aggregation.
    aggregator : str, default: 'sum'
        Aggregation function to apply.
        Options include: 'sum', 'mean', 'std'.
    skipna : bool, default: ``True``
        Whether to skip NaN values during aggregation.
    
    Returns
    -------
    xarray.DataArray
        1-dimensional diagnostic resulting from aggregation.
    """
    # -- Verify inputs -- #
    if not isinstance(data, xr.DataArray):
        raise ValueError("``data`` must be a xarray.DataArray.")
    if mask is not None:
        if not isinstance(mask, xr.DataArray):
            raise ValueError("``mask`` must be a xarray.DataArray.")
    if weights is not None:
        if not isinstance(weights, xr.DataArray):
            raise ValueError("``weights`` must be an xarray.DataArray.")
    if not isinstance(aggregator, str):
        raise ValueError("``aggregator`` must be a string.")
    if aggregator not in ['sum', 'mean', 'std']:
        raise ValueError(
            f"Invalid ``aggregator`` specified: {aggregator}. Must be one of ['sum', 'mean', 'std']."
            )
    if not isinstance(skipna, bool):
        raise ValueError("``skipna`` must be a boolean.")

    # -- Apply mask and aggregate to 1D -- #
    dims = [dim for dim in data.dims if dim != 'time']
    if mask is not None:
        data = data.where(mask.squeeze())

    if weights is not None:
        data = data.weighted(weights.squeeze())

    if aggregator == 'sum':
        result = data.sum(dim=dims, skipna=True)
    elif aggregator == 'mean':
        result = data.mean(dim=dims, skipna=True)
    elif aggregator == 'std':
        result = data.std(dim=dims, skipna=True)

    return result
