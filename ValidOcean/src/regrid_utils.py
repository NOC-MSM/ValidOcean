"""
regrid_utils.py

Description: This module contains functions for regridding data 
from one grid to another.

Author: Ollie Tooth (oliver.tooth@noc.ac.uk)
"""
# -- Import required packages -- #
import xarray as xr
import xesmf as xe

# -- Regrid source data onto target data grid -- #
def _regrid_data(source_grid:xr.DataArray, target_grid:xr.DataArray, method:str='bilinear') -> xr.DataArray:
    """
    Regrid data stored on a source grid onto a target grid
    using xesmf with chosen method.

    Parameters
    ----------
    source_grid : xarray.DataArray
        Source grid to regrid data from. Longitude and latitude
        coordinates must be called ``lon`` and ``lat``, respectively.
    target_grid : xarray.DataArray
        Target grid to regrid data onto. Longitude and latitude
        coordinates must be called ``lon`` and ``lat``, respectively.
    method : str
        Method used by xesmf for regridding.
        Options: ``bilinear``, ``conservative``, ``nearest_s2d``, ``nearest_d2s``.

    Returns
    -------
    xarray.DataArray
        Source data regridded onto target grid.
    """
    # -- Validate Input Arguments -- #
    if not isinstance(source_grid, xr.DataArray):
        raise TypeError("``source_grid`` must be an xarray.DataArray.")
    if not isinstance(target_grid, xr.DataArray):
        raise TypeError("``target_grid` must be an xarray.DataArray.")
    if not isinstance(method, str):
        raise TypeError("``method`` must be a string.")
    
    # -- Regrid Data -- #
    regridder = xe.Regridder(source_grid, target_grid, method=method, periodic=True)
    regridded_data = regridder(source_grid)

    return regridded_data
