"""
preprocess.py

Description: This module contains functions to preprocess
ocean general circulation models outputs.

Author: Ollie Tooth (oliver.tooth@noc.ac.uk)
"""

# -- Import required packages -- #
import xarray as xr

def _preprocess_model_data(var:xr.DataArray, freq:str, timeslice:slice) -> xr.DataArray:
    """
    Process variable stored in ocean model dataset & compute climatology
    for validation with ocean observations.

    Parameters
    ----------
    var : xarray.DataArray
        Variable to pre-process in ocean model dataset.
    freq : str
        Climatology frequency to compute from ocean model dataset.
        Options include ``total``, ``seasonal``, ``monthly``.
    timeslice : slice
        Time slice to compute variable climatologies.

    Returns
    -------
    xarray.DataArray
        Climatology of pre-processed variable.
    """
    # -- Validate Inputs -- #
    if not isinstance(var, xr.DataArray):
        raise TypeError("``var`` must be an xarray.DataArray.")
    if not isinstance(freq, str):
        raise TypeError("``freq`` must be specified as a string.")
    if freq not in ['total', 'seasonal', 'monthly']:
        raise ValueError("``freq`` must be one of ``total``, ``seasonal`` or ``monthly``.")
    if not isinstance(timeslice, slice):
        raise TypeError("``timeslice`` must be specified as a slice.")

    # -- Subset Variable -- #
    var_out = var.sel(time=timeslice)

    # -- Calculate Climatology -- #
    if freq == 'total':
        var_out = var_out.mean(dim='time')
    elif freq == 'seasonal':
        var_out = var_out.groupby('time.season').mean()
    elif freq == 'monthly':
        var_out = var_out.groupby('time.month').mean()

    return var_out