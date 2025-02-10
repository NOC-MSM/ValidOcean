"""
download_Latrabjarg_data.py

Description: Python script to download Latrabjarg hydrographic
section climatology to send to the JASMIN Object Store.

Created by: Ollie Tooth (oliver.tooth@noc.ac.uk)
Created on: 2024-12-19
"""
# -- Import Python packages -- #
import nsv
import xarray as xr

# -- Download Kogur Hydrographic Section Data -- #
# Define output filepath:
out_fpath = "/dssgfs01/scratch/otooth/npd_data/observations/Latrabjarg/data/nsv_latrabjarg_section_climatology_1990_2012.nc"

# Download standardised Kogur hydrographic section data:
ds = nsv.Standardizer().latrabjarg_climatology

# -- Write to .nc file -- #
ds.to_netcdf(out_fpath)
