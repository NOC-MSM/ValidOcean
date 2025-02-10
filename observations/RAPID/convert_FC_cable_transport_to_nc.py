"""
convert_FC_cable_transport_to_nc.py

Description: Script to convert Florida Current .dat file
to netcdf format for use with the metric package to
compute AMOC diagnostics.

Date Created: 2024-10-07
Last Modified: 2024-12-19
"""
# Import packages:
import xarray as xr
import pandas as pd

# Define column names:
col_names = ['year', 'month', 'day', 'florida_current_transport']
dtype = {'year': int, 'month': int, 'day': int, 'florida_current_transport': float}

# Read .dat file as a DataFrame:
file = "/dssgfs01/scratch/otooth/npd_data/observations/RAPID/data/fc_transport.dat"
df = pd.read_fwf(file, header=None, names=col_names, dtype=dtype, infer_nrows=500)

# Transform time columns to datetime column:
df['time'] = pd.to_datetime(dict(year=df.year, month=df.month, day=df.day))
df = df.drop(columns=['year', 'month', 'day'])
df = df.set_index('time')

# Construct xarray dataset:
ds = xr.Dataset.from_dataframe(df)

# Save dataset to netcdf file:
outfilename = "/dssgfs01/scratch/otooth/npd_data/observations/RAPID/data/FC_cable_transport_1982-2024.nc"
ds.to_netcdf(outfilename)
