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
col_names = ['year', 'month', 'day', 'florida_current_transport', 'flag']

# Read .dat file as a DataFrame:
file = "/dssgfs01/scratch/otooth/npd_data/observations/RAPID/fc_transport.csv"
df = pd.read_csv(file, header=None, names=col_names)

# Transform time columns to datetime column:
df['time'] = pd.to_datetime(dict(year=df.year, month=df.month, day=df.day))
df = df.drop(columns=['year', 'month', 'day'])
df = df.set_index('time')

# Construct xarray dataset:
ds = xr.Dataset.from_dataframe(df)

# Save dataset to netcdf file:
outfilename = "/dssgfs01/scratch/otooth/npd_data/observations/RAPID/2000-2024.nc"
ds.to_netcdf(outfilename)
