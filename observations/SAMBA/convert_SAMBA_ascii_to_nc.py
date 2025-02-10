"""
convert_SAMBA_ascii_to_nc.py

Description: Script to convert SAMBA ascii files to netcdf
format for use with the metric package to compute
AMOC diagnostics.

Date Created: 2024-10-07
Last Modified: 2024-12-19
"""
# Import packages:
import xarray as xr
import pandas as pd

# Define column names:
col_names = ['year', 'month', 'day', 'hour', 'moc', 'moc_clinic', 'moc_tropic', 'moc_ek', 'moc_west_dens', 'moc_east_dens', 'moc_west_pres', 'moc_east_pres']

# Read .asc file as a DataFrame:
file = "/dssgfs01/scratch/otooth/npd_data/observations/SAMBA/data/raw_MOC_TotalAnomaly_and_constituents.asc"
df = pd.read_csv(file, sep='\t', header=None, names=col_names)

# Transform time columns to datetime column:
df['time'] = pd.to_datetime(dict(year=df.year, month=df.month, day=df.day, hour=df.hour))
df = df.drop(columns=['year', 'month', 'day', 'hour'])
df = df.set_index('time')

# Construct xarray dataset:
ds = xr.Dataset.from_dataframe(df)

# Save dataset to netcdf file:
outfilename = "/dssgfs01/scratch/otooth/npd_data/observations/SAMBA/data/SAMBA_MOC_TotalAnomaly_and_constituents_ts.nc"
ds.to_netcdf(outfilename)
