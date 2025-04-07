"""
process_NSIDC_SSI_Antarctic_data.py

Description: Python script to post-process NSIDC Sea Ice Index
data for the Antarctic 1978-2025, including sea ice concentration,
extent and total area.

Created by: Ollie Tooth (oliver.tooth@noc.ac.uk)
Created on: 2025-02-21
"""
# -- Import Python packages -- #
import rioxarray
import numpy as np
import xarray as xr
from glob import glob
from datetime import datetime

# -- Define Utility Functions -- #
def get_datetimes_from_filenames(file_list):
    # Extract filenames from paths:
    filenames = [file.split('/')[-1] for file in file_list]

    # Convert filenames date str to datetime:
    datetimes = np.array([datetime(year=int(file[2:6]), month=int(file[6:8]), day=15) for file in filenames])

    return datetimes

# -- Post-Process NSIDC Observations -- #
print('In Progress: Post-Processing NSIDC Sea Ice Index Antarctic Observations...')

# -- Load NSIDC Ancillary Data -- #
# Define filepath to ancillary data:
anc_fpath = "/dssgfs01/scratch/otooth/npd_data/observations/NSIDC/ancillary/NSIDC0771_LatLon_PS_S25km_v1.0.nc"
# Open NSIDC ancillary data as dataset:
ds_si = xr.open_dataset(anc_fpath)

# Define filepath to NSIDC ancillary file - grid cell area:
area_fpath = "/dssgfs01/scratch/otooth/npd_data/observations/NSIDC/ancillary/NSIDC0771_CellArea_PS_S25km_v1.0.nc"
# Open NSIDC grid cell area:
ds_area = xr.open_dataset(area_fpath)

# -- Load NSIDC Monthly Data -- #
# Define directory path:
dir_path = "/dssgfs01/scratch/otooth/npd_data/observations/NSIDC/antarctic/"

# Get the list of files in the directory:
file_paths = glob(f"{dir_path}*.tif")
file_paths.sort()

# Retrieve sea ice extent & concentration files:
extent_files = [f for f in file_paths if "extent" in f]
conc_files = [f for f in file_paths if "concentration" in f]

# -- Post-Process Sea Ice Extent Data -- #
# Define the time dimension:
time_siext = xr.DataArray(data=get_datetimes_from_filenames(file_list=extent_files), dims='time', name='time')
# Load and concatenate all sea ice extent GeoTIFFs:
siext = xr.concat([rioxarray.open_rasterio(i) for i in extent_files], dim=time_siext)

# Sea Ice Extent is defined by [1: sea ice, 0: ocean]:
# Values greater than 1 (missing or land) are set to NaN:
ds_si['siext'] = xr.where(siext > 1, np.nan, siext).squeeze()
ds_si["siext"].attrs = {"units": "None", "long_name": "Sea Ice Extent Mask (1: sea ice, 0: ocean)", "standard_name": "sea ice extent"}

# -- Post-Process Sea Ice Concentration Data -- #
# Define the time dimension:
time_siconc = xr.DataArray(data=get_datetimes_from_filenames(file_list=conc_files), dims='time', name='time')
# Load and concatenate all sea ice extent GeoTIFFs:
siconc = xr.concat([rioxarray.open_rasterio(i) for i in conc_files], dim=time_siconc)

# Sea Ice Concentration is defined by 0% - 100%:
# Note concentration is scaled by 10.
# Values greater than 100% (missing or land) are set to NaN:
ds_si['siconc'] = xr.where(siconc > 1000, np.nan, siconc).squeeze() / 10
ds_si["siconc"].attrs = {"units": "%", "long_name": "Sea Ice Concentration (0: ocean, 1-15%: statistically insignificant, > 15%: sea ice)", "standard_name": "sea ice concentration"}

# -- Calculate sea ice area (m2) -- #
ds_si['areacello'] = ds_area['cell_area']
ds_si['areacello'].attrs = {'units': 'm2', 'long_name': 'Grid Cell Area', "standard_name": "cell area"}

ds_si['siarea'] = (ds_si['areacello']*ds_si['siext']).sum(dim=['x', 'y'])
ds_si['siarea'].attrs = {'units': 'm2', 'long_name': 'Total Sea Ice Area (where sea ice concentration > 15%).', 'standard_name': 'sea ice area'}

# -- Save NSIDC Sea Ice Index Dataset -- #
# Update variable encodings:
for var in ds_si.data_vars:
    if ds_si[var].dtype == 'float64':
        ds_si[var].encoding['missing_value'] = np.nan

# Define output filepath:
out_fpath = "/dssgfs01/scratch/otooth/npd_data/observations/NSIDC/NSIDC_Sea_Ice_Index_v3_Antarctic_combined_1978_2025.nc"
# Save dataset to netCDF file:
ds_si.to_netcdf(out_fpath, unlimited_dims='time')

print(f'Completed: Saved NSIDC Sea Ice Index Antarctic Observations to netCDF file: {out_fpath}.')
