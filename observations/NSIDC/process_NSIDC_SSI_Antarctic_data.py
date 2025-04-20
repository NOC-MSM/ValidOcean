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

# Retrieve sea ice mask & concentration files:
mask_files = [f for f in file_paths if "extent" in f]
conc_files = [f for f in file_paths if "concentration" in f]

# -- Post-Process Sea Ice Mask Data -- #
# Define the time dimension:
time_simask = xr.DataArray(data=get_datetimes_from_filenames(file_list=mask_files), dims='time', name='time')
# Load and concatenate all sea ice mask GeoTIFFs:
simask = xr.concat([rioxarray.open_rasterio(i) for i in mask_files], dim=time_simask)

# Sea Ice Mask is defined by [1: sea ice, 0: ocean]:
# Values greater than 1 (missing or land) are set to NaN:
ds_si['simask'] = xr.where(simask > 1, np.nan, simask).squeeze()
ds_si["simask"].attrs = {"long_name": "sea ice mask [1: sea ice, 0: ocean]", "standard_name": "sea_ice_mask", "valid_range": [0, 1], "valid_min": 0, "valid_max": 1}

# -- Post-Process Sea Ice Concentration Data -- #
# Define the time dimension:
time_siconc = xr.DataArray(data=get_datetimes_from_filenames(file_list=conc_files), dims='time', name='time')
# Load and concatenate all sea ice extent GeoTIFFs:
siconc = xr.concat([rioxarray.open_rasterio(i) for i in conc_files], dim=time_siconc)

# Sea Ice Concentration:
# Note concentration percentage is scaled by 10 -> requires division by 1000.
# Values greater than 1 (missing or land) are set to NaN:
ds_si['siconc'] = xr.where(siconc > 1000, np.nan, siconc).squeeze() / 1000
ds_si['siconc'].attrs = {'units': '1', 'long_name': 'sea ice area fraction [0: ocean, 0.01-0.15: statistically insignificant, > 0.15: sea ice]', 'standard_name': 'sea_ice_area_fraction', 'valid_range': [0, 1], 'valid_min': 0, 'valid_max': 1}

# -- Calculate sea ice area (m2) -- #
ds_si['cell_area'] = ds_area['cell_area']
ds_si['cell_area'].attrs = {'units': 'm2', 'long_name': 'area of grid cell', "standard_name": "cell_area"}

ds_si['siarea'] = (ds_si['cell_area']*ds_si['simask']).sum(dim=['x', 'y'])
ds_si['siarea'].attrs = {'units': 'm2', 'long_name': 'total area where sea ice concentration > 15%', 'standard_name': 'sea_ice_extent'}

# -- Update Coordinates -- #
ds_si.coords['lon'] = ds_si['longitude']
ds_si.coords['lat'] = ds_si['latitude']
# Drop auxiliary variables:
ds_si = ds_si.drop_vars(["band", "spatial_ref", "crs", "longitude", "latitude"])

# -- Save NSIDC Sea Ice Index Dataset -- #
# Update variable encodings:
for var in ds_si.variables:
    if ds_si[var].dtype == 'float64':
        ds_si[var].encoding['missing_value'] = np.nan
        ds_si[var].encoding['_FillValue'] = np.nan

# Define output filepath:
out_fpath = "/dssgfs01/scratch/otooth/npd_data/observations/NSIDC/NSIDC_Sea_Ice_Index_v3_Antarctic_combined_1978_2025.nc"
# Save dataset to netCDF file:
ds_si.to_netcdf(out_fpath, unlimited_dims='time')

print(f'Completed: Saved NSIDC Sea Ice Index Antarctic Observations to netCDF file: {out_fpath}.')
