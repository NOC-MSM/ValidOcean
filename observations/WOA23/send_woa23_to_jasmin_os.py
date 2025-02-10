"""
send_WOA2023_to_jasmin_os.py

Description: Python script to send the WOA2023 ocean
climate normals to the JASMIN Object Store.

Created by: Ollie Tooth (oliver.tooth@noc.ac.uk)
Created on: 2025-01-03
"""
# -- Import Python packages -- #
import sys
import glob
import numpy as np
import xarray as xr

# Add path to msm-os package:
sys.path.append('/dssgfs01/working/otooth/AtlantiS/jasmin_os/msm-os/src/')
from msm_os.object_store import ObjectStoreS3

# -- Define JASMIN S3 object store -- #
# Create ObjectStoreS3 instance:
store_credentials_json = '/dssgfs01/working/otooth/AtlantiS/jasmin_os/credentials/jasmin_os_credentials.json'
obj_store = ObjectStoreS3(anon=False, store_credentials_json=store_credentials_json)
# Define bucket name:
bucket = 'npd-obs'
print('Completed: Initialised JASMIN S3 object store using user credentials.')

# -- Define function to send WOA2023 dataset to JASMIN Object Store -- #
def send_ds_to_jasmin_os(ds:xr.Dataset, dest:str, description:str):
    """
    Send World Ocean Atlas Dataset to JASMIN Object Store.

    Parameters
    ----------
    ds : xarray.Dataset
        World Ocean Atlas Dataset to be sent to JASMIN Object Store.
    dest : str
        Destination path in JASMIN Object Store. Must consist of the
        bucket name, the desired object prefix and object name.
    description : str
        Description to be added to the xarray.Dataset before sending to
        JASMIN Object Store.
    
    Returns
    -------
    None
    """
    # Define s3fs mapper to destination:
    mapper = obj_store.get_mapper(dest)

    # Add description as attribute to Dataset:
    ds.attrs['description'] = description

    # Write Dataset to JASMIN OS as .zarr store:
    ds.to_zarr(mapper, mode='w', consolidated=True)

    return None

# -- Define function to get filepaths for WOA2023 data -- #
def get_filepaths(file_prefix:str, file_directory:str) -> list:
    """
    Get filepaths for World Ocean Atlas 2023 Climate Normals
    temperature and salinity data. 

    Parameters
    ----------
    file_prefix : str
        Prefix of the WOA23 filepaths to be returned.
    file_directory : str
        Directory containing the WOA23 files.

    Returns
    -------
    filepaths : list
        List of WOA23 filepaths for practical salinity & potential temperature.
    """
    # Define salinity and temperature filenames:
    salinity_filename = f"{file_prefix}_s*.nc"
    temperature_filename = f"{file_prefix}_t*.nc"

    # Get filepaths for salinity and temperature data:
    filepaths_sal = glob.glob(f"{file_directory}/{salinity_filename}")
    filepaths_temp = glob.glob(f"{file_directory}/{temperature_filename}")
    filepaths_sal.sort()
    filepaths_temp.sort()

    return filepaths_sal, filepaths_temp

# -- Send WOA23 Annual Climatologies to JASMIN Object Store -- #
print('In Progress: Sending WOA23 Annual Climatologies to JASMIN Object Store...')

# -- Processing Climate Normal annual climatologies -- #
# Define file directory:
filedir = "/dssgfs01/scratch/otooth/npd_data/observations/WOA2023/data"

# 1. WOA23 1971-2000 climate normal:
ds_s71 = xr.open_dataset(f"{filedir}/woa23_decav71A0_s00_04.nc", decode_times=False)
ds_t71 = xr.open_dataset(f"{filedir}/woa23_decav71A0_t00_04.nc", decode_times=False)

ds_annual71 = xr.merge([ds_s71, ds_t71])
ds_annual71 = ds_annual71.assign_coords({'time': np.array([np.datetime64('1986-06-15', 'ns')])})
ds_annual71['climatology_bounds'].values = np.array([[np.datetime64('1971-01-01', 'ns'), np.datetime64('2000-12-31', 'ns')]])

# 2. WOA23 1981-2010 climate normal:
ds_s81 = xr.open_dataset(f"{filedir}/woa23_decav81B0_s00_04.nc", decode_times=False)
ds_t81 = xr.open_dataset(f"{filedir}/woa23_decav81B0_t00_04.nc", decode_times=False)

ds_annual81 = xr.merge([ds_s81, ds_t81])
ds_annual81 = ds_annual81.assign_coords({'time': np.array([np.datetime64('1996-06-15', 'ns')])})
ds_annual81['climatology_bounds'].values = np.array([[np.datetime64('1981-01-01', 'ns'), np.datetime64('2010-12-31', 'ns')]])

# 3. WOA23 1991-2020 climate normal:
ds_s91 = xr.open_dataset(f"{filedir}/woa23_decav91C0_s00_04.nc", decode_times=False)
ds_t91 = xr.open_dataset(f"{filedir}/woa23_decav91C0_t00_04.nc", decode_times=False)

ds_annual91 = xr.merge([ds_s91, ds_t91])
ds_annual91 = ds_annual91.assign_coords({'time': np.array([np.datetime64('2006-06-15', 'ns')])})
ds_annual91['climatology_bounds'].values = np.array([[np.datetime64('1991-01-01', 'ns'), np.datetime64('2020-12-31', 'ns')]])

# Send annual climatologies to JASMIN Object Store:
send_ds_to_jasmin_os(ds=ds_annual71, dest=f"{bucket}/WOA23/WOA23_1971_2000_annual_climatology",
                     description="World Ocean Atlas 2023 1971-2000 Annual Climatological mean temperature & salinity for the global ocean from in situ profile data. Downloaded from https://www.ncei.noaa.gov/data/oceans/woa/WOA23/DATA/temperature/netcdf/decav71A0/0.25/woa23_decav71A0_t00_04.nc and https://www.ncei.noaa.gov/data/oceans/woa/WOA23/DATA/temperature/netcdf/decav71A0/0.25/woa23_decav71A0_s00_04.nc on 02/01/2025. Transferred to JASMIN Object Store on 02/01/2025.")

send_ds_to_jasmin_os(ds=ds_annual81, dest=f"{bucket}/WOA23/WOA23_1981_2010_annual_climatology",
                     description="World Ocean Atlas 2023 1981-2010 Annual Climatological mean temperature & salinity for the global ocean from in situ profile data. Downloaded from https://www.ncei.noaa.gov/data/oceans/woa/WOA23/DATA/temperature/netcdf/decav81B0/0.25/woa23_decav81B0_t00_04.nc and https://www.ncei.noaa.gov/data/oceans/woa/WOA23/DATA/temperature/netcdf/decav81B0/0.25/woa23_decav81B0_s00_04.nc on 02/01/2025. Transferred to JASMIN Object Store on 02/01/2025.")

send_ds_to_jasmin_os(ds=ds_annual91, dest=f"{bucket}/WOA23/WOA23_1991_2020_annual_climatology",
                     description="World Ocean Atlas 2023 1991-2020 Annual Climatological mean temperature & salinity for the global ocean from in situ profile data. Downloaded from https://www.ncei.noaa.gov/data/oceans/woa/WOA23/DATA/temperature/netcdf/decav91C0/0.25/woa23_decav91C0_t00_04.nc and https://www.ncei.noaa.gov/data/oceans/woa/WOA23/DATA/temperature/netcdf/decav91C0/0.25/woa23_decav91C0_s00_04.nc on 02/01/2025. Transferred to JASMIN Object Store on 02/01/2025.")

print('Completed: Sent Annual WOA23 Climatologies to JASMIN Object Store.')

# -- Send WOA23 Seasonal Climatologies to JASMIN Object Store -- #
print('In Progress: Sending WOA23 Seasonal Climatologies to JASMIN Object Store...')

# -- Processing Climate Normal Seasonal Climatologies -- #
# Define seasons:
seasons = np.array(['JFM', 'AMJ', 'JAS', 'OND'], dtype='str')

# 1. WOA23 1971-2000 climate normal:
filepaths_sal, filepaths_temp = get_filepaths(file_directory=filedir, file_prefix='woa23_decav71A0')

ds_s71 = xr.open_mfdataset(filepaths_sal[13:], decode_times=False)
ds_t71 = xr.open_mfdataset(filepaths_temp[13:], decode_times=False)

ds_seasonal71 = xr.merge([ds_s71, ds_t71])
ds_seasonal71 = ds_seasonal71.rename({'time': 'season'}).assign_coords({'season': seasons})
ds_seasonal71 = ds_seasonal71.drop_vars('climatology_bounds')

# 2. WOA23 1981-2010 climate normal:
filepaths_sal, filepaths_temp = get_filepaths(file_directory=filedir, file_prefix='woa23_decav81B0')

ds_s81 = xr.open_mfdataset(filepaths_sal[13:], decode_times=False)
ds_t81 = xr.open_mfdataset(filepaths_temp[13:], decode_times=False)

ds_seasonal81 = xr.merge([ds_s81, ds_t81])
ds_seasonal81 = ds_seasonal81.rename({'time': 'season'}).assign_coords({'season': seasons})
ds_seasonal81 = ds_seasonal81.drop_vars('climatology_bounds')

# 3. WOA23 1991-2020 climate normal:
filepaths_sal, filepaths_temp = get_filepaths(file_directory=filedir, file_prefix='woa23_decav91C0')

ds_s91 = xr.open_mfdataset(filepaths_sal[13:], decode_times=False)
ds_t91 = xr.open_mfdataset(filepaths_temp[13:], decode_times=False)

ds_seasonal91 = xr.merge([ds_s91, ds_t91])
ds_seasonal91 = ds_seasonal91.rename({'time': 'season'}).assign_coords({'season': seasons})
ds_seasonal91 = ds_seasonal91.drop_vars('climatology_bounds')

# Send seasonal climatologies to JASMIN Object Store:
send_ds_to_jasmin_os(ds=ds_seasonal71, dest=f"{bucket}/WOA23/WOA23_1971_2000_seasonal_climatology",
                     description="World Ocean Atlas 2023 1971-2000 Seasonal Climatological mean temperature & salinity for the global ocean from in situ profile data. Downloaded from https://www.ncei.noaa.gov/data/oceans/woa/WOA23/DATA/temperature/netcdf/decav71A0/0.25/woa23_decav71A0_t13...16_04.nc and https://www.ncei.noaa.gov/data/oceans/woa/WOA23/DATA/temperature/netcdf/decav71A0/0.25/woa23_decav71A0_s13...16_04.nc on 02/01/2025. Transferred to JASMIN Object Store on 02/01/2025.")

send_ds_to_jasmin_os(ds=ds_seasonal81, dest=f"{bucket}/WOA23/WOA23_1981_2010_seasonal_climatology",
                     description="World Ocean Atlas 2023 1981-2010 Seasonal Climatological mean temperature & salinity for the global ocean from in situ profile data. Downloaded from https://www.ncei.noaa.gov/data/oceans/woa/WOA23/DATA/temperature/netcdf/decav81B0/0.25/woa23_decav81B0_t13...16_04.nc and https://www.ncei.noaa.gov/data/oceans/woa/WOA23/DATA/temperature/netcdf/decav81B0/0.25/woa23_decav81B0_s13...16_04.nc on 02/01/2025. Transferred to JASMIN Object Store on 02/01/2025.")

send_ds_to_jasmin_os(ds=ds_seasonal91, dest=f"{bucket}/WOA23/WOA23_1991_2020_seasonal_climatology",
                     description="World Ocean Atlas 2023 1991-2020 Seasonal Climatological mean temperature & salinity for the global ocean from in situ profile data. Downloaded from https://www.ncei.noaa.gov/data/oceans/woa/WOA23/DATA/temperature/netcdf/decav91C0/0.25/woa23_decav91C0_t13...16_04.nc and https://www.ncei.noaa.gov/data/oceans/woa/WOA23/DATA/temperature/netcdf/decav91C0/0.25/woa23_decav91C0_s13...16_04.nc on 02/01/2025. Transferred to JASMIN Object Store on 02/01/2025.")

print('Completed: Sent Seasonal WOA23 Climatologies to JASMIN Object Store.')

# -- Send WOA23 Monthly Climatologies to JASMIN Object Store -- #
print('In Progress: Sending WOA23 Monthly Climatologies to JASMIN Object Store...')

# -- Processing Climate Normal Monthly Climatologies -- #
# 1. WOA23 1971-2000 climate normal:
filepaths_sal, filepaths_temp = get_filepaths(file_directory=filedir, file_prefix='woa23_decav71A0')

ds_s71 = xr.open_mfdataset(filepaths_sal[1:13], decode_times=False)
ds_t71 = xr.open_mfdataset(filepaths_temp[1:13], decode_times=False)

ds_monthly71 = xr.merge([ds_s71, ds_t71])
ds_monthly71 = ds_monthly71.rename({'time': 'month'}).assign_coords({'month': np.arange(1, 13)})
ds_monthly71 = ds_monthly71.drop_vars('climatology_bounds')

# 2. WOA23 1981-2010 climate normal:
filepaths_sal, filepaths_temp = get_filepaths(file_directory=filedir, file_prefix='woa23_decav81B0')

ds_s81 = xr.open_mfdataset(filepaths_sal[1:13], decode_times=False)
ds_t81 = xr.open_mfdataset(filepaths_temp[1:13], decode_times=False)

ds_monthly81 = xr.merge([ds_s81, ds_t81])
ds_monthly81 = ds_monthly81.rename({'time': 'month'}).assign_coords({'month': np.arange(1, 13)})
ds_monthly81 = ds_monthly81.drop_vars('climatology_bounds')

# 3. WOA23 1991-2020 climate normal:
filepaths_sal, filepaths_temp = get_filepaths(file_directory=filedir, file_prefix='woa23_decav91C0')

ds_s91 = xr.open_mfdataset(filepaths_sal[1:13], decode_times=False)
ds_t91 = xr.open_mfdataset(filepaths_temp[1:13], decode_times=False)

ds_monthly91 = xr.merge([ds_s91, ds_t91])
ds_monthly91 = ds_monthly91.rename({'time': 'month'}).assign_coords({'month': np.arange(1, 13)})
ds_monthly91 = ds_monthly91.drop_vars('climatology_bounds')

# Send monthly climatologies to JASMIN Object Store:
send_ds_to_jasmin_os(ds=ds_monthly71, dest=f"{bucket}/WOA23/WOA23_1971_2000_monthly_climatology",
                     description="World Ocean Atlas 2023 1971-2000 Monthly Climatological mean temperature & salinity for the global ocean from in situ profile data. Downloaded from https://www.ncei.noaa.gov/data/oceans/woa/WOA23/DATA/temperature/netcdf/decav71A0/0.25/woa23_decav71A0_t01...12_04.nc and https://www.ncei.noaa.gov/data/oceans/woa/WOA23/DATA/temperature/netcdf/decav71A0/0.25/woa23_decav71A0_s01...12_04.nc on 02/01/2025. Transferred to JASMIN Object Store on 02/01/2025.")

send_ds_to_jasmin_os(ds=ds_monthly81, dest=f"{bucket}/WOA23/WOA23_1981_2010_monthly_climatology",
                     description="World Ocean Atlas 2023 1981-2010 Monthly Climatological mean temperature & salinity for the global ocean from in situ profile data. Downloaded from https://www.ncei.noaa.gov/data/oceans/woa/WOA23/DATA/temperature/netcdf/decav81B0/0.25/woa23_decav81B0_t01...12_04.nc and https://www.ncei.noaa.gov/data/oceans/woa/WOA23/DATA/temperature/netcdf/decav81B0/0.25/woa23_decav81B0_s01...12_04.nc on 02/01/2025. Transferred to JASMIN Object Store on 02/01/2025.")

send_ds_to_jasmin_os(ds=ds_monthly91, dest=f"{bucket}/WOA23/WOA23_1991_2020_monthly_climatology",
                     description="World Ocean Atlas 2023 1991-2020 Monthly Climatological mean temperature & salinity for the global ocean from in situ profile data. Downloaded from https://www.ncei.noaa.gov/data/oceans/woa/WOA23/DATA/temperature/netcdf/decav91C0/0.25/woa23_decav91C0_t01...12_04.nc and https://www.ncei.noaa.gov/data/oceans/woa/WOA23/DATA/temperature/netcdf/decav91C0/0.25/woa23_decav91C0_s01...12_04.nc on 02/01/2025. Transferred to JASMIN Object Store on 02/01/2025.")

print('Completed: Sent Monthly WOA23 Climatologies to JASMIN Object Store.')
