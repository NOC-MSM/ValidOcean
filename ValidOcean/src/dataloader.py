"""
dataloader.py

Description: This module defines the DataLoader() class
including methods used to load ocean observations stored
in the JASMIN Object Store.

Author: Ollie Tooth (oliver.tooth@noc.ac.uk)
"""

# -- Import required packages -- #
import numpy as np
import xarray as xr

# -- Define DataLoader Class -- #
class DataLoader():
    """
    Create a DataLoader object to load ocean observations
    from the JASMIN Object Store or a local filesystem.

    Parameters
    ----------
    obs_name : str
        Name of ocean observations dataset to collect.
    freq : str, default: ``climatology``
        Output frequency of the ocean observations dataset.
        Options include ``climatology``, ``seasonal``, ``monthly``.
    """
    def __init__(self, obs_name:str, freq:str='climatology'):
        # -- Verify Inputs -- #
        if not isinstance(obs_name, str):
            raise TypeError("``obs_name`` must be a specfied as a string.")
        if not isinstance(freq, str):
            raise TypeError("``freq`` must be a specfied as a string.")
        
        # -- Define Class Attributes -- #
        self.obs_name = obs_name
        self.freq = freq
        self.jasmin_os_prefix = "https://noc-msm-o.s3-ext.jc.rl.ac.uk/npd-obs"

    
    def load_OISSTv2_data(self) -> xr.Dataset:
        """
        Load OISSTv2 sea surface temperature data from the JASMIN
        Object Store.

        Returns
        -------
        obs_data : xarray.Dataset
            xarray Dataset containing the OISSTv2 sea surface
            temperature data stored at specified frequency.
        """
        # Load monthly OISSTv2 data from the JASMIN Object Store:
        obs_var = 'sst'
        obs_url = f"{self.jasmin_os_prefix}/OISSTv2/OISSTv2_sst_global_monthly_climatology_1991_2020/"
        obs_data = xr.open_zarr(obs_url, consolidated=True)[obs_var]
        obs_data = obs_data.rename({'month': 'time'})

        # Transform time to datetime64 variable in OISSTv2 dataset:
        dates = np.datetime64('2001-01', 'M') + (np.timedelta64(1, 'M') * np.arange(obs_data['time'].size))
        obs_data['time'] = xr.DataArray(dates.astype('datetime64[ns]'), dims='time')
    
        # Convert OISSTv2 longitudes to [-180 to 180]:
        obs_data['lon'] = xr.where(obs_data['lon'] > 180, obs_data['lon'] - 360, obs_data['lon'])
        obs_data = obs_data.sortby('lon', ascending=True)

        # Calculate climatology of OISSTv2 data:
        if self.freq == 'climatology':
            obs_data = obs_data.mean(dim='time')
        elif self.freq == 'seasonal':
            obs_data = obs_data.groupby('time.season').mean()
        elif self.freq == 'monthly':
            obs_data = obs_data.groupby('time.month').mean()

        # Add time-period attributes to dataset:
        obs_data.attrs['start_date'] = '1991-01-01'
        obs_data.attrs['end_date'] = '2020-12-31'

        return obs_data
