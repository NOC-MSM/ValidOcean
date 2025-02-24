"""
dataloader.py

Description: This module defines the DataLoader() class
including methods used to load ocean observations stored
in the JASMIN Object Store.

Author: Ollie Tooth (oliver.tooth@noc.ac.uk)
"""

# -- Import required packages -- #
import abc
import numpy as np
import xarray as xr

# -- Utility Functions -- #
def _transform_longitudes(data: xr.DataArray) -> xr.DataArray:
    """
    Transform longitudes in a given xarray DataArray
    to the range [-180, 180].

    Parameters
    ----------
    data : xarray.DataArray
        DataArray containing longitudes variable, ``lon``.

    Returns
    -------
    xarray.DataArray
        DataArray containing longitudes transformed to
        the range [-180, 180].
    """
    # -- Verify Inputs -- #
    if not isinstance(data, xr.DataArray):
        raise TypeError("``data`` must be an xarray DataArray.")
    if 'lon' not in data.dims:
        raise ValueError("``data`` must contain variable ``lon``.")
    
    # -- Transform Longitudes -- #:
    data['lon'] = xr.where(data['lon'] > 180, data['lon'] - 360, data['lon'])
    data = data.sortby('lon', ascending=True)

    return data

def _compute_climatology(data: xr.DataArray, freq: str = 'total') -> xr.DataArray:
    """
    Compute the climatology of a given xarray DataArray
    at the specified frequency.

    Parameters
    ----------
    data : xarray.DataArray
        DataArray containing the variable to compute the
        climatology of.
    freq : str, default: ``total``
        Climatology frequency to compute. Options include
        ``total``, ``seasonal``, ``monthly``.

    Returns
    -------
    xarray.DataArray
        DataArray containing the climatology of the input
        data at the specified frequency.
    """
    # -- Verify Inputs -- #
    if not isinstance(data, xr.DataArray):
        raise TypeError("``data`` must be an xarray DataArray.")
    if 'time' not in data.dims:
        raise ValueError("``data`` must contain variable ``time``.")
    if not np.issubdtype(data.time.dtype, np.datetime64):
        raise TypeError("variable ``time`` dtype must be datetime64.")
    if not isinstance(freq, str):
        raise TypeError("``freq`` must be a string.")
    if freq not in ['total', 'seasonal', 'monthly']:
        raise ValueError("``freq`` must be one of 'total', 'seasonal', 'monthly'.")

    # -- Compute Climatology -- #
    if freq == 'total':
        data = data.mean(dim='time')
    elif freq == 'seasonal':
        data = data.groupby('time.season').mean()
    elif freq == 'monthly':
        data = data.groupby('time.month').mean()

    return data

# -- Define DataLoader Class -- #
class DataLoader(abc.ABC):
    """
    DataLoader Base Class to load ocean observations
    from cloud object storage or local filesystem.

    Parameters
    ----------
    var_name : str
        Name of variable to load from ocean observations dataset.
    source : str, default: ``jasmin-os``
        Source of ocean observations data. Default is ``jasmin-os``
        for pre-configured datasets stored in the JASMIN Object Store.
        Options include specifying the directory path for ocean
        observations stored in a local filesystem or a url path
        for ocean observations stored by an alternative cloud
        provider.
    region : str, default: ``global``
        Region of ocean observations dataset to load.
        Options include ``global``, ``arctic``, ``antarctic``.
    freq : str, default: ``total``
        Climatology frequency of the ocean observations dataset.
        Options include ``total``, ``seasonal``, ``monthly``.

    Attributes
    ----------
    _var_name : str
        Name of variable to load from ocean observations dataset.
    _source : str
        Source of ocean observations data.
    _region : str
        Region of ocean observations dataset to load.
    _freq : str
        Climatology frequency of the ocean observations dataset.
    """
    def __init__(self,
                 var_name: str,
                 source: str = 'jasmin-os',
                 region: str | None = None,
                 freq: str = 'total'
                 ):
        # -- Verify Inputs -- #
        if not isinstance(var_name, str):
            raise TypeError("``var_name`` must be a specfied as a string.")
        if not isinstance(source, str):
            raise TypeError("``source`` must be a specfied as a string.")
        if region is not None:
            if not isinstance(region, str):
                raise TypeError("``region`` must be a specfied as a string.")
        if not isinstance(freq, str):
            raise TypeError("``freq`` must be a specfied as a string.")
        
        # -- Define DataLoader Attributes -- #
        self._var_name = var_name
        self._freq = freq
        self._region = region
        if source == 'jasmin-os':
            self._source = "https://noc-msm-o.s3-ext.jc.rl.ac.uk/npd-obs"
        else:
            self._source = source

    @abc.abstractmethod
    def _load_data(self) -> xr.DataArray:
        """
        Abstract method to load ocean observations data
        from cloud object storage or local filesystem.

        Returns
        -------
        xarray.DataArray
            DataArray storing the chosen ocean observation
            variable climatology at specified frequency.
        """
        pass

# -- OISSTv2 DataLoader -- #
class OISSTv2Loader(DataLoader):
    """
    DataLoader to load OISSTv2 sea surface temperature
    observations stored in the JASMIN Object Store.

    Parameters
    ----------
    var_name : str, default: ``sst``
        Name of variable to load from OISSTv2 ocean observations.
    freq : str, default: ``total``
        Climatology frequency of the OISSTv2 ocean observations.
        Options include ``total``, ``seasonal``, ``monthly``.
    """
    def __init__(self,
                 var_name: str = 'sst',
                 freq: str = 'total'
                 ):

        # -- Initialise DataLoader -- #
        super().__init__(var_name=var_name,
                         source='jasmin-os',
                         freq=freq)

    def _load_data(self) -> xr.DataArray:
        """
        Load OISSTv2 sea surface temperature climatology
        from observations stored in the JASMIN Object Store.

        Returns
        -------
        xarray.DataArray
            Dataset storing OISSTv2 sea surface temperature
            climatology at specified frequency.
        """
        # Load data from the JASMIN Object Store:
        url = f"{self._source}/OISSTv2/OISSTv2_sst_global_monthly_climatology_1991_2020/"
        data = xr.open_zarr(url, consolidated=True)[self._var_name]

        # Transform time to datetime64:
        data = data.rename({'month': 'time'})
        dates = np.datetime64('2001-01', 'M') + (np.timedelta64(1, 'M') * np.arange(data['time'].size))
        data['time'] = xr.DataArray(dates.astype('datetime64[ns]'), dims='time')
    
        # Transform longitudes to [-180, 180]:
        data = _transform_longitudes(data)

        # Compute climatology:
        data = _compute_climatology(data, freq=self._freq)

        # Add climatology time-window attributes:
        data.attrs['start_time'] = '1991-01-01'
        data.attrs['end_time'] = '2020-12-31'

        return data
    
# -- CCI DataLoader -- #
class CCILoader(DataLoader):
    """
    DataLoader to load CCI sea surface temperature or
    sea ice fraction observations stored in the JASMIN 
    Object Store.

    Parameters
    ----------
    var_name : str
        Name of variable to load from CCI ocean observations.
        Options include ``sst``, ``siconc``.
    freq : str, default: ``total``
        Climatology frequency of the CCI ocean observations.
        Options include ``total``, ``seasonal``, ``monthly``.
    """
    def __init__(self,
                 var_name: str,
                 freq: str = 'total'
                 ):
        # -- Verify Inputs -- #
        if not isinstance(var_name, str):
            raise TypeError("``var_name`` must be a specfied as a string.")
        if var_name not in ['sst', 'siconc']:
            raise ValueError("``var_name`` must be one of 'sst', 'siconc'.")
        if not isinstance(freq, str):
            raise TypeError("``freq`` must be a specfied as a string.")
        if freq not in ['total', 'seasonal', 'monthly']:
            raise ValueError("``freq`` must be one of 'total', 'seasonal', 'monthly'.")

        # -- Initialise DataLoader -- #
        if var_name == 'sst':
            super().__init__(var_name='analysed_sst',
                             source='jasmin-os',
                             freq=freq)
        elif var_name == 'siconc':
            super().__init__(var_name='sea_ice_fraction',
                             source='jasmin-os',
                             freq=freq)

    def _load_data(self) -> xr.DataArray:
        """
        Load CCI variable climatology from observations
        stored in the JASMIN Object Store.

        Returns
        -------
        xarray.DataArray
            Dataset storing CCI variable
            climatology at specified frequency.
        """
        # Load data from the JASMIN Object Store:
        if self._freq == "total":
            url = f"{self._source}/CCI/ESACCI-v3.0-SST_global_climatology_1991_2020/"
        elif self._freq == "seasonal":
            url = f"{self._source}/CCI/ESACCI-v3.0-SST_global_seasonal_climatology_1991_2020/"
        elif self._freq == "monthly":
            url = f"{self._source}/CCI/ESACCI-v3.0-SST_global_monthly_climatology_1991_2020/"

        data = xr.open_zarr(url, consolidated=True)[self._var_name]

        # Standardise variable name:
        if self._var_name == 'analysed_sst':
            data = data.rename({'sst'})
        elif self._var_name == 'sea_ice_fraction':
            data = data.rename({'siconc'})

        # Add climatology time-window attributes:
        data.attrs['start_time'] = '1991-01-01'
        data.attrs['end_time'] = '2020-12-31'

        return data

# -- NSIDC DataLoader -- #
class NSIDCLoader(DataLoader):
    """
    DataLoader to load NSIDC sea ice concentration, sea
    ice extent and sea ice area observations stored in the
    JASMIN Object Store.

    Parameters
    ----------
    var_name : str
        Name of variable to load from NSIDC observations.
        Options include ``siconc``, ``siext``, ``siarea``.
    region : str, default: ``arctic``
        Region of NSIDC observations dataset to load.
        Options include ``arctic``, ``antarctic``.
    freq : str, default: ``total``
        Climatology frequency of the CCI ocean observations.
        Options include ``total``, ``seasonal``, ``monthly``.
    """
    def __init__(self,
                 var_name: str,
                 region: str = 'arctic',
                 freq: str = 'total'
                 ):
        # -- Verify Inputs -- #
        if not isinstance(var_name, str):
            raise TypeError("``var_name`` must be a specfied as a string.")
        if var_name not in ['siconc', 'siext', 'siarea']:
            raise ValueError("``var_name`` must be one of 'siconc', 'siext', 'siarea'.")
        if not isinstance(region, str):
            raise TypeError("``region`` must be a specfied as a string.")
        if region not in ['arctic', 'antarctic']:
            raise ValueError("``region`` must be one of 'arctic', 'antarctic'.")
        if not isinstance(freq, str):
            raise TypeError("``freq`` must be a specfied as a string.")
        if freq not in ['total', 'seasonal', 'monthly']:
            raise ValueError("``freq`` must be one of 'total', 'seasonal', 'monthly'.")

        # -- Initialise DataLoader -- #
        super().__init__(var_name=var_name,
                         source='jasmin-os',
                         region=region,
                         freq=freq)

    def _load_data(self) -> xr.DataArray:
        """
        Load NSIDC variable climatology from observations
        stored in the JASMIN Object Store.

        Returns
        -------
        xarray.DataArray
            Dataset storing NSIDC variable
            climatology at specified frequency.
        """
        # Load data from the JASMIN Object Store:
        if self._region == "arctic":
            url = f"{self._source}/npd-obs/NSIDC/NSIDC_Sea_Ice_Index_v3_Arctic_1978_2025/"
        elif self._freq == "antarctic":
            url = f"{self._source}/npd-obs/NSIDC/NSIDC_Sea_Ice_Index_v3_Antarctic_1978_2025/"

        data = xr.open_zarr(url, consolidated=True)[self._var_name]

        # Add climatology time-window attributes:
        data.attrs['start_time'] = '1991-01-01'
        data.attrs['end_time'] = '2020-12-31'

        return data