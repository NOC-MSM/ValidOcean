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

from ValidOcean.preprocess import _subset_data, _compute_climatology, _transform_longitudes

# -- DataLoader Abstract Base Class -- #
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
    region : str, default: ``None``
        Region of ocean observations dataset to load.
        Options include ``None``, ``arctic``, ``antarctic``.
    bounds : slice, str, default: None
        Time bounds to extract ocean observations.
        Default is ``None`` meaning the entire time series is loaded.
        Custom bounds should be specified using a slice object. Available
        pre-defined climatologies can be selected using a string
        (e.g., "1991-2020").
    freq : str, default: ``None``
        Climatology frequency of the ocean observations dataset.
        Options include ``None``, ``total``, ``seasonal``, ``monthly``.
        ``None`` means no climatology is computed from available data.

    Attributes
    ----------
    _var_name : str
        Name of variable to load from ocean observations dataset.
    _source : str
        Source of ocean observations data.
    _region : str
        Region of ocean observations dataset to load.
    _bounds : slice, str
        Time bounds to compute climatology using ocean observations.
    _freq : str
        Climatology frequency of the ocean observations dataset.
    """
    def __init__(self,
                 var_name: str,
                 source: str = 'jasmin-os',
                 region: str | None = None,
                 bounds: slice | str | None = None,
                 freq: str | None = None,
                 ):
        # -- Verify Inputs -- #
        if not isinstance(var_name, str):
            raise TypeError("``var_name`` must be a specfied as a string.")
        if not isinstance(source, str):
            raise TypeError("``source`` must be a specfied as a string.")
        if region is not None:
            if not isinstance(region, str):
                raise TypeError("``region`` must be a specfied as a string.")
        if bounds is not None:
            if not isinstance(bounds, slice) and not isinstance(bounds, str):
                raise TypeError("``bounds`` must be a specfied as either a slice or a string.")
            if isinstance(bounds, slice):
                if not isinstance(bounds.start, str):
                    raise TypeError("``bounds.start`` must be specified as a datetime string (e.g., 'YYYY-MM').")
                if not isinstance(bounds.stop, str):
                    raise TypeError("``bounds.stop`` must be specified as a datetime string (e.g., 'YYYY-MM')")
        if freq is not None:
            if not isinstance(freq, str):
                raise TypeError("``freq`` must be a specfied as a string.")
            if freq not in ['total', 'seasonal', 'monthly']:
                raise ValueError("``freq`` must be one of 'total', 'seasonal', 'monthly'.")
        
        # -- Define DataLoader Attributes -- #
        self._var_name = var_name
        self._bounds = bounds
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
        Options include ``sst``, ``siconc``.
    bounds : slice, str, default: None
        Time bounds to compute climatology using OISSTv2 ocean observations.
        Default is None, meaning the entire dataset is considered.
    freq : str, default: ``None``
        Climatology frequency of the OISSTv2 ocean observations.
        Options include ``None``, ``total``, ``seasonal``, ``monthly``.
        ``None`` means no climatology is computed from monthly data.
    """
    def __init__(self,
                 var_name: str = 'sst',
                 region: str | None = None,
                 bounds: slice | str | None = None,
                 freq: str | None = None,
                 ):
        # -- Verify Inputs -- #
        if var_name not in ['sst', 'siconc']:
            raise ValueError("``var_name`` must be one of 'sst', 'siconc'.")

        # -- Initialise DataLoader -- #
        super().__init__(var_name=var_name,
                         region=region,
                         source='jasmin-os',
                         bounds=bounds,
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
        if self._var_name == 'sst':
            if self._bounds == '1991-2020':
                url = f"{self._source}/OISSTv2/OISSTv2_sst_global_monthly_climatology_1991_2020/"
            elif isinstance(self._bounds, slice):
                url = f"{self._source}/OISSTv2/OISSTv2_sst_global_monthly_1981_2025/"
            else:
                raise ValueError("``bounds`` must be specified as a either a slice or string. Available pre-defined climatologies include: '1991-2020'.")

        elif self._var_name == 'siconc':
            if self._bounds == '1991-2020':
                url = f"{self._source}/OISSTv2/OISSTv2_siconc_global_monthly_climatology_1991_2020/"
            elif isinstance(self._bounds, slice):
                url = f"{self._source}/OISSTv2/OISSTv2_siconc_global_monthly_1981_2025/"
            else:
                raise ValueError("``bounds`` must be specified as a either a slice or string. Available pre-defined climatologies include: '1991-2020'.")

        data = xr.open_zarr(url, consolidated=True)[self._var_name]
    
        # Transform longitudes to [-180, 180]:
        data = _transform_longitudes(data)

        # Extract climatological period:
        if isinstance(self._bounds, slice):
            data = _subset_data(data, bounds=self._bounds)

        # Compute climatology:
        if self._freq is not None:
            data = _compute_climatology(data, freq=self._freq)

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
    bounds : slice, str, default: None
        Time bounds to compute climatology using NSIDC ocean observations.
        Default is None, meaning the entire dataset is considered.
    freq : str, default: ``total``
        Climatology frequency of the NSIDC ocean observations.
        Options include ``None``, ``total``, ``seasonal``, ``monthly``.
        ``None`` means no climatology is computed from monthly data.
    """
    def __init__(self,
                 var_name: str,
                 region: str = 'arctic',
                 bounds: slice | str | None = None,
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

        # -- Initialise DataLoader -- #
        super().__init__(var_name=var_name,
                         source='jasmin-os',
                         region=region,
                         bounds=bounds,
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

        # Compute climatology:
        if self._freq is not None:
            data = _compute_climatology(data, bounds=self._bounds, freq=self._freq)

        return data