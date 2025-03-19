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

from ValidOcean.processing import _get_spatial_bounds, _apply_spatial_bounds, _apply_time_bounds, _compute_climatology, _transform_longitudes

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
    time_bounds : slice, str, default: None
        Time bounds to extract ocean observations.
        Default is ``None`` meaning the entire time series is loaded.
        Custom bounds should be specified using a slice object. Available
        pre-defined climatologies can be selected using a string
        (e.g., "1991-2020").
    lon_bounds : tuple, default: None
        Longitude bounds to extract from ocean observations.
        Default is ``None`` meaning the entire longitude range is loaded.
    lat_bounds : tuple, default: None
        Latitude bounds to extract from ocean observations.
        Default is ``None`` meaning the entire latitude range is loaded.
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
    _time_bounds : slice, str
        Time bounds to compute climatology using ocean observations.
    _lon_bounds : tuple
        Longitude bounds to extract from ocean observations.
    _lat_bounds : tuple
        Latitude bounds to extract from ocean observations.
    _freq : str
        Climatology frequency of the ocean observations dataset.
    """
    def __init__(self,
                 var_name: str,
                 source: str = 'jasmin-os',
                 region: str | None = None,
                 time_bounds: slice | str | None = None,
                 lon_bounds: tuple | None = None,
                 lat_bounds: tuple | None = None,
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
        if time_bounds is not None:
            if not isinstance(time_bounds, slice) and not isinstance(time_bounds, str):
                raise TypeError("``time_bounds`` must be a specfied as either a slice or a string.")
            if isinstance(time_bounds, slice):
                if not isinstance(time_bounds.start, str):
                    raise TypeError("``time_bounds.start`` must be specified as a datetime string (e.g., 'YYYY-MM').")
                if not isinstance(time_bounds.stop, str):
                    raise TypeError("``time_bounds.stop`` must be specified as a datetime string (e.g., 'YYYY-MM')")
        if lon_bounds is not None:
            if not isinstance(lon_bounds, tuple):
                raise TypeError("``lon_bounds`` must be a specfied as a tuple.")
        if lat_bounds is not None:
            if not isinstance(lat_bounds, tuple):
                raise TypeError("``lat_bounds`` must be a specfied as a tuple.")
        if freq is not None:
            if not isinstance(freq, str):
                raise TypeError("``freq`` must be a specfied as a string.")
        
        # -- Class Attributes -- #
        self._var_name = var_name
        self._time_bounds = time_bounds
        self._freq = freq
        self._region = region

        if source == 'jasmin-os':
            self._source = "https://noc-msm-o.s3-ext.jc.rl.ac.uk/npd-obs"
        else:
            self._source = source

        if lon_bounds is None:
            self._lon_bounds = (-180, 180)
        else:
            self._lon_bounds = lon_bounds
        if lat_bounds is None:
            self._lat_bounds = (-90, 90)
        else:
            self._lat_bounds = lat_bounds

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
    time_bounds : slice, str, default: None
        Time bounds to compute climatology using OISSTv2 ocean observations.
        Default is ``None``, meaning the entire dataset is considered.
    lon_bounds : tuple, default: None
        Longitude bounds to extract from OISSTv2 ocean observations.
        Default is ``None``, meaning the entire longitude range is loaded.
    lat_bounds : tuple, default: None
        Latitude bounds to extract from OISSTv2 ocean observations.
        Default is ``None``, meaning the entire latitude range is loaded.
    freq : str, default: ``None``
        Climatology frequency of the OISSTv2 ocean observations.
        Options include ``None``, ``total``, ``seasonal``, ``monthly``,
        ``jan``, ``feb`` etc. for individual months. Default is``None``
        meaning no climatology is computed from monthly data.
    """
    def __init__(self,
                 var_name: str = 'sst',
                 region: str | None = None,
                 time_bounds: slice | str | None = None,
                 lon_bounds: tuple | None = None,
                 lat_bounds: tuple | None = None,
                 freq: str | None = None,
                 ):
        # -- Verify Inputs -- #
        if var_name not in ['sst', 'siconc']:
            raise ValueError("``var_name`` must be one of 'sst', 'siconc'.")

        # -- Initialise DataLoader -- #
        super().__init__(var_name=var_name,
                         region=region,
                         source='jasmin-os',
                         time_bounds=time_bounds,
                         lon_bounds=lon_bounds,
                         lat_bounds=lat_bounds,
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
            if self._time_bounds == '1991-2020':
                url = f"{self._source}/OISSTv2/OISSTv2_sst_global_monthly_climatology_1991_2020/"
            elif isinstance(self._time_bounds, slice):
                url = f"{self._source}/OISSTv2/OISSTv2_sst_global_monthly_1981_2025/"
            else:
                raise ValueError("``_time_bounds`` must be specified as a either a slice or string. Available pre-defined climatologies include: '1991-2020'.")

        elif self._var_name == 'siconc':
            if self._time_bounds == '1991-2020':
                url = f"{self._source}/OISSTv2/OISSTv2_siconc_global_monthly_climatology_1991_2020/"
            elif isinstance(self._time_bounds, slice):
                url = f"{self._source}/OISSTv2/OISSTv2_siconc_global_monthly_1981_2025/"
            else:
                raise ValueError("``_time_bounds`` must be specified as a either a slice or string. Available pre-defined climatologies include: '1991-2020'.")

        # Data to inherit source attributes:
        source = xr.open_zarr(url, consolidated=True)
        data = source[self._var_name]
        data.attrs = source.attrs
    
        # Transform longitudes to [-180, 180]:
        data = _transform_longitudes(data)

        # Extract observations for specified time, longitude and latitude bounds:
        data = _apply_spatial_bounds(data, lon_bounds=self._lon_bounds, lat_bounds=self._lat_bounds)

        if isinstance(self._time_bounds, slice):
            data = _apply_time_bounds(data, time_bounds=self._time_bounds)

        # Compute climatology:
        if self._freq is not None:
            data = _compute_climatology(data, freq=self._freq)

        # Add spatial bounds to attributes:
        data.attrs["lon_bounds"], data.attrs["lat_bounds"] = _get_spatial_bounds(lon=data["lon"], lat=data["lat"])

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
    time_bounds : slice, str, default: None
        Time bounds to compute climatology using NSIDC sea ice observations.
        Default is ``None``, meaning the entire dataset is considered.
    lon_bounds : tuple, default: None
        Longitude bounds to extract from NSIDC sea ice observations.
        Default is ``None``, meaning the entire longitude range is loaded.
    lat_bounds : tuple, default: None
        Latitude bounds to extract from NSIDC sea ice observations.
        Default is ``None``, meaning the entire latitude range is loaded.
    freq : str, default: ``total``
        Climatology frequency of the NSIDC ocean observations.
        Options include ``None``, ``total``, ``seasonal``, ``monthly``
        , ``jan``, ``feb`` etc. for individual months. Default is
        ``None`` meaning no climatology is computed from monthly data.
    """
    def __init__(self,
                 var_name: str,
                 region: str = 'arctic',
                 time_bounds: slice | str | None = None,
                 lon_bounds: tuple | None = None,
                 lat_bounds: tuple | None = None,
                 freq: str = 'total'
                 ):
        # -- Verify Inputs -- #
        if not isinstance(var_name, str):
            raise TypeError("``var_name`` must be specfied as a string.")
        if var_name not in ['siconc', 'siext', 'siarea']:
            raise ValueError("``var_name`` must be one of 'siconc', 'siext', 'siarea'.")
        if not isinstance(region, str):
            raise TypeError("``region`` must be specfied as a string.")
        if region not in ['arctic', 'antarctic']:
            raise ValueError("``region`` must be one of 'arctic', 'antarctic'.")

        # -- Initialise DataLoader -- #
        super().__init__(var_name=var_name,
                         source='jasmin-os',
                         region=region,
                         time_bounds=time_bounds,
                         lon_bounds=lon_bounds,
                         lat_bounds=lat_bounds,
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
            url = f"{self._source}/NSIDC/NSIDC_Sea_Ice_Index_v3_Arctic_1978_2025/"
        elif self._region == "antarctic":
            url = f"{self._source}/NSIDC/NSIDC_Sea_Ice_Index_v3_Antarctic_1978_2025/"

        # Data to inherit source attributes:
        source = xr.open_zarr(url, consolidated=True)
        data = source[self._var_name]
        data.attrs = source.attrs

        # Extract observations for specified time, longitude and latitude bounds:
        if ('x' in data.dims) & ('y' in data.dims):
            data = _apply_spatial_bounds(data, lon_bounds=self._lon_bounds, lat_bounds=self._lat_bounds)

        if isinstance(self._time_bounds, slice):
            data = _apply_time_bounds(data, time_bounds=self._time_bounds)

        # Compute climatology:
        if self._freq is not None:
            data = _compute_climatology(data, freq=self._freq)

        # Add spatial bounds to attributes:
        data.attrs["lon_bounds"], data.attrs["lat_bounds"] = _get_spatial_bounds(lon=source["lon"], lat=source["lat"])

        return data