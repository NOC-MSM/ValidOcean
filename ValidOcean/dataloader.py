"""
dataloader.py

Description: This module defines the DataLoader() class
including methods used to load ocean observations stored
in the JASMIN Object Store.

Author: Ollie Tooth (oliver.tooth@noc.ac.uk)
"""

# -- Import required packages -- #
import abc
import xarray as xr

from ValidOcean.processing import _get_spatial_bounds, _apply_geographic_bounds, _apply_depth_bounds, _apply_time_bounds, _compute_climatology, _transform_longitudes

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
    depth_bounds : tuple, default: None
        Depth bounds to extract from ocean observations.
        Default is ``None`` meaning the entire depth range is loaded where
        a depth axis is available.
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
    _depth_bounds : tuple
        Depth bounds to extract from ocean observations.
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
                 depth_bounds: tuple | None = None,
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
        if depth_bounds is not None:
            if not isinstance(depth_bounds, tuple):
                raise TypeError("``depth_bounds`` must be a specfied as a tuple.")
        if freq is not None:
            if not isinstance(freq, str):
                raise TypeError("``freq`` must be a specfied as a string.")
        
        # -- Class Attributes -- #
        self._var_name = var_name
        self._time_bounds = time_bounds
        self._freq = freq
        self._region = region

        if source == 'jasmin-os':
            self._source = "https://noc-msm-o.s3-ext.jc.rl.ac.uk/ocean-obs"
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
        if depth_bounds is None:
            self._depth_bounds = None
        else:
            self._depth_bounds = depth_bounds

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
    & sea ice concentration observations stored in the
    JASMIN Object Store.

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
        if region is not None:
            if not isinstance(region, str):
                raise TypeError("``region`` must be specfied as a string.")

        # -- Define Region -- #
        if region is not None:
            if region == 'arctic':
                lon_bounds = (-180, 180)
                lat_bounds = (60, 90)
            elif region == 'antarctic':
                lon_bounds = (-180, 180)
                lat_bounds = (-90, -60)
            else:
                raise ValueError("``region`` must be one of 'arctic', 'antarctic'.")

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
        Load OISSTv2 ocean observations data & optionally
        compute climatology using the JASMIN Object Store.

        Returns
        -------
        xarray.DataArray
            Dataset storing OISSTv2 sea surface temperature
            climatology at specified frequency.
        """
        # Define OISSTv2 S3 url:
        if self._var_name == 'sst':
            if self._time_bounds == '1991-2020':
                url = f"{self._source}/OISSTv2/OISSTv2_sst_global_climatology_1991_2020/"
            elif isinstance(self._time_bounds, slice):
                url = f"{self._source}/OISSTv2/OISSTv2_sst_global_monthly/"
            else:
                raise ValueError("``_time_bounds`` must be specified as a either a slice or string. Available pre-defined climatologies include: '1991-2020'.")

        elif self._var_name == 'siconc':
            if self._time_bounds == '1991-2020':
                url = f"{self._source}/OISSTv2/OISSTv2_siconc_global_climatology_1991_2020/"
            elif isinstance(self._time_bounds, slice):
                url = f"{self._source}/OISSTv2/OISSTv2_siconc_global_monthly/"
            else:
                raise ValueError("``_time_bounds`` must be specified as a either a slice or string. Available pre-defined climatologies include: '1991-2020'.")

        # Load data from the JASMIN Object Store:
        source = xr.open_zarr(url, zarr_format=3, consolidated=True)
        data = source[self._var_name]
        data = _transform_longitudes(data)

        # Extract observations for specified time, longitude and latitude bounds:
        data = _apply_geographic_bounds(data, lon_bounds=self._lon_bounds, lat_bounds=self._lat_bounds)

        if isinstance(self._time_bounds, slice):
            data = _apply_time_bounds(data, time_bounds=self._time_bounds)

        # Compute climatology:
        if self._freq is not None:
            data = _compute_climatology(data, freq=self._freq)

        # Inherit CF global attributes:
        global_attrs = {key: source.attrs[key] for key in ['Conventions', 'title', 'institution', 'source', 'history', 'references', 'comment'] if key in source.attrs}
        data = data.assign_attrs(global_attrs)
        # Append spatial bound attributes:
        data.attrs["lon_bounds"], data.attrs["lat_bounds"], data.attrs["depth_bounds"] = _get_spatial_bounds(lon=data["lon"], lat=data["lat"])

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
        if var_name not in ['siconc', 'simask', 'siarea']:
            raise ValueError("``var_name`` must be one of 'siconc', 'simask', 'siarea'.")
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
        Load NSIDC sea ice observations data & optionally
        compute climatology using the JASMIN Object Store.

        Returns
        -------
        xarray.DataArray
            Dataset storing NSIDC variable
            climatology at specified frequency.
        """
        # Define NSIDC S3 url:
        if self._region == "arctic":
            url = f"{self._source}/NSIDC/NSIDC_Sea_Ice_Index_v3_Arctic/"
        elif self._region == "antarctic":
            url = f"{self._source}/NSIDC/NSIDC_Sea_Ice_Index_v3_Antarctic/"

        # Load data from the JASMIN Object Store:
        source = xr.open_zarr(url, zarr_format=3, consolidated=True)
        data = source[self._var_name]

        # Extract observations for specified time, longitude and latitude bounds:
        if ('x' in data.dims) & ('y' in data.dims):
            data = _apply_geographic_bounds(data, lon_bounds=self._lon_bounds, lat_bounds=self._lat_bounds)

        # Extract observations for specified time bounds:
        if isinstance(self._time_bounds, str):
            self._time_bounds = slice(self._time_bounds.split('-')[0], self._time_bounds.split('-')[1])
        if isinstance(self._time_bounds, slice):
            data = _apply_time_bounds(data, time_bounds=self._time_bounds)

        # Compute climatology:
        if self._freq is not None:
            data = _compute_climatology(data, freq=self._freq)

        # Inherit CF global attributes:
        global_attrs = {key: source.attrs[key] for key in ['Conventions', 'title', 'institution', 'source', 'history', 'references', 'comment'] if key in source.attrs}
        data = data.assign_attrs(global_attrs)
        # Append spatial bound attributes:
        if ('x' in data.dims) & ('y' in data.dims):
            data.attrs["lon_bounds"], data.attrs["lat_bounds"], data.attrs["depth_bounds"] = _get_spatial_bounds(lon=data["lon"], lat=data["lat"])
        else:
            data.attrs["lon_bounds"], data.attrs["lat_bounds"], data.attrs["depth_bounds"] = _get_spatial_bounds(lon=source["lon"], lat=source["lat"])

        return data


# -- HadISST DataLoader -- #
class HadISSTLoader(DataLoader):
    """
    DataLoader to load HadISST sea surface temperature
    & sea ice concentration observations stored in the
    JASMIN Object Store.

    Parameters
    ----------
    var_name : str, default: ``sst``
        Name of variable to load from HadISST ocean observations.
        Options include ``sst``, ``siconc``.
    time_bounds : slice, str, default: None
        Time bounds to compute climatology using HadISST ocean observations.
        Default is ``None``, meaning the entire dataset is considered.
    lon_bounds : tuple, default: None
        Longitude bounds to extract from HadISST ocean observations.
        Default is ``None``, meaning the entire longitude range is loaded.
    lat_bounds : tuple, default: None
        Latitude bounds to extract from HadISST ocean observations.
        Default is ``None``, meaning the entire latitude range is loaded.
    freq : str, default: ``None``
        Climatology frequency of the HadISST ocean observations.
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
        if region is not None:
            if not isinstance(region, str):
                raise TypeError("``region`` must be specfied as a string.")

        # -- Define Region -- #
        if region is not None:
            if region == 'arctic':
                lon_bounds = (-180, 180)
                lat_bounds = (60, 90)
            elif region == 'antarctic':
                lon_bounds = (-180, 180)
                lat_bounds = (-90, -60)
            else:
                raise ValueError("``region`` must be one of 'arctic', 'antarctic'.")

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
        Load HadISST ocean observations data & optionally
        compute climatology using the JASMIN Object Store.

        Returns
        -------
        xarray.DataArray
            Dataset storing HadISST sea surface temperature
            climatology at specified frequency.
        """
        # Define HadISST S3 url:
        url = f"{self._source}/HadISST/HadISST_global_monthly/"

        # Load data from the JASMIN Object Store:
        source = xr.open_zarr(url, zarr_format=3, consolidated=True)
        data = source[self._var_name]

        # Extract observations for specified time, longitude and latitude bounds:
        data = _apply_geographic_bounds(data, lon_bounds=self._lon_bounds, lat_bounds=self._lat_bounds)

        # Extract observations for specified time bounds:
        if isinstance(self._time_bounds, str):
            self._time_bounds = slice(self._time_bounds.split('-')[0], self._time_bounds.split('-')[1])
        if isinstance(self._time_bounds, slice):
            data = _apply_time_bounds(data, time_bounds=self._time_bounds)

        # Compute climatology:
        if self._freq is not None:
            data = _compute_climatology(data, freq=self._freq)

        # Inherit CF global attributes:
        global_attrs = {key: source.attrs[key] for key in ['Conventions', 'title', 'institution', 'source', 'history', 'references', 'comment'] if key in source.attrs}
        data = data.assign_attrs(global_attrs)
        # Append spatial bound attributes:
        data.attrs["lon_bounds"], data.attrs["lat_bounds"], data.attrs["depth_bounds"] = _get_spatial_bounds(lon=data["lon"], lat=data["lat"])

        return data


# -- World Ocean Atlas DataLoader -- #
class WOA23Loader(DataLoader):
    """
    DataLoader to load World Ocean Atlas 2023 sea water in-situ temperature
    & practical salinity observations stored in the JASMIN Object Store.

    Parameters
    ----------
    var_name : str, default: ``temp``
        Name of variable to load from WOA23 ocean observations.
        Options include ``temp``, ``sal``.
    time_bounds : str, default: ``1971-2000``
        Time bounds of WOA23 climate normal. Options include ``1971-2000``,
        ``1981-2010`` and ``1991-2020``.
    lon_bounds : tuple, default: None
        Longitude bounds to extract from WOA23 ocean observations.
        Default is ``None``, meaning the entire longitude range is loaded.
    lat_bounds : tuple, default: None
        Latitude bounds to extract from WOA23 ocean observations.
        Default is ``None``, meaning the entire latitude range is loaded.
    depth_bounds : tuple, default: None
        Depth bounds to extract from WOA23 ocean observations.
        Default is ``None``, meaning the entire depth range is loaded.
    freq : str, default: ``total``
        Climatology frequency of the WOA23 ocean observations.
        Options include ``total``, ``seasonal``, ``monthly``.
    """
    def __init__(self,
                 var_name: str = 'temp',
                 region: str | None = None,
                 time_bounds: str = '1971-2000',
                 lon_bounds: tuple | None = None,
                 lat_bounds: tuple | None = None,
                 depth_bounds: tuple | None = None,
                 freq: str = 'total',
                 ):
        # -- Verify Inputs -- #
        if var_name not in ['sst', 'sss', 'temp', 'sal']:
            raise ValueError("``var_name`` must be one of 'sst', 'sss', 'temp' or 'sal'.")
        if not isinstance(time_bounds, str):
            raise TypeError("``time_bounds`` must be specified as a string.")
        if time_bounds not in ['1971-2000', '1981-2010', '1991-2020']:
            raise ValueError("``time_bounds`` must be one of '1971-2000', '1981-2010', '1991-2020'.")
        if freq not in ['total', 'seasonal', 'monthly']:
            raise ValueError("``freq`` must be one of 'total', 'seasonal', 'monthly'.")
        
        # -- Define Sea Surface Variables -- #
        if var_name == 'sst':
            var_name = 'temp'
            depth_bounds = (0, 0)
        elif var_name == 'sss':
            var_name = 'sal'
            depth_bounds = (0, 0)

        # -- Initialise DataLoader -- #
        super().__init__(var_name=var_name,
                         region=region,
                         source='jasmin-os',
                         time_bounds=time_bounds,
                         lon_bounds=lon_bounds,
                         lat_bounds=lat_bounds,
                         depth_bounds=depth_bounds,
                         freq=freq)

    def _load_data(self) -> xr.DataArray:
        """
        Load WOA23 ocean observations data from
        the JASMIN Object Store.

        Returns
        -------
        xarray.DataArray
            Dataset storing World Ocean Atlas 2023 Climate Normal sea water
            in-situ temperature or practical salinity climatology.
        """
        # Define WOA23 S3 url:
        if (self._freq is None) | (self._freq == 'total'):
            self._freq = 'annual'
        url = f"{self._source}/WOA23/WOA23_{self._time_bounds.replace('-', '_')}_{self._freq}_climatology/"

        # Define WOA23 variable names:
        var_names = {"temp": "t_an", "sal": "s_an"}

        # Load data from the JASMIN Object Store:
        source = xr.open_zarr(url, zarr_format=3, consolidated=True)
        data = source[var_names[self._var_name]].squeeze()

        # Extract observations for specified longitude, latitude & depth bounds:
        data = _apply_geographic_bounds(data, lon_bounds=self._lon_bounds, lat_bounds=self._lat_bounds)
        if self._depth_bounds is not None:
            data = _apply_depth_bounds(data, depth_bounds=self._depth_bounds).squeeze()

        # Inherit CF global attributes:
        global_attrs = {key: source.attrs[key] for key in ['Conventions', 'title', 'institution', 'source', 'history', 'references', 'comment'] if key in source.attrs}
        data = data.assign_attrs(global_attrs)
        # Append spatial bound attributes:
        data.attrs["lon_bounds"], data.attrs["lat_bounds"], data.attrs["depth_bounds"] = _get_spatial_bounds(lon=data["lon"], lat=data["lat"], depth=data["depth"])

        return data


# -- ARMOR3D DataLoader -- #
class ARMOR3DLoader(DataLoader):
    """
    DataLoader to load Multi Observation Global Ocean ARMOR3D L4 analysis
    sea water temperature, practical salinity & mixed layer depth observations
    stored in the JASMIN Object Store.

    Parameters
    ----------
    var_name : str, default: ``temp``
        Name of variable to load from ARMOR3D ocean observations.
        Options include ``sst``, ``sss``, ``temp``, ``sal`` & ``mld``.
    time_bounds : slice, str, default: None
        Time bounds to compute climatology using ARMOR3D ocean observations.
        Default is ``None``, meaning the entire dataset is considered.
    lon_bounds : tuple, default: None
        Longitude bounds to extract from ARMOR3D ocean observations.
        Default is ``None``, meaning the entire longitude range is loaded.
    lat_bounds : tuple, default: None
        Latitude bounds to extract from ARMOR3D ocean observations.
        Default is ``None``, meaning the entire latitude range is loaded.
    depth_bounds : tuple, default: None
        Depth bounds to extract from ARMOR3D ocean observations.
        Default is ``None``, meaning the entire depth range is loaded.
    freq : str, default: ``None``
        Climatology frequency of the ARMOR3D ocean observations.
        Options include ``None``, ``total``, ``seasonal``, ``monthly``,
        ``jan``, ``feb`` etc. for individual months. Default is``None``
        meaning no climatology is computed from monthly data.
    """
    def __init__(self,
                 var_name: str = 'temp',
                 region: str | None = None,
                 time_bounds: slice | str | None = None,
                 lon_bounds: tuple | None = None,
                 lat_bounds: tuple | None = None,
                 depth_bounds: tuple | None = None,
                 freq: str | None = None,
                 ):
        # -- Verify Inputs -- #
        if var_name not in ['sst', 'sss', 'temp', 'sal', 'mld']:
            raise ValueError("``var_name`` must be one of 'sst', 'sss', 'temp', 'sal', 'mld.")
        
        # -- Define Sea Surface Variables -- #
        if var_name == 'sst':
            var_name = 'temp'
            depth_bounds = (0, 0)
        elif var_name == 'sss':
            var_name = 'sal'
            depth_bounds = (0, 0)

        # -- Initialise DataLoader -- #
        super().__init__(var_name=var_name,
                         region=region,
                         source='jasmin-os',
                         time_bounds=time_bounds,
                         lon_bounds=lon_bounds,
                         lat_bounds=lat_bounds,
                         depth_bounds=depth_bounds,
                         freq=freq)

    def _load_data(self) -> xr.DataArray:
        """
        Load ARMOR3D ocean observations data from
        the JASMIN Object Store.

        Returns
        -------
        xarray.DataArray
            Dataset storing ARMOR3D L4 analysis sea water
            temperature, practical salinity or mixed layer
            depth climatology.
        """
        # Define ARMOR3D S3 url:
        url = f"{self._source}/ARMOR3D/ARMOR3D_RP_global_monthly_1993_2022/"

        # Define ARMOR3D variable names:
        var_names = {"temp": "to", "sal": "so", "mld": "mlotst"}

        # Load data from the JASMIN Object Store:
        source = xr.open_zarr(url, zarr_format=3, consolidated=True)
        source = source.rename({"longitude": "lon", "latitude": "lat"})
        data = source[var_names[self._var_name]]
        data.name = self._var_name

        # Extract observations for specified longitude, latitude & depth bounds:
        data = _apply_geographic_bounds(data, lon_bounds=self._lon_bounds, lat_bounds=self._lat_bounds)
        if ('depth' in data.dims) & (self._depth_bounds is not None):
            data = _apply_depth_bounds(data, depth_bounds=self._depth_bounds).squeeze()

        # Extract observations for specified time bounds:
        if isinstance(self._time_bounds, str):
            self._time_bounds = slice(self._time_bounds.split('-')[0], self._time_bounds.split('-')[1])
        if isinstance(self._time_bounds, slice):
            data = _apply_time_bounds(data, time_bounds=self._time_bounds)

        # Compute climatology:
        if self._freq is not None:
            data = _compute_climatology(data, freq=self._freq)

        # Inherit CF global attributes:
        global_attrs = {key: source.attrs[key] for key in ['Conventions', 'title', 'institution', 'source', 'history', 'references', 'comment'] if key in source.attrs}
        data = data.assign_attrs(global_attrs)
        # Append spatial bound attributes:
        if 'depth' in data.dims:
            data.attrs["lon_bounds"], data.attrs["lat_bounds"], data.attrs["depth_bounds"] = _get_spatial_bounds(lon=data["lon"], lat=data["lat"], depth=data["depth"])
        else:
            data.attrs["lon_bounds"], data.attrs["lat_bounds"], data.attrs["depth_bounds"] = _get_spatial_bounds(lon=data["lon"], lat=data["lat"])

        return data


# -- EN4 DataLoader -- #
class EN4Loader(DataLoader):
    """
    DataLoader to load EN.4.2.2 quality controlled objective analyses sea
    water temperature & practical salinity observations stored in the JASMIN
    Object Store.

    Parameters
    ----------
    var_name : str, default: ``temp``
        Name of variable to load from EN4 ocean observations.
        Options include ``sst``, ``sss``, ``temp`` & ``sal``.
    region : str, default: ``None``
        Region of EN4 ocean observations dataset to load.
    time_bounds : slice, str, default: None
        Time bounds to compute climatology using EN4 ocean observations.
        Default is ``None``, meaning the entire dataset is considered.
    lon_bounds : tuple, default: None
        Longitude bounds to extract from EN4 ocean observations.
        Default is ``None``, meaning the entire longitude range is loaded.
    lat_bounds : tuple, default: None
        Latitude bounds to extract from EN4 ocean observations.
        Default is ``None``, meaning the entire latitude range is loaded.
    depth_bounds : tuple, default: None
        Depth bounds to extract from EN4 ocean observations.
        Default is ``None``, meaning the entire depth range is loaded.
    freq : str, default: ``None``
        Climatology frequency of the EN4 ocean observations.
        Options include ``None``, ``total``, ``seasonal``, ``monthly``,
        ``jan``, ``feb`` etc. for individual months. Default is``None``
        meaning no climatology is computed from monthly data.
    """
    def __init__(self,
                 var_name: str = 'temp',
                 region: str | None = None,
                 time_bounds: slice | str | None = None,
                 lon_bounds: tuple | None = None,
                 lat_bounds: tuple | None = None,
                 depth_bounds: tuple | None = None,
                 freq: str | None = None,
                 ):
        # -- Verify Inputs -- #
        if var_name not in ['sst', 'sss', 'temp', 'sal']:
            raise ValueError("``var_name`` must be one of 'sst', 'sss', 'temp' or 'sal'.")
        
        # -- Define Sea Surface Variables -- #
        # Note: Nearest depth to sea surface is 5m in EN.4.2.2.
        if var_name == 'sst':
            var_name = 'temp'
            depth_bounds = (0, 0)
        elif var_name == 'sss':
            var_name = 'sal'
            depth_bounds = (0, 0)

        # -- Initialise DataLoader -- #
        super().__init__(var_name=var_name,
                         region=region,
                         source='jasmin-os',
                         time_bounds=time_bounds,
                         lon_bounds=lon_bounds,
                         lat_bounds=lat_bounds,
                         depth_bounds=depth_bounds,
                         freq=freq)

    def _load_data(self) -> xr.DataArray:
        """
        Load EN4 ocean observations data from
        the JASMIN Object Store.

        Returns
        -------
        xarray.DataArray
            Dataset storing EN4 objective analyses sea water
            temperature &practical salinity climatology.
        """
        # Define EN4 S3 url:
        url = f"{self._source}/EN4/EN4.2.2_global_monthly/"

        # Define EN4 variable names:
        var_names = {"temp": "temperature", "sal": "salinity"}

        # Load data from the JASMIN Object Store:
        source = xr.open_zarr(url, zarr_format=3, consolidated=True)
        data = source[var_names[self._var_name]]
        data.name = self._var_name
        data = _transform_longitudes(data)

        # Conversion: sea water temperature [K -> degC]:
        if self._var_name == 'temp':
            data = (data - 273.15)

        # Extract observations for specified longitude, latitude & depth bounds:
        data = _apply_geographic_bounds(data, lon_bounds=self._lon_bounds, lat_bounds=self._lat_bounds)
        if self._depth_bounds is not None:
            data = _apply_depth_bounds(data, depth_bounds=self._depth_bounds).squeeze()

        # Extract observations for specified time bounds:
        if isinstance(self._time_bounds, str):
            self._time_bounds = slice(self._time_bounds.split('-')[0], self._time_bounds.split('-')[1])
        if isinstance(self._time_bounds, slice):
            data = _apply_time_bounds(data, time_bounds=self._time_bounds)

        # Compute climatology:
        if self._freq is not None:
            data = _compute_climatology(data, freq=self._freq)

        # Inherit CF global attributes:
        global_attrs = {key: source.attrs[key] for key in ['Conventions', 'title', 'institution', 'source', 'history', 'references', 'comment'] if key in source.attrs}
        data = data.assign_attrs(global_attrs)
        # Append spatial bound attributes:
        data.attrs["lon_bounds"], data.attrs["lat_bounds"], data.attrs["depth_bounds"] = _get_spatial_bounds(lon=data["lon"], lat=data["lat"], depth=data["depth"])

        return data


# -- LOPS_MLD DataLoader -- #
class LOPSMLDLoader(DataLoader):
    """
    DataLoader to load monthly climatology of the observed oceanic Mixed Layer
    Depth (MLD) created by the LOPS laboratory (IFREMER) stored in the JASMIN
    Object Store.

    Parameters
    ----------
    var_name : str, default: ``mld``
        Name of variable to load from LOPS-MLD ocean observations.
    region : str, default: ``None``
        Region of LOPS-MLD ocean observations dataset to load.
    time_bounds : str, default: ``None``
        Time bounds to compute climatology using LOPS-MLD ocean observations.
    lon_bounds : tuple, default: None
        Longitude bounds to extract from LOPS-MLD ocean observations.
        Default is ``None``, meaning the entire longitude range is loaded.
    lat_bounds : tuple, default: None
        Latitude bounds to extract from LOPS-MLD ocean observations.
        Default is ``None``, meaning the entire latitude range is loaded.
    freq : str, default: ``None``
        Climatology frequency of the LOPS-MLD ocean observations.
        Options include ``None``, ``total``, ``seasonal``, ``monthly``,
        ``jan``, ``feb`` etc. for individual months. Default is``None``
        meaning the original monthly climatology is returned.
    """
    def __init__(self,
                 var_name: str = 'mld',
                 region: str | None = None,
                 time_bounds: str | None = None,
                 lon_bounds: tuple | None = None,
                 lat_bounds: tuple | None = None,
                 freq: str = 'total',
                 ):

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
        Load LOPS-MLD ocean observations data from
        the JASMIN Object Store.

        Returns
        -------
        xarray.DataArray
            Dataset storing LOPS-MLD ocean mixed layer depth climatology.
        """
        # Define LOPS-MLD S3 url:
        url = f"{self._source}/LOPS-MLD/LOPS-MLD_v2023_global_monthly_climatology/"

        # Define LOPS-MLD variable names:
        var_names = {"mld": "mld_dr003"}

        # Load data from the JASMIN Object Store:
        source = xr.open_zarr(url, zarr_format=3, consolidated=True) 
        data = source[var_names[self._var_name]]
        data.name = self._var_name

        # Update time coordinates to CF-compliant datetimes:
        data['time'] = xr.date_range(start='1995-01-01', end='1995-12-01', freq='MS')

        # Extract observations for specified longitude & latitude bounds:
        data = _apply_geographic_bounds(data, lon_bounds=self._lon_bounds, lat_bounds=self._lat_bounds)

        # Select climatology:
        if self._freq is not None:
            data = _compute_climatology(data, freq=self._freq)

        # Inherit CF global attributes:
        global_attrs = {key: source.attrs[key] for key in ['Conventions', 'title', 'institution', 'source', 'history', 'references', 'comment'] if key in source.attrs}
        data = data.assign_attrs(global_attrs)
        # Append spatial bound attributes:
        data.attrs["lon_bounds"], data.attrs["lat_bounds"], data.attrs["depth_bounds"] = _get_spatial_bounds(lon=data["lon"], lat=data["lat"])

        return data