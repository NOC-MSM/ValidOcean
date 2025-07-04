"""
validator.py

Description: Defines the ModelValidator Class including methods
used to validate ocean general circulation model outputs
using ocean observations.

Author: Ollie Tooth (oliver.tooth@noc.ac.uk)
"""

# -- Import required packages -- #
import numpy as np
import xarray as xr
import cmocean.cm as cmo
import cartopy.crs as ccrs

from typing import Self

# -- Import utility functions -- #
import ValidOcean.dataloader as dataloader
from ValidOcean.dataloader import DataLoader
from ValidOcean.aggregator import _aggregate_to_1D
from ValidOcean.processing import _get_spatial_bounds, _apply_geographic_bounds, _apply_time_bounds, _compute_climatology
from ValidOcean.statistics import _compute_agg_stats
from ValidOcean.regridding import _regrid_data
from ValidOcean.plotting import _plot_timeseries, _plot_2D_error

# -- Define ModelValidator Class -- #
class ModelValidator():
    """
    Create a ModelValidator object to validate ocean general
    circulation model outputs using ocean observations.

    Parameters
    ----------
    mdl_data : xarray.Dataset, default: None
        xarray Dataset containing ocean model output data. 
        Coordinates of model grid cell centres must be specified
        with names: (``lon``, ``lat``). Mask variable must be specified
        for regridding. For conservative regridding, the coordinates
        bounding each model grid cell must be specified with names:
        (``lon_b``, ``lat_b``).
    DataLoader : DataLoader, default: None
        Specify a custom DataLoader to load ocean observations from
        cloud object storage or local file system. Default is None.

    Attributes
    ----------
    data : xarray.Dataset
        xarray Dataset containing ocean model output data.
    results : xarray.Dataset
        xarray Dataset containing model validation data.
    obs : xarray.Dataset
        xarray Dataset containing ocean observations data.
    stats : xarray.Dataset
        xarray Dataset containing model validation statistics.
    """
    def __init__(self, mdl_data : xr.Dataset | None = None, dataloader : DataLoader | None = None):
        # -- Verify Inputs -- #
        if mdl_data is not None:
            if not isinstance(mdl_data, xr.Dataset):
                raise TypeError("``mdl_data`` must be specified as an xarray Dataset.")
        if dataloader is not None:
            if not isinstance(dataloader, DataLoader):
                raise TypeError("Custom ``dataloader`` must be a DataLoader object.")

        # -- Verify Domain Attributes -- #
        if mdl_data is not None:
            if 'time' not in mdl_data.dims:
                raise ValueError("dimension ``time`` must be included in model dataset.")
            if 'mask' not in mdl_data.variables:
                raise ValueError("``mask`` variable must be included in model dataset.")
            if 'lon' not in mdl_data.variables:
                raise ValueError("``lon`` coordinates of model grid cell centres must be included in model dataset.")
            if 'lat' not in mdl_data.variables:
                raise ValueError("``lat`` coordinates of model grid cell centres must be included in model dataset.")

        # -- Class Attributes -- #
        if mdl_data is not None:
            self._data = mdl_data
            self._lon = self._data['lon'].squeeze()
            self._lat = self._data['lat'].squeeze()
            if 'depth' in self._data.dims:
                self._depth = self._data['depth'].squeeze()
            else:
                self._depth = None
            # Get model domain bounds rounded to nearest largest integer:
            self._lon_bounds, self._lat_bounds, self._depth_bounds = _get_spatial_bounds(lon=self._lon, lat=self._lat, depth=self._depth)
        else:
            self._data = xr.Dataset()

        self._dataloader = dataloader
        self._obs = xr.Dataset()
        self._results = xr.Dataset()
        self._stats = xr.Dataset()

    # -- Class Properties -- #
    @property
    def data(self) -> xr.Dataset:
        return self._data
    @data.setter
    def data(self, value: xr.Dataset) -> None:
        self._data = value

    @property
    def obs(self) -> xr.Dataset:
        return self._obs
    @obs.setter
    def obs(self, value: xr.Dataset) -> None:
        self._obs = value

    @property
    def results(self) -> xr.Dataset:
        return self._results
    @results.setter
    def results(self, value: xr.Dataset) -> None:
        self._results = value

    @property
    def stats(self) -> xr.Dataset:
        return self._stats
    @stats.setter
    def stats(self, value: xr.Dataset) -> None:
        self._stats = value

    # -- Magic Methods -- #
    def __repr__(self) -> str:
        return (f"\n<ModelValidator>\n\n"
                f"-- Model Data --\n\n{self._data}\n\n"
                f"-- Observations --\n\n{self._obs}\n\n"
                f"-- Results --\n\n{self._results}\n\n"
                f"-- Stats --\n\n{self._stats}"
                )

    # -- Private Methods -- #

    def _load_obs_data(self,
                       obs_name : str,
                       var_name : str,
                       region : str | None = None,
                       time_bounds : slice | str | None = None,
                       lon_bounds : tuple | None = None,
                       lat_bounds : tuple | None = None,
                       freq : str | None = None,
                       ) -> xr.DataArray:
        """
        Load ocean observations dataset.

        Parameters
        ----------
        obs_name : str
            Name of observational dataset to load.
        var_name : str
            Name of variable to load from observational dataset.
        region : str, default: ``None``
            Region of ocean observations dataset to load. Default is ``None``
            meaning the entire dataset is loaded.
        time_bounds : slice, str, default: None
            Time bounds to compute climatology using ocean observations.
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
            Climatology frequency of the observational dataset.
            Options include ``total``, ``seasonal``, ``monthly``,
            ``jan``, ``feb``, `mar`` etc. Default is ``None``
            meaning the entire time series is loaded.
        
        Returns
        -------
        xarray DataArray
            xarray DataArray containing ocean observations.
        """
        # -- Verify Inputs -- #
        if not isinstance(obs_name, str):
            raise TypeError("``obs_name`` must be specified as a string.")
        
        # -- Load Ocean Observations Dataset -- #
        if self._dataloader is None:
            if hasattr(dataloader, f"{obs_name}Loader"):
                ObsDataLoader = getattr(dataloader, f"{obs_name}Loader")
                obs_data = ObsDataLoader(var_name=var_name,
                                         region=region,
                                         time_bounds=time_bounds,
                                         lon_bounds=lon_bounds,
                                         lat_bounds=lat_bounds,
                                         freq=freq
                                         )._load_data()
            else:
                raise ValueError(f"undefined DataLoader specified: {obs_name}Loader.")
        else:
            obs_data = self._dataloader(var_name=var_name,
                                        region=region,
                                        time_bounds=time_bounds,
                                        lon_bounds=lon_bounds,
                                        lat_bounds=lat_bounds,
                                        freq=freq
                                        )._load_data()

        return obs_data


    def _update_obs(self,
                    da: xr.DataArray,
                    obs_name: str
                    ) -> Self:
        """
        Update ocean observations attribute with a new DataArray.
        
        Parameters:
        -----------
        da: xr.DataArray
            Ocean observations DataArray to add to the Dataset.
        obs_name: str
            Name of ocean observations.

        Returns:
        --------
        ModelValidator
            ModelValidator with updated obs attribute.
        """
        # -- Verify Inputs -- #
        if not isinstance(da, xr.DataArray):
            raise TypeError("``da`` must be specified as an xarray DataArray.")
        if not isinstance(obs_name, str):
            raise TypeError("``obs_name`` must be specified as a string.")

        # -- Update Ocean Observations -- #
        # Update coord names:
        coords = [crd for crd in da.coords]
        new_coords = {key: value for key, value in zip(coords, [f"{crd}_{obs_name}" for crd in coords])}
        da = da.rename(new_coords)

        # Remove any existing vars & coords:
        var_name = f"{da.name}_{obs_name}"
        if var_name in self._obs.data_vars:
            names = [crd for crd in self._obs[var_name].coords]
            names.append(var_name)
            self._obs = self._obs.drop_vars(names=names)

        self._obs[var_name] = da

        # Remove redundant coords:
        coord_names = [self._obs[var].coords for var in self._obs.data_vars]
        coord_names = set([item for sublist in coord_names for item in sublist]) ^ set(self._obs.coords)
        self._obs = self._obs.drop_vars(names=coord_names)

        return self


    def _update_results(self,
                        da: xr.DataArray,
                        obs_name: str | None = None
                        ) -> Self:
        """
        Update model validation results attribute with a new DataArray.
        
        Parameters:
        -----------
        da: xr.DataArray
            Ocean model DataArray to add to the Dataset.
        obs_name: str | None, default=None
            Name of ocean observations if DataArray is defined
            on obs. grid. Default is ``None`` meaning DataArray
            is defined on ocean model grid.

        Returns:
        --------
            ModelValidator
                ModelValidator with updated results attribute.
        """
        # -- Verify Inputs -- #
        if not isinstance(da, xr.DataArray):
            raise TypeError("``da`` must be specified as an xarray DataArray.")
        if obs_name is not None:
            if not isinstance(obs_name, str):
                raise TypeError("``obs_name`` must be specified as a string.")

        # -- Update Model Validation Results -- #
        # Update coords if regridded to obs:
        if obs_name is not None:
            coords = [crd for crd in da.coords if crd != 'obs']
            new_coords = {key: value for key, value in zip(coords, [f"{crd}_{obs_name}" for crd in coords])}
            da = da.rename(new_coords)

        # Remove existing DataArray & coords from this source:
        var_name = da.name
        if var_name in self._results.data_vars:
            names = [crd for crd in self._results[var_name].coords]
            names.append(var_name)
            self._results = self._results.drop_vars(names=names)
            dims = [dim for dim in da.dims if dim in self._results.dims]
            self._results = self._results.drop_dims(drop_dims=dims)

        self._results[var_name] = da

        # Remove redundant coords:
        coord_names = [self._results[var].coords for var in self._results.data_vars]
        coord_names = set([item for sublist in coord_names for item in sublist]) ^ set(self._results.coords)
        self._results = self._results.drop_vars(names=coord_names)

        return self


    def _compute_2D_error(self,
                          var_name : str = 'tos_con',
                          obs : dict = dict(name='OISSTv2', var='sst'),
                          time_bounds : slice | str | None = None,
                          freq : str = 'total',
                          regrid_to : str = 'model',
                          method : str = 'bilinear',
                          stats : bool = False,
                          ) -> Self:
        """
        Compute error between ocean model output
        and observations for 2-dimensional variable.

        Parameters
        ----------
        var_name : str, default: ``tos_con``
            Name of variable in ocean model dataset.
        obs : dict, default: {``name``:``OISSTv2``, ``var``: ``sst``}
            Dictionary defining the ``name`` of the observational
            dataset and variable ``var`` to calculate model error.
        time_bounds : slice, str, default: ``None``
            Time bounds to compute climatology using ocean observations.
            Default is ``None`` meaning the entire time series is loaded.
            Custom bounds should be specified using a slice object. Available
            pre-defined climatologies can be selected using a string
            (e.g., "1991-2020").
        freq : str, default: ``total``
            Climatology frequency to calculate model error. 
            Options include ``total``, ``seasonal``, ``monthly``,
            ``jan``, ``feb``, `mar`` etc. for individual months.
        regrid_to : str, default: ``model``
            Regrid data to match either ``model`` or ``obs`` grid.
        method : str, default: ``bilinear``
            Method used to interpolate model and observed variables onto target grid.
            Options include ``bilinear``, ``nearest``, ``conservative``.
        stats : bool, default: ``False``
            Return aggregated statistics of (model - observation) error.
            Includes Mean Absolute Error, Mean Square Error & Root Mean Square Error.

        Returns
        -------
        ModelValidator
            ModelValidator object including (ocean model - observation) error, stored in the
            ``results`` attribute and aggregate statistics stored in the ``stats`` attribute.
        """
        # -- Verify Inputs -- #
        if not isinstance(var_name, str):
            raise TypeError("``var_name`` name must be specified as a string.")
        if var_name not in self._data.variables:
            raise ValueError(f"{var_name} not found in ocean model dataset.")
        
        if not isinstance(obs, dict):
            raise TypeError("``obs`` must be specified as a dictionary.")
        for key in ['name', 'region', 'var']:
            if key not in obs.keys():
                raise ValueError(f"``obs`` dictionary must contain key ``{key}``.")

        if not isinstance(regrid_to, str):
            raise TypeError("``regrid`` must be specified as a string.")
        if regrid_to not in ['model', 'obs']:
            raise ValueError("``regrid`` must be one of ``model`` or ``obs``.")

        if not isinstance(stats, bool):
            raise TypeError("``stats`` must be specified as a boolean.")

        # -- Load Observational Data -- #
        obs_data = self._load_obs_data(obs_name=obs['name'],
                                       var_name=obs['var'],
                                       region=obs['region'],
                                       time_bounds=time_bounds,
                                       lon_bounds=self._lon_bounds,
                                       lat_bounds=self._lat_bounds,
                                       freq=freq)

        # -- Process Ocean Model Data -- #
        if obs['region'] is not None:
            mdl_data = _apply_geographic_bounds(data=self._data[var_name], 
                                                lon_bounds=obs_data.attrs['lon_bounds'],
                                                lat_bounds=obs_data.attrs['lat_bounds'],
                                                is_obs=False)
        else:
            mdl_data = self._data[var_name]

        if time_bounds is not None:
            if isinstance(time_bounds, str):
                time_bounds = slice(time_bounds.split('-')[0], time_bounds.split('-')[1])
            mdl_data = _apply_time_bounds(data=mdl_data, time_bounds=time_bounds, is_obs=False)

        mdl_data = _compute_climatology(data=mdl_data, freq=freq)

        # -- Regrid Data -- #
        if regrid_to == 'model':
            obs_data = _regrid_data(source_grid=obs_data, target_grid=mdl_data, method=method)
            obs_data.name = var_name
        elif regrid_to == 'obs':
            mdl_data = _regrid_data(source_grid=mdl_data, target_grid=obs_data, method=method)
            mdl_data.name = var_name
        
        # -- Compute & Store Error -- #
        mdl_error = (mdl_data - obs_data).expand_dims(dim={'obs': np.array([obs['name']])}, axis=0)
        mdl_error.name = f"{var_name}_error"

        if regrid_to == 'model':
            self._update_results(da=mdl_error, obs_name=None)
            self._update_results(da=mdl_data, obs_name=None)
        elif regrid_to == 'obs':
            self._update_results(da=mdl_error, obs_name=obs['name'].lower())
            self._update_results(da=mdl_data, obs_name=obs['name'].lower())

        # -- Store Ocean Observations Data -- #
        self._update_obs(da=obs_data, obs_name=obs['name'].lower())

        # -- Compute Aggregate Statistics -- #
        if stats:
            self.stats = _compute_agg_stats(mdl_data=mdl_data, obs_data=obs_data)

        return self


    def _compute_1D_diagnostic(self,
                               var_name : str = 'areacello',
                               mask : xr.DataArray | None = None,
                               aggregator : str = 'sum',
                               out_name : str = 'siarea',
                               obs : dict = dict(name='NSIDC', var='siarea', region='arctic'),
                               time_bounds : slice | str | None = None,
                               stats : bool = False,
                               ) -> Self:
        """
        Compute 1-dimensional diagnostic from ocean model
        output and observations.

        Parameters
        ----------
        var_name : str, default: ``areacello``
            Name of variable in ocean model dataset.
        mask : xr.DataArray, default: None
            Mask variable to apply to ocean model variable.
        aggregator : str, default: ``sum``
            Aggregation function to apply to ocean model variable.
            Options include ``sum``, ``mean`` & ``std``
        out_name : str, default: ``siarea``
            Name of 1-dimensional diagnostic.
        obs : dict, default: {``name``:``NSIDC``, ``var``:``siarea``, ``region``:``arctic``}
            Dictionary defining the ``name`` of the observational
            dataset and the diagnostic variable ``var``.
        time_bounds : slice, str, default: ``None``
            Time bounds to compute sea ice area.
            Default is ``None`` meaning the entire time series is returned.
            Custom bounds should be specified using a slice object.
        stats : bool, default: ``False``
            Return aggregated statistics of 1-dimensional ocean model &
            observation diagnostic. Includes Mean Absolute Error, Mean Square Error,
            Root Mean Square Error & Pearson Correlation Coefficient.

        Returns
        -------
        ModelValidator
            ModelValidator object including 1-dimensional ocean model & observation statistics,
            stored in the ``results`` and ``obs`` attributes and aggregate statistics stored in
            the ``stats`` attribute.
        """
        # -- Verify Inputs -- #
        if not isinstance(var_name, str):
            raise TypeError("``var_name`` must be specified as a string.")
        if var_name not in self._data.variables:
            raise ValueError(f"{var_name} not found in ocean model dataset.")
        if mask is not None:
            if not isinstance(mask, xr.DataArray):
                raise TypeError("``mask`` must be specified as an xarray DataArray.")
    
        if not isinstance(aggregator, str):
            raise TypeError("``aggregator`` must be specified as a string.")
        if aggregator not in ['sum', 'mean', 'std']:
            raise ValueError("``aggregator`` must be one of ``sum``, ``mean`` or ``std``.")
        if not isinstance(out_name, str):
            raise TypeError("``out_name`` must be specified as a string.")
        
        if not isinstance(obs, dict):
            raise TypeError("``obs`` must be specified as a dictionary.")
        for key in ['name', 'region', 'var']:
            if key not in obs.keys():
                raise ValueError(f"``obs`` dictionary must contain key ``{key}``.")

        if not isinstance(stats, bool):
            raise TypeError("``stats`` must be specified as a boolean.")

        # -- Load Observational Data -- #
        obs_data = self._load_obs_data(obs_name=obs['name'],
                                       var_name=obs['var'],
                                       region=obs['region'],
                                       time_bounds=time_bounds,
                                       lon_bounds=self._lon_bounds,
                                       lat_bounds=self._lat_bounds,
                                       freq=None)

        # -- Process Ocean Model Data -- #
        if obs['region'] is not None:
            mdl_data = _apply_geographic_bounds(data=self._data[var_name], 
                                             lon_bounds=obs_data.attrs['lon_bounds'],
                                             lat_bounds=obs_data.attrs['lat_bounds'],
                                             is_obs=False)
            if mask is not None:
                mask = _apply_geographic_bounds(data=mask, 
                                             lon_bounds=obs_data.attrs['lon_bounds'],
                                             lat_bounds=obs_data.attrs['lat_bounds'],
                                             is_obs=False)
        else:
            mdl_data = self._data[var_name]

        if time_bounds is not None:
            if isinstance(time_bounds, str):
                time_bounds = slice(time_bounds.split('-')[0], time_bounds.split('-')[1])

            if 'time' in mdl_data.dims:
                mdl_data = _apply_time_bounds(data=mdl_data, time_bounds=time_bounds, is_obs=False)
            if (mask is not None) & ('time' in mask.dims):
                mask = _apply_time_bounds(data=mask, time_bounds=time_bounds, is_obs=False)

        mdl_data = _aggregate_to_1D(data=mdl_data, mask=mask, aggregator=aggregator)

        # -- Store Model & Observational Statistics -- #
        mdl_data = mdl_data.expand_dims(dim={'obs': np.array([obs['name']])}, axis=0)
        mdl_data.name = out_name
        self._update_results(da=mdl_data, obs_name=None)

        self._update_obs(da=obs_data, obs_name=obs['name'].lower())

        # -- Compute Aggregate Statistics -- #
        if stats:
            self.stats = _compute_agg_stats(mdl_data=mdl_data, obs_data=obs_data)

        return self


    # -- Public Methods -- #
    def compute_sst_error(self,
                          sst_name : str = 'tos_con',
                          obs_name : str = 'OISSTv2',
                          time_bounds : slice | str | None = None,
                          freq : str = 'total',
                          regrid_to : str = 'model',
                          method : str = 'bilinear',
                          stats : bool = False,
                          ) -> Self:
        """
        Compute sea surface temperature error between ocean model and observations (model - observation).

        Parameters
        ----------
        sst_name : str, default: ``tos_con``
            Name of sea surface temperature variable in ocean model dataset.
        obs_name : str, default: ``OISSTv2``
            Name of observational dataset.
            Options include ``ARMOR3D``, ``HadISST``, ``OISSTv2`` and ``WOA23``.
        time_bounds : slice, str, default: ``None``
            Time bounds to compute sea surface temperature climatologies.
            Default is ``None`` meaning the entire time series is used.
            Custom bounds should be specified using a slice object. Available
            pre-defined climatologies can be selected using a string
            (e.g., "1991-2020").
        freq : str, default: ``total``
            Climatology frequency to compute sea surface temperature error. 
            Options include ``total``, ``seasonal``, ``monthly``, ``jan``,
            ``feb``, `mar`` etc. for individual months.
        regrid_to : str, default: ``model``
            Regrid data to either ``model`` or observations (``obs``) target grid.
        method : str, default: ``bilinear``
            Method used to interpolate model and observed data onto target grid.
            Options include ``bilinear``, ``nearest``, ``conservative``.
        stats : bool, default: ``False``
            Return aggregated statistics of (model - observation) error.
            Includes Mean Absolute Error, Mean Square Error & Root Mean Square Error.

        Returns
        -------
        ModelValidator
            ModelValidator object including (ocean model - observation) sea surface temperature
            error, stored in the ``results`` attribute and aggregate statistics stored in the
            ``stats`` attribute.
        """
        # -- Compute SST Error -- #
        self._compute_2D_error(var_name=sst_name,
                               obs=dict(name=obs_name, region=None, var='sst'),
                               time_bounds=time_bounds,
                               freq=freq,
                               regrid_to=regrid_to,
                               method=method,
                               stats=stats,
                               )

        return self


    def plot_sst_error(self,
                       sst_name : str = 'tos_con',
                       obs_name : str = 'OISSTv2',
                       time_bounds : slice | str | None = None,
                       freq : str = 'total',
                       regrid_to : str = 'model',
                       method : str = 'bilinear',
                       stats : bool = False,
                       figsize : tuple = (15, 8),
                       error_kwargs : dict = dict(cmap=cmo.balance, vmin=-2.5, vmax=2.5),
                       source_plots : bool = True,
                       source_kwargs : dict = dict(cmap=cmo.thermal, vmin=-2, vmax=30),
                       ) -> None:
        """
        Plot sea surface temperature error between ocean model and observations (model - observation).

        Parameters
        ----------
        sst_name : str, default: ``tos_con``
            Name of sea surface temperature variable in model dataset.
        obs_name : str, default: ``OISSTv2``
            Name of observational dataset.
            Options include ``ARMOR3D``, ``HadISST``, ``OISSTv2`` and ``WOA23``.
        time_bounds : slice, str, default: ``None``
            Time bounds to compute sea surface temperature climatologies. Default is ``None``
            meaning the entire time series is used. Custom bounds should be specified using
            a slice object. Available pre-defined climatologies can be selected using a
            string (e.g., "1991-2020").
        freq : str, default: ``total``
            Climatology frequency to compute sea surface temperature error.
            Options include ``total``, ``seasonal``, ``monthly``, ``jan``,
            ``feb``, `mar`` etc. for individual months.
        regrid_to : str, default: ``model``
            Regrid data to either ``model`` or observations (``obs``) target grid.
        method : str, default: ``bilinear``
            Method used to interpolate model and observed data onto target grid.
            Options include ``bilinear``, ``nearest``, ``conservative``.
        stats : bool, default: ``False``
            Return aggregated statistics of (model - observation) error.
            Includes Mean Absolute Error, Mean Square Error & Root Mean Square Error.
        figsize : tuple, default: (15, 8)
            Figure size for the plot.
        error_kwargs : dict, default: ``{'cmap':'RdBu_r', 'vmin':-2.5, 'vmax':2.5}``
            Keyword arguments for matplotlib pcolormesh. Only applied to (model - observation)
            error.
        source_plots : bool, default: ``True``
            Plot model, observations and (model - observation) error as separate subplots.
            This option is only available where climatology frequency ``freq``='total' or
            ``freq`` is a individual month (e.g., ``jan``).
        source_kwargs : dict, default: ``{'cmap':'cmo.thermal', 'vmin':-2, 'vmax':30}``
            Keyword arguments for model and observation matplotlib pcolormeshes.

        Returns
        -------
        matplotlib Axes
            Matplotlib axes object displaying (model - observation) sea surface temperature error.
        """
        # -- Compute SST Error -- #
        self._compute_2D_error(var_name=sst_name,
                               obs=dict(name=obs_name, region=None, var='sst'),
                               time_bounds=time_bounds,
                               freq=freq,
                               regrid_to=regrid_to,
                               method=method,
                               stats=stats,
                               )

        # -- Plot SST Error -- #
        # Use global projection:
        projection = ccrs.Robinson(central_longitude=-1)

        ax = _plot_2D_error(mv=self,
                            obs_name=obs_name,
                            var_name=sst_name,
                            projection=projection,
                            figsize=figsize,
                            error_kwargs=error_kwargs,
                            source_plots=source_plots,
                            source_kwargs=source_kwargs,
                            )
        return ax


    def compute_sss_error(self,
                          sss_name : str = 'sos_abs',
                          obs_name : str = 'ARMOR3D',
                          time_bounds : slice | str | None = None,
                          freq : str = 'total',
                          regrid_to : str = 'model',
                          method : str = 'bilinear',
                          stats : bool = False,
                          ) -> Self:
        """
        Compute sea surface salinity error between ocean model and observations (model - observation).

        Parameters
        ----------
        sss_name : str, default: ``sos_abs``
            Name of sea surface salinity variable in ocean model dataset.
        obs_name : str, default: ``ARMOR3D``
            Name of observational dataset.
            Options include ``ARMOR3D``, ``EN4.2.2`` and ``WOA23``.
        time_bounds : slice, str, default: ``None``
            Time bounds to compute sea surface salinity climatologies.
            Default is ``None`` meaning the entire time series is used.
            Custom bounds should be specified using a slice object. Available
            pre-defined climatologies can be selected using a string
            (e.g., "1991-2020").
        freq : str, default: ``total``
            Climatology frequency to compute sea surface salinity error. 
            Options include ``total``, ``seasonal``, ``monthly``, ``jan``,
            ``feb``, ``mar`` etc. for individual months.
        regrid_to : str, default: ``model``
            Regrid data to either ``model`` or observations (``obs``) target grid.
        method : str, default: ``bilinear``
            Method used to interpolate model and observed data onto target grid.
            Options include ``bilinear``, ``nearest``, ``conservative``.
        stats : bool, default: ``False``
            Return aggregated statistics of (model - observation) error.
            Includes Mean Absolute Error, Mean Square Error & Root Mean Square Error.

        Returns
        -------
        ModelValidator
            ModelValidator object including (ocean model - observation) sea surface salinity
            error, stored in the ``results`` attribute and aggregate statistics stored in the
            ``stats`` attribute.
        """
        # -- Compute SSS Error -- #
        self._compute_2D_error(var_name=sss_name,
                               obs=dict(name=obs_name, region=None, var='sss'),
                               time_bounds=time_bounds,
                               freq=freq,
                               regrid_to=regrid_to,
                               method=method,
                               stats=stats,
                               )

        return self


    def plot_sss_error(self,
                       sss_name : str = 'sos_abs',
                       obs_name : str = 'ARMOR3D',
                       time_bounds : slice | str | None = None,
                       freq : str = 'total',
                       regrid_to : str = 'model',
                       method : str = 'bilinear',
                       stats : bool = False,
                       figsize : tuple = (15, 8),
                       error_kwargs : dict = dict(cmap=cmo.balance, vmin=-2.5, vmax=2.5),
                       source_plots : bool = True,
                       source_kwargs : dict = dict(cmap=cmo.haline, vmin=33, vmax=38),
                       ) -> None:
        """
        Plot sea surface salinity error between ocean model and observations (model - observation).

        Parameters
        ----------
        sss_name : str, default: ``sos_abs``
            Name of sea surface salinity variable in model dataset.
        obs_name : str, default: ``ARMOR3D``
            Name of observational dataset.
            Options include ``ARMOR3D``, ``EN4.2.2`` and ``WOA23``.
        time_bounds : slice, str, default: ``None``
            Time bounds to compute sea surface salinity climatologies. Default is ``None``
            meaning the entire time series is used. Custom bounds should be specified using
            a slice object. Available pre-defined climatologies can be selected using a
            string (e.g., "1991-2020").
        freq : str, default: ``total``
            Climatology frequency to compute sea surface temperature error.
            Options include ``total``, ``seasonal``, ``monthly``, ``jan``,
            ``feb``, `mar`` etc. for individual months.
        regrid_to : str, default: ``model``
            Regrid data to either ``model`` or observations (``obs``) target grid.
        method : str, default: ``bilinear``
            Method used to interpolate model and observed data onto target grid.
            Options include ``bilinear``, ``nearest``, ``conservative``.
        stats : bool, default: ``False``
            Return aggregated statistics of (model - observation) error.
            Includes Mean Absolute Error, Mean Square Error & Root Mean Square Error.
        figsize : tuple, default: (15, 8)
            Figure size for the plot.
        error_kwargs : dict, default: ``{'cmap':'RdBu_r', 'vmin':-1, 'vmax':1}``
            Keyword arguments for matplotlib pcolormesh. Only applied to (model - observation)
            error.
        source_plots : bool, default: ``True``
            Plot model, observations and (model - observation) error as separate subplots.
            This option is only available where climatology frequency ``freq``='total' or
            ``freq`` is a individual month (e.g., ``jan``).
        source_kwargs : dict, default: ``{'cmap':'cmo.thermal', 'vmin':10, 'vmax':36}``
            Keyword arguments for model and observation matplotlib pcolormeshes.

        Returns
        -------
        matplotlib Axes
            Matplotlib axes object displaying (model - observation) sea surface temperature error.
        """
        # -- Compute SSS Error -- #
        self._compute_2D_error(var_name=sss_name,
                               obs=dict(name=obs_name, region=None, var='sss'),
                               time_bounds=time_bounds,
                               freq=freq,
                               regrid_to=regrid_to,
                               method=method,
                               stats=stats,
                               )

        # -- Plot SSS Error -- #
        # Use global projection:
        projection = ccrs.Robinson(central_longitude=-1)

        ax = _plot_2D_error(mv=self,
                            obs_name=obs_name,
                            var_name=sss_name,
                            projection=projection,
                            figsize=figsize,
                            error_kwargs=error_kwargs,
                            source_plots=source_plots,
                            source_kwargs=source_kwargs,
                            )
        return ax


    def compute_mld_error(self,
                          mld_name : str = 'mld',
                          obs_name : str = 'ARMOR3D',
                          time_bounds : slice | str | None = None,
                          freq : str = 'total',
                          regrid_to : str = 'model',
                          method : str = 'bilinear',
                          stats : bool = False,
                          ) -> Self:
        """
        Compute mixed layer depth error between ocean model and observations (model - observation).

        Parameters
        ----------
        mld_name : str, default: ``mld``
            Name of sea surface salinity variable in ocean model dataset.
        obs_name : str, default: ``ARMOR3D``
            Name of observational dataset.
            Options include ``ARMOR3D`` and ``LOPSMLD``.
        time_bounds : slice, str, default: ``None``
            Time bounds to compute mixed layer depth climatology.
            Default is ``None`` meaning the entire time series is used.
            Custom bounds should be specified using a slice object. Available
            pre-defined climatologies can be selected using a string
            (e.g., "1991-2020").
        freq : str, default: ``total``
            Climatology frequency to compute mixed layer depth error. 
            Options include ``total``, ``seasonal``, ``monthly``, ``jan``,
            ``feb``, `mar`` etc. for individual months.
        regrid_to : str, default: ``model``
            Regrid data to either ``model`` or observations (``obs``) target grid.
        method : str, default: ``bilinear``
            Method used to interpolate model and observed data onto target grid.
            Options include ``bilinear``, ``nearest``, ``conservative``.
        stats : bool, default: ``False``
            Return aggregated statistics of (model - observation) error.
            Includes Mean Absolute Error, Mean Square Error & Root Mean Square Error.

        Returns
        -------
        ModelValidator
            ModelValidator object including (ocean model - observation) mixed layer depth
            error, stored in the ``results`` attribute and aggregate statistics stored in the
            ``stats`` attribute.
        """
        # -- Compute MLD Error -- #
        self._compute_2D_error(var_name=mld_name,
                               obs=dict(name=obs_name, region=None, var='mld'),
                               time_bounds=time_bounds,
                               freq=freq,
                               regrid_to=regrid_to,
                               method=method,
                               stats=stats,
                               )

        return self


    def plot_mld_error(self,
                       mld_name : str = 'mld',
                       obs_name : str = 'ARMOR3D',
                       time_bounds : slice | str | None = None,
                       freq : str = 'total',
                       regrid_to : str = 'model',
                       method : str = 'bilinear',
                       stats : bool = False,
                       figsize : tuple = (15, 8),
                       error_kwargs : dict = dict(cmap=cmo.balance, vmin=-250, vmax=250),
                       source_plots : bool = True,
                       source_kwargs : dict = dict(cmap=cmo.haline, vmin=0, vmax=500),
                       ) -> None:
        """
        Plot mixed layer depth error between ocean model and observations (model - observation).

        Parameters
        ----------
        mld_name : str, default: ``mld``
            Name of sea surface salinity variable in model dataset.
        obs_name : str, default: ``ARMOR3D``
            Name of observational dataset.
            Options include ``ARMOR3D`` and ``LOPSMLD``.
        time_bounds : slice, str, default: ``None``
            Time bounds to compute mixed layer depth climatology. Default is ``None``
            meaning the entire time series is used. Custom bounds should be specified using
            a slice object. Available pre-defined climatologies can be selected using a
            string (e.g., "1991-2020").
        freq : str, default: ``total``
            Climatology frequency to compute mixed layer depth error.
            Options include ``total``, ``seasonal``, ``monthly``, ``jan``,
            ``feb``, `mar`` etc. for individual months.
        regrid_to : str, default: ``model``
            Regrid data to either ``model`` or observations (``obs``) target grid.
        method : str, default: ``bilinear``
            Method used to interpolate model and observed data onto target grid.
            Options include ``bilinear``, ``nearest``, ``conservative``.
        stats : bool, default: ``False``
            Return aggregated statistics of (model - observation) error.
            Includes Mean Absolute Error, Mean Square Error & Root Mean Square Error.
        figsize : tuple, default: (15, 8)
            Figure size for the plot.
        error_kwargs : dict, default: ``{'cmap':'RdBu_r', 'vmin':-250, 'vmax':250}``
            Keyword arguments for matplotlib pcolormesh. Only applied to (model - observation)
            error.
        source_plots : bool, default: ``True``
            Plot model, observations and (model - observation) error as separate subplots.
            This option is only available where climatology frequency ``freq``='total' or
            ``freq`` is a individual month (e.g., ``jan``).
        source_kwargs : dict, default: ``{'cmap':'cmo.thermal', 'vmin':0, 'vmax':1200}``
            Keyword arguments for model and observation matplotlib pcolormeshes.

        Returns
        -------
        matplotlib Axes
            Matplotlib axes object displaying (model - observation) mixed layer depth error.
        """
        # -- Compute MLD Error -- #
        self._compute_2D_error(var_name=mld_name,
                               obs=dict(name=obs_name, region=None, var='mld'),
                               time_bounds=time_bounds,
                               freq=freq,
                               regrid_to=regrid_to,
                               method=method,
                               stats=stats,
                               )

        # -- Plot MLD Error -- #
        # Use global projection:
        projection = ccrs.Robinson(central_longitude=-1)

        ax = _plot_2D_error(mv=self,
                            obs_name=obs_name,
                            var_name=mld_name,
                            projection=projection,
                            figsize=figsize,
                            error_kwargs=error_kwargs,
                            source_plots=source_plots,
                            source_kwargs=source_kwargs,
                            )
        return ax


    def compute_siconc_error(self,
                             sic_name : str = 'siconc',
                             obs_name : str = 'NSIDC',
                             region : str = 'arctic',
                             time_bounds : slice | str | None = None,
                             freq : str = 'mar',
                             regrid_to : str = 'model',
                             method : str = 'bilinear',
                             stats : bool = False,
                             ) -> Self:
        """
        Compute sea ice concentration error between ocean model and observations (model - observation).

        Parameters
        ----------
        sic_name : str, default: ``siconc``
            Name of sea ice concentration variable in ocean model dataset.
        obs_name : str, default: ``NSIDC``
            Name of observational dataset.
            Options include ``NSIDC``, ``OISSTv2`` and ``HadISST``.
        region : str, default: ``arctic``
            Polar region of ocean observations dataset to calculate sea ice
            concentration error. Options are ``arctic`` or ``antarctic``.
        time_bounds : slice, str, default: ``None``
            Time bounds to compute sea ice concentration climatologies.
            Default is ``None`` meaning the entire time series is used.
            Custom bounds should be specified using a slice object. Available
            pre-defined climatologies can be selected using a string
            (e.g., "1991-2020").
        freq : str, default: ``mar``
            Climatology frequency to compute sea ice concentration error. 
            Options include ``total``, ``seasonal``, ``monthly``, ``jan``,
            ``feb``, `mar`` etc. for individual months.
        regrid_to : str, default: ``model``
            Regrid data to either ``model`` or observations (``obs``) target grid.
        method : str, default: ``bilinear``
            Method used to interpolate model and observed data onto target grid.
            Options include ``bilinear``, ``nearest``, ``conservative``.
        stats : bool, default: ``False``
            Return aggregated statistics of (model - observation) error.
            Includes Mean Absolute Error, Mean Square Error & Root Mean Square Error.

        Returns
        -------
        ModelValidator
            ModelValidator object including (ocean model - observation) sea ice concentration
            error, stored in the ``results`` attribute and aggregate statistics stored in the
            ``stats`` attribute.
        """
        # -- Compute SIC Error -- #
        self._compute_2D_error(var_name=sic_name,
                               obs=dict(name=obs_name, region=region, var='siconc'),
                               time_bounds=time_bounds,
                               freq=freq,
                               regrid_to=regrid_to,
                               method=method,
                               stats=stats,
                               )

        return self


    def plot_siconc_error(self,
                          sic_name : str = 'siconc',
                          obs_name : str = 'NSIDC',
                          region : str = 'arctic',
                          time_bounds : slice | str | None = None,
                          freq : str = 'mar',
                          regrid_to : str = 'model',
                          method : str = 'bilinear',
                          stats : bool = False,
                          figsize : tuple = (15, 8),
                          error_kwargs : dict = dict(cmap=cmo.balance, vmin=-0.5, vmax=0.5),
                          source_plots : bool = True,
                          source_kwargs : dict = dict(cmap=cmo.ice, vmin=0, vmax=1),
                          ) -> None:
        """
        Plot sea ice concentration error between ocean model and observations (model - observation).

        Parameters
        ----------
        sic_name : str, default: ``siconc``
            Name of sea ice concentration variable in model dataset.
        obs_name : str, default: ``NSIDC``
            Name of observational dataset.
            Options include ``NSIDC``, ``OISSTv2`` and ``HadISST``.
        region : str, default: ``arctic``
            Polar region of ocean observations dataset to calculate sea ice
            concentration error. Options are ``arctic`` or ``antarctic``.
        time_bounds : slice, str, default: ``None``
            Time bounds to compute sea ice concentration climatologies. Default is ``None``
            meaning the entire time series is used. Custom bounds should be specified using
            a slice object. Available pre-defined climatologies can be selected using a
            string (e.g., "1991-2020").
        freq : str, default: ``mar``
            Climatology frequency to compute sea ice concentration error.
            Options include ``total``, ``seasonal``, ``monthly``, ``jan``,
            ``feb``, `mar`` etc. for individual months.
        regrid_to : str, default: ``model``
            Regrid data to either ``model`` or observations (``obs``) target grid.
        method : str, default: ``bilinear``
            Method used to interpolate model and observed data onto target grid.
            Options include ``bilinear``, ``nearest``, ``conservative``.
        stats : bool, default: ``False``
            Return aggregated statistics of (model - observation) error.
            Includes Mean Absolute Error, Mean Square Error & Root Mean Square Error.
        figsize : tuple, default: (15, 8)
            Figure size for the plot.
        error_kwargs : dict, default: ``{'cmap':'RdBu_r', 'vmin':-0.5, 'vmax':0.5}``
            Keyword arguments for (model - observation) error matplotlib pcolormesh.
        source_plots : bool, default: ``True``
            Plot model, observations and (model - observation) error as separate subplots.
            This option is only available where climatology frequency ``freq``='total' or
            ``freq`` is a individual month (e.g., ``jan``).
        source_kwargs : dict, default: ``{'cmap':'cmo.ice', 'vmin':0, 'vmax':1}``
            Keyword arguments for model and observation matplotlib pcolormeshes.

        Returns
        -------
        matplotlib Axes
            Matplotlib axes object displaying (model - observation) sea ice concentration error.
        """
        # -- Compute Sea Ice Conc. Error -- #
        self._compute_2D_error(var_name=sic_name,
                               obs=dict(name=obs_name, region=region, var='siconc'),
                               time_bounds=time_bounds,
                               freq=freq,
                               regrid_to=regrid_to,
                               method=method,
                               stats=stats,
                               )

        # -- Plot Sea Ice Conc. Error -- #
        if region == 'arctic':
            projection = ccrs.NorthPolarStereo()
        elif region == 'antarctic':
            projection = ccrs.SouthPolarStereo()
        else:
            projection = ccrs.Robinson(central_longitude=-1)

        ax = _plot_2D_error(mv=self,
                            obs_name=obs_name,
                            var_name=sic_name,
                            projection=projection,
                            figsize=figsize,
                            error_kwargs=error_kwargs,
                            source_plots=source_plots,
                            source_kwargs=source_kwargs,
                            )
        return ax


    def compute_siarea_timeseries(self,
                                  sic_name : str = 'siconc',
                                  area_name : str = 'areacello',
                                  obs_name : str = 'NSIDC',
                                  region : str = 'arctic',
                                  time_bounds : slice | str | None = None,
                                  stats : bool = False,
                                  ) -> Self:
        """
        Compute sea ice area time series from sea ice concentration in
        ocean model and from observations.

        The sea ice area is calculated as the total area of grid cells
        where sea ice concentration exceeds 15%. This ensures compatibility
        with satellite-derived observations.

        Parameters
        ----------
        sic_name : str, default: ``siconc``
            Name of sea ice concentration variable in ocean model dataset.
        area_name : str, default: ``areacello``
            Name of ocean model grid cell area variable.
        obs_name : str, default: ``NSIDC``
            Name of observational dataset.
            Options include ``NSIDC``, ``OISSTv2`` and ``HadISST``.
        region : str, default: ``arctic``
            Polar region of ocean observations dataset to calculate sea ice
            concentration error. Options are ``arctic`` or ``antarctic``.
        time_bounds : slice, str, default: ``None``
            Time bounds to compute sea ice area.
            Default is ``None`` meaning the entire time series is returned.
            Custom bounds should be specified using a slice object.
        stats : bool, default: ``False``
            Return aggregated statistics of sea ice area in ocean model & observations.
            Includes Mean Absolute Error, Mean Square Error, Root Mean Square Error
            & Pearson Correlation Coefficient.

        Returns
        -------
        ModelValidator
            ModelValidator object including sea ice area in ocean model & observations,
            stored in the ``results`` attribute and aggregate statistics stored in the
            ``stats`` attribute.
        """
        # -- Verify Inputs -- #
        if not isinstance(sic_name, str):
            raise TypeError("``sic_name`` must be specified as a string.")
        if sic_name not in self._data.variables:
            raise ValueError(f"{sic_name} not found in ocean model dataset.")

        # -- Compute Sea Ice Area -- #
        self._compute_1D_diagnostic(var_name=area_name,
                                    mask=self._data[sic_name] > 0.15,
                                    aggregator='sum',
                                    out_name='siarea',
                                    obs=dict(name=obs_name, region=region, var='siarea'),
                                    time_bounds=time_bounds,
                                    stats=stats,
                                    )

        return self


    def plot_siarea_timeseries(self,
                               sic_name : str = 'siconc',
                               area_name : str = 'areacello',
                               obs_name : str = 'NSIDC',
                               region : str = 'arctic',
                               time_bounds : slice | str | None = None,
                               figsize : tuple = (12, 5),
                               plot_kwargs : dict = dict(linewidth=2),
                               stats : bool = False,
                               ) -> Self:
        """
        Plot sea ice area time series calculated from sea ice concentration in
        ocean model and observations.

        The sea ice area is calculated as the total area of grid cells
        where sea ice concentration exceeds 15%. This ensures compatibility
        with satellite-derived observations.

        Parameters
        ----------
        sic_name : str, default: ``siconc``
            Name of sea ice concentration variable in ocean model dataset.
        area_name : str, default: ``areacello``
            Name of ocean model grid cell area variable.
        obs_name : str, default: ``NSIDC``
            Name of observational dataset.
            Options include ``NSIDC``, ``OISSTv2`` and ``HadISST``.
        region : str, default: ``arctic``
            Polar region of ocean observations dataset to calculate sea ice
            concentration error. Options are ``arctic`` or ``antarctic``.
        time_bounds : slice, str, default: ``None``
            Time bounds to compute sea ice area.
            Default is ``None`` meaning the entire time series is plotted.
            Custom bounds should be specified using a slice object.
        figsize : tuple, default: (12, 5)
            Figure size for the plot.
        plot_kwargs : dict, default: ``{'linewidth':2}``
            Keyword arguments for matplotlib line plot. Applied to both model
            and observational sea ice area.
        stats : bool, default: ``False``
            Return aggregated statistics of sea ice area in ocean model & observations.
            Includes Mean Absolute Error, Mean Square Error, Root Mean Square Error
            & Pearson Correlation Coefficient.

        Returns
        -------
        matplotlib Axes
            Matplotlib axes object displaying model & observation sea ice area time series.
        """
        # -- Verify Inputs -- #
        if not isinstance(sic_name, str):
            raise TypeError("``sic_name`` must be specified as a string.")
        if sic_name not in self._data.variables:
            raise ValueError(f"{sic_name} not found in ocean model dataset.")

        # -- Compute Sea Ice Area -- #
        self._compute_1D_diagnostic(var_name=area_name,
                                    mask=self._data[sic_name] > 0.15,
                                    aggregator='sum',
                                    out_name='siarea',
                                    obs=dict(name=obs_name, region=region, var='siarea'),
                                    time_bounds=time_bounds,
                                    stats=stats,
                                    )

        # -- Plot Sea Ice Area -- #                          
        ax = _plot_timeseries(mv=self,
                              obs_name=obs_name,
                              var_name='siarea',
                              scale=1e-12,
                              figsize=figsize,
                              plot_kwargs=plot_kwargs,
                              labels={'x':'Time', 'y':'Sea Ice Area (km$^2$)'}
                              )

        return ax


    def load_observations(self,
                          obs_name : str,
                          var_name : str,
                          region : str | None = None,
                          time_bounds : slice | str | None = None,
                          lon_bounds : tuple | None = None,
                          lat_bounds : tuple | None = None,
                          freq : str | None = None,
                          ) -> Self:
        """
        Load ocean observations from cloud object storage.

        Parameters
        ----------
        obs_name : str
            Name of observational dataset to load.
        var_name : str
            Name of variable to load from observational dataset.
        region : str, default: ``None``
            Region of ocean observations dataset to load. Default is ``None``
            meaning the entire dataset is returned.
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
            Climatology frequency of the observational dataset.
            Options include ``None``, ``total``, ``seasonal``, ``monthly``,
            ``jan``, ``feb``, `mar`` etc. for individual months.
            Default is ``None`` meaning the entire time series is loaded.
        
        Returns
        -------
        ModelValidator
            ModelValidator object including observational data stored in ``obs``
            attribute.
        """       
        # -- Load Ocean Observations Dataset -- #
        obs_data = self._load_obs_data(obs_name=obs_name,
                                       var_name=var_name,
                                       region=region,
                                       time_bounds=time_bounds,
                                       lon_bounds=lon_bounds,
                                       lat_bounds=lat_bounds,
                                       freq=freq
                                       )

        # -- Update Observational Data -- #
        self._update_obs(da=obs_data, obs_name=obs_name.lower())

        return self
