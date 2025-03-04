"""
validator.py

Description: Defines the ModelValidator Class including methods
used to validate ocean general circulation model outputs
using ocean observations.

Author: Ollie Tooth (oliver.tooth@noc.ac.uk)
"""

# -- Import required packages -- #
import xarray as xr
import numpy as np
from typing import Self

# -- Import utility functions -- #
import ValidOcean.data_loader as data_loader
from ValidOcean.data_loader import DataLoader
from ValidOcean.preprocess import _apply_time_bounds, _compute_climatology
from ValidOcean.statistics import _compute_agg_stats
from ValidOcean.regridding import _regrid_data
from ValidOcean.plotting import _plot_2D_error

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
            # Model domain bounds rounded to nearest largest integer:
            self._lon_bounds = (np.floor(self._lon.min()), np.ceil(self._lon.max()))
            self._lat_bounds = (np.floor(self._lat.min()), np.ceil(self._lat.max()))
            if self._lon_bounds[0] >= 0:
                raise ValueError("``lon`` bounds must be within [-180, 180].")
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
    def results(self, value: xr.Dataset) -> None:
        self._data = value

    @property
    def obs(self) -> xr.Dataset:
        return self._obs
    @obs.setter
    def results(self, value: xr.Dataset) -> None:
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

    # -- Class Methods -- #
    def __repr__(self) -> str:
        return f"\n<ModelValidator>\n\n-- Model Data --\n\n{self._data}\n\n-- Observations --\n\n{self._obs}\n\n-- Results --\n\n{self._results}\n\n-- Stats --\n\n{self._stats}"


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
            if hasattr(data_loader, f"{obs_name}Loader"):
                ObsDataLoader = getattr(data_loader, f"{obs_name}Loader")
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
                    var_name: str,
                    obs_name: str
                    ) -> Self:
        """
        Update ocean observations attribute with a new DataArray.
        
        Parameters:
        -----------
        da: xr.DataArray
            Ocean observations DataArray to add to the Dataset.
        var_name: str
            Name of variable to add to Dataset.
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
        if not isinstance(var_name, str):
            raise TypeError("``var_name`` must be specified as a string.")
        if not isinstance(obs_name, str):
            raise TypeError("``obs_name`` must be specified as a string.")

        # Update ocean observations coords:
        coords = [crd for crd in da.coords]
        new_coords = {key: value for key, value in zip(coords, [f"{crd}_{obs_name}" for crd in coords])}
        da = da.rename(new_coords)

        # Remove any existing DataArray & coords:
        if f"{var_name}_{obs_name}" in self._obs.data_vars:
            names = [crd for crd in self._obs[f"{var_name}_{obs_name}"].coords]
            names.append(f"{var_name}_{obs_name}")
            self._obs = self._obs.drop_vars(names=names)

        self._obs[f"{var_name}_{obs_name}"] = da

        # Remove redundant coords:
        coord_names = [self._obs[var].coords for var in self._obs.data_vars]
        coord_names = set([item for sublist in coord_names for item in sublist]) ^ set(self._obs.coords)
        self._obs = self._obs.drop_vars(names=coord_names)

        return self


    def _update_results(self,
                        da: xr.DataArray,
                        var_name: str,
                        obs_name: str | None = None
                        ) -> Self:
        """
        Update model validation results attribute with a new DataArray.
        
        Parameters:
        -----------
        da: xr.DataArray
            Ocean model DataArray to add to the Dataset.
        var_name: str
            Name of variable to add to Dataset.
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
        if not isinstance(var_name, str):
            raise TypeError("``var_name`` must be specified as a string.")
        if obs_name is not None:
            if not isinstance(obs_name, str):
                raise TypeError("``obs_name`` must be specified as a string.")

        # Update DataArray coords if regridded to obs:
        if obs_name is not None:
            coords = [crd for crd in da.coords if crd != 'obs']
            new_coords = {key: value for key, value in zip(coords, [f"{crd}_{obs_name}" for crd in coords])}
            da = da.rename(new_coords)

        # Remove existing DataArray & coords from this source:
        if f"{var_name}" in self._results.data_vars:
            names = [crd for crd in self._results[var_name].coords]
            names.append(var_name)
            self._results = self._results.drop_vars(names=names)

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
        if time_bounds is not None:
            if isinstance(time_bounds, str):
                time_bounds = slice(time_bounds.split('-')[0], time_bounds.split('-')[1])
            mdl_data = _apply_time_bounds(data=self._data[var_name], time_bounds=time_bounds, is_obs=False)

        mdl_data = _compute_climatology(data=mdl_data, freq=freq)

        # -- Regrid Data -- #
        if regrid_to == 'model':
            obs_data = _regrid_data(source_grid=obs_data, target_grid=mdl_data, method=method)
        elif regrid_to == 'obs':
            mdl_data = _regrid_data(source_grid=mdl_data, target_grid=obs_data, method=method)
        
        # -- Compute & Store Error -- #
        mdl_error = (mdl_data - obs_data).expand_dims(dim={'obs': np.array([obs['name']])}, axis=0)
        if regrid_to == 'model':
            self._update_results(da=mdl_error, var_name=f"{var_name}_error", obs_name=None)
            self._update_results(da=mdl_data, var_name=var_name, obs_name=None)
        elif regrid_to == 'obs':
            self._update_results(da=mdl_error, var_name=f"{var_name}_error", obs_name=obs['name'].lower())
            self._update_results(da=mdl_data, var_name=var_name, obs_name=obs['name'].lower())

        # -- Store Ocean Observations Data -- #
        self._update_obs(da=obs_data, var_name=var_name, obs_name=obs['name'].lower())

        # -- Compute Aggregate Statistics -- #
        if stats:
            self.stats = _compute_agg_stats(error=mdl_error)

        return self


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
            Options include ``OISSTv2``, ``CCI`` and ``HadISST``.
        time_bounds : slice, str, default: ``None``
            Time bounds to compute sea surface temperature climatologies.
            Default is ``None`` meaning the entire time series is loaded.
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
                       stats : bool = True,
                       figsize : tuple = (15, 8),
                       plt_kwargs : dict = dict(cmap='RdBu_r', vmin=-2.5, vmax=2.5),
                       cbar_kwargs : dict = dict(orientation='vertical', shrink=0.8),
                       source_plots : bool = False,
                       ) -> None:
        """
        Plot sea surface temperature error between ocean model and observations (model - observation).

        Parameters
        ----------
        sst_name : str, default: ``tos_con``
            Name of sea surface temperature variable in model dataset.
        obs_name : str, default: ``OISSTv2``
            Name of observational dataset.
            Options include ``OISSTv2``, ``CCI`` and ``HadISST``.
        time_bounds : slice, str, default: ``None``
            Time bounds to compute sea surface temperature climatologies. Default is ``None``
            meaning the entire time series is loaded. Custom bounds should be specified using
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
        plt_kwargs : dict, default: ``{'cmap':'RdBu_r', 'vmin':-2, 'vmax':2}``
            Keyword arguments for matplotlib pcolormesh. Only applied to (model - observation)
            error.
        cbar_kwargs : dict, default: ``{'orientation':'horizontal', 'shrink':0.8}``
            Keyword arguments for matplotlib colorbar. Only applied to (model - observation)
            error.
        source_plots : bool, default: ``False``
            Plot model, observations and (model - observation) error as separate subplots.
            This option is only available where climatology frequency ``freq``='total'.

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
        ax = _plot_2D_error(mv=self,
                            obs_name=obs_name,
                            var_name=sst_name,
                            figsize=figsize,
                            plt_kwargs=plt_kwargs,
                            cbar_kwargs=cbar_kwargs,
                            source_plots=source_plots
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
            meaning the entire dataset is loaded.
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
        self._update_obs(da=obs_data, var_name=var_name, obs_name=obs_name.lower())

        return self