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
from ValidOcean.preprocess import _subset_data, _compute_climatology
from ValidOcean.metrics import _compute_agg_stats
from ValidOcean.regridding import _regrid_data
from ValidOcean.plotting import _plot_global_2d

# -- Define ModelValidator Class -- #
class ModelValidator():
    """
    Create a ModelValidator object to validate ocean general
    circulation model outputs using ocean observations.

    Parameters
    ----------
    mdl_data : xarray.Dataset
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
    def __init__(self, mdl_data : xr.Dataset, dataloader : DataLoader | None = None):
        # -- Verify Inputs -- #
        if not isinstance(mdl_data, xr.Dataset):
            raise TypeError("``mdl_data`` must be specified as an xarray Dataset.")
        if dataloader is not None:
            if not isinstance(dataloader, DataLoader):
                raise TypeError("Custom ``dataloader`` must be a DataLoader object.")

        # -- Verify Domain Attributes -- #
        if 'time' not in mdl_data.dims:
            raise ValueError("dimension ``time`` must be included in model dataset.")
        if 'mask' not in mdl_data.variables:
            raise ValueError("``mask`` variable must be included in model dataset.")
        if 'lon' not in mdl_data.variables:
            raise ValueError("``lon`` coordinates of model grid cell centres must be included in model dataset.")
        if 'lat' not in mdl_data.variables:
            raise ValueError("``lat`` coordinates of model grid cell centres must be included in model dataset.")

        # -- Class Attributes -- #
        self._data = mdl_data
        self._dataloader = dataloader
        self._obs = xr.Dataset()
        self._results = xr.Dataset()
        self._stats = {}

        self._lon = self._data['lon'].squeeze()
        self._lat = self._data['lat'].squeeze()

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
    def stats(self) -> dict:
        return self._stats
    @stats.setter
    def stats(self, value: dict) -> None:
        self._stats = value

    # -- Class Methods -- #
    def __repr__(self) -> str:
        return f"\n<ModelValidator>\n\n-- Model Data --\n\n{self._data}\n\n-- Observations --\n\n{self._obs}\n\n-- Results --\n\n{self._results}\n\n-- Stats --\n\n{self._stats}"


    def _load_obs_data(self,
                       obs_name : str,
                       var_name : str,
                       region : str | None = None,
                       bounds : slice | str | None = None,
                       freq : str | None = None,
                       ) -> Self:
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
        bounds : slice, str, default: None
            Time bounds to compute climatology using ocean observations.
            Default is ``None`` meaning the entire time series is loaded.
            Custom bounds should be specified using a slice object. Available
            pre-defined climatologies can be selected using a string
            (e.g., "1991-2020").
        freq : str, default: ``None``
            Climatology frequency of the observational dataset.
            Options include ``None``, ``total``, ``seasonal``, ``monthly``.
            Default is ``None`` meaning the entire time series is loaded.
        
        Returns
        -------
        ModelValidator
            ModelValidator object including observational data stored in ``obs``
            attribute.
        """
        # -- Verify Inputs -- #
        if not isinstance(obs_name, str):
            raise TypeError("``obs_name`` must be specified as a string.")
        
        # -- Load Ocean Observations Dataset -- #
        if self._dataloader is None:
            if hasattr(data_loader, f"{obs_name}Loader"):
                ObsDataLoader = getattr(data_loader, f"{obs_name}Loader")
                obs_data = ObsDataLoader(var_name=var_name, region=region, bounds=bounds, freq=freq)._load_data()
            else:
                raise ValueError(f"undefined DataLoader specified: {obs_name}Loader.")
        else:
            obs_data = self._dataloader(var_name=var_name, region=region, bounds=bounds, freq=freq)._load_data()

        return obs_data


    def _compute_2D_error(self,
                          var_name : str = 'tos_con',
                          obs : dict = dict(name='OISSTv2', var='sst'),
                          bounds : slice | str | None = None,
                          freq : str = 'total',
                          regrid_to : str = 'model',
                          method : str = 'bilinear',
                          stats : bool = True,
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
        bounds : slice, str, default: ``None``
            Time bounds to compute climatology using ocean observations.
            Default is ``None`` meaning the entire time series is loaded.
            Custom bounds should be specified using a slice object. Available
            pre-defined climatologies can be selected using a string
            (e.g., "1991-2020").
        freq : str, default: ``total``
            Frequency to calculate ocean model error. 
            Options include ``total``, ``seasonal``, ``monthly``.
        regrid_to : str, default: ``model``
            Regrid data to match either ``model`` or ``obs`` grid.
        method : str, default: ``bilinear``
            Method used to interpolate model and observed variables onto target grid.
            Options include ``bilinear``, ``nearest``, ``conservative``.
        stats : bool, default: ``True``
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
        obs_data = self._load_obs_data(obs_name=obs['name'], var_name=obs['var'], region=obs['region'], bounds=bounds, freq=freq)

        # -- Process Ocean Model Data -- #
        if bounds is not None:
            if isinstance(bounds, str):
                bounds = slice(bounds.split('-')[0], bounds.split('-')[1])
            mdl_data = _subset_data(data=self._data[var_name], bounds=bounds, is_obs=False)

        mdl_data = _compute_climatology(data=mdl_data, freq=freq)

        # -- Regrid Data -- #
        if regrid_to == 'model':
            obs_data = _regrid_data(source_grid=obs_data, target_grid=mdl_data, method=method)
        elif regrid_to == 'obs':
            mdl_data = _regrid_data(source_grid=mdl_data, target_grid=obs_data, method=method)
        
        # -- Compute Error -- #
        mdl_error = (mdl_data - obs_data).expand_dims(dim={'obs': np.array([obs['name']])}, axis=0)
        if regrid_to == 'model':
            self.results[f"{var_name}_error"] = mdl_error
        elif regrid_to == 'obs':
            self.results[f"{var_name}_error"] = mdl_error.rename({'lon':f"lon_{obs['name']}", 'lat':f"lat_{obs['name']}"})

        # -- Compute Aggregate Statistics -- #
        if stats:
            self.stats = _compute_agg_stats(error=mdl_error)

        return self


    def compute_sst_error(self,
                          sst_name : str = 'tos_con',
                          obs_name : str = 'OISSTv2',
                          bounds : slice | str | None = None,
                          freq : str = 'total',
                          regrid_to : str = 'model',
                          method : str = 'bilinear',
                          stats : bool = True,
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
        bounds : slice, str, default: ``None``
            Time bounds to compute sea surface temperature climatologies.
            Default is ``None`` meaning the entire time series is loaded.
            Custom bounds should be specified using a slice object. Available
            pre-defined climatologies can be selected using a string
            (e.g., "1991-2020").
        freq : str, default: ``total``
            Climatology frequency to compute sea surface temperature error. 
            Options include ``total``, ``seasonal``, ``monthly``.
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
                               bounds=bounds,
                               freq=freq,
                               regrid_to=regrid_to,
                               method=method,
                               stats=stats,
                               )

        return self


    def plot_sst_error(self,
                       sst_name : str = 'tos_con',
                       obs_name : str = 'OISSTv2',
                       bounds : slice | str | None = None,
                       freq : str = 'total',
                       regrid_to : str = 'model',
                       method : str = 'bilinear',
                       stats : bool = True,
                       plt_kwargs : dict = dict(cmap='RdBu_r', vmin=-2, vmax=2),
                       cbar_kwargs : dict = dict(orientation='horizontal', shrink=0.8)
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
        bounds : slice, str, default: ``None``
            Time bounds to compute sea surface temperature climatologies. Default is ``None``
            meaning the entire time series is loaded. Custom bounds should be specified using
            a slice object. Available pre-defined climatologies can be selected using a
            string (e.g., "1991-2020").
        freq : str, default: ``total``
            Climatology frequency to compute sea surface temperature error.
            Options include ``total``, ``seasonal``, ``monthly``.
        regrid_to : str, default: ``model``
            Regrid data to either ``model`` or observations (``obs``) target grid.
        method : str, default: ``bilinear``
            Method used to interpolate model and observed data onto target grid.
            Options include ``bilinear``, ``nearest``, ``conservative``.
        stats : bool, default: ``False``
            Return aggregated statistics of (model - observation) error.
            Includes Mean Absolute Error, Mean Square Error & Root Mean Square Error.
        plt_kwargs : dict, default: ``{'cmap':'RdBu_r', 'vmin':-2, 'vmax':2}``
            Keyword arguments for matplotlib pcolormesh.
        cbar_kwargs : dict, default: ``{'orientation':'horizontal', 'shrink':0.8}``
            Keyword arguments for matplotlib colorbar.
        
        Returns
        -------
        matplotlib Axes
            Matplotlib axes object displaying (model - observation) sea surface temperature error.
        """
        # -- Compute SST Error -- #
        if f"{sst_name}_error" not in self._results.variables:
            self._compute_2D_error(var_name=sst_name,
                                   obs=dict(name=obs_name, region=None, var='sst'),
                                   bounds=bounds,
                                   freq=freq,
                                   regrid_to=regrid_to,
                                   method=method,
                                   stats=stats,
                                   )

        # -- Plot SST Error -- #
        ax = _plot_global_2d(lon=self._lon,
                             lat=self._lat,
                             var=self._results[f"{sst_name}_error"].squeeze(),
                             plt_kwargs=plt_kwargs,
                             cbar_kwargs=cbar_kwargs
                             )
        return ax


    def load_observations(self,
                          obs_name : str,
                          var_name : str,
                          region : str | None = None,
                          bounds : slice | str | None = None,
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
        bounds : slice, str, default: None
            Time bounds to extract ocean observations.
            Default is ``None`` meaning the entire time series is loaded.
            Custom bounds should be specified using a slice object. Available
            pre-defined climatologies can be selected using a string
            (e.g., "1991-2020").
        freq : str, default: ``None``
            Climatology frequency of the observational dataset.
            Options include ``None``, ``total``, ``seasonal``, ``monthly``.
            Default is ``None`` meaning the entire time series is loaded.
        
        Returns
        -------
        ModelValidator
            ModelValidator object including observational data stored in ``obs``
            attribute.
        """       
        # -- Load Ocean Observations Dataset -- #
        obs_data = self._load_obs_data(obs_name=obs_name, var_name=var_name, region=region, bounds=bounds, freq=freq)

        # -- Store Observational Data -- #
        self._obs[f"{var_name}_{obs_name}"] = obs_data.rename({'lon':f"lon_{obs_name}", 'lat':f"lat_{obs_name}"})

        # -- Remove Redundant Coordinates -- #
        coord_names = [self._obs[var].coords for var in self._obs.data_vars]
        coord_names = set([item for sublist in coord_names for item in sublist]) ^ set(self._obs.coords)
        self._obs = self._obs.drop_vars(names=coord_names)

        return self