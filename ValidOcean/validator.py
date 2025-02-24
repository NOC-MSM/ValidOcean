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
from ValidOcean.preprocess import _preprocess_model_data
from ValidOcean.statistics import _compute_agg_stats
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
        self._results = xr.Dataset()
        self._stats = {}

        self._lon = self._data['lon'].squeeze()
        self._lat = self._data['lat'].squeeze()

    # -- Class Properties -- #
    @property
    def data(self) -> xr.Dataset:
        return self._data
    @data.setter
    def results(self, value: xr.Dataset = xr) -> None:
        self._data = value

    @property
    def results(self) -> xr.Dataset:
        return self._results
    @results.setter
    def results(self, value: xr.Dataset = xr) -> None:
        self._results = value

    @property
    def stats(self) -> dict:
        return self._stats
    @stats.setter
    def stats(self, value: dict) -> None:
        self._stats = value

    # -- Class Methods -- #
    def load_observations(self,
                          obs_name : str,
                          var_name : str,
                          freq : str = 'total'
                          ) -> Self:
        """
        Load ocean observations dataset.

        Parameters
        ----------
        obs_name : str
            Name of observational dataset to load.
        var_name : str
            Name of variable to load from observational dataset.
        freq : str, default: ``total``
            Climatology frequency of the observational dataset.
            Options include ``total``, ``seasonal``, ``monthly``.
        
        Returns
        -------
        ModelValidator
            ModelValidator object including observational data stored in ``results``
            attribute.
        """
        # -- Verify Inputs -- #
        if not isinstance(obs_name, str):
            raise TypeError("``obs_name`` must be specified as a string.")
        if not isinstance(var_name, str):
            raise TypeError("``var_name`` must be specified as a string.")
        if not isinstance(freq, str):
            raise TypeError("``freq`` must be specified as a string.")
        if freq not in ['total', 'seasonal', 'monthly']:
            raise ValueError("``freq`` must be one of ``total``, ``seasonal`` or ``monthly``.")
        
        # -- Load Ocean Observations Dataset -- #
        if self._dataloader is None:
            if hasattr(data_loader, f"{obs_name}Loader"):
                ObsDataLoader = getattr(data_loader, f"{obs_name}Loader")
                obs_data = ObsDataLoader(var_name=var_name, freq=freq)._load_data()
            else:
                raise ValueError(f"undefined DataLoader specified: {obs_name}Loader.")
        else:
            obs_data = self._dataloader(var_name=var_name, freq=freq)._load_data()

        # -- Store Observational Data -- #
        self.results[f"{var_name}_{obs_name}"] = obs_data.rename({'lon':f"lon_{obs_name}", 'lat':f"lat_{obs_name}"})

        return self

    def _compute_2D_error(self,
                          var_name : str = 'tos_con',
                          obs : dict = dict(name='OISSTv2', var='sst'),
                          freq : str = 'total',
                          regrid_to : str = 'model',
                          method : str = 'bilinear',
                          agg_stats : bool = True,
                          ) -> Self:
        """
        Compute 2-dimensional variable error between ocean model output
        and observations.

        Parameters
        ----------
        var_name : str, default: ``tos_con``
            Name of variable in ocean model dataset.
        obs : dict, default: {``name``:``OISSTv2``, ``var``: ``sst``}
            Dictionary defining the ``name`` of the observational
            dataset and variable ``var`` to calculate model error.
        freq : str, default: ``total``
            Frequency to calculate ocean model error. 
            Options include ``total``, ``seasonal``, ``monthly``.
        regrid_to : str, default: ``model``
            Regrid data to match either ``model`` or ``obs`` grid.
        method : str, default: ``bilinear``
            Method used to interpolate model and observed variables onto target grid.
            Options include ``bilinear``, ``nearest``, ``conservative``.
        agg_stats : bool, default: ``True``
            Return aggregated statistics of (model - observation) error.
            Includes Mean Absolute Error, Mean Square Error & Root Mean Square Error.

        Returns
        -------
        ModelValidator
            ModelValidator object including variable, {var_name}_error, stored in ``results``
            attribute and aggregate statistics stored in ``stats`` attribute.
        """
        # -- Verify Inputs -- #
        if not isinstance(var_name, str):
            raise TypeError("``var_name`` name must be specified as a string.")
        if var_name not in self._data.variables:
            raise ValueError(f"{var_name} not found in ocean model dataset.")
        
        if not isinstance(obs, dict):
            raise TypeError("``obs`` must be specified as a dictionary.")
        if 'name' not in obs.keys():
            raise ValueError("``obs`` dictionary must contain key ``name``.")
        if 'var' not in obs.keys():
            raise ValueError("``obs`` dictionary must contain key ``var``.")

        if not isinstance(freq, str):
            raise TypeError("``freq`` must be specified as a string.")
        if freq not in ['total', 'seasonal', 'monthly']:
            raise ValueError("``freq`` must be one of ``total``, ``seasonal`` or ``monthly``.")

        if not isinstance(regrid_to, str):
            raise TypeError("``regrid`` must be specified as a string.")
        if regrid_to not in ['model', 'obs']:
            raise ValueError("``regrid`` must be one of ``model`` or ``obs``.")

        if not isinstance(method, str):
            raise TypeError("``method`` must be specified as a string.")
        if method not in ['bilinear', 'nearest', 'conservative']:
            raise ValueError("``method`` must be one of ``bilinear``, ``nearest`` or ``conservative``.")

        if not isinstance(agg_stats, bool):
            raise TypeError("``agg_stats`` must be specified as a boolean.")

        # -- Load Observational Data -- #
        if self._dataloader is None:
            if hasattr(data_loader, f"{obs['name']}Loader"):
                ObsDataLoader = getattr(data_loader, f"{obs['name']}Loader")
                obs_data = ObsDataLoader(var_name=obs['var'], freq=freq)._load_data()
            else:
                raise ValueError(f"undefined DataLoader specified: {obs['name']}Loader.")
        else:
            obs_data = self._dataloader(var_name=obs['var'], freq=freq)._load_data()

        # -- Process Ocean Model Data -- #
        mdl_data = _preprocess_model_data(var=self._data[var_name],
                                          freq=freq,
                                          timeslice=slice(obs_data.attrs['start_time'], obs_data.attrs['end_time'])
                                          )

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
            self.results[f"{var_name}_error"] = mdl_error.rename({'lon':'lon_obs', 'lat':'lat_obs'})

        # -- Compute Aggregate Statistics -- #
        if agg_stats:
            self.stats = _compute_agg_stats(mdl_error=mdl_error)

        return self


    def compute_sst_error(self,
                          sst_name : str = 'tos_con',
                          obs_name : str = 'OISSTv2',
                          freq : str = 'total',
                          regrid_to : str = 'model',
                          method : str = 'bilinear',
                          agg_stats : bool = True,
                          ) -> Self:
        """
        Compute sea surface temperature error between ocean model and observations.

        Parameters
        ----------
        sst_name : str, default: ``tos_con``
            Name of sea surface temperature variable in ocean model dataset.
        obs_name : str, default: ``OISSTv2``
            Name of observational dataset.
            Options include ``OISSTv2``, ``CCI`` and ``HadISST``.
        freq : str, default: ``total``
            Climatology frequency to compute (model - observation) sea surface temperature error. 
            Options include ``total``, ``seasonal``, ``monthly``.
        regrid_to : str, default: ``model``
            Regrid data to either ``model`` or observations (``obs``) target grid.
        method : str, default: ``bilinear``
            Method used to interpolate model and observed data onto target grid.
            Options include ``bilinear``, ``nearest``, ``conservative``.
        agg_stats : bool, default: ``False``
            Return aggregated statistics of (model - observation) error.
            Includes Mean Absolute Error, Mean Square Error & Root Mean Square Error.

        Returns
        -------
        ModelValidator
            ModelValidator object including variable, {sst_name}_error, stored in ``data``
            attribute and aggregate statistics stored in ``stats`` attribute.
        """
        # -- Compute SST Error -- #
        self._compute_2D_error(var_name=sst_name,
                               obs=dict(name=obs_name, var='sst'),
                               freq=freq,
                               regrid_to=regrid_to,
                               method=method,
                               agg_stats=agg_stats,
                               )

        return self


    def plot_sst_error(self,
                       sst_name : str = 'tos_con',
                       obs_name : str = 'OISSTv2',
                       freq : str = 'total',
                       regrid_to : str = 'model',
                       method : str = 'bilinear',
                       agg_stats : bool = True,
                       plt_kwargs : dict = dict(cmap='RdBu_r', vmin=-2, vmax=2),
                       cbar_kwargs : dict = dict(orientation='horizontal', shrink=0.8)
                       ) -> None:
        """
        Plot sea surface temperature error between ocean model and observations.

        Parameters
        ----------
        sst_name : str, default: ``tos_con``
            Name of sea surface temperature variable in model dataset.
        obs_name : str, default: ``OISSTv2``
            Name of observational dataset.
            Options include ``OISSTv2``, ``CCI`` and ``HadISST``.
        freq : str, default: ``total``
            Climatology frequency to compute (model - observation) sea surface temperature error.
            Options include ``total``, ``seasonal``, ``monthly``.
        regrid_to : str, default: ``model``
            Regrid data to either ``model`` or observations (``obs``) target grid.
        method : str, default: ``bilinear``
            Method used to interpolate model and observed data onto target grid.
            Options include ``bilinear``, ``nearest``, ``conservative``.
        agg_stats : bool, default: ``False``
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
        if f"{sst_name}_error" not in self._data.variables:
            self._compute_2D_error(var_name=sst_name,
                                   obs=dict(name=obs_name, var='sst'),
                                   freq=freq,
                                   regrid_to=regrid_to,
                                   method=method,
                                   agg_stats=agg_stats,
                                   )

        # -- Plot SST Error -- #
        ax = _plot_global_2d(lon=self._lon,
                             lat=self._lat,
                             var=self.results[f"{sst_name}_error"].squeeze(),
                             plt_kwargs=plt_kwargs,
                             cbar_kwargs=cbar_kwargs
                             )
        return ax
