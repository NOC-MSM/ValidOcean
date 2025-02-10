"""
validator.py

Description: Defines the Validator Class including methods
used to validate ocean general circulation model outputs
using ocean observations.

Author: Ollie Tooth (oliver.tooth@noc.ac.uk)
"""

# -- Import required packages -- #
import xarray as xr
import cartopy
from typing import Self

# -- Import utility functions -- #
from dataloader import DataLoader
from regrid_utils import _regrid_data
from plot_utils import _plot_global_2d

# -- Define Validator Class -- #
class Validator():
    """
    Create a Validator object to validate ocean general
    circulation model outputs using ocean observations.

    Parameters
    ----------
    data : xarray.Dataset
        xarray Dataset containing ocean model output data. 
        Coordinates of model grid cell centres must be specified
        with names: (``lon``, ``lat``). Mask variable must be specified
        for regridding. For conservative regridding, the coordinates
        bounding each model grid cell must be specified with names:
        (``lon_b``, ``lat_b``).
    DataLoader : DataLoader
        DataLoader class used to load ocean observations from
        cloud object storage or local file system.

    Attributes
    ----------
    data : xarray.Dataset
        xarray Dataset containing ocean model output data.
    """
    def __init__(self, data:xr.Dataset, DataLoader:DataLoader=DataLoader):
        # -- Verify Inputs -- #
        if not isinstance(data, xr.Dataset):
            raise TypeError("``data`` must be specified as an xarray Dataset.")

        # -- Define Class Attributes -- #
        self.data = data
        self.DataLoader = DataLoader

        # -- Verify Domain Attributes -- #
        if 'time_counter' not in data.dims:
            raise ValueError("dimension ``time_counter`` must be included in dataset.")
        if 'mask' not in data.variables:
            raise ValueError("``mask`` variable must be included in dataset.")
        if 'lon' not in data.variables:
            raise ValueError("``lon`` coordinates of model grid cell centres must be included in dataset.")
        if 'lat' not in data.variables:
            raise ValueError("``lat`` coordinates of model grid cell centres must be included in dataset.")
        
        # -- Define Domain Attributes -- #
        self.lon = self.data['lon'].squeeze()
        self.lat = self.data['lat'].squeeze()
        
    @property
    def grid_size(self) -> dict:
        if 'z' in self.data.dims:
            return {'x': self.domain.sizes['x'], 'y': self.domain.sizes['y'], 'z': self.domain.sizes['z']}
        else:
            return {'x': self.domain.sizes['x'], 'y': self.domain.sizes['y']}
    
    def _process_model_data(self, var_name:str, freq:str, timeslice:slice) -> xr.Dataset:
        """
        Process variable stored in ocean model dataset & compute climatology
        for validation with ocean observations.

        Parameters
        ----------
        var_name : str
            Name of variable in ocean model dataset.
        freq : str
            Climatology frequency to compute from ocean model dataset.
            Options include ``climatology``, ``seasonal``, ``monthly``.
        timeslice : slice
            Time slice to compute variable climatologies.

        Returns
        -------
        xarray.DataArray
            Climatology of specified variable in ocean model dataset.
        """
        # -- Validate Inputs -- #
        if not isinstance(var_name, str):
            raise TypeError("``varname`` must be specified as a string.")
        if var_name not in self.data.variables:
            raise ValueError(f"{var_name} not found in ocean model dataset.")
        
        if not isinstance(freq, str):
            raise TypeError("``freq`` must be specified as a string.")
        if freq not in ['climatology', 'seasonal', 'monthly']:
            raise ValueError("``freq`` must be one of ``climatology``, ``seasonal`` or ``monthly``.")
        
        if not isinstance(timeslice, slice):
            raise TypeError("``timeslice`` must be specified as a slice.")

        # -- Subset Model Data -- #
        mdl_data = self.data[var_name].sel(time_counter=timeslice)

        # -- Compute Climatology -- #
        if freq == 'climatology':
            mdl_data = mdl_data.mean(dim='time_counter')
        elif freq == 'seasonal':
            mdl_data = mdl_data.groupby('time_counter.season').mean()
        elif freq == 'monthly':
            mdl_data = mdl_data.groupby('time_counter.month').mean()

        return mdl_data

    def compute_2D_bias(self, var_name:str='tos_con', obs_name:str='OISSTv2', freq:str='climatology', regrid:str='model', method:str='bilinear') -> Self:
        """
        Compute 2-dimensional variable bias between ocean model
        and observations.

        Parameters
        ----------
        var_name : str, default: ``tos_con``
            Name of variable in ocean model dataset.
        obs_name : str, default: ``OISSTv2``
            Name of observational dataset to calculate model bias.
        freq : str, default: ``climatology``
            Frequency to calculate ocean model bias. 
            Options include ``climatology``, ``seasonal``, ``monthly``.
        regrid : str, default: ``model``
            Regrid data to match either ``model`` or ``obs`` grid.
        method : str, default: ``bilinear``
            Method used to interpolate simulated and observed variables onto common grid.
            Options include ``bilinear``, ``nearest``, ``conservative``.

        Returns
        -------
        Self
            Validator object including output variable, {var}_bias.
        """
        # -- Load Observational Data -- #
        obs_loader = self.DataLoader(obs_name, freq)
        if hasattr(obs_loader, f"load_{obs_name}_data"):
            obs_data = getattr(obs_loader, f"load_{obs_name}_data")()
        else:
            raise ValueError(f"load_{obs_name}_data() method not defined in DataLoader.")

        # -- Process Ocean Model Data -- #
        timeslice = slice(obs_data.attrs['start_date'], obs_data.attrs['end_date'])
        mdl_data = self._process_model_data(var_name, freq, timeslice)

        # -- Regrid Data -- #
        if regrid == 'model':
            obs_data = _regrid_data(source_grid=obs_data, target_grid=mdl_data, method=method)
        elif regrid == 'obs':
            mdl_data = _regrid_data(source_grid=mdl_data, target_grid=obs_data, method=method)
        
        # -- Compute Bias Metric -- #
        self.data[f"{var_name}_bias"] = (mdl_data - obs_data)

        return self.data
    
    def compute_sst_bias(self, sst_name:str='tos_con', obs_name:str='OISSTv2', freq:str='climatology', regrid:str='model', method:str='bilinear') -> Self:
        """
        Compute sea surface temperature bias between ocean model and observations.

        Parameters
        ----------
        sst_name : str, default: ``tos_con``
            Name of sea surface temperature variable in ocean model dataset.
        obs_name : str, default: ``OISSTv2``
            Name of sea surface temperature observational dataset to calculate model bias.
            Options include ``OISSTv2v, ``CCI`` and ``HadISST``.
        freq : str, default: ``climatology``
            Frequency to calculate model-observations SST bias. 
            Options include ``climatology``, ``seasonal``, ``monthly``.
        regrid : str, default: ``model``
            Regrid data to match either ``model`` or observations (``obs``) grid.
        method : str, default: ``bilinear``
            Method used to interpolate simulated and observed variables onto common grid.
            Options include ``bilinear``, ``nearest``, ``conservative``.

        Returns
        -------
        Self
            Validator object including output variable, {sst_name}_bias.
        """
        # -- Verify Inputs -- #
        if not isinstance(sst_name, str):
            raise TypeError("``sst_name`` name must be specified as a string.")
        if sst_name not in self.data.variables:
            raise ValueError(f"{sst_name} not found in ocean model dataset.")
        
        if not isinstance(obs_name, str):
            raise TypeError("``obs_name`` must be specified as a string.")
        if obs_name not in ['OISSTv2', 'CCI', 'HadISST']:
            raise ValueError("``obs_name`` must be one of ``OISSTv2``, ``CCI`` or ``HadISST``.")

        if not isinstance(freq, str):
            raise TypeError("``freq`` must be specified as a string.")
        if freq not in ['climatology', 'seasonal', 'monthly']:
            raise ValueError("``freq`` must be one of ``climatology``, ``seasonal`` or ``monthly``.")
        
        if not isinstance(method, str):
            raise TypeError("``method`` must be specified as a string.")
        if method not in ['bilinear', 'nearest', 'conservative']:
            raise ValueError("``method`` must be one of ``bilinear``, ``nearest`` or ``conservative``.")

        if not isinstance(regrid, str):
            raise TypeError("``regrid`` must be specified as a string.")
        if regrid not in ['model', 'obs']:
            raise ValueError("``regrid`` must be one of ``model`` or ``obs``.")
        
        # -- Compute SST Bias -- #
        data = self.compute_2D_bias(var_name=sst_name, obs_name=obs_name, freq=freq, regrid=regrid, method=method)

        return Validator(data=data)

    def plot_sst_bias(self, sst_name:str='tos_con', obs_name:str='OISSTv2', freq:str='climatology', method:str='bilinear', regrid:str='model', plt_kwargs:dict=dict(cmap='RdBu_r', vmin=-2, vmax=2), cbar_kwargs:dict=dict(orientation='horizontal', shrink=0.8)) -> None:
        """
        Plot SST bias between ocean model and observations.

        Parameters
        ----------
        sst_name : str, default: ``tos_con``
            Name of SST variable in model dataset.
        obs_name : str, default: ``OISSTv2``
            Name of observational dataset to calculate model bias.
            Options include ``OISSTv2``, ``CCI`` and ``HadISST``.
        freq : str, default: ``climatology``
            Frequency to calculate model SST bias compared with observations. 
            Options include ``climatology``, ``seasonal``, ``monthly``.
        method : str, default: ``bilinear``
            Method used to interpolate simulated and observed SSTs onto common grid.
            Options include ``bilinear``, ``nearest``, ``conservative``.
        regrid : str, default: ``model``
            Regrid SST data to match either ``model`` or ``obs`` grid.
        plt_kwargs : dict, default: ``{'cmap':'RdBu_r', 'vmin':-2, 'vmax':2}``
            Keyword arguments for matplotlib pcolormesh.
        cbar_kwargs : dict, default: ``{'orientation':'horizontal', 'shrink':0.8}``
            Keyword arguments for matplotlib colorbar.
        """
        # -- Verify Inputs -- #
        if not isinstance(sst_name, str):
            raise TypeError("variable name must be specified as a string.")
        if sst_name not in self.data.variables:
            raise ValueError(f"Variable {sst_name} not found in ocean model dataset.")
        
        if not isinstance(obs_name, str):
            raise TypeError("Observational dataset must be specified as a string.")
        if obs_name not in ['OISSTv2', 'CCI', 'HadISST']:
            raise ValueError("Observational dataset must be one of 'OISSTv2', 'CCI' or 'HadISST'.")

        if not isinstance(freq, str):
            raise TypeError("Frequency must be specified as a string.")
        if freq not in ['climatology', 'seasonal', 'monthly']:
            raise ValueError("Frequency must be one of 'climatology', 'seasonal' or 'monthly'.")
        
        if not isinstance(method, str):
            raise TypeError("Method must be specified as a string.")
        if method not in ['bilinear', 'nearest', 'conservative']:
            raise ValueError("Method must be one of 'bilinear', 'nearest' or 'conservative'.")

        if not isinstance(regrid, str):
            raise TypeError("Regrid option must be specified as a string.")
        if regrid not in ['model', 'obs']:
            raise ValueError("Regrid option must be one of 'model' or 'obs'.")

        if not isinstance(plt_kwargs, dict):
            raise TypeError("Plot kwargs must be specified as a dictionary.")
        if not isinstance(cbar_kwargs, dict):
            raise TypeError("Colorbar kwargs must be specified as a dictionary.")

        # -- Compute SST Bias -- #
        if f"{sst_name}_bias" not in self.data.variables:
            self.data = self.compute_2D_bias(var_name=sst_name, obs_name=obs_name, freq=freq, regrid=regrid, method=method)
        
        # -- Plot SST Bias -- #
        ax = _plot_global_2d(lon=self.lon,
                             lat=self.lat,
                             var=self.data[f"{sst_name}_bias"],
                             plt_kwargs=plt_kwargs,
                             cbar_kwargs=cbar_kwargs
                             )
        return ax
