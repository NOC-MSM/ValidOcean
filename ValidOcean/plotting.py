"""
plot_utils.py

Description: This module contains utility functions for plotting
ocean model & observation outputs for validation.

Author: Ollie Tooth (oliver.tooth@noc.ac.uk)
"""
# -- Import required packages -- #
import warnings
import numpy as np
import xarray as xr
import cmocean.cm as cmo
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature

# -- Define Plotting Utility Functions -- #
def _plot_timeseries(mv,
                     obs_name : str,
                     var_name : str,
                     scale : float = 1.0,
                     figsize : tuple = (15, 8),
                     plot_kwargs: dict = {},
                     labels: dict = {'x': 'Time', 'y': 'Variable'},
                     ) -> plt.Axes:
    """
    Plot time series of model & observation data for a given
    1-dimensional variable.

    Parameters
    ----------
    mv : ModelValidator
        ModelValidator object.
    obs_name : str
        Name of observations dataset.
    var_name : str
        Name of variable to plot.
    scale : float, default: 1.0
        Scale factor to multiply variables by.
    figsize : tuple, default: (15, 8)
        Figure size for the plot.
    plot_kwargs : dict, default: {}
        Keyword arguments for matplotlib 
        line plot.

    Returns
    -------
    plt.Axes
        Matplotlib axes object.
    """
    # -- Verify Inputs -- #
    if not isinstance(obs_name, str):
        raise TypeError("``obs_name`` must be a string.")
    if not isinstance(var_name, str):
        raise TypeError("``var_name`` must be a string.")
    if not isinstance(scale, (int, float)):
        raise TypeError("``scale`` must be numeric.")
    if not isinstance(plot_kwargs, dict):
        raise TypeError("``plot_kwargs`` must be a dictionary.")
    if not isinstance(labels, dict):
        raise TypeError("``labels`` must be a dictionary.")

    # -- Plotting 1-dimensional variable -- #
    _, axs = plt.subplots(nrows=1, ncols=1, figsize=figsize)

    axs.grid(True, linewidth=1, linestyle='--', color='0.7')
    axs.plot(mv._results['time'], scale*mv._results[var_name].squeeze(), label='Model', color='dodgerblue', **plot_kwargs)
    axs.plot(mv._obs[f"time_{obs_name.lower()}"], scale*mv._obs[f"{var_name}_{obs_name.lower()}"].squeeze(), label='Observations', color='darkorange', **plot_kwargs)

    axs.set_xlabel(labels['x'], fontsize=12, fontweight='bold')
    axs.set_ylabel(labels['y'], fontsize=12, fontweight='bold')
    axs.tick_params(axis='both', which='major', labelsize=12)
    axs.tick_params(axis='both', which='minor', labelsize=12)
    axs.legend(prop={'size': 11, 'weight': 'bold'})

    return axs


def _plot_2D_error(mv,
                   obs_name : str,
                   var_name : str,
                   projection : ccrs.Projection,
                   figsize : tuple = (15, 8),
                   error_kwargs: dict = {},
                   source_plots: bool = False,
                   source_kwargs: dict = {},
                   ) -> plt.Axes:
    """
    Plot error between ocean model and observations (model - observation)
    for a given 2-dimensional variable.

    Parameters
    ----------
    mv : ModelValidator
        ModelValidator object containing error data.
    obs_name : str
        Name of observations dataset.
    var_name : str
        Name of variable to plot.
    projection : ccrs.Projection
        Cartopy geographic projection to use.
    figsize : tuple, default: (15, 8)
        Figure size for the plot.
    error_kwargs : dict, default: {}
        Keyword arguments for (model - obs.) error
        matplotlib pcolormesh.
    source_plots : bool, default: ``False``
        Plot model, obs. and (model - obs.) error
        as separate subplots. This option is only
        available where climatology frequency ``freq``='total'.
    source_kwargs : dict, default: {}
        Keyword arguments for model and obs. matplotlib 
        pcolormeshes.

    Returns
    -------
    plt.Axes
        Matplotlib axes object.
    """
    # -- Verify Inputs -- #
    if not isinstance(obs_name, str):
        raise TypeError("``obs_name`` must be a string.")
    if not isinstance(var_name, str):
        raise TypeError("``var_name`` must be a string.")
    if not isinstance(error_kwargs, dict):
        raise TypeError("``error_kwargs`` must be a dictionary.")
    if not isinstance(source_plots, bool):
        raise TypeError("``source_plots`` must be a boolean.")
    if not isinstance(source_kwargs, dict):
        raise TypeError("``source_kwargs`` must be a dictionary.")

    # -- Plotting 2-dimensional variable -- #

    # Climatology Frequency = ['monthly', 'seasonal']:
    if (mv._results[var_name].ndim > 2):
        if (source_plots):
            warnings.warn("source_plots = True is only available where freq = 'total'. Using source_plots = False.", RuntimeWarning)

        rows = mv._results[var_name].dims[0]
        if rows == 'season':
            row_vals = mv._results[rows].values
            row_labels = row_vals
            _, axs = plt.subplots(nrows=2, ncols=2, figsize=figsize, subplot_kw={"projection": projection})
        elif rows == 'month':
            row_vals = np.arange(1, 13)
            row_labels = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
            _, axs = plt.subplots(nrows=4, ncols=3, figsize=figsize, subplot_kw={"projection": projection})

        axs = axs.flatten()
        for n, row_n in enumerate(row_vals):
            _plot_var_2d(axs[n], mv._results['lon'], mv._results['lat'], mv._results[f"{var_name}_error"].sel({rows: row_n}).squeeze(), plt_kwargs=error_kwargs, cbar_label=f"(Model - Obs.) Error | {row_labels[n]}")

    # Climatology Frequency = ['total']:
    else:
        if source_plots:
            _, axs = plt.subplots(nrows=3, ncols=1, figsize=figsize, subplot_kw={"projection": projection})

            _plot_var_2d(axs[0], mv._results['lon'], mv._results['lat'], mv._results[var_name], plt_kwargs=source_kwargs, cbar_label="Model")

            _plot_var_2d(axs[1], mv._obs[f"lon_{obs_name.lower()}"], mv._obs[f"lat_{obs_name.lower()}"], mv._obs[f"{var_name}_{obs_name.lower()}"], plt_kwargs=source_kwargs, cbar_label="Observations")

            _plot_var_2d(axs[2], mv._results['lon'], mv._results['lat'], mv._results[f"{var_name}_error"].squeeze(), plt_kwargs=error_kwargs, cbar_label="(Model - Obs.) Error")

        else:
            _, axs = plt.subplots(nrows=1, ncols=1, figsize=figsize, subplot_kw={"projection": projection})

            _plot_var_2d(axs, mv._results['lon'], mv._results['lat'], mv._results[f"{var_name}_error"].squeeze(), plt_kwargs=error_kwargs, cbar_label="(Model - Obs.) Error")

    return axs


def _plot_var_2d(ax: plt.Axes,
                 lon: xr.DataArray,
                 lat: xr.DataArray,
                 var: xr.DataArray,
                 plt_kwargs: dict,
                 cbar_label: str = "(Model - Obs.) Error",
                 ) -> plt.Axes:
    """
    Plot pcolormesh of 2-dimensional variable.

    Parameters
    ----------
    ax : plt.Axes
        Matplotlib axes object.
    lon : xr.DataArray
        Longitude values.
    lat : xr.DataArray
        Latitude values.
    var : xr.DataArray
        2-dimensional variable to plot.
    figsize : tuple, optional
        Figure size for the plot. Default is (10,5).
    plt_kwargs : dict, optional
        Keyword arguments for plt.pcolormesh().
    cbar_kwargs : dict, optional
        Keyword arguments for plt.colorbar().
    cbar_label : str, optional
        Colorbar label. Default is "(Model - Obs.) Error".
    """
    # -- Verify Inputs -- #
    if not isinstance(ax, plt.Axes):
        raise TypeError("``ax`` must be a matplotlib axes object.")
    if not isinstance(lon, xr.DataArray):
        raise TypeError("``lon`` must be an xarray DataArray.")
    if not isinstance(lat, xr.DataArray):
        raise TypeError("``lat`` must be an xarray DataArray.")
    if not isinstance(var, xr.DataArray):
        raise TypeError("``var`` must be an xarray DataArray.")
    if var.ndim != 2:
        raise ValueError("``var`` must be a 2-dimensional xarray DataArray.")
    if not isinstance(plt_kwargs, dict):
        raise TypeError("``plt_kwargs`` must be specified as a dictionary.")
    if not isinstance(cbar_label, str):
        raise TypeError("``cbar_label`` must be a string.")

    # -- Land & coastline features -- #
    land_50m = cfeature.NaturalEarthFeature('physical', 'land', '50m',
                                    edgecolor='face',
                                    facecolor=cfeature.COLORS['land'])
    coast_50m = cfeature.NaturalEarthFeature('physical', 'coastline', '50m',
                                    edgecolor='black',
                                    facecolor='none')

    # -- Plotting 2-dimensional variable -- #
    ax.gridlines(draw_labels=False)

    # Plot variable using pcolormesh:
    colormesh = ax.pcolormesh(lon, lat, var, transform=ccrs.PlateCarree(), **plt_kwargs)
    
    # Add features:
    ax.add_feature(land_50m)
    ax.add_feature(coast_50m)

    # Add default attributes:
    cbar = plt.colorbar(colormesh, ax=ax, orientation='vertical', shrink=0.8)
    cbar.ax.tick_params(labelsize=10)
    cbar.ax.set_ylabel(cbar_label, rotation=270, labelpad=20, fontdict={'fontsize': 10, 'fontweight':'bold'})

    return ax
