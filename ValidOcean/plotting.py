"""
plot_utils.py

Description: This module contains utility functions for plotting
ocean model & observation outputs for validation.

Author: Ollie Tooth (oliver.tooth@noc.ac.uk)
"""
# -- Import required packages -- #
import xarray as xr
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature

# -- Define Plotting Utility Functions -- #
def _plot_global_2d(lon:xr.DataArray, lat:xr.DataArray, var:xr.DataArray,
                   figsize:tuple=(10,5),
                   plt_kwargs:dict={}, cbar_kwargs:dict={},
                   ) -> None:
    """
    Plot pcolormesh of 2-dimensional global variable.

    Parameters
    ----------
    lon : xarray.DataArray
        Longitude values for the field.
    lat : xarray.DataArray
        Latitude values for the field.
    var : xarray.DataArray
        2-dimensional variable to plot.
    figsize : tuple, optional
        Figure size for the plot. Default is (10,5).
    plt_kwargs : dict, optional
        Keyword arguments for plt.pcolormesh().
    cbar_kwargs : dict, optional
        Keyword arguments for plt.colorbar().
    """
    # -- Verify Inputs -- #
    if not isinstance(lon, xr.DataArray):
        raise TypeError("``lon`` must be an xarray.DataArray.")
    if not isinstance(lat, xr.DataArray):
        raise TypeError("``lat`` values must be an xarray.DataArray.")
    if not isinstance(var, xr.DataArray):
        raise TypeError("``var`` must be an xarray.DataArray.")
    if not isinstance(figsize, tuple):
        raise TypeError("figure size must be specified a tuple.")
    if not isinstance(plt_kwargs, dict):
        raise TypeError("plot kwargs must be specified as a dictionary.")
    if not isinstance(cbar_kwargs, dict):
        raise TypeError("colorbar kwargs must be specified as a dictionary.")

    # -- Land & coastline features -- #
    land_50m = cfeature.NaturalEarthFeature('physical', 'land', '50m',
                                    edgecolor='face',
                                    facecolor=cfeature.COLORS['land'])
    coast_50m = cfeature.NaturalEarthFeature('physical', 'coastline', '50m',
                                    edgecolor='black',
                                    facecolor='none')

    # -- Plotting 2-dimensional variable -- #
    fig = plt.figure(figsize=figsize)
    ax = plt.axes(projection=ccrs.Robinson(central_longitude=-1.0))
    ax.gridlines(draw_labels=True)

    # Plot variable using pcolormesh:
    colormesh = plt.pcolormesh(lon, lat, var, transform=ccrs.PlateCarree(), **plt_kwargs)
    
    # Add features:
    ax.add_feature(land_50m)
    ax.add_feature(coast_50m)

    # Add default attributes:
    cbar = plt.colorbar(colormesh, **cbar_kwargs)
    cbar.ax.tick_params(labelsize=10) 
    cbar.ax.set_xlabel('(Model - Obs.) Bias [$^{\\circ}$C]', fontdict={'fontsize': 11, 'fontweight':'bold'})

    return ax