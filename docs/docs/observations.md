# **Observations**

!!! abstract "Summary"

    **This page introduces the framework for accessing ocean observations via the **ValidOcean** package & includes a catalog of currently available ocean observation datasets.**

---

## **DataLoaders** :material-download:

One of the foundational concepts behind the **ValidOcean** library was to enable users to access ocean observations data stored in [zarr](https://zarr.dev) format in the cloud through a method that is independent of their local or remote machine.

A major benefit of storing ocean observations in cloud object storage is that users can then directly access this data via a read-only URL.

In **ValidOcean**, we've taken this data accessibility one step further by creating DataLoaders which load ocean observations from the JASMIN object store and pre-process them into a standardised [xarray Dataset](https://docs.xarray.dev/en/latest/user-guide/data-structures.html).

---

## **Available Ocean Observations** :ocean:

!!! info "Section Currently Under Development: Come Back Soon!"
    
    **This section will include a catalog of DataLoaders currently available in the ValidOcean package to access ocean observations stored in cloud object storage.**

When using the ocean observations ARCO datasets available in **ValidOcean**, users should always acknowledge the original source of the data. Full details of the source, versions and pre-processing steps are available in the [xarray](https://docs.xarray.dev/en/latest/generated/xarray.Dataset.attrs.html) ``.attrs`` property.

* NOAA High-resolution Blended Analysis of Daily SST ([OISSTv2](https://doi.org/10.1175/JCLI-D-20-0166.1)) - sea surface temperature (sst) and sea ice concentration (siconc) dataset.

* NOAA National Snow & Ice Data Center Sea Ice Index v3 ([NSIDC](https://doi.org/10.7265/N5K072F8)) - sea ice concentration (siconc), sea ice extent (siext), sea ice area (siarea).

* Met Office Hadley Centre global sea ice & sea surface temperature ([HadISST1](https://doi.org/10.1029/2002JD002670)) - sea ice (siconc) and sea surface temperature (sst) data set - **Academic Use Only**.