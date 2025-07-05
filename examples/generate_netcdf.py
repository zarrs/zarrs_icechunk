#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "xarray==2025.1.2",
#     "numpy==2.2.3",
#     "netcdf4==1.7.2",
# ]
# ///

import numpy as np
import xarray as xr

shape = (2, 5, 10)
data = np.arange(np.prod(shape), dtype=np.uint32).reshape(shape)
da = xr.DataArray(data, dims=("z", "y", "x")) # .chunk({"z": 1, "y": 3, "x": 2})

encoding = {"data": {
    "shuffle": True,
    "zlib": True,
    "chunksizes": (1, 2, 2),
    "scale_factor": 0.1,
    "add_offset": 3.0,
    # NOTE: Only one of scale_factor / fletcher32 should be permitted, but python netCDF4 allows both.
    #       h5py disallows it https://github.com/h5py/h5py/blob/ed8a9d94d59c7798109249ce9d33eb5ddba5af35/h5py/_hl/filters.py#L245-L247
    # "fletcher32": True,
}}

ds = xr.Dataset({"data": da})
ds.to_netcdf('examples/data/test0.nc', encoding=encoding)

ds = xr.Dataset({"data": da + 100})
ds.to_netcdf('examples/data/test1.nc', encoding=encoding)
