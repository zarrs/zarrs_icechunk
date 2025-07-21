#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "xarray==2025.6.0",
#     "icechunk==1.0.0",
#     "virtualizarr[icechunk,hdf]==2.0.0",
#     "h5netcdf==1.6.3",
#     "aiohttp==3.12.13",
#     "requests==2.32.4",
# ]
# ///

import os

import xarray as xr
import icechunk
from virtualizarr import open_virtual_mfdataset
from virtualizarr.parsers import HDFParser
from virtualizarr.registry import ObjectStoreRegistry
from obstore.store import HTTPStore
import warnings

warnings.filterwarnings(
    "ignore",
    message="Numcodecs codecs are not in the Zarr version 3 specification*",
    category=UserWarning,
)

registry = ObjectStoreRegistry({"https://github.com": HTTPStore("https://github.com")})

combined_vds = open_virtual_mfdataset(
    [
        "https://github.com/zarrs/zarrs_icechunk/raw/refs/heads/main/examples/data/test0.nc",
        "https://github.com/zarrs/zarrs_icechunk/raw/refs/heads/main/examples/data/test1.nc",
    ],
    registry=registry,
    parser=HDFParser(),
    concat_dim="z",
    combine="nested",
)

storage = icechunk.local_filesystem_storage("examples/data/test.icechunk.zarr")
repo = icechunk.Repository.create(storage=storage)
session = repo.writable_session(branch="main")
combined_vds.virtualize.to_icechunk(session.store)
session.commit("Initial commit")
