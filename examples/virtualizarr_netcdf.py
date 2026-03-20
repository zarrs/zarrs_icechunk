#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "xarray==2026.2.0",
#     "icechunk==1.1.21",
#     "virtualizarr[icechunk,hdf]==2.4.0",
#     "h5netcdf==1.8.1",
#     "aiohttp==3.13.3",
#     "requests==2.32.5",
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
config = icechunk.RepositoryConfig.default()
config.set_virtual_chunk_container(
    icechunk.VirtualChunkContainer("https://github.com/", icechunk.http_store())
)
repo = icechunk.Repository.create(storage=storage, config=config)
session = repo.writable_session(branch="main")
combined_vds.virtualize.to_icechunk(session.store)
session.commit("Initial commit")
