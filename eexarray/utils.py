import datetime
import itertools
import os
import re
import tempfile
from typing import Any, List, Union
from zipfile import ZipFile

import rasterio  # type: ignore
import requests
import xarray as xr
from requests.adapters import HTTPAdapter
from tqdm.auto import tqdm  # type: ignore
from urllib3.util.retry import Retry  # type: ignore


def _set_nodata(file: str, nodata: Union[float, int]) -> None:
    """Set the nodata value in the metadata of an image file.

    Parameters
    ----------
    file : str
        The path to the raster file to set.
    nodata : Union[float, int]
        The value to set as nodata.
    """
    with rasterio.open(file, "r+") as img:
        img.nodata = nodata


def _flatten_list(a: List[Any]) -> List[Any]:
    """Flatten a nested list."""
    return list(itertools.chain.from_iterable(a))


def _clean_filename(s: str) -> str:
    """Convert a string into a safe-ish file path. This removes invalid characters but doesn't check for reserved or
    invalid names."""
    return re.sub(r"(?u)[^-\w]", "_", s)


def _unpack_file(file: str, out_dir: str) -> List[str]:
    """Unpack a ZIP file to a directory.

    Parameters
    ----------
    file : str
        The path to a ZIP file.
    out_dir : str
        The path to a directory to unpack files within.

    Returns
    -------
    List[str]
        Paths to the unpacked files.
    """
    unzipped = []

    with ZipFile(file, "r") as zipped:
        unzipped += zipped.namelist()
        zipped.extractall(out_dir)

    return [os.path.join(out_dir, file) for file in unzipped]


def _download_url(url: str, out_dir: str, progress: bool, max_attempts: int) -> str:
    """Download a file from a URL to a specified directory.

    Parameters
    ----------
    url : str
        The URL address of the element to download.
    out_dir : str
        The directory path to save the temporary file to.
    progress : bool
        If true, a progress bar will be displayed to track download progress.
    max_attempts : int
        The maximum number of times to retry a connection.

    Returns
    -------
    str
        The path to the downloaded file.
    """
    filename = tempfile.NamedTemporaryFile(mode="w+b", dir=out_dir, delete=False).name
    r = _create_retry_session(max_attempts).get(url, stream=True)
    file_size = int(r.headers.get("content-length", 0))

    with open(filename, "w+b") as dst, tqdm(
        total=file_size,
        unit="iB",
        unit_scale=True,
        unit_divisor=1024,
        desc="Downloading image",
        disable=not progress,
    ) as bar:
        for data in r.iter_content(chunk_size=1024):
            size = dst.write(data)
            bar.update(size)

    return filename


def _create_retry_session(max_attempts: int) -> requests.Session:
    """Create a session with automatic retries.

    https://www.peterbe.com/plog/best-practice-with-retries-with-requests
    """
    session = requests.Session()
    retry = Retry(
        total=max_attempts, read=max_attempts, connect=max_attempts, backoff_factor=0.1
    )

    adapter = HTTPAdapter(max_retries=retry)

    session.mount("http://", adapter)
    session.mount("https://", adapter)

    return session


def _dataset_from_files(files: List[str]) -> xr.Dataset:
    """Create an xarray.Dataset from a list of raster files."""
    das = [_dataarray_from_file(file) for file in files]

    return xr.merge(das)


def _dataarray_from_file(file: str) -> xr.DataArray:
    """Create an xarray.DataArray from a single file by parsing datetimes and variables from the file name.

    The file name must follow the format "{datetime}.{variable}.{extension}".
    """
    da = xr.open_rasterio(file)
    dt = _datetime_from_filename(file)
    variable = _variable_from_filename(file)

    da = da.expand_dims({"time": [dt]}).rename(variable).squeeze("band").drop("band")

    return da


def _datetime_from_filename(file: str) -> datetime.datetime:
    """Extract a datetime from a filename that follows the format "{datetime}.{variable}.{extension}" """
    basename = os.path.basename(file).split(".")[0]
    return datetime.datetime.strptime(basename, "%Y%m%dT%H%M%S")


def _variable_from_filename(file: str) -> str:
    """Extract a variable name from a filename that follows the format "{datetime}.{variable}.{extension}" """
    return os.path.basename(file).split(".")[1]
