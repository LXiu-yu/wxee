import datetime
import os
import shutil
import tempfile
import warnings

import ee
import pytest
import rasterio

import wxee.utils

try:
    ee.Initialize()
except Exception:
    warnings.warn("Earth Engine could not be initialized!")


@pytest.mark.ee
def test_replace_if_null_with_null():
    """Test that a null value is correctly replaced."""
    null = None
    replace = "test_string"

    result = wxee.utils._replace_if_null(null, replace).getInfo()

    assert result == replace


@pytest.mark.ee
def test_replace_if_null_with_string():
    """Test that a non-null string is not replaced."""
    not_null = "not null"
    replace = "test_string"

    result = wxee.utils._replace_if_null(not_null, replace).getInfo()

    assert result == not_null


@pytest.mark.ee
def test_replace_if_null_with_num():
    """Test that a non-null number is not replaced."""
    not_null = 42
    replace = 12

    result = wxee.utils._replace_if_null(not_null, replace).getInfo()

    assert result == not_null


@pytest.mark.ee
def test_formatted_date_parsed():
    """Test that a time formatted in Earth Engine can be parsed in Python."""
    Y = 2020
    M = 9
    D = 2
    H = 16
    m = 43
    s = 1

    test_date = ee.Date(f"{Y}-{M}-{D}T{H}:{m}:{s}")
    test_datetime = datetime.datetime(
        year=Y, month=M, day=D, hour=H, minute=m, second=s
    )

    formatted_result = wxee.utils._format_date(test_date).getInfo()
    parsed_result = wxee.utils._parse_time(formatted_result)

    assert parsed_result == test_datetime


def test_parse_filename():
    """Test that dimensions, coordinates, and variables are correctly parsed from a filename"""
    test_id = "image_id"
    test_dim = "dim"
    test_coord = "100"
    test_var = "temp"
    test_ext = "tif"
    test_filename = ".".join([test_id, test_dim, test_coord, test_var, test_ext])

    result_dim, result_coord, result_var = wxee.utils._parse_filename(test_filename)

    assert (result_dim, result_coord, result_var) == (
        test_dim,
        int(test_coord),
        test_var,
    )


def test_parse_filename_time():
    """Test that a time coordinate is correctly decoded from a filename"""
    Y = 2020
    M = 9
    D = 2
    H = 16
    m = 43
    s = 1

    time_str = f"{Y}{M}{D}T{H}{m}{s}"
    test_datetime = datetime.datetime(
        year=Y, month=M, day=D, hour=H, minute=m, second=s
    )

    test_filename = f"id.time.{time_str}.var.ext"

    _, result_coord, _ = wxee.utils._parse_filename(test_filename)

    assert result_coord == test_datetime


def test_parse_invalid_time_warns():
    """Test that an invalid time coordinate is noticed and raises a warning"""
    invalid_time_str = "1"

    with pytest.warns(UserWarning):
        wxee.utils._parse_time(invalid_time_str)


def test_dataarray_from_file():
    """Test that an xarray.DataArray can be created from a valid GeoTIFF."""
    file_path = os.path.join(
        "test", "test_data", "IDAHO_EPSCOR_GRIDMET_20180201.time.20180201T060000.pr.tif"
    )
    da = wxee.utils._dataarray_from_file(file_path)

    assert da.name == "pr"


def test_dataset_from_files():
    """Test than an xarray.Dataset can be created from a list of valid GeoTIFFs."""
    file_paths = [
        os.path.join(
            "test",
            "test_data",
            "IDAHO_EPSCOR_GRIDMET_20180203.time.20180203T060000.pr.tif",
        ),
        os.path.join(
            "test",
            "test_data",
            "IDAHO_EPSCOR_GRIDMET_20180201.time.20180201T060000.pr.tif",
        ),
        os.path.join(
            "test",
            "test_data",
            "IDAHO_EPSCOR_GRIDMET_20180202.time.20180202T060000.rmax.tif",
        ),
        os.path.join(
            "test",
            "test_data",
            "IDAHO_EPSCOR_GRIDMET_20180202.time.20180202T060000.pr.tif",
        ),
        os.path.join(
            "test",
            "test_data",
            "IDAHO_EPSCOR_GRIDMET_20180203.time.20180203T060000.rmax.tif",
        ),
        os.path.join(
            "test",
            "test_data",
            "IDAHO_EPSCOR_GRIDMET_20180201.time.20180201T060000.rmax.tif",
        ),
    ]

    ds = wxee.utils._dataset_from_files(file_paths)

    assert ds.time.size == 3
    assert all([var in ds.variables for var in ["pr", "rmax"]])


def test_flatten_list():
    """Test that a nested list is correctly flattened"""
    nested = [[1, 2], [3], [4, 5, 6], [7, 8]]
    flat = [1, 2, 3, 4, 5, 6, 7, 8]

    result = wxee.utils._flatten_list(nested)

    assert result == flat


def test_set_nodata():
    """Test that nodata is correctly set in an image file. To do this, a temporary copy test image
    is created, the nodata value is read from the copy, incremented to ensure a new nodata value, set,
    and tested. The copy is automatically deleted after the test has run.
    """
    file_path = os.path.join(
        "test", "test_data", "IDAHO_EPSCOR_GRIDMET_20180201.time.20180201T060000.pr.tif"
    )

    tmp_copy = tempfile.NamedTemporaryFile(delete=True).name
    shutil.copy2(file_path, tmp_copy)

    with rasterio.open(tmp_copy) as r:
        old_nodata = r.nodata

    test_nodata = old_nodata + 1

    wxee.utils._set_nodata(tmp_copy, test_nodata)

    with rasterio.open(tmp_copy) as r:
        new_nodata = r.nodata

    assert new_nodata == test_nodata
