import os

import pytest
import requests
import requests_mock

import wxee.download
from wxee.exceptions import DownloadError

TEST_URL = "https://amockurlfortestingwxee.biz"
TEST_OUT_DIR = os.path.join("test", "test_data")
TEST_CONTENT = "Wenn ist das Nunstück git und Slotermeyer? Ja! Beiherhund das Oder die Flipperwaldt gersput!"
CONTENT_LENGTH = len(TEST_CONTENT.encode("utf-8"))
BAD_CODES = [
    requests.codes.internal_server_error,
    requests.codes.bad_gateway,
    requests.codes.service_unavailable,
    requests.codes.gateway_timeout,
    requests.codes.too_many_requests,
    requests.codes.request_timeout,
    requests.codes.not_found,
]


def test_download_url_creates_file():
    """Test that the download_url function downloads a mock file with correct content."""
    with requests_mock.Mocker() as m:
        m.get(
            TEST_URL, text=TEST_CONTENT, headers={"content-length": str(CONTENT_LENGTH)}
        )
        file = wxee.download._download_url(TEST_URL, TEST_OUT_DIR, progress=False)

        assert os.path.isfile(file)

        with open(file, "r") as result:
            assert result.read() == TEST_CONTENT

        os.remove(file)


def test_download_url_warns_if_incomplete():
    """Test that the download_url function raises a warning when incomplete data is downloaded."""
    with requests_mock.Mocker() as m:
        # Set an incorrect content length to trigger the incomplete download warning
        m.get(
            TEST_URL,
            text=TEST_CONTENT,
            headers={"content-length": str(CONTENT_LENGTH + 5)},
        )

        with pytest.warns(UserWarning):
            file = wxee.download._download_url(
                TEST_URL, TEST_OUT_DIR, progress=False, max_attempts=1
            )

        os.remove(file)


def test_download_url_fails_with_bad_status():
    """Test that the download_url function fails correctly with a bad status code."""
    with requests_mock.Mocker() as m:
        for code in BAD_CODES:
            m.get(TEST_URL, text=TEST_CONTENT, status_code=code)

            with pytest.raises(DownloadError):
                wxee.download._download_url(
                    TEST_URL, TEST_OUT_DIR, progress=False, max_attempts=1
                )


def test_download_url_fails_with_timeout():
    """Test that the download_url function fails correctly with a Timeout exception."""
    with requests_mock.Mocker() as m:
        m.get(TEST_URL, exc=requests.exceptions.Timeout)

        with pytest.raises(DownloadError):
            wxee.download._download_url(
                TEST_URL, TEST_OUT_DIR, progress=False, max_attempts=1
            )


def test_download_url_fails_with_connection_error():
    """Test that the download_url function fails correctly with a ConnectionError exception."""
    with requests_mock.Mocker() as m:
        m.get(TEST_URL, exc=requests.exceptions.ConnectionError)

        with pytest.raises(DownloadError):
            wxee.download._download_url(
                TEST_URL, TEST_OUT_DIR, progress=False, max_attempts=1
            )
