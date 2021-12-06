import os
import tempfile
import warnings
from typing import Optional

import requests
from requests.adapters import HTTPAdapter
from tqdm.auto import tqdm  # type: ignore
from urllib3.util.retry import Retry  # type: ignore

from wxee.exceptions import DownloadError


def _create_session(max_attempts: int, backoff: float) -> requests.Session:
    """Create a requests Session with retrying.

    References
    ----------
    https://www.peterbe.com/plog/best-practice-with-retries-with-requests
    """
    session = requests.Session()
    retry = Retry(total=max_attempts, backoff_factor=backoff)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)

    return session


def _download_url(
    url: str,
    out_dir: str,
    session: Optional[requests.Session] = None,
    timeout: int = 60,
    max_attempts: int = 10,
    backoff: float = 0.1,
    progress: bool = False,
) -> str:
    """Download a file from a URL to a tempfile in a specified directory.

    Parameters
    ----------
    url : str
        The URL address of the element to download.
    out_dir : str
        The directory path to save the temporary file to.
    session : requests.Session, optional
        An optional Session object to use for downloading. If none is given, a session
        will be created.
    timeout : int
        The maximum number of seconds to wait for responses before aborting the connection.
    max_attempts : int
        The maximum number of times to retry a connection.
    backoff : float
        A backoff factor to apply on successive failed attempts. Larger numbers will create
        increasingly long delays between requests.
    progress : bool
        If true, a progress bar will be displayed to track download progress.

    Returns
    -------
    str
        The path to the downloaded temp file.
    """
    session = _create_session(max_attempts, backoff) if not session else session

    filename = tempfile.NamedTemporaryFile(mode="w+b", dir=out_dir, delete=False).name
    try:
        try:
            r = session.get(url, stream=True, timeout=timeout)
            request_size = int(r.headers.get("content-length", 0))

            try:
                r.raise_for_status()
            except requests.exceptions.HTTPError as e:
                raise DownloadError(
                    "An HTTP Error was encountered. Try increasing 'max_attempts' or running again later."
                )

            with open(filename, "wb") as dst, tqdm(
                total=request_size,
                unit="iB",
                unit_scale=True,
                unit_divisor=1024,
                desc="Downloading",
                disable=not progress,
            ) as bar:
                for chunk in r.iter_content(chunk_size=1024):
                    size = dst.write(chunk)
                    bar.update(size)

        except requests.exceptions.Timeout as e:
            raise DownloadError(
                "The connection timed out. Try increasing 'timeout' or running again later."
            )
        except requests.exceptions.ConnectionError as e:
            raise DownloadError(
                "A ConnectionError was encountered. Try increasing 'max_attempts' or running again later."
            )

    # If the download fails for any reason, delete the temp file
    except Exception as e:
        os.remove(filename)
        raise e

    downloaded_size = os.path.getsize(filename)

    if downloaded_size != request_size:
        warnings.warn(
            f"Download error: {downloaded_size} bytes out of {request_size} were retrieved. Data may be incomplete or corrupted."
        )

    return filename
