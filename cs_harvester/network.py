"""
Set HTTP User-Agent parameter.
"""

import urllib
from time import sleep
from contextlib import contextmanager
import requests as req
from astropy.utils.data import conf as astropy_conf, download_file as _download_file

from .exceptions import LabelError
from .logger import get_logger
from . import __version__

user_agent = f"CATCH-SIS Harvester {__version__}"


@contextmanager
def session():
    """Set HTTP User-Agent in a requests session.


    Example
    -------

    >>> with session() as req:
    ...     req.get("https://pdssbn.astro.umd.edu/")

    """

    with req.Session() as s:
        s.headers.update({"User-Agent": user_agent})
        yield s


@contextmanager
def set_astropy_useragent():
    """Set astropy's HTTP User-Agent.


    Example
    -------

    >>> from astropy.io import fits
    >>> with set_astropy_useragent():
    ...     fits.open("https://pdssbn.astro.umd.edu/holdings/ear-c-ccd-3-edr-halley-outburst-uh-v1.0/data/19910412/uh00896.fit")

    """

    with astropy_conf.set_temp("default_http_user_agent", user_agent):
        yield


def download_file(url: str, max_attempts: int = 5) -> str:
    """Download a file from a URL and save.


    Parameters
    ----------

    url : str
        The URL.

    max_attempts : int, optional
        Re-try failed downloads ``max_attempts`` times with an increasing delay
        between each attempt.


    Returns
    -------
    filename : str
        The local file name of the saved data.

    """

    logger = get_logger()

    attempts = 0
    while attempts < max_attempts:
        try:
            with set_astropy_useragent():
                file_name = _download_file(url, cache=False, show_progress=False)
            break
        except urllib.error.URLError as e:
            logger.error(str(e))
            attempts += 1
            if attempts >= max_attempts:
                raise LabelError(
                    f"Failed to download {url} in {attempts} attempts"
                ) from e
            sleep(3 + 2**attempts)  # retry, but not too soon

    return file_name
