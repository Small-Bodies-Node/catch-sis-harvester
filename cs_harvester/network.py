"""
Set HTTP User-Agent parameter.
"""

from contextlib import contextmanager
import requests as req
from astropy.utils.data import conf as astropy_conf
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
