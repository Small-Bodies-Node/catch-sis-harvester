import os
import sys
import shlex
import logging

# version info
from astropy import __version__ as astropy_version
from catch import __version__ as catch_version
from pds4_tools import __version__ as pds4_tools_version
from sbpy import __version__ as sbpy_version
from sbsearch import __version__ as sbsearch_version


def get_logger():
    from . import config

    return logging.getLogger(config.logger_name)


def setup_logger():
    from . import config

    if not os.path.exists(os.path.dirname(config.log_filename)):
        os.makedirs(os.path.dirname(config.log_filename), exist_ok=True)

    logger = logging.getLogger(config.logger_name)
    logger.setLevel(logging.INFO)

    for handler in list(logger.handlers):
        handler.close()
        logger.removeHandler(handler)

    formatter = logging.Formatter("%(levelname)s:%(name)s:%(asctime)s: %(message)s")

    handler = logging.StreamHandler(sys.stderr)
    handler.setLevel(logging.ERROR)
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    handler = logging.FileHandler(config.log_filename)
    handler.setLevel(logging.INFO)
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    if config.verbose:
        logger.setLevel(logging.DEBUG)

    logger.info("Initialized.")
    logger.debug(f"astropy {astropy_version}")
    logger.debug(f"catch {catch_version}")
    logger.debug(f"pds4_tools {pds4_tools_version}")
    logger.debug(f"sbpy {sbpy_version}")
    logger.debug(f"sbsearch {sbsearch_version}")
    logger.info("%s", " ".join([shlex.quote(s) for s in sys.argv]))

    if config.dry_run:
        logger.info("Dry run, databases will not be updated.")

    return logger
