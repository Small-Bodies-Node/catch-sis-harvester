"""Harvest Catalina Sky Survey metadata from PSI.

As of Feb 2022, CSS data are continuously archived.  This script examines a file
list generated at PSI and downloads new calibrated data labels for metadata
harvesting.

"""

import os
import re
import sys
from time import sleep
import email
import urllib
import argparse
import logging
import sqlite3
import gzip
from datetime import datetime
from contextlib import contextmanager

import requests
from astropy.time import Time
from pds4_tools import pds4_read

from catch import Catch
from sbsearch.logging import ProgressTriangle

from ..lidvid import LIDVID
from ..logger import get_logger, setup_logger
from ..harvest_log import HarvestLog
from ..exceptions import ConcurrentHarvesting
from ..process import process

# URL for the latest list of all files.
LATEST_FILES = (
    "https://sbnarchive.psi.edu/pds4/surveys/catalina_extras/file_list.latest.txt.gz"
)

# URL prefix for the CSS archive at PSI
ARCHIVE_PREFIX = "https://sbnarchive.psi.edu/pds4/surveys/"


DB_SETUP = (
    """
    CREATE TABLE IF NOT EXISTS labels (
        path TEXT,
        date TEXT,
        status TEXT
    )
    """,
    "CREATE UNIQUE INDEX IF NOT EXISTS path_index ON labels (path)",
    "CREATE INDEX IF NOT EXISTS date_index ON labels (date)",
    "CREATE INDEX IF NOT EXISTS status_index ON labels (status)",
)


def get_arguments() -> argparse.Namespace:
    from .. import config

    parser = argparse.ArgumentParser(
        description="Add CSS data to CATCH.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    config.add_arguments(parser)
    parser.add_argument(
        "--db", default="harvest-css.db", help="harvester tracking database"
    )
    parser.add_argument(
        "-f",
        help="do not download a new file list, but use the provided file name",
    )
    args = parser.parse_args()
    config.with_args(args)
    return args


@contextmanager
def harvester_db(filename):
    db = sqlite3.connect(filename)
    try:
        for statement in DB_SETUP:
            db.execute(statement)
        yield db
        db.commit()
    finally:
        db.close()


def sync_list():
    """Check for a new file list and synchronize as needed.


    Returns
    -------
    listfile : str
        The name of the file.

    """

    logger = get_logger()
    local_filename = "css-file-list.txt.gz"
    sync = False

    if os.path.exists(local_filename):
        # file exists, check for an update
        last_sync = datetime.fromtimestamp(os.stat(local_filename).st_mtime)
        response = requests.head(LATEST_FILES)
        logger.info(
            "Previous file list downloaded %s", last_sync.strftime("%Y-%m-%d %H:%M")
        )
        try:
            file_date = response.headers["Last-Modified"]
            file_date = datetime(*email.utils.parsedate(file_date)[:6])
            logger.info(
                "Online file list dated %s", file_date.strftime("%Y-%m-%d %H:%M")
            )
            if last_sync < file_date:
                sync = True
                logger.info("New file list available.")
        except KeyError:
            pass
    else:
        # file does not exist, download new file
        sync = True

    if sync:
        with requests.get(LATEST_FILES, stream=True) as r:
            r.raise_for_status()
            with open(local_filename, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
            logger.info("Downloaded file list.")

            stat = os.stat(local_filename)
            file_date = Time(stat.st_mtime, format="unix")
            logger.info(f"  Size: {stat.st_size / 1048576:.2f} MiB")
            logger.info(f"  Last modified: {file_date.iso}")

            backup_file = local_filename.replace(
                ".txt.gz",
                "-" + file_date.isot[:16].replace("-", "").replace(":", "") + ".txt.gz",
            )
            os.system(f"cp {local_filename} {backup_file}")

    return local_filename


def read_label(path):
    logger = get_logger()
    url = "".join((ARCHIVE_PREFIX, path))

    attempts = 0
    # address timeout error by retrying with a delay
    while attempts < 4:
        try:
            label = pds4_read(url, lazy_load=True, quiet=True).label
            break
        except urllib.error.URLError as e:
            logger.error(str(e))
            attempts += 1
            sleep(1)  # retry, but not too soon
    else:
        raise e

    return label


def new_labels(db, listfile):
    """Iterator for new labels.


    Parameters
    ----------
    db : sqlite3.Connection
        Database of ingested labels (``harvester_db``).

    listfile : str
        Look for new labels in this file.

    Returns
    -------
    path : str

    """

    logger = get_logger()

    line_count: int = 0
    calibrated_count: int = 0
    processed_count: int = 0
    with gzip.open(listfile, "rt") as inf:
        for line in inf:
            line_count += 1
            if re.match(".*data_calibrated/.*\.xml\n$", line):
                if "collection" in line:
                    continue
                calibrated_count += 1
                path = line.strip()
                path = path[line.find("gbo.ast.catalina.survey") :]
                processed = db.execute(
                    "SELECT TRUE FROM labels WHERE path = ?", (path,)
                ).fetchone()
                if processed is None:
                    processed_count += 1
                    label = read_label(path)
                    yield path, label

    logger = logging.getLogger("add-css")
    logger.info("Processed:")
    logger.info("  %d lines", line_count)
    logger.info("  %d calibrated data labels", calibrated_count)
    logger.info("  %d new files", processed_count)


def main():
    from .. import config

    config.target = "catch"
    config.source = "atlas"

    args = get_arguments()
    logger = setup_logger()

    try:
        harvest_log = HarvestLog()
    except ConcurrentHarvesting:
        logger.error("Another process has locked the harvest log")
        sys.exit(1)

    if args.f is None:
        listfile = sync_list()
    else:
        listfile = args.f
        logger.info("Checking user-specified file list.")

    timestamp = Time(os.stat(listfile).st_mtime, format="unix").iso

    with harvester_db(args.db) as db:
        with Catch.with_config(args.config) as catch:
            harvest_log.data.add_row(
                {
                    "target": "catch",
                    "start": Time.now().iso,
                    "end": "processing",
                    "source": config.source,
                    "time_of_last": timestamp,
                    "files": 0,
                    "added": 0,
                    "errors": 0,
                }
            )
            harvest_log.write()

            observations = []
            failed = 0
            tri = ProgressTriangle(1, logger=logger, base=2)
            for path, label in new_labels(db, listfile):
                try:
                    observations.append(process(label, "css"))
                    msg = "added"
                except ValueError as e:
                    failed += 1
                    harvest_log[-1]["errored"] += 1
                    msg = str(e)
                except:
                    logger.error(
                        "A fatal error occurred processing %s",
                        path,
                        exc_info=True,
                    )
                    raise

                logger.debug("%s: %s", path, msg)
                tri.update()

                if args.dry_run:
                    continue

                db.execute(
                    "INSERT INTO labels VALUES (?,?,?)", (path, Time.now().iso, msg)
                )

                if len(observations) >= 10000:
                    catch.add_observations(catch, observations)
                    db.commit()

                    # update harvest log
                    harvest_log.data[-1]["files"] = tri.i
                    harvest_log.data[-1]["added"] += len(observations)
                    harvest_log.data[-1]["errors"] = failed
                    harvest_log.write()

                    observations = []

            # add any remaining files
            if len(observations) > 0:
                catch.add_observations(catch, observations)
                db.commit()

            if failed > 0:
                logger.error("Failed processing %d files", failed)

            # update harvest log
            harvest_log.data[-1]["files"] = tri.i
            harvest_log.data[-1]["end"] = Time.now().iso
            harvest_log.data[-1]["added"] += len(observations)
            harvest_log.data[-1]["errors"] = failed
            harvest_log.write()

            if not config.dry_run:
                logger.info("Updating survey statistics.")
                for source in (
                    "catalina_bigelow",
                    "catalina_lemmon",
                    "catalina_bokneosurvey",
                ):
                    catch.update_statistics(source=source)


if __name__ == "__main__":
    main()
