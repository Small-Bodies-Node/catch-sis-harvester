"""Harvest ATLAS metadata.

* ATLAS data will be continuously archived.
* We have a database that tracks validated collections, their locations, and the
  time they were validated.
* Check this database to identify new collections to harvest.
* Track harvest runs in a local file.
* Files are fz compressed.
* We just want labels with LIDs ending in .fits.

"""

import os
import sys
import sqlite3
import argparse
from glob import glob
from typing import Iterator
from packaging.version import Version

import numpy as np
import astropy.units as u
from astropy.time import Time
from astropy.table import Table
import pds4_tools

from catch import Catch
from sbsearch.logging import ProgressTriangle
from ...exceptions import LabelError
from ...lidvid import LIDVID, collection_version
from ...logger import get_logger
from ...collection import labels_from_inventory
from ...harvest_log import HarvestLog
from ...exceptions import ConcurrentHarvesting
from ...process import process


def open_validation_database(fn) -> sqlite3.Connection:
    logger = get_logger()

    try:
        db = sqlite3.connect(f"file:{fn}?mode=ro", uri=True)
        db.row_factory = sqlite3.Row
    except Exception as exc:
        logger.error("Could not connect to database %s", fn)
        raise exc

    return db


def validated_collections_from_db(db, start, stop):
    """Get collections validated between start and stop.

    The rows are ordered so that if a fatal error occurs the next run might be
    able to recover.

    """

    cursor = db.execute(
        """SELECT * FROM nn
           WHERE current_status = 'validated'
             AND recorded_at > ? AND recorded_at < ?
           ORDER BY recorded_at
        """,
        (start.unix, stop.unix),
    )
    return list(cursor.fetchall())


def latest_collection(files):
    """Search list of xml files for collections and return the highest versioned collection."""

    labels = [pds4_tools.read(fn, quiet=True, lazy_load=True) for fn in files]

    versions = []
    for label in labels:
        try:
            versions.append(collection_version(label))
        except LabelError:
            pass

    return labels[versions.index(max(versions))]


def process_collection(
    location: str,
    night_number: int,
    timestamp: str,
    harvest_log: HarvestLog,
    catch: Catch,
    only_process: str | None,
):
    from ... import config

    logger = get_logger()

    files = glob(f"/n/{location}/collection_{night_number}*.xml")
    collection = latest_collection(files)
    lidvid = LIDVID(collection.label)

    if only_process is not None and lid != only_process:
        return

    # Find image products in the data directory
    data_directory = os.path.normpath(f"/n/{location}/data")
    logger.debug(
        "Inspecting directory %s for image products",
        data_directory,
    )

    logger.info("%s, %s", lidvid, data_directory)

    # identify image labels of interest
    candidate_labels = [
        _lidvid
        for _lidvid in collection[0].data["LIDVID_LID"]
        if _lidvid[: _lidvid.rindex(":") - 1].endswith(".fits")
    ]
    try:
        labels = labels_from_inventory(
            candidate_labels,
            glob(f"{data_directory}/*fits.xml"),
            error_if_incomplete=True,
        )
    except ValueError as exc:
        logger.error(str(exc))
        raise exc

    # harvest metadata
    added = 0
    duplicates = 0
    errors = 0
    observations = []
    tri: ProgressTriangle = ProgressTriangle(1, logger)
    for label in labels:
        tri.update()
        try:
            observations.append(process(label))
            added += 1
        except Exception as exc:
            logger.error(exc)
            errors += 1

        if not config.dry_run:
            if config.update:
                catch.update_observations(observations)
            else:
                catch.add_observations(observations)

    logger.info("%d files processed", tri.i)
    logger.info("%d files added", added)
    logger.info("%d files already in the database", added)
    logger.info("%d files errored", errors)
    tri.done()

    # update harvest log
    harvest_log.data[-1]["files"] += tri.i
    harvest_log.data[-1]["added"] += added
    harvest_log.data[-1]["duplicates"] += duplicates
    harvest_log.data[-1]["errors"] += errors
    harvest_log.data[-1]["time_of_last"] = max(
        harvest_log[-1]["time_of_last"],
        timestamp,
    )
    harvest_log.write()


def add_arguments(parser):
    parser.add_argument(
        "file",
        type=os.path.normpath,
        help="ATLAS-PDS processing database or PDS4 label to test (with --test)",
    )
    mutex = parser.add_mutually_exclusive_group()
    mutex.add_argument(
        "--since-date", type=Time, help="harvest metadata validated since this date"
    )
    mutex.add_argument(
        "--past",
        type=int,
        help="harvest metadata validated in the past SINCE hours",
    )
    mutex.add_argument(
        "--between-dates",
        type=Time,
        nargs=2,
        help="harvest metadata validated between these dates",
    )
    return parser


def main(args):
    from ... import config

    logger = get_logger()

    validation_db = open_validation_database(args.file)

    try:
        harvest_log = HarvestLog()
    except ConcurrentHarvesting():
        sys.exit(1)

    start: Time
    stop: Time = Time.now().iso
    if args.between_dates is not None:
        start = args.between_dates[0]
        stop = args.between_dates[-1]
        logger.info(
            "Checking for collections validated between %s and %s", start.iso, stop.iso
        )
    elif args.past is not None:
        start = Time.now() - args.past * u.hr
        logger.info(
            "Checking for collections validated in the past %d hr (since %s)",
            args.past,
            start.iso,
        )
    elif args.since_date:
        start = args.since_date
        logger.info("Checking for collections validated since %s", start.iso)
    else:
        start = harvest_log.time_of_last()

    results = validated_collections_from_db(validation_db, start, stop)
    validation_db.close()

    if len(results) == 0:
        logger.info("No new data collections found.")
        return

    with Catch.with_config(config.catch_config) as catch:
        harvest_log.add_row(
            {
                "start": Time.now().iso,
                "end": "processing",
                "source": config.harvest_source,
                "time_of_last": "",
                "files": 0,
                "added": 0,
                "errors": 0,
            }
        )
        harvest_log.write()

        for i, row in enumerate(results):
            logger.info("%d collections to process.", len(results) - i)
            process_collection(
                row["location"],
                row["nn"],
                Time(row["recorded_at"], format="unix").iso,
                catch,
                harvest_log,
                args.only_process,
            )

        logger.info("Processing complete.")
        logger.info("%d files processed", harvest_log[-1]["files"])
        logger.info("%d files added", harvest_log[-1]["added"])
        logger.info("%d files already in the database", harvest_log[-1]["duplicates"])
        logger.info("%d files errored", harvest_log[-1]["errors"])

        harvest_log[-1]["end"] = Time.now().iso
        harvest_log.write()

        if not config.dry_run:
            logger.info("Updating survey statistics.")
            for source in (
                "atlas_mauna_loa",
                "atlas_haleakela",
                "atlas_rio_hurtado",
                "atlas_sutherland",
            ):
                catch.update_statistics(source=source)

    logger.info("Finished.")
