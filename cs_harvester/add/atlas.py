"""Harvest ATLAS metadata.

* ATLAS data will be continuously archived.
* We have a database that tracks validated collections, their locations, and the
  time they were validated.
* Check this database to identify new collections to harvest.
* Track harvest runs in a local file.
* Files are fz compressed.
* We just want labels with LIDs ending in .fits or .diff.

"""

import os
import sys
import sqlite3
import argparse
from glob import glob
from packaging.version import Version

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
import astropy.units as u
from astropy.time import Time
import pds4_tools
from pds4_tools.reader.general_objects import StructureList

from catch import Catch, stats
from sbsearch.logging import ProgressTriangle
from sbn_survey_image_service.data.add import add_label as add_label_to_sbnsis
from sbn_survey_image_service.services.database_provider import data_provider_session

from ..lidvid import LIDVID, collection_version
from ..logger import get_logger, setup_logger
from ..collection import labels_from_inventory
from ..harvest_log import HarvestLog
from ..exceptions import ConcurrentHarvesting
from ..process import process


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

    latest = None
    max_version = Version("0")
    for fn in files:
        struct = pds4_tools.read(fn, quiet=True, lazy_load=True)
        version = collection_version(struct.label)

        if version > max_version:
            latest = struct
            max_version = version

    return latest


def find_collection(location: str, night_number: int) -> StructureList:
    files = glob(f"/n/{location}/collection_{night_number}*.xml")
    return latest_collection(files)


def process_collection_for_catch(
    collection: StructureList,
    location: str,
    timestamp: str,
    harvest_log: HarvestLog,
):
    from .. import config

    logger = get_logger()

    lidvid = LIDVID.from_label(collection.label)

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
    for fn, label in labels:
        tri.update()
        try:
            observations.append(process(label, "atlas"))
        except Exception as exc:
            logger.error(": ".join((str(exc), fn)))
            errors += 1

    tri.done()

    if not config.dry_run:
        with Catch.with_config(config.catch_config) as catch:
            try:
                catch.add_observations(observations)
            except IntegrityError as exc:
                logger.error(exc)
                harvest_log.data[-1]["end"] = "failed"
                harvest_log.write()
                raise exc

            logger.info("Updating survey statistics.")
            for source in (
                "atlas_mauna_loa",
                "atlas_haleakela",
                "atlas_rio_hurtado",
                "atlas_sutherland",
            ):
                stats.update_statistics(catch, source=source)

    logger.info("%d files processed", tri.i)
    logger.info("%d files added", added)
    logger.info("%d files already in the database", duplicates)
    logger.info("%d files errored", errors)

    # update harvest log
    harvest_log.data[-1]["files"] += tri.i
    harvest_log.data[-1]["added"] += added
    harvest_log.data[-1]["duplicates"] += duplicates
    harvest_log.data[-1]["errors"] += errors
    harvest_log.data[-1]["time_of_last"] = max(
        harvest_log.data[-1]["time_of_last"],
        timestamp,
    )
    harvest_log.write()


def process_collection_for_sbnsis(
    collection: StructureList,
    location: str,
    timestamp: str,
    harvest_log: HarvestLog,
):
    from .. import config

    logger = get_logger()

    if not os.path.exists(".env"):
        raise FileNotFoundError("Missing sbnsis .env file")

    lidvid = LIDVID.from_label(collection.label)

    # Find image products in the data directory
    data_directory = os.path.normpath(f"/n/{location}/data")
    logger.debug(
        "Inspecting directory %s for image products",
        data_directory,
    )

    logger.info("%s, %s", lidvid, data_directory)

    # identify image labels of interest
    product_lidvids = [LIDVID(row) for row in collection[0].data["LIDVID_LID"]]
    candidate_labels = [
        str(lv) for lv in product_lidvids if lv.lid.endswith((".fits", ".diff"))
    ]
    try:
        labels = labels_from_inventory(
            candidate_labels,
            glob(f"{data_directory}/*fits.xml") + glob(f"{data_directory}/*diff.xml"),
            error_if_incomplete=True,
        )
    except ValueError as exc:
        logger.error(str(exc))
        raise exc

    # harvest metadata
    added = 0
    duplicates = 0
    errors = 0
    tri: ProgressTriangle = ProgressTriangle(1, logger)
    with data_provider_session() as sbnsis:
        for fn, label in labels:
            tri.update()
            try:
                success = add_label_to_sbnsis(fn, sbnsis, dry_run=config.dry_run)
                added += success
                duplicates += not success
            except Exception as exc:
                logger.error(": ".join((str(exc), fn)))
                errors += 1

    tri.done()

    logger.info("%d files processed", tri.i)
    logger.info("%d files added", added)
    logger.info("%d files already in the database", duplicates)
    logger.info("%d files errored", errors)

    # update harvest log
    harvest_log.data[-1]["files"] += tri.i
    harvest_log.data[-1]["added"] += added
    harvest_log.data[-1]["duplicates"] += duplicates
    harvest_log.data[-1]["errors"] += errors
    harvest_log.data[-1]["time_of_last"] = max(
        harvest_log.data[-1]["time_of_last"],
        timestamp,
    )
    harvest_log.write()


def time_string_or_float(t: str) -> Time:
    """Parse a command-line argument as a calendar date or UNIX timestamp."""
    try:
        return Time(float(t), format="unix")
    except (ValueError):
        return Time(t)


def get_arguments():
    from .. import config

    parser = argparse.ArgumentParser(
        description=(
            "The default behavior is to find collections validated since the "
            "time the last ingested collection was recorded in the database.  "
        ),
        epilog="Date parameter formats are YYYY-MM-DD HH:MM:SS.SSS or Unix timestamp.",
    )

    parser.add_argument("target", choices=("catch", "sbnsis"))

    config.add_arguments(parser)
    parser.add_argument(
        "file",
        type=os.path.normpath,
        help="ATLAS-PDS processing database",
    )
    mutex = parser.add_mutually_exclusive_group()
    mutex.add_argument(
        "--since",
        type=time_string_or_float,
        help="harvest metadata validated since this date and time",
    )
    mutex.add_argument(
        "--past",
        type=int,
        help="harvest metadata validated in the past SINCE hours",
    )
    parser.add_argument(
        "--before",
        type=time_string_or_float,
        default=Time.now(),
        help="harvest metadata validated before this date and time",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="only list the collections that would be ingested",
    )

    args = parser.parse_args()

    config.with_args(args)

    return args


def main():
    from .. import config

    args = get_arguments()

    config.target = args.target
    config.source = "atlas"

    logger = setup_logger()
    validation_db = open_validation_database(args.file)

    try:
        harvest_log = HarvestLog()
    except ConcurrentHarvesting:
        logger.error("Another process has locked the harvest log")
        sys.exit(1)

    now = Time.now()
    now.precision = 6
    harvest_log.data.add_row(
        {
            "target": config.target,
            "start": now.iso,
            "end": "processing",
            "source": config.source,
            "time_of_last": "",
            "files": 0,
            "added": 0,
            "errors": 0,
        }
    )

    since: Time = args.since
    if args.since is None:
        since = harvest_log.time_of_last()
    since.precision = 6

    before: Time = args.before
    before.precision = 6

    if args.past is None:
        logger.info(
            "Checking for collections validated between %s and %s",
            since.iso,
            before.iso,
        )
    else:
        since = Time.now() - args.past * u.hr
        since.precision = 6
        logger.info(
            "Checking for collections validated in the past %d hr (since %s)",
            args.past,
            since.iso,
        )

    results = validated_collections_from_db(validation_db, since, before)
    validation_db.close()

    if len(results) == 0:
        now = Time.now()
        now.precision = 6
        harvest_log.data[-1]["end"] = now.iso
        harvest_log.write()

        logger.info("No new data collections found")
        logger.info("Finished")
        return

    if args.list:
        logger.info("Listing %d collections to process", len(results))
        for i, row in enumerate(results):
            collection = find_collection(row["location"], row["nn"])
            print(LIDVID.from_label(collection.label))

        logger.info("Finished")
        return

    harvest_log.write()  # write "processing" state to the log

    for i, row in enumerate(results):
        collection = find_collection(row["location"], row["nn"])
        n = len(results) - i

        lidvid = LIDVID.from_label(collection.label)
        if config.only_process is not None:
            if lidvid in config.only_process:
                # use list of remaining lidvids from command line as the count
                n = len(config.only_process)
            else:
                continue

        logger.info("%d collections to process.", n)

        if config.target == "catch":
            process_collection_for_catch(
                collection,
                row["location"],
                Time(row["recorded_at"], format="unix", precision=6).iso,
                harvest_log,
            )
        else:
            process_collection_for_sbnsis(
                collection,
                row["location"],
                Time(row["recorded_at"], format="unix", precision=6).iso,
                harvest_log,
            )

        if config.only_process is not None:
            config.only_process.pop(config.only_process.index(lidvid))
            if len(config.only_process) == 0:
                break

    logger.info("Processing complete.")
    logger.info("%d files processed", harvest_log.data[-1]["files"])
    logger.info("%d files added", harvest_log.data[-1]["added"])
    logger.info("%d files already in the database", harvest_log.data[-1]["duplicates"])
    logger.info("%d files errored", harvest_log.data[-1]["errors"])

    now = Time.now()
    now.precision = 6
    harvest_log.data[-1]["end"] = now.iso
    harvest_log.write()

    logger.info("Finished")
