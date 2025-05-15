"""Harvest Spacewatch metadata from PSI.

This script reads in a collection label and inventory, then guesses the data web
directories (parsing the LIDs for date strings), downloads the index of each
directory, and parses all relevant XML files.

Label file names, two formats:
gbo.ast.spacewatch.survey/data/2003/03/23/sw_0993_09.01_2003_03_23_09_18_47.001.xml
gbo.ast.spacewatch.survey/data/2008/10/31/sw_1062_K03W25B_2008_10_31_12_00_52.005.xml
gbo.ast.spacewatch.survey/data/2003/07/08/sw_0996_SW403s_2003_07_08_08_40_33.001.xml

Can only be partially derived from the LIDs:
urn:nasa:pds:gbo.ast.spacewatch.survey:data:sw_0993_09.01_2003_03_23_09_18_47.001.fits
urn:nasa:pds:gbo.ast.spacewatch.survey:data:sw_1062_k03w25b_2008_10_31_12_00_52.005.fits
urn:nasa:pds:gbo.ast.spacewatch.survey:data:sw_0996_sw403s_2003_07_08_08_40_33.001.fits

The file name with the correct case is File_Area_Observational/File/file_name

And the LIDs may be found in the collection inventory:
gbo.ast.spacewatch.survey/data/collection_gbo.ast.spacewatch.survey_data_inventory.csv

"""

import os
import argparse
from urllib.parse import urljoin
import lxml.html

from sqlalchemy.orm.exc import NoResultFound
import pds4_tools

from catch import Catch, stats
from catch.model.spacewatch import Spacewatch
from sbsearch.logging import ProgressTriangle
from ..lidvid import LIDVID
from ..logger import setup_logger, get_logger
from ..collection import labels_from_inventory
from ..process import process
from .. import network

ARCHIVE_BASE_URL = "https://sbnarchive.psi.edu/pds4/surveys/gbo.ast.spacewatch.survey/"


def get_observation(catch, label) -> Spacewatch:
    lid = label.find("Identification_Area/logical_identifier").text
    obs = catch.db.session.query(Spacewatch).filter(Spacewatch.product_id == lid).one()
    return obs


def get_arguments():
    from .. import config

    parser = argparse.ArgumentParser(description="Add Spacewatch data to CATCH.")

    parser.add_argument(
        "--target",
        choices=("catch", "sbnsis"),
        action="append",
        required="true",
        help="target database; specify at least one",
    )

    config.add_arguments(parser)

    parser.add_argument(
        "collection",
        type=os.path.normpath,
        help="Spacewatch collection label",
    )
    parser.add_argument(
        "--update",
        action="store_true",
        help="update database with label metadata when there are conflicts",
    )
    parser.add_argument(
        "--vid",
        help="only process data products with this version ID",
    )
    parser.add_argument(
        "--diff",
        help="only process the differences with this, presumably older, collection",
    )

    args = parser.parse_args()

    config.with_args(args)

    return args


def get_inventory(args) -> list[str]:
    """Get a list of image LIDVIDs to be processed."""

    logger = get_logger()

    with network.set_astropy_useragent():
        collection = pds4_tools.read(args.collection, quiet=True, lazy_load=True)

    lidvid = LIDVID.from_label(collection.label)
    logger.info("Processing collection %s", lidvid)
    if args.vid is not None:
        logger.info("Only processing labels with version ID == %s", args.vid)

    lidvids = set(collection[0].data["LIDVID_LID"])
    if args.diff is not None:
        other_collection = pds4_tools.read(args.diff, quiet=True, lazy_load=True)
        other = set(other_collection[0].data["LIDVID_LID"])

        lidvids = list(lidvids - other)

    inventory = []
    for row in lidvids:
        lidvid = LIDVID(row)

        # must match requested version
        if args.vid is not None and lidvid.vid != args.vid:
            continue

        # must be in the data collection
        if not lidvid.lid.startswith("urn:nasa:pds:gbo.ast.spacewatch.survey:data:"):
            continue

        # must be an image file
        if not lidvid.lid.endswith(".fits"):
            continue

        inventory.append(row)

    logger.info("%d LIDS to check", len(inventory))

    return inventory


def get_labels(url, doc):
    """Find XML label URLs in this HTML document."""
    rows = doc.findall(".//table/tr")

    if len(rows) == 0:
        raise ValueError("got 0 table rows")

    labels = []
    for row in rows:
        if row[0].tag == "th":
            continue

        try:
            a = row[1][0]
        except IndexError:
            continue

        href = a.get("href")
        if href.endswith(".xml"):
            with network.set_astropy_useragent():
                label_url = urljoin(url, href)
                labels.append(pds4_tools.pds4_read(label_url))

    return labels


def process_date(inventory, date, targets):
    """Find and process all Spacewatch labels for date and in inventory."""

    logger = get_logger()

    # Find image products at the URL
    url = urljoin(ARCHIVE_BASE_URL, f"data/{date}/")
    logger.debug(
        "Inspecting URL %s for image labels",
        url,
    )
    with network.session() as req:
        logger.debug(url)
        response = req.get(url)
        response.raise_for_status()
        index = lxml.html.document_fromstring(response.content)

    labels = []
    for label in get_labels(url, index):
        lidvid = LIDVID.from_label(label)
        if str(lidvid) not in inventory:
            breakpoint()

        breakpoint()

    for target in targets:
        if target == "catch":
            add_to_catch(labels)
        elif target == "sbnsis":
            add_to_sbnsis(labels)


def add_to_catch(labels):
    from .. import config

    logger = get_logger()

    config.source = "catch"

    raise NotImplementedError

    def add_or_update(observations):
        try:
            if args.update:
                catch.update_observations(observations)
            else:
                catch.add_observations(observations)
        except Exception:
            logger.error(
                "A fatal error occurred saving data to the database.",
                exc_info=True,
            )
            raise

    with Catch.with_config(config.catch_config) as catch:
        observations = []
        failed = 0

        tri = ProgressTriangle(1, logger=logger, base=2)
        for fn, label in labels_from_inventory(inventory, files):
            tri.update()
            logger.debug("(%d) %s", tri.i, fn)

            obs = None
            if args.update:
                try:
                    obs = get_observation(catch, label)
                except NoResultFound:
                    # then just add it
                    pass

            try:
                obs = process(label, config.source, obs)
                observations.append(obs)
                msg = "updating" if args.update else "adding"
            except ValueError as e:
                failed += 1
                msg = str(e)
            except Exception:
                logger.error("A fatal error occurred processing %s", fn, exc_info=True)
                raise

            logger.debug("... %s", msg)

            if config.dry_run:
                continue

            if len(observations) >= 8192:
                add_or_update(observations)
                observations = []

        # add any remaining files
        if not config.dry_run and (len(observations) > 0):
            add_or_update(observations)

        logger.info("%d files processed.", tri.i)

        if failed > 0:
            logger.warning("Failed processing %d files", failed)

        if not args.dry_run:
            logger.info("Updating survey statistics.")
            stats.update_statistics(catch, source="spacewatch")


def add_to_sbnsis(labels):
    from .. import config

    logger = get_logger()

    config.source = "sbnsis"

    if not os.path.exists(".env"):
        raise FileNotFoundError("Missing sbnsis .env file")


def main():
    from .. import config

    config.source = "spacewatch"

    args = get_arguments()
    logger = setup_logger()

    inventory = get_inventory(args)

    # find unique dates
    dates = set()
    for row in inventory:
        lidvid = LIDVID(row)
        dates.add("/".join(lidvid.product_id.split("_")[-6:-3]))

    # process by date
    for date in dates:
        process_date(inventory, date, args.target)
