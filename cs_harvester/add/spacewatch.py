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
import sys
import argparse
from tempfile import TemporaryDirectory
from urllib.parse import urljoin
import lxml.html

from astropy.time import Time
import pds4_tools

from sbsearch.logging import ProgressTriangle
from sbn_survey_image_service.data.add import add_label
from sbn_survey_image_service.services.database_provider import data_provider_session

from .. import network
from ..exceptions import ConcurrentHarvesting
from ..harvest_log import HarvestLog
from ..lidvid import LIDVID
from ..logger import setup_logger, get_logger

ARCHIVE_BASE_URL = "https://sbnarchive.psi.edu/pds4/surveys/gbo.ast.spacewatch.survey/"


def get_arguments():
    from .. import config

    parser = argparse.ArgumentParser(description="Harvest Spacewatch metadata.")

    parser.add_argument(
        "--target",
        choices=("sbnsis"),
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


def get_labels(url: str, doc: lxml.html.HtmlElement, path: str) -> list[str]:
    """Download all XML label URLs linked in this HTML document's table.


    Parameters
    ----------

    url : str
        The base URL for the label locations.

    doc : lxml.html.HtmlElement
        The document that contains links to the labels.  It is assumed that the
        labels are in the second column of a table and that the anchor tag is
        the first child of the table cell.

    path : str
        The local directory to which to save the labels.


    Returns
    -------
    label_files : list[str]
        The full path to the downloaded label files.

    """

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
            label_url = urljoin(url, href)
            fn = network.download_file(label_url)
            os.rename(fn, os.path.join(path, os.path.basename(fn)))
            labels.append(fn)

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

    with TemporaryDirectory() as tempd:
        files = []
        for fn in get_labels(url, index, tempd):
            lidvid = LIDVID.from_label(pds4_tools.pds4_read(fn))
            if str(lidvid) in inventory:
                files.append(fn)
            else:
                logger.debug(f"Skipping {str(lidvid)}")

        for target in targets:
            if target == "sbnsis":
                add_to_sbnsis(files)


def add_to_sbnsis(files):
    from .. import config

    logger = get_logger()

    config.source = "sbnsis"

    if not os.path.exists(".env"):
        raise FileNotFoundError("Missing sbnsis .env file")

    try:
        harvest_log = HarvestLog()
    except ConcurrentHarvesting:
        logger.error("Another process has locked the harvest log")
        sys.exit(1)

    # harvest metadata
    added = 0
    duplicates = 0
    errors = 0
    tri: ProgressTriangle = ProgressTriangle(1, logger)
    with data_provider_session() as sbnsis:
        for fn in files:
            tri.update()
            try:
                success = add_label(fn, sbnsis, dry_run=config.dry_run)
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
        Time.now().iso,
    )
    harvest_log.write()


def main():
    from .. import config

    config.source = "spacewatch"

    args = get_arguments()
    setup_logger()

    inventory = get_inventory(args)

    # find unique dates
    dates = set()
    for row in inventory:
        lidvid = LIDVID(row)
        dates.add("/".join(lidvid.product_id.split("_")[-6:-3]))

    # process by date
    for date in dates:
        process_date(inventory, date, args.target)
