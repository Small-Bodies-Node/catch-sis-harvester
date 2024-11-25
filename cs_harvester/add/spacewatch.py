"""Harvest Spacewatch metadata from PSI.

Label file names, two formats:
gbo.ast.spacewatch.survey/data/2003/03/23/sw_0993_09.01_2003_03_23_09_18_47.001.xml
gbo.ast.spacewatch.survey/data/2008/10/31/sw_1062_K03W25B_2008_10_31_12_00_52.005.xml

Can be derived from the LIDs:
urn:nasa:pds:gbo.ast.spacewatch.survey:data:sw_0993_09.01_2003_03_23_09_18_47.001.fits
urn:nasa:pds:gbo.ast.spacewatch.survey:data:sw_1062_k03w25b_2008_10_31_12_00_52.005.fits

And the LIDs may be found in the collection inventory:
gbo.ast.spacewatch.survey/data/collection_gbo.ast.spacewatch.survey_data_inventory.csv

Download all data labels and the collection inventory to a local directory:

wget -r -R *.fits --no-parent https://sbnarchive.psi.edu/pds4/surveys/gbo.ast.spacewatch.survey/data/

"""

import os
import argparse
from glob import iglob

import pds4_tools

from catch import Catch
from catch.model.spacewatch import Spacewatch
from sbsearch.logging import ProgressTriangle
from ..lidvid import LIDVID
from ..logger import get_logger, setup_logger
from ..collection import labels_from_inventory
from ..process import process


def get_observation(catch, label) -> Spacewatch:
    lid = label.find("Identification_Area/logical_identifier").text
    obs = catch.db.session.query(Spacewatch).filter(Spacewatch.product_id == lid).one()
    return obs


def get_arguments():
    from .. import config

    parser = argparse.ArgumentParser(description="Add Spacewatch data to CATCH.")

    config.add_arguments(parser)
    parser.add_argument(
        "collection",
        type=os.path.normpath,
        help="Spacewatch collection label",
    )
    parser.add_argument(
        "--update",
        action="store_true",
        help="update database with label metadata (potentially very slow)",
    )
    parser.add_argument(
        "--vid",
        help="only process data products with this version ID",
    )

    args = parser.parse_args()

    config.target = "catch"
    config.source = "spacewatch"
    config.with_args(args)

    return args


def main():
    from .. import config

    args = get_arguments()
    logger = setup_logger()

    collection = pds4_tools.read(args.collection, quiet=True, lazy_load=True)
    lidvid = LIDVID.from_label(collection.label)
    logger.info("Processing collection %s", lidvid)
    if args.vid is not None:
        logger.info("Only processing labels with version ID == %s", args.vid)

    base_path = os.path.dirname(args.collection)

    inventory = []
    files = set()
    for row in collection[0].data["LIDVID_LID"]:
        lidvid = LIDVID(row)
        if args.vid is not None and lidvid.vid != args.vid:
            continue

        inventory.append(row)

        y, m, d = lidvid.product_id.split("_")[3:6]
        files.add(
            f"{base_path}/gbo.ast.spacewatch.survey/data/{y}/{m}/{d}/{lidvid.lid[:-4]}xml"
        )

    def add_or_update(observations):
        try:
            if args.update:
                catch.update_observations(observations)
            else:
                catch.add_observations(observations)
        except:
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

            if args.update:
                obs = get_observation(catch, label)
            else:
                obs = None

            try:
                obs = process(label, obs)
                observations.append(process(label, obs))
                msg = "updating" if args.update else "adding"
            except ValueError as e:
                failed += 1
                msg = str(e)
            except:
                logger.error("A fatal error occurred processing %s", fn, exc_info=True)
                raise

            logger.debug("%s: %s", msg, fn)

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

        if not (args.dry_run or args.t):
            logger.info("Updating survey statistics.")
            catch.update_statistics(source="spacewatch")
            logger.info("Consider database vacuum.")
