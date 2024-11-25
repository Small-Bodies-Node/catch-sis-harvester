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
import logging
from glob import iglob

from astropy.time import Time
import pds4_tools

from catch import Catch, Config
from catch.model.spacewatch import Spacewatch
from sbsearch.logging import ProgressTriangle
from ..lidvid import LIDVID, collection_version
from ..logger import get_logger, setup_logger
from ..collection import labels_from_inventory
from ..process import process


def inventory(base_path, vid=None):
    """Iterate over all files of interest.

    Returns
    -------
    labels : iterator of tuples
        Path and pds4_tools label object.

    """

    logger = logging.getLogger("add-spacewatch")
    inventory_fn = f"{base_path}/gbo.ast.spacewatch.survey/data/collection_gbo.ast.spacewatch.survey_data_inventory.csv"

    if not os.path.exists(base_path):
        raise Exception("Missing inventory list %s", fn)

    # Read in all relevant LIDs from the inventory.
    lids = set()
    with open(inventory_fn, "r") as inf:
        for line in inf:
            if not line.startswith("P,urn:nasa:pds:gbo.ast.spacewatch.survey:data:sw_"):
                continue
            if ".fits" not in line:
                continue

            if vid is not None and not line[:-1].endswith(vid):
                continue

            lid = line[2:-6]
            lids.add(lid)

    # search directory-by-directory for labels with those LIDs
    for fn in iglob(f"{base_path}/gbo.ast.spacewatch.survey/data/20*/*/*/*.xml"):
        label = pds4_read(fn, lazy_load=True, quiet=True).label
        lid = label.find("Identification_Area/logical_identifier").text
        if lid in lids:
            lids.remove(lid)
            yield fn, label

    # did we find all the labels?
    if len(lids) > 0:
        logger.error(f"{len(lids)} LIDs were not found.")


def get_observation(catch, label) -> Spacewatch:
    lid = label.find("Identification_Area/logical_identifier").text
    obs = catch.db.session.query(Spacewatch).filter(Spacewatch.product_id == lid).one()
    return obs


# def process(fn, label, obs=None):
#     """If obs is defined, then it is updated and returned."""
#     lid = label.find("Identification_Area/logical_identifier").text
#     if obs is None:
#         obs = Spacewatch()

#     obs.product_id = lid
#     obs.mjd_start = Time(
#         label.find("Observation_Area/Time_Coordinates/start_date_time").text
#     ).mjd
#     obs.mjd_stop = Time(
#         label.find("Observation_Area/Time_Coordinates/stop_date_time").text
#     ).mjd
#     obs.exposure = float(label.find(".//img:Exposure/img:exposure_duration").text)
#     obs.filter = label.find(".//img:Optical_Filter/img:filter_name").text
#     obs.label = fn[fn.index("gbo.ast.spacewatch.survey") :]

#     survey = label.find(".//survey:Survey")
#     ra, dec = [], []
#     for corner in ("Top Left", "Top Right", "Bottom Right", "Bottom Left"):
#         coordinate = survey.find(
#             "survey:Image_Corners"
#             f"/survey:Corner_Position[survey:corner_identification='{corner}']"
#             "/survey:Coordinate"
#         )
#         ra.append(float(coordinate.find("survey:right_ascension").text))
#         dec.append(float(coordinate.find("survey:declination").text))
#     obs.set_fov(ra, dec)

#     maglimit = survey.find(".//survey:Rollover/survey:rollover_magnitude")
#     if maglimit is not None:
#         obs.maglimit = float(maglimit.text)

#     return obs


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

    inventory = []
    for lidvid in collection[0].data["LIDVID_LID"]:
        if args.vid is not None and lidvid.vid != args.vid:
            continue
        inventory.append(lidvid)

    base_path = os.path.dirname(args.collection)
    files = set(iglob(f"{base_path}/gbo.ast.spacewatch.survey/data/20*/*/*/*.xml"))

    with Catch.with_config(args.config) as catch:
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
                obs.label = fn[fn.index("gbo.ast.spacewatch.survey") :]
                observations.append(process(label, obs))
                msg = "updating" if args.update else "adding"
            except ValueError as e:
                failed += 1
                msg = str(e)
            except:
                logger.error("A fatal error occurred processing %s", fn, exc_info=True)
                raise

            logger.debug("%s: %s", msg, fn)

            if args.dry_run or args.t:
                continue

            if len(observations) >= 8192:
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
                observations = []

        # add any remaining files
        if not (args.dry_run or args.t) and (len(observations) > 0):
            try:
                if args.update:
                    catch.update_observations(observations)
                else:
                    catch.add_observations(observations)
            except:
                logger.error(
                    "A fatal error occurred saving data to the database.", exc_info=True
                )
                raise

        logger.info("%d files processed.", tri.i)

        if failed > 0:
            logger.warning("Failed processing %d files", failed)

        if not (args.dry_run or args.t):
            logger.info("Updating survey statistics.")
            catch.update_statistics(source="spacewatch")
            logger.info("Consider database vacuum.")
