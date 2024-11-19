"""
Get metadata from a label.
"""

import numpy as np
from astropy.time import Time
from astropy.coordinates import SkyCoord
from pds4_tools.reader.label_objects import Label

from catch.model import Observation
from catch.model.atlas import (
    ATLAS,
    ATLASMaunaLoa,
    ATLASHaleakela,
    ATLASRioHurtado,
    ATLASSutherland,
)
from .lidvid import LIDVID


def process(label: Label):
    """Get common metadata from a PDS4 label.


    Parameters
    ----------
    label : pds4_tools.reader.label_objects.Label
        The label to process.


    Returns
    -------
    obs : Observation
        The observation metadata.  If the label is that of a survey known to
        CATCH, then the specific observational model object will be used, e.g.,
        ``ATLASHaleakela``.

    """

    lidvid = LIDVID(label)

    # use the label to determine which data model object to use
    cls: Observation = Observation
    if lidvid.bundle == "gbo.ast.atlas.survey":
        # example LID: urn:nasa:pds:gbo.ast.atlas.survey:59613:01a59613o0586o_fits
        tel = lid.product_id[:2]
        cls = {
            "01": ATLASMaunaLoa,
            "02": ATLASHaleakela,
            "03": ATLASSutherland,
            "04": ATLASRioHurtado,
        }[tel]

    obs = cls()
    obs.product_id = str(lidvid.lid)
    obs.mjd_start = Time(
        label.find("Observation_Area/Time_Coordinates/start_date_time").text
    ).mjd
    obs.mjd_stop = Time(
        label.find("Observation_Area/Time_Coordinates/stop_date_time").text
    ).mjd
    obs.exposure = float(label.find(".//img:Exposure/img:exposure_duration").text)
    obs.filter = label.find(".//img:Optical_Filter/img:filter_name").text

    survey = label.find(".//survey:Survey")
    ra, dec = [], []
    for corner in ("Top Left", "Top Right", "Bottom Right", "Bottom Left"):
        coordinate = survey.find(
            "survey:Image_Corners"
            f"/survey:Corner_Position[survey:corner_identification='{corner}']"
            "/survey:Coordinate"
        )
        ra.append(float(coordinate.find("survey:right_ascension").text))
        dec.append(float(coordinate.find("survey:declination").text))
    obs.set_fov(ra, dec)

    # Need to account for other maglimit types
    maglimit = survey.find(".//survey:N_Sigma_Limit/survey:limiting_magnitude")
    if maglimit is not None:
        obs.maglimit = float(maglimit.text)

    # survey specific sections here
    if isinstance(obs, ATLAS):
        obs.field_id = survey.find("survey:field_id").text

        # is there a diff image?
        derived_lids = label.findall(
            "Reference_List/Internal_Reference[reference_type='data_to_derived_product']/lid_reference"
        )
        expected_diff_lid = lidvid.lid[:-4] + "diff"  # replace fits with diff
        obs.diff = any(
            [derived_lid.text == expected_diff_lid for derived_lid in derived_lids]
        )

    return obs
