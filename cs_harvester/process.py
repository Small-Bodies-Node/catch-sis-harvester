"""
Get metadata from a label.
"""

import numpy as np
from astropy.time import Time
from pds4_tools.reader.label_objects import Label

from catch.model import Observation, Spacewatch
from catch.model.atlas import (
    ATLAS,
    ATLASMaunaLoa,
    ATLASHaleakela,
    ATLASRioHurtado,
    ATLASSutherland,
)
from catch.model.catalina import (
    CatalinaSkySurvey,
    CatalinaBigelow,
    CatalinaLemmon,
    CatalinaBokNEOSurvey,
)
from .lidvid import LIDVID


def process(label: Label, source: str, update: Observation | None = None):
    """Get common metadata from a PDS4 label.


    Parameters
    ----------
    label : pds4_tools.reader.label_objects.Label
        The label to process.

    source : str
        The expected data source for this label.

    update : Observation, optional
        If defined, then this observation is updated and returned.


    Returns
    -------
    obs : Observation
        The observation metadata.  If the label is that of a survey known to
        CATCH, then the specific observational model object will be used, e.g.,
        ``ATLASHaleakela``.

    """

    lidvid = LIDVID.from_label(label)

    # use the label to determine which data model object to use
    cls: Observation = Observation
    if lidvid.bundle.startswith("gbo.ast.atlas.survey"):
        # example LID: urn:nasa:pds:gbo.ast.atlas.survey:59613:01a59613o0586o_fits
        tel = lidvid.product_id[:2]
        cls = {
            "01": ATLASMaunaLoa,
            "02": ATLASHaleakela,
            "03": ATLASSutherland,
            "04": ATLASRioHurtado,
        }[tel]
    elif lidvid.bundle.startswith("gbo.ast.spacewatch.survey"):
        cls = Spacewatch
    elif lidvid.bundle.startswith("gbo.ast.catalina.survey"):
        tel = lidvid.product_id[:3].upper()
        if tel in CatalinaBigelow._telescopes:
            cls = CatalinaBigelow
        elif tel in CatalinaLemmon._telescopes:
            cls = CatalinaLemmon
        elif tel in CatalinaBokNEOSurvey._telescopes:
            cls = CatalinaBokNEOSurvey

    if update is not None:
        obs = update
    else:
        obs = cls()

    # verify observation model
    if source == "atlas" and not isinstance(obs, ATLAS):
        raise ValueError("Expected an ATLAS label")
    elif source == "spacewatch" and not isinstance(obs, Spacewatch):
        raise ValueError("Expected a Spacewatch label")
    elif source == "css" and not isinstance(obs, CatalinaSkySurvey):
        raise ValueError("Expected a Catalina Sky Survey label")

    obs.product_id = str(lidvid.lid)
    obs.mjd_start = Time(
        label.find("Observation_Area/Time_Coordinates/start_date_time").text
    ).mjd
    obs.mjd_stop = Time(
        label.find("Observation_Area/Time_Coordinates/stop_date_time").text
    ).mjd

    exposure = label.find(".//img:Exposure/img:exposure_duration")
    if exposure is None:
        obs.exposure = round((obs.mjd_stop - obs.mjd_start) * 86400, 3)
    else:
        obs.exposure = float(exposure.text)

    filter = label.find(".//img:Optical_Filter/img:filter_name")
    obs.filter = None if filter is None else filter.text

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

    maglimit = survey.find(".//survey:N_Sigma_Limit/survey:limiting_magnitude")
    if maglimit is not None:
        obs.maglimit = float(maglimit.text)

    maglimit = survey.find(".//survey:Rollover/survey:rollover_magnitude")
    if maglimit is not None:
        obs.maglimit = float(maglimit.text)

    maglimit = survey.find(
        "survey:Limiting_Magnitudes"
        "/survey:Percentage_Limit[survey:Percentage_Limit='50']"
        "/survey:limiting_magnitude"
    )
    if maglimit is not None:
        obs.maglimit = float(maglimit.text)

    obs.mjd_added = Time.now().mjd

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
    elif isinstance(obs, Spacewatch):
        obs.file_name = label.find(".//File_Area_Observational/File/file_name").text

    return obs
