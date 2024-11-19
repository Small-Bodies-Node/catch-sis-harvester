from glob import glob
from typing import Iterator
import pds4_tools
from .logger import get_logger
from .lidvid import LIDVID


def labels_from_inventory(
    inventory: list[str], files: list[str], error_if_incomplete: bool = False
) -> Iterator:
    """Iterator of labels from a collection inventory.


    Parameters
    ----------
    inventory : list of str
        List of LIDVIDs of interest.

    files : str
        A list of label file names to search for the LIDVIDs.

    error_if_incomplete : bool, optional
        Set to ``True`` and ``ValueError`` will be raised if any LIDVIDs from
        the inventory are not found in the file list.


    Returns
    -------
    iterator of Label


    """

    # yield all .xml labels with lidvids in the inventory
    remaining = set(inventory)
    for fn in files:
        label = pds4_tools.read(fn, quiet=True, lazy_load=True).label
        lidvid = str(LIDVID(label))
        if lidvid in remaining:
            remaining -= set([lidvid])
            yield label

    if len(remaining) > 0:
        raise ValueError(
            "Not all inventory LIDVIDs were found: {}".format(", ".join(remaining))
        )
