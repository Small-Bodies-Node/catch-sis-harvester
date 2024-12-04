import os
from glob import glob
from typing import Iterator
import pds4_tools
from .lidvid import LIDVID


def labels_from_inventory(
    inventory: list[str],
    files: list[str],
    error_if_incomplete: bool = False,
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
    iterator
        File name and label.

    """

    # yield all .xml labels with lidvids in the inventory
    remaining = set(inventory)
    for fn in files:
        label = pds4_tools.read(fn, quiet=True, lazy_load=True).label
        lidvid = str(LIDVID.from_label(label))
        if lidvid in remaining:
            remaining -= set([lidvid])
            yield fn, label

    if len(remaining) > 0:
        raise ValueError(
            "Not all inventory LIDVIDs were found: {}".format(", ".join(remaining))
        )


case_insensitive_find_file_lists: dict[str, dict[str, str]] = {}


def case_insensitive_find_xml_file(fn: str) -> str:
    """A case insensitive find file function.

    Helpful for finding file names with PDS4 LIDs.


    Parameters
    ----------
    fn : str
        The file name to find.


    Returns
    -------
    found : str

    """

    dir = os.path.dirname(fn)

    # only read the files once:
    if dir not in case_insensitive_find_file_lists:
        case_insensitive_find_file_lists[dir] = {
            _fn.lower(): _fn for _fn in glob(f"{dir}/*xml")
        }

    try:
        return case_insensitive_find_file_lists[dir][fn]
    except KeyError:
        raise FileNotFoundError(fn)
