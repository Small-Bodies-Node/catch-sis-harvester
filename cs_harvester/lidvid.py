from typing import Self
from packaging import Version
from pds4_tools.reader.label_objects import Label
from .exceptions import LabelError


class LIDVID:
    """PDS4 logical identifier and version identifier parser."""

    def __init__(self, lid: str, vid: str) -> None:
        self._lid = str(lid)
        self._vid = str(vid)
        if not self._lid.startswith("urn:nasa:pds"):
            raise ValueError(f"Invalid PDS4 LID: {lid}")

    @classmethod
    def from_label(cls, label: Label) -> Self:
        """Initialize from a PDS4 tools label object."""
        lid = label.find("Identification_Area/logical_identifier").text
        vid = label.find("Identification_Area/version_identifier").text
        return cls(lid, vid)

    def __str__(self) -> str:
        return "::".join(self._lid, self._vid)

    def __repr__(self) -> str:
        return f"<LIDVID: {str(self)}>"

    @property
    def logical_id(self) -> str:
        return self._lid

    @property
    def version_id(self) -> str:
        return self._vid

    lid = logical_id
    vid = version_id

    @property
    def bundle(self) -> str:
        return self._lid.split(":")[3]

    @property
    def collection(self) -> str:
        return self._lid.split(":")[4]

    @property
    def product_id(self) -> str:
        return self._lid.split(":")[5]


def collection_version(label: Label) -> Version:
    """Get the collection version."""
    is_collection = (
        label.find("Identification_Area/product_class").text == "Product_Collection"
    )
    if not is_collection:
        raise LabelError("This does not appear to be a collection label.")
    return Version(LIDVID(label).vid)
