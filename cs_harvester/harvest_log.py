import os
import numpy as np
from astropy.table import Table
from astropy.time import Time
from .exceptions import ConcurrentHarvesting
from .logger import get_logger


class HarvestLog:
    """File that tracks harvest runs and results."""

    def __init__(self):
        self.read()

    def read(self) -> None:
        """Read log data from the disk.


        Raises ConcurrentHarvesting if "processing" is found in the log file's
        "end" column.

        """

        from . import config

        logger = get_logger()

        self.data: Table
        if os.path.exists(config.harvest_log_filename):
            self.data = Table.read(
                config.harvest_log_filename, format=config.harvest_log_format
            )
        else:
            self.data = Table(
                names=[
                    "target",
                    "start",
                    "end",
                    "source",
                    "time_of_last",
                    "files",
                    "added",
                    "duplicates",
                    "errors",
                ],
                dtype=["<U8", "<U23", "<U23", "<U32", "<U23", int, int, int],
            )

        if any(self.data["end"] == "processing"):
            logger.error('Harvester log state is "processing"')
            raise ConcurrentHarvesting()

    def write(self) -> None:
        """Write harvest log data to the disk.

        A backup file is made and the last 5 backups are kept.

        """

        from . import config

        if config.dry_run:
            return

        os.system(
            f"cp -f --backup=numbered {config.harvest_log_filename} {config.harvest_log_filename}"
        )

        sixth_backup = config.harvest_log_filename + ".~6~"
        if os.path.exists(sixth_backup):
            os.unlink(sixth_backup)

        self.data.write(
            config.harvest_log_filename,
            format=config.harvest_log_format,
            overwrite=True,
        )

    def time_of_last(self) -> Time:
        """Get the time of the last file validation for the current target and source."""

        from . import config

        target = self.data["target"] == config.harvest_target
        source = self.data["source"] == config.harvest_source

        if not any(target * source):
            return Time(0, format="jd")

        last_run = np.argsort(self.data[target * source]["end"])[-1]
        return Time(self.data[last_run]["time_of_last"])
