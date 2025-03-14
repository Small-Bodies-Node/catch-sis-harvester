import os
from astropy.table import Table, Column
from astropy.time import Time
from .exceptions import ConcurrentHarvesting


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

        dtypes = ["<U16", "<U32", "<U32", "<U32", "<U32", int, int, int, int]
        self.data: Table
        if os.path.exists(config.harvest_log_filename):
            self.data = Table.read(
                config.harvest_log_filename, format=config.harvest_log_format
            )
            # enforce string field widths
            for col, dtype in zip(self.data.colnames[:5], dtypes[:5]):
                if self.data[col].dtype == dtype:
                    continue

                self.data.replace_column(col, Column(self.data[col].data, dtype=dtype))
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
                dtype=dtypes,
            )

        if any(self.data["end"] == "processing"):
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

        if len(self.data) == 0:
            return Time(0, format="jd")

        target = self.data["target"] == config.target
        source = self.data["source"] == config.source
        ingested = (self.data["time_of_last"] != "") * (
            self.data["time_of_last"] != "0"
        )

        if not any(target * source * ingested):
            return Time(0, format="jd", precision=6)

        last_good_run = self.data[target * source * ingested][-1]
        return Time(last_good_run["time_of_last"], precision=6)
