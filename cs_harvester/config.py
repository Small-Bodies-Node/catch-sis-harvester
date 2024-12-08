from argparse import ArgumentParser
from catch.config import Config as CatchConfig
from .lidvid import LIDVID


harvest_log_filename: str = "harvest-log.ecsv"
harvest_log_format: str = "ascii.ecsv"
target: str = ""
source: str = ""
logger_name: str = "CATCH/SIS Harvester"
verbose: bool = False
dry_run: bool = False
catch_config: CatchConfig = CatchConfig()
runtime_log_filename: str = ""
only_process: list[str] | None = None


def add_arguments(parser):
    """Add global configuration arguments to the argument parser."""

    parser.add_argument(
        "--catch-config",
        default="catch.config",
        type=CatchConfig.from_file,
        help="CATCH configuration file",
    )

    parser.add_argument(
        "--log",
        default="./logging/cs-harvester.log",
        help="log runtime messages to this file",
    )

    parser.add_argument(
        "--only-process",
        type=LIDVID,
        nargs="*",
        metavar="LIDVID",
        help="only process the collections matching these LIDVIDs",
    )

    parser.add_argument(
        "--dry-run",
        "-n",
        action="store_true",
        help="process labels, but do not add to the database",
    )

    parser.add_argument(
        "--verbose", "-v", action="store_true", help="log debugging messages"
    )


def with_args(args):
    """Update configuration from argparse arguments."""
    global catch_config, runtime_log_filename, only_process, dry_run, verbose, update

    catch_config = args.catch_config
    runtime_log_filename = args.log
    dry_run = args.dry_run
    only_process = args.only_process
    verbose = args.verbose
