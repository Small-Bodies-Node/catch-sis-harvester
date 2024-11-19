"""Command line interface."""

import argparse

from catch.config import Config as CatchConfig

from .logger import setup_logger
from .registry import source_arguments


def get_parser():
    """Add glob)al configuration arguments to the argument parser."""

    parser = argparse.ArgumentParser

    parser.add_argument(
        "target",
        choices=("catch", "sbnsis"),
        help="harvest metadata for this program",
    )

    parser.add_argument(
        "--catch_config",
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
        metavar="LID",
        help="only process the collection matching this LID",
    )

    parser.add_argument(
        "--update", action="store_true", help="Update database on conflicts"
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

    sources = parser.add_subparsers()
    for source, add_arguments in source_arguments:
        source_parser = sources.add_parser(source)
        add_arguments(source_parser)


def main():
    from . import config

    parser = get_parser()
    args = parser.parse_args()

    config.with_args(args)

    logger = setup_logger(args.log)
