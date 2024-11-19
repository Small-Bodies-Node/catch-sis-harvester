from argparse import ArgumentParser
from catch.config import Config as CatchConfig


harvest_log_filename: str = "harvest-log.ecsv"
harvest_log_format: str = "ascii.ecsv"
harvest_target: str = ""
harvest_source: str = ""
logger_name: str = "CATCH/SIS Harvester"
verbose: bool = False
dry_run: bool = False
update: bool = False
catch_config: CatchConfig = CatchConfig()
runtime_log_filename: str = ""
only_process: str | None = None


def with_args(args):
    """Update configuration from argparse arguments."""
    global catch_config, runtime_log_filename, only_process, dry_run, verbose, update, harvest_source, harvest_target

    catch_config = args.catch_config
    runtime_log_filename = args.log
    dry_run = args.dry_run
    only_process = args.only_process
    verbose = args.verbose
    update = args.update
    harvest_source = args.source
    harvest_target = args.target
