"""Command-line entry point."""

import argparse
import logging
from collections.abc import Sequence
from pathlib import Path

from paramiko import AuthenticationException, SSHException

from vps_sync import __version__
from vps_sync.config import DEFAULT_ENV_FILE, ConfigurationError, SyncSettings
from vps_sync.logging import configure_logging
from vps_sync.synchronization import DirectorySynchronizer, SynchronizationError

LOGGER = logging.getLogger("vps_sync.cli")


def build_parser() -> argparse.ArgumentParser:
    """Create the command-line parser."""
    parser = argparse.ArgumentParser(
        prog="vps-sync",
        description="Synchronize files over SFTP with MD5 integrity verification.",
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    parser.add_argument(
        "--env-file",
        type=Path,
        default=DEFAULT_ENV_FILE,
        help="environment file path (default: .env)",
    )
    parser.add_argument("direction", choices=("upload", "download"))
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run one upload or download synchronization."""
    arguments = build_parser().parse_args(argv)

    try:
        settings = SyncSettings.from_env_file(arguments.env_file)
        configure_logging(settings.log_level)
        LOGGER.info(
            "Synchronization started: direction=%s overwrite=%s",
            arguments.direction,
            settings.overwrite,
        )
        synchronizer = DirectorySynchronizer(settings)
        summary = (
            synchronizer.upload() if arguments.direction == "upload" else synchronizer.download()
        )
    except (ConfigurationError, SynchronizationError) as error:
        logging.basicConfig(level=logging.ERROR, format="[%(levelname)s] %(message)s")
        LOGGER.error("%s", error)
        return 2
    except AuthenticationException:
        LOGGER.error("SSH authentication failed")
        return 3
    except SSHException as error:
        LOGGER.error("SSH connection failed: %s", error)
        return 5
    except OSError as error:
        LOGGER.error("File or network operation failed: %s", error)
        return 6

    LOGGER.info(
        "Synchronization completed: scanned=%d transferred=%d skipped=%d",
        summary.scanned_files,
        summary.transferred_files,
        summary.skipped_files,
    )
    return 0
