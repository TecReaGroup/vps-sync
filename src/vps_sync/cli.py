"""Command-line entry point"""

import argparse
import logging
from collections.abc import Sequence
from pathlib import Path

from paramiko import AuthenticationException, SSHException

from vps_sync import __version__
from vps_sync.config import DEFAULT_ENV_FILE, ConfigurationError, SyncSettings
from vps_sync.log import configure_logging
from vps_sync.synchronization import DirectorySynchronizer
from vps_sync.tree import SynchronizationError

EXIT_SUCCESS = 0
EXIT_INVALID_STATE = 2
EXIT_AUTHENTICATION_FAILED = 3
EXIT_SSH_FAILED = 5
EXIT_IO_FAILED = 6
LOGGER = logging.getLogger("vps_sync.cli")


def build_parser() -> argparse.ArgumentParser:
    """Create the command-line parser"""
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


def confirm_on_terminal(question: str) -> bool:
    """Ask a yes/no question; non-interactive input counts as no"""
    try:
        answer = input(f"{question} Continue? [y/N] ")
    except EOFError:
        return False
    return answer.strip().lower() in {"y", "yes"}


def main(argv: Sequence[str] | None = None) -> int:
    """Run one upload or download synchronization"""
    arguments = build_parser().parse_args(argv)
    configure_logging()

    try:
        settings = SyncSettings.from_env_file(arguments.env_file)
        LOGGER.info("Synchronization started: direction=%s", arguments.direction)
        synchronizer = DirectorySynchronizer(settings, confirm_on_terminal)
        summary = (
            synchronizer.upload() if arguments.direction == "upload" else synchronizer.download()
        )
    except (ConfigurationError, SynchronizationError) as error:
        LOGGER.error("%s", error)
        return EXIT_INVALID_STATE
    except AuthenticationException:
        LOGGER.error("SSH authentication failed")
        return EXIT_AUTHENTICATION_FAILED
    except SSHException as error:
        LOGGER.error("SSH connection failed: %s", error)
        return EXIT_SSH_FAILED
    except OSError as error:
        LOGGER.error("File or network operation failed: %s", error)
        return EXIT_IO_FAILED

    LOGGER.info(
        "Synchronization completed: source_files=%d transferred=%d skipped=%d deleted=%d",
        summary.source_files,
        summary.transferred_files,
        summary.skipped_files,
        summary.deleted_entries,
    )
    return EXIT_SUCCESS
