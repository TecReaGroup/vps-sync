"""Application logging configuration."""

import logging
from datetime import date
from pathlib import Path

LOG_DIRECTORY = Path("log")
LOG_FORMAT = "[%(asctime)s] [%(levelname)s] [%(name)s] - %(message)s"
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def configure_logging(level: str) -> logging.Logger:
    """Configure console and date-based file logging."""
    LOG_DIRECTORY.mkdir(parents=True, exist_ok=True)
    formatter = logging.Formatter(LOG_FORMAT, datefmt=LOG_DATE_FORMAT)

    console_output = logging.StreamHandler()
    console_output.setFormatter(formatter)

    log_file = LOG_DIRECTORY / f"log_{date.today().isoformat()}.log"
    persisted_output = logging.FileHandler(log_file, encoding="utf-8")
    persisted_output.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.addHandler(console_output)
    root_logger.addHandler(persisted_output)
    root_logger.setLevel(level)
    return logging.getLogger("vps_sync")
