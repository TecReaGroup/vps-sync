"""Application logging configuration"""

import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import override

LOG_DIRECTORY = Path("log")
LOG_LEVEL = logging.INFO
CONSOLE_LOG_LEVEL = logging.WARNING
LOG_TIMEZONE = timezone(timedelta(hours=8))
LOG_FORMAT = "[%(asctime)s] [%(levelname)s] [%(name)s] - %(message)s"
LOG_TIME_FORMAT = "%Y-%m-%d %H:%M:%S %:z"


class FixedOffsetFormatter(logging.Formatter):
    """Render record times in ``LOG_TIMEZONE`` regardless of the host timezone"""

    @override
    def formatTime(self, record: logging.LogRecord, datefmt: str | None = None) -> str:
        return datetime.fromtimestamp(record.created, LOG_TIMEZONE).strftime(LOG_TIME_FORMAT)


def configure_logging() -> None:
    """Send warnings to the console and everything to ``log/log_YYYY-MM-DD.log``"""
    LOG_DIRECTORY.mkdir(parents=True, exist_ok=True)
    formatter = FixedOffsetFormatter(LOG_FORMAT)

    console_output = logging.StreamHandler()
    console_output.setFormatter(formatter)
    console_output.setLevel(CONSOLE_LOG_LEVEL)

    log_date = datetime.now(LOG_TIMEZONE).date().isoformat()
    persisted_output = logging.FileHandler(LOG_DIRECTORY / f"log_{log_date}.log", encoding="utf-8")
    persisted_output.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.addHandler(console_output)
    root_logger.addHandler(persisted_output)
    root_logger.setLevel(LOG_LEVEL)
    logging.getLogger("paramiko").setLevel(logging.WARNING)
