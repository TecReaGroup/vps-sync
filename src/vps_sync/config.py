"""Runtime configuration loaded from an environment file."""

import os
from dataclasses import dataclass
from pathlib import Path

DEFAULT_ENV_FILE = Path(".env")
DEFAULT_LOG_LEVEL = "INFO"
VALID_LOG_LEVELS = frozenset({"CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"})


class ConfigurationError(ValueError):
    """Raised when required synchronization configuration is invalid."""


@dataclass(frozen=True, slots=True)
class SyncSettings:
    """Validated settings for one local and remote directory pair."""

    local_directory: Path
    remote_directory: str
    host: str
    port: int
    username: str
    password: str
    connection_timeout_seconds: float
    log_level: str

    @classmethod
    def from_env_file(cls, env_file: Path = DEFAULT_ENV_FILE) -> "SyncSettings":
        """Load settings from an env file with process environment overrides."""
        file_values = _read_env_file(env_file)

        def required(name: str) -> str:
            value = os.getenv(name, file_values.get(name, "")).strip()
            if not value:
                raise ConfigurationError(f"Missing required configuration: {name}")
            return value

        return cls(
            local_directory=Path(required("VPS_SYNC_LOCAL_DIRECTORY")).expanduser().resolve(),
            remote_directory=required("VPS_SYNC_REMOTE_DIRECTORY"),
            host=required("VPS_SYNC_HOST"),
            port=_parse_port(os.getenv("VPS_SYNC_PORT", file_values.get("VPS_SYNC_PORT", "22"))),
            username=required("VPS_SYNC_USERNAME"),
            password=required("VPS_SYNC_PASSWORD"),
            connection_timeout_seconds=_parse_timeout(
                os.getenv(
                    "VPS_SYNC_CONNECTION_TIMEOUT_SECONDS",
                    file_values.get("VPS_SYNC_CONNECTION_TIMEOUT_SECONDS", "30"),
                )
            ),
            log_level=_parse_log_level(
                os.getenv(
                    "VPS_SYNC_LOG_LEVEL",
                    file_values.get("VPS_SYNC_LOG_LEVEL", DEFAULT_LOG_LEVEL),
                )
            ),
        )


def _read_env_file(env_file: Path) -> dict[str, str]:
    """Parse simple KEY=VALUE entries from an env file."""
    if not env_file.is_file():
        raise ConfigurationError(f"Environment file does not exist: {env_file}")

    values: dict[str, str] = {}
    for line_number, raw_line in enumerate(env_file.read_text(encoding="utf-8").splitlines(), 1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            raise ConfigurationError(f"Invalid env entry at {env_file}:{line_number}")
        name, raw_value = line.split("=", 1)
        key = name.strip()
        if not key:
            raise ConfigurationError(f"Empty env key at {env_file}:{line_number}")
        values[key] = _unquote(raw_value.strip())
    return values


def _unquote(value: str) -> str:
    """Remove matching single or double quotes from an env value."""
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
        return value[1:-1]
    return value


def _parse_port(raw_port: str) -> int:
    """Parse and validate an SSH port."""
    try:
        port = int(raw_port)
    except ValueError as error:
        raise ConfigurationError("VPS_SYNC_PORT must be an integer") from error
    if not 1 <= port <= 65535:
        raise ConfigurationError("VPS_SYNC_PORT must be between 1 and 65535")
    return port


def _parse_timeout(raw_timeout: str) -> float:
    """Parse and validate the SSH connection timeout."""
    try:
        timeout = float(raw_timeout)
    except ValueError as error:
        raise ConfigurationError("VPS_SYNC_CONNECTION_TIMEOUT_SECONDS must be a number") from error
    if timeout <= 0:
        raise ConfigurationError("VPS_SYNC_CONNECTION_TIMEOUT_SECONDS must be positive")
    return timeout


def _parse_log_level(raw_log_level: str) -> str:
    """Parse and validate the application log level."""
    log_level = raw_log_level.upper()
    if log_level not in VALID_LOG_LEVELS:
        allowed_levels = ", ".join(sorted(VALID_LOG_LEVELS))
        raise ConfigurationError(f"VPS_SYNC_LOG_LEVEL must be one of: {allowed_levels}")
    return log_level
