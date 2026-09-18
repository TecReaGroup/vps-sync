"""MD5-based directory synchronization over SFTP."""

import errno
import hashlib
import logging
import os
import stat
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from uuid import uuid4

import paramiko
from paramiko import SFTPAttributes, SFTPClient, SSHClient

from vps_sync.config import SyncSettings

HASH_CHUNK_SIZE = 1024 * 1024
TEMPORARY_FILE_MARKER = ".vps-sync-"
TEMPORARY_FILE_SUFFIX = ".tmp"
LOGGER = logging.getLogger("vps_sync.synchronization")


class SynchronizationError(RuntimeError):
    """Raised when synchronization cannot preserve file integrity."""


@dataclass(frozen=True, slots=True)
class SyncSummary:
    """Counts collected during one synchronization operation."""

    scanned_files: int
    transferred_files: int
    skipped_files: int


class DirectorySynchronizer:
    """Synchronize one configured local and remote directory pair."""

    def __init__(self, settings: SyncSettings) -> None:
        """Store validated synchronization settings."""
        self._settings = settings

    def upload(self) -> SyncSummary:
        """Upload new and changed local files to the remote directory."""
        local_root = self._settings.local_directory
        if not local_root.is_dir():
            raise SynchronizationError(f"Local upload directory does not exist: {local_root}")

        scanned_files = 0
        transferred_files = 0
        with self._open_sftp() as sftp:
            remote_root = _resolve_remote_directory(sftp, self._settings.remote_directory)
            _create_remote_directory_tree(sftp, remote_root)

            for local_file in _iter_local_files(local_root):
                scanned_files += 1
                relative_path = local_file.relative_to(local_root)
                remote_file = remote_root.joinpath(*relative_path.parts)
                local_md5 = _local_md5(local_file)
                if _remote_md5_if_file(sftp, remote_file) == local_md5:
                    LOGGER.info("Skipped unchanged upload: %s", relative_path)
                    continue

                _create_remote_directory_tree(sftp, remote_file.parent)
                _upload_verified(sftp, local_file, remote_file, local_md5)
                transferred_files += 1
                LOGGER.info("Uploaded: %s", relative_path)

        return SyncSummary(
            scanned_files=scanned_files,
            transferred_files=transferred_files,
            skipped_files=scanned_files - transferred_files,
        )

    def download(self) -> SyncSummary:
        """Download new and changed remote files to the local directory."""
        local_root = self._settings.local_directory
        local_root.mkdir(parents=True, exist_ok=True)

        scanned_files = 0
        transferred_files = 0
        with self._open_sftp() as sftp:
            remote_root = _resolve_remote_directory(sftp, self._settings.remote_directory)
            if not _remote_directory_exists(sftp, remote_root):
                raise SynchronizationError(
                    f"Remote download directory does not exist: {remote_root}"
                )

            for remote_file in _iter_remote_files(sftp, remote_root):
                scanned_files += 1
                relative_path = remote_file.relative_to(remote_root)
                local_file = local_root.joinpath(*relative_path.parts)
                remote_md5 = _remote_md5(sftp, remote_file)
                if local_file.is_file() and _local_md5(local_file) == remote_md5:
                    LOGGER.info("Skipped unchanged download: %s", relative_path)
                    continue

                _download_verified(sftp, remote_file, local_file, remote_md5)
                transferred_files += 1
                LOGGER.info("Downloaded: %s", relative_path)

        return SyncSummary(
            scanned_files=scanned_files,
            transferred_files=transferred_files,
            skipped_files=scanned_files - transferred_files,
        )

    @contextmanager
    def _open_sftp(self) -> Iterator[SFTPClient]:
        """Open an SSH and SFTP session, accepting unknown host keys."""
        ssh_client = SSHClient()
        ssh_client.load_system_host_keys()
        if self._settings.known_hosts_file is not None:
            if not self._settings.known_hosts_file.is_file():
                raise SynchronizationError(
                    f"Known-hosts file does not exist: {self._settings.known_hosts_file}"
                )
            ssh_client.load_host_keys(str(self._settings.known_hosts_file))
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        sftp: SFTPClient | None = None
        try:
            ssh_client.connect(
                hostname=self._settings.host,
                port=self._settings.port,
                username=self._settings.username,
                password=self._settings.password,
                timeout=self._settings.connection_timeout_seconds,
                banner_timeout=self._settings.connection_timeout_seconds,
                auth_timeout=self._settings.connection_timeout_seconds,
                look_for_keys=False,
                allow_agent=False,
            )
            sftp = ssh_client.open_sftp()
            yield sftp
        finally:
            if sftp is not None:
                sftp.close()
            ssh_client.close()


def _iter_local_files(local_root: Path) -> Iterator[Path]:
    """Yield regular local files without following symbolic links."""
    for local_path in sorted(local_root.rglob("*")):
        if _is_temporary_file_name(local_path.name):
            LOGGER.warning("Skipped vps-sync temporary file: %s", local_path)
        elif local_path.is_symlink():
            LOGGER.warning("Skipped local symbolic link: %s", local_path)
        elif local_path.is_file():
            yield local_path


def _iter_remote_files(sftp: SFTPClient, remote_root: PurePosixPath) -> Iterator[PurePosixPath]:
    """Yield regular remote files recursively without following symbolic links."""
    pending_directories = [remote_root]
    while pending_directories:
        remote_directory = pending_directories.pop()
        entries = sorted(sftp.listdir_attr(str(remote_directory)), key=lambda entry: entry.filename)
        for entry in entries:
            remote_path = remote_directory / entry.filename
            if _is_temporary_file_name(entry.filename):
                LOGGER.warning("Skipped vps-sync temporary file: %s", remote_path)
            elif stat.S_ISDIR(entry.st_mode):
                pending_directories.append(remote_path)
            elif stat.S_ISREG(entry.st_mode):
                yield remote_path
            else:
                LOGGER.warning("Skipped non-regular remote path: %s", remote_path)


def _local_md5(local_file: Path) -> str:
    """Calculate the MD5 digest of a local file."""
    digest = hashlib.md5(usedforsecurity=False)
    with local_file.open("rb") as file_stream:
        while chunk := file_stream.read(HASH_CHUNK_SIZE):
            digest.update(chunk)
    return digest.hexdigest()


def _remote_md5(sftp: SFTPClient, remote_file: PurePosixPath) -> str:
    """Calculate the MD5 digest of a remote file through SFTP."""
    digest = hashlib.md5(usedforsecurity=False)
    with sftp.open(str(remote_file), "rb") as file_stream:
        while chunk := file_stream.read(HASH_CHUNK_SIZE):
            digest.update(chunk)
    return digest.hexdigest()


def _remote_md5_if_file(sftp: SFTPClient, remote_file: PurePosixPath) -> str | None:
    """Return a remote regular file digest, or None when it does not exist."""
    try:
        attributes = sftp.stat(str(remote_file))
    except OSError as error:
        if error.errno == errno.ENOENT:
            return None
        raise
    if not stat.S_ISREG(attributes.st_mode):
        raise SynchronizationError(f"Remote destination is not a regular file: {remote_file}")
    return _remote_md5(sftp, remote_file)


def _upload_verified(
    sftp: SFTPClient,
    local_file: Path,
    remote_file: PurePosixPath,
    expected_md5: str,
) -> None:
    """Upload to a temporary remote file, verify it, then replace the target."""
    temporary_file = _temporary_remote_file(remote_file)
    try:
        sftp.put(str(local_file), str(temporary_file), confirm=True)
        if _remote_md5(sftp, temporary_file) != expected_md5:
            raise SynchronizationError(f"Uploaded file MD5 verification failed: {remote_file}")
        _replace_remote_file(sftp, temporary_file, remote_file)
    except Exception:
        _remove_remote_file_if_present(sftp, temporary_file)
        raise


def _download_verified(
    sftp: SFTPClient,
    remote_file: PurePosixPath,
    local_file: Path,
    expected_md5: str,
) -> None:
    """Download to a temporary local file, verify it, then replace the target."""
    local_file.parent.mkdir(parents=True, exist_ok=True)
    temporary_file = local_file.with_name(
        f".{local_file.name}{TEMPORARY_FILE_MARKER}{uuid4().hex}{TEMPORARY_FILE_SUFFIX}"
    )
    try:
        sftp.get(str(remote_file), str(temporary_file))
        if _local_md5(temporary_file) != expected_md5:
            raise SynchronizationError(f"Downloaded file MD5 verification failed: {remote_file}")
        os.replace(temporary_file, local_file)
    finally:
        temporary_file.unlink(missing_ok=True)


def _replace_remote_file(
    sftp: SFTPClient,
    temporary_file: PurePosixPath,
    remote_file: PurePosixPath,
) -> None:
    """Replace a remote file, preferring the OpenSSH atomic rename extension."""
    try:
        sftp.posix_rename(str(temporary_file), str(remote_file))
    except OSError:
        _replace_remote_file_with_backup(sftp, temporary_file, remote_file)


def _replace_remote_file_with_backup(
    sftp: SFTPClient,
    temporary_file: PurePosixPath,
    remote_file: PurePosixPath,
) -> None:
    """Replace a remote file while retaining the previous file for rollback."""
    backup_file = remote_file.with_name(
        f".{remote_file.name}{TEMPORARY_FILE_MARKER}{uuid4().hex}.backup"
    )
    remote_file_exists = _remote_regular_file_exists(sftp, remote_file)
    if remote_file_exists:
        sftp.rename(str(remote_file), str(backup_file))
    try:
        sftp.rename(str(temporary_file), str(remote_file))
    except Exception:
        if remote_file_exists:
            sftp.rename(str(backup_file), str(remote_file))
        raise
    if remote_file_exists:
        sftp.remove(str(backup_file))


def _remove_remote_file_if_present(sftp: SFTPClient, remote_file: PurePosixPath) -> None:
    """Remove a remote file when it exists."""
    try:
        sftp.remove(str(remote_file))
    except OSError as error:
        if error.errno != errno.ENOENT:
            raise


def _remote_regular_file_exists(sftp: SFTPClient, remote_file: PurePosixPath) -> bool:
    """Return whether a remote path exists as a regular file."""
    try:
        attributes = sftp.stat(str(remote_file))
    except OSError as error:
        if error.errno == errno.ENOENT:
            return False
        raise
    if not stat.S_ISREG(attributes.st_mode):
        raise SynchronizationError(f"Remote destination is not a regular file: {remote_file}")
    return True


def _temporary_remote_file(remote_file: PurePosixPath) -> PurePosixPath:
    """Create a unique temporary path beside a remote destination file."""
    return remote_file.with_name(
        f".{remote_file.name}{TEMPORARY_FILE_MARKER}{uuid4().hex}{TEMPORARY_FILE_SUFFIX}"
    )


def _is_temporary_file_name(file_name: str) -> bool:
    """Return whether a name belongs to an interrupted vps-sync transfer."""
    return file_name.startswith(".") and TEMPORARY_FILE_MARKER in file_name and (
        file_name.endswith(TEMPORARY_FILE_SUFFIX) or file_name.endswith(".backup")
    )


def _resolve_remote_directory(sftp: SFTPClient, configured_path: str) -> PurePosixPath:
    """Resolve absolute, relative, and current-user home remote paths."""
    remote_home = PurePosixPath(sftp.normalize("."))
    if configured_path == "~":
        return remote_home
    if configured_path.startswith("~/"):
        return remote_home / configured_path[2:]
    if configured_path.startswith("~"):
        raise SynchronizationError("Only the current user's ~ remote path is supported")

    remote_path = PurePosixPath(configured_path)
    return remote_path if remote_path.is_absolute() else remote_home / remote_path


def _create_remote_directory_tree(sftp: SFTPClient, remote_directory: PurePosixPath) -> None:
    """Create a remote directory and all missing parents."""
    current_directory = PurePosixPath("/")
    for part in remote_directory.parts[1:]:
        current_directory /= part
        try:
            attributes = sftp.stat(str(current_directory))
        except OSError as error:
            if error.errno != errno.ENOENT:
                raise
            sftp.mkdir(str(current_directory))
            continue
        _require_remote_directory(attributes, current_directory)


def _remote_directory_exists(sftp: SFTPClient, remote_directory: PurePosixPath) -> bool:
    """Return whether a remote path exists as a directory."""
    try:
        attributes = sftp.stat(str(remote_directory))
    except OSError as error:
        if error.errno == errno.ENOENT:
            return False
        raise
    _require_remote_directory(attributes, remote_directory)
    return True


def _require_remote_directory(
    attributes: SFTPAttributes,
    remote_directory: PurePosixPath,
) -> None:
    """Require a remote path to be a directory."""
    if not stat.S_ISDIR(attributes.st_mode):
        raise SynchronizationError(f"Remote path is not a directory: {remote_directory}")
