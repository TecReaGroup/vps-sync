"""MD5-based directory synchronization over SFTP."""

import errno
import hashlib
import logging
import shlex
import stat
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from uuid import uuid4

import paramiko
from paramiko import SFTPAttributes, SFTPClient, SSHClient
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TaskID,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
)

from vps_sync.config import SyncSettings

HASH_CHUNK_SIZE = 1024 * 1024
SFTP_PREFETCH_REQUESTS = 64
REMOTE_CHECKSUM_WORKERS = 4
REMOTE_CHECKSUM_BATCH_SIZE = 64
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


@dataclass(frozen=True, slots=True)
class SftpSession:
    """Active SSH and SFTP clients sharing one authenticated transport."""

    ssh: SSHClient
    sftp: SFTPClient


class TransferProgress:
    """Update a transient terminal progress task from SFTP callbacks."""

    def __init__(
        self,
        progress: Progress,
        parent_task_id: TaskID,
        action: str,
        relative_path: str,
    ) -> None:
        self._progress = progress
        self._parent_task_id = parent_task_id
        progress.update(parent_task_id, visible=False)
        self._task_id = progress.add_task(action, total=None, current=relative_path)
        self._last_update_time = time.monotonic()

    def __call__(self, transferred_bytes: int, total_bytes: int) -> None:
        """Refresh the transfer task periodically and at completion."""
        current_time = time.monotonic()
        completed = total_bytes > 0 and transferred_bytes >= total_bytes
        if not completed and current_time - self._last_update_time < 0.1:
            return
        self._progress.update(
            self._task_id,
            completed=transferred_bytes,
            total=total_bytes or None,
        )
        self._last_update_time = current_time

    def close(self) -> None:
        """Remove the completed transfer task from the terminal."""
        self._progress.remove_task(self._task_id)
        self._progress.update(self._parent_task_id, visible=True)


def create_terminal_progress() -> Progress:
    """Create a transient progress display that never enters persistent logs."""
    return Progress(
        SpinnerColumn(),
        TextColumn("{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        TimeElapsedColumn(),
        TextColumn("[dim]{task.fields[current]}"),
        transient=True,
        expand=True,
    )


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
        with self._open_sftp() as session:
            sftp = session.sftp
            remote_root = _resolve_remote_directory(sftp, self._settings.remote_directory)
            _create_remote_directory_tree(sftp, remote_root)
            local_files = list(_iter_local_files(local_root))
            with create_terminal_progress() as progress:
                scan_task = progress.add_task(
                    "Checking local files",
                    total=len(local_files),
                    current="",
                )
                try:
                    total_batches = (
                        len(local_files) + REMOTE_CHECKSUM_BATCH_SIZE - 1
                    ) // REMOTE_CHECKSUM_BATCH_SIZE
                    for offset in range(0, len(local_files), REMOTE_CHECKSUM_BATCH_SIZE):
                        batch = local_files[offset : offset + REMOTE_CHECKSUM_BATCH_SIZE]
                        batch_number = (offset // REMOTE_CHECKSUM_BATCH_SIZE) + 1
                        batch_transferred_files = 0
                        remote_files = [
                            remote_root.joinpath(*local_file.relative_to(local_root).parts)
                            for local_file in batch
                        ]
                        remote_digests = _remote_md5_for_files(
                            session,
                            remote_files,
                            progress,
                            scan_task,
                            batch_number=batch_number,
                            total_batches=total_batches,
                        )
                        for local_file, remote_file in zip(batch, remote_files, strict=True):
                            scanned_files += 1
                            relative_path = local_file.relative_to(local_root)
                            progress.update(scan_task, current=str(relative_path))
                            local_md5 = _local_md5(local_file)
                            if remote_digests.get(remote_file) != local_md5:
                                _create_remote_directory_tree(sftp, remote_file.parent)
                                _upload_verified(
                                    session,
                                    local_file,
                                    remote_file,
                                    local_md5,
                                    str(relative_path),
                                    progress,
                                    scan_task,
                                )
                                transferred_files += 1
                                batch_transferred_files += 1
                                LOGGER.info("Uploaded: %s", relative_path)
                            progress.advance(scan_task)
                        LOGGER.info(
                            "Upload batch completed: batch=%d/%d scanned=%d transferred=%d "
                            "skipped=%d",
                            batch_number,
                            total_batches,
                            len(batch),
                            batch_transferred_files,
                            len(batch) - batch_transferred_files,
                        )
                finally:
                    progress.remove_task(scan_task)

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
        with self._open_sftp() as session:
            sftp = session.sftp
            remote_root = _resolve_remote_directory(sftp, self._settings.remote_directory)
            if not _remote_directory_exists(sftp, remote_root):
                raise SynchronizationError(
                    f"Remote download directory does not exist: {remote_root}"
                )
            with create_terminal_progress() as progress:
                remote_files = _remote_file_list(session, remote_root, progress)
                scan_task = progress.add_task(
                    "Checking local files",
                    total=len(remote_files),
                    current="",
                )
                try:
                    total_batches = (
                        len(remote_files) + REMOTE_CHECKSUM_BATCH_SIZE - 1
                    ) // REMOTE_CHECKSUM_BATCH_SIZE
                    for offset in range(0, len(remote_files), REMOTE_CHECKSUM_BATCH_SIZE):
                        batch = remote_files[offset : offset + REMOTE_CHECKSUM_BATCH_SIZE]
                        batch_number = (offset // REMOTE_CHECKSUM_BATCH_SIZE) + 1
                        batch_transferred_files = 0
                        remote_digests = _remote_md5_for_files(
                            session,
                            batch,
                            progress,
                            scan_task,
                            batch_number=batch_number,
                            total_batches=total_batches,
                        )
                        for remote_file in batch:
                            scanned_files += 1
                            relative_path = remote_file.relative_to(remote_root)
                            progress.update(scan_task, current=str(relative_path))
                            local_file = local_root.joinpath(*relative_path.parts)
                            try:
                                remote_md5 = remote_digests[remote_file]
                            except KeyError as error:
                                raise SynchronizationError(
                                    f"Remote file disappeared during checksum scan: {remote_file}"
                                ) from error
                            if local_file.is_file() and _local_md5(local_file) == remote_md5:
                                progress.advance(scan_task)
                                continue

                            _download_verified(
                                session,
                                remote_file,
                                local_file,
                                remote_md5,
                                str(relative_path),
                                progress,
                                scan_task,
                            )
                            transferred_files += 1
                            batch_transferred_files += 1
                            LOGGER.info("Downloaded: %s", relative_path)
                            progress.advance(scan_task)
                        LOGGER.info(
                            "Download batch completed: batch=%d/%d scanned=%d transferred=%d "
                            "skipped=%d",
                            batch_number,
                            total_batches,
                            len(batch),
                            batch_transferred_files,
                            len(batch) - batch_transferred_files,
                        )
                finally:
                    progress.remove_task(scan_task)

        return SyncSummary(
            scanned_files=scanned_files,
            transferred_files=transferred_files,
            skipped_files=scanned_files - transferred_files,
        )

    @contextmanager
<<<<<<< HEAD
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
=======
    def _open_sftp(self) -> Iterator[SftpSession]:
        """Open a password-authenticated SSH and SFTP session."""
        ssh_client = SSHClient()
>>>>>>> 1d5e7c88d18eb8402f52db874131b24acce784cc
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
            yield SftpSession(
                ssh=ssh_client,
                sftp=sftp,
            )
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


def _local_md5(local_file: Path) -> str:
    """Calculate the MD5 digest of a local file."""
    digest = hashlib.md5(usedforsecurity=False)
    with local_file.open("rb") as file_stream:
        while chunk := file_stream.read(HASH_CHUNK_SIZE):
            digest.update(chunk)
    return digest.hexdigest()


def _remote_md5(session: SftpSession, remote_file: PurePosixPath) -> str:
    """Calculate a remote file digest on the VPS without transferring its content."""
    command_output = _execute_remote_command(
        session,
        f"md5sum --zero -- {shlex.quote(str(remote_file))}",
        f"Remote md5sum for {remote_file}",
    )
    digest_map = _parse_remote_md5_output(command_output)
    try:
        return digest_map[remote_file]
    except KeyError as error:
        raise SynchronizationError(f"Remote md5sum omitted {remote_file}") from error


def _remote_file_list(
    session: SftpSession,
    remote_root: PurePosixPath,
    progress: Progress,
) -> list[PurePosixPath]:
    """List remote regular files before processing them in checksum batches."""
    list_task = progress.add_task("Listing remote files", total=None, current="")
    command = f"find {shlex.quote(str(remote_root))} -type f -print0"
    try:
        command_output = _execute_remote_command(session, command, "Remote file listing")
    finally:
        progress.remove_task(list_task)
    try:
        remote_files = [
            PurePosixPath(record.decode("utf-8"))
            for record in command_output.split(b"\0")
            if record
        ]
    except UnicodeError as error:
        raise SynchronizationError("Remote file listing contains invalid UTF-8") from error
    return sorted(
        remote_file for remote_file in remote_files if not _is_temporary_file_name(remote_file.name)
    )


def _remote_md5_for_files(
    session: SftpSession,
    remote_files: list[PurePosixPath],
    progress: Progress,
    parent_task_id: TaskID,
    batch_number: int,
    total_batches: int,
) -> dict[PurePosixPath, str]:
    """Calculate digests for one upload batch and omit missing destinations."""
    progress.update(parent_task_id, visible=False)
    batch_task = progress.add_task(
        f"Checking remote batch {batch_number}/{total_batches}",
        total=None,
        current=f"{len(remote_files)} files",
    )
    arguments = " ".join(shlex.quote(str(remote_file)) for remote_file in remote_files)
    files_per_worker = max(1, len(remote_files) // REMOTE_CHECKSUM_WORKERS)
    worker_script = (
        'output_dir=$1; shift; for file do if [ -f "$file" ]; then '
        'md5sum --zero -- "$file" || exit; elif [ -e "$file" ]; then '
        "printf 'Remote destination is not a regular file: %s\\n' \"$file\" >&2; "
        'exit 2; fi; done > "$output_dir/$$"'
    )
    command = (
        "checksum_dir=$(mktemp -d); "
        "trap 'rm -rf \"$checksum_dir\"' EXIT; "
        f"printf '%s\\0' {arguments} | "
        f"xargs -0 -r -n {files_per_worker} -P {REMOTE_CHECKSUM_WORKERS} "
        f'sh -c {shlex.quote(worker_script)} sh "$checksum_dir"; '
        'xargs_status=$?; if [ "$xargs_status" -ne 0 ]; then exit "$xargs_status"; fi; '
        'find "$checksum_dir" -type f -exec cat {} +'
    )
    try:
        command_output = _execute_remote_command(session, command, "Remote batch checksum")
    finally:
        progress.remove_task(batch_task)
        progress.update(parent_task_id, visible=True)
    return _parse_remote_md5_output(command_output)


def _execute_remote_command(session: SftpSession, command: str, operation: str) -> bytes:
    """Execute one remote command and require a successful exit status."""
    command_input, command_output, command_error = session.ssh.exec_command(command)
    command_input.close()
    output_bytes = bytes(command_output.read())
    error_text = command_error.read().decode("utf-8", errors="replace").strip()
    exit_status = command_output.channel.recv_exit_status()
    if exit_status != 0:
        detail = error_text or f"exit status {exit_status}"
        raise SynchronizationError(f"{operation} failed: {detail}")
    return output_bytes


def _parse_remote_md5_output(output_bytes: bytes) -> dict[PurePosixPath, str]:
    """Parse null-delimited GNU md5sum output without filename ambiguity."""
    digest_map: dict[PurePosixPath, str] = {}
    for record in output_bytes.split(b"\0"):
        if not record:
            continue
        if len(record) < 35 or record[32:34] not in {b"  ", b" *"}:
            raise SynchronizationError("Remote md5sum returned malformed output")
        try:
            digest = record[:32].decode("ascii").lower()
            int(digest, 16)
            remote_file = PurePosixPath(record[34:].decode("utf-8"))
        except (UnicodeError, ValueError) as error:
            raise SynchronizationError("Remote md5sum returned malformed output") from error
        digest_map[remote_file] = digest
    return digest_map


def _upload_verified(
    session: SftpSession,
    local_file: Path,
    remote_file: PurePosixPath,
    expected_md5: str,
    relative_path: str,
    progress: Progress,
    parent_task_id: TaskID,
) -> None:
    """Upload to a temporary remote file, verify it, then replace the target."""
    sftp = session.sftp
    temporary_file = _temporary_remote_file(remote_file)
    transfer_progress = TransferProgress(progress, parent_task_id, "Uploading", relative_path)
    try:
        try:
            sftp.put(
                str(local_file),
                str(temporary_file),
                callback=transfer_progress,
                confirm=True,
            )
        finally:
            transfer_progress.close()
        if _remote_md5(session, temporary_file) != expected_md5:
            raise SynchronizationError(f"Uploaded file MD5 verification failed: {remote_file}")
        _replace_remote_file(sftp, temporary_file, remote_file)
    except Exception:
        _remove_remote_file_if_present(sftp, temporary_file)
        raise


def _download_verified(
    session: SftpSession,
    remote_file: PurePosixPath,
    local_file: Path,
    expected_md5: str,
    relative_path: str,
    progress: Progress,
    parent_task_id: TaskID,
) -> None:
    """Download to a temporary local file, verify it, then replace the target."""
    sftp = session.sftp
    local_file.parent.mkdir(parents=True, exist_ok=True)
    temporary_file = local_file.with_name(
        f".{local_file.name}{TEMPORARY_FILE_MARKER}{uuid4().hex}{TEMPORARY_FILE_SUFFIX}"
    )
    transfer_progress = TransferProgress(progress, parent_task_id, "Downloading", relative_path)
    try:
        try:
            sftp.get(
                str(remote_file),
                str(temporary_file),
                callback=transfer_progress,
                prefetch=True,
                max_concurrent_prefetch_requests=SFTP_PREFETCH_REQUESTS,
            )
        finally:
            transfer_progress.close()
        if _local_md5(temporary_file) != expected_md5:
            raise SynchronizationError(f"Downloaded file MD5 verification failed: {remote_file}")
        temporary_file.replace(local_file)
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
    return (
        file_name.startswith(".")
        and TEMPORARY_FILE_MARKER in file_name
        and (file_name.endswith((TEMPORARY_FILE_SUFFIX, ".backup")))
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
