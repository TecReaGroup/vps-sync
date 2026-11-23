"""SSH sessions and bulk operations on the remote synchronization root.

Bulk operations stream NUL-delimited relative paths through stdin so command
lines stay short regardless of how many files are involved. GNU coreutils and
findutils are required on the VPS
"""

import logging
import shlex
from collections.abc import Callable, Iterable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import PurePosixPath

import paramiko
from paramiko import SFTPClient, SSHClient

from vps_sync.config import SyncSettings
from vps_sync.tree import SynchronizationError, TreeSnapshot, is_temporary_file_name

CONNECTION_TIMEOUT_SECONDS = 30
REMOTE_HASH_WORKERS = 4
REMOTE_READ_CHUNK_SIZE = 64 * 1024
LOGGER = logging.getLogger("vps_sync.remote")


@dataclass(frozen=True, slots=True)
class SftpSession:
    """Active SSH and SFTP clients sharing one authenticated transport"""

    ssh: SSHClient
    sftp: SFTPClient


@contextmanager
def open_sftp_session(settings: SyncSettings) -> Iterator[SftpSession]:
    """Open a password-authenticated SSH and SFTP session"""
    ssh_client = SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    sftp: SFTPClient | None = None
    try:
        ssh_client.connect(
            hostname=settings.host,
            port=settings.port,
            username=settings.username,
            password=settings.password,
            timeout=CONNECTION_TIMEOUT_SECONDS,
            banner_timeout=CONNECTION_TIMEOUT_SECONDS,
            auth_timeout=CONNECTION_TIMEOUT_SECONDS,
            look_for_keys=False,
            allow_agent=False,
        )
        sftp = ssh_client.open_sftp()
        yield SftpSession(ssh=ssh_client, sftp=sftp)
    finally:
        if sftp is not None:
            sftp.close()
        ssh_client.close()


def stream_remote_command(
    ssh: SSHClient,
    command: str,
    operation: str,
    stdin_payload: bytes = b"",
) -> Iterator[bytes]:
    """Execute one remote command, yield stdout chunks as they arrive, and require exit 0"""
    command_input, command_output, command_error = ssh.exec_command(command)
    if stdin_payload:
        command_input.channel.sendall(stdin_payload)
    command_input.channel.shutdown_write()
    channel = command_output.channel
    while chunk := channel.recv(REMOTE_READ_CHUNK_SIZE):
        yield chunk
    error_text = command_error.read().decode("utf-8", errors="replace").strip()
    exit_status = channel.recv_exit_status()
    if exit_status != 0:
        detail = error_text or f"exit status {exit_status}"
        raise SynchronizationError(f"{operation} failed: {detail}")


def run_remote_command(
    ssh: SSHClient,
    command: str,
    operation: str,
    stdin_payload: bytes = b"",
) -> bytes:
    """Execute one remote command, feed it stdin, and require a zero exit status"""
    return b"".join(stream_remote_command(ssh, command, operation, stdin_payload))


def resolve_remote_root(sftp: SFTPClient, configured_path: str) -> PurePosixPath:
    """Resolve absolute, relative, and current-user home remote paths"""
    remote_home = PurePosixPath(sftp.normalize("."))
    if configured_path == "~":
        return remote_home
    if configured_path.startswith("~/"):
        return remote_home / configured_path[2:]
    if configured_path.startswith("~"):
        raise SynchronizationError("Only the current user's ~ remote path is supported")
    remote_path = PurePosixPath(configured_path)
    return remote_path if remote_path.is_absolute() else remote_home / remote_path


def _null_delimited(paths: Iterable[PurePosixPath]) -> bytes:
    return b"".join(str(path).encode("utf-8") + b"\0" for path in paths)


def _decode_path(raw_path: bytes) -> PurePosixPath:
    try:
        return PurePosixPath(raw_path.decode("utf-8"))
    except UnicodeError as error:
        raise SynchronizationError(f"Remote path is not valid UTF-8: {raw_path!r}") from error


def _parse_md5_record(record: bytes) -> tuple[PurePosixPath, str]:
    """Parse one GNU ``md5sum --zero`` record, which never escapes file names"""
    if len(record) < 35 or record[32:34] not in {b"  ", b" *"}:
        raise SynchronizationError("Remote md5sum returned malformed output")
    try:
        digest = record[:32].decode("ascii").lower()
        int(digest, 16)
    except (UnicodeError, ValueError) as error:
        raise SynchronizationError("Remote md5sum returned malformed output") from error
    return _decode_path(record[34:]), digest


class RemoteTree:
    """The remote synchronization root, operated through bulk shell commands"""

    def __init__(self, session: SftpSession, root: PurePosixPath) -> None:
        self.root = root
        self.label = f"remote directory {root}"
        self._ssh = session.ssh
        self._quoted_root = shlex.quote(str(root))

    def ensure_root(self) -> None:
        """Create the remote root and its parents when missing"""
        run_remote_command(self._ssh, f"mkdir -p -- {self._quoted_root}", "Remote root creation")

    def snapshot(self) -> TreeSnapshot:
        """List every entry below the root with one ``find`` call"""
        command = (
            f"[ -d {self._quoted_root} ] || {{ printf 'Remote directory does not exist: %s\\n' "
            f"{self._quoted_root} >&2; exit 1; }}; "
            f"cd -- {self._quoted_root} && find . -mindepth 1 -printf '%y\\0%s\\0%P\\0'"
        )
        fields = run_remote_command(self._ssh, command, "Remote listing").split(b"\0")
        file_sizes: dict[PurePosixPath, int] = {}
        directories: set[PurePosixPath] = set()
        unsupported: set[PurePosixPath] = set()
        # Records are (type, size, relative path) triples followed by one trailing empty field
        for offset in range(0, len(fields) - 1, 3):
            entry_type, raw_size, raw_path = fields[offset : offset + 3]
            relative_path = _decode_path(raw_path)
            if entry_type == b"d":
                directories.add(relative_path)
            elif entry_type == b"f" and not is_temporary_file_name(relative_path.name):
                file_sizes[relative_path] = int(raw_size)
            else:
                unsupported.add(relative_path)
        return TreeSnapshot(file_sizes, directories, unsupported)

    def hash_files(
        self,
        paths: list[PurePosixPath],
        on_hashed: Callable[[int], None],
    ) -> dict[PurePosixPath, str]:
        """Hash files on the VPS in parallel; ``on_hashed(1)`` fires as each digest arrives"""
        # One md5sum process per file emits its record in a single write at exit, which
        # is atomic on a shared pipe below PIPE_BUF. A longer path could interleave, but
        # that only corrupts records and trips the malformed/omitted checks below
        command = (
            f"cd -- {self._quoted_root} && "
            f"xargs -0 -r -n 1 -P {REMOTE_HASH_WORKERS} md5sum --zero --"
        )
        output_chunks = stream_remote_command(
            self._ssh, command, "Remote checksum", _null_delimited(paths)
        )
        digests: dict[PurePosixPath, str] = {}
        unfinished_record = b""
        for chunk in output_chunks:
            *records, unfinished_record = (unfinished_record + chunk).split(b"\0")
            for record in records:
                relative_path, digest = _parse_md5_record(record)
                digests[relative_path] = digest
                on_hashed(1)
        if unfinished_record:
            raise SynchronizationError("Remote md5sum returned malformed output")
        missing_paths = [path for path in paths if path not in digests]
        if missing_paths:
            raise SynchronizationError(f"Remote md5sum omitted {missing_paths[0]}")
        return digests

    def remove(self, files: list[PurePosixPath], directories: list[PurePosixPath]) -> None:
        """Delete files first, then directories in the given deepest-first order"""
        if files:
            run_remote_command(
                self._ssh,
                f"cd -- {self._quoted_root} && xargs -0 -r rm -f --",
                "Remote file deletion",
                _null_delimited(files),
            )
            for relative_path in files:
                LOGGER.info("Deleted remote file: %s", relative_path)
        if directories:
            run_remote_command(
                self._ssh,
                f"cd -- {self._quoted_root} && xargs -0 -r rmdir --",
                "Remote directory deletion",
                _null_delimited(directories),
            )
            for relative_path in directories:
                LOGGER.info("Deleted remote directory: %s", relative_path)

    def create_directories(self, directories: list[PurePosixPath]) -> None:
        """Create directories and any missing parents with one command"""
        if directories:
            run_remote_command(
                self._ssh,
                f"cd -- {self._quoted_root} && xargs -0 -r mkdir -p --",
                "Remote directory creation",
                _null_delimited(directories),
            )
