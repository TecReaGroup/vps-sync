"""SSH sessions and bulk operations on the remote synchronization root.

Bulk operations stream NUL-delimited relative paths through stdin so command
lines stay short regardless of how many files are involved. GNU coreutils and
findutils are required on the VPS.
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
REMOTE_HASH_BATCH_SIZE = 1000
LOGGER = logging.getLogger("vps_sync.remote")


@dataclass(frozen=True, slots=True)
class SftpSession:
    """Active SSH and SFTP clients sharing one authenticated transport."""

    ssh: SSHClient
    sftp: SFTPClient


@contextmanager
def open_sftp_session(settings: SyncSettings) -> Iterator[SftpSession]:
    """Open a password-authenticated SSH and SFTP session."""
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


def run_remote_command(
    ssh: SSHClient,
    command: str,
    operation: str,
    stdin_payload: bytes = b"",
) -> bytes:
    """Execute one remote command, feed it stdin, and require a zero exit status."""
    command_input, command_output, command_error = ssh.exec_command(command)
    if stdin_payload:
        command_input.channel.sendall(stdin_payload)
    command_input.channel.shutdown_write()
    output_bytes = bytes(command_output.read())
    error_text = command_error.read().decode("utf-8", errors="replace").strip()
    exit_status = command_output.channel.recv_exit_status()
    if exit_status != 0:
        detail = error_text or f"exit status {exit_status}"
        raise SynchronizationError(f"{operation} failed: {detail}")
    return output_bytes


def resolve_remote_root(sftp: SFTPClient, configured_path: str) -> PurePosixPath:
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


def _null_delimited(paths: Iterable[PurePosixPath]) -> bytes:
    return b"".join(str(path).encode("utf-8") + b"\0" for path in paths)


def _decode_path(raw_path: bytes) -> PurePosixPath:
    try:
        return PurePosixPath(raw_path.decode("utf-8"))
    except UnicodeError as error:
        raise SynchronizationError(f"Remote path is not valid UTF-8: {raw_path!r}") from error


def _parse_md5_output(output_bytes: bytes) -> dict[PurePosixPath, str]:
    """Parse GNU ``md5sum --zero`` output, which never escapes file names."""
    digests: dict[PurePosixPath, str] = {}
    for record in output_bytes.split(b"\0"):
        if not record:
            continue
        if len(record) < 35 or record[32:34] not in {b"  ", b" *"}:
            raise SynchronizationError("Remote md5sum returned malformed output")
        try:
            digest = record[:32].decode("ascii").lower()
            int(digest, 16)
        except (UnicodeError, ValueError) as error:
            raise SynchronizationError("Remote md5sum returned malformed output") from error
        digests[_decode_path(record[34:])] = digest
    return digests


class RemoteTree:
    """The remote synchronization root, operated through bulk shell commands."""

    def __init__(self, session: SftpSession, root: PurePosixPath) -> None:
        self.root = root
        self.label = f"remote directory {root}"
        self._ssh = session.ssh
        self._quoted_root = shlex.quote(str(root))

    def ensure_root(self) -> None:
        """Create the remote root and its parents when missing."""
        run_remote_command(self._ssh, f"mkdir -p -- {self._quoted_root}", "Remote root creation")

    def snapshot(self) -> TreeSnapshot:
        """List every entry below the root with one ``find`` call."""
        command = (
            f"[ -d {self._quoted_root} ] || {{ printf 'Remote directory does not exist: %s\\n' "
            f"{self._quoted_root} >&2; exit 1; }}; "
            f"cd -- {self._quoted_root} && find . -mindepth 1 -printf '%y\\0%s\\0%P\\0'"
        )
        fields = run_remote_command(self._ssh, command, "Remote listing").split(b"\0")
        file_sizes: dict[PurePosixPath, int] = {}
        directories: set[PurePosixPath] = set()
        unsupported: set[PurePosixPath] = set()
        # Records are (type, size, relative path) triples followed by one trailing empty field.
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
        """Hash files on the VPS in batches; ``on_hashed`` receives each batch size."""
        # Each md5sum worker writes to its own file because concurrent writers to one
        # pipe could interleave records that exceed PIPE_BUF.
        worker_script = shlex.quote('exec md5sum --zero -- "$@" > "$0/$$"')
        digests: dict[PurePosixPath, str] = {}
        for offset in range(0, len(paths), REMOTE_HASH_BATCH_SIZE):
            batch = paths[offset : offset + REMOTE_HASH_BATCH_SIZE]
            files_per_worker = -(-len(batch) // REMOTE_HASH_WORKERS)
            command = (
                f"cd -- {self._quoted_root} || exit; "
                "checksum_dir=$(mktemp -d) || exit; "
                "trap 'rm -rf -- \"$checksum_dir\"' EXIT; "
                f"xargs -0 -r -n {files_per_worker} -P {REMOTE_HASH_WORKERS} "
                f'sh -c {worker_script} "$checksum_dir" || exit; '
                'find "$checksum_dir" -type f -exec cat -- {} +'
            )
            output = run_remote_command(
                self._ssh, command, "Remote checksum", _null_delimited(batch)
            )
            digests.update(_parse_md5_output(output))
            on_hashed(len(batch))
        missing_paths = [path for path in paths if path not in digests]
        if missing_paths:
            raise SynchronizationError(f"Remote md5sum omitted {missing_paths[0]}")
        return digests

    def remove(self, files: list[PurePosixPath], directories: list[PurePosixPath]) -> None:
        """Delete files first, then directories in the given deepest-first order."""
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
        """Create directories and any missing parents with one command."""
        if directories:
            run_remote_command(
                self._ssh,
                f"cd -- {self._quoted_root} && xargs -0 -r mkdir -p --",
                "Remote directory creation",
                _null_delimited(directories),
            )
