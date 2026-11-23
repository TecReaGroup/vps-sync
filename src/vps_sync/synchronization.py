"""Mirror synchronization between the local and remote directories.

Flow: snapshot both trees -> plan -> hash only same-size files -> delete extras
-> create directories -> transfer over parallel SSH connections
"""

import errno
import hashlib
import logging
import queue
import shlex
import threading
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from functools import partial
from pathlib import PurePosixPath
from typing import Protocol

from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
)

from vps_sync.config import SyncSettings
from vps_sync.remote import (
    RemoteTree,
    SftpSession,
    open_sftp_session,
    resolve_remote_root,
    run_remote_command,
)
from vps_sync.tree import (
    HASH_CHUNK_SIZE,
    LocalTree,
    SynchronizationError,
    TreeSnapshot,
    build_sync_plan,
    temporary_file_name,
)

TRANSFER_WORKERS = 4
SFTP_PREFETCH_REQUESTS = 64
LOGGER = logging.getLogger("vps_sync.synchronization")

ConfirmEmptySource = Callable[[str], bool]
TransferFile = Callable[[SftpSession, PurePosixPath, str, Callable[[int], None]], None]


class DirectoryTree(Protocol):
    """Operations the mirror algorithm needs from either side"""

    label: str

    def snapshot(self) -> TreeSnapshot: ...

    def hash_files(
        self, paths: list[PurePosixPath], on_hashed: Callable[[int], None]
    ) -> dict[PurePosixPath, str]: ...

    def remove(self, files: list[PurePosixPath], directories: list[PurePosixPath]) -> None: ...

    def create_directories(self, directories: list[PurePosixPath]) -> None: ...


@dataclass(frozen=True, slots=True)
class SyncSummary:
    """Counts collected during one synchronization operation"""

    source_files: int
    transferred_files: int
    skipped_files: int
    deleted_entries: int


def create_terminal_progress() -> Progress:
    """Create a transient progress display that never enters persistent logs"""
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
    """Mirror one configured local and remote directory pair"""

    def __init__(self, settings: SyncSettings, confirm_empty_source: ConfirmEmptySource) -> None:
        """Store settings and the prompt used before mirroring an empty source"""
        self._settings = settings
        self._confirm_empty_source = confirm_empty_source

    def upload(self) -> SyncSummary:
        """Make the remote directory an exact mirror of the local directory"""
        local_tree = LocalTree(self._settings.local_directory)
        if not local_tree.root.is_dir():
            raise SynchronizationError(f"Local upload directory does not exist: {local_tree.root}")
        with open_sftp_session(self._settings) as session:
            remote_root = resolve_remote_root(session.sftp, self._settings.remote_directory)
            remote_tree = RemoteTree(session, remote_root)
            remote_tree.ensure_root()
            upload = partial(_upload_file, local_tree=local_tree, remote_root=remote_root)
            return self._mirror(local_tree, remote_tree, upload, "Uploading")

    def download(self) -> SyncSummary:
        """Make the local directory an exact mirror of the remote directory"""
        local_tree = LocalTree(self._settings.local_directory)
        local_tree.root.mkdir(parents=True, exist_ok=True)
        with open_sftp_session(self._settings) as session:
            remote_root = resolve_remote_root(session.sftp, self._settings.remote_directory)
            remote_tree = RemoteTree(session, remote_root)
            download = partial(_download_file, local_tree=local_tree, remote_root=remote_root)
            return self._mirror(remote_tree, local_tree, download, "Downloading")

    def _mirror(
        self,
        source: DirectoryTree,
        target: DirectoryTree,
        transfer_file: TransferFile,
        transfer_action: str,
    ) -> SyncSummary:
        with create_terminal_progress() as progress:
            listing_task = progress.add_task("Listing files", total=None, current="")
            with ThreadPoolExecutor(2) as pool:
                source_listing = pool.submit(source.snapshot)
                target_snapshot = target.snapshot()
                source_snapshot = source_listing.result()
            progress.remove_task(listing_task)

        if not source_snapshot.has_synchronized_entries() and not target_snapshot.is_empty():
            question = (
                f"Source {source.label} is empty; mirroring will delete everything in "
                f"the {target.label}."
            )
            if not self._confirm_empty_source(question):
                raise SynchronizationError("Synchronization cancelled: source directory is empty")

        for relative_path in sorted(source_snapshot.unsupported):
            LOGGER.warning("Skipped unsupported source entry: %s", relative_path)
        plan = build_sync_plan(source_snapshot, target_snapshot)
        LOGGER.info(
            "Plan: source_files=%d changed=%d same_size=%d delete_files=%d "
            "delete_directories=%d create_directories=%d",
            len(source_snapshot.file_sizes),
            len(plan.changed_files),
            len(plan.same_size_files),
            len(plan.deleted_files),
            len(plan.deleted_directories),
            len(plan.created_directories),
        )

        with create_terminal_progress() as progress:
            # Source digests of changed files are the integrity reference for their transfer
            source_digests, target_digests = _hash_both_sides(
                source,
                plan.changed_files + plan.same_size_files,
                target,
                plan.same_size_files,
                progress,
            )
            outdated_files = [
                path
                for path in plan.same_size_files
                if source_digests[path] != target_digests[path]
            ]
            transferred_files = sorted(plan.changed_files + outdated_files)

            target.remove(plan.deleted_files, plan.deleted_directories)
            target.create_directories(plan.created_directories)
            self._transfer_all(
                transferred_files,
                source_snapshot.file_sizes,
                source_digests,
                transfer_file,
                transfer_action,
                progress,
            )

        source_file_count = len(source_snapshot.file_sizes)
        return SyncSummary(
            source_files=source_file_count,
            transferred_files=len(transferred_files),
            skipped_files=source_file_count - len(transferred_files),
            deleted_entries=len(plan.deleted_files) + len(plan.deleted_directories),
        )

    def _transfer_all(
        self,
        paths: list[PurePosixPath],
        file_sizes: dict[PurePosixPath, int],
        digests: dict[PurePosixPath, str],
        transfer_file: TransferFile,
        transfer_action: str,
        progress: Progress,
    ) -> None:
        """Transfer files over independent SSH connections; the first failure stops all"""
        if not paths:
            return
        pending_paths: queue.SimpleQueue[PurePosixPath] = queue.SimpleQueue()
        for path in paths:
            pending_paths.put(path)
        stop_requested = threading.Event()
        transfer_task = progress.add_task(
            f"{transfer_action} {len(paths)} files",
            total=sum(file_sizes[path] for path in paths),
            current="",
        )

        def advance(byte_count: int) -> None:
            progress.advance(transfer_task, byte_count)

        def run_worker() -> None:
            with open_sftp_session(self._settings) as session:
                while not stop_requested.is_set():
                    try:
                        relative_path = pending_paths.get_nowait()
                    except queue.Empty:
                        return
                    progress.update(transfer_task, current=str(relative_path))
                    transfer_file(session, relative_path, digests[relative_path], advance)
                    LOGGER.info("%s done: %s", transfer_action, relative_path)

        worker_count = min(TRANSFER_WORKERS, len(paths))
        try:
            with ThreadPoolExecutor(worker_count) as pool:
                futures = [pool.submit(run_worker) for _ in range(worker_count)]
                try:
                    for future in as_completed(futures):
                        future.result()
                finally:
                    stop_requested.set()
        finally:
            progress.remove_task(transfer_task)


def _hash_both_sides(
    source: DirectoryTree,
    source_paths: list[PurePosixPath],
    target: DirectoryTree,
    target_paths: list[PurePosixPath],
    progress: Progress,
) -> tuple[dict[PurePosixPath, str], dict[PurePosixPath, str]]:
    """Hash the local and remote sides concurrently"""
    source_task = progress.add_task(f"Hashing {source.label}", total=len(source_paths), current="")
    target_task = progress.add_task(f"Hashing {target.label}", total=len(target_paths), current="")
    try:
        with ThreadPoolExecutor(1) as pool:
            source_hashing = pool.submit(
                source.hash_files, source_paths, partial(progress.advance, source_task)
            )
            target_digests = target.hash_files(target_paths, partial(progress.advance, target_task))
            return source_hashing.result(), target_digests
    finally:
        progress.remove_task(source_task)
        progress.remove_task(target_task)


class _CumulativeByteCounter:
    """Convert paramiko's cumulative callback into byte deltas"""

    def __init__(self, on_bytes: Callable[[int], None]) -> None:
        self._on_bytes = on_bytes
        self._reported_bytes = 0

    def __call__(self, transferred_bytes: int, _total_bytes: int) -> None:
        self._on_bytes(transferred_bytes - self._reported_bytes)
        self._reported_bytes = transferred_bytes


def _upload_file(
    session: SftpSession,
    relative_path: PurePosixPath,
    expected_md5: str,
    on_bytes: Callable[[int], None],
    *,
    local_tree: LocalTree,
    remote_root: PurePosixPath,
) -> None:
    """Upload to a temporary file, then verify and rename it in one remote command"""
    remote_file = remote_root / relative_path
    temporary_file = remote_file.with_name(temporary_file_name(remote_file.name))
    verify_and_replace = (
        f"tmp={shlex.quote(str(temporary_file))}; target={shlex.quote(str(remote_file))}; "
        'actual=$(md5sum < "$tmp") || exit; '
        f'if [ "${{actual%% *}}" = {expected_md5} ]; then mv -f -- "$tmp" "$target"; '
        'else rm -f -- "$tmp"; echo "MD5 mismatch after upload" >&2; exit 1; fi'
    )
    try:
        session.sftp.put(
            str(local_tree.path_of(relative_path)),
            str(temporary_file),
            callback=_CumulativeByteCounter(on_bytes),
            confirm=False,
        )
        run_remote_command(session.ssh, verify_and_replace, f"Upload of {relative_path}")
    except Exception:
        _remove_remote_file_if_present(session, temporary_file)
        raise


def _download_file(
    session: SftpSession,
    relative_path: PurePosixPath,
    expected_md5: str,
    on_bytes: Callable[[int], None],
    *,
    local_tree: LocalTree,
    remote_root: PurePosixPath,
) -> None:
    """Download to a temporary file while hashing, then verify and replace the target"""
    local_file = local_tree.path_of(relative_path)
    temporary_file = local_file.with_name(temporary_file_name(local_file.name))
    digest = hashlib.md5(usedforsecurity=False)
    try:
        with (
            session.sftp.open(str(remote_root / relative_path), "rb") as remote_stream,
            temporary_file.open("wb") as local_stream,
        ):
            remote_stream.prefetch(max_concurrent_requests=SFTP_PREFETCH_REQUESTS)
            while chunk := remote_stream.read(HASH_CHUNK_SIZE):
                local_stream.write(chunk)
                digest.update(chunk)
                on_bytes(len(chunk))
        if digest.hexdigest() != expected_md5:
            raise SynchronizationError(f"Downloaded file MD5 verification failed: {relative_path}")
        temporary_file.replace(local_file)
    finally:
        temporary_file.unlink(missing_ok=True)


def _remove_remote_file_if_present(session: SftpSession, remote_file: PurePosixPath) -> None:
    try:
        session.sftp.remove(str(remote_file))
    except OSError as error:
        if error.errno != errno.ENOENT:
            raise
