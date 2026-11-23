"""Directory snapshots, mirror planning, and local directory operations"""

import hashlib
import logging
import os
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from uuid import uuid4

HASH_CHUNK_SIZE = 1024 * 1024
LOCAL_HASH_WORKERS = 4
TEMPORARY_FILE_MARKER = ".vps-sync-"
TEMPORARY_FILE_SUFFIX = ".tmp"
LOGGER = logging.getLogger("vps_sync.tree")


class SynchronizationError(RuntimeError):
    """Raised when synchronization cannot preserve file integrity"""


@dataclass(frozen=True, slots=True)
class TreeSnapshot:
    """Entries below a synchronization root, keyed by POSIX relative path.

    ``unsupported`` holds symbolic links, special files, and vps-sync temporary
    files: they are skipped on the source side and deleted on the target side
    """

    file_sizes: dict[PurePosixPath, int]
    directories: set[PurePosixPath]
    unsupported: set[PurePosixPath]

    def has_synchronized_entries(self) -> bool:
        """Return whether any file or directory would be mirrored from this tree"""
        return bool(self.file_sizes or self.directories)

    def is_empty(self) -> bool:
        """Return whether the tree contains no entries at all"""
        return not (self.file_sizes or self.directories or self.unsupported)


@dataclass(frozen=True, slots=True)
class SyncPlan:
    """Operations that turn the target tree into a mirror of the source tree"""

    deleted_files: list[PurePosixPath]
    # Deepest first so every directory is empty when it is removed
    deleted_directories: list[PurePosixPath]
    created_directories: list[PurePosixPath]
    # Missing on the target or different in size: transfer without comparing digests
    changed_files: list[PurePosixPath]
    same_size_files: list[PurePosixPath]


def build_sync_plan(source: TreeSnapshot, target: TreeSnapshot) -> SyncPlan:
    """Compare two snapshots without touching either filesystem"""
    # NOTE: Unsupported target entries are always removed, even when the source has a
    # regular file there, because renaming onto a remote directory symlink would move
    # the file into the linked directory instead of replacing the link
    deleted_files = sorted(
        target.unsupported | {path for path in target.file_sizes if path not in source.file_sizes}
    )
    deleted_directories = sorted(
        target.directories - source.directories,
        key=lambda path: (len(path.parts), path),
        reverse=True,
    )
    changed_files: list[PurePosixPath] = []
    same_size_files: list[PurePosixPath] = []
    for path, size in sorted(source.file_sizes.items()):
        if target.file_sizes.get(path) == size:
            same_size_files.append(path)
        else:
            changed_files.append(path)
    return SyncPlan(
        deleted_files=deleted_files,
        deleted_directories=deleted_directories,
        created_directories=sorted(source.directories - target.directories),
        changed_files=changed_files,
        same_size_files=same_size_files,
    )


def temporary_file_name(file_name: str) -> str:
    """Create a unique hidden sibling name for an in-flight transfer"""
    return f".{file_name}{TEMPORARY_FILE_MARKER}{uuid4().hex}{TEMPORARY_FILE_SUFFIX}"


def is_temporary_file_name(file_name: str) -> bool:
    """Return whether a name belongs to an interrupted vps-sync transfer"""
    return (
        file_name.startswith(".")
        and TEMPORARY_FILE_MARKER in file_name
        and file_name.endswith(TEMPORARY_FILE_SUFFIX)
    )


def local_file_md5(local_file: Path) -> str:
    """Calculate the MD5 digest of a local file"""
    digest = hashlib.md5(usedforsecurity=False)
    with local_file.open("rb") as file_stream:
        while chunk := file_stream.read(HASH_CHUNK_SIZE):
            digest.update(chunk)
    return digest.hexdigest()


class LocalTree:
    """The local synchronization root"""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.label = f"local directory {root}"

    def path_of(self, relative_path: PurePosixPath) -> Path:
        """Map a POSIX relative path to a local filesystem path"""
        return self.root.joinpath(*relative_path.parts)

    def snapshot(self) -> TreeSnapshot:
        """List every entry below the root without following symbolic links"""
        file_sizes: dict[PurePosixPath, int] = {}
        directories: set[PurePosixPath] = set()
        unsupported: set[PurePosixPath] = set()
        pending_directories = [self.root]
        while pending_directories:
            with os.scandir(pending_directories.pop()) as entries:
                for entry in entries:
                    entry_path = Path(entry.path)
                    relative_path = PurePosixPath(*entry_path.relative_to(self.root).parts)
                    if entry.is_dir(follow_symlinks=False):
                        directories.add(relative_path)
                        pending_directories.append(entry_path)
                    elif entry.is_file(follow_symlinks=False) and not is_temporary_file_name(
                        entry.name
                    ):
                        file_sizes[relative_path] = entry.stat(follow_symlinks=False).st_size
                    else:
                        unsupported.add(relative_path)
        return TreeSnapshot(file_sizes, directories, unsupported)

    def hash_files(
        self,
        paths: list[PurePosixPath],
        on_hashed: Callable[[int], None],
    ) -> dict[PurePosixPath, str]:
        """Hash files in parallel; ``on_hashed`` receives the count just finished"""

        def hash_one(relative_path: PurePosixPath) -> tuple[PurePosixPath, str]:
            digest = local_file_md5(self.path_of(relative_path))
            on_hashed(1)
            return relative_path, digest

        with ThreadPoolExecutor(LOCAL_HASH_WORKERS) as pool:
            return dict(pool.map(hash_one, paths))

    def remove(self, files: list[PurePosixPath], directories: list[PurePosixPath]) -> None:
        """Delete files first, then directories in the given deepest-first order"""
        for relative_path in files:
            local_path = self.path_of(relative_path)
            try:
                local_path.unlink()
            except OSError:
                # NOTE: Windows directory symlinks can only be removed with rmdir
                local_path.rmdir()
            LOGGER.info("Deleted local file: %s", relative_path)
        for relative_path in directories:
            self.path_of(relative_path).rmdir()
            LOGGER.info("Deleted local directory: %s", relative_path)

    def create_directories(self, directories: list[PurePosixPath]) -> None:
        """Create directories and any missing parents"""
        for relative_path in directories:
            self.path_of(relative_path).mkdir(parents=True, exist_ok=True)
