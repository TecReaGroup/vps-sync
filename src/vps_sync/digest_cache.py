"""Persisted MD5 digests that stay valid while a file keeps its size and mtime"""

import json
import logging
from dataclasses import dataclass
from pathlib import PurePosixPath

LOGGER = logging.getLogger("vps_sync.digest_cache")


@dataclass(frozen=True, slots=True)
class FileStamp:
    """Metadata that changes whenever a file is rewritten by ordinary tools"""

    size: int
    # Kept as GNU find's %T@ text so comparisons never depend on float rounding
    modified_time: str


@dataclass(frozen=True, slots=True)
class CachedDigest:
    """An MD5 digest together with the file stamp it was computed for"""

    stamp: FileStamp
    md5: str


DigestCache = dict[PurePosixPath, CachedDigest]


def parse_digest_cache(raw_cache: bytes) -> DigestCache:
    """Decode a cache file; a malformed one is discarded so digests get recomputed"""
    try:
        entries = json.loads(raw_cache)
        return {
            PurePosixPath(path): CachedDigest(
                FileStamp(int(entry["size"]), str(entry["mtime"])), str(entry["md5"])
            )
            for path, entry in entries.items()
        }
    except (ValueError, TypeError, KeyError, AttributeError) as error:
        LOGGER.warning("Ignored malformed digest cache: %s", error)
        return {}


def serialize_digest_cache(cache: DigestCache) -> bytes:
    """Encode a cache as compact UTF-8 JSON sorted by path"""
    entries = {
        str(path): {
            "size": cached.stamp.size,
            "mtime": cached.stamp.modified_time,
            "md5": cached.md5,
        }
        for path, cached in sorted(cache.items())
    }
    return json.dumps(entries, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
