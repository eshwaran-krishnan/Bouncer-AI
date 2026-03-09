"""
bouncer/utils/hashing.py — SHA-256 file hashing for provenance tracking.
"""

from __future__ import annotations
import hashlib
from pathlib import Path


def sha256(path: str | Path, chunk_size: int = 1 << 20) -> str:
    """Return the hex SHA-256 digest of a file."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest()


def hash_files(paths: list[str | Path]) -> dict[str, str]:
    """Return {filename: sha256} for a list of paths."""
    return {Path(p).name: sha256(p) for p in paths}
