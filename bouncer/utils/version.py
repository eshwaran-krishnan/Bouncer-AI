"""
bouncer/utils/version.py — Semantic version parsing and comparison.
"""

from __future__ import annotations
from packaging.version import Version, InvalidVersion


def parse(version_str: str) -> Version:
    """Parse a semver string. Raises ValueError on invalid format."""
    try:
        return Version(version_str)
    except InvalidVersion as e:
        raise ValueError(f"Invalid version string '{version_str}': {e}") from e


def is_compatible(contract_version: str, supported_version: str) -> bool:
    """
    Returns True if contract_version is compatible with (≤) supported_version.
    Major version bumps are considered breaking.
    """
    cv = parse(contract_version)
    sv = parse(supported_version)
    return cv.major == sv.major and cv <= sv
