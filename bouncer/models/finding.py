"""
bouncer/models/finding.py — Finding and Report Pydantic models.
"""

from __future__ import annotations
from typing import Literal
from pydantic import BaseModel


class Finding(BaseModel):
    severity: Literal["hard", "soft", "warning"]
    stage: Literal["schema", "multiqc", "counts_matrix", "metadata", "design", "protocol"]
    check: str                    # rule/check name, e.g. "STAR_percent_uniquely_mapped"
    source_file: str | None = None
    sample: str | None = None     # which sample failed (None = dataset-level)
    field: str | None = None      # column or metric name
    expected: str | None = None
    found: str | None = None
    message: str = ""

    def is_blocking(self, mode: Literal["strict", "permissive"] = "strict") -> bool:
        """
        In strict mode: hard findings block.
        In permissive mode: nothing blocks (all findings are advisory).
        """
        if mode == "permissive":
            return False
        return self.severity == "hard"
