"""
bouncer/models/provenance.py — Provenance entry model.

Every registered AnnData object carries a provenance chain in .uns['provenance'].
Each entry records the exact inputs, versions, and pipeline used to produce it.
"""

from __future__ import annotations
from datetime import datetime
from pydantic import BaseModel, Field


class ProvenanceEntry(BaseModel):
    stage: str                             # "raw_counts", "tpm", "rpkm", etc.
    assay_type: str
    schema_version: str
    qc_version: str
    qc_mode: Literal["strict", "permissive"]
    input_hashes: dict[str, str]           # {filename: sha256}
    pipeline: str | None = None            # e.g. "nf-core/rnaseq"
    pipeline_version: str | None = None
    parent_feature_id: str | None = None   # set when derived via transform
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    bouncer_version: str = "0.1.0"


from typing import Literal  # noqa: E402 — after class to avoid circular at top
