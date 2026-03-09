"""
bouncer/agent/state.py — BouncerState TypedDict passed through the pipeline.
"""

from __future__ import annotations
from typing import Literal, TypedDict


class FileInfo(TypedDict):
    path: str
    file_type: Literal[
        "multiqc_json",
        "counts_matrix",
        "sample_sheet",
        "protocol_document",
        "fcs_file",
        "eds_file",
        "unknown",
    ]
    read_strategy: Literal[
        "read_json",
        "read_csv",
        "read_pdf",
        "read_fcs",
        "read_eds",
        "read_yaml",
        "skip",
    ]
    content: dict | None   # populated after extract node runs


class BouncerState(TypedDict):
    # ── Inputs ────────────────────────────────────────────────────────────────
    input_files: list[str]        # absolute paths to staged files
    assay_type: str               # "rna-seq" | "flow-cytometry" | "qpcr"
    schema_contract: dict         # raw dict from SchemaContract.model_dump()
    qc_contract: dict             # raw dict from QCContract.model_dump()
    mode: Literal["strict", "permissive"]

    # ── Classification ────────────────────────────────────────────────────────
    file_map: list[FileInfo]      # populated by classify node

    # ── Extraction ────────────────────────────────────────────────────────────
    extracted_data: dict          # keyed by file_type, value = reader output

    # ── Tagging ───────────────────────────────────────────────────────────────
    tags: dict                    # {organism, conditions, treatments, ...}

    # ── QC ────────────────────────────────────────────────────────────────────
    findings: list[dict]          # list of Finding.model_dump()
    missing_fields: list[str]     # required schema fields not found in any file

    # ── Output ────────────────────────────────────────────────────────────────
    report: str
    passed: bool
    feature_id: str | None        # set after successful registration


def initial_state(
    input_files: list[str],
    assay_type: str,
    schema_contract: dict,
    qc_contract: dict,
    mode: Literal["strict", "permissive"] = "strict",
) -> BouncerState:
    return BouncerState(
        input_files=input_files,
        assay_type=assay_type,
        schema_contract=schema_contract,
        qc_contract=qc_contract,
        mode=mode,
        file_map=[],
        extracted_data={},
        tags={},
        findings=[],
        missing_fields=[],
        report="",
        passed=False,
        feature_id=None,
    )
