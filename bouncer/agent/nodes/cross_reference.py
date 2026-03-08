"""
bouncer/agent/nodes/cross_reference.py — QC validation node.

Runs all deterministic QC checks from the engine. Dispatches to the
right checks based on assay_type. Accumulates findings in state.
"""

from __future__ import annotations

import pandas as pd

from bouncer.agent.state import BouncerState
from bouncer.config import SchemaContract, QCContract
from bouncer.models.finding import Finding
from bouncer.qc.schema_validator import validate_schema
from bouncer.qc.metric_checker import check_multiqc_metrics, check_counts_matrix
from bouncer.qc.design_checker import check_design
from bouncer.utils.logger import get_logger

log = get_logger("bouncer.cross_reference")


def cross_reference(state: BouncerState) -> BouncerState:
    """
    Run all QC checks appropriate for the assay type.
    Populates state["findings"] and state["missing_fields"].
    """
    schema    = SchemaContract(**state["schema_contract"])
    qc        = QCContract(**state["qc_contract"])
    extracted = state["extracted_data"]
    findings: list[Finding] = []
    missing: list[str] = []

    samplesheet_df = _load_samplesheet(extracted)
    counts_df      = _load_counts(extracted)
    multiqc_stats  = _load_multiqc(extracted)

    log.info("cross_reference_inputs",
             has_samplesheet=samplesheet_df is not None,
             has_counts=counts_df is not None,
             has_multiqc=multiqc_stats is not None,
             counts_shape=f"{counts_df.shape[0]}x{counts_df.shape[1]}" if counts_df is not None else None,
             n_samples_samplesheet=len(samplesheet_df) if samplesheet_df is not None else None,
             n_multiqc_samples=len(multiqc_stats) if multiqc_stats else None)

    # ── Schema validation (all assay types) ───────────────────────────────────
    if samplesheet_df is not None:
        counts_columns = list(counts_df.columns) if counts_df is not None else None
        findings.extend(validate_schema(samplesheet_df, schema, counts_columns))
        for col in schema.required_columns():
            if col not in samplesheet_df.columns:
                missing.append(col)
    else:
        findings.append(Finding(
            severity="hard",
            stage="schema",
            check="samplesheet_missing",
            message="No samplesheet found in input files. Schema and design checks skipped.",
        ))

    assay = state["assay_type"]

    # ── RNA-seq ───────────────────────────────────────────────────────────────
    if assay == "rna-seq":
        if multiqc_stats:
            findings.extend(check_multiqc_metrics(multiqc_stats, qc))
        else:
            findings.append(Finding(
                severity="soft", stage="multiqc", check="multiqc_missing",
                message="No MultiQC JSON found. Metric checks skipped.",
            ))
        if counts_df is not None:
            findings.extend(check_counts_matrix(counts_df, qc))
        else:
            findings.append(Finding(
                severity="soft", stage="counts_matrix", check="counts_matrix_missing",
                message="No counts matrix found. Counts integrity checks skipped.",
            ))

    # ── Flow cytometry ────────────────────────────────────────────────────────
    elif assay == "flow-cytometry":
        fcs_data = extracted.get("fcs_file")
        if fcs_data:
            findings.extend(_check_fcs(fcs_data, qc))

    # ── qPCR ──────────────────────────────────────────────────────────────────
    elif assay == "qpcr":
        eds_data = extracted.get("eds_file")
        if eds_data:
            findings.extend(_check_eds(eds_data, qc))

    # ── Design checks (all assay types with a samplesheet) ────────────────────
    if samplesheet_df is not None:
        findings.extend(check_design(samplesheet_df, qc, multiqc_stats))

    by_sev = {"hard": 0, "soft": 0, "warning": 0}
    for f in findings:
        by_sev[f.severity] = by_sev.get(f.severity, 0) + 1
    log.info("cross_reference_complete",
             n_findings_hard=by_sev["hard"],
             n_findings_soft=by_sev["soft"],
             n_findings_warning=by_sev["warning"],
             missing_fields=missing)
    for f in findings:
        lvl = "error" if f.severity == "hard" else ("warning" if f.severity == "soft" else "info")
        getattr(log, lvl)("finding",
                          severity=f.severity,
                          stage=f.stage,
                          check=f.check,
                          sample=f.sample,
                          detail=f.message)

    state["findings"]       = [f.model_dump() for f in findings]
    state["missing_fields"] = missing
    return state


# ── Helpers ────────────────────────────────────────────────────────────────────

def _load_samplesheet(extracted: dict) -> pd.DataFrame | None:
    data = extracted.get("sample_sheet")
    if not data or "error" in data:
        return None
    try:
        return pd.read_csv(data["path"], sep=data.get("separator_used", ","))
    except Exception:
        return None


def _load_counts(extracted: dict) -> pd.DataFrame | None:
    data = extracted.get("counts_matrix")
    if not data or "error" in data:
        return None
    try:
        df = pd.read_csv(data["path"], sep=data.get("separator_used", "\t"), index_col=0)
        # Drop any non-numeric annotation columns (e.g. gene_name in salmon output)
        return df.select_dtypes(include="number")
    except Exception:
        return None


def _load_multiqc(extracted: dict) -> dict | None:
    data = extracted.get("multiqc_json")
    if not data or "error" in data:
        return None
    return data.get("general_stats")


def _check_fcs(fcs_data: dict, qc: QCContract) -> list[Finding]:
    findings: list[Finding] = []
    n_events = fcs_data.get("n_events", 0)
    if n_events == 0:
        findings.append(Finding(
            severity="hard", stage="counts_matrix", check="fcs_no_events",
            message="FCS file contains 0 events. File may be corrupt or empty.",
        ))
    elif n_events < 1000:
        findings.append(Finding(
            severity="soft", stage="counts_matrix", check="fcs_low_events",
            found=str(n_events), expected=">= 1000",
            message=f"Only {n_events:,} events acquired. Low event counts reduce gating reliability.",
        ))
    return findings


def _check_eds(eds_data: dict, qc: QCContract) -> list[Finding]:
    findings: list[Finding] = []
    n_samples = eds_data.get("n_samples", 0)
    n_targets = eds_data.get("n_targets", 0)
    if n_samples == 0:
        findings.append(Finding(
            severity="hard", stage="counts_matrix", check="eds_no_samples",
            message="No samples found in EDS file.",
        ))
    if n_targets == 0:
        findings.append(Finding(
            severity="hard", stage="counts_matrix", check="eds_no_targets",
            message="No assay targets found in EDS file.",
        ))
    for rf in eds_data.get("result_files", []):
        n_undet = (rf.get("ct_summary") or {}).get("n_undetermined") or 0
        total   = max(n_samples * n_targets, 1)
        if n_undet / total > 0.3:
            findings.append(Finding(
                severity="soft", stage="counts_matrix", check="eds_high_undetermined",
                found=f"{n_undet} undetermined wells",
                message=f"{n_undet} undetermined CT values (> 30% of wells). "
                        "May indicate assay or primer failure.",
            ))
    return findings
