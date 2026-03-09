"""
bouncer/agent/nodes/cross_reference.py — QC validation node.

Runs all deterministic QC checks from the engine. Dispatches to the
right checks based on assay_type. Accumulates findings in state.
"""

from __future__ import annotations

import json
import os
import pandas as pd

import anthropic

from bouncer.agent.state import BouncerState
from bouncer.config import SchemaContract, QCContract
from bouncer.models.finding import Finding
from bouncer.qc.schema_validator import validate_schema
from bouncer.qc.metric_checker import check_multiqc_metrics, check_counts_matrix
from bouncer.qc.design_checker import check_design
from bouncer.utils.logger import get_logger

log = get_logger("bouncer.cross_reference")
MODEL = "claude-sonnet-4-6"


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

    # ── Protocol consistency (all assay types with protocol + samplesheet) ────
    protocol_data = extracted.get("protocol_document")
    if protocol_data and samplesheet_df is not None:
        findings.extend(
            _check_protocol_consistency(protocol_data, samplesheet_df, state["assay_type"])
        )
    elif protocol_data and samplesheet_df is None:
        log.warning("protocol_check_skipped", reason="no samplesheet to compare against")

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


def _check_protocol_consistency(
    protocol_data: dict,
    samplesheet_df: pd.DataFrame,
    assay_type: str,
) -> list[Finding]:
    """
    Use Claude to compare protocol structured parameters against samplesheet
    values and flag any sample-level or dataset-level discrepancies.

    Examples of what this catches:
      - Protocol specifies passage 3–7 but samplesheet has passage_number = 12
      - Protocol lists concentrations [1, 10] uM but samplesheet has 5 uM
      - Protocol states 24 h timepoint but samplesheet has timepoint_hr = 48
      - Protocol organism differs from samplesheet organism column
      - Protocol describes single-end library but samplesheet says paired-end
    """
    structured_params = protocol_data.get("structured_params") or {}
    if not structured_params or structured_params.get("_parse_error"):
        log.warning("protocol_check_skipped",
                    reason="structured_params missing or failed to parse")
        return []

    # Build a compact samplesheet summary (avoid huge token payloads)
    sheet_summary: dict = {}
    for col in samplesheet_df.columns:
        unique_vals = samplesheet_df[col].dropna().unique().tolist()
        if len(unique_vals) <= 20:
            sheet_summary[col] = unique_vals
        else:
            sheet_summary[col] = unique_vals[:20] + [f"... ({len(unique_vals)} total)"]

    # Per-sample view for numeric columns — useful for range/outlier checks
    numeric_cols = samplesheet_df.select_dtypes(include="number").columns.tolist()
    per_sample: dict = {}
    if "sample_id" in samplesheet_df.columns:
        id_col = "sample_id"
    else:
        id_col = samplesheet_df.columns[0]
    for col in numeric_cols[:10]:   # cap to avoid huge payloads
        per_sample[col] = samplesheet_df.set_index(id_col)[col].to_dict()

    _PROTOCOL_TOOL = {
        "name": "report_protocol_discrepancies",
        "description": (
            "Report discrepancies found between the protocol document and the samplesheet. "
            "Only report genuine inconsistencies — do not flag missing protocol fields "
            "unless they actively contradict samplesheet values."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "discrepancies": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "severity": {
                                "type": "string",
                                "enum": ["hard", "soft", "warning"],
                                "description": (
                                    "hard: clear factual contradiction (e.g. wrong species, "
                                    "out-of-range passage number, concentration not in protocol). "
                                    "soft: likely mismatch worth investigating. "
                                    "warning: minor or possibly intentional deviation."
                                ),
                            },
                            "check": {
                                "type": "string",
                                "description": "Short snake_case check name, e.g. passage_number_out_of_range",
                            },
                            "field": {
                                "type": "string",
                                "description": "Samplesheet column name that conflicts",
                            },
                            "sample": {
                                "type": "string",
                                "description": "Specific sample_id if the issue is per-sample, else omit",
                            },
                            "protocol_value": {
                                "type": "string",
                                "description": "What the protocol specifies",
                            },
                            "samplesheet_value": {
                                "type": "string",
                                "description": "What the samplesheet contains",
                            },
                            "message": {
                                "type": "string",
                                "description": "Human-readable explanation of the discrepancy",
                            },
                        },
                        "required": ["severity", "check", "message"],
                    },
                }
            },
            "required": ["discrepancies"],
        },
    }

    prompt = f"""You are a biological data quality auditor for a {assay_type} experiment.

Compare the PROTOCOL PARAMETERS (extracted from the experiment protocol PDF) against
the SAMPLESHEET VALUES (from the sample metadata file).

Flag any samples or dataset-level values that are inconsistent with what the
protocol specifies. Be precise — only flag genuine contradictions, not missing info.

PROTOCOL PARAMETERS (extracted from PDF):
{json.dumps(structured_params, indent=2, default=str)}

SAMPLESHEET COLUMN SUMMARIES (unique values per column):
{json.dumps(sheet_summary, indent=2, default=str)}

PER-SAMPLE NUMERIC VALUES:
{json.dumps(per_sample, indent=2, default=str)}

Call report_protocol_discrepancies with all discrepancies found.
If there are no discrepancies, call it with an empty list."""

    try:
        client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
        response = client.messages.create(
            model=MODEL,
            max_tokens=1024,
            tools=[_PROTOCOL_TOOL],
            tool_choice={"type": "tool", "name": "report_protocol_discrepancies"},
            messages=[{"role": "user", "content": prompt}],
        )
    except Exception as exc:
        log.error("protocol_check_failed", error=str(exc), exc_info=True)
        return []

    log.info("anthropic_api_call",
             node="protocol_check",
             model=MODEL,
             input_tokens=response.usage.input_tokens,
             output_tokens=response.usage.output_tokens)

    tool_block = next(
        (b for b in response.content if b.type == "tool_use"), None
    )
    if not tool_block:
        return []

    discrepancies = tool_block.input.get("discrepancies", [])
    log.info("protocol_discrepancies_found", n=len(discrepancies))

    findings: list[Finding] = []
    for d in discrepancies:
        expected = d.get("protocol_value")
        found    = d.get("samplesheet_value")
        findings.append(Finding(
            severity=d.get("severity", "warning"),
            stage="protocol",
            check=d.get("check", "protocol_mismatch"),
            field=d.get("field"),
            sample=d.get("sample"),
            expected=expected,
            found=found,
            message=d.get("message", ""),
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
