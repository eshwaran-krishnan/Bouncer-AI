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

    # ── Holistic scientist review ──────────────────────────────────────────────
    # Claude reads ALL documents together — protocol full text, every samplesheet
    # row, MultiQC per-sample stats, counts summary — and flags anomalies the
    # way a senior scientist would when reviewing an experiment end-to-end.
    if samplesheet_df is not None:
        findings.extend(
            _scientist_review(extracted, samplesheet_df, schema, state["assay_type"])
        )

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


_SCIENTIST_REVIEW_TOOL = {
    "name": "flag_experiment_anomalies",
    "description": (
        "Flag every anomaly, inconsistency, or suspicious value found by reading "
        "all experiment documents together. Report each finding individually so "
        "each affected sample gets its own entry."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "findings": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "severity": {
                            "type": "string",
                            "enum": ["hard", "soft", "warning"],
                            "description": (
                                "hard: clear factual error or contradiction that should block "
                                "registration (wrong organism name, concentration never in protocol, "
                                "impossible value). "
                                "soft: likely error requiring investigation before trusting the data. "
                                "warning: noteworthy deviation that may be intentional."
                            ),
                        },
                        "check": {
                            "type": "string",
                            "description": "snake_case name for this check, e.g. organism_mismatch, passage_out_of_range",
                        },
                        "field": {
                            "type": "string",
                            "description": "The samplesheet column or data field involved",
                        },
                        "sample": {
                            "type": "string",
                            "description": "sample_id of the affected sample, or omit for dataset-level issues",
                        },
                        "expected": {
                            "type": "string",
                            "description": "What the protocol / schema / other documents specify",
                        },
                        "found": {
                            "type": "string",
                            "description": "What the samplesheet or data actually contains",
                        },
                        "source_documents": {
                            "type": "string",
                            "description": "Which documents informed this finding, e.g. 'protocol + samplesheet'",
                        },
                        "message": {
                            "type": "string",
                            "description": "Clear explanation of the anomaly and why it matters scientifically",
                        },
                    },
                    "required": ["severity", "check", "message"],
                },
            }
        },
        "required": ["findings"],
    },
}


def _scientist_review(
    extracted: dict,
    samplesheet_df: pd.DataFrame,
    schema: SchemaContract,
    assay_type: str,
) -> list[Finding]:
    """
    Holistic scientist review using already-extracted data from each file.

    Each file was already read independently by the extract node (PDF → full_text,
    MultiQC → general_stats, counts → shape/columns). This function assembles
    those pre-extracted pieces into a single prompt — no file re-reading.

    Catches things deterministic checks miss:
      - Samples with wrong organism (e.g. human cell line listed as mouse)
      - Strandedness contradicting the protocol's dUTP / forward declaration
      - A single sample's RIN score contradicting what the protocol recorded
      - Empty required fields on specific samples
      - Condition labels inconsistent with the protocol design
      - Library type contradicting protocol
      - Experiment ID mismatches between protocol and samplesheet
    """
    # ── 1. Protocol text (already extracted by read_pdf in extract node) ───────
    protocol_data = extracted.get("protocol_document")
    protocol_text = ""
    has_protocol  = False

    if protocol_data and not protocol_data.get("error"):
        protocol_text = protocol_data.get("full_text", "")
        if protocol_text:
            has_protocol = True
            log.info("scientist_review_protocol_attached",
                     chars=len(protocol_text))

    # ── 2. Full samplesheet — every row ───────────────────────────────────────
    sheet_csv = samplesheet_df.to_csv(index=False)

    # ── 3. Schema column definitions ──────────────────────────────────────────
    col_defs = "\n".join(
        f"  {c.name}: dtype={c.dtype}, required={c.required}"
        + (f", allowed_values={c.allowed_values}" if c.allowed_values else "")
        for c in schema.metadata_columns
    )

    # ── 4. MultiQC per-sample stats (already parsed by read_json) ─────────────
    mq_data = extracted.get("multiqc_json")
    multiqc_section = "Not provided."
    if mq_data and not mq_data.get("error"):
        stats = mq_data.get("general_stats") or {}
        multiqc_section = json.dumps(stats, indent=2, default=str)[:3000]

    # ── 5. Counts matrix shape + sample IDs (already parsed by read_csv) ──────
    counts_data = extracted.get("counts_matrix")
    counts_section = "Not provided."
    if counts_data and not counts_data.get("error"):
        counts_section = (
            f"Shape: {counts_data.get('shape', 'unknown')} (genes × samples)\n"
            f"Sample IDs in matrix: {counts_data.get('columns', [])}"
        )

    # ── 6. Build the prompt ───────────────────────────────────────────────────
    protocol_note = (
        "The experiment protocol is provided below. Read it carefully before auditing the samplesheet."
        if has_protocol
        else "No protocol document was provided. Flag anomalies based on biological knowledge and internal consistency."
    )
    protocol_section = (
        f"\n=== EXPERIMENT PROTOCOL ===\n{protocol_text[:12000]}\n"
        if protocol_text
        else ""
    )

    prompt = f"""You are a senior research scientist and data quality auditor for a {assay_type} experiment.

{protocol_note}

Your job: read EVERY section below and flag EVERY anomaly, inconsistency, or error — especially samplesheet values that contradict what the protocol specifies.

Rules:
- Name the SPECIFIC sample_id for every per-sample issue. Never say "some samples".
- Flag empty/null values in required fields as hard findings.
- Flag any field value that contradicts the protocol as hard (e.g. wrong organism, wrong strandedness, RIN mismatch, wrong experiment ID).
- Flag biologically implausible values even without a protocol (e.g. a human cell line listed as mouse).
- Be exhaustive — list every issue you find.
{protocol_section}
=== SCHEMA: COLUMN DEFINITIONS ===
{col_defs}

=== SAMPLESHEET (full, all rows) ===
{sheet_csv}

=== MULTIQC PER-SAMPLE STATISTICS ===
{multiqc_section}

=== COUNTS MATRIX ===
{counts_section}

Call flag_experiment_anomalies with every finding you identify.
If nothing is wrong, call it with an empty list."""

    # ── 7. Call Claude ────────────────────────────────────────────────────────
    client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
    try:
        response = client.messages.create(
            model=MODEL,
            max_tokens=8192,
            tools=[_SCIENTIST_REVIEW_TOOL],
            tool_choice={"type": "tool", "name": "flag_experiment_anomalies"},
            messages=[{"role": "user", "content": prompt}],
        )
    except Exception as exc:
        log.error("scientist_review_failed", error=str(exc), exc_info=True)
        return [Finding(
            severity="warning",
            stage="protocol",
            check="scientist_review_unavailable",
            message=f"Holistic scientist review could not complete: {exc}. "
                    "Manual review of the protocol against the samplesheet is required.",
        )]

    log.info("anthropic_api_call",
             node="scientist_review",
             model=MODEL,
             input_tokens=response.usage.input_tokens,
             output_tokens=response.usage.output_tokens,
             stop_reason=response.stop_reason,
             has_protocol=has_protocol,
             has_multiqc=bool(mq_data))

    # If we hit the output token limit the tool JSON is truncated and unparseable
    if response.stop_reason == "max_tokens":
        log.error("scientist_review_truncated",
                  output_tokens=response.usage.output_tokens)
        return [Finding(
            severity="warning",
            stage="protocol",
            check="scientist_review_truncated",
            message="Scientist review response was truncated (max output tokens reached). "
                    "Some protocol inconsistencies may not have been reported. "
                    "Review the protocol against the samplesheet manually.",
        )]

    tool_block = next(
        (b for b in response.content if b.type == "tool_use"), None
    )
    if not tool_block:
        return []

    raw_findings = tool_block.input.get("findings", [])
    log.info("scientist_review_complete", n_findings=len(raw_findings))

    findings: list[Finding] = []
    for f in raw_findings:
        findings.append(Finding(
            severity=f.get("severity", "warning"),
            stage="protocol",
            check=f.get("check", "scientist_review"),
            field=f.get("field"),
            sample=f.get("sample"),
            expected=f.get("expected"),
            found=f.get("found"),
            message=f.get("message", ""),
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
