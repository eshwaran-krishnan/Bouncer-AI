"""
bouncer/agent/graph.py — Pipeline assembly.

Sequential pipeline using the Anthropic SDK directly.
No LangGraph — the QC workflow is a fixed linear graph, not a dynamic one.

Pipeline:
  classify_files → extract_data → assign_tags → cross_reference → generate_report
"""

from __future__ import annotations

import time

from bouncer.agent.state import BouncerState, initial_state
from bouncer.agent.nodes.classify import classify_files
from bouncer.agent.nodes.extract import extract_data
from bouncer.agent.nodes.tag import assign_tags
from bouncer.agent.nodes.cross_reference import cross_reference
from bouncer.agent.nodes.report import generate_report
from bouncer.config import load_schema, load_qc
from bouncer.utils.logger import get_logger, timer

log = get_logger("bouncer.graph")


def run(
    input_files: list[str],
    assay_type: str,
    schema_path: str,
    qc_path: str,
    mode: str = "strict",
) -> BouncerState:
    """
    Run the full Bouncer QC pipeline.

    Args:
        input_files:  Absolute paths to staged input files.
        assay_type:   e.g. "rna-seq", "flow-cytometry", "qpcr"
        schema_path:  Absolute path to the schema YAML contract.
        qc_path:      Absolute path to the QC YAML contract.
        mode:         "strict" | "permissive"

    Returns the final BouncerState with report, passed, and findings populated.
    """
    t_pipeline = time.perf_counter()
    log.info("pipeline_start",
             assay_type=assay_type,
             mode=mode,
             n_files=len(input_files),
             input_files=input_files,
             schema_path=schema_path,
             qc_path=qc_path)

    schema = load_schema(schema_path)
    qc     = load_qc(qc_path)

    state = initial_state(
        input_files=input_files,
        assay_type=assay_type,
        schema_contract=schema.model_dump(),
        qc_contract=qc.model_dump(),
        mode=mode,
    )

    with timer(log, "classify"):
        state = classify_files(state)

    with timer(log, "extract"):
        state = extract_data(state)

    with timer(log, "tag"):
        state = assign_tags(state)

    with timer(log, "cross_reference"):
        state = cross_reference(state)

    with timer(log, "report"):
        state = generate_report(state)

    elapsed_ms = round((time.perf_counter() - t_pipeline) * 1000)
    findings   = state["findings"]
    by_sev     = {"hard": 0, "soft": 0, "warning": 0}
    for f in findings:
        by_sev[f.get("severity", "warning")] = by_sev.get(f.get("severity", "warning"), 0) + 1

    log.info("pipeline_end",
             passed=state["passed"],
             mode=mode,
             elapsed_ms=elapsed_ms,
             n_findings_hard=by_sev["hard"],
             n_findings_soft=by_sev["soft"],
             n_findings_warning=by_sev["warning"],
             tags=state.get("tags", {}))

    return state
