"""
bouncer/agent/nodes/extract.py — Data extraction node.

Calls the appropriate reader tool for each classified file. For protocol
PDFs, uses Claude to pull structured parameters from free text.
All other formats are parsed deterministically.
"""

from __future__ import annotations

import json
import os
import anthropic

from bouncer.agent.state import BouncerState, FileInfo
from bouncer.agent.tools.read_csv import read_csv
from bouncer.agent.tools.read_json import read_json
from bouncer.agent.tools.read_pdf import read_pdf
from bouncer.agent.tools.read_fcs import read_fcs
from bouncer.agent.tools.read_eds import read_eds
from bouncer.utils.logger import get_logger

log = get_logger("bouncer.extract")
MODEL = "claude-sonnet-4-6"

_READERS = {
    "read_json": read_json,
    "read_csv":  read_csv,
    "read_pdf":  read_pdf,
    "read_fcs":  read_fcs,
    "read_eds":  read_eds,
}


def extract_data(state: BouncerState) -> BouncerState:
    """
    Extract content from each file in file_map.

    Updates state["extracted_data"] keyed by file_type.
    For protocol PDFs, runs a follow-up Claude call to extract
    structured parameters from the raw text.
    """
    extracted: dict = {}

    for info in state["file_map"]:
        if info["read_strategy"] == "skip" or info["file_type"] == "unknown":
            continue

        reader = _READERS.get(info["read_strategy"])
        if reader is None:
            continue

        try:
            result = reader(info["path"])
            # Log key shape info without dumping the whole content
            _log_extraction(info["path"], info["file_type"], result)
        except Exception as e:
            log.error("extraction_failed",
                      path=info["path"],
                      file_type=info["file_type"],
                      error=str(e),
                      exc_info=True)
            result = {"path": info["path"], "error": str(e)}

        file_type = info["file_type"]

        # For protocol documents, add structured parameter extraction
        if file_type == "protocol_document" and "full_text" in result:
            result["structured_params"] = _extract_protocol_params(
                result["full_text"], state["assay_type"]
            )

        # Store by file_type; if multiple files of same type, append
        if file_type in extracted:
            if isinstance(extracted[file_type], list):
                extracted[file_type].append(result)
            else:
                extracted[file_type] = [extracted[file_type], result]
        else:
            extracted[file_type] = result

        # Update file_map content reference
        info["content"] = result

    state["extracted_data"] = extracted
    return state


def _extract_protocol_params(text: str, assay_type: str) -> dict:
    """
    Use Claude to extract structured experimental parameters from protocol text.
    Returns a dict of parameter name → value.
    """
    client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

    assay_hints = {
        "rna-seq": "passage range, RNA extraction kit, library prep kit, sequencing depth target, strandedness protocol, rRNA depletion method, treatment duration, treatment concentrations, timepoints, reagent lots",
        "flow-cytometry": "instrument model, panel antibodies with clones and concentrations, acquisition events target, gating strategy, fixation protocol, staining buffer, incubation times",
        "qpcr": "instrument model, PCR chemistry (SYBR/probe), reference genes, primer sequences or catalogue numbers, annealing temperature, cycle number, quantification method (ddCt/standard curve)",
    }
    hints = assay_hints.get(assay_type, "experimental conditions, instrument settings, reagent details, timepoints")

    prompt = f"""Extract structured experimental parameters from this {assay_type} protocol document.

Parameters to look for: {hints}

Return a JSON object where each key is the parameter name and the value is what was found.
Use null for parameters mentioned but without a specified value.
Only include parameters actually mentioned in the document — do not fabricate values.

Protocol text:
{text[:8000]}

Return only valid JSON, no explanation."""

    response = client.messages.create(
        model=MODEL,
        max_tokens=1024,
        messages=[{"role": "user", "content": prompt}],
    )

    log.info("anthropic_api_call",
             node="extract_protocol",
             model=MODEL,
             input_tokens=response.usage.input_tokens,
             output_tokens=response.usage.output_tokens)

    raw = response.content[0].text.strip()
    # Strip markdown code fences if present
    if raw.startswith("```"):
        raw = raw.split("\n", 1)[-1].rsplit("```", 1)[0]

    try:
        params = json.loads(raw)
        log.info("protocol_params_extracted", n_params=len(params), keys=list(params.keys()))
        return params
    except json.JSONDecodeError:
        log.warning("protocol_params_parse_error", raw_preview=raw[:200])
        return {"_parse_error": raw[:500]}


def _log_extraction(path: str, file_type: str, result: dict) -> None:
    """Log a concise summary of what was extracted from a file."""
    if "error" in result:
        log.error("extraction_error", path=path, file_type=file_type, error=result["error"])
        return
    ctx: dict = {"path": path, "file_type": file_type}
    if file_type == "counts_matrix":
        ctx["shape"] = f"{result.get('n_rows', '?')}x{result.get('n_cols', '?')}"
        ctx["n_samples"] = result.get("n_cols")
    elif file_type == "sample_sheet":
        ctx["n_rows"] = result.get("n_rows")
        ctx["columns"] = result.get("columns", [])
    elif file_type == "multiqc_json":
        gs = result.get("general_stats") or {}
        ctx["n_samples"] = len(gs)
        ctx["metrics_per_sample"] = len(next(iter(gs.values()), {}))
    elif file_type in ("fcs_file",):
        ctx["n_events"] = result.get("n_events")
        ctx["n_channels"] = result.get("n_channels")
    elif file_type in ("eds_file",):
        ctx["n_samples"] = result.get("n_samples")
        ctx["n_targets"] = result.get("n_targets")
    elif file_type == "protocol_document":
        ctx["n_pages"] = result.get("n_pages")
        ctx["text_length"] = len(result.get("full_text", ""))
    log.info("extraction_ok", **ctx)
