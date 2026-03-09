"""
bouncer/agent/nodes/classify.py — File classification node.

Uses the Anthropic SDK with tool_use to classify each input file into
its type (counts_matrix, sample_sheet, multiqc_json, fcs_file, etc.)
based on peek_file output. This is the only LLM call needed for
classification — all structured data extraction is deterministic.
"""

from __future__ import annotations

import json
import os
import anthropic

from bouncer.agent.state import BouncerState, FileInfo
from bouncer.agent.tools.peek_file import peek_file
from bouncer.utils.logger import get_logger

log = get_logger("bouncer.classify")
MODEL = "claude-sonnet-4-6"

_CLASSIFY_TOOL = {
    "name": "classify_files",
    "description": (
        "Classify each input file into its biological data type and specify "
        "which reader tool to use for extraction."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "classifications": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "path": {"type": "string"},
                        "file_type": {
                            "type": "string",
                            "enum": [
                                "multiqc_json",
                                "counts_matrix",
                                "sample_sheet",
                                "protocol_document",
                                "fcs_file",
                                "eds_file",
                                "unknown",
                            ],
                        },
                        "read_strategy": {
                            "type": "string",
                            "enum": ["read_json", "read_csv", "read_pdf",
                                     "read_fcs", "read_eds", "read_yaml", "skip"],
                        },
                        "reasoning": {"type": "string"},
                    },
                    "required": ["path", "file_type", "read_strategy"],
                },
            }
        },
        "required": ["classifications"],
    },
}


def classify_files(state: BouncerState) -> BouncerState:
    """
    Peek at each input file and use Claude to classify it.

    Updates state["file_map"] with FileInfo entries.
    """
    previews: list[dict] = []
    for path in state["input_files"]:
        preview = peek_file(path)
        previews.append({"path": path, "preview": preview})

    assay_type = state["assay_type"]
    previews_text = json.dumps(previews, indent=2)

    prompt = f"""You are classifying input files for a biological data QC pipeline.

Assay type: {assay_type}

For each file below, determine:
1. file_type — what kind of biological data it contains
2. read_strategy — which parser to use

File type reference:
- multiqc_json    → MultiQC general_stats JSON output (detected_format: multiqc or json)
- counts_matrix   → Gene expression count matrix (TSV/CSV, genes × samples)
- sample_sheet    → Sample metadata table (CSV with sample_id, condition, etc.)
- protocol_document → Experimental protocol (PDF)
- fcs_file        → Flow cytometry FCS binary file (detected_format: fcs)
- eds_file        → QuantStudio qPCR EDS file (detected_format: eds_quantstudio)
- unknown         → Cannot be classified

Read strategy reference:
- read_json → for multiqc_json
- read_csv  → for counts_matrix, sample_sheet
- read_pdf  → for protocol_document
- read_fcs  → for fcs_file
- read_eds  → for eds_file
- skip      → for unknown

File previews:
{previews_text}

Call classify_files with your classifications for all {len(previews)} files."""

    client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
    response = client.messages.create(
        model=MODEL,
        max_tokens=1024,
        tools=[_CLASSIFY_TOOL],
        tool_choice={"type": "tool", "name": "classify_files"},
        messages=[{"role": "user", "content": prompt}],
    )

    log.info("anthropic_api_call",
             node="classify",
             model=MODEL,
             input_tokens=response.usage.input_tokens,
             output_tokens=response.usage.output_tokens,
             stop_reason=response.stop_reason)

    # Extract tool use block
    tool_block = next(
        (b for b in response.content if b.type == "tool_use"),
        None,
    )
    if tool_block is None:
        log.warning("classify_no_tool_block", n_files=len(state["input_files"]),
                    message="Claude returned no tool_use block; marking all files as unknown")
        state["file_map"] = [
            FileInfo(path=p, file_type="unknown", read_strategy="skip", content=None)
            for p in state["input_files"]
        ]
        return state

    classifications = tool_block.input.get("classifications", [])
    path_to_class = {c["path"]: c for c in classifications}

    file_map: list[FileInfo] = []
    for path in state["input_files"]:
        c = path_to_class.get(path, {})
        file_type     = c.get("file_type", "unknown")
        read_strategy = c.get("read_strategy", "skip")
        log.info("file_classified",
                 path=path,
                 file_type=file_type,
                 read_strategy=read_strategy,
                 reasoning=c.get("reasoning", ""))
        file_map.append(FileInfo(
            path=path,
            file_type=file_type,
            read_strategy=read_strategy,
            content=None,
        ))

    state["file_map"] = file_map
    return state
