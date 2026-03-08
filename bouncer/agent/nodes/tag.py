"""
bouncer/agent/nodes/tag.py — Dynamic biological tag assignment.

Uses Claude to read all extracted data and assign structured biological
tags: organism, conditions, treatments, cell lines, sample count, etc.
These tags are stored in AnnData.uns and in DuckDB for filtering.
"""

from __future__ import annotations

import json
import os
import anthropic

from bouncer.agent.state import BouncerState
from bouncer.utils.logger import get_logger

log = get_logger("bouncer.tag")
MODEL = "claude-sonnet-4-6"

_TAG_TOOL = {
    "name": "assign_tags",
    "description": "Assign biological metadata tags from the extracted experiment data.",
    "input_schema": {
        "type": "object",
        "properties": {
            "assay_type":      {"type": "string"},
            "organism":        {"type": "string", "description": "Species name, e.g. 'Homo sapiens'"},
            "tissue":          {"type": "string", "description": "Tissue or organ, e.g. 'liver', 'PBMC'"},
            "conditions":      {"type": "array", "items": {"type": "string"}, "description": "Unique condition labels"},
            "treatments":      {"type": "array", "items": {"type": "string"}, "description": "Perturbations applied"},
            "cell_lines":      {"type": "array", "items": {"type": "string"}},
            "sample_ids":      {"type": "array", "items": {"type": "string"}},
            "n_samples":       {"type": "integer"},
            "n_conditions":    {"type": "integer"},
            "pipeline":        {"type": "string", "description": "Bioinformatics pipeline, e.g. 'nf-core/rnaseq'"},
            "data_stage":      {"type": "string", "description": "e.g. 'raw_counts', 'tpm'"},
        },
        "required": ["assay_type", "organism", "conditions", "sample_ids", "n_samples"],
    },
}


def assign_tags(state: BouncerState) -> BouncerState:
    """
    Read extracted_data and ask Claude to assign biological tags.
    Updates state["tags"].
    """
    extracted = state["extracted_data"]

    # Build a compact summary of what was extracted (avoid huge payloads)
    summary: dict = {}
    for file_type, content in extracted.items():
        if isinstance(content, dict):
            summary[file_type] = _summarise(content)
        elif isinstance(content, list):
            summary[file_type] = [_summarise(c) for c in content]

    prompt = f"""You are assigning biological metadata tags for a {state['assay_type']} dataset.

Extract and assign tags from the experimental data below.
Focus on: organism, tissue, experimental conditions, treatments, sample IDs,
cell lines, pipeline used, and data stage.

If a field is not determinable from the data, omit it or use an empty list.
Do not guess — only report what is explicitly present.

Extracted data summary:
{json.dumps(summary, indent=2, default=str)[:6000]}

Call assign_tags with the biological tags for this experiment."""

    client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
    response = client.messages.create(
        model=MODEL,
        max_tokens=512,
        tools=[_TAG_TOOL],
        tool_choice={"type": "tool", "name": "assign_tags"},
        messages=[{"role": "user", "content": prompt}],
    )

    log.info("anthropic_api_call",
             node="tag",
             model=MODEL,
             input_tokens=response.usage.input_tokens,
             output_tokens=response.usage.output_tokens,
             stop_reason=response.stop_reason)

    tool_block = next(
        (b for b in response.content if b.type == "tool_use"), None
    )
    tags = tool_block.input if tool_block else {}
    log.info("tags_assigned",
             organism=tags.get("organism"),
             assay_type=tags.get("assay_type"),
             n_samples=tags.get("n_samples"),
             n_conditions=tags.get("n_conditions"),
             conditions=tags.get("conditions"),
             data_stage=tags.get("data_stage"))
    state["tags"] = tags
    return state


def _summarise(content: dict, max_keys: int = 20) -> dict:
    """Return a trimmed version of a content dict for the prompt."""
    out = {}
    for k, v in list(content.items())[:max_keys]:
        if isinstance(v, list) and len(v) > 10:
            out[k] = v[:10] + [f"... ({len(v)} total)"]
        elif isinstance(v, dict) and len(v) > 15:
            out[k] = {kk: vv for kk, vv in list(v.items())[:15]}
        elif isinstance(v, str) and len(v) > 500:
            out[k] = v[:500] + "..."
        else:
            out[k] = v
    return out
