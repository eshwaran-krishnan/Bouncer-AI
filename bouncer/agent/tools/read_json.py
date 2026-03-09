"""
read_json — reads JSON files.

Handles generic JSON and MultiQC-specific structure extraction.
For MultiQC files, surfaces the general_stats metrics per sample
directly so the agent doesn't need to navigate nested keys manually.
"""


def read_json(path: str, keys: list[str] | None = None) -> dict:
    """
    Read a JSON file and return a structured summary.

    For MultiQC JSON (detected automatically), extracts:
      - per-sample general_stats metrics
      - list of tools/modules present

    Args:
        path: Absolute path to the JSON file.
        keys: Optional list of top-level keys to extract. If None,
              returns full top-level key list + smart MultiQC extraction.

    Returns dict with content summary or MultiQC-specific structure.
    """
    import json

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        return {"path": path, "error": str(e)}

    if not isinstance(data, dict):
        return {
            "path": path,
            "type": type(data).__name__,
            "preview": str(data)[:3000],
        }

    top_keys = list(data.keys())

    # Explicit key extraction
    if keys:
        return {
            "path": path,
            "extracted_keys": {k: data[k] for k in keys if k in data},
            "missing_keys": [k for k in keys if k not in data],
        }

    # ── MultiQC detection ─────────────────────────────────────────────────────
    if "report_general_stats_data" in data:
        general_stats_data = data["report_general_stats_data"]

        # report_general_stats_data can be:
        #   - dict:  {tool_name: {sample: {metric: value}}}  (MultiQC ≥ 1.14)
        #   - list:  [{sample: {metric: value}}, ...]        (older MultiQC)
        if isinstance(general_stats_data, dict):
            sections = general_stats_data.values()
        else:
            sections = general_stats_data

        # Collect all samples and all metrics across all tool sections
        all_samples: set[str] = set()
        all_metrics: set[str] = set()
        per_sample: dict[str, dict] = {}

        # When iterating a dict we have the tool name; use it as prefix
        # so metrics from different tools don't silently overwrite each other.
        # e.g. star tool → "star_uniquely_mapped_percent"
        if isinstance(general_stats_data, dict):
            tool_sections = general_stats_data.items()
        else:
            tool_sections = ((None, s) for s in general_stats_data)

        for tool_name, section in tool_sections:
            if not isinstance(section, dict):
                continue
            for sample, metrics in section.items():
                if not isinstance(metrics, dict):
                    continue
                all_samples.add(sample)
                prefixed = (
                    {f"{tool_name}_{k}": v for k, v in metrics.items()}
                    if tool_name else metrics
                )
                all_metrics.update(prefixed.keys())
                per_sample.setdefault(sample, {}).update(prefixed)

        sample_list = sorted(all_samples)

        return {
            "path": path,
            "format": "multiqc",
            "multiqc_version": data.get("report_multiqc_version", "unknown"),
            "n_samples": len(sample_list),
            "samples": sample_list,
            "metrics_available": sorted(all_metrics),
            "general_stats": per_sample,              # full per-sample metrics
            "top_keys": top_keys,
        }

    # ── Generic JSON ──────────────────────────────────────────────────────────
    return {
        "path": path,
        "format": "generic_json",
        "top_keys": top_keys,
        "preview": {k: str(v)[:500] for k, v in list(data.items())[:10]},
    }
