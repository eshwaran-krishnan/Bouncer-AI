"""
read_fcs — reads FCS (Flow Cytometry Standard) binary files.

FCS is the standard binary format for flow cytometry and mass
cytometry (CyTOF) instruments. Contains:
  - Header: keyword-value pairs (instrument, channels, dates, etc.)
  - DATA segment: one row per event (cell), one column per channel

Used by the agent's extract node to surface:
  - Channel panel (what markers were measured)
  - Event count (number of cells)
  - Key acquisition metadata
  - Sample rows for sanity checks
"""


def read_fcs(path: str, meta_only: bool = False, n_sample_events: int = 5) -> dict:
    """
    Read an FCS file. Returns metadata and optionally a sample of events.

    Args:
        path:            Absolute path to the .fcs file.
        meta_only:       If True, parse header only — no event data loaded.
                         Faster for large files when only panel/metadata needed.
        n_sample_events: Number of events to include in sample_events.

    Returns dict with:
        n_events, n_channels, channels (list), instrument, date,
        acquisition_metadata, and optionally sample_events (list of dicts)
    """
    try:
        import fcsparser
    except ImportError:
        return {"path": path, "error": "fcsparser not installed. Add it to the Modal image."}

    try:
        meta, data = fcsparser.parse(
            path,
            meta_data_only=meta_only,
            reformat_meta=True,
        )
    except Exception as e:
        return {"path": path, "error": str(e)}

    n_par = int(meta.get("$PAR", 0))

    # Extract per-channel panel info
    channels = []
    for i in range(1, n_par + 1):
        channels.append({
            "index": i,
            "short_name": meta.get(f"$P{i}N", f"P{i}"),      # e.g. "BV421-A"
            "long_name":  meta.get(f"$P{i}S", ""),            # e.g. "CD3"
            "range":      meta.get(f"$P{i}R", None),
        })

    result = {
        "path": path,
        "format": "fcs",
        "fcs_version": meta.get("__header__", {}).get("version", "unknown"),
        "n_events": int(meta.get("$TOT", 0)),
        "n_channels": n_par,
        "channels": channels,
        # Key acquisition metadata
        "instrument": meta.get("$CYT", meta.get("CYTOMETER", "unknown")),
        "date": meta.get("$DATE", None),
        "acquisition_time": meta.get("$BTIM", None),
        "sample_id_meta": meta.get("$SRC", meta.get("SAMPLE ID", None)),
        "operator": meta.get("$OP", None),
        "institution": meta.get("$INST", None),
        "total_events_acquired": meta.get("$ABRT", None),
    }

    if not meta_only and data is not None:
        result["shape"] = list(data.shape)
        result["column_names"] = list(data.columns)
        result["sample_events"] = data.head(n_sample_events).to_dict(orient="records")

    return result
