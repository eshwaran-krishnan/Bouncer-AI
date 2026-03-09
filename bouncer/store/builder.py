"""
bouncer/store/builder.py — Build AnnData from a validated BouncerState.

The extract node stores only metadata summaries (shape, columns, sample rows),
not full DataFrames. This module re-reads the original staged files to
construct the AnnData object for registration.

Supported assays:
  rna-seq          — counts matrix (genes × samples) + samplesheet
  flow-cytometry   — FCS file + samplesheet  (h5ad, events as obs)
  qpcr             — EDS/CSV results + samplesheet
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import anndata as ad
    from bouncer.agent.state import BouncerState


def build_adata(state: "BouncerState") -> "ad.AnnData | None":
    """
    Build an AnnData object from the pipeline state.

    Dispatches to an assay-specific builder based on state["assay_type"].
    Returns None if the required source files are missing or unreadable —
    the caller decides whether to treat that as a hard error.
    """
    assay = state.get("assay_type", "")

    if assay == "rna-seq":
        return _build_rnaseq(state)
    if assay == "flow-cytometry":
        return _build_flow(state)
    if assay == "qpcr":
        return _build_qpcr(state)

    # Unknown assay — attempt generic counts + samplesheet approach
    return _build_rnaseq(state)


# ── RNA-seq ────────────────────────────────────────────────────────────────────

def _build_rnaseq(state: "BouncerState") -> "ad.AnnData | None":
    """
    Build AnnData from a genes × samples counts matrix + samplesheet.

    AnnData layout:
      X   = raw counts (samples × genes)
      obs = sample metadata (from samplesheet, indexed by sample_id)
      var = gene metadata (gene_id as index)
    """
    import anndata as ad
    import pandas as pd

    counts_info = _find_file(state, "counts_matrix")
    sheet_info  = _find_file(state, "sample_sheet")

    if counts_info is None or sheet_info is None:
        return None

    schema     = state["schema_contract"]
    index_col  = schema.get("index_column") or "gene_id"

    # ── Read counts (genes × samples) ─────────────────────────────────────────
    counts_path = counts_info["path"]
    sep = "\t" if Path(counts_path).suffix.lower() in (".tsv", ".txt") else ","
    try:
        counts_df = pd.read_csv(counts_path, sep=sep, index_col=0)
    except Exception:
        return None

    # First column might already be set as index; if not, try index_col name
    if counts_df.index.name != index_col and index_col in counts_df.columns:
        counts_df = counts_df.set_index(index_col)

    # ── Read samplesheet ───────────────────────────────────────────────────────
    try:
        sheet_df = pd.read_csv(sheet_info["path"])
    except Exception:
        return None

    if "sample_id" in sheet_df.columns:
        sheet_df = sheet_df.set_index("sample_id")

    # ── Align samples ──────────────────────────────────────────────────────────
    common = counts_df.columns.intersection(sheet_df.index)
    if len(common) == 0:
        return None

    counts_df = counts_df[common]
    sheet_df  = sheet_df.loc[common]

    # ── Build AnnData ──────────────────────────────────────────────────────────
    return ad.AnnData(
        X=counts_df.values.T.astype(float),       # samples × genes
        obs=sheet_df,
        var=pd.DataFrame(index=counts_df.index),
    )


# ── Flow cytometry ─────────────────────────────────────────────────────────────

def _build_flow(state: "BouncerState") -> "ad.AnnData | None":
    """
    Build AnnData from FCS file(s) + samplesheet.

    AnnData layout:
      X   = channel intensities (events × channels)
      obs = per-event metadata (sample_id injected)
      var = channel names
    """
    import anndata as ad
    import pandas as pd

    fcs_info   = _find_file(state, "fcs_file")
    sheet_info = _find_file(state, "sample_sheet")

    if fcs_info is None:
        return None

    try:
        import fcsparser
        meta, data_df = fcsparser.parse(fcs_info["path"], reformat_meta=True)
    except Exception:
        return None

    # Attach sample metadata from samplesheet if available
    obs = pd.DataFrame(index=data_df.index)
    if sheet_info is not None:
        try:
            sheet_df = pd.read_csv(sheet_info["path"])
            sample_id = Path(fcs_info["path"]).stem
            row = sheet_df[sheet_df["sample_id"] == sample_id]
            if not row.empty:
                for col in row.columns:
                    obs[col] = row.iloc[0][col]
        except Exception:
            pass

    return ad.AnnData(
        X=data_df.values.astype(float),
        obs=obs,
        var=pd.DataFrame(index=data_df.columns),
    )


# ── qPCR ───────────────────────────────────────────────────────────────────────

def _build_qpcr(state: "BouncerState") -> "ad.AnnData | None":
    """
    Build AnnData from a qPCR results table (samples × targets).

    AnnData layout:
      X   = Ct / ddCt values (samples × targets)
      obs = sample metadata
      var = target names
    """
    import anndata as ad
    import pandas as pd

    eds_info   = _find_file(state, "eds_file")
    sheet_info = _find_file(state, "sample_sheet")

    if eds_info is None:
        return None

    try:
        from bouncer.agent.tools.read_eds import _load_eds_dataframe  # type: ignore
        results_df = _load_eds_dataframe(eds_info["path"])
    except Exception:
        return None

    obs = pd.DataFrame(index=results_df.index)
    if sheet_info is not None:
        try:
            sheet_df = pd.read_csv(sheet_info["path"])
            if "sample_id" in sheet_df.columns:
                sheet_df = sheet_df.set_index("sample_id")
            common = results_df.index.intersection(sheet_df.index)
            if len(common) > 0:
                results_df = results_df.loc[common]
                obs = sheet_df.loc[common]
        except Exception:
            pass

    return ad.AnnData(
        X=results_df.values.astype(float),
        obs=obs,
        var=pd.DataFrame(index=results_df.columns),
    )


# ── Helpers ────────────────────────────────────────────────────────────────────

def _find_file(state: "BouncerState", file_type: str) -> "dict | None":
    """Return the first FileInfo entry matching file_type, or None."""
    return next(
        (f for f in state.get("file_map", []) if f["file_type"] == file_type),
        None,
    )
