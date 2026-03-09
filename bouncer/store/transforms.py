"""
bouncer/store/transforms.py — Built-in AnnData transforms.

Each transform returns a new AnnData with an updated provenance entry
pointing back to the source feature. Re-register the result with
bouncer.store.registry.register() to persist it.
"""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime

import anndata as ad
import numpy as np
import pandas as pd
import scipy.sparse as sp


def _new_adata(source: ad.AnnData, X_new, stage: str) -> ad.AnnData:
    """Create a new AnnData preserving obs/var/uns from source."""
    adata = ad.AnnData(
        X=X_new,
        obs=source.obs.copy(),
        var=source.var.copy(),
        uns=deepcopy(source.uns),
    )
    adata.uns.setdefault("provenance", [])
    adata.uns["provenance"].append({
        "stage": stage,
        "derived_from": source.uns.get("bouncer_qc", {}).get("feature_id"),
        "timestamp": datetime.utcnow().isoformat(),
    })
    return adata


def to_tpm(adata: ad.AnnData, gene_lengths: pd.Series) -> ad.AnnData:
    """
    Convert raw counts to TPM (Transcripts Per Million).

    Args:
        adata:        AnnData with raw counts in X (samples × genes).
        gene_lengths: pd.Series indexed by gene_id with length in bp.
    """
    X = adata.X.toarray() if sp.issparse(adata.X) else adata.X.copy().astype(float)
    lengths = gene_lengths.reindex(adata.var_names).values
    if np.isnan(lengths).any():
        raise ValueError("gene_lengths missing values for some genes in adata.var_names")
    rpk = X / (lengths / 1e3)
    tpm = rpk / rpk.sum(axis=1, keepdims=True) * 1e6
    return _new_adata(adata, tpm, "tpm")


def to_rpkm(adata: ad.AnnData, gene_lengths: pd.Series) -> ad.AnnData:
    """Convert raw counts to RPKM / FPKM."""
    X = adata.X.toarray() if sp.issparse(adata.X) else adata.X.copy().astype(float)
    lengths = gene_lengths.reindex(adata.var_names).values
    per_million = X.sum(axis=1, keepdims=True) / 1e6
    rpkm = X / (lengths / 1e3) / per_million
    return _new_adata(adata, rpkm, "rpkm")


def to_log2(adata: ad.AnnData, pseudocount: float = 1.0) -> ad.AnnData:
    """Log2(X + pseudocount) transform."""
    X = adata.X.toarray() if sp.issparse(adata.X) else adata.X.copy().astype(float)
    return _new_adata(adata, np.log2(X + pseudocount), "log2")


def normalize_deseq2(adata: ad.AnnData) -> ad.AnnData:
    """
    DESeq2-style median-of-ratios normalization.
    Requires raw integer counts in X.
    """
    X = adata.X.toarray() if sp.issparse(adata.X) else adata.X.copy().astype(float)
    # Geometric mean per gene (across samples)
    log_X = np.log(X + 1e-10)
    geom_mean = np.exp(log_X.mean(axis=0))
    # Ratio of each sample to geometric mean
    ratios = X / geom_mean
    # Size factor = median ratio per sample
    size_factors = np.median(ratios, axis=1, keepdims=True)
    normalized = X / size_factors
    return _new_adata(adata, normalized, "deseq2_normalized")


def custom_transform(
    adata: ad.AnnData,
    func: callable,
    stage_name: str,
) -> ad.AnnData:
    """
    Apply a user-defined function to adata.X.

    Args:
        adata:      Source AnnData.
        func:       Function that takes an ndarray (samples × genes) and
                    returns an ndarray of the same shape.
        stage_name: Provenance label for this transform, e.g. "vst_normalized".
    """
    X = adata.X.toarray() if sp.issparse(adata.X) else adata.X.copy().astype(float)
    X_new = func(X)
    return _new_adata(adata, X_new, stage_name)
