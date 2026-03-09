"""
bouncer/store/pull.py — Pull features from the DuckDB store.
"""

from __future__ import annotations

import json
from pathlib import Path

import anndata as ad
import duckdb
import pandas as pd


def pull_data(
    db_path: str,
    h5ad_dir: str,
    assay: list[str] | None = None,
    treatment: list[str] | None = None,
    condition: list[str] | None = None,
    organism: str | None = None,
    data_stage: str | None = None,
    qc_status: str = "passed",
    feature_id: str | None = None,
    include_warnings: bool = False,
) -> ad.AnnData:
    """
    Pull features from the store as a concatenated AnnData.

    Filters are applied with AND logic. JSON array columns (conditions,
    treatments) are matched with LIKE for simplicity.
    """
    conn = duckdb.connect(db_path)

    clauses: list[str] = []
    params:  list      = []

    if feature_id:
        clauses.append("id = ?")
        params.append(feature_id)
    else:
        if assay:
            placeholders = ",".join("?" for _ in assay)
            clauses.append(f"assay_type IN ({placeholders})")
            params.extend(assay)
        if data_stage:
            clauses.append("data_stage = ?")
            params.append(data_stage)
        if organism:
            clauses.append("organism = ?")
            params.append(organism)
        if not include_warnings:
            clauses.append("qc_status = ?")
            params.append(qc_status)
        else:
            clauses.append("qc_status IN ('passed', 'passed_with_warnings')")
        for val_list, col in [(treatment, "treatments"), (condition, "conditions")]:
            if val_list:
                for v in val_list:
                    clauses.append(f"{col} LIKE ?")
                    params.append(f"%{v}%")

    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    rows  = conn.execute(
        f"SELECT id, h5ad_path FROM features {where}", params
    ).fetchall()
    conn.close()

    if not rows:
        raise ValueError("No features match the given filters.")

    adatas = []
    for fid, h5ad_path in rows:
        if not Path(h5ad_path).exists():
            raise FileNotFoundError(f"h5ad not found for feature {fid}: {h5ad_path}")
        adatas.append(ad.read_h5ad(h5ad_path))

    if len(adatas) == 1:
        return adatas[0]
    return ad.concat(adatas, merge="same")
