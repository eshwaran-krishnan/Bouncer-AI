"""
bouncer/store/registry.py — DuckDB feature registration.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime
from pathlib import Path

import anndata as ad
import duckdb
import pandas as pd

from bouncer.agent.state import BouncerState
from bouncer.models.provenance import ProvenanceEntry
from bouncer.utils.hashing import hash_files

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS features (
    id                VARCHAR PRIMARY KEY,
    assay_type        VARCHAR NOT NULL,
    data_stage        VARCHAR NOT NULL,
    organism          VARCHAR,
    conditions        JSON,
    treatments        JSON,
    cell_lines        JSON,
    sample_ids        JSON,
    tags              JSON,
    qc_mode           VARCHAR NOT NULL,
    qc_status         VARCHAR NOT NULL,
    warnings          JSON,
    schema_version    VARCHAR NOT NULL,
    qc_version        VARCHAR NOT NULL,
    input_hashes      JSON NOT NULL,
    h5ad_path         VARCHAR NOT NULL,
    provenance        JSON NOT NULL,
    created_at        TIMESTAMP DEFAULT now(),
    parent_id         VARCHAR
);
"""


def _get_conn(db_path: str) -> duckdb.DuckDBPyConnection:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(db_path)
    conn.execute(_SCHEMA_SQL)
    return conn


def register(
    adata: ad.AnnData,
    state: BouncerState,
    db_path: str,
    h5ad_dir: str,
    parent_id: str | None = None,
) -> str:
    """
    Register a validated AnnData into the feature store.

    1. Builds provenance from state.
    2. Attaches provenance + warnings to adata.uns.
    3. Writes adata to h5ad file (UUID-named).
    4. Inserts metadata row into DuckDB.

    Returns the feature UUID.
    """
    Path(h5ad_dir).mkdir(parents=True, exist_ok=True)

    feature_id   = str(uuid.uuid4())
    h5ad_path    = str(Path(h5ad_dir) / f"{feature_id}.h5ad")
    schema_dict  = state["schema_contract"]
    qc_dict      = state["qc_contract"]
    tags         = state["tags"]
    findings     = state["findings"]
    mode         = state["mode"]

    # Determine QC status
    hard_count = sum(1 for f in findings if f["severity"] == "hard")
    warn_count = sum(1 for f in findings if f["severity"] in ("soft", "warning"))
    if hard_count == 0 and warn_count == 0:
        qc_status = "passed"
    elif hard_count == 0:
        qc_status = "passed_with_warnings"
    else:
        qc_status = "partial"  # only reachable in permissive mode

    provenance = ProvenanceEntry(
        stage=schema_dict.get("data_stage", "unknown"),
        assay_type=state["assay_type"],
        schema_version=schema_dict.get("version", "0.0.0"),
        qc_version=qc_dict.get("version", "0.0.0"),
        qc_mode=mode,
        input_hashes=hash_files(state["input_files"]),
        pipeline=qc_dict.get("pipeline"),
        parent_feature_id=parent_id,
        timestamp=datetime.utcnow(),
    )

    warnings = [f for f in findings if f["severity"] in ("soft", "warning")]

    # Attach to AnnData
    adata.uns["provenance"]    = [provenance.model_dump(mode="json")]
    adata.uns["warnings"]      = warnings
    adata.uns["tags"]          = tags
    adata.uns["bouncer_qc"]    = {"status": qc_status, "mode": mode}

    adata.write_h5ad(h5ad_path)

    # Insert into DuckDB
    conn = _get_conn(db_path)
    conn.execute(
        """INSERT INTO features VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,now(),?)""",
        [
            feature_id,
            state["assay_type"],
            schema_dict.get("data_stage", "unknown"),
            tags.get("organism"),
            json.dumps(tags.get("conditions", [])),
            json.dumps(tags.get("treatments", [])),
            json.dumps(tags.get("cell_lines", [])),
            json.dumps(tags.get("sample_ids", [])),
            json.dumps(tags),
            mode,
            qc_status,
            json.dumps(warnings),
            schema_dict.get("version", "0.0.0"),
            qc_dict.get("version", "0.0.0"),
            json.dumps(provenance.input_hashes),
            h5ad_path,
            json.dumps(provenance.model_dump(mode="json")),
            parent_id,
        ],
    )
    conn.close()
    return feature_id


def list_features(
    db_path: str,
    assay: str | None = None,
    data_stage: str | None = None,
) -> pd.DataFrame:
    """Return a summary DataFrame of all registered features."""
    conn = _get_conn(db_path)
    where_clauses = []
    params = []
    if assay:
        where_clauses.append("assay_type = ?")
        params.append(assay)
    if data_stage:
        where_clauses.append("data_stage = ?")
        params.append(data_stage)
    where = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""
    df = conn.execute(
        f"SELECT id, assay_type, data_stage, organism, qc_status, "
        f"schema_version, qc_version, created_at FROM features {where} "
        f"ORDER BY created_at DESC",
        params,
    ).df()
    conn.close()
    return df
