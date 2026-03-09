"""
Bouncer QC — Modal Infrastructure
===================================

Deploy:
    modal deploy infra/modal_app.py

Serve (dev, hot-reload):
    modal serve infra/modal_app.py

One-time setup before deploying:
    modal secret create anthropic-api-key ANTHROPIC_API_KEY=sk-ant-...

The Volume is created automatically on first deploy.
"""

import modal

# ── App ───────────────────────────────────────────────────────────────────────
app = modal.App("bouncer-qc")

# ── Container Image ───────────────────────────────────────────────────────────
# Heavy bioinformatics deps first so their cached layer survives code changes.
bouncer_image = (
    modal.Image.debian_slim(python_version="3.11")
    .apt_install(
        "libpoppler-cpp-dev",  # required by pdfplumber
        "pkg-config",
        "curl",
    )
    .pip_install(
        # Bioinformatics core
        "anndata>=0.10.6",
        "scanpy>=1.10.0",
        "pandas>=2.2.0",
        "numpy>=1.26.0",
        # QC engine
        "pandera>=0.19.0",
        "duckdb>=0.10.0",
        "pdfplumber>=0.10.3",
        # Assay-specific parsers
        "fcsparser>=0.2.8",
        # Agent + Anthropic
        "anthropic>=0.25.0",
        # API (inside containers only)
        "fastapi>=0.111.0",
        "python-multipart>=0.0.9",
        # Config
        "pydantic>=2.6.0",
        "pyyaml>=6.0.1",
        "rich>=13.7.0",
    )
    .add_local_python_source("bouncer")
    # Bundled schemas available as fallback at /schemas — NOT used when
    # the user supplies their own via the API upload.
    .add_local_dir("schemas", remote_path="/schemas")
)

# ── Persistent Volume ─────────────────────────────────────────────────────────
bouncer_volume = modal.Volume.from_name("bouncer-store", create_if_missing=True)

MOUNT_PATH   = "/data"
DB_PATH      = f"{MOUNT_PATH}/bouncer.db"
FEATURES_DIR = f"{MOUNT_PATH}/features"
STAGING_DIR  = f"{MOUNT_PATH}/staging"

# ── Secrets ───────────────────────────────────────────────────────────────────
anthropic_secret = modal.Secret.from_name("anthropic-api-key")


# ── QC Function ───────────────────────────────────────────────────────────────
@app.function(
    image=bouncer_image,
    volumes={MOUNT_PATH: bouncer_volume},
    secrets=[anthropic_secret],
    timeout=600,
    cpu=2.0,
    memory=4096,
)
def run_qc(
    staged_paths: list[str],
    assay_type: str,
    schema_path: str,
    qc_path: str,
    mode: str = "strict",
) -> dict:
    """
    Core QC function. Runs in its own container (2 vCPU / 4 GB).

    Called via spawn() from the API — never blocks the HTTP layer.

    Args:
        staged_paths: Absolute paths to input files on the Volume.
        assay_type:   e.g. "rna-seq", "flow-cytometry", "qpcr"
        schema_path:  Absolute path to the staged schema YAML on the Volume.
                      Always the user-supplied file — never assumed from name.
        qc_path:      Absolute path to the staged QC YAML on the Volume.
        mode:         "strict" or "permissive"

    Returns: {"passed": bool, "report": str, "report_html": str,
              "feature_id": str | None, "findings": list}
    """
    import os
    from bouncer.agent.graph import run as run_pipeline
    from bouncer.utils.logger import get_logger

    log = get_logger("bouncer.modal.run_qc")

    bouncer_volume.reload()
    os.makedirs(FEATURES_DIR, exist_ok=True)
    os.makedirs(STAGING_DIR, exist_ok=True)

    from bouncer.store.registry import init_db
    init_db(DB_PATH)

    log.info("job_start",
             assay_type=assay_type,
             schema_path=schema_path,
             qc_path=qc_path,
             mode=mode,
             n_staged_files=len(staged_paths),
             staged_paths=staged_paths)

    # Verify schema files exist — fail fast with a clear error
    for label, path in [("schema", schema_path), ("qc", qc_path)]:
        if not os.path.exists(path):
            msg = f"Staged {label} file not found: {path}"
            log.error("schema_file_missing", label=label, path=path)
            raise FileNotFoundError(msg)

    try:
        state = run_pipeline(
            input_files=staged_paths,
            assay_type=assay_type,
            schema_path=schema_path,
            qc_path=qc_path,
            mode=mode,
        )
    except Exception as exc:
        log.error("job_failed",
                  assay_type=assay_type,
                  schema_path=schema_path,
                  error=str(exc),
                  exc_info=True)
        raise

    # ── Register to feature store ──────────────────────────────────────────────
    # Register if QC passed (strict) or in permissive mode regardless.
    feature_id = None
    if state["passed"] or mode == "permissive":
        try:
            from bouncer.store.builder import build_adata
            from bouncer.store.registry import register

            adata = build_adata(state)
            if adata is not None:
                feature_id = register(adata, state, DB_PATH, FEATURES_DIR)
                state = {**state, "feature_id": feature_id}
                log.info("feature_registered",
                         feature_id=feature_id,
                         assay_type=assay_type,
                         n_obs=adata.n_obs,
                         n_vars=adata.n_vars)
            else:
                log.warning("adata_build_skipped",
                            reason="counts_matrix or sample_sheet not found in file_map")
        except Exception as exc:
            # Registration failure never fails the QC job — just log it
            log.error("registration_failed", error=str(exc), exc_info=True)

    bouncer_volume.commit()

    log.info("job_complete",
             passed=state["passed"],
             n_findings=len(state["findings"]),
             feature_id=feature_id)

    return {
        "passed":      state["passed"],
        "report":      state["report"],
        "feature_id":  feature_id,
        "findings":    state["findings"],
        "tags":        state.get("tags", {}),
    }


# ── Push-to-Store Function ────────────────────────────────────────────────────
@app.function(
    image=bouncer_image,
    volumes={MOUNT_PATH: bouncer_volume},
    timeout=300,
    cpu=2.0,
    memory=4096,
)
def push_to_store(
    staged_paths: list[str],
    schema_name: str,          # e.g. "rna-seq/basic"
    mode: str = "strict",
) -> dict:
    """
    Schema-driven registration without the AI QC agent.
    Loads schema from bundled copy in image, validates, builds AnnData, registers.
    """
    import os
    import yaml as _yaml
    import pandas as pd
    from bouncer.config import SchemaContract
    from bouncer.qc.schema_validator import validate_schema
    from bouncer.store.registry import init_db, register
    from bouncer.store.builder import build_adata
    from bouncer.utils.logger import get_logger

    log = get_logger("bouncer.modal.push_to_store")
    bouncer_volume.reload()
    os.makedirs(FEATURES_DIR, exist_ok=True)
    init_db(DB_PATH)

    # Resolve schema from bundled copy
    parts = schema_name.split("/", 1)
    if len(parts) != 2:
        raise ValueError(f"Invalid schema name: {schema_name!r}. Expected 'assay/name'.")
    assay_dir, schema_base = parts
    schema_path = f"/schemas/{assay_dir}/{schema_base}-schema.yaml"

    with open(schema_path) as f:
        schema_dict = _yaml.safe_load(f)
    schema = SchemaContract(**schema_dict)

    # Identify counts matrix and samplesheet from staged files
    tsv_files = [p for p in staged_paths if p.endswith(".tsv")]
    csv_files = [p for p in staged_paths if p.endswith(".csv")]

    if not tsv_files or not csv_files:
        raise ValueError("push requires a counts matrix (.tsv) and a samplesheet (.csv)")

    counts_path = tsv_files[0]
    sheet_path  = csv_files[0]

    counts_df = pd.read_csv(counts_path, sep="\t", index_col=schema.index_column)
    sheet_df  = pd.read_csv(sheet_path)

    # Validate metadata columns — drop non-numeric annotation columns (e.g. gene_name)
    # before extracting sample column names, matching the behaviour of cross_reference.py
    sample_columns = list(counts_df.select_dtypes(include="number").columns)
    findings = validate_schema(sheet_df, schema, counts_columns=sample_columns)
    hard = [f for f in findings if f.severity == "hard"]
    if hard and mode == "strict":
        return {
            "registered": False,
            "findings": [f.model_dump() for f in findings],
            "error": f"{len(hard)} hard finding(s) blocked registration.",
        }

    # Build state compatible with build_adata + register
    state = {
        "passed": len(hard) == 0,
        "mode": mode,
        "findings": [f.model_dump() for f in findings],
        "tags": {
            "assay_type": schema.assay_type,
            "data_stage": schema.data_stage,
            "n_samples":  int(counts_df.shape[1]),
        },
        "schema_contract": schema_dict,
        "qc_contract": {},
        "file_map": [
            {"path": counts_path, "file_type": "counts_matrix", "content": None},
            {"path": sheet_path,  "file_type": "sample_sheet",  "content": None},
        ],
        "extracted_data": {
            "counts_matrix": counts_df,
            "sample_sheet":  sheet_df,
        },
        "report": "",
    }

    adata = build_adata(state)
    if adata is None:
        raise ValueError("Could not build AnnData from provided files")

    feature_id = register(adata, state, DB_PATH, FEATURES_DIR)
    bouncer_volume.commit()

    log.info("push_complete", feature_id=feature_id, schema=schema_name)
    return {
        "registered": True,
        "feature_id": feature_id,
        "findings": [f.model_dump() for f in findings],
    }


# ── ASGI Web Endpoint ─────────────────────────────────────────────────────────
@app.function(
    image=bouncer_image,
    volumes={MOUNT_PATH: bouncer_volume},
    secrets=[anthropic_secret],
)
@modal.concurrent(max_inputs=20)
@modal.asgi_app()
def api():
    """
    FastAPI web endpoint. Modal assigns a stable HTTPS URL after `modal deploy`.
    Check the URL with: modal app list
    """
    import os
    import uuid
    from fastapi import FastAPI, File, Form, HTTPException, UploadFile
    from fastapi.responses import Response

    os.makedirs(STAGING_DIR, exist_ok=True)
    os.makedirs(FEATURES_DIR, exist_ok=True)

    # Ensure DB schema exists before any request is served
    from bouncer.store.registry import init_db
    init_db(DB_PATH)

    web_app = FastAPI(
        title="Bouncer QC API",
        description="Biological data quality contract layer",
        version="0.1.0",
    )

    @web_app.get("/health")
    def health():
        return {"status": "ok", "service": "bouncer-qc"}

    @web_app.post("/qc/run")
    async def api_run_qc(
        assay_type:  str = Form(..., description="rna-seq | flow-cytometry | qpcr"),
        mode:        str = Form("strict", description="strict | permissive"),
        files:       list[UploadFile] = File(..., description="Pipeline output files"),
        schema_file: UploadFile       = File(..., description="Schema YAML contract"),
        qc_file:     UploadFile       = File(..., description="QC YAML contract"),
    ):
        """
        Upload pipeline output files + schema/QC YAMLs and spawn a QC job.

        The schema_file and qc_file you supply are ALWAYS used — the container
        never falls back to any bundled defaults.

        Returns job_id immediately. Poll GET /qc/jobs/{job_id} for results.
        """
        run_id  = str(uuid.uuid4())[:8]
        run_dir = f"{STAGING_DIR}/{run_id}"
        os.makedirs(run_dir, exist_ok=True)

        # Stage data files
        staged_paths: list[str] = []
        for upload in files:
            dest = f"{run_dir}/{upload.filename}"
            with open(dest, "wb") as f:
                f.write(await upload.read())
            staged_paths.append(dest)

        # Stage schema + QC YAMLs from the user's upload — always use these
        staged_schema = f"{run_dir}/_schema.yaml"
        staged_qc     = f"{run_dir}/_qc.yaml"
        with open(staged_schema, "wb") as f:
            f.write(await schema_file.read())
        with open(staged_qc, "wb") as f:
            f.write(await qc_file.read())

        bouncer_volume.commit()

        call = run_qc.spawn(
            staged_paths,
            assay_type,
            staged_schema,
            staged_qc,
            mode,
        )

        return {"job_id": call.object_id, "status": "queued", "run_id": run_id}

    @web_app.get("/qc/jobs/{job_id}")
    def api_get_job(job_id: str):
        """Poll for QC job status. Returns results once the job completes."""
        fc = modal.FunctionCall.from_id(job_id)
        try:
            result = fc.get(timeout=0)
            return {"status": "complete", "result": result}
        except TimeoutError:
            return {"status": "running"}
        except Exception as e:
            return {"status": "failed", "error": str(e)}

    # ── Feature Store Endpoints ───────────────────────────────────────────────

    @web_app.get("/features")
    def api_list_features(
        assay:           str | None = None,
        stage:           str | None = None,
        experiment_name: str | None = None,
        organism:        str | None = None,
        qc_status:       str | None = None,
        conditions:      str | None = None,   # comma-separated
        treatments:      str | None = None,   # comma-separated
        limit:           int        = 100,
    ):
        """
        List registered features with optional filters.

        All filters are AND-combined. JSON array columns (conditions,
        treatments) are matched with LIKE.

        Query params:
          assay        — e.g. rna-seq
          stage        — e.g. raw_counts, tpm
          organism     — e.g. Homo sapiens
          qc_status    — passed | passed_with_warnings | partial
          conditions   — comma-separated values, e.g. treated,control
          treatments   — comma-separated values
          limit        — max rows returned (default 100)
        """
        import duckdb as _duckdb

        bouncer_volume.reload()

        clauses: list[str] = []
        params:  list      = []

        if assay:
            clauses.append("assay_type = ?")
            params.append(assay)
        if stage:
            clauses.append("data_stage = ?")
            params.append(stage)
        if experiment_name:
            clauses.append("experiment_name LIKE ?")
            params.append(f"%{experiment_name}%")
        if organism:
            clauses.append("organism = ?")
            params.append(organism)
        if qc_status:
            clauses.append("qc_status = ?")
            params.append(qc_status)
        for raw, col in [(conditions, "conditions"), (treatments, "treatments")]:
            if raw:
                for val in raw.split(","):
                    val = val.strip()
                    if val:
                        clauses.append(f"{col} LIKE ?")
                        params.append(f"%{val}%")

        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        try:
            conn = _duckdb.connect(DB_PATH, read_only=True)
            rows = conn.execute(
                f"""SELECT id, assay_type, data_stage, experiment_name, organism,
                           conditions, treatments, sample_ids,
                           qc_status, schema_version, qc_version,
                           created_at
                    FROM features {where}
                    ORDER BY created_at DESC
                    LIMIT ?""",
                params + [limit],
            ).fetchall()
            cols = ["id", "assay_type", "data_stage", "experiment_name", "organism",
                    "conditions", "treatments", "sample_ids",
                    "qc_status", "schema_version", "qc_version", "created_at"]
            conn.close()
        except Exception as exc:
            raise HTTPException(500, detail=str(exc))

        return [dict(zip(cols, row)) for row in rows]

    @web_app.get("/features/{feature_id}")
    def api_get_feature(feature_id: str):
        """Return full metadata for a single registered feature."""
        import duckdb as _duckdb

        bouncer_volume.reload()
        try:
            conn = _duckdb.connect(DB_PATH, read_only=True)
            row = conn.execute(
                "SELECT * FROM features WHERE id = ?", [feature_id]
            ).fetchone()
            cols = [d[0] for d in conn.description]
            conn.close()
        except Exception as exc:
            raise HTTPException(500, detail=str(exc))

        if row is None:
            raise HTTPException(404, detail=f"Feature {feature_id!r} not found.")
        return dict(zip(cols, row))

    @web_app.get("/features/{feature_id}/download")
    def api_pull_feature(feature_id: str):
        """Download a registered feature set as an h5ad file."""
        import tempfile
        from bouncer.store.pull import pull_data

        bouncer_volume.reload()
        try:
            adata = pull_data(DB_PATH, FEATURES_DIR, feature_id=feature_id)
        except (ValueError, FileNotFoundError) as exc:
            raise HTTPException(404, detail=str(exc))
        except Exception as exc:
            raise HTTPException(500, detail=str(exc))

        with tempfile.NamedTemporaryFile(suffix=".h5ad", delete=False) as tmp:
            adata.write_h5ad(tmp.name)
            data = open(tmp.name, "rb").read()

        return Response(
            content=data,
            media_type="application/octet-stream",
            headers={"Content-Disposition": f'attachment; filename="{feature_id}.h5ad"'},
        )

    @web_app.post("/features/sql")
    def api_sql_query(body: dict):
        """
        Run a read-only SQL query against the features table.

        Only SELECT statements are accepted. This is intended for data team
        power users who need filtering beyond what the REST params support.

        Request body:
          { "query": "SELECT id, assay_type FROM features WHERE organism = 'Homo sapiens'" }

        The features table schema:
          id, assay_type, data_stage, organism, conditions (JSON), treatments (JSON),
          cell_lines (JSON), sample_ids (JSON), tags (JSON), qc_mode, qc_status,
          warnings (JSON), schema_version, qc_version, input_hashes (JSON),
          h5ad_path, provenance (JSON), created_at, parent_id
        """
        import duckdb as _duckdb

        query = (body.get("query") or "").strip()
        if not query:
            raise HTTPException(400, detail="'query' field is required.")

        # Allow only SELECT statements
        if not query.upper().lstrip().startswith("SELECT"):
            raise HTTPException(400, detail="Only SELECT queries are permitted.")

        # Block obviously dangerous keywords as a belt-and-braces check
        blocked = {"DROP", "DELETE", "INSERT", "UPDATE", "ALTER", "CREATE",
                   "TRUNCATE", "ATTACH", "DETACH", "COPY", "EXPORT"}
        tokens = set(query.upper().split())
        if tokens & blocked:
            raise HTTPException(400, detail="Query contains disallowed keywords.")

        bouncer_volume.reload()
        try:
            conn = _duckdb.connect(DB_PATH, read_only=True)
            result = conn.execute(query).fetchdf()
            conn.close()
        except Exception as exc:
            raise HTTPException(400, detail=f"Query error: {exc}")

        return result.to_dict(orient="records")

    @web_app.post("/features/push")
    async def api_features_push(
        files:       list[UploadFile] = File(..., description="counts matrix (.tsv) + samplesheet (.csv)"),
        schema_name: str              = Form("rna-seq/basic", description="Bundled schema name, e.g. rna-seq/basic"),
        mode:        str              = Form("strict"),
    ):
        """
        Schema-driven registration without the AI QC agent.
        Schema is resolved from the bundled copy in the image — no upload needed.
        Returns job_id immediately. Poll GET /qc/jobs/{job_id} for results.
        """
        run_id  = str(uuid.uuid4())[:8]
        run_dir = f"{STAGING_DIR}/{run_id}"
        os.makedirs(run_dir, exist_ok=True)

        staged_paths: list[str] = []
        for upload in files:
            dest = f"{run_dir}/{upload.filename}"
            with open(dest, "wb") as f:
                f.write(await upload.read())
            staged_paths.append(dest)

        bouncer_volume.commit()

        call = push_to_store.spawn(staged_paths, schema_name, mode)
        return {"job_id": call.object_id, "status": "queued", "run_id": run_id}

    return web_app
