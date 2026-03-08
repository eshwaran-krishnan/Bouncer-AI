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
        "fcsparser>=0.2.8",        # Flow cytometry FCS binary files
        # EDS (QuantStudio qPCR) uses stdlib zipfile + xml — no extra dep needed
        # Agent framework + Anthropic
        "langgraph>=0.1.20",
        "langchain-anthropic>=0.1.15",
        "anthropic>=0.25.0",
        # API (inside containers only — not imported at module level locally)
        "fastapi>=0.111.0",
        "python-multipart>=0.0.9",
        # Config / CLI
        "pydantic>=2.6.0",
        "pyyaml>=6.0.1",
        "rich>=13.7.0",
    )
    # Local bouncer package — code changes picked up without rebuilding the dep layer
    .add_local_python_source("bouncer")
    # Schema contracts bundled into the container at /schemas
    # Add new assay folders here as they're created (e.g. schemas/flow-cytometry/)
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
    schema_name: str,
    mode: str = "strict",
) -> dict:
    """
    Core QC function. Runs in its own container (2 vCPU / 4 GB).

    Called via spawn() from the API — never blocks the HTTP layer.

    Args:
        staged_paths: Absolute paths to input files on the Volume.
        assay_type:   e.g. "rna-seq", "flow-cytometry", "qpcr"
        schema_name:  Schema subfolder + stem, e.g. "rna-seq/basic".
                      Resolves to /schemas/{schema_name}-schema.yaml
                      and            /schemas/{schema_name}-qc.yaml
        mode:         "strict" or "permissive"

    Returns: {"passed": bool, "report": str, "feature_id": str | None}

    ── Plug-in point ────────────────────────────────────────────────────────
    Once bouncer.agent is built, replace the NotImplementedError with:

        import yaml
        from bouncer.agent.graph import graph
        from bouncer.config import SchemaContract, QCContract

        schema_path = f"/schemas/{schema_name}-schema.yaml"
        qc_path     = f"/schemas/{schema_name}-qc.yaml"

        with open(schema_path) as f:
            schema = SchemaContract(**yaml.safe_load(f))
        with open(qc_path) as f:
            qc = QCContract(**yaml.safe_load(f))

        result = graph.invoke({
            "input_files":     staged_paths,
            "assay_type":      assay_type,
            "schema_contract": schema.model_dump(),
            "qc_contract":     qc.model_dump(),
            "mode":            mode,
        })

        bouncer_volume.commit()

        return {
            "passed":     result["passed"],
            "report":     result["report"],
            "feature_id": result.get("feature_id"),
        }
    ─────────────────────────────────────────────────────────────────────────
    """
    import os
    import yaml
    from bouncer.agent.graph import run as run_pipeline
    from bouncer.utils.logger import get_logger

    log = get_logger("bouncer.modal.run_qc")

    bouncer_volume.reload()
    os.makedirs(FEATURES_DIR, exist_ok=True)

    schema_path = f"/schemas/{schema_name}-schema.yaml"
    qc_path     = f"/schemas/{schema_name}-qc.yaml"

    log.info("job_start",
             assay_type=assay_type,
             schema_name=schema_name,
             mode=mode,
             n_staged_files=len(staged_paths),
             staged_paths=staged_paths)

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
                  schema_name=schema_name,
                  error=str(exc),
                  exc_info=True)
        raise

    bouncer_volume.commit()

    log.info("job_complete",
             passed=state["passed"],
             n_findings=len(state["findings"]),
             feature_id=state.get("feature_id"))

    return {
        "passed":     state["passed"],
        "report":     state["report"],
        "feature_id": state.get("feature_id"),
        "findings":   state["findings"],
    }


# ── ASGI Web Endpoint ─────────────────────────────────────────────────────────
# All FastAPI imports and app construction happen INSIDE this function so they
# execute inside the container image — not locally when `modal deploy` runs.
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
    from fastapi import FastAPI, File, Form, HTTPException, UploadFile
    from fastapi.responses import Response

    os.makedirs(STAGING_DIR, exist_ok=True)
    os.makedirs(FEATURES_DIR, exist_ok=True)

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
        schema_name: str = Form(..., description="Schema stem, e.g. 'rna-seq/basic'"),
        mode:        str = Form("strict", description="strict | permissive"),
        files: list[UploadFile] = File(...),
    ):
        """
        Upload pipeline output files and spawn a QC job.

        schema_name resolves to /schemas/{schema_name}-schema.yaml and
        /schemas/{schema_name}-qc.yaml inside the container.

        Returns job_id immediately. Poll GET /qc/jobs/{job_id} for results.
        """
        import uuid

        run_id  = str(uuid.uuid4())[:8]
        run_dir = f"{STAGING_DIR}/{run_id}"
        os.makedirs(run_dir, exist_ok=True)

        staged_paths: list[str] = []
        for upload in files:
            dest = f"{run_dir}/{upload.filename}"
            content = await upload.read()
            with open(dest, "wb") as f:
                f.write(content)
            staged_paths.append(dest)

        bouncer_volume.commit()

        call = run_qc.spawn(staged_paths, assay_type, schema_name, mode)

        return {"job_id": call.object_id, "status": "queued", "run_id": run_id}

    @web_app.get("/qc/jobs/{job_id}")
    def api_get_job(job_id: str):
        """
        Poll for QC job status. Returns results once the job completes.
        """
        fc = modal.FunctionCall.from_id(job_id)
        try:
            result = fc.get(timeout=0)  # non-blocking; raises TimeoutError if still running
            return {"status": "complete", "result": result}
        except TimeoutError:
            return {"status": "running"}
        except Exception as e:
            return {"status": "failed", "error": str(e)}

    @web_app.get("/features")
    def api_list_features(assay: str | None = None, data_stage: str | None = None):
        """
        List registered features from the DuckDB store.

        ── Plug-in point ────────────────────────────────────────────────────
        from bouncer.store.registry import list_features
        df = list_features(db_path=DB_PATH, assay=assay, data_stage=data_stage)
        return df.to_dict(orient="records")
        ─────────────────────────────────────────────────────────────────────
        """
        raise HTTPException(503, detail="Feature store not yet implemented.")

    @web_app.get("/features/{feature_id}/download")
    def api_pull_feature(feature_id: str):
        """
        Download a registered feature set as an h5ad file.

        ── Plug-in point ────────────────────────────────────────────────────
        import io
        from bouncer.store.pull import pull_data

        adata = pull_data(db_path=DB_PATH, h5ad_dir=FEATURES_DIR, feature_id=feature_id)
        buf   = io.BytesIO()
        adata.write_h5ad(buf)

        return Response(
            content=buf.getvalue(),
            media_type="application/octet-stream",
            headers={"Content-Disposition": f"attachment; filename={feature_id}.h5ad"},
        )
        ─────────────────────────────────────────────────────────────────────
        """
        raise HTTPException(503, detail="Pull API not yet implemented.")

    return web_app
