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

    bouncer_volume.commit()

    log.info("job_complete",
             passed=state["passed"],
             n_findings=len(state["findings"]),
             feature_id=state.get("feature_id"))

    return {
        "passed":      state["passed"],
        "report":      state["report"],
        "feature_id":  state.get("feature_id"),
        "findings":    state["findings"],
        "tags":        state.get("tags", {}),
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

    @web_app.get("/features")
    def api_list_features(assay: str | None = None, data_stage: str | None = None):
        raise HTTPException(503, detail="Feature store not yet implemented.")

    @web_app.get("/features/{feature_id}/download")
    def api_pull_feature(feature_id: str):
        raise HTTPException(503, detail="Pull API not yet implemented.")

    return web_app
