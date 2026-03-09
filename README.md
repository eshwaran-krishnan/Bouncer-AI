# Bouncer AI

A schema-driven biological data quality contract layer for Nextflow pipeline outputs.

Bouncer validates pipeline outputs against YAML contracts, generates QC reports, and registers clean datasets as versioned AnnData feature sets in a DuckDB feature store.

**Live API:** `https://lol882192--bouncer-qc-api.modal.run`

---

## What it does

1. **QC** — Upload pipeline output files + schema/QC contracts → AI agent classifies, extracts, tags, cross-references, and produces a structured findings report.
2. **Push** — Upload a counts matrix + samplesheet → container validates against a bundled schema, builds an h5ad, and registers it directly to the feature store (no AI agent).
3. **Feature store** — Browse, filter, query, and download registered feature sets as h5ad.

Supported assays: RNA-seq (primary), flow cytometry (FCS), qPCR (QuantStudio EDS).

---

## Installation

```bash
pip install -e .                      # CLI only (no container deps)
pip install -e ".[container]"         # full local install
pip install -e ".[infra]"             # adds modal for deployment
```

Set your API URL:

```bash
export BOUNCER_API_URL=https://lol882192--bouncer-qc-api.modal.run
```

---

## CLI commands

### `bouncer ping`
Verify the API is reachable.
```bash
bouncer ping
```

### `bouncer run` — AI QC agent
Run the full AI QC pipeline against local schema + QC contract files.
```bash
# Bundled schema (resolves schema + QC contract automatically)
bouncer run counts.tsv samplesheet.csv multiqc.json --schema rna-seq/basic

# Explicit local schema files
bouncer run counts.tsv samplesheet.csv \
    --schema schemas/rna-seq/basic-schema.yaml \
    --qc     schemas/rna-seq/basic-qc.yaml \
    --assay  rna-seq

# Permissive mode — register even if soft findings exist
bouncer run counts.tsv samplesheet.csv --schema rna-seq/basic --mode permissive

# Save HTML report to a specific path
bouncer run counts.tsv samplesheet.csv --schema rna-seq/basic --report-out report.html
```

Exits 0 on pass, 1 on failure. Saves an HTML report to the current directory.

### `bouncer push` — Schema-driven registration (no AI agent)
Upload a counts matrix + samplesheet; the server builds and registers the h5ad.
```bash
bouncer push counts.tsv samplesheet.csv
bouncer push counts.tsv samplesheet.csv --schema rna-seq/basic --mode permissive
```

The schema is resolved from the bundled copy inside the container — no schema upload needed.

### `bouncer list-features`
List registered features in the feature store.
```bash
bouncer list-features
bouncer list-features --assay rna-seq --stage raw_counts
```

### `bouncer pull`
Download a registered feature set as an h5ad file.
```bash
bouncer pull --id <feature_id> --output dataset.h5ad
```

---

## Schemas

Bundled schemas live in `schemas/`. Each assay has a schema contract and a QC contract:

```
schemas/
  rna-seq/
    basic-schema.yaml   # column definitions, required fields, index column
    basic-qc.yaml       # dual-threshold QC rules (hard_min/soft_min/hard_max/soft_max)
```

Reference by name: `rna-seq/basic`

---

## API reference

Base URL: `https://lol882192--bouncer-qc-api.modal.run`

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/health` | Health check |
| `POST` | `/qc/run` | Submit a QC job (multipart: files + schema_file + qc_file) |
| `GET` | `/qc/jobs/{job_id}` | Poll job status / retrieve result |
| `GET` | `/features` | List features (filter by assay, stage, organism, etc.) |
| `GET` | `/features/{id}` | Get feature metadata |
| `GET` | `/features/{id}/download` | Download feature as h5ad |
| `POST` | `/features/push` | Schema-driven push (multipart: files + schema_name) |
| `POST` | `/features/sql` | Run a read-only SELECT against the features table |

---

## Architecture

```
CLI (bouncer/cli.py)
  └── httpx → Modal API (infra/modal_app.py)
                ├── run_qc        → agent pipeline (bouncer/agent/graph.py)
                │     classify → extract → tag → cross_reference → report
                └── push_to_store → validate → build_adata → register
```

- Agent uses `claude-sonnet-4-6` via Anthropic tool_use for classify + tag nodes.
- Feature store: DuckDB registry + h5ad files on a Modal persistent volume.
- No LangGraph — plain sequential Python pipeline.

---

## Deployment

```bash
# Deploy to Modal
modal deploy infra/modal_app.py

# One-time secret setup
modal secret create anthropic-api-key ANTHROPIC_API_KEY=sk-ant-...
```
