# BOUNCER — Full Implementation Plan for Claude Code

## Overview

BOUNCER is a biological data quality contract layer. It validates Nextflow pipeline outputs against user-defined YAML contracts, generates actionable QC reports, and registers clean outputs as versioned AnnData feature sets in a DuckDB-backed feature store. Users can pull, transform, and re-register data with full provenance.

This plan is scoped to **RNA-seq** as the first supported assay type.

---

## Project Structure

```
bouncer/
├── pyproject.toml                  # uv/poetry project config
├── README.md
├── bouncer/
│   ├── __init__.py                 # version, public API
│   ├── cli.py                      # Typer CLI entrypoint
│   ├── config.py                   # Pydantic models for YAML contracts
│   ├── agent/
│   │   ├── __init__.py
│   │   ├── graph.py                # LangGraph agent definition
│   │   ├── state.py                # BouncerState TypedDict
│   │   ├── nodes/
│   │   │   ├── classify.py         # File classification node
│   │   │   ├── extract.py          # Data extraction node
│   │   │   ├── tag.py              # Dynamic tag assignment node
│   │   │   ├── cross_reference.py  # Cross-file QC + design checks
│   │   │   └── report.py           # Report generation node
│   │   └── tools/
│   │       ├── read_csv.py         # CSV/TSV reader tool
│   │       ├── read_json.py        # JSON reader tool (MultiQC)
│   │       ├── read_pdf.py         # PDF protocol reader tool
│   │       └── peek_file.py        # Quick file preview tool
│   ├── qc/
│   │   ├── __init__.py
│   │   ├── schema_validator.py     # Pandera-based schema validation
│   │   ├── metric_checker.py       # MultiQC metric threshold checks
│   │   └── design_checker.py       # Experimental design warnings
│   ├── store/
│   │   ├── __init__.py
│   │   ├── registry.py             # DuckDB feature registration
│   │   ├── pull.py                 # bouncer.pull_data() implementation
│   │   └── transforms.py           # TPM, RPKM, custom transforms
│   ├── models/
│   │   ├── __init__.py
│   │   ├── finding.py              # Finding/Report Pydantic models
│   │   └── provenance.py           # Provenance dict model
│   └── utils/
│       ├── __init__.py
│       ├── hashing.py              # File hash computation
│       └── version.py              # YAML version parsing
├── contracts/
│   ├── rna-seq-schema.yaml         # Example RNA-seq schema contract
│   └── rna-seq-qc.yaml            # Example RNA-seq QC contract
├── tests/
│   ├── test_cli.py
│   ├── test_agent.py
│   ├── test_schema_validator.py
│   ├── test_metric_checker.py
│   ├── test_design_checker.py
│   ├── test_registry.py
│   ├── test_pull.py
│   ├── test_transforms.py
│   └── fixtures/
│       ├── counts.tsv              # Toy counts matrix
│       ├── samplesheet.csv         # Toy sample sheet
│       ├── multiqc_data.json       # Toy MultiQC output
│       └── protocol.pdf            # Toy protocol doc
└── docs/
    ├── quickstart.md
    └── yaml_contract_reference.md
```

---

## Phase 1: Project Scaffolding & YAML Contracts

### Step 1.1 — Initialize project

```bash
mkdir bouncer && cd bouncer
uv init
```

Set up `pyproject.toml` with dependencies:
- typer, rich (CLI)
- pydantic>=2.0 (config validation)
- pandera (schema validation)
- langgraph, langchain-anthropic (agent)
- pdfplumber (PDF parsing)
- anndata, scanpy (AnnData objects)
- duckdb (feature store)
- pytest, hypothesis (testing)

### Step 1.2 — Define YAML contract Pydantic models (`bouncer/config.py`)

Both schema and QC YAML files must carry a `version` field. BOUNCER validates this on load.

**Schema YAML model:**

```python
from pydantic import BaseModel
from typing import Literal

class ColumnDef(BaseModel):
    name: str
    dtype: Literal["str", "int", "float", "category"]
    required: bool = True

class SchemaContract(BaseModel):
    version: str                    # e.g. "1.0.0" — REQUIRED
    assay_type: str                 # e.g. "rna-seq"
    data_stage: str                 # e.g. "raw_counts", "normalized", "tpm"
    index_column: str               # e.g. "gene_id"
    sample_columns: str             # e.g. "from_samplesheet" or explicit list
    metadata_columns: list[ColumnDef]
    output_features: list[str]      # columns/matrices to push to feature store
```

**QC YAML model:**

```python
class MultiqcRule(BaseModel):
    metric: str
    min: float | None = None
    max: float | None = None
    severity: Literal["hard", "soft"]

class CountsCheck(BaseModel):
    check: str
    value: float | None = None
    severity: Literal["hard", "soft"]

class DesignCheck(BaseModel):
    check: str
    value: float | int | None = None
    max_imbalance_ratio: float | None = None
    std_dev_threshold: float | None = None
    labels: list[str] | None = None
    severity: Literal["warning"]     # design checks are always warnings

class QCContract(BaseModel):
    version: str                     # e.g. "1.0.0" — REQUIRED
    assay_type: str
    pipeline: str                    # e.g. "nf-core/rnaseq"
    data_stage: str

    multiqc: list[MultiqcRule] = []
    counts_matrix: list[CountsCheck] = []
    design: list[DesignCheck] = []
```

### Step 1.3 — Create example RNA-seq contracts

**`contracts/rna-seq-schema.yaml`:**

```yaml
version: "1.0.0"
assay_type: rna-seq
data_stage: raw_counts
index_column: gene_id
sample_columns: from_samplesheet

metadata_columns:
  - name: sample_id
    dtype: str
    required: true
  - name: condition
    dtype: category
    required: true
  - name: replicate
    dtype: int
    required: true
  - name: batch
    dtype: str
    required: false
  - name: cell_line
    dtype: str
    required: false
  - name: treatment
    dtype: str
    required: false
  - name: timepoint
    dtype: str
    required: false
  - name: organism
    dtype: str
    required: true
  - name: sex
    dtype: category
    required: false
  - name: passage
    dtype: int
    required: false

output_features:
  - counts_matrix
  - sample_metadata
```

**`contracts/rna-seq-qc.yaml`:**

```yaml
version: "1.0.0"
assay_type: rna-seq
pipeline: nf-core/rnaseq
data_stage: raw_counts

multiqc:
  - metric: STAR_percent_uniquely_mapped
    min: 0.75
    severity: hard
  - metric: FastQC_percent_duplicates
    max: 0.50
    severity: hard
  - metric: Salmon_num_mapped
    min: 1000000
    severity: hard
  - metric: FastQC_percent_gc
    min: 0.35
    max: 0.65
    severity: soft

counts_matrix:
  - check: no_all_zero_samples
    severity: hard
  - check: min_expressed_genes
    value: 5000
    severity: soft

design:
  - check: min_replicates_per_condition
    value: 3
    severity: warning
  - check: balanced_groups
    max_imbalance_ratio: 2
    severity: warning
  - check: control_condition_present
    labels: [control, untreated, DMSO, wildtype, WT, vehicle]
    severity: warning
  - check: batch_not_confounded_with_condition
    severity: warning
  - check: library_size_outliers
    std_dev_threshold: 2
    severity: warning
  - check: condition_label_consistency
    severity: warning
```

---

## Phase 2: Agent Layer (LangGraph)

### Step 2.1 — Define agent state (`bouncer/agent/state.py`)

```python
from typing import TypedDict, Literal

class FileInfo(TypedDict):
    path: str
    file_type: Literal[
        "multiqc_json", "counts_matrix",
        "sample_sheet", "protocol_document"
    ]
    read_strategy: str
    content: dict | str | None

class Finding(TypedDict):
    severity: Literal["hard", "soft", "warning"]
    stage: Literal["schema", "qc", "design", "protocol"]
    source_file: str
    field: str
    expected: str
    found: str
    fix_location: str
    message: str

class BouncerState(TypedDict):
    input_files: list[str]
    schema_contract: dict
    qc_contract: dict
    file_map: list[FileInfo]
    extracted_data: dict
    tags: dict
    findings: list[Finding]
    missing_fields: list[str]
    report: str
    mode: Literal["strict", "permissive"]
    passed: bool
```

### Step 2.2 — Build agent tools (`bouncer/agent/tools/`)

Each tool is a `@tool` decorated function that the agent calls during extraction:

**`peek_file.py`** — Read first 20 lines of any file for classification
**`read_csv.py`** — Full CSV/TSV read, return headers, shape, dtypes, sample rows
**`read_json.py`** — Read JSON, optionally extract specific keys (for MultiQC `general_stats`)
**`read_pdf.py`** — Extract text from PDF using pdfplumber, return as string

### Step 2.3 — Build graph nodes (`bouncer/agent/nodes/`)

**`classify.py`** — Classify files node:
- Uses `peek_file` tool on each input file
- LLM prompt: "Given these file previews, classify each as multiqc_json, counts_matrix, sample_sheet, or protocol_document. Explain your reasoning."
- Updates `file_map` in state with type and read strategy per file
- If a file can't be classified after 2 attempts, mark as unknown and surface in report

**`extract.py`** — Extract data node:
- Iterates over `file_map`, calls appropriate reader tool per file type
- For MultiQC: extracts `report_general_stats` and `report_data_sources`
- For counts matrix: reads full matrix, extracts sample IDs (columns), gene IDs (rows), shape
- For sample sheet: reads full table, extracts column names and unique values per column
- For protocol PDF: extracts full text, sends to LLM to pull structured parameters (passage range, reagent lots, treatment duration, timepoints, conditions) as JSON
- Stores all extracted data in `extracted_data` dict keyed by file type

**`tag.py`** — Dynamic tagging node:
- LLM reads all extracted data and assigns tags based on what it found
- Cross-references sample IDs across counts matrix and sample sheet (must match)
- Determines: assay_type, organism, treatments, cell_lines, conditions from sample sheet
- Stores in `tags` dict

**`cross_reference.py`** — Validation node (the core logic):
- Runs schema validation using Pandera against sample sheet
- Runs MultiQC metric checks against QC YAML thresholds
- Runs counts matrix checks (zero samples, min genes)
- Runs design checks (replicates, balance, batch confounding, control present, label consistency)
- Cross-references protocol doc extracted parameters against sample sheet metadata
- Each failure/warning creates a Finding object appended to `findings`
- Identifies missing fields (required by schema but not found in any file)

**`report.py`** — Report generation node:
- Reads all findings, groups by stage
- Formats Rich terminal output with the three-stage summary header
- Determines `passed` boolean based on mode (strict: any hard failure = fail; permissive: hard failures flagged but pass)

### Step 2.4 — Assemble LangGraph (`bouncer/agent/graph.py`)

```python
from langgraph.graph import StateGraph, END
from bouncer.agent.state import BouncerState
from bouncer.agent.nodes import classify, extract, tag, cross_reference, report

builder = StateGraph(BouncerState)

builder.add_node("classify_files", classify.run)
builder.add_node("extract_data", extract.run)
builder.add_node("assign_tags", tag.run)
builder.add_node("cross_reference", cross_reference.run)
builder.add_node("generate_report", report.run)

builder.set_entry_point("classify_files")
builder.add_edge("classify_files", "extract_data")
builder.add_edge("extract_data", "assign_tags")
builder.add_edge("assign_tags", "cross_reference")
builder.add_edge("cross_reference", "generate_report")
builder.add_edge("generate_report", END)

graph = builder.compile()
```

Add `max_iterations=10` as a safety cap.

---

## Phase 3: QC Engine

### Step 3.1 — Schema validator (`bouncer/qc/schema_validator.py`)

Uses Pandera to validate the sample sheet DataFrame against the schema YAML:
- Check all required columns exist
- Check dtype of each column
- Check index column exists in counts matrix
- Check sample IDs in sample sheet match column headers in counts matrix
- Return list of Findings for any failures

### Step 3.2 — Metric checker (`bouncer/qc/metric_checker.py`)

Reads MultiQC JSON `general_stats` section. For each rule in QC YAML `multiqc` section:
- Look up the metric value per sample
- Check against min/max thresholds
- Create Finding with severity=hard or soft per rule

### Step 3.3 — Design checker (`bouncer/qc/design_checker.py`)

Implements each design check as a function:

```python
def check_min_replicates(samplesheet_df, condition_col, min_reps):
    """Group by condition, flag groups with < min_reps samples"""

def check_balanced_groups(samplesheet_df, condition_col, max_ratio):
    """Flag if largest group / smallest group > max_ratio"""

def check_control_present(samplesheet_df, condition_col, control_labels):
    """Flag if no condition value matches any control label"""

def check_batch_confounded(samplesheet_df, condition_col, batch_col):
    """Flag if batch and condition are perfectly correlated"""

def check_library_size_outliers(multiqc_stats, std_dev_threshold):
    """Flag samples with read count > N std devs from group mean"""

def check_condition_label_consistency(samplesheet_df, condition_col):
    """Use fuzzy matching to detect near-duplicate condition labels"""

def check_factorial_completeness(samplesheet_df, factor_cols):
    """Check all combinations of factor columns are represented"""
```

All return `Finding` objects with `severity: "warning"`.

---

## Phase 4: Feature Store

### Step 4.1 — DuckDB registry (`bouncer/store/registry.py`)

Initialize DuckDB database and features table on first run:

```sql
CREATE TABLE IF NOT EXISTS features (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    assay_type VARCHAR NOT NULL,
    data_stage VARCHAR NOT NULL,
    organism VARCHAR,
    conditions VARCHAR[],
    treatments VARCHAR[],
    cell_lines VARCHAR[],
    sample_ids VARCHAR[],
    tags JSON,
    qc_mode VARCHAR NOT NULL,           -- 'strict' or 'permissive'
    qc_status VARCHAR NOT NULL,         -- 'passed', 'passed_with_warnings', 'partial'
    warnings JSON,                      -- design warnings carried forward
    schema_version VARCHAR NOT NULL,
    qc_version VARCHAR NOT NULL,
    input_hashes JSON NOT NULL,         -- {filename: sha256}
    h5ad_path VARCHAR NOT NULL,
    provenance JSON NOT NULL,
    created_at TIMESTAMP DEFAULT now(),
    parent_id UUID                      -- for transformed data: points to source
);
```

**Register function:**

```python
def register(adata: AnnData, state: BouncerState, db_path: str, h5ad_dir: str) -> str:
    """
    1. Build provenance dict from state
    2. Attach provenance to adata.uns['provenance']
    3. Attach warnings to adata.uns['warnings']
    4. Write adata to h5ad file (named by UUID)
    5. Insert metadata row into DuckDB
    6. Return feature ID
    """
```

### Step 4.2 — Build AnnData from validated inputs

After QC passes, construct the AnnData object:

```python
import anndata as ad
import pandas as pd

def build_adata(counts_df, samplesheet_df, tags, provenance):
    """
    counts_df: genes x samples DataFrame
    samplesheet_df: samples x metadata DataFrame

    Returns AnnData with:
    - X = counts matrix (genes x samples)
    - obs = sample metadata from sample sheet
    - var = gene metadata (gene_id as index)
    - uns = {
        'provenance': [provenance_dict],
        'tags': tags,
        'schema_version': ...,
        'qc_version': ...,
      }
    """
    adata = ad.AnnData(
        X=counts_df.values.T,          # AnnData expects samples x genes
        obs=samplesheet_df.set_index('sample_id'),
        var=pd.DataFrame(index=counts_df.index)
    )
    adata.uns['provenance'] = [provenance]
    adata.uns['tags'] = tags
    return adata
```

### Step 4.3 — Provenance model (`bouncer/models/provenance.py`)

```python
from pydantic import BaseModel
from datetime import datetime

class ProvenanceEntry(BaseModel):
    stage: str                        # "raw_counts", "tpm", "rpkm"
    schema_version: str
    qc_version: str
    input_hashes: dict[str, str]      # {filename: sha256}
    pipeline: str | None = None       # "nf-core/rnaseq"
    pipeline_version: str | None = None
    parent_feature_id: str | None = None  # if transformed from another feature
    timestamp: datetime
    bouncer_version: str
```

---

## Phase 5: Pull API

### Step 5.1 — `bouncer.pull_data()` (`bouncer/store/pull.py`)

```python
import duckdb
import anndata as ad

def pull_data(
    db_path: str = "~/.bouncer/bouncer.db",
    h5ad_dir: str = "~/.bouncer/features/",
    assay: list[str] | None = None,
    treatment: list[str] | None = None,
    condition: list[str] | None = None,
    cell_line: list[str] | None = None,
    organism: str | None = None,
    data_stage: str | None = None,
    qc_status: str = "passed",
    version: str | None = None,          # specific schema version
    feature_id: str | None = None,       # pull specific feature by ID
    include_warnings: bool = False,      # include passed_with_warnings
) -> ad.AnnData:
    """
    1. Build SQL WHERE clause from filters
    2. Query DuckDB for matching feature rows
    3. Load corresponding h5ad files
    4. Concatenate into single AnnData (ad.concat)
    5. Return with full provenance chain in .uns
    """
```

**Usage:**

```python
import bouncer

# Pull all RNA-seq with treatment ABC
adata = bouncer.pull_data(assay=['rna-seq'], treatment=['ABC'])

# Pull specific data stage
adata = bouncer.pull_data(assay=['rna-seq'], data_stage='tpm')

# Pull specific version
adata = bouncer.pull_data(assay=['rna-seq'], version='1.0.0')

# Pull by feature ID
adata = bouncer.pull_data(feature_id='abc-123-def')

# Check provenance
print(adata.uns['provenance'])

# Check warnings
print(adata.uns.get('warnings', []))
```

### Step 5.2 — List available features

```python
def list_features(
    db_path: str = "~/.bouncer/bouncer.db",
    assay: str | None = None,
    data_stage: str | None = None,
) -> pd.DataFrame:
    """
    Returns a summary DataFrame of all registered features
    with id, assay_type, data_stage, sample count,
    schema_version, qc_version, created_at
    """
```

---

## Phase 6: Transformations

### Step 6.1 — Built-in transforms (`bouncer/store/transforms.py`)

```python
import numpy as np
import anndata as ad

def to_tpm(adata: ad.AnnData, gene_lengths: pd.Series) -> ad.AnnData:
    """
    Convert raw counts to TPM.
    gene_lengths: Series indexed by gene_id with gene length in bp.
    Returns new AnnData with data_stage='tpm' in provenance.
    """

def to_rpkm(adata: ad.AnnData, gene_lengths: pd.Series) -> ad.AnnData:
    """
    Convert raw counts to RPKM.
    Returns new AnnData with data_stage='rpkm' in provenance.
    """

def to_log2(adata: ad.AnnData, pseudocount: float = 1.0) -> ad.AnnData:
    """
    Log2 transform with pseudocount.
    Returns new AnnData with data_stage='log2' in provenance.
    """

def normalize_deseq2(adata: ad.AnnData) -> ad.AnnData:
    """
    DESeq2-style median-of-ratios normalization.
    Returns new AnnData with data_stage='deseq2_normalized' in provenance.
    """

def custom_transform(adata: ad.AnnData, func: callable, stage_name: str) -> ad.AnnData:
    """
    Apply any user-defined function to adata.X.
    User provides the stage name for provenance tracking.
    """
```

### Step 6.2 — Transform + re-register workflow

```python
import bouncer

# Pull raw counts
adata = bouncer.pull_data(assay=['rna-seq'], data_stage='raw_counts')

# Transform to TPM
adata_tpm = bouncer.transforms.to_tpm(adata, gene_lengths=gene_lengths_df)

# Re-register the transformed data
# This appends a new provenance entry with parent_feature_id pointing to source
# and requires a schema YAML for the tpm data stage
bouncer.register(
    adata_tpm,
    schema="contracts/rna-seq-tpm-schema.yaml",
    mode="strict"
)

# Now queryable
adata_tpm = bouncer.pull_data(assay=['rna-seq'], data_stage='tpm')
```

**Re-registration runs QC again.** The TPM data gets validated against a different schema YAML (one appropriate for normalized data — different expected ranges, no zero-count checks, etc.). This is the composability feature — BOUNCER reads its own AnnData output as input.

### Step 6.3 — TPM/RPKM schema YAML example

```yaml
version: "1.0.0"
assay_type: rna-seq
data_stage: tpm

index_column: gene_id
sample_columns: from_parent

metadata_columns:
  - name: sample_id
    dtype: str
    required: true
  - name: condition
    dtype: category
    required: true

output_features:
  - tpm_matrix
  - sample_metadata
```

```yaml
version: "1.0.0"
assay_type: rna-seq
pipeline: derived
data_stage: tpm

multiqc: []     # no multiqc checks for derived data

counts_matrix:
  - check: no_negative_values
    severity: hard
  - check: no_nan_values
    severity: hard
  - check: column_sums_approximately_equal
    tolerance: 0.01
    severity: soft    # TPM columns should sum to ~1M

design: []      # inherit warnings from parent
```

---

## Phase 7: CLI

### Step 7.1 — CLI commands (`bouncer/cli.py`)

```python
import typer
from rich.console import Console

app = typer.Typer()
console = Console()

@app.command()
def run(
    inputs: list[str] = typer.Argument(..., help="Input data files"),
    schema: str = typer.Option(..., help="Path to schema YAML"),
    qc: str = typer.Option(..., help="Path to QC YAML"),
    mode: str = typer.Option("strict", help="strict or permissive"),
    db: str = typer.Option("~/.bouncer/bouncer.db", help="DuckDB path"),
):
    """Run BOUNCER QC agent on input files."""

@app.command()
def register(
    h5ad: str = typer.Argument(..., help="Path to AnnData h5ad file"),
    schema: str = typer.Option(..., help="Path to schema YAML"),
    mode: str = typer.Option("strict"),
):
    """Register a pre-built AnnData to the feature store."""

@app.command()
def list_features(
    assay: str = typer.Option(None),
    data_stage: str = typer.Option(None),
):
    """List all registered features."""

@app.command()
def pull(
    assay: str = typer.Option(None),
    data_stage: str = typer.Option(None),
    treatment: str = typer.Option(None),
    output: str = typer.Option("output.h5ad", help="Output h5ad path"),
):
    """Pull features to an h5ad file."""
```

### Step 7.2 — CLI usage examples

```bash
# Run QC on pipeline outputs
bouncer run results/multiqc_data.json results/counts.tsv samplesheet.csv protocol.pdf \
  --schema contracts/rna-seq-schema.yaml \
  --qc contracts/rna-seq-qc.yaml \
  --mode strict

# List what's in the feature store
bouncer list-features --assay rna-seq

# Pull data to file
bouncer pull --assay rna-seq --treatment ABC --output my_data.h5ad
```

---

## Phase 8: Testing

### Step 8.1 — Test fixtures (`tests/fixtures/`)

Create minimal test data:
- `counts.tsv`: 100 genes x 6 samples, integer counts, 1 all-zero sample
- `samplesheet.csv`: 6 samples, 2 conditions (treated/control), 3 replicates each, with organism, treatment columns
- `multiqc_data.json`: Minimal general_stats section with STAR, FastQC, Salmon metrics per sample (1 sample below alignment threshold)
- `protocol.pdf`: Simple 1-page PDF specifying passage range 3-7, 24h treatment

### Step 8.2 — Unit tests

- `test_schema_validator.py`: Test required column detection, dtype validation, sample ID matching
- `test_metric_checker.py`: Test threshold checks against MultiQC JSON, hard vs soft severity
- `test_design_checker.py`: Test each design check independently (replicates, balance, batch confounding, control present, label consistency)
- `test_registry.py`: Test DuckDB registration, provenance attachment, h5ad write/read
- `test_pull.py`: Test filtering by assay, treatment, data_stage, version, feature_id
- `test_transforms.py`: Test TPM calculation correctness, provenance chain after transform, re-registration

### Step 8.3 — Integration test

- `test_agent.py`: Full end-to-end run with test fixtures:
  1. Agent classifies files correctly
  2. Extracts data from each
  3. Finds the all-zero sample (hard failure)
  4. Finds the below-threshold alignment sample (hard failure)
  5. Reports 2-replicate warning (design)
  6. Generates correct report
  7. After user "fixes" (swap in clean fixtures), rerun passes
  8. Data registers to DuckDB
  9. `bouncer.pull_data()` returns correct AnnData with provenance

---

## Phase 9: Implementation Order (for Claude Code)

Execute in this order — each phase builds on the previous:

1. **Scaffolding**: `pyproject.toml`, directory structure, `uv sync`
2. **Config models**: `config.py` with Pydantic models for both YAMLs, version validation
3. **Example contracts**: Both RNA-seq YAML files
4. **QC engine**: `schema_validator.py`, `metric_checker.py`, `design_checker.py` — pure functions, no agent dependency, fully testable
5. **Unit tests for QC**: Tests for all three checkers with fixtures
6. **Agent tools**: `peek_file.py`, `read_csv.py`, `read_json.py`, `read_pdf.py`
7. **Agent nodes**: `classify.py`, `extract.py`, `tag.py`, `cross_reference.py`, `report.py`
8. **Agent graph**: `graph.py` assembly + `state.py`
9. **CLI run command**: Wire agent to Typer CLI
10. **Feature store**: `registry.py` with DuckDB init + register function
11. **AnnData builder**: Build adata from validated data
12. **Pull API**: `pull.py` with all filters
13. **CLI pull/list commands**: Wire to Typer
14. **Transforms**: `to_tpm`, `to_rpkm`, `to_log2`, `normalize_deseq2`, `custom_transform`
15. **Re-registration**: Transform → QC → register loop
16. **Integration tests**: Full end-to-end test
17. **README + docs**: Quickstart, YAML reference

---

## Version Strategy

- Schema YAML version: tracks structural changes to expected columns/types
- QC YAML version: tracks changes to thresholds and rules
- Provenance records both versions for every registered feature
- If a user updates a YAML version, existing features in the store retain their original version — nothing is retroactively revalidated
- The `bouncer.pull_data(version='1.0.0')` filter matches schema_version

---

## Key Design Decisions Summary

| Decision | Choice | Rationale |
|---|---|---|
| Agent framework | LangGraph | Stateful graph, can loop and retry |
| LLM | Claude (via langchain-anthropic) | Best at document understanding for protocol parsing |
| Schema validation | Pandera + Pydantic | DataFrame-native, type-safe configs |
| Feature store metadata | DuckDB | Embedded, zero infra, SQL queryable |
| Feature store data | h5ad (AnnData) | Standard format, scanpy/ML compatible |
| CLI | Typer + Rich | Type-safe, beautiful terminal output |
| Package manager | uv | Fast, modern Python packaging |
| Testing | pytest + hypothesis | Property-based testing for QC rules |
| Contracts | Versioned YAML | Human-readable, git-trackable, versionable |
| Design checks | Warnings only | Never block, always inform |
| Transforms | Built-in + custom | TPM/RPKM built in, user can add any function |
