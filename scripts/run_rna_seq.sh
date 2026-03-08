#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────────────────────
# Bouncer — RNA-seq QC example run (macOS / Linux)
#
# Usage:
#   chmod +x scripts/run_rna_seq.sh
#   ./scripts/run_rna_seq.sh
# ──────────────────────────────────────────────────────────────────────────────

: "${BOUNCER_API_URL:=https://lol882192--bouncer-qc-api.modal.run}"

bouncer run \
  "RNA-seq/salmon.merged.gene_counts.tsv" \
  "RNA-seq/samplesheet - correct.csv" \
  "RNA-seq/multiqc_data.json" \
  "RNA-seq/rna-seq protocol.pdf" \
  --assay   rna-seq \
  --schema  "schemas/rna-seq/basic-schema.yaml" \
  --qc      "schemas/rna-seq/basic-qc.yaml" \
  --mode    strict \
  --api-url "$BOUNCER_API_URL"
