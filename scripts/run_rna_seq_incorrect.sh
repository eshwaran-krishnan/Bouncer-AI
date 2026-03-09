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
  "data/RNA-seq/incorrect/salmon.merged.gene_counts.tsv" \
  "data/RNA-seq/incorrect/samplesheet - incorrect.csv" \
  "data/RNA-seq/incorrect/multiqc_data_incorrect.json" \
  "data/RNA-seq/incorrect/rna-seq protocol.pdf" \
  --assay   rna-seq \
  --schema  "schemas/rna-seq/basic-schema.yaml" \
  --qc      "schemas/rna-seq/basic-qc.yaml" \
  --mode    strict \
  --api-url "$BOUNCER_API_URL"
