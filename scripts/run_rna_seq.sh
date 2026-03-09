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
  "data/RNA-seq/correct/salmon.merged.gene_counts.tsv" \
  "data/RNA-seq/correct/samplesheet - correct.csv" \
  "data/RNA-seq/correct/multiqc_data_correct.json" \
  "data/RNA-seq/correct/rna-seq protocol.pdf" \
  --assay   rna-seq \
  --schema  "schemas/rna-seq/basic-schema.yaml" \
  --qc      "schemas/rna-seq/basic-qc.yaml" \
  --mode    strict \
  --api-url "$BOUNCER_API_URL"
