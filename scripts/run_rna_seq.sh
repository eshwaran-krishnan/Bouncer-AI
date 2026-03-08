#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────────────────────
# Bouncer — RNA-seq QC example run
#
# Usage:
#   chmod +x scripts/run_rna_seq.sh
#   ./scripts/run_rna_seq.sh
#
# Or set the API URL inline:
#   BOUNCER_API_URL=https://your-org--bouncer-qc-api.modal.run ./scripts/run_rna_seq.sh
# ──────────────────────────────────────────────────────────────────────────────

# Set your deployed Modal API URL here (or export it in your shell first)
: "${BOUNCER_API_URL:=https://lol882192--bouncer-qc-api.modal.run}"  # override with your URL if different

bouncer run \
  "RNA-seq/salmon.merged.gene_counts.tsv" \
  "RNA-seq/samplesheet - correct.csv" \
  "RNA-seq/multiqc_data.json" \
  "RNA-seq/rna-seq protocol.pdf" \
  --assay   rna-seq \
  --schema  rna-seq/basic \
  --mode    strict \
  --api-url "$BOUNCER_API_URL"
