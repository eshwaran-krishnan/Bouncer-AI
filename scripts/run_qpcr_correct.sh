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
  "data/qPCR/correct/Ct.csv" \
  "data/qPCR/correct/samplesheet-qpcr - correct.csv" \
  "data/qPCR/correct/protocol.pdf" \
  --assay   qPCR \
  --schema  "schemas/qPCR/basic-schema.yaml" \
  --qc      "schemas/qPCR/basic-qc.yaml" \
  --mode    strict \
  --api-url "$BOUNCER_API_URL"
