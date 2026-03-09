#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────────────────────
# Bouncer — macOS / Linux install script
#
# Usage:
#   chmod +x scripts/install.sh
#   ./scripts/install.sh
# ──────────────────────────────────────────────────────────────────────────────
set -euo pipefail

BOLD="\033[1m"
GREEN="\033[0;32m"
YELLOW="\033[0;33m"
RED="\033[0;31m"
RESET="\033[0m"

info()    { echo -e "${BOLD}[bouncer]${RESET} $*"; }
success() { echo -e "${GREEN}[bouncer] ✓${RESET} $*"; }
warn()    { echo -e "${YELLOW}[bouncer] ⚠${RESET} $*"; }
error()   { echo -e "${RED}[bouncer] ✗${RESET} $*" >&2; exit 1; }

# ── 1. Check Python 3.11+ ─────────────────────────────────────────────────────
info "Checking Python version..."
if ! command -v python3 &>/dev/null; then
    error "Python 3 not found. Install Python 3.11+ from https://python.org"
fi

PY_VERSION=$(python3 -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
PY_MAJOR=$(echo "$PY_VERSION" | cut -d. -f1)
PY_MINOR=$(echo "$PY_VERSION" | cut -d. -f2)

if [[ "$PY_MAJOR" -lt 3 || ("$PY_MAJOR" -eq 3 && "$PY_MINOR" -lt 11) ]]; then
    error "Python 3.11+ required (found $PY_VERSION). Install from https://python.org"
fi
success "Python $PY_VERSION found"

# ── 2. Install uv if missing ──────────────────────────────────────────────────
if ! command -v uv &>/dev/null; then
    info "Installing uv (fast Python package manager)..."
    curl -LsSf https://astral.sh/uv/install.sh | sh
    # uv installs to ~/.local/bin — add to PATH for this session
    export PATH="$HOME/.local/bin:$PATH"
    success "uv installed"
else
    success "uv $(uv --version) already installed"
fi

# ── 3. Install bouncer package ────────────────────────────────────────────────
info "Installing bouncer..."
uv pip install -e "." --system 2>/dev/null \
    || uv pip install -e "."
success "bouncer installed"

# ── 4. Verify the CLI is available ───────────────────────────────────────────
if ! command -v bouncer &>/dev/null; then
    warn "'bouncer' not found in PATH after install."
    warn "Try: export PATH=\"\$HOME/.local/bin:\$PATH\""
    warn "     or add that line to your ~/.zshrc / ~/.bashrc"
else
    success "CLI ready: $(bouncer --version 2>/dev/null || echo 'bouncer')"
fi

# ── 5. Prompt for API URL ─────────────────────────────────────────────────────
echo ""
info "Enter your Modal API URL (leave blank to skip):"
info "  Find it with: modal app list"
read -rp "  BOUNCER_API_URL: " API_URL

if [[ -n "$API_URL" ]]; then
    SHELL_RC=""
    if [[ "$SHELL" == *"zsh"* ]]; then
        SHELL_RC="$HOME/.zshrc"
    elif [[ "$SHELL" == *"bash"* ]]; then
        SHELL_RC="$HOME/.bashrc"
    fi

    if [[ -n "$SHELL_RC" ]]; then
        echo "" >> "$SHELL_RC"
        echo "# Bouncer API" >> "$SHELL_RC"
        echo "export BOUNCER_API_URL=\"$API_URL\"" >> "$SHELL_RC"
        success "BOUNCER_API_URL saved to $SHELL_RC"
        info "Run: source $SHELL_RC  (or open a new terminal)"
    else
        warn "Unknown shell. Add manually:"
        warn "  export BOUNCER_API_URL=\"$API_URL\""
    fi
else
    warn "No URL set. Pass it at runtime with --api-url or set BOUNCER_API_URL."
fi

echo ""
success "Installation complete!"
echo ""
echo "  Run the RNA-seq example:"
echo "    chmod +x scripts/run_rna_seq.sh && ./scripts/run_rna_seq.sh"
echo ""
echo "  Or directly:"
echo "    bouncer run 'RNA-seq/salmon.merged.gene_counts.tsv' \\"
echo "      'RNA-seq/samplesheet - correct.csv' \\"
echo "      'RNA-seq/multiqc_data.json' \\"
echo "      'RNA-seq/rna-seq protocol.pdf' \\"
echo "      --assay rna-seq --schema rna-seq/basic --mode strict"
echo ""
