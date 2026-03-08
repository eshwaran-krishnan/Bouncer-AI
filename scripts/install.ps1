# ──────────────────────────────────────────────────────────────────────────────
# Bouncer -- Windows install script (PowerShell)
#
# Usage (run from the project root):
#   Set-ExecutionPolicy -Scope CurrentUser -ExecutionPolicy RemoteSigned
#   .\scripts\install.ps1
# ──────────────────────────────────────────────────────────────────────────────

$ErrorActionPreference = "Stop"

function Info    { param($msg) Write-Host "[bouncer] $msg" -ForegroundColor White }
function Success { param($msg) Write-Host "[bouncer] OK $msg" -ForegroundColor Green }
function Warn    { param($msg) Write-Host "[bouncer] WARN $msg" -ForegroundColor Yellow }
function Fail    { param($msg) Write-Host "[bouncer] FAIL $msg" -ForegroundColor Red; exit 1 }

# ── 1. Check Python 3.11+ ─────────────────────────────────────────────────────
Info "Checking Python version..."
try {
    $pyVersion = python --version 2>&1
    if ($pyVersion -match "Python (\d+)\.(\d+)") {
        $major = [int]$Matches[1]
        $minor = [int]$Matches[2]
        if ($major -lt 3 -or ($major -eq 3 -and $minor -lt 11)) {
            Fail "Python 3.11+ required (found $pyVersion). Install from https://python.org"
        }
        Success "$pyVersion found"
    } else {
        Fail "Could not parse Python version. Install Python 3.11+ from https://python.org"
    }
} catch {
    Fail "Python not found. Install Python 3.11+ from https://python.org"
}

# ── 2. Install uv if missing ──────────────────────────────────────────────────
if (-not (Get-Command uv -ErrorAction SilentlyContinue)) {
    Info "Installing uv (fast Python package manager)..."
    powershell -Command "irm https://astral.sh/uv/install.ps1 | iex"
    # Refresh PATH for this session
    $env:PATH = [System.Environment]::GetEnvironmentVariable("PATH", "User") + ";" + $env:PATH
    Success "uv installed"
} else {
    $uvVer = uv --version
    Success "uv $uvVer already installed"
}

# ── 3. Install bouncer package ────────────────────────────────────────────────
Info "Installing bouncer..."
try {
    uv pip install -e "." --system
} catch {
    uv pip install -e "."
}
Success "bouncer installed"

# ── 4. Verify CLI ─────────────────────────────────────────────────────────────
if (-not (Get-Command bouncer -ErrorAction SilentlyContinue)) {
    Warn "'bouncer' not found in PATH."
    Warn "Add your Python Scripts folder to PATH, e.g.:"
    Warn "  %APPDATA%\Python\Python311\Scripts"
} else {
    Success "CLI ready"
}

# ── 5. Prompt for API URL ─────────────────────────────────────────────────────
Write-Host ""
Info "Enter your Modal API URL (leave blank to skip):"
Info "  Find it with: modal app list"
$apiUrl = Read-Host "  BOUNCER_API_URL"

if ($apiUrl -ne "") {
    # Persist to user environment (survives reboots, no admin required)
    [System.Environment]::SetEnvironmentVariable("BOUNCER_API_URL", $apiUrl, "User")
    $env:BOUNCER_API_URL = $apiUrl
    Success "BOUNCER_API_URL saved to user environment variables"
    Info "Open a new terminal for the variable to take effect."
} else {
    Warn "No URL set. Pass it at runtime with --api-url or set BOUNCER_API_URL."
}

Write-Host ""
Success "Installation complete!"
Write-Host ""
Write-Host "  Run the RNA-seq example:"
Write-Host "    .\scripts\run_rna_seq.bat"
Write-Host ""
Write-Host "  Or directly:"
Write-Host "    bouncer run 'RNA-seq/salmon.merged.gene_counts.tsv' ``"
Write-Host "      'RNA-seq/samplesheet - correct.csv' ``"
Write-Host "      'RNA-seq/multiqc_data.json' ``"
Write-Host "      'RNA-seq/rna-seq protocol.pdf' ``"
Write-Host "      --assay rna-seq --schema rna-seq/basic --mode strict"
Write-Host ""
