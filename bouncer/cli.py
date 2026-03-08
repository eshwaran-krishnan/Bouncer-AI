"""
bouncer/cli.py — Typer CLI (API-client mode).

Quickstart — use a bundled schema name:
    bouncer run counts.tsv samplesheet.csv multiqc.json \\
        --schema rna-seq/basic

Or pass local schema files explicitly:
    bouncer run counts.tsv samplesheet.csv \\
        --assay   rna-seq \\
        --schema  schemas/rna-seq/basic-schema.yaml \\
        --qc      schemas/rna-seq/basic-qc.yaml

Set your API URL:
    export BOUNCER_API_URL=https://your-org--bouncer-api.modal.run
    bouncer ping   # verify the connection
"""

from __future__ import annotations

import os
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

app     = typer.Typer(
    help="Bouncer — biological data quality contract layer.",
    no_args_is_help=True,
)
console = Console()

_POLL_INTERVAL = 3
_POLL_MAX      = 200  # ~10 min ceiling

# Bundled schemas live at <project_root>/schemas/ (sibling of the bouncer/ package)
_SCHEMAS_ROOT = Path(__file__).parent.parent / "schemas"


# ── Helpers ────────────────────────────────────────────────────────────────────

def _resolve_api(api_url: str) -> str:
    url = api_url or os.environ.get("BOUNCER_API_URL", "")
    if not url:
        console.print(
            "[red]No API URL.[/red]  Set [bold]BOUNCER_API_URL[/bold] or pass [bold]--api-url[/bold].\n"
            "[dim]Example: export BOUNCER_API_URL=https://your-org--bouncer-api.modal.run[/dim]"
        )
        raise typer.Exit(1)
    return url.rstrip("/")


def _resolve_schema(
    schema_arg: str,
    qc_arg: Optional[str],
) -> tuple[Path, Path, Optional[str]]:
    """
    Resolve --schema and --qc to concrete file paths.

    Accepts two formats for --schema:
      • A schema name:  "rna-seq/basic"  → resolves from bundled schemas dir
      • A file path:    "path/to/basic-schema.yaml"  → used as-is

    Returns (schema_path, qc_path, inferred_assay_or_None).
    inferred_assay is set only when a schema name was used.
    """
    p = Path(schema_arg)

    # ── File path mode ──────────────────────────────────────────────────────
    if p.suffix in (".yaml", ".yml") or p.exists():
        schema_path = p
        if qc_arg:
            return schema_path, Path(qc_arg), None
        # Try to infer sibling QC file: basic-schema.yaml → basic-qc.yaml
        inferred_qc = p.parent / p.name.replace("-schema", "-qc")
        if inferred_qc.exists():
            console.print(f"[dim]Using inferred QC contract: {inferred_qc.name}[/dim]")
            return schema_path, inferred_qc, None
        console.print(
            f"[red]--qc is required when --schema is a file path "
            f"and no sibling *-qc.yaml was found next to {p.name}.[/red]"
        )
        raise typer.Exit(1)

    # ── Schema name mode: "rna-seq/basic" ──────────────────────────────────
    parts = schema_arg.split("/")
    if len(parts) != 2 or not all(parts):
        console.print(
            f"[red]Unrecognised --schema value '[bold]{schema_arg}[/bold]'.[/red]\n"
            "Use a bundled name like [cyan]rna-seq/basic[/cyan] "
            "or a local file path ending in [cyan].yaml[/cyan]."
        )
        raise typer.Exit(1)

    assay_dir, schema_base = parts
    schema_path = _SCHEMAS_ROOT / assay_dir / f"{schema_base}-schema.yaml"
    qc_path     = _SCHEMAS_ROOT / assay_dir / f"{schema_base}-qc.yaml"

    missing = [str(f) for f in (schema_path, qc_path) if not f.exists()]
    if missing:
        console.print(
            f"[red]Bundled schema '[bold]{schema_arg}[/bold]' not found.[/red]\n"
            + "\n".join(f"  Missing: {m}" for m in missing)
        )
        raise typer.Exit(1)

    if qc_arg:
        qc_path = Path(qc_arg)   # explicit override

    return schema_path, qc_path, assay_dir


def _mime(path: Path) -> str:
    return {
        ".json":  "application/json",
        ".yaml":  "application/x-yaml",
        ".yml":   "application/x-yaml",
        ".tsv":   "text/tab-separated-values",
        ".csv":   "text/csv",
        ".pdf":   "application/pdf",
        ".fcs":   "application/octet-stream",
        ".eds":   "application/octet-stream",
        ".h5ad":  "application/octet-stream",
    }.get(path.suffix.lower(), "application/octet-stream")


def _poll_job(base: str, job_id: str) -> dict | None:
    """Spin until the job is done; return result dict or None on failure."""
    import httpx

    with console.status(f"[bold]QC job running[/bold] [dim]({job_id})[/dim]") as status:
        for tick in range(_POLL_MAX):
            elapsed = tick * _POLL_INTERVAL
            status.update(
                f"[bold]QC job running[/bold] [dim]({job_id})[/dim] — {elapsed}s elapsed"
            )
            try:
                resp = httpx.get(f"{base}/qc/jobs/{job_id}", timeout=15)
                resp.raise_for_status()
            except httpx.HTTPError as exc:
                console.print(f"[red]Poll error: {exc}[/red]")
                return None

            payload = resp.json()
            job_status = payload.get("status")

            if job_status == "complete":
                return payload.get("result", {})

            if job_status == "failed":
                console.print(
                    f"[red]QC job failed:[/red] {payload.get('error', 'unknown error')}"
                )
                return None

            time.sleep(_POLL_INTERVAL)

    console.print("[red]Timed out waiting for QC job (>10 min).[/red]")
    return None


def _save_report_html(report: str, result: dict, schema: Path, qc: Path,
                      assay: str, mode: str, output: Path) -> None:
    """Write the QC report as a self-contained HTML file."""
    passed   = result.get("passed", False)
    tags     = result.get("tags", {})
    findings = result.get("findings", [])

    status_color = "#22c55e" if passed else "#ef4444"
    status_text  = "PASSED" if passed else "FAILED"
    n_hard    = sum(1 for f in findings if f.get("severity") == "hard")
    n_soft    = sum(1 for f in findings if f.get("severity") == "soft")
    n_warn    = sum(1 for f in findings if f.get("severity") == "warning")

    # Escape the plain-text report for HTML
    import html as html_lib
    report_escaped = html_lib.escape(report)

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Bouncer QC Report — {assay}</title>
  <style>
    * {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
            background: #0f172a; color: #e2e8f0; padding: 2rem; }}
    h1   {{ font-size: 1.5rem; font-weight: 700; margin-bottom: 0.25rem; }}
    .badge {{ display: inline-block; padding: 0.25rem 0.75rem; border-radius: 9999px;
              font-weight: 700; font-size: 1rem; color: #fff;
              background: {status_color}; margin-bottom: 1.5rem; }}
    .meta-grid {{ display: grid; grid-template-columns: repeat(auto-fill, minmax(200px, 1fr));
                  gap: 1rem; margin-bottom: 2rem; }}
    .meta-card {{ background: #1e293b; border-radius: 0.5rem; padding: 1rem; }}
    .meta-card .label {{ font-size: 0.75rem; color: #94a3b8; text-transform: uppercase;
                         letter-spacing: 0.05em; margin-bottom: 0.25rem; }}
    .meta-card .value {{ font-size: 1rem; font-weight: 600; }}
    .findings-bar {{ display: flex; gap: 1rem; margin-bottom: 2rem; }}
    .pill {{ padding: 0.4rem 1rem; border-radius: 0.375rem; font-weight: 600;
             font-size: 0.875rem; }}
    .pill.hard {{ background: #450a0a; color: #fca5a5; }}
    .pill.soft {{ background: #431407; color: #fdba74; }}
    .pill.warn {{ background: #1e3a5f; color: #93c5fd; }}
    .section {{ margin-bottom: 2rem; }}
    .section h2 {{ font-size: 1rem; font-weight: 600; color: #94a3b8;
                   text-transform: uppercase; letter-spacing: 0.05em;
                   margin-bottom: 0.75rem; border-bottom: 1px solid #334155;
                   padding-bottom: 0.5rem; }}
    .finding {{ background: #1e293b; border-left: 4px solid #334155;
                border-radius: 0 0.375rem 0.375rem 0; padding: 0.75rem 1rem;
                margin-bottom: 0.5rem; font-size: 0.875rem; }}
    .finding.hard {{ border-left-color: #ef4444; }}
    .finding.soft {{ border-left-color: #f97316; }}
    .finding.warning {{ border-left-color: #3b82f6; }}
    .finding .check {{ font-weight: 700; margin-bottom: 0.25rem; }}
    .finding .detail {{ color: #94a3b8; }}
    .finding .sample {{ color: #e2e8f0; font-size: 0.8rem; }}
    pre {{ background: #1e293b; border-radius: 0.5rem; padding: 1.5rem;
           font-family: "JetBrains Mono", "Fira Code", monospace; font-size: 0.8rem;
           line-height: 1.6; overflow-x: auto; white-space: pre-wrap;
           word-wrap: break-word; color: #cbd5e1; }}
    .footer {{ margin-top: 2rem; font-size: 0.75rem; color: #475569; }}
  </style>
</head>
<body>
  <h1>Bouncer QC Report</h1>
  <div class="badge">{status_text}</div>

  <div class="meta-grid">
    <div class="meta-card">
      <div class="label">Assay</div>
      <div class="value">{assay}</div>
    </div>
    <div class="meta-card">
      <div class="label">Mode</div>
      <div class="value">{mode}</div>
    </div>
    <div class="meta-card">
      <div class="label">Organism</div>
      <div class="value">{tags.get("organism", "—")}</div>
    </div>
    <div class="meta-card">
      <div class="label">Samples</div>
      <div class="value">{tags.get("n_samples", "—")}</div>
    </div>
    <div class="meta-card">
      <div class="label">Conditions</div>
      <div class="value">{", ".join(tags.get("conditions", [])) or "—"}</div>
    </div>
    <div class="meta-card">
      <div class="label">Data Stage</div>
      <div class="value">{tags.get("data_stage", "—")}</div>
    </div>
    <div class="meta-card">
      <div class="label">Schema</div>
      <div class="value" style="font-size:0.8rem">{schema.name}</div>
    </div>
    <div class="meta-card">
      <div class="label">QC Contract</div>
      <div class="value" style="font-size:0.8rem">{qc.name}</div>
    </div>
    <div class="meta-card">
      <div class="label">Generated</div>
      <div class="value" style="font-size:0.8rem">{datetime.now().strftime("%Y-%m-%d %H:%M")}</div>
    </div>
  </div>

  <div class="findings-bar">
    <span class="pill hard">{n_hard} hard</span>
    <span class="pill soft">{n_soft} soft</span>
    <span class="pill warn">{n_warn} warnings</span>
  </div>

  {''.join(_findings_section(findings))}

  <div class="section">
    <h2>Full Report</h2>
    <pre>{report_escaped}</pre>
  </div>

  <div class="footer">
    Generated by Bouncer QC &nbsp;|&nbsp;
    Schema: {schema} &nbsp;|&nbsp;
    QC: {qc}
  </div>
</body>
</html>"""

    output.write_text(html, encoding="utf-8")


def _findings_section(findings: list[dict]) -> list[str]:
    if not findings:
        return []
    parts = ['<div class="section"><h2>Findings</h2>']
    for f in findings:
        sev   = f.get("severity", "warning")
        check = f.get("check", "")
        msg   = f.get("message", "")
        sample = f.get("sample", "")
        found  = f.get("found", "")
        parts.append(
            f'<div class="finding {sev}">'
            f'<div class="check">{sev.upper()} — {check}</div>'
            + (f'<div class="sample">Sample: {sample}</div>' if sample else "")
            + (f'<div class="detail">Found: {found}</div>' if found else "")
            + f'<div class="detail">{msg}</div>'
            f'</div>'
        )
    parts.append("</div>")
    return parts


# ── Commands ───────────────────────────────────────────────────────────────────

@app.command()
def run(
    inputs:     list[str]      = typer.Argument(..., help="Local input files to upload"),
    schema:     str            = typer.Option(...,  "--schema", "-s",
                                              help="Bundled schema name (rna-seq/basic) OR local path to schema YAML"),
    assay:      Optional[str]  = typer.Option(None, "--assay",  "-a",
                                              help="rna-seq | flow-cytometry | qpcr  (inferred from schema name if omitted)"),
    qc:         Optional[str]  = typer.Option(None, "--qc",     "-q",
                                              help="Local path to QC YAML  (inferred from schema if omitted)"),
    mode:       str            = typer.Option("strict", "--mode", "-m", help="strict | permissive"),
    report_out: str            = typer.Option("", "--report-out", "-r",
                                              help="Save HTML report to this path (default: bouncer_report_<timestamp>.html)"),
    api_url:    str            = typer.Option("", "--api-url", envvar="BOUNCER_API_URL",
                                              help="Bouncer API base URL"),
):
    """
    Run QC on pipeline output files.

    Simplest usage — let Bouncer resolve everything from the schema name:

        bouncer run counts.tsv samplesheet.csv multiqc.json --schema rna-seq/basic

    Or supply local schema files explicitly:

        bouncer run counts.tsv samplesheet.csv \\
            --schema schemas/rna-seq/basic-schema.yaml \\
            --qc     schemas/rna-seq/basic-qc.yaml \\
            --assay  rna-seq
    """
    import httpx

    base = _resolve_api(api_url)

    # Resolve schema → concrete file paths (and maybe infer assay)
    schema_path, qc_path, inferred_assay = _resolve_schema(schema, qc)

    # Resolve assay: explicit flag wins; fall back to inferred; error if neither
    resolved_assay = assay or inferred_assay
    if not resolved_assay:
        console.print(
            "[red]--assay is required when --schema is a file path.[/red]\n"
            "[dim]Example: --assay rna-seq[/dim]"
        )
        raise typer.Exit(1)

    data_paths = [Path(p) for p in inputs]

    missing = [str(p) for p in [schema_path, qc_path, *data_paths] if not p.exists()]
    if missing:
        for m in missing:
            console.print(f"[red]File not found: {m}[/red]")
        raise typer.Exit(1)

    console.print(
        f"[bold]Bouncer QC[/bold]  "
        f"assay=[cyan]{resolved_assay}[/cyan]  "
        f"schema=[cyan]{schema_path.name}[/cyan]  "
        f"qc=[cyan]{qc_path.name}[/cyan]  "
        f"mode=[cyan]{mode}[/cyan]"
    )
    console.print(
        f"Uploading {len(data_paths)} data file(s) + schema + QC contract "
        f"to [dim]{base}[/dim] ..."
    )

    # Open all files and build multipart payload
    data_handles  = [(p.name, p.open("rb"), _mime(p)) for p in data_paths]
    schema_handle = (schema_path.name, schema_path.open("rb"), "application/x-yaml")
    qc_handle     = (qc_path.name,     qc_path.open("rb"),     "application/x-yaml")

    try:
        with httpx.Client(timeout=120) as client:
            resp = client.post(
                f"{base}/qc/run",
                data={"assay_type": resolved_assay, "mode": mode},
                files=(
                    [("files", fh) for fh in data_handles]
                    + [("schema_file", schema_handle), ("qc_file", qc_handle)]
                ),
            )
        resp.raise_for_status()
    except httpx.HTTPError as exc:
        console.print(f"[red]Upload failed: {exc}[/red]")
        raise typer.Exit(1)
    finally:
        for _, fobj, _ in data_handles:
            fobj.close()
        schema_handle[1].close()
        qc_handle[1].close()

    job = resp.json()
    job_id = job["job_id"]
    console.print(f"Job queued  [dim]id={job_id}[/dim]")

    result = _poll_job(base, job_id)
    if result is None:
        raise typer.Exit(1)

    # Print to terminal
    console.print()
    console.print(result.get("report", "(no report returned)"))

    # Save HTML report
    ts          = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = Path(report_out) if report_out else Path(f"bouncer_report_{ts}.html")
    _save_report_html(
        report=result.get("report", ""),
        result=result,
        schema=schema_path,
        qc=qc_path,
        assay=resolved_assay,
        mode=mode,
        output=report_path,
    )
    console.print(f"\n[green]Report saved →[/green] {report_path}")

    if not result.get("passed", False):
        raise typer.Exit(1)

    if fid := result.get("feature_id"):
        console.print(f"[green]Feature registered:[/green] {fid}")


@app.command("list-features")
def list_features_cmd(
    assay:   Optional[str] = typer.Option(None, "--assay"),
    stage:   Optional[str] = typer.Option(None, "--stage"),
    api_url: str           = typer.Option("", "--api-url", envvar="BOUNCER_API_URL"),
):
    """List registered features from the API feature store."""
    import httpx

    base   = _resolve_api(api_url)
    params: dict[str, str] = {}
    if assay:
        params["assay"] = assay
    if stage:
        params["data_stage"] = stage

    try:
        resp = httpx.get(f"{base}/features", params=params, timeout=30)
        resp.raise_for_status()
    except httpx.HTTPError as exc:
        console.print(f"[red]{exc}[/red]")
        raise typer.Exit(1)

    rows = resp.json()
    if not rows:
        console.print("[yellow]No features found.[/yellow]")
        return

    table = Table(show_header=True, header_style="bold")
    for col in rows[0].keys():
        table.add_column(col)
    for row in rows:
        table.add_row(*[str(v) for v in row.values()])
    console.print(table)


@app.command()
def pull(
    feature_id: str = typer.Option(..., "--id", help="Feature ID to download"),
    output:     str = typer.Option("output.h5ad", "--output", "-o"),
    api_url:    str = typer.Option("", "--api-url", envvar="BOUNCER_API_URL"),
):
    """Download a registered feature set as an h5ad file."""
    import httpx

    base = _resolve_api(api_url)
    console.print(f"Downloading feature [cyan]{feature_id}[/cyan] ...")

    try:
        resp = httpx.get(f"{base}/features/{feature_id}/download", timeout=120)
        resp.raise_for_status()
    except httpx.HTTPError as exc:
        console.print(f"[red]{exc}[/red]")
        raise typer.Exit(1)

    Path(output).write_bytes(resp.content)
    console.print(f"[green]Saved → {output}[/green]  ({len(resp.content):,} bytes)")


@app.command()
def ping(
    api_url: str = typer.Option("", "--api-url", envvar="BOUNCER_API_URL",
                                help="Bouncer API base URL"),
):
    """Check that the Bouncer API is reachable."""
    import httpx

    base = _resolve_api(api_url)
    console.print(f"Pinging [dim]{base}[/dim] ...")
    try:
        resp = httpx.get(f"{base}/health", timeout=15)
        resp.raise_for_status()
        console.print(f"[green]OK[/green]  {resp.json()}")
    except httpx.HTTPError as exc:
        console.print(f"[red]Unreachable:[/red] {exc}")
        raise typer.Exit(1)


if __name__ == "__main__":
    app()
