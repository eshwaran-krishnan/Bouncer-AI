"""
bouncer/cli.py — Typer CLI (API-client mode).

Set your deployed Modal endpoint before running:
    export BOUNCER_API_URL=https://your-org--bouncer-qc-api.modal.run

Or pass it inline:
    bouncer run file1 file2 --assay rna-seq --schema rna-seq/basic \\
        --api-url https://your-org--bouncer-qc-api.modal.run
"""

from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

app     = typer.Typer(help="Bouncer — biological data quality contract layer.")
console = Console()

_POLL_INTERVAL = 3    # seconds between status polls
_POLL_MAX      = 200  # ~10 min ceiling


# ── Helpers ────────────────────────────────────────────────────────────────────

def _resolve_api(api_url: str) -> str:
    url = api_url or os.environ.get("BOUNCER_API_URL", "")
    if not url:
        console.print(
            "[red]No API URL configured.[/red]\n"
            "Set [bold]BOUNCER_API_URL[/bold] or pass [bold]--api-url[/bold]."
        )
        raise typer.Exit(1)
    return url.rstrip("/")


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
                f"[bold]QC job running[/bold] [dim]({job_id})[/dim]"
                f" — {elapsed}s elapsed"
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


# ── Commands ───────────────────────────────────────────────────────────────────

@app.command()
def run(
    inputs:  list[str] = typer.Argument(..., help="Local input files to upload"),
    assay:   str       = typer.Option(..., "--assay",  "-a", help="rna-seq | flow-cytometry | qpcr"),
    schema:  str       = typer.Option(..., "--schema", "-s", help="Schema name, e.g. rna-seq/basic"),
    mode:    str       = typer.Option("strict", "--mode", "-m", help="strict | permissive"),
    api_url: str       = typer.Option("", "--api-url", envvar="BOUNCER_API_URL",
                                      help="Bouncer API base URL"),
):
    """Upload files to the Bouncer API and stream back QC results."""
    import httpx

    base = _resolve_api(api_url)

    # Validate local files
    paths   = [Path(p) for p in inputs]
    missing = [str(p) for p in paths if not p.exists()]
    if missing:
        for m in missing:
            console.print(f"[red]File not found: {m}[/red]")
        raise typer.Exit(1)

    console.print(
        f"[bold]Bouncer QC[/bold]  "
        f"assay=[cyan]{assay}[/cyan]  "
        f"schema=[cyan]{schema}[/cyan]  "
        f"mode=[cyan]{mode}[/cyan]"
    )
    console.print(f"Uploading {len(paths)} file(s) to [dim]{base}[/dim] ...")

    # Build multipart payload — keep files open until request completes
    file_handles = [(p.name, p.open("rb"), _mime(p)) for p in paths]
    try:
        with httpx.Client(timeout=120) as client:
            resp = client.post(
                f"{base}/qc/run",
                data={"assay_type": assay, "schema_name": schema, "mode": mode},
                files=[("files", fh) for fh in file_handles],
            )
        resp.raise_for_status()
    except httpx.HTTPError as exc:
        console.print(f"[red]Upload failed: {exc}[/red]")
        raise typer.Exit(1)
    finally:
        for _, fobj, _ in file_handles:
            fobj.close()

    job = resp.json()
    job_id = job["job_id"]
    console.print(f"Job queued  [dim]id={job_id}[/dim]")

    result = _poll_job(base, job_id)
    if result is None:
        raise typer.Exit(1)

    console.print()
    console.print(result.get("report", "(no report returned)"))

    if not result.get("passed", False):
        raise typer.Exit(1)

    if fid := result.get("feature_id"):
        console.print(f"\n[green]Feature registered:[/green] {fid}")


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
    output:     str = typer.Option("output.h5ad", "--output", "-o", help="Destination file"),
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


if __name__ == "__main__":
    app()
