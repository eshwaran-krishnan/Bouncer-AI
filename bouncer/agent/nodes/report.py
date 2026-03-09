"""
bouncer/agent/nodes/report.py — Report generation node.

Builds a structured text report from findings (no LLM needed — deterministic
formatting from structured Finding objects). Determines passed/failed based
on mode. Uses Rich markup for terminal output.
"""

from __future__ import annotations

from bouncer.agent.state import BouncerState
from bouncer.models.finding import Finding
from bouncer.utils.logger import get_logger

log = get_logger("bouncer.report")


_SEV_ICON = {"hard": "✗", "soft": "⚠", "warning": "ℹ"}
_SEV_ORDER = {"hard": 0, "soft": 1, "warning": 2}


def generate_report(state: BouncerState) -> BouncerState:
    """
    Build the QC report, set state["passed"] and state["report"].
    """
    findings = [Finding(**f) for f in state["findings"]]
    mode     = state["mode"]
    assay    = state["assay_type"]
    tags     = state["tags"]

    hard     = [f for f in findings if f.severity == "hard"]
    soft     = [f for f in findings if f.severity == "soft"]
    warnings = [f for f in findings if f.severity == "warning"]

    blocking = [f for f in hard if f.is_blocking(mode)]
    passed   = len(blocking) == 0

    lines: list[str] = []

    # ── Header ────────────────────────────────────────────────────────────────
    status_str = "PASSED" if passed else "FAILED"
    lines.append("=" * 72)
    lines.append(f"  BOUNCER QC REPORT — {assay.upper()}")
    lines.append("=" * 72)
    lines.append(f"  Status  : {status_str}")
    lines.append(f"  Mode    : {mode}")
    lines.append(f"  Samples : {tags.get('n_samples', 'unknown')}")
    lines.append(f"  Organism: {tags.get('organism', 'unknown')}")
    if tags.get("conditions"):
        lines.append(f"  Conditions: {', '.join(tags['conditions'])}")
    lines.append(f"  Findings: {len(hard)} hard  |  {len(soft)} soft  |  {len(warnings)} warnings")
    if state["missing_fields"]:
        lines.append(f"  Missing fields: {', '.join(state['missing_fields'])}")
    lines.append("=" * 72)

    # ── Hard findings ─────────────────────────────────────────────────────────
    if hard:
        lines.append("\n── HARD FAILURES " + "─" * 55)
        for f in hard:
            lines.extend(_format_finding(f))

    # ── Soft findings ─────────────────────────────────────────────────────────
    if soft:
        lines.append("\n── SOFT WARNINGS " + "─" * 55)
        for f in soft:
            lines.extend(_format_finding(f))

    # ── Design warnings ───────────────────────────────────────────────────────
    if warnings:
        lines.append("\n── DESIGN ADVISORIES " + "─" * 51)
        for f in warnings:
            lines.extend(_format_finding(f))

    # ── Summary ───────────────────────────────────────────────────────────────
    lines.append("\n" + "─" * 72)
    if passed:
        if soft or warnings:
            lines.append("  RESULT: PASSED WITH NOTES — review soft warnings before analysis.")
        else:
            lines.append("  RESULT: PASSED — all checks cleared.")
    else:
        lines.append(f"  RESULT: FAILED — {len(blocking)} hard finding(s) block registration.")
        lines.append("  Fix the hard failures and re-run to register this dataset.")

    if mode == "permissive" and hard:
        lines.append(f"  NOTE: permissive mode — {len(hard)} hard finding(s) logged but not blocking.")
    lines.append("─" * 72)

    state["report"] = "\n".join(lines)
    state["passed"] = passed

    log.info("qc_result",
             passed=passed,
             mode=mode,
             n_hard=len(hard),
             n_soft=len(soft),
             n_warnings=len(warnings),
             n_blocking=len(blocking))

    return state


def _format_finding(f: Finding) -> list[str]:
    icon  = _SEV_ICON[f.severity]
    stage = f.stage.upper().replace("_", " ")
    lines = [f"  {icon} [{stage}] {f.check}"]
    if f.sample:
        lines.append(f"      Sample  : {f.sample}")
    if f.found:
        lines.append(f"      Found   : {f.found}")
    if f.expected:
        lines.append(f"      Expected: {f.expected}")
    lines.append(f"      {f.message}")
    return lines
