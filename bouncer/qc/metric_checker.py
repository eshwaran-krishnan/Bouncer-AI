"""
bouncer/qc/metric_checker.py — Check MultiQC general_stats against QCContract rules.

Also handles counts matrix integrity checks.
"""

from __future__ import annotations

import pandas as pd
from bouncer.config import QCContract
from bouncer.models.finding import Finding


# ── MultiQC metric checks ──────────────────────────────────────────────────────

def check_multiqc_metrics(
    general_stats: dict[str, dict],   # {sample_name: {metric: value}}
    qc: QCContract,
) -> list[Finding]:
    """
    Apply each MultiqcRule to every sample in general_stats.

    A single sample failing a hard rule produces a hard Finding for that sample.
    Missing metrics produce a soft warning (metric may not be present for all tools).
    """
    findings: list[Finding] = []

    for rule in qc.multiqc:
        metric = rule.metric
        found_in_any = False

        for sample, metrics in general_stats.items():
            if metric not in metrics:
                continue
            found_in_any = True

            raw = metrics[metric]
            try:
                value = float(raw)
            except (TypeError, ValueError):
                findings.append(Finding(
                    severity="soft",
                    stage="multiqc",
                    check=metric,
                    sample=sample,
                    field=metric,
                    found=str(raw),
                    expected="numeric",
                    message=f"Non-numeric value for {metric} in sample '{sample}': {raw}",
                ))
                continue

            severity, message = rule.evaluate(value)
            if severity is not None:
                findings.append(Finding(
                    severity=severity,
                    stage="multiqc",
                    check=metric,
                    sample=sample,
                    field=metric,
                    found=f"{value:.4g}",
                    message=message or "",
                ))

        if not found_in_any and qc.multiqc:
            findings.append(Finding(
                severity="soft",
                stage="multiqc",
                check=metric,
                field=metric,
                message=f"Metric '{metric}' not found in MultiQC general_stats. "
                        f"Check that the expected tool ran and produced output.",
            ))

    return findings


# ── Counts matrix checks ───────────────────────────────────────────────────────

def check_counts_matrix(
    counts_df: pd.DataFrame,
    qc: QCContract,
) -> list[Finding]:
    """
    Apply counts_matrix checks from the QC contract to the counts DataFrame.

    counts_df should be (genes × samples): rows = genes, columns = samples.
    """
    findings: list[Finding] = []
    check_fns = {
        "no_negative_values":      _no_negative_values,
        "no_all_zero_samples":     _no_all_zero_samples,
        "no_all_zero_genes":       _no_all_zero_genes,
        "min_expressed_genes":     _min_expressed_genes,
        "min_library_size":        _min_library_size,
        "no_duplicate_sample_ids": _no_duplicate_sample_ids,
        # sample_ids_match_samplesheet is handled in schema_validator
    }

    for rule in qc.counts_matrix:
        fn = check_fns.get(rule.check)
        if fn is None:
            continue
        findings.extend(fn(counts_df, rule))

    return findings


def _no_negative_values(df: pd.DataFrame, rule) -> list[Finding]:
    if (df.values < 0).any():
        return [Finding(
            severity=rule.severity or "hard",
            stage="counts_matrix",
            check=rule.check,
            message="Counts matrix contains negative values — impossible for raw read counts.",
        )]
    return []


def _no_all_zero_samples(df: pd.DataFrame, rule) -> list[Finding]:
    zero_cols = df.columns[df.sum(axis=0) == 0].tolist()
    if zero_cols:
        return [Finding(
            severity=rule.severity or "hard",
            stage="counts_matrix",
            check=rule.check,
            found=str(zero_cols[:5]),
            message=f"{len(zero_cols)} sample(s) have all-zero counts: {zero_cols[:5]}",
        )]
    return []


def _no_all_zero_genes(df: pd.DataFrame, rule) -> list[Finding]:
    zero_rows = df.index[df.sum(axis=1) == 0].tolist()
    if zero_rows:
        return [Finding(
            severity=rule.severity or "soft",
            stage="counts_matrix",
            check=rule.check,
            found=f"{len(zero_rows)} genes",
            message=f"{len(zero_rows)} gene(s) have zero counts across all samples. "
                    "Filter before modelling.",
        )]
    return []


def _min_expressed_genes(df: pd.DataFrame, rule) -> list[Finding]:
    n_expressed = int((df.sum(axis=1) > 0).sum())
    sev, msg = rule.evaluate_numeric(n_expressed)
    if sev:
        return [Finding(
            severity=sev,
            stage="counts_matrix",
            check=rule.check,
            found=str(n_expressed),
            expected=f">= {rule.soft_min or rule.hard_min}",
            message=msg or f"Only {n_expressed} expressed genes.",
        )]
    return []


def _min_library_size(df: pd.DataFrame, rule) -> list[Finding]:
    findings: list[Finding] = []
    lib_sizes = df.sum(axis=0)
    for sample, size in lib_sizes.items():
        sev, msg = rule.evaluate_numeric(float(size))
        if sev:
            findings.append(Finding(
                severity=sev,
                stage="counts_matrix",
                check=rule.check,
                sample=str(sample),
                found=str(int(size)),
                expected=f">= {rule.soft_min or rule.hard_min}",
                message=msg or f"Sample '{sample}' library size {int(size)} is too low.",
            ))
    return findings


def _no_duplicate_sample_ids(df: pd.DataFrame, rule) -> list[Finding]:
    dupes = df.columns[df.columns.duplicated()].tolist()
    if dupes:
        return [Finding(
            severity=rule.severity or "hard",
            stage="counts_matrix",
            check=rule.check,
            found=str(dupes[:5]),
            message=f"Duplicate sample IDs in counts matrix columns: {dupes[:5]}",
        )]
    return []
