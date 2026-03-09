"""
bouncer/qc/design_checker.py — Experimental design checks.

All findings from this module have severity="warning" — design checks never
block registration. They are written to AnnData.uns['warnings'] so downstream
analysts are informed.
"""

from __future__ import annotations

import pandas as pd
from bouncer.config import QCContract, DesignCheck
from bouncer.models.finding import Finding


def check_design(
    samplesheet_df: pd.DataFrame,
    qc: QCContract,
    multiqc_general_stats: dict[str, dict] | None = None,  # for library_size_outliers
) -> list[Finding]:
    """
    Run all design checks defined in the QC contract.

    Args:
        samplesheet_df:       Full samplesheet DataFrame.
        qc:                   Loaded QCContract.
        multiqc_general_stats: Optional MultiQC stats for library-size outlier check.
    """
    findings: list[Finding] = []

    dispatch = {
        "min_replicates_per_condition":    _min_replicates,
        "balanced_groups":                 _balanced_groups,
        "control_condition_present":       _control_present,
        "batch_not_confounded_with_condition": _batch_confounded,
        "batch_column_present":            _batch_column_present,
        "sex_column_present":              _sex_column_present,
        "library_size_outliers":           _library_size_outliers,
        "condition_label_consistency":     _label_consistency,
        "donor_id_crosses_conditions":     _donor_crosses_conditions,
    }

    for rule in qc.design:
        fn = dispatch.get(rule.check)
        if fn is None:
            continue
        if rule.check == "library_size_outliers":
            findings.extend(fn(samplesheet_df, rule, multiqc_general_stats))
        else:
            findings.extend(fn(samplesheet_df, rule))

    return findings


# ── Individual check functions ─────────────────────────────────────────────────

def _min_replicates(df: pd.DataFrame, rule: DesignCheck) -> list[Finding]:
    if "condition" not in df.columns or "replicate" not in df.columns:
        return []
    min_reps = int(rule.value or 3)
    counts = df.groupby("condition")["replicate"].nunique()
    bad = counts[counts < min_reps]
    findings = []
    for cond, n in bad.items():
        findings.append(Finding(
            severity="warning",
            stage="design",
            check=rule.check,
            field="condition",
            found=f"{n} replicates",
            expected=f">= {min_reps}",
            message=f"Condition '{cond}' has only {n} biological replicate(s) "
                    f"(minimum recommended: {min_reps}).",
        ))
    return findings


def _balanced_groups(df: pd.DataFrame, rule: DesignCheck) -> list[Finding]:
    if "condition" not in df.columns:
        return []
    max_ratio = rule.max_imbalance_ratio or 3.0
    counts = df["condition"].value_counts()
    if len(counts) < 2:
        return []
    ratio = counts.iloc[0] / counts.iloc[-1]
    if ratio > max_ratio:
        return [Finding(
            severity="warning",
            stage="design",
            check=rule.check,
            field="condition",
            found=f"ratio {ratio:.1f}",
            expected=f"<= {max_ratio}",
            message=f"Group size imbalance: largest/smallest = {ratio:.1f} "
                    f"('{counts.index[0]}' n={counts.iloc[0]}, "
                    f"'{counts.index[-1]}' n={counts.iloc[-1]}). "
                    "This can reduce statistical power for the minority group.",
        )]
    return []


def _control_present(df: pd.DataFrame, rule: DesignCheck) -> list[Finding]:
    if "condition" not in df.columns:
        return []
    labels = rule.accepted_labels or [
        "control", "ctrl", "untreated", "vehicle", "DMSO",
        "wildtype", "WT", "mock", "neg_ctrl", "negative_control", "scramble",
    ]
    conditions = df["condition"].dropna().str.lower().unique()
    labels_lower = {l.lower() for l in labels}
    if not any(c in labels_lower for c in conditions):
        return [Finding(
            severity="warning",
            stage="design",
            check=rule.check,
            field="condition",
            message="No recognisable control condition found. "
                    f"Accepted control labels: {labels}. "
                    "Without a control, fold-change calculations have no reference anchor.",
        )]
    return []


def _batch_confounded(df: pd.DataFrame, rule: DesignCheck) -> list[Finding]:
    if "batch" not in df.columns or "condition" not in df.columns:
        return []
    cross = pd.crosstab(df["condition"], df["batch"])
    # Perfect confounding: each batch appears in only one condition
    batch_per_condition = (cross > 0).sum(axis=0)
    confounded_batches = batch_per_condition[batch_per_condition == 1].index.tolist()
    if len(confounded_batches) == len(cross.columns):
        return [Finding(
            severity="warning",
            stage="design",
            check=rule.check,
            message="Batch and condition are perfectly confounded — batch effects cannot "
                    "be separated from biological signal. DE results require extreme caution.",
        )]
    return []


def _batch_column_present(df: pd.DataFrame, rule: DesignCheck) -> list[Finding]:
    batch_cols = [c for c in df.columns if c.startswith("batch")]
    if not batch_cols:
        return [Finding(
            severity="warning",
            stage="design",
            check=rule.check,
            message="No batch column found in samplesheet. Strongly recommended even "
                    "if only one batch exists — label it explicitly to enable batch correction.",
        )]
    return []


def _sex_column_present(df: pd.DataFrame, rule: DesignCheck) -> list[Finding]:
    if "sex" not in df.columns:
        return [Finding(
            severity="warning",
            stage="design",
            check=rule.check,
            message="No 'sex' column found. Sex is a significant transcriptional covariate "
                    "and cannot be corrected at the analysis stage if absent.",
        )]
    return []


def _library_size_outliers(
    df: pd.DataFrame,
    rule: DesignCheck,
    multiqc_stats: dict[str, dict] | None,
) -> list[Finding]:
    """Flag samples whose total read count is > N std devs from the group mean."""
    if multiqc_stats is None:
        return []

    threshold = rule.std_dev_threshold or 2.5
    # Try to extract total reads from STAR or general stats
    total_reads: dict[str, float] = {}
    for sample, metrics in multiqc_stats.items():
        for key in ("STAR_total_reads", "FastQC_Total Sequences", "total_reads"):
            if key in metrics:
                try:
                    total_reads[sample] = float(metrics[key])
                    break
                except (TypeError, ValueError):
                    pass

    if len(total_reads) < 3:
        return []

    values = pd.Series(total_reads)
    mean, std = values.mean(), values.std()
    if std == 0:
        return []

    findings = []
    for sample, reads in total_reads.items():
        z = abs(reads - mean) / std
        if z > threshold:
            findings.append(Finding(
                severity="warning",
                stage="design",
                check=rule.check,
                sample=sample,
                found=f"{int(reads):,} reads (z={z:.1f})",
                expected=f"within {threshold} SD of group mean ({int(mean):,})",
                message=f"Sample '{sample}' library size is {z:.1f} SD from the group mean. "
                        "May indicate a failed library or sequencing lane issue.",
            ))
    return findings


def _label_consistency(df: pd.DataFrame, rule: DesignCheck) -> list[Finding]:
    """Fuzzy-match condition labels to detect typos / capitalisation differences."""
    if "condition" not in df.columns:
        return []
    from difflib import SequenceMatcher
    labels = df["condition"].dropna().unique().tolist()
    if len(labels) < 2:
        return []

    flagged: list[tuple[str, str, float]] = []
    for i, a in enumerate(labels):
        for b in labels[i + 1:]:
            ratio = SequenceMatcher(None, a.lower(), b.lower()).ratio()
            if 0.8 <= ratio < 1.0:
                flagged.append((a, b, ratio))

    if flagged:
        pairs = "; ".join(f"'{a}' ↔ '{b}' (sim={s:.0%})" for a, b, s in flagged[:5])
        return [Finding(
            severity="warning",
            stage="design",
            check=rule.check,
            field="condition",
            message=f"Near-duplicate condition labels detected (possible typos): {pairs}",
        )]
    return []


def _donor_crosses_conditions(df: pd.DataFrame, rule: DesignCheck) -> list[Finding]:
    if "donor_id" not in df.columns or "condition" not in df.columns:
        return []
    donor_conditions = df.groupby("donor_id")["condition"].nunique()
    single = donor_conditions[donor_conditions == 1].index.tolist()
    if single:
        return [Finding(
            severity="warning",
            stage="design",
            check=rule.check,
            field="donor_id",
            found=f"{len(single)} donor(s) in only one condition",
            message=f"{len(single)} donor(s) appear in only one condition and cannot "
                    "be used as a blocking factor for paired analysis: "
                    f"{single[:5]}",
        )]
    return []
