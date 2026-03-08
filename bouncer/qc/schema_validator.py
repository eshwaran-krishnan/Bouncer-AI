"""
bouncer/qc/schema_validator.py — Validate samplesheet against a SchemaContract.

Checks:
  - Required columns present
  - No nulls in required columns
  - Dtype compatibility
  - allowed_values constraints (category columns)
  - unique constraints
  - sample_id ↔ counts matrix column cross-reference
"""

from __future__ import annotations

import pandas as pd
from bouncer.config import SchemaContract
from bouncer.models.finding import Finding


def validate_schema(
    samplesheet_df: pd.DataFrame,
    schema: SchemaContract,
    counts_columns: list[str] | None = None,  # column headers from counts matrix
) -> list[Finding]:
    """
    Validate samplesheet_df against the schema contract.

    Args:
        samplesheet_df: The parsed samplesheet as a DataFrame.
        schema:         Loaded SchemaContract.
        counts_columns: Optional list of sample-ID column headers from the
                        counts matrix, used for cross-reference check.

    Returns list of Finding objects (empty = all passed).
    """
    findings: list[Finding] = []
    cols = set(samplesheet_df.columns)

    for col_def in schema.metadata_columns:
        name = col_def.name

        # ── Required column present ────────────────────────────────────────
        if name not in cols:
            if col_def.required:
                findings.append(Finding(
                    severity="hard",
                    stage="schema",
                    check="required_column_missing",
                    field=name,
                    message=f"Required column '{name}' not found in samplesheet.",
                ))
            continue  # can't check further if column absent

        series = samplesheet_df[name]

        # ── No nulls in required columns ──────────────────────────────────
        null_count = series.isna().sum()
        if col_def.required and null_count > 0:
            findings.append(Finding(
                severity="hard",
                stage="schema",
                check="null_in_required_column",
                field=name,
                found=f"{null_count} null(s)",
                expected="no nulls",
                message=f"Column '{name}' is required but has {null_count} null value(s).",
            ))

        # ── Dtype check ───────────────────────────────────────────────────
        dtype_ok, dtype_msg = _check_dtype(series.dropna(), col_def.dtype)
        if not dtype_ok:
            findings.append(Finding(
                severity="soft",
                stage="schema",
                check="dtype_mismatch",
                field=name,
                expected=col_def.dtype,
                found=dtype_msg,
                message=f"Column '{name}' dtype mismatch: expected {col_def.dtype}, got {dtype_msg}.",
            ))

        # ── Allowed values ────────────────────────────────────────────────
        if col_def.allowed_values:
            bad = series.dropna()[~series.dropna().astype(str).isin(col_def.allowed_values)]
            if not bad.empty:
                bad_vals = bad.unique().tolist()[:5]
                findings.append(Finding(
                    severity="hard",
                    stage="schema",
                    check="invalid_category_value",
                    field=name,
                    expected=str(col_def.allowed_values),
                    found=str(bad_vals),
                    message=f"Column '{name}' contains values not in allowed_values: {bad_vals}",
                ))

        # ── Unique constraint ─────────────────────────────────────────────
        if col_def.unique:
            dupes = series[series.duplicated(keep=False)].unique().tolist()[:5]
            if dupes:
                findings.append(Finding(
                    severity="hard",
                    stage="schema",
                    check="uniqueness_violation",
                    field=name,
                    found=str(dupes),
                    expected="all unique",
                    message=f"Column '{name}' must be unique but has duplicates: {dupes}",
                ))

    # ── Cross-reference: sample_id ↔ counts matrix columns ─────────────────
    if counts_columns is not None and "sample_id" in cols:
        sheet_ids = set(samplesheet_df["sample_id"].dropna().astype(str))
        counts_ids = set(counts_columns)

        only_sheet = sheet_ids - counts_ids
        only_counts = counts_ids - sheet_ids

        if only_sheet:
            findings.append(Finding(
                severity="hard",
                stage="schema",
                check="sample_ids_match_samplesheet",
                field="sample_id",
                found=f"in samplesheet only: {sorted(only_sheet)[:5]}",
                message=f"{len(only_sheet)} sample(s) in samplesheet not found in counts matrix columns.",
            ))
        if only_counts:
            findings.append(Finding(
                severity="hard",
                stage="schema",
                check="sample_ids_match_samplesheet",
                field="sample_id",
                found=f"in counts matrix only: {sorted(only_counts)[:5]}",
                message=f"{len(only_counts)} counts matrix column(s) not found in samplesheet.",
            ))

    return findings


def _check_dtype(series: pd.Series, expected: str) -> tuple[bool, str]:
    """Return (ok, actual_description)."""
    if series.empty:
        return True, "empty"

    if expected in ("int",):
        try:
            pd.to_numeric(series, errors="raise").apply(
                lambda x: int(x) == x
            )
            return True, "int"
        except Exception:
            return False, str(series.dtype)

    if expected == "float":
        try:
            pd.to_numeric(series, errors="raise")
            return True, "float"
        except Exception:
            return False, str(series.dtype)

    if expected in ("str", "category"):
        return True, str(series.dtype)  # always coercible

    if expected == "bool":
        valid = {"true", "false", "yes", "no", "1", "0", "t", "f"}
        bad = series.astype(str).str.lower()[~series.astype(str).str.lower().isin(valid)]
        if bad.empty:
            return True, "bool"
        return False, f"non-boolean values: {bad.unique()[:3].tolist()}"

    return True, str(series.dtype)
