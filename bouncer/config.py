"""
bouncer/config.py — Pydantic models for YAML schema and QC contracts.

Models are derived from the actual YAML structure in schemas/:
  - dual-threshold multiqc rules (hard_min/soft_min, severity_below_*)
  - metadata check section (samplesheet value-level checks)
  - rich ColumnDef with unique, allowed_values, description
"""

from __future__ import annotations

from typing import Literal
from pydantic import BaseModel, model_validator


# ── Schema Contract ────────────────────────────────────────────────────────────

class ColumnDef(BaseModel):
    name: str
    dtype: Literal["str", "int", "float", "category", "bool"]
    required: bool = True
    unique: bool = False
    allowed_values: list[str] | None = None
    description: str | None = None


class SchemaContract(BaseModel):
    version: str
    assay_type: str
    data_stage: str
    index_column: str | None = None          # e.g. "gene_id" for RNA-seq
    sample_columns: str = "from_samplesheet" # "from_samplesheet" or explicit list
    metadata_columns: list[ColumnDef] = []
    output_features: list[str] = []

    def required_columns(self) -> list[str]:
        return [c.name for c in self.metadata_columns if c.required]

    def column_by_name(self, name: str) -> ColumnDef | None:
        return next((c for c in self.metadata_columns if c.name == name), None)


# ── QC Contract ────────────────────────────────────────────────────────────────

SeverityLevel = Literal["hard", "soft", "warning"]


class MultiqcRule(BaseModel):
    """
    Dual-threshold MultiQC metric rule.

    hard_min/hard_max = absolute bounds; violation → severity_below/above_hard
    soft_min/soft_max = advisory bounds; violation → severity_below/above_soft
    severity_outside_* = symmetric shorthand when both min+max are set.
    """
    metric: str
    description: str | None = None
    rationale: str | None = None

    hard_min: float | None = None
    soft_min: float | None = None
    hard_max: float | None = None
    soft_max: float | None = None

    # Per-direction severities
    severity_below_hard:   SeverityLevel | None = None
    severity_below_soft:   SeverityLevel | None = None
    severity_above_hard:   SeverityLevel | None = None
    severity_above_soft:   SeverityLevel | None = None

    # Symmetric shorthand (used when rule has both min + max)
    severity_outside_hard: SeverityLevel | None = None
    severity_outside_soft: SeverityLevel | None = None

    def evaluate(self, value: float) -> tuple[SeverityLevel | None, str | None]:
        """
        Return (severity, message) for a given metric value, or (None, None) if OK.
        Hard bounds are checked first; soft bounds only checked if hard passes.
        """
        # Hard bounds
        if self.hard_min is not None and value < self.hard_min:
            sev = self.severity_below_hard or self.severity_outside_hard or "hard"
            return sev, f"{self.metric}={value:.4g} below hard_min={self.hard_min}"
        if self.hard_max is not None and value > self.hard_max:
            sev = self.severity_above_hard or self.severity_outside_hard or "hard"
            return sev, f"{self.metric}={value:.4g} above hard_max={self.hard_max}"

        # Soft bounds
        if self.soft_min is not None and value < self.soft_min:
            sev = self.severity_below_soft or self.severity_outside_soft or "soft"
            return sev, f"{self.metric}={value:.4g} below soft_min={self.soft_min}"
        if self.soft_max is not None and value > self.soft_max:
            sev = self.severity_above_soft or self.severity_outside_soft or "soft"
            return sev, f"{self.metric}={value:.4g} above soft_max={self.soft_max}"

        return None, None


class CountsCheck(BaseModel):
    check: str
    description: str | None = None
    rationale: str | None = None

    # Simple fixed severity (for boolean checks like no_negative_values)
    severity: SeverityLevel | None = None

    # Dual-threshold numeric checks
    hard_min: float | None = None
    soft_min: float | None = None
    hard_max: float | None = None
    soft_max: float | None = None
    severity_below_hard: SeverityLevel | None = None
    severity_below_soft: SeverityLevel | None = None
    severity_above_hard: SeverityLevel | None = None
    severity_above_soft: SeverityLevel | None = None

    def evaluate_numeric(self, value: float) -> tuple[SeverityLevel | None, str | None]:
        if self.hard_min is not None and value < self.hard_min:
            return self.severity_below_hard or "hard", f"{self.check}={value} below hard_min={self.hard_min}"
        if self.hard_max is not None and value > self.hard_max:
            return self.severity_above_hard or "hard", f"{self.check}={value} above hard_max={self.hard_max}"
        if self.soft_min is not None and value < self.soft_min:
            return self.severity_below_soft or "soft", f"{self.check}={value} below soft_min={self.soft_min}"
        if self.soft_max is not None and value > self.soft_max:
            return self.severity_above_soft or "soft", f"{self.check}={value} above soft_max={self.soft_max}"
        return None, None


class MetadataCheck(BaseModel):
    """Samplesheet value-level checks (beyond schema dtype/required)."""
    check: str
    description: str | None = None
    rationale: str | None = None

    severity: SeverityLevel | None = None

    hard_min: float | None = None
    soft_min: float | None = None
    hard_max: float | None = None
    soft_max: float | None = None
    severity_below_hard: SeverityLevel | None = None
    severity_below_soft: SeverityLevel | None = None

    def evaluate_numeric(self, value: float) -> tuple[SeverityLevel | None, str | None]:
        if self.hard_min is not None and value < self.hard_min:
            return self.severity_below_hard or "hard", f"{self.check}={value} below hard_min={self.hard_min}"
        if self.soft_min is not None and value < self.soft_min:
            return self.severity_below_soft or "soft", f"{self.check}={value} below soft_min={self.soft_min}"
        return None, None


class DesignCheck(BaseModel):
    check: str
    description: str | None = None
    rationale: str | None = None
    severity: SeverityLevel = "warning"

    # Check-specific parameters
    value: float | int | None = None
    max_imbalance_ratio: float | None = None
    accepted_labels: list[str] | None = None
    labels: list[str] | None = None          # legacy alias
    std_dev_threshold: float | None = None

    @model_validator(mode="after")
    def normalise_labels(self) -> "DesignCheck":
        """Normalise legacy 'labels' field to 'accepted_labels'."""
        if self.accepted_labels is None and self.labels is not None:
            self.accepted_labels = self.labels
        return self


class QCContract(BaseModel):
    version: str
    assay_type: str
    pipeline: str | None = None
    data_stage: str

    multiqc:       list[MultiqcRule]   = []
    counts_matrix: list[CountsCheck]   = []
    metadata:      list[MetadataCheck] = []
    design:        list[DesignCheck]   = []

    def multiqc_rule(self, metric: str) -> MultiqcRule | None:
        return next((r for r in self.multiqc if r.metric == metric), None)


# ── Loaders ───────────────────────────────────────────────────────────────────

def load_schema(path: str) -> SchemaContract:
    import yaml
    with open(path, "r") as f:
        raw = yaml.safe_load(f)
    try:
        return SchemaContract(**raw)
    except Exception as exc:
        raise ValueError(
            f"Schema validation failed for: {path}\n"
            f"  output_features: {raw.get('output_features') if isinstance(raw, dict) else '(not a dict)'}\n"
            f"  {exc}"
        ) from exc


def load_qc(path: str) -> QCContract:
    import yaml
    with open(path, "r") as f:
        raw = yaml.safe_load(f)
    try:
        return QCContract(**raw)
    except Exception as exc:
        raise ValueError(
            f"QC contract validation failed for: {path}\n"
            f"  {exc}"
        ) from exc
