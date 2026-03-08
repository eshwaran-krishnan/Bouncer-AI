"""
read_csv — reads CSV and TSV files.

Used by the extract node for samplesheets and counts matrices.
Returns shape, columns, dtypes, null counts, and sample rows so the
agent can reason about content without loading the full matrix.
"""

import os


def read_csv(path: str, sep: str = "infer", max_cols_preview: int = 20) -> dict:
    """
    Read a delimited file (CSV or TSV). Returns a structured summary.

    Args:
        path:             Absolute path to the file.
        sep:              Delimiter. "infer" auto-detects from extension.
        max_cols_preview: Cap on columns shown in sample_rows to avoid
                          huge payloads for wide counts matrices.

    Returns dict with:
        shape, columns, dtypes, null_counts, sample_rows, separator_used
    """
    import pandas as pd

    if sep == "infer":
        ext = os.path.splitext(path)[1].lower()
        sep = "\t" if ext in (".tsv", ".txt") else ","

    try:
        df = pd.read_csv(path, sep=sep, index_col=False, low_memory=False)
    except Exception as e:
        return {"path": path, "error": str(e)}

    # For very wide files (counts matrices), limit preview columns
    preview_cols = list(df.columns[:max_cols_preview])
    sample = df[preview_cols].head(5)

    return {
        "path": path,
        "shape": list(df.shape),           # [n_rows, n_cols]
        "columns": list(df.columns),
        "preview_columns": preview_cols,    # capped list
        "dtypes": {col: str(dtype) for col, dtype in df.dtypes.items()},
        "null_counts": df.isnull().sum().to_dict(),
        "sample_rows": sample.to_dict(orient="records"),
        "separator_used": sep,
    }
