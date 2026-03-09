"""
read_eds — reads Applied Biosystems / ThermoFisher QuantStudio EDS files.

EDS (Experiment Design & Results) is the native file format for
QuantStudio Real-Time PCR systems. The .eds file is a ZIP archive
containing XML metadata and result CSVs/data.

Surfaces for the QC agent:
  - Experiment metadata (instrument, run date, chemistry, plate type)
  - Sample names and their target assays
  - CT values per sample × target
  - Amplification status flags (Undetermined, Passed, etc.)
"""

from __future__ import annotations

import zipfile
import io
import os
import xml.etree.ElementTree as ET
from typing import Any


def read_eds(path: str) -> dict:
    """
    Read a QuantStudio EDS file and extract QC-relevant content.

    Args:
        path: Absolute path to the .eds file.

    Returns dict with:
        experiment_info, plate_layout (samples × targets),
        results (CT values, flags), n_samples, n_targets
    """
    if not os.path.isfile(path):
        return {"path": path, "error": f"File not found: {path}"}

    try:
        with zipfile.ZipFile(path, "r") as zf:
            contents = zf.namelist()
            result: dict[str, Any] = {
                "path": path,
                "format": "quantstudio_eds",
                "archive_contents": contents,
            }

            # ── Experiment metadata from appliedbiosystems XML ────────────────
            experiment_info: dict = {}
            xml_candidates = [f for f in contents if f.endswith(".xml") and "experiment" in f.lower()]
            if not xml_candidates:
                xml_candidates = [f for f in contents if f.endswith(".xml")]

            for xml_file in xml_candidates[:3]:
                try:
                    with zf.open(xml_file) as xf:
                        tree = ET.parse(xf)
                        root = tree.getroot()
                        # Strip namespace for simpler access
                        for elem in root.iter():
                            elem.tag = elem.tag.split("}")[-1]

                        for tag in ("InstrumentType", "InstrumentName", "RunStartTime",
                                    "RunEndTime", "Chemistry", "PlateType", "ExperimentName",
                                    "UserName", "SampleVolume", "PassivereferenceName"):
                            node = root.find(f".//{tag}")
                            if node is not None and node.text:
                                experiment_info[tag] = node.text.strip()
                except Exception:
                    continue

            result["experiment_info"] = experiment_info

            # ── Results data — try Results/Results.csv or similar ─────────────
            result_csv_candidates = [
                f for f in contents
                if f.lower().endswith(".csv") and any(
                    kw in f.lower() for kw in ("result", "amplification", "quantity", "data")
                )
            ]

            parsed_results: list[dict] = []
            sample_names: set[str] = set()
            target_names: set[str] = set()

            for csv_file in result_csv_candidates[:2]:
                try:
                    with zf.open(csv_file) as cf:
                        import pandas as pd
                        # QuantStudio CSVs have a header block before the data
                        raw = cf.read().decode("utf-8", errors="replace")
                        lines = raw.splitlines()

                        # Find the actual data table start (line with "Well" column)
                        data_start = 0
                        for i, line in enumerate(lines):
                            if line.startswith("Well,") or line.startswith('"Well"'):
                                data_start = i
                                break

                        df = pd.read_csv(
                            io.StringIO("\n".join(lines[data_start:])),
                            low_memory=False,
                        )

                        # Normalise common column name variants
                        col_map = {
                            "Sample Name": "sample_name",
                            "Target Name": "target_name",
                            "CT": "ct",
                            "Cт": "ct",
                            "Cq": "ct",
                            "CT Mean": "ct_mean",
                            "Quantity": "quantity",
                            "Quantity Mean": "quantity_mean",
                            "Automatic Ct Threshold": "auto_ct_threshold",
                            "CT Threshold": "ct_threshold",
                            "Amp Score": "amp_score",
                            "Cq Conf": "cq_conf",
                        }
                        df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})

                        if "sample_name" in df.columns:
                            sample_names.update(df["sample_name"].dropna().unique().tolist())
                        if "target_name" in df.columns:
                            target_names.update(df["target_name"].dropna().unique().tolist())

                        # Surface CT value table (cap rows for payload size)
                        if "sample_name" in df.columns and "ct" in df.columns:
                            ct_cols = ["sample_name", "target_name", "ct"]
                            if "ct_threshold" in df.columns:
                                ct_cols.append("ct_threshold")
                            if "amp_score" in df.columns:
                                ct_cols.append("amp_score")
                            available = [c for c in ct_cols if c in df.columns]
                            parsed_results.append({
                                "source_file": csv_file,
                                "shape": list(df.shape),
                                "columns": list(df.columns),
                                "ct_table": df[available].head(20).to_dict(orient="records"),
                                "ct_summary": {
                                    "n_undetermined": int((df.get("ct", "").astype(str) == "Undetermined").sum())
                                    if "ct" in df.columns else None,
                                },
                            })
                except Exception as e:
                    parsed_results.append({"source_file": csv_file, "parse_error": str(e)})

            result["n_samples"] = len(sample_names)
            result["samples"] = sorted(sample_names)
            result["n_targets"] = len(target_names)
            result["targets"] = sorted(target_names)
            result["result_files"] = parsed_results

            return result

    except zipfile.BadZipFile:
        return {"path": path, "error": "Not a valid ZIP/EDS archive. File may be corrupt."}
    except Exception as e:
        return {"path": path, "error": str(e)}
